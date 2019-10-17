//! A pure-rust TDS implementation for Microsoft SQL Server (>=2008)
#![allow(unused_imports, dead_code)] // TODO
#![recursion_limit = "256"]

use std::convert::TryInto;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::result;
use std::sync::{atomic, Arc};
use std::task::{self, Poll};

use async_stream::try_stream;
use byteorder::LittleEndian;
use futures_util::future::{self, FutureExt};
use futures_util::stream::StreamExt;
use futures_util::{pin_mut, ready};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{self, mpsc};
use tracing::{self, debug_span, event, trace_span, Level};
use winauth::NextBytes;

mod collation;
mod connect;
mod error;
mod protocol;
use protocol::EncryptionLevel;
mod row;

pub use connect::{connect_tcp, connect_tcp_sql_browser, ConnectParams};
pub use error::Error;
pub type Result<T> = result::Result<T, Error>;
pub use row::Row;

pub(crate) fn get_driver_version() -> u64 {
    env!("CARGO_PKG_VERSION")
        .splitn(6, '.')
        .enumerate()
        .fold(0u64, |acc, part| {
            acc | (part.1.parse::<u64>().unwrap() << (part.0 * 8))
        })
}

struct TlsNegotiateWrapper<S> {
    stream: S,
    pending_handshake: bool,

    header_buf: [u8; protocol::HEADER_BYTES],
    header_pos: usize,
    read_remaining: usize,

    wr_buf: Vec<u8>,
    header_written: bool,
}

impl<S> TlsNegotiateWrapper<S> {
    fn new(stream: S) -> Self {
        TlsNegotiateWrapper {
            stream,
            pending_handshake: true,

            header_buf: [0u8; protocol::HEADER_BYTES],
            header_pos: 0,
            read_remaining: 0,
            wr_buf: vec![0u8; protocol::HEADER_BYTES],
            header_written: false,
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for TlsNegotiateWrapper<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context,
        mut buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        if !self.pending_handshake {
            return Pin::new(&mut self.stream).poll_read(cx, buf);
        }

        let span = trace_span!("TlsNeg::poll_read");
        let enter_ = span.enter();

        let inner = self.get_mut();
        if !inner.header_buf[inner.header_pos..].is_empty() {
            event!(Level::TRACE, "read_header");
            while !inner.header_buf[inner.header_pos..].is_empty() {
                let read = ready!(Pin::new(&mut inner.stream)
                    .poll_read(cx, &mut inner.header_buf[inner.header_pos..]))?;
                event!(Level::TRACE, read_header_bytes = read);
                if read == 0 {
                    return Poll::Ready(Ok(0));
                }
                inner.header_pos += read;
            }

            let header = protocol::PacketHeader::unserialize(&inner.header_buf)
                .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;

            assert_eq!(header.ty, protocol::PacketType::PreLogin);
            assert_eq!(header.status, protocol::PacketStatus::EndOfMessage);
            inner.read_remaining = header.length as usize - protocol::HEADER_BYTES;
            event!(Level::TRACE, packet_bytes = inner.read_remaining);
        }

        let max_read = ::std::cmp::min(inner.read_remaining, buf.len());
        let read = ready!(Pin::new(&mut inner.stream).poll_read(cx, &mut buf[..max_read]))?;
        inner.read_remaining -= read;
        if inner.read_remaining == 0 {
            inner.header_pos = 0;
        }
        Poll::Ready(Ok(read))
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for TlsNegotiateWrapper<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if !self.pending_handshake {
            return Pin::new(&mut self.stream).poll_write(cx, buf);
        }

        let span = trace_span!("TlsNeg::poll_write");
        let enter_ = span.enter();
        event!(Level::TRACE, amount = buf.len());

        self.wr_buf.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<io::Result<()>> {
        let inner = self.get_mut();

        if inner.pending_handshake {
            let span = trace_span!("TlsNeg::poll_flush");
            let enter_ = span.enter();

            if !inner.header_written {
                event!(Level::TRACE, "prepending header to buf");
                let mut header = protocol::PacketHeader::new(inner.wr_buf.len(), 0);
                header.ty = protocol::PacketType::PreLogin;
                header.status = protocol::PacketStatus::EndOfMessage;
                header.serialize(&mut inner.wr_buf)?;
                inner.header_written = true;
            }

            while !inner.wr_buf.is_empty() {
                let written =
                    ready!(Pin::new(&mut inner.stream).poll_write(cx, &mut inner.wr_buf))?;
                event!(Level::TRACE, written = written);
                inner.wr_buf.drain(..written);
            }
            inner.wr_buf.resize(protocol::HEADER_BYTES, 0);
            inner.header_written = false;
            event!(Level::TRACE, "flushing underlying stream");
        }
        Pin::new(&mut inner.stream).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

#[derive(Debug)]
enum ConnectionResult {
    Row(protocol::TokenRow),
    NextResultSet,
}

struct Connection {
    ctx: Arc<protocol::Context>,
    reader: Box<dyn AsyncRead + Unpin>,
    // receives result-receivers, where incoming result rows should be sent to
    result_receivers: mpsc::UnboundedReceiver<mpsc::UnboundedSender<ConnectionResult>>,
}

impl Connection {
    async fn into_future(mut self) -> Result<()> {
        let mut reader =
            protocol::TokenStreamReader::new(protocol::PacketReader::new(&mut self.reader));

        let mut current_receiver = None;
        loop {
            event!(Level::TRACE, "reading next token");
            let ty = reader.read_token().await?;
            match ty {
                protocol::TokenType::ColMetaData => {
                    reader.read_colmetadata_token(&self.ctx).await?;
                }
                protocol::TokenType::Row => {
                    let row = reader.read_row_token(&self.ctx).await?;
                    if current_receiver.is_none() {
                        current_receiver = Some(self.result_receivers.next().await.unwrap());
                    }
                    event!(Level::TRACE, sent_row= ?row);
                    let _ = current_receiver
                        .as_mut()
                        .unwrap()
                        .try_send(ConnectionResult::Row(row));
                }
                protocol::TokenType::Done => {
                    let done = reader.read_done_token(&self.ctx).await?;

                    // TODO: make sure we panic when executing 2 queries but only expecting one result
                    if !done.status.contains(protocol::DoneStatus::MORE) {
                        current_receiver = None; // TODO: only if single resultset
                    } else {
                        let _ = current_receiver
                            .as_mut()
                            .unwrap()
                            .try_send(ConnectionResult::NextResultSet);
                    }
                }
                _ => panic!("TODO: unsupported token: {:?}", ty),
            }
        }
    }
}

pub struct Client {
    ctx: Arc<protocol::Context>,
    writer: Arc<sync::Mutex<Box<dyn AsyncWrite + Unpin>>>,
    conn_handler: future::Shared<Pin<Box<dyn Future<Output = Result<()>>>>>,
    sender: mpsc::UnboundedSender<mpsc::UnboundedSender<ConnectionResult>>,
}

impl Client {
    async fn simple_query<'a>(&'a self, query: &str) -> Result<QueryStream> {
        let span = debug_span!("simple_query", query = query);
        let _enter = span.enter();

        let mut writer = self.writer.clone();
        let mut writer = writer.lock().await;

        // Subscribe for results
        let (sender, mut receiver) = mpsc::unbounded_channel();
        // let mut receiver = receiver.fuse();
        self.sender.clone().try_send(sender).expect("TODO");

        // Fire a query
        event!(Level::DEBUG, "WRITING QUERY");
        let header = protocol::PacketHeader {
            ty: protocol::PacketType::SQLBatch,
            status: protocol::PacketStatus::NormalMessage,
            ..self.ctx.new_header(0)
        };
        let mut wr = protocol::PacketWriter::new(&mut *writer, header);
        protocol::write_trans_descriptor(&mut wr, &self.ctx, 0 /* TODO */).await?;
        for b2 in query.encode_utf16() {
            let bytes = b2.to_le_bytes();
            wr.write_bytes(&self.ctx, &bytes[..]).await?;
        }
        wr.finish(&self.ctx).await?;
        ::std::mem::drop(writer);

        println!("WAITING for results");
        let qs = QueryStream {
            conn_handler: self.conn_handler.clone(),
            results: receiver,
            done: false,
            has_next_resultset: false,
        };
        Ok(qs)
    }
}

struct QueryStream {
    conn_handler: future::Shared<Pin<Box<dyn Future<Output = Result<()>>>>>,
    results: mpsc::UnboundedReceiver<ConnectionResult>,

    done: bool,
    has_next_resultset: bool,
}

impl QueryStream {
    /// Move to the next resultset and make `poll_next` return rows for it
    pub fn next_resultset(&mut self) -> bool {
        if self.has_next_resultset {
            self.has_next_resultset = false;
            return true;
        }
        false
    }
}

impl Drop for QueryStream {
    fn drop(&mut self) {
        if self.has_next_resultset {
            panic!("QueryStream dropped but not all resultsets were handled");
        }
    }
}
impl tokio::stream::Stream for QueryStream {
    type Item = Result<row::Row>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context,
    ) -> std::task::Poll<Option<Self::Item>> {
        if self.done || self.has_next_resultset {
            return Poll::Ready(None);
        }

        // Handle incoming results and paralelly allow the connection
        // to dispatch results to other streams (or us)
        match self.results.poll_next_unpin(cx) {
            Poll::Pending => match self.conn_handler.poll_unpin(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Err(err)) => Poll::Ready(Some(Err(err))),
                // The connection future never terminates, except for errors.
                Poll::Ready(Ok(_)) => unreachable!(),
            },
            Poll::Ready(Some(ConnectionResult::Row(row))) => Poll::Ready(Some(Ok(row::Row(row)))),
            Poll::Ready(Some(ConnectionResult::NextResultSet)) => {
                self.has_next_resultset = true;
                Poll::Ready(None)
            }
            Poll::Ready(None) => {
                self.done = true;
                Poll::Ready(None)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures_util::pin_mut;
    use futures_util::stream::StreamExt;
    use tracing::{span, Level};

    #[tokio::test]
    async fn port() -> crate::Result<()> {
        let subscriber = tracing_subscriber::FmtSubscriber::builder()
            .with_env_filter("tiberius=trace")
            .finish();
        tracing::subscriber::set_global_default(subscriber).unwrap();

        let client = super::connect_tcp(
            super::ConnectParams {
                ssl: super::protocol::EncryptionLevel::Required,
                host: "".to_owned(),
                trust_cert: true,
            },
            "127.0.0.1:1433".parse().unwrap(),
        )
        .await?;

        let stream = client.simple_query("SELECT 42; SELECT 49").await?;
        pin_mut!(stream);
        while let Some(el) = stream.next().await {
            println!("item1 {:?}", el);
            // design does work, if we fire another query here, it gets handled aswell
            let stream = client.simple_query("SELECT 44").await?;
            pin_mut!(stream);
            while let Some(el) = stream.next().await {
                println!("item2 {:?}", el.unwrap().get::<_, i32>(0));
            }
        }
        println!("moving on {}", stream.next_resultset());
        while let Some(el) = stream.next().await {
            println!("item3 {:?}", el);
        }
        Ok(())
    }
}
