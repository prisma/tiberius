//! A pure-rust TDS implementation for Microsoft SQL Server (>=2008)
#![allow(unused_imports, dead_code)] // TODO
#![recursion_limit="256"]

use std::result;
use std::future::Future;
use std::pin::Pin;
use std::sync::{atomic, Arc};
use std::task::{self, Poll};
use std::io;

use async_stream::try_stream;
use byteorder::LittleEndian;
use futures_util::future::{self, FutureExt};
use futures_util::stream::StreamExt;
use futures_util::{pin_mut, ready};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{self, mpsc};
use tracing::{self, event, debug_span, trace_span, Level};
use winauth::NextBytes;

mod collation;
mod error;
mod protocol;
use protocol::EncryptionLevel;
mod row;

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

/// Settings for the connection, everything that isn't IO/transport specific (e.g. authentication)
pub struct ConnectParams {
    // pub host: Cow<'static, str>,
    pub ssl: EncryptionLevel,
    // pub trust_cert: bool,
    // pub auth: AuthMethod,
    // pub target_db: Option<Cow<'static, str>>,
    // pub spn: Cow<'static, str>,
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
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut task::Context, mut buf: &mut [u8]) -> Poll<io::Result<usize>> {
        if !self.pending_handshake {
            return Pin::new(&mut self.stream).poll_read(cx, buf);
        }
        
        let span = trace_span!("TlsNeg::poll_read");
        let enter_ = span.enter();

        let inner = self.get_mut();
        if !inner.header_buf[inner.header_pos..].is_empty() {
            event!(Level::TRACE, "read_header");
            while !inner.header_buf[inner.header_pos..].is_empty() {
                let read = ready!(Pin::new(&mut inner.stream).poll_read(cx, &mut inner.header_buf[inner.header_pos..]))?;
                event!(Level::TRACE, read_header_bytes= read);
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
            event!(Level::TRACE, packet_bytes= inner.read_remaining);
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
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut task::Context, buf: &[u8]) -> Poll<io::Result<usize>> {
        if !self.pending_handshake {
            return Pin::new(&mut self.stream).poll_write(cx, buf);
        }

        let span = trace_span!("TlsNeg::poll_write");
        let enter_ = span.enter();
        event!(Level::TRACE, amount= buf.len());

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
                let written = ready!(Pin::new(&mut inner.stream).poll_write(cx, &mut inner.wr_buf))?;
                event!(Level::TRACE, written= written);
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

struct Connecting<S> {
    stream: S,
    ctx: protocol::Context,
    params: ConnectParams,
}

impl<S: AsyncRead + AsyncWrite + Unpin> Connecting<S> {
    async fn prelogin(&mut self) -> Result<()> {
        // Send PreLogin
        let mut msg = protocol::PreloginMessage::new();
        msg.encryption = self.params.ssl;
        if msg.encryption != EncryptionLevel::NotSupported {
            #[cfg(not(feature = "tls"))]
            panic!("TLS support is not enabled in this build, but required for this configuration!");
        }
        let mut buf = msg.serialize()?;
        let header = protocol::PacketHeader {
            ty: protocol::PacketType::PreLogin,
            status: protocol::PacketStatus::EndOfMessage,
            ..self.ctx.new_header(buf.len())
        };
        header.serialize(&mut buf)?;
        self.stream.write_all(&buf).await?;

        // Wait for PreLogin response
        let mut reader = protocol::PacketReader::new(&mut self.stream);
        let response = protocol::PreloginMessage::unserialize(&mut reader).await?;
        println!("{:?}", response);
        self.params.ssl = match (msg.encryption, response.encryption) {
            (EncryptionLevel::NotSupported, EncryptionLevel::NotSupported) => EncryptionLevel::NotSupported,
            (EncryptionLevel::Off, EncryptionLevel::Off) => EncryptionLevel::Off,
            (EncryptionLevel::On, EncryptionLevel::Off) | 
            (EncryptionLevel::On, EncryptionLevel::NotSupported) => {
                panic!("todo: terminate connection, invalid encryption")
            }
            (_, _) => EncryptionLevel::On,
        };
        Ok(())
    }

    async fn tls_handshake(mut self) -> Result<Connecting<impl AsyncRead + AsyncWrite + Unpin>> {
        let mut builder = native_tls::TlsConnector::builder();
        // TODO if disable_verification {
            builder.danger_accept_invalid_certs(true);
                    // .danger_accept_invalid_hostnames(true)
                    // .use_sni(false);
        // }

        let cx = builder.build().unwrap();
        let connector = tokio_tls::TlsConnector::from(cx);

        let wrapped = TlsNegotiateWrapper::new(self.stream);
        let mut ret = Connecting {
            stream: connector.connect("", wrapped).await.expect("TODO: handle error"),
            ctx: self.ctx,
            params: self.params,
        };
        event!(Level::TRACE, "TLS handshake done");
        ret.stream.get_mut().pending_handshake = false;
        Ok(ret)
    }

    async fn login(&mut self) -> Result<()> {
        let mut login_message = protocol::LoginMessage::new();
        let mut builder = winauth::windows::NtlmSspiBuilder::new(); // TODO SPN, channel bindings
        let mut sso_client = builder.build()?;
        let buf = sso_client.next_bytes(None)?;
        login_message.integrated_security = buf;
        let mut buf = login_message.serialize(&self.ctx)?;
        let header = protocol::PacketHeader {
            ty: protocol::PacketType::TDSv7Login,
            status: protocol::PacketStatus::EndOfMessage,
            ..self.ctx.new_header(buf.len())
        };
        header.serialize(&mut buf)?;
        event!(Level::TRACE, "Sending login");
        self.stream.write_all(&buf).await?;

        // Perform SSO
        let mut reader = protocol::PacketReader::new(&mut self.stream);
        let header = reader.read_header().await?;
        event!(Level::TRACE, "Received {:?}", header.ty);
        if header.ty != protocol::PacketType::TabularResult {
            return Err(Error::Protocol(
                "expected tabular result in response to login message".into(),
            ));
        }
        reader.read_packet_with_header(&header).await?;

        let mut reader = protocol::TokenStreamReader::new(reader);
        assert_eq!(reader.read_token().await?, protocol::TokenType::SSPI);
        let sso_bytes = reader.read_sspi_token().await?;
        event!(Level::TRACE, sspi_len= sso_bytes.len());
        match sso_client.next_bytes(Some(sso_bytes))? {
            Some(sso_response) => {
                event!(Level::TRACE, sspi_response_len= sso_response.len());
                let header = protocol::PacketHeader {
                    ty: protocol::PacketType::TDSv7Login,
                    status: protocol::PacketStatus::EndOfMessage,
                    ..self.ctx.new_header(sso_response.len() + protocol::HEADER_BYTES)
                };
                let mut header_buf = [0u8; protocol::HEADER_BYTES];
                header.serialize(&mut header_buf)?;
                let mut buf = vec![];
                buf.extend_from_slice(&header_buf);
                buf.extend_from_slice(&sso_response);
                self.stream.write_all(&buf).await?;
            }
            None => unreachable!(),
        }
        Ok(())
    }
}


pub async fn connect(params: ConnectParams) -> Result<Client> {
    //let addr = .parse::<::std::net::SocketAddr>().unwrap();
    let mut stream = TcpStream::connect("localhost:1433").await?;
    stream.set_nodelay(true)?;
    let mut ctx = protocol::Context::new();

    let mut connecting = Connecting { stream, ctx, params };
    connecting.prelogin().await?;
    let mut connecting = connecting.tls_handshake().await?;
    connecting.login().await?;

    // TODO: if tokio-tls supports splitting itself, do that instead
    let (mut reader, writer) = tokio::io::split(connecting.stream);

    // Connection should be ready
    let mut ts = protocol::TokenStreamReader::new(protocol::PacketReader::new(&mut reader));
    while let token = ts.read_token().await? {
        match token {
            protocol::TokenType::EnvChange => {
                println!("env change {:?}", ts.read_env_change_token().await?);
            }
            protocol::TokenType::Info => {
                println!("info {:?}", ts.read_info_token().await?);
            }
            protocol::TokenType::LoginAck => {
                println!("login ack {:?}", ts.read_login_ack_token().await?);
            }
            protocol::TokenType::Done => {
                println!("done {:?}", ts.read_done_token(&connecting.ctx).await?);
                break;
            }
            _ => panic!("TODO"),
        }
    }

    let (sender, result_receivers) = mpsc::unbounded_channel();

    let ctx = Arc::new(connecting.ctx);
    let conn = Connection {
        ctx: ctx.clone(),
        reader: Box::new(reader),
        result_receivers,
    };

    let mut f = conn.into_future();
    let conn_fut: Pin<Box<dyn Future<Output = Result<()>>>> = Box::pin(future::poll_fn(move |ctx| {
        let span = trace_span!("conn");
        let _enter = span.enter();
        unsafe { Pin::new_unchecked(&mut f) }.poll(ctx)
    }));

    let inner = Client {
        ctx,
        writer: Arc::new(sync::Mutex::new(Box::new(writer))),
        conn_handler: conn_fut.shared(),
        sender,
    };
    Ok(inner)
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
        let mut reader = protocol::TokenStreamReader::new(protocol::PacketReader::new(&mut self.reader));
       
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
                    let _ = current_receiver.as_mut().unwrap().try_send(ConnectionResult::Row(row));
                }
                protocol::TokenType::Done => {
                    let done = reader.read_done_token(&self.ctx).await?;

                    // TODO: make sure we panic when executing 2 queries but only expecting one result
                    if !done.status.contains(protocol::DoneStatus::MORE) {
                        current_receiver = None; // TODO: only if single resultset
                    } else {
                        let _ = current_receiver.as_mut().unwrap().try_send(ConnectionResult::NextResultSet);
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
        let span = debug_span!("simple_query", query=query);
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

    fn poll_next(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context) -> std::task::Poll<Option<Self::Item>> {
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
            }
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
    use futures_util::stream::StreamExt;
    use futures_util::pin_mut;
    use tracing::{span, Level};

    #[tokio::test]
    async fn port() -> crate::Result<()> {
        let subscriber = tracing_subscriber::FmtSubscriber::builder()
            .with_env_filter("tiberius=trace")
            .finish();
        tracing::subscriber::set_global_default(subscriber).unwrap();

        let client = super::connect(super::ConnectParams {
            ssl: super::protocol::EncryptionLevel::Required,
        }).await?;

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
