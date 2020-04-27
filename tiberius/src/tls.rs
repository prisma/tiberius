use crate::protocol::{
    codec::{Decode, Encode, PacketHeader, PacketStatus, PacketType},
    HEADER_BYTES,
};
use bytes::BytesMut;
use futures::ready;
use std::{
    cmp, io,
    mem::MaybeUninit,
    pin::Pin,
    task::{self, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
};
#[cfg(feature = "tls")]
use tokio_tls::TlsStream;
use tracing::{self, event, Level};

/// A wrapper to handle either TLS or bare connections.
pub(crate) enum MaybeTlsStream {
    Raw(TcpStream),
    #[cfg(feature = "tls")]
    Tls(TlsStream<TlsPreloginWrapper<TcpStream>>),
}

impl AsyncRead for MaybeTlsStream {
    unsafe fn prepare_uninitialized_buffer(&self, buf: &mut [MaybeUninit<u8>]) -> bool {
        match self {
            MaybeTlsStream::Raw(s) => s.prepare_uninitialized_buffer(buf),
            #[cfg(feature = "tls")]
            MaybeTlsStream::Tls(s) => s.prepare_uninitialized_buffer(buf),
        }
    }

    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut task::Context,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            MaybeTlsStream::Raw(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(feature = "tls")]
            MaybeTlsStream::Tls(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for MaybeTlsStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut task::Context,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            MaybeTlsStream::Raw(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(feature = "tls")]
            MaybeTlsStream::Tls(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Raw(s) => Pin::new(s).poll_flush(cx),
            #[cfg(feature = "tls")]
            MaybeTlsStream::Tls(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Raw(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(feature = "tls")]
            MaybeTlsStream::Tls(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

/// On TLS handshake, the server expects to get and sends back normal TDS
/// packets. To use a common TLS library, we must implement a wrapper for
/// packet handling on this stage.
///
/// What it does is it interferes on handshake for TDS packet handling,
/// and when complete, just passes the calls to the underlying connection.
pub(crate) struct TlsPreloginWrapper<S> {
    stream: Option<S>,
    pending_handshake: bool,

    header_buf: [u8; HEADER_BYTES],
    header_pos: usize,
    read_remaining: usize,

    wr_buf: Vec<u8>,
    header_written: bool,
}

impl<S> TlsPreloginWrapper<S> {
    pub(crate) fn new(stream: S) -> Self {
        TlsPreloginWrapper {
            stream: Some(stream),
            pending_handshake: true,

            header_buf: [0u8; HEADER_BYTES],
            header_pos: 0,
            read_remaining: 0,
            wr_buf: vec![0u8; HEADER_BYTES],
            header_written: false,
        }
    }

    pub(crate) fn handshake_complete(&mut self) {
        self.pending_handshake = false;
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for TlsPreloginWrapper<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        // Normal operation does not need any extra treatment, we handle packets
        // in the codec.
        if !self.pending_handshake {
            return Pin::new(&mut self.stream.as_mut().unwrap()).poll_read(cx, buf);
        }

        let inner = self.get_mut();

        // Read the headers separately and do not send them to the Tls
        // connection handling.
        if !inner.header_buf[inner.header_pos..].is_empty() {
            while !inner.header_buf[inner.header_pos..].is_empty() {
                let read = ready!(Pin::new(&mut inner.stream.as_mut().unwrap())
                    .poll_read(cx, &mut inner.header_buf[inner.header_pos..]))?;

                if read == 0 {
                    return Poll::Ready(Ok(0));
                }

                inner.header_pos += read;
            }

            let header = PacketHeader::decode(&mut BytesMut::from(&inner.header_buf[..]))
                .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;

            // We only get pre-login packets in the handshake process.
            assert_eq!(header.ty, PacketType::PreLogin);

            // And we know from this point on how much data we should expect
            inner.read_remaining = header.length as usize - HEADER_BYTES;

            event!(
                Level::TRACE,
                "Reading packet of {} bytes",
                inner.read_remaining,
            );
        }

        let max_read = cmp::min(inner.read_remaining, buf.len());

        // TLS connector gets whatever we have after the header.
        let read = ready!(
            Pin::new(&mut inner.stream.as_mut().unwrap()).poll_read(cx, &mut buf[..max_read])
        )?;

        inner.read_remaining -= read;

        // All data is read, after this we're expecting a new header.
        if inner.read_remaining == 0 {
            inner.header_pos = 0;
        }

        Poll::Ready(Ok(read))
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for TlsPreloginWrapper<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        // Normal operation does not need any extra treatment, we handle
        // packets in the codec.
        if !self.pending_handshake {
            return Pin::new(&mut self.stream.as_mut().unwrap()).poll_write(cx, buf);
        }

        // Buffering data.
        self.wr_buf.extend_from_slice(buf);

        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<io::Result<()>> {
        let inner = self.get_mut();

        // If on handshake mode, wraps the data to a TDS packet before sending.
        if inner.pending_handshake {
            if !inner.header_written {
                let mut header = PacketHeader::new(inner.wr_buf.len(), 0);

                header.ty = PacketType::PreLogin;
                header.status = PacketStatus::EndOfMessage;

                header
                    .encode(&mut &mut inner.wr_buf[0..HEADER_BYTES])
                    .map_err(|_| {
                        io::Error::new(io::ErrorKind::InvalidInput, "Could not encode header.")
                    })?;

                inner.header_written = true;
            }

            while !inner.wr_buf.is_empty() {
                event!(
                    Level::TRACE,
                    "Writing a packet of {} bytes",
                    inner.wr_buf.len(),
                );

                let written =
                    ready!(Pin::new(&mut inner.stream.as_mut().unwrap())
                        .poll_write(cx, &mut inner.wr_buf))?;

                inner.wr_buf.drain(..written);
            }

            inner.wr_buf.resize(HEADER_BYTES, 0);
            inner.header_written = false;
        }

        Pin::new(&mut inner.stream.as_mut().unwrap()).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream.as_mut().unwrap()).poll_shutdown(cx)
    }
}
