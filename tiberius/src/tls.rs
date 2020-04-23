use std::io;
use std::mem::MaybeUninit;
use std::pin::Pin;
use std::task::{self, Poll};

use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
};
#[cfg(feature = "tls")]
use tokio_tls::TlsStream;

pub enum MaybeTlsStream {
    Raw(TcpStream),
    #[cfg(feature = "tls")]
    Tls(TlsStream<TcpStream>),
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
