use crate::Config;
use futures::{AsyncRead, AsyncWrite};
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};
#[cfg(feature = "native-tls")]
mod native_tls_stream;

#[cfg(feature = "rustls")]
mod rustls_tls_stream;

#[cfg(feature = "vendored-openssl")]
mod opentls_tls_stream;

// #[cfg(feature = "native-tls")]
// pub(crate) use native_tls_stream::TlsStream as NativeTlsStream;

// #[cfg(feature = "rustls")]
// pub(crate) use rustls_tls_stream::TlsStream as RustlsTlsStream;

// #[cfg(feature = "vendored-openssl")]
// pub(crate) use opentls_tls_stream::TlsStream as OptenSslTlsStream;

pub(crate) enum TlsStream<S: AsyncRead + AsyncWrite + Unpin + Send> {
    #[cfg(feature = "vendored-openssl")]
    Openssl(opentls_tls_stream::TlsStream<S>),
    #[cfg(feature = "rustls")]
    Rustls(rustls_tls_stream::TlsStream<S>),
    #[cfg(feature = "native-tls")]
    NativeTls(native_tls_stream::TlsStream<S>),
}

impl<S> TlsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    pub(crate) fn get_mut(&mut self) -> &mut S {
        match self {
            #[cfg(feature = "vendored-openssl")]
            TlsStream::Openssl(s) => s.get_mut(),
            #[cfg(feature = "rustls")]
            TlsStream::Rustls(s) => s.get_mut(),
            #[cfg(feature = "native-tls")]
            TlsStream::NativeTls(s) => s.get_mut(),
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send> AsyncRead for TlsStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let inner = Pin::get_mut(self);
        match inner {
            #[cfg(feature = "vendored-openssl")]
            TlsStream::Openssl(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(feature = "rustls")]
            TlsStream::Rustls(s) => Pin::new(&mut s.0).poll_read(cx, buf),
            #[cfg(feature = "native-tls")]
            TlsStream::NativeTls(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send> AsyncWrite for TlsStream<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let inner = Pin::get_mut(self);
        match inner {
            #[cfg(feature = "vendored-openssl")]
            TlsStream::Openssl(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(feature = "rustls")]
            TlsStream::Rustls(s) => Pin::new(&mut s.0).poll_write(cx, buf),
            #[cfg(feature = "native-tls")]
            TlsStream::NativeTls(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let inner = Pin::get_mut(self);
        match inner {
            #[cfg(feature = "vendored-openssl")]
            TlsStream::Openssl(s) => Pin::new(s).poll_flush(cx),
            #[cfg(feature = "rustls")]
            TlsStream::Rustls(s) => Pin::new(&mut s.0).poll_flush(cx),
            #[cfg(feature = "native-tls")]
            TlsStream::NativeTls(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let inner = Pin::get_mut(self);
        match inner {
            #[cfg(feature = "vendored-openssl")]
            TlsStream::Openssl(s) => Pin::new(s).poll_close(cx),
            #[cfg(feature = "rustls")]
            TlsStream::Rustls(s) => Pin::new(&mut s.0).poll_close(cx),
            #[cfg(feature = "native-tls")]
            TlsStream::NativeTls(s) => Pin::new(s).poll_close(cx),
        }
    }
}

#[cfg(feature = "rustls")]
pub(crate) async fn create_tls_stream_rustls<S: AsyncRead + AsyncWrite + Unpin + Send>(
    config: &Config,
    stream: S,
) -> crate::Result<TlsStream<S>> {
    rustls_tls_stream::TlsStream::new(config, stream)
        .await
        .map(TlsStream::Rustls)
}

#[cfg(feature = "native-tls")]
pub(crate) async fn create_tls_stream_native_tls<S: AsyncRead + AsyncWrite + Unpin + Send>(
    config: &Config,
    stream: S,
) -> crate::Result<TlsStream<S>> {
    native_tls_stream::create_tls_stream(config, stream)
        .await
        .map(TlsStream::NativeTls)
}

#[cfg(feature = "vendored-openssl")]
pub(crate) async fn create_tls_stream_openssl<S: AsyncRead + AsyncWrite + Unpin + Send>(
    config: &Config,
    stream: S,
) -> crate::Result<TlsStream<S>> {
    opentls_tls_stream::create_tls_stream(config, stream)
        .await
        .map(TlsStream::Openssl)
}
