use crate::Config;
use futures_util::io::{AsyncRead, AsyncWrite};

#[cfg(feature = "rustls")]
mod rustls_tls_stream;

#[cfg(feature = "rustls")]
pub(crate) use rustls_tls_stream::TlsStream;

#[cfg(all(feature = "rustls"))]
pub(crate) async fn create_tls_stream<S: AsyncRead + AsyncWrite + Unpin + Send>(
    config: &Config,
    stream: S,
) -> crate::Result<TlsStream<S>> {
    TlsStream::new(config, stream).await
}