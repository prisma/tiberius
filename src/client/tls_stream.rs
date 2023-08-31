use crate::Config;
use futures_util::io::{AsyncRead, AsyncWrite};

cfg_if::cfg_if! {
    if #[cfg(feature = "rustls")] {
        mod rustls_tls_stream;

        pub(crate) use rustls_tls_stream::TlsStream;

        pub(crate) async fn create_tls_stream<S: AsyncRead + AsyncWrite + Unpin + Send>(
            config: &Config,
            stream: S,
        ) -> crate::Result<TlsStream<S>> {
            TlsStream::new(config, stream).await
        }
    } else if #[cfg(feature = "vendored-openssl")] {
        mod opentls_tls_stream;

        pub(crate) use opentls_tls_stream::TlsStream;

        pub(crate) async fn create_tls_stream<S: AsyncRead + AsyncWrite + Unpin + Send>(
            config: &Config,
            stream: S,
        ) -> crate::Result<TlsStream<S>> {
            opentls_tls_stream::create_tls_stream(config, stream).await
        }
   } else if #[cfg(feature = "native-tls")] {
        mod native_tls_stream;

        pub(crate) use native_tls_stream::TlsStream;

        pub(crate) async fn create_tls_stream<S: AsyncRead + AsyncWrite + Unpin + Send>(
            config: &Config,
            stream: S,
        ) -> crate::Result<TlsStream<S>> {
            native_tls_stream::create_tls_stream(config, stream).await
        }
    }
}
