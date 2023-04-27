use crate::Config;
use futures_util::io::{AsyncRead, AsyncWrite};

#[cfg(feature = "native-tls")]
mod native_tls_stream;

#[cfg(feature = "rustls")]
mod rustls_tls_stream;

#[cfg(feature = "vendored-openssl")]
mod opentls_tls_stream;

#[cfg(feature = "native-tls")]
pub(crate) use native_tls_stream::TlsStream;

#[cfg(feature = "rustls")]
pub(crate) use rustls_tls_stream::TlsStream;

#[cfg(feature = "vendored-openssl")]
pub(crate) use opentls_tls_stream::TlsStream;

#[cfg(feature = "rustls")]
pub(crate) async fn create_tls_stream<S: AsyncRead + AsyncWrite + Unpin + Send>(
    config: &Config,
    stream: S,
) -> crate::Result<TlsStream<S>> {
    TlsStream::new(config, stream).await
}

#[cfg(feature = "native-tls")]
pub(crate) async fn create_tls_stream<S: AsyncRead + AsyncWrite + Unpin + Send>(
    config: &Config,
    stream: S,
) -> crate::Result<TlsStream<S>> {
    native_tls_stream::create_tls_stream(config, stream).await
}

#[cfg(feature = "vendored-openssl")]
pub(crate) async fn create_tls_stream<S: AsyncRead + AsyncWrite + Unpin + Send>(
    config: &Config,
    stream: S,
) -> crate::Result<TlsStream<S>> {
    opentls_tls_stream::create_tls_stream(config, stream).await
}

#[cfg(any(feature = "native-tls", feature = "vendored-openssl"))]
mod iter_certs {
    const BEGIN: &[u8] = b"-----BEGIN";
    pub struct IterCertBundle<'a> {
        bundle: &'a [u8],
        current_begin: Option<usize>,
    }
    impl<'a> IterCertBundle<'a> {
        pub fn new(bundle: &'a [u8]) -> Self {
            Self {
                bundle,
                current_begin: bundle.windows(BEGIN.len()).position(|x| x == BEGIN),
            }
        }
    }
    impl<'a> Iterator for IterCertBundle<'a> {
        type Item = &'a [u8];
        fn next(&mut self) -> Option<&'a [u8]> {
            self.current_begin.map(|begin| {
                let next_begin = self.bundle.windows(BEGIN.len()).skip(begin + 1).position(|x| x == BEGIN).map(|x| x + begin + 1);
                self.current_begin = next_begin;
                &self.bundle[begin..next_begin.unwrap_or(self.bundle.len() - 1)]
            })
        }
    }
}
