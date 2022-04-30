use crate::Config;
use futures::{AsyncRead, AsyncWrite};

#[cfg(not(feature = "use_rustls"))]
pub(crate) use native_tls_stream::TlsStream;
#[cfg(feature = "use_rustls")]
pub(crate) use rustls_tls_stream::TlsStream;

#[cfg(feature = "use_rustls")]
pub(crate) async fn create_tls_stream<S: AsyncRead + AsyncWrite + Unpin + Send>(
    config: &Config,
    stream: S,
) -> crate::Result<TlsStream<S>> {
    TlsStream::new(config, stream).await
}
#[cfg(not(feature = "use_rustls"))]
pub(crate) async fn create_tls_stream<S: AsyncRead + AsyncWrite + Unpin + Send>(
    config: &Config,
    stream: S,
) -> crate::Result<TlsStream<S>> {
    native_tls_stream::create_tls_stream(config, stream).await
}

#[cfg(not(feature = "use_rustls"))]
mod native_tls_stream {
    use crate::{
        client::{config::Config, TrustConfig},
        error::{Error, IoErrorKind},
    };
    #[cfg(all(not(target_os = "macos"), not(target_os = "ios")))]
    pub(crate) use async_native_tls::TlsStream;
    #[cfg(not(any(target_os = "macos", target_os = "ios")))]
    use async_native_tls::{Certificate, TlsConnector};
    use futures::{AsyncRead, AsyncWrite};
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    pub(crate) use opentls::async_io::TlsStream;
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    use opentls::{async_io::TlsConnector, Certificate};
    use std::fs;
    use tracing::{event, Level};

    pub(crate) async fn create_tls_stream<S: AsyncRead + AsyncWrite + Unpin + Send>(
        config: &Config,
        stream: S,
    ) -> crate::Result<TlsStream<S>> {
        let mut builder = TlsConnector::new();

        match &config.trust {
            TrustConfig::CaCertificateLocation(path) => {
                if let Ok(buf) = fs::read(path) {
                    let cert = match path.extension() {
                        Some(ext)
                        if ext.to_ascii_lowercase() == "pem"
                            || ext.to_ascii_lowercase() == "crt" =>
                            {
                                Some(Certificate::from_pem(&buf)?)
                            }
                        Some(ext) if ext.to_ascii_lowercase() == "der" => {
                            Some(Certificate::from_der(&buf)?)
                        }
                        Some(_) | None => return Err(Error::Io {
                            kind: IoErrorKind::InvalidInput,
                            message: "Provided CA certificate with unsupported file-extension! Supported types are pem, crt and der.".to_string()}),
                    };
                    if let Some(c) = cert {
                        builder = builder.add_root_certificate(c);
                    }
                } else {
                    return Err(Error::Io {
                        kind: IoErrorKind::InvalidData,
                        message: "Could not read provided CA certificate!".to_string(),
                    });
                }
            }
            TrustConfig::TrustAll => {
                event!(
                    Level::WARN,
                    "Trusting the server certificate without validation."
                );

                builder = builder.danger_accept_invalid_certs(true);
                builder = builder.danger_accept_invalid_hostnames(true);
                builder = builder.use_sni(false);
            }
            TrustConfig::Default => {
                event!(Level::INFO, "Using default trust configuration.");
            }
        }

        Ok(builder.connect(config.get_host(), stream).await?)
    }
}

#[cfg(feature = "use_rustls")]
mod rustls_tls_stream {
    use crate::{
        client::{config::Config, TrustConfig},
        error::IoErrorKind,
        Error,
    };
    use futures::{AsyncRead, AsyncWrite};
    use hyper_rustls::ConfigBuilderExt;
    use rustls::client::HandshakeSignatureValid;
    use rustls::internal::msgs::handshake::DigitallySignedStruct;
    use rustls::{
        client::{ServerCertVerified, ServerCertVerifier},
        Certificate, Error as RustlsError, RootCertStore, ServerName,
    };
    use std::{fs, io};
    use std::{
        pin::Pin,
        sync::Arc,
        task::{Context, Poll},
        time::SystemTime,
    };
    use tokio_rustls::{rustls::ClientConfig, TlsConnector};
    use tokio_util::compat::{Compat, FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};
    use tracing::{event, Level};

    impl From<tokio_rustls::webpki::Error> for Error {
        fn from(e: tokio_rustls::webpki::Error) -> Self {
            crate::Error::Tls(e.to_string())
        }
    }

    pub(crate) struct TlsStream<S: AsyncRead + AsyncWrite + Unpin + Send>(
        Compat<tokio_rustls::client::TlsStream<Compat<S>>>,
    );

    struct NoCertVerifier;

    impl ServerCertVerifier for NoCertVerifier {
        fn verify_server_cert(
            &self,
            _end_entity: &Certificate,
            _intermediates: &[Certificate],
            _server_name: &ServerName,
            _scts: &mut dyn Iterator<Item = &[u8]>,
            _ocsp_response: &[u8],
            _now: SystemTime,
        ) -> Result<ServerCertVerified, RustlsError> {
            Ok(ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            _message: &[u8],
            _cert: &Certificate,
            _dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, RustlsError> {
            Ok(HandshakeSignatureValid::assertion())
        }
    }

    fn get_server_name(config: &Config) -> crate::Result<ServerName> {
        match (ServerName::try_from(config.get_host()), &config.trust) {
            (Ok(sn), _) => Ok(sn),
            (Err(_), TrustConfig::TrustAll) => {
                Ok(ServerName::try_from("placeholder.domain.com").unwrap())
            }
            (Err(e), _) => Err(crate::Error::Tls(e.to_string())),
        }
    }

    impl<S: AsyncRead + AsyncWrite + Unpin + Send> TlsStream<S> {
        pub(super) async fn new(config: &Config, stream: S) -> crate::Result<Self> {
            event!(Level::INFO, "Performing a TLS handshake");

            let builder = ClientConfig::builder().with_safe_defaults();

            let client_config = match &config.trust {
                TrustConfig::CaCertificateLocation(path) => {
                    if let Ok(buf) = fs::read(path) {
                        let cert = match path.extension() {
                            Some(ext)
                            if ext.to_ascii_lowercase() == "pem"
                                || ext.to_ascii_lowercase() == "crt" =>
                                {
                                    let pem_cert = rustls_pemfile::certs(&mut buf.as_slice())?;
                                    if pem_cert.len() != 1 {
                                        return Err(crate::Error::Io {
                                            kind: IoErrorKind::InvalidInput,
                                            message: format!("Certificate file {} contain 0 or more than 1 certs", path.to_string_lossy()),
                                        });
                                    }

                                    Certificate(pem_cert.into_iter().next().unwrap())
                                }
                            Some(ext) if ext.to_ascii_lowercase() == "der" => {
                                Certificate(buf)
                            }
                            Some(_) | None => return Err(crate::Error::Io {
                                kind: IoErrorKind::InvalidInput,
                                message: "Provided CA certificate with unsupported file-extension! Supported types are pem, crt and der.".to_string(),
                            }),
                        };
                        let mut cert_store = RootCertStore::empty();
                        cert_store.add(&cert)?;
                        builder
                            .with_root_certificates(cert_store)
                            .with_no_client_auth()
                    } else {
                        return Err(Error::Io {
                            kind: IoErrorKind::InvalidData,
                            message: "Could not read provided CA certificate!".to_string(),
                        });
                    }
                }
                TrustConfig::TrustAll => {
                    event!(
                        Level::WARN,
                        "Trusting the server certificate without validation."
                    );
                    let mut config = builder.with_native_roots().with_no_client_auth();
                    config
                        .dangerous()
                        .set_certificate_verifier(Arc::new(NoCertVerifier {}));
                    // config.enable_sni = false;
                    config
                }
                TrustConfig::Default => {
                    event!(Level::INFO, "Using default trust configuration.");
                    builder.with_native_roots().with_no_client_auth()
                }
            };

            let connector = TlsConnector::from(Arc::new(client_config));

            let tls_stream = connector
                .connect(get_server_name(config)?, stream.compat())
                .await?;

            Ok(TlsStream(tls_stream.compat()))
        }

        pub(crate) fn get_mut(&mut self) -> &mut S {
            self.0.get_mut().get_mut().0.get_mut()
        }
    }

    impl<S: AsyncRead + AsyncWrite + Unpin + Send> AsyncRead for TlsStream<S> {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut [u8],
        ) -> Poll<io::Result<usize>> {
            let inner = Pin::get_mut(self);
            Pin::new(&mut inner.0).poll_read(cx, buf)
        }
    }

    impl<S: AsyncRead + AsyncWrite + Unpin + Send> AsyncWrite for TlsStream<S> {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            let inner = Pin::get_mut(self);
            Pin::new(&mut inner.0).poll_write(cx, buf)
        }

        fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            let inner = Pin::get_mut(self);
            Pin::new(&mut inner.0).poll_flush(cx)
        }

        fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            let inner = Pin::get_mut(self);
            Pin::new(&mut inner.0).poll_close(cx)
        }
    }
}
