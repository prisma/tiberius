use crate::client::config::Config;
use async_std::task::{Context, Poll};
use futures::{AsyncRead, AsyncWrite};
use hyper_rustls::ConfigBuilderExt;
use rustls::{
    client::{ServerCertVerified, ServerCertVerifier},
    Certificate, Error, ServerName,
};
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::time::SystemTime;
use tokio_rustls::rustls::ClientConfig;
use tokio_rustls::TlsConnector;
use std::convert::TryInto;
use tracing::{event, Level};
use tokio_util::compat::{TokioAsyncReadCompatExt, Compat};
use tokio_util::compat::FuturesAsyncReadCompatExt;

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
    ) -> Result<ServerCertVerified, Error> {
        Ok(ServerCertVerified::assertion())
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send> TlsStream<S> {
    pub(super) async fn new(config: &Config, trust_cert: bool, stream: S) -> io::Result<Self> {
        event!(Level::INFO, "Performing a TLS handshake");

        let mut client_config = ClientConfig::builder()
            .with_safe_defaults()
            .with_native_roots()
            .with_no_client_auth();
        if trust_cert {
            event!(
                Level::WARN,
                "Trusting the server certificate without validation."
            );

            client_config
                .dangerous()
                .set_certificate_verifier(Arc::new(NoCertVerifier {}));
        }
        let builder = TlsConnector::try_from(Arc::new(client_config)).unwrap();

        let tls_stream = builder
            .connect(
                config.get_host().try_into().unwrap(),
                stream.compat(),
            )
            .await?;

        Ok(TlsStream(tls_stream.compat()))
    }

    pub(super) fn get_mut(&mut self) -> &mut S {
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
