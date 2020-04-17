use crate::{
    client::AuthMethod,
    protocol::{
        codec::{self, Encode, LoginMessage, Packet, PacketHeader, PacketStatus, PreloginMessage},
        stream::TokenStream,
        Context, HEADER_BYTES,
    },
    tls::MaybeTlsStream,
    EncryptionLevel,
};
use bytes::BytesMut;
use codec::PacketCodec;
use futures::{SinkExt, Stream, TryStream};
use std::{
    cmp,
    pin::Pin,
    sync::{atomic::Ordering, Arc},
    task,
};
use task::Poll;
use tokio::future::poll_fn;
use tokio_util::codec::Framed;
use tracing::{event, Level};
#[cfg(windows)]
use winauth::windows::NtlmSspiBuilder;

pub type Transport = Framed<MaybeTlsStream, PacketCodec>;

pub struct Connection {
    transport: Transport,
    context: Arc<Context>,
}

impl Connection {
    pub(crate) fn new(transport: Transport, context: Arc<Context>) -> Self {
        Self { transport, context }
    }

    pub(crate) async fn send<E>(&mut self, mut header: PacketHeader, item: E) -> crate::Result<()>
    where
        E: Sized + Encode<BytesMut>,
    {
        let packet_size = self.context.packet_size.load(Ordering::SeqCst) as usize - HEADER_BYTES;

        let mut payload = BytesMut::new();
        item.encode(&mut payload)?;

        while !payload.is_empty() {
            let writable = cmp::min(payload.len(), packet_size);

            let split_payload = BytesMut::from(&payload[..writable]);
            payload = BytesMut::from(&payload[writable..]);

            if payload.is_empty() {
                header.set_status(PacketStatus::EndOfMessage);
            } else {
                header.set_status(PacketStatus::NormalMessage);
            }

            event!(
                Level::TRACE,
                "Encoding a packet from a buffer of size {}",
                split_payload.len() + HEADER_BYTES,
            );

            let packet = Packet::new(header, split_payload);
            self.transport.send(packet).await?;
        }

        // Rai rai says the fish goodbye
        SinkExt::<Packet>::flush(&mut self.transport).await?;

        Ok(())
    }

    pub(crate) async fn flush_packets(&mut self) -> crate::Result<()> {
        poll_fn(|cx| {
            while let Poll::Ready(Some(packet)) = Pin::new(&mut self.transport).try_poll_next(cx)? {
                event!(
                    Level::WARN,
                    "Flushing unhandled packet from the wire. Please consume your streams!",
                );

                let is_last = packet.is_last();

                if is_last {
                    break;
                }
            }

            Poll::Ready(Ok(()))
        })
        .await
    }

    pub(crate) fn token_stream(&mut self) -> TokenStream<Transport> {
        TokenStream::new(&mut self.transport, &self.context)
    }

    pub(crate) async fn prelogin(
        &mut self,
        ssl: EncryptionLevel,
    ) -> crate::Result<PreloginMessage> {
        let mut msg = PreloginMessage::new();
        msg.encryption = ssl;

        self.send(PacketHeader::pre_login(&self.context), msg)
            .await?;

        Ok(codec::collect_from(self).await?)
    }

    pub(crate) async fn login(
        &mut self,
        auth: AuthMethod,
        db: Option<String>,
    ) -> crate::Result<()> {
        let mut msg = LoginMessage::new();

        match auth {
            #[cfg(windows)]
            AuthMethod::WindowsIntegrated => {
                let builder = NtlmSspiBuilder::new();
                let sspi_client = builder.build()?;
                let buf = sspi_client.next_bytes(None)?;

                msg.integrated_security = buf;

                self.send(PacketHeader::login(&self.context), msg).await?;
                self.token_stream().flush_done().await?;

                let mut ts = self.token_stream();
                let sspi_bytes = ts.flush_sspi().await?;

                match sspi_client.next_bytes(Some(sspi_bytes))? {
                    Some(sspi_response) => {
                        event!(Level::TRACE, sspi_response_len = sspi_response.len());

                        let mut header = PacketHeader::login(&self.ctx);
                        header.set_length((sspi_response.len() + HEADER_BYTES) as u16);

                        let mut header_buf = [0u8; HEADER_BYTES];
                        header.serialize(&mut header_buf)?;

                        let mut buf = vec![];
                        buf.extend_from_slice(&header_buf);
                        buf.extend_from_slice(&sspi_response);

                        self.stream.write_all(&buf).await?;
                    }
                    None => unreachable!(),
                }
            }
            AuthMethod::None => panic!("No authentication method specified"), // TODO?
            AuthMethod::SqlServer { user, password } => {
                if let Some(db) = db {
                    msg.db_name = db.into();
                }

                msg.username = user.into();
                msg.password = password.into();

                self.send(PacketHeader::login(&self.context), msg).await?;
                self.token_stream().flush_done().await?;
            }
            x => panic!("Auth method not supported {:?}", x),
        }

        Ok(())
    }

    #[cfg(feature = "tls")]
    pub(crate) async fn tls_handshake(
        self,
        ssl: EncryptionLevel,
        trust_cert: bool,
    ) -> crate::Result<Self> {
        if ssl != EncryptionLevel::NotSupported {
            let mut builder = native_tls::TlsConnector::builder();

            if trust_cert {
                builder.danger_accept_invalid_certs(true);
                builder.danger_accept_invalid_hostnames(true);
                builder.use_sni(false);
            }

            let cx = builder.build().unwrap();
            let connector = tokio_tls::TlsConnector::from(cx);

            let stream = match self.transport.into_inner() {
                MaybeTlsStream::Raw(tcp) => connector
                    .connect("", tcp)
                    .await
                    .expect("TODO: handle error"),
                _ => unreachable!(),
            };

            let transport = Framed::new(MaybeTlsStream::Tls(stream), PacketCodec);

            Ok(Self {
                transport,
                context: self.context,
            })
        } else {
            Ok(self)
        }
    }

    #[cfg(not(feature = "tls"))]
    pub(crate) async fn tls_handshake(self, ssl: EncryptionLevel, _: bool) -> crate::Result<Self> {
        assert_eq!(ssl, EncryptionLevel::NotSupported);
        Ok(self)
    }
}

impl Stream for Connection {
    type Item = crate::Result<Packet>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.get_mut().transport).poll_next(cx)
    }
}
