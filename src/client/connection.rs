use crate::{
    async_read_le_ext::AsyncReadLeExt,
    client::AuthMethod,
    protocol::{
        codec::{self, Encode, LoginMessage, Packet, PacketHeader, PacketStatus, PreloginMessage},
        stream::{ReceivedToken, TokenStream},
        Context, HEADER_BYTES,
    },
    tls::{MaybeTlsStream, TlsPreloginWrapper},
    EncryptionLevel,
};
use bytes::BytesMut;
use codec::PacketCodec;
#[cfg(windows)]
use codec::TokenSSPI;
use futures::{ready, SinkExt, Stream, TryStream, TryStreamExt};
use pretty_hex::*;
use std::{
    cmp, io,
    pin::Pin,
    sync::{atomic::Ordering, Arc},
    task,
};
use task::Poll;
use tokio::io::AsyncRead;
use tokio_util::codec::Framed;
use tracing::{event, Level};
#[cfg(windows)]
use winauth::{windows::NtlmSspiBuilder, NextBytes};

/// A `Connection` is an abstraction between the [`Client`] and the server. It
/// can be used as a `Stream` to fetch [`Packet`]s from and to `send` packets
/// splitting them to the negotiated limit automatically.
///
/// `Connection` is not meant to use directly, but as an abstraction layer for
/// the numerous `Stream`s for easy packet handling.
///
/// [`Client`]: struct.Encode.html
/// [`Packet`]: ../protocol/codec/struct.Packet.html
pub struct Connection {
    transport: Framed<MaybeTlsStream, PacketCodec>,
    flushed: bool,
    context: Arc<Context>,
    buf: BytesMut,
}

impl Connection {
    /// Creates a new connection.
    pub(crate) fn new(
        transport: Framed<MaybeTlsStream, PacketCodec>,
        context: Arc<Context>,
    ) -> Self {
        Self {
            transport,
            context,
            flushed: false,
            buf: BytesMut::new(),
        }
    }

    /// Send an item to the wire. Header should define the item type and item should implement
    /// [`Encode`], defining the byte structure for the wire.
    ///
    /// The `send` will split the packet into multiple packets if bigger than
    /// the negotiated packet size, and handle flushing to the wire in an optimal way.
    ///
    /// [`Encode`]: ../protocol/codec/trait.Encode.html
    pub(crate) async fn send<E>(&mut self, mut header: PacketHeader, item: E) -> crate::Result<()>
    where
        E: Sized + Encode<BytesMut>,
    {
        self.flushed = false;
        let packet_size = self.context.packet_size.load(Ordering::SeqCst) as usize - HEADER_BYTES;

        let mut payload = BytesMut::new();
        item.encode(&mut payload)?;

        while !payload.is_empty() {
            let writable = cmp::min(payload.len(), packet_size);
            let split_payload = payload.split_to(writable);

            if payload.is_empty() {
                header.set_status(PacketStatus::EndOfMessage);
            } else {
                header.set_status(PacketStatus::NormalMessage);
            }

            event!(
                Level::TRACE,
                "Sending a packet ({} bytes)",
                split_payload.len() + HEADER_BYTES,
            );

            let packet = Packet::new(header, split_payload);
            self.transport.send(packet).await?;
        }

        // Rai rai says the fish goodbye
        SinkExt::<Packet>::flush(&mut self.transport).await?;

        Ok(())
    }

    /// Cleans the packet stream from previous use. It is important to use the
    /// whole stream before using the connection again. Flushing the stream
    /// makes sure we don't have any old data causing undefined behaviour after
    /// previous queries.
    ///
    /// Calling this will slow down the queries if stream is still dirty if all
    /// results are not handled.
    pub(crate) async fn flush_stream(&mut self) -> crate::Result<()> {
        self.buf.truncate(0);

        if self.flushed {
            return Ok(());
        }

        while let Some(packet) = self.try_next().await? {
            event!(
                Level::WARN,
                "Flushing unhandled packet from the wire. Please consume your streams!",
            );

            let is_last = packet.is_last();

            if is_last {
                break;
            }
        }

        Ok(())
    }

    /// A message sent by the client to set up context for login. The server
    /// responds to a client PRELOGIN message with a message of packet header
    /// type 0x04 and with the packet data containing a PRELOGIN structure.
    ///
    /// This message stream is also used to wrap the SSL handshake payload if
    /// encryption is needed. In this scenario, where PRELOGIN message is
    /// transporting the SSL handshake payload, the packet data is simply the
    /// raw bytes of the SSL handshake payload.
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

    /// Defines the login record rules with SQL Server. Authentication with
    /// connection options.
    pub(crate) async fn login(
        &mut self,
        auth: AuthMethod,
        db: Option<String>,
    ) -> crate::Result<()> {
        let mut msg = LoginMessage::new();

        match auth {
            #[cfg(windows)]
            AuthMethod::WindowsIntegrated => {
                let sspi_client = NtlmSspiBuilder::new()
                    .target_spn(self.context.spn())
                    .build()?;
                self.windows_auth(msg, sspi_client).await?;
            }
            #[cfg(windows)]
            AuthMethod::Windows {
                user,
                password,
                domain,
            } => {
                let spn = self.context.spn().to_string();
                let builder = winauth::NtlmV2ClientBuilder::new().target_spn(spn);
                let client = builder.build(domain, user, password);

                self.windows_auth(msg, client).await?;
            }
            AuthMethod::None => panic!("No authentication method specified"), // TODO?
            AuthMethod::SqlServer { user, password } => {
                if let Some(db) = db {
                    msg.db_name = db.into();
                }

                msg.username = user.into();
                msg.password = password.into();

                self.send(PacketHeader::login(&self.context), msg).await?;

                let ts = TokenStream::new(self, self.context.clone());
                ts.flush_done().await?;
            }
        }

        Ok(())
    }

    /// Implements the TLS handshake with the SQL Server.
    #[cfg(feature = "tls")]
    pub(crate) async fn tls_handshake(
        self,
        ssl: EncryptionLevel,
        trust_cert: bool,
    ) -> crate::Result<Self> {
        if ssl != EncryptionLevel::NotSupported {
            event!(Level::INFO, "Performing a TLS handshake");

            let mut builder = native_tls::TlsConnector::builder();

            if trust_cert {
                builder.danger_accept_invalid_certs(true);
                builder.danger_accept_invalid_hostnames(true);
                builder.use_sni(false);
            }

            let cx = builder.build().unwrap();
            let connector = tokio_tls::TlsConnector::from(cx);

            let mut stream = match self.transport.into_inner() {
                MaybeTlsStream::Raw(tcp) => {
                    connector.connect("", TlsPreloginWrapper::new(tcp)).await?
                }
                _ => unreachable!(),
            };

            stream.get_mut().handshake_complete();
            event!(Level::INFO, "TLS handshake successful");

            let transport = Framed::new(MaybeTlsStream::Tls(stream), PacketCodec);

            Ok(Self {
                transport,
                context: self.context,
                flushed: false,
                buf: BytesMut::new(),
            })
        } else {
            Ok(self)
        }
    }

    #[cfg(windows)]
    /// Performs needed handshakes for Windows-based authentications.
    async fn windows_auth<'a>(
        &'a mut self,
        mut msg: LoginMessage<'a>,
        mut client: impl NextBytes,
    ) -> crate::Result<()> {
        msg.integrated_security = client.next_bytes(None)?;
        self.send(PacketHeader::login(&self.context), msg).await?;

        let ts = TokenStream::new(self, self.context.clone());
        let sspi_bytes = ts.flush_sspi().await?;

        match client.next_bytes(Some(sspi_bytes.as_ref()))? {
            Some(sspi_response) => {
                event!(Level::TRACE, sspi_response_len = sspi_response.len());

                let header = PacketHeader::login(&self.context);
                let token = TokenSSPI::new(sspi_response);
                self.send(header, token).await?;

                let ts = TokenStream::new(self, self.context.clone());
                ts.flush_done().await?;
            }
            None => unreachable!(),
        }

        Ok(())
    }

    /// Implements the TLS handshake with the SQL Server.
    #[cfg(not(feature = "tls"))]
    pub(crate) async fn tls_handshake(self, ssl: EncryptionLevel, _: bool) -> crate::Result<Self> {
        assert_eq!(ssl, EncryptionLevel::NotSupported);
        Ok(self)
    }

    pub(crate) fn token_stream<'a>(
        &'a mut self,
    ) -> Box<dyn Stream<Item = crate::Result<ReceivedToken>> + 'a> {
        TokenStream::new(self, self.context.clone()).try_unfold()
    }

    pub(crate) fn is_eof(&self) -> bool {
        self.flushed && self.buf.is_empty()
    }
}

impl Stream for Connection {
    type Item = crate::Result<Packet>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        match ready!(this.transport.try_poll_next_unpin(cx)) {
            Some(Ok(packet)) => {
                this.flushed = packet.is_last();
                Poll::Ready(Some(Ok(packet)))
            }
            Some(Err(e)) => Err(e)?,
            None => Poll::Ready(None),
        }
    }
}

impl AsyncRead for Connection {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let mut this = self.get_mut();
        let size = buf.len();

        if this.buf.len() < size {
            match ready!(Pin::new(&mut this).try_poll_next(cx)) {
                Some(Ok(packet)) => {
                    let (_, payload) = packet.into_parts();
                    this.buf.extend(payload);

                    if this.buf.len() < size {
                        cx.waker().wake_by_ref();
                        return Poll::Pending;
                    }
                }
                Some(Err(e)) => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        e.to_string(),
                    )))
                }
                None => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "No more packets in the wire",
                    )))
                }
            }
        }

        buf.copy_from_slice(this.buf.split_to(size).as_ref());
        Poll::Ready(Ok(size))
    }
}

impl AsyncReadLeExt for Connection {
    fn debug_buffer(&self) {
        dbg!(self.buf.as_ref().hex_dump());
    }
}
