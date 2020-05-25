#[cfg(feature = "tls")]
use crate::client::tls::TlsPreloginWrapper;
use crate::{
    client::{tls::MaybeTlsStream, AuthMethod, ClientBuilder},
    tds::{
        codec::{
            self, Encode, LoginMessage, Packet, PacketCodec, PacketHeader, PacketStatus,
            PreloginMessage, TokenDone,
        },
        stream::TokenStream,
        Context, HEADER_BYTES,
    },
    EncryptionLevel, SqlReadBytes,
};
use bytes::BytesMut;
#[cfg(windows)]
use codec::TokenSSPI;
use futures::{ready, SinkExt, Stream, TryStream, TryStreamExt};
use pretty_hex::*;
use std::time::Duration;
use std::{cmp, net::SocketAddr, fmt::Debug, io, pin::Pin, sync::atomic::Ordering, task};
use task::Poll;
use futures_codec::Framed;
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
pub(crate) struct Connection<S: futures::AsyncRead + futures::AsyncWrite + Unpin> {
    transport: Framed<MaybeTlsStream<S>, PacketCodec>,
    flushed: bool,
    context: Context,
    buf: BytesMut,
}

impl<S: futures::AsyncRead + futures::AsyncWrite + Unpin> Debug for Connection<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection")
            .field("transport", &"Framed<..>")
            .field("flushed", &self.flushed)
            .field("context", &self.context)
            .field("buf", &self.buf.as_ref().hex_dump())
            .finish()
    }
}

pub(crate) struct ConnectOpts {
    pub encryption: EncryptionLevel,
    pub trust_cert: bool,
    pub auth: AuthMethod,
    pub database: Option<String>,
    pub instance_name: Option<String>,
}

enum LoginResult {
    Ok,
    #[cfg(windows)]
    Windows(Box<dyn NextBytes>),
}

impl<S: futures::AsyncRead + futures::AsyncWrite + Unpin> Connection<S> {
    /// Creates a new connection
    pub (crate) async fn connect(
        opts: ClientBuilder,
        tcp_stream: S,
    ) -> crate::Result<Connection<S>>
    where
    {
        // need to do a create context in hre
        #[cfg(windows)]
        let context = {
            let mut context = Context::new();
            context.set_spn(opts.get_host(), opts.get_port());
            context
        };

        #[cfg(not(windows))]
        let context = {
            Context::new()
        };

        let transport = Framed::new(MaybeTlsStream::Raw(tcp_stream), PacketCodec);

        let mut connection = Self {
            transport,
            context,
            flushed: false,
            buf: BytesMut::new(),
        };

        let prelogin = connection.prelogin(opts.encryption).await?;
        let encryption = prelogin.negotiated_encryption(opts.encryption);

        let mut connection = connection
            .tls_handshake(encryption, opts.trust_cert)
            .await?;

        let login_result = connection.login(opts.auth, opts.database).await?;
        connection = connection.post_login_encryption(encryption);

        match login_result {
            LoginResult::Ok => (),
            #[cfg(windows)]
            LoginResult::Windows(client) => {
                connection.windows_postlogin(client).await?;
            }
        }

        connection.flush_done().await?;

        Ok(connection)
    }

    /// Flush the incoming token stream until receiving `DONE` token.
    async fn flush_done(&mut self) -> crate::Result<TokenDone> {
        TokenStream::new(self).flush_done().await
    }

    #[cfg(windows)]
    /// Flush the incoming token stream until receiving `SSPI` token.
    async fn flush_sspi(&mut self) -> crate::Result<TokenSSPI> {
        TokenStream::new(self).flush_sspi().await
    }

    #[cfg(feature = "tls")]
    fn post_login_encryption(mut self, encryption: EncryptionLevel) -> Self {
        if let EncryptionLevel::Off = encryption {
            event!(
                Level::WARN,
                "Turning TLS off after a login. All traffic from here on is not encrypted.",
            );

            let Self { transport, .. } = self;
            let tcp = transport.release().0.into_inner();
            self.transport = Framed::new(MaybeTlsStream::Raw(tcp), PacketCodec);
        }

        self
    }

    #[cfg(not(feature = "tls"))]
    fn post_login_encryption(self, _: EncryptionLevel) -> Self {
        self
    }

    /// Send an item to the wire. Header should define the item type and item should implement
    /// [`Encode`], defining the byte structure for the wire.
    ///
    /// The `send` will split the packet into multiple packets if bigger than
    /// the negotiated packet size, and handle flushing to the wire in an optimal way.
    ///
    /// [`Encode`]: ../protocol/codec/trait.Encode.html
    pub async fn send<E>(&mut self, mut header: PacketHeader, item: E) -> crate::Result<()>
    where
        E: Sized + Encode<BytesMut>,
    {
        self.flushed = false;
        let packet_size = self.context.packet_size() - HEADER_BYTES;

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

        // Rai rai says the turbofish goodbye
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
    pub async fn flush_stream(&mut self) -> crate::Result<()> {
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

    /// True if the underlying stream has no more data and is consumed
    /// completely.
    pub fn is_eof(&self) -> bool {
        self.flushed && self.buf.is_empty()
    }

    /// A message sent by the client to set up context for login. The server
    /// responds to a client PRELOGIN message with a message of packet header
    /// type 0x04 and with the packet data containing a PRELOGIN structure.
    ///
    /// This message stream is also used to wrap the TLS handshake payload if
    /// encryption is needed. In this scenario, where PRELOGIN message is
    /// transporting the TLS handshake payload, the packet data is simply the
    /// raw bytes of the TLS handshake payload.
    async fn prelogin(&mut self, encryption: EncryptionLevel) -> crate::Result<PreloginMessage> {
        let mut msg = PreloginMessage::new();
        msg.encryption = encryption;

        self.send(PacketHeader::pre_login(&self.context), msg)
            .await?;

        Ok(codec::collect_from(self).await?)
    }

    /// Defines the login record rules with SQL Server. Authentication with
    /// connection options.
    async fn login<'a>(
        &'a mut self,
        auth: AuthMethod,
        db: Option<String>,
    ) -> crate::Result<LoginResult> {
        let mut msg = LoginMessage::new();

        if let Some(db) = db {
            msg.db_name = db.into();
        }

        match auth {
            #[cfg(windows)]
            AuthMethod::WindowsIntegrated => {
                let mut client = NtlmSspiBuilder::new()
                    .target_spn(self.context.spn())
                    .build()?;

                msg.integrated_security = client.next_bytes(None)?;
                self.send(PacketHeader::login(&self.context), msg).await?;

                Ok(LoginResult::Windows(Box::new(client)))
            }
            #[cfg(windows)]
            AuthMethod::Windows(auth) => {
                let spn = self.context.spn().to_string();
                let builder = winauth::NtlmV2ClientBuilder::new().target_spn(spn);
                let mut client = builder.build(auth.domain, auth.user, auth.password);

                msg.integrated_security = client.next_bytes(None)?;
                self.send(PacketHeader::login(&self.context), msg).await?;

                Ok(LoginResult::Windows(Box::new(client)))
            }
            AuthMethod::None => {
                self.send(PacketHeader::login(&self.context), msg).await?;

                Ok(LoginResult::Ok)
            }
            AuthMethod::SqlServer(auth) => {
                msg.username = auth.user().into();
                msg.password = auth.password().into();

                self.send(PacketHeader::login(&self.context), msg).await?;

                Ok(LoginResult::Ok)
            }
        }
    }

    /// Implements the TLS handshake with the SQL Server.
    #[cfg(feature = "tls")]
    async fn tls_handshake(
        self,
        encryption: EncryptionLevel,
        trust_cert: bool,
    ) -> crate::Result<Self> {
        if encryption != EncryptionLevel::NotSupported {
            event!(Level::INFO, "Performing a TLS handshake");

            //let mut builder = native_tls::TlsConnector::builder();
            let mut builder = async_native_tls::TlsConnector::new();

            if trust_cert {
                event!(
                    Level::WARN,
                    "Trusting the server certificate without validation."
                );

                builder = builder.danger_accept_invalid_certs(true);
                builder = builder.danger_accept_invalid_hostnames(true);
                builder = builder.use_sni(false);
            }

            let Self { transport, context, .. } = self;
            let mut stream = match transport.release().0 {
                MaybeTlsStream::Raw(tcp) => {
                    builder.connect("", TlsPreloginWrapper::new(tcp)).await?
                }
                _ => unreachable!(),
            };

            stream.get_mut().handshake_complete();
            event!(Level::INFO, "TLS handshake successful");

            let transport = Framed::new(MaybeTlsStream::Tls(stream), PacketCodec);

            Ok(Self {
                transport,
                context,
                flushed: false,
                buf: BytesMut::new(),
            })
        } else {
            event!(
                Level::WARN,
                "TLS encryption is not enabled. All traffic including the login credentials are not encrypted."
            );

            Ok(self)
        }
    }

    /// Implements the TLS handshake with the SQL Server.
    #[cfg(not(feature = "tls"))]
    async fn tls_handshake(self, _: EncryptionLevel, _: bool) -> crate::Result<Self> {
        Ok(self)
    }

    /// Performs needed handshakes for Windows-based authentications.
    #[cfg(windows)]
    async fn windows_postlogin(&mut self, mut client: Box<dyn NextBytes>) -> crate::Result<()> {
        let sspi_bytes = self.flush_sspi().await?;

        match client.next_bytes(Some(sspi_bytes.as_ref()))? {
            Some(sspi_response) => {
                event!(Level::TRACE, sspi_response_len = sspi_response.len());

                let header = PacketHeader::login(&self.context);
                let token = TokenSSPI::new(sspi_response);
                self.send(header, token).await?;
            }
            None => unreachable!(),
        }

        Ok(())
    }

}


impl<S: futures::AsyncRead + futures::AsyncWrite + Unpin> Stream for Connection<S> {
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

impl<S: futures::AsyncRead + futures::AsyncWrite + Unpin> futures::AsyncRead for Connection<S> {
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

impl<S: futures::AsyncRead + futures::AsyncWrite + Unpin> SqlReadBytes for Connection<S> {
    /// Hex dump of the current buffer.
    fn debug_buffer(&self) {
        dbg!(self.buf.as_ref().hex_dump());
    }

    /// The current execution context.
    fn context(&self) -> &Context {
        &self.context
    }

    /// A mutable reference to the current execution context.
    fn context_mut(&mut self) -> &mut Context {
        &mut self.context
    }
}
