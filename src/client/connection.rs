#[cfg(any(
    feature = "rustls",
    feature = "native-tls",
    feature = "vendored-openssl"
))]
use crate::client::{tls::TlsPreloginWrapper, tls_stream::create_tls_stream};
use crate::{
    client::{tls::MaybeTlsStream, AuthMethod, Config},
    tds::{
        codec::{
            self, Encode, FeatureLevel, LoginMessage, Packet, PacketCodec, PacketHeader,
            PacketStatus, PreloginMessage, TokenDone,
        },
        stream::TokenStream,
        Context, HEADER_BYTES,
    },
    EncryptionLevel, SqlReadBytes,
};
use asynchronous_codec::Framed;
use bytes::BytesMut;
#[cfg(any(windows, feature = "integrated-auth-gssapi"))]
use codec::TokenSspi;
use futures_util::io::{AsyncRead, AsyncWrite};
use futures_util::ready;
use futures_util::sink::SinkExt;
use futures_util::stream::{Stream, TryStream, TryStreamExt};
#[cfg(all(unix, feature = "integrated-auth-gssapi"))]
use libgssapi::{
    context::{ClientCtx, CtxFlags},
    credential::{Cred, CredUsage},
    name::Name,
    oid::{OidSet, GSS_MECH_KRB5, GSS_NT_KRB5_PRINCIPAL},
};
use pretty_hex::*;
#[cfg(all(unix, feature = "integrated-auth-gssapi"))]
use std::ops::Deref;
use std::{cmp, fmt::Debug, io, pin::Pin, task};
use task::Poll;
use tracing::{event, Level};
#[cfg(all(windows, feature = "winauth"))]
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
pub(crate) struct Connection<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    transport: Framed<MaybeTlsStream<S>, PacketCodec>,
    flushed: bool,
    context: Context,
    buf: BytesMut,
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send> Debug for Connection<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection")
            .field("transport", &"Framed<..>")
            .field("flushed", &self.flushed)
            .field("context", &self.context)
            .field("buf", &self.buf.as_ref().hex_dump())
            .finish()
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send> Connection<S> {
    /// Creates a new connection
    pub(crate) async fn connect(mut config: Config, tcp_stream: S) -> crate::Result<Connection<S>> {
        let context = {
            let mut context = Context::new();
            context.set_spn(config.get_host(), config.get_port());
            context
        };

        let transport = Framed::new(MaybeTlsStream::Raw(tcp_stream), PacketCodec);

        let mut connection = Self {
            transport,
            context,
            flushed: false,
            buf: BytesMut::new(),
        };

        // Auto-detect strict mode from hostname when encryption wasn't
        // explicitly configured by the caller.
        let resolved = config.resolve_encryption();
        if resolved == EncryptionLevel::Strict && !config.encryption_explicit {
            event!(
                Level::INFO,
                host = config.get_host(),
                "Auto-detected TDS 8 strict encryption from hostname"
            );
        }

        // TDS 8 strict mode: TLS handshake first, then PRELOGIN inside TLS.
        if config.encryption == EncryptionLevel::Strict {
            let connection = match connection.tls_handshake_strict(&config).await {
                Ok(c) => c,
                Err(e) => {
                    return Err(Self::wrap_strict_tls_error(e, config.get_host()));
                }
            };

            if config.strict_pipelined {
                // Backend reconnection after routing: pipeline PRELOGIN+LOGIN
                // without waiting for the PRELOGIN response (required by Fabric
                // backend servers).
                let mut connection =
                    Self::finish_connect_strict_pipelined(connection, config).await?;
                connection.flush_done().await?;
                return Ok(connection);
            }

            // Gateway (first connection): sequential PRELOGIN → LOGIN.
            // The gateway will respond with a routing token, which propagates
            // as Error::Routing for the caller to handle reconnection.
            let mut connection = Self::finish_connect_after_tls(connection, config).await?;
            connection.flush_done().await?;
            return Ok(connection);
        }

        let fed_auth_required = config.auth.is_aad();

        let prelogin = connection
            .prelogin(
                config.encryption,
                fed_auth_required,
                config.instance_name.clone(),
                false,
            )
            .await?;

        let encryption = prelogin.negotiated_encryption(config.encryption);

        let connection = connection.tls_handshake(&config, encryption).await?;

        let mut connection = connection
            .login(
                config.auth,
                encryption,
                config.database,
                config.host,
                config.application_name,
                config.readonly,
                prelogin,
            )
            .await?;

        connection.flush_done().await?;

        Ok(connection)
    }

    /// Complete connection setup after TLS is established (for strict mode).
    /// In TDS 8, PRELOGIN and LOGIN both happen inside the TLS tunnel.
    async fn finish_connect_after_tls(mut connection: Self, config: Config) -> crate::Result<Self> {
        // For backend connections (routing reconnect), we still need to send
        // FEDAUTHREQUIRED if using AAD auth — the backend needs it to prepare
        // for processing the FEDAUTH token in LOGIN.
        let is_backend = config.instance_name.is_some();
        let fed_auth_required = config.auth.is_aad();

        // In TDS 8 strict mode, send ENCRYPT_STRICT (0x08) on the wire.
        // TLS is already established, and the PRELOGIN encryption field signals
        // to the server that this is a TDS 8 strict mode connection.
        let prelogin_encryption = config.encryption;

        let prelogin = connection
            .prelogin(
                prelogin_encryption,
                fed_auth_required,
                config.instance_name.clone(),
                is_backend,
            )
            .await?;

        // Use login_server_name if set (for routed connections, this is the
        // original gateway hostname), otherwise fall back to host.
        let server_name = config.login_server_name.or(config.host);

        let connection = connection
            .login(
                config.auth,
                EncryptionLevel::Strict,
                config.database,
                server_name,
                config.application_name,
                config.readonly,
                prelogin,
            )
            .await?;

        Ok(connection)
    }

    /// Complete connection to a strict-mode backend after routing redirect.
    ///
    /// Fabric (and SQL Server 2022+ strict) backends require PRELOGIN and LOGIN
    /// to be sent back-to-back (pipelined) without waiting for the PRELOGIN
    /// response. This matches the behavior of the ODBC Driver 18.
    ///
    /// The flow is:
    /// 1. Encode PRELOGIN and LOGIN packets
    /// 2. Write all packets to the wire in a single flush
    /// 3. Read PRELOGIN response
    /// 4. Caller reads LOGIN response via flush_done()
    async fn finish_connect_strict_pipelined(
        mut connection: Self,
        config: Config,
    ) -> crate::Result<Self> {
        // 1. Build PRELOGIN — match ODBC Driver 18 format:
        //    6 options: VERSION, ENCRYPTION, INSTOPT, THREADID, MARS, TRACEID
        //    NO FEDAUTHREQUIRED (ODBC doesn't send it to backends)
        let mut prelogin_msg = PreloginMessage::new();
        prelogin_msg.encryption = EncryptionLevel::Strict;
        // Do NOT set fed_auth_required — ODBC omits it for backend PRELOGIN
        prelogin_msg.fed_auth_required = false;
        // Include TRACEID (36 bytes) — ODBC always sends this
        prelogin_msg.include_trace_id = true;
        // Include instance name from routing redirect in PRELOGIN to backend.
        // The backend uses this to identify which database instance to connect to.
        prelogin_msg.instance_name = config.instance_name.clone();

        // 2. Build LOGIN (using assumed fed_auth_required=true, nonce=None —
        //    standard for Fabric backends).
        // MS-TDS spec says TDS 8.0 (0x08000000) for strict mode.
        let mut login_message = LoginMessage::new();
        login_message.tds_version(FeatureLevel::SqlServer2022);
        // Azure SQL / Fabric backends require the AZURESQLSUPPORT feature
        // extension to indicate the client can handle Azure-specific tokens.
        login_message.azure_sql_support();

        // Keep the LOGIN minimal — match gateway LOGIN structure exactly.
        // No hostname, clt_int_name, client_pid, or client_prog_ver overrides.

        if let Some(db) = config.database {
            login_message.db_name(db);
        }

        // Use login_server_name if set (original gateway hostname for routed
        // connections), otherwise fall back to the connection host.
        let server_name = config.login_server_name.or(config.host);
        if let Some(sn) = server_name {
            login_message.server_name(sn);
        }

        if let Some(app_name) = config.application_name {
            login_message.app_name(app_name);
        }

        login_message.readonly(config.readonly);

        match config.auth {
            AuthMethod::AADToken(token) => {
                event!(
                    Level::INFO,
                    token_len = token.len(),
                    "Sending pipelined LOGIN with AAD token (fed_auth_required=true, nonce=None)"
                );
                login_message.aad_token(token, true, None);
            }
            AuthMethod::AADTokenProvider(provider) => {
                let token = provider.get_token().await.map_err(|e| {
                    crate::Error::Protocol(format!("Token provider failed: {}", e).into())
                })?;
                event!(
                    Level::INFO,
                    token_len = token.len(),
                    "Sending pipelined LOGIN with provider token (fed_auth_required=true, nonce=None)"
                );
                login_message.aad_token(token, true, None);
            }
            AuthMethod::SqlServer(auth) => {
                login_message.user_name(auth.user().to_string());
                login_message.password(auth.password().to_string());
            }
            AuthMethod::None => {}
            #[cfg(any(windows, feature = "integrated-auth-gssapi"))]
            _ => {
                return Err(crate::Error::Protocol(
                    "Integrated auth not supported for strict-mode pipelined backend connection"
                        .into(),
                ));
            }
        }

        // 3. Feed PRELOGIN packet(s) to the transport buffer (no flush yet)
        let packet_size = (connection.context.packet_size() as usize) - crate::tds::HEADER_BYTES;

        let mut prelogin_payload = BytesMut::new();
        prelogin_msg.encode(&mut prelogin_payload)?;

        let prelogin_id = connection.context.next_packet_id();
        let mut prelogin_header = PacketHeader::pre_login(prelogin_id);
        prelogin_header.set_status(PacketStatus::EndOfMessage);
        connection
            .feed_to_wire(prelogin_header, prelogin_payload)
            .await?;

        // 4. Feed LOGIN packet(s) to the transport buffer (no flush yet)
        let mut login_payload = BytesMut::new();
        login_message.encode(&mut login_payload)?;

        let login_id = connection.context.next_packet_id();
        let mut login_header = PacketHeader::login(login_id);

        let mut is_first_login_pkt = true;
        while !login_payload.is_empty() {
            let writable = cmp::min(login_payload.len(), packet_size);
            let split_payload = login_payload.split_to(writable);

            if login_payload.is_empty() {
                login_header.set_status(PacketStatus::EndOfMessage);
            } else {
                login_header.set_status(PacketStatus::NormalMessage);
            }

            // Per MS-TDS 2.2.3.1: PacketID increments by 1 within a message
            if !is_first_login_pkt {
                login_header.set_id(login_header.id().wrapping_add(1));
            }
            is_first_login_pkt = false;

            connection.feed_to_wire(login_header, split_payload).await?;
        }

        // 5. Single flush: send PRELOGIN+LOGIN together in one TLS write
        connection.flush_sink().await?;

        // 6. Read PRELOGIN response
        let _prelogin_response: PreloginMessage = codec::collect_from(&mut connection).await?;

        // The PRELOGIN response packet has EOM status which sets `flushed = true`.
        // Reset it because we still expect the LOGIN response tokens to follow.
        connection.flushed = false;

        // 7. The LOGIN response tokens will be read by flush_done() in the caller
        Ok(connection)
    }

    /// Flush the incoming token stream until receiving `DONE` token.
    async fn flush_done(&mut self) -> crate::Result<TokenDone> {
        TokenStream::new(self).flush_done().await
    }

    #[cfg(any(windows, feature = "integrated-auth-gssapi"))]
    /// Flush the incoming token stream until receiving `SSPI` token.
    async fn flush_sspi(&mut self) -> crate::Result<TokenSspi> {
        TokenStream::new(self).flush_sspi().await
    }

    #[cfg(any(
        feature = "rustls",
        feature = "native-tls",
        feature = "vendored-openssl"
    ))]
    fn post_login_encryption(mut self, encryption: EncryptionLevel) -> Self {
        if let EncryptionLevel::Off = encryption {
            event!(
                Level::WARN,
                "Turning TLS off after a login. All traffic from here on is not encrypted.",
            );

            let Self { transport, .. } = self;
            let tcp = transport.into_inner().into_inner();
            self.transport = Framed::new(MaybeTlsStream::Raw(tcp), PacketCodec);
        }

        self
    }

    #[cfg(not(any(
        feature = "rustls",
        feature = "native-tls",
        feature = "vendored-openssl"
    )))]
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
        let packet_size = (self.context.packet_size() as usize) - HEADER_BYTES;

        let mut payload = BytesMut::new();
        item.encode(&mut payload)?;

        let mut is_first = true;

        while !payload.is_empty() {
            let writable = cmp::min(payload.len(), packet_size);
            let split_payload = payload.split_to(writable);

            if payload.is_empty() {
                header.set_status(PacketStatus::EndOfMessage);
            } else {
                header.set_status(PacketStatus::NormalMessage);
            }

            // Per MS-TDS 2.2.3.1: PacketID is incremented by 1 (mod 256)
            // for each packet within a message. The first packet uses the
            // ID from the header as-is; subsequent packets increment.
            if !is_first {
                header.set_id(header.id().wrapping_add(1));
            }
            is_first = false;

            event!(
                Level::TRACE,
                "Sending a packet ({} bytes, id={})",
                split_payload.len() + HEADER_BYTES,
                header.id(),
            );

            // Send each packet individually (feed + flush). In TDS 8 strict
            // mode, each TDS packet must be written as a separate TLS record.
            // Using send() (vs feed + batch flush) ensures this.
            let packet = Packet::new(header, split_payload);
            self.transport.send(packet).await?;
        }

        Ok(())
    }

    /// Sends a packet of data to the database.
    ///
    /// # Warning
    ///
    /// Please be sure the packet size doesn't exceed the largest allowed size
    /// dictaded by the server.
    pub(crate) async fn write_to_wire(
        &mut self,
        header: PacketHeader,
        data: BytesMut,
    ) -> crate::Result<()> {
        self.flushed = false;

        let packet = Packet::new(header, data);
        self.transport.send(packet).await?;

        Ok(())
    }

    /// Feeds a packet to the transport buffer WITHOUT flushing.
    /// Use `flush_sink()` after feeding all packets to send them in one batch.
    async fn feed_to_wire(&mut self, header: PacketHeader, data: BytesMut) -> crate::Result<()> {
        self.flushed = false;

        let packet = Packet::new(header, data);
        self.transport.feed(packet).await?;

        Ok(())
    }

    /// Sends all pending packages to the wire.
    pub(crate) async fn flush_sink(&mut self) -> crate::Result<()> {
        self.transport.flush().await
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
    async fn prelogin(
        &mut self,
        encryption: EncryptionLevel,
        fed_auth_required: bool,
        instance_name: Option<String>,
        include_trace_id: bool,
    ) -> crate::Result<PreloginMessage> {
        let mut msg = PreloginMessage::new();
        msg.encryption = encryption;
        msg.fed_auth_required = fed_auth_required;
        msg.instance_name = instance_name.clone();
        msg.include_trace_id = include_trace_id;

        let id = self.context.next_packet_id();
        self.send(PacketHeader::pre_login(id), msg).await?;

        let response: PreloginMessage = codec::collect_from(self).await?;
        // threadid (should be empty when sent from server to client)
        debug_assert_eq!(response.thread_id, 0);
        event!(
            Level::INFO,
            version = response.version,
            sub_build = response.sub_build,
            encryption = ?response.encryption,
            fed_auth_required = response.fed_auth_required,
            has_nonce = response.nonce.is_some(),
            "PRELOGIN response received"
        );
        Ok(response)
    }

    /// Defines the login record rules with SQL Server. Authentication with
    /// connection options.
    #[allow(clippy::too_many_arguments)]
    async fn login(
        mut self,
        auth: AuthMethod,
        encryption: EncryptionLevel,
        db: Option<String>,
        server_name: Option<String>,
        application_name: Option<String>,
        readonly: bool,
        prelogin: PreloginMessage,
    ) -> crate::Result<Self> {
        let mut login_message = LoginMessage::new();

        // TDS 8 strict mode: the transport uses TLS-first, but the LOGIN7
        // protocol version remains TDS 7.4 (SqlServerN = 0x74000004). The TDS 8
        // "version" is a transport-mode indicator, not a protocol version.
        // Azure SQL gateways may not recognize 0x08000000 and misparse the LOGIN.
        if encryption == EncryptionLevel::Strict {
            login_message.tds_version(FeatureLevel::SqlServerN);
            // Set client interface name — ODBC sends "ODBC"; some backends
            // may require a non-empty value.
            login_message.clt_int_name("tiberius");
            // Azure SQL / Fabric backends require the AZURESQLSUPPORT feature
            // extension to indicate the client can handle Azure-specific tokens.
            login_message.azure_sql_support();
        }

        if let Some(db) = db {
            login_message.db_name(db);
        }

        if let Some(server_name) = server_name {
            login_message.server_name(server_name);
        }

        if let Some(app_name) = application_name {
            login_message.app_name(app_name);
        }

        login_message.readonly(readonly);

        match auth {
            #[cfg(all(windows, feature = "winauth"))]
            AuthMethod::Integrated => {
                let mut client = NtlmSspiBuilder::new()
                    .target_spn(self.context.spn())
                    .build()?;

                login_message.integrated_security(client.next_bytes(None)?);

                let id = self.context.next_packet_id();
                self.send(PacketHeader::login(id), login_message).await?;

                self = self.post_login_encryption(encryption);

                let sspi_bytes = self.flush_sspi().await?;

                match client.next_bytes(Some(sspi_bytes.as_ref()))? {
                    Some(sspi_response) => {
                        event!(Level::TRACE, sspi_response_len = sspi_response.len());

                        let id = self.context.next_packet_id();
                        let header = PacketHeader::login(id);

                        let token = TokenSspi::new(sspi_response);
                        self.send(header, token).await?;
                    }
                    None => unreachable!(),
                }
            }
            #[cfg(all(unix, feature = "integrated-auth-gssapi"))]
            AuthMethod::Integrated => {
                let mut s = OidSet::new()?;
                s.add(&GSS_MECH_KRB5)?;

                let client_cred = Cred::acquire(None, None, CredUsage::Initiate, Some(&s))?;

                let mut ctx = ClientCtx::new(
                    Some(client_cred),
                    Name::new(self.context.spn().as_bytes(), Some(&GSS_NT_KRB5_PRINCIPAL))?,
                    CtxFlags::GSS_C_MUTUAL_FLAG | CtxFlags::GSS_C_SEQUENCE_FLAG,
                    None,
                );

                let init_token = ctx.step(None, None)?;

                login_message.integrated_security(Some(Vec::from(init_token.unwrap().deref())));

                let id = self.context.next_packet_id();
                self.send(PacketHeader::login(id), login_message).await?;

                self = self.post_login_encryption(encryption);

                let auth_bytes = self.flush_sspi().await?;

                let next_token = match ctx.step(Some(auth_bytes.as_ref()), None)? {
                    Some(response) => {
                        event!(Level::TRACE, response_len = response.len());
                        TokenSspi::new(Vec::from(response.deref()))
                    }
                    None => {
                        event!(Level::TRACE, response_len = 0);
                        TokenSspi::new(Vec::new())
                    }
                };

                let id = self.context.next_packet_id();
                let header = PacketHeader::login(id);

                self.send(header, next_token).await?;
            }
            #[cfg(all(windows, feature = "winauth"))]
            AuthMethod::Windows(auth) => {
                let spn = self.context.spn().to_string();
                let builder = winauth::NtlmV2ClientBuilder::new().target_spn(spn);
                let mut client = builder.build(auth.domain, auth.user, auth.password);

                login_message.integrated_security(client.next_bytes(None)?);

                let id = self.context.next_packet_id();
                self.send(PacketHeader::login(id), login_message).await?;

                self = self.post_login_encryption(encryption);

                let sspi_bytes = self.flush_sspi().await?;

                match client.next_bytes(Some(sspi_bytes.as_ref()))? {
                    Some(sspi_response) => {
                        event!(Level::TRACE, sspi_response_len = sspi_response.len());

                        let id = self.context.next_packet_id();
                        let header = PacketHeader::login(id);

                        let token = TokenSspi::new(sspi_response);
                        self.send(header, token).await?;
                    }
                    None => unreachable!(),
                }
            }
            AuthMethod::None => {
                let id = self.context.next_packet_id();
                self.send(PacketHeader::login(id), login_message).await?;
                self = self.post_login_encryption(encryption);
            }
            AuthMethod::SqlServer(auth) => {
                login_message.user_name(auth.user());
                login_message.password(auth.password());

                let id = self.context.next_packet_id();
                self.send(PacketHeader::login(id), login_message).await?;
                self = self.post_login_encryption(encryption);
            }
            AuthMethod::AADToken(token) => {
                event!(
                    Level::INFO,
                    fed_auth_echo = prelogin.fed_auth_required,
                    has_nonce = prelogin.nonce.is_some(),
                    token_len = token.len(),
                    "Sending LOGIN with AAD token"
                );
                login_message.aad_token(token, prelogin.fed_auth_required, prelogin.nonce);

                let id = self.context.next_packet_id();
                self.send(PacketHeader::login(id), login_message).await?;
                self = self.post_login_encryption(encryption);
            }
            AuthMethod::AADTokenProvider(provider) => {
                let token = provider.get_token().await.map_err(|e| {
                    crate::Error::Protocol(format!("Token provider failed: {}", e).into())
                })?;
                event!(
                    Level::INFO,
                    fed_auth_echo = prelogin.fed_auth_required,
                    has_nonce = prelogin.nonce.is_some(),
                    token_len = token.len(),
                    "Sending LOGIN with provider token"
                );
                login_message.aad_token(token, prelogin.fed_auth_required, prelogin.nonce);

                let id = self.context.next_packet_id();
                self.send(PacketHeader::login(id), login_message).await?;
                self = self.post_login_encryption(encryption);
            }
        }

        Ok(self)
    }

    /// Implements the TLS handshake with the SQL Server.
    #[cfg(any(
        feature = "rustls",
        feature = "native-tls",
        feature = "vendored-openssl"
    ))]
    async fn tls_handshake(
        self,
        config: &Config,
        encryption: EncryptionLevel,
    ) -> crate::Result<Self> {
        if encryption != EncryptionLevel::NotSupported {
            event!(Level::INFO, "Performing a TLS handshake");

            let Self {
                transport, context, ..
            } = self;
            let mut stream = match transport.into_inner() {
                MaybeTlsStream::Raw(tcp) => {
                    create_tls_stream(config, TlsPreloginWrapper::new(tcp)).await?
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

    /// Implements TDS 8 strict TLS handshake: TLS is established directly on
    /// the raw TCP stream without any TDS packet wrapping. This is required
    /// for Microsoft Fabric and SQL Server 2022+ strict mode.
    #[cfg(any(
        feature = "rustls",
        feature = "native-tls",
        feature = "vendored-openssl"
    ))]
    async fn tls_handshake_strict(self, config: &Config) -> crate::Result<Self> {
        event!(
            Level::INFO,
            "Performing a TDS 8 strict TLS handshake (TLS-first)"
        );

        let Self {
            transport, context, ..
        } = self;

        let stream = match transport.into_inner() {
            MaybeTlsStream::Raw(tcp) => {
                // In strict mode, create the wrapper but immediately mark handshake
                // as complete so it acts as a transparent passthrough. This means
                // the TLS handshake goes directly over the TCP stream without any
                // TDS packet wrapping - exactly what TDS 8 requires.
                let mut wrapper = TlsPreloginWrapper::new(tcp);
                wrapper.handshake_complete();
                create_tls_stream(config, wrapper).await?
            }
            _ => unreachable!(),
        };

        event!(Level::INFO, "TDS 8 strict TLS handshake successful");

        let transport = Framed::new(MaybeTlsStream::Tls(stream), PacketCodec);

        Ok(Self {
            transport,
            context,
            flushed: false,
            buf: BytesMut::new(),
        })
    }

    /// Implements TDS 8 strict TLS handshake (no-op when TLS features are disabled).
    #[cfg(not(any(
        feature = "rustls",
        feature = "native-tls",
        feature = "vendored-openssl"
    )))]
    async fn tls_handshake_strict(self, _: &Config) -> crate::Result<Self> {
        Err(crate::Error::Protocol(
            "TDS 8 strict encryption requires a TLS feature (rustls, native-tls, or vendored-openssl) to be enabled".into()
        ))
    }

    /// Implements the TLS handshake with the SQL Server.
    #[cfg(not(any(
        feature = "rustls",
        feature = "native-tls",
        feature = "vendored-openssl"
    )))]
    async fn tls_handshake(self, _: &Config, _: EncryptionLevel) -> crate::Result<Self> {
        event!(
            Level::WARN,
            "TLS encryption is not enabled. All traffic including the login credentials are not encrypted."
        );

        Ok(self)
    }

    /// Wraps a TLS handshake error with context about strict mode requirements.
    ///
    /// When TDS 8 strict TLS fails, the raw error (connection reset, handshake
    /// failure) is often confusing. This adds actionable guidance.
    fn wrap_strict_tls_error(err: crate::Error, host: &str) -> crate::Error {
        match &err {
            crate::Error::Tls(msg) => crate::Error::Tls(format!(
                "TDS 8 strict TLS handshake with '{}' failed: {}. \
                 The server may not support TDS 8 strict mode \
                 (requires SQL Server 2025+ with forcestrict=1, Azure SQL, or Microsoft Fabric). \
                 For servers that don't support strict mode, use encrypt=true instead of encrypt=strict.",
                host, msg
            )),
            crate::Error::Io { kind, message } => crate::Error::Io {
                kind: *kind,
                message: format!(
                    "TDS 8 strict TLS handshake with '{}' failed: {}. \
                     The server may not support TDS 8 strict mode \
                     (requires SQL Server 2025+ with forcestrict=1, Azure SQL, or Microsoft Fabric). \
                     For servers that don't support strict mode, use encrypt=true instead of encrypt=strict.",
                    host, message
                ),
            },
            _ => err,
        }
    }

    pub(crate) async fn close(mut self) -> crate::Result<()> {
        self.transport.close().await
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send> Stream for Connection<S> {
    type Item = crate::Result<Packet>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        match this.transport.try_poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(packet))) => {
                this.flushed = packet.is_last();
                Poll::Ready(Some(Ok(packet)))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send> futures_util::io::AsyncRead for Connection<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let mut this = self.get_mut();
        let size = buf.len();

        if this.buf.len() < size {
            while let Some(item) = ready!(Pin::new(&mut this).try_poll_next(cx)) {
                match item {
                    Ok(packet) => {
                        let (_, payload) = packet.into_parts();
                        this.buf.extend(payload);

                        if this.buf.len() >= size {
                            break;
                        }
                    }
                    Err(e) => {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::BrokenPipe,
                            e.to_string(),
                        )))
                    }
                }
            }

            // Got EOF before having all the data.
            if this.buf.len() < size {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "No more packets in the wire",
                )));
            }
        }

        buf.copy_from_slice(this.buf.split_to(size).as_ref());
        Poll::Ready(Ok(size))
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send> SqlReadBytes for Connection<S> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::ErrorKind;

    #[test]
    fn wrap_strict_tls_error_wraps_tls_error() {
        let original = crate::Error::Tls("handshake failure".to_string());
        let wrapped =
            Connection::<futures_util::io::Cursor<Vec<u8>>>::wrap_strict_tls_error(original, "myserver.example.com");

        let msg = wrapped.to_string();
        assert!(msg.contains("myserver.example.com"), "should contain host");
        assert!(msg.contains("handshake failure"), "should contain original error");
        assert!(msg.contains("strict"), "should mention strict mode");
        assert!(
            msg.contains("encrypt=true"),
            "should suggest alternative: {}",
            msg
        );
        assert!(
            msg.contains("SQL Server 2025"),
            "should mention version requirement: {}",
            msg
        );
    }

    #[test]
    fn wrap_strict_tls_error_wraps_io_error() {
        let original = crate::Error::Io {
            kind: ErrorKind::ConnectionReset,
            message: "connection reset by peer".to_string(),
        };
        let wrapped =
            Connection::<futures_util::io::Cursor<Vec<u8>>>::wrap_strict_tls_error(original, "10.0.0.1");

        let msg = wrapped.to_string();
        assert!(msg.contains("10.0.0.1"), "should contain host");
        assert!(
            msg.contains("connection reset by peer"),
            "should contain original: {}",
            msg
        );
        assert!(msg.contains("strict"), "should mention strict mode");
    }

    #[test]
    fn wrap_strict_tls_error_passes_through_other_errors() {
        let original = crate::Error::Protocol("something else".into());
        let wrapped =
            Connection::<futures_util::io::Cursor<Vec<u8>>>::wrap_strict_tls_error(original.clone(), "host");

        // Non-TLS/IO errors should pass through unchanged
        assert_eq!(wrapped, original);
    }
}
