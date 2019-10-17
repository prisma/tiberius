use std::convert::TryInto;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures_util::future::{self, FutureExt};
use tokio::future::FutureExt as TokioFutureExt;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpStream, UdpSocket};
use tokio::sync::{self, mpsc};
use tracing::{self, debug_span, event, trace_span, Level};
use winauth::NextBytes;

use crate::protocol::{self, EncryptionLevel};
use crate::{Client, Connection, Error, Result, TlsNegotiateWrapper};

/// Settings for the connection, everything that isn't IO/transport specific (e.g. authentication)
pub struct ConnectParams {
    pub host: String,
    pub ssl: EncryptionLevel,
    pub trust_cert: bool,
    // pub auth: AuthMethod,
    // pub target_db: Option<Cow<'static, str>>,
    // pub spn: Cow<'static, str>,
}

pub async fn connect_tcp_sql_browser<P>(
    params: P,
    mut addr: SocketAddr,
    instance_name: &str,
) -> Result<Client>
where
    P: TryInto<ConnectParams>,
    P::Error: Into<Error>,
{
    // First resolve the instance to a port via the
    // SSRP protocol/MS-SQLR protocol [1]
    // [1] https://msdn.microsoft.com/en-us/library/cc219703.aspx

    let local_bind: SocketAddr = if addr.is_ipv4() {
        "0.0.0.0:0".parse().unwrap()
    } else {
        "[::]:0".parse().unwrap()
    };
    let msg = [&[4u8], instance_name.as_bytes()].concat();
    let mut socket = UdpSocket::bind(&local_bind).await?;
    socket.send_to(&msg, &addr).await?;
    let mut buf = vec![0u8; 4096];
    let timeout = Duration::from_millis(1000); // TODO: make configurable
    let len = socket.recv(&mut buf).timeout(timeout).await.map_err(
        |_elapsed: tokio::timer::timeout::Elapsed| {
            Error::Conversion("SQL browser timeout during resolving instance".into())
        },
    )??;
    buf.truncate(len);

    let err = Error::Conversion("could not resolve instance".into());
    if len == 0 {
        return Err(err);
    }

    let response = ::std::str::from_utf8(&buf[3..len])?;
    let port: u16 = response
        .find("tcp;")
        .and_then(|pos| response[pos..].split(';').nth(1))
        .ok_or(err)
        .and_then(|val| Ok(val.parse()?))?;
    addr.set_port(port);

    return connect_tcp(params, addr).await;
}

pub async fn connect_tcp<P>(params: P, addr: SocketAddr) -> Result<Client>
where
    P: TryInto<ConnectParams>,
    P::Error: Into<Error>,
{
    let mut stream = TcpStream::connect(addr).await?;
    stream.set_nodelay(true)?;
    let mut ctx = protocol::Context::new();

    let mut connecting = Connecting {
        stream,
        ctx,
        params: params.try_into().map_err(|err| err.into())?,
    };
    connecting.prelogin().await?;
    let mut connecting = connecting.tls_handshake().await?;
    connecting.login().await?;

    // TODO: if tokio-tls supports splitting itself, do that instead
    let (mut reader, writer) = tokio::io::split(connecting.stream);

    // Connection should be ready
    let mut ts = protocol::TokenStreamReader::new(protocol::PacketReader::new(&mut reader));
    while let token = ts.read_token().await? {
        match token {
            protocol::TokenType::EnvChange => {
                println!("env change {:?}", ts.read_env_change_token().await?);
            }
            protocol::TokenType::Info => {
                println!("info {:?}", ts.read_info_token().await?);
            }
            protocol::TokenType::LoginAck => {
                println!("login ack {:?}", ts.read_login_ack_token().await?);
            }
            protocol::TokenType::Done => {
                println!("done {:?}", ts.read_done_token(&connecting.ctx).await?);
                break;
            }
            _ => panic!("TODO"),
        }
    }

    let (sender, result_receivers) = mpsc::unbounded_channel();

    let ctx = Arc::new(connecting.ctx);
    let conn = Connection {
        ctx: ctx.clone(),
        reader: Box::new(reader),
        result_receivers,
    };

    let mut f = conn.into_future();
    let conn_fut: Pin<Box<dyn Future<Output = Result<()>>>> =
        Box::pin(future::poll_fn(move |ctx| {
            let span = trace_span!("conn");
            let _enter = span.enter();
            unsafe { Pin::new_unchecked(&mut f) }.poll(ctx)
        }));

    let inner = Client {
        ctx,
        writer: Arc::new(sync::Mutex::new(Box::new(writer))),
        conn_handler: conn_fut.shared(),
        sender,
    };
    Ok(inner)
}

struct Connecting<S> {
    stream: S,
    ctx: protocol::Context,
    params: ConnectParams,
}

impl<S: AsyncRead + AsyncWrite + Unpin> Connecting<S> {
    async fn prelogin(&mut self) -> Result<()> {
        // Send PreLogin
        let mut msg = protocol::PreloginMessage::new();
        msg.encryption = self.params.ssl;
        if msg.encryption != EncryptionLevel::NotSupported {
            #[cfg(not(feature = "tls"))]
            panic!(
                "TLS support is not enabled in this build, but required for this configuration!"
            );
        }
        let mut buf = msg.serialize()?;
        let header = protocol::PacketHeader {
            ty: protocol::PacketType::PreLogin,
            status: protocol::PacketStatus::EndOfMessage,
            ..self.ctx.new_header(buf.len())
        };
        header.serialize(&mut buf)?;
        self.stream.write_all(&buf).await?;

        // Wait for PreLogin response
        let mut reader = protocol::PacketReader::new(&mut self.stream);
        let response = protocol::PreloginMessage::unserialize(&mut reader).await?;
        println!("{:?}", response);
        self.params.ssl = match (msg.encryption, response.encryption) {
            (EncryptionLevel::NotSupported, EncryptionLevel::NotSupported) => {
                EncryptionLevel::NotSupported
            }
            (EncryptionLevel::Off, EncryptionLevel::Off) => EncryptionLevel::Off,
            (EncryptionLevel::On, EncryptionLevel::Off)
            | (EncryptionLevel::On, EncryptionLevel::NotSupported) => {
                panic!("todo: terminate connection, invalid encryption")
            }
            (_, _) => EncryptionLevel::On,
        };
        Ok(())
    }

    async fn tls_handshake(mut self) -> Result<Connecting<impl AsyncRead + AsyncWrite + Unpin>> {
        let mut builder = native_tls::TlsConnector::builder();
        if self.params.trust_cert {
            builder
                .danger_accept_invalid_certs(true)
                .danger_accept_invalid_hostnames(true)
                .use_sni(false);
        }

        let cx = builder.build().unwrap();
        let connector = tokio_tls::TlsConnector::from(cx);

        let wrapped = TlsNegotiateWrapper::new(self.stream);
        let mut ret = Connecting {
            stream: connector
                .connect("", wrapped)
                .await
                .expect("TODO: handle error"),
            ctx: self.ctx,
            params: self.params,
        };
        event!(Level::TRACE, "TLS handshake done");
        ret.stream.get_mut().pending_handshake = false;
        Ok(ret)
    }

    async fn login(&mut self) -> Result<()> {
        let mut login_message = protocol::LoginMessage::new();
        let mut builder = winauth::windows::NtlmSspiBuilder::new(); // TODO SPN, channel bindings
        let mut sso_client = builder.build()?;
        let buf = sso_client.next_bytes(None)?;
        login_message.integrated_security = buf;
        let mut buf = login_message.serialize(&self.ctx)?;
        let header = protocol::PacketHeader {
            ty: protocol::PacketType::TDSv7Login,
            status: protocol::PacketStatus::EndOfMessage,
            ..self.ctx.new_header(buf.len())
        };
        header.serialize(&mut buf)?;
        event!(Level::TRACE, "Sending login");
        self.stream.write_all(&buf).await?;

        // Perform SSO
        let mut reader = protocol::PacketReader::new(&mut self.stream);
        let header = reader.read_header().await?;
        event!(Level::TRACE, "Received {:?}", header.ty);
        if header.ty != protocol::PacketType::TabularResult {
            return Err(Error::Protocol(
                "expected tabular result in response to login message".into(),
            ));
        }
        reader.read_packet_with_header(&header).await?;

        let mut reader = protocol::TokenStreamReader::new(reader);
        assert_eq!(reader.read_token().await?, protocol::TokenType::SSPI);
        let sso_bytes = reader.read_sspi_token().await?;
        event!(Level::TRACE, sspi_len = sso_bytes.len());
        match sso_client.next_bytes(Some(sso_bytes))? {
            Some(sso_response) => {
                event!(Level::TRACE, sspi_response_len = sso_response.len());
                let header = protocol::PacketHeader {
                    ty: protocol::PacketType::TDSv7Login,
                    status: protocol::PacketStatus::EndOfMessage,
                    ..self
                        .ctx
                        .new_header(sso_response.len() + protocol::HEADER_BYTES)
                };
                let mut header_buf = [0u8; protocol::HEADER_BYTES];
                header.serialize(&mut header_buf)?;
                let mut buf = vec![];
                buf.extend_from_slice(&header_buf);
                buf.extend_from_slice(&sso_response);
                self.stream.write_all(&buf).await?;
            }
            None => unreachable!(),
        }
        Ok(())
    }
}
