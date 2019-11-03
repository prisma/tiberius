use std::collections::HashMap;
use std::convert::TryInto;
use std::future::Future;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::future::{self, FutureExt};
use tokio::future::FutureExt as TokioFutureExt;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpStream, UdpSocket};
use tokio::sync::{self, mpsc};
use tracing::{self, debug_span, event, trace_span, Level};
use winauth::NextBytes;

use crate::protocol::{self, EncryptionLevel};
use crate::tls::{MaybeTlsStream, TlsPreloginWrapper, TlsStream};
use crate::{Connection, Error, Result};

/// Settings for the connection, everything that isn't IO/transport specific (e.g. authentication)
pub struct ConnectParams {
    pub host: String,
    pub ssl: EncryptionLevel,
    pub trust_cert: bool,
    pub target_db: Option<String>, // TODO
                                   // pub auth: AuthMethod,
                                   // pub spn: Cow<'static, str>,
}

impl ConnectParams {
    /// Get a default/zeroed set of connection params
    pub fn new() -> ConnectParams {
        ConnectParams {
            host: "".to_owned(),
            ssl: if cfg!(feature = "tls") {
                EncryptionLevel::Off
            } else {
                EncryptionLevel::NotSupported
            },
            target_db: None,
            trust_cert: false,
            // auth: AuthMethod::SqlServer("".into(), "".into()),
            // spn: Cow::Borrowed(""),
        }
    }
}

pub async fn connect_tcp_sql_browser<P>(
    params: P,
    mut addr: SocketAddr,
    instance_name: &str,
) -> Result<Connection>
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

pub async fn connect_tcp<P>(params: P, addr: SocketAddr) -> Result<Connection>
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

    let (result_sender, result_receiver) = mpsc::unbounded_channel();

    let ctx = Arc::new(connecting.ctx);
    let writer = Arc::new(sync::Mutex::new(
        Box::new(writer) as Box<dyn AsyncWrite + Unpin>
    ));
    let close_handle_queue = Arc::new(Mutex::new(vec![]));
    let mut conn = Connection {
        ctx,
        writer,
        // This won't be used by the worker future anyways, so we do not care about this definition cycle
        conn_handler: (Box::pin(futures_util::future::ok(()))
            as Pin<Box<dyn Future<Output = Result<()>>>>)
            .shared(),
        result_sender,
        close_handle_queue,
    };

    let mut f = conn
        .clone()
        .into_worker_future(Box::new(reader), result_receiver);
    let conn_fut: Pin<Box<dyn Future<Output = Result<()>>>> =
        Box::pin(future::poll_fn(move |ctx| {
            let span = trace_span!("conn");
            let _enter = span.enter();
            unsafe { Pin::new_unchecked(&mut f) }.poll(ctx)
        }));

    conn.conn_handler = conn_fut.shared();
    Ok(conn)
}

/// A dynamic connection target
#[derive(PartialEq, Debug)]
enum ConnectTarget {
    Tcp(SocketAddr),
    TcpViaSQLBrowser(SocketAddr, String),
}

pub async fn connect(connection_str: &str) -> Result<Connection> {
    let mut connect_params = ConnectParams::new();
    let mut target: Option<ConnectTarget> = None;

    let mut input = &connection_str[..];
    while !input.is_empty() {
        // (MSDN) The basic format of a connection string includes a series of keyword/value
        // pairs separated by semicolons. The equal sign (=) connects each keyword and its value.
        let key = {
            let mut t = input.splitn(2, '=');
            let key = t.next().unwrap();
            if let Some(i) = t.next() {
                input = i;
            } else {
                return Err(Error::Conversion(
                    "connection string expected key. expected `=` never found".into(),
                ));
            }
            key.trim().to_lowercase()
        };

        // (MSDN) To include values that contain a semicolon, single-quote character,
        // or double-quote character, the value must be enclosed in double quotation marks.
        // If the value contains both a semicolon and a double-quote character, the value can
        // be enclosed in single quotation marks.
        let quote_char = match &input[0..1] {
            "'" => Some('\''),
            "\"" => Some('"'),
            _ => None,
        };

        let value = if let Some(quote_char) = quote_char {
            let mut value = String::new();
            loop {
                let mut t = input.splitn(3, quote_char);
                t.next().unwrap(); // skip first quote character
                value.push_str(t.next().unwrap());
                input = t.next().unwrap_or("");
                if input.starts_with(quote_char) {
                    // (MSDN) If the value contains both single-quote and double-quote
                    // characters, the quotation mark character used to enclose the value must
                    // be doubled every time it occurs within the value.
                    value.push(quote_char);
                } else if input.trim_start().starts_with(";") {
                    input = input.trim_start().trim_start_matches(";");
                    break;
                } else if input.is_empty() {
                    break;
                } else {
                    return Err(Error::Conversion(
                        "connection string: text after escape sequence".into(),
                    ));
                }
            }
            value
        } else {
            let mut t = input.splitn(2, ';');
            let value = t.next().unwrap();
            input = t.next().unwrap_or("");
            // (MSDN) To include preceding or trailing spaces in the string value, the value
            // must be enclosed in either single quotation marks or double quotation marks.
            value.trim().to_owned()
        };

        fn parse_bool<T: AsRef<str>>(v: T) -> Result<bool> {
            match v.as_ref().trim().to_lowercase().as_str() {
                "true" | "yes" => Ok(true),
                "false" | "no" => Ok(false),
                _ => Err(Error::Conversion(
                    "connection string: not a valid boolean".into(),
                )),
            }
        }

        match key.as_str() {
            "server" => {
                if value.starts_with("tcp:") {
                    let mut parts: Vec<_> = value[4..].split(',').collect();
                    assert!(!parts.is_empty() && parts.len() < 3);
                    if parts.len() == 1 {
                        // Connect using a host and an instance name, we first need to resolve to a port
                        parts = parts[0].split('\\').collect();
                        let addr =
                            (parts[0], 1434)
                                .to_socket_addrs()?
                                .nth(0)
                                .ok_or(Error::Conversion(
                                    "connection string: could not resolve server address".into(),
                                ))?;
                        target = Some(ConnectTarget::TcpViaSQLBrowser(addr, parts[1].to_owned()));
                    } else if parts.len() == 2 {
                        // Connect using a TCP target
                        let (host, port) = (parts[0], parts[1].parse::<u16>()?);
                        let addr =
                            (host, port)
                                .to_socket_addrs()?
                                .nth(0)
                                .ok_or(Error::Conversion(
                                    "connection string: could not resolve server address".into(),
                                ))?;
                        target = Some(ConnectTarget::Tcp(addr));
                    }
                    connect_params.host = parts[0].to_owned().into();
                }
            }
            "integratedsecurity" => {
                if value.to_lowercase() == "sspi" || parse_bool(&value)? {
                    /* TODO #[cfg(windows)]
                    {
                        connect_params.auth = AuthMethod::SSPI_SSO;
                    }
                    #[cfg(not(windows))]
                    {
                        connect_params.auth = AuthMethod::WinAuth("".into(), "".into());
                    }*/
                }
            }
            "uid" | "username" | "user" => {
                /* TODO connect_params.auth = match connect_params.auth {
                    AuthMethod::SqlServer(ref mut username, _) |
                    AuthMethod::WinAuth(ref mut username, _) => {
                        *username = value.into_owned().into();
                        continue;
                    }
                    #[cfg(windows)]
                    AuthMethod::SSPI_SSO => {
                        AuthMethod::WinAuth(value.into_owned().into(), "".into())
                    }
                };*/
            }
            "password" | "pwd" => {
                /* TODO connect_params.auth = match connect_params.auth {
                    AuthMethod::SqlServer(_, ref mut password) |
                    AuthMethod::WinAuth(_, ref mut password) => {
                        *password = value.into_owned().into();
                        continue;
                    }
                    #[cfg(windows)]
                    AuthMethod::SSPI_SSO => {
                        AuthMethod::WinAuth("".into(), value.into_owned().into())
                    }
                }; */
            }
            "database" => {
                connect_params.target_db = Some(value);
            }
            "trustservercertificate" => {
                connect_params.trust_cert = parse_bool(value)?;
            }
            "encrypt" => {
                connect_params.ssl = if value == "none" {
                    EncryptionLevel::NotSupported
                } else if parse_bool(value)? {
                    EncryptionLevel::Required
                } else {
                    EncryptionLevel::Off
                };
            }
            _ => {
                return Err(Error::Conversion(
                    format!("connection string: unknown config option: {:?}", key).into(),
                ))
            }
        }
    }

    let target = target.ok_or(Error::Conversion(
        "connection string pointing into the void. no connection endpoint specified.".into(),
    ))?;
    event!(Level::DEBUG, connecting = tracing::field::debug(&target));
    match target {
        ConnectTarget::Tcp(addr) => connect_tcp(connect_params, addr).await,
        ConnectTarget::TcpViaSQLBrowser(addr, instance_name) => {
            connect_tcp_sql_browser(connect_params, addr, &instance_name).await
        }
    }
}

struct Connecting<S> {
    stream: S,
    ctx: protocol::Context,
    params: ConnectParams,
}

impl<S: AsyncRead + AsyncWrite + Unpin + 'static> Connecting<S> {
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
        println!("{:?}", msg);
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

    async fn tls_handshake(
        mut self,
    ) -> Result<Connecting<MaybeTlsStream<S, impl TlsStream<Ret = S> + Unpin>>> {
        let stream: MaybeTlsStream<_, _> = if self.params.ssl != EncryptionLevel::NotSupported {
            let mut builder = native_tls::TlsConnector::builder();
            if self.params.trust_cert {
                builder
                    .danger_accept_invalid_certs(true)
                    .danger_accept_invalid_hostnames(true)
                    .use_sni(false);
            }

            let cx = builder.build().unwrap();
            let connector = tokio_tls::TlsConnector::from(cx);

            let wrapped = TlsPreloginWrapper::new(self.stream);
            let mut stream = connector
                .connect("", wrapped)
                .await
                .expect("TODO: handle error");
            event!(Level::TRACE, "TLS handshake done");

            stream.get_mut().pending_handshake = false;
            MaybeTlsStream::Tls(stream)
        } else {
            MaybeTlsStream::Raw(self.stream)
        };

        let mut ret = Connecting {
            stream,
            ctx: self.ctx,
            params: self.params,
        };
        Ok(ret)
    }
}

impl<R, T> Connecting<MaybeTlsStream<R, T>>
where
    R: AsyncRead + AsyncWrite + Unpin,
    T: TlsStream<Ret = R> + Unpin,
{
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
        self.stream.flush().await?;
        if let EncryptionLevel::Off = self.params.ssl {
            event!(Level::TRACE, "Disabling encryption again");
            self.stream = MaybeTlsStream::Raw(match self.stream {
                MaybeTlsStream::Tls(ref mut tls) => tls.take_inner(),
                _ => unreachable!(),
            });
        }

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
