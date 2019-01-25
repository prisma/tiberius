//! A pure-rust TDS implementation for Microsoft SQL Server (>=2008)
//!
//! # A simple example
//! **Warning:** Do not use `simple_query` with user-specified data. Resort to prepared statements for that.
//!
//! ```rust
//! extern crate futures;
//! extern crate futures_state_stream;
//! extern crate tokio;
//! extern crate tiberius;
//! use futures::Future;
//! use futures_state_stream::StateStream;
//! use tokio::executor::current_thread;
//! use tiberius::SqlConnection;
//!
//! fn main() {
//! 
//!    // 1: for windows we demonstrate the hardcoded variant
//!    // which is equivalent to:
//!    //     let conn_str = "server=tcp:localhost,1433;integratedSecurity=true;";
//!    //     let future = SqlConnection::connect(conn_str).and_then(|conn| {
//!    // and for linux we use the connection string from an environment variable
//!    let conn_str = if cfg!(windows) {
//!        "server=tcp:localhost,1433;integratedSecurity=true;".to_owned()
//!    } else {
//!        ::std::env::var("TIBERIUS_TEST_CONNECTION_STRING").unwrap()
//!    };
//!
//!    let future = SqlConnection::connect(conn_str.as_str())
//!        .and_then(|conn| {
//!            conn.simple_query("SELECT 1+2").for_each(|row| {
//!                let val: i32 = row.get(0);
//!                assert_eq!(val, 3i32);
//!                Ok(())
//!            })
//!        })
//!        .and_then(|conn| conn.simple_exec("create table #Temp(gg int);"))
//!        .and_then(|(_, conn)| conn.simple_exec("UPDATE #Temp SET gg=1 WHERE gg=1"));
//! 
//!    current_thread::block_on_all(future).unwrap();
//! }
//! ```
//!
//!
//! # Prepared Statements
//! Parameters use numeric indexes such as @P1, @P2 for the n-th parameter (starting with 1 for the first)
//!
//! ```rust
//! extern crate futures;
//! extern crate futures_state_stream;
//! extern crate tokio;
//! extern crate tiberius;
//! use futures::Future;
//! use futures_state_stream::StateStream;
//! use tokio::executor::current_thread;
//! use tiberius::SqlConnection;
//!
//! fn main() {
//! 
//!    // 1: Same as in the example above
//!    let conn_str = if cfg!(windows) {
//!        "server=tcp:localhost,1433;integratedSecurity=true;".to_owned()
//!    } else {
//!        ::std::env::var("TIBERIUS_TEST_CONNECTION_STRING").unwrap()
//!    };
//!
//!    let future = SqlConnection::connect(conn_str.as_str()).and_then(|conn| {
//!        conn.query("SELECT x FROM (VALUES (1),(2),(3),(4)) numbers(x) WHERE x%@P1=@P2",
//!            &[&2i32, &0i32]).for_each(|row| {
//!            let val: i32 = row.get(0);
//!            assert_eq!(val % 2, 0i32);
//!            Ok(())
//!        })
//!    });
//!    current_thread::block_on_all(future).unwrap();
//! }
//! ```
//! If you intend to execute the same statement multiple times for the same connection, you should use `.prepare`.
//! For most cases you'll want this though.

#[macro_use]
extern crate bitflags;
extern crate byteorder;
extern crate bytes;
extern crate encoding;
extern crate fnv;
#[macro_use]
extern crate futures;
extern crate futures_state_stream;
#[macro_use]
extern crate lazy_static;
extern crate tokio;
extern crate winauth;

use std::borrow::Cow;
use std::convert::From;
use std::net::{SocketAddr, ToSocketAddrs};
use std::marker::PhantomData;
use std::mem;
use std::result;
use std::sync::Arc;
use std::io;
use fnv::FnvHashMap;
use futures::{Async, Future, IntoFuture, Poll, Sink};
use futures::sync::oneshot;
// TODO: depend on tokio subcrates?
use tokio::net::{TcpStream, UdpSocket};

/// Trait to convert a u8 to a `enum` representation
trait FromUint
where
    Self: Sized,
{
    fn from_u8(n: u8) -> Option<Self>;
    fn from_u32(n: u32) -> Option<Self>;
}

macro_rules! uint_enum {
    ($( #[$gattr:meta] )* pub enum $ty:ident { $( $( #[$attr:meta] )* $variant:ident = $val:expr,)* }) => {
        uint_enum!($( #[$gattr ])* (pub) enum $ty { $( $( #[$attr] )* $variant = $val, )* });
    };
    ($( #[$gattr:meta] )* enum $ty:ident { $( $( #[$attr:meta] )* $variant:ident = $val:expr,)* }) => {
        uint_enum!($( #[$gattr ])* () enum $ty { $( $( #[$attr] )* $variant = $val, )* });
    };

    ($( #[$gattr:meta] )* ( $($vis:tt)* ) enum $ty:ident { $( $( #[$attr:meta] )* $variant:ident = $val:expr,)* }) => {
        #[derive(Debug, Copy, Clone)]
        $( #[$gattr] )*
        $( $vis )* enum $ty {
            $( $( #[$attr ])* $variant = $val, )*
        }

        impl FromUint for $ty {
            fn from_u8(n: u8) -> Option<$ty> {
                match n {
                    $( x if x == $ty::$variant as u8 => Some($ty::$variant), )*
                    _ => None
                }
            }

            fn from_u32(n: u32) -> Option<$ty> {
                match n {
                    $( x if x == $ty::$variant as u32 => Some($ty::$variant), )*
                    _ => None
                }
            }
        }
    }
}

mod collation;
mod transport;
mod plp;
mod protocol;
mod types;
mod tokens;
pub mod query;
pub mod stmt;
mod transaction;

use transport::{Io, TdsTransport, TransportStream};
use protocol::{LoginMessage, PacketType, PreloginMessage, SerializeMessage, SspiMessage,
               UnserializeMessage};
use types::{ColumnData, ToSql};
use tokens::{DoneStatus, RpcOptionFlags, RpcParam, RpcProcId, RpcProcIdValue, RpcStatusFlags,
             TdsResponseToken, TokenColMetaData, TokenRpcRequest, WriteToken};
use query::{ExecFuture, QueryStream, ResultSetStream};
use stmt::{Statement, StmtStream, ExecResult, QueryResult};
use transaction::new_transaction;
use winauth::NextBytes;
pub use protocol::EncryptionLevel;
pub use transaction::Transaction;
pub use types::prelude as ty;

lazy_static! {
    #[doc(hidden)]
    pub static ref DRIVER_VERSION: u64 = get_driver_version();
}

fn get_driver_version() -> u64 {
    env!("CARGO_PKG_VERSION")
        .splitn(6, '.')
        .enumerate()
        .fold(0u64, |acc, part| {
            acc | (part.1.parse::<u64>().unwrap() << (part.0 * 8))
        })
}

/// A unified error enum that contains several errors that might occurr during the lifecycle of this driver
#[derive(Debug)]
pub enum Error {
    /// An error occurred during the attempt of performing I/O
    Io(io::Error),
    /// An error occurred on the protocol level
    Protocol(Cow<'static, str>),
    Encoding(Cow<'static, str>),
    Conversion(Cow<'static, str>),
    Utf8(std::str::Utf8Error),
    Utf16(std::string::FromUtf16Error),
    ParseInt(std::num::ParseIntError),
    Server(tokens::TokenError),
    Canceled,
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::Io(err)
    }
}

impl From<std::num::ParseIntError> for Error {
    fn from(err: std::num::ParseIntError) -> Error {
        Error::ParseInt(err)
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(err: std::str::Utf8Error) -> Error {
        Error::Utf8(err)
    }
}

impl From<std::string::FromUtf16Error> for Error {
    fn from(err: std::string::FromUtf16Error) -> Error {
        Error::Utf16(err)
    }
}

pub type Result<T> = result::Result<T, Error>;

/// A connection in a state before any login has happened
enum SqlConnectionLoginState<I: Io, F: Future<Item = I, Error = Error> + Send + Sized> {
    Connection(Option<(F, ConnectParams)>),

    PreLoginSend,
    PreLoginRecv,
    #[cfg(feature = "tls")]
    TLSPending(Option<transport::tls::Connect<transport::tls::TlsTdsWrapper<I>>>),
    LoginSend,
    LoginRecv,
    TokenStreamRecv,
    TokenStreamSend,
    _Dummy(PhantomData<I>),
}

/// A pending SQL connection
#[must_use = "futures do nothing unless polled"]
struct Connect<I: BoxableIo, F: Future<Item = I, Error = Error> + Send + Sized> {
    state: SqlConnectionLoginState<I, F>,
    context: Option<SqlConnectionContext<I>>,
}

struct SqlConnectionContext<I: BoxableIo> {
    params: ConnectParams,
    transport: TdsTransport<TransportStream<I>>,
    wauth_client: Option<Box<NextBytes + Send>>,
}

impl<I: BoxableIo> SqlConnectionContext<I> {
    /// Queues a simple message which serializes to ONE packet
    fn queue_simple_message<M: SerializeMessage>(&mut self, m: M) -> Poll<(), io::Error> {
        let vec = m.serialize_message(&mut self.transport)?;
        self.transport.inner.queue_vec(vec);
        Ok(Async::Ready(()))
    }

    fn channel_bindings(&self) -> io::Result<Option<Vec<u8>>> {
        #[cfg(feature = "tls")]
        return self.transport.inner.io.channel_bindings();
        #[cfg(not(feature = "tls"))]
        Ok(None)
    }
}

impl<I: BoxableIo, F: Future<Item = I, Error = Error> + Send> Future for Connect<I, F> {
    type Item = SqlConnection<I>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Error> {
        loop {
            self.state = match self.state {
                SqlConnectionLoginState::Connection(ref mut pairs @ Some(_)) => {
                    let trans = try_ready!(pairs.as_mut().map(|x| &mut x.0).unwrap().poll());
                    #[cfg(feature = "tls")]
                    let trans = TransportStream::Raw(trans);
                    let trans = TdsTransport::new(trans);

                    self.context = Some(SqlConnectionContext {
                        params: pairs.take().map(|x| x.1).unwrap(),
                        transport: trans,
                        wauth_client: None,
                    });
                    SqlConnectionLoginState::PreLoginSend
                }
                ref mut state => {
                    let ctx = self.context
                        .as_mut()
                        .expect("expected context for post-login state");
                    match *state {
                        SqlConnectionLoginState::PreLoginSend => {
                            let mut msg = PreloginMessage::new();
                            if cfg!(feature = "tls") {
                                msg.encryption = ctx.params.ssl;
                            } else if ctx.params.ssl != EncryptionLevel::NotSupported {
                                panic!(
                                    "TLS support is not enabled in this build, but required for this configuration!"
                                );
                            }
                            try_ready!(ctx.queue_simple_message(msg));
                            SqlConnectionLoginState::PreLoginRecv
                        }
                        SqlConnectionLoginState::PreLoginRecv => {
                            try_ready!(ctx.transport.inner.poll_complete());
                            let header = try_ready!(ctx.transport.inner.next_packet());
                            assert_eq!(header.ty, PacketType::TabularResult);
                            // parse the prelogin packet
                            let buf = ctx.transport.inner.get_packet(header.length as usize);
                            let msg = buf.as_ref().unserialize_message(&mut ctx.transport)?;

                            let encr = match (ctx.params.ssl, msg.encryption) {
                                (EncryptionLevel::NotSupported, EncryptionLevel::NotSupported) => {
                                    EncryptionLevel::NotSupported
                                }
                                (EncryptionLevel::Off, EncryptionLevel::Off) => {
                                    EncryptionLevel::Off
                                }
                                (EncryptionLevel::On, EncryptionLevel::Off) |
                                (EncryptionLevel::On, EncryptionLevel::NotSupported) => {
                                    panic!("todo: terminate connection, invalid encryption")
                                }
                                (_, _) => EncryptionLevel::On,
                            };
                            ctx.params.ssl = encr;

                            // move to an TLS stream, if requested
                            match encr {
                                // encrypt entirely or only logon packet
                                EncryptionLevel::On |
                                EncryptionLevel::Off |
                                EncryptionLevel::Required => {
                                    #[cfg(feature = "tls")]
                                    {
                                        match mem::replace(
                                            &mut ctx.transport.inner.io,
                                            TransportStream::None,
                                        ) {
                                            TransportStream::Raw(stream) => {
                                                let wrapped_stream =
                                                    transport::tls::TlsTdsWrapper::new(stream);
                                                let host = if ctx.params.trust_cert {
                                                    None
                                                } else {
                                                    Some(&*ctx.params.host)
                                                };
                                                let tls_stream = transport::tls::connect_async(
                                                    wrapped_stream,
                                                    host,
                                                );
                                                SqlConnectionLoginState::TLSPending(
                                                    Some(tls_stream),
                                                )
                                            }
                                            _ => unreachable!(),
                                        }
                                    }
                                    #[cfg(not(feature = "tls"))]
                                    panic!("encryption requested without build support!");
                                }
                                // do not encrypt at all
                                EncryptionLevel::NotSupported => SqlConnectionLoginState::LoginSend,
                            }
                        }
                        #[cfg(feature = "tls")]
                        SqlConnectionLoginState::TLSPending(ref mut connect_async) => {
                            assert!(connect_async.is_some());
                            let mut stream = try_ready!(connect_async.as_mut().unwrap().poll());
                            connect_async.take();
                            stream.get_mut().get_mut().wrap = false;
                            ctx.transport.inner.io = TransportStream::TLS(stream);
                            SqlConnectionLoginState::LoginSend
                        }
                        SqlConnectionLoginState::LoginSend => {
                            let mut login_message = LoginMessage::new();

                            if let Some(ref db) = ctx.params.target_db {
                                login_message.db_name = db.clone();
                            }

                            // authentication
                            match ctx.params.auth {
                                #[cfg(windows)]
                                AuthMethod::SSPI_SSO => {
                                    let mut builder = winauth::windows::NtlmSspiBuilder::new()
                                        .target_spn(&ctx.params.spn);
                                    if let Some(ref hash) = ctx.channel_bindings()? {
                                        builder = builder.channel_bindings(hash);
                                    }
                                    let mut sso_client = builder.build()?;
                                    let buf = sso_client.next_bytes(None)?.map(|x| x.to_owned());
                                    login_message.integrated_security = buf;
                                    ctx.wauth_client = Some(Box::new(sso_client));
                                }
                                AuthMethod::SqlServer(ref username, ref password) => {
                                    login_message.username = username.clone();
                                    login_message.password = password.clone();
                                }
                                AuthMethod::WinAuth(ref username, ref password) => {
                                    // TODO: integrate channel binding
                                    // TODO: use NEGOTIATE
                                    let (domain, username) = if let Some(idx) = username.find("\\")
                                    {
                                        match *username {
                                            Cow::Borrowed(ref x) => {
                                                let (domain, username) = (&x[..idx], &x[idx + 1..]);
                                                (Some(Cow::Borrowed(domain)), username.into())
                                            }
                                            Cow::Owned(ref x) => {
                                                let (domain, username) = (&x[..idx], &x[idx + 1..]);
                                                (
                                                    Some(Cow::Owned(domain.to_owned())),
                                                    username.to_owned().into(),
                                                )
                                            }
                                        }
                                    } else {
                                        (None, username.clone())
                                    };
                                    let mut builder = winauth::NtlmV2ClientBuilder::new()
                                        .target_spn(ctx.params.spn.clone());
                                    if let Some(ref hash) = ctx.channel_bindings()? {
                                        builder = builder.channel_bindings(hash);
                                    }
                                    let mut client =
                                        builder.build(domain, username, password.clone());
                                    let buf = client.next_bytes(None)?.map(|x| x.to_owned());
                                    login_message.integrated_security = buf;
                                    ctx.wauth_client = Some(Box::new(client));
                                }
                            }

                            try_ready!(ctx.queue_simple_message(login_message));
                            SqlConnectionLoginState::LoginRecv
                        }
                        SqlConnectionLoginState::LoginRecv => {
                            try_ready!(ctx.transport.inner.poll_complete());
                            // if login only encryption was negotiated, disable encryption
                            // after we sent the first login packet
                            #[cfg(feature = "tls")]
                            {
                                if ctx.params.ssl == EncryptionLevel::Off {
                                    let stream = mem::replace(
                                        &mut ctx.transport.inner.io,
                                        TransportStream::None,
                                    );
                                    ctx.transport.inner.io = match stream {
                                        TransportStream::TLS(stream) => {
                                            TransportStream::TLSRaw(stream)
                                        }
                                        x => x,
                                    };
                                }
                            }
                            let header = try_ready!(ctx.transport.inner.next_packet());
                            assert_eq!(header.ty, PacketType::TabularResult);
                            SqlConnectionLoginState::TokenStreamRecv
                        }
                        SqlConnectionLoginState::TokenStreamRecv => {
                            let token = try_ready!(ctx.transport.next_token());
                            match token {
                                Some(TdsResponseToken::SSPI(bytes)) => {
                                    assert!(ctx.wauth_client.is_some());
                                    match ctx.wauth_client
                                        .as_mut()
                                        .unwrap()
                                        .next_bytes(Some(bytes.as_ref()))?
                                    {
                                        Some(bytes) => {
                                            ctx.queue_simple_message(SspiMessage(bytes))?;
                                            SqlConnectionLoginState::TokenStreamSend
                                        }
                                        None => {
                                            ctx.wauth_client = None;
                                            SqlConnectionLoginState::TokenStreamRecv
                                        }
                                    }
                                }
                                Some(TdsResponseToken::Done(done)) => {
                                    // the connection is ready 2 go, we're done with our initialization
                                    assert_eq!(done.status, DoneStatus::empty());
                                    break;
                                }
                                Some(_) | None => SqlConnectionLoginState::TokenStreamRecv,
                            }
                        }
                        SqlConnectionLoginState::TokenStreamSend => {
                            try_ready!(ctx.transport.inner.poll_complete());
                            SqlConnectionLoginState::TokenStreamRecv
                        }
                        SqlConnectionLoginState::_Dummy(_) => unreachable!(),
                        _ => {
                            panic!("Connect polled multiple times. item already consumed")
                        }
                    }
                }
            }
        }

        let ctx = self.context
            .take()
            .expect("expected context after future completion");
        let conn = InnerSqlConnection {
            transport: ctx.transport,
            stmts: FnvHashMap::default(),
        };
        return Ok(Async::Ready(SqlConnection(conn)));
    }
}

/// A type which is constructable from a statement as a statement's result
pub trait StmtResult<I: BoxableIo> {
    type Result: Sized;

    fn from_connection(SqlConnection<I>, oneshot::Sender<SqlConnection<I>>) -> Self::Result;
}

/// A representation of an authenticated and ready for use SQL connection
struct InnerSqlConnection<I: BoxableIo> {
    transport: TdsTransport<TransportStream<I>>,
    stmts: FnvHashMap<String, Vec<(Vec<&'static str>, i32, Option<Arc<TokenColMetaData>>)>>,
}

/// A connection to a SQL server with an underlying IO (e.g. socket)
pub struct SqlConnection<I: BoxableIo>(InnerSqlConnection<I>);

/// The authentication method that should be used during authentication
#[derive(Debug, PartialEq)]
#[allow(non_camel_case_types)]
pub enum AuthMethod {
    SqlServer(Cow<'static, str>, Cow<'static, str>),
    // TODO: this should map to negotiate, currently it maps to NTLMv2
    WinAuth(Cow<'static, str>, Cow<'static, str>),
    /// Single sign on using the local windows credentials (windows-only)
    #[cfg(windows)]
    SSPI_SSO,
}

/// Settings for the connection, everything that isn't IO/transport specific (e.g. authentication)
pub struct ConnectParams {
    pub host: Cow<'static, str>,
    pub ssl: EncryptionLevel,
    pub trust_cert: bool,
    pub auth: AuthMethod,
    pub target_db: Option<Cow<'static, str>>,
    pub spn: Cow<'static, str>,
}

impl ConnectParams {
    /// Get a default/zeroed set of connection params
    pub fn new() -> ConnectParams {
        ConnectParams {
            host: Cow::Borrowed(""),
            ssl: if cfg!(feature = "tls") {
                EncryptionLevel::Off
            } else {
                EncryptionLevel::NotSupported
            },
            trust_cert: false,
            auth: AuthMethod::SqlServer("".into(), "".into()),
            target_db: None,
            spn: Cow::Borrowed(""),
        }
    }

    pub fn set_spn(&mut self, host: &str, port: u16) {
        if self.spn.is_empty() {
            self.spn = format!("MSSQLSvc/{}:{}", host, port).into();
        }
    }
}

/// A variant of Io which can be boxed to allow dynamic dispatch
pub trait BoxableIo: Io + Send {}
impl<I: Io + Send> BoxableIo for I {}

/// A dynamic connection target
#[derive(PartialEq, Debug)]
enum ConnectTarget {
    Tcp(SocketAddr),
    TcpViaSQLBrowser(SocketAddr, String),
}

impl ConnectTarget {
    fn connect(self) 
        -> Box<Future<Item = Box<BoxableIo>, Error = Error> + Sync + Send>
    {
        match self {
            ConnectTarget::Tcp(ref addr) => {
                let future = TcpStream::connect(addr)
                    .and_then(|stream| {
                        stream.set_nodelay(true)?;
                        Ok(stream)
                    })
                    .from_err::<Error>()
                    .map(|stream| Box::new(stream) as Box<BoxableIo>);
                Box::new(future)
            }
            // First resolve the instance to a port via the
            // SSRP protocol/MS-SQLR protocol [1]
            // [1] https://msdn.microsoft.com/en-us/library/cc219703.aspx
            ConnectTarget::TcpViaSQLBrowser(addr, ref instance_name) => {
                let local_bind: SocketAddr = if addr.is_ipv4() {
                    "0.0.0.0:0".parse().unwrap()
                } else {
                    "[::]:0".parse().unwrap()
                };
                let msg = [&[4u8], instance_name.as_bytes()].concat();

                // TODO: implement a timeout for non-existing instances
                let future = UdpSocket::bind(&local_bind)
                    .into_future()
                    .and_then(move |socket| socket.send_dgram(msg, &addr))
                    .and_then(|(socket, _)| socket.recv_dgram(vec![0u8; 4096]))
                    .from_err::<Error>()
                    .and_then(|(_, buf, len, mut addr)| {
                        let err = Error::Conversion("could not resolve instance".into());
                        if len == 0 {
                            return Err(err);
                        }

                        let response = ::std::str::from_utf8(&buf[3..len])?;
                        let port: u16 = response.find("tcp;")
                            .and_then(|pos| response[pos..].split(';').nth(1))
                            .ok_or(err)
                            .and_then(|val| Ok(val.parse()?))?;
                        addr.set_port(port);
                        Ok(addr)
                    })
                    .and_then(move |addr| ConnectTarget::Tcp(addr).connect());
                Box::new(future)
            }
        }
    }
}

/// Parse connection strings
/// https://msdn.microsoft.com/de-de/library/system.data.sqlclient.sqlconnection.connectionstring(v=vs.110).aspx
fn parse_connection_str(connection_str: &str) -> Result<(ConnectParams, ConnectTarget)>
{ 
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
                } else if input.trim_left().starts_with(";") {
                    input = input.trim_left().trim_left_matches(";");
                    break;
                } else if input.is_empty() {
                    break;
                } else {
                    return Err(Error::Conversion(
                        "connection string: text after escape sequence".into(),
                    ));
                }
            }
            Cow::Owned(value)
        } else {
            let mut t = input.splitn(2, ';');
            let value = t.next().unwrap();
            input = t.next().unwrap_or("");
            // (MSDN) To include preceding or trailing spaces in the string value, the value
            // must be enclosed in either single quotation marks or double quotation marks.
            Cow::Borrowed(value.trim())
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
            "server" => if value.starts_with("tcp:") {
                let mut parts: Vec<_> = value[4..].split(',').collect();
                assert!(!parts.is_empty() && parts.len() < 3);
                if parts.len() == 1 {
                    // Connect using a host and an instance name, we first need to resolve to a port
                    parts = parts[0].split('\\').collect();
                    let addr = (parts[0], 1434).to_socket_addrs()?.nth(0).ok_or(Error::Conversion(
                        "connection string: could not resolve server address".into(),
                    ))?;
                    target = Some(ConnectTarget::TcpViaSQLBrowser(addr, parts[1].to_owned()));
                } else if parts.len() == 2 {
                    // Connect using a TCP target
                    let (host, port) = (parts[0], parts[1].parse::<u16>()?);
                    let addr = (host, port).to_socket_addrs()?.nth(0).ok_or(Error::Conversion(
                        "connection string: could not resolve server address".into(),
                    ))?;
                    target = Some(ConnectTarget::Tcp(addr));
                }
                connect_params.host = parts[0].to_owned().into();
            },
            "integratedsecurity" => if value.to_lowercase() == "sspi" || parse_bool(&value)? {
                #[cfg(windows)]
                {
                    connect_params.auth = AuthMethod::SSPI_SSO;
                }
                #[cfg(not(windows))]
                {
                    connect_params.auth = AuthMethod::WinAuth("".into(), "".into());
                }
            },
            "uid" | "username" | "user" => {
                connect_params.auth = match connect_params.auth {
                    AuthMethod::SqlServer(ref mut username, _) |
                    AuthMethod::WinAuth(ref mut username, _) => {
                        *username = value.into_owned().into();
                        continue;
                    }
                    #[cfg(windows)]
                    AuthMethod::SSPI_SSO => {
                        AuthMethod::WinAuth(value.into_owned().into(), "".into())
                    }
                };
            }
            "password" | "pwd" => {
                connect_params.auth = match connect_params.auth {
                    AuthMethod::SqlServer(_, ref mut password) |
                    AuthMethod::WinAuth(_, ref mut password) => {
                        *password = value.into_owned().into();
                        continue;
                    }
                    #[cfg(windows)]
                    AuthMethod::SSPI_SSO => {
                        AuthMethod::WinAuth("".into(), value.into_owned().into())
                    }
                };
            }
            "database" => {
                connect_params.target_db = Some(value.into_owned().into());
            }
            "trustservercertificate" => {
                connect_params.trust_cert = parse_bool(value)?;
            }
            "encrypt" => {
                connect_params.ssl = if parse_bool(value)? {
                    EncryptionLevel::Required
                } else if let EncryptionLevel::NotSupported = connect_params.ssl {
                    EncryptionLevel::NotSupported
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

    Ok((connect_params, target))
}

impl SqlConnection<Box<BoxableIo>> {
    /// Naive connection function for the SQL client
    pub fn connect(connection_str: &str) 
        -> Box<Future<Item = SqlConnection<Box<BoxableIo>>, Error=Error> + Send>
    {
        let future = parse_connection_str(connection_str)
            .into_future()
            .and_then(move |(connect_params, target)| {
                let stream = target.connect();
                SqlConnection::connect_to(connect_params, stream)
            });
        Box::new(future)
    }
}

impl<I: BoxableIo + Sized + 'static> SqlConnection<I> {
    /// Connect to the SQL server using given params and chosen stream
    pub fn connect_to<F>(params: ConnectParams, target: F) -> impl Future<Item=SqlConnection<I>, Error=Error>
        where F: Future<Item = I, Error = Error> + Sync + Send
    {
        let state = SqlConnectionLoginState::Connection(Some((target, params)));

        Connect {
            state,
            context: None,
        }
    }

    fn queue_sql_batch<'a, S>(&mut self, stmt: S) -> Result<()>
    where
        S: Into<Cow<'a, str>>,
    {
        let sql = stmt.into();
        protocol::write_sql_batch(&mut self.0.transport, &sql)?;
        Ok(())
    }

    fn simple_exec_internal<'a, Q, R: StmtResult<I>>(mut self, query: Q) -> ResultSetStream<I, R>
    where
        Q: Into<Cow<'a, str>>,
    {
        let result = self.queue_sql_batch(query);

        let ret = ResultSetStream::new(self);
        if let Err(err) = result {
            return ret.error(err);
        }
        ret
    }

    /// Execute a simple query and return multiple resultsets which consist of multiple rows.
    ///
    /// # Warning
    /// Do not use this with any user specified input.  
    /// Please resort to prepared statements in order to prevent SQL-Injections.  
    /// 
    /// You can access one resultset using [`for_each`](futures::Stream::for_each)  
    /// If you want to access multiple resultsets, go through [`into_stream`](stmt::QueryResult::into_stream)
    pub fn simple_query<'a, Q>(self, query: Q) -> QueryResult<ResultSetStream<I, QueryStream<I>>>
    where
        Q: Into<Cow<'a, str>>,
    {
        QueryResult::new(self.simple_exec_internal(query))
    }

    /// Execute a simple SQL-statement and return the affected rows  
    ///
    /// # Warning
    /// Do not use this with any user specified input.  
    /// Please resort to prepared statements in order to prevent SQL-Injections.  
    /// 
    /// If you want to access multiple resultsets, go through [`into_stream`](stmt::ExecResult::into_stream)
    pub fn simple_exec<'a, Q>(self, query: Q) -> ExecResult<ResultSetStream<I, ExecFuture<I>>>
    where
        Q: Into<Cow<'a, str>>,
    {
        ExecResult::new(self.simple_exec_internal(query))
    }

    fn do_prepare_exec<'b>(
        &self,
        stmt: &Statement,
        params: &'b [&'b ToSql],
    ) -> TokenRpcRequest<'b> {
        let mut param_str = String::with_capacity(10 * params.len());

        let mut params_meta = vec![
            RpcParam {
                name: Cow::Borrowed("handle"),
                flags: RpcStatusFlags::PARAM_BY_REF_VALUE,
                value: ColumnData::I32(0),
            },
            RpcParam {
                name: Cow::Borrowed("params"),
                flags: RpcStatusFlags::empty(),
                value: ColumnData::I32(0),
            },
            RpcParam {
                name: Cow::Borrowed("stmt"),
                flags: RpcStatusFlags::empty(),
                value: ColumnData::String(stmt.sql.clone()),
            },
        ];

        // determine the types from the given params
        for (i, param) in params.iter().enumerate() {
            if i > 0 {
                param_str.push(',')
            }
            param_str.push_str(&format!("@P{} ", i + 1));
            param_str.push_str(param.to_sql());

            params_meta.push(RpcParam {
                name: Cow::Owned(format!("@P{}", i + 1)),
                flags: RpcStatusFlags::empty(),
                value: param.to_column_data(),
            });
        }
        params_meta[1].value = ColumnData::String(param_str.into());

        // call sp_prepare to get a handle we can execute
        TokenRpcRequest {
            proc_id: RpcProcIdValue::Id(RpcProcId::SpPrepExec),
            flags: RpcOptionFlags::empty(),
            params: params_meta,
        }
    }

    fn do_exec<'a>(&self, handle: i32, params: &'a [&'a ToSql]) -> TokenRpcRequest<'a> {
        let mut params_meta = vec![
            RpcParam {
                // handle (using "handle" here makes RpcProcId::SpExecute not work and requires RpcProcIdValue::NAME, wtf)
                // not specifying the name is better anyways to reduce overhead on execute
                name: Cow::Borrowed(""),
                flags: RpcStatusFlags::empty(),
                value: ColumnData::I32(handle),
            },
        ];
        for (i, param) in params.iter().enumerate() {
            params_meta.push(RpcParam {
                name: Cow::Owned(format!("@P{}", i + 1)),
                flags: RpcStatusFlags::empty(),
                value: param.to_column_data(),
            });
        }

        TokenRpcRequest {
            proc_id: RpcProcIdValue::Id(RpcProcId::SpExecute),
            flags: RpcOptionFlags::NO_META,
            params: params_meta,
        }
    }

    fn internal_exec<R: StmtResult<I>>(
        mut self,
        stmt: Statement,
        params: &[&ToSql],
    ) -> StmtStream<I, R> {
        // call sp_prepare (with valid handle) or sp_prepexec (initializer)
        let (req, meta) = if let Some((handle, meta)) = stmt.get_handle_for(
            &self,
            &params.iter().map(|x| x.to_sql()).collect::<Vec<_>>(),
        ) {
            (self.do_exec(handle, params), meta)
        } else {
            (self.do_prepare_exec(&stmt, params), None)
        };

        // write everything (or atleast queue it for write)
        let result = req.write_token(&mut self.0.transport);
        let ret = StmtStream::new(self, stmt, meta, params);
        match result {
            Ok(_) => ret,
            Err(err) => ret.error(err),
        }
    }

    /// Execute a prepared statement and return each resultset and their associated rows
    /// 
    /// You can access one resultset using [`for_each`](futures::Stream::for_each)  
    /// If you want to access multiple resultsets, go through [`into_stream`](stmt::QueryResult::into_stream)
    pub fn query<S: Into<Statement>>(
        self,
        stmt: S,
        params: &[&ToSql],
    ) -> QueryResult<StmtStream<I, QueryStream<I>>> {
        QueryResult::new(self.internal_exec(stmt.into(), params))
    }

    /// Execute a prepared statement and return the affected rows for each resultset
    /// 
    /// If you want to access multiple resultsets, go through [`into_stream`](stmt::ExecResult::into_stream)
    pub fn exec<S: Into<Statement>>(
        self,
        stmt: S,
        params: &[&ToSql],
    ) -> ExecResult<StmtStream<I, ExecFuture<I>>> {
        ExecResult::new(self.internal_exec(stmt.into(), params))
    }

    /// Start a transaction
    pub fn transaction(self) -> Box<Future<Item = Transaction<I>, Error = Error>> {
        Box::new(
            self.simple_exec("set implicit_transactions on")
                .and_then(|(result, conn)| {
                    assert_eq!(result, 0);
                    Ok(new_transaction(conn))
                }),
        )
    }

    /// Create a statement associated to a given SQL which can be executed later on
    ///
    /// This is a lazy operation and will not do anything until the first call.
    /// The statement is prepared with the sql-types of the given parameters.
    /// It will only be reprepared if the given parameter's rust-types resolve to
    /// different sql-types as given for the first execution.
    pub fn prepare<S>(&self, stmt: S) -> Statement
    where
        S: Into<Cow<'static, str>>,
    {
        Statement::new(stmt.into())
    }
}

fn _ensure_sync() {
    fn _ensure<T: Send>() {}
    _ensure::<Error>();
    _ensure::<SqlConnection<Box<BoxableIo>>>();
    _ensure::<stmt::QueryResult<query::ResultSetStream<Box<BoxableIo>, QueryStream<Box<BoxableIo>>>>>();
}

#[cfg(test)]
mod tests {
    use std::env;
    use futures::{future, Future};
    use futures_state_stream::StateStream;
    use tokio;
    use tokio::executor::current_thread;
    use query::ExecFuture;
    use stmt::ExecResult;
    use super::{BoxableIo, SqlConnection, Error, Result, Transaction};

    /// allow to modify the
    pub fn connection_string() -> String {
        // TrustServerCertificate is just for local development, do not use on production!
        env::var("TIBERIUS_TEST_CONNECTION_STRING").unwrap_or(
            "server=tcp:localhost,1433;integratedSecurity=true;TrustServerCertificate=true"
                .to_owned(),
        )
    }

    pub fn new_connection() -> SqlConnection<Box<BoxableIo>> {
        let future = SqlConnection::connect(connection_string().as_str());
        current_thread::block_on_all(future).unwrap()
    }

    #[cfg(windows)]
    #[test]
    fn connect_to_named_instance() {
        let instance_name = env::var("TIBERIUS_TEST_INSTANCE").unwrap_or("MSSQLSERVER".to_owned());
        let conn_str = connection_string().replace(",1433", &format!("\\{}", instance_name));
        let future = SqlConnection::connect(conn_str.as_str());
        let conn = current_thread::block_on_all(future).unwrap();
        let query = conn.simple_query("SELECT 133").for_each(|row| {
            assert_eq!(row.get::<_, i32>(0), 133);
            Ok(())
        });
        current_thread::block_on_all(query).unwrap();
    }

    #[test]
    fn str_to_connect_endpoint() {
        use super::{ConnectTarget, parse_connection_str, AuthMethod};
        let (p, target) = parse_connection_str("server = tcp:127.0.0.1,1234 ; user=\"Test'\"\"User\";password='1''2\"3;4 ' ; integratedSecurity = false")
            .unwrap();

        assert_eq!(target, ConnectTarget::Tcp("127.0.0.1:1234".parse().unwrap()));
        assert_eq!(p.auth, AuthMethod::SqlServer("Test'\"User".into(), "1'2\"3;4 ".into()));
    }

    #[test]
    fn test_threadpool_executor() {
        let future = SqlConnection::connect(connection_string().as_str())
            .and_then(move |conn| conn
                .simple_query("select 18")
                .fold(false, |acc, row| {
                    if row.get::<_, i32>(0) == 17 { 
                        future::ok::<_, Error>(true)
                    } else { 
                        future::ok(acc)
                    }
                }, |success, _| future::ok::<_, Error>(success)))
            .map(|success| assert!(success))
            .map_err(|err| panic!("err: {:?}", err));
        tokio::run(future);
    }

    #[test]
    fn simple_select() {
        let c1 = new_connection();

        let limit = 5i32;
        let post_sql = format!("where II<{}", limit);
        let sql = (1..2 * limit).fold("select II FROM (select 0 as II ".to_owned(), |acc, x| {
            acc + &format!("union select {} ", x)
        }) + ") U " + &post_sql;
        let query = c1.simple_query(sql);
        let mut i = 0;
        {
            let future = query.for_each(|x| {
                let val: i32 = x.get("II");
                assert_eq!(val, i);
                i += 1;
                Ok(())
            });
            current_thread::block_on_all(future).unwrap();
        }
        assert_eq!(i, limit);
    }

    #[test]
    fn nbcrow() {
        let c1 = new_connection();
        let sql = "select null, null, null, null, 1, null, null, null, 2, null, null, 3, null, 4";

        let query = c1.simple_query(sql);
        let future = query.for_each(|x| {
            let expected_results: Vec<Option<i32>> = vec![
                None,
                None,
                None,
                None,
                Some(1),
                None,
                None,
                None,
                Some(2),
                None,
                None,
                Some(3),
                None,
                Some(4),
            ];
            let results: Vec<Option<i32>> = (0..expected_results.len()).map(|i| x.get(i)).collect();
            assert_eq!(results, expected_results);
            Ok(())
        });
        current_thread::block_on_all(future).unwrap();
    }

    fn helper_ddl_exec<I: BoxableIo, R: StateStream<Item = ExecFuture<I>, State = SqlConnection<I>, Error = Error>>(
        exec: ExecResult<R>,
    ) {
        let mut i = 0;
        {
            let future = exec.into_stream().and_then(|x| x).for_each(|_| {
                // This is executed for EACH resultset (e.g. 2 times for 2 sql queries)
                i += 1;
                Ok(())
            });
            current_thread::block_on_all(future).unwrap();
        }
        assert_eq!(i, 1);
    }

    #[test]
    fn row_recv_across_packets() {
        let c1 = new_connection();

        let future = c1.simple_query("select SPACE(8000)").for_each(|row| {
            assert_eq!(
                row.get::<_, &str>(0).as_bytes(),
                vec![b' '; 8000].as_slice()
            );
            Ok(())
        });
        current_thread::block_on_all(future).unwrap();
    }

    #[test]
    fn rows_recv_across_packets() {
        let c1 = new_connection();

        let future = c1.simple_query("select SPACE(8000), SPACE(8000)")
            .for_each(|row| {
                assert_eq!(
                    row.get::<_, &str>(0).as_bytes(),
                    vec![b' '; 8000].as_slice()
                );
                assert_eq!(
                    row.get::<_, &str>(1).as_bytes(),
                    vec![b' '; 8000].as_slice()
                );
                Ok(())
            });
        current_thread::block_on_all(future).unwrap();
    }

    #[test]
    fn tokenstream_send_across_packets() {
        let c1 = new_connection();

        let val = format!("x{:04500}x", 0);
        let input = format!("select '{}'", &val);
        let future = c1.query(input.clone(), &[]).for_each(|row| {
            assert_eq!(row.get::<_, &str>(0), val.as_str());
            Ok(())
        });
        let c1 = current_thread::block_on_all(future).unwrap();
        let future = c1.simple_query(input.clone()).for_each(|row| {
            assert_eq!(row.get::<_, &str>(0), val.as_str());
            Ok(())
        });
        current_thread::block_on_all(future).unwrap();
    }

    #[test]
    fn prepared_ddl_exec() {
        let c1 = new_connection();
        let stmt = c1.prepare("DECLARE @Mojo int");
        helper_ddl_exec(c1.exec(&stmt, &[]));
    }

    #[test]
    fn prepared_select_reexecute() {
        let mut conn = new_connection();

        // prepare once
        let sql = (1..10)
            .map(|_| "SELECT 1")
            .collect::<Vec<_>>()
            .join(" UNION ");
        let query = conn.prepare(sql);

        // prepare & execute
        for _ in 0..2 {
            let query = conn.query(&query, &[]);
            let future = query.for_each(|x| {
                let _: i32 = x.get(0);
                Ok(())
            });
            conn = current_thread::block_on_all(future).unwrap();
        }
    }

    #[test]
    fn prepared_select_reexecute_intermediate_query() {
        let conn = new_connection();
        let job = conn.query("SELECT [uid] FROM (select cast(1 as bigint) as uid) b", &[])
            .for_each(|_| Ok(()))
            .and_then(|conn| conn.query("SELECT 0", &[]).for_each(|_| Ok(())))
            .and_then(|conn| {
                conn.query("SELECT [uid] FROM (select cast(1 as bigint) as uid) b", &[])
                    .for_each(|_| Ok(()))
            });
        current_thread::block_on_all(job).unwrap();
    }

    #[test]
    fn prepared_select_empty_resultset() {
        let c1 = new_connection();

        let future = c1.query("SELECT TOP 0 NULL AS MyValue", &[])
            .for_each(|_| unreachable!());
        current_thread::block_on_all(future).unwrap();
    }

    #[test]
    fn ddl_exec() {
        let c1 = new_connection();
        helper_ddl_exec(c1.simple_exec("DECLARE @Mojo int"));
    }

    /// This test tests that the old value is returned after a rollback and the new value after a commit
    /// It also checks if changing the value generally works
    /// (1 check within the transaction and 1 after the rollback/commit, 4 checks in sum)
    #[test]
    fn transaction() {
        let connection_string = connection_string();

        fn check_test_value<I: BoxableIo + 'static>(
            conn: SqlConnection<I>,
            value: i32,
        ) -> Box<Future<Item = SqlConnection<I>, Error = Error>> {
            Box::new(conn.simple_query("SELECT test FROM #temp;").for_each(
                move |row| {
                    let val: i32 = row.get(0);
                    assert_eq!(val, value);
                    Ok(())
                },
            ))
        }

        fn update_test_value<I: BoxableIo + 'static>(
            transaction: Transaction<I>,
            commit: bool,
        ) -> Box<Future<Item = SqlConnection<I>, Error = Error>> {
            Box::new(
                transaction
                    .simple_exec("UPDATE #Temp SET test=44;")
                    .and_then(|(_, trans)| {
                        trans
                            .simple_query("SELECT test FROM #Temp;")
                            .for_each(|row| {
                                let val: i32 = row.get(0);
                                assert_eq!(val, 44i32);
                                Ok(())
                            })
                    })
                    .and_then(move |trans| if commit {
                        trans.commit()
                    } else {
                        trans.rollback()
                    }),
            )
        }

        let mut amount = 0;
        {
            let future = SqlConnection::connect(connection_string.as_str())
                .and_then(|conn| {
                    conn.simple_exec("CREATE TABLE #Temp(test int);INSERT INTO #Temp(test) VALUES (42);")
                        .into_stream()
                        .and_then(|future| future)
                        .for_each(|_| { amount += 1; Ok(()) })
                })
                .and_then(|conn| conn.transaction())
                .and_then(|trans| update_test_value(trans, false))
                .and_then(|conn| check_test_value(conn, 42))
                .and_then(|conn| conn.transaction())
                .and_then(|trans| update_test_value(trans, true))
                .and_then(|conn| check_test_value(conn, 44));
            current_thread::block_on_all(future).unwrap();
        };
        
        assert_eq!(amount, 2);
    }

    #[test]
    fn test_bug_canceled() {
        let connection_string = connection_string();

        fn check_test_value<I: BoxableIo + 'static>(
            conn: SqlConnection<I>,
        ) -> Box<Future<Item = SqlConnection<I>, Error = Error>> {
            Box::new(
                conn.simple_query("SELECT test FROM #temp;")
                    .for_each(move |row| {
                        let val: Option<f64> = row.get(0);
                        assert!(val.is_none());
                        Ok(())
                    }),
            )
        }

        let mut amount = 0;
        {
            let future = SqlConnection::connect(connection_string.as_str())
            .and_then(|conn| {
                conn.simple_exec("CREATE TABLE #Temp(test [decimal](19, 4) NULL);INSERT INTO #Temp(test) VALUES (NULL);")
                    .into_stream()
                    .and_then(|future| future)
                    .for_each(|_| { amount += 1; Ok(()) })
            })
            .and_then(|conn| check_test_value(conn));

            current_thread::block_on_all(future).unwrap();
        }
        assert_eq!(amount, 2);
    }

    #[cfg(feature = "chrono")]
    #[test]
    fn test_bug_65() {
        let connection_string = connection_string();

        let mut amount = 0;
        {
            let future = SqlConnection::connect(connection_string.as_str()).and_then(|conn| {
                let some_date: Option<chrono::NaiveDate> = None;
                conn.exec(
                    "CREATE TABLE #Temp(test [date] NULL);INSERT INTO #Temp(test) VALUES (@P1);",
                    &[&some_date],
                )
                .into_stream()
                .and_then(|future| future)
                .for_each(|_| {
                    amount += 1;
                    Ok(())
                })
            });

            current_thread::block_on_all(future).unwrap();
        }
        assert_eq!(amount, 2);
    }

    #[test]
    fn todo_doctest() {
        let connection_string = connection_string();

        let future = SqlConnection::connect(connection_string.as_str())
            .and_then(|conn| {
                ::futures::finished((conn.prepare("SELECT @P1 + @P2"), conn))
            })
            .and_then(|(stmt, conn)| {
                fn handle_row(row: ::query::QueryRow) -> Result<()> {
                    let val: i32 = row.get(0);
                    assert_eq!(val, 3i32);
                    Ok(())
                }
                // TODO: figure out some syntax sugar here, -> doc tests
                conn.query(&stmt, &[&1i32, &2i32])
                    .for_each(handle_row)
                    .map(|conn| (conn, stmt))
                    .and_then(|(conn, stmt)| {
                        conn.query(&stmt, &[&2i32, &1i32])
                            .for_each(handle_row)
                            .map(|conn| (conn, stmt))
                    })
                    .and_then(|(conn, stmt)| {
                        conn.query(&stmt, &[&4i32, &-1i32]).for_each(handle_row)
                    })
                    .map(|x| x.0)
            });
        current_thread::block_on_all(future).unwrap();
    }
}
