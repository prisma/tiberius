//! A pure-rust TDS implementation for Microsoft SQL Server (>=2008)
//!
//! # A simple example
//! **Warning:** Do not use `simple_query` with user-specified data. Resort to prepared statements for that.
//!
//! ```rust
//! extern crate futures;
//! extern crate tokio_core;
//! extern crate tiberius;
//! use futures::Future;
//! use tokio_core::reactor::Core;
//! use tiberius::SqlConnection;
//! use tiberius::stmt::ResultStreamExt;
//!
//! fn main() {
//!    let mut lp = Core::new().unwrap();
//!    // 1: for windows we demonstrate the hardcoded variant
//!    // which is equivalent to:
//!    //     let conn_str = "server=tcp:localhost,1433;integratedSecurity=true;";
//!    //     let future = SqlConnection::connect(lp.handle(), conn_str).and_then(|conn| {
//!    // and for linux we use the connection string from an environment variable
//!    let conn_str = if cfg!(windows) {
//!        "server=tcp:localhost,1433;integratedSecurity=true;".to_owned()
//!    } else {
//!        ::std::env::var("TIBERIUS_TEST_CONNECTION_STRING").unwrap()
//!    };
//!
//!    let future = SqlConnection::connect(lp.handle(), conn_str.as_str())
//!        .and_then(|conn| {
//!            conn.simple_query("SELECT 1+2").for_each_row(|row| {
//!                let val: i32 = row.get(0);
//!                assert_eq!(val, 3i32);
//!                Ok(())
//!            })
//!        })
//!        .and_then(|conn| conn.simple_exec("create table #Temp(gg int);").single())
//!        .and_then(|(_, conn)| conn.simple_exec("UPDATE #Temp SET gg=1 WHERE gg=1").single());
//!    lp.run(future).unwrap();
//! }
//! ```
//!
//!
//! # Prepared Statements
//! Parameters use numeric indexes such as @P1, @P2 for the n-th parameter (starting with 1 for the first)
//!
//! ```rust
//! extern crate futures;
//! extern crate tokio_core;
//! extern crate tiberius;
//! use futures::Future;
//! use tokio_core::reactor::Core;
//! use tiberius::SqlConnection;
//! use tiberius::stmt::ResultStreamExt;
//!
//! fn main() {
//!    let mut lp = Core::new().unwrap();
//!    // 1: Same as in the example above
//!    let conn_str = if cfg!(windows) {
//!        "server=tcp:localhost,1433;integratedSecurity=true;".to_owned()
//!    } else {
//!        ::std::env::var("TIBERIUS_TEST_CONNECTION_STRING").unwrap()
//!    };
//!
//!    let future = SqlConnection::connect(lp.handle(), conn_str.as_str()).and_then(|conn| {
//!        conn.query("SELECT x FROM (VALUES (1),(2),(3),(4)) numbers(x) WHERE x%@P1=@P2",
//!            &[&2i32, &0i32]).for_each_row(|row| {
//!            let val: i32 = row.get(0);
//!            assert_eq!(val % 2, 0i32);
//!            Ok(())
//!        })
//!    });
//!    lp.run(future).unwrap();
//! }
//! ```
//! If you intend to execute the same statement multiple times for the same connection, you should use `.prepare`.
//! For most cases you'll want this though.

#[macro_use]
extern crate bitflags;
extern crate bytes;
extern crate byteorder;
extern crate encoding;
extern crate fnv;
#[macro_use]
extern crate futures;
extern crate futures_state_stream;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate tokio_core;
extern crate tokio_io;
extern crate winauth;

use std::borrow::Cow;
use std::convert::From;
use std::cmp;
use std::net::{SocketAddr, ToSocketAddrs};
use std::marker::PhantomData;
use std::mem;
use std::sync::{Arc, Mutex};
use std::io;
use fnv::FnvHashMap;
use futures::{Async, BoxFuture, Future, Poll, Sink};
use futures::sync::oneshot;
use futures::future::FromErr;
use tokio_core::net::{TcpStream, TcpStreamNew};
use tokio_core::reactor::Handle;

/// Trait to convert a u8 to a `enum` representation
trait FromUint where Self: Sized {
    fn from_u8(n: u8) -> Option<Self>;
    fn from_u32(n: u32) -> Option<Self>;
}

macro_rules! uint_to_enum {
    ($ty:ident, $($variant:ident),*) => {
        impl FromUint for $ty {
            fn from_u8(n: u8) -> Option<$ty> {
                // this should get stripped on compilation and is only used
                // to ensure all enum variants are passed to this macro
                fn _static_verification(t: $ty) -> bool {
                    match t {
                        $( $ty::$variant => true, )*
                    }
                }

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
mod protocol;
mod types;
mod tokens;
pub mod query;
pub mod stmt;
mod transaction;

use transport::{Io, TdsTransport, TransportStream};
use protocol::{PacketType, PreloginMessage, LoginMessage, SspiMessage, SerializeMessage, UnserializeMessage};
use types::{ColumnData, ToSql};
use tokens::{TdsResponseToken, RpcParam, RpcProcIdValue, RpcProcId, RpcOptionFlags, RpcStatusFlags, TokenRpcRequest, WriteToken};
use query::{ResultSetStream, QueryStream, ExecFuture};
use stmt::{Statement, StmtStream, ResultStreamExt};
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
        .fold(0u64, |acc, part| acc | (part.1.parse::<u64>().unwrap() << (part.0*8)))
}

/// A unified error enum that contains several errors that might occurr during the lifecycle of this driver
#[derive(Debug)]
pub enum TdsError {
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

impl From<io::Error> for TdsError {
    fn from(err: io::Error) -> TdsError {
        TdsError::Io(err)
    }
}

impl From<std::num::ParseIntError> for TdsError {
    fn from(err: std::num::ParseIntError) -> TdsError {
        TdsError::ParseInt(err)
    }
}

impl From<std::str::Utf8Error> for TdsError {
    fn from(err: std::str::Utf8Error) -> TdsError {
        TdsError::Utf8(err)
    }
}

impl From<std::string::FromUtf16Error> for TdsError {
    fn from(err: std::string::FromUtf16Error) -> TdsError {
        TdsError::Utf16(err)
    }
}

pub type TdsResult<T> = Result<T, TdsError>;

/// A connection in a state before any login has happened
#[doc(hidden)]
enum SqlConnectionNewState<I: Io> {
    PreLoginSend,
    PreLoginRecv,
    #[cfg(feature = "tls")]
    TLSPending(Option<transport::tls::ConnectAsync<transport::tls::TlsTdsWrapper<I>>>),
    LoginSend,
    LoginRecv,
    TokenStreamRecv,
    TokenStreamSend,
    _Dummy(PhantomData<I>)
}

/// A representation of the initialization state of an SQL connection (pending connection)
#[doc(hidden)]
pub enum SqlConnectionNew<I: BoxableIo, F: Future<Item=I, Error=TdsError> + Send + Sized> {
    Connection(Option<(F, ConnectParams)>),
    Error(Option<TdsError>),
    Next(Option<SqlConnectionFuture<I>>),
}

impl<I: BoxableIo, F: Future<Item=I, Error=TdsError> + Send> Future for SqlConnectionNew<I, F> {
    type Item = SqlConnection<I>;
    type Error = TdsError;

    fn poll(&mut self) -> Poll<Self::Item, TdsError> {
        loop {
            *self = match *self {
                SqlConnectionNew::Connection(ref mut pairs @ Some(_)) => {
                    let trans = try_ready!(pairs.as_mut().map(|x| &mut x.0).unwrap().poll());
                    #[cfg(feature = "tls")]
                    let trans = TransportStream::Raw(trans);
                    let trans = TdsTransport::new(trans);

                    let future = SqlConnectionFuture {
                        params: pairs.take().map(|x| x.1).unwrap(),
                        state: SqlConnectionNewState::PreLoginSend,
                        transport: trans,
                        bindings: Arc::new(Mutex::new(None)),
                        wauth_client: None,
                    };
                    SqlConnectionNew::Next(Some(future))
                },
                SqlConnectionNew::Error(ref mut e @ Some(_)) => {
                    return Err(e.take().unwrap())
                },
                SqlConnectionNew::Next(ref mut future @ Some(_)) => {
                    try_ready!(future.as_mut().unwrap().poll());
                    let trans = future.take().unwrap().transport;
                    let conn = InnerSqlConnection {
                        transport: trans,
                        stmts: FnvHashMap::default(),
                    };

                    return Ok(Async::Ready(SqlConnection(conn)))
                },
                _ => panic!("SqlConnectionNew polled multiple times. item already consumed"),
            }
        }
    }
}

#[doc(hidden)]
pub struct SqlConnectionFuture<I: BoxableIo> {
    params: ConnectParams,
    state: SqlConnectionNewState<I>,
    transport: TdsTransport<TransportStream<I>>,
    /// receiver for channel bindings certificate hash (TLS)
    bindings: Arc<Mutex<Option<Vec<u8>>>>,
    wauth_client: Option<Box<NextBytes>>,
}

impl<I: BoxableIo> SqlConnectionFuture<I> {
    /// Queues a simple message which serializes to ONE packet
    pub fn queue_simple_message<M: SerializeMessage>(&mut self, m: M) -> io::Result<()> {
        let vec = try!(m.serialize_message(&mut self.transport));
        self.transport.inner.queue_vec(vec)
    }
}

impl<I: BoxableIo> Future for SqlConnectionFuture<I> {
    type Item = ();
    type Error = TdsError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            self.state = match self.state {
                SqlConnectionNewState::PreLoginSend => {
                    let mut msg = PreloginMessage::new();
                    if cfg!(feature = "tls") {
                        msg.encryption = self.params.ssl;
                    } else if self.params.ssl != EncryptionLevel::NotSupported {
                        panic!("TLS support is not enabled in this build, but required for this configuration!");
                    }
                    try_nb!(self.queue_simple_message(msg));
                    SqlConnectionNewState::PreLoginRecv
                },
                SqlConnectionNewState::PreLoginRecv => {
                    try_ready!(self.transport.inner.poll_complete());
                    let header = try_ready!(self.transport.inner.next_packet());
                    assert_eq!(header.ty, PacketType::TabularResult);
                    // parse the prelogin packet
                    let buf = self.transport.inner.get_packet(header.length as usize);
                    let msg = try!(buf.as_ref().unserialize_message(&mut self.transport));

                    let encr = match (self.params.ssl, msg.encryption) {
                        (EncryptionLevel::NotSupported, EncryptionLevel::NotSupported) => EncryptionLevel::NotSupported,
                        (EncryptionLevel::Off, EncryptionLevel::Off) => EncryptionLevel::Off,
                        (EncryptionLevel::On, EncryptionLevel::Off) |
                        (EncryptionLevel::On, EncryptionLevel::NotSupported) => panic!("todo: terminate connection, invalid encryption"),
                        (_, _) => EncryptionLevel::On,
                    };
                    self.params.ssl = encr;

                    // move to an TLS stream, if requested
                    match encr {
                        // encrypt entirely or only logon packet
                        EncryptionLevel::On | EncryptionLevel::Off | EncryptionLevel::Required => {
                            #[cfg(feature = "tls")]
                            {
                                match mem::replace(&mut self.transport.inner.io, TransportStream::None) {
                                    TransportStream::Raw(stream) => {
                                        let wrapped_stream = transport::tls::TlsTdsWrapper::new(stream);
                                        let host = if self.params.trust_cert {
                                            None
                                        } else {
                                            Some(&*self.params.host)
                                        };
                                        let bindings = self.bindings.clone();
                                        let tls_stream = transport::tls::connect_async(wrapped_stream, host, move |_| {
                                            *bindings.lock().unwrap() = Some(
                                                // MSSQL requires "tls-unique" bindings and doesn't support
                                                // "tls-server-end-point" anymore (even if the docs hints otherwise)
                                                [b"tls-unique:" as &[u8], unimplemented!()
                                            ].concat());
                                        });
                                        SqlConnectionNewState::TLSPending(Some(tls_stream))
                                    },
                                    _ => unreachable!(),
                                }
                            }
                            #[cfg(not(feature = "tls"))]
                            panic!("encryption requested without build support!");
                        }
                        // do not encrypt at all
                        EncryptionLevel::NotSupported => SqlConnectionNewState::LoginSend
                    }
                },
                #[cfg(feature = "tls")]
                SqlConnectionNewState::TLSPending(ref mut connect_async) => {
                    assert!(connect_async.is_some());
                    let mut stream = try_ready!(connect_async.as_mut().unwrap().poll());
                    connect_async.take();
                    stream.get_mut().get_mut().wrap = false;
                    self.transport.inner.io = TransportStream::TLS(stream);
                    SqlConnectionNewState::LoginSend
                },
                SqlConnectionNewState::LoginSend => {
                    let mut login_message = LoginMessage::new();

                    if let Some(ref db) = self.params.target_db {
                        login_message.db_name = db.clone();
                    }

                    // authentication
                    match self.params.auth {
                        #[cfg(windows)]
                        AuthMethod::SSPI_SSO => {
                            let mut builder = winauth::windows::NtlmSspiBuilder::new()
                                .target_spn(&self.params.spn);
                            if let Some(ref hash) = *self.bindings.lock().unwrap() {
                                builder = builder.channel_bindings(hash);
                            }
                            let mut sso_client = try!(builder.build());
                            let buf = try!(sso_client.next_bytes(None)).map(|x| x.to_owned());
                            login_message.integrated_security = buf;
                            self.wauth_client = Some(Box::new(sso_client));
                        },
                        AuthMethod::SqlServer(ref username, ref password) => {
                            login_message.username = username.clone();
                            login_message.password = password.clone();
                        },
                        AuthMethod::WinAuth(ref username, ref password) => {
                            // TODO: integrate channel binding
                            // TODO: use NEGOTIATE
                            let (domain, username) = if let Some(idx) = username.find("\\") {
                                match *username {
                                    Cow::Borrowed(ref x) => {
                                        let (domain, username) = (&x[..idx], &x[idx+1..]);
                                        (Some(Cow::Borrowed(domain)), username.into())
                                    }
                                    Cow::Owned(ref x) => {
                                        let (domain, username) = (&x[..idx], &x[idx+1..]);
                                        (Some(Cow::Owned(domain.to_owned())), username.to_owned().into())
                                    }
                                }
                            } else {
                                (None, username.clone())
                            };
                            let mut builder = winauth::NtlmV2ClientBuilder::new()
                                                      .target_spn(self.params.spn.clone());
                            if let Some(ref hash) = *self.bindings.lock().unwrap() {
                                builder = builder.channel_bindings(hash);
                            }
                            let mut client = builder.build(domain, username, password.clone());
                            let buf = try!(client.next_bytes(None)).map(|x| x.to_owned());
                            login_message.integrated_security = buf;
                            self.wauth_client = Some(Box::new(client));
                        }
                    }

                    try_nb!(self.queue_simple_message(login_message));
                    SqlConnectionNewState::LoginRecv
                },
                SqlConnectionNewState::LoginRecv => {
                    try_ready!(self.transport.inner.poll_complete());
                    // if login only encryption was negotiated, disable encryption
                    // after we sent the first login packet
                    #[cfg(feature = "tls")]
                    {
                        if self.params.ssl == EncryptionLevel::Off {
                            let stream = mem::replace(&mut self.transport.inner.io, TransportStream::None);
                            self.transport.inner.io = match stream {
                                TransportStream::TLS(mut stream) => TransportStream::Raw(
                                    stream.get_mut().get_mut().take_stream()
                                ),
                                x => x,
                            };
                        }
                    }
                    let header = try_ready!(self.transport.inner.next_packet());
                    assert_eq!(header.ty, PacketType::TabularResult);
                    SqlConnectionNewState::TokenStreamRecv
                },
                SqlConnectionNewState::TokenStreamRecv => {
                    let token = try_ready!(self.transport.next_token());
                    match token {
                        Some(TdsResponseToken::SSPI(bytes)) => {
                            assert!(self.wauth_client.is_some());
                            match try!(self.wauth_client.as_mut().unwrap().next_bytes(Some(bytes.as_ref()))) {
                                Some(bytes) => {
                                    try!(self.queue_simple_message(SspiMessage(bytes)));
                                    SqlConnectionNewState::TokenStreamSend
                                },
                                None => {
                                    self.wauth_client = None;
                                    SqlConnectionNewState::TokenStreamRecv
                                }
                            }
                        },
                        Some(TdsResponseToken::Done(done)) => {
                            // the connection is ready 2 go, we're done with our initialization
                            assert_eq!(done.status, tokens::DoneStatus::empty());
                            break;
                        },
                        Some(_) | None => SqlConnectionNewState::TokenStreamRecv,
                    }
                },
                SqlConnectionNewState::TokenStreamSend => {
                    try_ready!(self.transport.inner.poll_complete());
                    SqlConnectionNewState::TokenStreamRecv
                },
                SqlConnectionNewState::_Dummy(_) => unreachable!(),
            };
        }

        Ok(Async::Ready(()))
    }
}

/// A type which is constructable from a statement as a statement's result
pub trait StmtResult<I: BoxableIo> {
    type Result: Sized;

    fn from_connection(SqlConnection<I>, oneshot::Sender<SqlConnection<I>>) -> Self::Result;
}

/// A representation of an authenticated and ready for use SQL connection
#[doc(hidden)]
pub struct InnerSqlConnection<I: BoxableIo> {
    transport: TdsTransport<TransportStream<I>>,
    stmts: FnvHashMap<String, Vec<(Vec<&'static str>, i32)>>,
}

/// A connection to a SQL server with an underlying IO (e.g. socket)
pub struct SqlConnection<I: BoxableIo>(InnerSqlConnection<I>);

/// A variant of Io which can be boxed to allow dynamic dispatch
pub trait BoxableIo: Io + Send {}
impl<I: Io + Send> BoxableIo for I {}

/// Something that can be converted to an underlying IO
pub trait ToIo<I: BoxableIo + Sized> {
    type Result: Future<Item=I, Error=TdsError> + Send + Sized + 'static;

    fn to_io(self, handle: &Handle) -> Self::Result where Self: Sized;
}

impl<'a> ToIo<TcpStream> for &'a SocketAddr {
    type Result = FromErr<TcpStreamNew, TdsError>;

    fn to_io(self, handle: &Handle) -> Self::Result {
        TcpStream::connect(self, handle).from_err::<TdsError>()
    }
}

#[doc(hidden)]
pub enum DynamicConnectionTarget {
    Tcp(SocketAddr),
}

impl ToIo<Box<BoxableIo>> for DynamicConnectionTarget {
    type Result = BoxFuture<Box<BoxableIo>, TdsError>;

    fn to_io(self, handle: &Handle) -> Self::Result {
        match self {
            DynamicConnectionTarget::Tcp(ref addr) => addr.to_io(handle).map(|x| Box::new(x) as Box<BoxableIo>).boxed(),
        }
    }
}

/// The authentication method that should be used during authentication
#[derive(Debug)]
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

    fn set_spn(&mut self, host: &str, port: u16) {
        if self.spn.is_empty() {
            self.spn = format!("MSSQLSvc/{}:{}", host, port).into();
        }
    }
}

/// A target address and connection settings = all that's required to connect to a SQL server
///
/// # Example
/// This allows to explicitly construct the connection parameters.  
/// This might be useful for connecting to an underlying IO that isn't supported  
/// within a connection string, such as through a custom protocol implementation.  
///
/// (In theory it's slightly more efficient, since it should require less dynamic dispatch
///  but is also less flexible because the connection method has to be known at compile time)
///
/// ```rust,no_run
/// use std::net::SocketAddr;
/// use tiberius::{ConnectParams, EncryptionLevel, SqlConnection};
/// let addr: SocketAddr = "127.0.0.1:1433".parse().unwrap();
/// let params = ConnectParams {
///     ssl: EncryptionLevel::Required,
///     host: "localhost".into(),
///     ..ConnectParams::new()
/// };
/// // WARNING: This is just so this test can compile, actually you'd want a reactor handle here
/// let handle = unsafe { ::std::mem::zeroed() };
/// SqlConnection::connect(handle, (&addr, params));
/// ```
pub struct ConnectEndpoint<I, T: ToIo<I>> where I: BoxableIo + Sized + 'static {
    params: ConnectParams,
    target: T,
    _marker: PhantomData<*const I>,
}

/// Something that's able to construct all that's required to connect to an endpoint
///
/// This is for example implemented for a connection string parser
pub trait ToConnectEndpoint<I, T> where I: BoxableIo + Sized + 'static, T: ToIo<I> {
    fn to_connect_endpoint(self) -> TdsResult<ConnectEndpoint<I, T>>;
}

impl<I: 'static, T: ToIo<I>> ToConnectEndpoint<I, T> for (T, ConnectParams) where I: BoxableIo {
    fn to_connect_endpoint(self) -> TdsResult<ConnectEndpoint<I, T>> {
        Ok(ConnectEndpoint {
            params: self.1,
            target: self.0,
            _marker: PhantomData
        })
    }
}

/// Parse connection strings
/// https://msdn.microsoft.com/de-de/library/system.data.sqlclient.sqlconnection.connectionstring(v=vs.110).aspx
impl<'a> ToConnectEndpoint<Box<BoxableIo>, DynamicConnectionTarget> for &'a str {
    fn to_connect_endpoint(self) -> TdsResult<ConnectEndpoint<Box<BoxableIo>, DynamicConnectionTarget>>
    {
        let mut input = &self[..];

        let mut target: Option<DynamicConnectionTarget> = None;
        let mut connect_params = ConnectParams::new();

        while !input.is_empty() {
            let key_end = input.bytes().position(|x| x == b'=');
            let (key, key_end) = match key_end {
                None => return Err(TdsError::Conversion("connection string expected key. expected `=` never found".into())),
                Some(key_end) => ((&input[..key_end]).to_lowercase(), key_end)
            };
            input = &input[key_end+1..];
            // check if an escaped value (e.g. 'my password contains a;' or 'or a \' #kappa' follows)
            let escaped = input.starts_with('\'');
            let end = input.bytes().position(|x| x == b';').unwrap_or(input.len());

            let value = if !escaped {
                let ret = &input[..end];
                input = &input[cmp::min(end + 1, input.len())..];
                Cow::Borrowed(ret)
            } else {
                let mut val = String::with_capacity(end);
                loop {
                    let next = match input.bytes().position(|x| x == b'\'' || x == b'\\') {
                        None => return Err(TdsError::Conversion("connection string: unterminated escape sequence".into())),
                        Some(next) => next
                    };
                    match input.as_bytes()[next] {
                        b'\'' => {
                            val.push_str(&input[..next]);
                            break;
                        },
                        b'\\' if next+1 < input.len() && input.as_bytes()[next+1] == b'\'' => {
                            val.push('\'');
                            input = &input[1..]; // <=> &[next+2..] effectively
                        },
                        b'\\' => val.push('\\'),
                        _ => unreachable!()
                    }
                    input = &input[next+1..];
                }
                Cow::Owned(val)
            };

            match key.as_str() {
                "server" => {
                    if value.starts_with("tcp:") {
                        let parts: Vec<_> = value[4..].split(',').collect();
                        assert!(parts.len() <= 2 && !parts.is_empty());
                        connect_params.host = parts[0].to_owned().into();
                        let (host, port) = (parts[0], parts[1].parse::<u16>()?);
                        connect_params.set_spn(&host, port);
                        let addr = match (host, port).to_socket_addrs()?.nth(0) {
                            None => return Err(TdsError::Conversion("connection string: could not resolve server address".into())),
                            Some(x) => x,
                        };
                        target = Some(DynamicConnectionTarget::Tcp(addr));
                    }
                },
                "integratedsecurity" if ["true", "yes", "sspi"].contains(&value.to_lowercase().as_str()) => {
                    #[cfg(windows)] {
                        connect_params.auth = AuthMethod::SSPI_SSO;
                    }
                    #[cfg(not(windows))] {
                        connect_params.auth = AuthMethod::WinAuth("".into(), "".into());
                    }
                },
                "uid" | "username" | "user" => {
                    connect_params.auth = match connect_params.auth {
                        AuthMethod::SqlServer(ref mut username, _) | AuthMethod::WinAuth(ref mut username, _) => {
                            *username = value.into_owned().into();
                            continue;
                        },
                        #[cfg(windows)]
                        AuthMethod::SSPI_SSO => AuthMethod::WinAuth(value.into_owned().into(), "".into()),
                    };
                },
                "password" | "pwd" => {
                    connect_params.auth = match connect_params.auth {
                        AuthMethod::SqlServer(_, ref mut password) | AuthMethod::WinAuth(_, ref mut password) => {
                            *password = value.into_owned().into();
                            continue;
                        },
                        #[cfg(windows)]
                        AuthMethod::SSPI_SSO => AuthMethod::WinAuth("".into(), value.into_owned().into()),
                    };
                },
                "database" => {
                    connect_params.target_db = Some(value.into_owned().into());
                },
                "trustservercertificate" if ["true", "yes"].contains(&value.to_lowercase().as_str()) => {
                    connect_params.trust_cert = true;
                },
                "encrypt" if ["true", "yes"].contains(&value.to_lowercase().as_str()) => {
                    connect_params.ssl = EncryptionLevel::Required;
                },
                _ => return Err(TdsError::Conversion(format!("connection string: unknown config option: {:?}", key).into())),
            }
        }
        if target.is_none() {
            return Err(TdsError::Conversion("connection string pointing into the void. no connection endpoint specified.".into()))
        }
        let endpoint = ConnectEndpoint {
            params: connect_params,
            target: target.unwrap(),
            _marker: PhantomData,
        };
        Ok(endpoint)
    }
}

impl<I: BoxableIo + Sized + 'static> SqlConnection<I> {
    /// Naive connection function for the SQL client
    pub fn connect<E, T: ToIo<I>>(handle: Handle, endpoint: E) -> SqlConnectionNew<I, T::Result>
        where E: ToConnectEndpoint<I, T>
    {
        let ConnectEndpoint { target, params, .. } = match endpoint.to_connect_endpoint() {
            Err(x) => return SqlConnectionNew::Error(Some(x)),
            Ok(x) => x,
        };
        let stream = target.to_io(&handle);
        SqlConnectionNew::Connection(Some((stream, params)))
    }

    fn queue_sql_batch<'a, S>(&mut self, stmt: S) -> TdsResult<()> where S: Into<Cow<'a, str>> {
        let sql = stmt.into();
        let inner = &mut self.0;

        let batch_packet = try!(protocol::build_sql_batch(&mut inner.transport, &sql));
        try!(inner.transport.inner.queue_vec(batch_packet));

        // attempt to send right now, if it works great, if not ready yet, it will be done later
        // simply ensures that data is sent as fast as possible
        let _ = try!(inner.transport.inner.poll_complete());
        Ok(())
    }

    fn simple_exec_internal<'a, Q, R: StmtResult<I>>(mut self, query: Q) -> ResultSetStream<I, R> where Q: Into<Cow<'a, str>> {
        let result = self.queue_sql_batch(query);

        let ret = ResultSetStream::new(self);
        if let Err(err) = result {
            return ret.error(err)
        }
        ret
    }

    /// Execute a simple query and return multiple resultsets which consist of multiple rows.
    /// Usually only one resultset will be interesting for which you can use
    /// [`for_each_row`](struct.ResultSetStream.html#method.for_each_row)
    ///
    /// # Warning
    /// Do not use this with any user specified input.
    /// Please resort to prepared statements in order to prevent SQL-Injections.
    pub fn simple_query<'a, Q>(self, query: Q) -> ResultSetStream<I, QueryStream<I>> where Q: Into<Cow<'a, str>> {
        self.simple_exec_internal(query)
    }

    /// Execute a simple SQL-statement and return the affected rows
    ///
    /// # Warning
    /// Do not use this with any user specified input.
    /// Please resort to prepared statements in order to prevent SQL-Injections.
    /// You can access each resultset (in most cases only one) using [`for_each`](struct.ResultSetStream.html#method.for_each)
    pub fn simple_exec<'a, Q>(self, query: Q) -> ResultSetStream<I, ExecFuture<I>> where Q: Into<Cow<'a, str>> {
        self.simple_exec_internal(query)
    }

    fn do_prepare_exec<'b>(&self, stmt: &Statement, params: &'b [&'b ToSql]) -> TokenRpcRequest<'b> {
        let mut param_str = String::with_capacity(10 * params.len());

        let mut params_meta = vec![
            RpcParam {
                name: Cow::Borrowed("handle"),
                flags: tokens::RPC_PARAM_BY_REF_VALUE,
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
            }
        ];

        // determine the types from the given params
        for (i, param) in params.iter().enumerate() {
            if i > 0 {
                param_str.push(',')
            }
            param_str.push_str(&format!("@P{} ", i + 1));
            param_str.push_str(param.to_sql());

            params_meta.push(RpcParam {
                name: Cow::Owned(format!("@P{}", i+1)),
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
                name: Cow::Owned(format!("@P{}", i+1)),
                flags: RpcStatusFlags::empty(),
                value: param.to_column_data(),
            });
        }

        TokenRpcRequest {
            proc_id: RpcProcIdValue::Id(RpcProcId::SpExecute),
            flags: tokens::RPC_NO_META,
            params: params_meta,
        }
    }

    fn internal_exec<R: StmtResult<I>>(mut self, stmt: Statement, params: &[&ToSql]) -> StmtStream<I, R> {
        // call sp_prepare (with valid handle) or sp_prepexec (initializer)
        let req = if let Some(handle) = stmt.get_handle_for(&self, &params.iter().map(|x| x.to_sql()).collect::<Vec<_>>()) {
            self.do_exec(handle, params)
        } else {
            self.do_prepare_exec(&stmt, params)
        };

        // write everything (or atleast queue it for write)
        let result = req.write_token(&mut self.0.transport);
        let ret = StmtStream::new(self, stmt, params);
        match result {
            Ok(_) => ret,
            Err(err) => ret.error(err),
        }
    }

    /// Execute a prepared statement and return each resultset and their associated rows
    /// Usually only one resultset will be interesting for which you can use
    /// [`for_each_row`](struct.StmtStream.html#method.for_each_row)
    pub fn query<S: Into<Statement>>(self, stmt: S, params: &[&ToSql]) -> StmtStream<I, QueryStream<I>> {
        self.internal_exec(stmt.into(), params)
    }

    /// Execute a prepared statement and return the affected rows for each resultset
    ///
    /// You can access each resultset (in most cases only one) using [`for_each`](struct.StmtStream.html#method.for_each)
    pub fn exec<S: Into<Statement>>(self, stmt: S, params: &[&ToSql]) -> StmtStream<I, ExecFuture<I>> {
        self.internal_exec(stmt.into(), params)
    }

    /// Start a transaction
    pub fn transaction(self) -> BoxFuture<Transaction<I>, TdsError> {
        self.simple_exec("set implicit_transactions on")
            .single()
            .and_then(|(result, conn)| {
                assert_eq!(result, 0);
                Ok(new_transaction(conn))
            })
            .boxed()
    }

    /// Create a statement associated to a given SQL which can be executed later on
    ///
    /// This is a lazy operation and will not do anything until the first call.
    /// The statement is prepared with the sql-types of the given parameters.
    /// It will only be reprepared if the given parameter's rust-types resolve to
    /// different sql-types as given for the first execution.
    pub fn prepare<S>(&self, stmt: S) -> Statement where S: Into<Cow<'static, str>> {
        Statement::new(stmt.into())
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use futures::{Future, BoxFuture};
    use futures_state_stream::StateStream;
    use tokio_core::reactor::Core;
    use query::ExecFuture;
    use stmt::ResultStreamExt;
    use super::{BoxableIo, SqlConnection, Transaction, TdsError};

    /// allow to modify the
    pub fn connection_string() -> String {
        // TrustServerCertificate is just for local development, do not use on production!
        env::var("TIBERIUS_TEST_CONNECTION_STRING")
            .unwrap_or("server=tcp:localhost,1433;integratedSecurity=true;TrustServerCertificate=true".to_owned())
    }

    pub fn new_connection(lp: &mut Core) -> SqlConnection<Box<BoxableIo>> {
        let future = SqlConnection::connect(lp.handle(), connection_string().as_str());
        lp.run(future).unwrap()
    }

    #[test]
    fn simple_select() {
        let mut lp = Core::new().unwrap();
        let c1 = new_connection(&mut lp);

        let limit = 5i32;
        let post_sql = format!("where II<{}", limit);
        let sql = (1..2*limit).fold("select II FROM (select 0 as II ".to_owned(), |acc, x| acc + &format!("union select {} ", x)) + ") U " + &post_sql;
        let query = c1.simple_query(sql);
        let mut i = 0;
        {
            let future = query.for_each_row(|x| {
                let val: i32 = x.get("II");
                assert_eq!(val, i);
                i += 1;
                Ok(())
            });
            lp.run(future).unwrap();
        }
        assert_eq!(i, limit);
    }

    #[test]
    fn nbcrow() {
        let mut lp = Core::new().unwrap();
        let c1 = new_connection(&mut lp);
        let sql = "select null, null, null, null, 1, null, null, null, 2, null, null, 3, null, 4";

        let query = c1.simple_query(sql);
        let future = query.for_each_row(|x| {
            let expected_results: Vec<Option<i32>> = vec![
                None, None, None, None, Some(1),
                None, None, None, Some(2), None, None, Some(3), None, Some(4)
            ];
            let results: Vec<Option<i32>> = (0..expected_results.len()).map(|i| x.get(i)).collect();
            assert_eq!(results, expected_results);
            Ok(())
        });
        lp.run(future).unwrap();
    }

    #[test]
    fn prepared_select_empty_resultset() {
        let mut lp = Core::new().unwrap();
        let c1 = new_connection(&mut lp);

        let future = c1.query("SELECT TOP 0 NULL AS MyValue", &[]).for_each_row(|_| {
            unreachable!()
        });
        lp.run(future).unwrap();
    }

    /*#[test]
    fn prepared_select_reexecute() {
        let mut lp = Core::new().unwrap();
        let mut c1 = new_connection(&mut lp);

        let limit = 5i32;
        let query = (1..2*limit).fold("select II FROM (select 0 as II ".to_owned(), |acc, x| acc + &format!("union select {} ", x)) + ") U where II<@P1";
        let stmt = c1.prepare(query);

        for g in 0..2 {
            lp.run(stmt.query(&[&limit]).for_each_row(|x| {
                let val: i32 = x.get("II");
                assert_eq!(val, i - g*limit);
                Ok(())
            })).unwrap()
        }
        assert_eq!(i, 2*limit);
    }*/

    fn helper_ddl_exec<I: BoxableIo, R: StateStream<Item=ExecFuture<I>, State=SqlConnection<I>, Error=TdsError>>(exec: R, lp: &mut Core) {
        let mut i = 0;
        {
            let future = exec.and_then(|x| x).for_each(|_| {
                // This is executed for EACH resultset (e.g. 2 times for 2 sql queries)
                i += 1;
                Ok(())
            });
            lp.run(future).unwrap();
        }
        assert_eq!(i, 1);
    }

    #[test]
    fn row_recv_across_packets() {
        let mut lp = Core::new().unwrap();
        let c1 = new_connection(&mut lp);

        let future = c1.simple_query("select SPACE(8000)").for_each_row(|row| {
            assert_eq!(row.get::<_, &str>(0).as_bytes(), vec![b' '; 8000].as_slice());
            Ok(())
        });
        lp.run(future).unwrap();
    }

    #[test]
    fn rows_recv_across_packets() {
        let mut lp = Core::new().unwrap();
        let c1 = new_connection(&mut lp);

        let future = c1.simple_query("select SPACE(8000), SPACE(8000)").for_each_row(|row| {
            assert_eq!(row.get::<_, &str>(0).as_bytes(), vec![b' '; 8000].as_slice());
            assert_eq!(row.get::<_, &str>(1).as_bytes(), vec![b' '; 8000].as_slice());
            Ok(())
        });
        lp.run(future).unwrap();
    }

    #[test]
    fn prepared_ddl_exec() {
        let mut lp = Core::new().unwrap();
        let c1 = new_connection(&mut lp);
        let stmt = c1.prepare("DECLARE @Mojo int");
        helper_ddl_exec(c1.exec(&stmt, &[]), &mut lp);
    }

    #[test]
    fn ddl_exec() {
        let mut lp = Core::new().unwrap();
        let c1 = new_connection(&mut lp);
        helper_ddl_exec(c1.simple_exec("DECLARE @Mojo int"), &mut lp);
    }

    /// This test tests that the old value is returned after a rollback and the new value after a commit
    /// It also checks if changing the value generally works
    /// (1 check within the transaction and 1 after the rollback/commit, 4 checks in sum)
    #[test]
    fn transaction() {
        let mut lp = Core::new().unwrap();
        let connection_string = connection_string();

        fn check_test_value<I: BoxableIo + 'static>(conn: SqlConnection<I>, value: i32) -> BoxFuture<SqlConnection<I>, TdsError> {
            conn.simple_query("SELECT test FROM #temp;")
                .for_each_row(move |row| {
                    let val: i32 = row.get(0);
                    assert_eq!(val, value);
                    Ok(())
                })
                .boxed()
        }

        fn update_test_value<I: BoxableIo + 'static>(transaction: Transaction<I>, commit: bool) -> BoxFuture<SqlConnection<I>, TdsError> {
            transaction.simple_exec("UPDATE #Temp SET test=44;").single()
                .and_then(|(_, trans)| trans.simple_query("SELECT test FROM #Temp;").for_each_row(|row| {
                    let val: i32 = row.get(0);
                    assert_eq!(val, 44i32);
                    Ok(())
                }))
                .and_then(move |trans| if commit {
                    trans.commit()
                } else {
                    trans.rollback()
                })
                .boxed()
        }

        let future = SqlConnection::connect(lp.handle(), connection_string.as_str())
            .and_then(|conn| conn.simple_exec("CREATE TABLE #Temp(test int); INSERT INTO #Temp (test) VALUES (42);").and_then(|r| r).collect())
            .and_then(|(_, conn)| conn.transaction())
            .and_then(|trans| update_test_value(trans, false))
            .and_then(|conn| check_test_value(conn, 42))
            .and_then(|conn| conn.transaction())
            .and_then(|trans| update_test_value(trans, true))
            .and_then(|conn| check_test_value(conn, 44));
        lp.run(future).unwrap();
    }

    #[test]
    fn todo_doctest() {
        let mut lp = Core::new().unwrap();
        let connection_string = connection_string();

        let future = SqlConnection::connect(lp.handle(), connection_string.as_str()).and_then(|conn| {
            ::futures::finished((conn.prepare("SELECT @P1 + @P2"), conn))
        }).and_then(|(stmt, conn)| {
            fn handle_row(row: ::query::QueryRow) -> ::TdsResult<()> {
                let val: i32 = row.get(0);
                assert_eq!(val, 3i32);
                Ok(())
            }
            // TODO: figure out some syntax sugar here, -> doc tests
            conn
                .query(&stmt, &[&1i32, &2i32]).for_each_row(handle_row)
                .map(|conn| (conn, stmt))
                .and_then(|(conn, stmt)| conn.query(&stmt, &[&2i32, &1i32]).for_each_row(handle_row).map(|conn| (conn, stmt)))
                .and_then(|(conn, stmt)| conn.query(&stmt, &[&4i32, &-1i32]).for_each_row(handle_row))
                .map(|x| x.0)
        });
        lp.run(future).unwrap();
    }
}
