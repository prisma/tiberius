//! A pure-rust TDS implementation for Microsoft SQL Server (>=2008)

#[macro_use]
extern crate bitflags;
extern crate byteorder;
extern crate encoding;
#[macro_use]
extern crate futures;
extern crate futures_state_stream;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
#[macro_use]
extern crate tokio_core;

use std::borrow::Cow;
use std::cell::RefCell;
use std::convert::From;
use std::net::{SocketAddr, ToSocketAddrs};
use std::marker::PhantomData;
use std::ops::Deref;
use std::io;
use futures::{Async, BoxFuture, Future, Poll, Sink};
use futures::sync::oneshot;
use futures::future::FromErr;
use tokio_core::io::Io;
use tokio_core::net::{TcpStream, TcpStreamNew};
use tokio_core::reactor::Handle;

/// Trait to convert a u8 to a `enum` representation
trait FromUint where Self: Sized {
    fn from_u8(n: u8) -> Option<Self>;
    fn from_u32(n: u32) -> Option<Self>;
}

/// if the underlying buffer has not enough content yet, transform that error into a `None`
/// return value
macro_rules! try_eof {
    ($e:expr) => (match $e {
        Err(ref e) if e.kind() == ::std::io::ErrorKind::UnexpectedEof => {
            return Ok(None)
        },
        Err(e) => return Err(e),
        Ok(x) => x,
    })
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
mod ntlm;
mod protocol;
mod types;
mod tokens;
mod query;
mod stmt;

use transport::TdsTransport;
use protocol::{PacketType, PreloginMessage, LoginMessage, SspiMessage, SerializeMessage, UnserializeMessage};
use types::{ColumnData, ToSql};
use tokens::{TdsResponseToken, RpcParam, RpcProcIdValue, RpcProcId, RpcOptionFlags, RpcStatusFlags, TokenRpcRequest, WriteToken};
use query::{ResultSetStream, QueryStream, ExecFuture};
use stmt::{Statement, StmtStream};

lazy_static! {
    pub static ref DRIVER_VERSION: u64 = get_driver_version();
}

fn get_driver_version() -> u64 {
    env!("CARGO_PKG_VERSION")
        .splitn(6, '.')
        .enumerate()
        .fold(0u64, |acc, part| acc | (part.1.parse::<u64>().unwrap() << part.0*8))
}

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

/// a connection in a state before any login has happened
enum SqlConnectionNewState {
    PreLoginSend,
    PreLoginRecv,
    LoginSend,
    LoginRecv,
    TokenStreamRecv,
    TokenStreamSend,
}

/// a representation of the initialization state of an SQL connection (pending authentication)
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
                    let future = SqlConnectionFuture {
                        params: pairs.take().map(|x| x.1).unwrap(),
                        state: SqlConnectionNewState::PreLoginSend,
                        transport: TdsTransport::new(trans),
                        sso_client: None,
                    };
                    SqlConnectionNew::Next(Some(future))
                },
                SqlConnectionNew::Error(ref mut e @ Some(_)) => {
                    return Err(e.take().unwrap())
                },
                SqlConnectionNew::Next(ref mut future @ Some(_)) => {
                    let _ = try_ready!(future.as_mut().unwrap().poll());
                    let trans = future.take().unwrap().transport;
                    let conn = InnerSqlConnection {
                        transport: trans,
                    };

                    return Ok(Async::Ready(SqlConnection(RefCell::new(conn))))
                },
                _ => panic!("SqlConnectionNew polled multiple times. item already consumed"),
            }
        }
    }
}

#[doc(hidden)]
pub struct SqlConnectionFuture<I: BoxableIo> {
    params: ConnectParams,
    state: SqlConnectionNewState,
    transport: TdsTransport<I>,
    sso_client: Option<ntlm::sso::NtlmSso>,
}

impl<I: BoxableIo> SqlConnectionFuture<I> {
    /// queues a simple message which serializes to ONE packet
    pub fn queue_simple_message<M: SerializeMessage>(&mut self, m: M) -> io::Result<()> {
        let vec = try!(m.serialize_message(&mut self.transport));
        self.transport.queue_vec(vec)
    }
}

impl<I: BoxableIo> Future for SqlConnectionFuture<I> {
    type Item = ();
    type Error = TdsError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        trace!("init/poll");

        loop {
            match self.state {
                SqlConnectionNewState::PreLoginSend => {
                    trace!("init/send prelogin packet");
                    try_nb!(self.queue_simple_message(PreloginMessage::new()));
                    self.state = SqlConnectionNewState::PreLoginRecv;
                },
                SqlConnectionNewState::PreLoginRecv => {
                    trace!("init/recv prelogin...");
                    try_ready!(self.transport.poll_complete());
                    let header = try_ready!(self.transport.next_packet());
                    assert_eq!(header.ty, PacketType::TabularResult);
                    // parse the prelogin packet
                    let buf = self.transport.get_packet(header.length as usize);
                    let msg = try!(buf.as_ref().unserialize_message(&mut self.transport));
                    self.state = SqlConnectionNewState::LoginSend;
                },
                SqlConnectionNewState::LoginSend => {
                    let mut login_message = LoginMessage::new();

                    if let AuthMethod::SSPI_SSO = self.params.auth {
                        let (sso_client, buf) = try!(ntlm::sso::NtlmSso::new());
                        login_message.integrated_security = Some(buf.to_owned());
                        self.sso_client = Some(sso_client);
                    }

                    try_nb!(self.queue_simple_message(login_message));
                    self.state = SqlConnectionNewState::LoginRecv;
                },
                SqlConnectionNewState::LoginRecv => {
                    trace!("init/recv login...");
                    try_ready!(self.transport.poll_complete());
                    let header = try_ready!(self.transport.next_packet());
                    assert_eq!(header.ty, PacketType::TabularResult);
                    self.state = SqlConnectionNewState::TokenStreamRecv;
                },
                SqlConnectionNewState::TokenStreamRecv => {
                    let token = try_ready!(self.transport.read_token());
                    match token.map(|x| x.1) {
                        Some(TdsResponseToken::SSPI(bytes)) => {
                            assert!(self.sso_client.is_some());
                            match try!(self.sso_client.as_mut().unwrap().next_bytes(Some(bytes.as_ref()))) {
                                Some(bytes) => {
                                    try!(self.queue_simple_message(SspiMessage(bytes.to_vec())));
                                    self.state = SqlConnectionNewState::TokenStreamSend;
                                },
                                None => self.sso_client = None,
                            }
                        },
                        Some(TdsResponseToken::Done(done)) => {
                            // the connection is ready 2 go, we're done with our initialization
                            assert_eq!(done.status, tokens::DoneStatus::empty());
                            break;
                        },
                        Some(x) => trace!("init/got token {:?}", x),
                        None => (),
                    }
                },
                SqlConnectionNewState::TokenStreamSend => {
                    try_ready!(self.transport.poll_complete());
                    self.state = SqlConnectionNewState::TokenStreamRecv;
                }
            }
        }

        Ok(Async::Ready(()))
    }
}

/// a type which is constructable from a statement as a statement's result
pub trait StmtResult<I: BoxableIo> {
    type Result: Sized;

    fn from_connection(SqlConnection<I>, oneshot::Sender<SqlConnection<I>>) -> Self::Result;
}

/// a representation of an authenticated and ready for use SQL connection
pub struct InnerSqlConnection<I: BoxableIo> {
    transport: TdsTransport<I>,
}

pub struct SqlConnection<I: BoxableIo>(RefCell<InnerSqlConnection<I>>);

impl<I: BoxableIo> Deref for SqlConnection<I> {
    type Target = RefCell<InnerSqlConnection<I>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// a variant of Io which can be boxed to allow dynamic dispatch
pub trait BoxableIo: Io + Send {}
impl Io for Box<BoxableIo> {}
impl<I: Io + Send> BoxableIo for I {}

/// something that can be converted to an underlying IO
pub trait ToIo<I: BoxableIo + Sized> {
    type Result: Future<Item=I, Error=TdsError> + Send + Sized + 'static;

    fn to_io(self, handle: &Handle) -> Self::Result where Self: Sized;
}

impl<'a> ToIo<TcpStream> for &'a SocketAddr {
    type Result = FromErr<TcpStreamNew, TdsError>;

    fn to_io(self, handle: &Handle) -> Self::Result {
        TcpStream::connect(self, handle).from_err::<TdsError, TdsError>()
    }
}

enum DynamicConnectionTarget {
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

pub enum AuthMethod {
    /// single sign on using the local windows credentials (windows-only)
    #[cfg(windows)]
    SSPI_SSO,
    _DUMMY_AUTH_METHOD
}

/// settings for the connection, everything that isn't IO/transport specific (e.g. authentication)
pub struct ConnectParams {
    auth: AuthMethod,
}

/// a target address and connection settings, all that's required to connect to a SQL server
pub struct ConnectEndpoint<I, T: ToIo<I>> where I: BoxableIo + Sized + 'static {
    params: ConnectParams,
    target: T,
    _marker: PhantomData<*const I>,
}

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

/// parse connection strings
/// https://msdn.microsoft.com/de-de/library/system.data.sqlclient.sqlconnection.connectionstring(v=vs.110).aspx
impl<'a> ToConnectEndpoint<Box<BoxableIo>, DynamicConnectionTarget> for &'a str {
    fn to_connect_endpoint(self) -> TdsResult<ConnectEndpoint<Box<BoxableIo>, DynamicConnectionTarget>>
    {
        let mut input = &self[..];

        let mut target: Option<DynamicConnectionTarget> = None;
        let mut connect_params = ConnectParams {
            auth: AuthMethod::_DUMMY_AUTH_METHOD
        };

        while !input.is_empty() {
            let key_end = input.bytes().position(|x| x == b'=');
            let (key, key_end) = match key_end {
                None => return Err(TdsError::Conversion("connection string expected key. expected `=` never found".into())),
                Some(key_end) => ((&input[..key_end]).to_lowercase(), key_end)
            };
            input = &input[key_end+1..];
            // check if an escaped value (e.g. 'my password contains a;' or 'or a \' #kappa' follows)
            let escaped = input.starts_with('\'');
            let end = input.bytes().position(|x| x == b';').unwrap_or(input.len().saturating_sub(1));
            let mut tmp = None;

            let value = if !escaped {
                let ret = &input[..end];
                input = &input[end+1..];
                ret
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
                tmp = Some(val);
                tmp.as_ref().unwrap()
            };

            match key.as_str() {
                "server" => {
                    if value.starts_with("tcp:") {
                        let parts: Vec<_> = value[4..].split(",").collect();
                        assert!(parts.len() <= 2 && !parts.is_empty());
                        let addr = match (parts[0], parts[1].parse::<u16>()?).to_socket_addrs()?.nth(0) {
                            None => return Err(TdsError::Conversion("connection string: could not resolve server address".into())),
                            Some(x) => x,
                        };
                        target = Some(DynamicConnectionTarget::Tcp(addr));
                    }
                },
                "integratedsecurity" if ["true", "yes", "sspi"].contains(&value.to_lowercase().as_str()) => {
                    connect_params.auth = AuthMethod::SSPI_SSO;
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
    /// naive connection function for the SQL client
    pub fn connect<E, T: ToIo<I>>(handle: Handle, endpoint: E) -> SqlConnectionNew<I, T::Result>
        where E: ToConnectEndpoint<I, T>
    {
        let ConnectEndpoint { target, params, .. } = match endpoint.to_connect_endpoint() {
            Err(x) => return SqlConnectionNew::Error(Some(x)),
            Ok(x) => x,
        };
        SqlConnectionNew::Connection(Some((target.to_io(&handle), params)))
    }

    fn queue_sql_batch<'a, S>(&self, stmt: S) -> TdsResult<()> where S: Into<Cow<'a, str>> {
        let sql = stmt.into();
        let mut inner = self.borrow_mut();

        let batch_packet = try!(protocol::build_sql_batch(&mut inner.transport, &sql));
        try!(inner.transport.queue_vec(batch_packet));

        // attempt to send right now, if it works great, if not ready yet, it will be done later
        // simply ensures that data is sent as fast as possible
        let _ = try!(inner.transport.poll_complete());
        Ok(())
    }

    fn simple_exec_internal<'a, Q, R: StmtResult<I>>(self, query: Q) -> ResultSetStream<I, R> where Q: Into<Cow<'a, str>> {
        let result = self.queue_sql_batch(query);

        let ret = ResultSetStream::new(self);
        if let Err(err) = result {
            return ret.error(err)
        }
        ret
    }

    pub fn simple_query<'a, Q>(self, query: Q) -> ResultSetStream<I, QueryStream<I>> where Q: Into<Cow<'a, str>> {
        self.simple_exec_internal(query)
    }

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

    fn do_exec<'a>(&self, stmt: &Statement, params: &'a [&'a ToSql]) -> TokenRpcRequest<'a> {
        let mut params_meta = vec![
            RpcParam {
                // handle (using "handle" here makes RpcProcId::SpExecute not work and requires RpcProcIdValue::NAME, wtf)
                // not specifying the name is better anyways to reduce overhead on execute
                name: Cow::Borrowed(""),
                flags: RpcStatusFlags::empty(),
                value: ColumnData::I32(stmt.handle.borrow().as_ref().map(|h| h.handle).unwrap()),
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

    fn internal_exec<R: StmtResult<I>>(self, stmt: &Statement, params: &[&ToSql]) -> StmtStream<I, R> {
        // call sp_prepare (with valid handle) or sp_prepexec (initializer)
        let already_prepared = stmt.handle.borrow().as_ref().map(|handle| {
            // check if the param-type signature matches, if we have a handle
            handle.signature.iter().cloned().eq(params.iter().map(|x| x.to_sql()))
        }).unwrap_or(false);
        let req = if already_prepared {
            self.do_exec(stmt, params)
        } else {
            *stmt.handle.borrow_mut() = None;
            self.do_prepare_exec(stmt, params)
        };

        // write everything (or atleast queue it for write)
        let result = req.write_token(&mut self.borrow_mut().transport);
        let ret = StmtStream::new(self, stmt, params);
        match result {
            Ok(_) => ret,
            Err(err) => ret.error(err),
        }
    }

    pub fn query<'a>(self, stmt: &Statement, params: &[&ToSql]) -> StmtStream<I, QueryStream<I>> {
        self.internal_exec(stmt, params)
    }

    pub fn exec<'a>(self, stmt: &Statement, params: &[&ToSql]) -> StmtStream<I, ExecFuture<I>> {
        self.internal_exec(stmt, params)
    }

    pub fn prepare<S>(&self, stmt: S) -> Statement where S: Into<Cow<'static, str>> {
        Statement::new(stmt.into())
    }
}

#[cfg(test)]
mod tests {
    extern crate env_logger;
    use futures::{Future, Stream};
    use futures_state_stream::StateStream;
    use tokio_core::reactor::Core;
    use query::ExecFuture;
    use super::{AuthMethod, BoxableIo, ConnectParams, SqlConnection, TdsError};

    pub fn new_connection(lp: &mut Core) -> SqlConnection<Box<BoxableIo>> {
        let _ = env_logger::init();
        /*let addr: SocketAddr = "127.0.0.1:1433".parse().unwrap();
        let params = ConnectParams {
            auth: AuthMethod::SSPI_SSO,
        };
        let client = SqlConnection::connect(lp.handle(), (&addr, params));*/
        let future = SqlConnection::connect(lp.handle(), "server=tcp:127.0.0.1,1433;integratedSecurity=true;");
        lp.run(future).unwrap()
    }

    #[test]
    fn simple_select() {
        let mut lp = Core::new().unwrap();
        let mut c1 = new_connection(&mut lp);

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
            let future = exec.and_then(|x| x).for_each(|result| {
                // This is executed for EACH resultset (e.g. 2 times for 2 sql queries)
                i += 1;
                Ok(())
            });
            lp.run(future).unwrap();
        }
        assert_eq!(i, 1);
    }

    #[test]
    fn prepared_ddl_exec() {
        let mut lp = Core::new().unwrap();
        let mut c1 = new_connection(&mut lp);
        let stmt = c1.prepare("DECLARE @Mojo int");
        helper_ddl_exec(c1.exec(&stmt, &[]), &mut lp);
    }

    #[test]
    fn ddl_exec() {
        let mut lp = Core::new().unwrap();
        let mut c1 = new_connection(&mut lp);
        helper_ddl_exec(c1.simple_exec("DECLARE @Mojo int"), &mut lp);
    }

    #[test]
    fn todo_doctest() {
        let mut lp = Core::new().unwrap();
        let connection_string = "server=tcp:127.0.0.1,1433;integratedSecurity=true;";

        let future = SqlConnection::connect(lp.handle(), connection_string).and_then(|conn| {
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

    #[test]
    fn todo_doctest_simple() {
        let mut lp = Core::new().unwrap();
        let connection_string = "server=tcp:127.0.0.1,1433;integratedSecurity=true;";

        let future = SqlConnection::connect(lp.handle(), connection_string).and_then(|conn| {
            conn.simple_query("SELECT 1+2").for_each_row(|row| {
                let val: i32 = row.get(0);
                assert_eq!(val, 3i32);
                Ok(())
            })
        });
        lp.run(future).unwrap();
    }
}
