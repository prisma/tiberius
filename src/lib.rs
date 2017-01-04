#[macro_use]
extern crate bitflags;
extern crate byteorder;
extern crate encoding;
#[macro_use]
extern crate futures;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
#[macro_use]
extern crate tokio_core;

use std::borrow::Cow;
use std::cell::RefCell;
use std::convert::From;
use std::net::SocketAddr;
use std::ops::Deref;
use std::io;
use futures::{Async, BoxFuture, Future, Poll, Sink};
use tokio_core::io::Io;
use tokio_core::net::TcpStream;
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
use tokens::TdsResponseToken;
use query::QueryStream;
use stmt::LazyPreparedStatement;

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

/// naive connection function for the SQL client (TCP)
pub fn connect(handle: Handle, addr: &SocketAddr) -> BoxFuture<SqlConnection<TcpStream>, TdsError> {
    let addr = addr.clone();
    TcpStream::connect(&addr, &handle).map_err(|err| err.into()).and_then(|stream| {
        SqlConnectionFuture(Some(SqlConnectionNew::new(stream)))
    }).boxed()
}

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
struct SqlConnectionNew<T: Io> {
    transport: TdsTransport<T>,
    state: SqlConnectionNewState,
    sso_client: Option<ntlm::sso::NtlmSso>,
}

impl<T: Io> SqlConnectionNew<T> {
    fn new(transport: T) -> SqlConnectionNew<T> {
        SqlConnectionNew {
            transport: TdsTransport::new(transport),
            state: SqlConnectionNewState::PreLoginSend,
            sso_client: None,
        }
    }

    /// queues a simple message which serializes to ONE packet
    pub fn queue_simple_message<M: SerializeMessage>(&mut self, m: M) -> io::Result<()> {
        let vec = try!(m.serialize_message(&mut self.transport));
        self.transport.queue_vec(vec)
    }
}

impl<T: Io> Future for SqlConnectionNew<T> {
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

                    // TODO: make this configurable, just for debug
                    let (sso_client, buf) = try!(ntlm::sso::NtlmSso::new());
                    login_message.integrated_security = Some(buf.to_owned());
                    self.sso_client = Some(sso_client);

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
                    match token {
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

pub struct SqlConnectionFuture<I: Io>(Option<SqlConnectionNew<I>>);

impl<I: Io> Future for SqlConnectionFuture<I> {
    type Item = SqlConnection<I>;
    type Error = TdsError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if let Some(ref mut future) = self.0 {
            try_ready!(future.poll());
        }

        let trans = self.0.take().unwrap().transport;
        let conn = InnerSqlConnection {
            transport: trans,
        };

        Ok(Async::Ready(SqlConnection(RefCell::new(conn))))
    }
}

/// a type which is constructable from a statement as a statement's result
pub trait StmtResult<'c, I: 'c + Io> {
    type Result: Sized;

    fn from_connection(&'c SqlConnection<I>) -> Self::Result;
}

/// a representation of an authenticated and ready for use SQL connection
pub struct InnerSqlConnection<I: Io> {
    transport: TdsTransport<I>,
}

pub struct SqlConnection<I: Io>(RefCell<InnerSqlConnection<I>>);

impl<I: Io> Deref for SqlConnection<I> {
    type Target = RefCell<InnerSqlConnection<I>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<I: Io> SqlConnection<I> {
    fn queue_sql_batch<'a, S>(&self, stmt: S) -> TdsResult<()> where S: Into<Cow<'a, str>> {
        let sql = stmt.into();
        let mut inner = self.borrow_mut();

        let batch_packet = try!(protocol::build_sql_batch(&mut inner.transport, &sql));
        try!(inner.transport.queue_vec(batch_packet));
        Ok(())
    }

    pub fn query<'a, Q>(&self, query: Q) -> TdsResult<QueryStream<I>> where Q: Into<Cow<'a, str>> {
        try!(self.queue_sql_batch(query));
        Ok(QueryStream { conn: Some(self) })
    }

    pub fn prepare<'c, 'a, S>(&'c self, stmt: S) -> LazyPreparedStatement<'c, 'a, I> where S: Into<Cow<'a, str>> {
        LazyPreparedStatement::new(self, stmt.into())
    }
}

#[cfg(test)]
mod tests {
    extern crate env_logger;
    use futures::Stream;
    use tokio_core::reactor::Core;
    use super::{connect};

    #[test]
    fn test() {
        env_logger::init().unwrap();
        let addr = "127.0.0.1:1433".parse().unwrap();
        let mut lp = Core::new().unwrap();
        let client = connect(lp.handle(), &addr);
        let mut c1 = lp.run(client).unwrap();

        let query = c1.query("select cast(cast(N'cześć' as nvarchar(5)) collate Polish_CI_AI as varchar(5))").unwrap();
        println!("rows: ");
        let future = query.for_each(|x| {
            let val: &str = x.get(0);
            println!("row: {:?}", val);
            Ok(())
        });
        lp.run(future).unwrap();

        /*let stmt = c1.prepare("select test_num FROM test.dbo.test_ints WHERE test_num < @P1;");

        for g in 0..2 {
            let mut i = 0;
            let mut results = lp.run(stmt.query(&[&5i32]).and_then(|result| {
                i += 1;
                println!("rows: {}", i);
                result.for_each(|x| {
                    let val: i32 = x.get("test_num");
                    println!("row: {:?}", val);
                    Ok(())
                })
            }).for_each(|_| Ok(())));
        }*/

        /*let query = c1.query("select TOP 5000 test_num FROM test.dbo.test_ints").unwrap();
        println!("rows: ");
        let future = query.for_each(|x| {
            let val: i32 = x.get("test_num");
            println!("row: {:?}", val);
            Ok(())
        });*/
        println!("end");
    }
}
