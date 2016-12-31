use std::borrow::Cow;
use std::marker::PhantomData;
use futures::{Async, Poll, Future};
use tokio_core::io::Io;
use query::QueryStream;
use {SqlConnection, StmtResult, TdsError};

/// a type which can be translated as an SQL type (e.g. nvarchar) and is serializable (as `ColumnData`)
/// e.g. for usage within a ROW token
pub trait ToSql /*: ToColumnData*/ {
    fn to_sql() -> &'static str where Self: Sized;
}

/// a prepared statement which is prepared on the first execution
/// (which is a technical requirement since you need to know the types)
pub struct LazyPreparedStatement<'c, 'a, I: 'c + Io> {
    pub conn: &'c SqlConnection<I>,
    pub sql: Cow<'a, str>,
}

impl<'c, 'a, I: 'c + Io> LazyPreparedStatement<'c, 'a, I> {
    pub fn query<'s, 'b>(&'s self, params: &'b [&'b ToSql]) -> PreparedStmtFuture<'s, 'a, 'b, 'c, I, QueryStream<'c, I>> {
        PreparedStmtFuture {
            stmt: Some(self),
            params: params,
            state: None,
            _marker: PhantomData,
        }
    }
}

/// a future which handles the execution of a prepared statement and translates it
/// into the wished result (e.g. QueryStream)
pub struct PreparedStmtFuture<'s, 'a: 's, 'b: 's, 'c: 's, I: 'c + Io, R: StmtResult<'c, I>> {
    stmt: Option<&'s LazyPreparedStatement<'c, 'a, I>>,
    params: &'b [&'b ToSql],
    /// current state on which parameters serialization we're currently working on
    state: Option<usize>,

    /// This marker simply is used to allow this struct to be generic over a possible
    /// result, which allows us to share all state logic within this struct
    /// (e.g. we don't need a query specific future)
    _marker: PhantomData<*const R>,
}

impl<'s, 'a, 'b, 'c, I: Io, R: StmtResult<'c, I>> Future for PreparedStmtFuture<'s, 'a, 'b, 'c, I, R> {
    type Item = R;
    type Error = TdsError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        unimplemented!()
    }
}

// TODO: will need a macro
impl ToSql for i32 {
    fn to_sql() -> &'static str {
        "int"
    }
}
