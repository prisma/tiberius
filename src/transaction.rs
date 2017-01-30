use std::borrow::Cow;
use futures::{Async, Future, BoxFuture, Poll, Stream};
use futures_state_stream::{StateStream, StreamEvent};
use query::{ResultSetStream, ExecFuture, QueryStream};
use stmt::{ForEachRow, Statement, StmtStream, ResultStreamExt};
use types::ToSql;
use {BoxableIo, SqlConnection, TdsError};

/// A transaction
pub struct Transaction<I: BoxableIo>(SqlConnection<I>);

pub fn new_transaction<I: BoxableIo>(conn: SqlConnection<I>) -> Transaction<I> {
    Transaction(conn)
}

/// A stream which is a result from an operation which is executed within a transaction
/// This simply wraps the state (which internally is a SqlConnection) in the `Transaction` struct
pub struct TransactionStream<S> {
    stream: Option<S>,
}

impl<S> TransactionStream<S> {
    pub fn new(stream: S) -> TransactionStream<S> {
        TransactionStream {
            stream: Some(stream)
        }
    }
}

impl<I: BoxableIo, S: StateStream<State = SqlConnection<I>>> StateStream for TransactionStream<S> {
    type Item = S::Item;
    type State = Transaction<I>;
    type Error = S::Error;

    fn poll(&mut self) -> Poll<StreamEvent<Self::Item, Self::State>, Self::Error> {
        let item = match try_ready!(self.stream.as_mut().unwrap().poll()) {
            StreamEvent::Done(conn) => {
                self.stream.take();
                StreamEvent::Done(Transaction(conn))
            },
            StreamEvent::Next(next) => StreamEvent::Next(next),
        };
        Ok(Async::Ready(item))
    }
}

impl<I: BoxableIo, R> ResultStreamExt<I> for TransactionStream<R> where R: ResultStreamExt<I> + StateStream<State = SqlConnection<I>> {
    fn for_each_row<F>(self, f: F) -> ForEachRow<I, Self, F>
        where Self: StateStream<Item=QueryStream<I>, Error=<QueryStream<I> as Stream>::Error>,
              F: FnMut(<QueryStream<I> as Stream>::Item) -> Result<(), TdsError>
    {
        ForEachRow::new(self, f)
    }
}

impl<I: BoxableIo + 'static> Transaction<I> {
    pub fn simple_exec<'a, Q>(self, query: Q) -> TransactionStream<ResultSetStream<I, ExecFuture<I>>> where Q: Into<Cow<'a, str>> {
        TransactionStream::new(self.0.simple_exec(query))
    }

    pub fn simple_query<'a, Q>(self, query: Q) -> TransactionStream<ResultSetStream<I, QueryStream<I>>> where Q: Into<Cow<'a, str>> {
        TransactionStream::new(self.0.simple_query(query))
    }

    pub fn exec(self, stmt: &Statement, params: &[&ToSql]) -> TransactionStream<StmtStream<I, ExecFuture<I>>> {
        TransactionStream::new(self.0.exec(stmt, params))
    }

    pub fn query(self, stmt: &Statement, params: &[&ToSql]) -> TransactionStream<StmtStream<I, QueryStream<I>>> {
        TransactionStream::new(self.0.query(stmt, params))
    }

    pub fn prepare<S>(&self, stmt: S) -> Statement where S: Into<Cow<'static, str>> {
        self.0.prepare(stmt.into())
    }

    /// Commits a transaction
    pub fn commit(self) -> BoxFuture<SqlConnection<I>, TdsError> {
        self.internal_exec("COMMIT TRAN")
            .and_then(|trans| trans.finish())
            .boxed()
    }

    /// Rollback a transaction
    pub fn rollback(self) -> BoxFuture<SqlConnection<I>, TdsError> {
        self.internal_exec("ROLLBACK TRAN")
            .and_then(|trans| trans.finish())
            .boxed()
    }

    /// convert back to a normal connection (enable auto commit)
    fn finish(self) -> BoxFuture<SqlConnection<I>, TdsError> {
        self.internal_exec("set implicit_transactions off")
            .and_then(|trans| Ok(trans.0))
            .boxed()
    }

    /// executes an internal statement and checks if it succeeded
    fn internal_exec(self, sql: &str) -> BoxFuture<Transaction<I>, TdsError> {
        self.simple_exec(sql)
            .and_then(|resultset| resultset)
            .collect()
            .and_then(|(results, trans)| {
                assert_eq!(results.len(), 1);
                assert_eq!(results[0], 0);
                Ok(trans)
            })
            .boxed()
    }
}
