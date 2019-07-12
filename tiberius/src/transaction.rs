use std::borrow::Cow;
use futures::{Async, Future, Poll};
use futures_state_stream::{StateStream, StreamEvent};
use crate::query::{ExecFuture, QueryStream, ResultSetStream};
use crate::stmt::{ExecResult, QueryResult, Statement, StmtStream};
use crate::types::ToSql;
use crate::{BoxableIo, SqlConnection, Error};

/// A transaction
pub struct Transaction<I: BoxableIo>(SqlConnection<I>);

pub fn new_transaction<I: BoxableIo>(conn: SqlConnection<I>) -> Transaction<I> {
    Transaction(conn)
}

pub struct TransactionFuture<F> {
    future: Option<F>,
}

impl<F: Future> TransactionFuture<F> {
    pub fn new(future: F) -> TransactionFuture<F> {
        TransactionFuture {
            future: Some(future),
        }
    }
}

impl<I, T, F> Future for TransactionFuture<F>
where
    I: BoxableIo,
    F: Future<Item = (T, SqlConnection<I>)>
{
    type Item = (T, Transaction<I>);
    type Error = F::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let (item, conn) = try_ready!(self.future.as_mut().unwrap().poll());

        Ok(Async::Ready((item, Transaction(conn))))
    }
}

/// A stream which is a result from an operation which is executed within a transaction
/// This simply wraps the state (which internally is a SqlConnection) in the `Transaction` struct
#[must_use = "streams do nothing unless polled"]
pub struct TransactionStream<S> {
    stream: Option<S>,
}

impl<S: StateStream> TransactionStream<S> {
    pub fn new(stream: S) -> TransactionStream<S> {
        TransactionStream {
            stream: Some(stream),
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
            }
            StreamEvent::Next(next) => StreamEvent::Next(next),
        };
        Ok(Async::Ready(item))
    }
}

impl<I: BoxableIo + 'static> Transaction<I> {
    pub fn simple_exec<'a, Q>(self, query: Q) -> TransactionFuture<ExecResult<ResultSetStream<I, ExecFuture<I>>>>
    where
        Q: Into<Cow<'a, str>>,
    {
        TransactionFuture::new(self.0.simple_exec(query))
    }

    pub fn simple_query<'a, Q>(self, query: Q) -> TransactionStream<QueryResult<ResultSetStream<I, QueryStream<I>>>>
    where
        Q: Into<Cow<'a, str>>,
    {
        TransactionStream::new(self.0.simple_query(query))
    }

    pub fn exec<S: Into<Statement>>(self, stmt: S, params: &[&dyn ToSql]) -> TransactionFuture<ExecResult<StmtStream<I, ExecFuture<I>>>> {
        TransactionFuture::new(self.0.exec(stmt, params))
    }

    pub fn query<S: Into<Statement>>(self, stmt: S, params: &[&dyn ToSql]) -> TransactionStream<QueryResult<StmtStream<I, QueryStream<I>>>> {
        TransactionStream::new(self.0.query(stmt, params))
    }

    pub fn prepare<S>(&self, stmt: S) -> Statement
    where
        S: Into<Cow<'static, str>>,
    {
        self.0.prepare(stmt.into())
    }

    /// Commits a transaction
    pub fn commit(self) -> Box<dyn Future<Item = SqlConnection<I>, Error = Error>> {
        Box::new(
            self.internal_exec("COMMIT TRAN")
                .and_then(|trans| trans.finish()),
        )
    }

    /// Rollback a transaction
    pub fn rollback(self) -> Box<dyn Future<Item = SqlConnection<I>, Error = Error>> {
        Box::new(
            self.internal_exec("ROLLBACK TRAN")
                .and_then(|trans| trans.finish()),
        )
    }

    /// convert back to a normal connection (enable auto commit)
    fn finish(self) -> Box<dyn Future<Item = SqlConnection<I>, Error = Error>> {
        Box::new(
            self.internal_exec("set implicit_transactions off")
                .and_then(|trans| Ok(trans.0)),
        )
    }

    /// executes an internal statement and checks if it succeeded
    fn internal_exec(self, sql: &str) -> Box<dyn Future<Item = Transaction<I>, Error = Error>> {
        Box::new(self.simple_exec(sql).and_then(|(result, trans)| {
            assert_eq!(result, 0);
            Ok(trans)
        }))
    }
}
