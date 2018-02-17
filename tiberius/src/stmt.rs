//! Prepared statements
use std::borrow::Cow;
use std::marker::PhantomData;
use std::sync::Arc;
use futures::{Async, Future, Poll, Sink, Stream};
use futures::sync::oneshot;
use futures_state_stream::{StateStream, StreamEvent};
use query::{ExecFuture, QueryStream};
use tokens::{DoneStatus, TdsResponseToken, TokenColMetaData};
use types::{ColumnData, ToSql};
use {BoxableIo, SqlConnection, StmtResult, TdsError};

/// A prepared statement which is prepared on the first execution
/// (which is a technical requirement since you need to know the types)
#[derive(Clone)]
pub struct Statement {
    pub(crate) sql: Cow<'static, str>,
}

// TODO: implement Drop for StatementHandle (channel to the connection which unprepares the statement)
impl Statement {
    pub fn new(sql: Cow<'static, str>) -> Statement {
        Statement { sql: sql }
    }

    pub(crate) fn get_handle_for<I: BoxableIo>(
        &self,
        conn: &SqlConnection<I>,
        needed: &[&'static str],
    ) -> Option<(i32, Option<Arc<TokenColMetaData>>)> {
        if let Some(bindings) = conn.0.stmts.get(&*self.sql) {
            for binding in bindings {
                if needed.iter().eq(binding.0.iter()) {
                    return Some((binding.1, binding.2.clone()));
                }
            }
        }
        None
    }
}

impl<'a> From<&'a Statement> for Statement {
    fn from(stmt: &'a Statement) -> Statement {
        stmt.clone()
    }
}

impl<S> From<S> for Statement
where
    S: Into<Cow<'static, str>>,
{
    fn from(sql: S) -> Statement {
        Statement::new(sql.into())
    }
}

/// A future which handles the execution of a prepared statement and translates it
/// into a set of results (e.g. multiple elements of the type `QueryStream`)
#[must_use = "streams do nothing unless polled"]
pub struct StmtStream<I: BoxableIo, R: StmtResult<I>> {
    err: Option<TdsError>,
    done: bool,
    conn: Option<SqlConnection<I>>,
    param_sig: Option<Vec<&'static str>>,
    receiver: Option<oneshot::Receiver<SqlConnection<I>>>,
    stmt: Statement,
    meta: Option<Arc<TokenColMetaData>>,

    already_triggered: bool,
    /// This marker simply is used to allow this struct to be generic over a possible
    /// result, which allows us to share all state logic within this struct
    /// (e.g. we don't need a query specific future)
    _marker: PhantomData<*const R>,
}

impl<I: BoxableIo, R: StmtResult<I>> StmtStream<I, R> {
    pub fn new(
        conn: SqlConnection<I>,
        stmt: Statement,
        meta: Option<Arc<TokenColMetaData>>,
        params: &[&ToSql],
    ) -> Self {
        let signature = params.iter().map(|x| x.to_sql()).collect();
        StmtStream {
            err: None,
            done: false,
            conn: Some(conn),
            param_sig: Some(signature),
            receiver: None,
            stmt,
            meta,
            already_triggered: false,
            _marker: PhantomData,
        }
    }

    pub fn error(mut self, err: TdsError) -> Self {
        self.err = Some(err);
        self
    }
}

impl<I: BoxableIo, R: StmtResult<I>> StateStream for StmtStream<I, R> {
    type Item = R::Result;
    type State = SqlConnection<I>;
    type Error = TdsError;

    fn poll(&mut self) -> Poll<StreamEvent<Self::Item, Self::State>, Self::Error> {
        // return a stored error, if that's the case
        if let Some(err) = self.err.take() {
            return Err(err);
        }

        // attempt to receive the connection back to continue receiving further resultsets
        if self.receiver.is_some() {
            self.conn = Some(try_ready!(
                self.receiver
                    .as_mut()
                    .unwrap()
                    .poll()
                    .map_err(|_| TdsError::Canceled)
            ));
            self.receiver = None;
        }

        try_ready!(
            self.conn
                .as_mut()
                .map(|x| x.0.transport.inner.poll_complete())
                .unwrap()
        );

        // receive and handle the result of sp_prepare
        while !self.done {
            let token = try_ready!(
                self.conn
                    .as_mut()
                    .map(|x| x.0.transport.next_token())
                    .unwrap()
            ).expect("StateStream: expected token");
            let (do_ret, reinject) = match token {
                TdsResponseToken::ColMetaData(ref meta) => {
                    if !meta.columns.is_empty() {
                        self.meta = Some(meta.clone());
                    } else {
                        // use the meta data of the current statement for parsing
                        // TODO: our meta data handling likely wont work with a multi result set
                        //       prepared statement like "select 1; select 2;"
                        self.conn.as_mut().unwrap().0.transport.inner.last_meta = self.meta.clone();
                    }
                    self.already_triggered = !meta.columns.is_empty() || self.meta.is_some();

                    (self.already_triggered, false)
                }
                TdsResponseToken::DoneProc(ref done) => {
                    // we've read each query result, we're done with the current sp_exec, this stream may rest
                    assert_eq!(done.status, DoneStatus::empty());
                    let old = self.already_triggered;
                    self.already_triggered = false;
                    self.done = true;
                    (!old, !old) //reinject if !old, see below
                }
                // this simply notifies us that a DoneProc is following (DONE_MORE)
                TdsResponseToken::DoneInProc(_) => (false, false),
                TdsResponseToken::ReturnStatus(ref status) => {
                    assert_eq!(status & 1, 0); // ensure that failure is no part of status
                    (false, false)
                }
                TdsResponseToken::ReturnValue(ref retval) => {
                    assert_eq!(retval.param_name.as_str(), "handle");
                    let new_handle = match retval.value {
                        ColumnData::I32(val) => val,
                        _ => unreachable!(),
                    };
                    let signature = self.param_sig.take().unwrap();

                    if let Some(ref mut conn) = self.conn {
                        let target = conn.0
                            .stmts
                            .entry((&*self.stmt.sql).to_owned())
                            .or_insert(Vec::with_capacity(1));
                        target.retain(|x| x.0 != signature);
                        target.push((signature, new_handle, self.meta.as_ref().cloned()));
                    }

                    (false, false)
                }
                x => panic!("stmtstream: unexpected token: {:?}", x),
            };
            if do_ret {
                let mut conn = self.conn.take().unwrap();
                if reinject {
                    conn.0.transport.reinject(token);
                }
                let (sender, receiver) = oneshot::channel();
                self.receiver = Some(receiver);
                return Ok(Async::Ready(
                    StreamEvent::Next(R::from_connection(conn, sender)),
                ));
            }
        }

        // this stream is done, make sure it cannot be executed again
        let conn = self.conn.take().unwrap();
        Ok(Async::Ready(StreamEvent::Done(conn)))
    }
}

/// A single resultset yielding the status of query execution
/// (currently the amount of affected rows)
#[must_use = "futures do nothing unless polled"]
pub struct ExecResult<S: StateStream> where S::Item: Future {
    stream: S,
    idx: usize,
    resultset: Option<S::Item>,
    result: Option<<S::Item as Future>::Item>,
}

impl<S: StateStream> ExecResult<S> where S::Item: Future
{
    pub fn new(stream: S) -> ExecResult<S> {
        ExecResult {
            stream,
            idx: 0,
            resultset: None,
            result: None,
        }
    }

    /// Extract the underlying stream to e.g. access multiple resultsets
    pub fn into_stream(self) -> S {
        self.stream
    }
}

impl<I, S> Future for ExecResult<S>
where
    I: BoxableIo,
    S: StateStream<Item = ExecFuture<I>, Error = <ExecFuture<I> as Future>::Error>,
{
    type Item = (<ExecFuture<I> as Future>::Item, S::State);
    type Error = S::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            if let Some(ref mut resultset) = self.resultset {
                self.result = Some(try_ready!(resultset.poll()));
            }
            // ensure we do not poll the same resultset again
            self.resultset = None;
            self.resultset = match try_ready!(self.stream.poll()) {
                StreamEvent::Next(resultset) => Some(resultset),
                StreamEvent::Done(conn) => {
                    let result = self.result
                        .take()
                        .expect("ExecResult expected 1 resultset, got none");
                    return Ok(Async::Ready((result, conn)));
                }
            };
            if self.idx == 1 {
                panic!("ExecResult received more than 1 resultset");
            }
            self.idx += 1;
        }
    }
}

/// A single resultset yielding rows
#[must_use = "streams do nothing unless polled"]
pub struct QueryResult<S: StateStream> {
    stream: S,
    idx: usize,
    resultset: Option<S::Item>,
}

impl<S: StateStream> QueryResult<S>
{
    pub fn new(stream: S) -> QueryResult<S> {
        QueryResult {
            stream,
            idx: 0,
            resultset: None,
        }
    }

    /// Extract the underlying stream to e.g. access multiple resultsets
    pub fn into_stream(self) -> S {
        self.stream
    }
}

impl<I, S> StateStream for QueryResult<S> 
where 
    I: BoxableIo,
    S: StateStream<Item = QueryStream<I>, Error = <QueryStream<I> as Stream>::Error>,
{
    type State = S::State;
    type Item = <QueryStream<I> as Stream>::Item;
    type Error = <QueryStream<I> as Stream>::Error;

    fn poll(&mut self) -> Poll<StreamEvent<Self::Item, Self::State>, Self::Error> {
        loop {
            if let Some(ref mut resultset) = self.resultset {
                if let Some(result) = try_ready!(resultset.poll()) {
                    return Ok(Async::Ready(StreamEvent::Next(result)));
                }
            }
            
            // ensure we do not poll the same resultset again
            self.resultset = None;
            self.resultset = match try_ready!(self.stream.poll()) {
                StreamEvent::Next(resultset) => Some(resultset),
                StreamEvent::Done(conn) => return Ok(Async::Ready(StreamEvent::Done(conn))),
            };
            if self.idx == 1 {
                panic!("QueryResult received more than 1 resultset");
            }
            self.idx += 1;
        }
    }
}
