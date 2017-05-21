//! Prepared statements
use std::borrow::Cow;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;
use futures::{Async, Future, Poll, Stream, Sink};
use futures::sync::oneshot;
use futures_state_stream::{StateStream, StreamEvent};
use query::QueryStream;
use tokens::{DoneStatus, TdsResponseToken, TokenColMetaData};
use types::{ColumnData, ToSql};
use {BoxableIo, SqlConnection, StmtResult, TdsError};

/// A handle identifying a server-side prepared statement for a specific connection
/// and a specific set of parameter types
#[doc(hidden)]
pub struct StatementHandle {
    pub handle: i32,
    pub signature: Vec<&'static str>,
}

/// A prepared statement which is prepared on the first execution
/// (which is a technical requirement since you need to know the types)
#[derive(Clone)]
pub struct Statement(Arc<StatementInner>);

impl Deref for Statement {
    type Target = StatementInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[doc(hidden)]
pub struct StatementInner {
    pub sql: Cow<'static, str>,
    meta: RefCell<Option<Arc<TokenColMetaData>>>,
    pub handle: RefCell<Option<StatementHandle>>,
}

// TODO: implement Drop for StatementInner (channel to the connection which unprepares the statement)
impl Statement {
    pub fn new(sql: Cow<'static, str>) -> Statement {
        Statement(Arc::new(StatementInner {
            sql: sql,
            meta: RefCell::new(None),
            handle: RefCell::new(None),
        }))
    }
}

/// A future which handles the execution of a prepared statement and translates it
/// into the wished result (e.g. `QueryStream`)
pub struct StmtStream<I: BoxableIo, R: StmtResult<I>> {
    err: Option<TdsError>,
    done: bool,
    conn: Option<SqlConnection<I>>,
    param_sig: Option<Vec<&'static str>>,
    receiver: Option<oneshot::Receiver<SqlConnection<I>>>,
    stmt: Statement,

    already_triggered: bool,
    /// This marker simply is used to allow this struct to be generic over a possible
    /// result, which allows us to share all state logic within this struct
    /// (e.g. we don't need a query specific future)
    _marker: PhantomData<*const R>,
}

impl<I: BoxableIo, R: StmtResult<I>> StmtStream<I, R> {
    pub fn new(conn: SqlConnection<I>, stmt: &Statement, params: &[&ToSql]) -> Self {
        let signature = params.iter().map(|x| x.to_sql()).collect();
        StmtStream {
            err: None,
            done: false,
            conn: Some(conn),
            param_sig: Some(signature),
            receiver: None,
            stmt: stmt.clone(),
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
            self.conn = Some(try_ready!(self.receiver.as_mut().unwrap().poll().map_err(|_| TdsError::Canceled)));
            self.receiver = None;
        }

        try_ready!(self.conn.as_ref().map(|x| x.borrow_mut()).unwrap().transport.inner.poll_complete());

        // receive and handle the result of sp_prepare
        while !self.done {
            let (do_ret, new_state) = match try_ready!(self.conn.as_ref().map(|x| x.borrow_mut()).unwrap().transport.next_token()) {
                Some((last_pos, token)) => match token {
                    TdsResponseToken::ColMetaData(meta) => {
                        if !meta.columns.is_empty() {
                            *self.stmt.meta.borrow_mut() = Some(meta.clone());
                        }
                        self.already_triggered = !meta.columns.is_empty() || self.stmt.handle.borrow().is_some();
                        (self.already_triggered, None)
                    },
                    TdsResponseToken::DoneProc(done) => {
                        // we've read each query result, we're done with the current sp_exec, this stream may rest
                        assert_eq!(done.status, DoneStatus::empty());
                        let old = self.already_triggered;
                        self.already_triggered = false;
                        self.done = true;
                        (!old, Some(last_pos)) //reinject last_pos if !old
                    },
                    // this simply notifies us that a DoneProc is following (DONE_MORE)
                    TdsResponseToken::DoneInProc(_) => (false, None),
                    TdsResponseToken::ReturnStatus(status) => {
                        assert_eq!(status & 1, 0); // ensure that failure is no part of status
                        (false, None)
                    },
                    TdsResponseToken::ReturnValue(retval) => {
                        assert_eq!(retval.param_name.as_str(), "handle");
                        *self.stmt.handle.borrow_mut() = Some(match retval.value {
                            ColumnData::I32(val) => StatementHandle {
                                handle: val,
                                signature: self.param_sig.take().unwrap(),
                            },
                            _ => unreachable!()
                        });
                        (false, None)
                    },
                    x => panic!("stmtstream: unexpected token: {:?}", x),
                },
                _ => unimplemented!()
            };
            if do_ret {
                let conn = self.conn.take().unwrap();
                if let Some(new_state) = new_state {
                    conn.borrow_mut().transport.inner.rd = new_state;
                }
                let (sender, receiver) = oneshot::channel();
                self.receiver = Some(receiver);
                return Ok(Async::Ready(StreamEvent::Next(R::from_connection(conn, sender))));
            }
        }

        // this stream is done, make sure it cannot be executed again
        let conn = self.conn.take().unwrap();
        Ok(Async::Ready(StreamEvent::Done(conn)))
    }
}

pub trait ResultStreamExt<I: BoxableIo>: StateStream {
    /// Only expect 1 result set (e.g. if you're only executing one query)
    /// and execute a given closure for the results of the first result set
    ///
    /// # Panics
    /// This will panic if there is more than 1 resultset
    fn for_each_row<F>(self, f: F) -> ForEachRow<I, Self, F>
        where Self: Sized + StateStream<Item=QueryStream<I>, Error=<QueryStream<I> as Stream>::Error>,
                 F: FnMut(<QueryStream<I> as Stream>::Item) -> Result<(), TdsError>;
}

impl<I: BoxableIo> ResultStreamExt<I> for StmtStream<I, QueryStream<I>> {
    fn for_each_row<F>(self, f: F) -> ForEachRow<I, StmtStream<I, QueryStream<I>>, F>
        where F: FnMut(<QueryStream<I> as Stream>::Item) -> Result<(), TdsError>
    {
        ForEachRow::new(self, f)
    }
}

/// Iterate over resultsets and only return the rows of the first one
/// but handle/consume the entire result set so that we're ready to continue
/// after the execution of this
pub struct ForEachRow<I: BoxableIo, S: StateStream<Item=QueryStream<I>, Error=<QueryStream<I> as Stream>::Error>, F> {
    stream: Option<S>,
    f: F,
    idx: usize,
    resultset: Option<QueryStream<I>>,
}

impl<I: BoxableIo, S, F> ForEachRow<I, S, F>
     where S: StateStream<Item=QueryStream<I>, Error=<QueryStream<I> as Stream>::Error>,
           F: FnMut(<QueryStream<I> as Stream>::Item) -> Result<(), TdsError>
{
    pub fn new(stream: S, f: F) -> ForEachRow<I, S, F> {
        ForEachRow {
            stream: Some(stream),
            f: f,
            idx: 0,
            resultset: None,
        }
    }
}

impl<I: BoxableIo, S, F> Future for ForEachRow<I, S, F>
    where S: StateStream<Item=QueryStream<I>, Error=<QueryStream<I> as Stream>::Error>,
          F: FnMut(<QueryStream<I> as Stream>::Item) -> Result<(), TdsError>
{
    type Item = S::State;
    type Error = <QueryStream<I> as Stream>::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            while let Some(ref mut resultset) = self.resultset {
                match try_ready!(resultset.poll()) {
                    None => break,
                    Some(row) => if self.idx == 1 {
                        try!((self.f)(row))
                    },
                }
            }
            // ensure we do not poll the same resultset again
            self.resultset = None;
            self.resultset = match try_ready!(self.stream.as_mut().unwrap().poll()) {
                StreamEvent::Next(resultset) => Some(resultset),
                StreamEvent::Done(conn) => return Ok(Async::Ready(conn)),
            };
            if self.idx == 1 {
                panic!("for_each_row received more than 1 resultset");
            }
            self.idx += 1;
        }
    }
}
