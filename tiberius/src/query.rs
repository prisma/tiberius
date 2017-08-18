//! Query results and resultsets
use std::marker::PhantomData;
use futures::{Async, Future, Poll, Sink, Stream};
use futures::sync::oneshot;
use futures_state_stream::{StateStream, StreamEvent};
use tokens::{DoneStatus, TdsResponseToken, TokenRow};
use types::FromColumnData;
use {BoxableIo, SqlConnection, StmtResult, Error, Result};

/// A query result consists of multiple query streams (amount of executed queries = amount of results)
#[must_use = "streams do nothing unless polled"]
pub struct ResultSetStream<I: BoxableIo, R: StmtResult<I>> {
    err: Option<Error>,
    conn: Option<SqlConnection<I>>,
    receiver: Option<oneshot::Receiver<SqlConnection<I>>>,
    /// whether we already returned a result for the current resultset
    already_triggered: bool,
    done: bool,
    _marker: PhantomData<R>,
}

impl<I: BoxableIo, R: StmtResult<I>> ResultSetStream<I, R> {
    pub fn new(conn: SqlConnection<I>) -> ResultSetStream<I, R> {
        ResultSetStream {
            err: None,
            conn: Some(conn),
            receiver: None,
            already_triggered: false,
            done: false,
            _marker: PhantomData,
        }
    }

    pub fn error(mut self, err: Error) -> Self {
        self.err = Some(err);
        self
    }
}

impl<I: BoxableIo, R: StmtResult<I>> StateStream for ResultSetStream<I, R> {
    type Item = R::Result;
    type State = SqlConnection<I>;
    type Error = Error;

    fn poll(&mut self) -> Poll<StreamEvent<Self::Item, Self::State>, Self::Error> {
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
                    .map_err(|_| Error::Canceled)
            ));
            self.receiver = None;
        }

        assert!(self.conn.is_some());

        if !self.done {
            let do_ret = match self.conn {
                None => false,
                Some(ref mut conn) => {
                    let inner = &mut conn.0;
                    try_ready!(inner.transport.inner.poll_complete());

                    let token = try_ready!(inner.transport.next_token())
                        .expect("resultset: expected a token!");
                    let (do_ret, reinject) = match token {
                        TdsResponseToken::ColMetaData(_) => {
                            self.already_triggered = true;
                            (true, false)
                        }
                        TdsResponseToken::Done(ref done) => {
                            self.done = !done.status.contains(DoneStatus::MORE);
                            let old = self.already_triggered;
                            self.already_triggered = false;
                            // make sure to return exactly one time for each result set
                            (!old, !old)
                        }
                        tok => panic!("resultset: unexpected token: {:?}", tok),
                    };
                    if reinject {
                        inner.transport.reinject(token);
                    }
                    do_ret
                }
            };
            if do_ret {
                let conn = self.conn.take().unwrap();
                let (sender, receiver) = oneshot::channel();
                self.receiver = Some(receiver);
                return Ok(Async::Ready(
                    StreamEvent::Next(R::from_connection(conn, sender)),
                ));
            }
        }
        let conn = self.conn.take().unwrap();
        Ok(Async::Ready(StreamEvent::Done(conn)))
    }
}

/// A stream of [`Rows`](struct.QueryRow.html) returned for the current resultset
#[must_use = "streams do nothing unless polled"]
pub struct QueryStream<I: BoxableIo> {
    inner: ResultInner<I>
}

struct ResultInner<I: BoxableIo> (
    Option<(SqlConnection<I>, oneshot::Sender<SqlConnection<I>>)>,
);

impl<I: BoxableIo> ResultInner<I> {
    fn send_back(&mut self) -> Result<bool> {
        if let Some((conn, ret_conn)) = self.0.take() {
            ret_conn.send(conn)
                .map_err(|_| Error::Canceled)
                .map(|_| true)
        } else {
            Ok(false)
        }
    }
}

impl<I: BoxableIo> Drop for ResultInner<I> {
    fn drop(&mut self) {
        if !::std::thread::panicking() {
            self.send_back().unwrap();
        }
    }
}

impl<'a, I: BoxableIo> Stream for QueryStream<I> {
    type Item = QueryRow;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        assert!(self.inner.0.is_some());

        if let Some(ref mut inner) = self.inner.0 {
            let inner = &mut (inner.0).0;
            try_ready!(inner.transport.inner.poll_complete());

            let token = try_ready!(inner.transport.next_token()).expect("query: expected token");
            let reinject = match token {
                TdsResponseToken::Row(row) => {
                    return Ok(Async::Ready(Some(QueryRow(row))));
                }
                // if this is the final done token, we need to reinject it for result set stream to handle it
                TdsResponseToken::Done(ref done) if !done.status.contains(DoneStatus::MORE) => true,
                TdsResponseToken::Done(_) | TdsResponseToken::DoneInProc(_) => false,
                x => panic!("query: unexpected token: {:?}", x),
            };
            if reinject {
                inner.transport.reinject(token);
            }
        }

        self.inner.send_back()?;
        Ok(Async::Ready(None))
    }
}

impl<'a, I: BoxableIo> StmtResult<I> for QueryStream<I> {
    type Result = QueryStream<I>;

    fn from_connection(conn: SqlConnection<I>, ret_conn: oneshot::Sender<SqlConnection<I>>) -> QueryStream<I> {
        QueryStream {
            inner: ResultInner(Some((conn, ret_conn))),
        }
    }
}

/// The result of an execution operation, resolves to the affected rows count for the current resultset
#[must_use = "futures do nothing unless polled"]
pub struct ExecFuture<I: BoxableIo> {
    inner: ResultInner<I>,
    /// Whether only a Done token (that was previously injected) is the contents of this stream
    single_token: bool,
}

impl<I: BoxableIo> Future for ExecFuture<I> {
    /// Amount of affected rows
    type Item = u64;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        assert!(self.inner.0.is_some());

        let mut ret: u64 = 0;
        if let Some(ref mut inner) = self.inner.0 {
            let inner = &mut (inner.0).0;
            try_ready!(inner.transport.inner.poll_complete());

            loop {
                let token = try_ready!(inner.transport.next_token()).expect("exec: expected token");
                let reinject = match token {
                    TdsResponseToken::Row(_) => {
                        self.single_token = false;
                        false
                    }
                    TdsResponseToken::Done(ref done) |
                    TdsResponseToken::DoneInProc(ref done) |
                    TdsResponseToken::DoneProc(ref done) => {
                        let final_token = match token {
                            TdsResponseToken::Done(_) | TdsResponseToken::DoneProc(_) => true,
                            _ => false,
                        };

                        if done.status.contains(DoneStatus::COUNT) {
                            ret = done.done_rows;
                        }
                        // if this is the final done token, we need to reinject it for result set stream to handle it
                        // (as in querying, if self.single_token it already was reinjected and would result in an infinite cycle)
                        let reinject = !done.status.contains(DoneStatus::MORE) && !self.single_token
                            && final_token;
                        if !reinject {
                            break;
                        }
                        true
                    }
                    x => panic!("exec: unexpected token: {:?}", x),
                };
                if reinject {
                    inner.transport.reinject(token);
                }
            }
        }

        self.inner.send_back()?;
        Ok(Async::Ready(ret))
    }
}

impl<I: BoxableIo> StmtResult<I> for ExecFuture<I> {
    type Result = ExecFuture<I>;

    fn from_connection(
        conn: SqlConnection<I>,
        ret_conn: oneshot::Sender<SqlConnection<I>>,
    ) -> ExecFuture<I> {
        ExecFuture {
            inner: ResultInner(Some((conn, ret_conn))),
            single_token: true,
        }
    }
}

/// A row in one resultset of a query
#[derive(Debug)]
pub struct QueryRow(TokenRow);

/// Anything that can be used as an index to get a specific row.
///
/// Currently this can either be a numerical index (position) or the
/// name of the column.
pub trait QueryIdx: Sized {
    fn to_idx(&self, row: &QueryRow) -> Option<usize>;
}

impl<'a> QueryIdx for &'a str {
    fn to_idx(&self, row: &QueryRow) -> Option<usize> {
        for (i, column) in row.0.meta.columns.iter().enumerate() {
            if &column.col_name.as_str() == self {
                return Some(i);
            }
        }
        None
    }
}

impl QueryIdx for usize {
    fn to_idx(&self, _: &QueryRow) -> Option<usize> {
        Some(*self)
    }
}

impl QueryRow {
    /// Returns the amount of columns in the row
    pub fn len(&self) -> usize {
        self.0.columns.len()
    }

    /// Get a column's value for a given column index
    pub fn name<'a, I: QueryIdx>(&'a self, idx: I) -> Option<&'a str> {
        idx.to_idx(self).map(|idx| self.0.meta.columns[idx].col_name.as_str())
    }

    /// Attempt to get a column's value for a given column index
    pub fn try_get<'a, I: QueryIdx, R: FromColumnData<'a>>(
        &'a self,
        idx: I,
    ) -> Result<Option<R>> {
        let idx = match idx.to_idx(self) {
            Some(x) => x,
            None => return Ok(None),
        };

        let col_data = &self.0.columns[idx];
        R::from_column_data(col_data).map(Some)
    }

    /// Retrieve a column's value for a given column index
    ///
    /// # Panics
    /// This panics if:
    ///
    /// - the requested type conversion (SQL->Rust) is not possible
    /// - the given index does exist (does not have a value associated with it)
    pub fn get<'a, I: QueryIdx, R: FromColumnData<'a>>(&'a self, idx: I) -> R {
        self.try_get(idx).unwrap().unwrap()
    }
}

impl<'r> IntoIterator for &'r QueryRow {
    type Item = QueryRowItem<'r>;
    type IntoIter = QueryRowIter<'r>;
    fn into_iter(self) -> Self::IntoIter {
        QueryRowIter {
            row: self,
            idx: 0,
        }
    }
}

/// An iterator over row name-value pairs
#[derive(Debug)]
pub struct QueryRowIter<'r>
{
    row: &'r QueryRow,
    idx: usize,
}

impl<'r> Iterator for QueryRowIter<'r>
{
    type Item = QueryRowItem<'r>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.idx < self.row.len() {
            let item = QueryRowItem {
                row: self.row,
                idx: self.idx,
            };
            self.idx += 1;
            Some(item)
        } else {
            None
        }
    }
}

/// An item (column) of the query row
#[derive(Debug)]
pub struct QueryRowItem<'r> {
    row: &'r QueryRow,
    idx: usize,
}

impl<'r> QueryRowItem<'r> {
    /// Get the column name
    pub fn name(&self) -> &'r str {
        self.row.name(self.idx).unwrap()
    }

    /// Attempt to get a column's value
    pub fn try_get<R: FromColumnData<'r>>(&self) -> Result<Option<R>> {
        self.row.try_get(self.idx)
    }

    /// Retrieve a column's value
    ///
    /// # Panics
    /// This panics if:
    ///
    /// - the requested type conversion (SQL->Rust) is not possible
    pub fn get<R: FromColumnData<'r>>(&self) -> R {
        self.row.get(self.idx)
    }
}
