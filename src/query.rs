use std::marker::PhantomData;
use futures::{Async, Future, Poll, Stream, Sink};
use tokio_core::io::Io;
use stmt::ForEachRow;
use tokens::{self, TdsResponseToken, TokenRow};
use types::FromColumnData;
use {SqlConnection, StmtResult, TdsError, TdsResult};

/// a query result consists of multiple query streams (amount of executed queries = amount of results)
pub struct ResultSetStream<'a, I: 'a + Io, R: StmtResult<'a, I>> {
    conn: Option<&'a SqlConnection<I>>,
    /// whether we already returned a result for the current resultset
    already_triggered: bool,
    done: bool,
    _marker: PhantomData<*const R>,
}

impl<'a, I: Io, R: StmtResult<'a, I>> ResultSetStream<'a, I, R> {
    pub fn new(conn: &'a SqlConnection<I>) -> ResultSetStream<'a, I, R> {
        ResultSetStream {
            conn: Some(conn),
            already_triggered: false,
            done: false,
            _marker: PhantomData,
        }
    }
}

impl<'a, I: Io, R: StmtResult<'a, I>> Stream for ResultSetStream<'a, I, R> {
    type Item = R::Result;
    type Error = TdsError;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        assert!(self.conn.is_some());

        if !self.done {
            if let Some(ref mut conn) = self.conn {
                let mut inner = conn.borrow_mut();
                try_ready!(inner.transport.poll_complete());

                if let Some((last_pos, token)) = try_ready!(inner.transport.read_token()) {
                    match token {
                        TdsResponseToken::ColMetaData(_) => {
                            self.already_triggered = true;
                            return Ok(Async::Ready(Some(R::from_connection(conn))))
                        },
                        TdsResponseToken::Done(ref done) => {
                            assert!(!done.status.contains(tokens::DONE_MORE));
                            self.done = true;
                            let old = self.already_triggered;
                            self.already_triggered = false;
                            // make sure to return exactly one time for each result set
                            if !old {
                                inner.transport.set_position(last_pos); // reinject
                                return Ok(Async::Ready(Some(R::from_connection(conn))))
                            }
                        },
                        tok => panic!("resultset: unexpected token: {:?}", tok)
                    }
                }
                if !self.done {
                    panic!("resultset: expected a token!");
                }
            }
        }
        self.conn = None;
        Ok(Async::Ready(None))
    }
}

impl<'a, I: Io> ResultSetStream<'a, I, QueryStream<'a, I>> {
    /// Only expect 1 result set (e.g. if you're only executing one query)
    /// and execute a given closure for the results of the first result set
    ///
    /// other result sets are silently ignored
    pub fn for_each_row<F>(self, f: F) -> ForEachRow<'a, I, ResultSetStream<'a, I, QueryStream<'a, I>>, F>
        where F: FnMut(<QueryStream<'a, I> as Stream>::Item) -> Result<(), TdsError>
    {
        ForEachRow::new(self, f)
    }
}

pub struct QueryStream<'a, I: Io + 'a> {
    conn: Option<&'a SqlConnection<I>>,
}

impl<'a, I: Io> Stream for QueryStream<'a, I> {
    type Item = QueryRow;
    type Error = TdsError;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        assert!(self.conn.is_some());

        if let Some(ref mut conn) = self.conn {
            let mut inner = conn.borrow_mut();
            try_ready!(inner.transport.poll_complete());

            loop {
                let token = try_ready!(inner.transport.read_token());
                match token {
                    None => panic!("query: expected token"),
                    Some((last_pos, token)) => match token {
                        TdsResponseToken::Row(row) => {
                            return Ok(Async::Ready(Some(QueryRow(row))));
                        },
                        // if this is the final done token, we need to reinject it for result set stream to handle it
                        TdsResponseToken::Done(ref done) if !done.status.contains(tokens::DONE_MORE) => {
                            inner.transport.set_position(last_pos);
                            break;
                        },
                        TdsResponseToken::Done(_) | TdsResponseToken::DoneInProc(_) => break,
                        x => panic!("query: unexpected token: {:?}", x),
                    }
                }
            }
        }

        self.conn = None;
        Ok(Async::Ready(None))
    }
}

impl<'a, I: Io> StmtResult<'a, I> for QueryStream<'a, I> {
    type Result = QueryStream<'a, I>;

    fn from_connection(conn: &'a SqlConnection<I>) -> QueryStream<'a, I> {
        QueryStream {
            conn: Some(conn),
        }
    }
}

pub struct ExecFuture<'a, I: Io + 'a> {
    conn: Option<&'a SqlConnection<I>>,
    /// whether only a Done token (that was previously injected) is the contents of this stream
    single_token: bool,
}

impl<'a, I: Io> Future for ExecFuture<'a, I> {
    /// amount of affected rows
    type Item = u64;
    type Error = TdsError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        assert!(self.conn.is_some());

        let mut ret: u64 = 0;
        if let Some(ref mut conn) = self.conn {
            let mut inner = conn.borrow_mut();
            try_ready!(inner.transport.poll_complete());

            loop {
                match try_ready!(inner.transport.read_token()) {
                    Some((last_pos, token)) => match token {
                        TdsResponseToken::Row(_) => {
                            self.single_token = false;
                        },
                        TdsResponseToken::Done(ref done) | TdsResponseToken::DoneInProc(ref done) | TdsResponseToken::DoneProc(ref done) => {
                            let final_token = match token {
                                TdsResponseToken::Done(_) | TdsResponseToken::DoneProc(_) => true,
                                _ => false
                            };
                            // if this is the final done token, we need to reinject it for result set stream to handle it
                            // (as in querying, if self.single_token it already was reinjected and would result in an infinite cycle)
                            if !done.status.contains(tokens::DONE_MORE) && self.single_token && final_token {
                                inner.transport.set_position(last_pos);
                            }
                            if done.status.contains(tokens::DONE_COUNT) {
                                ret = done.done_rows;
                            }
                            break;
                        },
                        x => panic!("exec: unexpected token: {:?}", x),
                    },
                    None =>  panic!("expected token")
                }
            }
        }
        self.conn = None;
        return Ok(Async::Ready(ret))
    }
}

impl<'a, I: Io> StmtResult<'a, I> for ExecFuture<'a, I> {
    type Result = ExecFuture<'a, I>;

    fn from_connection(conn: &'a SqlConnection<I>) -> ExecFuture<'a, I> {
        ExecFuture {
            conn: Some(conn),
            single_token: true,
        }
    }
}

#[derive(Debug)]
pub struct QueryRow(TokenRow);

pub trait QueryIdx: Sized {
    fn to_idx(&self, row: &QueryRow) -> Option<usize>;
}

impl<'a> QueryIdx for &'a str {
    fn to_idx(&self, row: &QueryRow) -> Option<usize> {
        for (i, column) in row.0.meta.columns.iter().enumerate() {
            if &column.col_name.as_str() == self {
                return Some(i)
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
    /// attempt to get a column's value for a given column index
    pub fn try_get<'a, I: QueryIdx, R: FromColumnData<'a>>(&'a self, idx: I) -> TdsResult<Option<R>> {
        let idx = match idx.to_idx(self) {
            Some(x) => x,
            None => return Ok(None),
        };

        let col_data = &self.0.columns[idx];
        R::from_column_data(col_data).map(Some)
    }

    /// retrieve a column's value for a given column index
    pub fn get<'a, I: QueryIdx, R: FromColumnData<'a>>(&'a self, idx: I) -> R {
        self.try_get(idx)
            .unwrap()
            .unwrap()
    }
}
