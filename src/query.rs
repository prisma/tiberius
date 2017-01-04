use futures::{Async, Poll, Stream, Sink};
use tokio_core::io::Io;
use stmt::ForEachRow;
use tokens::{self, TdsResponseToken, TokenRow};
use types::FromColumnData;
use {SqlConnection, StmtResult, TdsError, TdsResult};

/// a query result consists of multiple query streams (amount of executed queries = amount of results)
pub struct ResultSetStream<'a, I: 'a + Io> {
    pub conn: Option<&'a SqlConnection<I>>,
}

impl<'a, I: Io> Stream for ResultSetStream<'a, I> {
    type Item = QueryStream<'a, I>;
    type Error = <QueryStream<'a, I> as Stream>::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        assert!(self.conn.is_some());

        if let Some(ref mut conn) = self.conn {
            let mut inner = conn.borrow_mut();
            try_ready!(inner.transport.poll_complete());

            match try_ready!(inner.transport.read_token()).map(|x| x.1) {
                Some(TdsResponseToken::ColMetaData(_)) => {
                    let stream = QueryStream {
                        conn: Some(*conn)
                    };
                    return Ok(Async::Ready(Some(stream)))
                },
                Some(TdsResponseToken::Done(_)) => (),
                tok => panic!("resultset: unexpected token: {:?}", tok)
            }
        }
        self.conn = None;
        Ok(Async::Ready(None))
    }
}

impl<'a, I: Io> ResultSetStream<'a, I> {
    /// Only expect 1 result set (e.g. if you're only executing one query)
    /// and execute a given closure for the results of the first result set
    ///
    /// other result sets are silently ignored
    pub fn for_each_row<F>(self, f: F) -> ForEachRow<'a, I, ResultSetStream<'a, I>, F>
        where F: FnMut(<QueryStream<'a, I> as Stream>::Item) -> Result<(), TdsError>
    {
        ForEachRow::new(self, f)
    }
}

pub struct QueryStream<'a, I: Io + 'a> {
    pub conn: Option<&'a SqlConnection<I>>,
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
