use futures::{Async, Poll, Stream, Sink};
use tokio_core::io::Io;
use tokens::{TdsResponseToken, TokenRow};
use types::FromColumnData;
use {SqlConnection, StmtResult, TdsError, TdsResult};

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
                    Some(TdsResponseToken::ColMetaData(meta)) => {
                    },
                    Some(TdsResponseToken::Row(row)) => {
                        return Ok(Async::Ready(Some(QueryRow(row))));
                    },
                    Some(TdsResponseToken::Done(done)) => {
                        break;
                    },
                    _ => unreachable!(),
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
            if &column.name.as_str() == self {
                return Some(i)
            }
        }
        None
    }
}

impl QueryIdx for usize {
    fn to_idx(&self, row: &QueryRow) -> Option<usize> {
        Some(*self)
    }
}

impl QueryRow {
    /// attempt to get a column's value for a given column index
    pub fn try_get<I: QueryIdx, R: FromColumnData>(&self, idx: I) -> TdsResult<Option<R>> {
        let idx = match idx.to_idx(self) {
            Some(x) => x,
            None => return Ok(None),
        };

        let col_data = &self.0.columns[idx];
        R::from_column_data(col_data).map(Some)
    }

    /// retrieve a column's value for a given column index
    pub fn get<I: QueryIdx, R: FromColumnData>(&self, idx: I) -> R {
        self.try_get(idx)
            .unwrap()
            .unwrap()
    }
}
