use crate::tds::stream::ReceivedToken;
use crate::{row::ColumnType, Column, Row};
use futures::{ready, stream::BoxStream, Stream, StreamExt, TryStreamExt};
use std::{
    fmt::Debug,
    pin::Pin,
    sync::Arc,
    task::{self, Poll},
};

#[derive(Debug, Copy, Clone, PartialEq)]
pub(crate) enum QueryStreamState {
    Initial,
    HasPotentiallyNext,
    HasNext,
    GotRowsAffected,
    Done,
}

/// A stream of rows, needed for queries returning data.
pub struct QueryStream<'a> {
    token_stream: BoxStream<'a, crate::Result<ReceivedToken>>,
    current_columns: Option<Arc<Vec<Column>>>,
    previous_columns: Option<Arc<Vec<Column>>>,
    pub(crate) state: QueryStreamState,
}

impl<'a> Debug for QueryStream<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Querystream")
            .field(
                "token_stream",
                &"BoxStream<'a, crate::Result<ReceivedToken>>",
            )
            .field("current_columns", &self.current_columns)
            .field("previous_columns", &self.previous_columns)
            .field("state", &self.state)
            .finish()
    }
}

impl<'a> QueryStream<'a> {
    pub(crate) fn new(token_stream: BoxStream<'a, crate::Result<ReceivedToken>>) -> Self {
        Self {
            token_stream,
            current_columns: None,
            previous_columns: None,
            state: QueryStreamState::Initial,
        }
    }

    pub(crate) async fn fetch_metadata(&mut self) -> crate::Result<()> {
        loop {
            match self.token_stream.try_next().await? {
                Some(ReceivedToken::NewResultset(meta)) => {
                    let columns = meta
                        .columns
                        .iter()
                        .map(|x| Column {
                            name: x.col_name.clone(),
                            column_type: ColumnType::from(&x.base.ty),
                        })
                        .collect::<Vec<_>>();

                    self.store_columns(columns);

                    return Ok(());
                }
                Some(ReceivedToken::DoneInProc(done))
                | Some(ReceivedToken::DoneProc(done))
                | Some(ReceivedToken::Done(done)) => {
                    if !done.has_more() {
                        self.state = QueryStreamState::Done;
                    }

                    return Ok(());
                }
                _ => return Ok(()),
            }
        }
    }

    pub(crate) fn columns(&self) -> Option<&[Column]> {
        let cols = match self.state {
            QueryStreamState::HasNext => self.previous_columns.as_ref(),
            _ => self.current_columns.as_ref(),
        };

        cols.map(|cols| cols.as_slice())
    }

    fn store_columns(&mut self, columns: Vec<Column>) {
        if let Some(columns) = self.current_columns.take() {
            self.previous_columns = Some(columns);
        }

        self.current_columns = Some(Arc::new(columns));

        if let QueryStreamState::HasPotentiallyNext = self.state {
            self.state = QueryStreamState::HasNext;
        };
    }
}

impl<'a> Stream for QueryStream<'a> {
    type Item = crate::Result<Row>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            match this.state {
                QueryStreamState::Initial
                | QueryStreamState::HasPotentiallyNext
                | QueryStreamState::GotRowsAffected => (),
                _ => return Poll::Ready(None),
            }

            let token = match ready!(this.token_stream.poll_next_unpin(cx)) {
                Some(res) => res?,
                None => return Poll::Ready(None),
            };

            return match token {
                ReceivedToken::NewResultset(meta) => {
                    let column_meta = meta
                        .columns
                        .iter()
                        .map(|x| Column {
                            name: x.col_name.clone(),
                            column_type: ColumnType::from(&x.base.ty),
                        })
                        .collect::<Vec<_>>();

                    this.store_columns(column_meta);

                    continue;
                }
                ReceivedToken::Row(data) => {
                    let columns = this.current_columns.as_ref().unwrap().clone();
                    Poll::Ready(Some(Ok(Row { columns, data })))
                }
                ReceivedToken::Done(ref done)
                | ReceivedToken::DoneProc(ref done)
                | ReceivedToken::DoneInProc(ref done) => {
                    if !done.has_more() {
                        this.state = QueryStreamState::Done;
                    } else {
                        // Justification here: if there are no columns set this is because
                        // we haven't yet recieved any tabular resultsets from the server.
                        // and therefore we can just skip to ensure that we don't cause the
                        // QueryResult implementation to start a new resultset.
                        //
                        // Otherwise, any received DoneInProc etc tokens will just add 0 more rows
                        // into the end of the resultset, so they wont' affect it and this means
                        // we don't need to handle those cases.
                        // Later if we decide to include the rows affected amounts we woud need
                        // to create a new resultset each time.
                        if this.current_columns.is_none() {
                            this.state = QueryStreamState::GotRowsAffected;
                        } else {
                            this.state = QueryStreamState::HasPotentiallyNext;
                        }
                    }
                    continue;
                }
                _ => continue,
            };
        }
    }
}
