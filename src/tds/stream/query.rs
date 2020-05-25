use crate::tds::{codec::DoneStatus, stream::ReceivedToken};
use crate::{row::ColumnType, Column, Error, Row};
use futures::{ready, Stream, StreamExt, TryStreamExt};
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
    Done,
}

/// A stream of rows, needed for queries returning data.
pub struct QueryStream<'a> {
    token_stream: Pin<Box<dyn Stream<Item = crate::Result<ReceivedToken>> + 'a>>,
    current_columns: Option<Arc<Vec<Column>>>,
    previous_columns: Option<Arc<Vec<Column>>>,
    pub(crate) state: QueryStreamState,
}

impl<'a> Debug for QueryStream<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Querystream")
            .field(
                "token_stream",
                &"Pin<Box<dyn Stream<Item = crate::Result<ReceivedToken>> + 'a>>",
            )
            .field("current_columns", &self.current_columns)
            .field("previous_columns", &self.previous_columns)
            .field("state", &self.state)
            .finish()
    }
}

impl<'a> QueryStream<'a> {
    pub(crate) fn new(
        token_stream: Pin<Box<dyn Stream<Item = crate::Result<ReceivedToken>> + 'a>>,
    ) -> Self {
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
                Some(ReceivedToken::Done(_)) => {
                    return Err(Error::Protocol("Never got result metadata".into()))
                }
                _ => continue,
            }
        }
    }

    pub(crate) fn columns(&self) -> &[Column] {
        match self.state {
            QueryStreamState::HasNext => &self.previous_columns.as_ref().unwrap(),
            _ => &self.current_columns.as_ref().unwrap(),
        }
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
                QueryStreamState::Initial | QueryStreamState::HasPotentiallyNext => (),
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
                    if !done.status.contains(DoneStatus::MORE) {
                        this.state = QueryStreamState::Done;
                    } else {
                        this.state = QueryStreamState::HasPotentiallyNext;
                    }
                    continue;
                }
                _ => continue,
            };
        }
    }
}
