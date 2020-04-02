use crate::protocol::{
    codec::{DoneStatus, Packet},
    stream::{prepared::PreparedStream, ReceivedToken},
    Context,
};
use crate::{client::Connection, Column, Row};
use futures::{ready, Stream, StreamExt};
use std::{
    pin::Pin,
    sync::Arc,
    task::{self, Poll},
};

pub type RowStream<'a> = QueryStream<'a, Connection>;

#[derive(Debug, Copy, Clone)]
enum QueryStreamState {
    Initial,
    HasPotentiallyNext,
    HasNext,
    Done,
}

pub struct QueryStream<'a, S> {
    prepared_stream: PreparedStream<'a, S>,
    current_columns: Option<Arc<Vec<Column>>>,
    state: QueryStreamState,
}

impl<'a, S> QueryStream<'a, S>
where
    S: Stream<Item = crate::Result<Packet>> + Unpin + 'a,
{
    pub fn new(
        packet_stream: &'a mut S,
        stmt_handle: Arc<std::sync::atomic::AtomicI32>,
        context: &'a Context,
    ) -> Self {
        let prepared_stream = PreparedStream::new(packet_stream, stmt_handle, context);

        Self {
            prepared_stream,
            current_columns: None,
            state: QueryStreamState::Initial,
        }
    }
}

impl<'a, S> Stream for QueryStream<'a, S>
where
    S: Stream<Item = crate::Result<Packet>> + Unpin + 'a,
{
    type Item = crate::Result<Row>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            match this.state {
                QueryStreamState::Initial | QueryStreamState::HasPotentiallyNext => (),
                _ => return Poll::Ready(None),
            }

            let token = match ready!(this.prepared_stream.poll_next_unpin(cx)) {
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
                        })
                        .collect::<Vec<_>>();

                    this.current_columns = Some(Arc::new(column_meta));

                    if let QueryStreamState::HasPotentiallyNext = this.state {
                        this.state = QueryStreamState::HasNext;
                    };

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
                _ => todo!(),
            };
        }
    }
}
