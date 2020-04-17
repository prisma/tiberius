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

pub struct ResultSet<'a> {
    stream: QueryStream<'a, Connection>,
}

impl<'a> ResultSet<'a> {
    pub fn new(connection: &'a mut Connection, context: Arc<Context>) -> Self {
        let stream = QueryStream::new(connection, context);
        Self { stream }
    }

    pub fn next_resultset(&mut self) -> bool {
        if self.stream.state == QueryStreamState::HasNext {
            self.stream.state = QueryStreamState::Initial;
            true
        } else {
            false
        }
    }
}

impl<'a> Stream for ResultSet<'a> {
    type Item = crate::Result<Row>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.get_mut().stream).poll_next(cx)
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
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
    pub fn new(packet_stream: &'a mut S, context: Arc<Context>) -> Self {
        let prepared_stream = PreparedStream::new(packet_stream, context);

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
