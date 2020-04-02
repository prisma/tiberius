use super::{ReceivedToken, TokenStream};
use crate::protocol::{
    codec::{ColumnData, DoneStatus, Packet},
    Context,
};
use atomic::Ordering;
use futures::{ready, Stream, StreamExt};
use std::{
    pin::Pin,
    sync::{
        atomic::{self, AtomicI32},
        Arc,
    },
    task::{self, Poll},
};

pub(crate) struct PreparedStream<'a, S> {
    token_stream: TokenStream<'a, S>,
    read_ahead: Option<ReceivedToken>,
    stmt_handle: Arc<AtomicI32>,
}

impl<'a, S> PreparedStream<'a, S>
where
    S: Stream<Item = crate::Result<Packet>> + Unpin + 'a,
{
    pub fn new(
        packet_stream: &'a mut S,
        stmt_handle: Arc<AtomicI32>,
        context: &'a Context,
    ) -> Self {
        Self {
            token_stream: TokenStream::new(packet_stream, context),
            read_ahead: None,
            stmt_handle,
        }
    }
}

impl<'a, S> Stream for PreparedStream<'a, S>
where
    S: Stream<Item = crate::Result<Packet>> + Unpin + 'a,
{
    type Item = crate::Result<ReceivedToken>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(ReceivedToken::NewResultset(_)) = self.read_ahead {
                return Poll::Ready(Some(Ok(self.read_ahead.take().unwrap())));
            }

            let item = match ready!(self.token_stream.poll_next_unpin(cx)) {
                Some(res) => res?,
                None => return Poll::Ready(None),
            };

            return match item {
                token @ ReceivedToken::NewResultset(_) => {
                    // Make sure the held back DONEINPROC token finds its way to the querystream
                    if let Some(read_ahead) = self.read_ahead.take() {
                        self.read_ahead = Some(token);
                        return Poll::Ready(Some(Ok(read_ahead)));
                    }
                    Poll::Ready(Some(Ok(token)))
                }
                ReceivedToken::Done(done) if done.status.contains(DoneStatus::MORE) => {
                    // we do not know yet, if what follows is the trailer of the
                    // stored procedure call or another resultset
                    self.read_ahead = Some(ReceivedToken::Done(done));
                    continue;
                }
                ReceivedToken::DoneProc(done) => {
                    // ... other stored procedures that we "called"
                    if done.status.contains(DoneStatus::MORE) {
                        continue;
                    }
                    // signal completion of all resultsets, when the stored procedure completed
                    Poll::Ready(Some(Ok(ReceivedToken::Done(done))))
                }
                // TODO: ensure it's the "last" one
                ReceivedToken::ReturnValue(ref ret_val) => {
                    let handle = match ret_val.value {
                        ColumnData::I32(handle) => handle,
                        _ => unreachable!(),
                    };
                    // TODO: think about multiple competing prepares (=> we prepared the same thing multiple times,
                    //       because stmt handle not ready yet and 0 is still stored before updated by a successful prepare)
                    self.stmt_handle.store(handle, Ordering::SeqCst); // TODO
                    continue;
                }
                ReceivedToken::ReturnStatus(_) => continue,
                item => Poll::Ready(Some(Ok(item))),
            };
        }
    }
}
