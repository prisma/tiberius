use super::{ReceivedToken, TokenStream};
use crate::protocol::{
    codec::{DoneStatus, Packet},
    Context,
};
use futures::{ready, Stream, StreamExt};
use std::{
    pin::Pin,
    task::{self, Poll},
};

pub(crate) struct PreparedStream<'a, S> {
    token_stream: TokenStream<'a, S>,
    read_ahead: Option<ReceivedToken>,
}

impl<'a, S> PreparedStream<'a, S>
where
    S: Stream<Item = crate::Result<Packet>> + Unpin + 'a,
{
    pub fn new(packet_stream: &'a mut S, context: &'a Context) -> Self {
        Self {
            token_stream: TokenStream::new(packet_stream, context),
            read_ahead: None,
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
                ReceivedToken::ReturnValue(_) => continue,
                ReceivedToken::ReturnStatus(_) => continue,
                item => Poll::Ready(Some(Ok(item))),
            };
        }
    }
}
