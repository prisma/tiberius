use super::ReceivedToken;
use crate::tds::codec::DoneStatus;
use futures::{ready, Stream};
use futures_util::StreamExt;
use std::{
    fmt::Debug,
    pin::Pin,
    task::{self, Poll},
};

pub(crate) struct PreparedStream<'a> {
    token_stream: Pin<Box<dyn Stream<Item = crate::Result<ReceivedToken>> + 'a>>,
    read_ahead: Option<ReceivedToken>,
}

impl<'a> Debug for PreparedStream<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedStream")
            .field(
                "token_stream",
                &"Stream<Item = crate::Result<ReceivedToken>>",
            )
            .field("read_ahead", &self.read_ahead)
            .finish()
    }
}

impl<'a> PreparedStream<'a> {
    pub fn new(
        token_stream: Pin<Box<dyn Stream<Item = crate::Result<ReceivedToken>> + 'a>>,
    ) -> Self {
        Self {
            token_stream,
            read_ahead: None,
        }
    }
}

impl<'a> Stream for PreparedStream<'a> {
    type Item = crate::Result<ReceivedToken>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.get_mut();
        loop {
            if let Some(ReceivedToken::NewResultset(_)) = this.read_ahead {
                return Poll::Ready(Some(Ok(this.read_ahead.take().unwrap())));
            }

            let stream = &mut this.token_stream;
            let item = match ready!(stream.poll_next_unpin(cx)) {
                Some(res) => res?,
                None => return Poll::Ready(None),
            };

            return match item {
                token @ ReceivedToken::NewResultset(_) => {
                    if let Some(read_ahead) = this.read_ahead.take() {
                        this.read_ahead = Some(token);
                        return Poll::Ready(Some(Ok(read_ahead)));
                    }
                    Poll::Ready(Some(Ok(token)))
                }
                ReceivedToken::Done(done) if done.status.contains(DoneStatus::MORE) => {
                    // we do not know yet, if what follows is the trailer of the
                    // stored procedure call or another resultset
                    this.read_ahead = Some(ReceivedToken::Done(done));
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
