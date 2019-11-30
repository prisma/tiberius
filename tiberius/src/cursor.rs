use std::borrow::Cow;
use std::sync::Arc;

use crate::protocol::rpc::{
    RpcOptionFlags, RpcParam, RpcProcId, RpcProcIdValue, RpcStatusFlags, TokenRpcRequest,
};
use crate::protocol::{self, ColumnData};
use crate::row;
use crate::{Error, MotoredFuture, QueryStream, ReceivedToken, Result, ResultSet};
use async_stream::try_stream;
use bitflags::bitflags;
use futures_util::future::FutureExt;
use futures_util::stream::StreamExt;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{self, mpsc};
use tracing::{self, debug_span, event, trace_span, Level};

bitflags! {
    struct ScrollOption: i32 {
        const FORWARD_ONLY = 0x04;
        /// READONLY, FORWARD_ONLY cursor with performance optimizations
        const FAST_FORWARD = 0x10;
        /// Immediately fetch first set of rows (saving 1 roundtrip)
        const AUTO_FETCH = 0x2000;
    }
}
bitflags! {
    struct ConcurrencyControlOption: i32 {
        const READ_ONLY = 1;
        const ALLOW_DIRECT = 0x2000;
    }
}
bitflags! {
    struct FetchOption: i32 {
        const NEXT = 0x02;
    }
}

pub struct Connection(pub(crate) crate::Connection);

impl Connection {
    /// Execute a simple query and return multiple resultsets which consist of multiple rows.
    ///
    /// # Warning
    /// Do not use this with any user specified input.  
    /// Please resort to prepared statements ([query](Client::query) or [prepare](Client::prepare)) in order to prevent SQL-Injections.  
    pub async fn simple_query(&self, query: &str) -> Result<impl ResultSet<Result<row::Row>>> {
        let self_ = &self.0;

        let span = debug_span!("simple_query", query = query);
        let _enter = span.enter();

        let writer_arc = self_.writer.clone();
        let mut writer = writer_arc.lock().await;

        // Subscribe for results
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let result_callback_queue = self_.result_callback_queue.clone();
        result_callback_queue.send(sender.clone()).expect("TODO");

        // Fire a query (TODO: use for simple_exec?)
        event!(Level::DEBUG, "WRITING simple QUERY (cursored)");
        let mut conn_handler = self_.conn_handler.clone();
        let req = open_req(query);
        req.write_to(&self_.ctx, &mut *writer).await?;
        ::std::mem::drop(writer);

        let mut cursor_id = None;
        while let Some(msg) = (MotoredFuture {
            motor: &mut conn_handler,
            future: receiver.next().map(Ok),
        })
        .await?
        {
            println!("cursor init {:?}", msg);
            match msg {
                ReceivedToken::ReturnStatus(status) => {
                    assert_eq!(status, 0); // TODO
                }
                ReceivedToken::ReturnValue(val) if val.param_name == "cursor" => {
                    cursor_id = Some(val.value);
                }
                ReceivedToken::DoneProc(_) => break,
                _ => (),
            }
        }

        let qs = QueryStream {
            conn_handler,
            results: Stream {
                ctx: self.0.ctx.clone(),
                cursor_id: cursor_id,
                result_callback_queue,
                sender,
                receiver,
                writer: writer_arc,
            }
            .into_stream(),
            done: false,
            has_next_resultset: false,
        };
        Ok(qs)
    }
}

pub(crate) struct Stream<S> {
    pub(crate) cursor_id: Option<ColumnData>,
    pub(crate) result_callback_queue: mpsc::UnboundedSender<mpsc::UnboundedSender<ReceivedToken>>,
    pub(crate) sender: mpsc::UnboundedSender<ReceivedToken>,
    pub(crate) receiver: S,
    pub(crate) ctx: Arc<protocol::Context>,
    pub(crate) writer: Arc<sync::Mutex<Box<dyn AsyncWrite + Unpin>>>,
}

const CURSOR_ROWS: u64 = 128;

impl<S> Stream<S>
where
    S: futures_util::stream::Stream<Item = ReceivedToken> + Unpin + 'static,
{
    pub(crate) fn into_stream(
        mut self,
    ) -> impl futures_util::stream::Stream<Item = Result<ReceivedToken>> + Unpin {
        Box::pin(try_stream! {
            let mut pending_rows = 0u64;
            let mut fetched_rows = 0u64;
            let mut closing = false;

            loop {
                // Prefetch new rows, before we reach the end of buffered rows
                if pending_rows - fetched_rows < (CURSOR_ROWS / 2) {
                    pending_rows += CURSOR_ROWS;

                    let req = fetch_req(self.cursor_id.clone().unwrap());
                    // Reregister callbacks for receiver
                    self.result_callback_queue.send(self.sender.clone()).expect("TODO");
                    let mut writer = self.writer.lock().await;
                    req.write_to(&self.ctx, &mut *writer).await?;
                }

                let token = self.receiver.next().await;
                match token {
                    Some(next @ ReceivedToken::Row(_)) => {
                        yield next;
                        fetched_rows += 1;
                    },
                    Some(ReceivedToken::Done(done)) => {
                        if done.done_rows < CURSOR_ROWS {
                            event!(Level::TRACE, close_cursor=tracing::field::debug(&self.cursor_id));
                            let req = close_request(self.cursor_id.take().unwrap());
                            // Reregister callbacks for receiver
                            self.result_callback_queue.send(self.sender.clone()).expect("TODO");
                            let mut writer = self.writer.lock().await;
                            req.write_to(&self.ctx, &mut *writer).await?;
                            closing = true;
                        }
                    }
                    Some(ReceivedToken::ReturnStatus(fetch_return_status)) => {
                        assert_eq!(fetch_return_status, 0);
                    },
                    Some(ReceivedToken::DoneProc(_)) => {
                        if (closing) {
                            break;
                        }
                    }
                    Some(x) => {
                        panic!("TODO: {:?}", x)
                    },
                    None => break,
                }
            }
        })
    }
}

fn close_request(cursor_id: ColumnData) -> TokenRpcRequest<'static> {
    TokenRpcRequest {
        proc_id: RpcProcIdValue::Id(RpcProcId::SpCursorClose),
        flags: RpcOptionFlags::empty(),
        params: vec![RpcParam {
            name: Cow::Borrowed("cursor"),
            flags: RpcStatusFlags::empty(),
            value: cursor_id,
        }],
    }
}

fn fetch_req(cursor_id: ColumnData) -> TokenRpcRequest<'static> {
    let rpc_params = vec![
        RpcParam {
            name: Cow::Borrowed("cursor"),
            flags: RpcStatusFlags::empty(),
            value: cursor_id,
        },
        RpcParam {
            name: Cow::Borrowed("fetchtype"),
            flags: RpcStatusFlags::empty(),
            value: ColumnData::I32(FetchOption::NEXT.bits),
        },
        RpcParam {
            name: Cow::Borrowed("rownum"),
            flags: RpcStatusFlags::empty(),
            value: ColumnData::I32(0), // unused for FetchOption::NEXT, but params are position-based
        },
        RpcParam {
            name: Cow::Borrowed("nrows"),
            flags: RpcStatusFlags::empty(),
            value: ColumnData::I32(128),
        },
    ];
    TokenRpcRequest {
        proc_id: RpcProcIdValue::Id(RpcProcId::SpCursorFetch),
        flags: RpcOptionFlags::empty(),
        params: rpc_params,
    }
}

pub(crate) fn open_req(query: &str) -> TokenRpcRequest<'static> {
    let rpc_params = vec![
        RpcParam {
            name: Cow::Borrowed("cursor"),
            flags: RpcStatusFlags::PARAM_BY_REF_VALUE,
            value: ColumnData::I32(0),
        },
        RpcParam {
            name: Cow::Borrowed("stmt"),
            flags: RpcStatusFlags::empty(),
            value: ColumnData::String(query.to_owned()),
        },
        RpcParam {
            name: Cow::Borrowed("scrollopt"),
            flags: RpcStatusFlags::PARAM_BY_REF_VALUE,
            value: ColumnData::I32((ScrollOption::FAST_FORWARD).bits),
        },
        RpcParam {
            name: Cow::Borrowed("ccopt"),
            flags: RpcStatusFlags::PARAM_BY_REF_VALUE,
            value: ColumnData::I32(
                (ConcurrencyControlOption::READ_ONLY | ConcurrencyControlOption::ALLOW_DIRECT).bits,
            ),
        },
    ];
    TokenRpcRequest {
        proc_id: RpcProcIdValue::Id(RpcProcId::SpCursorOpen),
        flags: RpcOptionFlags::empty(),
        params: rpc_params,
    }
}
