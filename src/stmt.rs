use std::borrow::Cow;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::sync::Arc;
use futures::{Async, Future, Poll, Stream};
use tokio_core::io::Io;
use query::QueryStream;
use tokens::{self, DoneStatus, TdsResponseToken, TokenRpcRequest, TokenColMetaData, RpcParam, RpcProcIdValue, RpcProcId, RpcOptionFlags, RpcStatusFlags, WriteToken};
use types::{ColumnData, ToColumnData};
use {SqlConnection, StmtResult, TdsError};

/// a type which can be translated as an SQL type (e.g. nvarchar) and is serializable (as `ColumnData`)
/// e.g. for usage within a ROW token
pub trait ToSql : ToColumnData {
    fn to_sql(&self) -> &'static str;
}

/// a handle identifying a server-side prepared statement for a specific connection
/// and a specific set of parameter types
struct StatementHandle {
    handle: i32,
    signature: Vec<&'static str>,
}

/// a prepared statement which is prepared on the first execution
/// (which is a technical requirement since you need to know the types)
pub struct LazyPreparedStatement<'c, 'a, I: 'c + Io> {
    pub conn: &'c SqlConnection<I>,
    pub sql: Cow<'a, str>,
    meta: RefCell<Option<Arc<TokenColMetaData>>>,
    handle: RefCell<Option<StatementHandle>>,
}

impl<'c, 'a, I: 'c + Io> LazyPreparedStatement<'c, 'a, I> {
    pub fn new(conn: &'c SqlConnection<I>, sql: Cow<'a, str>) -> LazyPreparedStatement<'c, 'a, I> {
        LazyPreparedStatement {
            conn: conn,
            sql: sql,
            meta: RefCell::new(None),
            handle: RefCell::new(None),
        }
    }
}

impl<'c, 'a, I: 'c + Io> LazyPreparedStatement<'c, 'a, I> {
    pub fn query<'s, 'b>(&'s self, params: &'b [&'b ToSql]) -> PreparedStmtStream<'s, 'a, 'b, 'c, I, QueryStream<'c, I>> {
        PreparedStmtStream {
            stmt: Some(self),
            params: params,
            prep: PrepState::Initial,
            _marker: PhantomData,
        }
    }
}

enum PrepState<'a, 'b> {
    Initial,
    Req(TokenRpcRequest<'a>),
    Reading,
    Prepared,
    ExecReq(TokenRpcRequest<'b>),
    ExecReading,
    Done,
}

/// a future which handles the execution of a prepared statement and translates it
/// into the wished result (e.g. QueryStream)
pub struct PreparedStmtStream<'s, 'a: 's, 'b: 's, 'c: 's, I: 'c + Io, R: StmtResult<'c, I>> {
    stmt: Option<&'s LazyPreparedStatement<'c, 'a, I>>,
    params: &'b [&'b ToSql],

    prep: PrepState<'a, 'b>,

    /// This marker simply is used to allow this struct to be generic over a possible
    /// result, which allows us to share all state logic within this struct
    /// (e.g. we don't need a query specific future)
    _marker: PhantomData<*const R>,
}

impl<'s, 'a: 's, 'b: 's, 'c: 's, I: 'c + Io, R: StmtResult<'c, I>> PreparedStmtStream<'s, 'a, 'b, 'c, I, R> {
    fn do_prepare(&mut self) -> TokenRpcRequest<'a> {
        let mut param_str = String::with_capacity(10 * self.params.len());
        // determine the types from the given params
        for (i, param) in self.params.iter().enumerate() {
            if i > 0 {
                param_str.push(',')
            }
            param_str.push_str(&format!("@P{} ", i + 1));
            param_str.push_str(param.to_sql());
        }
        // call sp_prepare to get a handle we can execute
        TokenRpcRequest {
            proc_id: RpcProcIdValue::Id(RpcProcId::SpPrepare),
            flags: RpcOptionFlags::empty(),
            params: vec![
                RpcParam {
                    name: Cow::Borrowed("handle"),
                    flags: tokens::RPC_PARAM_BY_REF_VALUE,
                    value: ColumnData::I32(0),
                },
                RpcParam {
                    name: Cow::Borrowed("params"),
                    flags: RpcStatusFlags::empty(),
                    value: ColumnData::String(Cow::Owned(param_str))
                },
                RpcParam {
                    name: Cow::Borrowed("stmt"),
                    flags: RpcStatusFlags::empty(),
                    value: ColumnData::String(self.stmt.map(|stmt| stmt.sql.clone()).unwrap()),
                }
            ],
        }
    }

    fn do_exec(&mut self) -> TokenRpcRequest<'b> {
        let mut params_meta = vec![
            RpcParam {
                name: Cow::Borrowed("handle"),
                flags: RpcStatusFlags::empty(),
                value: ColumnData::I32(self.stmt.map(|stmt| stmt.handle.borrow().as_ref().map(|h| h.handle).unwrap()).unwrap()),
            },
        ];
        for (i, param) in self.params.iter().enumerate() {
            params_meta.push(RpcParam {
                name: Cow::Owned(format!("@P{}", i+1)),
                flags: RpcStatusFlags::empty(),
                value: param.to_column_data(),
            });
        }

        TokenRpcRequest {
            // as freeTDS, use sp_execute since SpPrepare (as int) seems broken, even microsofts odbc driver seems to use this
            proc_id: RpcProcIdValue::Name(Cow::Borrowed("sp_execute")),
            flags: tokens::RPC_NO_META,
            params: params_meta,
        }
    }
}

impl<'s, 'a, 'b, 'c, I: Io, R: StmtResult<'c, I>> Stream for PreparedStmtStream<'s, 'a, 'b, 'c, I, R> {
    type Item = R::Result;
    type Error = TdsError;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        if self.stmt.is_none() {
            return Ok(Async::Ready(None))
        }

        let trans = &mut self.stmt.map(|stmt| stmt.conn.borrow_mut()).unwrap().transport;

        // call sp_prepare, if we don't already have a (valid) handle
        if let PrepState::Initial = self.prep {
            let already_prepared = self.stmt.map(|stmt| {
                let handle = stmt.handle.borrow();
                // check if the param-type signature matches, if we have a handle
                handle.is_some() && handle.as_ref().map(|handle| handle.signature.iter().cloned().eq(self.params.iter().map(|x| x.to_sql()))).unwrap()
            });
            self.prep = if let Some(true) = already_prepared {
                PrepState::Prepared
            } else {
                PrepState::Req(self.do_prepare())
            };
        }

        let ready = match self.prep {
            PrepState::Req(ref prep_req) => try_ready!(prep_req.write_token(trans).map(|f| f.map(|_| true))),
            _ => false
        };

        if ready {
            self.prep = PrepState::Reading;
        }

        // receive and handle the result of sp_prepare
        while let PrepState::Reading = self.prep {
            match try_ready!(trans.read_token()).map(|x| x.1) {
                Some(TdsResponseToken::ColMetaData(meta)) => {
                    *self.stmt.map(|stmt| stmt.meta.borrow_mut()).unwrap() = Some(meta.clone());
                },
                Some(TdsResponseToken::DoneProc(done)) => {
                    assert_eq!(done.status, DoneStatus::empty());
                    self.prep = PrepState::Prepared;
                },
                // this simply notifies us that a DoneProc is following (DONE_MORE)
                Some(TdsResponseToken::DoneInProc(_)) => (),
                Some(TdsResponseToken::ReturnStatus(status)) => {
                    assert_eq!(status, 0);
                },
                Some(TdsResponseToken::ReturnValue(retval)) => {
                    assert_eq!(retval.param_name.as_str(), "handle");
                    *self.stmt.map(|stmt| stmt.handle.borrow_mut()).unwrap() = Some(match retval.value {
                        ColumnData::I32(val) => StatementHandle {
                            handle: val,
                            signature: self.params.iter().map(|x| x.to_sql()).collect(),
                        },
                        _ => unreachable!()
                    });
                },
                _ => unimplemented!()
            }
        }

        if let PrepState::Prepared = self.prep {
            self.prep = PrepState::ExecReq(self.do_exec());
        }
        let ready = match self.prep {
            PrepState::ExecReq(ref prep_req) => try_ready!(prep_req.write_token(trans).map(|f| f.map(|_| true))),
            _ => false
        };

        if ready {
            self.prep = PrepState::ExecReading;
        }

        while let PrepState::ExecReading = self.prep {
            match try_ready!(trans.read_token()).map(|x| x.1) {
                Some(TdsResponseToken::ColMetaData(meta)) => {
                    assert_eq!(meta.columns.len(), 0);
                    return Ok(Async::Ready(Some(R::from_connection(self.stmt.map(|stmt| stmt.conn).unwrap()))));
                },
                Some(TdsResponseToken::ReturnStatus(status)) => {
                    assert_eq!(status, 0);
                },
                Some(TdsResponseToken::DoneProc(done)) => {
                    // we've read each query result, we're done with the current sp_exec, this stream may rest
                    assert_eq!(done.status, DoneStatus::empty());
                    break;
                },
                tok => panic!("execreading: unexpected token: {:?}", tok)
            }
        }

        // this stream is done, make sure it cannot be executed again
        self.stmt.take().unwrap();
        Ok(Async::Ready(None))
    }
}

impl<'s, 'a, 'b, 'c, I: Io> PreparedStmtStream<'s, 'a, 'b, 'c, I, QueryStream<'c, I>> {
    /// Only expect 1 result set (e.g. if you're only executing one query)
    /// and execute a given closure for the results of the first result set
    ///
    /// other result sets are silently ignored
    pub fn for_each_row<F>(self, f: F) -> ForEachRow<'c, I, PreparedStmtStream<'s, 'a, 'b, 'c, I, QueryStream<'c, I>>, F>
        where F: FnMut(<QueryStream<'c, I> as Stream>::Item) -> Result<(), TdsError>
    {
        ForEachRow::new(self, f)
    }
}

/// iterate over resultsets and only return the rows of the first one
/// but handle/consume the entire result set so that we're ready to continue
/// after the execution of this
pub struct ForEachRow<'c, I: 'c + Io, S: Stream<Item=QueryStream<'c, I>, Error=<QueryStream<'c, I> as Stream>::Error>, F> {
    stream: Option<S>,
    f: F,
    idx: usize,
    resultset: Option<QueryStream<'c, I>>,
}

impl<'c, I: Io, S, F> ForEachRow<'c, I, S, F>
     where S: Stream<Item=QueryStream<'c, I>, Error=<QueryStream<'c, I> as Stream>::Error>,
           F: FnMut(<QueryStream<'c, I> as Stream>::Item) -> Result<(), TdsError>
{
    pub fn new(stream: S, f: F) -> ForEachRow<'c, I, S, F> {
        ForEachRow {
            stream: Some(stream),
            f: f,
            idx: 0,
            resultset: None,
        }
    }
}

impl<'c, I: Io, S, F> Future for ForEachRow<'c, I, S, F>
    where S: Stream<Item=QueryStream<'c, I>, Error=<QueryStream<'c, I> as Stream>::Error>,
          F: FnMut(<QueryStream<'c, I> as Stream>::Item) -> Result<(), TdsError>
{
    type Item = ();
    type Error = <QueryStream<'c, I> as Stream>::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let stream = self.stream.as_mut().unwrap();
        loop {
            while let Some(ref mut resultset) = self.resultset {
                match try_ready!(resultset.poll()) {
                    None => break,
                    Some(row) => if self.idx == 1 {
                        try!((self.f)(row))
                    },
                }
            }
            // ensure we do not poll the same resultset again
            self.resultset = None;
            self.resultset = try_ready!(stream.poll());
            if self.resultset.is_none() {
                break;
            }
            self.idx += 1;
        }
        Ok(Async::Ready(()))
    }
}

// TODO: will need a macro
impl ToSql for i32 {
    fn to_sql(&self) -> &'static str {
        "int"
    }
}
