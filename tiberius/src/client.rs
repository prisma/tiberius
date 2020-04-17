mod builder;
mod connection;

pub use builder::*;
pub use connection::*;

use crate::{
    prepared,
    protocol::{
        codec::{self, RpcOptionFlags, RpcStatusFlags},
        stream::ResultSet,
        Context,
    },
    statement::{private, ToStatement},
};
use codec::{ColumnData, PacketHeader, RpcParam, RpcProcId, RpcProcIdValue, TokenRpcRequest};
use std::{
    borrow::Cow,
    sync::{atomic, Arc},
};

#[derive(Debug, Clone)]
pub enum AuthMethod {
    None,
    /// SQL Server integrated authentication
    SqlServer {
        user: String,
        password: String,
    },
    /// Windows authentication
    Windows {
        user: String,
        password: String,
    },
    /// Windows-integrated Authentication (SSPI / sspi)
    WindowsIntegrated,
}

impl AuthMethod {
    pub fn sql_server(user: impl ToString, password: impl ToString) -> Self {
        Self::SqlServer {
            user: user.to_string(),
            password: password.to_string(),
        }
    }

    pub fn windows(user: impl ToString, password: impl ToString) -> Self {
        Self::Windows {
            user: user.to_string(),
            password: password.to_string(),
        }
    }
}

pub struct Client {
    connection: Connection,
    context: Arc<Context>,
}

impl Client {
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    pub async fn query<'a, 'b, T: ?Sized>(
        &'a mut self,
        stmt: &'b T,
        params: &'b [&'b dyn prepared::ToSql],
    ) -> crate::Result<ResultSet<'a>>
    where
        'a: 'b,
        T: ToStatement + 'a,
    {
        match stmt.to_stmt() {
            private::StatementRepr::QueryString(ref query) => {
                self.sp_execute_sql(query, params).await
            }
            private::StatementRepr::Statement(ref stmt) => {
                let query = &stmt.query;
                let mut query_signature = format!("{}:{}:", query.len(), query);

                for param in params {
                    query_signature += param.to_sql().0;
                }

                let mut inserted = true;

                let stmt_handle = stmt
                    .handles
                    .lock()
                    .entry(query_signature)
                    .and_modify(|_| inserted = false)
                    .or_insert_with(|| Arc::new(atomic::AtomicI32::new(0)))
                    .clone();

                if inserted {
                    return self.sp_execute_sql(query, params).await;
                }

                if stmt_handle.load(atomic::Ordering::SeqCst) == 0 {
                    return self.sp_prep_exec(stmt_handle, query, params).await;
                }

                // sp_execute
                self.sp_execute(stmt_handle, params).await
            }
        }
    }

    async fn rpc_perform_query<'a, 'b>(
        &'a mut self,
        proc_id: RpcProcId,
        mut rpc_params: Vec<RpcParam<'static>>,
        params: &'b [&'b dyn prepared::ToSql],
        stmt_handle: Arc<atomic::AtomicI32>,
    ) -> crate::Result<ResultSet<'a>>
    where
        'a: 'b,
    {
        let mut param_str = String::new();

        for (i, param) in params.iter().enumerate() {
            if i > 0 {
                param_str.push(',')
            }
            param_str.push_str(&format!("@P{} ", i + 1));
            let (sql_type, param_data) = param.to_sql();
            param_str.push_str(sql_type);

            rpc_params.push(RpcParam {
                name: Cow::Owned(format!("@P{}", i + 1)),
                flags: RpcStatusFlags::empty(),
                value: param_data,
            });
        }

        if let Some(params) = rpc_params.iter_mut().find(|x| x.name == "params") {
            params.value = ColumnData::String(param_str.into());
        }

        let req = TokenRpcRequest {
            proc_id: RpcProcIdValue::Id(proc_id),
            flags: RpcOptionFlags::empty(),
            params: rpc_params,
        };

        self.connection
            .send(PacketHeader::rpc(&self.context), req)
            .await?;

        Ok(ResultSet::new(
            &mut self.connection,
            stmt_handle,
            &self.context,
        ))
    }

    async fn sp_execute_sql<'a, 'b>(
        &'a mut self,
        query: &'b str,
        params: &'b [&'b dyn prepared::ToSql],
    ) -> crate::Result<ResultSet<'a>>
    where
        'a: 'b,
    {
        let rpc_params = vec![
            RpcParam {
                name: Cow::Borrowed("stmt"),
                flags: RpcStatusFlags::empty(),
                value: ColumnData::String(query.to_string().into()),
            },
            RpcParam {
                name: Cow::Borrowed("params"),
                flags: RpcStatusFlags::empty(),
                value: ColumnData::I32(0),
            },
        ];

        let dummy = Arc::new(atomic::AtomicI32::new(0));

        let res = self
            .rpc_perform_query(RpcProcId::SpExecuteSQL, rpc_params, params, dummy)
            .await?;

        Ok(res)
    }

    async fn sp_prep_exec<'a, 'b>(
        &'a mut self,
        ret_handle: Arc<atomic::AtomicI32>,
        query: &'b str,
        params: &'b [&'b dyn prepared::ToSql],
    ) -> crate::Result<ResultSet<'a>>
    where
        'a: 'b,
    {
        let rpc_params = vec![
            RpcParam {
                name: Cow::Borrowed("handle"),
                flags: RpcStatusFlags::PARAM_BY_REF_VALUE,
                value: ColumnData::I32(0),
            },
            RpcParam {
                name: Cow::Borrowed("params"),
                flags: RpcStatusFlags::empty(),
                value: ColumnData::I32(0),
            },
            RpcParam {
                name: Cow::Borrowed("stmt"),
                flags: RpcStatusFlags::empty(),
                value: ColumnData::String(query.to_string().into()),
            },
        ];

        self.rpc_perform_query(RpcProcId::SpPrepExec, rpc_params, params, ret_handle)
            .await
    }

    async fn sp_execute<'a, 'b>(
        &'a mut self,
        stmt_handle: Arc<atomic::AtomicI32>,
        params: &'b [&'b dyn prepared::ToSql],
    ) -> crate::Result<ResultSet<'a>>
    where
        'a: 'b,
    {
        let rpc_params = vec![RpcParam {
            // handle (using "handle" here makes RpcProcId::SpExecute not work and requires RpcProcIdValue::NAME, wtf)
            // not specifying the name is better anyways to reduce overhead on execute
            name: Cow::Borrowed(""),
            flags: RpcStatusFlags::empty(),
            value: ColumnData::I32(stmt_handle.load(atomic::Ordering::SeqCst)),
        }];

        self.rpc_perform_query(RpcProcId::SpExecute, rpc_params, params, stmt_handle)
            .await
    }
}
