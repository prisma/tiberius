mod builder;
mod connection;

pub use builder::*;
pub use connection::*;

use crate::{
    prepared,
    protocol::{
        codec::{self, RpcOptionFlags, RpcStatusFlags},
        stream::{ExecuteResult, QueryResult},
        Context,
    },
};
use codec::{ColumnData, PacketHeader, RpcParam, RpcProcId, RpcProcIdValue, TokenRpcRequest};
use std::{borrow::Cow, sync::Arc};

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

    pub async fn execute<'b, 'a: 'b>(
        &'a mut self,
        query: impl Into<Cow<'_, str>>,
        params: &'b [&'b dyn prepared::ToSql],
    ) -> crate::Result<ExecuteResult<'a>> {
        self.connection.flush_packets().await?;
        let rpc_params = Self::rpc_params(query);

        self.rpc_perform_query(RpcProcId::SpExecuteSQL, rpc_params, params)
            .await?;

        Ok(ExecuteResult::new(
            &mut self.connection,
            self.context.clone(),
        ))
    }

    pub async fn query<'a, 'b>(
        &'a mut self,
        query: impl Into<Cow<'a, str>>,
        params: &'b [&'b dyn prepared::ToSql],
    ) -> crate::Result<QueryResult<'a>>
    where
        'a: 'b,
    {
        self.connection.flush_packets().await?;
        let rpc_params = Self::rpc_params(query);

        self.rpc_perform_query(RpcProcId::SpExecuteSQL, rpc_params, params)
            .await?;

        Ok(QueryResult::new(&mut self.connection, self.context.clone()))
    }

    fn rpc_params<'a>(query: impl Into<Cow<'a, str>>) -> Vec<RpcParam<'a>> {
        vec![
            RpcParam {
                name: Cow::Borrowed("stmt"),
                flags: RpcStatusFlags::empty(),
                value: ColumnData::String(query.into()),
            },
            RpcParam {
                name: Cow::Borrowed("params"),
                flags: RpcStatusFlags::empty(),
                value: ColumnData::I32(0),
            },
        ]
    }

    async fn rpc_perform_query<'a, 'b>(
        &'a mut self,
        proc_id: RpcProcId,
        mut rpc_params: Vec<RpcParam<'b>>,
        params: &'b [&'b dyn prepared::ToSql],
    ) -> crate::Result<()>
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

        Ok(())
    }
}
