mod builder;
mod connection;

pub use builder::*;
pub use connection::*;

use crate::{
    prepared,
    protocol::{
        codec::{self, RpcOptionFlags, RpcStatusFlags},
        Context,
    },
    result::{ExecuteResult, QueryResult},
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

/// `Client` is the main entry point to the SQL Server, providing query
/// execution capabilities.
///
/// A `Client` is created using the [`ClientBuilder`], defining the needed
/// connection options and capabilities.
///
/// ```no_run
/// # use tiberius::{Client, AuthMethod};
/// # async fn foo() -> Result<(), Box<dyn std::error::Error>> {
/// let mut builder = Client::builder();
///
/// builder.host("0.0.0.0");
/// builder.port(1433);
/// builder.authentication(AuthMethod::sql_server("SA", "<Mys3cureP4ssW0rD>"));
///
/// // Client is ready to use.
/// let mut conn = builder.build().await?;
/// # Ok(())
/// # }
/// ```
///
/// [`ClientBuilder`]: struct.ClientBuilder.html
pub struct Client {
    connection: Connection,
    context: Arc<Context>,
}

impl Client {
    /// Starts an instance of [`ClientBuilder`] for specifying the connect
    /// options.
    ///
    /// [`ClientBuilder`]: struct.ClientBuilder.html
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    /// Executes SQL statements in the SQL Server, returning the number rows
    /// affected. Useful for `INSERT`, `UPDATE` and `DELETE` statements.
    ///
    /// The `query` can define the parameter placement by annotating them with
    /// `@PN`, where N is the index of the parameter, starting from `1`.
    ///
    /// ```no_run
    /// # use tiberius::Client;
    /// # async fn foo() -> Result<(), Box<dyn std::error::Error>> {
    /// # let mut builder = Client::builder();
    /// # let mut conn = builder.build().await?;
    /// let results = conn
    ///     .execute(
    ///         "INSERT INTO ##Test (id) VALUES (@P1), (@P2), (@P3)",
    ///         &[&1i32, &2i32, &3i32],
    ///     )
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// See the documentation for the resulting [`ExecuteResult`] on how to
    /// handle the results correctly.
    ///
    /// [`ExecuteResult`]: ../struct.ExecuteResult.html
    pub async fn execute<'b, 'a: 'b>(
        &'a mut self,
        query: impl Into<Cow<'_, str>>,
        params: &'b [&'b dyn prepared::ToSql],
    ) -> crate::Result<ExecuteResult<'a>> {
        self.connection.flush_stream().await?;
        let rpc_params = Self::rpc_params(query);

        self.rpc_perform_query(RpcProcId::SpExecuteSQL, rpc_params, params)
            .await?;

        Ok(ExecuteResult::new(
            &mut self.connection,
            self.context.clone(),
        ))
    }

    /// Executes SQL statements in the SQL Server, returning resulting rows.
    /// Useful for `SELECT` statements.
    ///
    /// The `query` can define the parameter placement by annotating them with
    /// `@PN`, where N is the index of the parameter, starting from `1`.
    ///
    /// ```no_run
    /// # use tiberius::Client;
    /// # async fn foo() -> Result<(), Box<dyn std::error::Error>> {
    /// # let mut builder = Client::builder();
    /// # let mut conn = builder.build().await?;
    /// let rows = conn
    ///     .query(
    ///         "SELECT @P1, @P2, @P3",
    ///         &[&1i32, &2i32, &3i32],
    ///     )
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// See the documentation for the resulting [`QueryResult`] on how to
    /// handle the results correctly.
    ///
    /// [`QueryResult`]: ../struct.QueryResult.html
    pub async fn query<'a, 'b>(
        &'a mut self,
        query: impl Into<Cow<'a, str>>,
        params: &'b [&'b dyn prepared::ToSql],
    ) -> crate::Result<QueryResult<'a>>
    where
        'a: 'b,
    {
        self.connection.flush_stream().await?;
        let rpc_params = Self::rpc_params(query);

        self.rpc_perform_query(RpcProcId::SpExecuteSQL, rpc_params, params)
            .await?;

        let mut result = QueryResult::new(self.connection.token_stream());
        result.fetch_metadata().await?;

        Ok(result)
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
