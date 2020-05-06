mod builder;
mod connection;
mod tls;

pub use builder::*;
pub(crate) use connection::*;

use crate::{
    result::{ExecuteResult, QueryResult},
    tds::{
        codec::{self, RpcOptionFlags, RpcStatusFlags},
        stream::TokenStream,
    },
    SqlReadBytes, ToSql,
};
use codec::{ColumnData, PacketHeader, RpcParam, RpcProcId, RpcProcIdValue, TokenRpcRequest};
use std::borrow::Cow;

#[derive(Clone, Debug, PartialEq)]
pub struct SqlServerAuth {
    pub(crate) user: String,
    pub(crate) password: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WindowsAuth {
    pub(crate) user: String,
    pub(crate) password: String,
    pub(crate) domain: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum AuthMethod {
    /// Authenticate directly with SQL Server. The only authentication method
    /// that works on all platforms.
    SqlServer(SqlServerAuth),
    #[cfg(any(windows, doc))]
    /// Authenticate with Windows credentials. Only available on Windows
    /// platforms.
    Windows(WindowsAuth),
    #[cfg(any(windows, doc))]
    /// Authenticate as the currently logged in user. Only available on Windows
    /// platforms.
    WindowsIntegrated,
    #[doc(hidden)]
    None,
}

impl AuthMethod {
    /// Construct a new SQL Server authentication configuration.
    pub fn sql_server(user: impl ToString, password: impl ToString) -> Self {
        Self::SqlServer(SqlServerAuth {
            user: user.to_string(),
            password: password.to_string(),
        })
    }

    /// Construct a new Windows authentication configuration. Only available on
    /// Windows platforms.
    #[cfg(any(windows, doc))]
    pub fn windows(user: impl AsRef<str>, password: impl ToString) -> Self {
        let (domain, user) = match user.as_ref().find("\\") {
            Some(idx) => (Some(&user.as_ref()[..idx]), &user.as_ref()[idx + 1..]),
            _ => (None, user.as_ref()),
        };

        Self::Windows(WindowsAuth {
            user: user.to_string(),
            password: password.to_string(),
            domain: domain.map(|s| s.to_string()),
        })
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
    /// [`ExecuteResult`]: struct.ExecuteResult.html
    pub async fn execute<'a, 'b>(
        &'a mut self,
        query: impl Into<Cow<'b, str>>,
        params: &'b [&'b dyn ToSql],
    ) -> crate::Result<ExecuteResult>
    where
        'a: 'b,
    {
        self.connection.flush_stream().await?;
        let rpc_params = Self::rpc_params(query);

        self.rpc_perform_query(RpcProcId::SpExecuteSQL, rpc_params, params)
            .await?;

        Ok(ExecuteResult::new(&mut self.connection).await?)
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
    /// [`QueryResult`]: struct.QueryResult.html
    pub async fn query<'a, 'b>(
        &'a mut self,
        query: impl Into<Cow<'b, str>>,
        params: &'b [&'b dyn ToSql],
    ) -> crate::Result<QueryResult<'a>>
    where
        'a: 'b,
    {
        self.connection.flush_stream().await?;
        let rpc_params = Self::rpc_params(query);

        self.rpc_perform_query(RpcProcId::SpExecuteSQL, rpc_params, params)
            .await?;

        let ts = TokenStream::new(&mut self.connection);
        let mut result = QueryResult::new(ts.try_unfold());

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
        params: &'b [&'b dyn ToSql],
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
            .send(PacketHeader::rpc(self.connection.context()), req)
            .await?;

        Ok(())
    }
}
