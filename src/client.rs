mod builder;
mod connection;
mod tls;
mod auth;

pub use builder::*;
pub use auth::*;
pub(crate) use connection::*;

use crate::{
    result::{ExecuteResult, QueryResult},
    tds::{
        codec::{self, RpcOptionFlags, RpcStatusFlags},
        stream::TokenStream,
    },
    SqlReadBytes, ToSql,
};
use codec::{
    BatchRequest, ColumnData, PacketHeader, RpcParam, RpcProcId, RpcProcIdValue, TokenRpcRequest,
};
use std::{borrow::Cow, fmt::Debug};

/// `Client` is the main entry point to the SQL Server, providing query
/// execution capabilities.
///
/// A `Client` is created using the [`ClientBuilder`], defining the needed
/// connection options and capabilities.
///
/// # Example
///
/// ```no_run
/// # use tiberius::{Client, AuthMethod};
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let mut builder = Client::builder();
///
/// builder.host("0.0.0.0");
/// builder.port(1433);
/// builder.authentication(AuthMethod::sql_server("SA", "<Mys3cureP4ssW0rD>"));
///
/// // Client is ready to use.
/// let conn = builder.build().await?;
/// # Ok(())
/// # }
/// ```
///
/// [`ClientBuilder`]: struct.ClientBuilder.html
#[derive(Debug)]
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
    /// affected. Useful for `INSERT`, `UPDATE` and `DELETE` statements. The
    /// `query` can define the parameter placement by annotating them with
    /// `@PN`, where N is the index of the parameter, starting from `1`. If
    /// executing multiple queries at a time, delimit them with `;` and refer to
    /// [`ExecuteResult`] how to get results for the separate queries.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use tiberius::ClientBuilder;
    /// # use std::env;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let c_str = env::var("TIBERIUS_TEST_CONNECTION_STRING").unwrap_or(
    /// #     "server=tcp:localhost,1433;integratedSecurity=true;TrustServerCertificate=true".to_owned(),
    /// # );
    /// # let builder = ClientBuilder::from_ado_string(&c_str)?;
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
    /// Useful for `SELECT` statements. The `query` can define the parameter
    /// placement by annotating them with `@PN`, where N is the index of the
    /// parameter, starting from `1`. If executing multiple queries at a time,
    /// delimit them with `;` and refer to [`QueryResult`] on proper stream
    /// handling.
    ///
    /// # Example
    ///
    /// ```
    /// # use tiberius::ClientBuilder;
    /// # use std::env;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let c_str = env::var("TIBERIUS_TEST_CONNECTION_STRING").unwrap_or(
    /// #     "server=tcp:localhost,1433;integratedSecurity=true;TrustServerCertificate=true".to_owned(),
    /// # );
    /// # let builder = ClientBuilder::from_ado_string(&c_str)?;
    /// # let mut conn = builder.build().await?;
    /// let stream = conn
    ///     .query(
    ///         "SELECT @P1, @P2, @P3",
    ///         &[&1i32, &2i32, &3i32],
    ///     )
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
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

    /// Execute multiple queries, delimited with `;` and return multiple result
    /// sets; one for each query.
    ///
    /// # Example
    ///
    /// ```
    /// # use tiberius::ClientBuilder;
    /// # use std::env;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let c_str = env::var("TIBERIUS_TEST_CONNECTION_STRING").unwrap_or(
    /// #     "server=tcp:localhost,1433;integratedSecurity=true;TrustServerCertificate=true".to_owned(),
    /// # );
    /// # let builder = ClientBuilder::from_ado_string(&c_str)?;
    /// # let mut conn = builder.build().await?;
    /// let row = conn.simple_query("SELECT 1 AS col").await?.into_row().await?.unwrap();
    /// assert_eq!(Some(1i32), row.get("col"));
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Warning
    ///
    /// Do not use this with any user specified input. Please resort to prepared
    /// statements using the [`query`] method.
    ///
    /// [`query`]: #method.query
    pub async fn simple_query<'a, 'b>(
        &'a mut self,
        query: impl Into<Cow<'b, str>>,
    ) -> crate::Result<QueryResult<'a>>
    where
        'a: 'b,
    {
        self.connection.flush_stream().await?;

        let req = BatchRequest {
            queries: query.into(),
        };

        self.connection
            .send(PacketHeader::batch(self.connection.context()), req)
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
                value: ColumnData::String(Some(query.into())),
            },
            RpcParam {
                name: Cow::Borrowed("params"),
                flags: RpcStatusFlags::empty(),
                value: ColumnData::I32(Some(0)),
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
            let param_data = param.to_sql();
            param_str.push_str(&param_data.type_name());

            rpc_params.push(RpcParam {
                name: Cow::Owned(format!("@P{}", i + 1)),
                flags: RpcStatusFlags::empty(),
                value: param_data,
            });
        }

        if let Some(params) = rpc_params.iter_mut().find(|x| x.name == "params") {
            params.value = ColumnData::String(Some(param_str.into()));
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
