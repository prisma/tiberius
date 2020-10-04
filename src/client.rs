mod auth;
mod builder;
mod connection;
mod tls;

pub use auth::*;
pub use builder::*;
pub(crate) use connection::*;

use crate::{
    result::{ExecuteResult, QueryResult},
    tds::{
        codec::{self, RpcStatusFlags},
        stream::TokenStream,
    },
    SqlReadBytes, ToSql,
};
use codec::{BatchRequest, ColumnData, PacketHeader, RpcParam, RpcProcId, TokenRpcRequest};
use futures::{AsyncRead, AsyncWrite};
use std::{borrow::Cow, fmt::Debug};

/// `Client` is the main entry point to the SQL Server, providing query
/// execution capabilities.
///
/// A `Client` is created using the [`Config`], defining the needed
/// connection options and capabilities.
///
/// # Example
///
/// ```no_run
/// # use tiberius::{Config, AuthMethod};
/// # use tokio_util::compat::Tokio02AsyncWriteCompatExt;
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let mut config = Config::new();
///
/// config.host("0.0.0.0");
/// config.port(1433);
/// config.authentication(AuthMethod::sql_server("SA", "<Mys3cureP4ssW0rD>"));
///
/// let tcp = tokio::net::TcpStream::connect(config.get_addr()).await?;
/// tcp.set_nodelay(true)?;
/// // Client is ready to use.
/// let client = tiberius::Client::connect(config, tcp.compat_write()).await?;
/// # Ok(())
/// # }
/// ```
///
/// [`Config`]: struct.Config.html
#[derive(Debug)]
pub struct Client<S: AsyncRead + AsyncWrite + Unpin + Send> {
    connection: Connection<S>,
}

pub trait HasData {}

#[derive(Debug)]
pub struct Yes<T: crate::FromSqlOwned + Debug> {
    data: T,
}

impl<T: crate::FromSqlOwned + Debug> HasData for Yes<T> {}

#[derive(Debug)]
pub struct No;
impl HasData for No {}

#[must_use]
#[derive(Debug)]
pub struct Transaction<'client, S, D> 
    where 
        S: AsyncRead + AsyncWrite + Unpin + Send,
        D: HasData,
{
    client: &'client mut Client<S>,
    previous_result: crate::Result<D>,
}

impl<'client, S: AsyncRead + AsyncWrite + Unpin + Send, T: crate::FromSqlOwned + Debug> Transaction<'client, S, Yes<T>> {
    pub async fn exec<'a>(self, query: impl Into<Cow<'a, str>>, params: impl FnOnce(T) -> Vec<Box<dyn ToSql>>) -> Transaction<'client, S, No> {
        let previous_result = if let Ok(Yes { data }) = self.previous_result {
            self.client.execute(query, params(data).iter().map(AsRef::as_ref).collect::<Vec<_>>().as_slice()).await.map(|_| No)
        } else {
            self.previous_result.map(|_| No)
        };
        Transaction {
            client: self.client,
            previous_result,
        }
    }
}

impl<'client, S: AsyncRead + AsyncWrite + Unpin + Send> Transaction<'client, S, No> {
    pub async fn finalize(self) -> crate::Result<()> {
        let _ = match &self.previous_result {
            Ok(_) => self.client.simple_query("COMMIT TRANSACTION").await?,
            Err(_) => self.client.simple_query("ROLLBACK TRANSACTION").await?,
        };
        self.previous_result.map(|_| ())
    }
    pub async fn exec<'a>(mut self, query: impl Into<Cow<'a, str>>, params: &[&dyn ToSql]) -> Transaction<'client, S, No> {
        if let Ok(No) = self.previous_result {
            self.previous_result = self.client.execute(query, params).await.map(|_| No)
        };
        self
    }
    pub async fn loop_exec<'a>(&mut self, query: impl Into<Cow<'a, str>>, params: &[&dyn ToSql]) {
        if let Ok(No) = self.previous_result {
            self.previous_result = self.client.execute(query, params).await.map(|_| No)
        } 
    }
    pub async fn query<T: crate::FromSqlOwned + Debug>(self, query: &str, params: &[&dyn ToSql]) -> Transaction<'client, S, Yes<T>> {
        let previous_result = match self.previous_result {
            Ok(No) =>  match self.client.query(query, params).await {
                Ok(query_result) => {
                    match query_result.into_row().await {
                        Ok(Some(row)) => {
                            if let Some(col_data) = row.into_iter().next() {
                                if let Ok(Some(data)) = crate::FromSqlOwned::from_sql_owned(col_data) {
                                    Ok(Yes { data })
                                } else {
                                   todo!()
                                }
                            } else {
                                todo!()
                            }
                        }
                        Ok(None) => todo!(),
                        Err(e) => Err(e),
                    }
                }
                Err(e) => Err(e),
            }
            Err(e) => Err(e),
        };

        Transaction {
            client: self.client,
            previous_result,
        }
    }
}


impl<S: AsyncRead + AsyncWrite + Unpin + Send> Client<S> {

    /// Gives a handle on a open transaction in which queries can be run.
    /// Once the desired queries have been run, the transaction must be [`finalize`]d
    /// [`finalize`]: struct.Transaction.html#method.finalize
    pub async fn transaction<'client>(&'client mut self) -> crate::Result<Transaction<'client, S, No>> {
        self.simple_query("BEGIN TRANSACTION").await?;
        Ok(Transaction {
            client: self,
            previous_result: Ok(No),
        })
    }

    /// Uses an instance of [`Config`] to specify the connection
    /// options required to connect to the database using an established
    /// tcp connection
    ///
    /// [`Config`]: struct.Config.html
    pub async fn connect(config: Config, tcp_stream: S) -> crate::Result<Client<S>> {
        Ok(Client {
            connection: Connection::connect(config, tcp_stream).await?,
        })
    }


    /// Executes SQL statements in the SQL Server, returning the number rows
    /// affected. Useful for `INSERT`, `UPDATE` and `DELETE` statements. The
    /// `query` can define the parameter placement by annotating them with
    /// `@PN`, where N is the index of the parameter, starting from `1`. If
    /// executing multiple queries at a time, delimit them with `;` and refer to
    /// [`ExecuteResult`] how to get results for the separate queries.
    ///
    /// For mapping of Rust types when writing, see the documentation for
    /// [`ToSql`]. For reading data from the database, see the documentation for
    /// [`FromSql`].
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use tiberius::Config;
    /// # use tokio_util::compat::Tokio02AsyncWriteCompatExt;
    /// # use std::env;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let c_str = env::var("TIBERIUS_TEST_CONNECTION_STRING").unwrap_or(
    /// #     "server=tcp:localhost,1433;integratedSecurity=true;TrustServerCertificate=true".to_owned(),
    /// # );
    /// # let config = Config::from_ado_string(&c_str)?;
    /// # let tcp = tokio::net::TcpStream::connect(config.get_addr()).await?;
    /// # tcp.set_nodelay(true)?;
    /// # let mut client = tiberius::Client::connect(config, tcp.compat_write()).await?;
    /// let results = client
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
    /// [`ToSql`]: trait.ToSql.html
    /// [`FromSql`]: trait.FromSql.html
    pub async fn execute<'a>(
        &mut self,
        query: impl Into<Cow<'a, str>>,
        params: &[&dyn ToSql],
    ) -> crate::Result<ExecuteResult> {
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
    /// For mapping of Rust types when writing, see the documentation for
    /// [`ToSql`]. For reading data from the database, see the documentation for
    /// [`FromSql`].
    ///
    /// # Example
    ///
    /// ```
    /// # use tiberius::Config;
    /// # use tokio_util::compat::Tokio02AsyncWriteCompatExt;
    /// # use std::env;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let c_str = env::var("TIBERIUS_TEST_CONNECTION_STRING").unwrap_or(
    /// #     "server=tcp:localhost,1433;integratedSecurity=true;TrustServerCertificate=true".to_owned(),
    /// # );
    /// # let config = Config::from_ado_string(&c_str)?;
    /// # let tcp = tokio::net::TcpStream::connect(config.get_addr()).await?;
    /// # tcp.set_nodelay(true)?;
    /// # let mut client = tiberius::Client::connect(config, tcp.compat_write()).await?;
    /// let stream = client
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
    /// [`ToSql`]: trait.ToSql.html
    /// [`FromSql`]: trait.FromSql.html
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
    /// # use tiberius::Config;
    /// # use tokio_util::compat::Tokio02AsyncWriteCompatExt;
    /// # use std::env;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let c_str = env::var("TIBERIUS_TEST_CONNECTION_STRING").unwrap_or(
    /// #     "server=tcp:localhost,1433;integratedSecurity=true;TrustServerCertificate=true".to_owned(),
    /// # );
    /// # let config = Config::from_ado_string(&c_str)?;
    /// # let tcp = tokio::net::TcpStream::connect(config.get_addr()).await?;
    /// # tcp.set_nodelay(true)?;
    /// # let mut client = tiberius::Client::connect(config, tcp.compat_write()).await?;
    /// let row = client.simple_query("SELECT 1 AS col").await?.into_row().await?.unwrap();
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

        let req = BatchRequest::new(query, self.connection.context().transaction_id());

        let id = self.connection.context_mut().next_packet_id();
        self.connection.send(PacketHeader::batch(id), req).await?;

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

        let req = TokenRpcRequest::new(
            proc_id,
            rpc_params,
            self.connection.context().transaction_id(),
        );

        let id = self.connection.context_mut().next_packet_id();
        self.connection.send(PacketHeader::rpc(id), req).await?;

        Ok(())
    }
}
