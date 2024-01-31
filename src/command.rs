use std::borrow::Cow;
pub use tvp_macro;

use enumflags2::BitFlags;
use futures_util::io::{AsyncRead, AsyncWrite};

use crate::{
    tds::{
        codec::{RpcParam, RpcStatus::ByRefValue, RpcValue, TypeInfoTvp},
        stream::{CommandStream, TokenStream},
    },
    Client, ColumnData, IntoSql,
};

#[doc(inline)]
pub use tvp_macro::TableValueRow;

/// Any structure that represents a row in a Table Value parameter must implement this trait
pub trait TableValueRow<'a> {
    /// Bind row field values, called by `Command` instance before making the call to the server
    fn bind_fields(&self, data_row: &mut SqlTableDataRow<'a>); // call data_row.add_field(val) for each field
    /// Database type name that represents this TVP, like `dbo.MyType`.
    fn get_db_type() -> &'static str;
}

/// Implemented as generic for `IntoIterator<Item = TableValueRow>`
pub trait TableValue<'a> {
    fn into_sql(self) -> SqlTableData<'a>;
}

impl<'a, R, C> TableValue<'a> for C
where
    R: TableValueRow<'a> + 'a,
    C: IntoIterator<Item = R>,
{
    fn into_sql(self) -> SqlTableData<'a> {
        let mut data = Vec::new();
        for row in self.into_iter() {
            let mut data_row = SqlTableDataRow::new();
            row.bind_fields(&mut data_row);
            data.push(data_row);
        }

        SqlTableData {
            rows: data,
            db_type: R::get_db_type(),
        }
    }
}

/// Remote command (Stored Procedure of UDF) with bound parameters
#[derive(Debug)]
pub struct Command<'a> {
    name: Cow<'a, str>,
    params: Vec<CommandParam<'a>>, // TODO: might make sense to check if param names are unique, but server would recject repeating params anyway
}

#[derive(Debug)]
struct CommandParam<'a> {
    name: Cow<'a, str>,
    out: bool,
    data: CommandParamData<'a>,
}

#[derive(Debug)]
enum CommandParamData<'a> {
    Scalar(ColumnData<'a>),
    Table(SqlTableData<'a>),
}

#[derive(Debug)]
pub struct SqlTableData<'a> {
    rows: Vec<SqlTableDataRow<'a>>,
    db_type: &'static str,
}

#[derive(Debug)]
/// TVP row binding public API
pub struct SqlTableDataRow<'a> {
    col_data: Vec<ColumnData<'a>>,
}
impl<'a> SqlTableDataRow<'a> {
    fn new() -> SqlTableDataRow<'a> {
        SqlTableDataRow {
            col_data: Vec::new(),
        }
    }
    /// Adds TVP field value to the row. Must be called for each column.
    /// The values are sent to the server in the same order as these calls.
    pub fn add_field(&mut self, data: impl IntoSql<'a> + 'a) {
        self.col_data.push(data.into_sql());
    }
}

impl<'a> Command<'a> {
    /// Constructs a new command object with given name.
    pub fn new(proc_name: impl Into<Cow<'a, str>>) -> Self {
        Self {
            name: proc_name.into(),
            params: Vec::new(),
        }
    }

    /// Binds scalar parameter with the given name to the command.
    pub fn bind_param(&mut self, name: impl Into<Cow<'a, str>>, data: impl IntoSql<'a> + 'a) {
        self.params.push(CommandParam {
            name: name.into(),
            out: false,
            data: CommandParamData::Scalar(data.into_sql()),
        });
    }

    /// Binds by-ref (OUT) scalar parameter to the command.
    /// Returned value can be found by the same name in the returned values collection.
    pub fn bind_out_param(&mut self, name: impl Into<Cow<'a, str>>, data: impl IntoSql<'a> + 'a) {
        self.params.push(CommandParam {
            name: name.into(),
            out: true,
            data: CommandParamData::Scalar(data.into_sql()),
        });
    }

    /// Binds table-valued parameter to the command.
    /// Provided argument must implement `TableValue` trait.
    ///
    /// Example
    ///
    /// ```no_run
    /// # use tiberius::{numeric::Numeric, Client, Command, TableValueRow};
    /// # use tokio_util::compat::TokioAsyncWriteCompatExt;
    /// # use std::env;
    /// #[derive(TableValueRow)]
    /// struct SomeGeoList {
    ///     eid: i32,
    ///     lat: Numeric,
    ///     lon: Numeric,
    /// }
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let c_str = env::var("TIBERIUS_TEST_CONNECTION_STRING").unwrap_or(
    /// #     "server=tcp:localhost,1433;integratedSecurity=true;TrustServerCertificate=true".to_owned(),
    /// # );
    /// # let config = Config::from_ado_string(&c_str)?;
    /// # let tcp = tokio::net::TcpStream::connect(config.get_addr()).await?;
    /// # tcp.set_nodelay(true)?;
    /// # let mut client = tiberius::Client::connect(config, tcp.compat_write()).await?;
    ///
    /// let r1 = SomeGeoList {
    ///     eid: 1,
    ///     lat: Numeric::new_with_scale(10, 6),
    ///     lon: Numeric::new_with_scale(14, 6),
    /// };
    /// let r2 = SomeGeoList {
    ///     eid: 4,
    ///     lat: Numeric::new_with_scale(101, 6),
    ///     lon: Numeric::new_with_scale(142, 6),
    ///     };
    ///
    /// let tbl = vec![r1, r2];
    ///
    /// let mut cmd = Command::new("dbo.usp_TheGeoProcedure");
    ///
    /// cmd.bind_table("@table", tbl);
    /// # Ok(())
    /// # }
    /// ```
    ///
    pub fn bind_table(&mut self, name: impl Into<Cow<'a, str>>, data: impl TableValue<'a> + 'a) {
        self.params.push(CommandParam {
            name: name.into(),
            out: false,
            data: CommandParamData::Table(data.into_sql()),
        });
    }

    /// Executes the `Command` in the SQL Server, returning `CommandStream` that
    /// can be collected into `CommandResult` for convinience.
    ///
    /// Example
    ///
    /// ```no_run
    /// # use tiberius::{numeric::Numeric, Client, Command};
    /// # use tokio_util::compat::TokioAsyncWriteCompatExt;
    /// # use std::env;
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let c_str = env::var("TIBERIUS_TEST_CONNECTION_STRING").unwrap_or(
    /// #     "server=tcp:localhost,1433;integratedSecurity=true;TrustServerCertificate=true".to_owned(),
    /// # );
    /// # let config = Config::from_ado_string(&c_str)?;
    /// # let tcp = tokio::net::TcpStream::connect(config.get_addr()).await?;
    /// # tcp.set_nodelay(true)?;
    /// # let mut client = tiberius::Client::connect(config, tcp.compat_write()).await?;
    /// let mut cmd = Command::new("dbo.usp_SomeStoredProc");
    ///
    /// cmd.bind_param("@foo", 34i32);
    /// cmd.bind_out_param("@bar", "bar");
    /// let res = cmd.exec(&mut client).await?.into_command_result().await?;
    ///
    /// let rv: Option<String> = res.try_return_value("@bar")?;
    /// let rc = res.return_code();
    ///
    /// println!("And we got bar: {:#?}, return_code: {}", rv, rc);
    /// # Ok(())
    /// # }
    /// ```
    ///
    pub async fn exec<'b, S>(self, client: &'b mut Client<S>) -> crate::Result<CommandStream<'b>>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        let rpc_params = Command::build_rpc_params(self.params, client).await?;

        client.connection.flush_stream().await?;
        client.rpc_run_command(self.name, rpc_params).await?;

        let ts = TokenStream::new(&mut client.connection);
        let result = CommandStream::new(ts.try_unfold());

        Ok(result)
    }

    async fn build_rpc_params<'b, S>(
        cmd_params: Vec<CommandParam<'a>>,
        client: &'b mut Client<S>,
    ) -> crate::Result<Vec<RpcParam<'a>>>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        let mut rpc_params = Vec::new();
        for p in cmd_params.into_iter() {
            let rpc_val = match p.data {
                CommandParamData::Scalar(col) => RpcValue::Scalar(col),
                CommandParamData::Table(t) => {
                    let type_info_tvp = TypeInfoTvp::new(
                        t.db_type,
                        t.rows.into_iter().map(|r| r.col_data).collect(),
                    );
                    // it might make sense to expose some API for the caller so they could cache metadata
                    let cols_metadata = client
                        .query_run_for_metadata(format!(
                            "DECLARE @P AS {};SELECT TOP 0 * FROM @P",
                            t.db_type
                        ))
                        .await?;
                    RpcValue::Table(if let Some(cm) = cols_metadata {
                        type_info_tvp.with_metadata(cm)
                    } else {
                        type_info_tvp
                    })
                }
            };
            let rpc_param = RpcParam {
                name: p.name,
                flags: if p.out {
                    BitFlags::from_flag(ByRefValue)
                } else {
                    BitFlags::empty()
                },
                value: rpc_val,
            };
            rpc_params.push(rpc_param);
        }
        Ok(rpc_params)
    }
}
