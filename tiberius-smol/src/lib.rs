#[cfg(windows)]
use std::{str, time, convert};
use std::{io, net::{self, ToSocketAddrs}, future::Future, pin::Pin, borrow::Cow, convert};

#[cfg(windows)]
use futures_timer;
#[cfg(windows)]
use futures::{select, future::FutureExt};

use tiberius::ToSql;

/// `Client` is the main entry point to the SQL Server, providing query
/// execution capabilities.
///
/// A `Client` is created using the [`ClientBuilder`], defining the needed
/// connection options and capabilities.
///
/// ```no_run
/// # use tiberius_smol::Client;
/// # use tiberius::AuthMethod;
/// # #[allow(unused)]
/// # async fn foo() -> Result<(), Box<dyn std::error::Error>> {
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
    inner: tiberius::Client<smol::Async<net::TcpStream>>,
}

impl convert::From<tiberius::Client<smol::Async<net::TcpStream>>> for Client {
    fn from(inner: tiberius::Client<smol::Async<net::TcpStream>>) -> Client {
        Client { inner }
    }
}

impl Client {
    pub fn new(inner: tiberius::Client<smol::Async<net::TcpStream>>) -> Client {
        Client { inner }
    }

    /// Executes SQL statements in the SQL Server, returning the number rows
    /// affected. Useful for `INSERT`, `UPDATE` and `DELETE` statements.
    ///
    /// The `query` can define the parameter placement by annotating them with
    /// `@PN`, where N is the index of the parameter, starting from `1`.
    ///
    /// ```no_run
    /// # use tiberius_smol::Client;
    /// # #[allow(unused)]
    /// # async fn foo() -> Result<(), Box<dyn std::error::Error>> {
    /// # let builder = Client::builder();
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
    ) -> tiberius::Result<tiberius::ExecuteResult>
    where
        'a: 'b,
    {
        self.inner.execute(query, params).await
    }

    /// Executes SQL statements in the SQL Server, returning resulting rows.
    /// Useful for `SELECT` statements.
    ///
    /// The `query` can define the parameter placement by annotating them with
    /// `@PN`, where N is the index of the parameter, starting from `1`.
    ///
    /// ```no_run
    /// # use tiberius_smol::Client;
    /// # #[allow(unused)]
    /// # async fn foo() -> Result<(), Box<dyn std::error::Error>> {
    /// # let builder = Client::builder();
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
    ) -> tiberius::Result<tiberius::QueryResult<'a>>
    where
        'a: 'b,
    {
        self.inner.query(query, params).await
    }

    /// Starts an instance of [`ClientBuilder`] for specifying the connect
    /// options.
    ///
    /// [`ClientBuilder`]: struct.ClientBuilder.html
    pub fn builder() -> tiberius::ClientBuilder<smol::Async<net::TcpStream>, Client> {
        tiberius::ClientBuilder::new(Self::new, connector).into()
    }
}

/// A builder for creating a new [`Client`].
///
/// [`Client`]: struct.Client.html
#[derive(Debug)]
pub struct ClientBuilder {
    inner: tiberius::ClientBuilder<smol::Async<net::TcpStream>, Client>,
}

impl convert::From<tiberius::ClientBuilder<smol::Async<net::TcpStream>, Client>> for ClientBuilder {
    fn from(inner: tiberius::ClientBuilder<smol::Async<net::TcpStream>, Client>) -> ClientBuilder {
        ClientBuilder { inner }
    }
}

impl convert::From<ClientBuilder> for tiberius::ClientBuilder<smol::Async<net::TcpStream>, Client> {
    fn from(local_builder: ClientBuilder )-> tiberius::ClientBuilder<smol::Async<net::TcpStream>, Client> {
        local_builder.inner
    }
}

impl ClientBuilder {
    /// Creates a new client and connects to the server.
    pub async fn build(self) -> tiberius::Result<Client> {
        self.inner.build().await
    }

    /// Create a `ClientBuilder` with options specified in the ADO string format
    pub fn from_ado_string(conn_str: &str) -> tiberius::Result<ClientBuilder> {
        let builder = tiberius::ClientBuilder::from_ado_string(Client::new, connector, conn_str)?;
        Ok(builder.into())
    }

    /// A host or ip address to connect to.
    ///
    /// - Defaults to `localhost`.
    pub fn host(&mut self, host: impl ToString) {
        self.inner.host(host)
    }

    /// The server port.
    ///
    /// - Defaults to `1433`.
    pub fn port(&mut self, port: u16) {
        self.inner.port(port)
    }

    /// The database to connect to.
    ///
    /// - Defaults to `master`.
    pub fn database(&mut self, database: impl ToString) {
        self.inner.database(database)
    }

    /// The instance name as defined in the SQL Browser. Only available on
    /// Windows platforms.
    ///
    /// If specified, the port is replaced with the value returned from the
    /// browser.
    #[cfg(any(windows, doc))]
    pub fn instance_name(&mut self, name: impl ToString) {
        self.inner.instance_name(name)
    }

    /// Set the preferred encryption level.
    pub fn encryption(&mut self, encryption: tiberius::EncryptionLevel) {
        self.inner.encryption(encryption)
    }

    /// If set, the server certificate will not be validated and it is accepted
    /// as-is.
    ///
    /// On production setting, the certificate should be added to the local key
    /// storage, using this setting is potentially dangerous.
    pub fn trust_cert(&mut self) {
        self.inner.trust_cert()
    }

    /// Sets the authentication method.
    pub fn authentication(&mut self, auth: tiberius::AuthMethod) {
        self.inner.authentication(auth)
    }
}


fn connector(addr: String, instance_name: Option<String>) -> Pin<Box<dyn Future<Output = tiberius::Result<smol::Async<net::TcpStream>>>>>
{
    let stream = async move {
        let mut addr = addr.to_socket_addrs()?.next().ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, "Could not resolve server host.")
        })?;
        if let Some(ref instance_name) = instance_name {
            addr = find_tcp_port(addr, instance_name).await?;
        };

        let mut stream = smol::Async::<net::TcpStream>::connect(addr).await?;
        stream.get_mut().set_nodelay(true)?;

        Ok(stream)
    };
    Box::pin(stream)
}


#[cfg(not(windows))]
async fn find_tcp_port(addr: std::net::SocketAddr, _: &str) -> tiberius::Result<std::net::SocketAddr> {
    Ok(addr)
}

#[cfg(windows)]
async fn find_tcp_port(addr: std::net::SocketAddr, instance_name: &str) -> tiberius::Result<std::net::SocketAddr> {
    // First resolve the instance to a port via the
    // SSRP protocol/MS-SQLR protocol [1]
    // [1] https://msdn.microsoft.com/en-us/library/cc219703.aspx

    let local_bind: std::net::SocketAddr = if addr.is_ipv4() {
        "0.0.0.0:0".parse().unwrap()
    } else {
        "[::]:0".parse().unwrap()
    };

    let msg = [&[4u8], instance_name.as_bytes()].concat();
    let mut buf = vec![0u8; 4096];

    let socket = smol::Async::<net::UdpSocket>::bind(&local_bind)?;
    socket.send_to(&msg, addr).await?;

    let mut recieve = Box::pin(socket.recv(&mut buf)).fuse();
    let mut timeout = futures_timer::Delay::new(time::Duration::from_millis(1000)).fuse();
    let err = |name| format!("SQL browser timeout during resolving instance {}", name).into();

    let len = async move {
        select! {
            len = recieve => len.map_err(convert::Into::into),
            _ = timeout => Err(tiberius::Error::Conversion(err(instance_name))),
        }
    }.await?;

    tiberius::consume_sql_browser_message(addr, buf, len, instance_name)
}
