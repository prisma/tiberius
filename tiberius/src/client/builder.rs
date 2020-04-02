use super::{connection::Connection, AuthMethod};
use crate::{
    protocol::{codec::PacketCodec, Context},
    tls::MaybeTlsStream,
    Client, EncryptionLevel, Error,
};
use ::std::str;
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    net::{TcpStream, UdpSocket},
    time,
};
use tokio_util::codec::Framed;

#[derive(Debug, Clone)]
pub struct ClientBuilder {
    host: Option<String>,
    port: Option<u16>,
    database: Option<String>,
    instance_name: Option<String>,
    ssl: EncryptionLevel,
    trust_cert: bool,
    auth: AuthMethod,
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self {
            host: None,
            port: None,
            database: None,
            instance_name: None,
            ssl: EncryptionLevel::NotSupported,
            trust_cert: false,
            auth: AuthMethod::None,
        }
    }
}

impl ClientBuilder {
    pub fn host(&mut self, host: impl ToString) {
        self.host = Some(host.to_string());
    }

    pub fn port(&mut self, port: u16) {
        self.port = Some(port);
    }

    pub fn database(&mut self, database: impl ToString) {
        self.database = Some(database.to_string())
    }

    pub fn instance_name(&mut self, name: impl ToString) {
        self.instance_name = Some(name.to_string());
    }

    pub fn ssl(&mut self, ssl: EncryptionLevel) {
        self.ssl = ssl;
    }

    pub fn trust_cert(&mut self) {
        self.trust_cert = true;
    }

    pub fn authentication(&mut self, auth: AuthMethod) {
        self.auth = auth;
    }

    pub async fn build(self) -> crate::Result<Client> {
        let context = Arc::new(Context::new());

        let host = self
            .host
            .as_ref()
            .map(|s| s.as_str())
            .unwrap_or("127.0.0.1");

        let port = self.port.unwrap_or(1433);
        let mut addr = format!("{}:{}", host, port).parse().unwrap();

        if let Some(ref instance_name) = self.instance_name {
            addr = find_tcp_sql_browser_addr(addr, instance_name).await?;
        };

        let mut connection = connect_tcp(addr, context.clone()).await?;
        let prelogin = connection.prelogin(self.ssl).await?;
        let ssl = prelogin.negotiated_encryption(self.ssl);

        let mut connection = connection.tls_handshake(ssl, self.trust_cert).await?;
        connection.login(self.auth, self.database).await?;

        Ok(Client {
            connection,
            context,
        })
    }
}

async fn connect_tcp(addr: SocketAddr, context: Arc<Context>) -> crate::Result<Connection> {
    let stream = TcpStream::connect(addr).await?;
    stream.set_nodelay(true)?;

    let transport = Framed::new(MaybeTlsStream::Raw(stream), PacketCodec);

    Ok(Connection::new(transport, context.clone()))
}

async fn find_tcp_sql_browser_addr(
    mut addr: SocketAddr,
    instance_name: &str,
) -> crate::Result<SocketAddr> {
    // First resolve the instance to a port via the
    // SSRP protocol/MS-SQLR protocol [1]
    // [1] https://msdn.microsoft.com/en-us/library/cc219703.aspx

    let local_bind: SocketAddr = if addr.is_ipv4() {
        "0.0.0.0:0".parse().unwrap()
    } else {
        "[::]:0".parse().unwrap()
    };

    let msg = [&[4u8], instance_name.as_bytes()].concat();

    let mut socket = UdpSocket::bind(&local_bind).await?;
    socket.send_to(&msg, &addr).await?;

    let mut buf = vec![0u8; 4096];
    let timeout = Duration::from_millis(1000);

    let len = time::timeout(timeout, socket.recv(&mut buf))
        .await
        .map_err(|_: time::Elapsed| {
            Error::Conversion("SQL browser timeout during resolving instance".into())
        })??;

    buf.truncate(len);

    let err = Error::Conversion("Could not resolve SQL browser instance".into());

    if len == 0 {
        return Err(err);
    }

    let response = str::from_utf8(&buf[3..len])?;

    let port: u16 = response
        .find("tcp;")
        .and_then(|pos| response[pos..].split(';').nth(1))
        .ok_or(err)
        .and_then(|val| Ok(val.parse()?))?;

    addr.set_port(port);

    Ok(addr)
}
