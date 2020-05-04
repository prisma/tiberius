use futures::TryStreamExt;
use tiberius::{AuthMethod, Client, GenericTcpStream};

use tokio_util::compat::{self, Tokio02AsyncWriteCompatExt};
use tokio::{io, net};
use async_trait::async_trait;

pub struct TokioTcpStreamWrapper();

#[async_trait]
impl GenericTcpStream<compat::Compat<net::TcpStream>> for TokioTcpStreamWrapper {
    async fn connect(&self, addr: String, instance_name: &Option<String>) -> tiberius::Result<compat::Compat<net::TcpStream>> 
    {
        let mut addr = tokio::net::lookup_host(addr).await?.next().ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, "Could not resolve server host.")
        })?;

        if let Some(ref instance_name) = instance_name {
            addr = tiberius::find_tcp_port(addr, instance_name).await?;
        };
        Ok(net::TcpStream::connect(addr).await.map(|s| s.compat_write())?)
    }
}



#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let mut builder = Client::<compat::Compat<net::TcpStream>>::builder();
    builder.host("localhost");
    builder.port(1433);
    builder.database("master");
    builder.authentication(AuthMethod::sql_server("SA", "<YourStrong@Passw0rd>"));
    builder.trust_cert();
    let mut conn = builder.build::<TokioTcpStreamWrapper, compat::Compat<net::TcpStream>>(crate::TokioTcpStreamWrapper()).await?;
    let stream = conn.query("SELECT @P1", &[&1]).await?;

    let results = conn
        .simple_query("CREATE TABLE ##NoResults (id INT)")
        .await?
        .into_results()
        .await?;

    assert!(results.is_empty());

    Ok(())
}
