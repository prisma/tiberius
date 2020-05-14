use futures::TryStreamExt;
use tiberius::{AuthMethod, Client};

use tokio_util::compat::{self, Tokio02AsyncWriteCompatExt};
use tokio::{io, net};



#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let mut builder = Client::<compat::Compat<net::TcpStream>>::builder();
    builder.host("localhost");
    builder.port(1433);
    builder.database("master");
    builder.authentication(AuthMethod::sql_server("SA", "<YourStrong@Passw0rd>"));
    builder.trust_cert();

    let mut conn = builder.build(tiberius_tokio::connector).await?;
    let stream = conn.query("SELECT @P1", &[&1]).await?;

    let results = conn
        .simple_query("CREATE TABLE ##NoResults (id INT)")
        .await?
        .into_results()
        .await?;

    assert!(results.is_empty());

    Ok(())
}
