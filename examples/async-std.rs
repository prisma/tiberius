use once_cell::sync::Lazy;
use std::env;
use futures::TryStreamExt;

static CONN_STR: Lazy<String> = Lazy::new(|| {
    env::var("TIBERIUS_TEST_CONNECTION_STRING")
        .unwrap_or_else(|_| "server=tcp:localhost,1433;IntegratedSecurity=true;TrustServerCertificate=true".to_owned())
});


#[cfg(not(all(windows, feature = "named-instance-async")))]
#[async_std::main]
async fn main() -> anyhow::Result<()> {
    let config = tiberius::ClientBuilder::from_ado_string(&CONN_STR)?;
    let tcp = async_std::net::TcpStream::connect(config.get_addr()).await?;
    tcp.set_nodelay(true)?;
    let mut client = tiberius::Client::connect(config, tcp).await?;

    let row = client.query("SELECT @P1", &[&1]).await?.into_row().await?.unwrap();
    assert_eq!(Some(1), row.get(0));
    Ok(())
}

#[cfg(all(windows, feature = "named-instance-async"))]
#[async_std::main]
async fn main() -> anyhow::Result<()> {
    let config = tiberius::ClientBuilder::from_ado_string(&CONN_STR)?;
    let tcp = config.connect().await?;
    tcp.set_nodelay(true)?;
    let mut client = tiberius::Client::connect(config, tcp).await?;

    let stream = client.query("SELECT @P1", &[&1]).await?;
    let rows: Vec<_> = stream.map_ok(|x| x.get::<_, i32>(0)).try_collect().await?;
    println!("{:?}", rows);
    assert_eq!(1, rows[0]);
    Ok(())
}
