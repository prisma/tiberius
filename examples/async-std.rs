use async_std::net::TcpStream;
use once_cell::sync::Lazy;
use std::env;
use tiberius::{Client, Config};

static CONN_STR: Lazy<String> = Lazy::new(|| {
    env::var("TIBERIUS_TEST_CONNECTION_STRING").unwrap_or_else(|_| {
        "server=tcp:localhost,1433;IntegratedSecurity=true;TrustServerCertificate=true;encrypt=true".to_owned()
    })
});

#[cfg(not(all(windows, feature = "sql-browser-async-std")))]
#[async_std::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::from_ado_string(&*CONN_STR)?;

    let tcp = TcpStream::connect(config.get_addr()).await?;
    tcp.set_nodelay(true)?;

    let mut client = Client::connect(config, tcp).await?;

    let stream = client.query("SELECT @P1", &[&1i32]).await?;
    let row = stream.into_row().await?.unwrap();

    println!("{:?}", row);
    assert_eq!(Some(1), row.get(0));

    Ok(())
}

#[cfg(all(windows, feature = "sql-browser-async-std"))]
#[async_std::main]
async fn main() -> anyhow::Result<()> {
    use tiberius::SqlBrowser;

    let config = Config::from_ado_string(&CONN_STR)?;

    let tcp = TcpStream::connect_named(&config).await?;
    tcp.set_nodelay(true)?;

    let mut client = Client::connect(config, tcp).await?;

    let stream = client.query("SELECT @P1", &[&1i32]).await?;
    let row = stream.into_row().await?.unwrap();

    println!("{:?}", row);
    assert_eq!(Some(1), row.get(0));

    Ok(())
}
