use once_cell::sync::Lazy;
use std::env;
use tiberius::{BulkLoadMetadata, Client, Config, IntoSql, TokenRow, TypeInfo};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

static CONN_STR: Lazy<String> = Lazy::new(|| {
    env::var("TIBERIUS_TEST_CONNECTION_STRING").unwrap_or_else(|_| {
        "server=tcp:localhost,1433;IntegratedSecurity=true;TrustServerCertificate=true".to_owned()
    })
});

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let config = Config::from_ado_string(&CONN_STR)?;

    let tcp = TcpStream::connect(config.get_addr()).await?;
    tcp.set_nodelay(true)?;

    let mut client = Client::connect(config, tcp.compat_write()).await?;

    let mut meta = BulkLoadMetadata::new();
    meta.add_column("val", TypeInfo::int());

    let mut req = client.bulk_insert("bulk_test1", meta).await?;

    for i in [0, 1, 2, 3, 4, 5] {
        let mut row = TokenRow::new();
        row.push(i.into_sql());

        req.send(row).await?;
    }

    let res = req.finalize().await?;

    dbg!(res);

    Ok(())
}
