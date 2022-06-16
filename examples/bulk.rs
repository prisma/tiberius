use indicatif::ProgressBar;
use once_cell::sync::Lazy;
use std::env;
use tiberius::{
    BulkLoadMetadata, Client, ColumnFlag, Config, IntoSql, TokenRow, TypeInfo, TypeLength,
};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;
use tracing::log::{error, info, LevelFilter};

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

    client
        .execute("DROP TABLE IF EXISTS bulk_test1", &[])
        .await?;
    info!("drop table");
    client
        .execute(
            "CREATE TABLE bulk_test1 (null_int int NULL, nonnull_int int NOT NULL)",
            &[],
        )
        .await?;
    info!("create table done");

    let mut req = client.bulk_insert_1("bulk_test1").await?;

    let count = 1000i32;

    let pb = ProgressBar::new(count as u64);

    info!("start loading data");
    for i in 0..1000 {
        let mut row = TokenRow::new();

        // null_int
        let null_int = [Some(32), None][i % 2];
        row.push(null_int.into_sql());

        // nonnull_int
        let nonnull_int = 44;
        row.push(nonnull_int.into_sql());

        req.send(row).await?;
        pb.inc(1);
    }

    pb.finish_with_message("waiting...");

    let res = req.finalize().await?;

    info!("{:?}", res);

    Ok(())
}
