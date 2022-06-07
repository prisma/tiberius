use indicatif::ProgressBar;
use once_cell::sync::Lazy;
use std::env;
use tiberius::{BulkLoadMetadata, Client, ColumnFlag, Config, IntoSql, TokenRow, TypeInfo};
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

    client
        .execute(
            "CREATE TABLE ##bulk_test1 (id INT IDENTITY PRIMARY KEY, content VARCHAR(255))",
            &[],
        )
        .await?;

    let mut meta = BulkLoadMetadata::new();
    meta.add_column("content", TypeInfo::int(), ColumnFlag::Nullable.into());

    let mut req = client.bulk_insert("##bulk_test1", meta).await?;
    let count = 2000i32;

    let pb = ProgressBar::new(count as u64);

    for i in vec!["aaaaaaaaaaaaaaaaaaaa"; 1000].into_iter() {
        let mut row = TokenRow::new();
        row.push(i.into_sql());
        req.send(row).await?;
        pb.inc(1);
    }

    pb.finish_with_message("waiting...");

    let res = req.finalize().await?;

    dbg!(res);

    Ok(())
}
