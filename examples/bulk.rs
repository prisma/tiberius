use indicatif::ProgressBar;
use once_cell::sync::Lazy;
use std::env;
use tiberius::{Client, Config, IntoRow};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;
use tracing::log::info;

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
            r#"CREATE TABLE bulk_test1 (
                        id INT IDENTITY PRIMARY KEY,
                        int int NULL, 
                        float real NULL,
                        string varchar(40) NULL)"#,
            &[],
        )
        .await?;
    info!("create table done");

    let mut req = client.bulk_insert("bulk_test1").await?;

    let count = 1000i32;

    let pb = ProgressBar::new(count as u64);

    info!("start loading data");
    for i in 0..1000 {
        let int_column = [Some(32), None][i % 2];
        let float_column = [Some(34f32), None][i % 2];
        let string_column = [Some("aaa"), None][i % 2];

        let row = (int_column, float_column, string_column).into_row();

        req.send(row).await?;
        pb.inc(1);
    }

    pb.finish_with_message("waiting...");

    let res = req.finalize().await?;

    info!("{:?}", res);

    Ok(())
}
