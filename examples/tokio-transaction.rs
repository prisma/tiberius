use once_cell::sync::Lazy;
use std::env;
use tiberius::{Client, Config};
use tokio::net::TcpStream;
use tokio_util::compat::Tokio02AsyncWriteCompatExt;

static CONN_STR: Lazy<String> = Lazy::new(|| {
    env::var("TIBERIUS_TEST_CONNECTION_STRING").unwrap_or_else(|_| {
        "server=tcp:localhost,1433;IntegratedSecurity=true;TrustServerCertificate=true".to_owned()
    })
});

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::from_ado_string(&CONN_STR)?;

    let tcp = TcpStream::connect(config.get_addr()).await?;
    tcp.set_nodelay(true)?;

    let mut client = Client::connect(config, tcp.compat_write()).await?;

    let _ = client
        .simple_query("create table ##Test ( id int )")
        .await?;

    let client = client
        .transaction(|f| { tokio::spawn(f); })
        .await?
        .exec(
            "INSERT INTO ##Test (id) VALUES (@P1), (@P2), (@P3)",
            &[&1i32, &2i32, &3i32],
        )
        .await
        .finalize()
        .await?;

    let mut t1 = client
        .transaction(|f| { tokio::spawn(f); })
            .await?;
    for _ in (1..).take(3) {
        t1.loop_exec(
            "INSERT INTO ##Test (id) VALUES (@P1), (@P2), (@P3)",
            &[&1i32, &2i32, &3i32],
        )
        .await;
    }
    let mut client = t1.finalize().await?;


    let stream = client.query("SELECT * from ##Test", &[]).await?;
    let rows = stream.into_first_result().await?;

    println!("{:?}", rows);

    let data = rows.into_iter().try_fold(Vec::new(), |mut acc, x| {
        acc.push(x.try_get(0)?.unwrap());
        Ok::<Vec<i32>, tiberius::error::Error>(acc)
    })?;

    println!("{:?}", data);

    assert_eq!(vec![1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3], data);


    let timed = tokio::time::timeout(
        std::time::Duration::from_millis(1),
        client
        .transaction(|f| { tokio::spawn(f); })
        .await?
        .exec("select * from sys.all_objects a cross join sys.all_objects b cross join sys.all_objects c cross join sys.all_objects d", &[])
    ).await;



    Ok(())
}
