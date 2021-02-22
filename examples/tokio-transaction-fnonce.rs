use once_cell::sync::Lazy;
use std::env;
use tiberius::{ToSql, params, transaction, Config, Client};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

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

    client.transaction(|conn| Box::pin(async move {

        conn
            .execute(
                "delete from vegetables where name = @P1",
                &[&"asparagus"],
            )
            .await?;
        conn
            .execute(
                "insert into vegetables (name) values (@P1)",
                &[&"cabbage"],
            )
            .await?;

        Ok(())
    })).await?;


    transaction!(|client| async move {

        client
            .execute(
                "delete from vegetables where name = @P1",
                &[&"asparagus"],
            )
            .await?;
        client
            .execute(
                "insert into vegetables (name) values (@P1)",
                params!["cabbage"],
            )
            .await?;

        Ok(())
    });

    let ext = String::new();

    client.transaction_split(|mut conn| async move {
        drop(ext);
        conn
            .execute(
                "delete from vegetables where name = @P1",
                &[&"asparagus"],
            )
            .await?;
        conn
            .execute(
                "insert into vegetables (name) values (@P1)",
                &[&"cabbage"],
            )
            .await?;

        Ok(())
    }).await?;

    Ok(())
}
