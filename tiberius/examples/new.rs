use futures::TryStreamExt;
use tiberius::{client::AuthMethod, Client};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let mut builder = Client::builder();
    builder.host("0.0.0.0");
    builder.port(1433);
    builder.database("master");
    builder.authentication(AuthMethod::sql_server("SA", "<YourStrong@Passw0rd>"));

    let mut conn = builder.build().await?;

    {
        let stream = conn
            .query(
                "SELECT @P1; SELECT @P2;",
                &[&"a".repeat(8000), &"b".repeat(80001)],
            )
            .await?;

        let res: Vec<_> = stream
            .map_ok(|x| x.get::<_, String>(0))
            .try_collect()
            .await?;

        assert_eq!("a".repeat(8000), res[0]);
    }

    {
        let stream = conn.query("SELECT @P1", &[&1i32]).await?;
        let res: Vec<_> = stream.map_ok(|x| x.get::<_, i32>(0)).try_collect().await?;
        assert_eq!(1i32, res[0]);
    }

    Ok(())
}
