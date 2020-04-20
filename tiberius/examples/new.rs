use futures::TryStreamExt;
use tiberius::{AuthMethod, Client};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let mut builder = Client::builder();
    builder.host("0.0.0.0");
    builder.port(1433);
    builder.database("master");
    builder.authentication(AuthMethod::sql_server("SA", "<YourStrong@Passw0rd>"));

    let mut conn = builder.build().await?;

    let string = "a".repeat(4001);
    let stream = conn.query("SELECT @P1", &[&string.as_str()]).await?;

    let rows: Vec<String> = stream
        .map_ok(|x| x.get::<_, String>(0))
        .try_collect()
        .await?;

    assert_eq!(rows, vec![string]);

    Ok(())
}
