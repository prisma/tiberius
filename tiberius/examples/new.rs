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

    conn.execute("CREATE TABLE ##TestText (content NTEXT)", &[])
        .await?
        .total()
        .await?;

    let kanji = "a".repeat(4);

    let res = conn
        .execute("INSERT INTO ##TestText (content) VALUES (@P1)", &[&kanji])
        .await?
        .total()
        .await?;

    assert_eq!(1, res);

    let stream = conn.query("SELECT content FROM ##TestText", &[]).await?;

    let results: Vec<String> = stream
        .map_ok(|r| r.get::<_, String>(0))
        .try_collect()
        .await?;

    assert_eq!(vec![kanji], results);

    Ok(())
}
