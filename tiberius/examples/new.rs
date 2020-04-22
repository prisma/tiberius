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

    conn.execute("CREATE TABLE ##TestTextEmpty (content TEXT)", &[])
        .await?;

    conn.execute(
        "INSERT INTO ##TestTextEmpty (content) VALUES (@P1)",
        &[&Option::<String>::None],
    )
    .await?;

    let mut stream = conn
        .query("SELECT content FROM ##TestTextEmpty", &[])
        .await?;

    let mut rows: Vec<Option<String>> = Vec::new();

    while let Some(row) = stream.try_next().await? {
        let s: Option<String> = row.try_get(0)?;
        rows.push(s);
    }

    assert_eq!(rows, vec![None]);

    Ok(())
}
