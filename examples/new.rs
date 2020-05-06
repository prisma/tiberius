use futures::TryStreamExt;
use tiberius::{xml::XmlData, AuthMethod, Client};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let mut builder = Client::builder();
    builder.host("localhost");
    builder.port(1433);
    builder.database("master");
    builder.authentication(AuthMethod::sql_server("SA", "<YourStrong@Passw0rd>"));
    builder.trust_cert();

    let mut conn = builder.build().await?;

    let xml = XmlData::new("<root><child attr = \"attr-value\"/></root>");
    let stream = conn.query("SELECT @P1", &[&xml]).await?;

    let rows: Vec<_> = stream
        .map_ok(|x| x.get::<_, XmlData>(0))
        .try_collect()
        .await?;

    dbg!(rows);

    Ok(())
}
