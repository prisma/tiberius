use tiberius::{AuthMethod, Client};

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

    let results = conn
        .simple_query("CREATE TABLE ##NoResults (id INT)")
        .await?
        .into_results()
        .await?;

    assert!(results.is_empty());

    Ok(())
}
