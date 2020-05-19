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

    conn.execute("INSERT INTO nulltest (id) VALUES (@P1)", &[&1])
        .await?;

    Ok(())
}
