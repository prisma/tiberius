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
                "INSERT INTO test1 (first_name, last_name) VALUES (@P1, @P2), (@P3, @P4)",
                &[&"foo", &"bar", &"omg", &"lol"],
            )
            .await?;

        let rows: Vec<String> = stream
            .map_ok(|x| x.get::<_, String>(0))
            .try_collect()
            .await?;

        //println!("Result for SELECT of a big string: {:?}", rows);
        println!("{:?}", rows);
    }

    Ok(())
}
