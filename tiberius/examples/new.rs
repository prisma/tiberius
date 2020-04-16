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

    /*
    {
        let stream = conn.query("SELECT name from test2", &[]).await?;

        let rows: Vec<String> = stream
            .map_ok(|x| x.get::<_, String>(0))
            .try_collect()
            .await?;

        println!("Result for SELECT of a big string: {:?}", rows);
        println!("length: {}", rows[0].len());
    }
    */

    {
        let stream = conn.query("SELECT name FROM test2", &[]).await?;

        let rows: Vec<String> = stream
            .map_ok(|x| x.get::<_, String>(0))
            .try_collect()
            .await?;

        //println!("Result for SELECT of a big string: {:?}", rows);
        println!("length: {}", rows[0].len());
    }

    {
        let stream = conn.query("SELECT @P1", &[&"a".repeat(8001)]).await?;

        let rows: Vec<String> = stream
            .map_ok(|x| x.get::<_, String>(0))
            .try_collect()
            .await?;

        //println!("Result for SELECT of a big string: {:?}", rows);
        println!("length: {}", rows[0].len());
    }

    {
        let stream = conn.query("SELECT first_name from test1", &[]).await?;

        let rows: Vec<String> = stream
            .map_ok(|x| x.get::<_, String>(0))
            .try_collect()
            .await?;

        //println!("Result for SELECT of a big string: {:?}", rows);
        println!("length: {}", rows[0].len());
        println!("length: {}", rows[1].len());
    }

    Ok(())
}
