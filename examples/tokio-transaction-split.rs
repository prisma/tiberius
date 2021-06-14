use once_cell::sync::Lazy;
use std::env;
use tiberius::{client_split::Client, Config};
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

    let (mut client, conn) = Client::connect(config, tcp.compat_write()).await?;

    tokio::spawn(async {
        conn.run().await
    });

    client.simple_query("drop table vegetables").await?;

    client.simple_query(r#"
        create table vegetables (
            id int identity(1,1),
            name varchar(50),
        )
    "#).await?;

    let mut transaction = client.begin_transaction().await?;

    transaction.execute("insert into vegetables (name) values (@P1)",  vec![Box::new("carrot".to_string())]).await?;

    transaction.commit().await?;


    let row = client
        .query(
            "select id from vegetables where name = @P1",
            vec![Box::new("carrot".to_string())],
        )
        .await?
        .into_row()
        .await?
        .unwrap();

    assert_eq!(row.get(0), Some(1));

    {
        let mut transaction = client.begin_transaction().await?;

        transaction.execute("insert into vegetables (name) values (@P1)", vec![Box::new("asparagus".to_string())]).await?;
    
    }

    let row = client
        .query(
            "select id from vegetables where name = @P1",
            vec![Box::new("asparagus".to_string())],
        )
        .await?
        .into_row()
        .await?;

    dbg!(&row);

    assert!(if let None = row { true } else { false });

    {
        let mut transaction = client.begin_transaction().await?;

        transaction.execute("insert into vegetables (name) values (@P1)", vec![Box::new("asparagus".to_string())]).await?;

        transaction.commit().await?;
    
    }


    let row = client
        .query(
            "select id from vegetables where name = @P1",
            vec![Box::new("asparagus".to_string())],
        )
        .await?
        .into_row()
        .await?
        .unwrap();

    dbg!(&row);
    // ID should be 3 instead of 2 as we will have incremented the
    // sequence during the transaction which was eventually rolled back
    assert_eq!(row.get(0), Some(3));

    Ok(())
}
