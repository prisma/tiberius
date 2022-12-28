use std::env;

use indoc::indoc;
use once_cell::sync::Lazy;
use tiberius::{error::Error, Client, Config};
use tokio::{net::TcpStream, sync::mpsc};
use tokio_util::compat::TokioAsyncWriteCompatExt;

static CONN_STR: Lazy<String> = Lazy::new(|| {
    env::var("TIBERIUS_TEST_CONNECTION_STRING").unwrap_or_else(|_| {
        "server=tcp:localhost,1433;IntegratedSecurity=true;TrustServerCertificate=true".to_owned()
    })
});

/// Two separate actors running queries with their own connections. We
/// keep them as actors so we can trigger queries that will halt, and
/// can still continue using our other connection in the main task.
async fn spawn_actor() -> (
    mpsc::Sender<&'static str>,
    mpsc::Receiver<tiberius::Result<()>>,
) {
    let (query_sender, mut query_receiver) = mpsc::channel(100);
    let (response_sender, response_receiver) = mpsc::channel(100);

    tokio::spawn(async move {
        let config = Config::from_ado_string(&CONN_STR)?;

        let tcp = TcpStream::connect(config.get_addr()).await?;
        tcp.set_nodelay(true)?;

        let mut conn = Client::connect(config, tcp.compat_write()).await?;

        while let Some(query) = query_receiver.recv().await {
            match conn.simple_query(query).await?.into_results().await {
                Ok(_) => response_sender.send(Ok(())).await.unwrap(),
                Err(e) => response_sender.send(Err(e)).await.unwrap(),
            }
        }

        Result::<(), Error>::Ok(())
    });

    (query_sender, response_receiver)
}

#[tokio::test]
async fn deadlocks_should_not_prevent_further_queries() -> anyhow::Result<()> {
    let (sender1, mut receiver1) = spawn_actor().await;
    let (sender2, mut receiver2) = spawn_actor().await;

    let schema = indoc! {r#"
        DROP TABLE IF EXISTS City;
        DROP TABLE IF EXISTS Country;

        CREATE TABLE Country
        (
            Id         INT PRIMARY KEY,
            Name       VARCHAR(100) not null,
            Population INT          not null
        );

        CREATE TABLE City
        (
            Id         INT PRIMARY KEY,
            CountryId  INT          not null,
            Name       VARCHAR(100) not null,
            Population INT          not null,
            CONSTRAINT fk_country FOREIGN KEY (CountryId) REFERENCES Country (Id)
        );
    "#};

    // Create a new schema defined above.
    sender1.send(schema).await?;
    receiver1.recv().await;

    // The first connection starts a transaction.
    sender1.send("BEGIN TRAN").await?;
    receiver1.recv().await;

    // The second connection starts a transaction.
    sender2.send("BEGIN TRAN").await?;
    receiver2.recv().await;

    // The following commands should not still trigger deadlocks.
    sender1
        .send("INSERT INTO Country (Id, Name, Population) VALUES (1, 'USA', 12313)")
        .await?;
    receiver1.recv().await;

    sender2
        .send("INSERT INTO Country (Id, Name, Population) VALUES (2, 'Finland', 42069)")
        .await?;
    receiver2.recv().await;

    sender1.send("SELECT * FROM Country WHERE id = 1").await?;
    receiver1.recv().await;

    sender2.send("SELECT * FROM Country WHERE id = 2").await?;
    receiver2.recv().await;

    sender1
        .send("INSERT INTO City (Id, CountryId, Name, Population) VALUES (1, 1, 'New York', 12313)")
        .await?;
    receiver1.recv().await;

    sender2
        .send("INSERT INTO City (Id, CountryId, Name, Population) VALUES (2, 2, 'Delhi', 12313)")
        .await?;
    receiver2.recv().await;

    // This query will halt forever, therefore we do not yet fetch the
    // results.
    sender1
        .send("SELECT * FROM City WHERE CountryId = 1")
        .await?;

    // This one causes a deadlock, which can be in either of the connections.
    sender2
        .send("SELECT * FROM City WHERE CountryId = 2")
        .await?;

    // We wait here maximum of five seconds until the deadlock
    // detector kicks in and one of the results will be an error, the
    // other transaction can continue.
    let (res1, res2) = (receiver1.recv().await, receiver2.recv().await);

    match (res1, res2) {
        (Some(Err(e)), Some(Ok(_))) if e.is_deadlock() => {
            // Preventing further locks in the other connection, we
            // close the transaction in the connection that did not
            // throw a deadlock error.
            sender2.send("ROLLBACK").await?;
            receiver2.recv().await;

            // The deadlocked connection must be able to query again.
            sender1.send("SELECT * FROM City").await?;
            let res = receiver1.recv().await.unwrap();

            assert!(res.is_ok());
        }
        (Some(Ok(_)), Some(Err(e))) if e.is_deadlock() => {
            // Preventing further locks in the other connection, we
            // close the transaction in the connection that did not
            // throw a deadlock error.
            sender1.send("ROLLBACK").await?;
            receiver1.recv().await;

            // The deadlocked connection must be able to query again.
            sender2.send("SELECT * FROM City").await?;
            let res = receiver2.recv().await.unwrap();

            assert!(res.is_ok());
        }
        (res1, res2) => panic!(
            "Excepted exactly one of the connections to be in a deadlock: {:?} / {:?}",
            res1, res2
        ),
    }

    Ok(())
}
