use futures_util::{StreamExt, TryStreamExt};
use once_cell::sync::Lazy;
use std::env;
use std::sync::Once;
use uuid::Uuid;

use tiberius::{ClientBuilder, Result, Client, GenericTcpStream};

use async_std::{io, net::{self, ToSocketAddrs}};
use async_trait::async_trait;

pub struct AsyncStdTcpStreamWrapper();

#[async_trait]
impl GenericTcpStream<net::TcpStream> for AsyncStdTcpStreamWrapper {
    async fn connect(&self, addr: String, instance_name: &Option<String>) -> tiberius::Result<net::TcpStream> 
    {
        let mut addr = addr.to_socket_addrs().await?.next().ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, "Could not resolve server host.")
        })?;

        if let Some(ref instance_name) = instance_name {
            addr = tiberius::find_tcp_port(addr, instance_name).await?;
        };
        Ok(net::TcpStream::connect(addr).await?)
    }
}

static LOGGER_SETUP: Once = Once::new();

static CONN_STR: Lazy<String> = Lazy::new(|| {
    env::var("TIBERIUS_TEST_CONNECTION_STRING")
        .unwrap_or("server=tcp:localhost,1433;TrustServerCertificate=true".to_owned())
});

async fn connect() -> Result<Client<net::TcpStream>> {
    LOGGER_SETUP.call_once(|| {
        env_logger::init();
    });

    let builder = ClientBuilder::from_ado_string(&*CONN_STR)?;
    builder.build(AsyncStdTcpStreamWrapper()).await
}

#[cfg(feature = "tls")]
#[async_std::test]
async fn test_conn_full_encryption() -> Result<()> {
    LOGGER_SETUP.call_once(|| {
        env_logger::init();
    });

    let conn_str = format!("{};encrypt=true", *CONN_STR);
    let mut conn = ClientBuilder::from_ado_string(&conn_str)?.build(AsyncStdTcpStreamWrapper()).await?;

    let stream = conn.query("SELECT @P1", &[&-4i32]).await?;

    let rows: Result<Vec<i32>> = stream.map_ok(|x| x.get::<_, i32>(0)).try_collect().await;
    assert_eq!(rows?, vec![-4i32]);

    Ok(())
}

#[async_std::test]
async fn test_kanji_varchars() -> Result<()> {
    let mut conn = connect().await?;

    conn.execute("CREATE TABLE ##TestKanji (content NVARCHAR(max))", &[])
        .await?;

    let kanji = "余ったものを後で皆に分けようと思っていただけなのに".to_string();
    let long_kanji = "余".repeat(80001);

    let res = conn
        .execute(
            "INSERT INTO ##TestKanji (content) VALUES (@P1), (@P2)",
            &[&kanji, &long_kanji],
        )
        .await?;

    assert_eq!(2, res.total());

    let stream = conn.query("SELECT content FROM ##TestKanji", &[]).await?;

    let results: Vec<String> = stream
        .map_ok(|r| r.get::<_, String>(0))
        .try_collect()
        .await?;

    assert_eq!(vec![kanji, long_kanji], results);

    Ok(())
}

#[async_std::test]
async fn test_finnish_varchars() -> Result<()> {
    let mut conn = connect().await?;

    conn.execute("CREATE TABLE ##TestFinnish (content NVARCHAR(max))", &[])
        .await?;

    let kalevala = "Vaka vanha Väinämöinen / elelevi aikojansa / noilla Väinölän ahoilla, Kalevalan kankahilla.";

    let res = conn
        .execute(
            "INSERT INTO ##TestFinnish (content) VALUES (@P1)",
            &[&kalevala],
        )
        .await?;

    assert_eq!(1, res.total());

    let stream = conn.query("SELECT content FROM ##TestFinnish", &[]).await?;

    let results: Vec<String> = stream
        .map_ok(|r| r.get::<_, String>(0))
        .try_collect()
        .await?;

    assert_eq!(vec![kalevala], results);

    Ok(())
}

#[async_std::test]
async fn test_execute() -> Result<()> {
    let mut conn = connect().await?;

    conn.execute("CREATE TABLE ##TestExecute (id int)", &[])
        .await?;

    let insert_count = conn
        .execute(
            "INSERT INTO ##TestExecute (id) VALUES (@P1), (@P2), (@P3)",
            &[&1i32, &2i32, &3i32],
        )
        .await?
        .total();

    assert_eq!(3, insert_count);

    let update_count = conn
        .execute(
            "UPDATE ##TestExecute SET id = @P1 WHERE id = @P2",
            &[&2i32, &1i32],
        )
        .await?
        .total();

    assert_eq!(1, update_count);

    let delete_count = conn
        .execute("DELETE ##TestExecute WHERE id <> @P1", &[&3i32])
        .await?
        .total();

    assert_eq!(2, delete_count);

    Ok(())
}

#[async_std::test]
async fn test_execute_multiple_separate_results() -> Result<()> {
    let mut conn = connect().await?;

    conn.execute("CREATE TABLE ##TestExecuteMultiple1 (id int)", &[])
        .await?;

    let insert_count = conn
        .execute(
            "INSERT INTO ##TestExecuteMultiple1 (id) VALUES (@P1); INSERT INTO ##TestExecuteMultiple1 (id) VALUES (@P2), (@P3);",
            &[&1i32, &2i32, &3i32],
        )
        .await?;

    let result: Vec<u64> = insert_count.into_iter().collect();
    assert_eq!(vec![1, 2], result);

    Ok(())
}

