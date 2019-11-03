use tiberius::{Error, Result};
use futures_util::{StreamExt, TryStreamExt};

use std::sync::Once;
static LOGGER_SETUP: Once = Once::new();

async fn connect() -> Result<tiberius::Connection> {
    LOGGER_SETUP.call_once(|| {
        env_logger::init();
    });
    
    tiberius::connect_tcp(
        tiberius::ConnectParams {
            ssl: tiberius::EncryptionLevel::Required,
            host: "".to_owned(),
            trust_cert: true,
        },
        "127.0.0.1:1433".parse().unwrap(),
    ).await
}

#[tokio::test]
async fn test_type_i32() -> Result<()> {
    let conn = connect().await?;
    let mut stream = conn.query("SELECT @P1", &[&-4i32]).await?;
    
    let rows: Result<Vec<i32>> = stream.map_ok(|x| x.get::<_, i32>(0)).try_collect().await;
    assert_eq!(rows?, vec![-4i32]);
    Ok(())
}

#[tokio::test]
async fn test_prepared_select_reexecute() -> Result<()> {
    let conn = connect().await?;
    let sql = (0..10)
        .map(|_| "SELECT 1")
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    
    println!("a{}b", sql);
    let stmt = conn.prepare(&sql).await?;
    for _ in 0..3 {
        let stream = conn.query(&stmt, &[]).await?;
        let rows: Result<Vec<i32>> = stream.map_ok(|x| x.get::<_, i32>(0)).try_collect().await;
        assert_eq!(rows?, vec![1; 10]);
    }
    Ok(())
}

#[tokio::test]
async fn test_unprepare() -> Result<()> {
    let conn = connect().await?;
    // This forces the statement to be dropped, which leads to a reprepare
    for _ in 0..20 {
        let stmt = conn.prepare("SELECT 42").await?;
        for _ in 0..3 {
            let stream = conn.query(&stmt, &[]).await?;
            let rows: Result<Vec<i32>> = stream.map_ok(|x| x.get::<_, i32>(0)).try_collect().await;
            assert_eq!(rows?, vec![42]);
        }
    }
    Ok(())
}
