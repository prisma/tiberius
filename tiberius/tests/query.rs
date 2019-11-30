use futures_util::{StreamExt, TryStreamExt};
use std::env;
use tiberius::{Error, Result};

use std::sync::Once;
static LOGGER_SETUP: Once = Once::new();

// encrypt=false (default with tls feature enabled)
async fn connect() -> Result<tiberius::Connection> {
    LOGGER_SETUP.call_once(|| {
        env_logger::init();
    });

    let conn_str = env::var("TIBERIUS_TEST_CONNECTION_STRING").unwrap_or(
        "server=tcp:127.0.0.1,1433;integratedSecurity=true;TrustServerCertificate=true".to_owned(),
    );
    tiberius::connect(&conn_str).await
}

// encrypt=true
#[tokio::test]
async fn test_conn_full_encryption() -> Result<()> {
    LOGGER_SETUP.call_once(|| {
        env_logger::init();
    });

    let conn_str = env::var("TIBERIUS_TEST_CONNECTION_STRING").unwrap_or(
        "server=tcp:127.0.0.1,1433;integratedSecurity=true;TrustServerCertificate=true".to_owned(),
    ) + ";encrypt=true";
    let conn = tiberius::connect(&conn_str).await?;
    let stream = conn.query("SELECT @P1", &[&-4i32]).await?;

    let rows: Result<Vec<i32>> = stream.map_ok(|x| x.get::<_, i32>(0)).try_collect().await;
    assert_eq!(rows?, vec![-4i32]);
    Ok(())
}

// encrypt=none
#[tokio::test]
async fn test_conn_unencrypted() -> Result<()> {
    LOGGER_SETUP.call_once(|| {
        env_logger::init();
    });

    let conn_str = env::var("TIBERIUS_TEST_CONNECTION_STRING").unwrap_or(
        "server=tcp:127.0.0.1,1433;integratedSecurity=true;TrustServerCertificate=true".to_owned(),
    ) + ";encrypt=none";
    let conn = tiberius::connect(&conn_str).await?;
    let stream = conn.query("SELECT @P1", &[&-4i32]).await?;

    let rows: Result<Vec<i32>> = stream.map_ok(|x| x.get::<_, i32>(0)).try_collect().await;
    assert_eq!(rows?, vec![-4i32]);
    Ok(())
}

#[tokio::test]
async fn test_simple_query() -> Result<()> {
    let conn = connect().await?;

    let mut sum: i64 = 0;
    let mut stream = conn.simple_query("SELECT TOP (1000) n = ROW_NUMBER() OVER (ORDER BY [object_id]) FROM sys.all_objects ORDER BY n;").await?;
    while let Some(row) = stream.next().await {
        sum += row?.get::<_, i64>(0);
    }
    assert_eq!(sum, (1000 * 1001) / 2);
    Ok(())
}

#[tokio::test]
async fn test_simple_query_cursored() -> Result<()> {
    let conn = connect().await?;

    let mut sum: i64 = 0;
    let mut stream = conn.cursored().simple_query("SELECT TOP (1000) n = ROW_NUMBER() OVER (ORDER BY [object_id]) FROM sys.all_objects ORDER BY n;").await?;
    while let Some(row) = stream.next().await {
        sum += row?.get::<_, i64>(0);
    }
    assert_eq!(sum, (1000 * 1001) / 2);
    Ok(())
}

#[tokio::test]
async fn test_type_i32() -> Result<()> {
    let conn = connect().await?;
    let stream = conn.query("SELECT @P1", &[&-4i32]).await?;

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
