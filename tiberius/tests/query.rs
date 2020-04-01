use futures_util::{StreamExt, TryStreamExt};
use once_cell::sync::Lazy;
use std::env;
use std::sync::Once;
use tiberius::{Result, ResultSet};

static LOGGER_SETUP: Once = Once::new();

static CONN_STR: Lazy<String> = Lazy::new(|| {
    env::var("TIBERIUS_TEST_CONNECTION_STRING").unwrap_or(
        "server=tcp:127.0.0.1,1433;integratedSecurity=true;TrustServerCertificate=true".to_owned(),
    )
});

// encrypt=false (default with tls feature enabled)
async fn connect() -> Result<tiberius::Connection> {
    LOGGER_SETUP.call_once(|| {
        env_logger::init();
    });

    tiberius::connect(&*CONN_STR).await
}

// encrypt=true
#[cfg(feature = "tls")]
#[tokio::test]
async fn test_conn_full_encryption() -> Result<()> {
    LOGGER_SETUP.call_once(|| {
        env_logger::init();
    });

    let conn_str = format!("{};encrypt=true", *CONN_STR);
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

    let conn_str = format!("{};encrypt=none", *CONN_STR);
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
async fn test_type_bool() -> Result<()> {
    let conn = connect().await?;
    let mut stream = conn.query("SELECT @P1, @P2", &[&true, &false]).await?;
    let mut rows: Vec<bool> = Vec::with_capacity(2);

    while let Some(row) = stream.next().await {
        let row = row?;
        rows.push(row.get(0));
        rows.push(row.get(1));
    }

    assert_eq!(rows, vec![true, false]);
    Ok(())
}

#[tokio::test]
async fn test_type_i8() -> Result<()> {
    let conn = connect().await?;
    let stream = conn.query("SELECT @P1", &[&-4i8]).await?;

    let rows: Result<Vec<i8>> = stream.map_ok(|x| x.get::<_, i8>(0)).try_collect().await;
    assert_eq!(rows?, vec![-4i8]);
    Ok(())
}

#[tokio::test]
async fn test_type_i16() -> Result<()> {
    let conn = connect().await?;
    let stream = conn.query("SELECT @P1", &[&-4i16]).await?;

    let rows: Result<Vec<i16>> = stream.map_ok(|x| x.get::<_, i16>(0)).try_collect().await;
    assert_eq!(rows?, vec![-4i16]);
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
async fn test_type_i64() -> Result<()> {
    let conn = connect().await?;
    let stream = conn.query("SELECT @P1", &[&-4i64]).await?;

    let rows: Result<Vec<i64>> = stream.map_ok(|x| x.get::<_, i64>(0)).try_collect().await;
    assert_eq!(rows?, vec![-4i64]);
    Ok(())
}

#[tokio::test]
async fn test_type_f32() -> Result<()> {
    let conn = connect().await?;
    let stream = conn.query("SELECT @P1", &[&4.20f32]).await?;

    let rows: Result<Vec<f32>> = stream.map_ok(|x| x.get::<_, f32>(0)).try_collect().await;
    assert_eq!(rows?, vec![4.20f32]);
    Ok(())
}

#[tokio::test]
async fn test_type_f64() -> Result<()> {
    let conn = connect().await?;
    let stream = conn.query("SELECT @P1", &[&4.20f64]).await?;

    let rows: Result<Vec<f64>> = stream.map_ok(|x| x.get::<_, f64>(0)).try_collect().await;
    assert_eq!(rows?, vec![4.20f64]);
    Ok(())
}

#[tokio::test]
async fn test_type_short_string() -> Result<()> {
    let conn = connect().await?;
    let stream = conn.query("SELECT @P1", &[&"Hallo"]).await?;

    let rows: Result<Vec<String>> = stream.map_ok(|x| x.get::<_, String>(0)).try_collect().await;
    assert_eq!(rows?, vec![String::from("Hallo")]);
    Ok(())
}

#[tokio::test]
async fn test_type_long_string() -> Result<()> {
    let conn = connect().await?;
    let string = "a".repeat(4001);
    let s_param: &dyn tiberius::prepared::ToSql = &string.as_str();
    let stream = conn.query("SELECT @P1", &[s_param]).await?;

    let rows: Result<Vec<String>> = stream.map_ok(|x| x.get::<_, String>(0)).try_collect().await;
    assert_eq!(rows?, vec![string]);
    Ok(())
}

#[tokio::test]
async fn test_type_very_long_string() -> Result<()> {
    let conn = connect().await?;
    let string = "a".repeat(1073741830);
    let s = string.as_str();
    let ss = &s;
    let stream = conn.query("SELECT @P1", &[ss]).await?;

    let rows: Result<Vec<String>> = stream.map_ok(|x| x.get::<_, String>(0)).try_collect().await;
    assert_eq!(rows?, vec![string]);
    Ok(())
}

// TODO: Flaky
/*
#[tokio::test]
async fn test_prepared_select_reexecute() -> Result<()> {
    let conn = connect().await?;

    let sql = (0..10).map(|_| "").collect::<Vec<_>>().join(" UNION ALL ");

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
*/

#[tokio::test]
async fn test_stored_procedure() -> Result<()> {
    let conn = connect().await?;
    let stream = conn
        .query(
            "EXECUTE sp_executesql N'SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1'",
            &[],
        )
        .await?;
    let rows: Result<Vec<i32>> = stream.map_ok(|x| x.get::<_, i32>(0)).try_collect().await;
    assert_eq!(rows?, vec![1; 3]);
    Ok(())
}

#[tokio::test]
async fn test_stored_procedure_multiple() -> Result<()> {
    let conn = connect().await?;
    let mut stream = conn.query("EXECUTE sp_executesql N'SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1'; EXECUTE sp_executesql N'SELECT 1 UNION ALL SELECT 1'", &[]).await?;
    let rows: Result<Vec<i32>> = stream
        .by_ref()
        .map_ok(|x| x.get::<_, i32>(0))
        .try_collect()
        .await;
    assert_eq!(rows?, vec![1; 3]);
    assert!(stream.next_resultset());
    let rows: Result<Vec<i32>> = stream.map_ok(|x| x.get::<_, i32>(0)).try_collect().await;
    assert_eq!(rows?, vec![1; 2]);
    Ok(())
}
