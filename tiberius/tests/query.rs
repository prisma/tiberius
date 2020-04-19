use futures_util::{StreamExt, TryStreamExt};
use std::env;
use std::sync::Once;
use tiberius::{AuthMethod, Client, Result};

static LOGGER_SETUP: Once = Once::new();

async fn connect() -> Result<Client> {
    LOGGER_SETUP.call_once(|| {
        env_logger::init();
    });

    let mut builder = Client::builder();

    if let Ok(host) = env::var("TIBERIUS_TEST_HOST") {
        builder.host(host);
    };

    if let Ok(port) = env::var("TIBERIUS_TEST_PORT") {
        let port: u16 = port.parse().unwrap();
        builder.port(port);
    };

    if let Ok(user) = env::var("TIBERIUS_TEST_USER") {
        let pw = env::var("TIBERIUS_TEST_PW").unwrap();
        builder.authentication(AuthMethod::sql_server(user, pw));
    };

    builder.build().await
}

/*
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

*/

#[tokio::test]
async fn test_kanji_varchars() -> Result<()> {
    let mut conn = connect().await?;

    conn.execute("CREATE TABLE ##TestKanji (content NVARCHAR(max))", &[])
        .await?;

    let kanji = "余ったものを後で皆に分けようと思っていただけなのに".to_string();
    let long_kanji = "余".repeat(8001);

    let res = conn
        .execute(
            "INSERT INTO ##TestKanji (content) VALUES (@P1), (@P2)",
            &[&kanji, &long_kanji],
        )
        .await?;

    assert_eq!(2, res.total().await?);

    let stream = conn.query("SELECT content FROM ##TestKanji", &[]).await?;

    let results: Vec<String> = stream
        .map_ok(|r| r.get::<_, String>(0))
        .try_collect()
        .await?;

    assert_eq!(vec![kanji, long_kanji], results);

    Ok(())
}

#[tokio::test]
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

    assert_eq!(1, res.total().await?);

    let stream = conn.query("SELECT content FROM ##TestFinnish", &[]).await?;

    let results: Vec<String> = stream
        .map_ok(|r| r.get::<_, String>(0))
        .try_collect()
        .await?;

    assert_eq!(vec![kalevala], results);

    Ok(())
}

#[tokio::test]
async fn test_execute() -> Result<()> {
    let mut conn = connect().await?;

    conn.execute("CREATE TABLE ##TestExecute (id int)", &[])
        .await?;

    let mut insert_count = conn
        .execute(
            "INSERT INTO ##TestExecute (id) VALUES (@P1), (@P2), (@P3)",
            &[&1i32, &2i32, &3i32],
        )
        .await?;

    assert_eq!(Some(3), insert_count.try_next().await?);

    let mut update_count = conn
        .execute(
            "UPDATE ##TestExecute SET id = @P1 WHERE id = @P2",
            &[&2i32, &1i32],
        )
        .await?;

    assert_eq!(Some(1), update_count.try_next().await?);

    let mut delete_count = conn
        .execute("DELETE ##TestExecute WHERE id <> @P1", &[&3i32])
        .await?;

    assert_eq!(Some(2), delete_count.try_next().await?);

    Ok(())
}

#[tokio::test]
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

    let result: Vec<_> = insert_count.try_collect().await?;
    assert_eq!(vec![1, 2], result);

    Ok(())
}

#[tokio::test]
async fn test_execute_multiple_total() -> Result<()> {
    let mut conn = connect().await?;

    conn.execute("CREATE TABLE ##TestExecuteMultiple2 (id int)", &[])
        .await?;

    let insert_count = conn
        .execute(
            "INSERT INTO ##TestExecuteMultiple2 (id) VALUES (@P1); INSERT INTO ##TestExecuteMultiple2 (id) VALUES (@P2), (@P3);",
            &[&1i32, &2i32, &3i32],
        )
        .await?;

    let result = insert_count.total().await?;
    assert_eq!(3, result);

    Ok(())
}

#[tokio::test]
async fn test_correct_row_handling_when_not_enough_data() -> Result<()> {
    let mut conn = connect().await?;

    let mut stream = conn
        .query(
            "SELECT @P1; SELECT @P2;",
            &[&"a".repeat(4001), &"b".repeat(2095)],
        )
        .await?;

    let result: Vec<String> = stream
        .by_ref()
        .map_ok(|x| x.get::<_, String>(0))
        .try_collect()
        .await?;

    assert_eq!(vec!["a".repeat(4001)], result);

    stream.next_resultset();

    let result: Vec<String> = stream
        .map_ok(|x| x.get::<_, String>(0))
        .try_collect()
        .await?;

    assert_eq!(vec!["b".repeat(2095)], result);

    Ok(())
}

#[tokio::test]
async fn test_stored_procedure_multiple_sp() -> Result<()> {
    let mut conn = connect().await?;
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

#[tokio::test]
async fn test_stored_procedure_multiple() -> Result<()> {
    let mut conn = connect().await?;
    let mut stream = conn.query("SELECT 'a'; SELECT 'b';", &[]).await?;

    let rows: Result<Vec<String>> = stream
        .by_ref()
        .map_ok(|x| x.get::<_, String>(0))
        .try_collect()
        .await;

    assert_eq!(rows?, vec!["a".to_string()]);
    assert!(stream.next_resultset());

    let rows: Result<Vec<String>> = stream.map_ok(|x| x.get::<_, String>(0)).try_collect().await;
    assert_eq!(rows?, vec!["b".to_string()]);

    Ok(())
}

#[tokio::test]
async fn test_type_bool() -> Result<()> {
    let mut conn = connect().await?;
    let mut stream = conn.query("SELECT @P1, @P2", &[&false, &false]).await?;
    let mut rows: Vec<bool> = Vec::with_capacity(2);

    while let Some(row) = stream.next().await {
        let row = row?;
        rows.push(row.get(0));
        rows.push(row.get(1));
    }

    assert_eq!(rows, vec![false, false]);
    Ok(())
}

#[tokio::test]
async fn test_type_i8() -> Result<()> {
    let mut conn = connect().await?;
    let stream = conn.query("SELECT @P1", &[&-4i8]).await?;

    let rows: Result<Vec<i8>> = stream.map_ok(|x| x.get::<_, i8>(0)).try_collect().await;
    assert_eq!(rows?, vec![-4i8]);
    Ok(())
}

#[tokio::test]
async fn test_type_i16() -> Result<()> {
    let mut conn = connect().await?;
    let stream = conn.query("SELECT @P1", &[&-4i16]).await?;

    let rows: Result<Vec<i16>> = stream.map_ok(|x| x.get::<_, i16>(0)).try_collect().await;
    assert_eq!(rows?, vec![-4i16]);
    Ok(())
}

#[tokio::test]
async fn test_type_i32() -> Result<()> {
    let mut conn = connect().await?;
    let stream = conn.query("SELECT @P1", &[&-4i32]).await?;

    let rows: Result<Vec<i32>> = stream.map_ok(|x| x.get::<_, i32>(0)).try_collect().await;
    assert_eq!(rows?, vec![-4i32]);
    Ok(())
}

#[tokio::test]
async fn test_type_i64() -> Result<()> {
    let mut conn = connect().await?;
    let stream = conn.query("SELECT @P1", &[&-4i64]).await?;

    let rows: Result<Vec<i64>> = stream.map_ok(|x| x.get::<_, i64>(0)).try_collect().await;
    assert_eq!(rows?, vec![-4i64]);
    Ok(())
}

#[tokio::test]
async fn test_type_f32() -> Result<()> {
    let mut conn = connect().await?;
    let stream = conn.query("SELECT @P1", &[&4.20f32]).await?;

    let rows: Result<Vec<f32>> = stream.map_ok(|x| x.get::<_, f32>(0)).try_collect().await;
    assert_eq!(rows?, vec![4.20f32]);
    Ok(())
}

#[tokio::test]
async fn test_type_f64() -> Result<()> {
    let mut conn = connect().await?;
    let stream = conn.query("SELECT @P1", &[&4.20f64]).await?;

    let rows: Result<Vec<f64>> = stream.map_ok(|x| x.get::<_, f64>(0)).try_collect().await;
    assert_eq!(rows?, vec![4.20f64]);
    Ok(())
}

#[tokio::test]
async fn test_type_short_string() -> Result<()> {
    let mut conn = connect().await?;
    let stream = conn.query("SELECT @P1", &[&"Hallo"]).await?;

    let rows: Result<Vec<String>> = stream.map_ok(|x| x.get::<_, String>(0)).try_collect().await;
    assert_eq!(rows?, vec![String::from("Hallo")]);
    Ok(())
}

#[tokio::test]
async fn test_type_long_string() -> Result<()> {
    let mut conn = connect().await?;
    let string = "a".repeat(4001);
    let stream = conn.query("SELECT @P1", &[&string.as_str()]).await?;

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
    let mut conn = connect().await?;
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
async fn test_drop_stream_before_handling_all_results() -> Result<()> {
    let mut conn = connect().await?;

    {
        let stream = conn
            .query(
                "SELECT @P1; SELECT @P2;",
                &[&"a".repeat(8000), &"b".repeat(8001)],
            )
            .await?;

        let res: Vec<_> = stream
            .map_ok(|x| x.get::<_, String>(0))
            .try_collect()
            .await?;

        assert_eq!("a".repeat(8000), res[0]);
    }

    {
        let stream = conn.query("SELECT @P1", &[&1i32]).await?;
        let res: Vec<_> = stream.map_ok(|x| x.get::<_, i32>(0)).try_collect().await?;
        assert_eq!(1i32, res[0]);
    }

    Ok(())
}
