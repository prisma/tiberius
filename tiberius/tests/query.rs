use futures_util::{StreamExt, TryStreamExt};
use std::env;
use std::sync::Once;
use tiberius::{AuthMethod, Client, Result};
use uuid::Uuid;

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
    let long_kanji = "余".repeat(80001);

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

    let insert_count = conn
        .execute(
            "INSERT INTO ##TestExecute (id) VALUES (@P1), (@P2), (@P3)",
            &[&1i32, &2i32, &3i32],
        )
        .await?
        .total()
        .await?;

    assert_eq!(3, insert_count);

    let update_count = conn
        .execute(
            "UPDATE ##TestExecute SET id = @P1 WHERE id = @P2",
            &[&2i32, &1i32],
        )
        .await?
        .total()
        .await?;

    assert_eq!(1, update_count);

    let delete_count = conn
        .execute("DELETE ##TestExecute WHERE id <> @P1", &[&3i32])
        .await?
        .total()
        .await?;

    assert_eq!(2, delete_count);

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

    let mut stream = conn
        .query("SELECT 'a' AS first; SELECT 'b' AS second;", &[])
        .await?;

    assert_eq!(vec!["first"], stream.columns());

    let rows: Result<Vec<String>> = stream
        .by_ref()
        .map_ok(|x| x.get::<_, String>(0))
        .try_collect()
        .await;

    assert_eq!(vec!["first"], stream.columns());
    assert_eq!(rows?, vec!["a".to_string()]);

    assert!(stream.next_resultset());
    assert_eq!(vec!["second"], stream.columns());

    let rows: Result<Vec<String>> = stream
        .by_ref()
        .map_ok(|x| x.get::<_, String>(0))
        .try_collect()
        .await;

    assert_eq!(vec!["second"], stream.columns());
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

#[tokio::test]
async fn test_nbc_row() -> Result<()> {
    let mut conn = connect().await?;

    let mut stream = conn
        .query(
            "SELECT NULL, NULL, NULL, NULL, 1, NULL, NULL, NULL, 2, NULL, NULL, 3, NULL, 4",
            &[],
        )
        .await?;

    let expected_results = vec![
        None,
        None,
        None,
        None,
        Some(1),
        None,
        None,
        None,
        Some(2),
        None,
        None,
        Some(3),
        None,
        Some(4),
    ];

    let mut res: Vec<Option<i32>> = Vec::new();
    while let Some(row) = stream.try_next().await? {
        for i in 0..expected_results.len() {
            res.push(row.try_get(i)?)
        }
    }

    assert_eq!(expected_results, res);

    Ok(())
}

#[tokio::test]
async fn test_ntext() -> Result<()> {
    let mut conn = connect().await?;

    let string = r#"Hääpuhetta voi värittää kertomalla vitsejä, aforismeja,
        sananlaskuja, laulunsäkeitä ja muita lainauksia. Huumori sopii hääpuheeseen,
        mutta vitsit eivät saa loukata. Häissä ensimmäisen juhlapuheen pitää
        perinteisesti morsiamen isä."#;

    conn.execute("CREATE TABLE ##TestNText (content NTEXT)", &[])
        .await?;

    conn.execute("INSERT INTO ##TestNText (content) VALUES (@P1)", &[&string])
        .await?;

    let stream = conn.query("SELECT content FROM ##TestNText", &[]).await?;

    let rows: Vec<String> = stream
        .map_ok(|x| x.get::<_, String>(0))
        .try_collect()
        .await?;

    assert_eq!(rows, vec![string]);

    Ok(())
}

#[tokio::test]
async fn test_ntext_empty() -> Result<()> {
    let mut conn = connect().await?;

    conn.execute("CREATE TABLE ##TestNTextEmpty (content NTEXT)", &[])
        .await?;

    conn.execute(
        "INSERT INTO ##TestNTextEmpty (content) VALUES (@P1)",
        &[&Option::<String>::None],
    )
    .await?;

    let mut stream = conn
        .query("SELECT content FROM ##TestNTextEmpty", &[])
        .await?;

    let mut rows: Vec<Option<String>> = Vec::new();

    while let Some(row) = stream.try_next().await? {
        let s: Option<String> = row.try_get(0)?;
        rows.push(s);
    }

    assert_eq!(rows, vec![None]);

    Ok(())
}

#[tokio::test]
async fn test_text() -> Result<()> {
    let mut conn = connect().await?;

    let string = "a".repeat(10000);

    conn.execute("CREATE TABLE ##TestText (content TEXT)", &[])
        .await?;

    conn.execute(
        "INSERT INTO ##TestText (content) VALUES (@P1)",
        &[&string.as_str()],
    )
    .await?;

    let stream = conn.query("SELECT content FROM ##TestText", &[]).await?;

    let rows: Vec<String> = stream
        .map_ok(|x| x.get::<_, String>(0))
        .try_collect()
        .await?;

    assert_eq!(rows, vec![string]);

    Ok(())
}

#[tokio::test]
async fn test_text_empty() -> Result<()> {
    let mut conn = connect().await?;

    conn.execute("CREATE TABLE ##TestTextEmpty (content TEXT)", &[])
        .await?;

    conn.execute(
        "INSERT INTO ##TestTextEmpty (content) VALUES (@P1)",
        &[&Option::<String>::None],
    )
    .await?;

    let mut stream = conn
        .query("SELECT content FROM ##TestTextEmpty", &[])
        .await?;

    let mut rows: Vec<Option<String>> = Vec::new();

    while let Some(row) = stream.try_next().await? {
        let s: Option<String> = row.try_get(0)?;
        rows.push(s);
    }

    assert_eq!(rows, vec![None]);

    Ok(())
}

#[tokio::test]
async fn test_varbinary_empty() -> Result<()> {
    let mut conn = connect().await?;

    conn.execute(
        "CREATE TABLE ##TestVarBinaryEmpty (content VARBINARY(max))",
        &[],
    )
    .await?;

    let total = conn
        .execute(
            "INSERT INTO ##TestVarBinaryEmpty (content) VALUES (@P1)",
            &[&Option::<Vec<u8>>::None],
        )
        .await?
        .total()
        .await?;

    assert_eq!(1, total);

    let mut stream = conn
        .query("SELECT content FROM ##TestVarBinaryEmpty", &[])
        .await?;

    let mut rows: Vec<Option<Vec<u8>>> = Vec::new();

    while let Some(row) = stream.try_next().await? {
        let s: Option<Vec<u8>> = row.try_get(0)?;
        rows.push(s);
    }

    assert_eq!(rows, vec![None]);

    Ok(())
}

#[tokio::test]
async fn test_binary() -> Result<()> {
    let mut conn = connect().await?;

    conn.execute("CREATE TABLE ##TestBinary (content BINARY(8000))", &[])
        .await?;

    let mut binary = vec![0; 7999];
    binary.push(5);

    let inserted = conn
        .execute(
            "INSERT INTO ##TestBinary (content) VALUES (@P1)",
            &[&binary.as_slice()],
        )
        .await?
        .total()
        .await?;

    assert_eq!(1, inserted);

    let mut stream = conn.query("SELECT content FROM ##TestBinary", &[]).await?;

    let mut rows: Vec<Vec<u8>> = Vec::new();
    while let Some(row) = stream.try_next().await? {
        let s: Vec<u8> = row.get::<_, Vec<u8>>(0);
        rows.push(s);
    }

    assert_eq!(8000, rows[0].len());
    assert_eq!(binary, rows[0]);

    Ok(())
}

#[tokio::test]
async fn test_var_binary() -> Result<()> {
    let mut conn = connect().await?;

    conn.execute("CREATE TABLE ##VarBinary (content VARBINARY(8000))", &[])
        .await?;

    let mut binary = vec![0; 79];
    binary.push(5);

    let inserted = conn
        .execute(
            "INSERT INTO ##VarBinary (content) VALUES (@P1)",
            &[&binary.as_slice()],
        )
        .await?
        .total()
        .await?;

    assert_eq!(1, inserted);

    let mut stream = conn.query("SELECT content FROM ##VarBinary", &[]).await?;

    let mut rows: Vec<Vec<u8>> = Vec::new();
    while let Some(row) = stream.try_next().await? {
        let s: Vec<u8> = row.get::<_, Vec<u8>>(0);
        rows.push(s);
    }

    assert_eq!(80, rows[0].len());
    assert_eq!(binary, rows[0]);

    Ok(())
}

#[tokio::test]
async fn test_guid() -> Result<()> {
    let mut conn = connect().await?;

    let id = Uuid::new_v4();
    let stream = conn.query("SELECT @P1", &[&id]).await?;

    let rows: Vec<Uuid> = stream.map_ok(|x| x.get::<_, Uuid>(0)).try_collect().await?;

    assert_eq!(id, rows[0]);

    Ok(())
}

#[tokio::test]
async fn test_max_binary() -> Result<()> {
    let mut conn = connect().await?;

    conn.execute("CREATE TABLE ##MaxBinary (content VARBINARY(max))", &[])
        .await?;

    let mut binary = vec![0; 8000];
    binary.push(5);

    let inserted = conn
        .execute(
            "INSERT INTO ##MaxBinary (content) VALUES (@P1)",
            &[&binary.as_slice()],
        )
        .await?
        .total()
        .await?;

    assert_eq!(1, inserted);

    let mut stream = conn.query("SELECT content FROM ##MaxBinary", &[]).await?;

    let mut rows: Vec<Vec<u8>> = Vec::new();
    while let Some(row) = stream.try_next().await? {
        let s: Vec<u8> = row.get::<_, Vec<u8>>(0);
        rows.push(s);
    }

    assert_eq!(8001, rows[0].len());
    assert_eq!(binary, rows[0]);

    Ok(())
}
