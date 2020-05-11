use futures_util::{StreamExt, TryStreamExt};
use once_cell::sync::Lazy;
use std::env;
use std::sync::Once;
use tiberius::{numeric::Numeric, xml::XmlData, Client, ClientBuilder, Result};
use uuid::Uuid;

static LOGGER_SETUP: Once = Once::new();

static CONN_STR: Lazy<String> = Lazy::new(|| {
    env::var("TIBERIUS_TEST_CONNECTION_STRING").unwrap_or(
        "server=tcp:localhost,1433;integratedSecurity=true;TrustServerCertificate=true".to_owned(),
    )
});

async fn connect() -> Result<Client> {
    LOGGER_SETUP.call_once(|| {
        env_logger::init();
    });

    let builder = ClientBuilder::from_ado_string(&*CONN_STR)?;
    builder.build().await
}

#[cfg(feature = "tls")]
#[tokio::test]
async fn connect_with_full_encryption() -> Result<()> {
    LOGGER_SETUP.call_once(|| {
        env_logger::init();
    });

    let conn_str = format!("{};encrypt=true", *CONN_STR);
    let mut conn = ClientBuilder::from_ado_string(&conn_str)?.build().await?;

    let stream = conn.query("SELECT @P1", &[&-4i32]).await?;

    let rows: Result<Vec<i32>> = stream.map_ok(|x| x.get::<_, i32>(0)).try_collect().await;
    assert_eq!(rows?, vec![-4i32]);

    Ok(())
}

#[cfg(windows)]
#[tokio::test]
async fn connect_to_named_instance() -> Result<()> {
    let instance_name = env::var("TIBERIUS_TEST_INSTANCE").unwrap_or("MSSQLSERVER".to_owned());

    let conn_str = CONN_STR.replace(",1433", &format!("\\{}", instance_name));
    let mut conn = ClientBuilder::from_ado_string(&conn_str)?.build().await?;

    let stream = conn.query("SELECT @P1", &[&-4i32]).await?;

    let rows: Result<Vec<i32>> = stream.map_ok(|x| x.get::<_, i32>(0)).try_collect().await;
    assert_eq!(rows?, vec![-4i32]);

    Ok(())
}

#[tokio::test]
async fn read_and_write_kanji_varchars() -> Result<()> {
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

#[tokio::test]
async fn read_and_write_finnish_varchars() -> Result<()> {
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

#[tokio::test]
async fn execute_insert_update_delete() -> Result<()> {
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

#[tokio::test]
async fn execute_with_multiple_separate_results() -> Result<()> {
    let mut conn = connect().await?;

    conn.execute("CREATE TABLE ##TestExecuteMultiple1 (id int)", &[])
        .await?;

    let insert_count = conn
        .execute(
            "INSERT INTO ##TestExecuteMultiple1 (id) VALUES (@P1); INSERT INTO ##TestExecuteMultiple1 (id) VALUES (@P2), (@P3);",
            &[&1i32, &2i32, &3i32],
        )
        .await?;

    let result: Vec<_> = insert_count.into_iter().collect();
    assert_eq!(vec![1, 2], result);

    Ok(())
}

#[tokio::test]
async fn execute_multiple_count_total() -> Result<()> {
    let mut conn = connect().await?;

    conn.execute("CREATE TABLE ##TestExecuteMultiple2 (id int)", &[])
        .await?;

    let insert_count = conn
        .execute(
            "INSERT INTO ##TestExecuteMultiple2 (id) VALUES (@P1); INSERT INTO ##TestExecuteMultiple2 (id) VALUES (@P2), (@P3);",
            &[&1i32, &2i32, &3i32],
        )
        .await?;

    let result = insert_count.total();
    assert_eq!(3, result);

    Ok(())
}

#[tokio::test]
async fn correct_row_handling_when_not_enough_data() -> Result<()> {
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
async fn multiple_stored_procedure_functions() -> Result<()> {
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
async fn multiple_queries() -> Result<()> {
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
async fn bool_type() -> Result<()> {
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
async fn i8_token() -> Result<()> {
    let mut conn = connect().await?;
    let stream = conn.query("SELECT @P1", &[&-4i8]).await?;

    let rows: Result<Vec<i8>> = stream.map_ok(|x| x.get::<_, i8>(0)).try_collect().await;
    assert_eq!(rows?, vec![-4i8]);
    Ok(())
}

#[tokio::test]
async fn i16_token() -> Result<()> {
    let mut conn = connect().await?;
    let stream = conn.query("SELECT @P1", &[&-4i16]).await?;

    let rows: Result<Vec<i16>> = stream.map_ok(|x| x.get::<_, i16>(0)).try_collect().await;
    assert_eq!(rows?, vec![-4i16]);
    Ok(())
}

#[tokio::test]
async fn i32_token() -> Result<()> {
    let mut conn = connect().await?;
    let stream = conn.query("SELECT @P1", &[&-4i32]).await?;

    let rows: Result<Vec<i32>> = stream.map_ok(|x| x.get::<_, i32>(0)).try_collect().await;
    assert_eq!(rows?, vec![-4i32]);
    Ok(())
}

#[tokio::test]
async fn i64_token() -> Result<()> {
    let mut conn = connect().await?;
    let stream = conn.query("SELECT @P1", &[&-4i64]).await?;

    let rows: Result<Vec<i64>> = stream.map_ok(|x| x.get::<_, i64>(0)).try_collect().await;
    assert_eq!(rows?, vec![-4i64]);
    Ok(())
}

#[tokio::test]
async fn f32_token() -> Result<()> {
    let mut conn = connect().await?;
    let stream = conn.query("SELECT @P1", &[&4.20f32]).await?;

    let rows: Result<Vec<f32>> = stream.map_ok(|x| x.get::<_, f32>(0)).try_collect().await;
    assert_eq!(rows?, vec![4.20f32]);
    Ok(())
}

#[tokio::test]
async fn f64_token() -> Result<()> {
    let mut conn = connect().await?;
    let stream = conn.query("SELECT @P1", &[&4.20f64]).await?;

    let rows: Result<Vec<f64>> = stream.map_ok(|x| x.get::<_, f64>(0)).try_collect().await;
    assert_eq!(rows?, vec![4.20f64]);
    Ok(())
}

#[tokio::test]
async fn short_strings() -> Result<()> {
    let mut conn = connect().await?;
    let stream = conn.query("SELECT @P1", &[&"Hallo"]).await?;

    let rows: Result<Vec<String>> = stream.map_ok(|x| x.get::<_, String>(0)).try_collect().await;
    assert_eq!(rows?, vec![String::from("Hallo")]);
    Ok(())
}

#[tokio::test]
async fn long_strings() -> Result<()> {
    let mut conn = connect().await?;
    let string = "a".repeat(4001);
    let stream = conn.query("SELECT @P1", &[&string.as_str()]).await?;

    let rows: Result<Vec<String>> = stream.map_ok(|x| x.get::<_, String>(0)).try_collect().await;
    assert_eq!(rows?, vec![string]);
    Ok(())
}

#[tokio::test]
async fn stored_procedures() -> Result<()> {
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
async fn drop_stream_before_handling_all_results_should_not_cause_weird_things() -> Result<()> {
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

        assert_eq!(vec!["a".repeat(8000)], res);
    }

    {
        let stream = conn.query("SELECT @P1", &[&1i32]).await?;
        let res: Vec<_> = stream.map_ok(|x| x.get::<_, i32>(0)).try_collect().await?;
        assert_eq!(1i32, res[0]);
    }

    Ok(())
}

#[tokio::test]
async fn nbc_rows() -> Result<()> {
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
async fn ntext_type() -> Result<()> {
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
async fn ntext_empty() -> Result<()> {
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
async fn text_type() -> Result<()> {
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
async fn text_empty() -> Result<()> {
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
async fn varbinary_empty() -> Result<()> {
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
        .total();

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
async fn binary_type() -> Result<()> {
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
        .total();

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
async fn varbinary_type() -> Result<()> {
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
        .total();

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
async fn image_type() -> Result<()> {
    let mut conn = connect().await?;

    conn.execute("CREATE TABLE ##ImageType (content IMAGE)", &[])
        .await?;

    let mut binary = vec![0; 79];
    binary.push(5);

    let inserted = conn
        .execute(
            "INSERT INTO ##ImageType (content) VALUES (@P1)",
            &[&binary.as_slice()],
        )
        .await?
        .total();

    assert_eq!(1, inserted);

    let mut stream = conn.query("SELECT content FROM ##ImageType", &[]).await?;

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
async fn guid_type() -> Result<()> {
    let mut conn = connect().await?;

    let id = Uuid::new_v4();
    let stream = conn.query("SELECT @P1", &[&id]).await?;

    let rows: Vec<Uuid> = stream.map_ok(|x| x.get::<_, Uuid>(0)).try_collect().await?;

    assert_eq!(id, rows[0]);

    Ok(())
}

#[tokio::test]
async fn varbinary_max() -> Result<()> {
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
        .total();

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

#[tokio::test]
async fn numeric_type_u32_presentation() -> Result<()> {
    let mut conn = connect().await?;
    let num = Numeric::new_with_scale(2, 1);
    let stream = conn.query("SELECT @P1", &[&num]).await?;

    let rows: Vec<_> = stream
        .map_ok(|x| x.get::<_, Numeric>(0))
        .try_collect()
        .await?;

    assert_eq!(num, rows[0]);

    Ok(())
}

#[tokio::test]
async fn numeric_type_u64_presentation() -> Result<()> {
    let mut conn = connect().await?;
    let num = Numeric::new_with_scale(i32::MAX as i128 + 10, 1);
    let stream = conn.query("SELECT @P1", &[&num]).await?;

    let rows: Vec<_> = stream
        .map_ok(|x| x.get::<_, Numeric>(0))
        .try_collect()
        .await?;

    assert_eq!(num, rows[0]);

    Ok(())
}

#[tokio::test]
async fn numeric_type_u96_presentation() -> Result<()> {
    let mut conn = connect().await?;
    let num = Numeric::new_with_scale(i64::MAX as i128, 19);
    let stream = conn.query("SELECT @P1", &[&num]).await?;

    let rows: Vec<_> = stream
        .map_ok(|x| x.get::<_, Numeric>(0))
        .try_collect()
        .await?;

    assert_eq!(num, rows[0]);

    Ok(())
}

#[tokio::test]
async fn numeric_type_u128_presentation() -> Result<()> {
    let mut conn = connect().await?;
    let num = Numeric::new_with_scale(i64::MAX as i128, 37);
    let stream = conn.query("SELECT @P1", &[&num]).await?;

    let rows: Vec<_> = stream
        .map_ok(|x| x.get::<_, Numeric>(0))
        .try_collect()
        .await?;

    assert_eq!(num, rows[0]);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "rust_decimal")]
async fn decimal_type_u32_presentation() -> Result<()> {
    use rust_decimal::Decimal;

    let mut conn = connect().await?;
    let num = Decimal::from_i128_with_scale(2, 1);
    let stream = conn.query("SELECT @P1", &[&num]).await?;

    let rows: Vec<_> = stream
        .map_ok(|x| x.get::<_, Decimal>(0))
        .try_collect()
        .await?;

    assert_eq!(num, rows[0]);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "rust_decimal")]
async fn decimal_type_u64_presentation() -> Result<()> {
    use rust_decimal::Decimal;

    let mut conn = connect().await?;
    let num = Decimal::from_i128_with_scale(i32::MAX as i128 + 10, 1);
    let stream = conn.query("SELECT @P1", &[&num]).await?;

    let rows: Vec<_> = stream
        .map_ok(|x| x.get::<_, Decimal>(0))
        .try_collect()
        .await?;

    assert_eq!(num, rows[0]);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "rust_decimal")]
async fn decimal_type_u96_presentation() -> Result<()> {
    use rust_decimal::Decimal;

    let mut conn = connect().await?;
    let num = Decimal::from_i128_with_scale(i64::MAX as i128, 19);
    let stream = conn.query("SELECT @P1", &[&num]).await?;

    let rows: Vec<_> = stream
        .map_ok(|x| x.get::<_, Decimal>(0))
        .try_collect()
        .await?;

    assert_eq!(num, rows[0]);

    Ok(())
}

#[tokio::test]
#[cfg(feature = "rust_decimal")]
async fn decimal_type_u128_presentation() -> Result<()> {
    use rust_decimal::Decimal;

    let mut conn = connect().await?;
    let num = Decimal::from_i128_with_scale(i64::MAX as i128, 28);
    let stream = conn.query("SELECT @P1", &[&num]).await?;

    let rows: Vec<_> = stream
        .map_ok(|x| x.get::<_, Decimal>(0))
        .try_collect()
        .await?;

    assert_eq!(num, rows[0]);

    Ok(())
}

#[tokio::test]
#[cfg(all(feature = "chrono", feature = "tds73"))]
async fn naive_small_date_time_tds73() -> Result<()> {
    use chrono::{NaiveDate, NaiveDateTime};

    let mut conn = connect().await?;
    let dt = NaiveDate::from_ymd(2020, 4, 20).and_hms(16, 20, 0);

    conn.execute("CREATE TABLE ##TestSmallDt (date smalldatetime)", &[])
        .await?;

    conn.execute("INSERT INTO ##TestSmallDt (date) VALUES (@P1)", &[&dt])
        .await?
        .total();

    let stream = conn.query("SELECT date FROM ##TestSmallDt", &[]).await?;

    let rows: Vec<_> = stream
        .map_ok(|x| x.get::<_, NaiveDateTime>(0))
        .try_collect()
        .await?;

    assert_eq!(dt, rows[0]);
    Ok(())
}

#[tokio::test]
#[cfg(all(feature = "chrono", feature = "tds73"))]
async fn naive_date_time2_tds73() -> Result<()> {
    use chrono::{NaiveDate, NaiveDateTime};

    let mut conn = connect().await?;
    let dt = NaiveDate::from_ymd(2020, 4, 20).and_hms(16, 20, 0);

    conn.execute("CREATE TABLE ##NaiveDateTime2 (date datetime2)", &[])
        .await?;

    conn.execute("INSERT INTO ##NaiveDateTime2 (date) VALUES (@P1)", &[&dt])
        .await?
        .total();

    let stream = conn.query("SELECT date FROM ##NaiveDateTime2", &[]).await?;

    let rows: Vec<_> = stream
        .map_ok(|x| x.get::<_, NaiveDateTime>(0))
        .try_collect()
        .await?;

    assert_eq!(dt, rows[0]);
    Ok(())
}

#[tokio::test]
#[cfg(all(feature = "chrono", not(feature = "tds73")))]
async fn naive_date_time_tds72() -> Result<()> {
    use chrono::{NaiveDate, NaiveDateTime};

    let mut conn = connect().await?;
    let dt = NaiveDate::from_ymd(2020, 4, 20).and_hms(16, 20, 0);

    let stream = conn.query("SELECT @P1", &[&dt]).await?;

    let rows: Vec<_> = stream
        .map_ok(|x| x.get::<_, NaiveDateTime>(0))
        .try_collect()
        .await?;

    assert_eq!(dt, rows[0]);
    Ok(())
}

#[tokio::test]
#[cfg(all(feature = "chrono", feature = "tds73"))]
async fn naive_time() -> Result<()> {
    use chrono::NaiveTime;

    let mut conn = connect().await?;
    let time = NaiveTime::from_hms(16, 20, 0);

    let stream = conn.query("SELECT @P1", &[&time]).await?;

    let rows: Vec<_> = stream
        .map_ok(|x| x.get::<_, NaiveTime>(0))
        .try_collect()
        .await?;

    assert_eq!(time, rows[0]);
    Ok(())
}

#[tokio::test]
#[cfg(all(feature = "chrono", feature = "tds73"))]
async fn naive_date() -> Result<()> {
    use chrono::NaiveDate;

    let mut conn = connect().await?;
    let date = NaiveDate::from_ymd(2020, 4, 20);

    let stream = conn.query("SELECT @P1", &[&date]).await?;

    let rows: Vec<_> = stream
        .map_ok(|x| x.get::<_, NaiveDate>(0))
        .try_collect()
        .await?;

    assert_eq!(date, rows[0]);
    Ok(())
}

#[tokio::test]
#[cfg(all(feature = "chrono", feature = "tds73"))]
async fn date_time_utc() -> Result<()> {
    use chrono::{offset::Utc, DateTime, NaiveDate};

    let mut conn = connect().await?;
    let naive = NaiveDate::from_ymd(2020, 4, 20).and_hms(16, 20, 0);
    let dt: DateTime<Utc> = DateTime::from_utc(naive, Utc);

    let stream = conn.query("SELECT @P1", &[&dt]).await?;

    let rows: Vec<_> = stream
        .map_ok(|x| x.get::<_, DateTime<Utc>>(0))
        .try_collect()
        .await?;

    assert_eq!(dt, rows[0]);
    Ok(())
}

#[tokio::test]
#[cfg(all(feature = "chrono", feature = "tds73"))]
async fn date_time_fixed() -> Result<()> {
    use chrono::{offset::FixedOffset, DateTime, NaiveDate};

    let mut conn = connect().await?;
    let naive = NaiveDate::from_ymd(2020, 4, 20).and_hms(16, 20, 0);
    let fixed = FixedOffset::east(3600 * 3);
    let dt: DateTime<FixedOffset> = DateTime::from_utc(naive, fixed);

    let stream = conn.query("SELECT @P1", &[&dt]).await?;

    let rows: Vec<_> = stream
        .map_ok(|x| x.get::<_, DateTime<FixedOffset>>(0))
        .try_collect()
        .await?;

    assert_eq!(dt, rows[0]);
    Ok(())
}

#[tokio::test]
async fn xml_read_write() -> Result<()> {
    let mut conn = connect().await?;

    let xml = XmlData::new("<root><child attr=\"attr-value\"/></root>");
    let stream = conn.query("SELECT @P1", &[&xml]).await?;

    let rows: Vec<_> = stream
        .map_ok(|x| x.get::<_, XmlData>(0))
        .try_collect()
        .await?;

    assert_eq!(xml, rows[0]);

    Ok(())
}

#[tokio::test]
async fn xml_read_null_xml() -> Result<()> {
    let mut conn = connect().await?;
    let mut stream = conn.query("SELECT CAST(NULL AS XML)", &[]).await?;
    let mut rows: Vec<Option<XmlData>> = Vec::new();

    while let Some(row) = stream.try_next().await? {
        let s: Option<XmlData> = row.try_get(0)?;
        rows.push(s);
    }

    assert_eq!(None, rows[0]);

    Ok(())
}
