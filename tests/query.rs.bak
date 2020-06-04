use futures::lock::Mutex;
use futures_util::{StreamExt, TryStreamExt};
use names::{Generator, Name};
use once_cell::sync::Lazy;
use std::env;
use std::sync::Once;
use tiberius::{numeric::Numeric, xml::XmlData, Result};
use uuid::Uuid;

// This is used in the testing macro :)
#[allow(dead_code)]
static LOGGER_SETUP: Once = Once::new();

static CONN_STR: Lazy<String> = Lazy::new(|| {
    env::var("TIBERIUS_TEST_CONNECTION_STRING")
        .unwrap_or_else(|_| "server=tcp:localhost,1433;IntegratedSecurity=true;TrustServerCertificate=true".to_owned())
});

static NAMES: Lazy<Mutex<Generator>> =
    Lazy::new(|| Mutex::new(Generator::with_naming(Name::Plain)));

async fn random_table() -> String {
    NAMES.lock().await.next().unwrap().replace('-', "")
}

macro_rules! test_on_runtimes {
    ($code:ident, $connstr:expr) => {
        paste::item! {
            #[test]
            fn [<$code _on_asyncstd>]()-> Result<()> {
                LOGGER_SETUP.call_once(|| {
                    env_logger::init();
                });
                async_std::task::block_on(async {
                    let builder = tiberius_async_std::ClientBuilder::from_ado_string($connstr)?;
                    let conn = builder.build().await?.into();
                    $code(conn).await?;
                    Ok(())
                })
            }
        }
        paste::item! {
            #[test]
            fn [<$code _on_tokio>]()-> Result<()> {
                LOGGER_SETUP.call_once(|| {
                    env_logger::init();
                });
                let mut rt = tokio::runtime::Runtime::new()?;
                rt.block_on(async {
                    let builder = tiberius_tokio::ClientBuilder::from_ado_string($connstr)?;
                    let conn = builder.build().await?.into();
                    $code(conn).await?;
                    Ok(())
                })
            }
        }
    };
    ($code:ident) => {
        paste::item! {
            #[test]
            fn [<$code _on_asyncstd>]()-> Result<()> {
                LOGGER_SETUP.call_once(|| {
                    env_logger::init();
                });
                async_std::task::block_on(async {
                    let builder = tiberius_async_std::ClientBuilder::from_ado_string(&*CONN_STR)?;
                    let conn = builder.build().await?.into();
                    $code(conn).await?;
                    Ok(())
                })
            }
        }
        paste::item! {
            #[test]
            fn [<$code _on_tokio>]()-> Result<()> {
                LOGGER_SETUP.call_once(|| {
                    env_logger::init();
                });
                let mut rt = tokio::runtime::Runtime::new()?;
                rt.block_on(async {
                    let builder = tiberius_tokio::ClientBuilder::from_ado_string(&*CONN_STR)?;
                    let conn = builder.build().await?.into();
                    $code(conn).await?;
                    Ok(())
                })
            }
        }
    };
}

#[cfg(feature = "tls")]
test_on_runtimes! { connect_with_full_encryption, &format!("{};encrypt=true", *CONN_STR)}

#[cfg(feature = "tls")]
async fn connect_with_full_encryption<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let stream = conn.query("SELECT @P1", &[&-4i32]).await?;

    let rows: Result<Vec<i32>> = stream.map_ok(|x| x.get::<_, i32>(0)).try_collect().await;
    assert_eq!(rows?, vec![-4i32]);

    Ok(())
}

#[cfg(windows)]
test_on_runtimes! {
    connect_to_named_instance,
    &{
        let instance_name = env::var("TIBERIUS_TEST_INSTANCE").unwrap_or("MSSQLSERVER".to_owned());
        CONN_STR.replace(",1433", &format!("\\{}", instance_name))
    }
}

#[cfg(windows)]
async fn connect_to_named_instance<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let stream = conn.query("SELECT @P1", &[&-4i32]).await?;

    let rows: Result<Vec<i32>> = stream.map_ok(|x| x.get::<_, i32>(0)).try_collect().await;
    assert_eq!(rows?, vec![-4i32]);

    Ok(())
}

test_on_runtimes! { test_kanji_varchars }
async fn test_kanji_varchars<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let table = random_table().await;

    conn.execute(
        format!("CREATE TABLE ##{} (content NVARCHAR(max))", table),
        &[],
    )
    .await?;

    let kanji = "余ったものを後で皆に分けようと思っていただけなのに".to_string();
    let long_kanji = "余".repeat(80001);

    let res = conn
        .execute(
            format!("INSERT INTO ##{} (content) VALUES (@P1), (@P2)", table),
            &[&kanji, &long_kanji],
        )
        .await?;

    assert_eq!(2, res.total());

    let stream = conn
        .query(format!("SELECT content FROM ##{}", table), &[])
        .await?;

    let results: Vec<String> = stream
        .map_ok(|r| r.get::<_, String>(0))
        .try_collect()
        .await?;

    assert_eq!(vec![kanji, long_kanji], results);

    Ok(())
}

test_on_runtimes! { read_and_write_finnish_varchars }
async fn read_and_write_finnish_varchars<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let table = random_table().await;

    conn.execute(
        format!("CREATE TABLE ##{} (content NVARCHAR(max))", table),
        &[],
    )
    .await?;

    let kalevala = "Vaka vanha Väinämöinen / elelevi aikojansa / noilla Väinölän ahoilla, Kalevalan kankahilla.";

    let res = conn
        .execute(
            format!("INSERT INTO ##{} (content) VALUES (@P1)", table),
            &[&kalevala],
        )
        .await?;

    assert_eq!(1, res.total());

    let stream = conn
        .query(format!("SELECT content FROM ##{}", table), &[])
        .await?;

    let results: Vec<String> = stream
        .map_ok(|r| r.get::<_, String>(0))
        .try_collect()
        .await?;

    assert_eq!(vec![kalevala], results);

    Ok(())
}

test_on_runtimes! { execute_insert_update_delete }
async fn execute_insert_update_delete<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let table = random_table().await;

    conn.execute(format!("CREATE TABLE ##{} (id int)", table), &[])
        .await?;

    let insert_count = conn
        .execute(
            format!("INSERT INTO ##{} (id) VALUES (@P1), (@P2), (@P3)", table),
            &[&1i32, &2i32, &3i32],
        )
        .await?
        .total();

    assert_eq!(3, insert_count);

    let update_count = conn
        .execute(
            format!("UPDATE ##{} SET id = @P1 WHERE id = @P2", table),
            &[&2i32, &1i32],
        )
        .await?
        .total();

    assert_eq!(1, update_count);

    let delete_count = conn
        .execute(format!("DELETE ##{} WHERE id <> @P1", table), &[&3i32])
        .await?
        .total();

    assert_eq!(2, delete_count);

    Ok(())
}

test_on_runtimes! { execute_with_multiple_separate_results }
async fn execute_with_multiple_separate_results<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let table = random_table().await;

    conn.execute(format!("CREATE TABLE ##{} (id int)", table), &[])
        .await?;

    let insert_count = conn
        .execute(
            format!("INSERT INTO ##{table} (id) VALUES (@P1); INSERT INTO ##{table} (id) VALUES (@P2), (@P3);", table = table),
            &[&1i32, &2i32, &3i32],
        )
        .await?;

    let result: Vec<_> = insert_count.into_iter().collect();
    assert_eq!(vec![1, 2], result);

    Ok(())
}

test_on_runtimes! { execute_multiple_count_total }
async fn execute_multiple_count_total<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let table = random_table().await;

    conn.execute(format!("CREATE TABLE ##{} (id int)", table), &[])
        .await?;

    let insert_count = conn
        .execute(
            format!(
                "INSERT INTO ##{table} (id) VALUES (@P1); INSERT INTO ##{table} (id) VALUES (@P2), (@P3);",
                table = table
            ),
            &[&1i32, &2i32, &3i32],
        )
        .await?;

    let result = insert_count.total();
    assert_eq!(3, result);

    Ok(())
}

test_on_runtimes! { correct_row_handling_when_not_enough_data }
async fn correct_row_handling_when_not_enough_data<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
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

test_on_runtimes! { multiple_stored_procedure_functions }
async fn multiple_stored_procedure_functions<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
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

test_on_runtimes! { multiple_queries }
async fn multiple_queries<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
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

test_on_runtimes! { bool_type }
async fn bool_type<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let mut stream = conn
        .query("SELECT @P1, @P2 ORDER BY 1", &[&false, &false])
        .await?;

    let mut rows: Vec<bool> = Vec::with_capacity(2);

    while let Some(row) = stream.next().await {
        let row = row?;
        rows.push(row.get(0));
        rows.push(row.get(1));
    }

    assert_eq!(rows, vec![false, false]);
    Ok(())
}

test_on_runtimes! { i8_token }
async fn i8_token<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let stream = conn.query("SELECT @P1", &[&-4i8]).await?;

    let rows: Result<Vec<i8>> = stream.map_ok(|x| x.get::<_, i8>(0)).try_collect().await;
    assert_eq!(rows?, vec![-4i8]);
    Ok(())
}

test_on_runtimes! { i16_token }
async fn i16_token<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let stream = conn.query("SELECT @P1", &[&-4i16]).await?;

    let rows: Result<Vec<i16>> = stream.map_ok(|x| x.get::<_, i16>(0)).try_collect().await;
    assert_eq!(rows?, vec![-4i16]);
    Ok(())
}

test_on_runtimes! { i32_token }
async fn i32_token<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let stream = conn.query("SELECT @P1", &[&-4i32]).await?;

    let rows: Result<Vec<i32>> = stream.map_ok(|x| x.get::<_, i32>(0)).try_collect().await;
    assert_eq!(rows?, vec![-4i32]);
    Ok(())
}

test_on_runtimes! { i64_token }
async fn i64_token<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let stream = conn.query("SELECT @P1", &[&-4i64]).await?;

    let rows: Result<Vec<i64>> = stream.map_ok(|x| x.get::<_, i64>(0)).try_collect().await;
    assert_eq!(rows?, vec![-4i64]);
    Ok(())
}

test_on_runtimes! { f32_token }
async fn f32_token<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let stream = conn.query("SELECT @P1", &[&4.20f32]).await?;

    let rows: Result<Vec<f32>> = stream.map_ok(|x| x.get::<_, f32>(0)).try_collect().await;
    assert_eq!(rows?, vec![4.20f32]);
    Ok(())
}

test_on_runtimes! { f64_token }
async fn f64_token<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let stream = conn.query("SELECT @P1", &[&4.20f64]).await?;

    let rows: Result<Vec<f64>> = stream.map_ok(|x| x.get::<_, f64>(0)).try_collect().await;
    assert_eq!(rows?, vec![4.20f64]);
    Ok(())
}

test_on_runtimes! { short_strings }
async fn short_strings<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let stream = conn.query("SELECT @P1", &[&"Hallo"]).await?;

    let rows: Result<Vec<String>> = stream.map_ok(|x| x.get::<_, String>(0)).try_collect().await;
    assert_eq!(rows?, vec![String::from("Hallo")]);
    Ok(())
}

test_on_runtimes! { long_strings }
async fn long_strings<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let string = "a".repeat(4001);
    let stream = conn.query("SELECT @P1", &[&string.as_str()]).await?;

    let rows: Result<Vec<String>> = stream.map_ok(|x| x.get::<_, String>(0)).try_collect().await;
    assert_eq!(rows?, vec![string]);
    Ok(())
}

test_on_runtimes! { stored_procedures }
async fn stored_procedures<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
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

test_on_runtimes! { drop_stream_before_handling_all_results_should_not_cause_weird_things }
async fn drop_stream_before_handling_all_results_should_not_cause_weird_things<S>(
    mut conn: tiberius::Client<S>,
) -> Result<()>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
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

test_on_runtimes! { nbc_rows }
async fn nbc_rows<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
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

test_on_runtimes! { ntext_type }
async fn ntext_type<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let table = random_table().await;

    let string = r#"Hääpuhetta voi värittää kertomalla vitsejä, aforismeja,
        sananlaskuja, laulunsäkeitä ja muita lainauksia. Huumori sopii hääpuheeseen,
        mutta vitsit eivät saa loukata. Häissä ensimmäisen juhlapuheen pitää
        perinteisesti morsiamen isä."#;

    conn.execute(format!("CREATE TABLE ##{} (content NTEXT)", table), &[])
        .await?;

    conn.execute(
        format!("INSERT INTO ##{} (content) VALUES (@P1)", table),
        &[&string],
    )
    .await?;

    let stream = conn
        .query(format!("SELECT content FROM ##{}", table), &[])
        .await?;

    let rows: Vec<String> = stream
        .map_ok(|x| x.get::<_, String>(0))
        .try_collect()
        .await?;

    assert_eq!(rows, vec![string]);

    Ok(())
}

test_on_runtimes! { ntext_empty }
async fn ntext_empty<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let table = random_table().await;

    conn.execute(format!("CREATE TABLE ##{} (content NTEXT)", table), &[])
        .await?;

    conn.execute(
        format!("INSERT INTO ##{} (content) VALUES (@P1)", table),
        &[&Option::<String>::None],
    )
    .await?;

    let mut stream = conn
        .query(format!("SELECT content FROM ##{}", table), &[])
        .await?;

    let mut rows: Vec<Option<String>> = Vec::new();

    while let Some(row) = stream.try_next().await? {
        let s: Option<String> = row.try_get(0)?;
        rows.push(s);
    }

    assert_eq!(rows, vec![None]);

    Ok(())
}

test_on_runtimes! { text_type }
async fn text_type<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let table = random_table().await;
    let string = "a".repeat(10000);

    conn.execute(format!("CREATE TABLE ##{} (content TEXT)", table), &[])
        .await?;

    conn.execute(
        format!("INSERT INTO ##{} (content) VALUES (@P1)", table),
        &[&string.as_str()],
    )
    .await?;

    let stream = conn
        .query(format!("SELECT content FROM ##{}", table), &[])
        .await?;

    let rows: Vec<String> = stream
        .map_ok(|x| x.get::<_, String>(0))
        .try_collect()
        .await?;

    assert_eq!(rows, vec![string]);

    Ok(())
}

test_on_runtimes! { text_empty }
async fn text_empty<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let table = random_table().await;

    conn.execute(format!("CREATE TABLE ##{} (content TEXT)", table), &[])
        .await?;

    conn.execute(
        format!("INSERT INTO ##{} (content) VALUES (@P1)", table),
        &[&Option::<String>::None],
    )
    .await?;

    let mut stream = conn
        .query(format!("SELECT content FROM ##{}", table), &[])
        .await?;

    let mut rows: Vec<Option<String>> = Vec::new();

    while let Some(row) = stream.try_next().await? {
        let s: Option<String> = row.try_get(0)?;
        rows.push(s);
    }

    assert_eq!(rows, vec![None]);

    Ok(())
}

test_on_runtimes! { varbinary_empty }
async fn varbinary_empty<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let table = random_table().await;

    conn.execute(
        format!("CREATE TABLE ##{} (content VARBINARY(max))", table),
        &[],
    )
    .await?;

    let total = conn
        .execute(
            format!("INSERT INTO ##{} (content) VALUES (@P1)", table),
            &[&Option::<Vec<u8>>::None],
        )
        .await?
        .total();

    assert_eq!(1, total);

    let mut stream = conn
        .query(format!("SELECT content FROM ##{}", table), &[])
        .await?;

    let mut rows: Vec<Option<Vec<u8>>> = Vec::new();

    while let Some(row) = stream.try_next().await? {
        let s: Option<Vec<u8>> = row.try_get(0)?;
        rows.push(s);
    }

    assert_eq!(rows, vec![None]);

    Ok(())
}

test_on_runtimes! { binary_type }
async fn binary_type<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let table = random_table().await;

    conn.execute(
        format!("CREATE TABLE ##{} (content BINARY(8000))", table),
        &[],
    )
    .await?;

    let mut binary = vec![0; 7999];
    binary.push(5);

    let inserted = conn
        .execute(
            format!("INSERT INTO ##{} (content) VALUES (@P1)", table),
            &[&binary.as_slice()],
        )
        .await?
        .total();

    assert_eq!(1, inserted);

    let mut stream = conn
        .query(format!("SELECT content FROM ##{}", table), &[])
        .await?;

    let mut rows: Vec<Vec<u8>> = Vec::new();
    while let Some(row) = stream.try_next().await? {
        let s: Vec<u8> = row.get::<_, Vec<u8>>(0);
        rows.push(s);
    }

    assert_eq!(8000, rows[0].len());
    assert_eq!(binary, rows[0]);

    Ok(())
}

test_on_runtimes! { varbinary_type }
async fn varbinary_type<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let table = random_table().await;

    conn.execute(
        format!("CREATE TABLE ##{} (content VARBINARY(8000))", table),
        &[],
    )
    .await?;

    let mut binary = vec![0; 79];
    binary.push(5);

    let inserted = conn
        .execute(
            format!("INSERT INTO ##{} (content) VALUES (@P1)", table),
            &[&binary.as_slice()],
        )
        .await?
        .total();

    assert_eq!(1, inserted);

    let mut stream = conn
        .query(format!("SELECT content FROM ##{}", table), &[])
        .await?;

    let mut rows: Vec<Vec<u8>> = Vec::new();
    while let Some(row) = stream.try_next().await? {
        let s: Vec<u8> = row.get::<_, Vec<u8>>(0);
        rows.push(s);
    }

    assert_eq!(80, rows[0].len());
    assert_eq!(binary, rows[0]);

    Ok(())
}

test_on_runtimes! { image_type }
async fn image_type<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let table = random_table().await;

    conn.execute(format!("CREATE TABLE ##{} (content IMAGE)", table), &[])
        .await?;

    let mut binary = vec![0; 79];
    binary.push(5);

    let inserted = conn
        .execute(
            format!("INSERT INTO ##{} (content) VALUES (@P1)", table),
            &[&binary.as_slice()],
        )
        .await?
        .total();

    assert_eq!(1, inserted);

    let mut stream = conn
        .query(format!("SELECT content FROM ##{}", table), &[])
        .await?;

    let mut rows: Vec<Vec<u8>> = Vec::new();
    while let Some(row) = stream.try_next().await? {
        let s: Vec<u8> = row.get::<_, Vec<u8>>(0);
        rows.push(s);
    }

    assert_eq!(80, rows[0].len());
    assert_eq!(binary, rows[0]);

    Ok(())
}

test_on_runtimes! { guid_type }
async fn guid_type<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let id = Uuid::new_v4();
    let stream = conn.query("SELECT @P1", &[&id]).await?;

    let rows: Vec<Uuid> = stream.map_ok(|x| x.get::<_, Uuid>(0)).try_collect().await?;

    assert_eq!(id, rows[0]);

    Ok(())
}

test_on_runtimes! { varbinary_max }
async fn varbinary_max<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let table = random_table().await;

    conn.execute(
        format!("CREATE TABLE ##{} (content VARBINARY(max))", table),
        &[],
    )
    .await?;

    let mut binary = vec![0; 8000];
    binary.push(5);

    let inserted = conn
        .execute(
            format!("INSERT INTO ##{} (content) VALUES (@P1)", table),
            &[&binary.as_slice()],
        )
        .await?
        .total();

    assert_eq!(1, inserted);

    let mut stream = conn
        .query(format!("SELECT content FROM ##{}", table), &[])
        .await?;

    let mut rows: Vec<Vec<u8>> = Vec::new();
    while let Some(row) = stream.try_next().await? {
        let s: Vec<u8> = row.get::<_, Vec<u8>>(0);
        rows.push(s);
    }

    assert_eq!(8001, rows[0].len());
    assert_eq!(binary, rows[0]);

    Ok(())
}

test_on_runtimes! { numeric_type_u32_presentation }
async fn numeric_type_u32_presentation<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let num = Numeric::new_with_scale(2, 1);
    let stream = conn.query("SELECT @P1", &[&num]).await?;

    let rows: Vec<_> = stream
        .map_ok(|x| x.get::<_, Numeric>(0))
        .try_collect()
        .await?;

    assert_eq!(num, rows[0]);

    Ok(())
}

test_on_runtimes! { numeric_type_u64_presentation }
async fn numeric_type_u64_presentation<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let num = Numeric::new_with_scale(std::i32::MAX as i128 + 10, 1);
    let stream = conn.query("SELECT @P1", &[&num]).await?;

    let rows: Vec<_> = stream
        .map_ok(|x| x.get::<_, Numeric>(0))
        .try_collect()
        .await?;

    assert_eq!(num, rows[0]);

    Ok(())
}

test_on_runtimes! { numeric_type_u96_presentation }
async fn numeric_type_u96_presentation<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let num = Numeric::new_with_scale(std::i64::MAX as i128, 19);
    let stream = conn.query("SELECT @P1", &[&num]).await?;

    let rows: Vec<_> = stream
        .map_ok(|x| x.get::<_, Numeric>(0))
        .try_collect()
        .await?;

    assert_eq!(num, rows[0]);

    Ok(())
}

test_on_runtimes! { numeric_type_u128_presentation }
async fn numeric_type_u128_presentation<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let num = Numeric::new_with_scale(std::i64::MAX as i128, 37);
    let stream = conn.query("SELECT @P1", &[&num]).await?;

    let rows: Vec<_> = stream
        .map_ok(|x| x.get::<_, Numeric>(0))
        .try_collect()
        .await?;

    assert_eq!(num, rows[0]);

    Ok(())
}

#[cfg(feature = "rust_decimal")]
test_on_runtimes! { decimal_type_u32_presentation }

#[cfg(feature = "rust_decimal")]
async fn decimal_type_u32_presentation<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    use rust_decimal::Decimal;

    let num = Decimal::from_i128_with_scale(2, 1);
    let stream = conn.query("SELECT @P1", &[&num]).await?;

    let rows: Vec<_> = stream
        .map_ok(|x| x.get::<_, Decimal>(0))
        .try_collect()
        .await?;

    assert_eq!(num, rows[0]);

    Ok(())
}

#[cfg(feature = "rust_decimal")]
test_on_runtimes! { decimal_type_u64_presentation }

#[cfg(feature = "rust_decimal")]
async fn decimal_type_u64_presentation<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    use rust_decimal::Decimal;

    let num = Decimal::from_i128_with_scale(i32::MAX as i128 + 10, 1);
    let stream = conn.query("SELECT @P1", &[&num]).await?;

    let rows: Vec<_> = stream
        .map_ok(|x| x.get::<_, Decimal>(0))
        .try_collect()
        .await?;

    assert_eq!(num, rows[0]);

    Ok(())
}

#[cfg(feature = "rust_decimal")]
test_on_runtimes! { decimal_type_u96_presentation }

#[cfg(feature = "rust_decimal")]
async fn decimal_type_u96_presentation<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    use rust_decimal::Decimal;

    let num = Decimal::from_i128_with_scale(i64::MAX as i128, 19);
    let stream = conn.query("SELECT @P1", &[&num]).await?;

    let rows: Vec<_> = stream
        .map_ok(|x| x.get::<_, Decimal>(0))
        .try_collect()
        .await?;

    assert_eq!(num, rows[0]);

    Ok(())
}

#[cfg(feature = "rust_decimal")]
test_on_runtimes! { decimal_type_u128_presentation }

#[cfg(feature = "rust_decimal")]
async fn decimal_type_u128_presentation<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    use rust_decimal::Decimal;

    let num = Decimal::from_i128_with_scale(i64::MAX as i128, 28);
    let stream = conn.query("SELECT @P1", &[&num]).await?;

    let rows: Vec<_> = stream
        .map_ok(|x| x.get::<_, Decimal>(0))
        .try_collect()
        .await?;

    assert_eq!(num, rows[0]);

    Ok(())
}

#[cfg(all(feature = "chrono", feature = "tds73"))]
test_on_runtimes! { naive_small_date_time_tds73 }

#[cfg(all(feature = "chrono", feature = "tds73"))]
async fn naive_small_date_time_tds73<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    use chrono::{NaiveDate, NaiveDateTime};

    let dt = NaiveDate::from_ymd(2020, 4, 20).and_hms(16, 20, 0);
    let table = random_table().await;

    conn.execute(
        format!("CREATE TABLE ##{} (date smalldatetime)", table),
        &[],
    )
    .await?;

    conn.execute(
        format!("INSERT INTO ##{} (date) VALUES (@P1)", table),
        &[&dt],
    )
    .await?
    .total();

    let stream = conn
        .query(format!("SELECT date FROM ##{}", table), &[])
        .await?;

    let rows: Vec<_> = stream
        .map_ok(|x| x.get::<_, NaiveDateTime>(0))
        .try_collect()
        .await?;

    assert_eq!(dt, rows[0]);
    Ok(())
}

#[cfg(all(feature = "chrono", feature = "tds73"))]
test_on_runtimes! { naive_date_time2_tds73 }

#[cfg(all(feature = "chrono", feature = "tds73"))]
async fn naive_date_time2_tds73<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    use chrono::{NaiveDate, NaiveDateTime};

    let dt = NaiveDate::from_ymd(2020, 4, 20).and_hms(16, 20, 0);
    let table = random_table().await;

    conn.execute(format!("CREATE TABLE ##{} (date datetime2)", table), &[])
        .await?;

    conn.execute(
        format!("INSERT INTO ##{} (date) VALUES (@P1)", table),
        &[&dt],
    )
    .await?
    .total();

    let stream = conn
        .query(format!("SELECT date FROM ##{}", table), &[])
        .await?;

    let rows: Vec<_> = stream
        .map_ok(|x| x.get::<_, NaiveDateTime>(0))
        .try_collect()
        .await?;

    assert_eq!(dt, rows[0]);
    Ok(())
}

#[cfg(all(feature = "chrono", not(feature = "tds73")))]
test_on_runtimes! { naive_date_time_tds72 }

#[cfg(all(feature = "chrono", not(feature = "tds73")))]
async fn naive_date_time_tds72<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    use chrono::{NaiveDate, NaiveDateTime};

    let dt = NaiveDate::from_ymd(2020, 4, 20).and_hms(16, 20, 0);

    let stream = conn.query("SELECT @P1", &[&dt]).await?;

    let rows: Vec<_> = stream
        .map_ok(|x| x.get::<_, NaiveDateTime>(0))
        .try_collect()
        .await?;

    assert_eq!(dt, rows[0]);
    Ok(())
}

#[cfg(all(feature = "chrono", feature = "tds73"))]
test_on_runtimes! { naive_time }

#[cfg(all(feature = "chrono", feature = "tds73"))]
async fn naive_time<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    use chrono::NaiveTime;

    let time = NaiveTime::from_hms(16, 20, 0);

    let stream = conn.query("SELECT @P1", &[&time]).await?;

    let rows: Vec<_> = stream
        .map_ok(|x| x.get::<_, NaiveTime>(0))
        .try_collect()
        .await?;

    assert_eq!(time, rows[0]);
    Ok(())
}

#[cfg(all(feature = "chrono", feature = "tds73"))]
test_on_runtimes! { naive_date }

#[cfg(all(feature = "chrono", feature = "tds73"))]
async fn naive_date<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    use chrono::NaiveDate;

    let date = NaiveDate::from_ymd(2020, 4, 20);

    let stream = conn.query("SELECT @P1", &[&date]).await?;

    let rows: Vec<_> = stream
        .map_ok(|x| x.get::<_, NaiveDate>(0))
        .try_collect()
        .await?;

    assert_eq!(date, rows[0]);
    Ok(())
}

#[cfg(all(feature = "chrono", feature = "tds73"))]
test_on_runtimes! { date_time_utc }

#[cfg(all(feature = "chrono", feature = "tds73"))]
async fn date_time_utc<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    use chrono::{offset::Utc, DateTime, NaiveDate};

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

#[cfg(all(feature = "chrono", feature = "tds73"))]
test_on_runtimes! { date_time_fixed }

#[cfg(all(feature = "chrono", feature = "tds73"))]
async fn date_time_fixed<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    use chrono::{offset::FixedOffset, DateTime, NaiveDate};

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

test_on_runtimes! { xml_read_write }
async fn xml_read_write<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let xml = XmlData::new("<root><child attr=\"attr-value\"/></root>");
    let stream = conn.query("SELECT @P1", &[&xml]).await?;

    let rows: Vec<_> = stream
        .map_ok(|x| x.get::<_, XmlData>(0))
        .try_collect()
        .await?;

    assert_eq!(xml, rows[0]);

    Ok(())
}

test_on_runtimes! { xml_read_null_xml }
async fn xml_read_null_xml<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let mut stream = conn.query("SELECT CAST(NULL AS XML)", &[]).await?;
    let mut rows: Vec<Option<XmlData>> = Vec::new();

    while let Some(row) = stream.try_next().await? {
        let s: Option<XmlData> = row.try_get(0)?;
        rows.push(s);
    }

    assert_eq!(None, rows[0]);

    Ok(())
}
