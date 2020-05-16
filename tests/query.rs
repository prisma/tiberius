use futures_util::{TryStreamExt, StreamExt};
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
        .unwrap_or_else(|_| "server=tcp:localhost,1433;TrustServerCertificate=true".to_owned())
});



macro_rules! test_on_runtimes {
    ($code:ident, $connstr:expr) => {
        paste::item! {
            #[test]
            fn [<$code _on_asyncstd>]()-> Result<()> {
                async_std::task::block_on(async {
                    let builder = tiberius_asyncstd::ClientBuilder::from_ado_string($connstr)?;
                    let conn = builder.build().await?.into();
                    $code(conn).await?;
                    Ok(())
                })
            }
        }
        paste::item! {
            #[test]
            fn [<$code _on_smol>]()-> Result<()> {
                smol::run( async {
                    let builder = tiberius_smol::ClientBuilder::from_ado_string($connstr)?;
                    let conn = builder.build().await?.into();
                    $code(conn).await?;
                    Ok(())
                })
            }
        }
        paste::item! {
            #[test]
            fn [<$code _on_tokio>]()-> Result<()> {
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
                async_std::task::block_on(async {
                    let builder = tiberius_asyncstd::ClientBuilder::from_ado_string(&*CONN_STR)?;
                    let conn = builder.build().await?.into();
                    $code(conn).await?;
                    Ok(())
                })
            }
        }
        paste::item! {
            #[test]
            fn [<$code _on_smol>]()-> Result<()> {
                smol::run( async {
                    let builder = tiberius_smol::ClientBuilder::from_ado_string(&*CONN_STR)?;
                    let conn = builder.build().await?.into();
                    $code(conn).await?;
                    Ok(())
                })
            }
        }
        paste::item! {
            #[test]
            fn [<$code _on_tokio>]()-> Result<()> {
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
    {
        let instance_name = env::var("TIBERIUS_TEST_INSTANCE").unwrap_or("MSSQLSERVER".to_owned());
        CONN_STR.replace(",1433", &format!("\\{}", instance_name));
    },
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

    let rows = conn
        .query("SELECT content FROM ##TestKanji", &[])
        .await?
        .into_first_result()
        .await?;

    assert_eq!(Some(kanji.as_str()), rows[0].get(0));
    assert_eq!(Some(long_kanji.as_str()), rows[1].get(0));

    Ok(())
}

test_on_runtimes! { read_and_write_finnish_varchars }
async fn read_and_write_finnish_varchars<S>(mut conn: tiberius::Client<S>) -> Result<()> 
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
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

    let row = conn
        .query("SELECT content FROM ##TestFinnish", &[])
        .await?
        .into_row()
        .await?
        .unwrap();

    assert_eq!(Some(kalevala), row.get(0));

    Ok(())
}

#[tokio::test]
async fn read_and_write_char() -> Result<()> {
    let mut conn = connect().await?;

    conn.simple_query("CREATE TABLE ##ReadAndWriteChar (a char)")
        .await?
        .into_results()
        .await?;

    conn.execute("INSERT INTO ##ReadAndWriteChar (a) values (@P1)", &[&"a"])
        .await?;

    let row = conn
        .query("SELECT a FROM ##ReadAndWriteChar", &[])
        .await?
        .into_row()
        .await?
        .unwrap();

    assert_eq!(Some("a"), row.get(0));

    Ok(())
}

#[tokio::test]
async fn read_and_write_nchar() -> Result<()> {
    let mut conn = connect().await?;

    conn.simple_query("CREATE TABLE ##ReadAndWriteNChar (a nchar)")
        .await?
        .into_results()
        .await?;

    conn.execute("INSERT INTO ##ReadAndWriteNChar (a) values (@P1)", &[&"ä"])
        .await?;

    let row = conn
        .query("SELECT a FROM ##ReadAndWriteNChar", &[])
        .await?
        .into_row()
        .await?
        .unwrap();

    assert_eq!(Some("ä"), row.get(0));

    Ok(())
}

test_on_runtimes! { execute_insert_update_delete }
async fn execute_insert_update_delete<S>(mut conn: tiberius::Client<S>) -> Result<()> 
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{

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

test_on_runtimes! { execute_with_multiple_separate_results }
async fn execute_with_multiple_separate_results<S>(mut conn: tiberius::Client<S>) -> Result<()> 
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{

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

test_on_runtimes! { execute_multiple_count_total }
async fn execute_multiple_count_total<S>(mut conn: tiberius::Client<S>) -> Result<()> 
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{

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

    while let Some(x) = stream.by_ref().try_next().await? {
        assert_eq!(Some("a".repeat(4001).as_str()), x.get(0));
    }

    stream.next_resultset();

    while let Some(x) = stream.by_ref().try_next().await? {
        assert_eq!(Some("b".repeat(2095).as_str()), x.get(0))
    }

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
        .map_ok(|x| x.get::<i32, _>(0).unwrap())
        .try_collect()
        .await;

    assert_eq!(rows?, vec![1; 3]);
    assert!(stream.next_resultset());

    let rows: Result<Vec<i32>> = stream
        .map_ok(|x| x.get::<i32, _>(0).unwrap())
        .try_collect()
        .await;

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

    assert_eq!("first", stream.columns().unwrap()[0].name());

    while let Some(x) = stream.by_ref().try_next().await? {
        assert_eq!(Some("a"), x.get(0))
    }

    assert_eq!("first", stream.columns().unwrap()[0].name());

    assert!(stream.next_resultset());
    assert_eq!("second", stream.columns().unwrap()[0].name());

    while let Some(x) = stream.by_ref().try_next().await? {
        assert_eq!(Some("b"), x.get(0))
    }

    assert_eq!("second", stream.columns().unwrap()[0].name());

    Ok(())
}

test_on_runtimes! { bool_type }
async fn bool_type<S>(mut conn: tiberius::Client<S>) -> Result<()> 
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{

    let row = conn
        .query("SELECT @P1, @P2 ORDER BY 1", &[&false, &true])
        .await?
        .into_row()
        .await?
        .unwrap();

    assert_eq!(Some(false), row.get(0));
    assert_eq!(Some(true), row.get(1));

    Ok(())
}

test_on_runtimes! { i8_token }
async fn i8_token<S>(mut conn: tiberius::Client<S>) -> Result<()> 
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let row = conn
        .query("SELECT @P1", &[&-4i8])
        .await?
        .into_row()
        .await?
        .unwrap();
    assert_eq!(Some(-4i8), row.get(0));
    Ok(())
}

test_on_runtimes! { i16_token }
async fn i16_token<S>(mut conn: tiberius::Client<S>) -> Result<()> 
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let row = conn
        .query("SELECT @P1", &[&-4i16])
        .await?
        .into_row()
        .await?
        .unwrap();

    assert_eq!(Some(-4i16), row.get(0));

    Ok(())
}

test_on_runtimes! { i32_token }
async fn i32_token<S>(mut conn: tiberius::Client<S>) -> Result<()> 
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{

    let row = conn
        .query("SELECT @P1", &[&-4i32])
        .await?
        .into_row()
        .await?
        .unwrap();

    assert_eq!(Some(-4i32), row.get(0));

    Ok(())
}

test_on_runtimes! { i64_token }
async fn i64_token<S>(mut conn: tiberius::Client<S>) -> Result<()> 
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{

    let row = conn
        .query("SELECT @P1", &[&-4i64])
        .await?
        .into_row()
        .await?
        .unwrap();

    assert_eq!(Some(-4i64), row.get(0));

    Ok(())
}

test_on_runtimes! { f32_token }
async fn f32_token<S>(mut conn: tiberius::Client<S>) -> Result<()> 
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let row = conn
        .query("SELECT @P1", &[&4.20f32])
        .await?
        .into_row()
        .await?
        .unwrap();

    assert_eq!(Some(4.20f32), row.get(0));

    Ok(())
}

test_on_runtimes! { f64_token }
async fn f64_token<S>(mut conn: tiberius::Client<S>) -> Result<()> 
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{

    let row = conn
        .query("SELECT @P1", &[&4.20f64])
        .await?
        .into_row()
        .await?
        .unwrap();

    assert_eq!(Some(4.20f64), row.get(0));

    Ok(())
}

test_on_runtimes! { short_strings }
async fn short_strings<S>(mut conn: tiberius::Client<S>) -> Result<()> 
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let row = conn
        .query("SELECT @P1", &[&s])
        .await?
        .into_row()
        .await?
        .unwrap();
    assert_eq!(Some(s), row.get::<&str, _>(0));

    Ok(())
}


test_on_runtimes! { long_strings }
async fn long_strings<S>(mut conn: tiberius::Client<S>) -> Result<()> 
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let string = "a".repeat(4001);

    let row = conn
        .query("SELECT @P1", &[&string.as_str()])
        .await?
        .into_row()
        .await?
        .unwrap();

    assert_eq!(Some(string.as_str()), row.get(0));
    Ok(())
}


test_on_runtimes! { stored_procedures }
async fn stored_procedures<S>(mut conn: tiberius::Client<S>) -> Result<()> 
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let rows = conn
        .query(
            "EXECUTE sp_executesql N'SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3'",
            &[],
        )
        .await?
        .into_first_result()
        .await?;

    assert_eq!(Some(1), rows[0].get(0));
    assert_eq!(Some(2), rows[1].get(0));
    assert_eq!(Some(3), rows[2].get(0));

    Ok(())
}


test_on_runtimes! { drop_stream_before_handling_all_results_should_not_cause_weird_things }
async fn drop_stream_before_handling_all_results_should_not_cause_weird_things<S>(mut conn: tiberius::Client<S>) -> Result<()> 
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{

    {
        let mut stream = conn
            .query(
                "SELECT @P1; SELECT @P2;",
                &[&"a".repeat(8000), &"b".repeat(8001)],
            )
            .await?;

        while let Some(x) = stream.try_next().await? {
            assert_eq!(Some("a".repeat(8000).as_str()), x.get(0))
        }
    }

    {
        let row = conn
            .query("SELECT @P1", &[&1i32])
            .await?
            .into_row()
            .await?
            .unwrap();
        assert_eq!(Some(1i32), row.get(0));
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
            res.push(row.get(i))
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

    let string = r#"Hääpuhetta voi värittää kertomalla vitsejä, aforismeja,
        sananlaskuja, laulunsäkeitä ja muita lainauksia. Huumori sopii hääpuheeseen,
        mutta vitsit eivät saa loukata. Häissä ensimmäisen juhlapuheen pitää
        perinteisesti morsiamen isä."#;

    conn.execute("CREATE TABLE ##TestNText (content NTEXT)", &[])
        .await?;

    conn.execute("INSERT INTO ##TestNText (content) VALUES (@P1)", &[&string])
        .await?;

    let row = conn
        .query("SELECT content FROM ##TestNText", &[])
        .await?
        .into_row()
        .await?
        .unwrap();

    assert_eq!(Some(string), row.get::<&str, _>(0));

    Ok(())
}


test_on_runtimes! { ntext_empty }
async fn ntext_empty<S>(mut conn: tiberius::Client<S>) -> Result<()> 
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{

    conn.execute("CREATE TABLE ##TestNTextEmpty (content NTEXT)", &[])
        .await?;

    conn.execute(
        "INSERT INTO ##TestNTextEmpty (content) VALUES (@P1)",
        &[&Option::<String>::None],
    )
    .await?;

    let row = conn
        .query("SELECT content FROM ##TestNTextEmpty", &[])
        .await?
        .into_row()
        .await?
        .unwrap();

    assert_eq!(None, row.get::<&str, _>(0));

    Ok(())
}


test_on_runtimes! { text_type }
async fn text_type<S>(mut conn: tiberius::Client<S>) -> Result<()> 
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{

    let string = "a".repeat(10000);

    conn.execute("CREATE TABLE ##TestText (content TEXT)", &[])
        .await?;

    conn.execute(
        "INSERT INTO ##TestText (content) VALUES (@P1)",
        &[&string.as_str()],
    )
    .await?;

    let row = conn
        .query("SELECT content FROM ##TestText", &[])
        .await?
        .into_row()
        .await?
        .unwrap();

    assert_eq!(Some(string.as_str()), row.get(0));

    Ok(())
}

#[tokio::test]
async fn varchar_empty() -> Result<()> {
    let mut conn = connect().await?;

    conn.execute("CREATE TABLE ##TestFoo (content NVARCHAR(max))", &[])
        .await?;

    conn.execute(
        "INSERT INTO ##TestFoo (content) VALUES (@P1)",
        &[&Option::<String>::None],
    )
    .await?;

    let row = conn
        .query("SELECT content FROM ##TestFoo", &[])
        .await?
        .into_row()
        .await?
        .unwrap();

    assert_eq!(None, row.get::<&str, _>(0));

    Ok(())
}


test_on_runtimes! { text_empty }
async fn text_empty<S>(mut conn: tiberius::Client<S>) -> Result<()> 
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{

    conn.execute("CREATE TABLE ##TestTextEmpty (content TEXT)", &[])
        .await?;

    conn.execute(
        "INSERT INTO ##TestTextEmpty (content) VALUES (@P1)",
        &[&Option::<String>::None],
    )
    .await?;

    let row = conn
        .query("SELECT content FROM ##TestTextEmpty", &[])
        .await?
        .into_row()
        .await?
        .unwrap();

    assert_eq!(None, row.get::<&str, _>(0));

    Ok(())
}


test_on_runtimes! { varbinary_empty }
async fn varbinary_empty<S>(mut conn: tiberius::Client<S>) -> Result<()> 
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{

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

    let row = conn
        .query("SELECT content FROM ##TestVarBinaryEmpty", &[])
        .await?
        .into_row()
        .await?
        .unwrap();

    assert_eq!(None, row.get::<&[u8], _>(0));

    Ok(())
}


test_on_runtimes! { binary_type }
async fn binary_type<S>(mut conn: tiberius::Client<S>) -> Result<()> 
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{

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

    let row = conn
        .query("SELECT content FROM ##TestBinary", &[])
        .await?
        .into_row()
        .await?
        .unwrap();

    let result: &[u8] = row.get(0).unwrap();

    assert_eq!(8000, result.len());
    assert_eq!(binary.as_slice(), result);

    Ok(())
}


test_on_runtimes! { varbinary_type }
async fn varbinary_type<S>(mut conn: tiberius::Client<S>) -> Result<()> 
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{

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

    let row = conn
        .query("SELECT content FROM ##VarBinary", &[])
        .await?
        .into_row()
        .await?
        .unwrap();

    let result: &[u8] = row.get(0).unwrap();

    assert_eq!(80, result.len());
    assert_eq!(binary, result);

    Ok(())
}


test_on_runtimes! { image_type }
async fn image_type<S>(mut conn: tiberius::Client<S>) -> Result<()> 
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{

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

    let row = conn
        .query("SELECT content FROM ##ImageType", &[])
        .await?
        .into_row()
        .await?
        .unwrap();

    let result: &[u8] = row.get(0).unwrap();

    assert_eq!(80, result.len());
    assert_eq!(binary, result);

    Ok(())
}


test_on_runtimes! { guid_type }
async fn guid_type<S>(mut conn: tiberius::Client<S>) -> Result<()> 
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let id = Uuid::new_v4();
    let row = conn
        .query("SELECT @P1", &[&id])
        .await?
        .into_row()
        .await?
        .unwrap();

    assert_eq!(Some(id), row.get(0));

    Ok(())
}


test_on_runtimes! { varbinary_max }
async fn varbinary_max<S>(mut conn: tiberius::Client<S>) -> Result<()> 
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{

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

    let row = conn
        .query("SELECT content FROM ##MaxBinary", &[])
        .await?
        .into_row()
        .await?
        .unwrap();

    let result: &[u8] = row.get(0).unwrap();

    assert_eq!(8001, result.len());
    assert_eq!(binary, result);

    Ok(())
}

test_on_runtimes! { numeric_type_u32_presentation }
async fn numeric_type_u32_presentation<S>(mut conn: tiberius::Client<S>) -> Result<()> 
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let num = Numeric::new_with_scale(2, 1);
    let row = conn
        .query("SELECT @P1", &[&num])
        .await?
        .into_row()
        .await?
        .unwrap();

    assert_eq!(Some(num), row.get(0));

    Ok(())
}


test_on_runtimes! { numeric_type_u64_presentation }
async fn numeric_type_u64_presentation<S>(mut conn: tiberius::Client<S>) -> Result<()> 
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let num = Numeric::new_with_scale(std::i32::MAX as i128 + 10, 1);
    let stream = conn.query("SELECT @P1", &[&num]).await?;

    let row = conn
        .query("SELECT @P1", &[&num])
        .await?
        .into_row()
        .await?
        .unwrap();

    assert_eq!(Some(num), row.get(0));

    Ok(())
}


test_on_runtimes! { numeric_type_u96_presentation }
async fn numeric_type_u96_presentation<S>(mut conn: tiberius::Client<S>) -> Result<()> 
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let num = Numeric::new_with_scale(std::i64::MAX as i128, 19);
    let stream = conn.query("SELECT @P1", &[&num]).await?;

    let row = conn
        .query("SELECT @P1", &[&num])
        .await?
        .into_row()
        .await?
        .unwrap();

    assert_eq!(Some(num), row.get(0));

    Ok(())
}


test_on_runtimes! { numeric_type_u128_presentation }
async fn numeric_type_u128_presentation<S>(mut conn: tiberius::Client<S>) -> Result<()> 
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let num = Numeric::new_with_scale(std::i64::MAX as i128, 37);
    let stream = conn.query("SELECT @P1", &[&num]).await?;

    let row = conn
        .query("SELECT @P1", &[&num])
        .await?
        .into_row()
        .await?
        .unwrap();

    assert_eq!(Some(num), row.get(0));

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
    let row = conn
        .query("SELECT @P1", &[&num])
        .await?
        .into_row()
        .await?
        .unwrap();

    assert_eq!(Some(num), row.get(0));

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

    let row = conn
        .query("SELECT @P1 AS foo", &[&num])
        .await?
        .into_row()
        .await?
        .unwrap();

    assert_eq!(Some(num), row.get("foo"));

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
    let row = conn
        .query("SELECT @P1", &[&num])
        .await?
        .into_row()
        .await?
        .unwrap();

    assert_eq!(Some(num), row.get(0));

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
    let row = conn
        .query("SELECT @P1", &[&num])
        .await?
        .into_row()
        .await?
        .unwrap();

    assert_eq!(Some(num), row.get(0));

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

    conn.execute("CREATE TABLE ##TestSmallDt (date smalldatetime)", &[])
        .await?;

    conn.execute("INSERT INTO ##TestSmallDt (date) VALUES (@P1)", &[&dt])
        .await?
        .total();

    let row = conn
        .query("SELECT date FROM ##TestSmallDt", &[])
        .await?
        .into_row()
        .await?
        .unwrap();

    assert_eq!(Some(dt), row.get(0));

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

    conn.execute("CREATE TABLE ##NaiveDateTime2 (date datetime2)", &[])
        .await?;

    conn.execute("INSERT INTO ##NaiveDateTime2 (date) VALUES (@P1)", &[&dt])
        .await?
        .total();

    let row = conn
        .query("SELECT date FROM ##NaiveDateTime2", &[])
        .await?
        .into_row()
        .await?
        .unwrap();

    assert_eq!(Some(dt), row.get(0));

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

    let row = conn
        .query("SELECT @P1", &[&dt])
        .await?
        .into_row()
        .await?
        .unwrap();

    assert_eq!(Some(dt), row.get(0));

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

    let row = conn
        .query("SELECT @P1", &[&time])
        .await?
        .into_row()
        .await?
        .unwrap();

    assert_eq!(Some(time), row.get(0));

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

    let row = conn
        .query("SELECT @P1", &[&date])
        .await?
        .into_row()
        .await?
        .unwrap();

    assert_eq!(Some(date), row.get(0));

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

    let row = conn
        .query("SELECT @P1", &[&dt])
        .await?
        .into_row()
        .await?
        .unwrap();

    assert_eq!(Some(dt), row.get(0));

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

    let row = conn
        .query("SELECT @P1", &[&dt])
        .await?
        .into_row()
        .await?
        .unwrap();

    assert_eq!(Some(dt), row.get(0));

    Ok(())
}


test_on_runtimes! { xml_read_write }
async fn xml_read_write<S>(mut conn: tiberius::Client<S>) -> Result<()> 
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{

    let xml = XmlData::new("<root><child attr=\"attr-value\"/></root>");
    let row = conn
        .query("SELECT @P1", &[&xml])
        .await?
        .into_row()
        .await?
        .unwrap();

    assert_eq!(Some(&xml), row.get(0));

    Ok(())
}

test_on_runtimes! { xml_read_null_xml }
async fn xml_read_null_xml<S>(mut conn: tiberius::Client<S>) -> Result<()> 
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{

    let row = conn
        .query("SELECT CAST(NULL AS XML)", &[])
        .await?
        .into_row()
        .await?
        .unwrap();

    assert_eq!(None, row.get::<&XmlData, _>(0));

    Ok(())
}
