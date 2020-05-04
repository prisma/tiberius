use futures_util::{StreamExt, TryStreamExt};
use once_cell::sync::Lazy;
use std::env;
use std::sync::Once;
//use tiberius::{Client, ClientBuilder, Result};
use uuid::Uuid;

use tiberius::{ClientBuilder, Result, Client, GenericTcpStream};

use async_std::{io, net::{self, ToSocketAddrs}};
use async_trait::async_trait;

pub struct AsyncStdTcpStreamWrapper();

#[async_trait]
impl GenericTcpStream<smol::Async<std::net::TcpStream>> for AsyncStdTcpStreamWrapper {
    async fn connect(&self, addr: String, instance_name: &Option<String>) -> tiberius::Result<smol::Async<std::net::TcpStream>>
    {
        let mut addr = addr.to_socket_addrs().await?.next().ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, "Could not resolve server host.")
        })?;

        if let Some(ref instance_name) = instance_name {
            addr = tiberius::find_tcp_port(addr, instance_name).await?;
        };
        Ok(smol::Async::<std::net::TcpStream>::connect(addr).await?)
    }
}


//#[async_trait]
//impl GenericTcpStream<net::TcpStream> for AsyncStdTcpStreamWrapper {
//    async fn connect(&self, addr: String, instance_name: &Option<String>) -> tiberius::Result<net::TcpStream> 
//    {
//        let mut addr = addr.to_socket_addrs().await?.next().ok_or_else(|| {
//            io::Error::new(io::ErrorKind::NotFound, "Could not resolve server host.")
//        })?;
//
//        if let Some(ref instance_name) = instance_name {
//            addr = tiberius::find_tcp_port(addr, instance_name).await?;
//        };
//        Ok(net::TcpStream::connect(addr).await?)
//    }
//}
//
static LOGGER_SETUP: Once = Once::new();

static CONN_STR: Lazy<String> = Lazy::new(|| {
    env::var("TIBERIUS_TEST_CONNECTION_STRING")
        .unwrap_or("server=tcp:localhost,1433;TrustServerCertificate=true".to_owned())
});

async fn connect() -> Result<Client<smol::Async<std::net::TcpStream>>> {
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

    assert_eq!(2, res.total().await?);

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

    assert_eq!(1, res.total().await?);

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

    let result: Vec<_> = insert_count.try_collect().await?;
    assert_eq!(vec![1, 2], result);

    Ok(())
}

#[async_std::test]
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

#[async_std::test]
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

#[async_std::test]
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

#[async_std::test]
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

#[async_std::test]
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

#[async_std::test]
async fn test_type_i8() -> Result<()> {
    let mut conn = connect().await?;
    let stream = conn.query("SELECT @P1", &[&-4i8]).await?;

    let rows: Result<Vec<i8>> = stream.map_ok(|x| x.get::<_, i8>(0)).try_collect().await;
    assert_eq!(rows?, vec![-4i8]);
    Ok(())
}

#[async_std::test]
async fn test_type_i16() -> Result<()> {
    let mut conn = connect().await?;
    let stream = conn.query("SELECT @P1", &[&-4i16]).await?;

    let rows: Result<Vec<i16>> = stream.map_ok(|x| x.get::<_, i16>(0)).try_collect().await;
    assert_eq!(rows?, vec![-4i16]);
    Ok(())
}

#[async_std::test]
async fn test_type_i32() -> Result<()> {
    let mut conn = connect().await?;
    let stream = conn.query("SELECT @P1", &[&-4i32]).await?;

    let rows: Result<Vec<i32>> = stream.map_ok(|x| x.get::<_, i32>(0)).try_collect().await;
    assert_eq!(rows?, vec![-4i32]);
    Ok(())
}

#[async_std::test]
async fn test_type_i64() -> Result<()> {
    let mut conn = connect().await?;
    let stream = conn.query("SELECT @P1", &[&-4i64]).await?;

    let rows: Result<Vec<i64>> = stream.map_ok(|x| x.get::<_, i64>(0)).try_collect().await;
    assert_eq!(rows?, vec![-4i64]);
    Ok(())
}

#[async_std::test]
async fn test_type_f32() -> Result<()> {
    let mut conn = connect().await?;
    let stream = conn.query("SELECT @P1", &[&4.20f32]).await?;

    let rows: Result<Vec<f32>> = stream.map_ok(|x| x.get::<_, f32>(0)).try_collect().await;
    assert_eq!(rows?, vec![4.20f32]);
    Ok(())
}

#[async_std::test]
async fn test_type_f64() -> Result<()> {
    let mut conn = connect().await?;
    let stream = conn.query("SELECT @P1", &[&4.20f64]).await?;

    let rows: Result<Vec<f64>> = stream.map_ok(|x| x.get::<_, f64>(0)).try_collect().await;
    assert_eq!(rows?, vec![4.20f64]);
    Ok(())
}

#[async_std::test]
async fn test_type_short_string() -> Result<()> {
    let mut conn = connect().await?;
    let stream = conn.query("SELECT @P1", &[&"Hallo"]).await?;

    let rows: Result<Vec<String>> = stream.map_ok(|x| x.get::<_, String>(0)).try_collect().await;
    assert_eq!(rows?, vec![String::from("Hallo")]);
    Ok(())
}

#[async_std::test]
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
#[async_std::test]
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

#[async_std::test]
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

#[async_std::test]
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

#[async_std::test]
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

#[async_std::test]
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

#[async_std::test]
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

#[async_std::test]
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

#[async_std::test]
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

#[async_std::test]
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

#[async_std::test]
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

#[async_std::test]
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

#[async_std::test]
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

#[async_std::test]
async fn test_guid() -> Result<()> {
    let mut conn = connect().await?;

    let id = Uuid::new_v4();
    let stream = conn.query("SELECT @P1", &[&id]).await?;

    let rows: Vec<Uuid> = stream.map_ok(|x| x.get::<_, Uuid>(0)).try_collect().await?;

    assert_eq!(id, rows[0]);

    Ok(())
}

#[async_std::test]
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

#[async_std::test]
#[cfg(all(feature = "chrono", feature = "tds73"))]
async fn test_naive_small_date_time_tds73() -> Result<()> {
    use chrono::{NaiveDate, NaiveDateTime};

    let mut conn = connect().await?;
    let dt = NaiveDate::from_ymd(2020, 4, 20).and_hms(16, 20, 0);

    conn.execute("CREATE TABLE ##TestSmallDt (date smalldatetime)", &[])
        .await?;

    conn.execute("INSERT INTO ##TestSmallDt (date) VALUES (@P1)", &[&dt])
        .await?
        .total()
        .await?;

    let stream = conn.query("SELECT date FROM ##TestSmallDt", &[]).await?;

    let rows: Vec<_> = stream
        .map_ok(|x| x.get::<_, NaiveDateTime>(0))
        .try_collect()
        .await?;

    assert_eq!(dt, rows[0]);
    Ok(())
}

#[async_std::test]
#[cfg(all(feature = "chrono", feature = "tds73"))]
async fn test_naive_date_time2_tds73() -> Result<()> {
    use chrono::{NaiveDate, NaiveDateTime};

    let mut conn = connect().await?;
    let dt = NaiveDate::from_ymd(2020, 4, 20).and_hms(16, 20, 0);

    conn.execute("CREATE TABLE ##NaiveDateTime2 (date datetime2)", &[])
        .await?;

    conn.execute("INSERT INTO ##NaiveDateTime2 (date) VALUES (@P1)", &[&dt])
        .await?
        .total()
        .await?;

    let stream = conn.query("SELECT date FROM ##NaiveDateTime2", &[]).await?;

    let rows: Vec<_> = stream
        .map_ok(|x| x.get::<_, NaiveDateTime>(0))
        .try_collect()
        .await?;

    assert_eq!(dt, rows[0]);
    Ok(())
}

#[async_std::test]
#[cfg(all(feature = "chrono", not(feature = "tds73")))]
async fn test_naive_date_time_tds72() -> Result<()> {
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

#[async_std::test]
#[cfg(all(feature = "chrono", feature = "tds73"))]
async fn test_naive_time() -> Result<()> {
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

#[async_std::test]
#[cfg(all(feature = "chrono", feature = "tds73"))]
async fn test_naive_date() -> Result<()> {
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

#[async_std::test]
#[cfg(all(feature = "chrono", feature = "tds73"))]
async fn test_date_time_utc() -> Result<()> {
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

#[async_std::test]
#[cfg(all(feature = "chrono", feature = "tds73"))]
async fn test_date_time_fixed() -> Result<()> {
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
