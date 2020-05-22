use futures::lock::Mutex;
use futures_util::{StreamExt, TryStreamExt};
use names::{Generator, Name};
use once_cell::sync::Lazy;
use std::env;
use std::sync::Once;
use tiberius::{numeric::Numeric, xml::XmlData, Result};
use uuid::Uuid;

use runtimes_macro::test_on_runtimes;

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


#[test_on_runtimes]
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
