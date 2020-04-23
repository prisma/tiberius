use chrono::{offset::*, DateTime, NaiveDate};
use futures::TryStreamExt;
use tiberius::{AuthMethod, Client};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let mut builder = Client::builder();
    builder.host("0.0.0.0");
    builder.port(1433);
    builder.database("master");
    builder.authentication(AuthMethod::sql_server("SA", "<YourStrong@Passw0rd>"));

    let mut conn = builder.build().await?;

    let naive = NaiveDate::from_ymd(2020, 4, 20).and_hms(16, 20, 0);
    let fixed = FixedOffset::west(3600 * 3);
    let dt: DateTime<FixedOffset> = DateTime::from_utc(naive, fixed);

    conn.execute("CREATE TABLE TestSmallDt (date datetimeoffset)", &[])
        .await?;

    conn.execute("INSERT INTO TestSmallDt (date) VALUES (@P1)", &[&dt])
        .await?
        .total()
        .await?;

    let stream = conn.query("SELECT date FROM TestSmallDt", &[]).await?;

    let rows: Vec<_> = stream
        .map_ok(|x| x.get::<_, DateTime<Utc>>(0))
        .try_collect()
        .await?;

    assert_eq!(dt, rows[0]);

    Ok(())
}
