use indicatif::ProgressBar;
use once_cell::sync::Lazy;
use std::env;
use tiberius::numeric::Numeric;
use tiberius::time::{Date, DateTime, DateTime2, DateTimeOffset, SmallDateTime, Time};
use tiberius::{Client, ColumnData, Config, IntoSql, TokenRow};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;
use tracing::log::info;
use uuid::Uuid;

static CONN_STR: Lazy<String> = Lazy::new(|| {
    env::var("TIBERIUS_TEST_CONNECTION_STRING").unwrap_or_else(|_| {
        "server=tcp:localhost,1433;IntegratedSecurity=true;TrustServerCertificate=true".to_owned()
    })
});

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let config = Config::from_ado_string(&CONN_STR)?;

    let tcp = TcpStream::connect(config.get_addr()).await?;
    tcp.set_nodelay(true)?;

    let mut client = Client::connect(config, tcp.compat_write()).await?;

    client
        .execute("DROP TABLE IF EXISTS bulk_test1", &[])
        .await?;
    info!("drop table");
    client
        .execute(
            r#"CREATE TABLE bulk_test1 (
                        id INT IDENTITY PRIMARY KEY,
                        null_bit bit NULL, 
                        nonnull_bit bit NOT NULL,
                        null_tinyint tinyint NULL,
                        nonnull_tinyint tinyint NOT NULL, 
                        null_smallint smallint NULL,
                        nonnull_smallint smallint NOT NULL, 
                        null_int int NULL, 
                        nonnull_int int NOT NULL,
                        null_big bigint NULL,
                        nonnull_bigint bigint NOT NULL,
                        null_float real NULL,
                        nonnull_float real NOT NULL,
                        null_double float NULL,
                        nonnull_double float NOT NULL,
                        null_guid uniqueidentifier NULL,
                        nonnull_guid uniqueidentifier NOT NULL,
                        null_char40 char(40) NULL,
                        nonnull_char40 char(40) NOT NULL,
                        varchar40 varchar(40) NULL,
                        wvarchar40 nvarchar(40) NULL,
                        binary40 binary(40) NULL,
                        varbinary40 varbinary(40) NULL,
                        null_datetime datetime NULL,
                        nonnull_datetime datetime NOT NULL,
                        null_time time NULL,
                        null_datetime2 datetime2 NULL,
                        null_datetimeoffset datetimeoffset NULL,
                        null_numeric numeric NULL,
                        nonnull_numeric numeric NOT NULL)"#,
            &[],
        )
        .await?;
    info!("create table done");

    let mut req = client.bulk_insert_1("bulk_test1").await?;

    let count = 1000i32;

    let pb = ProgressBar::new(count as u64);

    info!("start loading data");
    for i in 0..1000 {
        let mut row = TokenRow::new();

        let null_bit = [Some(true), None][i % 2];
        row.push(null_bit.into_sql());

        let nonnull_bit = false;
        row.push(nonnull_bit.into_sql());

        let null_tinyint = [Some(23u8), None][i % 2];
        row.push(null_tinyint.into_sql());

        let nonnull_tinyint = 45u8;
        row.push(nonnull_tinyint.into_sql());

        let null_smallint = [Some(23i16), None][i % 2];
        row.push(null_smallint.into_sql());

        let nonnull_smallint = 45i16;
        row.push(nonnull_smallint.into_sql());

        let null_int = [Some(32), None][i % 2];
        row.push(null_int.into_sql());

        let nonnull_int = 44;
        row.push(nonnull_int.into_sql());

        let null_bigint = [Some(32i64), None][i % 2];
        row.push(null_bigint.into_sql());

        let nonnull_bigint = 44i64;
        row.push(nonnull_bigint.into_sql());

        let null_float = [Some(34f32), None][i % 2];
        row.push(null_float.into_sql());

        let nonnull_float = 32f32;
        row.push(nonnull_float.into_sql());

        let null_double = [Some(34f64), None][i % 2];
        row.push(null_double.into_sql());

        let nonnull_double = 32f64;
        row.push(nonnull_double.into_sql());

        let null_guid = [Some(Uuid::new_v4()), None][i % 2];
        row.push(null_guid.into_sql());

        let nonnull_guid = Uuid::new_v4();
        row.push(nonnull_guid.into_sql());

        let null_char40 = [Some("aaa"), None][i % 2];
        row.push(null_char40.into_sql());

        let nonnull_char40 = "ddddddddddddddddddd";
        row.push(nonnull_char40.into_sql());

        let varchar40 = [Some("fffff"), None][i % 2];
        row.push(varchar40.into_sql());

        let wvarchar40 = [Some("fffff"), None][i % 2];
        row.push(wvarchar40.into_sql());

        let binary40 = [Some(b"fffff".as_slice()), None][i % 2];
        row.push(binary40.into_sql());

        let varbinary40 = [Some(b"fffff".as_slice()), None][i % 2];
        row.push(varbinary40.into_sql());

        let null_datetime = [Some(DateTime::new(200, 3000)), None][i % 2];
        row.push(ColumnData::DateTime(null_datetime));

        let nonnull_datetime = DateTime::new(300, 2000);
        row.push(ColumnData::DateTime(Some(nonnull_datetime)));

        let null_time = [Some(Time::new(55, 7)), None][i % 2];
        row.push(ColumnData::Time(null_time));

        let null_datetime2 = [Some(DateTime2::new(Date::new(55), Time::new(44, 7))), None][i % 2];
        row.push(ColumnData::DateTime2(null_datetime2));

        let null_datetimeoffset = [
            Some(DateTimeOffset::new(
                DateTime2::new(Date::new(55), Time::new(44, 7)),
                -9,
            )),
            None,
        ][i % 2];
        row.push(ColumnData::DateTimeOffset(null_datetimeoffset));

        let null_numeric = [Some(Numeric::new_with_scale(12, 0)), None][i % 2];
        row.push(null_numeric.into_sql());

        let nonnull_numeric = Numeric::new_with_scale(23, 0);
        row.push(nonnull_numeric.into_sql());

        req.send(row).await?;
        pb.inc(1);
    }

    pb.finish_with_message("waiting...");

    let res = req.finalize().await?;

    info!("{:?}", res);

    Ok(())
}
