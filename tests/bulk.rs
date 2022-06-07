use enumflags2::BitFlags;
use futures::{lock::Mutex, AsyncRead, AsyncWrite};
use names::{Generator, Name};
use once_cell::sync::Lazy;
use std::env;
use std::sync::Once;
use tiberius::{BulkLoadMetadata, ColumnFlag, IntoSql, Result, TokenRow, TypeInfo, TypeLength};

use runtimes_macro::test_on_runtimes;

// This is used in the testing macro :)
#[allow(dead_code)]
static LOGGER_SETUP: Once = Once::new();

static CONN_STR: Lazy<String> = Lazy::new(|| {
    env::var("TIBERIUS_TEST_CONNECTION_STRING").unwrap_or_else(|_| {
        "server=tcp:localhost,1433;IntegratedSecurity=true;TrustServerCertificate=true".to_owned()
    })
});

static NAMES: Lazy<Mutex<Generator>> =
    Lazy::new(|| Mutex::new(Generator::with_naming(Name::Plain)));

async fn random_table() -> String {
    NAMES.lock().await.next().unwrap().replace('-', "")
}

macro_rules! test_bulk_type {
    ($name:ident($sql_type:literal, $type_info:expr, $total_generated:expr, $generator:expr)) => {
        paste::item! {
            #[test_on_runtimes]
            async fn [< bulk_load_optional_ $name >]<S>(mut conn: tiberius::Client<S>) -> Result<()>
            where
                S: AsyncRead + AsyncWrite + Unpin + Send,
            {
                let table = format!("##{}", random_table().await);

                conn.execute(
                    &format!(
                        "CREATE TABLE {} (id INT IDENTITY PRIMARY KEY, content {} NULL)",
                        table,
                        $sql_type,
                    ),
                    &[],
                )
                    .await?;

                let mut meta = BulkLoadMetadata::new();
                meta.add_column("content", $type_info, ColumnFlag::Nullable);

                let mut req = conn.bulk_insert(&table, meta).await?;

                for i in $generator {
                    let mut row = TokenRow::new();
                    row.push(i.into_sql());
                    req.send(row).await?;
                }

                let res = req.finalize().await?;

                assert_eq!($total_generated, res.total());

                Ok(())
            }

            #[test_on_runtimes]
            async fn [< bulk_load_required_ $name >]<S>(mut conn: tiberius::Client<S>) -> Result<()>
            where
                S: AsyncRead + AsyncWrite + Unpin + Send,
            {
                let table = format!("##{}", random_table().await);

                conn.execute(
                    &format!(
                        "CREATE TABLE {} (id INT IDENTITY PRIMARY KEY, content {} NOT NULL)",
                        table,
                        $sql_type
                    ),
                    &[],
                )
                    .await?;

                let mut meta = BulkLoadMetadata::new();
                meta.add_column("content", $type_info, BitFlags::empty());

                let mut req = conn.bulk_insert(&table, meta).await?;

                for i in $generator {
                    let mut row = TokenRow::new();
                    row.push(i.into_sql());
                    req.send(row).await?;
                }

                let res = req.finalize().await?;

                assert_eq!($total_generated, res.total());

                Ok(())
            }
        }
    };
}

test_bulk_type!(tinyint("TINYINT", TypeInfo::tinyint(), 256, 0..=255u8));
test_bulk_type!(smallint("SMALLINT", TypeInfo::smallint(), 2000, 0..2000i16));
test_bulk_type!(int("INT", TypeInfo::int(), 2000, 0..2000i32));
test_bulk_type!(bigint("BIGINT", TypeInfo::bigint(), 2000, 0..2000i64));

test_bulk_type!(real(
    "REAL",
    TypeInfo::real(),
    1000,
    vec![3.14f32; 1000].into_iter()
));

test_bulk_type!(float(
    "FLOAT",
    TypeInfo::float(),
    1000,
    vec![3.14f64; 1000].into_iter()
));

test_bulk_type!(varchar_limited(
    "VARCHAR(255)",
    TypeInfo::varchar(TypeLength::Limited(255)),
    1000,
    vec!["aaaaaaaaaaaaaaaaaaaaaaa"; 1000].into_iter()
));
