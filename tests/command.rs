use futures_util::io::{AsyncRead, AsyncWrite};
use futures_util::stream::TryStreamExt;
use names::{Generator, Name};
use once_cell::sync::Lazy;
use std::cell::RefCell;
use std::env;
use std::sync::Once;

use tiberius::FromSql;
use tiberius::{numeric::Numeric, xml::XmlData, ColumnType, Command, CommandItem, Result};
use uuid::Uuid;

use runtimes_macro::test_on_runtimes;

// This is used in the testing macro :)
#[allow(dead_code)]
static LOGGER_SETUP: Once = Once::new();

static CONN_STR: Lazy<String> = Lazy::new(|| {
    env::var("TIBERIUS_TEST_CONNECTION_STRING").unwrap_or_else(|_| {
        "server=tcp:localhost,1433;user=SA;password=<YourStrong@Passw0rd>;IntegratedSecurity=true;TrustServerCertificate=true".to_owned()
    })
});

thread_local! {
    static NAMES: RefCell<Option<Generator<'static>>> =
    RefCell::new(None);
}

async fn random_table() -> String {
    NAMES.with(|maybe_generator| {
        maybe_generator
            .borrow_mut()
            .get_or_insert_with(|| Generator::with_naming(Name::Plain))
            .next()
            .unwrap()
            .replace('-', "")
    })
}

#[test_on_runtimes]
async fn basic_proc_exec<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let table = random_table().await;
    let proc = random_table().await;

    conn.simple_query(format!(
        r#"
        create table ##{} (
            id int identity(1,1),
            other varchar(50),
        )
    "#,
        table
    ))
    .await?;

    conn.simple_query(format!(
        r#"
        create or alter procedure {} 
          @Param1 varchar(50)
        as
            insert into ##{} (other)
            values (@Param1)

            return scope_identity()
    "#,
        proc, table,
    ))
    .await?;

    let mut ins_cmd = Command::new(&proc);
    ins_cmd.bind_param("@Param1", "some text");

    let result = ins_cmd.exec(&mut conn).await?.into_command_result().await?;
    assert_eq!(1, result.return_code());

    let mut ins_cmd = Command::new(&proc);
    ins_cmd.bind_param("@Param1", "another text");

    let result = ins_cmd.exec(&mut conn).await?.into_command_result().await?;
    assert_eq!(2, result.return_code());

    Ok(())
}
