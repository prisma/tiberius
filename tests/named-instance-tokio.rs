#![cfg(all(windows, feature = "sql-browser-tokio"))]

use once_cell::sync::Lazy;
use std::env;
use std::sync::Once;
use tiberius::{Client, Config, Result, SqlBrowser};
use tokio::{net::TcpStream, runtime::Runtime};
use tokio_util::compat::TokioAsyncWriteCompatExt;

// This is used in the testing macro :)
#[allow(dead_code)]
static LOGGER_SETUP: Once = Once::new();

static CONN_STR: Lazy<String> = Lazy::new(|| {
    env::var("TIBERIUS_TEST_CONNECTION_STRING").unwrap_or_else(|_| {
        "server=tcp:localhost,1433;IntegratedSecurity=true;TrustServerCertificate=true".to_owned()
    })
});

static NAMED_INSTANCE_CONN_STR: Lazy<String> = Lazy::new(|| {
    let instance_name = env::var("TIBERIUS_TEST_INSTANCE").unwrap_or("MSSQLSERVER".to_owned());
    CONN_STR.replace(",1433", &format!("\\{}", instance_name))
});

#[test]
fn connect_to_named_instance() -> Result<()> {
    LOGGER_SETUP.call_once(|| {
        env_logger::init();
    });

    let rt = Runtime::new()?;

    rt.block_on(async {
        let config = Config::from_ado_string(&NAMED_INSTANCE_CONN_STR)?;

        let tcp = TcpStream::connect_named(&config).await?;
        tcp.set_nodelay(true)?;

        let mut client = Client::connect(config, tcp.compat_write()).await?;

        let row = client
            .query("SELECT @P1", &[&-4i32])
            .await?
            .into_row()
            .await?
            .unwrap();

        assert_eq!(Some(-4i32), row.get(0));
        Ok(())
    })
}
