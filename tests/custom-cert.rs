#![cfg(unix)]
use std::sync::Once;
use tiberius::{AuthMethod, Client, Config, EncryptionLevel, Result};
use tokio::{net::TcpStream, runtime::Runtime};
use tokio_util::compat::TokioAsyncWriteCompatExt;

#[allow(dead_code)]
static LOGGER_SETUP: Once = Once::new();

#[allow(dead_code)]
fn load_ca_bytes() -> Result<Vec<u8>> {
    let ca_path = std::env::current_dir()?.join("docker/certs/customCA.crt");
    let ca_bytes = std::fs::read(&ca_path)?;
    Ok(ca_bytes)
}

#[test]
#[cfg(any(
    feature = "rustls",
    feature = "native-tls",
    feature = "vendored-openssl"
))]
fn connect_to_custom_cert_instance_ado() -> Result<()> {
    LOGGER_SETUP.call_once(|| {
        env_logger::init();
    });

    let rt = Runtime::new()?;

    rt.block_on(async {
        #[allow(unused_variables)]
        let ca_bytes = load_ca_bytes()?;

        let mut config =
            Config::from_ado_string("server=tcp:localhost,1433;IntegratedSecurity=true")?;
        config.trust_cert();
        config.authentication(AuthMethod::sql_server("sa", "<YourStrong@Passw0rd>"));

        let tcp = TcpStream::connect(config.get_addr()).await?;
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

#[test]
#[cfg(any(
    feature = "rustls",
    feature = "native-tls",
    feature = "vendored-openssl"
))]
fn connect_to_custom_cert_instance_jdbc() -> Result<()> {
    LOGGER_SETUP.call_once(|| {
        env_logger::init();
    });

    let rt = Runtime::new()?;
    rt.block_on(async {
        #[allow(unused_variables)]
        let ca_bytes = load_ca_bytes()?;

        let mut config = Config::from_jdbc_string("jdbc:sqlserver://localhost:1433")?;
        config.trust_cert();
        config.authentication(AuthMethod::sql_server("sa", "<YourStrong@Passw0rd>"));

        let tcp = TcpStream::connect(config.get_addr()).await?;
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

#[test]
fn connect_to_custom_cert_instance_without_ca() -> Result<()> {
    LOGGER_SETUP.call_once(|| {
        env_logger::init();
    });

    let rt = Runtime::new()?;
    rt.block_on(async {
        let mut config = Config::new();
        config.authentication(AuthMethod::sql_server("sa", "<YourStrong@Passw0rd>"));
        config.encryption(EncryptionLevel::On);
        config.host("localhost");
        config.port(1433);

        let tcp = TcpStream::connect(config.get_addr()).await?;
        let client = Client::connect(config, tcp.compat_write()).await;

        // Should fail because we didn’t add the CA
        assert!(client.is_err());
        Ok(())
    })
}
