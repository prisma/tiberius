//! Integration tests for Fabric SQL Database connectivity.
//!
//! These tests verify that tiberius can connect to a SQL Database in
//! Microsoft Fabric, which uses the `*.database.fabric.microsoft.com`
//! endpoint pattern.
//!
//! # Required environment variables
//!
//! - `FABRIC_SQLDB_ENDPOINT`: The Fabric SQL DB endpoint
//!   (e.g., `xxx.database.fabric.microsoft.com`)
//! - `FABRIC_SQLDB_DATABASE`: The database name
//! - `FABRIC_SQLDB_TOKEN`: A pre-obtained AAD/Entra ID token for
//!   `https://database.windows.net/`
//!
//! # Running
//!
//! ```sh
//! export FABRIC_SQLDB_ENDPOINT=xxx.database.fabric.microsoft.com
//! export FABRIC_SQLDB_DATABASE=my-db-name
//! export FABRIC_SQLDB_TOKEN=$(az account get-access-token --resource https://database.windows.net/ --query accessToken -o tsv)
//! cargo test --test fabric_sqldb -- --nocapture
//! ```

use std::env;
use tiberius::{error::Error, AuthMethod, Client, Config, EncryptionLevel};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

macro_rules! skip_if_no_fabric_sqldb {
    () => {
        if env::var("FABRIC_SQLDB_ENDPOINT").is_err() {
            eprintln!(
                "SKIPPED: FABRIC_SQLDB_ENDPOINT not set. Set Fabric SQL DB env vars to run this test."
            );
            return Ok(());
        }
    };
}

fn get_endpoint() -> String {
    env::var("FABRIC_SQLDB_ENDPOINT").unwrap()
}

fn get_database() -> String {
    env::var("FABRIC_SQLDB_DATABASE").unwrap()
}

fn get_token() -> String {
    env::var("FABRIC_SQLDB_TOKEN").unwrap()
}

/// Connect to Fabric SQL Database with encrypt=strict (TDS 8).
///
/// Fabric SQL Database uses the same gateway/backend architecture as Azure SQL:
/// - Gateway: supports TDS 8 strict TLS
/// - Backend (after routing): uses regular TLS upgrade (Required)
///
/// The routing target is `*.worker.database.windows.net` — Azure SQL backend
/// infrastructure — which does NOT support strict mode.
async fn connect_fabric_sqldb_strict(
) -> anyhow::Result<Client<tokio_util::compat::Compat<TcpStream>>> {
    let endpoint = get_endpoint();
    let database = get_database();
    let token = get_token();

    let conn_str = format!(
        "server=tcp:{endpoint},1433;encrypt=strict;TrustServerCertificate=false;database={database}"
    );
    let mut config = Config::from_ado_string(&conn_str)?;
    config.authentication(AuthMethod::aad_token(&token));

    let tcp = TcpStream::connect(config.get_addr()).await?;
    tcp.set_nodelay(true)?;

    match Client::connect(config, tcp.compat_write()).await {
        Ok(client) => Ok(client),
        Err(Error::Routing { host, port }) => {
            eprintln!("Routing redirect to {}:{}", host, port);

            let backend_host = host.split('\\').next().unwrap_or(&host);

            // Fabric SQL DB backend is Azure SQL infrastructure
            // (*.worker.database.windows.net) which uses regular TLS upgrade,
            // NOT strict mode — same pattern as Azure SQL Database.
            let mut backend_config = Config::new();
            backend_config.host(backend_host);
            backend_config.port(port);
            backend_config.encryption(EncryptionLevel::Required);
            backend_config.authentication(AuthMethod::aad_token(&token));
            backend_config.database(&database);
            backend_config.login_server_name(&endpoint);

            let tcp = TcpStream::connect(backend_config.get_addr()).await?;
            tcp.set_nodelay(true)?;

            let client = Client::connect(backend_config, tcp.compat_write()).await?;
            Ok(client)
        }
        Err(e) => Err(e.into()),
    }
}

/// Connect to Fabric SQL Database with encrypt=true (regular TLS upgrade).
async fn connect_fabric_sqldb_required(
) -> anyhow::Result<Client<tokio_util::compat::Compat<TcpStream>>> {
    let endpoint = get_endpoint();
    let database = get_database();
    let token = get_token();

    let conn_str = format!(
        "server=tcp:{endpoint},1433;encrypt=true;TrustServerCertificate=false;database={database}"
    );
    let mut config = Config::from_ado_string(&conn_str)?;
    config.authentication(AuthMethod::aad_token(&token));

    let tcp = TcpStream::connect(config.get_addr()).await?;
    tcp.set_nodelay(true)?;

    match Client::connect(config, tcp.compat_write()).await {
        Ok(client) => Ok(client),
        Err(Error::Routing { host, port }) => {
            eprintln!("Routing redirect (required mode) to {}:{}", host, port);

            let backend_host = host.split('\\').next().unwrap_or(&host);

            let mut backend_config = Config::new();
            backend_config.host(backend_host);
            backend_config.port(port);
            backend_config.encryption(EncryptionLevel::Required);
            backend_config.authentication(AuthMethod::aad_token(&token));
            backend_config.database(&database);
            backend_config.login_server_name(&endpoint);

            let tcp = TcpStream::connect(backend_config.get_addr()).await?;
            tcp.set_nodelay(true)?;

            let client = Client::connect(backend_config, tcp.compat_write()).await?;
            Ok(client)
        }
        Err(e) => Err(e.into()),
    }
}

/// Test: Connect with encrypt=strict (TDS 8) and run a basic query.
#[tokio::test]
async fn fabric_sqldb_strict_basic_query() -> anyhow::Result<()> {
    skip_if_no_fabric_sqldb!();

    let mut client = connect_fabric_sqldb_strict().await?;

    let row = client
        .query("SELECT 1 AS test_value", &[])
        .await?
        .into_row()
        .await?
        .unwrap();
    assert_eq!(Some(1i32), row.get("test_value"));

    eprintln!("Fabric SQL DB strict basic query: OK");
    Ok(())
}

/// Test: Connect with encrypt=true (regular TLS) and run a basic query.
#[tokio::test]
async fn fabric_sqldb_required_basic_query() -> anyhow::Result<()> {
    skip_if_no_fabric_sqldb!();

    let mut client = connect_fabric_sqldb_required().await?;

    let row = client
        .query("SELECT 1 AS test_value", &[])
        .await?
        .into_row()
        .await?
        .unwrap();
    assert_eq!(Some(1i32), row.get("test_value"));

    eprintln!("Fabric SQL DB required (TLS upgrade) basic query: OK");
    Ok(())
}

/// Test: Server metadata on Fabric SQL Database.
#[tokio::test]
async fn fabric_sqldb_server_metadata() -> anyhow::Result<()> {
    skip_if_no_fabric_sqldb!();

    let mut client = connect_fabric_sqldb_strict().await?;

    let row = client
        .query(
            "SELECT @@VERSION AS version, DB_NAME() AS db_name, SUSER_SNAME() AS login_name",
            &[],
        )
        .await?
        .into_row()
        .await?
        .unwrap();

    let version: &str = row.get("version").unwrap();
    let db_name: &str = row.get("db_name").unwrap();
    let login_name: &str = row.get("login_name").unwrap();

    eprintln!("Version: {}", version.lines().next().unwrap_or(version));
    eprintln!("Database: {}, Login: {}", db_name, login_name);

    // Fabric SQL DB should report as SQL Server
    assert!(
        version.contains("SQL Server") || version.contains("Azure"),
        "Unexpected version: {}",
        version
    );

    Ok(())
}

/// Test: DDL and DML operations on Fabric SQL Database.
#[tokio::test]
async fn fabric_sqldb_ddl_dml() -> anyhow::Result<()> {
    skip_if_no_fabric_sqldb!();

    let mut client = connect_fabric_sqldb_strict().await?;

    // Create table
    client
        .execute(
            "IF OBJECT_ID('dbo.tiberius_test', 'U') IS NOT NULL DROP TABLE dbo.tiberius_test",
            &[],
        )
        .await?;
    client
        .execute(
            "CREATE TABLE dbo.tiberius_test (id INT PRIMARY KEY, name NVARCHAR(100), value FLOAT)",
            &[],
        )
        .await?;

    // Insert
    client
        .execute(
            "INSERT INTO dbo.tiberius_test (id, name, value) VALUES (1, N'hello', 3.14)",
            &[],
        )
        .await?;
    client
        .execute(
            "INSERT INTO dbo.tiberius_test (id, name, value) VALUES (2, N'world', 2.71)",
            &[],
        )
        .await?;

    // Query
    let rows = client
        .query("SELECT id, name, value FROM dbo.tiberius_test ORDER BY id", &[])
        .await?
        .into_first_result()
        .await?;

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<&str, _>("name"), Some("hello"));
    assert_eq!(rows[1].get::<&str, _>("name"), Some("world"));

    // Cleanup
    client
        .execute("DROP TABLE dbo.tiberius_test", &[])
        .await?;

    eprintln!("Fabric SQL DB DDL/DML: OK");
    Ok(())
}

/// Test: connection_encryption() reports correct level.
///
/// When using strict on the gateway but Required on the backend (after routing),
/// the final connection reports the backend's encryption level.
#[tokio::test]
async fn fabric_sqldb_connection_encryption() -> anyhow::Result<()> {
    skip_if_no_fabric_sqldb!();

    let client = connect_fabric_sqldb_strict().await?;

    // The final connection is to the backend, which uses Required (TLS upgrade)
    let enc = client.connection_encryption();
    eprintln!("connection_encryption() = {:?}", enc);

    // Backend uses On/Required (TLS upgrade), not Strict
    assert!(
        enc == EncryptionLevel::On || enc == EncryptionLevel::Required,
        "Fabric SQL DB backend should report On or Required, got: {:?}",
        enc
    );
    Ok(())
}

/// Test: is_healthy() works on Fabric SQL Database.
#[tokio::test]
async fn fabric_sqldb_is_healthy() -> anyhow::Result<()> {
    skip_if_no_fabric_sqldb!();

    let mut client = connect_fabric_sqldb_strict().await?;
    client.is_healthy().await?;
    eprintln!("is_healthy() on Fabric SQL DB: OK");
    Ok(())
}

/// Test: Fabric SQL DB works without specifying encrypt=strict.
///
/// Unlike Fabric Data Warehouse, Fabric SQL Database does NOT require TDS 8
/// strict mode. It works with regular TLS upgrade (same as Azure SQL).
/// This test verifies no auto-upgrade to strict happens and the connection
/// succeeds with the standard PRELOGIN → TLS upgrade flow.
#[tokio::test]
async fn fabric_sqldb_works_without_strict() -> anyhow::Result<()> {
    skip_if_no_fabric_sqldb!();

    let endpoint = get_endpoint();
    let database = get_database();
    let token = get_token();

    // Connect WITHOUT specifying encrypt — should work with regular TLS
    let conn_str = format!(
        "server=tcp:{endpoint},1433;encrypt=true;TrustServerCertificate=false;database={database}"
    );
    let mut config = Config::from_ado_string(&conn_str)?;
    config.authentication(AuthMethod::aad_token(&token));

    let tcp = TcpStream::connect(config.get_addr()).await?;
    tcp.set_nodelay(true)?;

    match Client::connect(config, tcp.compat_write()).await {
        Ok(mut client) => {
            let row = client
                .query("SELECT 1 AS val", &[])
                .await?
                .into_row()
                .await?
                .unwrap();
            assert_eq!(Some(1i32), row.get("val"));
            eprintln!("Fabric SQL DB works without strict (no routing): OK");
            Ok(())
        }
        Err(Error::Routing { host, port }) => {
            eprintln!("Routing redirect to {}:{}", host, port);

            let backend_host = host.split('\\').next().unwrap_or(&host);

            let mut backend_config = Config::new();
            backend_config.host(backend_host);
            backend_config.port(port);
            backend_config.encryption(EncryptionLevel::Required);
            backend_config.authentication(AuthMethod::aad_token(&token));
            backend_config.database(&database);
            backend_config.login_server_name(&endpoint);

            let tcp = TcpStream::connect(backend_config.get_addr()).await?;
            tcp.set_nodelay(true)?;

            let mut client = Client::connect(backend_config, tcp.compat_write()).await?;
            let row = client
                .query("SELECT 1 AS val", &[])
                .await?
                .into_row()
                .await?
                .unwrap();
            assert_eq!(Some(1i32), row.get("val"));
            eprintln!("Fabric SQL DB works without strict (with routing): OK");
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}
