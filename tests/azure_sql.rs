//! Integration tests for Azure SQL Database connectivity with TDS 8 strict encryption.
//!
//! These tests verify that TDS 8 strict encryption works correctly when
//! connecting through Azure SQL Database gateways with routing redirects.
//!
//! Azure SQL architecture:
//! 1. Gateway (port 1433): Accepts TDS 8 strict (TLS-first), returns routing redirect
//! 2. Backend worker (port 11010+): Requires regular TLS-upgrade (PRELOGIN → TLS → LOGIN)
//!
//! This differs from Microsoft Fabric where backends also support strict + pipelined.
//!
//! # Required environment variables
//!
//! - `AZURE_SQL_ENDPOINT`: The Azure SQL server (e.g., `myserver.database.windows.net`)
//! - `AZURE_SQL_DATABASE`: The database name
//!
//! Authentication:
//! - `AZURE_SQL_TOKEN`: A pre-obtained AAD/Entra ID token for `https://database.windows.net/`
//!
//! # Running
//!
//! ```sh
//! export AZURE_SQL_ENDPOINT=myserver.database.windows.net
//! export AZURE_SQL_DATABASE=mydb
//! export AZURE_SQL_TOKEN=$(az account get-access-token --resource https://database.windows.net/ --query accessToken -o tsv)
//! cargo test --test azure_sql -- --nocapture
//! ```

use std::env;
use tiberius::{error::Error, AuthMethod, Client, Config, EncryptionLevel};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

/// Helper to skip tests when required environment variables are missing.
macro_rules! skip_if_no_azure_sql {
    () => {
        if env::var("AZURE_SQL_ENDPOINT").is_err() {
            eprintln!(
                "SKIPPED: AZURE_SQL_ENDPOINT not set. Set Azure SQL env vars to run this test."
            );
            return Ok(());
        }
    };
}

/// Connect to Azure SQL Database using TDS 8 strict encryption on the gateway,
/// then regular TLS-upgrade on the backend after routing.
///
/// This matches the behavior of ODBC Driver 18+ and go-mssqldb:
/// - Gateway connection uses TDS 8 strict (TLS-first) to prove we can negotiate it
/// - Backend connection uses regular TLS-upgrade (which Azure SQL backends require)
async fn connect_to_azure_sql(
    endpoint: &str,
    database: &str,
    token: &str,
) -> anyhow::Result<Client<tokio_util::compat::Compat<TcpStream>>> {
    // Phase 1: Connect to gateway with TDS 8 strict encryption
    let conn_str = format!(
        "server=tcp:{endpoint},1433;encrypt=strict;TrustServerCertificate=true;database={database}"
    );
    let mut config = Config::from_ado_string(&conn_str)?;
    config.authentication(AuthMethod::aad_token(token));

    let tcp = TcpStream::connect(config.get_addr()).await?;
    tcp.set_nodelay(true)?;

    match Client::connect(config, tcp.compat_write()).await {
        Ok(client) => Ok(client),
        Err(Error::Routing { host, port }) => {
            eprintln!(
                "Routing redirect to {}:{}, reconnecting with TLS-upgrade...",
                host, port
            );

            // Azure SQL routing targets don't include instance names (unlike Fabric)
            let backend_host = host.split('\\').next().unwrap_or(&host);

            // Phase 2: Connect to backend with regular TLS-upgrade (NOT strict).
            // Azure SQL backend workers don't support TDS 8 strict mode.
            let mut backend_config = Config::new();
            backend_config.host(backend_host);
            backend_config.port(port);
            backend_config.encryption(EncryptionLevel::Required);
            backend_config.trust_cert();
            backend_config.authentication(AuthMethod::aad_token(token));
            backend_config.database(database);
            // MS-TDS spec: LOGIN server_name = original gateway endpoint
            backend_config.login_server_name(endpoint);

            let tcp = TcpStream::connect(backend_config.get_addr()).await?;
            tcp.set_nodelay(true)?;

            let client = Client::connect(backend_config, tcp.compat_write()).await?;
            Ok(client)
        }
        Err(e) => Err(e.into()),
    }
}

/// Connect to Azure SQL Database using regular (non-strict) encryption end-to-end.
/// This is the traditional flow matching ODBC Driver 17 behavior.
async fn connect_to_azure_sql_regular(
    endpoint: &str,
    database: &str,
    token: &str,
) -> anyhow::Result<Client<tokio_util::compat::Compat<TcpStream>>> {
    let conn_str = format!(
        "server=tcp:{endpoint},1433;encrypt=true;TrustServerCertificate=true;database={database}"
    );
    let mut config = Config::from_ado_string(&conn_str)?;
    config.authentication(AuthMethod::aad_token(token));

    let tcp = TcpStream::connect(config.get_addr()).await?;
    tcp.set_nodelay(true)?;

    match Client::connect(config, tcp.compat_write()).await {
        Ok(client) => Ok(client),
        Err(Error::Routing { host, port }) => {
            eprintln!("Routing redirect to {}:{}, reconnecting...", host, port);

            let backend_host = host.split('\\').next().unwrap_or(&host);

            let mut backend_config = Config::new();
            backend_config.host(backend_host);
            backend_config.port(port);
            backend_config.encryption(EncryptionLevel::Required);
            backend_config.trust_cert();
            backend_config.authentication(AuthMethod::aad_token(token));
            backend_config.database(database);
            backend_config.login_server_name(endpoint);

            let tcp = TcpStream::connect(backend_config.get_addr()).await?;
            tcp.set_nodelay(true)?;

            let client = Client::connect(backend_config, tcp.compat_write()).await?;
            Ok(client)
        }
        Err(e) => Err(e.into()),
    }
}

/// Test: Connect with TDS 8 strict gateway + regular backend and run a query.
#[tokio::test]
async fn azure_sql_strict_gateway_query() -> anyhow::Result<()> {
    skip_if_no_azure_sql!();

    let endpoint = env::var("AZURE_SQL_ENDPOINT")?;
    let database = env::var("AZURE_SQL_DATABASE")?;
    let token = env::var("AZURE_SQL_TOKEN")?;

    let mut client = connect_to_azure_sql(&endpoint, &database, &token).await?;

    let row = client
        .query("SELECT 1 AS test_value", &[])
        .await?
        .into_row()
        .await?
        .unwrap();

    assert_eq!(Some(1i32), row.get("test_value"));
    eprintln!("Azure SQL TDS 8 strict gateway + regular backend: OK");

    Ok(())
}

/// Test: Verify @@VERSION and DB_NAME() after strict gateway connection.
#[tokio::test]
async fn azure_sql_strict_server_metadata() -> anyhow::Result<()> {
    skip_if_no_azure_sql!();

    let endpoint = env::var("AZURE_SQL_ENDPOINT")?;
    let database = env::var("AZURE_SQL_DATABASE")?;
    let token = env::var("AZURE_SQL_TOKEN")?;

    let mut client = connect_to_azure_sql(&endpoint, &database, &token).await?;

    let row = client
        .query(
            "SELECT @@VERSION AS ver, DB_NAME() AS db_name, SUSER_SNAME() AS login_name",
            &[],
        )
        .await?
        .into_row()
        .await?
        .unwrap();

    let ver: &str = row.get("ver").unwrap();
    let db_name: &str = row.get("db_name").unwrap();
    let login_name: &str = row.get("login_name").unwrap();

    assert!(
        ver.contains("Microsoft SQL Azure"),
        "Should be Azure SQL, got: {}",
        ver
    );
    assert_eq!(
        db_name, database,
        "Should connect to the requested database"
    );
    eprintln!("Version: {}", &ver[..ver.find('\n').unwrap_or(ver.len())]);
    eprintln!("Database: {}", db_name);
    eprintln!("Login: {}", login_name);

    Ok(())
}

/// Test: Multiple sequential queries over the connection.
#[tokio::test]
async fn azure_sql_strict_multiple_queries() -> anyhow::Result<()> {
    skip_if_no_azure_sql!();

    let endpoint = env::var("AZURE_SQL_ENDPOINT")?;
    let database = env::var("AZURE_SQL_DATABASE")?;
    let token = env::var("AZURE_SQL_TOKEN")?;

    let mut client = connect_to_azure_sql(&endpoint, &database, &token).await?;

    // Query 1
    let row = client
        .query("SELECT 42 AS answer", &[])
        .await?
        .into_row()
        .await?
        .unwrap();
    assert_eq!(Some(42i32), row.get("answer"));

    // Query 2: server time
    let row = client
        .query(
            "SELECT CAST(GETUTCDATE() AS NVARCHAR(50)) AS server_time",
            &[],
        )
        .await?
        .into_row()
        .await?
        .unwrap();
    let time: &str = row.get("server_time").unwrap();
    assert!(!time.is_empty(), "Should get server time");

    // Query 3: parameterized
    let row = client
        .query("SELECT @P1 + @P2 AS sum_result", &[&10i32, &32i32])
        .await?
        .into_row()
        .await?
        .unwrap();
    assert_eq!(Some(42i32), row.get("sum_result"));

    eprintln!("Multiple queries over strict gateway connection: OK");
    Ok(())
}

/// Test: Regular (non-strict) encryption also works for comparison.
#[tokio::test]
async fn azure_sql_regular_encryption() -> anyhow::Result<()> {
    skip_if_no_azure_sql!();

    let endpoint = env::var("AZURE_SQL_ENDPOINT")?;
    let database = env::var("AZURE_SQL_DATABASE")?;
    let token = env::var("AZURE_SQL_TOKEN")?;

    let mut client = connect_to_azure_sql_regular(&endpoint, &database, &token).await?;

    let row = client
        .query("SELECT DB_NAME() AS db_name", &[])
        .await?
        .into_row()
        .await?
        .unwrap();

    let db_name: &str = row.get("db_name").unwrap();
    assert_eq!(db_name, database);
    eprintln!("Regular encryption (non-strict) also works: OK");

    Ok(())
}

/// Test: DDL and DML operations work over strict gateway connection.
#[tokio::test]
async fn azure_sql_strict_ddl_dml() -> anyhow::Result<()> {
    skip_if_no_azure_sql!();

    let endpoint = env::var("AZURE_SQL_ENDPOINT")?;
    let database = env::var("AZURE_SQL_DATABASE")?;
    let token = env::var("AZURE_SQL_TOKEN")?;

    let mut client = connect_to_azure_sql(&endpoint, &database, &token).await?;

    // Create a temp table (must consume result before next command)
    client
        .simple_query("CREATE TABLE #tds8_test (id INT, name NVARCHAR(50), value DECIMAL(10,2))")
        .await?
        .into_results()
        .await?;

    // Insert data
    let rows_affected = client
        .execute(
            "INSERT INTO #tds8_test VALUES (@P1, @P2, @P3)",
            &[&1i32, &"hello", &42.5f64],
        )
        .await?
        .total();
    assert_eq!(rows_affected, 1);

    let rows_affected = client
        .execute(
            "INSERT INTO #tds8_test VALUES (@P1, @P2, @P3)",
            &[&2i32, &"world", &99.9f64],
        )
        .await?
        .total();
    assert_eq!(rows_affected, 1);

    // Query data back
    let rows: Vec<_> = client
        .query("SELECT id, name, value FROM #tds8_test ORDER BY id", &[])
        .await?
        .into_first_result()
        .await?;

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<i32, _>("id"), Some(1));
    assert_eq!(rows[0].get::<&str, _>("name"), Some("hello"));
    assert_eq!(rows[1].get::<i32, _>("id"), Some(2));
    assert_eq!(rows[1].get::<&str, _>("name"), Some("world"));

    eprintln!("DDL/DML over strict gateway connection: OK");
    Ok(())
}

/// Test: Large result set over Azure SQL strict connection to stress TLS framing.
#[tokio::test]
async fn azure_sql_strict_large_result() -> anyhow::Result<()> {
    skip_if_no_azure_sql!();

    let endpoint = env::var("AZURE_SQL_ENDPOINT")?;
    let database = env::var("AZURE_SQL_DATABASE")?;
    let token = env::var("AZURE_SQL_TOKEN")?;

    let mut client = connect_to_azure_sql(&endpoint, &database, &token).await?;

    // Generate a large result set to exercise TLS framing across multiple packets
    let rows: Vec<_> = client
        .query(
            "SELECT TOP 1000 \
                ROW_NUMBER() OVER (ORDER BY a.object_id) AS row_num, \
                REPLICATE(N'X', 200) AS padding \
             FROM sys.all_objects a CROSS JOIN sys.all_objects b",
            &[],
        )
        .await?
        .into_first_result()
        .await?;

    assert_eq!(rows.len(), 1000, "Should get exactly 1000 rows");

    // Verify first and last row
    assert_eq!(rows[0].get::<i64, _>("row_num"), Some(1));
    assert_eq!(rows[999].get::<i64, _>("row_num"), Some(1000));

    let padding: &str = rows[0].get("padding").unwrap();
    assert_eq!(padding.len(), 200, "Padding should be 200 chars");

    eprintln!("Large result set (1000 rows) over Azure SQL strict connection: OK");
    Ok(())
}

/// Test: Verify encryption status via sys.dm_exec_connections on Azure SQL.
#[tokio::test]
async fn azure_sql_strict_verify_encryption() -> anyhow::Result<()> {
    skip_if_no_azure_sql!();

    let endpoint = env::var("AZURE_SQL_ENDPOINT")?;
    let database = env::var("AZURE_SQL_DATABASE")?;
    let token = env::var("AZURE_SQL_TOKEN")?;

    let mut client = connect_to_azure_sql(&endpoint, &database, &token).await?;

    let row = client
        .query(
            "SELECT encrypt_option, auth_scheme, protocol_type, net_transport \
             FROM sys.dm_exec_connections WHERE session_id = @@SPID",
            &[],
        )
        .await?
        .into_row()
        .await?
        .unwrap();

    let encrypt_option: &str = row.get("encrypt_option").unwrap();
    let auth_scheme: &str = row.get("auth_scheme").unwrap();
    let net_transport: &str = row.get("net_transport").unwrap();

    assert!(
        encrypt_option == "TRUE" || encrypt_option == "STRICT",
        "Expected encrypted connection, got encrypt_option='{}'",
        encrypt_option
    );
    assert_eq!(net_transport, "TCP");
    // Azure SQL with AAD token uses NTML at transport but AAD at auth layer
    assert!(!auth_scheme.is_empty(), "Should have an auth scheme");

    eprintln!(
        "Azure SQL encryption verified: option={}, scheme={}, transport={}",
        encrypt_option, auth_scheme, net_transport
    );
    Ok(())
}

/// Test: String and unicode operations over Azure SQL strict connection.
#[tokio::test]
async fn azure_sql_strict_string_operations() -> anyhow::Result<()> {
    skip_if_no_azure_sql!();

    let endpoint = env::var("AZURE_SQL_ENDPOINT")?;
    let database = env::var("AZURE_SQL_DATABASE")?;
    let token = env::var("AZURE_SQL_TOKEN")?;

    let mut client = connect_to_azure_sql(&endpoint, &database, &token).await?;

    // Test unicode handling over TDS 8 strict TLS
    let row = client
        .query(
            "SELECT CONCAT(@P1, N' ', @P2) AS greeting",
            &[&"Hello", &"TDS8"],
        )
        .await?
        .into_row()
        .await?
        .unwrap();
    assert_eq!(Some("Hello TDS8"), row.get::<&str, _>("greeting"));

    // Unicode characters
    let row = client
        .query("SELECT @P1 AS unicode_text", &[&"日本語テスト 🚀"])
        .await?
        .into_row()
        .await?
        .unwrap();
    assert_eq!(Some("日本語テスト 🚀"), row.get::<&str, _>("unicode_text"));

    // Long string that spans multiple TDS packets
    let long_string = "A".repeat(8000);
    let row = client
        .query("SELECT @P1 AS long_text", &[&long_string.as_str()])
        .await?
        .into_row()
        .await?
        .unwrap();
    let result: &str = row.get("long_text").unwrap();
    assert_eq!(result.len(), 8000);

    eprintln!("String/unicode operations over Azure SQL strict connection: OK");
    Ok(())
}
