//! Integration tests for SQL Server 2025+ TDS 8 strict encryption.
//!
//! These tests verify that TDS 8 strict encryption works correctly when
//! connecting directly to a SQL Server instance with `forcestrict = 1`.
//!
//! Unlike Azure SQL/Fabric, SQL Server with strict mode does NOT use routing
//! redirects — the connection goes directly to the server.
//!
//! # Required environment variables
//!
//! - `SQL_SERVER_HOST`: The server hostname (default: `localhost`)
//! - `SQL_SERVER_PORT`: The server port (default: `1434`)
//! - `SQL_SERVER_USER`: SQL login (default: `sa`)
//! - `SQL_SERVER_PASSWORD`: SQL password
//! - `SQL_SERVER_CA_CERT`: Path to CA cert for TLS verification (optional; uses trust-all if unset)
//!
//! # Running
//!
//! ```sh
//! # Start SQL Server 2025 in Docker with strict mode:
//! # docker run -d --name mssql-tds8 -p 1434:1433 \
//! #   -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD=StrictMode!2022 \
//! #   -v ./certs/mssql-cert.pem:/var/opt/mssql/certs/mssql-cert.pem:ro \
//! #   -v ./certs/mssql-key.pem:/var/opt/mssql/certs/mssql-key.pem:ro \
//! #   -v ./mssql.conf:/var/opt/mssql/mssql.conf \
//! #   mcr.microsoft.com/mssql/server:2025-latest
//! #
//! # mssql.conf should contain:
//! # [network]
//! # tlscert = /var/opt/mssql/certs/mssql-cert.pem
//! # tlskey = /var/opt/mssql/certs/mssql-key.pem
//! # tlsprotocols = 1.2
//! # forceencryption = 1
//! # forcestrict = 1
//!
//! export SQL_SERVER_PASSWORD=StrictMode!2022
//! cargo test --test sql_server_tds8 -- --nocapture
//! ```

use std::env;
use tiberius::{AuthMethod, Client, Config, EncryptionLevel};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

/// Helper to skip tests when required environment variables are missing.
macro_rules! skip_if_no_sql_server {
    () => {
        if env::var("SQL_SERVER_PASSWORD").is_err() {
            eprintln!(
                "SKIPPED: SQL_SERVER_PASSWORD not set. Set SQL Server env vars to run this test."
            );
            return Ok(());
        }
    };
}

/// Connect to SQL Server with TDS 8 strict encryption (TLS-first, no routing).
async fn connect_strict() -> anyhow::Result<Client<tokio_util::compat::Compat<TcpStream>>> {
    let host = env::var("SQL_SERVER_HOST").unwrap_or_else(|_| "localhost".to_string());
    let port = env::var("SQL_SERVER_PORT").unwrap_or_else(|_| "1434".to_string());
    let user = env::var("SQL_SERVER_USER").unwrap_or_else(|_| "sa".to_string());
    let password = env::var("SQL_SERVER_PASSWORD")?;

    let conn_str = if let Ok(ca_path) = env::var("SQL_SERVER_CA_CERT") {
        format!("server=tcp:{host},{port};encrypt=strict;database=master;Certificate={ca_path}")
    } else {
        format!(
            "server=tcp:{host},{port};encrypt=strict;TrustServerCertificate=true;database=master"
        )
    };

    let mut config = Config::from_ado_string(&conn_str)?;
    config.authentication(AuthMethod::sql_server(&user, &password));

    let tcp = TcpStream::connect(config.get_addr()).await?;
    tcp.set_nodelay(true)?;

    let client = Client::connect(config, tcp.compat_write()).await?;
    Ok(client)
}

/// Test: Basic strict mode connection and simple query.
#[tokio::test]
async fn sql_server_strict_basic_query() -> anyhow::Result<()> {
    skip_if_no_sql_server!();

    let mut client = connect_strict().await?;

    let row = client
        .query("SELECT 1 AS test_value", &[])
        .await?
        .into_row()
        .await?
        .unwrap();

    assert_eq!(Some(1i32), row.get("test_value"));
    eprintln!("SQL Server TDS 8 strict basic query: OK");

    Ok(())
}

/// Test: Verify server version and metadata via strict connection.
#[tokio::test]
async fn sql_server_strict_server_metadata() -> anyhow::Result<()> {
    skip_if_no_sql_server!();

    let mut client = connect_strict().await?;

    let row = client
        .query(
            "SELECT @@VERSION AS ver, DB_NAME() AS db_name, SUSER_SNAME() AS login_name, \
             CAST(CONNECTIONPROPERTY('net_transport') AS NVARCHAR(50)) AS transport, \
             CAST(CONNECTIONPROPERTY('protocol_type') AS NVARCHAR(50)) AS protocol",
            &[],
        )
        .await?
        .into_row()
        .await?
        .unwrap();

    let ver: &str = row.get("ver").unwrap();
    let db_name: &str = row.get("db_name").unwrap();
    let login_name: &str = row.get("login_name").unwrap();
    let transport: &str = row.get("transport").unwrap();

    assert!(
        ver.contains("Microsoft SQL Server 2025") || ver.contains("Microsoft SQL Server 2022"),
        "Expected SQL Server 2022+, got: {}",
        &ver[..ver.find('\n').unwrap_or(80.min(ver.len()))]
    );
    assert_eq!(db_name, "master");
    assert_eq!(transport, "TCP");
    eprintln!("Version: {}", &ver[..ver.find('\n').unwrap_or(ver.len())]);
    eprintln!("Database: {}, Login: {}", db_name, login_name);

    Ok(())
}

/// Test: Multiple sequential queries over strict connection.
#[tokio::test]
async fn sql_server_strict_multiple_queries() -> anyhow::Result<()> {
    skip_if_no_sql_server!();

    let mut client = connect_strict().await?;

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

    // Query 4: string operations
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

    eprintln!("Multiple queries over strict connection: OK");
    Ok(())
}

/// Test: DDL and DML operations over strict connection.
#[tokio::test]
async fn sql_server_strict_ddl_dml() -> anyhow::Result<()> {
    skip_if_no_sql_server!();

    let mut client = connect_strict().await?;

    // Create temp table
    client
        .simple_query(
            "CREATE TABLE #tds8_strict_test (id INT, name NVARCHAR(100), value DECIMAL(10,2))",
        )
        .await?
        .into_results()
        .await?;

    // Insert data using parameters
    let rows_affected = client
        .execute(
            "INSERT INTO #tds8_strict_test VALUES (@P1, @P2, @P3)",
            &[&1i32, &"strict_mode", &123.45f64],
        )
        .await?
        .total();
    assert_eq!(rows_affected, 1);

    let rows_affected = client
        .execute(
            "INSERT INTO #tds8_strict_test VALUES (@P1, @P2, @P3)",
            &[&2i32, &"tds_eight", &678.90f64],
        )
        .await?
        .total();
    assert_eq!(rows_affected, 1);

    // Batch insert
    client
        .simple_query(
            "INSERT INTO #tds8_strict_test VALUES (3, N'batch_one', 11.11), (4, N'batch_two', 22.22)",
        )
        .await?
        .into_results()
        .await?;

    // Query data back
    let rows: Vec<_> = client
        .query(
            "SELECT id, name, value FROM #tds8_strict_test ORDER BY id",
            &[],
        )
        .await?
        .into_first_result()
        .await?;

    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0].get::<i32, _>("id"), Some(1));
    assert_eq!(rows[0].get::<&str, _>("name"), Some("strict_mode"));
    assert_eq!(rows[3].get::<i32, _>("id"), Some(4));
    assert_eq!(rows[3].get::<&str, _>("name"), Some("batch_two"));

    // Update
    let rows_affected = client
        .execute(
            "UPDATE #tds8_strict_test SET value = @P1 WHERE id = @P2",
            &[&999.99f64, &1i32],
        )
        .await?
        .total();
    assert_eq!(rows_affected, 1);

    // Delete
    let rows_affected = client
        .execute("DELETE FROM #tds8_strict_test WHERE id > @P1", &[&2i32])
        .await?
        .total();
    assert_eq!(rows_affected, 2);

    // Verify final state
    let rows: Vec<_> = client
        .query("SELECT id, value FROM #tds8_strict_test ORDER BY id", &[])
        .await?
        .into_first_result()
        .await?;
    assert_eq!(rows.len(), 2);

    eprintln!("DDL/DML over strict connection: OK");
    Ok(())
}

/// Test: Verify encryption is active via sys.dm_exec_connections.
#[tokio::test]
async fn sql_server_strict_verify_encryption() -> anyhow::Result<()> {
    skip_if_no_sql_server!();

    let mut client = connect_strict().await?;

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
    let protocol_type: &str = row.get("protocol_type").unwrap();
    let net_transport: &str = row.get("net_transport").unwrap();

    // In strict mode, encrypt_option should be TRUE (or STRICT on newer versions)
    assert!(
        encrypt_option == "TRUE" || encrypt_option == "STRICT",
        "Expected encrypted connection, got encrypt_option='{}'",
        encrypt_option
    );
    assert_eq!(auth_scheme, "SQL");
    assert_eq!(protocol_type, "TSQL");
    assert_eq!(net_transport, "TCP");

    eprintln!(
        "Encryption verified: option={}, scheme={}, transport={}",
        encrypt_option, auth_scheme, net_transport
    );
    Ok(())
}

/// Test: Large result set over strict connection (verifies TLS framing is stable).
#[tokio::test]
async fn sql_server_strict_large_result() -> anyhow::Result<()> {
    skip_if_no_sql_server!();

    let mut client = connect_strict().await?;

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

    eprintln!("Large result set (1000 rows) over strict connection: OK");
    Ok(())
}

/// Test: Connection with CA certificate verification (not just trust-all).
#[tokio::test]
async fn sql_server_strict_ca_cert_validation() -> anyhow::Result<()> {
    skip_if_no_sql_server!();

    // This test only runs if a CA cert path is provided
    let ca_cert = match env::var("SQL_SERVER_CA_CERT") {
        Ok(path) => path,
        Err(_) => {
            eprintln!("SKIPPED: SQL_SERVER_CA_CERT not set. Set it to test CA cert validation.");
            return Ok(());
        }
    };

    let host = env::var("SQL_SERVER_HOST").unwrap_or_else(|_| "localhost".to_string());
    let port = env::var("SQL_SERVER_PORT").unwrap_or_else(|_| "1434".to_string());
    let user = env::var("SQL_SERVER_USER").unwrap_or_else(|_| "sa".to_string());
    let password = env::var("SQL_SERVER_PASSWORD")?;

    let conn_str =
        format!("server=tcp:{host},{port};encrypt=strict;database=master;Certificate={ca_cert}");

    let mut config = Config::from_ado_string(&conn_str)?;
    config.authentication(AuthMethod::sql_server(&user, &password));

    let tcp = TcpStream::connect(config.get_addr()).await?;
    tcp.set_nodelay(true)?;

    let mut client = Client::connect(config, tcp.compat_write()).await?;

    let row = client
        .query("SELECT 1 AS test", &[])
        .await?
        .into_row()
        .await?
        .unwrap();
    assert_eq!(Some(1i32), row.get("test"));

    eprintln!("CA cert validation with strict mode: OK");
    Ok(())
}

/// Test: Verify improved error message when strict TLS handshake fails.
///
/// This spins up a local TCP listener that immediately closes the connection,
/// simulating a server that doesn't support TDS 8 strict mode. The error
/// message should contain actionable guidance.
#[tokio::test]
async fn strict_error_message_on_non_strict_server() -> anyhow::Result<()> {
    use tokio::net::TcpListener;

    // Start a local TCP listener that immediately drops incoming connections
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();

    tokio::spawn(async move {
        // Accept one connection and immediately drop it (simulates non-TLS server)
        if let Ok((conn, _)) = listener.accept().await {
            drop(conn);
        }
    });

    let mut config = Config::new();
    config.host("my-server.example.com");
    config.port(port);
    config.encryption(tiberius::EncryptionLevel::Strict);
    config.authentication(AuthMethod::sql_server("sa", "dummy"));
    config.trust_cert();

    let tcp = TcpStream::connect(format!("127.0.0.1:{}", port)).await?;
    tcp.set_nodelay(true)?;

    let result = Client::connect(config, tcp.compat_write()).await;
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();

    // The error should mention strict mode and provide guidance
    assert!(
        err_msg.contains("strict") || err_msg.contains("TDS 8"),
        "Error message should mention strict mode, got: {}",
        err_msg
    );
    assert!(
        err_msg.contains("my-server.example.com"),
        "Error message should contain the hostname, got: {}",
        err_msg
    );
    assert!(
        err_msg.contains("encrypt=true"),
        "Error message should suggest alternative, got: {}",
        err_msg
    );

    eprintln!("Strict TLS error message: {}", err_msg);
    Ok(())
}

/// Test: connection_encryption() returns Strict for TDS 8 strict connections.
#[tokio::test]
async fn connection_encryption_reports_strict() -> anyhow::Result<()> {
    skip_if_no_sql_server!();

    let client = connect_strict().await?;

    assert_eq!(
        client.connection_encryption(),
        EncryptionLevel::Strict,
        "SQL Server with forcestrict=1 should report Strict encryption"
    );

    eprintln!("connection_encryption() = {:?}", client.connection_encryption());
    Ok(())
}

/// Test: is_healthy() succeeds on a live strict connection.
#[tokio::test]
async fn is_healthy_on_strict_connection() -> anyhow::Result<()> {
    skip_if_no_sql_server!();

    let mut client = connect_strict().await?;

    // First health check
    client.is_healthy().await?;

    // Run a real query in between
    let _ = client
        .query("SELECT @@VERSION", &[])
        .await?
        .into_row()
        .await?;

    // Second health check — still healthy after use
    client.is_healthy().await?;

    eprintln!("is_healthy() passed on strict connection");
    Ok(())
}
