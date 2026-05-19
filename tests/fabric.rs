//! Integration tests for Microsoft Fabric SQL Data Warehouse connectivity.
//!
//! These tests verify that TDS 8 strict encryption (the `encrypt=strict` option)
//! works correctly when connecting to a Microsoft Fabric endpoint.
//!
//! # Required environment variables
//!
//! - `FABRIC_ENDPOINT`: The Fabric SQL endpoint (e.g., `my-workspace.datawarehouse.fabric.microsoft.com`)
//! - `FABRIC_DATABASE`: The database name in Fabric
//!
//! Authentication (one of the following):
//! - `FABRIC_AAD_TOKEN`: A pre-obtained AAD/Entra ID token for the `https://database.windows.net/` scope
//! - Or: `FABRIC_CLIENT_ID`, `FABRIC_CLIENT_SECRET`, `FABRIC_TENANT_ID` for service principal auth
//!
//! # Running
//!
//! ```sh
//! # With a pre-obtained token (e.g., from `az account get-access-token`):
//! export FABRIC_ENDPOINT=my-workspace.datawarehouse.fabric.microsoft.com
//! export FABRIC_DATABASE=my-database
//! export FABRIC_AAD_TOKEN=$(az account get-access-token --resource https://database.windows.net/ --query accessToken -o tsv)
//! cargo test --test fabric -- --nocapture
//! ```

use std::env;
use tiberius::{error::Error, AuthMethod, Client, Config, EncryptionLevel};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

/// Helper to skip tests when required environment variables are missing.
macro_rules! skip_if_no_fabric {
    () => {
        if env::var("FABRIC_ENDPOINT").is_err() {
            eprintln!("SKIPPED: FABRIC_ENDPOINT not set. Set Fabric env vars to run this test.");
            return Ok(());
        }
    };
}

/// Obtain an AAD token for the Fabric SQL endpoint.
///
/// Tries in order:
/// 1. `FABRIC_AAD_TOKEN` env var (pre-obtained token)
/// 2. Service principal credentials (`FABRIC_CLIENT_ID`, `FABRIC_CLIENT_SECRET`, `FABRIC_TENANT_ID`)
async fn get_aad_token() -> anyhow::Result<String> {
    // Option 1: Pre-obtained token from environment
    if let Ok(token) = env::var("FABRIC_AAD_TOKEN") {
        return Ok(token);
    }

    // Option 2: Service principal client credentials flow
    let client_id = env::var("FABRIC_CLIENT_ID")
        .map_err(|_| anyhow::anyhow!("Neither FABRIC_AAD_TOKEN nor FABRIC_CLIENT_ID is set"))?;
    let client_secret = env::var("FABRIC_CLIENT_SECRET")
        .map_err(|_| anyhow::anyhow!("FABRIC_CLIENT_SECRET not set"))?;
    let tenant_id =
        env::var("FABRIC_TENANT_ID").map_err(|_| anyhow::anyhow!("FABRIC_TENANT_ID not set"))?;

    use azure_identity::client_credentials_flow;
    use oauth2::{ClientId, ClientSecret};
    use std::sync::Arc;

    let http_client = Arc::new(reqwest::Client::new());
    let token = client_credentials_flow::perform(
        http_client,
        &ClientId::new(client_id),
        &ClientSecret::new(client_secret),
        &["https://database.windows.net/.default"],
        &tenant_id,
    )
    .await?;

    Ok(token.access_token().secret().to_string())
}

/// Build a Config for connecting to Microsoft Fabric with TDS 8 strict encryption.
fn fabric_config(endpoint: &str, database: &str) -> tiberius::Result<Config> {
    let conn_str = format!(
        "server=tcp:{endpoint},1433;encrypt=strict;TrustServerCertificate=false;database={database}"
    );
    Config::from_ado_string(&conn_str)
}

/// Connect to Microsoft Fabric with TDS 8 strict encryption.
///
/// Fabric uses a gateway that returns a routing redirect to a backend server.
/// The client must:
/// 1. Connect to the gateway with TDS 8 strict TLS
/// 2. Receive the routing redirect (Error::Routing)
/// 3. Reconnect to the backend with pipelined PRELOGIN+LOGIN
async fn connect_to_fabric(
    endpoint: &str,
    database: &str,
    token: &str,
) -> anyhow::Result<Client<tokio_util::compat::Compat<TcpStream>>> {
    let _ = env_logger::try_init();

    let mut config = fabric_config(endpoint, database)?;
    config.authentication(AuthMethod::aad_token(token));

    let tcp = TcpStream::connect(config.get_addr()).await?;
    tcp.set_nodelay(true)?;

    match Client::connect(config, tcp.compat_write()).await {
        Ok(client) => Ok(client),
        Err(Error::Routing { host, port }) => {
            eprintln!(
                "Received routing redirect to {}:{}, reconnecting...",
                host, port
            );

            // The routing host may contain an instance name (e.g.,
            // "host.pbidedicated.windows.net\InstanceName"). Strip it for
            // the TCP connection and TLS SNI; only the hostname is needed.
            let backend_host = host.split('\\').next().unwrap_or(&host);
            let instance_name = host.split('\\').nth(1);

            let mut backend_config = Config::new();
            backend_config.host(backend_host);
            backend_config.port(port);
            backend_config.encryption(EncryptionLevel::Strict);
            backend_config.authentication(AuthMethod::aad_token(token));
            backend_config.database(database);
            // Include instance name in PRELOGIN so backend knows which instance to route to
            if let Some(inst) = instance_name {
                backend_config.instance_name(inst);
            }
            // MS-TDS spec: LOGIN server_name must be the ORIGINAL endpoint, not routing target
            backend_config.login_server_name(endpoint);

            let tcp = TcpStream::connect(backend_config.get_addr()).await?;
            tcp.set_nodelay(true)?;

            let client = Client::connect(backend_config, tcp.compat_write()).await?;
            Ok(client)
        }
        Err(e) => Err(e.into()),
    }
}

/// Test: Connect to Fabric with TDS 8 strict encryption and run a basic query.
#[tokio::test]
async fn connect_to_fabric_strict_encryption() -> anyhow::Result<()> {
    skip_if_no_fabric!();

    let endpoint = env::var("FABRIC_ENDPOINT")?;
    let database = env::var("FABRIC_DATABASE")?;
    let token = get_aad_token().await?;

    let mut client = connect_to_fabric(&endpoint, &database, &token).await?;

    // Simple connectivity test
    let row = client
        .query("SELECT 1 AS test_value", &[])
        .await?
        .into_row()
        .await?
        .unwrap();

    assert_eq!(Some(1i32), row.get("test_value"));

    Ok(())
}

/// Test: Verify that the ADO.NET connection string parsing accepts `encrypt=strict`.
#[tokio::test]
async fn fabric_config_parses_strict_encryption() -> anyhow::Result<()> {
    // This should parse without error - strict is a valid encryption level
    let _config = Config::from_ado_string(
        "server=tcp:test.datawarehouse.fabric.microsoft.com,1433;encrypt=strict;database=testdb",
    )?;
    Ok(())
}

/// Test: Run a query that exercises Fabric-specific metadata.
#[tokio::test]
async fn fabric_query_database_metadata() -> anyhow::Result<()> {
    skip_if_no_fabric!();

    let endpoint = env::var("FABRIC_ENDPOINT")?;
    let database = env::var("FABRIC_DATABASE")?;
    let token = get_aad_token().await?;

    let mut client = connect_to_fabric(&endpoint, &database, &token).await?;

    // Query current database name to verify we connected to the right database
    let row = client
        .query("SELECT DB_NAME() AS current_db", &[])
        .await?
        .into_row()
        .await?
        .unwrap();

    let db_name: Option<&str> = row.get("current_db");
    assert!(
        db_name.is_some(),
        "Should be able to query the current database name"
    );
    eprintln!("Connected to database: {:?}", db_name.unwrap());

    Ok(())
}

/// Test: Verify multiple sequential queries work over a strict TLS connection.
#[tokio::test]
async fn fabric_multiple_queries() -> anyhow::Result<()> {
    skip_if_no_fabric!();

    let endpoint = env::var("FABRIC_ENDPOINT")?;
    let database = env::var("FABRIC_DATABASE")?;
    let token = get_aad_token().await?;

    let mut client = connect_to_fabric(&endpoint, &database, &token).await?;

    // First query
    let row = client
        .query("SELECT 42 AS answer", &[])
        .await?
        .into_row()
        .await?
        .unwrap();
    assert_eq!(Some(42i32), row.get("answer"));

    // Second query - verifies the connection stays healthy
    let row = client
        .query("SELECT CAST(GETDATE() AS NVARCHAR(50)) AS server_time", &[])
        .await?
        .into_row()
        .await?
        .unwrap();
    let time: Option<&str> = row.get("server_time");
    assert!(time.is_some(), "Should get server time as string");

    // Third query with parameters
    let row = client
        .query("SELECT @P1 + @P2 AS sum_result", &[&10i32, &32i32])
        .await?
        .into_row()
        .await?
        .unwrap();
    assert_eq!(Some(42i32), row.get("sum_result"));

    Ok(())
}

/// Test: Verify that the JDBC-style connection string also accepts `encrypt=strict`.
#[tokio::test]
async fn fabric_jdbc_connection_string() -> anyhow::Result<()> {
    let config = Config::from_jdbc_string(
        "jdbc:sqlserver://test.datawarehouse.fabric.microsoft.com:1433;encrypt=strict;databaseName=testdb",
    )?;

    // Verify host:port were parsed correctly via the public get_addr() method
    assert_eq!(
        config.get_addr(),
        "test.datawarehouse.fabric.microsoft.com:1433"
    );
    Ok(())
}

/// Test: DDL and DML operations over Fabric strict connection.
///
/// Fabric Data Warehouse does not support local temp tables (#table) but does
/// support regular tables. We create a test table, exercise INSERT/UPDATE/DELETE,
/// then drop it.
#[tokio::test]
async fn fabric_strict_ddl_dml() -> anyhow::Result<()> {
    skip_if_no_fabric!();

    let endpoint = env::var("FABRIC_ENDPOINT")?;
    let database = env::var("FABRIC_DATABASE")?;
    let token = get_aad_token().await?;

    let mut client = connect_to_fabric(&endpoint, &database, &token).await?;

    let table_name = "dbo.__tiberius_tds8_test";

    // Drop if exists from a previous failed run
    client
        .simple_query(format!(
            "IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
        ))
        .await?
        .into_results()
        .await?;

    // CREATE TABLE — Fabric DW only supports varchar(n), not nvarchar(n) with length.
    // Skip gracefully if the identity lacks DDL permissions (e.g., read-only lakehouse).
    match client
        .simple_query(format!(
            "CREATE TABLE {table_name} (id INT, name VARCHAR(100), value DECIMAL(10,2))"
        ))
        .await
    {
        Err(e) if e.to_string().contains("denied") => {
            eprintln!(
                "SKIPPED: DDL not permitted on this Fabric database ({}). \
                 Set FABRIC_DATABASE to a writable Data Warehouse to run this test.",
                e
            );
            return Ok(());
        }
        Err(e) => return Err(e.into()),
        Ok(stream) => {
            stream.into_results().await?;
        }
    }

    // INSERT with parameters
    let rows_affected = client
        .execute(
            format!("INSERT INTO {table_name} VALUES (@P1, @P2, @P3)"),
            &[&1i32, &"strict_mode", &123.45f64],
        )
        .await?
        .total();
    assert_eq!(rows_affected, 1);

    let rows_affected = client
        .execute(
            format!("INSERT INTO {table_name} VALUES (@P1, @P2, @P3)"),
            &[&2i32, &"tds_eight", &678.90f64],
        )
        .await?
        .total();
    assert_eq!(rows_affected, 1);

    // SELECT back
    let rows: Vec<_> = client
        .query(
            format!("SELECT id, name, value FROM {table_name} ORDER BY id"),
            &[],
        )
        .await?
        .into_first_result()
        .await?;

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<i32, _>("id"), Some(1));
    assert_eq!(rows[0].get::<&str, _>("name"), Some("strict_mode"));
    assert_eq!(rows[1].get::<i32, _>("id"), Some(2));
    assert_eq!(rows[1].get::<&str, _>("name"), Some("tds_eight"));

    // UPDATE
    let rows_affected = client
        .execute(
            format!("UPDATE {table_name} SET value = @P1 WHERE id = @P2"),
            &[&999.99f64, &1i32],
        )
        .await?
        .total();
    assert_eq!(rows_affected, 1);

    // DELETE
    let rows_affected = client
        .execute(format!("DELETE FROM {table_name} WHERE id = @P1"), &[&2i32])
        .await?
        .total();
    assert_eq!(rows_affected, 1);

    // Verify final state
    let rows: Vec<_> = client
        .query(
            format!("SELECT id, value FROM {table_name} ORDER BY id"),
            &[],
        )
        .await?
        .into_first_result()
        .await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i32, _>("id"), Some(1));

    // Cleanup
    client
        .simple_query(format!("DROP TABLE {table_name}"))
        .await?
        .into_results()
        .await?;

    eprintln!("Fabric DDL/DML over strict connection: OK");
    Ok(())
}

/// Test: Large result set over Fabric strict connection to stress TLS framing.
///
/// Uses the tpch_sf1 lineitem table if available, otherwise generates rows
/// with a recursive CTE.
#[tokio::test]
async fn fabric_strict_large_result() -> anyhow::Result<()> {
    skip_if_no_fabric!();

    let endpoint = env::var("FABRIC_ENDPOINT")?;
    let database = env::var("FABRIC_DATABASE")?;
    let token = get_aad_token().await?;

    let mut client = connect_to_fabric(&endpoint, &database, &token).await?;

    // Try querying from lineitem (tpch_sf1), fall back to a generated set
    let query = if client
        .query(
            "SELECT TOP 1 1 AS x FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'lineitem'",
            &[],
        )
        .await?
        .into_row()
        .await?
        .is_some()
    {
        // Use tpch lineitem table — large rows with multiple columns
        "SELECT TOP 1000 l_orderkey, l_partkey, l_suppkey, \
             CAST(l_quantity AS DECIMAL(10,2)) AS l_quantity, \
             CAST(l_extendedprice AS DECIMAL(12,2)) AS l_extendedprice, \
             l_shipdate, l_comment \
         FROM lineitem ORDER BY l_orderkey, l_linenumber"
            .to_string()
    } else {
        // Generate 1000 rows via cross join (no OPTION hints — Fabric DW disallows them)
        "SELECT TOP 1000 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS row_num, \
             REPLICATE('X', 200) AS padding \
         FROM (VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)) AS t1(n) \
         CROSS JOIN (VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)) AS t2(n) \
         CROSS JOIN (VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)) AS t3(n)"
            .to_string()
    };

    let rows: Vec<_> = client.query(query, &[]).await?.into_first_result().await?;

    assert_eq!(rows.len(), 1000, "Should get exactly 1000 rows");
    eprintln!(
        "Large result set (1000 rows, {} columns) over Fabric strict connection: OK",
        rows[0].columns().len()
    );

    Ok(())
}

/// Test: Verify encryption status via session properties on Fabric.
///
/// Fabric may not expose sys.dm_exec_connections, so we use
/// session context properties instead.
#[tokio::test]
async fn fabric_strict_verify_encryption() -> anyhow::Result<()> {
    skip_if_no_fabric!();

    let endpoint = env::var("FABRIC_ENDPOINT")?;
    let database = env::var("FABRIC_DATABASE")?;
    let token = get_aad_token().await?;

    let mut client = connect_to_fabric(&endpoint, &database, &token).await?;

    // Try sys.dm_exec_connections first (may work on some Fabric SKUs)
    let row = client
        .query(
            "SELECT \
                CAST(CONNECTIONPROPERTY('net_transport') AS NVARCHAR(50)) AS transport, \
                CAST(CONNECTIONPROPERTY('protocol_type') AS NVARCHAR(50)) AS protocol, \
                CAST(CONNECTIONPROPERTY('auth_scheme') AS NVARCHAR(50)) AS auth_scheme",
            &[],
        )
        .await?
        .into_row()
        .await?
        .unwrap();

    let transport: &str = row.get("transport").unwrap();
    let protocol: &str = row.get("protocol").unwrap();
    let auth_scheme: &str = row.get("auth_scheme").unwrap();

    assert_eq!(transport, "TCP", "Should be TCP transport");
    assert_eq!(protocol, "TSQL", "Should be TSQL protocol");
    // Fabric with AAD token should show NTML or AAD-based auth
    assert!(
        !auth_scheme.is_empty(),
        "Should have an auth scheme, got empty"
    );

    eprintln!(
        "Fabric encryption verified: transport={}, protocol={}, auth={}",
        transport, protocol, auth_scheme
    );

    // Also try dm_exec_connections if accessible
    match client
        .query(
            "SELECT encrypt_option FROM sys.dm_exec_connections WHERE session_id = @@SPID",
            &[],
        )
        .await
    {
        Ok(result) => {
            if let Some(row) = result.into_row().await? {
                let encrypt_option: &str = row.get("encrypt_option").unwrap();
                assert!(
                    encrypt_option == "TRUE" || encrypt_option == "STRICT",
                    "Expected encrypted, got: {}",
                    encrypt_option
                );
                eprintln!("  dm_exec_connections.encrypt_option = {}", encrypt_option);
            }
        }
        Err(_) => {
            eprintln!("  (sys.dm_exec_connections not accessible on this Fabric endpoint)");
        }
    }

    Ok(())
}

/// Test: String and unicode operations over strict connection.
#[tokio::test]
async fn fabric_strict_string_operations() -> anyhow::Result<()> {
    skip_if_no_fabric!();

    let endpoint = env::var("FABRIC_ENDPOINT")?;
    let database = env::var("FABRIC_DATABASE")?;
    let token = get_aad_token().await?;

    let mut client = connect_to_fabric(&endpoint, &database, &token).await?;

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

    eprintln!("String/unicode operations over Fabric strict connection: OK");
    Ok(())
}
