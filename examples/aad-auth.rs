//! Use AAD-Auth to connect to SQL server.
//!
//! To Setup:
//! - Follow this [link](https://docs.microsoft.com/en-us/azure/azure-sql/database/authentication-aad-configure?view=azuresql&tabs=azure-powershell) to setup your Azure SQL with AAD auth;
//! - Create an AAD Service Principal [link](https://docs.microsoft.com/en-us/azure/azure-sql/database/authentication-aad-service-principal?view=azuresql) and configure it to access your SQL instance;
//! - Setup the environment variable with:
//!   - CLIENT_ID: service principal ID;
//!   - CLIENT_SECRET: service principal secret;
//!   - TENANT_ID: tenant id of service principal and sql instance;
//!   - SERVER: SQL server URI
use azure_identity::client_credentials_flow;
use oauth2::{ClientId, ClientSecret};
use std::{env, sync::Arc};
use tiberius::{AuthMethod, Client, Config, Query};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // following code will retrive token with AAD Service Principal Auth
    let client_id =
        ClientId::new(env::var("CLIENT_ID").expect("Missing CLIENT_ID environment variable."));
    let client_secret = ClientSecret::new(
        env::var("CLIENT_SECRET").expect("Missing CLIENT_SECRET environment variable."),
    );
    let tenant_id = env::var("TENANT_ID").expect("Missing TENANT_ID environment variable.");

    let client = Arc::new(reqwest::Client::new());
    // This will give you the final token to use in authorization.
    let token = client_credentials_flow::perform(
        client,
        &client_id,
        &client_secret,
        &["https://management.azure.com/"],
        &tenant_id,
    )
    .await?;

    let server = env::var("SERVER").expect("Missing SERVER environment variable.");
    let config = Config::builder()
        .host(server)
        .port(1433)
        .authentication(AuthMethod::AADToken(
            token.access_token().secret().to_string(),
        ))
        .trust_cert()
        .build();

    let tcp = TcpStream::connect(config.get_addr()).await?;
    tcp.set_nodelay(true)?;

    let mut client = Client::connect(config, tcp.compat_write()).await?;
    let params = vec![String::from("foo"), String::from("bar")];
    let mut select = Query::new("SELECT @P1, @P2, @P3");

    for param in params.into_iter() {
        select.bind(param);
    }

    let _res = select.query(&mut client).await?;

    Ok(())
}
