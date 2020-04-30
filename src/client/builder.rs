use super::{connection::*, AuthMethod};
use crate::{protocol::Context, Client, EncryptionLevel};

#[derive(Debug, Clone)]
pub struct ClientBuilder {
    host: Option<String>,
    port: Option<u16>,
    database: Option<String>,
    instance_name: Option<String>,
    ssl: EncryptionLevel,
    trust_cert: bool,
    auth: AuthMethod,
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self {
            host: None,
            port: None,
            database: None,
            instance_name: None,
            ssl: EncryptionLevel::NotSupported,
            trust_cert: false,
            auth: AuthMethod::None,
        }
    }
}

impl ClientBuilder {
    pub fn host(&mut self, host: impl ToString) {
        self.host = Some(host.to_string());
    }

    pub fn port(&mut self, port: u16) {
        self.port = Some(port);
    }

    pub fn database(&mut self, database: impl ToString) {
        self.database = Some(database.to_string())
    }

    pub fn instance_name(&mut self, name: impl ToString) {
        self.instance_name = Some(name.to_string());
    }

    pub fn ssl(&mut self, ssl: EncryptionLevel) {
        self.ssl = ssl;
    }

    pub fn trust_cert(&mut self) {
        self.trust_cert = true;
    }

    pub fn authentication(&mut self, auth: AuthMethod) {
        self.auth = auth;
    }

    fn get_host(&self) -> &str {
        self.host
            .as_ref()
            .map(|s| s.as_str())
            .unwrap_or("127.0.0.1")
    }

    fn get_port(&self) -> u16 {
        self.port.unwrap_or(1433)
    }

    #[cfg(windows)]
    fn create_context(&self) -> Context {
        let mut context = Context::new();
        context.set_spn(self.get_host(), self.get_port());
        context
    }

    #[cfg(not(windows))]
    fn create_context(&self) -> Context {
        Context::new()
    }

    pub async fn build(self) -> crate::Result<Client> {
        let context = self.create_context();
        let addr = format!("{}:{}", self.get_host(), self.get_port());

        let opts = ConnectOpts {
            ssl: self.ssl,
            trust_cert: self.trust_cert,
            auth: self.auth,
            database: self.database,
            instance_name: self.instance_name,
        };

        let connection = Connection::connect_tcp(addr, context, opts).await?;

        Ok(Client { connection })
    }
}
