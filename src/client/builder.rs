use super::{connection::*, AuthMethod};
use crate::{tds::Context, Client, EncryptionLevel};

#[derive(Clone)]
/// A builder for creating a new [`Client`].
///
/// [`Client`]: struct.Client.html
pub struct ClientBuilder {
    host: Option<String>,
    port: Option<u16>,
    database: Option<String>,
    #[cfg(windows)]
    instance_name: Option<String>,
    encryption: EncryptionLevel,
    trust_cert: bool,
    auth: AuthMethod,
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self {
            host: None,
            port: None,
            database: None,
            #[cfg(windows)]
            instance_name: None,
            #[cfg(feature = "tls")]
            encryption: EncryptionLevel::Required,
            #[cfg(not(feature = "tls"))]
            encryption: EncryptionLevel::NotSupported,
            trust_cert: false,
            auth: AuthMethod::None,
        }
    }
}

impl ClientBuilder {
    /// A host or ip address to connect to.
    ///
    /// - Defaults to `localhost`.
    pub fn host(&mut self, host: impl ToString) {
        self.host = Some(host.to_string());
    }

    /// The server port.
    ///
    /// - Defaults to `1433`.
    pub fn port(&mut self, port: u16) {
        self.port = Some(port);
    }

    /// The database to connect to.
    ///
    /// - Defaults to `master`.
    pub fn database(&mut self, database: impl ToString) {
        self.database = Some(database.to_string())
    }

    /// The instance name as defined in the SQL Browser. Only available on
    /// Windows platforms.
    ///
    /// If specified, the port is replaced with the value returned from the
    /// browser.
    #[cfg(any(windows, doc))]
    pub fn instance_name(&mut self, name: impl ToString) {
        self.instance_name = Some(name.to_string());
    }

    /// Set the preferred encryption level.
    pub fn encryption(&mut self, encryption: EncryptionLevel) {
        self.encryption = encryption;
    }

    /// If set, the server certificate will not be validated and it is accepted
    /// as-is.
    ///
    /// On production setting, the certificate should be added to the local key
    /// storage, using this setting is potentially dangerous.
    pub fn trust_cert(&mut self) {
        self.trust_cert = true;
    }

    /// Sets the authentication method.
    pub fn authentication(&mut self, auth: AuthMethod) {
        self.auth = auth;
    }

    fn get_host(&self) -> &str {
        self.host
            .as_ref()
            .map(|s| s.as_str())
            .unwrap_or("localhost")
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

    /// Creates a new client and connects to the server.
    pub async fn build(self) -> crate::Result<Client> {
        let context = self.create_context();
        let addr = format!("{}:{}", self.get_host(), self.get_port());

        let opts = ConnectOpts {
            encryption: self.encryption,
            trust_cert: self.trust_cert,
            auth: self.auth,
            database: self.database,
            #[cfg(windows)]
            instance_name: self.instance_name,
            #[cfg(not(windows))]
            instance_name: None,
        };

        let connection = Connection::connect_tcp(addr, context, opts).await?;

        Ok(Client { connection })
    }
}
