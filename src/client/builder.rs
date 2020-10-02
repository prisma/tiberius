use super::AuthMethod;
use crate::EncryptionLevel;
use std::collections::HashMap;

#[derive(Clone, Debug)]
/// The `Config` struct contains all configuration information
/// required for connecting to the database with a [`Client`]. It also provides
/// the server address when connecting to a `TcpStream` via the
/// [`get_addr`] method.
///
/// When using an [ADO.NET connection string], it can be
/// constructed using the [`from_ado_string`] function.
///
/// [`Client`]: struct.Client.html
/// [ADO.NET connection string]: https://docs.microsoft.com/en-us/dotnet/framework/data/adonet/connection-strings
/// [`from_ado_string`]: struct.Config.html#method.from_ado_string
/// [`get_addr`]: struct.Config.html#method.get_addr
pub struct Config {
    pub(crate) host: Option<String>,
    pub(crate) port: Option<u16>,
    pub(crate) database: Option<String>,
    pub(crate) instance_name: Option<String>,
    pub(crate) encryption: EncryptionLevel,
    pub(crate) trust_cert: bool,
    pub(crate) auth: AuthMethod,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            host: None,
            port: None,
            database: None,
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

impl Config {
    /// Create a new `Config` with the default settings.
    pub fn new() -> Self {
        Self::default()
    }

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
    ///
    /// - Defaults to no name specified.
    pub fn instance_name(&mut self, name: impl ToString) {
        self.instance_name = Some(name.to_string());
    }

    /// Set the preferred encryption level.
    ///
    /// - With `tls` feature, defaults to `Required`.
    /// - Without `tls` feature, defaults to `NotSupported`.
    pub fn encryption(&mut self, encryption: EncryptionLevel) {
        self.encryption = encryption;
    }

    /// If set, the server certificate will not be validated and it is accepted
    /// as-is.
    ///
    /// On production setting, the certificate should be added to the local key
    /// storage, using this setting is potentially dangerous.
    ///
    /// - Defaults to `false`.
    pub fn trust_cert(&mut self) {
        self.trust_cert = true;
    }

    /// Sets the authentication method.
    ///
    /// - Defaults to `None`.
    pub fn authentication(&mut self, auth: AuthMethod) {
        self.auth = auth;
    }

    pub(crate) fn get_host(&self) -> &str {
        self.host
            .as_ref()
            .map(|s| s.as_str())
            .unwrap_or("localhost")
    }

    pub(crate) fn get_port(&self) -> u16 {
        self.port.unwrap_or(1433)
    }

    /// Get the host address including port
    pub fn get_addr(&self) -> String {
        format!("{}:{}", self.get_host(), self.get_port())
    }

    /// Creates a new `Config` from an [ADO.NET connection
    /// string](https://docs.microsoft.com/en-us/dotnet/framework/data/adonet/connection-strings).
    ///
    /// # Supported parameters
    ///
    /// All parameter keys are handled case-insensitive.
    ///
    /// |Parameter|Allowed values|Description|
    /// |--------|--------|--------|
    /// |`server`|`<string>`|The name or network address of the instance of SQL Server to which to connect. The port number can be specified after the server name. The correct form of this parameter is either `tcp:host,port` or `tcp:host\\instance`|
    /// |`IntegratedSecurity`|`true`,`false`,`yes`,`no`|Toggle between Windows/Kerberos authentication and SQL authentication.|
    /// |`uid`,`username`,`user`,`user id`|`<string>`|The SQL Server login account.|
    /// |`password`,`pwd`|`<string>`|The password for the SQL Server account logging on.|
    /// |`database`|`<string>`|The name of the database.|
    /// |`TrustServerCertificate`|`true`,`false`,`yes`,`no`|Specifies whether the driver trusts the server certificate when connecting using TLS.|
    /// |`encrypt`|`true`,`false`,`yes`,`no`|Specifies whether the driver uses TLS to encrypt communication.|
    pub fn from_ado_string(s: &str) -> crate::Result<Self> {
        let ado = AdoNetString::parse(s)?;
        let mut builder = Self::new();

        let server = ado.server()?;

        if let Some(host) = server.host {
            builder.host(host);
        }

        if let Some(port) = server.port {
            builder.port(port);
        }

        if let Some(_instance) = server.instance {
            #[cfg(windows)]
            builder.instance_name(_instance);
        }

        builder.authentication(ado.authentication()?);

        if let Some(database) = ado.database() {
            builder.database(database);
        }

        if ado.trust_cert()? {
            builder.trust_cert();
        }

        builder.encryption(ado.encrypt()?);

        Ok(builder)
    }
}

pub(crate) struct AdoNetString {
    dict: HashMap<String, String>,
}

impl AdoNetString {
    pub fn parse(s: &str) -> crate::Result<Self> {
        let dict: crate::Result<HashMap<String, String>> = s
            .split(";")
            .filter(|kv| kv != &"")
            .map(|kv| {
                let mut splitted = kv.split("=");

                let key = splitted
                    .next()
                    .ok_or_else(|| {
                        crate::Error::Conversion(
                            "Missing a valid key in connection string parameters.".into(),
                        )
                    })?
                    .trim()
                    .to_lowercase();

                let value = splitted
                    .next()
                    .ok_or_else(|| {
                        crate::Error::Conversion(
                            "Missing a valid value in connection string parameters.".into(),
                        )
                    })?
                    .trim()
                    .to_string();

                Ok((key, value))
            })
            .collect();

        Ok(Self { dict: dict? })
    }

    pub fn server(&self) -> crate::Result<ServerDefinition> {
        fn parse_server(parts: Vec<&str>) -> crate::Result<ServerDefinition> {
            if parts.is_empty() || parts.len() >= 3 {
                return Err(crate::Error::Conversion("Server value faulty.".into()));
            }

            let definition = if parts[0].contains('\\') {
                let port = if parts.len() == 1 {
                    1434
                } else {
                    parts[1].parse::<u16>()?
                };

                let parts: Vec<&str> = parts[0].split('\\').collect();

                ServerDefinition {
                    host: Some(parts[0].into()),
                    port: Some(port),
                    instance: Some(parts[1].into()),
                }
            } else {
                // Connect using a TCP target
                let (host, port) = (parts[0], parts[1].parse::<u16>()?);

                ServerDefinition {
                    host: Some(host.into()),
                    port: Some(port),
                    instance: None,
                }
            };

            Ok(definition)
        }

        match self.dict.get("server") {
            Some(value) if value.starts_with("tcp:") => {
                parse_server(value[4..].split(',').collect())
            }
            Some(value) => parse_server(value.split(',').collect()),
            None => Ok(ServerDefinition {
                host: None,
                port: None,
                instance: None,
            }),
        }
    }

    pub fn authentication(&self) -> crate::Result<AuthMethod> {
        let user = self
            .dict
            .get("uid")
            .or_else(|| self.dict.get("username"))
            .or_else(|| self.dict.get("user"))
            .or_else(|| self.dict.get("user id"))
            .map(|s| s.as_str());

        let pw = self
            .dict
            .get("password")
            .or_else(|| self.dict.get("pwd"))
            .map(|s| s.as_str());

        match self.dict.get("integratedsecurity") {
            #[cfg(windows)]
            Some(val) if val.to_lowercase() == "sspi" || Self::parse_bool(val)? => match (user, pw)
            {
                (None, None) => Ok(AuthMethod::Integrated),
                _ => Ok(AuthMethod::windows(user.unwrap_or(""), pw.unwrap_or(""))),
            },
            #[cfg(feature = "integrated-auth-gssapi")]
            Some(val) if val.to_lowercase() == "sspi" || Self::parse_bool(val)? => {
                Ok(AuthMethod::Integrated)
            }
            _ => Ok(AuthMethod::sql_server(user.unwrap_or(""), pw.unwrap_or(""))),
        }
    }

    pub fn database(&self) -> Option<String> {
        self.dict
            .get("database")
            .or_else(|| self.dict.get("initial catalog"))
            .map(|db| db.to_string())
    }

    pub fn trust_cert(&self) -> crate::Result<bool> {
        self.dict
            .get("trustservercertificate")
            .map(|val| Self::parse_bool(val))
            .unwrap_or(Ok(false))
    }

    #[cfg(feature = "tls")]
    pub fn encrypt(&self) -> crate::Result<EncryptionLevel> {
        self.dict
            .get("encrypt")
            .map(|val| match Self::parse_bool(val) {
                Ok(true) => Ok(EncryptionLevel::Required),
                Ok(false) => Ok(EncryptionLevel::Off),
                Err(_) if val == "DANGER_PLAINTEXT" => Ok(EncryptionLevel::NotSupported),
                Err(e) => Err(e)?,
            })
            .unwrap_or(Ok(EncryptionLevel::Off))
    }

    #[cfg(not(feature = "tls"))]
    pub fn encrypt(&self) -> crate::Result<EncryptionLevel> {
        Ok(EncryptionLevel::NotSupported)
    }

    fn parse_bool<T: AsRef<str>>(v: T) -> crate::Result<bool> {
        match v.as_ref().trim().to_lowercase().as_str() {
            "true" | "yes" => Ok(true),
            "false" | "no" => Ok(false),
            _ => Err(crate::Error::Conversion(
                "Connection string: Not a valid boolean".into(),
            )),
        }
    }
}

pub(crate) struct ServerDefinition {
    host: Option<String>,
    port: Option<u16>,
    instance: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_parsing_no_browser() -> crate::Result<()> {
        let test_str = "server=tcp:my-server.com,4200";
        let ado = AdoNetString::parse(test_str)?;
        let server = ado.server()?;

        assert_eq!(Some("my-server.com".to_string()), server.host);
        assert_eq!(Some(4200), server.port);
        assert_eq!(None, server.instance);

        Ok(())
    }

    #[test]
    fn server_parsing_no_tcp() -> crate::Result<()> {
        let test_str = "server=my-server.com,4200";
        let ado = AdoNetString::parse(test_str)?;
        let server = ado.server()?;

        assert_eq!(Some("my-server.com".to_string()), server.host);
        assert_eq!(Some(4200), server.port);
        assert_eq!(None, server.instance);

        Ok(())
    }

    #[test]
    fn server_parsing_with_browser() -> crate::Result<()> {
        let test_str = "server=tcp:my-server.com\\TIBERIUS";
        let ado = AdoNetString::parse(test_str)?;
        let server = ado.server()?;

        assert_eq!(Some("my-server.com".to_string()), server.host);
        assert_eq!(Some(1434), server.port);
        assert_eq!(Some("TIBERIUS".to_string()), server.instance);

        Ok(())
    }

    #[test]
    fn server_parsing_with_browser_and_port() -> crate::Result<()> {
        let test_str = "server=tcp:my-server.com\\TIBERIUS,666";
        let ado = AdoNetString::parse(test_str)?;
        let server = ado.server()?;

        assert_eq!(Some("my-server.com".to_string()), server.host);
        assert_eq!(Some(666), server.port);
        assert_eq!(Some("TIBERIUS".to_string()), server.instance);

        Ok(())
    }

    #[test]
    fn database_parsing() -> crate::Result<()> {
        let test_str = "database=Foo";
        let ado = AdoNetString::parse(test_str)?;

        assert_eq!(Some("Foo".to_string()), ado.database());

        Ok(())
    }

    #[test]
    fn trust_cert_parsing_true() -> crate::Result<()> {
        let test_str = "TrustServerCertificate=true";
        let ado = AdoNetString::parse(test_str)?;

        assert_eq!(true, ado.trust_cert()?);

        Ok(())
    }

    #[test]
    fn trust_cert_parsing_false() -> crate::Result<()> {
        let test_str = "TrustServerCertificate=false";
        let ado = AdoNetString::parse(test_str)?;

        assert_eq!(false, ado.trust_cert()?);

        Ok(())
    }

    #[test]
    fn trust_cert_parsing_yes() -> crate::Result<()> {
        let test_str = "TrustServerCertificate=yes";
        let ado = AdoNetString::parse(test_str)?;

        assert_eq!(true, ado.trust_cert()?);

        Ok(())
    }

    #[test]
    fn trust_cert_parsing_no() -> crate::Result<()> {
        let test_str = "TrustServerCertificate=no";
        let ado = AdoNetString::parse(test_str)?;

        assert_eq!(false, ado.trust_cert()?);

        Ok(())
    }

    #[test]
    fn trust_cert_parsing_missing() -> crate::Result<()> {
        let test_str = "Something=foo;";
        let ado = AdoNetString::parse(test_str)?;

        assert_eq!(false, ado.trust_cert()?);

        Ok(())
    }

    #[test]
    fn trust_cert_parsing_faulty() -> crate::Result<()> {
        let test_str = "TrustServerCertificate=musti;";
        let ado = AdoNetString::parse(test_str)?;

        assert!(ado.trust_cert().is_err());

        Ok(())
    }

    #[test]
    fn parsing_sql_server_authentication() -> crate::Result<()> {
        let test_str = "uid=Musti; pwd=Naukio;";
        let ado = AdoNetString::parse(test_str)?;

        assert_eq!(
            AuthMethod::sql_server("Musti", "Naukio"),
            ado.authentication()?
        );

        Ok(())
    }

    #[test]
    #[cfg(windows)]
    fn parsing_sspi_authentication() -> crate::Result<()> {
        let test_str = "IntegratedSecurity=SSPI";
        let ado = AdoNetString::parse(test_str)?;

        assert_eq!(AuthMethod::Integrated, ado.authentication()?);

        Ok(())
    }

    #[test]
    #[cfg(all(feature = "integrated-auth-gssapi", unix))]
    fn parsing_sspi_authentication() -> crate::Result<()> {
        let test_str = "IntegratedSecurity=true";
        let ado = AdoNetString::parse(test_str)?;

        assert_eq!(AuthMethod::Integrated, ado.authentication()?);

        Ok(())
    }

    #[test]
    #[cfg(windows)]
    fn parsing_windows_authentication() -> crate::Result<()> {
        let test_str = "uid=Musti;pwd=Naukio; IntegratedSecurity=SSPI;";
        let ado = AdoNetString::parse(test_str)?;

        assert_eq!(
            AuthMethod::windows("Musti", "Naukio"),
            ado.authentication()?
        );

        Ok(())
    }

    #[test]
    fn parsing_database() -> crate::Result<()> {
        let test_str = "database=Cats";
        let ado = AdoNetString::parse(test_str)?;

        assert_eq!(Some("Cats".to_string()), ado.database());

        Ok(())
    }

    #[test]
    #[cfg(feature = "tls")]
    fn encryption_parsing_on() -> crate::Result<()> {
        let test_str = "encrypt=true";
        let ado = AdoNetString::parse(test_str)?;

        assert_eq!(EncryptionLevel::Required, ado.encrypt()?);

        Ok(())
    }

    #[test]
    #[cfg(feature = "tls")]
    fn encryption_parsing_off() -> crate::Result<()> {
        let test_str = "encrypt=false";
        let ado = AdoNetString::parse(test_str)?;

        assert_eq!(EncryptionLevel::Off, ado.encrypt()?);

        Ok(())
    }

    #[test]
    #[cfg(feature = "tls")]
    fn encryption_parsing_missing() -> crate::Result<()> {
        let test_str = "";
        let ado = AdoNetString::parse(test_str)?;

        assert_eq!(EncryptionLevel::Off, ado.encrypt()?);

        Ok(())
    }

    #[test]
    #[cfg(not(feature = "tls"))]
    fn encryption_parsing_on() -> crate::Result<()> {
        let test_str = "encrypt=true";
        let ado = AdoNetString::parse(test_str)?;

        assert_eq!(EncryptionLevel::NotSupported, ado.encrypt()?);

        Ok(())
    }

    #[test]
    #[cfg(not(feature = "tls"))]
    fn encryption_parsing_off() -> crate::Result<()> {
        let test_str = "encrypt=false";
        let ado = AdoNetString::parse(test_str)?;

        assert_eq!(EncryptionLevel::NotSupported, ado.encrypt()?);

        Ok(())
    }

    #[test]
    #[cfg(not(feature = "tls"))]
    fn encryption_parsing_missing() -> crate::Result<()> {
        let test_str = "";
        let ado = AdoNetString::parse(test_str)?;

        assert_eq!(EncryptionLevel::NotSupported, ado.encrypt()?);

        Ok(())
    }
}
