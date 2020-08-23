use std::fmt::Debug;

#[derive(Clone, PartialEq)]
pub struct SqlServerAuth {
    user: String,
    password: String,
}

impl SqlServerAuth {
    pub(crate) fn user(&self) -> &str {
        &self.user
    }

    pub(crate) fn password(&self) -> &str {
        &self.password
    }
}

impl Debug for SqlServerAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlServerAuth")
            .field("user", &self.user)
            .field("password", &"<HIDDEN>")
            .finish()
    }
}

#[derive(Clone, PartialEq)]
#[cfg(any(windows, doc))]
pub struct WindowsAuth {
    pub(crate) user: String,
    pub(crate) password: String,
    pub(crate) domain: Option<String>,
}

#[cfg(any(windows, doc))]
impl Debug for WindowsAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WindowsAuth")
            .field("user", &self.user)
            .field("password", &"<HIDDEN>")
            .field("domain", &self.domain)
            .finish()
    }
}

/// Defines the method of authentication to the server.
#[derive(Clone, Debug, PartialEq)]
pub enum AuthMethod {
    /// Authenticate directly with SQL Server. The only authentication method
    /// that works on all platforms.
    SqlServer(SqlServerAuth),
    #[cfg(any(windows, doc))]
    /// Authenticate with Windows credentials. Only available on Windows
    /// platforms.
    Windows(WindowsAuth),
    #[cfg(any(windows, doc))]
    /// Authenticate as the currently logged in user. Only available on Windows
    /// platforms.
    WindowsIntegrated,
    #[cfg(any(feature = "integrated-auth-gssapi", doc))]
    /// Authenticate as the currently logged in (Kerberos) user. (Enable feature
    /// `integrated-auth-gssapi`) to enable.
    Integrated,
    #[doc(hidden)]
    None,
}

impl AuthMethod {
    /// Construct a new SQL Server authentication configuration.
    pub fn sql_server(user: impl ToString, password: impl ToString) -> Self {
        Self::SqlServer(SqlServerAuth {
            user: user.to_string(),
            password: password.to_string(),
        })
    }

    /// Construct a new Windows authentication configuration. Only available on
    /// Windows platforms.
    #[cfg(any(windows, doc))]
    pub fn windows(user: impl AsRef<str>, password: impl ToString) -> Self {
        let (domain, user) = match user.as_ref().find("\\") {
            Some(idx) => (Some(&user.as_ref()[..idx]), &user.as_ref()[idx + 1..]),
            _ => (None, user.as_ref()),
        };

        Self::Windows(WindowsAuth {
            user: user.to_string(),
            password: password.to_string(),
            domain: domain.map(|s| s.to_string()),
        })
    }
}
