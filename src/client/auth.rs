use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;

#[derive(Clone, PartialEq, Eq)]
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

#[derive(Clone, PartialEq, Eq)]
#[cfg(any(all(windows, feature = "winauth"), doc))]
#[cfg_attr(feature = "docs", doc(all(windows, feature = "winauth")))]
pub struct WindowsAuth {
    pub(crate) user: String,
    pub(crate) password: String,
    pub(crate) domain: Option<String>,
}

#[cfg(any(all(windows, feature = "winauth"), doc))]
#[cfg_attr(feature = "docs", doc(all(windows, feature = "winauth")))]
impl Debug for WindowsAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WindowsAuth")
            .field("user", &self.user)
            .field("password", &"<HIDDEN>")
            .field("domain", &self.domain)
            .finish()
    }
}

/// A trait for providing AAD/Entra ID tokens dynamically, supporting token refresh.
///
/// Implement this trait to supply fresh tokens on each connection or reconnection.
/// This is useful for long-lived applications where tokens expire (~1 hour) and
/// need to be refreshed transparently.
///
/// # Example
///
/// ```rust,no_run
/// use async_trait::async_trait;
/// use tiberius::TokenProvider;
///
/// struct MyTokenProvider {
///     // your credential state (e.g., client_id, client_secret, tenant_id)
/// }
///
/// #[async_trait]
/// impl TokenProvider for MyTokenProvider {
///     async fn get_token(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
///         // Call your identity provider here to get a fresh token
///         // e.g., azure_identity::DefaultAzureCredential, MSAL, etc.
///         Ok("fresh-token".to_string())
///     }
/// }
/// ```
#[async_trait]
pub trait TokenProvider: Send + Sync {
    /// Obtain a fresh AAD/Entra ID access token for the `https://database.windows.net/` resource.
    ///
    /// This method is called each time a new connection or reconnection is established.
    /// Implementations should handle caching and refresh internally.
    async fn get_token(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>>;
}

/// Defines the method of authentication to the server.
pub enum AuthMethod {
    /// Authenticate directly with SQL Server.
    SqlServer(SqlServerAuth),
    /// Authenticate with Windows credentials.
    #[cfg(any(all(windows, feature = "winauth"), doc))]
    #[cfg_attr(feature = "docs", doc(cfg(all(windows, feature = "winauth"))))]
    Windows(WindowsAuth),
    /// Authenticate as the currently logged in user. On Windows uses SSPI and
    /// Kerberos on Unix platforms.
    #[cfg(any(
        all(windows, feature = "winauth"),
        all(unix, feature = "integrated-auth-gssapi"),
        doc
    ))]
    #[cfg_attr(
        feature = "docs",
        doc(cfg(any(windows, all(unix, feature = "integrated-auth-gssapi"))))
    )]
    Integrated,
    /// Authenticate with a static AAD token. The token should encode an AAD
    /// user/service principal which has access to SQL Server.
    ///
    /// For long-lived applications where tokens may expire, prefer
    /// [`AuthMethod::token_provider`] instead.
    AADToken(String),
    /// Authenticate with a dynamic AAD token provider that supports refresh.
    ///
    /// The provider's `get_token()` method is called each time a connection
    /// (or routing reconnection) is established, ensuring fresh tokens.
    AADTokenProvider(Arc<dyn TokenProvider>),
    #[doc(hidden)]
    None,
}

impl Clone for AuthMethod {
    fn clone(&self) -> Self {
        match self {
            Self::SqlServer(a) => Self::SqlServer(a.clone()),
            #[cfg(any(all(windows, feature = "winauth"), doc))]
            Self::Windows(a) => Self::Windows(a.clone()),
            #[cfg(any(
                all(windows, feature = "winauth"),
                all(unix, feature = "integrated-auth-gssapi"),
                doc
            ))]
            Self::Integrated => Self::Integrated,
            Self::AADToken(t) => Self::AADToken(t.clone()),
            Self::AADTokenProvider(p) => Self::AADTokenProvider(Arc::clone(p)),
            Self::None => Self::None,
        }
    }
}

impl Debug for AuthMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SqlServer(a) => f.debug_tuple("SqlServer").field(a).finish(),
            #[cfg(any(all(windows, feature = "winauth"), doc))]
            Self::Windows(a) => f.debug_tuple("Windows").field(a).finish(),
            #[cfg(any(
                all(windows, feature = "winauth"),
                all(unix, feature = "integrated-auth-gssapi"),
                doc
            ))]
            Self::Integrated => write!(f, "Integrated"),
            Self::AADToken(_) => write!(f, "AADToken(<HIDDEN>)"),
            Self::AADTokenProvider(_) => write!(f, "AADTokenProvider(...)"),
            Self::None => write!(f, "None"),
        }
    }
}

impl PartialEq for AuthMethod {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::SqlServer(a), Self::SqlServer(b)) => a == b,
            #[cfg(any(all(windows, feature = "winauth"), doc))]
            (Self::Windows(a), Self::Windows(b)) => a == b,
            #[cfg(any(
                all(windows, feature = "winauth"),
                all(unix, feature = "integrated-auth-gssapi"),
                doc
            ))]
            (Self::Integrated, Self::Integrated) => true,
            (Self::AADToken(a), Self::AADToken(b)) => a == b,
            // Token providers are compared by Arc pointer identity
            (Self::AADTokenProvider(a), Self::AADTokenProvider(b)) => Arc::ptr_eq(a, b),
            (Self::None, Self::None) => true,
            _ => false,
        }
    }
}

impl Eq for AuthMethod {}

impl AuthMethod {
    /// Construct a new SQL Server authentication configuration.
    pub fn sql_server(user: impl ToString, password: impl ToString) -> Self {
        Self::SqlServer(SqlServerAuth {
            user: user.to_string(),
            password: password.to_string(),
        })
    }

    /// Construct a new Windows authentication configuration.
    #[cfg(any(all(windows, feature = "winauth"), doc))]
    #[cfg_attr(feature = "docs", doc(cfg(all(windows, feature = "winauth"))))]
    pub fn windows(user: impl AsRef<str>, password: impl ToString) -> Self {
        let (domain, user) = match user.as_ref().find('\\') {
            Some(idx) => (Some(&user.as_ref()[..idx]), &user.as_ref()[idx + 1..]),
            _ => (None, user.as_ref()),
        };

        Self::Windows(WindowsAuth {
            user: user.to_string(),
            password: password.to_string(),
            domain: domain.map(|s| s.to_string()),
        })
    }

    /// Construct a new configuration with a static AAD auth token.
    ///
    /// For long-lived applications, prefer [`AuthMethod::token_provider`] which
    /// supports automatic token refresh.
    pub fn aad_token(token: impl ToString) -> Self {
        Self::AADToken(token.to_string())
    }

    /// Construct a new configuration with a dynamic token provider.
    ///
    /// The provider's `get_token()` method is called on each new connection,
    /// ensuring fresh tokens even for long-lived applications.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use std::sync::Arc;
    /// use async_trait::async_trait;
    /// use tiberius::{AuthMethod, TokenProvider};
    ///
    /// struct AzCliTokenProvider;
    ///
    /// #[async_trait]
    /// impl TokenProvider for AzCliTokenProvider {
    ///     async fn get_token(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    ///         // In real code, call azure_identity or az CLI
    ///         Ok("fresh-token".to_string())
    ///     }
    /// }
    ///
    /// let auth = AuthMethod::token_provider(Arc::new(AzCliTokenProvider));
    /// ```
    pub fn token_provider(provider: Arc<dyn TokenProvider>) -> Self {
        Self::AADTokenProvider(provider)
    }

    /// Returns true if this auth method uses AAD token authentication
    /// (either static or via provider).
    pub(crate) fn is_aad(&self) -> bool {
        matches!(self, Self::AADToken(_) | Self::AADTokenProvider(_))
    }
}
