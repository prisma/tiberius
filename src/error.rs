//! Error module
pub use crate::tds::codec::TokenError;
pub use std::io::ErrorKind as IoErrorKind;
use std::{borrow::Cow, convert::Infallible, io};
use thiserror::Error;

/// A unified error enum that contains several errors that might occurr during
/// the lifecycle of this driver
#[derive(Debug, Clone, Error)]
pub enum Error {
    #[error("An error occured during the attempt of performing I/O: {}", message)]
    /// An error occured when performing I/O to the server.
    Io {
        /// A list specifying general categories of I/O error.
        kind: IoErrorKind,
        /// The error description.
        message: String,
    },
    #[error("Protocol error: {}", _0)]
    /// An error happened during the request or response parsing.
    Protocol(Cow<'static, str>),
    #[error("Encoding error: {}", _0)]
    /// Server responded with encoding not supported.
    Encoding(Cow<'static, str>),
    #[error("Conversion error: {}", _0)]
    /// Conversion failure from one type to another.
    Conversion(Cow<'static, str>),
    #[error("UTF-8 error")]
    /// Tried to convert data to UTF-8 that was not valid.
    Utf8,
    #[error("UTF-16 error")]
    /// Tried to convert data to UTF-16 that was not valid.
    Utf16,
    #[error("Error parsing an integer: {}", _0)]
    /// Tried to parse an integer that was not an integer.
    ParseInt(std::num::ParseIntError),
    #[error("Token error: {}", _0)]
    /// An error returned by the server.
    Server(TokenError),
    #[error("Error forming TLS connection: {}", _0)]
    /// An error in the TLS handshake.
    Tls(String),
    #[cfg(any(feature = "integrated-auth-gssapi", doc))]
    /// An error from the GSSAPI library.
    #[error("GSSAPI Error: {}", _0)]
    Gssapi(String),
    #[error("Server commanded a redirect to an alternate address\n{}:{} => {}:{}", alternate_address, port, old_address, old_port)]
    /// A server's command to use an alternative address instead of the one requested by the user
    RouteToAlternateAddress {
        /// base address, most likely a domain
        alternate_address: String,
        /// port
        port: usize,
        /// previous address; probably not very useful, but is there in the packet
        old_address: String,
        /// previous server port; same as above
        old_port: usize,
    }

}

impl From<uuid::Error> for Error {
    fn from(e: uuid::Error) -> Self {
        Self::Conversion(format!("Error convertiong a Guid value {}", e).into())
    }
}

#[cfg(feature = "tls")]
impl From<async_native_tls::Error> for Error {
    fn from(v: async_native_tls::Error) -> Self {
        Error::Tls(format!("{}", v))
    }
}

impl From<Infallible> for Error {
    fn from(_: Infallible) -> Self {
        unreachable!()
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Self::Io {
            kind: err.kind(),
            message: format!("{}", err),
        }
    }
}

impl From<std::num::ParseIntError> for Error {
    fn from(err: std::num::ParseIntError) -> Error {
        Error::ParseInt(err)
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(_: std::str::Utf8Error) -> Error {
        Error::Utf8
    }
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(_err: std::string::FromUtf8Error) -> Error {
        Error::Utf8
    }
}

impl From<std::string::FromUtf16Error> for Error {
    fn from(_err: std::string::FromUtf16Error) -> Error {
        Error::Utf16
    }
}

#[cfg(feature = "integrated-auth-gssapi")]
impl From<libgssapi::error::Error> for Error {
    fn from(err: libgssapi::error::Error) -> Error {
        Error::Gssapi(format!("{}", err))
    }
}
