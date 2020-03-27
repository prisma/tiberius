use crate::protocol;
use std::{borrow::Cow, convert::Infallible, fmt, io};
use thiserror::Error;

// TODO: keep private
#[derive(Debug, Error)]
#[error("{}", _0)]
pub struct CloneableIoError(io::Error);

impl Clone for CloneableIoError {
    fn clone(&self) -> Self {
        Self(io::Error::new(self.0.kind(), format!("{}", self)))
    }
}

/// A unified error enum that contains several errors that might occurr during
/// the lifecycle of this driver
#[derive(Debug, Clone, Error)]
pub enum Error {
    #[error("An error occured during the attempt of performing I/O: {}", _0)]
    Io(CloneableIoError),
    #[error("Protocol error: {}", _0)]
    Protocol(Cow<'static, str>),
    #[error("Encoding error: {}", _0)]
    Encoding(Cow<'static, str>),
    #[error("Conversion error: {}", _0)]
    Conversion(Cow<'static, str>),
    #[error("UTF-8 error: {}", _0)]
    Utf8(std::str::Utf8Error),
    #[error("UTF-16 error")]
    Utf16,
    #[error("Error parsing an integer: {}", _0)]
    ParseInt(std::num::ParseIntError),
    #[error("Token error: {}", _0)]
    Server(protocol::TokenError),
    #[error("Operation cancelled")]
    Canceled,
}

impl From<Infallible> for Error {
    fn from(_: Infallible) -> Self {
        unreachable!()
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::Io(CloneableIoError(err))
    }
}

impl From<std::num::ParseIntError> for Error {
    fn from(err: std::num::ParseIntError) -> Error {
        Error::ParseInt(err)
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(err: std::str::Utf8Error) -> Error {
        Error::Utf8(err)
    }
}

impl From<std::string::FromUtf16Error> for Error {
    fn from(_err: std::string::FromUtf16Error) -> Error {
        Error::Utf16
    }
}
