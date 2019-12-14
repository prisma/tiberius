use std::borrow::Cow;
use std::convert::Infallible;
use std::fmt;
use std::io;

use crate::protocol;

// TODO: keep private
#[derive(Debug)]
pub struct ClonableIoError(io::Error);
impl Clone for ClonableIoError {
    fn clone(&self) -> ClonableIoError {
        ClonableIoError(io::Error::new(self.0.kind(), ""))
    }
}
/// A unified error enum that contains several errors that might occurr during the lifecycle of this driver
#[derive(Debug, Clone)]
pub enum Error {
    /// An error occurred during the attempt of performing I/O
    Io(ClonableIoError),
    /// An error occurred on the protocol level
    Protocol(Cow<'static, str>),
    Encoding(Cow<'static, str>),
    Conversion(Cow<'static, str>),
    Utf8(std::str::Utf8Error),
    Utf16,
    ParseInt(std::num::ParseIntError),
    Server(protocol::TokenError),
    Canceled,
}

impl std::fmt::Display for Error {
    fn fmt(&self, _f: &mut std::fmt::Formatter) -> fmt::Result {
        unimplemented!() // TODO
    }
}

impl From<Infallible> for Error {
    fn from(_: Infallible) -> Self {
        unreachable!()
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match *self {
            // TODO we only keep the kind around, due to shared
            Error::Io(ref err) => Some(&err.0),
            Error::Utf8(ref err) => Some(err),
            Error::ParseInt(ref err) => Some(err),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::Io(ClonableIoError(err))
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
