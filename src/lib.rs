//! A pure-rust TDS implementation for Microsoft SQL Server (>=2008)
#![recursion_limit = "512"]

mod client;
mod prepared;
mod sql_read_bytes;

mod collation;
mod error;
mod macros;
mod protocol;
mod result;
mod row;
mod tls;

pub use client::{AuthMethod, Client, ClientBuilder};
pub use error::Error;
pub use protocol::EncryptionLevel;
pub use result::*;
pub use row::{Column, Row};
pub use uuid::Uuid;

use protocol::codec::*;
use sql_read_bytes::*;

pub type Result<T> = std::result::Result<T, Error>;

pub(crate) fn get_driver_version() -> u64 {
    env!("CARGO_PKG_VERSION")
        .splitn(6, '.')
        .enumerate()
        .fold(0u64, |acc, part| {
            acc | (part.1.parse::<u64>().unwrap() << (part.0 * 8))
        })
}
