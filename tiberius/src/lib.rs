//! A pure-rust TDS implementation for Microsoft SQL Server (>=2008)
#![recursion_limit = "512"]

use protocol::codec::*;

mod client;
mod prepared;

mod collation;
mod error;
mod macros;
mod protocol;
mod row;
mod tls;

pub use client::{AuthMethod, Client, ClientBuilder};
pub use error::Error;
pub use protocol::{
    stream::{ExecuteResult, QueryResult},
    EncryptionLevel,
};
pub use row::{Column, Row};

pub type Result<T> = std::result::Result<T, Error>;

pub(crate) fn get_driver_version() -> u64 {
    env!("CARGO_PKG_VERSION")
        .splitn(6, '.')
        .enumerate()
        .fold(0u64, |acc, part| {
            acc | (part.1.parse::<u64>().unwrap() << (part.0 * 8))
        })
}
