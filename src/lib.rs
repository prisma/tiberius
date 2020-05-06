//! A pure-rust TDS implementation for Microsoft SQL Server (>=2008)
#![recursion_limit = "512"]
#[macro_use]
mod macros;

mod client;
mod sql_read_bytes;
mod to_sql;

mod error;
mod result;
mod row;
mod tds;

pub use client::{AuthMethod, Client, ClientBuilder};
pub use error::Error;
pub use result::*;
pub use row::{Column, Row};
pub use tds::{numeric, time, xml, EncryptionLevel};
pub use to_sql::ToSql;
pub use uuid::Uuid;

use sql_read_bytes::*;
use tds::codec::*;

pub type Result<T> = std::result::Result<T, Error>;

pub(crate) fn get_driver_version() -> u64 {
    env!("CARGO_PKG_VERSION")
        .splitn(6, '.')
        .enumerate()
        .fold(0u64, |acc, part| {
            acc | (part.1.parse::<u64>().unwrap() << (part.0 * 8))
        })
}
