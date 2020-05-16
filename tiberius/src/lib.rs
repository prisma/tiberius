//! A pure-rust TDS implementation for Microsoft SQL Server (>=2008)
#![recursion_limit = "512"]
#![warn(missing_docs)]
#![warn(missing_debug_implementations, rust_2018_idioms)]
#![doc(test(attr(deny(rust_2018_idioms, warnings))))]
#![doc(test(attr(allow(unused_extern_crates, unused_variables))))]

#[macro_use]
mod macros;

mod client;
mod from_sql;
mod sql_read_bytes;
mod to_sql;

pub mod error;
mod result;
mod row;
mod tds;

pub use client::{AuthMethod, Client, ClientBuilder};
pub(crate) use error::Error;
pub use from_sql::{FromSql, FromSqlOwned};
pub use result::*;
pub use row::{Column, ColumnType, Row};
pub use tds::{codec::ColumnData, numeric, time, xml, EncryptionLevel};
pub use to_sql::{IntoSql, ToSql};
pub use uuid::Uuid;

use sql_read_bytes::*;
use tds::codec::*;

#[cfg(all(windows, doc))]
use std::{str, net};

/// An alias for a result that holds crate's error type as the error.
pub type Result<T> = std::result::Result<T, Error>;

pub(crate) fn get_driver_version() -> u64 {
    env!("CARGO_PKG_VERSION")
        .splitn(6, '.')
        .enumerate()
        .fold(0u64, |acc, part| match part.1.parse::<u64>() {
            Ok(num) => acc | num << (part.0 * 8),
            _ => acc | 0 << (part.0 * 8),
        })
}

/// Helper function for consuming a reply
/// from the SQL browser on Windows
#[cfg(all(windows, doc))]
pub fn consume_sql_browser_message(mut addr: net::SocketAddr, mut buf: Vec<u8>, len: usize, instance_name: &str) -> crate::Result<net::SocketAddr> {
    buf.truncate(len);

    let err = Error::Conversion(
        format!("Could not resolve SQL browser instance {}", instance_name).into(),
    );

    if len == 0 {
        return Err(err);
    }

    let response = str::from_utf8(&buf[3..len])?;

    let port: u16 = response
        .find("tcp;")
        .and_then(|pos| response[pos..].split(';').nth(1))
        .ok_or(err)
        .and_then(|val| Ok(val.parse()?))?;

    addr.set_port(port);

    Ok(addr)
}
