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

//#[cfg(any(windows, doc))]
use std::{str, net, future, convert};

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


//#[cfg(windows)]
use futures_timer;
//#[cfg(windows)]
use futures::{select, future::FutureExt};

//#[cfg(any(windows, doc))]
pub async fn find_tcp_port<FUT>(
    mut addr: net::SocketAddr, 
    instance_name: &str, 
    //udp_sender: for<'a> fn(&'a str, &'a net::SocketAddr, &'a [u8], &'a mut [u8]) -> futures::future::BoxFuture<'a, Result<usize>>,
    udp_sender: for<'a> fn(&'a str, &'a net::SocketAddr, &'a [u8], &'a mut [u8]) -> FUT,
    //udp_sender: fn(&'a str, &'a net::SocketAddr, &'a [u8], &'a mut [u8]) -> FUT,
    ) -> Result<net::SocketAddr> 
where 
    //'a: 'fut,
    FUT: future::Future<Output = Result<usize>> ,
    //for<'a> FUT: future::Future<Output = Result<usize>> + 'a,
    //FUT: 'static,
{
    // First resolve the instance to a port via the
    // SSRP protocol/MS-SQLR protocol [1]
    // [1] https://msdn.microsoft.com/en-us/library/cc219703.aspx
    let local_bind = if addr.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    };

    let msg = [&[4u8], instance_name.as_bytes()].concat();
    let mut buf = vec![0u8; 4096];

    let len = async {
        let mut recieve = Box::pin(udp_sender(local_bind, &addr, &msg, &mut buf)).fuse();

        let mut timeout = futures_timer::Delay::new(std::time::Duration::from_millis(1000)).fuse();
        let err = |name| format!("SQL browser timeout during resolving instance {}", name).into();

        select! {
            len = recieve => len.map_err(convert::Into::into),
            _ = timeout => Err(error::Error::Conversion(err(instance_name))),
        }
    }.await?;

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

//#[cfg(any(windows, doc))]
pub async fn find_tcp_port_closure<FN, FUT>(
    mut addr: net::SocketAddr, 
    instance_name: &str, 
    udp_sender: FN,
    ) -> Result<net::SocketAddr> 
where
    FN: for<'a> Fn(&'a str, &'a net::SocketAddr, &'a [u8],  &'a mut [u8]) -> FUT,
    FUT: future::Future<Output = Result<usize>>,
{
    // First resolve the instance to a port via the
    // SSRP protocol/MS-SQLR protocol [1]
    // [1] https://msdn.microsoft.com/en-us/library/cc219703.aspx
    let local_bind = if addr.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    };

    let msg = [&[4u8], instance_name.as_bytes()].concat();
    let mut buf = vec![0u8; 4096];

    let len = async {
        let mut recieve = Box::pin(udp_sender(local_bind, &addr, &msg, &mut buf)).fuse();

        let mut timeout = futures_timer::Delay::new(std::time::Duration::from_millis(1000)).fuse();
        let err = |name| format!("SQL browser timeout during resolving instance {}", name).into();

        select! {
            len = recieve => len.map_err(convert::Into::into),
            _ = timeout => Err(error::Error::Conversion(err(instance_name))),
        }
    }.await?;

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

