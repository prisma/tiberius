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


#[cfg(all(windows, feature = "named-instance-tokio"))]
impl client::ClientBuilder {

    pub async fn connect<'a>(&self) -> tiberius::Result<compat::Compat<net::TcpStream>>
    {
        use tokio::{net, time};
        use tokio_util::compat::{self, Tokio02AsyncWriteCompatExt};
        use futures::{future, TryFutureExt};
        use std::io;

        let mut addr = tokio::net::lookup_host(addr).await?.next().ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, "Could not resolve server host.")
        })?;

        if let Some(ref instance_name) = self.instance_name {
            // First resolve the instance to a port via the
            // SSRP protocol/MS-SQLR protocol [1]
            // [1] https://msdn.microsoft.com/en-us/library/cc219703.aspx

            let local_bind: std::net::SocketAddr = if addr.is_ipv4() {
                "0.0.0.0:0".parse().unwrap()
            } else {
                "[::]:0".parse().unwrap()
            };

            let msg = [&[4u8], instance_name.as_bytes()].concat();
            let mut buf = vec![0u8; 4096];

            let mut socket = net::UdpSocket::bind(&local_bind).await?;
            socket.send_to(&msg, &addr).await?;

            let timeout = time::Duration::from_millis(1000);

            let len = time::timeout(timeout, socket.recv(&mut buf))
                .map_err(|_: time::Elapsed| {
                    tiberius::error::Error::Conversion(
                        format!(
                            "SQL browser timeout during resolving instance {}",
                            instance_name
                        )
                        .into(),
                    )
                }).await??;

            tiberius::consume_sql_browser_message(addr, buf, len, instance_name)
        };
        let stream = net::TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Ok(stream.compat_write())
    }
}


#[cfg(all(windows, feature = "named-instance-async"))]
impl client::ClientBuilder {
    
    pub async fn connector<'a>(&self) -> crate::Result<net::TcpStream>
    {
        use futures::{future, TryFutureExt};
        use async_std::net;
        use std::{io, time};

        let mut addr = addr.to_socket_addrs().await?.next().ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, "Could not resolve server host.")
        })?;

        if let Some(ref instance_name) = self.instance_name {
            // First resolve the instance to a port via the
            // SSRP protocol/MS-SQLR protocol [1]
            // [1] https://msdn.microsoft.com/en-us/library/cc219703.aspx

            let local_bind: std::net::SocketAddr = if addr.is_ipv4() {
                "0.0.0.0:0".parse().unwrap()
            } else {
                "[::]:0".parse().unwrap()
            };

            let msg = [&[4u8], instance_name.as_bytes()].concat();
            let mut buf = vec![0u8; 4096];

            let socket = net::UdpSocket::bind(&local_bind).await?;
            socket.send_to(&msg, &addr).await?;

            let timeout = time::Duration::from_millis(1000);

            let len = async_std::io::timeout(timeout, socket.recv(&mut buf))
                .map_err(|_| {
                    tiberius::error::Error::Conversion(
                        format!(
                            "SQL browser timeout during resolving instance {}",
                            instance_name
                        )
                        .into(),
                    )
                }).await?;

            tiberius::consume_sql_browser_message(addr, buf, len, instance_name)
        };

        let stream = net::TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Ok(stream)
    }
}

#[cfg(windows)]
fn consume_sql_browser_message(mut addr: net::SocketAddr, mut buf: Vec<u8>, len: usize, instance_name: &str) -> crate::Result<net::SocketAddr> {
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
