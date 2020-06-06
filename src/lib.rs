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


#[cfg(any(doc, all(windows, feature = "named-instance-tokio")))]
mod named_instance_tokio {
    use tokio::{net, time};
    use tokio_util::compat::{self, Tokio02AsyncWriteCompatExt};
    use futures::TryFutureExt;
    use std::io;

    impl crate::client::ClientBuilder {

        /// This method can be used to connect to SQL Server named instances
        /// when on a Windows paltform with the `named-instance-async` feature
        /// enabled. Please see the crate examples for more detailed examples.
        ///
        /// # Example
        ///
        /// ```rust,ignore
        /// # #[tokio::main]
        /// # async fn main() -> Result<(),  Box<dyn std::error::Error>> {
        /// # let conn_str = std::env::var("TIBERIUS_TEST_CONNECTION_STRING")
        /// #   .unwrap_or_else(|_| "server=tcp:localhost,1433;IntegratedSecurity=true;TrustServerCertificate=true".to_owned());
        /// # let instance_name = std::env::var("TIBERIUS_TEST_INSTANCE").unwrap_or("MSSQLSERVER".to_owned());
        /// # let conn_str = conn_str.replace(",1433", &format!("\\{}", instance_name));
        /// let config = tiberius::ClientBuilder::from_ado_string(&conn_str)?;
        /// let tcp = config.connect_tokio().await?;
        /// # #[allow(unused_mut)]
        /// let mut client = tiberius::Client::connect(config, tcp).await?;
        /// # Ok(())
        /// # }
        ///
        /// ```
        pub async fn connect_tokio<'a>(&self) -> crate::Result<compat::Compat<net::TcpStream>>
        {
            let mut addr: std::net::SocketAddr = net::lookup_host(self.get_addr()).await?.next().ok_or_else(|| {
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
                        crate::error::Error::Conversion(
                            format!(
                                "SQL browser timeout during resolving instance {}",
                                instance_name
                            )
                            .into(),
                        )
                    }).await??;

                let port = crate::get_port_from_sql_browser_reply(buf, len, instance_name)?;
                addr.set_port(port);
            };
            let stream = net::TcpStream::connect(addr).await?;
            stream.set_nodelay(true)?;
            Ok(stream.compat_write())
        }
    }
}


#[cfg(any(doc, all(windows, feature = "named-instance-async")))]
mod named_instance_async {
    use futures::TryFutureExt;
    use async_std::{io, net::{self, ToSocketAddrs}};
    use std::time;
    
    impl crate::client::ClientBuilder {
        /// This method can be used to connect to SQL Server named instances
        /// when on a Windows paltform with the `named-instance-async` feature
        /// enabled. Please see the crate examples for more detailed examples.
        ///
        /// # Example
        ///
        /// ```rust,ignore
        /// # #[async_std::main]
        /// # async fn main() -> Result<(),  Box<dyn std::error::Error>> {
        /// # let conn_str = std::env::var("TIBERIUS_TEST_CONNECTION_STRING")
        /// #   .unwrap_or_else(|_| "server=tcp:localhost,1433;IntegratedSecurity=true;TrustServerCertificate=true".to_owned());
        /// # let instance_name = std::env::var("TIBERIUS_TEST_INSTANCE").unwrap_or("MSSQLSERVER".to_owned());
        /// # let conn_str = conn_str.replace(",1433", &format!("\\{}", instance_name));
        /// let config = tiberius::ClientBuilder::from_ado_string(&conn_str)?;
        /// let tcp = config.connect_async().await?;
        /// # #[allow(unused_mut)]
        /// let mut client = tiberius::Client::connect(config, tcp).await?;
        /// # Ok(())
        /// # }
        ///
        /// ```
        pub async fn connect_async<'a>(&self) -> crate::Result<net::TcpStream>
        {

            let mut addr = self.get_addr().to_socket_addrs().await?.next().ok_or_else(|| {
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

                let len = io::timeout(timeout, socket.recv(&mut buf))
                    .map_err(|_| {
                        crate::error::Error::Conversion(
                            format!(
                                "SQL browser timeout during resolving instance {}",
                                instance_name
                            )
                            .into(),
                        )
                    }).await?;

                let port = crate::get_port_from_sql_browser_reply(buf, len, instance_name)?;
                addr.set_port(port);
            };

            let stream = net::TcpStream::connect(addr).await?;
            stream.set_nodelay(true)?;
            Ok(stream)
        }
    }
}

#[cfg(any(doc, all(windows, any(feature = "named-instance-async", feature = "named-instance-tokio"))))]
fn get_port_from_sql_browser_reply(mut buf: Vec<u8>, len: usize, instance_name: &str) -> crate::Result<u16> {
    buf.truncate(len);

    let err = Error::Conversion(
        format!("Could not resolve SQL browser instance {}", instance_name).into(),
    );

    if len == 0 {
        return Err(err);
    }

    let response = std::str::from_utf8(&buf[3..len])?;

    let port: u16 = response
        .find("tcp;")
        .and_then(|pos| response[pos..].split(';').nth(1))
        .ok_or(err)
        .and_then(|val| Ok(val.parse()?))?;
    Ok(port)
}
