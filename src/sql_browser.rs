#[cfg(feature = "sql-browser-tokio")]
mod tokio;

#[cfg(feature = "sql-browser-async-std")]
mod async_std;

use crate::client::ClientBuilder;
use async_trait::async_trait;

/// An extension trait to a `TcpStream` to find a port and connecting to a
/// named database instance.
///
/// Only needed on Windows platforms, where the server port is not known and the
/// address is in the form of `hostname\\INSTANCE`.
#[async_trait]
pub trait SqlBrowser {
    /// If the given builder defines a named instance, finds the correct port
    /// and returns a `TcpStream` to be used in the [`Client`]. If instance name
    /// is not defined, connects directly to the given host and port.
    ///
    /// [`Client`]: struct.Client.html
    async fn connect_named(builder: &ClientBuilder) -> crate::Result<Self>
    where
        Self: Sized + Send + Sync;
}

#[cfg(any(feature = "sql-browser-async-std", feature = "sql-browser-tokio"))]
fn get_port_from_sql_browser_reply(
    mut buf: Vec<u8>,
    len: usize,
    instance_name: &str,
) -> crate::Result<u16> {
    buf.truncate(len);

    let err = crate::Error::Conversion(
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
