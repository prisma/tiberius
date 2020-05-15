use tokio_util::compat::{self, Tokio02AsyncWriteCompatExt};
use tokio::{io, net};
#[cfg(windows)]
use tokio::time;
#[cfg(windows)]
use futures::TryFutureExt;
#[cfg(windows)]
use std::str;

pub async fn connector(addr: String, instance_name: Option<String>) -> tiberius::Result<compat::Compat<net::TcpStream>> 
{
    let mut addr = tokio::net::lookup_host(addr).await?.next().ok_or_else(|| {
        io::Error::new(io::ErrorKind::NotFound, "Could not resolve server host.")
    })?;

    if let Some(ref instance_name) = instance_name {
        addr = find_tcp_port(addr, instance_name).await?;
    };
    let stream = net::TcpStream::connect(addr).await?;
    stream.set_nodelay(true)?;
    Ok(stream.compat_write())
}

pub type Client = tiberius::Client<compat::Compat<net::TcpStream>>;

/// This feature is not used on platforms other than Windows
#[cfg(not(windows))]
async fn find_tcp_port(addr: std::net::SocketAddr, _: &str) -> tiberius::Result<std::net::SocketAddr> {
    Ok(addr)
}

/// Use the SQL Browser to find the correct TCP port for the server
/// instance.
#[cfg(windows)]
async fn find_tcp_port(addr: std::net::SocketAddr, instance_name: &str) -> tiberius::Result<std::net::SocketAddr> {
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
            tiberius::Error::Conversion(
                format!(
                    "SQL browser timeout during resolving instance {}",
                    instance_name
                )
                .into(),
            )
        }).await??;

    tiberius::consume_sql_browser_message(addr, buf, len, instance_name)
}

