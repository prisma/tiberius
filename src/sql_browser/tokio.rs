use super::SqlBrowser;
use crate::client::Config;
use async_trait::async_trait;
use futures::TryFutureExt;
use net::{TcpStream, UdpSocket};
use std::{io, net::SocketAddr};
use time::Duration;
use tokio::{net, time};

#[async_trait]
impl SqlBrowser for TcpStream {
    /// This method can be used to connect to SQL Server named instances
    /// when on a Windows paltform with the `sql-browser-tokio` feature
    /// enabled. Please see the crate examples for more detailed examples.
    async fn connect_named(builder: &Config) -> crate::Result<Self> {
        let mut addr: SocketAddr = net::lookup_host(builder.get_addr())
            .await?
            .next()
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, "Could not resolve server host.")
            })?;

        if let Some(ref instance_name) = builder.instance_name {
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

            let socket = UdpSocket::bind(&local_bind).await?;
            socket.send_to(&msg, &addr).await?;

            let timeout = Duration::from_millis(1000);

            let len = time::timeout(timeout, socket.recv(&mut buf))
                .map_err(|_| {
                    crate::error::Error::Conversion(
                        format!(
                            "SQL browser timeout during resolving instance {}",
                            instance_name
                        )
                        .into(),
                    )
                })
                .await??;

            let port = super::get_port_from_sql_browser_reply(buf, len, instance_name)?;
            addr.set_port(port);
        };

        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;

        Ok(stream)
    }
}
