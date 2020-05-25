
use tokio_util::compat::Tokio02AsyncWriteCompatExt;
use once_cell::sync::Lazy;
use std::env;
use futures::TryStreamExt;

static CONN_STR: Lazy<String> = Lazy::new(|| {
    env::var("TIBERIUS_TEST_CONNECTION_STRING")
        .unwrap_or_else(|_| "server=tcp:localhost,1433;IntegratedSecurity=true;TrustServerCertificate=true".to_owned())
});


#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = tiberius::ClientBuilder::from_ado_string(&CONN_STR)?;
    let tcp = tokio::net::TcpStream::connect(config.get_addr()).await?;
    tcp.set_nodelay(true)?;
    let mut client = tiberius::Client::connect(config, tcp.compat_write()).await?;

    let stream = client.query("SELECT @P1", &[&1]).await?;
    let rows: Vec<_> = stream.map_ok(|x| x.get::<_, i32>(0)).try_collect().await?;
    println!("{:?}", rows);
    assert_eq!(1, rows[0]);
    Ok(())
}


#[cfg(windows)]
mod with_named_instance {
    use tokio::net;
    use tokio_util::compat::{self, Tokio02AsyncWriteCompatExt};
    use futures::future;
    use std::io;

    pub fn connector<'a>(addr: String, instance_name: Option<String>) -> future::BoxFuture<'a, tiberius::Result<compat::Compat<net::TcpStream>>>
    {
        let stream = async move {
            let mut addr = tokio::net::lookup_host(addr).await?.next().ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, "Could not resolve server host.")
            })?;

            if let Some(ref instance_name) = instance_name {
                addr = find_tcp_port(addr, instance_name).await?;
            };
            let stream = net::TcpStream::connect(addr).await?;
            stream.set_nodelay(true)?;
            Ok(stream.compat_write())
        };
        Box::pin(stream)
    }

    async fn find_tcp_port(addr: std::net::SocketAddr, instance_name: &str) -> tiberius::Result<std::net::SocketAddr> {
        use tokio::time;
        use futures::TryFutureExt;
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
    }
}
