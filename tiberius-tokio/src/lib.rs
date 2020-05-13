use tokio_util::compat::{self, Tokio02AsyncWriteCompatExt};
use tokio::{io, net};


pub async fn connector(addr: String, instance_name: Option<String>) -> tiberius::Result<compat::Compat<net::TcpStream>> 
{
    let mut addr = tokio::net::lookup_host(addr).await?.next().ok_or_else(|| {
        io::Error::new(io::ErrorKind::NotFound, "Could not resolve server host.")
    })?;

    if let Some(ref instance_name) = instance_name {
        addr = tiberius::find_tcp_port(addr, instance_name).await?;
    };
    let stream = net::TcpStream::connect(addr).await?;
    stream.set_nodelay(true)?;
    Ok(stream.compat_write())
}
