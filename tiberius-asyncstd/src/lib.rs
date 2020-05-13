use async_std::{io, net::{self, ToSocketAddrs}};

pub async fn connector(addr: String, instance_name: Option<String>) -> tiberius::Result<net::TcpStream> 
{
    let mut addr = addr.to_socket_addrs().await?.next().ok_or_else(|| {
        io::Error::new(io::ErrorKind::NotFound, "Could not resolve server host.")
    })?;

    if let Some(ref instance_name) = instance_name {
        addr = tiberius::find_tcp_port(addr, instance_name).await?;
    };

    let stream = net::TcpStream::connect(addr).await?;
    stream.set_nodelay(true)?;
    Ok(stream)
}
