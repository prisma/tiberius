use std::{io, net::{self, ToSocketAddrs}};

pub async fn connector(addr: String, instance_name: Option<String>) -> tiberius::Result<smol::Async<net::TcpStream>> 
{
    let mut addr = addr.to_socket_addrs()?.next().ok_or_else(|| {
        io::Error::new(io::ErrorKind::NotFound, "Could not resolve server host.")
    })?;

    if let Some(ref instance_name) = instance_name {
        addr = tiberius::find_tcp_port(addr, instance_name).await?;
    };

    // connection might block, but this is required to allow set_nodelay(true)
    let stream = net::TcpStream::connect(addr)?;
    stream.set_nodelay(true)?;

    Ok(smol::Async::new(stream)?)
}

