use once_cell::sync::Lazy;
use std::env;
use futures::TryStreamExt;

static CONN_STR: Lazy<String> = Lazy::new(|| {
    env::var("TIBERIUS_TEST_CONNECTION_STRING")
        .unwrap_or_else(|_| "server=tcp:localhost,1433;IntegratedSecurity=true;TrustServerCertificate=true".to_owned())
});


#[async_std::main]
async fn main() -> anyhow::Result<()> {
    let config = tiberius::ClientBuilder::from_ado_string(&CONN_STR)?;
    let tcp = async_std::net::TcpStream::connect(config.get_addr()).await?;
    tcp.set_nodelay(true)?;
    let mut client = tiberius::Client::connect(config, tcp).await?;

    let stream = client.query("SELECT @P1", &[&1]).await?;
    let rows: Vec<_> = stream.map_ok(|x| x.get::<_, i32>(0)).try_collect().await?;
    println!("{:?}", rows);
    assert_eq!(1, rows[0]);
    Ok(())
}

//#[cfg(windows)]
mod with_named_instance {
    use futures::future;
    use async_std::net;
    use std::io;
    use async_std::net::ToSocketAddrs;
    
    pub fn connector<'a>(addr: String, instance_name: Option<String>) -> future::BoxFuture<'a, tiberius::Result<net::TcpStream>>
    {
        let stream = async move {
            let mut addr = addr.to_socket_addrs().await?.next().ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, "Could not resolve server host.")
            })?;

            if let Some(ref instance_name) = instance_name {
                addr = tiberius::find_tcp_port(addr, instance_name, udp_sender).await?;
            };

            let stream = net::TcpStream::connect(addr).await?;
            stream.set_nodelay(true)?;
            Ok(stream)
        };
        Box::pin(stream)
    }
 
     async fn udp_sender(
         local_bind: &str, 
         server_addr: &std::net::SocketAddr,
         msg: &[u8], 
         buf: &mut [u8]) -> Result<usize, tiberius::error::Error> {
         let socket: net::UdpSocket = net::UdpSocket::bind(local_bind).await?;
         socket.send_to(&msg, server_addr).await?;
 
         let len = socket.recv(&mut buf).await?;
         Ok(len)
     }
}

 mod with_named_instance_closure {
     use futures::future;
     use async_std::net;
     use std::io;
     use async_std::net::ToSocketAddrs;
     
     pub fn connector<'a>(addr: String, instance_name: Option<String>) -> future::BoxFuture<'a, tiberius::Result<net::TcpStream>>
     {
         let stream = async move {
             let mut addr = addr.to_socket_addrs().await?.next().ok_or_else(|| {
                 io::Error::new(io::ErrorKind::NotFound, "Could not resolve server host.")
             })?;

             let addr = if let Some(ref instance_name) = instance_name {
                 let new_addr = tiberius::find_tcp_port_closure(addr, instance_name, |local_bind, server_addr, msg, buf| async {
                     let socket = net::UdpSocket::bind(&local_bind).await?;
                     socket.send_to(&msg, &server_addr).await?;
 
                     let len = socket.recv(&mut buf).await?;
                     Ok(len)
                 }).await?;
                 new_addr
             } else {
                 addr
             };
 
             let stream = net::TcpStream::connect(addr).await?;
             stream.set_nodelay(true)?;
             Ok(stream)
         };
         Box::pin(stream)
     }
 }
 
