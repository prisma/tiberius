use tiberius::{Client, Config};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

#[tokio::test]
async fn login_errors_are_propagated_on_init() -> anyhow::Result<()> {
    let conn_str =
        "server=tcp:localhost,1433;user=SA;password=ObviouslyWrong;TrustServerCertificate=true";

    let config = Config::from_ado_string(conn_str)?;
    let tcp = TcpStream::connect(config.get_addr()).await?;

    tcp.set_nodelay(true)?;

    let res = Client::connect(config, tcp.compat_write()).await;
    assert!(res.is_err());

    let err = res.unwrap_err();
    assert_eq!(Some(18456), err.code());

    Ok(())
}
