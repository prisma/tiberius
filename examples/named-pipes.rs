#[cfg(windows)]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    use tokio::net::windows::named_pipe::NamedPipe;
    use tokio_util::compat::TokioAsyncWriteCompatExt;
    use tiberius::{Config, AuthMethod, Client};

    let mut config = Config::new();
    config.authentication(AuthMethod::Integrated);
    config.trust_cert();

    let pipe = NamedPipe::connect(r#"\\.\pipe\sql\query"#).await?;
    let mut client = Client::connect(config, pipe.compat_write()).await?;

    let stream = client.query("SELECT @P1", &[&1i32]).await?;
    let row = stream.into_row().await?.unwrap();

    println!("{:?}", row);
    assert_eq!(Some(1), row.get(0));

    Ok(())
}

#[cfg(not(windows))]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    panic!("Only works on Windows.");
}
