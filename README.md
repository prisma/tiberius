# Tiberius
[![crates.io](https://meritbadge.herokuapp.com/tiberius)](https://crates.io/crates/tiberius)
[![docs.rs](https://docs.rs/tiberius/badge.svg)](https://docs.rs/tiberius)
[![Build status](https://badge.buildkite.com/172053d935f64a275beca911ab20bad34e7597775ce024469d.svg)](https://buildkite.com/prisma/tiberius)
[![Build status](https://ci.appveyor.com/api/projects/status/vr39e8qd42n3yf0i/branch/master?svg=true)](https://ci.appveyor.com/project/pimeys/tiberius/branch/master)
[![Chat](https://img.shields.io/discord/664092374359605268)](https://discord.gg/xX4xp9x)

A native Microsoft SQL Server (TDS) client for Rust.

### Supported SQL Server versions

| Version | Support level | Notes                               |
|---------|---------------|-------------------------------------|
|    2019 | Tested on CI  |                                     |
|    2017 | Tested on CI  |                                     |
|    2016 | Should work   |                                     |
|    2014 | Should work   |                                     |
|    2012 | Should work   |                                     |
|    2008 | Should work   |                                     |
|    2005 | Should work   | With feature flag `tds73` disabled. |

### Documentation

- [Master](https://prisma.github.io/tiberius/tiberius/index.html)
- [Released](https://docs.rs/tiberius)

The system should work with the Docker and Azure versions of SQL Server without
trouble. For installing with Windows systems, please don't forget the following
things:

### Feature flags

| Flag           | Description                                                                           | Default    |
|----------------|---------------------------------------------------------------------------------------|------------|
| `tds73`        | Support for new date and time types in TDS version 7.3. Disable if using version 7.2. | `enabled`  |
| `chrono`       | Read and write date and time values using `chrono`'s types.                           | `disabled` |
| `rust_decimal` | Read and write `numeric`/`decimal` values using `rust_decimal`'s `Decimal`.           | `disabled` |
| `bigdecimal`   | Read and write `numeric`/`decimal` values using `bigdecimal`'s `BigDecimal`.          | `disabled` |
| `sql-browser-async-std` | SQL Browser implementation for the `TcpStream` of async-std.                 | `disabled` |
| `sql-browser-tokio`     | SQL Browser implementation for the `TcpStream` of Tokio.                     | `disabled` |
| `integrated-auth-gssapi`     | Support for using Integrated Auth via GSSAPI                            | `disabled` |
| `vendored-openssl` | On Linux and macOS platforms links statically against a vendored version of OpenSSL | `disabled` |

### Supported protocols

Tiberius does not rely on any protocol when connecting to an SQL Server instance. Instead the `Client` takes a socket that implements the `AsyncRead` and `AsyncWrite` traits from the [futures-rs](https://crates.io/crates/futures) crate.

Currently there are good async implementations for TCP in the [async-std](https://crates.io/crates/async-std), [Tokio](https://crates.io/crates/tokio) and [Smol](https://crates.io/crates/smol) projects. To be able to use them together with Tiberius on Windows platforms with SQL Server, the TCP should be enabled in the [server settings](https://technet.microsoft.com/en-us/library/hh231672(v=sql.110).aspx) (disabled by default). In the offficial [Docker image](https://hub.docker.com/_/microsoft-mssql-server) TCP is is enabled by default.

To use named pipes, [Miow](https://crates.io/crates/miow) provides the `NamedPipe` that implements sync `Read` and `Write` traits. With some extra work one could write a crate that implements the `AsyncRead` and `AsyncWrite` traits to it. When this happens, Tiberius will support named pipes without any changes to the crate code.

The shared memory protocol is not documented and seems there is no Rust crates implementing it.

### Encryption (TLS/SSL)

#### a) Make sure to use a trusted certificate

Make sure the certificate your using is trusted by your local machine. To create a self-signed certificate that is trusted you can use the following powershell:

```powershell
$cert = New-SelfSignedCertificate -DnsName $serverName,localhost -CertStoreLocation cert:\LocalMachine\My
$rootStore = Get-Item cert:\LocalMachine\Root
$rootStore.Open("ReadWrite")
$rootStore.Add($cert)
$rootStore.Close();
```

You also have to [change the certificate in the SQL Server settings](https://support.microsoft.com/en-us/help/316898/how-to-enable-ssl-encryption-for-an-instance-of-sql-server-by-using-microsoft-management-console).  
In a production setting you likely want to use a certificate that is issued by a
CA.

#### b) Disable certificate validation by using `TrustServerCertificate=true` in your connection string (requires 0.2.2)

#### c) Alternatively: Disable Encryption for LOCALHOST
For a connection to localhost, which will never leave your machine, it's safe to disable encryption. Add `encrypt=DANGER_PLAINTEXT` to your connection string or set use the `EncryptionLevel::NotSupported` variant.

```toml
tiberius = { version = "0.X", default-features=false, features=["chrono"] }
```

#### MacOS Catalina and TLS

Some SQL Server databases, such as the public Docker image use a TLS certificate not accepted by Apple's Secure Transport. Therefore on macOS systems we use OpenSSL instead of Secure Transport, meaning by default Tiberius requires a working OpenSSL installation. By using a feature flag `vendored-openssl` the compilation links statically to a vendored version of OpenSSL, allowing compilation on systems with no OpenSSL installed.

Please be aware of the security implications if deciding to use vendoring.

### Integrated Authentication (TrustedConnection) on \*nix

With the `integrated-auth-gssapi` feature enabled, the crate requires the GSSAPI/Kerberos libraries/headers installed:
  * [CentOS](https://pkgs.org/download/krb5-devel)
  * [Arch](https://www.archlinux.org/packages/core/x86_64/krb5/)
  * [Debian](https://tracker.debian.org/pkg/krb5) (you need the -dev packages to build)
  * [Ubuntu](https://packages.ubuntu.com/bionic-updates/libkrb5-dev)
  * Mac: as of version `0.4.2` the [libgssapi](https://crates.io/crates/libgssapi) crate used for this feature now uses Apple's [GSS Framework](https://developer.apple.com/documentation/gss?language=objc) which ships with MacOS 10.14+.

Additionally, your runtime system will need to be trusted by and configured for the Active Directory domain your SQL Server is part of. In particular, you'll need to be able to get a valid TGT for your identity, via `kinit` or a keytab. This setup varies by environment and OS, but your friendly network/system administrator should be able to help figure out the specifics.

## Redirects

With certain Azure firewall settings, a login might return `Error::Routing { host, port }`. This means the user must create a new `TcpStream` to the given address, and connect again.

A simple connection procedure would then be:

```rust
use tiberius::{Client, Config, AuthMethod, error::Error};
use tokio_util::compat::TokioAsyncWriteCompatExt;
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = Config::new();

    config.host("0.0.0.0");
    config.port(1433);
    config.authentication(AuthMethod::sql_server("SA", "<Mys3cureP4ssW0rD>"));

    let tcp = TcpStream::connect(config.get_addr()).await?;
    tcp.set_nodelay(true)?;

    let client = match Client::connect(config, tcp.compat_write()).await {
        // Connection successful.
        Ok(client) => client,
        // The server wants us to redirect to a different address
        Err(Error::Routing { host, port }) => {
            let mut config = Config::new();

            config.host(&host);
            config.port(port);
            config.authentication(AuthMethod::sql_server("SA", "<Mys3cureP4ssW0rD>"));

            let tcp = TcpStream::connect(config.get_addr()).await?;
            tcp.set_nodelay(true)?;

            // we should not have more than one redirect, so we'll short-circuit here.
            Client::connect(config, tcp.compat_write()).await?
        }
        Err(e) => Err(e)?,
    };

    Ok(())
}
```

## Security

If you have a security issue to report, please contact us at [security@prisma.io](mailto:security@prisma.io?subject=[GitHub]%20Prisma%202%20Security%20Report%20Tiberius)
