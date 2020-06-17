# Tiberius
[![crates.io](https://meritbadge.herokuapp.com/tiberius)](https://crates.io/crates/tiberius)
[![docs.rs](https://docs.rs/tiberius/badge.svg)](https://docs.rs/tiberius)
[![Build status](https://badge.buildkite.com/172053d935f64a275beca911ab20bad34e7597775ce024469d.svg)](https://buildkite.com/prisma/tiberius)
[![Build status](https://ci.appveyor.com/api/projects/status/vr39e8qd42n3yf0i/branch/master?svg=true)](https://ci.appveyor.com/project/pimeys/tiberius/branch/master)
[![Chat](https://img.shields.io/discord/664092374359605268)](https://discord.gg/8sF4ej)

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
| `tls`          | Enables TLS support.                                                                  | `enabled`  |
| `tds73`        | Support for new date and time types in TDS version 7.3. Disable if using version 7.2. | `enabled`  |
| `rust_decimal` | Read and write `numeric`/`decimal` values using `rust_decimal`'s `Decimal`.           | `disabled` |
| `sql-browser-async-std` | SQL Browser implementation for the `TcpStream` of async-std.                  | `disabled` |
| `sql-browser-tokio`     | SQL Browser implementation for the `TcpStream` of Tokio.                      | `disabled` |

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
For a connection to localhost, which will never leave your machine, it's safe to disable encryption. Currently this is only possible by doing someting like the following in your `cargo.toml`:

```toml
tiberius = { version = "0.X", default-features=false, features=["chrono"] }
```

**This will disable encryption for your ENTIRE crate**  

## Security

If you have a security issue to report, please contact us at [security@prisma.io](mailto:security@prisma.io?subject=[GitHub]%20Prisma%202%20Security%20Report%20Tiberius)
