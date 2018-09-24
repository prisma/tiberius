# Tiberius
[![Build status](https://ci.appveyor.com/api/projects/status/ei34it5ppntytrev/branch/master?svg=true)](https://ci.appveyor.com/project/steffengy/tiberius/branch/master)
[![Build Status](https://travis-ci.org/steffengy/tiberius.svg?branch=master)](https://travis-ci.org/steffengy/tiberius)
[![Documentation](https://docs.rs/tiberius/badge.svg)](https://docs.rs/tiberius)  
A native TDS (MSSQL) 7.4 driver for Rust

[Documentation](https://docs.rs/tiberius) 

This is the new async rewrite, please check the documentation above for any examples.

For installing please don't forget the following things:

### Enable TCP for SQL Server
As of now only TCP is supported, which is **disabled by default**.  
Make sure to enable TCP in your [MSSQL settings](https://technet.microsoft.com/en-us/library/hh231672(v=sql.110).aspx).

### Encryption (TLS/SSL)

#### a) Make sure to use a trusted certificate
Make sure the certificate your using is trusted by your local machine.  
To create a self-signed certificate that is trusted you can use the following powershell:
```powershell
$cert = New-SelfSignedCertificate -DnsName $serverName,localhost -CertStoreLocation cert:\LocalMachine\My
$rootStore = Get-Item cert:\LocalMachine\Root
$rootStore.Open("ReadWrite")
$rootStore.Add($cert)
$rootStore.Close();
```
You also have to [change the certificate in the SQL Server settings](https://support.microsoft.com/en-us/help/316898/how-to-enable-ssl-encryption-for-an-instance-of-sql-server-by-using-microsoft-management-console).  
In a production setting you likely want to use a certificate that is issued by a CA.

#### b) Disable certificate validation by using `TrustServerCertificate=true` in your connection string (requires 0.2.2)

#### c) Alternatively: Disable Encryption for LOCALHOST
For a connection to localhost, which will never leave your machine, it's safe to disable encryption.
Currently this is only possible by doing someting like the following in your `cargo.toml`:
```toml
tiberius = { version = "0.X", default-features=false,features=["chrono"] }
```
**This will disable encryption for your ENTIRE crate**  

### Securing Windows Authentication over TCP (non-localhost)
To ensure `Windows-Authentication` is secure, enable `Extended-Protection`.  
Channel-Bindings only work when `Force Encryption` and `Extended Protection`  
are enabled in the [SQL Server Settings as described here](https://docs.microsoft.com/en-us/sql/database-engine/configure-windows/connect-to-the-database-engine-using-extended-protection).  
It also leads to SPN's being used, which makes replay attacks harder.  
Not supported yet.  

### SQL Type Mappings
Any nullable type should be accessed as `Option<T>` where T is any Rust Type listed below.
This table unfortunately still is very incomplete, if you have a question about a specific
type please create an issue, which will make this table grow.

|SQL Type|Rust Type|Feature|
|--------|--------|-------|
|NVARCHAR, BigVarChar|&str|
|uniqueidentifier|tiberius::ty::Guid|
|DATETIME|tiberius::ty::DateTime
|DATETIME2|tiberius::ty::DateTime2
|DATETIME,DATETIME2|chrono::NaiveDateTime|chrono|
Support for versions below 7.4 (to 7.2 so that everything >= SQL Server 2008 works) is desired.

### Supported Connection String Parameters

Not every parameter is supported yet. Below is a list of all parameters supported.

|Parameters|Description|
|--------|--------|
|server|The name or network address of the instance of SQL Server to which to connect. The port number can be specified after the server name.|
|integratedsecurity|Toggle between Windows authentication and SQL authentication.|
|uid, username, user|The SQL Server login account.|
|password, pwd|The password for the SQL Server account logging on.|
|database|The name of the database.|
|trustservercertificate|Specifies whether the driver trusts the server certificate when connecting using TLS.|
|encrypt|Specifies whether the driver uses TLS to encrypt communication.|


## Old State (v0.1)
The old state can be found [in the "old" branch (click me)](https://github.com/steffengy/tiberius/tree/old)
