Import-Module 'sqlps'

[reflection.assembly]::LoadWithPartialName("Microsoft.SqlServer.Smo") | Out-Null
[reflection.assembly]::LoadWithPartialName("Microsoft.SqlServer.SqlWmiManagement") | Out-Null

$serverName = $env:COMPUTERNAME
$instanceName = $args[0]
$regInstance = $args[1]

$smo = 'Microsoft.SqlServer.Management.Smo.'
$wmi = new-object ($smo + 'Wmi.ManagedComputer')

# Enable TCP/IP
echo "Enabling TCP/IP"
$uri = "ManagedComputer[@Name='$serverName']/ ServerInstance[@Name='$instanceName']/ServerProtocol[@Name='Tcp']"
$Tcp = $wmi.GetSmoObject($uri)
$Tcp.IsEnabled = $true
$Tcp.alter()
$Tcp

# Enable named pipes
echo "Enabling named pipes"
$uri = "ManagedComputer[@Name='$serverName']/ ServerInstance[@Name='$instanceName']/ServerProtocol[@Name='Np']"
$Np = $wmi.GetSmoObject($uri)
$Np.IsEnabled = $true
$Np.Alter()
$Np

# Set Alias
echo "Setting the alias"
New-Item HKLM:\SOFTWARE\Microsoft\MSSQLServer\Client -Name ConnectTo | Out-Null
Set-ItemProperty -Path HKLM:\SOFTWARE\Microsoft\MSSQLServer\Client\ConnectTo -Name '(local)' -Value "DBMSSOCN,$serverName\$instanceName" | Out-Null

# Generate Certificate & use and trust it
echo "Generating certificate"
$cert = New-SelfSignedCertificate -DnsName $serverName,localhost -CertStoreLocation cert:\LocalMachine\My
Get-Item -Path "HKLM:\SOFTWARE\Microsoft\Microsoft SQL Server\$regInstance\MSSQLServer\SuperSocketNetLib" | New-ItemProperty -Name Certificate -Value $cert.Thumbprint.ToLower() -Force
$rootStore = Get-Item cert:\LocalMachine\Root
$rootStore.Open("ReadWrite")
$rootStore.Add($cert)
$rootStore.Close();

# Bypass certificate (private key) permission errors by simply running as system
# DONT DO THIS IN PRODUCTION OR REAL WORLD USE, assign proper permissions to the private key!
echo "Bypassing certificate errors"
sc.exe config "MSSQL`$$instanceName" obj= "LocalSystem" password= "dummy"

# Start services
echo "Starting services"
Set-Service SQLBrowser -StartupType Manual
Start-Service SQLBrowser
Start-Service "MSSQL`$$instanceName"
# workaround appveyor failure that we first get a "Start-Service : Failed to start service" error
Start-Service "MSSQL`$$instanceName"
