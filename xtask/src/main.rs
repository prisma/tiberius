use std::{
    env,
    process::{Command, exit},
    thread::sleep,
    time::Duration,
};

fn main() {
    let mut args = env::args().skip(1);
    let cmd = args.next().unwrap_or_default();

    match cmd.as_str() {
        "container" => {
            // need to change unwrap_or_else in document
            // default version is 2019, any opposition?

            // doesn't run tests
            let version = args.next().unwrap_or_else(|| "2019".into());
            start_container(&version);
        }
        "test" => {
            // run the tests
            run_tests(args.collect::<Vec<_>>());
        }
        "local" => {
            // local runs test AND container
            let version = args.next().unwrap_or_else(|| "2019".into());
            start_container(&version);
            // start_container calls wait_for_sql anyway
            // so we don't need to call the below line:
            // wait_for_sql();
            run_tests(vec![]);
            stop_container(&version);
        }
        "stop" => {
            // stops running containers
            let version = args.next().unwrap_or_else(|| "2019".into());
            stop_container(&version);
        }
        _ => {
            // eprintln!("Usage: cargo xtask <container|stop|test|local> [args]");
            exit(1);
        }
    }
}

fn start_container(version: &str) {
    let sa_password =
        env::var("SA_PASSWORD").unwrap_or_else(|_| "<YourStrong@Passw0rd>".to_string());
    let container_name = format!("mssql-{}", version);
    let image_tag = match version {
        "2017" => "mcr.microsoft.com/mssql/server:2017-latest",
        "2019" => "mcr.microsoft.com/mssql/server:2019-latest",
        "2022" => "mcr.microsoft.com/mssql/server:2022-latest",
        "azure" => "mcr.microsoft.com/azure-sql-edge",
        _ => panic!("Unsupported version, {}", version),
    };

    println!("Cleaning up existing container, {}", container_name);

    let _ = Command::new("docker")
        .args(["rm", "-f", &container_name])
        .status();

    println!("Starting SQL Server {} container...", version);

    let status = Command::new("docker")
        .args([
            "run",
            "-d",
            "--name",
            &container_name,
            "-e",
            "ACCEPT_EULA=Y",
            "-e",
            &format!("SA_PASSWORD={}", sa_password),
            "-p",
            "1433:1433",
            image_tag,
        ])
        .status()
        .expect("Failed to run docker");

    if !status.success() {
        eprintln!("Failed to start container, {}", version);
        exit(1);
    }

    println!("Started container: {}", container_name);
    wait_for_sql();
}

fn wait_for_sql() {
    println!("Waiting for SQL Server to start. 25 seconds. Do not change or exit.");
    sleep(Duration::from_secs(25));
}

fn stop_container(version: &str) {
    let name = format!("mssql-{}", version);
    let _ = Command::new("docker").args(["rm", "-f", &name]).status();
    println!("Stopped container {}", name);
}

fn run_tests(_flags: Vec<String>) {
    let sa_password =
        env::var("SA_PASSWORD").unwrap_or_else(|_| "<YourStrong@Passw0rd>".to_string());
    let connection_string = env::var("TIBERIUS_TEST_CONNECTION_STRING").unwrap_or_else(|_| {
        format!(
            "server=tcp:localhost,1433;user=sa;password={};TrustServerCertificate=true",
            sa_password
        )
    });

    // for debugging: println!("Running tests with connection {}", connection_string);

    let status = Command::new("cargo")
        .arg("test")
        .env("TIBERIUS_TEST_CONNECTION_STRING", &connection_string)
        .status()
        .expect("failed to run cargo test");

    if !status.success() {
        exit(1);
    }
}
