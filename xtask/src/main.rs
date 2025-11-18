use std::{
    env,
    process::{Command, exit},
    thread::sleep,
    time::Duration,
};

fn main() {
    let mut args = env::args().skip(1);
    let cmd = args.next().unwrap_or_default();

    let container_engine = env::var("CONTAINER_ENGINE").unwrap_or_else(|_| "docker".to_string());

    match cmd.as_str() {
        "container" => {
            // need to change unwrap_or_else in document
            // default version is 2019, any opposition?

            // doesn't run tests
            let version = args.next().unwrap_or_else(|| "2019".into());
            start_container(&version, &container_engine);
        }
        "test" => {
            // run the tests
            run_tests(args.collect::<Vec<_>>());
        }
        "local" => {
            // local runs test AND container
            let version = args.next().unwrap_or_else(|| "2019".into());
            start_container(&version, &container_engine);
            // start_container calls wait_for_sql anyway
            run_tests(args.collect::<Vec<_>>());
            stop_container(&version, &container_engine);
        }
        "stop" => {
            // stops running containers
            let version = args.next().unwrap_or_else(|| "2019".into());
            stop_container(&version, &container_engine);
        }
        _ => {
            exit(1);
        }
    }
}

fn start_container(version: &str, container_engine: &str) {
    Command::new("bash")
        .arg("-c")
        .arg("./generate.sh")
        .status()
        .unwrap();

    let sa_password =
        env::var("SA_PASSWORD").unwrap_or_else(|_| "<YourStrong@Passw0rd>".to_string());
    let container_name = format!("mssql-{}", version);

    let dockerfile = format!("docker/docker-mssql-{}.dockerfile", version);
    let image_tag = format!("my-mssql:{}", version);

    println!("Cleaning up existing container, {}", container_name);

    Command::new(container_engine)
        .args(["rm", "-f", &container_name])
        .status()
        .unwrap();

    println!("Building image {} from {}...", image_tag, dockerfile);

    let status = Command::new(container_engine)
        .args(["build", "-f", &dockerfile, "-t", &image_tag, "."])
        .status()
        .expect("Failed to build docker image");

    if !status.success() {
        eprintln!("Docker build failed for {}", version);
        exit(1);
    }

    println!("Starting SQL Server {} container...", version);

    let status = Command::new(container_engine)
        .args([
            "run",
            "-d",
            "--name",
            &container_name,
            "-e",
            "ACCEPT_EULA=Y",
            "-e",
            &format!("MSSQL_SA_PASSWORD={}", sa_password),
            "-e",
            "MSSQL_PID=Developer",
            "-p",
            "1433:1433",
            &image_tag,
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
    println!("Waiting for SQL Server to start. 25 seconds. Do not change or exit. - please.");
    sleep(Duration::from_secs(25));
}

fn stop_container(version: &str, container_engine: &str) {
    let name = format!("mssql-{}", version);
    let _ = Command::new(container_engine)
        .args(["rm", "-f", &name])
        .status();
    println!("Stopped container {}", name);
}

fn run_tests(flags: Vec<String>) {
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
        .args(&flags)
        .status()
        .expect("failed to run cargo test");

    if !status.success() {
        exit(1);
    }
}
