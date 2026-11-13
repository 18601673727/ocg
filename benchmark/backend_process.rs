use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::Value;

#[derive(Clone, Debug)]
pub struct BackendConfig {
    pub data_dir: PathBuf,
    pub workspace: PathBuf,
    pub provider_endpoint: String,
    pub port: u16,
    pub token: String,
    pub concurrency: usize,
    pub startup_timeout: Duration,
    pub shutdown_timeout: Duration,
    pub hang_before_handshake: bool,
}

impl BackendConfig {
    pub fn new(
        data_dir: PathBuf,
        workspace: PathBuf,
        provider_endpoint: String,
        token: String,
        concurrency: usize,
    ) -> Self {
        Self {
            data_dir,
            workspace,
            provider_endpoint,
            port: 0,
            token,
            concurrency,
            startup_timeout: Duration::from_secs(30),
            shutdown_timeout: Duration::from_secs(15),
            hang_before_handshake: false,
        }
    }
}

#[derive(Clone, Debug)]
pub struct BackendHandshake {
    pub pid: u32,
    pub address: String,
    pub token: String,
    pub executor: String,
    pub llm_profile: String,
    pub agent_profile: String,
    pub project: String,
    pub pool: String,
    pub startup: Duration,
}

pub struct BackendProcess {
    owner: ChildOwner,
    address: String,
    token: String,
    pid: u32,
    startup: Duration,
    shutdown_timeout: Duration,
    data_dir: PathBuf,
    handshake: Option<BackendHandshake>,
}

struct ChildOwner {
    child: Child,
}

impl Drop for ChildOwner {
    fn drop(&mut self) {
        let _ = terminate(&mut self.child);
        let _ = self.child.wait();
    }
}

impl BackendProcess {
    pub fn spawn(config: &BackendConfig) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let started = Instant::now();
        let binary = host_binary()?;
        let mut config = config.clone();
        if config.port == 0 {
            config.port = std::net::TcpListener::bind("127.0.0.1:0")?
                .local_addr()?
                .port();
        }
        let config = &config;
        if config.hang_before_handshake {
            std::thread::sleep(config.startup_timeout + Duration::from_secs(1));
            return Err(format!(
                "the backend child did not become ready within {:?}",
                config.startup_timeout
            )
            .into());
        }
        std::fs::create_dir_all(&config.data_dir)?;
        std::fs::create_dir_all(&config.workspace)?;
        if !database_exists(&config.data_dir)? {
            create_database(&binary, config)?;
        }
        let activation = preflight(&binary, config)?;
        let child = serve(&binary, config, &activation)?;
        let pid = child.id();
        let mut owner = ChildOwner { child };
        let address = read_address(&mut owner.child, started, config.startup_timeout, pid)?;
        Ok(Self {
            owner,
            address,
            token: config.token.clone(),
            pid,
            startup: started.elapsed(),
            shutdown_timeout: config.shutdown_timeout,
            data_dir: config.data_dir.clone(),
            handshake: None,
        })
    }

    pub fn attach_handshake(&mut self, handshake: BackendHandshake) {
        self.handshake = Some(handshake);
    }

    pub fn handshake(&self) -> &BackendHandshake {
        self.handshake
            .as_ref()
            .expect("the harness attaches a handshake before using the host")
    }

    pub fn address(&self) -> &str {
        &self.address
    }

    pub fn token(&self) -> &str {
        &self.token
    }

    pub fn pid(&self) -> u32 {
        self.pid
    }

    pub fn startup(&self) -> Duration {
        self.startup
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn shutdown(mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        terminate(&mut self.owner.child)?;
        let deadline = Instant::now() + self.shutdown_timeout;
        loop {
            if let Some(status) = self.owner.child.try_wait()? {
                if status.success() {
                    return Ok(());
                }
                return Err(format!("backend child exited unsuccessfully: {status}").into());
            }
            if Instant::now() >= deadline {
                self.owner.child.kill()?;
                let status = self.owner.child.wait()?;
                return Err(format!(
                    "backend child required forced termination after {:?}; it is not a clean close (status {status})",
                    self.shutdown_timeout
                )
                .into());
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

fn host_binary() -> Result<PathBuf, Box<dyn std::error::Error + Send + Sync>> {
    if let Some(path) = std::env::var_os("OCG_BIN") {
        return Ok(PathBuf::from(path));
    }
    let current = std::env::current_exe()?;
    let directory = current
        .parent()
        .and_then(Path::parent)
        .ok_or("the benchmark executable has no target directory")?;
    let binary = directory.join("ocg");
    if !binary.is_file() {
        return Err(format!(
            "the ocg binary is missing at {}; build it with `cargo build` or set OCG_BIN",
            binary.display()
        )
        .into());
    }
    Ok(binary)
}

fn database_exists(data_dir: &Path) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    if !data_dir.is_dir() {
        return Ok(false);
    }
    for entry in std::fs::read_dir(data_dir)? {
        let entry = entry?;
        if entry.path().extension().is_some_and(|kind| kind == "redb") {
            return Ok(true);
        }
    }
    Ok(false)
}

fn command(binary: &Path, config: &BackendConfig) -> Command {
    let mut command = Command::new(binary);
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    command
        .env("OCG_DATA_DIR", &config.data_dir)
        .env("OCG_CONTROL_PORT", config.port.to_string())
        .env("OCG_CONTROL_TOKEN", &config.token);
    let excluded = match std::env::var("NO_PROXY") {
        Ok(existing) if !existing.trim().is_empty() => format!("{existing},127.0.0.1,localhost"),
        _ => "127.0.0.1,localhost".to_owned(),
    };
    command
        .env("NO_PROXY", &excluded)
        .env("no_proxy", &excluded);
    command
}

fn create_database(
    binary: &Path,
    config: &BackendConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let started = Instant::now();
    let mut child = command(binary, config)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let pid = child.id();
    let address = read_address(&mut child, started, config.startup_timeout, pid)?;
    let _ = address;
    terminate(&mut child)?;
    let status = child.wait()?;
    if !status.success() {
        return Err(format!("the database-creating host exited with {status}").into());
    }
    Ok(())
}

fn preflight(
    binary: &Path,
    config: &BackendConfig,
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    let first = run_cli(binary, config, &["loopback-scheduling", "preflight"])?;
    let identity = first
        .get("database_identity")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("the preflight named no database identity: {first}"))?
        .to_owned();
    let second = run_cli(
        binary,
        config,
        &[
            "loopback-scheduling",
            "preflight",
            "--database",
            identity.as_str(),
        ],
    )?;
    if second.get("verdict").and_then(Value::as_str) != Some("pass") {
        return Err(format!("the activation preflight refused this run: {second}").into());
    }
    if second
        .get("confirmation_challenge")
        .and_then(Value::as_str)
        .is_none()
    {
        return Err(format!("the preflight issued no challenge: {second}").into());
    }
    Ok(second)
}

fn run_cli(
    binary: &Path,
    config: &BackendConfig,
    arguments: &[&str],
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    let output = command(binary, config)
        .args(arguments)
        .arg("--data-dir")
        .arg(&config.data_dir)
        .stdin(Stdio::null())
        .output()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let trimmed = stdout.trim();
    if trimmed.is_empty() {
        return Err(format!(
            "`{}` printed nothing (status {}): {}",
            arguments.join(" "),
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    serde_json::from_str(trimmed)
        .map_err(|error| format!("`{}` printed no JSON: {error}", arguments.join(" ")).into())
}

fn serve(
    binary: &Path,
    config: &BackendConfig,
    activation: &Value,
) -> Result<Child, Box<dyn std::error::Error + Send + Sync>> {
    let identity = activation["requested"]["database_identity"]
        .as_str()
        .or_else(|| activation["database_identity"].as_str())
        .ok_or("the preflight report carries no database identity")?;
    let challenge = activation["confirmation_challenge"]
        .as_str()
        .ok_or("the preflight report carries no challenge")?;
    let child = command(binary, config)
        .args([
            "loopback-scheduling",
            "serve",
            "--data-dir",
            &config.data_dir.to_string_lossy(),
            "--database",
            identity,
            "--confirm-first-activation",
            "--confirm-loopback-only",
            "--confirm-challenge",
            challenge,
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    Ok(child)
}

fn terminate(child: &mut Child) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    #[cfg(unix)]
    {
        let status = unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGTERM) };
        if status != 0 {
            return Err(format!(
                "could not signal the backend child: {}",
                std::io::Error::last_os_error()
            )
            .into());
        }
        Ok(())
    }
    #[cfg(not(unix))]
    {
        child.kill()?;
        Ok(())
    }
}

fn read_address(
    child: &mut Child,
    started: Instant,
    timeout: Duration,
    pid: u32,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let (sender, receiver) = std::sync::mpsc::sync_channel(4);
    let stderr = child
        .stderr
        .take()
        .ok_or("the backend child has no standard error")?;
    let stdout = child
        .stdout
        .take()
        .ok_or("the backend child has no standard output")?;
    for stream in [
        Box::new(stdout) as Box<dyn std::io::Read + Send>,
        Box::new(stderr),
    ] {
        let sender = sender.clone();
        std::thread::spawn(move || {
            let mut reader = BufReader::new(stream);
            loop {
                let mut line = String::new();
                match reader.read_line(&mut line) {
                    Ok(0) => return,
                    Ok(_) => {
                        if sender.send(line).is_err() {
                            return;
                        }
                    }
                    Err(error) => {
                        let _ = sender.send(format!("stream read failed: {error}\n"));
                        return;
                    }
                }
            }
        });
    }
    drop(sender);
    let deadline = started + timeout;
    loop {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or_else(|| {
                format!("the backend child {pid} did not become ready within {timeout:?}")
            })?;
        match receiver.recv_timeout(remaining) {
            Ok(line) => {
                let trimmed = line.trim();
                if let Some(address) = listening_address(trimmed) {
                    return Ok(address);
                }
                if !trimmed.is_empty() {
                    eprintln!("backend {pid}: {trimmed}");
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                return Err(format!(
                    "the backend child {pid} did not become ready within {timeout:?}"
                )
                .into());
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                return Err(format!("the backend child {pid} exited before it was ready").into());
            }
        }
    }
}

fn listening_address(line: &str) -> Option<String> {
    let (_, rest) = line.split_once("listening on ")?;
    let address = rest.trim();
    address.starts_with("http").then(|| address.to_owned())
}

#[allow(dead_code)]
fn drain_until_exit(mut child: Child) {
    let mut stdin = child.stdin.take();
    if let Some(stdin) = stdin.as_mut() {
        let _ = stdin.write_all(b"\n");
    }
}
