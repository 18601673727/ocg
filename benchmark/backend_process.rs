use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
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
    pub address: String,
    pub token: String,
    pub executor: String,
    pub llm_profile: String,
    pub agent_profile: String,
    pub project: String,
    pub pool: String,
    pub startup: Duration,
}

impl BackendHandshake {
    fn parse(line: &str, startup: Duration) -> Result<Self, String> {
        let value: Value = serde_json::from_str(line)
            .map_err(|error| format!("the backend handshake is not JSON: {error}"))?;
        let field = |name: &str| {
            value
                .get(name)
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .ok_or_else(|| format!("the backend handshake has no {name}"))
        };
        let handshake = Self {
            address: field("address")?,
            token: field("token")?,
            executor: field("executor")?,
            llm_profile: field("llm_profile")?,
            agent_profile: field("agent_profile")?,
            project: field("project")?,
            pool: field("pool")?,
            startup,
        };
        if !handshake.address.starts_with("http://") && !handshake.address.starts_with("https://") {
            return Err(format!("invalid backend address: {}", handshake.address));
        }
        Ok(handshake)
    }
}

struct ChildOwner {
    child: Child,
}

impl Drop for ChildOwner {
    fn drop(&mut self) {
        if !matches!(self.child.try_wait(), Ok(Some(_))) {
            eprintln!(
                "forcing cleanup of benchmark backend child {}",
                self.child.id()
            );
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
    }
}

pub struct BackendProcess {
    owner: ChildOwner,
    handshake: BackendHandshake,
    pid: u32,
    shutdown_timeout: Duration,
}

impl BackendProcess {
    pub fn spawn(config: &BackendConfig) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let executable = std::env::current_exe()?;
        let started = Instant::now();
        let child = Command::new(executable)
            .arg("server")
            .arg("--data-dir")
            .arg(&config.data_dir)
            .arg("--workspace")
            .arg(&config.workspace)
            .arg("--provider-endpoint")
            .arg(&config.provider_endpoint)
            .arg("--port")
            .arg(config.port.to_string())
            .arg("--token")
            .arg(&config.token)
            .arg("--concurrency")
            .arg(config.concurrency.to_string())
            .args(if config.hang_before_handshake {
                vec!["--hang-before-handshake"]
            } else {
                Vec::new()
            })
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;

        let pid = child.id();
        let mut owner = ChildOwner { child };
        let stdout = owner
            .child
            .stdout
            .take()
            .ok_or("the backend child has no standard output")?;
        let handshake = read_handshake(stdout, started, config.startup_timeout, pid)?;
        Ok(Self {
            owner,
            handshake,
            pid,
            shutdown_timeout: config.shutdown_timeout,
        })
    }

    pub fn handshake(&self) -> &BackendHandshake {
        &self.handshake
    }

    pub fn pid(&self) -> u32 {
        self.pid
    }

    pub fn base_url(&self) -> &str {
        &self.handshake.address
    }

    pub fn shutdown(mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        request_graceful_shutdown(&mut self.owner.child)?;
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

fn request_graceful_shutdown(
    child: &mut Child,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut stdin = child
        .stdin
        .take()
        .ok_or("backend shutdown pipe is unavailable")?;
    stdin.write_all(b"\n")?;
    stdin.flush()?;
    Ok(())
}

fn read_handshake(
    stdout: std::process::ChildStdout,
    started: Instant,
    timeout: Duration,
    pid: u32,
) -> Result<BackendHandshake, Box<dyn std::error::Error + Send + Sync>> {
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        loop {
            let mut line = String::new();
            match reader.read_line(&mut line) {
                Ok(0) => {
                    let _ = sender.send(Err("backend standard output closed".to_owned()));
                    return;
                }
                Ok(_) => {
                    if sender.send(Ok(line)).is_err() {
                        return;
                    }
                }
                Err(error) => {
                    let _ = sender.send(Err(error.to_string()));
                    return;
                }
            }
        }
    });
    let deadline = Instant::now() + timeout;
    loop {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or_else(|| {
                format!("the backend child {pid} did not become ready within {timeout:?}")
            })?;
        match receiver.recv_timeout(remaining) {
            Ok(Ok(line)) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                if trimmed.starts_with('{') {
                    return BackendHandshake::parse(trimmed, started.elapsed()).map_err(Into::into);
                }
                eprintln!("backend {pid}: {trimmed}");
            }
            Ok(Err(error)) => {
                return Err(format!("backend child {pid} handshake read failed: {error}").into());
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
