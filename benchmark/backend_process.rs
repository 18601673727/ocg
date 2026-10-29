//! The backend, isolated in its own process.
//!
//! A measurement that runs the backend inside the harness measures the
//! harness as much as the backend, and a run that leaves a backend behind
//! measures whatever the next run left on disk. The backend therefore runs
//! as a child process of its own, with its own data directory, and the
//! harness talks to it over the control plane it serves.

use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::Value;

/// How the child backend is started.
#[derive(Clone, Debug)]
pub struct BackendConfig {
    /// The directory the child keeps its database and logs in.
    pub data_dir: PathBuf,
    /// The workspace the child's Project is rooted at.
    pub workspace: PathBuf,
    /// The endpoint of the mock provider the child sends its requests to.
    pub provider_endpoint: String,
    /// The port the child's control plane listens on; 0 for any free one.
    pub port: u16,
    /// The bearer token the child's control plane requires.
    pub token: String,
    /// How many jobs the child's pool may run at once.
    pub concurrency: usize,
    /// How long the child may take to become ready.
    pub startup_timeout: Duration,
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
        }
    }
}

/// What the child reported about itself once it was ready.
#[derive(Clone, Debug)]
pub struct BackendHandshake {
    pub address: String,
    pub token: String,
    pub executor: String,
    pub llm_profile: String,
    pub agent_profile: String,
    pub project: String,
    pub pool: String,
    /// How long the child took from spawn to ready.
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
                .map(str::to_owned)
                .ok_or_else(|| format!("the backend handshake has no {name}"))
        };
        Ok(Self {
            address: field("address")?,
            token: field("token")?,
            executor: field("executor")?,
            llm_profile: field("llm_profile")?,
            agent_profile: field("agent_profile")?,
            project: field("project")?,
            pool: field("pool")?,
            startup,
        })
    }
}

/// A running child backend.
pub struct BackendProcess {
    child: Child,
    handshake: BackendHandshake,
    pid: u32,
}

impl BackendProcess {
    /// Starts the child and waits for it to report that it is ready.
    ///
    /// The startup time the handshake carries is the time from the spawn to
    /// that report, which is what a cold start costs: the process is new,
    /// its data directory is empty and its database has to be created.
    pub fn spawn(config: &BackendConfig) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let executable = std::env::current_exe()?;
        let started = Instant::now();
        let mut child = Command::new(executable)
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
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;

        let pid = child.id();
        let stdout = child
            .stdout
            .take()
            .ok_or("the backend child has no standard output")?;
        let handshake = read_handshake(stdout, started, config.startup_timeout, pid)?;
        Ok(Self {
            child,
            handshake,
            pid,
        })
    }

    pub fn handshake(&self) -> &BackendHandshake {
        &self.handshake
    }

    pub fn pid(&self) -> u32 {
        self.pid
    }

    /// The base URL of the child's control plane.
    pub fn base_url(&self) -> &str {
        &self.handshake.address
    }

    /// Stops the child and waits for it to exit.
    pub fn shutdown(mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.child.kill()?;
        self.child.wait()?;
        Ok(())
    }
}

impl Drop for BackendProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Reads the child's handshake line, ignoring anything it logged before it.
fn read_handshake(
    stdout: std::process::ChildStdout,
    started: Instant,
    timeout: Duration,
    pid: u32,
) -> Result<BackendHandshake, Box<dyn std::error::Error + Send + Sync>> {
    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    let deadline = Instant::now() + timeout;
    loop {
        if Instant::now() > deadline {
            return Err(
                format!("the backend child {pid} did not become ready within {timeout:?}").into(),
            );
        }
        line.clear();
        let read = reader.read_line(&mut line)?;
        if read == 0 {
            return Err(format!("the backend child {pid} exited before it was ready").into());
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if trimmed.starts_with('{') {
            return Ok(BackendHandshake::parse(trimmed, started.elapsed())?);
        }
        // Anything else the child printed is a log line, not the handshake.
        eprintln!("backend {pid}: {trimmed}");
    }
}
