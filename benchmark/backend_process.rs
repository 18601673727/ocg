use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

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
    child: Option<Child>,
}

impl Drop for ChildOwner {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take()
            && child.try_wait().ok().flatten().is_none()
        {
            let _ = child.kill();
            let _ = std::thread::Builder::new()
                .name("backend-benchmark-reaper".to_owned())
                .spawn(move || {
                    let _ = child.wait();
                });
        }
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
        let child = command(&binary, config)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let pid = child.id();
        let mut owner = ChildOwner { child: Some(child) };
        let child = owner.child.as_mut().ok_or("backend child is missing")?;
        let address = read_address(child, started, config.startup_timeout, pid)?;
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
        let child = self
            .owner
            .child
            .as_mut()
            .ok_or("backend child is missing")?;
        terminate(child)?;
        let deadline = Instant::now() + self.shutdown_timeout;
        loop {
            if let Some(status) = child.try_wait()? {
                if status.success() {
                    return Ok(());
                }
                return Err(format!("backend child exited unsuccessfully: {status}").into());
            }
            if Instant::now() >= deadline {
                child.kill()?;
                let status = child.wait()?;
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
            let mut receiving = true;
            loop {
                let mut line = String::new();
                match reader.read_line(&mut line) {
                    Ok(0) => return,
                    Ok(_) => {
                        if receiving {
                            match sender.send(line) {
                                Ok(()) => continue,
                                Err(error) => {
                                    receiving = false;
                                    line = error.0;
                                }
                            }
                        }
                        let trimmed = line.trim();
                        if !trimmed.is_empty()
                            && !trimmed.starts_with("OCG local browser pairing code:")
                        {
                            eprintln!("backend {pid}: {trimmed}");
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
    let mut address = None;
    let mut signal_ready = false;
    loop {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or_else(|| {
                format!("the backend child {pid} did not become ready within {timeout:?}")
            })?;
        match receiver.recv_timeout(remaining) {
            Ok(line) => {
                let trimmed = line.trim();
                if let Some(found) = listening_address(trimmed) {
                    address = Some(found);
                }
                if trimmed == "OCG signal listener ready" {
                    signal_ready = true;
                }
                if signal_ready && let Some(address) = address.take() {
                    return Ok(address);
                }
                if !trimmed.is_empty() && !trimmed.starts_with("OCG local browser pairing code:") {
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
