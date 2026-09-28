//! Invocation-scoped OpenCode 2 server ownership.
//!
//! OpenCode 2 is a daemon, and a daemon OCG did not start carries a catalogue
//! and configuration OCG cannot reason about. OCG therefore never attaches a
//! production launch to an ambient background service: it starts its own
//! loopback server, hands it the exact generated configuration through
//! `OPENCODE_CONFIG_CONTENT`, and owns that process for the lifetime of one
//! invocation.
//!
//! [`OwnedV2Server`] is the only place that knows how an OpenCode 2 server is
//! spawned and how its startup handshake is read. Ownership is explicit:
//! [`RuntimeIdentity`] keeps the endpoint in invocation memory and
//! [`RuntimeOwnership`] records who started it, so a stale service can never be
//! mistaken for this invocation's runtime. User-facing identity and Debug
//! output deliberately omit the endpoint and password.
//!
//! Startup is a handshake followed by a *real* readiness check. The server
//! prints `server listening on <url>` and `server password <password>` and only
//! then does it accept the API. Reading those two lines is necessary but not
//! sufficient — the socket may not be bound yet, the process may have died, or
//! the runtime may reject the credentials. [`OwnedV2Server::start`] therefore
//! waits for an actual authenticated API response, bounded by
//! [`StartupBudget`], and fails with a distinct error when readiness is never
//! reached.
//!
//! The local service password is a credential: it is held in
//! [`ServiceRegistration`] and is never rendered.

use crate::error::{OcgError, Result};
use crate::proxy::ChildProxyEnv;
use crate::runtime::compat::v2_client::{wait_ready, ServiceRegistration, V2SessionClient};
use std::collections::VecDeque;
use std::ffi::OsString;
use std::fmt;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

/// How long to wait for the startup handshake (both lines) before giving up.
const HANDSHAKE_DEADLINE: Duration = Duration::from_secs(15);
const STARTUP_STDERR_LIMIT: usize = 8192;

/// The bounded readiness budget used by production launches.
///
/// The first probe runs immediately, so an already-ready runtime costs exactly
/// one request and no sleep. Only a runtime that is still starting consumes the
/// interval between probes, and the attempt count caps the overall wait.
#[derive(Debug, Clone, Copy)]
pub struct StartupBudget {
    pub handshake_deadline: Duration,
    pub attempts: u32,
    pub interval: Duration,
}

impl Default for StartupBudget {
    fn default() -> Self {
        Self {
            handshake_deadline: HANDSHAKE_DEADLINE,
            attempts: 150,
            interval: Duration::from_millis(100),
        }
    }
}

/// Who started the runtime OCG is talking to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeOwnership {
    /// OCG spawned this server for exactly one invocation and terminates it on
    /// drop. No ambient daemon is involved.
    OcgManagedInvocation,
    /// A server discovered through OpenCode's own background-service
    /// registration. Diagnostics only; production launches never use this.
    RegisteredService,
}

impl RuntimeOwnership {
    pub fn as_str(self) -> &'static str {
        match self {
            RuntimeOwnership::OcgManagedInvocation => "ocg-managed-invocation",
            RuntimeOwnership::RegisteredService => "registered-service",
        }
    }
}

/// The deterministic identity of the runtime endpoint OCG resolved.
#[derive(Clone, PartialEq, Eq)]
pub struct RuntimeIdentity {
    pub ownership: RuntimeOwnership,
    /// Loopback base URL without a trailing slash. It is invocation-scoped
    /// and deliberately omitted from user-facing diagnostics and Debug output.
    pub endpoint: String,
    /// Owning process id, when OCG started it.
    pub pid: Option<u32>,
}

impl fmt::Debug for RuntimeIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeIdentity")
            .field("ownership", &self.ownership)
            .field("endpoint", &"<redacted>")
            .field("pid", &self.pid)
            .finish()
    }
}

impl RuntimeIdentity {
    /// A concise, credential-free description for reports.
    pub fn describe(&self) -> String {
        match self.pid {
            Some(pid) => format!("{} (pid {pid})", self.ownership.as_str()),
            None => self.ownership.as_str().to_string(),
        }
    }
}

/// An OpenCode 2 server started and owned by one OCG invocation.
pub struct OwnedV2Server {
    child: Child,
    registration: ServiceRegistration,
    identity: RuntimeIdentity,
}

impl OwnedV2Server {
    /// Start a private loopback server with the exact generated configuration
    /// and wait until it is really ready to serve the API.
    pub fn start(
        program: &Path,
        config_content: &str,
        extra_env: &[(OsString, OsString)],
        proxy: &ChildProxyEnv,
    ) -> Result<Self> {
        Self::start_with(
            program,
            config_content,
            extra_env,
            proxy,
            StartupBudget::default(),
        )
    }

    /// [`Self::start`] with an explicit readiness budget. Tests use a small
    /// budget so a runtime that never becomes ready fails in milliseconds.
    pub fn start_with(
        program: &Path,
        config_content: &str,
        extra_env: &[(OsString, OsString)],
        proxy: &ChildProxyEnv,
        budget: StartupBudget,
    ) -> Result<Self> {
        let mut command = Command::new(program);
        command
            .args(["serve", "--hostname", "127.0.0.1", "--port", "0"])
            .env("OPENCODE_CONFIG_CONTENT", config_content)
            .env_remove("OPENCODE_CONFIG")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for (key, value) in extra_env {
            command.env(key, value);
        }
        proxy.apply(&mut command);
        let mut child = command.spawn().map_err(|error| {
            OcgError::io(
                format!(
                    "cannot start the private OpenCode V2 server at {}",
                    program.display()
                ),
                error,
            )
        })?;
        let Some(stdout) = child.stdout.take() else {
            terminate(&mut child);
            return Err(OcgError::config(
                "the private OpenCode V2 server did not provide startup output",
            ));
        };
        let stderr = child.stderr.take();
        let stderr_tail = Arc::new(Mutex::new(VecDeque::new()));
        if let Some(stderr) = stderr {
            spawn_stderr_tail(stderr, Arc::clone(&stderr_tail));
        }
        let (sender, receiver) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                // After startup the receiver is dropped, but keep draining so a
                // chatty private server cannot block on its stdout pipe.
                let _ = sender.send(line.unwrap_or_default());
            }
        });
        let deadline = Instant::now() + budget.handshake_deadline;
        let mut url: Option<String> = None;
        let mut password: Option<String> = None;
        while Instant::now() < deadline && (url.is_none() || password.is_none()) {
            match receiver.recv_timeout(Duration::from_millis(100)) {
                Ok(line) => parse_startup_line(&line, &mut url, &mut password),
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }
        let (Some(url), Some(password)) = (url, password) else {
            // A just-exited child can outlive its stdout EOF briefly under
            // load; poll its exit status so an exited server is reported as
            // such instead of misdiagnosed as a slow handshake.
            let exit = poll_child_exit(&mut child, Duration::from_millis(250));
            let detail = stderr_detail(&stderr_tail);
            terminate(&mut child);
            return Err(OcgError::config(match exit {
                Some(status) => format!(
                    "the private OpenCode V2 server exited before reporting its loopback URL and password ({status}){detail}; it cannot serve this invocation"
                ),
                None => format!(
                    "the private OpenCode V2 server did not report its loopback URL and password within {:?}{detail}",
                    budget.handshake_deadline
                ),
            }));
        };

        let registration = ServiceRegistration::new(url, password);
        let identity = RuntimeIdentity {
            ownership: RuntimeOwnership::OcgManagedInvocation,
            endpoint: registration.url().to_string(),
            pid: Some(child.id()),
        };
        let server = Self {
            child,
            registration,
            identity,
        };
        if let Err(error) = server.wait_until_ready(budget) {
            drop(server);
            return Err(error);
        }
        Ok(server)
    }

    /// Poll the runtime with a real authenticated API request until it is ready.
    ///
    /// This distinguishes "the process started" from "the runtime can serve
    /// OCG". A runtime that exits while starting, a port that is not bound and
    /// a runtime that rejects OCG's credentials all fail with a distinct
    /// message instead of being treated as ready.
    fn wait_until_ready(&self, budget: StartupBudget) -> Result<()> {
        let client = V2SessionClient::connect(&self.registration, "")?;
        let mut attempt = 0u32;
        wait_ready(
            || {
                attempt += 1;
                client.readiness()
            },
            || std::thread::sleep(budget.interval),
            budget.attempts,
        )
        .map_err(|error| {
            OcgError::config(format!(
                "the private OpenCode V2 server never became ready: {error}"
            ))
        })
    }

    pub fn registration(&self) -> &ServiceRegistration {
        &self.registration
    }

    pub fn identity(&self) -> &RuntimeIdentity {
        &self.identity
    }

    pub fn url(&self) -> &str {
        self.registration.url()
    }

    /// The local service password. Never log, print or persist the result.
    pub fn password(&self) -> &str {
        self.registration.password().expose()
    }
}

impl Drop for OwnedV2Server {
    fn drop(&mut self) {
        terminate(&mut self.child);
    }
}

fn terminate(child: &mut Child) {
    if child.try_wait().ok().flatten().is_none() {
        let _ = child.kill();
        let _ = child.wait();
    }
}

/// Poll `try_wait` for a bounded period. The exit status of a just-exited child
/// can lag behind its stdout EOF under load; a single check would misdiagnose
/// an exited server as a slow handshake.
fn poll_child_exit(child: &mut Child, timeout: Duration) -> Option<std::process::ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait().ok().flatten() {
            return Some(status);
        }
        if Instant::now() >= deadline {
            return None;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn spawn_stderr_tail(stderr: impl Read + Send + 'static, tail: Arc<Mutex<VecDeque<u8>>>) {
    std::thread::spawn(move || {
        let mut reader = BufReader::new(stderr);
        let mut buffer = [0u8; 1024];
        loop {
            let count = match reader.read(&mut buffer) {
                Ok(0) | Err(_) => break,
                Ok(count) => count,
            };
            let Ok(mut tail) = tail.lock() else { break };
            tail.extend(&buffer[..count]);
            while tail.len() > STARTUP_STDERR_LIMIT {
                tail.pop_front();
            }
        }
    });
}

fn stderr_detail(tail: &Arc<Mutex<VecDeque<u8>>>) -> String {
    let Ok(mut tail) = tail.lock() else {
        return String::new();
    };
    if tail.is_empty() {
        return String::new();
    }
    let text = String::from_utf8_lossy(tail.make_contiguous())
        .trim()
        .to_string();
    if text.is_empty() {
        String::new()
    } else {
        format!("; child stderr: {}", sanitize_startup_stderr(&text))
    }
}

fn sanitize_startup_stderr(text: &str) -> String {
    text.lines()
        .map(|line| {
            let lower = line.to_ascii_lowercase();
            if lower.contains("password")
                || lower.contains("api_key")
                || lower.contains("apikey")
                || lower.contains("secret")
                || lower.contains("http://")
                || lower.contains("https://")
            {
                "[redacted startup diagnostic]"
            } else {
                line
            }
        })
        .collect::<Vec<_>>()
        .join("\\n")
}

/// Apply one server startup line to the handshake state. Pure so the parsing
/// contract (`server listening on <url>` / `server password <password>`) is
/// tested without a process.
fn parse_startup_line(line: &str, url: &mut Option<String>, password: &mut Option<String>) {
    if let Some(value) = line.strip_prefix("server listening on ") {
        let value = value.trim();
        if !value.is_empty() {
            *url = Some(value.to_string());
        }
    } else if let Some(value) = line.strip_prefix("server password ") {
        let value = value.trim();
        if !value.is_empty() {
            *password = Some(value.to_string());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proxy::{resolve, MapProxyEnv, NoStaticProxy};

    fn no_proxy() -> ChildProxyEnv {
        resolve(false, &MapProxyEnv::new(), &NoStaticProxy).child_env()
    }

    fn tiny_budget() -> StartupBudget {
        StartupBudget {
            handshake_deadline: Duration::from_secs(5),
            attempts: 5,
            interval: Duration::from_millis(20),
        }
    }

    /// Write an executable shell script into a fresh temp dir and return it.
    #[cfg(unix)]
    fn script(body: &str) -> (tempfile::TempDir, std::path::PathBuf) {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("fake-opencode");
        std::fs::write(&path, body).expect("write script");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
            .expect("chmod script");
        (dir, path)
    }

    /// An HTTP server that answers any request with `200 OK`. Used to prove the
    /// readiness path against a real socket.
    #[cfg(unix)]
    fn spawn_ok_server() -> (std::net::SocketAddr, std::thread::JoinHandle<()>) {
        use std::io::{Read, Write};
        use std::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
        let address = listener.local_addr().expect("local addr");
        let handle = std::thread::spawn(move || {
            for stream in listener.incoming().take(8) {
                let Ok(mut stream) = stream else { break };
                let mut buffer = [0u8; 1024];
                let _ = stream.read(&mut buffer);
                let _ = stream.write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
                );
            }
        });
        (address, handle)
    }

    #[test]
    fn parses_the_two_handshake_lines_and_ignores_everything_else() {
        let mut url = None;
        let mut password = None;
        parse_startup_line("unrelated log line", &mut url, &mut password);
        parse_startup_line(
            "server listening on http://127.0.0.1:34163",
            &mut url,
            &mut password,
        );
        parse_startup_line("server password s3cret", &mut url, &mut password);
        assert_eq!(url.as_deref(), Some("http://127.0.0.1:34163"));
        assert_eq!(password.as_deref(), Some("s3cret"));

        // Empty payloads never overwrite a good value.
        parse_startup_line("server listening on  ", &mut url, &mut password);
        parse_startup_line("server password ", &mut url, &mut password);
        assert_eq!(url.as_deref(), Some("http://127.0.0.1:34163"));
        assert_eq!(password.as_deref(), Some("s3cret"));
    }

    #[test]
    fn ownership_identity_is_credential_free_and_deterministic() {
        let identity = RuntimeIdentity {
            ownership: RuntimeOwnership::OcgManagedInvocation,
            endpoint: "http://127.0.0.1:1".to_string(),
            pid: Some(42),
        };
        assert_eq!(identity.describe(), "ocg-managed-invocation (pid 42)");
        assert!(!identity.describe().contains("127.0.0.1"));
        assert!(!format!("{identity:?}").contains("127.0.0.1"));
        assert_eq!(
            RuntimeOwnership::RegisteredService.as_str(),
            "registered-service"
        );
    }

    #[test]
    #[cfg(unix)]
    fn a_listening_server_that_serves_the_api_is_ready() {
        let (address, _server) = spawn_ok_server();
        let (_dir, program) = script(&format!(
            "#!/bin/sh\necho \"server listening on http://{address}\"\necho \"server password local-secret\"\nsleep 30\n"
        ));
        let server = OwnedV2Server::start_with(&program, "{}", &[], &no_proxy(), tiny_budget())
            .expect("ready server");
        assert_eq!(
            server.identity().ownership,
            RuntimeOwnership::OcgManagedInvocation
        );
        assert_eq!(server.url(), format!("http://{address}"));
        assert_eq!(server.password(), "local-secret");
        assert!(server.identity().pid.is_some());
    }

    #[test]
    #[cfg(unix)]
    fn a_printed_handshake_with_no_listener_is_not_ready() {
        // The process prints both handshake lines but never binds a socket:
        // "started" must never be equated with "ready".
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve port");
        let port = listener.local_addr().expect("addr").port();
        drop(listener);
        let (_dir, program) = script(&format!(
            "#!/bin/sh\necho \"server listening on http://127.0.0.1:{port}\"\necho \"server password local-secret\"\nsleep 30\n"
        ));
        let error = OwnedV2Server::start_with(&program, "{}", &[], &no_proxy(), tiny_budget())
            .err()
            .expect("a server that never binds must not be ready");
        let message = error.to_string();
        assert!(message.contains("never became ready"), "{message}");
    }

    #[test]
    #[cfg(unix)]
    fn a_server_that_exits_before_the_handshake_is_reported_clearly() {
        let (_dir, program) = script("#!/bin/sh\nexit 3\n");
        let error = OwnedV2Server::start_with(&program, "{}", &[], &no_proxy(), tiny_budget())
            .err()
            .expect("an exiting server must fail");
        let message = error.to_string();
        assert!(message.contains("exited before reporting"), "{message}");
    }
}
