//! Ephemeral, invocation-owned bridge connectivity for a late-starting V2 server.
//! Only the owning OCG process publishes a registration. No credential is written
//! to disk: the temporary directory contains a Unix socket, not a document.

use crate::error::{OcgError, Result};
use crate::runtime::compat::v2_client::ServiceRegistration;
use serde_json::{json, Value};
use std::fs;
use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

pub const CHANNEL_ENV: &str = "OCG_V2_CHANNEL";
pub const ID_ENV: &str = "OCG_V2_INVOCATION";
const MAX_REPLY: u64 = 8192;

/// Created before server spawn; dropped after the client and owned server exit.
pub struct InvocationChannel {
    dir: tempfile::TempDir,
    identity: String,
    published: Arc<Mutex<Option<(ServiceRegistration, u32)>>>,
    stopped: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl InvocationChannel {
    pub fn new(project: &Path) -> Result<Self> {
        let base = project.join(".ocg");
        fs::create_dir_all(&base).map_err(|e| OcgError::io("cannot create invocation state", e))?;
        let dir = tempfile::Builder::new()
            .prefix("v2-bridge-")
            .tempdir_in(base)
            .map_err(|e| OcgError::io("cannot create invocation channel", e))?;
        fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700))
            .map_err(|e| OcgError::io("cannot secure invocation channel", e))?;
        let listener = UnixListener::bind(dir.path().join("bridge.sock"))
            .map_err(|e| OcgError::io("cannot bind invocation channel", e))?;
        listener
            .set_nonblocking(true)
            .map_err(|e| OcgError::io("cannot configure invocation channel", e))?;
        let published: Arc<Mutex<Option<(ServiceRegistration, u32)>>> = Arc::new(Mutex::new(None));
        let identity = dir
            .path()
            .file_name()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        let reply_identity = identity.clone();
        let state = Arc::clone(&published);
        let stopped = Arc::new(AtomicBool::new(false));
        let shutdown = Arc::clone(&stopped);
        let worker = thread::spawn(move || {
            while !shutdown.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        // The temporary directory is mode 0700; only processes
                        // in this invocation can reach this socket.
                        let response = state.lock().ok().and_then(|guard| {
                            guard.as_ref().map(|(registration, pid)| {
                                json!({
                                    "url": registration.url(),
                                    "password": registration.password().expose(),
                                    "pid": pid,
                                    "invocation": reply_identity,
                                })
                            })
                        });
                        let value =
                            response.unwrap_or_else(|| json!({"error":"runtime not published"}));
                        if let Ok(bytes) = serde_json::to_vec(&value) {
                            let _ = stream.write_all(&bytes);
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(_) => break,
                }
            }
        });
        Ok(Self {
            dir,
            identity,
            published,
            stopped,
            worker: Some(worker),
        })
    }

    pub fn socket(&self) -> PathBuf {
        self.dir.path().join("bridge.sock")
    }

    pub fn identity(&self) -> &str {
        &self.identity
    }

    pub fn publish(&self, runtime: &super::v2_server::OwnedV2Server) -> Result<()> {
        let registration = runtime.registration();
        let pid = runtime
            .identity()
            .pid
            .ok_or_else(|| OcgError::config("owned runtime has no PID"))?;
        let mut state = self
            .published
            .lock()
            .map_err(|_| OcgError::config("invocation channel unavailable"))?;
        if state.is_some() {
            return Err(OcgError::config("invocation runtime already published"));
        }
        if pid == 0 || !loopback(registration.url()) || registration.password().expose().is_empty()
        {
            return Err(OcgError::config("invalid invocation runtime registration"));
        }
        *state = Some((registration.clone(), pid));
        Ok(())
    }
}

impl Drop for InvocationChannel {
    fn drop(&mut self) {
        // Drop the publisher so the serving thread stops, then remove the
        // socket/directory through TempDir. No registration survives teardown.
        let _ = self.published.lock().map(|mut state| *state = None);
        self.stopped.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn loopback(url: &str) -> bool {
    url.strip_prefix("http://127.0.0.1:")
        .and_then(|port| port.parse::<u16>().ok())
        .is_some_and(|port| port != 0)
}

/// Resolve only the pre-spawn channel named by this bridge's inherited env.
pub fn resolve(path: &Path, identity: &str) -> Result<ServiceRegistration> {
    if identity.is_empty()
        || path
            .parent()
            .and_then(Path::file_name)
            .and_then(|name| name.to_str())
            != Some(identity)
    {
        return Err(OcgError::config("invocation identity mismatch"));
    }
    let stream = UnixStream::connect(path)
        .map_err(|_| OcgError::config("invocation channel unavailable"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .map_err(|_| OcgError::config("invocation channel timeout"))?;
    let mut bytes = Vec::new();
    stream
        .take(MAX_REPLY)
        .read_to_end(&mut bytes)
        .map_err(|_| OcgError::config("invocation channel unreadable"))?;
    let value: Value = serde_json::from_slice(&bytes)
        .map_err(|_| OcgError::config("invalid invocation registration"))?;
    let url = value.get("url").and_then(Value::as_str).unwrap_or("");
    let password = value.get("password").and_then(Value::as_str).unwrap_or("");
    if !loopback(url)
        || value.get("invocation").and_then(Value::as_str) != Some(identity)
        || password.is_empty()
        || value.get("pid").and_then(Value::as_u64).unwrap_or(0) == 0
    {
        return Err(OcgError::config("invocation runtime not published"));
    }
    Ok(ServiceRegistration::new(url, password))
}
