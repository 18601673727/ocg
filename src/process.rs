//! Centralized external process execution.
//!
//! This module is the only place that builds a [`std::process::Command`], so
//! every child process the tool ever starts goes through one auditable path.
//!
//! [`ProcessRunner`] replaces the current process with OpenCode.
//! [`ProcessHost`] is the runtime-facing side (PATH lookup, `--version`
//! probing and OpenCode's own `upgrade`); tests inject a fake host instead of
//! spawning anything.

use crate::error::{OcgError, Result};
use crate::proxy::{ChildProxyEnv, StaticProxyProvider};
use semver::Version;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::Instant;

const MAX_MODELS_OUTPUT_BYTES: usize = 4 * 1024 * 1024;

/// The `opencode` binary and how to run it.
#[derive(Debug, Clone)]
pub struct ProcessRunner {
    program: OsString,
}

impl ProcessRunner {
    pub fn new(program: OsString) -> Self {
        Self { program }
    }

    pub fn program(&self) -> &OsStr {
        &self.program
    }

    /// Replace the current process with the configured program.
    ///
    /// `OPENCODE_CONFIG_CONTENT` carries the generated config;
    /// `OPENCODE_CONFIG` is removed so a stale file path cannot override it.
    /// `extra_env` carries additional variables (for example the orchestration
    /// bridge's executable and project); `proxy_env` is the resolved proxy
    /// policy, which is applied last so it cannot be overridden. On Unix this
    /// `exec`s so signals and exit codes behave exactly like the historical
    /// shell wrapper.
    pub fn exec(
        &self,
        args: &[OsString],
        cwd: &Path,
        config_content: &str,
        extra_env: &[(OsString, OsString)],
        proxy_env: &ChildProxyEnv,
    ) -> Result<()> {
        let mut command = Command::new(&self.program);
        command
            .args(args)
            .current_dir(cwd)
            .env("OPENCODE_CONFIG_CONTENT", config_content)
            .env_remove("OPENCODE_CONFIG");
        for (key, value) in extra_env {
            command.env(key, value);
        }
        proxy_env.apply(&mut command);

        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            let error = command.exec();
            Err(OcgError::io(
                format!("cannot run {}", self.program.to_string_lossy()),
                error,
            ))
        }

        #[cfg(not(unix))]
        {
            let status = command.status().map_err(|error| {
                OcgError::io(
                    format!("cannot run {}", self.program.to_string_lossy()),
                    error,
                )
            })?;
            std::process::exit(status.code().unwrap_or(1));
        }
    }

    /// Run OpenCode as a child and return its exit code. V2 uses this rather
    /// than `exec` so the invocation-scoped private server can be reaped after
    /// the OpenCode client exits. The v1 path deliberately continues to use
    /// [`Self::exec`].
    pub fn run(
        &self,
        args: &[OsString],
        cwd: &Path,
        config_content: &str,
        extra_env: &[(OsString, OsString)],
        proxy_env: &ChildProxyEnv,
    ) -> Result<i32> {
        let mut command = Command::new(&self.program);
        command
            .args(args)
            .current_dir(cwd)
            .env("OPENCODE_CONFIG_CONTENT", config_content)
            .env_remove("OPENCODE_CONFIG");
        for (key, value) in extra_env {
            command.env(key, value);
        }
        proxy_env.apply(&mut command);
        let status = command.status().map_err(|error| {
            OcgError::io(
                format!("cannot run {}", self.program.to_string_lossy()),
                error,
            )
        })?;
        Ok(status.code().unwrap_or(1))
    }
}

/// Reads static macOS proxy configuration through `/usr/sbin/scutil --proxy`.
///
/// This is the only place the command is constructed, and it is compiled to
/// `None` on every non-macOS platform. Tests inject
/// [`crate::proxy::NoStaticProxy`] or a fixed provider instead.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemStaticProxy;

impl StaticProxyProvider for SystemStaticProxy {
    fn raw_scutil(&self) -> Option<String> {
        read_scutil_proxy()
    }
}

#[cfg(target_os = "macos")]
fn read_scutil_proxy() -> Option<String> {
    let output = Command::new("/usr/sbin/scutil")
        .arg("--proxy")
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout).into_owned();
    if text.trim().is_empty() {
        None
    } else {
        Some(text)
    }
}

#[cfg(not(target_os = "macos"))]
fn read_scutil_proxy() -> Option<String> {
    None
}

/// Process discovery, version probing and OpenCode's own upgrade command.
pub trait ProcessHost: Send + Sync {
    /// Resolve a bare program name against `PATH`.
    fn find_in_path(&self, program: &str) -> Option<PathBuf>;

    /// Run `<program> --version` and return the trimmed output.
    fn version(&self, program: &Path) -> Result<String>;

    /// Run `<program> models` with the exact generated configuration and
    /// return its machine-oriented line output. This is the supported
    /// OpenCode CLI surface used by runtime model preflight.
    fn models(
        &self,
        program: &Path,
        cwd: &Path,
        config_content: &str,
        proxy: &ChildProxyEnv,
    ) -> Result<String>;

    /// Run OpenCode's own `<program> upgrade [target]` and return its output.
    ///
    /// This is the only upgrade path for an existing system runtime; it must
    /// never be replaced by a managed download. `target` is the concrete
    /// version OCG resolved through its own transport, and `proxy`
    /// is the resolved proxy policy for the child.
    fn upgrade(
        &self,
        program: &Path,
        target: Option<&Version>,
        proxy: &ChildProxyEnv,
    ) -> Result<String>;
}

/// The real host, backed by `PATH` and `std::process::Command`.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemProcessHost;

impl ProcessHost for SystemProcessHost {
    fn find_in_path(&self, program: &str) -> Option<PathBuf> {
        let path = std::env::var_os("PATH")?;
        for directory in std::env::split_paths(&path) {
            if directory.as_os_str().is_empty() {
                continue;
            }
            let candidate = directory.join(program);
            if is_executable(&candidate) {
                return Some(candidate);
            }
            #[cfg(windows)]
            {
                for extension in ["exe", "cmd", "bat"] {
                    let candidate = directory.join(format!("{program}.{extension}"));
                    if is_executable(&candidate) {
                        return Some(candidate);
                    }
                }
            }
        }
        None
    }

    fn version(&self, program: &Path) -> Result<String> {
        let output = Command::new(program)
            .arg("--version")
            .output()
            .map_err(|error| {
                OcgError::io(format!("cannot run {} --version", program.display()), error)
            })?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            let detail = if stderr.is_empty() {
                format!("exit status {}", output.status)
            } else {
                stderr
            };
            return Err(OcgError::config(format!(
                "{} --version failed: {detail}",
                program.display()
            )));
        }
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if !stdout.is_empty() {
            return Ok(stdout);
        }
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        if !stderr.is_empty() {
            return Ok(stderr);
        }
        Err(OcgError::config(format!(
            "{} --version produced no output",
            program.display()
        )))
    }

    fn models(
        &self,
        program: &Path,
        cwd: &Path,
        config_content: &str,
        proxy: &ChildProxyEnv,
    ) -> Result<String> {
        let mut command = Command::new(program);
        command
            .arg("models")
            .current_dir(cwd)
            .env("OPENCODE_CONFIG_CONTENT", config_content)
            .env_remove("OPENCODE_CONFIG")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        for (key, value) in crate::vault::Vault::user_global()?.child_environment()? {
            command.env(key, value);
        }
        proxy.apply(&mut command);
        let mut child = command.spawn().map_err(|error| {
            OcgError::io(format!("cannot run {} models", program.display()), error)
        })?;
        let stdout_handle = child.stdout.take().map(|stdout| {
            std::thread::spawn(move || read_drain_bounded(stdout, MAX_MODELS_OUTPUT_BYTES))
        });
        let stderr_handle = child.stderr.take().map(|stderr| {
            std::thread::spawn(move || read_drain_bounded(stderr, MAX_MODELS_OUTPUT_BYTES))
        });
        let status = child.wait().map_err(|error| {
            OcgError::io(
                format!("cannot wait for {} models", program.display()),
                error,
            )
        })?;
        let (stdout, stdout_truncated) = stdout_handle
            .and_then(|handle| handle.join().ok())
            .unwrap_or_default();
        let (_, stderr_truncated) = stderr_handle
            .and_then(|handle| handle.join().ok())
            .unwrap_or_default();
        if stdout_truncated || stderr_truncated {
            return Err(OcgError::config(format!(
                "{} models exceeded the {} byte output limit",
                program.display(),
                MAX_MODELS_OUTPUT_BYTES
            )));
        }
        if !status.success() {
            // OpenCode/provider diagnostics can contain arbitrary third-party
            // text. Keep this probe failure actionable without echoing it.
            return Err(OcgError::config(format!(
                "{} models failed with {}",
                program.display(),
                status
            )));
        }
        String::from_utf8(stdout).map_err(|_| {
            OcgError::config(format!(
                "{} models produced non-UTF-8 output",
                program.display()
            ))
        })
    }

    fn upgrade(
        &self,
        program: &Path,
        target: Option<&Version>,
        proxy: &ChildProxyEnv,
    ) -> Result<String> {
        let mut command = Command::new(program);
        command.arg("upgrade");
        if let Some(version) = target {
            command.arg(version.to_string());
        }
        proxy.apply(&mut command);
        let output = command.output().map_err(|error| {
            OcgError::io(format!("cannot run {} upgrade", program.display()), error)
        })?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
            let detail = if !stderr.is_empty() {
                stderr
            } else if !stdout.is_empty() {
                stdout
            } else {
                format!("exit status {}", output.status)
            };
            return Err(OcgError::config(format!(
                "{} upgrade failed: {detail}",
                program.display()
            )));
        }
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        Ok(if !stdout.is_empty() { stdout } else { stderr })
    }
}

/// The outcome configured for a fake `upgrade` call.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub enum FakeUpgrade {
    /// Succeed, optionally changing the version the host reports afterwards.
    Success(Option<String>),
    /// Fail with a message.
    Failure(String),
}

/// The outcome configured for a fake `models` call.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub enum FakeModels {
    Success(String),
    Failure(String),
}

/// A fake host for tests: a fixed `PATH` map, canned version strings, canned
/// `upgrade` outcomes and call counters.
#[doc(hidden)]
#[derive(Debug, Default, Clone)]
pub struct FakeProcessHost {
    programs: HashMap<String, PathBuf>,
    versions: Arc<Mutex<HashMap<PathBuf, String>>>,
    default_version: Option<String>,
    upgrades: Arc<Mutex<HashMap<PathBuf, FakeUpgrade>>>,
    models: Arc<Mutex<HashMap<PathBuf, FakeModels>>>,
    upgrade_calls: Arc<Mutex<Vec<PathBuf>>>,
    upgrade_targets: Arc<Mutex<Vec<Option<Version>>>>,
    version_calls: Arc<Mutex<Vec<PathBuf>>>,
    model_calls: Arc<Mutex<Vec<PathBuf>>>,
}

impl FakeProcessHost {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_program(mut self, name: &str, path: impl Into<PathBuf>) -> Self {
        self.programs.insert(name.to_string(), path.into());
        self
    }

    pub fn with_version(self, path: impl Into<PathBuf>, version: &str) -> Self {
        self.versions
            .lock()
            .expect("fake versions")
            .insert(path.into(), version.to_string());
        self
    }

    /// Report this version for any path without a specific version.
    pub fn with_default_version(mut self, version: &str) -> Self {
        self.default_version = Some(version.to_string());
        self
    }

    /// Configure `<path> upgrade` to succeed, optionally moving to a new version.
    pub fn with_upgrade_success(self, path: impl Into<PathBuf>, new_version: Option<&str>) -> Self {
        self.upgrades.lock().expect("fake upgrades").insert(
            path.into(),
            FakeUpgrade::Success(new_version.map(str::to_string)),
        );
        self
    }

    /// Configure `<path> upgrade` to fail.
    pub fn with_upgrade_failure(self, path: impl Into<PathBuf>, message: &str) -> Self {
        self.upgrades
            .lock()
            .expect("fake upgrades")
            .insert(path.into(), FakeUpgrade::Failure(message.to_string()));
        self
    }

    pub fn with_models_success(self, path: impl Into<PathBuf>, output: &str) -> Self {
        self.models
            .lock()
            .expect("fake models")
            .insert(path.into(), FakeModels::Success(output.to_string()));
        self
    }

    pub fn with_models_failure(self, path: impl Into<PathBuf>, message: &str) -> Self {
        self.models
            .lock()
            .expect("fake models")
            .insert(path.into(), FakeModels::Failure(message.to_string()));
        self
    }

    /// Paths for which `upgrade` was invoked, in order.
    pub fn upgrade_calls(&self) -> Vec<PathBuf> {
        self.upgrade_calls.lock().expect("upgrade calls").clone()
    }

    /// Requested target versions for each `upgrade` call, in order.
    pub fn upgrade_targets(&self) -> Vec<Option<Version>> {
        self.upgrade_targets
            .lock()
            .expect("upgrade targets")
            .clone()
    }

    /// Paths for which `--version` was invoked, in order.
    pub fn version_calls(&self) -> Vec<PathBuf> {
        self.version_calls.lock().expect("version calls").clone()
    }

    pub fn model_calls(&self) -> Vec<PathBuf> {
        self.model_calls.lock().expect("model calls").clone()
    }
}

impl ProcessHost for FakeProcessHost {
    fn find_in_path(&self, program: &str) -> Option<PathBuf> {
        self.programs.get(program).cloned()
    }

    fn version(&self, program: &Path) -> Result<String> {
        self.version_calls
            .lock()
            .expect("version calls")
            .push(program.to_path_buf());
        let versions = self.versions.lock().expect("fake versions");
        if let Some(version) = versions.get(program) {
            return Ok(version.clone());
        }
        drop(versions);
        self.default_version.clone().ok_or_else(|| {
            OcgError::config(format!(
                "{} --version produced no output",
                program.display()
            ))
        })
    }

    fn models(
        &self,
        program: &Path,
        _cwd: &Path,
        _config_content: &str,
        _proxy: &ChildProxyEnv,
    ) -> Result<String> {
        self.model_calls
            .lock()
            .expect("model calls")
            .push(program.to_path_buf());
        match self
            .models
            .lock()
            .expect("fake models")
            .get(program)
            .cloned()
        {
            Some(FakeModels::Success(output)) => Ok(output),
            Some(FakeModels::Failure(message)) => Err(OcgError::config(message)),
            None => Err(OcgError::config(format!(
                "{} models is not configured in this fake",
                program.display()
            ))),
        }
    }

    fn upgrade(
        &self,
        program: &Path,
        target: Option<&Version>,
        _proxy: &ChildProxyEnv,
    ) -> Result<String> {
        self.upgrade_calls
            .lock()
            .expect("upgrade calls")
            .push(program.to_path_buf());
        self.upgrade_targets
            .lock()
            .expect("upgrade targets")
            .push(target.cloned());
        let outcome = self
            .upgrades
            .lock()
            .expect("fake upgrades")
            .get(program)
            .cloned();
        match outcome {
            Some(FakeUpgrade::Success(new_version)) => {
                if let Some(new_version) = new_version {
                    self.versions
                        .lock()
                        .expect("fake versions")
                        .insert(program.to_path_buf(), new_version);
                }
                Ok("upgraded".to_string())
            }
            Some(FakeUpgrade::Failure(message)) => Err(OcgError::config(message)),
            None => Err(OcgError::config(format!(
                "{} upgrade is not configured in this fake",
                program.display()
            ))),
        }
    }
}

/// A captured `git` invocation.
///
/// A non-zero exit status is *data*, not an error: "this is not a git
/// repository" is a normal answer the context engine must handle gracefully.
/// Only a failure to spawn `git` at all is an [`Err`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitOutput {
    pub success: bool,
    pub stdout: String,
    pub stderr: String,
    /// Set when stdout or stderr was capped by [`GitHost::run_bounded`].
    pub truncated: bool,
}

/// The only place the context engine reaches Git. Keeping it here preserves the
/// "every child process goes through one auditable module" invariant.
pub trait GitHost: Send + Sync {
    /// Run `git` with `args` in `cwd` and capture its output.
    fn run(&self, args: &[&str], cwd: &Path) -> Result<GitOutput>;

    /// Run `git` and capture at most `max_bytes` of stdout, killing the child if
    /// it keeps producing output. The default implementation truncates the
    /// result of [`GitHost::run`] on a UTF-8 boundary, which is deterministic
    /// and sufficient for fakes; real hosts should override it.
    fn run_bounded(&self, args: &[&str], cwd: &Path, max_bytes: usize) -> Result<GitOutput> {
        let mut output = self.run(args, cwd)?;
        if output.stdout.len() > max_bytes {
            let mut end = max_bytes;
            while end > 0 && !output.stdout.is_char_boundary(end) {
                end -= 1;
            }
            output.stdout.truncate(end);
            output.truncated = true;
        }
        Ok(output)
    }
}

/// Read at most `max` bytes, stopping (and signalling truncation) as soon as
/// the cap is reached. Used for stdout, where the caller kills the child.
fn read_bounded<R: std::io::Read>(mut reader: R, max: usize) -> (Vec<u8>, bool) {
    let mut out = Vec::new();
    let mut buffer = [0u8; 8192];
    let mut truncated = false;
    loop {
        match reader.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => {
                let remaining = max.saturating_sub(out.len());
                if remaining == 0 {
                    truncated = true;
                    break;
                }
                let take = read.min(remaining);
                out.extend_from_slice(&buffer[..take]);
                if take < read {
                    truncated = true;
                    break;
                }
            }
            Err(_) => break,
        }
    }
    (out, truncated)
}

/// Drain a stream to EOF while retaining at most `max` bytes. Used for stderr,
/// so a full pipe can never deadlock the child.
fn read_drain_bounded<R: std::io::Read>(mut reader: R, max: usize) -> (Vec<u8>, bool) {
    let mut out = Vec::new();
    let mut buffer = [0u8; 8192];
    let mut truncated = false;
    loop {
        match reader.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => {
                let remaining = max.saturating_sub(out.len());
                if remaining == 0 {
                    truncated = true;
                    continue;
                }
                let take = read.min(remaining);
                out.extend_from_slice(&buffer[..take]);
                if take < read {
                    truncated = true;
                }
            }
            Err(_) => break,
        }
    }
    (out, truncated)
}

/// The real Git host, backed by `std::process::Command`.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemGitHost;

impl GitHost for SystemGitHost {
    fn run(&self, args: &[&str], cwd: &Path) -> Result<GitOutput> {
        let output = Command::new("git")
            .args(args)
            .current_dir(cwd)
            .output()
            .map_err(|error| OcgError::io("cannot run git", error))?;
        Ok(GitOutput {
            success: output.status.success(),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            truncated: false,
        })
    }

    fn run_bounded(&self, args: &[&str], cwd: &Path, max_bytes: usize) -> Result<GitOutput> {
        let mut child = Command::new("git")
            .args(args)
            .current_dir(cwd)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|error| OcgError::io("cannot run git", error))?;

        // Drain stderr concurrently, retaining a bounded prefix.
        let stderr_handle = child
            .stderr
            .take()
            .map(|stderr| std::thread::spawn(move || read_drain_bounded(stderr, max_bytes)));

        let (stdout_bytes, stdout_truncated) = match child.stdout.take() {
            Some(stdout) => read_bounded(stdout, max_bytes),
            None => (Vec::new(), false),
        };
        // Stop a runaway producer before waiting on it.
        if stdout_truncated {
            let _ = child.kill();
        }
        let status = match child.wait() {
            Ok(status) => status,
            Err(error) => {
                if let Some(handle) = stderr_handle {
                    let _ = handle.join();
                }
                return Err(OcgError::io("cannot wait for git", error));
            }
        };
        let (stderr_bytes, stderr_truncated) = match stderr_handle {
            Some(handle) => handle.join().unwrap_or_default(),
            None => (Vec::new(), false),
        };
        Ok(GitOutput {
            success: status.success(),
            stdout: String::from_utf8_lossy(&stdout_bytes).into_owned(),
            stderr: String::from_utf8_lossy(&stderr_bytes).into_owned(),
            truncated: stdout_truncated || stderr_truncated,
        })
    }
}

/// One exact-argument fake git response.
#[doc(hidden)]
pub type FakeGitResponse = (Vec<String>, GitOutput);

/// A fake Git host for tests: exact-argument responses, with a default
/// "not a repository" answer for unmatched calls.
#[doc(hidden)]
#[derive(Debug, Default, Clone)]
pub struct FakeGitHost {
    responses: Arc<Mutex<Vec<FakeGitResponse>>>,
    calls: Arc<Mutex<Vec<Vec<String>>>>,
    default: Option<GitOutput>,
}

impl FakeGitHost {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register the output for an exact argument vector (ignoring `cwd`).
    pub fn with_response(self, args: &[&str], output: GitOutput) -> Self {
        self.responses
            .lock()
            .expect("fake git responses")
            .push((args.iter().map(|arg| arg.to_string()).collect(), output));
        self
    }

    /// Convenience: a successful response with the given stdout.
    pub fn with_stdout(self, args: &[&str], stdout: &str) -> Self {
        self.with_response(
            args,
            GitOutput {
                success: true,
                stdout: stdout.to_string(),
                stderr: String::new(),
                truncated: false,
            },
        )
    }

    /// Convenience: a failed response with the given stderr.
    pub fn with_failure(self, args: &[&str], stderr: &str) -> Self {
        self.with_response(
            args,
            GitOutput {
                success: false,
                stdout: String::new(),
                stderr: stderr.to_string(),
                truncated: false,
            },
        )
    }

    /// The output returned for any unmatched call. Defaults to "not a repo".
    pub fn with_default(mut self, output: GitOutput) -> Self {
        self.default = Some(output);
        self
    }

    /// Every argument vector this fake handled, in call order.
    pub fn calls(&self) -> Vec<Vec<String>> {
        self.calls.lock().expect("fake git calls").clone()
    }
}

impl GitHost for FakeGitHost {
    fn run(&self, args: &[&str], _cwd: &Path) -> Result<GitOutput> {
        let args: Vec<String> = args.iter().map(|arg| arg.to_string()).collect();
        self.calls
            .lock()
            .expect("fake git calls")
            .push(args.clone());
        let responses = self.responses.lock().expect("fake git responses");
        if let Some((_, output)) = responses.iter().find(|(expected, _)| *expected == args) {
            return Ok(output.clone());
        }
        drop(responses);
        Ok(self.default.clone().unwrap_or(GitOutput {
            success: false,
            stdout: String::new(),
            stderr: "fatal: not a git repository".to_string(),
            truncated: false,
        }))
    }
}

/// How a captured child process ended: an exit code, a signal, or something
/// the platform did not report. Kept explicit so a killed process is never
/// mistaken for an unknown success.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessExit {
    Code(i32),
    Signal(i32),
    Unknown,
}

impl ProcessExit {
    pub fn is_success(self) -> bool {
        matches!(self, ProcessExit::Code(0))
    }

    /// A stable, human-readable label. Never claims success for `Unknown`.
    pub fn label(self) -> String {
        match self {
            ProcessExit::Code(code) => format!("exit {code}"),
            ProcessExit::Signal(signal) => format!("signal {signal}"),
            ProcessExit::Unknown => "unknown exit status".to_string(),
        }
    }
}

/// One bounded capture of an external command. Bytes are retained as raw
/// bytes so invalid UTF-8 is preserved lossily by callers instead of being
/// dropped; `*_truncated` records that a stream exceeded the cap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapturedOutput {
    pub exit: ProcessExit,
    pub success: bool,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
    pub duration_ms: u64,
}

impl CapturedOutput {
    pub fn success(stdout: impl Into<Vec<u8>>) -> Self {
        Self {
            exit: ProcessExit::Code(0),
            success: true,
            stdout: stdout.into(),
            stderr: Vec::new(),
            stdout_truncated: false,
            stderr_truncated: false,
            duration_ms: 1,
        }
    }

    pub fn failure(code: i32, stderr: impl Into<Vec<u8>>) -> Self {
        Self {
            exit: ProcessExit::Code(code),
            success: false,
            stdout: Vec::new(),
            stderr: stderr.into(),
            stdout_truncated: false,
            stderr_truncated: false,
            duration_ms: 1,
        }
    }

    pub fn stdout_lossy(&self) -> String {
        String::from_utf8_lossy(&self.stdout).into_owned()
    }

    pub fn stderr_lossy(&self) -> String {
        String::from_utf8_lossy(&self.stderr).into_owned()
    }

    pub fn truncated(&self) -> bool {
        self.stdout_truncated || self.stderr_truncated
    }
}

/// The one trait verification (and any future capture) uses to run a command
/// without constructing a [`std::process::Command`] outside this module.
///
/// `max_bytes` bounds each stream independently and must be greater than zero.
pub trait CaptureRunner: Send + Sync {
    fn run(
        &self,
        program: &str,
        args: &[String],
        cwd: &Path,
        max_bytes: usize,
    ) -> Result<CapturedOutput>;
}

/// The real capture runner, backed by `std::process::Command`.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemCaptureRunner;

fn process_exit(status: &std::process::ExitStatus) -> ProcessExit {
    if let Some(code) = status.code() {
        return ProcessExit::Code(code);
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        if let Some(signal) = status.signal() {
            return ProcessExit::Signal(signal);
        }
    }
    ProcessExit::Unknown
}

impl CaptureRunner for SystemCaptureRunner {
    fn run(
        &self,
        program: &str,
        args: &[String],
        cwd: &Path,
        max_bytes: usize,
    ) -> Result<CapturedOutput> {
        let max_bytes = max_bytes.max(1);
        let start = Instant::now();
        let mut child = Command::new(program)
            .args(args)
            .current_dir(cwd)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|error| OcgError::io(format!("cannot run {program}"), error))?;

        // Drain both streams concurrently to EOF, retaining only a bounded
        // prefix. The child is never killed for being verbose: its own exit
        // status stays authoritative and truncation is reported separately.
        // Memory stays bounded; a genuinely non-terminating command is a
        // documented limitation (there is no timeout).
        let stdout_handle = child
            .stdout
            .take()
            .map(|stdout| std::thread::spawn(move || read_drain_bounded(stdout, max_bytes)));
        let (stderr_bytes, stderr_truncated) = match child.stderr.take() {
            Some(stderr) => read_drain_bounded(stderr, max_bytes),
            None => (Vec::new(), false),
        };
        let (stdout_bytes, stdout_truncated) = match stdout_handle {
            Some(handle) => handle.join().unwrap_or_default(),
            None => (Vec::new(), false),
        };
        let status = child
            .wait()
            .map_err(|error| OcgError::io(format!("cannot wait for {program}"), error))?;
        let duration_ms = start.elapsed().as_millis().min(u64::MAX as u128) as u64;
        Ok(CapturedOutput {
            exit: process_exit(&status),
            success: status.success(),
            stdout: stdout_bytes,
            stderr: stderr_bytes,
            stdout_truncated,
            stderr_truncated,
            duration_ms,
        })
    }
}

/// One exact-argument fake capture response.
#[doc(hidden)]
pub type FakeCaptureResponse = (String, Vec<String>, CapturedOutput);

/// One recorded fake capture call.
#[doc(hidden)]
pub type FakeCaptureCall = (String, Vec<String>);

/// A test-only capture runner: exact (program, args) responses plus call
/// recording. Unmatched calls fail the spawn, which exercises the error path.
#[doc(hidden)]
#[derive(Debug, Default, Clone)]
pub struct FakeCaptureRunner {
    responses: Arc<Mutex<Vec<FakeCaptureResponse>>>,
    calls: Arc<Mutex<Vec<FakeCaptureCall>>>,
    default: Option<CapturedOutput>,
}

impl FakeCaptureRunner {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_response(self, program: &str, args: &[&str], output: CapturedOutput) -> Self {
        self.responses
            .lock()
            .expect("fake capture responses")
            .push((
                program.to_string(),
                args.iter().map(|arg| arg.to_string()).collect(),
                output,
            ));
        self
    }

    pub fn with_success(self, program: &str, args: &[&str], stdout: &str) -> Self {
        self.with_response(
            program,
            args,
            CapturedOutput::success(stdout.as_bytes().to_vec()),
        )
    }

    pub fn with_failure(self, program: &str, args: &[&str], code: i32, stderr: &str) -> Self {
        self.with_response(
            program,
            args,
            CapturedOutput::failure(code, stderr.as_bytes().to_vec()),
        )
    }

    /// The output returned for any command without an exact match.
    pub fn with_default(mut self, output: CapturedOutput) -> Self {
        self.default = Some(output);
        self
    }

    pub fn calls(&self) -> Vec<FakeCaptureCall> {
        self.calls.lock().expect("fake capture calls").clone()
    }
}

impl CaptureRunner for FakeCaptureRunner {
    fn run(
        &self,
        program: &str,
        args: &[String],
        _cwd: &Path,
        _max_bytes: usize,
    ) -> Result<CapturedOutput> {
        self.calls
            .lock()
            .expect("fake capture calls")
            .push((program.to_string(), args.to_vec()));
        let responses = self.responses.lock().expect("fake capture responses");
        if let Some((_, _, output)) =
            responses
                .iter()
                .find(|(expected_program, expected_args, _)| {
                    expected_program == program && expected_args == args
                })
        {
            return Ok(output.clone());
        }
        drop(responses);
        match &self.default {
            Some(output) => Ok(output.clone()),
            None => Err(OcgError::config(format!(
                "{program} is not configured in this fake capture runner"
            ))),
        }
    }
}

/// Whether a path points at an executable regular file.
pub fn is_executable(path: &Path) -> bool {
    if !path.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        path.metadata()
            .map(|metadata| metadata.permissions().mode() & 0o111 != 0)
            .unwrap_or(false)
    }
    #[cfg(not(unix))]
    {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fake_host_resolves_registered_programs() {
        let host = FakeProcessHost::new()
            .with_program("opencode", "/opt/opencode")
            .with_version("/opt/opencode", "1.18.31");
        assert_eq!(
            host.find_in_path("opencode"),
            Some(PathBuf::from("/opt/opencode"))
        );
        assert_eq!(host.find_in_path("missing"), None);
        assert_eq!(host.version(Path::new("/opt/opencode")).unwrap(), "1.18.31");
        assert!(host.version(Path::new("/missing")).is_err());
    }

    #[test]
    fn fake_host_records_and_applies_upgrades() {
        let host = FakeProcessHost::new()
            .with_default_version("1.18.20")
            .with_upgrade_success("/opt/opencode", Some("1.18.31"));
        assert_eq!(host.version(Path::new("/opt/opencode")).unwrap(), "1.18.20");
        host.upgrade(
            Path::new("/opt/opencode"),
            Some(&Version::new(1, 18, 31)),
            &ChildProxyEnv::default(),
        )
        .unwrap();
        assert_eq!(host.version(Path::new("/opt/opencode")).unwrap(), "1.18.31");
        assert_eq!(host.upgrade_calls(), vec![PathBuf::from("/opt/opencode")]);
        assert_eq!(host.upgrade_targets(), vec![Some(Version::new(1, 18, 31))]);
    }

    #[test]
    fn system_static_proxy_is_absent_off_macos() {
        #[cfg(not(target_os = "macos"))]
        assert!(SystemStaticProxy.raw_scutil().is_none());
    }

    #[cfg(unix)]
    #[test]
    fn system_host_rejects_failed_version_commands() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let program = dir.path().join("broken-version");
        std::fs::write(&program, "#!/bin/sh\necho 9.9.9\nexit 1\n").unwrap();
        std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(SystemProcessHost.version(&program).is_err());
    }

    #[test]
    fn bounded_reads_stop_and_report_truncation() {
        let (out, truncated) = read_bounded(std::io::Cursor::new(b"abcdef".to_vec()), 3);
        assert_eq!(out, b"abc");
        assert!(truncated);

        let (out, truncated) = read_bounded(std::io::Cursor::new(b"ab".to_vec()), 5);
        assert_eq!(out, b"ab");
        assert!(!truncated);

        // The drain variant keeps reading to EOF but retains only the prefix.
        let (out, truncated) = read_drain_bounded(std::io::Cursor::new(b"abcdef".to_vec()), 3);
        assert_eq!(out, b"abc");
        assert!(truncated);

        let (out, truncated) = read_drain_bounded(std::io::Cursor::new(b"ab".to_vec()), 5);
        assert_eq!(out, b"ab");
        assert!(!truncated);
    }

    #[test]
    fn fake_git_run_bounded_truncates_deterministically() {
        let fake = FakeGitHost::new().with_stdout(&["big"], "0123456789");
        let output = fake.run_bounded(&["big"], Path::new("."), 4).unwrap();
        assert_eq!(output.stdout, "0123");
        assert!(output.truncated);
    }

    #[test]
    fn process_exit_labels_never_claim_success() {
        assert!(ProcessExit::Code(0).is_success());
        assert!(!ProcessExit::Code(1).is_success());
        assert!(!ProcessExit::Signal(9).is_success());
        assert!(!ProcessExit::Unknown.is_success());
        assert_eq!(ProcessExit::Code(3).label(), "exit 3");
        assert_eq!(ProcessExit::Signal(9).label(), "signal 9");
        assert_eq!(ProcessExit::Unknown.label(), "unknown exit status");
    }

    #[test]
    fn fake_capture_records_calls_and_returns_responses() {
        let fake = FakeCaptureRunner::new()
            .with_success("cargo", &["check"], "ok")
            .with_failure("cargo", &["test"], 101, "nope");
        let ok = fake
            .run("cargo", &["check".to_string()], Path::new("."), 1024)
            .unwrap();
        assert!(ok.success);
        assert_eq!(ok.exit, ProcessExit::Code(0));
        assert_eq!(ok.stdout_lossy(), "ok");
        let bad = fake
            .run("cargo", &["test".to_string()], Path::new("."), 1024)
            .unwrap();
        assert!(!bad.success);
        assert_eq!(bad.exit, ProcessExit::Code(101));
        assert_eq!(bad.stderr_lossy(), "nope");
        assert_eq!(
            fake.calls(),
            vec![
                ("cargo".to_string(), vec!["check".to_string()]),
                ("cargo".to_string(), vec!["test".to_string()]),
            ]
        );
        assert!(fake.run("missing", &[], Path::new("."), 1024).is_err());
    }

    #[test]
    fn lossy_conversion_preserves_invalid_utf8() {
        let output = CapturedOutput {
            exit: ProcessExit::Code(0),
            success: true,
            stdout: vec![0x66, 0xff, 0x6f],
            stderr: vec![0x80],
            stdout_truncated: false,
            stderr_truncated: false,
            duration_ms: 0,
        };
        assert_eq!(output.stdout_lossy(), "f\u{fffd}o");
        assert_eq!(output.stderr_lossy(), "\u{fffd}");
    }

    #[cfg(unix)]
    #[test]
    fn system_capture_reports_status_and_streams() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let program = dir.path().join("producer");
        std::fs::write(
            &program,
            "#!/bin/sh\nprintf 'out'\nprintf 'err' >&2\nexit 7\n",
        )
        .unwrap();
        std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o755)).unwrap();

        let runner = SystemCaptureRunner;
        let output = runner
            .run(program.to_string_lossy().as_ref(), &[], dir.path(), 1024)
            .unwrap();
        assert_eq!(output.exit, ProcessExit::Code(7));
        assert!(!output.success);
        assert!(!output.truncated());
        assert_eq!(output.stdout, b"out");
        assert_eq!(output.stderr, b"err");
    }

    #[cfg(unix)]
    #[test]
    fn system_capture_bounds_runaway_output_and_keeps_the_exit_status() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let program = dir.path().join("flood");
        std::fs::write(
            &program,
            "#!/bin/sh\nprintf 'abcdefghij'\nprintf 'stderr-flood' >&2\nexit 7\n",
        )
        .unwrap();
        std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o755)).unwrap();

        let runner = SystemCaptureRunner;
        let output = runner
            .run(program.to_string_lossy().as_ref(), &[], dir.path(), 4)
            .unwrap();
        // Truncation is reported, but the command's own exit status is kept.
        assert_eq!(output.exit, ProcessExit::Code(7));
        assert!(!output.success);
        assert!(output.stdout_truncated);
        assert!(output.stderr_truncated);
        assert_eq!(output.stdout, b"abcd");
        assert_eq!(output.stderr, b"stde");
    }
}
