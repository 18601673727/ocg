//! Centralized external process execution.
//!
//! This module is the only place that builds a [`std::process::Command`], so
//! every child process the tool ever starts goes through one auditable path.
//!
//! `ProcessHost` provides generic executable discovery and version probing.

use crate::error::{OcgError, Result};
use crate::proxy::StaticProxyProvider;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Instant;

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

/// Generic executable discovery and version probing.
pub trait ProcessHost: Send + Sync {
    /// Resolve a bare program name against `PATH`.
    fn find_in_path(&self, program: &str) -> Option<PathBuf>;

    /// Run `<program> --version` and return the trimmed output.
    fn version(&self, program: &Path) -> Result<String>;
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

    /// Run with an OCG-owned cancellation fence. Implementations that cannot
    /// interrupt their child still get the pre/post check from this default;
    /// the system implementation terminates the child when the fence closes.
    fn run_cancellable(
        &self,
        program: &str,
        args: &[String],
        cwd: &Path,
        max_bytes: usize,
        cancelled: &AtomicBool,
    ) -> Result<CapturedOutput> {
        self.run_with_cancellation(program, args, cwd, max_bytes, &|| {
            cancelled.load(Ordering::SeqCst)
        })
    }

    fn run_with_cancellation(
        &self,
        program: &str,
        args: &[String],
        cwd: &Path,
        max_bytes: usize,
        cancelled: &dyn Fn() -> bool,
    ) -> Result<CapturedOutput> {
        if cancelled() {
            return Ok(CapturedOutput {
                exit: ProcessExit::Unknown,
                success: false,
                stdout: Vec::new(),
                stderr: b"cancelled".to_vec(),
                stdout_truncated: false,
                stderr_truncated: false,
                duration_ms: 0,
            });
        }
        let output = self.run(program, args, cwd, max_bytes)?;
        if cancelled() {
            return Ok(CapturedOutput {
                exit: ProcessExit::Unknown,
                success: false,
                stdout: output.stdout,
                stderr: output.stderr,
                stdout_truncated: output.stdout_truncated,
                stderr_truncated: output.stderr_truncated,
                duration_ms: output.duration_ms,
            });
        }
        Ok(output)
    }
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

    fn run_with_cancellation(
        &self,
        program: &str,
        args: &[String],
        cwd: &Path,
        max_bytes: usize,
        cancelled: &dyn Fn() -> bool,
    ) -> Result<CapturedOutput> {
        if cancelled() {
            return Ok(CapturedOutput {
                exit: ProcessExit::Unknown,
                success: false,
                stdout: Vec::new(),
                stderr: b"cancelled".to_vec(),
                stdout_truncated: false,
                stderr_truncated: false,
                duration_ms: 0,
            });
        }
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
        let stdout_handle = child
            .stdout
            .take()
            .map(|stdout| thread::spawn(move || read_drain_bounded(stdout, max_bytes)));
        let stderr_handle = child
            .stderr
            .take()
            .map(|stderr| thread::spawn(move || read_drain_bounded(stderr, max_bytes)));
        let mut cancelled_child = false;
        let mut kill_error_reported = false;
        loop {
            if cancelled() && !cancelled_child {
                match child.kill() {
                    Ok(()) => cancelled_child = true,
                    Err(error) => {
                        if !kill_error_reported {
                            tracing::warn!(%error, program, "cannot kill cancelled child process; retrying until it exits");
                            kill_error_reported = true;
                        }
                    }
                }
            }
            if child
                .try_wait()
                .map_err(|error| OcgError::io(format!("cannot poll {program}"), error))?
                .is_some()
            {
                break;
            }
            thread::sleep(std::time::Duration::from_millis(10));
        }
        let status = child
            .wait()
            .map_err(|error| OcgError::io(format!("cannot wait for {program}"), error))?;
        let (stdout_bytes, stdout_truncated) = stdout_handle
            .map(|handle| handle.join().unwrap_or_default())
            .unwrap_or_default();
        let (stderr_bytes, stderr_truncated) = stderr_handle
            .map(|handle| handle.join().unwrap_or_default())
            .unwrap_or_default();
        Ok(CapturedOutput {
            exit: if cancelled_child {
                ProcessExit::Unknown
            } else {
                process_exit(&status)
            },
            success: !cancelled_child && status.success(),
            stdout: stdout_bytes,
            stderr: stderr_bytes,
            stdout_truncated,
            stderr_truncated,
            duration_ms: start.elapsed().as_millis().min(u64::MAX as u128) as u64,
        })
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
