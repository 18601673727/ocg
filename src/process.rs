//! Centralized external process execution.
//!
//! This module is the only place that builds a [`std::process::Command`], so
//! every child process the tool ever starts goes through one auditable path.
//!
//! `ProcessHost` provides generic executable discovery and version probing.

use crate::error::{OcgError, Result};
use crate::proxy::StaticProxyProvider;
use serde::{Deserialize, Serialize};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread;
use std::time::{Duration, Instant};

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

/// Why a captured command stopped. Only [`CommandTermination::Completed`] means
/// the command ended on its own; the other two mean OCG terminated its process
/// group, so its exit status is not authoritative.
///
/// Termination is group-wide on Unix. On other platforms only the direct child
/// is stopped, so descendants there are not reaped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandTermination {
    Completed,
    Cancelled,
    DeadlineExceeded,
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
    pub termination: CommandTermination,
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
            termination: CommandTermination::Completed,
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
            termination: CommandTermination::Completed,
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
                termination: CommandTermination::Cancelled,
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
                termination: CommandTermination::Cancelled,
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

/// Longest a captured command may run before OCG terminates its process group.
/// Cancellation normally arrives first; this ceiling covers paths that have no
/// cancellation source, so no command can hold its executor indefinitely.
pub const COMMAND_DEADLINE: Duration = Duration::from_secs(30 * 60);

/// How long OCG waits for a terminated command to be reaped. SIGKILL cannot be
/// caught, so this only expires when the process is stuck in the kernel.
const TERMINATION_GRACE: Duration = Duration::from_secs(5);

/// How long OCG waits for a stream's reader to reach EOF once the command has
/// ended. A reader still blocked after this is held open by a process outside
/// the command's group.
const READER_GRACE: Duration = Duration::from_secs(2);

const POLL_INTERVAL: Duration = Duration::from_millis(10);

/// Bytes retained from one stream. The reader thread fills it and the parent
/// takes what it holds, even if the reader is still blocked on the pipe.
#[derive(Default)]
struct StreamSink {
    bytes: Vec<u8>,
    truncated: bool,
    finished: bool,
}

type SharedSink = Arc<Mutex<StreamSink>>;

fn lock_sink(sink: &SharedSink) -> MutexGuard<'_, StreamSink> {
    sink.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Drain a stream to EOF on its own thread, retaining at most `max` bytes. The
/// drain never stops early, so a full pipe cannot stall the child.
fn spawn_drain<R: Read + Send + 'static>(mut reader: R, max: usize) -> SharedSink {
    let sink = SharedSink::default();
    let shared = Arc::clone(&sink);
    thread::spawn(move || {
        let mut buffer = [0u8; 8192];
        loop {
            match reader.read(&mut buffer) {
                Ok(0) | Err(_) => break,
                Ok(read) => {
                    let mut guard = lock_sink(&shared);
                    let take = read.min(max.saturating_sub(guard.bytes.len()));
                    guard.bytes.extend_from_slice(&buffer[..take]);
                    if take < read {
                        guard.truncated = true;
                    }
                }
            }
        }
        lock_sink(&shared).finished = true;
    });
    sink
}

/// Take what a stream retained. A reader that has not reached EOF within
/// `READER_GRACE` is abandoned and the stream is reported truncated, because
/// its output is incomplete. The thread ends by itself when the pipe closes.
fn collect_sink(sink: SharedSink) -> (Vec<u8>, bool) {
    let started = Instant::now();
    loop {
        let mut guard = lock_sink(&sink);
        if guard.finished || started.elapsed() >= READER_GRACE {
            let complete = guard.finished;
            let truncated = guard.truncated || !complete;
            return (std::mem::take(&mut guard.bytes), truncated);
        }
        drop(guard);
        thread::sleep(POLL_INTERVAL);
    }
}

/// Send SIGKILL to the command's process group. The child leads that group
/// because it was spawned with `process_group(0)`, so the group id is the
/// child's pid. A group that OCG itself belongs to is never signalled.
#[cfg(unix)]
fn signal_group(child: &mut Child) -> std::io::Result<()> {
    use rustix::io::Errno;
    use rustix::process::{getpgrp, kill_process_group, Pid, Signal};
    let group = Pid::from_child(child);
    if group == getpgrp() {
        return Err(std::io::Error::other("command shares OCG's process group"));
    }
    match kill_process_group(group, Signal::KILL) {
        // A group with no members left is the outcome we wanted.
        Ok(()) | Err(Errno::SRCH) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[cfg(not(unix))]
fn signal_group(child: &mut Child) -> std::io::Result<()> {
    child.kill()
}

/// Stop the command and everything it started, then wait, boundedly, until the
/// command itself has been reaped. A failure to signal the group or to reap the
/// command is an error, because the tree may still be running.
fn terminate(child: &mut Child, program: &str) -> Result<ExitStatus> {
    if let Err(error) = signal_group(child) {
        // The leader is the one process still reachable; stop it directly too.
        let _ = child.kill();
        return Err(OcgError::io(format!("cannot terminate {program}"), error));
    }
    let started = Instant::now();
    loop {
        if let Some(status) = child
            .try_wait()
            .map_err(|error| OcgError::io(format!("cannot poll {program}"), error))?
        {
            return Ok(status);
        }
        if started.elapsed() >= TERMINATION_GRACE {
            return Err(OcgError::io(
                format!("cannot terminate {program}"),
                std::io::Error::other("process did not exit after SIGKILL"),
            ));
        }
        thread::sleep(POLL_INTERVAL);
    }
}

/// After a command exits on its own, stop anything it left in its group. Such a
/// process still holds the output pipes and is part of the command's tree, so
/// it must not outlive the command.
#[cfg(unix)]
fn reap_descendants(child: &mut Child, program: &str) -> Result<()> {
    signal_group(child).map_err(|error| {
        OcgError::io(
            format!("cannot terminate processes left by {program}"),
            error,
        )
    })
}

/// Windows has no process groups in this module, so only the direct child is
/// stopped. See the platform limitation on [`CommandTermination`].
#[cfg(not(unix))]
fn reap_descendants(_child: &mut Child, _program: &str) -> Result<()> {
    Ok(())
}

/// Run one command under OCG's lifecycle: the child leads its own process
/// group; normal exit, cancellation, and the deadline are the only ways the
/// call returns; termination always targets the whole group.
fn run_owned(
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
            termination: CommandTermination::Cancelled,
        });
    }
    let max_bytes = max_bytes.max(1);
    let start = Instant::now();
    let mut command = Command::new(program);
    command
        .args(args)
        .current_dir(cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    let mut child = command
        .spawn()
        .map_err(|error| OcgError::io(format!("cannot run {program}"), error))?;
    let stdout = child
        .stdout
        .take()
        .map(|stream| spawn_drain(stream, max_bytes));
    let stderr = child
        .stderr
        .take()
        .map(|stream| spawn_drain(stream, max_bytes));

    let deadline = start + COMMAND_DEADLINE;
    let mut termination = CommandTermination::Completed;
    // An exit observed before the fence closes is a normal completion, so
    // the status is checked first on every poll.
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) => {}
            Err(error) => {
                let _ = signal_group(&mut child);
                return Err(OcgError::io(format!("cannot poll {program}"), error));
            }
        }
        if cancelled() {
            termination = CommandTermination::Cancelled;
            break terminate(&mut child, program)?;
        }
        if Instant::now() >= deadline {
            termination = CommandTermination::DeadlineExceeded;
            break terminate(&mut child, program)?;
        }
        thread::sleep(POLL_INTERVAL);
    };
    if termination == CommandTermination::Completed {
        reap_descendants(&mut child, program)?;
    }
    let (stdout_bytes, stdout_truncated) = stdout.map(collect_sink).unwrap_or_default();
    let (stderr_bytes, stderr_truncated) = stderr.map(collect_sink).unwrap_or_default();
    let completed = termination == CommandTermination::Completed;
    Ok(CapturedOutput {
        exit: if completed {
            process_exit(&status)
        } else {
            ProcessExit::Unknown
        },
        success: completed && status.success(),
        stdout: stdout_bytes,
        stderr: stderr_bytes,
        stdout_truncated,
        stderr_truncated,
        duration_ms: start.elapsed().as_millis().min(u64::MAX as u128) as u64,
        termination,
    })
}

impl CaptureRunner for SystemCaptureRunner {
    fn run(
        &self,
        program: &str,
        args: &[String],
        cwd: &Path,
        max_bytes: usize,
    ) -> Result<CapturedOutput> {
        run_owned(program, args, cwd, max_bytes, &|| false)
    }

    fn run_with_cancellation(
        &self,
        program: &str,
        args: &[String],
        cwd: &Path,
        max_bytes: usize,
        cancelled: &dyn Fn() -> bool,
    ) -> Result<CapturedOutput> {
        run_owned(program, args, cwd, max_bytes, cancelled)
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
