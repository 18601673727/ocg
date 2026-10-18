//! `workspace_run_command`: runs one of OCG's fixed commands in a Workspace
//! the operator authorized, and returns how it exited and what it printed.
//!
//! A call names a preset, never a command line. `cargo_check` runs
//! `cargo check --all-targets` and `cargo_test` runs
//! `cargo test --all-targets`; no other program, argument, flag, directory
//! or variable can be chosen. The argument vector is OCG's own, and the
//! process is started directly, never through a shell. Its program is found
//! on the PATH OCG itself was started with, among absolute entries only, so
//! never in a path a call names or the Workspace provides by a relative
//! entry. A program that is not found is refused before anything starts.
//!
//! The process runs in the Workspace root, once the root is checked to still
//! be the directory authorized. Its environment is built, not inherited:
//! only [`PASSED_VARIABLES`] are taken from OCG's environment, with PATH
//! reduced to its absolute entries, and [`SET_VARIABLES`] are added. Nothing
//! else of OCG's environment reaches it: no Provider credential, credential
//! reference or OCG setting.
//!
//! The process is started from a thread of its own, as the leader of a new
//! process group, and supervised there; the executor only awaits the
//! supervisor's report, so it never blocks on the process. Its stdout and
//! stderr are each read to their end by a thread of their own, so the process
//! never waits on a full pipe, and kept within a bound. Once the process has
//! exited, and before it is reaped, the rest of its group is killed, so a
//! process it left behind in the group does not outlive the call. When the
//! call is stopped instead, by its own [`COMMAND_TIMEOUT`], by the tool
//! loop's limit or by cancellation, the whole group is killed (`SIGKILL`)
//! and the supervisor reaps the process. A group is only ever signalled
//! while its leader is unreaped, so its id cannot be another's.
//!
//! An exit, with any status, is a completed call: a failing build or test is
//! a result for the Provider to read. A call is refused (not applied) only
//! when nothing was started; once the process has started, a call whose end
//! cannot be observed, or that was stopped, leaves its effect uncertain.
//!
//! Output: at most [`OUTPUT_BYTES`] of stdout and stderr together are
//! returned, each stream whole or as its beginning and end around a marker
//! that says how many bytes were omitted, and `truncated` says whether
//! anything was. The serialized result stays within [`RESULT_BYTES`].
//!
//! What this is not: a sandbox. Build scripts, procedural macros and tests
//! run arbitrary code with the permissions of the OCG process; they may read
//! or write anything it may, outside the Workspace too, and use the network.
//! Cargo may fetch the dependencies a manifest names, and a rustup proxy may
//! install the toolchain a project names. The working directory and the
//! environment restrict none of this. A descendant that leaves the process
//! group (`setsid`, `setpgid`, daemonizing) is not reached by the group kill
//! and may keep running; when it keeps the output open, the call is not seen
//! to end and stops at its limit as uncertain. The root is checked before
//! the process starts, then entered by path. This tool is for a trusted,
//! local Workspace of a single operator; it must never be offered to a
//! remote or untrusted caller.
//!
//! Every call is recorded with its arguments before it runs, and its result,
//! the output included, is kept in plaintext tool evidence and sent to the
//! Provider. Output may carry secrets or private paths; nothing here logs it,
//! and no failure message repeats it.

use std::collections::VecDeque;
use std::ffi::OsString;
use std::io::Read;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::ExitStatusExt;
use std::path::PathBuf;
use std::process::{Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use serde_json::{Value, json};

#[cfg(test)]
use rustix::io::Errno;
#[cfg(test)]
use rustix::process::Pid;

use crate::orchestration::blocking::{self, FinishError, Kind, Process, StopSignal};
use crate::orchestration::execution_runtime::{ToolExecution, ToolLoopLimits};
use crate::orchestration::tools::{
    ToolFailure, ToolFuture, ToolHandler, ToolRegistry, ToolRegistryError, ToolStart,
};
use crate::orchestration::workspace_read::{self, Refusal as PathRefusal, Workspace};
use crate::orchestration::{workspace_discovery, workspace_write};
use crate::persistence::{JobId, ToolCallRef, ToolEffect};

/// The name the tool is offered and recorded under.
pub const TOOL_NAME: &str = "workspace_run_command";

/// Identifies this handler's behavior in every call it records.
const HANDLER_VERSION: &str = "workspace_run_command-4";

/// Longest a command may run before it is stopped.
pub const COMMAND_TIMEOUT: Duration = Duration::from_secs(120);

/// How long a stopped command is given to be reaped before the call returns
/// without it, and how much longer than its command the tool loop gives a
/// call, so the command's own limit stops it first.
const STOP_GRACE: Duration = blocking::STOP_GRACE;

/// The most output, of stdout and stderr together, a result carries.
pub const OUTPUT_BYTES: usize = 12 * 1024;

/// The largest serialized result: within the coding loop's limit of a single
/// result, whatever the output's JSON escaping costs.
pub const RESULT_BYTES: usize = 15 * 1024;

/// The variables taken from OCG's own environment, when set: what locates
/// the toolchain and its temporary files. PATH is taken too, reduced to its
/// absolute entries.
pub const PASSED_VARIABLES: &[&str] = &[
    "HOME",
    "TMPDIR",
    "CARGO_HOME",
    "RUSTUP_HOME",
    "RUSTUP_TOOLCHAIN",
];

/// Variables every command gets.
pub const SET_VARIABLES: &[(&str, &str)] = &[("CARGO_TERM_COLOR", "never")];

/// The commands a call may name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Preset {
    CargoCheck,
    CargoTest,
}

impl Preset {
    const ALL: [Self; 2] = [Self::CargoCheck, Self::CargoTest];

    fn of(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|preset| preset.name() == name)
    }

    fn name(self) -> &'static str {
        match self {
            Self::CargoCheck => "cargo_check",
            Self::CargoTest => "cargo_test",
        }
    }

    fn program(self) -> &'static str {
        "cargo"
    }

    fn arguments(self) -> &'static [&'static str] {
        match self {
            Self::CargoCheck => &["check", "--all-targets", "--message-format=json"],
            Self::CargoTest => &["test", "--all-targets"],
        }
    }
}

/// Why a call started nothing. Its message is recorded and may reach the
/// Provider.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
enum Refusal {
    #[error("the arguments do not name exactly one supported command")]
    InvalidArguments,
    #[error(transparent)]
    Path(#[from] PathRefusal),
    #[error("the command's program was not found on OCG's PATH")]
    ProgramNotFound,
    #[error("the command could not be started")]
    NotStarted,
    #[error("another command is still running")]
    CommandBusy,
}

/// Why a started command's effect is not known. Its message never repeats
/// the command's output.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
enum Uncertainty {
    #[error("the command did not finish within {0:?} and was stopped")]
    TimedOut(Duration),
    #[error("the command was started, but how it ended could not be observed")]
    NotObserved,
    #[error("the command ended, but its exit status could not be observed")]
    ExitNotObserved,
    #[error("the command was started, but its output could not be captured")]
    OutputNotCaptured,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Failure {
    Refused(Refusal),
    Uncertain(Uncertainty),
}

impl From<Refusal> for Failure {
    fn from(refusal: Refusal) -> Self {
        Self::Refused(refusal)
    }
}

impl From<Uncertainty> for Failure {
    fn from(uncertainty: Uncertainty) -> Self {
        Self::Uncertain(uncertainty)
    }
}

impl From<Failure> for ToolFailure {
    fn from(failure: Failure) -> Self {
        match failure {
            Failure::Refused(refusal) => Self::not_applied(refusal.to_string()),
            Failure::Uncertain(uncertainty) => Self::uncertain(uncertainty.to_string()),
        }
    }
}

/// The preset a call names. The schema already allows nothing else; the
/// arguments are checked again here, so nothing else can ever run.
fn parse(arguments: &Value) -> Result<Preset, Refusal> {
    let object = arguments.as_object().ok_or(Refusal::InvalidArguments)?;
    if object.len() != 1 {
        return Err(Refusal::InvalidArguments);
    }
    object
        .get("command")
        .and_then(Value::as_str)
        .and_then(Preset::of)
        .ok_or(Refusal::InvalidArguments)
}

/// What a command may see of OCG's environment: where its program is looked
/// for, and every variable it gets. Read once, from the operator's
/// environment, when the tool is registered.
#[derive(Clone, Debug)]
pub(crate) struct CommandEnvironment {
    search: Vec<PathBuf>,
    variables: Vec<(OsString, OsString)>,
}

impl CommandEnvironment {
    /// From the environment OCG was started with.
    pub(crate) fn of_process() -> Self {
        Self::from_source(|name| std::env::var_os(name))
    }

    /// From the variables `source` gives.
    pub(crate) fn from_source(source: impl Fn(&str) -> Option<OsString>) -> Self {
        let search = source("PATH")
            .map(|path| {
                std::env::split_paths(&path)
                    .filter(|directory| directory.is_absolute())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let mut variables = Vec::new();
        if let Ok(path) = std::env::join_paths(&search)
            && !search.is_empty()
        {
            variables.push((OsString::from("PATH"), path));
        }
        for name in PASSED_VARIABLES {
            if let Some(value) = source(name) {
                variables.push((OsString::from(name), value));
            }
        }
        for (name, value) in SET_VARIABLES {
            variables.push((OsString::from(name), OsString::from(value)));
        }
        Self { search, variables }
    }

    /// The first executable regular file named `program` in the search
    /// path. The path is used as found, never resolved further, since a
    /// toolchain proxy may tell the tool to run by the name it is run as.
    fn find(&self, program: &str) -> Option<PathBuf> {
        self.search
            .iter()
            .map(|directory| directory.join(program))
            .find(|candidate| {
                std::fs::metadata(candidate).is_ok_and(|metadata| {
                    metadata.is_file() && metadata.permissions().mode() & 0o111 != 0
                })
            })
    }
}

/// Everything needed to start one command, prepared by trusted code.
struct Invocation {
    program: PathBuf,
    arguments: &'static [&'static str],
    directory: PathBuf,
    variables: Vec<(OsString, OsString)>,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum CommandCall {
    Job(ToolCallRef),
    Standalone(u64),
}

type ActiveCommands =
    std::collections::BTreeMap<(PathBuf, CommandCall), (StopSignal, flume::Receiver<bool>)>;

static ACTIVE: OnceLock<Mutex<ActiveCommands>> = OnceLock::new();
static NEXT_ACTIVE: AtomicU64 = AtomicU64::new(1);

fn active() -> &'static Mutex<ActiveCommands> {
    ACTIVE.get_or_init(|| Mutex::new(std::collections::BTreeMap::new()))
}

struct StopOnDrop(StopSignal);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.request();
    }
}

/// Up to [`OUTPUT_BYTES`] from the beginning of one output stream and up to
/// as many from its end, and how long it was.
#[derive(Default)]
struct Capture {
    head: Vec<u8>,
    tail: VecDeque<u8>,
    total: usize,
    /// Reading failed before the stream's end.
    unread: bool,
}

impl Capture {
    fn text(&self) -> Option<String> {
        if self.unread || self.total != self.head.len() + self.tail.len() {
            return None;
        }
        String::from_utf8(self.head.iter().chain(&self.tail).copied().collect()).ok()
    }

    fn push(&mut self, bytes: &[u8]) {
        self.total = self.total.saturating_add(bytes.len());
        let room = OUTPUT_BYTES
            .saturating_sub(self.head.len())
            .min(bytes.len());
        let (head, rest) = bytes.split_at(room);
        self.head.extend_from_slice(head);
        self.tail.extend(rest);
        let excess = self.tail.len().saturating_sub(OUTPUT_BYTES);
        self.tail.drain(..excess);
    }

    /// The stream in at most `budget` bytes, and whether any was left out:
    /// whole, or its first and last bytes around a marker saying how many
    /// were omitted. Bytes that are not UTF-8 are replaced.
    fn render(&self, stream: &str, budget: usize) -> (String, bool) {
        let kept = self
            .head
            .iter()
            .chain(&self.tail)
            .copied()
            .collect::<Vec<_>>();
        let unread = if self.unread {
            format!("\n[OCG could not read the rest of {stream}]")
        } else {
            String::new()
        };
        if self.total <= budget {
            return (
                String::from_utf8_lossy(&kept).into_owned() + &unread,
                self.unread,
            );
        }
        let first = budget / 2;
        let last = budget - first;
        let omitted = self.total - budget;
        let text = format!(
            "{}\n[OCG omitted {omitted} bytes of {stream} here]\n{}{unread}",
            String::from_utf8_lossy(&kept[..first]),
            String::from_utf8_lossy(&kept[kept.len() - last..]),
        );
        (text, true)
    }
}

/// How `budget` bytes are shared by two streams of these lengths: each gets
/// all it has when that fits, and what one leaves goes to the other.
fn shares(stdout: usize, stderr: usize, budget: usize) -> (usize, usize) {
    let half = budget / 2;
    if stdout.saturating_add(stderr) <= budget {
        (stdout, stderr)
    } else if stdout <= half {
        (stdout, budget - stdout)
    } else if stderr <= half {
        (budget - stderr, stderr)
    } else {
        (half, budget - half)
    }
}

/// What a command that ran to its end left.
struct Ended {
    status: ExitStatus,
    stdout: Capture,
    stderr: Capture,
}

fn compact_check(stdout: &str) -> Option<String> {
    let mut diagnostics = Vec::new();
    let mut success = None;
    for line in stdout.lines().filter(|line| !line.trim().is_empty()) {
        let record: Value = serde_json::from_str(line).ok()?;
        match record["reason"].as_str()? {
            "compiler-message" => {
                let message = &record["message"];
                let level = message["level"].as_str()?;
                if !matches!(level, "error" | "warning") {
                    continue;
                }
                let spans = message["spans"].as_array()?;
                let span = spans
                    .iter()
                    .find(|span| span["is_primary"] == true)
                    .or_else(|| spans.first());
                let (path, line, column) = match span {
                    Some(span) => (
                        span["file_name"].as_str()?,
                        span["line_start"].as_u64()?,
                        span["column_start"].as_u64()?,
                    ),
                    None => ("<unknown>", 0, 0),
                };
                let code = message["code"]["code"]
                    .as_str()
                    .map(|code| format!("[{code}]"))
                    .unwrap_or_default();
                let text = message["message"]
                    .as_str()?
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" ");
                diagnostics.push((
                    level == "warning",
                    format!("{path}:{line}:{column} {level}{code} {text}"),
                ));
            }
            "compiler-artifact" | "build-script-executed" => {}
            "build-finished" => success = record["success"].as_bool(),
            _ => return None,
        }
    }
    if !success? && !diagnostics.iter().any(|(warning, _)| !warning) {
        return None;
    }
    diagnostics.sort_by_key(|(warning, _)| *warning);
    Some(
        diagnostics
            .into_iter()
            .map(|(_, line)| line)
            .collect::<Vec<_>>()
            .join("\n"),
    )
}

fn compact_test(stdout: &str) -> Option<String> {
    let lines = stdout.lines().collect::<Vec<_>>();
    let mut output = lines
        .iter()
        .filter(|line| {
            line.starts_with("test result: ok.") || line.starts_with("test result: FAILED.")
        })
        .map(|line| (*line).to_owned())
        .collect::<Vec<_>>();
    if output.is_empty() {
        return None;
    }
    for line in &lines {
        let Some(name) = line
            .strip_prefix("test ")
            .and_then(|line| line.strip_suffix(" ... FAILED"))
        else {
            continue;
        };
        output.push(name.to_owned());
        let header = format!("---- {name} stdout ----");
        let Some(index) = lines.iter().position(|line| *line == header) else {
            continue;
        };
        let details = lines[index + 1..]
            .iter()
            .take_while(|line| {
                !line.starts_with("---- ")
                    && !line.starts_with("failures:")
                    && !line.starts_with("test result:")
            })
            .copied()
            .collect::<Vec<_>>();
        let panic = details
            .iter()
            .position(|line| line.starts_with("thread '") && line.contains("panicked at"))
            .unwrap_or(0);
        output.extend(
            details[panic..]
                .iter()
                .filter(|line| !line.trim().is_empty())
                .take(4)
                .map(|line| (*line).to_owned()),
        );
    }
    Some(output.join("\n"))
}

fn compact(preset: Preset, ended: &Ended) -> Option<Capture> {
    ended.status.code()?;
    let stdout = ended.stdout.text()?;
    ended.stderr.text()?;
    let text = match preset {
        Preset::CargoCheck => compact_check(&stdout)?,
        Preset::CargoTest => {
            if !ended.status.success()
                && !stdout
                    .lines()
                    .any(|line| line.starts_with("test result: FAILED."))
            {
                return None;
            }
            compact_test(&stdout)?
        }
    };
    if !ended.status.success() && text.is_empty() {
        return None;
    }
    let mut capture = Capture::default();
    capture.push(text.as_bytes());
    Some(capture)
}

/// The call's result: the command, its exit, and its output within the
/// bounds. A process ended by a signal has no exit code, and its signal is
/// given instead.
fn result(preset: Preset, ended: &Ended) -> Value {
    let compact = compact(preset, ended);
    let empty = Capture::default();
    let (stdout, stderr) = match &compact {
        Some(stdout) => (stdout, &empty),
        None => (&ended.stdout, &ended.stderr),
    };
    let mut budget = OUTPUT_BYTES;
    loop {
        let (stdout_share, stderr_share) = shares(stdout.total, stderr.total, budget);
        let (stdout, stdout_cut) = stdout.render("stdout", stdout_share);
        let (stderr, stderr_cut) = stderr.render("stderr", stderr_share);
        let mut value = json!({
            "command": preset.name(),
            "exit_code": ended.status.code(),
            "stdout": stdout,
            "stderr": stderr,
            "truncated": stdout_cut || stderr_cut,
        });
        if let Some(signal) = ended.status.signal() {
            value["signal"] = json!(signal);
        }
        if budget == 0 || value.to_string().len() <= RESULT_BYTES {
            return value;
        }
        budget /= 2;
    }
}

/// Starts the command and supervises it to its end, on the calling thread,
/// which is never the executor's.
fn supervise(
    invocation: Invocation,
    stop: StopSignal,
    start: ToolStart,
    job: Option<JobId>,
) -> Result<Ended, Failure> {
    let mut command = Command::new(&invocation.program);
    command
        .args(invocation.arguments)
        .current_dir(&invocation.directory)
        .env_clear()
        .envs(
            invocation
                .variables
                .iter()
                .map(|(name, value)| (name, value)),
        )
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut process = Process::spawn(command, Kind::Command, stop.clone(), job).map_err(|error| {
        if error.kind() == std::io::ErrorKind::TimedOut {
            Refusal::CommandBusy
        } else {
            Refusal::NotStarted
        }
    })?;
    start.mark_started();
    let (stdout_pipe, stderr_pipe) = process.take_output();
    let readers = match (stdout_pipe, stderr_pipe) {
        (Some(stdout), Some(stderr)) => {
            reader(stdout).and_then(|stdout| reader(stderr).map(|stderr| (stdout, stderr)))
        }
        _ => None,
    };
    let Some((stdout, stderr)) = readers else {
        stop.request();
        if let Err(error) = process.finish(|| true) {
            match error {
                FinishError::NotProven(_) => return Err(Uncertainty::NotObserved.into()),
                FinishError::Wait(error) => {
                    log::debug!("command wait failed after cleanup: {error}")
                }
            }
        }
        return Err(Uncertainty::OutputNotCaptured.into());
    };
    let status = match process.finish(|| stop.requested()) {
        Ok(status) => status,
        Err(FinishError::NotProven(_)) => return Err(Uncertainty::NotObserved.into()),
        Err(FinishError::Wait(_)) => return Err(Uncertainty::ExitNotObserved.into()),
    };
    match (stdout.join(), stderr.join()) {
        (Ok(stdout), Ok(stderr)) => Ok(Ended {
            status,
            stdout,
            stderr,
        }),
        _ => Err(Uncertainty::OutputNotCaptured.into()),
    }
}

/// Reads `pipe` to its end on a thread of its own.
fn reader(mut pipe: impl Read + Send + 'static) -> Option<JoinHandle<Capture>> {
    thread::Builder::new()
        .name("ocg-command-output".to_owned())
        .spawn(move || {
            let mut capture = Capture::default();
            let mut buffer = [0; 8 * 1024];
            loop {
                match pipe.read(&mut buffer) {
                    Ok(0) => break,
                    Ok(read) => capture.push(&buffer[..read]),
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                    Err(_) => {
                        capture.unread = true;
                        break;
                    }
                }
            }
            capture
        })
        .ok()
}

/// The handler of `workspace_run_command`, bound to one authorized Workspace
/// and the environment its commands get.
struct RunCommand {
    workspace: Workspace,
    environment: CommandEnvironment,
    timeout: Duration,
}

impl RunCommand {
    /// The command `preset` names, ready to start: the Workspace root must
    /// still be the directory authorized, and the program must be found.
    fn prepare(&self, preset: Preset) -> Result<Invocation, Refusal> {
        self.workspace.open_root()?;
        let program = self
            .environment
            .find(preset.program())
            .ok_or(Refusal::ProgramNotFound)?;
        Ok(Invocation {
            program,
            arguments: preset.arguments(),
            directory: self.workspace.root().to_path_buf(),
            variables: self.environment.variables.clone(),
        })
    }
}

impl ToolHandler for RunCommand {
    fn version(&self) -> &str {
        HANDLER_VERSION
    }

    /// A build or a test may run project code that changes anything, so a
    /// command run again may change things again.
    fn effect(&self) -> ToolEffect {
        ToolEffect::NonIdempotent
    }

    fn spawns(&self) -> bool {
        true
    }

    fn stop(
        &self,
        job: JobId,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ToolFailure>> + '_>> {
        let root = self.workspace.root().to_path_buf();
        Box::pin(async move {
            let commands = active()
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .iter()
                .filter(|((active_root, call), _)| {
                    active_root == &root
                        && matches!(call, CommandCall::Job(call) if call.job == job)
                })
                .map(|(call, command)| (call.clone(), command.clone()))
                .collect::<Vec<_>>();
            for (_, (signal, _)) in &commands {
                signal.request();
            }
            let mut proven = true;
            for (call, (_, finished)) in commands {
                proven &= finished.recv_async().await == Ok(true);
                active()
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .remove(&call);
            }
            if proven {
                Ok(())
            } else {
                Err(ToolFailure::uncertain(
                    "the command's process cleanup was not proven",
                ))
            }
        })
    }

    /// Starts the command on a supervising thread and awaits its report
    /// within the command's limit. Dropping the call, as its time limit or
    /// cancellation does, stops the command.
    fn call(&self, arguments: Value) -> ToolFuture<'_> {
        self.invoke(
            CommandCall::Standalone(NEXT_ACTIVE.fetch_add(1, Ordering::Relaxed)),
            arguments,
            ToolStart::default(),
        )
    }

    fn call_for(&self, call: ToolCallRef, arguments: Value) -> ToolFuture<'_> {
        self.invoke(CommandCall::Job(call), arguments, ToolStart::default())
    }

    fn call_with_start(
        &self,
        call: ToolCallRef,
        arguments: Value,
        start: ToolStart,
    ) -> ToolFuture<'_> {
        self.invoke(CommandCall::Job(call), arguments, start)
    }
}

impl RunCommand {
    fn invoke(&self, call: CommandCall, arguments: Value, start: ToolStart) -> ToolFuture<'_> {
        Box::pin(async move {
            let preset = parse(&arguments).map_err(Failure::from)?;
            let invocation = self.prepare(preset).map_err(Failure::from)?;
            let stop = StopSignal::new();
            let _stop_on_drop = StopOnDrop(stop.clone());
            let (report, reported) = flume::bounded(1);
            let supervised = stop.clone();
            let mutation = workspace_read::WorkspaceMutation::begin();
            let (proof, proven) = flume::bounded(1);
            let active_id = (self.workspace.root().to_path_buf(), call);
            active()
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(active_id.clone(), (stop.clone(), proven));
            let process_start = start.clone();
            let job = match call {
                CommandCall::Job(call_ref) => Some(call_ref.job),
                CommandCall::Standalone(_) => None,
            };
            thread::Builder::new()
                .name("ocg-command".to_owned())
                .spawn(move || {
                    let _mutation = mutation;
                    let result = supervise(invocation, supervised, process_start, job);
                    if proof
                        .send(
                            result.as_ref().err()
                                != Some(&Failure::Uncertain(Uncertainty::NotObserved)),
                        )
                        .is_err()
                    {
                        log::debug!("command cleanup proof receiver was dropped");
                    }
                    if report.send(result).is_err() {
                        log::debug!("command report receiver was dropped after cleanup");
                    }
                })
                .map_err(|_| {
                    active()
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .remove(&active_id);
                    Failure::from(Refusal::NotStarted)
                })?;
            let expires = async {
                start.wait().await;
                ntex::time::sleep(start.remaining(self.timeout)).await;
            };
            let (ended, timed_out) =
                match ntex::util::select(reported.recv_async(), expires).await {
                    ntex::util::Either::Left(Ok(ended)) => (ended, false),
                    ntex::util::Either::Left(Err(_)) => {
                        return Err(Failure::from(Uncertainty::NotObserved).into());
                    }
                    ntex::util::Either::Right(()) => {
                        stop.request();
                        let ended = reported.recv_async().await.map_err(|_| {
                            ToolFailure::from(Failure::from(Uncertainty::NotObserved))
                        })?;
                        (ended, true)
                    }
                };
            if ended.as_ref().err() != Some(&Failure::Uncertain(Uncertainty::NotObserved)) {
                active()
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .remove(&active_id);
            }
            let ended = ended?;
            if timed_out {
                return Err(Failure::from(Uncertainty::TimedOut(self.timeout)).into());
            }
            Ok(result(preset, &ended))
        })
    }
}

/// The parameters a call must match exactly: one preset.
fn parameters() -> Value {
    json!({
        "type": "object",
        "properties": {
            "command": {
                "type": "string",
                "enum": Preset::ALL.map(Preset::name),
                "description": "cargo_check runs `cargo check --all-targets`; cargo_test runs `cargo test --all-targets`"
            }
        },
        "required": ["command"],
        "additionalProperties": false
    })
}

/// Registers `workspace_run_command` for `workspace` in `registry`, without
/// authorizing it. Its commands get `environment` and run for at most
/// `timeout`.
pub(crate) fn register(
    registry: &ToolRegistry,
    workspace: Workspace,
    environment: CommandEnvironment,
    timeout: Duration,
) -> Result<(), ToolRegistryError> {
    registry.register(
        TOOL_NAME,
        Some(
            "Run one of OCG's fixed commands in the authorized Workspace root and return its exit code, stdout and stderr. A nonzero exit code, such as a failing build or test, is a normal result to read, not a failure of this tool. Output is limited to about 12 KiB; `truncated` says whether part of it was omitted. No other command, argument, directory or environment can be chosen.",
        ),
        parameters(),
        Arc::new(RunCommand {
            workspace,
            environment,
            timeout,
        }),
    )
}

/// The tool loop limits of a coding execution: several rounds of reading,
/// editing and testing, with every call given as long as a command may run
/// and a little more, so a command is stopped by its own limit first.
pub(crate) fn coding_limits(timeout: Duration) -> ToolLoopLimits {
    ToolLoopLimits {
        max_provider_requests: 12,
        max_tool_calls: 16,
        max_tool_result_bytes: 16 * 1024,
        max_tool_results_bytes: 64 * 1024,
        handler_timeout: timeout.saturating_add(STOP_GRACE),
    }
}

/// The tools of an execution that may read, write and build `workspace`:
/// `workspace_read_file`, `workspace_write_file`, `workspace_run_command`,
/// `workspace_list_directory`, `workspace_search`, `workspace_code` and
/// lazy `workspace_fts_search`, registered in a registry of their own and authorized
/// there, with [`coding_limits`]. Commands get the environment
/// OCG was started with, filtered, and run for at most [`COMMAND_TIMEOUT`].
///
/// # Errors
///
/// Returns a registration failure, which these tools' fixed definitions
/// never cause.
pub fn code_tools(workspace: Workspace) -> Result<ToolExecution, ToolRegistryError> {
    code_tools_with(workspace, CommandEnvironment::of_process(), COMMAND_TIMEOUT)
}

/// [`code_tools`] with the given command environment and limit.
pub(crate) fn code_tools_with(
    workspace: Workspace,
    environment: CommandEnvironment,
    timeout: Duration,
) -> Result<ToolExecution, ToolRegistryError> {
    let registry = ToolRegistry::default();
    let limits = coding_limits(timeout);
    workspace_read::register(&registry, workspace.clone())?;
    workspace_write::register(&registry, workspace.clone())?;
    register(&registry, workspace.clone(), environment, timeout)?;
    workspace_discovery::register(&registry, workspace.clone())?;
    crate::orchestration::workspace_code::register(
        &registry,
        workspace.clone(),
        limits.max_tool_result_bytes,
        timeout,
    )?;
    let mut names = vec![
        workspace_read::TOOL_NAME,
        workspace_write::TOOL_NAME,
        TOOL_NAME,
        workspace_discovery::LIST_TOOL_NAME,
        workspace_discovery::SEARCH_TOOL_NAME,
        crate::orchestration::workspace_code::TOOL_NAME,
    ];
    super::workspace_fts::register(&registry, workspace.clone())?;
    names.push(super::workspace_fts::TOOL_NAME);
    for name in &names {
        registry.authorize(name)?;
    }
    Ok(ToolExecution {
        offer: registry.offer(&names)?,
        limits,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::future::Future;
    use std::path::Path;
    use std::time::Instant;

    use ntex::util::{Either, select};

    use super::*;
    use crate::data_directory::DataDirectory;
    use crate::test_support::{TestRoot, WAIT, data_directory, temporary_directory};

    const SUITE: &str = "ocg-workspace-command";

    /// A Workspace beside a data directory of its own, and a directory
    /// holding a fake `cargo`, a `/bin/sh` script.
    struct Setup {
        root: PathBuf,
        bin: PathBuf,
        directory: DataDirectory,
        _guards: [TestRoot; 3],
    }

    fn setup(name: &str, script: &str) -> Setup {
        let (workspace_guard, root) = temporary_directory(SUITE, &format!("{name}-workspace"));
        let (bin_guard, bin) = temporary_directory(SUITE, &format!("{name}-bin"));
        let (data_guard, directory) = data_directory(SUITE, name);
        install(&bin, script, 0o755);
        Setup {
            root,
            bin,
            directory,
            _guards: [workspace_guard, bin_guard, data_guard],
        }
    }

    /// Writes a fake `cargo` and, when it is executable, runs it once with
    /// `warm`, which it answers at once: a new executable's first start may
    /// be slow, as on macOS, and that must not count against a limit.
    fn install(bin: &Path, script: &str, mode: u32) {
        let cargo = bin.join("cargo");
        std::fs::write(
            &cargo,
            format!("#!/bin/sh\n[ \"$1\" = warm ] && exit 0\n{script}\n"),
        )
        .expect("write a fake cargo");
        std::fs::set_permissions(&cargo, std::fs::Permissions::from_mode(mode))
            .expect("set its mode");
        if mode & 0o111 != 0 {
            let warmed = Command::new(&cargo).arg("warm").status().expect("start it");
            assert!(warmed.success());
        }
    }

    /// OCG's environment as a test pretends it: `path` and HOME, nothing
    /// else.
    fn environment_with_path(path: String) -> CommandEnvironment {
        CommandEnvironment::from_source(move |name| match name {
            "PATH" => Some(OsString::from(&path)),
            "HOME" => std::env::var_os("HOME"),
            _ => None,
        })
    }

    impl Setup {
        /// The fake toolchain first on PATH, then the system's.
        fn environment(&self) -> CommandEnvironment {
            environment_with_path(format!("{}:/usr/bin:/bin", self.bin.display()))
        }

        fn handler_with(&self, environment: CommandEnvironment, timeout: Duration) -> RunCommand {
            RunCommand {
                workspace: Workspace::authorize(&self.root, &self.directory).expect("authorize"),
                environment,
                timeout,
            }
        }

        fn handler(&self, timeout: Duration) -> RunCommand {
            self.handler_with(self.environment(), timeout)
        }

        /// The process id the script wrote to `file` in the Workspace.
        fn pid(&self, file: &str) -> Pid {
            let text = std::fs::read_to_string(self.root.join(file)).expect("a pid file");
            Pid::from_raw(text.trim().parse().expect("a pid")).expect("a positive pid")
        }

        fn ran(&self) -> bool {
            self.root.join("ran").exists()
        }
    }

    fn on_ntex<F: Future + 'static>(future: F) -> F::Output
    where
        F::Output: 'static,
    {
        ntex::rt::System::new("command-test", ntex::rt::DefaultRuntime).block_on(future)
    }

    /// Runs one call as the tool loop does, on an executor of its own.
    fn run(handler: RunCommand, arguments: Value) -> Result<Value, ToolFailure> {
        on_ntex(async move { handler.call(arguments).await })
    }

    fn test_command() -> Value {
        json!({"command": "cargo_test"})
    }

    /// Whether no process has the id: it ended and was reaped.
    fn gone(pid: Pid) -> bool {
        rustix::process::test_kill_process(pid) == Err(Errno::SRCH)
    }

    /// Waits for `condition`, which is checked rather than timed, within [`WAIT`].
    fn until(mut condition: impl FnMut() -> bool) {
        let deadline = Instant::now() + WAIT;
        while !condition() {
            assert!(Instant::now() < deadline, "not reached within {WAIT:?}");
            thread::sleep(Duration::from_millis(5));
        }
    }

    fn refused(refusal: impl Into<Failure>) -> Result<Value, ToolFailure> {
        Err(ToolFailure::from(refusal.into()))
    }

    #[test]
    fn only_the_exact_presets_are_accepted() {
        assert_eq!(
            Preset::CargoCheck.arguments(),
            ["check", "--all-targets", "--message-format=json"]
        );
        assert_eq!(Preset::CargoTest.arguments(), ["test", "--all-targets"]);
        assert!(Preset::ALL.iter().all(|preset| preset.program() == "cargo"));
        let setup = setup("presets", "touch ran");
        let validator = crate::openai_compatible::admission::compile_parameters(&parameters())
            .expect("an enforceable schema");
        for (name, preset) in [
            ("cargo_check", Preset::CargoCheck),
            ("cargo_test", Preset::CargoTest),
        ] {
            let arguments = json!({ "command": name });
            assert!(validator.is_valid(&arguments), "{arguments}");
            assert_eq!(parse(&arguments), Ok(preset));
        }
        let invalid = [
            json!({"command": "cargo_build"}),
            json!({"command": "cargo_install"}),
            json!({"command": "CARGO_TEST"}),
            json!({"command": "cargo test"}),
            json!({"command": "cargo test --all-targets"}),
            json!({"command": "cargo_test; touch ran"}),
            json!({"command": "cargo_test && touch ran"}),
            json!({"command": "$(touch ran)"}),
            json!({"command": "/usr/bin/touch"}),
            json!({"command": "sh"}),
            json!({"command": "RUSTFLAGS=x cargo_test"}),
            json!({"command": "cargo_test", "args": ["--release"]}),
            json!({"command": "cargo_test", "arguments": "--", "flags": "-q"}),
            json!({"command": "cargo_test", "cwd": "/"}),
            json!({"command": "cargo_test", "env": {"RUSTC_WRAPPER": "/usr/bin/touch"}}),
            json!({"command": ["cargo_test"]}),
            json!({"command": null}),
            json!({}),
            json!("cargo_test"),
            json!(["cargo", "test"]),
        ];
        for arguments in invalid {
            assert!(
                !validator.is_valid(&arguments),
                "the schema refuses {arguments}"
            );
            assert_eq!(
                run(setup.handler(WAIT), arguments.clone()),
                refused(Refusal::InvalidArguments),
                "the handler refuses {arguments} too"
            );
        }
        assert!(!setup.ran(), "nothing was started");
    }

    #[test]
    fn a_command_invalidates_indexes_and_refresh_observes_indirect_writes() {
        use crate::orchestration::workspace_fts::{FtsLimits, WorkspaceFts};
        let setup = setup(
            "command-index-dirty",
            "printf 'changedneedle\\n' > source.rs",
        );
        std::fs::write(setup.root.join("source.rs"), "oldneedle\n").expect("source");
        let workspace = Workspace::authorize(&setup.root, &setup.directory).expect("authorize");
        let index = WorkspaceFts::new(workspace, FtsLimits::default());
        index.build().expect("build");
        run(setup.handler(Duration::from_secs(2)), test_command()).expect("command");
        let stale = index.search("oldneedle", 10).expect("old snapshot");
        assert_eq!(stale["index"]["freshness"], "known_dirty");
        index.rebuild().expect("refresh");
        let updated = index.search("changedneedle", 10).expect("updated");
        assert_eq!(updated["results"].as_array().expect("results").len(), 1);
        assert_eq!(updated["index"]["external_modifications"], "unverified");
    }

    #[test]
    fn the_coding_tools_are_offered_only_together_and_only_when_asked() {
        let setup = setup("registration", "exit 0");
        let authorize = || Workspace::authorize(&setup.root, &setup.directory).expect("authorize");
        let names = |tools: &ToolExecution| {
            tools
                .offer
                .wires()
                .expect("offered")
                .into_iter()
                .map(|wire| wire.function.name)
                .collect::<Vec<_>>()
        };
        let coding = code_tools(authorize()).expect("coding tools");
        assert_eq!(
            names(&coding),
            [
                workspace_read::TOOL_NAME,
                workspace_write::TOOL_NAME,
                TOOL_NAME,
                workspace_discovery::LIST_TOOL_NAME,
                workspace_discovery::SEARCH_TOOL_NAME,
                crate::orchestration::workspace_code::TOOL_NAME,
                super::super::workspace_fts::TOOL_NAME,
            ]
        );
        assert_eq!(
            coding.offer.wires().expect("offered")[2]
                .function
                .parameters,
            Some(parameters())
        );
        assert_eq!(
            coding.limits,
            ToolLoopLimits {
                max_provider_requests: 12,
                max_tool_calls: 16,
                max_tool_result_bytes: 16 * 1024,
                max_tool_results_bytes: 64 * 1024,
                handler_timeout: COMMAND_TIMEOUT + STOP_GRACE,
            }
        );
        assert!(coding.limits.validate().is_ok());
        assert!(RESULT_BYTES < coding.limits.max_tool_result_bytes);
        assert_ne!(
            coding.limits,
            ToolLoopLimits::default(),
            "the defaults are not changed"
        );
        for other in [
            workspace_read::read_file_tools(authorize()).expect("read tools"),
            workspace_write::edit_file_tools(authorize()).expect("edit tools"),
        ] {
            assert!(!names(&other).iter().any(|name| name == TOOL_NAME));
            assert_eq!(other.limits, ToolLoopLimits::default());
        }

        let handler = setup.handler(COMMAND_TIMEOUT);
        assert_eq!(handler.effect(), ToolEffect::NonIdempotent);
        assert_eq!(handler.version(), "workspace_run_command-4");
        assert!(
            crate::persistence::ToolAdmission {
                handler_version: handler.version().to_owned(),
                effect: handler.effect(),
                parameters: parameters().to_string(),
                arguments: crate::persistence::ToolPayload::new(test_command().to_string()),
                authorization: crate::persistence::AuthorizationBasis::RegistryGrant,
            }
            .spawns()
        );
        assert!(crate::persistence::is_handler_version(handler.version()));
        let registry = ToolRegistry::default();
        register(&registry, authorize(), setup.environment(), COMMAND_TIMEOUT).expect("register");
        assert!(
            matches!(
                registry.offer(&[TOOL_NAME]),
                Err(ToolRegistryError::Unauthorized(_))
            ),
            "registering authorizes nothing"
        );
    }

    #[test]
    fn workspace_code_and_search_tools_coexist_with_default_fts() {
        use crate::openai_compatible::CompletedToolCall;
        use crate::orchestration::{workspace_code, workspace_fts};

        const CHILD: &str = "OCG_TEST_FTS_REGISTRATION_CHILD";
        if std::env::var_os(CHILD).is_none() {
            for value in [None, Some(""), Some("0"), Some("true"), Some("1")] {
                let mut command = Command::new(std::env::current_exe().expect("test executable"));
                command
                    .args([
                        "--exact",
                        "orchestration::workspace_command::tests::workspace_code_and_search_tools_coexist_with_default_fts",
                        "--nocapture",
                    ])
                    .env(CHILD, "1");
                match value {
                    Some(value) => command.env("OCG_EXPERIMENTAL_WORKSPACE_FTS", value),
                    None => command.env_remove("OCG_EXPERIMENTAL_WORKSPACE_FTS"),
                };
                let output = command.output().expect("registration subprocess");
                assert!(
                    output.status.success(),
                    "FTS setting {value:?}: {}{}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr),
                );
            }
            return;
        }

        let setup = setup("search-coexistence", "exit 0");
        std::fs::write(setup.root.join("source.rs"), "pub fn needle() {}\n").expect("source");
        let workspace = Workspace::authorize(&setup.root, &setup.directory).expect("authorize");
        let coding = code_tools(workspace).expect("coding tools");
        let names = coding
            .offer
            .wires()
            .expect("offered")
            .into_iter()
            .map(|wire| wire.function.name)
            .collect::<Vec<_>>();
        assert!(
            names
                .iter()
                .any(|name| name == workspace_discovery::SEARCH_TOOL_NAME)
        );
        assert!(names.iter().any(|name| name == workspace_code::TOOL_NAME));
        assert!(names.iter().any(|name| name == workspace_fts::TOOL_NAME));
        assert_eq!(names.len(), 7);
        for (name, arguments, version) in [
            (
                workspace_discovery::SEARCH_TOOL_NAME,
                json!({"text": "needle"}),
                "workspace_search-1",
            ),
            (
                workspace_code::TOOL_NAME,
                json!({"queries": [{"path": "source.rs", "op": "outline"}]}),
                "workspace_code-1",
            ),
            (
                workspace_code::TOOL_NAME,
                json!({"queries": [{"path": "source.rs", "op": "errors"}]}),
                "workspace_code-1",
            ),
            (
                workspace_fts::TOOL_NAME,
                json!({"query": "needle"}),
                "workspace_fts_search-2",
            ),
        ] {
            let call = CompletedToolCall {
                index: 0,
                id: "coexistence".to_owned(),
                name: name.to_owned(),
                arguments: arguments.to_string(),
            };
            let dispatch = coding.offer.resolve(&call);
            let dispatch = dispatch.expect("authorized dispatch");
            assert_eq!(dispatch.version, version);
            assert_eq!(dispatch.effect, ToolEffect::ReadOnly);
            let result = on_ntex(async move { dispatch.handler.call(dispatch.arguments).await })
                .expect("tool result");
            if name == workspace_code::TOOL_NAME {
                if arguments["queries"][0]["op"] == "outline" {
                    assert_eq!(result["counts"][0]["found"], 1);
                    assert!(result["text"].as_str().expect("text").contains("needle"));
                } else {
                    assert_eq!(result["counts"][0]["found"], 0);
                    assert_eq!(result["counts"][0]["files"], 1);
                }
            } else if name == workspace_discovery::SEARCH_TOOL_NAME {
                assert_eq!(result["files"][0]["path"], "source.rs");
            } else {
                assert!(!result["results"].as_array().expect("results").is_empty());
            }
        }
    }

    #[test]
    fn any_exit_is_a_completed_result_with_each_stream_apart() {
        let setup = setup(
            "exit",
            "echo $$ > leader.pid\npwd -P\necho \"arguments: $*\"\necho 'to stderr' >&2\nexit $(cat code)",
        );
        let root = setup.root.display().to_string();
        for (command, arguments, code) in [
            ("cargo_test", "test --all-targets", 0),
            ("cargo_test", "test --all-targets", 101),
            (
                "cargo_check",
                "check --all-targets --message-format=json",
                1,
            ),
        ] {
            std::fs::write(setup.root.join("code"), code.to_string()).expect("write");
            assert_eq!(
                run(setup.handler(WAIT), json!({ "command": command })),
                Ok(json!({
                    "command": command,
                    "exit_code": code,
                    "stdout": format!("{root}\narguments: {arguments}\n"),
                    "stderr": "to stderr\n",
                    "truncated": false,
                })),
                "a nonzero exit is a result, not a failure of the call"
            );
            assert!(gone(setup.pid("leader.pid")), "the process was reaped");
        }
    }

    #[test]
    fn check_output_keeps_diagnostics_in_error_order() {
        let diagnostic = |level, code: Value, message, spans: Value| {
            json!({
                "reason": "compiler-message",
                "message": {
                    "level": level,
                    "code": code,
                    "message": message,
                    "spans": spans,
                    "rendered": "a long rendered diagnostic",
                },
            })
        };
        let spans = json!([
            {"file_name": "other.rs", "line_start": 1, "column_start": 1, "is_primary": false},
            {"file_name": "src/main.rs", "line_start": 12, "column_start": 8, "is_primary": true},
        ]);
        let stdout = [
            json!({"reason": "compiler-artifact"}),
            diagnostic("warning", Value::Null, "unused variable", spans.clone()),
            diagnostic(
                "error",
                json!({"code": "E0308"}),
                "mismatched\ntypes",
                spans,
            ),
            diagnostic("warning", Value::Null, "no location", json!([])),
            diagnostic("note", Value::Null, "an explanation", json!([])),
            json!({"reason": "build-script-executed"}),
            json!({"reason": "build-finished", "success": false}),
        ]
        .map(|record| record.to_string())
        .join("\n");
        let result = captured_result(Preset::CargoCheck, &stdout, "Checking example\n", 101);
        assert_eq!(result["exit_code"], 101);
        assert_eq!(
            result["stdout"],
            "src/main.rs:12:8 error[E0308] mismatched types\nsrc/main.rs:12:8 warning unused variable\n<unknown>:0:0 warning no location"
        );
        assert_eq!(result["stderr"], "");
        assert_eq!(result["truncated"], false);
        assert_eq!(
            captured_result(
                Preset::CargoCheck,
                "{\"reason\":\"build-finished\",\"success\":true}\n",
                "Finished dev profile\n",
                0,
            )["stdout"],
            ""
        );
    }

    fn captured_result(preset: Preset, stdout: &str, stderr: &str, code: i32) -> Value {
        let mut ended = Ended {
            status: ExitStatus::from_raw(code << 8),
            stdout: Capture::default(),
            stderr: Capture::default(),
        };
        ended.stdout.push(stdout.as_bytes());
        ended.stderr.push(stderr.as_bytes());
        result(preset, &ended)
    }

    #[test]
    fn test_output_keeps_summaries_and_the_start_of_each_failure() {
        let passed = "test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s";
        let failed = "test result: FAILED. 1 passed; 2 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s";
        let stdout = format!(
            "running 1 test\ntest passing ... ok\n{passed}\n\n\
             running 3 tests\ntest passing_again ... ok\ntest first ... FAILED\ntest second ... FAILED\n\n\
             failures:\n\n---- first stdout ----\nnoise before the panic\n\n\
             thread 'first' (123) panicked at src/lib.rs:4:5:\nassertion failed\n  left: 1\n right: 2\nstack backtrace:\nlong backtrace\n\n\
             ---- second stdout ----\nthread 'second' (124) panicked at src/lib.rs:8:9:\nsomething broke\n\n\
             failures:\n    first\n    second\n\n{failed}\n"
        );
        let result = captured_result(Preset::CargoTest, &stdout, "error: test failed\n", 101);
        assert_eq!(
            result["stdout"],
            format!(
                "{passed}\n{failed}\nfirst\nthread 'first' (123) panicked at src/lib.rs:4:5:\nassertion failed\n  left: 1\n right: 2\nsecond\nthread 'second' (124) panicked at src/lib.rs:8:9:\nsomething broke"
            )
        );
        assert_eq!(result["exit_code"], 101);
        assert_eq!(result["stderr"], "");
        assert_eq!(
            captured_result(
                Preset::CargoTest,
                &format!("test passing ... ok\n{passed}\n"),
                "",
                0
            )["stdout"],
            passed
        );
    }

    #[test]
    fn unparseable_command_output_uses_the_raw_result() {
        for (preset, stdout) in [
            (Preset::CargoCheck, "{broken json}\n"),
            (Preset::CargoCheck, "{\"reason\":\"compiler-message\"}\n"),
            (Preset::CargoTest, "a custom test harness failed\n"),
            (
                Preset::CargoTest,
                "test result: ok. 1 passed; 0 failed\nsecond test binary aborted\n",
            ),
            (Preset::CargoTest, ""),
        ] {
            assert_eq!(
                captured_result(preset, stdout, "failure details\n", 101),
                json!({
                    "command": preset.name(),
                    "exit_code": 101,
                    "stdout": stdout,
                    "stderr": "failure details\n",
                    "truncated": false,
                })
            );
        }
    }

    #[test]
    fn output_is_bounded_explicitly_and_never_blocks_the_command() {
        let setup = setup(
            "output",
            "if [ -e small ]; then echo small; head -c 100000 /dev/zero | tr '\\0' b >&2; exit 0; fi\n\
             if [ -e binary ]; then head -c 100000 /dev/zero; exit 0; fi\n\
             head -c 1000000 /dev/zero | tr '\\0' a\n\
             head -c 1000000 /dev/zero | tr '\\0' b >&2\n\
             echo end",
        );
        let result = run(setup.handler(WAIT), test_command()).expect("completed");
        let half = OUTPUT_BYTES / 2;
        assert_eq!(
            result,
            json!({
                "command": "cargo_test",
                "exit_code": 0,
                "stdout": format!(
                    "{}\n[OCG omitted {} bytes of stdout here]\n{}end\n",
                    "a".repeat(half / 2),
                    1_000_004 - half,
                    "a".repeat(half / 2 - 4),
                ),
                "stderr": format!(
                    "{}\n[OCG omitted {} bytes of stderr here]\n{}",
                    "b".repeat(half / 2),
                    1_000_000 - half,
                    "b".repeat(half / 2),
                ),
                "truncated": true,
            })
        );

        // What one stream leaves of the budget goes to the other.
        std::fs::write(setup.root.join("small"), "").expect("write");
        let result = run(setup.handler(WAIT), test_command()).expect("completed");
        let rest = OUTPUT_BYTES - "small\n".len();
        assert_eq!(result["stdout"], "small\n");
        assert_eq!(
            result["stderr"],
            format!(
                "{}\n[OCG omitted {} bytes of stderr here]\n{}",
                "b".repeat(rest / 2),
                100_000 - rest,
                "b".repeat(rest - rest / 2),
            )
        );
        assert_eq!(result["truncated"], true);

        // Output that escapes to far more JSON still fits a result.
        std::fs::remove_file(setup.root.join("small")).expect("remove");
        std::fs::write(setup.root.join("binary"), "").expect("write");
        let result = run(setup.handler(WAIT), test_command()).expect("completed");
        assert!(result.to_string().len() <= RESULT_BYTES);
        assert_eq!(result["truncated"], true);
        assert!(
            result["stdout"]
                .as_str()
                .is_some_and(|stdout| stdout.contains("bytes of stdout here"))
        );
    }

    #[test]
    fn a_program_that_is_not_found_is_refused_before_anything_starts() {
        let setup = setup("missing", "touch ran");
        let (_empty_guard, empty) = temporary_directory(SUITE, "missing-empty");
        let not_found = || refused(Refusal::ProgramNotFound);
        let empty_path = environment_with_path(empty.display().to_string());
        assert_eq!(
            run(setup.handler_with(empty_path, WAIT), test_command()),
            not_found()
        );
        // Relative entries are never searched, wherever they would lead.
        install(&setup.root, "touch ran", 0o755);
        let relative = environment_with_path(".:bin:./".to_owned());
        assert!(relative.search.is_empty());
        assert!(!relative.variables.iter().any(|(name, _)| name == "PATH"));
        assert_eq!(
            run(setup.handler_with(relative, WAIT), test_command()),
            not_found()
        );
        let unset = CommandEnvironment::from_source(|_| None);
        assert_eq!(
            run(setup.handler_with(unset, WAIT), test_command()),
            not_found()
        );
        // A file that is not executable is no program.
        install(&setup.bin, "touch ran", 0o644);
        assert_eq!(run(setup.handler(WAIT), test_command()), not_found());
        assert!(!setup.ran(), "nothing was started");
    }

    #[test]
    fn a_replaced_workspace_root_starts_nothing() {
        let setup = setup("replaced", "touch ran");
        let handler = setup.handler(WAIT);
        let (_moved_guard, moved) = temporary_directory(SUITE, "replaced-original");
        std::fs::remove_dir(&moved).expect("free the name");
        std::fs::rename(&setup.root, &moved).expect("move the Workspace away");
        std::fs::create_dir(&setup.root).expect("create another directory");
        assert_eq!(
            run(handler, test_command()),
            refused(Refusal::Path(PathRefusal::WorkspaceChanged))
        );
        assert!(!setup.ran() && !moved.join("ran").exists());
    }

    #[test]
    fn the_command_gets_only_the_environment_built_for_it() {
        let setup = setup("environment", "exec /usr/bin/env");
        let bin = setup.bin.display().to_string();
        let source = BTreeMap::from([
            ("PATH", format!("{bin}:.:relative/bin:/usr/bin:/bin")),
            ("HOME", "/home/operator".to_owned()),
            ("TMPDIR", "/tmp/operator".to_owned()),
            ("COMMAND_CODE_API_KEY", "sk-secret-command-code".to_owned()),
            (
                "OCG_PROVIDER_CREDENTIAL_REF",
                "COMMAND_CODE_API_KEY".to_owned(),
            ),
            ("OPENAI_API_KEY", "sk-secret-openai".to_owned()),
            ("OCG_DATA_DIR", "/private/ocg".to_owned()),
            ("RUSTC_WRAPPER", "/usr/bin/touch".to_owned()),
            ("CARGO_TARGET_DIR", "/elsewhere".to_owned()),
            ("CARGO_TERM_COLOR", "always".to_owned()),
        ]);
        let environment =
            CommandEnvironment::from_source(|name| source.get(name).map(OsString::from));
        let result = run(setup.handler_with(environment, WAIT), test_command()).expect("completed");
        let stdout = result["stdout"].as_str().expect("text");
        let seen = stdout
            .lines()
            .filter_map(|line| line.split_once('='))
            .collect::<BTreeMap<_, _>>();
        // A shell may add its own few variables before it runs `env`.
        let shell = ["PWD", "OLDPWD", "SHLVL", "_"];
        let built = seen
            .iter()
            .filter(|(name, _)| !shell.contains(name))
            .map(|(name, value)| (*name, *value))
            .collect::<BTreeMap<_, _>>();
        let path = format!("{bin}:/usr/bin:/bin");
        assert_eq!(
            built,
            BTreeMap::from([
                ("PATH", path.as_str()),
                ("HOME", "/home/operator"),
                ("TMPDIR", "/tmp/operator"),
                ("CARGO_TERM_COLOR", "never"),
            ])
        );
        assert!(!stdout.contains("sk-secret") && !stdout.contains("COMMAND_CODE_API_KEY"));

        // OCG's own environment is filtered the same way.
        let process = CommandEnvironment::of_process();
        for (name, _) in &process.variables {
            let name = name.to_str().expect("a known name");
            assert!(
                name == "PATH"
                    || PASSED_VARIABLES.contains(&name)
                    || SET_VARIABLES.iter().any(|(set, _)| *set == name),
                "{name}"
            );
        }
    }

    #[test]
    fn a_command_past_its_limit_is_stopped_with_its_process_group() {
        let setup = setup(
            "timeout",
            "echo $$ > leader.pid\nsleep 60 &\necho $! > child.pid\nwait",
        );
        let limit = Duration::from_secs(1);
        let started = Instant::now();
        assert_eq!(
            run(setup.handler(limit), test_command()),
            Err(ToolFailure::uncertain(
                "the command did not finish within 1s and was stopped"
            ))
        );
        assert!(started.elapsed() < limit + STOP_GRACE);
        assert!(
            gone(setup.pid("leader.pid")),
            "the process is reaped before the call returns"
        );
        let child = setup.pid("child.pid");
        until(|| gone(child));
    }

    #[test]
    fn a_dropped_call_stops_its_command() {
        let setup = setup(
            "dropped",
            "echo $$ > leader.pid\nsleep 60 &\necho $! > child.pid\ntouch ready\nwait",
        );
        let handler = setup.handler(WAIT);
        let ready = setup.root.join("ready");
        on_ntex(async move {
            let started = async {
                while !ready.exists() {
                    ntex::time::sleep(Duration::from_millis(5)).await;
                }
            };
            match select(handler.call(test_command()), started).await {
                Either::Left(ended) => panic!("the command cannot end by itself: {ended:?}"),
                // The call is dropped here, as a cancellation drops it.
                Either::Right(()) => {}
            }
        });
        let (leader, child) = (setup.pid("leader.pid"), setup.pid("child.pid"));
        until(|| gone(leader) && gone(child));
    }

    #[test]
    fn the_executor_runs_on_while_a_command_runs() {
        let setup = setup(
            "concurrent",
            "while [ ! -e go ]; do sleep 0.01; done\necho went",
        );
        let handler = setup.handler(WAIT);
        let go = setup.root.join("go");
        let result = on_ntex(async move {
            // Runs on the same thread as the call, so it runs only if the
            // call yields while its command runs; the command waits for it.
            ntex::rt::spawn(async move {
                ntex::time::sleep(Duration::from_millis(50)).await;
                std::fs::write(go, "").expect("write");
            });
            handler.call(test_command()).await
        });
        assert_eq!(result.expect("completed")["stdout"], "went\n");
    }

    #[test]
    fn a_process_left_in_the_group_does_not_outlive_the_command() {
        let setup = setup(
            "left-behind",
            "sleep 60 > /dev/null 2>&1 &\necho $! > child.pid\necho done",
        );
        let result = run(setup.handler(WAIT), test_command()).expect("completed");
        assert_eq!(result["stdout"], "done\n");
        let child = setup.pid("child.pid");
        until(|| gone(child));
    }
}
