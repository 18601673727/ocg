//! `workspace_run_command`: runs one of the Project's commands in a
//! Workspace the operator authorized, and returns how it exited and what it
//! printed.
//!
//! A call names a command, never a command line. The built-in `cargo_check`
//! runs `cargo check --all-targets` and `cargo_test` runs
//! `cargo test --all-targets`; the Project adds commands of its own by name,
//! each with the base argument vector its owner stored. A call may append
//! literal strings through its optional `arguments` array. It cannot replace
//! the program, base arguments, directory or environment. The process is
//! started directly, never through a shell. A bare program name is found on
//! the PATH OCG itself was started with, among absolute entries only, so never
//! in a path a call names or the Workspace provides by a relative entry; an
//! absolute program path is used
//! when it is an executable regular file. A program that is not found is
//! refused before anything starts.
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
//! Resource usage: `wall_ms`, `cpu_user_ms`, `cpu_system_ms`, and
//! `peak_rss_bytes` describe the leader and the descendants it waited for
//! (cargo waits for rustc). Nothing else stores them: the result is the
//! evidence, once.
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
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use serde_json::{Value, json};

use crate::db::entities::{BUILT_IN_COMMANDS, JobId, ProjectCommand, ToolCallRef, ToolEffect};
use crate::system::executor::blocking::{FinishError, Finished, Kind, Process, StopSignal};
use crate::system::executor::execution_runtime::{ToolExecution, ToolLoopLimits};
use crate::system::executor::tools::{
    ToolFailure, ToolFuture, ToolHandler, ToolRegistry, ToolRegistryError, ToolStart,
};
use crate::system::executor::workspace_read::{self, Refusal as PathRefusal, Workspace};
use crate::system::executor::{workspace_discovery, workspace_write};

/// The name the tool is offered and recorded under.
pub const TOOL_NAME: &str = "workspace_run_command";

/// Identifies this handler's behavior in every call it records.
const HANDLER_VERSION: &str = "workspace_run_command-7";

const MAX_EXTRA_ARGUMENTS: usize = 64;
const MAX_EXTRA_ARGUMENT_BYTES: usize = 4096;

/// Longest a command may run before it is stopped.
pub use crate::system::executor::job_tools::COMMAND_TIMEOUT;

/// The most output, of stdout and stderr together, a result carries.
pub const OUTPUT_BYTES: usize = 12 * 1024;

/// The largest serialized result: within the coding loop's limit of a single
/// result, whatever the output's JSON escaping costs.
pub const RESULT_BYTES: usize = 15 * 1024;

/// The variables taken from OCG's own environment, when set: what locates
/// the toolchain, its temporary files and the SSH agent. PATH is taken too,
/// reduced to its absolute entries.
pub const PASSED_VARIABLES: &[&str] = &[
    "HOME",
    "TMPDIR",
    "CARGO_HOME",
    "RUSTUP_HOME",
    "RUSTUP_TOOLCHAIN",
    "SSH_AUTH_SOCK",
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
            Self::CargoCheck => BUILT_IN_COMMANDS[0],
            Self::CargoTest => BUILT_IN_COMMANDS[1],
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
    #[error(
        "the arguments must name a supported command and an optional array of literal arguments"
    )]
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

/// The command a call names.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Selected {
    Preset(Preset),
    Project(ProjectCommand),
}

impl Selected {
    fn name(&self) -> &str {
        match self {
            Self::Preset(preset) => preset.name(),
            Self::Project(command) => &command.name,
        }
    }

    fn argv(&self) -> Vec<String> {
        match self {
            Self::Preset(preset) => std::iter::once(preset.program())
                .chain(preset.arguments().iter().copied())
                .map(str::to_owned)
                .collect(),
            Self::Project(command) => command.argv.clone(),
        }
    }
}

pub(super) struct RequestCommand;
impl ToolHandler for RequestCommand {
    fn version(&self) -> &str {
        "workspace_request_command-1"
    }
    fn effect(&self) -> ToolEffect {
        ToolEffect::ReadOnly
    }
    fn call(&self, arguments: Value) -> ToolFuture<'_> {
        Box::pin(async move {
            Ok(
                json!({"granted": arguments.get("name"), "next": "Use workspace_run_command with this command name. The request itself does not execute it."}),
            )
        })
    }
}

/// The command a call names, after checking its optional extra arguments.
fn parse(arguments: &Value, commands: &[ProjectCommand]) -> Result<Selected, Refusal> {
    let object = arguments.as_object().ok_or(Refusal::InvalidArguments)?;
    if object
        .keys()
        .any(|key| !matches!(key.as_str(), "command" | "arguments"))
    {
        return Err(Refusal::InvalidArguments);
    }
    if let Some(extra) = object.get("arguments") {
        let extra = extra.as_array().ok_or(Refusal::InvalidArguments)?;
        if extra.len() > MAX_EXTRA_ARGUMENTS
            || extra.iter().any(|argument| {
                argument.as_str().is_none_or(|argument| {
                    argument.len() > MAX_EXTRA_ARGUMENT_BYTES || argument.contains('\0')
                })
            })
        {
            return Err(Refusal::InvalidArguments);
        }
    }
    let name = object
        .get("command")
        .and_then(Value::as_str)
        .ok_or(Refusal::InvalidArguments)?;
    if let Some(preset) = Preset::of(name) {
        return Ok(Selected::Preset(preset));
    }
    commands
        .iter()
        .find(|command| {
            command.name == name
                && command.status == crate::db::entities::CommandStatus::Allowed
                && (command.owner == crate::db::entities::CommandOwner::User
                    || object
                        .get("arguments")
                        .and_then(Value::as_array)
                        .is_none_or(Vec::is_empty))
        })
        .cloned()
        .map(Selected::Project)
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
            .find(|candidate| executable(candidate))
    }

    /// The program a command names: a bare name found on the search path,
    /// or an absolute path that is an executable regular file.
    fn locate(&self, program: &str) -> Option<PathBuf> {
        if !program.contains('/') {
            return self.find(program);
        }
        let path = PathBuf::from(program);
        (path.is_absolute() && executable(&path)).then_some(path)
    }
}

fn executable(path: &std::path::Path) -> bool {
    std::fs::metadata(path)
        .is_ok_and(|metadata| metadata.is_file() && metadata.permissions().mode() & 0o111 != 0)
}

/// Everything needed to start one command, prepared by trusted code.
struct Invocation {
    program: PathBuf,
    arguments: Vec<String>,
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
    finished: Finished,
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
    ended.finished.status.code()?;
    let stdout = ended.stdout.text()?;
    ended.stderr.text()?;
    let text = match preset {
        Preset::CargoCheck => compact_check(&stdout)?,
        Preset::CargoTest => {
            if !ended.finished.status.success()
                && !stdout
                    .lines()
                    .any(|line| line.starts_with("test result: FAILED."))
            {
                return None;
            }
            compact_test(&stdout)?
        }
    };
    if !ended.finished.status.success() && text.is_empty() {
        return None;
    }
    let mut capture = Capture::default();
    capture.push(text.as_bytes());
    Some(capture)
}

/// The call's result: the command, its exit, and its output within the
/// bounds. A process ended by a signal has no exit code, and its signal is
/// given instead.
fn result(selected: &Selected, argv: &[String], ended: &Ended) -> Value {
    let compact = match selected {
        Selected::Preset(preset) => compact(*preset, ended),
        Selected::Project(_) => None,
    };
    let empty = Capture::default();
    let (stdout, stderr) = match &compact {
        Some(stdout) => (stdout, &empty),
        None => (&ended.stdout, &ended.stderr),
    };
    let (cpu_user_ms, cpu_system_ms, peak_rss_bytes) = match &ended.finished.usage {
        Some(usage) => (
            json!(usage.user.as_millis() as u64),
            json!(usage.system.as_millis() as u64),
            json!(usage.peak_rss_bytes),
        ),
        None => (Value::Null, Value::Null, Value::Null),
    };
    let mut budget = OUTPUT_BYTES;
    loop {
        let (stdout_share, stderr_share) = shares(stdout.total, stderr.total, budget);
        let (stdout, stdout_cut) = stdout.render("stdout", stdout_share);
        let (stderr, stderr_cut) = stderr.render("stderr", stderr_share);
        let mut value = json!({
            "command": selected.name(),
            "argv": argv,
            "exit_code": ended.finished.status.code(),
            "stdout": stdout,
            "stderr": stderr,
            "truncated": stdout_cut || stderr_cut,
            "wall_ms": ended.finished.wall.as_millis() as u64,
            "cpu_user_ms": cpu_user_ms,
            "cpu_system_ms": cpu_system_ms,
            "peak_rss_bytes": peak_rss_bytes,
        });
        if let Some(signal) = ended.finished.status.signal() {
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
        .args(&invocation.arguments)
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
    let mut process =
        Process::spawn(command, Kind::Command, stop.clone(), job).map_err(|error| {
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
    let finished = match process.finish(|| stop.requested()) {
        Ok(finished) => finished,
        Err(FinishError::NotProven(_)) => return Err(Uncertainty::NotObserved.into()),
        Err(FinishError::Wait(_)) => return Err(Uncertainty::ExitNotObserved.into()),
    };
    match (stdout.join(), stderr.join()) {
        (Ok(stdout), Ok(stderr)) => Ok(Ended {
            finished,
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
    commands: Vec<ProjectCommand>,
    environment: CommandEnvironment,
    timeout: Duration,
}

impl RunCommand {
    /// The command `selected` names, ready to start: the Workspace root must
    /// still be the directory authorized, and the program must be found.
    fn prepare(&self, selected: &Selected) -> Result<Invocation, Refusal> {
        self.workspace.open_root()?;
        let mut argv = selected.argv().into_iter();
        let program = argv
            .next()
            .and_then(|program| self.environment.locate(&program))
            .ok_or(Refusal::ProgramNotFound)?;
        Ok(Invocation {
            program,
            arguments: argv.collect(),
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
            let selected = parse(&arguments, &self.commands).map_err(Failure::from)?;
            let mut invocation = self.prepare(&selected).map_err(Failure::from)?;
            invocation.arguments.extend(
                arguments
                    .get("arguments")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_str)
                    .map(str::to_owned),
            );
            let argv = std::iter::once(invocation.program.to_string_lossy().into_owned())
                .chain(invocation.arguments.iter().cloned())
                .collect::<Vec<_>>();
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
            Ok(result(&selected, &argv, &ended))
        })
    }
}

/// One command name and optional extra arguments. The names list the built-in
/// presets first, then the Project's commands.
fn parameters(commands: &[ProjectCommand]) -> Value {
    let names = Preset::ALL
        .map(Preset::name)
        .into_iter()
        .chain(
            commands
                .iter()
                .filter(|command| command.status == crate::db::entities::CommandStatus::Allowed)
                .map(|command| command.name.as_str()),
        )
        .collect::<Vec<_>>();
    let description = Preset::ALL
        .into_iter()
        .map(Selected::Preset)
        .chain(
            commands
                .iter()
                .filter(|command| command.status == crate::db::entities::CommandStatus::Allowed)
                .cloned()
                .map(Selected::Project),
        )
        .map(|selected| {
            let provenance = match &selected {
                Selected::Project(command)
                    if command.owner == crate::db::entities::CommandOwner::Decision =>
                {
                    format!(
                        "; decision managed, no extra arguments; proposal {}",
                        command
                            .proposal
                            .map(|job| job.to_string())
                            .unwrap_or_default()
                    )
                }
                Selected::Preset(_) | Selected::Project(_) => String::new(),
            };
            format!(
                "{} runs `{}`{}",
                selected.name(),
                selected.argv().join(" "),
                provenance
            )
        })
        .collect::<Vec<_>>()
        .join("; ");
    json!({
        "type": "object",
        "properties": {
            "command": {
                "type": "string",
                "enum": names,
                "description": description
            },
            "arguments": {
                "type": "array",
                "items": { "type": "string", "maxLength": MAX_EXTRA_ARGUMENT_BYTES },
                "maxItems": MAX_EXTRA_ARGUMENTS,
                "description": "Extra argv entries appended after the command's configured arguments. Each item is one literal argument, including any spaces; shell syntax is not interpreted."
            }
        },
        "required": ["command"],
        "additionalProperties": false
    })
}

/// Registers `workspace_run_command` for `workspace` in `registry`, without
/// authorizing it. It offers the built-in presets and `commands`; they get
/// `environment` and run for at most `timeout`.
pub(crate) fn register(
    registry: &ToolRegistry,
    workspace: Workspace,
    commands: &[ProjectCommand],
    environment: CommandEnvironment,
    timeout: Duration,
) -> Result<(), ToolRegistryError> {
    registry.register(
        TOOL_NAME,
        Some(
            "Run one of the Project's commands in the authorized Workspace root, optionally appending literal entries from `arguments`, and return its argv, exit code, stdout, stderr, wall_ms, cpu_user_ms, cpu_system_ms and peak_rss_bytes. A nonzero exit code, such as a failing build or test, is a normal result to read, not a failure of this tool. Output is limited to about 12 KiB; `truncated` says whether part of it was omitted. The command's executable, configured arguments, directory and environment cannot be replaced.",
        ),
        parameters(commands),
        Arc::new(RunCommand {
            workspace,
            commands: commands.to_vec(),
            environment,
            timeout,
        }),
    )
}

/// The tool loop limits of a coding execution: several rounds of reading,
/// editing and testing, with every call given as long as a command may run
/// and a little more, so a command is stopped by its own limit first.
pub(crate) fn coding_limits(timeout: Duration) -> ToolLoopLimits {
    crate::system::executor::job_tools::coding_limits(timeout)
}

/// The grant authorized on a Workspace.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Grant {
    Read,
    Edit,
    Code,
}

impl Grant {
    pub fn parse(grant: &str) -> Option<Self> {
        match grant {
            "read" => Some(Self::Read),
            "edit" => Some(Self::Edit),
            "code" => Some(Self::Code),
            _ => None,
        }
    }
}

/// Builds the tools for `workspace` under `grant` with [`coding_limits(COMMAND_TIMEOUT)`].
pub fn workspace_tools(
    workspace: Workspace,
    grant: Grant,
    commands: &[ProjectCommand],
) -> Result<ToolExecution, ToolRegistryError> {
    workspace_tools_with(
        workspace,
        grant,
        commands,
        CommandEnvironment::of_process(),
        COMMAND_TIMEOUT,
    )
}

pub(crate) fn workspace_tools_managed(
    workspace: Workspace,
    grant: Grant,
    commands: &[ProjectCommand],
    managed: bool,
) -> Result<ToolExecution, ToolRegistryError> {
    workspace_tools_configured(
        workspace,
        grant,
        commands,
        CommandEnvironment::of_process(),
        COMMAND_TIMEOUT,
        managed,
    )
}

/// [`workspace_tools`] with the given command environment and limit.
pub(crate) fn workspace_tools_with(
    workspace: Workspace,
    grant: Grant,
    commands: &[ProjectCommand],
    environment: CommandEnvironment,
    timeout: Duration,
) -> Result<ToolExecution, ToolRegistryError> {
    workspace_tools_configured(workspace, grant, commands, environment, timeout, false)
}

fn workspace_tools_configured(
    workspace: Workspace,
    grant: Grant,
    commands: &[ProjectCommand],
    environment: CommandEnvironment,
    timeout: Duration,
    managed: bool,
) -> Result<ToolExecution, ToolRegistryError> {
    let registry = ToolRegistry::default();
    let limits = coding_limits(timeout);

    workspace_read::register(&registry, workspace.clone())?;
    workspace_discovery::register(&registry, workspace.clone())?;
    crate::service::fts::register(&registry, workspace.clone())?;
    #[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
    crate::service::lsp::register(&registry, workspace.clone(), timeout)?;

    if matches!(grant, Grant::Edit | Grant::Code) {
        workspace_write::register(&registry, workspace.clone())?;
    }
    if matches!(grant, Grant::Code) {
        register(&registry, workspace.clone(), commands, environment, timeout)?;
    }

    let mut names = vec![
        workspace_read::TOOL_NAME,
        workspace_discovery::LIST_TOOL_NAME,
        workspace_discovery::SEARCH_TOOL_NAME,
        crate::service::fts::TOOL_NAME,
    ];
    #[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
    names.push(crate::service::lsp::TOOL_NAME);

    if matches!(grant, Grant::Edit | Grant::Code) {
        names.push(workspace_write::TOOL_NAME);
    }
    if matches!(grant, Grant::Code) {
        names.push(TOOL_NAME);
        if managed {
            registry.register("workspace_request_command", Some("Request a new named command grant before running it. Supply the exact literal argv and a task-specific reason. The assigned Decision Profile assesses it; low or missing confidence waits for user approval. A grant adds the command to the Project, but does not run it. Then call workspace_run_command by name."), json!({"type":"object","additionalProperties":false,"properties":{"name":{"type":"string","minLength":1,"maxLength":64,"pattern":"^[a-z][a-z0-9_]{0,63}$"},"argv":{"type":"array","minItems":1,"maxItems":64,"items":{"type":"string","minLength":1,"maxLength":4096}},"reason":{"type":"string","minLength":1,"maxLength":4096}},"required":["name","argv","reason"]}), Arc::new(RequestCommand))?;
            names.push("workspace_request_command");
        }
    }

    for name in &names {
        registry.authorize(name)?;
    }
    Ok(ToolExecution {
        decider: false,
        offer: registry.offer(&names)?,
        limits,
    })
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
pub fn code_tools(
    workspace: Workspace,
    commands: &[ProjectCommand],
) -> Result<ToolExecution, ToolRegistryError> {
    code_tools_with(
        workspace,
        commands,
        CommandEnvironment::of_process(),
        COMMAND_TIMEOUT,
    )
}

/// [`code_tools`] with the given command environment and limit.
pub(crate) fn code_tools_with(
    workspace: Workspace,
    commands: &[ProjectCommand],
    environment: CommandEnvironment,
    timeout: Duration,
) -> Result<ToolExecution, ToolRegistryError> {
    workspace_tools_with(workspace, Grant::Code, commands, environment, timeout)
}
