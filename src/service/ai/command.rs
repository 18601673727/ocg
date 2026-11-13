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
use crate::system::executor::blocking::{
    self, FinishError, Finished, Kind, Process, StartingCall, StopSignal,
};
use crate::system::executor::tool_loop::ToolLoopLimits;
use crate::system::executor::tools::{
    ToolFailure, ToolFuture, ToolHandler, ToolRegistry, ToolRegistryError, ToolStart,
};
use crate::system::executor::workspace_read::{self, Refusal as PathRefusal, Workspace};

pub const TOOL_NAME: &str = "workspace_run_command";

const HANDLER_VERSION: &str = "workspace_run_command-7";

const MAX_EXTRA_ARGUMENTS: usize = 64;
const MAX_EXTRA_ARGUMENT_BYTES: usize = 4096;

pub(crate) const COMMAND_TIMEOUT: Duration = Duration::from_secs(120);

pub const OUTPUT_BYTES: usize = 12 * 1024;

pub const RESULT_BYTES: usize = 15 * 1024;

pub const PASSED_VARIABLES: &[&str] = &[
    "HOME",
    "TMPDIR",
    "CARGO_HOME",
    "RUSTUP_HOME",
    "RUSTUP_TOOLCHAIN",
    "SSH_AUTH_SOCK",
];

pub const SET_VARIABLES: &[(&str, &str)] = &[("CARGO_TERM_COLOR", "never")];

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

#[derive(Clone, Debug)]
pub(crate) struct CommandEnvironment {
    search: Vec<PathBuf>,
    variables: Vec<(OsString, OsString)>,
}

impl CommandEnvironment {
    pub(crate) fn of_process() -> Self {
        Self::from_source(|name| std::env::var_os(name))
    }

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

    fn find(&self, program: &str) -> Option<PathBuf> {
        self.search
            .iter()
            .map(|directory| directory.join(program))
            .find(|candidate| executable(candidate))
    }

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

#[derive(Default)]
struct Capture {
    head: Vec<u8>,
    tail: VecDeque<u8>,
    total: usize,
    unread: bool,
}

impl Capture {
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

struct Ended {
    finished: Finished,
    stdout: Capture,
    stderr: Capture,
}

fn result(selected: &Selected, argv: &[String], ended: &Ended) -> Value {
    let (stdout, stderr) = (&ended.stdout, &ended.stderr);
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

fn supervise(
    invocation: Invocation,
    stop: StopSignal,
    start: ToolStart,
    starting: Option<StartingCall>,
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
        Process::spawn(command, Kind::Command, stop.clone(), starting).map_err(|error| {
            if error.kind() == std::io::ErrorKind::TimedOut {
                Refusal::CommandBusy
            } else {
                Refusal::NotStarted
            }
        })?;
    start.mark_started();
    let (stdout_pipe, stderr_pipe) = process.take_output();
    let readers = match (stdout_pipe, stderr_pipe) {
        (Some(stdout), Some(stderr)) => match reader(stdout) {
            Some(stdout) => reader(stderr).map(|stderr| (stdout, stderr)),
            None => None,
        },
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

struct RunCommand {
    workspace: Workspace,
    commands: Vec<ProjectCommand>,
    environment: CommandEnvironment,
    timeout: Duration,
}

impl RunCommand {
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

    fn effect(&self) -> ToolEffect {
        ToolEffect::NonIdempotent
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

    fn spawns(&self) -> bool {
        true
    }

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
            let starting = match call {
                CommandCall::Job(call_ref) => Some(
                    start
                        .take_starting()
                        .unwrap_or_else(|| blocking::begin_call(call_ref)),
                ),
                CommandCall::Standalone(_) => None,
            };
            thread::Builder::new()
                .name("ocg-command".to_owned())
                .spawn(move || {
                    let _mutation = mutation;
                    let result = supervise(invocation, supervised, process_start, starting);
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

pub(crate) fn coding_limits(timeout: Duration) -> ToolLoopLimits {
    ToolLoopLimits {
        handler_timeout: timeout.saturating_add(blocking::STOP_GRACE),
        ..ToolLoopLimits::default()
    }
}
