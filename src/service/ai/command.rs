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
    preset: Option<Preset>,
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
    compacted: Option<(String, bool)>,
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

enum StreamCompactor {
    CargoCheck(CargoCheckCompactor),
    CargoTest(CargoTestCompactor),
}

impl StreamCompactor {
    fn for_preset(preset: Option<Preset>) -> Option<Self> {
        match preset? {
            Preset::CargoCheck => Some(Self::CargoCheck(CargoCheckCompactor::default())),
            Preset::CargoTest => Some(Self::CargoTest(CargoTestCompactor::default())),
        }
    }

    fn push(&mut self, bytes: &[u8]) {
        match self {
            Self::CargoCheck(compactor) => compactor.push(bytes),
            Self::CargoTest(compactor) => compactor.push(bytes),
        }
    }

    fn finish(self) -> Option<(String, bool)> {
        match self {
            Self::CargoCheck(compactor) => compactor.finish(),
            Self::CargoTest(compactor) => compactor.finish(),
        }
    }
}

const MAX_COMPACT_LINE_BYTES: usize = 64 * 1024;
const MAX_CHECK_ERRORS: usize = 64;
const MAX_CHECK_WARNINGS: usize = 32;
const MAX_DIAGNOSTIC_LINE_BYTES: usize = 512;
const MAX_TEST_RESULTS: usize = 32;
const MAX_FAILED_TESTS: usize = 32;
const MAX_TEST_DETAIL_LINES: usize = 16;

fn truncate_str(text: &str, max_bytes: usize) -> (String, bool) {
    if text.len() <= max_bytes {
        return (text.to_owned(), false);
    }
    let mut end = max_bytes.saturating_sub(1);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    (format!("{}…", &text[..end]), true)
}

#[derive(Default)]
struct LineScanner {
    line: Vec<u8>,
    overflowed: bool,
    any_overflowed: bool,
    invalid_utf8: bool,
}

impl LineScanner {
    fn feed(&mut self, bytes: &[u8], mut on_line: impl FnMut(&str)) {
        for &byte in bytes {
            if byte == b'\n' {
                if !self.overflowed {
                    match std::str::from_utf8(&self.line) {
                        Ok(line) => on_line(line.trim_end_matches('\r')),
                        Err(_) => self.invalid_utf8 = true,
                    }
                }
                self.line.clear();
                self.overflowed = false;
            } else if !self.overflowed {
                if self.line.len() < MAX_COMPACT_LINE_BYTES {
                    self.line.push(byte);
                } else {
                    self.overflowed = true;
                    self.any_overflowed = true;
                    self.line.clear();
                }
            }
        }
    }

    fn finish(&mut self, mut on_line: impl FnMut(&str)) {
        if !self.overflowed && !self.line.is_empty() {
            match std::str::from_utf8(&self.line) {
                Ok(line) => on_line(line.trim_end_matches('\r')),
                Err(_) => self.invalid_utf8 = true,
            }
        }
        self.line.clear();
        self.overflowed = false;
    }

    fn unreliable(&self) -> bool {
        self.any_overflowed || self.invalid_utf8
    }
}

#[derive(Default)]
struct CargoCheckCompactor {
    scanner: LineScanner,
    errors: Vec<String>,
    warnings: Vec<String>,
    dropped: usize,
    truncated_line: bool,
    success: Option<bool>,
    invalid: bool,
}

impl CargoCheckCompactor {
    fn push(&mut self, bytes: &[u8]) {
        if self.invalid || self.scanner.unreliable() {
            self.invalid = true;
            return;
        }
        let invalid = &mut self.invalid;
        let errors = &mut self.errors;
        let warnings = &mut self.warnings;
        let dropped = &mut self.dropped;
        let truncated_line = &mut self.truncated_line;
        let success = &mut self.success;
        self.scanner.feed(bytes, |line| {
            if *invalid || line.trim().is_empty() {
                return;
            }
            if !parse_check_line(line, errors, warnings, dropped, truncated_line, success) {
                *invalid = true;
            }
        });
    }

    fn finish(mut self) -> Option<(String, bool)> {
        if !self.invalid && !self.scanner.unreliable() {
            let invalid = &mut self.invalid;
            let errors = &mut self.errors;
            let warnings = &mut self.warnings;
            let dropped = &mut self.dropped;
            let truncated_line = &mut self.truncated_line;
            let success = &mut self.success;
            self.scanner.finish(|line| {
                if *invalid || line.trim().is_empty() {
                    return;
                }
                if !parse_check_line(line, errors, warnings, dropped, truncated_line, success) {
                    *invalid = true;
                }
            });
        }
        if self.invalid || self.scanner.unreliable() {
            return None;
        }
        if !self.success? && self.errors.is_empty() {
            return None;
        }
        let mut lines = self.errors;
        lines.extend(self.warnings);
        if self.dropped > 0 {
            lines.push(format!(
                "[OCG omitted {} additional diagnostics]",
                self.dropped
            ));
        }
        let omitted = self.dropped > 0 || self.truncated_line;
        Some((lines.join("\n"), omitted))
    }
}

fn parse_check_line(
    line: &str,
    errors: &mut Vec<String>,
    warnings: &mut Vec<String>,
    dropped: &mut usize,
    truncated_line: &mut bool,
    success: &mut Option<bool>,
) -> bool {
    let Ok(record) = serde_json::from_str::<Value>(line) else {
        return false;
    };
    let Some(reason) = record["reason"].as_str() else {
        return false;
    };
    match reason {
        "compiler-message" => {
            let message = &record["message"];
            let Some(level) = message["level"].as_str() else {
                return false;
            };
            if !matches!(level, "error" | "warning") {
                return true;
            }
            let Some(spans) = message["spans"].as_array() else {
                return false;
            };
            let span = spans
                .iter()
                .find(|span| span["is_primary"] == true)
                .or_else(|| spans.first());
            let (path, line_num, column) = match span {
                Some(span) => {
                    let (Some(p), Some(l), Some(c)) = (
                        span["file_name"].as_str(),
                        span["line_start"].as_u64(),
                        span["column_start"].as_u64(),
                    ) else {
                        return false;
                    };
                    (p, l, c)
                }
                None => ("<unknown>", 0, 0),
            };
            let code = message["code"]["code"]
                .as_str()
                .map(|code| format!("[{code}]"))
                .unwrap_or_default();
            let Some(raw_text) = message["message"].as_str() else {
                return false;
            };
            let text = raw_text.split_whitespace().collect::<Vec<_>>().join(" ");
            let (formatted, was_truncated) = truncate_str(
                &format!("{path}:{line_num}:{column} {level}{code} {text}"),
                MAX_DIAGNOSTIC_LINE_BYTES,
            );
            *truncated_line |= was_truncated;
            if level == "error" {
                if errors.len() < MAX_CHECK_ERRORS {
                    errors.push(formatted);
                } else {
                    *dropped = dropped.saturating_add(1);
                }
            } else if warnings.len() < MAX_CHECK_WARNINGS {
                warnings.push(formatted);
            } else {
                *dropped = dropped.saturating_add(1);
            }
            true
        }
        "compiler-artifact" | "build-script-executed" => true,
        "build-finished" => {
            *success = record["success"].as_bool();
            true
        }
        _ => false,
    }
}

#[derive(Default)]
struct CargoTestCompactor {
    scanner: LineScanner,
    results: Vec<String>,
    failed: Vec<(String, Vec<String>)>,
    dropped_failures: usize,
    dropped_results: usize,
    dropped_detail_lines: usize,
    truncated_line: bool,
    active_failure: Option<usize>,
    in_panic: bool,
}

impl CargoTestCompactor {
    fn push(&mut self, bytes: &[u8]) {
        if self.scanner.unreliable() {
            return;
        }
        let mut lines = Vec::new();
        self.scanner.feed(bytes, |line| lines.push(line.to_owned()));
        for line in lines {
            self.handle_line(&line);
        }
    }

    fn handle_line(&mut self, line: &str) {
        if line.starts_with("test result: ok.") || line.starts_with("test result: FAILED.") {
            self.active_failure = None;
            self.in_panic = false;
            let (formatted, was_truncated) = truncate_str(line, MAX_DIAGNOSTIC_LINE_BYTES);
            self.truncated_line |= was_truncated;
            if self.results.len() < MAX_TEST_RESULTS {
                self.results.push(formatted);
            } else if line.starts_with("test result: FAILED.")
                && let Some(pos) = self
                    .results
                    .iter()
                    .rposition(|item| item.starts_with("test result: ok."))
            {
                self.results[pos] = formatted;
                self.dropped_results = self.dropped_results.saturating_add(1);
            } else {
                self.dropped_results = self.dropped_results.saturating_add(1);
            }
            return;
        }
        if let Some(name) = line
            .strip_prefix("test ")
            .and_then(|rest| rest.strip_suffix(" ... FAILED"))
        {
            self.active_failure = None;
            self.in_panic = false;
            if self.failed.len() < MAX_FAILED_TESTS {
                self.failed.push((name.to_owned(), Vec::new()));
            } else {
                self.dropped_failures = self.dropped_failures.saturating_add(1);
            }
            return;
        }
        if let Some(name) = line
            .strip_prefix("---- ")
            .and_then(|rest| rest.strip_suffix(" stdout ----"))
        {
            self.active_failure = self.failed.iter().position(|(n, _)| n == name);
            self.in_panic = false;
            return;
        }
        if line.starts_with("failures:") {
            self.active_failure = None;
            self.in_panic = false;
            return;
        }
        if let Some(idx) = self.active_failure {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                return;
            }
            let details = &mut self.failed[idx].1;
            if !self.in_panic && line.starts_with("thread '") && line.contains("panicked at") {
                self.in_panic = true;
                details.clear();
            }
            if details.len() < MAX_TEST_DETAIL_LINES {
                let (formatted, was_truncated) = truncate_str(line, MAX_DIAGNOSTIC_LINE_BYTES);
                self.truncated_line |= was_truncated;
                details.push(formatted);
            } else {
                self.dropped_detail_lines = self.dropped_detail_lines.saturating_add(1);
            }
        }
    }

    fn finish(mut self) -> Option<(String, bool)> {
        let mut tail = Vec::new();
        self.scanner.finish(|line| tail.push(line.to_owned()));
        for line in tail {
            self.handle_line(&line);
        }
        if self.scanner.unreliable() || self.results.is_empty() {
            return None;
        }
        let mut output = Vec::new();
        for (name, details) in self.failed {
            output.push(name);
            output.extend(details);
        }
        let (failed_results, ok_results): (Vec<_>, Vec<_>) = self
            .results
            .into_iter()
            .partition(|line| line.starts_with("test result: FAILED."));
        output.extend(failed_results);
        output.extend(ok_results);
        if self.dropped_failures > 0 {
            output.push(format!(
                "[OCG omitted {} additional failed tests]",
                self.dropped_failures
            ));
        }
        if self.dropped_detail_lines > 0 {
            output.push(format!(
                "[OCG omitted {} additional failure detail lines]",
                self.dropped_detail_lines
            ));
        }
        if self.dropped_results > 0 {
            output.push(format!(
                "[OCG omitted {} additional test results]",
                self.dropped_results
            ));
        }
        let omitted = self.dropped_failures > 0
            || self.dropped_detail_lines > 0
            || self.dropped_results > 0
            || self.truncated_line;
        Some((output.join("\n"), omitted))
    }
}

fn result(selected: &Selected, argv: &[String], ended: &Ended) -> Value {
    let mut compacted_capture = None;
    let mut compacted_omitted = false;
    if matches!(selected, Selected::Preset(_))
        && let Some((text, omitted)) = &ended.stdout.compacted
    {
        let mut capture = Capture::default();
        capture.push(text.as_bytes());
        compacted_capture = Some(capture);
        compacted_omitted = *omitted;
    }
    let (stdout, stderr) = match &compacted_capture {
        Some(stdout) => (stdout, &ended.stderr),
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
            "truncated": stdout_cut || stderr_cut || compacted_omitted,
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
        (Some(stdout), Some(stderr)) => match reader(stdout, invocation.preset) {
            Some(stdout) => reader(stderr, None).map(|stderr| (stdout, stderr)),
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

fn reader(
    mut pipe: impl Read + Send + 'static,
    preset: Option<Preset>,
) -> Option<JoinHandle<Capture>> {
    thread::Builder::new()
        .name("ocg-command-output".to_owned())
        .spawn(move || {
            let mut capture = Capture::default();
            let mut compactor = StreamCompactor::for_preset(preset);
            let mut buffer = [0; 8 * 1024];
            loop {
                match pipe.read(&mut buffer) {
                    Ok(0) => break,
                    Ok(read) => {
                        capture.push(&buffer[..read]);
                        if let Some(compactor) = &mut compactor {
                            compactor.push(&buffer[..read]);
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                    Err(_) => {
                        capture.unread = true;
                        break;
                    }
                }
            }
            if !capture.unread {
                capture.compacted = compactor.and_then(StreamCompactor::finish);
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
            preset: match selected {
                Selected::Preset(preset) => Some(*preset),
                Selected::Project(_) => None,
            },
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
