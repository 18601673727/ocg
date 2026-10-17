//! The smallest Language Server Protocol client that answers a question
//! about one file: `initialize`/`initialized`, `textDocument/didOpen`,
//! `textDocument/publishDiagnostics`, `textDocument/definition`,
//! `textDocument/hover` and `shutdown`/`exit`, over JSON-RPC on a server's
//! stdin and stdout. This is the bounded transport for Workspace semantic queries.
//!
//! A server is started as a native process, named by the operator and with
//! the arguments, environment and client identity this module fixes, and is
//! spoken to over pipes: `Content-Length` framed messages correlated by
//! JSON-RPC id. stdout is read by a thread of its own, which routes every
//! decoded message: a response to the request that is still waiting for that
//! id, a notification to the caller waiting for one, or a server-to-client
//! request to a fixed answer or refusal. stderr is drained by a thread of its
//! own, so neither pipe can block the server.
//!
//! Two things this deliberately does not do. It does not trust ordering: no
//! message is a response because it arrived after a request was sent, only
//! because its id names one that is still waiting, and a
//! `publishDiagnostics` notification is never taken for a response. And it
//! does not act on a server's initiative: `workspace/applyEdit` and
//! `workspace/executeCommand` are refused, no code action, rename or
//! workspace edit is applied, and only the capabilities this client
//! implements are advertised, so a server is never asked for what it cannot
//! have here.
//!
//! What *starting* a language server does is not bounded by this module: a
//! server may read the whole Workspace, load plugins and run project code
//! such as a build script, with OCG's own permissions and outside any
//! sandbox. See [`crate::orchestration::workspace_lsp`].

use std::collections::HashMap;
use std::io::{Read, Write};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread;
use std::time::{Duration, Instant};

use flume::{self, Sender};
use rustix::process::{Pid, Signal, WaitId, WaitIdOptions};
use serde_json::{Value, json};

use crate::orchestration::workspace_lsp::file_uri;

/// The largest message this client accepts or sends. A server that announces
/// more is a protocol failure, not a larger answer to bound later.
pub(crate) const MAX_MESSAGE_BYTES: usize = 8 * 1024 * 1024;

/// The largest header block this client reads before it stops waiting for one.
const MAX_HEADER_BYTES: usize = 8 * 1024;

/// The header terminator of a `Content-Length` framed message.
const HEADER_END: &[u8] = b"\r\n\r\n";

/// The id the best-effort `shutdown` of [`Client::drop`] sends. Ordinary ids
/// start at 1 and count up, so this can never collide with one.
const SHUTDOWN_ID: i64 = -1;

/// How long [`Client::drop`] waits for a `shutdown` answer, and then for the
/// process to leave. Bounded, because a drop cannot wait indefinitely.
const SHUTDOWN_GRACE: Duration = Duration::from_millis(500);
const EXIT_GRACE: Duration = Duration::from_millis(500);
const POLL: Duration = Duration::from_millis(10);

/// Why a server's stream could not be read, or could not be believed.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ProtocolError {
    #[error("the server's output is not `Content-Length` framed: {0}")]
    Framing(String),
    #[error("the server sent something that is not a JSON-RPC message: {0}")]
    Message(String),
    #[error("the server's stream ended in the middle of a message")]
    Truncated,
    #[error("the server announced a message of more than {MAX_MESSAGE_BYTES} bytes")]
    TooLarge,
    #[error("the server stopped before it answered")]
    Closed,
    #[error("the server exceeded the bounded notification queue")]
    QueueFull,
}

/// Why a request did not produce an answer.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum LspError {
    #[error("the language server could not be started: {0}")]
    NotStarted(String),
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
    #[error("{method} was not answered within {limit:?}")]
    TimedOut { method: String, limit: Duration },
    #[error("the language server refused {method} with error {code}: {message}")]
    Rejected {
        method: String,
        code: i64,
        message: String,
    },
}

/// How one language server is started. Every field is OCG's: the operator
/// chose the program, and this module fixes the arguments, the working
/// directory, the environment and what the server is told about its client.
pub(crate) struct Launch {
    /// The executable, run directly. No shell, and no name from a Provider.
    pub(crate) program: PathBuf,
    /// The fixed arguments of this server's LSP transport.
    pub(crate) arguments: Vec<String>,
    /// The Workspace root, as the server's working directory and `rootUri`.
    pub(crate) root: PathBuf,
    /// The whole environment of the process: an allowlist OCG chooses, never
    /// OCG's own environment and never a Provider's credential.
    pub(crate) variables: Vec<(String, String)>,
}

/// The child process of a session, which its `Drop` stops and reaps.
struct Process {
    child: Mutex<Option<Child>>,
    group: Pid,
}

impl Process {
    /// Reaps the process when it exits within `limit`, and says whether it
    /// was. Polled rather than waited on, so a server that never leaves
    /// cannot hold this up for longer than `limit`.
    fn reaps_within(&self, limit: Duration) -> bool {
        let deadline = Instant::now() + limit;
        loop {
            let reaped = {
                let mut guard = self.lock();
                match guard.as_ref() {
                    None => true,
                    Some(child) => match rustix::process::waitid(
                        WaitId::Pid(Pid::from_child(child)),
                        WaitIdOptions::EXITED | WaitIdOptions::NOHANG | WaitIdOptions::NOWAIT,
                    ) {
                        Ok(Some(_)) => {
                            if let Err(error) =
                                rustix::process::kill_process_group(self.group, Signal::KILL)
                                && error != rustix::io::Errno::SRCH
                            {
                                log::error!("LSP descendant cleanup failed: {error}");
                            }
                            if let Some(mut child) = guard.take()
                                && let Err(error) = child.wait()
                            {
                                log::error!("LSP reap failed: {error}");
                            }
                            true
                        }
                        Ok(None) => false,
                        Err(rustix::io::Errno::INTR) => false,
                        Err(error) => {
                            log::error!("LSP wait failed: {error}");
                            false
                        }
                    },
                }
            };
            if reaped {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            thread::sleep(POLL);
        }
    }

    /// Kills the whole group and reaps its leader. Whatever is left of the
    /// group is killed before the leader is reaped, so a server that spawned
    /// Cargo, Node or a plugin cannot outlive its session. Doing it twice is
    /// not an error: the leader is reaped once.
    fn kill_and_reap(&self) {
        let mut guard = self.lock();
        let Some(mut child) = guard.take() else {
            return;
        };
        if let Err(error) = rustix::process::kill_process_group(self.group, Signal::KILL)
            && error != rustix::io::Errno::SRCH
        {
            log::error!("LSP group cleanup failed: {error}");
        }
        if let Err(error) = child.wait() {
            log::error!("LSP reap failed: {error}");
        }
    }

    fn lock(&self) -> MutexGuard<'_, Option<Child>> {
        self.child.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// What a waiting request is sent when its answer arrives.
#[derive(Clone, Debug, PartialEq)]
enum Outcome {
    Answered(Value),
    Refused { code: i64, message: String },
}

/// What OCG answers a server-to-client request with.
enum Answer {
    Result(Value),
    Refusal { code: i64, message: String },
}

/// Where a session's messages go: the standard input of a server process, or
/// whatever stands in for one in a test.
type Sink = Box<dyn Write + Send>;

/// Frames and writes `Content-Length` delimited JSON-RPC messages.
struct Writer {
    sink: Mutex<Sink>,
}

impl Writer {
    fn new(sink: Sink) -> Self {
        Self {
            sink: Mutex::new(sink),
        }
    }

    fn send(&self, message: &Value) -> Result<(), ProtocolError> {
        let body = serde_json::to_vec(message)
            .map_err(|error| ProtocolError::Framing(error.to_string()))?;
        if body.len() > MAX_MESSAGE_BYTES {
            return Err(ProtocolError::TooLarge);
        }
        let mut sink = self.sink.lock().unwrap_or_else(PoisonError::into_inner);
        let mut frame = format!("Content-Length: {}\r\n\r\n", body.len()).into_bytes();
        frame.extend_from_slice(&body);
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut remaining = frame.as_slice();
        while !remaining.is_empty() {
            if Instant::now() >= deadline {
                return Err(ProtocolError::Framing(
                    "server input write timed out".to_owned(),
                ));
            }
            match sink.write(remaining) {
                Ok(0) => return Err(ProtocolError::Closed),
                Ok(written) => remaining = &remaining[written..],
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(1))
                }
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) => return Err(ProtocolError::Framing(error.to_string())),
            }
        }
        sink.flush()
            .map_err(|error| ProtocolError::Framing(error.to_string()))
    }
}

/// Routes the decoded messages of one server. Which message is a response is
/// decided only by its JSON-RPC id and its shape, never by when it arrived.
struct Router {
    writer: Arc<Writer>,
    pending: Arc<Mutex<HashMap<i64, Sender<Outcome>>>>,
    notifications: Sender<Value>,
    root: PathBuf,
    shutdown_acked: Arc<AtomicBool>,
}

impl Router {
    fn route(&mut self, message: Value) -> Result<(), ProtocolError> {
        let Value::Object(object) = &message else {
            return Err(ProtocolError::Message(
                "a message that is not a JSON object".to_owned(),
            ));
        };
        if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
            return Err(ProtocolError::Message(
                "JSON-RPC version must be 2.0".to_owned(),
            ));
        }
        // Taken out of the message before it is matched, so the whole
        // message can be moved on when it is only a notification.
        let parameters = object.get("params").cloned();
        let method = object.get("method").cloned();
        let id = object.get("id").cloned();
        // The protocol allows a message to carry a method or a result, never
        // both: a server that answers with a method is not a server whose
        // answers can be told apart from its questions.
        if method.is_some() && (object.get("result").is_some() || object.get("error").is_some()) {
            return Err(ProtocolError::Message(
                "a response that also carries a method".to_owned(),
            ));
        }
        match (method, id) {
            (Some(method), Some(id)) => {
                let Some(method) = method.as_str() else {
                    return Err(ProtocolError::Message(
                        "a request whose method is not a string".to_owned(),
                    ));
                };
                let id = request_id(&id)?;
                let response = match answer(method, parameters.as_ref(), &self.root) {
                    Answer::Result(result) => json!({"jsonrpc": "2.0", "id": id, "result": result}),
                    Answer::Refusal { code, message } => json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "error": {"code": code, "message": message},
                    }),
                };
                if id == SHUTDOWN_ID {
                    self.shutdown_acked.store(true, Ordering::Relaxed);
                }
                self.writer.send(&response)?;
                Ok(())
            }
            (Some(method), None) => {
                if !method.is_string() {
                    return Err(ProtocolError::Message(
                        "a notification whose method is not a string".to_owned(),
                    ));
                }
                if message.to_string().len() > 1024 * 1024 {
                    return Err(ProtocolError::TooLarge);
                }
                self.notifications
                    .try_send(message)
                    .map_err(|_| ProtocolError::QueueFull)?;
                Ok(())
            }
            (None, Some(id)) => {
                let id = response_id(&id)?;
                if id == SHUTDOWN_ID {
                    self.shutdown_acked.store(true, Ordering::Relaxed);
                }
                let outcome = match (object.get("result"), object.get("error")) {
                    (Some(_), Some(_)) => {
                        return Err(ProtocolError::Message(
                            "a response with both a result and an error".to_owned(),
                        ));
                    }
                    (Some(result), None) => Outcome::Answered(result.clone()),
                    (None, Some(error)) => Outcome::Refused {
                        code: error.get("code").and_then(Value::as_i64).unwrap_or(0),
                        message: error
                            .get("message")
                            .and_then(Value::as_str)
                            .unwrap_or("no message")
                            .to_owned(),
                    },
                    (None, None) => {
                        return Err(ProtocolError::Message(
                            "a response with neither a result nor an error".to_owned(),
                        ));
                    }
                };
                // Claim the response once before delivering it. A duplicate
                // response must never block the reader on a full answer slot
                // while cancellation is waiting for the pending-map lock.
                let waiting = self
                    .pending
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .remove(&id);
                if let Some(waiting) = waiting {
                    waiting.try_send(outcome).ok();
                }
                Ok(())
            }
            (None, None) => Err(ProtocolError::Message(
                "a message that is neither a request, a notification nor a response".to_owned(),
            )),
        }
    }
}

/// The JSON-RPC id of a server-to-client request: an integer or a string,
/// which are the two forms the protocol allows.
fn request_id(id: &Value) -> Result<Value, ProtocolError> {
    match id {
        Value::Number(number) if number.is_i64() || number.is_u64() => Ok(id.clone()),
        Value::String(_) => Ok(id.clone()),
        _ => Err(ProtocolError::Message(
            "a request whose id is neither a number nor a string".to_owned(),
        )),
    }
}

/// The JSON-RPC id of a response, when it is one this client could have sent.
fn response_id(id: &Value) -> Result<i64, ProtocolError> {
    match id {
        Value::Number(number) => number.as_i64().ok_or_else(|| {
            ProtocolError::Message("a response whose id is not an integer".to_owned())
        }),
        _ => Err(ProtocolError::Message(
            "a response whose id is not a number".to_owned(),
        )),
    }
}

/// What OCG answers a server-to-client request with. Anything it does not
/// implement, and anything that would change the Workspace, is refused with
/// `MethodNotFound` rather than left unanswered, so a server is never left
/// waiting for a client that will not answer.
fn answer(method: &str, parameters: Option<&Value>, root: &Path) -> Answer {
    let items = parameters
        .and_then(|parameters| parameters.get("items"))
        .and_then(Value::as_array)
        .map_or(0, Vec::len);
    match method {
        // Not advertised, so a conforming server does not ask; answered
        // anyway, with "no configuration", rather than left waiting.
        "workspace/configuration" => Answer::Result(Value::Array(vec![Value::Null; items])),
        "workspace/workspaceFolders" => Answer::Result(json!([{
            "uri": file_uri(root),
            "name": root.file_name().and_then(|name| name.to_str()).unwrap_or("workspace"),
        }])),
        "window/workDoneProgress/create"
        | "client/registerCapability"
        | "client/unregisterCapability"
        | "window/showMessageRequest" => Answer::Result(Value::Null),
        "workspace/applyEdit" | "workspace/executeCommand" => Answer::Refusal {
            code: -32601,
            message: "OCG's language-server client applies no edit and runs no command".to_owned(),
        },
        _ => Answer::Refusal {
            code: -32601,
            message: "not supported by OCG's language-server client".to_owned(),
        },
    }
}

/// Frames and decodes `Content-Length` delimited JSON-RPC messages from a
/// blocking source. It never assumes a message arrives whole, or in one read,
/// or aligned with anything else.
struct Frames<R> {
    source: R,
    buffer: Vec<u8>,
    start: usize,
    ended: bool,
}

impl<R: Read> Frames<R> {
    fn new(source: R) -> Self {
        Self {
            source,
            buffer: Vec::with_capacity(8 * 1024),
            start: 0,
            ended: false,
        }
    }

    /// The next message body, or `None` at a clean end of stream. An end of
    /// stream with a partial message in the buffer is
    /// [`ProtocolError::Truncated`].
    fn next(&mut self) -> Result<Option<Vec<u8>>, ProtocolError> {
        loop {
            let available = &self.buffer[self.start..];
            match find(available, HEADER_END) {
                Some(index) => {
                    let length = content_length(&available[..index])?;
                    if length > MAX_MESSAGE_BYTES {
                        return Err(ProtocolError::TooLarge);
                    }
                    let body = index + HEADER_END.len();
                    if available.len() >= body + length {
                        let message = available[body..body + length].to_vec();
                        self.start += body + length;
                        return Ok(Some(message));
                    }
                }
                None if available.len() > MAX_HEADER_BYTES => {
                    return Err(ProtocolError::Framing(format!(
                        "a header block of more than {MAX_HEADER_BYTES} bytes"
                    )));
                }
                None => {}
            }
            if self.ended {
                return if self.start == self.buffer.len() {
                    Ok(None)
                } else {
                    Err(ProtocolError::Truncated)
                };
            }
            self.fill()?;
        }
    }

    fn fill(&mut self) -> Result<(), ProtocolError> {
        if self.start > 32 * 1024 {
            self.buffer.drain(..self.start);
            self.start = 0;
        }
        let at = self.buffer.len();
        self.buffer.resize(at + 16 * 1024, 0);
        let read = self
            .source
            .read(&mut self.buffer[at..])
            .map_err(|error| ProtocolError::Framing(error.to_string()))?;
        self.buffer.truncate(at + read);
        if read == 0 {
            self.ended = true;
        }
        Ok(())
    }
}

/// The first index of `needle` in `haystack`, or `None`.
fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// The `Content-Length` a header block announces. A block without one, or
/// with one this client cannot read, is a protocol failure rather than a
/// message to guess at.
fn content_length(header: &[u8]) -> Result<usize, ProtocolError> {
    let mut length = None;
    for line in header.split(|byte| *byte == b'\n') {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        if line.is_empty() {
            continue;
        }
        let Some(colon) = line.iter().position(|byte| *byte == b':') else {
            return Err(ProtocolError::Framing(
                "a header line without a colon".to_owned(),
            ));
        };
        if !line[..colon].eq_ignore_ascii_case(b"content-length") {
            continue;
        }
        let value = std::str::from_utf8(&line[colon + 1..])
            .map_err(|_| ProtocolError::Framing("a `Content-Length` that is not UTF-8".to_owned()))?
            .trim();
        let parsed = value.parse::<usize>().map_err(|_| {
            ProtocolError::Framing("a `Content-Length` that is not a length".to_owned())
        })?;
        if length.replace(parsed).is_some() {
            return Err(ProtocolError::Framing(
                "two `Content-Length` headers".to_owned(),
            ));
        }
    }
    length.ok_or_else(|| ProtocolError::Framing("no `Content-Length` header".to_owned()))
}

/// One running language server session. Dropping it stops and reaps the
/// process: gracefully when the server answers `shutdown` in time, and by
/// killing its group otherwise.
pub(crate) struct Client {
    writer: Arc<Writer>,
    process: Option<Arc<Process>>,
    root: PathBuf,
    next_id: AtomicI64,
    pending: Arc<Mutex<HashMap<i64, Sender<Outcome>>>>,
    notifications: Mutex<flume::Receiver<Value>>,
    failure: Arc<Mutex<Option<ProtocolError>>>,
    shutdown_acked: Arc<AtomicBool>,
}

/// Builds a session over whatever streams it is given.
fn assemble(
    sink: Sink,
    stdout: impl Read + Send + 'static,
    stderr: impl Read + Send + 'static,
    process: Option<Arc<Process>>,
    root: PathBuf,
) -> Result<Arc<Client>, LspError> {
    let writer = Arc::new(Writer::new(sink));
    let pending: Arc<Mutex<HashMap<i64, Sender<Outcome>>>> = Arc::new(Mutex::new(HashMap::new()));
    let (notifications, receiver) = flume::bounded(32);
    let failure = Arc::new(Mutex::new(None));
    let shutdown_acked = Arc::new(AtomicBool::new(false));
    thread::Builder::new()
        .name("ocg-lsp-stderr".to_owned())
        .spawn(move || drain(stderr))
        .map_err(|error| {
            if let Some(process) = &process {
                process.kill_and_reap();
            }
            LspError::NotStarted(error.to_string())
        })?;
    thread::Builder::new()
        .name("ocg-lsp-output".to_owned())
        .spawn({
            let writer = writer.clone();
            let pending = pending.clone();
            let failure = failure.clone();
            let shutdown_acked = shutdown_acked.clone();
            let root = root.clone();
            let process = process.clone();
            move || {
                let outcome = read_messages(
                    stdout,
                    writer,
                    pending.clone(),
                    notifications,
                    shutdown_acked,
                    &root,
                );
                if let Err(problem) = outcome {
                    record(&failure, problem);
                }
                if let Some(process) = process {
                    process.kill_and_reap();
                }
                // The stream is over: no answer can arrive from here on, so
                // everything waiting is failed rather than left waiting.
                record(&failure, ProtocolError::Closed);
                let waiting = pending
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .drain()
                    .map(|(_, waiting)| waiting)
                    .collect::<Vec<_>>();
                for waiting in waiting {
                    waiting
                        .try_send(Outcome::Refused {
                            code: -32000,
                            message: ProtocolError::Closed.to_string(),
                        })
                        .ok();
                }
            }
        })
        .map_err(|error| {
            if let Some(process) = &process {
                process.kill_and_reap();
            }
            LspError::NotStarted(error.to_string())
        })?;
    Ok(Arc::new(Client {
        writer,
        process,
        root,
        next_id: AtomicI64::new(1),
        pending,
        notifications: Mutex::new(receiver),
        failure,
        shutdown_acked,
    }))
}

impl Client {
    pub(crate) fn terminate(&self) {
        record(&self.failure, ProtocolError::Closed);
        if let Some(process) = &self.process {
            process.kill_and_reap();
        }
    }

    pub(crate) fn clear_notifications(&self) {
        let notifications = self
            .notifications
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        while notifications.try_recv().is_ok() {}
    }

    /// Starts `launch` and its two reader threads. It does not initialize:
    /// [`Self::initialize`] does, so a session's own timeout bounds that too.
    ///
    /// # Errors
    ///
    /// Returns [`LspError::NotStarted`] when the process cannot be started or
    /// its pipes cannot be taken.
    pub(crate) fn start(launch: &Launch) -> Result<Arc<Self>, LspError> {
        let mut command = Command::new(&launch.program);
        command
            .args(&launch.arguments)
            .current_dir(&launch.root)
            .env_clear()
            .envs(launch.variables.iter().cloned())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .process_group(0);
        let mut child = command
            .spawn()
            .map_err(|error| LspError::NotStarted(error.to_string()))?;
        let group = Pid::from_child(&child);
        let (Some(stdin), Some(stdout), Some(stderr)) =
            (child.stdin.take(), child.stdout.take(), child.stderr.take())
        else {
            rustix::process::kill_process_group(group, Signal::KILL).ok();
            child.wait().ok();
            return Err(LspError::NotStarted(
                "the server's standard streams were not piped".to_owned(),
            ));
        };
        if let Err(error) = rustix::fs::fcntl_setfl(&stdin, rustix::fs::OFlags::NONBLOCK) {
            if let Err(problem) = rustix::process::kill_process_group(group, Signal::KILL) {
                log::error!("LSP startup cleanup failed: {problem}");
            }
            if let Err(problem) = child.wait() {
                log::error!("LSP startup reap failed: {problem}");
            }
            return Err(LspError::NotStarted(error.to_string()));
        }
        let process = Arc::new(Process {
            child: Mutex::new(Some(child)),
            group,
        });
        assemble(
            Box::new(stdin),
            stdout,
            stderr,
            Some(process),
            launch.root.clone(),
        )
    }

    /// A session over pipes a test provides, so what this client does with a
    /// server can be exercised without one. Everything but starting and
    /// stopping a process is the same code an execution runs.
    #[cfg(test)]
    pub(crate) fn attach(
        sink: Sink,
        stdout: impl Read + Send + 'static,
        root: PathBuf,
    ) -> Arc<Self> {
        assemble(sink, stdout, std::io::empty(), None, root).expect("reader threads")
    }

    /// The Workspace root this session's server was started in.
    fn root(&self) -> &Path {
        &self.root
    }

    /// Whether the server's stream is still believed.
    pub(crate) fn alive(&self) -> bool {
        self.failure
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_none()
    }

    /// Sends `initialize` and `initialized`, advertising only what this
    /// client implements.
    ///
    /// # Errors
    ///
    /// Returns why the server did not complete initialization.
    pub(crate) async fn initialize(self: &Arc<Self>, limit: Duration) -> Result<(), LspError> {
        let root = file_uri(self.root());
        let name = self
            .root()
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("workspace")
            .to_owned();
        let parameters = json!({
            "processId": std::process::id(),
            "clientInfo": {"name": "ocg-lsp", "version": env!("CARGO_PKG_VERSION")},
            "rootUri": root,
            "workspaceFolders": [{"uri": root, "name": name}],
            "capabilities": capabilities(),
            // Suppress eager build-script/proc-macro loading and automatic
            // package acquisition. Rust diagnostics use the server's bounded
            // cargo check lifecycle; native server execution is not sandboxed.
            "initializationOptions": {
                "cargo": {"buildScripts": {"enable": false}},
                "cachePriming": {"enable": false},
                "numThreads": 2,
                "checkOnSave": true,
                "procMacro": {"enable": false},
                "tsserver": {"disableAutomaticTypeAcquisition": true},
            },
        });
        self.request("initialize", parameters, limit).await?;
        self.notify("initialized", json!({}))?;
        Ok(())
    }

    /// Sends `method` with `parameters` and waits for the answer that names
    /// its id, or for `limit` to pass.
    ///
    /// # Errors
    ///
    /// Returns why there is no answer: the stream already failed, the message
    /// could not be written, the server refused it, or it did not answer in
    /// time.
    pub(crate) async fn request(
        &self,
        method: &str,
        parameters: Value,
        limit: Duration,
    ) -> Result<Value, LspError> {
        if !self.alive() {
            return Err(self.failure().into());
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (answer, answers) = flume::bounded(1);
        self.pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(id, answer);
        let mut pending = PendingRequest {
            client: self,
            id,
            completed: false,
        };
        let message = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": parameters});
        if let Err(problem) = self.writer.send(&message) {
            self.forget(id);
            return Err(problem.into());
        }
        let outcome = match ntex::time::timeout(limit, answers.recv_async()).await {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(_)) => {
                self.forget(id);
                return Err(self.failure().into());
            }
            Err(()) => {
                self.forget(id);
                return Err(LspError::TimedOut {
                    method: method.to_owned(),
                    limit,
                });
            }
        };
        pending.completed = true;
        self.forget(id);
        match outcome {
            Outcome::Answered(value) => Ok(value),
            Outcome::Refused { code, message } => Err(LspError::Rejected {
                method: method.to_owned(),
                code,
                message,
            }),
        }
    }

    /// Sends a notification, which has no answer to wait for.
    ///
    /// # Errors
    ///
    /// Returns why the message could not be written.
    pub(crate) fn notify(&self, method: &str, parameters: Value) -> Result<(), LspError> {
        let message = json!({"jsonrpc": "2.0", "method": method, "params": parameters});
        self.writer.send(&message).map_err(LspError::from)
    }

    /// Waits for the next notification of `method`, or for `limit` to pass.
    /// A notification of another method is not an answer to this and is
    /// dropped rather than kept for a later caller.
    pub(crate) async fn notification(&self, method: &str, limit: Duration) -> Option<Value> {
        let deadline = Instant::now() + limit;
        let notifications = self
            .notifications
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        loop {
            let remaining = deadline.checked_duration_since(Instant::now())?;
            let received = ntex::time::timeout(remaining, notifications.recv_async()).await;
            let message = match received {
                Ok(Ok(message)) => message,
                // The reader is gone, or the limit passed.
                _ => return None,
            };
            if message.get("method").and_then(Value::as_str) == Some(method) {
                return Some(message);
            }
        }
    }

    /// Sends `shutdown` and `exit`, then reaps the process, killing its group
    /// when it does not leave within `limit`.
    ///
    /// # Errors
    ///
    /// Returns why the server did not confirm `shutdown`.
    #[cfg(test)]
    pub(crate) async fn shutdown(&self, limit: Duration) -> Result<(), LspError> {
        let result = self.request("shutdown", Value::Null, limit).await;
        self.notify("exit", Value::Null).ok();
        if let Some(process) = &self.process {
            process.reaps_within(limit);
            process.kill_and_reap();
        }
        result.map(|_| ())
    }

    fn failure(&self) -> ProtocolError {
        self.failure
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
            .unwrap_or(ProtocolError::Closed)
    }

    fn forget(&self, id: i64) {
        self.pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&id);
    }
}

struct PendingRequest<'a> {
    client: &'a Client,
    id: i64,
    completed: bool,
}
impl Drop for PendingRequest<'_> {
    fn drop(&mut self) {
        self.client.forget(self.id);
        if !self.completed
            && let Err(error) = self
                .client
                .notify("$/cancelRequest", json!({"id": self.id}))
        {
            log::debug!("LSP cancellation could not be sent: {error}");
        }
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        // A best-effort `shutdown`, then `exit` for a server that answers it
        // in time; either way the group is killed and the leader reaped, so
        // nothing a server started outlives this drop.
        let sent = self
            .writer
            .send(&json!({"jsonrpc": "2.0", "id": SHUTDOWN_ID, "method": "shutdown", "params": Value::Null}))
            .is_ok();
        if sent {
            let deadline = Instant::now() + SHUTDOWN_GRACE;
            while !self.shutdown_acked.load(Ordering::Relaxed) && Instant::now() < deadline {
                thread::sleep(POLL);
            }
            if self.shutdown_acked.load(Ordering::Relaxed) {
                self.writer
                    .send(&json!({"jsonrpc": "2.0", "method": "exit"}))
                    .ok();
            }
        }
        if let Some(process) = &self.process {
            process.reaps_within(EXIT_GRACE);
            process.kill_and_reap();
        }
    }
}

/// The capabilities this client actually implements: UTF-16 positions, full
/// document synchronization, definition and hover. Nothing it does not do is
/// advertised, so a conforming server is never asked for it.
fn capabilities() -> Value {
    json!({
        "general": {"positionEncodings": ["utf-16"]},
        "textDocument": {
            "synchronization": {
                "dynamicRegistration": false,
                "willSave": false,
                "willSaveWaitUntil": false,
                "didSave": true,
            },
            "definition": {"dynamicRegistration": false, "linkSupport": true},
            "hover": {"dynamicRegistration": false, "contentFormat": ["plaintext"]},
            "publishDiagnostics": {
                "relatedInformation": false,
                "versionSupport": true,
                "tagSupport": {"valueSet": []},
            },
        },
        "window": {"workDoneProgress": true},
        "workspace": {"workspaceFolders": true, "configuration": false},
    })
}

/// Reads a server's stdout until it ends or fails, routing every message.
fn read_messages(
    stdout: impl Read,
    writer: Arc<Writer>,
    pending: Arc<Mutex<HashMap<i64, Sender<Outcome>>>>,
    notifications: Sender<Value>,
    shutdown_acked: Arc<AtomicBool>,
    root: &Path,
) -> Result<(), ProtocolError> {
    let mut frames = Frames::new(stdout);
    let mut router = Router {
        writer,
        pending,
        notifications,
        root: root.to_path_buf(),
        shutdown_acked,
    };
    while let Some(body) = frames.next()? {
        let message: Value = serde_json::from_slice(&body)
            .map_err(|error| ProtocolError::Message(error.to_string()))?;
        router.route(message)?;
    }
    Ok(())
}

fn record(failure: &Mutex<Option<ProtocolError>>, problem: ProtocolError) {
    let mut guard = failure.lock().unwrap_or_else(PoisonError::into_inner);
    if guard.is_none() {
        *guard = Some(problem);
    }
}

/// Reads a server's stderr to its end so that it can never block, keeping
/// nothing: it is the server's own logging, which OCG neither returns nor
/// records.
fn drain(mut stderr: impl Read) {
    let mut buffer = [0; 8 * 1024];
    while let Ok(read) = stderr.read(&mut buffer) {
        if read == 0 {
            break;
        }
    }
}
#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::net::UnixStream;
    use std::sync::Arc;

    use rustix::io::Errno;
    use serde_json::json;

    use super::*;

    const SUITE: &str = "ocg-lsp-client";

    /// A session over a socket pair, with `server` acting as the language
    /// server on the other end. No process, no harness banner on the
    /// protocol stream: everything the client does with a server is the same
    /// code an execution runs.
    fn attached<F>(root: &Path, server: F) -> Arc<Client>
    where
        F: FnOnce(&mut UnixStream, &mut UnixStream) + Send + 'static,
    {
        // Two socket pairs, one per direction. The client writes on one and
        // reads on the other; the server holds the far end of each, so the
        // client never reads back what it sent.
        let (client_out, mut server_in) = UnixStream::pair().expect("a socket pair");
        let (client_in, mut server_out) = UnixStream::pair().expect("a socket pair");
        thread::Builder::new()
            .name("ocg-lsp-test-server".to_owned())
            .spawn(move || server(&mut server_out, &mut server_in))
            .expect("a thread for the fake server");
        Client::attach(Box::new(client_out), client_in, root.to_path_buf())
    }

    /// Acts as the language server on `input`, handing OCG's messages to
    /// `handle` one at a time, the way a real server would.
    fn serve(
        mut input: &mut UnixStream,
        output: &mut UnixStream,
        mut handle: impl FnMut(&mut UnixStream, Value),
    ) {
        let mut frames = Frames::new(&mut input);
        while let Ok(Some(body)) = frames.next() {
            if let Ok(message) = serde_json::from_slice::<Value>(&body) {
                handle(output, message);
            }
        }
    }

    /// The method of a message OCG sent, and its id when it was a request.
    fn request(message: &Value) -> (String, Option<Value>) {
        (
            message
                .get("method")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            message.get("id").cloned(),
        )
    }

    fn answer(stream: &mut UnixStream, message: Value) {
        let body = serde_json::to_vec(&message).expect("JSON");
        let framed = format!("Content-Length: {}\r\n\r\n", body.len()).into_bytes();
        stream.write_all(&framed).expect("write the header");
        stream.write_all(&body).expect("write the body");
        stream.flush().expect("flush");
    }

    /// What the router under test writes into, so what it sent can be read.
    #[derive(Clone)]
    struct SharedWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for SharedWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("written").extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// A reader that hands out at most `step` bytes per read, so a message
    /// can be made to arrive in as many pieces as it likes.
    struct Chunked {
        data: Vec<u8>,
        at: usize,
        step: usize,
    }

    impl Read for Chunked {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            if self.at == self.data.len() {
                return Ok(0);
            }
            let count = self.step.min(buffer.len()).min(self.data.len() - self.at);
            buffer[..count].copy_from_slice(&self.data[self.at..self.at + count]);
            self.at += count;
            Ok(count)
        }
    }

    fn framed(bodies: &[&str]) -> Vec<u8> {
        bodies
            .iter()
            .flat_map(|body| format!("Content-Length: {}\r\n\r\n{body}", body.len()).into_bytes())
            .collect()
    }

    #[test]
    fn a_message_split_across_many_reads_is_read_whole() {
        let bodies = [
            json!({"jsonrpc": "2.0", "id": 1, "result": "üñî"}).to_string(),
            json!({"jsonrpc": "2.0", "method": "textDocument/publishDiagnostics", "params": {"diagnostics": []}}).to_string(),
            json!({"jsonrpc": "2.0", "id": 2, "result": {"deep": [1, 2, 3]}}).to_string(),
        ];
        let references = bodies.iter().map(String::as_str).collect::<Vec<_>>();
        let bytes = framed(&references);
        for step in [1, 2, 3, 7, 64, 4096] {
            let mut frames = Frames::new(Chunked {
                data: bytes.clone(),
                at: 0,
                step,
            });
            for expected in &bodies {
                let body = frames
                    .next()
                    .unwrap_or_else(|problem| panic!("{step}-byte reads: {problem}"))
                    .unwrap_or_else(|| panic!("{step}-byte reads: the stream ended early"));
                assert_eq!(
                    serde_json::from_slice::<Value>(&body).expect("JSON"),
                    serde_json::from_str::<Value>(expected).expect("JSON"),
                    "a message read in {step}-byte pieces"
                );
            }
            assert_eq!(frames.next().expect("a clean end"), None);
        }
    }

    #[test]
    fn a_header_this_client_cannot_read_is_a_protocol_failure() {
        for header in [
            "Content-Type: application/vscode-jsonrpc\r\n\r\n",
            "Content-Length: nonsense\r\n\r\n",
            "Content-Length: -1\r\n\r\n",
            "Content-Length: 4\r\nContent-Length: 4\r\n\r\n",
            "Content-Length 4\r\n\r\n",
        ] {
            let mut frames = Frames::new(Chunked {
                data: header.as_bytes().to_vec(),
                at: 0,
                step: 4096,
            });
            assert!(
                matches!(frames.next(), Err(ProtocolError::Framing(_))),
                "{header:?} is refused rather than guessed at"
            );
        }
    }

    #[test]
    fn a_message_larger_than_the_limit_and_a_half_message_are_refused() {
        let oversized = format!("Content-Length: {}\r\n\r\n", MAX_MESSAGE_BYTES + 1);
        let mut frames = Frames::new(Chunked {
            data: oversized.into_bytes(),
            at: 0,
            step: 4096,
        });
        assert_eq!(frames.next(), Err(ProtocolError::TooLarge));

        let mut frames = Frames::new(Chunked {
            data: b"Content-Length: 40\r\n\r\nonly part of it".to_vec(),
            at: 0,
            step: 4096,
        });
        assert_eq!(frames.next(), Err(ProtocolError::Truncated));
    }

    /// A router over an in-memory writer, with the channels it feeds.
    type RouterFixture = (
        Router,
        Arc<Mutex<Vec<u8>>>,
        HashMap<i64, flume::Receiver<Outcome>>,
        flume::Receiver<Value>,
        Arc<AtomicBool>,
    );
    fn router() -> RouterFixture {
        let written: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
        let writer = Arc::new(Writer::new(Box::new(SharedWriter(written.clone()))));
        let pending = Arc::new(Mutex::new(HashMap::new()));
        let mut waiting = HashMap::new();
        for id in 1..=4 {
            let (sender, receiver) = flume::bounded(1);
            pending.lock().expect("pending").insert(id, sender);
            waiting.insert(id, receiver);
        }
        let (notifications, received) = flume::bounded(32);
        let shutdown_acked = Arc::new(AtomicBool::new(false));
        let router = Router {
            writer: writer.clone(),
            pending,
            notifications,
            root: PathBuf::from("/workspace"),
            shutdown_acked: shutdown_acked.clone(),
        };
        (router, written, waiting, received, shutdown_acked)
    }

    fn answered(receiver: &flume::Receiver<Outcome>) -> Outcome {
        receiver.try_recv().expect("an answer for this id")
    }

    #[test]
    fn a_response_is_matched_only_by_its_id_never_by_its_order() {
        let (mut router, _writer, mut waiting, _notifications, _acked) = router();
        // Answered out of order, with a notification between two of them,
        // and a second answer for an id that already had one.
        router
            .route(json!({"jsonrpc": "2.0", "id": 3, "result": "third"}))
            .expect("routed");
        router
            .route(json!({"jsonrpc": "2.0", "method": "window/logMessage", "params": {"type": 3, "message": "ignored"}}))
            .expect("routed");
        router
            .route(json!({"jsonrpc": "2.0", "id": 1, "result": "first"}))
            .expect("routed");
        router
            .route(json!({"jsonrpc": "2.0", "id": 4, "result": "fourth"}))
            .expect("routed");
        // An id nothing is waiting for is not an error, and answers nobody.
        router
            .route(json!({"jsonrpc": "2.0", "id": 99, "result": "nobody"}))
            .expect("routed");

        assert_eq!(
            answered(&waiting.remove(&1).expect("1")),
            Outcome::Answered(json!("first"))
        );
        assert_eq!(
            answered(&waiting.remove(&3).expect("3")),
            Outcome::Answered(json!("third"))
        );
        assert_eq!(
            answered(&waiting.remove(&4).expect("4")),
            Outcome::Answered(json!("fourth"))
        );
        assert!(
            waiting.remove(&2).expect("2").try_recv().is_err(),
            "the id that was never answered is still waiting"
        );
    }

    #[test]
    fn a_duplicate_response_cannot_block_cancellation_or_replace_the_first_answer() {
        let (mut router, _, waiting, _, _) = router();
        router
            .route(json!({"jsonrpc":"2.0","id":1,"result":"first"}))
            .expect("first answer");
        router
            .route(json!({"jsonrpc":"2.0","id":1,"result":"duplicate"}))
            .expect("late duplicate ignored");
        assert_eq!(answered(&waiting[&1]), Outcome::Answered(json!("first")));
        assert!(waiting[&1].try_recv().is_err());
        assert!(!router.pending.lock().expect("pending").contains_key(&1));
    }

    #[test]
    fn a_notification_is_never_taken_for_a_response() {
        let (mut router, _written, waiting, notifications, _acked) = router();
        router
            .route(json!({
                "jsonrpc": "2.0",
                "method": "textDocument/publishDiagnostics",
                "params": {"uri": "file:///a.rs", "diagnostics": [{"message": "no"}]},
            }))
            .expect("routed");
        assert_eq!(
            notifications.try_recv().expect("the notification"),
            json!({
                "jsonrpc": "2.0",
                "method": "textDocument/publishDiagnostics",
                "params": {"uri": "file:///a.rs", "diagnostics": [{"message": "no"}]},
            })
        );
        for (id, receiver) in waiting {
            assert!(
                receiver.try_recv().is_err(),
                "a notification must not answer the request {id}"
            );
        }
    }

    #[test]
    fn a_server_request_is_answered_and_a_workspace_edit_is_refused() {
        let (mut router, written, _waiting, _notifications, _acked) = router();
        router
            .route(json!({
                "jsonrpc": "2.0",
                "id": 900,
                "method": "workspace/configuration",
                "params": {"items": [{}, {}, {}]},
            }))
            .expect("routed");
        router
            .route(json!({
                "jsonrpc": "2.0",
                "id": 901,
                "method": "workspace/applyEdit",
                "params": {"edit": {"changes": {}}},
            }))
            .expect("routed");
        router
            .route(json!({
                "jsonrpc": "2.0",
                "id": 902,
                "method": "workspace/executeCommand",
                "params": {"command": "run"},
            }))
            .expect("routed");
        router
            .route(json!({
                "jsonrpc": "2.0",
                "id": 903,
                "method": "window/workDoneProgress/create",
                "params": {"token": "t"},
            }))
            .expect("routed");
        let sent = String::from_utf8(written.lock().expect("written").clone()).expect("UTF-8");
        assert!(sent.contains("\"result\":[null,null,null]"), "{sent}");
        assert!(
            sent.contains("applies no edit and runs no command"),
            "{sent}"
        );
        assert!(
            sent.matches("\"code\":-32601").count() == 2,
            "both the edit and the command are refused, and only they: {sent}"
        );
        assert!(
            sent.contains("\"id\":903"),
            "work done progress is answered: {sent}"
        );
    }

    #[test]
    fn a_message_that_is_not_json_rpc_is_refused_not_swallowed() {
        let (mut router, _writer, _waiting, _notifications, _acked) = router();
        for message in [
            json!({"jsonrpc": "2.0"}),
            json!({"id": 1}),
            json!({"jsonrpc": "2.0", "id": 1}),
            json!({"jsonrpc": "2.0", "id": 1, "result": "a", "error": {"code": 1}}),
            json!({"jsonrpc": "2.0", "id": 1, "method": "initialize", "result": "a"}),
            json!([1, 2, 3]),
            json!("text"),
        ] {
            assert!(
                router.route(message.clone()).is_err(),
                "{message} is refused"
            );
        }
    }

    #[test]
    fn an_initialize_is_answered_and_a_shutdown_is_then_graceful() {
        let (_guard, root) = crate::test_support::temporary_directory(SUITE, "lifecycle");
        let answered = Arc::new(AtomicBool::new(false));
        let exited = Arc::new(AtomicBool::new(false));
        let outcome = on_ntex({
            let answered = answered.clone();
            let exited = exited.clone();
            let root = root.clone();
            async move {
                let client = attached(&root, move |out, input| {
                    serve(input, out, |out, message| {
                        let (method, id) = request(&message);
                        if method == "exit" {
                            exited.store(true, Ordering::Relaxed);
                            return;
                        }
                        let Some(id) = id else {
                            return;
                        };
                        answer(
                            out,
                            json!({"jsonrpc": "2.0", "id": id, "result": {"method": method}}),
                        );
                        if method == "shutdown" {
                            answered.store(true, Ordering::Relaxed);
                        }
                    });
                });
                client
                    .initialize(Duration::from_secs(30))
                    .await
                    .expect("initialize");
                let answer = client
                    .request(
                        "textDocument/hover",
                        json!({"textDocument": {"uri": "file:///a.rs"}}),
                        Duration::from_secs(10),
                    )
                    .await
                    .expect("hover");
                assert_eq!(answer, json!({"method": "textDocument/hover"}));
                client.shutdown(Duration::from_secs(10)).await
            }
        });
        outcome.expect("a graceful shutdown");
        assert!(
            answered.load(Ordering::Relaxed),
            "the server was asked to shut down"
        );
        assert!(
            until(|| exited.load(Ordering::Relaxed)),
            "the server was told to exit"
        );
    }

    #[test]
    fn a_pushed_diagnostic_answers_nothing_and_is_read_as_a_notification() {
        let (_guard, root) = crate::test_support::temporary_directory(SUITE, "notification");
        let pushed = on_ntex({
            let root = root.clone();
            async move {
                let client = attached(&root, |out, input| {
                    serve(input, out, |out, message| {
                        let (method, id) = request(&message);
                        if let Some(id) = id {
                            answer(out, json!({"jsonrpc": "2.0", "id": id, "result": null}));
                        }
                        if method == "initialized" {
                            // Asked of the client, unasked for, before the
                            // diagnostic: neither may hang the other.
                            answer(
                                out,
                                json!({
                                    "jsonrpc": "2.0",
                                    "id": 900,
                                    "method": "workspace/workspaceFolders",
                                    "params": {},
                                }),
                            );
                            answer(
                                out,
                                json!({
                                    "jsonrpc": "2.0",
                                    "method": "textDocument/publishDiagnostics",
                                    "params": {
                                        "uri": "file:///a.rs",
                                        "diagnostics": [{"severity": 1, "message": "no"}],
                                    },
                                }),
                            );
                        }
                    });
                });
                client
                    .initialize(Duration::from_secs(30))
                    .await
                    .expect("initialize");
                client
                    .notification("textDocument/publishDiagnostics", Duration::from_secs(10))
                    .await
            }
        });
        let pushed = pushed.expect("the diagnostic arrived");
        assert_eq!(
            pushed["params"]["uri"],
            json!("file:///a.rs"),
            "a notification is routed as a notification: {pushed}"
        );
    }

    #[test]
    fn a_request_a_server_never_answers_times_out_on_its_own_limit() {
        let (_guard, root) = crate::test_support::temporary_directory(SUITE, "silent");
        let started = std::time::Instant::now();
        let outcome = on_ntex({
            let root = root.clone();
            async move {
                let client = attached(&root, |_out, input| {
                    // Reads everything OCG sends and answers nothing.
                    serve(input, _out, |_, _| {});
                });
                client
                    .request("textDocument/hover", json!({}), Duration::from_millis(300))
                    .await
            }
        });
        assert!(
            matches!(outcome, Err(LspError::TimedOut { ref method, .. }) if method == "textDocument/hover"),
            "the request times out on its own limit: {outcome:?}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "a session that is dropped after a timeout finishes promptly"
        );
    }

    #[test]
    fn a_server_that_leaves_before_anything_is_answered_fails_the_call() {
        let (_guard, root) = crate::test_support::temporary_directory(SUITE, "gone");
        let outcome = on_ntex({
            let root = root.clone();
            async move {
                let client = attached(&root, |_client, _server| {});
                client.initialize(Duration::from_secs(10)).await
            }
        });
        assert!(outcome.is_err(), "a server that leaves cannot initialize");
    }

    #[test]
    fn a_server_whose_output_is_not_framed_fails_the_call_rather_than_hanging() {
        let (_guard, root) = crate::test_support::temporary_directory(SUITE, "garbled");
        let outcome = on_ntex({
            let root = root.clone();
            async move {
                let client = attached(&root, |out, input| {
                    serve(input, out, |_, _| {});
                    out.write_all(b"this is not a framed message\r\n").ok();
                });
                client.initialize(Duration::from_secs(10)).await
            }
        });
        assert!(outcome.is_err(), "unframed output is a protocol failure");
    }

    #[test]
    fn a_real_process_that_answers_nothing_is_stopped_and_reaped_when_its_session_ends() {
        let (_guard, root) = crate::test_support::temporary_directory(SUITE, "process");
        let script = root.join("silent-server");
        std::fs::write(
            &script,
            "#!/bin/sh\necho $$ > \"$OCG_LSP_TEST_PID\"\ncat > /dev/null\n",
        )
        .expect("write the script");
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755))
            .expect("make it executable");
        let pid_file = root.join("server.pid");
        let launch = Launch {
            program: script,
            arguments: Vec::new(),
            root: root.clone(),
            variables: vec![(
                "OCG_LSP_TEST_PID".to_owned(),
                pid_file.display().to_string(),
            )],
        };
        let started = std::time::Instant::now();
        let outcome = on_ntex(async move {
            let client = Client::start(&launch).expect("start the process");
            let timed_out = client.initialize(Duration::from_millis(500)).await;
            let pid = pid_of(&pid_file);
            // Dropping the session is what has to stop and reap it.
            drop(client);
            (timed_out, pid)
        });
        let (timed_out, pid) = outcome;
        assert!(
            timed_out.is_err(),
            "a process that answers nothing cannot initialize"
        );
        let pid = pid.expect("the process recorded its own id");
        assert!(
            until(|| rustix::process::test_kill_process(pid) == Err(Errno::SRCH)),
            "the server process is gone once its session ended"
        );
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "the session ended promptly"
        );
    }

    /// The process id the script recorded, once it has.
    #[test]
    fn dropped_request_forgets_its_pending_id() {
        let (_guard, root) = crate::test_support::temporary_directory(SUITE, "cancelled-request");
        on_ntex(async move {
            let (socket, peer) = UnixStream::pair().expect("pipes");
            let client =
                Client::attach(Box::new(socket.try_clone().expect("writer")), socket, root);
            let result = ntex::util::select(
                Box::pin(client.request("textDocument/hover", json!({}), Duration::from_secs(30))),
                ntex::time::sleep(ntex::time::Millis(10)),
            )
            .await;
            assert!(matches!(result, ntex::util::Either::Right(_)));
            assert!(client.pending.lock().expect("pending").is_empty());
            drop(client);
            drop(peer);
        });
    }

    #[test]
    fn notification_overflow_fails_closed_without_growing_the_queue() {
        let (mut router, _, _, received, _) = router();
        for index in 0..32 {
            router.route(json!({"jsonrpc":"2.0","method":"textDocument/publishDiagnostics","params":{"index":index}})).expect("bounded notification");
        }
        assert_eq!(received.len(), 32);
        assert_eq!(
            router.route(
                json!({"jsonrpc":"2.0","method":"textDocument/publishDiagnostics","params":{}})
            ),
            Err(ProtocolError::QueueFull)
        );
        assert_eq!(received.len(), 32);
    }

    #[test]
    fn descendants_are_killed_even_when_the_server_leader_already_exited() {
        let (_guard, root) = crate::test_support::temporary_directory(SUITE, "descendants");
        let script = root.join("server");
        std::fs::write(
            &script,
            "#!/bin/sh\nsleep 30 &\necho $! > child.pid\nexit 0\n",
        )
        .expect("script");
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755))
            .expect("executable");
        let client = Client::start(&Launch {
            program: script,
            arguments: vec![],
            root: root.clone(),
            variables: vec![("PATH".to_owned(), "/usr/bin:/bin".to_owned())],
        })
        .expect("start");
        let descendant = pid_of(&root.join("child.pid")).expect("descendant pid");
        drop(client);
        assert!(
            until(|| rustix::process::test_kill_process(descendant) == Err(Errno::SRCH)),
            "descendant must not outlive its session"
        );
    }

    fn pid_of(path: &Path) -> Option<Pid> {
        if !until(|| path.exists()) {
            return None;
        }
        let text = std::fs::read_to_string(path).expect("read the recorded id");
        Some(Pid::from_raw(text.trim().parse().expect("a process id")).expect("a positive pid"))
    }

    /// Whether `condition` holds, checked rather than timed, within 10s.
    fn until(condition: impl Fn() -> bool) -> bool {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            if condition() {
                return true;
            }
            if std::time::Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// Runs one future on an executor of its own, which is what the tool loop
    /// does with a handler.
    fn on_ntex<F: std::future::Future + 'static>(future: F) -> F::Output
    where
        F::Output: 'static,
    {
        ntex::rt::System::new("lsp-test", ntex::rt::DefaultRuntime).block_on(future)
    }
}
