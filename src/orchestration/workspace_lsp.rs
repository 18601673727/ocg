//! Semantic Workspace queries through rust-analyzer and vtsls. Servers are
//! discovered from the operator's absolute PATH entries or explicit executable
//! overrides, and start lazily only for an authorized local coding execution.
//! Queries reuse at most one server per language. Native servers are not
//! sandboxed: they run with the same trust boundary as Workspace commands.
//! Syntax-only Tree-sitter results are a separate tool, never an LSP fallback.

use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

use crate::orchestration::lsp_client::{Client, Launch, LspError};
use crate::orchestration::tools::{
    ToolFailure, ToolFuture, ToolHandler, ToolRegistry, ToolRegistryError,
};
use crate::orchestration::workspace_read::{self, Refusal as PathRefusal, Workspace};
use crate::persistence::ToolEffect;

/// The name the tool is offered and recorded under.
pub const TOOL_NAME: &str = "workspace_lsp_query";

/// Identifies this handler's behavior in every call it records.
const HANDLER_VERSION: &str = "workspace_lsp_query-2";
const MAX_OPEN_DOCUMENTS: usize = 16;

/// The rust-analyzer executable for `.rs` files, an absolute path.
pub const RUST_ANALYZER_VARIABLE: &str = "OCG_LSP_RUST_ANALYZER";

/// The vtsls executable for TypeScript and JavaScript files, an absolute path
/// to `@vtsls/language-server`'s `vtsls`, which speaks the protocol over
/// `--stdio`.
pub const VTSLS_VARIABLE: &str = "OCG_LSP_VTSLS";

/// The largest source file this capability will show a server. A larger one
/// is refused; it is not truncated into an answer that means something else.
pub const MAX_DOCUMENT_BYTES: usize = 1024 * 1024;

/// The most diagnostics one call returns, and the most characters of one
/// message. Both are bounds, never a truncation of the truth: the count the
/// server reported is returned with them.
const MAX_DIAGNOSTICS: usize = 20;
const MAX_MESSAGE_CHARS: usize = 300;

/// The most characters of hover text one call returns.
const MAX_HOVER_CHARS: usize = 2_000;

/// The most locations one `definition` call returns.
const MAX_LOCATIONS: usize = 10;

/// What the serialized result of one call may weigh. The tool loop has
/// its own limit; this is the tool's, so a result is bounded before it is
/// ever handed to the loop.
const RESULT_BYTES: usize = 12 * 1024;

/// How long `initialize` may take. A server's first start indexes the
/// project, which is slower than anything else here.
const INITIALIZE_TIMEOUT: Duration = Duration::from_secs(60);

/// How long one `textDocument` request may take, when a `Session` does not
/// say otherwise.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);

/// How long one call waits for a `publishDiagnostics` notification to arrive.
/// Diagnostics are pushed when the server has finished analyzing, not when
/// it was asked, so this is a wait and not a request; when it passes, the
/// result says so instead of reporting no diagnostics there are none.
const DIAGNOSTICS_WAIT: Duration = Duration::from_secs(30);

/// The environment variables a server process is given: what it needs to find
/// its own toolchain and to run, and nothing else. OCG's own environment is
/// cleared first, so a Provider's credential — or anything else that was
/// only in OCG's environment — cannot reach a server process.
const PASSED_VARIABLES: &[&str] = &[
    "PATH",
    "HOME",
    "TMPDIR",
    "TMP",
    "TEMP",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "RUSTUP_HOME",
    "CARGO_HOME",
    "RUSTUP_TOOLCHAIN",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
    "NO_COLOR",
];

fn variable(name: &'static str) -> Option<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

/// The language servers this capability may use, as the operator named them.
/// A language with no configured server is refused, never served by some
/// other executable.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Servers {
    rust_analyzer: Option<PathBuf>,
    vtsls: Option<PathBuf>,
}

impl Servers {
    /// Discover installed servers, honoring explicit executable overrides.
    pub fn from_environment() -> Self {
        Self {
            rust_analyzer: discover(RUST_ANALYZER_VARIABLE, "rust-analyzer"),
            vtsls: discover(VTSLS_VARIABLE, "vtsls"),
        }
    }

    /// Whether any server at all is configured.
    pub fn any(&self) -> bool {
        self.rust_analyzer.is_some() || self.vtsls.is_some()
    }

    fn program(&self, language: Language) -> Option<&Path> {
        match language {
            Language::Rust => self.rust_analyzer.as_deref(),
            Language::TypeScript => self.vtsls.as_deref(),
        }
    }

    /// How `language` is started in `root`, or `None` when the operator named
    /// no server for it. The executable is the operator's; the arguments, the
    /// working directory and the environment are this module's, and nothing a
    /// Provider sends reaches any of them.
    fn launch(&self, language: Language, root: &Path) -> Option<Launch> {
        let program = self.program(language)?.to_path_buf();
        Some(Launch {
            program,
            arguments: match language {
                // rust-analyzer speaks the protocol over stdin and stdout
                // when it is given no transport argument.
                Language::Rust => Vec::new(),
                Language::TypeScript => vec!["--stdio".to_owned()],
            },
            root: root.to_path_buf(),
            variables: passed_variables(),
        })
    }
}

fn discover(variable_name: &'static str, program: &str) -> Option<PathBuf> {
    let executable = |path: &Path| {
        path.is_absolute()
            && std::fs::metadata(path).is_ok_and(|metadata| {
                metadata.is_file() && metadata.permissions().mode() & 0o111 != 0
            })
    };
    if let Some(configured) = variable(variable_name) {
        let path = PathBuf::from(configured);
        return executable(&path).then_some(path);
    }
    std::env::var_os("PATH")
        .into_iter()
        .flat_map(|value| std::env::split_paths(&value).collect::<Vec<_>>())
        .filter(|path| path.is_absolute())
        .map(|path| path.join(program))
        .find(|path| executable(path))
}

/// The environment of a server process: an allowlist, read from OCG's own.
fn passed_variables() -> Vec<(String, String)> {
    let mut variables = PASSED_VARIABLES
        .iter()
        .filter_map(|name| {
            std::env::var(name)
                .ok()
                .map(|value| ((*name).to_owned(), value))
        })
        .collect::<Vec<_>>();
    for (name, value) in &mut variables {
        if name == "PATH" {
            *value = std::env::join_paths(
                std::env::split_paths(value).filter(|path| path.is_absolute()),
            )
            .ok()
            .and_then(|value| value.into_string().ok())
            .unwrap_or_default();
        }
    }
    variables.push(("CARGO_NET_OFFLINE".to_owned(), "true".to_owned()));
    variables.push(("RUSTUP_AUTO_INSTALL".to_owned(), "0".to_owned()));
    variables
}

/// The source languages this capability serves, and the server each is asked
/// for. The extension decides, and nothing else: a call cannot name a server.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Language {
    Rust,
    TypeScript,
}

impl Language {
    fn of(path: &str) -> Option<Self> {
        let extension = Path::new(path)
            .extension()
            .and_then(OsStr::to_str)?
            .to_ascii_lowercase();
        match extension.as_str() {
            "rs" => Some(Self::Rust),
            "ts" | "tsx" | "js" | "jsx" => Some(Self::TypeScript),
            _ => None,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Rust => "rust",
            Self::TypeScript => "typescript",
        }
    }

    /// The server OCG names in results: the identity of what answered, so a
    /// result can never be read as if another server had produced it.
    fn server(self, servers: &Servers) -> Option<&'static str> {
        match (self, servers.program(self)) {
            (Self::Rust, Some(_)) => Some("rust-analyzer"),
            (Self::TypeScript, Some(_)) => Some("vtsls"),
            _ => None,
        }
    }

    /// The `languageId` of an opened document, from its extension.
    fn identifier(self, path: &str) -> &'static str {
        let extension = Path::new(path)
            .extension()
            .and_then(OsStr::to_str)
            .unwrap_or_default()
            .to_ascii_lowercase();
        match (self, extension.as_str()) {
            (Self::TypeScript, "ts") => "typescript",
            (Self::TypeScript, "tsx") => "typescriptreact",
            (Self::TypeScript, "js") => "javascript",
            (Self::TypeScript, "jsx") => "javascriptreact",
            _ => "rust",
        }
    }
}

/// What one call asks the server.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Operation {
    Diagnostics,
    Definition,
    Hover,
}

impl Operation {
    fn of(name: &str) -> Option<Self> {
        match name {
            "diagnostics" => Some(Self::Diagnostics),
            "definition" => Some(Self::Definition),
            "hover" => Some(Self::Hover),
            _ => None,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Diagnostics => "diagnostics",
            Self::Definition => "definition",
            Self::Hover => "hover",
        }
    }

    /// Whether the operation needs a position. `diagnostics` is about the
    /// whole document, so a position with it is a mistake worth refusing.
    fn needs_position(self) -> bool {
        self != Self::Diagnostics
    }
}

/// Why a call asked nothing. Its message is recorded and may reach the
/// Provider, so it never repeats the file's contents.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Refusal {
    #[error("only .rs, .ts, .tsx, .js and .jsx files are served by the language servers")]
    UnsupportedLanguage,
    #[error("the operation must be `diagnostics`, `definition` or `hover`")]
    UnknownOperation,
    #[error("{0} is unavailable: no usable server is configured or this session failed")]
    NoServer(&'static str),
    #[error("{0} is busy with another semantic query in this execution")]
    Busy(&'static str),
    #[error("`{0}` needs `line` and `character`, counted from 1")]
    PositionRequired(&'static str),
    #[error("`line` and `character` mean nothing for `diagnostics`")]
    PositionUnexpected,
    #[error("the position is outside the file")]
    PositionOutside,
    #[error("the file is larger than {MAX_DOCUMENT_BYTES} bytes")]
    DocumentTooLarge,
    #[error(transparent)]
    Path(#[from] PathRefusal),
}

/// Why a call failed. A [`Refusal`] is a decision OCG made before a server
/// was started; anything else happened with a server running.
#[derive(Debug, thiserror::Error)]
enum CallError {
    #[error(transparent)]
    Refused(#[from] Refusal),
    #[error(transparent)]
    Server(#[from] LspError),
}

/// The parameters a call must match exactly: a path, an operation, and the
/// position the operations that need one name.
fn parameters() -> Value {
    json!({
        "type": "object",
        "properties": {
            "path": {
                "type": "string",
                "description": "A path relative to the authorized Workspace root"
            },
            "operation": {
                "type": "string",
                "enum": ["diagnostics", "definition", "hover"],
                "description": "What to ask the language server"
            },
            "line": {
                "type": "integer",
                "minimum": 1,
                "description": "The line of the position, counted from 1; required for `definition` and `hover`"
            },
            "character": {
                "type": "integer",
                "minimum": 1,
                "description": "The UTF-16 code unit within `line` of the position, counted from 1"
            }
        },
        "required": ["path", "operation"],
        "additionalProperties": false
    })
}

/// The handler of `workspace_lsp_query`, bound to one authorized Workspace and
/// to the servers its Job's execution may start there.
struct QueryLsp {
    workspace: Workspace,
    servers: Servers,
    sessions: Arc<Sessions>,
}

impl ToolHandler for QueryLsp {
    fn version(&self) -> &str {
        HANDLER_VERSION
    }

    /// Not [`ToolEffect::ReadOnly`]. OCG itself writes nothing, but it starts
    /// a process that runs project code — Cargo through rust-analyzer, plugins
    /// through vtsls — and that is not something running it again cannot
    /// repeat. A failure after a server started says so.
    fn effect(&self) -> ToolEffect {
        ToolEffect::NonIdempotent
    }

    /// Asks the one question, yielding while the server works, so the
    /// execution's own time limit and cancellation can stop the call.
    fn call(&self, arguments: Value) -> ToolFuture<'_> {
        Box::pin(async move {
            let arguments = Request::of(&arguments)?;
            query(&self.workspace, &self.sessions, &self.servers, &arguments).await
        })
    }
}

/// A call's arguments as this tool reads them, after the schema admitted them.
/// They are read twice on purpose: the schema's job is to refuse a Provider's
/// mistake, and this handler's is not to be the only thing standing between a
/// call and a server.
struct Request<'a> {
    path: &'a str,
    operation: Operation,
    line: Option<u64>,
    character: Option<u64>,
}

impl<'a> Request<'a> {
    fn of(arguments: &'a Value) -> Result<Self, ToolFailure> {
        let refused = |refusal: Refusal| ToolFailure::not_applied(refusal.to_string());
        let Some(path) = arguments.get("path").and_then(Value::as_str) else {
            return Err(refused(Refusal::Path(PathRefusal::InvalidPath)));
        };
        let Some(operation) = arguments
            .get("operation")
            .and_then(Value::as_str)
            .and_then(Operation::of)
        else {
            return Err(refused(Refusal::UnknownOperation));
        };
        let number = |name: &str| match arguments.get(name) {
            None => Ok(None),
            Some(value) => value.as_u64().map(Some).ok_or(Refusal::PositionOutside),
        };
        let line = number("line").map_err(refused)?;
        let character = number("character").map_err(refused)?;
        Ok(Self {
            path,
            operation,
            line,
            character,
        })
    }
}

/// One language server's session within one Job execution: the running client,
/// the documents already shown to it, and the timeouts that bound both.
struct Session {
    name: &'static str,
    language: Language,
    launch: Option<Launch>,
    client: Mutex<Option<Arc<Client>>>,
    opened: Mutex<BTreeMap<String, (Vec<u8>, i64)>>,
    busy: AtomicBool,
    disabled: AtomicBool,
    limits: Limits,
}

/// The timeouts of one session.
#[derive(Clone, Copy, Debug)]
struct Limits {
    initialize: Duration,
    request: Duration,
    diagnostics: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            initialize: INITIALIZE_TIMEOUT,
            request: REQUEST_TIMEOUT,
            diagnostics: DIAGNOSTICS_WAIT,
        }
    }
}

impl Session {
    /// A session starts once. A failed stream is not silently restarted.
    async fn client(&self) -> Result<Arc<Client>, CallError> {
        let running = self
            .client
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        if let Some(client) = running {
            if client.alive() {
                return Ok(client);
            }
            return Err(LspError::Protocol(
                crate::orchestration::lsp_client::ProtocolError::Closed,
            )
            .into());
        }
        let launch = self
            .launch
            .as_ref()
            .ok_or(CallError::Refused(Refusal::NoServer(self.name)))?;
        let client = Client::start(launch)?;
        client.initialize(self.limits.initialize).await?;
        let previous = self
            .client
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .replace(client.clone());
        drop(previous);
        // A new server knows nothing, so nothing may be withheld from it on
        // the grounds that some other server was told about it.
        self.opened
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
        Ok(client)
    }

    fn open(&self, client: &Client, uri: &str, path: &str, text: &str) -> Result<i64, CallError> {
        let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, text.as_bytes())
            .as_ref()
            .to_vec();
        let mut opened = self.opened.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some((previous, version)) = opened.get(uri) {
            if *previous == digest {
                return Ok(*version);
            }
            let next = *version + 1;
            client.clear_notifications();
            client.notify("textDocument/didChange", json!({
                "textDocument": {"uri": uri, "version": next}, "contentChanges": [{"text": text}]
            }))?;
            // These bytes were read from the authorized on-disk file. A save
            // notification also triggers rust-analyzer's semantic cargo check.
            client.notify("textDocument/didSave", json!({"textDocument":{"uri":uri}}))?;
            opened.insert(uri.to_owned(), (digest, next));
            return Ok(next);
        }
        if opened.len() >= MAX_OPEN_DOCUMENTS
            && let Some(oldest) = opened.keys().next().cloned()
        {
            client.notify(
                "textDocument/didClose",
                json!({"textDocument": {"uri": oldest}}),
            )?;
            opened.remove(&oldest);
        }
        client.clear_notifications();
        client.notify(
            "textDocument/didOpen",
            json!({"textDocument": {
                "uri": uri, "languageId": self.language.identifier(path), "version": 1, "text": text
            }}),
        )?;
        opened.insert(uri.to_owned(), (digest, 1));
        Ok(1)
    }

    fn stop(&self) {
        self.disabled.store(true, Ordering::Release);
        let client = self
            .client
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(client) = client {
            client.terminate();
        }
    }
}

/// The sessions of one Job execution: at most one per language, reused across
/// that execution's calls and dropped with it.
struct Sessions {
    rust: Session,
    typescript: Session,
}

impl Sessions {
    fn new(workspace: &Workspace, servers: &Servers, limits: Limits) -> Self {
        let session = |name: &'static str, language: Language| Session {
            name,
            language,
            launch: servers.launch(language, workspace.root()),
            client: Mutex::new(None),
            opened: Mutex::new(BTreeMap::new()),
            busy: AtomicBool::new(false),
            disabled: AtomicBool::new(false),
            limits,
        };
        Self {
            rust: session("rust-analyzer", Language::Rust),
            typescript: session("vtsls", Language::TypeScript),
        }
    }

    fn of(&self, language: Language) -> &Session {
        match language {
            Language::Rust => &self.rust,
            Language::TypeScript => &self.typescript,
        }
    }

    /// Stops every running server through the protocol's own `shutdown` and
    /// `exit`, and reaps them. The tool loop has no end-of-execution hook to
    /// call this from, so in an execution this is not what stops a server:
    /// dropping `Sessions` is, and it sends the same `shutdown` and `exit`
    /// before killing the group. This is that path said out loud, so a test
    /// can watch a session end the graceful way.
    #[cfg(test)]
    async fn shutdown(&self, limit: Duration) {
        for session in [&self.rust, &self.typescript] {
            let running = session
                .client
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
            if let Some(client) = running {
                client.shutdown(limit).await.expect("server shutdown");
            }
        }
    }
}

struct SessionUse<'a> {
    session: &'a Session,
    complete: bool,
}
impl Drop for SessionUse<'_> {
    fn drop(&mut self) {
        if !self.complete {
            self.session.stop();
        }
        self.session.busy.store(false, Ordering::Release);
    }
}

/// Asks the server the one question and renders what it said.
async fn query(
    workspace: &Workspace,
    sessions: &Sessions,
    servers: &Servers,
    request: &Request<'_>,
) -> Result<Value, ToolFailure> {
    match query_once(workspace, sessions, servers, request).await {
        Ok(value) => Ok(value),
        // Settled before a server was started, so this call left nothing
        // behind and may say so.
        Err(CallError::Refused(refusal)) => Err(ToolFailure::not_applied(refusal.to_string())),
        // A server was started, and a server runs project code, so this call
        // cannot claim to have left nothing behind.
        Err(CallError::Server(problem)) => Err(ToolFailure::uncertain(format!(
            "language server unavailable: {problem}"
        ))),
    }
}

async fn query_once(
    workspace: &Workspace,
    sessions: &Sessions,
    servers: &Servers,
    request: &Request<'_>,
) -> Result<Value, CallError> {
    let refused = CallError::Refused;
    // Everything a Provider can get wrong is settled here, before a server
    // is started, so a refusal is known to have had no effect at all.
    let language = Language::of(request.path).ok_or(refused(Refusal::UnsupportedLanguage))?;
    let session = sessions.of(language);
    if language.server(servers).is_none() {
        return Err(refused(Refusal::NoServer(session.name)));
    }
    let position = match (
        request.operation.needs_position(),
        request.line,
        request.character,
    ) {
        (true, Some(line), Some(character)) => Some((line, character)),
        (true, _, _) => return Err(refused(Refusal::PositionRequired(request.operation.name()))),
        (false, None, None) => None,
        (false, _, _) => return Err(refused(Refusal::PositionUnexpected)),
    };
    let absolute = absolute_path(workspace, request.path)?;
    let text = read_document(workspace, request.path)?;
    let at = position.map(|(line, character)| position_of(&text, line, character));
    let at = match at {
        Some(attempt) => Some(attempt?),
        None => None,
    };

    if session.disabled.load(Ordering::Acquire) {
        return Err(refused(Refusal::NoServer(session.name)));
    }
    if session
        .busy
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return Err(refused(Refusal::Busy(session.name)));
    }
    let mut usage = SessionUse {
        session,
        complete: false,
    };
    let uri = file_uri(&absolute);
    let client = session.client().await?;
    let version = session.open(&client, &uri, request.path, &text)?;
    let mut value = json!({
        "server": session.name,
        "language": language.name(),
        "operation": request.operation.name(),
        "path": request.path,
        "analysis": "language_server",
        "document_version": version,
    });
    match request.operation {
        Operation::Diagnostics => {
            let (items, complete) =
                diagnostics(&client, &uri, version, session.limits.diagnostics).await?;
            value["diagnostics"] = items;
            value["diagnostics_complete"] = json!(complete);
            value["available"] = json!(true);
            if !complete {
                value["note"] = json!(
                    "the server had not reported finished analyzing before the wait passed; \
                     an empty list here is not a claim that the file is clean"
                );
            }
        }
        Operation::Definition => {
            let found = definition(
                &client,
                &uri,
                at.expect("a position"),
                session.limits.request,
                workspace.root(),
            )
            .await?;
            value["definition"] = found.clone();
            value["available"] = json!(!found.is_null());
            if found.is_null() {
                value["note"] = json!("the server reported no definition at that position");
            }
        }
        Operation::Hover => {
            let found = hover(
                &client,
                &uri,
                at.expect("a position"),
                session.limits.request,
            )
            .await?;
            value["hover"] = found.clone();
            value["available"] = json!(!found.is_null());
            if found.is_null() {
                value["note"] = json!("the server reported no hover at that position");
            }
        }
    }
    usage.complete = true;
    Ok(bounded(value))
}

/// The result as it may be handed to the tool loop: never larger than
/// [`RESULT_BYTES`]. A shortened string says so with an ellipsis; nothing is
/// ever dropped silently, and no diagnostic or symbol is invented to fill a
/// gap.
fn bounded(value: Value) -> Value {
    let mut value = value;
    for _ in 0..8 {
        if value.to_string().len() <= RESULT_BYTES {
            break;
        }
        if !cut(&mut value, 64) {
            break;
        }
    }
    value
}

/// Cuts every string of `value` longer than `keep` characters, with an
/// ellipsis. Returns whether anything was left to cut.
fn cut(value: &mut Value, keep: usize) -> bool {
    match value {
        Value::String(text) => {
            if text.chars().count() <= keep {
                return false;
            }
            *text = format!("{}\u{2026}", text.chars().take(keep).collect::<String>());
            true
        }
        Value::Array(items) => {
            let mut changed = false;
            for item in items {
                changed |= cut(item, keep);
            }
            changed
        }
        Value::Object(map) => {
            let mut changed = false;
            for item in map.values_mut() {
                changed |= cut(item, keep);
            }
            changed
        }
        _ => false,
    }
}

/// The absolute path of `path` in the Workspace: its checked components under
/// the authorized root, which was canonicalized when the operator authorized
/// it. The walk itself is [`Workspace::parent`]'s, so the path rules, the
/// refusal of links and of the OCG data directory, and the check that the
/// root is still the authorized directory all apply.
fn absolute_path(workspace: &Workspace, path: &str) -> Result<PathBuf, Refusal> {
    let components = workspace_read::components(path)?;
    Ok(workspace.root().join(components.join("/")))
}

/// The whole text of `path`, opened and read exactly as
/// [`crate::orchestration::workspace_read`] opens and reads one: through every
/// directory before it without following a link, a regular file with a single
/// link, unchanged while it was read, and bounded by [`MAX_DOCUMENT_BYTES`].
fn read_document(workspace: &Workspace, path: &str) -> Result<String, Refusal> {
    let (directory, name) = workspace.parent(path)?;
    let too_large = |refusal: PathRefusal| match refusal {
        PathRefusal::TooLarge => Refusal::DocumentTooLarge,
        other => Refusal::Path(other),
    };
    let file = workspace_read::open_within(
        &directory,
        name,
        workspace_read::Step::File,
        MAX_DOCUMENT_BYTES,
    )
    .map_err(too_large)?;
    let content = workspace_read::read_bytes(&file, MAX_DOCUMENT_BYTES).map_err(too_large)?;
    let text = String::from_utf8(content).map_err(|_| PathRefusal::NotText)?;
    if text.contains('\0') {
        return Err(PathRefusal::NotText.into());
    }
    Ok(text)
}

/// The protocol position of `(line, character)`, both counted from 1 and
/// `character` counted in UTF-16 code units. A position past the end of its
/// line is refused, not clamped: a clamped position would silently ask about
/// somewhere else.
fn position_of(text: &str, line: u64, character: u64) -> Result<(u64, u64), Refusal> {
    let zero_based = line.checked_sub(1).ok_or(Refusal::PositionOutside)?;
    // A final newline ends the last line rather than starting an empty one,
    // so the lines here are the ones `workspace_read_file` counts too.
    let mut lines = text.split('\n').collect::<Vec<_>>();
    if text.ends_with('\n') {
        lines.pop();
    }
    let line_text = *lines
        .get(zero_based as usize)
        .ok_or(Refusal::PositionOutside)?;
    let units: usize = line_text.chars().map(char::len_utf16).sum();
    let offset = usize::try_from(character.checked_sub(1).ok_or(Refusal::PositionOutside)?)
        .map_err(|_| Refusal::PositionOutside)?;
    if offset > units {
        return Err(Refusal::PositionOutside);
    }
    Ok((
        zero_based,
        u64::try_from(offset).map_err(|_| Refusal::PositionOutside)?,
    ))
}

/// A `file://` URI for an absolute path, every byte outside the unreserved set
/// and `/` percent-encoded, from the path's own bytes rather than its lossy
/// text form.
pub(crate) fn file_uri(path: &Path) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut uri = String::from("file://");
    for byte in path.as_os_str().as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' | b'/' => {
                uri.push(char::from(*byte));
            }
            _ => {
                uri.push('%');
                uri.push(char::from(HEX[usize::from(byte >> 4)]));
                uri.push(char::from(HEX[usize::from(byte & 0x0f)]));
            }
        }
    }
    uri
}

/// The path a `file://` URI names, or `None` for any other scheme or a URI
/// this cannot decode. Only ever used to *compare* a server's answer with the
/// authorized Workspace: the file is not opened, so naming it grants nothing.
fn path_of_uri(uri: &str) -> Option<PathBuf> {
    let rest = uri.strip_prefix("file://")?;
    let rest = rest.strip_prefix("localhost").unwrap_or(rest);
    if !rest.starts_with('/') {
        return None;
    }
    let bytes = rest.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != b'%' {
            decoded.push(bytes[index]);
            index += 1;
            continue;
        }
        decoded.push(digit(*bytes.get(index + 1)?)? << 4 | digit(*bytes.get(index + 2)?)?);
        index += 3;
    }
    Some(PathBuf::from(OsStr::from_bytes(&decoded).to_os_string()))
}

fn digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// A protocol range, reported in this tool's own convention: 1-based lines
/// and 1-based UTF-16 characters, so it can be passed straight back in.
fn range_value(range: Option<&Value>) -> Value {
    let point = |value: Option<&Value>| {
        let line = value
            .and_then(|value| value.get("line"))
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let character = value
            .and_then(|value| value.get("character"))
            .and_then(Value::as_u64)
            .unwrap_or(0);
        json!({"line": line + 1, "character": character + 1})
    };
    let Some(range) = range.filter(|range| range.is_object()) else {
        return Value::Null;
    };
    json!({"start": point(range.get("start")), "end": point(range.get("end"))})
}

/// The severity a diagnostic reports, as a name.
fn severity_name(severity: Option<&Value>) -> Value {
    match severity.and_then(Value::as_u64) {
        Some(1) => json!("error"),
        Some(2) => json!("warning"),
        Some(3) => json!("information"),
        Some(4) => json!("hint"),
        _ => Value::Null,
    }
}

/// Diagnostics published for the requested document version. Empty startup
/// publications are not proof that analysis has finished. A bounded wait
/// without a substantive publication is reported as incomplete.
async fn diagnostics(
    client: &Client,
    uri: &str,
    version: i64,
    limit: Duration,
) -> Result<(Value, bool), CallError> {
    let deadline = Instant::now() + limit;
    let mut latest = Vec::new();
    loop {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default();
        let Some(message) = client
            .notification("textDocument/publishDiagnostics", remaining)
            .await
        else {
            if !client.alive() {
                return Err(LspError::Protocol(
                    crate::orchestration::lsp_client::ProtocolError::Closed,
                )
                .into());
            }
            return Ok((diagnostics_value(&latest, MAX_DIAGNOSTICS), false));
        };
        let params = message.get("params").cloned().unwrap_or(Value::Null);
        if params.get("uri").and_then(Value::as_str) != Some(uri) {
            // Some other document's diagnostics; not this call's answer.
            continue;
        }
        match params.get("version").and_then(Value::as_i64) {
            Some(published) if published != version => continue,
            None if version > 1 => continue,
            _ => {}
        }
        latest = params
            .get("diagnostics")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        if !latest.is_empty() {
            return Ok((diagnostics_value(&latest, MAX_DIAGNOSTICS), true));
        }
    }
}

fn diagnostics_value(items: &[Value], kept: usize) -> Value {
    let total = items.len();
    let values = items
        .iter()
        .take(kept)
        .map(|item| {
            let message = item.get("message").and_then(Value::as_str).unwrap_or("");
            json!({
                "severity": severity_name(item.get("severity")),
                "code": item.get("code").cloned().unwrap_or(Value::Null),
                "source": item.get("source").and_then(Value::as_str).unwrap_or(""),
                "message": bounded_text(message, MAX_MESSAGE_CHARS),
                "range": range_value(item.get("range")),
            })
        })
        .collect::<Vec<_>>();
    json!({
        "items": values,
        "total": total,
        "truncated": total > kept,
    })
}

/// The location `textDocument/definition` named, as an object with the
/// Workspace-relative path when the file is inside the authorized Workspace.
/// A file outside it is named by its URI alone: OCG does not read it, and a
/// server naming it does not make it authorized.
async fn definition(
    client: &Client,
    uri: &str,
    (line, character): (u64, u64),
    limit: Duration,
    root: &Path,
) -> Result<Value, CallError> {
    let answer = client
        .request(
            "textDocument/definition",
            position_params(uri, line, character),
            limit,
        )
        .await?;
    Ok(locations_value(&answer, root))
}

fn locations_value(answer: &Value, root: &Path) -> Value {
    let locations = match answer {
        Value::Null => Vec::new(),
        Value::Array(items) => items.clone(),
        one => vec![one.clone()],
    };
    let values = locations
        .iter()
        .take(MAX_LOCATIONS)
        .map(|location| location_value(location, root))
        .collect::<Vec<_>>();
    if values.is_empty() {
        return Value::Null;
    }
    json!({"locations": values})
}

fn location_value(location: &Value, root: &Path) -> Value {
    // A server may answer with a Location, or with a LocationLink, whose URI
    // and range sit under their own names.
    let uri = location
        .get("uri")
        .or_else(|| location.get("targetUri"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let range = location
        .get("range")
        .or_else(|| location.get("targetSelectionRange"))
        .or_else(|| location.get("targetRange"));
    let mut value = json!({"uri": uri, "range": range_value(range)});
    let inside = path_of_uri(uri).is_some_and(|path| path.starts_with(root));
    if inside {
        let path = path_of_uri(uri).unwrap_or_default();
        value["path"] = json!(path.strip_prefix(root).unwrap_or(&path).to_string_lossy());
        value["inside_workspace"] = json!(true);
    } else {
        value["inside_workspace"] = json!(false);
        value["note"] =
            json!("the server named a file outside the authorized Workspace; OCG did not read it");
    }
    value
}

/// The hover text `textDocument/hover` returned, as plain text, or `null`.
async fn hover(
    client: &Client,
    uri: &str,
    (line, character): (u64, u64),
    limit: Duration,
) -> Result<Value, CallError> {
    let answer = client
        .request(
            "textDocument/hover",
            position_params(uri, line, character),
            limit,
        )
        .await?;
    let contents = answer.get("contents").cloned().unwrap_or(Value::Null);
    let Some(text) = hover_text(&contents) else {
        return Ok(Value::Null);
    };
    Ok(json!({
        "contents": bounded_text(&text, MAX_HOVER_CHARS),
        "range": range_value(answer.get("range")),
    }))
}

/// The text of a `MarkupContent`, a `MarkedString`, or a list of either.
fn hover_text(contents: &Value) -> Option<String> {
    let one = |value: &Value| match value {
        Value::String(text) => Some(text.clone()),
        Value::Object(_) => value
            .get("value")
            .and_then(Value::as_str)
            .map(str::to_owned),
        _ => None,
    };
    let text = match contents {
        Value::Array(items) => {
            let parts = items.iter().filter_map(one).collect::<Vec<_>>();
            (!parts.is_empty()).then(|| parts.join("\n\n"))?
        }
        other => one(other)?,
    };
    (!text.is_empty()).then_some(text)
}

fn position_params(uri: &str, line: u64, character: u64) -> Value {
    json!({
        "textDocument": {"uri": uri},
        "position": {"line": line, "character": character},
    })
}

fn bounded_text(value: &str, limit: usize) -> String {
    if value.chars().count() <= limit {
        return value.to_owned();
    }
    let mut cut = value.chars().take(limit).collect::<String>();
    cut.push('\u{2026}');
    cut
}

/// Registers a lazy language-server handler for one authorized coding execution.
pub(crate) fn register(
    registry: &ToolRegistry,
    workspace: Workspace,
    servers: Servers,
) -> Result<(), ToolRegistryError> {
    let sessions = Sessions::new(&workspace, &servers, Limits::default());
    registry.register(
        TOOL_NAME,
        Some(
            "Ask the language server of this Workspace about one source file: `rust-analyzer` for `.rs`, `vtsls` for `.ts`, `.tsx`, `.js` and `.jsx`. `diagnostics` returns what the server has found for the whole file; `definition` and `hover` ask about a position, given as `line` and `character` counted from 1, with `character` in UTF-16 code units. The server is started once per run and reused. It is a native process that runs project code with your permissions, outside any sandbox.",
        ),
        parameters(),
        Arc::new(QueryLsp {
            workspace,
            servers,
            sessions: Arc::new(sessions),
        }),
    )
}
#[cfg(test)]
mod tests {
    use crate::data_directory::DataDirectory;
    use crate::openai_compatible::CompletedToolCall;
    use crate::orchestration::tools::{Dispatch, FailureCertainty, ToolRegistry};
    use crate::test_support::{TestRoot, data_directory, temporary_directory};

    use super::*;

    const SUITE: &str = "ocg-workspace-lsp";

    /// A real server's first start indexes a project, which takes longer than
    /// anything a unit test would wait for.
    const REAL_TIMEOUT: Duration = Duration::from_secs(120);

    fn real_limits() -> Limits {
        Limits {
            initialize: REAL_TIMEOUT,
            request: REAL_TIMEOUT,
            diagnostics: REAL_TIMEOUT,
        }
    }

    /// The executable the operator named for rust-analyzer. A test that
    /// cannot find one fails loudly: it is never silently skipped.
    fn rust_analyzer() -> PathBuf {
        PathBuf::from(variable(RUST_ANALYZER_VARIABLE).unwrap_or_else(|| {
            panic!(
                "{RUST_ANALYZER_VARIABLE} must name a rust-analyzer executable for the real-server \
                 tests of this capability; they fail rather than skip"
            )
        }))
    }

    /// The executable the operator named for vtsls, read the same way.
    fn vtsls() -> PathBuf {
        PathBuf::from(variable(VTSLS_VARIABLE).unwrap_or_else(|| {
            panic!(
                "{VTSLS_VARIABLE} must name a vtsls executable for the real-server tests of this \
                 capability; they fail rather than skip"
            )
        }))
    }

    fn on_ntex<F: std::future::Future + 'static>(future: F) -> F::Output
    where
        F::Output: 'static,
    {
        ntex::rt::System::new("lsp-spike", ntex::rt::DefaultRuntime).block_on(future)
    }

    fn write(root: &Path, name: &str, content: &str) {
        let path = root.join(name);
        std::fs::create_dir_all(path.parent().expect("a parent")).expect("create a directory");
        std::fs::write(path, content).expect("write");
    }

    /// A Workspace and a data directory of its own, under the system temp
    /// dir, removed when dropped.
    fn workspace(name: &str) -> (TestRoot, PathBuf, TestRoot, DataDirectory) {
        let (guard, root) = temporary_directory(SUITE, &format!("{name}-workspace"));
        let (data_guard, directory) = data_directory(SUITE, name);
        (guard, root, data_guard, directory)
    }

    /// A tiny Rust project with one known diagnostic and one known symbol.
    fn rust_sample(root: &Path) {
        write(
            root,
            "Cargo.toml",
            "[package]\nname = \"ocg-lsp-sample\"\nversion = \"0.0.0\"\nedition = \"2021\"\n",
        );
        write(
            root,
            "src/lib.rs",
            concat!(
                "/// Adds two numbers. Grüße 😀\n",
                "pub fn add(left: i64, right: i64) -> i64 {\n",
                "    left + right\n",
                "}\n",
                "\n",
                "pub fn broken() -> i64 {\n",
                "    \"not a number\"\n",
                "}\n",
                "\n",
                "pub fn uses_add() -> i64 {\n",
                "    add(1, 2)\n",
                "}\n",
            ),
        );
    }

    /// A tiny TypeScript project with a local `tsconfig.json`, one known
    /// diagnostic and one known symbol.
    fn typescript_sample(root: &Path) {
        write(
            root,
            "tsconfig.json",
            concat!(
                "{\n",
                "  \"compilerOptions\": {\n",
                "    \"target\": \"ES2020\",\n",
                "    \"module\": \"commonjs\",\n",
                "    \"strict\": true,\n",
                "    \"noEmit\": true\n",
                "  },\n",
                "  \"include\": [\"src\"]\n",
                "}\n",
            ),
        );
        write(
            root,
            "src/index.ts",
            concat!(
                "export function greet(name: string): string {\n",
                "  return `héllo ${name}`;\n",
                "}\n",
                "\n",
                "// A deliberate type error.\n",
                "export const total: number = \"forty two\";\n",
                "\n",
                "export function useGreet(): string {\n",
                "  return greet(\"world\");\n",
                "}\n",
            ),
        );
    }

    /// A handler with its own sessions, as a call's would have, plus a
    /// handle on those sessions so a test can watch them end.
    fn handler(
        workspace: Workspace,
        servers: Servers,
        limits: Limits,
    ) -> (Arc<QueryLsp>, Arc<Sessions>) {
        let sessions = Arc::new(Sessions::new(&workspace, &servers, limits));
        (
            Arc::new(QueryLsp {
                workspace,
                servers,
                sessions: sessions.clone(),
            }),
            sessions,
        )
    }

    /// Runs one call as the tool loop does, on an executor of its own.
    fn call(handler: Arc<QueryLsp>, arguments: Value) -> Result<Value, ToolFailure> {
        on_ntex(async move { handler.call(arguments).await })
    }

    #[test]
    fn only_the_extensions_of_the_two_servers_are_served() {
        assert_eq!(Language::of("src/main.rs"), Some(Language::Rust));
        assert_eq!(Language::of("src/a/b/c.RS"), Some(Language::Rust));
        assert_eq!(Language::of("src/index.ts"), Some(Language::TypeScript));
        assert_eq!(Language::of("src/index.tsx"), Some(Language::TypeScript));
        assert_eq!(Language::of("src/index.js"), Some(Language::TypeScript));
        assert_eq!(Language::of("src/index.jsx"), Some(Language::TypeScript));
        for path in [
            "README.md",
            "Cargo.toml",
            "Makefile",
            "lib.rs.bak",
            "notes.py",
            "noextension",
            "",
            ".rs",
        ] {
            assert_eq!(Language::of(path), None, "{path:?} is not served");
        }
        assert_eq!(Language::Rust.name(), "rust");
        assert_eq!(Language::TypeScript.name(), "typescript");
        assert_eq!(Language::Rust.identifier("a.rs"), "rust");
        assert_eq!(Language::TypeScript.identifier("a.ts"), "typescript");
        assert_eq!(Language::TypeScript.identifier("a.tsx"), "typescriptreact");
        assert_eq!(Language::TypeScript.identifier("a.js"), "javascript");
        assert_eq!(Language::TypeScript.identifier("a.jsx"), "javascriptreact");
    }

    #[test]
    fn positions_count_from_one_in_utf16_code_units() {
        // Four characters before the caret, of which `😀` is two UTF-16 code
        // units: a byte offset, a character offset and a UTF-16 offset are
        // three different numbers, and only the last one is asked for.
        let text = "let 😀 = 1;\nlet x = 2;\n\nlast\n";
        assert_eq!(position_of(text, 1, 1), Ok((0, 0)));
        // `l`,`e`,`t`,` ` are four units, then the emoji is two.
        assert_eq!(position_of(text, 1, 5), Ok((0, 4)));
        assert_eq!(position_of(text, 1, 7), Ok((0, 6)));
        assert_eq!(position_of(text, 2, 9), Ok((1, 8)));
        // The end of a line is a position; past it is not.
        assert_eq!(position_of(text, 2, 10), Ok((1, 9)));
        // The end of a line is a position of its own.
        assert_eq!(position_of(text, 1, 12), Ok((0, 11)));
        assert_eq!(position_of(text, 2, 11), Ok((1, 10)));
        assert_eq!(position_of(text, 3, 1), Ok((2, 0)));
        assert_eq!(position_of(text, 4, 5), Ok((3, 4)));
        for (line, character) in [(0, 1), (1, 0), (2, 12), (5, 1), (1, 13)] {
            assert_eq!(
                position_of(text, line, character),
                Err(Refusal::PositionOutside),
                "({line}, {character}) is outside the file"
            );
        }
    }

    #[test]
    fn a_uri_is_a_path_and_back_without_guessing() {
        for path in [
            "/a/b.rs",
            "/a/b c.rs",
            "/a/üñî.rs",
            "/a/100%/x.rs",
            "/a/#?&=.ts",
            "/tmp/.hidden/x.rs",
        ] {
            let uri = file_uri(Path::new(path));
            assert!(uri.starts_with("file:///"), "{uri}");
            assert_eq!(path_of_uri(&uri).as_deref(), Some(Path::new(path)), "{uri}");
        }
        for uri in [
            "",
            "/a/b.rs",
            "https://example.com/a.rs",
            "file://relative",
            "file:///a/%zz.rs",
            "file:///a/%2.rs",
        ] {
            assert_eq!(path_of_uri(uri), None, "{uri:?} is not a local path");
        }
    }

    #[test]
    fn a_path_that_is_not_plain_and_relative_is_refused_before_a_server_is_started() {
        let (_guard, root, _data, directory) = workspace("paths");
        rust_sample(&root);
        let workspace = Workspace::authorize(&root, &directory).expect("authorize");
        for path in [
            "",
            "/etc/passwd",
            "../outside.rs",
            "src/../src/lib.rs",
            "./src/lib.rs",
            "src//lib.rs",
            "src/lib.rs/",
            "src\\lib.rs",
            "file:///etc/passwd",
            ".env",
            "src/.git/config",
            "src/.hidden.rs",
        ] {
            assert_eq!(
                absolute_path(&workspace, path).map(|_| ()),
                Err(Refusal::Path(
                    workspace_read::components(path).expect_err("refused")
                )),
                "{path:?}"
            );
        }
        // A path that is plain and relative but names nothing is refused
        // when the Workspace is walked, not before.
        assert_eq!(
            absolute_path(&workspace, "does/not/exist.rs").map(|_| ()),
            Ok(())
        );
        assert_eq!(
            read_document(&workspace, "does/not/exist.rs"),
            Err(PathRefusal::NotFound.into())
        );
    }

    #[test]
    fn a_document_is_read_under_the_workspaces_own_rules() {
        let (_guard, root, _data, directory) = workspace("documents");
        rust_sample(&root);
        let workspace = Workspace::authorize(&root, &directory).expect("authorize");
        assert!(
            read_document(&workspace, "src/lib.rs")
                .expect("read")
                .contains("pub fn add")
        );
        // Not UTF-8, and too large, are refused rather than guessed at.
        std::fs::write(root.join("latin1.rs"), [0xff, 0xfe, 0x00]).expect("write");
        assert_eq!(
            read_document(&workspace, "latin1.rs"),
            Err(PathRefusal::NotText.into())
        );
        let big = root.join("big.rs");
        std::fs::write(&big, vec![b'a'; MAX_DOCUMENT_BYTES + 1]).expect("write");
        assert_eq!(
            read_document(&workspace, "big.rs"),
            Err(Refusal::DocumentTooLarge)
        );
    }

    #[test]
    fn the_tool_is_registered_only_when_asked_and_admits_only_its_own_parameters() {
        let (_guard, root, _data, directory) = workspace("registration");
        rust_sample(&root);
        let workspace = Workspace::authorize(&root, &directory).expect("authorize");
        let servers = Servers::default();
        let registry = ToolRegistry::default();
        assert_eq!(
            registry
                .offer(&[TOOL_NAME])
                .expect_err("unregistered")
                .to_string(),
            format!("no tool named `{TOOL_NAME}` is registered")
        );
        register(&registry, workspace.clone(), servers.clone()).expect("register");
        assert_eq!(
            registry
                .offer(&[TOOL_NAME])
                .expect_err("unauthorized")
                .to_string(),
            format!("tool `{TOOL_NAME}` is not authorized")
        );
        registry.authorize(TOOL_NAME).expect("authorize");
        let offer = registry.offer(&[TOOL_NAME]).expect("offer");
        let wire = offer.wires().expect("wires");
        assert_eq!(wire.len(), 1);
        assert_eq!(wire[0].function.name, TOOL_NAME);

        let call = |arguments: Value| CompletedToolCall {
            index: 0,
            id: "call_1".to_owned(),
            name: TOOL_NAME.to_owned(),
            arguments: arguments.to_string(),
        };
        for refused in [
            json!({"path": "src/lib.rs"}),
            json!({"operation": "diagnostics"}),
            json!({"path": "src/lib.rs", "operation": "refs"}),
            json!({"path": "src/lib.rs", "operation": "diagnostics", "line": 0}),
            json!({"path": "src/lib.rs", "operation": "definition", "line": -1, "character": 1}),
            json!({"path": "src/lib.rs", "operation": "definition", "line": 1.5, "character": 1}),
            json!({"path": "src/lib.rs", "operation": "diagnostics", "offset": 3}),
            json!({"path": "src/lib.rs", "operation": "diagnostics", "server": "rust-analyzer"}),
        ] {
            assert!(
                offer.resolve(&call(refused.clone())).is_err(),
                "the schema refuses {refused}"
            );
        }
        let admitted = [
            json!({"path": "src/lib.rs", "operation": "diagnostics"}),
            json!({"path": "src/lib.rs", "operation": "definition", "line": 11, "character": 5}),
            json!({"path": "src/index.ts", "operation": "hover", "line": 9, "character": 10}),
        ];
        for arguments in admitted {
            assert!(
                offer.resolve(&call(arguments.clone())).is_ok(),
                "the schema admits {arguments}"
            );
        }
    }

    #[test]
    fn the_tool_declares_that_it_is_not_side_effect_free() {
        let (_guard, root, _data, directory) = workspace("effect");
        rust_sample(&root);
        let workspace = Workspace::authorize(&root, &directory).expect("authorize");
        let registry = ToolRegistry::default();
        register(&registry, workspace, Servers::default()).expect("register");
        registry.authorize(TOOL_NAME).expect("authorize");
        let offer = registry.offer(&[TOOL_NAME]).expect("offer");
        let call = CompletedToolCall {
            index: 0,
            id: "call_1".to_owned(),
            name: TOOL_NAME.to_owned(),
            arguments: json!({"path": "src/lib.rs", "operation": "diagnostics"}).to_string(),
        };
        let dispatch = offer.resolve(&call).expect("resolve");
        assert_eq!(
            dispatch.effect,
            ToolEffect::NonIdempotent,
            "starting a language server runs project code, so this is not `ReadOnly`"
        );
        assert_eq!(dispatch.version, HANDLER_VERSION);
    }

    #[test]
    fn nothing_is_started_for_a_query_that_cannot_be_asked() {
        let (_guard, root, _data, directory) = workspace("refusals");
        rust_sample(&root);
        let workspace = Workspace::authorize(&root, &directory).expect("authorize");
        // No server is named at all, so nothing may be started even though
        // every other argument is right.
        let servers = Servers::default();
        let (handler, sessions) = handler(workspace, servers, real_limits());
        let refused = [
            json!({"path": "README.md", "operation": "diagnostics"}),
            json!({"path": "src/lib.rs", "operation": "hover"}),
            json!({"path": "src/lib.rs", "operation": "definition", "line": 999, "character": 1}),
            json!({"path": "../outside.rs", "operation": "diagnostics"}),
            json!({"path": "src/lib.rs", "operation": "diagnostics", "line": 1, "character": 1}),
        ];
        for arguments in refused {
            let failure = call(handler.clone(), arguments.clone()).expect_err("refused");
            assert_eq!(
                failure.certainty,
                FailureCertainty::NotApplied,
                "{arguments} is refused before anything is started"
            );
        }
        assert!(
            sessions.rust.client.lock().expect("the session").is_none(),
            "no server was started for a call that could not be asked"
        );
    }

    /// The Rust demonstration, against the real rust-analyzer the operator
    /// named. Every result is printed, so the report can show what the server
    /// actually said rather than only what was asserted about it.
    #[test]
    #[ignore = "requires installed rust-analyzer; run explicit semantic acceptance"]
    fn rust_analyzer_answers_a_real_query_about_a_real_workspace() {
        let (_guard, root, _data, directory) = workspace("rust-real");
        rust_sample(&root);
        let workspace = Workspace::authorize(&root, &directory).expect("authorize");
        let servers = Servers {
            rust_analyzer: Some(rust_analyzer()),
            vtsls: None,
        };
        let (handler, sessions) = handler(workspace, servers, real_limits());

        let diagnostics = call(
            handler.clone(),
            json!({"path": "src/lib.rs", "operation": "diagnostics"}),
        )
        .expect("rust-analyzer answered");
        report("rust-analyzer diagnostics", &diagnostics);
        assert_eq!(diagnostics["server"], json!("rust-analyzer"));
        assert_eq!(diagnostics["language"], json!("rust"));
        assert_eq!(diagnostics["operation"], json!("diagnostics"));
        assert_eq!(diagnostics["path"], json!("src/lib.rs"));
        assert_eq!(
            diagnostics["diagnostics_complete"],
            json!(true),
            "the server pushed diagnostics for this document"
        );
        let items = diagnostics["diagnostics"]["items"]
            .as_array()
            .expect("items");
        let error = items
            .iter()
            .find(|item| item["severity"] == json!("error"))
            .unwrap_or_else(|| panic!("a genuine error is reported: {diagnostics}"));
        assert_eq!(
            error["range"]["start"]["line"],
            json!(7),
            "the error is on `\"not a number\"`, in this tool's own 1-based convention"
        );
        assert!(
            error["message"]
                .as_str()
                .is_some_and(|message| !message.is_empty()),
            "the server's own message is returned: {error}"
        );

        // The position of `add` in `uses_add`, and what the server says is
        // there. Both are in this tool's convention, so they can be fed back.
        let definition = call(
            handler.clone(),
            json!({
                "path": "src/lib.rs",
                "operation": "definition",
                "line": 11,
                "character": 5,
            }),
        )
        .expect("rust-analyzer answered");
        report("rust-analyzer definition", &definition);
        assert_eq!(definition["available"], json!(true));
        let location = &definition["definition"]["locations"][0];
        assert_eq!(location["path"], json!("src/lib.rs"));
        assert_eq!(location["inside_workspace"], json!(true));
        assert_eq!(location["range"]["start"]["line"], json!(2));
        assert_eq!(location["range"]["start"]["character"], json!(8));
        // The returned location is interpretable: it names `add` itself.
        assert_eq!(
            std::fs::read_to_string(root.join("src/lib.rs")).expect("read"),
            rust_sample_text(),
            "the sample the query ran against is the one asserted"
        );
        assert_eq!(&text_at(&root, "src/lib.rs", 2, 8, 3), "add");

        // The session ends through the protocol, and a later call starts a
        // fresh server rather than reusing one that is gone. It answers the
        // same question about the same file.
        on_ntex({
            let sessions = sessions.clone();
            async move { sessions.shutdown(Duration::from_secs(30)).await }
        });
        let restarted = call(
            handler.clone(),
            json!({"path": "src/lib.rs", "operation": "diagnostics"}),
        )
        .expect("a second session answers the same question");
        report("rust-analyzer diagnostics, second session", &restarted);
        assert_eq!(restarted["server"], json!("rust-analyzer"));
        assert_eq!(
            restarted["diagnostics"]["items"]
                .as_array()
                .expect("items")
                .first()
                .expect("the same error again")["code"],
            json!("E0308"),
            "the same genuine error is reported by the new server"
        );
    }

    /// The TypeScript demonstration, against the real vtsls the operator
    /// named, through the tool's own registry boundary.
    #[test]
    #[ignore = "requires installed vtsls; run explicit semantic acceptance"]
    fn vtsls_answers_a_real_query_through_the_tool_registry() {
        let (_guard, root, _data, directory) = workspace("typescript-real");
        typescript_sample(&root);
        let workspace = Workspace::authorize(&root, &directory).expect("authorize");
        let servers = Servers {
            rust_analyzer: None,
            vtsls: Some(vtsls()),
        };
        let registry = ToolRegistry::default();
        register(&registry, workspace, servers).expect("register");
        registry.authorize(TOOL_NAME).expect("authorize");
        let offer = registry.offer(&[TOOL_NAME]).expect("offer");
        let call_of = |arguments: Value| CompletedToolCall {
            index: 0,
            id: "call_1".to_owned(),
            name: TOOL_NAME.to_owned(),
            arguments: arguments.to_string(),
        };

        let dispatch = offer
            .resolve(&call_of(
                json!({"path": "src/index.ts", "operation": "diagnostics"}),
            ))
            .expect("the schema admits this call");
        assert_eq!(dispatch.version, HANDLER_VERSION);
        assert_eq!(dispatch.effect, ToolEffect::NonIdempotent);
        assert!(
            dispatch.schema.contains("workspace_lsp_query")
                || dispatch.schema.contains("operation"),
            "the call is recorded with the schema it was admitted against"
        );
        let Dispatch {
            handler, arguments, ..
        } = dispatch;
        let diagnostics =
            on_ntex(async move { handler.call(arguments).await }).expect("vtsls answered");
        report("vtsls diagnostics", &diagnostics);
        assert_eq!(diagnostics["server"], json!("vtsls"));
        assert_eq!(diagnostics["language"], json!("typescript"));
        assert_eq!(diagnostics["diagnostics_complete"], json!(true));
        let items = diagnostics["diagnostics"]["items"]
            .as_array()
            .expect("items");
        let error = items
            .iter()
            .find(|item| item["severity"] == json!("error"))
            .unwrap_or_else(|| panic!("a genuine type error is reported: {diagnostics}"));
        assert_eq!(
            error["range"]["start"]["line"],
            json!(6),
            "the error is on `total = \"forty two\"`, in this tool's 1-based convention"
        );

        let dispatch = offer
            .resolve(&call_of(json!({
                "path": "src/index.ts",
                "operation": "definition",
                "line": 9,
                "character": 10,
            })))
            .expect("the schema admits this call");
        let Dispatch {
            handler, arguments, ..
        } = dispatch;
        let definition =
            on_ntex(async move { handler.call(arguments).await }).expect("vtsls answered");
        report("vtsls definition", &definition);
        assert_eq!(definition["available"], json!(true));
        let location = &definition["definition"]["locations"][0];
        assert_eq!(location["path"], json!("src/index.ts"));
        assert_eq!(location["range"]["start"]["line"], json!(1));
        assert_eq!(location["range"]["start"]["character"], json!(17));
        assert_eq!(&text_at(&root, "src/index.ts", 1, 17, 5), "greet");

        let dispatch = offer
            .resolve(&call_of(json!({
                "path": "src/index.ts",
                "operation": "hover",
                "line": 9,
                "character": 10,
            })))
            .expect("the schema admits this call");
        let Dispatch {
            handler, arguments, ..
        } = dispatch;
        let hover = on_ntex(async move { handler.call(arguments).await }).expect("vtsls answered");
        report("vtsls hover", &hover);
        assert_eq!(hover["available"], json!(true));
        let contents = hover["hover"]["contents"].as_str().expect("hover text");
        assert!(
            contents.contains("greet"),
            "the hover names the function the position is on: {contents}"
        );
        assert!(
            hover["hover"]["contents"].as_str().map(str::len) < Some(MAX_HOVER_CHARS + 8),
            "the hover text is bounded"
        );
    }

    #[test]
    fn cancelling_a_query_stops_its_server_and_does_not_restart_it() {
        let (_guard, root, _data, directory) = workspace("cancelled-query");
        rust_sample(&root);
        let server = root.join("fake-server");
        write(
            &root,
            "fake-server",
            "#!/bin/sh\necho $$ > server.pid\nsleep 30\n",
        );
        std::fs::set_permissions(&server, std::fs::Permissions::from_mode(0o755))
            .expect("executable");
        let authorized = Workspace::authorize(&root, &directory).expect("authorize");
        let (handler, sessions) = handler(
            authorized,
            Servers {
                rust_analyzer: Some(server),
                vtsls: None,
            },
            real_limits(),
        );
        let kept = handler.clone();
        let pid_path = root.join("server.pid");
        on_ntex(async move {
            let outcome = ntex::util::select(
                handler.call(json!({"path":"src/lib.rs","operation":"diagnostics"})),
                async move {
                    let deadline = Instant::now() + Duration::from_secs(10);
                    while !pid_path.exists() {
                        assert!(
                            Instant::now() < deadline,
                            "the fake server must start before cancellation"
                        );
                        ntex::time::sleep(ntex::time::Millis(10)).await;
                    }
                },
            )
            .await;
            assert!(matches!(outcome, ntex::util::Either::Right(_)));
        });
        let pid: i32 = std::fs::read_to_string(root.join("server.pid"))
            .expect("started pid")
            .trim()
            .parse()
            .expect("pid");
        let pid = rustix::process::Pid::from_raw(pid).expect("nonzero pid");
        assert_eq!(
            rustix::process::test_kill_process(pid),
            Err(rustix::io::Errno::SRCH)
        );
        assert!(sessions.rust.disabled.load(Ordering::Acquire));
        let failure = call(kept, json!({"path":"src/lib.rs","operation":"diagnostics"}))
            .expect_err("failed session stays unavailable");
        assert_eq!(failure.certainty, FailureCertainty::NotApplied);
        assert!(failure.message.contains("unavailable"));
    }

    #[test]
    fn an_unavailable_server_is_not_reported_as_semantic_success() {
        let (_guard, root, _data, directory) = workspace("unavailable");
        rust_sample(&root);
        let workspace = Workspace::authorize(&root, &directory).expect("authorize");
        let (handler, sessions) = handler(workspace, Servers::default(), real_limits());
        let failure = call(
            handler,
            json!({"path":"src/lib.rs","operation":"diagnostics"}),
        )
        .expect_err("unavailable");
        assert_eq!(failure.certainty, FailureCertainty::NotApplied);
        assert!(failure.message.contains("unavailable"));
        assert!(sessions.rust.client.lock().expect("session").is_none());
    }

    #[test]
    #[ignore = "requires installed rust-analyzer and vtsls; explicit default-enabled semantic acceptance"]
    fn default_tools_discover_real_servers_and_synchronize_changed_documents() {
        const CHILD: &str = "OCG_TEST_DEFAULT_LSP_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let mut paths = vec![
                rust_analyzer()
                    .parent()
                    .expect("server directory")
                    .to_path_buf(),
                vtsls().parent().expect("server directory").to_path_buf(),
            ];
            paths.extend(
                std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default())
                    .filter(|p| p.is_absolute()),
            );
            let output = std::process::Command::new(std::env::current_exe().expect("test executable"))
                .args(["--exact","orchestration::workspace_lsp::tests::default_tools_discover_real_servers_and_synchronize_changed_documents","--ignored","--nocapture"])
                .env(CHILD,"1").env("PATH",std::env::join_paths(paths).expect("server search path"))
                .env_remove(RUST_ANALYZER_VARIABLE).env_remove(VTSLS_VARIABLE)
                .env_remove("OCG_WORKSPACE_LSP").env_remove("OCG_EXPERIMENTAL_WORKSPACE_FTS")
                .output().expect("default-enabled subprocess");
            println!("{}", String::from_utf8_lossy(&output.stdout));
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }
        assert!(std::env::var_os("OCG_WORKSPACE_LSP").is_none());
        assert!(std::env::var_os("OCG_EXPERIMENTAL_WORKSPACE_FTS").is_none());
        assert!(std::env::var_os(RUST_ANALYZER_VARIABLE).is_none());
        assert!(std::env::var_os(VTSLS_VARIABLE).is_none());
        for (name, path) in [
            ("rust-default", "src/lib.rs"),
            ("typescript-default", "src/index.ts"),
        ] {
            let (_guard, root, _data, directory) = workspace(name);
            if path.ends_with(".rs") {
                rust_sample(&root);
            } else {
                typescript_sample(&root);
            }
            let authorized = Workspace::authorize(&root, &directory).expect("authorize");
            let tools = crate::orchestration::workspace_command::code_tools(authorized)
                .expect("default coding tools");
            assert_eq!(tools.offer.wires().expect("wires").len(), 8);
            let dispatch = |arguments: Value| {
                tools
                    .offer
                    .resolve(&CompletedToolCall {
                        index: 0,
                        id: "default-lsp".to_owned(),
                        name: TOOL_NAME.to_owned(),
                        arguments: arguments.to_string(),
                    })
                    .expect("default LSP offered")
            };
            let first = dispatch(json!({"path":path,"operation":"diagnostics"}));
            let diagnostics = on_ntex(async move { first.handler.call(first.arguments).await })
                .expect("real diagnostics");
            report("default-discovered semantic diagnostics", &diagnostics);
            assert_eq!(diagnostics["analysis"], "language_server");
            assert_eq!(diagnostics["diagnostics_complete"], true);
            assert!(
                diagnostics["diagnostics"]["items"]
                    .as_array()
                    .expect("diagnostics")
                    .iter()
                    .any(|item| item["severity"] == "error")
            );
            if path.ends_with(".ts") {
                write(&root, path, "export const meaning = \"updated\";\n");
                let next =
                    dispatch(json!({"path":path,"operation":"hover","line":1,"character":15}));
                let hover = on_ntex(async move { next.handler.call(next.arguments).await })
                    .expect("updated document hover");
                report("hover after didChange", &hover);
                assert_eq!(hover["document_version"], 2);
                assert!(
                    hover["hover"]["contents"]
                        .as_str()
                        .expect("semantic hover")
                        .contains("updated")
                );
            }
        }
    }

    fn rust_sample_text() -> String {
        concat!(
            "/// Adds two numbers. Grüße 😀\n",
            "pub fn add(left: i64, right: i64) -> i64 {\n",
            "    left + right\n",
            "}\n",
            "\n",
            "pub fn broken() -> i64 {\n",
            "    \"not a number\"\n",
            "}\n",
            "\n",
            "pub fn uses_add() -> i64 {\n",
            "    add(1, 2)\n",
            "}\n",
        )
        .to_owned()
    }

    /// `length` UTF-16 code units of `name` at a 1-based `line` and
    /// `character`, so a returned location can be shown to point at what it
    /// says it points at.
    fn text_at(root: &Path, name: &str, line: usize, character: usize, length: usize) -> String {
        let text = std::fs::read_to_string(root.join(name)).expect("read the sample");
        let line = text
            .split('\n')
            .nth(line - 1)
            .expect("the line the server named");
        line.chars().skip(character - 1).take(length).collect()
    }

    /// Prints what the server actually said, for the demonstration's record.
    fn report(what: &str, value: &Value) {
        eprintln!(
            "\n--- {what}\n{}",
            serde_json::to_string_pretty(value).expect("a serializable result")
        );
    }
}
