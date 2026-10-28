//! `workspace_list_directory` and `workspace_search`: let a Provider find
//! files in a Workspace the operator authorized without knowing their names,
//! and nothing else. The Workspace, its path rules and its walk are those of
//! `workspace_read_file`, see [`crate::system::executor::workspace_read`].
//!
//! A call names a directory by a plain relative path, or `.` for the root,
//! and it is reached from the root as a read reaches a file's directory: each
//! component opened relative to the one before, never through a symbolic
//! link, never into the data directory, from a root that must still be the
//! directory authorized. Entries are read from the opened directory itself,
//! and each is examined relative to it without following a link.
//!
//! What is never shown: an entry whose name is hidden, secret-bearing or
//! OCG-internal, or that no Workspace path could name (not UTF-8, or holding
//! a backslash, a colon or a control character). A symbolic link is listed
//! as one, by name only: its target is never read or followed. A search
//! never enters a symbolic link, another filesystem, the data directory, or
//! a directory named in [`SKIPPED_DIRECTORY_NAMES`] below the directory it
//! was asked to search, and it reads the content only of regular files with
//! a single link, within [`DiscoveryLimits::file_bytes`], that are UTF-8
//! text without NUL and unchanged while they were read; a file it cannot
//! search is counted, never shown.
//!
//! Bounds: a directory's names are read up to
//! [`DiscoveryLimits::directory_entries`] and sorted by their bytes, so the
//! order is the same on every call; a listing shows the first ones that fit
//! [`DiscoveryLimits::listed_entries`] and [`DiscoveryLimits::result_bytes`].
//! A search walks depth first in that order, within a depth, a number of
//! entries examined, a number of content bytes read and a time, and stops
//! once its result is full; its result says whether it reached the end and,
//! if not, which bound stopped it. A search runs on the blocking pool, so the
//! executor does not wait on its filesystem work; a call stopped by its time
//! limit or cancellation signals a running search to stop at its next entry.
//!
//! What this does not establish is what `workspace_read_file` does not: a
//! bind mount or hard-linked directory may still lead outside the Workspace
//! when named directly, and a file that changes while it is searched without
//! changing its size or times is not detected. Every call is recorded with
//! its arguments and result in plaintext tool evidence; no failure message
//! repeats a path or any content.

use std::fs::File;
use std::os::unix::fs::MetadataExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use regex::RegexBuilder;
use rustix::fs::{AtFlags, Dir, FileType};
use rustix::io::Errno;
use serde_json::{Value, json};

use super::workspace_read::{Refusal, Step, Workspace, check_component, open_within, read_bytes};
use crate::db::entities::ToolEffect;
use crate::system::executor::blocking::{self, Kind as BlockingKind};
use crate::system::executor::tools::{
    ToolFailure, ToolFuture, ToolHandler, ToolRegistry, ToolRegistryError,
};

/// The name the listing tool is offered and recorded under.
pub const LIST_TOOL_NAME: &str = "workspace_list_directory";
/// The name the search tool is offered and recorded under.
pub const SEARCH_TOOL_NAME: &str = "workspace_search";

/// Identify these handlers' behavior in every call they record.
const LIST_HANDLER_VERSION: &str = "workspace_list_directory-1";
const SEARCH_HANDLER_VERSION: &str = "workspace_search-2";

/// Directories a search does not enter below the directory it searches:
/// build output and installed dependencies, which are large and rarely what
/// is looked for. Naming one, or a directory inside one, searches it.
pub const SKIPPED_DIRECTORY_NAMES: &[&str] = &["target", "node_modules"];

/// The longest name pattern and text a search may look for, in bytes.
const MAX_NAME_PATTERN_BYTES: usize = 128;
const MAX_TEXT_BYTES: usize = 256;

/// The most of a matching line a search returns, in bytes.
const MAX_LINE_TEXT_BYTES: usize = 200;

/// The bounds of listing and searching.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DiscoveryLimits {
    /// Names read from one directory; the rest are not seen.
    pub(crate) directory_entries: usize,
    /// Entries one listing shows.
    pub(crate) listed_entries: usize,
    /// The largest serialized result of either tool.
    pub(crate) result_bytes: usize,
    /// How many directories deep below its directory a search goes.
    pub(crate) depth: usize,
    /// Entries one search examines.
    pub(crate) entries: usize,
    /// The largest file whose content a search reads.
    pub(crate) file_bytes: usize,
    /// Content bytes one search reads in all.
    pub(crate) content_bytes: u64,
    /// How long one search may walk.
    pub(crate) time: Duration,
    /// Matching lines returned for one file.
    pub(crate) matches_per_file: usize,
}

impl Default for DiscoveryLimits {
    fn default() -> Self {
        Self {
            directory_entries: 10_000,
            listed_entries: 200,
            result_bytes: 8 * 1024,
            depth: 16,
            entries: 20_000,
            file_bytes: 1024 * 1024,
            content_bytes: 32 * 1024 * 1024,
            time: Duration::from_secs(5),
            matches_per_file: 10,
        }
    }
}

/// The names of a directory that a Workspace path may name, sorted by their
/// bytes.
struct Names {
    names: Vec<String>,
    /// Not every name was read: there were more than the limit, or reading
    /// failed.
    partial: bool,
}

/// Reads the names in `directory`, at most `limit` of them, leaving out `.`
/// and `..` and every name a Workspace path could not name.
fn names(directory: &File, limit: usize) -> Result<Names, Refusal> {
    let mut entries = Dir::read_from(directory).map_err(Refusal::from_errno)?;
    let mut names = Vec::new();
    let mut read = 0;
    let mut partial = false;
    while let Some(entry) = entries.read() {
        let Ok(entry) = entry else {
            partial = true;
            break;
        };
        let name = entry.file_name().to_bytes();
        if name == b"." || name == b".." {
            continue;
        }
        if read == limit {
            partial = true;
            break;
        }
        read += 1;
        if let Ok(name) = std::str::from_utf8(name)
            && check_component(name).is_ok()
        {
            names.push(name.to_owned());
        }
    }
    names.sort_unstable();
    Ok(Names { names, partial })
}

/// What an entry is, as examined without following a link.
enum Kind {
    File {
        bytes: u64,
    },
    Directory,
    SymbolicLink,
    Other,
    /// The entry could not be examined.
    Inaccessible,
}

impl Kind {
    /// The entry `name` of `directory`; `None` when it no longer exists.
    fn of(directory: &File, name: &str) -> Option<Self> {
        match rustix::fs::statat(directory, name, AtFlags::SYMLINK_NOFOLLOW) {
            Err(Errno::NOENT) => None,
            Err(_) => Some(Self::Inaccessible),
            Ok(stat) => Some(match FileType::from_raw_mode(stat.st_mode) {
                FileType::RegularFile => Self::File {
                    bytes: u64::try_from(stat.st_size).unwrap_or(0),
                },
                FileType::Directory => Self::Directory,
                FileType::Symlink => Self::SymbolicLink,
                _ => Self::Other,
            }),
        }
    }

    fn entry(&self, name: &str) -> Value {
        match self {
            Self::File { bytes } => json!({"name": name, "type": "file", "bytes": bytes}),
            Self::Directory => json!({"name": name, "type": "directory"}),
            Self::SymbolicLink => json!({"name": name, "type": "symlink"}),
            Self::Other => json!({"name": name, "type": "other"}),
            Self::Inaccessible => json!({"name": name, "type": "inaccessible"}),
        }
    }
}

/// The directory a call names, `.` when it names none.
fn directory_path(arguments: &Value) -> Result<&str, Refusal> {
    if !only_fields(arguments, &["path"]) {
        return Err(Refusal::InvalidPath);
    }
    match arguments.get("path") {
        None => Ok("."),
        Some(path) => path.as_str().ok_or(Refusal::InvalidPath),
    }
}

/// Lists the directory at `path`: its first names in order, each with its
/// type, and a file's size, as many as fit the limits.
fn list(workspace: &Workspace, path: &str, limits: &DiscoveryLimits) -> Result<Value, Refusal> {
    let directory = workspace.directory(path)?;
    let Names { names, partial } = names(&directory, limits.directory_entries)?;
    let result = |entries: Vec<Value>, truncated: bool| {
        json!({
            "path": path,
            "entries": entries,
            "total_entries": names.len(),
            "truncated": truncated,
        })
    };
    let mut size = result(Vec::new(), true).to_string().len();
    let mut entries = Vec::new();
    let mut truncated = partial;
    for name in &names {
        let Some(kind) = Kind::of(&directory, name) else {
            continue;
        };
        let entry = kind.entry(name);
        let cost = entry.to_string().len() + 1;
        if entries.len() == limits.listed_entries || size + cost > limits.result_bytes {
            truncated = true;
            break;
        }
        size += cost;
        entries.push(entry);
    }
    Ok(result(entries, truncated))
}

/// A file name pattern: a part of the name, or with `*` and `?`, the whole
/// name. ASCII letters match either case.
#[derive(Debug)]
struct NamePattern {
    pattern: Vec<char>,
    wildcard: bool,
}

impl NamePattern {
    fn new(pattern: &str) -> Self {
        Self {
            pattern: pattern.to_ascii_lowercase().chars().collect(),
            wildcard: pattern.contains(['*', '?']),
        }
    }

    fn matches(&self, name: &str) -> bool {
        let name = name.to_ascii_lowercase().chars().collect::<Vec<_>>();
        if self.wildcard {
            wildcard_match(&self.pattern, &name)
        } else {
            name.windows(self.pattern.len())
                .any(|window| window == self.pattern.as_slice())
        }
    }
}

/// Whether `name` is matched whole by `pattern`, in which `*` matches any
/// run of characters and `?` any one. Backtracks only to the last `*`, so it
/// takes at most the product of the lengths in steps.
fn wildcard_match(pattern: &[char], name: &[char]) -> bool {
    let (mut at, mut position) = (0, 0);
    let mut star: Option<(usize, usize)> = None;
    while position < name.len() {
        match pattern.get(at) {
            Some('*') => {
                star = Some((at, position));
                at += 1;
            }
            Some(&character) if character == '?' || character == name[position] => {
                at += 1;
                position += 1;
            }
            _ => match star {
                Some((star_at, star_position)) => {
                    at = star_at + 1;
                    position = star_position + 1;
                    star = Some((star_at, star_position + 1));
                }
                None => return false,
            },
        }
    }
    pattern[at..].iter().all(|character| *character == '*')
}

/// How a search matches file content.
#[derive(Debug)]
enum TextMatcher {
    Literal(String),
    Regex(regex::Regex),
}

/// What a search looks for: files whose name matches, files holding a text,
/// or files that do both.
#[derive(Debug)]
struct Query {
    path: String,
    name: Option<NamePattern>,
    text: Option<TextMatcher>,
}

/// Why a search's arguments are refused. Its message is recorded and may
/// reach the Provider, so it never repeats them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
enum QueryRefusal {
    #[error(
        "a search needs a `name` of 1 to {MAX_NAME_PATTERN_BYTES} bytes without `/` or control characters, a `text` of 1 to {MAX_TEXT_BYTES} bytes on one line, or both"
    )]
    Invalid,
    #[error("the regular expression cannot be compiled")]
    Regex,
    #[error(transparent)]
    Path(#[from] Refusal),
}

/// Whether `arguments` is an object with no field but `fields`.
fn only_fields(arguments: &Value, fields: &[&str]) -> bool {
    arguments
        .as_object()
        .is_some_and(|object| object.keys().all(|key| fields.contains(&key.as_str())))
}

/// The search a call asks for. The schema already bounds the arguments;
/// they are checked again here.
fn query(arguments: &Value) -> Result<Query, QueryRefusal> {
    if !only_fields(arguments, &["path", "name", "text", "regex"]) {
        return Err(QueryRefusal::Invalid);
    }
    let path = match arguments.get("path") {
        None => ".".to_owned(),
        Some(path) => path.as_str().ok_or(Refusal::InvalidPath)?.to_owned(),
    };
    let field = |name: &str| match arguments.get(name) {
        None => Ok(None),
        Some(value) => value.as_str().map(Some).ok_or(QueryRefusal::Invalid),
    };
    let name = field("name")?;
    let text = field("text")?;
    let regex = match arguments.get("regex") {
        None => false,
        Some(Value::Bool(b)) => *b,
        Some(_) => return Err(QueryRefusal::Invalid),
    };
    if let Some(name) = name
        && (name.is_empty()
            || name.len() > MAX_NAME_PATTERN_BYTES
            || name.contains('/')
            || name.chars().any(char::is_control))
    {
        return Err(QueryRefusal::Invalid);
    }
    if let Some(text) = text
        && (text.is_empty() || text.len() > MAX_TEXT_BYTES || text.contains(['\n', '\r', '\0']))
    {
        return Err(QueryRefusal::Invalid);
    }
    if regex && text.is_none() {
        return Err(QueryRefusal::Invalid);
    }
    if name.is_none() && text.is_none() {
        return Err(QueryRefusal::Invalid);
    }
    let text_matcher = match (text, regex) {
        (None, _) => None,
        (Some(text), false) => Some(TextMatcher::Literal(text.to_owned())),
        (Some(text), true) => {
            let compiled = RegexBuilder::new(text)
                .size_limit(1 << 20)
                .dfa_size_limit(1 << 20)
                .build()
                .map_err(|_| QueryRefusal::Regex)?;
            Some(TextMatcher::Regex(compiled))
        }
    };
    Ok(Query {
        path,
        name: name.map(NamePattern::new),
        text: text_matcher,
    })
}

/// Which bound ended a search before it walked everything.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Limit {
    /// The result is full.
    Results,
    /// It examined as many entries as it may.
    Entries,
    /// It read as much content as it may.
    ContentBytes,
    /// It ran as long as it may.
    Time,
    /// The call was stopped, so the result is never returned.
    Stopped,
}

impl Limit {
    fn name(self) -> &'static str {
        match self {
            Self::Results => "results",
            Self::Entries => "entries",
            Self::ContentBytes => "content_bytes",
            Self::Time => "time",
            Self::Stopped => "stopped",
        }
    }
}

/// One search in progress.
struct Walk<'a> {
    workspace: &'a Workspace,
    query: &'a Query,
    limits: &'a DiscoveryLimits,
    stop: &'a AtomicBool,
    deadline: Instant,
    /// The filesystem of the directory searched; no other is entered.
    device: u64,
    examined: usize,
    content_bytes: u64,
    skipped: usize,
    files: Vec<Value>,
    /// The serialized result so far.
    size: usize,
    limit: Option<Limit>,
}

impl Walk<'_> {
    /// Whether the search must end now, recording why.
    fn ended(&mut self) -> bool {
        if self.limit.is_some() {
            return true;
        }

        if self.stop.load(Ordering::Relaxed) {
            self.limit = Some(Limit::Stopped);
        } else if Instant::now() >= self.deadline {
            self.limit = Some(Limit::Time);
        } else if self.examined == self.limits.entries {
            self.limit = Some(Limit::Entries);
        }
        self.limit.is_some()
    }

    /// Searches `directory`, at `path` and `depth` directories below the
    /// directory searched, in the order of its names.
    fn directory(&mut self, directory: &File, path: &str, depth: usize) {
        let Ok(Names { names, partial }) = names(directory, self.limits.directory_entries) else {
            self.skipped += 1;
            return;
        };
        if partial {
            self.skipped += 1;
        }
        for name in &names {
            if self.ended() {
                return;
            }
            self.examined += 1;
            let path = if path == "." {
                name.clone()
            } else {
                format!("{path}/{name}")
            };
            match Kind::of(directory, name) {
                None | Some(Kind::SymbolicLink | Kind::Other) => {}
                Some(Kind::Inaccessible) => self.skipped += 1,
                Some(Kind::File { bytes }) => self.file(directory, name, &path, bytes),
                Some(Kind::Directory) => {
                    if SKIPPED_DIRECTORY_NAMES.contains(&name.as_str()) {
                        continue;
                    }
                    if depth == self.limits.depth {
                        self.skipped += 1;
                        continue;
                    }
                    let entered =
                        self.workspace
                            .subdirectory(directory, name)
                            .ok()
                            .filter(|child| {
                                child
                                    .metadata()
                                    .is_ok_and(|metadata| metadata.dev() == self.device)
                            });
                    match entered {
                        Some(child) => self.directory(&child, &path, depth + 1),
                        None => self.skipped += 1,
                    }
                }
            }
        }
    }

    /// Searches the file `name` of `directory`, at `path`, of `bytes` bytes
    /// as it was examined.
    fn file(&mut self, directory: &File, name: &str, path: &str, bytes: u64) {
        if let Some(pattern) = &self.query.name
            && !pattern.matches(name)
        {
            return;
        }
        let Some(text) = &self.query.text else {
            self.push(json!({"path": path, "bytes": bytes}));
            return;
        };
        if bytes > self.limits.file_bytes as u64 {
            self.skipped += 1;
            return;
        }
        if self.content_bytes.saturating_add(bytes) > self.limits.content_bytes {
            self.limit = Some(Limit::ContentBytes);
            return;
        }
        let content = open_within(directory, name, Step::File, self.limits.file_bytes)
            .and_then(|file| read_bytes(&file, self.limits.file_bytes));
        let Ok(content) = content else {
            self.skipped += 1;
            return;
        };
        self.content_bytes += content.len() as u64;
        let content = match String::from_utf8(content) {
            Ok(content) if !content.contains('\0') => content,
            _ => {
                self.skipped += 1;
                return;
            }
        };
        let mut matches = Vec::new();
        let mut more = false;
        for (index, line) in content.split('\n').enumerate() {
            let line = line.strip_suffix('\r').unwrap_or(line);
            let matched = match text {
                TextMatcher::Literal(text) => line.contains(text.as_str()),
                TextMatcher::Regex(regex) => regex.is_match(line),
            };
            if !matched {
                continue;
            }
            if matches.len() == self.limits.matches_per_file {
                more = true;
                break;
            }
            matches.push(json!({"line": index + 1, "text": excerpt(line)}));
        }
        if !matches.is_empty() {
            self.push(json!({
                "path": path,
                "bytes": content.len(),
                "matches": matches,
                "more_matches": more,
            }));
        }
    }

    /// Adds a file to the result, unless the result is full.
    fn push(&mut self, file: Value) {
        let cost = file.to_string().len() + 1;
        if self.size + cost > self.limits.result_bytes {
            self.limit = Some(Limit::Results);
            return;
        }
        self.size += cost;
        self.files.push(file);
    }
}

/// A matching line as a search returns it: without its line end, and cut to
/// [`MAX_LINE_TEXT_BYTES`] at a character boundary.
fn excerpt(line: &str) -> &str {
    let line = line.strip_suffix('\r').unwrap_or(line);
    &line[..line.floor_char_boundary(MAX_LINE_TEXT_BYTES)]
}

/// The result of a search that ended for `limit`, or walked everything.
fn search_result(path: &str, files: Vec<Value>, limit: Option<Limit>, skipped: usize) -> Value {
    json!({
        "path": path,
        "files": files,
        "complete": limit.is_none(),
        "limit": limit.map(Limit::name),
        "skipped": skipped,
    })
}

/// Searches below the directory the query names, until it is walked, a
/// bound is reached or `stop` is set.
fn search(
    workspace: &Workspace,
    query: &Query,
    limits: &DiscoveryLimits,
    stop: &AtomicBool,
) -> Result<Value, Refusal> {
    let deadline = Instant::now() + limits.time;
    let directory = workspace.directory(&query.path)?;
    let device = directory.metadata().map_err(|_| Refusal::Unreadable)?.dev();
    let mut walk = Walk {
        workspace,
        query,
        limits,
        stop,
        deadline,
        device,
        examined: 0,
        content_bytes: 0,
        skipped: 0,
        files: Vec::new(),
        size: search_result(
            &query.path,
            Vec::new(),
            Some(Limit::ContentBytes),
            usize::MAX,
        )
        .to_string()
        .len(),
        limit: None,
    };
    walk.directory(&directory, &query.path, 0);
    Ok(search_result(
        &query.path,
        walk.files,
        walk.limit,
        walk.skipped,
    ))
}

/// Tells a search to stop when its call ends without its result, as when
/// the call's limit or cancellation drops it.
struct StopOnDrop(Arc<AtomicBool>);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

fn refused(refusal: impl std::fmt::Display) -> ToolFailure {
    ToolFailure::not_applied(refusal.to_string())
}

/// The handler of `workspace_list_directory`, bound to one authorized
/// Workspace.
struct ListDirectory {
    workspace: Workspace,
    limits: DiscoveryLimits,
}

impl ToolHandler for ListDirectory {
    fn version(&self) -> &str {
        LIST_HANDLER_VERSION
    }

    /// Listing changes nothing outside OCG.
    fn effect(&self) -> ToolEffect {
        ToolEffect::ReadOnly
    }

    /// Lists on the blocking pool while holding a file permit.
    fn call(&self, arguments: Value) -> ToolFuture<'_> {
        Box::pin(async move {
            let path = directory_path(&arguments).map_err(refused)?.to_owned();
            let workspace = self.workspace.clone();
            let limits = self.limits;
            blocking::run(BlockingKind::File, move || list(&workspace, &path, &limits))
                .await
                .map_err(ToolFailure::uncertain)?
                .map_err(refused)
        })
    }
}

/// The handler of `workspace_search`, bound to one authorized Workspace.
struct Search {
    workspace: Workspace,
    limits: DiscoveryLimits,
}

impl ToolHandler for Search {
    fn version(&self) -> &str {
        SEARCH_HANDLER_VERSION
    }

    /// Searching changes nothing outside OCG.
    fn effect(&self) -> ToolEffect {
        ToolEffect::ReadOnly
    }

    /// Searches on the blocking pool and awaits its result. Dropping the call
    /// signals a running search to stop.
    fn call(&self, arguments: Value) -> ToolFuture<'_> {
        Box::pin(async move {
            let query = query(&arguments).map_err(refused)?;
            let stop = Arc::new(AtomicBool::new(false));
            let stopping = StopOnDrop(stop.clone());
            let (workspace, limits) = (self.workspace.clone(), self.limits);

            let found = blocking::run(BlockingKind::File, move || {
                search(&workspace, &query, &limits, &stop)
            })
            .await
            .map_err(|_| ToolFailure::not_applied("the search ended without a result"))?;
            drop(stopping);
            found.map_err(refused)
        })
    }
}

/// The parameters a listing call must match exactly: at most a directory.
fn list_parameters() -> Value {
    json!({
        "type": "object",
        "properties": {
            "path": {
                "type": "string",
                "description": "A directory relative to the authorized Workspace root; \".\" or omitted for the root"
            }
        },
        "additionalProperties": false
    })
}

/// The parameters a search call must match exactly: at most a directory, a
/// name pattern and a text.
fn search_parameters() -> Value {
    json!({
        "type": "object",
        "properties": {
            "path": {
                "type": "string",
                "description": "The directory to search below, relative to the authorized Workspace root; \".\" or omitted for the root"
            },
            "name": {
                "type": "string",
                "minLength": 1,
                "maxLength": MAX_NAME_PATTERN_BYTES,
                "description": "Match file names: a part of the name, or with * and ? a whole-name pattern such as *.rs; case-insensitive for ASCII letters"
            },
            "text": {
                "type": "string",
                "minLength": 1,
                "maxLength": MAX_TEXT_BYTES,
                "description": "Match files whose content contains this case-sensitive text on one line (exact by default, or as a regular expression when `regex` is true); returns the matching lines"
            },
            "regex": {
                "type": "boolean",
                "description": "When true, match `text` as a regular expression against each line with ^ and $ as line anchors; refuses an invalid pattern"
            }
        },
        "additionalProperties": false
    })
}

/// Registers `workspace_list_directory` and `workspace_search` for
/// `workspace` in `registry`, with the default [`DiscoveryLimits`], without
/// authorizing them.
pub(crate) fn register(
    registry: &ToolRegistry,
    workspace: Workspace,
) -> Result<(), ToolRegistryError> {
    let limits = DiscoveryLimits::default();
    registry.register(
        LIST_TOOL_NAME,
        Some(
            "List one directory of the authorized Workspace: its entries sorted by name, each with its type (file, directory, symlink, other, inaccessible) and a file's size in bytes. Hidden and secret-bearing names are not shown, and symlinks are never followed. At most 200 entries; `truncated` says some were left out and `total_entries` counts them all.",
        ),
        list_parameters(),
        Arc::new(ListDirectory {
            workspace: workspace.clone(),
            limits,
        }),
    )?;
    registry.register(
        SEARCH_TOOL_NAME,
        Some(
            "Find files below a directory of the authorized Workspace by name, by content, or both: give `name`, `text`, or both. Returns matching file paths with their sizes and, for `text`, the matching line numbers and lines (each cut to 200 bytes, at most 10 per file). Hidden, secret-bearing, symlinked, binary and over 1 MiB files are not searched, nor `target` or `node_modules` directories unless `path` names them. `complete` is false when a bound (results, entries, content_bytes, time) named by `limit` ended the search early; `skipped` counts entries that could not be searched.",
        ),
        search_parameters(),
        Arc::new(Search {
            workspace,
            limits,

        }),
    )
}
