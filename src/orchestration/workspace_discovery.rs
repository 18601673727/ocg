//! `workspace_list_directory` and `workspace_search`: let a Provider find
//! files in a Workspace the operator authorized without knowing their names,
//! and nothing else. The Workspace, its path rules and its walk are those of
//! `workspace_read_file`, see [`crate::orchestration::workspace_read`].
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
//! if not, which bound stopped it. A search runs on a thread of its own, so
//! the executor never blocks on it; a call stopped by its time limit or by
//! cancellation tells the search to stop, and it does at its next entry.
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
use std::thread;
use std::time::{Duration, Instant};

use rustix::fs::{AtFlags, Dir, FileType};
use rustix::io::Errno;
use serde_json::{Value, json};

use crate::orchestration::tools::{
    ToolFailure, ToolFuture, ToolHandler, ToolRegistry, ToolRegistryError,
};
use crate::orchestration::workspace_read::{
    Refusal, Step, Workspace, check_component, open_within, read_bytes,
};
use crate::persistence::ToolEffect;

/// The name the listing tool is offered and recorded under.
pub const LIST_TOOL_NAME: &str = "workspace_list_directory";
/// The name the search tool is offered and recorded under.
pub const SEARCH_TOOL_NAME: &str = "workspace_search";

/// Identify these handlers' behavior in every call they record.
const LIST_HANDLER_VERSION: &str = "workspace_list_directory-1";
const SEARCH_HANDLER_VERSION: &str = "workspace_search-1";

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

/// What a search looks for: files whose name matches, files holding a text,
/// or files that do both.
#[derive(Debug)]
struct Query {
    path: String,
    name: Option<NamePattern>,
    text: Option<String>,
}

/// Why a search's arguments are refused. Its message is recorded and may
/// reach the Provider, so it never repeats them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
enum QueryRefusal {
    #[error(
        "a search needs a `name` of 1 to {MAX_NAME_PATTERN_BYTES} bytes without `/` or control characters, a `text` of 1 to {MAX_TEXT_BYTES} bytes on one line, or both"
    )]
    Invalid,
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
    if !only_fields(arguments, &["path", "name", "text"]) {
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
    if name.is_none() && text.is_none() {
        return Err(QueryRefusal::Invalid);
    }
    Ok(Query {
        path,
        name: name.map(NamePattern::new),
        text: text.map(str::to_owned),
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

/// Called at every entry a search examines, on the search's thread, with
/// the flag that stops it.
#[cfg(test)]
pub(crate) type Probe = Arc<dyn Fn(&AtomicBool) + Send + Sync>;

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
    #[cfg(test)]
    probe: Option<&'a Probe>,
}

impl Walk<'_> {
    /// Whether the search must end now, recording why.
    fn ended(&mut self) -> bool {
        if self.limit.is_some() {
            return true;
        }
        #[cfg(test)]
        if let Some(probe) = self.probe {
            probe(self.stop);
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
            if !line.contains(text.as_str()) {
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
    #[cfg(test)] probe: Option<&Probe>,
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
        #[cfg(test)]
        probe,
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

    /// Lists synchronously: one directory, read within its bound, so the
    /// call does not yield while it reads.
    fn call(&self, arguments: Value) -> ToolFuture<'_> {
        Box::pin(async move {
            let path = directory_path(&arguments).map_err(refused)?;
            list(&self.workspace, path, &self.limits).map_err(refused)
        })
    }
}

/// The handler of `workspace_search`, bound to one authorized Workspace.
struct Search {
    workspace: Workspace,
    limits: DiscoveryLimits,
    #[cfg(test)]
    probe: Option<Probe>,
}

impl ToolHandler for Search {
    fn version(&self) -> &str {
        SEARCH_HANDLER_VERSION
    }

    /// Searching changes nothing outside OCG.
    fn effect(&self) -> ToolEffect {
        ToolEffect::ReadOnly
    }

    /// Searches on a thread of its own and awaits its result. Dropping the
    /// call, as its time limit or cancellation does, stops the search.
    fn call(&self, arguments: Value) -> ToolFuture<'_> {
        Box::pin(async move {
            let query = query(&arguments).map_err(refused)?;
            let stop = Arc::new(AtomicBool::new(false));
            let stopping = StopOnDrop(stop.clone());
            let (workspace, limits) = (self.workspace.clone(), self.limits);
            #[cfg(test)]
            let probe = self.probe.clone();
            let (report, reported) = flume::bounded(1);
            thread::Builder::new()
                .name("ocg-workspace-search".to_owned())
                .spawn(move || {
                    let found = search(
                        &workspace,
                        &query,
                        &limits,
                        &stop,
                        #[cfg(test)]
                        probe.as_ref(),
                    );
                    report.send(found).ok();
                })
                .map_err(|_| ToolFailure::not_applied("the search could not be started"))?;
            let found = reported
                .recv_async()
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
                "description": "Match files whose content contains this exact, case-sensitive text on one line; returns the matching lines"
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
            #[cfg(test)]
            probe: None,
        }),
    )
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::symlink;
    use std::path::{Path, PathBuf};

    use super::*;
    use crate::data_directory::DataDirectory;
    use crate::openai_compatible::admission::compile_parameters;
    use crate::test_support::{TestRoot, WAIT, data_directory, temporary_directory};

    const SUITE: &str = "ocg-workspace-discovery";

    /// An empty Workspace beside a data directory of its own.
    fn workspace(name: &str) -> (TestRoot, PathBuf, TestRoot, DataDirectory) {
        let (guard, root) = temporary_directory(SUITE, &format!("{name}-workspace"));
        let (data_root, directory) = data_directory(SUITE, name);
        (guard, root, data_root, directory)
    }

    fn write(root: &Path, path: &str, content: &[u8]) {
        let path = root.join(path);
        std::fs::create_dir_all(path.parent().expect("a parent")).expect("create parents");
        std::fs::write(path, content).expect("write");
    }

    fn authorize(root: &Path, directory: &DataDirectory) -> Workspace {
        Workspace::authorize(root, directory).expect("authorize")
    }

    /// Runs a listing as the tool loop does.
    fn listing(
        workspace: &Workspace,
        limits: DiscoveryLimits,
        arguments: Value,
    ) -> Result<Value, ToolFailure> {
        let handler = ListDirectory {
            workspace: workspace.clone(),
            limits,
        };
        crate::test_support::wait(handler.call(arguments))
    }

    fn list_default(workspace: &Workspace, arguments: Value) -> Result<Value, ToolFailure> {
        listing(workspace, DiscoveryLimits::default(), arguments)
    }

    fn handler(workspace: &Workspace, limits: DiscoveryLimits, probe: Option<Probe>) -> Search {
        Search {
            workspace: workspace.clone(),
            limits,
            probe,
        }
    }

    fn on_ntex<F: std::future::Future + 'static>(future: F) -> F::Output
    where
        F::Output: 'static,
    {
        ntex::rt::System::new("discovery-test", ntex::rt::DefaultRuntime).block_on(future)
    }

    /// Runs a search as the tool loop does, on an executor of its own.
    fn searching(
        workspace: &Workspace,
        limits: DiscoveryLimits,
        arguments: Value,
    ) -> Result<Value, ToolFailure> {
        let handler = handler(workspace, limits, None);
        on_ntex(async move { handler.call(arguments).await })
    }

    fn search_default(workspace: &Workspace, arguments: Value) -> Result<Value, ToolFailure> {
        searching(workspace, DiscoveryLimits::default(), arguments)
    }

    fn paths(result: &Value) -> Vec<&str> {
        result["files"]
            .as_array()
            .expect("files")
            .iter()
            .map(|file| file["path"].as_str().expect("a path"))
            .collect()
    }

    fn refused(refusal: Refusal) -> Result<Value, ToolFailure> {
        Err(ToolFailure::not_applied(refusal.to_string()))
    }

    /// A small project with every kind of entry a Workspace may hold.
    fn project(root: &Path, outside: &Path, directory: &DataDirectory) {
        write(root, "Cargo.toml", b"[package]\nname = \"demo\"\n");
        write(root, "README.md", b"# Demo\n");
        write(root, "src/lib.rs", b"pub mod config;\n");
        write(
            root,
            "src/config.rs",
            b"pub const RETRY_LIMIT: u32 = 7;\n\npub fn retries() -> u32 {\n    RETRY_LIMIT\n}\n",
        );
        write(
            root,
            "src/nested/deep/mod.rs",
            b"// RETRY_LIMIT is in config\n",
        );
        write(root, ".env", b"RETRY_LIMIT=secret\n");
        write(root, ".git/config", b"RETRY_LIMIT\n");
        write(root, "keys/server.pem", b"RETRY_LIMIT\n");
        write(root, "config/secrets.yaml", b"RETRY_LIMIT: 1\n");
        write(root, "target/debug/out.rs", b"RETRY_LIMIT\n");
        write(root, "node_modules/pkg/index.js", b"RETRY_LIMIT\n");
        write(root, "image.png", b"\x89PNG\r\n\x1a\n\x00RETRY_LIMIT");
        write(outside, "secret.rs", b"RETRY_LIMIT = outside\n");
        symlink(outside.join("secret.rs"), root.join("linked.rs")).expect("link");
        symlink(outside, root.join("outside")).expect("link");
        symlink(directory.root(), root.join("data")).expect("link");
        let made = std::process::Command::new("mkfifo")
            .arg(root.join("pipe"))
            .status()
            .expect("run mkfifo");
        assert!(made.success(), "make a FIFO");
    }

    #[test]
    fn a_directory_is_listed_in_order_with_types_sizes_and_nothing_hidden() {
        let (_guard, root, _data, directory) = workspace("list");
        let (_outside_guard, outside, _outside_data, _) = workspace("list-outside");
        project(&root, &outside, &directory);
        let workspace = authorize(&root, &directory);
        let expected = json!({
            "path": ".",
            "entries": [
                {"name": "Cargo.toml", "type": "file", "bytes": 24},
                {"name": "README.md", "type": "file", "bytes": 7},
                {"name": "config", "type": "directory"},
                {"name": "data", "type": "symlink"},
                {"name": "image.png", "type": "file", "bytes": 20},
                {"name": "keys", "type": "directory"},
                {"name": "linked.rs", "type": "symlink"},
                {"name": "node_modules", "type": "directory"},
                {"name": "outside", "type": "symlink"},
                {"name": "pipe", "type": "other"},
                {"name": "src", "type": "directory"},
                {"name": "target", "type": "directory"},
            ],
            "total_entries": 12,
            "truncated": false,
        });
        assert_eq!(list_default(&workspace, json!({})), Ok(expected.clone()));
        assert_eq!(
            list_default(&workspace, json!({"path": "."})),
            Ok(expected),
            "`.` is the root, as an omitted path is"
        );
        assert_eq!(
            list_default(&workspace, json!({"path": "src"})),
            Ok(json!({
                "path": "src",
                "entries": [
                    {"name": "config.rs", "type": "file", "bytes": 77},
                    {"name": "lib.rs", "type": "file", "bytes": 16},
                    {"name": "nested", "type": "directory"},
                ],
                "total_entries": 3,
                "truncated": false,
            }))
        );
        // A directory holding only protected names lists none of them.
        assert_eq!(
            list_default(&workspace, json!({"path": "keys"})).expect("listed")["entries"],
            json!([])
        );
    }

    #[test]
    fn listing_keeps_the_workspace_boundary() {
        let (_guard, root, _data, directory) = workspace("list-boundary");
        let (_outside_guard, outside, _outside_data, _) = workspace("list-boundary-outside");
        project(&root, &outside, &directory);
        let workspace = authorize(&root, &directory);
        let absolute = outside.display().to_string();
        for (path, refusal) in [
            ("outside", Refusal::SymbolicLink),
            ("data", Refusal::SymbolicLink),
            ("outside/nested", Refusal::SymbolicLink),
            (".git", Refusal::Protected),
            ("src/.hidden", Refusal::Protected),
            ("keys/server.pem", Refusal::Protected),
            ("..", Refusal::InvalidPath),
            ("../list-boundary-outside-workspace", Refusal::InvalidPath),
            ("src/", Refusal::InvalidPath),
            ("", Refusal::InvalidPath),
            (absolute.as_str(), Refusal::InvalidPath),
            ("Cargo.toml", Refusal::NotDirectory),
            ("pipe", Refusal::NotDirectory),
            ("missing", Refusal::NotFound),
        ] {
            assert_eq!(
                list_default(&workspace, json!({"path": path})),
                refused(refusal),
                "{path:?}"
            );
        }
        assert_eq!(
            list_default(&workspace, json!({"path": 3})),
            refused(Refusal::InvalidPath)
        );

        // A replaced root is refused, whatever the new one holds.
        let (_moved_guard, moved) = temporary_directory(SUITE, "list-boundary-moved");
        std::fs::remove_dir(&moved).expect("free the name");
        std::fs::rename(&root, &moved).expect("move the Workspace away");
        std::fs::create_dir(&root).expect("create another directory");
        write(&root, "other.txt", b"replaced");
        assert_eq!(
            list_default(&workspace, json!({})),
            refused(Refusal::WorkspaceChanged)
        );
        assert_eq!(
            search_default(&workspace, json!({"name": "other"})),
            refused(Refusal::WorkspaceChanged)
        );
    }

    #[test]
    fn a_listing_is_bounded_and_says_so() {
        let (_guard, root, _data, directory) = workspace("list-bounds");
        for index in 0..250 {
            write(&root, &format!("many/file-{index:03}.txt"), b"x");
        }
        let long = "n".repeat(200);
        for index in 0..60 {
            write(&root, &format!("long/{long}-{index:02}"), b"x");
        }
        let workspace = authorize(&root, &directory);
        let limits = DiscoveryLimits::default();
        let result = list_default(&workspace, json!({"path": "many"})).expect("listed");
        let entries = result["entries"].as_array().expect("entries");
        assert!(entries.len() > 100 && entries.len() <= limits.listed_entries);
        for (index, entry) in entries.iter().enumerate() {
            assert_eq!(
                entry["name"],
                format!("file-{index:03}.txt"),
                "the first names, in order"
            );
        }
        assert_eq!(result["total_entries"], 250);
        assert_eq!(result["truncated"], true);
        assert!(result.to_string().len() <= limits.result_bytes);
        let fifty = DiscoveryLimits {
            listed_entries: 50,
            ..limits
        };
        let result = listing(&workspace, fifty, json!({"path": "many"})).expect("listed");
        assert_eq!(result["entries"].as_array().expect("entries").len(), 50);
        assert_eq!(result["entries"][49]["name"], "file-049.txt");
        assert_eq!(result["truncated"], true);

        let result = list_default(&workspace, json!({"path": "long"})).expect("listed");
        assert!(result.to_string().len() <= limits.result_bytes);
        assert_eq!(result["truncated"], true);
        assert!(result["entries"].as_array().expect("entries").len() < 60);

        // Names beyond what one directory may be read for are not seen.
        let few = DiscoveryLimits {
            directory_entries: 10,
            ..limits
        };
        let result = listing(&workspace, few, json!({"path": "many"})).expect("listed");
        assert_eq!(result["total_entries"], 10);
        assert_eq!(result["truncated"], true);
    }

    #[test]
    fn files_are_found_by_name_in_order_without_following_links() {
        let (_guard, root, _data, directory) = workspace("names");
        let (_outside_guard, outside, _outside_data, _) = workspace("names-outside");
        project(&root, &outside, &directory);
        let workspace = authorize(&root, &directory);
        let found = |arguments: Value| search_default(&workspace, arguments).expect("searched");

        let result = found(json!({"name": "*.rs"}));
        assert_eq!(
            result,
            json!({
                "path": ".",
                "files": [
                    {"path": "src/config.rs", "bytes": 77},
                    {"path": "src/lib.rs", "bytes": 16},
                    {"path": "src/nested/deep/mod.rs", "bytes": 28},
                ],
                "complete": true,
                "limit": null,
                "skipped": 0,
            }),
            "no link, hidden name, target or node_modules is searched"
        );
        assert_eq!(paths(&found(json!({"name": "CONFIG"}))), ["src/config.rs"]);
        assert_eq!(paths(&found(json!({"name": "?ib.r?"}))), ["src/lib.rs"]);
        assert_eq!(paths(&found(json!({"name": "*"}))).len(), 6);
        assert_eq!(
            paths(&found(json!({"name": "secret"}))),
            Vec::<&str>::new(),
            "neither the protected file nor the linked one is found"
        );
        assert_eq!(
            paths(&found(json!({"name": "*.rs", "path": "target"}))),
            ["target/debug/out.rs"],
            "a skipped directory is searched when named"
        );
        assert_eq!(
            paths(&found(json!({"name": "mod.rs", "path": "src/nested"}))),
            ["src/nested/deep/mod.rs"]
        );
        for (path, refusal) in [
            ("outside", Refusal::SymbolicLink),
            (".git", Refusal::Protected),
            ("../names-outside-workspace", Refusal::InvalidPath),
            ("missing", Refusal::NotFound),
        ] {
            assert_eq!(
                search_default(&workspace, json!({"name": "*", "path": path})),
                refused(refusal),
                "{path}"
            );
        }
    }

    #[test]
    fn wildcards_match_whole_names() {
        for (pattern, name, matches) in [
            ("*.rs", "lib.rs", true),
            ("*.rs", "lib.rsx", false),
            ("lib*", "lib.rs", true),
            ("*", "", true),
            ("a*b*c", "aXbYc", true),
            ("a*b*c", "aXbYcZ", false),
            ("?", "ab", false),
            ("*a*a*a*a*b", &"a".repeat(255), false),
            ("Ü*", "ü", false),
        ] {
            assert_eq!(
                NamePattern::new(pattern).matches(name),
                matches,
                "{pattern} {name}"
            );
        }
        assert!(NamePattern::new("Lib").matches("my_LIB.rs"));
    }

    #[test]
    fn content_is_found_with_its_line_numbers_and_nothing_protected() {
        let (_guard, root, _data, directory) = workspace("content");
        let (_outside_guard, outside, _outside_data, _) = workspace("content-outside");
        project(&root, &outside, &directory);
        write(&root, "src/many.rs", "RETRY_LIMIT\n".repeat(12).as_bytes());
        write(
            &root,
            "src/long.rs",
            format!("{}RETRY_LIMIT{}\r\n", "é".repeat(150), "x".repeat(300)).as_bytes(),
        );
        write(&root, "src/latin1.rs", b"RETRY_LIMIT caf\xe9\n");
        std::fs::write(directory.key_path(), b"RETRY_LIMIT key").expect("write a key");
        std::fs::hard_link(directory.key_path(), root.join("src/planted.rs")).expect("hard link");
        let workspace = authorize(&root, &directory);
        let result = search_default(&workspace, json!({"text": "RETRY_LIMIT"})).expect("searched");
        let long = result["files"][1]["matches"][0]["text"]
            .as_str()
            .expect("text")
            .to_owned();
        assert!(long.len() <= MAX_LINE_TEXT_BYTES && long.starts_with("éé"));
        assert_eq!(
            result,
            json!({
                "path": ".",
                "files": [
                    {
                        "path": "src/config.rs",
                        "bytes": 77,
                        "matches": [
                            {"line": 1, "text": "pub const RETRY_LIMIT: u32 = 7;"},
                            {"line": 4, "text": "    RETRY_LIMIT"},
                        ],
                        "more_matches": false,
                    },
                    {
                        "path": "src/long.rs",
                        "bytes": 613,
                        "matches": [{"line": 1, "text": long}],
                        "more_matches": false,
                    },
                    {
                        "path": "src/many.rs",
                        "bytes": 144,
                        "matches": (1..=10)
                            .map(|line| json!({"line": line, "text": "RETRY_LIMIT"}))
                            .collect::<Vec<_>>(),
                        "more_matches": true,
                    },
                    {
                        "path": "src/nested/deep/mod.rs",
                        "bytes": 28,
                        "matches": [{"line": 1, "text": "// RETRY_LIMIT is in config"}],
                        "more_matches": false,
                    },
                ],
                "complete": true,
                "limit": null,
                // The PNG, the Latin-1 file and the hard-linked key.
                "skipped": 3,
            })
        );
        let text = result.to_string();
        for secret in [
            "secret",
            "outside",
            "key",
            ".env",
            "pem",
            "yaml",
            "target",
            "node_modules",
        ] {
            assert!(!text.contains(secret), "{secret} is never shown");
        }

        // Name and text together find files that match both.
        let both = search_default(&workspace, json!({"text": "RETRY_LIMIT", "name": "config"}))
            .expect("searched");
        assert_eq!(paths(&both), ["src/config.rs"]);
        let none = search_default(&workspace, json!({"text": "retry_limit"})).expect("searched");
        assert_eq!(none["files"], json!([]), "text is matched with its case");
        assert_eq!(none["complete"], true);
    }

    #[test]
    fn a_search_stops_at_each_of_its_bounds_and_says_which() {
        let (_guard, root, _data, directory) = workspace("bounds");
        for index in 0..40 {
            write(&root, &format!("d{index:02}/f.txt"), b"needle\n");
        }
        write(&root, "a/b/c/d/e/deep.txt", b"needle\n");
        write(&root, "big.txt", &[b'n'; 4096]);
        let workspace = authorize(&root, &directory);
        let limits = DiscoveryLimits::default();
        let outcome = |limits: DiscoveryLimits, arguments: Value| {
            let result = searching(&workspace, limits, arguments).expect("searched");
            assert!(result.to_string().len() <= limits.result_bytes);
            result
        };
        let text = json!({"text": "needle"});

        let result = outcome(limits, text.clone());
        assert_eq!(
            (&result["complete"], &result["skipped"]),
            (&json!(true), &json!(0))
        );
        assert_eq!(paths(&result).len(), 41);

        let result = outcome(
            DiscoveryLimits {
                entries: 6,
                ..limits
            },
            text.clone(),
        );
        assert_eq!(
            (&result["complete"], &result["limit"]),
            (&json!(false), &json!("entries"))
        );
        assert_eq!(
            paths(&result),
            ["a/b/c/d/e/deep.txt"],
            "the first entries, in order"
        );

        let result = outcome(DiscoveryLimits { depth: 2, ..limits }, text.clone());
        assert_eq!(result["complete"], true);
        assert_eq!(result["skipped"], 1, "the directory below the depth");
        assert!(!paths(&result).contains(&"a/b/c/d/e/deep.txt"));

        let result = outcome(
            DiscoveryLimits {
                content_bytes: 20,
                ..limits
            },
            text.clone(),
        );
        assert_eq!(result["limit"], "content_bytes");
        assert_eq!(
            paths(&result),
            ["a/b/c/d/e/deep.txt"],
            "big.txt, next in order, would read past the bound"
        );

        let result = outcome(
            DiscoveryLimits {
                file_bytes: 100,
                ..limits
            },
            json!({"text": "nnn"}),
        );
        assert_eq!(result["files"], json!([]));
        assert_eq!(result["skipped"], 1, "the file over the limit is not read");

        let result = outcome(
            DiscoveryLimits {
                result_bytes: 1024,
                ..limits
            },
            text.clone(),
        );
        assert_eq!(result["limit"], "results");
        assert!(!paths(&result).is_empty() && paths(&result).len() < 41);

        let result = outcome(
            DiscoveryLimits {
                time: Duration::ZERO,
                ..limits
            },
            text.clone(),
        );
        assert_eq!(
            (&result["limit"], &result["files"]),
            (&json!("time"), &json!([]))
        );

        let result = outcome(
            DiscoveryLimits {
                directory_entries: 3,
                ..limits
            },
            json!({"name": "*"}),
        );
        assert!(
            result["skipped"].as_u64().expect("a count") >= 1,
            "names not read are counted"
        );
    }

    #[test]
    fn invalid_search_arguments_are_refused_by_the_schema_or_the_handler() {
        let (_guard, root, _data, directory) = workspace("arguments");
        write(&root, "file.txt", b"text\n");
        let workspace = authorize(&root, &directory);
        let validator = compile_parameters(&search_parameters()).expect("an enforceable schema");
        let invalid = QueryRefusal::Invalid.to_string();
        // The schema cannot say that one of two fields is needed without
        // a top-level combinator; the handler says it.
        for arguments in [json!({}), json!({"path": "."})] {
            assert!(validator.is_valid(&arguments));
            assert_eq!(
                search_default(&workspace, arguments),
                Err(ToolFailure::not_applied(invalid.clone()))
            );
        }
        for arguments in [
            json!({"name": ""}),
            json!({"text": ""}),
            json!({"name": 3}),
            json!({"text": ["a"]}),
            json!({"text": "a".repeat(MAX_TEXT_BYTES + 1)}),
            json!({"name": "a".repeat(MAX_NAME_PATTERN_BYTES + 1)}),
            json!({"name": "*", "regex": ".*"}),
            json!({"name": "*", "path": ["."]}),
        ] {
            assert!(
                !validator.is_valid(&arguments),
                "the schema refuses {arguments}"
            );
            assert!(search_default(&workspace, arguments).is_err());
        }
        for arguments in [
            json!({"name": "src/*.rs"}),
            json!({"name": "a\u{7}"}),
            json!({"text": "two\nlines"}),
            json!({"text": "nul\u{0}"}),
        ] {
            assert_eq!(
                search_default(&workspace, arguments.clone()),
                Err(ToolFailure::not_applied(invalid.clone())),
                "{arguments}"
            );
        }
        let validator = compile_parameters(&list_parameters()).expect("an enforceable schema");
        for arguments in [json!({"path": 1}), json!({"path": ".", "recursive": true})] {
            assert!(!validator.is_valid(&arguments), "{arguments}");
        }
    }

    #[test]
    fn a_dropped_search_call_stops_its_search() {
        let (_guard, root, _data, directory) = workspace("dropped");
        for index in 0..20 {
            write(&root, &format!("f{index:02}.txt"), b"needle\n");
        }
        let workspace = authorize(&root, &directory);
        let (started, entered) = flume::bounded(1);
        let (stopped, seen) = flume::bounded(1);
        let examined = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = examined.clone();
        // At its first entry, the search waits until it is told to stop.
        let probe: Probe = Arc::new(move |stop: &AtomicBool| {
            if counted.fetch_add(1, Ordering::Relaxed) > 0 {
                return;
            }
            started.send(()).ok();
            let deadline = Instant::now() + WAIT;
            while !stop.load(Ordering::Relaxed) && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(1));
            }
            stopped.send(stop.load(Ordering::Relaxed)).ok();
        });
        let handler = handler(&workspace, DiscoveryLimits::default(), Some(probe));
        on_ntex(async move {
            let call = handler.call(json!({"text": "needle"}));
            match ntex::util::select(call, entered.recv_async()).await {
                ntex::util::Either::Left(ended) => {
                    panic!("the search cannot end by itself: {ended:?}")
                }
                // The call is dropped here, as a time limit or cancellation drops it.
                ntex::util::Either::Right(_) => {}
            }
        });
        assert_eq!(
            seen.recv_timeout(WAIT),
            Ok(true),
            "the search was told to stop"
        );
        // It ends there, rather than walking the other entries.
        thread::sleep(Duration::from_millis(50));
        assert_eq!(examined.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn a_stopped_search_examines_nothing_more() {
        let (_guard, root, _data, directory) = workspace("stopped");
        write(&root, "a.txt", b"needle\n");
        let workspace = authorize(&root, &directory);
        let query = query(&json!({"text": "needle"})).expect("a query");
        let stop = AtomicBool::new(true);
        let result =
            search(&workspace, &query, &DiscoveryLimits::default(), &stop, None).expect("searched");
        assert_eq!(
            (&result["limit"], &result["files"]),
            (&json!("stopped"), &json!([]))
        );
    }

    #[test]
    fn the_tools_are_read_only_and_registered_unauthorized() {
        let (_guard, root, _data, directory) = workspace("registration");
        let workspace = authorize(&root, &directory);
        let registry = ToolRegistry::default();
        register(&registry, workspace.clone()).expect("register");
        for name in [LIST_TOOL_NAME, SEARCH_TOOL_NAME] {
            assert!(
                matches!(
                    registry.offer(&[name]),
                    Err(ToolRegistryError::Unauthorized(_))
                ),
                "registering authorizes nothing"
            );
        }
        let list = ListDirectory {
            workspace: workspace.clone(),
            limits: DiscoveryLimits::default(),
        };
        let search = handler(&workspace, DiscoveryLimits::default(), None);
        for (handler, version) in [
            (&list as &dyn ToolHandler, "workspace_list_directory-1"),
            (&search, "workspace_search-1"),
        ] {
            assert_eq!(handler.effect(), ToolEffect::ReadOnly);
            assert_eq!(handler.version(), version);
            assert!(crate::persistence::is_handler_version(version));
        }
        let limits = DiscoveryLimits::default();
        assert!(
            limits.result_bytes
                < crate::orchestration::execution_runtime::ToolLoopLimits::default()
                    .max_tool_result_bytes
        );
    }
}
