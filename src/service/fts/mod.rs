//! `workspace_fts_search`: indexed full-text search over an
//! authorized Workspace, and the bounded in-memory index behind it.
//!
//! The scan-based workspace_search remains available. Indexes are lazy,
//! immutable, and owned by their registered handler.
//!
//! # What searching means here
//!
//! Retrieval is lexical. There are no embeddings, no vectors and no
//! semantic similarity: only exact tokens, scored by Okapi BM25.
//!
//! Tokenization, stated exactly:
//!
//! * Text splits into words at every character that is not alphanumeric, so
//!   `_` separates and `job_admission` is two words.
//! * Each word splits at camelCase and acronym boundaries into subtokens:
//!   `JobScheduler` is `job` + `scheduler`, `HTTPServer` is `http` +
//!   `server`, `u64` stays `u64`.
//! * Every subtoken is lowercased, so matching is case-insensitive.
//! * A subtoken over [`MAX_TOKEN_BYTES`] is not indexed and never matches.
//!
//! A query word is tokenized the same way. One subtoken matches any
//! document holding it as a token; a word that becomes several subtokens,
//! and any `"quoted phrase"`, requires those subtokens as adjacent tokens
//! in order. Separate query words and phrases must all match (AND). This is
//! tokenized search, not substring search: `job` matches `Job` and the
//! `job` of `JobScheduler`, but not `jobless`, and `scheduler` does not
//! match `schedulers`. A phrase may span lines, because adjacency is over
//! the token stream of the whole file.
//!
//! # Freshness
//!
//! A first call builds synchronously from the caller's perspective; refresh
//! stages a full replacement while readers retain the preceding snapshot.
//! Generations fence obsolete and disposed builds. Known OCG mutations mark
//! snapshots dirty, including writes overlapping construction. No snapshot
//! proves the absence of external edits; every response states that limit.
//!
//! # Bounds and security
//!
//! The same protections as `workspace_search`: only the Workspace an
//! operator authorized (the Provider never names the root), no symlinks, no
//! hidden, secret-bearing or OCG-internal names, no OCG data files, only
//! UTF-8 text without NUL, opened component by component from the root,
//! never through a joined path. Everything is read within [`FtsLimits`],
//! which bounds files, content, tokens and time, and every build reports
//! what its bounds left out. The index lives in memory only: it is dropped
//! with its handler or on rebuild, and is never written to disk.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rustix::fs::{AtFlags, Dir, FileType};
use serde_json::{Value, json};
use std::os::unix::fs::MetadataExt;

use crate::db::entities::ToolEffect;
use crate::system::executor::blocking::{Kind, Permit};
use crate::system::executor::tools::{
    ToolFailure, ToolFuture, ToolHandler, ToolRegistry, ToolRegistryError,
};
use crate::system::executor::workspace_discovery::SKIPPED_DIRECTORY_NAMES;
use crate::system::executor::workspace_read::{
    MutationStamp, Refusal, Step, Workspace, check_component, mutation_stamp, open_within,
    read_bytes,
};

/// The name the tool is offered and recorded under.
pub const TOOL_NAME: &str = "workspace_fts_search";

/// Identifies this handler's behavior in every call it records.
const HANDLER_VERSION: &str = "workspace_fts_search-2";

/// The longest query, in bytes, and the most of a line an excerpt returns.
const MAX_QUERY_BYTES: usize = 256;
const MAX_EXCERPT_BYTES: usize = 200;

/// The longest run of letters and digits, in bytes, that a query may hold
/// or an index holds as one subtoken.
const MAX_TOKEN_BYTES: usize = 128;

/// The most query clauses and subtokens per clause.
const MAX_CLAUSES: usize = 8;
const MAX_CLAUSE_TOKENS: usize = 16;

/// Okapi BM25 parameters, the standard values Lucene and Tantivy use.
const BM25_K1: f64 = 1.2;
const BM25_B: f64 = 0.75;

/// The bounds of indexing and searching. Everything a build or a query may
/// read, hold or return is bounded here; a build that reaches one of the
/// build bounds installs a partial snapshot that says so.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FtsLimits {
    /// Names read from one directory; the rest are not seen.
    pub directory_entries: usize,
    /// How many directories deep below the root indexing goes.
    pub depth: usize,
    /// Entries one build examines.
    pub entries: usize,
    /// The largest file one build indexes.
    pub file_bytes: usize,
    /// Content bytes one snapshot holds in all.
    pub content_bytes: u64,
    /// Files one snapshot holds in all.
    pub files: usize,
    /// Subtokens one snapshot holds in all; bounds index memory together
    /// with [`Self::content_bytes`].
    pub tokens: u64,
    /// How long one build may walk.
    pub time: Duration,
    /// How long one query may run.
    pub query_time: Duration,
    /// Ranked documents one query may open to verify phrases and excerpt.
    pub query_candidates: usize,
    /// The most result records one query returns.
    pub results: usize,
    /// The largest serialized result of a query.
    pub result_bytes: usize,
    /// Result records returned for one file.
    pub matches_per_file: usize,
}

impl Default for FtsLimits {
    fn default() -> Self {
        Self {
            directory_entries: 10_000,
            depth: 16,
            entries: 20_000,
            file_bytes: 1024 * 1024,
            content_bytes: 16 * 1024 * 1024,
            files: 4_096,
            tokens: 4_000_000,
            time: Duration::from_secs(10),
            query_time: Duration::from_secs(2),
            query_candidates: 2_048,
            results: 50,
            result_bytes: 8 * 1024,
            matches_per_file: 3,
        }
    }
}

impl FtsLimits {
    fn bounded(mut self) -> Self {
        let maximum = Self::default();
        self.directory_entries = self.directory_entries.min(maximum.directory_entries);
        self.depth = self.depth.min(maximum.depth);
        self.entries = self.entries.min(maximum.entries);
        self.file_bytes = self.file_bytes.min(maximum.file_bytes);
        self.content_bytes = self.content_bytes.min(maximum.content_bytes);
        self.files = self.files.min(maximum.files);
        self.tokens = self.tokens.min(maximum.tokens);
        self.time = self.time.min(maximum.time);
        self.query_time = self.query_time.min(maximum.query_time);
        self.query_candidates = self.query_candidates.min(maximum.query_candidates);
        self.results = self.results.min(maximum.results);
        self.result_bytes = self.result_bytes.min(maximum.result_bytes);
        self.matches_per_file = self.matches_per_file.min(maximum.matches_per_file);
        self
    }
}

fn query_checkpoint(stop: &AtomicBool, deadline: Instant) -> Result<(), SearchRefusal> {
    if stop.load(Ordering::Relaxed) {
        return Err(SearchRefusal::Stopped);
    }
    if Instant::now() >= deadline {
        return Err(SearchRefusal::QueryTime);
    }
    Ok(())
}

/// What a build could not index, counted for its coverage report.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Skipped {
    /// Hidden, secret-bearing or OCG-internal names, and names no Workspace
    /// path could name.
    pub protected: usize,
    /// Symbolic links, never followed.
    pub symlink: usize,
    /// Not UTF-8 text, or text holding NUL.
    pub binary: usize,
    /// Regular files over [`FtsLimits::file_bytes`].
    pub too_large: usize,
    /// Files with more than one link.
    pub hard_linked: usize,
    /// Anything that could not be opened, read or examined.
    pub unreadable: usize,
    /// Entries that are neither a regular file nor a directory, including
    /// build output and installed dependencies directories.
    pub other: usize,
}

impl Skipped {
    fn to_json(self) -> Value {
        json!({
            "protected": self.protected,
            "symlink": self.symlink,
            "binary": self.binary,
            "too_large": self.too_large,
            "hard_linked": self.hard_linked,
            "unreadable": self.unreadable,
            "other": self.other,
        })
    }
}

/// Which bound ended a build before it walked everything.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BuildLimit {
    Entries,
    ContentBytes,
    Files,
    Tokens,
    Time,
    Depth,
    Unreadable,
}

impl BuildLimit {
    fn name(self) -> &'static str {
        match self {
            Self::Entries => "entries",
            Self::ContentBytes => "content_bytes",
            Self::Files => "files",
            Self::Tokens => "tokens",
            Self::Time => "time",
            Self::Depth => "depth",
            Self::Unreadable => "unreadable",
        }
    }
}

/// What a snapshot holds, honestly: how far its build walked and what its
/// bounds left out. `complete` is false whenever a bound ended the walk, so
/// a caller never mistakes a partial index for the whole Workspace.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Coverage {
    pub complete: bool,
    pub limit: Option<BuildLimit>,
    pub entries_examined: usize,
    pub files_indexed: usize,
    pub content_bytes_indexed: u64,
    pub tokens_indexed: u64,
    /// Distinct subtokens: the term dictionary's size.
    pub terms: u64,
    /// `(document, subtoken)` postings held.
    pub posting_entries: u64,
    pub skipped: Skipped,
}

/// What building an index did: which snapshot now exists, what it covers,
/// and what it occupies.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BuildReport {
    pub generation: u64,
    pub coverage: Coverage,
    /// What the snapshot occupies in memory, by the accounting of
    /// [`Index::memory_bytes_approx`]; not a measured allocation.
    pub memory_bytes_approx: u64,
}

/// Why a call produced no result. Messages are recorded and may reach the
/// Provider, so none repeats the query or anything read.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum SearchRefusal {
    #[error(transparent)]
    Query(#[from] QueryRefusal),
    #[error("no index of the Workspace is built yet")]
    NoIndex,
    #[error("the call was stopped before it finished")]
    Stopped,
    #[error("the index has been disposed")]
    Disposed,
    #[error("the build was superseded by a newer operation")]
    Superseded,
    #[error("the query reached its time limit")]
    QueryTime,
    #[error("the result envelope exceeds the serialized byte limit")]
    ResultBytes,
    #[error(transparent)]
    Access(#[from] Refusal),
}

/// Why a query is refused before it runs. Its message is recorded and may
/// reach the Provider, so it never repeats the query.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum QueryRefusal {
    #[error(
        "a search needs a `query` of 1 to {MAX_QUERY_BYTES} bytes on one line, made of words and \"phrases\"; at most {MAX_CLAUSES} of them, each of at most {MAX_CLAUSE_TOKENS} indexed words"
    )]
    Invalid,
    #[error(
        "a `query` run of letters and digits over {MAX_TOKEN_BYTES} bytes is never indexed, so it can never match"
    )]
    OversizedToken,
}

/// One indexed file: its path, the content held for excerpts and phrase
/// verification, the byte offset of every line, and its length in subtokens
/// for BM25.
#[derive(Debug)]
struct Document {
    path: String,
    text: String,
    line_starts: Vec<u32>,
    token_count: u32,
}

/// One `(document, subtoken)` posting: the document and the subtoken's term
/// frequency there, which is all BM25 needs.
#[derive(Clone, Copy, Debug)]
struct Posting {
    document: u32,
    frequency: u32,
}

/// The indexed snapshot of one build: immutable once installed, shared
/// between queries, and dropped when a rebuild replaces it or its handler
/// goes away.
#[derive(Debug)]
struct Index {
    generation: u64,
    built_at: SystemTime,
    documents: Vec<Document>,
    /// Subtoken to its postings, in ascending document order. A `BTreeMap`
    /// keeps every query and every report deterministic.
    postings: BTreeMap<String, Vec<Posting>>,
    total_tokens: u64,
    coverage: Coverage,
    mutation: MutationStamp,
    memory_estimate: u64,
}

impl Index {
    /// What the snapshot occupies: its documents (path, text, line offsets),
    /// its term dictionary and its postings, counted by their fields' own
    /// sizes and a fixed per-record allowance. A structural approximation,
    /// not a measured allocation; the formula is the point of the field.
    fn memory_bytes_approx(&self) -> u64 {
        let documents = self
            .documents
            .iter()
            .map(|document| {
                document.path.len() as u64
                    + document.text.len() as u64
                    + document.line_starts.len() as u64 * 4
                    + 64
            })
            .sum::<u64>();
        let terms = self
            .postings
            .iter()
            .map(|(term, postings)| term.len() as u64 + 48 + postings.len() as u64 * 8)
            .sum::<u64>();
        documents + terms
    }

    fn average_length(&self) -> f64 {
        if self.documents.is_empty() {
            return 0.0;
        }
        self.total_tokens as f64 / self.documents.len() as f64
    }
}

/// The installed snapshot: the latest generation number, and the snapshot
/// itself while one is installed.
#[derive(Default)]
struct Slot {
    generation: u64,
    index: Option<Arc<Index>>,
    building: Option<(u64, Arc<AtomicBool>)>,
    disposed: bool,
    refresh_failed: bool,
}

/// An index of one authorized Workspace. It builds, rebuilds, searches and
/// disposes snapshots of that Workspace only; the Workspace came from
/// trusted OCG code and nothing in a call may widen it. Calls share one
/// snapshot; the tool loop of one execution runs them one at a time.
pub struct WorkspaceFts {
    workspace: Workspace,
    limits: FtsLimits,
    slot: Mutex<Slot>,
}

/// Why a walk or scan stopped without finishing: the call was cancelled, so
/// nothing it produced is used or claimed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Stopped;

impl From<Stopped> for SearchRefusal {
    fn from(_stopped: Stopped) -> Self {
        Self::Stopped
    }
}

/// One parsed query clause: one or more adjacent subtokens. A single
/// subtoken is an ordinary term; more is an exact phrase, whether it came
/// from `"quotes"` or from one word like `JobScheduler`.
#[derive(Debug, PartialEq, Eq)]
struct Clause {
    subtokens: Vec<String>,
}

/// A query as it is searched: deduplicated clauses, all of which must match.
#[derive(Debug)]
struct ParsedQuery {
    clauses: Vec<Clause>,
    /// Every distinct subtoken of every clause, for the postings lookup and
    /// the BM25 sum.
    subtokens: Vec<String>,
}

/// One scanned subtoken: its lowercased form, the byte offset where it
/// starts in the text, and its line.
#[derive(Debug)]
struct ScannedToken<'query> {
    lower: &'query str,
    start: u32,
    line: u32,
    position: u32,
}

/// Every subtoken occurrence of one document, in stream order.
struct Scanned<'query> {
    tokens: Vec<ScannedToken<'query>>,
}

/// Splits `text` into lowercased subtokens and hands each to `sink` with its
/// byte range. Words split at every non-alphanumeric character, then at
/// camelCase and acronym boundaries; see the module documentation for the
/// exact rules this implements.
fn tokenize(text: &str, sink: &mut impl FnMut(&str, u32, u32)) {
    tokenize_while(text, sink, &mut || true);
}

fn tokenize_while(
    text: &str,
    sink: &mut impl FnMut(&str, u32, u32),
    keep_going: &mut impl FnMut() -> bool,
) {
    let mut word_start = None;
    let mut checked = 0;
    for (offset, character) in text.char_indices() {
        if offset >= checked {
            if !keep_going() {
                return;
            }
            checked = offset + 4096;
        }
        if character.is_alphanumeric() {
            word_start.get_or_insert(offset);
        } else if let Some(start) = word_start.take() {
            split_word(&text[start..offset], start as u32, sink);
        }
    }
    if let Some(start) = word_start {
        split_word(&text[start..], start as u32, sink);
    }
}

/// Splits one word into subtokens at case boundaries and hands each to
/// `sink` with its byte range in the text. An uppercase run belongs to one
/// subtoken unless it ends in an uppercase letter followed by a lowercase
/// one (`HTTPServer` is `HTTP` + `Server`); digits belong to the subtoken
/// beside them (`u64`).
fn split_word(word: &str, base: u32, sink: &mut impl FnMut(&str, u32, u32)) {
    let mut chars = word.char_indices().peekable();
    let Some((_, mut previous)) = chars.next() else {
        return;
    };
    let mut start = 0;
    while let Some((offset, character)) = chars.next() {
        let next = chars.peek().map(|(_, character)| *character);
        let boundary = character.is_uppercase()
            && (previous.is_lowercase()
                || previous.is_numeric()
                || (previous.is_uppercase() && next.is_some_and(char::is_lowercase)));
        if boundary {
            emit(&word[start..offset], base + start as u32, sink);
            start = offset;
        }
        previous = character;
    }
    emit(&word[start..], base + start as u32, sink);
}

/// Lowercases one subtoken into a buffer and hands it to `sink` with its
/// range. A subtoken over [`MAX_TOKEN_BYTES`] is dropped, never half-indexed.
fn emit(subtoken: &str, start: u32, sink: &mut impl FnMut(&str, u32, u32)) {
    if subtoken.is_empty() || subtoken.len() > MAX_TOKEN_BYTES {
        return;
    }
    if subtoken.is_ascii() && !subtoken.bytes().any(|byte| byte.is_ascii_uppercase()) {
        sink(subtoken, start, start + subtoken.len() as u32);
        return;
    }
    let mut buffer = String::with_capacity(subtoken.len());
    for character in subtoken.chars() {
        for lower in character.to_lowercase() {
            buffer.push(lower);
        }
    }
    sink(&buffer, start, start + subtoken.len() as u32);
}

/// Parses `query` into the clauses a search must all find. Quoted spans are
/// phrases; unquoted words are terms or, when a word becomes several
/// subtokens, phrases of those subtokens.
fn parse(query: &str) -> Result<ParsedQuery, QueryRefusal> {
    if query.is_empty() || query.len() > MAX_QUERY_BYTES || query.chars().any(char::is_control) {
        return Err(QueryRefusal::Invalid);
    }
    if query
        .split(|character: char| !character.is_alphanumeric())
        .any(|run| run.len() > MAX_TOKEN_BYTES)
    {
        return Err(QueryRefusal::OversizedToken);
    }
    let mut clauses = Vec::new();
    let mut quoted = false;
    let mut span_start = 0;
    let mut collecting = false;
    for (offset, character) in query.char_indices() {
        if character == '"' {
            if quoted {
                add_clause(query, span_start, offset, &mut clauses)?;
                collecting = false;
            } else {
                flush_word(query, span_start, offset, collecting, &mut clauses)?;
                span_start = offset + 1;
                collecting = true;
            }
            quoted = !quoted;
        } else if !quoted && character.is_whitespace() {
            flush_word(query, span_start, offset, collecting, &mut clauses)?;
            collecting = false;
        } else if !collecting {
            span_start = offset;
            collecting = true;
        }
    }
    if quoted {
        return Err(QueryRefusal::Invalid);
    }
    flush_word(query, span_start, query.len(), collecting, &mut clauses)?;
    clauses.sort_by(|left, right| left.subtokens.cmp(&right.subtokens));
    clauses.dedup();
    if clauses.is_empty() || clauses.len() > MAX_CLAUSES {
        return Err(QueryRefusal::Invalid);
    }
    let mut subtokens = Vec::new();
    for clause in &clauses {
        for subtoken in &clause.subtokens {
            if !subtokens.contains(subtoken) {
                subtokens.push(subtoken.clone());
            }
        }
    }
    Ok(ParsedQuery { clauses, subtokens })
}

/// Adds the unquoted word in `query` at `[start, end)`, when one was being
/// collected.
fn flush_word(
    query: &str,
    start: usize,
    end: usize,
    collecting: bool,
    clauses: &mut Vec<Clause>,
) -> Result<(), QueryRefusal> {
    if collecting && start < end {
        add_clause(query, start, end, clauses)?;
    }
    Ok(())
}

/// Adds the span of `query` at `[start, end)` as one clause: every subtoken
/// of the span, adjacent and in order.
fn add_clause(
    query: &str,
    start: usize,
    end: usize,
    clauses: &mut Vec<Clause>,
) -> Result<(), QueryRefusal> {
    let mut subtokens = Vec::new();
    tokenize(&query[start..end], &mut |lower, _, _| {
        subtokens.push(lower.to_owned());
    });
    if subtokens.is_empty() || subtokens.len() > MAX_CLAUSE_TOKENS {
        return Err(QueryRefusal::Invalid);
    }
    clauses.push(Clause { subtokens });
    Ok(())
}

/// What the entry `name` of `directory` is, examined without following a
/// link: a file of so many bytes, a directory, or nothing that may be
/// indexed.
enum Entry {
    File { stamp: FileStamp },
    Directory,
    Skip(Skipped),
}

fn entry(directory: &std::fs::File, name: &str) -> Entry {
    let unreadable = || {
        Entry::Skip(Skipped {
            unreadable: 1,
            ..Skipped::default()
        })
    };
    let Ok(stat) = rustix::fs::statat(directory, name, AtFlags::SYMLINK_NOFOLLOW) else {
        return unreadable();
    };
    match FileType::from_raw_mode(stat.st_mode) {
        FileType::RegularFile if stat.st_nlink > 1 => Entry::Skip(Skipped {
            hard_linked: 1,
            ..Skipped::default()
        }),
        FileType::RegularFile => Entry::File {
            stamp: FileStamp {
                bytes: u64::try_from(stat.st_size).unwrap_or(u64::MAX),
                seconds: stat.st_mtime,
                nanos: stat.st_mtime_nsec as u32,
                ctime_seconds: stat.st_ctime,
                ctime_nanos: stat.st_ctime_nsec as u32,
                inode: stat.st_ino,
            },
        },
        FileType::Directory => Entry::Directory,
        FileType::Symlink => Entry::Skip(Skipped {
            symlink: 1,
            ..Skipped::default()
        }),
        _ => Entry::Skip(Skipped {
            other: 1,
            ..Skipped::default()
        }),
    }
}

/// The names of a directory that a Workspace path could name, sorted, with
/// the count of names it refused: hidden, secret-bearing, OCG-internal, or
/// not text a path could hold. `partial` says reading stopped at the limit.
fn names(directory: &std::fs::File, limit: usize) -> (Vec<String>, usize, Option<BuildLimit>) {
    let mut refused = 0;
    let Ok(mut entries) = Dir::read_from(directory) else {
        return (Vec::new(), 0, Some(BuildLimit::Unreadable));
    };
    let mut names = Vec::new();
    let mut read = 0;
    let mut omission = None;
    while let Some(entry) = entries.read() {
        let Ok(entry) = entry else {
            omission = Some(BuildLimit::Unreadable);
            break;
        };
        let name = entry.file_name().to_bytes();
        if name == b"." || name == b".." {
            continue;
        }
        if read == limit {
            omission = Some(BuildLimit::Entries);
            break;
        }
        read += 1;
        match std::str::from_utf8(name) {
            Ok(name) if check_component(name).is_ok() => names.push(name.to_owned()),
            _ => refused += 1,
        }
    }
    names.sort_unstable();
    (names, refused, omission)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct FileStamp {
    pub bytes: u64,
    pub seconds: i64,
    pub nanos: u32,
    pub ctime_seconds: i64,
    pub ctime_nanos: u32,
    pub inode: u64,
}

pub(super) trait WalkVisitor {
    fn checkpoint(&mut self) -> Result<(), Stopped>;
    fn examined(&mut self);
    fn skipped(&mut self, skipped: Skipped);
    fn omitted(&mut self, limit: BuildLimit);
    fn file(
        &mut self,
        directory: &std::fs::File,
        name: &str,
        path: &str,
        stamp: FileStamp,
    ) -> Result<(), Stopped>;
}

pub(super) fn walk_directory(
    workspace: &Workspace,
    directory: &std::fs::File,
    path: &str,
    depth: usize,
    device: u64,
    limits: &FtsLimits,
    visitor: &mut impl WalkVisitor,
) -> Result<(), Stopped> {
    let (names, refused, omission) = names(directory, limits.directory_entries);
    visitor.skipped(Skipped {
        protected: refused,
        ..Skipped::default()
    });
    if let Some(omission) = omission {
        visitor.omitted(omission);
        if omission == BuildLimit::Unreadable {
            visitor.skipped(Skipped {
                unreadable: 1,
                ..Skipped::default()
            });
        }
    }
    for name in &names {
        visitor.checkpoint()?;
        visitor.examined();
        let path = if path == "." {
            name.clone()
        } else {
            format!("{path}/{name}")
        };
        match entry(directory, name) {
            Entry::Skip(skipped) => {
                visitor.skipped(skipped);
            }
            Entry::File { stamp } => visitor.file(directory, name, &path, stamp)?,
            Entry::Directory => {
                if SKIPPED_DIRECTORY_NAMES.contains(&name.as_str()) {
                    visitor.skipped(Skipped {
                        other: 1,
                        ..Skipped::default()
                    });
                    continue;
                }
                if depth == limits.depth {
                    visitor.omitted(BuildLimit::Depth);
                    continue;
                }
                let entered = workspace
                    .subdirectory(directory, name)
                    .ok()
                    .filter(|child| {
                        child
                            .metadata()
                            .is_ok_and(|metadata| metadata.dev() == device)
                    });
                match entered {
                    Some(child) => walk_directory(
                        workspace,
                        &child,
                        &path,
                        depth + 1,
                        device,
                        limits,
                        visitor,
                    )?,
                    None => visitor.skipped(Skipped {
                        unreadable: 1,
                        ..Skipped::default()
                    }),
                }
            }
        }
    }
    Ok(())
}

impl WalkVisitor for Build<'_> {
    fn checkpoint(&mut self) -> Result<(), Stopped> {
        self.ended()
    }
    fn examined(&mut self) {
        self.coverage.entries_examined += 1;
    }
    fn skipped(&mut self, skipped: Skipped) {
        self.coverage.skipped.protected += skipped.protected;
        self.coverage.skipped.symlink += skipped.symlink;
        self.coverage.skipped.binary += skipped.binary;
        self.coverage.skipped.too_large += skipped.too_large;
        self.coverage.skipped.hard_linked += skipped.hard_linked;
        self.coverage.skipped.unreadable += skipped.unreadable;
        self.coverage.skipped.other += skipped.other;
    }
    fn omitted(&mut self, limit: BuildLimit) {
        self.omission = Some(limit);
    }
    fn file(
        &mut self,
        directory: &std::fs::File,
        name: &str,
        path: &str,
        stamp: FileStamp,
    ) -> Result<(), Stopped> {
        Build::file(self, directory, name, path, stamp.bytes)
    }
}

/// One build in progress: its budgets, its coverage so far, and the
/// documents and postings it has gathered.
struct Build<'a> {
    limits: &'a FtsLimits,
    stop: &'a AtomicBool,
    obsolete: &'a AtomicBool,
    deadline: Instant,
    device: u64,
    coverage: Coverage,
    documents: Vec<Document>,
    postings: BTreeMap<String, Vec<Posting>>,
    limit: Option<BuildLimit>,
    omission: Option<BuildLimit>,
}

impl Build<'_> {
    /// Whether the build must end now, recording why. A stop is not a
    /// coverage limit: a stopped build installs nothing.
    fn ended(&mut self) -> Result<(), Stopped> {
        if self.limit.is_some() {
            return Err(Stopped);
        }
        if self.stop.load(Ordering::Relaxed) || self.obsolete.load(Ordering::Relaxed) {
            return Err(Stopped);
        }
        if Instant::now() >= self.deadline {
            self.limit = Some(BuildLimit::Time);
        } else if self.coverage.entries_examined == self.limits.entries {
            self.limit = Some(BuildLimit::Entries);
        } else if self.coverage.files_indexed == self.limits.files {
            self.limit = Some(BuildLimit::Files);
        }
        if self.limit.is_some() {
            Err(Stopped)
        } else {
            Ok(())
        }
    }

    /// Indexes `directory`, at `path` and `depth` directories below the
    /// root, in the order of its names. Every entry is examined before it is
    /// read, and every read is within the budgets above.
    fn directory(
        &mut self,
        workspace: &Workspace,
        directory: &std::fs::File,
        path: &str,
        depth: usize,
    ) -> Result<(), Stopped> {
        walk_directory(
            workspace,
            directory,
            path,
            depth,
            self.device,
            self.limits,
            self,
        )
    }

    /// Indexes the file `name` of `directory` at `path`, of `bytes` bytes as
    /// it was examined, or counts why it was not indexed.
    fn file(
        &mut self,
        directory: &std::fs::File,
        name: &str,
        path: &str,
        bytes: u64,
    ) -> Result<(), Stopped> {
        if bytes > self.limits.file_bytes as u64 {
            self.coverage.skipped.too_large += 1;
            return Ok(());
        }

        let content = open_within(directory, name, Step::File, self.limits.file_bytes)
            .and_then(|file| read_bytes(&file, self.limits.file_bytes));
        let content = match content {
            Ok(content) => content,
            Err(Refusal::TooLarge { .. } | Refusal::TooLargeForRange) => {
                self.coverage.skipped.too_large += 1;
                return Ok(());
            }
            Err(Refusal::HardLinked) => {
                self.coverage.skipped.hard_linked += 1;
                return Ok(());
            }
            Err(Refusal::SymbolicLink | Refusal::NotRegularFile) => {
                self.coverage.skipped.symlink += 1;
                return Ok(());
            }
            Err(_) => {
                self.coverage.skipped.unreadable += 1;
                return Ok(());
            }
        };
        let text = match String::from_utf8(content) {
            Ok(text) if !text.contains('\0') => text,
            _ => {
                self.coverage.skipped.binary += 1;
                return Ok(());
            }
        };
        // Only securely read, indexable content consumes the snapshot budget;
        // directory metadata can be stale and excluded binary files consume none.
        if self
            .coverage
            .content_bytes_indexed
            .saturating_add(text.len() as u64)
            > self.limits.content_bytes
        {
            self.limit = Some(BuildLimit::ContentBytes);
            return Err(Stopped);
        }

        let mut frequencies: BTreeMap<String, u32> = BTreeMap::new();
        let mut token_count = 0u32;
        let mut interrupted = false;
        tokenize_while(
            &text,
            &mut |lower, _, _| {
                if interrupted {
                    return;
                }
                if token_count.is_multiple_of(256)
                    && (self.stop.load(Ordering::Relaxed)
                        || self.obsolete.load(Ordering::Relaxed)
                        || Instant::now() >= self.deadline)
                {
                    interrupted = true;
                    return;
                }
                token_count += 1;
                match frequencies.get_mut(lower) {
                    Some(frequency) => *frequency += 1,
                    None => {
                        frequencies.insert(lower.to_owned(), 1);
                    }
                }
            },
            &mut || {
                !self.stop.load(Ordering::Relaxed)
                    && !self.obsolete.load(Ordering::Relaxed)
                    && Instant::now() < self.deadline
            },
        );
        if self.stop.load(Ordering::Relaxed)
            || self.obsolete.load(Ordering::Relaxed)
            || Instant::now() >= self.deadline
        {
            interrupted = true;
        }
        if interrupted {
            if Instant::now() >= self.deadline {
                self.limit = Some(BuildLimit::Time);
            }
            return Err(Stopped);
        }
        if self
            .coverage
            .tokens_indexed
            .saturating_add(u64::from(token_count))
            > self.limits.tokens
        {
            self.limit = Some(BuildLimit::Tokens);
            return Err(Stopped);
        }
        let mut line_starts = vec![0u32];
        for (offset, _) in text.match_indices('\n') {
            line_starts.push(offset as u32 + 1);
        }
        let document = self.coverage.files_indexed as u32;
        for (term, frequency) in frequencies {
            self.postings.entry(term).or_default().push(Posting {
                document,
                frequency,
            });
            self.coverage.posting_entries += 1;
        }
        self.coverage.files_indexed += 1;
        self.coverage.content_bytes_indexed += text.len() as u64;
        self.coverage.tokens_indexed += u64::from(token_count);
        self.documents.push(Document {
            path: path.to_owned(),
            text,
            line_starts,
            token_count,
        });
        Ok(())
    }
}

/// Walks the Workspace under `limits` until it is walked or a bound ends it,
/// gathering one snapshot's documents and postings. A stop returns
/// [`SearchRefusal::Stopped`] and no partial state escapes it; a bound
/// returns an honest partial snapshot.
fn build_index(
    fts: &WorkspaceFts,
    stop: &AtomicBool,
    deadline: Instant,
    generation: u64,
    obsolete: &AtomicBool,
    mutation: MutationStamp,
) -> Result<Index, SearchRefusal> {
    let workspace = &fts.workspace;
    let limits = &fts.limits;
    let root = workspace.open_root()?;
    let device = root.metadata().map_err(|_| Refusal::Unreadable)?.dev();
    let mut build = Build {
        limits,
        stop,
        obsolete,
        deadline,
        device,
        coverage: Coverage {
            complete: true,
            limit: None,
            entries_examined: 0,
            files_indexed: 0,
            content_bytes_indexed: 0,
            tokens_indexed: 0,
            terms: 0,
            posting_entries: 0,
            skipped: Skipped::default(),
        },
        documents: Vec::new(),
        postings: BTreeMap::new(),
        limit: None,
        omission: None,
    };
    let walked = build.directory(workspace, &root, ".", 0);
    let limit = build.limit;
    if walked.is_err() && limit.is_none() {
        return Err(SearchRefusal::Stopped);
    }
    if stop.load(Ordering::Relaxed) || obsolete.load(Ordering::Relaxed) {
        return Err(SearchRefusal::Stopped);
    }
    let limit = limit
        .or(build.omission)
        .or((build.coverage.skipped.unreadable != 0).then_some(BuildLimit::Unreadable));
    build.coverage.complete = limit.is_none();
    build.coverage.limit = limit;
    build.coverage.terms = build.postings.len() as u64;
    let total_tokens = build.coverage.tokens_indexed;
    let mut index = Index {
        generation,
        built_at: SystemTime::now(),
        documents: build.documents,
        postings: build.postings,
        total_tokens,
        coverage: build.coverage,
        mutation,
        memory_estimate: 0,
    };
    index.memory_estimate = index.memory_bytes_approx();
    Ok(index)
}

/// Which bound ended a query before it found everything it could.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum QueryLimit {
    Results,
    ResultBytes,
    Candidates,
    MatchesPerFile,
}

impl QueryLimit {
    fn name(self) -> &'static str {
        match self {
            Self::Results => "results",
            Self::ResultBytes => "result_bytes",
            Self::Candidates => "candidates",
            Self::MatchesPerFile => "matches_per_file",
        }
    }
}

/// One result record before it is serialized: the line a clause matched,
/// what matched it, and the score of its file.
struct Hit {
    path: String,
    line: u32,
    excerpt: String,
    excerpt_truncated: bool,
    excerpt_offset: usize,
    terms: Vec<String>,
    offset: u32,
    score: f64,
}

impl Hit {
    fn to_json(&self) -> Value {
        json!({
            "path": self.path,
            "line": self.line,
            "excerpt": self.excerpt,
            "excerpt_truncated": self.excerpt_truncated,
            "excerpt_offset": self.excerpt_offset,
            "terms": self.terms,
            "offset": self.offset,
            "score": (self.score * 10_000.0).round() / 10_000.0,
        })
    }
}

/// The outcome of one query over one snapshot.
struct Answer {
    hits: Vec<Hit>,
    files_matched: usize,
    limit: Option<QueryLimit>,
}

/// Searches `index` for `query`, returning at most `limits.results` records
/// within `budget` serialized bytes, ranked by BM25 score with the path and
/// line breaking ties deterministically. Every bound that ends the search
/// early is named in its result.
fn search_index(
    index: &Index,
    query: &ParsedQuery,
    limits: &FtsLimits,
    budget: usize,
    stop: &AtomicBool,
    deadline: Instant,
) -> Result<Answer, SearchRefusal> {
    let mut answer = Answer {
        hits: Vec::new(),
        files_matched: 0,
        limit: None,
    };
    let total = index.documents.len();
    let average = index.average_length();
    // The candidates are the documents holding every subtoken of the query,
    // each scored by the BM25 sum over those subtokens. The intersection
    // walks the postings in ascending document order, which the build kept.
    let mut candidates: Vec<(u32, f64)> = Vec::new();
    let mut seeded = false;
    for subtoken in &query.subtokens {
        query_checkpoint(stop, deadline)?;
        let Some(postings) = index.postings.get(subtoken) else {
            return Ok(answer);
        };
        let document_frequency = postings.len() as f64;
        let inverse =
            (1.0 + (total as f64 - document_frequency + 0.5) / (document_frequency + 0.5)).ln();
        let scored = |posting: &Posting| {
            let length = index.documents[posting.document as usize].token_count as f64;
            let frequency = f64::from(posting.frequency);
            inverse * (frequency * (BM25_K1 + 1.0))
                / (frequency + BM25_K1 * (1.0 - BM25_B + BM25_B * length / average.max(1.0)))
        };
        if !seeded {
            candidates = postings
                .iter()
                .map(|posting| (posting.document, scored(posting)))
                .collect();
            seeded = true;
            continue;
        }
        let mut intersected = Vec::new();
        let mut at = 0;
        for (document, score) in &mut candidates {
            while at < postings.len() && postings[at].document < *document {
                at += 1;
            }
            if at < postings.len() && postings[at].document == *document {
                *score += scored(&postings[at]);
                intersected.push((*document, *score));
            }
        }
        candidates = intersected;
        if candidates.is_empty() {
            return Ok(answer);
        }
    }
    candidates.sort_by(
        |(left_document, left_score), (right_document, right_score)| {
            right_score.total_cmp(left_score).then_with(|| {
                index.documents[*left_document as usize]
                    .path
                    .cmp(&index.documents[*right_document as usize].path)
            })
        },
    );
    // Documents are opened in rank order, and only as many as the result
    // needs or `query_candidates` allows, so a phrase query never opens the
    // whole Workspace's worth of candidates.
    let mut size = 0;
    let mut suppressed_lines = false;
    for (rank, (document, score)) in candidates.iter().enumerate() {
        if rank == limits.query_candidates {
            answer.limit = Some(QueryLimit::Candidates);
            break;
        }
        if answer.hits.len() == limits.results {
            answer.limit = Some(QueryLimit::Results);
            break;
        }
        query_checkpoint(stop, deadline)?;
        let document = &index.documents[*document as usize];
        let scanned = scan_document(document, query, stop, deadline)?;
        let mut lines = matching_lines(document, &scanned, query, stop, deadline)?;
        if lines.is_empty() {
            continue;
        }
        answer.files_matched += 1;
        lines.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));
        suppressed_lines |= lines.len() > limits.matches_per_file;
        for (_matches, line, offset, matched) in lines.into_iter().take(limits.matches_per_file) {
            let (excerpt, excerpt_truncated, excerpt_offset) = excerpt_of(document, line, offset);
            let hit = Hit {
                path: document.path.clone(),
                line: line + 1,
                excerpt,
                excerpt_truncated,
                excerpt_offset,
                terms: matched,
                offset,
                score: *score,
            };
            let cost = hit.to_json().to_string().len() + 1;
            if answer.hits.len() == limits.results {
                answer.limit = Some(QueryLimit::Results);
                break;
            }
            if size + cost > budget {
                answer.limit = Some(QueryLimit::ResultBytes);
                break;
            }
            size += cost;
            answer.hits.push(hit);
        }
        if answer.limit.is_some() {
            break;
        }
    }
    if answer.limit.is_none() && suppressed_lines {
        answer.limit = Some(QueryLimit::MatchesPerFile);
    }
    Ok(answer)
}

/// Every subtoken occurrence of `document`, with its line, in stream order.
/// Phrase adjacency is over `position`, so a phrase may span lines.
fn scan_document<'query>(
    document: &Document,
    query: &'query ParsedQuery,
    stop: &AtomicBool,
    deadline: Instant,
) -> Result<Scanned<'query>, SearchRefusal> {
    let mut tokens = Vec::new();
    let mut position = 0u32;
    let mut interrupted = false;
    tokenize_while(
        &document.text,
        &mut |lower, start, _end| {
            if interrupted {
                return;
            }
            if position.is_multiple_of(1024)
                && (stop.load(Ordering::Relaxed) || Instant::now() >= deadline)
            {
                interrupted = true;
                return;
            }
            // Keep only query terms. Positions preserve adjacency across omitted
            // tokens, without allocating a String and line lookup for every word.
            if let Some(term) = query.subtokens.iter().find(|term| *term == lower) {
                tokens.push(ScannedToken {
                    lower: term,
                    start,
                    line: line_of(&document.line_starts, start),
                    position,
                });
            }
            position += 1;
        },
        &mut || !stop.load(Ordering::Relaxed) && Instant::now() < deadline,
    );
    query_checkpoint(stop, deadline)?;
    Ok(Scanned { tokens })
}

/// The 1-based line holding the byte `offset`, through a document's stored
/// line offsets.
fn line_of(line_starts: &[u32], offset: u32) -> u32 {
    let index = line_starts.partition_point(|start| *start <= offset);
    index.saturating_sub(1) as u32
}

/// The lines of `document` its scanned tokens match: for each, how many
/// clause occurrences matched there, its number, the byte offset of the
/// earliest match within the line, and the distinct subtokens matched. A
/// clause matches where its subtokens are adjacent in the stream. Lines
/// come out in line order; the caller ranks them.
type MatchingLine = (usize, u32, u32, Vec<String>);

fn matching_lines(
    document: &Document,
    scanned: &Scanned<'_>,
    query: &ParsedQuery,
    stop: &AtomicBool,
    deadline: Instant,
) -> Result<Vec<MatchingLine>, SearchRefusal> {
    let mut by_line: BTreeMap<u32, (Vec<String>, u32, usize)> = BTreeMap::new();
    for clause in &query.clauses {
        let mut clause_matched = false;
        let width = clause.subtokens.len();
        for (number, window) in scanned.tokens.windows(width).enumerate() {
            if number.is_multiple_of(1024) {
                query_checkpoint(stop, deadline)?;
            }
            let matched = window.iter().zip(&clause.subtokens).enumerate().all(
                |(position, (token, subtoken))| {
                    token.lower == *subtoken
                        && token.position == window[0].position + position as u32
                },
            );
            if !matched {
                continue;
            }
            clause_matched = true;
            let line = window[0].line;
            let entry = by_line
                .entry(line)
                .or_insert((Vec::new(), window[0].start, 0));
            entry.1 = entry.1.min(window[0].start);
            entry.2 += 1;
            for subtoken in &clause.subtokens {
                if !entry.0.contains(subtoken) {
                    entry.0.push(subtoken.clone());
                }
            }
        }
        if !clause_matched {
            return Ok(Vec::new());
        }
    }
    Ok(by_line
        .into_iter()
        .map(|(line, (mut terms, start, matches))| {
            terms.sort();
            (
                matches,
                line,
                start - document.line_starts[line as usize],
                terms,
            )
        })
        .collect())
}

/// The line `line` of `document` as an excerpt: without its line end, cut to
/// [`MAX_EXCERPT_BYTES`] at a character boundary, with whether it was cut.
fn excerpt_of(document: &Document, line: u32, offset: u32) -> (String, bool, usize) {
    let start = document.line_starts[line as usize] as usize;
    let end = document
        .line_starts
        .get(line as usize + 1)
        .map_or(document.text.len(), |next| *next as usize - 1);
    let text = &document.text[start..end];
    let text = text.strip_suffix('\r').unwrap_or(text);
    // Leave room for the longest original subtoken, even at a late match.
    let window = if offset as usize > MAX_EXCERPT_BYTES - MAX_TOKEN_BYTES {
        text.ceil_char_boundary(
            (offset as usize).saturating_sub(MAX_EXCERPT_BYTES - MAX_TOKEN_BYTES),
        )
    } else {
        0
    };
    let end = text.floor_char_boundary((window + MAX_EXCERPT_BYTES).min(text.len()));
    (
        text[window..end].to_owned(),
        window != 0 || end != text.len(),
        window,
    )
}

impl WorkspaceFts {
    /// An index of `workspace` under `limits`. It holds no snapshot until
    /// [`Self::build`] installs one.
    pub fn new(workspace: Workspace, limits: FtsLimits) -> Self {
        Self {
            workspace,
            limits: limits.bounded(),
            slot: Mutex::new(Slot::default()),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Slot> {
        self.slot.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Builds a snapshot of the Workspace now and installs it, replacing
    /// and releasing any earlier one. A build a bound ended installs a
    /// partial snapshot whose coverage names the bound; a cancelled build
    /// installs nothing. This is the refresh path too: there is no
    /// incremental indexing, so a refresh rebuilds everything.
    ///
    /// # Errors
    ///
    /// [`SearchRefusal::Access`] when the Workspace root is no longer as it
    /// was authorized, [`SearchRefusal::Stopped`] when the build was
    /// cancelled.
    pub fn build(&self) -> Result<BuildReport, SearchRefusal> {
        self.build_with(&AtomicBool::new(false), Instant::now() + self.limits.time)
    }

    /// Replaces the installed snapshot with one built now: an explicit
    /// refresh after files changed. The old snapshot is released when
    /// nothing is reading it any more.
    ///
    /// # Errors
    ///
    /// As [`Self::build`].
    pub fn rebuild(&self) -> Result<BuildReport, SearchRefusal> {
        self.build()
    }

    /// [`Self::build`], with the cancellation and deadline a call carries.
    fn build_with(
        &self,
        stop: &AtomicBool,
        deadline: Instant,
    ) -> Result<BuildReport, SearchRefusal> {
        let mutation = mutation_stamp();
        let (generation, obsolete) = {
            let mut slot = self.lock();
            if slot.disposed {
                return Err(SearchRefusal::Disposed);
            }
            slot.generation += 1;
            if let Some((_, previous)) = slot.building.take() {
                previous.store(true, Ordering::Relaxed);
            }
            let generation = slot.generation;
            let obsolete = Arc::new(AtomicBool::new(false));
            slot.building = Some((generation, obsolete.clone()));
            (generation, obsolete)
        };
        let lease = BuildLease {
            fts: self,
            generation,
        };

        let mut index = build_index(self, stop, deadline, generation, &obsolete, mutation)?;

        self.workspace.open_root()?;
        if Instant::now() >= deadline {
            index.coverage.complete = false;
            index.coverage.limit = Some(BuildLimit::Time);
        }
        let report = BuildReport {
            generation,
            coverage: index.coverage,
            memory_bytes_approx: index.memory_estimate,
        };
        let mut slot = self.lock();
        if slot.disposed {
            return Err(SearchRefusal::Disposed);
        }
        if slot.generation != generation {
            return Err(SearchRefusal::Superseded);
        }
        if stop.load(Ordering::Relaxed) || obsolete.load(Ordering::Relaxed) {
            return Err(SearchRefusal::Stopped);
        }
        slot.index = Some(Arc::new(index));
        slot.building = None;
        slot.refresh_failed = false;
        drop(slot);
        drop(lease);
        Ok(report)
    }

    /// Searches the installed snapshot. The Provider never names the
    /// snapshot or the files it may see: every call searches the one OCG
    /// built, over the whole authorized Workspace.
    ///
    /// # Errors
    ///
    /// [`SearchRefusal::Query`] when the query cannot be searched,
    /// [`SearchRefusal::NoIndex`] when no snapshot is installed,
    /// [`SearchRefusal::Stopped`] when the call was cancelled.
    pub fn search(&self, query: &str, results: usize) -> Result<Value, SearchRefusal> {
        self.search_with(
            query,
            results,
            &AtomicBool::new(false),
            Instant::now() + self.limits.query_time,
        )
    }

    /// [`Self::search`], with the cancellation and deadline a call carries.
    fn search_with(
        &self,
        query: &str,
        results: usize,
        stop: &AtomicBool,
        deadline: Instant,
    ) -> Result<Value, SearchRefusal> {
        let parsed = parse(query)?;
        let index = {
            let slot = self.lock();
            if slot.disposed {
                return Err(SearchRefusal::Disposed);
            }
            slot.index.clone().ok_or(SearchRefusal::NoIndex)?
        };
        let mut limits = self.limits;
        limits.results = limits.results.min(results.max(1));
        self.workspace.open_root()?;
        if self.lock().disposed {
            return Err(SearchRefusal::Disposed);
        }

        let answer = search_index(
            &index,
            &parsed,
            &limits,
            limits.result_bytes,
            stop,
            deadline,
        )?;
        if stop.load(Ordering::Relaxed) {
            return Err(SearchRefusal::Stopped);
        }
        if Instant::now() >= deadline {
            return Err(SearchRefusal::QueryTime);
        }
        let mut value = envelope(&index, answer);
        let slot = self.lock();
        let dirty = index.mutation.is_dirty() || slot.refresh_failed;
        value["index"]["known_modifications"] = json!(dirty);
        value["index"]["freshness"] = json!(if dirty {
            "known_dirty"
        } else {
            "no_known_modifications"
        });
        value["index"]["external_modifications"] = json!("unverified");
        value["index"]["lifecycle"] = json!(if slot.disposed {
            "disposed"
        } else if slot.building.is_some() {
            "refreshing"
        } else if dirty {
            "known_dirty"
        } else {
            "available"
        });
        drop(slot);
        while value.to_string().len() > limits.result_bytes {
            if value["results"].as_array_mut().and_then(Vec::pop).is_none() {
                return Err(SearchRefusal::ResultBytes);
            }
            value["complete"] = json!(false);
            value["limit"] = json!("result_bytes");
        }
        query_checkpoint(stop, deadline)?;
        Ok(value)
    }

    /// The installed snapshot's generation, coverage and build time, and
    /// nothing of its content, or `None` while none is installed.
    pub fn coverage(&self) -> Option<(u64, Coverage, SystemTime)> {
        let slot = self.lock();
        let index = slot.index.as_ref()?;
        Some((index.generation, index.coverage, index.built_at))
    }

    /// Drops the installed snapshot and everything it holds. A query
    /// already reading the snapshot finishes with it.
    pub fn dispose(&self) {
        let mut slot = self.lock();
        slot.disposed = true;
        slot.generation += 1;
        if let Some((_, cancelled)) = slot.building.take() {
            cancelled.store(true, Ordering::Relaxed);
        }
        slot.index = None;
    }
}

struct BuildLease<'a> {
    fts: &'a WorkspaceFts,
    generation: u64,
}

impl Drop for BuildLease<'_> {
    fn drop(&mut self) {
        let mut slot = self.fts.lock();
        if slot
            .building
            .as_ref()
            .is_some_and(|(ticket, _)| *ticket == self.generation)
        {
            slot.building = None;
            slot.refresh_failed = true;
        }
    }
}

/// The result of a query: its records and limits, and the snapshot they
/// describe, said plainly.
fn envelope(index: &Index, answer: Answer) -> Value {
    let age = SystemTime::now()
        .duration_since(index.built_at)
        .unwrap_or(Duration::ZERO);
    json!({
        "results": answer.hits.iter().map(Hit::to_json).collect::<Vec<_>>(),
        "files_matched": answer.files_matched,
        "complete": answer.limit.is_none(),
        "limit": answer.limit.map(QueryLimit::name),
        "index": index_block(index, age),
    })
}

/// The snapshot a result describes, and how stale it may be.
fn index_block(index: &Index, age: Duration) -> Value {
    json!({
        "generation": index.generation,
        "built_at_unix_ms": index.built_at.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO).as_millis(),
        "age_seconds": age.as_secs(),
        "snapshot": format!(
            "results describe indexed snapshot generation {}, not necessarily the files as they are now; \
             call again with refresh to rebuild after edits",
            index.generation
        ),
        "files_indexed": index.coverage.files_indexed,
        "entries_examined": index.coverage.entries_examined,
        "content_bytes_indexed": index.coverage.content_bytes_indexed,
        "tokens_indexed": index.coverage.tokens_indexed,
        "terms_indexed": index.coverage.terms,
        "memory_bytes_approx": index.memory_estimate,
        "memory_accounting": "structural estimate; not measured allocations or RSS",
        "complete": index.coverage.complete,
        "limit": index.coverage.limit.map(BuildLimit::name),
        "skipped": index.coverage.skipped.to_json(),
    })
}

/// The parameters a search call must match exactly: the query, and bounds
/// below [`FtsLimits`] the call may tighten.
fn parameters() -> Value {
    json!({
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "minLength": 1,
                "maxLength": MAX_QUERY_BYTES,
                "description": "Words to find, all of which must match, case-insensitively, as whole words of the text: `job` matches `Job` and the `job` of `JobScheduler`, not `jobless`. A word of several parts, like `JobScheduler`, and a \"quoted phrase\" require those words adjacent in order. Results are ranked, not substring matches."
            },
            "limit": {
                "type": "integer",
                "minimum": 1,
                "maximum": 50,
                "description": "The most result records to return, 1 to 50; 10 when omitted"
            },
            "refresh": {
                "type": "boolean",
                "description": "Build a replacement snapshot before searching. Known OCG mutations overlapping construction leave it dirty; external edits remain unverified. Omitted or false reuses the installed snapshot."
            }
        },
        "required": ["query"],
        "additionalProperties": false
    })
}

/// The handler of `workspace_fts_search`, bound to one authorized Workspace
/// and one index of it. The index is built on the first call or a call that
/// asks to refresh, and reused for every call between.
struct FtsSearch {
    fts: Arc<WorkspaceFts>,
}

/// The parsed arguments of one call, checked again beyond the schema.
struct Call {
    query: String,
    results: usize,
    refresh: bool,
}

fn call_of(arguments: &Value) -> Result<Call, QueryRefusal> {
    let only_fields = arguments.as_object().is_some_and(|object| {
        object
            .keys()
            .all(|key| matches!(key.as_str(), "query" | "limit" | "refresh"))
    });
    if !only_fields {
        return Err(QueryRefusal::Invalid);
    }
    let query = arguments
        .get("query")
        .and_then(Value::as_str)
        .ok_or(QueryRefusal::Invalid)?;
    parse(query)?;
    let results = match arguments.get("limit") {
        None => 10,
        Some(limit) => limit
            .as_u64()
            .and_then(|limit| usize::try_from(limit).ok())
            .filter(|limit| (1..=50).contains(limit))
            .ok_or(QueryRefusal::Invalid)?,
    };
    let refresh = match arguments.get("refresh") {
        None => false,
        Some(refresh) => refresh.as_bool().ok_or(QueryRefusal::Invalid)?,
    };
    Ok(Call {
        query: query.to_owned(),
        results,
        refresh,
    })
}

/// What one worker thread does for one call: build or rebuild the snapshot
/// when the call asks for one, then search it. A build is installed only if
/// it finishes, so a stop leaves no half-built index behind.
fn run_call(fts: &WorkspaceFts, call: &Call, stop: &AtomicBool) -> Result<Value, SearchRefusal> {
    let installed = fts.lock().index.is_some();
    if call.refresh || !installed {
        fts.build_with(stop, Instant::now() + fts.limits.time)?;
    }
    fts.search_with(
        &call.query,
        call.results,
        stop,
        Instant::now() + fts.limits.query_time,
    )
}

impl ToolHandler for FtsSearch {
    fn version(&self) -> &str {
        HANDLER_VERSION
    }

    /// Searching changes nothing outside OCG.
    fn effect(&self) -> ToolEffect {
        ToolEffect::ReadOnly
    }

    /// Runs on a thread of its own and awaits its result. Dropping the
    /// call, as its time limit or cancellation does, stops the work and
    /// leaves no index a stopped call was building.
    fn call(&self, arguments: Value) -> ToolFuture<'_> {
        Box::pin(async move {
            let call = call_of(&arguments).map_err(refused)?;
            let stop = Arc::new(AtomicBool::new(false));
            let fts = self.fts.clone();
            let worker_fts = fts.clone();
            let worker_stop = stop.clone();
            let (report, reported) = flume::bounded(1);
            let worker = thread::Builder::new()
                .name("ocg-workspace-fts".to_owned())
                .spawn(move || {
                    let _permit = Permit::acquire(Kind::Cpu);
                    if report
                        .send(run_call(&worker_fts, &call, &worker_stop))
                        .is_err()
                    {
                        worker_stop.store(true, Ordering::Relaxed);
                    }
                })
                .map_err(|_| ToolFailure::not_applied("the search could not be started"))?;
            let stopping = StopOnDrop {
                stop,
                fts,
                worker: Some(worker),
            };
            let found = reported
                .recv_async()
                .await
                .map_err(|_| ToolFailure::not_applied("the search ended without a result"))?;
            drop(stopping);
            found.map_err(refused)
        })
    }
}

/// Tells a call to stop when it ends without its result, as when its limit
/// or cancellation drops it.
struct StopOnDrop {
    stop: Arc<AtomicBool>,
    fts: Arc<WorkspaceFts>,
    worker: Option<thread::JoinHandle<()>>,
}

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        {
            let _slot = self.fts.lock();
            self.stop.store(true, Ordering::Relaxed);
        }

        if let Some(worker) = self.worker.take() {
            let _ = thread::Builder::new()
                .name("ocg-workspace-fts-join".to_owned())
                .spawn(move || {
                    if worker.join().is_err() {
                        log::error!("workspace search worker panicked");
                    }
                });
        }
    }
}

impl Drop for FtsSearch {
    fn drop(&mut self) {
        self.fts.dispose();
    }
}

fn refused(refusal: impl std::fmt::Display) -> ToolFailure {
    ToolFailure::not_applied(refusal.to_string())
}

/// Registers `workspace_fts_search` for `workspace` in `registry`, without
/// authorizing it. Coding executions offer it by default; its first query
/// builds the bounded index, and refresh remains explicit.
pub(crate) fn register(
    registry: &ToolRegistry,
    workspace: Workspace,
) -> Result<(), ToolRegistryError> {
    registry.register(
        TOOL_NAME,
        Some(
            "Opt-in: find files of the authorized Workspace with an in-memory full-text index built once and reused. All `query` words must match as whole words, case-insensitively (this is word search, not substring search), and a \"quoted phrase\" or one word of several parts, like JobScheduler, must appear as adjacent words. Results are ranked by BM25 with the matching line and an excerpt; offsets are UTF-8 byte columns, and excerpt_offset locates the excerpt within that line. AND applies across each file and phrases may cross lines. files_matched counts verified matching candidates examined, not an uncapped total. The index is a snapshot: its generation, age and coverage are in every result, and `refresh` rebuilds it after edits. Known OCG writes and commands mark snapshots dirty; external edits remain unverified even after refresh. Hidden, secret-bearing, symlinked, binary and over 1 MiB files are not indexed, nor `target` or `node_modules` directories; `index.complete` is false when a bound left part of the Workspace out of the snapshot.",
        ),
        parameters(),
        Arc::new(FtsSearch {
            fts: Arc::new(WorkspaceFts::new(workspace, FtsLimits::default())),
        }),
    )
}
