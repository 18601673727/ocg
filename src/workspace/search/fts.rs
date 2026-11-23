use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rustix::fs::{AtFlags, Dir, FileType};
use serde_json::{Value, json};
use std::os::unix::fs::MetadataExt;

use crate::workspace::files::discovery::SKIPPED_DIRECTORY_NAMES;
use crate::workspace::files::read::{
    MutationStamp, Refusal, Step, Workspace, check_component, mutation_stamp, open_within,
    read_bytes,
};

pub const TOOL_NAME: &str = "workspace_fts_search";

const MAX_QUERY_BYTES: usize = 256;
const MAX_EXCERPT_BYTES: usize = 200;

const MAX_TOKEN_BYTES: usize = 128;

const MAX_CLAUSES: usize = 8;
const MAX_CLAUSE_TOKENS: usize = 16;

const BM25_K1: f64 = 1.2;
const BM25_B: f64 = 0.75;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FtsLimits {
    pub directory_entries: usize,
    pub depth: usize,
    pub entries: usize,
    pub file_bytes: usize,
    pub content_bytes: u64,
    pub files: usize,
    pub tokens: u64,
    pub time: Duration,
    pub query_time: Duration,
    pub query_candidates: usize,
    pub results: usize,
    pub result_bytes: usize,
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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Skipped {
    pub protected: usize,
    pub symlink: usize,
    pub binary: usize,
    pub too_large: usize,
    pub hard_linked: usize,
    pub unreadable: usize,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Coverage {
    pub complete: bool,
    pub limit: Option<BuildLimit>,
    pub entries_examined: usize,
    pub files_indexed: usize,
    pub content_bytes_indexed: u64,
    pub tokens_indexed: u64,
    pub terms: u64,
    pub posting_entries: u64,
    pub skipped: Skipped,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BuildReport {
    pub generation: u64,
    pub coverage: Coverage,
    pub memory_bytes_approx: u64,
}

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

#[derive(Debug)]
struct Document {
    path: String,
    text: String,
    line_starts: Vec<u32>,
    byte_starts: Vec<u32>,
}

#[derive(Clone, Copy, Debug)]
struct Posting {
    document: u32,
    frequency: u32,
    position_start: u32,
}

#[derive(Debug)]
struct Index {
    generation: u64,
    built_at: SystemTime,
    documents: Vec<Document>,
    postings: BTreeMap<String, Vec<Posting>>,
    positions: Vec<u32>,
    total_tokens: u64,
    coverage: Coverage,
    mutation: MutationStamp,
    memory_estimate: u64,
}

impl Index {
    fn memory_bytes_approx(&self) -> u64 {
        let documents = self
            .documents
            .iter()
            .map(|document| {
                document.path.len() as u64
                    + document.text.len() as u64
                    + document.line_starts.len() as u64 * 4
                    + document.byte_starts.len() as u64 * 4
                    + std::mem::size_of::<Document>() as u64
            })
            .sum::<u64>();
        let terms = self
            .postings
            .iter()
            .map(|(term, postings)| term.len() as u64 + 48 + postings.len() as u64 * 12)
            .sum::<u64>();
        documents + terms + 24 + self.positions.len() as u64 * 4
    }

    fn average_length(&self) -> f64 {
        if self.documents.is_empty() {
            return 0.0;
        }
        self.total_tokens as f64 / self.documents.len() as f64
    }
}

#[derive(Default)]
struct Slot {
    generation: u64,
    index: Option<Arc<Index>>,
    building: Option<(u64, Arc<AtomicBool>)>,
    disposed: bool,
    refresh_failed: bool,
}

pub struct WorkspaceFts {
    workspace: Workspace,
    limits: FtsLimits,
    slot: Mutex<Slot>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Stopped;

impl From<Stopped> for SearchRefusal {
    fn from(_stopped: Stopped) -> Self {
        Self::Stopped
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Clause {
    subtokens: Vec<String>,
}

#[derive(Debug)]
struct ParsedQuery {
    clauses: Vec<Clause>,
    subtokens: Vec<String>,
}

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

struct Build<'a> {
    limits: &'a FtsLimits,
    stop: &'a AtomicBool,
    obsolete: &'a AtomicBool,
    deadline: Instant,
    device: u64,
    coverage: Coverage,
    documents: Vec<Document>,
    postings: BTreeMap<String, Vec<Posting>>,
    positions: Vec<u32>,
    limit: Option<BuildLimit>,
    omission: Option<BuildLimit>,
}

impl Build<'_> {
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
        if self
            .coverage
            .content_bytes_indexed
            .saturating_add(text.len() as u64)
            > self.limits.content_bytes
        {
            self.limit = Some(BuildLimit::ContentBytes);
            return Err(Stopped);
        }

        let mut positions: BTreeMap<String, Vec<u32>> = BTreeMap::new();
        let mut byte_starts = Vec::new();
        let mut interrupted = false;
        tokenize_while(
            &text,
            &mut |lower, start, _| {
                if interrupted {
                    return;
                }
                let position = byte_starts.len() as u32;
                if position.is_multiple_of(256)
                    && (self.stop.load(Ordering::Relaxed)
                        || self.obsolete.load(Ordering::Relaxed)
                        || Instant::now() >= self.deadline)
                {
                    interrupted = true;
                    return;
                }
                match positions.get_mut(lower) {
                    Some(term_positions) => term_positions.push(position),
                    None => {
                        positions.insert(lower.to_owned(), vec![position]);
                    }
                }
                byte_starts.push(start);
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
            .saturating_add(byte_starts.len() as u64)
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
        for (term, term_positions) in positions {
            let frequency = term_positions.len() as u32;
            let position_start = self.positions.len() as u32;
            self.positions.extend(term_positions);
            self.postings.entry(term).or_default().push(Posting {
                document,
                frequency,
                position_start,
            });
            self.coverage.posting_entries += 1;
        }
        self.coverage.files_indexed += 1;
        self.coverage.content_bytes_indexed += text.len() as u64;
        self.coverage.tokens_indexed += byte_starts.len() as u64;
        self.documents.push(Document {
            path: path.to_owned(),
            text,
            line_starts,
            byte_starts,
        });
        Ok(())
    }
}

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
        positions: Vec::new(),
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
        positions: build.positions,
        total_tokens,
        coverage: build.coverage,
        mutation,
        memory_estimate: 0,
    };
    index.memory_estimate = index.memory_bytes_approx();
    Ok(index)
}

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

struct Answer {
    hits: Vec<Hit>,
    files_matched: usize,
    limit: Option<QueryLimit>,
}

struct IndexedClause<'a> {
    terms: &'a [String],
    postings: Vec<&'a [Posting]>,
}

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
    let mut terms = Vec::with_capacity(query.subtokens.len());
    for subtoken in &query.subtokens {
        query_checkpoint(stop, deadline)?;
        let Some(postings) = index.postings.get(subtoken) else {
            return Ok(answer);
        };
        let frequency = postings.len() as f64;
        let inverse = (1.0 + (total as f64 - frequency + 0.5) / (frequency + 0.5)).ln();
        terms.push((postings.as_slice(), inverse));
    }
    let Some(&(rarest, _)) = terms.iter().min_by_key(|(postings, _)| postings.len()) else {
        return Ok(answer);
    };
    // Each cursor advances in document order. Keep scores in the original term order.
    let mut cursors = terms;
    let mut candidates = Vec::with_capacity(rarest.len());
    let mut matched = Vec::with_capacity(cursors.len());
    for (number, seed) in rarest.iter().enumerate() {
        if number.is_multiple_of(256) {
            query_checkpoint(stop, deadline)?;
        }
        matched.clear();
        for (postings, inverse) in &mut cursors {
            if postings
                .first()
                .is_some_and(|posting| posting.document < seed.document)
            {
                let skip = postings.partition_point(|posting| posting.document < seed.document);
                *postings = &postings[skip..];
            }
            let Some((first, rest)) = postings.split_first() else {
                break;
            };
            if first.document != seed.document {
                break;
            }
            matched.push((f64::from(first.frequency), *inverse));
            *postings = rest;
        }
        if matched.len() == cursors.len() {
            let length = index.documents[seed.document as usize].byte_starts.len() as f64;
            let norm = BM25_K1 * (1.0 - BM25_B + BM25_B * length / average.max(1.0));
            let score: f64 = matched
                .iter()
                .map(|(frequency, inverse)| {
                    inverse * (frequency * (BM25_K1 + 1.0)) / (frequency + norm)
                })
                .sum();
            candidates.push((seed.document, score));
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
    let mut clauses = Vec::with_capacity(query.clauses.len());
    for clause in &query.clauses {
        let mut postings = Vec::with_capacity(clause.subtokens.len());
        for term in &clause.subtokens {
            let Some(list) = index.postings.get(term) else {
                return Ok(answer);
            };
            postings.push(list.as_slice());
        }
        clauses.push(IndexedClause {
            terms: &clause.subtokens,
            postings,
        });
    }
    let mut size = 0;
    let mut suppressed_lines = false;
    for (rank, (document, score)) in candidates.iter().enumerate() {
        if answer.hits.len() == limits.results {
            answer.limit = Some(QueryLimit::Results);
            break;
        }
        if rank == limits.query_candidates {
            if answer.hits.is_empty() && !suppressed_lines {
                for (remaining, _) in &candidates[rank..] {
                    query_checkpoint(stop, deadline)?;
                    if phrases_match(index, *remaining, &clauses, stop, deadline)? {
                        answer.limit = Some(QueryLimit::Candidates);
                        break;
                    }
                }
            } else {
                answer.limit = Some(QueryLimit::Candidates);
            }
            break;
        }
        query_checkpoint(stop, deadline)?;
        let mut lines = matching_lines(index, *document, &clauses, stop, deadline)?;
        let document = &index.documents[*document as usize];
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
            let cost = crate::db::serialized_len(&hit.to_json())
                .map_err(|_| SearchRefusal::ResultBytes)?
                + 1;
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

fn phrases_match(
    index: &Index,
    document: u32,
    clauses: &[IndexedClause<'_>],
    stop: &AtomicBool,
    deadline: Instant,
) -> Result<bool, SearchRefusal> {
    for clause in clauses {
        if clause.terms.len() > 1
            && !visit_clause(index, document, clause, stop, deadline, |_| false)?
        {
            return Ok(false);
        }
    }
    Ok(true)
}

// Return whether a clause occurs. The visitor can stop after the first match.
fn visit_clause(
    index: &Index,
    document: u32,
    clause: &IndexedClause<'_>,
    stop: &AtomicBool,
    deadline: Instant,
    mut visit: impl FnMut(u32) -> bool,
) -> Result<bool, SearchRefusal> {
    // Parsing bounds each clause to MAX_CLAUSE_TOKENS. Reuse stack storage
    // while visiting candidates, including documents that yield no matches.
    let mut slices = [&[][..]; MAX_CLAUSE_TOKENS];
    let positions = &mut slices[..clause.postings.len()];
    for (positions, postings) in positions.iter_mut().zip(&clause.postings) {
        let at = if postings.len() == index.documents.len() {
            // A term present in every document has one posting per document ID.
            document as usize
        } else if let Ok(at) = postings.binary_search_by_key(&document, |p| p.document) {
            at
        } else {
            return Ok(false);
        };
        let posting = postings[at];
        let start = posting.position_start as usize;
        *positions = &index.positions[start..start + posting.frequency as usize];
    }
    let Some((anchor, &occurrences)) = positions.iter().enumerate().min_by_key(|(_, p)| p.len())
    else {
        return Ok(false);
    };
    let mut found = false;
    for (number, position) in occurrences.iter().enumerate() {
        if number.is_multiple_of(1024) {
            query_checkpoint(stop, deadline)?;
        }
        let Some(first) = position.checked_sub(anchor as u32) else {
            continue;
        };
        if positions.iter().enumerate().all(|(offset, slice)| {
            offset == anchor || slice.binary_search(&(first + offset as u32)).is_ok()
        }) {
            found = true;
            if !visit(first) {
                break;
            }
        }
    }
    Ok(found)
}

fn line_of(line_starts: &[u32], offset: u32) -> u32 {
    let index = line_starts.partition_point(|start| *start <= offset);
    index.saturating_sub(1) as u32
}

type MatchingLine = (usize, u32, u32, Vec<String>);

fn matching_lines(
    index: &Index,
    id: u32,
    clauses: &[IndexedClause<'_>],
    stop: &AtomicBool,
    deadline: Instant,
) -> Result<Vec<MatchingLine>, SearchRefusal> {
    let document = &index.documents[id as usize];
    let mut by_line: BTreeMap<u32, (Vec<String>, u32, usize)> = BTreeMap::new();
    for clause in clauses {
        let matched = visit_clause(index, id, clause, stop, deadline, |position| {
            let start = document.byte_starts[position as usize];
            let line = line_of(&document.line_starts, start);
            let entry = by_line
                .entry(line)
                .or_insert_with(|| (Vec::new(), start, 0));
            entry.1 = entry.1.min(start);
            entry.2 += 1;
            for subtoken in clause.terms {
                if !entry.0.contains(subtoken) {
                    entry.0.push(subtoken.clone());
                }
            }
            true
        })?;
        if !matched {
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

fn excerpt_of(document: &Document, line: u32, offset: u32) -> (String, bool, usize) {
    let start = document.line_starts[line as usize] as usize;
    let end = document
        .line_starts
        .get(line as usize + 1)
        .map_or(document.text.len(), |next| *next as usize - 1);
    let text = &document.text[start..end];
    let text = text.strip_suffix('\r').unwrap_or(text);
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

    pub fn build(&self) -> Result<BuildReport, SearchRefusal> {
        self.build_with(&AtomicBool::new(false), Instant::now() + self.limits.time)
    }

    pub fn rebuild(&self) -> Result<BuildReport, SearchRefusal> {
        self.build()
    }

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

    pub fn search(&self, query: &str, results: usize) -> Result<Value, SearchRefusal> {
        self.search_with(
            query,
            results,
            &AtomicBool::new(false),
            Instant::now() + self.limits.query_time,
        )
    }

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
        while crate::db::serialized_len(&value).map_err(|_| SearchRefusal::ResultBytes)?
            > limits.result_bytes
        {
            if value["results"].as_array_mut().and_then(Vec::pop).is_none() {
                return Err(SearchRefusal::ResultBytes);
            }
            value["complete"] = json!(false);
            value["limit"] = json!("result_bytes");
        }
        query_checkpoint(stop, deadline)?;
        Ok(value)
    }

    pub fn coverage(&self) -> Option<(u64, Coverage, SystemTime)> {
        let slot = self.lock();
        let index = slot.index.as_ref()?;
        Some((index.generation, index.coverage, index.built_at))
    }

    pub fn dispose(&self) {
        let mut slot = self.lock();
        slot.disposed = true;
        slot.generation += 1;
        if let Some((_, cancelled)) = slot.building.take() {
            cancelled.store(true, Ordering::Relaxed);
        }
        slot.index = None;
    }

    pub(crate) fn stop_query(&self, stop: &AtomicBool) {
        let _slot = self.lock();
        stop.store(true, Ordering::Relaxed);
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

pub(crate) fn parameters() -> Value {
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

pub(crate) struct Call {
    query: String,
    results: usize,
    refresh: bool,
}

pub(crate) fn call_of(arguments: &Value) -> Result<Call, QueryRefusal> {
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

pub(crate) fn run_call(
    fts: &WorkspaceFts,
    call: &Call,
    stop: &AtomicBool,
) -> Result<Value, SearchRefusal> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, path::PathBuf};

    struct Corpus {
        index: WorkspaceFts,
        directory: PathBuf,
    }

    impl Corpus {
        fn new(files: &[(&str, &str)]) -> Self {
            let directory = std::env::temp_dir().join(format!("ocg-fts-{}", uuid::Uuid::now_v7()));
            let root = directory.join("workspace");
            let data = directory.join("data");
            fs::create_dir(&directory).unwrap();
            fs::create_dir(&root).unwrap();
            fs::create_dir(&data).unwrap();
            let corpus = Self {
                index: WorkspaceFts::new(
                    Workspace::authorize(&root, &data).unwrap(),
                    FtsLimits::default(),
                ),
                directory,
            };
            for (name, text) in files {
                fs::write(root.join(name), text).unwrap();
            }
            corpus.index.build().unwrap();
            corpus
        }

        fn hits(&self, query: &str) -> Vec<Value> {
            self.index.search(query, 16).unwrap()["results"]
                .as_array()
                .unwrap()
                .clone()
        }
    }

    impl Drop for Corpus {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.directory);
        }
    }

    #[test]
    fn bm25_intersects_rare_terms_and_orders_ties_by_path() {
        let corpus = Corpus::new(&[
            ("z.txt", "common rare filler"),
            ("common.txt", "common common common"),
            ("dense.txt", "common rare rare"),
            ("a.txt", "common rare filler"),
        ]);
        let hits = corpus.hits("common rare");
        assert_eq!(
            hits.iter()
                .map(|hit| hit["path"].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["dense.txt", "a.txt", "z.txt"]
        );
        assert!(hits[0]["score"].as_f64().unwrap() > hits[1]["score"].as_f64().unwrap());
        assert_eq!(hits[1]["score"], hits[2]["score"]);
        corpus.index.rebuild().unwrap();
        assert_eq!(corpus.hits("rare common"), hits);
    }

    #[test]
    fn phrase_with_rare_middle_term_requires_both_adjacent_positions() {
        let corpus = Corpus::new(&[
            ("adjacent.txt", "rare right left left rare right right"),
            ("separate.txt", "left left rare filler right right"),
            ("reversed.txt", "rare right left left right"),
        ]);
        let hits = corpus.hits("\"left rare right\"");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0]["path"], "adjacent.txt");
        assert_eq!(hits[0]["offset"], "rare right left ".len());
    }

    #[test]
    fn offsets_and_clipped_excerpts_use_utf8_byte_boundaries() {
        let prefix = "界".repeat(73);
        let line = format!("{prefix} needle {}", "é".repeat(120));
        let text = format!("intro\r\n{line}\r\nshort needle\r\n");
        let corpus = Corpus::new(&[("unicode.txt", &text)]);
        let hits = corpus.hits("needle");
        assert_eq!(hits.len(), 2);
        let long = &hits[0];
        assert_eq!(long["line"], 2);
        let offset = long["offset"].as_u64().unwrap() as usize;
        assert_eq!(offset, prefix.len() + 1);
        let start = long["excerpt_offset"].as_u64().unwrap() as usize;
        let excerpt = long["excerpt"].as_str().unwrap();
        assert!(start > 0 && excerpt.len() <= MAX_EXCERPT_BYTES);
        assert_eq!(&line[start..start + excerpt.len()], excerpt);
        assert!(excerpt[offset - start..].starts_with("needle"));
        assert_eq!(long["excerpt_truncated"], true);
        assert_eq!(hits[1]["line"], 3);
        assert_eq!(hits[1]["offset"], 6);
        assert_eq!(hits[1]["excerpt"], "short needle");
        assert_eq!(hits[1]["excerpt_offset"], 0);
        assert_eq!(hits[1]["excerpt_truncated"], false);
    }
}
