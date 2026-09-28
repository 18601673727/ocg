//! Git-aware repository state and bounded diff summaries.
//!
//! Git is optional. Every function degrades to a deterministic non-repo result
//! when `git` is missing or the directory is not a work tree. A changed path is
//! never silently dropped: only hunk *bodies* are bounded, while the complete
//! path/status list is always reported.

use crate::context::config::ContextConfig;
use crate::context::index::ContextIndex;
use crate::context::symbols::SymbolRef;
use crate::error::Result;
use crate::process::{GitHost, GitOutput};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

/// The small, serializable git state carried into maps and plans.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct GitState {
    pub is_repo: bool,
    pub root: Option<String>,
    pub head: Option<String>,
    pub branch: Option<String>,
    pub dirty: bool,
}

/// How a path changed relative to the base.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiffStatus {
    Added,
    Modified,
    Deleted,
    Renamed,
    Copied,
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiffEntry {
    pub path: String,
    pub old_path: Option<String>,
    pub status: DiffStatus,
    /// True for an untracked (`??`) path.
    #[serde(default)]
    pub untracked: bool,
}

/// The working-tree snapshot used by the index and the diff.
#[derive(Debug, Clone, Default)]
pub struct GitSnapshot {
    pub state: GitState,
    /// Relative path -> blob id, only for files clean in the index.
    pub blobs: BTreeMap<String, String>,
    /// The parsed `git status` entries, in git's native order.
    pub entries: Vec<DiffEntry>,
    /// A human-readable reason git was unavailable, if any.
    pub error: Option<String>,
}

impl GitSnapshot {
    pub fn not_a_repo() -> Self {
        Self::default()
    }

    /// Collect the snapshot. A missing `git` binary is not fatal.
    pub fn collect(root: &Path, git: &dyn GitHost) -> Self {
        let inside = match git.run(&["rev-parse", "--is-inside-work-tree"], root) {
            Ok(output) => output,
            Err(error) => {
                return Self {
                    error: Some(error.to_string()),
                    ..Self::not_a_repo()
                }
            }
        };
        if !inside.success || inside.stdout.trim() != "true" {
            return Self::not_a_repo();
        }

        let toplevel = run_trim(git, &["rev-parse", "--show-toplevel"], root);
        let head = run_trim(git, &["rev-parse", "HEAD"], root);
        let branch = run_trim(git, &["symbolic-ref", "--short", "-q", "HEAD"], root);
        let status = git
            .run(
                &["status", "--porcelain=v1", "-z", "--untracked-files=all"],
                root,
            )
            .unwrap_or_else(|_| empty_git_output());
        let ls_files = git
            .run(&["ls-files", "-s", "-z"], root)
            .unwrap_or_else(|_| empty_git_output());

        let prefix = root_prefix(root, toplevel.as_deref());
        let mut entries = parse_status(&status.stdout);
        entries.retain(|entry| under_prefix(&entry.path, prefix.as_deref()));
        entries.retain(|entry| !is_gear_path(&entry.path));
        for entry in &mut entries {
            entry.path = strip_prefix(&entry.path, prefix.as_deref());
            if let Some(old) = entry.old_path.as_mut() {
                *old = strip_prefix(old, prefix.as_deref());
            }
        }
        entries.sort_by(|a, b| {
            a.path
                .cmp(&b.path)
                .then_with(|| a.status.cmp_key().cmp(&b.status.cmp_key()))
        });
        entries.dedup_by(|a, b| a.path == b.path);

        let mut blobs = BTreeMap::new();
        for record in ls_files.stdout.split('\0') {
            if record.is_empty() {
                continue;
            }
            let Some((meta, path)) = record.split_once('\t') else {
                continue;
            };
            let path = if under_prefix(path, prefix.as_deref()) {
                strip_prefix(path, prefix.as_deref())
            } else {
                continue;
            };
            let blob = meta.split_whitespace().nth(1).unwrap_or("").to_string();
            if !blob.is_empty() {
                blobs.insert(path, blob);
            }
        }

        Self {
            state: GitState {
                is_repo: true,
                root: toplevel,
                head,
                branch,
                dirty: !entries.is_empty(),
            },
            blobs,
            entries,
            error: None,
        }
    }
}

impl DiffStatus {
    fn cmp_key(self) -> u8 {
        match self {
            DiffStatus::Added => 0,
            DiffStatus::Modified => 1,
            DiffStatus::Deleted => 2,
            DiffStatus::Renamed => 3,
            DiffStatus::Copied => 4,
            DiffStatus::Other => 5,
        }
    }

    /// A stable wire name used in the git identity fingerprint.
    pub fn as_str(self) -> &'static str {
        match self {
            DiffStatus::Added => "added",
            DiffStatus::Modified => "modified",
            DiffStatus::Deleted => "deleted",
            DiffStatus::Renamed => "renamed",
            DiffStatus::Copied => "copied",
            DiffStatus::Other => "other",
        }
    }
}

/// A deterministic fingerprint of the git identity that a plan depends on:
/// repository-ness, HEAD, branch and the complete sorted status entries
/// (path, status, old path, untracked). A commit or any status change produces
/// a different fingerprint, so a cached plan can never silently keep stale
/// `changed_paths` or a stale HEAD/branch.
pub fn snapshot_fingerprint(snapshot: &GitSnapshot) -> String {
    let mut material = format!(
        "repo={};head={};branch={};status=",
        snapshot.state.is_repo,
        snapshot.state.head.as_deref().unwrap_or(""),
        snapshot.state.branch.as_deref().unwrap_or(""),
    );
    for (index, entry) in snapshot.entries.iter().enumerate() {
        if index > 0 {
            material.push(';');
        }
        material.push_str(&format!(
            "{}|{}|{}|{}",
            entry.path,
            entry.status.as_str(),
            entry.old_path.as_deref().unwrap_or(""),
            entry.untracked,
        ));
    }
    format!(
        "sha256:{}",
        crate::runtime::hash::sha256_hex(material.as_bytes())
    )
}

fn empty_git_output() -> GitOutput {
    GitOutput {
        success: false,
        stdout: String::new(),
        stderr: String::new(),
        truncated: false,
    }
}

fn run_trim(git: &dyn GitHost, args: &[&str], cwd: &Path) -> Option<String> {
    let output = git.run(args, cwd).ok()?;
    if output.success {
        let text = output.stdout.trim().to_string();
        if !text.is_empty() {
            return Some(text);
        }
    }
    None
}

/// The path of `root` relative to `git_root`, when `root` is nested.
fn root_prefix(root: &Path, git_root: Option<&str>) -> Option<String> {
    let git_root = git_root?;
    let git_root = std::fs::canonicalize(git_root).ok()?;
    let root = std::fs::canonicalize(root).ok()?;
    let relative = root.strip_prefix(&git_root).ok()?;
    let text = relative.to_string_lossy().replace('\\', "/");
    if text.is_empty() {
        None
    } else {
        Some(format!("{text}/"))
    }
}

fn under_prefix(path: &str, prefix: Option<&str>) -> bool {
    match prefix {
        None => true,
        Some(prefix) => path.starts_with(prefix),
    }
}

/// `.ocg/` is OCG's own state, never project content.
fn is_gear_path(path: &str) -> bool {
    path == crate::context::repomap::OCG_DIR
        || path.starts_with(&format!("{}/", crate::context::repomap::OCG_DIR))
}

fn strip_prefix(path: &str, prefix: Option<&str>) -> String {
    match prefix {
        None => path.to_string(),
        Some(prefix) => path.strip_prefix(prefix).unwrap_or(path).to_string(),
    }
}

/// Parse `git status --porcelain=v1 -z`.
fn parse_status(stdout: &str) -> Vec<DiffEntry> {
    let mut entries = Vec::new();
    let mut records = stdout.split('\0').filter(|record| !record.is_empty());
    while let Some(record) = records.next() {
        if record.len() < 3 {
            continue;
        }
        let x = record.as_bytes()[0] as char;
        let y = record.as_bytes()[1] as char;
        let path = record[3..].to_string();
        let status = match (x, y) {
            ('?', _) | ('A', _) | (_, 'A') => DiffStatus::Added,
            ('R', _) | (_, 'R') => DiffStatus::Renamed,
            ('C', _) | (_, 'C') => DiffStatus::Copied,
            ('D', _) | (_, 'D') => DiffStatus::Deleted,
            ('M', _) | (_, 'M') | ('T', _) | (_, 'T') => DiffStatus::Modified,
            ('!', _) => continue,
            _ => DiffStatus::Other,
        };
        let mut old_path = None;
        if matches!(status, DiffStatus::Renamed | DiffStatus::Copied) {
            old_path = records.next().map(str::to_string);
        }
        entries.push(DiffEntry {
            path,
            old_path,
            status,
            untracked: x == '?',
        });
    }
    entries
}

/// One bounded diff hunk.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiffHunk {
    pub path: String,
    pub header: String,
    pub old_start: u32,
    pub old_lines: u32,
    pub new_start: u32,
    pub new_lines: u32,
    pub body: Vec<String>,
}

/// A 1-based inclusive line range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LineRange {
    pub start: u32,
    pub end: u32,
}

/// Structural summary produced when hunks are truncated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StructuralSummary {
    pub files_changed: usize,
    pub files_added: usize,
    pub files_deleted: usize,
    pub files_renamed: usize,
    pub hunks: usize,
    pub truncated: bool,
}

/// The complete diff summary handed to the context planner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DiffSummary {
    pub state: GitState,
    pub base: Option<String>,
    pub entries: Vec<DiffEntry>,
    pub hunks: Vec<DiffHunk>,
    pub changed_lines: BTreeMap<String, Vec<LineRange>>,
    pub changed_symbols: Vec<SymbolRef>,
    pub likely_references: Vec<SymbolRef>,
    pub likely_tests: Vec<String>,
    pub structural: Option<StructuralSummary>,
    /// True when hunks were bounded by `maxHunks`/`maxDiffBytes`.
    pub truncated: bool,
    /// True when the git stdout capture itself was bounded.
    pub capture_truncated: bool,
    pub diff_bytes: usize,
    /// Deleted paths whose symbols have no current source (never guessed).
    pub unsourced_paths: Vec<String>,
}

impl DiffSummary {
    pub fn changed_paths(&self) -> Vec<String> {
        self.entries
            .iter()
            .map(|entry| entry.path.clone())
            .collect()
    }
}

/// Compute a bounded diff summary for the working tree.
pub fn compute(
    root: &Path,
    git: &dyn GitHost,
    snapshot: &GitSnapshot,
    index: &ContextIndex,
    config: &ContextConfig,
) -> Result<DiffSummary> {
    let mut summary = DiffSummary {
        state: snapshot.state.clone(),
        base: snapshot.state.head.clone(),
        entries: snapshot.entries.clone(),
        ..DiffSummary::default()
    };
    if !config.include_untracked {
        summary.entries.retain(|entry| !entry.untracked);
    }
    if !snapshot.state.is_repo {
        return Ok(summary);
    }

    // Bound the diff capture before it can allocate an arbitrarily large
    // stdout. The hunks are separately bounded by `parse_diff`.
    let raw = git
        .run_bounded(
            &["diff", "--no-color", "--no-ext-diff", "--unified=3"],
            root,
            config.max_diff_bytes,
        )
        .unwrap_or_else(|_| empty_git_output());
    summary.capture_truncated = raw.truncated;
    let (hunks, changed_lines, diff_bytes, seen_hunks, truncated) =
        parse_diff(&raw.stdout, config, snapshot);
    summary.hunks = hunks;
    summary.changed_lines = changed_lines;
    summary.diff_bytes = diff_bytes;
    summary.truncated = truncated || raw.truncated;

    summary.unsourced_paths = summary
        .entries
        .iter()
        .filter(|entry| entry.status == DiffStatus::Deleted)
        .map(|entry| entry.path.clone())
        .collect();
    summary.unsourced_paths.sort();
    summary.unsourced_paths.dedup();

    let mut changed_symbols = collect_changed_symbols(index, &summary, config);
    changed_symbols
        .sort_by(|a, b| (&a.path, a.start_line, &a.name).cmp(&(&b.path, b.start_line, &b.name)));
    changed_symbols.dedup();
    changed_symbols.truncate(config.max_candidates);
    summary.changed_symbols = changed_symbols;
    summary.likely_references = collect_references(index, &summary.changed_symbols, config);
    summary.likely_tests = collect_likely_tests(index, &summary, config);

    if summary.truncated || diff_bytes > config.max_diff_bytes {
        summary.structural = Some(structural_summary(&summary.entries, seen_hunks, true));
        summary.truncated = true;
    }
    Ok(summary)
}

fn structural_summary(entries: &[DiffEntry], hunks: usize, truncated: bool) -> StructuralSummary {
    let count = |status: DiffStatus| {
        entries
            .iter()
            .filter(|entry| entry.status == status)
            .count()
    };
    StructuralSummary {
        files_changed: entries.len(),
        files_added: count(DiffStatus::Added),
        files_deleted: count(DiffStatus::Deleted),
        files_renamed: count(DiffStatus::Renamed),
        hunks,
        truncated,
    }
}

type ParsedDiff = (
    Vec<DiffHunk>,
    BTreeMap<String, Vec<LineRange>>,
    usize,
    usize,
    bool,
);

fn parse_diff(stdout: &str, config: &ContextConfig, snapshot: &GitSnapshot) -> ParsedDiff {
    let mut hunks: Vec<DiffHunk> = Vec::new();
    let mut changed_lines: BTreeMap<String, Vec<LineRange>> = BTreeMap::new();
    let mut current_path: Option<String> = None;
    let mut pending_old: Option<String> = None;
    let mut seen_hunks = 0usize;
    let mut retained_bytes = 0usize;
    let mut truncated = false;
    let mut current: Option<DiffHunk> = None;

    fn flush(current: &mut Option<DiffHunk>, hunks: &mut Vec<DiffHunk>) {
        if let Some(hunk) = current.take() {
            hunks.push(hunk);
        }
    }

    for line in stdout.lines() {
        if line.starts_with("diff --git ") {
            flush(&mut current, &mut hunks);
            current_path = None;
            pending_old = None;
            continue;
        }
        if let Some(path) = line.strip_prefix("--- ") {
            pending_old = parse_patch_path(path);
            continue;
        }
        if let Some(path) = line.strip_prefix("+++ ") {
            current_path = parse_patch_path(path).or_else(|| pending_old.clone());
            continue;
        }
        if line.starts_with("@@") {
            flush(&mut current, &mut hunks);
            seen_hunks += 1;
            let path = current_path.clone().unwrap_or_default();
            let (old_start, old_lines, new_start, new_lines) = parse_hunk_range(line);
            // Always record the range for changed-symbol detection, even when
            // the hunk body itself is not retained.
            let range = if new_lines > 0 {
                LineRange {
                    start: new_start,
                    end: new_start + new_lines - 1,
                }
            } else if old_lines > 0 {
                LineRange {
                    start: old_start,
                    end: old_start + old_lines - 1,
                }
            } else {
                LineRange {
                    start: new_start,
                    end: new_start,
                }
            };
            changed_lines.entry(path.clone()).or_default().push(range);

            let retain = hunks.len() < config.max_hunks
                && retained_bytes < config.max_diff_bytes
                && !truncated;
            if retain {
                current = Some(DiffHunk {
                    path,
                    header: line.to_string(),
                    old_start,
                    old_lines,
                    new_start,
                    new_lines,
                    body: Vec::new(),
                });
            } else {
                // Never retain an empty placeholder hunk past the limits.
                truncated = true;
                current = None;
            }
            continue;
        }
        if let Some(hunk) = current.as_mut() {
            let next = line.len() + 1;
            if !truncated && retained_bytes.saturating_add(next) <= config.max_diff_bytes {
                retained_bytes += next;
                hunk.body.push(line.to_string());
            } else {
                truncated = true;
            }
        }
    }
    flush(&mut current, &mut hunks);

    // Added/deleted files may have no textual hunk; still record their ranges.
    for entry in &snapshot.entries {
        if !config.include_untracked && entry.untracked {
            continue;
        }
        match entry.status {
            DiffStatus::Added | DiffStatus::Deleted => {
                changed_lines.entry(entry.path.clone()).or_default();
            }
            _ => {}
        }
    }
    (hunks, changed_lines, retained_bytes, seen_hunks, truncated)
}

fn parse_patch_path(raw: &str) -> Option<String> {
    let raw = raw.trim();
    if raw == "/dev/null" {
        return None;
    }
    Some(
        raw.strip_prefix("b/")
            .or_else(|| raw.strip_prefix("a/"))
            .unwrap_or(raw)
            .to_string(),
    )
}

fn parse_hunk_range(header: &str) -> (u32, u32, u32, u32) {
    let mut old = (0u32, 1u32);
    let mut new = (0u32, 1u32);
    for part in header.split_whitespace() {
        if let Some(rest) = part.strip_prefix('-') {
            old = parse_range(rest);
        } else if let Some(rest) = part.strip_prefix('+') {
            new = parse_range(rest);
        }
    }
    (old.0, old.1, new.0, new.1)
}

fn parse_range(text: &str) -> (u32, u32) {
    let text = text.trim_end_matches("@@");
    match text.split_once(',') {
        Some((start, count)) => (start.parse().unwrap_or(0), count.parse().unwrap_or(1)),
        None => (text.parse().unwrap_or(0), 1),
    }
}

fn collect_changed_symbols(
    index: &ContextIndex,
    summary: &DiffSummary,
    config: &ContextConfig,
) -> Vec<SymbolRef> {
    let mut out = Vec::new();
    for entry in &summary.entries {
        let Some(file) = index.file(&entry.path) else {
            continue;
        };
        match entry.status {
            DiffStatus::Added | DiffStatus::Renamed => {
                for symbol in &file.symbols {
                    out.push(SymbolRef::new(&file.path, symbol));
                }
            }
            // A deleted path has no current source; never claim its symbols.
            // The path is reported through `unsourced_paths` instead.
            DiffStatus::Deleted => {}
            _ => {
                if let Some(ranges) = summary.changed_lines.get(&entry.path) {
                    for symbol in &file.symbols {
                        let overlaps = ranges.iter().any(|range| {
                            symbol.start_line <= range.end && symbol.end_line >= range.start
                        });
                        if overlaps {
                            out.push(SymbolRef::new(&file.path, symbol));
                        }
                    }
                }
            }
        }
        if out.len() >= config.max_candidates {
            break;
        }
    }
    out
}

fn collect_references(
    index: &ContextIndex,
    changed: &[SymbolRef],
    config: &ContextConfig,
) -> Vec<SymbolRef> {
    let names: BTreeSet<&str> = changed
        .iter()
        .filter(|symbol| symbol.kind != crate::context::symbols::SymbolKind::Import)
        .map(|symbol| symbol.name.as_str())
        .collect();
    if names.is_empty() {
        return Vec::new();
    }
    let origin: BTreeSet<&str> = changed.iter().map(|symbol| symbol.path.as_str()).collect();
    let mut out = Vec::new();
    for file in &index.files {
        if origin.contains(file.path.as_str()) {
            continue;
        }
        for symbol in &file.symbols {
            if names.contains(symbol.name.as_str()) {
                out.push(SymbolRef::new(&file.path, symbol));
                if out.len() >= config.max_candidates {
                    return out;
                }
            }
        }
    }
    out.sort_by(|a, b| (&a.name, &a.path, a.start_line).cmp(&(&b.name, &b.path, b.start_line)));
    out
}

fn collect_likely_tests(
    index: &ContextIndex,
    summary: &DiffSummary,
    config: &ContextConfig,
) -> Vec<String> {
    let names: BTreeSet<&str> = summary
        .changed_symbols
        .iter()
        .map(|symbol| symbol.name.as_str())
        .collect();
    let mut tests = BTreeSet::new();
    for entry in &summary.entries {
        if is_test_path(&entry.path) {
            tests.insert(entry.path.clone());
        }
    }
    for reference in &summary.likely_references {
        if is_test_path(&reference.path) {
            tests.insert(reference.path.clone());
        }
    }
    for file in &index.files {
        if !is_test_path(&file.path) {
            continue;
        }
        if file
            .symbols
            .iter()
            .any(|symbol| names.contains(symbol.name.as_str()))
        {
            tests.insert(file.path.clone());
        }
    }
    tests.into_iter().take(config.max_candidates).collect()
}

/// Whether a path looks like a test file. Kept intentionally simple and
/// deterministic; it only nudges ranking, it never hides a changed path.
pub fn is_test_path(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    let name = lower.rsplit('/').next().unwrap_or(&lower);
    lower.starts_with("tests/")
        || lower.contains("/tests/")
        || lower.starts_with("test/")
        || lower.contains("/test/")
        || name.starts_with("test_")
        || name.ends_with("_test.rs")
        || name.ends_with("_test.go")
        || name.ends_with("_test.py")
        || name.contains(".test.")
        || name.contains(".spec.")
        || lower.starts_with("spec/")
        || lower.contains("/spec/")
}
