//! Incremental, inspectable JSON context index.
//!
//! The index stores one entry per file with a fingerprint, optional Git blob
//! id and the extracted symbols. Files whose fingerprint is unchanged are
//! reused without re-parsing. Corruption, a schema bump or an engine bump
//! discards the index and recomputes it safely.

use crate::context::config::ContextConfig;
use crate::context::gitdiff::GitSnapshot;
use crate::context::repomap::RepoMap;
use crate::context::symbols::{self, Symbol, SymbolRef};
use crate::error::{OcgError, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

/// Directory (inside `<project>/.ocg/`) holding the inspectable JSON
/// index. Kept separate from `cache/` so `ocg cache clean` never touches it.
pub const INDEX_DIR: &str = "index";
/// The index file name.
pub const INDEX_FILE: &str = "context-index.json";

/// One indexed file.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct IndexFile {
    pub path: String,
    pub size: u64,
    /// `sha256:...`, `blob:...`, `size:...` or a sentinel for excluded files.
    pub fingerprint: String,
    pub git_blob: Option<String>,
    pub language: String,
    pub kind: String,
    pub supported: bool,
    pub excluded: bool,
    pub binary: bool,
    pub huge: bool,
    pub symbols: Vec<Symbol>,
}

/// Counters for one index update.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct IndexMetrics {
    pub files: usize,
    pub updated: usize,
    pub reused: usize,
    pub excluded: usize,
    pub binary: usize,
    pub huge: usize,
    pub unsupported: usize,
    pub symbols: usize,
    /// True when the file walk stopped at `context.maxRepositoryFiles`.
    #[serde(default)]
    pub truncated: bool,
}

/// The persisted index.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ContextIndex {
    pub schema_version: u32,
    pub engine_version: String,
    pub repo_id: String,
    pub root: String,
    pub generated_at: i64,
    pub files: Vec<IndexFile>,
    pub metrics: IndexMetrics,
    /// True when the map/index is incomplete because of the repository cap.
    #[serde(default)]
    pub truncated: bool,
    /// A deterministic identity of the indexed repository content. It changes
    /// only when the indexed files, the repo root, the engine or the schema
    /// changes, never because the current task wording changed.
    #[serde(default)]
    pub generation_id: String,
}

impl ContextIndex {
    pub fn file(&self, path: &str) -> Option<&IndexFile> {
        self.files.iter().find(|file| file.path == path)
    }

    /// All symbols whose name equals `name`.
    pub fn find(&self, name: &str) -> Vec<SymbolRef> {
        let mut out = Vec::new();
        for file in &self.files {
            for symbol in &file.symbols {
                if symbol.name == name {
                    out.push(SymbolRef::new(&file.path, symbol));
                }
            }
        }
        out
    }

    /// The first non-import declaration named `name`, if any.
    pub fn definition(&self, name: &str) -> Option<SymbolRef> {
        let mut candidates: Vec<SymbolRef> = self
            .files
            .iter()
            .flat_map(|file| {
                file.symbols
                    .iter()
                    .map(move |symbol| SymbolRef::new(&file.path, symbol))
            })
            .filter(|symbol| symbol.name == name && symbol.kind != symbols::SymbolKind::Import)
            .collect();
        candidates.sort_by(|a, b| (&a.path, a.start_line).cmp(&(&b.path, b.start_line)));
        candidates.into_iter().next()
    }

    /// Case-insensitive substring search over symbol names.
    pub fn search(&self, query: &str, limit: usize) -> Vec<SymbolRef> {
        let needle = query.to_ascii_lowercase();
        let mut out = Vec::new();
        for file in &self.files {
            for symbol in &file.symbols {
                if symbol.name.to_ascii_lowercase().contains(&needle) {
                    out.push(SymbolRef::new(&file.path, symbol));
                    if out.len() >= limit {
                        return out;
                    }
                }
            }
        }
        out
    }

    /// Symbols declared in `path`.
    pub fn symbols_in(&self, path: &str) -> Vec<SymbolRef> {
        self.file(path)
            .map(|file| {
                file.symbols
                    .iter()
                    .map(|symbol| SymbolRef::new(&file.path, symbol))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Imports declared in `path`.
    pub fn imports(&self, path: &str) -> Vec<SymbolRef> {
        self.symbols_in(path)
            .into_iter()
            .filter(|symbol| symbol.kind == symbols::SymbolKind::Import)
            .collect()
    }

    /// Symbols whose name is referenced in other files' symbol lists. This is
    /// a cheap, deterministic "probable reference" signal, not a type checker.
    pub fn probable_references(&self, name: &str, limit: usize) -> Vec<SymbolRef> {
        let mut out = Vec::new();
        for file in &self.files {
            for symbol in &file.symbols {
                if symbol.name == name {
                    out.push(SymbolRef::new(&file.path, symbol));
                    if out.len() >= limit {
                        return out;
                    }
                }
            }
        }
        out
    }

    /// The file content slice for a symbol range, read lazily from disk.
    pub fn source_slice(&self, root: &Path, reference: &SymbolRef) -> Result<Option<String>> {
        if crate::context::classify::classify(Path::new(&reference.path)).sensitive {
            return Ok(None);
        }
        let path = root.join(&reference.path);
        let content = fs::read_to_string(&path).map_err(|error| OcgError::read(&path, error))?;
        let lines: Vec<&str> = content.lines().collect();
        let start = reference.start_line.saturating_sub(1) as usize;
        let end = (reference.end_line as usize).min(lines.len());
        if start >= lines.len() || start >= end {
            return Ok(None);
        }
        Ok(Some(lines[start..end].join("\n")))
    }
}

/// One index update outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexReport {
    pub metrics: IndexMetrics,
    pub changed_paths: Vec<String>,
    pub written: bool,
    /// A deterministic identity of the indexed repository content that produced
    /// this report. Independent of the current task or ranking.
    pub generation_id: String,
}

/// `<project>/.ocg/index`.
pub fn index_dir(root: &Path) -> PathBuf {
    root.join(crate::context::repomap::OCG_DIR).join(INDEX_DIR)
}

/// `<project>/.ocg/index/context-index.json`.
pub fn index_path(root: &Path) -> PathBuf {
    index_dir(root).join(INDEX_FILE)
}

/// Load an index, returning `None` for a missing, corrupt or version-mismatched
/// file. Callers recompute in that case; this is never fatal.
pub fn load(root: &Path) -> Option<ContextIndex> {
    let text = fs::read_to_string(index_path(root)).ok()?;
    let index: ContextIndex = serde_json::from_str(&text).ok()?;
    if index.schema_version != crate::context::freshness::SCHEMA_VERSION {
        return None;
    }
    if index.engine_version != crate::context::freshness::ENGINE_VERSION {
        return None;
    }
    Some(index)
}

/// Persist the index atomically, keeping `.ocg/` out of the project
/// VCS with the same idempotent helper the runtime uses.
pub fn save(root: &Path, index: &ContextIndex) -> Result<()> {
    crate::runtime::install::ensure_gitignore(root)?;
    let value = serde_json::to_value(index).map_err(|error| {
        OcgError::config(format!("cannot serialize the context index: {error}"))
    })?;
    crate::runtime::install::write_json_atomic(&index_path(root), &value)
}

/// Compute a fresh index incrementally from `previous`.
pub fn update(
    root: &Path,
    map: &RepoMap,
    snapshot: &GitSnapshot,
    config: &ContextConfig,
    previous: Option<&ContextIndex>,
    now: i64,
) -> Result<(ContextIndex, IndexReport)> {
    let previous_files: BTreeMap<&str, &IndexFile> = previous
        .map(|index| {
            index
                .files
                .iter()
                .map(|file| (file.path.as_str(), file))
                .collect()
        })
        .unwrap_or_default();
    let dirty: BTreeSet<&str> = snapshot
        .entries
        .iter()
        .map(|entry| entry.path.as_str())
        .collect();

    let mut files = Vec::with_capacity(map.files.len());
    let mut metrics = IndexMetrics::default();
    let mut changed_paths = Vec::new();

    for entry in &map.files {
        metrics.files += 1;
        let prior = previous_files.get(entry.path.as_str()).copied();
        let mut index_file = IndexFile {
            path: entry.path.clone(),
            size: entry.size,
            language: entry.language.clone(),
            kind: entry.kind.as_str().to_string(),
            supported: entry.supported,
            ..IndexFile::default()
        };

        if entry.sensitive {
            index_file.excluded = true;
            index_file.fingerprint = "sensitive".to_string();
            metrics.excluded += 1;
            changed_paths.push(entry.path.clone());
            files.push(index_file);
            continue;
        }
        if entry.huge {
            index_file.huge = true;
            index_file.fingerprint = format!("huge:{}", entry.size);
            metrics.huge += 1;
            files.push(index_file);
            continue;
        }
        if entry.binary {
            index_file.binary = true;
            index_file.fingerprint = format!("binary:{}", entry.size);
            metrics.binary += 1;
            files.push(index_file);
            continue;
        }
        if !entry.supported {
            index_file.fingerprint = format!("size:{}", entry.size);
            metrics.unsupported += 1;
            files.push(index_file);
            continue;
        }

        let blob = snapshot.blobs.get(&entry.path).cloned();
        let clean = blob.is_some() && !dirty.contains(entry.path.as_str());
        index_file.git_blob = blob.clone();

        let (fingerprint, content) = if clean {
            (format!("blob:{}", blob.clone().unwrap_or_default()), None)
        } else {
            match fs::read(root.join(&entry.path)) {
                Ok(bytes) => (
                    format!("sha256:{}", crate::runtime::hash::sha256_hex(&bytes)),
                    Some(bytes),
                ),
                Err(_) => ("unreadable".to_string(), None),
            }
        };

        let reusable = prior
            .map(|prior| {
                prior.fingerprint == fingerprint
                    && prior.supported == entry.supported
                    && prior.symbols.len() <= config.max_symbols_per_file
            })
            .unwrap_or(false);

        if reusable {
            index_file.fingerprint = fingerprint;
            index_file.symbols = prior.map(|prior| prior.symbols.clone()).unwrap_or_default();
            metrics.reused += 1;
        } else {
            let parsed = match content {
                Some(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
                None => match fs::read(root.join(&entry.path)) {
                    Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
                    Err(_) => String::new(),
                },
            };
            index_file.symbols =
                symbols::extract(&entry.language, &parsed, config.max_symbols_per_file);
            index_file.fingerprint = fingerprint;
            metrics.updated += 1;
            changed_paths.push(entry.path.clone());
        }
        metrics.symbols += index_file.symbols.len();
        files.push(index_file);
    }

    files.sort_by(|a, b| a.path.cmp(&b.path));
    changed_paths.sort();
    changed_paths.dedup();
    metrics.truncated = map.truncated;

    let repo_id_value = repo_id(root, &map.git);
    let generation_id_value = generation_id(
        &repo_id_value,
        crate::context::freshness::ENGINE_VERSION,
        crate::context::freshness::SCHEMA_VERSION,
        map.truncated,
        &files,
    );
    // Metrics describe *this* update, not the indexed content, so they must not
    // make an otherwise identical index look changed.
    let same_as_previous = previous
        .map(|previous| {
            previous.files == files
                && previous.repo_id == repo_id_value
                && previous.truncated == map.truncated
        })
        .unwrap_or(false);
    let generated_at = if same_as_previous {
        previous
            .map(|previous| previous.generated_at)
            .unwrap_or(now)
    } else {
        now
    };

    let written = !same_as_previous;

    let index = ContextIndex {
        schema_version: crate::context::freshness::SCHEMA_VERSION,
        engine_version: crate::context::freshness::ENGINE_VERSION.to_string(),
        repo_id: repo_id_value,
        root: root.to_string_lossy().into_owned(),
        generated_at,
        files,
        metrics: metrics.clone(),
        truncated: map.truncated,
        generation_id: generation_id_value.clone(),
    };
    if written {
        save(root, &index)?;
    }
    Ok((
        index,
        IndexReport {
            metrics,
            changed_paths,
            written,
            generation_id: generation_id_value,
        },
    ))
}

/// A deterministic identity for the indexed repository content.
///
/// It is independent of the current task, the ranked projection or any
/// per-turn state. It changes only when the repo root, the engine/schema
/// version, the truncated flag or the indexed file contents change.
pub fn generation_id(
    repo_id: &str,
    engine_version: &str,
    schema_version: u32,
    truncated: bool,
    files: &[IndexFile],
) -> String {
    let mut material = String::new();
    material.push_str(repo_id);
    material.push('|');
    material.push_str(engine_version);
    material.push('|');
    material.push_str(&schema_version.to_string());
    material.push('|');
    material.push_str(if truncated { "1" } else { "0" });
    for file in files {
        material.push('|');
        material.push_str(&file.path);
        material.push(',');
        material.push_str(&file.fingerprint);
        material.push(',');
        material.push_str(&file.size.to_string());
        material.push(',');
        material.push_str(if file.supported { "1" } else { "0" });
        material.push(',');
        material.push_str(if file.excluded { "1" } else { "0" });
        material.push(',');
        material.push_str(if file.binary { "1" } else { "0" });
        material.push(',');
        material.push_str(if file.huge { "1" } else { "0" });
        material.push(',');
        material.push_str(&file.symbols.len().to_string());
    }
    format!(
        "sha256:{}",
        crate::runtime::hash::sha256_hex(material.as_bytes())
    )
}

/// A stable repository identity: the canonical root path plus, when known, the
/// git root. Deliberately local and path-based; no remote detection.
pub fn repo_id(root: &Path, git: &crate::context::gitdiff::GitState) -> String {
    let canonical = std::fs::canonicalize(root).unwrap_or_else(|_| root.to_path_buf());
    let mut material = canonical.to_string_lossy().replace('\\', "/");
    if let Some(git_root) = &git.root {
        material.push('|');
        material.push_str(git_root);
    }
    format!(
        "repo:{}",
        crate::runtime::hash::sha256_hex(material.as_bytes())
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::gitdiff::GitState;

    fn map_with(path: &str, supported: bool) -> RepoMap {
        RepoMap {
            roots: vec![],
            source_roots: vec![],
            test_roots: vec![],
            languages: vec![],
            manifests: vec![],
            build_files: vec![],
            config_files: vec![],
            entrypoints: vec![],
            migrations: vec![],
            git: GitState::default(),
            files: vec![crate::context::repomap::FileEntry {
                path: path.to_string(),
                size: 11,
                language: "rust".to_string(),
                supported,
                kind: crate::context::repomap::FileKind::Source,
                sensitive: false,
                sensitive_reason: None,
                binary: false,
                huge: false,
            }],
            excluded_dirs: vec![],
            truncated: false,
        }
    }

    #[test]
    fn reuses_unchanged_files_and_updates_changed_ones() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.rs"), "fn a() {}\n").unwrap();
        let config = ContextConfig::default();
        let snapshot = GitSnapshot::not_a_repo();
        let map = map_with("a.rs", true);

        let (first, report) = update(dir.path(), &map, &snapshot, &config, None, 10).unwrap();
        assert_eq!(report.metrics.updated, 1);
        assert_eq!(first.files[0].symbols.len(), 1);

        let (second, report) =
            update(dir.path(), &map, &snapshot, &config, Some(&first), 11).unwrap();
        assert_eq!(report.metrics.reused, 1);
        assert_eq!(report.metrics.updated, 0);
        assert_eq!(second.generated_at, 10);

        std::fs::write(dir.path().join("a.rs"), "fn b() {}\n").unwrap();
        let (third, report) =
            update(dir.path(), &map, &snapshot, &config, Some(&second), 12).unwrap();
        assert_eq!(report.metrics.updated, 1);
        assert_eq!(third.files[0].symbols[0].name, "b");
    }

    #[test]
    fn corrupt_index_is_ignored() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(index_dir(dir.path())).unwrap();
        std::fs::write(index_path(dir.path()), "{not json").unwrap();
        assert!(load(dir.path()).is_none());
    }

    #[test]
    fn version_mismatch_is_ignored() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(index_dir(dir.path())).unwrap();
        std::fs::write(
            index_path(dir.path()),
            r#"{"schema_version":999,"engine_version":"x","repo_id":"","root":"","generated_at":0,"files":[],"metrics":{}}"#,
        )
        .unwrap();
        assert!(load(dir.path()).is_none());
    }

    #[test]
    fn query_apis_are_deterministic() {
        let mut index = ContextIndex {
            schema_version: 1,
            engine_version: "test".to_string(),
            repo_id: "r".to_string(),
            root: ".".to_string(),
            generated_at: 0,
            files: vec![],
            metrics: Default::default(),
            truncated: false,
            generation_id: String::new(),
        };
        index.files.push(IndexFile {
            path: "src/a.rs".to_string(),
            symbols: vec![Symbol {
                name: "run".to_string(),
                kind: symbols::SymbolKind::Function,
                start_line: 1,
                end_line: 2,
                signature: String::new(),
            }],
            ..IndexFile::default()
        });
        assert_eq!(index.find("run").len(), 1);
        assert_eq!(index.definition("run").unwrap().path, "src/a.rs");
        assert_eq!(index.search("RU", 10).len(), 1);
        assert!(index.imports("src/a.rs").is_empty());
    }
}
