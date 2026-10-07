//! Deterministic repository map.
//!
//! The map is the cheap, content-free description of a repository: which
//! directories exist, where source and tests live, which languages and
//! manifests are present, git state, and one entry per file with path metadata
//! (and no content for sensitive, binary or oversized files).

use crate::context::classify;
use crate::context::config::ContextConfig;
use crate::context::gitdiff::GitSnapshot;
use crate::context::symbols;
use crate::error::{OcgError, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

/// Directory names that are never walked.
pub const EXCLUDED_DIRS: &[&str] = &[
    ".git",
    "target",
    "node_modules",
    "dist",
    "build",
    "coverage",
    "vendor",
    ".next",
    ".cache",
    "__pycache__",
    ".venv",
    "venv",
    ".tox",
    ".mypy_cache",
    ".pytest_cache",
];

/// OCG's own state directory is never part of the repository map.
pub const OCG_DIR: &str = ".ocg";

/// What a file is used for. Used by ranking and reporting, never to hide paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileKind {
    Source,
    Test,
    Manifest,
    Build,
    Config,
    Entrypoint,
    Migration,
    Doc,
    Other,
}

impl FileKind {
    pub fn as_str(self) -> &'static str {
        match self {
            FileKind::Source => "source",
            FileKind::Test => "test",
            FileKind::Manifest => "manifest",
            FileKind::Build => "build",
            FileKind::Config => "config",
            FileKind::Entrypoint => "entrypoint",
            FileKind::Migration => "migration",
            FileKind::Doc => "doc",
            FileKind::Other => "other",
        }
    }
}

/// One file, with metadata only for sensitive/binary/oversized files.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileEntry {
    pub path: String,
    pub size: u64,
    pub language: String,
    pub supported: bool,
    pub kind: FileKind,
    pub sensitive: bool,
    pub sensitive_reason: Option<String>,
    pub binary: bool,
    pub huge: bool,
}

/// Aggregate language composition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LanguageStat {
    pub language: String,
    pub files: usize,
    pub bytes: u64,
}

/// The complete repo map.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepoMap {
    pub roots: Vec<String>,
    pub source_roots: Vec<String>,
    pub test_roots: Vec<String>,
    pub languages: Vec<LanguageStat>,
    pub manifests: Vec<String>,
    pub build_files: Vec<String>,
    pub config_files: Vec<String>,
    pub entrypoints: Vec<String>,
    pub migrations: Vec<String>,
    pub git: crate::context::gitdiff::GitState,
    pub files: Vec<FileEntry>,
    pub excluded_dirs: Vec<String>,
    /// True when the walk stopped at `context.maxRepositoryFiles`; the map is
    /// explicitly incomplete and must not be presented as complete.
    pub truncated: bool,
}

impl RepoMap {
    pub fn file(&self, path: &str) -> Option<&FileEntry> {
        self.files.iter().find(|entry| entry.path == path)
    }
}

/// Build the map for `root`.
pub fn build(root: &Path, config: &ContextConfig, snapshot: &GitSnapshot) -> Result<RepoMap> {
    if !root.is_dir() {
        return Err(OcgError::config(format!(
            "{} is not a directory",
            root.display()
        )));
    }

    let mut raw = Vec::new();
    let mut truncated = false;
    let walker = ignore::WalkBuilder::new(root)
        .standard_filters(false)
        .follow_links(false)
        .sort_by_file_name(Ord::cmp)
        .filter_entry(|entry| {
            entry.depth() == 0
                || (entry.file_name() != OCG_DIR
                    && !(entry.file_type().is_some_and(|kind| kind.is_dir())
                        && EXCLUDED_DIRS.contains(&entry.file_name().to_string_lossy().as_ref())))
        })
        .build();
    for entry in walker.flatten() {
        if entry.depth() == 0 || !entry.file_type().is_some_and(|kind| kind.is_file()) {
            continue;
        }
        if raw.len() >= config.max_repository_files {
            truncated = true;
            break;
        }
        let size = entry.metadata().map(|metadata| metadata.len()).unwrap_or(0);
        raw.push((entry.into_path(), size));
    }
    raw.sort_by(|a, b| a.0.cmp(&b.0));

    let mut files = Vec::with_capacity(raw.len());
    for (path, size) in raw {
        let relative = relative_path(root, &path);
        let name = path
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default();
        let classification = classify::classify(Path::new(&relative));
        let language = detect_language(&name).to_string();
        let kind = detect_kind(&relative, &name, &language);
        let huge = size > config.max_file_bytes;
        let binary = if classification.sensitive || huge {
            false
        } else {
            read_head(&path)
                .map(|head| is_binary(&head))
                .unwrap_or(false)
        };
        let supported = symbols::is_supported(&language)
            && matches!(kind, FileKind::Source | FileKind::Test)
            && !classification.sensitive
            && !binary
            && !huge;
        files.push(FileEntry {
            path: relative,
            size,
            language,
            supported,
            kind,
            sensitive: classification.sensitive,
            sensitive_reason: classification.reason,
            binary,
            huge,
        });
    }

    let mut roots = BTreeMap::new();
    let mut source_roots = BTreeMap::new();
    let mut test_roots = BTreeMap::new();
    let mut languages: BTreeMap<String, (usize, u64)> = BTreeMap::new();
    let mut manifests = Vec::new();
    let mut build_files = Vec::new();
    let mut config_files = Vec::new();
    let mut entrypoints = Vec::new();
    let mut migrations = Vec::new();

    for file in &files {
        if let Some(segment) = file.path.split('/').next() {
            if file.path.contains('/') {
                roots.insert(segment.to_string(), ());
            }
        }
        if matches!(file.kind, FileKind::Source) {
            if let Some(segment) = file.path.split('/').next() {
                if file.path.contains('/') {
                    source_roots.insert(segment.to_string(), ());
                }
            }
        }
        if matches!(file.kind, FileKind::Test) {
            if let Some(segment) = file.path.split('/').next() {
                if file.path.contains('/') {
                    test_roots.insert(segment.to_string(), ());
                }
            }
        }
        if file.kind != FileKind::Doc || file.language == "other" {
            let entry = languages
                .entry(file.language.clone())
                .or_insert((0usize, 0u64));
            entry.0 += 1;
            entry.1 = entry.1.saturating_add(file.size);
        }
        match file.kind {
            FileKind::Manifest => manifests.push(file.path.clone()),
            FileKind::Build => build_files.push(file.path.clone()),
            FileKind::Config => config_files.push(file.path.clone()),
            FileKind::Entrypoint => entrypoints.push(file.path.clone()),
            FileKind::Migration => migrations.push(file.path.clone()),
            _ => {}
        }
    }

    Ok(RepoMap {
        roots: roots.into_keys().collect(),
        source_roots: source_roots.into_keys().collect(),
        test_roots: test_roots.into_keys().collect(),
        languages: languages
            .into_iter()
            .map(|(language, (count, bytes))| LanguageStat {
                language,
                files: count,
                bytes,
            })
            .collect(),
        manifests,
        build_files,
        config_files,
        entrypoints,
        migrations,
        git: snapshot.state.clone(),
        files,
        excluded_dirs: EXCLUDED_DIRS.iter().map(|dir| dir.to_string()).collect(),
        truncated,
    })
}

/// The repo-relative, forward-slash path of `path`.
pub fn relative_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

fn read_head(path: &Path) -> Option<Vec<u8>> {
    use std::io::Read;
    let mut file = fs::File::open(path).ok()?;
    let mut buffer = vec![0u8; 8192];
    let read = file.read(&mut buffer).ok()?;
    buffer.truncate(read);
    Some(buffer)
}

fn is_binary(bytes: &[u8]) -> bool {
    if bytes.contains(&0) {
        return true;
    }
    if bytes.is_empty() {
        return false;
    }
    let non_text = bytes
        .iter()
        .filter(|byte| !(byte.is_ascii_graphic() || byte.is_ascii_whitespace()))
        .count();
    non_text * 100 / bytes.len() > 30
}

/// Map a file name to a language identifier.
pub fn detect_language(name: &str) -> &'static str {
    let lower = name.to_ascii_lowercase();
    let extension = lower.rsplit_once('.').map(|(_, extension)| extension);
    match extension {
        Some("rs") => "rust",
        Some("ts") | Some("tsx") => "typescript",
        Some("js") | Some("jsx") | Some("mjs") | Some("cjs") => "javascript",
        Some("py") | Some("pyi") => "python",
        Some("go") => "go",
        Some("java") => "java",
        Some("kt") | Some("kts") => "kotlin",
        Some("c") | Some("h") => "c",
        Some("cc") | Some("cpp") | Some("cxx") | Some("hpp") => "cpp",
        Some("rb") => "ruby",
        Some("php") => "php",
        Some("cs") => "csharp",
        Some("swift") => "swift",
        Some("scala") => "scala",
        Some("sh") | Some("bash") | Some("zsh") => "shell",
        Some("sql") => "sql",
        Some("html") | Some("htm") => "html",
        Some("css") | Some("scss") => "css",
        Some("md") | Some("mdx") => "markdown",
        Some("json") => "json",
        Some("toml") => "toml",
        Some("yaml") | Some("yml") => "yaml",
        _ => "other",
    }
}

fn detect_kind(path: &str, name: &str, language: &str) -> FileKind {
    let lower = name.to_ascii_lowercase();
    if is_manifest(&lower) {
        return FileKind::Manifest;
    }
    if is_build(&lower) {
        return FileKind::Build;
    }
    if is_entrypoint(&lower) {
        return FileKind::Entrypoint;
    }
    if is_migration(path) {
        return FileKind::Migration;
    }
    if crate::context::gitdiff::is_test_path(path) {
        return FileKind::Test;
    }
    if matches!(language, "markdown") {
        return FileKind::Doc;
    }
    if is_config(&lower, language) {
        return FileKind::Config;
    }
    if symbols::is_supported(language) {
        return FileKind::Source;
    }
    FileKind::Other
}

fn is_manifest(lower: &str) -> bool {
    matches!(
        lower,
        "cargo.toml"
            | "cargo.lock"
            | "package.json"
            | "package-lock.json"
            | "pnpm-lock.yaml"
            | "yarn.lock"
            | "pyproject.toml"
            | "setup.py"
            | "setup.cfg"
            | "requirements.txt"
            | "poetry.lock"
            | "go.mod"
            | "go.sum"
            | "pom.xml"
            | "build.gradle"
            | "build.gradle.kts"
            | "gemfile"
            | "gemfile.lock"
            | "composer.json"
            | "mix.exs"
            | "pubspec.yaml"
    )
}

fn is_build(lower: &str) -> bool {
    matches!(
        lower,
        "makefile"
            | "gnumakefile"
            | "build.rs"
            | "dockerfile"
            | "justfile"
            | "cmakelists.txt"
            | "build.sh"
            | "rakefile"
    ) || lower.ends_with(".cmake")
}

fn is_entrypoint(lower: &str) -> bool {
    matches!(
        lower,
        "main.rs"
            | "lib.rs"
            | "main.ts"
            | "main.js"
            | "index.ts"
            | "index.js"
            | "index.tsx"
            | "index.jsx"
            | "main.py"
            | "app.py"
            | "__main__.py"
            | "__init__.py"
            | "main.go"
            | "program.cs"
    )
}

fn is_migration(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    lower.contains("/migrations/")
        || lower.starts_with("migrations/")
        || lower.contains("/migration/")
        || lower.starts_with("migration/")
        || lower.contains("/schema/")
}

fn is_config(lower: &str, language: &str) -> bool {
    matches!(
        lower,
        ".gitignore"
            | ".gitattributes"
            | ".editorconfig"
            | "tsconfig.json"
            | "vite.config.ts"
            | "vite.config.js"
            | "rustfmt.toml"
            | "clippy.toml"
            | "deny.toml"
    ) || matches!(language, "toml" | "yaml" | "json" | "ini" | "cfg")
}
