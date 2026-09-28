//! Deterministic integration tests for the context engine.
//!
//! No network, no model and no paid API is involved. Git tests are skipped
//! when the `git` binary is unavailable; everything else uses plain
//! directories.

use ocg::clock::FixedClock;
use ocg::context::cache::{self, CacheKey, ContextCache};
use ocg::context::capsule::TaskCapsule;
use ocg::context::config::ContextConfig;
use ocg::context::engine::ContextEngine;
use ocg::context::gitdiff::{self, DiffStatus, GitSnapshot};
use ocg::context::ranking;
use ocg::context::repomap::{self, FileKind};
use ocg::context::symbols::{self, SymbolKind};
use ocg::process::{FakeGitHost, GitHost, SystemGitHost};
use std::fs;
use std::path::Path;
use std::process::Command;

fn write(root: &Path, relative: &str, content: &str) {
    let path = root.join(relative);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create parent");
    }
    fs::write(path, content).expect("write fixture");
}

fn write_bytes(root: &Path, relative: &str, content: &[u8]) {
    let path = root.join(relative);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create parent");
    }
    fs::write(path, content).expect("write fixture");
}

fn config() -> ContextConfig {
    ContextConfig::default()
}

fn engine<'a>(root: &Path, git: &'a dyn GitHost, clock: &'a FixedClock) -> ContextEngine<'a> {
    ContextEngine::new(root, config(), git, clock)
}

/// A runtime-assembled placeholder secret. Keeping the pieces separate means
/// the repository hygiene scan never sees a credential shape in this file.
fn placeholder_secret() -> String {
    ["API", "_TOKEN=", "placeholder-value-not-real"].concat()
}

#[test]
fn repo_map_classifies_polyglot_and_ignores_build_dirs() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    write(root, "src/lib.rs", "pub fn a() {}\n");
    write(root, "src/main.rs", "fn main() {}\n");
    write(root, "web/app.ts", "export function run() {}\n");
    write(root, "web/app.test.ts", "test('x', () => {});\n");
    write(root, "py/mod.py", "def go():\n    pass\n");
    write(root, "node_modules/pkg/index.js", "module.exports = {};\n");
    write(root, "target/debug/artifact.rs", "fn hidden() {}\n");
    write(root, ".ocg/index/x.json", "{}\n");
    write(root, "Cargo.toml", "[package]\nname = \"x\"\n");
    write(root, "Makefile", "all:\n\ttrue\n");
    write(
        root,
        "migrations/0001_init.sql",
        "CREATE TABLE t (id INT);\n",
    );
    write(root, "README.md", "# docs\n");
    write_bytes(root, "logo.png", &[0u8, 1, 2, 3, 0]);
    let secret = placeholder_secret();
    write(root, ".env", &format!("{secret}\n"));

    let snapshot = GitSnapshot::not_a_repo();
    let map = repomap::build(root, &config(), &snapshot).unwrap();
    let paths: Vec<&str> = map.files.iter().map(|file| file.path.as_str()).collect();

    assert!(paths.contains(&"src/lib.rs"));
    assert!(paths.contains(&"web/app.ts"));
    assert!(paths.contains(&"py/mod.py"));
    assert!(!paths.contains(&"node_modules/pkg/index.js"));
    assert!(!paths.contains(&"target/debug/artifact.rs"));
    assert!(!paths.contains(&".ocg/index/x.json"));

    assert!(map
        .languages
        .iter()
        .any(|stat| stat.language == "rust" && stat.files >= 2));
    assert!(map
        .languages
        .iter()
        .any(|stat| stat.language == "typescript"));
    assert!(map.manifests.contains(&"Cargo.toml".to_string()));
    assert!(map.build_files.contains(&"Makefile".to_string()));
    assert!(map.entrypoints.contains(&"src/main.rs".to_string()));
    assert!(map
        .migrations
        .contains(&"migrations/0001_init.sql".to_string()));
    assert_eq!(map.file("web/app.test.ts").unwrap().kind, FileKind::Test);
    assert!(map.file("logo.png").unwrap().binary);
    assert!(map.file(".env").unwrap().sensitive);
    assert!(!map.file(".env").unwrap().supported);
}

#[test]
fn unsupported_language_keeps_path_metadata_only() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    write(root, "main.go", "package main\nfunc main() {}\n");
    let snapshot = GitSnapshot::not_a_repo();
    let map = repomap::build(root, &config(), &snapshot).unwrap();
    let entry = map.file("main.go").unwrap();
    assert_eq!(entry.language, "go");
    assert!(!entry.supported);

    let clock = FixedClock::new(1);
    let engine = engine(root, &SystemGitHost, &clock);
    let (index, _) = engine
        .repo_map()
        .and_then(|(map, snapshot)| engine.update_index(&map, &snapshot, None))
        .unwrap();
    assert!(index.file("main.go").unwrap().symbols.is_empty());
}

#[test]
fn incremental_index_reuses_updates_and_drops_files() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    write(root, "a.rs", "fn a() {}\n");
    let clock = FixedClock::new(10);
    let engine = engine(root, &SystemGitHost, &clock);

    let (map, snapshot) = engine.repo_map().unwrap();
    let (first, report) = engine.update_index(&map, &snapshot, None).unwrap();
    assert_eq!(report.metrics.updated, 1);
    assert!(report.changed_paths.contains(&"a.rs".to_string()));

    let (map, snapshot) = engine.repo_map().unwrap();
    let (second, report) = engine.update_index(&map, &snapshot, Some(&first)).unwrap();
    assert_eq!(report.metrics.reused, 1);
    assert_eq!(report.metrics.updated, 0);
    assert_eq!(second.generated_at, first.generated_at);

    write(root, "a.rs", "fn changed() {}\n");
    let (map, snapshot) = engine.repo_map().unwrap();
    let (third, report) = engine.update_index(&map, &snapshot, Some(&second)).unwrap();
    assert_eq!(report.metrics.updated, 1);
    let a_third = third.file("a.rs").unwrap();
    assert_eq!(a_third.symbols[0].name, "changed");

    write(root, "b.rs", "fn b() {}\n");
    let (map, snapshot) = engine.repo_map().unwrap();
    let (fourth, report) = engine.update_index(&map, &snapshot, Some(&third)).unwrap();
    assert!(fourth.file("a.rs").is_some());
    assert!(fourth.file("b.rs").is_some());
    assert!(report.changed_paths.contains(&"b.rs".to_string()));

    fs::remove_file(root.join("a.rs")).unwrap();
    let (map, snapshot) = engine.repo_map().unwrap();
    let (fifth, _) = engine.update_index(&map, &snapshot, Some(&fourth)).unwrap();
    assert!(fifth.file("a.rs").is_none());
    assert!(fifth.file("b.rs").is_some());
}

#[test]
fn symbol_queries_cover_definition_imports_slices_and_references() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    write(
        root,
        "src/parser.rs",
        "use crate::lexer::Lexer;\n\npub fn parse(input: &str) -> u32 {\n    input.len() as u32\n}\n",
    );
    write(
        root,
        "tests/parser_test.rs",
        "fn parse_again() {\n    let _ = parse(\"x\");\n}\n",
    );
    let clock = FixedClock::new(1);
    let engine = engine(root, &SystemGitHost, &clock);

    let definition = engine.definition("parse").unwrap().unwrap();
    assert_eq!(definition.path, "src/parser.rs");
    assert_eq!(definition.kind, SymbolKind::Function);
    assert_eq!((definition.start_line, definition.end_line), (3, 5));

    let slice = engine.source_slice(&definition).unwrap().unwrap();
    assert!(slice.contains("pub fn parse"));

    let imports = engine.imports("src/parser.rs").unwrap();
    assert!(imports
        .iter()
        .any(|symbol| symbol.name == "crate::lexer::Lexer"));

    let references = engine.probable_references("parse", 10).unwrap();
    assert!(references
        .iter()
        .any(|symbol| symbol.path == "tests/parser_test.rs"));

    let related = engine.related(&definition).unwrap();
    assert!(related
        .iter()
        .any(|symbol| symbol.path == "tests/parser_test.rs"));

    let hits = engine.search_symbols("parse", 10).unwrap();
    assert!(hits.len() >= 2);
}

#[test]
fn symbols_cover_rust_ts_js_and_python() {
    assert!(
        symbols::extract("rust", "struct A;\nimpl A {\n    fn m(&self) {}\n}\n", 10)
            .iter()
            .any(|symbol| symbol.kind == SymbolKind::Method)
    );
    assert!(
        symbols::extract("typescript", "export class C { m() {} }\n", 10)
            .iter()
            .any(|symbol| symbol.kind == SymbolKind::Class)
    );
    assert!(
        symbols::extract("javascript", "import x from 'y';\nfunction f() {}\n", 10)
            .iter()
            .any(|symbol| symbol.kind == SymbolKind::Import)
    );
    assert!(
        symbols::extract("python", "class C:\n    def m(self):\n        pass\n", 10)
            .iter()
            .any(|symbol| symbol.kind == SymbolKind::Method)
    );
    assert!(symbols::extract("go", "func main() {}\n", 10).is_empty());
}

#[test]
fn large_binary_and_sensitive_files_are_metadata_only() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    write(root, "big.rs", "fn a() {}\nfn b() {}\n");
    write_bytes(root, "blob.bin", &[0u8, 1, 2, 3, 4]);
    let secret = placeholder_secret();
    write(root, ".env", &format!("{secret}\n"));
    write(root, "id_rsa", "not a real key\n");

    let mut config = config();
    config.max_file_bytes = 10;
    let snapshot = GitSnapshot::not_a_repo();
    let map = repomap::build(root, &config, &snapshot).unwrap();
    assert!(map.file("big.rs").unwrap().huge);
    assert!(map.file("blob.bin").unwrap().binary);
    assert!(map.file(".env").unwrap().sensitive);
    assert!(map.file("id_rsa").unwrap().sensitive);

    let clock = FixedClock::new(1);
    let engine = ContextEngine::new(root, config, &SystemGitHost, &clock);
    let (map, snapshot) = engine.repo_map().unwrap();
    let (index, _) = engine.update_index(&map, &snapshot, None).unwrap();
    let secret_paths = [".env", "id_rsa", "blob.bin", "big.rs"];
    for path in secret_paths {
        let entry = index.file(path).unwrap();
        assert!(entry.symbols.is_empty(), "{path} must have no symbols");
    }
    assert!(index.file(".env").unwrap().excluded);
    let serialized = serde_json::to_string(&index).unwrap();
    assert!(!serialized.contains("placeholder-value-not-real"));
}

#[test]
fn plan_slices_never_include_sensitive_content() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let secret = placeholder_secret();
    write(root, ".env", &format!("{secret}\n"));
    write(root, "src/secret_store.rs", "fn token_store() {}\n");
    let clock = FixedClock::new(1);
    let engine = engine(root, &SystemGitHost, &clock);
    let outcome = engine.plan("token placeholder", None).unwrap();
    assert!(outcome.plan.slices.iter().all(|slice| slice.path != ".env"));
    assert!(outcome.plan.sensitive_excluded >= 1);
    assert!(outcome.plan.cacheable());
    let serialized = serde_json::to_string(&outcome.plan).unwrap();
    assert!(!serialized.contains("placeholder-value-not-real"));
}

#[test]
fn ranking_is_deterministic_and_respects_limits() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    for index in 0..8 {
        write(
            root,
            &format!("src/module{index}.rs"),
            &format!("pub fn handler{index}() {{}}\n"),
        );
    }
    write(root, "tests/handler_test.rs", "fn t() {}\n");
    let mut config = config();
    config.max_files = 2;
    config.max_slices = 2;
    config.max_bytes = 120;
    config.max_candidates = 4;
    let clock = FixedClock::new(5);
    let engine = ContextEngine::new(root, config.clone(), &SystemGitHost, &clock);
    let first = engine.plan("handler", None).unwrap();
    let second = engine.plan("handler", None).unwrap();
    assert_eq!(first.plan, second.plan);
    assert!(first.plan.candidates.len() <= config.max_candidates);
    assert!(first.plan.selected_files.len() <= config.max_files);
    assert!(first.plan.slices.len() <= config.max_slices);
    assert!(first.plan.selected_bytes <= config.max_bytes);
    assert_eq!(
        first.plan.estimated_tokens,
        first.plan.selected_bytes.div_ceil(4)
    );
}

#[test]
fn cache_hits_then_invalidates_on_relevant_change_only() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    write(root, "src/a.rs", "pub fn alpha() {}\n");
    write(root, "docs/notes.md", "# notes\n");
    let clock = FixedClock::new(3);
    let engine = engine(root, &SystemGitHost, &clock);

    let first = engine.plan("alpha", None).unwrap();
    assert!(!first.from_cache);
    let second = engine.plan("alpha", None).unwrap();
    assert!(second.from_cache);

    // Unrelated, unsupported file change: it was never a plan dependency.
    write(root, "docs/notes.md", "# different notes\n");
    let third = engine.plan("alpha", None).unwrap();
    assert!(third.from_cache, "unrelated change must not invalidate");

    // Relevant file change: alpha source changed, so the entry is stale.
    write(root, "src/a.rs", "pub fn alpha_two() {}\n");
    let fourth = engine.plan("alpha", None).unwrap();
    assert!(!fourth.from_cache);
}

#[test]
fn corrupt_cache_is_ignored_and_recomputed() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    write(root, "src/a.rs", "pub fn alpha() {}\n");
    let clock = FixedClock::new(3);
    let engine = engine(root, &SystemGitHost, &clock);
    engine.plan("alpha", None).unwrap();

    let cache_dir = ContextCache::new(root).dir().to_path_buf();
    let entry = fs::read_dir(&cache_dir)
        .unwrap()
        .flatten()
        .map(|entry| entry.path())
        .find(|path| path.extension().map(|ext| ext == "json").unwrap_or(false))
        .expect("cache entry");
    fs::write(&entry, "{not json").unwrap();

    let outcome = engine.plan("alpha", None).unwrap();
    assert!(!outcome.from_cache);
}

#[test]
fn cache_clean_never_touches_the_runtime() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    write(root, "src/a.rs", "pub fn alpha() {}\n");
    let runtime = root.join(".ocg/runtime/opencode/1.18.31");
    fs::create_dir_all(&runtime).unwrap();
    let clock = FixedClock::new(3);
    let engine = engine(root, &SystemGitHost, &clock);
    engine.plan("alpha", None).unwrap();
    let report = engine.cache_clean().unwrap();
    assert!(report.removed_entries >= 1);
    assert!(runtime.exists());
}

#[test]
fn capsule_roundtrip_and_stale_detection() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    write(root, "src/a.rs", "fn a() {}\n");
    let clock = FixedClock::new(7);
    let engine = engine(root, &SystemGitHost, &clock);
    let outcome = engine.plan("a", None).unwrap();

    let mut capsule = TaskCapsule::new("a");
    capsule.files.push(ocg::context::capsule::CapsuleFile {
        path: "src/a.rs".to_string(),
        reason: Some("source".to_string()),
        changed: false,
    });
    capsule.provenance = outcome.plan.provenance.clone();
    capsule
        .verification
        .push(ocg::context::capsule::Verification::pending("cargo test"));
    capsule.recompute_size();
    let text = capsule.to_json().unwrap();
    let parsed = TaskCapsule::from_json(&text).unwrap();
    assert_eq!(capsule, parsed);
    assert!(!capsule.is_stale(root).unwrap());

    write(root, "src/a.rs", "fn b() {}\n");
    assert!(capsule.is_stale(root).unwrap());
}

#[test]
fn non_git_repositories_use_the_graceful_fallback() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    write(root, "src/a.rs", "fn a() {}\n");
    let fake = FakeGitHost::new();
    let snapshot = GitSnapshot::collect(root, &fake);
    assert!(!snapshot.state.is_repo);
    assert!(snapshot.entries.is_empty());

    let clock = FixedClock::new(1);
    let engine = engine(root, &fake, &clock);
    let outcome = engine.plan("a", None).unwrap();
    assert!(outcome.plan.changed_paths.is_empty());
    assert!(!outcome.plan.git.is_repo);
}

#[test]
fn git_repository_context_cache_hits_and_invalidates() {
    if !git_available() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    write(root, "src/a.rs", "pub fn alpha() {}\n");
    write(root, ".gitignore", ".ocg/\n");
    git(root, &["init", "-q"]);
    git(root, &["add", "-A"]);
    git(root, &["commit", "-qm", "init"]);

    let clock = FixedClock::new(9);
    let engine = engine(root, &SystemGitHost, &clock);
    let first = engine.plan("alpha", None).unwrap();
    assert!(!first.from_cache);
    let second = engine.plan("alpha", None).unwrap();
    assert!(second.from_cache, "clean git files must still cache-hit");

    // A worktree status change (new untracked file) must invalidate the plan so
    // `changed_paths` reflects current git state.
    write(root, "src/b.rs", "pub fn beta() {}\n");
    let third = engine.plan("alpha", None).unwrap();
    assert!(!third.from_cache, "status change must invalidate");
    assert!(third.plan.changed_paths.contains(&"src/b.rs".to_string()));

    // Committing changes HEAD, so the plan must be recomputed as clean.
    git(root, &["add", "-A"]);
    git(root, &["commit", "-qm", "add b"]);
    let fourth = engine.plan("alpha", None).unwrap();
    assert!(!fourth.from_cache, "commit must invalidate");
    assert!(fourth.plan.changed_paths.is_empty());
    assert!(fourth.plan.provenance.validated);
}

#[test]
fn git_blob_fingerprints_and_diff_are_bounded_but_complete() {
    if !git_available() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    write(root, "src/a.rs", "pub fn alpha() {}\n");
    write(root, "src/b.rs", "pub fn beta() {}\n");
    write(root, "src/gone.rs", "pub fn gone() {}\n");
    write(root, ".gitignore", ".ocg/\n");
    git(root, &["init", "-q"]);
    git(root, &["add", "-A"]);
    git(root, &["commit", "-qm", "init"]);

    let clock = FixedClock::new(2);
    let engine = engine(root, &SystemGitHost, &clock);
    let (map, snapshot) = engine.repo_map().unwrap();
    let (first, report) = engine.update_index(&map, &snapshot, None).unwrap();
    assert_eq!(report.metrics.updated, 3);
    let a_entry = first.file("src/a.rs").unwrap();
    assert!(
        a_entry.fingerprint.starts_with("blob:"),
        "clean tracked files should use git blob ids"
    );
    assert!(a_entry.git_blob.is_some());

    // Unchanged: everything is reused from the blob fingerprints.
    let (map, snapshot) = engine.repo_map().unwrap();
    let (second, report) = engine.update_index(&map, &snapshot, Some(&first)).unwrap();
    assert_eq!(report.metrics.reused, 3);
    assert_eq!(second.generated_at, first.generated_at);

    // Change set: modify, rename, delete and add. The rename is staged so git
    // reports it as a rename; the modification stays unstaged so it appears in
    // the working-tree diff.
    write(root, "src/a.rs", "pub fn alpha() {}\n\npub fn extra() {}\n");
    fs::rename(root.join("src/b.rs"), root.join("src/c.rs")).unwrap();
    fs::remove_file(root.join("src/gone.rs")).unwrap();
    write(root, "src/new.rs", "pub fn brand_new() {}\n");
    git(root, &["add", "src/b.rs", "src/c.rs"]);

    let (map, snapshot) = engine.repo_map().unwrap();
    let (index, _) = engine.update_index(&map, &snapshot, Some(&second)).unwrap();
    let diff = gitdiff::compute(root, &SystemGitHost, &snapshot, &index, &config()).unwrap();

    let mut entry_paths: Vec<(&str, DiffStatus)> = diff
        .entries
        .iter()
        .map(|entry| (entry.path.as_str(), entry.status))
        .collect();
    entry_paths.sort();
    let expected = [
        ("src/a.rs", DiffStatus::Modified),
        ("src/c.rs", DiffStatus::Renamed),
        ("src/gone.rs", DiffStatus::Deleted),
        ("src/new.rs", DiffStatus::Added),
    ];
    assert_eq!(entry_paths, expected.to_vec());
    let renamed = diff
        .entries
        .iter()
        .find(|entry| entry.path == "src/c.rs")
        .unwrap();
    assert_eq!(renamed.old_path.as_deref(), Some("src/b.rs"));

    // Every changed path is present, even when hunks are heavily bounded.
    let mut tiny = config();
    tiny.max_hunks = 1;
    tiny.max_diff_bytes = 1;
    let bounded = gitdiff::compute(root, &SystemGitHost, &snapshot, &index, &tiny).unwrap();
    assert_eq!(bounded.entries.len(), 4);
    assert!(bounded.truncated);
    assert!(bounded.structural.is_some());
    assert!(
        bounded.hunks.len() <= 1,
        "retained hunks must respect maxHunks"
    );
    let body_bytes: usize = bounded
        .hunks
        .iter()
        .map(|hunk| hunk.body.iter().map(|line| line.len() + 1).sum::<usize>())
        .sum();
    assert!(body_bytes <= tiny.max_diff_bytes);

    // Changed symbols include the newly added function in the modified file.
    assert!(diff
        .changed_symbols
        .iter()
        .any(|symbol| symbol.name == "extra" && symbol.path == "src/a.rs"));
    // A renamed file is sourced from its new path...
    assert!(diff
        .changed_symbols
        .iter()
        .any(|symbol| symbol.name == "beta" && symbol.path == "src/c.rs"));
    // ...while a deleted path is recorded as unsourced and never guessed.
    assert!(diff.unsourced_paths.contains(&"src/gone.rs".to_string()));
    assert!(!diff
        .changed_symbols
        .iter()
        .any(|symbol| symbol.path == "src/gone.rs"));
}

#[test]
fn cache_dependencies_only_include_relevant_files() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    write(root, "src/a.rs", "fn a() {}\n");
    write(root, "docs/notes.md", "# notes\n");
    let clock = FixedClock::new(1);
    let engine = engine(root, &SystemGitHost, &clock);
    let outcome = engine.plan("a", None).unwrap();
    let deps = cache::dependencies(&outcome.plan);
    assert!(deps.iter().any(|dep| dep.path == "src/a.rs"));
    assert!(!deps.iter().any(|dep| dep.path == "src/unrelated.rs"));
    assert!(!deps.iter().any(|dep| dep.path == "docs/notes.md"));

    // A second identical query is served from the cache without re-assembly.
    let second = engine.plan("a", None).unwrap();
    assert!(second.from_cache);

    // The key holds identity + query; the entry carries the dependencies.
    let key = CacheKey::new(
        &outcome.plan.repo_id,
        &engine.cache_config_fingerprint(),
        &ranking::fingerprint_text("a|"),
        &gitdiff::snapshot_fingerprint(&GitSnapshot::not_a_repo()),
    );
    let cache = ContextCache::new(root);
    assert!(cache.get(root, &key).is_some());
}

#[test]
fn plan_creation_ignores_gear_state_in_a_gitignore() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    write(root, "src/a.rs", "fn a() {}\n");
    let clock = FixedClock::new(1);
    let engine = engine(root, &SystemGitHost, &clock);
    engine.plan("a", None).unwrap();
    let gitignore = fs::read_to_string(root.join(".gitignore")).unwrap();
    assert_eq!(gitignore, ".ocg/\n");
    // Idempotent: a second plan must not duplicate the entry.
    engine.plan("a", None).unwrap();
    assert_eq!(
        fs::read_to_string(root.join(".gitignore")).unwrap(),
        gitignore
    );
}

#[test]
fn truncation_notes_are_visible_in_human_text() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    for index in 0..6 {
        write(
            root,
            &format!("src/m{index}.rs"),
            &format!("pub fn handler{index}() {{}}\n"),
        );
    }
    let mut config = config();
    config.max_slices = 1;
    config.max_bytes = 1;
    let clock = FixedClock::new(1);
    let engine = ContextEngine::new(root, config, &SystemGitHost, &clock);
    let outcome = engine.plan("handler", None).unwrap();
    assert!(outcome.plan.truncated);
    assert!(outcome
        .plan
        .notes
        .iter()
        .any(|note| note.contains("truncated")));
    let text = ocg::context::plan_text(&outcome.plan);
    assert!(text.contains("truncated"), "{text}");
    assert!(text.contains("fresh (validated)"), "{text}");
    assert!(text.contains("notes:"), "{text}");
}

#[test]
fn repository_file_cap_marks_plan_and_index_truncated() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    for index in 0..4 {
        write(
            root,
            &format!("src/m{index}.rs"),
            &format!("pub fn handler{index}() {{}}\n"),
        );
    }
    let mut config = config();
    config.max_repository_files = 2;
    let clock = FixedClock::new(1);
    let engine = ContextEngine::new(root, config, &SystemGitHost, &clock);
    let outcome = engine.plan("handler", None).unwrap();
    assert!(
        outcome.plan.truncated,
        "capped plan must be marked truncated"
    );
    assert!(outcome
        .plan
        .notes
        .iter()
        .any(|note| note.contains("maxRepositoryFiles")));
    let text = ocg::context::plan_text(&outcome.plan);
    assert!(text.contains("maxRepositoryFiles"), "{text}");

    let index = engine.load_index().unwrap();
    assert!(index.truncated);
    assert!(index.metrics.truncated);
    assert_eq!(index.files.len(), 2);
}

#[test]
fn repository_file_cap_preserves_git_changed_paths() {
    if !git_available() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    write(root, "src/a.rs", "pub fn alpha() {}\n");
    write(root, "src/b.rs", "pub fn beta() {}\n");
    write(root, "src/c.rs", "pub fn gamma() {}\n");
    write(root, ".gitignore", ".ocg/\n");
    git(root, &["init", "-q"]);
    git(root, &["add", "-A"]);
    git(root, &["commit", "-qm", "init"]);

    let mut config = config();
    config.max_repository_files = 1;
    let clock = FixedClock::new(1);
    let engine = ContextEngine::new(root, config, &SystemGitHost, &clock);
    write(root, "src/a.rs", "pub fn alpha_changed() {}\n");
    write(root, "src/b.rs", "pub fn beta_changed() {}\n");

    let outcome = engine.plan("alpha", None).unwrap();
    assert!(outcome.plan.truncated);
    // Git status is collected separately, so changed paths stay complete even
    // though the map/index only holds the capped subset.
    assert!(outcome.plan.changed_paths.contains(&"src/a.rs".to_string()));
    assert!(outcome.plan.changed_paths.contains(&"src/b.rs".to_string()));
    let index = engine.load_index().unwrap();
    assert!(index.truncated);
    assert_eq!(index.files.len(), 1);
}

#[test]
fn cache_write_failure_keeps_the_plan() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    write(root, "src/a.rs", "pub fn alpha() {}\n");
    // Make `.ocg/cache` a file so the cache write fails.
    fs::create_dir_all(root.join(".ocg")).unwrap();
    fs::write(root.join(".ocg/cache"), "not a directory\n").unwrap();
    let clock = FixedClock::new(1);
    let engine = engine(root, &SystemGitHost, &clock);
    let outcome = engine.plan("alpha", None).unwrap();
    assert!(!outcome.from_cache);
    assert!(outcome
        .plan
        .slices
        .iter()
        .any(|slice| slice.path == "src/a.rs"));
    assert!(
        outcome
            .plan
            .notes
            .iter()
            .any(|note| note.contains("cache was not written")),
        "expected a cache warning note, got {:?}",
        outcome.plan.notes
    );
    assert!(outcome
        .warnings
        .iter()
        .any(|warning| warning.contains("cache was not written")));
}

#[test]
fn prepare_is_silent_when_the_context_is_disabled() {
    use ocg::config::build_effective;
    use ocg::defaults::{load_defaults, OcgSource};
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let project = root.join(".ocg.yaml");
    write(root, ".ocg.yaml", "{\"context\": {\"enabled\": false}}\n");
    let defaults = load_defaults(&OcgSource::Embedded).unwrap();
    let effective = build_effective(
        defaults,
        None,
        root,
        &root.join("no-user.yaml"),
        &project,
        None,
    )
    .unwrap();
    let clock = FixedClock::new(1);
    let git = SystemGitHost;
    assert!(ocg::context::prepare_explicit_with(root, &effective, &git, &clock).is_empty());
    assert!(
        !root.join(".ocg/index").exists(),
        "disabled context must not build an index"
    );
}

fn git_available() -> bool {
    Command::new("git")
        .arg("--version")
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

fn git(dir: &Path, args: &[&str]) {
    let email = ["ocg", "example", "invalid"].join("@");
    let output = Command::new("git")
        .args([
            "-c",
            &format!("user.email={email}"),
            "-c",
            "user.name=ocg-test",
            "-c",
            "commit.gpgsign=false",
        ])
        .args(args)
        .current_dir(dir)
        .output()
        .expect("run git");
    assert!(
        output.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
