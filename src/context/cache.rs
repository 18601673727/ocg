//! Local context cache with fine-grained invalidation.
//!
//! An entry is keyed by repo identity, engine/schema/config versions, a query
//! fingerprint and the exact dependency fingerprints it was built from. A
//! change to an unrelated file therefore does not invalidate an entry that
//! does not depend on it. Sensitive content never reaches the cache, and a
//! corrupt, stale or version-mismatched entry is removed and ignored.
//!
//! The cache lives at `<project>/.ocg/cache/context/`; `cache clean`
//! removes that directory only and never touches `.ocg/runtime/`.

use crate::context::freshness::{validate, SourceFingerprint, ENGINE_VERSION, SCHEMA_VERSION};
use crate::context::ranking::ContextPlan;
use crate::error::{OcgError, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

/// The cache container under `.ocg/`.
pub const CACHE_DIR: &str = "cache";
/// The context cache subdirectory.
pub const CACHE_CONTEXT_DIR: &str = "context";

/// Everything that distinguishes one cached plan from another. The dependency
/// fingerprints are stored on the entry (not in the key) so a plan can be
/// looked up before it is assembled; [`ContextCache::get`] still validates them.
/// `git_fingerprint` covers HEAD, branch and the complete sorted status entries,
/// so a commit or status change can never serve a stale `changed_paths`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheKey {
    pub repo_id: String,
    pub engine_version: String,
    pub schema_version: u32,
    pub config_fingerprint: String,
    pub query_fingerprint: String,
    pub git_fingerprint: String,
}

impl CacheKey {
    pub fn new(
        repo_id: &str,
        config_fingerprint: &str,
        query_fingerprint: &str,
        git_fingerprint: &str,
    ) -> Self {
        Self {
            repo_id: repo_id.to_string(),
            engine_version: ENGINE_VERSION.to_string(),
            schema_version: SCHEMA_VERSION,
            config_fingerprint: config_fingerprint.to_string(),
            query_fingerprint: query_fingerprint.to_string(),
            git_fingerprint: git_fingerprint.to_string(),
        }
    }

    fn file_name(&self) -> String {
        let value = serde_json::to_value(self).unwrap_or(serde_json::Value::Null);
        format!(
            "{}.json",
            crate::runtime::hash::sha256_hex(value.to_string().as_bytes())
        )
    }
}

/// The persisted cache entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheEntry {
    pub schema_version: u32,
    pub engine_version: String,
    pub created_at: i64,
    pub key: CacheKey,
    pub dependencies: Vec<SourceFingerprint>,
    pub plan: ContextPlan,
}

/// Aggregate cache statistics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheStats {
    pub dir: String,
    pub entries: usize,
    pub bytes: u64,
    pub corrupt: usize,
    pub oldest: Option<i64>,
    pub newest: Option<i64>,
}

/// Outcome of a cache clean.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CleanReport {
    pub dir: String,
    pub removed_entries: usize,
    pub removed_bytes: u64,
}

/// A handle to one repository's context cache.
#[derive(Debug, Clone)]
pub struct ContextCache {
    root: PathBuf,
    dir: PathBuf,
}

impl ContextCache {
    pub fn new(root: &Path) -> Self {
        Self {
            root: root.to_path_buf(),
            dir: root
                .join(crate::context::repomap::OCG_DIR)
                .join(CACHE_DIR)
                .join(CACHE_CONTEXT_DIR),
        }
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Look up a fresh, trusted entry. Corrupt, stale, mismatched or tampered
    /// entries are removed and ignored.
    pub fn get(&self, root: &Path, key: &CacheKey) -> Option<ContextPlan> {
        let path = self.dir.join(key.file_name());
        let text = fs::read_to_string(&path).ok()?;
        let entry: CacheEntry = match serde_json::from_str(&text) {
            Ok(entry) => entry,
            Err(_) => {
                let _ = fs::remove_file(&path);
                return None;
            }
        };
        if entry.schema_version != SCHEMA_VERSION || entry.engine_version != ENGINE_VERSION {
            let _ = fs::remove_file(&path);
            return None;
        }
        if entry.key != *key {
            let _ = fs::remove_file(&path);
            return None;
        }
        match validate(root, &entry.dependencies) {
            Ok(report) if report.is_fresh() => {}
            _ => {
                let _ = fs::remove_file(&path);
                return None;
            }
        }
        // Trust the *content*, not the stored claim: each slice must still match
        // the current non-sensitive source lines, and no selected path may be
        // sensitive. A poisoned entry is discarded.
        if !trusted(root, &entry.plan) {
            let _ = fs::remove_file(&path);
            return None;
        }
        let mut plan = entry.plan;
        plan.provenance.stale = false;
        // Only now, after git identity + dependencies + slices were checked.
        plan.provenance.validated = true;
        Some(plan)
    }

    /// Store a plan, unless it contains sensitive content.
    pub fn put(
        &self,
        key: &CacheKey,
        dependencies: &[SourceFingerprint],
        plan: &ContextPlan,
        now: i64,
    ) -> Result<bool> {
        if !plan.cacheable() {
            return Ok(false);
        }
        // Keep `.ocg/` out of the project's VCS, exactly once.
        crate::runtime::install::ensure_gitignore(&self.root)?;
        let entry = CacheEntry {
            schema_version: SCHEMA_VERSION,
            engine_version: ENGINE_VERSION.to_string(),
            created_at: now,
            key: key.clone(),
            dependencies: dependencies.to_vec(),
            plan: plan.clone(),
        };
        let value = serde_json::to_value(&entry).map_err(|error| {
            OcgError::config(format!("cannot serialize the context cache entry: {error}"))
        })?;
        crate::runtime::install::write_json_atomic(&self.dir.join(key.file_name()), &value)?;
        Ok(true)
    }

    /// Remove the context cache directory. The index and the managed runtime
    /// live elsewhere and are never touched.
    pub fn clean(&self) -> Result<CleanReport> {
        let stats = self.stats();
        if self.dir.exists() {
            fs::remove_dir_all(&self.dir).map_err(|error| {
                OcgError::io(format!("cannot remove {}", self.dir.display()), error)
            })?;
        }
        Ok(CleanReport {
            dir: stats.dir,
            removed_entries: stats.entries + stats.corrupt,
            removed_bytes: stats.bytes,
        })
    }

    /// Count and measure entries. Corrupt files are counted, not trusted.
    pub fn stats(&self) -> CacheStats {
        let mut stats = CacheStats {
            dir: self.dir.to_string_lossy().into_owned(),
            entries: 0,
            bytes: 0,
            corrupt: 0,
            oldest: None,
            newest: None,
        };
        let Ok(entries) = fs::read_dir(&self.dir) else {
            return stats;
        };
        let mut newest = None;
        let mut oldest = None;
        for entry in entries.flatten() {
            let metadata = match entry.metadata() {
                Ok(metadata) => metadata,
                Err(_) => continue,
            };
            if !metadata.is_file() {
                continue;
            }
            stats.bytes = stats.bytes.saturating_add(metadata.len());
            let Ok(text) = fs::read_to_string(entry.path()) else {
                stats.corrupt += 1;
                continue;
            };
            match serde_json::from_str::<CacheEntry>(&text) {
                Ok(parsed)
                    if parsed.schema_version == SCHEMA_VERSION
                        && parsed.engine_version == ENGINE_VERSION =>
                {
                    stats.entries += 1;
                    match oldest {
                        Some(value) if value <= parsed.created_at => {}
                        _ => oldest = Some(parsed.created_at),
                    }
                    match newest {
                        Some(value) if value >= parsed.created_at => {}
                        _ => newest = Some(parsed.created_at),
                    }
                }
                _ => stats.corrupt += 1,
            }
        }
        stats.oldest = oldest;
        stats.newest = newest;
        stats
    }
}

/// Build provenance dependencies from a plan.
pub fn dependencies(plan: &ContextPlan) -> Vec<SourceFingerprint> {
    let mut deps: Vec<SourceFingerprint> = plan
        .provenance
        .sources
        .iter()
        .filter(|source| plan.slices.iter().any(|slice| slice.path == source.path))
        .cloned()
        .collect();
    deps.sort_by(|a, b| a.path.cmp(&b.path));
    deps.dedup_by(|a, b| a.path == b.path);
    deps
}

/// A path a cached plan is allowed to reference: relative, no `..`, `.` or
/// absolute components. Rejects crafted entries that try to point outside the
/// project.
fn safe_relative(path: &str) -> bool {
    if path.is_empty() || path.starts_with('/') || path.starts_with('\\') {
        return false;
    }
    Path::new(path)
        .components()
        .all(|component| matches!(component, std::path::Component::Normal(_)))
}

/// Verify that a cached plan's selected paths and slice contents still match
/// the current working tree. Never trust the stored bytes alone.
fn trusted(root: &Path, plan: &ContextPlan) -> bool {
    if !plan.cacheable() {
        return false;
    }
    for path in &plan.selected_files {
        if !safe_relative(path) || crate::context::classify::classify(Path::new(path)).sensitive {
            return false;
        }
    }
    for slice in &plan.slices {
        if !safe_relative(&slice.path)
            || crate::context::classify::classify(Path::new(&slice.path)).sensitive
        {
            return false;
        }
        let Ok(content) = fs::read_to_string(root.join(&slice.path)) else {
            return false;
        };
        let lines: Vec<&str> = content.lines().collect();
        let start = slice.start_line.saturating_sub(1) as usize;
        let end = (slice.end_line as usize).min(lines.len());
        if start >= lines.len() || start >= end {
            return false;
        }
        let current = lines[start..end].join("\n");
        if current != slice.content || current.len() != slice.bytes {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::ranking::{estimated_tokens, sections, ContextLimits};
    use std::fs;

    fn plan(path: &str, content: &str, fingerprint: &str, _now: i64) -> ContextPlan {
        let bytes = content.len();
        ContextPlan {
            schema_version: SCHEMA_VERSION,
            engine_version: ENGINE_VERSION.to_string(),
            task: "task".to_string(),
            role: None,
            repo_id: "repo".to_string(),
            root: ".".to_string(),
            git: Default::default(),
            sections: sections(),
            candidates: vec![],
            selected_files: vec![path.to_string()],
            changed_paths: vec![],
            diff: None,
            slices: vec![crate::context::ranking::ContextSlice {
                path: path.to_string(),
                start_line: 1,
                end_line: content.lines().count().max(1) as u32,
                content: content.to_string(),
                bytes,
                estimated_tokens: estimated_tokens(bytes),
            }],
            candidate_bytes: 0,
            selected_bytes: bytes,
            estimated_tokens: estimated_tokens(bytes),
            limits: ContextLimits {
                max_candidates: 1,
                max_files: 1,
                max_slices: 1,
                max_bytes: 1,
                max_diff_bytes: 1,
                max_hunks: 1,
                max_file_bytes: 1,
                max_symbols_per_file: 1,
                max_repository_files: 1,
            },
            provenance: crate::context::freshness::Provenance {
                sources: vec![SourceFingerprint {
                    path: path.to_string(),
                    fingerprint: fingerprint.to_string(),
                    size: bytes as u64,
                }],
                ..crate::context::freshness::Provenance::default()
            },
            sensitive_excluded: 0,
            truncated: false,
            notes: vec![],
            instructions: Default::default(),
            policy: Default::default(),
            capabilities: Default::default(),
            capsule: None,
            test_proposal: None,
            verification: Default::default(),
        }
    }

    #[test]
    fn round_trips_and_invalidates_on_change() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("a.rs");
        fs::write(&file, "fn a() {}").unwrap();
        let bytes = fs::read(&file).unwrap();
        let fingerprint = format!("sha256:{}", crate::runtime::hash::sha256_hex(&bytes));
        let stored = plan("a.rs", "fn a() {}", &fingerprint, 1);
        let deps = dependencies(&stored);
        let cache = ContextCache::new(dir.path());
        let key = CacheKey::new("repo", "config", "query", "git");
        assert!(cache.put(&key, &deps, &stored, 1).unwrap());
        assert!(cache.get(dir.path(), &key).is_some());

        fs::write(&file, "fn b() {}").unwrap();
        assert!(cache.get(dir.path(), &key).is_none());

        let other = dir.path().join("b.rs");
        fs::write(&other, "fn c() {}").unwrap();
        let other_bytes = fs::read(&other).unwrap();
        let other_fingerprint =
            format!("sha256:{}", crate::runtime::hash::sha256_hex(&other_bytes));
        let plan2 = plan("b.rs", "fn c() {}", &other_fingerprint, 2);
        let deps2 = dependencies(&plan2);
        let key2 = CacheKey::new("repo", "config", "query", "git");
        cache.put(&key2, &deps2, &plan2, 2).unwrap();
        // Changing a.rs (unrelated to b.rs) must not invalidate the b.rs entry.
        fs::write(&file, "fn d() {}").unwrap();
        assert!(cache.get(dir.path(), &key2).is_some());
    }

    #[test]
    fn poisoned_slice_content_is_discarded() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("a.rs"), "fn a() {}").unwrap();
        let bytes = fs::read(dir.path().join("a.rs")).unwrap();
        let fingerprint = format!("sha256:{}", crate::runtime::hash::sha256_hex(&bytes));
        let stored = plan("a.rs", "fn a() {}", &fingerprint, 1);
        let deps = dependencies(&stored);
        let cache = ContextCache::new(dir.path());
        let key = CacheKey::new("repo", "config", "query", "git");
        cache.put(&key, &deps, &stored, 1).unwrap();

        let path = cache.dir().join(key.file_name());
        let mut value: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        value["plan"]["slices"][0]["content"] = serde_json::json!("fn evil() {}");
        fs::write(&path, value.to_string()).unwrap();

        assert!(cache.get(dir.path(), &key).is_none());
        assert!(!path.exists(), "poisoned entry must be removed");
    }

    #[test]
    fn sensitive_selected_path_is_discarded() {
        let dir = tempfile::tempdir().unwrap();
        let cache = ContextCache::new(dir.path());
        let key = CacheKey::new("repo", "config", "query", "git");
        let mut stored = plan("a.rs", "fn a() {}", "sha256:fp", 1);
        stored.slices.clear();
        stored.selected_files = vec![".env".to_string()];
        stored.provenance.sources.clear();
        // `cacheable` only inspects slices, so this entry is stored...
        assert!(cache.put(&key, &[], &stored, 1).unwrap());
        // ...but get must reject a sensitive selected path.
        assert!(cache.get(dir.path(), &key).is_none());
    }

    #[test]
    fn path_traversal_slice_is_discarded() {
        let dir = tempfile::tempdir().unwrap();
        let cache = ContextCache::new(dir.path());
        let key = CacheKey::new("repo", "config", "query", "git");
        let mut stored = plan("a.rs", "fn a() {}", "sha256:fp", 1);
        stored.slices[0].path = "../outside.rs".to_string();
        stored.provenance.sources.clear();
        assert!(cache.put(&key, &[], &stored, 1).unwrap());
        assert!(cache.get(dir.path(), &key).is_none());
    }

    #[test]
    fn corrupt_entries_are_removed() {
        let dir = tempfile::tempdir().unwrap();
        let cache = ContextCache::new(dir.path());
        fs::create_dir_all(cache.dir()).unwrap();
        let key = CacheKey::new("repo", "config", "query", "git");
        fs::write(cache.dir().join(key.file_name()), "{not json").unwrap();
        assert!(cache.get(dir.path(), &key).is_none());
        assert!(!cache.dir().join(key.file_name()).exists());
    }

    #[test]
    fn clean_only_removes_the_cache_directory() {
        let dir = tempfile::tempdir().unwrap();
        let cache = ContextCache::new(dir.path());
        fs::create_dir_all(cache.dir()).unwrap();
        fs::write(cache.dir().join("x.json"), "{}").unwrap();
        let runtime = dir.path().join(".ocg/runtime/opencode");
        fs::create_dir_all(&runtime).unwrap();
        let report = cache.clean().unwrap();
        assert_eq!(report.removed_entries, 1);
        assert!(!cache.dir().exists());
        assert!(runtime.exists(), "runtime must never be cleaned");
    }

    #[test]
    fn stats_count_valid_and_corrupt_entries() {
        let dir = tempfile::tempdir().unwrap();
        let cache = ContextCache::new(dir.path());
        fs::create_dir_all(cache.dir()).unwrap();
        let key = CacheKey::new("repo", "config", "query", "git");
        let plan = plan("a.rs", "fn a() {}", "sha256:fp", 1);
        cache.put(&key, &[], &plan, 1).unwrap();
        fs::write(cache.dir().join("bad.json"), "nope").unwrap();
        let stats = cache.stats();
        assert_eq!(stats.entries, 1);
        assert_eq!(stats.corrupt, 1);
    }

    #[test]
    fn put_creates_the_gitignore_entry() {
        let dir = tempfile::tempdir().unwrap();
        let cache = ContextCache::new(dir.path());
        let key = CacheKey::new("repo", "config", "query", "git");
        let plan = plan("a.rs", "fn a() {}", "sha256:fp", 1);
        cache.put(&key, &[], &plan, 1).unwrap();
        let gitignore = fs::read_to_string(dir.path().join(".gitignore")).unwrap();
        assert_eq!(gitignore, ".ocg/\n");
    }
}
