//! Local context cache with fine-grained invalidation.
//!
//! An entry is keyed by repo identity, engine/schema/config versions, a query
//! fingerprint and the exact dependency fingerprints it was built from. A
//! change to an unrelated file therefore does not invalidate an entry that
//! does not depend on it. Sensitive content never reaches the cache, and a
//! corrupt, stale or version-mismatched entry is removed and ignored.
//!
//! Entries are Project documents in the OCG database, at most
//! [`MAX_ENTRIES`] per Project; `cache clean` removes them and nothing else.

use crate::context::freshness::{validate, SourceFingerprint, ENGINE_VERSION, SCHEMA_VERSION};
use crate::context::ranking::ContextPlan;
use crate::error::{OcgError, Result};
use crate::project_store::ProjectDocuments;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// The document kind cache entries are stored under.
const DOCUMENT_KIND: &str = "context-cache";
/// The most entries one Project keeps; the oldest are pruned beyond it.
pub const MAX_ENTRIES: usize = 256;

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

    fn document_key(&self) -> String {
        let value = serde_json::to_value(self).unwrap_or(serde_json::Value::Null);
        crate::hash::sha256_hex(value.to_string().as_bytes())
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
}

impl ContextCache {
    pub fn new(root: &Path) -> Self {
        Self {
            root: root.to_path_buf(),
        }
    }

    /// Where the entries live, for reports.
    pub fn dir(&self) -> String {
        format!("ocg.db:{DOCUMENT_KIND}")
    }

    /// Look up a fresh, trusted entry. Corrupt, stale, mismatched or tampered
    /// entries are removed and ignored.
    pub fn get(&self, root: &Path, key: &CacheKey) -> Option<ContextPlan> {
        let documents = ProjectDocuments::existing(&self.root).ok()??;
        let name = key.document_key();
        let bytes = documents.get(DOCUMENT_KIND, &name).ok()??;
        let discard = || {
            let _ = documents.remove(DOCUMENT_KIND, &name);
        };
        let entry: CacheEntry = match serde_json::from_slice(&bytes) {
            Ok(entry) => entry,
            Err(_) => {
                discard();
                return None;
            }
        };
        if entry.schema_version != SCHEMA_VERSION
            || entry.engine_version != ENGINE_VERSION
            || entry.key != *key
        {
            discard();
            return None;
        }
        match validate(root, &entry.dependencies) {
            Ok(report) if report.is_fresh() => {}
            _ => {
                discard();
                return None;
            }
        }
        // Trust the *content*, not the stored claim: each slice must still match
        // the current non-sensitive source lines, and no selected path may be
        // sensitive. A poisoned entry is discarded.
        if !trusted(root, &entry.plan) {
            discard();
            return None;
        }
        let mut plan = entry.plan;
        plan.provenance.stale = false;
        // Only now, after git identity + dependencies + slices were checked.
        plan.provenance.validated = true;
        Some(plan)
    }

    /// Store a plan, unless it contains sensitive content. The oldest entries
    /// beyond [`MAX_ENTRIES`] are pruned.
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
        let entry = CacheEntry {
            schema_version: SCHEMA_VERSION,
            engine_version: ENGINE_VERSION.to_string(),
            created_at: now,
            key: key.clone(),
            dependencies: dependencies.to_vec(),
            plan: plan.clone(),
        };
        let bytes = serde_json::to_vec(&entry).map_err(|error| {
            OcgError::config(format!("cannot serialize the context cache entry: {error}"))
        })?;
        let documents = ProjectDocuments::for_root(&self.root)?;
        documents.put(DOCUMENT_KIND, &key.document_key(), &bytes)?;
        let mut stored = documents.sizes(DOCUMENT_KIND)?;
        if stored.len() > MAX_ENTRIES {
            stored.sort_by_key(|(_, _, updated_at)| *updated_at);
            for (name, _, _) in stored.iter().take(stored.len() - MAX_ENTRIES) {
                documents.remove(DOCUMENT_KIND, name)?;
            }
        }
        Ok(true)
    }

    /// Remove every cache entry. The index lives elsewhere and is never
    /// touched.
    pub fn clean(&self) -> Result<CleanReport> {
        let stats = self.stats();
        if let Some(documents) = ProjectDocuments::existing(&self.root)? {
            documents.clear(DOCUMENT_KIND)?;
        }
        Ok(CleanReport {
            dir: stats.dir,
            removed_entries: stats.entries + stats.corrupt,
            removed_bytes: stats.bytes,
        })
    }

    /// Count and measure entries. Corrupt entries are counted, not trusted.
    pub fn stats(&self) -> CacheStats {
        let mut stats = CacheStats {
            dir: self.dir(),
            entries: 0,
            bytes: 0,
            corrupt: 0,
            oldest: None,
            newest: None,
        };
        let Ok(Some(documents)) = ProjectDocuments::existing(&self.root) else {
            return stats;
        };
        let Ok(entries) = documents.list(DOCUMENT_KIND) else {
            return stats;
        };
        for entry in entries {
            stats.bytes = stats.bytes.saturating_add(entry.value.len() as u64);
            match serde_json::from_slice::<CacheEntry>(&entry.value) {
                Ok(parsed)
                    if parsed.schema_version == SCHEMA_VERSION
                        && parsed.engine_version == ENGINE_VERSION =>
                {
                    stats.entries += 1;
                    stats.oldest = Some(
                        stats
                            .oldest
                            .map_or(parsed.created_at, |oldest| oldest.min(parsed.created_at)),
                    );
                    stats.newest = Some(
                        stats
                            .newest
                            .map_or(parsed.created_at, |newest| newest.max(parsed.created_at)),
                    );
                }
                _ => stats.corrupt += 1,
            }
        }
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
        let Ok(content) = std::fs::read_to_string(root.join(&slice.path)) else {
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
