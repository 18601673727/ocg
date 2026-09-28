//! Provenance and freshness for indexed and planned artifacts.
//!
//! Every artifact the engine reuses carries the fingerprints it was built from.
//! Before reuse, those fingerprints are revalidated against the working tree.
//! A stale artifact is never trusted: the caller recomputes it.

use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

/// The engine version stamped into indexes, plans and capsules.
pub const ENGINE_VERSION: &str = crate::cli::VERSION;
/// The plan/index/cache schema version. Bumping it invalidates old artifacts.
pub const SCHEMA_VERSION: u32 = 2;

/// A path plus the fingerprint it had when an artifact was built.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceFingerprint {
    pub path: String,
    pub fingerprint: String,
    pub size: u64,
}

/// Where a plan or capsule came from, and whether it is still fresh.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Provenance {
    pub engine_version: String,
    pub schema_version: u32,
    pub repo_id: String,
    pub git_head: Option<String>,
    pub git_dirty: bool,
    pub generated_at: i64,
    pub sources: Vec<SourceFingerprint>,
    /// Set when the artifact was served from cache but its sources changed.
    pub stale: bool,
    /// True only after the sources and git identity were checked against the
    /// working tree. A provenance value is never labelled fresh without this.
    #[serde(default)]
    pub validated: bool,
    pub notes: Vec<String>,
}

/// The result of revalidating fingerprints.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct FreshnessReport {
    pub checked: usize,
    /// Paths whose fingerprint no longer matches the working tree.
    pub stale: Vec<String>,
    /// Paths that disappeared.
    pub missing: Vec<String>,
}

impl FreshnessReport {
    pub fn is_fresh(&self) -> bool {
        self.stale.is_empty() && self.missing.is_empty()
    }
}

/// Revalidate a set of fingerprints against the working tree.
///
/// Sensitive paths are not read: their content is never fingerprinted, so they
/// only need to still exist.
pub fn validate(root: &Path, sources: &[SourceFingerprint]) -> Result<FreshnessReport> {
    let mut report = FreshnessReport::default();
    for source in sources {
        report.checked += 1;
        let path = root.join(&source.path);
        let metadata = match fs::metadata(&path) {
            Ok(metadata) => metadata,
            Err(_) => {
                report.missing.push(source.path.clone());
                continue;
            }
        };
        if crate::context::classify::classify(Path::new(&source.path)).sensitive {
            continue;
        }
        let bytes = fs::read(&path).map_err(|error| crate::error::OcgError::read(&path, error))?;
        let fingerprint = format!("sha256:{}", crate::runtime::hash::sha256_hex(&bytes));
        if fingerprint != source.fingerprint || metadata.len() != source.size {
            report.stale.push(source.path.clone());
        }
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn detects_changed_missing_and_fresh_sources() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.rs");
        fs::write(&path, "fn a() {}\n").unwrap();
        let bytes = fs::read(&path).unwrap();
        let fingerprint = SourceFingerprint {
            path: "a.rs".to_string(),
            fingerprint: format!("sha256:{}", crate::runtime::hash::sha256_hex(&bytes)),
            size: bytes.len() as u64,
        };
        assert!(validate(dir.path(), std::slice::from_ref(&fingerprint))
            .unwrap()
            .is_fresh());

        fs::write(&path, "fn b() {}\n").unwrap();
        let stale = validate(dir.path(), std::slice::from_ref(&fingerprint)).unwrap();
        assert_eq!(stale.stale, vec!["a.rs".to_string()]);

        fs::remove_file(&path).unwrap();
        let missing = validate(dir.path(), std::slice::from_ref(&fingerprint)).unwrap();
        assert_eq!(missing.missing, vec!["a.rs".to_string()]);
    }

    #[test]
    fn sensitive_sources_are_not_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".env");
        fs::write(&path, "PASSWORD=placeholder\n").unwrap();
        let source = SourceFingerprint {
            path: ".env".to_string(),
            fingerprint: String::new(),
            size: 21,
        };
        assert!(validate(dir.path(), &[source]).unwrap().is_fresh());
    }
}
