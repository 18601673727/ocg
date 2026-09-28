//! Versioned, serializable task capsules.
//!
//! A capsule is the structured hand-off between an exploration step and a
//! later session: findings, files, symbols, decisions, verification
//! placeholders and provenance. It is pure data (serde JSON), has an explicit
//! `schema_version`, and its size accounting is labelled as an estimate.

use crate::context::freshness::{validate, Provenance, ENGINE_VERSION, SCHEMA_VERSION};
use crate::context::gitdiff::GitState;
use crate::context::ranking::estimated_tokens;
use crate::context::symbols::SymbolRef;
use crate::error::{OcgError, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;

/// One finding. Evidence stays explicit; no chain-of-thought is stored.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Finding {
    pub summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
}

/// A file touched or inspected by the task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapsuleFile {
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default)]
    pub changed: bool,
}

/// A decision with no invented dates: `date` is only set when it is known.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Decision {
    pub decision: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rationale: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub date: Option<String>,
    #[serde(default)]
    pub date_unknown: bool,
}

/// A verification step that has not necessarily run yet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Verification {
    pub check: String,
    /// One of pending / not_run / passed / failed / unknown. Never inferred.
    pub status: String,
}

impl Verification {
    pub fn pending(check: impl Into<String>) -> Self {
        Self {
            check: check.into(),
            status: "pending".to_string(),
        }
    }
}

/// Byte and token accounting for a capsule.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct SizeAccounting {
    pub bytes: usize,
    /// Estimated only (bytes / 4), never exact.
    pub estimated_tokens: usize,
    pub files: usize,
    pub symbols: usize,
}

/// The capsule itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskCapsule {
    pub schema_version: u32,
    pub task: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub goal: Option<String>,
    #[serde(default)]
    pub constraints: Vec<String>,
    #[serde(default)]
    pub findings: Vec<Finding>,
    #[serde(default)]
    pub files: Vec<CapsuleFile>,
    #[serde(default)]
    pub symbols: Vec<SymbolRef>,
    #[serde(default)]
    pub decisions: Vec<Decision>,
    #[serde(default)]
    pub git: GitState,
    #[serde(default)]
    pub verification: Vec<Verification>,
    #[serde(default)]
    pub failures: Vec<String>,
    #[serde(default)]
    pub questions: Vec<String>,
    #[serde(default)]
    pub provenance: Provenance,
    #[serde(default)]
    pub size: SizeAccounting,
}

impl TaskCapsule {
    /// A new, empty capsule for a task.
    pub fn new(task: impl Into<String>) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            task: task.into(),
            goal: None,
            constraints: Vec::new(),
            findings: Vec::new(),
            files: Vec::new(),
            symbols: Vec::new(),
            decisions: Vec::new(),
            git: GitState::default(),
            verification: Vec::new(),
            failures: Vec::new(),
            questions: Vec::new(),
            provenance: Provenance {
                engine_version: ENGINE_VERSION.to_string(),
                schema_version: SCHEMA_VERSION,
                ..Provenance::default()
            },
            size: SizeAccounting::default(),
        }
    }

    /// Recompute the size accounting. The token count is explicitly an estimate.
    pub fn recompute_size(&mut self) {
        let mut probe = self.clone();
        probe.size = SizeAccounting::default();
        let bytes = serde_json::to_string(&probe)
            .map(|text| text.len())
            .unwrap_or(0);
        self.size = SizeAccounting {
            bytes,
            estimated_tokens: estimated_tokens(bytes),
            files: self.files.len(),
            symbols: self.symbols.len(),
        };
    }

    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string(self)
            .map_err(|error| OcgError::config(format!("cannot serialize capsule: {error}")))
    }

    pub fn to_pretty_json(&self) -> Result<String> {
        serde_json::to_string_pretty(self)
            .map_err(|error| OcgError::config(format!("cannot serialize capsule: {error}")))
    }

    pub fn from_json(text: &str) -> Result<Self> {
        let capsule: Self = serde_json::from_str(text)
            .map_err(|error| OcgError::config(format!("capsule is not valid JSON: {error}")))?;
        if capsule.schema_version != SCHEMA_VERSION {
            return Err(OcgError::config(format!(
                "capsule schema_version {} is not supported (expected {SCHEMA_VERSION})",
                capsule.schema_version
            )));
        }
        Ok(capsule)
    }

    /// Whether any source the capsule was built from changed.
    pub fn is_stale(&self, root: &Path) -> Result<bool> {
        let report = validate(root, &self.provenance.sources)?;
        Ok(!report.is_fresh())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::freshness::SourceFingerprint;

    #[test]
    fn round_trips_through_json() {
        let mut capsule = TaskCapsule::new("add a cache clean command");
        capsule.goal = Some("make cache safe to clear".to_string());
        capsule.constraints = vec!["never delete runtime".to_string()];
        capsule.findings.push(Finding {
            summary: "cache lives under .ocg/cache".to_string(),
            detail: None,
            source: Some("src/context/cache.rs".to_string()),
        });
        capsule.files.push(CapsuleFile {
            path: "src/context/cache.rs".to_string(),
            reason: Some("implementation".to_string()),
            changed: true,
        });
        capsule.decisions.push(Decision {
            decision: "clean cache only".to_string(),
            rationale: Some("runtime is expensive to reinstall".to_string()),
            date: None,
            date_unknown: true,
        });
        capsule
            .verification
            .push(Verification::pending("cargo test"));
        capsule.recompute_size();
        assert!(capsule.size.bytes > 0);
        assert_eq!(
            capsule.size.estimated_tokens,
            capsule.size.bytes.div_ceil(4)
        );

        let text = capsule.to_json().unwrap();
        let parsed = TaskCapsule::from_json(&text).unwrap();
        assert_eq!(capsule, parsed);
    }

    #[test]
    fn rejects_unknown_schema_version() {
        let text = r#"{"schema_version":99,"task":"x"}"#;
        assert!(TaskCapsule::from_json(text).is_err());
    }

    #[test]
    fn detects_stale_sources() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.rs");
        std::fs::write(&path, "fn a() {}\n").unwrap();
        let bytes = std::fs::read(&path).unwrap();
        let mut capsule = TaskCapsule::new("task");
        capsule.provenance.sources.push(SourceFingerprint {
            path: "a.rs".to_string(),
            fingerprint: format!("sha256:{}", crate::runtime::hash::sha256_hex(&bytes)),
            size: bytes.len() as u64,
        });
        assert!(!capsule.is_stale(dir.path()).unwrap());
        std::fs::write(&path, "fn b() {}\n").unwrap();
        assert!(capsule.is_stale(dir.path()).unwrap());
    }
}
