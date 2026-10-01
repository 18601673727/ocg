//! Versioned, inspectable phase checkpoints.
//!
//! A checkpoint records the structured hand-off between orchestration phases:
//! the task capsule, a Git state fingerprint, verification state, provenance,
//! decisions and a creation time. It is stored as JSON under
//! `.ocg/checkpoints/` and loaded with an explicit schema **and
//! freshness** check: a stale checkpoint is marked stale and can be ignored,
//! and a corrupt checkpoint is reported, never allowed to block normal `ocg`.

use crate::context::capsule::{Decision, TaskCapsule};
use crate::context::freshness::{validate, Provenance, ENGINE_VERSION};
use crate::context::gitdiff::{snapshot_fingerprint, GitSnapshot, GitState};
use crate::error::{OcgError, Result};
use crate::process::GitHost;
use crate::verification::result::VerificationReport;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

/// The checkpoint schema version.
pub const CHECKPOINT_SCHEMA_VERSION: u32 = 1;
/// The checkpoint directory under `.ocg/`.
pub const CHECKPOINTS_DIR: &str = "checkpoints";

/// The canonical phase sequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    ExploreToBuild,
    BuildToVerify,
    VerifyToDebug,
    DebugToBuild,
    Decision,
}

impl Phase {
    /// Every phase, in order.
    pub fn all() -> [Phase; 5] {
        [
            Phase::ExploreToBuild,
            Phase::BuildToVerify,
            Phase::VerifyToDebug,
            Phase::DebugToBuild,
            Phase::Decision,
        ]
    }

    /// A stable index for ordering comparisons.
    pub fn order(self) -> u8 {
        match self {
            Phase::ExploreToBuild => 0,
            Phase::BuildToVerify => 1,
            Phase::VerifyToDebug => 2,
            Phase::DebugToBuild => 3,
            Phase::Decision => 4,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Phase::ExploreToBuild => "explore_to_build",
            Phase::BuildToVerify => "build_to_verify",
            Phase::VerifyToDebug => "verify_to_debug",
            Phase::DebugToBuild => "debug_to_build",
            Phase::Decision => "decision",
        }
    }

    /// Parse the stable name, also accepting hyphenated CLI spelling.
    pub fn parse(text: &str) -> Option<Phase> {
        let normalized = text.trim().to_ascii_lowercase().replace('-', "_");
        match normalized.as_str() {
            "explore_to_build" | "explore" | "build" => Some(Phase::ExploreToBuild),
            "build_to_verify" | "verify" => Some(Phase::BuildToVerify),
            "verify_to_debug" | "debug" => Some(Phase::VerifyToDebug),
            "debug_to_build" => Some(Phase::DebugToBuild),
            "decision" => Some(Phase::Decision),
            _ => None,
        }
    }

    /// Whether `self` comes before `other` in the canonical sequence.
    pub fn precedes(self, other: Phase) -> bool {
        self.order() < other.order()
    }
}

/// The stored checkpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checkpoint {
    pub schema_version: u32,
    pub engine_version: String,
    pub id: String,
    pub phase: Phase,
    pub capsule: TaskCapsule,
    #[serde(default)]
    pub git: GitState,
    pub git_fingerprint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verification: Option<VerificationReport>,
    #[serde(default)]
    pub provenance: Provenance,
    #[serde(default)]
    pub decisions: Vec<Decision>,
    pub created_at: i64,
    #[serde(default)]
    pub notes: Vec<String>,
}

impl Checkpoint {
    /// Build a checkpoint and derive its safe, deterministic id.
    #[allow(clippy::too_many_arguments)]
    pub fn build(
        phase: Phase,
        capsule: TaskCapsule,
        git: GitState,
        git_fingerprint: String,
        verification: Option<VerificationReport>,
        provenance: Provenance,
        decisions: Vec<Decision>,
        created_at: i64,
    ) -> Self {
        let id = checkpoint_id(phase, &capsule.task, &git_fingerprint, created_at);
        Self {
            schema_version: CHECKPOINT_SCHEMA_VERSION,
            engine_version: ENGINE_VERSION.to_string(),
            id,
            phase,
            capsule,
            git,
            git_fingerprint,
            verification,
            provenance,
            decisions,
            created_at,
            notes: Vec::new(),
        }
    }

    /// Persist atomically under `.ocg/checkpoints/`.
    pub fn save(&self, root: &Path) -> Result<PathBuf> {
        crate::install::ensure_gitignore(root)?;
        let path = checkpoint_path(root, &self.id)?;
        let value = serde_json::to_value(self)
            .map_err(|error| OcgError::config(format!("cannot serialize checkpoint: {error}")))?;
        crate::install::write_json_atomic(&path, &value)?;
        Ok(path)
    }

    /// Whether the checkpoint's sources or Git identity changed.
    pub fn staleness(&self, root: &Path, git: &dyn GitHost) -> Staleness {
        let mut reasons = Vec::new();
        if self.schema_version != CHECKPOINT_SCHEMA_VERSION {
            reasons.push(format!(
                "schema_version {} is not supported (expected {CHECKPOINT_SCHEMA_VERSION})",
                self.schema_version
            ));
        }
        if self.engine_version != ENGINE_VERSION {
            reasons.push(format!(
                "checkpoint was created by engine {} (running {ENGINE_VERSION})",
                self.engine_version
            ));
        }
        match validate(root, &self.provenance.sources) {
            Ok(report) => {
                for path in report.stale {
                    reasons.push(format!("source changed: {path}"));
                }
                for path in report.missing {
                    reasons.push(format!("source missing: {path}"));
                }
            }
            Err(error) => reasons.push(format!("sources could not be revalidated: {error}")),
        }
        let snapshot = GitSnapshot::collect(root, git);
        let fingerprint = snapshot_fingerprint(&snapshot);
        if fingerprint != self.git_fingerprint {
            reasons.push("git state changed since the checkpoint".to_string());
        }
        Staleness {
            stale: !reasons.is_empty(),
            reasons,
        }
    }
}

/// The result of a freshness check.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Staleness {
    pub stale: bool,
    #[serde(default)]
    pub reasons: Vec<String>,
}

/// A short summary used by `ocg checkpoint list`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointSummary {
    pub id: String,
    pub phase: Phase,
    pub task: String,
    pub created_at: i64,
    pub file: String,
}

/// A loaded checkpoint plus its staleness verdict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedCheckpoint {
    pub checkpoint: Checkpoint,
    pub staleness: Staleness,
}

/// A safe, deterministic id. Only `[a-z0-9-]`, so it can never traverse paths.
pub fn checkpoint_id(phase: Phase, task: &str, git_fingerprint: &str, created_at: i64) -> String {
    let digest = crate::hash::sha256_hex(
        format!(
            "{}|{}|{}|{}",
            phase.as_str(),
            task,
            git_fingerprint,
            created_at
        )
        .as_bytes(),
    );
    let short = digest.get(..16).unwrap_or(&digest);
    let phase = phase.as_str().replace('_', "-");
    format!("cp-{phase}-{short}")
}

/// Whether an id is safe to use as a filename.
pub fn is_safe_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-')
}

/// The directory that stores checkpoints.
pub fn checkpoints_dir(root: &Path) -> PathBuf {
    root.join(crate::context::repomap::OCG_DIR)
        .join(CHECKPOINTS_DIR)
}

/// The path for one checkpoint id.
pub fn checkpoint_path(root: &Path, id: &str) -> Result<PathBuf> {
    if !is_safe_id(id) {
        return Err(OcgError::config(format!(
            "unsafe checkpoint id '{id}' (expected lowercase letters, digits and '-')"
        )));
    }
    Ok(checkpoints_dir(root).join(format!("{id}.json")))
}

/// Load one checkpoint by id with an explicit schema and freshness check.
pub fn load(root: &Path, id: &str, git: &dyn GitHost) -> Result<LoadedCheckpoint> {
    let path = checkpoint_path(root, id)?;
    let text = fs::read_to_string(&path).map_err(|error| OcgError::read(&path, error))?;
    let checkpoint: Checkpoint = serde_json::from_str(&text)
        .map_err(|error| OcgError::config(format!("checkpoint {id} is not valid JSON: {error}")))?;
    if checkpoint.schema_version != CHECKPOINT_SCHEMA_VERSION {
        return Err(OcgError::config(format!(
            "checkpoint {id} has schema_version {} (expected {CHECKPOINT_SCHEMA_VERSION})",
            checkpoint.schema_version
        )));
    }
    let staleness = checkpoint.staleness(root, git);
    Ok(LoadedCheckpoint {
        checkpoint,
        staleness,
    })
}

/// List every readable checkpoint, newest first. Corrupt files are counted,
/// never returned, and never abort the listing.
pub fn list(root: &Path) -> (Vec<CheckpointSummary>, usize) {
    let dir = checkpoints_dir(root);
    let Ok(entries) = fs::read_dir(&dir) else {
        return (Vec::new(), 0);
    };
    let mut summaries = Vec::new();
    let mut corrupt = 0usize;
    for entry in entries.flatten() {
        let path = entry.path();
        if path
            .extension()
            .map(|extension| extension != "json")
            .unwrap_or(true)
        {
            continue;
        }
        let Ok(text) = fs::read_to_string(&path) else {
            corrupt += 1;
            continue;
        };
        match serde_json::from_str::<Checkpoint>(&text) {
            Ok(checkpoint) if checkpoint.schema_version == CHECKPOINT_SCHEMA_VERSION => {
                summaries.push(CheckpointSummary {
                    id: checkpoint.id,
                    phase: checkpoint.phase,
                    task: checkpoint.capsule.task,
                    created_at: checkpoint.created_at,
                    file: path
                        .file_name()
                        .map(|name| name.to_string_lossy().into_owned())
                        .unwrap_or_default(),
                });
            }
            _ => corrupt += 1,
        }
    }
    summaries.sort_by(|a, b| {
        b.created_at
            .cmp(&a.created_at)
            .then_with(|| a.id.cmp(&b.id))
    });
    (summaries, corrupt)
}
