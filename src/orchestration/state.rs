//! Non-authoritative provider context caches at `.ocg/orchestration/state.json`.
//! Compiler baselines and compaction summaries may be dropped and recomputed.
//! They never decide Job/Attempt/Call lifecycle, scheduling, fencing or budgets;
//! those decisions belong exclusively to the canonical SQLite substrate.
//! Legacy session/controller fields are ignored on load and omitted on save.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

/// The storage directory shared by the canonical SQLite substrate and local caches.
pub const ORCHESTRATION_DIR: &str = "orchestration";
pub const STATE_FILE: &str = "state.json";
pub const STATE_SCHEMA_VERSION: u32 = 1;
/// The maximum number of compiler diagnostic baselines retained per project.
///
/// One entry per distinct verification command. Compiler diagnostics are a
/// property of the checked tree, not of a task session, so the baseline is
/// project-scoped: the next compile of the same command compares against the
/// previous one regardless of which session ran it.
pub const MAX_COMPILER_BASELINES: usize = 8;
/// The maximum number of compaction points retained per Attempt.
///
/// A later point folds earlier ones forward, so only the most recent few can
/// still project a transcript. Keeping more would store summaries nothing can
/// apply.
pub const MAX_COMPACTION_POINTS: usize = 4;
/// The maximum number of Attempts that retain compaction points.
pub const MAX_COMPACTION_ATTEMPTS: usize = 8;

// Old files remain readable: serde ignores the removed session/controller fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ContextCache {
    pub schema_version: u32,
    /// The last machine-readable diagnostic set seen per verification command.
    ///
    /// This is the baseline a later compile is compared against, so that only
    /// the delta reaches a reader. It is replaced on every run and never grows:
    /// each entry is the current compile's diagnostics, not a history.
    ///
    /// The stored delta carries the current set in `current`, so the set is not
    /// also kept separately: one compile, one record.
    #[serde(default)]
    pub compiler_baselines: BTreeMap<String, CompilerBaseline>,
    #[serde(default)]
    pub compaction_points: BTreeMap<String, Vec<crate::compaction::CompactionPoint>>,
}

/// One verification command's most recent machine-readable compile.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct CompilerBaseline {
    /// The delta the run reported. `current` is the baseline a later compile is
    /// compared against.
    pub diagnostics: crate::compiler_feedback::DiagnosticDelta,
    /// When the compile ran, used to identify the most recent one.
    pub recorded_at: i64,
}

impl Default for CompilerBaseline {
    fn default() -> Self {
        Self {
            diagnostics: crate::compiler_feedback::DiagnosticDelta {
                current: Vec::new(),
                new: Vec::new(),
                remaining: Vec::new(),
                resolved: Vec::new(),
                baseline_known: false,
            },
            recorded_at: 0,
        }
    }
}

impl Default for ContextCache {
    fn default() -> Self {
        Self {
            schema_version: STATE_SCHEMA_VERSION,
            compiler_baselines: BTreeMap::new(),
            compaction_points: BTreeMap::new(),
        }
    }
}

impl ContextCache {
    /// Record the current diagnostics for a command, replacing any baseline.
    ///
    /// `live` names the commands this run actually executed. When the retained
    /// set would exceed the cap, entries the run did not touch are dropped first,
    /// so the baseline a following compile compares against is always the most
    /// recent one rather than an arbitrary older one.
    pub fn record_compiler_baseline(
        &mut self,
        command: &str,
        diagnostics: crate::compiler_feedback::DiagnosticDelta,
        recorded_at: i64,
        live: &[String],
    ) {
        self.compiler_baselines.insert(
            command.to_string(),
            CompilerBaseline {
                diagnostics,
                recorded_at,
            },
        );
        if self.compiler_baselines.len() <= MAX_COMPILER_BASELINES {
            return;
        }
        let retained: BTreeMap<String, CompilerBaseline> = self
            .compiler_baselines
            .iter()
            .filter(|(key, _)| live.iter().any(|name| name == *key))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        self.compiler_baselines = retained;
    }

    /// The baseline for a command, if one was recorded.
    pub fn compiler_baseline(
        &self,
        command: &str,
    ) -> Option<&crate::compiler_feedback::DiagnosticDelta> {
        self.compiler_baselines
            .get(command)
            .map(|baseline| &baseline.diagnostics)
    }

    /// The most recent compile of any verification command, with its command.
    ///
    /// A provider request has no verification command of its own, so this is how
    /// the newest compiler feedback reaches the model without inventing a
    /// separate store for it. Ties fall back to the command name so the answer
    /// is stable.
    pub fn latest_compiler_baseline(
        &self,
    ) -> Option<(&str, &crate::compiler_feedback::DiagnosticDelta)> {
        self.compiler_baselines
            .iter()
            .max_by(|left, right| {
                left.1
                    .recorded_at
                    .cmp(&right.1.recorded_at)
                    .then_with(|| left.0.cmp(right.0))
            })
            .map(|(command, baseline)| (command.as_str(), &baseline.diagnostics))
    }

    /// Record an accepted compaction point for an Attempt, oldest first.
    ///
    /// Recording is idempotent on the point id: replanning the same boundary
    /// yields the same id, so a retry replaces rather than duplicates. Only the
    /// newest few points are kept, because a later point folds earlier ones
    /// forward and an old one can no longer be projected on its own.
    pub fn record_compaction_point(
        &mut self,
        attempt_id: &str,
        point: crate::compaction::CompactionPoint,
    ) {
        if attempt_id.is_empty() {
            return;
        }
        let points = self
            .compaction_points
            .entry(attempt_id.to_string())
            .or_default();
        match points.iter().position(|held| held.id == point.id) {
            Some(index) => points[index] = point,
            None => points.push(point),
        }
        points.sort_by(|left, right| {
            left.through
                .cmp(&right.through)
                .then_with(|| left.created_at.cmp(&right.created_at))
        });
        if points.len() > MAX_COMPACTION_POINTS {
            let excess = points.len() - MAX_COMPACTION_POINTS;
            points.drain(..excess);
        }
        self.prune_compaction_attempts();
    }

    /// The cached summaries for an Attempt, oldest first.
    pub fn compaction_points(&self, attempt_id: &str) -> &[crate::compaction::CompactionPoint] {
        self.compaction_points
            .get(attempt_id)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    /// Drop the least recently recorded Attempts, so the map cannot grow without
    /// bound across a long-lived project.
    fn prune_compaction_attempts(&mut self) {
        if self.compaction_points.len() <= MAX_COMPACTION_ATTEMPTS {
            return;
        }
        let mut ordered: Vec<(i64, String)> = self
            .compaction_points
            .iter()
            .map(|(attempt, points)| {
                (
                    points.last().map(|point| point.created_at).unwrap_or(0),
                    attempt.clone(),
                )
            })
            .collect();
        ordered.sort();
        let excess = ordered.len() - MAX_COMPACTION_ATTEMPTS;
        for (_, attempt) in ordered.into_iter().take(excess) {
            self.compaction_points.remove(&attempt);
        }
    }
}

/// The orchestration state directory.
pub fn state_dir(root: &Path) -> PathBuf {
    root.join(crate::context::repomap::OCG_DIR)
        .join(ORCHESTRATION_DIR)
}

/// The state file path.
pub fn state_path(root: &Path) -> PathBuf {
    state_dir(root).join(STATE_FILE)
}

/// The result of a load, including corruption accounting for diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedState {
    pub state: ContextCache,
    pub corrupt: bool,
    pub exists: bool,
}

/// Load state, recovering to an empty state on a missing, corrupt or
/// unsupported file. Never creates state and never fails.
pub fn load(root: &Path) -> LoadedState {
    let path = state_path(root);
    if !path.is_file() {
        return LoadedState {
            state: ContextCache::default(),
            corrupt: false,
            exists: false,
        };
    }
    match fs::read_to_string(&path) {
        Ok(text) => match serde_json::from_str::<ContextCache>(&text) {
            Ok(state) if state.schema_version == STATE_SCHEMA_VERSION => LoadedState {
                state,
                corrupt: false,
                exists: true,
            },
            _ => LoadedState {
                state: ContextCache::default(),
                corrupt: true,
                exists: true,
            },
        },
        Err(_) => LoadedState {
            state: ContextCache::default(),
            corrupt: true,
            exists: true,
        },
    }
}

/// Persist state atomically. Ensures the state tree stays git-ignored.
pub fn save(root: &Path, state: &ContextCache) -> crate::error::Result<PathBuf> {
    crate::install::ensure_gitignore(root)?;
    let path = state_path(root);
    let value = serde_json::to_value(state).map_err(|error| {
        crate::error::OcgError::config(format!("cannot serialize context cache: {error}"))
    })?;
    crate::install::write_json_atomic(&path, &value)?;
    Ok(path)
}
