//! Local orchestration state.
//!
//! State lives under `<project>/.ocg/orchestration/state.json`. It is
//! intentionally small, inspectable and **fail-soft**:
//!
//! - a corrupt or unsupported state file is reported and replaced with a fresh
//!   empty state, never an error;
//! - ids are hashed to safe `[a-z0-9-]` names before they become keys or paths,
//!   so a hostile session id cannot traverse the filesystem;
//! - a bounded number of sessions is retained (newest first) so the file cannot
//!   grow without limit.
//!
//! This module never contacts a model or a network. It stores the
//! controller's phase, retry budgets and the latest bounded findings so a later
//! bridge call can resume the same task.

use crate::orchestration::handoff::{
    HandoffFinding, HandoffVerification, ModelHandoffCapsule, Role, Transition,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

/// The state directory under `.ocg/`.
pub const ORCHESTRATION_DIR: &str = "orchestration";
/// The state file name.
pub const STATE_FILE: &str = "state.json";
/// The state schema version.
pub const STATE_SCHEMA_VERSION: u32 = 1;
/// The maximum number of sessions retained.
pub const MAX_SESSIONS: usize = 16;
/// The maximum number of bounded findings retained per session.
pub const MAX_SESSION_FINDINGS: usize = 32;
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

/// The controller phase. `Idle` is a fresh session; `Done` means the task
/// completed cleanly; `Debug` means a Debug hand-off is recommended or active.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrchestrationPhase {
    #[default]
    Idle,
    Explore,
    Build,
    Verify,
    Debug,
    Done,
}

impl OrchestrationPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            OrchestrationPhase::Idle => "idle",
            OrchestrationPhase::Explore => "explore",
            OrchestrationPhase::Build => "build",
            OrchestrationPhase::Verify => "verify",
            OrchestrationPhase::Debug => "debug",
            OrchestrationPhase::Done => "done",
        }
    }
}

/// The retry budget actually consumed by one session.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Attempts {
    pub build: usize,
    /// Verification runs that were executed after Build. Retained for metrics
    /// and post-hoc analysis; the retry *policy* is driven by `build` and
    /// `debug`.
    pub verify: usize,
    pub debug: usize,
}

/// The retained session repository baseline for the native control plane
/// model-dispatch path.
///
/// The execution adapter injects the baseline into the outgoing model request's
/// system context, which is never persisted, so every root-Lead dispatch must
/// receive the full body again. The baseline *body* is retained here, keyed by
/// `repository_generation_id`: an unchanged generation reuses the stored body
/// verbatim (baseline computation is task-independent), while a material
/// repository generation change re-renders and replaces it. The V1
/// persisted-prompt path never reads this field.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct RepositoryBaseline {
    /// The rendered baseline body exactly as supplied to the model.
    pub body: String,
    /// Number of relevant files in the prepared input when rendered.
    pub file_count: usize,
    /// Number of relevant symbols in the prepared input when rendered.
    pub symbol_count: usize,
}

/// One task session's bounded state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct SessionState {
    pub session_id: String,
    pub task_id: String,
    pub phase: OrchestrationPhase,
    pub source: Role,
    pub destination: Option<Role>,
    pub last_transition: Option<Transition>,
    pub task: Option<String>,
    pub goal: Option<String>,
    pub constraints: Vec<String>,
    pub findings: Vec<HandoffFinding>,
    pub files: Vec<String>,
    pub symbols: Vec<String>,
    pub failures: Vec<String>,
    pub evidence: Vec<String>,
    pub debug_reason: Option<String>,
    pub last_verification: Option<HandoffVerification>,
    /// The most recent structured verification report. Kept so a later
    /// Debug→Build checkpoint can record the actual verification result.
    pub last_report: Option<crate::verification::result::VerificationReport>,
    pub checkpoints: Vec<String>,
    pub attempts: Attempts,
    /// The byte size of the most recent full rich task context. Used as the
    /// ratio reference for deliberately narrow hand-offs such as Debug.
    pub last_rich_bytes: usize,
    /// The most recent bounded, real diff rendering. Reused by Debug hand-offs
    /// so they carry a relevant diff without re-planning.
    pub last_diff_context: String,
    /// The identity of the session repository baseline. It is derived from the
    /// indexed repository content (repo root, engine/schema version and file
    /// fingerprints), not from the current task or ranked projection. A fresh
    /// Lead session starts with `None`; after the first baseline injection it
    /// holds the generation that was injected. A later turn only appends another
    /// full repository snapshot when this generation differs, which happens only
    /// when the indexed repository content materially changes.
    #[serde(default)]
    pub repository_generation_id: Option<String>,
    /// The retained baseline body for `repository_generation_id`, used by the
    /// model-dispatch path so an unchanged generation is reused without
    /// re-rendering while still being supplied to every outgoing request.
    #[serde(default)]
    pub repository_baseline: Option<RepositoryBaseline>,
    pub updated_at: i64,
}

impl Default for SessionState {
    fn default() -> Self {
        Self {
            session_id: String::new(),
            task_id: String::new(),
            phase: OrchestrationPhase::Idle,
            source: Role::Lead,
            destination: None,
            last_transition: None,
            task: None,
            goal: None,
            constraints: Vec::new(),
            findings: Vec::new(),
            files: Vec::new(),
            symbols: Vec::new(),
            failures: Vec::new(),
            evidence: Vec::new(),
            debug_reason: None,
            last_verification: None,
            last_report: None,
            checkpoints: Vec::new(),
            attempts: Attempts::default(),
            last_rich_bytes: 0,
            last_diff_context: String::new(),
            repository_generation_id: None,
            repository_baseline: None,
            updated_at: 0,
        }
    }
}

impl SessionState {
    pub fn new(session_id: &str, task_id: &str, now: i64) -> Self {
        Self {
            session_id: session_id.to_string(),
            task_id: task_id.to_string(),
            updated_at: now,
            ..Self::default()
        }
    }

    /// Add a bounded finding, dropping the oldest beyond the cap.
    pub fn push_finding(&mut self, finding: HandoffFinding) {
        if let Some(existing) = self
            .findings
            .iter_mut()
            .find(|existing| existing.summary == finding.summary)
        {
            if finding.severity > existing.severity {
                existing.severity = finding.severity;
            }
            return;
        }
        self.findings.push(finding);
        if self.findings.len() > MAX_SESSION_FINDINGS {
            let excess = self.findings.len() - MAX_SESSION_FINDINGS;
            self.findings.drain(0..excess);
        }
    }

    /// Record a checkpoint id, deduplicated and bounded.
    pub fn push_checkpoint(&mut self, id: &str) {
        if !self.checkpoints.iter().any(|existing| existing == id) {
            self.checkpoints.push(id.to_string());
        }
        if self.checkpoints.len() > 32 {
            let excess = self.checkpoints.len() - 32;
            self.checkpoints.drain(0..excess);
        }
    }
}

/// The whole state document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct OrchestrationState {
    pub schema_version: u32,
    pub updated_at: i64,
    pub sessions: BTreeMap<String, SessionState>,
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
    /// Durable compaction points, per Attempt.
    ///
    /// A point records that a summary stands in for the conversation before a
    /// transcript position. It lives here, in the existing orchestration state,
    /// rather than in a new store: it is canonical execution state about an
    /// Attempt, exactly like the Attempts and Calls beside it.
    ///
    /// Keyed by Attempt because positions are relative to one Attempt's
    /// transcript. A point from another Attempt would refer to different
    /// messages and is never a valid projection of this one.
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

impl Default for OrchestrationState {
    fn default() -> Self {
        Self {
            schema_version: STATE_SCHEMA_VERSION,
            updated_at: 0,
            sessions: BTreeMap::new(),
            compiler_baselines: BTreeMap::new(),
            compaction_points: BTreeMap::new(),
        }
    }
}

impl OrchestrationState {
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
    pub fn latest_compiler_baseline(&self) -> Option<(&str, &crate::compiler_feedback::DiagnosticDelta)> {
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

    /// The durable points for an Attempt, oldest first.
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

    pub fn session(&self, session_id: &str) -> Option<&SessionState> {
        self.sessions.get(&safe_id(session_id))
    }

    /// Insert or replace a session and enforce the retention bound. Newest
    /// `updated_at` wins; ties fall back to the key so ordering is stable.
    pub fn upsert(&mut self, mut session: SessionState, now: i64) {
        let key = safe_id(&session.session_id);
        session.session_id = key.clone();
        session.updated_at = now;
        self.sessions.insert(key, session);
        self.updated_at = now;
        if self.sessions.len() > MAX_SESSIONS {
            let mut entries: Vec<(String, i64)> = self
                .sessions
                .iter()
                .map(|(key, session)| (key.clone(), session.updated_at))
                .collect();
            entries.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| b.0.cmp(&a.0)));
            for (key, _) in entries.into_iter().skip(MAX_SESSIONS) {
                self.sessions.remove(&key);
            }
        }
    }
}

/// A safe, deterministic id. Unsafe ids are hashed so raw text never becomes a
/// key or a path component.
pub fn safe_id(seed: &str) -> String {
    let trimmed = seed.trim();
    if !trimmed.is_empty()
        && trimmed.len() <= 96
        && trimmed
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
    {
        return trimmed.to_string();
    }
    let digest = crate::hash::sha256_hex(seed.as_bytes());
    format!("s-{}", digest.get(..16).unwrap_or(&digest))
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
    pub state: OrchestrationState,
    pub corrupt: bool,
    pub exists: bool,
}

/// Load state, recovering to an empty state on a missing, corrupt or
/// unsupported file. Never creates state and never fails.
pub fn load(root: &Path) -> LoadedState {
    let path = state_path(root);
    if !path.is_file() {
        return LoadedState {
            state: OrchestrationState::default(),
            corrupt: false,
            exists: false,
        };
    }
    match fs::read_to_string(&path) {
        Ok(text) => match serde_json::from_str::<OrchestrationState>(&text) {
            Ok(state) if state.schema_version == STATE_SCHEMA_VERSION => LoadedState {
                state,
                corrupt: false,
                exists: true,
            },
            _ => LoadedState {
                state: OrchestrationState::default(),
                corrupt: true,
                exists: true,
            },
        },
        Err(_) => LoadedState {
            state: OrchestrationState::default(),
            corrupt: true,
            exists: true,
        },
    }
}

/// Persist state atomically. Ensures the state tree stays git-ignored.
pub fn save(root: &Path, state: &OrchestrationState) -> crate::error::Result<PathBuf> {
    crate::install::ensure_gitignore(root)?;
    let path = state_path(root);
    let value = serde_json::to_value(state).map_err(|error| {
        crate::error::OcgError::config(format!("cannot serialize orchestration state: {error}"))
    })?;
    crate::install::write_json_atomic(&path, &value)?;
    Ok(path)
}

/// Whether a hand-off capsule's session is still present and fresh. A missing
/// session is *not* stale; it simply has no prior state.
pub fn matches_session(state: &OrchestrationState, capsule: &ModelHandoffCapsule) -> bool {
    state
        .session(&capsule.session_id)
        .map(|session| session.task_id == capsule.task_id)
        .unwrap_or(false)
}
