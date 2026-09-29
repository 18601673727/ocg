//! Durable Mission state: the product-state boundary above sessions.
//!
//! A **session is disposable execution state**. It lives in
//! `.ocg/orchestration/state.json`, keyed by the OpenCode session
//! id, bounded and evicted; it can be dropped, compacted, rolled over or
//! replaced at any time without losing work.
//!
//! A **Mission is durable product state**: the versioned record of one
//! admitted task, keyed by `mission_id` (the deterministic task id derived
//! from the admitted task text — never from a session). It survives the
//! death, failure, compaction, replacement or rollover of any individual
//! OpenCode/model session:
//!
//! - progress is written per mission under
//!   `.ocg/orchestration/missions/<mission_id>.json`, atomically
//!   (temp sibling + rename), alongside — never instead of — the session
//!   view;
//! - the current OpenCode session is recorded as `session_id`, a
//!   **replaceable execution binding**, not part of Mission identity;
//! - a fresh session that admits the same task binds to the same Mission
//!   and is seeded from it, so committed work (findings, attempts,
//!   checkpoints, verification) is never replayed from zero;
//! - `completed`, `failed` and `cancelled` are typed terminal states;
//! - a bounded history of consequential transitions (admission, session
//!   binding, checkpointed hand-offs, terminal states) carries a
//!   deterministic event identity each, so replaying the same transition
//!   identity is a no-op instead of a duplicate effect.
//!
//! Corruption is handled strictly, unlike the disposable session state: a
//! corrupt or unsupported Mission record is **quarantined** (renamed to
//! `<mission_id>.corrupt.json`, preserving the bytes for inspection) and the
//! load fails explicitly. It never silently reads as "no Mission", which
//! would restart committed work from scratch.
//!
//! The controller keeps the session view and the Mission in sync at every
//! mutation point; between them the Mission is authoritative. Single-owner
//! semantics still apply: reconciling two *concurrently live* sessions bound
//! to one Mission is deferred to the reconciliation work that builds on this
//! foundation.

use crate::error::{OcgError, Result};
use crate::orchestration::budget::{self, MissionBudget, MissionBudgetReceipt};
use crate::orchestration::handoff::{HandoffFinding, HandoffVerification, Role, Transition};
use crate::orchestration::state::{Attempts, OrchestrationPhase};
use crate::verification::result::VerificationReport;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

/// The Mission schema version. Bumping it requires explicit migration code;
/// unknown versions are quarantined, never silently adopted.
pub const MISSION_SCHEMA_VERSION: u32 = 1;
/// The Mission directory under `.ocg/orchestration/`.
pub const MISSIONS_DIR: &str = "missions";
/// The maximum number of transition events retained per Mission.
pub const MAX_MISSION_HISTORY: usize = 64;

/// The Mission lifecycle state. Terminal states are unambiguous and durable:
/// once a generation reaches one, the only way forward is a new generation
/// (an explicit re-admission), never a silent reset.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MissionStatus {
    /// Admitted and not yet terminal. Work may be in any phase.
    #[default]
    Active,
    /// Verification passed; the task completed cleanly.
    Completed,
    /// The task was explicitly failed.
    Failed,
    /// The task was explicitly cancelled.
    Cancelled,
}

impl MissionStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            MissionStatus::Active => "active",
            MissionStatus::Completed => "completed",
            MissionStatus::Failed => "failed",
            MissionStatus::Cancelled => "cancelled",
        }
    }

    /// Whether this is a durable terminal state.
    pub fn is_terminal(self) -> bool {
        !matches!(self, MissionStatus::Active)
    }
}

/// The consequential transitions recorded in the Mission history.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MissionEventKind {
    /// The Mission was admitted (generation start).
    Admitted,
    /// The execution binding was set, replaced or released.
    SessionBound,
    /// Explore findings were consumed and checkpointed.
    ExploreToBuild,
    /// A post-Build verification was run and checkpointed.
    BuildToVerify,
    /// The Build budget was exhausted; Debug was recommended.
    VerifyToDebug,
    /// Work returned from Debug to Build.
    DebugToBuild,
    /// A context-pressure rollover was requested for this generation.
    RolloverRequested,
    /// A bounded continuation artifact was durably prepared.
    RolloverPrepared,
    /// A fresh target session was created and its Lead was verified.
    RolloverTargetReady,
    /// A verified target session became the Mission execution binding.
    RolloverBound,
    /// The target session acknowledged the continuation packet.
    RolloverApplied,
    /// A rollover attempt failed without changing the Mission owner.
    RolloverFailed,
    /// A rollover was abandoned because its optimistic ownership witness no
    /// longer matched.
    RolloverConflict,
    /// The generation completed cleanly.
    Completed,
    /// The generation was explicitly failed.
    Failed,
    /// The generation was explicitly cancelled.
    Cancelled,
}

impl MissionEventKind {
    pub fn as_str(self) -> &'static str {
        match self {
            MissionEventKind::Admitted => "admitted",
            MissionEventKind::SessionBound => "session_bound",
            MissionEventKind::ExploreToBuild => "explore_to_build",
            MissionEventKind::BuildToVerify => "build_to_verify",
            MissionEventKind::VerifyToDebug => "verify_to_debug",
            MissionEventKind::DebugToBuild => "debug_to_build",
            MissionEventKind::RolloverRequested => "rollover_requested",
            MissionEventKind::RolloverPrepared => "rollover_prepared",
            MissionEventKind::RolloverTargetReady => "rollover_target_ready",
            MissionEventKind::RolloverBound => "rollover_bound",
            MissionEventKind::RolloverApplied => "rollover_applied",
            MissionEventKind::RolloverFailed => "rollover_failed",
            MissionEventKind::RolloverConflict => "rollover_conflict",
            MissionEventKind::Completed => "completed",
            MissionEventKind::Failed => "failed",
            MissionEventKind::Cancelled => "cancelled",
        }
    }
}

/// One durable, idempotent transition record. `id` is deterministic over
/// `(mission_id, kind, generation, key)`, so re-recording the same logical
/// transition (for example after a crash/replay) collides with the existing
/// entry and is skipped instead of producing a duplicate effect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct MissionEvent {
    pub id: String,
    pub kind: MissionEventKind,
    pub generation: u32,
    /// The session that performed the transition, when one was involved.
    pub session_id: Option<String>,
    /// The checkpoint committing the transition's evidence, when one exists.
    pub checkpoint_id: Option<String>,
    /// A bounded, privacy-safe reason for consequential transitions.
    pub note: Option<String>,
    pub created_at: i64,
}

impl Default for MissionEvent {
    fn default() -> Self {
        Self {
            id: String::new(),
            kind: MissionEventKind::Admitted,
            generation: 1,
            session_id: None,
            checkpoint_id: None,
            note: None,
            created_at: 0,
        }
    }
}

/// The exact recoverable next semantic action of a non-terminal Mission,
/// derived from durable state alone — no conversation history required.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NextAction {
    /// The Lead owns the next move (fresh or reset admission).
    Lead,
    /// An Explore delegation is in flight or its result must be consumed.
    Explore,
    /// A Build (or a bounded Build retry) is next.
    Build,
    /// Verification is next or in flight.
    Verify,
    /// A Debug delegation is next.
    Debug,
    /// The Debug budget is exhausted: the user must decide.
    Escalate,
    /// Durable terminal state: completed.
    Complete,
    /// Durable terminal state: failed.
    Failed,
    /// Durable terminal state: cancelled.
    Cancelled,
}

impl NextAction {
    pub fn as_str(self) -> &'static str {
        match self {
            NextAction::Lead => "lead",
            NextAction::Explore => "explore",
            NextAction::Build => "build",
            NextAction::Verify => "verify",
            NextAction::Debug => "debug",
            NextAction::Escalate => "escalate",
            NextAction::Complete => "complete",
            NextAction::Failed => "failed",
            NextAction::Cancelled => "cancelled",
        }
    }
}

/// Mission-local rollover lifecycle.  This is intentionally small: the
/// detailed continuation and runtime operation state lives in a separate
/// artifact, while the Mission records only the durable binding intent and its
/// retry/failure boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MissionRolloverStatus {
    /// No rollover is active for this generation.
    #[default]
    Idle,
    /// The governor requested a rollover, but a safe boundary is not yet
    /// confirmed.
    Pending,
    /// A continuation artifact is durable and target creation is in progress.
    Preparing,
    /// A target session has been verified by the runtime.
    TargetReady,
    /// Mission ownership now points at the target; continuation is awaiting
    /// acknowledgement.
    Active,
    /// The target consumed the continuation packet.
    Applied,
    /// A retryable attempt failed while the old binding remained authoritative.
    Failed,
    /// A stale-owner/revision check prevented automatic cutover.
    Conflict,
}

impl MissionRolloverStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Pending => "pending",
            Self::Preparing => "preparing",
            Self::TargetReady => "target_ready",
            Self::Active => "active",
            Self::Applied => "applied",
            Self::Failed => "failed",
            Self::Conflict => "conflict",
        }
    }
}

/// The durable Mission-side rollover checkpoint.  It never contains the
/// continuation itself, so a corrupted artifact cannot make the Mission
/// silently claim that a target was activated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct MissionRolloverState {
    pub status: MissionRolloverStatus,
    pub artifact_id: Option<String>,
    pub source_session_id: Option<String>,
    pub target_session_id: Option<String>,
    pub generation: u32,
    pub reason: Option<String>,
    pub requested_at: Option<i64>,
    pub last_attempt_at: Option<i64>,
    pub retry_after: Option<i64>,
    pub last_error: Option<String>,
}

impl Default for MissionRolloverState {
    fn default() -> Self {
        Self {
            status: MissionRolloverStatus::Idle,
            artifact_id: None,
            source_session_id: None,
            target_session_id: None,
            generation: 0,
            reason: None,
            requested_at: None,
            last_attempt_at: None,
            retry_after: None,
            last_error: None,
        }
    }
}

/// The small, durable state machine used by the single-node reconciler.
///
/// This is execution-control metadata, not Mission phase semantics. It records
/// which idempotent recovery step has been claimed so a later tick can resume
/// after a process crash without blindly repeating a runtime side effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MissionReconcileStatus {
    #[default]
    Idle,
    Creating,
    Created,
    Bound,
    Preparing,
    Prepared,
    Staging,
    Staged,
    Resuming,
    Applied,
    Failed,
    Blocked,
    Conflict,
}

impl MissionReconcileStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Creating => "creating",
            Self::Created => "created",
            Self::Bound => "bound",
            Self::Preparing => "preparing",
            Self::Prepared => "prepared",
            Self::Staging => "staging",
            Self::Staged => "staged",
            Self::Resuming => "resuming",
            Self::Applied => "applied",
            Self::Failed => "failed",
            Self::Blocked => "blocked",
            Self::Conflict => "conflict",
        }
    }

    pub fn is_inflight(self) -> bool {
        matches!(
            self,
            Self::Creating
                | Self::Created
                | Self::Bound
                | Self::Preparing
                | Self::Prepared
                | Self::Staging
                | Self::Staged
                | Self::Resuming
        )
    }
}

/// A bounded, privacy-safe explanation of the latest reconcile outcome. The
/// control-plane enums live in the reconciler module; strings keep this
/// additive Mission field independent of that module and preserve old records.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct MissionReconcileReceipt {
    pub generation: u32,
    pub current_execution_id: Option<String>,
    pub observed: String,
    pub action: String,
    pub reason: String,
    pub result: String,
    pub timestamp: i64,
    /// The bounded Policy projection for this pass, when Policy was evaluated.
    /// It is additive to schema version 1; old records deserialize as `None`.
    pub policy: Option<MissionPolicyReceipt>,
    /// The bounded mandatory economic projection for this pass, when the
    /// economic admission boundary was evaluated. Additive to schema 1.
    pub budget: Option<MissionBudgetReceipt>,
}

/// The durable, bounded projection of one Policy assessment. It is a plain
/// string record so the durable Mission schema stays independent of the Policy
/// module's types.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct MissionPolicyReceipt {
    /// The effective decision (`allow`, `defer`, `require_approval`, `deny`).
    pub decision: String,
    /// The winning rule identifier.
    pub rule: String,
    pub reason_code: String,
    /// Bounded, secret-redacted reason.
    pub reason: String,
    /// The evaluated action name.
    pub action: String,
    /// The approval id, when the decision requires an approval.
    pub approval_id: Option<String>,
    /// Bounded `kind=status` fact summaries the winning rule used.
    pub required_facts: Vec<String>,
    /// Bounded `rule=decision` summaries for every non-allow co-firing rule, so
    /// a hard cap and an exhausted quota are never silently reduced to one.
    pub blocking: Vec<String>,
}

/// Durable reconcile intent and its current local phase. The bounded
/// continuation packet remains in the reconcile artifact, so Mission records do
/// not grow with transcript or prompt content.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct MissionReconcileState {
    pub status: MissionReconcileStatus,
    pub operation_id: Option<String>,
    pub base_execution_id: Option<String>,
    pub target_execution_id: Option<String>,
    pub continuation_id: Option<String>,
    pub operation_count: u32,
    /// Number of create attempts claimed for the current operation. A claim is
    /// made before the runtime call so concurrent local ticks cannot both
    /// create; after a crash the adapter recovery lookup decides whether the
    /// claim reached the runtime.
    pub create_attempt: u32,
    pub failed_phase: Option<MissionReconcileStatus>,
    pub retry_after: Option<i64>,
    /// Observation failures use a separate bounded backoff so a missing
    /// runtime cannot be confused with an authoritative missing execution.
    pub observation_retry_after: Option<i64>,
    pub observation_status: Option<String>,
    pub last_error: Option<String>,
    pub last_receipt: Option<MissionReconcileReceipt>,
}

impl Default for MissionReconcileState {
    fn default() -> Self {
        Self {
            status: MissionReconcileStatus::Idle,
            operation_id: None,
            base_execution_id: None,
            target_execution_id: None,
            continuation_id: None,
            operation_count: 0,
            create_attempt: 0,
            failed_phase: None,
            retry_after: None,
            observation_retry_after: None,
            observation_status: None,
            last_error: None,
            last_receipt: None,
        }
    }
}

///
/// The task-scoped fields mirror the session's live execution view
/// ([`SessionState`]); the controller syncs them at every mutation point.
/// Runtime/session-specific data (the repository baseline, the OpenCode
/// session entry itself) deliberately stays out of this record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Mission {
    pub schema_version: u32,
    /// Durable identity: the deterministic task id of the admitted task
    /// text. It never changes because a session does.
    pub mission_id: String,
    /// A monotonically increasing mutation witness used for same-generation
    /// rollover cutover. It is not a distributed lock; it only makes a stale
    /// owner/artifact pair fail closed when local Mission progress changed.
    pub revision: u64,
    /// The admission generation. Re-admitting a terminal Mission starts the
    /// next generation with fresh progress; history is retained across
    /// generations.
    pub generation: u32,
    /// Desired state: the stored (bounded, secret-redacted) task text and
    /// the goal/constraints refined by exploration.
    pub task: Option<String>,
    pub goal: Option<String>,
    pub constraints: Vec<String>,
    /// Observed lifecycle state.
    pub status: MissionStatus,
    /// The operational sub-state (who owns the next move).
    pub phase: OrchestrationPhase,
    pub source: Role,
    pub destination: Option<Role>,
    pub last_transition: Option<Transition>,
    /// Attempt identity consumed so far.
    pub attempts: Attempts,
    /// The current execution binding: replaceable metadata, NOT identity.
    pub session_id: Option<String>,
    pub findings: Vec<HandoffFinding>,
    pub files: Vec<String>,
    pub symbols: Vec<String>,
    pub failures: Vec<String>,
    pub evidence: Vec<String>,
    pub debug_reason: Option<String>,
    pub last_verification: Option<HandoffVerification>,
    pub last_report: Option<VerificationReport>,
    pub last_rich_bytes: usize,
    pub last_diff_context: String,
    /// References to the meaningful-progress checkpoints (newest last).
    pub checkpoints: Vec<String>,
    /// The bounded durable transition history.
    pub history: Vec<MissionEvent>,
    /// Stable event identities retained beyond the visible history window so
    /// delayed replays cannot reapply an old transition.
    #[serde(default)]
    pub event_index: Vec<String>,
    /// Same-generation session replacement state.  It is additive to schema
    /// version 1 and old records deserialize with `Idle`.
    #[serde(default)]
    pub rollover: MissionRolloverState,
    /// Additive single-node reconcile intent. It is not phase semantics and is
    /// ignored for terminal Missions.
    #[serde(default)]
    pub reconcile: MissionReconcileState,
    /// Additive mandatory economic accounting. It is not phase semantics; the
    /// hard budget and its reservations survive restart, rollover, retries and
    /// recovery.
    #[serde(default)]
    pub budget: MissionBudget,
    pub created_at: i64,
    pub updated_at: i64,
}

impl Default for Mission {
    fn default() -> Self {
        Self {
            schema_version: MISSION_SCHEMA_VERSION,
            mission_id: String::new(),
            revision: 1,
            generation: 1,
            task: None,
            goal: None,
            constraints: Vec::new(),
            status: MissionStatus::Active,
            phase: OrchestrationPhase::Idle,
            source: Role::Lead,
            destination: Some(Role::Lead),
            last_transition: None,
            attempts: Attempts::default(),
            session_id: None,
            findings: Vec::new(),
            files: Vec::new(),
            symbols: Vec::new(),
            failures: Vec::new(),
            evidence: Vec::new(),
            debug_reason: None,
            last_verification: None,
            last_report: None,
            last_rich_bytes: 0,
            last_diff_context: String::new(),
            checkpoints: Vec::new(),
            history: Vec::new(),
            event_index: Vec::new(),
            rollover: MissionRolloverState::default(),
            reconcile: MissionReconcileState::default(),
            budget: MissionBudget::default(),
            created_at: 0,
            updated_at: 0,
        }
    }
}

impl Mission {
    pub fn is_terminal(&self) -> bool {
        self.status.is_terminal()
    }
}

/// The Mission directory.
pub fn missions_dir(root: &Path) -> PathBuf {
    crate::orchestration::state::state_dir(root).join(MISSIONS_DIR)
}

/// The path for one Mission id. Unsafe ids are rejected, never hashed into
/// place: Mission identity must be used verbatim.
pub fn mission_path(root: &Path, mission_id: &str) -> Result<PathBuf> {
    if !crate::orchestration::checkpoint::is_safe_id(mission_id) {
        return Err(OcgError::config(format!(
            "unsafe mission id '{mission_id}' (expected lowercase letters, digits and '-')"
        )));
    }
    Ok(missions_dir(root).join(format!("{mission_id}.json")))
}

pub(crate) fn validate_mission(mission: &Mission) -> Result<()> {
    if mission.schema_version != MISSION_SCHEMA_VERSION {
        return Err(OcgError::config(format!(
            "mission {} has unsupported schema_version {}",
            mission.mission_id, mission.schema_version
        )));
    }
    if !crate::orchestration::checkpoint::is_safe_id(&mission.mission_id) {
        return Err(OcgError::config(format!(
            "unsafe mission id '{}'",
            mission.mission_id
        )));
    }
    if mission.generation == 0 || mission.revision == 0 {
        return Err(OcgError::config(
            "mission generation and revision must be positive",
        ));
    }
    if mission.rollover.generation != 0 && mission.rollover.generation != mission.generation {
        return Err(OcgError::config(
            "mission rollover state belongs to a different generation",
        ));
    }
    if mission.reconcile.status != MissionReconcileStatus::Idle
        && mission
            .reconcile
            .operation_id
            .as_deref()
            .is_none_or(str::is_empty)
    {
        return Err(OcgError::config(
            "mission reconcile state has no operation identity",
        ));
    }
    if let Some(limit) = mission.budget.hard_limit.as_ref() {
        if limit.micros <= 0 {
            return Err(OcgError::config(
                "mission hard budget must be a positive amount",
            ));
        }
        if mission.budget.currency.is_empty() || limit.currency != mission.budget.currency {
            return Err(OcgError::config(
                "mission budget currency is inconsistent with its hard limit",
            ));
        }
    }
    if mission.budget.settled.micros > 0
        && !mission.budget.currency.is_empty()
        && mission.budget.settled.currency != mission.budget.currency
    {
        return Err(OcgError::config(
            "mission settled spend currency is inconsistent with the budget currency",
        ));
    }
    if mission.budget.reservations.len() > budget::MAX_RESERVATIONS {
        return Err(OcgError::config(
            "mission budget retains too many reservations",
        ));
    }
    Ok(())
}

/// A strict raw read of the legacy projection, without consulting the replay
/// authority and without quarantining. Used to bootstrap the authority.
///
/// - `Ok(None)`: no record exists.
/// - `Err(_)`: the record exists but is corrupt, unsupported or belongs to a
///   different identity. The bytes are left untouched so a bootstrap can fail
///   closed with unresolved corruption.
pub(crate) fn load_raw(root: &Path, mission_id: &str) -> Result<Option<Mission>> {
    let path = mission_path(root, mission_id)?;
    let text = match fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(OcgError::read(&path, error)),
    };
    let mission: Mission = serde_json::from_str(&text)
        .map_err(|error| OcgError::config(format!("mission {mission_id} is corrupt: {error}")))?;
    if mission.mission_id != mission_id {
        return Err(OcgError::config(format!(
            "mission {mission_id} record identity {} does not match its path",
            mission.mission_id
        )));
    }
    validate_mission(&mission)?;
    Ok(Some(mission))
}

pub fn load(root: &Path, mission_id: &str) -> Result<Option<Mission>> {
    mission_path(root, mission_id)?;
    match crate::orchestration::replay::read_authoritative_snapshot(root)? {
        Some(snapshot) => Ok(snapshot.missions.get(mission_id).cloned()),
        None => load_raw(root, mission_id),
    }
}

/// A short summary used by `ocg doctor` and tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissionSummary {
    pub mission_id: String,
    pub status: MissionStatus,
    pub phase: OrchestrationPhase,
    pub generation: u32,
    pub session_id: Option<String>,
    pub updated_at: i64,
    pub file: String,
}

fn mission_summary(mission: &Mission) -> MissionSummary {
    MissionSummary {
        mission_id: mission.mission_id.clone(),
        status: mission.status,
        phase: mission.phase,
        generation: mission.generation,
        session_id: mission.session_id.clone(),
        updated_at: mission.updated_at,
        file: format!("{}.json", mission.mission_id),
    }
}

/// List every readable Mission, most recently updated first.
///
/// Once the replay authority is initialized it is the read source, so a
/// Mission whose projection file failed or was removed is still listed from
/// authoritative state. Only before initialization is the legacy projection
/// scanned. A read-only operation: it never initializes or creates the replay
/// store and never quarantines.
pub fn list(root: &Path) -> (Vec<MissionSummary>, usize) {
    match crate::orchestration::replay::read_authoritative_snapshot(root) {
        Ok(Some(snapshot)) => {
            let mut summaries: Vec<MissionSummary> =
                snapshot.missions.values().map(mission_summary).collect();
            summaries.sort_by(|a, b| {
                b.updated_at
                    .cmp(&a.updated_at)
                    .then_with(|| a.mission_id.cmp(&b.mission_id))
            });
            (summaries, 0)
        }
        Ok(None) => list_projection(root),
        // `list` has no error channel. An initialized-but-unreadable authority
        // is reported as one unresolved corruption instead of an empty list,
        // so a caller cannot mistake authority loss for "no Missions".
        Err(_) => (Vec::new(), 1),
    }
}

fn list_projection(root: &Path) -> (Vec<MissionSummary>, usize) {
    let dir = missions_dir(root);
    let Ok(entries) = fs::read_dir(&dir) else {
        return (Vec::new(), 0);
    };
    let mut summaries = Vec::new();
    let mut corrupt = 0usize;
    for entry in entries.flatten() {
        let path = entry.path();
        let name = path
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default();
        if name.ends_with(".corrupt.json") {
            continue;
        }
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
        match serde_json::from_str::<Mission>(&text) {
            Ok(mission)
                if mission.schema_version == MISSION_SCHEMA_VERSION
                    && mission.mission_id == name.strip_suffix(".json").unwrap_or_default()
                    && validate_mission(&mission).is_ok() =>
            {
                summaries.push(MissionSummary {
                    mission_id: mission.mission_id,
                    status: mission.status,
                    phase: mission.phase,
                    generation: mission.generation,
                    session_id: mission.session_id,
                    updated_at: mission.updated_at,
                    file: name,
                });
            }
            _ => corrupt += 1,
        }
    }
    summaries.sort_by(|a, b| {
        b.updated_at
            .cmp(&a.updated_at)
            .then_with(|| a.mission_id.cmp(&b.mission_id))
    });
    (summaries, corrupt)
}
