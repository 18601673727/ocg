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
use crate::orchestration::budget::{
    self, BudgetConfig, CostBasis, MissionBudget, MissionBudgetReceipt, Money, QuotaFacts,
    SpendAction, SpendAssessment, SpendDecision,
};
use crate::orchestration::handoff::{HandoffFinding, HandoffVerification, Role, Transition};
use crate::orchestration::state::{Attempts, OrchestrationPhase, SessionState};
use crate::runtime::lifecycle::{
    resolve_execution_lineage, RuntimeAdapter, RuntimeError, RuntimeErrorKind, RuntimeExecutionId,
    RuntimeExecutionLineage,
};
use crate::verification::result::VerificationReport;
use serde::{Deserialize, Serialize};
use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

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
    /// Admit a new Mission (generation 1, active, bound to `session_key`).
    pub fn admit(mission_id: &str, task_text: &str, session_key: &str, now: i64) -> Self {
        let mut mission = Self {
            mission_id: mission_id.to_string(),
            task: if task_text.is_empty() {
                None
            } else {
                Some(task_text.to_string())
            },
            session_id: Some(session_key.to_string()),
            created_at: now,
            updated_at: now,
            ..Self::default()
        };
        let event = mission.event(
            MissionEventKind::Admitted,
            "",
            None,
            Some(session_key.to_string()),
            None,
            now,
        );
        mission.record(event);
        mission
    }

    /// Whether the Mission reached a durable terminal state.
    pub fn is_terminal(&self) -> bool {
        self.status.is_terminal()
    }

    /// The replaceable runtime execution binding, kept distinct from the
    /// durable Mission identity. The serialized field remains `session_id` for
    /// backward compatibility with existing Mission records.
    pub fn runtime_execution_id(&self) -> Option<RuntimeExecutionId> {
        self.session_id.as_deref().map(RuntimeExecutionId::new)
    }

    /// Start a new same-generation execution recovery operation. The Mission
    /// remains the authority; this only claims the local reconcile state before
    /// a runtime side effect is attempted.
    pub fn begin_reconcile(
        &mut self,
        operation_id: &str,
        base_execution_id: Option<&RuntimeExecutionId>,
        now: i64,
    ) -> bool {
        if self.is_terminal() || operation_id.is_empty() {
            return false;
        }
        if self.reconcile.status == MissionReconcileStatus::Conflict
            || self.reconcile.status == MissionReconcileStatus::Blocked
            || self.reconcile.status == MissionReconcileStatus::Failed
        {
            return false;
        }
        if self.reconcile.status.is_inflight()
            || self.reconcile.operation_id.as_deref() == Some(operation_id)
        {
            return false;
        }
        self.reconcile = MissionReconcileState {
            status: MissionReconcileStatus::Creating,
            operation_id: Some(operation_id.to_string()),
            base_execution_id: base_execution_id.map(|id| id.as_str().to_string()),
            target_execution_id: None,
            continuation_id: None,
            operation_count: self.reconcile.operation_count.saturating_add(1),
            create_attempt: 0,
            failed_phase: None,
            retry_after: None,
            observation_retry_after: None,
            observation_status: None,
            last_error: None,
            last_receipt: self.reconcile.last_receipt.clone(),
        };
        self.revision = self.revision.saturating_add(1);
        self.updated_at = now;
        true
    }

    /// Transition an already-claimed reconcile operation. The expected phase
    /// makes a stale concurrent tick fail closed; callers still persist with
    /// [`mission::save_if_revision`] at the Mission boundary.
    pub fn transition_reconcile(
        &mut self,
        expected: MissionReconcileStatus,
        next: MissionReconcileStatus,
        target_execution_id: Option<&RuntimeExecutionId>,
        now: i64,
    ) -> bool {
        if self.is_terminal() || self.reconcile.status != expected {
            return false;
        }
        let next_target = target_execution_id
            .map(|id| id.as_str().to_string())
            .or_else(|| self.reconcile.target_execution_id.clone());
        let changed =
            self.reconcile.status != next || self.reconcile.target_execution_id != next_target;
        self.reconcile.status = next;
        self.reconcile.target_execution_id = next_target;
        if next != MissionReconcileStatus::Failed {
            self.reconcile.failed_phase = None;
            self.reconcile.retry_after = None;
            self.reconcile.observation_retry_after = None;
            self.reconcile.observation_status = None;
            self.reconcile.last_error = None;
        }
        if changed {
            self.revision = self.revision.saturating_add(1);
            self.updated_at = now;
        }
        changed
    }

    /// Claim one local create attempt for a `Creating` operation. The claim is
    /// durable before the runtime call; a concurrent tick that loses the CAS
    /// must perform recovery/observation instead of creating as well.
    pub fn claim_reconcile_create(&mut self, now: i64) -> bool {
        if self.is_terminal() || self.reconcile.status != MissionReconcileStatus::Creating {
            return false;
        }
        self.reconcile.create_attempt = self.reconcile.create_attempt.saturating_add(1);
        self.revision = self.revision.saturating_add(1);
        self.updated_at = now;
        true
    }

    /// Record a bounded observation-failure backoff without changing the
    /// Mission's semantic or execution state.
    pub fn defer_observation(&mut self, status: &str, retry_after: i64, now: i64) -> bool {
        if self.is_terminal() || status.is_empty() {
            return false;
        }
        let status = bounded_reconcile_text(status, 64);
        let next = self
            .reconcile
            .observation_retry_after
            .map(|current| current.max(retry_after))
            .unwrap_or(retry_after);
        let changed = self.reconcile.observation_retry_after != Some(next)
            || self.reconcile.observation_status.as_deref() != Some(status.as_str());
        if changed {
            self.reconcile.observation_retry_after = Some(next);
            self.reconcile.observation_status = Some(status);
            self.revision = self.revision.saturating_add(1);
            self.updated_at = now;
        }
        changed
    }

    /// Clear an observation backoff after an authoritative healthy observation.
    pub fn clear_observation_defer(&mut self, now: i64) -> bool {
        if self.is_terminal()
            || (self.reconcile.observation_retry_after.is_none()
                && self.reconcile.observation_status.is_none())
        {
            return false;
        }
        self.reconcile.observation_retry_after = None;
        self.reconcile.observation_status = None;
        self.revision = self.revision.saturating_add(1);
        self.updated_at = now;
        true
    }

    /// Mark a claimed operation retryable without changing its target or
    /// generation. A stable continuation id is retained by the reconciler
    /// artifact, so a retry cannot manufacture a new semantic continuation.
    pub fn fail_reconcile(
        &mut self,
        expected: MissionReconcileStatus,
        failed_phase: MissionReconcileStatus,
        error: &str,
        retry_after: Option<i64>,
        now: i64,
    ) -> bool {
        if self.is_terminal() || self.reconcile.status != expected {
            return false;
        }
        self.reconcile.status = MissionReconcileStatus::Failed;
        self.reconcile.failed_phase = Some(failed_phase);
        self.reconcile.retry_after = retry_after;
        self.reconcile.last_error = Some(redact_reconcile_reason(error));
        self.revision = self.revision.saturating_add(1);
        self.updated_at = now;
        true
    }

    /// Freeze a recovery that cannot safely proceed without an operator or a
    /// changed runtime capability. This is deliberately not a Mission failure.
    pub fn block_reconcile(&mut self, reason: &str, now: i64) -> bool {
        if self.is_terminal()
            || matches!(
                self.reconcile.status,
                MissionReconcileStatus::Blocked | MissionReconcileStatus::Conflict
            )
        {
            return false;
        }
        self.reconcile.status = MissionReconcileStatus::Blocked;
        self.reconcile.last_error = Some(redact_reconcile_reason(reason));
        self.revision = self.revision.saturating_add(1);
        self.updated_at = now;
        true
    }

    /// Record a safe, bounded latest reconcile explanation. This is diagnostic
    /// metadata, not a Mission event, and therefore cannot duplicate semantic
    /// history on replay.
    pub fn record_reconcile_receipt(&mut self, mut receipt: MissionReconcileReceipt) -> bool {
        if self.is_terminal() {
            return false;
        }
        receipt.observed = bounded_reconcile_text(&receipt.observed, 64);
        receipt.action = bounded_reconcile_text(&receipt.action, 64);
        receipt.reason = redact_reconcile_reason(&receipt.reason);
        receipt.result = bounded_reconcile_text(&receipt.result, 32);
        if let Some(policy) = receipt.policy.as_mut() {
            policy.decision = bounded_reconcile_text(&policy.decision, 32);
            policy.rule = bounded_reconcile_text(&policy.rule, 64);
            policy.reason_code = bounded_reconcile_text(&policy.reason_code, 64);
            policy.reason = redact_reconcile_reason(&policy.reason);
            policy.action = bounded_reconcile_text(&policy.action, 64);
            policy.approval_id = policy
                .approval_id
                .as_deref()
                .map(|id| bounded_reconcile_text(id, 96));
            policy.required_facts = policy
                .required_facts
                .iter()
                .take(16)
                .map(|fact| bounded_reconcile_text(fact, 64))
                .collect();
            policy.blocking = policy
                .blocking
                .iter()
                .take(8)
                .map(|block| bounded_reconcile_text(block, 64))
                .collect();
        }
        if let Some(budget) = receipt.budget.as_mut() {
            budget.status = bounded_reconcile_text(&budget.status, 32);
            budget.origin = bounded_reconcile_text(&budget.origin, 32);
            budget.currency = bounded_reconcile_text(&budget.currency, 16);
            budget.reason = budget.reason.as_deref().map(redact_reconcile_reason);
        }
        let changed = self.reconcile.last_receipt.as_ref().is_none_or(|previous| {
            previous.generation != receipt.generation
                || previous.current_execution_id != receipt.current_execution_id
                || previous.observed != receipt.observed
                || previous.action != receipt.action
                || previous.reason != receipt.reason
                || previous.result != receipt.result
                || previous.policy != receipt.policy
                || previous.budget != receipt.budget
        });
        if changed {
            self.reconcile.last_receipt = Some(receipt.clone());
            self.revision = self.revision.saturating_add(1);
            self.updated_at = receipt.timestamp;
        }
        changed
    }

    /// Evaluate and durably reserve the bounded spend for one provider-costly
    /// action. This is the mandatory economic admission boundary: it runs
    /// independently of `policy.enabled` and can never be satisfied by a
    /// generic approval. An `Allow` with a hard limit records a reservation
    /// exactly once; a `Defer` or `Deny` mutates nothing.
    pub fn admit_spend(
        &mut self,
        config: &BudgetConfig,
        action: SpendAction,
        operation_id: &str,
        estimate: CostBasis,
        quota: QuotaFacts,
        now: i64,
    ) -> SpendAssessment {
        let configured_limit = config.hard_limit_micros.is_some();
        let materialized = self.budget.materialize_config(config);
        // A hard budget is configured but is not in force for this Mission
        // (for example the Mission already accounts in a different currency and
        // OCG never performs FX conversion). Absence of an enforceable limit
        // must never become an uncapped admission: fail closed.
        if configured_limit && self.budget.hard_limit.is_none() {
            let conflict = !self.budget.currency.is_empty()
                && config
                    .currency
                    .as_deref()
                    .is_some_and(|currency| currency != self.budget.currency);
            if conflict {
                let assessment = budget::SpendAssessment::configured_but_unenforceable(
                    &self.budget,
                    budget::REASON_CURRENCY,
                    "a hard Mission budget is configured in another currency and cannot be applied without FX conversion",
                );
                let reason_changed =
                    self.budget.reason.as_deref() != Some(assessment.reason_code.as_str());
                self.budget.reason = Some(assessment.reason_code.clone());
                if reason_changed {
                    self.revision = self.revision.saturating_add(1);
                    self.updated_at = now;
                }
                return assessment;
            }
        }
        // A live or settled reservation for exactly this operation means the
        // bounded spend is already authorized and accounted. Re-admission is
        // idempotent: it never double-counts and never re-reserves.
        let existing = self
            .budget
            .reservation_for(action, &self.mission_id, self.generation, operation_id)
            .filter(|reservation| reservation.state != budget::ReservationState::Released)
            .map(|reservation| reservation.reservation_id.clone());
        let already_reserved = existing.is_some();
        let request = budget::SpendRequest {
            action,
            operation_id,
            estimate,
            quota,
            already_reserved,
        };
        let mut assessment = budget::admit(&self.budget, config.require_quota, &request);
        let mut mutated = materialized;
        if let Some(id) = existing {
            assessment.reservation_id = Some(id);
        }
        if assessment.decision == SpendDecision::Allow {
            if let Some(amount) = assessment.amount.clone() {
                let id =
                    budget::reservation_id(action, &self.mission_id, self.generation, operation_id);
                mutated |= self.budget.reserve(
                    action,
                    &self.mission_id,
                    self.generation,
                    operation_id,
                    amount,
                    now,
                );
                assessment.reservation_id = Some(id);
            }
        }
        let reason_changed = self.budget.reason.as_deref() != Some(assessment.reason_code.as_str());
        self.budget.reason = Some(assessment.reason_code.clone());
        if mutated || reason_changed {
            self.revision = self.revision.saturating_add(1);
            self.updated_at = now;
        }
        assessment
    }

    /// Settle a bounded spend exactly once against real or estimated actual
    /// usage. Actual overage is recorded, never clamped.
    pub fn settle_spend(
        &mut self,
        reservation_id: &str,
        actual: Option<Money>,
        now: i64,
    ) -> Result<bool> {
        let changed = self.budget.settle(reservation_id, actual, now)?;
        if changed {
            self.revision = self.revision.saturating_add(1);
            self.updated_at = now;
        }
        Ok(changed)
    }

    /// Release a reservation that is proven not to have reached the provider.
    pub fn release_spend(&mut self, reservation_id: &str, now: i64) -> bool {
        let changed = self.budget.release(reservation_id, now);
        if changed {
            self.revision = self.revision.saturating_add(1);
            self.updated_at = now;
        }
        changed
    }

    /// Retain a reservation whose dispatch outcome is uncertain.
    pub fn mark_spend_unresolved(&mut self, reservation_id: &str, now: i64) -> bool {
        let changed = self.budget.mark_unresolved(reservation_id, now);
        if changed {
            self.revision = self.revision.saturating_add(1);
            self.updated_at = now;
        }
        changed
    }

    /// Explicitly change the hard Mission budget. This is the only supported
    /// way past a hard cap; an ordinary approval can never do it.
    pub fn set_hard_budget(&mut self, amount: Money, now: i64) -> Result<bool> {
        let changed = self.budget.set_hard_limit(amount, now)?;
        if changed {
            self.revision = self.revision.saturating_add(1);
            self.updated_at = now;
        }
        Ok(changed)
    }

    /// Supersede a recoverable rollover when a new explicit user session
    /// admission replaces an interrupted execution session. The old artifact
    /// remains on disk as a conflict record; the same Mission generation and
    /// progress continue under the new owner.
    pub fn supersede_rollover_for_rebind(&mut self, session_key: &str, now: i64) -> Result<bool> {
        self.ensure_rollover_mutable()?;
        if self.rollover.status == MissionRolloverStatus::Conflict {
            return Err(OcgError::config(
                "Mission rollover is conflicted; explicit operator recovery is required before rebinding",
            ));
        }
        if self.rollover.status == MissionRolloverStatus::Idle
            || self.rollover.status == MissionRolloverStatus::Applied
        {
            return Ok(false);
        }
        let old_artifact = self.rollover.artifact_id.clone();
        self.rollover = MissionRolloverState::default();
        // An explicit admission is a user-owned cutover. If an execution
        // recovery was in flight for a different target, the reconciler must
        // not later promote that stale target over the explicit binding, so
        // freeze the operation as an explicit conflict. The direct rebind below
        // would otherwise bypass `bind_session`'s in-flight guard.
        if self.reconcile.status.is_inflight()
            && self.reconcile.target_execution_id.as_deref() != Some(session_key)
        {
            self.reconcile.status = MissionReconcileStatus::Conflict;
            self.reconcile.last_error = Some(redact_reconcile_reason(
                "explicit session admission superseded reconcile recovery",
            ));
        }
        if let Some(artifact_id) = old_artifact {
            let event = self.event(
                MissionEventKind::RolloverConflict,
                &artifact_id,
                None,
                Some(session_key.to_string()),
                Some(redact_reason(
                    "explicit session admission superseded an interrupted rollover",
                )),
                now,
            );
            self.record(event);
        }
        self.session_id = Some(session_key.to_string());
        let event = self.event(
            MissionEventKind::SessionBound,
            session_key,
            None,
            Some(session_key.to_string()),
            None,
            now,
        );
        self.record(event);
        self.updated_at = now;
        Ok(true)
    }

    /// The exact recoverable next semantic action, from durable state alone.
    pub fn next_action(&self, max_debug_retries: usize) -> NextAction {
        match self.status {
            MissionStatus::Completed => NextAction::Complete,
            MissionStatus::Failed => NextAction::Failed,
            MissionStatus::Cancelled => NextAction::Cancelled,
            MissionStatus::Active => match self.phase {
                OrchestrationPhase::Idle => NextAction::Lead,
                OrchestrationPhase::Explore => NextAction::Explore,
                OrchestrationPhase::Build => NextAction::Build,
                OrchestrationPhase::Verify => NextAction::Verify,
                OrchestrationPhase::Debug => {
                    if self.attempts.debug > max_debug_retries {
                        NextAction::Escalate
                    } else {
                        NextAction::Debug
                    }
                }
                // A passed build marks the Mission completed in the same
                // transition; an active Mission in `Done` is defensive only.
                OrchestrationPhase::Done => NextAction::Complete,
            },
        }
    }

    /// Build a transition event with its deterministic identity. `key` is the
    /// transition-specific identity input: the checkpoint id for checkpointed
    /// hand-offs, the session id for bindings, empty for generation-scoped
    /// events (admission, terminal transitions).
    pub fn event(
        &self,
        kind: MissionEventKind,
        key: &str,
        checkpoint_id: Option<String>,
        session_id: Option<String>,
        note: Option<String>,
        created_at: i64,
    ) -> MissionEvent {
        MissionEvent {
            id: event_id(&self.mission_id, kind, self.generation, key),
            kind,
            generation: self.generation,
            session_id,
            checkpoint_id,
            note,
            created_at,
        }
    }

    /// Append an event, skipping an already-recorded identity. Returns
    /// whether the event was appended. This is the idempotent-replay
    /// mechanism: the same transition identity never applies twice.
    pub fn record(&mut self, event: MissionEvent) -> bool {
        if self.history.iter().any(|existing| existing.id == event.id)
            || self
                .event_index
                .iter()
                .any(|existing| existing == &event.id)
        {
            return false;
        }
        self.event_index.push(event.id.clone());
        const EVENT_INDEX_LIMIT: usize = MAX_MISSION_HISTORY * 2;
        if self.event_index.len() > EVENT_INDEX_LIMIT {
            let excess = self.event_index.len() - EVENT_INDEX_LIMIT;
            self.event_index.drain(0..excess);
        }
        self.history.push(event);
        self.revision = self.revision.saturating_add(1);
        if self.history.len() > MAX_MISSION_HISTORY {
            let excess = self.history.len() - MAX_MISSION_HISTORY;
            self.history.drain(0..excess);
        }
        true
    }

    /// Record a rollover transition whose event identity is stable across
    /// retries. [`Mission::record`] deduplicates that identity; when the caller
    /// has nevertheless changed durable control metadata (for example re-opening
    /// a retryable rollover or refreshing its retry cooldown), the mutation
    /// witness must still advance. Otherwise the replay authority would reject
    /// a content change committed at an unchanged revision.
    fn record_or_advance(&mut self, event: MissionEvent, before: &Mission) -> bool {
        let recorded = self.record(event);
        let state_changed = self != before;
        if !recorded && state_changed {
            self.revision = self.revision.saturating_add(1);
        }
        recorded || state_changed
    }

    /// Bind a runtime execution as the Mission's current execution state. The
    /// serialized `session_id` remains the backward-compatible wire field.
    pub fn bind_runtime_execution(&mut self, execution_id: &RuntimeExecutionId, now: i64) -> bool {
        self.bind_session(execution_id.as_str(), now)
    }

    /// Bind a session as the Mission's current execution state. A changed
    /// binding is recorded; rebinding the same session is a no-op.
    pub fn bind_session(&mut self, session_key: &str, now: i64) -> bool {
        if self.is_terminal() {
            return false;
        }
        if self.session_id.as_deref() == Some(session_key) {
            return false;
        }
        if self.reconcile.status.is_inflight()
            && self.reconcile.target_execution_id.as_deref() != Some(session_key)
        {
            self.reconcile.status = MissionReconcileStatus::Conflict;
            self.reconcile.last_error = Some(redact_reconcile_reason(
                "current execution binding changed during reconcile recovery",
            ));
        }
        self.session_id = Some(session_key.to_string());
        let event = self.event(
            MissionEventKind::SessionBound,
            session_key,
            None,
            Some(session_key.to_string()),
            None,
            now,
        );
        self.record(event);
        self.updated_at = now;
        true
    }

    /// Release the current binding (the session moved to another Mission).
    pub fn release_session(&mut self, now: i64) -> bool {
        if self.is_terminal() || self.session_id.is_none() {
            return false;
        }
        if self.reconcile.status.is_inflight() {
            self.reconcile.status = MissionReconcileStatus::Conflict;
            self.reconcile.last_error = Some(redact_reconcile_reason(
                "current execution binding was released during reconcile recovery",
            ));
        }
        self.session_id = None;
        let event = self.event(
            MissionEventKind::SessionBound,
            "released",
            None,
            None,
            None,
            now,
        );
        self.record(event);
        self.updated_at = now;
        true
    }

    /// Request a same-generation rollover from the current execution owner.
    /// The request never changes `generation` and never changes `session_id`.
    pub fn request_rollover(
        &mut self,
        source_session_id: &str,
        artifact_id: &str,
        reason: &str,
        now: i64,
    ) -> Result<bool> {
        self.ensure_rollover_mutable()?;
        if source_session_id.is_empty() || artifact_id.is_empty() {
            return Err(OcgError::config(
                "rollover request requires a source session and artifact id",
            ));
        }
        if self.session_id.as_deref() != Some(source_session_id) {
            return Err(OcgError::config(format!(
                "rollover source session {source_session_id} is not the current Mission owner"
            )));
        }
        if self.rollover.artifact_id.as_deref() == Some(artifact_id) {
            return Ok(false);
        }
        if self.rollover.status == MissionRolloverStatus::Failed
            && self
                .rollover
                .retry_after
                .is_some_and(|retry_after| now < retry_after)
        {
            return Err(OcgError::config(format!(
                "Mission {} rollover retry is cooling down until {}",
                self.mission_id,
                self.rollover.retry_after.unwrap_or_default()
            )));
        }
        if matches!(
            self.rollover.status,
            MissionRolloverStatus::Pending
                | MissionRolloverStatus::Preparing
                | MissionRolloverStatus::TargetReady
                | MissionRolloverStatus::Active
        ) {
            return Err(OcgError::config(format!(
                "Mission {} already has an active rollover artifact {}",
                self.mission_id,
                self.rollover.artifact_id.as_deref().unwrap_or("unknown")
            )));
        }
        if self.rollover.status == MissionRolloverStatus::Conflict {
            return Err(OcgError::config(format!(
                "Mission {} rollover is conflicted and requires operator review",
                self.mission_id
            )));
        }
        self.rollover = MissionRolloverState {
            status: MissionRolloverStatus::Pending,
            artifact_id: Some(artifact_id.to_string()),
            source_session_id: Some(source_session_id.to_string()),
            target_session_id: None,
            generation: self.generation,
            reason: Some(redact_reason(reason)),
            requested_at: Some(now),
            last_attempt_at: Some(now),
            retry_after: None,
            last_error: None,
        };
        let event = self.event(
            MissionEventKind::RolloverRequested,
            artifact_id,
            None,
            Some(source_session_id.to_string()),
            Some(redact_reason(reason)),
            now,
        );
        self.record(event);
        self.updated_at = now;
        Ok(true)
    }

    /// Mark a continuation artifact as durable while retaining the old owner.
    pub fn mark_rollover_prepared(&mut self, artifact_id: &str, now: i64) -> Result<bool> {
        self.ensure_rollover_mutable()?;
        if self.rollover.artifact_id.as_deref() != Some(artifact_id) {
            return Err(OcgError::config(
                "rollover artifact does not match the Mission request",
            ));
        }
        if self.rollover.status == MissionRolloverStatus::Applied
            || self.rollover.status == MissionRolloverStatus::Conflict
        {
            return Ok(false);
        }
        if !matches!(
            self.rollover.status,
            MissionRolloverStatus::Pending | MissionRolloverStatus::Failed
        ) {
            return Ok(false);
        }
        let before = self.clone();
        self.rollover.status = MissionRolloverStatus::Preparing;
        self.rollover.last_attempt_at = Some(now);
        self.rollover.last_error = None;
        let event = self.event(
            MissionEventKind::RolloverPrepared,
            artifact_id,
            None,
            self.rollover.source_session_id.clone(),
            None,
            now,
        );
        let changed = self.record_or_advance(event, &before);
        self.updated_at = now;
        Ok(changed)
    }

    /// Record a verified target without changing Mission ownership.
    pub fn mark_rollover_target_ready(
        &mut self,
        artifact_id: &str,
        target_session_id: &str,
        now: i64,
    ) -> Result<bool> {
        self.ensure_rollover_mutable()?;
        if self.rollover.artifact_id.as_deref() != Some(artifact_id)
            || target_session_id.is_empty()
            || self.rollover.source_session_id.as_deref() == Some(target_session_id)
        {
            return Err(OcgError::config(
                "rollover target is invalid or does not match the Mission request",
            ));
        }
        if !matches!(
            self.rollover.status,
            MissionRolloverStatus::Preparing | MissionRolloverStatus::Failed
        ) {
            return Err(OcgError::config(format!(
                "Mission {} rollover is {}; a target cannot be accepted",
                self.mission_id,
                self.rollover.status.as_str()
            )));
        }
        if self.rollover.target_session_id.as_deref() == Some(target_session_id) {
            return Ok(false);
        }
        let before = self.clone();
        self.rollover.status = MissionRolloverStatus::TargetReady;
        self.rollover.target_session_id = Some(target_session_id.to_string());
        self.rollover.last_attempt_at = Some(now);
        self.rollover.last_error = None;
        let event = self.event(
            MissionEventKind::RolloverTargetReady,
            artifact_id,
            None,
            Some(target_session_id.to_string()),
            None,
            now,
        );
        let changed = self.record_or_advance(event, &before);
        self.updated_at = now;
        Ok(changed)
    }

    /// Atomically switch the Mission execution binding to a verified target,
    /// preserving identity, generation, progress and terminal semantics.
    pub fn bind_rollover_session(
        &mut self,
        artifact_id: &str,
        source_session_id: &str,
        target_session_id: &str,
        now: i64,
    ) -> Result<bool> {
        self.ensure_rollover_mutable()?;
        let source_matches = self.rollover.source_session_id.as_deref() == Some(source_session_id)
            || (self.session_id.as_deref() == Some(target_session_id)
                && source_session_id == target_session_id);
        if self.rollover.artifact_id.as_deref() != Some(artifact_id)
            || !source_matches
            || self.rollover.target_session_id.as_deref() != Some(target_session_id)
        {
            return Err(OcgError::config(
                "rollover cutover does not match the prepared Mission intent",
            ));
        }
        if self.rollover.status != MissionRolloverStatus::TargetReady
            && self.rollover.status != MissionRolloverStatus::Active
        {
            return Err(OcgError::config(format!(
                "Mission {} rollover is {}; target cutover is not ready",
                self.mission_id,
                self.rollover.status.as_str()
            )));
        }
        if self.session_id.as_deref() == Some(target_session_id) {
            // Recovery may replay the cutover after the Mission was already
            // switched but before continuation acknowledgement was recorded.
            // Treat that as the same transition; never emit a second owner or
            // reset generation/terminal state.
            if self.rollover.status == MissionRolloverStatus::TargetReady {
                let before = self.clone();
                self.rollover.status = MissionRolloverStatus::Active;
                self.rollover.last_error = None;
                let event = self.event(
                    MissionEventKind::RolloverBound,
                    artifact_id,
                    None,
                    Some(target_session_id.to_string()),
                    Some(redact_reason("same-generation rollover recovery")),
                    now,
                );
                let _ = self.record_or_advance(event, &before);
                self.updated_at = now;
                return Ok(true);
            }
            return Ok(false);
        }
        if self.session_id.as_deref() != Some(source_session_id) {
            return Err(OcgError::config(format!(
                "Mission {} owner changed before rollover cutover; refusing to overwrite",
                self.mission_id
            )));
        }
        let before = self.clone();
        self.session_id = Some(target_session_id.to_string());
        self.rollover.status = MissionRolloverStatus::Active;
        self.rollover.last_error = None;
        let event = self.event(
            MissionEventKind::RolloverBound,
            artifact_id,
            None,
            Some(target_session_id.to_string()),
            Some(redact_reason("same-generation rollover")),
            now,
        );
        let changed = self.record_or_advance(event, &before);
        self.updated_at = now;
        Ok(changed)
    }

    /// Mark continuation acknowledgement for the current target.
    pub fn mark_rollover_applied(
        &mut self,
        artifact_id: &str,
        target_session_id: &str,
        now: i64,
    ) -> Result<bool> {
        self.ensure_rollover_mutable()?;
        if self.rollover.artifact_id.as_deref() != Some(artifact_id)
            || self.rollover.target_session_id.as_deref() != Some(target_session_id)
            || self.session_id.as_deref() != Some(target_session_id)
        {
            return Err(OcgError::config(
                "rollover acknowledgement does not match the active target",
            ));
        }
        if self.rollover.status == MissionRolloverStatus::Applied {
            return Ok(false);
        }
        if self.rollover.status != MissionRolloverStatus::Active {
            return Err(OcgError::config(format!(
                "Mission {} rollover is {}; continuation is not active",
                self.mission_id,
                self.rollover.status.as_str()
            )));
        }
        let before = self.clone();
        self.rollover.status = MissionRolloverStatus::Applied;
        self.rollover.retry_after = None;
        self.rollover.last_error = None;
        let event = self.event(
            MissionEventKind::RolloverApplied,
            artifact_id,
            None,
            Some(target_session_id.to_string()),
            None,
            now,
        );
        let changed = self.record_or_advance(event, &before);
        self.updated_at = now;
        Ok(changed)
    }

    /// Record a retryable failure without changing the old execution owner.
    pub fn mark_rollover_failed(
        &mut self,
        artifact_id: &str,
        error: &str,
        now: i64,
        retry_after: Option<i64>,
    ) -> Result<bool> {
        if self.is_terminal() {
            return Ok(false);
        }
        if self.rollover.artifact_id.as_deref() != Some(artifact_id) {
            return Err(OcgError::config(
                "rollover failure does not match the Mission request",
            ));
        }
        if self.rollover.status == MissionRolloverStatus::Conflict
            || self.rollover.status == MissionRolloverStatus::Applied
        {
            return Ok(false);
        }
        let old_status = self.rollover.status;
        let before = self.clone();
        self.rollover.status = MissionRolloverStatus::Failed;
        self.rollover.last_attempt_at = Some(now);
        self.rollover.retry_after = retry_after;
        self.rollover.last_error = Some(redact_reason(error));
        if old_status == MissionRolloverStatus::Active {
            // An acknowledgement failure after cutover is still a failure of the
            // continuation, but the binding remains target-authoritative.  A
            // later idempotent acknowledgement can recover it.
            self.rollover.status = MissionRolloverStatus::Active;
        }
        let event = self.event(
            MissionEventKind::RolloverFailed,
            artifact_id,
            None,
            self.session_id.clone(),
            Some(redact_reason(error)),
            now,
        );
        let changed = self.record_or_advance(event, &before);
        self.updated_at = now;
        Ok(changed)
    }

    /// Freeze automatic rollover when its ownership witness is stale.
    pub fn mark_rollover_conflict(
        &mut self,
        artifact_id: &str,
        reason: &str,
        now: i64,
    ) -> Result<bool> {
        if self.is_terminal() {
            return Ok(false);
        }
        if self.rollover.artifact_id.as_deref() != Some(artifact_id) {
            return Err(OcgError::config(
                "rollover conflict does not match the Mission request",
            ));
        }
        if self.rollover.status == MissionRolloverStatus::Conflict {
            return Ok(false);
        }
        let before = self.clone();
        self.rollover.status = MissionRolloverStatus::Conflict;
        self.rollover.last_error = Some(redact_reason(reason));
        let event = self.event(
            MissionEventKind::RolloverConflict,
            artifact_id,
            None,
            self.session_id.clone(),
            Some(redact_reason(reason)),
            now,
        );
        let changed = self.record_or_advance(event, &before);
        self.updated_at = now;
        Ok(changed)
    }

    /// Whether a failed request is outside its retry cooldown.
    pub fn rollover_retry_allowed(&self, now: i64) -> bool {
        match self.rollover.status {
            MissionRolloverStatus::Failed => self
                .rollover
                .retry_after
                .map(|at| now >= at)
                .unwrap_or(true),
            MissionRolloverStatus::Active => self.rollover.retry_after.is_some_and(|at| now >= at),
            _ => false,
        }
    }

    fn ensure_rollover_mutable(&self) -> Result<()> {
        if self.is_terminal() {
            return Err(OcgError::config(format!(
                "Mission {} is terminal; rollover cannot change its generation or owner",
                self.mission_id
            )));
        }
        if self.rollover.generation != 0 && self.rollover.generation != self.generation {
            return Err(OcgError::config(
                "rollover state belongs to a different Mission generation",
            ));
        }
        Ok(())
    }

    /// Start the next generation after a terminal state: progress resets,
    /// history is retained, identity is unchanged.
    pub fn begin_new_generation(&mut self, now: i64) {
        if !self.is_terminal() {
            return;
        }
        self.generation = self.generation.saturating_add(1);
        self.status = MissionStatus::Active;
        self.phase = OrchestrationPhase::Idle;
        self.source = Role::Lead;
        self.destination = Some(Role::Lead);
        self.last_transition = None;
        self.attempts = Attempts::default();
        self.goal = None;
        self.constraints.clear();
        self.findings.clear();
        self.files.clear();
        self.symbols.clear();
        self.failures.clear();
        self.evidence.clear();
        self.debug_reason = None;
        self.last_verification = None;
        self.last_report = None;
        self.last_rich_bytes = 0;
        self.last_diff_context = String::new();
        self.checkpoints.clear();
        self.rollover = MissionRolloverState::default();
        self.reconcile = MissionReconcileState::default();
        let event = self.event(
            MissionEventKind::Admitted,
            "",
            None,
            self.session_id.clone(),
            None,
            now,
        );
        self.record(event);
        self.updated_at = now;
    }

    /// Durable completion. Replaying the same completion (same generation) is
    /// a no-op; completing after another terminal state fails explicitly.
    pub fn complete(&mut self, session_key: &str, note: Option<String>, now: i64) -> Result<bool> {
        self.transition_to_terminal(MissionStatus::Completed, session_key, note, now)
    }

    /// Durable failure, with an inspectable reason.
    pub fn fail(&mut self, session_key: &str, note: Option<String>, now: i64) -> Result<bool> {
        self.transition_to_terminal(MissionStatus::Failed, session_key, note, now)
    }

    /// Durable cancellation, with an inspectable reason.
    pub fn cancel(&mut self, session_key: &str, note: Option<String>, now: i64) -> Result<bool> {
        self.transition_to_terminal(MissionStatus::Cancelled, session_key, note, now)
    }

    fn transition_to_terminal(
        &mut self,
        status: MissionStatus,
        session_key: &str,
        note: Option<String>,
        now: i64,
    ) -> Result<bool> {
        debug_assert!(status.is_terminal());
        let kind = match status {
            MissionStatus::Completed => MissionEventKind::Completed,
            MissionStatus::Failed => MissionEventKind::Failed,
            MissionStatus::Cancelled => MissionEventKind::Cancelled,
            MissionStatus::Active => unreachable!("terminal transition from active"),
        };
        let event = self.event(kind, "", None, Some(session_key.to_string()), note, now);
        if self.history.iter().any(|existing| existing.id == event.id) {
            // Idempotent replay of the same terminal transition identity.
            return Ok(false);
        }
        if self.status.is_terminal() {
            return Err(OcgError::config(format!(
                "mission {} already reached a terminal state ({}) in generation {}; \
                 cannot transition to {} — re-admit the task to start a new generation",
                self.mission_id,
                self.status.as_str(),
                self.generation,
                status.as_str()
            )));
        }
        self.status = status;
        self.record(event);
        self.updated_at = now;
        Ok(true)
    }

    /// Sync the task-scoped durable fields from the session's live view.
    /// Called by the controller after every mutation, before persisting.
    ///
    /// A terminal generation is frozen: its record stays an inspectable
    /// snapshot of how the work ended. Only an explicit re-admission, which
    /// starts the next generation, can carry new progress.
    pub fn sync_from_session(&mut self, session: &SessionState) {
        debug_assert_eq!(session.task_id, self.mission_id);
        if self.is_terminal() {
            return;
        }
        let previous = self.clone();
        self.task = session.task.clone();
        self.goal = session.goal.clone();
        self.constraints = session.constraints.clone();
        self.phase = session.phase;
        self.source = session.source;
        self.destination = session.destination;
        self.last_transition = session.last_transition;
        self.attempts = session.attempts;
        self.findings = session.findings.clone();
        self.files = session.files.clone();
        self.symbols = session.symbols.clone();
        self.failures = session.failures.clone();
        self.evidence = session.evidence.clone();
        self.debug_reason = session.debug_reason.clone();
        self.last_verification = session.last_verification.clone();
        self.last_report = session.last_report.clone();
        self.last_rich_bytes = session.last_rich_bytes;
        self.last_diff_context = session.last_diff_context.clone();
        self.checkpoints = session.checkpoints.clone();
        let changed = previous.task != self.task
            || previous.goal != self.goal
            || previous.constraints != self.constraints
            || previous.phase != self.phase
            || previous.source != self.source
            || previous.destination != self.destination
            || previous.last_transition != self.last_transition
            || previous.attempts != self.attempts
            || previous.findings != self.findings
            || previous.files != self.files
            || previous.symbols != self.symbols
            || previous.failures != self.failures
            || previous.evidence != self.evidence
            || previous.debug_reason != self.debug_reason
            || previous.last_verification != self.last_verification
            || previous.last_report != self.last_report
            || previous.last_rich_bytes != self.last_rich_bytes
            || previous.last_diff_context != self.last_diff_context
            || previous.checkpoints != self.checkpoints;
        if changed {
            self.revision = self.revision.saturating_add(1);
            self.updated_at = session.updated_at;
        }
    }

    /// Seed a fresh session's live view from this Mission. The repository
    /// baseline stays out: it is session-scoped and derived from the indexed
    /// repository generation, so the caller carries or re-renders it.
    pub fn seed_session(&self, session_key: &str, now: i64) -> SessionState {
        let mut session = SessionState::new(session_key, &self.mission_id, now);
        session.task = self.task.clone();
        session.goal = self.goal.clone();
        session.constraints = self.constraints.clone();
        session.phase = self.phase;
        session.source = self.source;
        session.destination = self.destination;
        session.last_transition = self.last_transition;
        session.attempts = self.attempts;
        session.findings = self.findings.clone();
        session.files = self.files.clone();
        session.symbols = self.symbols.clone();
        session.failures = self.failures.clone();
        session.evidence = self.evidence.clone();
        session.debug_reason = self.debug_reason.clone();
        session.last_verification = self.last_verification.clone();
        session.last_report = self.last_report.clone();
        session.last_rich_bytes = self.last_rich_bytes;
        session.last_diff_context = self.last_diff_context.clone();
        session.checkpoints = self.checkpoints.clone();
        session
    }
}

fn redact_reason(reason: &str) -> String {
    crate::telemetry::task::redact(reason)
}

fn redact_reconcile_reason(reason: &str) -> String {
    bounded_reconcile_text(&crate::telemetry::task::redact(reason), 1024)
}

fn bounded_reconcile_text(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_string();
    }
    let mut end = max_bytes;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &value[..end])
}

/// A deterministic, safe event identity. Replays of the same logical
/// transition produce the same id and are deduplicated by [`Mission::record`].
fn event_id(mission_id: &str, kind: MissionEventKind, generation: u32, key: &str) -> String {
    let digest = crate::runtime::hash::sha256_hex(
        format!(
            "mission-v1|{mission_id}|{}|{generation}|{key}",
            kind.as_str()
        )
        .as_bytes(),
    );
    let short = digest.get(..16).unwrap_or(&digest);
    format!("ev-{}-{short}", kind.as_str().replace('_', "-"))
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

/// Read one Mission from the legacy projection, quarantining a corrupt record
/// and preserving its bytes. This is only consulted before the replay authority
/// is initialized.
fn load_projection(root: &Path, mission_id: &str) -> Result<Option<Mission>> {
    let path = mission_path(root, mission_id)?;
    if !path.is_file() {
        return Ok(None);
    }
    let text = fs::read_to_string(&path).map_err(|error| OcgError::read(&path, error))?;
    match serde_json::from_str::<Mission>(&text) {
        Ok(mission)
            if mission.schema_version == MISSION_SCHEMA_VERSION
                && mission.mission_id == mission_id =>
        {
            match validate_mission(&mission) {
                Ok(()) => Ok(Some(mission)),
                Err(error) => Err(quarantine(&path, mission_id, &error.to_string())),
            }
        }
        Ok(mission) if mission.mission_id != mission_id => Err(quarantine(
            &path,
            mission_id,
            &format!(
                "record identity {} does not match requested {mission_id}",
                mission.mission_id
            ),
        )),
        Ok(mission) => Err(quarantine(
            &path,
            mission_id,
            &format!(
                "unsupported schema_version {} (expected {MISSION_SCHEMA_VERSION})",
                mission.schema_version
            ),
        )),
        Err(error) => Err(quarantine(
            &path,
            mission_id,
            &format!("not valid JSON: {error}"),
        )),
    }
}

/// Load one Mission by id.
///
/// Once the replay authority is initialized it is the read source: a Mission
/// removed from the projection is still returned while it remains in the
/// authority. Only before first initialization is the legacy projection
/// consulted for migration bootstrap.
///
/// - `Ok(None)`: no Mission record exists (that is not corruption).
/// - `Ok(Some(_))`: the durable record, verified against the schema version.
/// - `Err(_)`: the authority is unreadable, or (before initialization) the
///   legacy record is corrupt or from an unsupported schema version. A corrupt
///   legacy record is quarantined to `<mission_id>.corrupt.json` (bytes
///   preserved) and must never be silently treated as "no Mission".
pub fn load(root: &Path, mission_id: &str) -> Result<Option<Mission>> {
    // Unsafe ids are rejected before any lookup, whether the authority or the
    // legacy projection answers the read.
    mission_path(root, mission_id)?;
    match crate::orchestration::replay::read_authoritative_snapshot(root)? {
        Some(snapshot) => Ok(snapshot.missions.get(mission_id).cloned()),
        None => load_projection(root, mission_id),
    }
}

/// Persist a Mission atomically. Ensures the state tree stays git-ignored.
///
/// Mission writes use a small sibling lock so independent bridge processes do
/// not interleave a read/rename sequence. The lock is an advisory local-write
/// boundary, not a distributed consensus mechanism; the revision/CAS helper
/// below is the final protection against overwriting newer progress.
pub fn save(root: &Path, mission: &Mission) -> Result<PathBuf> {
    validate_mission(mission)?;
    let _lock = MissionLock::acquire(root, &mission.mission_id)?;
    write_mission_unchecked(root, mission)
}

/// Compare-and-swap a Mission mutation at the durable boundary.
///
/// `expected_owner` may be supplied for a binding cutover. The current record
/// is read while the sibling lock is held; a missing, changed, or stale record
/// returns `Ok(false)` and never overwrites it. This is deliberately a local
/// Mission CAS, not a distributed lock or a general exactly-once facility.
pub fn save_if_revision(
    root: &Path,
    mission: &Mission,
    expected_revision: u64,
    expected_owner: Option<&str>,
) -> Result<bool> {
    validate_mission(mission)?;
    let _lock = MissionLock::acquire(root, &mission.mission_id)?;
    if !crate::orchestration::replay::SnapshotService::compare_and_append_mission(
        root,
        mission,
        expected_revision,
        expected_owner,
    )? {
        return Ok(false);
    }
    write_mission_projection(root, mission)?;
    Ok(true)
}

fn write_mission_unchecked(root: &Path, mission: &Mission) -> Result<PathBuf> {
    crate::runtime::install::ensure_gitignore(root)?;
    // The replay authority is committed before the compatibility projection.
    // If the projection write below fails, the authority already holds the
    // update and re-running this save is idempotent.
    crate::orchestration::replay::record_mission_update(root, mission)?;
    write_mission_projection(root, mission)
}

fn write_mission_projection(root: &Path, mission: &Mission) -> Result<PathBuf> {
    let path = mission_path(root, &mission.mission_id)?;
    let value = serde_json::to_value(mission).map_err(|error| {
        OcgError::config(format!(
            "cannot serialize mission {}: {error}",
            mission.mission_id
        ))
    })?;
    crate::runtime::install::write_json_atomic(&path, &value)?;
    Ok(path)
}

struct MissionLock {
    path: PathBuf,
}

impl MissionLock {
    fn acquire(root: &Path, mission_id: &str) -> Result<Self> {
        let path = mission_path(root, mission_id)?.with_extension("lock");
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                OcgError::io(
                    format!("cannot create Mission lock directory {}", parent.display()),
                    error,
                )
            })?;
        }
        for attempt in 0..200u32 {
            match OpenOptions::new().write(true).create_new(true).open(&path) {
                Ok(mut file) => {
                    let _ = writeln!(file, "{}", std::process::id());
                    return Ok(Self { path });
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    // A crashed writer must not make a Mission permanently
                    // unrecoverable. Only remove a clearly stale lock; a live
                    // short-lived write is retried instead.
                    let stale = fs::metadata(&path)
                        .and_then(|metadata| metadata.modified())
                        .ok()
                        .and_then(|modified| modified.elapsed().ok())
                        .is_some_and(|age| age > Duration::from_secs(30));
                    if stale {
                        let _ = fs::remove_file(&path);
                        continue;
                    }
                    if attempt < 199 {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                }
                Err(error) => {
                    return Err(OcgError::io(
                        format!("cannot acquire Mission lock {}", path.display()),
                        error,
                    ))
                }
            }
        }
        Err(OcgError::config(format!(
            "Mission {} is busy; retry the durable write",
            mission_id
        )))
    }
}

impl Drop for MissionLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

/// Find the durable Mission currently bound to `session_id` without relying on
/// the disposable session state file. This is the recovery seam after a state
/// file loss or a UI/client restart.
pub fn find_by_session(root: &Path, session_id: &str) -> Result<Option<Mission>> {
    if session_id.is_empty() {
        return Ok(None);
    }
    if let Some(snapshot) = crate::orchestration::replay::read_authoritative_snapshot(root)? {
        return Ok(snapshot
            .missions
            .into_values()
            .find(|mission| mission.session_id.as_deref() == Some(session_id)));
    }
    find_by_session_projection(root, session_id)
}

/// Resolve a runtime execution through verified ancestry to the Mission's
/// current root binding. The Mission store contains no runtime-specific parent
/// fields. A missing or terminal owner is not an authorization to dispatch.
pub fn owner_for_execution(
    root: &Path,
    adapter: &dyn RuntimeAdapter,
    execution_id: &RuntimeExecutionId,
) -> std::result::Result<(RuntimeExecutionLineage, Option<Mission>), RuntimeError> {
    let lineage = resolve_execution_lineage(adapter, execution_id)?;
    let mission = unique_active_owner(root, lineage.root_id.as_str()).map_err(|_| {
        RuntimeError::new(
            RuntimeErrorKind::ObservationFailed,
            "Mission ownership is unavailable",
        )
    })?;
    Ok((lineage, mission))
}

fn unique_active_owner(root: &Path, execution_id: &str) -> Result<Option<Mission>> {
    let missions =
        if let Some(snapshot) = crate::orchestration::replay::read_authoritative_snapshot(root)? {
            snapshot.missions.into_values().collect::<Vec<_>>()
        } else {
            let dir = missions_dir(root);
            match fs::read_dir(dir) {
                Ok(entries) => {
                    let mut missions = Vec::new();
                    for entry in entries {
                        let path = entry
                            .map_err(|error| OcgError::io("cannot read Mission entry", error))?
                            .path();
                        if path.extension().and_then(|value| value.to_str()) != Some("json") {
                            continue;
                        }
                        let name = path
                            .file_stem()
                            .and_then(|value| value.to_str())
                            .ok_or_else(|| OcgError::config("invalid Mission entry name"))?;
                        if !name.ends_with(".corrupt") {
                            if let Some(mission) = load_projection(root, name)? {
                                missions.push(mission);
                            }
                        }
                    }
                    missions
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
                Err(error) => return Err(OcgError::io("cannot read Mission directory", error)),
            }
        };
    let mut owners = missions.into_iter().filter(|mission| {
        !mission.is_terminal() && mission.session_id.as_deref() == Some(execution_id)
    });
    let owner = owners.next();
    if owners.next().is_some() {
        return Err(OcgError::config(
            "multiple active Missions claim the same execution",
        ));
    }
    Ok(owner)
}

fn find_by_session_projection(root: &Path, session_id: &str) -> Result<Option<Mission>> {
    let dir = missions_dir(root);
    let Ok(entries) = fs::read_dir(dir) else {
        return Ok(None);
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        let Some(name) = path.file_stem().and_then(|value| value.to_str()) else {
            continue;
        };
        if name.ends_with(".corrupt") {
            continue;
        }
        let Some(mission) = load_projection(root, name)? else {
            continue;
        };
        if mission.session_id.as_deref() == Some(session_id) {
            return Ok(Some(mission));
        }
    }
    Ok(None)
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

/// Quarantine a corrupt record (preserving its bytes) and build the explicit
/// error. The corrupt file is renamed aside, never deleted and never silently
/// replaced by an empty Mission.
fn quarantine(path: &Path, mission_id: &str, reason: &str) -> OcgError {
    let quarantined = path.with_extension("corrupt.json");
    // One quarantine slot per Mission: the newest corrupt record wins, so the
    // recoverable data preserved is always the most recent state.
    let _ = fs::remove_file(&quarantined);
    let note = match fs::rename(path, &quarantined) {
        Ok(()) => format!("the record was quarantined to {}", quarantined.display()),
        Err(_) => "the record could not be quarantined".to_string(),
    };
    OcgError::config(format!(
        "mission {mission_id} is corrupt ({reason}); {note}. \
         It was NOT silently reset — inspect the quarantined record to recover it"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::lifecycle::{RuntimeCapabilities, RuntimeIdentity, RuntimeResult};
    use serde_json::json;

    const TASK_ID: &str = "task-0123456789abcdef";

    fn admitted() -> Mission {
        Mission::admit(TASK_ID, "update the parser", "session-1", 7)
    }

    struct ParentAdapter;

    impl RuntimeAdapter for ParentAdapter {
        fn identity(&self) -> RuntimeIdentity {
            RuntimeIdentity::new("test", "test", "test")
        }

        fn capabilities(&self) -> RuntimeCapabilities {
            RuntimeCapabilities {
                execution_lineage: true,
                ..RuntimeCapabilities::NONE
            }
        }

        fn execution_parent(
            &self,
            id: &RuntimeExecutionId,
        ) -> RuntimeResult<Option<RuntimeExecutionId>> {
            match id.as_str() {
                "nested" => Ok(Some("worker".into())),
                "worker" => Ok(Some("session-1".into())),
                "session-1" | "session-2" => Ok(None),
                _ => Err(RuntimeError::new(
                    RuntimeErrorKind::ExecutionMissing,
                    "execution not found",
                )),
            }
        }
    }

    #[test]
    fn current_mission_owner_is_resolved_from_runtime_root_across_reopen_and_rebind() {
        let dir = tempfile::tempdir().unwrap();
        let mission = admitted();
        save(dir.path(), &mission).unwrap();
        for (id, depth) in [("session-1", 0), ("worker", 1), ("nested", 2)] {
            let (lineage, owner) =
                owner_for_execution(dir.path(), &ParentAdapter, &id.into()).unwrap();
            assert_eq!(lineage.root_id.as_str(), "session-1");
            assert_eq!(lineage.depth, depth);
            assert_eq!(owner.unwrap().mission_id, TASK_ID);
        }

        let mut loaded = load(dir.path(), TASK_ID).unwrap().unwrap();
        assert!(loaded.bind_runtime_execution(&"session-2".into(), 12));
        save(dir.path(), &loaded).unwrap();
        assert!(
            owner_for_execution(dir.path(), &ParentAdapter, &"worker".into())
                .unwrap()
                .1
                .is_none()
        );
        assert_eq!(
            owner_for_execution(dir.path(), &ParentAdapter, &"session-2".into())
                .unwrap()
                .1
                .unwrap()
                .mission_id,
            TASK_ID
        );
        loaded
            .cancel("session-2", Some("stopped".into()), 13)
            .unwrap();
        save(dir.path(), &loaded).unwrap();
        assert!(
            owner_for_execution(dir.path(), &ParentAdapter, &"session-2".into())
                .unwrap()
                .1
                .is_none()
        );
    }

    #[test]
    fn ambiguous_current_root_cannot_claim_a_mission() {
        let dir = tempfile::tempdir().unwrap();
        save(dir.path(), &admitted()).unwrap();
        save(
            dir.path(),
            &Mission::admit("task-abcdef0123456789", "another task", "session-1", 8),
        )
        .unwrap();
        let error = owner_for_execution(dir.path(), &ParentAdapter, &"worker".into()).unwrap_err();
        assert_eq!(error.kind(), RuntimeErrorKind::ObservationFailed);
    }

    #[test]
    fn mission_serializes_with_a_stable_versioned_shape() {
        let mission = admitted();
        let value = serde_json::to_value(&mission).unwrap();
        assert_eq!(value["schema_version"], json!(MISSION_SCHEMA_VERSION));
        assert_eq!(value["mission_id"], json!(TASK_ID));
        assert_eq!(value["generation"], json!(1));
        assert_eq!(value["status"], json!("active"));
        assert_eq!(value["phase"], json!("idle"));
        assert_eq!(value["session_id"], json!("session-1"));
        assert_eq!(value["task"], json!("update the parser"));
        assert!(value["history"].is_array());
        let parsed: Mission = serde_json::from_value(value).unwrap();
        assert_eq!(parsed, mission);
    }

    #[test]
    fn mission_round_trips_through_disk() {
        let dir = tempfile::tempdir().unwrap();
        let mut mission = admitted();
        mission.attempts.build = 2;
        mission.phase = OrchestrationPhase::Build;
        mission.checkpoints.push("cp-1".to_string());
        let path = save(dir.path(), &mission).unwrap();
        assert!(path.is_file());
        let loaded = load(dir.path(), TASK_ID).unwrap().unwrap();
        assert_eq!(loaded, mission);
        assert_eq!(loaded.attempts.build, 2);
        // A different mission id is a clean miss, not corruption.
        assert_eq!(load(dir.path(), "task-ffffffffffffffff").unwrap(), None);
    }

    #[test]
    fn unsafe_mission_ids_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        assert!(mission_path(dir.path(), "../escape").is_err());
        assert!(mission_path(dir.path(), "task/x").is_err());
        assert!(mission_path(dir.path(), TASK_ID).is_ok());
        assert!(load(dir.path(), "../escape").is_err());
    }

    #[test]
    fn corrupt_mission_is_quarantined_and_reported_never_silently_reset() {
        let dir = tempfile::tempdir().unwrap();
        let mission = admitted();
        // Write the legacy projection directly so the replay authority is not
        // initialized: this exercises the migration-bootstrap read path.
        let path = mission_path(dir.path(), TASK_ID).unwrap();
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, serde_json::to_vec(&mission).unwrap()).unwrap();
        fs::write(&path, "{ this is not json").unwrap();
        let error = load(dir.path(), TASK_ID).unwrap_err();
        let text = error.to_string();
        assert!(text.contains("corrupt"), "{text}");
        assert!(text.contains("quarantined"), "{text}");
        // The live path was moved aside; the corrupt bytes are preserved.
        assert!(!path.exists());
        let quarantined = path.with_extension("corrupt.json");
        assert_eq!(
            fs::read_to_string(&quarantined).unwrap(),
            "{ this is not json"
        );
        // After quarantine the id is a clean miss — but only because the
        // explicit error already surfaced and the data was preserved.
        assert_eq!(load(dir.path(), TASK_ID).unwrap(), None);
        // Listing skips the quarantined record.
        let (summaries, corrupt) = list(dir.path());
        assert!(summaries.is_empty());
        assert_eq!(corrupt, 0);
    }

    #[test]
    fn unknown_schema_version_is_quarantined() {
        let dir = tempfile::tempdir().unwrap();
        let path = mission_path(dir.path(), TASK_ID).unwrap();
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(
            &path,
            r#"{"schema_version":99,"mission_id":"task-0123456789abcdef"}"#,
        )
        .unwrap();
        let error = load(dir.path(), TASK_ID).unwrap_err();
        assert!(error.to_string().contains("schema_version 99"), "{error}");
        assert!(path.with_extension("corrupt.json").is_file());
    }

    #[test]
    fn terminal_states_are_typed_and_invalid_transitions_fail() {
        let mut mission = admitted();
        assert!(!mission.is_terminal());
        // Complete: active -> completed.
        assert!(mission.complete("session-1", None, 10).unwrap());
        assert_eq!(mission.status, MissionStatus::Completed);
        assert!(mission.is_terminal());
        // Replaying the same completion identity is a no-op, not an error.
        assert!(!mission.complete("session-1", None, 11).unwrap());
        // A different terminal transition is invalid and fails explicitly.
        let error = mission.fail("session-1", Some("too late".to_string()), 12);
        assert!(error.is_err());
        assert!(error
            .unwrap_err()
            .to_string()
            .contains("already reached a terminal state"));
        let error = mission.cancel("session-1", None, 13);
        assert!(error.is_err());

        // Cancel and fail behave the same way from active.
        let mut other = admitted();
        assert!(other.cancel("session-1", None, 10).unwrap());
        assert!(!other.cancel("session-1", None, 11).unwrap());
        assert!(other.complete("session-1", None, 12).is_err());
        let mut third = admitted();
        assert!(third.fail("session-1", None, 10).unwrap());
        assert!(!third.fail("session-1", None, 11).unwrap());
        assert!(third.cancel("session-1", None, 12).is_err());
    }

    #[test]
    fn terminal_states_persist_with_reasons() {
        let dir = tempfile::tempdir().unwrap();
        let mut failed = admitted();
        failed
            .fail("session-1", Some("abandoned by the user".to_string()), 10)
            .unwrap();
        save(dir.path(), &failed).unwrap();
        let loaded = load(dir.path(), TASK_ID).unwrap().unwrap();
        assert_eq!(loaded.status, MissionStatus::Failed);
        let event = loaded
            .history
            .iter()
            .find(|event| event.kind == MissionEventKind::Failed)
            .unwrap();
        assert_eq!(event.note.as_deref(), Some("abandoned by the user"));
        assert_eq!(event.session_id.as_deref(), Some("session-1"));
    }

    #[test]
    fn event_recording_is_idempotent_and_bounded() {
        let mut mission = admitted();
        let event = mission.event(
            MissionEventKind::BuildToVerify,
            "cp-build-to-verify-abc",
            Some("cp-build-to-verify-abc".to_string()),
            Some("session-1".to_string()),
            None,
            10,
        );
        assert!(event.id.starts_with("ev-build-to-verify-"));
        let before = mission.history.len();
        assert!(mission.record(event.clone()));
        // The exact same transition identity is a no-op.
        assert!(!mission.record(event));
        assert_eq!(mission.history.len(), before + 1);
        // Event ids are deterministic.
        let again = mission.event(
            MissionEventKind::BuildToVerify,
            "cp-build-to-verify-abc",
            None,
            None,
            None,
            11,
        );
        let first = mission
            .history
            .iter()
            .find(|event| event.kind == MissionEventKind::BuildToVerify)
            .unwrap();
        assert_eq!(again.id, first.id);
        // History is bounded.
        for index in 0..(MAX_MISSION_HISTORY + 8) {
            let event = mission.event(
                MissionEventKind::BuildToVerify,
                &format!("cp-{index}"),
                None,
                None,
                None,
                index as i64,
            );
            mission.record(event);
        }
        assert!(mission.history.len() <= MAX_MISSION_HISTORY);
    }

    #[test]
    fn new_generation_resets_progress_but_keeps_identity_and_history() {
        let mut mission = admitted();
        mission.attempts.build = 3;
        mission.phase = OrchestrationPhase::Done;
        mission
            .findings
            .push(crate::orchestration::handoff::HandoffFinding {
                summary: "a finding".to_string(),
                detail: None,
                source: None,
                severity: crate::orchestration::handoff::Severity::Info,
            });
        mission.checkpoints.push("cp-1".to_string());
        mission.complete("session-1", None, 10).unwrap();
        let history_before = mission.history.len();

        mission.begin_new_generation(20);
        assert_eq!(mission.mission_id, TASK_ID);
        assert_eq!(mission.generation, 2);
        assert_eq!(mission.status, MissionStatus::Active);
        assert_eq!(mission.phase, OrchestrationPhase::Idle);
        assert_eq!(mission.attempts, Attempts::default());
        assert!(mission.findings.is_empty());
        assert!(mission.checkpoints.is_empty());
        // The terminal event and the new admission are both in the history.
        assert!(mission.history.len() > history_before);
        assert!(mission
            .history
            .iter()
            .any(|event| event.kind == MissionEventKind::Completed && event.generation == 1));
        assert!(mission
            .history
            .iter()
            .any(|event| event.kind == MissionEventKind::Admitted && event.generation == 2));
        // A new generation can complete again: the identity differs.
        assert!(mission.complete("session-1", None, 30).unwrap());
    }

    #[test]
    fn session_binding_is_replaceable_and_recorded() {
        let mut mission = admitted();
        assert_eq!(mission.session_id.as_deref(), Some("session-1"));
        // Rebinding the same execution is a no-op.
        assert!(!mission.bind_runtime_execution(&RuntimeExecutionId::new("session-1"), 10));
        // Replacing the binding keeps identity and is recorded.
        assert!(mission.bind_runtime_execution(&RuntimeExecutionId::new("session-2"), 11));
        assert_eq!(mission.mission_id, TASK_ID);
        assert_eq!(mission.session_id.as_deref(), Some("session-2"));
        assert_ne!(
            mission.runtime_execution_id().unwrap().as_str(),
            mission.mission_id
        );
        assert!(mission.history.iter().any(|event| {
            event.kind == MissionEventKind::SessionBound
                && event.session_id.as_deref() == Some("session-2")
        }));
        // Releasing the binding clears it without touching identity.
        assert!(mission.release_session(12));
        assert_eq!(mission.session_id, None);
        assert!(!mission.release_session(13));
    }

    #[test]
    fn next_action_is_derived_from_durable_state() {
        let mut mission = admitted();
        assert_eq!(mission.next_action(1), NextAction::Lead);
        mission.phase = OrchestrationPhase::Explore;
        assert_eq!(mission.next_action(1), NextAction::Explore);
        mission.phase = OrchestrationPhase::Build;
        assert_eq!(mission.next_action(1), NextAction::Build);
        mission.phase = OrchestrationPhase::Verify;
        assert_eq!(mission.next_action(1), NextAction::Verify);
        mission.phase = OrchestrationPhase::Debug;
        mission.attempts.debug = 1;
        assert_eq!(mission.next_action(1), NextAction::Debug);
        mission.attempts.debug = 2;
        assert_eq!(mission.next_action(1), NextAction::Escalate);
        mission.complete("session-1", None, 10).unwrap();
        assert_eq!(mission.next_action(1), NextAction::Complete);
    }

    #[test]
    fn list_reports_live_missions_newest_first() {
        let dir = tempfile::tempdir().unwrap();
        let mut older = Mission::admit("task-aaaaaaaaaaaaaaaa", "one", "s", 1);
        older.updated_at = 1;
        let mut newer = Mission::admit("task-bbbbbbbbbbbbbbbb", "two", "s", 2);
        newer.updated_at = 2;
        // Write the legacy projections directly so the replay authority is not
        // initialized and the projection scan is what is under test.
        let missions = missions_dir(dir.path());
        fs::create_dir_all(&missions).unwrap();
        fs::write(
            missions.join("task-aaaaaaaaaaaaaaaa.json"),
            serde_json::to_vec(&older).unwrap(),
        )
        .unwrap();
        fs::write(
            missions.join("task-bbbbbbbbbbbbbbbb.json"),
            serde_json::to_vec(&newer).unwrap(),
        )
        .unwrap();
        // A corrupt sibling is counted, never listed, never quarantined here.
        fs::write(
            missions_dir(dir.path()).join("task-cccccccccccccccc.json"),
            "{bad",
        )
        .unwrap();
        let (summaries, corrupt) = list(dir.path());
        assert_eq!(corrupt, 1);
        assert_eq!(summaries.len(), 2);
        assert_eq!(summaries[0].mission_id, "task-bbbbbbbbbbbbbbbb");
        assert_eq!(summaries[1].mission_id, "task-aaaaaaaaaaaaaaaa");
        assert_eq!(summaries[0].status, MissionStatus::Active);
        // The corrupt file is untouched by listing.
        assert!(missions_dir(dir.path())
            .join("task-cccccccccccccccc.json")
            .is_file());
    }

    #[test]
    fn seed_and_sync_preserve_the_task_scoped_view() {
        let mut mission = admitted();
        mission.goal = Some("goal".to_string());
        mission.attempts.build = 2;
        mission.phase = OrchestrationPhase::Build;
        mission.checkpoints.push("cp-1".to_string());
        let session = mission.seed_session("session-9", 42);
        assert_eq!(session.session_id, "session-9");
        assert_eq!(session.task_id, TASK_ID);
        assert_eq!(session.attempts.build, 2);
        assert_eq!(session.phase, OrchestrationPhase::Build);
        assert_eq!(session.goal.as_deref(), Some("goal"));
        assert_eq!(session.checkpoints, vec!["cp-1".to_string()]);

        let mut synced = Mission::admit(TASK_ID, "", "session-9", 1);
        synced.sync_from_session(&session);
        assert_eq!(synced.goal.as_deref(), Some("goal"));
        assert_eq!(synced.attempts.build, 2);
        assert_eq!(synced.phase, OrchestrationPhase::Build);
        assert_eq!(synced.checkpoints, vec!["cp-1".to_string()]);
    }

    #[test]
    fn a_terminal_generation_is_frozen_against_later_session_activity() {
        let mut mission = admitted();
        mission.phase = OrchestrationPhase::Done;
        mission.attempts.build = 2;
        mission.checkpoints.push("cp-1".to_string());
        mission.complete("session-1", None, 10).unwrap();
        let frozen = mission.clone();

        // A session that keeps running after the Mission ended cannot rewrite
        // how the work ended.
        let mut session = mission.seed_session("session-2", 20);
        session.phase = OrchestrationPhase::Build;
        session.attempts.build = 9;
        mission.sync_from_session(&session);
        assert_eq!(mission, frozen);
        assert_eq!(mission.next_action(1), NextAction::Complete);
        assert_eq!(mission.next_action(1).as_str(), "complete");
    }
}
