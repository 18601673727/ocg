//! Single-node desired-state reconciliation for durable Missions.
//!
//! The reconciler is intentionally a small convergence layer, not a scheduler
//! or a workflow engine. A Mission supplies durable desired/semantic state, a
//! [`RuntimeAdapter`](crate::runtime::lifecycle::RuntimeAdapter) supplies
//! execution mechanics and observations, and this module chooses at most one
//! next action. Planning is pure; execution and persistence are separate.

use crate::error::{OcgError, Result};
use crate::orchestration::budget::{self, MissionBudgetReceipt, SpendAction, SpendAssessment};
use crate::orchestration::context_governor::{ContextObservation, TelemetryProvenance};
use crate::orchestration::controller::Controller;
use crate::orchestration::mission::{
    self, Mission, MissionPolicyReceipt, MissionReconcileReceipt, MissionReconcileStatus,
    MissionRolloverStatus,
};
use crate::orchestration::policy::{
    self, approval_id, resource_identity_for_profile, ApprovalView, PolicyAction, PolicyAssessment,
    PolicyContext, PolicyDecision, PolicySummary, ResourceFacts,
};
use crate::orchestration::rollover::{self, ContinuationPacket, RolloverArtifact, RolloverStatus};
use crate::orchestration::state;
use crate::resources::ResourceId;
use crate::runtime::lifecycle::{
    RuntimeAdapter, RuntimeCapabilities, RuntimeContinuation, RuntimeError, RuntimeErrorKind,
    RuntimeExecution, RuntimeExecutionId, RuntimeProfile, RuntimeRecoveryKey, RuntimeResult,
};
use crate::telemetry::task::redact;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::fs;
use std::path::{Path, PathBuf};

/// Current schema for a generic execution-recovery artifact.
pub const RECONCILE_SCHEMA_VERSION: u32 = 1;
/// The neutral observation classification used by the planner and receipts.
///
/// In particular, `Missing` is not used for transport, authentication, or
/// malformed-response failures. An observation failure must never authorize a
/// replacement execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservationStatus {
    Exists,
    Missing,
    Unbound,
    ObservationFailed,
    RuntimeUnavailable,
    AuthenticationFailure,
    TransientTransportFailure,
    Unsupported,
}

impl ObservationStatus {
    pub fn is_failure(self) -> bool {
        matches!(
            self,
            Self::ObservationFailed
                | Self::RuntimeUnavailable
                | Self::AuthenticationFailure
                | Self::TransientTransportFailure
                | Self::Unsupported
        )
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Exists => "exists",
            Self::Missing => "missing",
            Self::Unbound => "unbound",
            Self::ObservationFailed => "observation_failed",
            Self::RuntimeUnavailable => "runtime_unavailable",
            Self::AuthenticationFailure => "authentication_failure",
            Self::TransientTransportFailure => "transient_transport_failure",
            Self::Unsupported => "unsupported",
        }
    }
}

/// A runtime observation without OpenCode transport or response types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeObservation {
    /// The exact execution named by the durable Mission binding exists.
    Exists { execution: RuntimeExecution },
    /// The runtime authoritatively reported that the exact execution is gone.
    Missing,
    /// The Mission has no current durable execution binding yet.
    Unbound,
    /// The adapter answered, but could not provide a trustworthy observation.
    ObservationFailed(RuntimeError),
    /// The runtime could not be reached or is not ready.
    RuntimeUnavailable(RuntimeError),
    /// The runtime rejected its credentials/configuration.
    AuthenticationFailure(RuntimeError),
    /// A transient transport failure prevented observation.
    TransientTransportFailure(RuntimeError),
    /// The adapter explicitly does not support the required observation.
    Unsupported(RuntimeError),
}

impl RuntimeObservation {
    pub fn status(&self) -> ObservationStatus {
        match self {
            Self::Exists { .. } => ObservationStatus::Exists,
            Self::Missing => ObservationStatus::Missing,
            Self::Unbound => ObservationStatus::Unbound,
            Self::ObservationFailed(_) => ObservationStatus::ObservationFailed,
            Self::RuntimeUnavailable(_) => ObservationStatus::RuntimeUnavailable,
            Self::AuthenticationFailure(_) => ObservationStatus::AuthenticationFailure,
            Self::TransientTransportFailure(_) => ObservationStatus::TransientTransportFailure,
            Self::Unsupported(_) => ObservationStatus::Unsupported,
        }
    }

    pub fn is_absence(&self) -> bool {
        matches!(self, Self::Missing | Self::Unbound)
    }

    pub fn is_failure(&self) -> bool {
        !matches!(self, Self::Exists { .. } | Self::Missing | Self::Unbound)
    }

    pub fn detail(&self) -> Option<&str> {
        match self {
            Self::Exists { .. } | Self::Missing | Self::Unbound => None,
            Self::ObservationFailed(error)
            | Self::RuntimeUnavailable(error)
            | Self::AuthenticationFailure(error)
            | Self::TransientTransportFailure(error)
            | Self::Unsupported(error) => Some(error.detail()),
        }
    }

    /// Convert an adapter result while preserving the typed error taxonomy.
    pub fn from_result(
        requested: &RuntimeExecutionId,
        result: RuntimeResult<RuntimeExecution>,
    ) -> Self {
        match result {
            Ok(execution) if execution.id == *requested => Self::Exists { execution },
            Ok(execution) => Self::ObservationFailed(RuntimeError::new(
                RuntimeErrorKind::InvalidResponse,
                format!(
                    "runtime reported execution {}, expected {}",
                    execution.id, requested
                ),
            )),
            Err(error) => Self::from_error(error),
        }
    }

    pub fn from_error(error: RuntimeError) -> Self {
        match error.kind() {
            RuntimeErrorKind::ExecutionMissing => Self::Missing,
            RuntimeErrorKind::Unavailable => Self::RuntimeUnavailable(error),
            RuntimeErrorKind::Authentication => Self::AuthenticationFailure(error),
            RuntimeErrorKind::Transport => Self::TransientTransportFailure(error),
            RuntimeErrorKind::Unsupported => Self::Unsupported(error),
            RuntimeErrorKind::InvalidResponse
            | RuntimeErrorKind::ObservationFailed
            | RuntimeErrorKind::ProfileSelection
            | RuntimeErrorKind::ProviderCompletion => Self::ObservationFailed(error),
        }
    }
}

/// The next control-plane action. These are lifecycle decisions, not generic
/// role/work-flow operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReconcileAction {
    Noop,
    Wait,
    RecoverRollover,
    RecoverExecution,
    EnsureExecution,
    ContinueExecution,
    Escalate,
    Blocked,
}

impl ReconcileAction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Noop => "noop",
            Self::Wait => "wait",
            Self::RecoverRollover => "recover_rollover",
            Self::RecoverExecution => "recover_execution",
            Self::EnsureExecution => "ensure_execution",
            Self::ContinueExecution => "continue_execution",
            Self::Escalate => "escalate",
            Self::Blocked => "blocked",
        }
    }
}

/// The result category retained in a bounded diagnostic receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReconcileOutcome {
    Applied,
    Deferred,
    Noop,
    Failed,
    /// Policy refused the action outright; no side effect was attempted.
    Denied,
    /// Policy requires an explicit approval before the action may proceed.
    AwaitingApproval,
}

impl ReconcileOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::Deferred => "deferred",
            Self::Noop => "noop",
            Self::Failed => "failed",
            Self::Denied => "denied",
            Self::AwaitingApproval => "awaiting_approval",
        }
    }
}

/// A deterministic planning decision. It contains no runtime response types
/// and can be inspected or tested without a process, socket or filesystem.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcileDecision {
    pub mission_id: String,
    pub generation: u32,
    pub current_execution_id: Option<RuntimeExecutionId>,
    pub observation: ObservationStatus,
    pub action: ReconcileAction,
    pub reason: String,
}

impl ReconcileDecision {
    fn new(
        mission: &Mission,
        observation: &RuntimeObservation,
        action: ReconcileAction,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            mission_id: mission.mission_id.clone(),
            generation: mission.generation,
            current_execution_id: mission.runtime_execution_id(),
            observation: observation.status(),
            action,
            reason: redact(&reason.into()),
        }
    }
}

/// Inputs to the pure planner. `rollover` is a durable artifact snapshot, not
/// a live runtime response.
pub struct ReconcileInput<'a> {
    pub mission: &'a Mission,
    pub observation: RuntimeObservation,
    pub rollover: Option<&'a RolloverArtifact>,
    pub capabilities: RuntimeCapabilities,
    pub max_debug_retries: usize,
    pub now: i64,
}

/// Derive the next action from durable state and one exact observation.
///
/// The function performs no I/O. In particular, it never calls
/// `resolve_execution`, consults an agent/model string, or converts an
/// observation failure into `Missing`.
pub fn plan(input: ReconcileInput<'_>) -> ReconcileDecision {
    let ReconcileInput {
        mission,
        observation,
        rollover,
        capabilities,
        max_debug_retries,
        now,
    } = input;

    // Terminal state is checked before every other concern. No stale runtime
    // object, rollover artifact, or report can resurrect it.
    if mission.is_terminal() {
        return ReconcileDecision::new(
            mission,
            &observation,
            ReconcileAction::Noop,
            "terminal Mission is permanently inert",
        );
    }

    // A durable rollover always wins over generic replacement. A conflict is
    // explicit and must not be converted into a new execution. An active target
    // can be recovered from its durable identity even when the old source is
    // gone; pre-cutover states wait when the current observation is uncertain.
    let rollover_status = mission.rollover.status;
    let artifact_status = rollover.map(|artifact| artifact.status);
    if rollover_status == MissionRolloverStatus::Conflict
        || artifact_status == Some(RolloverStatus::Conflict)
    {
        return ReconcileDecision::new(
            mission,
            &observation,
            ReconcileAction::Blocked,
            "durable rollover is conflicted; operator recovery is required",
        );
    }
    let rollover_incomplete = !matches!(
        rollover_status,
        MissionRolloverStatus::Idle | MissionRolloverStatus::Applied
    );
    if rollover_incomplete {
        if rollover_status == MissionRolloverStatus::Failed
            && mission
                .rollover
                .retry_after
                .is_some_and(|retry_after| now < retry_after)
        {
            return ReconcileDecision::new(
                mission,
                &observation,
                ReconcileAction::Wait,
                "rollover recovery is in its retry cooldown",
            );
        }
        let can_use_durable_target = matches!(
            rollover_status,
            MissionRolloverStatus::Active | MissionRolloverStatus::TargetReady
        );
        if observation.is_failure() && !can_use_durable_target {
            return ReconcileDecision::new(
                mission,
                &observation,
                ReconcileAction::Wait,
                "rollover recovery is waiting for an authoritative current execution observation",
            );
        }
        return ReconcileDecision::new(
            mission,
            &observation,
            ReconcileAction::RecoverRollover,
            "incomplete rollover recovery has priority over generic replacement",
        );
    }
    if let Some(retry_after) = mission.reconcile.observation_retry_after {
        if now < retry_after {
            let blocked = matches!(
                mission.reconcile.observation_status.as_deref(),
                Some("unsupported" | "authentication_failure")
            );
            return ReconcileDecision::new(
                mission,
                &observation,
                if blocked {
                    ReconcileAction::Blocked
                } else {
                    ReconcileAction::Wait
                },
                "runtime observation is in its bounded failure backoff",
            );
        }
    }

    // Once a recovery operation has been claimed, its durable phase—not a
    // second runtime lookup—drives the next safe step. A failed phase remains
    // on the same target and stable continuation identity.
    match mission.reconcile.status {
        MissionReconcileStatus::Conflict => {
            return ReconcileDecision::new(
                mission,
                &observation,
                ReconcileAction::Blocked,
                "reconcile ownership witness is conflicted; no automatic overwrite",
            );
        }
        MissionReconcileStatus::Blocked => {
            return ReconcileDecision::new(
                mission,
                &observation,
                ReconcileAction::Blocked,
                "reconcile is blocked until the runtime capability or operator condition changes",
            );
        }
        MissionReconcileStatus::Failed => {
            if mission
                .reconcile
                .retry_after
                .is_some_and(|retry_after| now < retry_after)
            {
                return ReconcileDecision::new(
                    mission,
                    &observation,
                    ReconcileAction::Wait,
                    "reconcile retry is in its failure cooldown",
                );
            }
            let action = if mission.reconcile.failed_phase == Some(MissionReconcileStatus::Creating)
                || mission.reconcile.target_execution_id.is_none()
            {
                ReconcileAction::RecoverExecution
            } else {
                ReconcileAction::ContinueExecution
            };
            return ReconcileDecision::new(
                mission,
                &observation,
                action,
                "retry the same durable recovery operation after its cooldown",
            );
        }
        MissionReconcileStatus::Creating => {
            let action = if mission.reconcile.target_execution_id.is_some() {
                ReconcileAction::ContinueExecution
            } else {
                ReconcileAction::RecoverExecution
            };
            let reason = if action == ReconcileAction::ContinueExecution {
                "the create side effect is recorded; continue the same target"
            } else {
                "an execution create intent is durable; inspect or recover that exact operation"
            };
            return ReconcileDecision::new(mission, &observation, action, reason);
        }
        MissionReconcileStatus::Created
        | MissionReconcileStatus::Bound
        | MissionReconcileStatus::Preparing
        | MissionReconcileStatus::Prepared
        | MissionReconcileStatus::Staging
        | MissionReconcileStatus::Staged
        | MissionReconcileStatus::Resuming => {
            return ReconcileDecision::new(
                mission,
                &observation,
                ReconcileAction::ContinueExecution,
                "continue the claimed same-generation recovery from its durable phase",
            );
        }
        MissionReconcileStatus::Applied => {
            if matches!(observation, RuntimeObservation::Exists { .. }) {
                return ReconcileDecision::new(
                    mission,
                    &observation,
                    ReconcileAction::Noop,
                    "current execution is healthy and recovery is already applied",
                );
            }
            // A later loss of the replacement is a new same-generation
            // recovery; it never advances the Mission generation.
        }
        MissionReconcileStatus::Idle => {}
    }

    match observation {
        RuntimeObservation::Unsupported(_) => ReconcileDecision::new(
            mission,
            &observation,
            ReconcileAction::Blocked,
            "runtime inspection capability is unsupported; no replacement is inferred",
        ),
        RuntimeObservation::AuthenticationFailure(_) => ReconcileDecision::new(
            mission,
            &observation,
            ReconcileAction::Blocked,
            "runtime authentication failed; OCG will not create a replacement",
        ),
        RuntimeObservation::RuntimeUnavailable(_)
        | RuntimeObservation::TransientTransportFailure(_)
        | RuntimeObservation::ObservationFailed(_) => ReconcileDecision::new(
            mission,
            &observation,
            ReconcileAction::Wait,
            "runtime observation failed; absence was not inferred",
        ),
        RuntimeObservation::Exists { .. } => {
            if mission.next_action(max_debug_retries)
                == crate::orchestration::mission::NextAction::Escalate
            {
                ReconcileDecision::new(
                    mission,
                    &observation,
                    ReconcileAction::Escalate,
                    "durable Mission next action requires operator escalation",
                )
            } else {
                ReconcileDecision::new(
                    mission,
                    &observation,
                    ReconcileAction::Noop,
                    "current durable execution exists; no recovery or identity churn is needed",
                )
            }
        }
        RuntimeObservation::Missing => {
            if capabilities.create_execution {
                ReconcileDecision::new(
                    mission,
                    &observation,
                    ReconcileAction::RecoverExecution,
                    "the current durable execution is authoritatively missing; recover it",
                )
            } else {
                ReconcileDecision::new(
                    mission,
                    &observation,
                    ReconcileAction::Blocked,
                    "runtime cannot create a replacement execution",
                )
            }
        }
        RuntimeObservation::Unbound => {
            if capabilities.create_execution {
                ReconcileDecision::new(
                    mission,
                    &observation,
                    ReconcileAction::EnsureExecution,
                    "Mission has no current durable execution; create and bind one",
                )
            } else {
                ReconcileDecision::new(
                    mission,
                    &observation,
                    ReconcileAction::Blocked,
                    "Mission has no current execution and runtime creation is unsupported",
                )
            }
        }
    }
}

/// A bounded, inspectable outcome of one Mission tick.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconcileResult {
    pub mission_id: String,
    pub generation: u32,
    pub current_execution_id: Option<RuntimeExecutionId>,
    pub observed: ObservationStatus,
    pub decision: ReconcileAction,
    pub reason: String,
    pub result: ReconcileOutcome,
    pub timestamp: i64,
    /// The bounded Policy projection for this pass, when Policy was evaluated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<PolicySummary>,
    /// The bounded mandatory economic projection for this pass, when the
    /// economic admission boundary was evaluated. Additive; old results
    /// deserialize as `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub budget: Option<MissionBudgetReceipt>,
}

impl ReconcileResult {
    fn new(
        mission: &Mission,
        decision: &ReconcileDecision,
        result: ReconcileOutcome,
        reason: impl Into<String>,
        timestamp: i64,
    ) -> Self {
        Self {
            mission_id: mission.mission_id.clone(),
            generation: mission.generation,
            current_execution_id: mission.runtime_execution_id(),
            observed: decision.observation,
            decision: decision.action,
            reason: redact(&reason.into()),
            result,
            timestamp,
            policy: None,
            budget: None,
        }
    }
}

/// A read-only issue discovered while enumerating the Mission store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissionStoreIssue {
    pub file: String,
    pub classification: String,
    pub detail: String,
}

/// The result of one bounded pass over the durable Mission store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconcileRun {
    pub results: Vec<ReconcileResult>,
    pub issues: Vec<MissionStoreIssue>,
}

impl ReconcileRun {
    pub fn has_failures(&self) -> bool {
        !self.issues.is_empty()
            || self.results.iter().any(|result| {
                matches!(
                    result.result,
                    ReconcileOutcome::Deferred
                        | ReconcileOutcome::Failed
                        | ReconcileOutcome::Denied
                        | ReconcileOutcome::AwaitingApproval
                )
            })
    }
}

/// Purely observational: publish this pass's neutral runtime observations as
/// resource health facts.
///
/// This never changes a reconcile decision, a receipt or the Mission store. It
/// only describes the resource the pass observed, so the existing Reconciler
/// behavior is unchanged. `Missing`/`Unbound` produce no health fact: the
/// absence of an execution is not evidence about the resource.
pub fn publish_resource_observations(
    run: &ReconcileRun,
    registry: &mut crate::resources::ResourceRegistry,
    identity: &crate::resources::ResourceIdentity,
    now: i64,
) {
    let mut best: Option<crate::resources::HealthFacts> = None;
    for result in &run.results {
        let Some(health) = health_for(result.observed, now) else {
            continue;
        };
        let take = best
            .as_ref()
            .map(|current| health.state.severity() > current.state.severity())
            .unwrap_or(true);
        if take {
            best = Some(health);
        }
    }
    if let Some(health) = best {
        registry.observe_health(identity, health, now);
    }
}

fn health_for(status: ObservationStatus, now: i64) -> Option<crate::resources::HealthFacts> {
    use crate::resources::{HealthFacts, ResourceHealth, ResourceProvenance};
    let state = match status {
        ObservationStatus::Exists => ResourceHealth::Available,
        ObservationStatus::Missing | ObservationStatus::Unbound => return None,
        ObservationStatus::RuntimeUnavailable | ObservationStatus::AuthenticationFailure => {
            ResourceHealth::Unavailable
        }
        ObservationStatus::ObservationFailed
        | ObservationStatus::TransientTransportFailure
        | ObservationStatus::Unsupported => ResourceHealth::Degraded,
    };
    Some(HealthFacts {
        state,
        reason: Some(format!("reconcile observation: {}", status.as_str())),
        provenance: ResourceProvenance::RuntimeObserved,
        observed_at: Some(now),
    })
}

/// A bounded generic recovery artifact. It contains the Mission-derived
/// continuation, not a transcript or an OpenCode response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ReconcileArtifact {
    pub schema_version: u32,
    pub artifact_id: String,
    pub mission_id: String,
    pub generation: u32,
    pub phase: MissionReconcileStatus,
    pub base_execution_id: Option<String>,
    pub target_execution_id: Option<String>,
    pub continuation_id: String,
    pub continuation: ContinuationPacket,
    pub profile: Option<RuntimeProfile>,
    pub created_at: i64,
    pub updated_at: i64,
}

impl Default for ReconcileArtifact {
    fn default() -> Self {
        Self {
            schema_version: RECONCILE_SCHEMA_VERSION,
            artifact_id: String::new(),
            mission_id: String::new(),
            generation: 1,
            phase: MissionReconcileStatus::Idle,
            base_execution_id: None,
            target_execution_id: None,
            continuation_id: String::new(),
            continuation: ContinuationPacket::default(),
            profile: None,
            created_at: 0,
            updated_at: 0,
        }
    }
}

impl ReconcileArtifact {
    pub fn new(
        mission: &Mission,
        operation_id: &str,
        profile: RuntimeProfile,
        now: i64,
        max_debug_retries: usize,
        max_bytes: usize,
    ) -> Result<Self> {
        let continuation = ContinuationPacket::from_mission(mission, max_debug_retries, max_bytes)?;
        let artifact = Self {
            schema_version: RECONCILE_SCHEMA_VERSION,
            artifact_id: operation_id.to_string(),
            mission_id: mission.mission_id.clone(),
            generation: mission.generation,
            phase: MissionReconcileStatus::Creating,
            base_execution_id: mission.runtime_execution_id().map(|id| id.to_string()),
            target_execution_id: None,
            continuation_id: continuation_id(operation_id),
            continuation,
            profile: Some(profile),
            created_at: now,
            updated_at: now,
        };
        validate_artifact(&artifact)?;
        Ok(artifact)
    }

    pub fn target_id(&self) -> Option<RuntimeExecutionId> {
        self.target_execution_id
            .as_deref()
            .map(RuntimeExecutionId::new)
    }

    pub fn recovery_key(&self) -> RuntimeRecoveryKey {
        RuntimeRecoveryKey::new(
            self.mission_id.clone(),
            self.generation,
            self.artifact_id.clone(),
        )
    }

    pub fn continuation_value(&self) -> RuntimeContinuation {
        RuntimeContinuation::new(
            self.continuation_id.clone(),
            self.continuation.render(),
            "OCG same-generation Mission recovery",
            json!({
                "marker": "OCG_RECONCILIATION",
                "mission_id": self.continuation.mission_id,
                "generation": self.continuation.generation,
                "operation_id": self.artifact_id,
                "digest": self.continuation.digest,
            }),
        )
    }
}

/// Directory containing generic reconcile artifacts.
pub fn reconcile_dir(root: &Path) -> PathBuf {
    state::state_dir(root).join("reconcile")
}

/// Path for one operation artifact.
pub fn artifact_path(root: &Path, operation_id: &str) -> Result<PathBuf> {
    if !crate::orchestration::checkpoint::is_safe_id(operation_id) {
        return Err(OcgError::config(format!(
            "unsafe reconcile operation id '{operation_id}'"
        )));
    }
    Ok(reconcile_dir(root).join(format!("{operation_id}.json")))
}

/// Load and validate a generic recovery artifact.
pub fn load_artifact(root: &Path, operation_id: &str) -> Result<Option<ReconcileArtifact>> {
    let path = artifact_path(root, operation_id)?;
    if !path.is_file() {
        return Ok(None);
    }
    let text = fs::read_to_string(&path).map_err(|error| OcgError::read(&path, error))?;
    let artifact: ReconcileArtifact = serde_json::from_str(&text).map_err(|error| {
        OcgError::config(format!(
            "reconcile artifact {operation_id} is corrupt: {error}"
        ))
    })?;
    if artifact.artifact_id != operation_id {
        return Err(OcgError::config(format!(
            "reconcile artifact {operation_id} contains mismatched identity {}",
            artifact.artifact_id
        )));
    }
    validate_artifact(&artifact)?;
    Ok(Some(artifact))
}

/// Persist a generic recovery artifact atomically.
pub fn save_artifact(root: &Path, artifact: &ReconcileArtifact) -> Result<PathBuf> {
    validate_artifact(artifact)?;
    crate::runtime::install::ensure_gitignore(root)?;
    let path = artifact_path(root, &artifact.artifact_id)?;
    let value = serde_json::to_value(artifact).map_err(|error| {
        OcgError::config(format!("cannot serialize reconcile artifact: {error}"))
    })?;
    crate::runtime::install::write_json_atomic(&path, &value)?;
    Ok(path)
}

fn validate_artifact(artifact: &ReconcileArtifact) -> Result<()> {
    if artifact.schema_version != RECONCILE_SCHEMA_VERSION {
        return Err(OcgError::config(format!(
            "reconcile artifact {} has unsupported schema_version {}",
            artifact.artifact_id, artifact.schema_version
        )));
    }
    if !crate::orchestration::checkpoint::is_safe_id(&artifact.artifact_id)
        || !crate::orchestration::checkpoint::is_safe_id(&artifact.mission_id)
        || artifact.generation == 0
    {
        return Err(OcgError::config(
            "reconcile artifact contains an unsafe identity or generation",
        ));
    }
    if artifact.continuation.mission_id != artifact.mission_id
        || artifact.continuation.generation != artifact.generation
        || artifact.continuation_id.is_empty()
        || artifact.continuation.digest.is_empty()
        || artifact.continuation.bytes == 0
    {
        return Err(OcgError::config(
            "reconcile continuation does not match its Mission identity",
        ));
    }
    if artifact.phase == MissionReconcileStatus::Idle {
        return Err(OcgError::config(
            "reconcile artifact cannot be in the idle phase",
        ));
    }
    if artifact
        .base_execution_id
        .as_deref()
        .zip(artifact.target_execution_id.as_deref())
        .is_some_and(|(base, target)| base == target)
    {
        return Err(OcgError::config(
            "reconcile target must differ from its base execution",
        ));
    }
    Ok(())
}

/// Find the newest generic artifact for a Mission.
///
/// The scan is a fallback only. A corrupt artifact must not poison the scan of
/// an unrelated Mission, so unreadable entries are skipped unless they name the
/// Mission being resolved; only that Mission's own corruption fails closed.
pub fn latest_artifact(root: &Path, mission_id: &str) -> Result<Option<ReconcileArtifact>> {
    let dir = reconcile_dir(root);
    let Ok(entries) = fs::read_dir(dir) else {
        return Ok(None);
    };
    let mut newest: Option<(i64, ReconcileArtifact)> = None;
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        let Some(name) = path.file_stem().and_then(|value| value.to_str()) else {
            continue;
        };
        match load_artifact(root, name) {
            Ok(Some(artifact)) => {
                if artifact.mission_id == mission_id
                    && newest
                        .as_ref()
                        .is_none_or(|(at, _)| artifact.updated_at >= *at)
                {
                    newest = Some((artifact.updated_at, artifact));
                }
            }
            Ok(None) => {}
            Err(error) => {
                if artifact_mission_id(root, name).as_deref() == Some(mission_id) {
                    return Err(error);
                }
            }
        }
    }
    Ok(newest.map(|(_, artifact)| artifact))
}

/// Best-effort Mission identity read for a corrupt artifact. It only decides
/// whether corruption belongs to the Mission being scanned; it never
/// authorizes a recovery.
fn artifact_mission_id(root: &Path, name: &str) -> Option<String> {
    let path = artifact_path(root, name).ok()?;
    let text = fs::read_to_string(path).ok()?;
    serde_json::from_str::<serde_json::Value>(&text)
        .ok()?
        .get("mission_id")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
}

/// A small adapter-independent service that can acquire a live V2 client when
/// the invocation already has one. It is deliberately only a constructor
/// convenience; no OpenCode type crosses the planner or Reconciler API.
pub struct Reconciler<'a, 'r> {
    controller: &'a Controller<'a>,
    runtime: Option<&'r mut (dyn RuntimeAdapter + 'r)>,
    profile: RuntimeProfile,
    /// The Policy projection for the action currently being dispatched. It is
    /// reset at the start of every `reconcile_mission` call so a stale summary
    /// can never leak into an unrelated Mission's receipt.
    policy_summary: Option<PolicySummary>,
    /// The mandatory economic projection for the action currently being
    /// dispatched. Reset with `policy_summary` for the same reason.
    budget_receipt: Option<MissionBudgetReceipt>,
}

impl<'a, 'r> Reconciler<'a, 'r> {
    pub fn new(
        controller: &'a Controller<'a>,
        runtime: &'r mut (dyn RuntimeAdapter + 'r),
        profile: RuntimeProfile,
    ) -> Self {
        Self {
            controller,
            runtime: Some(runtime),
            profile,
            policy_summary: None,
            budget_receipt: None,
        }
    }

    pub fn without_runtime(controller: &'a Controller<'a>, profile: RuntimeProfile) -> Self {
        Self {
            controller,
            runtime: None,
            profile,
            policy_summary: None,
            budget_receipt: None,
        }
    }

    pub fn profile(&self) -> &RuntimeProfile {
        &self.profile
    }

    /// Enumerate and reconcile each readable Mission once. Corrupt records are
    /// reported and do not prevent unrelated healthy records from being
    /// examined.
    pub fn reconcile_once(&mut self) -> ReconcileRun {
        let root = self.controller.root().to_path_buf();
        let mut issues = mission_store_issues(&root);
        issues.extend(artifact_store_issues(&root));
        issues.sort_by(|left, right| {
            left.file
                .cmp(&right.file)
                .then_with(|| left.classification.cmp(&right.classification))
        });
        let (summaries, authority_corrupt) = mission::list(&root);
        if authority_corrupt > 0
            && !issues
                .iter()
                .any(|issue| issue.classification == "replay_authority")
        {
            issues.push(MissionStoreIssue {
                file: crate::orchestration::replay::state_path(&root)
                    .display()
                    .to_string(),
                classification: "replay_authority".to_string(),
                detail: "the authoritative Mission snapshot is unreadable".to_string(),
            });
        }
        let mut results = Vec::new();
        for summary in summaries {
            results.push(self.reconcile_mission(&summary.mission_id));
        }
        ReconcileRun { results, issues }
    }

    /// Reconcile one Mission for at most one consequential action.
    pub fn reconcile_mission(&mut self, mission_id: &str) -> ReconcileResult {
        let now = self.controller.now_unix();
        // Reset the per-tick Policy projection first: a stale summary must never
        // leak into an unrelated Mission's receipt or result.
        self.policy_summary = None;
        self.budget_receipt = None;
        if !self.controller.config().enabled {
            return ReconcileResult {
                mission_id: mission_id.to_string(),
                generation: 0,
                current_execution_id: None,
                observed: ObservationStatus::Unbound,
                decision: ReconcileAction::Noop,
                reason: "orchestration reconciliation is disabled".to_string(),
                result: ReconcileOutcome::Noop,
                timestamp: now,
                policy: None,
                budget: None,
            };
        }
        let mission = match mission::load(self.controller.root(), mission_id) {
            Ok(Some(mission)) => mission,
            Ok(None) => {
                return ReconcileResult {
                    mission_id: mission_id.to_string(),
                    generation: 0,
                    current_execution_id: None,
                    observed: ObservationStatus::ObservationFailed,
                    decision: ReconcileAction::Blocked,
                    reason: "Mission record is missing".to_string(),
                    result: ReconcileOutcome::Failed,
                    timestamp: now,
                    policy: None,
                    budget: None,
                };
            }
            Err(error) => {
                return ReconcileResult {
                    mission_id: mission_id.to_string(),
                    generation: 0,
                    current_execution_id: None,
                    observed: ObservationStatus::ObservationFailed,
                    decision: ReconcileAction::Blocked,
                    reason: redact(&error.to_string()),
                    result: ReconcileOutcome::Failed,
                    timestamp: now,
                    policy: None,
                    budget: None,
                };
            }
        };

        if mission.is_terminal() {
            // Terminality is checked before runtime work. A stale runtime
            // object must not even be inspected, let alone repaired.
            let observation = RuntimeObservation::Missing;
            let decision = plan(ReconcileInput {
                mission: &mission,
                observation,
                rollover: None,
                capabilities: RuntimeCapabilities::NONE,
                max_debug_retries: self.controller.config().max_debug_retries,
                now,
            });
            return self.finish_noop(&mission, &decision, now);
        }

        let rollover_artifact =
            match rollover::latest_for_mission(self.controller.root(), &mission.mission_id) {
                Ok(artifact) => artifact,
                Err(error) => {
                    let observation = self.observe_current(&mission);
                    let decision = ReconcileDecision::new(
                        &mission,
                        &observation,
                        ReconcileAction::Blocked,
                        "durable rollover artifact is corrupt or unavailable",
                    );
                    return self.finish_failed(&mission, &decision, error.to_string(), now);
                }
            };
        let rollover_incomplete = !matches!(
            mission.rollover.status,
            MissionRolloverStatus::Idle | MissionRolloverStatus::Applied
        );
        // A durable, incomplete rollover is recovered from its own artifact
        // alone. The generic reconcile artifact is not loaded here, so a
        // corrupt or stale generic artifact cannot preempt nor block the
        // authoritative rollover recovery path.
        let artifact = if rollover_incomplete {
            None
        } else {
            match self.load_operation_artifact(&mission) {
                Ok(artifact) => artifact,
                Err(error) => {
                    let observation = self.observe_current(&mission);
                    let decision = ReconcileDecision::new(
                        &mission,
                        &observation,
                        ReconcileAction::Blocked,
                        "durable reconcile artifact is corrupt or unavailable",
                    );
                    return self.finish_failed(&mission, &decision, error.to_string(), now);
                }
            }
        };
        let observation_backoff = mission
            .reconcile
            .observation_retry_after
            .is_some_and(|retry_after| now < retry_after);
        if !rollover_incomplete
            && (observation_backoff
                || matches!(
                    mission.reconcile.status,
                    MissionReconcileStatus::Blocked | MissionReconcileStatus::Conflict
                ))
        {
            let observation = match mission.reconcile.observation_status.as_deref() {
                Some("unsupported") => RuntimeObservation::Unsupported(RuntimeError::new(
                    RuntimeErrorKind::Unsupported,
                    "runtime observation is unsupported",
                )),
                Some("authentication_failure") => {
                    RuntimeObservation::AuthenticationFailure(RuntimeError::new(
                        RuntimeErrorKind::Authentication,
                        "runtime authentication is unavailable",
                    ))
                }
                Some("runtime_unavailable") => RuntimeObservation::RuntimeUnavailable(
                    RuntimeError::new(RuntimeErrorKind::Unavailable, "runtime is unavailable"),
                ),
                Some("transient_transport_failure") => {
                    RuntimeObservation::TransientTransportFailure(RuntimeError::new(
                        RuntimeErrorKind::Transport,
                        "runtime transport is unavailable",
                    ))
                }
                Some(_) => RuntimeObservation::ObservationFailed(RuntimeError::new(
                    RuntimeErrorKind::ObservationFailed,
                    "runtime observation is in backoff",
                )),
                None => RuntimeObservation::Missing,
            };
            let decision = plan(ReconcileInput {
                mission: &mission,
                observation,
                rollover: None,
                capabilities: self.capabilities(),
                max_debug_retries: self.controller.config().max_debug_retries,
                now,
            });
            return self.finish_deferred(&mission, &decision, decision.reason.clone(), now);
        }
        let observation = self.observe_current(&mission);
        let decision = plan(ReconcileInput {
            mission: &mission,
            observation: observation.clone(),
            rollover: rollover_artifact.as_ref(),
            capabilities: self.capabilities(),
            max_debug_retries: self.controller.config().max_debug_retries,
            now,
        });

        // Policy admission boundary. Every consequential action passes through
        // an explicit, inspectable decision *before* its costly or destructive
        // side effect. A Deny, Defer or RequireApproval performs no runtime
        // work; only Allow reaches the action executor.
        if let Some(action) = PolicyAction::from_reconcile(decision.action) {
            if self.controller.policy().enabled {
                let assessment = self.evaluate_policy(&mission, &decision, action, now);
                self.policy_summary = Some(assessment.summary());
                match assessment.decision {
                    PolicyDecision::Allow => {}
                    PolicyDecision::Deny => {
                        return self.finish_policy(
                            &mission,
                            &decision,
                            ReconcileOutcome::Denied,
                            assessment.reason.clone(),
                            now,
                        );
                    }
                    PolicyDecision::Defer => {
                        return self.finish_policy(
                            &mission,
                            &decision,
                            ReconcileOutcome::Deferred,
                            assessment.reason.clone(),
                            now,
                        );
                    }
                    PolicyDecision::RequireApproval => {
                        // Record the pending approval idempotently. A failure to
                        // persist it is fail-closed: the action stays deferred.
                        if let Some(request) = assessment.approval.as_ref() {
                            if let Err(error) =
                                policy::ensure_pending(self.controller.root(), request)
                            {
                                return self.finish_policy(
                                    &mission,
                                    &decision,
                                    ReconcileOutcome::Deferred,
                                    format!(
                                        "the required approval could not be recorded: {}",
                                        redact(&error.to_string())
                                    ),
                                    now,
                                );
                            }
                        }
                        return self.finish_policy(
                            &mission,
                            &decision,
                            ReconcileOutcome::AwaitingApproval,
                            assessment.reason.clone(),
                            now,
                        );
                    }
                }
            }
        }

        let mut result = match decision.action {
            ReconcileAction::Noop => self.finish_noop(&mission, &decision, now),
            ReconcileAction::Wait | ReconcileAction::Escalate | ReconcileAction::Blocked => {
                self.finish_deferred(&mission, &decision, decision.reason.clone(), now)
            }
            ReconcileAction::RecoverRollover => {
                self.execute_rollover(&mission, rollover_artifact.as_ref(), &decision, now)
            }
            ReconcileAction::RecoverExecution | ReconcileAction::EnsureExecution => {
                self.execute_recovery(&mission, artifact, &decision, now)
            }
            ReconcileAction::ContinueExecution => {
                self.execute_continuation(&mission, artifact, &decision, now)
            }
        };
        // Surface the admission decision that let the action run. Non-policy
        // outcomes keep `None`.
        result.policy = self.policy_summary.clone();
        result.budget = self.budget_receipt.clone();
        result
    }

    fn capabilities(&self) -> RuntimeCapabilities {
        self.runtime
            .as_ref()
            .map(|runtime| runtime.capabilities())
            .unwrap_or(RuntimeCapabilities::NONE)
    }

    /// Evaluate Policy for one consequential action. Pure with respect to
    /// runtime state: it reads durable Mission/registry facts and an approval
    /// record, and performs no runtime side effect.
    fn evaluate_policy(
        &self,
        mission: &Mission,
        decision: &ReconcileDecision,
        action: PolicyAction,
        now: i64,
    ) -> PolicyAssessment {
        let current_execution_id = mission.runtime_execution_id();
        let approval = self.approval_view(mission, action, current_execution_id.as_ref());
        let context = PolicyContext {
            mission_id: mission.mission_id.clone(),
            generation: mission.generation,
            mission_status: mission.status,
            action,
            current_execution_id,
            observation: decision.observation,
            reconcile_status: mission.reconcile.status,
            capabilities: self.capabilities(),
            resource: self.associated_resource_facts(),
            approval,
            evaluated_at: now,
        };
        policy::evaluate(&context)
    }

    /// Resolve the pre-existing approval state for an action. Policy itself
    /// performs no I/O, so the caller resolves the record. A corrupt or
    /// identity-mismatched record is reported as `corrupt` (fail-closed), never
    /// silently treated as "no approval".
    fn approval_view(
        &self,
        mission: &Mission,
        action: PolicyAction,
        current_execution_id: Option<&RuntimeExecutionId>,
    ) -> ApprovalView {
        if !self.controller.policy().requires_approval(action) {
            return ApprovalView::not_required();
        }
        let id = approval_id(
            &mission.mission_id,
            mission.generation,
            action,
            current_execution_id,
        );
        match policy::load_approval(self.controller.root(), &id) {
            Ok(Some(record))
                if record.mission_id == mission.mission_id
                    && record.generation == mission.generation
                    && record.action == action.as_str() =>
            {
                ApprovalView {
                    required: true,
                    status: Some(record.status),
                    corrupt: false,
                }
            }
            Ok(Some(_)) => ApprovalView {
                required: true,
                status: None,
                corrupt: true,
            },
            Ok(None) => ApprovalView {
                required: true,
                status: None,
                corrupt: false,
            },
            Err(_) => ApprovalView {
                required: true,
                status: None,
                corrupt: true,
            },
        }
    }

    /// The registry facts for the *currently associated* resource.
    ///
    /// The identity is resolved from the runtime profile plus the attached
    /// adapter identity, then looked up directly. Policy never enumerates,
    /// ranks or substitutes: a missing record stays Unknown (which does not
    /// authorize anything) rather than selecting another resource.
    ///
    /// Registry read failure is fail-soft for the admission boundary. The
    /// registry is a descriptive substrate; a registry problem that is unrelated
    /// to a required fact must not deny a safe action. If the registry cannot be
    /// read, every fact is simply Unknown and the rules that require a fact
    /// treat it conservatively.
    fn associated_resource_facts(&self) -> ResourceFacts {
        let runtime_identity = self.runtime.as_ref().map(|runtime| runtime.identity());
        let identity = resource_identity_for_profile(&self.profile, runtime_identity.as_ref());
        let loaded = crate::resources::load(self.controller.root());
        match loaded.registry.resource(&ResourceId::derive(&identity)) {
            Some(record) => ResourceFacts::from_record(&record),
            None => ResourceFacts::unknown(identity),
        }
    }

    /// Mandatory economic admission for the provider-costly continuation
    /// resume. It records the bounded spend durably before the side effect and
    /// returns `None` when the action is not admissible, in which case the
    /// caller must not perform it. This runs independently of
    /// `policy.enabled` and cannot be satisfied by a generic approval.
    fn admit_continuation_spend(
        &mut self,
        mission: &Mission,
        operation_id: &str,
        now: i64,
    ) -> Result<SpendAssessment> {
        let runtime_identity = self.runtime.as_ref().map(|runtime| runtime.identity());
        let identity = resource_identity_for_profile(&self.profile, runtime_identity.as_ref());
        let quota = budget::quota_facts(self.controller.root(), &identity, now);
        let (assessment, receipt) = self.controller.admit_mandatory_spend(
            &mission.mission_id,
            SpendAction::ResumeContinuation,
            operation_id,
            quota,
            now,
        )?;
        self.budget_receipt = Some(receipt);
        Ok(assessment)
    }

    fn observe_current(&self, mission: &Mission) -> RuntimeObservation {
        let Some(runtime) = self.runtime.as_ref() else {
            return RuntimeObservation::RuntimeUnavailable(RuntimeError::new(
                RuntimeErrorKind::Unavailable,
                "no runtime adapter is attached to this reconcile invocation",
            ));
        };
        let Some(execution_id) = mission.runtime_execution_id() else {
            return RuntimeObservation::Unbound;
        };
        if !self
            .controller
            .is_current_execution_for(mission, &execution_id)
        {
            return RuntimeObservation::ObservationFailed(RuntimeError::new(
                RuntimeErrorKind::InvalidResponse,
                "runtime observation was not for the Mission's durable current execution",
            ));
        }
        RuntimeObservation::from_result(&execution_id, runtime.inspect_execution(&execution_id))
    }

    fn load_operation_artifact(&self, mission: &Mission) -> Result<Option<ReconcileArtifact>> {
        if let Some(operation_id) = mission.reconcile.operation_id.as_deref() {
            if let Some(artifact) = load_artifact(self.controller.root(), operation_id)? {
                if artifact.mission_id != mission.mission_id
                    || artifact.generation != mission.generation
                {
                    return Err(OcgError::config(
                        "reconcile artifact identity does not match the Mission",
                    ));
                }
                return Ok(Some(artifact));
            }
        }
        latest_artifact(self.controller.root(), &mission.mission_id)
    }

    fn finish_noop(
        &mut self,
        mission: &Mission,
        decision: &ReconcileDecision,
        now: i64,
    ) -> ReconcileResult {
        if decision.observation == ObservationStatus::Exists {
            self.clear_observation_backoff(mission, now);
        }
        ReconcileResult::new(
            mission,
            decision,
            ReconcileOutcome::Noop,
            decision.reason.clone(),
            now,
        )
    }

    fn finish_deferred(
        &mut self,
        mission: &Mission,
        decision: &ReconcileDecision,
        reason: String,
        now: i64,
    ) -> ReconcileResult {
        let result =
            ReconcileResult::new(mission, decision, ReconcileOutcome::Deferred, reason, now);
        if result.observed.is_failure() {
            self.persist_observation_backoff(mission, &result, now);
        }
        self.persist_receipt(mission, &result);
        result
    }

    fn finish_failed(
        &mut self,
        mission: &Mission,
        decision: &ReconcileDecision,
        reason: String,
        now: i64,
    ) -> ReconcileResult {
        let result = ReconcileResult::new(mission, decision, ReconcileOutcome::Failed, reason, now);
        self.persist_receipt(mission, &result);
        result
    }

    /// Finish a tick whose only outcome is a Policy decision that did not
    /// execute anything (`Denied`, `Deferred` or `AwaitingApproval`). The
    /// bounded assessment is attached and persisted so the refusal is
    /// inspectable and durable.
    fn finish_policy(
        &mut self,
        mission: &Mission,
        decision: &ReconcileDecision,
        outcome: ReconcileOutcome,
        reason: String,
        now: i64,
    ) -> ReconcileResult {
        let mut result = ReconcileResult::new(mission, decision, outcome, reason, now);
        result.policy = self.policy_summary.clone();
        self.persist_receipt(mission, &result);
        result
    }

    fn persist_observation_backoff(&self, mission: &Mission, result: &ReconcileResult, now: i64) {
        let retry_after = now.saturating_add(
            self.controller
                .config()
                .context_governor
                .retry_cooldown_seconds,
        );
        let Ok(Some(mut current)) = mission::load(self.controller.root(), &mission.mission_id)
        else {
            return;
        };
        if current.generation != result.generation
            || current.runtime_execution_id().as_ref() != result.current_execution_id.as_ref()
        {
            return;
        }
        let expected_revision = current.revision;
        let expected_owner = current.session_id.clone();
        if current.defer_observation(result.observed.as_str(), retry_after, now)
            && !mission::save_if_revision(
                self.controller.root(),
                &current,
                expected_revision,
                expected_owner.as_deref(),
            )
            .unwrap_or(false)
        {
            // Another local writer won; its revision remains authoritative.
        }
    }

    fn clear_observation_backoff(&self, mission: &Mission, now: i64) {
        let Ok(Some(mut current)) = mission::load(self.controller.root(), &mission.mission_id)
        else {
            return;
        };
        if current.generation != mission.generation
            || current.runtime_execution_id().as_ref() != mission.runtime_execution_id().as_ref()
        {
            return;
        }
        let expected_revision = current.revision;
        let expected_owner = current.session_id.clone();
        if current.clear_observation_defer(now)
            && !mission::save_if_revision(
                self.controller.root(),
                &current,
                expected_revision,
                expected_owner.as_deref(),
            )
            .unwrap_or(false)
        {
            // Another local writer won; its revision remains authoritative.
        }
    }

    fn persist_receipt(&self, mission: &Mission, result: &ReconcileResult) {
        if result.result == ReconcileOutcome::Noop {
            return;
        }
        let Ok(Some(mut current)) = mission::load(self.controller.root(), &mission.mission_id)
        else {
            return;
        };
        if current.generation != result.generation
            || current.runtime_execution_id().as_ref() != result.current_execution_id.as_ref()
        {
            return;
        }
        let expected_revision = current.revision;
        let expected_owner = current.session_id.clone();
        // Prefer the summary attached to the result; fall back to the per-tick
        // summary so an action that executed after an Allow still records the
        // admitting rule.
        let policy = result.policy.as_ref().or(self.policy_summary.as_ref());
        let receipt = MissionReconcileReceipt {
            generation: result.generation,
            current_execution_id: result
                .current_execution_id
                .as_ref()
                .map(|id| id.as_str().to_string()),
            observed: result.observed.as_str().to_string(),
            action: result.decision.as_str().to_string(),
            reason: result.reason.clone(),
            result: result.result.as_str().to_string(),
            timestamp: result.timestamp,
            policy: policy.map(|summary| MissionPolicyReceipt {
                decision: summary.decision.clone(),
                rule: summary.rule.clone(),
                reason_code: summary.reason_code.clone(),
                reason: summary.reason.clone(),
                action: summary.action.clone(),
                approval_id: summary.approval_id.clone(),
                required_facts: summary.required_facts.clone(),
                blocking: summary.blocking.clone(),
            }),
            // The freshest durable budget projection, so the mandatory economic
            // state is inspectable alongside the (optional) Policy decision.
            budget: result
                .budget
                .clone()
                .or_else(|| self.budget_receipt.clone())
                .or_else(|| Some(current.budget.receipt())),
        };
        if current.record_reconcile_receipt(receipt)
            && !mission::save_if_revision(
                self.controller.root(),
                &current,
                expected_revision,
                expected_owner.as_deref(),
            )
            .unwrap_or(false)
        {
            // A concurrent local writer won. Its state remains authoritative;
            // the result is still returned to the caller for diagnostics.
        }
    }

    fn execute_rollover(
        &mut self,
        mission: &Mission,
        artifact: Option<&RolloverArtifact>,
        decision: &ReconcileDecision,
        now: i64,
    ) -> ReconcileResult {
        let Some(runtime) = self.runtime.as_deref_mut() else {
            return self.finish_deferred(
                mission,
                decision,
                "runtime is unavailable; rollover recovery was not attempted".to_string(),
                now,
            );
        };
        let Some(current) = mission.runtime_execution_id() else {
            return self.finish_deferred(
                mission,
                decision,
                "rollover has no current durable execution binding".to_string(),
                now,
            );
        };
        let profile = artifact
            .and_then(RolloverArtifact::runtime_profile)
            .unwrap_or_else(|| self.profile.clone());
        let observation = ContextObservation {
            session_id: current.to_string(),
            event_id: format!(
                "ocg-reconcile-rollover-{}-{}",
                mission.mission_id, mission.reconcile.operation_count
            ),
            observed_at: now,
            safe_boundary: true,
            finish: Some("stop".to_string()),
            used_tokens: Some(1),
            limit_tokens: Some(1),
            usage_provenance: TelemetryProvenance::Exact,
            context_provenance: TelemetryProvenance::Exact,
            ..ContextObservation::default()
        };
        let result =
            self.controller
                .observe_context(current.as_str(), observation, runtime, &profile);
        match result {
            Ok(result) if result.rollover_status == Some(MissionRolloverStatus::Applied) => {
                let current_id = self
                    .mission(mission)
                    .and_then(|value| value.runtime_execution_id())
                    .unwrap_or(current);
                let mut result_mission = mission.clone();
                result_mission.session_id = Some(current_id.to_string());
                let outcome = ReconcileResult::new(
                    &result_mission,
                    decision,
                    ReconcileOutcome::Applied,
                    "existing rollover recovery completed",
                    now,
                );
                self.persist_receipt(mission, &outcome);
                outcome
            }
            Ok(result) => self.finish_deferred(
                mission,
                decision,
                format!(
                    "rollover recovery remains pending ({})",
                    result
                        .rollover_status
                        .map(|status| status.as_str())
                        .unwrap_or("unknown")
                ),
                now,
            ),
            Err(error) => self.finish_failed(mission, decision, error.to_string(), now),
        }
    }

    fn execute_recovery(
        &mut self,
        mission: &Mission,
        artifact: Option<ReconcileArtifact>,
        decision: &ReconcileDecision,
        now: i64,
    ) -> ReconcileResult {
        if self.runtime.is_none() {
            return self.finish_deferred(
                mission,
                decision,
                "runtime is unavailable; no replacement was created".to_string(),
                now,
            );
        }
        if !self.capabilities().create_execution {
            return self.finish_deferred(
                mission,
                decision,
                "runtime does not support execution creation".to_string(),
                now,
            );
        }

        // A completed recovery starts a fresh operation: reusing the applied
        // operation id would make `begin_reconcile` reject the claim and trap
        // the Mission in a permanent deferred loop after a later loss of its
        // replacement. An in-flight (creating/failed) operation is retried with
        // its stable identity instead.
        let operation_id =
            match mission.reconcile.status {
                MissionReconcileStatus::Idle | MissionReconcileStatus::Applied => {
                    operation_id(mission, mission.reconcile.operation_count + 1)
                }
                _ => mission.reconcile.operation_id.clone().unwrap_or_else(|| {
                    operation_id(mission, mission.reconcile.operation_count + 1)
                }),
            };
        let artifact = match artifact {
            Some(artifact) if artifact.artifact_id == operation_id => artifact,
            _ => match ReconcileArtifact::new(
                mission,
                &operation_id,
                self.profile.clone(),
                now,
                self.controller.config().max_debug_retries,
                self.controller
                    .config()
                    .context_governor
                    .max_continuation_bytes,
            ) {
                Ok(artifact) => artifact,
                Err(error) => return self.finish_failed(mission, decision, error.to_string(), now),
            },
        };
        if let Err(error) = save_artifact(self.controller.root(), &artifact) {
            return self.finish_failed(mission, decision, error.to_string(), now);
        }

        // A retry after a recorded create failure reopens the same operation,
        // not a new one. This preserves its stable continuation identity while
        // allowing an adapter-authoritative `None` recovery result to prove
        // that a fresh create is safe.
        if mission.reconcile.status == MissionReconcileStatus::Failed
            && artifact.target_id().is_none()
        {
            if let Err(error) = self.persist_phase(
                mission,
                MissionReconcileStatus::Failed,
                MissionReconcileStatus::Creating,
                None,
                now,
            ) {
                return self.finish_deferred(
                    mission,
                    decision,
                    format!("recovery retry could not be claimed: {error}"),
                    now,
                );
            }
        }

        // Claim the operation in the Mission before the first runtime side
        // effect. Another local tick will see Creating and must recover, not
        // create, after a restart.
        let mut claimed = mission.clone();
        let mut claimed_now = false;
        if claimed.reconcile.status == MissionReconcileStatus::Idle
            || claimed.reconcile.status == MissionReconcileStatus::Applied
        {
            claimed_now = true;
            if !claimed.begin_reconcile(&operation_id, mission.runtime_execution_id().as_ref(), now)
            {
                return self.finish_deferred(
                    mission,
                    decision,
                    "another local reconcile tick claimed the operation".to_string(),
                    now,
                );
            }
            claimed.reconcile.continuation_id = Some(artifact.continuation_id.clone());
            let expected_revision = mission.revision;
            let expected_owner = mission.session_id.clone();
            if !mission::save_if_revision(
                self.controller.root(),
                &claimed,
                expected_revision,
                expected_owner.as_deref(),
            )
            .unwrap_or(false)
            {
                return self.finish_deferred(
                    mission,
                    decision,
                    "Mission revision changed while claiming recovery; replan required".to_string(),
                    now,
                );
            }
        }

        let key = RuntimeRecoveryKey::new(
            mission.mission_id.clone(),
            mission.generation,
            operation_id.clone(),
        );
        let current = match self.mission(mission) {
            Some(current) => current,
            None => {
                return self.finish_failed(
                    mission,
                    decision,
                    "Mission disappeared while recovering execution".to_string(),
                    now,
                )
            }
        };
        let current_status = current.reconcile.status;
        let create_attempt = current.reconcile.create_attempt;
        let artifact_target = artifact.target_id();
        let recovered = if !claimed_now
            && current_status == MissionReconcileStatus::Creating
            && current.reconcile.operation_id.as_deref() == Some(operation_id.as_str())
            && artifact_target.is_none()
            && create_attempt > 0
        {
            // A previous local claim may have reached the runtime. Only an
            // adapter-authoritative recovery result may distinguish that from
            // a claim that died before the side effect.
            let recovery_result = self
                .runtime
                .as_deref_mut()
                .expect("runtime presence checked above")
                .recover_execution(&key);
            match recovery_result {
                Ok(Some(execution)) => Some(execution),
                Ok(None) => None,
                Err(error) if error.kind() == RuntimeErrorKind::Unsupported => {
                    return self.finish_deferred(
                        mission,
                        decision,
                        "interrupted create has no idempotent runtime recovery capability"
                            .to_string(),
                        now,
                    )
                }
                Err(error) => {
                    return self.finish_deferred(
                        mission,
                        decision,
                        format!("could not determine interrupted create outcome: {error}"),
                        now,
                    )
                }
            }
        } else {
            None
        };

        let target = if let Some(target) = artifact_target {
            target
        } else if let Some(execution) = recovered {
            execution.id
        } else {
            let create_claimed = match self.claim_create_attempt(mission, now) {
                Ok(claimed) => claimed,
                Err(error) => return self.finish_failed(mission, decision, error.to_string(), now),
            };
            if !create_claimed {
                return self.finish_deferred(
                    mission,
                    decision,
                    "another local reconcile tick owns the create attempt".to_string(),
                    now,
                );
            }
            match self
                .runtime
                .as_deref_mut()
                .expect("runtime presence checked above")
                .create_execution()
            {
                Ok(id) => id,
                Err(error) => {
                    let retry_after = Some(
                        now.saturating_add(
                            self.controller
                                .config()
                                .context_governor
                                .retry_cooldown_seconds,
                        ),
                    );
                    let _ = self.persist_reconcile_failure(
                        mission,
                        MissionReconcileStatus::Creating,
                        &error.to_string(),
                        retry_after,
                        now,
                    );
                    return self.finish_deferred(
                        mission,
                        decision,
                        format!("execution creation failed: {error}"),
                        now,
                    );
                }
            }
        };
        if target.as_str().is_empty()
            || mission
                .runtime_execution_id()
                .is_some_and(|current| current == target)
        {
            let error = "runtime returned an invalid replacement execution";
            let _ = self.persist_reconcile_failure(
                mission,
                MissionReconcileStatus::Creating,
                error,
                None,
                now,
            );
            return self.finish_failed(mission, decision, error.to_string(), now);
        }
        let mut updated_artifact = artifact;
        updated_artifact.target_execution_id = Some(target.to_string());
        updated_artifact.phase = MissionReconcileStatus::Created;
        updated_artifact.updated_at = now;
        if let Err(error) = save_artifact(self.controller.root(), &updated_artifact) {
            return self.finish_failed(mission, decision, error.to_string(), now);
        }
        if let Err(error) = self.persist_phase(
            mission,
            MissionReconcileStatus::Creating,
            MissionReconcileStatus::Created,
            Some(&target),
            now,
        ) {
            return self.finish_failed(mission, decision, error.to_string(), now);
        }
        let result = ReconcileResult::new(
            mission,
            decision,
            ReconcileOutcome::Applied,
            format!(
                "replacement execution {} was created and durably recorded",
                target
            ),
            now,
        );
        self.persist_receipt(mission, &result);
        result
    }

    fn execute_continuation(
        &mut self,
        mission: &Mission,
        artifact: Option<ReconcileArtifact>,
        decision: &ReconcileDecision,
        now: i64,
    ) -> ReconcileResult {
        if self.runtime.is_none() {
            return self.finish_deferred(
                mission,
                decision,
                "runtime is unavailable; recovery phase was not advanced".to_string(),
                now,
            );
        }
        let Some(mut artifact) = artifact else {
            return self.finish_failed(
                mission,
                decision,
                "durable recovery phase has no continuation artifact".to_string(),
                now,
            );
        };
        let Some(target) = artifact.target_id() else {
            return self.finish_failed(
                mission,
                decision,
                "durable recovery phase has no target execution".to_string(),
                now,
            );
        };
        if artifact.mission_id != mission.mission_id || artifact.generation != mission.generation {
            return self.finish_failed(
                mission,
                decision,
                "recovery artifact identity changed during the tick".to_string(),
                now,
            );
        }
        if mission.runtime_execution_id().as_ref() == Some(&target) {
            let observation = self.observe_current(mission);
            if !matches!(observation, RuntimeObservation::Exists { .. }) {
                let reason = format!(
                    "current recovery target {} is not authoritatively available: {}",
                    target,
                    observation.status().as_str()
                );
                let expected = mission.reconcile.status;
                let retry_after = Some(
                    now.saturating_add(
                        self.controller
                            .config()
                            .context_governor
                            .retry_cooldown_seconds,
                    ),
                );
                let _ =
                    self.persist_reconcile_failure(mission, expected, &reason, retry_after, now);
                return self.finish_deferred(mission, decision, reason, now);
            }
        }

        // Before binding, inspect the exact target. Never use a newest-session
        // resolver and never promote a child/worker execution.
        if mission.runtime_execution_id().as_ref() != Some(&target) {
            let observation = {
                let runtime = self
                    .runtime
                    .as_deref_mut()
                    .expect("runtime presence checked above");
                RuntimeObservation::from_result(&target, runtime.inspect_execution(&target))
            };
            if !matches!(observation, RuntimeObservation::Exists { .. }) {
                let reason = format!(
                    "recovery target {} is not authoritatively available: {}",
                    target,
                    observation.status().as_str()
                );
                let retry_after = Some(
                    now.saturating_add(
                        self.controller
                            .config()
                            .context_governor
                            .retry_cooldown_seconds,
                    ),
                );
                let _ = self.persist_reconcile_failure(
                    mission,
                    MissionReconcileStatus::Created,
                    &reason,
                    retry_after,
                    now,
                );
                return self.finish_deferred(mission, decision, reason, now);
            }
            return match self.bind_recovered_target(mission, &target, now) {
                Ok(()) => {
                    if let Err(error) = self.persist_artifact_phase(
                        &mut artifact,
                        MissionReconcileStatus::Bound,
                        Some(&target),
                        now,
                    ) {
                        return self.finish_failed(mission, decision, error.to_string(), now);
                    }
                    let mut result_mission = mission.clone();
                    result_mission.session_id = Some(target.to_string());
                    let value = ReconcileResult::new(
                        &result_mission,
                        decision,
                        ReconcileOutcome::Applied,
                        format!("recovery target {} became the current execution", target),
                        now,
                    );
                    self.persist_receipt(mission, &value);
                    value
                }
                Err(error) => self.finish_failed(mission, decision, error.to_string(), now),
            };
        }

        let phase = mission.reconcile.status;
        let profile = artifact
            .profile
            .clone()
            .unwrap_or_else(|| self.profile.clone());
        let continuation = artifact.continuation_value();

        // A local phase claim is itself the crash boundary before the next
        // runtime operation. If the process dies after the claim, the next
        // tick repeats only that same operation and stable continuation id.
        if phase == MissionReconcileStatus::Created {
            if let Err(error) = self.persist_phase(
                mission,
                MissionReconcileStatus::Created,
                MissionReconcileStatus::Bound,
                Some(&target),
                now,
            ) {
                return self.finish_deferred(
                    mission,
                    decision,
                    format!("recovery binding phase deferred: {error}"),
                    now,
                );
            }
            if let Err(error) = self.persist_artifact_phase(
                &mut artifact,
                MissionReconcileStatus::Bound,
                Some(&target),
                now,
            ) {
                return self.finish_failed(mission, decision, error.to_string(), now);
            }
            return self.finish_deferred(
                mission,
                decision,
                "recovery target binding phase is durable; continue on the next tick".to_string(),
                now,
            );
        }
        if phase == MissionReconcileStatus::Bound
            && self
                .claim_phase_with_artifact(
                    mission,
                    &mut artifact,
                    MissionReconcileStatus::Bound,
                    MissionReconcileStatus::Preparing,
                    &target,
                    now,
                )
                .is_err()
        {
            return self.finish_deferred(
                mission,
                decision,
                "Mission changed before profile preparation was claimed".to_string(),
                now,
            );
        }
        if phase == MissionReconcileStatus::Prepared
            && self
                .claim_phase_with_artifact(
                    mission,
                    &mut artifact,
                    MissionReconcileStatus::Prepared,
                    MissionReconcileStatus::Staging,
                    &target,
                    now,
                )
                .is_err()
        {
            return self.finish_deferred(
                mission,
                decision,
                "Mission changed before continuation staging was claimed".to_string(),
                now,
            );
        }
        if phase == MissionReconcileStatus::Staged
            && self
                .claim_phase_with_artifact(
                    mission,
                    &mut artifact,
                    MissionReconcileStatus::Staged,
                    MissionReconcileStatus::Resuming,
                    &target,
                    now,
                )
                .is_err()
        {
            return self.finish_deferred(
                mission,
                decision,
                "Mission changed before continuation resume was claimed".to_string(),
                now,
            );
        }

        let result = match phase {
            MissionReconcileStatus::Bound | MissionReconcileStatus::Preparing => {
                let runtime_result = self
                    .runtime
                    .as_deref_mut()
                    .expect("runtime presence checked above")
                    .prepare_execution(&target, &profile);
                match runtime_result {
                    Ok(execution) if execution.id == target => {
                        match self.persist_artifact_phase(
                            &mut artifact,
                            MissionReconcileStatus::Prepared,
                            Some(&target),
                            now,
                        ) {
                            Ok(()) => self.persist_phase(
                                mission,
                                MissionReconcileStatus::Preparing,
                                MissionReconcileStatus::Prepared,
                                Some(&target),
                                now,
                            ),
                            Err(error) => Err(error),
                        }
                    }
                    Ok(_) => Err(OcgError::config(
                        "runtime returned a different recovery execution after profile selection",
                    )),
                    Err(error) => Err(OcgError::config(error.to_string())),
                }
            }
            MissionReconcileStatus::Prepared | MissionReconcileStatus::Staging => {
                let runtime_result = self
                    .runtime
                    .as_deref_mut()
                    .expect("runtime presence checked above")
                    .stage_runtime_continuation(&target, &continuation);
                match runtime_result {
                    Ok(()) => match self.persist_artifact_phase(
                        &mut artifact,
                        MissionReconcileStatus::Staged,
                        Some(&target),
                        now,
                    ) {
                        Ok(()) => self
                            .persist_phase(
                                mission,
                                MissionReconcileStatus::Staging,
                                MissionReconcileStatus::Staged,
                                Some(&target),
                                now,
                            )
                            .map(|_| ()),
                        Err(error) => Err(error),
                    },
                    Err(error) => Err(OcgError::config(error.to_string())),
                }
            }
            MissionReconcileStatus::Staged | MissionReconcileStatus::Resuming => {
                // Mandatory economic admission before the one OCG-initiated
                // provider-costly side effect. A Deny/Defer performs no runtime
                // work; only Allow reaches the resume.
                let assessment =
                    match self.admit_continuation_spend(mission, &artifact.artifact_id, now) {
                        Ok(assessment) => assessment,
                        Err(error) => {
                            return self.finish_deferred(
                                mission,
                                decision,
                                format!(
                                    "economic admission could not be evaluated: {}",
                                    redact(&error.to_string())
                                ),
                                now,
                            );
                        }
                    };
                if !assessment.is_allowed() {
                    return self.finish_policy(
                        mission,
                        decision,
                        ReconcileOutcome::Deferred,
                        format!(
                            "economic admission deferred the provider-costly continuation: {}",
                            assessment.reason
                        ),
                        now,
                    );
                }
                let reservation_id = assessment.reservation_id.clone();
                let runtime_result = self
                    .runtime
                    .as_deref_mut()
                    .expect("runtime presence checked above")
                    .resume_runtime_continuation(&target, &continuation);
                match runtime_result {
                    Ok(()) => {
                        // Settle the bounded spend exactly once on a proven
                        // dispatch. No provider-reported actual is available, so
                        // the bounded reservation amount is settled.
                        if let Some(id) = reservation_id.as_deref() {
                            let _ = self.controller.settle_mandatory_spend(
                                &mission.mission_id,
                                id,
                                None,
                                now,
                            );
                        }
                        match self.persist_artifact_phase(
                            &mut artifact,
                            MissionReconcileStatus::Applied,
                            Some(&target),
                            now,
                        ) {
                            Ok(()) => self
                                .persist_phase(
                                    mission,
                                    MissionReconcileStatus::Resuming,
                                    MissionReconcileStatus::Applied,
                                    Some(&target),
                                    now,
                                )
                                .map(|_| ()),
                            Err(error) => Err(error),
                        }
                    }
                    Err(error) => {
                        // The dispatch outcome is uncertain: retain the
                        // reservation rather than optimistically releasing it.
                        if let Some(id) = reservation_id.as_deref() {
                            let _ = self.controller.mark_mandatory_spend_unresolved(
                                &mission.mission_id,
                                id,
                                now,
                            );
                        }
                        Err(OcgError::config(error.to_string()))
                    }
                }
            }
            MissionReconcileStatus::Applied => Ok(()),
            MissionReconcileStatus::Created => Err(OcgError::config(
                "recovery target has not been durably bound",
            )),
            MissionReconcileStatus::Creating
            | MissionReconcileStatus::Failed
            | MissionReconcileStatus::Blocked
            | MissionReconcileStatus::Conflict
            | MissionReconcileStatus::Idle => Err(OcgError::config(
                "recovery phase cannot continue without a claimed target",
            )),
        };
        match result {
            Ok(()) => {
                let mut result_mission = mission.clone();
                result_mission.session_id = Some(target.to_string());
                let value = ReconcileResult::new(
                    &result_mission,
                    decision,
                    ReconcileOutcome::Applied,
                    format!("recovery phase advanced for {}", target),
                    now,
                );
                self.persist_receipt(mission, &value);
                value
            }
            Err(error) => {
                let expected = match phase {
                    MissionReconcileStatus::Bound | MissionReconcileStatus::Preparing => {
                        MissionReconcileStatus::Preparing
                    }
                    MissionReconcileStatus::Prepared | MissionReconcileStatus::Staging => {
                        MissionReconcileStatus::Staging
                    }
                    MissionReconcileStatus::Staged | MissionReconcileStatus::Resuming => {
                        MissionReconcileStatus::Resuming
                    }
                    _ => phase,
                };
                let retry_after = Some(
                    now.saturating_add(
                        self.controller
                            .config()
                            .context_governor
                            .retry_cooldown_seconds,
                    ),
                );
                let _ = self.persist_reconcile_failure(
                    mission,
                    expected,
                    &error.to_string(),
                    retry_after,
                    now,
                );
                self.finish_deferred(
                    mission,
                    decision,
                    format!("recovery phase deferred: {error}"),
                    now,
                )
            }
        }
    }

    fn bind_recovered_target(
        &self,
        mission: &Mission,
        target: &RuntimeExecutionId,
        now: i64,
    ) -> Result<()> {
        let mut current = self
            .mission(mission)
            .ok_or_else(|| OcgError::config("Mission disappeared before recovery binding"))?;
        if current.generation != mission.generation {
            return Err(OcgError::config(
                "Mission generation changed before recovery binding",
            ));
        }
        if current.reconcile.operation_id != mission.reconcile.operation_id {
            return Err(OcgError::config(
                "Mission reconcile operation changed before recovery binding",
            ));
        }
        if current.runtime_execution_id().as_ref() == Some(target) {
            return self.persist_phase(
                mission,
                MissionReconcileStatus::Created,
                MissionReconcileStatus::Bound,
                Some(target),
                now,
            );
        }
        let expected_revision = current.revision;
        let expected_owner = current.session_id.clone();
        current.bind_runtime_execution(target, now);
        current.transition_reconcile(
            MissionReconcileStatus::Created,
            MissionReconcileStatus::Bound,
            Some(target),
            now,
        );
        if !mission::save_if_revision(
            self.controller.root(),
            &current,
            expected_revision,
            expected_owner.as_deref(),
        )? {
            return Err(OcgError::config(
                "Mission revision or owner changed during recovery binding",
            ));
        }
        let mut state_after = state::load(self.controller.root());
        state_after
            .state
            .upsert(current.seed_session(target.as_str(), now), now);
        let _ = state::save(self.controller.root(), &state_after.state);
        Ok(())
    }

    fn claim_phase_with_artifact(
        &self,
        mission: &Mission,
        artifact: &mut ReconcileArtifact,
        expected: MissionReconcileStatus,
        next: MissionReconcileStatus,
        target: &RuntimeExecutionId,
        now: i64,
    ) -> Result<()> {
        self.persist_artifact_phase(artifact, next, Some(target), now)?;
        self.persist_phase(mission, expected, next, Some(target), now)
    }

    fn claim_create_attempt(&self, mission: &Mission, now: i64) -> Result<bool> {
        let mut current = self
            .mission(mission)
            .ok_or_else(|| OcgError::config("Mission disappeared while claiming create"))?;
        if current.generation != mission.generation {
            return Err(OcgError::config(
                "Mission generation changed while claiming create",
            ));
        }
        if mission.reconcile.operation_id.is_some()
            && current.reconcile.operation_id != mission.reconcile.operation_id
        {
            return Err(OcgError::config(
                "Mission reconcile operation changed while claiming create",
            ));
        }
        let expected_revision = current.revision;
        let expected_owner = current.session_id.clone();
        if !current.claim_reconcile_create(now) {
            return Ok(false);
        }
        if !mission::save_if_revision(
            self.controller.root(),
            &current,
            expected_revision,
            expected_owner.as_deref(),
        )? {
            return Ok(false);
        }
        Ok(true)
    }

    fn persist_artifact_phase(
        &self,
        artifact: &mut ReconcileArtifact,
        phase: MissionReconcileStatus,
        target: Option<&RuntimeExecutionId>,
        now: i64,
    ) -> Result<()> {
        artifact.phase = phase;
        if let Some(target) = target {
            artifact.target_execution_id = Some(target.to_string());
        }
        artifact.updated_at = now;
        save_artifact(self.controller.root(), artifact).map(|_| ())
    }

    fn persist_phase(
        &self,
        mission: &Mission,
        expected: MissionReconcileStatus,
        next: MissionReconcileStatus,
        target: Option<&RuntimeExecutionId>,
        now: i64,
    ) -> Result<()> {
        let mut current = self
            .mission(mission)
            .ok_or_else(|| OcgError::config("Mission disappeared during reconcile recovery"))?;
        if current.generation != mission.generation {
            return Err(OcgError::config(
                "Mission generation changed during reconcile recovery",
            ));
        }
        if mission.reconcile.operation_id.is_some()
            && current.reconcile.operation_id != mission.reconcile.operation_id
        {
            return Err(OcgError::config(
                "Mission reconcile operation changed during recovery",
            ));
        }
        let expected_revision = current.revision;
        let expected_owner = current.session_id.clone();
        if !current.transition_reconcile(expected, next, target, now) {
            // A same-state replay is already converged; a different state is
            // a conflict and must be replanned by the caller.
            if current.reconcile.status == next {
                return Ok(());
            }
            return Err(OcgError::config(
                "Mission reconcile phase changed before the local CAS",
            ));
        }
        if !mission::save_if_revision(
            self.controller.root(),
            &current,
            expected_revision,
            expected_owner.as_deref(),
        )? {
            return Err(OcgError::config(
                "Mission revision changed during reconcile recovery",
            ));
        }
        Ok(())
    }

    fn persist_reconcile_failure(
        &self,
        mission: &Mission,
        expected: MissionReconcileStatus,
        error: &str,
        retry_after: Option<i64>,
        now: i64,
    ) -> Result<()> {
        let mut current = self.mission(mission).ok_or_else(|| {
            OcgError::config("Mission disappeared while recording reconcile failure")
        })?;
        if current.generation != mission.generation {
            return Err(OcgError::config(
                "Mission generation changed while recording reconcile failure",
            ));
        }
        if mission.reconcile.operation_id.is_some()
            && current.reconcile.operation_id != mission.reconcile.operation_id
        {
            return Err(OcgError::config(
                "Mission reconcile operation changed while recording failure",
            ));
        }
        let expected_revision = current.revision;
        let expected_owner = current.session_id.clone();
        if !current.fail_reconcile(expected, expected, error, retry_after, now) {
            return Ok(());
        }
        if !mission::save_if_revision(
            self.controller.root(),
            &current,
            expected_revision,
            expected_owner.as_deref(),
        )? {
            return Err(OcgError::config(
                "Mission revision changed while recording reconcile failure",
            ));
        }
        Ok(())
    }

    fn mission(&self, expected: &Mission) -> Option<Mission> {
        mission::load(self.controller.root(), &expected.mission_id)
            .ok()
            .flatten()
            .filter(|mission| mission.generation == expected.generation)
    }
}

fn operation_id(mission: &Mission, attempt: u32) -> String {
    let digest = crate::runtime::hash::sha256_hex(
        format!(
            "ocg-reconcile-v1|{}|{}|{}",
            mission.mission_id, mission.generation, attempt
        )
        .as_bytes(),
    );
    format!("rec-{}", digest.get(..16).unwrap_or(&digest))
}

fn continuation_id(operation_id: &str) -> String {
    format!("msg_ocg_reconcile_{}", operation_id.replace('-', "_"))
}

fn mission_store_issues(root: &Path) -> Vec<MissionStoreIssue> {
    let mut issues = Vec::new();
    let dir = mission::missions_dir(root);
    let Ok(entries) = fs::read_dir(dir) else {
        return issues;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        if name.ends_with(".corrupt.json") {
            issues.push(MissionStoreIssue {
                file: name.to_string(),
                classification: "quarantined".to_string(),
                detail: "a corrupt Mission was preserved for inspection".to_string(),
            });
            continue;
        }
        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        let Some(mission_id) = name.strip_suffix(".json") else {
            continue;
        };
        if let Err(error) = mission::load(root, mission_id) {
            issues.push(MissionStoreIssue {
                file: name.to_string(),
                classification: "corrupt".to_string(),
                detail: redact(&error.to_string()),
            });
        }
    }
    issues.sort_by(|left, right| {
        left.file
            .cmp(&right.file)
            .then_with(|| left.classification.cmp(&right.classification))
    });
    issues
}

fn artifact_store_issues(root: &Path) -> Vec<MissionStoreIssue> {
    let mut issues = Vec::new();
    let dir = reconcile_dir(root);
    let Ok(entries) = fs::read_dir(dir) else {
        return issues;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        let Some(stem) = path.file_stem().and_then(|value| value.to_str()) else {
            continue;
        };
        if let Err(error) = load_artifact(root, stem) {
            issues.push(MissionStoreIssue {
                file: name.to_string(),
                classification: "corrupt_artifact".to_string(),
                detail: redact(&error.to_string()),
            });
        }
    }
    issues
}
