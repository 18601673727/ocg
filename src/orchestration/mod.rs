//! Orchestration: typed role hand-offs, durable Mission state, a local
//! controller, and the generated OpenCode plugin adapter. Runtime execution
//! mechanics enter through the neutral `runtime::lifecycle` boundary; OpenCode
//! protocol details stay in the concrete adapter.
//!
//! This module owns durable, inspectable hand-offs between phases. It does not
//! own the conversation or the model loop — OpenCode does. The Rust controller
//! is the authority; the JavaScript adapter generated at launch is a thin
//! transport that only carries bytes to and from `ocg __bridge`.
//!
//! State lives under `.ocg/orchestration/` (ignored local state) and
//! checkpoints stay under `.ocg/checkpoints/`.
//!
//! Two different lifetimes share that directory:
//!
//! - **Sessions are disposable execution state** (`state.json`): bounded,
//!   evicted, keyed by the OpenCode session id, recoverable-to-empty on
//!   corruption.
//! - **Missions are durable product state** (`missions/<mission_id>.json`):
//!   the versioned record of one admitted task, independent of any session,
//!   strictly versioned and quarantined on corruption.

pub mod bridge;
pub mod budget;
pub mod canonical_control;
pub mod checkpoint;
pub mod config;
pub mod context_governor;
pub mod control;
pub mod controller;
pub mod dispatch;
pub mod handoff;
pub mod mission;
pub mod plugin;
pub mod policy;
pub mod projection;
pub mod reconcile;
pub mod replay;
pub mod rollover;
pub mod state;
pub mod substrate;

pub use budget::{
    admit as admit_spend, reservation_id, BudgetConfig, BudgetOrigin, BudgetStatus, CostBasis,
    MissionBudget, MissionBudgetReceipt, Money, QuotaFacts, QuotaState, Reservation,
    ReservationState, SpendAction, SpendAssessment, SpendBlock, SpendDecision, SpendRequest,
    MAX_RESERVATIONS,
};
pub use canonical_control::{
    CanonicalConfigurationResponse, CanonicalControlService, CanonicalDashboardResponse,
    CanonicalMissionResponse, CanonicalProjectResponse, CanonicalWorkEvent, CanonicalWorkSnapshot,
    GlobalConfiguration, ProjectConfiguration, ProjectConfigurationView, ProjectRecord,
    CANONICAL_CONTROL_API_VERSION,
};
pub use checkpoint::{Checkpoint, CheckpointSummary, LoadedCheckpoint, Phase, Staleness};
pub use config::OrchestrationConfig;
pub use context_governor::{
    ContextGovernorConfig, ContextObservation, GovernorAction, GovernorDecision, GovernorState,
    ModelMetadata, TelemetryProvenance, TokenUsage,
};
pub use control::{
    ApiIssue, ApprovalsView, AuthoritativeApprovalsView, AuthoritativeResourcesView, BudgetView,
    ControlError, ControlService, MissionListItem, MissionView, MissionsView, ReplaySlice,
    ResourcesView, SnapshotView, StateSummaryView, CONTROL_API_SCHEMA_VERSION,
    MAX_ERROR_MESSAGE_BYTES,
};
pub use controller::{
    BuildDecision, BuildOutcome, ContextGovernanceResult, Controller, ExploreDigest,
    HandoffOutcome, LeadContext,
};
pub use handoff::{
    HandoffFinding, HandoffVerification, ModelHandoffCapsule, ProjectionInput, Role, Severity,
    Transition,
};
pub use mission::{
    owner_for_execution, Mission, MissionEvent, MissionEventKind, MissionPolicyReceipt,
    MissionReconcileReceipt, MissionReconcileState, MissionReconcileStatus, MissionRolloverState,
    MissionRolloverStatus, MissionStatus, MissionSummary, NextAction, MISSION_SCHEMA_VERSION,
};
pub use policy::{
    approval_dir, approval_id, approval_path, ensure_pending, evaluate as evaluate_policy,
    list_approvals, load_approval, resolve_approval, save_approval, ApprovalIssue, ApprovalRecord,
    ApprovalRequest, ApprovalStatus, ApprovalView, FactProbe, FactStatus, LoadedApprovals,
    PolicyAction, PolicyAssessment, PolicyConfig, PolicyContext, PolicyDecision, PolicySummary,
    ResourceFacts, APPROVALS_DIR, APPROVAL_SCHEMA_VERSION, MAX_APPROVALS,
};
pub use reconcile::{
    latest_artifact, load_artifact, plan, MissionStoreIssue, ObservationStatus, ReconcileAction,
    ReconcileArtifact, ReconcileDecision, ReconcileInput, ReconcileOutcome, ReconcileResult,
    ReconcileRun, RuntimeObservation, RECONCILE_SCHEMA_VERSION,
};
pub use replay::{
    replay_dir, state_path, AuthoritativeSnapshot, Cursor, DomainEvent, EventEnvelope, ReplayAfter,
    SnapshotConfig, SnapshotService, DEFAULT_RETENTION, MAX_RETENTION, REPLAY_DIR, REPLAY_FILE,
    REPLAY_SCHEMA_VERSION,
};
pub use rollover::{
    ContinuationPacket, LeadBinding, RolloverArtifact, RolloverStatus, ROLLOVER_SCHEMA_VERSION,
};
pub use state::{
    Attempts, OrchestrationPhase, OrchestrationState, RepositoryBaseline, SessionState,
    STATE_SCHEMA_VERSION,
};
pub use substrate::{
    Dependency, Event as SubstrateEvent, MissionId, MissionState, Run as SubstrateRun, RunContract,
    RunId, RunState, SubstrateRepository, WorkNode as SubstrateWorkNode, WorkNodeId, WorkState,
};
