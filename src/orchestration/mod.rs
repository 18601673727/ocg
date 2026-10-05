//! Canonical Project, Job, Attempt, Executor, Call and DispatchIntent execution
//! state, plus independent runtime context and configuration facilities.
//!
//! Context hand-offs and checkpoints remain independent of execution identity.
pub mod budget;
pub mod call_schema;
pub mod canonical_control;
pub mod checkpoint;
pub mod config;
pub mod context_governor;
pub mod domain;
pub mod execution_dispatch;
pub mod execution_runtime;
pub mod handoff;
pub mod journal;
pub(crate) mod placement;
pub mod projection;
pub mod state;

pub use budget::{
    admit as admit_spend, conflict_settlement_id, reservation_id, settlement_id,
    settlement_payload_digest, BillableUsage, BudgetConfig, BudgetOrigin, BudgetStatus, CostBasis,
    ProjectBudget, ProjectBudgetReceipt, Money, PriceOutcome, PriceRefusal, PricingBasis,
    QuotaFacts, QuotaState, Reservation, ReservationState, Settlement, SettlementDisposition,
    SettlementEffect, SettlementVariance, SpendAction, SpendAssessment, SpendBlock, SpendDecision,
    SpendRequest, TokenPrice, UsageRecord, UsageSource, MAX_RATE_MICROS_PER_MILLION,
    MAX_RESERVATIONS, MAX_SETTLEMENTS, PRICE_WILDCARD, TOKENS_PER_PRICE_UNIT,
};
pub use canonical_control::{
    CanonicalConfigurationResponse, CanonicalControlService, CanonicalDashboardResponse,
    CanonicalEventTail, CanonicalJobConfigResponse, CanonicalJobEvent, CanonicalJobSnapshot,
    CanonicalProjectResponse, GlobalConfiguration, ProjectConfiguration, ProjectConfigurationView,
    ProjectRecord, CANONICAL_CONTROL_API_VERSION,
};
pub use checkpoint::{Checkpoint, CheckpointSummary, LoadedCheckpoint, Phase, Staleness};
pub use config::OrchestrationConfig;
pub use context_governor::{
    ContextGovernorConfig, ContextObservation, GovernorAction, GovernorDecision, GovernorState,
    ModelMetadata, TelemetryProvenance, TokenUsage,
};
pub use handoff::{
    HandoffFinding, HandoffVerification, ModelHandoffCapsule, ProjectionInput, Role, Severity,
    Transition,
};
pub use journal::{
    replay as replay_execution_events, ApplyOutcome, BudgetLimitFact, DependencyEdge,
    EventAuthority, EventDelta, EventKind, ExecutionEvent, ExecutionProjection, ExecutionSnapshot,
    JobBinding, JournalBoundary, JournalPrune, ProjectAccounting, ReplayStatus, ReservationFact,
    ResultEvidence, StoredJobConfiguration, StoredVerification, UsageEvidence, AUTHORITY_ACTOR,
    INITIAL_CURSOR, JOURNAL_SCHEMA_VERSION, MAX_EVENT_READ,
};
pub use state::{ContextCache, STATE_SCHEMA_VERSION};
