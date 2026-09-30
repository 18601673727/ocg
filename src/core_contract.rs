//! Rust-owned 0.5 core vocabulary and transport primitives.
//!
//! These types are deliberately transport-safe and execution-neutral. Durable
//! execution remains owned by the canonical Project/Job/Attempt repository; this module
//! gives that repository, the control API and the PWA one vocabulary for refs,
//! lifecycle, failure and measurement facts.

use crate::error::{OcgError, Result};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use ts_rs::TS;
use uuid::{Uuid, Variant};

fn invalid(message: impl Into<String>) -> OcgError {
    OcgError::config(message.into())
}

/// A lowercase canonical UUIDv7 text identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, TS)]
pub struct EntityId(String);

impl EntityId {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        let uuid = Uuid::parse_str(&value)
            .map_err(|_| invalid("EntityId must be a lowercase canonical UUIDv7"))?;
        if uuid.to_string() != value
            || uuid.get_version_num() != 7
            || uuid.get_variant() != Variant::RFC4122
        {
            return Err(invalid("EntityId must be a lowercase canonical UUIDv7"));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Ord for EntityId {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl PartialOrd for EntityId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum EntityKind {
    Job,
    Attempt,
    Call,
    Command,
    Approval,
    Artifact,
    ChangeSet,
    Conversation,
    Message,
    Fact,
    BudgetScope,
    CapabilityRevocation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct EntityRef {
    pub kind: EntityKind,
    pub id: EntityId,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, TS)]
pub struct ProjectScope(String);

impl ProjectScope {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 128
            || value.bytes().any(|byte| {
                !(byte.is_ascii_lowercase()
                    || byte.is_ascii_digit()
                    || matches!(byte, b'-' | b'_' | b'.'))
            })
        {
            return Err(invalid("ProjectScope must be a bounded stable identifier"));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Actor {
    User {
        user_id: String,
    },
    Client {
        client_kind: ClientKind,
        client_id: Option<String>,
    },
    Core,
    Attempt {
        attempt_ref: EntityRef,
    },
    Call {
        call_ref: EntityRef,
    },
    System {
        policy_ref: Option<String>,
    },
    External {
        source_kind: ExternalSourceKind,
        source_ref: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum ClientKind {
    Pwa,
    Cli,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum ExternalSourceKind {
    Provider,
    Capability,
    Runtime,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct JobBlocker {
    pub kind: BlockerKind,
    pub blocking_ref: Option<EntityRef>,
    pub reason_code: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum BlockerKind {
    Dependency,
    RequiredChild,
    Approval,
    Policy,
    Budget,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum JobLifecycle {
    Pending,
    Ready,
    Settling,
    Blocked,
    Completed,
    Failed,
    Cancelled,
}

impl JobLifecycle {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }

    pub fn transition(self, next: Self) -> Result<Self> {
        let allowed = matches!(
            (self, next),
            (
                Self::Pending,
                Self::Ready | Self::Blocked | Self::Failed | Self::Cancelled
            ) | (
                Self::Blocked,
                Self::Pending
                    | Self::Ready
                    | Self::Settling
                    | Self::Completed
                    | Self::Failed
                    | Self::Cancelled
            ) | (
                Self::Ready,
                Self::Pending
                    | Self::Settling
                    | Self::Blocked
                    | Self::Completed
                    | Self::Failed
                    | Self::Cancelled
            ) | (
                Self::Settling,
                Self::Pending
                    | Self::Ready
                    | Self::Blocked
                    | Self::Completed
                    | Self::Failed
                    | Self::Cancelled
            ) | (Self::Failed, Self::Ready)
                | (Self::Cancelled, Self::Ready)
        );
        allowed
            .then_some(next)
            .ok_or_else(|| invalid(format!("illegal Job transition: {self:?} -> {next:?}")))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum AttemptLifecycle {
    Created,
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
    Preempted,
}

impl AttemptLifecycle {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::Preempted
        )
    }

    pub fn transition(self, next: Self) -> Result<Self> {
        let allowed = matches!(
            (self, next),
            (
                Self::Created,
                Self::Queued | Self::Running | Self::Failed | Self::Cancelled
            ) | (
                Self::Queued,
                Self::Running | Self::Failed | Self::Cancelled | Self::Preempted
            ) | (
                Self::Running,
                Self::Succeeded | Self::Failed | Self::Cancelled | Self::Preempted
            )
        );
        allowed
            .then_some(next)
            .ok_or_else(|| invalid(format!("illegal Attempt transition: {self:?} -> {next:?}")))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum CallLifecycle {
    Created,
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

impl CallLifecycle {
    pub fn transition(self, next: Self) -> Result<Self> {
        let allowed = matches!(
            (self, next),
            (
                Self::Created,
                Self::Queued | Self::Running | Self::Failed | Self::Cancelled
            ) | (Self::Queued, Self::Running | Self::Failed | Self::Cancelled)
                | (
                    Self::Running,
                    Self::Succeeded | Self::Failed | Self::Cancelled
                )
        );
        allowed
            .then_some(next)
            .ok_or_else(|| invalid(format!("illegal Call transition: {self:?} -> {next:?}")))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum FailureClass {
    Validation,
    Authentication,
    Authorization,
    Conflict,
    Concurrency,
    NotFound,
    Sandbox,
    Capability,
    Provider,
    Budget,
    ResourceLimit,
    RateLimit,
    Timeout,
    Cancelled,
    Preempted,
    Internal,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct Failure {
    pub code: String,
    pub class: FailureClass,
    pub message: String,
    pub source: Actor,
    pub retryable: bool,
    pub details: Option<serde_json::Value>,
    pub cause: Option<EntityRef>,
    pub entity_ref: Option<EntityRef>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum MeasurementQuality {
    Measured,
    Reported,
    Estimated,
    Derived,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct Fact {
    pub id: EntityId,
    pub project_scope: ProjectScope,
    pub subject_ref: EntityRef,
    pub metric: String,
    pub value: Option<String>,
    pub unit: String,
    pub source: Actor,
    pub observed_at: String,
    pub provenance: Option<String>,
    pub quality: MeasurementQuality,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct MeasurementView {
    pub subject_ref: EntityRef,
    pub facts: Vec<Fact>,
    pub derived_metrics: Vec<DerivedMetric>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct DerivedMetric {
    pub metric: String,
    pub value: Option<String>,
    pub unit: String,
    pub quality: MeasurementQuality,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct ExecutionLimits {
    pub cpu: Option<String>,
    pub memory: Option<String>,
    pub wall_time: Option<String>,
    pub concurrency: Option<u64>,
    pub process_count: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct BudgetSnapshot {
    pub tokens_remaining: Option<u64>,
    pub cost_remaining: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct ExecutorContract {
    pub executor_contract_version: String,
    pub executor_ref: String,
    pub runtime_ref: String,
    pub provider_ref: Option<EntityRef>,
    pub model_ref: Option<EntityRef>,
    pub capability_grants: Vec<CapabilityGrant>,
    pub execution_limits: ExecutionLimits,
    pub budget_scope_ref: EntityRef,
    pub budget_snapshot: BudgetSnapshot,
    pub deadline: Option<String>,
    pub execution_boundary: String,
    pub effective_config_fingerprint: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct CapabilityRef {
    pub capability_id: String,
    pub version: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct CapabilityConstraints {
    pub filesystem: serde_json::Value,
    pub network: serde_json::Value,
    pub process: serde_json::Value,
    pub environment: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct CapabilityGrant {
    pub capability_ref: CapabilityRef,
    pub constraints: CapabilityConstraints,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionMode {
    SingleShot,
    ChangeSetEdit,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct ExecutionRetryPolicy {
    pub max_attempts_per_phase: u32,
    pub retryable_failure_classes: Vec<FailureClass>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct ExecutionPolicy {
    pub schema_version: u32,
    pub mode: ExecutionMode,
    pub retry: ExecutionRetryPolicy,
}

impl ExecutionPolicy {
    pub fn validate(&self) -> Result<()> {
        if self.schema_version != 1 || self.retry.max_attempts_per_phase == 0 {
            return Err(invalid(
                "ExecutionPolicy requires schema_version=1 and at least one attempt per phase",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct ExecutionLeaseRecord {
    pub attempt_ref: EntityRef,
    pub lease_id: EntityId,
    pub holder_id: String,
    pub fence_epoch: String,
    pub issued_at: String,
    pub expires_at: String,
    pub renewal_interval_ms: u64,
    pub state: LeaseState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum LeaseState {
    Active,
    Fenced,
    Expired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct BudgetScope {
    pub id: EntityId,
    pub project_scope: ProjectScope,
    pub job_ref: EntityRef,
    pub ceiling_tokens: Option<u64>,
    pub ceiling_cost: Option<String>,
    pub reserved_tokens: Option<u64>,
    pub reserved_cost: Option<String>,
    pub consumed_tokens: Option<u64>,
    pub consumed_cost: Option<String>,
    pub revision: u64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct ResumeCursor {
    pub cursor_schema_version: String,
    pub contract_version: String,
    pub project_scope: ProjectScope,
    pub generation: String,
    pub sequence: String,
    pub last_event_id: Option<EntityId>,
}

/// Version of the 0.5 Core contract. A change to a wire enum or canonical
/// projection shape requires a deliberate version review.
pub const CORE_CONTRACT_VERSION: &str = "ocg.core.v2";
pub const SNAPSHOT_SCHEMA_VERSION: &str = "ocg.snapshot.v1";
pub const CURSOR_SCHEMA_VERSION: &str = "ocg.cursor.v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum Consistency {
    ReadYourWrites,
    Monotonic,
    Eventual,
    Snapshot,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct ActivityCursor {
    pub generation: String,
    pub sequence: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum PostImagePolicy {
    Required,
    Forbidden,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct EventRegistryEntry {
    pub event_type: EventType,
    pub subject_kind: EntityKind,
    pub schema_version: String,
    pub producer: String,
    pub post_image_policy: PostImagePolicy,
    pub projection_effect_policy: ProjectionEffectPolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionEffectPolicy {
    ReducibleOnly,
    BarrierAllowed,
}

/// The stable event names are part of the sync protocol. Transient message
/// deltas intentionally do not appear here: they are droppable transport data,
/// not canonical facts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum EventType {
    JobCreated,
    JobStateChanged,
    JobDependenciesChanged,
    JobReopened,
    JobArchived,
    JobBlockerAdded,
    JobBlockerResolved,
    JobReady,
    DependencySatisfied,
    SpawnRequested,
    SpawnDeduplicated,
    SchedulerDecision,
    RetryRequested,
    ReplacementRequested,
    RecoveryRequested,
    AttemptCreated,
    AttemptQueued,
    AttemptStarted,
    AttemptSucceeded,
    AttemptFailed,
    AttemptCancelled,
    AttemptPreempted,
    AttemptFenced,
    CallCreated,
    CallQueued,
    CallStarted,
    CallSucceeded,
    CallFailed,
    CallCancelled,
    CallFenced,
    CommandReceived,
    CommandAwaitingApproval,
    CommandAccepted,
    CommandRejected,
    CommandCompleted,
    CommandFailed,
    CommandCancelled,
    ApprovalCreated,
    ApprovalApproved,
    ApprovalRejected,
    ApprovalCancelled,
    ApprovalExpired,
    ChangeSetCreated,
    ChangeSetValidated,
    ChangeSetApplying,
    ChangeSetApplied,
    ChangeSetVerifying,
    ChangeSetVerified,
    ChangeSetConflict,
    ChangeSetFailed,
    ChangeSetCancelled,
    ArtifactCreated,
    ArtifactMetadataUpdated,
    ArtifactArchived,
    ConversationCreated,
    ConversationUpdated,
    ConversationArchived,
    MessageCreated,
    MessageStreamingStarted,
    MessageCompleted,
    MessageFailed,
    MessageUpdated,
    MessageDeleted,
    FactRecorded,
    BudgetReserved,
    BudgetCommitted,
    BudgetReleased,
    BudgetCeilingRaised,
    BudgetExceeded,
    CapabilityRevocationCreated,
    CapabilityRevocationLifted,
}

impl EventType {
    pub fn projection_effect_policy(self) -> ProjectionEffectPolicy {
        match self {
            Self::SpawnRequested
            | Self::SpawnDeduplicated
            | Self::SchedulerDecision
            | Self::RetryRequested
            | Self::ReplacementRequested
            | Self::RecoveryRequested => ProjectionEffectPolicy::ReducibleOnly,
            _ => ProjectionEffectPolicy::BarrierAllowed,
        }
    }

    pub fn is_post_image_required(self) -> bool {
        !matches!(
            self,
            Self::SpawnRequested
                | Self::SpawnDeduplicated
                | Self::SchedulerDecision
                | Self::RetryRequested
                | Self::ReplacementRequested
                | Self::RecoveryRequested
        )
    }

    pub fn subject_kind(self) -> EntityKind {
        use EntityKind::*;
        match self {
            Self::JobCreated
            | Self::JobStateChanged
            | Self::JobDependenciesChanged
            | Self::JobReopened
            | Self::JobArchived
            | Self::JobBlockerAdded
            | Self::JobBlockerResolved
            | Self::JobReady
            | Self::DependencySatisfied
            | Self::SpawnRequested
            | Self::SpawnDeduplicated => Job,
            Self::AttemptCreated
            | Self::AttemptQueued
            | Self::AttemptStarted
            | Self::AttemptSucceeded
            | Self::AttemptFailed
            | Self::AttemptCancelled
            | Self::AttemptPreempted
            | Self::AttemptFenced => Attempt,
            Self::CallCreated
            | Self::CallQueued
            | Self::CallStarted
            | Self::CallSucceeded
            | Self::CallFailed
            | Self::CallCancelled
            | Self::CallFenced => Call,
            Self::CommandReceived
            | Self::CommandAwaitingApproval
            | Self::CommandAccepted
            | Self::CommandRejected
            | Self::CommandCompleted
            | Self::CommandFailed
            | Self::CommandCancelled => Command,
            Self::ApprovalCreated
            | Self::ApprovalApproved
            | Self::ApprovalRejected
            | Self::ApprovalCancelled
            | Self::ApprovalExpired => Approval,
            Self::ChangeSetCreated
            | Self::ChangeSetValidated
            | Self::ChangeSetApplying
            | Self::ChangeSetApplied
            | Self::ChangeSetVerifying
            | Self::ChangeSetVerified
            | Self::ChangeSetConflict
            | Self::ChangeSetFailed
            | Self::ChangeSetCancelled => ChangeSet,
            Self::ArtifactCreated | Self::ArtifactMetadataUpdated | Self::ArtifactArchived => {
                Artifact
            }
            Self::ConversationCreated | Self::ConversationUpdated | Self::ConversationArchived => {
                Conversation
            }
            Self::MessageCreated
            | Self::MessageStreamingStarted
            | Self::MessageCompleted
            | Self::MessageFailed
            | Self::MessageUpdated
            | Self::MessageDeleted => Message,
            Self::FactRecorded => Fact,
            Self::BudgetReserved
            | Self::BudgetCommitted
            | Self::BudgetReleased
            | Self::BudgetCeilingRaised
            | Self::BudgetExceeded => BudgetScope,
            Self::CapabilityRevocationCreated | Self::CapabilityRevocationLifted => {
                CapabilityRevocation
            }
            Self::SchedulerDecision
            | Self::RetryRequested
            | Self::ReplacementRequested
            | Self::RecoveryRequested => Job,
        }
    }
}

/// Registry metadata is generated from the same Rust enum used on the wire.
/// Producers remain stable identifiers rather than free-form user text.
pub fn event_registry() -> Vec<EventRegistryEntry> {
    use EventType::*;
    let all = [
        JobCreated,
        JobStateChanged,
        JobDependenciesChanged,
        JobReopened,
        JobArchived,
        JobBlockerAdded,
        JobBlockerResolved,
        JobReady,
        DependencySatisfied,
        SpawnRequested,
        SpawnDeduplicated,
        SchedulerDecision,
        RetryRequested,
        ReplacementRequested,
        RecoveryRequested,
        AttemptCreated,
        AttemptQueued,
        AttemptStarted,
        AttemptSucceeded,
        AttemptFailed,
        AttemptCancelled,
        AttemptPreempted,
        AttemptFenced,
        CallCreated,
        CallQueued,
        CallStarted,
        CallSucceeded,
        CallFailed,
        CallCancelled,
        CallFenced,
        CommandReceived,
        CommandAwaitingApproval,
        CommandAccepted,
        CommandRejected,
        CommandCompleted,
        CommandFailed,
        CommandCancelled,
        ApprovalCreated,
        ApprovalApproved,
        ApprovalRejected,
        ApprovalCancelled,
        ApprovalExpired,
        ChangeSetCreated,
        ChangeSetValidated,
        ChangeSetApplying,
        ChangeSetApplied,
        ChangeSetVerifying,
        ChangeSetVerified,
        ChangeSetConflict,
        ChangeSetFailed,
        ChangeSetCancelled,
        ArtifactCreated,
        ArtifactMetadataUpdated,
        ArtifactArchived,
        ConversationCreated,
        ConversationUpdated,
        ConversationArchived,
        MessageCreated,
        MessageStreamingStarted,
        MessageCompleted,
        MessageFailed,
        MessageUpdated,
        MessageDeleted,
        FactRecorded,
        BudgetReserved,
        BudgetCommitted,
        BudgetReleased,
        BudgetCeilingRaised,
        BudgetExceeded,
        CapabilityRevocationCreated,
        CapabilityRevocationLifted,
    ];
    all.into_iter()
        .map(|event_type| EventRegistryEntry {
            event_type,
            subject_kind: event_type.subject_kind(),
            schema_version: "v1".to_string(),
            producer: match event_type {
                SchedulerDecision => "scheduler".to_string(),
                SpawnRequested | SpawnDeduplicated => "spawn_service".to_string(),
                RetryRequested | ReplacementRequested | RecoveryRequested => "recovery".to_string(),
                _ => "core".to_string(),
            },
            post_image_policy: if event_type.is_post_image_required() {
                PostImagePolicy::Required
            } else {
                PostImagePolicy::Forbidden
            },
            projection_effect_policy: event_type.projection_effect_policy(),
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(tag = "event_type", content = "payload", rename_all = "snake_case")]
pub enum EventPayload {
    JobCreated(serde_json::Value),
    JobStateChanged(serde_json::Value),
    JobDependenciesChanged(serde_json::Value),
    JobReopened(serde_json::Value),
    JobArchived(serde_json::Value),
    JobBlockerAdded(serde_json::Value),
    JobBlockerResolved(serde_json::Value),
    JobReady(serde_json::Value),
    DependencySatisfied(serde_json::Value),
    SpawnRequested(serde_json::Value),
    SpawnDeduplicated(serde_json::Value),
    SchedulerDecision(serde_json::Value),
    RetryRequested(serde_json::Value),
    ReplacementRequested(serde_json::Value),
    RecoveryRequested(serde_json::Value),
    AttemptCreated(serde_json::Value),
    AttemptQueued(serde_json::Value),
    AttemptStarted(serde_json::Value),
    AttemptSucceeded(serde_json::Value),
    AttemptFailed(serde_json::Value),
    AttemptCancelled(serde_json::Value),
    AttemptPreempted(serde_json::Value),
    AttemptFenced(serde_json::Value),
    CallCreated(serde_json::Value),
    CallQueued(serde_json::Value),
    CallStarted(serde_json::Value),
    CallSucceeded(serde_json::Value),
    CallFailed(serde_json::Value),
    CallCancelled(serde_json::Value),
    CallFenced(serde_json::Value),
    CommandReceived(serde_json::Value),
    CommandAwaitingApproval(serde_json::Value),
    CommandAccepted(serde_json::Value),
    CommandRejected(serde_json::Value),
    CommandCompleted(serde_json::Value),
    CommandFailed(serde_json::Value),
    CommandCancelled(serde_json::Value),
    ApprovalCreated(serde_json::Value),
    ApprovalApproved(serde_json::Value),
    ApprovalRejected(serde_json::Value),
    ApprovalCancelled(serde_json::Value),
    ApprovalExpired(serde_json::Value),
    ChangeSetCreated(serde_json::Value),
    ChangeSetValidated(serde_json::Value),
    ChangeSetApplying(serde_json::Value),
    ChangeSetApplied(serde_json::Value),
    ChangeSetVerifying(serde_json::Value),
    ChangeSetVerified(serde_json::Value),
    ChangeSetConflict(serde_json::Value),
    ChangeSetFailed(serde_json::Value),
    ChangeSetCancelled(serde_json::Value),
    ArtifactCreated(serde_json::Value),
    ArtifactMetadataUpdated(serde_json::Value),
    ArtifactArchived(serde_json::Value),
    ConversationCreated(serde_json::Value),
    ConversationUpdated(serde_json::Value),
    ConversationArchived(serde_json::Value),
    MessageCreated(serde_json::Value),
    MessageStreamingStarted(serde_json::Value),
    MessageCompleted(serde_json::Value),
    MessageFailed(serde_json::Value),
    MessageUpdated(serde_json::Value),
    MessageDeleted(serde_json::Value),
    FactRecorded(serde_json::Value),
    BudgetReserved(serde_json::Value),
    BudgetCommitted(serde_json::Value),
    BudgetReleased(serde_json::Value),
    BudgetCeilingRaised(serde_json::Value),
    BudgetExceeded(serde_json::Value),
    CapabilityRevocationCreated(serde_json::Value),
    CapabilityRevocationLifted(serde_json::Value),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct EventEnvelope {
    pub event_id: EntityId,
    pub project_scope: ProjectScope,
    pub entity_ref: EntityRef,
    pub generation: String,
    pub sequence: String,
    pub timestamp: String,
    pub correlation_id: EntityId,
    pub causation_event_id: Option<EntityId>,
    pub transaction_id: EntityId,
    pub transaction_index: u32,
    pub transaction_count: u32,
    pub event_type: EventType,
    pub schema_version: String,
    pub projection_effect: ProjectionEffect,
    pub payload: EventPayload,
    pub entity_post_image: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionEffect {
    Reducible,
    SnapshotBarrier,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SyncWindowPolicy {
    pub nonterminal_jobs_per_project: u32,
    pub terminal_jobs_per_project: u32,
    pub attempts_per_job: u32,
    pub calls_per_attempt: u32,
    pub commands_per_project: u32,
    pub approvals_per_project: u32,
    pub changesets_per_project: u32,
    pub artifacts_per_project: u32,
    pub active_conversations_per_project: u32,
    pub messages_per_conversation: u32,
    pub archived_conversations_per_project: u32,
    pub facts_per_subject: u32,
    pub capability_revocations_per_project: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct CanonicalEntityProjection {
    pub entity_ref: EntityRef,
    pub project_scope: ProjectScope,
    pub revision: u64,
    pub activity_cursor: ActivityCursor,
    pub state: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct CanonicalSyncProjection {
    pub project_scope: ProjectScope,
    pub sync_policy_version: String,
    pub window: SyncWindowPolicy,
    pub jobs: Vec<CanonicalEntityProjection>,
    pub attempts: Vec<CanonicalEntityProjection>,
    pub calls: Vec<CanonicalEntityProjection>,
    pub commands: Vec<CanonicalEntityProjection>,
    pub approvals: Vec<CanonicalEntityProjection>,
    pub changesets: Vec<CanonicalEntityProjection>,
    pub artifacts: Vec<CanonicalEntityProjection>,
    pub conversations: Vec<CanonicalEntityProjection>,
    pub messages: Vec<CanonicalEntityProjection>,
    pub facts: Vec<CanonicalEntityProjection>,
    pub budget_scopes: Vec<CanonicalEntityProjection>,
    pub capability_revocations: Vec<CanonicalEntityProjection>,
    pub ref_stubs: Vec<EntityRef>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Snapshot {
    pub project_scope: ProjectScope,
    pub generation: String,
    pub covered_sequence: String,
    pub last_event_id: Option<EntityId>,
    pub snapshot_schema_version: String,
    pub contract_version: String,
    pub captured_at: String,
    pub state_hash: String,
    pub state: CanonicalSyncProjection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum CommandState {
    Received,
    AwaitingApproval,
    Accepted,
    Rejected,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Command {
    pub id: EntityId,
    pub project_scope: ProjectScope,
    pub origin: Actor,
    pub action: String,
    pub target: Option<EntityRef>,
    pub arguments: serde_json::Value,
    pub idempotency_key: Option<String>,
    pub request_fingerprint: String,
    pub correlation_id: EntityId,
    pub state: CommandState,
    pub result: Option<serde_json::Value>,
    pub failure: Option<Failure>,
    pub revision: u64,
    pub created_at: String,
    pub completed_at: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalState {
    Pending,
    Approved,
    Rejected,
    Cancelled,
    Expired,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ApprovalDecision {
    pub actor: Actor,
    pub rationale: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Approval {
    pub id: EntityId,
    pub project_scope: ProjectScope,
    pub subject_ref: EntityRef,
    pub subject_fingerprint: String,
    pub requester: Actor,
    pub state: ApprovalState,
    pub decision: Option<ApprovalDecision>,
    pub expires_at: Option<String>,
    pub revision: u64,
    pub created_at: String,
    pub resolved_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum MessageBlockKind {
    Markdown,
    EntityRef,
    ArtifactRef,
    DiffRef,
    StatusProjection,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct MessageBlock {
    pub kind: MessageBlockKind,
    pub content: Option<String>,
    pub entity_ref: Option<EntityRef>,
    pub artifact_ref: Option<EntityRef>,
    pub changeset_ref: Option<EntityRef>,
    pub projection_kind: Option<String>,
    pub raw: Option<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Conversation {
    pub id: EntityId,
    pub project_scope: ProjectScope,
    pub title: Option<String>,
    pub revision: u64,
    pub created_at: String,
    pub updated_at: String,
    pub archived_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct Message {
    pub id: EntityId,
    pub project_scope: ProjectScope,
    pub conversation_ref: EntityRef,
    pub author: Actor,
    pub produced_by_attempt_ref: Option<EntityRef>,
    pub blocks: Vec<MessageBlock>,
    pub state: MessageLifecycle,
    pub revision: u64,
    pub created_at: String,
    pub updated_at: String,
    pub deleted_at: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum MessageLifecycle {
    Pending,
    Streaming,
    Complete,
    Failed,
    Deleted,
}

impl MessageLifecycle {
    pub fn transition(self, next: Self) -> Result<Self> {
        let allowed = matches!(
            (self, next),
            (
                Self::Pending,
                Self::Streaming | Self::Complete | Self::Failed | Self::Deleted
            ) | (
                Self::Streaming,
                Self::Complete | Self::Failed | Self::Deleted
            ) | (Self::Complete | Self::Failed, Self::Deleted)
        );
        allowed
            .then_some(next)
            .ok_or_else(|| invalid(format!("illegal Message transition: {self:?} -> {next:?}")))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct TransientMessageDelta {
    pub project_scope: ProjectScope,
    pub generation: String,
    pub stream_id: String,
    pub message_ref: EntityRef,
    pub attempt_ref: EntityRef,
    pub chunk_index: String,
    pub delta_utf8: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum ChildJoinPolicy {
    Required,
    NotRequired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum ChildCancellationPolicy {
    Cascade,
    SoftCascade,
    Independent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum ChildFailurePolicy {
    BlockParent,
    FailParent,
    NonBlocking,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct ChildPolicy {
    pub join: ChildJoinPolicy,
    pub cancellation: ChildCancellationPolicy,
    pub failure: ChildFailurePolicy,
    pub grace_period_ms: Option<u64>,
}

impl ChildPolicy {
    pub fn validate(&self) -> Result<()> {
        if self.join == ChildJoinPolicy::NotRequired
            && self.failure != ChildFailurePolicy::NonBlocking
        {
            return Err(invalid("join=not_required requires failure=non_blocking"));
        }
        match (self.cancellation, self.grace_period_ms) {
            (ChildCancellationPolicy::SoftCascade, Some(1..=300_000)) => Ok(()),
            (ChildCancellationPolicy::SoftCascade, _) => Err(invalid(
                "soft_cascade requires grace_period_ms in 1..=300000",
            )),
            (_, None) => Ok(()),
            (_, Some(_)) => Err(invalid("grace_period_ms is only valid for soft_cascade")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct CapabilityRevocation {
    pub id: EntityId,
    pub project_scope: ProjectScope,
    pub capability_ref: CapabilityRef,
    pub attempt_ref: Option<EntityRef>,
    pub actor: Actor,
    pub reason_code: String,
    pub state: CapabilityRevocationState,
    pub revision: u64,
    pub created_at: String,
    pub lifted_at: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityRevocationState {
    Active,
    Lifted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum SideEffectMode {
    Idempotent,
    StrictFenced,
    Reconcilable,
    NonRetryable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum EffectIntentState {
    Intended,
    Effected,
    Reconciled,
    Unknown,
}

/// Durable execution outbox record. It is intentionally not an EntityRef.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
pub struct EffectIntent {
    pub intent_id: EntityId,
    pub project_scope: ProjectScope,
    pub call_ref: EntityRef,
    pub capability_ref: CapabilityRef,
    pub reconciliation_key: String,
    pub input_fingerprint: String,
    pub effect_mode: SideEffectMode,
    pub admitted_fence_epoch: String,
    pub state: EffectIntentState,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct SpawnChildRequest {
    pub project_scope: ProjectScope,
    pub parent_ref: EntityRef,
    pub spawn_key: String,
    pub spawn_fingerprint: String,
    pub child_spec: serde_json::Value,
    pub child_policy: ChildPolicy,
    pub causation_event_id: EntityId,
    pub idempotency_key: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ExecutionGraphNode {
    pub entity_ref: EntityRef,
    pub level: String,
    pub state: String,
    pub activity_cursor: Option<ActivityCursor>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ExecutionGraphEdge {
    pub from: EntityRef,
    pub to: EntityRef,
    pub relation: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
pub struct ExecutionGraphProjection {
    pub project_scope: ProjectScope,
    pub nodes: Vec<ExecutionGraphNode>,
    pub edges: Vec<ExecutionGraphEdge>,
}
