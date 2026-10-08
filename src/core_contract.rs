//! Shared value types and the persisted Conversation/Message contract.
//! Execution lifecycles and events belong to orchestration::domain and journal.

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum MessageBlockKind {
    Markdown,
    /// Model thinking, distinct from the user-visible markdown answer.
    Reasoning,
    Image,
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
