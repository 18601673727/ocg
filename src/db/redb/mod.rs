use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use ::redb::{
    CommitError, Database, Durability, Key, ReadOnlyTable, ReadTransaction, ReadableDatabase,
    ReadableTable, Table, TableDefinition, TableError, Value, WriteTransaction,
};

use crate::db::entities::{
    Actor, Appended, ApprovalDecision, ApprovalRecord, Entry, JobKind, JobLimits, Origin,
    OutcomeKind, Recurrence, RecurrenceHalt, RecurrencePhase, StopProof,
};
use crate::db::entities::{
    AgentCapability, AgentConfig, AttachmentId, AttachmentOrigin, AttachmentRecord,
    AuthorizationBasis, CleanupKind, ConfigId, ConfigRef, ContentDigest, DependencyCondition,
    EffectConclusion, ExecutionOperation, ExecutionPoolId, ExecutorId, FrozenExecutionConfig,
    FrozenLlmConfig, JobId, JobInputId, JobOutputId, JobOutputRecord, JobRecord, LlmConfig,
    LlmParameters, LlmProviderId, LlmTarget, LlmTransport, Measurements, MetricsId,
    ModelCapability, OutputKind, ProfileId, ProjectId, ProjectRecord, ProviderSnapshot, RecordRef,
    RequestEligibility, RequestNumber, SubmissionKey, SubmissionRecord, Timestamp, ToolEffect,
    ToolEvidenceRecord, UncertainCause, WithheldReason,
};
use crate::service::ai::providers::openai_compatible::{
    ChatFinishReason, NormalizedUsage, ProviderFailure, ToolCallRefusal,
};

use crate::db::entities::{
    AgentModelConfig, ChildConversationEntry, ContextState, ContextWorkDetails, ContextWorkState,
    ConversationEntry, ConversationsPage, CredentialSecret, CursorPage, DevelopmentCandidate,
    DevelopmentContextTurn, DevelopmentDecisionContext, DevelopmentExecutionContext,
    DevelopmentRetainedInput, ExecutionHistoryItem, ExecutionHistoryPage, JobSearchPage, JobTree,
    MutableConfiguration, MutableConversationPage, MutableConversationTurn, MutableJobDetails,
    NativeJobItem, ProfilePageRecord, ProfilePageRecords, ProviderResponse, ProviderResponsesPage,
    TreeFacts, TreeNode, UsageRetriesPage, VerifiedNativeSummary, escaped_json_char_cost,
    valid_credential_ref,
};

mod approvals;
mod codec;
pub(crate) mod execution;
mod indexes;
mod job_records;
mod job_tools;
pub(crate) use job_tools::JobToolApplied;
mod job_tree;
mod observations;
mod reads;
mod recovery;
mod recurrences;
mod rows;
mod schema;
mod state;
use state::JobRuntime;
mod allocations;
mod batch;
mod configs;
mod execution_writes;
mod jobs;
mod reservations;
mod scheduled_external;
mod settlement;

use crate::db::entities::AvailabilityStatId;
use allocations::*;
pub(crate) use allocations::{
    NativeResourceCatalog, read_executors, read_native_resource_catalog, read_pool,
    read_pool_allocations, read_pools, read_uncertain_allocations,
};
pub(crate) use batch::{
    Batch, BatchError, BatchFailure, BatchOutcome, Commit, CompactFailure, Failure,
    NativeResourceDelta, Probe, allocated_bytes, compact, persist_staged, probe_committed,
    run_batch_declinable, run_batch_with,
};
use codec::{
    Code, Dec, DecodeResult, Enc, TAG_ATTACHMENT, TAG_CONFIG, TAG_GLOBAL_COMPACTION,
    TAG_GLOBAL_HEADERS, TAG_JOB, TAG_JOB_RUNTIME, TAG_OUTPUT, TAG_PROJECT, TAG_REQUEST_EVENT,
    TAG_SUBMISSION, TAG_TOOL_CALL, decode_scheduled_external_binding,
    decode_scheduled_external_policy, encode_scheduled_external_binding,
    encode_scheduled_external_policy,
};
use jobs::*;
pub(crate) use reservations::queue_counts;
use reservations::*;

pub(crate) use execution::{read_execution_history, read_execution_snapshot};
pub(crate) use reads::*;
pub(crate) use recovery::*;
pub(crate) use scheduled_external::{
    read_scheduled_external_binding_with_policy, read_scheduled_external_policies,
    read_scheduled_external_policy,
};

pub(crate) use observations::{
    read_availability_stats, read_job_availability, read_job_metrics, read_metrics,
    read_provider_request, read_provider_responses,
};
use reads::read;

pub(crate) use crate::db::entities::{JobLifecycle, JobSpec, JobState};
pub(crate) use rows::{ConfigRow, CredentialKey, create_credential_key, load_credential_key};
use rows::{
    decode_attachment, decode_credential, decode_output, decode_project, decode_project_root,
    decode_submission, decode_tool_evidence, encode_attachment, encode_output, encode_project,
    encode_submission, encode_tool_evidence, tool_call_key, tool_call_ref,
};
use schema::*;
pub(crate) use schema::{
    CACHE_BYTES, CREDENTIAL_KEY_FILE_NAME, DATABASE_FILE_NAME, MAX_OBSERVATION_SCAN, MAX_PAGE,
};

pub(crate) const fn storage_format_versions() -> (u64, u8) {
    (schema::FORMAT_VERSION, schema::record_version_byte())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageErrorKind {
    Io(std::io::ErrorKind),
    PreviousIo,
    Corrupted,
    AlreadyOpen,
    Closed,
    ValueTooLarge,
    Other,
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{kind:?}: {detail}")]
pub struct StorageError {
    pub kind: StorageErrorKind,
    pub os_error: Option<i32>,
    pub detail: String,
}

impl StorageError {
    pub(crate) fn corrupted(detail: impl Into<String>) -> Self {
        Self {
            kind: StorageErrorKind::Corrupted,
            os_error: None,
            detail: detail.into(),
        }
    }
}

impl From<::redb::Error> for StorageError {
    fn from(error: ::redb::Error) -> Self {
        let (kind, os_error) = match &error {
            ::redb::Error::Io(io) => (StorageErrorKind::Io(io.kind()), io.raw_os_error()),
            ::redb::Error::PreviousIo => (StorageErrorKind::PreviousIo, None),
            ::redb::Error::Corrupted(_) => (StorageErrorKind::Corrupted, None),
            ::redb::Error::DatabaseAlreadyOpen => (StorageErrorKind::AlreadyOpen, None),
            ::redb::Error::DatabaseClosed => (StorageErrorKind::Closed, None),
            ::redb::Error::ValueTooLarge(_) => (StorageErrorKind::ValueTooLarge, None),
            _ => (StorageErrorKind::Other, None),
        };
        Self {
            kind,
            os_error,
            detail: error.to_string(),
        }
    }
}

pub(crate) trait TxRead {
    type Table<'t, K: Key + 'static, V: Value + 'static>: ReadableTable<K, V>
    where
        Self: 't;
    fn table<K: Key + 'static, V: Value + 'static>(
        &self,
        definition: TableDefinition<K, V>,
    ) -> Result<Self::Table<'_, K, V>, TableError>;
}
impl TxRead for ReadTransaction {
    type Table<'t, K: Key + 'static, V: Value + 'static> = ReadOnlyTable<K, V>;
    fn table<K: Key + 'static, V: Value + 'static>(
        &self,
        definition: TableDefinition<K, V>,
    ) -> Result<Self::Table<'_, K, V>, TableError> {
        self.open_table(definition)
    }
}
impl TxRead for WriteTransaction {
    type Table<'t, K: Key + 'static, V: Value + 'static> = Table<'t, K, V>;
    fn table<K: Key + 'static, V: Value + 'static>(
        &self,
        definition: TableDefinition<K, V>,
    ) -> Result<Self::Table<'_, K, V>, TableError> {
        self.open_table(definition)
    }
}

pub(crate) fn record_clean_shutdown(database: &Database) -> Result<(), StorageError> {
    let tx = database.begin_write()?;
    {
        let mut unknown = false;
        for entry in tx.open_table(schema::JOB_RUNTIME)?.iter()? {
            let (key, value) = entry?;
            if state::decode_state(key.value(), value.value())?
                .outcome
                .is_some_and(|outcome| outcome.kind == OutcomeKind::Unknown)
            {
                unknown = true;
                break;
            }
        }
        tx.open_table(schema::META)?
            .insert("clean_shutdown", [u8::from(!unknown)].as_slice())?;
    }
    tx.commit()?;
    Ok(())
}

macro_rules! storage_from {
    ($($t:ty),*) => {$(
        impl From<$t> for StorageError {
            fn from(error: $t) -> Self { ::redb::Error::from(error).into() }
        }
    )*};
}
storage_from!(
    ::redb::StorageError,
    ::redb::TableError,
    ::redb::TransactionError,
    ::redb::DatabaseError,
    ::redb::SetDurabilityError,
    CommitError
);

use crate::db::entities::JobUsage;
