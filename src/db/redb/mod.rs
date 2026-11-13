use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use ::redb::{
    CommitError, Database, Durability, Key, ReadOnlyTable, ReadTransaction, ReadableDatabase,
    ReadableTable, Table, TableDefinition, TableError, Value, WriteTransaction,
};

use crate::db::entities::{
    Actor, Appended, ApprovalDecision, ApprovalRecord, Entry, JobExecution, JobGroup,
    JobInputEntry, JobKind, JobLimits, Notice, NoticeBody, NoticeClass, NoticeEvent, Origin,
    OutcomeKind, PlanSource, RecordEvent, Recurrence, RecurrenceHalt, RecurrencePhase,
    ResponseFormat, StopProof,
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

mod approvals;
mod codec;
mod indexes;
mod job_records;
mod job_tools;
pub(crate) use job_tools::JobToolApplied;
mod job_tree;
mod migrate;
pub(crate) use migrate::{HeldSource, MigrationInspection, apply, inspect};
mod notices;
mod observations;
mod reader;
mod recovery;
pub(crate) use recovery::reclaim_pending;
mod recurrences;
mod rows;
mod schema;
mod transactions;
mod writer;

use crate::db::entities::AvailabilityStatId;
use codec::{
    Code, Dec, DecodeResult, Enc, TAG_ATTACHMENT, TAG_CONFIG, TAG_GLOBAL_COMPACTION,
    TAG_GLOBAL_HEADERS, TAG_JOB, TAG_JOB_CONTROL, TAG_JOB_STATE, TAG_OUTPUT, TAG_PROJECT,
    TAG_REQUEST_EVENT, TAG_SUBMISSION, TAG_TOOL_CALL, decode_scheduled_external_binding,
    decode_scheduled_external_policy, encode_scheduled_external_binding,
    encode_scheduled_external_policy,
};
pub(crate) use writer::Failure;
pub(crate) use writer::{
    Batch, BatchError, BatchFailure, BatchOutcome, Commit, CompactFailure, Probe, allocated_bytes,
    compact, persist_staged, run_batch_declinable, run_batch_with,
};

pub(crate) use reader::verify_offline;
pub(crate) use reader::{
    DevelopmentCandidate, DevelopmentResolvedCandidate, DevelopmentResolvedContext,
    read_development_execution_context,
};
pub(crate) use reader::{
    NativeResource, NativeResourceKind, PoolAllocationPage, read_native_resources,
    read_pool_allocations,
};
pub(crate) use reader::{ProfilePageRecord, read_global_compaction_settings};
pub(crate) fn escaped_json_char_cost(character: char) -> usize {
    match character {
        '"' | '\\' | '\n' | '\r' | '\t' | '\u{08}' | '\u{0c}' => 2,
        c if c <= '\u{1f}' => 6,
        c => c.len_utf8(),
    }
}

pub(crate) use observations::{
    ProviderResponsesPage, read_availability_stats, read_job_availability, read_job_metrics,
    read_metrics, read_provider_request, read_provider_responses,
};
use reader::read;
use recovery::allocate;
pub(crate) use recovery::{OpenedFile, database_file, open};

pub(crate) use reader::read_global_outbound_headers;
pub(crate) use reader::read_handoff_credential;
pub(crate) use reader::read_trial_job;
pub(crate) use reader::{
    AgentModelConfig, ChildConversationEntry, ConversationsPage, CursorPage, JobSearchPage,
    NativeJobItem, read_cards, read_conversation_children,
};
pub(crate) use reader::{
    ContextState, ContextWorkDetails, JobTree, TreeFacts, TreeNode, UncertainAllocation,
    UsageRetriesPage, probe_committed, queue_counts, read_agent_model_configs, read_attachment,
    read_attachments, read_children, read_config, read_configs, read_context_state,
    read_conversation_search, read_conversations, read_credential_ref_in_use, read_decision_work,
    read_job, read_job_configuration, read_job_input, read_job_search, read_job_tree,
    read_latest_output, read_native_job, read_outputs, read_profile_page, read_project,
    read_projects, read_retries, read_submission, read_tool_evidence, read_tool_stats,
    read_uncertain_allocations, read_usage_buckets, read_usage_retries, read_usage_summary,
};
pub(crate) use reader::{
    MutableConversationPage, MutableJobDetails, MutableJobDetailsLookup, read_conversation_limits,
    read_mutable_conversation_turn_page, read_mutable_job_details, read_notices,
};
pub(crate) use reader::{ProfilePageRecords, StoredCredential, credential_count, read_credential};
pub(crate) use reader::{
    read_scheduled_external_binding_with_policy, read_scheduled_external_policies,
    read_scheduled_external_policy,
};

pub(crate) use crate::db::entities::{JobControl, JobSpec, JobState};
pub(crate) use rows::{
    ConfigRow, CredentialKey, CredentialSecret, create_credential_key, load_credential_key,
    usage_agent, valid_credential_ref,
};
use rows::{
    decode_attachment, decode_credential, decode_output, decode_project, decode_project_root,
    decode_submission, decode_tool_evidence, encode_attachment, encode_output, encode_project,
    encode_submission, encode_tool_evidence, tool_call_key, tool_call_ref,
};
use schema::*;
pub(crate) use schema::{
    CACHE_BYTES, CREDENTIAL_KEY_FILE_NAME, DATABASE_FILE_NAME, MAX_OBSERVATION_SCAN, MAX_PAGE,
};

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

pub(super) trait TxRead {
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
        for entry in tx.open_table(schema::JOB_STATES)?.iter()? {
            let (key, value) = entry?;
            if JobState::decode_at(key.value(), value.value())?
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

pub(crate) use crate::db::entities::{JobActivity, JobUsage};
