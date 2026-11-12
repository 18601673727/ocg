//! The only module that touches redb.
//!
//! redb is the durable source of truth. Every record is stored under its
//! stable business ID with an explicit, versioned encoding. Authoritative
//! tables hold the facts; derived tables (`pending`, `ready`, `delayed`,
//! `dependency_waiter`, `unsettled`, `held_slot`, `children`, `successor`,
//! `tip`, `recent`, `live_child`, `tool_call_id` and the later ones) are written by the
//! [`Batch`] operation that writes their source, in the same transaction, so
//! none is an authority of its own. After an unclean shutdown the open
//! procedure compares them with their definitions and rebuilds the ones that
//! differ. The database, its transactions and its tables never leave this
//! module: mutations go through [`Batch`], the only write path.
//!
//! A batch is one redb write transaction. Its first failure, a rule
//! violation as much as a storage error, poisons it: every later operation
//! returns that failure again and [`run_batch_with`] aborts instead of
//! committing, even when the operation's caller ignored the error.
//!
//! A Job is SubtreeSettled exactly when it has no `unsettled` row. That row,
//! with the open dependency and child counts, is kept by T1, settlement and
//! the open reconcile, so the predicate is one point read.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use ::redb::{
    CommitError, Database, Durability, Key, ReadOnlyTable, ReadTransaction, ReadableDatabase,
    ReadableTable, Table, TableDefinition, TableError, Value, WriteTransaction,
};

use crate::db::entities::{
    Actor, Appended, ApprovalDecision, ApprovalRecord, Entry, EntryKind, JobExecution, JobGroup,
    JobInputEntry, JobKind, JobLimits, Notice, NoticeBody, NoticeClass, NoticeEvent, Origin,
    OutcomeKind, PlanSource, RecordEvent, Recurrence, RecurrenceHalt, RecurrencePhase,
    ResponseFormat, StopProof,
};
use crate::db::entities::{
    AgentCapability, AgentConfig, AttachmentId, AttachmentOrigin, AttachmentRecord,
    AuthorizationBasis, CapacityScope, CleanupKind, ConfigId, ConfigRef, ContentDigest,
    DependencyCondition, EffectConclusion, ExecutionOperation, ExecutionPoolId,
    ExecutionPoolRecord, ExecutorId, ExecutorRecord, FinalResult, FrozenExecutionConfig,
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
mod migrate;
pub(crate) use migrate::{HeldSource, MigrationInspection, apply, inspect};
mod notices;
mod observations;
pub(crate) use observations::{DecisionAvailabilityRecord, read_decision_availability};
mod reader;
mod recovery;
pub(crate) use recovery::reclaim_pending;
mod recurrences;
mod rows;
mod schema;
mod transactions;
mod writer;
pub(crate) use transactions::valid_reservation_request_id;

use crate::db::entities::AvailabilityStatId;
pub(crate) use codec::encode_response_format;
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

pub(crate) use reader::{
    DevelopmentCandidate, DevelopmentResolvedCandidate, DevelopmentResolvedContext,
    read_development_execution_context, read_development_execution_preview,
};
pub(crate) use reader::{
    JobScope, ProfilePageRecord, read_delegated_workspace, read_global_compaction_settings,
    read_job_scope,
};
pub(crate) use reader::{
    NativeResource, NativeResourceKind, PoolAllocationPage, read_native_resources,
    read_pool_allocations,
};
pub(crate) use reader::{VerifiedNativeSummary, verify_native_summary};
pub(crate) use reader::{
    inspect_held_allocations_offline, reconcile_development_reservation_offline, verify_offline,
};
#[cfg(unix)]
pub(crate) use reader::{verify_recovered_publication, verify_reserved_publication};
pub(crate) use recovery::{
    HeldRecoveryPlan, ReservedRefusal, reconcile_held_apply, reconcile_held_preflight,
    reconcile_reserved_apply, reconcile_reserved_preflight, verify_held_publication_delta,
    verify_reserved_publication_delta,
};
/// The bytes `character` costs inside a JSON string. Projection chunks text
/// by this cost so every serialized page fits the response budget.
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
    ActivationSnapshot, ContextState, ContextWorkDetails, GENESIS_JOB, JobTree, OfflineActivation,
    RecurrenceAudit, SummarizationInput, TreeFacts, TreeNode, UnresolvedExecution,
    UsageRetriesPage, activation_snapshot, content_available, inspect_activation_offline,
    probe_committed, queue_counts, read_agent_model_configs, read_attachment, read_attachments,
    read_candidate_compatible, read_children, read_compatible_executors, read_config, read_configs,
    read_context_state, read_conversation_search, read_conversations, read_credential_ref_in_use,
    read_decision_executors, read_decision_work, read_effective_context_policy,
    read_execution_pool, read_execution_pools, read_executor, read_executors, read_job,
    read_job_configuration, read_job_input, read_job_input_for_job, read_job_search, read_job_tree,
    read_jobs, read_latest_output, read_native_job, read_occupancy, read_outputs,
    read_pool_binding, read_prediction_current, read_preparation_current, read_profile_page,
    read_project, read_projects, read_retries, read_submission, read_summarization_input,
    read_tool_evidence, read_tool_stats, read_unresolved_executions, read_usage_buckets,
    read_usage_retries, read_usage_summary,
};
pub(crate) use reader::{
    AgentModelConfig, ChildConversationEntry, ConversationsPage, CursorPage, JobSearchPage,
    NativeJobItem, read_approved_calls, read_cards, read_conversation_children, read_has_decider,
    read_has_pending_approvals,
};
pub(crate) use reader::{
    MutableConversationPage, MutableJobDetails, MutableJobDetailsLookup, TurnContext,
    read_conversation_limits, read_conversation_turns, read_mutable_conversation_turn_page,
    read_mutable_job_details, read_notices, read_turn_context, read_turn_execution,
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
    encode_submission, encode_tool_evidence, job_tool_evidence, tool_call_key, tool_call_ref,
};
use schema::*;
pub(crate) use schema::{
    CACHE_BYTES, CREDENTIAL_KEY_FILE_NAME, DATABASE_FILE_NAME, MAX_OBSERVATION_SCAN, MAX_PAGE,
};

pub(crate) const fn storage_format_versions() -> (u64, u8) {
    (schema::DATABASE_FORMAT_VERSION, codec::RECORD_VERSION)
}

// ---- storage errors -------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageErrorKind {
    /// An operating-system I/O error, such as `StorageFull`.
    Io(std::io::ErrorKind),
    /// An earlier I/O error was latched; the open database refuses further work.
    PreviousIo,
    Corrupted,
    AlreadyOpen,
    Closed,
    ValueTooLarge,
    Other,
}

/// A storage-engine failure. Its kind is structured; the detail is for humans.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{kind:?}: {detail}")]
pub struct StorageError {
    pub kind: StorageErrorKind,
    /// The raw OS error number for I/O failures (28 is ENOSPC on macOS and Linux).
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

/// The table-opening half of both transaction kinds, so a reader that opens
/// only tables it owns works on a read and a write transaction alike.
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
        // Unknown ownership, resolved or not, must always force the next open
        // through the Full Path. In particular, never turn a recovered
        // Released + Unknown database into a legacy Quick Path database.
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

pub(crate) fn recurrence_due(database: &Database, clock: i64) -> Result<bool, StorageError> {
    let tx = read(database)?;
    Ok(tx
        .open_table(RECURRENCE_DUE)?
        .first()?
        .is_some_and(|(key, _)| key.value().0 <= clock))
}
pub(crate) fn promotion_clock() -> i64 {
    recovery::promotion_clock()
}

pub(crate) fn next_scheduled_delay(
    database: &Database,
) -> Result<Option<std::time::Duration>, StorageError> {
    let tx = read(database)?;
    let delayed = tx
        .open_table(DELAYED)?
        .first()?
        .map(|(key, _)| key.value().0);
    let recurrence = tx
        .open_table(RECURRENCE_DUE)?
        .first()?
        .map(|(key, _)| key.value().0);
    Ok(delayed.into_iter().chain(recurrence).min().map(|due| {
        std::time::Duration::from_micros(
            due.saturating_sub(recovery::promotion_clock()).max(0) as u64
        )
    }))
}

pub(crate) fn terminal_jobs(
    database: &Database,
    jobs: &[JobId],
) -> Result<Vec<JobId>, StorageError> {
    let tx = read(database)?;
    let states = tx.open_table(JOB_STATES)?;
    let mut terminal = Vec::new();
    for &job in jobs {
        if let Some(state) = states.get(job.get())?
            && JobState::decode_at(job.get(), state.value())?
                .outcome
                .is_some()
        {
            terminal.push(job);
        }
    }
    Ok(terminal)
}

/// Where a Job's durable state stands toward the confirmation of its stop.
/// Both the outcome and the confirmation only ever appear, never revert, so a
/// standing read once stays true for as long as it is not `Open`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StopStanding {
    /// No outcome is durable: the Job may still admit calls.
    Open,
    /// The outcome is durably Unknown and its stop is not yet confirmed. No
    /// call can be admitted any more.
    Unconfirmed,
    /// There is nothing to confirm: the outcome is known, or the stop is
    /// confirmed, or the Job is gone.
    Settled,
}

pub(crate) fn stop_standings(
    database: &Database,
    jobs: &[JobId],
) -> Result<Vec<(JobId, StopStanding)>, StorageError> {
    let tx = read(database)?;
    let states = tx.open_table(JOB_STATES)?;
    let mut standings = Vec::with_capacity(jobs.len());
    for &job in jobs {
        let standing = match states.get(job.get())? {
            None => StopStanding::Settled,
            Some(state) => {
                let state = JobState::decode_at(job.get(), state.value())?;
                match &state.outcome {
                    None => StopStanding::Open,
                    Some(outcome)
                        if outcome.kind == OutcomeKind::Unknown
                            && state.stop_confirmed_at.is_none() =>
                    {
                        StopStanding::Unconfirmed
                    }
                    Some(_) => StopStanding::Settled,
                }
            }
        };
        standings.push((job, standing));
    }
    Ok(standings)
}

pub(crate) use crate::db::entities::{JobActivity, JobUsage};
