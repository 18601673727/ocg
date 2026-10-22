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

use redb::{
    CommitError, Database, Durability, ReadTransaction, ReadableDatabase, ReadableTable,
    TableDefinition, WriteTransaction,
};

use crate::openai_compatible::{
    ChatFinishReason, NormalizedUsage, ProviderFailure, ToolCallRefusal,
};
use crate::persistence::{
    Actor, Appended, ApprovalDecision, ApprovalRecord, ConversationAction, ConversationSpec, Entry,
    EntryKind, JobKind, Notice, NoticeBody, NoticeClass, NoticeEvent, Origin, OutcomeKind,
    PlanSource, RecordEvent, Recurrence, RecurrenceHalt, RecurrencePhase, ResponseFormat,
    StopProof, TurnEntry, TurnExecution,
};
use crate::persistence::{
    AgentBehavior, AgentConfig, ApproveClass, AttachmentId, AttachmentOrigin, AttachmentRecord,
    AuthorizationBasis, CapacityScope, CleanupKind, ConfigId, ConfigRef, ContentDigest,
    DependencyCondition, EffectConclusion, ExecutionOperation, ExecutionPool, ExecutionPoolId,
    ExecutorId, ExecutorRecord, FinalResult, FrozenExecutionConfig, FrozenLlmConfig, JobId,
    JobInputId, JobOutputId, JobOutputRecord, JobRecord, LlmConfig, LlmParameters, LlmProviderId,
    LlmTarget, LlmTransport, MAX_STORED_TOOL_ARGUMENTS_PER_JOB_BYTES,
    MAX_STORED_TOOL_SCHEMAS_PER_JOB_BYTES, Measurements, MetricsId, ModelCapability, OutputKind,
    PendingJobKey, PendingJobsPage, ProfileId, ProjectId, ProjectRecord, ProviderSnapshot,
    RecordRef, RequestNumber, SubmissionKey, SubmissionRecord, Timestamp, ToolEffect,
    ToolEvidenceRecord, UncertainCause, WithheldReason,
};
use crate::token_usage::RequestEligibility;

mod approvals;
mod batch;
mod codec;
mod conversations;
mod derived;
mod job_tools;
mod notices;
mod observations;
mod open;
mod pending;
mod reads;
mod recurrences;
mod rows;
mod schema;
mod sequences;
mod transactions;

use crate::persistence::{
    AvailabilityStatId, PendingCapturePage, PendingFence, PendingReadFault, PendingSnapshot,
};
use batch::Failure;
pub(crate) use batch::{
    Batch, BatchError, BatchFailure, BatchOutcome, Commit, CompactFailure, Probe, allocated_bytes,
    compact, persist_staged, run_batch_declinable, run_batch_with,
};
pub(crate) use codec::encode_response_format;
use codec::{
    Code, Dec, DecodeResult, Enc, TAG_ATTACHMENT, TAG_CONFIG, TAG_CONVERSATION, TAG_JOB,
    TAG_JOB_STATE, TAG_OUTPUT, TAG_PROJECT, TAG_REQUEST_EVENT, TAG_SUBMISSION, TAG_TOOL_CALL,
};
#[cfg(test)]
pub(crate) use derived::relationship_inconsistencies;
pub(crate) use observations::{
    read_availability_stats, read_job_availability, read_job_metrics, read_metrics,
    read_observed_models, read_provider_request,
};
use open::allocate;
pub(crate) use open::{OpenError, OpenedFile, database_file, open};
pub(crate) use pending::{read_pending_capture_page, read_pending_keys, read_pending_snapshot};
use reads::read;
#[cfg(test)]
pub(crate) use reads::record_attachment_cleanup;
pub(crate) use reads::{
    ChildConversationEntry, ConversationsPage, read_approved_calls, read_cards,
    read_conversation_children, read_has_decider,
};
#[allow(unused_imports)]
pub(crate) use reads::{ConversationEntry, JobUsage};
pub(crate) use reads::{
    JobDetails, JobTree, TreeFacts, TreeNode, UnresolvedExecution, content_available,
    probe_committed, queue_counts, read_attachment, read_attachment_inputs, read_attachments,
    read_children, read_compatible_executors, read_config, read_configs, read_conversations,
    read_execution_pool, read_execution_pools, read_executor, read_executors, read_job,
    read_job_compatible_executors, read_job_configuration, read_job_details, read_job_input,
    read_job_input_for_job, read_job_tree, read_jobs, read_latest_output, read_occupancy,
    read_outputs, read_pending_jobs, read_pool_binding, read_project, read_projects, read_retries,
    read_submission, read_tool_evidence, read_tool_stats, read_unresolved_executions,
    read_usage_buckets, read_usage_summary, unneeded_content,
};
pub(crate) use reads::{
    TurnContext, read_conversation_turns, read_notices, read_turn_context, read_turn_entries,
    read_turn_execution,
};
pub(crate) use rows::{ConfigRow, ConversationRow, JobRow, JobStateRow, usage_agent};
use rows::{
    decode_attachment, decode_output, decode_project, decode_project_root, decode_submission,
    decode_tool_evidence, encode_attachment, encode_output, encode_project, encode_submission,
    encode_tool_evidence, job_tool_evidence, tool_call_key, tool_call_ref,
};
use schema::*;
pub(crate) use schema::{
    CACHE_BYTES, DATABASE_FILE_NAME, FORMAT_NAME, FORMAT_VERSION, MAX_OBSERVATION_SCAN,
    MAX_OBSERVED_MODEL_SCAN, MAX_PAGE, MAX_PENDING_JOBS,
};
pub(crate) use sequences::Sequence;

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

impl From<redb::Error> for StorageError {
    fn from(error: redb::Error) -> Self {
        let (kind, os_error) = match &error {
            redb::Error::Io(io) => (StorageErrorKind::Io(io.kind()), io.raw_os_error()),
            redb::Error::PreviousIo => (StorageErrorKind::PreviousIo, None),
            redb::Error::Corrupted(_) => (StorageErrorKind::Corrupted, None),
            redb::Error::DatabaseAlreadyOpen => (StorageErrorKind::AlreadyOpen, None),
            redb::Error::DatabaseClosed => (StorageErrorKind::Closed, None),
            redb::Error::ValueTooLarge(_) => (StorageErrorKind::ValueTooLarge, None),
            _ => (StorageErrorKind::Other, None),
        };
        Self {
            kind,
            os_error,
            detail: error.to_string(),
        }
    }
}

pub(crate) fn record_clean_shutdown(database: &Database) -> Result<(), StorageError> {
    let tx = database.begin_write()?;
    {
        let mut meta = tx.open_table(schema::META)?;
        meta.insert("clean_shutdown", [1u8].as_slice())?;
    }
    tx.commit()?;
    Ok(())
}

macro_rules! storage_from {
    ($($t:ty),*) => {$(
        impl From<$t> for StorageError {
            fn from(error: $t) -> Self { redb::Error::from(error).into() }
        }
    )*};
}
storage_from!(
    redb::StorageError,
    redb::TableError,
    redb::TransactionError,
    redb::DatabaseError,
    redb::SetDurabilityError,
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
    open::promotion_clock()
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
        std::time::Duration::from_micros(due.saturating_sub(open::promotion_clock()).max(0) as u64)
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
            && JobStateRow::decode_at(job.get(), state.value())?
                .outcome
                .is_some()
        {
            terminal.push(job);
        }
    }
    Ok(terminal)
}
