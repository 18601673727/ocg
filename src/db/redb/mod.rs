use ::redb::{
    CommitError, Key, ReadOnlyTable, ReadTransaction, ReadableTable, Table, TableDefinition,
    TableError, Value, WriteTransaction,
};

pub(crate) mod approvals;
pub(crate) use approvals::read_cards;
mod codec;
pub(crate) mod execution;
mod indexes;
mod job_records;
mod job_tools;
pub(crate) use job_tools::JobToolApplied;
mod allocations;
mod batch;
mod configs;
mod execution_writes;
mod job_tree;
pub(crate) use job_tree::{read_child_report_page, read_child_turns_need_settlement};
mod jobs;
mod observations;
pub(crate) mod reads;
mod recovery;
pub(crate) use recovery::held::stop_proof;
mod reservations;
mod rows;
mod scheduled_external;
mod schema;
mod settlement;
pub(crate) use settlement::read_recurrence_delay;
mod state;

pub(crate) use allocations::{
    NativeResourceCatalog, read_executors, read_native_resource_catalog, read_pool,
    read_pool_allocations, read_pools, read_uncertain_allocations,
};
pub(crate) use batch::{
    Batch, BatchError, BatchFailure, BatchOutcome, Commit, CompactFailure, Failure,
    NativeResourceDelta, Probe, allocated_bytes, compact, persist_staged, probe_committed,
    run_batch_declinable, run_batch_with,
};
pub(crate) use reads::catalog::{
    StoredCredential, credential_count, read_agent_model_configs, read_attachment, read_config,
    read_configs, read_credential, read_credential_ref_origin_matches,
    read_global_compaction_settings, read_global_outbound_headers, read_handoff_credential,
    read_profile_page, read_project, read_projects, read_submission,
};
pub(crate) use reads::check_readable;
pub(crate) use reads::conversations::{
    read_context_state, read_conversation_children, read_conversation_limits,
    read_conversation_search, read_conversations, read_mutable_conversation_turn_page,
};
pub(crate) use reads::execution_context::{read_development_execution_context, read_trial_job};
pub(crate) use reads::jobs::{
    read_job_search, read_job_tree, read_latest_output, read_mutable_job_details, read_native_job,
    read_output_job_is_decision, read_outputs, read_outputs_with_pieces, read_tool_evidence,
};
pub(crate) use reads::usage::{
    read_tool_stats, read_usage_buckets, read_usage_retries, read_usage_summary,
};
pub(crate) use recovery::held::{
    reconcile_held_apply, reconcile_held_preflight, verify_held_publication_delta,
};
#[cfg(unix)]
pub(crate) use recovery::migration::{migrate_storage, verify_storage_migration};
#[cfg(unix)]
pub(crate) use recovery::offline::{
    apply_compaction_ref_rebuild, plan_compaction_ref_rebuild,
    verify_compaction_ref_publication_delta,
};
pub(crate) use recovery::offline::{
    inspect_held_allocations_offline, verify_offline, verify_publication,
};
pub(crate) use recovery::reserved::{
    ReservedRefusal, reconcile_reserved_apply, reconcile_reserved_preflight,
    verify_reserved_publication_delta,
};
pub(crate) use recovery::{
    OpenedFile, database_file, open, reclaim_pending, record_clean_shutdown,
};
pub(crate) use reservations::{QueueCounts, queue_counts};

pub(crate) use execution::{read_execution_history, read_execution_snapshot};
pub(crate) use scheduled_external::{
    read_scheduled_external_binding_with_policy, read_scheduled_external_policies,
    read_scheduled_external_policy,
};

pub(crate) use observations::{
    PieceReader, read_availability_stats, read_job_availability, read_job_metrics, read_metrics,
    read_provider_request, read_provider_responses,
};

pub(crate) use rows::{CredentialKey, create_credential_key, load_credential_key};
pub(crate) use schema::{DATABASE_FILE_NAME, MAX_OBSERVATION_SCAN};

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
