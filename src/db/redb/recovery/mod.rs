use super::{StorageError, StorageErrorKind, TxRead, execution, jobs};
use crate::db::entities::{
    ApprovalRecord, ContentDigest, ExecutionOperation, ExecutionPoolId, ExecutorId, JobSpec,
    JobState, LlmTransport, OutputKind, Recurrence,
};
use crate::db::entities::{JobId, OutcomeKind, Timestamp};
use crate::db::redb::batch::{Batch, Commit};
use crate::db::redb::codec::{Code, Enc, TAG_GLOBAL_COMPACTION};
use crate::db::redb::rows::ConfigRow;
use crate::db::redb::schema::{
    ACTIVE_ALLOCATIONS, ALLOCATION_REQUESTS, APPROVALS, ATTACHMENTS, CACHE_BYTES, CHILD_GROUPS,
    CHILDREN, CONFIGS, CONTENT_RECLAIM, CONVERSATIONS, CREDENTIALS, DATABASE_FILE_NAME,
    DATABASE_FORMAT_KEY, DEPENDENCY_WAITERS, EXECUTION_COMMITS, EXECUTORS, FORMAT_VERSION,
    JOB_EVENTS, JOB_INPUTS, JOB_OUTPUTS, JOB_RUNTIME, JOBS, LIVE_CHILDREN, MAX_PAGE, META,
    POOL_ALLOCATIONS, POOL_EXECUTORS, POOLS, PROJECTS, RECURRENCES, REQUEST_EVENTS,
    REQUEST_TIMELINE, RETRY_JOBS, SCHEDULED_EXTERNAL_BINDING, SCHEDULED_EXTERNAL_POLICY,
    SCHEMA_FINGERPRINT, SCHEMA_FINGERPRINT_KEY, SEQUENCES, SUBMISSIONS, TOOL_CALLS, TURNS,
    check_record_headers, check_required_tables, create_tables,
};
use crate::db::redb::state::JobRuntime;
use crate::db::redb::{allocations, codec, indexes, job_tree, reads, reservations, rows, state};
use ::redb::{
    Database, Durability, ReadTransaction, ReadableTable, TableDefinition, TableError,
    WriteTransaction,
};
use std::path::{Path, PathBuf};

pub(super) mod audit;
pub(super) mod held;
pub(super) mod offline;
pub(super) mod reserved;
mod settle;

pub(crate) use settle::*;

pub(crate) fn database_file(directory: &Path) -> PathBuf {
    directory.join(DATABASE_FILE_NAME)
}

pub(crate) struct OpenedFile {
    pub(crate) database: Database,
    pub(crate) epoch: u64,
}

pub(crate) fn open(directory: &Path) -> Result<OpenedFile, StorageError> {
    let path = database_file(directory);
    let mut options = std::fs::OpenOptions::new();
    options.read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let (file, fresh) = match options.open(&path) {
        Ok(file) => (file, false),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let mut create = std::fs::OpenOptions::new();
            create.read(true).write(true).create_new(true);
            let file = crate::host::data_directory::private_file(&mut create)
                .open(&path)
                .map_err(io_storage_error)?;
            (file, true)
        }
        Err(error) => return Err(io_storage_error(error)),
    };
    let metadata = file.metadata().map_err(io_storage_error)?;
    if !metadata.is_file() {
        return Err(StorageError::corrupted("database is not a regular file"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Err(StorageError::corrupted(
                "database must not have multiple hard links",
            ));
        }
    }
    if !fresh {
        if metadata.len() == 0 {
            return Err(unsupported_format(
                "existing database is empty; refusing initialization",
            ));
        }
        check_database_identity(&file, &path)?;
        preflight(&file, &path, directory)?;
        check_database_identity(&file, &path)?;
        if file.metadata().map_err(io_storage_error)?.len() == 0 {
            return Err(StorageError::corrupted(
                "database became empty during preflight",
            ));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            file.set_permissions(std::fs::Permissions::from_mode(0o600))
                .map_err(io_storage_error)?;
        }
    }
    let mut builder = Database::builder();
    builder.set_cache_size(CACHE_BYTES);
    let database = builder.create_file(file).map_err(StorageError::from)?;
    let epoch = initialize(&database, fresh)?;
    Ok(OpenedFile { database, epoch })
}

fn io_storage_error(error: std::io::Error) -> StorageError {
    StorageError {
        kind: StorageErrorKind::Io(error.kind()),
        os_error: error.raw_os_error(),
        detail: error.to_string(),
    }
}

fn check_database_identity(file: &std::fs::File, path: &Path) -> Result<(), StorageError> {
    let actual = file.metadata().map_err(io_storage_error)?;
    let named = std::fs::symlink_metadata(path).map_err(io_storage_error)?;
    if !named.is_file() || !same_file_identity(&actual, &named) {
        return Err(StorageError::corrupted("database pathname changed"));
    }
    Ok(())
}

fn same_file_identity(actual: &std::fs::Metadata, named: &std::fs::Metadata) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        actual.dev() == named.dev() && actual.ino() == named.ino()
    }
    #[cfg(windows)]
    {
        let _ = (actual, named);
        true
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (actual, named);
        false
    }
}

pub(super) fn check_identity(
    meta: &impl ReadableTable<&'static str, &'static [u8]>,
) -> Result<(), StorageError> {
    let found = match meta.get(DATABASE_FORMAT_KEY)? {
        None => {
            return Err(unsupported_format(&format!(
                "unmarked database; this build opens only format {FORMAT_VERSION}"
            )));
        }
        Some(value) => <[u8; 8]>::try_from(value.value())
            .map(u64::from_le_bytes)
            .map_err(|_| unsupported_format("database format marker is malformed"))?,
    };
    if found != FORMAT_VERSION {
        return Err(unsupported_format(&format!(
            "database format {found} is not supported; this build opens only format {FORMAT_VERSION}"
        )));
    }
    if meta
        .get(SCHEMA_FINGERPRINT_KEY)?
        .as_ref()
        .map(|value| value.value())
        != Some(SCHEMA_FINGERPRINT)
    {
        return Err(unsupported_format(
            "database layout is incompatible with this build",
        ));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Verification {
    Quick,
    Full,
}

fn full_verification_requested() -> bool {
    std::env::var_os("OCG_VERIFY_STARTUP").is_some_and(|value| value == "full")
}

fn validate_preflight(
    database: &impl ::redb::ReadableDatabase,
) -> Result<Verification, StorageError> {
    startup_checks(database, full_verification_requested())
}

pub(crate) fn verify_recovered(
    database: &impl ::redb::ReadableDatabase,
) -> Result<Verification, StorageError> {
    let tx = database.begin_read()?;
    if startup_metadata(&tx)? {
        return Err(StorageError::corrupted(
            "held recovery must preserve clean_shutdown = 0",
        ));
    }
    check_records(&tx)?;
    let allocations = tx.open_table(POOL_ALLOCATIONS)?;
    for entry in tx.open_table(JOB_RUNTIME)?.iter()? {
        let (key, value) = entry?;
        let state = state::decode_state(key.value(), value.value())?;
        if state
            .outcome
            .as_ref()
            .is_some_and(|outcome| outcome.kind == OutcomeKind::Unknown)
            && !resolved_unknown_ownership(&tx, &state)?
        {
            let retained = allocations
                .get(state.job_id.get())?
                .map(|row| allocations::decode_pool_allocation(state.job_id.get(), row.value()))
                .transpose()?
                .is_some_and(|allocation| allocation.holds_capacity());
            if !retained {
                return Err(StorageError::corrupted(format!(
                    "{} is unresolved Unknown without a retained held Allocation",
                    state.job_id
                )));
            }
        }
    }
    Ok(Verification::Full)
}

fn startup_checks(
    database: &impl ::redb::ReadableDatabase,
    force_full: bool,
) -> Result<Verification, StorageError> {
    let tx = database.begin_read()?;
    let clean = startup_metadata(&tx)?;
    if clean && !force_full && allocations_are_quiet(&tx)? {
        if !indexes::verify_compaction_refs(&tx)? {
            return Ok(Verification::Quick);
        }
        check_records_after_compaction_refs(&tx)?;
    } else {
        check_records(&tx)?;
    }
    refuse_unresolved_ownership(&tx)?;
    Ok(Verification::Full)
}

fn startup_metadata(tx: &ReadTransaction) -> Result<bool, StorageError> {
    let meta = match tx.open_table(META) {
        Ok(meta) => meta,
        Err(TableError::TableDoesNotExist(_)) => {
            return Err(unsupported_format(
                "not an OCG database: it has no metadata table",
            ));
        }
        Err(error) => return Err(error.into()),
    };
    check_identity(&meta)?;
    check_required_tables(tx)?;
    let epoch = meta
        .get("open_epoch")?
        .ok_or_else(|| StorageError::corrupted("missing open epoch"))?;
    let shutdown = meta
        .get("clean_shutdown")?
        .ok_or_else(|| StorageError::corrupted("missing shutdown marker"))?;
    if epoch.value().len() != 8 || !matches!(shutdown.value(), [0] | [1]) {
        return Err(StorageError::corrupted("invalid startup metadata"));
    }
    let clean = matches!(shutdown.value(), [1]);
    let settings = meta
        .get("compaction_settings")?
        .ok_or_else(|| StorageError::corrupted("missing compaction settings"))?;
    check_preflight_header(settings.value(), &[TAG_GLOBAL_COMPACTION])?;
    reads::catalog::global_compaction_settings(&meta)?;
    Ok(clean)
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ResidualOwnership {
    pub(crate) held: std::collections::BTreeSet<u128>,
    pub(crate) retained_unknown: std::collections::BTreeSet<u128>,
    pub(crate) resolved_unknown: std::collections::BTreeSet<u128>,
}

impl ResidualOwnership {
    fn unresolved(&self) -> bool {
        !self.held.is_empty() || !self.retained_unknown.is_empty()
    }

    fn facts(&self) -> serde_json::Value {
        let ids = |set: &std::collections::BTreeSet<u128>| {
            set.iter()
                .map(|id| JobId::new(*id).to_string())
                .collect::<Vec<_>>()
        };
        serde_json::json!({
            "held_allocations": ids(&self.held),
            "retained_unknown_jobs": ids(&self.retained_unknown),
            "resolved_unknown_jobs": ids(&self.resolved_unknown),
        })
    }
}

fn residual_ownership_in(tx: &ReadTransaction) -> Result<ResidualOwnership, StorageError> {
    use crate::db::entities::{AllocationPhase, MutableJobPhase};
    let mut ownership = ResidualOwnership::default();
    for entry in tx.open_table(POOL_ALLOCATIONS)?.iter()? {
        let (key, value) = entry?;
        let allocation = allocations::decode_pool_allocation(key.value(), value.value())?;
        if !allocation.holds_capacity() {
            continue;
        }
        let job = allocation.token.job;
        let record = match reads::jobs::mutable_job_record_in(tx, job) {
            Ok(Some(record)) => record,
            Ok(None) => {
                return Err(StorageError::corrupted(format!(
                    "the held Allocation of {job} names no native Job"
                )));
            }
            Err(error) if error.kind == StorageErrorKind::Corrupted => {
                return Err(StorageError::corrupted(format!(
                    "the held Allocation of {job} is inconsistent: {}",
                    error.detail
                )));
            }
            Err(error) => return Err(error),
        };
        let started = record.job.phase == MutableJobPhase::Executing;
        let outcome = record.outcome.as_ref().map(|outcome| outcome.kind);
        let shaped = allocation.stop_confirmed_at.is_none()
            && allocation.stop_proof.is_none()
            && allocation.job_revision == record.job.revision
            && allocation.started_at.is_some() == started
            && (started || record.job.phase == MutableJobPhase::Queued)
            && match allocation.phase {
                AllocationPhase::Reserved => !started && outcome.is_none(),
                AllocationPhase::Running => started && outcome.is_none(),
                AllocationPhase::StopRequested => outcome.is_none(),
                AllocationPhase::Uncertain => outcome == Some(OutcomeKind::Unknown),
                AllocationPhase::Released => false,
            };
        if !shaped {
            return Err(StorageError::corrupted(format!(
                "the held Allocation of {job} contradicts its Job"
            )));
        }
        if confirmed_answer(tx, &allocation)?.is_some() {
            continue;
        }
        ownership.held.insert(job.get());
    }
    for entry in tx.open_table(JOB_RUNTIME)?.iter()? {
        let (key, value) = entry?;
        let state = state::decode_state(key.value(), value.value())?;
        if !state
            .outcome
            .as_ref()
            .is_some_and(|outcome| outcome.kind == OutcomeKind::Unknown)
        {
            continue;
        }
        if resolved_unknown_ownership(tx, &state)? {
            ownership.resolved_unknown.insert(key.value());
        } else if ownership.held.contains(&key.value()) {
            ownership.retained_unknown.insert(key.value());
        } else {
            return Err(StorageError::corrupted(format!(
                "{} is unresolved Unknown without a retained held Allocation",
                state.job_id
            )));
        }
    }
    Ok(ownership)
}

pub(crate) struct ReservedResidual {
    pub(crate) ownership: ResidualOwnership,
    pub(crate) normal_startup_accepts: bool,
}

pub(crate) fn verify_reserved_residual(
    database: &impl ::redb::ReadableDatabase,
) -> Result<ReservedResidual, StorageError> {
    let tx = database.begin_read()?;
    startup_metadata(&tx)?;
    check_records(&tx)?;
    let ownership = residual_ownership_in(&tx)?;
    let normal_startup_accepts = !ownership.unresolved();
    Ok(ReservedResidual {
        ownership,
        normal_startup_accepts,
    })
}

fn check_records(tx: &ReadTransaction) -> Result<(), StorageError> {
    indexes::verify_compaction_refs(tx)?;
    check_records_after_compaction_refs(tx)
}

fn check_records_after_compaction_refs(tx: &ReadTransaction) -> Result<(), StorageError> {
    check_record_headers(tx)?;
    check_record_payloads(tx)?;
    check_native_invariants(tx)?;
    indexes::verify_job_indexes(tx)?;
    check_execution_history(tx)
}

fn allocations_are_quiet(tx: &ReadTransaction) -> Result<bool, StorageError> {
    let (allocations, pools, active, requests) = match (
        tx.open_table(POOL_ALLOCATIONS),
        tx.open_table(POOLS),
        tx.open_table(ACTIVE_ALLOCATIONS),
        tx.open_table(ALLOCATION_REQUESTS),
    ) {
        (Ok(allocations), Ok(pools), Ok(active), Ok(requests)) => {
            (allocations, pools, active, requests)
        }
        _ => return Ok(false),
    };
    let Ok(computed) = indexes::compute_allocation_indexes(&allocations, &pools) else {
        return Ok(false);
    };
    Ok(computed.active.is_empty()
        && computed == indexes::stored_allocation_indexes(&active, &requests)?)
}

fn check_native_invariants(tx: &ReadTransaction) -> Result<(), StorageError> {
    indexes::verify_pool_executor_index(
        &tx.open_table(EXECUTORS)?,
        &tx.open_table(POOL_EXECUTORS)?,
    )?;
    let (allocations, pools, active, requests) = match (
        tx.open_table(POOL_ALLOCATIONS),
        tx.open_table(POOLS),
        tx.open_table(ACTIVE_ALLOCATIONS),
        tx.open_table(ALLOCATION_REQUESTS),
    ) {
        (Ok(allocations), Ok(pools), Ok(active), Ok(requests)) => {
            (allocations, pools, active, requests)
        }
        _ => {
            return Err(StorageError::corrupted(
                "an Allocation table or index is missing",
            ));
        }
    };
    if !indexes::allocation_indexes_agree(&allocations, &pools, &active, &requests)? {
        return Err(StorageError::corrupted(
            "the Allocation indexes differ from their source rows",
        ));
    }
    let jobs = tx.open_table(JOBS)?;
    let conversations = tx.open_table(CONVERSATIONS)?;
    let turns = tx.open_table(TURNS)?;
    let retries = tx.open_table(RETRY_JOBS)?;
    let mut agent_retries = std::collections::BTreeMap::<JobId, usize>::new();
    for entry in conversations.iter()? {
        let (key, value) = entry?;
        let spec = JobSpec::decode_at(key.value(), value.value())?;
        if jobs.get(spec.id.get())?.is_some() {
            return Err(StorageError::corrupted(
                "Job and conversation identities overlap",
            ));
        }
        if let Some(parent) = spec.parent_id {
            require_job_exists(&conversations, parent.get(), "conversation parent")?;
        }
        if spec.is_standalone_decision_group() {
            let mut members = turns.range((spec.id.get(), 0)..=(spec.id.get(), u128::MAX))?;
            let member = members
                .next()
                .transpose()?
                .ok_or_else(|| StorageError::corrupted("standalone Decision Group has no Job"))?
                .0
                .value()
                .1;
            if members.next().transpose()?.is_some() {
                return Err(StorageError::corrupted(
                    "standalone Decision Group has multiple Jobs",
                ));
            }
            let row = jobs
                .get(member)?
                .ok_or_else(|| StorageError::corrupted("standalone Decision Job is missing"))?;
            let job = jobs::decode_mutable_job(member, row.value())?;
            if job.parent_id != spec.id
                || job.previous.is_some()
                || job.summary_source.is_some()
                || !matches!(
                    job.input.content,
                    crate::db::entities::MutableJobContent::Decision { .. }
                )
            {
                return Err(StorageError::corrupted("invalid standalone Decision Job"));
            }
            let states = tx.open_table(JOB_RUNTIME)?;
            let group_state = states.get(spec.id.get())?.ok_or_else(|| {
                StorageError::corrupted("standalone Decision Group state is missing")
            })?;
            let job_state = states
                .get(member)?
                .ok_or_else(|| StorageError::corrupted("standalone Decision state is missing"))?;
            let group_outcome = state::decode_state(spec.id.get(), group_state.value())?.outcome;
            let job_outcome = state::decode_state(member, job_state.value())?.outcome;
            let confirmed = job_outcome
                .as_ref()
                .filter(|outcome| outcome.kind != OutcomeKind::Unknown);
            if group_outcome.as_ref() != confirmed {
                return Err(StorageError::corrupted(
                    "standalone Decision Group outcome disagrees with its Job",
                ));
            }
        }
    }
    for entry in jobs.iter()? {
        let (key, value) = entry?;
        let job = jobs::decode_mutable_job(key.value(), value.value())?;
        if let Some(retry) = &job.retry {
            let original = jobs
                .get(retry.original.get())?
                .map(|row| jobs::decode_mutable_job(retry.original.get(), row.value()))
                .transpose()?
                .ok_or_else(|| StorageError::corrupted("retry source missing"))?;
            jobs::validate_retry_source(&job, &original)?;
            if retries.get((retry.original.get(), job.id.get()))?.is_none() {
                return Err(StorageError::corrupted(
                    "retry Job has no retry index entry",
                ));
            }
            let previous = job
                .previous
                .ok_or_else(|| StorageError::corrupted("retry has no preceding turn"))?;
            let prior = jobs
                .get(previous.get())?
                .map(|row| jobs::decode_mutable_job(previous.get(), row.value()))
                .transpose()?
                .ok_or_else(|| StorageError::corrupted("retry predecessor missing"))?;
            if prior.parent_id != job.parent_id
                || prior
                    .retry
                    .as_ref()
                    .map_or(prior.id, |retry| retry.original)
                    != retry.original
                || !state::require(tx, previous)?
                    .state
                    .outcome
                    .is_some_and(|outcome| {
                        matches!(outcome.kind, OutcomeKind::Failed | OutcomeKind::Cancelled)
                    })
            {
                return Err(StorageError::corrupted(
                    "retry predecessor is not a settled retry source",
                ));
            }
            if let crate::db::entities::Actor::Agent { conversation } = retry.by {
                require_job_exists(&conversations, conversation.get(), "retry actor")?;
                let count = agent_retries.entry(retry.original).or_default();
                *count += 1;
                if *count > job_tree::MAX_AGENT_RETRIES {
                    return Err(StorageError::corrupted("agent retry limit exceeded"));
                }
            }
        }
        let parent = conversations
            .get(job.parent_id.get())?
            .ok_or_else(|| StorageError::corrupted("mutable Job conversation is missing"))?;
        let spec = JobSpec::decode_at(job.parent_id.get(), parent.value())?;
        if spec.is_standalone_decision_group()
            && (job.previous.is_some()
                || job.summary_source.is_some()
                || !matches!(
                    job.input.content,
                    crate::db::entities::MutableJobContent::Decision { .. }
                ))
        {
            return Err(StorageError::corrupted("invalid standalone Decision Job"));
        }
        let indexed_turn = turns.get((job.parent_id.get(), job.id.get()))?.is_some();
        let predictive_child = jobs::is_predictive_helper(tx, &job)?;
        let expected = job.phase != crate::db::entities::MutableJobPhase::Unsubmitted
            && job.summary_source.is_none()
            && !spec.is_chunk_group()
            && !predictive_child;
        if indexed_turn != expected {
            return Err(StorageError::corrupted("turn index differs from its Job"));
        }
    }
    for entry in turns.iter()? {
        let (key, _) = entry?;
        let (parent, id) = key.value();
        let row = jobs
            .get(id)?
            .ok_or_else(|| StorageError::corrupted("turn index Job missing"))?;
        let job = jobs::decode_mutable_job(id, row.value())?;
        if job.parent_id.get() != parent {
            return Err(StorageError::corrupted(
                "turn index names another conversation",
            ));
        }
    }
    for entry in retries.iter()? {
        let (key, _) = entry?;
        let (original, id) = key.value();
        let job = jobs
            .get(id)?
            .map(|row| jobs::decode_mutable_job(id, row.value()))
            .transpose()?
            .ok_or_else(|| StorageError::corrupted("retry index Job missing"))?;
        if job.retry.as_ref().map(|retry| retry.original.get()) != Some(original) {
            return Err(StorageError::corrupted(
                "retry index disagrees with its Job",
            ));
        }
    }
    let states = tx.open_table(JOB_RUNTIME)?;
    for entry in states.iter()? {
        let (key, value) = entry?;
        let state = state::decode_state(key.value(), value.value())?;
        if jobs.get(state.job_id.get())?.is_none()
            && conversations.get(state.job_id.get())?.is_none()
        {
            return Err(StorageError::corrupted(
                "job_state row has no matching job record",
            ));
        }
    }
    Ok(())
}

fn check_execution_history(tx: &ReadTransaction) -> Result<(), StorageError> {
    use crate::db::entities::{
        AllocationPhase, DependencyState, ExecutionEventKind, MutableJobPhase,
    };
    use std::collections::{BTreeMap, BTreeSet};

    let jobs = tx.open_table(JOBS)?;
    let conversations = tx.open_table(CONVERSATIONS)?;
    let states = tx.open_table(JOB_RUNTIME)?;
    let allocations = tx.open_table(POOL_ALLOCATIONS)?;
    let events = tx.open_table(JOB_EVENTS)?;
    let commits = tx.open_table(EXECUTION_COMMITS)?;
    let mut last_commit = 0u64;
    for entry in commits.iter()? {
        let (first, last) = entry?;
        let first = first.value();
        let last = last.value();
        if last_commit.checked_add(1) != Some(first) || last < first {
            return Err(StorageError::corrupted(
                "execution commit sequence is not contiguous",
            ));
        }
        last_commit = last;
    }
    let recorded = tx
        .open_table(SEQUENCES)?
        .get(execution::EVENT_SEQUENCE_KEY)?
        .map_or(0, |value| value.value());
    if recorded != u128::from(last_commit) {
        return Err(StorageError::corrupted(
            "execution event counter disagrees with commits",
        ));
    }

    let mut sequences = Vec::new();
    let mut reserved = BTreeSet::new();
    let mut request_refs = BTreeSet::new();
    let mut output_refs = BTreeSet::new();
    let mut tool_refs = BTreeMap::<(u128, u32, u32), bool>::new();
    let mut dependency_refs = BTreeMap::new();
    let mut input_refs = BTreeSet::new();
    for entry in events.iter()? {
        let (key, bytes) = entry?;
        let key = key.value();
        let event = execution::decode_event(key, bytes.value())?;
        if jobs.get(key.0)?.is_none() {
            require_job_exists(&conversations, key.0, "execution event")?;
        }
        let end = commits
            .get(event.transaction)?
            .ok_or_else(|| StorageError::corrupted("execution event has no committed transaction"))?
            .value();
        if event.seq < event.transaction || event.seq > end {
            return Err(StorageError::corrupted(
                "execution event is outside its transaction",
            ));
        }
        sequences.push(event.seq);
        match &event.kind {
            ExecutionEventKind::InputRevisionRecorded { revision } => {
                if !input_refs.insert((key.0, *revision)) {
                    return Err(StorageError::corrupted(
                        "duplicate historical input revision",
                    ));
                }
            }
            ExecutionEventKind::AllocationReserved { allocation } => {
                if allocation.token.job.get() != key.0
                    || allocation.phase != AllocationPhase::Reserved
                    || allocation.started_at.is_some()
                    || allocation.stop_confirmed_at.is_some()
                    || allocation.stop_proof.is_some()
                    || !reserved.insert(key.0)
                {
                    return Err(StorageError::corrupted(
                        "Job has an invalid or duplicate reservation event",
                    ));
                }
                // The payload pass already decoded these rows in this snapshot.
                jobs.get(key.0)?.ok_or_else(|| {
                    StorageError::corrupted("reservation references a missing Job")
                })?;
                let pools = tx.open_table(POOLS)?;
                let executors = tx.open_table(EXECUTORS)?;
                pools
                    .get(allocation.token.pool.get())?
                    .ok_or_else(|| StorageError::corrupted("reservation Pool is missing"))?;
                executors
                    .get(allocation.token.executor.get())?
                    .ok_or_else(|| StorageError::corrupted("reservation Executor is missing"))?;
            }
            ExecutionEventKind::RequestRecorded { request, seq } => {
                if !reserved.contains(&key.0) || !request_refs.insert((key.0, request.get(), *seq))
                {
                    return Err(StorageError::corrupted(
                        "request event precedes reservation or duplicates its evidence",
                    ));
                }
            }
            ExecutionEventKind::ToolRecorded { call, completed } => {
                if call.job.get() != key.0 || !reserved.contains(&key.0) {
                    return Err(StorageError::corrupted(
                        "tool event has incorrect Job ownership or precedes reservation",
                    ));
                }
                *tool_refs.entry(rows::tool_call_key(*call)).or_default() |= *completed;
            }
            ExecutionEventKind::OutputRecorded { seq } => {
                if !output_refs.insert((key.0, *seq)) {
                    return Err(StorageError::corrupted(
                        "output has duplicate execution events",
                    ));
                }
            }
            ExecutionEventKind::DependencyAdded {
                prerequisite,
                condition,
            } => {
                if jobs.get(prerequisite.get())?.is_none() {
                    require_job_exists(&conversations, prerequisite.get(), "execution dependency")?;
                }
                if dependency_refs
                    .insert((prerequisite.get(), key.0), *condition)
                    .is_some()
                {
                    return Err(StorageError::corrupted("duplicate historical dependency"));
                }
            }
            ExecutionEventKind::DependencyResolved {
                prerequisite,
                state,
            } => {
                let condition = dependency_refs
                    .get(&(prerequisite.get(), key.0))
                    .ok_or_else(|| {
                        StorageError::corrupted("dependency resolution has no declaration")
                    })?;
                let source = execution::replay_job(tx, *prerequisite, Some(end))?
                    .ok_or_else(|| StorageError::corrupted("dependency source has no history"))?;
                if Batch::dependency_outcome_state(source.outcome.as_ref(), *condition) != *state
                    || *state == DependencyState::Waiting
                {
                    return Err(StorageError::corrupted(
                        "dependency resolution precedes its authoritative outcome",
                    ));
                }
            }
            ExecutionEventKind::Created { .. }
            | ExecutionEventKind::PhaseChanged { .. }
            | ExecutionEventKind::AllocationStarted
            | ExecutionEventKind::AllocationPhaseChanged { .. }
            | ExecutionEventKind::AllocationStopConfirmed { .. }
            | ExecutionEventKind::JobOutcome { .. }
            | ExecutionEventKind::Closing { .. }
            | ExecutionEventKind::ClosingRunningChanged { .. }
            | ExecutionEventKind::HoldChanged { .. }
            | ExecutionEventKind::VisibilityChanged { .. }
            | ExecutionEventKind::PrerequisitesChanged { .. } => {}
        }
    }
    sequences.sort_unstable();
    if sequences.len() as u128 != u128::from(last_commit)
        || sequences
            .iter()
            .enumerate()
            .any(|(index, seq)| u128::from(*seq) != index as u128 + 1)
    {
        return Err(StorageError::corrupted(
            "execution event order has a gap or duplicate",
        ));
    }
    for entry in tx.open_table(REQUEST_EVENTS)?.iter()? {
        let (key, _) = entry?;
        if !request_refs.remove(&key.value()) {
            return Err(StorageError::corrupted(
                "request evidence has no execution event",
            ));
        }
    }
    for entry in tx.open_table(JOB_OUTPUTS)?.iter()? {
        let (key, _) = entry?;
        if !output_refs.remove(&key.value()) {
            return Err(StorageError::corrupted("output has no execution event"));
        }
    }
    for entry in tx.open_table(TOOL_CALLS)?.iter()? {
        let (key, bytes) = entry?;
        let key = key.value();
        let evidence = rows::decode_tool_evidence(rows::tool_call_ref(key)?, bytes.value())?;
        if tool_refs.remove(&key) != Some(evidence.outcome.is_some()) {
            return Err(StorageError::corrupted(
                "tool evidence disagrees with execution history",
            ));
        }
    }
    for entry in tx.open_table(DEPENDENCY_WAITERS)?.iter()? {
        let (key, value) = entry?;
        if dependency_refs
            .remove(&key.value())
            .map(|condition| condition.code())
            != Some(value.value())
        {
            return Err(StorageError::corrupted(
                "dependency index disagrees with execution history",
            ));
        }
    }
    if !dependency_refs.is_empty() {
        return Err(StorageError::corrupted(
            "execution history has an unindexed dependency",
        ));
    }
    for entry in tx.open_table(JOB_INPUTS)?.iter()? {
        let (key, _) = entry?;
        let (job, revision) = key.value();
        if !input_refs.remove(&(job, u64::from(revision))) {
            return Err(StorageError::corrupted(
                "input revision has no execution event",
            ));
        }
    }
    if !request_refs.is_empty() || !output_refs.is_empty() || !tool_refs.is_empty() {
        return Err(StorageError::corrupted(
            "execution history references missing evidence",
        ));
    }

    for entry in jobs.iter()?.chain(conversations.iter()?) {
        let (key, _) = entry?;
        let id = JobId::new(key.value());
        let snapshot = execution::replay_job(tx, id, None)?
            .ok_or_else(|| StorageError::corrupted("Job has no execution history"))?;
        let row = states
            .get(id.get())?
            .ok_or_else(|| StorageError::corrupted("Job state projection is missing"))?;
        let runtime = JobRuntime::decode_at(id.get(), row.value())?;
        let state = &runtime.state;
        if snapshot.outcome != state.outcome {
            return Err(StorageError::corrupted(
                "Job state projection disagrees with execution history",
            ));
        }
        let mutable = jobs
            .get(id.get())?
            .map(|bytes| jobs::decode_mutable_job(id.get(), bytes.value()))
            .transpose()?;
        let phase = mutable.as_ref().map(|job| job.phase);
        if snapshot.input_revision != mutable.as_ref().map(|job| job.revision)
            || mutable
                .as_ref()
                .is_some_and(|job| !input_refs.remove(&(id.get(), job.revision)))
        {
            return Err(StorageError::corrupted(
                "current input revision disagrees with execution history",
            ));
        }
        if let Some(job) = mutable.as_ref() {
            let expected = if job.phase != MutableJobPhase::Unsubmitted
                && snapshot
                    .outcome
                    .as_ref()
                    .is_none_or(|outcome| outcome.kind == OutcomeKind::Unknown)
            {
                let waiting = u32::try_from(snapshot.dependencies.len())
                    .map_err(|_| StorageError::corrupted("native dependency count overflow"))?;
                Some((waiting, 0))
            } else {
                None
            };
            if runtime.pending != expected {
                return Err(StorageError::corrupted(
                    "native Job unsettled counters disagree with execution history",
                ));
            }
        }
        let allocation = allocations
            .get(id.get())?
            .map(|bytes| allocations::decode_pool_allocation(id.get(), bytes.value()))
            .transpose()?;
        if phase != snapshot.phase
            || allocation != snapshot.allocation
            || allocation.is_some() != reserved.contains(&id.get())
        {
            return Err(StorageError::corrupted(
                "Job phase or Allocation projection disagrees with execution history",
            ));
        }
        if let Some(allocation) = allocation.as_ref()
            && snapshot
                .outcome
                .as_ref()
                .is_some_and(|outcome| outcome.kind != OutcomeKind::Unknown)
        {
            if allocation.holds_capacity() {
                return Err(StorageError::corrupted(
                    "settled Job still holds an Allocation",
                ));
            }
            check_settled_job_effects(tx, id)?;
        }
        let lifecycle = &runtime.lifecycle;
        if snapshot.closing != lifecycle.closing
            || snapshot.on_hold != lifecycle.on_hold
            || snapshot.hidden_at != lifecycle.hidden_at
            || snapshot.prerequisites != lifecycle.after
        {
            return Err(StorageError::corrupted(
                "Job lifecycle projection disagrees with execution history",
            ));
        }
        for (prerequisite, condition) in &snapshot.dependencies {
            let source = states.get(prerequisite.get())?.ok_or_else(|| {
                StorageError::corrupted("unresolved dependency source is missing")
            })?;
            let source = state::decode_state(prerequisite.get(), source.value())?;
            if Batch::dependency_outcome_state(source.outcome.as_ref(), *condition)
                != DependencyState::Waiting
            {
                return Err(StorageError::corrupted(
                    "settled dependency has no resolution event",
                ));
            }
        }
    }
    if !input_refs.is_empty() {
        return Err(StorageError::corrupted(
            "execution history references missing input revisions",
        ));
    }
    Ok(())
}

fn check_settled_job_effects(tx: &ReadTransaction, job: JobId) -> Result<(), StorageError> {
    use crate::db::entities::{
        AvailabilityClassification, AvailabilityObservation, RequestNumber, RequestPayload,
    };

    let mut requests = std::collections::BTreeMap::<u32, (bool, bool)>::new();
    for entry in tx
        .open_table(REQUEST_EVENTS)?
        .range((job.get(), 0, 0)..=(job.get(), u32::MAX, u32::MAX))?
    {
        let (key, row) = entry?;
        let (_, request, seq) = key.value();
        let number = RequestNumber::new(request)
            .ok_or_else(|| StorageError::corrupted("zero request identity"))?;
        let state = requests.entry(request).or_default();
        match rows::decode_request_event(job, number, seq, row.value())?.payload {
            RequestPayload::Sent { .. } => state.0 = true,
            RequestPayload::Received { .. } => state.1 = true,
            RequestPayload::Availability {
                status_code,
                finish_reason,
                raw_finish_reason,
                failure,
            } => {
                let observation = AvailabilityObservation {
                    status_code,
                    finish_reason,
                    raw_finish_reason,
                    failure,
                };
                state.1 |= matches!(
                    observation.classification(),
                    AvailabilityClassification::Rejected(_)
                );
            }
            RequestPayload::Measurement { .. } | RequestPayload::Token { .. } => {}
        }
    }
    if requests
        .values()
        .any(|&(sent, confirmed)| !sent || !confirmed)
    {
        return Err(StorageError::corrupted(
            "settled Job contains an unconfirmed provider request",
        ));
    }
    for entry in tx
        .open_table(TOOL_CALLS)?
        .range((job.get(), 0, 0)..=(job.get(), u32::MAX, u32::MAX))?
    {
        let (key, row) = entry?;
        let tool = rows::decode_tool_evidence(rows::tool_call_ref(key.value())?, row.value())?;
        if tool.admission().is_some()
            && tool.reconciliation.is_none()
            && tool
                .outcome
                .as_ref()
                .is_none_or(|outcome| !outcome.fact.is_definite())
        {
            return Err(StorageError::corrupted(
                "settled Job contains an unconfirmed admitted tool",
            ));
        }
    }
    Ok(())
}

fn refuse_unresolved_ownership(tx: &ReadTransaction) -> Result<(), StorageError> {
    let ownership = residual_ownership_in(tx)?;
    if ownership.unresolved() {
        return Err(held_ownership(
            ownership.held.len() as u64,
            ownership.retained_unknown.len() as u64,
        ));
    }
    Ok(())
}

fn unknown_outcomes_exist(tx: &impl TxRead) -> Result<bool, StorageError> {
    for entry in tx.table(JOB_RUNTIME)?.iter()? {
        let (key, value) = entry?;
        if state::decode_state(key.value(), value.value())?
            .outcome
            .is_some_and(|outcome| outcome.kind == OutcomeKind::Unknown)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn resolved_unknown_ownership(
    tx: &ReadTransaction,
    state: &JobState,
) -> Result<bool, StorageError> {
    use crate::db::entities::{AllocationPhase, MutableJobPhase};

    let Some(outcome) = state
        .outcome
        .as_ref()
        .filter(|outcome| outcome.kind == OutcomeKind::Unknown)
    else {
        return Ok(false);
    };
    let Some(row) = tx.open_table(POOL_ALLOCATIONS)?.get(state.job_id.get())? else {
        return Ok(false);
    };
    let allocation = allocations::decode_pool_allocation(state.job_id.get(), row.value())?;
    drop(row);
    if allocation.phase != AllocationPhase::Released
        || allocation.started_at.is_none()
        || allocation.stop_proof.is_none()
        || allocation
            .stop_confirmed_at
            .is_none_or(|confirmed| confirmed < outcome.at)
        || allocation.job_revision == 0
    {
        return Ok(false);
    }
    Ok(
        reads::jobs::mutable_job_record_in(tx, state.job_id)?.is_some_and(|record| {
            record.job.phase == MutableJobPhase::Executing
                && record.job.revision == allocation.job_revision
        }),
    )
}

fn held_ownership(held: u64, unknown: u64) -> StorageError {
    StorageError {
        kind: StorageErrorKind::Other,
        os_error: None,
        detail: format!(
            "refusing to start: {held} Allocation(s) still hold capacity and {unknown} Job outcome(s) are Unknown. Inspect with `ocg inspect-held <data-directory>`. To plan offline release, run `ocg reconcile-held <data-directory> --dry-run`"
        ),
    }
}

pub(super) fn check_preflight_header(bytes: &[u8], tags: &[u8]) -> Result<(), StorageError> {
    let valid = match bytes {
        [tag, version, ..] if tags.contains(tag) => u64::from(*version) == FORMAT_VERSION,
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(unsupported_format(
            "unsupported or malformed record encoding",
        ))
    }
}

fn require_job_exists(
    jobs: &impl ReadableTable<u128, &'static [u8]>,
    job_id: u128,
    context: &str,
) -> Result<(), StorageError> {
    if jobs.get(job_id)?.is_none() {
        return Err(StorageError::corrupted(format!(
            "{context} references missing job {job_id}"
        )));
    }
    Ok(())
}

fn check_pair_job_refs(
    jobs: &impl ReadableTable<u128, &'static [u8]>,
    tx: &ReadTransaction,
    definition: TableDefinition<(u128, u128), ()>,
    context: &str,
) -> Result<(), StorageError> {
    for entry in tx.open_table(definition)?.iter()? {
        let (key, _) = entry?;
        let (first, second) = key.value();
        require_job_exists(jobs, first, context)?;
        require_job_exists(jobs, second, context)?;
    }
    Ok(())
}

fn check_record_payloads(tx: &ReadTransaction) -> Result<(), StorageError> {
    let jobs = tx.open_table(JOBS)?;
    let conversations = tx.open_table(CONVERSATIONS)?;
    let states = tx.open_table(JOB_RUNTIME)?;
    let configs = tx.open_table(CONFIGS)?;
    let pools = tx.open_table(POOLS)?;
    let executors = tx.open_table(EXECUTORS)?;
    for entry in configs.iter()? {
        let (key, value) = entry?;
        ConfigRow::decode_at(key.value(), value.value())?;
    }
    for entry in pools.iter()? {
        let (key, value) = entry?;
        allocations::decode_allocation_pool(key.value(), value.value())?;
        if configs.get(key.value())?.is_some() {
            return Err(StorageError::corrupted(
                "Pool and config identities overlap",
            ));
        }
    }
    for entry in executors.iter()? {
        let (key, value) = entry?;
        let executor = allocations::decode_allocation_executor(key.value(), value.value())?;
        if pools.get(executor.pool.get())?.is_none() {
            return Err(StorageError::corrupted(
                "Executor references a missing Pool",
            ));
        }
        if configs.get(key.value())?.is_some() || pools.get(key.value())?.is_some() {
            return Err(StorageError::corrupted(
                "Executor identity overlaps another resource",
            ));
        }
    }
    for entry in jobs.iter()? {
        let (key, value) = entry?;
        let job_id = key.value();
        jobs::decode_mutable_job(job_id, value.value())?;
        if states.get(job_id)?.is_none() {
            return Err(StorageError::corrupted(format!(
                "job {job_id} has no matching job_state record"
            )));
        }
    }
    for entry in tx.open_table(POOL_ALLOCATIONS)?.iter()? {
        let (key, value) = entry?;
        let allocation = allocations::decode_pool_allocation(key.value(), value.value())?;
        if jobs.get(allocation.token.job.get())?.is_none() {
            return Err(StorageError::corrupted(format!(
                "pool_allocation references missing job {}",
                allocation.token.job
            )));
        }
        if pools.get(allocation.token.pool.get())?.is_none() {
            return Err(StorageError::corrupted(
                "pool_allocation references missing Pool",
            ));
        }
        if executors.get(allocation.token.executor.get())?.is_none() {
            return Err(StorageError::corrupted(
                "pool_allocation references missing Executor",
            ));
        }
    }
    for entry in tx.open_table(SUBMISSIONS)?.iter()? {
        let (key, value) = entry?;
        let submission = rows::decode_submission(key.value(), value.value())?;
        require_job_exists(&jobs, submission.job.get(), "submission")?;
    }
    for entry in tx.open_table(JOB_OUTPUTS)?.iter()? {
        let (key, value) = entry?;
        let output_key = key.value();
        let output = rows::decode_output(output_key, value.value())?;
        require_job_exists(&jobs, output_key.0, "job_output")?;
        if output.kind == OutputKind::Result {
            let state = states
                .get(output_key.0)?
                .ok_or_else(|| StorageError::corrupted("result Job state is missing"))?;
            let state = state::decode_state(output_key.0, state.value())?;
            if !state
                .outcome
                .as_ref()
                .is_some_and(|outcome| outcome.kind != OutcomeKind::Unknown)
            {
                return Err(StorageError::corrupted(
                    "result output has no confirmed Job outcome",
                ));
            }
        }
    }
    for entry in tx.open_table(ATTACHMENTS)?.iter()? {
        let (key, value) = entry?;
        rows::decode_attachment(key.value(), value.value())?;
    }
    let tool_calls = tx.open_table(TOOL_CALLS)?;
    for entry in tool_calls.iter()? {
        let (key, value) = entry?;
        let call = rows::tool_call_ref(key.value())?;
        rows::decode_tool_evidence(call, value.value())?;
        require_job_exists(&jobs, call.job.get(), "tool_call")?;
    }
    let request_events = tx.open_table(REQUEST_EVENTS)?;
    for entry in request_events.iter()? {
        let (key, value) = entry?;
        let (job_u128, request_u32, seq) = key.value();
        let request = crate::db::entities::RequestNumber::new(request_u32)
            .ok_or_else(|| StorageError::corrupted("invalid request number in request_event"))?;
        rows::decode_request_event(JobId::new(job_u128), request, seq, value.value())?;
        require_job_exists(&jobs, job_u128, "request_event")?;
    }
    for entry in tx.open_table(REQUEST_TIMELINE)?.iter()? {
        let (key, _) = entry?;
        let (_, job_u128, request_u32, seq) = key.value();
        if request_events.get((job_u128, request_u32, seq))?.is_none() {
            return Err(StorageError::corrupted(
                "request_timeline references missing request_event",
            ));
        }
    }
    for entry in tx.open_table(PROJECTS)?.iter()? {
        let (key, value) = entry?;
        rows::decode_project(key.value(), value.value())?;
    }
    for entry in tx.open_table(JOB_INPUTS)?.iter()? {
        let (key, value) = entry?;
        let (job_u128, second) = key.value();
        require_job_exists(&jobs, job_u128, "job_input")?;
        jobs::decode_mutable_input_revision(JobId::new(job_u128), second, value.value())?;
    }
    for entry in tx.open_table(RECURRENCES)?.iter()? {
        let (key, value) = entry?;
        let recurrence = Recurrence::decode(value.value())?;
        require_job_exists(&conversations, key.value(), "recurrence conversation")?;
        require_job_exists(&jobs, recurrence.current_turn.get(), "recurrence turn")?;
    }
    for entry in tx.open_table(APPROVALS)?.iter()? {
        let (key, value) = entry?;
        ApprovalRecord::decode(value.value())?;
        let approval_key = key.value();
        require_job_exists(&jobs, approval_key.0, "approval")?;
        if tool_calls.get(approval_key)?.is_none() {
            return Err(StorageError::corrupted(
                "approval references missing tool_call",
            ));
        }
    }
    for entry in tx.open_table(CREDENTIALS)?.iter()? {
        let (key, value) = entry?;
        rows::decode_credential(key.value(), value.value())?;
    }
    let policies = tx.open_table(SCHEDULED_EXTERNAL_POLICY)?;
    for entry in policies.iter()? {
        let (key, value) = entry?;
        codec::decode_scheduled_external_policy(key.value(), value.value())?;
    }
    for entry in tx.open_table(SCHEDULED_EXTERNAL_BINDING)?.iter()? {
        let (key, value) = entry?;
        let binding = codec::decode_scheduled_external_binding(key.value(), value.value())?;
        require_job_exists(&jobs, binding.job.get(), "scheduled_external_binding")?;
        if policies.get(binding.policy.get())?.is_none() {
            return Err(StorageError::corrupted(
                "scheduled_external_binding references missing policy",
            ));
        }
    }
    for entry in conversations.iter()? {
        let (key, bytes) = entry?;
        JobSpec::decode_at(key.value(), bytes.value())?;
        if states.get(key.value())?.is_none() {
            return Err(StorageError::corrupted("conversation state is missing"));
        }
    }
    for entry in tx.open_table(TURNS)?.iter()? {
        let (key, _) = entry?;
        let (conv, turn) = key.value();
        if conversations.get(conv)?.is_none() {
            return Err(StorageError::corrupted(
                "turn references missing conversation",
            ));
        }
        require_job_exists(&jobs, turn, "turn")?;
    }
    check_pair_job_refs(&conversations, tx, CHILD_GROUPS, "child_group")?;
    check_pair_job_refs(&conversations, tx, LIVE_CHILDREN, "live_child")?;
    for entry in tx.open_table(CHILDREN)?.iter()? {
        let (key, _) = entry?;
        let (parent, child) = key.value();
        if parent != 0 {
            require_job_exists(&conversations, parent, "children parent")?;
        }
        if jobs.get(child)?.is_none() {
            require_job_exists(&conversations, child, "children child")?;
        }
    }
    for entry in tx.open_table(DEPENDENCY_WAITERS)?.iter()? {
        let (key, _) = entry?;
        let (dep, waiter) = key.value();
        if jobs.get(dep)?.is_none() {
            require_job_exists(&conversations, dep, "dependency_waiter dependency")?;
        }
        require_job_exists(&jobs, waiter, "dependency_waiter waiter")?;
    }
    Ok(())
}

struct PreflightDirectory {
    path: PathBuf,
    identity: std::fs::Metadata,
    copy_identity: Option<std::fs::Metadata>,
}
impl Drop for PreflightDirectory {
    fn drop(&mut self) {
        let Ok(directory) = std::fs::symlink_metadata(&self.path) else {
            return;
        };
        if !directory.is_dir() || !same_file_identity(&self.identity, &directory) {
            return;
        }
        let copy = self.path.join("copy.redb");
        if let (Some(expected), Ok(actual)) =
            (&self.copy_identity, std::fs::symlink_metadata(&copy))
            && actual.is_file()
            && same_file_identity(expected, &actual)
        {
            let _ = std::fs::remove_file(copy);
        }
        let _ = std::fs::remove_dir(&self.path);
    }
}

fn preflight(
    file: &std::fs::File,
    path: &Path,
    directory: &Path,
) -> Result<Verification, StorageError> {
    match ::redb::ReadOnlyDatabase::open(path) {
        Ok(database) => validate_preflight(&database),
        Err(::redb::DatabaseError::RepairAborted) => {
            let scratch = directory.join(format!(".ocg-preflight-{}", uuid::Uuid::now_v7()));
            let mut mkdir = std::fs::DirBuilder::new();
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                mkdir.mode(0o700);
            }
            mkdir.create(&scratch).map_err(io_storage_error)?;
            let identity = std::fs::symlink_metadata(&scratch).map_err(io_storage_error)?;
            let mut cleanup = PreflightDirectory {
                path: scratch,
                identity,
                copy_identity: None,
            };
            let copy_path = cleanup.path.join("copy.redb");
            let mut options = std::fs::OpenOptions::new();
            options.read(true).write(true).create_new(true);
            #[cfg(windows)]
            {
                use std::os::windows::fs::OpenOptionsExt;
                const GENERIC_READ: u32 = 0x8000_0000;
                const GENERIC_WRITE: u32 = 0x4000_0000;
                const DELETE: u32 = 0x0001_0000;
                const DELETE_ON_CLOSE: u32 = 0x0400_0000;
                options.access_mode(GENERIC_READ | GENERIC_WRITE | DELETE);
                options.custom_flags(DELETE_ON_CLOSE);
            }
            let copy = crate::host::data_directory::private_file(&mut options)
                .open(&copy_path)
                .map_err(io_storage_error)?;
            cleanup.copy_identity = Some(copy.metadata().map_err(io_storage_error)?);
            #[cfg(windows)]
            {
                cleanup.copy_identity = None;
            }
            #[cfg(unix)]
            {
                std::fs::remove_file(&copy_path).map_err(io_storage_error)?;
                cleanup.copy_identity = None;
            }
            let copy = offline::copy_database(
                file,
                copy,
                file.metadata().map_err(io_storage_error)?.len(),
                "database changed during preflight copy",
            )?;
            let database = Database::builder().create_file(copy)?;
            validate_preflight(&database).map(|_| Verification::Full)
        }
        Err(error) => Err(error.into()),
    }
}

fn initialize(database: &Database, fresh: bool) -> Result<u64, StorageError> {
    reservations::init_promotion_clock(Timestamp::now().as_unix_micros());
    let mut excluded = std::collections::BTreeSet::new();
    let batch = loop {
        let mut batch = initialize_batch(database, fresh)?;
        match settle::settle_startup(&mut batch, &excluded) {
            Ok(()) => break batch,
            Err(settle::StartupSettlementError::Storage(error)) => return Err(error),
            Err(settle::StartupSettlementError::Rejected(target, reason)) => {
                // A settlement can reject after writing. Retry from the committed
                // snapshot so the excluded object keeps its facts and ownership.
                batch.tx.abort()?;
                if !excluded.insert(target) {
                    return Err(StorageError::corrupted(format!(
                        "startup settlement attempted excluded object {target:?}"
                    )));
                }
                eprintln!("OCG startup settlement deferred {target:?}: {reason}");
            }
        }
    };
    let epoch = batch.epoch;
    batch.tx.commit()?;
    Ok(epoch)
}

fn initialize_batch(database: &Database, fresh: bool) -> Result<Batch, StorageError> {
    let mut tx = database.begin_write()?;
    tx.set_durability(Durability::Immediate)?;

    if fresh {
        create_tables(&tx)?;
        let settings = crate::db::entities::GlobalCompactionSettings::default();
        let mut encoded = Enc::new(TAG_GLOBAL_COMPACTION);
        encoded.u64(settings.revision);
        encoded.bool(settings.prediction_enabled);
        encoded.compaction_policy(&settings.policy);
        let mut meta = tx.open_table(META)?;
        meta.insert(DATABASE_FORMAT_KEY, FORMAT_VERSION.to_le_bytes().as_slice())?;
        meta.insert(SCHEMA_FINGERPRINT_KEY, SCHEMA_FINGERPRINT)?;
        meta.insert("compaction_settings", encoded.0.as_slice())?;
        meta.insert("open_epoch", 0u64.to_le_bytes().as_slice())?;
        meta.insert("clean_shutdown", [1u8].as_slice())?;
    }

    let epoch = {
        let mut meta = tx.open_table(META)?;
        check_identity(&meta)?;
        let previous = meta
            .get("open_epoch")?
            .and_then(|value| <[u8; 8]>::try_from(value.value()).ok())
            .map(u64::from_le_bytes)
            .ok_or_else(|| StorageError::corrupted("missing or invalid open epoch"))?;
        let epoch = previous
            .checked_add(1)
            .ok_or_else(|| StorageError::corrupted("open epoch overflow"))?;
        meta.insert("open_epoch", epoch.to_le_bytes().as_slice())?;
        meta.insert("clean_shutdown", [0u8].as_slice())?;
        epoch
    };

    if fresh {
        let pool_id = ExecutionPoolId::new(allocate(&tx)?);
        let executor_id = ExecutorId::new(allocate(&tx)?);
        let pool = crate::db::entities::AllocationPool {
            id: pool_id,
            name: "Default".to_owned(),
            capacity: std::num::NonZeroU32::MIN,
            enabled: true,
            archived: false,
            revision: 1,
        };
        let executor = crate::db::entities::AllocationExecutor {
            id: executor_id,
            pool: pool_id,
            name: "Local Executor".to_owned(),
            enabled: true,
            archived: false,
            revision: 1,
            capabilities: [(
                ExecutionOperation::LlmRequest,
                Some(LlmTransport::OpenAiChatCompletions),
            )]
            .into_iter()
            .collect(),
        };
        tx.open_table(POOLS)?.insert(
            pool_id.get(),
            allocations::encode_allocation_pool(&pool).as_slice(),
        )?;
        tx.open_table(EXECUTORS)?.insert(
            executor_id.get(),
            allocations::encode_allocation_executor(&executor).as_slice(),
        )?;
        tx.open_table(POOL_EXECUTORS)?
            .insert((pool_id.get(), executor_id.get()), ())?;
    }

    let mut batch = Batch::new(tx, epoch);
    batch.commit_mode = Some(Commit::Durable);
    Ok(batch)
}

pub(super) fn allocate(tx: &WriteTransaction) -> Result<u128, StorageError> {
    let mut table = tx.open_table(SEQUENCES)?;
    let previous = table.get("entity")?.map_or(0, |value| value.value());
    let fresh = uuid::Uuid::now_v7().as_u128();
    let next = if fresh > previous {
        fresh
    } else {
        let payload_mask = (1u128 << 74) - 1;
        let payload = (((previous >> 64) & 0xfff) << 62) | (previous & ((1u128 << 62) - 1));
        let next_payload = payload + 1;
        let millis = (previous >> 80) + (next_payload >> 74);
        if millis >= 1u128 << 48 {
            return Err(StorageError::corrupted("UUIDv7 allocation space exhausted"));
        }
        let payload = next_payload & payload_mask;
        (millis << 80)
            | (7u128 << 76)
            | ((payload >> 62) << 64)
            | (2u128 << 62)
            | (payload & ((1u128 << 62) - 1))
    };
    table.insert("entity", next)?;
    Ok(next)
}

pub(crate) struct ReclamationReport {
    pub(crate) details: Vec<(
        ContentDigest,
        Result<crate::db::attachments::Reclamation, crate::db::attachments::StoreError>,
    )>,
    pub(crate) omitted: u64,
}

pub(crate) fn reclaim_pending(
    database: &Database,
    attachments: &crate::db::attachments::AttachmentStore,
) -> Result<ReclamationReport, StorageError> {
    let mut tx = database.begin_write()?;
    tx.set_durability(Durability::Immediate)?;
    let mut after: Option<[u8; 32]> = None;
    let mut report = ReclamationReport {
        details: Vec::new(),
        omitted: 0,
    };
    loop {
        let page = {
            let queue = tx.open_table(CONTENT_RECLAIM)?;
            let start = after.as_ref().map_or(std::ops::Bound::Unbounded, |after| {
                std::ops::Bound::Excluded(after.as_slice())
            });
            let mut page = Vec::new();
            for entry in queue
                .range::<&[u8]>((start, std::ops::Bound::Unbounded))?
                .take(256)
            {
                let (key, value) = entry?;
                let digest: [u8; 32] = key
                    .value()
                    .try_into()
                    .map_err(|_| StorageError::corrupted("invalid content digest"))?;
                page.push((ContentDigest::from_bytes(digest), value.value()));
            }
            page
        };
        if page.is_empty() {
            break;
        }
        let found = attachments.reclaim_unneeded(&page);
        for (digest, _) in &page {
            after = Some(*digest.as_bytes());
            let failed = found.iter().any(|(candidate, result)| {
                candidate == digest
                    && !matches!(
                        result,
                        Ok(crate::db::attachments::Reclamation::Removed
                            | crate::db::attachments::Reclamation::Absent)
                    )
            });
            if !failed {
                tx.open_table(CONTENT_RECLAIM)?
                    .remove(digest.as_bytes().as_slice())?;
            }
        }
        for detail in found {
            if report.details.len() < MAX_PAGE {
                report.details.push(detail);
            } else {
                report.omitted = report.omitted.saturating_add(1);
            }
        }
    }
    tx.commit()?;
    Ok(report)
}

fn unsupported_format(detail: &str) -> StorageError {
    StorageError {
        kind: StorageErrorKind::Other,
        os_error: None,
        detail: detail.to_owned(),
    }
}

pub(crate) fn record_clean_shutdown(database: &Database) -> Result<(), StorageError> {
    let tx = database.begin_write()?;
    let unknown = unknown_outcomes_exist(&tx)?;
    tx.open_table(META)?
        .insert("clean_shutdown", [u8::from(!unknown)].as_slice())?;
    tx.commit()?;
    Ok(())
}
