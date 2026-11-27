use super::{StorageError, execution};
use crate::db::entities::Timestamp;
use crate::db::redb::codec::DecodeResult;
use crate::db::redb::reads::read;
use crate::db::redb::recovery::allocate;
use crate::db::redb::rows::{decode_tool_evidence, tool_call_key};
use crate::db::redb::schema::{
    JOB_EVENTS, JOB_RUNTIME, JOBS, POOL_ALLOCATIONS, SUBMISSIONS, TOOL_CALLS,
};
use crate::db::redb::{allocations, state};
use ::redb::{
    CommitError, Database, Durability, Key, ReadableTable, TableDefinition, Value, WriteTransaction,
};

use crate::db::entities::{JobId, SubmissionKey, ToolCallRef, Violation};
use std::time::Instant;

#[derive(Clone, Debug)]
pub(crate) struct BatchError(pub(crate) Failure);

impl From<StorageError> for BatchError {
    fn from(error: StorageError) -> Self {
        Self(Failure::Storage(error))
    }
}

impl From<Violation> for BatchError {
    fn from(reason: Violation) -> Self {
        Self(Failure::Rejected(reason))
    }
}

#[derive(Clone, Debug)]
pub(crate) enum Failure {
    Rejected(Violation),
    Storage(StorageError),
}

#[derive(Debug)]
pub(crate) enum BatchFailure {
    Rejected(Violation),
    NotCommitted(StorageError),
    OutcomeUnknown(StorageError),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Probe {
    Submission(SubmissionKey),
    PoolAllocation(crate::db::entities::AllocationToken),
    MutableJobStarted(crate::db::entities::AllocationToken, u64),
    ToolCall(ToolCallRef),
    ToolOutcome(ToolCallRef),
    Outcome(JobId),
    ExecutionEvent(JobId, u64),
}

#[derive(Clone, Debug)]
pub(crate) enum NativeResourceDelta {
    Pool(crate::db::entities::AllocationPool),
    Executor(crate::db::entities::AllocationExecutor),
}

pub(crate) struct BatchOutcome<T> {
    pub(crate) result: Result<T, BatchFailure>,
    pub(crate) storage_failure: Option<StorageError>,
    pub(crate) needs_full_verification: bool,
    pub(crate) durable_commit_us: Option<u64>,
    pub(crate) scheduling_wake: bool,
    pub(crate) capacity_freed: bool,
    pub(crate) catalog_changed: bool,
    pub(crate) generative_config_changed: bool,
    pub(crate) probe: Option<Probe>,
    pub(crate) resource_deltas: Vec<NativeResourceDelta>,
}

impl<T> BatchOutcome<T> {
    fn failed_storage(error: StorageError) -> Self {
        Self::failed(BatchFailure::NotCommitted(error.clone()), Some(error))
    }

    fn failed(failure: BatchFailure, storage_failure: Option<StorageError>) -> Self {
        Self {
            result: Err(failure),
            storage_failure,
            needs_full_verification: false,
            durable_commit_us: None,
            scheduling_wake: false,
            capacity_freed: false,
            catalog_changed: false,
            generative_config_changed: false,
            probe: None,
            resource_deltas: Vec::new(),
        }
    }
}

pub(crate) struct Batch {
    pub(super) tx: WriteTransaction,
    pub(super) now: Timestamp,
    pub(super) recovery_id_seed: Option<u128>,
    pub(super) epoch: u64,
    pub(super) commit_mode: Option<Commit>,
    pub(super) touched_ready_or_delayed: bool,
    pub(super) freed_capacity: bool,
    pub(super) changed_pool: bool,
    pub(super) catalog_changed: bool,
    pub(super) generative_config_changed: bool,
    pub(super) probe: Option<Probe>,
    pub(super) resource_deltas: Vec<NativeResourceDelta>,
    pub(super) execution_transaction: Option<u64>,
    pub(super) needs_full_verification: bool,
    pub(crate) native_stops: Option<crate::db::NativeStops>,
    failure: Option<Failure>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Commit {
    Durable,
    Staged,
}

fn begin(database: &Database, epoch: u64, commit: Commit) -> Result<Batch, StorageError> {
    let mut tx = database.begin_write()?;
    let durability = match commit {
        Commit::Durable => Durability::Immediate,
        Commit::Staged => Durability::None,
    };
    if let Err(error) = tx.set_durability(durability) {
        let error = StorageError::from(error);
        let abort = tx.abort();
        drop(abort);
        return Err(error);
    }
    let mut batch = Batch::new(tx, epoch);
    batch.commit_mode = Some(commit);
    Ok(batch)
}

pub(crate) fn run_batch_with<T>(
    database: &Database,
    epoch: u64,
    commit: Commit,
    body: impl FnOnce(&mut Batch) -> Result<T, BatchError>,
) -> BatchOutcome<T> {
    run_batch_declinable(database, epoch, commit, |batch| {
        body(batch).map(std::ops::ControlFlow::Continue)
    })
    .0
}

pub(crate) fn run_batch_declinable<T>(
    database: &Database,
    epoch: u64,
    commit: Commit,
    body: impl FnOnce(&mut Batch) -> Result<std::ops::ControlFlow<T, T>, BatchError>,
) -> (BatchOutcome<T>, bool) {
    use std::ops::ControlFlow::{Break, Continue};
    let mut batch = match begin(database, epoch, commit) {
        Ok(batch) => batch,
        Err(error) => return (BatchOutcome::failed_storage(error), false),
    };
    let result = body(&mut batch);
    let needs_full_verification = batch.needs_full_verification;
    let result = match batch.failure.take() {
        Some(failure) => Err(BatchError(failure)),
        None => result,
    };
    let (mut outcome, declined) = match result {
        Err(BatchError(failure)) => (finish_abort(batch.tx, failure), false),
        Ok(Continue(value)) => (commit_batch(batch, value, commit), false),
        Ok(Break(value)) => match batch.tx.abort() {
            Ok(()) => (
                BatchOutcome {
                    result: Ok(value),
                    storage_failure: None,
                    needs_full_verification: false,
                    durable_commit_us: None,
                    scheduling_wake: false,
                    capacity_freed: false,
                    catalog_changed: false,
                    generative_config_changed: false,
                    probe: None,
                    resource_deltas: Vec::new(),
                },
                true,
            ),
            Err(error) => (BatchOutcome::failed_storage(error.into()), false),
        },
    };
    // A failed read can discover damage already present in the committed snapshot.
    outcome.needs_full_verification = needs_full_verification;
    (outcome, declined)
}

fn commit_batch<T>(batch: Batch, value: T, commit: Commit) -> BatchOutcome<T> {
    let started = (commit == Commit::Durable).then(Instant::now);
    let result = batch.tx.commit();
    let durable_commit_us = started.map(|started| started.elapsed().as_micros() as u64);
    match result {
        Ok(()) => BatchOutcome {
            result: Ok(value),
            storage_failure: None,
            needs_full_verification: false,
            durable_commit_us,
            scheduling_wake: batch.touched_ready_or_delayed || batch.changed_pool,
            capacity_freed: batch.freed_capacity,
            catalog_changed: batch.catalog_changed,
            generative_config_changed: batch.generative_config_changed,
            probe: batch.probe,
            resource_deltas: batch.resource_deltas,
        },
        Err(CommitError::TransactionPoisoned) => {
            let error = StorageError::from(CommitError::TransactionPoisoned);
            BatchOutcome::failed(BatchFailure::NotCommitted(error.clone()), Some(error))
        }
        Err(error) => {
            let error = StorageError::from(error);
            let mut outcome =
                BatchOutcome::failed(BatchFailure::OutcomeUnknown(error.clone()), Some(error));
            outcome.probe = batch.probe;
            outcome
        }
    }
}

pub(crate) fn persist_staged(database: &Database) -> Result<u64, StorageError> {
    let mut tx = database.begin_write()?;
    tx.set_durability(Durability::Immediate)?;
    let started = Instant::now();
    tx.commit()?;
    Ok(started.elapsed().as_micros() as u64)
}

pub(crate) fn allocated_bytes(database: &Database) -> Result<u64, StorageError> {
    let tx = database.begin_write()?;
    let stats = tx.stats()?;
    tx.abort()?;
    let page_size = u64::try_from(stats.page_size()).unwrap_or(u64::MAX);
    Ok(stats.allocated_pages().saturating_mul(page_size))
}

#[derive(Debug)]
pub(crate) enum CompactFailure {
    TransactionInProgress,
    SavepointExists,
    Storage(StorageError),
}

pub(crate) fn compact(database: &mut Database) -> Result<bool, CompactFailure> {
    match database.compact() {
        Ok(moved) => Ok(moved),
        Err(::redb::CompactionError::TransactionInProgress) => {
            Err(CompactFailure::TransactionInProgress)
        }
        Err(
            ::redb::CompactionError::PersistentSavepointExists
            | ::redb::CompactionError::EphemeralSavepointExists,
        ) => Err(CompactFailure::SavepointExists),
        Err(::redb::CompactionError::Storage(error)) => Err(CompactFailure::Storage(error.into())),
        Err(error) => Err(CompactFailure::Storage(::redb::Error::from(error).into())),
    }
}

pub(super) fn finish_abort<T>(tx: WriteTransaction, failure: Failure) -> BatchOutcome<T> {
    let abort_failure = tx.abort().err().map(StorageError::from);
    match failure {
        Failure::Rejected(violation) => {
            BatchOutcome::failed(BatchFailure::Rejected(violation), abort_failure)
        }
        Failure::Storage(error) => {
            BatchOutcome::failed(BatchFailure::NotCommitted(error.clone()), Some(error))
        }
    }
}

impl Batch {
    pub(crate) fn has_execution_events(&self) -> bool {
        self.execution_transaction.is_some()
    }

    pub(super) fn new(tx: WriteTransaction, epoch: u64) -> Self {
        Self {
            tx,
            now: Timestamp::now(),
            recovery_id_seed: None,
            epoch,
            commit_mode: None,
            touched_ready_or_delayed: false,
            freed_capacity: false,
            changed_pool: false,
            catalog_changed: false,
            generative_config_changed: false,
            probe: None,
            resource_deltas: Vec::new(),
            execution_transaction: None,
            needs_full_verification: false,
            native_stops: None,
            failure: None,
        }
    }

    pub(crate) fn now(&self) -> Timestamp {
        self.now
    }

    pub(crate) fn reject<T>(&mut self, violation: Violation) -> Result<T, BatchError> {
        Err(BatchError(Failure::Rejected(violation)))
    }

    pub(crate) fn corrupted<T>(&mut self, detail: String) -> Result<T, BatchError> {
        let failure = Failure::Storage(StorageError::corrupted(detail));
        self.remember_failure(&failure);
        Err(BatchError(failure))
    }

    fn remember_failure(&mut self, failure: &Failure) {
        if self.failure.is_none()
            || (matches!(self.failure, Some(Failure::Rejected(_)))
                && matches!(failure, Failure::Storage(_)))
        {
            self.failure = Some(failure.clone());
        }
    }

    pub(super) fn check_failure(&self) -> Result<(), BatchError> {
        match &self.failure {
            Some(failure) => Err(BatchError(failure.clone())),
            None => Ok(()),
        }
    }

    // Once writes start, even a caught business rejection must abort the batch.
    pub(super) fn atomic<T>(
        &mut self,
        body: impl FnOnce(&mut Self) -> Result<T, BatchError>,
    ) -> Result<T, BatchError> {
        self.check_failure()?;
        let result = body(self);
        if let Err(BatchError(failure)) = &result {
            self.remember_failure(failure);
        }
        result
    }

    pub(super) fn step<T>(
        &mut self,
        f: impl FnOnce(&WriteTransaction) -> Result<T, StorageError>,
    ) -> Result<T, BatchError> {
        self.inspect(|tx| Ok(f(tx)?))
    }

    pub(super) fn inspect<T>(
        &mut self,
        f: impl FnOnce(&WriteTransaction) -> Result<T, BatchError>,
    ) -> Result<T, BatchError> {
        self.check_failure()?;
        let result = f(&self.tx);
        if let Err(BatchError(failure @ Failure::Storage(_))) = &result {
            self.remember_failure(failure);
        }
        result
    }

    pub(super) fn insert<'k, 'v, K, V>(
        &mut self,
        table: TableDefinition<K, V>,
        key: K::SelfType<'k>,
        value: V::SelfType<'v>,
    ) -> Result<(), BatchError>
    where
        K: Key + 'static,
        V: Value + 'static,
    {
        self.step(|tx| {
            tx.open_table(table)?.insert(&key, &value)?;
            Ok(())
        })
    }

    pub(super) fn get<K, V, T>(
        &mut self,
        table: TableDefinition<K, V>,
        key: K::SelfType<'_>,
        decode: impl FnOnce(V::SelfType<'_>) -> DecodeResult<T>,
    ) -> Result<Option<T>, BatchError>
    where
        K: Key + 'static,
        V: Value + 'static,
    {
        self.step(|tx| {
            tx.open_table(table)?
                .get(&key)?
                .map(|bytes| decode(bytes.value()))
                .transpose()
        })
    }

    pub(super) fn remove<K, V>(
        &mut self,
        table: TableDefinition<K, V>,
        key: K::SelfType<'_>,
    ) -> Result<(), BatchError>
    where
        K: Key + 'static,
        V: Value + 'static,
    {
        self.step(|tx| {
            tx.open_table(table)?.remove(&key)?;
            Ok(())
        })
    }

    pub(crate) fn next_id(&mut self) -> Result<u128, BatchError> {
        let seed = self.recovery_id_seed;
        self.step(|tx| allocate(tx, seed))
    }
}

impl Batch {
    pub(crate) fn wake_scheduler(&mut self) {
        self.touched_ready_or_delayed = true;
    }
}

pub(crate) fn probe_committed(database: &Database, probe: Probe) -> Result<bool, StorageError> {
    let rx = read(database)?;
    Ok(match probe {
        Probe::Submission(key) => rx
            .open_table(SUBMISSIONS)?
            .get(key.as_bytes().as_slice())?
            .is_some(),
        Probe::PoolAllocation(token) => rx
            .open_table(POOL_ALLOCATIONS)?
            .get(token.job.get())?
            .map(|bytes| allocations::decode_pool_allocation(token.job.get(), bytes.value()))
            .transpose()?
            .is_some_and(|allocation| allocation.token == token),
        Probe::MutableJobStarted(token, revision) => {
            let job = rx
                .open_table(JOBS)?
                .get(token.job.get())?
                .map(|bytes| {
                    crate::db::redb::jobs::decode_mutable_job(token.job.get(), bytes.value())
                })
                .transpose()?;
            let allocation = rx
                .open_table(POOL_ALLOCATIONS)?
                .get(token.job.get())?
                .map(|bytes| allocations::decode_pool_allocation(token.job.get(), bytes.value()))
                .transpose()?;
            job.is_some_and(|job| {
                job.revision == revision
                    && job.phase == crate::db::entities::MutableJobPhase::Executing
                    && job.frozen.is_some()
            }) && allocation.is_some_and(|allocation| {
                allocation.token == token
                    && allocation.job_revision == revision
                    && allocation.phase != crate::db::entities::AllocationPhase::Reserved
            })
        }
        Probe::ToolCall(call) => rx
            .open_table(TOOL_CALLS)?
            .get(tool_call_key(call))?
            .is_some(),
        Probe::ToolOutcome(call) => rx
            .open_table(TOOL_CALLS)?
            .get(tool_call_key(call))?
            .map(|bytes| decode_tool_evidence(call, bytes.value()))
            .transpose()?
            .is_some_and(|record| record.outcome.is_some()),
        Probe::Outcome(job) => rx
            .open_table(JOB_RUNTIME)?
            .get(job.get())?
            .map(|bytes| state::decode_state(job.get(), bytes.value()))
            .transpose()?
            .is_some_and(|state| state.outcome.is_some()),
        Probe::ExecutionEvent(job, seq) => {
            let table = rx.open_table(JOB_EVENTS)?;
            match table.get((job.get(), seq))? {
                Some(row) => {
                    let event = execution::decode_event((job.get(), seq), row.value())?;
                    event.seq <= execution::commit_end(&rx, event.transaction)?
                }
                None => false,
            }
        }
    })
}
