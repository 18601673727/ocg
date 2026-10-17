use redb::ReadableTableMetadata;

use super::*;

// ---- batches -------------------------------------------------------------------

/// A failure inside a batch. Only [`Batch`] creates one, and creating it
/// poisons that batch.
#[derive(Clone, Debug)]
pub(crate) struct BatchError(pub(super) Failure);

#[derive(Clone, Debug)]
pub(super) enum Failure {
    Rejected(Violation),
    Storage(StorageError),
}

/// Who the operation marker names.
pub(crate) struct MarkerSubjects {
    subject: RecordRef,
    created: Option<RecordRef>,
}

impl MarkerSubjects {
    pub(crate) fn addressed(subject: RecordRef) -> Self {
        Self {
            subject,
            created: None,
        }
    }

    pub(crate) fn created(subject: RecordRef, created: RecordRef) -> Self {
        Self {
            subject,
            created: Some(created),
        }
    }
}

/// How a batch ended when it did not commit.
#[derive(Debug)]
pub(crate) enum BatchFailure {
    /// A rule refused the batch; nothing was committed.
    Rejected(Violation),
    /// Storage failed before the commit point; nothing was committed.
    NotCommitted(StorageError),
    /// The commit itself failed; its changes may or may not be durable.
    OutcomeUnknown(StorageError),
}

pub(crate) struct BatchOutcome<T> {
    pub(crate) result: Result<T, BatchFailure>,
    /// Set when the storage engine failed, so the open database must stop
    /// admitting work and be released before it can be reopened.
    pub(crate) storage_failure: Option<StorageError>,
}

pub(crate) struct Batch {
    pub(super) tx: WriteTransaction,
    pub(super) failed: Option<Failure>,
    pub(super) now: Timestamp,
    /// [`closure_certified`], once read.
    pub(super) certified: Option<bool>,
    /// Net change to [`PENDING_JOBS`] membership made by this batch so far.
    pub(super) pending_delta: i64,
}

/// How a batch's commit becomes durable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Commit {
    /// The commit is durable when it returns.
    Durable,
    /// The commit is visible to new read transactions at once but is durable
    /// only after a later [`persist_staged`] succeeds. Until then a crash or
    /// a failed [`persist_staged`] can lose it.
    Staged,
}

/// Runs one command as one batch and commits it durably with its operation
/// marker only if no operation in it failed.
#[cfg(test)]
pub(crate) fn run_batch<T>(
    database: &Database,
    operation: OperationId,
    kind: OperationKind,
    body: impl FnOnce(&mut Batch) -> Result<(T, MarkerSubjects), BatchError>,
) -> BatchOutcome<T> {
    run_batch_with(database, operation, kind, Commit::Durable, body)
}

/// Like [`run_batch`], with the commit's durability chosen. A
/// [`Commit::Staged`] success is not durable yet, and the caller must keep
/// readers away from it until [`persist_staged`] has succeeded.
pub(crate) fn run_batch_with<T>(
    database: &Database,
    operation: OperationId,
    kind: OperationKind,
    commit: Commit,
    body: impl FnOnce(&mut Batch) -> Result<(T, MarkerSubjects), BatchError>,
) -> BatchOutcome<T> {
    let not_committed = |error: StorageError| BatchOutcome {
        result: Err(BatchFailure::NotCommitted(error.clone())),
        storage_failure: Some(error),
    };
    let mut tx = match database.begin_write() {
        Ok(tx) => tx,
        Err(error) => return not_committed(error.into()),
    };
    let durability = match commit {
        Commit::Durable => Durability::Immediate,
        Commit::Staged => Durability::None,
    };
    if let Err(error) = tx.set_durability(durability) {
        return finish_abort(tx, Failure::Storage(error.into()));
    }
    let mut batch = Batch {
        tx,
        failed: None,
        now: Timestamp::now(),
        certified: None,
        pending_delta: 0,
    };
    let body_result = body(&mut batch);
    // A failure the body swallowed still prevents the commit.
    let value = match (body_result, batch.failed.take()) {
        (_, Some(failure)) | (Err(BatchError(failure)), None) => {
            return finish_abort(batch.tx, failure);
        }
        (Ok(value), None) => value,
    };
    if let Err(BatchError(failure)) = batch.check_pending_limit() {
        return finish_abort(batch.tx, failure);
    }
    let (value, subjects) = value;
    let marker = OperationMarker {
        operation,
        kind,
        subject: subjects.subject,
        created: subjects.created,
        committed_at: batch.now,
    };
    if let Err(BatchError(failure)) = batch.write_marker(&marker) {
        return finish_abort(batch.tx, failure);
    }
    commit_transaction(batch.tx, value)
}

/// Like [`run_batch_with`], but the body may decline: `Ok(None)` aborts the
/// transaction, so nothing is committed and no operation marker is written.
/// Work uses it when it finds, after reading, that it must not apply.
pub(crate) fn run_batch_declinable<T>(
    database: &Database,
    operation: OperationId,
    kind: OperationKind,
    commit: Commit,
    body: impl FnOnce(&mut Batch) -> Result<Option<(T, MarkerSubjects)>, BatchError>,
) -> BatchOutcome<Option<T>> {
    let mut tx = match database.begin_write() {
        Ok(tx) => tx,
        Err(error) => {
            let error = StorageError::from(error);
            return BatchOutcome {
                result: Err(BatchFailure::NotCommitted(error.clone())),
                storage_failure: Some(error),
            };
        }
    };
    let durability = match commit {
        Commit::Durable => Durability::Immediate,
        Commit::Staged => Durability::None,
    };
    if let Err(error) = tx.set_durability(durability) {
        return finish_abort(tx, Failure::Storage(error.into()));
    }
    let mut batch = Batch {
        tx,
        failed: None,
        now: Timestamp::now(),
        certified: None,
        pending_delta: 0,
    };
    let body_result = body(&mut batch);
    let (value, subjects) = match (body_result, batch.failed.take()) {
        (_, Some(failure)) | (Err(BatchError(failure)), None) => {
            return finish_abort(batch.tx, failure);
        }
        (Ok(None), None) => {
            return match batch.tx.abort() {
                Ok(()) => BatchOutcome {
                    result: Ok(None),
                    storage_failure: None,
                },
                Err(error) => {
                    let error = StorageError::from(error);
                    BatchOutcome {
                        result: Err(BatchFailure::NotCommitted(error.clone())),
                        storage_failure: Some(error),
                    }
                }
            };
        }
        (Ok(Some(value)), None) => value,
    };
    if let Err(BatchError(failure)) = batch.check_pending_limit() {
        return finish_abort(batch.tx, failure);
    }
    let marker = OperationMarker {
        operation,
        kind,
        subject: subjects.subject,
        created: subjects.created,
        committed_at: batch.now,
    };
    if let Err(BatchError(failure)) = batch.write_marker(&marker) {
        return finish_abort(batch.tx, failure);
    }
    let outcome = commit_transaction(batch.tx, value);
    BatchOutcome {
        result: outcome.result.map(Some),
        storage_failure: outcome.storage_failure,
    }
}

/// Commits a batch's transaction and reports how the commit ended.
pub(super) fn commit_transaction<T>(tx: WriteTransaction, value: T) -> BatchOutcome<T> {
    match tx.commit() {
        Ok(()) => BatchOutcome {
            result: Ok(value),
            storage_failure: None,
        },
        // redb documents a poisoned commit as rolled back.
        Err(CommitError::TransactionPoisoned) => {
            let error = StorageError::from(CommitError::TransactionPoisoned);
            BatchOutcome {
                result: Err(BatchFailure::NotCommitted(error.clone())),
                storage_failure: Some(error),
            }
        }
        Err(error) => {
            let error = StorageError::from(error);
            BatchOutcome {
                result: Err(BatchFailure::OutcomeUnknown(error.clone())),
                storage_failure: Some(error),
            }
        }
    }
}

/// Makes every earlier [`Commit::Staged`] commit durable with an empty
/// `Durability::Immediate` transaction: redb writes the unpersisted pages of
/// earlier non-durable commits and publishes their roots when a durable
/// commit follows. An error leaves those commits' durability unknown, as for
/// any failed commit.
pub(crate) fn persist_staged(database: &Database) -> Result<(), StorageError> {
    let mut tx = database.begin_write()?;
    tx.set_durability(Durability::Immediate)?;
    tx.commit()?;
    Ok(())
}

// ---- maintenance ---------------------------------------------------------------

/// The bytes of the file that redb's live pages occupy, read in a write
/// transaction that changes nothing. The file's length minus this is an upper
/// bound on what compaction can return, not a promise: it counts free pages
/// anywhere in the file, and `fragmented_bytes` is deliberately not part of
/// it because compaction does not recover space inside pages.
pub(crate) fn allocated_bytes(database: &Database) -> Result<u64, StorageError> {
    let tx = database.begin_write()?;
    let stats = tx.stats()?;
    tx.abort()?;
    let page_size = u64::try_from(stats.page_size()).unwrap_or(u64::MAX);
    Ok(stats.allocated_pages().saturating_mul(page_size))
}

/// Why compaction did not run to completion.
#[derive(Debug)]
pub(crate) enum CompactFailure {
    /// A transaction was still open on the handle. Nothing was changed.
    TransactionInProgress,
    /// A savepoint exists. Nothing was changed.
    SavepointExists,
    /// The storage engine failed part way. Every compaction step is its own
    /// atomic commit, so the file is consistent, but the handle is no longer
    /// usable and must be reopened.
    Storage(StorageError),
}

/// Compacts the file. This is redb's iterative, uninterruptible
/// `Database::compact`: it needs exclusive access to the handle and runs for
/// as long as it finds pages to move. Every step commits durably. The result
/// is `true` when redb moved pages and `false` when none could be moved; the
/// file may still have shrunk on `false`, because redb frees pending pages
/// before it checks for progress.
pub(crate) fn compact(database: &mut Database) -> Result<bool, CompactFailure> {
    match database.compact() {
        Ok(moved) => Ok(moved),
        Err(redb::CompactionError::TransactionInProgress) => {
            Err(CompactFailure::TransactionInProgress)
        }
        Err(
            redb::CompactionError::PersistentSavepointExists
            | redb::CompactionError::EphemeralSavepointExists,
        ) => Err(CompactFailure::SavepointExists),
        Err(redb::CompactionError::Storage(error)) => Err(CompactFailure::Storage(error.into())),
        Err(error) => Err(CompactFailure::Storage(redb::Error::from(error).into())),
    }
}

pub(super) fn finish_abort<T>(tx: WriteTransaction, failure: Failure) -> BatchOutcome<T> {
    let abort_failure = tx.abort().err().map(StorageError::from);
    match failure {
        Failure::Rejected(violation) => BatchOutcome {
            result: Err(BatchFailure::Rejected(violation)),
            storage_failure: abort_failure,
        },
        Failure::Storage(error) => BatchOutcome {
            result: Err(BatchFailure::NotCommitted(error.clone())),
            storage_failure: Some(error),
        },
    }
}

impl Batch {
    /// The wall-clock time read once when this batch's write transaction
    /// began. Every timestamp the batch writes uses it.
    pub(crate) fn now(&self) -> Timestamp {
        self.now
    }

    /// Refuses the batch for a business rule. The batch stays poisoned even
    /// if the returned error is ignored.
    pub(crate) fn reject<T>(&mut self, violation: Violation) -> Result<T, BatchError> {
        self.fail(Failure::Rejected(violation))
    }

    pub(super) fn fail<T>(&mut self, failure: Failure) -> Result<T, BatchError> {
        let first = self.failed.get_or_insert(failure).clone();
        Err(BatchError(first))
    }

    pub(super) fn corrupted<T>(&mut self, detail: String) -> Result<T, BatchError> {
        self.fail(Failure::Storage(StorageError::corrupted(detail)))
    }

    /// Runs one storage step; a failed batch refuses further steps.
    pub(super) fn step<T>(
        &mut self,
        f: impl FnOnce(&WriteTransaction) -> Result<T, StorageError>,
    ) -> Result<T, BatchError> {
        if let Some(failure) = &self.failed {
            return Err(BatchError(failure.clone()));
        }
        match f(&self.tx) {
            Ok(value) => Ok(value),
            Err(error) => self.fail(Failure::Storage(error)),
        }
    }

    pub(super) fn get_bytes(
        &mut self,
        table: Records,
        id: u64,
    ) -> Result<Option<Vec<u8>>, BatchError> {
        self.step(|tx| Ok(tx.open_table(table)?.get(id)?.map(|v| v.value().to_vec())))
    }

    /// Refuses the batch unless `record` exists in `table`.
    pub(super) fn require_exists(
        &mut self,
        table: Records,
        record: RecordRef,
        id: u64,
    ) -> Result<(), BatchError> {
        match self.get_bytes(table, id)? {
            Some(_) => Ok(()),
            None => self.reject(Violation::NotFound(record)),
        }
    }

    pub(super) fn insert_new(
        &mut self,
        table: Records,
        id: u64,
        bytes: Vec<u8>,
        record: RecordRef,
    ) -> Result<(), BatchError> {
        let existed = self.step(|tx| {
            let mut t = tx.open_table(table)?;
            if t.get(id)?.is_some() {
                return Ok(true);
            }
            t.insert(id, bytes.as_slice())?;
            Ok(false)
        })?;
        if existed {
            self.reject(Violation::DuplicateId(record))
        } else {
            Ok(())
        }
    }

    pub(super) fn overwrite(
        &mut self,
        table: Records,
        id: u64,
        bytes: Vec<u8>,
    ) -> Result<(), BatchError> {
        self.step(|tx| {
            tx.open_table(table)?.insert(id, bytes.as_slice())?;
            Ok(())
        })
    }

    pub(super) fn decoded<T>(&mut self, decoded: DecodeResult<T>) -> Result<T, BatchError> {
        match decoded {
            Ok(value) => Ok(value),
            Err(error) => self.fail(Failure::Storage(error)),
        }
    }

    pub(crate) fn next_id(&mut self, sequence: Sequence) -> Result<u64, BatchError> {
        self.step(|tx| allocate(tx, sequence))
    }

    /// The instance's Genesis Job, as recorded in this transaction.
    pub(crate) fn genesis(&mut self) -> Result<JobId, BatchError> {
        self.step(|tx| genesis_id(&tx.open_table(META)?))
    }

    // -- records --

    pub(crate) fn job(&mut self, id: JobId) -> Result<Option<JobRow>, BatchError> {
        let bytes = self.get_bytes(JOBS, id.get())?;
        let row = bytes.map(|b| JobRow::decode(&b)).transpose();
        self.decoded(row)
    }

    pub(crate) fn require_job(&mut self, id: JobId) -> Result<JobRow, BatchError> {
        match self.job(id)? {
            Some(row) => Ok(row),
            None => self.reject(Violation::NotFound(RecordRef::Job(id))),
        }
    }

    pub(crate) fn insert_job(&mut self, row: &JobRow) -> Result<(), BatchError> {
        self.insert_new(JOBS, row.id.get(), row.encode(), RecordRef::Job(row.id))?;
        self.index_pending(row)
    }

    pub(crate) fn put_job(&mut self, row: &JobRow) -> Result<(), BatchError> {
        if let Some(previous) = self.job(row.id)?
            && previous.created_at != row.created_at
        {
            return self.corrupted(format!("{} changed its creation time", row.id));
        }
        self.overwrite(JOBS, row.id.get(), row.encode())?;
        self.index_pending(row)
    }

    /// Brings the Job's [`PENDING_JOBS`] entry in line with the row just
    /// written. The key is fixed by the Job's identity and creation time, so
    /// removing it can only remove this Job's own entry.
    fn index_pending(&mut self, row: &JobRow) -> Result<(), BatchError> {
        let genesis = self.genesis()?;
        let delta = self.step(|tx| {
            let mut pending = tx.open_table(PENDING_JOBS)?;
            if row.id != genesis && row.awaits_start() {
                Ok(i64::from(pending.insert(row.pending_key(), ())?.is_none()))
            } else {
                Ok(-i64::from(pending.remove(row.pending_key())?.is_some()))
            }
        })?;
        self.pending_delta = self.pending_delta.saturating_add(delta);
        Ok(())
    }

    /// Refuses the batch when its net [`PENDING_JOBS`] membership change is
    /// positive and would leave [`PENDING_JOBS`] above [`MAX_PENDING_JOBS`].
    /// Checked after the body so a Job created and started in one transaction
    /// is not refused for the entry it removed before committing.
    fn check_pending_limit(&mut self) -> Result<(), BatchError> {
        if self.pending_delta <= 0 {
            return Ok(());
        }
        let count = self.step(|tx| Ok(tx.open_table(PENDING_JOBS)?.len()?))?;
        if count > MAX_PENDING_JOBS {
            return self.reject(Violation::PendingJobsLimit {
                max_pending_jobs: MAX_PENDING_JOBS,
            });
        }
        Ok(())
    }

    pub(crate) fn require_input(&mut self, id: JobInputId) -> Result<JobInputRow, BatchError> {
        match self.get_bytes(JOB_INPUTS, id.get())? {
            Some(bytes) => {
                let row = JobInputRow::decode(&bytes);
                self.decoded(row)
            }
            None => self.reject(Violation::NotFound(RecordRef::JobInput(id))),
        }
    }

    pub(crate) fn insert_input(&mut self, row: &JobInputRow) -> Result<(), BatchError> {
        self.insert_new(
            JOB_INPUTS,
            row.id.get(),
            row.encode(),
            RecordRef::JobInput(row.id),
        )
    }

    pub(crate) fn put_input(&mut self, row: &JobInputRow) -> Result<(), BatchError> {
        self.overwrite(JOB_INPUTS, row.id.get(), row.encode())
    }

    pub(crate) fn require_attachment(
        &mut self,
        id: AttachmentId,
    ) -> Result<AttachmentRecord, BatchError> {
        match self.get_bytes(ATTACHMENTS, id.get())? {
            Some(bytes) => {
                let record = decode_attachment(&bytes);
                self.decoded(record)
            }
            None => self.reject(Violation::NotFound(RecordRef::Attachment(id))),
        }
    }

    pub(crate) fn insert_attachment(
        &mut self,
        record: &AttachmentRecord,
    ) -> Result<(), BatchError> {
        self.insert_new(
            ATTACHMENTS,
            record.id.get(),
            encode_attachment(record),
            RecordRef::Attachment(record.id),
        )
    }

    pub(crate) fn put_attachment(&mut self, record: &AttachmentRecord) -> Result<(), BatchError> {
        self.overwrite(ATTACHMENTS, record.id.get(), encode_attachment(record))
    }

    /// Replaces the input's HAS_ATTACHMENT entries, and their incoming
    /// index, with `attachments` in order. Each must exist.
    pub(crate) fn set_input_attachments(
        &mut self,
        input: JobInputId,
        attachments: &[AttachmentId],
    ) -> Result<(), BatchError> {
        self.require_input(input)?;
        for attachment in attachments {
            self.require_attachment(*attachment)?;
        }
        let Ok(count) = u32::try_from(attachments.len()) else {
            return self.reject(Violation::ValueOutOfRange("attachments"));
        };
        let duplicate = self.step(|tx| {
            let input = input.get();
            let mut forward = tx.open_table(HAS_ATTACHMENT)?;
            let mut incoming = tx.open_table(ATTACHMENT_INPUTS)?;
            let mut previous = Vec::new();
            for entry in forward.range((input, 0)..=(input, u32::MAX))? {
                let (key, attachment) = entry?;
                previous.push((key.value().1, attachment.value()));
            }
            for (position, attachment) in previous {
                forward.remove((input, position))?;
                incoming.remove((attachment, input))?;
            }
            for (position, attachment) in (0..count).zip(attachments) {
                forward.insert((input, position), attachment.get())?;
                if incoming.insert((attachment.get(), input), ())?.is_some() {
                    return Ok(Some(*attachment));
                }
            }
            Ok(None)
        })?;
        match duplicate {
            Some(attachment) => self.reject(Violation::DuplicateReference(RecordRef::Attachment(
                attachment,
            ))),
            None => Ok(()),
        }
    }

    /// Inserts the output, its HAS_OUTPUT entry and moves LATEST_OUTPUT to it.
    pub(crate) fn insert_output(&mut self, record: &JobOutputRecord) -> Result<(), BatchError> {
        self.insert_new(
            JOB_OUTPUTS,
            record.id.get(),
            encode_output(record),
            RecordRef::JobOutput(record.id),
        )?;
        let (job, output) = (record.job.get(), record.id.get());
        let Ok(seq) = u64::try_from(record.seq) else {
            return self.reject(Violation::ValueOutOfRange("seq"));
        };
        let taken = self.step(|tx| {
            let mut t = tx.open_table(HAS_OUTPUT)?;
            if t.get((job, seq))?.is_some() {
                return Ok(true);
            }
            t.insert((job, seq), output)?;
            tx.open_table(LATEST_OUTPUT)?.insert(job, output)?;
            Ok(false)
        })?;
        if taken {
            self.reject(Violation::DuplicateId(RecordRef::JobOutput(record.id)))
        } else {
            Ok(())
        }
    }

    pub(crate) fn last_output_seq(&mut self, job: JobId) -> Result<u64, BatchError> {
        self.step(|tx| last_seq(&tx.open_table(HAS_OUTPUT)?, job.get()))
    }

    pub(super) fn require_executor_row(
        &mut self,
        id: ExecutorId,
    ) -> Result<ExecutorRow, BatchError> {
        let Some(bytes) = self.get_bytes(EXECUTORS, id.get())? else {
            return self.reject(Violation::NotFound(RecordRef::Executor(id)));
        };
        let decoded = ExecutorRow::decode(&bytes);
        self.decoded(decoded)
    }

    pub(crate) fn require_executor(&mut self, id: ExecutorId) -> Result<(), BatchError> {
        self.require_exists(EXECUTORS, RecordRef::Executor(id), id.get())
    }

    /// Refuses the batch unless the Executor exists and, when the Job runs
    /// an LLM configuration, is an Llm Executor whose adapter supports the
    /// transport that configuration froze. The Executor never changes which
    /// Provider, model or parameters the Job runs.
    pub(crate) fn require_executor_for(
        &mut self,
        executor: ExecutorId,
        frozen: Option<&FrozenLlmConfig>,
    ) -> Result<(), BatchError> {
        let row = self.require_executor_row(executor)?;
        let Some(frozen) = frozen else {
            return Ok(());
        };
        if row.executor_type != ExecutorType::Llm {
            return self.reject(Violation::IncompatibleExecutorKind {
                executor,
                found: row.executor_type,
            });
        }
        let transport = frozen.provider.transport;
        if !row.llm_transports.contains(&transport) {
            return self.reject(Violation::ExecutorLacksTransport {
                executor,
                transport,
            });
        }
        Ok(())
    }

    pub(crate) fn require_core_executor(&mut self, id: ExecutorId) -> Result<(), BatchError> {
        let row = self.require_executor_row(id)?;
        if row.name != "__CORE__"
            || row.executor_type != ExecutorType::System
            || row.concurrency_limit == 0
        {
            return self.reject(Violation::NotCoreExecutor(id));
        }
        Ok(())
    }

    /// Inserts the Executor and indexes its LLM transports. Inserting refuses
    /// an existing Executor, so its transports never change after registration.
    pub(crate) fn insert_executor(&mut self, row: &ExecutorRow) -> Result<(), BatchError> {
        if !row.llm_transports.is_empty() && row.executor_type != ExecutorType::Llm {
            return self.reject(Violation::IncompatibleExecutorKind {
                executor: row.id,
                found: row.executor_type,
            });
        }
        self.insert_new(
            EXECUTORS,
            row.id.get(),
            row.encode(),
            RecordRef::Executor(row.id),
        )?;
        let executor = row.id.get();
        let transports: Vec<u8> = row
            .llm_transports
            .iter()
            .map(|transport| transport.code())
            .collect();
        self.step(|tx| {
            let mut index = tx.open_table(TRANSPORT_EXECUTORS)?;
            for transport in transports {
                index.insert((transport, executor), ())?;
            }
            Ok(())
        })
    }

    pub(crate) fn require_project(&mut self, id: ProjectId) -> Result<(), BatchError> {
        self.require_exists(PROJECTS, RecordRef::Project(id), id.get())
    }

    pub(crate) fn insert_project(&mut self, record: &ProjectRecord) -> Result<(), BatchError> {
        self.insert_new(
            PROJECTS,
            record.id.get(),
            encode_project(record),
            RecordRef::Project(record.id),
        )
    }

    pub(crate) fn require_llm_provider(&mut self, id: LlmProviderId) -> Result<(), BatchError> {
        self.require_exists(LLM_PROVIDERS, RecordRef::LlmProvider(id), id.get())
    }

    pub(crate) fn insert_llm_provider(
        &mut self,
        record: &LlmProviderRecord,
    ) -> Result<(), BatchError> {
        self.insert_new(
            LLM_PROVIDERS,
            record.id.get(),
            encode_llm_provider(record),
            RecordRef::LlmProvider(record.id),
        )
    }

    /// Refuses the batch unless the Profile exists and is of `kind`.
    pub(crate) fn require_profile_row(&mut self, id: ProfileId) -> Result<ProfileRow, BatchError> {
        let Some(bytes) = self.get_bytes(PROFILES, id.get())? else {
            return self.reject(Violation::NotFound(RecordRef::Profile(id)));
        };
        let decoded = ProfileRow::decode(&bytes);
        self.decoded(decoded)
    }

    pub(crate) fn require_profile_kind(
        &mut self,
        id: ProfileId,
        kind: ProfileKind,
    ) -> Result<(), BatchError> {
        let found = self.require_profile_row(id)?.config.kind();
        if found != kind {
            return self.reject(
                WrongProfileKind {
                    profile: id,
                    expected: kind,
                    found,
                }
                .into(),
            );
        }
        Ok(())
    }

    /// Inserts the Profile with the references that define it. Inserting
    /// refuses an existing Profile; only [`Self::replace_profile`] changes
    /// one, payload and references together.
    pub(crate) fn insert_profile(
        &mut self,
        row: &ProfileRow,
        links: &ProfileLinks,
    ) -> Result<(), BatchError> {
        self.insert_new(
            PROFILES,
            row.id.get(),
            row.encode(),
            RecordRef::Profile(row.id),
        )?;
        self.write_profile_links(row, links)
    }

    /// Replaces a stored Profile's payload and the references that define it,
    /// with their incoming indexes. The caller has checked that no unfinished
    /// Job holds the Profile.
    pub(crate) fn replace_profile(
        &mut self,
        row: &ProfileRow,
        links: &ProfileLinks,
    ) -> Result<(), BatchError> {
        let existing = self.require_profile_row(row.id)?;
        if existing.config.kind() != row.config.kind() {
            return self.reject(
                WrongProfileKind {
                    profile: row.id,
                    expected: existing.config.kind(),
                    found: row.config.kind(),
                }
                .into(),
            );
        }
        let profile = row.id.get();
        self.step(|tx| {
            let mut targets = tx.open_table(TARGETS_LLM)?;
            let old_targets = pair_targets(&targets, profile, None, usize::MAX)?;
            for target in old_targets {
                targets.remove((profile, target))?;
            }
            drop(targets);
            let old_provider = tx
                .open_table(USES_PROVIDER)?
                .remove(profile)?
                .map(|v| v.value());
            if let Some(provider) = old_provider {
                tx.open_table(PROVIDER_PROFILES)?
                    .remove((provider, profile))?;
            }
            Ok(())
        })?;
        self.overwrite(PROFILES, profile, row.encode())?;
        self.write_profile_links(row, links)
    }

    pub(super) fn write_profile_links(
        &mut self,
        row: &ProfileRow,
        links: &ProfileLinks,
    ) -> Result<(), BatchError> {
        match (links, row.config.kind()) {
            (ProfileLinks::ExecutionPool, ProfileKind::ExecutionPool) => Ok(()),
            (ProfileLinks::Llm { provider }, ProfileKind::Llm) => {
                self.link_profile_provider(row.id, *provider)
            }
            (ProfileLinks::Agent { llm_targets }, ProfileKind::Agent) => {
                for target in llm_targets {
                    self.require_profile_kind(*target, ProfileKind::Llm)?;
                    if !self.insert_pair(TARGETS_LLM, row.id.get(), target.get())? {
                        return self
                            .reject(Violation::DuplicateReference(RecordRef::Profile(*target)));
                    }
                }
                Ok(())
            }
            (_, kind) => self.corrupted(format!(
                "{} is an {kind} Profile given the other kind's references",
                row.id
            )),
        }
    }

    /// Refuses the batch unless the Profiles have the right kinds and, when
    /// an Agent Profile is named, the LLM Profile is one of its configured
    /// targets.
    pub(crate) fn require_configuration(
        &mut self,
        configuration: ExecutionConfiguration,
    ) -> Result<(), BatchError> {
        let ExecutionConfiguration {
            agent_profile,
            llm_profile,
        } = configuration;
        if let Some(agent_profile) = agent_profile {
            self.require_profile_kind(agent_profile, ProfileKind::Agent)?;
        }
        self.require_profile_kind(llm_profile, ProfileKind::Llm)?;
        if let Some(agent_profile) = agent_profile
            && !self.has_pair(TARGETS_LLM, agent_profile.get(), llm_profile.get())?
        {
            return self.reject(Violation::NotAConfiguredTarget {
                agent_profile,
                llm_profile,
            });
        }
        Ok(())
    }

    pub(crate) fn insert_agent(&mut self, row: &AgentRow) -> Result<(), BatchError> {
        self.insert_new(AGENTS, row.id.get(), row.encode(), RecordRef::Agent(row.id))
    }

    /// Inserts the Metrics with its MEASURES edge, the edge's incoming index
    /// and its scope index entries.
    pub(crate) fn insert_metrics(
        &mut self,
        row: &MetricsRow,
        job: JobId,
    ) -> Result<(), BatchError> {
        self.insert_new(
            METRICS,
            row.id.get(),
            row.encode(),
            RecordRef::Metrics(row.id),
        )?;
        self.insert_observation(
            MEASURES,
            JOB_METRICS,
            METRICS_BY_SCOPE,
            row.id.get(),
            job,
            row.observed_at,
        )?;
        Ok(())
    }

    /// The Token eligibility the committed Metrics of one request of the
    /// Job state: that of its first annotated record, which every later
    /// annotated record of the request shares. Reads only the Job's own
    /// Metrics, in ID order, up to that record.
    pub(crate) fn request_token_eligibility(
        &mut self,
        job: JobId,
        request: RequestNumber,
    ) -> Result<Option<RequestEligibility>, BatchError> {
        self.step(|tx| {
            let index = tx.open_table(JOB_METRICS)?;
            let records = tx.open_table(METRICS)?;
            for entry in index.range((job.get(), 0)..=(job.get(), u64::MAX))? {
                let metrics = entry?.0.value().1;
                let bytes = records.get(metrics)?.ok_or_else(|| {
                    StorageError::corrupted(format!(
                        "{job} is measured by missing metrics {metrics}"
                    ))
                })?;
                let row = MetricsRow::decode(bytes.value())?;
                if row.request == request
                    && let Some(token) = row.token
                {
                    return Ok(Some(token.eligibility));
                }
            }
            Ok(None)
        })
    }

    /// Whether the Job has a Metrics or AvailabilityStat record of the
    /// request: the observation that makes the request a recorded one.
    pub(crate) fn request_observed(
        &mut self,
        job: JobId,
        request: RequestNumber,
    ) -> Result<bool, BatchError> {
        self.step(|tx| {
            for (index, records, decode) in [
                (
                    JOB_AVAILABILITY,
                    AVAILABILITY_STATS,
                    (|bytes| AvailabilityStatRow::decode(bytes).map(|row| row.request))
                        as fn(&[u8]) -> DecodeResult<RequestNumber>,
                ),
                (JOB_METRICS, METRICS, |bytes| {
                    MetricsRow::decode(bytes).map(|row| row.request)
                }),
            ] {
                let index = tx.open_table(index)?;
                let records = tx.open_table(records)?;
                for entry in index.range((job.get(), 0)..=(job.get(), u64::MAX))? {
                    let record = entry?.0.value().1;
                    let bytes = records.get(record)?.ok_or_else(|| {
                        StorageError::corrupted(format!(
                            "{job} is linked to missing observation {record}"
                        ))
                    })?;
                    if decode(bytes.value())? == request {
                        return Ok(true);
                    }
                }
            }
            Ok(false)
        })
    }

    pub(crate) fn tool_evidence(
        &mut self,
        call: ToolCallRef,
    ) -> Result<Option<ToolEvidenceRecord>, BatchError> {
        self.step(|tx| {
            tx.open_table(TOOL_EVIDENCE)?
                .get(tool_call_key(call))?
                .map(|bytes| decode_tool_evidence(call, bytes.value()))
                .transpose()
        })
    }

    /// Every tool evidence record of the Job, in call order.
    pub(crate) fn job_tool_evidence(
        &mut self,
        job: JobId,
    ) -> Result<Vec<ToolEvidenceRecord>, BatchError> {
        self.step(|tx| job_tool_evidence(&tx.open_table(TOOL_EVIDENCE)?, job))
    }

    /// Inserts a new record with its Provider id. Refuses a call that
    /// already has a record, and an id another record of the Job uses.
    pub(crate) fn insert_tool_evidence(
        &mut self,
        record: &ToolEvidenceRecord,
    ) -> Result<(), BatchError> {
        let call = record.call;
        let key = tool_call_key(call);
        let job = call.job.get();
        let id = record.tool_call_id.as_str();
        let taken = self.step(|tx| {
            if tx.open_table(TOOL_EVIDENCE)?.get(key)?.is_some() {
                return Ok(Some(None));
            }
            let mut ids = tx.open_table(TOOL_CALL_IDS)?;
            if let Some(existing) = ids.get((job, id))? {
                return Ok(Some(Some(existing.value())));
            }
            ids.insert((job, id), (key.1, key.2))?;
            tx.open_table(TOOL_EVIDENCE)?
                .insert(key, encode_tool_evidence(record).as_slice())?;
            Ok(None)
        })?;
        match taken {
            None => Ok(()),
            Some(None) => self.reject(Violation::DuplicateToolCall(call)),
            Some(Some((request, index))) => {
                let Some(request) = RequestNumber::new(request) else {
                    return self.corrupted(format!("{call}: an indexed request number is 0"));
                };
                self.reject(Violation::DuplicateToolCallId {
                    call,
                    existing: ToolCallRef {
                        job: call.job,
                        request,
                        index,
                    },
                })
            }
        }
    }

    /// Replaces an existing record, which keeps its call and Provider id.
    pub(crate) fn put_tool_evidence(
        &mut self,
        record: &ToolEvidenceRecord,
    ) -> Result<(), BatchError> {
        let key = tool_call_key(record.call);
        let replaced = self.step(|tx| {
            Ok(tx
                .open_table(TOOL_EVIDENCE)?
                .insert(key, encode_tool_evidence(record).as_slice())?
                .is_some())
        })?;
        if replaced {
            Ok(())
        } else {
            self.corrupted(format!("{} was replaced without a record", record.call))
        }
    }

    /// Inserts the AvailabilityStat with its OBSERVES edge, the edge's
    /// incoming index and its scope index entries. The caller has checked
    /// that the Job ran an LLM.
    pub(crate) fn insert_availability_stat(
        &mut self,
        row: &AvailabilityStatRow,
        job: JobId,
    ) -> Result<(), BatchError> {
        self.insert_new(
            AVAILABILITY_STATS,
            row.id.get(),
            row.encode(),
            RecordRef::AvailabilityStat(row.id),
        )?;
        if !self.insert_observation(
            OBSERVES,
            JOB_AVAILABILITY,
            AVAILABILITY_BY_SCOPE,
            row.id.get(),
            job,
            row.observed_at,
        )? {
            return self.corrupted(format!(
                "availability_stat {} observes {job}, which ran no LLM",
                row.id.get()
            ));
        }
        Ok(())
    }

    /// Links a just-inserted observation record to its Job and writes its
    /// scope index entries from the target the Job froze. The target is
    /// never taken from the caller. Returns whether the Job ran an LLM.
    pub(super) fn insert_observation(
        &mut self,
        forward: Link,
        incoming: PairSet,
        by_scope: ScopeIndex,
        record: u64,
        job: JobId,
        observed_at: Timestamp,
    ) -> Result<bool, BatchError> {
        self.require_job(job)?;
        let target = match (self.job_llm_profile(job)?, self.job_frozen_llm(job)?) {
            (Some(llm_profile), Some(frozen)) => Some(LlmTarget {
                llm_profile,
                frozen,
            }),
            _ => None,
        };
        if !self.insert_indexed_link(forward, incoming, record, job.get())? {
            return self.corrupted(format!("observation record {record} is already linked"));
        }
        let keys: Vec<Vec<u8>> = Scope::of(target.as_ref()).iter().map(Scope::key).collect();
        let at = observed_at.as_unix_micros();
        self.step(|tx| {
            let mut index = tx.open_table(by_scope)?;
            for key in &keys {
                index.insert((key.as_slice(), at, record), ())?;
            }
            Ok(())
        })?;
        Ok(target.is_some())
    }
}
