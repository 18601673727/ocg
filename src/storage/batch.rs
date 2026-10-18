use super::rows::{decode_request_event, encode_request_event};
use super::*;
use crate::persistence::{
    AttachmentId, ConfigBody, ConfigId, ConfigKind, JobId, OutputKind, ProjectId,
    RequestEventRecord, RequestNumber, RequestPayload, SubmissionKey, SubmissionRecord,
    ToolCallRef, ToolEvidenceRecord, Violation,
};

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

/// A fact whose presence in a reopened file says whether a command whose
/// commit had an unknown outcome committed (section 11.2): the natural key
/// the command writes, never a marker written for the purpose.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Probe {
    /// `submission[key]` exists.
    Submission(SubmissionKey),
    /// The Job has a claim.
    Claim(JobId),
    /// `tool_call[call]` exists.
    ToolCall(ToolCallRef),
    /// `tool_call[call]` has an outcome.
    ToolOutcome(ToolCallRef),
    /// `job_state[job]` has an outcome.
    Outcome(JobId),
}

pub(crate) struct BatchOutcome<T> {
    pub(crate) result: Result<T, BatchFailure>,
    pub(crate) storage_failure: Option<StorageError>,
    /// The commit added a `ready` or `delayed` row or changed a Pool.
    pub(crate) scheduling_wake: bool,
    /// The commit freed a held slot.
    pub(crate) capacity_freed: bool,
    pub(crate) probe: Option<Probe>,
}

impl<T> BatchOutcome<T> {
    fn failed(failure: BatchFailure, storage_failure: Option<StorageError>) -> Self {
        Self {
            result: Err(failure),
            storage_failure,
            scheduling_wake: false,
            capacity_freed: false,
            probe: None,
        }
    }
}

pub(crate) struct Batch {
    pub(super) tx: WriteTransaction,
    pub(super) failed: Option<Failure>,
    pub(super) now: Timestamp,
    pub(super) epoch: u64,
    pub(super) host_id: Option<String>,
    pub(super) boot_id: Option<String>,
    pub(super) touched_ready_or_delayed: bool,
    pub(super) freed_capacity: bool,
    pub(super) changed_pool: bool,
    pub(super) probe: Option<Probe>,
}

/// How a batch's commit becomes durable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Commit {
    /// The commit is durable when it returns.
    Durable,
    /// The commit is visible to new read transactions at once but is durable
    /// only after a later [`persist_staged`] succeeds.
    Staged,
}

fn begin(database: &Database, epoch: u64, commit: Commit) -> Result<Batch, BatchOutcome<()>> {
    let mut tx = database
        .begin_write()
        .map_err(|error| BatchOutcome::failed_storage(error.into()))?;
    let durability = match commit {
        Commit::Durable => Durability::Immediate,
        Commit::Staged => Durability::None,
    };
    if let Err(error) = tx.set_durability(durability) {
        let error = StorageError::from(error);
        let abort = tx.abort();
        drop(abort);
        return Err(BatchOutcome::failed_storage(error));
    }
    Ok(Batch::new(tx, epoch))
}

impl BatchOutcome<()> {
    fn failed_storage(error: StorageError) -> Self {
        Self::failed(BatchFailure::NotCommitted(error.clone()), Some(error))
    }

    fn cast<T>(self) -> BatchOutcome<T> {
        BatchOutcome {
            result: match self.result {
                Ok(()) => unreachable!("only failures are cast"),
                Err(failure) => Err(failure),
            },
            storage_failure: self.storage_failure,
            scheduling_wake: self.scheduling_wake,
            capacity_freed: self.capacity_freed,
            probe: self.probe,
        }
    }
}

pub(crate) fn run_batch_with<T>(
    database: &Database,
    epoch: u64,
    commit: Commit,
    body: impl FnOnce(&mut Batch) -> Result<T, BatchError>,
) -> BatchOutcome<T> {
    let mut batch = match begin(database, epoch, commit) {
        Ok(batch) => batch,
        Err(outcome) => return outcome.cast(),
    };
    let value = match (body(&mut batch), batch.failed.take()) {
        (_, Some(failure)) | (Err(BatchError(failure)), None) => {
            return finish_abort(batch.tx, failure);
        }
        (Ok(value), None) => value,
    };
    let (wake, freed, probe) = (
        batch.touched_ready_or_delayed || batch.changed_pool,
        batch.freed_capacity,
        batch.probe,
    );
    commit_transaction(batch.tx, value, wake, freed, probe)
}

/// Like [`run_batch_with`], and a body that returns `None` ends the batch
/// without committing anything.
pub(crate) fn run_batch_declinable<T>(
    database: &Database,
    epoch: u64,
    commit: Commit,
    body: impl FnOnce(&mut Batch) -> Result<Option<T>, BatchError>,
) -> BatchOutcome<Option<T>> {
    let mut batch = match begin(database, epoch, commit) {
        Ok(batch) => batch,
        Err(outcome) => return outcome.cast(),
    };
    let value = match (body(&mut batch), batch.failed.take()) {
        (_, Some(failure)) | (Err(BatchError(failure)), None) => {
            return finish_abort(batch.tx, failure);
        }
        (Ok(None), None) => {
            return match batch.tx.abort() {
                Ok(()) => BatchOutcome {
                    result: Ok(None),
                    storage_failure: None,
                    scheduling_wake: false,
                    capacity_freed: false,
                    probe: None,
                },
                Err(error) => {
                    let error = StorageError::from(error);
                    BatchOutcome::failed(BatchFailure::NotCommitted(error.clone()), Some(error))
                }
            };
        }
        (Ok(Some(value)), None) => value,
    };
    let (wake, freed, probe) = (
        batch.touched_ready_or_delayed || batch.changed_pool,
        batch.freed_capacity,
        batch.probe,
    );
    commit_transaction(batch.tx, Some(value), wake, freed, probe)
}

pub(super) fn commit_transaction<T>(
    tx: WriteTransaction,
    value: T,
    wake: bool,
    freed: bool,
    probe: Option<Probe>,
) -> BatchOutcome<T> {
    match tx.commit() {
        Ok(()) => BatchOutcome {
            result: Ok(value),
            storage_failure: None,
            scheduling_wake: wake,
            capacity_freed: freed,
            probe,
        },
        Err(CommitError::TransactionPoisoned) => {
            let error = StorageError::from(CommitError::TransactionPoisoned);
            BatchOutcome::failed(BatchFailure::NotCommitted(error.clone()), Some(error))
        }
        Err(error) => {
            let error = StorageError::from(error);
            let mut outcome =
                BatchOutcome::failed(BatchFailure::OutcomeUnknown(error.clone()), Some(error));
            outcome.probe = probe;
            outcome
        }
    }
}

pub(crate) fn persist_staged(database: &Database) -> Result<(), StorageError> {
    let mut tx = database.begin_write()?;
    tx.set_durability(Durability::Immediate)?;
    tx.commit()?;
    Ok(())
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
        Failure::Rejected(violation) => {
            BatchOutcome::failed(BatchFailure::Rejected(violation), abort_failure)
        }
        Failure::Storage(error) => {
            BatchOutcome::failed(BatchFailure::NotCommitted(error.clone()), Some(error))
        }
    }
}

impl Batch {
    pub(super) fn new(tx: WriteTransaction, epoch: u64) -> Self {
        Self {
            tx,
            failed: None,
            now: Timestamp::now(),
            epoch,
            host_id: open::current_host_id(),
            boot_id: open::current_boot_id(),
            touched_ready_or_delayed: false,
            freed_capacity: false,
            changed_pool: false,
            probe: None,
        }
    }

    pub(crate) fn now(&self) -> Timestamp {
        self.now
    }

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

    /// Runs `f` on the transaction. A storage error poisons the batch.
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

    pub(super) fn decoded<T>(&mut self, decoded: DecodeResult<T>) -> Result<T, BatchError> {
        match decoded {
            Ok(value) => Ok(value),
            Err(error) => self.fail(Failure::Storage(error)),
        }
    }

    pub(crate) fn next_id(&mut self, sequence: Sequence) -> Result<u64, BatchError> {
        self.step(|tx| allocate(tx, sequence))
    }

    // ---- configs ---------------------------------------------------------------------

    pub(crate) fn config(&mut self, id: ConfigId) -> Result<Option<ConfigRow>, BatchError> {
        let row = self.step(|tx| {
            tx.open_table(CONFIGS)?
                .get(id.get())?
                .map(|bytes| ConfigRow::decode(bytes.value()))
                .transpose()
        })?;
        Ok(row)
    }

    pub(crate) fn require_config(&mut self, id: ConfigId) -> Result<ConfigRow, BatchError> {
        match self.config(id)? {
            Some(row) => Ok(row),
            None => self.reject(Violation::NotFound(RecordRef::Profile(id.into()))),
        }
    }

    /// The config `id`, which must have kind `kind` (I17).
    pub(crate) fn require_config_kind(
        &mut self,
        id: ConfigId,
        kind: ConfigKind,
    ) -> Result<ConfigRow, BatchError> {
        let row = self.require_config(id)?;
        if row.kind != kind {
            return self.reject(Violation::WrongConfigKind {
                config: id,
                expected: kind,
                found: row.kind,
            });
        }
        Ok(row)
    }

    fn check_config_body(&mut self, name: &str, body: &ConfigBody) -> Result<(), BatchError> {
        if name.is_empty() || name.len() > 255 {
            return self.reject(Violation::InvalidValue("name", "must be 1 to 255 bytes"));
        }
        match body {
            ConfigBody::Connection(_) => {}
            ConfigBody::Model(model) => {
                self.require_config_kind(model.connection_id, ConfigKind::Connection)?;
            }
            ConfigBody::Agent(agent) => {
                for model in &agent.models {
                    self.require_config_kind(*model, ConfigKind::Model)?;
                }
                if agent
                    .default_model
                    .is_some_and(|default| !agent.models.contains(&default))
                {
                    return self.reject(Violation::InvalidValue(
                        "default_model",
                        "must be one of the Agent's models",
                    ));
                }
            }
            ConfigBody::Pool(pool) => {
                if pool.concurrency_limit == 0 {
                    return self.reject(Violation::InvalidValue(
                        "concurrency_limit",
                        "must be at least 1",
                    ));
                }
            }
        }
        Ok(())
    }

    /// Inserts a new config with edit counter 1.
    pub(crate) fn create_config(
        &mut self,
        name: String,
        body: ConfigBody,
    ) -> Result<ConfigId, BatchError> {
        self.check_config_body(&name, &body)?;
        let id = ConfigId::new(self.next_id(Sequence::Config)?);
        let row = ConfigRow {
            id,
            kind: body.kind(),
            name,
            edit_seq: 1,
            created_at: self.now,
            updated_at: self.now,
            archived_at: None,
            body,
        };
        if row.kind == ConfigKind::Pool {
            self.changed_pool = true;
        }
        self.step(|tx| {
            tx.open_table(CONFIGS)?
                .insert(id.get(), row.encode().as_slice())?;
            Ok(())
        })?;
        Ok(id)
    }

    /// Edits a config in place: a new name and body of the same kind, and the
    /// edit counter up by one. Jobs that froze the old values keep them.
    pub(crate) fn update_config(
        &mut self,
        id: ConfigId,
        name: String,
        body: ConfigBody,
    ) -> Result<(), BatchError> {
        let existing = self.require_config(id)?;
        if existing.kind != body.kind() {
            return self.reject(Violation::WrongConfigKind {
                config: id,
                expected: existing.kind,
                found: body.kind(),
            });
        }
        self.check_config_body(&name, &body)?;
        if existing.kind == ConfigKind::Pool {
            self.changed_pool = true;
            // A larger limit or an enabled Pool frees capacity.
            self.freed_capacity = true;
        }
        let row = ConfigRow {
            name,
            edit_seq: existing.edit_seq + 1,
            updated_at: self.now,
            body,
            ..existing
        };
        self.step(|tx| {
            tx.open_table(CONFIGS)?
                .insert(id.get(), row.encode().as_slice())?;
            Ok(())
        })
    }

    // ---- jobs --------------------------------------------------------------------------

    pub(crate) fn job(&mut self, id: JobId) -> Result<Option<JobRow>, BatchError> {
        self.step(|tx| {
            tx.open_table(JOBS)?
                .get(id.get())?
                .map(|bytes| JobRow::decode(bytes.value()))
                .transpose()
        })
    }

    pub(crate) fn require_job(&mut self, id: JobId) -> Result<JobRow, BatchError> {
        match self.job(id)? {
            Some(row) => Ok(row),
            None => self.reject(Violation::NotFound(RecordRef::Job(id))),
        }
    }

    pub(crate) fn job_state(&mut self, id: JobId) -> Result<Option<JobStateRow>, BatchError> {
        self.step(|tx| {
            tx.open_table(JOB_STATES)?
                .get(id.get())?
                .map(|bytes| JobStateRow::decode(bytes.value()))
                .transpose()
        })
    }

    pub(crate) fn require_job_state(&mut self, id: JobId) -> Result<JobStateRow, BatchError> {
        match self.job_state(id)? {
            Some(row) => Ok(row),
            None => self.reject(Violation::NotFound(RecordRef::Job(id))),
        }
    }

    /// Inserts a Job spec, which is never rewritten (I1).
    pub(super) fn insert_job(&mut self, row: &JobRow) -> Result<(), BatchError> {
        let duplicate = self.step(|tx| {
            let mut jobs = tx.open_table(JOBS)?;
            if jobs.get(row.id.get())?.is_some() {
                return Ok(true);
            }
            jobs.insert(row.id.get(), row.encode().as_slice())?;
            Ok(false)
        })?;
        if duplicate {
            return self.reject(Violation::DuplicateId(RecordRef::Job(row.id)));
        }
        Ok(())
    }

    pub(crate) fn put_job_state(
        &mut self,
        job: JobId,
        state: &JobStateRow,
    ) -> Result<(), BatchError> {
        self.step(|tx| {
            tx.open_table(JOB_STATES)?
                .insert(job.get(), state.encode().as_slice())?;
            Ok(())
        })
    }

    /// SubtreeSettled is the absence of an `unsettled` row (I20).
    pub(crate) fn is_subtree_settled(&mut self, id: JobId) -> Result<bool, BatchError> {
        let unsettled = self.step(|tx| Ok(tx.open_table(UNSETTLED)?.get(id.get())?.is_some()))?;
        Ok(!unsettled)
    }

    // ---- outputs -------------------------------------------------------------------------

    /// Appends an output as the Job's next one (I11) and returns its number.
    pub(crate) fn append_output(
        &mut self,
        job: JobId,
        kind: OutputKind,
        summary: Option<String>,
        code: Option<String>,
        goal_reached: Option<bool>,
        value: Option<serde_json::Value>,
    ) -> Result<u32, BatchError> {
        let seq = self.step(|tx| {
            let outputs = tx.open_table(JOB_OUTPUTS)?;
            let last = outputs
                .range((job.get(), 0)..=(job.get(), u32::MAX))?
                .next_back()
                .transpose()?
                .map_or(0, |(key, _)| key.value().1);
            Ok(last)
        })?;
        let Some(seq) = seq.checked_add(1) else {
            return self.reject(Violation::ValueOutOfRange("seq"));
        };
        let record = JobOutputRecord {
            id: JobOutputId::new(u64::from(seq)),
            job,
            seq: i64::from(seq),
            kind,
            summary,
            code,
            goal_reached,
            value,
            created_at: self.now,
        };
        self.step(|tx| {
            tx.open_table(JOB_OUTPUTS)?
                .insert((job.get(), seq), encode_output(&record).as_slice())?;
            Ok(())
        })?;
        Ok(seq)
    }

    // ---- submissions, attachments, projects ----------------------------------------------

    pub(crate) fn submission(
        &mut self,
        key: SubmissionKey,
    ) -> Result<Option<SubmissionRecord>, BatchError> {
        self.step(|tx| {
            tx.open_table(SUBMISSIONS)?
                .get(key.as_bytes().as_slice())?
                .map(|bytes| decode_submission(key.as_bytes(), bytes.value()))
                .transpose()
        })
    }

    pub(crate) fn require_attachment(
        &mut self,
        id: AttachmentId,
    ) -> Result<AttachmentRecord, BatchError> {
        let record = self.step(|tx| {
            tx.open_table(ATTACHMENTS)?
                .get(id.get())?
                .map(|bytes| decode_attachment(bytes.value()))
                .transpose()
        })?;
        match record {
            Some(record) => Ok(record),
            None => self.reject(Violation::NotFound(RecordRef::Attachment(id))),
        }
    }

    pub(crate) fn insert_attachment(
        &mut self,
        record: &AttachmentRecord,
    ) -> Result<(), BatchError> {
        self.step(|tx| {
            tx.open_table(ATTACHMENTS)?
                .insert(record.id.get(), encode_attachment(record).as_slice())?;
            Ok(())
        })
    }

    pub(crate) fn require_project(&mut self, id: ProjectId) -> Result<(), BatchError> {
        let exists = self.step(|tx| Ok(tx.open_table(PROJECTS)?.get(id.get())?.is_some()))?;
        if exists {
            Ok(())
        } else {
            self.reject(Violation::NotFound(RecordRef::Project(id)))
        }
    }

    pub(crate) fn insert_project(&mut self, record: &ProjectRecord) -> Result<(), BatchError> {
        self.step(|tx| {
            tx.open_table(PROJECTS)?
                .insert(record.id.get(), encode_project(record).as_slice())?;
            Ok(())
        })
    }

    /// Adds `job` to `project`, in `project_member` and its mirror
    /// `job_project`.
    pub(crate) fn link_project(
        &mut self,
        job: JobId,
        project: ProjectId,
    ) -> Result<(), BatchError> {
        let added = self.step(|tx| {
            let mut members = tx.open_table(PROJECT_MEMBERS)?;
            if members.get((project.get(), job.get()))?.is_some() {
                return Ok(false);
            }
            members.insert((project.get(), job.get()), ())?;
            tx.open_table(JOB_PROJECTS)?
                .insert((job.get(), project.get()), ())?;
            Ok(true)
        })?;
        if added {
            Ok(())
        } else {
            self.reject(Violation::AlreadyInProject { job, project })
        }
    }

    // ---- tool calls ------------------------------------------------------------------------

    pub(crate) fn job_tool_evidence(
        &mut self,
        job: JobId,
    ) -> Result<Vec<ToolEvidenceRecord>, BatchError> {
        self.step(|tx| job_tool_evidence(&tx.open_table(TOOL_CALLS)?, job))
    }

    pub(crate) fn tool_evidence(
        &mut self,
        call: ToolCallRef,
    ) -> Result<Option<ToolEvidenceRecord>, BatchError> {
        self.step(|tx| {
            tx.open_table(TOOL_CALLS)?
                .get(tool_call_key(call))?
                .map(|bytes| decode_tool_evidence(call, bytes.value()))
                .transpose()
        })
    }

    pub(crate) fn put_tool_evidence(
        &mut self,
        record: &ToolEvidenceRecord,
    ) -> Result<(), BatchError> {
        self.step(|tx| {
            tx.open_table(TOOL_CALLS)?.insert(
                tool_call_key(record.call),
                encode_tool_evidence(record).as_slice(),
            )?;
            Ok(())
        })
    }

    // ---- request events ----------------------------------------------------------------------

    pub(crate) fn request_observed(
        &mut self,
        job: JobId,
        request: RequestNumber,
    ) -> Result<bool, BatchError> {
        self.step(|tx| {
            let events = tx.open_table(REQUEST_EVENTS)?;
            let observed = events
                .range((job.get(), request.get(), 0)..=(job.get(), request.get(), u32::MAX))?
                .next()
                .is_some();
            Ok(observed)
        })
    }

    /// The eligibility the request's Token events already state, if any.
    pub(crate) fn request_token_eligibility(
        &mut self,
        job: JobId,
        request: RequestNumber,
    ) -> Result<Option<RequestEligibility>, BatchError> {
        self.step(|tx| {
            let events = tx.open_table(REQUEST_EVENTS)?;
            for entry in events
                .range((job.get(), request.get(), 0)..=(job.get(), request.get(), u32::MAX))?
            {
                let (key, bytes) = entry?;
                let event = decode_request_event(job, request, key.value().2, bytes.value())?;
                if let RequestPayload::Token { eligibility, .. } = event.payload {
                    return Ok(Some(eligibility));
                }
            }
            Ok(None)
        })
    }

    /// Appends an event to a request, as its next (gapless) one, and indexes
    /// it by the Connection and Model its Job froze.
    pub(crate) fn insert_request_event(
        &mut self,
        job: JobId,
        request: RequestNumber,
        observed_at: Timestamp,
        payload: RequestPayload,
    ) -> Result<u32, BatchError> {
        let row = self.require_job(job)?;
        let seq = self.step(|tx| {
            let events = tx.open_table(REQUEST_EVENTS)?;
            let last = events
                .range((job.get(), request.get(), 0)..=(job.get(), request.get(), u32::MAX))?
                .next_back()
                .transpose()?
                .map_or(0, |(key, _)| key.value().2);
            Ok(last)
        })?;
        let Some(seq) = seq.checked_add(1) else {
            return self.reject(Violation::ValueOutOfRange("seq"));
        };
        let event = RequestEventRecord {
            job_id: job,
            request,
            seq,
            observed_at,
            recorded_at: self.now,
            payload,
        };
        let scope = row.execution.as_ref().map(|execution| {
            (
                execution.llm.provider.id.get(),
                *ContentDigest::of(execution.llm.config.model.as_bytes()).as_bytes(),
            )
        });
        self.step(|tx| {
            tx.open_table(REQUEST_EVENTS)?.insert(
                (job.get(), request.get(), seq),
                encode_request_event(&event).as_slice(),
            )?;
            if let Some((connection, model_hash)) = scope {
                tx.open_table(REQUEST_SCOPES)?.insert(
                    (
                        connection,
                        model_hash,
                        observed_at.as_unix_micros(),
                        job.get(),
                        request.get(),
                        seq,
                    ),
                    (),
                )?;
            }
            Ok(())
        })?;
        Ok(seq)
    }
}
