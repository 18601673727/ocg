use super::rows::{decode_request_event, encode_request_event};
use super::*;
use crate::persistence::{
    AttachmentId, ConfigBody, ConfigId, ConfigKind, JobId, OutputKind, ProjectId, ProjectRecord,
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
    Job(JobId),
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
    fn failed_storage(error: StorageError) -> Self {
        Self::failed(BatchFailure::NotCommitted(error.clone()), Some(error))
    }

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
    pub(crate) event_append: bool,
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
    Ok(Batch::new(tx, epoch))
}

pub(crate) fn run_batch_with<T>(
    database: &Database,
    epoch: u64,
    commit: Commit,
    body: impl FnOnce(&mut Batch) -> Result<T, BatchError>,
) -> BatchOutcome<T> {
    let mut batch = match begin(database, epoch, commit) {
        Ok(batch) => batch,
        Err(error) => return BatchOutcome::failed_storage(error),
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
        Err(error) => return BatchOutcome::failed_storage(error),
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
            event_append: false,
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

    pub(crate) fn next_id(&mut self) -> Result<u128, BatchError> {
        self.step(allocate)
    }

    // ---- configs ---------------------------------------------------------------------

    pub(crate) fn config(&mut self, id: ConfigId) -> Result<Option<ConfigRow>, BatchError> {
        let row = self.step(|tx| {
            tx.open_table(CONFIGS)?
                .get(id.get())?
                .map(|bytes| ConfigRow::decode_at(id.get(), bytes.value()))
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
                for (field, value) in [
                    ("temperature", model.parameters.temperature),
                    ("top_p", model.parameters.top_p),
                    ("presence_penalty", model.parameters.presence_penalty),
                    ("frequency_penalty", model.parameters.frequency_penalty),
                ] {
                    if value.is_some_and(|value| !value.is_finite()) {
                        return self.reject(Violation::InvalidValue(field, "must be finite"));
                    }
                }
            }
            ConfigBody::Agent(agent) => {
                if agent.approve.windows(2).any(|pair| pair[0] >= pair[1]) {
                    return self.reject(Violation::InvalidValue(
                        "approve",
                        "must be sorted and unique",
                    ));
                }
                if agent.models.windows(2).any(|pair| pair[0] >= pair[1]) {
                    return self.reject(Violation::InvalidValue(
                        "models",
                        "must be sorted and unique",
                    ));
                }
                if agent.dispatch_to.windows(2).any(|pair| pair[0] >= pair[1]) {
                    return self.reject(Violation::InvalidValue(
                        "dispatch_to",
                        "must be sorted and unique",
                    ));
                }
                for target in &agent.dispatch_to {
                    self.require_config_kind(*target, ConfigKind::Agent)?;
                }
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
        let id = ConfigId::new(self.next_id()?);
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
                .map(|bytes| JobRow::decode_at(id.get(), bytes.value()))
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
                .map(|bytes| JobStateRow::decode_at(id.get(), bytes.value()))
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
    ) -> Result<(u32, JobOutputId), BatchError> {
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
        let id = JobOutputId::new(self.next_id()?);
        let record = JobOutputRecord {
            id,
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
        Ok((seq, id))
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
                .map(|bytes| decode_attachment(id.get(), bytes.value()))
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

    pub(crate) fn require_project_record(
        &mut self,
        id: ProjectId,
    ) -> Result<ProjectRecord, BatchError> {
        let record = self.step(|tx| {
            tx.open_table(PROJECTS)?
                .get(id.get())?
                .map(|bytes| decode_project(bytes.value()))
                .transpose()
        })?;
        match record {
            Some(record) => Ok(record),
            None => self.reject(Violation::NotFound(RecordRef::Project(id))),
        }
    }

    /// Every Project's root, to refuse one that overlaps another.
    pub(crate) fn project_roots(&mut self) -> Result<Vec<(ProjectId, String)>, BatchError> {
        self.step(|tx| {
            let mut roots = Vec::new();
            for entry in tx.open_table(PROJECTS)?.iter()? {
                roots.push(decode_project_root(entry?.1.value())?);
            }
            Ok(roots)
        })
    }

    // ---- conversations ---------------------------------------------------------------------

    pub(crate) fn conversation(&mut self, root: JobId) -> Result<ConversationRow, BatchError> {
        self.step(|tx| {
            tx.open_table(CONVERSATIONS)?
                .get(root.get())?
                .map(|bytes| ConversationRow::decode(bytes.value()))
                .transpose()?
                .ok_or_else(|| StorageError::corrupted(format!("{root} has no conversation row")))
        })
    }

    pub(crate) fn insert_conversation(
        &mut self,
        root: JobId,
        row: &ConversationRow,
    ) -> Result<(), BatchError> {
        self.step(|tx| {
            tx.open_table(CONVERSATIONS)?
                .insert(root.get(), row.encode().as_slice())?;
            Ok(())
        })
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

    pub(crate) fn put_message(
        &mut self,
        body: &crate::persistence::MessageBody,
    ) -> Result<(), BatchError> {
        self.step(|tx| {
            let mut table = tx.open_table(MESSAGES)?;
            {
                if let Some(existing) = table.get(body.digest.as_bytes().as_slice())? {
                    if existing.value() != body.bytes.as_slice() {
                        return Err(StorageError::corrupted("message digest collision"));
                    }
                    return Ok(());
                }
            }
            table.insert(body.digest.as_bytes().as_slice(), body.bytes.as_slice())?;
            Ok(())
        })
    }

    pub(crate) fn require_messages(
        &mut self,
        digests: impl Iterator<Item = ContentDigest>,
    ) -> Result<(), BatchError> {
        let missing = self.step(|tx| {
            let table = tx.open_table(MESSAGES)?;
            for digest in digests {
                if table.get(digest.as_bytes().as_slice())?.is_none() {
                    return Ok(true);
                }
            }
            Ok(false)
        })?;
        if missing {
            return self.reject(Violation::InvalidValue(
                "messages",
                "request references a missing message",
            ));
        }
        Ok(())
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
    /// it by the Connection and Model its Job froze. The event is returned so
    /// a caller that reduces the request right after can feed the reduction
    /// without reading the table again.
    pub(crate) fn insert_request_event(
        &mut self,
        job: JobId,
        request: RequestNumber,
        observed_at: Timestamp,
        payload: RequestPayload,
    ) -> Result<RequestEventRecord, BatchError> {
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
            id: crate::persistence::RequestEventId::new(self.next_id()?),
            job_id: job,
            request,
            seq,
            observed_at,
            recorded_at: self.now,
            payload,
        };
        let execution = if row.kind.is_conversation() {
            None
        } else {
            Some(self.turn_execution(&row)?.0)
        };
        let scope = derived::request_scope_key(execution.as_ref(), &event);
        self.step(|tx| {
            tx.open_table(REQUEST_EVENTS)?.insert(
                (job.get(), request.get(), seq),
                encode_request_event(&event).as_slice(),
            )?;
            if let Some(scope) = scope {
                tx.open_table(REQUEST_SCOPES)?.insert(scope, ())?;
            }
            Ok(())
        })?;
        Ok(event)
    }

    /// The request's events as stored now, and its Token sample reduced from
    /// them. The events are returned so the caller can reduce the request
    /// again after appending to it, without a second read of the table.
    pub(crate) fn usage_sample(
        &mut self,
        job: JobId,
        request: RequestNumber,
    ) -> Result<
        (
            Option<crate::token_usage::TokenRequestSample>,
            Vec<RequestEventRecord>,
        ),
        BatchError,
    > {
        let row = self.require_job(job)?;
        if row.kind.is_conversation() {
            return Ok((None, Vec::new()));
        }
        let execution = self.turn_execution(&row)?.0;
        let target = Some(LlmTarget {
            llm_profile: execution.llm.model.id.into(),
            frozen: execution.llm.clone(),
        });
        let events = self.step(|tx| {
            let table = tx.open_table(REQUEST_EVENTS)?;
            let mut events = Vec::new();
            for entry in
                table.range((job.get(), request.get(), 0)..=(job.get(), request.get(), u32::MAX))?
            {
                let (key, bytes) = entry?;
                let (_, req, seq) = key.value();
                if let Some(r) = RequestNumber::new(req) {
                    events.push(decode_request_event(job, r, seq, bytes.value())?);
                }
            }
            Ok(events)
        })?;
        if events.is_empty() {
            return Ok((None, events));
        }
        let sample = observations::usage_sample_of(job, request, &target, &events);
        Ok((sample, events))
    }

    /// Replaces the request's usage bucket with the sample its events now
    /// reduce to. `events` is what the first read returned and `inserted` the
    /// events this transaction just appended; they are reduced as one list.
    pub(crate) fn update_usage_total(
        &mut self,
        job: JobId,
        request: RequestNumber,
        old_sample: Option<crate::token_usage::TokenRequestSample>,
        mut events: Vec<RequestEventRecord>,
        inserted: Vec<RequestEventRecord>,
    ) -> Result<(), BatchError> {
        events.extend(inserted);
        let row = self.require_job(job)?;
        if row.kind.is_conversation() {
            return Ok(());
        }
        let execution = self.turn_execution(&row)?.0;
        let parent = match row.parent_id {
            Some(parent) => self.job(parent)?,
            None => None,
        };
        let agent = usage_agent(&row, parent.as_ref());
        let provider = execution.llm.provider.id.get();
        let model_hash = *ContentDigest::of(execution.llm.config.model.as_bytes()).as_bytes();
        let target = Some(LlmTarget {
            llm_profile: execution.llm.model.id.into(),
            frozen: execution.llm.clone(),
        });
        if events.is_empty() {
            return Ok(());
        }
        let new_sample = observations::usage_sample_of(job, request, &target, &events);

        if old_sample == new_sample {
            return Ok(());
        }

        self.step(|tx| {
            let mut table = tx.open_table(USAGE_TOTALS)?;
            if let Some(old) = old_sample {
                let hour = old
                    .observed_at
                    .as_unix_micros()
                    .div_euclid(crate::persistence::USAGE_SUMMARY_HOUR_MICROS);
                let key = (hour, agent, provider, model_hash);
                let current_val = table.get(key)?.map(|c| c.value()).ok_or_else(|| {
                    StorageError::corrupted(format!("missing usage bucket for {key:?}"))
                })?;
                let old_complete = if old.usage == crate::token_usage::TokenUsageState::Complete {
                    1
                } else {
                    0
                };
                let reqs = current_val
                    .0
                    .checked_sub(1)
                    .ok_or_else(|| StorageError::corrupted("usage total requests underflow"))?;
                if reqs == 0 {
                    let comp = current_val.1.checked_sub(old_complete).ok_or_else(|| {
                        StorageError::corrupted("usage total complete_requests underflow")
                    })?;
                    let inp = current_val
                        .2
                        .checked_sub(old.input.value().unwrap_or(0) as u64)
                        .ok_or_else(|| StorageError::corrupted("usage total input underflow"))?;
                    let out = current_val
                        .3
                        .checked_sub(old.output.value().unwrap_or(0) as u64)
                        .ok_or_else(|| StorageError::corrupted("usage total output underflow"))?;
                    let cr = current_val
                        .4
                        .checked_sub(old.cache_read.value().unwrap_or(0) as u64)
                        .ok_or_else(|| {
                            StorageError::corrupted("usage total cache_read underflow")
                        })?;
                    let cw = current_val
                        .5
                        .checked_sub(old.cache_write.value().unwrap_or(0) as u64)
                        .ok_or_else(|| {
                            StorageError::corrupted("usage total cache_write underflow")
                        })?;
                    let reas = current_val
                        .6
                        .checked_sub(old.reasoning.value().unwrap_or(0) as u64)
                        .ok_or_else(|| {
                            StorageError::corrupted("usage total reasoning underflow")
                        })?;
                    if comp != 0 || inp != 0 || out != 0 || cr != 0 || cw != 0 || reas != 0 {
                        return Err(StorageError::corrupted(
                            "usage total non-zero counters when requests reached 0",
                        ));
                    }
                    table.remove(key)?;
                } else {
                    let updated = (
                        reqs,
                        current_val.1.checked_sub(old_complete).ok_or_else(|| {
                            StorageError::corrupted("usage total complete_requests underflow")
                        })?,
                        current_val
                            .2
                            .checked_sub(old.input.value().unwrap_or(0) as u64)
                            .ok_or_else(|| {
                                StorageError::corrupted("usage total input underflow")
                            })?,
                        current_val
                            .3
                            .checked_sub(old.output.value().unwrap_or(0) as u64)
                            .ok_or_else(|| {
                                StorageError::corrupted("usage total output underflow")
                            })?,
                        current_val
                            .4
                            .checked_sub(old.cache_read.value().unwrap_or(0) as u64)
                            .ok_or_else(|| {
                                StorageError::corrupted("usage total cache_read underflow")
                            })?,
                        current_val
                            .5
                            .checked_sub(old.cache_write.value().unwrap_or(0) as u64)
                            .ok_or_else(|| {
                                StorageError::corrupted("usage total cache_write underflow")
                            })?,
                        current_val
                            .6
                            .checked_sub(old.reasoning.value().unwrap_or(0) as u64)
                            .ok_or_else(|| {
                                StorageError::corrupted("usage total reasoning underflow")
                            })?,
                    );
                    table.insert(key, updated)?;
                }
            }
            if let Some(new) = new_sample {
                let hour = new
                    .observed_at
                    .as_unix_micros()
                    .div_euclid(crate::persistence::USAGE_SUMMARY_HOUR_MICROS);
                let key = (hour, agent, provider, model_hash);
                let current_val = table
                    .get(key)?
                    .map(|v| v.value())
                    .unwrap_or((0, 0, 0, 0, 0, 0, 0));
                let new_complete = if new.usage == crate::token_usage::TokenUsageState::Complete {
                    1
                } else {
                    0
                };
                let updated = (
                    current_val.0.saturating_add(1),
                    current_val.1.saturating_add(new_complete),
                    current_val
                        .2
                        .saturating_add(new.input.value().unwrap_or(0) as u64),
                    current_val
                        .3
                        .saturating_add(new.output.value().unwrap_or(0) as u64),
                    current_val
                        .4
                        .saturating_add(new.cache_read.value().unwrap_or(0) as u64),
                    current_val
                        .5
                        .saturating_add(new.cache_write.value().unwrap_or(0) as u64),
                    current_val
                        .6
                        .saturating_add(new.reasoning.value().unwrap_or(0) as u64),
                );
                table.insert(key, updated)?;
            }
            Ok(())
        })?;
        Ok(())
    }
}
