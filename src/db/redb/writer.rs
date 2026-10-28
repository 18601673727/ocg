use super::rows::{decode_request_event, encode_request_event};
use super::*;
use crate::db::entities::{
    AttachmentId, Config, ConfigBody, ConfigId, ConfigKind, JobId, OutputKind, ProjectId,
    ProjectRecord, RequestEventRecord, RequestNumber, RequestPayload, SubmissionKey,
    SubmissionRecord, ToolCallRef, ToolEvidenceRecord, Violation,
};
use ::redb::ReadableTableMetadata;
use std::time::Instant;

// ---- batches -------------------------------------------------------------------

/// A failure inside a batch. Only [`Batch`] creates one.
#[derive(Clone, Debug)]
pub(crate) struct BatchError(pub(crate) Failure);

#[derive(Clone, Debug)]
pub(crate) enum Failure {
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
    ProviderOutcome(JobId),
    ProviderAvailability(JobId, AvailabilityStatId, ContentDigest),
}

pub(crate) struct BatchOutcome<T> {
    pub(crate) result: Result<T, BatchFailure>,
    pub(crate) storage_failure: Option<StorageError>,
    pub(crate) durable_commit_us: Option<u64>,
    /// The commit added a `ready` or `delayed` row or changed a Pool.
    pub(crate) scheduling_wake: bool,
    /// The commit freed a held slot.
    pub(crate) capacity_freed: bool,
    pub(crate) catalog_changed: bool,
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
            durable_commit_us: None,
            scheduling_wake: false,
            capacity_freed: false,
            catalog_changed: false,
            probe: None,
        }
    }
}

pub(crate) struct Batch {
    pub(crate) event_append: bool,
    pub(super) tx: WriteTransaction,
    pub(super) now: Timestamp,
    pub(super) epoch: u64,
    pub(super) host_id: Option<String>,
    pub(super) boot_id: Option<String>,
    pub(super) touched_ready_or_delayed: bool,
    pub(super) freed_capacity: bool,
    pub(super) changed_pool: bool,
    pub(super) catalog_changed: bool,
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
    let value = match body(&mut batch) {
        Err(BatchError(failure)) => return finish_abort(batch.tx, failure),
        Ok(value) => value,
    };
    commit_batch(batch, value, commit)
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
    let value = match body(&mut batch) {
        Err(BatchError(failure)) => return finish_abort(batch.tx, failure),
        Ok(None) => {
            return match batch.tx.abort() {
                Ok(()) => BatchOutcome {
                    result: Ok(None),
                    storage_failure: None,
                    durable_commit_us: None,
                    scheduling_wake: false,
                    capacity_freed: false,
                    catalog_changed: false,
                    probe: None,
                },
                Err(error) => {
                    let error = StorageError::from(error);
                    BatchOutcome::failed(BatchFailure::NotCommitted(error.clone()), Some(error))
                }
            };
        }
        Ok(Some(value)) => value,
    };
    commit_batch(batch, Some(value), commit)
}

fn commit_batch<T>(batch: Batch, value: T, commit: Commit) -> BatchOutcome<T> {
    commit_transaction(
        batch.tx,
        value,
        batch.touched_ready_or_delayed || batch.changed_pool,
        batch.freed_capacity,
        batch.probe,
        batch.catalog_changed,
        commit,
    )
}

pub(super) fn commit_transaction<T>(
    tx: WriteTransaction,
    value: T,
    wake: bool,
    freed: bool,
    probe: Option<Probe>,
    catalog_changed: bool,
    commit: Commit,
) -> BatchOutcome<T> {
    let started = (commit == Commit::Durable).then(Instant::now);
    let result = tx.commit();
    let durable_commit_us = started.map(|started| started.elapsed().as_micros() as u64);
    match result {
        Ok(()) => BatchOutcome {
            result: Ok(value),
            storage_failure: None,
            durable_commit_us,
            scheduling_wake: wake,
            capacity_freed: freed,
            catalog_changed,
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
    pub(super) fn new(tx: WriteTransaction, epoch: u64) -> Self {
        Self {
            event_append: false,
            tx,
            now: Timestamp::now(),
            epoch,
            host_id: recovery::current_host_id(),
            boot_id: recovery::current_boot_id(),
            touched_ready_or_delayed: false,
            freed_capacity: false,
            changed_pool: false,
            catalog_changed: false,
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
        Err(BatchError(failure))
    }

    pub(crate) fn corrupted<T>(&mut self, detail: String) -> Result<T, BatchError> {
        self.fail(Failure::Storage(StorageError::corrupted(detail)))
    }

    /// Runs `f` on the transaction.
    pub(super) fn step<T>(
        &mut self,
        f: impl FnOnce(&WriteTransaction) -> Result<T, StorageError>,
    ) -> Result<T, BatchError> {
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

    pub(crate) fn set_global_compaction_settings(
        &mut self,
        mut settings: crate::db::entities::GlobalCompactionSettings,
    ) -> Result<(), BatchError> {
        let current = self.step(|tx| reader::global_compaction_settings(&tx.open_table(META)?))?;
        if settings.revision != current.revision {
            return self.reject(Violation::InvalidValue(
                "revision",
                "global compaction settings changed; reload before saving",
            ));
        }
        self.check_compaction_policy(&settings.policy)?;
        let Some(revision) = settings.revision.checked_add(1) else {
            return self.corrupted("compaction revision exhausted".into());
        };
        settings.revision = revision;
        self.step(|tx| {
            let mut e = Enc::new(TAG_GLOBAL_COMPACTION);
            e.u64(settings.revision);
            e.bool(settings.prediction_enabled);
            e.compaction_policy(&settings.policy);
            tx.open_table(META)?
                .insert("compaction_settings", e.0.as_slice())?;
            Ok(())
        })?;
        self.cancel_stale_context_work()
    }

    fn provision_decision_executor(&mut self, model: ConfigId) -> Result<(), BatchError> {
        use crate::db::entities::*;
        let row = self.require_config(model)?;
        if !matches!(&row.body, ConfigBody::DecisionModel(body) if body.declared_capabilities.contains(&DecisionCapability::Choice))
        {
            return Ok(());
        }
        let has_pool = self.step(|tx| {
            for entry in tx.open_table(CONFIGS)?.iter()? {
                let (id, bytes) = entry?;
                let row = ConfigRow::decode_at(id.value(), bytes.value())?;
                if row.archived_at.is_none() && matches!(row.body, ConfigBody::Pool(pool) if pool.offers(ExecutionOperation::Decision, None)) { return Ok(true); }
            }
            Ok(false)
        })?;
        if !has_pool {
            self.create_config(
                "Decision execution".to_owned(),
                ConfigBody::Pool(PoolConfig {
                    capabilities: BTreeSet::from([(ExecutionOperation::Decision, None)]),
                    concurrency_limit: 1,
                    enabled: true,
                }),
            )?;
        }
        Ok(())
    }

    // ---- configs ---------------------------------------------------------------------

    pub(crate) fn agent_profile_ids(
        &mut self,
        after: u128,
        limit: usize,
    ) -> Result<(Vec<ConfigId>, Option<ConfigId>), BatchError> {
        self.step(|tx| {
            let mut ids = Vec::new();
            let mut next = None;
            for (scanned, entry) in tx
                .open_table(CONFIGS)?
                .range(after.saturating_add(1)..)?
                .enumerate()
            {
                let (key, bytes) = entry?;
                let id = key.value();
                let record = ConfigRow::decode_at(id, bytes.value())?;
                if record.kind == ConfigKind::Agent && record.archived_at.is_none() {
                    ids.push(record.id);
                }
                if ids.len() == limit || scanned == 511 {
                    next = Some(ConfigId::new(id));
                    break;
                }
            }
            Ok((ids, next))
        })
    }

    pub(crate) fn dispatch_selection(
        &mut self,
        approval: ToolCallRef,
    ) -> Result<crate::db::entities::DispatchSelection, BatchError> {
        self.step(|tx| {
            let record = tx
                .open_table(APPROVALS)?
                .get(tool_call_key(approval))?
                .map(|bytes| ApprovalRecord::decode(bytes.value()))
                .transpose()?
                .ok_or_else(|| StorageError::corrupted("missing dispatch selection"))?;
            match record.decision {
                ApprovalDecision::Answered { text } if record.by == Actor::User => {
                    serde_json::from_str(&text)
                        .map_err(|_| StorageError::corrupted("invalid dispatch selection"))
                }
                ApprovalDecision::Approved
                | ApprovalDecision::Rejected
                | ApprovalDecision::Voided
                | ApprovalDecision::Answered { .. } => Err(StorageError::corrupted(
                    "dispatch was not selected by the user",
                )),
            }
        })
    }

    pub(crate) fn config(&mut self, id: ConfigId) -> Result<Option<ConfigRow>, BatchError> {
        let row = self.step(|tx| {
            tx.open_table(CONFIGS)?
                .get(id.get())?
                .map(|bytes| ConfigRow::decode_at(id.get(), bytes.value()))
                .transpose()
        })?;
        Ok(row)
    }

    pub(crate) fn config_record(
        &mut self,
        id: ConfigId,
    ) -> Result<Option<crate::db::entities::ConfigRecord>, BatchError> {
        self.config(id).map(|row| row.map(reader::config_record))
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
        if row.archived_at.is_some() {
            return self.reject(Violation::NotFound(RecordRef::Profile(id.into())));
        }
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
                if model.reasoning_efforts.as_ref().is_some_and(|values| {
                    values.len() > 7
                        || values.iter().any(|value| {
                            crate::db::entities::ReasoningEffort::parse(value).is_none()
                        })
                }) {
                    return self.reject(Violation::InvalidValue(
                        "reasoning_efforts",
                        "must contain supported effort values",
                    ));
                }
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
                if let Some(policy) = &agent.compaction_policy {
                    self.check_compaction_policy(policy)?;
                }

                if agent.capabilities.is_empty()
                    || agent
                        .models
                        .iter()
                        .collect::<std::collections::BTreeSet<_>>()
                        .len()
                        != agent.models.len()
                {
                    return self.reject(Violation::InvalidValue(
                        "capabilities",
                        "select Agent capabilities and unique ordered models",
                    ));
                }
                for model in &agent.models {
                    let record = self.require_config(*model)?;
                    if record.archived_at.is_some() || !agent.supports_model(&record.body) {
                        return self.reject(Violation::InvalidValue(
                            "models",
                            "each assigned model must support every selected Agent capability",
                        ));
                    }
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
            ConfigBody::DecisionModel(dm) => {
                self.require_config_kind(dm.connection_id, ConfigKind::Connection)?;
                for (question, request) in [
                    (
                        dm.limits.max_input_tokens_per_question,
                        dm.limits.max_input_tokens_per_request,
                    ),
                    (
                        dm.limits.max_output_tokens_per_question,
                        dm.limits.max_output_tokens_per_request,
                    ),
                ] {
                    if question.zip(request).is_some_and(|(q, r)| q > r) {
                        return self.reject(Violation::InvalidValue(
                            "limits",
                            "per-question limit cannot exceed per-request limit",
                        ));
                    }
                }
                if dm.declared_capabilities.is_empty() {
                    return self.reject(Violation::InvalidValue(
                        "declared_capabilities",
                        "decision models require at least one supported primitive",
                    ));
                }
                if dm.model.trim().is_empty() || dm.model.len() > 512 {
                    return self.reject(Violation::InvalidValue("model", "must be 1 to 512 bytes"));
                }
                if dm
                    .limits
                    .max_input_tokens_per_request
                    .is_some_and(|v| v == 0 || v > 1_000_000)
                {
                    return self.reject(Violation::InvalidValue(
                        "max_input_tokens_per_request",
                        "must be between 1 and 1000000",
                    ));
                }
                if dm
                    .limits
                    .max_output_tokens_per_request
                    .is_some_and(|v| v == 0 || v > 1_000_000)
                {
                    return self.reject(Violation::InvalidValue(
                        "max_output_tokens_per_request",
                        "must be between 1 and 1000000",
                    ));
                }
                if dm
                    .limits
                    .max_input_tokens_per_question
                    .is_some_and(|v| v == 0 || v > 1_000_000)
                {
                    return self.reject(Violation::InvalidValue(
                        "max_input_tokens_per_question",
                        "must be between 1 and 1000000",
                    ));
                }
                if dm
                    .limits
                    .max_output_tokens_per_question
                    .is_some_and(|v| v == 0 || v > 1_000_000)
                {
                    return self.reject(Violation::InvalidValue(
                        "max_output_tokens_per_question",
                        "must be between 1 and 1000000",
                    ));
                }
            }
            ConfigBody::DecisionAgent(da) => {
                if da
                    .instructions
                    .as_ref()
                    .is_some_and(|text| text.len() > 64 * 1024)
                {
                    return self.reject(Violation::InvalidValue(
                        "instructions",
                        "decision instructions exceed 64 KiB",
                    ));
                }
                // Empty model lists are valid drafts, but cannot be frozen for execution.
                if da.models.len() > 256 {
                    return self.reject(Violation::InvalidValue(
                        "models",
                        "at most 256 decision targets",
                    ));
                }

                if da.models.windows(2).any(|pair| pair[0] >= pair[1]) {
                    return self.reject(Violation::InvalidValue(
                        "models",
                        "must be sorted and unique",
                    ));
                }
                for model in &da.models {
                    let row = self.require_config(*model)?;
                    if row.archived_at.is_some()
                        || !matches!(row.kind, ConfigKind::Model | ConfigKind::DecisionModel)
                    {
                        return self.reject(Violation::InvalidValue(
                            "models",
                            "decision targets must be configured models",
                        ));
                    }
                }
                if da
                    .default_model
                    .is_some_and(|default| !da.models.contains(&default))
                {
                    return self.reject(Violation::InvalidValue(
                        "default_model",
                        "must be one of the Agent's models",
                    ));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn check_compaction_policy(
        &mut self,
        policy: &crate::db::entities::CompactionPolicy,
    ) -> Result<(), BatchError> {
        if let Err(reason) = policy.validate() {
            return self.reject(Violation::InvalidValue("compaction_policy", reason));
        }
        if let Some(model) = policy.summarization_model {
            self.require_config_kind(model, ConfigKind::Model)?;
        }
        if let Some(predictive) = &policy.predictive {
            let agent =
                self.require_config_kind(predictive.decision_agent, ConfigKind::DecisionAgent)?;
            let ConfigBody::DecisionAgent(body) = agent.body else {
                return self.corrupted("mismatched decision agent".into());
            };
            if body.models.is_empty() {
                return self.reject(Violation::InvalidValue(
                    "decision_agent",
                    "predictive compaction requires a configured decision model",
                ));
            }
            let selected = body.default_model.or_else(|| body.models.first().copied());
            for model in body.models {
                let record = self.require_config_kind(model, ConfigKind::DecisionModel)?;
                if selected == Some(model)
                    && !matches!(record.body, ConfigBody::DecisionModel(model) if model.declared_capabilities.contains(&crate::db::entities::DecisionCapability::Choice))
                {
                    return self.reject(Violation::InvalidValue(
                        "decision_agent",
                        "context prediction requires Choice capability",
                    ));
                }
            }
        }
        Ok(())
    }

    fn put_config(&mut self, row: &ConfigRow) -> Result<(), BatchError> {
        self.step(|tx| {
            tx.open_table(CONFIGS)?
                .insert(row.id.get(), row.encode().as_slice())?;
            Ok(())
        })
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
        self.put_config(&row)?;
        if row.kind == ConfigKind::DecisionModel {
            self.provision_decision_executor(id)?;
        }
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
        if existing.archived_at.is_some() {
            return self.reject(Violation::NotFound(RecordRef::Profile(id.into())));
        }
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
        self.put_config(&row)?;
        if row.kind == ConfigKind::DecisionModel {
            self.provision_decision_executor(id)?;
        }
        Ok(())
    }

    pub(crate) fn import_models(
        &mut self,
        connection: Config,
        models: Vec<crate::db::entities::DiscoveredModel>,
        capabilities: std::collections::BTreeSet<crate::db::entities::ModelCapability>,
    ) -> Result<(Vec<ConfigId>, usize), BatchError> {
        use crate::db::entities::{LlmParameters, ModelConfig};
        let ConfigBody::Connection(wanted) = &connection.body else {
            return self.reject(Violation::InvalidValue(
                "connection",
                "must be a Connection",
            ));
        };
        if models.is_empty() || models.len() > 200 || capabilities.is_empty() {
            return self.reject(Violation::InvalidValue(
                "models",
                "select 1 to 200 models with capabilities",
            ));
        }
        let existing = self.step(|tx| {
            for entry in tx.open_table(CONFIGS)?.iter()? {
                let (key, value) = entry?;
                let row = ConfigRow::decode_at(key.value(), value.value())?;
                if row.archived_at.is_none()
                    && matches!(&row.body, ConfigBody::Connection(body) if body == wanted)
                {
                    return Ok(Some(row.id));
                }
            }
            Ok(None)
        })?;
        let connection_id = match existing {
            Some(id) => id,
            None => self.create_config(connection.name, connection.body)?,
        };
        let mut known = self.step(|tx| {
            let mut known = std::collections::BTreeMap::new();
            for entry in tx.open_table(CONFIGS)?.iter()? {
                let (key, value) = entry?;
                let row = ConfigRow::decode_at(key.value(), value.value())?;
                if row.archived_at.is_none()
                    && let ConfigBody::Model(model) = row.body
                    && model.connection_id == connection_id
                    && models.iter().any(|entry| entry.id == model.model)
                {
                    known.entry(model.model).or_insert(row.id);
                }
            }
            Ok(known)
        })?;
        let mut ids = Vec::with_capacity(models.len());
        let mut created = 0;
        for entry in models {
            let crate::db::entities::DiscoveredModel {
                id: model,
                context_window,
                reasoning_efforts,
            } = entry;
            if model.trim().is_empty()
                || model.len() > 256
                || model.chars().any(char::is_control)
                || context_window == Some(0)
            {
                return self.reject(Violation::InvalidValue(
                    "model",
                    "invalid model or context window",
                ));
            }
            let id = match known.get(&model) {
                Some(id) => {
                    let id = *id;
                    let row = self.require_config(id)?;
                    if let ConfigBody::Model(mut body) = row.body
                        && body.context_window.is_none()
                        && context_window.is_some()
                    {
                        body.context_window = context_window;
                        self.update_config(id, row.name, ConfigBody::Model(body))?;
                    }
                    id
                }
                None => {
                    let name: String = model
                        .chars()
                        .scan(0usize, |bytes, ch| {
                            *bytes += ch.len_utf8();
                            (*bytes <= 128).then_some(ch)
                        })
                        .collect();
                    let id = self.create_config(
                        name,
                        ConfigBody::Model(ModelConfig {
                            connection_id,
                            reasoning_efforts,
                            model: model.clone(),
                            context_window,
                            parameters: LlmParameters::default(),
                            declared_capabilities: capabilities.clone(),
                        }),
                    )?;
                    known.insert(model, id);
                    created += 1;
                    id
                }
            };
            ids.push(id);
        }
        Ok((ids, created))
    }

    pub(crate) fn refresh_model_metadata(
        &mut self,
        connection: crate::db::entities::ConnectionConfig,
        models: Vec<crate::db::entities::DiscoveredModel>,
    ) -> Result<(), BatchError> {
        let models = models
            .into_iter()
            .map(|model| (model.id.clone(), model))
            .collect::<std::collections::BTreeMap<_, _>>();
        let rows = self.step(|tx| {
            let mut rows = Vec::new();
            let mut connections = std::collections::BTreeSet::new();
            for entry in tx.open_table(CONFIGS)?.iter()? {
                let (key, value) = entry?;
                let row = ConfigRow::decode_at(key.value(), value.value())?;
                if row.archived_at.is_none() && matches!(&row.body, ConfigBody::Connection(body) if body.protocol == connection.protocol && body.credential_ref == connection.credential_ref && {
                    let default = if body.protocol == LlmTransport::AnthropicMessages { "https://api.anthropic.com/v1" } else { "https://api.openai.com/v1" };
                    body.endpoint.as_deref().unwrap_or(default).trim_end_matches('/') == connection.endpoint.as_deref().unwrap_or(default).trim_end_matches('/')
                }) {
                    connections.insert(row.id);
                }
            }
            for entry in tx.open_table(CONFIGS)?.iter()? {
                let (key, value) = entry?;
                let row = ConfigRow::decode_at(key.value(), value.value())?;
                if row.archived_at.is_none() && matches!(&row.body, ConfigBody::Model(model) if connections.contains(&model.connection_id)) {
                    rows.push(row);
                }
            }
            Ok(rows)
        })?;
        for row in rows {
            let ConfigBody::Model(mut body) = row.body else {
                continue;
            };
            let efforts = models
                .get(&body.model)
                .and_then(|model| model.reasoning_efforts.clone());
            let context_window = body.context_window.or_else(|| {
                models
                    .get(&body.model)
                    .and_then(|model| model.context_window)
                    .filter(|value| *value > 0)
            });
            if body.reasoning_efforts != efforts || body.context_window != context_window {
                body.reasoning_efforts = efforts;
                body.context_window = context_window;
                self.update_config(row.id, row.name, ConfigBody::Model(body))?;
            }
        }
        Ok(())
    }

    pub(crate) fn archive_model(&mut self, id: ConfigId) -> Result<(), BatchError> {
        let mut row = self.require_config(id)?;
        if !matches!(
            row.kind,
            ConfigKind::Model | ConfigKind::DecisionModel | ConfigKind::DecisionAgent
        ) {
            return self.reject(Violation::WrongConfigKind {
                config: id,
                expected: ConfigKind::Model,
                found: row.kind,
            });
        }
        if row.archived_at.is_some() {
            return Ok(());
        }
        let emptied = self.step(|tx| {
            let mut emptied = BTreeSet::new();
            if row.kind == ConfigKind::DecisionAgent {
                emptied.insert(id);
            }
            for entry in tx.open_table(CONFIGS)?.iter()? {
                let (key, value) = entry?;
                let row = ConfigRow::decode_at(key.value(), value.value())?;
                if row.archived_at.is_none()
                    && let ConfigBody::DecisionAgent(agent) = row.body
                    && agent.models.contains(&id)
                    && agent.models.iter().all(|model| *model == id)
                {
                    emptied.insert(row.id);
                }
            }
            Ok(emptied)
        })?;
        let agents = self.step(|tx| {
            let mut agents = Vec::new();
            for entry in tx.open_table(CONFIGS)?.iter()? {
                let (key, value) = entry?;
                let row = ConfigRow::decode_at(key.value(), value.value())?;
                let references_model = match &row.body {
                    ConfigBody::Agent(agent) => {
                        agent.models.contains(&id)
                            || agent.compaction_policy.as_ref().is_some_and(|policy| {
                                policy.summarization_model == Some(id)
                                    || policy.predictive.as_ref().is_some_and(|prediction| {
                                        emptied.contains(&prediction.decision_agent)
                                            || prediction
                                                .acceptance
                                                .as_ref()
                                                .is_some_and(|pin| pin.model.id == id)
                                    })
                            })
                    }
                    ConfigBody::DecisionAgent(agent) => {
                        agent.models.contains(&id) || agent.default_model == Some(id)
                    }
                    ConfigBody::Connection(_)
                    | ConfigBody::Model(_)
                    | ConfigBody::DecisionModel(_)
                    | ConfigBody::Pool(_) => false,
                };
                if row.archived_at.is_none() && references_model {
                    agents.push(row);
                }
            }
            Ok(agents)
        })?;
        let clear_policy = |policy: &mut crate::db::entities::CompactionPolicy| {
            if policy.summarization_model == Some(id) {
                policy.summarization_model = None;
            }
            if policy
                .predictive
                .as_ref()
                .is_some_and(|prediction| emptied.contains(&prediction.decision_agent))
            {
                policy.predictive = None;
            } else if let Some(prediction) = &mut policy.predictive
                && prediction
                    .acceptance
                    .as_ref()
                    .is_some_and(|pin| pin.model.id == id)
            {
                prediction.mode = crate::db::entities::PredictionMode::Shadow;
                prediction.acceptance = None;
            }
        };
        for mut agent_row in agents {
            let before = agent_row.body.clone();
            match &mut agent_row.body {
                ConfigBody::Agent(agent) => {
                    agent.models.retain(|model| *model != id);
                    if let Some(policy) = &mut agent.compaction_policy {
                        clear_policy(policy);
                    }
                }
                ConfigBody::DecisionAgent(agent) => {
                    agent.models.retain(|model| *model != id);
                    if agent.default_model == Some(id) {
                        agent.default_model = None;
                    }
                }
                ConfigBody::Connection(_)
                | ConfigBody::Model(_)
                | ConfigBody::DecisionModel(_)
                | ConfigBody::Pool(_) => {}
            }
            if agent_row.body != before {
                self.update_config(agent_row.id, agent_row.name, agent_row.body)?;
            }
        }
        let conversations = self.step(|tx| {
            let mut changes = Vec::new();
            for entry in tx.open_table(JOB_CONTROLS)?.iter()? {
                let (key, bytes) = entry?;
                if let Some(mut policy) = JobControl::decode(bytes.value())?.compaction_policy {
                    let before = policy.clone();
                    clear_policy(&mut policy);
                    if policy != before {
                        changes.push((JobId::new(key.value()), policy));
                    }
                }
            }
            Ok(changes)
        })?;
        for (conversation, policy) in conversations {
            crate::db::mutations::set_conversation(
                self,
                conversation,
                crate::db::entities::JobEdit::CompactionPolicy(Some(policy)),
            )?;
        }
        let projects = self.step(|tx| {
            let mut changes = Vec::new();
            for entry in tx.open_table(PROJECTS)?.iter()? {
                let (key, bytes) = entry?;
                let project = decode_project(key.value(), bytes.value())?;
                if project
                    .decision
                    .as_ref()
                    .and_then(|policy| policy.decision_profile)
                    .is_some_and(|profile| profile == id || emptied.contains(&profile))
                {
                    changes.push(project);
                }
            }
            Ok(changes)
        })?;
        for project in projects {
            let mut policy = project.decision_policy();
            policy.decision_profile = None;
            crate::db::mutations::configure_project_decision(
                self,
                project.id,
                project.updated_at,
                Some(policy),
            )?;
        }
        let mut settings =
            self.step(|tx| reader::global_compaction_settings(&tx.open_table(META)?))?;
        let before = settings.policy.clone();
        clear_policy(&mut settings.policy);
        if settings.policy != before {
            self.set_global_compaction_settings(settings)?;
        }
        row.archived_at = Some(self.now);
        row.updated_at = self.now;
        row.edit_seq += 1;
        self.put_config(&row)
    }

    // ---- jobs --------------------------------------------------------------------------

    pub(crate) fn job(&mut self, id: JobId) -> Result<Option<JobSpec>, BatchError> {
        self.step(|tx| {
            tx.open_table(JOBS)?
                .get(id.get())?
                .map(|bytes| JobSpec::decode_at(id.get(), bytes.value()))
                .transpose()
        })
    }

    pub(crate) fn require_job(&mut self, id: JobId) -> Result<JobSpec, BatchError> {
        match self.job(id)? {
            Some(row) => Ok(row),
            None => self.reject(Violation::NotFound(RecordRef::Job(id))),
        }
    }

    pub(crate) fn job_state(&mut self, id: JobId) -> Result<Option<JobState>, BatchError> {
        self.step(|tx| {
            tx.open_table(JOB_STATES)?
                .get(id.get())?
                .map(|bytes| JobState::decode_at(id.get(), bytes.value()))
                .transpose()
        })
    }

    pub(crate) fn require_job_state(&mut self, id: JobId) -> Result<JobState, BatchError> {
        match self.job_state(id)? {
            Some(row) => Ok(row),
            None => self.reject(Violation::NotFound(RecordRef::Job(id))),
        }
    }

    /// Inserts a Job spec, which is never rewritten (I1).
    pub(super) fn insert_job(&mut self, row: &JobSpec) -> Result<(), BatchError> {
        self.catalog_changed = true;
        let duplicate = self.step(|tx| {
            let mut jobs = tx.open_table(JOBS)?;
            if jobs.get(row.id.get())?.is_some() {
                return Ok(true);
            }
            jobs.insert(row.id.get(), row.encode().as_slice())?;
            if let Some(allocation) = &row.allocation {
                tx.open_table(JOB_ALLOCATIONS)?
                    .insert((allocation.owner.get(), row.id.get()), ())?;
            }
            if row.kind.is_group() {
                tx.open_table(CONVERSATIONS)?.insert(row.id.get(), ())?;
            } else if row.kind.is_persistent()
                && let Some(group) = row.parent_id
            {
                tx.open_table(TURNS)?
                    .insert((group.get(), row.id.get()), ())?;
            }
            Ok(false)
        })?;
        if duplicate {
            return self.reject(Violation::DuplicateId(RecordRef::Job(row.id)));
        }
        Ok(())
    }

    pub(crate) fn put_job_state(&mut self, job: JobId, state: &JobState) -> Result<(), BatchError> {
        self.catalog_changed = true;
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
        let summary = summary.map(
            |text| match crate::db::entities::output_filter::text(&text) {
                std::borrow::Cow::Borrowed(_) => text,
                std::borrow::Cow::Owned(filtered) => filtered,
            },
        );
        let code = code.map(
            |text| match crate::db::entities::output_filter::text(&text) {
                std::borrow::Cow::Borrowed(_) => text,
                std::borrow::Cow::Owned(filtered) => filtered,
            },
        );
        let value = value.map(
            |value| match crate::db::entities::output_filter::value(&value) {
                std::borrow::Cow::Borrowed(_) => value,
                std::borrow::Cow::Owned(filtered) => filtered,
            },
        );
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
            let completed = tx
                .open_table(JOB_STATES)?
                .get(job.get())?
                .map(|bytes| JobState::decode_at(job.get(), bytes.value()))
                .transpose()?
                .and_then(|state| state.outcome)
                .is_some_and(|outcome| outcome.kind == OutcomeKind::Completed);
            let conversation = tx
                .open_table(JOBS)?
                .get(job.get())?
                .map(|bytes| JobSpec::decode_at(job.get(), bytes.value()))
                .transpose()?
                .and_then(|row| row.parent_id);
            if let Some(text) = indexes::search_output_text(&record, completed)
                && let Some(conversation) = conversation
            {
                indexes::index_search_text(tx, conversation, job, 1, seq, text)?;
            }
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
            let previous = tx
                .open_table(ATTACHMENTS)?
                .get(record.id.get())?
                .map(|bytes| decode_attachment(record.id.get(), bytes.value()))
                .transpose()?;
            indexes::content_reference(tx, record, previous.as_ref())?;
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
        self.catalog_changed = true;
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
                .map(|bytes| decode_project(id.get(), bytes.value()))
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
                let (key, bytes) = entry?;
                roots.push(decode_project_root(key.value(), bytes.value())?);
            }
            Ok(roots)
        })
    }

    // ---- conversations ---------------------------------------------------------------------

    pub(crate) fn job_control(&mut self, root: JobId) -> Result<JobControl, BatchError> {
        self.step(|tx| {
            tx.open_table(JOB_CONTROLS)?
                .get(root.get())?
                .map(|bytes| JobControl::decode(bytes.value()))
                .transpose()?
                .ok_or_else(|| StorageError::corrupted(format!("{root} has no conversation row")))
        })
    }

    pub(crate) fn put_job_control(
        &mut self,
        root: JobId,
        row: &JobControl,
    ) -> Result<(), BatchError> {
        self.catalog_changed = true;
        self.step(|tx| {
            tx.open_table(JOB_CONTROLS)?
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
        self.catalog_changed = true;
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
        body: &crate::db::entities::MessageBody,
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

    pub(crate) fn provider_settlement_probe(
        &mut self,
        job: JobId,
        availability: Option<AvailabilityStatId>,
    ) -> Result<(), BatchError> {
        self.probe = Some(match availability {
            Some(id) => {
                let digest = self.step(|tx| {
                    let index = tx.open_table(JOB_OBSERVATIONS)?;
                    let key = index
                        .get((job.get(), 2, id.get()))?
                        .ok_or_else(|| {
                            StorageError::corrupted("missing provider settlement observation")
                        })?
                        .value();
                    let events = tx.open_table(REQUEST_EVENTS)?;
                    let bytes = events.get((job.get(), key.0, key.1))?.ok_or_else(|| {
                        StorageError::corrupted("missing provider settlement event")
                    })?;
                    Ok(ContentDigest::of(bytes.value()))
                })?;
                Probe::ProviderAvailability(job, id, digest)
            }
            None => Probe::ProviderOutcome(job),
        });
        Ok(())
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
            id: crate::db::entities::RequestEventId::new(self.next_id()?),
            job_id: job,
            request,
            seq,
            observed_at,
            recorded_at: self.now,
            payload,
        };
        let identity =
            self.step(|tx| reader::turn_usage_identity_of(&tx.open_table(JOBS)?, &row))?;
        let scope = indexes::request_scope_key(identity.as_ref(), &event);
        self.step(|tx| {
            tx.open_table(REQUEST_EVENTS)?.insert(
                (job.get(), request.get(), seq),
                encode_request_event(&event).as_slice(),
            )?;
            if let Some(kind) =
                observations::observation_kind(&tx.open_table(REQUEST_EVENTS)?, &event)?
            {
                tx.open_table(JOB_OBSERVATIONS)?
                    .insert((job.get(), kind, event.id.get()), (request.get(), seq))?;
            }
            if let Some(scope) = scope {
                tx.open_table(REQUEST_SCOPES)?.insert(scope, ())?;
                tx.open_table(REQUEST_TIMELINE)?
                    .insert((scope.2, scope.3, scope.4, scope.5), ())?;
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
            Option<crate::db::entities::TokenRequestSample>,
            Vec<RequestEventRecord>,
        ),
        BatchError,
    > {
        let row = self.require_job(job)?;
        if row.kind.is_group() {
            return Ok((None, Vec::new()));
        }
        let target = self.step(|tx| reader::turn_target_of(&tx.open_table(JOBS)?, &row))?;
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
        old_sample: Option<crate::db::entities::TokenRequestSample>,
        mut events: Vec<RequestEventRecord>,
        inserted: Vec<RequestEventRecord>,
    ) -> Result<(), BatchError> {
        events.extend(inserted);
        let row = self.require_job(job)?;
        if row.kind.is_group() {
            return Ok(());
        }
        let Some(identity) =
            self.step(|tx| reader::turn_usage_identity_of(&tx.open_table(JOBS)?, &row))?
        else {
            return Ok(());
        };
        let target = self.step(|tx| reader::turn_target_of(&tx.open_table(JOBS)?, &row))?;
        let parent = match row.parent_id {
            Some(parent) => self.job(parent)?,
            None => None,
        };
        let agent = usage_agent(&row, parent.as_ref());
        let provider = identity.provider;
        let model_hash = *ContentDigest::of(identity.model.as_bytes()).as_bytes();
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
                    .div_euclid(crate::db::entities::USAGE_SUMMARY_HOUR_MICROS);
                let key = (hour, agent, provider, model_hash);
                let current_val = table.get(key)?.map(|c| c.value()).ok_or_else(|| {
                    StorageError::corrupted(format!("missing usage bucket for {key:?}"))
                })?;
                let old_complete = if old.usage == crate::db::entities::TokenUsageState::Complete {
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
                    .div_euclid(crate::db::entities::USAGE_SUMMARY_HOUR_MICROS);
                let key = (hour, agent, provider, model_hash);
                let current_val = table
                    .get(key)?
                    .map(|v| v.value())
                    .unwrap_or((0, 0, 0, 0, 0, 0, 0));
                let new_complete = if new.usage == crate::db::entities::TokenUsageState::Complete {
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

impl Batch {
    pub(crate) fn has_user_input(&mut self, group: JobId) -> Result<bool, BatchError> {
        self.step(|tx| Ok(tx.open_table(USER_INPUT_AT)?.get(group.get())?.is_some()))
    }
    pub(crate) fn child_groups(
        &mut self,
        parent: JobId,
        after: u128,
        limit: usize,
    ) -> Result<Vec<JobId>, BatchError> {
        self.step(|tx| {
            tx.open_table(CHILD_GROUPS)?
                .range((parent.get(), after.saturating_add(1))..=(parent.get(), u128::MAX))?
                .take(limit)
                .map(|entry| Ok(JobId::new(entry?.0.value().1)))
                .collect()
        })
    }
    pub(crate) fn has_user_message(&mut self, job: JobId) -> Result<bool, BatchError> {
        self.step(|tx| {
            Ok(reader::effective_entries(reader::job_entries_of(
                &tx.open_table(JOB_INPUTS)?,
                job,
            )?)?
            .iter()
            .any(|(_, entry)| matches!(entry.entry, Entry::UserMessage { .. })))
        })
    }
    pub(crate) fn previous_tool_evidence(
        &mut self,
        call: ToolCallRef,
    ) -> Result<Vec<ToolEvidenceRecord>, BatchError> {
        self.step(|tx| {
            tx.open_table(TOOL_CALLS)?
                .range((call.job.get(), 0, 0)..(call.job.get(), call.request.get(), call.index))?
                .map(|entry| {
                    let (key, bytes) = entry?;
                    rows::decode_tool_evidence(rows::tool_call_ref(key.value())?, bytes.value())
                })
                .collect()
        })
    }
}

impl Batch {
    pub(crate) fn link_job_group(&mut self, parent: JobId, child: JobId) -> Result<(), BatchError> {
        self.step(|tx| {
            tx.open_table(CHILD_GROUPS)?
                .insert((parent.get(), child.get()), ())?;
            tx.open_table(LIVE_CHILDREN)?
                .insert((parent.get(), child.get()), ())?;
            Ok(())
        })
    }
    pub(crate) fn pending_job_count(&mut self) -> Result<u64, BatchError> {
        self.step(|tx| Ok(tx.open_table(PENDING)?.len()?))
    }
    pub(crate) fn record_submission(
        &mut self,
        record: &SubmissionRecord,
    ) -> Result<(), BatchError> {
        self.step(|tx| {
            tx.open_table(SUBMISSIONS)?.insert(
                record.key.as_bytes().as_slice(),
                encode_submission(record).as_slice(),
            )?;
            Ok(())
        })?;
        self.probe = Some(Probe::Submission(record.key));
        Ok(())
    }
}

impl Batch {
    pub(crate) fn group_allocation(
        &mut self,
        group: JobId,
    ) -> Result<Option<crate::db::entities::JobAllocation>, BatchError> {
        self.step(|tx| {
            let jobs = tx.open_table(JOBS)?;
            for entry in tx
                .open_table(CHILDREN)?
                .range((group.get(), 0)..=(group.get(), u128::MAX))?
            {
                let (key, _) = entry?;
                let job = key.value().1;
                let bytes = jobs
                    .get(job)?
                    .ok_or_else(|| StorageError::corrupted("child job missing"))?;
                let row = JobSpec::decode_at(job, bytes.value())?;
                if row.kind.is_persistent() {
                    return Ok(row.allocation);
                }
            }
            Ok(None)
        })
    }
}

impl Batch {
    pub(crate) fn wake_scheduler(&mut self) {
        self.touched_ready_or_delayed = true;
    }
    pub(crate) fn job_notice(
        &mut self,
        job: JobId,
        seq: u32,
    ) -> Result<Option<Notice>, BatchError> {
        self.step(|tx| {
            tx.open_table(NOTICES)?
                .get((job.get(), seq))?
                .map(|bytes| Notice::decode(bytes.value()))
                .transpose()
        })
    }
    pub(crate) fn open_job_dependencies(&mut self, job: JobId) -> Result<u32, BatchError> {
        self.step(|tx| {
            Ok(tx
                .open_table(UNSETTLED)?
                .get(job.get())?
                .map_or(0, |counts| counts.value().0))
        })
    }
    pub(crate) fn remove_job_from_ready(&mut self, row: &JobSpec) -> Result<(), BatchError> {
        let key = self.execution_key(row)?;
        self.step(|tx| {
            tx.open_table(READY)?.remove((key, row.id.get()))?;
            if let Some(at) = row.kind.eligible_at() {
                tx.open_table(DELAYED)?
                    .remove((at.as_unix_micros(), row.id.get()))?;
            }
            Ok(())
        })
    }
    pub(crate) fn job_inputs(
        &mut self,
        job: JobId,
    ) -> Result<Vec<(u32, JobInputEntry)>, BatchError> {
        self.step(|tx| reader::job_entries_of(&tx.open_table(JOB_INPUTS)?, job))
    }
    pub(crate) fn effective_job_inputs(
        &mut self,
        job: JobId,
    ) -> Result<Vec<(u32, JobInputEntry)>, BatchError> {
        self.step(|tx| {
            reader::effective_entries(reader::job_entries_of(&tx.open_table(JOB_INPUTS)?, job)?)
        })
    }
    pub(crate) fn job_retries(&mut self, job: JobId) -> Result<Vec<(JobId, bool)>, BatchError> {
        self.step(|tx| {
            tx.open_table(RETRIES)?
                .range((job.get(), 0)..=(job.get(), u128::MAX))?
                .map(|entry| {
                    let (key, agent) = entry?;
                    Ok((JobId::new(key.value().1), agent.value()))
                })
                .collect()
        })
    }
    pub(crate) fn approval_effects(
        &mut self,
        job: JobId,
    ) -> Result<Vec<ToolEvidenceRecord>, BatchError> {
        self.step(|tx| {
            let calls = tx.open_table(TOOL_CALLS)?;
            let links = tx.open_table(APPROVAL_CALLS)?;
            let mut effects = Vec::new();
            for record in job_tool_evidence(&calls, job)? {
                if record.approval.is_some() {
                    effects.push(record.clone());
                }
                if let Some(executing) = links.get(tool_call_key(record.call))? {
                    let call = tool_call_ref(executing.value())?;
                    let bytes = calls
                        .get(tool_call_key(call))?
                        .ok_or_else(|| StorageError::corrupted("approval execution missing"))?;
                    effects.push(decode_tool_evidence(call, bytes.value())?);
                }
            }
            Ok(effects)
        })
    }
    pub(crate) fn has_open_job_approvals(&mut self, group: JobId) -> Result<bool, BatchError> {
        self.step(|tx| {
            Ok(tx
                .open_table(OPEN_APPROVALS)?
                .range((group.get(), 0, 0, 0)..=(group.get(), u128::MAX, u32::MAX, u32::MAX))?
                .next()
                .transpose()?
                .is_some())
        })
    }
}
