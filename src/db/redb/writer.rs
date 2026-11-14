use super::rows::{decode_request_event, encode_request_event};
use super::*;
use crate::db::entities::{
    AttachmentId, ConfigBody, ConfigId, ConfigKind, ConnectionConfig, DecisionModelConfig, JobId,
    ModelConfig, OutputKind, ProjectId, ProjectRecord, RequestEventRecord, RequestNumber,
    RequestPayload, SubmissionKey, SubmissionRecord, ToolCallRef, ToolEvidenceRecord, Violation,
};
use std::time::Instant;

#[derive(Clone, Debug)]
pub(crate) struct BatchError(pub(crate) Failure);

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
}

pub(crate) struct BatchOutcome<T> {
    pub(crate) result: Result<T, BatchFailure>,
    pub(crate) storage_failure: Option<StorageError>,
    pub(crate) durable_commit_us: Option<u64>,
    pub(crate) scheduling_wake: bool,
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
    pub(super) tx: WriteTransaction,
    pub(super) now: Timestamp,
    pub(super) epoch: u64,
    pub(super) commit_mode: Option<Commit>,
    pub(super) host_id: Option<String>,
    pub(super) boot_id: Option<String>,
    pub(super) touched_ready_or_delayed: bool,
    pub(super) freed_capacity: bool,
    pub(super) changed_pool: bool,
    pub(super) catalog_changed: bool,
    pub(super) probe: Option<Probe>,
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
            tx,
            now: Timestamp::now(),
            epoch,
            commit_mode: None,
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

    pub(super) fn step<T>(
        &mut self,
        f: impl FnOnce(&WriteTransaction) -> Result<T, StorageError>,
    ) -> Result<T, BatchError> {
        match f(&self.tx) {
            Ok(value) => Ok(value),
            Err(error) => self.fail(Failure::Storage(error)),
        }
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
        })
    }

    fn provision_decision_executor(&mut self, model: ConfigId) -> Result<(), BatchError> {
        use crate::db::entities::*;
        let row = self.require_config(model)?;
        if !matches!(&row.body, ConfigBody::DecisionModel(body) if body.declared_capabilities.contains(&DecisionCapability::Choice))
        {
            return Ok(());
        }
        self.require_native_capability(ExecutionOperation::Decision, None)
    }

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
                let record = ConfigRow::scan_at(id, bytes.value())?;
                if let Some(record) = record
                    && record.kind == ConfigKind::Agent
                    && record.archived_at.is_none()
                {
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

    pub(crate) fn config(&mut self, id: ConfigId) -> Result<Option<ConfigRow>, BatchError> {
        self.get(CONFIGS, id.get(), |bytes| {
            ConfigRow::decode_at(id.get(), bytes)
        })
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

    fn resolve_and_write_credential(
        &mut self,
        change: crate::db::mutations::ProfileCredentialRef,
        prior: Option<String>,
        credential: Option<&crate::db::mutations::CredentialPayload>,
    ) -> Result<Option<String>, BatchError> {
        use crate::db::mutations::ProfileCredentialRef;
        let base = match change {
            ProfileCredentialRef::Preserve => prior,
            ProfileCredentialRef::Clear => None,
            ProfileCredentialRef::Explicit(reference) => Some(reference),
            ProfileCredentialRef::PreserveOrGenerate(base) => match prior {
                Some(reference) => Some(reference),
                None => Some(self.allocate_credential_ref(&base)?),
            },
        };
        let resolved = match (credential, base) {
            (Some(_), Some(reference)) => Some(self.credential_generation(&reference)?),
            (Some(_), None) => {
                return self.reject(Violation::InvalidValue(
                    "credential_ref",
                    "credential secret requires a reference",
                ));
            }
            (None, reference) => reference,
        };
        if let (Some(secret), Some(reference)) = (credential, resolved.as_deref()) {
            self.write_credential(reference, secret)?;
        }
        Ok(resolved)
    }

    fn find_or_create_connection(
        &mut self,
        name: String,
        body: ConnectionConfig,
        require_unarchived: bool,
    ) -> Result<ConfigId, BatchError> {
        let existing = self.step(|tx| {
            let mut found = None;
            for entry in tx.open_table(CONFIGS)?.iter()? {
                let (key, value) = entry?;
                let Some(row) = ConfigRow::scan_at(key.value(), value.value())? else {
                    continue;
                };
                if (!require_unarchived || row.archived_at.is_none())
                    && matches!(&row.body, ConfigBody::Connection(c) if c == &body)
                {
                    found = Some(row.id);
                    if require_unarchived {
                        break;
                    }
                }
            }
            Ok(found)
        })?;
        match existing {
            Some(id) => Ok(id),
            None => self.create_config(name, ConfigBody::Connection(body)),
        }
    }

    pub(crate) fn save_model_profile(
        &mut self,
        save: crate::db::mutations::ModelProfileSave,
    ) -> Result<(ConfigId, Option<String>), BatchError> {
        use crate::db::mutations::ModelProfileDefinition;

        let profile_kind = match &save.definition {
            ModelProfileDefinition::Llm { .. } => ConfigKind::Model,
            ModelProfileDefinition::Decision { .. } => ConfigKind::DecisionModel,
        };
        let previous = save
            .existing_id
            .map(|id| self.require_config_kind(id.into(), profile_kind))
            .transpose()?;
        let previous_connection = if let Some(profile) = &previous {
            let connection_id = match &profile.body {
                ConfigBody::Model(model) => model.connection_id,
                ConfigBody::DecisionModel(model) => model.connection_id,
                ConfigBody::Connection(_)
                | ConfigBody::Agent(_)
                | ConfigBody::DecisionAgent(_)
                | ConfigBody::Pool(_) => {
                    return self.reject(Violation::WrongConfigKind {
                        config: profile.id,
                        expected: profile_kind,
                        found: profile.kind,
                    });
                }
            };
            let Some(connection) = self.config(connection_id)? else {
                return self.corrupted("Model Profile Connection is missing".into());
            };
            let ConfigBody::Connection(connection_body) = &connection.body else {
                return self.corrupted("Model Profile references a non-Connection config".into());
            };
            if let Some(guard) = &save.guard
                && (guard.profile_edit_seq != profile.edit_seq
                    || guard.connection_id != connection_id
                    || guard.connection_edit_seq != connection.edit_seq)
            {
                return self.reject(Violation::ProfileChanged);
            }
            Some((connection_id, connection_body.clone()))
        } else {
            if save.guard.is_some() {
                return self.reject(Violation::ProfileChanged);
            }
            None
        };

        let protocol = match &save.definition {
            ModelProfileDefinition::Llm { protocol, .. } => protocol
                .or_else(|| {
                    previous_connection
                        .as_ref()
                        .map(|(_, connection)| connection.protocol)
                })
                .unwrap_or(LlmTransport::OpenAiChatCompletions),
            ModelProfileDefinition::Decision { .. } => LlmTransport::OpenAiChatCompletions,
        };
        let prior_credential_ref = previous_connection
            .as_ref()
            .and_then(|(_, connection)| connection.credential_ref.clone());
        let credential_ref = self.resolve_and_write_credential(
            save.credential_ref,
            prior_credential_ref,
            save.credential.as_ref(),
        )?;

        let previous_headers = previous_connection
            .as_ref()
            .map(|(_, connection)| connection.headers.clone())
            .unwrap_or_default();
        let headers = self.apply_header_change(&previous_headers, save.headers)?;

        if matches!(&save.definition, ModelProfileDefinition::Llm { .. }) {
            self.ensure_llm_executor(protocol)?;
        }

        let connection_body = ConnectionConfig {
            protocol,
            endpoint: save.endpoint,
            credential_ref: credential_ref.clone(),
            headers,
        };
        let connection_id =
            self.find_or_create_connection(save.connection_name, connection_body, false)?;
        if profile_kind == ConfigKind::Model {
            self.set_config_enabled(connection_id, ConfigKind::Connection, true)?;
        }

        let body = match save.definition {
            ModelProfileDefinition::Llm {
                model,
                context_window,
                parameters,
                declared_capabilities,
                ..
            } => {
                let reasoning_efforts = previous.as_ref().and_then(|profile| {
                    let ConfigBody::Model(previous) = &profile.body else {
                        return None;
                    };
                    (previous.connection_id == connection_id && previous.model == model)
                        .then(|| previous.reasoning_efforts.clone())
                        .flatten()
                });
                ConfigBody::Model(ModelConfig {
                    reasoning_efforts,
                    connection_id,
                    model,
                    context_window,
                    parameters,
                    declared_capabilities,
                })
            }
            ModelProfileDefinition::Decision {
                protocol,
                model,
                limits,
                declared_capabilities,
            } => ConfigBody::DecisionModel(DecisionModelConfig {
                connection_id,
                model,
                protocol,
                limits,
                declared_capabilities,
            }),
        };
        let profile_id = match save.existing_id {
            Some(id) => {
                self.update_config(id.into(), save.name, body)?;
                id.into()
            }
            None => self.create_config(save.name, body)?,
        };
        self.purge_unused_credentials()?;
        Ok((profile_id, credential_ref))
    }

    fn apply_header_change(
        &mut self,
        previous: &[crate::db::entities::OutboundHeader],
        change: crate::db::mutations::HeaderChange,
    ) -> Result<Vec<crate::db::entities::OutboundHeader>, BatchError> {
        use crate::db::entities::{HeaderScope, OutboundHeader, OutboundHeaderValue};
        use crate::db::mutations::{HeaderChange, HeaderEditValue};
        let edits = match change {
            HeaderChange::Preserve => return Ok(previous.to_vec()),
            HeaderChange::Replace(edits) => edits,
        };
        let mut fields = Vec::with_capacity(edits.len());
        for edit in edits {
            let existing = previous.iter().find_map(|field| match &field.value {
                OutboundHeaderValue::Secret(reference) if field.name == edit.name => {
                    Some(reference.clone())
                }
                OutboundHeaderValue::Secret(_) | OutboundHeaderValue::Plain(_) => None,
            });
            let value = match edit.value {
                HeaderEditValue::Plain(value) => OutboundHeaderValue::Plain(value),
                HeaderEditValue::Secret(None) => match existing {
                    Some(reference) => OutboundHeaderValue::Secret(reference),
                    None => {
                        return self.reject(Violation::InvalidValue(
                            "headers",
                            "no stored secret exists for this header; supply its value",
                        ));
                    }
                },
                HeaderEditValue::Secret(Some(payload)) => {
                    let sanitized: String = edit
                        .name
                        .chars()
                        .map(|character| {
                            if character.is_ascii_alphanumeric() || character == '-' {
                                character
                            } else {
                                '-'
                            }
                        })
                        .take(100)
                        .collect();
                    let reference = self.allocate_credential_ref(&format!("header-{sanitized}"))?;
                    self.write_credential(&reference, &payload)?;
                    OutboundHeaderValue::Secret(reference)
                }
            };
            fields.push(OutboundHeader {
                name: edit.name,
                value,
            });
        }
        fields.sort_by(|left, right| left.name.cmp(&right.name));
        if let Err(error) = crate::db::entities::validate_fields(&fields, HeaderScope::Connection) {
            return self.reject(Violation::InvalidValue("headers", error.reason()));
        }
        Ok(fields)
    }

    fn credential_generation(&mut self, reference: &str) -> Result<String, BatchError> {
        if self.allocate_credential_ref(reference)? == reference {
            return Ok(reference.to_owned());
        }
        let stem = reference
            .rsplit_once('_')
            .filter(|(stem, number)| {
                !stem.is_empty()
                    && number
                        .parse::<u64>()
                        .is_ok_and(|value| value >= 2 && value.to_string() == *number)
            })
            .map_or(reference, |(stem, _)| stem);
        let mut end = stem.len().min(100);
        while !stem.is_char_boundary(end) {
            end = end.saturating_sub(1);
        }
        self.allocate_credential_ref(&stem[..end])
    }

    fn allocate_credential_ref(&mut self, base: &str) -> Result<String, BatchError> {
        let (base_taken, suffixes) = self.step(|tx| {
            let mut base_taken = false;
            let mut suffixes = std::collections::BTreeSet::new();
            let mut record = |reference: &str| {
                if reference == base {
                    base_taken = true;
                } else if let Some(suffix) = reference
                    .strip_prefix(base)
                    .and_then(|value| value.strip_prefix('_'))
                    && let Ok(number) = suffix.parse::<u64>()
                    && number.to_string() == suffix
                {
                    suffixes.insert(number);
                }
            };
            let table = tx.open_table(CONFIGS)?;
            for entry in table.iter()? {
                let (key, bytes) = entry?;
                let Some(row) = ConfigRow::scan_at(key.value(), bytes.value())? else {
                    continue;
                };
                if let ConfigBody::Connection(connection) = &row.body {
                    connection.credential_refs().for_each(&mut record);
                }
            }
            for entry in tx.open_table(CREDENTIALS)?.iter()? {
                let (key, _) = entry?;
                record(key.value());
            }
            Ok((base_taken, suffixes))
        })?;
        if !base_taken {
            return Ok(base.to_owned());
        }
        let mut suffix = 2u64;
        while suffixes.contains(&suffix) {
            suffix = suffix.checked_add(1).ok_or({
                BatchError(Failure::Rejected(Violation::InvalidValue(
                    "credential_ref",
                    "could not allocate a unique reference",
                )))
            })?;
        }
        Ok(format!("{base}_{suffix}"))
    }

    fn write_credential(
        &mut self,
        reference: &str,
        credential: &crate::db::mutations::CredentialPayload,
    ) -> Result<(), BatchError> {
        if !valid_credential_ref(reference) {
            return self.reject(Violation::InvalidValue(
                "credential_ref",
                "must be 1..=128 non-control bytes",
            ));
        }
        self.step(|tx| {
            let encrypted = credential.key.encrypt(reference, &credential.secret)?;
            tx.open_table(CREDENTIALS)?
                .insert(reference, encrypted.as_slice())?;
            Ok(())
        })
    }

    fn purge_unused_credentials(&mut self) -> Result<(), BatchError> {
        self.step(|tx| {
            let mut connection_refs = std::collections::BTreeMap::new();
            let mut live_connections = std::collections::BTreeSet::new();
            for entry in tx.open_table(CONFIGS)?.iter()? {
                let (key, bytes) = entry?;
                let Some(row) = ConfigRow::scan_at(key.value(), bytes.value())? else {
                    continue;
                };
                match &row.body {
                    ConfigBody::Connection(connection) => {
                        connection_refs.insert(
                            row.id,
                            connection
                                .credential_refs()
                                .map(str::to_owned)
                                .collect::<Vec<_>>(),
                        );
                    }
                    ConfigBody::Model(model) if row.archived_at.is_none() => {
                        live_connections.insert(model.connection_id);
                    }
                    ConfigBody::DecisionModel(model) if row.archived_at.is_none() => {
                        live_connections.insert(model.connection_id);
                    }
                    ConfigBody::Model(_)
                    | ConfigBody::DecisionModel(_)
                    | ConfigBody::Agent(_)
                    | ConfigBody::DecisionAgent(_)
                    | ConfigBody::Pool(_) => {}
                }
            }
            let used_refs: std::collections::BTreeSet<&str> = live_connections
                .iter()
                .filter_map(|id| connection_refs.get(id))
                .flatten()
                .map(String::as_str)
                .collect();
            let mut credentials = tx.open_table(CREDENTIALS)?;
            let unused: Vec<String> = credentials
                .iter()?
                .map(|entry| entry.map(|(key, _)| key.value().to_owned()))
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .filter(|reference| !used_refs.contains(reference.as_str()))
                .collect();
            for reference in unused {
                credentials.remove(reference.as_str())?;
            }
            Ok(())
        })
    }

    fn ensure_llm_executor(&mut self, protocol: LlmTransport) -> Result<(), BatchError> {
        self.require_native_capability(ExecutionOperation::LlmRequest, Some(protocol))
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
                if let Some(policy) = &agent.compaction_policy {
                    self.check_compaction_policy(policy)?;
                }
                if agent.capabilities.is_empty() {
                    return self.reject(Violation::InvalidValue(
                        "capabilities",
                        "select Agent capabilities",
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
            }
            ConfigBody::DecisionAgent(da) => {
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
        self.insert(CONFIGS, row.id.get(), row.encode().as_slice())
    }

    pub(crate) fn set_config_enabled(
        &mut self,
        id: ConfigId,
        kind: ConfigKind,
        enabled: bool,
    ) -> Result<(), BatchError> {
        let mut row = self.require_config_kind(id, kind)?;
        if row.archived_at.is_some() {
            return self.reject(Violation::NotFound(RecordRef::Profile(id.into())));
        }
        let value = match kind {
            ConfigKind::Connection => &mut row.discovery_visible,
            ConfigKind::Model => &mut row.model_enabled,
            ConfigKind::Agent
            | ConfigKind::Pool
            | ConfigKind::DecisionModel
            | ConfigKind::DecisionAgent => {
                return self.reject(Violation::InvalidValue(
                    "kind",
                    "only connections and models have availability",
                ));
            }
        };
        if *value == enabled {
            return Ok(());
        }
        *value = enabled;
        row.updated_at = self.now;
        row.edit_seq += 1;
        self.catalog_changed = true;
        self.put_config(&row)
    }

    pub(crate) fn create_config(
        &mut self,
        name: String,
        body: ConfigBody,
    ) -> Result<ConfigId, BatchError> {
        if matches!(body, ConfigBody::Pool(_)) {
            return self.reject(Violation::InvalidValue(
                "pool",
                "legacy combined Pool/Executor configurations are unavailable",
            ));
        }
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
            model_enabled: true,
            discovery_visible: true,
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

    pub(crate) fn update_config(
        &mut self,
        id: ConfigId,
        name: String,
        body: ConfigBody,
    ) -> Result<(), BatchError> {
        self.update_config_from(id, name, body, None)
    }

    pub(crate) fn update_config_from(
        &mut self,
        id: ConfigId,
        name: String,
        body: ConfigBody,
        expected_edit_seq: Option<u32>,
    ) -> Result<(), BatchError> {
        let existing = self.require_config(id)?;
        if existing.archived_at.is_some() {
            return self.reject(Violation::NotFound(RecordRef::Profile(id.into())));
        }
        if expected_edit_seq.is_some_and(|expected| expected != existing.edit_seq) {
            return self.reject(Violation::ProfileChanged);
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
        save: crate::db::mutations::ImportModelsSave,
    ) -> Result<(Vec<ConfigId>, usize), BatchError> {
        use crate::db::entities::{LlmParameters, ModelConfig};
        let crate::db::mutations::ImportModelsSave {
            headers,
            mut connection,
            credential_ref,
            secret: _,
            credential,
            models,
            capabilities,
        } = save;
        if models.is_empty() || models.len() > 200 || capabilities.is_empty() {
            return self.reject(Violation::InvalidValue(
                "models",
                "select 1 to 200 models with capabilities",
            ));
        }
        let prior_ref = match &connection.body {
            ConfigBody::Connection(wanted) => wanted.credential_ref.clone(),
            _ => None,
        };
        let resolved_ref =
            self.resolve_and_write_credential(credential_ref, prior_ref, credential.as_ref())?;
        let ConfigBody::Connection(wanted) = &mut connection.body else {
            return self.reject(Violation::InvalidValue(
                "connection",
                "must be a Connection",
            ));
        };
        let own_headers = wanted.headers.clone();
        wanted.headers = self.apply_header_change(&own_headers, headers)?;
        wanted.credential_ref = resolved_ref;
        let wanted = wanted.clone();
        let wanted_protocol = wanted.protocol;
        let connection_id = self.find_or_create_connection(connection.name, wanted, true)?;
        self.set_config_enabled(connection_id, ConfigKind::Connection, true)?;
        self.ensure_llm_executor(wanted_protocol)?;
        let mut known = self.step(|tx| {
            let mut known = std::collections::BTreeMap::new();
            for entry in tx.open_table(CONFIGS)?.iter()? {
                let (key, value) = entry?;
                let Some(row) = ConfigRow::scan_at(key.value(), value.value())? else {
                    continue;
                };
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
        self.purge_unused_credentials()?;
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
                let Some(row) = ConfigRow::scan_at(key.value(), value.value())? else {
                    continue;
                };
                if row.archived_at.is_none() && matches!(&row.body, ConfigBody::Connection(body) if body.protocol == connection.protocol && body.credential_ref == connection.credential_ref && {
                    let default = if body.protocol == LlmTransport::AnthropicMessages { "https://api.anthropic.com/v1" } else { "https://api.openai.com/v1" };
                    body.endpoint.as_deref().unwrap_or(default).trim_end_matches('/') == connection.endpoint.as_deref().unwrap_or(default).trim_end_matches('/')
                }) {
                    connections.insert(row.id);
                }
            }
            for entry in tx.open_table(CONFIGS)?.iter()? {
                let (key, value) = entry?;
                let Some(row) = ConfigRow::scan_at(key.value(), value.value())? else {
                    continue;
                };
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
        let row = self.require_config(id)?;
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
        self.archive_models(vec![row])
    }

    fn archive_models(&mut self, rows: Vec<ConfigRow>) -> Result<(), BatchError> {
        if rows.is_empty() {
            return self.purge_unused_credentials();
        }
        let ids: BTreeSet<_> = rows.iter().map(|row| row.id).collect();
        let agents = self.step(|tx| {
            let mut agents = Vec::new();
            for entry in tx.open_table(CONFIGS)?.iter()? {
                let (key, value) = entry?;
                let Some(row) = ConfigRow::scan_at(key.value(), value.value())? else {
                    continue;
                };
                if row.archived_at.is_none()
                    && matches!(
                        &row.body,
                        ConfigBody::Agent(_) | ConfigBody::DecisionAgent(_)
                    )
                {
                    agents.push(row);
                }
            }
            Ok(agents)
        })?;
        let mut emptied: BTreeSet<_> = rows
            .iter()
            .filter(|row| row.kind == ConfigKind::DecisionAgent)
            .map(|row| row.id)
            .collect();
        for row in &agents {
            if let ConfigBody::DecisionAgent(agent) = &row.body
                && agent.models.iter().any(|model| ids.contains(model))
                && agent.models.iter().all(|model| ids.contains(model))
            {
                emptied.insert(row.id);
            }
        }
        let clear_policy = |policy: &mut crate::db::entities::CompactionPolicy| {
            if policy
                .summarization_model
                .is_some_and(|model| ids.contains(&model))
            {
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
                    .is_some_and(|pin| ids.contains(&pin.model.id))
            {
                prediction.mode = crate::db::entities::PredictionMode::Shadow;
                prediction.acceptance = None;
            }
        };
        for mut agent_row in agents {
            let before = agent_row.body.clone();
            match &mut agent_row.body {
                ConfigBody::Agent(agent) => {
                    agent.models.retain(|model| !ids.contains(model));
                    if let Some(policy) = &mut agent.compaction_policy {
                        clear_policy(policy);
                    }
                }
                ConfigBody::DecisionAgent(agent) => {
                    agent.models.retain(|model| !ids.contains(model));
                    if agent
                        .default_model
                        .is_some_and(|model| ids.contains(&model))
                    {
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
                    .is_some_and(|profile| ids.contains(&profile) || emptied.contains(&profile))
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
        for mut row in rows {
            row.archived_at = Some(self.now);
            row.updated_at = self.now;
            row.edit_seq += 1;
            self.put_config(&row)?;
        }
        self.purge_unused_credentials()?;
        Ok(())
    }

    pub(crate) fn remove_provider(&mut self, id: ConfigId) -> Result<(), BatchError> {
        self.set_config_enabled(id, ConfigKind::Connection, false)?;
        let models = self.step(|tx| {
            let mut models = Vec::new();
            for entry in tx.open_table(CONFIGS)?.iter()? {
                let (key, value) = entry?;
                let Some(row) = ConfigRow::scan_at(key.value(), value.value())? else {
                    continue;
                };
                if row.archived_at.is_none()
                    && matches!(&row.body, ConfigBody::Model(model) if model.connection_id == id)
                {
                    models.push(row);
                }
            }
            Ok(models)
        })?;
        self.archive_models(models)
    }

    pub(crate) fn job(&mut self, id: JobId) -> Result<Option<JobSpec>, BatchError> {
        self.get(JOBS, id.get(), |bytes| JobSpec::decode_at(id.get(), bytes))
    }

    pub(crate) fn turn_conversation(&mut self, id: JobId) -> Result<Option<JobId>, BatchError> {
        self.get(JOBS, id.get(), |bytes| {
            if matches!(bytes, [TAG_JOB, version, ..] if u64::from(*version) == FORMAT_VERSION) {
                return transactions::decode_mutable_job(id.get(), bytes)
                    .map(|job| Some(job.parent_id));
            }
            let row = JobSpec::decode_at(id.get(), bytes)?;
            Ok(row.kind.is_persistent().then_some(row.parent_id).flatten())
        })?
        .map(Ok)
        .unwrap_or_else(|| self.reject(Violation::InvalidValue("job", "the Job does not exist")))
    }

    pub(crate) fn require_job(&mut self, id: JobId) -> Result<JobSpec, BatchError> {
        match self.job(id)? {
            Some(row) => Ok(row),
            None => self.reject(Violation::NotFound(RecordRef::Job(id))),
        }
    }

    pub(crate) fn job_state(&mut self, id: JobId) -> Result<Option<JobState>, BatchError> {
        self.get(JOB_STATES, id.get(), |bytes| {
            JobState::decode_at(id.get(), bytes)
        })
    }

    pub(crate) fn require_job_state(&mut self, id: JobId) -> Result<JobState, BatchError> {
        match self.job_state(id)? {
            Some(row) => Ok(row),
            None => self.reject(Violation::NotFound(RecordRef::Job(id))),
        }
    }

    pub(crate) fn put_job_state(&mut self, job: JobId, state: &JobState) -> Result<(), BatchError> {
        self.catalog_changed = true;
        self.insert(JOB_STATES, job.get(), state.encode().as_slice())
    }

    pub(crate) fn append_development_output(
        &mut self,
        proof: &crate::db::handle::DevelopmentWriteProof,
        expected_seq: u32,
        output: crate::db::entities::JobOutput,
    ) -> Result<crate::db::entities::AppendedOutput, BatchError> {
        use crate::db::entities::IntermediateOutputKind;
        self.require_development_write(proof)?;
        if !matches!(
            output.kind,
            IntermediateOutputKind::Progress
                | IntermediateOutputKind::Evaluation
                | IntermediateOutputKind::Reasoning
        ) {
            return self.reject(Violation::InvalidValue("output", "unsupported output kind"));
        }
        let job = proof.token().job;
        let last = self.step(|tx| {
            Ok(tx
                .open_table(JOB_OUTPUTS)?
                .range((job.get(), 0)..=(job.get(), u32::MAX))?
                .next_back()
                .transpose()?
                .map_or(0, |(key, _)| key.value().1))
        })?;
        if last.checked_add(1) != Some(expected_seq) {
            return self.reject(Violation::InvalidValue(
                "output",
                "sequence is not the next output",
            ));
        }
        let (seq, id) = self.write_output(
            job,
            output.kind.into(),
            output.summary,
            output.code,
            output.goal_reached,
            output.value,
        )?;
        Ok(crate::db::entities::AppendedOutput {
            output: id,
            seq: i64::from(seq),
        })
    }

    pub(crate) fn record_development_sent_request(
        &mut self,
        proof: &crate::db::handle::DevelopmentWriteProof,
        sent: crate::db::entities::SentRequest,
        attachments: &[crate::db::entities::AttachmentRecord],
    ) -> Result<(), BatchError> {
        if self
            .pool_allocation(proof.token().job)?
            .is_some_and(|allocation| {
                allocation.token == proof.token()
                    && allocation.phase == crate::db::entities::AllocationPhase::StopRequested
            })
        {
            return self.reject(Violation::InvalidValue(
                "execution",
                crate::db::entities::STOPPED_BEFORE_SEND,
            ));
        }
        self.scheduled_external_authorization(proof.token().job)?;
        self.require_development_write_any(proof)?;
        if sent.job != proof.token().job {
            return self.reject(Violation::InvalidValue(
                "request",
                "wrong Job or request identity",
            ));
        }
        for expected in attachments {
            if !self.require_attachment(expected.id)?.is_available() {
                return self.reject(Violation::InvalidValue(
                    "attachments",
                    "prepared Attachment changed or was cleaned up",
                ));
            }
        }
        if !self.development_request_slot_is_next(sent.job, sent.request)? {
            return self.reject(Violation::InvalidValue(
                "request",
                "the request is not the next one, or its evidence already exists",
            ));
        }
        for body in &sent.bodies {
            self.put_message(body)?;
        }
        self.require_messages(
            sent.bodies
                .iter()
                .map(crate::db::entities::MessageBody::digest)
                .chain(sent.messages.iter().copied())
                .chain(sent.required_messages.iter().copied()),
        )?;
        let event = RequestEventRecord {
            id: crate::db::entities::RequestEventId::new(self.next_id()?),
            job_id: sent.job,
            request: sent.request,
            seq: 1,
            observed_at: sent.observed_at,
            recorded_at: self.now,
            payload: RequestPayload::Sent {
                messages: sent.messages,
                tools: sent.tools,
                parameters: sent.parameters,
                dropped_turns: sent.dropped_turns,
            },
        };
        self.insert(
            REQUEST_EVENTS,
            (event.job_id.get(), event.request.get(), event.seq),
            encode_request_event(&event).as_slice(),
        )?;
        Ok(())
    }

    fn development_usage_identity(
        &mut self,
        job: JobId,
    ) -> Result<reader::UsageIdentity, BatchError> {
        let Some(row) = self.mutable_job(job)? else {
            return self.reject(Violation::NotFound(RecordRef::Job(job)));
        };
        let identity = match row.frozen {
            Some(crate::db::entities::MutableFrozenConfiguration::Generative(frozen)) => {
                reader::UsageIdentity {
                    provider: frozen.llm.provider.id.get(),
                    model: frozen.llm.config.model,
                }
            }
            Some(crate::db::entities::MutableFrozenConfiguration::Decision(frozen)) => {
                reader::UsageIdentity {
                    provider: frozen.connection_ref.id.get(),
                    model: frozen.model.model,
                }
            }
            None => {
                return self.reject(Violation::InvalidValue(
                    "execution",
                    "frozen configuration required",
                ));
            }
        };
        Ok(identity)
    }

    pub(in crate::db) fn record_development_provider_answer(
        &mut self,
        proof: &crate::db::handle::DevelopmentWriteProof,
        answer: crate::db::entities::ProviderAnswer,
        usages: Vec<(u32, crate::db::entities::Metrics)>,
        checkpoint: crate::db::handle::DevelopmentUsageCheckpoint,
    ) -> Result<
        (
            MetricsId,
            AvailabilityStatId,
            Vec<MetricsId>,
            crate::db::handle::DevelopmentUsageCheckpoint,
        ),
        BatchError,
    > {
        self.require_development_write_any(proof)?;
        let job = proof.token().job;
        let stat = answer.stat;
        if stat.job != job {
            return self.reject(Violation::InvalidValue(
                "request",
                "wrong Job or request identity",
            ));
        }
        if answer.measurements.is_empty() || stat.observation.is_empty() {
            return self.reject(Violation::InvalidValue(
                "answer",
                "missing measurements or availability",
            ));
        }
        let usage = &answer.measurements.usage;
        if let (Some(input), Some(output)) = (usage.input_tokens, usage.output_tokens)
            && input.checked_add(output).is_none()
        {
            return self.reject(Violation::ValueOutOfRange("usage"));
        }
        let only_sent = self.development_only_sent(job, stat.request)?;
        if !only_sent {
            return self.reject(Violation::InvalidValue(
                "response",
                "request missing or answer already recorded",
            ));
        }
        let mut evidence = checkpoint.evidence();
        let old = evidence.sample();
        let mut prepared_usages = Vec::with_capacity(usages.len());
        let mut next_seq = 5u32;
        for (expected_seq, metrics) in usages {
            if metrics.job != job
                || metrics.request != stat.request
                || expected_seq != next_seq
                || expected_seq < 5
                || expected_seq.is_multiple_of(2)
            {
                return self.reject(Violation::InvalidValue(
                    "usage",
                    "wrong Job, request or event sequence",
                ));
            }
            let Some(token) = metrics.token else {
                return self.reject(Violation::InvalidValue(
                    "usage",
                    "Token annotation required",
                ));
            };
            let Some(token_seq) = expected_seq.checked_add(1) else {
                return self.reject(Violation::ValueOutOfRange("seq"));
            };
            let Some(next_expected) = expected_seq.checked_add(2) else {
                return self.reject(Violation::ValueOutOfRange("seq"));
            };
            next_seq = next_expected;
            let first_id = self.next_id()?;
            let second_id = self.next_id()?;
            let measurement = crate::db::entities::MetricsRecord {
                id: MetricsId::new(first_id),
                job,
                request: stat.request,
                target: None,
                decision_target: None,
                observed_at: metrics.observed_at,
                measurements: metrics.measurements.clone(),
                token: Some(token),
                recorded_at: self.now,
            };
            let Some(observation) =
                crate::db::entities::TokenObservation::from_record(&measurement)
            else {
                return self.reject(Violation::InvalidValue(
                    "usage",
                    "Token annotation required",
                ));
            };
            if evidence.add(observation).is_err() {
                return self.reject(Violation::InvalidValue(
                    "usage",
                    "inconsistent Token evidence",
                ));
            }
            prepared_usages.push((
                first_id,
                second_id,
                expected_seq,
                token_seq,
                metrics.observed_at,
                metrics.measurements,
                token,
            ));
        }
        let new = evidence.sample();
        self.step(|_| indexes::validate_native_sample(new.as_ref()))?;
        let identity = self.development_usage_identity(job)?;
        self.put_message(&answer.response)?;
        let observed_at = stat.observed_at;
        let payloads = [
            RequestPayload::Measurement {
                measurements: answer.measurements,
            },
            RequestPayload::Received {
                digest: answer.response.digest(),
            },
            RequestPayload::Availability {
                status_code: stat.observation.status_code,
                finish_reason: stat.observation.finish_reason,
                raw_finish_reason: stat.observation.raw_finish_reason,
                failure: stat.observation.failure,
            },
        ];
        let mut ids = [0u128; 3];
        for (offset, (seq, payload)) in [2u32, 3, 4].into_iter().zip(payloads).enumerate() {
            ids[offset] = self.insert_development_request_event(
                &identity,
                job,
                stat.request,
                seq,
                observed_at,
                payload,
            )?;
        }
        let mut usage_ids = Vec::with_capacity(prepared_usages.len());
        let mut last_token_seq = None;
        for (first_id, second_id, expected_seq, token_seq, observed_at, measurements, token) in
            prepared_usages
        {
            self.insert_development_request_event_with_id(
                &identity,
                job,
                stat.request,
                first_id,
                expected_seq,
                observed_at,
                RequestPayload::Measurement { measurements },
            )?;
            self.insert_development_request_event_with_id(
                &identity,
                job,
                stat.request,
                second_id,
                token_seq,
                observed_at,
                RequestPayload::Token {
                    eligibility: token.eligibility,
                    finality: token.finality,
                },
            )?;
            usage_ids.push(MetricsId::new(first_id));
            last_token_seq = Some(token_seq);
        }
        let next_checkpoint = match last_token_seq {
            Some(token_seq) => {
                self.replace_development_usage_total(job, old, new)?;
                crate::db::handle::DevelopmentUsageCheckpoint::advanced(token_seq, evidence)
            }
            None => checkpoint,
        };
        Ok((
            MetricsId::new(ids[0]),
            AvailabilityStatId::new(ids[2]),
            usage_ids,
            next_checkpoint,
        ))
    }

    fn insert_development_request_event(
        &mut self,
        identity: &crate::db::redb::reader::UsageIdentity,
        job: JobId,
        request: RequestNumber,
        seq: u32,
        observed_at: Timestamp,
        payload: RequestPayload,
    ) -> Result<u128, BatchError> {
        let id = self.next_id()?;
        self.insert_development_request_event_with_id(
            identity,
            job,
            request,
            id,
            seq,
            observed_at,
            payload,
        )?;
        Ok(id)
    }

    #[allow(clippy::too_many_arguments)]
    fn insert_development_request_event_with_id(
        &mut self,
        identity: &crate::db::redb::reader::UsageIdentity,
        job: JobId,
        request: RequestNumber,
        id: u128,
        seq: u32,
        observed_at: Timestamp,
        payload: RequestPayload,
    ) -> Result<(), BatchError> {
        let event = RequestEventRecord {
            id: crate::db::entities::RequestEventId::new(id),
            job_id: job,
            request,
            seq,
            observed_at,
            recorded_at: self.now,
            payload,
        };
        let timeline = indexes::request_timeline_key(Some(identity), &event);
        let model = indexes::usage_model_key(Some(identity), &event);
        self.step(|tx| {
            tx.open_table(REQUEST_EVENTS)?.insert(
                (job.get(), request.get(), seq),
                encode_request_event(&event).as_slice(),
            )?;
            if let Some(kind) =
                observations::observation_kind(&tx.open_table(REQUEST_EVENTS)?, &event)?
            {
                tx.open_table(JOB_OBSERVATIONS)?
                    .insert((job.get(), kind, id), (request.get(), seq))?;
            }
            if let Some(key) = timeline {
                tx.open_table(REQUEST_TIMELINE)?.insert(key, ())?;
            }
            if let Some(key) = model {
                indexes::record_usage_model(tx, key, job)?;
            }
            Ok(())
        })
    }

    fn development_only_sent(
        &mut self,
        job: JobId,
        request: RequestNumber,
    ) -> Result<bool, BatchError> {
        self.step(|tx| {
            let events = tx.open_table(REQUEST_EVENTS)?;
            let mut rows =
                events.range((job.get(), request.get(), 0)..=(job.get(), u32::MAX, u32::MAX))?;
            let valid = if let Some((key, bytes)) = rows.next().transpose()? {
                let (_, number, seq) = key.value();
                number == request.get()
                    && seq == 1
                    && matches!(
                        decode_request_event(job, request, seq, bytes.value())?.payload,
                        RequestPayload::Sent { .. }
                    )
            } else {
                false
            };
            Ok(valid && rows.next().transpose()?.is_none())
        })
    }

    fn development_request_slot_is_next(
        &mut self,
        job: JobId,
        request: RequestNumber,
    ) -> Result<bool, BatchError> {
        self.step(|tx| {
            let events = tx.open_table(REQUEST_EVENTS)?;
            if events
                .range((job.get(), request.get(), 0)..=(job.get(), u32::MAX, u32::MAX))?
                .next()
                .transpose()?
                .is_some()
            {
                return Ok(false);
            }
            let Some(previous) = request.get().checked_sub(1).and_then(RequestNumber::new) else {
                return Ok(true);
            };
            let (mut sent, mut answered) = (false, false);
            for entry in events
                .range((job.get(), previous.get(), 0)..=(job.get(), previous.get(), u32::MAX))?
            {
                let (key, bytes) = entry?;
                let (_, _, seq) = key.value();
                match decode_request_event(job, previous, seq, bytes.value())?.payload {
                    RequestPayload::Sent { .. } => sent = true,
                    RequestPayload::Availability { .. } => answered = true,
                    _ => {}
                }
            }
            Ok(sent && answered)
        })
    }

    pub(crate) fn record_development_provider_failure(
        &mut self,
        proof: &crate::db::handle::DevelopmentWriteProof,
        stat: crate::db::entities::AvailabilityStat,
    ) -> Result<AvailabilityStatId, BatchError> {
        self.require_development_write_any(proof)?;
        let job = proof.token().job;
        if stat.job != job {
            return self.reject(Violation::InvalidValue(
                "request",
                "wrong Job or request identity",
            ));
        }
        let observation = stat.observation;
        if observation.status_code.is_none() || observation.failure.is_none() {
            return self.reject(Violation::InvalidValue(
                "failure",
                "a received Provider failure needs its status and failure facts",
            ));
        }
        if !self.development_only_sent(job, stat.request)? {
            return self.reject(Violation::InvalidValue(
                "failure",
                "request missing or response evidence already recorded",
            ));
        }
        let identity = self.development_usage_identity(job)?;
        let id = self.insert_development_request_event(
            &identity,
            job,
            stat.request,
            2,
            stat.observed_at,
            RequestPayload::Availability {
                status_code: observation.status_code,
                finish_reason: observation.finish_reason,
                raw_finish_reason: observation.raw_finish_reason,
                failure: observation.failure,
            },
        )?;
        Ok(AvailabilityStatId::new(id))
    }

    fn replace_development_usage_total(
        &mut self,
        job: JobId,
        old: Option<crate::db::entities::TokenRequestSample>,
        new: Option<crate::db::entities::TokenRequestSample>,
    ) -> Result<(), BatchError> {
        if old == new {
            return Ok(());
        }
        let Some(row) = self.mutable_job(job)? else {
            return self.reject(Violation::NotFound(RecordRef::Job(job)));
        };
        let Some(crate::db::entities::MutableFrozenConfiguration::Generative(frozen)) = row.frozen
        else {
            return self.reject(Violation::InvalidValue(
                "usage",
                "frozen generative configuration required",
            ));
        };
        let parent = self.require_mutable_conversation(row.parent_id)?;
        let agent = usage_agent(&parent, None);
        let provider = frozen.llm.provider.id.get();
        let hash = *ContentDigest::of(frozen.llm.config.model.as_bytes()).as_bytes();
        let key = |sample: &crate::db::entities::TokenRequestSample| {
            (
                sample
                    .observed_at
                    .as_unix_micros()
                    .div_euclid(crate::db::entities::USAGE_SUMMARY_HOUR_MICROS),
                agent,
                provider,
                hash,
            )
        };
        let numbers = |sample: &crate::db::entities::TokenRequestSample| {
            [
                1u64,
                u64::from(sample.usage == crate::db::entities::TokenUsageState::Complete),
                u64::from(sample.input.value().unwrap_or(0)),
                u64::from(sample.output.value().unwrap_or(0)),
                u64::from(sample.cache_read.value().unwrap_or(0)),
                u64::from(sample.cache_write.value().unwrap_or(0)),
                u64::from(sample.reasoning.value().unwrap_or(0)),
            ]
        };
        let ok = self.step(|tx| {
            let mut totals = tx.open_table(USAGE_TOTALS)?;
            for (remove, sample) in [(true, old.as_ref()), (false, new.as_ref())] {
                let Some(sample) = sample else {
                    continue;
                };
                let k = key(sample);
                let mut values = match totals.get(k)?.map(|v| v.value()) {
                    Some(v) => [v.0, v.1, v.2, v.3, v.4, v.5, v.6],
                    None if remove => {
                        return Err(StorageError::corrupted("native usage aggregate missing"));
                    }
                    None => [0; 7],
                };
                for (value, delta) in values.iter_mut().zip(numbers(sample)) {
                    let Some(next) = (if remove {
                        value.checked_sub(delta)
                    } else {
                        value.checked_add(delta)
                    }) else {
                        return Ok(false);
                    };
                    *value = next;
                }
                if values[0] == 0 {
                    if values.iter().any(|v| *v != 0) {
                        return Ok(false);
                    }
                    totals.remove(k)?;
                } else {
                    totals.insert(
                        k,
                        (
                            values[0], values[1], values[2], values[3], values[4], values[5],
                            values[6],
                        ),
                    )?;
                }
            }
            Ok(true)
        })?;
        if !ok {
            return self.reject(Violation::ValueOutOfRange("usage aggregate"));
        }
        Ok(())
    }

    pub(super) fn write_output(
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
            if record.kind == OutputKind::Result {
                let conversation = tx
                    .open_table(JOBS)?
                    .get(job.get())?
                    .map(|bytes| {
                        if matches!(bytes.value(), [TAG_JOB, version, ..] if u64::from(*version) == FORMAT_VERSION) {
                            return transactions::decode_mutable_job(job.get(), bytes.value())
                                .map(|job| Some(job.parent_id));
                        }
                        JobSpec::decode_at(job.get(), bytes.value()).map(|row| row.parent_id)
                    })
                    .transpose()?
                    .flatten();
                if let Some(conversation) = conversation {
                    let completed = tx
                        .open_table(JOB_STATES)?
                        .get(job.get())?
                        .map(|bytes| JobState::decode_at(job.get(), bytes.value()))
                        .transpose()?
                        .and_then(|state| state.outcome)
                        .is_some_and(|outcome| outcome.kind == OutcomeKind::Completed);
                    indexes::append_output_search_index(tx, conversation, job, &record, completed)?;
                }
            }
            Ok(())
        })?;
        Ok((seq, id))
    }

    pub(crate) fn submission(
        &mut self,
        key: SubmissionKey,
    ) -> Result<Option<SubmissionRecord>, BatchError> {
        self.get(SUBMISSIONS, key.as_bytes().as_slice(), |bytes| {
            decode_submission(key.as_bytes(), bytes)
        })
    }

    pub(crate) fn require_attachment(
        &mut self,
        id: AttachmentId,
    ) -> Result<AttachmentRecord, BatchError> {
        let record = self.get(ATTACHMENTS, id.get(), |bytes| {
            decode_attachment(id.get(), bytes)
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
        self.insert(PROJECTS, record.id.get(), encode_project(record).as_slice())
    }

    pub(crate) fn require_project_record(
        &mut self,
        id: ProjectId,
    ) -> Result<ProjectRecord, BatchError> {
        let record = self.get(PROJECTS, id.get(), |bytes| decode_project(id.get(), bytes))?;
        match record {
            Some(record) => Ok(record),
            None => self.reject(Violation::NotFound(RecordRef::Project(id))),
        }
    }

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

    pub(crate) fn require_mutable_conversation(
        &mut self,
        id: JobId,
    ) -> Result<JobSpec, BatchError> {
        let is_conversation = self.step(|tx| {
            Ok(tx
                .open_table(JOBS)?
                .get(id.get())?
                .is_some_and(|row| row.value().first() == Some(&JobSpec::MUTABLE_CONVERSATION_TAG)))
        })?;
        if !is_conversation {
            return self.reject(Violation::NotFound(RecordRef::Job(id)));
        }
        self.require_job(id)
    }

    pub(crate) fn create_mutable_conversation(
        &mut self,
        project: Option<ProjectId>,
    ) -> Result<JobId, BatchError> {
        if let Some(project) = project {
            self.require_project(project)?;
        }
        let id = JobId::new(self.next_id()?);
        let row = JobSpec {
            id,
            created_at: self.now,
            parent_id: None,
            allocation: None,
            kind: JobKind::Group {
                agent: None,
                project,
                origin: Origin::User,
                keep_open: true,
            },
        };
        self.insert(JOBS, id.get(), row.encode_mutable_conversation().as_slice())?;
        self.put_job_state(id, &JobState::new(id))?;
        self.put_job_control(id, &JobControl::default())?;
        self.insert(CONVERSATIONS, id.get(), ())?;
        self.insert(UNSETTLED, id.get(), (0, 0))?;
        self.insert(CHILDREN, (0, id.get()), ())?;
        Ok(id)
    }

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
        self.insert(JOB_CONTROLS, root.get(), row.encode().as_slice())?;
        if (row.closing.is_some() || row.hidden_at.is_some())
            && let Some(mut rec) = self.get(RECURRENCES, root.get(), Recurrence::decode)?
            && !matches!(rec.phase, RecurrencePhase::Halted { .. })
        {
            if let RecurrencePhase::WaitingDue { due } = rec.phase {
                self.step(|tx| {
                    tx.open_table(RECURRENCE_DUE)?.remove((due, root.get()))?;
                    Ok(())
                })?;
            }
            rec.phase = RecurrencePhase::Halted {
                reason: RecurrenceHalt::Closed,
            };
            self.insert(RECURRENCES, root.get(), rec.encode().as_slice())?;
        }
        Ok(())
    }

    pub(crate) fn tool_evidence(
        &mut self,
        call: ToolCallRef,
    ) -> Result<Option<ToolEvidenceRecord>, BatchError> {
        self.get(TOOL_CALLS, tool_call_key(call), |bytes| {
            decode_tool_evidence(call, bytes)
        })
    }

    pub(crate) fn put_tool_evidence(
        &mut self,
        record: &ToolEvidenceRecord,
    ) -> Result<(), BatchError> {
        self.catalog_changed = true;
        self.insert(
            TOOL_CALLS,
            tool_call_key(record.call),
            encode_tool_evidence(record).as_slice(),
        )
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
        let timeline = indexes::request_timeline_key(identity.as_ref(), &event);
        let usage_model = indexes::usage_model_key(identity.as_ref(), &event);
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
            if let Some(key) = timeline {
                tx.open_table(REQUEST_TIMELINE)?.insert(key, ())?;
            }
            if let Some(key) = usage_model {
                indexes::record_usage_model(tx, key, job)?;
            }
            Ok(())
        })?;
        Ok(event)
    }
}

impl Batch {
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
}

impl Batch {
    pub(crate) fn wake_scheduler(&mut self) {
        self.touched_ready_or_delayed = true;
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
