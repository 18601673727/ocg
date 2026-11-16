use crate::db::entities::{
    AgentModelConfig, AttachmentRecord, CredentialSecret, CursorPage, ExecutionOperation,
    ProfilePageRecord, ProfilePageRecords, ProjectRecord, SubmissionKey, SubmissionRecord,
};
use crate::db::entities::{AttachmentId, ExecutorId, ProjectId};
use crate::db::entities::{ConfigBody, ConfigId, ConfigKind, ConfigRecord};
use crate::db::redb::StorageError;
use crate::db::redb::TxRead;
use crate::db::redb::allocations;
use crate::db::redb::codec::{Dec, TAG_GLOBAL_COMPACTION, TAG_GLOBAL_HEADERS};
use crate::db::redb::reads::read;
use crate::db::redb::rows::{
    ConfigRow, CredentialKey, decode_attachment, decode_credential, decode_project,
    decode_submission,
};
use crate::db::redb::schema::MAX_PAGE;
use crate::db::redb::schema::{
    ATTACHMENTS, CONFIGS, CREDENTIALS, EXECUTORS, META, POOLS, PROJECTS, SUBMISSIONS,
};
use ::redb::{Database, ReadTransaction, ReadableTable};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) fn read_submission(
    database: &Database,
    key: SubmissionKey,
) -> Result<Option<SubmissionRecord>, StorageError> {
    let rx = read(database)?;
    rx.open_table(SUBMISSIONS)?
        .get(key.as_bytes().as_slice())?
        .map(|bytes| decode_submission(key.as_bytes(), bytes.value()))
        .transpose()
}

pub(crate) fn read_attachment(
    database: &Database,
    id: AttachmentId,
) -> Result<Option<AttachmentRecord>, StorageError> {
    let rx = read(database)?;
    rx.open_table(ATTACHMENTS)?
        .get(id.get())?
        .map(|bytes| decode_attachment(id.get(), bytes.value()))
        .transpose()
}

pub(crate) fn read_project(
    database: &Database,
    id: ProjectId,
) -> Result<Option<ProjectRecord>, StorageError> {
    let rx = read(database)?;
    rx.open_table(PROJECTS)?
        .get(id.get())?
        .map(|bytes| decode_project(id.get(), bytes.value()))
        .transpose()
}

pub(crate) fn read_projects(
    database: &Database,
    before: Option<ProjectId>,
    limit: usize,
) -> Result<Vec<ProjectRecord>, StorageError> {
    let rx = read(database)?;
    let table = rx.open_table(PROJECTS)?;
    let end = before.map_or(u128::MAX, ProjectId::get);
    let mut records = Vec::new();
    for entry in table.range(..end)?.rev().take(limit.min(MAX_PAGE)) {
        let (key, bytes) = entry?;
        records.push(decode_project(key.value(), bytes.value())?);
    }
    Ok(records)
}

pub(crate) fn config_record(row: ConfigRow) -> ConfigRecord {
    ConfigRecord {
        id: row.id,
        kind: row.kind,
        name: row.name,
        edit_seq: row.edit_seq,
        created_at: row.created_at,
        updated_at: row.updated_at,
        archived_at: row.archived_at,
        model_enabled: row.model_enabled,
        discovery_visible: row.discovery_visible,
        body: row.body,
    }
}

pub(crate) fn read_config(
    database: &Database,
    id: ConfigId,
) -> Result<Option<ConfigRecord>, StorageError> {
    config(&read(database)?, id)
}

pub(crate) fn config(
    rx: &ReadTransaction,
    id: ConfigId,
) -> Result<Option<ConfigRecord>, StorageError> {
    rx.open_table(CONFIGS)?
        .get(id.get())?
        .map(|bytes| ConfigRow::decode_at(id.get(), bytes.value()))
        .transpose()
        .map(|row| row.map(config_record))
}

/// Credential references held by Connections that a live (unarchived) Model uses.
pub(crate) fn live_credential_refs(
    tx: &impl TxRead,
) -> Result<std::collections::BTreeSet<String>, StorageError> {
    let mut connection_refs = std::collections::BTreeMap::new();
    let mut live_connections = std::collections::BTreeSet::new();
    for entry in tx.table(CONFIGS)?.iter()? {
        let (key, bytes) = entry?;
        let row = ConfigRow::decode_at(key.value(), bytes.value())?;
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
            | ConfigBody::DecisionAgent(_) => {}
        }
    }
    Ok(live_connections
        .iter()
        .filter_map(|id| connection_refs.remove(id))
        .flatten()
        .collect())
}

pub(crate) fn read_credential_ref_in_use(
    database: &Database,
    reference: &str,
) -> Result<bool, StorageError> {
    Ok(live_credential_refs(&read(database)?)?.contains(reference))
}

pub(crate) fn read_configs(
    database: &Database,
    kind: Option<ConfigKind>,
    before: Option<ConfigId>,
    limit: usize,
) -> Result<CursorPage<ConfigRecord, ConfigId>, StorageError> {
    let rx = read(database)?;
    let table = rx.open_table(CONFIGS)?;
    let end = before.map_or(u128::MAX, ConfigId::get);
    let limit = limit.clamp(1, MAX_PAGE);
    let mut records = Vec::new();
    let mut last = None;
    let mut more = false;
    for (scanned, entry) in table.range(..end)?.rev().enumerate() {
        if records.len() == limit || scanned == MAX_PAGE {
            more = true;
            break;
        }
        let (key, bytes) = entry?;
        last = Some(ConfigId::new(key.value()));
        let row = ConfigRow::decode_at(key.value(), bytes.value())?;
        if kind.is_none_or(|kind| kind == row.kind) {
            records.push(config_record(row));
        }
    }
    Ok(CursorPage {
        records,
        next: last.filter(|_| more),
    })
}

pub(crate) enum StoredCredential {
    Absent,
    Decrypted(CredentialSecret),
    Undecryptable,
}

pub(crate) fn read_handoff_credential(
    database: &Database,
    reference: &str,
    key: Option<&CredentialKey>,
) -> Result<Option<(CredentialSecret, Vec<u8>)>, StorageError> {
    let rx = read(database)?;
    let table = rx.open_table(CREDENTIALS)?;
    let Some(value) = table.get(reference)? else {
        return Ok(None);
    };
    let bytes = value.value();
    let Some(key) = key else {
        decode_credential(reference, bytes)?;
        return Ok(None);
    };
    Ok(key
        .decrypt(reference, bytes)?
        .map(|secret| (secret, bytes.to_vec())))
}

pub(crate) fn read_credential(
    database: &Database,
    reference: &str,
    key: Option<&CredentialKey>,
) -> Result<StoredCredential, StorageError> {
    let rx = read(database)?;
    let table = rx.open_table(CREDENTIALS)?;
    let Some(bytes) = table.get(reference)? else {
        return Ok(StoredCredential::Absent);
    };
    let Some(key) = key else {
        decode_credential(reference, bytes.value())?;
        return Ok(StoredCredential::Undecryptable);
    };
    match key.decrypt(reference, bytes.value())? {
        Some(secret) => Ok(StoredCredential::Decrypted(secret)),
        None => Ok(StoredCredential::Undecryptable),
    }
}

pub(crate) fn credential_count(database: &Database) -> Result<u64, StorageError> {
    use ::redb::ReadableTableMetadata;
    let rx = read(database)?;
    Ok(rx.open_table(CREDENTIALS)?.len()?)
}

pub(crate) fn read_profile_page(
    database: &Database,
    key: Option<&CredentialKey>,
    before: Option<ConfigId>,
    limit: usize,
    scan: usize,
) -> Result<ProfilePageRecords, StorageError> {
    let rx = read(database)?;
    let configs = rx.open_table(CONFIGS)?;
    let executors = rx.open_table(EXECUTORS)?;
    let pools = rx.open_table(POOLS)?;
    let end = before.map_or(u128::MAX, ConfigId::get);
    let limit = limit.max(1);
    let scan = scan.max(limit);
    let mut records = Vec::new();
    let mut last = None;
    let mut config_rows = configs.range(..end)?.rev();
    let mut executor_rows = executors.range(..end)?.rev();
    let mut next_config = config_rows.next().transpose()?;
    let mut next_executor = executor_rows.next().transpose()?;
    for _ in 0..scan {
        if records.len() >= limit {
            break;
        }
        let is_executor = match (&next_config, &next_executor) {
            (Some((config, _)), Some((executor, _))) => executor.value() > config.value(),
            (None, Some(_)) => true,
            (Some(_), None) => false,
            (None, None) => break,
        };
        let Some((key, bytes)) = (if is_executor {
            next_executor.take()
        } else {
            next_config.take()
        }) else {
            break;
        };
        if is_executor {
            next_executor = executor_rows.next().transpose()?;
        } else {
            next_config = config_rows.next().transpose()?;
        }
        let id = key.value();
        last = Some(ConfigId::new(id));
        if is_executor {
            let executor = allocations::decode_allocation_executor(id, bytes.value())?;
            let pool_bytes = pools
                .get(executor.pool.get())?
                .ok_or_else(|| StorageError::corrupted("Executor references a missing Pool"))?;
            let pool =
                allocations::decode_allocation_pool(executor.pool.get(), pool_bytes.value())?;
            if executor.enabled && !executor.archived && pool.enabled && !pool.archived {
                records.push(ProfilePageRecord::Executor(executor));
            }
            continue;
        }
        let row = ConfigRow::decode_at(id, bytes.value())?;
        if matches!(
            row.body,
            ConfigBody::Model(_)
                | ConfigBody::Agent(_)
                | ConfigBody::DecisionModel(_)
                | ConfigBody::DecisionAgent(_)
        ) && row.archived_at.is_none()
        {
            records.push(ProfilePageRecord::Config(config_record(row)));
        }
    }
    let more = next_config.is_some() || next_executor.is_some();
    let next_before = more.then_some(last).flatten();

    let mut connections = BTreeMap::new();
    for record in &records {
        let record = match record {
            ProfilePageRecord::Config(record) => record,
            ProfilePageRecord::Executor(_) => continue,
        };
        let connection_id = match &record.body {
            ConfigBody::Model(model) => model.connection_id,
            ConfigBody::DecisionModel(model) => model.connection_id,
            ConfigBody::Agent(_) | ConfigBody::DecisionAgent(_) | ConfigBody::Connection(_) => {
                continue;
            }
        };
        if connections.contains_key(&connection_id) {
            continue;
        }
        let Some(bytes) = configs.get(connection_id.get())? else {
            continue;
        };
        let row = ConfigRow::decode_at(connection_id.get(), bytes.value())?;
        if let ConfigBody::Connection(connection) = row.body {
            connections.insert(connection_id, (row.name, connection, row.discovery_visible));
        }
    }
    let mut credentials = BTreeSet::new();
    if let Some(key) = key {
        let table = rx.open_table(CREDENTIALS)?;
        let references: BTreeSet<&str> = connections
            .values()
            .flat_map(|(_, connection, _)| connection.credential_refs())
            .map(str::trim)
            .filter(|reference| !reference.is_empty())
            .collect();
        let mut warned = false;
        for reference in references {
            let Some(bytes) = table.get(reference)? else {
                continue;
            };
            match key.holds(reference, bytes.value()) {
                Ok(true) => {
                    credentials.insert(reference.to_owned());
                }
                Ok(false) => {}
                Err(_) if warned => {}
                Err(_) => {
                    warned = true;
                    log::warn!("credential record {reference} could not be decoded");
                }
            }
        }
    }
    Ok(ProfilePageRecords {
        records,
        connections,
        credentials,
        next_before,
    })
}

pub(crate) fn read_agent_model_configs(
    database: &Database,
    ids: &[ConfigId],
) -> Result<Vec<AgentModelConfig>, StorageError> {
    let rx = read(database)?;
    let mut executors_by_protocol = BTreeMap::<_, Vec<ExecutorId>>::new();
    ids.iter()
        .map(|id| {
            let record = config(&rx, *id)?;
            let executors = if let Some(ConfigRecord {
                body: ConfigBody::Model(model),
                model_enabled: true,
                archived_at: None,
                ..
            }) = &record
            {
                let connection = config(&rx, model.connection_id)?.ok_or_else(|| {
                    StorageError::corrupted("Model references a missing Connection")
                })?;
                let ConfigBody::Connection(connection) = connection.body else {
                    return Err(StorageError::corrupted(
                        "Model references a non-Connection configuration",
                    ));
                };
                use std::collections::btree_map::Entry;
                match executors_by_protocol.entry(connection.protocol) {
                    Entry::Occupied(entry) => entry.get().clone(),
                    Entry::Vacant(entry) => entry
                        .insert(allocations::pools_offering_capability(
                            &rx,
                            ExecutionOperation::LlmRequest,
                            Some(connection.protocol),
                        )?)
                        .clone(),
                }
            } else {
                Vec::new()
            };
            Ok(AgentModelConfig {
                config: record,
                executors,
            })
        })
        .collect()
}

pub(crate) fn global_compaction_settings(
    meta: &impl ReadableTable<&'static str, &'static [u8]>,
) -> Result<crate::db::entities::GlobalCompactionSettings, StorageError> {
    let bytes = meta
        .get("compaction_settings")?
        .ok_or_else(|| StorageError::corrupted("global compaction settings missing"))?;
    let mut d = Dec::new(bytes.value(), TAG_GLOBAL_COMPACTION)?;
    let settings = crate::db::entities::GlobalCompactionSettings {
        revision: d.u64()?,
        prediction_enabled: d.bool()?,
        policy: d.compaction_policy()?,
    };
    d.end()?;
    Ok(settings)
}

pub(crate) fn global_outbound_headers(
    meta: &impl ReadableTable<&'static str, &'static [u8]>,
) -> Result<crate::db::entities::GlobalHeaders, StorageError> {
    let Some(bytes) = meta.get("outbound_headers")? else {
        return Ok(crate::db::entities::GlobalHeaders::default());
    };
    let mut d = Dec::new(bytes.value(), TAG_GLOBAL_HEADERS)?;
    let headers = crate::db::entities::GlobalHeaders {
        revision: d.u32()?,
        fields: d.outbound_headers()?,
    };
    d.end()?;
    Ok(headers)
}

pub(crate) fn read_global_outbound_headers(
    database: &Database,
) -> Result<crate::db::entities::GlobalHeaders, StorageError> {
    global_outbound_headers(&read(database)?.open_table(META)?)
}

pub(crate) fn read_global_compaction_settings(
    database: &Database,
) -> Result<crate::db::entities::GlobalCompactionSettings, StorageError> {
    global_compaction_settings(&read(database)?.open_table(META)?)
}

pub(crate) fn effective_context_policy(
    meta: &impl ReadableTable<&'static str, &'static [u8]>,
) -> Result<(crate::db::entities::CompactionPolicy, u64), StorageError> {
    let mut settings = global_compaction_settings(meta)?;
    if !settings.prediction_enabled
        || matches!(
            settings.policy.mode,
            crate::db::entities::CompactionMode::Off
        )
    {
        settings.policy.predictive = None;
    }
    Ok((settings.policy, settings.revision))
}
