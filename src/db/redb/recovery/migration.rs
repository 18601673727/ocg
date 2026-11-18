use std::fs::{File, OpenOptions};
use std::io::Read;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

use ::redb::{
    Database, Durability, Key, ReadTransaction, ReadableDatabase, ReadableTable,
    ReadableTableMetadata, TableDefinition, TableHandle, WriteTransaction,
};
use serde_json::{Value as Json, json};

use super::offline::{finalization_copy, with_database_copy};
use super::reserved::ReservedRefusal;
use super::{io_storage_error, verify_reserved_residual};
use crate::db::entities::{ContentDigest, OutcomeKind, UncertaintyReason};
use crate::db::redb::codec::{
    Code, Dec, TAG_GLOBAL_COMPACTION, TAG_GLOBAL_HEADERS, TAG_JOB_RUNTIME, TAG_REQUEST_EVENT,
    TAG_TOOL_CALL,
};
use crate::db::redb::execution::TAG_EXECUTION_EVENT;
use crate::db::redb::schema::{
    CREDENTIAL_KEY_FILE_NAME, CREDENTIALS, DATABASE_FORMAT_KEY, FORMAT_VERSION, MESSAGES, META,
    check_format1_layout, check_required_tables, record_version_byte, rewrite_format1_records,
    verify_format1_records,
};
use crate::db::redb::{StorageError, indexes, reads, rows};

const SOURCE_FORMAT: u64 = 1;
const OLD_HOST_SHUTDOWN: &str =
    "The host shut down after external work may have started. Termination was not confirmed.";
const OLD_DEADLINE: &str = "The dispatch deadline expired after external work may have started. Termination was not confirmed.";

fn source_layout(source: &ReadTransaction) -> Result<(), ReservedRefusal> {
    let meta = source.open_table(META).map_err(StorageError::from)?;
    let format = meta.get(DATABASE_FORMAT_KEY).map_err(StorageError::from)?;
    match format.as_ref().map(|marker| marker.value()) {
        Some(bytes) if bytes == SOURCE_FORMAT.to_le_bytes() => {}
        Some(bytes) if bytes == FORMAT_VERSION.to_le_bytes() => {
            check_required_tables(source).map_err(|_| {
                ReservedRefusal::new(
                    "unsupported_source",
                    "this Format 2 layout is obsolete; migrate an original Format 1 database",
                )
            })?;
            return Err(ReservedRefusal::new(
                "already_migrated",
                "the database is already in Format 2",
            ));
        }
        _ => {
            return Err(ReservedRefusal::new(
                "unsupported_source",
                "migration requires Format 1",
            ));
        }
    }
    check_format1_layout(source)?;
    Ok(())
}

fn promote_header(bytes: &mut [u8], tags: &[u8]) -> Result<(), StorageError> {
    match bytes {
        [tag, version, ..] if tags.contains(tag) && u64::from(*version) == SOURCE_FORMAT => {
            *version = record_version_byte();
            Ok(())
        }
        _ => Err(StorageError::corrupted(format!(
            "invalid Format 1 record header for {tags:?}"
        ))),
    }
}

// Format 1 had seven typed codes and exactly two text encodings for Unknown.
// Keep all other bytes, including known outcomes and any history not touched
// by the migration. Even an outcome with a familiar-looking message is
// ambiguous unless it has the original kind and exact encoding.
fn promote_outcome(d: &mut Dec<'_>) -> Result<Option<(usize, usize, u8)>, StorageError> {
    let kind = d.code(OutcomeKind::from_code)?;
    d.ts()?;
    let unknown = kind == OutcomeKind::Unknown;
    let start = d.position();
    let converted = d.opt(|d| {
        match d.u8()? {
            2 if unknown => {
                let reason = match d.str_ref()? {
                    OLD_HOST_SHUTDOWN => UncertaintyReason::HostShutdown,
                    OLD_DEADLINE => UncertaintyReason::DeadlineExceeded,
                    _ => {
                        return Err(StorageError::corrupted(
                            "unrecognized Format 1 Unknown outcome text",
                        ));
                    }
                };
                Ok(Some((start + 1, d.position(), reason.code())))
            }
            7 => {
                if !matches!(d.u8()?, 1..=7) {
                    return Err(StorageError::corrupted("invalid Format 1 uncertainty code"));
                }
                Ok(None)
            }
            _ if unknown => Err(StorageError::corrupted("untyped Format 1 Unknown outcome")),
            _ => Ok(None), // The current full decoder checks unchanged causes.
        }
    })?;
    if unknown && converted.is_none() {
        return Err(StorageError::corrupted(
            "Format 1 Unknown outcome lacks a cause",
        ));
    }
    Ok(converted.flatten())
}

fn promote_record(bytes: &[u8], tags: &[u8]) -> Result<Vec<u8>, StorageError> {
    let mut row = bytes.to_vec();
    promote_header(&mut row, tags)?;
    let tag = row[0];
    let mut d = Dec::new(&row, tag)?;
    let outcome = match tag {
        TAG_JOB_RUNTIME => d.opt(promote_outcome)?.flatten(),
        TAG_TOOL_CALL => {
            d.str_ref()?;
            d.str_ref()?;
            d.bool()?;
            let position = d.position();
            if d.bool()? {
                return Err(StorageError::corrupted(
                    "Format 1 tool evidence names a legacy approval link",
                ));
            }
            row.remove(position);
            None
        }
        TAG_REQUEST_EVENT => {
            d.u128()?;
            d.ts()?;
            d.ts()?;
            if d.u8()? == 6 {
                return Err(StorageError::corrupted(
                    "Format 1 request history contains a legacy approved-call entry",
                ));
            }
            None
        }
        TAG_EXECUTION_EVENT => {
            d.u128()?; // Job
            d.u64()?; // Sequence
            d.u64()?; // Previous sequence
            d.u64()?; // Transaction
            d.ts()?;
            match d.u8()? {
                2 => {
                    let len = d.u32()? as usize;
                    let start = d.position();
                    d.take(len)?;
                    d.end()?;
                    // AllocationReserved contains a full encoded allocation.
                    promote_header(&mut row[start..start + len], &[42])?;
                    None
                }
                6 => promote_outcome(&mut d)?,
                _ => None,
            }
        }
        _ => None,
    };
    if let Some((start, end, code)) = outcome {
        row.splice(start..end, [7, code]);
    }
    Ok(row)
}

pub(in crate::db::redb) fn rewrite_tagged<K: Key + 'static>(
    source: &ReadTransaction,
    tx: &WriteTransaction,
    definition: TableDefinition<'_, K, &'static [u8]>,
    tags: &[u8],
) -> Result<(), StorageError> {
    let mut result = tx.open_table(definition)?;
    for entry in source.open_table(definition)?.iter()? {
        let (key, bytes) = entry?;
        result.insert(key.value(), promote_record(bytes.value(), tags)?.as_slice())?;
    }
    Ok(())
}

fn promote_metadata(key: &str, value: &[u8]) -> Result<Vec<u8>, StorageError> {
    match key {
        DATABASE_FORMAT_KEY => Ok(FORMAT_VERSION.to_le_bytes().to_vec()),
        "compaction_settings" => promote_record(value, &[TAG_GLOBAL_COMPACTION]),
        "outbound_headers" => promote_record(value, &[TAG_GLOBAL_HEADERS]),
        "open_epoch" | "clean_shutdown" => Ok(value.to_vec()),
        _ => Err(StorageError::corrupted(format!(
            "unrecognized Format 1 metadata row {key:?}"
        ))),
    }
}

fn verify_transformed<K: Key + 'static>(
    source: &ReadTransaction,
    result: &ReadTransaction,
    definition: TableDefinition<'_, K, &'static [u8]>,
    mut promote: impl FnMut(K::SelfType<'_>, &[u8]) -> Result<Vec<u8>, StorageError>,
) -> Result<(), StorageError> {
    let source = source.open_table(definition)?;
    let result = result.open_table(definition)?;
    let mut after = result.iter()?;
    for entry in source.iter()? {
        let (key, value) = entry?;
        let Some((new_key, new_value)) = after.next().transpose()? else {
            return Err(StorageError::corrupted(format!(
                "migration lost rows in {}",
                definition.name()
            )));
        };
        if K::as_bytes(&key.value()).as_ref() != K::as_bytes(&new_key.value()).as_ref()
            || promote(key.value(), value.value())? != new_value.value()
        {
            return Err(StorageError::corrupted(format!(
                "migration changed unexpected data in {}",
                definition.name()
            )));
        }
    }
    if after.next().transpose()?.is_some() {
        return Err(StorageError::corrupted(format!(
            "migration added rows in {}",
            definition.name()
        )));
    }
    Ok(())
}

pub(in crate::db::redb) fn verify_tagged<K: Key + 'static>(
    source: &ReadTransaction,
    result: &ReadTransaction,
    definition: TableDefinition<'_, K, &'static [u8]>,
    tags: &[u8],
) -> Result<(), StorageError> {
    verify_transformed(source, result, definition, |_, bytes| {
        promote_record(bytes, tags)
    })
}

pub(in crate::db::redb) fn verify_metadata(
    source: &ReadTransaction,
    result: &ReadTransaction,
) -> Result<(), StorageError> {
    verify_transformed(source, result, META, promote_metadata)
}

fn verify_payloads(tx: &ReadTransaction, directory: &Path) -> Result<(), StorageError> {
    reads::catalog::global_outbound_headers(&tx.open_table(META)?)?;
    for entry in tx.open_table(MESSAGES)?.iter()? {
        let (key, bytes) = entry?;
        if ContentDigest::of(bytes.value()).as_bytes().as_slice() != key.value()
            || serde_json::from_slice::<Json>(bytes.value()).is_err()
        {
            return Err(StorageError::corrupted("invalid content-addressed message"));
        }
    }
    let credentials = tx.open_table(CREDENTIALS)?;
    if !credentials.is_empty()? {
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(directory.join(CREDENTIAL_KEY_FILE_NAME))
            .map_err(io_storage_error)?;
        if !file.metadata().map_err(io_storage_error)?.is_file() {
            return Err(StorageError::corrupted(
                "credential key is not a regular file",
            ));
        }
        let mut raw = Vec::with_capacity(33);
        let key = file
            .take(33)
            .read_to_end(&mut raw)
            .map_err(io_storage_error)
            .and_then(|_| rows::CredentialKey::from_bytes(&raw));
        crate::support::secrets::wipe_vec(&mut raw);
        let key = key?;
        for entry in credentials.iter()? {
            let (name, bytes) = entry?;
            if key.decrypt(name.value(), bytes.value())?.is_none() {
                return Err(StorageError::corrupted(
                    "cannot authenticate a stored credential with ocg.key",
                ));
            }
        }
    }
    Ok(())
}

fn with_intact_copy<T>(
    file: File,
    body: impl FnOnce(&mut Database) -> Result<T, ReservedRefusal>,
) -> Result<T, ReservedRefusal> {
    let mut builder = Database::builder();
    // Opening a damaged primary slot in repair mode can select an older
    // transaction. That would make the snapshot comparison prove the wrong source.
    builder.set_repair_callback(|repair| repair.abort());
    with_database_copy(file, &builder, |database| {
        if !database.check_integrity().map_err(StorageError::from)? {
            return Err(ReservedRefusal::new(
                "page_integrity",
                "redb found damage in the migration copy",
            ));
        }
        body(database)
    })
}

fn verify_delta(
    source: &ReadTransaction,
    result: &impl ReadableDatabase,
    directory: &Path,
) -> Result<Json, ReservedRefusal> {
    source_layout(source)?;
    let residual = verify_reserved_residual(result)?;
    let tx = result.begin_read().map_err(StorageError::from)?;
    verify_format1_records(source, &tx)?;
    verify_payloads(&tx, directory)?;
    Ok(json!({
        "source_format": SOURCE_FORMAT,
        "target_format": FORMAT_VERSION,
        "rows": "verified against Format 1, including execution history and stop proofs",
        "conversation_configuration_references": "rebuilt and verified",
        "ownership": residual.ownership.facts(),
        "normal_startup_accepts": residual.normal_startup_accepts,
    }))
}

fn migrate(database: &Database, directory: &Path) -> Result<Json, ReservedRefusal> {
    let source = database.begin_read().map_err(StorageError::from)?;
    source_layout(&source)?;
    let mut tx = database.begin_write().map_err(StorageError::from)?;
    tx.set_durability(Durability::Immediate)
        .map_err(StorageError::from)?;
    if tx
        .list_persistent_savepoints()
        .map_err(StorageError::from)?
        .next()
        .is_some()
    {
        return Err(ReservedRefusal::new(
            "unsupported_source",
            "the source contains persistent redb savepoints",
        ));
    }
    rewrite_format1_records(&source, &tx)?;
    {
        let mut meta = tx.open_table(META).map_err(StorageError::from)?;
        let old = source.open_table(META).map_err(StorageError::from)?;
        for entry in old.iter().map_err(StorageError::from)? {
            let (key, value) = entry.map_err(StorageError::from)?;
            let bytes = promote_metadata(key.value(), value.value())?;
            meta.insert(key.value(), bytes.as_slice())
                .map_err(StorageError::from)?;
        }
    }
    indexes::rebuild_compaction_refs(&tx)?;
    tx.commit().map_err(StorageError::from)?;
    verify_delta(&source, database, directory)
}

pub(crate) fn migrate_format1_apply(file: File, directory: &Path) -> Result<Json, ReservedRefusal> {
    with_intact_copy(file, |database| {
        let facts = migrate(database, directory)?;
        if !database.check_integrity().map_err(StorageError::from)? {
            return Err(ReservedRefusal::new(
                "page_integrity",
                "redb had to repair the migrated copy",
            ));
        }
        Ok(facts)
    })
}

pub(crate) fn verify_migration_publication_delta(
    source: &File,
    published: &File,
    directory: &Path,
) -> Result<Json, ReservedRefusal> {
    with_intact_copy(finalization_copy(source)?, |source| {
        let source = source.begin_read().map_err(StorageError::from)?;
        with_intact_copy(finalization_copy(published)?, |result| {
            verify_delta(&source, result, directory)
        })
    })
}
