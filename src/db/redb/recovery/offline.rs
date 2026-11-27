use super::io_storage_error;
use crate::db::entities::{ContentDigest, RequestNumber};
use crate::db::redb::StorageError;
use crate::db::redb::batch::{Batch, BatchError};
use crate::db::redb::reads::jobs::{mutable_job_details_in, mutable_job_record_in};
use crate::db::redb::recovery::reserved::ReservedRefusal;
use crate::db::redb::schema::{
    ACTIVE_ALLOCATIONS, ALLOCATION_REQUESTS, CREDENTIAL_KEY_FILE_NAME, CREDENTIALS,
    MAX_OBSERVATION_SCAN, MESSAGES, META, POOL_ALLOCATIONS, POOLS, REQUEST_EVENTS,
};
#[cfg(unix)]
use crate::db::redb::schema::{CONVERSATION_CONFIG_REFS, verify_compaction_rebuild_preservation};
use crate::db::redb::schema::{DATABASE_FILE_NAME, FORMAT_VERSION};
use crate::db::redb::{allocations, batch, indexes, reads, recovery, rows};
use crate::host::identity::{current_boot_id, current_host_id};
use ::redb::{Database, ReadTransaction, ReadableDatabase, ReadableTable, ReadableTableMetadata};
use serde_json::Value as Json;
use std::fs::OpenOptions;
use std::io::Read;
use std::path::Path;

#[cfg(all(test, unix))]
#[path = "offline_tests.rs"]
mod tests;

enum OfflineAccess<'a> {
    InPlace(&'a ::redb::ReadOnlyDatabase),
    Copy(&'a mut Database),
}

const MAX_INSPECTED_HELD: usize = 64;

fn with_offline_database<T>(
    directory: &std::path::Path,
    force_copy: bool,
    inspect: impl FnOnce(OfflineAccess<'_>) -> Result<T, StorageError>,
) -> Result<(T, u64), StorageError> {
    let path = recovery::database_file(directory);
    let bytes = std::fs::metadata(&path).map_err(io_storage_error)?.len();
    let in_place = if force_copy {
        None
    } else {
        match ::redb::ReadOnlyDatabase::open(&path) {
            Ok(db) => Some(db),
            Err(::redb::DatabaseError::RepairAborted) => None,
            Err(error) => return Err(error.into()),
        }
    };
    let result = if let Some(database) = &in_place {
        inspect(OfflineAccess::InPlace(database))?
    } else {
        let source = std::fs::File::open(&path).map_err(io_storage_error)?;
        let copy = verification_copy(&source, bytes, "database changed during inspection copy")?;
        let mut database =
            Database::builder().create_with_backend(::redb::backends::FileBackend::new(copy))?;
        inspect(OfflineAccess::Copy(&mut database))?
    };
    Ok((result, bytes))
}

pub(super) fn verification_copy(
    file: &std::fs::File,
    expected: u64,
    changed: &str,
) -> Result<std::fs::File, StorageError> {
    let scratch = std::env::temp_dir().join(format!(".ocg-verify-{}", uuid::Uuid::now_v7()));
    let mut create = std::fs::OpenOptions::new();
    create.read(true).write(true).create_new(true);
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        const GENERIC_READ: u32 = 0x8000_0000;
        const GENERIC_WRITE: u32 = 0x4000_0000;
        const DELETE: u32 = 0x0001_0000;
        const DELETE_ON_CLOSE: u32 = 0x0400_0000;
        create.access_mode(GENERIC_READ | GENERIC_WRITE | DELETE);
        create.custom_flags(DELETE_ON_CLOSE);
    }
    let copy = crate::host::data_directory::private_file(&mut create)
        .open(&scratch)
        .map_err(io_storage_error)?;
    #[cfg(not(windows))]
    std::fs::remove_file(&scratch).map_err(io_storage_error)?;
    copy_database(file, copy, expected, changed)
}

pub(super) fn copy_database(
    source: &std::fs::File,
    mut copy: std::fs::File,
    expected: u64,
    changed: &str,
) -> Result<std::fs::File, StorageError> {
    use std::io::{Seek, SeekFrom};

    let mut source = source.try_clone().map_err(io_storage_error)?;
    source.seek(SeekFrom::Start(0)).map_err(io_storage_error)?;
    let copied = std::io::copy(&mut source, &mut copy).map_err(io_storage_error)?;
    if copied != expected || copied == 0 {
        return Err(StorageError::corrupted(changed));
    }
    Ok(copy)
}

pub(super) fn with_reconciliation_copy<T>(
    file: std::fs::File,
    body: impl FnOnce(&mut Database) -> Result<T, ReservedRefusal>,
) -> Result<T, ReservedRefusal> {
    with_database_copy(file, &Database::builder(), body)
}

pub(super) fn with_database_copy<T>(
    file: std::fs::File,
    builder: &::redb::Builder,
    body: impl FnOnce(&mut Database) -> Result<T, ReservedRefusal>,
) -> Result<T, ReservedRefusal> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut database =
            match builder.create_with_backend(::redb::backends::FileBackend::new(file)) {
                Err(::redb::DatabaseError::RepairAborted) => {
                    return Err(ReservedRefusal::new(
                        "page_integrity",
                        "the offline copy requires redb repair",
                    ));
                }
                result => result?,
            };
        body(&mut database)
    }))
    .unwrap_or_else(|_| {
        Err(ReservedRefusal::new(
            "inconsistent_or_corrupt",
            "redb aborted while reading a damaged page",
        ))
    })
}

pub(super) fn finalization_copy(file: &std::fs::File) -> Result<std::fs::File, ReservedRefusal> {
    Ok(verification_copy(
        file,
        file.metadata().map_err(io_storage_error)?.len(),
        "a finalization input changed while it was copied",
    )?)
}

pub(super) fn with_verified_finalization_copy<T>(
    file: std::fs::File,
    name: &str,
    body: impl FnOnce(&mut Database) -> Result<T, ReservedRefusal>,
) -> Result<T, ReservedRefusal> {
    with_reconciliation_copy(file, |database| {
        if !database.check_integrity().map_err(StorageError::from)? {
            return Err(ReservedRefusal::new(
                "page_integrity",
                format!("redb found damage in the {name} copy and had to repair it"),
            ));
        }
        body(database)
    })
}

#[cfg(unix)]
pub(super) fn with_verified_rebuild_copy<T>(
    file: std::fs::File,
    body: impl FnOnce(&mut Database) -> Result<T, ReservedRefusal>,
) -> Result<T, ReservedRefusal> {
    let mut builder = Database::builder();
    builder.set_repair_callback(|repair| repair.abort());
    with_database_copy(file, &builder, |database| {
        if !database.check_integrity().map_err(StorageError::from)? {
            return Err(ReservedRefusal::new(
                "page_integrity",
                "the offline rebuild copy failed redb integrity verification",
            ));
        }
        body(database)
    })
}

#[cfg(unix)]
pub(crate) struct CompactionRefRebuildPlan {
    pub(crate) evidence: Json,
    pub(crate) digest: String,
    changes_required: bool,
}

#[cfg(unix)]
fn compaction_ref_rebuild_plan(
    database: &Database,
    directory: &Path,
    source_sha256: &str,
) -> Result<CompactionRefRebuildPlan, ReservedRefusal> {
    use ::redb::TableHandle;
    use serde_json::json;

    let tx = database.begin_read().map_err(StorageError::from)?;
    recovery::startup_metadata(&tx)?;
    recovery::check_records_after_compaction_refs(&tx)?;
    recovery::residual_ownership_in(&tx)?;
    verify_payloads(&tx, directory)?;
    let counts = indexes::compaction_ref_rebuild_counts(&tx)?;
    let changes_required = counts.missing != 0 || counts.extra != 0;
    let evidence = json!({
        "kind": "ocg-compaction-ref-rebuild-plan-v1",
        "table": CONVERSATION_CONFIG_REFS.name(),
        "database_format": FORMAT_VERSION,
        "source_sha256": source_sha256,
        "expected_refs": counts.expected,
        "missing_refs": counts.missing,
        "extra_refs": counts.extra,
        "changes_required": changes_required,
    });
    let bytes = serde_json::to_vec(&evidence)
        .map_err(|error| ReservedRefusal::new("invalid_plan", error.to_string()))?;
    Ok(CompactionRefRebuildPlan {
        evidence,
        digest: ContentDigest::of(&bytes).to_string(),
        changes_required,
    })
}

#[cfg(unix)]
pub(crate) fn plan_compaction_ref_rebuild(
    file: std::fs::File,
    directory: &Path,
    source_sha256: &str,
) -> Result<CompactionRefRebuildPlan, ReservedRefusal> {
    with_verified_rebuild_copy(file, |database| {
        compaction_ref_rebuild_plan(database, directory, source_sha256)
    })
}

#[cfg(unix)]
pub(crate) fn apply_compaction_ref_rebuild(
    file: std::fs::File,
    directory: &Path,
    source_sha256: &str,
    expected_digest: &str,
) -> Result<Json, ReservedRefusal> {
    with_verified_rebuild_copy(file, |database| {
        let plan = compaction_ref_rebuild_plan(database, directory, source_sha256)?;
        if plan.digest != expected_digest {
            return Err(ReservedRefusal::new(
                "plan_digest_mismatch",
                "the source or rebuild plan changed; repeat the dry-run",
            ));
        }
        if !plan.changes_required {
            return Err(ReservedRefusal::new(
                "index_already_current",
                "the conversation configuration reference index already matches its source rows",
            ));
        }
        let mut tx = database.begin_write().map_err(StorageError::from)?;
        tx.set_durability(::redb::Durability::Immediate)
            .map_err(StorageError::from)?;
        indexes::rebuild_compaction_refs(&tx)?;
        tx.commit().map_err(StorageError::from)?;
        Ok(serde_json::json!({"plan": plan.evidence, "plan_digest": plan.digest}))
    })
}

#[cfg(unix)]
pub(crate) fn verify_compaction_ref_publication_delta(
    directory: &Path,
    source: &std::fs::File,
    replacement: &std::fs::File,
    source_sha256: &str,
    expected_plan: &Json,
    expected_digest: &str,
) -> Result<Json, ReservedRefusal> {
    let source = finalization_copy(source)?;
    let replacement = finalization_copy(replacement)?;
    with_verified_rebuild_copy(source, |source| {
        let plan = compaction_ref_rebuild_plan(source, directory, source_sha256)?;
        if plan.digest != expected_digest
            || &plan.evidence != expected_plan
            || !plan.changes_required
        {
            return Err(ReservedRefusal::new(
                "plan_digest_mismatch",
                "the prepared plan is not the exact rebuild plan authorized by its source",
            ));
        }
        with_verified_rebuild_copy(replacement, |replacement| {
            let residual = recovery::verify_reserved_residual(replacement)?;
            let source_tx = source.begin_read().map_err(StorageError::from)?;
            let replacement_tx = replacement.begin_read().map_err(StorageError::from)?;
            verify_payloads(&replacement_tx, directory)?;
            verify_compaction_rebuild_preservation(&source_tx, &replacement_tx)?;
            Ok(serde_json::json!({
                "plan": plan.evidence,
                "plan_digest": plan.digest,
                "other_tables_byte_identical": true,
                "residual_ownership": residual.facts(),
            }))
        })
    })
}

pub(super) fn apply_reconciliation<T>(
    database: &mut Database,
    epoch: u64,
    action: &str,
    apply: impl FnOnce(&mut Batch) -> Result<T, BatchError>,
) -> Result<(), ReservedRefusal> {
    match batch::run_batch_with(database, epoch, batch::Commit::Durable, apply).result {
        Ok(_) => {}
        Err(batch::BatchFailure::Rejected(violation)) => {
            return Err(ReservedRefusal::new("rejected", violation.to_string()));
        }
        Err(
            batch::BatchFailure::NotCommitted(error) | batch::BatchFailure::OutcomeUnknown(error),
        ) => {
            return Err(error.into());
        }
    }
    if !database.check_integrity().map_err(StorageError::from)? {
        return Err(ReservedRefusal::new(
            "page_integrity",
            format!("redb found damage in the {action} copy and had to repair it"),
        ));
    }
    Ok(())
}

pub(crate) struct OfflineVerification {
    pub(crate) report: serde_json::Value,
    pub(crate) passed: bool,
}

pub(crate) fn verify_offline(
    directory: &std::path::Path,
    pages: bool,
) -> Result<OfflineVerification, StorageError> {
    use serde_json::json;
    let ((mut report, passed), bytes) = with_offline_database(directory, pages, |access| {
        match access {
            OfflineAccess::InPlace(database) => {
                let (logical, logical_passed) = residual_logical_report(directory, database);
                Ok((
                    json!({
                        "access": "read-only; the database file was not copied",
                        "logical_checks": logical,
                        "page_checksums": "not checked; pass --pages to check them on a private copy",
                    }),
                    logical_passed,
                ))
            }
            OfflineAccess::Copy(database) => {
                let (logical, logical_passed) = residual_logical_report(directory, &*database);
                let intact = database.check_integrity()?;
                Ok((
                    json!({
                        "access": "read from a private unlinked copy; the original file was not opened for writing",
                        "logical_checks": logical,
                        "page_checksums": if intact {
                            "every page checksum verified"
                        } else {
                            "redb found damage and had to repair the copy; the original needs restoring from a backup"
                        },
                    }),
                    logical_passed && intact,
                ))
            }
        }
    })?;
    report["file_bytes"] = json!(bytes);
    report["file_unchanged_during_check"] = json!(true);
    Ok(OfflineVerification { report, passed })
}

fn residual_logical_report(directory: &Path, database: &impl ReadableDatabase) -> (Json, bool) {
    let verification = recovery::verify_reserved_residual(database).and_then(|result| {
        verify_payloads(&database.begin_read()?, directory)?;
        Ok(result)
    });
    match verification {
        Ok(residual) => (
            serde_json::json!({
                "passed": true,
                "held_allocations": residual.held.len(),
                "retained_unknown_outcomes": residual.retained_unknown.len(),
                "resolved_unknown_outcomes": residual.resolved_unknown.len(),
                "normal_startup": "accepted",
            }),
            true,
        ),
        Err(error) => (
            serde_json::json!({"passed": false, "detail": error.detail}),
            false,
        ),
    }
}

pub(super) fn verify_payloads(tx: &ReadTransaction, directory: &Path) -> Result<(), StorageError> {
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
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
        }
        let file = options
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

fn logical_report(result: Result<recovery::Verification, StorageError>) -> serde_json::Value {
    match result {
        Ok(_) => serde_json::json!({"passed": true}),
        Err(error) => serde_json::json!({"passed": false, "detail": error.detail}),
    }
}

pub(crate) fn inspect_held_allocations_offline(
    directory: &std::path::Path,
) -> Result<serde_json::Value, StorageError> {
    let (mut report, bytes) = with_offline_database(directory, false, |access| match access {
        OfflineAccess::InPlace(database) => {
            inspect_held_in(database, "read-only; the database file was not copied")
        }
        OfflineAccess::Copy(database) => inspect_held_in(
            database,
            "the database was not closed cleanly; read from a private unlinked copy \
                 that redb repaired, the original file was not opened for writing",
        ),
    })?;
    report["database"]["file_bytes"] = serde_json::json!(bytes);
    report["database"]["file_unchanged_during_inspection"] = serde_json::json!(true);
    Ok(report)
}

#[cfg(unix)]
pub(crate) fn verify_publication(
    directory: &std::path::Path,
    database_file: &std::fs::File,
    exclusive_lock: &std::fs::File,
    expected_device: u64,
    expected_inode: u64,
    recovered: bool,
) -> Result<serde_json::Value, StorageError> {
    use serde_json::json;
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

    fn same_file(left: &std::fs::Metadata, right: &std::fs::Metadata) -> bool {
        left.dev() == right.dev() && left.ino() == right.ino()
    }
    fn open_named(path: &std::path::Path) -> Result<std::fs::File, StorageError> {
        let mut options = std::fs::OpenOptions::new();
        options.read(true).custom_flags(libc::O_NOFOLLOW);
        options.open(path).map_err(io_storage_error)
    }

    let directory = std::fs::canonicalize(directory).map_err(io_storage_error)?;
    let named_lock = open_named(&directory.join(crate::host::data_directory::DATABASE_LOCK_FILE))?;
    if !same_file(
        &named_lock.metadata().map_err(io_storage_error)?,
        &exclusive_lock.metadata().map_err(io_storage_error)?,
    ) {
        return Err(StorageError::corrupted(
            "the publication lock pathname no longer identifies the held lock",
        ));
    }
    match named_lock.try_lock() {
        Err(std::fs::TryLockError::WouldBlock) => {}
        Ok(()) => {
            return Err(StorageError::corrupted(
                "the publication lock is not exclusively held",
            ));
        }
        Err(std::fs::TryLockError::Error(error)) => return Err(io_storage_error(error)),
    }
    let named_database = open_named(&directory.join(DATABASE_FILE_NAME))?;
    let named_before = named_database.metadata().map_err(io_storage_error)?;
    let held_before = database_file.metadata().map_err(io_storage_error)?;
    if !named_before.is_file()
        || named_before.nlink() != 1
        || named_before.dev() != expected_device
        || named_before.ino() != expected_inode
        || !same_file(&named_before, &held_before)
    {
        return Err(StorageError::corrupted(
            "the published database pathname no longer identifies the intended file",
        ));
    }

    let copy = verification_copy(
        database_file,
        held_before.len(),
        "the published database changed while it was copied",
    )?;

    let guarded = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut database =
            Database::builder().create_with_backend(::redb::backends::FileBackend::new(copy))?;
        let logical = if recovered {
            logical_report(recovery::verify_recovered(&database))
        } else {
            residual_logical_report(&directory, &database).0
        };
        let intact = database.check_integrity()?;
        Ok::<_, StorageError>((logical, intact))
    }))
    .unwrap_or_else(|_| {
        Err(StorageError::corrupted(
            "redb aborted while reading a damaged published page",
        ))
    })?;
    let named_after = open_named(&directory.join(DATABASE_FILE_NAME))?
        .metadata()
        .map_err(io_storage_error)?;
    let held_after = database_file.metadata().map_err(io_storage_error)?;
    if !same_file(&named_before, &named_after)
        || !same_file(&held_before, &held_after)
        || named_after.len() != named_before.len()
        || named_after.modified().map_err(io_storage_error)?
            != named_before.modified().map_err(io_storage_error)?
        || held_after.len() != held_before.len()
        || held_after.modified().map_err(io_storage_error)?
            != held_before.modified().map_err(io_storage_error)?
    {
        return Err(StorageError::corrupted(
            "the published database changed during held-lock verification",
        ));
    }
    match named_lock.try_lock() {
        Err(std::fs::TryLockError::WouldBlock) => {}
        Ok(()) => {
            return Err(StorageError::corrupted(
                "the publication lock was released during verification",
            ));
        }
        Err(std::fs::TryLockError::Error(error)) => return Err(io_storage_error(error)),
    }
    Ok(json!({
        "access": "private copy of the exact open published file; the exclusive publication lock remained held",
        "exclusive_lock_held": true,
        "logical_checks": guarded.0,
        "page_checksums": if guarded.1 {
            "every page checksum verified"
        } else {
            "redb found damage and had to repair the copy; publication is inconsistent"
        },
        "file_bytes": named_after.len(),
        "file_unchanged_during_check": true,
    }))
}

fn inspect_held_in(
    database: &impl ::redb::ReadableDatabase,
    access: &str,
) -> Result<serde_json::Value, StorageError> {
    use crate::db::entities::{MutableFrozenConfiguration, RequestPayload};
    use serde_json::json;

    fn text(value: &impl std::fmt::Debug) -> String {
        let mut text = format!("{value:?}");
        if text.len() > 240 {
            let mut end = 240;
            while !text.is_char_boundary(end) {
                end -= 1;
            }
            text.truncate(end);
            text.push('…');
        }
        text
    }

    let tx = database.begin_read()?;
    let meta = tx.open_table(META)?;
    recovery::check_identity(&meta)?;
    let open_epoch = meta
        .get("open_epoch")?
        .and_then(|value| <[u8; 8]>::try_from(value.value()).ok())
        .map(u64::from_le_bytes);
    let clean_shutdown = meta
        .get("clean_shutdown")?
        .map(|value| value.value() == [1]);

    let allocations = tx.open_table(POOL_ALLOCATIONS)?;
    let (mut total, mut held_total) = (0u64, 0u64);
    let mut held = Vec::new();
    for entry in allocations.iter()? {
        let (key, value) = entry?;
        let allocation = allocations::decode_pool_allocation(key.value(), value.value())?;
        total += 1;
        if allocation.holds_capacity() {
            held_total += 1;
            if held.len() < MAX_INSPECTED_HELD {
                held.push(allocation);
            }
        }
    }

    let inspector_host = current_host_id();
    let inspector_boot = current_boot_id();
    let events_table = tx.open_table(REQUEST_EVENTS)?;
    let mut reports = Vec::new();
    for allocation in &held {
        let job = allocation.token.job;
        let mut established = vec![
            "The Allocation row exists and has not been released; its capacity is still owned."
                .to_owned(),
        ];
        let mut inferred = Vec::new();
        let mut unknown = vec![
            "Whether any external provider work for this Job is still running or was billed."
                .to_owned(),
            "Whether any process of the host that claimed this Allocation is still running."
                .to_owned(),
        ];

        let record = mutable_job_record_in(&tx, job)?;
        let consistency = match mutable_job_details_in(&tx, job, None, false) {
            Ok(Some(_)) => "consistent".to_owned(),
            Ok(None) => "the Job row is missing".to_owned(),
            Err(error) => format!("inconsistent: {error}"),
        };
        let (job_json, configuration) = match &record {
            Some(record) => {
                let configuration = match &record.job.frozen {
                    Some(MutableFrozenConfiguration::Generative(_)) => "generative",
                    Some(MutableFrozenConfiguration::Decision(_)) => "decision",
                    None => "not frozen",
                };
                let outcome = record.outcome.as_ref().map(|outcome| {
                    json!({
                        "kind": text(&outcome.kind),
                        "at_unix_micros": outcome.at.as_unix_micros(),
                        "cause": outcome.cause.as_ref().map(|cause| match cause {
                            crate::db::entities::OutcomeCause::Uncertain(reason) => {
                                format!("uncertain:{}", reason.as_str())
                            }
                            other => text(other),
                        }),
                    })
                });
                (
                    json!({
                        "phase": text(&record.job.phase),
                        "revision": record.job.revision,
                        "frozen_configuration": configuration,
                        "outcome": outcome,
                    }),
                    configuration,
                )
            }
            None => (json!(null), "unknown"),
        };
        match record.as_ref().and_then(|record| record.outcome.as_ref()) {
            Some(outcome) if outcome.kind == crate::db::entities::OutcomeKind::Unknown => {
                established.push(
                    "The Job's stored outcome is Unknown: the execution outcome was recorded as uncertain."
                        .to_owned(),
                );
                match &outcome.cause {
                    Some(crate::db::entities::OutcomeCause::Uncertain(reason)) => {
                        established.push(format!(
                            "The recorded local reason is {}: what this process could not establish. It is not evidence that remote work stopped, was refused, or incurred no charge.",
                            reason.as_str()
                        ));
                    }
                    Some(crate::db::entities::OutcomeCause::Text(detail)) => {
                        established.push(detail.clone());
                    }
                    _ => unknown.push(
                        "Why the outcome is uncertain: this Unknown outcome stores no typed reason."
                            .to_owned(),
                    ),
                }
            }
            Some(_) => inferred.push(
                "The Job has a terminal outcome other than Unknown but still owns capacity; this is not a state the runtime writes."
                    .to_owned(),
            ),
            None => inferred.push(
                "The Job has no stored outcome: it is Reserved or Running, or the process stopped before it recorded one. The stored model cannot tell an active run from a stopped one."
                    .to_owned(),
            ),
        }

        let mut requests: std::collections::BTreeMap<u32, Vec<serde_json::Value>> =
            std::collections::BTreeMap::new();
        let mut kinds: std::collections::BTreeMap<u32, Vec<&'static str>> =
            std::collections::BTreeMap::new();
        let mut truncated = false;
        for (scanned, entry) in events_table
            .range((job.get(), 0, 0)..=(job.get(), u32::MAX, u32::MAX))?
            .enumerate()
        {
            if scanned == MAX_OBSERVATION_SCAN {
                truncated = true;
                break;
            }
            let (key, bytes) = entry?;
            let (_, request, seq) = key.value();
            let number = RequestNumber::new(request)
                .ok_or_else(|| StorageError::corrupted("a stored request number is 0"))?;
            let event = super::rows::decode_request_event(job, number, seq, bytes.value())?;
            let (kind, detail) = match &event.payload {
                RequestPayload::Sent { .. } => ("sent", json!(null)),
                RequestPayload::Received { .. } => ("received", json!(null)),
                RequestPayload::Measurement { .. } => ("measurement", json!(null)),
                RequestPayload::Availability {
                    status_code,
                    failure,
                    ..
                } => (
                    "availability",
                    json!({
                        "http_status": status_code,
                        "failure": failure.as_ref().map(text),
                    }),
                ),
                RequestPayload::Token { .. } => ("token", json!(null)),
            };
            kinds.entry(request).or_default().push(kind);
            requests.entry(request).or_default().push(json!({
                "seq": seq,
                "kind": kind,
                "observed_at_unix_micros": event.observed_at.as_unix_micros(),
                "detail": detail,
            }));
        }
        let latest = kinds.iter().next_back().map(|(_, kinds)| kinds.as_slice());
        let progress = match latest {
            None => {
                inferred.push(
                    "No request evidence exists. The runtime writes the Sent row before it transmits, so no transmission is believed to have started (code ordering, not stored proof)."
                        .to_owned(),
                );
                "no_request_evidence"
            }
            Some(kinds) if kinds.contains(&"received") => {
                established.push(
                    "A Received row is stored: a complete provider response was recorded."
                        .to_owned(),
                );
                inferred.push(
                    "Whether the Job result was finalized cannot be told from the Allocation alone; see the Job outcome and result."
                        .to_owned(),
                );
                "response_received_recorded"
            }
            Some(kinds) if kinds.contains(&"availability") => {
                established.push(
                    "An Availability row is stored: the runtime recorded a classified provider failure."
                        .to_owned(),
                );
                "failure_recorded_without_response"
            }
            Some(_) => {
                established.push(
                    "A Sent row is stored: the runtime recorded that it began transmitting the request."
                        .to_owned(),
                );
                inferred.push(
                    "The request may have reached the provider; no response was recorded, so the provider may or may not have processed it."
                        .to_owned(),
                );
                "sent_without_response"
            }
        };

        let host_matches = allocation
            .host_id
            .as_ref()
            .zip(inspector_host.as_ref())
            .map(|(claimed, current)| claimed == current);
        let boot_matches = allocation
            .boot_id
            .as_ref()
            .zip(inspector_boot.as_ref())
            .map(|(claimed, current)| claimed == current);
        let commands = super::held::unfinished_commands(&tx, job)?;
        let proof = super::held::stop_proof(
            allocation,
            &commands,
            inspector_host.as_deref(),
            inspector_boot.as_deref(),
        );
        if host_matches == Some(true) && boot_matches == Some(false) {
            inferred.push(
                "The claiming boot differs from this machine's boot, so the claiming process cannot still be running here. This says nothing about the provider."
                    .to_owned(),
            );
        }
        reports.push(json!({
            "job": job.to_string(),
            "allocation": {
                "pool": allocation.token.pool.to_string(),
                "executor": allocation.token.executor.to_string(),
                "generation": allocation.token.generation.get(),
                "epoch": allocation.token.epoch,
                "available_stop_proof": proof.map(super::held::recovery_proof_name),
                "job_revision": allocation.job_revision,
                "phase": text(&allocation.phase),
                "holds_capacity": allocation.holds_capacity(),
                "operation": text(&allocation.operation),
                "protocol": allocation.protocol.as_ref().map(text),
                "claimed_at_unix_micros": allocation.claimed_at.as_unix_micros(),
                "started_at_unix_micros": allocation.started_at.map(|at| at.as_unix_micros()),
                "host_id": allocation.host_id,
                "boot_id": allocation.boot_id,
                "claim_host_matches_inspector": host_matches,
                "claim_boot_matches_inspector": boot_matches,
            },
            "job_state": job_json,
            "frozen_configuration": configuration,
            "consistency": consistency,
            "request_evidence": {
                "progress": progress,
                "requests": requests,
                "truncated": truncated,
            },
            "established": established,
            "inferred": inferred,
            "unknown": unknown,
        }));
    }

    let allocation_indexes = match (
        tx.open_table(POOLS),
        tx.open_table(ACTIVE_ALLOCATIONS),
        tx.open_table(ALLOCATION_REQUESTS),
    ) {
        (Ok(pools), Ok(active), Ok(requests)) => {
            match indexes::allocation_indexes_agree(&allocations, &pools, &active, &requests) {
                Ok(true) => "consistent".to_owned(),
                Ok(false) => "the stored indexes differ from their source rows".to_owned(),
                Err(error) => format!("inconsistent: {error}"),
            }
        }
        _ => "an index table is missing".to_owned(),
    };
    Ok(json!({
        "database": {
            "format": FORMAT_VERSION,
            "open_epoch": open_epoch,
            "clean_shutdown": clean_shutdown,
            "access": access,
        },
        "allocations_total": total,
        "held_total": held_total,
        "held_reported": reports.len(),
        "held": reports,
        "allocation_indexes": allocation_indexes,
        "note": "Read-only. Nothing was adopted, released, cancelled, retried or finalized.",
    }))
}
