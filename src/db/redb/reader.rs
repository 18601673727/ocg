use std::ops::Bound;

use ::redb::ReadableTableMetadata;

use super::*;
use crate::db::derived::ActivityProjection;
use crate::db::entities::{
    AllocationPhase, ConfigBody, ConfigId, ConfigKind, ConfigRecord, ConnectionConfig,
    DependencyState, ExecutionConfiguration, ExecutionOperation, JobInputRecord, OutcomeKind,
    SubmittedConfiguration, ToolCallRef,
};

pub(super) fn read(database: &Database) -> Result<ReadTransaction, StorageError> {
    Ok(database.begin_read()?)
}

enum OfflineAccess<'a> {
    InPlace(&'a ::redb::ReadOnlyDatabase),
    Copy(&'a mut Database),
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct OfflineFile {
    pub(crate) bytes: u64,
    pub(crate) device: u64,
    pub(crate) inode: u64,
    pub(crate) mode: u32,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct OfflineOptions {
    pub(crate) force_copy: bool,
    pub(crate) require_host_lock: bool,
}

const MAX_INSPECTED_HELD: usize = 64;

fn with_offline_database<T>(
    directory: &std::path::Path,
    options: OfflineOptions,
    inspect: impl FnOnce(OfflineAccess<'_>) -> Result<T, StorageError>,
) -> Result<(T, OfflineFile), StorageError> {
    let _ = options.require_host_lock;
    let io = |error: std::io::Error| StorageError {
        kind: StorageErrorKind::Io(error.kind()),
        os_error: error.raw_os_error(),
        detail: error.to_string(),
    };
    let path = recovery::database_file(directory);
    let meta = std::fs::metadata(&path).map_err(io)?;
    let mut identity = OfflineFile {
        bytes: meta.len(),
        device: 0,
        inode: 0,
        mode: 0,
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        identity.device = meta.dev();
        identity.inode = meta.ino();
        identity.mode = meta.permissions().mode() & 0o7777;
    }
    let in_place = if options.force_copy {
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
        let scratch = std::env::temp_dir().join(format!(".ocg-inspect-{}", uuid::Uuid::now_v7()));
        std::fs::copy(&path, &scratch).map_err(io)?;
        let copy = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&scratch)
            .map_err(io)?;
        let _ = std::fs::remove_file(&scratch);
        let mut database = Database::builder().create_file(copy)?;
        inspect(OfflineAccess::Copy(&mut database))?
    };
    Ok((result, identity))
}

pub(crate) fn verify_offline(
    directory: &std::path::Path,
    pages: bool,
) -> Result<serde_json::Value, StorageError> {
    use serde_json::json;
    let (mut report, file) = with_offline_database(
        directory,
        OfflineOptions {
            force_copy: pages,
            require_host_lock: false,
        },
        |access| {
            let (access_note, page_checksums) = match access {
                OfflineAccess::InPlace(database) => {
                    let logical = recovery::verify_full(database);
                    return Ok(json!({
                        "access": "read-only; the database file was not copied",
                        "logical_checks": logical_report(logical),
                        "page_checksums": "not checked; pass --pages to check them on a private copy",
                    }));
                }
                OfflineAccess::Copy(database) => {
                    let logical = logical_report(recovery::verify_full(&*database));
                    let intact = database.check_integrity()?;
                    (
                        "read from a private unlinked copy; the original file was not opened for writing",
                        (logical, intact),
                    )
                }
            };
            Ok(json!({
                "access": access_note,
                "logical_checks": page_checksums.0,
                "page_checksums": if page_checksums.1 {
                    "every page checksum verified"
                } else {
                    "redb found damage and had to repair the copy; the original needs restoring from a backup"
                },
            }))
        },
    )?;
    report["file_bytes"] = json!(file.bytes);
    report["file_unchanged_during_check"] = json!(true);
    Ok(report)
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
    let (mut report, file) = with_offline_database(
        directory,
        OfflineOptions::default(),
        |access| match access {
            OfflineAccess::InPlace(database) => {
                inspect_held_in(database, "read-only; the database file was not copied")
            }
            OfflineAccess::Copy(database) => inspect_held_in(
                database,
                "the database was not closed cleanly; read from a private unlinked copy \
                 that redb repaired, the original file was not opened for writing",
            ),
        },
    )?;
    report["database"]["file_bytes"] = serde_json::json!(file.bytes);
    report["database"]["file_unchanged_during_inspection"] = serde_json::json!(true);
    Ok(report)
}

#[cfg(unix)]
pub(crate) fn verify_reserved_publication(
    directory: &std::path::Path,
    database: &std::fs::File,
    exclusive_lock: &std::fs::File,
    expected_device: u64,
    expected_inode: u64,
) -> Result<serde_json::Value, StorageError> {
    verify_publication_with_exclusive_lock(
        directory,
        database,
        exclusive_lock,
        expected_device,
        expected_inode,
        false,
    )
}

#[cfg(unix)]
pub(crate) fn verify_recovered_publication(
    directory: &std::path::Path,
    database: &std::fs::File,
    exclusive_lock: &std::fs::File,
    expected_device: u64,
    expected_inode: u64,
) -> Result<serde_json::Value, StorageError> {
    verify_publication_with_exclusive_lock(
        directory,
        database,
        exclusive_lock,
        expected_device,
        expected_inode,
        true,
    )
}

#[cfg(unix)]
fn verify_publication_with_exclusive_lock(
    directory: &std::path::Path,
    database_file: &std::fs::File,
    exclusive_lock: &std::fs::File,
    expected_device: u64,
    expected_inode: u64,
    recovered: bool,
) -> Result<serde_json::Value, StorageError> {
    use serde_json::json;
    use std::io::{Seek, SeekFrom};
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

    fn io(error: std::io::Error) -> StorageError {
        StorageError {
            kind: StorageErrorKind::Io(error.kind()),
            os_error: error.raw_os_error(),
            detail: error.to_string(),
        }
    }
    fn same_file(left: &std::fs::Metadata, right: &std::fs::Metadata) -> bool {
        left.dev() == right.dev() && left.ino() == right.ino()
    }
    fn open_named(path: &std::path::Path) -> Result<std::fs::File, StorageError> {
        let mut options = std::fs::OpenOptions::new();
        options.read(true).custom_flags(libc::O_NOFOLLOW);
        options.open(path).map_err(io)
    }

    let directory = std::fs::canonicalize(directory).map_err(io)?;
    let named_lock =
        open_named(&directory.join(crate::system::data_directory::DATABASE_LOCK_FILE))?;
    if !same_file(
        &named_lock.metadata().map_err(io)?,
        &exclusive_lock.metadata().map_err(io)?,
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
        Err(std::fs::TryLockError::Error(error)) => return Err(io(error)),
    }
    let named_database = open_named(&directory.join(DATABASE_FILE_NAME))?;
    let named_before = named_database.metadata().map_err(io)?;
    let held_before = database_file.metadata().map_err(io)?;
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

    let scratch = std::env::temp_dir().join(format!(".ocg-publish-{}", uuid::Uuid::now_v7()));
    let mut create = std::fs::OpenOptions::new();
    create.read(true).write(true).create_new(true);
    let mut copy = crate::system::data_directory::private_file(&mut create)
        .open(&scratch)
        .map_err(io)?;
    std::fs::remove_file(&scratch).map_err(io)?;
    let mut original = database_file.try_clone().map_err(io)?;
    original.seek(SeekFrom::Start(0)).map_err(io)?;
    let copied = std::io::copy(&mut original, &mut copy).map_err(io)?;
    if copied != held_before.len() || copied == 0 {
        return Err(StorageError::corrupted(
            "the published database changed while it was copied",
        ));
    }

    let guarded = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut database = Database::builder().create_file(copy)?;
        let logical = if recovered {
            logical_report(recovery::verify_recovered(&database))
        } else {
            match recovery::verify_reserved_residual(&database) {
                Ok(residual) => serde_json::json!({
                    "passed": true,
                    "held_allocations": residual.ownership.held.len(),
                    "retained_unknown_outcomes": residual.ownership.retained_unknown.len(),
                    "resolved_unknown_outcomes": residual.ownership.resolved_unknown.len(),
                    "normal_startup": if residual.normal_startup_accepts { "accepted" } else { "refused_residual_held_ownership" },
                }),
                Err(error) => serde_json::json!({"passed": false, "detail": error.detail}),
            }
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
        .map_err(io)?;
    let held_after = database_file.metadata().map_err(io)?;
    if !same_file(&named_before, &named_after)
        || !same_file(&held_before, &held_after)
        || named_after.len() != named_before.len()
        || named_after.modified().map_err(io)? != named_before.modified().map_err(io)?
        || held_after.len() != held_before.len()
        || held_after.modified().map_err(io)? != held_before.modified().map_err(io)?
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
        Err(std::fs::TryLockError::Error(error)) => return Err(io(error)),
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
        let allocation = transactions::decode_pool_allocation(key.value(), value.value())?;
        total += 1;
        if allocation.holds_capacity() {
            held_total += 1;
            if held.len() < MAX_INSPECTED_HELD {
                held.push(allocation);
            }
        }
    }

    let inspector_host = recovery::current_host_id();
    let inspector_boot = recovery::current_boot_id();
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
                RequestPayload::ApprovedCalls => ("approved_calls", json!(null)),
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
        if boot_matches == Some(false) {
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
        tx.open_table(CONFIGS),
        tx.open_table(ACTIVE_ALLOCATIONS),
        tx.open_table(ALLOCATION_REQUESTS),
    ) {
        (Ok(configs), Ok(active), Ok(requests)) => {
            match indexes::allocation_indexes_agree(&allocations, &configs, &active, &requests) {
                Ok(true) => "consistent".to_owned(),
                Ok(false) => "the stored indexes differ from their source rows".to_owned(),
                Err(error) => format!("inconsistent: {error}"),
            }
        }
        _ => "an index table is missing".to_owned(),
    };
    let controls: Vec<_> = crate::system::execution_controls::ExecutionControl::ALL
        .iter()
        .map(|control| json!({"control": control.name(), "state": text(&control.state())}))
        .collect();
    Ok(json!({
        "database": {
            "format": DATABASE_FORMAT_VERSION,
            "schema_fingerprint": String::from_utf8_lossy(SCHEMA_FINGERPRINT),
            "open_epoch": open_epoch,
            "clean_shutdown": clean_shutdown,
            "access": access,
        },
        "allocations_total": total,
        "held_total": held_total,
        "held_reported": reports.len(),
        "held": reports,
        "allocation_indexes": allocation_indexes,
        "execution_controls": controls,
        "note": "Read-only. Nothing was adopted, released, cancelled, retried or finalized.",
    }))
}

fn read_job_row(rx: &ReadTransaction, id: JobId) -> Result<Option<JobSpec>, StorageError> {
    rx.open_table(JOBS)?
        .get(id.get())?
        .map(|bytes| JobSpec::decode_at(id.get(), bytes.value()))
        .transpose()
}

fn read_job_control(rx: &ReadTransaction, id: JobId) -> Result<JobControl, StorageError> {
    rx.open_table(JOB_CONTROLS)?
        .get(id.get())?
        .map(|bytes| JobControl::decode(bytes.value()))
        .transpose()?
        .ok_or_else(|| StorageError::corrupted(format!("{id} has no conversation row")))
}

fn read_state(rx: &ReadTransaction, id: JobId) -> Result<JobState, StorageError> {
    rx.open_table(JOB_STATES)?
        .get(id.get())?
        .map(|bytes| JobState::decode_at(id.get(), bytes.value()))
        .transpose()?
        .ok_or_else(|| StorageError::corrupted(format!("{id} has no job_state")))
}

pub(super) fn turn_head(
    turns: &impl ReadableTable<(u128, u128), ()>,
    group: JobId,
) -> Result<Option<JobId>, StorageError> {
    Ok(turns
        .range((group.get(), 0)..=(group.get(), u128::MAX))?
        .next_back()
        .transpose()?
        .map(|(key, _)| JobId::new(key.value().1)))
}

fn turn_first(
    turns: &impl ReadableTable<(u128, u128), ()>,
    group: JobId,
) -> Result<Option<JobId>, StorageError> {
    Ok(turns
        .range((group.get(), 0)..=(group.get(), u128::MAX))?
        .next()
        .transpose()?
        .map(|(key, _)| JobId::new(key.value().1)))
}

pub(super) fn job_execution(
    jobs: &impl ReadableTable<u128, &'static [u8]>,
    row: &JobSpec,
) -> Result<(FrozenExecutionConfig, Option<ProjectId>, Option<ConfigId>), StorageError> {
    let JobKind::Execution {
        execution,
        plan_source,
        ..
    } = &row.kind
    else {
        return Err(StorageError::corrupted("expected a turn"));
    };
    let execution = match execution {
        JobExecution::Frozen(execution) => execution.clone(),
        JobExecution::Summarization(work) => work.frozen.clone(),
        JobExecution::Decision(_) => {
            return Err(StorageError::corrupted(
                "decision work cannot use the generative context reader",
            ));
        }
        JobExecution::Ref(owner) => {
            let bytes = jobs
                .get(owner.get())?
                .ok_or_else(|| StorageError::corrupted("missing execution owner"))?;
            let owner = JobSpec::decode_at(owner.get(), bytes.value())?;
            match owner.kind {
                JobKind::Execution {
                    execution: JobExecution::Frozen(execution),
                    ..
                } if owner.parent_id == row.parent_id => execution,
                JobKind::Group { .. } | JobKind::Execution { .. } => {
                    return Err(StorageError::corrupted(
                        "execution reference must name a Frozen turn in this conversation",
                    ));
                }
            }
        }
    };
    let (project, agent) = match plan_source {
        PlanSource::Process { project, .. } => {
            (*project, execution.agent_ref.map(|agent| agent.id))
        }
        PlanSource::Stored { .. } => {
            let conversation = row
                .parent_id
                .ok_or_else(|| StorageError::corrupted("stored turn has no conversation"))?;
            let bytes = jobs
                .get(conversation.get())?
                .ok_or_else(|| StorageError::corrupted("missing conversation"))?;
            match JobSpec::decode_at(conversation.get(), bytes.value())?.kind {
                JobKind::Group { project, agent, .. } => (project, agent),
                JobKind::Execution { .. } => {
                    return Err(StorageError::corrupted(
                        "stored turn parent must be a conversation",
                    ));
                }
            }
        }
    };
    Ok((execution, project, agent))
}

pub(super) fn job_record(rx: &ReadTransaction, row: JobSpec) -> Result<JobRecord, StorageError> {
    let id = row.id;
    let state = read_state(rx, id)?;
    let at = |kind: OutcomeKind| {
        state
            .outcome
            .as_ref()
            .filter(|outcome| outcome.kind == kind)
            .map(|outcome| outcome.at)
    };
    let subtree_settled = rx.open_table(UNSETTLED)?.get(id.get())?.is_none();
    let execution = turn_config(rx, &row)?;
    let configuration = execution.as_ref().map(|execution| ExecutionConfiguration {
        reasoning_effort: execution
            .llm
            .config
            .parameters
            .reasoning_effort
            .as_deref()
            .and_then(crate::db::entities::ReasoningEffort::parse),
        agent_profile: execution.agent_ref.map(|agent| agent.id.into()),
        llm_profile: execution.llm.model.id.into(),
    });
    let updated_at = [
        Some(row.created_at),
        state.outcome.as_ref().map(|outcome| outcome.at),
        state.stop_confirmed_at,
    ]
    .into_iter()
    .flatten()
    .max()
    .unwrap_or(row.created_at);
    let children_closed_at = if row.kind.is_group() {
        read_job_control(rx, id)?.closing.map(|closing| closing.at)
    } else {
        state.outcome.as_ref().map(|outcome| outcome.at)
    };
    Ok(JobRecord {
        id,
        name: row.kind.name().map(str::to_owned),
        started_at: None,
        completed_at: at(OutcomeKind::Completed),
        failed_at: at(OutcomeKind::Failed),
        cancelled_at: at(OutcomeKind::Cancelled),
        unknown_at: at(OutcomeKind::Unknown),
        stop_confirmed_at: state.stop_confirmed_at,
        stop_confirmed_by: state.stop_confirmed_by,
        created_at: row.created_at,
        updated_at,
        input: JobInputId::new(id.get()),
        parent: row.parent_id.unwrap_or(JobId::new(0)),
        parent_id: row.parent_id,
        group: row.kind.is_persistent().then_some(row.parent_id).flatten(),
        objective: row.kind.objective().map(str::to_owned),
        configuration,
        assigned_executor: None,
        previous: row.kind.previous(),
        outcome: state.outcome,
        children_closed_at,
        kind: row.kind,
        subtree_settled,
    })
}

pub(crate) fn read_job(database: &Database, id: JobId) -> Result<Option<JobRecord>, StorageError> {
    let rx = read(database)?;
    read_job_row(&rx, id)?
        .map(|row| job_record(&rx, row))
        .transpose()
}

pub(super) fn mutable_job_record_in(
    rx: &ReadTransaction,
    id: JobId,
) -> Result<Option<crate::db::entities::MutableJobRecord>, StorageError> {
    let jobs = rx.open_table(JOBS)?;
    let Some(bytes) = jobs.get(id.get())? else {
        return Ok(None);
    };
    if bytes.value().first() == Some(&JobSpec::MUTABLE_CONVERSATION_TAG) {
        JobSpec::decode_at(id.get(), bytes.value())?;
        return Ok(None);
    }
    let job = transactions::decode_mutable_job(id.get(), bytes.value())?;
    let states = rx.open_table(JOB_STATES)?;
    let state = states
        .get(id.get())?
        .ok_or_else(|| StorageError::corrupted("mutable Job state is missing"))?;
    if !matches!(state.value(), [TAG_JOB_STATE, codec::RECORD_VERSION, ..]) {
        return Err(StorageError::corrupted("invalid mutable Job state version"));
    }
    let state = JobState::decode_at(id.get(), state.value())?;
    if state.stop_confirmed_at.is_some() || state.stop_confirmed_by.is_some() {
        return Err(StorageError::corrupted(
            "mutable Job has legacy ownership evidence",
        ));
    }
    let parent = read_job_row(rx, job.parent_id)?
        .ok_or_else(|| StorageError::corrupted("mutable Job conversation missing"))?;
    let JobKind::Group { project, .. } = parent.kind else {
        return Err(StorageError::corrupted(
            "mutable Job parent is not a conversation",
        ));
    };
    Ok(Some(crate::db::entities::MutableJobRecord {
        job,
        outcome: state.outcome,
        project,
    }))
}

#[allow(dead_code)]
pub(crate) struct DevelopmentExecutionContext {
    pub allocation: crate::db::entities::PoolAllocation,
    pub job: crate::db::entities::MutableJob,
    pub pool: crate::db::entities::AllocationPool,
    pub executor: crate::db::entities::AllocationExecutor,
    pub configuration: FrozenExecutionConfig,
    pub conversation: JobSpec,
    pub conversation_control: JobControl,
    pub plan_step: bool,
    pub project: Option<ProjectRecord>,
    pub compaction: crate::db::entities::GlobalCompactionSettings,
    pub attachments: Vec<AttachmentRecord>,
    pub adopted_summary: Option<VerifiedNativeSummary>,
    pub covered_inputs: Vec<DevelopmentRetainedInput>,
    pub history: Vec<DevelopmentContextTurn>,
    pub estimated_tokens: u64,
}

impl DevelopmentExecutionContext {
    pub(crate) fn is_summary_work(&self) -> bool {
        self.job.summary_source.is_some()
            || matches!(
                self.conversation.kind,
                JobKind::Group {
                    origin: Origin::System,
                    ..
                }
            )
    }
}

#[allow(dead_code)]
pub(crate) struct DevelopmentDecisionContext {
    pub allocation: crate::db::entities::PoolAllocation,
    pub job: crate::db::entities::MutableJob,
    pub pool: crate::db::entities::AllocationPool,
    pub executor: crate::db::entities::AllocationExecutor,
    pub configuration: crate::db::entities::FrozenDecisionConfig,
    pub conversation: JobSpec,
    pub conversation_control: JobControl,
    pub project: Option<ProjectRecord>,
}

#[allow(dead_code, clippy::large_enum_variant)]
pub(crate) enum DevelopmentCandidate {
    Generative(DevelopmentExecutionContext),
    Decision(DevelopmentDecisionContext),
}

#[allow(dead_code)]
pub(crate) struct DevelopmentRetainedInput {
    pub job: JobId,
    pub text: String,
    pub attachments: Vec<AttachmentRecord>,
}

#[allow(dead_code)]
pub(crate) struct DevelopmentResolvedContext {
    pub snapshot: DevelopmentExecutionContext,
    pub images: Vec<(
        AttachmentId,
        crate::service::ai::providers::openai_compatible::ImageInput,
    )>,
}

#[allow(dead_code, clippy::large_enum_variant)]
pub(crate) enum DevelopmentResolvedCandidate {
    Generative(DevelopmentResolvedContext),
    Decision(DevelopmentDecisionContext),
}

#[allow(dead_code)]
pub(crate) struct DevelopmentContextTurn {
    pub record: crate::db::entities::MutableJobRecord,
    pub attachments: Vec<AttachmentRecord>,
    pub outputs: Vec<JobOutputRecord>,
}

fn context_unavailable(detail: &str) -> StorageError {
    StorageError {
        kind: super::StorageErrorKind::Other,
        os_error: None,
        detail: detail.to_owned(),
    }
}

fn context_attachments(
    rx: &ReadTransaction,
    ids: &[AttachmentId],
) -> Result<Vec<AttachmentRecord>, StorageError> {
    let table = rx.open_table(ATTACHMENTS)?;
    ids.iter()
        .map(|id| {
            let bytes = table
                .get(id.get())?
                .ok_or_else(|| StorageError::corrupted("context Attachment missing"))?;
            let attachment = decode_attachment(id.get(), bytes.value())?;
            if !attachment.is_available() {
                return Err(context_unavailable("context Attachment is unavailable"));
            }
            Ok(attachment)
        })
        .collect()
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct VerifiedNativeSummary {
    pub manifest: crate::db::entities::NativeSummaryManifest,
    pub summary: crate::db::entities::SummarizationResult,
}

const SUMMARY_MAX_OUTPUTS: usize = 100_000;
const SUMMARY_MAX_BYTES: usize = 128 * 1024 * 1024;
const SUMMARY_MAX_RECORD_BYTES: usize = 8 * 1024 * 1024;

pub(super) struct SummaryBudget {
    pub(super) bytes: usize,
    pub(super) outputs: usize,
}

impl SummaryBudget {
    fn bytes(&mut self, n: usize) -> Result<(), StorageError> {
        if n > SUMMARY_MAX_RECORD_BYTES {
            return Err(context_unavailable(
                "summary verification record exceeds byte limit",
            ));
        }
        self.bytes = self
            .bytes
            .checked_add(n)
            .ok_or_else(|| context_unavailable("summary verification byte budget exceeded"))?;
        if self.bytes > SUMMARY_MAX_BYTES {
            return Err(context_unavailable(
                "summary verification byte budget exceeded",
            ));
        }
        Ok(())
    }

    fn output(&mut self) -> Result<(), StorageError> {
        self.outputs += 1;
        if self.outputs > SUMMARY_MAX_OUTPUTS {
            return Err(context_unavailable(
                "summary verification output budget exceeded",
            ));
        }
        Ok(())
    }
}

fn native_summary_frame(
    hash: &mut aws_lc_rs::digest::Context,
    budget: &mut SummaryBudget,
    bytes: &[u8],
) -> Result<(), StorageError> {
    budget.bytes(bytes.len())?;
    hash.update(&(bytes.len() as u64).to_le_bytes());
    hash.update(bytes);
    Ok(())
}

fn native_summary_owner(
    configs: &impl ReadableTable<u128, &'static [u8]>,
    allocation: &crate::db::entities::PoolAllocation,
    budget: &mut SummaryBudget,
) -> Result<(), StorageError> {
    let pool = configs
        .get(allocation.token.pool.get())?
        .ok_or_else(|| StorageError::corrupted("summary allocation Pool missing"))?;
    let executor = configs
        .get(allocation.token.executor.get())?
        .ok_or_else(|| StorageError::corrupted("summary allocation Executor missing"))?;
    budget.bytes(pool.value().len())?;
    budget.bytes(executor.value().len())?;
    let pool = transactions::decode_allocation_pool(allocation.token.pool.get(), pool.value())?;
    let executor = transactions::decode_allocation_executor(
        allocation.token.executor.get(),
        executor.value(),
    )?;
    if executor.pool != pool.id
        || pool.revision < allocation.pool_revision
        || executor.revision < allocation.executor_revision
        || !executor
            .capabilities
            .contains(&(allocation.operation, allocation.protocol))
    {
        return Err(context_unavailable(
            "summary allocation ownership is inconsistent",
        ));
    }
    Ok(())
}

pub(super) struct NativeSummaryTables<J, S, A, O, AT, T, C> {
    pub jobs: J,
    pub states: S,
    pub allocations: A,
    pub outputs: O,
    pub attachments: AT,
    pub tools: T,
    pub configs: C,
}

pub(super) fn native_source_boundary<J, S, A, O, AT, T, C>(
    tables: &NativeSummaryTables<J, S, A, O, AT, T, C>,
    id: JobId,
    conversation: JobId,
    budget: &mut SummaryBudget,
) -> Result<(crate::db::entities::NativeSummaryBoundary, Option<JobId>), StorageError>
where
    J: ReadableTable<u128, &'static [u8]>,
    S: ReadableTable<u128, &'static [u8]>,
    A: ReadableTable<u128, &'static [u8]>,
    O: ReadableTable<(u128, u32), &'static [u8]>,
    AT: ReadableTable<u128, &'static [u8]>,
    T: ReadableTable<(u128, u32, u32), &'static [u8]>,
    C: ReadableTable<u128, &'static [u8]>,
{
    use crate::db::entities::MutableJobPhase;
    use aws_lc_rs::digest::{Context, SHA256};
    let jobs = &tables.jobs;
    let states = &tables.states;
    let allocations = &tables.allocations;
    let attachments = &tables.attachments;
    let outputs = &tables.outputs;
    let tools = &tables.tools;
    let job_bytes = jobs
        .get(id.get())?
        .ok_or_else(|| StorageError::corrupted("summary source Job missing"))?;
    let state_bytes = states
        .get(id.get())?
        .ok_or_else(|| StorageError::corrupted("summary source state missing"))?;
    let allocation_bytes = allocations
        .get(id.get())?
        .ok_or_else(|| StorageError::corrupted("summary source allocation missing"))?;
    budget.bytes(job_bytes.value().len())?;
    budget.bytes(state_bytes.value().len())?;
    budget.bytes(allocation_bytes.value().len())?;
    let job = transactions::decode_mutable_job(id.get(), job_bytes.value())?;
    if matches!(
        job.input.content,
        crate::db::entities::MutableJobContent::Decision { .. }
    ) {
        return Err(context_unavailable(
            "Decision Jobs cannot be summary sources",
        ));
    }
    let state = JobState::decode_at(id.get(), state_bytes.value())?;
    let allocation = transactions::decode_pool_allocation(id.get(), allocation_bytes.value())?;
    if job.parent_id != conversation
        || job.phase != MutableJobPhase::Executing
        || job.summary_source.is_some()
        || job.context_selection != crate::db::entities::MutableContextSelection::FullHistory
        || state.stop_confirmed_at.is_some()
        || state.stop_confirmed_by.is_some()
        || !state.outcome.as_ref().is_some_and(|o| {
            matches!(
                o.kind,
                OutcomeKind::Completed | OutcomeKind::Failed | OutcomeKind::Cancelled
            )
        })
        || allocation.phase != AllocationPhase::Released
        || allocation.stop_confirmed_at.is_some()
        || allocation.stop_proof.is_some()
        || allocation.job_revision != job.revision
        || allocation.operation != ExecutionOperation::LlmRequest
        || !job
            .frozen
            .as_ref()
            .is_some_and(|frozen| matches!(frozen, crate::db::entities::MutableFrozenConfiguration::Generative(frozen) if allocation.protocol == Some(frozen.llm.provider.transport)))
        || !job
            .submitted_at
            .is_some_and(|at| at <= allocation.claimed_at)
        || !state
            .outcome
            .as_ref()
            .is_some_and(|outcome| outcome.at >= allocation.claimed_at)
    {
        return Err(context_unavailable(
            "summary source is not a settled conversation Job",
        ));
    }
    native_summary_owner(&tables.configs, &allocation, budget)?;
    if tools
        .range((id.get(), 0, 0)..=(id.get(), u32::MAX, u32::MAX))?
        .next()
        .transpose()?
        .is_some()
    {
        return Err(context_unavailable(
            "summary source tool evidence is not supported",
        ));
    }
    let mut hash = Context::new(&SHA256);
    hash.update(b"ocg-native-summary-source-v1");
    native_summary_frame(&mut hash, budget, job_bytes.value())?;
    native_summary_frame(&mut hash, budget, state_bytes.value())?;
    native_summary_frame(&mut hash, budget, allocation_bytes.value())?;
    for id in job.input.attachments() {
        let bytes = attachments
            .get(id.get())?
            .ok_or_else(|| StorageError::corrupted("summary source attachment missing"))?;
        native_summary_frame(&mut hash, budget, bytes.value())?;
        let attachment = decode_attachment(id.get(), bytes.value())?;
        if !attachment.is_available() {
            return Err(context_unavailable("summary source attachment unavailable"));
        }
    }
    let mut through = 0u32;
    let mut final_result = false;
    for entry in outputs.range((id.get(), 0)..=(id.get(), u32::MAX))? {
        let (key, bytes) = entry?;
        budget.output()?;
        let seq = key.value().1;
        if seq
            != through
                .checked_add(1)
                .ok_or_else(|| context_unavailable("summary source output overflow"))?
        {
            return Err(StorageError::corrupted(
                "summary source output sequence has a gap",
            ));
        }
        native_summary_frame(&mut hash, budget, bytes.value())?;
        let output = decode_output(key.value(), bytes.value())?;
        if output.created_at < allocation.claimed_at
            || state
                .outcome
                .as_ref()
                .is_none_or(|outcome| output.created_at > outcome.at)
        {
            return Err(context_unavailable(
                "summary source output timing is inconsistent",
            ));
        }
        if final_result {
            return Err(StorageError::corrupted(
                "summary source output follows its final result",
            ));
        }
        final_result = output.kind == OutputKind::Result;
        through = seq;
    }
    if through == 0 || !final_result {
        return Err(StorageError::corrupted(
            "summary source final output missing",
        ));
    }
    let digest: [u8; 32] = hash
        .finish()
        .as_ref()
        .try_into()
        .map_err(|_| StorageError::corrupted("invalid source digest"))?;
    Ok((
        crate::db::entities::NativeSummaryBoundary {
            job: id,
            revision: job.revision,
            output_through: through,
            content: ContentDigest::from_bytes(digest),
        },
        job.previous,
    ))
}

pub(super) fn verify_native_summary_in(
    rx: &ReadTransaction,
    conversation: JobId,
    consumer: JobId,
    producer: JobId,
    source: ContentDigest,
) -> Result<VerifiedNativeSummary, StorageError> {
    verify_native_summary_tables(
        &rx.open_table(JOBS)?,
        &rx.open_table(JOB_STATES)?,
        &rx.open_table(JOB_OUTPUTS)?,
        &rx.open_table(MESSAGES)?,
        conversation,
        consumer,
        producer,
        source,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn verify_native_summary_tables<J, S, O, M>(
    jobs: &J,
    states: &S,
    outputs: &O,
    messages: &M,
    conversation: JobId,
    consumer: JobId,
    producer: JobId,
    source: ContentDigest,
) -> Result<VerifiedNativeSummary, StorageError>
where
    J: ReadableTable<u128, &'static [u8]>,
    S: ReadableTable<u128, &'static [u8]>,
    O: ReadableTable<(u128, u32), &'static [u8]>,
    M: ReadableTable<&'static [u8], &'static [u8]>,
{
    if consumer == producer {
        return Err(context_unavailable(
            "summary consumer cannot be its own producer",
        ));
    }
    let producer_bytes = jobs
        .get(producer.get())?
        .ok_or_else(|| StorageError::corrupted("summary producer missing"))?;
    let producer_job = transactions::decode_mutable_job(producer.get(), producer_bytes.value())?;
    let state = JobState::decode_at(
        producer.get(),
        states
            .get(producer.get())?
            .ok_or_else(|| StorageError::corrupted("summary producer state missing"))?
            .value(),
    )?;
    if producer_job.parent_id != conversation
        || producer_job.summary_source != Some(source)
        || !state
            .outcome
            .as_ref()
            .is_some_and(|o| o.kind == OutcomeKind::Completed)
    {
        return Err(context_unavailable(
            "summary producer is not authorized and completed",
        ));
    }
    let manifest_bytes = messages
        .get(source.as_bytes().as_slice())?
        .ok_or_else(|| StorageError::corrupted("summary manifest missing"))?;
    let manifest: crate::db::entities::NativeSummaryManifest =
        serde_json::from_slice(manifest_bytes.value())
            .map_err(|_| StorageError::corrupted("malformed summary manifest"))?;
    if manifest.conversation != conversation {
        return Err(context_unavailable(
            "summary manifest has an unsupported scope",
        ));
    }
    let output = outputs
        .range((producer.get(), 0)..=(producer.get(), u32::MAX))?
        .next_back()
        .transpose()?
        .map(|(k, b)| decode_output(k.value(), b.value()))
        .transpose()?
        .filter(|o| o.kind == OutputKind::Result)
        .ok_or_else(|| StorageError::corrupted("summary producer result missing"))?;
    let link: crate::db::entities::NativeSummaryResultLink = serde_json::from_value(
        output
            .value
            .ok_or_else(|| StorageError::corrupted("summary result link missing"))?,
    )
    .map_err(|_| StorageError::corrupted("malformed summary result link"))?;
    if link.source != source {
        return Err(context_unavailable("summary result source changed"));
    }
    let payload = messages
        .get(link.payload.as_bytes().as_slice())?
        .ok_or_else(|| StorageError::corrupted("summary payload missing"))?;
    let summary: crate::db::entities::SummarizationResult = serde_json::from_slice(payload.value())
        .map_err(|_| StorageError::corrupted("malformed summary payload"))?;
    summary.validate().map_err(StorageError::corrupted)?;
    if summary.source != source {
        return Err(context_unavailable("summary payload names another source"));
    }
    Ok(VerifiedNativeSummary { manifest, summary })
}

pub(crate) fn read_trial_job(
    database: &Database,
    job: JobId,
) -> Result<
    Option<(
        crate::db::entities::MutableJobRecord,
        Option<crate::db::entities::PoolAllocation>,
    )>,
    StorageError,
> {
    let rx = read(database)?;
    let Some(record) = mutable_job_record_in(&rx, job)? else {
        return Ok(None);
    };
    let allocations = rx.open_table(POOL_ALLOCATIONS)?;
    let allocation = match allocations.get(job.get())? {
        Some(row) => Some(transactions::decode_pool_allocation(
            job.get(),
            row.value(),
        )?),
        None => None,
    };
    Ok(Some((record, allocation)))
}

fn native_epoch(
    meta: &impl ReadableTable<&'static str, &'static [u8]>,
) -> Result<u64, StorageError> {
    let epoch = meta
        .get("open_epoch")?
        .ok_or_else(|| StorageError::corrupted("database epoch missing"))?;
    let bytes: [u8; 8] = epoch
        .value()
        .try_into()
        .map_err(|_| StorageError::corrupted("invalid database epoch"))?;
    Ok(u64::from_le_bytes(bytes))
}

pub(crate) fn read_development_execution_context(
    database: &Database,
    token: crate::db::entities::AllocationToken,
) -> Result<DevelopmentCandidate, StorageError> {
    use crate::db::entities::AllocationPhase;
    let rx = read(database)?;
    let meta = rx.open_table(META)?;
    if native_epoch(&meta)? != token.epoch {
        return Err(context_unavailable(
            "Allocation belongs to another database epoch",
        ));
    }
    let allocation = rx
        .open_table(POOL_ALLOCATIONS)?
        .get(token.job.get())?
        .map(|v| transactions::decode_pool_allocation(token.job.get(), v.value()))
        .transpose()?
        .ok_or_else(|| context_unavailable("Allocation is missing"))?;
    if allocation.token != token
        || allocation.phase != AllocationPhase::Reserved
        || allocation.stop_confirmed_at.is_some()
        || allocation.stop_proof.is_some()
    {
        return Err(context_unavailable(
            "Allocation ownership is not a current reservation",
        ));
    }
    development_candidate_in(&rx, &meta, allocation)
}

fn development_candidate_in(
    rx: &ReadTransaction,
    meta: &impl ReadableTable<&'static str, &'static [u8]>,
    allocation: crate::db::entities::PoolAllocation,
) -> Result<DevelopmentCandidate, StorageError> {
    use crate::db::entities::MutableJobPhase;
    let token = allocation.token;
    let record = mutable_job_record_in(rx, token.job)?
        .ok_or_else(|| context_unavailable("Job is missing"))?;
    if record.job.phase != MutableJobPhase::Queued
        || record.outcome.is_some()
        || record.job.revision != allocation.job_revision
        || record.job.frozen.is_none()
    {
        return Err(context_unavailable("Job is not the reserved revision"));
    }
    let adopted_summary = match record.job.context_selection {
        crate::db::entities::MutableContextSelection::FullHistory => None,
        crate::db::entities::MutableContextSelection::AdoptedSummary { producer, source } => Some(
            verify_native_summary_in(rx, record.job.parent_id, record.job.id, producer, source)?,
        ),
    };
    let configs = rx.open_table(CONFIGS)?;
    let pool = configs
        .get(token.pool.get())?
        .map(|v| transactions::decode_allocation_pool(token.pool.get(), v.value()))
        .transpose()?
        .ok_or_else(|| context_unavailable("Pool is missing"))?;
    let executor = configs
        .get(token.executor.get())?
        .map(|v| transactions::decode_allocation_executor(token.executor.get(), v.value()))
        .transpose()?
        .ok_or_else(|| context_unavailable("Executor is missing"))?;
    if !pool.enabled
        || pool.archived
        || !executor.enabled
        || executor.archived
        || executor.pool != pool.id
        || !executor
            .capabilities
            .contains(&(allocation.operation, allocation.protocol))
    {
        return Err(context_unavailable(
            "Pool or Executor configuration changed",
        ));
    }
    let conversation = read_job_row(rx, record.job.parent_id)?
        .ok_or_else(|| StorageError::corrupted("conversation missing"))?;
    let conversation_control = read_job_control(rx, conversation.id)?;
    if conversation_control.on_hold.is_some() || conversation_control.closing.is_some() {
        return Err(context_unavailable("conversation is not eligible"));
    }
    let project = record
        .project
        .map(|id| {
            rx.open_table(PROJECTS)?
                .get(id.get())?
                .map(|v| decode_project(id.get(), v.value()))
                .transpose()?
                .ok_or_else(|| StorageError::corrupted("conversation Project missing"))
        })
        .transpose()?;
    if matches!(
        &record.job.input.content,
        crate::db::entities::MutableJobContent::Decision { .. }
    ) {
        let configuration = match record.job.frozen.clone() {
            Some(crate::db::entities::MutableFrozenConfiguration::Decision(configuration)) => {
                crate::db::entities::FrozenDecisionConfig::Native(configuration)
            }
            Some(crate::db::entities::MutableFrozenConfiguration::Generative(configuration)) => {
                crate::db::entities::FrozenDecisionConfig::Generative(configuration, None)
            }
            None => return Err(context_unavailable("Decision configuration is missing")),
        };
        return Ok(DevelopmentCandidate::Decision(DevelopmentDecisionContext {
            allocation,
            job: record.job,
            pool,
            executor,
            configuration,
            conversation,
            conversation_control,
            project,
        }));
    }
    let Some(crate::db::entities::MutableFrozenConfiguration::Generative(configuration)) =
        record.job.frozen.clone()
    else {
        return Err(context_unavailable("Job operation is unsupported"));
    };
    let plan_step = match &conversation.kind {
        crate::db::entities::JobKind::Group {
            origin: crate::db::entities::Origin::Agent { call },
            ..
        } => tool_call_name(rx, *call)?.as_deref() == Some("job_plan"),
        _ => false,
    };
    let attachments = context_attachments(rx, record.job.input.attachments())?;
    let compaction = global_compaction_settings(meta)?;
    let covered: BTreeSet<_> = adopted_summary
        .as_ref()
        .map_or_else(BTreeSet::new, |summary| {
            summary
                .manifest
                .sources
                .iter()
                .map(|source| source.job)
                .collect()
        });
    let mut covered_inputs = Vec::with_capacity(covered.len());
    if let Some(source_digest) = record.job.summary_source {
        let messages = rx.open_table(MESSAGES)?;
        let manifest_bytes = messages
            .get(source_digest.as_bytes().as_slice())?
            .ok_or_else(|| StorageError::corrupted("summary manifest missing"))?;
        let manifest: crate::db::entities::NativeSummaryManifest =
            serde_json::from_slice(manifest_bytes.value())
                .map_err(|_| StorageError::corrupted("malformed summary manifest"))?;
        manifest.validate().map_err(StorageError::corrupted)?;
        let jobs_table = rx.open_table(JOBS)?;
        let outputs_table = rx.open_table(JOB_OUTPUTS)?;
        let is_merge = record.job.input.text().starts_with("Merge ");
        if !is_merge {
            for boundary in &manifest.sources {
                let job_bytes = jobs_table
                    .get(boundary.job.get())?
                    .ok_or_else(|| StorageError::corrupted("summary source Job missing"))?;
                let src_job =
                    transactions::decode_mutable_job(boundary.job.get(), job_bytes.value())?;
                let mut turn_text = format!("User: {}\n", src_job.input.text());
                for entry in
                    outputs_table.range((boundary.job.get(), 0)..=(boundary.job.get(), u32::MAX))?
                {
                    let (key, val) = entry?;
                    let out = decode_output(key.value(), val.value())?;
                    if let Some(t) = out.display_text() {
                        turn_text.push_str("Assistant: ");
                        turn_text.push_str(&t);
                        turn_text.push('\n');
                    }
                }
                covered_inputs.push(DevelopmentRetainedInput {
                    job: boundary.job,
                    text: turn_text,
                    attachments: Vec::new(),
                });
            }
        }
        if is_merge {
            let child_groups = rx.open_table(CHILD_GROUPS)?;
            let children = rx.open_table(CHILDREN)?;
            let waiters = rx.open_table(DEPENDENCY_WAITERS)?;
            for cg in child_groups
                .range((conversation.id.get(), 0)..=(conversation.id.get(), u128::MAX))?
            {
                let (cg_key, _) = cg?;
                let (_, cg_id) = cg_key.value();
                for ch in children.range((cg_id, 0)..=(cg_id, u128::MAX))? {
                    let (ch_key, _) = ch?;
                    let (_, chunk_id) = ch_key.value();
                    if waiters.get((chunk_id, record.job.id.get()))?.is_some() {
                        let mut chunk_text = String::new();
                        for entry in outputs_table.range((chunk_id, 0)..=(chunk_id, u32::MAX))? {
                            let (key, val) = entry?;
                            let out = decode_output(key.value(), val.value())?;
                            if out.kind == OutputKind::Result
                                && let Some(t) = out.display_text()
                            {
                                chunk_text.push_str(&t);
                            }
                        }
                        if !chunk_text.is_empty() {
                            covered_inputs.push(DevelopmentRetainedInput {
                                job: JobId::new(chunk_id),
                                text: chunk_text,
                                attachments: Vec::new(),
                            });
                        }
                    }
                }
            }
            if covered_inputs.is_empty() {
                return Err(context_unavailable("chunk summaries missing for merge"));
            }
        }
    }
    let mut history = Vec::new();
    let mut previous = record.job.previous;
    let outputs = rx.open_table(JOB_OUTPUTS)?;
    let mut output_count = 0usize;
    let mut output_bytes = 0usize;
    let mut selected_bytes = record.job.input.text().len().saturating_add(
        adopted_summary
            .as_ref()
            .map_or(0, |summary| {
                summary.summary.context_text().len().saturating_add(
                    "Generated context summary (advisory, not an original user instruction). Originals remain available:\n".len(),
                )
            }),
    );
    selected_bytes = selected_bytes
        .saturating_add(crate::service::ai::generative_model::CHART_INSTRUCTIONS.len())
        .saturating_add(
            configuration
                .agent
                .as_ref()
                .and_then(|agent| agent.instructions.as_ref())
                .map_or(0, String::len),
        );
    if let Some(project) = &project {
        let commands: Vec<_> = crate::db::entities::BUILT_IN_COMMANDS
            .into_iter()
            .chain(
                project
                    .commands
                    .iter()
                    .filter(|command| command.status == crate::db::entities::CommandStatus::Allowed)
                    .map(|command| command.name.as_str()),
            )
            .collect();
        let grant = configuration
            .agent
            .as_ref()
            .and_then(|agent| agent.workspace_grant());
        let grounding = crate::service::ai::generative_model::ground_project(
            &project.name,
            &project.root,
            grant,
            &commands,
        );
        selected_bytes = selected_bytes.saturating_add(
            crate::db::serialized_len(&grounding)
                .map_err(|_| StorageError::corrupted("invalid project context"))?,
        );
    }
    let mut image_count = attachments.len();
    while let Some(id) = previous {
        let is_covered = covered.contains(&id);
        if !is_covered && history.len() == 128 {
            return Err(context_unavailable(
                "conversation history exceeds context limit",
            ));
        }
        let prior = mutable_job_record_in(rx, id)?
            .ok_or_else(|| StorageError::corrupted("predecessor missing"))?;
        if matches!(
            prior.job.input.content,
            crate::db::entities::MutableJobContent::Decision { .. }
        ) {
            return Err(context_unavailable(
                "Decision predecessors cannot be projected as generative messages",
            ));
        }
        let ended_unstarted = prior.job.parent_id == record.job.parent_id
            && prior.job.phase == MutableJobPhase::Queued
            && unstarted_settlement_in(
                rx,
                &prior,
                rx.open_table(POOL_ALLOCATIONS)?
                    .get(id.get())?
                    .map(|row| transactions::decode_pool_allocation(id.get(), row.value()))
                    .transpose()?
                    .as_ref(),
            )?
            .is_some();
        if !ended_unstarted
            && (prior.job.parent_id != record.job.parent_id
                || prior.job.phase != MutableJobPhase::Executing
                || prior
                    .outcome
                    .as_ref()
                    .is_none_or(|outcome| outcome.kind == OutcomeKind::Unknown))
        {
            return Err(context_unavailable(
                "predecessor is not settled in this conversation",
            ));
        }
        selected_bytes = selected_bytes.saturating_add(prior.job.input.text().len());
        if selected_bytes > 8 * 1024 * 1024 {
            return Err(context_unavailable(
                "selected execution context exceeds byte limit",
            ));
        }
        image_count = image_count.saturating_add(prior.job.input.attachments().len());
        if image_count > crate::service::ai::providers::openai_compatible::image::MAX_REQUEST_IMAGES
        {
            return Err(context_unavailable(
                "selected execution context has too many images",
            ));
        }
        let prior_attachments = context_attachments(rx, prior.job.input.attachments())?;
        if is_covered {
            previous = prior.job.previous;
            covered_inputs.push(DevelopmentRetainedInput {
                job: id,
                text: prior.job.input.text().to_owned(),
                attachments: prior_attachments,
            });
            continue;
        }
        let mut prior_outputs = Vec::new();
        for (expected_sequence, entry) in
            (1u32..).zip(outputs.range((id.get(), 0)..=(id.get(), u32::MAX))?)
        {
            if output_count == 1000 {
                return Err(context_unavailable(
                    "conversation output exceeds context limit",
                ));
            }
            let (key, value) = entry?;
            if key.value().1 != expected_sequence {
                return Err(StorageError::corrupted(
                    "predecessor output sequence has a gap",
                ));
            }
            output_bytes = output_bytes.saturating_add(value.value().len());
            selected_bytes = selected_bytes.saturating_add(value.value().len());
            if selected_bytes > 8 * 1024 * 1024 {
                return Err(context_unavailable(
                    "selected execution context exceeds byte limit",
                ));
            }
            if output_bytes > 8 * 1024 * 1024 {
                return Err(context_unavailable(
                    "conversation output exceeds byte limit",
                ));
            }
            prior_outputs.push(decode_output(key.value(), value.value())?);
            output_count += 1;
        }
        if prior_outputs
            .last()
            .is_none_or(|output| output.kind != OutputKind::Result)
        {
            return Err(StorageError::corrupted(
                "settled predecessor has no final output",
            ));
        }
        previous = prior.job.previous;
        history.push(DevelopmentContextTurn {
            record: prior,
            attachments: prior_attachments,
            outputs: prior_outputs,
        });
    }
    covered_inputs.reverse();
    history.reverse();
    if record.job.summary_source.is_none() && covered_inputs.len() != covered.len() {
        return Err(StorageError::corrupted(
            "verified summary coverage is incomplete",
        ));
    }
    let model = &configuration.llm.config;
    if image_count > 0
        && !model
            .declared_capabilities
            .contains(&ModelCapability::Vision)
    {
        return Err(context_unavailable(
            "selected Model does not support images",
        ));
    }
    let output_reserve = model
        .parameters
        .max_completion_tokens
        .or(model.parameters.max_tokens)
        .unwrap_or(4096);
    let estimated_tokens = (selected_bytes.div_ceil(3) as u64)
        .saturating_add((image_count as u64).saturating_mul(1600))
        .saturating_add(u64::from(output_reserve));
    let window = model
        .context_window
        .unwrap_or(crate::service::ai::compact::DEFAULT_CONTEXT_WINDOW);
    if estimated_tokens > u64::from(window) {
        return Err(context_unavailable(
            "selected execution context exceeds estimated model token budget",
        ));
    }
    Ok(DevelopmentCandidate::Generative(
        DevelopmentExecutionContext {
            allocation,
            job: record.job,
            pool,
            executor,
            configuration,
            conversation,
            conversation_control,
            plan_step,
            project,
            compaction,
            attachments,
            adopted_summary,
            covered_inputs,
            history,
            estimated_tokens,
        },
    ))
}

pub(crate) struct MutableJobDetails {
    pub record: crate::db::entities::MutableJobRecord,
    pub allocation: Option<crate::db::entities::PoolAllocation>,
    pub result: Option<JobOutputRecord>,
    pub usage: Option<JobUsage>,
    pub activity: JobActivity,
    pub unknown_reason: Option<String>,
    pub configuration: Option<MutableConfiguration>,
}

pub(crate) struct MutableConfiguration {
    pub selection: ExecutionConfiguration,
    pub model: String,
    pub context_window: Option<u32>,
}

pub(crate) enum MutableJobDetailsLookup {
    NotMutable,
    Inaccessible,
    Found(Box<MutableJobDetails>),
}

pub(crate) fn read_mutable_job_details(
    database: &Database,
    id: JobId,
    context: Option<crate::db::entities::ComposerContextId>,
) -> Result<MutableJobDetailsLookup, StorageError> {
    let rx = read(database)?;
    let jobs = rx.open_table(JOBS)?;
    let Some(bytes) = jobs.get(id.get())? else {
        return Ok(MutableJobDetailsLookup::NotMutable);
    };
    if bytes.value().first() == Some(&JobSpec::MUTABLE_CONVERSATION_TAG) {
        JobSpec::decode_at(id.get(), bytes.value())?;
        return Ok(MutableJobDetailsLookup::NotMutable);
    }
    match mutable_job_details_in(&rx, id, context, true)? {
        Some(details) => Ok(MutableJobDetailsLookup::Found(Box::new(details))),
        None => Ok(MutableJobDetailsLookup::Inaccessible),
    }
}

fn mutable_configuration_in(
    rx: &ReadTransaction,
    job: &crate::db::entities::MutableJob,
) -> Result<Option<MutableConfiguration>, StorageError> {
    let Some(selection) = job.input.configuration() else {
        return Ok(None);
    };
    if let Some(crate::db::entities::MutableFrozenConfiguration::Generative(frozen)) = &job.frozen {
        if frozen.llm.model.id.get() != selection.llm_profile.get()
            || frozen.agent_ref.map(|agent| agent.id.get())
                != selection.agent_profile.map(|agent| agent.get())
        {
            return Err(StorageError::corrupted(
                "frozen Job configuration disagrees with input",
            ));
        }
        return Ok(Some(MutableConfiguration {
            selection,
            model: frozen.llm.config.model.clone(),
            context_window: frozen.llm.config.context_window,
        }));
    }
    let row = rx
        .open_table(CONFIGS)?
        .get(selection.llm_profile.get())?
        .map(|bytes| ConfigRow::decode_at(selection.llm_profile.get(), bytes.value()))
        .transpose()?;
    let Some(row) = row else {
        if job.phase == crate::db::entities::MutableJobPhase::Unsubmitted {
            return Ok(None);
        }
        return Err(StorageError::corrupted("queued Job model missing"));
    };
    let ConfigBody::Model(model) = row.body else {
        if job.phase == crate::db::entities::MutableJobPhase::Unsubmitted {
            return Ok(None);
        }
        return Err(StorageError::corrupted(
            "queued Job model has the wrong kind",
        ));
    };
    Ok(Some(MutableConfiguration {
        selection,
        model: model.model,
        context_window: model.context_window,
    }))
}

pub(super) fn unstarted_settlement_in(
    rx: &ReadTransaction,
    record: &crate::db::entities::MutableJobRecord,
    allocation: Option<&crate::db::entities::PoolAllocation>,
) -> Result<Option<JobOutputRecord>, StorageError> {
    use crate::db::entities::MutableJobPhase;
    let Some(allocation) = allocation.filter(|allocation| {
        record.job.phase == MutableJobPhase::Queued && allocation.phase == AllocationPhase::Released
    }) else {
        return Ok(None);
    };
    if record.outcome.is_none() || allocation.started_at.is_some() {
        return Err(StorageError::corrupted(format!(
            "{} was released before its start without an exact unstarted settlement",
            record.job.id
        )));
    }
    let id = record.job.id.get();
    rx.open_table(JOB_OUTPUTS)?
        .range((id, 0)..=(id, u32::MAX))?
        .next()
        .transpose()?
        .map(|(key, value)| decode_output(key.value(), value.value()))
        .transpose()
}

pub(super) fn mutable_job_details_in(
    rx: &ReadTransaction,
    id: JobId,
    context: Option<crate::db::entities::ComposerContextId>,
    authorize: bool,
) -> Result<Option<MutableJobDetails>, StorageError> {
    use crate::db::entities::MutableJobPhase;
    let Some(record) = mutable_job_record_in(rx, id)? else {
        return Ok(None);
    };
    if authorize
        && (context.is_some_and(|context| context != record.job.composer_context)
            || (record.job.phase == MutableJobPhase::Unsubmitted && context.is_none()))
    {
        return Ok(None);
    }
    let allocation = rx
        .open_table(POOL_ALLOCATIONS)?
        .get(id.get())?
        .map(|bytes| transactions::decode_pool_allocation(id.get(), bytes.value()))
        .transpose()?;
    let latest_output = rx
        .open_table(JOB_OUTPUTS)?
        .range((id.get(), 0)..=(id.get(), u32::MAX))?
        .next_back()
        .transpose()?
        .map(|(key, bytes)| decode_output(key.value(), bytes.value()))
        .transpose()?;
    let mut activity =
        ActivityProjection::new_mutable(&record, allocation.as_ref(), latest_output.as_ref());
    let result = latest_output.filter(|output| output.kind == OutputKind::Result);
    let mut usage = JobUsage::default();
    let mut events = Vec::new();
    let mut scanned = 0;
    for entry in rx
        .open_table(REQUEST_EVENTS)?
        .range((id.get(), 0, 0)..=(id.get(), u32::MAX, u32::MAX))?
        .rev()
        .take(MAX_OBSERVATION_SCAN + 1)
    {
        if scanned == MAX_OBSERVATION_SCAN {
            usage.truncated = true;
            break;
        }
        let (key, bytes) = entry?;
        let (_, request, seq) = key.value();
        let request = RequestNumber::new(request)
            .ok_or_else(|| StorageError::corrupted("a stored request number is 0"))?;
        scanned += 1;
        let event = super::rows::decode_request_event(id, request, seq, bytes.value())?;
        activity.request_event(&event);
        let seq = event.seq;
        events.push(event);
        if seq == 1 {
            events.reverse();
            if let Some(sample) = observations::usage_sample_of(id, request, None, &events) {
                usage.add_sample(&sample);
            }
            events.clear();
        }
    }
    for entry in rx
        .open_table(TOOL_CALLS)?
        .range((id.get(), 0, 0)..=(id.get(), u32::MAX, u32::MAX))?
        .rev()
        .take(256)
    {
        let (key, bytes) = entry?;
        let call = decode_tool_evidence(tool_call_ref(key.value())?, bytes.value())?;
        let awaiting_user = match &call.decision.fact {
            crate::db::entities::ToolDecision::AwaitingAnswer(_)
            | crate::db::entities::ToolDecision::AwaitingApproval(_) => rx
                .open_table(OPEN_APPROVALS)?
                .get((
                    record.job.parent_id.get(),
                    call.call.job.get(),
                    call.call.request.get(),
                    call.call.index,
                ))?
                .is_some(),
            crate::db::entities::ToolDecision::Admitted(_)
            | crate::db::entities::ToolDecision::Refused(_) => false,
        };
        activity.tool_call(call, awaiting_user);
    }
    let (activity, unknown_reason) = activity.finish_mutable(&record);
    let unknown_reason = unknown_reason.or_else(|| {
        (record
            .outcome
            .as_ref()
            .is_some_and(|outcome| outcome.kind == OutcomeKind::Unknown)
            && allocation
                .as_ref()
                .is_some_and(crate::db::entities::PoolAllocation::holds_capacity))
        .then(|| {
            let detail = match record.outcome.as_ref().and_then(|outcome| outcome.cause.as_ref()) {
                Some(crate::db::entities::OutcomeCause::Uncertain(reason)) => {
                    format!(" {}", reason.description())
                }
                Some(_) | None => String::new(),
            };
            format!(
                "Execution outcome is uncertain. The Pool Allocation is retained and its capacity is not released until termination is confirmed.{detail}"
            )
        })
    });
    let configuration = mutable_configuration_in(rx, &record.job)?;
    Ok(Some(MutableJobDetails {
        record,
        allocation,
        result,
        usage: (scanned != 0).then_some(usage),
        activity,
        unknown_reason,
        configuration,
    }))
}

pub(crate) struct MutableConversationPage {
    pub turns: Vec<MutableConversationTurn>,
    pub has_more: bool,
}

pub(crate) struct MutableConversationTurn {
    pub details: MutableJobDetails,
    pub outputs: Vec<JobOutputRecord>,
    pub outputs_truncated: bool,
    pub evidence: Vec<ToolEvidenceRecord>,
}

pub(crate) fn read_mutable_conversation_turn_page(
    database: &Database,
    conversation: JobId,
    before: Option<JobId>,
    limit: usize,
) -> Result<Option<MutableConversationPage>, StorageError> {
    let rx = read(database)?;
    if !rx
        .open_table(JOBS)?
        .get(conversation.get())?
        .is_some_and(|row| row.value().first() == Some(&JobSpec::MUTABLE_CONVERSATION_TAG))
    {
        return Ok(None);
    }
    if !read_job_row(&rx, conversation)?.is_some_and(|row| row.kind.is_group()) {
        return Ok(None);
    }
    if let Some(before) = before {
        if rx
            .open_table(TURNS)?
            .get((conversation.get(), before.get()))?
            .is_none()
        {
            return Ok(Some(MutableConversationPage {
                turns: Vec::new(),
                has_more: false,
            }));
        }
        let valid = mutable_job_record_in(&rx, before)?.is_some_and(|row| {
            row.job.parent_id == conversation
                && row.job.phase != crate::db::entities::MutableJobPhase::Unsubmitted
        });
        if !valid {
            return Ok(Some(MutableConversationPage {
                turns: Vec::new(),
                has_more: false,
            }));
        }
    }
    let bounds = (
        Bound::Included((conversation.get(), 0)),
        before.map_or(Bound::Included((conversation.get(), u128::MAX)), |id| {
            Bound::Excluded((conversation.get(), id.get()))
        }),
    );
    let turns = rx.open_table(TURNS)?;
    let mut rows = Vec::new();
    for entry in turns
        .range(bounds)?
        .rev()
        .take(limit.saturating_add(1).min(MAX_PAGE + 1))
    {
        let (key, _) = entry?;
        let details = mutable_job_details_in(&rx, JobId::new(key.value().1), None, false)?
            .ok_or_else(|| StorageError::corrupted("turn projection has no source Job"))?;
        if details.record.job.parent_id != conversation
            || details.record.job.phase == crate::db::entities::MutableJobPhase::Unsubmitted
        {
            return Err(StorageError::corrupted(
                "turn projection disagrees with source Job",
            ));
        }
        let id = details.record.job.id;
        let mut outputs = Vec::new();
        for entry in rx
            .open_table(JOB_OUTPUTS)?
            .range((id.get(), 0)..=(id.get(), u32::MAX))?
            .take(TURN_PAGE_OUTPUTS)
        {
            let (key, bytes) = entry?;
            outputs.push(decode_output(key.value(), bytes.value())?);
        }
        let outputs_truncated = outputs.len() == TURN_PAGE_OUTPUTS
            && outputs
                .last()
                .is_some_and(|output| output.kind != OutputKind::Result);
        if outputs_truncated && let Some(result) = &details.result {
            outputs.push(result.clone());
        }
        let evidence = if details
            .record
            .outcome
            .as_ref()
            .is_some_and(|outcome| outcome.kind == OutcomeKind::Unknown)
        {
            tool_evidence(&rx, id, None, TURN_PAGE_OUTPUTS)?
        } else {
            Vec::new()
        };
        rows.push(MutableConversationTurn {
            details,
            outputs,
            outputs_truncated,
            evidence,
        });
    }
    let has_more = rows.len() > limit;
    rows.truncate(limit);
    Ok(Some(MutableConversationPage {
        turns: rows,
        has_more,
    }))
}

pub(crate) struct NativeJobItem {
    pub record: crate::db::entities::MutableJobRecord,
    pub allocation: Option<crate::db::entities::PoolAllocation>,
    pub updated_at_us: i64,
}

pub(crate) struct JobSearchPage {
    pub items: Vec<NativeJobItem>,
    pub next_before: Option<JobId>,
    pub has_more: bool,
}

fn native_job_item(
    rx: &ReadTransaction,
    record: crate::db::entities::MutableJobRecord,
) -> Result<NativeJobItem, StorageError> {
    let id = record.job.id;
    let allocation = rx
        .open_table(POOL_ALLOCATIONS)?
        .get(id.get())?
        .map(|bytes| transactions::decode_pool_allocation(id.get(), bytes.value()))
        .transpose()?;
    if allocation
        .as_ref()
        .is_some_and(|allocation| allocation.job_revision != record.job.revision)
    {
        return Err(StorageError::corrupted(
            "allocation refers to another Job revision",
        ));
    }
    let updated_at = [
        Some(record.job.created_at),
        record.job.submitted_at,
        record.outcome.as_ref().map(|outcome| outcome.at),
    ]
    .into_iter()
    .flatten()
    .max()
    .unwrap_or(record.job.created_at);
    Ok(NativeJobItem {
        record,
        allocation,
        updated_at_us: updated_at.as_unix_micros(),
    })
}

pub(crate) fn read_native_job(
    database: &Database,
    id: JobId,
) -> Result<Option<NativeJobItem>, StorageError> {
    let rx = read(database)?;
    let Some(record) = mutable_job_record_in(&rx, id)? else {
        return Ok(None);
    };
    if record.job.phase == crate::db::entities::MutableJobPhase::Unsubmitted {
        return Ok(None);
    }
    native_job_item(&rx, record).map(Some)
}

const JOB_LIST_SCAN: usize = 5_000;

pub(crate) fn read_job_search(
    database: &Database,
    terms: &[String],
    before: Option<JobId>,
    limit: usize,
) -> Result<JobSearchPage, StorageError> {
    use crate::db::entities::MutableJobPhase;
    let rx = read(database)?;
    let jobs = rx.open_table(JOBS)?;
    let end = before.map_or(u128::MAX, JobId::get);
    let limit = limit.clamp(1, 100);
    let mut items: Vec<NativeJobItem> = Vec::new();
    let mut last_examined = None;
    let mut has_more = false;
    let mut next_before = None;
    for (examined, entry) in jobs.range(..end)?.rev().enumerate() {
        if examined == JOB_LIST_SCAN {
            has_more = true;
            next_before = last_examined;
            break;
        }
        let (key, bytes) = entry?;
        let id = JobId::new(key.value());
        last_examined = Some(id);
        if bytes.value().first() == Some(&JobSpec::MUTABLE_CONVERSATION_TAG) {
            continue;
        }
        let Some(record) = mutable_job_record_in(&rx, id)? else {
            continue;
        };
        if record.job.phase == MutableJobPhase::Unsubmitted {
            continue;
        }
        if !terms.is_empty() {
            let identity = id.to_string().to_lowercase();
            let text = record.job.input.text().to_lowercase();
            if !terms
                .iter()
                .all(|term| identity.contains(term.as_str()) || text.contains(term.as_str()))
            {
                continue;
            }
        }
        if items.len() == limit {
            has_more = true;
            next_before = items.last().map(|item| item.record.job.id);
            break;
        }
        items.push(native_job_item(&rx, record)?);
    }
    Ok(JobSearchPage {
        items,
        next_before,
        has_more,
    })
}

pub(crate) fn read_children(
    database: &Database,
    parent: JobId,
    after: Option<JobId>,
    limit: usize,
) -> Result<Vec<JobId>, StorageError> {
    let rx = read(database)?;
    let table = rx.open_table(CHILDREN)?;
    let start = after.map_or(0, |after| after.get().saturating_add(1));
    let mut children = Vec::new();
    for entry in table
        .range((parent.get(), start)..=(parent.get(), u128::MAX))?
        .take(limit.min(MAX_PAGE))
    {
        children.push(JobId::new(entry?.0.value().1));
    }
    Ok(children)
}

pub(crate) const TREE_PAGE_MAX: usize = 100;
pub(crate) const TREE_NESTED: usize = 20;
pub(crate) const TREE_NODES_MAX: usize = 300;
pub(crate) const TREE_DEPTH_MAX: u8 = 4;
const TREE_ANCESTORS_MAX: usize = 8;
const TREE_DEPENDENCIES_MAX: usize = 16;
const TITLE_CHARS: usize = 80;

#[derive(Clone, Debug, PartialEq)]
pub enum TreeFacts {
    Conversation {
        title: String,
        state: &'static str,
        on_hold: Option<Actor>,
        origin: &'static str,
        keep_open: bool,
        active_turn: Option<(JobId, ConfigId, String)>,
        recurrence: Option<Recurrence>,
        depends_on: Vec<(JobId, DependencyCondition, DependencyState)>,
    },
    Turn {
        title: String,
        state: &'static str,
        conversation: Option<JobId>,
        previous: Option<JobId>,
        model: (ConfigId, String),
        depends_on: Vec<(JobId, DependencyCondition, DependencyState)>,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct TreeNode {
    pub id: JobId,
    pub parent_id: Option<JobId>,
    pub created_at: Timestamp,
    pub facts: TreeFacts,
    pub agent: Option<(ConfigId, Option<String>)>,
    pub children: Option<Vec<usize>>,
    pub has_more_children: bool,
    pub next_after: Option<JobId>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct JobTree {
    pub ancestors: Vec<JobId>,
    pub nodes: Vec<TreeNode>,
}

struct ChildPage {
    listed: Vec<JobId>,
    has_more: bool,
    next_after: Option<JobId>,
}

fn child_page(
    rx: &ReadTransaction,
    parent: JobId,
    after: Option<JobId>,
    limit: usize,
) -> Result<ChildPage, StorageError> {
    let start = after.map_or(0, |after| after.get().saturating_add(1));
    let mut listed = Vec::new();
    let mut has_more = false;
    for entry in rx
        .open_table(CHILDREN)?
        .range((parent.get(), start)..=(parent.get(), u128::MAX))?
    {
        let child = JobId::new(entry?.0.value().1);
        if child == parent {
            continue;
        }
        if listed.len() == limit {
            has_more = true;
            break;
        }
        listed.push(child);
    }
    let next_after = has_more.then(|| listed.last().copied()).flatten();
    Ok(ChildPage {
        listed,
        has_more,
        next_after,
    })
}

fn has_children(rx: &ReadTransaction, parent: JobId) -> Result<bool, StorageError> {
    Ok(rx
        .open_table(CHILDREN)?
        .range((parent.get(), 0)..=(parent.get(), u128::MAX))?
        .next()
        .transpose()?
        .is_some())
}

fn read_ancestors(rx: &ReadTransaction, root: JobId) -> Result<Vec<JobId>, StorageError> {
    let mut ancestors = Vec::new();
    let mut current = read_job_row(rx, root)?.and_then(|row| row.parent_id);
    while let Some(id) = current {
        if id.get() == 0 {
            break;
        }
        ancestors.push(id);
        if ancestors.len() > TREE_ANCESTORS_MAX {
            return Err(StorageError::corrupted("too many ancestors"));
        }
        current = match read_job_row(rx, id)? {
            Some(row) => row.parent_id,
            None => return Err(StorageError::corrupted(format!("{id} has no job row"))),
        };
    }
    ancestors.reverse();
    Ok(ancestors)
}

fn agent_name(
    rx: &ReadTransaction,
    agent: ConfigId,
    names: &mut BTreeMap<ConfigId, Option<String>>,
) -> Result<Option<String>, StorageError> {
    if let Some(name) = names.get(&agent) {
        return Ok(name.clone());
    }
    let name = rx
        .open_table(CONFIGS)?
        .get(agent.get())?
        .map(|bytes| ConfigRow::decode_at(agent.get(), bytes.value()))
        .transpose()?
        .map(|row| row.name);
    names.insert(agent, name.clone());
    Ok(name)
}

fn active_job(
    rx: &ReadTransaction,
    conversation: JobId,
) -> Result<Option<(JobId, ConfigId, String)>, StorageError> {
    let Some(turn) = rx
        .open_table(ACTIVE_JOBS)?
        .get(conversation.get())?
        .map(|id| JobId::new(id.value()))
    else {
        return Ok(None);
    };
    let _ = turn;
    Ok(None)
}

fn turn_state(rx: &ReadTransaction, id: JobId) -> Result<&'static str, StorageError> {
    let state = read_state(rx, id)?;
    if let Some(outcome) = &state.outcome {
        return Ok(match outcome.kind {
            OutcomeKind::Completed => "completed",
            OutcomeKind::Failed => "failed",
            OutcomeKind::Cancelled => "cancelled",
            OutcomeKind::Unknown => "unknown",
        });
    }
    let open = rx
        .open_table(UNSETTLED)?
        .get(id.get())?
        .map(|counts| counts.value().0)
        .unwrap_or(0);
    Ok(if open > 0 { "waiting" } else { "queued" })
}

fn dependency_state(
    rx: &ReadTransaction,
    dependency: JobId,
    condition: DependencyCondition,
) -> Result<DependencyState, StorageError> {
    if rx.open_table(UNSETTLED)?.get(dependency.get())?.is_some() {
        return Ok(DependencyState::Waiting);
    }
    let state = rx
        .open_table(JOB_STATES)?
        .get(dependency.get())?
        .map(|bytes| JobState::decode_at(dependency.get(), bytes.value()))
        .transpose()?;
    let completed = state
        .as_ref()
        .and_then(|state| state.outcome.as_ref())
        .is_some_and(|outcome| outcome.kind == OutcomeKind::Completed);
    Ok(if completed || condition == DependencyCondition::Settled {
        DependencyState::Satisfied
    } else {
        DependencyState::Impossible
    })
}

fn cap_title(title: &str) -> String {
    title.chars().take(TITLE_CHARS).collect()
}

fn first_entry_text(rx: &ReadTransaction, turn: JobId) -> Result<String, StorageError> {
    let job = mutable_job_record_in(rx, turn)?
        .ok_or_else(|| StorageError::corrupted("conversation turn missing"))?;
    Ok(job.job.input.text().chars().take(TITLE_CHARS).collect())
}

enum TreeRow {
    Conversation(Box<JobSpec>),
    Turn(Box<crate::db::entities::MutableJobRecord>),
}

impl TreeRow {
    fn id(&self) -> JobId {
        match self {
            Self::Conversation(row) => row.id,
            Self::Turn(record) => record.job.id,
        }
    }
}

fn tree_row(rx: &ReadTransaction, id: JobId) -> Result<Option<TreeRow>, StorageError> {
    let is_conversation = rx
        .open_table(JOBS)?
        .get(id.get())?
        .map(|bytes| bytes.value().first() == Some(&JobSpec::MUTABLE_CONVERSATION_TAG));
    match is_conversation {
        None => Ok(None),
        Some(true) => Ok(read_job_row(rx, id)?.map(|row| TreeRow::Conversation(Box::new(row)))),
        Some(false) => {
            Ok(mutable_job_record_in(rx, id)?.map(|record| TreeRow::Turn(Box::new(record))))
        }
    }
}

fn tree_node(
    rx: &ReadTransaction,
    row: &TreeRow,
    names: &mut BTreeMap<ConfigId, Option<String>>,
) -> Result<TreeNode, StorageError> {
    let (parent_id, created_at, facts, agent) = match row {
        TreeRow::Conversation(row) => {
            let JobKind::Group {
                origin, keep_open, ..
            } = &row.kind
            else {
                return Err(StorageError::corrupted("conversation row is not a group"));
            };
            let tip = turn_head(&rx.open_table(TURNS)?, row.id)?
                .ok_or_else(|| StorageError::corrupted(format!("{} has no tip", row.id)))?;
            let summary = conversation_summary(rx, row.id, tip)?;
            let state = match summary.state.as_str() {
                "open" => "open",
                "closing" => "closing",
                "settled" => "settled",
                _ => return Err(StorageError::corrupted("unknown conversation state")),
            };
            let origin = match origin {
                Origin::User => "user",
                Origin::Agent { .. } => "agent",
                Origin::System => "system",
            };
            let recurrence = rx
                .open_table(RECURRENCES)?
                .get(row.id.get())?
                .map(|bytes| Recurrence::decode(bytes.value()))
                .transpose()?;
            let mut depends_on = Vec::new();
            for (dependency, condition) in read_job_control(rx, row.id)?
                .after
                .iter()
                .take(TREE_DEPENDENCIES_MAX)
            {
                depends_on.push((
                    *dependency,
                    *condition,
                    dependency_state(rx, *dependency, *condition)?,
                ));
            }
            (
                row.parent_id,
                row.created_at,
                TreeFacts::Conversation {
                    title: cap_title(&summary.title),
                    state,
                    on_hold: summary.on_hold,
                    origin,
                    keep_open: *keep_open,
                    active_turn: active_job(rx, row.id)?,
                    recurrence,
                    depends_on,
                },
                summary.agent,
            )
        }
        TreeRow::Turn(record) => {
            use crate::db::entities::{
                MutableFrozenConfiguration, MutableJobContent, MutableJobPhase,
            };
            let job = &record.job;
            let (model, agent) = match (&job.frozen, &job.input.content) {
                (Some(MutableFrozenConfiguration::Generative(frozen)), _) => (
                    (frozen.llm.model.id, frozen.llm.config.model.clone()),
                    frozen.agent_ref.as_ref().map(|reference| reference.id),
                ),
                (
                    _,
                    MutableJobContent::Generative {
                        configuration: Some(configuration),
                        ..
                    },
                ) => (
                    (configuration.llm_profile.into(), String::new()),
                    configuration.agent_profile.map(Into::into),
                ),
                (_, MutableJobContent::Decision { profile, .. }) => {
                    ((*profile, String::new()), Some(*profile))
                }
                (_, MutableJobContent::Generative { .. }) => {
                    return Err(StorageError::corrupted("turn has no configuration"));
                }
            };
            let state = match record.outcome.as_ref().map(|outcome| outcome.kind) {
                Some(OutcomeKind::Completed) => "completed",
                Some(OutcomeKind::Failed) => "failed",
                Some(OutcomeKind::Cancelled) => "cancelled",
                Some(OutcomeKind::Unknown) => "unknown",
                None => match job.phase {
                    MutableJobPhase::Executing => "running",
                    MutableJobPhase::Unsubmitted | MutableJobPhase::Queued => "queued",
                },
            };
            (
                Some(job.parent_id),
                job.created_at,
                TreeFacts::Turn {
                    title: cap_title(job.input.text()),
                    state,
                    conversation: Some(job.parent_id),
                    previous: job.previous,
                    model,
                    depends_on: Vec::new(),
                },
                agent,
            )
        }
    };
    let agent = match agent {
        Some(agent) => Some((agent, agent_name(rx, agent, names)?)),
        None => None,
    };
    Ok(TreeNode {
        id: row.id(),
        parent_id,
        created_at,
        facts,
        agent,
        children: None,
        has_more_children: false,
        next_after: None,
    })
}

pub(crate) fn read_job_tree(
    database: &Database,
    root: JobId,
    after: Option<JobId>,
    limit: usize,
    depth: u8,
) -> Result<Option<JobTree>, StorageError> {
    let rx = read(database)?;
    let Some(root_row) = tree_row(&rx, root)? else {
        return Ok(None);
    };
    let ancestors = read_ancestors(&rx, root)?;
    let mut names: BTreeMap<ConfigId, Option<String>> = BTreeMap::new();
    let mut nodes = vec![tree_node(&rx, &root_row, &mut names)?];
    let page = child_page(&rx, root, after, limit.min(TREE_PAGE_MAX))?;
    let mut level = Vec::new();
    for child in page.listed {
        let row = tree_row(&rx, child)?
            .ok_or_else(|| StorageError::corrupted(format!("{child} has no job row")))?;
        nodes.push(tree_node(&rx, &row, &mut names)?);
        level.push(nodes.len() - 1);
    }
    nodes[0].children = Some(level.clone());
    nodes[0].has_more_children = page.has_more;
    nodes[0].next_after = page.next_after;
    for _ in 2..=depth.min(TREE_DEPTH_MAX) {
        let mut next = Vec::new();
        for index in level {
            if nodes.len() + TREE_NESTED > TREE_NODES_MAX {
                nodes[index].has_more_children = has_children(&rx, nodes[index].id)?;
                continue;
            }
            let page = child_page(&rx, nodes[index].id, None, TREE_NESTED)?;
            let mut children = Vec::new();
            for child in page.listed {
                let row = tree_row(&rx, child)?
                    .ok_or_else(|| StorageError::corrupted(format!("{child} has no job row")))?;
                nodes.push(tree_node(&rx, &row, &mut names)?);
                children.push(nodes.len() - 1);
            }
            nodes[index].children = Some(children.clone());
            nodes[index].has_more_children = page.has_more;
            nodes[index].next_after = page.next_after;
            next.extend(children);
        }
        level = next;
    }
    for index in level {
        nodes[index].has_more_children = has_children(&rx, nodes[index].id)?;
    }
    Ok(Some(JobTree { ancestors, nodes }))
}

#[derive(Clone, Debug, PartialEq)]
pub struct CursorPage<T, C> {
    pub records: Vec<T>,
    pub next: Option<C>,
}

pub(crate) fn read_retries(
    database: &Database,
    original: JobId,
    after: Option<JobId>,
    limit: usize,
) -> Result<CursorPage<JobId, JobId>, StorageError> {
    let rx = read(database)?;
    let table = rx.open_table(RETRIES)?;
    let limit = limit.clamp(1, MAX_PAGE);
    let mut records = Vec::new();
    let mut more = false;
    for entry in table.range((
        std::ops::Bound::Excluded((original.get(), after.map_or(0, JobId::get))),
        std::ops::Bound::Included((original.get(), u128::MAX)),
    ))? {
        if records.len() == limit {
            more = true;
            break;
        }
        records.push(JobId::new(entry?.0.value().1));
    }
    let next = records.last().copied().filter(|_| more);
    Ok(CursorPage { records, next })
}

pub(crate) fn read_outputs(
    database: &Database,
    job: JobId,
    after_seq: Option<i64>,
    limit: usize,
) -> Result<Vec<JobOutputRecord>, StorageError> {
    outputs(&read(database)?, job, after_seq, limit)
}

fn output_is_group(rx: &ReadTransaction, job: JobId) -> Result<bool, StorageError> {
    {
        let jobs = rx.open_table(JOBS)?;
        let Some(row) = jobs.get(job.get())? else {
            return Ok(false);
        };
        if row.value().first() == Some(&JobSpec::MUTABLE_CONVERSATION_TAG) {
            return Ok(JobSpec::decode_at(job.get(), row.value())?.kind.is_group());
        }
        transactions::decode_mutable_job(job.get(), row.value())?;
        Ok(false)
    }
}

fn outputs(
    rx: &ReadTransaction,
    job: JobId,
    after_seq: Option<i64>,
    limit: usize,
) -> Result<Vec<JobOutputRecord>, StorageError> {
    if output_is_group(rx, job)? {
        if after_seq.is_some_and(|seq| seq >= 1) || limit == 0 {
            return Ok(Vec::new());
        }
        return Ok(conversation_result(rx, job)?.into_iter().collect());
    }
    let outputs = rx.open_table(JOB_OUTPUTS)?;
    let start = match after_seq.map(u32::try_from) {
        None => 0,
        Some(Err(_)) => return Ok(Vec::new()),
        Some(Ok(u32::MAX)) => return Ok(Vec::new()),
        Some(Ok(seq)) => seq + 1,
    };
    let mut records = Vec::new();
    for entry in outputs
        .range((job.get(), start)..=(job.get(), u32::MAX))?
        .take(limit.min(MAX_PAGE))
    {
        let (key, bytes) = entry?;
        records.push(decode_output(key.value(), bytes.value())?);
    }
    Ok(records)
}

pub(crate) fn read_latest_output(
    database: &Database,
    job: JobId,
) -> Result<Option<JobOutputRecord>, StorageError> {
    latest_output(&read(database)?, job)
}

fn latest_output(
    rx: &ReadTransaction,
    job: JobId,
) -> Result<Option<JobOutputRecord>, StorageError> {
    if output_is_group(rx, job)? {
        return conversation_result(rx, job);
    }
    let outputs = rx.open_table(JOB_OUTPUTS)?;
    outputs
        .range((job.get(), 0)..=(job.get(), u32::MAX))?
        .next_back()
        .transpose()?
        .map(|(key, bytes)| decode_output(key.value(), bytes.value()))
        .transpose()
}

fn conversation_result(
    rx: &ReadTransaction,
    conversation: JobId,
) -> Result<Option<JobOutputRecord>, StorageError> {
    let state = read_state(rx, conversation)?;
    if state.outcome.is_none() {
        return Ok(None);
    }
    let mut summary = String::new();
    let mut value = None;
    let answered = rx
        .open_table(GROUP_RESULTS)?
        .get(conversation.get())?
        .map(|tips| tips.value().1)
        .filter(|turn| *turn != 0);
    if let Some(turn) = answered
        && let Some((key, bytes)) = rx
            .open_table(JOB_OUTPUTS)?
            .range((turn, 0)..=(turn, u32::MAX))?
            .next_back()
            .transpose()?
    {
        let output = decode_output(key.value(), bytes.value())?;
        summary = output.summary.unwrap_or_default();
        value = output.value;
    }
    let after = rx
        .open_table(NOTICE_READ)?
        .get(conversation.get())?
        .map_or(0, |seq| seq.value());
    if after < u32::MAX {
        for entry in rx
            .open_table(NOTICES)?
            .range((conversation.get(), after + 1)..=(conversation.get(), u32::MAX))?
        {
            let notice = Notice::decode(entry?.1.value())?;
            if matches!(notice.class, NoticeClass::Attention | NoticeClass::Progress) {
                summary.push_str(&format!("\nUnread notice: {:?}", notice.body));
            }
        }
    }
    for call in approved_calls(rx, conversation)? {
        summary.push_str(&format!(
            "\nApproved, never ran: {} {}",
            call.name,
            call.decision
                .fact
                .stored_admission()
                .map_or("{}", |a| a.arguments.as_str())
        ));
    }
    Ok(Some(JobOutputRecord {
        id: JobOutputId::new(1),
        job: conversation,
        seq: 1,
        kind: OutputKind::Result,
        summary: Some(summary),
        code: None,
        goal_reached: None,
        value,
        created_at: state
            .outcome
            .map_or(Timestamp::from_unix_micros(0), |outcome| outcome.at),
    }))
}

pub(crate) fn read_tool_evidence(
    database: &Database,
    job: JobId,
    after: Option<(RequestNumber, u32)>,
    limit: usize,
) -> Result<Vec<ToolEvidenceRecord>, StorageError> {
    tool_evidence(&read(database)?, job, after, limit)
}

fn tool_evidence(
    rx: &ReadTransaction,
    job: JobId,
    after: Option<(RequestNumber, u32)>,
    limit: usize,
) -> Result<Vec<ToolEvidenceRecord>, StorageError> {
    let table = rx.open_table(TOOL_CALLS)?;
    let start = match after {
        None => (job.get(), 0, 0),
        Some((request, u32::MAX)) => match request.get().checked_add(1) {
            Some(next) => (job.get(), next, 0),
            None => return Ok(Vec::new()),
        },
        Some((request, index)) => (job.get(), request.get(), index + 1),
    };
    let mut records = Vec::new();
    for entry in table
        .range(start..=(job.get(), u32::MAX, u32::MAX))?
        .take(limit.min(MAX_PAGE))
    {
        let (key, bytes) = entry?;
        records.push(decode_tool_evidence(
            tool_call_ref(key.value())?,
            bytes.value(),
        )?);
    }
    Ok(records)
}

fn tool_call_name(rx: &ReadTransaction, call: ToolCallRef) -> Result<Option<String>, StorageError> {
    rx.open_table(TOOL_CALLS)?
        .get(rows::tool_call_key(call))?
        .map(|bytes| decode_tool_evidence(call, bytes.value()).map(|record| record.name))
        .transpose()
}

fn input_of(rx: &ReadTransaction, row: JobSpec) -> Result<JobInputRecord, StorageError> {
    let execution = turn_config(rx, &row)?;
    let mut objective = row.kind.objective().map(str::to_owned);
    let mut attachments = row.kind.attachments().to_vec();
    if row.kind.is_persistent() {
        let mut messages = Vec::new();
        for (_, entry) in turn_entries(rx, row.id)? {
            match entry.entry {
                Entry::UserMessage {
                    text,
                    attachments: images,
                } => {
                    messages.push(text);
                    attachments.extend(images);
                }
                Entry::AgentMessage { text, .. } => messages.push(text),
                Entry::UserMessageEdit { .. }
                | Entry::UserMessageWithdraw { .. }
                | Entry::Notice { .. }
                | Entry::Decision { .. }
                | Entry::RetryOf { .. }
                | Entry::Iteration { .. }
                | Entry::ResponseFormat { .. }
                | Entry::ContextPreparation { .. }
                | Entry::ContextUse { .. } => {}
            }
        }
        if !messages.is_empty() {
            objective = Some(messages.join("\n"));
        }
    }
    Ok(JobInputRecord {
        id: JobInputId::new(row.id.get()),
        objective,
        submitted_at: Some(row.created_at),
        created_at: row.created_at,
        updated_at: row.created_at,
        configuration: execution,
        attachments,
    })
}

pub(crate) fn read_job_input(
    database: &Database,
    id: JobInputId,
) -> Result<Option<JobInputRecord>, StorageError> {
    read_job_input_for_job(database, JobId::new(id.get()))
}

pub(crate) fn read_job_input_for_job(
    database: &Database,
    job: JobId,
) -> Result<Option<JobInputRecord>, StorageError> {
    let rx = read(database)?;
    read_job_row(&rx, job)?
        .map(|row| input_of(&rx, row))
        .transpose()
}

pub(super) fn turn_config(
    tx: &impl TxRead,
    row: &JobSpec,
) -> Result<Option<FrozenExecutionConfig>, StorageError> {
    let jobs = tx.table(JOBS)?;
    if matches!(
        &row.kind,
        JobKind::Execution {
            execution: JobExecution::Decision(_),
            ..
        }
    ) || row.kind.is_group()
    {
        return Ok(None);
    }
    Ok(Some(job_execution(&jobs, row)?.0))
}

#[derive(Clone)]
pub(super) struct UsageIdentity {
    pub(super) provider: u128,
    pub(super) model: String,
}

pub(super) fn turn_usage_identity_of(
    jobs: &impl ReadableTable<u128, &'static [u8]>,
    row: &JobSpec,
) -> Result<Option<UsageIdentity>, StorageError> {
    match &row.kind {
        JobKind::Group { .. } => Ok(None),
        JobKind::Execution {
            execution: JobExecution::Decision(work),
            ..
        } => Ok(Some(UsageIdentity {
            provider: work.frozen.connection_ref().id.get(),
            model: work.frozen.model_name().to_owned(),
        })),
        JobKind::Execution { .. } => {
            let (execution, _, _) = job_execution(jobs, row)?;
            Ok(Some(UsageIdentity {
                provider: execution.llm.provider.id.get(),
                model: execution.llm.config.model,
            }))
        }
    }
}

pub(super) fn turn_target(
    tx: &impl TxRead,
    row: &JobSpec,
) -> Result<Option<crate::db::entities::LlmTarget>, StorageError> {
    let jobs = tx.table(JOBS)?;
    turn_target_of(&jobs, row)
}

pub(super) fn turn_target_of(
    jobs: &impl ReadableTable<u128, &'static [u8]>,
    row: &JobSpec,
) -> Result<Option<crate::db::entities::LlmTarget>, StorageError> {
    if matches!(
        &row.kind,
        JobKind::Execution {
            execution: JobExecution::Decision(_),
            ..
        }
    ) || row.kind.is_group()
    {
        return Ok(None);
    }
    let (execution, _, _) = job_execution(jobs, row)?;
    Ok(Some(crate::db::entities::LlmTarget {
        llm_profile: execution.llm.model.id.into(),
        frozen: execution.llm,
    }))
}

pub(crate) fn read_job_configuration(
    database: &Database,
    job: JobId,
) -> Result<Option<SubmittedConfiguration>, StorageError> {
    job_configuration(&read(database)?, job)
}

fn job_configuration(
    rx: &ReadTransaction,
    job: JobId,
) -> Result<Option<SubmittedConfiguration>, StorageError> {
    let Some(row) = read_job_row(rx, job)? else {
        return Ok(None);
    };
    if row.kind.is_group() {
        return Ok(None);
    }
    let Some(execution) = turn_config(rx, &row)? else {
        return Ok(None);
    };
    Ok(Some(SubmittedConfiguration {
        agent_profile: execution.agent_ref.map(|agent| agent.id.into()),
        llm_profile: execution.llm.model.id.into(),
        frozen: execution,
    }))
}

pub(crate) fn read_decision_work(
    database: &Database,
    job: JobId,
) -> Result<Option<crate::db::entities::DecisionWork>, StorageError> {
    let rx = read(database)?;
    let Some(row) = read_job_row(&rx, job)? else {
        return Ok(None);
    };
    match row.kind {
        JobKind::Execution {
            execution: JobExecution::Decision(work),
            ..
        } => Ok(Some(work)),
        JobKind::Group { .. } | JobKind::Execution { .. } => Ok(None),
    }
}

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

pub(crate) fn read_attachments(
    database: &Database,
    before: Option<AttachmentId>,
    limit: usize,
) -> Result<Vec<AttachmentRecord>, StorageError> {
    let rx = read(database)?;
    let table = rx.open_table(ATTACHMENTS)?;
    let end = before.map_or(u128::MAX, AttachmentId::get);
    let mut records = Vec::new();
    for entry in table.range(..end)?.rev().take(limit.min(MAX_PAGE)) {
        let (key, bytes) = entry?;
        records.push(decode_attachment(key.value(), bytes.value())?);
    }
    Ok(records)
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

pub(super) fn config_record(row: ConfigRow) -> ConfigRecord {
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

fn config(rx: &ReadTransaction, id: ConfigId) -> Result<Option<ConfigRecord>, StorageError> {
    rx.open_table(CONFIGS)?
        .get(id.get())?
        .map(|bytes| ConfigRow::decode_at(id.get(), bytes.value()).map(config_record))
        .transpose()
}

pub(crate) fn read_credential_ref_in_use(
    database: &Database,
    reference: &str,
) -> Result<bool, StorageError> {
    let rx = read(database)?;
    let mut matching_connections = std::collections::BTreeSet::new();
    let mut live_connections = std::collections::BTreeSet::new();
    for entry in rx.open_table(CONFIGS)?.iter()? {
        let (key, bytes) = entry?;
        let Some(row) = ConfigRow::scan_at(key.value(), bytes.value())? else {
            continue;
        };
        match &row.body {
            ConfigBody::Connection(connection) => {
                if connection.credential_refs().any(|used| used == reference) {
                    matching_connections.insert(row.id);
                }
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
    Ok(!matching_connections.is_disjoint(&live_connections))
}

pub(crate) fn read_configs(
    database: &Database,
    kind: Option<ConfigKind>,
    before: Option<ConfigId>,
    limit: usize,
) -> Result<CursorPage<ConfigRecord, ConfigId>, StorageError> {
    if kind.is_none() || kind == Some(ConfigKind::Pool) {
        return Err(StorageError {
            kind: super::StorageErrorKind::Other,
            os_error: None,
            detail:
                "legacy configuration projection cannot represent independent Pools and Executors"
                    .to_owned(),
        });
    }
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
        let row = if kind.is_some_and(|kind| kind != ConfigKind::Pool) {
            let Some(row) = ConfigRow::scan_at(key.value(), bytes.value())? else {
                continue;
            };
            row
        } else {
            ConfigRow::decode_at(key.value(), bytes.value())?
        };
        if kind.is_none_or(|kind| kind == row.kind) {
            records.push(config_record(row));
        }
    }
    Ok(CursorPage {
        records,
        next: last.filter(|_| more),
    })
}

#[derive(Clone, Copy)]
pub(crate) enum NativeResourceKind {
    Pool,
    Executor,
}

pub(crate) enum NativeResource {
    Pool(crate::db::entities::AllocationPool),
    Executor(crate::db::entities::AllocationExecutor),
}

pub(crate) fn read_native_resources(
    database: &Database,
    before: Option<ConfigId>,
    limit: usize,
    kind: NativeResourceKind,
) -> Result<CursorPage<NativeResource, ConfigId>, StorageError> {
    let tx = read(database)?;
    let table = tx.open_table(CONFIGS)?;
    let end = before.map_or(u128::MAX, ConfigId::get);
    let mut records = Vec::new();
    let mut last = None;
    let mut more = false;
    for (scanned, entry) in table.range(..end)?.rev().enumerate() {
        if records.len() >= limit.clamp(1, MAX_PAGE) || scanned == MAX_PAGE {
            more = true;
            break;
        }
        let (key, bytes) = entry?;
        let id = key.value();
        last = Some(ConfigId::new(id));
        match bytes.value().first() {
            Some(&transactions::TAG_ALLOCATION_POOL) => {
                let pool = transactions::decode_allocation_pool(id, bytes.value())?;
                if matches!(kind, NativeResourceKind::Pool) && !pool.archived {
                    records.push(NativeResource::Pool(pool));
                }
            }
            Some(&transactions::TAG_ALLOCATION_EXECUTOR) => {
                let executor = transactions::decode_allocation_executor(id, bytes.value())?;
                let pool = table
                    .get(executor.pool.get())?
                    .ok_or_else(|| StorageError::corrupted("Executor references a missing Pool"))?;
                transactions::decode_allocation_pool(executor.pool.get(), pool.value())?;
                if matches!(kind, NativeResourceKind::Executor) && !executor.archived {
                    records.push(NativeResource::Executor(executor));
                }
            }
            _ => {
                let row = ConfigRow::decode_at(id, bytes.value())?;
                if matches!(row.body, ConfigBody::Pool(_)) {
                    return Err(StorageError {
                        kind: super::StorageErrorKind::Other,
                        os_error: None,
                        detail: "legacy combined Pool/Executor cannot be projected as an independent resource".to_owned(),
                    });
                }
            }
        }
    }
    Ok(CursorPage {
        records,
        next: last.filter(|_| more),
    })
}

pub(crate) const MAX_POOL_OCCUPANCY_ENTRIES: usize = 4096;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct PoolOccupancy {
    pub held: u64,
    pub reserved: u64,
    pub running: u64,
    pub stop_requested: u64,
    pub uncertain: u64,
}

pub(crate) struct PoolAllocationPage {
    pub pool: crate::db::entities::AllocationPool,
    pub occupancy: Option<PoolOccupancy>,
    pub allocations: Vec<crate::db::entities::PoolAllocation>,
    pub next: Option<(u128, u128)>,
}

pub(super) fn indexed_allocation(
    allocations: &impl ReadableTable<u128, &'static [u8]>,
    pool: u128,
    executor: u128,
    job: u128,
) -> Result<crate::db::entities::PoolAllocation, StorageError> {
    let bytes = allocations.get(job)?.ok_or_else(|| {
        StorageError::corrupted("active Allocation index references a missing Allocation")
    })?;
    let allocation = transactions::decode_pool_allocation(job, bytes.value())?;
    if allocation.token.job.get() != job
        || allocation.token.pool.get() != pool
        || allocation.token.executor.get() != executor
    {
        return Err(StorageError::corrupted(
            "active Allocation index disagrees with its Allocation's identity",
        ));
    }
    if !allocation.holds_capacity() {
        return Err(StorageError::corrupted(
            "active Allocation index references a released Allocation",
        ));
    }
    Ok(allocation)
}

pub(crate) fn read_pool_allocations(
    database: &Database,
    pool_id: crate::db::entities::ExecutionPoolId,
    after: Option<(u128, u128)>,
    limit: usize,
) -> Result<Option<PoolAllocationPage>, StorageError> {
    use crate::db::entities::AllocationPhase;
    let tx = read(database)?;
    let pool = {
        let configs = tx.open_table(CONFIGS)?;
        let Some(bytes) = configs.get(pool_id.get())? else {
            return Ok(None);
        };
        if bytes.value().first() != Some(&transactions::TAG_ALLOCATION_POOL) {
            return Ok(None);
        }
        transactions::decode_allocation_pool(pool_id.get(), bytes.value())?
    };
    let active = tx.open_table(ACTIVE_ALLOCATIONS)?;
    let allocations = tx.open_table(POOL_ALLOCATIONS)?;
    let pool_key = pool_id.get();
    let high = (pool_key, u128::MAX, u128::MAX);

    let limit = limit.clamp(1, MAX_PAGE);
    let start = match after {
        Some((executor, job)) => Bound::Excluded((pool_key, executor, job)),
        None => Bound::Included((pool_key, 0, 0)),
    };
    let mut page = Vec::new();
    let mut next = None;
    for entry in active.range((start, Bound::Included(high)))? {
        let (_, executor, job) = entry?.0.value();
        if page.len() == limit {
            next = page
                .last()
                .map(|last: &crate::db::entities::PoolAllocation| {
                    (last.token.executor.get(), last.token.job.get())
                });
            break;
        }
        page.push(indexed_allocation(&allocations, pool_key, executor, job)?);
    }

    let mut occupancy = PoolOccupancy::default();
    let mut counted = Some(());
    for (scanned, entry) in active
        .range((Bound::Included((pool_key, 0, 0)), Bound::Included(high)))?
        .enumerate()
    {
        let (_, executor, job) = entry?.0.value();
        if scanned == MAX_POOL_OCCUPANCY_ENTRIES {
            counted = None;
            break;
        }
        let allocation = indexed_allocation(&allocations, pool_key, executor, job)?;
        occupancy.held += 1;
        match allocation.phase {
            AllocationPhase::Reserved => occupancy.reserved += 1,
            AllocationPhase::Running => occupancy.running += 1,
            AllocationPhase::StopRequested => occupancy.stop_requested += 1,
            AllocationPhase::Uncertain => occupancy.uncertain += 1,
            AllocationPhase::Released => {
                return Err(StorageError::corrupted(
                    "active Allocation index references a released Allocation",
                ));
            }
        }
    }
    Ok(Some(PoolAllocationPage {
        pool,
        occupancy: counted.map(|()| occupancy),
        allocations: page,
        next,
    }))
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

pub(crate) struct ProfilePageRecords {
    pub records: Vec<ProfilePageRecord>,
    pub connections: BTreeMap<ConfigId, (String, ConnectionConfig, bool)>,
    pub credentials: BTreeSet<String>,
    pub next_before: Option<ConfigId>,
}

#[allow(clippy::large_enum_variant)]
pub(crate) enum ProfilePageRecord {
    Config(ConfigRecord),
    Executor(crate::db::entities::AllocationExecutor),
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
    let end = before.map_or(u128::MAX, ConfigId::get);
    let limit = limit.max(1);
    let scan = scan.max(limit);
    let mut records = Vec::new();
    let mut last = None;
    let mut more = false;
    for (examined, entry) in configs.range(..end)?.rev().enumerate() {
        if examined >= scan {
            more = true;
            break;
        }
        let (key, bytes) = entry?;
        let id = key.value();
        last = Some(ConfigId::new(id));
        if bytes.value().first() == Some(&transactions::TAG_ALLOCATION_EXECUTOR) {
            let executor = transactions::decode_allocation_executor(id, bytes.value())?;
            let pool_bytes = configs
                .get(executor.pool.get())?
                .ok_or_else(|| StorageError::corrupted("Executor references a missing Pool"))?;
            let pool =
                transactions::decode_allocation_pool(executor.pool.get(), pool_bytes.value())?;
            if executor.enabled && !executor.archived && pool.enabled && !pool.archived {
                records.push(ProfilePageRecord::Executor(executor));
            }
            if records.len() >= limit {
                more = true;
                break;
            }
            continue;
        }
        let Some(row) = ConfigRow::scan_at(id, bytes.value())? else {
            continue;
        };
        if matches!(
            row.body,
            ConfigBody::Model(_)
                | ConfigBody::Agent(_)
                | ConfigBody::Pool(_)
                | ConfigBody::DecisionModel(_)
                | ConfigBody::DecisionAgent(_)
        ) && row.archived_at.is_none()
        {
            records.push(ProfilePageRecord::Config(config_record(row)));
        }
        if records.len() >= limit {
            more = true;
            break;
        }
    }
    if more
        && let Some(last) = last
        && configs.range(..last.get())?.next().is_none()
    {
        more = false;
    }
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
            ConfigBody::Agent(_)
            | ConfigBody::DecisionAgent(_)
            | ConfigBody::Pool(_)
            | ConfigBody::Connection(_) => continue,
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

fn pools_offering(
    rx: &impl TxRead,
    protocol: LlmTransport,
) -> Result<Vec<ExecutorId>, StorageError> {
    pools_offering_capability(rx, ExecutionOperation::LlmRequest, Some(protocol))
}

fn pools_offering_capability(
    rx: &impl TxRead,
    operation: ExecutionOperation,
    protocol: Option<LlmTransport>,
) -> Result<Vec<ExecutorId>, StorageError> {
    {
        let configs = rx.table(CONFIGS)?;
        let mut executors = Vec::new();
        for entry in configs.iter()? {
            let (key, bytes) = entry?;
            if bytes.value().first() == Some(&transactions::TAG_ALLOCATION_EXECUTOR) {
                let executor =
                    transactions::decode_allocation_executor(key.value(), bytes.value())?;
                let pool = configs
                    .get(executor.pool.get())?
                    .ok_or_else(|| StorageError::corrupted("Executor references a missing Pool"))?;
                let pool = transactions::decode_allocation_pool(executor.pool.get(), pool.value())?;
                if executor.enabled
                    && !executor.archived
                    && pool.enabled
                    && !pool.archived
                    && executor.capabilities.contains(&(operation, protocol))
                {
                    executors.push(executor.id);
                    if executors.len() >= MAX_PAGE {
                        break;
                    }
                }
            } else {
                ConfigRow::scan_at(key.value(), bytes.value())?;
            }
        }
        Ok(executors)
    }
}

pub(super) fn candidate_compatible(
    rx: &impl TxRead,
    candidate: crate::db::AdmissionCandidate,
) -> Result<bool, StorageError> {
    let (executor, model, decision) = match candidate {
        crate::db::AdmissionCandidate::Decision(executor, model) => (executor, model, true),
    };
    let configs = rx.table(CONFIGS)?;
    let Some(pool) = configs
        .get(executor.get())?
        .map(|bytes| ConfigRow::decode_at(executor.get(), bytes.value()))
        .transpose()?
    else {
        return Ok(false);
    };
    let Some(model_row) = configs
        .get(model.get())?
        .map(|bytes| ConfigRow::decode_at(model.get(), bytes.value()))
        .transpose()?
    else {
        return Ok(false);
    };
    let capability = match model_row.body {
        ConfigBody::DecisionModel(_) if decision => Some((ExecutionOperation::Decision, None)),
        ConfigBody::Model(model) => {
            let connection = configs
                .get(model.connection_id.get())?
                .map(|bytes| ConfigRow::decode_at(model.connection_id.get(), bytes.value()))
                .transpose()?;
            match connection.map(|row| row.body) {
                Some(ConfigBody::Connection(connection)) => {
                    Some((ExecutionOperation::LlmRequest, Some(connection.protocol)))
                }
                Some(
                    ConfigBody::Model(_)
                    | ConfigBody::Agent(_)
                    | ConfigBody::Pool(_)
                    | ConfigBody::DecisionModel(_)
                    | ConfigBody::DecisionAgent(_),
                )
                | None => None,
            }
        }
        ConfigBody::Connection(_)
        | ConfigBody::Agent(_)
        | ConfigBody::Pool(_)
        | ConfigBody::DecisionModel(_)
        | ConfigBody::DecisionAgent(_) => None,
    };
    Ok(pool.archived_at.is_none()
        && matches!(pool.body, ConfigBody::Pool(body) if body.enabled && capability.is_some_and(|(operation, protocol)| body.offers(operation, protocol))))
}

pub(super) fn compatible_executors(
    rx: &impl TxRead,
    llm_profile: ProfileId,
) -> Result<Vec<ExecutorId>, StorageError> {
    let configs = rx.table(CONFIGS)?;
    let Some(model) = configs
        .get(llm_profile.get())?
        .map(|bytes| ConfigRow::decode_at(llm_profile.get(), bytes.value()))
        .transpose()?
    else {
        return Ok(Vec::new());
    };
    let model_eligible = model.model_enabled && model.archived_at.is_none();
    let ConfigBody::Model(model) = model.body else {
        return Err(StorageError::corrupted("expected a Model configuration"));
    };
    let Some(connection) = configs
        .get(model.connection_id.get())?
        .map(|bytes| ConfigRow::decode_at(model.connection_id.get(), bytes.value()))
        .transpose()?
    else {
        return Err(StorageError::corrupted(
            "Model references a missing Connection",
        ));
    };
    let ConfigBody::Connection(connection) = connection.body else {
        return Err(StorageError::corrupted(
            "Model references a non-Connection configuration",
        ));
    };
    if !model_eligible {
        return Ok(Vec::new());
    }
    drop(configs);
    pools_offering(rx, connection.protocol)
}

pub(crate) struct AgentModelConfig {
    pub config: Option<ConfigRecord>,
    pub executors: Vec<ExecutorId>,
}

pub(crate) fn read_agent_model_configs(
    database: &Database,
    ids: &[ConfigId],
) -> Result<Vec<AgentModelConfig>, StorageError> {
    let rx = read(database)?;
    ids.iter()
        .map(|id| {
            let record = config(&rx, *id)?;
            let executors = if record.as_ref().is_some_and(|record| {
                record.model_enabled
                    && record.archived_at.is_none()
                    && matches!(record.body, ConfigBody::Model(_))
            }) {
                compatible_executors(&rx, (*id).into())?
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UncertainAllocation {
    pub job: JobId,
    pub pool: ExecutionPoolId,
    pub epoch: u64,
    pub since: Timestamp,
}

pub(crate) fn read_uncertain_allocations(
    database: &Database,
) -> Result<Vec<UncertainAllocation>, StorageError> {
    use crate::db::entities::MutableJobPhase;
    let rx = read(database)?;
    let active = rx.open_table(ACTIVE_ALLOCATIONS)?;
    let allocations = rx.open_table(POOL_ALLOCATIONS)?;
    let mut uncertain = Vec::new();
    for entry in active.iter()? {
        let (pool, executor, job) = entry?.0.value();
        let allocation = indexed_allocation(&allocations, pool, executor, job)?;
        if allocation.phase != AllocationPhase::Uncertain {
            continue;
        }
        let record = mutable_job_record_in(&rx, JobId::new(job))?
            .ok_or_else(|| StorageError::corrupted("an Uncertain Allocation has no Job"))?;
        let started = record.job.phase == MutableJobPhase::Executing;
        let shaped = allocation.job_revision == record.job.revision
            && allocation.stop_confirmed_at.is_none()
            && allocation.stop_proof.is_none()
            && allocation.started_at.is_some() == started
            && (started || record.job.phase == MutableJobPhase::Queued);
        if !shaped {
            return Err(StorageError::corrupted(
                "an Uncertain Allocation contradicts its Job",
            ));
        }
        let Some(outcome) = record
            .outcome
            .filter(|outcome| outcome.kind == OutcomeKind::Unknown)
        else {
            return Err(StorageError::corrupted(
                "an Uncertain Allocation has no Unknown outcome",
            ));
        };
        uncertain.push(UncertainAllocation {
            job: JobId::new(job),
            pool: allocation.token.pool,
            epoch: allocation.token.epoch,
            since: outcome.at,
        });
    }
    Ok(uncertain)
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
            .map(|bytes| transactions::decode_pool_allocation(token.job.get(), bytes.value()))
            .transpose()?
            .is_some_and(|allocation| allocation.token == token),
        Probe::MutableJobStarted(token, revision) => {
            let job = rx
                .open_table(JOBS)?
                .get(token.job.get())?
                .map(|bytes| transactions::decode_mutable_job(token.job.get(), bytes.value()))
                .transpose()?;
            let allocation = rx
                .open_table(POOL_ALLOCATIONS)?
                .get(token.job.get())?
                .map(|bytes| transactions::decode_pool_allocation(token.job.get(), bytes.value()))
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
            .open_table(JOB_STATES)?
            .get(job.get())?
            .map(|bytes| JobState::decode_at(job.get(), bytes.value()))
            .transpose()?
            .is_some_and(|state| state.outcome.is_some()),
    })
}

#[derive(Clone, Debug, PartialEq)]
pub struct ConversationEntry {
    pub root_job: JobId,
    pub tip_job: JobId,
    pub agent: Option<ConfigId>,
    pub title: String,
    pub project: Option<ProjectId>,
    pub state: String,
    pub on_hold: Option<Actor>,
    pub tip_settlement_state: String,
    pub tip_created_at: Timestamp,
    pub job_state: String,
    pub has_children: bool,
}
#[derive(Clone, Debug, PartialEq)]
pub struct ConversationsPage {
    pub conversations: Vec<ConversationEntry>,
    pub next_before: Option<JobId>,
    pub has_more: bool,
}

fn conversation_job_state(
    rx: &ReadTransaction,
    conversation: JobId,
    tip: JobId,
) -> Result<String, StorageError> {
    let mut awaiting_user = false;
    for entry in rx.open_table(OPEN_APPROVALS)?.range(
        (conversation.get(), 0, 0, 0)..=(conversation.get(), u128::MAX, u32::MAX, u32::MAX),
    )? {
        let (_, state) = entry?;
        if state.value() == 0 {
            awaiting_user = true;
            break;
        }
    }
    Ok(if read_state(rx, conversation)?.outcome.is_some() {
        turn_state(rx, conversation)?
    } else if awaiting_user {
        "waiting_for_user"
    } else if let Some((active, _, _)) = active_job(rx, conversation)? {
        turn_state(rx, active)?
    } else {
        turn_state(rx, tip)?
    }
    .to_owned())
}

fn has_visible_child(rx: &ReadTransaction, conversation: JobId) -> Result<bool, StorageError> {
    for entry in rx
        .open_table(CHILD_GROUPS)?
        .range((conversation.get(), 0)..=(conversation.get(), u128::MAX))?
    {
        let child = JobId::new(entry?.0.value().1);
        if let Some(row) = read_job_row(rx, child)?
            && let JobKind::Group {
                origin: Origin::User | Origin::Agent { .. },
                ..
            } = row.kind
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn conversation_title(
    rx: &ReadTransaction,
    conversation: JobId,
    metadata: &JobControl,
) -> Result<String, StorageError> {
    Ok(match &metadata.title {
        Some(title) => title.clone(),
        None => match turn_first(&rx.open_table(TURNS)?, conversation)? {
            Some(first) => first_entry_text(rx, first)?,
            None => String::new(),
        },
    })
}

fn conversation_summary(
    rx: &ReadTransaction,
    conversation: JobId,
    tip: JobId,
) -> Result<ConversationEntry, StorageError> {
    let row = read_job_row(rx, conversation)?
        .ok_or_else(|| StorageError::corrupted("missing conversation"))?;
    let JobKind::Group { agent, project, .. } = row.kind else {
        return Err(StorageError::corrupted(
            "recent entry is not a conversation",
        ));
    };
    let metadata = rx
        .open_table(JOB_CONTROLS)?
        .get(conversation.get())?
        .map(|bytes| JobControl::decode(bytes.value()))
        .transpose()?
        .ok_or_else(|| StorageError::corrupted("missing conversation state"))?;
    let title = conversation_title(rx, conversation, &metadata)?;
    let state = read_state(rx, conversation)?;
    let on_hold = metadata.on_hold.map(|code| {
        if code == super::job_tree::HELD_BY_AGENT {
            Actor::Agent {
                conversation: row.parent_id.unwrap_or(conversation),
            }
        } else {
            Actor::User
        }
    });
    let tip_state = read_state(rx, tip)?;
    let tip_created_at = mutable_job_record_in(rx, tip)?
        .ok_or_else(|| StorageError::corrupted("missing tip"))?
        .job
        .created_at;
    Ok(ConversationEntry {
        root_job: conversation,
        tip_job: tip,
        agent,
        project,
        title,
        state: if state.outcome.is_some() {
            "settled"
        } else if metadata.closing.is_some() {
            "closing"
        } else {
            "open"
        }
        .to_owned(),
        on_hold,
        tip_settlement_state: tip_state
            .outcome
            .map_or("unsettled", |outcome| match outcome.kind {
                OutcomeKind::Completed => "completed",
                OutcomeKind::Failed => "failed",
                OutcomeKind::Cancelled => "cancelled",
                OutcomeKind::Unknown => "unknown",
            })
            .to_owned(),
        tip_created_at,
        job_state: conversation_job_state(rx, conversation, tip)?,
        has_children: has_visible_child(rx, conversation)?,
    })
}
#[derive(Clone, Debug, PartialEq)]
pub struct ChildConversationEntry {
    pub conversation: ConversationEntry,
    pub origin: Origin,
    pub keep_open: bool,
    pub recurrence: Option<Recurrence>,
    pub job_state: String,
}
pub(crate) fn read_conversation_children(
    database: &Database,
    parent: JobId,
    before: Option<JobId>,
    limit: usize,
) -> Result<Vec<ChildConversationEntry>, StorageError> {
    let rx = read(database)?;
    let mut children = Vec::new();
    for entry in rx
        .open_table(CHILD_GROUPS)?
        .range((parent.get(), 0)..(parent.get(), before.map_or(u128::MAX, JobId::get)))?
        .rev()
        .take(limit.min(MAX_PAGE))
    {
        let child = JobId::new(entry?.0.value().1);
        let row = read_job_row(&rx, child)?
            .ok_or_else(|| StorageError::corrupted("child conversation missing"))?;
        let JobKind::Group {
            origin, keep_open, ..
        } = row.kind
        else {
            return Err(StorageError::corrupted("child is not a conversation"));
        };
        let tip = turn_head(&rx.open_table(TURNS)?, child)?
            .ok_or_else(|| StorageError::corrupted("child tip missing"))?;
        let conversation = conversation_summary(&rx, child, tip)?;
        children.push(ChildConversationEntry {
            job_state: conversation.job_state.clone(),
            conversation,
            origin,
            keep_open,
            recurrence: rx
                .open_table(RECURRENCES)?
                .get(child.get())?
                .map(|bytes| Recurrence::decode(bytes.value()))
                .transpose()?,
        });
    }
    Ok(children)
}

pub(crate) fn read_conversations(
    database: &Database,
    before: Option<JobId>,
    limit: usize,
) -> Result<ConversationsPage, StorageError> {
    conversation_page(database, before, limit, &[])
}

pub(crate) fn read_conversation_search(
    database: &Database,
    terms: &[String],
    before: Option<JobId>,
    limit: usize,
) -> Result<ConversationsPage, StorageError> {
    conversation_page(database, before, limit, terms)
}

fn conversation_page(
    database: &Database,
    before: Option<JobId>,
    limit: usize,
    terms: &[String],
) -> Result<ConversationsPage, StorageError> {
    let rx = read(database)?;
    let groups = rx.open_table(CONVERSATIONS)?;
    let limit = limit.clamp(1, 100);
    let mut conversations = Vec::new();
    let mut last = None;
    let mut last_examined = None;
    let mut has_more = false;
    for (examined, item) in groups
        .range(..before.map_or(u128::MAX, JobId::get))?
        .rev()
        .enumerate()
    {
        if !terms.is_empty() && examined == 5_000 {
            has_more = true;
            last = last_examined;
            break;
        }
        let conversation = JobId::new(item?.0.value());
        last_examined = Some(conversation);
        let row = read_job_row(&rx, conversation)?
            .ok_or_else(|| StorageError::corrupted("missing conversation"))?;
        if !indexes::searchable_conversation(&row) {
            continue;
        }
        let metadata = read_job_control(&rx, conversation)?;
        if metadata.hidden_at.is_some() {
            continue;
        }
        let Some(tip) = turn_head(&rx.open_table(TURNS)?, conversation)? else {
            continue;
        };
        let matches = if terms.is_empty() {
            true
        } else if let JobKind::Group { .. } = &row.kind {
            let title = conversation_title(&rx, conversation, &metadata)?;
            conversation_matches(&rx, conversation, &title, terms)?
        } else {
            return Err(StorageError::corrupted(
                "recent entry is not a conversation",
            ));
        };
        if matches {
            if conversations.len() == limit {
                has_more = true;
                break;
            }
            last = Some(conversation);
            conversations.push(conversation_summary(&rx, conversation, tip)?);
            if !terms.is_empty() && conversations.len() == limit {
                has_more = true;
                break;
            }
        }
    }
    Ok(ConversationsPage {
        conversations,
        next_before: last.filter(|_| has_more),
        has_more,
    })
}

fn conversation_matches(
    rx: &ReadTransaction,
    conversation: JobId,
    title: &str,
    terms: &[String],
) -> Result<bool, StorageError> {
    let title = title.to_lowercase();
    let index = rx.open_table(CONVERSATION_SEARCH)?;
    for term in terms {
        if title.contains(term) {
            continue;
        }
        let chars = term.chars().take(3).collect::<Vec<_>>();
        let gram = indexes::search_gram(&chars);
        let mut matched = false;
        for candidate in
            index.range((conversation.get(), gram, 0)..=(conversation.get(), gram, u128::MAX))?
        {
            let (_, _, turn) = candidate?.0.value();
            let turn = JobId::new(turn);
            {
                let job = mutable_job_record_in(rx, turn)?
                    .ok_or_else(|| StorageError::corrupted("search turn missing"))?;
                if job.job.parent_id != conversation
                    || job.job.phase == crate::db::entities::MutableJobPhase::Unsubmitted
                {
                    return Err(StorageError::corrupted(
                        "search turn has invalid conversation",
                    ));
                }
                let entry = Entry::UserMessage {
                    text: job.job.input.text().to_owned(),
                    attachments: job.job.input.attachments().to_vec(),
                };
                if indexes::search_input_text(&rx.open_table(ATTACHMENTS)?, &entry)?
                    .is_some_and(|text| text.to_lowercase().contains(term))
                {
                    matched = true;
                    break;
                }
            }
        }
        if !matched {
            return Ok(false);
        }
    }
    Ok(true)
}
pub(crate) struct QueueCounts {
    pub(crate) pending: u64,
    pub(crate) ready: u64,
    pub(crate) delayed: u64,
    pub(crate) running: u64,
    pub(crate) held: u64,
}

pub(crate) fn queue_counts(
    database: &Database,
    for_update: bool,
) -> Result<QueueCounts, StorageError> {
    let rx = read(database)?;
    queue_counts_in(&rx, for_update)
}

fn queue_counts_in(rx: &ReadTransaction, for_update: bool) -> Result<QueueCounts, StorageError> {
    use crate::db::entities::AllocationPhase;
    let pending_table = rx.open_table(PENDING)?;
    let delayed_table = rx.open_table(DELAYED)?;
    let (pending, delayed) = if for_update {
        let clock = recovery::promotion_clock();
        let jobs = rx.open_table(JOBS)?;
        let mut pending = 0;
        for entry in pending_table.iter()? {
            let id = entry?.0.value();
            if jobs.get(id)?.is_none() {
                return Err(StorageError::corrupted("pending Job missing"));
            }
            pending += 1;
        }
        let mut delayed = 0;
        for entry in delayed_table.range(..=(clock, u128::MAX))? {
            entry?;
            delayed += 1;
        }
        if rx
            .open_table(RECURRENCE_DUE)?
            .first()?
            .is_some_and(|(key, _)| key.value().0 <= clock)
        {
            delayed += 1;
        }
        (pending, delayed)
    } else {
        (pending_table.len()?, delayed_table.len()?)
    };
    let ready = rx.open_table(MUTABLE_READY)?.len()?;
    let allocations = rx.open_table(POOL_ALLOCATIONS)?;
    let (mut running, mut held) = (0, 0);
    for entry in rx.open_table(ACTIVE_ALLOCATIONS)?.iter()? {
        let (_, _, job) = entry?.0.value();
        let bytes = allocations.get(job)?.ok_or_else(|| {
            StorageError::corrupted("active Allocation index references a missing Allocation")
        })?;
        let allocation = transactions::decode_pool_allocation(job, bytes.value())?;
        match allocation.phase {
            AllocationPhase::Running | AllocationPhase::StopRequested => running += 1,
            AllocationPhase::Reserved | AllocationPhase::Uncertain => {}
            AllocationPhase::Released => {
                return Err(StorageError::corrupted(
                    "active Allocation index references a released Allocation",
                ));
            }
        }
        held += 1;
    }
    Ok(QueueCounts {
        pending,
        ready,
        delayed,
        running,
        held,
    })
}

pub(crate) fn read_tool_stats(
    database: &Database,
) -> Result<Vec<crate::db::entities::ToolStatRecord>, StorageError> {
    let rx = read(database)?;
    let table = rx.open_table(TOOL_STATS)?;
    let mut stats = Vec::new();
    for entry in table.iter()? {
        let (key, value) = entry?;
        let (admitted, refused_arguments, completed, failed, duration_us_total, duration_us_max) =
            value.value();
        stats.push(crate::db::entities::ToolStatRecord {
            name: key.value().to_owned(),
            admitted,
            refused_arguments,
            completed,
            failed,
            duration_us_total,
            duration_us_max,
        });
    }
    Ok(stats)
}

pub const MAX_USAGE_SCAN: usize = 100_000;
const MAX_USAGE_PAGE_GROUPS: usize = 512;

fn resolve_model_name(
    rx: &ReadTransaction,
    provider: u128,
    model_hash: [u8; 32],
) -> Result<Option<String>, StorageError> {
    let Some(job) = rx
        .open_table(USAGE_MODEL)?
        .get((provider, model_hash))?
        .map(|job| JobId::new(job.value()))
    else {
        return Ok(None);
    };
    if let Some(value) = rx.open_table(JOBS)?.get(job.get())?
        && matches!(value.value(), [TAG_JOB, codec::RECORD_VERSION, ..])
    {
        let row = transactions::decode_mutable_job(job.get(), value.value())?;
        let (actual_provider, model) = match row.frozen {
            Some(crate::db::entities::MutableFrozenConfiguration::Generative(frozen)) => {
                (frozen.llm.provider.id.get(), frozen.llm.config.model)
            }
            Some(crate::db::entities::MutableFrozenConfiguration::Decision(frozen)) => {
                (frozen.connection_ref.id.get(), frozen.model.model)
            }
            None => return Err(StorageError::corrupted("usage model has no frozen target")),
        };
        if actual_provider != provider
            || ContentDigest::of(model.as_bytes()).as_bytes() != &model_hash
        {
            return Err(StorageError::corrupted(
                "usage model references a different identity",
            ));
        }
        return Ok(Some(model));
    }
    let row = read_job_row(rx, job)?
        .ok_or_else(|| StorageError::corrupted("usage model references a missing job"))?;
    let identity = turn_usage_identity_of(&rx.open_table(JOBS)?, &row)?
        .filter(|identity| {
            identity.provider == provider
                && ContentDigest::of(identity.model.as_bytes()).as_bytes() == &model_hash
        })
        .ok_or_else(|| StorageError::corrupted("usage model references a different identity"))?;
    Ok(Some(identity.model))
}

type UsageGroupKey = (i64, u128, u128, [u8; 32]);
type UsageCounters = (u64, u64, u64, u64, u64, u64, u64);

pub(crate) fn read_usage_summary(
    database: &Database,
    filter: &crate::db::entities::UsageSummaryFilter,
    after: Option<crate::db::entities::UsageCursor>,
) -> Result<crate::db::entities::UsageSummaryResponse, StorageError> {
    let (row_budget, group_budget) = (MAX_USAGE_SCAN, MAX_USAGE_PAGE_GROUPS);
    use crate::db::entities::{USAGE_SUMMARY_HOUR_MICROS as HOUR, UsageSummaryResponse};

    let from_hour = filter
        .from_us
        .map(|us| us.div_euclid(HOUR))
        .unwrap_or(i64::MIN);
    let until_end = match filter.until_us {
        None => None,
        Some(us) => {
            let hour = us.div_euclid(HOUR);
            Some(if us.rem_euclid(HOUR) == 0 {
                hour
            } else {
                hour.checked_add(1).ok_or_else(|| {
                    StorageError::corrupted("usage summary until_us overflows its hour bucket")
                })?
            })
        }
    };
    let until_end_us = match until_end {
        Some(hour) => Some(hour.checked_mul(HOUR).ok_or_else(|| {
            StorageError::corrupted("usage summary until_us overflows its hour bucket")
        })?),
        None => None,
    };
    let until_hour = match until_end {
        Some(hour) => hour - 1,
        None => i64::MAX,
    };

    let requested_from_us = match filter.from_us {
        Some(us) => Some(us.div_euclid(HOUR).checked_mul(HOUR).ok_or_else(|| {
            StorageError::corrupted("usage summary from_us underflows its hour bucket")
        })?),
        None => None,
    };

    if from_hour > until_hour {
        return Ok(UsageSummaryResponse {
            from_us: requested_from_us.unwrap_or(0),
            until_us: until_end_us.unwrap_or(0),
            complete: true,
            resume_after: None,
            rows: Vec::new(),
        });
    }

    let UsageScan {
        rows,
        earliest_hour,
        latest_hour,
        next,
    } = scan_usage_totals(
        database,
        filter,
        from_hour,
        until_hour,
        None,
        after,
        UsageBudget {
            rows: row_budget,
            groups: group_budget,
        },
    )?;
    let rows = rows.into_iter().map(|bucket| bucket.row).collect();

    let effective_from_us = requested_from_us
        .or_else(|| earliest_hour.map(|hour| hour.saturating_mul(HOUR)))
        .unwrap_or(0);

    let effective_until_us = match until_end_us {
        Some(until) => until,
        None => latest_hour
            .and_then(|hour| hour.checked_add(1))
            .map_or(0, |hour| hour.saturating_mul(HOUR)),
    };

    Ok(UsageSummaryResponse {
        from_us: effective_from_us,
        until_us: effective_until_us,
        complete: next.is_none(),
        resume_after: next,
        rows,
    })
}

pub(crate) fn read_usage_buckets(
    database: &Database,
    filter: &crate::db::entities::UsageSummaryFilter,
    bucketing: &crate::db::entities::UsageBucketing,
    after: Option<crate::db::entities::UsageCursor>,
) -> Result<crate::db::entities::UsageBucketResponse, StorageError> {
    let (row_budget, group_budget) = (MAX_USAGE_SCAN, MAX_USAGE_PAGE_GROUPS);
    use crate::db::entities::{USAGE_SUMMARY_HOUR_MICROS as HOUR, UsageBucketResponse};

    if bucketing.offset_minutes % 60 != 0 || !(-840..=840).contains(&bucketing.offset_minutes) {
        return Err(StorageError::corrupted("invalid usage bucket offset"));
    }
    let from_us = filter
        .from_us
        .and_then(|us| bucketing.start_of(us))
        .ok_or_else(|| StorageError::corrupted("usage buckets require a representable from_us"))?;
    let until_us = filter
        .until_us
        .and_then(|us| us.checked_sub(1))
        .and_then(|us| bucketing.end_of(us))
        .ok_or_else(|| StorageError::corrupted("usage buckets require a representable until_us"))?;
    let scan = scan_usage_totals(
        database,
        filter,
        from_us.div_euclid(HOUR),
        until_us.div_euclid(HOUR) - 1,
        Some(bucketing),
        after,
        UsageBudget {
            rows: row_budget,
            groups: group_budget,
        },
    )?;
    Ok(UsageBucketResponse {
        from_us,
        until_us,
        complete: scan.next.is_none(),
        resume_after: scan.next,
        rows: scan.rows,
    })
}

#[derive(Clone, Copy)]
struct UsageBudget {
    rows: usize,
    groups: usize,
}

#[derive(Default)]
struct UsageScan {
    rows: Vec<crate::db::entities::UsageBucketRow>,
    earliest_hour: Option<i64>,
    latest_hour: Option<i64>,
    next: Option<crate::db::entities::UsageCursor>,
}

fn scan_usage_totals(
    database: &Database,
    filter: &crate::db::entities::UsageSummaryFilter,
    from_hour: i64,
    until_hour: i64,
    bucketing: Option<&crate::db::entities::UsageBucketing>,
    after: Option<crate::db::entities::UsageCursor>,
    budget: UsageBudget,
) -> Result<UsageScan, StorageError> {
    use crate::db::entities::{
        USAGE_SUMMARY_HOUR_MICROS as HOUR, UsageBucketRow, UsageCursor, UsageSummaryRow,
    };

    let rx = read(database)?;
    let table = rx.open_table(USAGE_TOTALS)?;
    if from_hour > until_hour || budget.rows == 0 || budget.groups == 0 {
        return Ok(UsageScan::default());
    }
    let range_start = (from_hour, 0, 0, [0u8; 32]);
    let end = (until_hour, u128::MAX, u128::MAX, [0xffu8; 32]);

    let configs = rx.open_table(CONFIGS)?;
    let mut agents = std::collections::HashMap::new();
    let mut matches_agent = |agent: u128| -> Result<bool, StorageError> {
        if filter.agent.is_some_and(|target| agent != target) {
            return Ok(false);
        }
        if !filter.generative_only || agent == 0 {
            return Ok(true);
        }
        if let Some(generative) = agents.get(&agent) {
            return Ok(*generative);
        }
        let bytes = configs
            .get(agent)?
            .ok_or_else(|| StorageError::corrupted("usage Agent missing"))?;
        let generative = match ConfigRow::decode_at(agent, bytes.value())?.kind {
            ConfigKind::Agent => true,
            ConfigKind::DecisionAgent => false,
            ConfigKind::Connection
            | ConfigKind::Model
            | ConfigKind::Pool
            | ConfigKind::DecisionModel => {
                return Err(StorageError::corrupted(
                    "usage Agent has an incompatible kind",
                ));
            }
        };
        agents.insert(agent, generative);
        Ok(generative)
    };
    let mut find_hour = |rev: bool| -> Result<Option<i64>, StorageError> {
        let mut iter = table.range(range_start..=end)?;
        while let Some(entry) = if rev { iter.next_back() } else { iter.next() } {
            let key = entry?.0.value();
            if matches_agent(key.1)? {
                return Ok(Some(key.0));
            }
        }
        Ok(None)
    };
    let earliest_hour = if filter.from_us.is_none() {
        find_hour(false)?
    } else {
        None
    };
    let latest_hour = if filter.until_us.is_none() {
        find_hour(true)?
    } else {
        None
    };

    let start = match after {
        Some(cursor) => {
            let key = (
                cursor.hour,
                cursor.agent,
                cursor.provider,
                cursor.model_hash,
            );
            if key < range_start {
                Bound::Included(range_start)
            } else {
                Bound::Excluded(key)
            }
        }
        None => Bound::Included(range_start),
    };

    let mut grouped: std::collections::BTreeMap<UsageGroupKey, UsageCounters> =
        std::collections::BTreeMap::new();
    let mut last = None;
    let mut stopped = false;

    for (examined, entry) in table.range((start, Bound::Included(end)))?.enumerate() {
        if examined >= budget.rows {
            stopped = true;
            break;
        }
        let (key, value) = entry?;
        let (hour, agent, provider, model_hash) = key.value();
        let cursor = UsageCursor {
            hour,
            agent,
            provider,
            model_hash,
        };
        if !matches_agent(agent)? {
            last = Some(cursor);
            continue;
        }

        let bucket_us = match bucketing {
            Some(bucketing) => hour
                .checked_mul(HOUR)
                .and_then(|us| bucketing.start_of(us))
                .ok_or_else(|| StorageError::corrupted("usage bucket overflows"))?,
            None => hour,
        };
        let group = (
            if bucketing.is_some() { bucket_us } else { 0 },
            agent,
            provider,
            model_hash,
        );
        if !grouped.contains_key(&group) && grouped.len() >= budget.groups {
            stopped = true;
            break;
        }

        last = Some(cursor);

        let (reqs, comp, inp, out, cr, cw, reas) = value.value();
        let slot = grouped.entry(group).or_insert((0, 0, 0, 0, 0, 0, 0));
        slot.0 = slot.0.saturating_add(reqs);
        slot.1 = slot.1.saturating_add(comp);
        slot.2 = slot.2.saturating_add(inp);
        slot.3 = slot.3.saturating_add(out);
        slot.4 = slot.4.saturating_add(cr);
        slot.5 = slot.5.saturating_add(cw);
        slot.6 = slot.6.saturating_add(reas);
    }

    let next = if stopped {
        match last {
            Some(cursor) => {
                let key = (
                    cursor.hour,
                    cursor.agent,
                    cursor.provider,
                    cursor.model_hash,
                );
                table
                    .range((Bound::Excluded(key), Bound::Included(end)))?
                    .next()
                    .transpose()?
                    .map(|_| cursor)
            }
            None => None,
        }
    } else {
        None
    };

    let mut model_cache: std::collections::HashMap<(u128, [u8; 32]), Option<String>> =
        std::collections::HashMap::new();
    let mut rows = Vec::with_capacity(grouped.len());

    for ((bucket_us, agent, provider, model_hash), (reqs, comp, inp, out, cr, cw, reas)) in grouped
    {
        let model = match model_cache.entry((provider, model_hash)) {
            std::collections::hash_map::Entry::Occupied(e) => e.get().clone(),
            std::collections::hash_map::Entry::Vacant(e) => {
                let resolved = resolve_model_name(&rx, provider, model_hash)?;
                e.insert(resolved.clone());
                resolved
            }
        };
        rows.push(UsageBucketRow {
            bucket_us,
            row: UsageSummaryRow {
                agent,
                provider,
                model,
                requests: reqs,
                complete_requests: comp,
                input_tokens: inp,
                output_tokens: out,
                cache_read_tokens: cr,
                cache_write_tokens: cw,
                reasoning_tokens: reas,
            },
        });
    }

    Ok(UsageScan {
        rows,
        earliest_hour,
        latest_hour,
        next,
    })
}

fn turn_entries(
    rx: &ReadTransaction,
    turn: JobId,
) -> Result<Vec<(u32, JobInputEntry)>, StorageError> {
    effective_entries(job_entries_of(&rx.open_table(JOB_INPUTS)?, turn)?)
}

pub(super) fn job_entries_of(
    inputs: &impl ReadableTable<(u128, u32), &'static [u8]>,
    turn: JobId,
) -> Result<Vec<(u32, JobInputEntry)>, StorageError> {
    inputs
        .range((turn.get(), 0)..=(turn.get(), u32::MAX))?
        .map(|entry| {
            let (key, value) = entry?;
            Ok((key.value().1, JobInputEntry::decode(value.value())?))
        })
        .collect()
}

pub(super) fn effective_entries(
    entries: Vec<(u32, JobInputEntry)>,
) -> Result<Vec<(u32, JobInputEntry)>, StorageError> {
    crate::db::entities::effective_entries(entries).map_err(StorageError::corrupted)
}

pub(crate) fn read_notices(
    database: &Database,
    conversation: JobId,
    after: u32,
    limit: usize,
) -> Result<Vec<(u32, Notice)>, StorageError> {
    let rx = read(database)?;
    if after == u32::MAX {
        return Ok(Vec::new());
    }
    rx.open_table(NOTICES)?
        .range((conversation.get(), after + 1)..=(conversation.get(), u32::MAX))?
        .take(limit.min(MAX_PAGE))
        .map(|entry| {
            let (key, value) = entry?;
            Ok((key.value().1, Notice::decode(value.value())?))
        })
        .collect()
}

const TURN_PAGE_OUTPUTS: usize = 1000;

pub(crate) fn read_conversation_limits(
    database: &Database,
    conversation: JobId,
) -> Result<Option<(JobLimits, u8)>, StorageError> {
    let rx = read(database)?;
    let mut current = conversation;
    let mut limits = JobLimits::CEILING;
    for depth in 0..=crate::db::entities::MAX_JOB_DEPTH {
        let Some(row) = read_job_row(&rx, current)? else {
            return Ok(None);
        };
        if !row.kind.is_group() {
            return Ok(None);
        }
        let own = read_job_control(&rx, current)?.limits;
        if let Some(own) = own {
            limits = limits.intersect(own);
        }
        match row.parent_id {
            Some(parent) => current = parent,
            None => return Ok(Some((limits.intersect(own.unwrap_or_default()), depth))),
        }
    }
    Err(StorageError::corrupted(
        "conversation ancestry exceeds the hard depth limit",
    ))
}

pub(super) fn global_compaction_settings(
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

pub(super) fn global_outbound_headers(
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

pub(super) fn effective_context_policy(
    meta: &impl ReadableTable<&'static str, &'static [u8]>,
) -> Result<
    (
        crate::db::entities::CompactionPolicy,
        u64,
        Option<ConfigRef>,
    ),
    StorageError,
> {
    let mut settings = global_compaction_settings(meta)?;
    if !settings.prediction_enabled
        || matches!(
            settings.policy.mode,
            crate::db::entities::CompactionMode::Off
        )
    {
        settings.policy.predictive = None;
    }
    Ok((settings.policy, settings.revision, None))
}

fn mutable_context_usage(
    rx: &ReadTransaction,
    conversation: JobId,
    start: JobId,
) -> Result<(usize, u32, u32), StorageError> {
    use crate::db::entities::MutableJobPhase;
    let mut cursor = Some(start);
    let mut seen = BTreeSet::new();
    let mut total = 0usize;
    let mut growth = 0usize;
    let mut settled = 0u32;
    let outputs = rx.open_table(JOB_OUTPUTS)?;
    let allocations = rx.open_table(POOL_ALLOCATIONS)?;
    while let Some(turn) = cursor {
        if !seen.insert(turn) {
            return Err(StorageError::corrupted("context ancestry cycle"));
        }
        if seen.len() > 4096 {
            return Ok((usize::MAX, u32::try_from(growth).unwrap_or(u32::MAX), 4096));
        }
        let record = mutable_job_record_in(rx, turn)?
            .ok_or_else(|| StorageError::corrupted("context turn missing"))?;
        if record.job.parent_id != conversation || record.job.phase == MutableJobPhase::Unsubmitted
        {
            return Err(StorageError::corrupted("invalid context turn relationship"));
        }
        let mut bytes = 16usize
            .saturating_add(record.job.input.text().len())
            .saturating_add(record.job.input.attachments().len().saturating_mul(4800));
        let ended_unstarted = record.job.phase == MutableJobPhase::Queued
            && unstarted_settlement_in(
                rx,
                &record,
                allocations
                    .get(turn.get())?
                    .map(|row| transactions::decode_pool_allocation(turn.get(), row.value()))
                    .transpose()?
                    .as_ref(),
            )?
            .is_some();
        for entry in outputs.range((turn.get(), 0)..=(turn.get(), u32::MAX))? {
            if record.job.phase != MutableJobPhase::Executing && !ended_unstarted {
                return Err(StorageError::corrupted("Job has output before execution"));
            }
            let (key, value) = entry?;
            let output = decode_output(key.value(), value.value())?;
            if output.kind == OutputKind::Result {
                bytes = bytes.saturating_add(output.display_text().as_ref().map_or(0, String::len));
            }
        }
        let tokens = bytes.div_ceil(3);
        total = total.saturating_add(tokens);
        let stopped = match record.outcome.as_ref().map(|outcome| outcome.kind) {
            Some(OutcomeKind::Completed | OutcomeKind::Failed | OutcomeKind::Cancelled) => true,
            Some(OutcomeKind::Unknown) => {
                let allocation = allocations.get(turn.get())?.ok_or_else(|| {
                    StorageError::corrupted("unknown Job has no allocation evidence")
                })?;
                let allocation =
                    transactions::decode_pool_allocation(turn.get(), allocation.value())?;
                if allocation.token.job != turn || allocation.job_revision != record.job.revision {
                    return Err(StorageError::corrupted("invalid context allocation"));
                }
                allocation.stop_confirmed_at.is_some()
            }
            None => false,
        };
        if stopped {
            settled = settled.saturating_add(1);
            if settled <= 8 {
                growth = growth.max(tokens);
            }
        }
        cursor = record.job.previous;
    }
    Ok((total, u32::try_from(growth).unwrap_or(u32::MAX), settled))
}

pub(crate) struct ContextWorkState {
    pub job: JobId,
    pub conversation: JobId,
    pub state: &'static str,
    pub details: ContextWorkDetails,
}
pub(crate) enum ContextWorkDetails {
    Summary {
        summarizer: ConfigId,
        failure: Option<String>,
        automatic: bool,
        stage: &'static str,
        chain: Option<JobId>,
        prediction: Option<JobId>,
        reserved_tokens: u32,
        reserved_requests: u32,
        source: ContentDigest,
        first: crate::db::entities::SourceBoundary,
        through: crate::db::entities::SourceBoundary,
        turn_count: usize,
    },
}

pub(crate) struct ContextState {
    pub policy: crate::db::entities::CompactionPolicy,
    pub stored_override: Option<crate::db::entities::CompactionPolicy>,
    pub policy_source: &'static str,
    pub revision: u64,
    pub model: Option<FrozenLlmConfig>,
    pub estimated_tokens: Option<u64>,
    pub growth: u32,
    pub predictor: Option<ConfigId>,
    pub predictor_model: Option<ConfigId>,
    pub prediction_unavailable: Option<String>,
    pub last_use: Option<(JobId, u32, Option<JobId>)>,
    pub steer_blocked_by: Option<&'static str>,
    pub work: Vec<ContextWorkState>,
    pub next_before: Option<JobId>,
}

pub(crate) fn read_context_state(
    database: &Database,
    conversation: JobId,
    before: Option<JobId>,
    limit: usize,
) -> Result<Option<ContextState>, StorageError> {
    let rx = read(database)?;
    let Some(owner) = read_job_row(&rx, conversation)? else {
        return Ok(None);
    };
    if !owner.kind.is_group() {
        return Ok(None);
    }
    let (mut policy, revision, _profile) = effective_context_policy(&rx.open_table(META)?)?;
    let tip = turn_head(&rx.open_table(TURNS)?, conversation)?;
    let tip_details = tip
        .map(|tip| mutable_job_details_in(&rx, tip, None, false))
        .transpose()?
        .flatten();
    let active = tip_details
        .as_ref()
        .and_then(|details| match &details.record.job.frozen {
            Some(crate::db::entities::MutableFrozenConfiguration::Generative(configuration)) => {
                Some(configuration.clone())
            }
            Some(crate::db::entities::MutableFrozenConfiguration::Decision(_)) | None => None,
        });
    let model = active.as_ref().map(|frozen| frozen.llm.clone());
    let (raw, growth, _) = tip
        .map(|tip| mutable_context_usage(&rx, conversation, tip))
        .transpose()?
        .unwrap_or_default();
    let settings = global_compaction_settings(&rx.open_table(META)?)?;
    let project = match owner.kind {
        JobKind::Group { project, .. } => project,
        JobKind::Execution { .. } => None,
    };
    let decision = project
        .map(|id| {
            rx.open_table(PROJECTS)?
                .get(id.get())?
                .map(|bytes| decode_project(id.get(), bytes.value()))
                .transpose()?
                .ok_or_else(|| StorageError::corrupted("context project missing"))
        })
        .transpose()?
        .map(|project| project.decision_policy())
        .unwrap_or_default();
    let mut predictor = None;
    let mut predictor_model = None;
    let previous_binding = policy.predictive.take();
    let prediction_unavailable = if !settings.prediction_enabled {
        Some("prediction is disabled in global settings".to_owned())
    } else if matches!(policy.mode, crate::db::entities::CompactionMode::Off) {
        Some("automatic compaction is disabled".to_owned())
    } else if !decision.enabled {
        Some("prediction is disabled by the project decision configuration".to_owned())
    } else if let Some(active) = &active {
        let resolved = crate::service::jobs::ConfigurationResolver::new(|id| {
            read_config_record(&rx, id).map_err(|error| BatchError(Failure::Storage(error)))
        })
        .project_decision(&decision, active);
        match resolved {
            Ok(Some(frozen)) => {
                predictor = Some(frozen.profile_ref().id);
                predictor_model = Some(frozen.model_ref().id);
                policy.predictive = previous_binding;
                crate::service::jobs::bind_prediction(&mut policy, &frozen);
                if !decision_executor_available(&rx.open_table(CONFIGS)?, &frozen)? {
                    Some("no enabled Executor offers the project decision model".to_owned())
                } else if active.llm.config.context_window.is_none() {
                    Some("foreground model capacity is unknown".to_owned())
                } else {
                    None
                }
            }
            Ok(None) => {
                Some("prediction is disabled by the project decision configuration".to_owned())
            }
            Err(BatchError(Failure::Rejected(violation))) => Some(violation.to_string()),
            Err(BatchError(Failure::Storage(error))) => return Err(error),
        }
    } else {
        Some("foreground model is unavailable".to_owned())
    };
    let limit = limit.clamp(1, 100);
    let mut work = Vec::new();
    let adopted_producer = tip_details.and_then(|d| match d.record.job.context_selection {
        crate::db::entities::MutableContextSelection::AdoptedSummary { producer, .. } => {
            Some(producer)
        }
        crate::db::entities::MutableContextSelection::FullHistory => None,
    });
    for entry in rx
        .open_table(CHILDREN)?
        .range((conversation.get(), 0)..(conversation.get(), before.map_or(u128::MAX, JobId::get)))?
        .rev()
        .take(limit)
    {
        let child_id = JobId::new(entry?.0.value().1);
        let Some(bytes) = rx.open_table(JOBS)?.get(child_id.get())? else {
            continue;
        };
        if bytes.value().first() != Some(&TAG_JOB) {
            continue;
        }
        let job = transactions::decode_mutable_job(child_id.get(), bytes.value())?;
        let Some(source) = job.summary_source else {
            continue;
        };
        let state = read_state(&rx, child_id)?;
        let manifest_bytes = rx.open_table(MESSAGES)?.get(source.as_bytes().as_slice())?;
        let Some(manifest_bytes) = manifest_bytes else {
            continue;
        };
        let Ok(manifest) = serde_json::from_slice::<crate::db::entities::NativeSummaryManifest>(
            manifest_bytes.value(),
        ) else {
            continue;
        };
        let (Some(first_b), Some(last_b)) = (manifest.sources.first(), manifest.sources.last())
        else {
            continue;
        };
        let label = match state.outcome.as_ref().map(|o| o.kind) {
            None => match job.phase {
                crate::db::entities::MutableJobPhase::Unsubmitted => "queued",
                crate::db::entities::MutableJobPhase::Queued => "queued",
                crate::db::entities::MutableJobPhase::Executing => "preparing",
            },
            Some(OutcomeKind::Failed) => "failed",
            Some(OutcomeKind::Cancelled) => "cancelled",
            Some(OutcomeKind::Unknown) => "unknown",
            Some(OutcomeKind::Completed) => {
                if adopted_producer == Some(child_id) {
                    "adopted"
                } else if let Some(t) = tip
                    && verify_native_summary_in(&rx, conversation, t, child_id, source).is_ok()
                {
                    "completed"
                } else {
                    "stale"
                }
            }
        };
        let failure = if matches!(label, "failed" | "cancelled" | "unknown") {
            rx.open_table(JOB_OUTPUTS)?
                .range((child_id.get(), 0)..=(child_id.get(), u32::MAX))?
                .next_back()
                .transpose()?
                .map(|(k, b)| decode_output(k.value(), b.value()))
                .transpose()?
                .and_then(|o| o.summary)
        } else {
            None
        };
        let summarizer = match &job.frozen {
            Some(crate::db::entities::MutableFrozenConfiguration::Generative(f)) => f.llm.model.id,
            _ => ConfigId::new(0),
        };
        let boundary =
            |b: &crate::db::entities::NativeSummaryBoundary| crate::db::entities::SourceBoundary {
                turn: b.job,
                input_through: u32::try_from(b.revision).unwrap_or(u32::MAX),
                output_through: b.output_through,
                content: b.content,
            };
        work.push(ContextWorkState {
            job: child_id,
            conversation,
            state: label,
            details: ContextWorkDetails::Summary {
                summarizer,
                failure,
                automatic: job.input.priority != crate::db::entities::JobQueuePriority::Foreground,
                stage: "direct",
                chain: None,
                prediction: None,
                reserved_tokens: 0,
                reserved_requests: 0,
                source,
                first: boundary(first_b),
                through: boundary(last_b),
                turn_count: manifest.sources.len(),
            },
        });
    }
    Ok(Some(ContextState {
        steer_blocked_by: None,
        policy,
        stored_override: None,
        policy_source: "global",
        revision,
        model,
        estimated_tokens: (raw != usize::MAX).then_some(u64::try_from(raw).unwrap_or(u64::MAX)),
        growth,
        predictor,
        predictor_model,
        prediction_unavailable,
        last_use: None,
        work,
        next_before: None,
    }))
}

fn read_config_record(
    rx: &ReadTransaction,
    id: ConfigId,
) -> Result<Option<ConfigRecord>, StorageError> {
    rx.open_table(CONFIGS)?
        .get(id.get())?
        .map(|bytes| ConfigRow::decode_at(id.get(), bytes.value()).map(config_record))
        .transpose()
}

pub(super) fn decision_executor_available(
    configs: &impl ReadableTable<u128, &'static [u8]>,
    frozen: &crate::db::entities::FrozenDecisionConfig,
) -> Result<bool, StorageError> {
    let (operation, protocol) = match frozen {
        crate::db::entities::FrozenDecisionConfig::Native(_) => {
            (ExecutionOperation::Decision, None)
        }
        crate::db::entities::FrozenDecisionConfig::Generative(active, _) => (
            ExecutionOperation::LlmRequest,
            Some(active.llm.provider.transport),
        ),
    };
    for entry in configs.iter()? {
        let (key, bytes) = entry?;
        let row = ConfigRow::decode_at(key.value(), bytes.value())?;
        if row.archived_at.is_none()
            && let ConfigBody::Pool(pool) = row.body
            && pool.enabled
            && pool.offers(operation, protocol)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn approved_calls(
    rx: &ReadTransaction,
    conversation: JobId,
) -> Result<Vec<ToolEvidenceRecord>, StorageError> {
    let mut calls = Vec::new();
    let table = rx.open_table(TOOL_CALLS)?;
    for entry in rx.open_table(OPEN_APPROVALS)?.range(
        (conversation.get(), 0, 0, 0)..=(conversation.get(), u128::MAX, u32::MAX, u32::MAX),
    )? {
        let (key, state) = entry?;
        if state.value() != 1 {
            continue;
        }
        let (_, turn, request, index) = key.value();
        let call = tool_call_ref((turn, request, index))?;
        let bytes = table
            .get(tool_call_key(call))?
            .ok_or_else(|| StorageError::corrupted("approved call missing"))?;
        calls.push(decode_tool_evidence(call, bytes.value())?);
    }
    Ok(calls)
}
fn question(arguments: &str) -> String {
    serde_json::from_str::<serde_json::Value>(arguments)
        .ok()
        .and_then(|v| {
            v.get("question")
                .and_then(|v| v.as_str())
                .map(str::to_owned)
        })
        .unwrap_or_default()
}

fn approval_assessment(
    rx: &ReadTransaction,
    call: ToolCallRef,
    project: Option<crate::db::entities::ProjectId>,
) -> Result<Option<serde_json::Value>, StorageError> {
    use crate::db::entities::{DecisionAnswer, DecisionOutcome, DecisionResult};
    let Some(project) = project else {
        return Ok(None);
    };
    let policy = rx
        .open_table(PROJECTS)?
        .get(project.get())?
        .map(|bytes| decode_project(project.get(), bytes.value()))
        .transpose()?
        .ok_or_else(|| StorageError::corrupted("approval project missing"))?
        .decision_policy();
    if !policy.enabled {
        return Ok(None);
    }
    let mut assessment = serde_json::json!({
        "state": "unavailable", "confidence": null,
        "minimum_confidence": f64::from(policy.minimum_confidence_millis) / 1000.0
    });
    let identity = approvals::command_assessment_identity(call)
        .map_err(|_| StorageError::corrupted("invalid command assessment identity"))?;
    let digest = identity.digest();
    let Some(submission) = rx
        .open_table(SUBMISSIONS)?
        .get(digest.as_bytes().as_slice())?
        .map(|bytes| decode_submission(digest.as_bytes(), bytes.value()))
        .transpose()?
    else {
        return Ok(Some(assessment));
    };
    let state = read_state(rx, submission.job)?;
    assessment["state"] = serde_json::json!(match state.outcome {
        None => "pending",
        Some(ref outcome) if outcome.kind == OutcomeKind::Completed => "completed",
        Some(_) => "failed",
    });
    if let Some((key, bytes)) = rx
        .open_table(JOB_OUTPUTS)?
        .range((submission.job.get(), 0)..=(submission.job.get(), u32::MAX))?
        .next_back()
        .transpose()?
    {
        let output = decode_output(key.value(), bytes.value())?;
        if let Some(result) = output
            .value
            .as_ref()
            .and_then(|value| value.get("decision"))
        {
            let result: DecisionResult = serde_json::from_value(result.clone())
                .map_err(|_| StorageError::corrupted("invalid command assessment result"))?;
            if let DecisionOutcome::Answer {
                answer: DecisionAnswer::Choice { confidence, .. },
            } = result.outcome
            {
                assessment["confidence"] = confidence
                    .map_or(serde_json::Value::Null, |confidence| {
                        serde_json::json!(confidence.value)
                    });
            }
        }
    }
    Ok(Some(assessment))
}

pub(crate) fn read_cards(
    database: &Database,
    root: JobId,
) -> Result<Vec<serde_json::Value>, StorageError> {
    use serde_json::json;
    let rx = read(database)?;
    let mut cards = Vec::new();
    let mut pending = vec![root];
    let mut seen = BTreeSet::new();
    while let Some(conversation) = pending.pop() {
        if !seen.insert(conversation) || seen.len() > 65 {
            return Err(StorageError::corrupted(
                "live conversation tree exceeds its bound",
            ));
        }
        let row = read_job_row(&rx, conversation)?
            .ok_or_else(|| StorageError::corrupted("conversation missing"))?;
        let JobKind::Group {
            origin, project, ..
        } = row.kind
        else {
            return Err(StorageError::corrupted("expected a conversation"));
        };
        let parent = row.parent_id.map(|id| id.to_string());
        if origin != Origin::System {
            for entry in rx.open_table(OPEN_APPROVALS)?.range(
                (conversation.get(), 0, 0, 0)..=(conversation.get(), u128::MAX, u32::MAX, u32::MAX),
            )? {
                let (key, state) = entry?;
                if state.value() != 0 {
                    continue;
                }
                let (_, turn, request, index) = key.value();
                let call = tool_call_ref((turn, request, index))?;
                let table = rx.open_table(TOOL_CALLS)?;
                let bytes = table
                    .get(tool_call_key(call))?
                    .ok_or_else(|| StorageError::corrupted("awaiting call missing"))?;
                let call = decode_tool_evidence(call, bytes.value())?;
                let arguments = call
                    .decision
                    .fact
                    .stored_admission()
                    .ok_or_else(|| StorageError::corrupted("awaiting admission missing"))?
                    .arguments
                    .as_str();
                let mut card = json!({"kind": if matches!(call.decision.fact, crate::db::entities::ToolDecision::AwaitingAnswer(_)) { "question" } else { "approval" }, "conversation": conversation.to_string(), "parent":parent, "approval":[JobId::new(turn).to_string(),request,index], "tool":call.name});
                if call.name == "job_dispatch" {
                    card["kind"] = json!("dispatch");
                    card["arguments"] = json!(arguments);
                } else if matches!(
                    call.decision.fact,
                    crate::db::entities::ToolDecision::AwaitingAnswer(_)
                ) {
                    card["question"] = json!(question(arguments));
                } else {
                    card["arguments"] = json!(arguments);
                    if matches!(
                        call.name.as_str(),
                        "workspace_request_command" | "workspace_run_command"
                    ) && let Some(assessment) = approval_assessment(&rx, call.call, project)?
                    {
                        card["assessment"] = assessment;
                    }
                }
                cards.push(card);
            }
        }
        for entry in rx
            .open_table(LIVE_CHILDREN)?
            .range((conversation.get(), 0)..=(conversation.get(), u128::MAX))?
        {
            pending.push(JobId::new(entry?.0.value().1));
        }
    }
    Ok(cards)
}

pub(crate) struct UsageRetriesPage {
    pub rows: Vec<crate::db::entities::UsageRetryRow>,
    pub next: Option<(u128, u128)>,
}

pub(crate) fn read_usage_retries(
    database: &Database,
    filter: &crate::db::entities::UsageSummaryFilter,
    bucketing: crate::db::entities::UsageBucketing,
    after: Option<(u128, u128)>,
    limit: usize,
) -> Result<UsageRetriesPage, StorageError> {
    let rx = read(database)?;
    let retries = rx.open_table(RETRIES)?;
    let start = after.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
    let mut entries = retries.range((start, std::ops::Bound::Unbounded))?;
    let mut grouped: BTreeMap<UsageGroupKey, (String, u64)> = BTreeMap::new();
    let mut last = None;
    for _ in 0..limit.clamp(1, 100) {
        let Some(entry) = entries.next() else { break };
        let key = entry?.0.value();
        last = Some(key);
        let retry = read_job_row(&rx, JobId::new(key.1))?
            .ok_or_else(|| StorageError::corrupted("retry has no job row"))?;
        let at = retry.created_at.as_unix_micros();
        if filter.from_us.is_some_and(|from| at < from)
            || filter.until_us.is_some_and(|until| at >= until)
        {
            continue;
        }
        let original = read_job_row(&rx, JobId::new(key.0))?
            .ok_or_else(|| StorageError::corrupted("retry names a missing turn"))?;
        let parent = original
            .parent_id
            .map(|id| read_job_row(&rx, id))
            .transpose()?
            .flatten();
        let agent = usage_agent(&original, parent.as_ref());
        if filter.agent.is_some_and(|target| agent != target) {
            continue;
        }
        let Some(target) = turn_target(&rx, &original)? else {
            continue;
        };
        let config = target.frozen;
        let bucket = bucketing
            .start_of(at)
            .ok_or_else(|| StorageError::corrupted("retry bucket overflows"))?;
        let model = config.config.model;
        let hash = *ContentDigest::of(model.as_bytes()).as_bytes();
        let slot = grouped
            .entry((bucket, agent, config.provider.id.get(), hash))
            .or_insert((model, 0));
        slot.1 += 1;
    }
    let more = entries.next().transpose()?.is_some();
    Ok(UsageRetriesPage {
        rows: grouped
            .into_iter()
            .map(|((bucket_us, agent, provider, _), (model, retries))| {
                crate::db::entities::UsageRetryRow {
                    bucket_us,
                    agent,
                    provider,
                    model: Some(model),
                    retries,
                }
            })
            .collect(),
        next: if more { last } else { None },
    })
}

pub(crate) fn read_scheduled_external_policy(
    database: &Database,
    id: crate::db::entities::ScheduledExternalPolicyId,
) -> Result<Option<crate::db::entities::ScheduledExternalPolicyRecord>, StorageError> {
    let rx = read(database)?;
    let table = rx.open_table(SCHEDULED_EXTERNAL_POLICY)?;
    table
        .get(id.get())?
        .map(|row| decode_scheduled_external_policy(id.get(), row.value()))
        .transpose()
}

pub(crate) fn read_scheduled_external_policies(
    database: &Database,
    after: Option<crate::db::entities::ScheduledExternalPolicyId>,
    limit: usize,
) -> Result<
    CursorPage<
        crate::db::entities::ScheduledExternalPolicyRecord,
        crate::db::entities::ScheduledExternalPolicyId,
    >,
    StorageError,
> {
    let rx = read(database)?;
    let table = rx.open_table(SCHEDULED_EXTERNAL_POLICY)?;
    let limit = limit.clamp(1, MAX_PAGE);
    let mut records = Vec::new();
    let mut more = false;
    for entry in table.range((
        std::ops::Bound::Excluded(after.map_or(0, |id| id.get())),
        std::ops::Bound::Unbounded,
    ))? {
        if records.len() == limit {
            more = true;
            break;
        }
        let (key, row) = entry?;
        records.push(decode_scheduled_external_policy(key.value(), row.value())?);
    }
    let next = records.last().map(|record| record.id).filter(|_| more);
    Ok(CursorPage { records, next })
}

pub(crate) fn read_scheduled_external_binding_with_policy(
    database: &Database,
    job: JobId,
) -> Result<
    Option<(
        crate::db::entities::ScheduledExternalBinding,
        crate::db::entities::ScheduledExternalPolicyRecord,
    )>,
    StorageError,
> {
    let rx = read(database)?;
    let Some(row) = rx.open_table(SCHEDULED_EXTERNAL_BINDING)?.get(job.get())? else {
        return Ok(None);
    };
    let binding = decode_scheduled_external_binding(job.get(), row.value())?;
    let Some(policy_row) = rx
        .open_table(SCHEDULED_EXTERNAL_POLICY)?
        .get(binding.policy.get())?
    else {
        return Err(StorageError::corrupted(
            "a scheduled external binding names a missing policy",
        ));
    };
    let policy = decode_scheduled_external_policy(binding.policy.get(), policy_row.value())?;
    Ok(Some((binding, policy)))
}
