use std::sync::OnceLock;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use super::*;
use crate::db::entities::GLOBAL_EXECUTION_LIMIT;
use crate::db::entities::{JobId, OutcomeCause, OutcomeKind, OutcomeRecord, Timestamp};
use ::redb::ReadableTableMetadata;

// ---- open ----------------------------------------------------------------------

static PROMOTION_CLOCK_BASE: OnceLock<(i64, Instant)> = OnceLock::new();

/// Fixes the promotion clock: the wall time at open, plus the monotonic time
/// elapsed since.
pub(crate) fn init_promotion_clock(wall_time_at_open: i64) {
    let _ = PROMOTION_CLOCK_BASE.set((wall_time_at_open, Instant::now()));
}

static PROMOTION_HIGH_WATER: AtomicI64 = AtomicI64::new(i64::MIN);

/// The later of the monotonic and the wall reading, never going back. Wall
/// time catches up after a suspend, which the monotonic reading does not
/// count, and deadlines are made from wall time; a backward wall step moves
/// nothing.
pub(crate) fn promotion_clock() -> i64 {
    let wall = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_micros()).unwrap_or(i64::MAX))
        .unwrap_or(0);
    let reading =
        match PROMOTION_CLOCK_BASE.get() {
            Some((wall_base, instant_base)) => wall.max(wall_base.saturating_add(
                i64::try_from(instant_base.elapsed().as_micros()).unwrap_or(i64::MAX),
            )),
            None => wall,
        };
    PROMOTION_HIGH_WATER
        .fetch_max(reading, Ordering::SeqCst)
        .max(reading)
}

static IDENTIFIERS: OnceLock<(Option<String>, Option<String>)> = OnceLock::new();

/// The host and boot identifiers, read once per process; either may be
/// unreadable.
fn identifiers() -> &'static (Option<String>, Option<String>) {
    IDENTIFIERS.get_or_init(|| (read_host_id(), read_boot_id()))
}

pub(crate) fn current_host_id() -> Option<String> {
    identifiers().0.clone()
}

pub(crate) fn current_boot_id() -> Option<String> {
    identifiers().1.clone()
}

#[cfg(target_os = "macos")]
fn read_host_id() -> Option<String> {
    let output = std::process::Command::new("ioreg")
        .args(["-rd1", "-c", "IOPlatformExpertDevice"])
        .output()
        .ok()?;
    let text = String::from_utf8(output.stdout).ok()?;
    text.lines()
        .find(|line| line.contains("IOPlatformUUID"))
        .and_then(|line| line.split('"').nth(3))
        .map(str::to_owned)
}

#[cfg(target_os = "linux")]
fn read_host_id() -> Option<String> {
    std::fs::read_to_string("/etc/machine-id")
        .ok()
        .map(|text| text.trim().to_owned())
        .filter(|text| !text.is_empty())
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn read_host_id() -> Option<String> {
    None
}

#[cfg(target_os = "macos")]
fn read_boot_id() -> Option<String> {
    let output = std::process::Command::new("sysctl")
        .args(["-n", "kern.bootsessionuuid"])
        .output()
        .ok()?;
    let text = String::from_utf8(output.stdout).ok()?;
    Some(text.trim().to_owned()).filter(|text| !text.is_empty())
}

#[cfg(target_os = "linux")]
fn read_boot_id() -> Option<String> {
    std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
        .ok()
        .map(|text| text.trim().to_owned())
        .filter(|text| !text.is_empty())
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn read_boot_id() -> Option<String> {
    None
}

pub(crate) fn database_file(directory: &Path) -> PathBuf {
    directory.join(DATABASE_FILE_NAME)
}

/// An opened database file with this opening's operation epoch and the
/// instance's Genesis Job (always Job 0).
pub(crate) struct OpenedFile {
    pub(crate) database: Database,
    pub(crate) epoch: u64,
    pub(crate) genesis: JobId,
    pub(crate) abandoned: Vec<JobId>,
}

pub(crate) fn open(directory: &Path) -> Result<OpenedFile, StorageError> {
    let path = database_file(directory);
    prepare_private_file(&path)?;
    let mut builder = Database::builder();
    builder.set_cache_size(CACHE_BYTES);
    let database = builder.create(&path).map_err(StorageError::from)?;
    let (epoch, genesis, abandoned) = initialize(&database)?;
    Ok(OpenedFile {
        database,
        epoch,
        genesis,
        abandoned,
    })
}

pub(super) fn prepare_private_file(path: &Path) -> Result<(), StorageError> {
    let io_error = |error: std::io::Error| StorageError {
        kind: StorageErrorKind::Io(error.kind()),
        os_error: error.raw_os_error(),
        detail: format!("cannot make {} private: {error}", path.display()),
    };
    match crate::system::data_directory::private_file(&mut std::fs::OpenOptions::new())
        .write(true)
        .create_new(true)
        .open(path)
    {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            crate::system::data_directory::restrict_file(path).map_err(io_error)
        }
        Err(error) => Err(io_error(error)),
    }
}

pub(super) fn initialize(database: &Database) -> Result<(u64, JobId, Vec<JobId>), StorageError> {
    let mut tx = database.begin_write().map_err(StorageError::from)?;
    tx.set_durability(Durability::Immediate)
        .map_err(StorageError::from)?;
    use ::redb::TableHandle;
    let existing: Vec<_> = tx
        .list_tables()
        .map_err(StorageError::from)?
        .map(|table| table.name().to_owned())
        .collect();
    let indexes_missing = [
        "job_observation",
        "content_reference",
        "content_reclaim",
        "request_timeline",
    ]
    .iter()
    .any(|name| !existing.iter().any(|table| table == name));
    let empty = existing.is_empty() && tx.list_multimap_tables()?.next().is_none();
    let now = Timestamp::now();
    init_promotion_clock(now.as_unix_micros());

    let (epoch, clean_shutdown) = {
        let mut meta = tx.open_table(META).map_err(StorageError::from)?;
        if empty {
            meta.insert(
                DATABASE_FORMAT_KEY,
                DATABASE_FORMAT_VERSION.to_le_bytes().as_slice(),
            )?;
            let settings = crate::db::entities::GlobalCompactionSettings::default();
            let mut encoded = Enc::new(TAG_GLOBAL_COMPACTION);
            encoded.u64(settings.revision);
            encoded.bool(settings.prediction_enabled);
            encoded.compaction_policy(&settings.policy);
            meta.insert("compaction_settings", encoded.0.as_slice())
                .map_err(StorageError::from)?;
            meta.insert("open_epoch", 0u64.to_le_bytes().as_slice())
                .map_err(StorageError::from)?;
            meta.insert("clean_shutdown", [1u8].as_slice())
                .map_err(StorageError::from)?;
        } else {
            match meta.get(DATABASE_FORMAT_KEY)? {
                Some(version) if version.value() == DATABASE_FORMAT_VERSION.to_le_bytes() => {}
                Some(_) => return Err(unsupported_format("unsupported database format")),
                None => return Err(unsupported_format("unmarked database; run ocg migrate")),
            }
        }
        let epoch = meta
            .get("open_epoch")
            .map_err(StorageError::from)?
            .map_or(0, |value| {
                <[u8; 8]>::try_from(value.value())
                    .map(u64::from_le_bytes)
                    .unwrap_or(0)
            })
            + 1;
        meta.insert("open_epoch", epoch.to_le_bytes().as_slice())
            .map_err(StorageError::from)?;
        let clean_shutdown = meta
            .get("clean_shutdown")
            .map_err(StorageError::from)?
            .is_some_and(|value| value.value().first().is_some_and(|flag| *flag != 0));
        meta.insert("clean_shutdown", [0u8].as_slice())
            .map_err(StorageError::from)?;
        (epoch, clean_shutdown)
    };
    create_tables(&tx)?;
    if !clean_shutdown || indexes_missing {
        indexes::rebuild(&tx, promotion_clock())?;
    }
    let mut batch = Batch::new(tx, epoch);
    let abandoned = match reconcile(&mut batch) {
        Ok(abandoned) => abandoned,
        Err(BatchError(failure)) => {
            let error = match failure {
                Failure::Storage(error) => error,
                Failure::Rejected(violation) => {
                    StorageError::corrupted(format!("open reconcile refused: {violation}"))
                }
            };
            batch.tx.abort().map_err(StorageError::from)?;
            return Err(error);
        }
    };
    batch.tx.commit().map_err(StorageError::from)?;
    Ok((epoch, JobId::new(0), abandoned))
}

pub(super) fn create_tables(tx: &WriteTransaction) -> Result<(), StorageError> {
    tx.open_table(META)?;
    tx.open_table(SEQUENCES)?;
    tx.open_table(JOB_OBSERVATIONS)?;
    tx.open_table(CONTENT_REFERENCES)?;
    tx.open_table(CONTENT_RECLAIM)?;
    tx.open_table(CONFIGS)?;
    tx.open_table(JOBS)?;
    tx.open_table(JOB_ALLOCATIONS)?;
    tx.open_table(CONVERSATIONS)?;
    tx.open_table(TURNS)?;
    tx.open_table(JOB_STATES)?;
    tx.open_table(JOB_OUTPUTS)?;
    tx.open_table(SUBMISSIONS)?;
    tx.open_table(ATTACHMENTS)?;
    tx.open_table(TOOL_CALLS)?;
    tx.open_table(REQUEST_EVENTS)?;
    tx.open_table(MESSAGES)?;
    tx.open_table(PROJECTS)?;
    tx.open_table(JOB_CONTROLS)?;
    tx.open_table(PENDING)?;
    tx.open_table(READY)?;
    tx.open_table(DELAYED)?;
    tx.open_table(DEPENDENCY_WAITERS)?;
    tx.open_table(UNSETTLED)?;
    tx.open_table(HELD_SLOTS)?;
    tx.open_table(CHILDREN)?;
    tx.open_table(SUCCESSORS)?;
    tx.open_table(JOB_INPUTS)?;
    tx.open_table(NOTICES)?;
    tx.open_table(GROUP_HEADS)?;
    tx.open_table(CONVERSATION_SEARCH)?;
    tx.open_table(CHILD_GROUPS)?;
    tx.open_table(LIVE_CHILDREN)?;
    tx.open_table(USER_INPUT_AT)?;
    tx.open_table(NOTICE_READ)?;
    tx.open_table(GROUP_RESULTS)?;
    tx.open_table(ACTIVE_JOBS)?;
    tx.open_table(TOOL_CALL_IDS)?;
    tx.open_table(TOOL_STATS)?;
    tx.open_table(USAGE_TOTALS)?;
    tx.open_table(RETRIES)?;
    tx.open_table(APPROVALS)?;
    tx.open_table(OPEN_APPROVALS)?;
    tx.open_table(APPROVAL_CALLS)?;
    tx.open_table(RECURRENCES)?;
    tx.open_table(RECURRENCE_DUE)?;
    tx.open_table(ATTACHMENT_REFS)?;
    tx.open_table(REQUEST_SCOPES)?;
    tx.open_table(REQUEST_TIMELINE)?;
    Ok(())
}

/// Allocates monotonically across restarts and clock rollback. The persisted
/// high water retains UUIDv7's random starting payload; a stale clock advances
/// that payload as the UUIDv7 monotonic counter rather than reusing an ID.
pub(super) fn allocate(tx: &WriteTransaction) -> Result<u128, StorageError> {
    let mut table = tx.open_table(SEQUENCES)?;
    let previous = table.get("entity")?.map_or(0, |value| value.value());
    let fresh = uuid::Uuid::now_v7().as_u128();
    let next = if fresh > previous {
        fresh
    } else {
        let payload_mask = (1u128 << 74) - 1;
        let payload = (((previous >> 64) & 0xfff) << 62) | (previous & ((1u128 << 62) - 1));
        let next_payload = payload + 1;
        let millis = (previous >> 80) + (next_payload >> 74);
        if millis >= 1u128 << 48 {
            return Err(StorageError::corrupted("UUIDv7 allocation space exhausted"));
        }
        let payload = next_payload & payload_mask;
        (millis << 80)
            | (7u128 << 76)
            | ((payload >> 62) << 64)
            | (2u128 << 62)
            | (payload & ((1u128 << 62) - 1))
    };
    table.insert("entity", next)?;
    Ok(next)
}

/// Whether a claim made under `claim_host` and `claim_boot` was made before a
/// reboot of this host. A file opened on another host, or with either
/// identifier unreadable, never counts as rebooted.
fn rebooted(
    claim_host: Option<&str>,
    claim_boot: Option<&str>,
    host: Option<&str>,
    boot: Option<&str>,
) -> bool {
    matches!(
        (claim_host, host, claim_boot, boot),
        (Some(claim_host), Some(host), Some(claim_boot), Some(boot))
            if claim_host == host && claim_boot != boot
    )
}

/// OPEN (section 8.8): every Job running or suspended at the crash takes an
/// Unknown outcome; its stop is confirmed and its slot freed when no OS
/// process of it can remain or the host has rebooted since its claim, and its
/// slot is quarantined otherwise. Returns the Jobs that were abandoned.
fn reconcile(batch: &mut Batch) -> Result<Vec<JobId>, BatchError> {
    let epoch = batch.epoch;
    let now = batch.now;
    let states = batch.step(|tx| {
        let slots = tx.open_table(HELD_SLOTS)?;
        if slots.len()? > u64::from(GLOBAL_EXECUTION_LIMIT) {
            return Err(StorageError::corrupted(
                "held slot count exceeds execution limit",
            ));
        }
        let mut states = Vec::new();
        for entry in slots.iter()? {
            let job = entry?.0.value().1;
            let state = tx
                .open_table(JOB_STATES)?
                .get(job)?
                .map(|bytes| JobState::decode_at(job, bytes.value()))
                .transpose()?
                .ok_or_else(|| StorageError::corrupted("held slot has no job state"))?;
            states.push(state);
        }
        Ok(states)
    })?;
    let mut abandoned = Vec::new();
    let mut unconfirmed = Vec::new();
    for mut state in states {
        let is_unknown = state
            .outcome
            .as_ref()
            .is_some_and(|outcome| outcome.kind == OutcomeKind::Unknown);
        if state.outcome.is_none()
            && state
                .claim
                .as_ref()
                .is_some_and(|claim| claim.epoch != epoch)
        {
            state.outcome = Some(OutcomeRecord {
                kind: OutcomeKind::Unknown,
                at: now,
                cause: Some(OutcomeCause::Abandoned),
            });
            batch.put_job_state(state.job_id, &state)?;
            abandoned.push(state.job_id);
            unconfirmed.push(state);
        } else if is_unknown && state.stop_confirmed_at.is_none() {
            unconfirmed.push(state);
        }
    }
    // Newer waiters cancel first, so all receive Abandoned rather than a dependency failure.
    let mut before = u128::MAX;
    loop {
        let pending = batch.step(|tx| {
            tx.open_table(PENDING)?
                .range(..before)?
                .rev()
                .take(256)
                .map(|entry| {
                    entry
                        .map(|(key, _)| JobId::new(key.value()))
                        .map_err(StorageError::from)
                })
                .collect::<Result<Vec<_>, _>>()
        })?;
        if pending.is_empty() {
            break;
        }
        for job in pending {
            before = job.get();
            if matches!(
                batch.require_job(job)?.kind,
                JobKind::Execution {
                    plan_source: PlanSource::Process { .. },
                    ..
                }
            ) && batch.require_job_state(job)?.outcome.is_none()
            {
                batch.finalize(
                    job,
                    OutcomeKind::Cancelled,
                    Some(OutcomeCause::Abandoned),
                    None,
                    false,
                )?;
            }
        }
    }
    let (host, boot) = (batch.host_id.clone(), batch.boot_id.clone());
    for mut state in unconfirmed {
        let job = state.job_id;
        let confirmed = batch.no_live_process(job)?
            || state.claim.as_ref().is_some_and(|claim| {
                rebooted(
                    claim.host_id.as_deref(),
                    claim.boot_id.as_deref(),
                    host.as_deref(),
                    boot.as_deref(),
                )
            });
        if let Some(claim) = state.claim.clone() {
            let pool = claim.pool_id.get();
            if confirmed {
                batch.step(|tx| {
                    tx.open_table(HELD_SLOTS)?.remove((pool, job.get()))?;
                    Ok(())
                })?;
            } else {
                batch.step(|tx| {
                    tx.open_table(HELD_SLOTS)?
                        .insert((pool, job.get()), (claim.epoch, true))?;
                    Ok(())
                })?;
            }
        }
        if confirmed {
            state.stop_confirmed_at = Some(now);
            state.stop_confirmed_by = Some(if batch.no_live_process(job)? {
                StopProof::Proof
            } else {
                StopProof::Reboot
            });
            batch.put_job_state(job, &state)?;
            let row = batch.require_job(job)?;
            if row.kind.is_persistent()
                && let Some(conversation) = row.parent_id
            {
                batch.step(|tx| {
                    tx.open_table(ACTIVE_JOBS)?.remove(conversation.get())?;
                    Ok(())
                })?;
            }
        }
        let row = batch.require_job(job)?;
        batch.index_answer(&row, OutcomeKind::Unknown, false)?;
        let unresolved = batch.step(|tx| {
            let mut calls = Vec::new();
            for entry in tx
                .open_table(TOOL_CALLS)?
                .range((job.get(), 0, 0)..=(job.get(), u32::MAX, u32::MAX))?
            {
                let (key, bytes) = entry?;
                let record = decode_tool_evidence(tool_call_ref(key.value())?, bytes.value())?;
                if record
                    .admission()
                    .is_some_and(|admission| admission.handler_version.starts_with("job_"))
                    && record.outcome.is_none()
                    && record.reconciliation.is_none()
                {
                    calls.push(record);
                }
            }
            Ok(calls)
        })?;
        for mut record in unresolved {
            record.reconciliation = Some(crate::db::entities::Recorded {
                fact: crate::db::entities::ToolReconciliation {
                    conclusion: EffectConclusion::NotApplied,
                    reconciler: "open".into(),
                    evidence: "effect commits only with its outcome".into(),
                    explanation: "the host stopped before the call's transaction".into(),
                },
                at: batch.now(),
            });
            batch.put_tool_evidence(&record)?;
        }
        batch.settle(job)?;
        if abandoned.contains(&job) {
            batch.turn_ended(job)?;
        }
    }
    Ok(abandoned)
}

pub(crate) struct ReclamationReport {
    pub(crate) details: Vec<(
        ContentDigest,
        Result<crate::db::attachments::Reclamation, crate::db::attachments::StoreError>,
    )>,
    pub(crate) omitted: u64,
}

pub(crate) fn reclaim_pending(
    database: &Database,
    attachments: &crate::db::attachments::AttachmentStore,
) -> Result<ReclamationReport, StorageError> {
    let mut tx = database.begin_write()?;
    tx.set_durability(Durability::Immediate)?;
    let mut after: Option<[u8; 32]> = None;
    let mut report = ReclamationReport {
        details: Vec::new(),
        omitted: 0,
    };
    loop {
        let page = {
            let queue = tx.open_table(CONTENT_RECLAIM)?;
            let start = after.as_ref().map_or(std::ops::Bound::Unbounded, |after| {
                std::ops::Bound::Excluded(after.as_slice())
            });
            let mut page = Vec::new();
            for entry in queue
                .range::<&[u8]>((start, std::ops::Bound::Unbounded))?
                .take(256)
            {
                let (key, value) = entry?;
                let digest: [u8; 32] = key
                    .value()
                    .try_into()
                    .map_err(|_| StorageError::corrupted("invalid content digest"))?;
                page.push((ContentDigest::from_bytes(digest), value.value()));
            }
            page
        };
        if page.is_empty() {
            break;
        }
        let found = attachments.reclaim_unneeded(&page);
        for (digest, _) in &page {
            after = Some(*digest.as_bytes());
            let failed = found.iter().any(|(candidate, result)| {
                candidate == digest
                    && !matches!(
                        result,
                        Ok(crate::db::attachments::Reclamation::Removed
                            | crate::db::attachments::Reclamation::Absent)
                    )
            });
            if !failed {
                tx.open_table(CONTENT_RECLAIM)?
                    .remove(digest.as_bytes().as_slice())?;
            }
        }
        for detail in found {
            if report.details.len() < MAX_PAGE {
                report.details.push(detail);
            } else {
                report.omitted = report.omitted.saturating_add(1);
            }
        }
    }
    tx.commit()?;
    Ok(report)
}

const LEGACY_CONVERSATION_SEARCH: TableDefinition<(u128, u128, u128, u8, u32), ()> =
    TableDefinition::new("conversation_search");

fn unsupported_format(detail: &str) -> StorageError {
    StorageError {
        kind: StorageErrorKind::Other,
        os_error: None,
        detail: detail.to_owned(),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MigrationFormat {
    Legacy,
    UnmarkedCurrent,
    Current,
}

pub(crate) struct MigrationSource(::redb::ReadOnlyDatabase);

pub(crate) fn migration_source(path: &Path) -> Result<MigrationSource, StorageError> {
    if std::fs::metadata(path)
        .map_err(|error| StorageError::from(::redb::Error::Io(error)))?
        .len()
        == 0
    {
        return Err(unsupported_format(
            "database is empty/uninitialized; start OCG to initialize it",
        ));
    }
    Ok(MigrationSource(::redb::ReadOnlyDatabase::open(path)?))
}

fn check_record_headers<K: Key + 'static>(
    tx: &ReadTransaction,
    definition: TableDefinition<K, &'static [u8]>,
    tag: u8,
    format: MigrationFormat,
) -> Result<(), StorageError> {
    for row in tx.open_table(definition)?.iter()? {
        let (_, value) = row?;
        match value.value() {
            [actual, version, ..]
                if *actual == tag
                    && (matches!(*version, 11 | 13)
                        || (*version == 14 && format != MigrationFormat::Legacy)) => {}
            _ => return Err(unsupported_format("unsupported record encoding")),
        }
    }
    Ok(())
}

pub(crate) fn migration_inspect(source: &MigrationSource) -> Result<MigrationFormat, StorageError> {
    use ::redb::TableHandle;
    let tx = source.0.begin_read()?;
    if tx.list_tables()?.next().is_none() && tx.list_multimap_tables()?.next().is_none() {
        return Err(unsupported_format(
            "database is empty/uninitialized; start OCG to initialize it",
        ));
    }
    let mut known = BTreeSet::new();
    macro_rules! check_tables {
        ($($table:ident),+ $(,)?) => { $(
            known.insert($table.name().to_owned());
            drop(tx.open_table($table)?);
        )+ };
    }
    check_tables!(
        META,
        SEQUENCES,
        CONFIGS,
        JOBS,
        JOB_STATES,
        JOB_OUTPUTS,
        SUBMISSIONS,
        ATTACHMENTS,
        TOOL_CALLS,
        MESSAGES,
        REQUEST_EVENTS,
        PROJECTS,
        JOB_CONTROLS,
        JOB_INPUTS,
        NOTICES,
        APPROVALS,
        RECURRENCES,
        JOB_OBSERVATIONS,
        CONTENT_REFERENCES,
        CONTENT_RECLAIM,
        JOB_ALLOCATIONS,
        CONVERSATIONS,
        TURNS,
        PENDING,
        READY,
        DELAYED,
        DEPENDENCY_WAITERS,
        UNSETTLED,
        HELD_SLOTS,
        CHILDREN,
        SUCCESSORS,
        GROUP_HEADS,
        CHILD_GROUPS,
        LIVE_CHILDREN,
        USER_INPUT_AT,
        ACTIVE_JOBS,
        GROUP_RESULTS,
        NOTICE_READ,
        TOOL_CALL_IDS,
        TOOL_STATS,
        USAGE_TOTALS,
        RETRIES,
        ATTACHMENT_REFS,
        REQUEST_SCOPES,
        REQUEST_TIMELINE,
        OPEN_APPROVALS,
        APPROVAL_CALLS,
        RECURRENCE_DUE
    );
    known.insert(CONVERSATION_SEARCH.name().to_owned());
    let actual: BTreeSet<String> = tx
        .list_tables()?
        .map(|table| table.name().to_owned())
        .collect();
    if actual != known || tx.list_multimap_tables()?.next().is_some() {
        return Err(unsupported_format("unsupported set of database tables"));
    }
    let meta = tx.open_table(META)?;
    let marker = meta.get(DATABASE_FORMAT_KEY)?;
    let legacy = tx.open_table(LEGACY_CONVERSATION_SEARCH).is_ok();
    let format = match marker.as_ref().map(|value| value.value()) {
        Some(value) if value == DATABASE_FORMAT_VERSION.to_le_bytes() && !legacy => {
            drop(tx.open_table(CONVERSATION_SEARCH)?);
            MigrationFormat::Current
        }
        None if legacy => MigrationFormat::Legacy,
        None => {
            drop(tx.open_table(CONVERSATION_SEARCH)?);
            MigrationFormat::UnmarkedCurrent
        }
        Some(_) => {
            return Err(unsupported_format("unsupported database format marker"));
        }
    };
    for key in ["clean_shutdown", "open_epoch", "compaction_settings"] {
        if meta.get(key)?.is_none() {
            return Err(StorageError::corrupted("missing required metadata"));
        }
    }
    let shutdown = meta
        .get("clean_shutdown")?
        .ok_or_else(|| StorageError::corrupted("missing shutdown flag"))?;
    let epoch = meta
        .get("open_epoch")?
        .ok_or_else(|| StorageError::corrupted("missing open epoch"))?;
    if !matches!(shutdown.value(), [0] | [1]) || epoch.value().len() != 8 {
        return Err(unsupported_format("unsupported metadata encoding"));
    }
    macro_rules! headers { ($($table:ident => $tag:expr),+ $(,)?) => { $(check_record_headers(&tx, $table, $tag, format)?;)+ }; }
    headers!(CONFIGS => 1, JOBS => 2, JOB_STATES => 3, JOB_OUTPUTS => 4, SUBMISSIONS => 5,
        ATTACHMENTS => 6, TOOL_CALLS => 7, REQUEST_EVENTS => 8, PROJECTS => 9,
        JOB_CONTROLS => 10, JOB_INPUTS => 11, NOTICES => 12, RECURRENCES => 13, APPROVALS => 15);
    let settings = meta
        .get("compaction_settings")?
        .ok_or_else(|| StorageError::corrupted("missing compaction settings"))?;
    if !matches!(settings.value(), [14, 11 | 13 | 14, ..]) {
        return Err(unsupported_format(
            "unsupported compaction settings encoding",
        ));
    }
    reader::global_compaction_settings(&meta)?;
    macro_rules! decode_rows {
        ($table:ident, |$key:ident, $bytes:ident| $body:expr) => {
            for row in tx.open_table($table)?.iter()? {
                let (key, value) = row?;
                let $key = key.value();
                let $bytes = value.value();
                $body?;
            }
        };
    }
    decode_rows!(CONFIGS, |key, bytes| ConfigRow::decode_at(key, bytes));
    decode_rows!(JOBS, |key, bytes| JobSpec::decode_at(key, bytes));
    decode_rows!(JOB_STATES, |key, bytes| JobState::decode_at(key, bytes));
    decode_rows!(JOB_OUTPUTS, |key, bytes| rows::decode_output(key, bytes));
    decode_rows!(SUBMISSIONS, |key, bytes| rows::decode_submission(
        key, bytes
    ));
    decode_rows!(ATTACHMENTS, |key, bytes| rows::decode_attachment(
        key, bytes
    ));
    decode_rows!(TOOL_CALLS, |key, bytes| rows::decode_tool_evidence(
        rows::tool_call_ref(key)?,
        bytes
    ));
    decode_rows!(REQUEST_EVENTS, |key, bytes| rows::decode_request_event(
        JobId::new(key.0),
        RequestNumber::new(key.1)
            .ok_or_else(|| StorageError::corrupted("invalid request number"))?,
        key.2,
        bytes
    ));
    decode_rows!(PROJECTS, |key, bytes| rows::decode_project(key, bytes));
    decode_rows!(JOB_CONTROLS, |_key, bytes| JobControl::decode(bytes));
    decode_rows!(JOB_INPUTS, |_key, bytes| JobInputEntry::decode(bytes));
    decode_rows!(NOTICES, |_key, bytes| Notice::decode(bytes));
    decode_rows!(APPROVALS, |_key, bytes| ApprovalRecord::decode(bytes));
    decode_rows!(RECURRENCES, |_key, bytes| Recurrence::decode(bytes));
    Ok(format)
}

fn migration_fingerprint<K: Key + 'static, V: Value + 'static>(
    tx: &impl TxRead,
    definition: TableDefinition<K, V>,
) -> Result<[u8; 32], StorageError> {
    let table = tx.table(definition)?;
    let mut hash = aws_lc_rs::digest::Context::new(&aws_lc_rs::digest::SHA256);
    for row in table.iter()? {
        let (key, value) = row?;
        let key = key.value();
        let value = value.value();
        let kb = K::as_bytes(&key);
        let vb = V::as_bytes(&value);
        for bytes in [kb.as_ref(), vb.as_ref()] {
            hash.update(&(bytes.len() as u64).to_le_bytes());
            hash.update(bytes);
        }
    }
    hash.finish()
        .as_ref()
        .try_into()
        .map_err(|_| StorageError::corrupted("invalid digest"))
}

fn authoritative_snapshot(tx: &impl TxRead) -> Result<Vec<[u8; 32]>, StorageError> {
    let meta = tx.table(META)?;
    let mut hash = aws_lc_rs::digest::Context::new(&aws_lc_rs::digest::SHA256);
    for row in meta.iter()? {
        let (key, value) = row?;
        if key.value() == DATABASE_FORMAT_KEY {
            continue;
        }
        for bytes in [key.value().as_bytes(), value.value()] {
            hash.update(&(bytes.len() as u64).to_le_bytes());
            hash.update(bytes);
        }
    }
    let metadata: [u8; 32] = hash
        .finish()
        .as_ref()
        .try_into()
        .map_err(|_| StorageError::corrupted("invalid digest"))?;
    Ok(vec![
        metadata,
        migration_fingerprint(tx, SEQUENCES)?,
        migration_fingerprint(tx, CONFIGS)?,
        migration_fingerprint(tx, JOBS)?,
        migration_fingerprint(tx, JOB_STATES)?,
        migration_fingerprint(tx, JOB_OUTPUTS)?,
        migration_fingerprint(tx, SUBMISSIONS)?,
        migration_fingerprint(tx, ATTACHMENTS)?,
        migration_fingerprint(tx, TOOL_CALLS)?,
        migration_fingerprint(tx, MESSAGES)?,
        migration_fingerprint(tx, REQUEST_EVENTS)?,
        migration_fingerprint(tx, PROJECTS)?,
        migration_fingerprint(tx, JOB_CONTROLS)?,
        migration_fingerprint(tx, JOB_INPUTS)?,
        migration_fingerprint(tx, NOTICES)?,
        migration_fingerprint(tx, APPROVALS)?,
        migration_fingerprint(tx, RECURRENCES)?,
    ])
}

pub(crate) fn migration_snapshot(source: &MigrationSource) -> Result<Vec<[u8; 32]>, StorageError> {
    authoritative_snapshot(&source.0.begin_read()?)
}

pub(crate) fn migration_apply(
    path: &Path,
    format: MigrationFormat,
    expected: &[[u8; 32]],
) -> Result<(), StorageError> {
    let db = Database::open(path)?;
    let mut tx = db.begin_write()?;
    tx.set_durability(Durability::Immediate)?;
    if authoritative_snapshot(&tx)? != expected {
        return Err(StorageError::corrupted(
            "copied authoritative records differ",
        ));
    }
    match format {
        MigrationFormat::Legacy => {
            if !tx.delete_table(LEGACY_CONVERSATION_SEARCH)? {
                return Err(StorageError::corrupted("legacy search index missing"));
            }
            drop(tx.open_table(CONVERSATION_SEARCH)?);
            indexes::rebuild_conversation_search(&tx)?;
            indexes::verify_search_index(&tx)?;
        }
        MigrationFormat::UnmarkedCurrent => {
            drop(tx.open_table(CONVERSATION_SEARCH)?);
        }
        MigrationFormat::Current => return Err(unsupported_format("database is already current")),
    }
    if authoritative_snapshot(&tx)? != expected {
        return Err(StorageError::corrupted(
            "authoritative records changed during migration",
        ));
    }
    {
        let mut meta = tx.open_table(META)?;
        meta.insert(
            DATABASE_FORMAT_KEY,
            DATABASE_FORMAT_VERSION.to_le_bytes().as_slice(),
        )?;
    }
    tx.commit()?;
    Ok(())
}
