use std::sync::OnceLock;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use super::*;
use crate::persistence::{JobId, OutcomeCause, OutcomeKind, OutcomeRecord, Timestamp};

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

pub(crate) enum OpenError {
    Storage(StorageError),
    /// The file is a redb database that this application did not create.
    ForeignFile,
    /// The file uses a storage format this build does not support.
    UnsupportedFormat {
        found: u32,
    },
}

impl From<StorageError> for OpenError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
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

pub(crate) fn open(directory: &Path) -> Result<OpenedFile, OpenError> {
    let path = database_file(directory);
    prepare_private_file(&path)?;
    let mut builder = Database::builder();
    builder.set_cache_size(CACHE_BYTES);
    if path.metadata().is_ok_and(|metadata| metadata.len() > 0)
        && let Ok(existing) = builder.open_read_only(&path)
    {
        check_existing_format(&existing)?;
    }
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
    match crate::data_directory::private_file(&mut std::fs::OpenOptions::new())
        .write(true)
        .create_new(true)
        .open(path)
    {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            crate::data_directory::restrict_file(path).map_err(io_error)
        }
        Err(error) => Err(io_error(error)),
    }
}

pub(super) enum FormatVerdict {
    New,
    Supported,
}

pub(super) fn format_verdict(
    format: Option<Vec<u8>>,
    version: Option<Vec<u8>>,
    existing_tables: usize,
) -> Result<FormatVerdict, OpenError> {
    match (format, version) {
        (None, None) if existing_tables == 0 => Ok(FormatVerdict::New),
        (Some(format), Some(version)) if format == FORMAT_NAME => {
            let found = <[u8; 4]>::try_from(version.as_slice())
                .map(u32::from_le_bytes)
                .unwrap_or(u32::MAX);
            match found {
                FORMAT_VERSION => Ok(FormatVerdict::Supported),
                _ => Err(OpenError::UnsupportedFormat { found }),
            }
        }
        _ => Err(OpenError::ForeignFile),
    }
}

pub(super) fn check_existing_format(database: &redb::ReadOnlyDatabase) -> Result<(), OpenError> {
    let rx = database.begin_read().map_err(StorageError::from)?;
    let existing_tables = rx.list_tables().map_err(StorageError::from)?.count();
    let (format, version) = match rx.open_table(META) {
        Ok(meta) => (
            meta.get("format")
                .map_err(StorageError::from)?
                .map(|v| v.value().to_vec()),
            meta.get("format_version")
                .map_err(StorageError::from)?
                .map(|v| v.value().to_vec()),
        ),
        Err(redb::TableError::TableDoesNotExist(_)) => (None, None),
        Err(error) => return Err(StorageError::from(error).into()),
    };
    format_verdict(format, version, existing_tables).map(|_| ())
}

pub(super) fn initialize(database: &Database) -> Result<(u64, JobId, Vec<JobId>), OpenError> {
    let mut tx = database.begin_write().map_err(StorageError::from)?;
    tx.set_durability(Durability::Immediate)
        .map_err(StorageError::from)?;
    let existing_tables = tx.list_tables().map_err(StorageError::from)?.count();
    let now = Timestamp::now();
    init_promotion_clock(now.as_unix_micros());

    let (epoch, clean_shutdown) = {
        let mut meta = tx.open_table(META).map_err(StorageError::from)?;
        let format = meta
            .get("format")
            .map_err(StorageError::from)?
            .map(|value| value.value().to_vec());
        let version = meta
            .get("format_version")
            .map_err(StorageError::from)?
            .map(|value| value.value().to_vec());
        let verdict = match format_verdict(format, version, existing_tables) {
            Ok(verdict) => verdict,
            Err(refusal) => {
                drop(meta);
                tx.abort().map_err(StorageError::from)?;
                return Err(refusal);
            }
        };
        if matches!(verdict, FormatVerdict::New) {
            meta.insert("format", FORMAT_NAME)
                .map_err(StorageError::from)?;
            meta.insert("format_version", FORMAT_VERSION.to_le_bytes().as_slice())
                .map_err(StorageError::from)?;
            meta.insert("open_epoch", 0u64.to_le_bytes().as_slice())
                .map_err(StorageError::from)?;
            meta.insert("clean_shutdown", [1u8].as_slice())
                .map_err(StorageError::from)?;
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
    if !clean_shutdown {
        derived::rebuild(&tx, promotion_clock())?;
    }
    let mut batch = Batch::new(tx, epoch);
    let abandoned = reconcile(&mut batch);
    let abandoned = match (abandoned, batch.failed.take()) {
        (Ok(abandoned), None) => abandoned,
        (_, Some(failure)) | (Err(BatchError(failure)), None) => {
            let error = match failure {
                Failure::Storage(error) => error,
                Failure::Rejected(violation) => {
                    StorageError::corrupted(format!("open reconcile refused: {violation}"))
                }
            };
            batch.tx.abort().map_err(StorageError::from)?;
            return Err(error.into());
        }
    };
    batch.tx.commit().map_err(StorageError::from)?;
    Ok((epoch, JobId::new(0), abandoned))
}

pub(super) fn create_tables(tx: &WriteTransaction) -> Result<(), StorageError> {
    tx.open_table(META)?;
    tx.open_table(SEQUENCES)?;
    tx.open_table(CONFIGS)?;
    tx.open_table(JOBS)?;
    tx.open_table(JOB_STATES)?;
    tx.open_table(JOB_OUTPUTS)?;
    tx.open_table(SUBMISSIONS)?;
    tx.open_table(ATTACHMENTS)?;
    tx.open_table(TOOL_CALLS)?;
    tx.open_table(REQUEST_EVENTS)?;
    tx.open_table(MESSAGES)?;
    tx.open_table(PROJECTS)?;
    tx.open_table(CONVERSATIONS)?;
    tx.open_table(PENDING)?;
    tx.open_table(READY)?;
    tx.open_table(DELAYED)?;
    tx.open_table(DEPENDENCY_WAITERS)?;
    tx.open_table(UNSETTLED)?;
    tx.open_table(HELD_SLOTS)?;
    tx.open_table(CHILDREN)?;
    tx.open_table(SUCCESSORS)?;
    tx.open_table(TURN_INPUTS)?;
    tx.open_table(NOTICES)?;
    tx.open_table(TIPS)?;
    tx.open_table(RECENT)?;
    tx.open_table(CHILD_CONVERSATIONS)?;
    tx.open_table(LIVE_CHILDREN)?;
    tx.open_table(USER_ENTRY_AT)?;
    tx.open_table(NOTICE_READ)?;
    tx.open_table(ANSWER_TIPS)?;
    tx.open_table(ACTIVE_TURNS)?;
    tx.open_table(TOOL_CALL_IDS)?;
    tx.open_table(TOOL_TOTALS)?;
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
    Ok(())
}

pub(super) fn allocate(tx: &WriteTransaction, sequence: Sequence) -> Result<u64, StorageError> {
    let mut table = tx.open_table(SEQUENCES)?;
    let next = table.get(sequence.key())?.map_or(0, |value| value.value()) + 1;
    table.insert(sequence.key(), next)?;
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
        let mut states = Vec::new();
        for entry in tx.open_table(JOB_STATES)?.iter()? {
            let (key, bytes) = entry?;
            states.push(JobStateRow::decode_at(key.value(), bytes.value())?);
        }
        Ok(states)
    })?;
    let pending: Vec<_> = states
        .iter()
        .filter(|state| state.claim.is_none() && state.outcome.is_none())
        .map(|state| state.job_id)
        .collect();
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
    for job in pending.into_iter().rev() {
        if matches!(
            batch.require_job(job)?.kind,
            JobKind::Turn {
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
            if row.kind.is_stored()
                && let Some(conversation) = row.parent_id
            {
                batch.step(|tx| {
                    tx.open_table(ACTIVE_TURNS)?.remove(conversation.get())?;
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
            record.reconciliation = Some(crate::persistence::Recorded {
                fact: crate::persistence::ToolReconciliation {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::DatabaseWorker;
    use crate::persistence::{JobInputDraft, NewJob};
    use crate::test_support::{data_directory, execution_configuration, wait};
    use redb::ReadableTableMetadata;

    #[test]
    fn open_cancels_every_previous_pending_readiness_state_and_settles_the_graph() {
        for clean in [true, false] {
            let (_root, directory) =
                data_directory("open-pending", if clean { "clean" } else { "unclean" });
            let worker = DatabaseWorker::start(&directory).expect("database");
            let database = worker.handle();
            let spec = NewJob {
                process_grant: None,
                name: None,
                eligible_at: None,
                parent: database.genesis(),
                project: None,
                input: JobInputDraft::default(),
                configuration: Some(execution_configuration(&database)),
                previous: None,
            };
            let parent = wait(database.create_job(spec.clone())).expect("ready parent");
            let child = wait(database.create_job(NewJob {
                parent,
                ..spec.clone()
            }))
            .expect("child");
            let waiter = wait(database.create_job(spec.clone())).expect("blocked root");
            let delayed = wait(database.create_job(NewJob {
                eligible_at: Some(Timestamp::from_unix_micros(
                    Timestamp::now().as_unix_micros() + 3_600_000_000,
                )),
                ..spec
            }))
            .expect("delayed root");
            worker.shutdown().expect("shutdown");
            if !clean {
                let database =
                    Database::open(database_file(directory.database_path())).expect("raw file");
                let tx = database.begin_write().expect("write");
                tx.open_table(META)
                    .expect("meta")
                    .insert("clean_shutdown", [0u8].as_slice())
                    .expect("unclean marker");
                tx.commit().expect("marker committed");
            }
            let worker = DatabaseWorker::start(&directory).expect("reopen");
            let database = worker.handle();
            for job in [parent, child, waiter, delayed] {
                let record = wait(database.job(job)).expect("read").expect("Job");
                let outcome = record.outcome.expect("abandoned pending Job");
                assert_eq!(
                    (outcome.kind, outcome.cause),
                    (OutcomeKind::Cancelled, Some(OutcomeCause::Abandoned))
                );
                assert!(
                    record.claim.is_none()
                        && record.children_closed_at.is_some()
                        && record.subtree_settled
                );
            }
            wait(database.with_database(|database| {
                let tx = database.begin_read()?;
                assert_eq!(tx.open_table(PENDING)?.len()?, 0);
                assert_eq!(tx.open_table(READY)?.len()?, 0);
                assert_eq!(tx.open_table(DELAYED)?.len()?, 0);
                assert_eq!(tx.open_table(DEPENDENCY_WAITERS)?.len()?, 0);
                assert_eq!(tx.open_table(UNSETTLED)?.len()?, 0);
                Ok(())
            }))
            .expect("pending indexes and settlement counts are empty");
            worker.shutdown().expect("shutdown");
        }
    }
}
