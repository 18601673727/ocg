use std::sync::OnceLock;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use super::*;
use crate::db::entities::{JobId, OutcomeKind, Timestamp};

static PROMOTION_CLOCK_BASE: OnceLock<(i64, Instant)> = OnceLock::new();

pub(crate) fn init_promotion_clock(wall_time_at_open: i64) {
    let _ = PROMOTION_CLOCK_BASE.set((wall_time_at_open, Instant::now()));
}

static PROMOTION_HIGH_WATER: AtomicI64 = AtomicI64::new(i64::MIN);

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

pub(crate) struct OpenedFile {
    pub(crate) database: Database,
    pub(crate) epoch: u64,
    pub(crate) genesis: JobId,
    pub(crate) abandoned: Vec<JobId>,
}

pub(crate) fn open(directory: &Path) -> Result<OpenedFile, StorageError> {
    let path = database_file(directory);
    let mut options = std::fs::OpenOptions::new();
    options.read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let (file, fresh) = match options.open(&path) {
        Ok(file) => (file, false),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let mut create = std::fs::OpenOptions::new();
            create.read(true).write(true).create_new(true);
            let file = crate::system::data_directory::private_file(&mut create)
                .open(&path)
                .map_err(io_storage_error)?;
            (file, true)
        }
        Err(error) => return Err(io_storage_error(error)),
    };
    let metadata = file.metadata().map_err(io_storage_error)?;
    if !metadata.is_file() {
        return Err(StorageError::corrupted("database is not a regular file"));
    }
    if !fresh {
        if metadata.len() == 0 {
            return Err(unsupported_format(
                "existing database is empty; refusing initialization",
            ));
        }
        check_database_identity(&file, &path)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            file.set_permissions(std::fs::Permissions::from_mode(0o600))
                .map_err(io_storage_error)?;
        }
    }
    let mut builder = Database::builder();
    builder.set_cache_size(CACHE_BYTES);
    let database = builder.create_file(file).map_err(StorageError::from)?;
    let (epoch, genesis, abandoned) = initialize(&database)?;
    Ok(OpenedFile {
        database,
        epoch,
        genesis,
        abandoned,
    })
}

fn io_storage_error(error: std::io::Error) -> StorageError {
    StorageError {
        kind: StorageErrorKind::Io(error.kind()),
        os_error: error.raw_os_error(),
        detail: error.to_string(),
    }
}

fn check_database_identity(file: &std::fs::File, path: &Path) -> Result<(), StorageError> {
    let actual = file.metadata().map_err(io_storage_error)?;
    let named = std::fs::symlink_metadata(path).map_err(io_storage_error)?;
    if !named.is_file() || !same_file_identity(&actual, &named) {
        return Err(StorageError::corrupted("database pathname changed"));
    }
    Ok(())
}

fn same_file_identity(actual: &std::fs::Metadata, named: &std::fs::Metadata) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        actual.dev() == named.dev() && actual.ino() == named.ino()
    }
    #[cfg(windows)]
    {
        let _ = (actual, named);
        true
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (actual, named);
        false
    }
}

pub(super) fn check_identity(
    meta: &impl ReadableTable<&'static str, &'static [u8]>,
) -> Result<(), StorageError> {
    let found = match meta.get(DATABASE_FORMAT_KEY)? {
        None => {
            return Err(unsupported_format(&format!(
                "unmarked database; this build opens only format {DATABASE_FORMAT_VERSION}"
            )));
        }
        Some(value) => <[u8; 8]>::try_from(value.value())
            .map(u64::from_le_bytes)
            .map_err(|_| unsupported_format("database format marker is malformed"))?,
    };
    if found != DATABASE_FORMAT_VERSION {
        return Err(unsupported_format(&format!(
            "database format {found} is not supported; this build opens only format {DATABASE_FORMAT_VERSION}; run `ocg migrate --check`"
        )));
    }
    if !meta
        .get(SCHEMA_FINGERPRINT_KEY)?
        .is_some_and(|value| value.value() == SCHEMA_FINGERPRINT)
    {
        return Err(unsupported_format(&format!(
            "database format {found} has a different schema fingerprint; run `ocg migrate --check`"
        )));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Verification {
    Quick,
    Full,
}

pub(crate) fn verify_full(
    database: &impl ::redb::ReadableDatabase,
) -> Result<Verification, StorageError> {
    startup_checks(database)
}

fn startup_checks(database: &impl ::redb::ReadableDatabase) -> Result<Verification, StorageError> {
    let tx = database.begin_read()?;
    let clean = startup_metadata(&tx)?;
    check_native_invariants(&tx)?;
    Ok(if clean {
        Verification::Quick
    } else {
        Verification::Full
    })
}

fn startup_metadata(tx: &ReadTransaction) -> Result<bool, StorageError> {
    let meta = match tx.open_table(META) {
        Ok(meta) => meta,
        Err(TableError::TableDoesNotExist(_)) => {
            return Err(unsupported_format(
                "not an OCG database: it has no metadata table",
            ));
        }
        Err(error) => return Err(error.into()),
    };
    check_identity(&meta)?;
    let epoch = meta
        .get("open_epoch")?
        .ok_or_else(|| StorageError::corrupted("missing open epoch"))?;
    let shutdown = meta
        .get("clean_shutdown")?
        .ok_or_else(|| StorageError::corrupted("missing shutdown marker"))?;
    if epoch.value().len() != 8 || !matches!(shutdown.value(), [0] | [1]) {
        return Err(StorageError::corrupted("invalid startup metadata"));
    }
    let clean = matches!(shutdown.value(), [1]);
    reader::global_compaction_settings(&meta)?;
    Ok(clean)
}

pub(super) fn verify_migrated(
    database: &impl ::redb::ReadableDatabase,
) -> Result<(), StorageError> {
    startup_checks(database)?;
    Ok(())
}

fn check_native_invariants(tx: &ReadTransaction) -> Result<(), StorageError> {
    if tx.open_table(POOL_ALLOCATIONS).is_err()
        || tx.open_table(CONFIGS).is_err()
        || tx.open_table(ACTIVE_ALLOCATIONS).is_err()
        || tx.open_table(ALLOCATION_REQUESTS).is_err()
    {
        return Err(StorageError::corrupted(
            "an Allocation table or index is missing",
        ));
    }
    Ok(())
}

pub(super) fn check_preflight_header(bytes: &[u8], tags: &[u8]) -> Result<(), StorageError> {
    let valid = match bytes {
        [tag, version, ..] if tags.contains(tag) => *version == codec::RECORD_VERSION,
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(unsupported_format(
            "unsupported or malformed record encoding",
        ))
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
            meta.insert(SCHEMA_FINGERPRINT_KEY, SCHEMA_FINGERPRINT)?;
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
            check_identity(&meta)?;
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
    let mut batch = Batch::new(tx, epoch);
    if empty {
        use crate::db::entities::{AllocationExecutor, AllocationPool};
        let pool_id =
            ExecutionPoolId::new(
                batch
                    .next_id()
                    .map_err(|BatchError(failure)| match failure {
                        Failure::Storage(error) => error,
                        Failure::Rejected(violation) => {
                            StorageError::corrupted(format!("default Pool ID refused: {violation}"))
                        }
                    })?,
            );
        let executor_id =
            ExecutorId::new(
                batch
                    .next_id()
                    .map_err(|BatchError(failure)| match failure {
                        Failure::Storage(error) => error,
                        Failure::Rejected(violation) => StorageError::corrupted(format!(
                            "default Executor ID refused: {violation}"
                        )),
                    })?,
            );
        let pool = AllocationPool {
            id: pool_id,
            name: "Default".to_owned(),
            capacity: std::num::NonZeroU32::MIN,
            enabled: true,
            archived: false,
            revision: 1,
        };
        let executor = AllocationExecutor {
            id: executor_id,
            pool: pool_id,
            name: "Local Executor".to_owned(),
            enabled: true,
            archived: false,
            revision: 1,
            capabilities: [
                LlmTransport::OpenAiChatCompletions,
                LlmTransport::OpenAiResponses,
                LlmTransport::AnthropicMessages,
            ]
            .into_iter()
            .map(|protocol| (ExecutionOperation::LlmRequest, Some(protocol)))
            .collect(),
        };
        batch
            .step(|tx| {
                let mut configs = tx.open_table(CONFIGS)?;
                configs.insert(
                    pool_id.get(),
                    transactions::encode_allocation_pool(&pool).as_slice(),
                )?;
                configs.insert(
                    executor_id.get(),
                    transactions::encode_allocation_executor(&executor).as_slice(),
                )?;
                Ok(())
            })
            .map_err(|BatchError(failure)| match failure {
                Failure::Storage(error) => error,
                Failure::Rejected(violation) => StorageError::corrupted(format!(
                    "default execution resources refused: {violation}"
                )),
            })?;
    }
    let abandoned = {
        let rebuild = !clean_shutdown
            || indexes_missing
            || std::env::var_os("OCG_VERIFY_STARTUP").is_some_and(|value| value == "full");
        if rebuild {
            rebuild_mutable_jobs(&mut batch).map_err(|BatchError(failure)| match failure {
                Failure::Storage(error) => error,
                Failure::Rejected(violation) => {
                    StorageError::corrupted(format!("new-model recovery refused: {violation}"))
                }
            })?;
        }
        Vec::new()
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
    tx.open_table(CREDENTIALS)?;
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
    tx.open_table(MUTABLE_READY)?;
    tx.open_table(DELAYED)?;
    tx.open_table(DEPENDENCY_WAITERS)?;
    tx.open_table(UNSETTLED)?;
    tx.open_table(POOL_ALLOCATIONS)?;
    tx.open_table(ACTIVE_ALLOCATIONS)?;
    tx.open_table(ALLOCATION_REQUESTS)?;
    tx.open_table(DRAFT_CONTEXTS)?;
    tx.open_table(CHILDREN)?;
    tx.open_table(SUCCESSORS)?;
    tx.open_table(JOB_INPUTS)?;
    tx.open_table(NOTICES)?;
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
    tx.open_table(USAGE_MODEL)?;
    tx.open_table(REQUEST_TIMELINE)?;
    tx.open_table(SCHEDULED_EXTERNAL_POLICY)?;
    tx.open_table(SCHEDULED_EXTERNAL_BINDING)?;
    Ok(())
}

fn rebuild_mutable_jobs(batch: &mut Batch) -> Result<(), BatchError> {
    use crate::db::entities::MutableJobPhase;
    batch.step(|tx| {
        indexes::rebuild_allocation_indexes(tx)?;
        tx.delete_table(DRAFT_CONTEXTS)?;
        tx.open_table(DRAFT_CONTEXTS)?;
        for entry in tx.open_table(POOL_ALLOCATIONS)?.iter()? {
            let (key, value) = entry?;
            if transactions::decode_pool_allocation(key.value(), value.value())?.holds_capacity() {
                return Err(StorageError::corrupted(
                    "recovery is disabled and this database holds Allocations; run `ocg inspect-held`",
                ));
            }
        }
        tx.delete_table(MUTABLE_READY)?;
        tx.delete_table(PENDING)?;
        tx.delete_table(CONVERSATIONS)?;
        tx.delete_table(TURNS)?;
        tx.delete_table(CHILDREN)?;
        tx.delete_table(SUCCESSORS)?;
        tx.delete_table(UNSETTLED)?;
        tx.delete_table(CONVERSATION_SEARCH)?;
        tx.delete_table(GROUP_RESULTS)?;
        tx.open_table(MUTABLE_READY)?;
        tx.open_table(PENDING)?;
        tx.open_table(CONVERSATIONS)?;
        tx.open_table(TURNS)?;
        tx.open_table(CHILDREN)?;
        tx.open_table(SUCCESSORS)?;
        tx.open_table(UNSETTLED)?;
        tx.open_table(CONVERSATION_SEARCH)?;
        tx.open_table(GROUP_RESULTS)?;
        Ok(())
    })?;
    let mut after = None;
    loop {
        let next = batch.step(|tx| {
            use std::ops::Bound::{Excluded, Unbounded};
            let jobs = tx.open_table(JOBS)?;
            let mut range = jobs.range((after.map_or(Unbounded, Excluded), Unbounded))?;
            let Some(entry) = range.next() else {
                return Ok(None);
            };
            let (key, value) = entry?;
            let state = tx
                .open_table(JOB_STATES)?
                .get(key.value())?
                .map(|row| JobState::decode_at(key.value(), row.value()))
                .transpose()?
                .ok_or_else(|| StorageError::corrupted("Job state is missing"))?;
            if value.value().first() == Some(&JobSpec::MUTABLE_CONVERSATION_TAG) {
                let spec = JobSpec::decode_at(key.value(), value.value())?;
                return Ok(Some((
                    key.value(),
                    Err((spec.parent_id, state.outcome.is_none())),
                )));
            }
            let job = transactions::decode_mutable_job(key.value(), value.value())?;
            Ok(Some((key.value(), Ok((job, state)))))
        })?;
        let Some((id, row)) = next else {
            break;
        };
        after = Some(id);
        let (job, state) = match row {
            Ok(turn) => turn,
            Err((parent, live)) => {
                batch.insert(CONVERSATIONS, id, ())?;
                batch.insert(CHILDREN, (parent.map_or(0, JobId::get), id), ())?;
                batch.insert(UNSETTLED, id, (0, 0))?;
                if let Some(parent) = parent {
                    batch.insert(CHILD_GROUPS, (parent.get(), id), ())?;
                    if live {
                        batch.insert(LIVE_CHILDREN, (parent.get(), id), ())?;
                    }
                }
                continue;
            }
        };
        if job.summary_source.is_some() {
            continue;
        }
        if job.phase != MutableJobPhase::Unsubmitted {
            batch.insert(TURNS, (job.parent_id.get(), job.id.get()), ())?;
            batch.step(|tx| {
                indexes::update_mutable_turn_search(tx, job.parent_id, job.id, None, &job.input)
            })?;
            if let Some(outcome) = &state.outcome {
                batch.step(|tx| {
                    let outputs = tx.open_table(JOB_OUTPUTS)?;
                    let mut result = None;
                    for entry in outputs.range((job.id.get(), 0)..=(job.id.get(), u32::MAX))? {
                        let (key, value) = entry?;
                        if rows::output_kind(value.value())? == OutputKind::Result {
                            result = Some(decode_output(key.value(), value.value())?);
                        }
                    }
                    if let Some(result) = result {
                        indexes::append_output_search_index(
                            tx,
                            job.parent_id,
                            job.id,
                            &result,
                            outcome.kind == OutcomeKind::Completed,
                        )?;
                        let mut results = tx.open_table(GROUP_RESULTS)?;
                        let old = results
                            .get(job.parent_id.get())?
                            .map_or((0, 0), |v| v.value());
                        let latest = if outcome.kind == OutcomeKind::Cancelled {
                            old.0
                        } else {
                            old.0.max(job.id.get())
                        };
                        results.insert(job.parent_id.get(), (latest, old.1.max(job.id.get())))?;
                    }
                    Ok(())
                })?;
            }
            batch.insert(CHILDREN, (job.parent_id.get(), job.id.get()), ())?;
            if let Some(previous) = job.previous {
                batch.insert(SUCCESSORS, previous.get(), ())?;
            }
            if state
                .outcome
                .as_ref()
                .is_none_or(|outcome| outcome.kind == OutcomeKind::Unknown)
            {
                batch.insert(UNSETTLED, job.id.get(), (0, 0))?;
                batch.step(|tx| {
                    let mut unsettled = tx.open_table(UNSETTLED)?;
                    let (dependencies, children) = unsettled
                        .get(job.parent_id.get())?
                        .map(|v| v.value())
                        .ok_or_else(|| {
                            StorageError::corrupted("conversation unsettled row missing")
                        })?;
                    unsettled.insert(job.parent_id.get(), (dependencies, children + 1))?;
                    Ok(())
                })?;
            }
        }
        if state.outcome.is_some() {
            continue;
        }
        match job.phase {
            MutableJobPhase::Unsubmitted => {
                let duplicate = batch.step(|tx| {
                    Ok(tx
                        .open_table(DRAFT_CONTEXTS)?
                        .insert(job.composer_context.get(), job.id.get())?
                        .is_some())
                })?;
                if duplicate {
                    return batch.corrupted("duplicate unsubmitted Composer context".to_owned());
                }
            }
            MutableJobPhase::Queued => {
                if matches!(
                    &job.input.content,
                    crate::db::entities::MutableJobContent::Generative { .. }
                ) {
                    match batch.require_generative_predecessors(&job) {
                        Err(BatchError(Failure::Rejected(
                            crate::db::entities::Violation::InvalidValue(
                                "execution_context",
                                "Generative Jobs cannot follow Decision history",
                            ),
                        ))) => {
                            batch.insert(PENDING, job.id.get(), ())?;
                            continue;
                        }
                        Err(BatchError(Failure::Rejected(_))) | Ok(()) => {}
                        Err(error) => return Err(error),
                    }
                }
                let requirement = match &job.frozen {
                    Some(crate::db::entities::MutableFrozenConfiguration::Decision(_)) => {
                        transactions::decision_requirement()
                    }
                    Some(crate::db::entities::MutableFrozenConfiguration::Generative(frozen)) => {
                        transactions::execution_requirement(frozen)
                    }
                    None => {
                        return batch
                            .corrupted("queued Job has no frozen configuration".to_owned());
                    }
                };
                let submitted = job.submitted_at.ok_or_else(|| {
                    BatchError(Failure::Storage(StorageError::corrupted(
                        "queued Job missing submission time",
                    )))
                })?;
                batch.step(|tx| {
                    tx.open_table(MUTABLE_READY)?.insert(
                        (
                            transactions::mutable_priority(job.input.priority),
                            submitted.as_unix_micros(),
                            job.id.get(),
                            requirement,
                        ),
                        (),
                    )?;
                    tx.open_table(PENDING)?.insert(job.id.get(), ())?;
                    Ok(())
                })?;
            }
            MutableJobPhase::Executing => {
                return batch.corrupted("executing Job requires allocation recovery".to_owned());
            }
        }
    }
    settle_rebuilt_conversations(batch)
}

fn settle_rebuilt_conversations(batch: &mut Batch) -> Result<(), BatchError> {
    let mut before: Option<u128> = None;
    loop {
        let next = batch.step(|tx| {
            use std::ops::Bound::{Excluded, Unbounded};
            let conversations = tx.open_table(CONVERSATIONS)?;
            let mut range =
                conversations.range::<u128>((Unbounded, before.map_or(Unbounded, Excluded)))?;
            let Some(entry) = range.next_back() else {
                return Ok(None);
            };
            let id = entry?.0.value();
            let spec = {
                let jobs = tx.open_table(JOBS)?;
                let row = jobs
                    .get(id)?
                    .ok_or_else(|| StorageError::corrupted("conversation row is missing"))?;
                JobSpec::decode_at(id, row.value())?
            };
            let outcome = tx
                .open_table(JOB_STATES)?
                .get(id)?
                .map(|row| JobState::decode_at(id, row.value()))
                .transpose()?
                .ok_or_else(|| StorageError::corrupted("conversation state is missing"))?
                .outcome
                .is_some();
            let mut unsettled = tx.open_table(UNSETTLED)?;
            let (_, children) = unsettled
                .get(id)?
                .map(|row| row.value())
                .ok_or_else(|| StorageError::corrupted("conversation unsettled row missing"))?;
            if outcome && children == 0 {
                unsettled.remove(id)?;
            } else if let Some(parent) = spec.parent_id {
                let (dependencies, count) = unsettled
                    .get(parent.get())?
                    .map(|row| row.value())
                    .ok_or_else(|| StorageError::corrupted("parent unsettled row missing"))?;
                let count = count
                    .checked_add(1)
                    .ok_or_else(|| StorageError::corrupted("conversation child count overflow"))?;
                unsettled.insert(parent.get(), (dependencies, count))?;
            }
            Ok(Some(id))
        })?;
        let Some(id) = next else {
            return Ok(());
        };
        before = Some(id);
    }
}

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

fn unsupported_format(detail: &str) -> StorageError {
    StorageError {
        kind: StorageErrorKind::Other,
        os_error: None,
        detail: detail.to_owned(),
    }
}
