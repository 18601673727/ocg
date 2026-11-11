use std::sync::OnceLock;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use super::*;
use crate::db::entities::{JobId, OutcomeKind, Timestamp};
use crate::system::execution_controls::ExecutionControl;

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
    let mut options = std::fs::OpenOptions::new();
    options.read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let mut verification = Verification::Full;
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
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Err(StorageError::corrupted(
                "database must not have multiple hard links",
            ));
        }
    }
    if !fresh {
        if metadata.len() == 0 {
            return Err(unsupported_format(
                "existing database is empty; refusing initialization",
            ));
        }
        check_database_identity(&file, &path)?;
        verification = preflight(&file, &path, directory)?;
        check_database_identity(&file, &path)?;
        if file.metadata().map_err(io_storage_error)?.len() == 0 {
            return Err(StorageError::corrupted(
                "database became empty during preflight",
            ));
        }
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
    let (epoch, genesis, abandoned) = initialize(&database, verification)?;
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
        // The cooperative lock excludes OCG pathname replacement; stable
        // Windows metadata APIs do not expose a file identity here.
        let _ = (actual, named);
        true
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (actual, named);
        false
    }
}

/// Refuses any database whose declared identity is not this build's, with a
/// message that names what was found. No migration exists: an older or newer
/// database is archived and replaced, never reinterpreted.
pub(super) fn check_identity(
    meta: &impl ReadableTable<&'static str, &'static [u8]>,
) -> Result<(), StorageError> {
    let found = match meta.get(DATABASE_FORMAT_KEY)? {
        None => {
            return Err(unsupported_format(&format!(
                "unmarked database; this build opens only format {DATABASE_FORMAT_VERSION}; run `ocg migrate --check` against a format 22 database"
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

/// How much of the database `open` verified before it opened the file for
/// writing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Verification {
    /// The file opened without repair, shut down cleanly, and holds no
    /// capacity-holding Allocation: only its identity was read.
    Quick,
    /// Every record header, derived index and ownership row was checked.
    Full,
}

/// Set `OCG_VERIFY_STARTUP=full` to make every open do the full verification
/// and rebuild, as a database that did not shut down cleanly always does.
fn full_verification_requested() -> bool {
    std::env::var_os("OCG_VERIFY_STARTUP").is_some_and(|value| value == "full")
}

fn validate_preflight(
    database: &impl ::redb::ReadableDatabase,
) -> Result<Verification, StorageError> {
    startup_checks(database, full_verification_requested())
}

/// The full startup checks on a database nothing will open for writing.
pub(crate) fn verify_full(
    database: &impl ::redb::ReadableDatabase,
) -> Result<Verification, StorageError> {
    startup_checks(database, true)
}

/// Verifies a published held-recovery result without rejecting Allocations
/// that the plan explicitly left held. Released Unknown outcomes still need
/// their own durable stop proof; every record and derived index is checked.
pub(crate) fn verify_recovered(
    database: &impl ::redb::ReadableDatabase,
) -> Result<Verification, StorageError> {
    let tx = database.begin_read()?;
    let meta = tx.open_table(META)?;
    check_identity(&meta)?;
    check_required_tables(&tx)?;
    let epoch = meta
        .get("open_epoch")?
        .ok_or_else(|| StorageError::corrupted("missing open epoch"))?;
    let shutdown = meta
        .get("clean_shutdown")?
        .ok_or_else(|| StorageError::corrupted("missing shutdown marker"))?;
    if epoch.value().len() != 8 || shutdown.value() != [0] {
        return Err(StorageError::corrupted(
            "held recovery must preserve clean_shutdown = 0",
        ));
    }
    let settings = meta
        .get("compaction_settings")?
        .ok_or_else(|| StorageError::corrupted("missing compaction settings"))?;
    check_preflight_header(settings.value(), &[TAG_GLOBAL_COMPACTION])?;
    reader::global_compaction_settings(&meta)?;
    drop(meta);
    check_records(&tx)?;
    for entry in tx.open_table(JOB_STATES)?.iter()? {
        let (key, value) = entry?;
        let state = JobState::decode_at(key.value(), value.value())?;
        if state
            .outcome
            .as_ref()
            .is_some_and(|outcome| outcome.kind == OutcomeKind::Unknown)
            && !resolved_unknown_ownership(&tx, &state)?
        {
            let retained = tx
                .open_table(POOL_ALLOCATIONS)?
                .get(state.job_id.get())?
                .map(|row| transactions::decode_pool_allocation(state.job_id.get(), row.value()))
                .transpose()?
                .is_some_and(|allocation| allocation.holds_capacity());
            if !retained {
                return Err(StorageError::corrupted(format!(
                    "{} is unresolved Unknown without a retained held Allocation",
                    state.job_id
                )));
            }
        }
    }
    Ok(Verification::Full)
}

fn startup_checks(
    database: &impl ::redb::ReadableDatabase,
    force_full: bool,
) -> Result<Verification, StorageError> {
    let tx = database.begin_read()?;
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
    check_required_tables(&tx)?;
    let epoch = meta
        .get("open_epoch")?
        .ok_or_else(|| StorageError::corrupted("missing open epoch"))?;
    let shutdown = meta
        .get("clean_shutdown")?
        .ok_or_else(|| StorageError::corrupted("missing shutdown marker"))?;
    if epoch.value().len() != 8 || !matches!(shutdown.value(), [0] | [1]) {
        return Err(StorageError::corrupted("invalid startup metadata"));
    }
    let settings = meta
        .get("compaction_settings")?
        .ok_or_else(|| StorageError::corrupted("missing compaction settings"))?;
    check_preflight_header(settings.value(), &[TAG_GLOBAL_COMPACTION])?;
    reader::global_compaction_settings(&meta)?;
    if matches!(shutdown.value(), [1])
        && !force_full
        && allocations_are_quiet(&tx)?
        && !unknown_outcomes_exist(&tx)?
    {
        return Ok(Verification::Quick);
    }
    check_records(&tx)?;
    refuse_unresolved_ownership(&tx)?;
    Ok(Verification::Full)
}

/// A format this build accepts names the tables of that format. A database
/// that claims the current format but lacks a required table is a partial
/// migration, not an old one: it is refused rather than silently extended.
fn check_required_tables(tx: &ReadTransaction) -> Result<(), StorageError> {
    use ::redb::TableHandle;
    let present: Vec<String> = tx
        .list_tables()?
        .map(|table| table.name().to_owned())
        .collect();
    for required in [
        SCHEDULED_EXTERNAL_POLICY.name(),
        SCHEDULED_EXTERNAL_BINDING.name(),
    ] {
        if !present.iter().any(|table| table == required) {
            return Err(StorageError::corrupted(format!(
                "database is marked format {DATABASE_FORMAT_VERSION} but lacks the {required} table; it is a partial migration, not a supported database"
            )));
        }
    }
    Ok(())
}

/// Every record header and the native invariants, without any judgement of
/// who owns capacity.
fn check_records(tx: &ReadTransaction) -> Result<(), StorageError> {
    migration_check_headers(tx)?;
    check_native_invariants(tx)
}

/// The record-header checks of every authoritative table, shared with the
/// offline migration's inspection of the preceding format. Those tables that
/// the preceding format does not have are simply absent and checked as empty.
pub(super) fn migration_check_headers(tx: &ReadTransaction) -> Result<(), StorageError> {
    macro_rules! headers {
        ($($table:ident => $tags:expr),+ $(,)?) => { $(check_preflight_table(tx, $table, $tags)?;)+ };
    }
    headers!(CONFIGS => &[1,40,41], JOBS => &[2,53], JOB_STATES => &[3], JOB_INPUTS => &[11,52], POOL_ALLOCATIONS => &[42]);
    headers!(JOB_OUTPUTS => &[4], SUBMISSIONS => &[5], ATTACHMENTS => &[6], TOOL_CALLS => &[7],
        REQUEST_EVENTS => &[8], PROJECTS => &[9], JOB_CONTROLS => &[10], NOTICES => &[12],
        RECURRENCES => &[13], APPROVALS => &[15]);
    check_preflight_table(tx, CREDENTIALS, &[16])?;
    check_preflight_table(tx, SCHEDULED_EXTERNAL_POLICY, &[18])?;
    check_preflight_table(tx, SCHEDULED_EXTERNAL_BINDING, &[19])?;
    Ok(())
}

/// The native invariants and ownership policy a normal host applies, read-only,
/// for the offline migration's inspection of its source. A clean, quiet
/// database passes as the host's quick path would let it; any other must hold
/// no capacity and no Unknown outcome, as the host's full verification
/// requires. Nothing is released, adopted or cleared here.
pub(super) fn migration_check_native(tx: &ReadTransaction) -> Result<(), StorageError> {
    check_native_invariants(tx)?;
    let clean = tx
        .open_table(META)?
        .get("clean_shutdown")?
        .is_some_and(|value| matches!(value.value(), [1]));
    if clean && allocations_are_quiet(tx)? {
        return Ok(());
    }
    refuse_unresolved_ownership(tx)
}

/// What the offline migration requires of its staged destination before it is
/// published: the very checks a host's startup preflight runs on a database
/// (identity, required tables, metadata, and the quick-or-full ownership
/// decision), and the record headers and native invariants even where the
/// host's quick path would skip them. It opens nothing for writing, so no
/// epoch or shutdown marker changes.
pub(super) fn verify_migrated(
    database: &impl ::redb::ReadableDatabase,
) -> Result<(), StorageError> {
    if startup_checks(database, false)? == Verification::Quick {
        check_records(&database.begin_read()?)?;
    }
    Ok(())
}

/// Whether the Allocation rows, the source of ownership, hold no capacity and
/// the stored indexes equal what they compute. This reads the Allocation rows
/// (one small row per Allocation ever made) and the Pool configurations they
/// name, never the large Job tables. A missing index, a legacy slot table, a
/// corrupt row or any difference is not a quick-path database: the full checks
/// then run and report it.
fn allocations_are_quiet(tx: &ReadTransaction) -> Result<bool, StorageError> {
    if tx.list_tables()?.any(|table| {
        use ::redb::TableHandle;
        table.name() == HELD_SLOTS.name()
    }) {
        return Ok(false);
    }
    let (allocations, configs, active, requests) = match (
        tx.open_table(POOL_ALLOCATIONS),
        tx.open_table(CONFIGS),
        tx.open_table(ACTIVE_ALLOCATIONS),
        tx.open_table(ALLOCATION_REQUESTS),
    ) {
        (Ok(allocations), Ok(configs), Ok(active), Ok(requests)) => {
            (allocations, configs, active, requests)
        }
        _ => return Ok(false),
    };
    let Ok(computed) = indexes::compute_allocation_indexes(&allocations, &configs) else {
        return Ok(false);
    };
    Ok(computed.active.is_empty()
        && computed == indexes::stored_allocation_indexes(&active, &requests)?)
}

/// Read-only fail-closed checks of the facts the native model relies on: the
/// Allocation indexes equal what their source computes, and no Job state
/// carries a legacy claim (a claim is a lease nothing in this build honors).
fn check_native_invariants(tx: &ReadTransaction) -> Result<(), StorageError> {
    let (allocations, configs, active, requests) = match (
        tx.open_table(POOL_ALLOCATIONS),
        tx.open_table(CONFIGS),
        tx.open_table(ACTIVE_ALLOCATIONS),
        tx.open_table(ALLOCATION_REQUESTS),
    ) {
        (Ok(allocations), Ok(configs), Ok(active), Ok(requests)) => {
            (allocations, configs, active, requests)
        }
        _ => {
            return Err(StorageError::corrupted(
                "an Allocation table or index is missing",
            ));
        }
    };
    if !indexes::allocation_indexes_agree(&allocations, &configs, &active, &requests)? {
        return Err(StorageError::corrupted(
            "the Allocation indexes differ from their source rows",
        ));
    }
    if let Ok(states) = tx.open_table(JOB_STATES) {
        for entry in states.iter()? {
            let (key, value) = entry?;
            if JobState::decode_at(key.value(), value.value())?
                .claim
                .is_some()
            {
                return Err(StorageError::corrupted(
                    "a Job state carries a legacy claim",
                ));
            }
        }
    }
    Ok(())
}

/// Refuses, before anything is opened for writing, a database in which an
/// Allocation still holds capacity or a Job's outcome is Unknown. A restart
/// is not evidence that the work stopped, so neither is adopted, retried or
/// released here; `ocg inspect-held` reports them without changing the file.
fn refuse_unresolved_ownership(tx: &ReadTransaction) -> Result<(), StorageError> {
    if !ExecutionControl::RecoveryAndRedispatch.is_enabled() {
        let mut held = 0u64;
        match tx.open_table(POOL_ALLOCATIONS) {
            Ok(table) => {
                for entry in table.iter()? {
                    let (key, value) = entry?;
                    if transactions::decode_pool_allocation(key.value(), value.value())?
                        .holds_capacity()
                    {
                        held += 1;
                    }
                }
            }
            Err(TableError::TableDoesNotExist(_)) => {}
            Err(error) => return Err(error.into()),
        }
        let mut unknown = 0u64;
        match tx.open_table(JOB_STATES) {
            Ok(table) => {
                for entry in table.iter()? {
                    let (key, value) = entry?;
                    let state = JobState::decode_at(key.value(), value.value())?;
                    if state
                        .outcome
                        .as_ref()
                        .is_some_and(|outcome| outcome.kind == OutcomeKind::Unknown)
                        && !resolved_unknown_ownership(tx, &state)?
                    {
                        unknown += 1;
                    }
                }
            }
            Err(TableError::TableDoesNotExist(_)) => {}
            Err(error) => return Err(error.into()),
        }
        if held > 0 || unknown > 0 {
            return Err(held_ownership(held, unknown));
        }
    }
    Ok(())
}

/// The Quick Path is safe only when no Unknown outcome needs its ownership
/// proof checked. Even a Released Unknown therefore always takes the Full Path.
fn unknown_outcomes_exist(tx: &ReadTransaction) -> Result<bool, StorageError> {
    match tx.open_table(JOB_STATES) {
        Ok(table) => {
            for entry in table.iter()? {
                let (key, value) = entry?;
                if JobState::decode_at(key.value(), value.value())?
                    .outcome
                    .is_some_and(|outcome| outcome.kind == OutcomeKind::Unknown)
                {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Err(TableError::TableDoesNotExist(_)) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

/// A native Unknown is resolved only by its own Released Allocation and durable
/// stop evidence. The Job remains Unknown forever; this proof releases local
/// capacity, never execution uncertainty, and never makes the Job dispatchable.
fn resolved_unknown_ownership(
    tx: &ReadTransaction,
    state: &JobState,
) -> Result<bool, StorageError> {
    use crate::db::entities::{AllocationPhase, MutableJobPhase};

    let Some(outcome) = state
        .outcome
        .as_ref()
        .filter(|outcome| outcome.kind == OutcomeKind::Unknown && state.claim.is_none())
    else {
        return Ok(false);
    };
    if state.stop_confirmed_at.is_some() || state.stop_confirmed_by.is_some() {
        return Ok(false);
    }
    let Some(row) = tx.open_table(POOL_ALLOCATIONS)?.get(state.job_id.get())? else {
        return Ok(false);
    };
    let allocation = transactions::decode_pool_allocation(state.job_id.get(), row.value())?;
    drop(row);
    if allocation.phase != AllocationPhase::Released
        || allocation.started_at.is_none()
        || allocation.stop_proof.is_none()
        || allocation
            .stop_confirmed_at
            .is_none_or(|confirmed| confirmed < outcome.at)
        || allocation.job_revision == 0
    {
        return Ok(false);
    }
    Ok(
        reader::mutable_job_record_in(tx, state.job_id)?.is_some_and(|record| {
            record.job.phase == MutableJobPhase::Executing
                && record.job.revision == allocation.job_revision
        }),
    )
}

fn held_ownership(held: u64, unknown: u64) -> StorageError {
    StorageError {
        kind: StorageErrorKind::Other,
        os_error: None,
        detail: format!(
            "refusing to start: {held} Allocation(s) still hold capacity and {unknown} Job outcome(s) are Unknown; a restart does not prove the work stopped, and recovery is disabled. Inspect with `ocg inspect-held <data-directory>`"
        ),
    }
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

fn check_preflight_table<K: Key + 'static>(
    tx: &ReadTransaction,
    definition: TableDefinition<K, &'static [u8]>,
    tags: &[u8],
) -> Result<(), StorageError> {
    let table = match tx.open_table(definition) {
        Ok(table) => table,
        Err(TableError::TableDoesNotExist(_)) => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    for row in table.iter()? {
        let (_, value) = row?;
        check_preflight_header(value.value(), tags)?;
    }
    Ok(())
}

struct PreflightDirectory {
    path: PathBuf,
    identity: std::fs::Metadata,
    copy_identity: Option<std::fs::Metadata>,
}
impl Drop for PreflightDirectory {
    fn drop(&mut self) {
        let Ok(directory) = std::fs::symlink_metadata(&self.path) else {
            return;
        };
        if !directory.is_dir() || !same_file_identity(&self.identity, &directory) {
            return;
        }
        let copy = self.path.join("copy.redb");
        if let (Some(expected), Ok(actual)) =
            (&self.copy_identity, std::fs::symlink_metadata(&copy))
            && actual.is_file()
            && same_file_identity(expected, &actual)
        {
            let _ = std::fs::remove_file(copy);
        }
        let _ = std::fs::remove_dir(&self.path);
    }
}

fn preflight(
    file: &std::fs::File,
    path: &Path,
    directory: &Path,
) -> Result<Verification, StorageError> {
    match ::redb::ReadOnlyDatabase::open(path) {
        Ok(database) => validate_preflight(&database),
        Err(::redb::DatabaseError::RepairAborted) => {
            use std::io::{Seek, SeekFrom};
            let scratch = directory.join(format!(".ocg-preflight-{}", uuid::Uuid::now_v7()));
            let mut mkdir = std::fs::DirBuilder::new();
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                mkdir.mode(0o700);
            }
            mkdir.create(&scratch).map_err(io_storage_error)?;
            let identity = std::fs::symlink_metadata(&scratch).map_err(io_storage_error)?;
            let mut cleanup = PreflightDirectory {
                path: scratch,
                identity,
                copy_identity: None,
            };
            let copy_path = cleanup.path.join("copy.redb");
            let mut options = std::fs::OpenOptions::new();
            options.read(true).write(true).create_new(true);
            #[cfg(windows)]
            {
                use std::os::windows::fs::OpenOptionsExt;
                const GENERIC_READ: u32 = 0x8000_0000;
                const GENERIC_WRITE: u32 = 0x4000_0000;
                const DELETE: u32 = 0x0001_0000;
                const DELETE_ON_CLOSE: u32 = 0x0400_0000;
                options.access_mode(GENERIC_READ | GENERIC_WRITE | DELETE);
                options.custom_flags(DELETE_ON_CLOSE);
            }
            let mut copy = crate::system::data_directory::private_file(&mut options)
                .open(&copy_path)
                .map_err(io_storage_error)?;
            cleanup.copy_identity = Some(copy.metadata().map_err(io_storage_error)?);
            #[cfg(windows)]
            {
                cleanup.copy_identity = None;
            }
            #[cfg(unix)]
            {
                std::fs::remove_file(&copy_path).map_err(io_storage_error)?;
                cleanup.copy_identity = None;
            }
            let mut original = file.try_clone().map_err(io_storage_error)?;
            original
                .seek(SeekFrom::Start(0))
                .map_err(io_storage_error)?;
            let copied = std::io::copy(&mut original, &mut copy).map_err(io_storage_error)?;
            if copied != file.metadata().map_err(io_storage_error)?.len() || copied == 0 {
                return Err(StorageError::corrupted(
                    "database changed during preflight copy",
                ));
            }
            let database = Database::builder().create_file(copy)?;
            validate_preflight(&database).map(|_| Verification::Full)
        }
        Err(error) => Err(error.into()),
    }
}

pub(super) fn initialize(
    database: &Database,
    verification: Verification,
) -> Result<(u64, JobId, Vec<JobId>), StorageError> {
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
        if existing.iter().any(|name| name == HELD_SLOTS.name()) {
            return Err(StorageError::corrupted(
                "legacy slot table in a native database",
            ));
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
        // Derived tables are committed in the same transaction as the rows
        // they derive from, so a database that shut down cleanly and passed
        // the quick checks needs no rebuild. Every other state rebuilds them
        // from their source rows and verifies the result.
        let rebuild = match verification {
            Verification::Full => true,
            Verification::Quick => !clean_shutdown || indexes_missing,
        };
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
            if value.value().first() == Some(&JobSpec::MUTABLE_CONVERSATION_TAG) {
                JobSpec::decode_at(key.value(), value.value())?;
                let state = tx
                    .open_table(JOB_STATES)?
                    .get(key.value())?
                    .map(|row| JobState::decode_at(key.value(), row.value()))
                    .transpose()?
                    .ok_or_else(|| {
                        StorageError::corrupted("mutable conversation state is missing")
                    })?;
                if state.claim.is_some()
                    || state.outcome.is_some()
                    || state.stop_confirmed_at.is_some()
                    || state.stop_confirmed_by.is_some()
                {
                    return Err(StorageError::corrupted(
                        "mutable conversation recovery does not support this state",
                    ));
                }
                tx.open_table(JOB_CONTROLS)?
                    .get(key.value())?
                    .map(|row| JobControl::decode(row.value()))
                    .transpose()?
                    .ok_or_else(|| {
                        StorageError::corrupted("mutable conversation control is missing")
                    })?;
                return Ok(Some((key.value(), None)));
            }
            let job = transactions::decode_mutable_job(key.value(), value.value())?;
            let state_bytes = tx
                .open_table(JOB_STATES)?
                .get(key.value())?
                .map(|v| v.value().to_vec())
                .ok_or_else(|| StorageError::corrupted("mutable Job state is missing"))?;
            if !matches!(
                state_bytes.as_slice(),
                [TAG_JOB_STATE, codec::RECORD_VERSION, ..]
            ) {
                return Err(StorageError::corrupted("invalid mutable Job state header"));
            }
            let state = JobState::decode_at(key.value(), &state_bytes)?;
            if state.claim.is_some() {
                return Err(StorageError::corrupted(
                    "a Job state carries a legacy claim",
                ));
            }
            Ok(Some((key.value(), Some((job, state)))))
        })?;
        let Some((id, row)) = next else {
            break;
        };
        after = Some(id);
        let Some((job, state)) = row else {
            batch.insert(CONVERSATIONS, id, ())?;
            batch.insert(CHILDREN, (0, id), ())?;
            batch.insert(UNSETTLED, id, (0, 0))?;
            continue;
        };
        if !batch.mutable_context_valid(&job, job.context_selection)? {
            return batch.corrupted("summary provenance is invalid or unavailable".to_owned());
        }
        let parent = batch.require_job(job.parent_id)?;
        if !parent.kind.is_group() {
            return batch.corrupted("mutable Job has invalid conversation".to_owned());
        }
        if job.summary_source.is_some() {
            let allocation = batch.pool_allocation(job.id)?;
            if job.phase != MutableJobPhase::Executing
                || !state
                    .outcome
                    .as_ref()
                    .is_some_and(|outcome| outcome.kind == OutcomeKind::Completed)
                || !allocation.is_some_and(|allocation| {
                    allocation.phase == crate::db::entities::AllocationPhase::Released
                        && allocation.token.job == job.id
                        && allocation.job_revision == job.revision
                })
            {
                return batch.corrupted("invalid native summary producer state".to_owned());
            }
            // Summary producer Jobs are not conversation turns. The verifier
            // checks their source and output when an artifact is requested.
            continue;
        }
        if job.phase != MutableJobPhase::Unsubmitted {
            let JobKind::Group { agent, .. } = parent.kind else {
                return batch.corrupted("mutable Job has invalid conversation".to_owned());
            };
            let compatible = match &job.input.content {
                crate::db::entities::MutableJobContent::Generative { configuration, .. } => {
                    configuration.and_then(|config| config.agent_profile.map(Into::into)) == agent
                }
                crate::db::entities::MutableJobContent::Decision { profile, .. } => {
                    agent.is_none() || Some(*profile) == agent
                }
            };
            if !compatible {
                return batch
                    .corrupted("mutable Job Agent differs from its conversation".to_owned());
            }
            if batch.group_head(job.parent_id)? != job.previous {
                return batch.corrupted("mutable conversation predecessor mismatch".to_owned());
            }
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
                            if result.is_some() {
                                return Err(StorageError::corrupted(
                                    "multiple native final results",
                                ));
                            }
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
            if state.outcome.is_none()
                || state
                    .outcome
                    .as_ref()
                    .is_some_and(|outcome| outcome.kind == OutcomeKind::Unknown)
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
                    let count = children.checked_add(1).ok_or_else(|| {
                        StorageError::corrupted("conversation child count overflow")
                    })?;
                    unsettled.insert(job.parent_id.get(), (dependencies, count))?;
                    Ok(())
                })?;
            }
        }
        if state
            .outcome
            .as_ref()
            .is_some_and(|outcome| outcome.kind == OutcomeKind::Unknown)
        {
            let resolved = batch.pool_allocation(job.id)?.is_some_and(|allocation| {
                allocation.phase == crate::db::entities::AllocationPhase::Released
                    && allocation.stop_confirmed_at.is_some()
                    && allocation.stop_proof.is_some()
            });
            if !resolved {
                return batch.corrupted(
                    "recovery is disabled and this database holds unresolved Unknown execution outcomes; run `ocg inspect-held`"
                        .to_owned(),
                );
            }
            // Unknown is terminal for dispatch but has no Result and remains
            // unsettled in the conversation, exactly as the runtime records it.
            continue;
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
                // A submitted Job's requirement comes from the configuration it
                // froze, never from settings edited since.
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

// ---- offline reconciliation of a crash-surviving Reserved Allocation -------------

/// Why an offline reconciliation refused. Nothing was changed: the only file
/// it ever wrote was a private copy.
#[derive(Debug)]
pub(crate) struct ReservedRefusal {
    pub(crate) code: &'static str,
    pub(crate) detail: String,
}

impl ReservedRefusal {
    fn new(code: &'static str, detail: impl Into<String>) -> Self {
        Self {
            code,
            detail: detail.into(),
        }
    }
}

impl From<StorageError> for ReservedRefusal {
    fn from(error: StorageError) -> Self {
        let code = match error.kind {
            StorageErrorKind::Corrupted => "inconsistent_or_corrupt",
            StorageErrorKind::Io(_)
            | StorageErrorKind::PreviousIo
            | StorageErrorKind::AlreadyOpen
            | StorageErrorKind::Closed
            | StorageErrorKind::ValueTooLarge
            | StorageErrorKind::Other => "unverifiable",
        };
        Self::new(code, error.detail)
    }
}

impl From<::redb::DatabaseError> for ReservedRefusal {
    fn from(error: ::redb::DatabaseError) -> Self {
        StorageError::from(error).into()
    }
}

/// Reads the facts that decide whether `job` is a Reserved Allocation that
/// never started, from a database nothing is writing.
///
/// The epoch rule: a Reservation is minted in the epoch of the process that
/// opened the database, and an open that finds a held Allocation refuses
/// before it increments the epoch. So the Allocation's epoch equals the
/// stored `open_epoch` exactly when no process has opened the database since
/// the one that made it. That is necessary (a different epoch means history
/// this build cannot see) but not sufficient: the owner may still be running.
/// Its absence is established by the exclusive host lock the caller holds.
fn reserved_preflight_in(
    database: &impl ReadableDatabase,
    job: JobId,
) -> Result<(crate::db::entities::AllocationToken, serde_json::Value), ReservedRefusal> {
    use crate::db::entities::{AllocationPhase, MutableJobPhase};
    use serde_json::json;

    let tx = database.begin_read().map_err(StorageError::from)?;
    let meta = tx.open_table(META).map_err(StorageError::from)?;
    check_identity(&meta)?;
    let epoch = meta
        .get("open_epoch")
        .map_err(StorageError::from)?
        .and_then(|value| <[u8; 8]>::try_from(value.value()).ok())
        .map(u64::from_le_bytes)
        .ok_or_else(|| ReservedRefusal::new("inconsistent_or_corrupt", "missing open epoch"))?;
    let clean_shutdown = meta
        .get("clean_shutdown")
        .map_err(StorageError::from)?
        .map(|value| value.value() == [1]);
    drop(meta);
    check_records(&tx)?;

    let allocations = tx
        .open_table(POOL_ALLOCATIONS)
        .map_err(StorageError::from)?;
    let row = allocations
        .get(job.get())
        .map_err(StorageError::from)?
        .ok_or_else(|| ReservedRefusal::new("allocation_not_found", "the Job has no Allocation"))?;
    let allocation = transactions::decode_pool_allocation(job.get(), row.value())?;
    drop(row);
    match allocation.phase {
        AllocationPhase::Reserved => {}
        AllocationPhase::Released => {
            return Err(ReservedRefusal::new(
                "already_released",
                "the Allocation is already released; there is nothing to reconcile",
            ));
        }
        AllocationPhase::Running => {
            return Err(ReservedRefusal::new(
                "running",
                "the Allocation is Running: the Job started, and a restart is not evidence its work stopped",
            ));
        }
        AllocationPhase::StopRequested => {
            return Err(ReservedRefusal::new(
                "stop_requested",
                "a stop was requested of a started Job; its stop is not confirmed",
            ));
        }
        AllocationPhase::Uncertain => {
            return Err(ReservedRefusal::new(
                "uncertain",
                "the Allocation is Uncertain; its outcome is unknown, not unstarted",
            ));
        }
    }
    if allocation.started_at.is_some() {
        return Err(ReservedRefusal::new(
            "started",
            "a start was committed for this Allocation",
        ));
    }
    if allocation.stop_confirmed_at.is_some() || allocation.stop_proof.is_some() {
        return Err(ReservedRefusal::new(
            "stop_evidence",
            "the Allocation carries stop evidence, so it is not an untouched reservation",
        ));
    }
    let token = allocation.token;
    if token.epoch != epoch {
        return Err(ReservedRefusal::new(
            "epoch_mismatch",
            format!(
                "the Allocation was made in epoch {} but the database was last opened in epoch {epoch}; the intervening history is not visible",
                token.epoch
            ),
        ));
    }
    let current_host = current_host_id();
    if current_host.is_none() || allocation.host_id != current_host {
        return Err(ReservedRefusal::new(
            "other_host",
            "the Allocation was not claimed by this host, or this host cannot be identified; a lock here says nothing about another machine",
        ));
    }
    let same_boot = allocation.boot_id.is_some() && allocation.boot_id == current_boot_id();

    // Capacity ownership elsewhere is not this command's to resolve, and the
    // result could not pass the startup verification with it.
    let (mut held, mut unknown) = (0u64, 0u64);
    for entry in allocations.iter().map_err(StorageError::from)? {
        let (key, value) = entry.map_err(StorageError::from)?;
        if transactions::decode_pool_allocation(key.value(), value.value())?.holds_capacity() {
            held += 1;
        }
    }
    drop(allocations);
    let states = tx.open_table(JOB_STATES).map_err(StorageError::from)?;
    for entry in states.iter().map_err(StorageError::from)? {
        let (key, value) = entry.map_err(StorageError::from)?;
        if JobState::decode_at(key.value(), value.value())?
            .outcome
            .is_some_and(|outcome| outcome.kind == OutcomeKind::Unknown)
        {
            unknown += 1;
        }
    }
    drop(states);
    if held != 1 || unknown != 0 {
        return Err(ReservedRefusal::new(
            "other_unresolved_ownership",
            format!(
                "{held} Allocation(s) hold capacity and {unknown} Job outcome(s) are Unknown; only a database whose single held Allocation is this one is reconciled"
            ),
        ));
    }

    let record = reader::mutable_job_record_in(&tx, job)?
        .ok_or_else(|| ReservedRefusal::new("job_missing", "the reserved Job does not exist"))?;
    if let Some(outcome) = &record.outcome {
        return Err(ReservedRefusal::new(
            if outcome.kind == OutcomeKind::Unknown {
                "outcome_unknown"
            } else {
                "job_has_outcome"
            },
            "the Job already has an outcome",
        ));
    }
    if record.job.phase != MutableJobPhase::Queued
        || record.job.revision != allocation.job_revision
        || record.job.frozen.is_none()
    {
        return Err(ReservedRefusal::new(
            "job_not_prestart",
            "the Job is not the frozen, queued revision that was reserved",
        ));
    }

    let configs = tx.open_table(CONFIGS).map_err(StorageError::from)?;
    let pool = match configs.get(token.pool.get()).map_err(StorageError::from)? {
        Some(bytes) => Some(transactions::decode_allocation_pool(
            token.pool.get(),
            bytes.value(),
        )?),
        None => None,
    };
    let executor = match configs
        .get(token.executor.get())
        .map_err(StorageError::from)?
    {
        Some(bytes) => Some(transactions::decode_allocation_executor(
            token.executor.get(),
            bytes.value(),
        )?),
        None => None,
    };
    if pool.as_ref().is_none_or(|pool| pool.id != token.pool)
        || executor
            .as_ref()
            .is_none_or(|executor| executor.id != token.executor || executor.pool != token.pool)
    {
        return Err(ReservedRefusal::new(
            "binding_inconsistent",
            "the reserved Pool or Executor is missing or no longer bound to the Allocation",
        ));
    }

    let id = job.get();
    let mut evidence = Vec::new();
    if tx
        .open_table(JOB_OUTPUTS)
        .map_err(StorageError::from)?
        .range((id, 0)..=(id, u32::MAX))
        .map_err(StorageError::from)?
        .next()
        .is_some()
    {
        evidence.push("output");
    }
    if tx
        .open_table(REQUEST_EVENTS)
        .map_err(StorageError::from)?
        .range((id, 0, 0)..=(id, u32::MAX, u32::MAX))
        .map_err(StorageError::from)?
        .next()
        .is_some()
    {
        evidence.push("request");
    }
    if tx
        .open_table(TOOL_CALLS)
        .map_err(StorageError::from)?
        .range((id, 0, 0)..=(id, u32::MAX, u32::MAX))
        .map_err(StorageError::from)?
        .next()
        .is_some()
    {
        evidence.push("tool");
    }
    if tx
        .open_table(APPROVALS)
        .map_err(StorageError::from)?
        .range((id, 0, 0)..=(id, u32::MAX, u32::MAX))
        .map_err(StorageError::from)?
        .next()
        .is_some()
    {
        evidence.push("approval");
    }
    if tx
        .open_table(JOB_OBSERVATIONS)
        .map_err(StorageError::from)?
        .range((id, 0, 0)..=(id, u8::MAX, u128::MAX))
        .map_err(StorageError::from)?
        .next()
        .is_some()
    {
        evidence.push("observation");
    }
    if tx
        .open_table(LIVE_CHILDREN)
        .map_err(StorageError::from)?
        .range((id, 0)..=(id, u128::MAX))
        .map_err(StorageError::from)?
        .next()
        .is_some()
    {
        evidence.push("child");
    }
    if !evidence.is_empty() {
        return Err(ReservedRefusal::new(
            "evidence_present",
            format!(
                "the Job has {} evidence; an unstarted reservation has none",
                evidence.join(", ")
            ),
        ));
    }

    let facts = json!({
        "job": job.to_string(),
        "pool": token.pool.to_string(),
        "executor": token.executor.to_string(),
        "generation": token.generation.get(),
        "allocation_epoch": token.epoch,
        "open_epoch": epoch,
        "clean_shutdown": clean_shutdown,
        "claimed_at_unix_micros": allocation.claimed_at.as_unix_micros(),
        "job_revision": allocation.job_revision,
        "claimed_by_this_host": true,
        "claimed_in_this_boot": same_boot,
        "held_allocations": held,
        "unknown_outcomes": unknown,
    });
    Ok((token, facts))
}

/// Runs `body` on the database the file holds. The file is a private copy:
/// redb may repair it on open, and a damaged page is reported as a refusal
/// instead of aborting the process.
fn with_reconciliation_copy<T>(
    file: std::fs::File,
    body: impl FnOnce(&mut Database) -> Result<T, ReservedRefusal>,
) -> Result<T, ReservedRefusal> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut database = Database::builder().create_file(file)?;
        body(&mut database)
    }))
    .unwrap_or_else(|_| {
        Err(ReservedRefusal::new(
            "inconsistent_or_corrupt",
            "redb aborted while reading a damaged page",
        ))
    })
}

/// Copies an already-open file into an unlinked private file for finalization.
/// Neither the published database nor its rollback artifact is opened by a new
/// pathname or exposed to redb's repair path.
fn verification_copy(file: &std::fs::File) -> Result<std::fs::File, ReservedRefusal> {
    use std::io::{Seek, SeekFrom};

    let scratch = std::env::temp_dir().join(format!(".ocg-finalize-{}", uuid::Uuid::now_v7()));
    let mut create = std::fs::OpenOptions::new();
    create.read(true).write(true).create_new(true);
    let copy = crate::system::data_directory::private_file(&mut create)
        .open(&scratch)
        .map_err(io_storage_error)?;
    std::fs::remove_file(&scratch).map_err(io_storage_error)?;
    let mut source = file.try_clone().map_err(io_storage_error)?;
    source.seek(SeekFrom::Start(0)).map_err(io_storage_error)?;
    let mut copy = copy;
    let expected = file.metadata().map_err(io_storage_error)?.len();
    let copied = std::io::copy(&mut source, &mut copy).map_err(io_storage_error)?;
    if copied != expected || copied == 0 {
        return Err(ReservedRefusal::new(
            "inconsistent_or_corrupt",
            "a finalization input changed while it was copied",
        ));
    }
    Ok(copy)
}

/// Decides, on a private copy, whether `job` is an unstarted Reservation.
/// Writes nothing the caller keeps.
pub(crate) fn reconcile_reserved_preflight(
    file: std::fs::File,
    job: JobId,
) -> Result<serde_json::Value, ReservedRefusal> {
    with_reconciliation_copy(file, |database| {
        reserved_preflight_in(&*database, job).map(|(_, facts)| facts)
    })
}

/// Ends the unstarted Reservation on a private copy with the one transaction
/// the runtime uses for it, then verifies the copy completely: every page
/// checksum, every record and every derived index. The returned report is
/// what was verified.
pub(crate) fn reconcile_reserved_apply(
    file: std::fs::File,
    job: JobId,
) -> Result<serde_json::Value, ReservedRefusal> {
    use crate::db::entities::{AllocationPhase, UnstartedCause};
    with_reconciliation_copy(file, |database| {
        let (token, mut facts) = reserved_preflight_in(&*database, job)?;
        let outcome =
            writer::run_batch_with(database, token.epoch, writer::Commit::Durable, |batch| {
                batch.fail_unstarted_reservation(token, UnstartedCause::OfflineReconciled)
            });
        match outcome.result {
            Ok(_) => {}
            Err(writer::BatchFailure::Rejected(violation)) => {
                return Err(ReservedRefusal::new("rejected", violation.to_string()));
            }
            Err(
                writer::BatchFailure::NotCommitted(error)
                | writer::BatchFailure::OutcomeUnknown(error),
            ) => return Err(error.into()),
        }
        if !database.check_integrity().map_err(StorageError::from)? {
            return Err(ReservedRefusal::new(
                "page_integrity",
                "redb found damage in the reconciled copy and had to repair it",
            ));
        }
        // Every record and derived index, and no held Allocation or Unknown outcome left.
        verify_full(&*database)?;
        let tx = database.begin_read().map_err(StorageError::from)?;
        let allocations = tx
            .open_table(POOL_ALLOCATIONS)
            .map_err(StorageError::from)?;
        let row = allocations
            .get(job.get())
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                ReservedRefusal::new("inconsistent_or_corrupt", "Allocation vanished")
            })?;
        let released = transactions::decode_pool_allocation(job.get(), row.value())?;
        let record = reader::mutable_job_record_in(&tx, job)?;
        let failed = record
            .as_ref()
            .and_then(|record| record.outcome.as_ref())
            .is_some_and(|outcome| outcome.kind == OutcomeKind::Failed);
        if released.phase != AllocationPhase::Released || released.started_at.is_some() || !failed {
            return Err(ReservedRefusal::new(
                "inconsistent_or_corrupt",
                "the reconciled copy does not show a Released Allocation and a Failed Job",
            ));
        }
        facts["result"] = serde_json::json!({
            "allocation_phase": "released",
            "job_outcome": "failed",
            "failure_code": UnstartedCause::OfflineReconciled.code(),
            "page_checksums": "every page checksum verified",
            "logical_checks": "every record header, derived index and ownership row verified",
        });
        Ok(facts)
    })
}

/// Reconstructs the reserved-recovery verification from the exact source and
/// published files after a crash. This never invokes the release transaction.
pub(crate) fn verify_reserved_publication_delta(
    source: &std::fs::File,
    published: &std::fs::File,
    job: JobId,
) -> Result<serde_json::Value, ReservedRefusal> {
    use crate::db::entities::{AllocationPhase, UnstartedCause};
    use serde_json::json;

    let source = verification_copy(source)?;
    let published = verification_copy(published)?;
    with_reconciliation_copy(source, |source_database| {
        if !source_database
            .check_integrity()
            .map_err(StorageError::from)?
        {
            return Err(ReservedRefusal::new(
                "page_integrity",
                "redb found damage in the rollback copy and had to repair it",
            ));
        }
        let (_, mut facts) = reserved_preflight_in(&*source_database, job)?;
        with_reconciliation_copy(published, |database| {
            if !database.check_integrity().map_err(StorageError::from)? {
                return Err(ReservedRefusal::new(
                    "page_integrity",
                    "redb found damage in the published copy and had to repair it",
                ));
            }
            verify_full(&*database)?;
            verify_unrelated_recovery_rows(
                &*source_database,
                &*database,
                &std::collections::BTreeSet::from([job.get()]),
            )?;
            let source_tx = source_database.begin_read().map_err(StorageError::from)?;
            let source_record = reader::mutable_job_record_in(&source_tx, job)?
                .ok_or_else(|| audit_delta("the rollback source Job vanished"))?;
            let parent = source_record.job.parent_id;
            let source_allocation_row = source_tx
                .open_table(POOL_ALLOCATIONS)
                .map_err(StorageError::from)?
                .get(job.get())
                .map_err(StorageError::from)?
                .ok_or_else(|| audit_delta("the rollback source Allocation vanished"))?;
            let source_allocation =
                transactions::decode_pool_allocation(job.get(), source_allocation_row.value())?;
            drop(source_allocation_row);
            let source_state_row = source_tx
                .open_table(JOB_STATES)
                .map_err(StorageError::from)?
                .get(job.get())
                .map_err(StorageError::from)?
                .ok_or_else(|| audit_delta("the rollback source Job state vanished"))?;
            let source_state = JobState::decode_at(job.get(), source_state_row.value())?;
            drop(source_state_row);

            let tx = database.begin_read().map_err(StorageError::from)?;
            let row = tx
                .open_table(POOL_ALLOCATIONS)
                .map_err(StorageError::from)?
                .get(job.get())
                .map_err(StorageError::from)?
                .ok_or_else(|| {
                    ReservedRefusal::new("inconsistent_or_corrupt", "Allocation vanished")
                })?;
            let released = transactions::decode_pool_allocation(job.get(), row.value())?;
            drop(row);
            let state_row = tx
                .open_table(JOB_STATES)
                .map_err(StorageError::from)?
                .get(job.get())
                .map_err(StorageError::from)?
                .ok_or_else(|| audit_delta("the published Job state vanished"))?;
            let published_state = JobState::decode_at(job.get(), state_row.value())?;
            drop(state_row);
            let outcome = published_state
                .outcome
                .as_ref()
                .ok_or_else(|| audit_delta("the published Job has no OfflineReconciled outcome"))?;
            let mut expected_allocation = source_allocation;
            expected_allocation.phase = AllocationPhase::Released;
            let mut expected_state = source_state;
            expected_state.outcome = published_state.outcome.clone();
            if released != expected_allocation
                || published_state != expected_state
                || outcome.kind != OutcomeKind::Failed
                || outcome.cause.is_some()
            {
                return Err(audit_delta(
                    "the published target rows are not the exact authorized Released Allocation and Failed Job",
                ));
            }
            let output = verify_reserved_output_delta(&source_tx, &tx, job)?;
            if output.created_at != outcome.at {
                return Err(audit_delta(
                    "the Result and Job outcome do not share the transaction timestamp",
                ));
            }
            verify_active_allocation_delta(
                &source_tx,
                &tx,
                &std::collections::BTreeSet::from([(
                    released.token.pool.get(),
                    released.token.executor.get(),
                    job.get(),
                )]),
            )?;
            verify_reserved_settlement_delta(&source_tx, &tx, job, parent)?;
            facts["result"] = json!({
                "allocation_phase": "released",
                "job_outcome": "failed",
                "failure_code": UnstartedCause::OfflineReconciled.code(),
                "result_output_sequence": output.seq,
                "result_output_id": output.id.to_string(),
                "settlement_indexes": "exact Job removal, conversation child decrement and group-result advance",
                "page_checksums": "every page checksum verified",
                "logical_checks": "every record header, derived index and ownership row verified",
                "unrelated_allocations_and_jobs": "byte-identical to the rollback source",
                "preserved_evidence_and_derived_rows": "byte-identical to the rollback source except the authorized Result, sequence, settlement and active-index deltas",
                "reconstructed_without_reapply": true,
            });
            Ok(facts)
        })
    })
}

// ---- offline reconciliation of stopped loopback executions ----------------------

/// One source-bound action. The expected bytes make apply fail closed if the
/// authoritative rows differ from the dry-run, even when their decoded facts
/// would still look eligible.
#[derive(Clone, Debug)]
pub(crate) struct RecoveryAction {
    pub(super) token: crate::db::entities::AllocationToken,
    pub(super) phase: crate::db::entities::AllocationPhase,
    pub(super) proof: StopProof,
    pub(super) reason: crate::db::entities::UncertaintyReason,
    pub(super) preserve_unknown: bool,
    pub(super) expected_allocation: Vec<u8>,
    pub(super) expected_state: Vec<u8>,
}

pub(crate) struct HeldRecoveryPlan {
    pub(crate) canonical: serde_json::Value,
    pub(crate) digest: String,
    actions: Vec<RecoveryAction>,
    epoch: u64,
}

fn recovery_phase_name(phase: crate::db::entities::AllocationPhase) -> &'static str {
    use crate::db::entities::AllocationPhase;
    match phase {
        AllocationPhase::Reserved => "reserved",
        AllocationPhase::Running => "running",
        AllocationPhase::StopRequested => "stop_requested",
        AllocationPhase::Uncertain => "uncertain",
        AllocationPhase::Released => "released",
    }
}

fn recovery_proof_name(proof: StopProof) -> &'static str {
    match proof {
        StopProof::Proof => "proof",
        StopProof::Reboot => "reboot",
        StopProof::Settled => "settled",
        StopProof::User => "user",
        StopProof::Operator => "operator",
    }
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(DIGITS[(byte >> 4) as usize] as char);
        encoded.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn sha256_json(value: &serde_json::Value) -> Result<String, ReservedRefusal> {
    use aws_lc_rs::digest::{Context, SHA256};
    let bytes = serde_json::to_vec(value).map_err(|error| {
        ReservedRefusal::new(
            "unverifiable",
            format!("cannot encode the recovery plan: {error}"),
        )
    })?;
    let mut context = Context::new(&SHA256);
    context.update(&bytes);
    Ok(hex(context.finish().as_ref()))
}

/// Plans every held Allocation in stable Job order. Ineligible rows are
/// explicit refusals and are never silently converted or removed from the plan.
fn held_recovery_plan_in(
    database: &impl ReadableDatabase,
    source_sha256: &str,
) -> Result<HeldRecoveryPlan, ReservedRefusal> {
    use serde_json::json;

    let tx = database.begin_read().map_err(StorageError::from)?;
    let meta = tx.open_table(META).map_err(StorageError::from)?;
    check_identity(&meta)?;
    let epoch = meta
        .get("open_epoch")
        .map_err(StorageError::from)?
        .and_then(|value| <[u8; 8]>::try_from(value.value()).ok())
        .map(u64::from_le_bytes)
        .ok_or_else(|| ReservedRefusal::new("inconsistent_or_corrupt", "missing open epoch"))?;
    let clean = meta
        .get("clean_shutdown")
        .map_err(StorageError::from)?
        .is_some_and(|value| value.value() == [1]);
    drop(meta);
    check_records(&tx)?;

    let mut held = Vec::new();
    for entry in tx
        .open_table(POOL_ALLOCATIONS)
        .map_err(StorageError::from)?
        .iter()
        .map_err(StorageError::from)?
    {
        let (key, value) = entry.map_err(StorageError::from)?;
        let allocation = transactions::decode_pool_allocation(key.value(), value.value())?;
        if allocation.holds_capacity() {
            held.push((allocation, value.value().to_vec()));
        }
    }
    if clean && !held.is_empty() {
        return Err(ReservedRefusal::new(
            "unguarded_source",
            "a clean-shutdown database with held Allocations is not a supported recovery source",
        ));
    }

    let current_host = current_host_id();
    let current_boot = current_boot_id();
    let mut actions = Vec::new();
    let mut action_facts = Vec::new();
    let mut refusals = Vec::new();
    for (allocation, allocation_bytes) in held {
        let job = allocation.token.job;
        let state_bytes = match tx
            .open_table(JOB_STATES)
            .map_err(StorageError::from)?
            .get(job.get())
            .map_err(StorageError::from)?
        {
            Some(row) => row.value().to_vec(),
            None => {
                return Err(ReservedRefusal::new(
                    "inconsistent_or_corrupt",
                    format!("{job} has no Job state"),
                ));
            }
        };
        let planned_revision = allocation.job_revision;
        let planned_phase = allocation.phase;
        match recovery_action_in(
            &tx,
            epoch,
            current_host.as_deref(),
            current_boot.as_deref(),
            allocation,
            allocation_bytes,
            state_bytes,
        )? {
            Ok(action) => {
                action_facts.push(json!({
                    "job": job.to_string(),
                    "pool": action.token.pool.to_string(),
                    "executor": action.token.executor.to_string(),
                    "generation": action.token.generation.get(),
                    "epoch": action.token.epoch,
                    "job_revision": planned_revision,
                    "from_phase": recovery_phase_name(action.phase),
                    "to_phase": "released",
                    "stop_proof": recovery_proof_name(action.proof),
                    "unknown": if action.preserve_unknown { "preserve_exact" } else { "record_unknown" },
                    "uncertainty_reason": action.reason.as_str(),
                    "allocation_sha256": sha256_bytes(&action.expected_allocation),
                    "job_state_sha256": sha256_bytes(&action.expected_state),
                }));
                actions.push(action);
            }
            Err((code, detail)) => refusals.push(json!({
                "job": job.to_string(),
                "phase": recovery_phase_name(planned_phase),
                "code": code,
                "detail": detail,
            })),
        }
    }
    let canonical = json!({
        "kind": "ocg-held-recovery-plan-v1",
        "database_format": DATABASE_FORMAT_VERSION,
        "record_version": codec::RECORD_VERSION,
        "source_sha256": source_sha256,
        "open_epoch": epoch,
        "clean_shutdown": clean,
        "current_host_id": current_host,
        "current_boot_id": current_boot,
        "actions": action_facts,
        "refused": refusals,
    });
    let digest = sha256_json(&canonical)?;
    Ok(HeldRecoveryPlan {
        canonical,
        digest,
        actions,
        epoch,
    })
}

fn sha256_bytes(bytes: &[u8]) -> String {
    use aws_lc_rs::digest::{Context, SHA256};
    let mut context = Context::new(&SHA256);
    context.update(bytes);
    hex(context.finish().as_ref())
}

fn recovery_action_in(
    tx: &ReadTransaction,
    epoch: u64,
    current_host: Option<&str>,
    current_boot: Option<&str>,
    allocation: crate::db::entities::PoolAllocation,
    allocation_bytes: Vec<u8>,
    state_bytes: Vec<u8>,
) -> Result<Result<RecoveryAction, (&'static str, String)>, ReservedRefusal> {
    use crate::db::entities::{
        AllocationPhase, ExecutionOperation, MutableFrozenConfiguration, MutableJobPhase,
        OutcomeCause, UncertaintyReason,
    };

    macro_rules! ineligible {
        ($code:expr, $detail:expr) => {
            return Ok(Err(($code, $detail.to_owned())))
        };
    }

    let job_id = allocation.token.job;
    if allocation.phase == AllocationPhase::Reserved {
        ineligible!(
            "reserved",
            "an unstarted Reservation belongs to `ocg reconcile-reserved`; it is left untouched"
        );
    }
    if allocation.token.epoch != epoch {
        ineligible!(
            "epoch_mismatch",
            "the database has intervening open history that this recovery cannot see"
        );
    }
    if current_host.is_none() || allocation.host_id.as_deref() != current_host {
        ineligible!(
            "other_host",
            "the Allocation is not owned by this identified host"
        );
    }
    let same_boot = current_boot.is_some() && allocation.boot_id.as_deref() == current_boot;
    let boot_changed = current_boot.is_some()
        && allocation.boot_id.is_some()
        && allocation.boot_id.as_deref() != current_boot;
    if allocation.started_at.is_none()
        || allocation.stop_confirmed_at.is_some()
        || allocation.stop_proof.is_some()
    {
        ineligible!(
            "allocation_inconsistent",
            "the held Allocation lacks a started, unconfirmed shape"
        );
    }
    let Some(record) = reader::mutable_job_record_in(tx, job_id)? else {
        ineligible!("job_not_native", "a native mutable Job is required");
    };
    if record.job.phase != MutableJobPhase::Executing
        || record.job.revision != allocation.job_revision
        || record.job.summary_source.is_some()
    {
        ineligible!(
            "job_revision",
            "the Job is not the ordinary executing revision named by the Allocation"
        );
    }
    let Some(MutableFrozenConfiguration::Generative(frozen)) = record.job.frozen.as_ref() else {
        ineligible!(
            "not_loopback_generative",
            "only a frozen Generative Job is eligible"
        );
    };
    if frozen.single_request_refusal().is_some()
        || !frozen
            .llm
            .provider
            .endpoint
            .as_deref()
            .is_some_and(crate::system::http::direct_diagnostic_loopback)
        || allocation.operation != ExecutionOperation::LlmRequest
        || allocation.protocol != Some(frozen.llm.provider.transport)
    {
        ineligible!(
            "not_loopback_generative",
            "the Job is not the strict single-request, tool-free literal-loopback shape"
        );
    }
    let pool = tx
        .open_table(CONFIGS)
        .map_err(StorageError::from)?
        .get(allocation.token.pool.get())
        .map_err(StorageError::from)?
        .map(|row| transactions::decode_allocation_pool(allocation.token.pool.get(), row.value()))
        .transpose()?;
    let executor = tx
        .open_table(CONFIGS)
        .map_err(StorageError::from)?
        .get(allocation.token.executor.get())
        .map_err(StorageError::from)?
        .map(|row| {
            transactions::decode_allocation_executor(allocation.token.executor.get(), row.value())
        })
        .transpose()?;
    if pool
        .as_ref()
        .is_none_or(|pool| pool.id != allocation.token.pool)
        || executor.as_ref().is_none_or(|executor| {
            executor.id != allocation.token.executor
                || executor.pool != allocation.token.pool
                || !executor
                    .capabilities
                    .contains(&(allocation.operation, allocation.protocol))
        })
    {
        ineligible!(
            "binding_changed",
            "the Pool or Executor binding no longer proves this Allocation"
        );
    }
    if tx
        .open_table(SCHEDULED_EXTERNAL_BINDING)
        .map_err(StorageError::from)?
        .get(job_id.get())
        .map_err(StorageError::from)?
        .is_some()
    {
        ineligible!(
            "external_binding",
            "scheduled external work is never eligible for loopback recovery"
        );
    }

    // Conservative evidence audit: preserve every row, but refuse shapes that
    // can represent another request, tools, children, or a published Result.
    for entry in tx
        .open_table(REQUEST_EVENTS)
        .map_err(StorageError::from)?
        .range((job_id.get(), 0, 0)..=(job_id.get(), u32::MAX, u32::MAX))
        .map_err(StorageError::from)?
    {
        let (key, _) = entry.map_err(StorageError::from)?;
        if key.value().1 != 1 {
            ineligible!(
                "multiple_requests",
                "more than the first request has durable evidence"
            );
        }
    }
    for entry in tx
        .open_table(JOB_OUTPUTS)
        .map_err(StorageError::from)?
        .range((job_id.get(), 0)..=(job_id.get(), u32::MAX))
        .map_err(StorageError::from)?
    {
        let (_, value) = entry.map_err(StorageError::from)?;
        if rows::output_kind(value.value())? == OutputKind::Result {
            ineligible!(
                "result_present",
                "a final Result already exists and will not be replaced"
            );
        }
    }
    if tx
        .open_table(TOOL_CALLS)
        .map_err(StorageError::from)?
        .range((job_id.get(), 0, 0)..=(job_id.get(), u32::MAX, u32::MAX))
        .map_err(StorageError::from)?
        .next()
        .is_some()
        || tx
            .open_table(APPROVALS)
            .map_err(StorageError::from)?
            .range((job_id.get(), 0, 0)..=(job_id.get(), u32::MAX, u32::MAX))
            .map_err(StorageError::from)?
            .next()
            .is_some()
        || tx
            .open_table(LIVE_CHILDREN)
            .map_err(StorageError::from)?
            .range((job_id.get(), 0)..=(job_id.get(), u128::MAX))
            .map_err(StorageError::from)?
            .next()
            .is_some()
    {
        ineligible!(
            "execution_evidence",
            "tool, approval, or child evidence is outside this recovery's authority"
        );
    }
    for entry in tx
        .open_table(MUTABLE_READY)
        .map_err(StorageError::from)?
        .iter()
        .map_err(StorageError::from)?
    {
        if entry.map_err(StorageError::from)?.0.value().2 == job_id.get() {
            ineligible!("already_queued", "the executing Job is unexpectedly ready");
        }
    }
    if tx
        .open_table(PENDING)
        .map_err(StorageError::from)?
        .get(job_id.get())
        .map_err(StorageError::from)?
        .is_some()
        || tx
            .open_table(UNSETTLED)
            .map_err(StorageError::from)?
            .get(job_id.get())
            .map_err(StorageError::from)?
            .map(|row| row.value())
            != Some((0, 0))
    {
        ineligible!(
            "derived_state",
            "the executing Job's pending or unsettled rows are inconsistent"
        );
    }

    let (proof, reason, preserve_unknown) = match allocation.phase {
        AllocationPhase::Running => {
            if record.outcome.is_some() {
                ineligible!(
                    "outcome_present",
                    "a Running Allocation cannot have a final outcome"
                );
            }
            if !boot_changed {
                ineligible!(
                    "same_boot_running",
                    "Running recovery requires a genuine, identified Boot-ID change"
                );
            }
            (StopProof::Reboot, UncertaintyReason::Unclassified, false)
        }
        AllocationPhase::StopRequested => {
            if record.outcome.is_some() {
                ineligible!(
                    "outcome_present",
                    "a StopRequested Allocation cannot have a final outcome"
                );
            }
            if !boot_changed {
                ineligible!(
                    "same_boot_stop_requested",
                    "StopRequested recovery requires a genuine, identified Boot-ID change"
                );
            }
            (
                StopProof::Reboot,
                UncertaintyReason::StopRequestedWithoutConfirmedTermination,
                false,
            )
        }
        AllocationPhase::Uncertain => {
            let Some(outcome) = record.outcome.as_ref() else {
                ineligible!(
                    "outcome_missing",
                    "an Uncertain Allocation must preserve an Unknown outcome"
                );
            };
            if outcome.kind != OutcomeKind::Unknown {
                ineligible!(
                    "outcome_not_unknown",
                    "an Uncertain Allocation cannot have a known outcome"
                );
            }
            if boot_changed {
                (StopProof::Reboot, UncertaintyReason::Unclassified, true)
            } else if same_boot {
                let returned = outcome.cause.as_ref().is_some_and(|cause| {
                    matches!(
                        cause,
                        OutcomeCause::Uncertain(
                            UncertaintyReason::RequestPossiblySent
                                | UncertaintyReason::ConnectionInterrupted
                                | UncertaintyReason::ResponseIncompleteOrInvalid
                                | UncertaintyReason::StopRequestedWithoutConfirmedTermination
                        )
                    )
                });
                if !returned {
                    ineligible!(
                        "local_return_unproven",
                        "same-Boot Uncertain recovery requires a durable typed local-return reason"
                    );
                }
                (StopProof::Proof, UncertaintyReason::Unclassified, true)
            } else {
                ineligible!(
                    "boot_unproven",
                    "neither a genuine Boot-ID change nor same-Boot local return is proved"
                );
            }
        }
        AllocationPhase::Reserved | AllocationPhase::Released => {
            return Err(ReservedRefusal::new(
                "inconsistent_or_corrupt",
                "unreachable held Allocation phase",
            ));
        }
    };
    Ok(Ok(RecoveryAction {
        token: allocation.token,
        phase: allocation.phase,
        proof,
        reason,
        preserve_unknown,
        expected_allocation: allocation_bytes,
        expected_state: state_bytes,
    }))
}

pub(crate) fn reconcile_held_preflight(
    file: std::fs::File,
    source_sha256: &str,
) -> Result<HeldRecoveryPlan, ReservedRefusal> {
    with_reconciliation_copy(file, |database| {
        held_recovery_plan_in(&*database, source_sha256)
    })
}

/// Applies the entire eligible plan in one transaction. Other held rows remain
/// held and are included in the audit; no commit occurs inside the action loop.
pub(crate) fn reconcile_held_apply(
    file: std::fs::File,
    source_sha256: &str,
    expected_digest: &str,
) -> Result<serde_json::Value, ReservedRefusal> {
    use serde_json::json;
    with_reconciliation_copy(file, |database| {
        let plan = held_recovery_plan_in(&*database, source_sha256)?;
        if plan.digest != expected_digest {
            return Err(ReservedRefusal::new(
                "plan_digest_mismatch",
                "the source or its recovery decisions changed; repeat the dry-run",
            ));
        }
        if plan.actions.is_empty() {
            return Err(ReservedRefusal::new(
                "no_eligible_allocations",
                "the plan contains no Allocation this command may release",
            ));
        }
        let outcome =
            writer::run_batch_with(database, plan.epoch, writer::Commit::Durable, |batch| {
                for action in &plan.actions {
                    batch.reconcile_stopped_allocation(action)?;
                }
                batch.step(|tx| {
                    tx.open_table(META)?
                        .insert("clean_shutdown", [0u8].as_slice())?;
                    Ok(())
                })
            });
        match outcome.result {
            Ok(()) => {}
            Err(writer::BatchFailure::Rejected(violation)) => {
                return Err(ReservedRefusal::new("rejected", violation.to_string()));
            }
            Err(
                writer::BatchFailure::NotCommitted(error)
                | writer::BatchFailure::OutcomeUnknown(error),
            ) => return Err(error.into()),
        }
        if !database.check_integrity().map_err(StorageError::from)? {
            return Err(ReservedRefusal::new(
                "page_integrity",
                "redb found damage in the recovered copy and had to repair it",
            ));
        }
        let audit = audit_held_recovery(&*database, &plan)?;
        Ok(json!({
            "plan": plan.canonical,
            "plan_digest": plan.digest,
            "result": audit,
            "page_checksums": "every page checksum verified",
            "clean_shutdown": false,
        }))
    })
}

fn audit_held_recovery(
    database: &impl ReadableDatabase,
    plan: &HeldRecoveryPlan,
) -> Result<serde_json::Value, ReservedRefusal> {
    use crate::db::entities::{AllocationPhase, OutcomeCause};
    use serde_json::json;

    let tx = database.begin_read().map_err(StorageError::from)?;
    check_records(&tx)?;
    let mut recovered = Vec::new();
    for action in &plan.actions {
        let job = action.token.job;
        let allocation_row = tx
            .open_table(POOL_ALLOCATIONS)
            .map_err(StorageError::from)?
            .get(job.get())
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                ReservedRefusal::new("inconsistent_or_corrupt", "recovered Allocation vanished")
            })?;
        let allocation = transactions::decode_pool_allocation(job.get(), allocation_row.value())?;
        drop(allocation_row);
        let state_row = tx
            .open_table(JOB_STATES)
            .map_err(StorageError::from)?
            .get(job.get())
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                ReservedRefusal::new("inconsistent_or_corrupt", "recovered Job state vanished")
            })?;
        let state = JobState::decode_at(job.get(), state_row.value())?;
        let exact_state = if action.preserve_unknown {
            state_row.value() == action.expected_state
        } else {
            let mut expected = JobState::decode_at(job.get(), &action.expected_state)?;
            expected.outcome = state.outcome.clone();
            expected == state
                && state
                    .outcome
                    .as_ref()
                    .is_some_and(|outcome| Some(outcome.at) == allocation.stop_confirmed_at)
        };
        drop(state_row);
        let outcome_valid = state.outcome.as_ref().is_some_and(|outcome| {
            outcome.kind == OutcomeKind::Unknown
                && (action.preserve_unknown
                    || outcome.cause == Some(OutcomeCause::Uncertain(action.reason)))
        });
        let mut expected_allocation =
            transactions::decode_pool_allocation(job.get(), &action.expected_allocation)?;
        expected_allocation.phase = AllocationPhase::Released;
        expected_allocation.stop_confirmed_at = allocation.stop_confirmed_at;
        expected_allocation.stop_proof = Some(action.proof);
        let mut ready = false;
        for entry in tx
            .open_table(MUTABLE_READY)
            .map_err(StorageError::from)?
            .iter()
            .map_err(StorageError::from)?
        {
            if entry.map_err(StorageError::from)?.0.value().2 == job.get() {
                ready = true;
                break;
            }
        }
        let mut result_present = false;
        for entry in tx
            .open_table(JOB_OUTPUTS)
            .map_err(StorageError::from)?
            .range((job.get(), 0)..=(job.get(), u32::MAX))
            .map_err(StorageError::from)?
        {
            let (_, value) = entry.map_err(StorageError::from)?;
            if rows::output_kind(value.value())? == OutputKind::Result {
                result_present = true;
                break;
            }
        }
        if allocation != expected_allocation
            || allocation.stop_confirmed_at.is_none()
            || !outcome_valid
            || !exact_state
            || ready
            || result_present
            || !resolved_unknown_ownership(&tx, &state)?
        {
            return Err(ReservedRefusal::new(
                "audit_failed",
                format!("{job} does not have the exact recovered Unknown ownership"),
            ));
        }
        let active_key = (
            action.token.pool.get(),
            action.token.executor.get(),
            job.get(),
        );
        if tx
            .open_table(ACTIVE_ALLOCATIONS)
            .map_err(StorageError::from)?
            .get(active_key)
            .map_err(StorageError::from)?
            .is_some()
            || tx
                .open_table(ALLOCATION_REQUESTS)
                .map_err(StorageError::from)?
                .get(allocation.request_id.get())
                .map_err(StorageError::from)?
                .map(|row| row.value())
                != Some(job.get())
            || tx
                .open_table(PENDING)
                .map_err(StorageError::from)?
                .get(job.get())
                .map_err(StorageError::from)?
                .is_some()
            || tx
                .open_table(UNSETTLED)
                .map_err(StorageError::from)?
                .get(job.get())
                .map_err(StorageError::from)?
                .map(|row| row.value())
                != Some((0, 0))
        {
            return Err(ReservedRefusal::new(
                "audit_failed",
                format!("{job} has inconsistent recovered derived indexes"),
            ));
        }
        recovered.push(json!({
            "job": job.to_string(),
            "allocation_phase": "released",
            "job_outcome": "unknown",
            "stop_proof": recovery_proof_name(action.proof),
            "unknown_preserved_exactly": action.preserve_unknown,
            "dispatchable": false,
            "original_evidence_preserved": true,
        }));
    }
    let mut held = 0u64;
    for entry in tx
        .open_table(POOL_ALLOCATIONS)
        .map_err(StorageError::from)?
        .iter()
        .map_err(StorageError::from)?
    {
        let (key, value) = entry.map_err(StorageError::from)?;
        if transactions::decode_pool_allocation(key.value(), value.value())?.holds_capacity() {
            held += 1;
        }
    }
    let mut unresolved_unknown = 0u64;
    let mut resolved_unknown = 0u64;
    for entry in tx
        .open_table(JOB_STATES)
        .map_err(StorageError::from)?
        .iter()
        .map_err(StorageError::from)?
    {
        let (key, value) = entry.map_err(StorageError::from)?;
        let state = JobState::decode_at(key.value(), value.value())?;
        if state
            .outcome
            .as_ref()
            .is_some_and(|outcome| outcome.kind == OutcomeKind::Unknown)
        {
            if resolved_unknown_ownership(&tx, &state)? {
                resolved_unknown += 1;
            } else {
                unresolved_unknown += 1;
                let retained = tx
                    .open_table(POOL_ALLOCATIONS)
                    .map_err(StorageError::from)?
                    .get(key.value())
                    .map_err(StorageError::from)?
                    .map(|row| transactions::decode_pool_allocation(key.value(), row.value()))
                    .transpose()?
                    .is_some_and(|allocation| allocation.holds_capacity());
                if !retained {
                    return Err(ReservedRefusal::new(
                        "audit_failed",
                        format!(
                            "{} is unresolved Unknown without a retained held Allocation",
                            state.job_id
                        ),
                    ));
                }
            }
        }
    }
    if held == 0 && unresolved_unknown == 0 {
        refuse_unresolved_ownership(&tx)?;
    }
    let clean = tx
        .open_table(META)
        .map_err(StorageError::from)?
        .get("clean_shutdown")
        .map_err(StorageError::from)?
        .is_some_and(|value| value.value() == [0]);
    if !clean {
        return Err(ReservedRefusal::new(
            "audit_failed",
            "recovery did not leave clean_shutdown = 0",
        ));
    }
    Ok(json!({
        "recovered": recovered,
        "remaining_held_allocations": held,
        "remaining_unresolved_unknown": unresolved_unknown,
        "resolved_unknown": resolved_unknown,
        "allocation_indexes": "consistent with authoritative rows",
        "ready_or_pending_recovered_jobs": 0,
        "logical_checks": "every record header and native Allocation invariant verified",
    }))
}

/// Verifies a published-unfinalized held recovery against the exact prepared
/// plan and rollback source. The Allocation release transaction is never run.
pub(crate) fn verify_held_publication_delta(
    source: &std::fs::File,
    published: &std::fs::File,
    source_sha256: &str,
    expected_plan: &serde_json::Value,
    expected_digest: &str,
) -> Result<serde_json::Value, ReservedRefusal> {
    use serde_json::json;

    let source = verification_copy(source)?;
    let published = verification_copy(published)?;
    with_reconciliation_copy(source, |source_database| {
        if !source_database
            .check_integrity()
            .map_err(StorageError::from)?
        {
            return Err(ReservedRefusal::new(
                "page_integrity",
                "redb found damage in the rollback copy and had to repair it",
            ));
        }
        let plan = held_recovery_plan_in(&*source_database, source_sha256)?;
        if plan.digest != expected_digest || &plan.canonical != expected_plan {
            return Err(ReservedRefusal::new(
                "plan_digest_mismatch",
                "the prepared plan is not the exact plan authorized by the rollback source",
            ));
        }
        with_reconciliation_copy(published, |database| {
            if !database.check_integrity().map_err(StorageError::from)? {
                return Err(ReservedRefusal::new(
                    "page_integrity",
                    "redb found damage in the published copy and had to repair it",
                ));
            }
            verify_recovered(&*database)?;
            let targets = plan
                .actions
                .iter()
                .map(|action| action.token.job.get())
                .collect::<std::collections::BTreeSet<_>>();
            let released = plan
                .actions
                .iter()
                .map(|action| {
                    (
                        action.token.pool.get(),
                        action.token.executor.get(),
                        action.token.job.get(),
                    )
                })
                .collect::<std::collections::BTreeSet<_>>();
            verify_unrelated_recovery_rows(&*source_database, &*database, &targets)?;
            verify_held_publication_preservation(&*source_database, &*database, &released)?;
            let mut audit = audit_held_recovery(&*database, &plan)?;
            audit["unrelated_allocations_and_jobs"] =
                json!("byte-identical to the rollback source");
            audit["preserved_evidence_and_derived_rows"] = json!(
                "byte-identical to the rollback source except authorized target and active-index deltas"
            );
            Ok(json!({
                "plan": plan.canonical,
                "plan_digest": plan.digest,
                "result": audit,
                "page_checksums": "every page checksum verified",
                "clean_shutdown": false,
                "reconstructed_without_reapply": true,
            }))
        })
    })
}

fn audit_delta(detail: impl Into<String>) -> ReservedRefusal {
    ReservedRefusal::new("audit_failed", detail)
}

fn verify_identical_table<K, V>(
    source_tx: &ReadTransaction,
    published_tx: &ReadTransaction,
    definition: TableDefinition<'_, K, V>,
) -> Result<(), ReservedRefusal>
where
    K: Key + 'static,
    V: Value + 'static,
{
    use ::redb::TableHandle;

    let name = definition.name().to_owned();
    let source = source_tx
        .open_table(definition)
        .map_err(StorageError::from)?;
    let published = published_tx
        .open_table(definition)
        .map_err(StorageError::from)?;
    let mut source_rows = source.iter().map_err(StorageError::from)?;
    let mut published_rows = published.iter().map_err(StorageError::from)?;
    loop {
        let source_row = source_rows.next().transpose().map_err(StorageError::from)?;
        let published_row = published_rows
            .next()
            .transpose()
            .map_err(StorageError::from)?;
        match (source_row, published_row) {
            (None, None) => return Ok(()),
            (Some((source_key, source_value)), Some((published_key, published_value))) => {
                if K::as_bytes(&source_key.value()).as_ref()
                    != K::as_bytes(&published_key.value()).as_ref()
                    || V::as_bytes(&source_value.value()).as_ref()
                        != V::as_bytes(&published_value.value()).as_ref()
                {
                    return Err(audit_delta(format!(
                        "publication changed preserved table {name}"
                    )));
                }
            }
            _ => {
                return Err(audit_delta(format!(
                    "publication changed the row count of preserved table {name}"
                )));
            }
        }
    }
}

fn verify_unrelated_recovery_rows(
    source: &impl ReadableDatabase,
    published: &impl ReadableDatabase,
    targets: &std::collections::BTreeSet<u128>,
) -> Result<(), ReservedRefusal> {
    use ::redb::{MultimapTableHandle, TableHandle};

    let source_tx = source.begin_read().map_err(StorageError::from)?;
    let published_tx = published.begin_read().map_err(StorageError::from)?;
    let table_names = |tx: &ReadTransaction| -> Result<Vec<String>, ReservedRefusal> {
        let mut names = tx
            .list_tables()
            .map_err(StorageError::from)?
            .map(|table| table.name().to_owned())
            .collect::<Vec<_>>();
        names.extend(
            tx.list_multimap_tables()
                .map_err(StorageError::from)?
                .map(|table| format!("multimap:{}", table.name())),
        );
        Ok(names)
    };
    if table_names(&source_tx)? != table_names(&published_tx)? {
        return Err(audit_delta("publication changed the database table set"));
    }
    if source_tx
        .list_tables()
        .map_err(StorageError::from)?
        .any(|table| table.name() == HELD_SLOTS.name())
        || source_tx
            .list_multimap_tables()
            .map_err(StorageError::from)?
            .next()
            .is_some()
    {
        return Err(audit_delta(
            "the publication contains a legacy or multimap table outside this format",
        ));
    }

    macro_rules! unchanged {
        ($($table:ident),+ $(,)?) => {
            $(verify_identical_table(&source_tx, &published_tx, $table)?;)+
        };
    }
    unchanged!(
        META,
        CONFIGS,
        CREDENTIALS,
        JOBS,
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
        SCHEDULED_EXTERNAL_POLICY,
        SCHEDULED_EXTERNAL_BINDING,
        JOB_OBSERVATIONS,
        CONTENT_REFERENCES,
        CONTENT_RECLAIM,
        ALLOCATION_REQUESTS,
        DRAFT_CONTEXTS,
        JOB_ALLOCATIONS,
        CONVERSATIONS,
        TURNS,
        PENDING,
        MUTABLE_READY,
        DELAYED,
        DEPENDENCY_WAITERS,
        CHILDREN,
        SUCCESSORS,
        CONVERSATION_SEARCH,
        CHILD_GROUPS,
        LIVE_CHILDREN,
        USER_INPUT_AT,
        ACTIVE_JOBS,
        NOTICE_READ,
        TOOL_CALL_IDS,
        TOOL_STATS,
        USAGE_TOTALS,
        RETRIES,
        USAGE_MODEL,
        REQUEST_TIMELINE,
        OPEN_APPROVALS,
        APPROVAL_CALLS,
        RECURRENCE_DUE,
    );

    for definition in [POOL_ALLOCATIONS, JOB_STATES] {
        let source_table = source_tx
            .open_table(definition)
            .map_err(StorageError::from)?;
        let published_table = published_tx
            .open_table(definition)
            .map_err(StorageError::from)?;
        let mut source_count = 0u64;
        for entry in source_table.iter().map_err(StorageError::from)? {
            let (key, value) = entry.map_err(StorageError::from)?;
            source_count += 1;
            if targets.contains(&key.value()) {
                continue;
            }
            let current = published_table
                .get(key.value())
                .map_err(StorageError::from)?;
            if current.as_ref().map(|row| row.value()) != Some(value.value()) {
                return Err(audit_delta(
                    "an unrelated Allocation or Job state differs from the rollback source",
                ));
            }
        }
        let mut published_count = 0u64;
        for entry in published_table.iter().map_err(StorageError::from)? {
            let (key, _) = entry.map_err(StorageError::from)?;
            published_count += 1;
            if !targets.contains(&key.value())
                && source_table
                    .get(key.value())
                    .map_err(StorageError::from)?
                    .is_none()
            {
                return Err(audit_delta(
                    "publication introduced an unrelated Allocation or Job state",
                ));
            }
        }
        if source_count != published_count {
            return Err(audit_delta(
                "publication changed the number of Allocation or Job state rows",
            ));
        }
    }
    Ok(())
}

fn verify_active_allocation_delta(
    source_tx: &ReadTransaction,
    published_tx: &ReadTransaction,
    released: &std::collections::BTreeSet<(u128, u128, u128)>,
) -> Result<(), ReservedRefusal> {
    let source = source_tx
        .open_table(ACTIVE_ALLOCATIONS)
        .map_err(StorageError::from)?;
    let published = published_tx
        .open_table(ACTIVE_ALLOCATIONS)
        .map_err(StorageError::from)?;
    let mut removed = 0usize;
    for entry in source.iter().map_err(StorageError::from)? {
        let (key, _) = entry.map_err(StorageError::from)?;
        let current = published.get(key.value()).map_err(StorageError::from)?;
        if released.contains(&key.value()) {
            removed += 1;
            if current.is_some() {
                return Err(audit_delta(
                    "a released Allocation remains in the active index",
                ));
            }
        } else if current.is_none() {
            return Err(audit_delta(
                "publication removed an unrelated active Allocation",
            ));
        }
    }
    if removed != released.len() {
        return Err(audit_delta(
            "the rollback source lacks an authorized active Allocation",
        ));
    }
    for entry in published.iter().map_err(StorageError::from)? {
        let (key, _) = entry.map_err(StorageError::from)?;
        if source
            .get(key.value())
            .map_err(StorageError::from)?
            .is_none()
        {
            return Err(audit_delta("publication introduced an active Allocation"));
        }
    }
    Ok(())
}

fn verify_held_publication_preservation(
    source: &impl ReadableDatabase,
    published: &impl ReadableDatabase,
    released: &std::collections::BTreeSet<(u128, u128, u128)>,
) -> Result<(), ReservedRefusal> {
    let source_tx = source.begin_read().map_err(StorageError::from)?;
    let published_tx = published.begin_read().map_err(StorageError::from)?;
    verify_identical_table(&source_tx, &published_tx, SEQUENCES)?;
    verify_identical_table(&source_tx, &published_tx, JOB_OUTPUTS)?;
    verify_identical_table(&source_tx, &published_tx, UNSETTLED)?;
    verify_identical_table(&source_tx, &published_tx, GROUP_RESULTS)?;
    verify_active_allocation_delta(&source_tx, &published_tx, released)
}

fn verify_reserved_output_delta(
    source_tx: &ReadTransaction,
    published_tx: &ReadTransaction,
    job: JobId,
) -> Result<crate::db::entities::JobOutputRecord, ReservedRefusal> {
    use crate::db::entities::{OutputKind, UnstartedCause};

    let source = source_tx
        .open_table(JOB_OUTPUTS)
        .map_err(StorageError::from)?;
    let published = published_tx
        .open_table(JOB_OUTPUTS)
        .map_err(StorageError::from)?;
    let output_key = (job.get(), 1);
    let output_row = published
        .get(output_key)
        .map_err(StorageError::from)?
        .ok_or_else(|| audit_delta("the OfflineReconciled Result is missing"))?;
    let output = rows::decode_output(output_key, output_row.value())?;
    drop(output_row);
    if output.job != job
        || output.seq != 1
        || output.kind != OutputKind::Result
        || output.summary.as_deref() != Some(UnstartedCause::OfflineReconciled.summary())
        || output.code.as_deref() != Some(UnstartedCause::OfflineReconciled.code())
        || output.goal_reached != Some(false)
        || output.value.is_some()
    {
        return Err(audit_delta(
            "the published output is not the exact OfflineReconciled Result",
        ));
    }
    for entry in source.iter().map_err(StorageError::from)? {
        let (key, value) = entry.map_err(StorageError::from)?;
        if published
            .get(key.value())
            .map_err(StorageError::from)?
            .as_ref()
            .map(|row| row.value())
            != Some(value.value())
        {
            return Err(audit_delta("publication changed a pre-existing output"));
        }
    }
    let mut additions = 0usize;
    for entry in published.iter().map_err(StorageError::from)? {
        let (key, _) = entry.map_err(StorageError::from)?;
        if source
            .get(key.value())
            .map_err(StorageError::from)?
            .is_none()
        {
            if key.value() != output_key {
                return Err(audit_delta("publication introduced an unrelated output"));
            }
            additions += 1;
        }
    }
    if additions != 1 {
        return Err(audit_delta(
            "publication did not add exactly one OfflineReconciled Result",
        ));
    }

    let source_sequences = source_tx
        .open_table(SEQUENCES)
        .map_err(StorageError::from)?;
    let published_sequences = published_tx
        .open_table(SEQUENCES)
        .map_err(StorageError::from)?;
    let previous = source_sequences
        .get("entity")
        .map_err(StorageError::from)?
        .map(|row| row.value())
        .ok_or_else(|| audit_delta("the rollback source lacks the entity sequence"))?;
    if output.id.get() <= previous
        || published_sequences
            .get("entity")
            .map_err(StorageError::from)?
            .map(|row| row.value())
            != Some(output.id.get())
    {
        return Err(audit_delta(
            "the Result identity and entity sequence are not the authorized allocation",
        ));
    }
    for entry in source_sequences.iter().map_err(StorageError::from)? {
        let (key, value) = entry.map_err(StorageError::from)?;
        let expected = if key.value() == "entity" {
            output.id.get()
        } else {
            value.value()
        };
        if published_sequences
            .get(key.value())
            .map_err(StorageError::from)?
            .map(|row| row.value())
            != Some(expected)
        {
            return Err(audit_delta("publication changed an unrelated sequence"));
        }
    }
    for entry in published_sequences.iter().map_err(StorageError::from)? {
        let (key, _) = entry.map_err(StorageError::from)?;
        if source_sequences
            .get(key.value())
            .map_err(StorageError::from)?
            .is_none()
        {
            return Err(audit_delta("publication introduced a sequence"));
        }
    }
    Ok(output)
}

fn verify_reserved_settlement_delta(
    source_tx: &ReadTransaction,
    published_tx: &ReadTransaction,
    job: JobId,
    parent: JobId,
) -> Result<(), ReservedRefusal> {
    let source = source_tx
        .open_table(UNSETTLED)
        .map_err(StorageError::from)?;
    let published = published_tx
        .open_table(UNSETTLED)
        .map_err(StorageError::from)?;
    if source
        .get(job.get())
        .map_err(StorageError::from)?
        .map(|row| row.value())
        != Some((0, 0))
        || published
            .get(job.get())
            .map_err(StorageError::from)?
            .is_some()
    {
        return Err(audit_delta(
            "the recovered Job's unsettled row was not removed exactly",
        ));
    }
    let (dependencies, children) = source
        .get(parent.get())
        .map_err(StorageError::from)?
        .map(|row| row.value())
        .ok_or_else(|| audit_delta("the source conversation unsettled row is missing"))?;
    let expected_children = children
        .checked_sub(1)
        .ok_or_else(|| audit_delta("the source conversation child count underflows"))?;
    for entry in source.iter().map_err(StorageError::from)? {
        let (key, value) = entry.map_err(StorageError::from)?;
        if key.value() == job.get() {
            continue;
        }
        let expected = if key.value() == parent.get() {
            (dependencies, expected_children)
        } else {
            value.value()
        };
        if published
            .get(key.value())
            .map_err(StorageError::from)?
            .map(|row| row.value())
            != Some(expected)
        {
            return Err(audit_delta(
                "publication changed an unrelated unsettled row",
            ));
        }
    }
    for entry in published.iter().map_err(StorageError::from)? {
        let (key, _) = entry.map_err(StorageError::from)?;
        if source
            .get(key.value())
            .map_err(StorageError::from)?
            .is_none()
        {
            return Err(audit_delta("publication introduced an unsettled row"));
        }
    }

    let source = source_tx
        .open_table(GROUP_RESULTS)
        .map_err(StorageError::from)?;
    let published = published_tx
        .open_table(GROUP_RESULTS)
        .map_err(StorageError::from)?;
    let previous = source
        .get(parent.get())
        .map_err(StorageError::from)?
        .map_or((0, 0), |row| row.value());
    let expected = (previous.0.max(job.get()), previous.1.max(job.get()));
    if published
        .get(parent.get())
        .map_err(StorageError::from)?
        .map(|row| row.value())
        != Some(expected)
    {
        return Err(audit_delta(
            "the conversation settlement result is not the authorized delta",
        ));
    }
    for entry in source.iter().map_err(StorageError::from)? {
        let (key, value) = entry.map_err(StorageError::from)?;
        if key.value() != parent.get()
            && published
                .get(key.value())
                .map_err(StorageError::from)?
                .map(|row| row.value())
                != Some(value.value())
        {
            return Err(audit_delta(
                "publication changed an unrelated conversation result",
            ));
        }
    }
    for entry in published.iter().map_err(StorageError::from)? {
        let (key, _) = entry.map_err(StorageError::from)?;
        if key.value() != parent.get()
            && source
                .get(key.value())
                .map_err(StorageError::from)?
                .is_none()
        {
            return Err(audit_delta("publication introduced a conversation result"));
        }
    }
    Ok(())
}
