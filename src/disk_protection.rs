//! Runtime disk protection.
//!
//! Storage admission locks when a protected filesystem runs short of space,
//! when its space cannot be measured, or when storage itself fails. The lock
//! is held in memory only: operating it never writes Jobs, JobSteps, Events
//! or any other record. It clears only through an explicit recovery, never
//! merely because space has become sufficient again.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use crate::data_directory::DataDirectory;
use crate::database::{OperationStatus, StorageError};
use crate::persistence::{OperationId, Timestamp};

pub const DEFAULT_DISK_LOCK_THRESHOLD_BYTES: u64 = 1024 * 1024 * 1024;
pub const DEFAULT_DISK_RESUME_THRESHOLD_BYTES: u64 = 2 * 1024 * 1024 * 1024;
pub const DEFAULT_DISK_CHECK_INTERVAL_MS: u64 = 120 * 60 * 1000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CleanupConfig {
    pub max_log_files_per_run: u32,
}

impl Default for CleanupConfig {
    fn default() -> Self {
        Self { max_log_files_per_run: 1000 }
    }
}

/// The disk-protection settings of the system policy's filesystem
/// configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FilesystemConfig {
    /// A write is refused when it would leave less than this many bytes
    /// available on a protected filesystem.
    pub disk_lock_threshold_bytes: u64,
    /// Recovery requires at least this many bytes available on every
    /// protected filesystem.
    pub disk_resume_threshold_bytes: u64,
    /// How often every protected filesystem is checked.
    pub disk_check_interval_ms: u64,
    #[serde(default)]
    pub cleanup_config: CleanupConfig,
}

impl Default for FilesystemConfig {
    fn default() -> Self {
        Self {
            disk_lock_threshold_bytes: DEFAULT_DISK_LOCK_THRESHOLD_BYTES,
            disk_resume_threshold_bytes: DEFAULT_DISK_RESUME_THRESHOLD_BYTES,
            disk_check_interval_ms: DEFAULT_DISK_CHECK_INTERVAL_MS,
            cleanup_config: CleanupConfig::default(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum FilesystemConfigError {
    #[error("disk_lock_threshold_bytes must be greater than zero")]
    LockThresholdZero,
    #[error("disk_resume_threshold_bytes ({resume}) must be greater than disk_lock_threshold_bytes ({lock})")]
    ResumeNotAboveLock { lock: u64, resume: u64 },
    #[error("disk_check_interval_ms must be greater than zero")]
    CheckIntervalZero,
    #[error("cleanup_config.max_log_files_per_run must be greater than zero")]
    CleanupLimitZero,
}

impl FilesystemConfig {
    pub fn validate(&self) -> Result<(), FilesystemConfigError> {
        if self.disk_lock_threshold_bytes == 0 {
            return Err(FilesystemConfigError::LockThresholdZero);
        }
        if self.disk_resume_threshold_bytes <= self.disk_lock_threshold_bytes {
            return Err(FilesystemConfigError::ResumeNotAboveLock {
                lock: self.disk_lock_threshold_bytes,
                resume: self.disk_resume_threshold_bytes,
            });
        }
        if self.disk_check_interval_ms == 0 {
            return Err(FilesystemConfigError::CheckIntervalZero);
        }
        if self.cleanup_config.max_log_files_per_run == 0 {
            return Err(FilesystemConfigError::CleanupLimitZero);
        }
        Ok(())
    }

    pub fn check_interval(&self) -> Duration {
        Duration::from_millis(self.disk_check_interval_ms)
    }
}

/// A directory of the data layout whose filesystem is protected.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ProtectedArea {
    Database,
    Attachments,
    Logs,
}

pub(crate) fn protected_paths(data_directory: &DataDirectory) -> [(ProtectedArea, PathBuf); 3] {
    [
        (ProtectedArea::Database, data_directory.database_path().to_path_buf()),
        (ProtectedArea::Attachments, data_directory.attachments_path().to_path_buf()),
        (ProtectedArea::Logs, data_directory.logs_path().to_path_buf()),
    ]
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpaceShortage {
    pub area: ProtectedArea,
    pub path: PathBuf,
    /// Space available to the current user when it was measured.
    pub available_bytes: u64,
    /// The allowance of the refused write; zero for a check without a write.
    pub expected_write_bytes: u64,
    /// The threshold that was not met: the lock threshold, or the resume
    /// threshold during recovery.
    pub threshold_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpaceQueryFailure {
    pub area: ProtectedArea,
    pub path: PathBuf,
    pub kind: io::ErrorKind,
    pub os_error: Option<i32>,
    pub detail: String,
}

/// Why storage admission is locked.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ProtectionReason {
    #[error(
        "{} has {} bytes available; {} more bytes would leave less than {}",
        .0.path.display(), .0.available_bytes, .0.expected_write_bytes, .0.threshold_bytes
    )]
    InsufficientSpace(SpaceShortage),
    #[error("the available space of {} could not be measured: {}", .0.path.display(), .0.detail)]
    SpaceQueryFailed(SpaceQueryFailure),
    #[error("filesystem write to {} failed: {detail}", path.display())]
    FilesystemWriteFailed {
        area: ProtectedArea,
        path: PathBuf,
        kind: io::ErrorKind,
        os_error: Option<i32>,
        detail: String,
    },
    #[error("storage failed and the database must be reopened: {0}")]
    StorageUnavailable(StorageError),
}

impl ProtectionReason {
    /// A newer reason replaces an older one about the same subject.
    fn same_subject(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::InsufficientSpace(a), Self::InsufficientSpace(b)) => a.area == b.area,
            (Self::SpaceQueryFailed(a), Self::SpaceQueryFailed(b)) => a.area == b.area,
            (
                Self::FilesystemWriteFailed { area: a, .. },
                Self::FilesystemWriteFailed { area: b, .. },
            ) => a == b,
            (Self::StorageUnavailable(_), Self::StorageUnavailable(_)) => true,
            _ => false,
        }
    }
}

/// An active lock with every reason found while it has been held.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProtectionLock {
    pub reasons: Vec<ProtectionReason>,
    pub locked_at: Timestamp,
}

impl ProtectionLock {
    /// The storage failure that released the database, if any.
    pub fn storage_failure(&self) -> Option<&StorageError> {
        self.reasons.iter().find_map(|reason| match reason {
            ProtectionReason::StorageUnavailable(error) => Some(error),
            _ => None,
        })
    }
}

impl std::fmt::Display for ProtectionLock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (index, reason) in self.reasons.iter().enumerate() {
            if index > 0 {
                f.write_str("; ")?;
            }
            write!(f, "{reason}")?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DiskProtectionStatus {
    /// Present while mutations are refused.
    pub lock: Option<ProtectionLock>,
    pub recovering: bool,
    /// Full checks of every protected filesystem, periodic or requested.
    pub completed_checks: u64,
    pub last_checked_at: Option<Timestamp>,
    pub recovery_attempts: u64,
    /// Increases on every change, so a watcher can tell how many changes
    /// coalesced into one notification.
    pub generation: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpaceMeasurement {
    pub area: ProtectedArea,
    pub path: PathBuf,
    pub available_bytes: u64,
}

/// A successful recovery. Recovering an unlocked database is a no-op that
/// returns an empty report.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RecoveryReport {
    /// The epoch of the reopened database, when the failed one was replaced.
    pub reopened_epoch: Option<u64>,
    /// Every operation whose outcome had been unknown, as its marker proves
    /// it now. None of them is retried.
    pub reconciled: Vec<(OperationId, OperationStatus)>,
    pub available: Vec<SpaceMeasurement>,
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ReopenFailure {
    #[error("{0}")]
    Storage(StorageError),
    #[error("{} is not an OCG database", .0.display())]
    ForeignDatabase(PathBuf),
    #[error("the database uses storage format {found}; this build supports only format {supported}")]
    UnsupportedFormat { found: u32, supported: u32 },
}

/// Why a recovery attempt left the lock in place.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum RecoveryError {
    #[error("the control queue is full; recovery was not accepted")]
    ControlQueueFull,
    #[error("a protected filesystem does not have the resume threshold available")]
    Space(Vec<ProtectionReason>),
    #[error("reopening the database failed: {0}")]
    Reopen(ReopenFailure),
    #[error("reconciling {operation} failed: {error}")]
    Reconcile { operation: OperationId, error: StorageError },
    #[error("the database is shutting down")]
    ShuttingDown,
    #[error("the database worker stopped")]
    WorkerStopped,
}

fn measure(area: ProtectedArea, path: &Path) -> Result<u64, SpaceQueryFailure> {
    fs4::available_space(path).map_err(|error| SpaceQueryFailure {
        area,
        path: path.to_path_buf(),
        kind: error.kind(),
        os_error: error.raw_os_error(),
        detail: error.to_string(),
    })
}

/// Whether `available_bytes` minus `expected_write_bytes` still reaches
/// `threshold_bytes`; a write larger than the available space never does.
pub(crate) fn leaves_threshold(available_bytes: u64, expected_write_bytes: u64, threshold_bytes: u64) -> bool {
    available_bytes
        .checked_sub(expected_write_bytes)
        .is_some_and(|remaining| remaining >= threshold_bytes)
}

/// Measures the filesystem holding `path` and refuses a write of
/// `expected_write_bytes` that would leave less than `threshold_bytes`
/// available. A failed measurement refuses too, for its own reason. Returns
/// the measured space.
pub(crate) fn check_space(
    area: ProtectedArea,
    path: &Path,
    expected_write_bytes: u64,
    threshold_bytes: u64,
) -> Result<u64, ProtectionReason> {
    let available_bytes = measure(area, path).map_err(ProtectionReason::SpaceQueryFailed)?;
    if leaves_threshold(available_bytes, expected_write_bytes, threshold_bytes) {
        Ok(available_bytes)
    } else {
        Err(ProtectionReason::InsufficientSpace(SpaceShortage {
            area,
            path: path.to_path_buf(),
            available_bytes,
            expected_write_bytes,
            threshold_bytes,
        }))
    }
}

/// The published protection status and its watchers. Its locks are only
/// ever held to copy or change the status, never across a database
/// operation, so reading it does not wait for storage.
pub(crate) struct ProtectionCell {
    blocked: AtomicBool,
    lock_epoch: AtomicU64,
    status: Mutex<DiskProtectionStatus>,
    /// `None` once the database worker has stopped, so watchers see the end.
    watchers: Mutex<Option<Vec<flume::Sender<()>>>>,
}

impl ProtectionCell {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            blocked: AtomicBool::new(false),
            lock_epoch: AtomicU64::new(0),
            status: Mutex::new(DiskProtectionStatus::default()),
            watchers: Mutex::new(Some(Vec::new())),
        })
    }

    pub(crate) fn status(&self) -> DiskProtectionStatus {
        self.status.lock().unwrap_or_else(PoisonError::into_inner).clone()
    }

    pub(crate) fn logging_state(&self) -> (bool, u64) {
        let epoch = self.lock_epoch.load(Ordering::SeqCst);
        (self.blocked.load(Ordering::SeqCst), epoch)
    }

    pub(crate) fn update<T>(&self, change: impl FnOnce(&mut DiskProtectionStatus) -> T) -> T {
        let value = {
            let mut status = self.status.lock().unwrap_or_else(PoisonError::into_inner);
            let value = change(&mut status);
            self.blocked.store(status.lock.is_some(), Ordering::SeqCst);
            status.generation += 1;
            value
        };
        let mut watchers = self.watchers.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(watchers) = watchers.as_mut() {
            // A full channel already holds an unread notification.
            watchers.retain(|watcher| !matches!(watcher.try_send(()), Err(flume::TrySendError::Disconnected(()))));
        }
        value
    }

    /// Locks with `reasons`, or adds them to the held lock, replacing older
    /// reasons about the same subject. Returns the resulting lock.
    pub(crate) fn lock(&self, reasons: Vec<ProtectionReason>) -> ProtectionLock {
        self.blocked.store(true, Ordering::SeqCst);
        self.lock_epoch.fetch_add(1, Ordering::SeqCst);
        self.update(|status| {
            let lock = status.lock.get_or_insert_with(|| ProtectionLock {
                reasons: Vec::new(),
                locked_at: Timestamp::now(),
            });
            for reason in reasons {
                match lock.reasons.iter_mut().find(|held| held.same_subject(&reason)) {
                    Some(held) => *held = reason,
                    None => lock.reasons.push(reason),
                }
            }
            lock.clone()
        })
    }

    pub(crate) fn watch(self: &Arc<Self>) -> ProtectionWatch {
        let (sender, receiver) = flume::bounded(1);
        if let Some(watchers) = self.watchers.lock().unwrap_or_else(PoisonError::into_inner).as_mut() {
            watchers.push(sender);
        }
        ProtectionWatch { receiver, cell: self.clone() }
    }

    pub(crate) fn close(&self) {
        self.watchers.lock().unwrap_or_else(PoisonError::into_inner).take();
    }
}

/// Notifies of protection status changes. Changes made while nobody is
/// waiting coalesce into one notification; compare
/// [`DiskProtectionStatus::generation`] to count them.
pub struct ProtectionWatch {
    receiver: flume::Receiver<()>,
    cell: Arc<ProtectionCell>,
}

impl ProtectionWatch {
    pub(crate) fn notifications(&self) -> &flume::Receiver<()> {
        &self.receiver
    }
    pub fn status(&self) -> DiskProtectionStatus {
        self.cell.status()
    }

    /// Waits for the next change and returns the status after it, or `None`
    /// once the database worker has stopped. Needs no particular async
    /// runtime.
    pub async fn changed(&self) -> Option<DiskProtectionStatus> {
        self.receiver.recv_async().await.ok().map(|()| self.cell.status())
    }
}

/// A request that callers share while it is pending: the first caller
/// starts it, later callers wait for the same result.
pub(crate) struct SharedRequest<T> {
    waiters: Mutex<Option<Vec<flume::Sender<T>>>>,
}

impl<T: Clone> SharedRequest<T> {
    pub(crate) fn new() -> Self {
        Self { waiters: Mutex::new(None) }
    }

    /// Joins the pending request, or starts one with `start` when none is
    /// pending. `start` runs under this request's lock, so a completion
    /// cannot slip between starting and registering.
    pub(crate) fn join<E>(&self, start: impl FnOnce() -> Result<(), E>) -> Result<flume::Receiver<T>, E> {
        let mut waiters = self.waiters.lock().unwrap_or_else(PoisonError::into_inner);
        let (sender, receiver) = flume::bounded(1);
        match waiters.as_mut() {
            Some(waiters) => waiters.push(sender),
            None => {
                start()?;
                *waiters = Some(vec![sender]);
            }
        }
        Ok(receiver)
    }

    pub(crate) fn complete(&self, result: T) {
        let waiters = self.waiters.lock().unwrap_or_else(PoisonError::into_inner).take();
        for waiter in waiters.into_iter().flatten() {
            waiter.send(result.clone()).ok();
        }
    }

    /// Drops every waiter unanswered, which they observe as a stopped worker.
    pub(crate) fn abandon(&self) {
        self.waiters.lock().unwrap_or_else(PoisonError::into_inner).take();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::{DatabaseStatus, DatabaseWorker};
    use crate::test_support::{data_directory as test_data_directory, wait};

    const SUITE: &str = "ocg-disk-protection";

    fn data_directory(name: &str) -> (crate::test_support::TestRoot, DataDirectory) {
        test_data_directory(SUITE, name)
    }

    #[test]
    fn space_allowance_respects_lock_threshold() {
        assert!(leaves_threshold(100, 10, 90));
        assert!(!leaves_threshold(100, 11, 90));
        assert!(!leaves_threshold(10, 11, 1));

        let root = std::env::temp_dir().join(format!("ocg-space-check-{}", std::process::id()));
        std::fs::remove_dir_all(&root).ok();
        std::fs::create_dir_all(&root).expect("create space-check directory");
        assert!(check_space(ProtectedArea::Database, &root, 0, 1).is_ok());
        assert!(matches!(
            check_space(ProtectedArea::Database, &root, u64::MAX, 1),
            Err(ProtectionReason::InsufficientSpace(_))
        ));
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn protected_database_recovers_only_after_resume_threshold_is_met() {
        let (_root, directory) = data_directory("hysteresis");
        let worker = DatabaseWorker::start_with_config(
            &directory,
            FilesystemConfig {
                disk_lock_threshold_bytes: 1,
                disk_resume_threshold_bytes: 2,
                disk_check_interval_ms: 60_000,
                cleanup_config: CleanupConfig::default(),
            },
        )
        .expect("start database");
        let handle = worker.handle();
        let reason = ProtectionReason::InsufficientSpace(SpaceShortage {
            area: ProtectedArea::Database,
            path: directory.database_path().to_path_buf(),
            available_bytes: 0,
            expected_write_bytes: 0,
            threshold_bytes: 1,
        });
        assert!(handle.activate_filesystem_protection(reason));
        assert_eq!(handle.status(), DatabaseStatus::Protected);
        assert!(handle.disk_protection().lock.is_some());

        let report = wait(handle.recover_storage()).expect("recover protected database");
        assert_eq!(report.reopened_epoch, None);
        assert_eq!(handle.status(), DatabaseStatus::Open);
        assert!(handle.disk_protection().lock.is_none());
        assert!(handle.disk_protection().recovery_attempts >= 1);
        worker.shutdown().expect("shutdown database");
    }

    #[test]
    fn protected_database_stays_locked_when_resume_threshold_is_not_met() {
        let (_root, directory) = data_directory("hysteresis-blocked");
        let worker = DatabaseWorker::start_with_config(
            &directory,
            FilesystemConfig {
                disk_lock_threshold_bytes: 1,
                disk_resume_threshold_bytes: u64::MAX,
                disk_check_interval_ms: 60_000,
                cleanup_config: CleanupConfig::default(),
            },
        )
        .expect("start database at the lock threshold");
        let handle = worker.handle();
        let reason = ProtectionReason::InsufficientSpace(SpaceShortage {
            area: ProtectedArea::Database,
            path: directory.database_path().to_path_buf(),
            available_bytes: 0,
            expected_write_bytes: 1,
            threshold_bytes: 1,
        });
        assert!(handle.activate_filesystem_protection(reason));

        let recovery = wait(handle.recover_storage());
        assert!(matches!(recovery, Err(RecoveryError::Space(_))));
        assert_eq!(handle.status(), DatabaseStatus::Protected);
        assert!(handle.disk_protection().lock.is_some());
        worker.shutdown().expect("shutdown database");
    }

    /// Every protected filesystem is checked every 20 milliseconds.
    fn checked_often(resume_threshold_bytes: u64) -> FilesystemConfig {
        FilesystemConfig {
            disk_lock_threshold_bytes: 1,
            disk_resume_threshold_bytes: resume_threshold_bytes,
            disk_check_interval_ms: 20,
            cleanup_config: CleanupConfig::default(),
        }
    }

    #[test]
    fn a_locked_database_is_checked_at_least_every_protected_interval() {
        use crate::database::{PROTECTED_CHECK_INTERVAL, next_check_interval};
        let hours = Duration::from_secs(2 * 60 * 60);
        assert_eq!(next_check_interval(hours, false), hours);
        assert_eq!(next_check_interval(hours, true), PROTECTED_CHECK_INTERVAL);
        let short = Duration::from_millis(20);
        assert_eq!(next_check_interval(short, true), short);
        assert_eq!(next_check_interval(short, false), short);
    }

    fn shortage(area: ProtectedArea, path: &Path) -> ProtectionReason {
        ProtectionReason::InsufficientSpace(SpaceShortage {
            area,
            path: path.to_path_buf(),
            available_bytes: 0,
            expected_write_bytes: 0,
            threshold_bytes: 1,
        })
    }

    /// Waits until the database has completed `checks` more full checks.
    fn await_checks(handle: &crate::database::DatabaseHandle, checks: u64) {
        let target = handle.disk_protection().completed_checks + checks;
        let deadline = std::time::Instant::now() + crate::test_support::WAIT;
        while handle.disk_protection().completed_checks < target {
            assert!(std::time::Instant::now() < deadline, "no check ran in time");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn await_open(handle: &crate::database::DatabaseHandle) {
        let deadline = std::time::Instant::now() + crate::test_support::WAIT;
        while handle.status() != DatabaseStatus::Open {
            assert!(
                std::time::Instant::now() < deadline,
                "the lock was not lifted in time"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn a_lock_for_lack_of_space_on_several_paths_lifts_by_itself_once_space_returns() {
        let (_root, directory) = data_directory("auto-recovery");
        // Checked every second, so both reasons are in place before the
        // first check after they were locked runs.
        let worker = DatabaseWorker::start_with_config(
            &directory,
            FilesystemConfig {
                disk_check_interval_ms: 1_000,
                ..checked_often(2)
            },
        )
        .expect("start database");
        let handle = worker.handle();
        let checks = handle.disk_protection().completed_checks;
        assert!(handle.activate_filesystem_protection(shortage(
            ProtectedArea::Database,
            directory.database_path()
        )));
        assert!(
            handle.activate_filesystem_protection(shortage(
                ProtectedArea::Logs,
                directory.logs_path()
            ))
        );
        assert_eq!(
            handle.disk_protection().lock.map(|lock| lock.reasons.len()),
            Some(2)
        );
        assert_eq!(handle.status(), DatabaseStatus::Protected);

        // The next check finds the resume threshold on every path.
        await_open(&handle);
        let status = handle.disk_protection();
        assert!(status.lock.is_none());
        assert!(status.recovery_attempts >= 1);
        assert!(status.completed_checks > checks);
        wait(handle.create_project(crate::persistence::NewProject {
            name: "after recovery".into(),
            description: None,
        }))
        .expect("commands are accepted again");
        worker.shutdown().expect("shutdown database");
    }

    #[test]
    fn a_lock_stays_while_a_path_lacks_the_resume_threshold() {
        let (_root, directory) = data_directory("auto-recovery-short");
        let worker = DatabaseWorker::start_with_config(&directory, checked_often(u64::MAX))
            .expect("start database");
        let handle = worker.handle();
        assert!(handle.activate_filesystem_protection(shortage(
            ProtectedArea::Attachments,
            directory.attachments_path()
        )));
        let attempts = handle.disk_protection().recovery_attempts;
        await_checks(&handle, 3);
        let status = handle.disk_protection();
        assert_eq!(handle.status(), DatabaseStatus::Protected);
        assert!(status.lock.is_some());
        assert!(
            status.recovery_attempts > attempts,
            "every check tried, and each attempt kept the lock"
        );
        worker.shutdown().expect("shutdown database");
    }

    #[test]
    fn a_failed_space_measurement_keeps_the_lock_until_an_explicit_recovery() {
        let (_root, directory) = data_directory("auto-recovery-unmeasured");
        let worker = DatabaseWorker::start_with_config(&directory, checked_often(2))
            .expect("start database");
        let handle = worker.handle();
        // A path that cannot be measured fails closed.
        std::fs::remove_dir(directory.attachments_path()).expect("remove attachments");
        assert!(handle.activate_filesystem_protection(shortage(
            ProtectedArea::Database,
            directory.database_path()
        )));
        await_checks(&handle, 1);
        let lock = handle.disk_protection().lock.expect("still locked");
        assert!(lock.reasons.iter().any(|reason| matches!(
            reason,
            ProtectionReason::SpaceQueryFailed(failure) if failure.area == ProtectedArea::Attachments
        )));

        // Measurable again, but the failed measurement is no shortage of
        // space, so the lock is left to an explicit recovery.
        std::fs::create_dir(directory.attachments_path()).expect("restore attachments");
        await_checks(&handle, 3);
        assert_eq!(handle.status(), DatabaseStatus::Protected);
        wait(handle.recover_storage()).expect("explicit recovery");
        assert_eq!(handle.status(), DatabaseStatus::Open);
        worker.shutdown().expect("shutdown database");
    }

    #[test]
    fn storage_and_write_failures_are_never_lifted_automatically() {
        let failures = [
            ProtectionReason::StorageUnavailable(crate::storage::StorageError::corrupted(
                "injected failed commit",
            )),
            ProtectionReason::FilesystemWriteFailed {
                area: ProtectedArea::Logs,
                path: PathBuf::from("injected"),
                kind: io::ErrorKind::Other,
                os_error: None,
                detail: "injected failed write".into(),
            },
        ];
        for (index, failure) in failures.into_iter().enumerate() {
            let (_root, directory) = data_directory(&format!("no-auto-recovery-{index}"));
            let worker = DatabaseWorker::start_with_config(&directory, checked_often(2))
                .expect("start database");
            let handle = worker.handle();
            // A shortage beside the failure does not make it lift either.
            assert!(handle.activate_filesystem_protection(shortage(
                ProtectedArea::Database,
                directory.database_path()
            )));
            assert!(handle.activate_filesystem_protection(failure));
            let attempts = handle.disk_protection().recovery_attempts;
            await_checks(&handle, 3);
            assert_eq!(handle.status(), DatabaseStatus::Protected);
            assert_eq!(
                handle.disk_protection().recovery_attempts,
                attempts,
                "no recovery is attempted for it"
            );
            wait(handle.recover_storage()).expect("explicit recovery");
            assert_eq!(handle.status(), DatabaseStatus::Open);
            worker.shutdown().expect("shutdown database");
        }
    }
}
