use std::fs::{self, Metadata};
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime};

use super::logger::is_runtime_file_name;
use crate::db::entities::JobOutcome;
use crate::db::{DatabaseError, DatabaseHandle, DatabaseStatus};
use crate::system::monitor::{ProtectedArea, ProtectionReason};

pub const CLEANUP_QUEUE_CAPACITY: usize = 8;
/// How often a scheduled worker looks for expired runtime logs. Retention is
/// counted in days, so an hour keeps a log at most an hour past it.
pub const RETENTION_CHECK_INTERVAL: Duration = Duration::from_secs(60 * 60);
pub const CLEANUP_FAILURE_DETAIL_LIMIT: usize = 16;
const FAILURE_MESSAGE_BYTES: usize = 256;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct CleanupFailure {
    pub file: Option<String>,
    pub operation: &'static str,
    pub os_error: Option<i32>,
    pub message: String,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct CleanupSummary {
    pub retention_days: u32,
    pub max_log_files_per_run: u32,
    pub inspected_count: u64,
    pub deleted_count: u64,
    pub skipped_count: u64,
    pub failure_count: u64,
    pub deleted_file_bytes: u64,
    pub failure_details: Vec<CleanupFailure>,
    pub failure_details_truncated: bool,
    // None means eligibility could not be determined, not that nothing remains.
    pub additional_candidates_remain: Option<bool>,
    pub scan_interrupted: bool,
    pub interruption_reason: Option<&'static str>,
}

impl CleanupSummary {
    fn failure(&mut self, file: Option<&Path>, operation: &'static str, error: io::Error) {
        self.failure_count = self.failure_count.saturating_add(1);
        if self.failure_details.len() == CLEANUP_FAILURE_DETAIL_LIMIT {
            self.failure_details_truncated = true;
            return;
        }
        self.failure_details.push(CleanupFailure {
            file: file
                .and_then(Path::file_name)
                .map(|name| bounded(&name.to_string_lossy(), 128)),
            operation,
            os_error: error.raw_os_error(),
            message: bounded(&error.to_string(), FAILURE_MESSAGE_BYTES),
        });
    }

    fn interrupt(&mut self, reason: &'static str) {
        self.scan_interrupted = true;
        self.interruption_reason = Some(reason);
        self.additional_candidates_remain = None;
    }
}

fn bounded(text: &str, limit: usize) -> String {
    let mut end = text.len().min(limit);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_owned()
}

#[derive(Debug)]
pub struct CleanupResult {
    pub outcome: JobOutcome,
    pub summary: CleanupSummary,
}

#[derive(Debug, thiserror::Error)]
pub enum CleanupError {
    #[error("the log cleanup executor queue is full; invocation was not accepted")]
    QueueFull,
    #[error("log cleanup is shutting down; invocation was not started")]
    ShuttingDown,
    #[error("a log cleanup worker already exists for this database")]
    AlreadyRunning,
    #[error("failed to start log cleanup worker: {0}")]
    Spawn(#[source] io::Error),
    #[error(
        "log cleanup worker stopped without answering; inspect recorded work before invoking again"
    )]
    WorkerStopped,
    #[error("log cleanup worker panicked")]
    WorkerPanicked,
    #[error("log cleanup {stage} persistence failed: {source}")]
    Persistence {
        stage: &'static str,
        summary: Option<Box<CleanupSummary>>,
        #[source]
        source: DatabaseError,
    },
}

struct Shared {
    database: DatabaseHandle,
    admission: Mutex<bool>,
    stopping: AtomicBool,
}
struct Request {
    reply: flume::Sender<Result<CleanupResult, CleanupError>>,
}
#[derive(Clone)]
pub struct CoreLogCleanupHandle {
    shared: Arc<Shared>,
    sender: flume::Sender<Request>,
}

impl CoreLogCleanupHandle {
    pub async fn invoke(&self) -> Result<CleanupResult, CleanupError> {
        let (reply, response) = flume::bounded(1);
        {
            let admission = self
                .shared
                .admission
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if !*admission {
                return Err(CleanupError::ShuttingDown);
            }
            check_database(&self.shared.database).map_err(|source| CleanupError::Persistence {
                stage: "admission",
                summary: None,
                source,
            })?;
            self.sender
                .try_send(Request { reply })
                .map_err(|error| match error {
                    flume::TrySendError::Full(_) => CleanupError::QueueFull,
                    flume::TrySendError::Disconnected(_) => CleanupError::WorkerStopped,
                })?;
        }
        response
            .recv_async()
            .await
            .unwrap_or(Err(CleanupError::WorkerStopped))
    }
}

pub struct CoreLogCleanup {
    handle: CoreLogCleanupHandle,
    stop: flume::Sender<()>,
    thread: Option<JoinHandle<()>>,
}

/// Retention runs once at host startup and then every `interval`, without
/// creating Jobs or consuming execution capacity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetentionSchedule {
    pub interval: Duration,
}

/// What wakes the worker.
enum WorkerWake {
    Request(Result<Request, flume::RecvError>),
    Stop,
    Due,
}

pub async fn start_retention(database: &DatabaseHandle) -> Result<CoreLogCleanup, CleanupError> {
    CoreLogCleanup::start_scheduled(
        database.clone(),
        RetentionSchedule {
            interval: RETENTION_CHECK_INTERVAL,
        },
    )
}

impl CoreLogCleanup {
    /// Starts a worker that runs cleanup only when invoked.
    pub fn start(database: DatabaseHandle) -> Result<Self, CleanupError> {
        Self::start_with(database, None)
    }

    /// Starts a worker that also runs `schedule` by itself, until shutdown.
    pub fn start_scheduled(
        database: DatabaseHandle,
        schedule: RetentionSchedule,
    ) -> Result<Self, CleanupError> {
        Self::start_with(database, Some(schedule))
    }

    fn start_with(
        database: DatabaseHandle,
        schedule: Option<RetentionSchedule>,
    ) -> Result<Self, CleanupError> {
        check_database(&database).map_err(|source| CleanupError::Persistence {
            stage: "worker startup",
            summary: None,
            source,
        })?;
        let claimed = database.cleanup_worker();
        if claimed
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(CleanupError::AlreadyRunning);
        }
        let shared = Arc::new(Shared {
            database,
            admission: Mutex::new(true),
            stopping: AtomicBool::new(false),
        });
        let (sender, receiver) = flume::bounded(CLEANUP_QUEUE_CAPACITY);
        let (stop, stopping) = flume::bounded(1);
        let worker_shared = shared.clone();
        let worker_claimed = claimed.clone();
        let thread = match std::thread::Builder::new()
            .name("ocg-core-log-cleanup".into())
            .spawn(move || {
                let _exit = WorkerExit {
                    shared: worker_shared.clone(),
                    receiver: receiver.clone(),
                    claimed: worker_claimed,
                };
                // A schedule runs once at start, then waits its interval.
                let mut due = schedule.map(|_| Instant::now());
                loop {
                    if worker_shared.stopping.load(Ordering::SeqCst) {
                        break;
                    }
                    let selector = flume::Selector::new()
                        .recv(&receiver, WorkerWake::Request)
                        .recv(&stopping, |_| WorkerWake::Stop);
                    let wake = match due {
                        Some(at) => selector.wait_deadline(at).unwrap_or(WorkerWake::Due),
                        None => selector.wait(),
                    };
                    match wake {
                        WorkerWake::Request(Ok(request)) => {
                            let result = execute(&worker_shared);
                            // Losing an observer never cancels recorded work or triggers a retry.
                            request.reply.send(result).ok();
                        }
                        WorkerWake::Due => {
                            let Some(schedule) = schedule else { break };
                            let again = if run_scheduled(&worker_shared) {
                                Duration::ZERO
                            } else {
                                schedule.interval
                            };
                            due = Instant::now().checked_add(again);
                        }
                        WorkerWake::Request(Err(_)) | WorkerWake::Stop => break,
                    }
                }
            }) {
            Ok(thread) => thread,
            Err(error) => {
                claimed.store(false, Ordering::SeqCst);
                return Err(CleanupError::Spawn(error));
            }
        };
        Ok(Self {
            handle: CoreLogCleanupHandle { shared, sender },
            stop,
            thread: Some(thread),
        })
    }

    pub fn handle(&self) -> CoreLogCleanupHandle {
        self.handle.clone()
    }

    pub fn shutdown(mut self) -> Result<(), CleanupError> {
        self.stop_and_join()
    }

    fn stop_and_join(&mut self) -> Result<(), CleanupError> {
        let mut admission = self
            .handle
            .shared
            .admission
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        self.handle.shared.stopping.store(true, Ordering::SeqCst);
        *admission = false;
        drop(admission);
        self.stop.try_send(()).ok();
        if let Some(thread) = self.thread.take() {
            thread.join().map_err(|_| CleanupError::WorkerPanicked)?;
        }
        Ok(())
    }
}
impl Drop for CoreLogCleanup {
    fn drop(&mut self) {
        if let Err(error) = self.stop_and_join() {
            eprintln!("{error}");
        }
    }
}
struct WorkerExit {
    shared: Arc<Shared>,
    receiver: flume::Receiver<Request>,
    claimed: Arc<AtomicBool>,
}
impl Drop for WorkerExit {
    fn drop(&mut self) {
        *self
            .shared
            .admission
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = false;
        for request in self.receiver.drain() {
            let error = if std::thread::panicking() {
                CleanupError::WorkerStopped
            } else {
                CleanupError::ShuttingDown
            };
            request.reply.send(Err(error)).ok();
        }
        self.claimed.store(false, Ordering::SeqCst);
    }
}

fn check_database(database: &DatabaseHandle) -> Result<(), DatabaseError> {
    match database.status() {
        DatabaseStatus::Open => Ok(()),
        DatabaseStatus::Protected => match database.disk_protection().lock {
            Some(lock) => Err(DatabaseError::DiskProtected(lock)),
            None => Err(DatabaseError::ShuttingDown),
        },
        _ => Err(DatabaseError::ShuttingDown),
    }
}

fn execute(shared: &Shared) -> Result<CleanupResult, CleanupError> {
    if shared.stopping.load(Ordering::SeqCst) {
        return Err(CleanupError::ShuttingDown);
    }
    check_database(&shared.database).map_err(|source| CleanupError::Persistence {
        stage: "execution admission",
        summary: None,
        source,
    })?;
    let policy = shared.database.active_system_policy();
    let mut summary = CleanupSummary {
        retention_days: policy.logger_config.retention_days,
        max_log_files_per_run: policy
            .filesystem_config
            .cleanup_config
            .max_log_files_per_run,
        inspected_count: 0,
        deleted_count: 0,
        skipped_count: 0,
        failure_count: 0,
        deleted_file_bytes: 0,
        failure_details: Vec::with_capacity(CLEANUP_FAILURE_DETAIL_LIMIT),
        failure_details_truncated: false,
        additional_candidates_remain: None,
        scan_interrupted: false,
        interruption_reason: None,
    };
    let epoch = shared.database.logging_protection_state().1;
    let cutoff = SystemTime::now().checked_sub(Duration::from_secs(
        u64::from(summary.retention_days) * 86400,
    ));
    match cutoff {
        Some(cutoff) => scan(shared, epoch, cutoff, &mut summary),
        None => {
            summary.failure(
                None,
                "retention cutoff",
                io::Error::other("retention cutoff is outside the platform clock range"),
            );
            summary.interrupt("invalid retention cutoff");
        }
    }
    if summary.deleted_count > 0 {
        shared.database.logs_changed();
    }
    let outcome = if summary.failure_count != 0 {
        JobOutcome::Failed
    } else if summary.scan_interrupted {
        JobOutcome::Cancelled
    } else {
        JobOutcome::Completed
    };
    Ok(CleanupResult { outcome, summary })
}

/// One scheduled cleanup. Returns whether to run again at once: only after a
/// run that deleted logs and stopped at its batch limit, so every repeat
/// makes progress.
fn run_scheduled(shared: &Shared) -> bool {
    if !expired_logs_may_exist(shared) {
        return false;
    }
    match execute(shared) {
        Ok(result) => {
            let summary = &result.summary;
            log::info!(
                "log cleanup deleted {} expired runtime log(s), {} failure(s)",
                summary.deleted_count,
                summary.failure_count
            );
            summary.additional_candidates_remain == Some(true) && summary.deleted_count > 0
        }
        // Protection and shutdown leave the files for a later interval.
        Err(error) => {
            log::warn!("scheduled log cleanup did not complete: {error}");
            false
        }
    }
}

/// Whether the log directory may hold a runtime log older than the retention
/// period. Reads metadata only and stops at the first one; anything it cannot
/// read counts as possibly expired, so the cleanup records the failure.
fn expired_logs_may_exist(shared: &Shared) -> bool {
    let retention_days = shared
        .database
        .active_system_policy()
        .logger_config
        .retention_days;
    let Some(cutoff) =
        SystemTime::now().checked_sub(Duration::from_secs(u64::from(retention_days) * 86400))
    else {
        return true;
    };
    let entries = match fs::read_dir(shared.database.log_directory()) {
        Ok(entries) => entries,
        Err(_) => return true,
    };
    for entry in entries {
        let Ok(entry) = entry else { return true };
        if !is_runtime_file_name(&entry.file_name()) {
            continue;
        }
        match fs::symlink_metadata(entry.path()).and_then(|metadata| eligible(&metadata, cutoff)) {
            Ok(false) => {}
            Ok(true) | Err(_) => return true,
        }
    }
    false
}

fn interrupted(shared: &Shared, epoch: u64) -> Option<&'static str> {
    if shared.stopping.load(Ordering::SeqCst) {
        return Some("cleanup shutdown");
    }
    let (protected, current_epoch) = shared.database.logging_protection_state();
    if protected || current_epoch != epoch {
        return Some("disk protection began during execution");
    }
    if shared.database.status() != DatabaseStatus::Open {
        return Some("database shutdown or worker stop");
    }
    None
}

#[cfg(unix)]
fn same_file(first: &Metadata, second: &Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    first.dev() == second.dev() && first.ino() == second.ino()
}

fn eligible(metadata: &Metadata, cutoff: SystemTime) -> io::Result<bool> {
    if !metadata.file_type().is_file() {
        return Ok(false);
    }
    Ok(metadata.modified()? < cutoff)
}

#[cfg(not(unix))]
fn scan(_shared: &Shared, _epoch: u64, _cutoff: SystemTime, summary: &mut CleanupSummary) {
    summary.failure(
        None,
        "scan",
        io::Error::new(
            io::ErrorKind::Unsupported,
            "log cleanup requires supported filesystem identity semantics",
        ),
    );
    summary.interrupt("unsupported filesystem identity semantics");
}

#[cfg(unix)]
fn scan(shared: &Shared, epoch: u64, cutoff: SystemTime, summary: &mut CleanupSummary) {
    let directory = shared.database.log_directory();
    if let Some(reason) = interrupted(shared, epoch) {
        summary.interrupt(reason);
        return;
    }
    let root = match fs::symlink_metadata(directory) {
        Ok(metadata) if metadata.is_dir() => metadata,
        Ok(_) => {
            summary.failure(
                None,
                "open log directory",
                io::Error::other("ocg_logs must be an ordinary directory"),
            );
            summary.interrupt("invalid log directory");
            return;
        }
        Err(error) => {
            summary.failure(None, "open log directory", error);
            summary.interrupt("log directory metadata failed");
            return;
        }
    };
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) => {
            summary.failure(None, "scan log directory", error);
            summary.interrupt("log directory scan failed");
            return;
        }
    };
    let files = shared.database.log_files();
    let mut attempted = 0;
    let mut unknown_eligibility = false;
    let mut remaining = false;
    for entry in entries {
        if let Some(reason) = interrupted(shared, epoch) {
            summary.interrupt(reason);
            return;
        }
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                summary.failure(None, "read directory entry", error);
                summary.interrupt("directory scan interrupted");
                return;
            }
        };
        summary.inspected_count = summary.inspected_count.saturating_add(1);
        if !is_runtime_file_name(&entry.file_name()) {
            summary.skipped_count += 1;
            continue;
        }
        let path = entry.path();
        let metadata = match fs::symlink_metadata(&path)
            .and_then(|metadata| eligible(&metadata, cutoff).map(|eligible| (metadata, eligible)))
        {
            Ok((metadata, true)) => metadata,
            Ok((_, false)) => {
                summary.skipped_count += 1;
                continue;
            }
            Err(error) => {
                summary.failure(Some(&path), "inspect log", error);
                unknown_eligibility = true;
                continue;
            }
        };
        let active = files.lock().unwrap_or_else(PoisonError::into_inner);
        if active
            .iter()
            .any(|(active_path, identity)| *active_path == path || same_file(identity, &metadata))
        {
            summary.skipped_count += 1;
            continue;
        }
        match fs::symlink_metadata(directory) {
            Ok(metadata) if metadata.is_dir() && same_file(&root, &metadata) => {}
            Ok(_) => {
                summary.failure(
                    None,
                    "revalidate directory",
                    io::Error::other("log directory identity changed"),
                );
                summary.interrupt("log directory changed");
                return;
            }
            Err(error) => {
                summary.failure(None, "revalidate directory", error);
                summary.interrupt("log directory metadata failed");
                return;
            }
        }
        let revalidated = match fs::symlink_metadata(&path)
            .and_then(|metadata| eligible(&metadata, cutoff).map(|eligible| (metadata, eligible)))
        {
            Ok((metadata, true)) => metadata,
            Ok((_, false)) => {
                summary.skipped_count += 1;
                continue;
            }
            Err(error) => {
                summary.failure(Some(&path), "revalidate log", error);
                unknown_eligibility = true;
                continue;
            }
        };
        if active.iter().any(|(active_path, identity)| {
            *active_path == path || same_file(identity, &revalidated)
        }) {
            summary.skipped_count += 1;
            continue;
        }
        if attempted == summary.max_log_files_per_run {
            summary.skipped_count += 1;
            summary.additional_candidates_remain = Some(true);
            return;
        }
        attempted += 1;
        if let Some(reason) = interrupted(shared, epoch) {
            summary.skipped_count += 1;
            summary.interrupt(reason);
            return;
        }
        let deleted = fs::remove_file(&path);
        drop(active);
        match deleted {
            Ok(()) => {
                summary.deleted_count += 1;
                summary.deleted_file_bytes =
                    summary.deleted_file_bytes.saturating_add(revalidated.len());
            }
            Err(error) => {
                remaining = true;
                let protection = (error.kind() == io::ErrorKind::StorageFull).then(|| {
                    ProtectionReason::FilesystemWriteFailed {
                        area: ProtectedArea::Logs,
                        path: path.clone(),
                        kind: error.kind(),
                        os_error: error.raw_os_error(),
                        detail: error.to_string(),
                    }
                });
                summary.failure(Some(&path), "delete log", error);
                if let Some(reason) = protection {
                    let active = shared.database.activate_filesystem_protection(reason);
                    summary.interrupt(if active {
                        "log deletion encountered ENOSPC"
                    } else {
                        "protection service unavailable after ENOSPC"
                    });
                    return;
                }
            }
        }
    }
    summary.additional_candidates_remain = if remaining {
        Some(true)
    } else if unknown_eligibility {
        None
    } else {
        Some(false)
    };
}
