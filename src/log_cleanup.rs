use std::fs::{self, Metadata};
use std::future::Future;
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::database::{DatabaseError, DatabaseHandle, DatabaseStatus};
use crate::disk_protection::{ProtectedArea, ProtectionReason};
use crate::logger::is_runtime_file_name;
use crate::persistence::{
    ExecutorId, FinalResult, JobFinalization, JobId, JobStepId, StepFacts, StepSettlement,
};

pub const CLEANUP_QUEUE_CAPACITY: usize = 8;
pub const CLEANUP_FAILURE_DETAIL_LIMIT: usize = 16;
const FAILURE_MESSAGE_BYTES: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CleanupRecords {
    pub job: JobId,
    pub step: JobStepId,
}

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
    pub records: CleanupRecords,
    pub outcome: StepSettlement,
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
        records: Option<CleanupRecords>,
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
    owner: JobId,
    executor: ExecutorId,
    reply: flume::Sender<Result<CleanupResult, CleanupError>>,
}
#[derive(Clone)]
pub struct CoreLogCleanupHandle {
    shared: Arc<Shared>,
    sender: flume::Sender<Request>,
}

impl CoreLogCleanupHandle {
    pub async fn invoke(
        &self,
        owner: JobId,
        core_executor: ExecutorId,
    ) -> Result<CleanupResult, CleanupError> {
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
                records: None,
                summary: None,
                source,
            })?;
            self.sender
                .try_send(Request {
                    owner,
                    executor: core_executor,
                    reply,
                })
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

impl CoreLogCleanup {
    pub fn start(database: DatabaseHandle) -> Result<Self, CleanupError> {
        check_database(&database).map_err(|source| CleanupError::Persistence {
            stage: "worker startup",
            records: None,
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
                loop {
                    if worker_shared.stopping.load(Ordering::SeqCst) {
                        break;
                    }
                    let request = flume::Selector::new()
                        .recv(&receiver, Some)
                        .recv(&stopping, |_| None)
                        .wait();
                    let Some(Ok(request)) = request else {
                        break;
                    };
                    let result = execute(&worker_shared, request.owner, request.executor);
                    // Losing an observer never cancels recorded work or triggers a retry.
                    request.reply.send(result).ok();
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

struct Unpark(std::thread::Thread);
impl Wake for Unpark {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}
fn wait<F: Future>(future: F) -> F::Output {
    // Only the dedicated worker drives database futures; service threads await replies.
    let mut future = std::pin::pin!(future);
    let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
    loop {
        match future.as_mut().poll(&mut Context::from_waker(&waker)) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::park(),
        }
    }
}

fn execute(
    shared: &Shared,
    owner: JobId,
    executor: ExecutorId,
) -> Result<CleanupResult, CleanupError> {
    if shared.stopping.load(Ordering::SeqCst) {
        return Err(CleanupError::ShuttingDown);
    }
    check_database(&shared.database).map_err(|source| CleanupError::Persistence {
        stage: "execution admission",
        records: None,
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
    let records = wait(
        shared.database.begin_core_log_cleanup(
            owner,
            executor,
            StepFacts {
                structured_result: Some(serde_json::Map::from_iter([
                    ("retention_days".into(), summary.retention_days.into()),
                    (
                        "max_log_files_per_run".into(),
                        summary.max_log_files_per_run.into(),
                    ),
                    (
                        "cutoff_unix_micros".into(),
                        cutoff
                            .and_then(|cutoff| cutoff.duration_since(UNIX_EPOCH).ok())
                            .map(|duration| duration.as_micros().to_string())
                            .into(),
                    ),
                ])),
                ..StepFacts::default()
            },
        ),
    )
    .map_err(|source| CleanupError::Persistence {
        stage: "begin",
        records: None,
        summary: None,
        source,
    })?;
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
    let outcome = if summary.failure_count != 0 {
        StepSettlement::Failed
    } else if summary.scan_interrupted {
        StepSettlement::Cancelled
    } else {
        StepSettlement::Completed
    };
    let code = match outcome {
        StepSettlement::Failed => "LOG_CLEANUP_FAILED",
        StepSettlement::Cancelled => "LOG_CLEANUP_INTERRUPTED",
        _ if summary.additional_candidates_remain == Some(true) => "LOG_CLEANUP_BATCH_LIMIT",
        _ => "LOG_CLEANUP_COMPLETED",
    };
    let value = serde_json::json!(summary);
    let facts = StepFacts {
        code: Some(code.into()),
        structured_result: value.as_object().cloned(),
        truncated: summary.failure_details_truncated,
        ..StepFacts::default()
    };
    wait(
        shared
            .database
            .settle_job_step(records.step, outcome, facts),
    )
    .map_err(|source| CleanupError::Persistence {
        stage: "step settlement",
        records: Some(records),
        summary: Some(Box::new(summary.clone())),
        source,
    })?;
    let result = FinalResult {
        summary: Some(value.to_string()),
        code: Some(code.into()),
        goal_reached: Some(
            summary.failure_count == 0
                && !summary.scan_interrupted
                && summary.additional_candidates_remain == Some(false),
        ),
    };
    let finalization = match outcome {
        StepSettlement::Failed => JobFinalization::Failed(result),
        StepSettlement::Cancelled => JobFinalization::Cancelled(result),
        _ => JobFinalization::Completed(result),
    };
    wait(shared.database.finalize_job(records.job, finalization)).map_err(|source| {
        CleanupError::Persistence {
            stage: "job finalization",
            records: Some(records),
            summary: Some(Box::new(summary.clone())),
            source,
        }
    })?;
    Ok(CleanupResult {
        records,
        outcome,
        summary,
    })
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
