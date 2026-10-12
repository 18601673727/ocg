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
    ExecutorId, FinalResult, IntermediateOutputKind, JobId, JobOutcome, NewJobOutput,
};

pub const CLEANUP_QUEUE_CAPACITY: usize = 8;
pub const CLEANUP_FAILURE_DETAIL_LIMIT: usize = 16;
const FAILURE_MESSAGE_BYTES: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CleanupRecords {
    pub job: JobId,
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
    // The cleanup Job publishes the parameters it runs with as its first
    // output, in the transaction that creates it.
    let parameters = serde_json::json!({
        "retention_days": summary.retention_days,
        "max_log_files_per_run": summary.max_log_files_per_run,
        "cutoff_unix_micros": cutoff
            .and_then(|cutoff| cutoff.duration_since(UNIX_EPOCH).ok())
            .map(|duration| duration.as_micros().to_string()),
    });
    let records = wait(shared.database.begin_core_log_cleanup(
        owner,
        executor,
        NewJobOutput {
            kind: IntermediateOutputKind::Progress,
            summary: Some(parameters.to_string()),
            code: None,
            goal_reached: None,
        },
    ))
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
        JobOutcome::Failed
    } else if summary.scan_interrupted {
        JobOutcome::Cancelled
    } else {
        JobOutcome::Completed
    };
    let code = match outcome {
        JobOutcome::Failed => "LOG_CLEANUP_FAILED",
        JobOutcome::Cancelled => "LOG_CLEANUP_INTERRUPTED",
        _ if summary.additional_candidates_remain == Some(true) => "LOG_CLEANUP_BATCH_LIMIT",
        _ => "LOG_CLEANUP_COMPLETED",
    };
    // The summary, `failure_details_truncated` included, is the result.
    let value = serde_json::json!(summary);
    let result = FinalResult {
        summary: Some(value.to_string()),
        code: Some(code.into()),
        goal_reached: Some(
            summary.failure_count == 0
                && !summary.scan_interrupted
                && summary.additional_candidates_remain == Some(false),
        ),
    };
    wait(
        shared
            .database
            .finalize_job(records.job, outcome.finalization(result)),
    )
    .map_err(|source| CleanupError::Persistence {
        stage: "job finalization",
        records: Some(records),
        summary: Some(Box::new(summary.clone())),
        source,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_directory::DataDirectory;
    use crate::database::{DatabaseStatus, DatabaseWorker};
    use crate::logger::{LoggerLevel, RuntimeLogger};
    use crate::persistence::{
        ExecutionMode, ExecutorType, JobInputDraft, NewExecutor, NewJob, OutputKind,
    };
    use crate::system_policy::SystemPolicy;
    use crate::test_support::{data_directory as test_data_directory, wait};
    use std::num::NonZeroU32;

    const SUITE: &str = "ocg-log-cleanup";

    fn data_directory(name: &str) -> (crate::test_support::TestRoot, DataDirectory) {
        test_data_directory(SUITE, name)
    }

    fn policy() -> SystemPolicy {
        let mut policy = SystemPolicy::default();
        policy.filesystem_config.disk_lock_threshold_bytes = 1;
        policy.filesystem_config.disk_resume_threshold_bytes = 2;
        policy.filesystem_config.disk_check_interval_ms = 60_000;
        policy.logger_config.level = LoggerLevel::Trace;
        policy.logger_config.retention_days = 1;
        policy.logger_config.max_file_size_bytes = 96;
        policy.logger_config.flush_interval_ms = 1;
        policy
    }

    #[test]
    fn logger_creates_and_rotates_runtime_files() {
        let (_root, directory) = data_directory("rotation");
        let worker = DatabaseWorker::start_with_policy(&directory, policy()).expect("start database");
        let database = worker.handle();
        let logger = RuntimeLogger::start(database).expect("start runtime logger");
        let handle = logger.handle();

        for index in 0..12 {
            log::Log::log(
                &handle,
                &log::Record::builder()
                    .args(format_args!("rotation record {index} with enough bytes to roll the file"))
                    .level(log::Level::Info)
                    .target("log-cleanup-test")
                    .build(),
            );
        }
        log::Log::flush(&handle);
        let status = logger.shutdown().expect("flush and stop logger");
        let entries = std::fs::read_dir(directory.logs_path())
            .expect("read log directory")
            .collect::<Result<Vec<_>, _>>()
            .expect("read log entries");
        let runtime_files = entries
            .iter()
            .filter(|entry| crate::logger::is_runtime_file_name(&entry.file_name()))
            .count();
        assert!(status.written_records >= 12, "logger status: {status:?}");
        assert!(runtime_files >= 2, "expected rotation, found {runtime_files} runtime file(s)");
        worker.shutdown().expect("shutdown database");
    }

    #[test]
    fn housekeeping_executes_and_publishes_cleanup_result() {
        let (_root, directory) = data_directory("housekeeping");
        let worker = DatabaseWorker::start_with_policy(&directory, policy()).expect("start database");
        let database = worker.handle();
        let executor = wait(database.create_executor(NewExecutor {
            name: "__CORE__".into(),
            executor_type: ExecutorType::System,
            concurrency_limit: NonZeroU32::new(1).expect("nonzero concurrency"),
            serves: Vec::new(),
        }))
        .expect("create core executor");
        let input = wait(database.save_job_input(None, JobInputDraft::default())).expect("save owner input");
        let owner = wait(database.create_job_with_input(
            NewJob {
                name: Some("cleanup owner".into()),
                execution_mode: ExecutionMode::Once,
                eligible_at: None,
                scheduled_at: None,
                next_due_at: None,
                next_iteration_no: None,
                iteration_no: None,
                parent: database.genesis(),
                projects: Vec::new(),
                dependencies: Vec::new(),
                configuration: None,
                assigned_executor: Some(executor),
                previous: None,
                replaces: None,
            },
            input,
        ))
        .expect("create cleanup owner");

        let cleanup = CoreLogCleanup::start(database.clone()).expect("start cleanup worker");
        let result = wait(cleanup.handle().invoke(owner, executor)).expect("run cleanup");
        assert_eq!(result.outcome, JobOutcome::Completed);
        assert_eq!(result.summary.failure_count, 0);
        assert_eq!(result.summary.additional_candidates_remain, Some(false));

        let job = wait(database.job(result.records.job))
            .expect("read cleanup job")
            .expect("cleanup job exists");
        assert!(job.started_at.is_some() && job.completed_at.is_some());
        assert_eq!(job.parent, owner, "the cleanup Job is a child of its owner");
        assert_eq!(job.assigned_executor, Some(executor));
        let outputs = wait(database.job_outputs(job.id, None, 10)).expect("read cleanup outputs");
        let kinds: Vec<_> = outputs.iter().map(|output| output.kind).collect();
        assert_eq!(kinds, [OutputKind::Progress, OutputKind::Result]);
        let parameters: serde_json::Value = serde_json::from_str(
            outputs[0]
                .summary
                .as_deref()
                .expect("parameters are published"),
        )
        .expect("parameters are JSON");
        assert_eq!(parameters["retention_days"], 1);
        assert!(parameters["cutoff_unix_micros"].is_string());
        assert_eq!(outputs[1].code.as_deref(), Some("LOG_CLEANUP_COMPLETED"));
        let published: serde_json::Value = serde_json::from_str(
            outputs[1]
                .summary
                .as_deref()
                .expect("the summary is published"),
        )
        .expect("summary is JSON");
        assert_eq!(published["failure_details_truncated"], false);
        cleanup.shutdown().expect("shutdown cleanup worker");
        assert_eq!(database.status(), DatabaseStatus::Open);
        worker.shutdown().expect("shutdown database");
    }
}
