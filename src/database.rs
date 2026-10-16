use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, PoisonError, RwLock, RwLockWriteGuard};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use redb::Database;

use crate::attachments::{
    AttachmentCleanupReport, AttachmentContent, AttachmentError, AttachmentStore, Reclamation,
    ReclamationError, StoreError,
};
use crate::data_directory::DataDirectory;
use crate::disk_protection::{
    self, DiskProtectionStatus, FilesystemConfig, FilesystemConfigError, ProtectedArea,
    ProtectionCell, ProtectionLock, ProtectionReason, ProtectionWatch, RecoveryError,
    RecoveryReport, ReopenFailure, SharedRequest, SpaceMeasurement, SpaceShortage,
};
use crate::persistence::{
    self, AgentId, AgentRecord, AppendedOutput, AttachmentId, AttachmentOrigin, AttachmentRecord,
    AvailabilityAggregationProgress, AvailabilityBatch, AvailabilityCounts, AvailabilityStatId,
    AvailabilityStatRecord, CleanupKind, ContentDigest, ExecutorId, JobFinalization, JobId,
    JobInputDraft, JobInputId, JobOutputRecord, JobRecord, JobRetry, LlmProviderId,
    LlmProviderRecord, MetricsId, MetricsRecord, NewAgent, NewAttachment, NewAvailabilityStat,
    NewExecutor, NewJob, NewJobOutput, NewLlmProvider, NewMetrics, NewProfile, NewProfileConfig,
    NewProject, NewSubmission, NewToolEvidence, ObservationCursor, ObservationFilter,
    ObservationPage, ObservedModel, ObservedModelFilter, ObservedModelPage, OperationId,
    OperationKind, OperationMarker, Profile, ProfileId, ProfileRecord, ProfileVariant, ProjectId,
    ProjectRecord, PublishedAttachment, ReadSnapshot, RequestNumber, RequestRef,
    RequestSampleRecord, SubmissionKey, SubmissionRecord, Timestamp, ToolCallRef, ToolDecision,
    ToolEvidenceRecord, ToolOutcome, ToolReconciliation, Violation, WrongProfileKind,
};
use crate::storage::{
    self, BatchError, BatchFailure, Commit, CompactFailure, MarkerSubjects, OpenError, OpenedFile,
};
pub use crate::storage::{StorageError, StorageErrorKind};
use crate::system_policy::{
    self, PolicyOperationError, PolicySaveResult, SystemPolicy, SystemPolicyError,
};
use crate::token_usage::{
    TokenAggregationProgress, TokenBatch, TokenEvidenceError, TokenMinuteTotals,
    TokenRequestRecord, TokenSnapshot,
};

pub const COMMAND_QUEUE_CAPACITY: usize = 256;
/// The most queued commands one group commit covers. Commands that queue up
/// while the worker is busy form a group without any waiting, so a lone
/// command is never delayed. A group is also the most commands whose outcome
/// a failed durable commit leaves unknown, and the longest stretch readers
/// can be held back, so it stays small.
const GROUP_LIMIT: usize = 32;
/// Compaction policy. `compact` is uninterruptible and holds off every read
/// and command while it runs, so it is only worth running on a large file
/// with a lot of free space, and not again soon after an attempt. These are
/// deliberately conservative and not tuned to a measured workload.
const COMPACTION_MIN_FILE_BYTES: u64 = 64 * 1024 * 1024;
const COMPACTION_MIN_RECLAIMABLE_BYTES: u64 = 32 * 1024 * 1024;
/// The estimated free space must also be at least `1 / this` of the file.
const COMPACTION_MIN_RECLAIMABLE_DIVISOR: u64 = 4;
const COMPACTION_COOLDOWN: Duration = Duration::from_secs(24 * 60 * 60);
/// While admission is locked, the protected filesystems are checked at
/// least this often, so a lock that only lacked space lifts soon after the
/// space returns, however long the configured interval is.
pub(crate) const PROTECTED_CHECK_INTERVAL: Duration = Duration::from_secs(30);

/// How long to wait for the next full check: the configured interval, but
/// no longer than [`PROTECTED_CHECK_INTERVAL`] while admission is locked.
pub(crate) fn next_check_interval(configured: Duration, locked: bool) -> Duration {
    if locked {
        configured.min(PROTECTED_CHECK_INTERVAL)
    } else {
        configured
    }
}
/// Reads run on this many dedicated threads, separate from the write worker.
pub const READ_WORKERS: usize = 2;
pub const READ_QUEUE_CAPACITY: usize = 64;
/// The most index entries one observation query reads, matching or not.
pub const MAX_OBSERVATION_SCAN: usize = storage::MAX_OBSERVATION_SCAN;
/// The most distinct (Provider, model) pairs one choice query examines.
pub const MAX_OBSERVED_MODEL_SCAN: usize = storage::MAX_OBSERVED_MODEL_SCAN;
/// At most one space check and one recovery are pending at a time; later
/// requests join them.
const CONTROL_QUEUE_CAPACITY: usize = 2;
/// The space a command's transaction is allowed beyond twice its
/// variable-length payload. This is an allowance, not a prediction of redb
/// file growth, which depends on page reuse, B-tree splits and region
/// allocation; real I/O errors are still handled after a passing check.
const TRANSACTION_ALLOWANCE_BYTES: u64 = 1024 * 1024;

mod commands;
mod errors;
mod shared;
#[cfg(test)]
use commands::{ByteCounter, counted_json_bytes, json_bytes};
use commands::{Command, Control, Envelope, Message, Reply, byte_count};
use errors::operation_status;
pub use errors::{
    CommandOutcome, CompactionOutcome, CompactionReport, CompactionSkip, DatabaseError,
    DatabaseStatus, MaintenanceError, OperationStatus, TokenConsumeError, UnknownCause,
};
pub(crate) use shared::AggregationClaim;
#[cfg(test)]
pub(crate) use shared::GroupProbe;
use shared::{
    Admission, Opened, ReadJob, ReadMessage, ReadWorkerExit, ReleaseOnPanic, Shared,
    StagingBarrier, admitted, flush_staged, serve_reads,
};

/// A storage failure that ends this opening of the database.
struct StorageFailure {
    error: StorageError,
    /// The command's commit failed, so whether it committed is unknown.
    outcome_unknown: bool,
}

/// Sends a command's result. It receives the error of the group's durable
/// commit when that failed, and `None` otherwise.
type Answer = Box<dyn FnOnce(Option<&StorageError>) + Send>;

/// A command that ran and has not been answered.
struct Executed {
    operation: OperationId,
    /// Its changes are in a staged commit that is not durable yet.
    staged: bool,
    answer: Answer,
    /// The storage failure that must release this opening of the database.
    failure: Option<StorageFailure>,
}

fn run<T: Send + 'static>(
    database: &Database,
    operation: OperationId,
    commit: Commit,
    kind: OperationKind,
    reply: Reply<T>,
    body: impl FnOnce(&mut storage::Batch) -> Result<(T, MarkerSubjects), BatchError>,
) -> Executed {
    let outcome = storage::run_batch_with(database, operation, kind, commit, body);
    settle(operation, commit, reply, outcome, false)
}

/// Answers a command from its batch outcome and says how it must be waited on.
/// `declined` is set only by a command that deliberately ended without
/// committing and replied `Ok`. It leaves nothing staged for the group's
/// durable commit to cover, so a lost group commit did not lose it: its answer
/// then reports that nothing of it was committed, like a refusal, because the
/// decline may rest on state that the lost commit had written.
fn settle<T: Send + 'static>(
    operation: OperationId,
    commit: Commit,
    reply: Reply<T>,
    outcome: storage::BatchOutcome<T>,
    declined: bool,
) -> Executed {
    let result = outcome.result.map_err(|failure| match failure {
        BatchFailure::Rejected(violation) => DatabaseError::Rejected(violation),
        BatchFailure::NotCommitted(error) => DatabaseError::RolledBack(error),
        BatchFailure::OutcomeUnknown(error) => DatabaseError::OutcomeUnknown {
            operation,
            cause: UnknownCause::Commit(error),
        },
    });
    let outcome_unknown = matches!(result, Err(DatabaseError::OutcomeUnknown { .. }));
    let staged = commit == Commit::Staged && result.is_ok() && !declined;
    let answer: Answer = Box::new(move |lost| {
        let result = match (result, lost) {
            (Ok(_), Some(error)) if declined => Err(DatabaseError::RolledBack(error.clone())),
            (Ok(_), Some(error)) => Err(DatabaseError::OutcomeUnknown {
                operation,
                cause: UnknownCause::Commit(error.clone()),
            }),
            // The refusal may rest on state that a lost commit of the same
            // group had written. Nothing of this command was committed.
            (Err(DatabaseError::Rejected(_)), Some(error)) => {
                Err(DatabaseError::RolledBack(error.clone()))
            }
            (result, _) => result,
        };
        reply.send(result).ok();
    });
    Executed {
        operation,
        staged,
        answer,
        failure: outcome.storage_failure.map(|error| StorageFailure {
            error,
            outcome_unknown,
        }),
    }
}

/// Owns the process's single embedded database. Commands are executed one at
/// a time on a dedicated write thread, in admission order, each in its own
/// write transaction. Reads run on [`READ_WORKERS`] dedicated read threads.
pub struct DatabaseWorker {
    handle: DatabaseHandle,
    thread: JoinHandle<Result<(), DatabaseError>>,
    readers: Vec<JoinHandle<()>>,
}

/// A cloneable endpoint for the database worker.
///
/// Submissions never wait for queue space: when [`COMMAND_QUEUE_CAPACITY`]
/// commands are already pending, they fail with [`DatabaseError::QueueFull`],
/// and when [`READ_QUEUE_CAPACITY`] reads are pending, reads fail with
/// [`DatabaseError::ReadQueueFull`]; nothing is retried automatically.
/// Waiting for a result never blocks the caller's thread and needs no
/// particular async runtime: every redb access and decode happens on a
/// database thread.
#[derive(Clone)]
pub struct DatabaseHandle {
    sender: flume::Sender<Message>,
    read_sender: flume::Sender<ReadMessage>,
    control_sender: flume::Sender<Control>,
    shared: Arc<Shared>,
}

impl DatabaseWorker {
    /// Starts with the default [`FilesystemConfig`].
    pub fn start(data_directory: &DataDirectory) -> Result<Self, DatabaseError> {
        Self::start_with_config(data_directory, FilesystemConfig::default())
    }

    /// Opens the database, checks its storage format, allocates this
    /// opening's operation epoch and checks every protected filesystem
    /// before returning, so a returned worker is ready to accept commands and
    /// reads, or already refuses commands under disk protection.
    pub fn start_with_config(
        data_directory: &DataDirectory,
        config: FilesystemConfig,
    ) -> Result<Self, DatabaseError> {
        config.validate().map_err(DatabaseError::Configuration)?;
        Self::start_with_policy(
            data_directory,
            SystemPolicy {
                filesystem_config: config,
                ..SystemPolicy::default()
            },
        )
    }

    pub fn start_with_policy(
        data_directory: &DataDirectory,
        policy: SystemPolicy,
    ) -> Result<Self, DatabaseError> {
        policy
            .validate(&SystemPolicy::path(data_directory))
            .map_err(DatabaseError::StartupPolicy)?;
        let config = policy.filesystem_config;
        let log_directory = data_directory.logs_path().to_path_buf();
        let attachment_directory = data_directory.attachments_path().to_path_buf();
        let directory = data_directory.database_path().to_path_buf();
        let paths = disk_protection::protected_paths(data_directory);
        let (sender, receiver) = flume::bounded(COMMAND_QUEUE_CAPACITY);
        let (control_sender, control) = flume::bounded(CONTROL_QUEUE_CAPACITY);
        let (ready_sender, ready_receiver) = flume::bounded(1);
        let thread = std::thread::Builder::new()
            .name("ocg-database".to_string())
            .spawn(move || {
                // Every protected filesystem must meet the configured lock
                // threshold before the database file is opened or its epoch
                // is written.
                let shortfalls = space_shortfalls(&paths, config.disk_lock_threshold_bytes);
                if !shortfalls.is_empty() {
                    ready_sender
                        .send(Err(DatabaseError::InsufficientStartupSpace(shortfalls)))
                        .ok();
                    return Ok(());
                }
                let OpenedFile {
                    database,
                    epoch,
                    genesis,
                } = match storage::open(&directory) {
                    Ok(opened) => opened,
                    Err(error) => {
                        ready_sender.send(Err(open_error(error, &directory))).ok();
                        return Ok(());
                    }
                };
                let (abandoned_at_open, next_operation) = match settle_abandoned(&database, epoch) {
                    Ok(settled) => settled,
                    Err(error) => {
                        ready_sender.send(Err(DatabaseError::Open(error))).ok();
                        return Ok(());
                    }
                };
                // Opened only once the database file is known to be of this
                // format and locked by this process, so no other OCG process
                // publishes into the directory while it is cleaned up.
                let attachments = match AttachmentStore::open(&attachment_directory)
                    .and_then(|store| store.remove_staging_files().map(|_| store))
                {
                    Ok(store) => store,
                    Err(error) => {
                        ready_sender
                            .send(Err(DatabaseError::AttachmentStore(error)))
                            .ok();
                        return Ok(());
                    }
                };
                // Finishes reclamations that an earlier opening's cleanups
                // committed and did not complete, before anything can be
                // published. A file that cannot be reclaimed is reported,
                // and does not keep the database from starting.
                let reclaimed_at_open = match storage::unneeded_content(&database) {
                    Ok(unneeded) => attachments.reclaim_unneeded(&unneeded),
                    Err(error) => {
                        ready_sender.send(Err(DatabaseError::Open(error))).ok();
                        return Ok(());
                    }
                };
                let shared = Arc::new(Shared {
                    active_policy: policy,
                    log_directory,
                    log_files: Arc::new(crate::logger::LogFiles::default()),
                    cleanup_worker: Arc::new(AtomicBool::new(false)),
                    admission: RwLock::new(Admission::Open),
                    database: RwLock::new(Some(Opened { database, epoch })),
                    staging: RwLock::new(()),
                    genesis,
                    epoch: AtomicU64::new(epoch),
                    next_operation: AtomicU64::new(next_operation),
                    abandoned_at_open,
                    released: AtomicBool::new(false),
                    live_readers: AtomicUsize::new(0),
                    reads_stopped: RwLock::new(false),
                    protection: ProtectionCell::new(),
                    attachments,
                    reclaimed_at_open,
                    space_check: SharedRequest::new(),
                    recovery: SharedRequest::new(),
                    aggregation_host: AtomicBool::new(false),
                    #[cfg(test)]
                    probe: GroupProbe::default(),
                });
                let _release = ReleaseOnPanic {
                    shared: shared.clone(),
                    receiver: receiver.clone(),
                    control: control.clone(),
                };
                let mut worker = WriteWorker {
                    shared: shared.clone(),
                    receiver,
                    control,
                    directory,
                    paths,
                    config,
                    next_check: None,
                    uncertain: Vec::new(),
                    storage_failure: None,
                    last_compaction: None,
                    shutdown_requested: false,
                };
                worker.check_all_paths();
                if ready_sender.send(Ok(shared)).is_ok() {
                    worker.serve();
                }
                worker.exit()
            })
            .map_err(DatabaseError::Startup)?;

        let shared = match ready_receiver.recv() {
            Ok(Ok(shared)) => shared,
            Ok(Err(error)) => {
                join(thread)?;
                return Err(error);
            }
            Err(flume::RecvError::Disconnected) => {
                join(thread)?;
                return Err(DatabaseError::WorkerPanicked);
            }
        };
        let (read_sender, read_receiver) = flume::bounded(READ_QUEUE_CAPACITY);
        let mut worker = Self {
            handle: DatabaseHandle {
                sender,
                read_sender,
                control_sender,
                shared,
            },
            thread,
            readers: Vec::with_capacity(READ_WORKERS),
        };
        for index in 0..READ_WORKERS {
            let shared = worker.handle.shared.clone();
            shared.live_readers.fetch_add(1, Ordering::SeqCst);
            let exit = ReadWorkerExit {
                shared,
                receiver: read_receiver.clone(),
            };
            match std::thread::Builder::new()
                .name(format!("ocg-database-read-{index}"))
                .spawn(move || serve_reads(&exit.shared, &exit.receiver))
            {
                Ok(reader) => worker.readers.push(reader),
                Err(error) => {
                    worker.shutdown().ok();
                    return Err(DatabaseError::Startup(error));
                }
            }
        }
        Ok(worker)
    }

    pub fn handle(&self) -> DatabaseHandle {
        self.handle.clone()
    }

    /// Closes admission, lets the worker finish every command accepted before
    /// that point, releases the database once executing reads have returned,
    /// answers every queued read, and waits for all database threads to exit.
    /// When this returns, the database file is closed and may be reopened
    /// immediately.
    ///
    /// Commands refused by disk protection meanwhile stay refused, and a
    /// pending space check or recovery is answered as shutting down.
    ///
    /// Returns [`DatabaseError::StorageUnavailable`] when storage had failed
    /// and was not recovered, or [`DatabaseError::WorkerPanicked`] when the
    /// worker panicked; resources are released all the same.
    ///
    /// Blocks the calling thread; call it from the process's lifecycle code,
    /// not from inside an async task.
    pub fn shutdown(self) -> Result<(), DatabaseError> {
        {
            let mut admission = self
                .handle
                .shared
                .admission
                .write()
                .unwrap_or_else(PoisonError::into_inner);
            if matches!(*admission, Admission::Open | Admission::Protected(_)) {
                *admission = Admission::ShuttingDown;
            }
        }
        // The marker queues behind every accepted command, waiting for space
        // if needed. If the worker is already gone, joining reports why.
        self.handle.sender.send(Message::Shutdown).ok();
        let written = join(self.thread);
        // The database is released by now, so reads still queued ahead of the
        // stop markers are answered with the refusal.
        for _ in &self.readers {
            self.handle.read_sender.send(ReadMessage::Stop).ok();
        }
        let mut readers_ok = true;
        for reader in self.readers {
            readers_ok &= reader.join().is_ok();
        }
        written?;
        if readers_ok {
            Ok(())
        } else {
            Err(DatabaseError::WorkerPanicked)
        }
    }
}

fn file_length(path: &std::path::Path) -> Result<u64, MaintenanceError> {
    std::fs::metadata(path)
        .map(|metadata| metadata.len())
        .map_err(|error| {
            MaintenanceError::Storage(StorageError {
                kind: StorageErrorKind::Io(error.kind()),
                os_error: error.raw_os_error(),
                detail: format!("cannot measure {}: {error}", path.display()),
            })
        })
}

fn database_not_open() -> StorageError {
    StorageError {
        kind: StorageErrorKind::Closed,
        os_error: None,
        detail: "the database is not open".to_string(),
    }
}

fn open_error(error: OpenError, directory: &std::path::Path) -> DatabaseError {
    match error {
        OpenError::Storage(error) => DatabaseError::Open(error),
        OpenError::ForeignFile => DatabaseError::ForeignDatabase(storage::database_file(directory)),
        OpenError::UnsupportedFormat { found } => DatabaseError::UnsupportedFormat {
            found,
            supported: storage::FORMAT_VERSION,
        },
    }
}

/// Settles every Job that an earlier opening started as its own local
/// execution and left unsettled, before this opening admits any command; see
/// [`persistence::reconcile_abandoned_job`]. Only one opening of the file can
/// exist at a time, so when a worker starts, every earlier opening is closed
/// and nothing it ran can still settle its Jobs. A reopen during recovery
/// keeps its process's executions running and never comes here.
///
/// Each Job is settled in its own durable transaction, so a failure part-way
/// leaves the rest claimed for the next start, and a settled Job is not
/// touched again. Returns the settled Jobs and the next operation sequence
/// number of this opening.
fn settle_abandoned(database: &Database, epoch: u64) -> Result<(Vec<JobId>, u64), StorageError> {
    let mut settled = Vec::new();
    let mut seq = 1;
    for (job, claimed) in storage::abandoned_local_executions(database, epoch)? {
        let operation = OperationId::new(epoch, seq);
        seq += 1;
        let outcome = storage::run_batch_declinable(
            database,
            operation,
            OperationKind::ReconcileAbandonedJob,
            Commit::Durable,
            |batch| persistence::reconcile_abandoned_job(batch, job, claimed),
        );
        match outcome.result {
            Ok(Some(())) => settled.push(job),
            Ok(None) => {}
            Err(BatchFailure::Rejected(violation)) => {
                return Err(StorageError::corrupted(format!(
                    "{job}, abandoned by opening {claimed}, could not be settled: {violation}"
                )));
            }
            Err(BatchFailure::NotCommitted(error) | BatchFailure::OutcomeUnknown(error)) => {
                return Err(error);
            }
        }
    }
    Ok((settled, seq))
}

fn join(thread: JoinHandle<Result<(), DatabaseError>>) -> Result<(), DatabaseError> {
    thread.join().map_err(|_| DatabaseError::WorkerPanicked)?
}

/// Every protected filesystem that is below `threshold_bytes` or cannot be
/// measured.
fn space_shortfalls(
    paths: &[(ProtectedArea, PathBuf)],
    threshold_bytes: u64,
) -> Vec<ProtectionReason> {
    paths
        .iter()
        .filter_map(|(area, path)| {
            disk_protection::check_space(*area, path, 0, threshold_bytes).err()
        })
        .collect()
}

fn reopen_failure(error: OpenError, directory: &std::path::Path) -> ReopenFailure {
    match error {
        OpenError::Storage(error) => ReopenFailure::Storage(error),
        OpenError::ForeignFile => ReopenFailure::ForeignDatabase(storage::database_file(directory)),
        OpenError::UnsupportedFormat { found } => ReopenFailure::UnsupportedFormat {
            found,
            supported: storage::FORMAT_VERSION,
        },
    }
}

/// The write thread. It alone executes commands, measures space, changes
/// the protection lock, and releases or reopens the database, so every
/// protection transition happens between commands, never inside one.
struct WriteWorker {
    shared: Arc<Shared>,
    receiver: flume::Receiver<Message>,
    control: flume::Receiver<Control>,
    /// The database directory, which is also the path checked before each
    /// command.
    directory: PathBuf,
    paths: [(ProtectedArea, PathBuf); 3],
    config: FilesystemConfig,
    /// `None` when the interval does not fit in an `Instant`.
    next_check: Option<Instant>,
    /// Operations whose commit failed with an unknown outcome; the next
    /// successful recovery reconciles them.
    uncertain: Vec<OperationId>,
    /// Set from a storage failure until a reopen replaces the failed handle.
    storage_failure: Option<StorageError>,
    /// When the last successful compaction started, including one that moved
    /// no pages. Failed attempts leave it unchanged.
    last_compaction: Option<Instant>,
    shutdown_requested: bool,
}

enum SpaceRefusal {
    Short(SpaceShortage),
    Locked(ProtectionLock),
}

enum Event {
    Control(Result<Control, flume::RecvError>),
    Message(Result<Message, flume::RecvError>),
    CheckDue,
}

impl WriteWorker {
    /// Runs control requests, due checks and commands until shutdown. A due
    /// check runs before the next command, so steady traffic cannot
    /// postpone it by more than one command.
    fn serve(&mut self) {
        while !self.shutdown_requested {
            if self.next_check.is_some_and(|due| Instant::now() >= due) {
                self.check_all_paths();
                continue;
            }
            if let Ok(control) = self.control.try_recv() {
                self.handle(control);
                continue;
            }
            let selector = flume::Selector::new()
                .recv(&self.control, Event::Control)
                .recv(&self.receiver, Event::Message);
            let event = match self.next_check {
                Some(due) => selector.wait_deadline(due).unwrap_or(Event::CheckDue),
                None => selector.wait(),
            };
            match event {
                Event::Control(Ok(control)) => self.handle(control),
                Event::Message(Ok(Message::Run(envelope))) => self.execute_group(*envelope),
                Event::CheckDue => {}
                // Without senders, no handle and therefore no caller remains.
                Event::Message(Ok(Message::Shutdown))
                | Event::Message(Err(_))
                | Event::Control(Err(_)) => return,
            }
        }
    }

    fn handle(&mut self, control: Control) {
        match control {
            Control::FilesystemFailure { reason, reply } => {
                let active = matches!(
                    self.shared.admission(),
                    Admission::Open | Admission::Protected(_)
                );
                if active {
                    self.activate(vec![reason]);
                }
                reply.send(active).ok();
            }
            Control::CheckSpace => {
                self.check_all_paths();
                self.shared
                    .space_check
                    .complete(Some(self.shared.protection.status()));
            }
            Control::Recover => {
                let result = self.recover();
                self.shared.recovery.complete(result);
            }
            Control::CompactStorage(reply) => {
                let result = self.compact_storage();
                reply.send(result).ok();
            }
            Control::ReadPolicy(reply) => {
                let result = match self.shared.admission() {
                    Admission::ShuttingDown => Err(PolicyOperationError::ShuttingDown),
                    Admission::WorkerStopped => Err(PolicyOperationError::WorkerStopped),
                    Admission::Open | Admission::Protected(_) => {
                        SystemPolicy::read_saved(&self.policy_path())
                            .map_err(PolicyOperationError::Read)
                    }
                };
                reply.send(result).ok();
            }
            Control::SavePolicy {
                expected,
                candidate,
                started,
                reply,
            } => {
                let result = self.save_policy(expected, candidate, &started);
                reply.send(result).ok();
            }
        }
    }

    fn policy_path(&self) -> PathBuf {
        self.directory.join(system_policy::SYSTEM_POLICY_FILE_NAME)
    }

    fn policy_save_admission(&self) -> Result<(), PolicyOperationError> {
        match self.shared.admission() {
            Admission::Open => Ok(()),
            Admission::Protected(lock) => Err(PolicyOperationError::DiskProtected(lock)),
            Admission::ShuttingDown => Err(PolicyOperationError::ShuttingDown),
            Admission::WorkerStopped => Err(PolicyOperationError::WorkerStopped),
        }
    }

    fn save_policy(
        &mut self,
        expected: SystemPolicy,
        candidate: SystemPolicy,
        started: &AtomicBool,
    ) -> Result<PolicySaveResult, PolicyOperationError> {
        self.policy_save_admission()?;
        let path = self.policy_path();
        let current = SystemPolicy::read_saved(&path).map_err(PolicyOperationError::NotSaved)?;
        if current != expected {
            return Err(PolicyOperationError::Conflict {
                expected: Box::new(expected),
                actual: Box::new(current),
            });
        }
        candidate
            .validate(&path)
            .map_err(PolicyOperationError::NotSaved)?;
        self.policy_save_admission()?;
        if current == candidate {
            return Ok(PolicySaveResult::Unchanged);
        }
        let result = self.replace_policy(&path, candidate, started);
        if let Err(
            PolicyOperationError::NotSaved(SystemPolicyError::Io { source, .. })
            | PolicyOperationError::OutcomeUncertain(SystemPolicyError::Io { source, .. }),
        ) = &result
            && source.kind() == std::io::ErrorKind::StorageFull
        {
            self.activate(vec![ProtectionReason::FilesystemWriteFailed {
                area: ProtectedArea::Database,
                path,
                kind: source.kind(),
                os_error: source.raw_os_error(),
                detail: source.to_string(),
            }]);
        }
        result
    }

    fn replace_policy(
        &mut self,
        path: &std::path::Path,
        candidate: SystemPolicy,
        started: &AtomicBool,
    ) -> Result<PolicySaveResult, PolicyOperationError> {
        let fail = |operation: String, source| {
            PolicyOperationError::NotSaved(system_policy::io_error(path, operation, source))
        };
        let directory_file = system_policy::open_directory(&self.directory).map_err(|source| {
            fail(
                "open policy directory for durable replacement".into(),
                source,
            )
        })?;
        let mut bytes = serde_json::to_vec_pretty(&candidate).map_err(|source| {
            PolicyOperationError::NotSaved(SystemPolicyError::Json {
                path: path.to_path_buf(),
                source,
            })
        })?;
        bytes.push(b'\n');
        let expected_write_bytes =
            byte_count(bytes.len()).saturating_add(TRANSACTION_ALLOWANCE_BYTES);
        match disk_protection::check_space(
            ProtectedArea::Database,
            &self.directory,
            expected_write_bytes,
            self.config.disk_lock_threshold_bytes,
        ) {
            Ok(_) => {}
            Err(ProtectionReason::InsufficientSpace(shortage))
                if shortage.available_bytes >= shortage.threshold_bytes =>
            {
                return Err(PolicyOperationError::InsufficientSpace(shortage));
            }
            Err(reason) => {
                return Err(PolicyOperationError::DiskProtected(
                    self.activate(vec![reason]),
                ));
            }
        }
        self.policy_save_admission()?;
        static NEXT_TEMPORARY: AtomicU64 = AtomicU64::new(0);
        let temporary = self.directory.join(format!(
            ".system_policy-edit-{}-{}.tmp",
            std::process::id(),
            NEXT_TEMPORARY.fetch_add(1, Ordering::Relaxed)
        ));
        let mut file = crate::data_directory::private_file(&mut std::fs::OpenOptions::new())
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|source| {
                fail(
                    format!("create temporary file {}", temporary.display()),
                    source,
                )
            })?;
        let result = (|| {
            file.write_all(&bytes)
                .and_then(|()| file.flush())
                .and_then(|()| file.sync_all())
                .map_err(|source| {
                    fail(
                        format!("write and persist temporary file {}", temporary.display()),
                        source,
                    )
                })?;
            self.policy_save_admission()?;
            started.store(true, Ordering::SeqCst);
            std::fs::rename(&temporary, path).map_err(|source| {
                fail(
                    format!("replace policy from {}", temporary.display()),
                    source,
                )
            })?;
            if let Some(directory_file) = directory_file {
                directory_file.sync_all().map_err(|source| {
                    PolicyOperationError::OutcomeUncertain(system_policy::io_error(
                        path,
                        "confirm replacement directory durability",
                        source,
                    ))
                })?;
            }
            Ok(PolicySaveResult::SavedForRestart)
        })();
        drop(file);
        if result.is_err()
            && let Err(source) = std::fs::remove_file(&temporary)
            && source.kind() != std::io::ErrorKind::NotFound
        {
            eprintln!(
                "{}",
                system_policy::io_error(
                    path,
                    format!("clean up temporary file {}", temporary.display()),
                    source
                )
            );
        }
        result
    }

    /// Refuses a command whose allowance the database filesystem cannot
    /// give, or any command while protection is locked. A refused command is
    /// answered here, before it starts, and `None` is returned.
    fn admit(&mut self, envelope: Envelope) -> Option<Envelope> {
        let operation = envelope.operation;
        // Activation drains the queue, so no command should arrive while
        // locked; refusing here keeps that true even if one did.
        if let Some(lock) = self.shared.protection.status().lock {
            envelope
                .command
                .refuse(|| DatabaseError::RefusedByDiskProtection {
                    operation,
                    lock: lock.clone(),
                });
            return None;
        }
        match self.check_database_space(envelope.command.expected_write_bytes()) {
            Ok(()) => Some(envelope),
            Err(SpaceRefusal::Short(shortage)) => {
                envelope
                    .command
                    .refuse(|| DatabaseError::InsufficientSpace {
                        operation,
                        shortage: shortage.clone(),
                    });
                None
            }
            Err(SpaceRefusal::Locked(lock)) => {
                envelope
                    .command
                    .refuse(|| DatabaseError::RefusedByDiskProtection {
                        operation,
                        lock: lock.clone(),
                    });
                None
            }
        }
    }

    /// Checks the database filesystem for `expected_write_bytes`. Space
    /// already below the lock threshold, or a failed measurement, locks; an
    /// allowance that alone would cross the threshold refuses only this write.
    fn check_database_space(&mut self, expected_write_bytes: u64) -> Result<(), SpaceRefusal> {
        match disk_protection::check_space(
            ProtectedArea::Database,
            &self.directory,
            expected_write_bytes,
            self.config.disk_lock_threshold_bytes,
        ) {
            Ok(_) => Ok(()),
            Err(ProtectionReason::InsufficientSpace(shortage))
                if shortage.available_bytes >= shortage.threshold_bytes =>
            {
                Err(SpaceRefusal::Short(shortage))
            }
            Err(reason) => Err(SpaceRefusal::Locked(self.activate(vec![reason]))),
        }
    }

    /// Runs `first` and the commands queued behind it, up to [`GROUP_LIMIT`],
    /// in queue order, and answers each of them.
    ///
    /// A lone command commits durably, as it always did. A group runs each
    /// command in its own write transaction with staged durability, so a
    /// refused or failed command rolls back only itself, then makes all of
    /// them durable with one empty durable transaction. The separate
    /// transactions are not one atomic batch: a failure of the durable commit
    /// leaves every staged command's outcome unknown. A command that declined
    /// to commit staged nothing, so it neither needs that durable commit nor
    /// stays unknown when it fails.
    ///
    /// No command is answered before that durable commit, so no caller acts
    /// on a change that can still be lost, and readers are held back by
    /// [`Shared::staging`] meanwhile. A storage failure stops the group; the
    /// commands not yet run are refused like queued ones, and the staged
    /// commands are still made durable if the database allows it.
    fn execute_group(&mut self, first: Envelope) {
        #[cfg(test)]
        self.shared.probe.hold_if_armed();
        let mut group = vec![first];
        while group.len() < GROUP_LIMIT {
            match self.receiver.try_recv() {
                Ok(Message::Run(envelope)) => group.push(*envelope),
                Ok(Message::Shutdown) => {
                    self.shutdown_requested = true;
                    break;
                }
                Err(_) => break,
            }
        }
        let commit = if group.len() > 1 {
            Commit::Staged
        } else {
            Commit::Durable
        };
        let shared = self.shared.clone();
        let staging = (commit == Commit::Staged).then(|| StagingBarrier::raise(&shared));

        let mut executed = Vec::with_capacity(group.len());
        let mut unrun = Vec::new();
        let mut failure: Option<StorageFailure> = None;
        let mut lost: Option<StorageError> = None;
        {
            let guard = shared
                .database
                .read()
                .unwrap_or_else(PoisonError::into_inner);
            for envelope in group {
                if failure.is_some() {
                    unrun.push(envelope);
                    continue;
                }
                let Some(envelope) = self.admit(envelope) else {
                    continue;
                };
                let operation = envelope.operation;
                let Some(opened) = guard.as_ref() else {
                    envelope
                        .command
                        .refuse(|| DatabaseError::NeverExecuted { operation });
                    continue;
                };
                envelope.started.store(true, Ordering::SeqCst);
                let mut ran = envelope
                    .command
                    .execute(&opened.database, operation, commit);
                failure = ran.failure.take();
                executed.push((
                    ran,
                    failure
                        .as_ref()
                        .is_some_and(|failure| failure.outcome_unknown),
                ));
            }
            if executed.iter().any(|(ran, _)| ran.staged)
                && let Some(opened) = guard.as_ref()
            {
                lost = flush_staged(&shared, &opened.database).err();
            }
        }
        if let Some(error) = &lost {
            for (ran, _) in &executed {
                if ran.staged {
                    self.uncertain.push(ran.operation);
                }
            }
            failure.get_or_insert(StorageFailure {
                error: error.clone(),
                outcome_unknown: false,
            });
        }
        for (ran, outcome_unknown) in &executed {
            if *outcome_unknown {
                self.uncertain.push(ran.operation);
            }
        }
        // The database guard is gone and the staging barrier still held, so
        // releasing the database waits for no reader and no reader can see a
        // lost change before it is released.
        let lock = failure.map(|failure| {
            self.storage_failure = Some(failure.error.clone());
            self.activate(vec![ProtectionReason::StorageUnavailable(failure.error)])
        });
        drop(staging);
        for (ran, _) in executed {
            (ran.answer)(lost.as_ref());
        }
        if let Some(lock) = lock {
            for envelope in unrun {
                let operation = envelope.operation;
                envelope
                    .command
                    .refuse(|| DatabaseError::RefusedByDiskProtection {
                        operation,
                        lock: lock.clone(),
                    });
            }
        }
    }

    /// Compacts the file if the policy allows, between command groups, so
    /// nothing is staged and every earlier commit is already durable.
    ///
    /// Taking the database lock for writing waits for every executing read
    /// and holds off new ones, so redb sees no read transaction on the handle
    /// and `compact` gets its exclusive `&mut Database` without any unsafe
    /// code. The worker is the only writer, so no write transaction exists
    /// either. Reads and commands wait, up to their queue limits, for as long
    /// as `compact` runs, which grows with the file; it cannot be interrupted,
    /// and shutdown waits for it. Hence the policy and the cooldown.
    fn compact_storage(&mut self) -> Result<CompactionOutcome, MaintenanceError> {
        match self.shared.admission() {
            Admission::Open => {}
            Admission::Protected(lock) => return Err(MaintenanceError::DiskProtected(lock)),
            Admission::ShuttingDown => return Err(MaintenanceError::ShuttingDown),
            Admission::WorkerStopped => return Err(MaintenanceError::WorkerStopped),
        }
        if let Some(remaining) = self
            .last_compaction
            .and_then(|last| COMPACTION_COOLDOWN.checked_sub(last.elapsed()))
        {
            return Ok(CompactionOutcome::Skipped(CompactionSkip::CoolingDown {
                remaining,
            }));
        }
        let file = storage::database_file(&self.directory);
        let file_bytes = file_length(&file)?;
        if file_bytes < COMPACTION_MIN_FILE_BYTES {
            return Ok(CompactionOutcome::Skipped(CompactionSkip::FileTooSmall {
                file_bytes,
            }));
        }
        let shared = self.shared.clone();
        let allocated = {
            let guard = shared
                .database
                .read()
                .unwrap_or_else(PoisonError::into_inner);
            match guard.as_ref() {
                Some(opened) => storage::allocated_bytes(&opened.database),
                None => Err(database_not_open()),
            }
        };
        let allocated = allocated.map_err(|error| self.fail_storage(error))?;
        let reclaimable_bytes = file_bytes.saturating_sub(allocated);
        if reclaimable_bytes < COMPACTION_MIN_RECLAIMABLE_BYTES
            || reclaimable_bytes < file_bytes / COMPACTION_MIN_RECLAIMABLE_DIVISOR
        {
            return Ok(CompactionOutcome::Skipped(
                CompactionSkip::LittleReclaimable {
                    file_bytes,
                    reclaimable_bytes,
                },
            ));
        }
        match self.check_database_space(TRANSACTION_ALLOWANCE_BYTES) {
            Ok(()) => {}
            Err(SpaceRefusal::Short(shortage)) => {
                return Err(MaintenanceError::InsufficientSpace(shortage));
            }
            Err(SpaceRefusal::Locked(lock)) => return Err(MaintenanceError::DiskProtected(lock)),
        }

        let began = Instant::now();
        // The guard ends before any failure is handled, which takes the same
        // lock to release the database.
        let compacted = {
            let mut guard = shared
                .database
                .write()
                .unwrap_or_else(PoisonError::into_inner);
            match guard.as_mut() {
                Some(opened) => storage::compact(&mut opened.database),
                None => Err(CompactFailure::Storage(database_not_open())),
            }
        };
        let duration = began.elapsed();
        match compacted {
            Ok(moved) => {
                // Only a completed attempt starts the cooldown. A refused or
                // failed one leaves it unset.
                self.last_compaction = Some(began);
                let report = CompactionReport {
                    file_bytes_before: file_bytes,
                    file_bytes_after: file_length(&file)?,
                    duration,
                };
                Ok(if moved {
                    CompactionOutcome::Compacted(report)
                } else {
                    CompactionOutcome::NoProgress(report)
                })
            }
            Err(CompactFailure::TransactionInProgress) => {
                Err(MaintenanceError::TransactionInProgress)
            }
            Err(CompactFailure::SavepointExists) => Err(MaintenanceError::SavepointExists),
            Err(CompactFailure::Storage(error)) => Err(self.fail_storage(error)),
        }
    }

    /// Ends this opening of the database for a storage failure that no
    /// command observed, so it is released and awaits recovery like any other.
    fn fail_storage(&mut self, error: StorageError) -> MaintenanceError {
        self.storage_failure = Some(error.clone());
        self.activate(vec![ProtectionReason::StorageUnavailable(error.clone())]);
        MaintenanceError::Storage(error)
    }

    /// Closes command admission, refuses every queued command, and releases
    /// the database when storage failed. Runs between commands, so the one
    /// that was executing has already answered with its own result.
    fn activate(&mut self, reasons: Vec<ProtectionReason>) -> ProtectionLock {
        let storage_failed = reasons
            .iter()
            .any(|reason| matches!(reason, ProtectionReason::StorageUnavailable(_)));
        let lock = {
            let mut admission = self
                .shared
                .admission
                .write()
                .unwrap_or_else(PoisonError::into_inner);
            let lock = self.shared.protection.lock(reasons);
            if matches!(*admission, Admission::Open | Admission::Protected(_)) {
                *admission = Admission::Protected(lock.clone());
            }
            lock
        };
        for message in self.receiver.drain() {
            match message {
                Message::Run(envelope) => {
                    let envelope = *envelope;
                    let operation = envelope.operation;
                    envelope
                        .command
                        .refuse(|| DatabaseError::RefusedByDiskProtection {
                            operation,
                            lock: lock.clone(),
                        });
                }
                Message::Shutdown => self.shutdown_requested = true,
            }
        }
        if storage_failed {
            self.shared.release_database();
        }
        // A locked database is checked again soon, whatever the interval.
        let soon = Instant::now().checked_add(self.check_interval());
        self.next_check = match (self.next_check, soon) {
            (Some(due), Some(soon)) => Some(due.min(soon)),
            (due, soon) => soon.or(due),
        };
        lock
    }

    fn check_interval(&self) -> Duration {
        next_check_interval(
            self.config.check_interval(),
            self.shared.protection.status().lock.is_some(),
        )
    }

    /// Checks every protected filesystem against the lock threshold and
    /// locks when any is short or cannot be measured. When every one meets
    /// it and the lock only ever lacked space, it attempts the recovery a
    /// caller could request, which lifts the lock only once every
    /// filesystem has the resume threshold available.
    fn check_all_paths(&mut self) {
        let reasons = space_shortfalls(&self.paths, self.config.disk_lock_threshold_bytes);
        if !reasons.is_empty() {
            self.activate(reasons);
        } else if self.only_lacks_space() {
            // A failed attempt keeps the lock; the next check tries again.
            let _ = self.recover();
        }
        self.shared.protection.update(|status| {
            status.completed_checks += 1;
            status.last_checked_at = Some(Timestamp::now());
        });
        self.next_check = Instant::now().checked_add(self.check_interval());
    }

    /// Whether admission is locked for no reason but measured shortages of
    /// space, so lifting it needs nothing but space. A failed measurement, a
    /// failed write, a failed commit or flush, and any operation whose
    /// outcome is unknown all leave the lock to an explicit recovery.
    fn only_lacks_space(&self) -> bool {
        matches!(self.shared.admission(), Admission::Protected(_))
            && self.storage_failure.is_none()
            && self.uncertain.is_empty()
            && !self.shared.released.load(Ordering::SeqCst)
            && self.shared.protection.status().lock.is_some_and(|lock| {
                !lock.reasons.is_empty()
                    && lock
                        .reasons
                        .iter()
                        .all(|reason| matches!(reason, ProtectionReason::InsufficientSpace(_)))
            })
    }

    /// Runs one recovery attempt for every caller that joined it. The lock
    /// stays unless every step succeeds.
    fn recover(&mut self) -> Result<RecoveryReport, RecoveryError> {
        match self.shared.admission() {
            Admission::Open => return Ok(RecoveryReport::default()),
            Admission::Protected(_) => {}
            Admission::ShuttingDown => return Err(RecoveryError::ShuttingDown),
            Admission::WorkerStopped => return Err(RecoveryError::WorkerStopped),
        }
        self.shared.protection.update(|status| {
            status.recovering = true;
            status.recovery_attempts += 1;
        });
        let result = self.attempt_recovery().and_then(|report| {
            let mut admission = self
                .shared
                .admission
                .write()
                .unwrap_or_else(PoisonError::into_inner);
            // Shutdown began during the attempt; it keeps the lock.
            if !matches!(*admission, Admission::Protected(_)) {
                return Err(RecoveryError::ShuttingDown);
            }
            *admission = Admission::Open;
            self.shared.protection.update(|status| {
                status.lock = None;
                status.recovering = false;
            });
            Ok(report)
        });
        if result.is_err() {
            self.shared
                .protection
                .update(|status| status.recovering = false);
        }
        result
    }

    fn attempt_recovery(&mut self) -> Result<RecoveryReport, RecoveryError> {
        let threshold = self.config.disk_resume_threshold_bytes;
        let mut available = Vec::with_capacity(self.paths.len());
        let mut shortages = Vec::new();
        for (area, path) in &self.paths {
            match disk_protection::check_space(*area, path, 0, threshold) {
                Ok(available_bytes) => available.push(SpaceMeasurement {
                    area: *area,
                    path: path.clone(),
                    available_bytes,
                }),
                Err(reason) => shortages.push(reason),
            }
        }
        if !shortages.is_empty() {
            return Err(RecoveryError::Space(shortages));
        }
        let released = self
            .shared
            .database
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .is_none();
        let reopened_epoch = if self.storage_failure.is_some() || released {
            Some(self.reopen()?)
        } else {
            None
        };
        let reconciled = self.reconcile_uncertain()?;
        self.uncertain.clear();
        Ok(RecoveryReport {
            reopened_epoch,
            reconciled,
            available,
        })
    }

    /// Releases the failed handle completely, waiting for executing reads,
    /// then opens the database again under a new epoch. Admission is closed,
    /// so no identifier is allocated while the epoch changes.
    fn reopen(&mut self) -> Result<u64, RecoveryError> {
        self.shared.release_database();
        let OpenedFile {
            database,
            epoch,
            genesis,
        } = storage::open(&self.directory)
            .map_err(|error| RecoveryError::Reopen(reopen_failure(error, &self.directory)))?;
        if genesis != self.shared.genesis {
            return Err(RecoveryError::Reopen(ReopenFailure::Storage(
                StorageError::corrupted(format!(
                    "the reopened database names {genesis} as its Genesis Job, not {}",
                    self.shared.genesis
                )),
            )));
        }
        *self
            .shared
            .database
            .write()
            .unwrap_or_else(PoisonError::into_inner) = Some(Opened { database, epoch });
        self.shared.epoch.store(epoch, Ordering::SeqCst);
        self.shared.next_operation.store(1, Ordering::SeqCst);
        self.shared.released.store(false, Ordering::SeqCst);
        self.storage_failure = None;
        Ok(epoch)
    }

    /// Reads the marker of every operation whose outcome was unknown. A
    /// failed read is a storage failure, so the handle is released again.
    fn reconcile_uncertain(
        &mut self,
    ) -> Result<Vec<(OperationId, OperationStatus)>, RecoveryError> {
        let reconciled = {
            let guard = self
                .shared
                .database
                .read()
                .unwrap_or_else(PoisonError::into_inner);
            match guard.as_ref() {
                Some(opened) => self
                    .uncertain
                    .iter()
                    .map(|&operation| {
                        storage::read_marker(&opened.database, operation)
                            .map(|marker| {
                                (operation, operation_status(marker, operation, opened.epoch))
                            })
                            .map_err(|error| (operation, error))
                    })
                    .collect::<Result<Vec<_>, _>>(),
                None if self.uncertain.is_empty() => Ok(Vec::new()),
                None => {
                    return Err(RecoveryError::Reopen(ReopenFailure::Storage(
                        database_not_open(),
                    )));
                }
            }
        };
        reconciled.map_err(|(operation, error)| {
            self.storage_failure = Some(error.clone());
            self.activate(vec![ProtectionReason::StorageUnavailable(error.clone())]);
            RecoveryError::Reconcile { operation, error }
        })
    }

    /// Closes admission for good, releases the database, and answers every
    /// queued command and pending request, so no caller is left waiting.
    fn exit(self) -> Result<(), DatabaseError> {
        {
            let mut admission = self
                .shared
                .admission
                .write()
                .unwrap_or_else(PoisonError::into_inner);
            *admission = Admission::ShuttingDown;
        }
        self.shared.release_database();
        // Admission closed before the shutdown marker was queued, so this
        // finds nothing unless every handle was dropped without a shutdown.
        for message in self.receiver.drain() {
            if let Message::Run(envelope) = message {
                let envelope = *envelope;
                let operation = envelope.operation;
                envelope
                    .command
                    .refuse(|| DatabaseError::NeverExecuted { operation });
            }
        }
        self.shared.space_check.complete(None);
        self.shared
            .recovery
            .complete(Err(RecoveryError::ShuttingDown));
        for control in self.control.drain() {
            control.refuse_policy(false);
        }
        self.shared.protection.close();
        match self.storage_failure {
            None => Ok(()),
            Some(error) => Err(DatabaseError::StorageUnavailable(error)),
        }
    }
}

impl DatabaseHandle {
    pub(crate) fn log_files(&self) -> Arc<crate::logger::LogFiles> {
        self.shared.log_files.clone()
    }

    pub(crate) fn cleanup_worker(&self) -> Arc<AtomicBool> {
        self.shared.cleanup_worker.clone()
    }

    pub(crate) async fn begin_core_log_cleanup(
        &self,
        owner: JobId,
        executor: ExecutorId,
        started: NewJobOutput,
    ) -> Result<crate::log_cleanup::CleanupRecords, DatabaseError> {
        self.submit(|reply| Command::BeginCoreLogCleanup {
            owner,
            executor,
            started,
            reply,
        })
        .await
    }

    pub(crate) fn log_directory(&self) -> &std::path::Path {
        &self.shared.log_directory
    }

    pub(crate) fn logging_protection_state(&self) -> (bool, u64) {
        self.shared.protection.logging_state()
    }

    pub(crate) fn activate_filesystem_protection(&self, reason: ProtectionReason) -> bool {
        let (reply, response) = flume::bounded(1);
        if self
            .control_sender
            .send(Control::FilesystemFailure { reason, reply })
            .is_err()
        {
            return false;
        }
        response.recv().unwrap_or(false)
    }

    pub fn active_system_policy(&self) -> SystemPolicy {
        self.shared.active_policy
    }

    pub async fn read_saved_system_policy(&self) -> Result<SystemPolicy, PolicyOperationError> {
        let (reply, response) = flume::bounded(1);
        self.request_policy(Control::ReadPolicy(reply), false)?;
        response
            .recv_async()
            .await
            .unwrap_or(Err(PolicyOperationError::WorkerStopped))
    }

    pub async fn save_system_policy(
        &self,
        candidate: SystemPolicy,
        expected_previous: SystemPolicy,
    ) -> Result<PolicySaveResult, PolicyOperationError> {
        let (reply, response) = flume::bounded(1);
        let started = Arc::new(AtomicBool::new(false));
        self.request_policy(
            Control::SavePolicy {
                expected: expected_previous,
                candidate,
                started: started.clone(),
                reply,
            },
            true,
        )?;
        response.recv_async().await.unwrap_or_else(|_| {
            Err(if started.load(Ordering::SeqCst) {
                PolicyOperationError::SaveInterrupted
            } else {
                PolicyOperationError::WorkerStopped
            })
        })
    }

    fn request_policy(&self, control: Control, saving: bool) -> Result<(), PolicyOperationError> {
        let admission = self
            .shared
            .admission
            .read()
            .unwrap_or_else(PoisonError::into_inner);
        match &*admission {
            Admission::Open => {}
            Admission::Protected(lock) if saving => {
                return Err(PolicyOperationError::DiskProtected(lock.clone()));
            }
            Admission::Protected(_) => {}
            Admission::ShuttingDown => return Err(PolicyOperationError::ShuttingDown),
            Admission::WorkerStopped => return Err(PolicyOperationError::WorkerStopped),
        }
        self.control_sender
            .try_send(control)
            .map_err(|error| match error {
                flume::TrySendError::Full(_) => PolicyOperationError::QueueFull,
                flume::TrySendError::Disconnected(_) => PolicyOperationError::WorkerStopped,
            })
    }

    async fn submit<T>(
        &self,
        command: impl FnOnce(Reply<T>) -> Command,
    ) -> Result<T, DatabaseError> {
        let (reply, response) = flume::bounded(1);
        let started = Arc::new(AtomicBool::new(false));
        let operation = {
            let admission = self
                .shared
                .admission
                .read()
                .unwrap_or_else(PoisonError::into_inner);
            admitted(&admission)?;
            let epoch = self.shared.epoch.load(Ordering::SeqCst);
            let operation = OperationId::new(
                epoch,
                self.shared.next_operation.fetch_add(1, Ordering::Relaxed),
            );
            let envelope = Envelope {
                operation,
                started: started.clone(),
                command: command(reply),
            };
            self.sender
                .try_send(Message::Run(Box::new(envelope)))
                .map_err(|error| match error {
                    flume::TrySendError::Full(_) => DatabaseError::QueueFull,
                    flume::TrySendError::Disconnected(_) => DatabaseError::ShuttingDown,
                })?;
            operation
        };
        // Every command that reaches the worker is answered, so a closed
        // reply channel means the worker stopped. Its execution mark tells
        // whether the command began; worker termination alone says nothing
        // about a begun command's commit.
        response.recv_async().await.map_err(|_| {
            if started.load(Ordering::SeqCst) {
                DatabaseError::OutcomeUnknown {
                    operation,
                    cause: UnknownCause::WorkerStopped,
                }
            } else {
                DatabaseError::NeverExecuted { operation }
            }
        })?
    }

    async fn read<T: Send + 'static>(
        &self,
        read: impl FnOnce(&Database) -> Result<T, StorageError> + Send + 'static,
    ) -> Result<T, DatabaseError> {
        self.read_opened(move |opened| read(&opened.database)).await
    }

    /// Reads stay available under disk protection unless storage failed.
    async fn read_opened<T: Send + 'static>(
        &self,
        read: impl FnOnce(&Opened) -> Result<T, StorageError> + Send + 'static,
    ) -> Result<T, DatabaseError> {
        let (reply, response) = flume::bounded(1);
        {
            let admission = self
                .shared
                .admission
                .read()
                .unwrap_or_else(PoisonError::into_inner);
            match &*admission {
                Admission::Open => {}
                Admission::Protected(lock) => {
                    if let Some(error) = lock.storage_failure() {
                        return Err(DatabaseError::ReadsStorageFailed(error.clone()));
                    }
                }
                Admission::ShuttingDown | Admission::WorkerStopped => {
                    return Err(DatabaseError::ReadsClosed);
                }
            }
            let stopped = self
                .shared
                .reads_stopped
                .read()
                .unwrap_or_else(PoisonError::into_inner);
            if *stopped {
                return Err(DatabaseError::ReadPoolStopped);
            }
            let job: ReadJob = Box::new(move |opened| {
                let result = opened.and_then(|opened| read(opened).map_err(DatabaseError::Read));
                reply.send(result).ok();
            });
            self.read_sender
                .try_send(ReadMessage::Run(job))
                .map_err(|error| match error {
                    flume::TrySendError::Full(_) => DatabaseError::ReadQueueFull,
                    flume::TrySendError::Disconnected(_) => DatabaseError::ReadPoolStopped,
                })?;
        }
        response
            .recv_async()
            .await
            .map_err(|_| DatabaseError::ReadInterrupted)?
    }

    /// The Jobs that this worker's start found abandoned by an earlier
    /// opening of the database, a process that died or a worker that was
    /// shut down while they ran, and settled as unknown with their stop
    /// confirmed. Empty when nothing was abandoned.
    pub fn abandoned_at_open(&self) -> &[JobId] {
        &self.shared.abandoned_at_open
    }

    /// The content files this worker's start found that committed cleanups
    /// authorized it to reclaim, those of cleaned-up Attachments whose
    /// content no available Attachment names, and what became of each:
    /// [`Reclamation::Removed`], or why it is still there. Such a file is
    /// left over by a reclamation that failed or was interrupted. Empty
    /// when there was none; a file already gone is not listed.
    pub fn reclaimed_at_open(&self) -> &[(ContentDigest, Result<Reclamation, StoreError>)] {
        &self.shared.reclaimed_at_open
    }

    pub fn status(&self) -> DatabaseStatus {
        match self.shared.admission() {
            Admission::Open => DatabaseStatus::Open,
            Admission::Protected(_) => DatabaseStatus::Protected,
            Admission::WorkerStopped => DatabaseStatus::WorkerStopped,
            Admission::ShuttingDown => {
                if self.shared.released.load(Ordering::SeqCst) {
                    DatabaseStatus::Closed
                } else {
                    DatabaseStatus::ShuttingDown
                }
            }
        }
    }

    /// The operation epoch of the current opening. A recovery that reopens
    /// the database moves it forward.
    pub fn epoch(&self) -> u64 {
        self.shared.epoch.load(Ordering::SeqCst)
    }

    /// The current disk protection status. Never waits for storage.
    pub fn disk_protection(&self) -> DiskProtectionStatus {
        self.shared.protection.status()
    }

    /// Notifies of every later disk protection change until the database
    /// worker stops.
    pub fn watch_disk_protection(&self) -> ProtectionWatch {
        self.shared.protection.watch()
    }

    /// Checks every protected filesystem now, on the write worker, and
    /// returns the status after the check. Concurrent requests share one
    /// check.
    pub async fn check_disk_space(&self) -> Result<DiskProtectionStatus, DatabaseError> {
        let response = self.shared.space_check.join(|| {
            self.request(Control::CheckSpace)
                .map_err(|error| match error {
                    RecoveryError::ControlQueueFull => DatabaseError::ControlQueueFull,
                    _ => DatabaseError::ShuttingDown,
                })
        })?;
        match response.recv_async().await {
            Ok(Some(status)) => Ok(status),
            Ok(None) => Err(DatabaseError::ShuttingDown),
            Err(_) => Err(DatabaseError::WorkerPanicked),
        }
    }

    /// Compacts the database file when the policy says it is worth it: the
    /// file is large, a large share of it is free, and no attempt was made
    /// recently. Otherwise reports why it did not. Nothing calls this
    /// automatically, because redb's compaction cannot be interrupted and
    /// holds off every read and command for as long as it runs; the caller
    /// chooses a quiet moment. Commands and reads that arrive meanwhile wait
    /// in their queues and fail with a full-queue error beyond their limits,
    /// and [`DatabaseWorker::shutdown`] waits for it.
    ///
    /// Every earlier command is already durable and acknowledged by the time
    /// it runs, and a failed compaction leaves the file consistent.
    pub async fn compact_storage(&self) -> Result<CompactionOutcome, MaintenanceError> {
        let (reply, response) = flume::bounded(1);
        {
            let admission = self
                .shared
                .admission
                .read()
                .unwrap_or_else(PoisonError::into_inner);
            match &*admission {
                Admission::Open => {}
                Admission::Protected(lock) => {
                    return Err(MaintenanceError::DiskProtected(lock.clone()));
                }
                Admission::ShuttingDown => return Err(MaintenanceError::ShuttingDown),
                Admission::WorkerStopped => return Err(MaintenanceError::WorkerStopped),
            }
            self.control_sender
                .try_send(Control::CompactStorage(reply))
                .map_err(|error| match error {
                    flume::TrySendError::Full(_) => MaintenanceError::ControlQueueFull,
                    flume::TrySendError::Disconnected(_) => MaintenanceError::WorkerStopped,
                })?;
        }
        response
            .recv_async()
            .await
            .unwrap_or(Err(MaintenanceError::WorkerStopped))
    }

    /// Clears disk protection once every protected filesystem has the resume
    /// threshold available, a failed database has been released and
    /// reopened, and every operation whose outcome was unknown has been
    /// reconciled against its marker. Otherwise the lock stays and the
    /// failed step is returned. Concurrent requests share one attempt.
    ///
    /// Commands refused while locked are not replayed, operations with an
    /// unknown outcome are not retried, and no Job is resumed.
    pub async fn recover_storage(&self) -> Result<RecoveryReport, RecoveryError> {
        let response = self
            .shared
            .recovery
            .join(|| self.request(Control::Recover))?;
        response
            .recv_async()
            .await
            .unwrap_or(Err(RecoveryError::WorkerStopped))
    }

    /// Queues a control request unless the worker is stopping or stopped.
    /// Callers hold the request's lock, and the worker closes admission
    /// before answering pending requests on exit, so none is left unanswered.
    fn request(&self, control: Control) -> Result<(), RecoveryError> {
        let admission = self
            .shared
            .admission
            .read()
            .unwrap_or_else(PoisonError::into_inner);
        match &*admission {
            Admission::Open | Admission::Protected(_) => {}
            Admission::ShuttingDown => return Err(RecoveryError::ShuttingDown),
            Admission::WorkerStopped => return Err(RecoveryError::WorkerStopped),
        }
        self.control_sender
            .try_send(control)
            .map_err(|error| match error {
                flume::TrySendError::Full(_) => RecoveryError::ControlQueueFull,
                flume::TrySendError::Disconnected(_) => RecoveryError::WorkerStopped,
            })
    }

    /// The instance's Genesis Job: the root of every parent chain, and the
    /// parent to name for top-level work.
    pub fn genesis(&self) -> JobId {
        self.shared.genesis
    }

    pub async fn job(&self, job: JobId) -> Result<Option<JobRecord>, DatabaseError> {
        self.read(move |database| storage::read_job(database, job))
            .await
    }

    /// Newest Jobs first, strictly before `before` when given; at most
    /// 1,000 per call.
    pub async fn jobs(
        &self,
        before: Option<JobId>,
        limit: usize,
    ) -> Result<Vec<JobRecord>, DatabaseError> {
        self.read(move |database| storage::read_jobs(database, before, limit))
            .await
    }

    /// A Job's ordinary children in ID order, strictly after `after` when
    /// given; at most 1,000 per call. The Genesis Job is not its own child.
    pub async fn children(
        &self,
        parent: JobId,
        after: Option<JobId>,
        limit: usize,
    ) -> Result<Vec<JobId>, DatabaseError> {
        self.read(move |database| storage::read_children(database, parent, after, limit))
            .await
    }

    /// What a submitted LLM execution child was configured to execute, as
    /// frozen in its input, with the Profiles it was submitted with as
    /// provenance. Later Profile edits do not change it. `None` for a Job
    /// submitted without a configuration.
    pub async fn job_configuration(
        &self,
        job: JobId,
    ) -> Result<Option<persistence::SubmittedConfiguration>, DatabaseError> {
        self.read(move |database| storage::read_job_configuration(database, job))
            .await
    }

    /// The Executors that support the transport of an LLM Profile's Provider
    /// as configured now, oldest first, at most 1,000. This lists candidates
    /// for new work only; it neither ranks nor reserves them, and running on
    /// any of them never changes the Provider, model or parameters.
    pub async fn compatible_executors(
        &self,
        llm_profile: ProfileId,
    ) -> Result<Vec<ExecutorId>, DatabaseError> {
        self.read(move |database| storage::read_compatible_executors(database, llm_profile))
            .await
    }

    /// The Executors that support the transport of the LLM configuration a
    /// submitted Job froze, oldest first, at most 1,000; empty for an
    /// unconfigured Job.
    pub async fn job_compatible_executors(
        &self,
        job: JobId,
    ) -> Result<Vec<ExecutorId>, DatabaseError> {
        self.read(move |database| storage::read_job_compatible_executors(database, job))
            .await
    }

    /// Whether a Profile can be edited now, with the unfinished Jobs that
    /// hold it. `None` for a missing Profile.
    pub async fn profile_editability(
        &self,
        profile: ProfileId,
    ) -> Result<Option<persistence::ProfileEditability>, DatabaseError> {
        self.read(move |database| storage::read_profile_editability(database, profile))
            .await
    }

    /// The Jobs submitted with a Profile in ID order, strictly after `after`
    /// when given; at most 1,000 per call.
    pub async fn profile_jobs(
        &self,
        profile: ProfileId,
        after: Option<JobId>,
        limit: usize,
    ) -> Result<Vec<JobId>, DatabaseError> {
        self.read(move |database| storage::read_profile_jobs(database, profile, after, limit))
            .await
    }

    /// The Jobs submitted with an input, oldest first: the Job that froze it
    /// and every retry that reused it.
    pub async fn input_jobs(&self, input: JobInputId) -> Result<Vec<JobId>, DatabaseError> {
        self.read(move |database| storage::read_input_jobs(database, input))
            .await
    }

    /// The Jobs that explicitly retry `original`, oldest first.
    pub async fn retries(&self, original: JobId) -> Result<Vec<JobId>, DatabaseError> {
        self.read(move |database| storage::read_retries(database, original))
            .await
    }

    /// A Job's outputs in seq order, strictly after `after_seq` when given;
    /// at most 1,000 per call.
    pub async fn job_outputs(
        &self,
        job: JobId,
        after_seq: Option<i64>,
        limit: usize,
    ) -> Result<Vec<JobOutputRecord>, DatabaseError> {
        self.read(move |database| storage::read_outputs(database, job, after_seq, limit))
            .await
    }

    pub async fn latest_output(
        &self,
        job: JobId,
    ) -> Result<Option<JobOutputRecord>, DatabaseError> {
        self.read(move |database| storage::read_latest_output(database, job))
            .await
    }

    pub async fn job_input(
        &self,
        id: JobInputId,
    ) -> Result<Option<persistence::JobInputRecord>, DatabaseError> {
        self.read(move |database| storage::read_job_input(database, id))
            .await
    }

    pub async fn job_input_for_job(
        &self,
        job: JobId,
    ) -> Result<Option<persistence::JobInputRecord>, DatabaseError> {
        self.read(move |database| storage::read_job_input_for_job(database, job))
            .await
    }

    pub async fn executor(
        &self,
        id: ExecutorId,
    ) -> Result<Option<persistence::ExecutorRecord>, DatabaseError> {
        self.read(move |database| storage::read_executor(database, id))
            .await
    }

    /// Executors in reverse ID order, newest first. Strictly before `before` when given;
    /// at most 1,000 per call.
    pub async fn executors(
        &self,
        before: Option<ExecutorId>,
        limit: usize,
    ) -> Result<Vec<persistence::ExecutorRecord>, DatabaseError> {
        self.read(move |database| storage::read_executors(database, before, limit))
            .await
    }

    pub async fn project(&self, id: ProjectId) -> Result<Option<ProjectRecord>, DatabaseError> {
        self.read(move |database| storage::read_project(database, id))
            .await
    }

    /// Projects newest first, strictly before `before` when given; at most
    /// 1,000 per call.
    pub async fn projects(
        &self,
        before: Option<ProjectId>,
        limit: usize,
    ) -> Result<Vec<ProjectRecord>, DatabaseError> {
        self.read(move |database| storage::read_projects(database, before, limit))
            .await
    }

    /// The Jobs associated with a Project in ID order, strictly after
    /// `after` when given; at most 1,000 per call.
    pub async fn project_jobs(
        &self,
        project: ProjectId,
        after: Option<JobId>,
        limit: usize,
    ) -> Result<Vec<JobId>, DatabaseError> {
        self.read(move |database| storage::read_project_jobs(database, project, after, limit))
            .await
    }

    pub async fn llm_provider(
        &self,
        id: LlmProviderId,
    ) -> Result<Option<LlmProviderRecord>, DatabaseError> {
        self.read(move |database| storage::read_llm_provider(database, id))
            .await
    }

    /// LLM Providers newest first, strictly before `before` when given; at
    /// most 1,000 per call.
    pub async fn llm_providers(
        &self,
        before: Option<LlmProviderId>,
        limit: usize,
    ) -> Result<Vec<LlmProviderRecord>, DatabaseError> {
        self.read(move |database| storage::read_llm_providers(database, before, limit))
            .await
    }

    /// The LLM Profiles that use a Provider, oldest first.
    pub async fn provider_profiles(
        &self,
        provider: LlmProviderId,
    ) -> Result<Vec<ProfileId>, DatabaseError> {
        self.read(move |database| storage::read_provider_profiles(database, provider))
            .await
    }

    /// A Profile of either kind.
    pub async fn profile(&self, id: ProfileId) -> Result<Option<ProfileRecord>, DatabaseError> {
        self.read(move |database| storage::read_profile(database, id))
            .await
    }

    /// Profiles of both kinds newest first, strictly before `before` when
    /// given; at most 1,000 per call.
    pub async fn profiles(
        &self,
        before: Option<ProfileId>,
        limit: usize,
    ) -> Result<Vec<ProfileRecord>, DatabaseError> {
        self.read(move |database| storage::read_profiles(database, before, limit))
            .await
    }

    /// A Profile in its typed form, such as `typed_profile::<AgentConfig>`
    /// for an Agent Profile. A Profile of the other kind is refused with
    /// [`DatabaseError::WrongProfileKind`].
    pub async fn typed_profile<C: ProfileVariant>(
        &self,
        id: ProfileId,
    ) -> Result<Option<Profile<C>>, DatabaseError> {
        let Some(record) = self.profile(id).await? else {
            return Ok(None);
        };
        Profile::try_from(record)
            .map(Some)
            .map_err(DatabaseError::WrongProfileKind)
    }

    pub async fn agent_profile(
        &self,
        id: ProfileId,
    ) -> Result<Option<persistence::AgentProfile>, DatabaseError> {
        self.typed_profile(id).await
    }

    pub async fn llm_profile(
        &self,
        id: ProfileId,
    ) -> Result<Option<persistence::LlmProfile>, DatabaseError> {
        self.typed_profile(id).await
    }

    pub async fn agent(&self, id: AgentId) -> Result<Option<AgentRecord>, DatabaseError> {
        self.read(move |database| storage::read_agent(database, id))
            .await
    }

    /// A Job's Metrics in recording order, strictly after `after` when
    /// given; at most 1,000 per call.
    pub async fn job_metrics(
        &self,
        job: JobId,
        after: Option<MetricsId>,
        limit: usize,
    ) -> Result<Vec<MetricsRecord>, DatabaseError> {
        self.read(move |database| storage::read_job_metrics(database, job, after, limit))
            .await
    }

    /// A Job's AvailabilityStats in recording order, strictly after `after`
    /// when given; at most 1,000 per call.
    pub async fn job_availability(
        &self,
        job: JobId,
        after: Option<AvailabilityStatId>,
        limit: usize,
    ) -> Result<Vec<AvailabilityStatRecord>, DatabaseError> {
        self.read(move |database| storage::read_job_availability(database, job, after, limit))
            .await
    }

    /// One page of the Metrics `filter` selects, in observation order
    /// (`observed_at`, then record ID), strictly after `after` when given.
    /// Pass the returned `next` back as `after` to continue; a page can be
    /// empty while `next` is set. At most `limit` records (clamped to
    /// 1..=1,000) per page, and at most [`MAX_OBSERVATION_SCAN`] index
    /// entries read, matching or not. Each record names the target its Job
    /// froze, not a Profile's current values.
    pub async fn metrics(
        &self,
        filter: ObservationFilter,
        after: Option<ObservationCursor<MetricsId>>,
        limit: usize,
    ) -> Result<ObservationPage<MetricsRecord, MetricsId>, DatabaseError> {
        self.read(move |database| {
            storage::read_metrics(database, &filter, after, limit, MAX_OBSERVATION_SCAN)
        })
        .await
    }

    /// One page of the AvailabilityStats `filter` selects, paged like
    /// [`Self::metrics`]. These are raw observations; no availability
    /// verdict or rate is derived from them here.
    pub async fn availability_stats(
        &self,
        filter: ObservationFilter,
        after: Option<ObservationCursor<AvailabilityStatId>>,
        limit: usize,
    ) -> Result<ObservationPage<AvailabilityStatRecord, AvailabilityStatId>, DatabaseError> {
        self.read(move |database| {
            storage::read_availability_stats(database, &filter, after, limit, MAX_OBSERVATION_SCAN)
        })
        .await
    }

    /// One page of the distinct (Provider, model) pairs that LLM Metrics or
    /// AvailabilityStats observed within `filter`'s time range were recorded
    /// against, as their Jobs froze them, ordered by Provider ID and then by
    /// model name bytes, strictly after `after` when given. Pass the
    /// returned `next` back as `after` to continue; a page can be empty while
    /// `next` is set. At most `limit` choices (clamped to 1..=1,000) and
    /// [`MAX_OBSERVED_MODEL_SCAN`] examined pairs per call.
    ///
    /// Each call reads one snapshot; separate calls do not share one. Records
    /// are never removed, so a pair a page returned never stops being a
    /// choice, and pages never repeat a pair. A pair first recorded, or first
    /// observed within the range, after a later page has passed its position
    /// is not revisited.
    pub async fn observed_models(
        &self,
        filter: ObservedModelFilter,
        after: Option<ObservedModel>,
        limit: usize,
    ) -> Result<ObservedModelPage, DatabaseError> {
        self.read(move |database| {
            storage::read_observed_models(
                database,
                &filter,
                after.as_ref(),
                limit,
                MAX_OBSERVED_MODEL_SCAN,
            )
        })
        .await
    }

    /// Claims the background aggregation loop for one host, or `None` when
    /// another host holds it.
    pub(crate) fn claim_aggregation_host(&self) -> Option<AggregationClaim> {
        self.shared
            .aggregation_host
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .ok()?;
        Some(AggregationClaim {
            shared: self.shared.clone(),
        })
    }

    /// The seams of the write worker's group commit.
    #[cfg(test)]
    pub(crate) fn group_probe(&self) -> &GroupProbe {
        &self.shared.probe
    }

    /// Writes Metrics as a database written before Token eligibility was
    /// checked at ingest could hold them, contradictions included. Its
    /// marker uses epoch 0, which no opening has.
    #[cfg(test)]
    pub(crate) async fn record_preexisting_metrics(
        &self,
        metrics: NewMetrics,
    ) -> Result<MetricsId, DatabaseError> {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        self.with_database(move |database| {
            let operation = OperationId::new(0, NEXT.fetch_add(1, Ordering::SeqCst));
            storage::run_batch(database, operation, OperationKind::RecordMetrics, |batch| {
                persistence::record_preexisting_metrics(batch, metrics)
            })
            .result
            .map_err(|failure| StorageError::corrupted(format!("{failure:?}")))
        })
        .await
    }

    /// Starts a Job locally as a database written before dependencies were
    /// checked at the start could hold it, claimed by the current opening.
    /// Its marker uses epoch 0, which no opening has.
    #[cfg(test)]
    pub(crate) async fn start_preexisting_local_job(
        &self,
        job: JobId,
    ) -> Result<(), DatabaseError> {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let epoch = self.epoch();
        self.with_database(move |database| {
            let operation = OperationId::new(0, NEXT.fetch_add(1, Ordering::SeqCst));
            storage::run_batch(database, operation, OperationKind::StartLocalJob, |batch| {
                persistence::start_preexisting_local_job(batch, job, epoch)
            })
            .result
            .map_err(|failure| StorageError::corrupted(format!("{failure:?}")))
        })
        .await
    }

    /// Runs `f` on a read worker with the open database, for tests that
    /// inject a storage fault into the database the worker is using.
    #[cfg(test)]
    pub(crate) async fn with_database<T: Send + 'static>(
        &self,
        f: impl FnOnce(&Database) -> Result<T, StorageError> + Send + 'static,
    ) -> Result<T, DatabaseError> {
        self.read(f).await
    }

    /// Operation markers in identifier order, strictly after `after` when
    /// given; at most 1,000 per call.
    pub async fn operation_markers(
        &self,
        after: Option<OperationId>,
        limit: usize,
    ) -> Result<Vec<OperationMarker>, DatabaseError> {
        self.read(move |database| storage::read_markers(database, after, limit))
            .await
    }

    /// Resolves a command reported as [`DatabaseError::OutcomeUnknown`]. Call
    /// it after reopening: for an operation of an earlier opening, a missing
    /// marker proves the command did not commit.
    pub async fn reconcile_operation(
        &self,
        operation: OperationId,
    ) -> Result<OperationStatus, DatabaseError> {
        if operation.epoch() > self.epoch() {
            return Err(DatabaseError::UnknownOperation(operation));
        }
        self.read_opened(move |opened| {
            storage::read_marker(&opened.database, operation)
                .map(|marker| operation_status(marker, operation, opened.epoch))
        })
        .await
    }

    pub async fn save_job_input(
        &self,
        existing: Option<JobInputId>,
        draft: JobInputDraft,
    ) -> Result<JobInputId, DatabaseError> {
        self.submit(|reply| Command::SaveJobInput {
            existing,
            draft,
            reply,
        })
        .await
    }

    pub async fn create_job_with_input(
        &self,
        job: NewJob,
        input: JobInputId,
    ) -> Result<JobId, DatabaseError> {
        self.submit(|reply| Command::CreateJobWithInput { job, input, reply })
            .await
    }

    /// Like [`DatabaseHandle::create_job_with_input`], and starts the Job
    /// like [`DatabaseHandle::start_local_job`] in the same transaction, so
    /// the Job exists only with its local execution claimed. Either both
    /// commit or neither does.
    pub async fn create_local_job_with_input(
        &self,
        job: NewJob,
        input: JobInputId,
    ) -> Result<JobId, DatabaseError> {
        self.submit(|reply| Command::CreateLocalJobWithInput {
            job,
            input,
            submission: None,
            reply,
        })
        .await
    }

    /// Like [`DatabaseHandle::create_local_job_with_input`], and records
    /// `submission` with the Job in the same transaction. A key already
    /// recorded refuses the creation with
    /// [`Violation::SubmissionKeyUsed`], which names the Job the key
    /// created; nothing of the refused creation is committed.
    pub async fn create_submitted_local_job(
        &self,
        job: NewJob,
        input: JobInputId,
        submission: NewSubmission,
    ) -> Result<JobId, DatabaseError> {
        self.submit(|reply| Command::CreateLocalJobWithInput {
            job,
            input,
            submission: Some(submission),
            reply,
        })
        .await
    }

    /// The submission recorded under `key`: the Job it created and the
    /// fingerprint of its request. `None` when no committed Job was created
    /// with it. A read never sees a creation whose commit is not durable.
    pub async fn submission(
        &self,
        key: SubmissionKey,
    ) -> Result<Option<SubmissionRecord>, DatabaseError> {
        self.read(move |database| storage::read_submission(database, key))
            .await
    }

    /// Creates a new Job that retries `retry.original`; see [`JobRetry`].
    pub async fn retry_job(&self, retry: JobRetry) -> Result<JobId, DatabaseError> {
        self.submit(|reply| Command::RetryJob { retry, reply })
            .await
    }

    pub async fn start_job(&self, job: JobId) -> Result<(), DatabaseError> {
        self.submit(|reply| Command::StartJob { job, reply }).await
    }

    /// Starts a Job that this process executes itself, claiming its
    /// execution for the current opening of the database. The claim ends
    /// when the Job settles: with a confirmed outcome, or with a confirmed
    /// stop after an unknown one. If this opening closes first, as it does
    /// when the process dies, the next start of a database worker records
    /// the Job's outcome as unknown and its stop as confirmed; see
    /// [`DatabaseHandle::abandoned_at_open`]. Work executed outside this
    /// process must use [`DatabaseHandle::start_job`] instead.
    pub async fn start_local_job(&self, job: JobId) -> Result<(), DatabaseError> {
        self.submit(|reply| Command::StartLocalJob { job, reply })
            .await
    }

    pub async fn assign_job_executor(
        &self,
        job: JobId,
        executor: ExecutorId,
    ) -> Result<(), DatabaseError> {
        self.submit(|reply| Command::AssignJobExecutor {
            job,
            executor,
            reply,
        })
        .await
    }

    pub async fn associate_job_project(
        &self,
        job: JobId,
        project: ProjectId,
    ) -> Result<(), DatabaseError> {
        self.submit(|reply| Command::AssociateJobProject {
            job,
            project,
            reply,
        })
        .await
    }

    pub async fn append_job_output(
        &self,
        job: JobId,
        output: NewJobOutput,
    ) -> Result<AppendedOutput, DatabaseError> {
        self.submit(|reply| Command::AppendJobOutput { job, output, reply })
            .await
    }

    pub async fn finalize_job(
        &self,
        job: JobId,
        finalization: JobFinalization,
    ) -> Result<Option<AppendedOutput>, DatabaseError> {
        self.submit(|reply| Command::FinalizeJob {
            job,
            finalization,
            reply,
        })
        .await
    }

    pub async fn create_executor(
        &self,
        executor: NewExecutor,
    ) -> Result<ExecutorId, DatabaseError> {
        self.submit(|reply| Command::CreateExecutor { executor, reply })
            .await
    }

    pub async fn create_project(&self, project: NewProject) -> Result<ProjectId, DatabaseError> {
        self.submit(|reply| Command::CreateProject { project, reply })
            .await
    }

    pub async fn create_llm_provider(
        &self,
        provider: NewLlmProvider,
    ) -> Result<LlmProviderId, DatabaseError> {
        self.submit(|reply| Command::CreateLlmProvider { provider, reply })
            .await
    }

    pub async fn create_profile(&self, profile: NewProfile) -> Result<ProfileId, DatabaseError> {
        self.submit(|reply| Command::CreateProfile { profile, reply })
            .await
    }

    /// Replaces a Profile's name, payload and defining references, keeping
    /// its identity and kind. Refused with [`Violation::ProfileLocked`] while
    /// unfinished work holds it; see [`Self::profile_editability`].
    pub async fn update_profile(
        &self,
        id: ProfileId,
        profile: NewProfile,
    ) -> Result<(), DatabaseError> {
        self.submit(|reply| Command::UpdateProfile { id, profile, reply })
            .await
    }

    pub async fn create_agent(&self, agent: NewAgent) -> Result<AgentId, DatabaseError> {
        self.submit(|reply| Command::CreateAgent { agent, reply })
            .await
    }

    /// Stores `attachment` as a new user-provided Attachment. Its content is
    /// published durably first, see [`crate::attachments`]; only then is the
    /// record committed. When the commit fails, the published content stays,
    /// named by no Attachment, and a later publication of the same bytes
    /// reuses it. Nothing is published while commands are not admitted.
    ///
    /// The content is pinned from before it is published until the commit's
    /// outcome is known, so no cleanup reclaims it meanwhile; a returned
    /// Attachment always has its content. When the outcome is unknown, the
    /// content stays pinned until the process ends. Publishing waits, on the
    /// caller's thread like the rest of the publication, while a cleanup
    /// reclaims content.
    pub async fn create_user_attachment(
        &self,
        attachment: NewAttachment,
    ) -> Result<AttachmentId, AttachmentError> {
        persistence::check_new_attachment(&attachment).map_err(DatabaseError::Rejected)?;
        admitted(
            &self
                .shared
                .admission
                .read()
                .unwrap_or_else(PoisonError::into_inner),
        )?;
        let pin = self
            .shared
            .attachments
            .publish(&attachment.content)
            .map_err(AttachmentError::NotStored)?;
        let published = PublishedAttachment {
            sha256: pin.digest(),
            byte_length: attachment.content.len() as u64,
            mime: attachment.mime,
            display_name: attachment.display_name,
            origin: AttachmentOrigin::UserProvided,
        };
        let created = self
            .submit(|reply| Command::CreateAttachment {
                attachment: Box::new(published),
                reply,
            })
            .await;
        // Committed, and durable, so every later read sees the record that
        // now protects the content; or certainly not committed.
        let settled = match &created {
            Ok(_) => true,
            Err(error) => matches!(
                error.command_outcome(),
                Some(CommandOutcome::NotAccepted | CommandOutcome::NotCommitted)
            ),
        };
        if settled {
            pin.release();
        }
        Ok(created?)
    }

    pub async fn attachment(
        &self,
        id: AttachmentId,
    ) -> Result<Option<AttachmentRecord>, DatabaseError> {
        self.read(move |database| storage::read_attachment(database, id))
            .await
    }

    /// Attachments in reverse ID order; newest first, cleaned up or not.
    /// Strictly before `before` when given; at most 1,000 per call.
    pub async fn attachments(
        &self,
        before: Option<AttachmentId>,
        limit: usize,
    ) -> Result<Vec<AttachmentRecord>, DatabaseError> {
        self.read(move |database| storage::read_attachments(database, before, limit))
            .await
    }

    /// The inputs that carry an Attachment, oldest first; see
    /// [`DatabaseHandle::input_jobs`] for the Jobs of each.
    pub async fn attachment_inputs(
        &self,
        attachment: AttachmentId,
    ) -> Result<Vec<JobInputId>, DatabaseError> {
        self.read(move |database| storage::read_attachment_inputs(database, attachment))
            .await
    }

    /// An available Attachment with its content, read from the attachment
    /// store and verified against the record's length and SHA-256. Content
    /// that is missing or does not match is refused, never replaced.
    pub async fn attachment_content(
        &self,
        id: AttachmentId,
    ) -> Result<AttachmentContent, AttachmentError> {
        let record = self
            .attachment(id)
            .await?
            .ok_or(AttachmentError::NotFound(id))?;
        if let Some(cleanup) = record.cleanup {
            return Err(AttachmentError::CleanedUp { id, cleanup });
        }
        let bytes = match self
            .shared
            .attachments
            .load(record.sha256, record.byte_length)
        {
            Ok(bytes) => bytes,
            // Reclaimed since the record was read, by a cleanup that
            // committed meanwhile; otherwise the file is truly missing.
            Err(StoreError::Missing) => {
                return Err(match self.attachment(id).await?.and_then(|r| r.cleanup) {
                    Some(cleanup) => AttachmentError::CleanedUp { id, cleanup },
                    None => AttachmentError::Content {
                        id,
                        error: StoreError::Missing,
                    },
                });
            }
            Err(error) => return Err(AttachmentError::Content { id, error }),
        };
        Ok(AttachmentContent { record, bytes })
    }

    /// The operator's manual cleanup of an Attachment: records that it was
    /// cleaned up, [`CleanupKind::Manual`] at the database's time, in one
    /// commit, then reclaims its content file unless another available
    /// Attachment, or a publication in progress, still needs that content;
    /// see [`crate::attachments`]. The Attachment keeps its identity and
    /// every other field, and every input keeps naming it; from the commit
    /// on, its content is refused to every new read and to every Job whose
    /// execution has not yet read it. Bytes already read, or already sent to
    /// a Provider, are not recalled.
    ///
    /// An Attachment already cleaned up, manually or automatically, keeps
    /// its cleanup: nothing is recorded, and only the reclamation of its
    /// content is tried again. The report says which, and what became of
    /// the content file; a failed reclamation does not undo the cleanup.
    ///
    /// # Errors
    ///
    /// [`AttachmentError::NotFound`] for an unknown Attachment, or why the
    /// cleanup was not recorded.
    pub async fn cleanup_attachment(
        &self,
        id: AttachmentId,
    ) -> Result<AttachmentCleanupReport, AttachmentError> {
        let record = self
            .attachment(id)
            .await?
            .ok_or(AttachmentError::NotFound(id))?;
        let (record, recorded) = if record.is_available() {
            let kind = CleanupKind::Manual;
            match self
                .submit(|reply| Command::CleanupAttachment { id, kind, reply })
                .await
            {
                Ok(record) => (record, true),
                // Cleaned up meanwhile by another cleanup, which is kept.
                Err(DatabaseError::Rejected(Violation::AttachmentCleanedUp(_))) => (
                    self.attachment(id)
                        .await?
                        .ok_or(AttachmentError::NotFound(id))?,
                    false,
                ),
                Err(error) => return Err(error.into()),
            }
        } else {
            (record, false)
        };
        let content = self.reclaim(record.sha256, record.byte_length).await;
        Ok(AttachmentCleanupReport {
            record,
            recorded,
            content,
        })
    }

    /// Reclaims the content `digest` on a read worker, so that whether an
    /// available Attachment still names it is read from committed, durable
    /// records while the store holds its pins' lock.
    async fn reclaim(
        &self,
        digest: ContentDigest,
        length: u64,
    ) -> Result<Reclamation, ReclamationError> {
        let shared = self.shared.clone();
        self.read(move |database| {
            shared.attachments.reclaim(digest, length, || {
                storage::content_available(database, digest)
            })
        })
        .await
        .map_err(ReclamationError::Database)?
        .map_err(ReclamationError::Store)
    }

    #[cfg(test)]
    pub(crate) fn attachment_store(&self) -> &AttachmentStore {
        &self.shared.attachments
    }

    /// Records that an unknown-outcome Job's execution has actually stopped,
    /// releasing the Profiles it holds; the outcome stays unknown. Only the
    /// trusted execution path that observed the termination may call this,
    /// so it is not part of the public API.
    pub(crate) async fn confirm_job_stopped(&self, job: JobId) -> Result<(), DatabaseError> {
        self.submit(|reply| Command::ConfirmJobStopped { job, reply })
            .await
    }

    /// Appends measurements for a Job. Their LLM is the one the Job froze.
    pub async fn record_metrics(&self, metrics: NewMetrics) -> Result<MetricsId, DatabaseError> {
        self.submit(|reply| Command::RecordMetrics {
            metrics: Box::new(metrics),
            reply,
        })
        .await
    }

    /// Appends one availability observation of an LLM request a configured
    /// Job sent. Its LLM is the one the Job froze.
    pub async fn record_availability_stat(
        &self,
        stat: NewAvailabilityStat,
    ) -> Result<AvailabilityStatId, DatabaseError> {
        self.submit(|reply| Command::RecordAvailabilityStat {
            stat: Box::new(stat),
            reply,
        })
        .await
    }

    /// Records OCG's refusal or admission of a tool call a Provider
    /// requested, before anything about the call can run. Only the trusted
    /// execution path of the Job, while it executes it, may record it; a
    /// handler is invoked only once its admission is durable, which is when
    /// this returns `Ok`. See [`persistence::record_tool_decision`].
    pub(crate) async fn record_tool_decision(
        &self,
        evidence: NewToolEvidence,
    ) -> Result<(), DatabaseError> {
        self.submit(|reply| Command::RecordToolDecision {
            evidence: Box::new(evidence),
            reply,
        })
        .await
    }

    /// Records, once, what is known of an admitted call's execution. Only
    /// the trusted execution path of the Job, while it executes it, may
    /// record it.
    pub(crate) async fn record_tool_outcome(
        &self,
        call: ToolCallRef,
        outcome: ToolOutcome,
    ) -> Result<(), DatabaseError> {
        self.submit(|reply| Command::RecordToolOutcome {
            call,
            outcome: Box::new(outcome),
            reply,
        })
        .await
    }

    /// Records, once, a verified conclusion about an admitted call whose
    /// outcome is uncertain, once its Job is settled. Only trusted OCG code
    /// that verified the conclusion against the external system may call
    /// it; OCG has no such reconciler yet, so it is not part of the public
    /// API.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "OCG has no trusted reconciler outside its tests yet"
        )
    )]
    pub(crate) async fn reconcile_tool_effect(
        &self,
        call: ToolCallRef,
        reconciliation: ToolReconciliation,
    ) -> Result<(), DatabaseError> {
        self.submit(|reply| Command::ReconcileToolEffect {
            call,
            reconciliation: Box::new(reconciliation),
            reply,
        })
        .await
    }

    /// A Job's tool evidence in call order, after the call named by its
    /// request and index when given; at most 1,000 per call.
    pub async fn tool_evidence(
        &self,
        job: JobId,
        after: Option<(RequestNumber, u32)>,
        limit: usize,
    ) -> Result<Vec<ToolEvidenceRecord>, DatabaseError> {
        self.read(move |database| storage::read_tool_evidence(database, job, after, limit))
            .await
    }

    /// Aggregation progress: the last AvailabilityStat ID consumed and the
    /// wall-clock time of the last batch, as of one read snapshot.
    pub async fn availability_progress(
        &self,
    ) -> Result<AvailabilityAggregationProgress, DatabaseError> {
        self.read(storage::read_availability_progress).await
    }

    /// The sample of one Provider request of a Job, with the progress of the
    /// same snapshot.
    pub async fn request_sample(
        &self,
        request: RequestRef,
    ) -> Result<ReadSnapshot<Option<RequestSampleRecord>>, DatabaseError> {
        self.read(move |database| storage::read_request_sample(database, request))
            .await
    }

    /// The counts of one (Provider, model, minute) bucket, with the progress
    /// of the same snapshot. `minute` is a value of [`minute_of`].
    pub async fn availability_minute(
        &self,
        provider: LlmProviderId,
        model: String,
        minute: i64,
    ) -> Result<ReadSnapshot<Option<AvailabilityCounts>>, DatabaseError> {
        self.read(move |database| {
            storage::read_availability_minute(database, provider, &model, minute)
        })
        .await
    }

    /// Consumes at most `limit` records (and never more than the batch
    /// maximum) of the next committed AvailabilityStat records, in ID order.
    /// One call prepares a batch from one snapshot and commits it only if the
    /// rows it read are unchanged. A stale preparation is prepared again, at
    /// most the attempt bound. It is an internal operation: nothing here
    /// schedules it.
    pub async fn consume_availability(
        &self,
        limit: usize,
    ) -> Result<AvailabilityBatch, DatabaseError> {
        for attempt in 1..=storage::MAX_AGGREGATION_ATTEMPTS {
            let plan = self.prepare_availability(limit).await?;
            let Some(report) = plan.report(attempt) else {
                return Ok(AvailabilityBatch::Idle {
                    consumed_through: plan.consumed_through(),
                });
            };
            if self.commit_availability(plan).await?.is_some() {
                return Ok(AvailabilityBatch::Consumed(report));
            }
        }
        Ok(AvailabilityBatch::Contended {
            attempts: storage::MAX_AGGREGATION_ATTEMPTS,
        })
    }

    /// Prepares the next batch from one read snapshot. Nothing is written.
    pub(crate) async fn prepare_availability(
        &self,
        limit: usize,
    ) -> Result<storage::AvailabilityPlan, DatabaseError> {
        #[allow(unused_mut)]
        let mut plan = self
            .read(move |database| storage::prepare_availability_aggregation(database, limit))
            .await?;
        #[cfg(test)]
        if plan.report(1).is_some()
            && self
                .shared
                .probe
                .fail_next_plan
                .swap(false, Ordering::SeqCst)
        {
            plan.inject_failure = true;
        }
        Ok(plan)
    }

    /// Commits a prepared batch if the rows it read still hold. `None` means
    /// the preparation was stale, and nothing was applied.
    pub(crate) async fn commit_availability(
        &self,
        plan: storage::AvailabilityPlan,
    ) -> Result<Option<Timestamp>, DatabaseError> {
        self.submit(|reply| Command::CommitAvailabilityAggregation {
            plan: Box::new(plan),
            reply,
        })
        .await
    }

    /// Token aggregation progress: the last Metrics ID consumed by the Token
    /// stream, which is independent of the availability stream, and the
    /// wall-clock time of its last batch, as of one read snapshot.
    pub async fn token_progress(&self) -> Result<TokenAggregationProgress, DatabaseError> {
        self.read(storage::read_token_progress).await
    }

    /// The Token state of one Provider request of a Job, with the progress
    /// of the same snapshot. A request with no annotated record has none.
    pub async fn token_request(
        &self,
        request: RequestRef,
    ) -> Result<TokenSnapshot<Option<TokenRequestRecord>>, DatabaseError> {
        self.read(move |database| storage::read_token_request(database, request))
            .await
    }

    /// The (Provider, model) buckets that exist in the `count` minutes from
    /// `from_minute` (a value of [`minute_of`]), in minute order, with the
    /// progress of the same snapshot. `count` is clamped to 1..=1,000 and the
    /// read does one lookup per minute, so its cost never depends on history.
    pub async fn token_minutes(
        &self,
        provider: LlmProviderId,
        model: String,
        from_minute: i64,
        count: usize,
    ) -> Result<TokenSnapshot<Vec<(i64, TokenMinuteTotals)>>, DatabaseError> {
        self.read(move |database| {
            storage::read_token_minute_window(database, provider, &model, from_minute, count)
        })
        .await
    }

    /// Consumes at most `limit` of the next committed Metrics records (never
    /// more than the batch maximum), in ID order, annotated or not. Like
    /// [`Self::consume_availability`] it prepares from one snapshot, commits
    /// only if the rows it read are unchanged, and prepares again at most the
    /// attempt bound. Records that contradict each other refuse the batch
    /// with nothing written. It is an internal operation: nothing here
    /// schedules it.
    pub async fn consume_token_usage(&self, limit: usize) -> Result<TokenBatch, TokenConsumeError> {
        for attempt in 1..=storage::MAX_AGGREGATION_ATTEMPTS {
            let plan = self.prepare_token(limit).await??;
            let Some(report) = plan.report(attempt) else {
                return Ok(TokenBatch::Idle {
                    consumed_through: plan.consumed_through(),
                });
            };
            if self.commit_token(plan).await?.is_some() {
                return Ok(TokenBatch::Consumed(report));
            }
        }
        Ok(TokenBatch::Contended {
            attempts: storage::MAX_AGGREGATION_ATTEMPTS,
        })
    }

    /// Prepares the next Token batch from one read snapshot. Nothing is written.
    pub(crate) async fn prepare_token(
        &self,
        limit: usize,
    ) -> Result<Result<storage::TokenPlan, TokenConsumeError>, DatabaseError> {
        let prepared = self
            .read(move |database| storage::prepare_token_aggregation(database, limit))
            .await?;
        #[allow(unused_mut)]
        let mut prepared = prepared.map_err(|refusal| TokenConsumeError::Evidence {
            metrics: refusal.metrics,
            job: refusal.job,
            error: refusal.error,
        });
        #[cfg(test)]
        if let Ok(plan) = &mut prepared
            && plan.report(1).is_some()
            && self
                .shared
                .probe
                .fail_next_plan
                .swap(false, Ordering::SeqCst)
        {
            plan.inject_failure = true;
        }
        Ok(prepared)
    }

    /// Commits a prepared Token batch if the rows it read still hold. `None`
    /// means the preparation was stale, and nothing was applied.
    pub(crate) async fn commit_token(
        &self,
        plan: storage::TokenPlan,
    ) -> Result<Option<Timestamp>, DatabaseError> {
        self.submit(|reply| Command::CommitTokenAggregation {
            plan: Box::new(plan),
            reply,
        })
        .await
    }
}

#[cfg(test)]
mod read_pool_tests {
    //! Read-pool failure and shutdown. Faults are injected by submitting
    //! test-only read closures that block on a channel and then panic, which
    //! makes a read worker exit; every wait is bounded.

    use std::future::Future;
    use std::pin::Pin;
    use std::time::Instant;

    use super::*;
    use crate::test_support::{WAIT, data_directory as test_data_directory, poll_once, wait};

    const SUITE: &str = "ocg-read-pool";
    const QUEUED: usize = 5;

    type PendingRead<'a, T> = Pin<Box<dyn Future<Output = Result<T, DatabaseError>> + 'a>>;

    fn data_directory(name: &str) -> (crate::test_support::TestRoot, DataDirectory) {
        test_data_directory(SUITE, name)
    }

    /// Occupies every read worker with a read that reports it has started,
    /// waits for release and then panics, so the worker exits.
    struct HeldWorkers {
        release: flume::Sender<()>,
    }

    fn hold_every_read_worker<'a>(
        handle: &'a DatabaseHandle,
    ) -> (HeldWorkers, Vec<PendingRead<'a, ()>>) {
        let (entered_sender, entered) = flume::unbounded();
        let (release, released) = flume::unbounded::<()>();
        let mut reads: Vec<PendingRead<'a, ()>> = Vec::new();
        for _ in 0..READ_WORKERS {
            let (entered_sender, released) = (entered_sender.clone(), released.clone());
            let mut read: PendingRead<'a, ()> = Box::pin(handle.read(move |_| {
                entered_sender.send(()).expect("test observer");
                released.recv_timeout(WAIT).expect("test release");
                panic!("test: read worker exits unexpectedly");
            }));
            assert!(
                poll_once(read.as_mut()).is_none(),
                "a held read must be admitted, not answered"
            );
            reads.push(read);
        }
        for _ in 0..READ_WORKERS {
            entered
                .recv_timeout(WAIT)
                .expect("every read worker picked up a held read");
        }
        (HeldWorkers { release }, reads)
    }

    impl HeldWorkers {
        fn release(&self) {
            for _ in 0..READ_WORKERS {
                self.release.send(()).expect("held reads are waiting");
            }
        }
    }

    fn queue_reads(handle: &DatabaseHandle) -> Vec<PendingRead<'_, Vec<JobRecord>>> {
        (0..QUEUED)
            .map(|_| {
                let mut read: PendingRead<'_, Vec<JobRecord>> = Box::pin(handle.jobs(None, 1));
                assert!(
                    poll_once(read.as_mut()).is_none(),
                    "a queued read must be admitted, not answered"
                );
                read
            })
            .collect()
    }

    fn pool_stopped(result: &Result<impl std::fmt::Debug, DatabaseError>) -> bool {
        matches!(result, Err(DatabaseError::ReadPoolStopped))
    }

    fn reopen_and_read(directory: &DataDirectory) {
        let worker = DatabaseWorker::start(directory).expect("immediate reopen");
        let handle = worker.handle();
        assert!(
            wait(handle.jobs(None, 1)).is_ok(),
            "reads work after reopening"
        );
        worker.shutdown().expect("clean shutdown after reopening");
    }

    #[test]
    fn both_read_workers_exiting_resolve_every_queued_read() {
        let (_root, directory) = data_directory("queued");
        let worker = DatabaseWorker::start(&directory).unwrap();
        let handle = worker.handle();
        let (held, executing) = hold_every_read_worker(&handle);
        let queued = queue_reads(&handle);

        held.release();
        for read in executing {
            assert!(
                matches!(wait(read), Err(DatabaseError::ReadInterrupted)),
                "an executing read stays ReadInterrupted"
            );
        }
        for read in queued {
            let result = wait(read);
            assert!(
                pool_stopped(&result),
                "a queued read is answered explicitly, got {result:?}"
            );
        }

        assert!(matches!(
            worker.shutdown(),
            Err(DatabaseError::WorkerPanicked)
        ));
        reopen_and_read(&directory);
    }

    #[test]
    fn new_reads_are_refused_once_the_pool_has_stopped() {
        let (_root, directory) = data_directory("refused");
        let worker = DatabaseWorker::start(&directory).unwrap();
        let handle = worker.handle();
        let (held, executing) = hold_every_read_worker(&handle);
        held.release();
        for read in executing {
            assert!(matches!(wait(read), Err(DatabaseError::ReadInterrupted)));
        }
        // A caller sees ReadInterrupted while its worker is still unwinding;
        // the pool stops when the last worker's exit has run. A read issued
        // in between is admitted and then answered by that exit.
        let deadline = Instant::now() + WAIT;
        while !*handle.shared.reads_stopped.read().unwrap() {
            assert!(
                Instant::now() < deadline,
                "the read pool did not stop in time"
            );
            let result = wait(handle.jobs(None, 1));
            assert!(
                pool_stopped(&result),
                "a read during pool exit is answered explicitly, got {result:?}"
            );
        }

        for _ in 0..3 {
            let mut read = Box::pin(handle.job(JobId::new(1)));
            let result = poll_once(read.as_mut())
                .expect("a read is refused at admission once the pool has stopped");
            assert!(pool_stopped(&result), "got {result:?}");
        }
        // A read-worker panic is not a storage failure: commands still run.
        assert_eq!(handle.status(), DatabaseStatus::Open);
        assert!(wait(handle.save_job_input(None, JobInputDraft::default())).is_ok());

        assert!(matches!(
            worker.shutdown(),
            Err(DatabaseError::WorkerPanicked)
        ));
        reopen_and_read(&directory);
    }

    #[test]
    fn read_pool_failure_racing_shutdown_resolves_every_caller() {
        let (_root, directory) = data_directory("shutdown");
        let worker = DatabaseWorker::start(&directory).unwrap();
        let handle = worker.handle();
        let (held, executing) = hold_every_read_worker(&handle);
        let queued = queue_reads(&handle);

        // Shutdown closes admission, then waits for the executing reads,
        // which hold the database open.
        let (finished_sender, finished) = flume::bounded(1);
        let shutdown = std::thread::spawn(move || finished_sender.send(worker.shutdown()).ok());
        let deadline = Instant::now() + WAIT;
        while handle.status() == DatabaseStatus::Open {
            assert!(
                Instant::now() < deadline,
                "shutdown did not close admission in time"
            );
            std::thread::yield_now();
        }
        assert!(
            finished.try_recv().is_err(),
            "shutdown cannot finish while reads hold the database"
        );
        assert!(matches!(
            wait(handle.jobs(None, 1)),
            Err(DatabaseError::ReadsClosed)
        ));
        assert!(matches!(
            wait(handle.save_job_input(None, JobInputDraft::default())),
            Err(DatabaseError::ShuttingDown)
        ));

        held.release();
        for read in executing {
            assert!(matches!(wait(read), Err(DatabaseError::ReadInterrupted)));
        }
        for read in queued {
            let result = wait(read);
            assert!(
                pool_stopped(&result) || matches!(result, Err(DatabaseError::ReadsClosed)),
                "a queued read is answered explicitly, got {result:?}"
            );
        }
        let result = finished.recv_timeout(WAIT).expect("shutdown finished");
        assert!(
            matches!(result, Err(DatabaseError::WorkerPanicked)),
            "got {result:?}"
        );
        shutdown.join().unwrap();
        reopen_and_read(&directory);
    }
}

#[cfg(test)]
mod json_bytes_tests {
    use serde_json::json;

    use super::*;
    use crate::persistence::IntermediateOutputKind;

    fn reference(value: &serde_json::Value) -> u64 {
        byte_count(serde_json::to_vec(value).expect("a Value serializes").len())
    }

    #[test]
    fn matches_compact_serialization() {
        let large = serde_json::Value::Array(
            (0..5_000)
                .map(|index| {
                    json!({
                        "index": index,
                        "text": format!("line \"{index}\"\n\t\\ \u{1F600} \u{e9}"),
                        "nested": [null, true, 1.5, {"deep": ["a", "b"]}],
                    })
                })
                .collect(),
        );
        let values = [
            json!(null),
            json!(true),
            json!(false),
            json!(0),
            json!(-12345),
            json!(u64::MAX),
            json!(3.25),
            json!(""),
            json!("plain"),
            json!("héllo 世界 😀"),
            json!("quote \" backslash \\ newline \n tab \t control \u{1} slash /"),
            json!([]),
            json!({}),
            json!({"a": [1, {"b": null}], "ключ": "значение", "k\"ey": ["x\ny"]}),
            large,
        ];
        for value in values {
            assert_eq!(
                json_bytes(&Some(value.clone())),
                reference(&value),
                "{value}"
            );
        }
    }

    #[test]
    fn absent_is_zero_and_null_is_four() {
        assert_eq!(json_bytes(&None), 0);
        assert_eq!(json_bytes(&Some(serde_json::Value::Null)), 4);
    }

    #[test]
    fn counter_overflow_is_never_a_short_count() {
        let mut counter = ByteCounter {
            bytes: u64::MAX - 1,
        };
        assert!(counter.write(b"ab").is_err());

        let value = json!("abc");
        let near_limit = ByteCounter {
            bytes: u64::MAX - 2,
        };
        assert_eq!(counted_json_bytes(&value, near_limit), u64::MAX);
        let fits = ByteCounter { bytes: 10 };
        assert_eq!(counted_json_bytes(&value, fits), 10 + reference(&value));
    }

    #[test]
    fn json_bearing_command_allowance_uses_the_reference_formula() {
        let value = json!({"tokens": [1, 2, 3], "note": "héllo \"x\""});
        let output = NewJobOutput {
            kind: IntermediateOutputKind::Progress,
            summary: Some("summary".to_owned()),
            code: Some("code".to_owned()),
            goal_reached: None,
            value: Some(value.clone()),
        };
        let (reply, _answer) = flume::bounded(1);
        let command = Command::AppendJobOutput {
            job: JobId::new(1),
            output,
            reply,
        };
        let payload = byte_count("summary".len())
            .saturating_add(byte_count("code".len()))
            .saturating_add(reference(&value));
        assert_eq!(
            command.expected_write_bytes(),
            payload
                .saturating_mul(2)
                .saturating_add(TRANSACTION_ALLOWANCE_BYTES)
        );
    }
}
