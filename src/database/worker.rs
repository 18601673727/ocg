use super::*;

/// A storage failure that ends this opening of the database.
pub(super) struct StorageFailure {
    pub(super) error: StorageError,
    /// The command's commit failed, so whether it committed is unknown.
    pub(super) outcome_unknown: bool,
}

/// Sends a command's result. It receives the error of the group's durable
/// commit when that failed, and `None` otherwise.
pub(super) type Answer = Box<dyn FnOnce(Option<&StorageError>) + Send>;

/// A command that ran and has not been answered.
pub(super) struct Executed {
    operation: OperationId,
    /// Its changes are in a staged commit that is not durable yet.
    staged: bool,
    scheduling_dirty: bool,
    capacity_released: bool,
    /// The natural key that says whether the command committed, should its
    /// commit turn out uncertain.
    probe: Option<Probe>,
    answer: Answer,
    /// The storage failure that must release this opening of the database.
    failure: Option<StorageFailure>,
}

pub(super) fn run<T: Send + 'static>(
    database: &Database,
    operation: OperationId,
    commit: Commit,
    kind: OperationKind,
    reply: Reply<T>,
    body: impl FnOnce(&mut storage::Batch) -> Result<T, BatchError>,
) -> Executed {
    let outcome = storage::run_batch_with(database, operation.epoch, commit, body);
    settle(operation, commit, kind, reply, outcome, false)
}

/// Answers a command from its batch outcome and says how it must be waited on.
/// `declined` is set only by a command that deliberately ended without
/// committing and replied `Ok`. It leaves nothing staged for the group's
/// durable commit to cover, so a lost group commit did not lose it: its answer
/// then reports that nothing of it was committed, like a refusal, because the
/// decline may rest on state that the lost commit had written.
pub(super) fn settle<T: Send + 'static>(
    operation: OperationId,
    commit: Commit,
    kind: OperationKind,
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
    let scheduling_dirty =
        result.is_ok() && !declined && (affects_scheduling(kind) || outcome.scheduling_wake);
    let capacity_released =
        result.is_ok() && !declined && (releases_capacity(kind) || outcome.capacity_freed);
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
        scheduling_dirty,
        capacity_released,
        probe: outcome.probe,
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
    pub(super) handle: DatabaseHandle,
    pub(super) thread: JoinHandle<Result<(), DatabaseError>>,
    pub(super) readers: Vec<JoinHandle<()>>,
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
    pub(super) sender: flume::Sender<Message>,
    pub(super) read_sender: flume::Sender<ReadMessage>,
    pub(super) control_sender: flume::Sender<Control>,
    pub(super) shared: Arc<Shared>,
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
        let (scheduler_wake, scheduler_subscription) = SchedulerWake::new();
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
                    abandoned,
                } = match storage::open(&directory) {
                    Ok(opened) => opened,
                    Err(error) => {
                        ready_sender.send(Err(open_error(error, &directory))).ok();
                        return Ok(());
                    }
                };
                let abandoned_at_open = abandoned;
                let next_operation = 1;
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
                let credentials_path = directory.join("credentials.json");
                let credentials = load_credentials_file(&credentials_path);
                let shared = Arc::new(Shared {
                    scheduler_wake,
                    scheduler_subscription: std::sync::Mutex::new(Some(scheduler_subscription)),
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
                    durable_commits: AtomicU64::new(0),
                    abandoned_at_open,
                    released: AtomicBool::new(false),
                    live_readers: AtomicUsize::new(0),
                    reads_stopped: RwLock::new(false),
                    protection: ProtectionCell::new(),
                    attachments,
                    reclaimed_at_open,
                    space_check: SharedRequest::new(),
                    recovery: SharedRequest::new(),
                    probes: std::sync::Mutex::new(std::collections::HashMap::new()),
                    credentials_path,
                    credentials: RwLock::new(credentials),
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
                if !worker.shared.abandoned_at_open.is_empty() {
                    worker.publish_scheduling_change(true, true);
                }
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

    pub fn take_scheduler_wake(&mut self) -> Option<SchedulerWakeSubscription> {
        self.handle.take_scheduler_wake()
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

pub(super) fn file_length(path: &std::path::Path) -> Result<u64, MaintenanceError> {
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

pub(super) fn database_not_open() -> StorageError {
    StorageError {
        kind: StorageErrorKind::Closed,
        os_error: None,
        detail: "the database is not open".to_string(),
    }
}

pub(super) fn open_error(error: OpenError, directory: &std::path::Path) -> DatabaseError {
    match error {
        OpenError::Storage(error) => DatabaseError::Open(error),
        OpenError::ForeignFile => DatabaseError::ForeignDatabase(storage::database_file(directory)),
        OpenError::UnsupportedFormat { found } => DatabaseError::UnsupportedFormat {
            found,
            supported: storage::FORMAT_VERSION,
        },
    }
}

fn load_credentials_file(path: &std::path::Path) -> std::collections::BTreeMap<String, String> {
    if !path.exists() {
        return std::collections::BTreeMap::new();
    }
    let _ = crate::data_directory::restrict_file(path);
    std::fs::read_to_string(path)
        .ok()
        .and_then(|contents| serde_json::from_str(&contents).ok())
        .unwrap_or_default()
}

pub(super) fn join(thread: JoinHandle<Result<(), DatabaseError>>) -> Result<(), DatabaseError> {
    thread.join().map_err(|_| DatabaseError::WorkerPanicked)?
}

/// Every protected filesystem that is below `threshold_bytes` or cannot be
/// measured.
pub(super) fn space_shortfalls(
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

pub(super) fn reopen_failure(error: OpenError, directory: &std::path::Path) -> ReopenFailure {
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
pub(super) struct WriteWorker {
    pub(super) shared: Arc<Shared>,
    pub(super) receiver: flume::Receiver<Message>,
    pub(super) control: flume::Receiver<Control>,
    /// The database directory, which is also the path checked before each
    /// command.
    pub(super) directory: PathBuf,
    pub(super) paths: [(ProtectedArea, PathBuf); 3],
    pub(super) config: FilesystemConfig,
    /// `None` when the interval does not fit in an `Instant`.
    pub(super) next_check: Option<Instant>,
    /// Operations whose commit failed with an unknown outcome; the next
    /// successful recovery reconciles them.
    pub(super) uncertain: Vec<(OperationId, Option<Probe>)>,
    /// Set from a storage failure until a reopen replaces the failed handle.
    pub(super) storage_failure: Option<StorageError>,
    /// When the last successful compaction started, including one that moved
    /// no pages. Failed attempts leave it unchanged.
    pub(super) last_compaction: Option<Instant>,
    pub(super) shutdown_requested: bool,
}

pub(super) enum SpaceRefusal {
    Short(SpaceShortage),
    Locked(ProtectionLock),
}

pub(super) enum Event {
    Control(Result<Control, flume::RecvError>),
    Message(Result<Message, flume::RecvError>),
    CheckDue,
}

impl WriteWorker {
    /// Runs control requests, due checks and commands until shutdown. A due
    /// check runs before the next command, so steady traffic cannot
    /// postpone it by more than one command.
    pub(super) fn serve(&mut self) {
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

    pub(super) fn handle(&mut self, control: Control) {
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

    pub(super) fn policy_path(&self) -> PathBuf {
        self.directory.join(system_policy::SYSTEM_POLICY_FILE_NAME)
    }

    pub(super) fn policy_save_admission(&self) -> Result<(), PolicyOperationError> {
        match self.shared.admission() {
            Admission::Open => Ok(()),
            Admission::Protected(lock) => Err(PolicyOperationError::DiskProtected(lock)),
            Admission::ShuttingDown => Err(PolicyOperationError::ShuttingDown),
            Admission::WorkerStopped => Err(PolicyOperationError::WorkerStopped),
        }
    }

    pub(super) fn save_policy(
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

    pub(super) fn replace_policy(
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
    pub(super) fn admit(&mut self, envelope: Envelope) -> Option<Envelope> {
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
    pub(super) fn check_database_space(
        &mut self,
        expected_write_bytes: u64,
    ) -> Result<(), SpaceRefusal> {
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
    pub(super) fn execute_group(&mut self, first: Envelope) {
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
                    self.note_uncertain(ran.operation, ran.probe);
                }
            }
            failure.get_or_insert(StorageFailure {
                error: error.clone(),
                outcome_unknown: false,
            });
        }
        // The group's durability is settled: a lone command committed its own
        // transaction, and a group of them made all of theirs durable together.
        if !executed.is_empty() {
            self.shared.durable_commits.fetch_add(1, Ordering::SeqCst);
        }
        for (ran, outcome_unknown) in &executed {
            if *outcome_unknown {
                self.note_uncertain(ran.operation, ran.probe);
            }
        }
        // The database guard is gone and the staging barrier still held, so
        // releasing the database waits for no reader and no reader can see a
        // lost change before it is released.
        let scheduling_dirty = failure.is_none()
            && lost.is_none()
            && executed.iter().any(|(ran, _)| ran.scheduling_dirty);
        let capacity_released = failure.is_none()
            && lost.is_none()
            && executed.iter().any(|(ran, _)| ran.capacity_released);
        let lock = failure.map(|failure| {
            self.storage_failure = Some(failure.error.clone());
            self.activate(vec![ProtectionReason::StorageUnavailable(failure.error)])
        });
        drop(staging);
        if scheduling_dirty || capacity_released {
            self.publish_scheduling_change(scheduling_dirty, capacity_released);
        }
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

    fn publish_scheduling_change(&self, dirty: bool, capacity: bool) {
        // Shutdown drains accepted writes too. A known durable commit may
        // still dirty the generation then, but protected storage must recover
        // before a notification can advertise its state as readable again.
        if self.storage_failure.is_none()
            && !self.shared.released.load(Ordering::SeqCst)
            && self.shared.protection.status().lock.is_none()
        {
            self.shared.scheduler_wake.publish(dirty, capacity);
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
    pub(super) fn compact_storage(&mut self) -> Result<CompactionOutcome, MaintenanceError> {
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
    pub(super) fn fail_storage(&mut self, error: StorageError) -> MaintenanceError {
        self.storage_failure = Some(error.clone());
        self.activate(vec![ProtectionReason::StorageUnavailable(error.clone())]);
        MaintenanceError::Storage(error)
    }

    /// Closes command admission, refuses every queued command, and releases
    /// the database when storage failed. Runs between commands, so the one
    /// that was executing has already answered with its own result.
    pub(super) fn activate(&mut self, reasons: Vec<ProtectionReason>) -> ProtectionLock {
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

    pub(super) fn check_interval(&self) -> Duration {
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
    pub(super) fn check_all_paths(&mut self) {
        let reasons = space_shortfalls(&self.paths, self.config.disk_lock_threshold_bytes);
        if !reasons.is_empty() {
            self.activate(reasons);
        } else if self.only_lacks_space() {
            // A failed attempt keeps the lock; the next check tries again.
            let _ = self.recover();
        }
        self.shared.protection.update(|status| {
            status.completed_checks += 1;
            status.last_checked_at = Some(persistence::Timestamp::now());
        });
        self.next_check = Instant::now().checked_add(self.check_interval());
    }

    /// Whether admission is locked for no reason but measured shortages of
    /// space, so lifting it needs nothing but space. A failed measurement, a
    /// failed write, a failed commit or flush, and any operation whose
    /// outcome is unknown all leave the lock to an explicit recovery.
    pub(super) fn only_lacks_space(&self) -> bool {
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
    pub(super) fn recover(&mut self) -> Result<RecoveryReport, RecoveryError> {
        match self.shared.admission() {
            Admission::Open => {
                self.publish_scheduling_change(true, true);
                return Ok(RecoveryReport::default());
            }
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
        } else {
            self.publish_scheduling_change(true, true);
        }
        result
    }

    pub(super) fn attempt_recovery(&mut self) -> Result<RecoveryReport, RecoveryError> {
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
    pub(super) fn reopen(&mut self) -> Result<u64, RecoveryError> {
        self.shared.release_database();
        let OpenedFile {
            database,
            epoch,
            genesis,
            ..
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

    /// Remembers an operation whose commit has an unknown outcome, with the
    /// natural key that will say whether it committed.
    pub(super) fn note_uncertain(&mut self, operation: OperationId, probe: Option<Probe>) {
        self.uncertain.push((operation, probe));
        self.shared
            .probes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(operation, probe);
    }

    /// Probes the natural key of every operation whose outcome was unknown.
    /// A failed read is a storage failure, so the handle is released again.
    pub(super) fn reconcile_uncertain(
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
                    .map(|&(operation, probe)| {
                        operation_status(&opened.database, operation, probe, opened.epoch)
                            .map(|status| (operation, status))
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
    pub(super) fn exit(self) -> Result<(), DatabaseError> {
        if self.storage_failure.is_none()
            && let Some(opened) = self
                .shared
                .database
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .as_ref()
        {
            let _ = crate::storage::record_clean_shutdown(&opened.database);
        }
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
