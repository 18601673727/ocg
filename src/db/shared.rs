use super::*;

/// A read, boxed with its reply channel. It receives the opened database
/// while a read worker holds it open, or the reason reads are refused.
pub(super) type ReadJob = Box<dyn FnOnce(Result<&Opened, DatabaseError>) + Send>;

/// The database with the epoch of the opening that produced it, so a read
/// always judges an operation against the epoch of the data it saw.
pub(super) struct Opened {
    pub(super) database: Database,
    pub(super) epoch: u64,
}

pub(super) enum ReadMessage {
    Run(ReadJob),
    Stop,
}

#[derive(Clone)]
pub(super) enum Admission {
    Open,
    /// Disk protection refuses commands. Reads continue unless the lock
    /// names a storage failure, which released the database.
    Protected(ProtectionLock),
    ShuttingDown,
    WorkerStopped,
}

impl Admission {
    pub(super) fn check_read(&self) -> Result<(), DatabaseError> {
        match self {
            Self::Open => Ok(()),
            Self::Protected(lock) => match lock.storage_failure() {
                Some(error) => Err(DatabaseError::ReadsStorageFailed(error.clone())),
                None => Ok(()),
            },
            Self::ShuttingDown | Self::WorkerStopped => Err(DatabaseError::ReadsClosed),
        }
    }

    pub(super) fn check_maintenance(&self) -> Result<(), MaintenanceError> {
        match self {
            Self::Open => Ok(()),
            Self::Protected(lock) => Err(MaintenanceError::DiskProtected(lock.clone())),
            Self::ShuttingDown => Err(MaintenanceError::ShuttingDown),
            Self::WorkerStopped => Err(MaintenanceError::WorkerStopped),
        }
    }

    pub(super) fn check_recovery_enqueue(&self) -> Result<(), RecoveryError> {
        match self {
            Self::Open | Self::Protected(_) => Ok(()),
            Self::ShuttingDown => Err(RecoveryError::ShuttingDown),
            Self::WorkerStopped => Err(RecoveryError::WorkerStopped),
        }
    }
}

/// Refuses a command unless admission is open.
pub(super) fn admitted(admission: &Admission) -> Result<(), DatabaseError> {
    match admission {
        Admission::Open => Ok(()),
        Admission::Protected(lock) => Err(DatabaseError::DiskProtected(lock.clone())),
        Admission::ShuttingDown | Admission::WorkerStopped => Err(DatabaseError::ShuttingDown),
    }
}

pub(super) struct Shared {
    pub(super) data_directory: DataDirectory,
    pub(super) output_changes: crate::system::notification::KeyedNotifications<u128>,
    pub(super) catalog_changes: crate::system::notification::Notifications,
    pub(super) observation_changes: crate::system::notification::Journal<ObservationChange>,
    pub(super) log_changes: crate::system::notification::Notifications,
    pub(super) scheduler_wake: SchedulerWake,
    pub(super) scheduler_subscription: std::sync::Mutex<Option<SchedulerWakeSubscription>>,
    pub(super) active_policy: SystemPolicy,
    pub(super) database_directory: PathBuf,
    pub(super) log_directory: PathBuf,
    pub(super) log_files: Arc<crate::system::logger::LogFiles>,
    pub(super) cleanup_worker: Arc<AtomicBool>,
    /// Admission is checked and the command or read enqueued under the read
    /// lock, so once admission is closed under the write lock, every accepted
    /// command and read is already in its queue.
    pub(super) admission: RwLock<Admission>,
    /// Read workers and the write worker hold the read lock while they use
    /// the database; releasing it takes the write lock, so it waits for them.
    pub(super) database: RwLock<Option<Opened>>,
    /// The read barrier of group commit. A redb read transaction sees every
    /// commit, durable or not, so the write worker holds this for writing from
    /// a group's first staged commit until the group's durable commit has
    /// succeeded, or until a failed one has released the database. Every read
    /// holds it for reading, so none can see changes that a failure could
    /// still lose. Always taken before `database`, never while holding it.
    pub(super) staging: RwLock<()>,
    /// The instance's Genesis Job, read from the file when it was opened.
    /// A reopen that finds another identity fails, so it never changes.
    pub(super) genesis: JobId,
    /// The current opening's epoch. Only recovery changes it, by reopening
    /// while admission is closed, so identifiers allocated under the
    /// admission read lock never mix openings.
    pub(super) epoch: AtomicU64,
    pub(super) next_operation: AtomicU64,
    /// Successful durable commits of command groups since this worker started.
    pub(super) durable_commits: AtomicU64,
    /// Total microseconds inside successful durable `commit()` calls.
    pub(super) commit_us_total: AtomicU64,
    /// Maximum microseconds inside a successful durable `commit()` call.
    pub(super) commit_us_max: AtomicU64,
    /// Successful non-empty claim_batch calls.
    pub(super) claims: AtomicU64,
    /// Total round-trip microseconds spent in successful claim_batch calls.
    pub(super) claim_us_total: AtomicU64,
    /// Maximum round-trip microseconds spent in any successful claim_batch call.
    pub(super) claim_us_max: AtomicU64,
    /// The Jobs this worker's start settled as abandoned by an earlier
    /// opening; see [`DatabaseHandle::abandoned_at_open`].
    pub(super) abandoned_at_open: Vec<JobId>,
    /// Set while the database is released, so status checks never wait on
    /// the database lock behind a pending release.
    pub(super) released: AtomicBool,
    /// Read workers whose thread has not yet exited.
    pub(super) live_readers: AtomicUsize,
    /// Set by the last exiting read worker. Reads are enqueued under its read
    /// lock and the queue is drained under its write lock, so once it is set
    /// no read can be left in the queue unanswered.
    pub(super) reads_stopped: RwLock<bool>,
    pub(super) protection: Arc<ProtectionCell>,
    /// The content files of Attachments, opened with the database.
    pub(super) attachments: AttachmentStore,
    pub(super) attachment_slots: (flume::Sender<()>, flume::Receiver<()>),
    /// What this worker's start did to the content files that committed
    /// cleanups authorized it to reclaim; see
    /// [`DatabaseHandle::reclaimed_at_open`].
    pub(super) reclaimed_at_open: Vec<(ContentDigest, Result<Reclamation, StoreError>)>,
    pub(super) reclamation_omitted_at_open: u64,
    /// `None` answers a check that the stopping worker will never run.
    pub(super) recovery: SharedRequest<Result<RecoveryReport, RecoveryError>>,
    /// The natural keys of the commands whose commit had an unknown outcome,
    /// by operation, so [`DatabaseHandle::reconcile_operation`] can read them.
    pub(super) probes: std::sync::Mutex<std::collections::HashMap<OperationId, Option<Probe>>>,
    pub(super) credentials_path: PathBuf,
    pub(super) credentials: RwLock<std::collections::BTreeMap<String, String>>,
    /// Serializes credential-store writers. It is taken on a blocking worker,
    /// so its wait never ties up an async worker, and it orders the file
    /// replacement with the in-memory publication it belongs to.
    pub(super) credentials_writer: std::sync::Mutex<()>,
}

/// Makes the staged commits of a group durable.
pub(super) fn flush_staged(_shared: &Shared, database: &Database) -> Result<u64, StorageError> {
    redb::persist_staged(database)
}

impl Shared {
    pub(super) fn record_durable_commit(&self, commit_us: u64) {
        self.durable_commits.fetch_add(1, Ordering::SeqCst);
        self.commit_us_total.fetch_add(commit_us, Ordering::Relaxed);
        self.commit_us_max.fetch_max(commit_us, Ordering::Relaxed);
    }

    pub(super) fn admission(&self) -> Admission {
        self.admission
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub(super) fn read_refusal(&self) -> DatabaseError {
        match self.admission() {
            Admission::Protected(lock) => match lock.storage_failure() {
                Some(error) => DatabaseError::ReadsStorageFailed(error.clone()),
                None => DatabaseError::ReadsClosed,
            },
            Admission::Open | Admission::ShuttingDown | Admission::WorkerStopped => {
                DatabaseError::ReadsClosed
            }
        }
    }

    pub(super) fn release_database(&self) {
        let database = self
            .database
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        drop(database);
        self.released.store(true, Ordering::SeqCst);
    }

    /// Durably writes one credential and then publishes it. This runs on a
    /// blocking worker: admission was checked before it was scheduled, the
    /// staging file is created private and new, the file and its directory
    /// are synced, and the rename replaces the store atomically. The map is
    /// replaced only after the new file is durable, so a failed write leaves
    /// the old credentials in force. [`Self::credentials_writer`] orders
    /// concurrent writers, so the published map always matches the file.
    pub(super) fn publish_credential(&self, reference: &str, secret: &str) -> Result<(), String> {
        let _writer = self
            .credentials_writer
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let mut updated = self
            .credentials
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        updated.insert(reference.to_owned(), secret.to_owned());
        let serialized = serde_json::to_vec(&updated)
            .map_err(|error| format!("failed to serialize credentials: {error}"))?;
        let temp_path = self.credentials_path.with_extension(format!(
            "json.{}.{}.tmp",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |elapsed| elapsed.as_nanos())
        ));
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::custom_flags(
            &mut options,
            rustix::fs::OFlags::NOFOLLOW.bits() as i32,
        );
        crate::system::data_directory::private_file(&mut options);
        let mut file = options
            .open(&temp_path)
            .map_err(|error| format!("failed to open credential staging file: {error}"))?;
        if let Err(error) = file.write_all(&serialized).and_then(|()| file.sync_all()) {
            drop(file);
            let _ = std::fs::remove_file(&temp_path);
            return Err(format!("failed to write credential staging file: {error}"));
        }
        drop(file);
        if let Err(error) = std::fs::rename(&temp_path, &self.credentials_path) {
            let _ = std::fs::remove_file(&temp_path);
            return Err(format!("failed to commit credential store: {error}"));
        }
        std::fs::File::open(&self.database_directory)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| format!("failed to sync the credential directory: {error}"))?;
        let _ = crate::system::data_directory::restrict_file(&self.credentials_path);
        *self
            .credentials
            .write()
            .unwrap_or_else(PoisonError::into_inner) = updated;
        Ok(())
    }
}

/// The write worker's hold on [`Shared::staging`]. If the worker unwinds while
/// it is held, the staged commits can no longer be made durable, so the
/// database is released before readers are let in again; otherwise a reader
/// could see changes that are lost and whose commands are answered as unknown.
pub(super) struct StagingBarrier<'a> {
    shared: &'a Shared,
    _held: RwLockWriteGuard<'a, ()>,
}

impl<'a> StagingBarrier<'a> {
    pub(super) fn raise(shared: &'a Shared) -> Self {
        Self {
            shared,
            _held: shared
                .staging
                .write()
                .unwrap_or_else(PoisonError::into_inner),
        }
    }
}

impl Drop for StagingBarrier<'_> {
    fn drop(&mut self) {
        // The fields drop after this, so the lock is still held while the
        // database is released. The worker's own database guard is already gone.
        if std::thread::panicking() {
            self.shared.release_database();
        }
    }
}

/// If the write worker unwinds, closes admission, answers every queued
/// command as never executed, releases the database and ends pending space
/// checks, recoveries and watches, so no caller is left waiting and the file
/// can be reopened. A queued message outlives the
/// worker's receiver while handles still hold senders, so it must be drained
/// here. The command that was executing is answered by its own submitter's
/// execution mark.
pub(super) struct ReleaseOnPanic {
    pub(super) shared: Arc<Shared>,
    pub(super) receiver: flume::Receiver<Message>,
    pub(super) control: flume::Receiver<Control>,
}

impl Drop for ReleaseOnPanic {
    fn drop(&mut self) {
        if std::thread::panicking() {
            {
                let mut admission = self
                    .shared
                    .admission
                    .write()
                    .unwrap_or_else(PoisonError::into_inner);
                *admission = Admission::WorkerStopped;
            }
            for message in self.receiver.drain() {
                if let Message::Run(envelope) = message {
                    let envelope = *envelope;
                    let operation = envelope.operation;
                    envelope
                        .command
                        .refuse(|| DatabaseError::NeverExecuted { operation });
                }
            }
            self.shared.recovery.abandon();
            for control in self.control.drain() {
                control.refuse_policy(true);
            }
            self.shared.release_database();
            self.shared.protection.close();
        }
    }
}

/// Moves into each read worker's closure, so it runs however the worker
/// ends: a normal stop, a panic, or a closure dropped because its thread
/// never spawned. The last one to go stops read admission and answers every
/// queued read. A worker panic says nothing about storage, so command
/// admission and status are left alone, and no worker is respawned.
pub(super) struct ReadWorkerExit {
    pub(super) shared: Arc<Shared>,
    pub(super) receiver: flume::Receiver<ReadMessage>,
}

impl Drop for ReadWorkerExit {
    fn drop(&mut self) {
        if self.shared.live_readers.fetch_sub(1, Ordering::SeqCst) != 1 {
            return;
        }
        let mut stopped = self
            .shared
            .reads_stopped
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        *stopped = true;
        for message in self.receiver.drain() {
            if let ReadMessage::Run(read) = message {
                read(Err(DatabaseError::ReadPoolStopped));
            }
        }
    }
}

/// Runs reads until stopped, holding the database open for each one. The
/// database guard lasts as long as the read's transactions, which is what
/// lets the write worker release or reopen the database knowing no read is
/// left on it.
pub(super) fn serve_reads(shared: &Shared, receiver: &flume::Receiver<ReadMessage>) {
    while let Ok(ReadMessage::Run(read)) = receiver.recv() {
        let _staging = shared
            .staging
            .read()
            .unwrap_or_else(PoisonError::into_inner);
        let guard = shared
            .database
            .read()
            .unwrap_or_else(PoisonError::into_inner);
        match guard.as_ref() {
            Some(opened) => read(Ok(opened)),
            None => read(Err(shared.read_refusal())),
        }
    }
}
