use super::*;

pub(super) type ReadJob = Box<dyn FnOnce(Result<&Opened, DatabaseError>) + Send>;

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
    pub(super) admission: RwLock<Admission>,
    pub(super) update_paused: AtomicBool,
    pub(super) database: RwLock<Option<Opened>>,
    pub(super) staging: RwLock<()>,
    pub(super) genesis: JobId,
    pub(super) epoch: AtomicU64,
    pub(super) next_operation: AtomicU64,
    pub(super) durable_commits: AtomicU64,
    pub(super) commit_us_total: AtomicU64,
    pub(super) commit_us_max: AtomicU64,
    pub(super) abandoned_at_open: Vec<JobId>,
    pub(super) released: AtomicBool,
    pub(super) live_readers: AtomicUsize,
    pub(super) reads_stopped: RwLock<bool>,
    pub(super) protection: Arc<ProtectionCell>,
    pub(super) attachments: AttachmentStore,
    pub(super) attachment_slots: (flume::Sender<()>, flume::Receiver<()>),
    pub(super) reclaimed_at_open: Vec<(ContentDigest, Result<Reclamation, StoreError>)>,
    pub(super) reclamation_omitted_at_open: u64,
    pub(super) recovery: SharedRequest<Result<RecoveryReport, RecoveryError>>,
    pub(super) probes: std::sync::Mutex<std::collections::HashMap<OperationId, Option<Probe>>>,
    pub(super) in_flight_commands: AtomicUsize,
    pub(super) credential_key: std::sync::OnceLock<Arc<redb::CredentialKey>>,
    pub(super) credential_key_writer: std::sync::Mutex<()>,
    pub(super) undecryptable_credentials_at_open: u64,
    pub(super) open_warnings_logged: AtomicBool,
}

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

    pub(super) fn credential_key(&self) -> Option<Arc<redb::CredentialKey>> {
        self.credential_key.get().cloned()
    }

    pub(super) fn ensure_credential_key(&self) -> Result<Arc<redb::CredentialKey>, StorageError> {
        if let Some(key) = self.credential_key.get() {
            return Ok(key.clone());
        }
        let _writer = self
            .credential_key_writer
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(key) = self.credential_key.get() {
            return Ok(key.clone());
        }
        let key = match redb::load_credential_key(&self.database_directory)? {
            Some(key) => key,
            None => redb::create_credential_key(&self.database_directory)?,
        };
        let _ = self.credential_key.set(key.clone());
        Ok(key)
    }
}

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
        if std::thread::panicking() {
            self.shared.release_database();
        }
    }
}

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
