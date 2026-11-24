use super::{
    COMMAND_QUEUE_CAPACITY, COMPACTION_COOLDOWN, COMPACTION_MIN_FILE_BYTES,
    COMPACTION_MIN_RECLAIMABLE_BYTES, COMPACTION_MIN_RECLAIMABLE_DIVISOR, GROUP_LIMIT,
    MAINTENANCE_QUEUE_CAPACITY, ObservationChange, READ_QUEUE_CAPACITY, READ_WORKERS,
    TRANSACTION_ALLOWANCE_BYTES, next_check_interval,
};
use crate::db::attachments::AttachmentStore;
use crate::db::commands::{Envelope, Maintenance, Message, Reply};
use crate::db::entities::{JobId, OperationId, OperationKind};
use crate::db::errors::{
    CompactionOutcome, CompactionReport, CompactionSkip, DatabaseError, MaintenanceError,
    OperationStatus, UnknownCause, operation_status,
};
use crate::db::protection::{
    ProtectedArea, ProtectionCell, ProtectionLock, ProtectionReason, RecoveryError, RecoveryReport,
    ReopenFailure, SpaceMeasurement, SpaceShortage,
};
use crate::db::redb::{
    BatchError, BatchFailure, Commit, CompactFailure, OpenedFile, Probe, StorageError,
    StorageErrorKind,
};
use crate::db::scheduler_wake::{SchedulerWake, affects_scheduling};
use crate::db::shared::{
    Admission, MaintenanceRequest, Opened, ReadMessage, ReadWorkerExit, ReleaseOnPanic, Shared,
    StagingBarrier, flush_staged, serve_reads,
};
use crate::db::{entities, redb};
use crate::host::data_directory::DataDirectory;
use crate::host::disk::{self, FilesystemConfig};
use ::redb::Database;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, PoisonError, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

pub(super) struct StorageFailure {
    pub(super) error: StorageError,
    pub(super) outcome_unknown: bool,
}

pub(super) type Answer = Box<dyn FnOnce(Option<&StorageError>) + Send>;

fn send_reply<T>(reply: &flume::Sender<T>, value: T) {
    let _ = reply.send(value);
}

pub(super) struct Executed {
    operation: OperationId,
    staged: bool,
    durable_commit_us: Option<u64>,
    scheduling_dirty: bool,
    catalog_changed: bool,
    generative_config_changed: bool,
    pub(super) output_changed: Option<JobId>,
    pub(super) observations: Vec<ObservationChange>,
    pub(super) committed: bool,
    probe: Option<Probe>,
    resource_deltas: Vec<redb::NativeResourceDelta>,
    answer: Answer,
    failure: Option<StorageFailure>,
    needs_full_verification: bool,
}

pub(super) fn run<T: Send + 'static>(
    database: &Database,
    operation: OperationId,
    commit: Commit,
    kind: OperationKind,
    reply: Reply<T>,
    body: impl FnOnce(&mut redb::Batch) -> Result<T, BatchError>,
) -> Executed {
    let outcome = redb::run_batch_with(database, operation.epoch, commit, body);
    settle(operation, commit, kind, reply, outcome, false)
}

pub(super) fn settle<T: Send + 'static>(
    operation: OperationId,
    commit: Commit,
    kind: OperationKind,
    reply: Reply<T>,
    outcome: redb::BatchOutcome<T>,
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
    let committed = result.is_ok() && !declined;
    let staged = commit == Commit::Staged && committed;
    let scheduling_dirty = committed
        && (affects_scheduling(kind) || outcome.scheduling_wake || outcome.capacity_freed);
    let catalog_changed = committed && (outcome.catalog_changed || outcome.scheduling_wake);
    let output_changed = None;
    let answer: Answer = Box::new(move |lost| {
        let result = match (result, lost) {
            (Ok(_), Some(error)) if declined => Err(DatabaseError::RolledBack(error.clone())),
            (Ok(_), Some(error)) => Err(DatabaseError::OutcomeUnknown {
                operation,
                cause: UnknownCause::Commit(error.clone()),
            }),
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
        durable_commit_us: outcome.durable_commit_us,
        scheduling_dirty,
        catalog_changed,
        generative_config_changed: committed && outcome.generative_config_changed,
        output_changed,
        observations: Vec::new(),
        committed,
        probe: outcome.probe,
        resource_deltas: if committed {
            outcome.resource_deltas
        } else {
            Vec::new()
        },
        answer,
        failure: outcome.storage_failure.map(|error| StorageFailure {
            error,
            outcome_unknown,
        }),
        needs_full_verification: outcome.needs_full_verification,
    }
}

pub struct DatabaseWorker {
    pub(super) handle: DatabaseHandle,
    pub(super) thread: JoinHandle<Result<(), DatabaseError>>,
    pub(super) readers: Vec<JoinHandle<()>>,
}

#[derive(Clone)]
pub struct DatabaseHandle {
    pub(super) sender: flume::Sender<Message>,
    pub(super) read_sender: flume::Sender<ReadMessage>,
    pub(super) maintenance_sender: flume::Sender<MaintenanceRequest>,
    pub(super) shared: Arc<Shared>,
}

impl DatabaseWorker {
    pub fn start(data_directory: &DataDirectory) -> Result<Self, DatabaseError> {
        Self::start_with_config(data_directory, FilesystemConfig::default())
    }

    pub fn start_with_config(
        data_directory: &DataDirectory,
        config: FilesystemConfig,
    ) -> Result<Self, DatabaseError> {
        config.validate().map_err(DatabaseError::Configuration)?;
        let attachment_directory = data_directory.attachments_path();
        let directory = data_directory.database_path().to_path_buf();
        let data_directory = data_directory.clone();
        let paths = disk::protected_paths(&data_directory);
        let (sender, receiver) = flume::bounded(COMMAND_QUEUE_CAPACITY);
        let (maintenance_sender, maintenance) = flume::bounded(MAINTENANCE_QUEUE_CAPACITY);
        let (ready_sender, ready_receiver) = flume::bounded(1);
        let (scheduler_wake, scheduler_subscription) = SchedulerWake::new();
        let thread = std::thread::Builder::new()
            .name("ocg-database".to_string())
            .spawn(move || {
                let shortfalls = space_shortfalls(&paths, config.disk_lock_threshold_bytes);
                if !shortfalls.is_empty() {
                    ready_sender
                        .send(Err(DatabaseError::InsufficientStartupSpace(shortfalls)))
                        .ok();
                    return Ok(());
                }
                let _database_lock = match crate::host::data_directory::lock_database(&directory) {
                    Ok(lock) => lock,
                    Err(error) => {
                        ready_sender.send(Err(DatabaseError::Startup(error))).ok();
                        return Ok(());
                    }
                };
                // Opening redb changes the digest needed to finalize an offline publication.
                #[cfg(unix)]
                if let Err(error) = crate::cli::check_offline_publication(&directory) {
                    ready_sender.send(Err(DatabaseError::Startup(error))).ok();
                    return Ok(());
                }
                let OpenedFile { database, epoch } = match redb::open(&directory) {
                    Ok(opened) => opened,
                    Err(error) => {
                        ready_sender.send(Err(DatabaseError::Open(error))).ok();
                        return Ok(());
                    }
                };
                let next_operation = 1;
                // Hold the database lock before removing staging files from earlier runs.
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
                let reclamation = match redb::reclaim_pending(&database, &attachments) {
                    Ok(report) => report,
                    Err(error) => {
                        ready_sender.send(Err(DatabaseError::Open(error))).ok();
                        return Ok(());
                    }
                };
                let credential_key = match redb::load_credential_key(&directory) {
                    Ok(key) => key,
                    Err(error) => {
                        ready_sender.send(Err(DatabaseError::Open(error))).ok();
                        return Ok(());
                    }
                };
                let undecryptable_credentials_at_open = if credential_key.is_none() {
                    match redb::credential_count(&database) {
                        Ok(count) => count,
                        Err(error) => {
                            ready_sender.send(Err(DatabaseError::Open(error))).ok();
                            return Ok(());
                        }
                    }
                } else {
                    0
                };
                let credential_key_cell = std::sync::OnceLock::new();
                if let Some(key) = credential_key {
                    let _ = credential_key_cell.set(key);
                }
                let native_resources = match redb::read_native_resource_catalog(&database) {
                    Ok(catalog) => Arc::new(RwLock::new(catalog)),
                    Err(error) => {
                        ready_sender.send(Err(DatabaseError::Open(error))).ok();
                        return Ok(());
                    }
                };
                let attachment_slots = flume::bounded(4);
                for _ in 0..4 {
                    let _ = attachment_slots.0.try_send(());
                }
                let shared = Arc::new(Shared {
                    data_directory: data_directory.clone(),
                    catalog_changes: Default::default(),
                    generative_config_changes: Default::default(),
                    observation_changes: Default::default(),
                    output_changes: Default::default(),
                    scheduler_wake,
                    native_stops: Default::default(),
                    scheduler_subscription: std::sync::Mutex::new(Some(scheduler_subscription)),
                    database_directory: directory.to_path_buf(),
                    admission: RwLock::new(Admission::Open),
                    update_paused: AtomicBool::new(false),
                    database: RwLock::new(Some(Opened { database, epoch })),
                    staging: RwLock::new(()),
                    epoch: AtomicU64::new(epoch),
                    next_operation: AtomicU64::new(next_operation),
                    durable_commits: AtomicU64::new(0),
                    commit_us_total: AtomicU64::new(0),
                    commit_us_max: AtomicU64::new(0),
                    released: AtomicBool::new(false),
                    live_readers: AtomicUsize::new(0),
                    reads_stopped: RwLock::new(false),
                    protection: ProtectionCell::new(),
                    attachments,
                    attachment_slots,
                    reclaimed_at_open: reclamation.details,
                    reclamation_omitted_at_open: reclamation.omitted,
                    probes: std::sync::Mutex::new(std::collections::HashMap::new()),
                    in_flight_commands: AtomicUsize::new(0),
                    in_flight_maintenance: AtomicUsize::new(0),
                    credential_key: credential_key_cell,
                    credential_key_writer: std::sync::Mutex::new(()),
                    undecryptable_credentials_at_open,
                    open_warnings_logged: AtomicBool::new(false),
                    needs_full_verification: AtomicBool::new(false),
                    native_resources,
                });
                let _release = ReleaseOnPanic {
                    shared: shared.clone(),
                    receiver: receiver.clone(),
                    maintenance: maintenance.clone(),
                };
                let mut worker = WriteWorker {
                    shared: shared.clone(),
                    receiver,
                    maintenance,
                    directory,
                    paths,
                    config,
                    next_check: None,
                    uncertain: Vec::new(),
                    storage_failure: None,
                    needs_full_verification: false,
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
                maintenance_sender,
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
        worker.handle.log_open_warnings();
        Ok(worker)
    }

    pub fn handle(&self) -> DatabaseHandle {
        self.handle.clone()
    }

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
        self.handle.sender.send(Message::Shutdown).ok();
        let written = join(self.thread);
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

pub(super) fn join(thread: JoinHandle<Result<(), DatabaseError>>) -> Result<(), DatabaseError> {
    thread.join().map_err(|_| DatabaseError::WorkerPanicked)?
}

impl DatabaseHandle {
    pub(crate) fn log_open_warnings(&self) {
        if log::max_level() < log::LevelFilter::Warn
            || self
                .shared
                .open_warnings_logged
                .swap(true, Ordering::SeqCst)
        {
            return;
        }
        if self.shared.undecryptable_credentials_at_open > 0 {
            log::warn!(
                "ocg.key is missing in {}; {} stored credential(s) cannot be decrypted",
                self.shared.database_directory.display(),
                self.shared.undecryptable_credentials_at_open
            );
        }
    }
}

pub(super) fn space_shortfalls(
    paths: &[(ProtectedArea, PathBuf)],
    threshold_bytes: u64,
) -> Vec<ProtectionReason> {
    paths
        .iter()
        .filter_map(|(area, path)| disk::check_space(*area, path, 0, threshold_bytes).err())
        .collect()
}

pub(super) struct WriteWorker {
    pub(super) shared: Arc<Shared>,
    pub(super) receiver: flume::Receiver<Message>,
    pub(super) maintenance: flume::Receiver<MaintenanceRequest>,
    pub(super) directory: PathBuf,
    pub(super) paths: [(ProtectedArea, PathBuf); 3],
    pub(super) config: FilesystemConfig,
    pub(super) next_check: Option<Instant>,
    pub(super) uncertain: Vec<(OperationId, Option<Probe>)>,
    pub(super) storage_failure: Option<StorageError>,
    needs_full_verification: bool,
    pub(super) last_compaction: Option<Instant>,
    pub(super) shutdown_requested: bool,
}

pub(super) enum SpaceRefusal {
    Short(SpaceShortage),
    Locked(ProtectionLock),
}

pub(super) enum Event {
    Maintenance(Result<MaintenanceRequest, flume::RecvError>),
    Message(Result<Message, flume::RecvError>),
    CheckDue,
}

impl WriteWorker {
    pub(super) fn serve(&mut self) {
        while !self.shutdown_requested {
            if let Some(lock) = self.shared.protection.status().lock
                && !self.receiver.is_empty()
            {
                self.drain_protected(&lock);
                continue;
            }
            if self.next_check.is_some_and(|due| Instant::now() >= due) {
                self.check_all_paths();
                continue;
            }
            if let Ok(maintenance) = self.maintenance.try_recv() {
                self.handle(maintenance);
                continue;
            }
            let selector = flume::Selector::new()
                .recv(&self.maintenance, Event::Maintenance)
                .recv(&self.receiver, Event::Message);
            let event = match self.next_check {
                Some(due) => selector.wait_deadline(due).unwrap_or(Event::CheckDue),
                None => selector.wait(),
            };
            match event {
                Event::Maintenance(Ok(maintenance)) => self.handle(maintenance),
                Event::Message(Ok(Message::Run(envelope))) => self.execute_group(*envelope),
                Event::CheckDue => {}
                Event::Message(Ok(Message::Shutdown))
                | Event::Message(Err(_))
                | Event::Maintenance(Err(_)) => return,
            }
        }
    }

    pub(super) fn handle(&mut self, maintenance: MaintenanceRequest) {
        match maintenance.message {
            Maintenance::FilesystemFailure { reason } => {
                let active = matches!(
                    self.shared.admission(),
                    Admission::Open | Admission::Protected(_)
                );
                if active {
                    self.activate(vec![reason]);
                }
            }
            Maintenance::CompactStorage(reply) => {
                let result = if self.shared.update_paused.load(Ordering::SeqCst) {
                    Err(MaintenanceError::ShuttingDown)
                } else {
                    self.compact_storage()
                };
                send_reply(&reply, result);
            }
        }
    }

    fn admit(&mut self, envelope: Envelope, space_checked: bool) -> Option<Envelope> {
        let operation = envelope.operation;
        if self.shared.update_paused.load(Ordering::SeqCst) {
            let Envelope {
                command, in_flight, ..
            } = envelope;
            drop(in_flight);
            command.refuse(|| DatabaseError::NeverExecuted { operation });
            return None;
        }
        if let Some(lock) = self.shared.protection.status().lock {
            let Envelope {
                command, in_flight, ..
            } = envelope;
            drop(in_flight);
            command.refuse(|| DatabaseError::RefusedByDiskProtection {
                operation,
                lock: lock.clone(),
            });
            return None;
        }
        if space_checked {
            return Some(envelope);
        }
        match self.check_database_space(envelope.command.expected_write_bytes()) {
            Ok(()) => Some(envelope),
            Err(SpaceRefusal::Short(shortage)) => {
                let Envelope {
                    command, in_flight, ..
                } = envelope;
                drop(in_flight);
                command.refuse(|| DatabaseError::InsufficientSpace {
                    operation,
                    shortage: shortage.clone(),
                });
                None
            }
            Err(SpaceRefusal::Locked(lock)) => {
                let Envelope {
                    command, in_flight, ..
                } = envelope;
                drop(in_flight);
                command.refuse(|| DatabaseError::RefusedByDiskProtection {
                    operation,
                    lock: lock.clone(),
                });
                None
            }
        }
    }

    pub(super) fn check_database_space(
        &mut self,
        expected_write_bytes: u64,
    ) -> Result<(), SpaceRefusal> {
        match disk::check_space(
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

    pub(super) fn execute_group(&mut self, first: Envelope) {
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
        // Keep staged writes hidden until they are durable or the database is released.
        let staging = (commit == Commit::Staged).then(|| StagingBarrier::raise(&shared));

        // A failed aggregate estimate must leave protection and individual refusals
        // to the original per-command admission path.
        let space_checked = group.len() > 1
            && !shared.update_paused.load(Ordering::SeqCst)
            && shared.protection.status().lock.is_none()
            && disk::check_space(
                ProtectedArea::Database,
                &self.directory,
                group.iter().fold(0u64, |total, envelope| {
                    total.saturating_add(envelope.command.expected_write_bytes())
                }),
                self.config.disk_lock_threshold_bytes,
            )
            .is_ok();

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
                let Some(envelope) = self.admit(envelope, space_checked) else {
                    continue;
                };
                let Envelope {
                    operation,
                    started,
                    command,
                    in_flight,
                } = envelope;
                let Some(opened) = guard.as_ref() else {
                    drop(in_flight);
                    command.refuse(|| DatabaseError::NeverExecuted { operation });
                    continue;
                };
                started.store(true, Ordering::SeqCst);
                let mut ran = command.execute(&opened.database, operation, commit);
                self.needs_full_verification |= ran.needs_full_verification;
                drop(in_flight);
                if ran.committed && !ran.resource_deltas.is_empty() {
                    let mut catalog = shared
                        .native_resources
                        .write()
                        .unwrap_or_else(PoisonError::into_inner);
                    for delta in ran.resource_deltas.drain(..) {
                        catalog.apply(delta);
                    }
                }
                if let Some(commit_us) = ran.durable_commit_us {
                    shared.record_durable_commit(commit_us);
                }
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
                match flush_staged(&shared, &opened.database) {
                    Ok(commit_us) => shared.record_durable_commit(commit_us),
                    Err(error) => lost = Some(error),
                }
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
        for (ran, outcome_unknown) in &executed {
            if *outcome_unknown {
                self.note_uncertain(ran.operation, ran.probe);
            }
        }
        let scheduling_dirty = failure.is_none()
            && lost.is_none()
            && executed.iter().any(|(ran, _)| ran.scheduling_dirty);
        let catalog_changed = failure.is_none()
            && lost.is_none()
            && executed.iter().any(|(ran, _)| ran.catalog_changed);
        let lock = failure.map(|failure| {
            self.storage_failure = Some(failure.error.clone());
            self.activate(vec![ProtectionReason::StorageUnavailable(failure.error)])
        });
        drop(staging);
        if scheduling_dirty {
            self.publish_scheduling_change();
        }
        if catalog_changed {
            self.shared.catalog_changes.publish();
        }
        if lock.is_none() && lost.is_none() {
            if executed
                .iter()
                .any(|(ran, _)| ran.generative_config_changed)
            {
                self.shared.generative_config_changes.publish();
            }
            for (ran, _) in &executed {
                if ran.committed {
                    for observation in &ran.observations {
                        self.shared.observation_changes.publish(*observation);
                    }
                    if let Some(job) = ran.output_changed {
                        self.shared.output_changes.publish(job.get());
                    }
                }
            }
        } else {
            self.shared.observation_changes.reset();
        }
        for (ran, _) in executed {
            (ran.answer)(lost.as_ref());
        }
        if let Some(lock) = lock {
            for envelope in unrun {
                let Envelope {
                    operation,
                    command,
                    in_flight,
                    ..
                } = envelope;
                drop(in_flight);
                command.refuse(|| DatabaseError::RefusedByDiskProtection {
                    operation,
                    lock: lock.clone(),
                });
            }
        }
    }

    fn publish_scheduling_change(&self) {
        if self.storage_failure.is_none()
            && !self.shared.released.load(Ordering::SeqCst)
            && self.shared.protection.status().lock.is_none()
        {
            self.shared.scheduler_wake.publish();
        }
    }

    pub(super) fn compact_storage(&mut self) -> Result<CompactionOutcome, MaintenanceError> {
        self.shared.admission().check_maintenance()?;
        if let Some(remaining) = self
            .last_compaction
            .and_then(|last| COMPACTION_COOLDOWN.checked_sub(last.elapsed()))
        {
            return Ok(CompactionOutcome::Skipped(CompactionSkip::CoolingDown {
                remaining,
            }));
        }
        let file = redb::database_file(&self.directory);
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
                Some(opened) => redb::allocated_bytes(&opened.database),
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
        let compacted = {
            let mut guard = shared
                .database
                .write()
                .unwrap_or_else(PoisonError::into_inner);
            match guard.as_mut() {
                Some(opened) => redb::compact(&mut opened.database),
                None => Err(CompactFailure::Storage(database_not_open())),
            }
        };
        let duration = began.elapsed();
        match compacted {
            Ok(moved) => {
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

    pub(super) fn fail_storage(&mut self, error: StorageError) -> MaintenanceError {
        self.storage_failure = Some(error.clone());
        self.activate(vec![ProtectionReason::StorageUnavailable(error.clone())]);
        MaintenanceError::Storage(error)
    }

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
        self.drain_protected(&lock);
        if storage_failed {
            self.shared.release_database();
        }
        let soon = Instant::now().checked_add(self.check_interval());
        self.next_check = match (self.next_check, soon) {
            (Some(due), Some(soon)) => Some(due.min(soon)),
            (due, soon) => soon.or(due),
        };
        lock
    }

    fn drain_protected(&mut self, lock: &ProtectionLock) {
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
    }

    pub(super) fn check_interval(&self) -> Duration {
        next_check_interval(
            self.config.check_interval(),
            self.shared.protection.status().lock.is_some(),
        )
    }

    pub(super) fn check_all_paths(&mut self) {
        let reasons = space_shortfalls(&self.paths, self.config.disk_lock_threshold_bytes);
        if !reasons.is_empty() {
            self.activate(reasons);
        } else if self.only_lacks_space() {
            let _ = self.recover();
        }
        self.shared.protection.update(|status| {
            status.completed_checks += 1;
            status.last_checked_at = Some(entities::Timestamp::now());
        });
        self.next_check = Instant::now().checked_add(self.check_interval());
    }

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

    pub(super) fn recover(&mut self) -> Result<RecoveryReport, RecoveryError> {
        match self.shared.admission() {
            Admission::Open => {
                self.publish_scheduling_change();
                self.shared.catalog_changes.publish();
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
            self.publish_scheduling_change();
            self.shared.catalog_changes.publish();
        }
        result
    }

    pub(super) fn attempt_recovery(&mut self) -> Result<RecoveryReport, RecoveryError> {
        let threshold = self.config.disk_resume_threshold_bytes;
        let mut available = Vec::with_capacity(self.paths.len());
        let mut shortages = Vec::new();
        for (area, path) in &self.paths {
            match disk::check_space(*area, path, 0, threshold) {
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

    pub(super) fn reopen(&mut self) -> Result<u64, RecoveryError> {
        self.shared.release_database();
        let OpenedFile { database, epoch } = redb::open(&self.directory)
            .map_err(|error| RecoveryError::Reopen(ReopenFailure::Storage(error)))?;
        let native_resources = redb::read_native_resource_catalog(&database)
            .map_err(|error| RecoveryError::Reopen(ReopenFailure::Storage(error)))?;
        *self
            .shared
            .native_resources
            .write()
            .unwrap_or_else(PoisonError::into_inner) = native_resources;
        *self
            .shared
            .database
            .write()
            .unwrap_or_else(PoisonError::into_inner) = Some(Opened { database, epoch });
        self.shared.epoch.store(epoch, Ordering::SeqCst);
        self.shared.next_operation.store(1, Ordering::SeqCst);
        self.shared.released.store(false, Ordering::SeqCst);
        self.storage_failure = None;
        self.needs_full_verification = false;
        self.shared
            .needs_full_verification
            .store(false, Ordering::SeqCst);
        Ok(epoch)
    }

    pub(super) fn note_uncertain(&mut self, operation: OperationId, probe: Option<Probe>) {
        self.uncertain.push((operation, probe));
        self.shared
            .probes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(operation, probe);
    }

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

    pub(super) fn exit(self) -> Result<(), DatabaseError> {
        {
            let mut admission = self
                .shared
                .admission
                .write()
                .unwrap_or_else(PoisonError::into_inner);
            *admission = Admission::ShuttingDown;
        }
        let opened = self
            .shared
            .database
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        // Opening durably cleared clean_shutdown. Leave it clear if a source read
        // detected corruption, even if that command rolled back, only declined,
        // or ran on the read pool.
        if self.storage_failure.is_none()
            && !self.needs_full_verification
            && !self.shared.needs_full_verification.load(Ordering::SeqCst)
            && let Some(opened) = opened.as_ref()
        {
            let _ = crate::db::redb::record_clean_shutdown(&opened.database);
        }
        drop(opened);
        self.shared.released.store(true, Ordering::SeqCst);
        for message in self.receiver.drain() {
            if let Message::Run(envelope) = message {
                let Envelope {
                    operation,
                    command,
                    in_flight,
                    ..
                } = *envelope;
                drop(in_flight);
                command.refuse(|| DatabaseError::NeverExecuted { operation });
            }
        }
        for maintenance in self.maintenance.drain() {
            maintenance.message.refuse_policy(false);
        }
        self.shared.protection.close();
        match self.storage_failure {
            None => Ok(()),
            Some(error) => Err(DatabaseError::StorageUnavailable(error)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::task::{Context, Poll, Wake, Waker};

    use ::redb::ReadableTable;
    use uuid::Uuid;

    use super::{DatabaseError, DatabaseWorker};
    use crate::db::entities::Project;
    use crate::db::{StorageErrorKind, database_file};
    use crate::host::data_directory::DataDirectory;

    const PROJECTS_TABLE: ::redb::TableDefinition<'_, u128, &[u8]> =
        ::redb::TableDefinition::new("project");

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(prefix: &str) -> Self {
            let path = std::env::temp_dir().join(format!("{prefix}-{}", Uuid::now_v7()));
            std::fs::create_dir_all(&path).expect("create temp dir");
            Self(path)
        }

        fn data_directory(&self) -> DataDirectory {
            DataDirectory::bootstrap(self.0.clone().into_os_string()).expect("bootstrap data dir")
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    struct Unpark(std::thread::Thread);

    impl Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }

    fn block_on<F: Future>(future: F) -> F::Output {
        let mut future = std::pin::pin!(future);
        let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
        loop {
            if let Poll::Ready(value) = future.as_mut().poll(&mut Context::from_waker(&waker)) {
                return value;
            }
            std::thread::park();
        }
    }

    fn corrupt_project_row(directory: &Path, project_id: u128) {
        let db = ::redb::Database::open(database_file(directory)).expect("open raw redb");
        let tx = db.begin_write().expect("begin write");
        {
            let mut table = tx.open_table(PROJECTS_TABLE).expect("open PROJECTS");
            let mut bytes = table
                .get(project_id)
                .expect("read PROJECTS row")
                .expect("project row exists")
                .value()
                .to_vec();
            assert!(bytes.pop().is_some(), "non-empty project record");
            table
                .insert(project_id, bytes.as_slice())
                .expect("corrupt PROJECTS row");
        }
        tx.commit().expect("commit corruption");
    }

    #[test]
    fn read_only_corruption_detection_forces_full_verification_on_next_startup() {
        let temp = TempDir::new("ocg-ro-corrupt");
        let directory = temp.data_directory();

        let worker = DatabaseWorker::start(&directory).expect("initial start");
        let project_id = block_on(worker.handle().create_project(Project {
            name: "alpha".to_owned(),
            description: None,
            root: "/tmp/alpha".to_owned(),
            commands: Vec::new(),
        }))
        .expect("create project");
        worker.shutdown().expect("initial clean shutdown");

        corrupt_project_row(&temp.0, project_id.get());

        // clean_shutdown = 1 -> Quick verification opens without scanning PROJECTS.
        let worker = DatabaseWorker::start(&directory).expect("quick startup before detection");
        let read_err = block_on(worker.handle().project(project_id))
            .expect_err("read-only projection must report corruption");
        assert!(
            matches!(
                read_err,
                DatabaseError::Read(ref storage) if storage.kind == StorageErrorKind::Corrupted
            ),
            "unexpected read error: {read_err:?}"
        );

        // No further writes; clean shutdown must not mark clean_shutdown = 1.
        worker
            .shutdown()
            .expect("clean shutdown after ro detection");

        // Next startup must take Full verification and refuse the corrupt database.
        let reopen_err = match DatabaseWorker::start(&directory) {
            Ok(_) => panic!("next startup must refuse corrupt database via Full verification"),
            Err(error) => error,
        };
        assert!(
            matches!(
                reopen_err,
                DatabaseError::Open(ref storage)
                    if storage.kind == StorageErrorKind::Corrupted
            ),
            "unexpected startup error: {reopen_err:?}"
        );
    }

    #[test]
    fn read_only_corruption_preserves_unrelated_writes_and_pause_boundary() {
        let temp = TempDir::new("ocg-ro-writes");
        let directory = temp.data_directory();

        let worker = DatabaseWorker::start(&directory).expect("initial start");
        let bad_project_id = block_on(worker.handle().create_project(Project {
            name: "corrupt-target".to_owned(),
            description: None,
            root: "/tmp/corrupt-target".to_owned(),
            commands: Vec::new(),
        }))
        .expect("create project");
        worker.shutdown().expect("initial clean shutdown");

        corrupt_project_row(&temp.0, bad_project_id.get());

        let worker = DatabaseWorker::start(&directory).expect("quick startup");
        let handle = worker.handle();

        // Trigger read-only corruption detection.
        let read_err = block_on(handle.project(bad_project_id))
            .expect_err("read-only projection must report corruption");
        assert!(matches!(
            read_err,
            DatabaseError::Read(ref storage) if storage.kind == StorageErrorKind::Corrupted
        ));

        // Unrelated writes and reads must remain available.
        let saved_headers = block_on(handle.set_global_outbound_headers(0, Vec::new()))
            .expect("unrelated write must remain available");
        assert_eq!(saved_headers.revision, 1);
        let read_headers = block_on(handle.global_outbound_headers())
            .expect("unrelated read must remain available");
        assert_eq!(read_headers.revision, 1);

        // pause_for_update -> drop(pause) must not clear needs_full_verification.
        let pause = handle
            .pause_for_update()
            .expect("pause_for_update")
            .expect("idle pause acquired");
        assert!(
            worker
                .handle
                .shared
                .needs_full_verification
                .load(Ordering::SeqCst)
        );
        drop(pause);
        assert!(
            worker
                .handle
                .shared
                .needs_full_verification
                .load(Ordering::SeqCst)
        );

        worker.shutdown().expect("clean shutdown");

        let reopen_err = match DatabaseWorker::start(&directory) {
            Ok(_) => panic!("next startup must refuse corrupt database"),
            Err(error) => error,
        };
        assert!(matches!(
            reopen_err,
            DatabaseError::Open(ref storage)
                if storage.kind == StorageErrorKind::Corrupted
        ));
    }
}
