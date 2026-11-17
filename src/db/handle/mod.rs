use super::{MAX_OBSERVATION_SCAN, ObservationChange};
use crate::db::attachments::{Reclamation, StoreError};
use crate::db::commands::{Command, Envelope, InFlightCommand, Maintenance, Message, Reply};
use crate::db::entities::{ContentDigest, JobId, OperationId, OperationKind};
use crate::db::errors::{
    CompactionOutcome, DatabaseError, DatabaseStatus, MaintenanceError, OperationStatus,
    UnknownCause,
};
use crate::db::redb::QueueCounts;
use crate::db::redb::StorageError;
use crate::db::scheduler_wake::SchedulerWakeSubscription;
use crate::db::shared::{Admission, Opened, ReadJob, ReadMessage, Shared, admitted};
use crate::db::worker::{DatabaseHandle, run};
use crate::db::{errors, redb};
use crate::host::data_directory::DataDirectory;
use crate::host::monitor::{ProtectionReason, ProtectionWatch};
use crate::host::policy::SystemPolicy;
use ::redb::Database;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, PoisonError};

mod catalog;
mod execution;
mod jobs;
mod observations;
mod scheduling;

struct AttachmentPermit(flume::Sender<()>);

pub(crate) struct UpdatePause(Arc<Shared>);

impl Drop for UpdatePause {
    fn drop(&mut self) {
        self.0.update_paused.store(false, Ordering::SeqCst);
        self.0.scheduler_wake.publish();
    }
}

impl Drop for AttachmentPermit {
    fn drop(&mut self) {
        let _ = self.0.try_send(());
    }
}

impl DatabaseHandle {
    pub(crate) fn wake_scheduler(&self) {
        self.shared.scheduler_wake.publish();
    }

    async fn attachment_permit(&self) -> Result<AttachmentPermit, DatabaseError> {
        self.shared
            .attachment_slots
            .1
            .recv_async()
            .await
            .map_err(|_| DatabaseError::ShuttingDown)?;
        Ok(AttachmentPermit(self.shared.attachment_slots.0.clone()))
    }

    pub fn durable_commits(&self) -> u64 {
        self.shared.durable_commits.load(Ordering::SeqCst)
    }

    pub fn commit_timing(&self) -> (Option<u64>, u64) {
        let durable_commits = self.shared.durable_commits.load(Ordering::Relaxed);
        let total = self.shared.commit_us_total.load(Ordering::Relaxed);
        (
            total.checked_div(durable_commits),
            self.shared.commit_us_max.load(Ordering::Relaxed),
        )
    }

    pub fn commands_waiting(&self) -> u64 {
        self.sender.len() as u64
    }

    pub(crate) async fn queue_counts(&self) -> Result<QueueCounts, DatabaseError> {
        self.read(|database| redb::queue_counts(database, false))
            .await
    }

    pub(crate) fn take_scheduler_wake(&self) -> Option<SchedulerWakeSubscription> {
        self.shared
            .scheduler_subscription
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }

    pub(crate) fn subscribe_catalog(&self) -> crate::support::notification::Subscription {
        self.shared.catalog_changes.subscribe()
    }

    pub(crate) fn subscribe_observations(&self) -> crate::support::notification::Subscription {
        self.shared.observation_changes.subscribe()
    }

    pub(crate) fn observation_cursor(&self) -> crate::support::notification::JournalCursor {
        self.shared.observation_changes.cursor()
    }

    pub(crate) fn observation_changes(
        &self,
        cursor: crate::support::notification::JournalCursor,
    ) -> Result<
        Vec<(
            crate::support::notification::JournalCursor,
            ObservationChange,
        )>,
        (),
    > {
        self.shared.observation_changes.after(cursor)
    }

    pub(crate) fn subscribe_outputs(
        &self,
        job: JobId,
    ) -> crate::support::notification::Subscription {
        self.shared.output_changes.subscribe(job.get())
    }

    pub(crate) fn data_directory(&self) -> DataDirectory {
        self.shared.data_directory.clone()
    }

    pub(crate) fn logging_protection_state(&self) -> (bool, u64) {
        self.shared.protection.logging_state()
    }

    pub(crate) fn activate_filesystem_protection(&self, reason: ProtectionReason) -> bool {
        {
            let mut admission = self
                .shared
                .admission
                .write()
                .unwrap_or_else(PoisonError::into_inner);
            if !matches!(*admission, Admission::Open | Admission::Protected(_)) {
                return false;
            }
            let lock = self.shared.protection.lock(vec![reason.clone()]);
            *admission = Admission::Protected(lock);
        }
        if let Err(error) = self
            .maintenance_sender
            .try_send(Maintenance::FilesystemFailure { reason })
        {
            log::debug!("failed to enqueue filesystem protection maintenance notice: {error}");
        }
        true
    }

    pub fn active_system_policy(&self) -> SystemPolicy {
        self.shared.active_policy
    }

    pub(crate) fn pause_for_update(&self) -> Result<Option<UpdatePause>, DatabaseError> {
        let _staging = self
            .shared
            .staging
            .read()
            .unwrap_or_else(PoisonError::into_inner);
        let database = self
            .shared
            .database
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        let admission = self
            .shared
            .admission
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        admitted(&admission)?;
        if self.shared.update_paused.load(Ordering::SeqCst) {
            return Err(DatabaseError::ShuttingDown);
        }
        let opened = database.as_ref().ok_or(DatabaseError::ReadsClosed)?;
        let counts = redb::queue_counts(&opened.database, true).map_err(DatabaseError::Read)?;
        if counts.pending != 0
            || counts.ready != 0
            || counts.delayed != 0
            || counts.running != 0
            || counts.held != 0
            || !self.sender.is_empty()
            || self.shared.in_flight_commands.load(Ordering::SeqCst) != 0
        {
            return Ok(None);
        }
        if crate::workspace::process::groups_tracked() {
            return Ok(None);
        }
        self.shared.update_paused.store(true, Ordering::SeqCst);
        Ok(Some(UpdatePause(self.shared.clone())))
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
            if self.shared.update_paused.load(Ordering::SeqCst) {
                return Err(DatabaseError::ShuttingDown);
            }
            let epoch = self.shared.epoch.load(Ordering::SeqCst);
            let operation = OperationId::new(
                epoch,
                self.shared.next_operation.fetch_add(1, Ordering::Relaxed),
            );
            let envelope = Envelope {
                operation,
                started: started.clone(),
                command: command(reply),
                in_flight: InFlightCommand::new(self.shared.clone()),
            };
            self.sender
                .try_send(Message::Run(Box::new(envelope)))
                .map_err(|error| match error {
                    flume::TrySendError::Full(_) => DatabaseError::QueueFull,
                    flume::TrySendError::Disconnected(_) => DatabaseError::ShuttingDown,
                })?;
            operation
        };
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

    async fn write<A: Send + 'static, T: Send + 'static>(
        &self,
        bytes: u64,
        kind: OperationKind,
        args: A,
        f: impl FnOnce(&mut redb::Batch, A) -> Result<T, redb::BatchError> + Send + 'static,
    ) -> Result<T, DatabaseError> {
        self.submit(|reply| Command::new(args, reply, |_| bytes, kind, move |a, b| f(b, a)))
            .await
    }

    async fn write_sized<A: Send + 'static, T: Send + 'static>(
        &self,
        args: A,
        size: impl FnOnce(&A) -> u64,
        kind: OperationKind,
        f: impl FnOnce(&mut redb::Batch, A) -> Result<T, redb::BatchError> + Send + 'static,
    ) -> Result<T, DatabaseError> {
        self.submit(|reply| Command::new(args, reply, size, kind, move |a, b| f(b, a)))
            .await
    }

    async fn write_output<A: Send + 'static, T: Send + 'static>(
        &self,
        job: JobId,
        args: A,
        size: impl FnOnce(&A) -> u64,
        kind: OperationKind,
        f: impl FnOnce(&mut redb::Batch, A) -> Result<T, redb::BatchError> + Send + 'static,
    ) -> Result<T, DatabaseError> {
        self.submit(|reply| {
            Command::custom(args, reply, size, move |args, reply, db, op, commit| {
                let mut outcome = run(db, op, commit, kind, reply, |batch| f(batch, args));
                if outcome.committed {
                    outcome.output_changed = Some(job);
                }
                outcome
            })
        })
        .await
    }

    pub(super) async fn read<T: Send + 'static>(
        &self,
        read: impl FnOnce(&Database) -> Result<T, StorageError> + Send + 'static,
    ) -> Result<T, DatabaseError> {
        self.read_opened(move |opened| read(&opened.database)).await
    }

    async fn read_opened<T: Send + 'static>(
        &self,
        read: impl FnOnce(&Opened) -> Result<T, StorageError> + Send + 'static,
    ) -> Result<T, DatabaseError> {
        let (reply, response) = flume::bounded(1);
        let job = {
            let admission = self
                .shared
                .admission
                .read()
                .unwrap_or_else(PoisonError::into_inner);
            admission.check_read()?;
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
            ReadMessage::Run(job)
        };
        self.read_sender
            .send_async(job)
            .await
            .map_err(|_| DatabaseError::ReadPoolStopped)?;
        response
            .recv_async()
            .await
            .map_err(|_| DatabaseError::ReadInterrupted)?
    }

    pub fn reclaimed_at_open(&self) -> &[(ContentDigest, Result<Reclamation, StoreError>)] {
        &self.shared.reclaimed_at_open
    }

    pub fn reclamation_omitted_at_open(&self) -> u64 {
        self.shared.reclamation_omitted_at_open
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

    pub fn epoch(&self) -> u64 {
        self.shared.epoch.load(Ordering::SeqCst)
    }

    pub fn watch_disk_protection(&self) -> ProtectionWatch {
        self.shared.protection.watch()
    }

    pub async fn compact_storage(&self) -> Result<CompactionOutcome, MaintenanceError> {
        let (reply, response) = flume::bounded(1);
        {
            let admission = self
                .shared
                .admission
                .read()
                .unwrap_or_else(PoisonError::into_inner);
            admission.check_maintenance()?;
            self.maintenance_sender
                .try_send(Maintenance::CompactStorage(reply))
                .map_err(|error| match error {
                    flume::TrySendError::Full(_) => MaintenanceError::MaintenanceQueueFull,
                    flume::TrySendError::Disconnected(_) => MaintenanceError::WorkerStopped,
                })?;
        }
        response
            .recv_async()
            .await
            .unwrap_or(Err(MaintenanceError::WorkerStopped))
    }

    pub(crate) async fn check_readable(&self) -> Result<(), DatabaseError> {
        self.read(redb::check_readable).await
    }

    pub async fn reconcile_operation(
        &self,
        operation: OperationId,
    ) -> Result<OperationStatus, DatabaseError> {
        if operation.epoch() > self.epoch() {
            return Err(DatabaseError::UnknownOperation(operation));
        }
        let probe = self
            .shared
            .probes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&operation)
            .copied()
            .flatten();
        self.read_opened(move |opened| {
            errors::operation_status(&opened.database, operation, probe, opened.epoch)
        })
        .await
    }
}
