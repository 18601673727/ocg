use super::*;

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
        self.0.scheduler_wake.publish(true, true);
    }
}

impl Drop for AttachmentPermit {
    fn drop(&mut self) {
        let _ = self.0.try_send(());
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Vitals {
    pub database_bytes: Option<u64>,
    pub available_bytes: Option<u64>,
    pub durable_commits: u64,
    pub commit_us_mean: Option<u64>,
    pub commit_us_max: u64,
    pub queue_pending: Option<u64>,
    pub queue_ready: Option<u64>,
    pub queue_delayed: Option<u64>,
    pub queue_running: Option<u64>,
    pub commands_waiting: u64,
    pub rss_bytes: Option<u64>,
    pub peak_rss_bytes: Option<u64>,
}

fn filesystem_vitals(directory: &std::path::Path) -> (Option<u64>, Option<u64>) {
    let db_file = crate::db::redb::database_file(directory);
    (
        std::fs::metadata(&db_file)
            .map(|metadata| metadata.len())
            .ok(),
        fs4::available_space(directory).ok(),
    )
}

async fn filesystem_vitals_async(directory: std::path::PathBuf) -> (Option<u64>, Option<u64>) {
    ntex::rt::spawn_blocking(move || filesystem_vitals(&directory))
        .await
        .unwrap_or((None, None))
}

fn process_memory() -> (Option<u64>, Option<u64>) {
    let mut peak = None;
    #[cfg(unix)]
    {
        let mut rusage: libc::rusage = unsafe { std::mem::zeroed() };
        if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut rusage) } == 0 {
            #[cfg(target_os = "macos")]
            {
                peak = Some(rusage.ru_maxrss.max(0) as u64);
            }
            #[cfg(not(target_os = "macos"))]
            {
                peak = Some((rusage.ru_maxrss.max(0) as u64).saturating_mul(1024));
            }
        }
    }

    #[allow(unused_mut)]
    let mut current = None;
    #[cfg(target_os = "linux")]
    {
        if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
            for line in status.lines() {
                if let Some(rest) = line.strip_prefix("VmRSS:") {
                    let parts: Vec<&str> = rest.split_whitespace().collect();
                    if let Some(kb) = parts.first().and_then(|s| s.parse::<u64>().ok()) {
                        current = Some(kb.saturating_mul(1024));
                    }
                }
                if let Some(rest) = line.strip_prefix("VmHWM:") {
                    let parts: Vec<&str> = rest.split_whitespace().collect();
                    if let Some(kb) = parts.first().and_then(|s| s.parse::<u64>().ok()) {
                        peak = Some(kb.saturating_mul(1024));
                    }
                }
            }
        }
    }

    (current, peak)
}

impl DatabaseHandle {
    pub(crate) fn wake_scheduler(&self) {
        self.shared.scheduler_wake.publish(false, true);
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

    pub async fn vitals(&self) -> Vitals {
        let directory = self.shared.database_directory.clone();
        let (database_bytes, available_bytes) = filesystem_vitals_async(directory).await;

        let queue_counts = match self
            .read(|database| redb::queue_counts(database, false))
            .await
        {
            Ok(counts) => (
                Some(counts.pending),
                Some(counts.ready),
                Some(counts.delayed),
                Some(counts.running),
            ),
            Err(error) => {
                log::debug!("failed to read database vitals queue counts: {error}");
                (None, None, None, None)
            }
        };

        let commands_waiting = self.sender.len() as u64;
        let (rss_bytes, peak_rss_bytes) = process_memory();

        let durable_commits = self.shared.durable_commits.load(Ordering::Relaxed);
        let commit_us_total = self.shared.commit_us_total.load(Ordering::Relaxed);
        let commit_us_max = self.shared.commit_us_max.load(Ordering::Relaxed);
        let commit_us_mean = commit_us_total.checked_div(durable_commits);

        Vitals {
            database_bytes,
            available_bytes,
            durable_commits,
            commit_us_mean,
            commit_us_max,
            queue_pending: queue_counts.0,
            queue_ready: queue_counts.1,
            queue_delayed: queue_counts.2,
            queue_running: queue_counts.3,
            commands_waiting,
            rss_bytes,
            peak_rss_bytes,
        }
    }

    pub(crate) fn take_scheduler_wake(&self) -> Option<SchedulerWakeSubscription> {
        self.shared
            .scheduler_subscription
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }

    pub(crate) fn subscribe_catalog(&self) -> crate::system::notification::Subscription {
        self.shared.catalog_changes.subscribe()
    }

    pub(crate) fn subscribe_observations(&self) -> crate::system::notification::Subscription {
        self.shared.observation_changes.subscribe()
    }

    pub(crate) fn observation_cursor(&self) -> crate::system::notification::JournalCursor {
        self.shared.observation_changes.cursor()
    }

    pub(crate) fn observation_changes(
        &self,
        cursor: crate::system::notification::JournalCursor,
    ) -> Result<
        Vec<(
            crate::system::notification::JournalCursor,
            ObservationChange,
        )>,
        (),
    > {
        self.shared.observation_changes.after(cursor)
    }

    pub(crate) fn subscribe_outputs(
        &self,
        job: JobId,
    ) -> crate::system::notification::Subscription {
        self.shared.output_changes.subscribe(job.get())
    }

    pub(crate) fn subscribe_logs(&self) -> crate::system::notification::Subscription {
        self.shared.log_changes.subscribe()
    }

    pub(crate) fn logs_changed(&self) {
        self.shared.log_changes.publish();
    }

    pub(crate) fn log_files(&self) -> Arc<crate::system::logger::LogFiles> {
        self.shared.log_files.clone()
    }

    pub(crate) fn data_directory(&self) -> DataDirectory {
        self.shared.data_directory.clone()
    }

    pub(crate) fn log_directory(&self) -> &std::path::Path {
        &self.shared.log_directory
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
        if crate::system::executor::blocking::groups_tracked() {
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
