use super::*;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Vitals {
    pub database_bytes: Option<u64>,
    pub available_bytes: Option<u64>,
    pub durable_commits: u64,
    pub commit_us_mean: Option<u64>,
    pub commit_us_max: u64,
    pub claims_count: u64,
    pub claim_us_mean: Option<u64>,
    pub claim_us_max: u64,
    pub queue_pending: Option<u64>,
    pub queue_ready: Option<u64>,
    pub queue_delayed: Option<u64>,
    pub commands_waiting: u64,
    pub rss_bytes: Option<u64>,
    pub peak_rss_bytes: Option<u64>,
}

/// The database file's size and the space free beside it. Both readings are
/// optional: a probe that fails stays unknown rather than becoming zero.
fn filesystem_vitals(directory: &std::path::Path) -> (Option<u64>, Option<u64>) {
    let db_file = crate::db::redb::database_file(directory);
    (
        std::fs::metadata(&db_file)
            .map(|metadata| metadata.len())
            .ok(),
        fs4::available_space(directory).ok(),
    )
}

/// Reads [`filesystem_vitals`] on a blocking worker, so a slow filesystem
/// never holds an async worker.
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
    pub(crate) async fn terminal_jobs(
        &self,
        jobs: Vec<JobId>,
    ) -> Result<Vec<JobId>, DatabaseError> {
        self.read(move |database| redb::terminal_jobs(database, &jobs))
            .await
    }

    /// Successful durable commits of command groups since this worker started.
    /// A flushed group counts once; refused or declined commands and failed
    /// commits do not count. Recovery and maintenance commits are excluded.
    pub fn durable_commits(&self) -> u64 {
        self.shared.durable_commits.load(Ordering::SeqCst)
    }

    pub async fn vitals(&self) -> Vitals {
        let directory = self.shared.database_directory.clone();
        let (database_bytes, available_bytes) = filesystem_vitals_async(directory).await;

        let queue_counts = match self.read(redb::queue_counts).await {
            Ok(counts) => (Some(counts.0), Some(counts.1), Some(counts.2)),
            Err(error) => {
                log::debug!("failed to read database vitals queue counts: {error}");
                (None, None, None)
            }
        };

        let commands_waiting = self.sender.len() as u64;
        let (rss_bytes, peak_rss_bytes) = process_memory();

        let durable_commits = self.shared.durable_commits.load(Ordering::Relaxed);
        let commit_us_total = self.shared.commit_us_total.load(Ordering::Relaxed);
        let commit_us_max = self.shared.commit_us_max.load(Ordering::Relaxed);
        let commit_us_mean = commit_us_total.checked_div(durable_commits);

        let claims_count = self.shared.claims.load(Ordering::Relaxed);
        let claim_us_total = self.shared.claim_us_total.load(Ordering::Relaxed);
        let claim_us_max = self.shared.claim_us_max.load(Ordering::Relaxed);
        let claim_us_mean = claim_us_total.checked_div(claims_count);

        Vitals {
            database_bytes,
            available_bytes,
            durable_commits,
            commit_us_mean,
            commit_us_max,
            claims_count,
            claim_us_mean,
            claim_us_max,
            queue_pending: queue_counts.0,
            queue_ready: queue_counts.1,
            queue_delayed: queue_counts.2,
            commands_waiting,
            rss_bytes,
            peak_rss_bytes,
        }
    }

    pub async fn tool_stats(
        &self,
    ) -> Result<Vec<crate::db::entities::ToolStatRecord>, DatabaseError> {
        self.read(redb::read_tool_stats).await
    }

    pub async fn usage_summary(
        &self,
        filter: crate::db::entities::UsageSummaryFilter,
        after: Option<crate::db::entities::UsageCursor>,
    ) -> Result<crate::db::entities::UsageSummaryResponse, DatabaseError> {
        self.read(move |database| redb::read_usage_summary(database, &filter, after))
            .await
    }

    pub async fn usage_buckets(
        &self,
        filter: crate::db::entities::UsageSummaryFilter,
        bucketing: crate::db::entities::UsageBucketing,
        after: Option<crate::db::entities::UsageCursor>,
    ) -> Result<crate::db::entities::UsageBucketResponse, DatabaseError> {
        self.read(move |database| redb::read_usage_buckets(database, &filter, &bucketing, after))
            .await
    }

    pub(crate) async fn usage_retries(
        &self,
        filter: crate::db::entities::UsageSummaryFilter,
        bucketing: crate::db::entities::UsageBucketing,
        after: Option<(u128, u128)>,
        limit: usize,
    ) -> Result<redb::UsageRetriesPage, DatabaseError> {
        self.read(move |database| {
            redb::read_usage_retries(database, &filter, bucketing, after, limit)
        })
        .await
    }

    pub(crate) fn take_scheduler_wake(&self) -> Option<SchedulerWakeSubscription> {
        self.shared
            .scheduler_subscription
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }

    pub(crate) async fn next_scheduled_delay(&self) -> Result<Option<Duration>, DatabaseError> {
        self.read(redb::next_scheduled_delay).await
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

    pub(crate) fn cleanup_worker(&self) -> Arc<AtomicBool> {
        self.shared.cleanup_worker.clone()
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

    pub(super) async fn read<T: Send + 'static>(
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
    /// when there was none; a file already gone is not listed. At most 1,000
    /// details are retained; see [`Self::reclamation_omitted_at_open`].
    pub fn reclaimed_at_open(&self) -> &[(ContentDigest, Result<Reclamation, StoreError>)] {
        &self.shared.reclaimed_at_open
    }

    /// Reclamation details omitted after the first 1,000 entries at startup.
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
            admission.check_maintenance()?;
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
        admission.check_recovery_enqueue()?;
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

    pub(crate) async fn job_execution(
        &self,
        turn: JobId,
    ) -> Result<
        (
            crate::db::entities::FrozenExecutionConfig,
            Option<ProjectId>,
            Option<ConfigId>,
        ),
        DatabaseError,
    > {
        self.read(move |database| redb::read_turn_execution(database, turn))
            .await
    }
    pub(crate) async fn read_turn_context(
        &self,
        turn: JobId,
        budget: usize,
    ) -> Result<redb::TurnContext, DatabaseError> {
        self.read(move |database| redb::read_turn_context(database, turn, budget))
            .await
    }
    pub(crate) async fn turn_compaction(
        &self,
        turn: JobId,
    ) -> Result<Option<(JobId, &'static str, crate::db::entities::Timestamp)>, DatabaseError> {
        self.read(move |database| redb::read_turn_compaction(database, turn))
            .await
    }
    pub async fn turn_entries(
        &self,
        turn: JobId,
    ) -> Result<Vec<(u32, crate::db::entities::JobInputEntry)>, DatabaseError> {
        self.read(move |database| redb::read_turn_entries(database, turn))
            .await
    }
    pub async fn conversation_children(
        &self,
        conversation: JobId,
        before: Option<JobId>,
        limit: usize,
    ) -> Result<Vec<redb::ChildConversationEntry>, DatabaseError> {
        self.read(move |database| {
            redb::read_conversation_children(database, conversation, before, limit)
        })
        .await
    }

    pub async fn group_limits(
        &self,
        conversation: JobId,
    ) -> Result<Option<(crate::db::entities::JobLimits, u8)>, DatabaseError> {
        self.read(move |database| redb::read_conversation_limits(database, conversation))
            .await
    }
    pub async fn conversation_turns(
        &self,
        conversation: JobId,
        before: Option<JobId>,
        limit: usize,
    ) -> Result<Vec<JobRecord>, DatabaseError> {
        self.read(move |database| {
            redb::read_conversation_turns(database, conversation, before, limit)
        })
        .await
    }
    pub async fn notices(
        &self,
        conversation: JobId,
        after: u32,
        limit: usize,
    ) -> Result<Vec<(u32, crate::db::entities::Notice)>, DatabaseError> {
        self.read(move |database| redb::read_notices(database, conversation, after, limit))
            .await
    }

    pub(crate) async fn job_details(
        &self,
        job: JobId,
    ) -> Result<Option<redb::JobDetails>, DatabaseError> {
        self.read(move |database| redb::read_job_details(database, job))
            .await
    }

    pub async fn job(&self, job: JobId) -> Result<Option<JobRecord>, DatabaseError> {
        self.read(move |database| redb::read_job(database, job))
            .await
    }

    /// Newest Jobs first, strictly before `before` when given; at most
    /// 1,000 per call.
    pub async fn jobs(
        &self,
        before: Option<JobId>,
        limit: usize,
    ) -> Result<Vec<JobRecord>, DatabaseError> {
        self.read(move |database| redb::read_jobs(database, before, limit))
            .await
    }

    /// Newest Jobs whose identity, name or objective contains every term,
    /// with cursor pagination.
    pub(crate) async fn search_jobs(
        &self,
        terms: Vec<String>,
        before: Option<JobId>,
        limit: usize,
    ) -> Result<redb::JobSearchPage, DatabaseError> {
        self.read(move |database| redb::read_job_search(database, &terms, before, limit))
            .await
    }

    /// One page of conversations by immutable conversation ID, newest first.
    pub async fn conversations(
        &self,
        before: Option<JobId>,
        limit: usize,
    ) -> Result<redb::ConversationsPage, DatabaseError> {
        self.read(move |database| redb::read_conversations(database, before, limit))
            .await
    }

    /// Conversations by immutable ID, newest first, whose title or message and result text holds
    /// every term. `terms` are already lowercased query terms. `before` and
    /// `limit` page like [`Self::conversations`].
    pub async fn search_conversations(
        &self,
        terms: Vec<String>,
        before: Option<JobId>,
        limit: usize,
    ) -> Result<redb::ConversationsPage, DatabaseError> {
        self.read(move |database| redb::read_conversation_search(database, &terms, before, limit))
            .await
    }

    pub async fn claim_execution(
        &self,
        request: ClaimRequest,
    ) -> Result<Result<ClaimGrant, ClaimRefusal>, DatabaseError> {
        self.submit(|reply| Command::ClaimExecution { request, reply })
            .await
    }

    /// CLAIM_BATCH (section 8.3): one write transaction that promotes the due
    /// Jobs and claims ready Jobs, smallest Job id first, while a global slot
    /// and a compatible Pool with room are free. The Jobs it returns are
    /// claimed once this returns; execution starts only after the commit.
    pub async fn claim_batch(&self) -> Result<Vec<JobId>, DatabaseError> {
        let started = Instant::now();
        let jobs = self.submit(|reply| Command::ClaimBatch { reply }).await?;
        if !jobs.is_empty() {
            let duration_us = started.elapsed().as_micros() as u64;
            self.shared.claims.fetch_add(1, Ordering::Relaxed);
            self.shared
                .claim_us_total
                .fetch_add(duration_us, Ordering::Relaxed);
            self.shared
                .claim_us_max
                .fetch_max(duration_us, Ordering::Relaxed);
        }
        Ok(jobs)
    }

    pub async fn frozen_pool_binding(
        &self,
        job: JobId,
    ) -> Result<Option<FrozenPoolBinding>, DatabaseError> {
        self.read(move |database| redb::read_pool_binding(database, job))
            .await
    }

    /// Every execution whose external work OCG never credibly confirmed
    /// stopped, and the capacity each one still holds. Read-only and never a
    /// route to release any of it: see
    /// [`mutations::confirm_job_stopped`](crate::db::mutations::confirm_job_stopped).
    pub async fn unresolved_executions(
        &self,
    ) -> Result<Vec<(JobId, crate::db::redb::UnresolvedExecution)>, DatabaseError> {
        self.read(redb::read_unresolved_executions).await
    }
    pub async fn occupancy(&self) -> Result<Vec<(ExecutionPoolId, JobId)>, DatabaseError> {
        self.read(redb::read_occupancy).await
    }

    /// A Job's ordinary children in ID order, strictly after `after` when
    /// given; at most 1,000 per call. The Genesis Job is not its own child.
    pub async fn children(
        &self,
        parent: JobId,
        after: Option<JobId>,
        limit: usize,
    ) -> Result<Vec<JobId>, DatabaseError> {
        self.read(move |database| redb::read_children(database, parent, after, limit))
            .await
    }

    /// The tree of the work below a Job: the ancestors above it and the
    /// nodes below it, the root's own children in one page. `None` when the
    /// root has no Job of its own.
    pub async fn job_tree(
        &self,
        job: JobId,
        after: Option<JobId>,
        limit: usize,
        depth: u8,
    ) -> Result<Option<crate::db::redb::JobTree>, DatabaseError> {
        self.read(move |database| redb::read_job_tree(database, job, after, limit, depth))
            .await
    }

    /// What a submitted LLM execution child was configured to execute, as
    /// frozen in its input, with the Profiles it was submitted with as
    /// provenance. Later Profile edits do not change it. `None` for a Job
    /// submitted without a configuration.
    pub(crate) async fn job_scope(
        &self,
        job: JobId,
    ) -> Result<Option<redb::JobScope>, DatabaseError> {
        self.read(move |database| redb::read_job_scope(database, job))
            .await
    }

    pub async fn job_configuration(
        &self,
        job: JobId,
    ) -> Result<Option<entities::SubmittedConfiguration>, DatabaseError> {
        self.read(move |database| redb::read_job_configuration(database, job))
            .await
    }

    /// The typed work of a decision turn, or `None` for every other Job. A
    /// caller that receives `Some` must run the decision runtime, not the
    /// generative one.
    pub async fn decision_work(
        &self,
        job: JobId,
    ) -> Result<Option<entities::DecisionWork>, DatabaseError> {
        self.read(move |database| redb::read_decision_work(database, job))
            .await
    }

    /// The Executors that support the transport of an LLM Profile's Provider
    /// as configured now, oldest first, at most 1,000. This lists candidates
    /// for new work only; it neither ranks nor reserves them, and running on
    /// any of them never changes the Provider, model or parameters.
    pub async fn compatible_executors(
        &self,
        llm_profile: crate::db::entities::ProfileId,
    ) -> Result<Vec<ExecutorId>, DatabaseError> {
        self.read(move |database| redb::read_compatible_executors(database, llm_profile))
            .await
    }

    /// The Executors that offer typed decision work, oldest first, at most
    /// 1,000. These are the candidates for new decision Jobs; running on any
    /// of them never changes the Decision Model or its request.
    pub async fn decision_executors(&self) -> Result<Vec<ExecutorId>, DatabaseError> {
        self.read(redb::read_decision_executors).await
    }

    /// The Jobs that explicitly retry `original`, oldest first.
    pub async fn retries(
        &self,
        original: JobId,
        after: Option<JobId>,
        limit: usize,
    ) -> Result<redb::CursorPage<JobId, JobId>, DatabaseError> {
        self.read(move |database| redb::read_retries(database, original, after, limit))
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
        self.read(move |database| redb::read_outputs(database, job, after_seq, limit))
            .await
    }

    pub async fn latest_output(
        &self,
        job: JobId,
    ) -> Result<Option<JobOutputRecord>, DatabaseError> {
        self.read(move |database| redb::read_latest_output(database, job))
            .await
    }

    pub async fn job_input(
        &self,
        id: JobInputId,
    ) -> Result<Option<entities::JobInputRecord>, DatabaseError> {
        self.read(move |database| redb::read_job_input(database, id))
            .await
    }

    pub async fn job_input_for_job(
        &self,
        job: JobId,
    ) -> Result<Option<entities::JobInputRecord>, DatabaseError> {
        self.read(move |database| redb::read_job_input_for_job(database, job))
            .await
    }

    pub async fn executor(
        &self,
        id: ExecutorId,
    ) -> Result<Option<entities::ExecutorRecord>, DatabaseError> {
        self.read(move |database| redb::read_executor(database, id))
            .await
    }

    /// Executors in reverse ID order, newest first. Strictly before `before` when given;
    /// at most 1,000 per call.
    pub async fn executors(
        &self,
        before: Option<ExecutorId>,
        limit: usize,
    ) -> Result<Vec<entities::ExecutorRecord>, DatabaseError> {
        self.read(move |database| redb::read_executors(database, before, limit))
            .await
    }

    pub async fn project(&self, id: ProjectId) -> Result<Option<ProjectRecord>, DatabaseError> {
        self.read(move |database| redb::read_project(database, id))
            .await
    }

    /// Projects newest first, strictly before `before` when given; at most
    /// 1,000 per call.
    pub async fn projects(
        &self,
        before: Option<ProjectId>,
        limit: usize,
    ) -> Result<Vec<ProjectRecord>, DatabaseError> {
        self.read(move |database| redb::read_projects(database, before, limit))
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
        self.read(move |database| redb::read_job_metrics(database, job, after, limit))
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
        self.read(move |database| redb::read_job_availability(database, job, after, limit))
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
        after: Option<ObservationCursor>,
        limit: usize,
    ) -> Result<ObservationPage<MetricsRecord>, DatabaseError> {
        self.read(move |database| {
            redb::read_metrics(database, &filter, after, limit, MAX_OBSERVATION_SCAN)
        })
        .await
    }

    /// One page of the AvailabilityStats `filter` selects, paged like
    /// [`Self::metrics`]. These are raw observations; no availability
    /// verdict or rate is derived from them here.
    pub async fn availability_stats(
        &self,
        filter: ObservationFilter,
        after: Option<ObservationCursor>,
        limit: usize,
    ) -> Result<ObservationPage<AvailabilityStatRecord>, DatabaseError> {
        self.read(move |database| {
            redb::read_availability_stats(database, &filter, after, limit, MAX_OBSERVATION_SCAN)
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
        self.read(move |database| redb::read_submission(database, key))
            .await
    }

    /// Resolves a command reported as [`DatabaseError::OutcomeUnknown`] by
    /// reading the natural key it writes (section 11.2). Call it after
    /// reopening: for an operation of an earlier opening, a missing key proves
    /// the command did not commit. A command that writes no key to read is
    /// [`OperationStatus::Unresolvable`].
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

    /// T1: submits a Job, or the next turn of a conversation when it names a
    /// predecessor in `previous`. The Job is pending; the claim batch grants
    /// it when a compatible Pool has room.
    pub async fn create_job(&self, job: Job) -> Result<JobId, DatabaseError> {
        self.submit(|reply| Command::CreateJob { job, reply }).await
    }

    #[doc(hidden)]
    pub async fn bench_create_dependent_turn(&self, job: Job) -> Result<JobId, DatabaseError> {
        self.submit(|reply| Command::BenchCreateDependentTurn { job, reply })
            .await
    }

    pub async fn create_submitted_job(
        &self,
        job: Job,
        submission: Submission,
    ) -> Result<JobId, DatabaseError> {
        self.submit(|reply| Command::CreateSubmittedJob {
            job,
            submission,
            reply,
        })
        .await
    }

    /// A config, whatever its kind.
    pub async fn config(&self, id: ConfigId) -> Result<Option<ConfigRecord>, DatabaseError> {
        self.read(move |database| redb::read_config(database, id))
            .await
    }

    /// Configs of `kind` (every kind when `None`), newest first, strictly
    /// before `before` when given; at most 1,000 rows are examined per call.
    /// Archived configs are included; an empty page can have a continuation.
    pub async fn configs(
        &self,
        kind: Option<ConfigKind>,
        before: Option<ConfigId>,
        limit: usize,
    ) -> Result<redb::CursorPage<ConfigRecord, ConfigId>, DatabaseError> {
        self.read(move |database| redb::read_configs(database, kind, before, limit))
            .await
    }

    pub(crate) async fn find_config(
        &self,
        kind: ConfigKind,
        matches: impl Fn(&ConfigRecord) -> bool,
    ) -> Result<Option<ConfigRecord>, DatabaseError> {
        let mut before = None;
        loop {
            let page = self.configs(Some(kind), before, 1_000).await?;
            if let Some(record) = page.records.into_iter().find(&matches) {
                return Ok(Some(record));
            }
            before = page.next;
            if before.is_none() {
                return Ok(None);
            }
        }
    }

    /// One bounded page of active Profile configs (Models and Agents), newest
    /// first and strictly before `before`, with the Connections those Models
    /// name and their compatible Executors read in the same transaction.
    /// `scan` bounds the configs examined, so an empty page can still advance
    /// its `next_before`; `None` there means the configs were exhausted.
    pub(crate) async fn profile_page(
        &self,
        before: Option<ConfigId>,
        limit: usize,
        scan: usize,
    ) -> Result<redb::ProfilePageRecords, DatabaseError> {
        self.read(move |database| redb::read_profile_page(database, before, limit, scan))
            .await
    }

    /// Creates a Connection, Model, Agent or Pool config. Its references must
    /// name configs of the kinds its body requires.
    pub async fn create_config(&self, config: Config) -> Result<ConfigId, DatabaseError> {
        self.submit(|reply| Command::CreateConfig { config, reply })
            .await
    }

    pub async fn refresh_model_metadata(
        &self,
        connection: crate::db::entities::ConnectionConfig,
        models: Vec<crate::db::entities::DiscoveredModel>,
    ) -> Result<(), DatabaseError> {
        self.submit(|reply| Command::RefreshModelMetadata {
            connection,
            models,
            reply,
        })
        .await
    }

    /// Imports missing models and their Provider connection in one transaction.
    pub async fn import_models(
        &self,
        connection: Config,
        models: Vec<crate::db::entities::DiscoveredModel>,
        capabilities: std::collections::BTreeSet<crate::db::entities::ModelCapability>,
    ) -> Result<(Vec<ConfigId>, usize), DatabaseError> {
        self.submit(|reply| Command::ImportModels {
            connection,
            models,
            capabilities,
            reply,
        })
        .await
    }

    /// Archives a model and removes it from Agent choices, preserving frozen Jobs.
    pub async fn global_compaction_settings(
        &self,
    ) -> Result<crate::db::entities::GlobalCompactionSettings, DatabaseError> {
        self.read(redb::read_global_compaction_settings).await
    }

    pub async fn set_global_compaction_settings(
        &self,
        settings: crate::db::entities::GlobalCompactionSettings,
    ) -> Result<(), DatabaseError> {
        self.submit(|reply| Command::SetGlobalCompactionSettings { settings, reply })
            .await
    }

    pub async fn archive_model(&self, id: ConfigId) -> Result<(), DatabaseError> {
        self.submit(|reply| Command::ArchiveModel { id, reply })
            .await
    }

    /// Edits a config in place: a new name and body of the same kind. Every
    /// edit is allowed and raises the edit counter; Jobs that froze the old
    /// values keep them.
    pub async fn update_config(&self, id: ConfigId, config: Config) -> Result<(), DatabaseError> {
        self.submit(|reply| Command::UpdateConfig { id, config, reply })
            .await
    }

    pub async fn decide_approval(
        &self,
        approval: ToolCallRef,
        decision: entities::ApprovalDecision,
    ) -> Result<Appended, DatabaseError> {
        self.submit(|reply| Command::DecideApproval {
            approval,
            decision,
            reply,
        })
        .await
    }
    pub async fn has_decider(&self, conversation: Option<JobId>) -> Result<bool, DatabaseError> {
        self.read(move |database| redb::read_has_decider(database, conversation))
            .await
    }
    pub(crate) async fn delegated_workspace(&self, job: JobId) -> Result<bool, DatabaseError> {
        self.read(move |database| redb::read_delegated_workspace(database, job))
            .await
    }
    pub async fn approved_calls(
        &self,
        conversation: JobId,
    ) -> Result<Vec<ToolEvidenceRecord>, DatabaseError> {
        self.read(move |database| redb::read_approved_calls(database, conversation))
            .await
    }
    pub(crate) async fn has_pending_approvals(
        &self,
        conversation: JobId,
    ) -> Result<bool, DatabaseError> {
        self.read(move |database| redb::read_has_pending_approvals(database, conversation))
            .await
    }
    pub async fn conversation_cards(
        &self,
        conversation: JobId,
    ) -> Result<Vec<serde_json::Value>, DatabaseError> {
        self.read(move |database| redb::read_cards(database, conversation))
            .await
    }
    pub async fn apply_job_tool(
        &self,
        call: ToolCallRef,
        tool: entities::JobToolCall,
    ) -> Result<serde_json::Value, DatabaseError> {
        self.submit(|reply| Command::ApplyJobTool { call, tool, reply })
            .await
    }
    pub async fn boundary_b(&self, clock: i64) -> Result<(), DatabaseError> {
        self.submit(|reply| Command::BoundaryB { clock, reply })
            .await
    }
    pub(crate) async fn recurrence_due(&self, clock: i64) -> Result<bool, DatabaseError> {
        self.read(move |database| redb::recurrence_due(database, clock))
            .await
    }
    pub async fn open_group(
        &self,
        parent: Option<JobId>,
        spec: JobGroup,
        first: Entry,
        configuration: ExecutionConfiguration,
        submission: Option<Submission>,
    ) -> Result<(JobId, Appended), DatabaseError> {
        self.open_group_with_format(parent, spec, first, configuration, submission, None)
            .await
    }
    pub(crate) async fn open_group_with_format(
        &self,
        parent: Option<JobId>,
        spec: JobGroup,
        first: Entry,
        configuration: ExecutionConfiguration,
        submission: Option<Submission>,
        format: Option<crate::db::entities::ResponseFormat>,
    ) -> Result<(JobId, Appended), DatabaseError> {
        self.submit(|reply| Command::Open {
            parent,
            spec,
            first,
            configuration,
            submission,
            format,
            reply,
        })
        .await
    }
    pub(crate) async fn context_state(
        &self,
        conversation: JobId,
        before: Option<JobId>,
        limit: usize,
    ) -> Result<Option<redb::ContextState>, DatabaseError> {
        self.read(move |database| redb::read_context_state(database, conversation, before, limit))
            .await
    }
    pub(crate) async fn preparation_current(&self, job: JobId) -> Result<bool, DatabaseError> {
        self.read(move |database| redb::read_preparation_current(database, job))
            .await
    }
    pub(crate) async fn prediction_current(&self, job: JobId) -> Result<bool, DatabaseError> {
        self.read(move |database| redb::read_prediction_current(database, job))
            .await
    }
    pub(crate) async fn promote_prediction(
        &self,
        job: JobId,
    ) -> Result<Option<JobId>, DatabaseError> {
        self.submit(|reply| Command::PromotePrediction { job, reply })
            .await
    }
    pub(crate) async fn predict_context(
        &self,
        conversation: JobId,
    ) -> Result<Option<JobId>, DatabaseError> {
        self.submit(|reply| Command::PredictContext {
            conversation,
            reply,
        })
        .await
    }
    pub(crate) async fn effective_compaction_policy(
        &self,
        conversation: JobId,
    ) -> Result<Option<(entities::CompactionPolicy, u64, Option<entities::ConfigRef>)>, DatabaseError>
    {
        self.read(move |database| redb::read_effective_context_policy(database, conversation))
            .await
    }
    pub(crate) async fn summarization_input(
        &self,
        job: JobId,
    ) -> Result<Option<redb::SummarizationInput>, DatabaseError> {
        self.read(move |database| redb::read_summarization_input(database, job))
            .await
    }
    /// Opens a system conversation with one typed decision turn and freezes
    /// its Decision Model and Profile in the same transaction.
    pub(crate) async fn submit_decision(
        &self,
        parent: Option<JobId>,
        spec: JobGroup,
        first: Entry,
        configuration: ExecutionConfiguration,
        request: entities::DecisionRequest,
        submission: Option<Submission>,
    ) -> Result<(JobId, Appended), DatabaseError> {
        self.submit(|reply| Command::SubmitDecision {
            parent,
            spec,
            first,
            configuration,
            request,
            submission,
            reply,
        })
        .await
    }
    /// Requests one manual compaction of `conversation` through generative
    /// `model`. The submission key returns the same Job on repeat. A source
    /// that will not fit the model's window is refused, not truncated.
    pub async fn request_compaction(
        &self,
        conversation: JobId,
        model: ConfigId,
        submission: Submission,
    ) -> Result<entities::CompactionRequest, DatabaseError> {
        self.submit(|reply| Command::RequestCompaction {
            conversation,
            model,
            submission,
            reply,
        })
        .await
    }

    pub async fn append(
        &self,
        conversation: JobId,
        entry: Entry,
        freeze: Option<ExecutionConfiguration>,
        submission: Option<Submission>,
    ) -> Result<Appended, DatabaseError> {
        self.append_with_format(conversation, entry, freeze, submission, None)
            .await
    }
    pub(crate) async fn append_with_format(
        &self,
        conversation: JobId,
        entry: Entry,
        freeze: Option<ExecutionConfiguration>,
        submission: Option<Submission>,
        format: Option<crate::db::entities::ResponseFormat>,
    ) -> Result<Appended, DatabaseError> {
        self.submit(|reply| Command::Append {
            conversation,
            entry,
            freeze,
            submission,
            format,
            reply,
        })
        .await
    }
    pub(crate) async fn amend_queued_input(
        &self,
        conversation: JobId,
        turn: JobId,
        entry: Entry,
    ) -> Result<Appended, DatabaseError> {
        self.submit(|reply| Command::AmendQueuedInput {
            conversation,
            turn,
            entry,
            reply,
        })
        .await
    }
    pub(crate) async fn steer_with_format(
        &self,
        conversation: JobId,
        entry: Entry,
        freeze: Option<ExecutionConfiguration>,
        submission: Option<Submission>,
        format: Option<crate::db::entities::ResponseFormat>,
        automatic: bool,
    ) -> Result<(Appended, Vec<(JobId, crate::db::entities::StopCause)>), DatabaseError> {
        self.submit(|reply| Command::Steer {
            conversation,
            entry,
            freeze,
            submission,
            format,
            automatic,
            reply,
        })
        .await
    }
    pub async fn stop_turn(
        &self,
        conversation: JobId,
        by: Actor,
    ) -> Result<JobAction, DatabaseError> {
        self.submit(|reply| Command::StopTurn {
            conversation,
            by,
            reply,
        })
        .await
    }
    pub async fn close(&self, conversation: JobId, by: Actor) -> Result<JobAction, DatabaseError> {
        self.submit(|reply| Command::Close {
            conversation,
            by,
            reply,
        })
        .await
    }
    pub async fn hold(&self, conversation: JobId, by: Actor) -> Result<JobAction, DatabaseError> {
        self.submit(|reply| Command::Hold {
            conversation,
            by,
            reply,
        })
        .await
    }
    pub async fn unhold(&self, conversation: JobId, by: Actor) -> Result<JobAction, DatabaseError> {
        self.submit(|reply| Command::Unhold {
            conversation,
            by,
            reply,
        })
        .await
    }
    pub async fn retry(
        &self,
        conversation: JobId,
        turn: JobId,
        by: Actor,
    ) -> Result<Appended, DatabaseError> {
        self.submit(|reply| Command::Retry {
            conversation,
            turn,
            by,
            reply,
        })
        .await
    }

    pub async fn append_job_output(
        &self,
        job: JobId,
        output: JobOutput,
    ) -> Result<AppendedOutput, DatabaseError> {
        self.submit(|reply| Command::AppendJobOutput { job, output, reply })
            .await
    }

    pub(crate) fn with_stop_cause(
        &self,
        cause: Arc<std::sync::Mutex<Option<crate::db::entities::StopCause>>>,
    ) -> Self {
        let mut handle = self.clone();
        handle.stop_cause = Some(cause);
        handle.ignore_stop_cause = false;
        handle
    }

    /// Whether both handles reach the same database.
    pub(crate) fn same_database(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.shared, &other.shared)
    }

    pub(crate) fn without_stop_cause(&self) -> Self {
        let mut handle = self.clone();
        handle.stop_cause = None;
        handle.ignore_stop_cause = true;
        handle
    }

    pub async fn finalize_job(
        &self,
        job: JobId,
        finalization: JobFinalization,
    ) -> Result<Option<AppendedOutput>, DatabaseError> {
        let cause = self
            .stop_cause
            .as_ref()
            .and_then(|cause| *cause.lock().unwrap_or_else(PoisonError::into_inner))
            .map(|cause| match cause {
                crate::db::entities::StopCause::Stopped(record) => {
                    crate::db::entities::OutcomeCause::Stopped { record }
                }
                crate::db::entities::StopCause::Closed => crate::db::entities::OutcomeCause::Closed,
            });
        self.submit(|reply| Command::FinalizeJob {
            job,
            finalization,
            cause,
            ignore_stop_cause: self.ignore_stop_cause,
            reply,
        })
        .await
    }

    pub async fn create_executor(&self, executor: Executor) -> Result<ExecutorId, DatabaseError> {
        self.submit(|reply| Command::CreateExecutor { executor, reply })
            .await
    }

    pub async fn create_project(&self, project: Project) -> Result<ProjectId, DatabaseError> {
        self.submit(|reply| Command::CreateProject { project, reply })
            .await
    }

    pub(crate) async fn assess_command_grant(
        &self,
        call: ToolCallRef,
    ) -> Result<Option<JobId>, DatabaseError> {
        self.submit(|reply| Command::AssessCommandGrant { call, reply })
            .await
    }

    pub(crate) async fn configure_project_decision(
        &self,
        id: ProjectId,
        expected: crate::db::entities::Timestamp,
        assignment: Option<entities::ProjectDecisionConfig>,
    ) -> Result<entities::ProjectRecord, DatabaseError> {
        self.submit(|reply| Command::ConfigureProjectDecision {
            id,
            expected,
            assignment,
            reply,
        })
        .await
    }

    /// Replaces a Project's name, description and commands. Its root never
    /// changes.
    pub async fn update_project(
        &self,
        id: ProjectId,
        name: String,
        description: Option<String>,
        commands: Vec<ProjectCommand>,
    ) -> Result<(), DatabaseError> {
        self.submit(|reply| Command::UpdateProject {
            id,
            name,
            description,
            commands,
            reply,
        })
        .await
    }

    /// Renames or hides the conversation whose root Job is `root`.
    pub async fn set_conversation(&self, root: JobId, edit: JobEdit) -> Result<(), DatabaseError> {
        self.submit(|reply| Command::SetConversation { root, edit, reply })
            .await
    }

    /// Stores `attachment` as a new user-provided Attachment. Its content is
    /// published durably first, see [`crate::db::attachments`]; only then is the
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
        attachment: Attachment,
    ) -> Result<AttachmentId, AttachmentError> {
        mutations::check_new_attachment(&attachment).map_err(DatabaseError::Rejected)?;
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
        self.read(move |database| redb::read_attachment(database, id))
            .await
    }

    /// Attachments in reverse ID order; newest first, cleaned up or not.
    /// Strictly before `before` when given; at most 1,000 per call.
    pub async fn attachments(
        &self,
        before: Option<AttachmentId>,
        limit: usize,
    ) -> Result<Vec<AttachmentRecord>, DatabaseError> {
        self.read(move |database| redb::read_attachments(database, before, limit))
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
    /// see [`crate::db::attachments`]. The Attachment keeps its identity and
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
            shared
                .attachments
                .reclaim(digest, length, || redb::content_available(database, digest))
        })
        .await
        .map_err(ReclamationError::Database)?
        .map_err(ReclamationError::Store)
    }

    /// Records that an unknown-outcome Job's execution has actually stopped,
    /// releasing the slot it holds; the outcome stays unknown. Only the
    /// trusted execution path that observed the termination may call this,
    /// so it is not part of the public API.
    pub(crate) async fn confirm_job_stopped(
        &self,
        job: JobId,
        proof: StopProof,
    ) -> Result<(), DatabaseError> {
        self.submit(|reply| Command::ConfirmJobStopped { job, proof, reply })
            .await
    }

    /// Appends measurements for a Job. Their LLM is the one the Job froze.
    pub async fn record_metrics(&self, metrics: Metrics) -> Result<MetricsId, DatabaseError> {
        self.submit(|reply| Command::RecordMetrics {
            metrics: Box::new(metrics),
            reply,
        })
        .await
    }

    pub async fn record_sent_request(&self, sent: SentRequest) -> Result<(), DatabaseError> {
        self.submit(|reply| Command::RecordSentRequest {
            sent: Box::new(sent),
            reply,
        })
        .await
    }

    pub async fn record_provider_answer(
        &self,
        answer: ProviderAnswer,
    ) -> Result<(), DatabaseError> {
        self.submit(|reply| Command::RecordProviderAnswer {
            answer: Box::new(answer),
            reply,
        })
        .await
    }

    pub(crate) async fn provider_responses(
        &self,
        job: JobId,
        after: Option<RequestNumber>,
        limit: usize,
        max_text_bytes: usize,
    ) -> Result<redb::ProviderResponsesPage, DatabaseError> {
        self.read(move |database| {
            redb::read_provider_responses(database, job, after, limit, max_text_bytes)
        })
        .await
    }

    pub async fn provider_request(
        &self,
        job: JobId,
        request: RequestNumber,
    ) -> Result<Option<ProviderRequestRecord>, DatabaseError> {
        self.read(move |database| redb::read_provider_request(database, job, request))
            .await
    }

    /// Appends one availability observation of an LLM request a configured
    /// Job sent. Its LLM is the one the Job froze.
    pub async fn record_availability_stat(
        &self,
        stat: AvailabilityStat,
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
    /// this returns `Ok`. See [`mutations::record_tool_decision`].
    pub(crate) async fn record_tool_decision(
        &self,
        evidence: ToolEvidence,
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

    /// [`Self::record_tool_decision`] for the backend benchmark, which drives
    /// the trusted execution path itself. Not part of the API.
    #[doc(hidden)]
    pub async fn bench_record_tool_decision(
        &self,
        evidence: ToolEvidence,
    ) -> Result<(), DatabaseError> {
        self.record_tool_decision(evidence).await
    }

    /// [`Self::record_tool_outcome`] for the backend benchmark, which drives
    /// the trusted execution path itself. Not part of the API.
    #[doc(hidden)]
    pub async fn bench_record_tool_outcome(
        &self,
        call: ToolCallRef,
        outcome: ToolOutcome,
    ) -> Result<(), DatabaseError> {
        self.record_tool_outcome(call, outcome).await
    }

    /// A Job's tool evidence in call order, after the call named by its
    /// request and index when given; at most 1,000 per call.
    pub async fn tool_evidence(
        &self,
        job: JobId,
        after: Option<(RequestNumber, u32)>,
        limit: usize,
    ) -> Result<Vec<ToolEvidenceRecord>, DatabaseError> {
        self.read(move |database| redb::read_tool_evidence(database, job, after, limit))
            .await
    }
}

impl DatabaseHandle {
    pub async fn create_execution_pool(
        &self,
        pool: ExecutionPool,
    ) -> Result<ExecutionPoolId, DatabaseError> {
        self.submit(|reply| Command::CreateExecutionPool { pool, reply })
            .await
    }

    pub async fn set_execution_pool_enabled(
        &self,
        id: ExecutionPoolId,
        enabled: bool,
    ) -> Result<(), DatabaseError> {
        self.submit(|reply| Command::SetExecutionPoolEnabled { id, enabled, reply })
            .await
    }

    /// Replaces a Pool's capabilities and concurrency limit.
    pub async fn update_execution_pool(
        &self,
        id: ExecutionPoolId,
        config: ExecutionPoolConfig,
    ) -> Result<(), DatabaseError> {
        self.submit(|reply| Command::UpdateExecutionPool { id, config, reply })
            .await
    }

    pub async fn execution_pool(
        &self,
        id: ExecutionPoolId,
    ) -> Result<Option<ExecutionPoolRecord>, DatabaseError> {
        self.read(move |database| redb::read_execution_pool(database, id))
            .await
    }

    pub async fn execution_pools(
        &self,
        before: Option<ExecutionPoolId>,
        limit: usize,
    ) -> Result<Vec<ExecutionPoolRecord>, DatabaseError> {
        self.read(move |database| redb::read_execution_pools(database, before, limit))
            .await
    }

    pub async fn set_credential(&self, reference: &str, secret: &str) -> Result<(), String> {
        let reference = reference.trim().to_owned();
        let secret = secret.trim().to_owned();
        if reference.is_empty() || reference.len() > 128 || reference.chars().any(char::is_control)
        {
            return Err("credential reference must be 1..=128 non-control bytes".to_string());
        }
        if secret.is_empty() || secret.len() > 4096 {
            return Err("credential secret must be 1..=4096 bytes".to_string());
        }
        if ntex::http::header::HeaderValue::try_from(format!("Bearer {secret}")).is_err() {
            return Err("credential secret is not a valid HTTP Bearer token".to_string());
        }
        {
            let admission = self
                .shared
                .admission
                .read()
                .unwrap_or_else(PoisonError::into_inner);
            admitted(&admission).map_err(|error| error.to_string())?;
        }
        let shared = self.shared.clone();

        ntex::rt::spawn_blocking(move || shared.publish_credential(&reference, &secret))
            .await
            .map_err(|error| format!("credential writer failed: {error}"))?
    }

    pub fn has_credential(&self, reference: &str) -> bool {
        let reference = reference.trim();
        if reference.is_empty() {
            return false;
        }
        let in_store = self
            .shared
            .credentials
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(reference)
            .is_some_and(|secret| !secret.is_empty());
        in_store || std::env::var_os(reference).is_some_and(|value| !value.is_empty())
    }

    pub fn credential_resolver(
        &self,
    ) -> Arc<dyn crate::service::ai::providers::CredentialResolver> {
        Arc::new(LocalCredentialResolver {
            shared: self.shared.clone(),
        })
    }
}

struct LocalCredentialResolver {
    shared: Arc<Shared>,
}

impl crate::service::ai::providers::CredentialResolver for LocalCredentialResolver {
    fn resolve(
        &self,
        credential_ref: Option<&str>,
    ) -> Result<Option<String>, crate::service::ai::providers::ProviderExecutionError> {
        let Some(name) = credential_ref else {
            return Ok(None);
        };
        if let Some(secret) = self
            .shared
            .credentials
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(name)
            .filter(|secret| !secret.is_empty())
            .cloned()
        {
            return Ok(Some(secret));
        }
        crate::service::ai::providers::EnvironmentCredentialResolver.resolve(Some(name))
    }
}
