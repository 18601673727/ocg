use super::*;

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

    /// The Jobs that are submitted but neither started nor settled, oldest
    /// first by Job ID, strictly after `after` when given.
    /// At most `limit`, and never more than 1,000, index entries are
    /// examined per call, see [`persistence::PendingJobsPage`].
    pub async fn pending_jobs(
        &self,
        after: Option<JobId>,
        limit: usize,
    ) -> Result<persistence::PendingJobsPage, DatabaseError> {
        self.read(move |database| storage::read_pending_jobs(database, after, limit))
            .await
    }

    pub async fn pending_capture_page(
        &self,
        after: Option<JobId>,
        limit: std::num::NonZeroUsize,
    ) -> Result<Result<PendingCapturePage, PendingReadFault>, DatabaseError> {
        self.read(move |database| storage::read_pending_capture_page(database, after, limit))
            .await
    }

    pub async fn pending_keys(
        &self,
        after: Option<JobId>,
        limit: std::num::NonZeroUsize,
    ) -> Result<Result<PendingCapturePage, PendingReadFault>, DatabaseError> {
        self.read(move |database| storage::read_pending_keys(database, after, limit))
            .await
    }

    pub async fn pending_snapshot(
        &self,
        budget: std::num::NonZeroUsize,
    ) -> Result<Result<PendingSnapshot, PendingReadFault>, DatabaseError> {
        self.read(move |database| storage::read_pending_snapshot(database, budget))
            .await
    }

    pub async fn evaluate_pending(&self, request: EvalRequest) -> Result<EvalPage, DatabaseError> {
        self.read(move |database| storage::evaluate_pending(database, request))
            .await
    }

    pub async fn claim_execution(
        &self,
        request: ClaimRequest,
    ) -> Result<Result<ClaimGrant, ClaimRefusal>, DatabaseError> {
        self.submit(|reply| Command::ClaimExecution { request, reply })
            .await
    }

    #[allow(dead_code, reason = "M2 keeps Scheduler dispatch dormant")]
    pub(crate) async fn reconcile_unexecuted_claim(
        &self,
        job: JobId,
        claim: OperationId,
    ) -> Result<(), DatabaseError> {
        self.submit(|reply| Command::ReconcileUnexecutedClaim { job, claim, reply })
            .await
    }

    pub async fn frozen_pool_binding(
        &self,
        job: JobId,
    ) -> Result<Option<FrozenPoolBinding>, DatabaseError> {
        self.read(move |database| storage::read_pool_binding(database, job))
            .await
    }

    /// Every execution whose external work OCG never credibly confirmed
    /// stopped, and the capacity each one still holds. Read-only and never a
    /// route to release any of it: see
    /// [`persistence::confirm_job_stopped`](crate::persistence::confirm_job_stopped).
    pub async fn unresolved_executions(
        &self,
    ) -> Result<Vec<(JobId, crate::storage::UnresolvedExecution)>, DatabaseError> {
        self.read(storage::read_unresolved_executions).await
    }
    pub async fn occupancy(&self) -> Result<Vec<(ExecutionPoolId, JobId)>, DatabaseError> {
        self.read(storage::read_occupancy).await
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

impl DatabaseHandle {
    pub async fn create_execution_pool(
        &self,
        pool: NewExecutionPool,
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

    pub async fn set_execution_pool_profile(
        &self,
        id: ExecutionPoolId,
        profile: ProfileId,
    ) -> Result<(), DatabaseError> {
        self.submit(|reply| Command::SetExecutionPoolProfile { id, profile, reply })
            .await
    }

    pub async fn execution_pool(
        &self,
        id: ExecutionPoolId,
    ) -> Result<Option<ExecutionPool>, DatabaseError> {
        self.read(move |database| storage::read_execution_pool(database, id))
            .await
    }

    pub async fn execution_pools(
        &self,
        before: Option<ExecutionPoolId>,
        limit: usize,
    ) -> Result<Vec<ExecutionPool>, DatabaseError> {
        self.read(move |database| storage::read_execution_pools(database, before, limit))
            .await
    }

    pub fn set_credential(&self, reference: &str, secret: &str) -> Result<(), String> {
        let reference = reference.trim();
        let secret = secret.trim();
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
        let mut guard = self
            .shared
            .credentials
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        let mut updated = guard.clone();
        updated.insert(reference.to_string(), secret.to_string());
        let serialized = serde_json::to_vec(&updated)
            .map_err(|error| format!("failed to serialize credentials: {error}"))?;
        let temp_path = self.shared.credentials_path.with_extension("json.tmp");
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create(true).truncate(true);
        crate::data_directory::private_file(&mut options);
        let mut file = options
            .open(&temp_path)
            .map_err(|error| format!("failed to open credential staging file: {error}"))?;
        file.write_all(&serialized)
            .and_then(|()| file.sync_all())
            .map_err(|error| format!("failed to write credential staging file: {error}"))?;
        drop(file);
        std::fs::rename(&temp_path, &self.shared.credentials_path)
            .map_err(|error| format!("failed to commit credential store: {error}"))?;
        let _ = crate::data_directory::restrict_file(&self.shared.credentials_path);
        *guard = updated;
        Ok(())
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
    ) -> Arc<dyn crate::provider_execution::CredentialResolver> {
        Arc::new(LocalCredentialResolver {
            shared: self.shared.clone(),
        })
    }
}

struct LocalCredentialResolver {
    shared: Arc<Shared>,
}

impl crate::provider_execution::CredentialResolver for LocalCredentialResolver {
    fn resolve(
        &self,
        credential_ref: Option<&str>,
    ) -> Result<Option<String>, crate::provider_execution::ProviderExecutionError> {
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
        crate::provider_execution::EnvironmentCredentialResolver.resolve(Some(name))
    }
}

