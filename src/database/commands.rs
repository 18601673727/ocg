use super::*;

pub(super) type Reply<T> = flume::Sender<Result<T, DatabaseError>>;

pub(super) enum Message {
    Run(Box<Envelope>),
    Shutdown,
}

/// Write-worker requests that are not commands. They have their own queue,
/// so a full command queue never delays them, and the worker takes them
/// before its next command.
pub(super) enum Control {
    FilesystemFailure {
        reason: ProtectionReason,
        reply: flume::Sender<bool>,
    },
    CheckSpace,
    Recover,
    CompactStorage(flume::Sender<Result<CompactionOutcome, MaintenanceError>>),
    ReadPolicy(flume::Sender<Result<SystemPolicy, PolicyOperationError>>),
    SavePolicy {
        expected: SystemPolicy,
        candidate: SystemPolicy,
        started: Arc<AtomicBool>,
        reply: flume::Sender<Result<PolicySaveResult, PolicyOperationError>>,
    },
}

impl Control {
    pub(super) fn refuse_policy(self, stopped: bool) {
        let error = if stopped {
            PolicyOperationError::WorkerStopped
        } else {
            PolicyOperationError::ShuttingDown
        };
        match self {
            Self::ReadPolicy(reply) => {
                reply.send(Err(error)).ok();
            }
            Self::SavePolicy { reply, .. } => {
                reply.send(Err(error)).ok();
            }
            Self::CompactStorage(reply) => {
                reply
                    .send(Err(if stopped {
                        MaintenanceError::WorkerStopped
                    } else {
                        MaintenanceError::ShuttingDown
                    }))
                    .ok();
            }
            Self::CheckSpace | Self::Recover => {}
            Self::FilesystemFailure { reply, .. } => {
                reply.send(false).ok();
            }
        }
    }
}

/// An admitted command with its execution mark, which the worker sets before
/// the command touches storage. The submitter keeps the other reference, so
/// when the reply is lost it can tell a command that never began from one
/// that began.
pub(super) struct Envelope {
    pub(super) operation: OperationId,
    pub(super) started: Arc<AtomicBool>,
    pub(super) command: Command,
}

pub(super) enum Command {
    BeginCoreLogCleanup {
        owner: JobId,
        executor: ExecutorId,
        started: NewJobOutput,
        reply: Reply<crate::log_cleanup::CleanupRecords>,
    },
    SaveJobInput {
        existing: Option<JobInputId>,
        draft: JobInputDraft,
        reply: Reply<JobInputId>,
    },
    CreateJobWithInput {
        job: NewJob,
        input: JobInputId,
        reply: Reply<JobId>,
    },
    CreateLocalJobWithInput {
        job: NewJob,
        input: JobInputId,
        /// Recorded with the Job, in the same transaction, when given.
        submission: Option<NewSubmission>,
        reply: Reply<JobId>,
    },
    RetryJob {
        retry: JobRetry,
        reply: Reply<JobId>,
    },
    StartJob {
        job: JobId,
        reply: Reply<()>,
    },
    StartLocalJob {
        job: JobId,
        reply: Reply<()>,
    },
    AssignJobExecutor {
        job: JobId,
        executor: ExecutorId,
        reply: Reply<()>,
    },
    AssociateJobProject {
        job: JobId,
        project: ProjectId,
        reply: Reply<()>,
    },
    AppendJobOutput {
        job: JobId,
        output: NewJobOutput,
        reply: Reply<AppendedOutput>,
    },
    FinalizeJob {
        job: JobId,
        finalization: JobFinalization,
        reply: Reply<Option<AppendedOutput>>,
    },
    CreateExecutor {
        executor: NewExecutor,
        reply: Reply<ExecutorId>,
    },
    CreateProject {
        project: NewProject,
        reply: Reply<ProjectId>,
    },
    CreateLlmProvider {
        provider: NewLlmProvider,
        reply: Reply<LlmProviderId>,
    },
    CreateProfile {
        profile: NewProfile,
        reply: Reply<ProfileId>,
    },
    UpdateProfile {
        id: ProfileId,
        profile: NewProfile,
        reply: Reply<()>,
    },
    CreateAgent {
        agent: NewAgent,
        reply: Reply<AgentId>,
    },
    CreateAttachment {
        attachment: Box<PublishedAttachment>,
        reply: Reply<AttachmentId>,
    },
    CleanupAttachment {
        id: AttachmentId,
        kind: CleanupKind,
        reply: Reply<AttachmentRecord>,
    },
    ConfirmJobStopped {
        job: JobId,
        reply: Reply<()>,
    },
    // Observations are boxed so they do not size every queued command.
    RecordMetrics {
        metrics: Box<NewMetrics>,
        reply: Reply<MetricsId>,
    },
    RecordAvailabilityStat {
        stat: Box<NewAvailabilityStat>,
        reply: Reply<AvailabilityStatId>,
    },
    RecordToolDecision {
        evidence: Box<NewToolEvidence>,
        reply: Reply<()>,
    },
    RecordToolOutcome {
        call: ToolCallRef,
        outcome: Box<ToolOutcome>,
        reply: Reply<()>,
    },
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "OCG has no trusted reconciler outside its tests yet"
        )
    )]
    ReconcileToolEffect {
        call: ToolCallRef,
        reconciliation: Box<ToolReconciliation>,
        reply: Reply<()>,
    },
    /// Commits one prepared AvailabilityStat aggregation batch. Its reply is
    /// `None` when the preparation was stale and nothing was applied.
    CommitAvailabilityAggregation {
        plan: Box<storage::AvailabilityPlan>,
        reply: Reply<Option<Timestamp>>,
    },
    /// Commits one prepared Token aggregation batch, like
    /// [`Self::CommitAvailabilityAggregation`].
    CommitTokenAggregation {
        plan: Box<storage::TokenPlan>,
        reply: Reply<Option<Timestamp>>,
    },
}

impl Command {
    /// Runs the command as one batch. The answer is returned, not sent, so
    /// the caller can hold it until the commit is durable. A dropped reply
    /// receiver means the caller stopped waiting; the batch has ended either
    /// way.
    pub(super) fn execute(
        self,
        database: &Database,
        operation: OperationId,
        commit: Commit,
    ) -> Executed {
        use OperationKind as Kind;
        match self {
            Self::BeginCoreLogCleanup {
                owner,
                executor,
                started,
                reply,
            } => run(
                database,
                operation,
                commit,
                Kind::BeginCoreLogCleanup,
                reply,
                |batch| {
                    persistence::begin_core_log_cleanup(
                        batch,
                        owner,
                        executor,
                        started,
                        operation.epoch,
                    )
                },
            ),
            Self::SaveJobInput {
                existing,
                draft,
                reply,
            } => run(
                database,
                operation,
                commit,
                Kind::SaveJobInput,
                reply,
                |batch| persistence::save_job_input(batch, existing, draft),
            ),
            Self::CreateJobWithInput { job, input, reply } => run(
                database,
                operation,
                commit,
                Kind::CreateJobWithInput,
                reply,
                |batch| persistence::create_job_with_input(batch, job, input),
            ),
            // Its marker is that of the creation; the start is part of it.
            Self::CreateLocalJobWithInput {
                job,
                input,
                submission,
                reply,
            } => run(
                database,
                operation,
                commit,
                Kind::CreateJobWithInput,
                reply,
                |batch| match submission {
                    None => {
                        persistence::create_local_job_with_input(batch, job, input, operation.epoch)
                    }
                    Some(submission) => persistence::create_submitted_local_job(
                        batch,
                        job,
                        input,
                        operation.epoch,
                        submission,
                    ),
                },
            ),
            Self::RetryJob { retry, reply } => run(
                database,
                operation,
                commit,
                Kind::RetryJob,
                reply,
                |batch| persistence::retry_job(batch, retry),
            ),
            Self::StartJob { job, reply } => run(
                database,
                operation,
                commit,
                Kind::StartJob,
                reply,
                |batch| persistence::start_job(batch, job),
            ),
            Self::StartLocalJob { job, reply } => run(
                database,
                operation,
                commit,
                Kind::StartLocalJob,
                reply,
                |batch| persistence::start_local_job(batch, job, operation.epoch),
            ),
            Self::AssignJobExecutor {
                job,
                executor,
                reply,
            } => run(
                database,
                operation,
                commit,
                Kind::AssignJobExecutor,
                reply,
                |batch| persistence::assign_job_executor(batch, job, executor),
            ),
            Self::AssociateJobProject {
                job,
                project,
                reply,
            } => run(
                database,
                operation,
                commit,
                Kind::AssociateJobProject,
                reply,
                |batch| persistence::associate_job_project(batch, job, project),
            ),
            Self::AppendJobOutput { job, output, reply } => run(
                database,
                operation,
                commit,
                Kind::AppendJobOutput,
                reply,
                |batch| persistence::append_job_output(batch, job, output),
            ),
            Self::FinalizeJob {
                job,
                finalization,
                reply,
            } => run(
                database,
                operation,
                commit,
                Kind::FinalizeJob,
                reply,
                |batch| persistence::finalize_job(batch, job, finalization),
            ),
            Self::CreateExecutor { executor, reply } => run(
                database,
                operation,
                commit,
                Kind::CreateExecutor,
                reply,
                |batch| persistence::create_executor(batch, executor),
            ),
            Self::CreateProject { project, reply } => run(
                database,
                operation,
                commit,
                Kind::CreateProject,
                reply,
                |batch| persistence::create_project(batch, project),
            ),
            Self::CreateLlmProvider { provider, reply } => run(
                database,
                operation,
                commit,
                Kind::CreateLlmProvider,
                reply,
                |batch| persistence::create_llm_provider(batch, provider),
            ),
            Self::CreateProfile { profile, reply } => run(
                database,
                operation,
                commit,
                Kind::CreateProfile,
                reply,
                |batch| persistence::create_profile(batch, profile),
            ),
            Self::UpdateProfile { id, profile, reply } => run(
                database,
                operation,
                commit,
                Kind::UpdateProfile,
                reply,
                |batch| persistence::update_profile(batch, id, profile),
            ),
            Self::CreateAgent { agent, reply } => run(
                database,
                operation,
                commit,
                Kind::CreateAgent,
                reply,
                |batch| persistence::create_agent(batch, agent),
            ),
            Self::CreateAttachment { attachment, reply } => run(
                database,
                operation,
                commit,
                Kind::CreateAttachment,
                reply,
                |batch| persistence::create_attachment(batch, *attachment),
            ),
            Self::CleanupAttachment { id, kind, reply } => run(
                database,
                operation,
                commit,
                Kind::CleanupAttachment,
                reply,
                |batch| persistence::cleanup_attachment(batch, id, kind),
            ),
            Self::ConfirmJobStopped { job, reply } => run(
                database,
                operation,
                commit,
                Kind::ConfirmJobStopped,
                reply,
                |batch| persistence::confirm_job_stopped(batch, job),
            ),
            Self::RecordMetrics { metrics, reply } => run(
                database,
                operation,
                commit,
                Kind::RecordMetrics,
                reply,
                |batch| persistence::record_metrics(batch, *metrics),
            ),
            Self::RecordAvailabilityStat { stat, reply } => run(
                database,
                operation,
                commit,
                Kind::RecordAvailabilityStat,
                reply,
                |batch| persistence::record_availability_stat(batch, *stat),
            ),
            Self::RecordToolDecision { evidence, reply } => run(
                database,
                operation,
                commit,
                Kind::RecordToolDecision,
                reply,
                |batch| persistence::record_tool_decision(batch, *evidence),
            ),
            Self::RecordToolOutcome {
                call,
                outcome,
                reply,
            } => run(
                database,
                operation,
                commit,
                Kind::RecordToolOutcome,
                reply,
                |batch| persistence::record_tool_outcome(batch, call, *outcome),
            ),
            Self::ReconcileToolEffect {
                call,
                reconciliation,
                reply,
            } => run(
                database,
                operation,
                commit,
                Kind::ReconcileToolEffect,
                reply,
                |batch| persistence::reconcile_tool_effect(batch, call, *reconciliation),
            ),
            Self::CommitAvailabilityAggregation { plan, reply } => {
                let outcome =
                    storage::commit_availability_aggregation(database, operation, commit, *plan);
                // Only this command can decline: `Ok(None)` is its explicit
                // statement that nothing was committed, not a reply value.
                let declined = matches!(outcome.result, Ok(None));
                settle(operation, commit, reply, outcome, declined)
            }
            Self::CommitTokenAggregation { plan, reply } => {
                let outcome = storage::commit_token_aggregation(database, operation, commit, *plan);
                // The same explicit decline as the availability commit.
                let declined = matches!(outcome.result, Ok(None));
                settle(operation, commit, reply, outcome, declined)
            }
        }
    }

    /// The space this command's transaction is allowed to need: twice its
    /// variable-length payload plus [`TRANSACTION_ALLOWANCE_BYTES`]. Saturates
    /// rather than wraps, so an absurd payload fails the space check.
    pub(super) fn expected_write_bytes(&self) -> u64 {
        let payload = match self {
            Self::BeginCoreLogCleanup { started, .. } => {
                output_bytes(&started.summary, &started.code, &started.value).saturating_add(256)
            }
            Self::SaveJobInput { draft, .. } => {
                text_bytes(&draft.submission_id).saturating_add(text_bytes(&draft.objective))
            }
            Self::CreateJobWithInput { job, .. } | Self::CreateLocalJobWithInput { job, .. } => {
                text_bytes(&job.name)
            }
            Self::RetryJob { retry, .. } => text_bytes(&retry.name),
            Self::StartJob { .. }
            | Self::StartLocalJob { .. }
            | Self::AssignJobExecutor { .. }
            | Self::AssociateJobProject { .. }
            | Self::ConfirmJobStopped { .. } => 0,
            // The record rewritten keeps its length; the allowance covers it.
            Self::CleanupAttachment { .. } => 0,
            Self::AppendJobOutput { output, .. } => {
                output_bytes(&output.summary, &output.code, &output.value)
            }
            Self::FinalizeJob { finalization, .. } => match finalization {
                JobFinalization::Completed(result)
                | JobFinalization::Failed(result)
                | JobFinalization::Cancelled(result) => {
                    output_bytes(&result.summary, &result.code, &result.value)
                }
                JobFinalization::Unknown => 0,
            },
            Self::CreateExecutor { executor, .. } => byte_count(executor.name.len()),
            Self::CreateProject { project, .. } => {
                byte_count(project.name.len()).saturating_add(text_bytes(&project.description))
            }
            Self::CreateLlmProvider { provider, .. } => byte_count(provider.name.len())
                .saturating_add(text_bytes(&provider.endpoint))
                .saturating_add(text_bytes(&provider.credential_ref)),
            Self::CreateProfile { profile, .. } | Self::UpdateProfile { profile, .. } => {
                byte_count(profile.name.len()).saturating_add(match &profile.config {
                    NewProfileConfig::Agent { config, .. } => text_bytes(&config.instructions),
                    NewProfileConfig::Llm { config, .. } => byte_count(config.model.len()),
                })
            }
            Self::CreateAgent { agent, .. } => byte_count(agent.name.len()),
            Self::CreateAttachment { attachment, .. } => byte_count(attachment.mime.len())
                .saturating_add(text_bytes(&attachment.display_name)),
            Self::RecordMetrics { metrics, .. } => json_bytes(&metrics.measurements.usage.raw),
            Self::CommitAvailabilityAggregation { plan, .. } => plan.encoded_bytes(),
            Self::CommitTokenAggregation { plan, .. } => plan.encoded_bytes(),
            Self::RecordAvailabilityStat { stat, .. } => {
                let observation = &stat.observation;
                let failure = observation.failure.as_ref().map_or(0, |failure| {
                    byte_count(failure.message.len())
                        .saturating_add(text_bytes(&failure.provider_code))
                        .saturating_add(text_bytes(&failure.response_body))
                        .saturating_add(text_bytes(&failure.request_id))
                });
                failure.saturating_add(text_bytes(&observation.raw_finish_reason))
            }
            Self::RecordToolDecision { evidence, .. } => {
                let identity = byte_count(evidence.tool_call_id.len())
                    .saturating_add(byte_count(evidence.name.len()));
                match &evidence.decision {
                    ToolDecision::Refused(_) => identity,
                    ToolDecision::Admitted(admission) => identity
                        .saturating_add(byte_count(admission.handler_version.len()))
                        .saturating_add(byte_count(admission.parameters.len()))
                        .saturating_add(byte_count(admission.arguments.len())),
                }
            }
            Self::RecordToolOutcome { outcome, .. } => match outcome.as_ref() {
                ToolOutcome::Completed { result } => byte_count(result.len()),
                ToolOutcome::NotApplied { message } => byte_count(message.len()),
                ToolOutcome::Uncertain { message, .. } => text_bytes(message),
                ToolOutcome::CompletedWithheld { .. } | ToolOutcome::NotInvoked { .. } => 0,
            },
            Self::ReconcileToolEffect { reconciliation, .. } => {
                byte_count(reconciliation.reconciler.len())
                    .saturating_add(byte_count(reconciliation.evidence.len()))
                    .saturating_add(byte_count(reconciliation.explanation.len()))
            }
        };
        payload
            .saturating_mul(2)
            .saturating_add(TRANSACTION_ALLOWANCE_BYTES)
    }

    /// Answers a command that will never run, so its caller is not left
    /// waiting and knows nothing was applied.
    pub(super) fn refuse(self, error: impl Fn() -> DatabaseError) {
        match self {
            Self::BeginCoreLogCleanup { reply, .. } => refuse(reply, error),
            Self::SaveJobInput { reply, .. } => refuse(reply, error),
            Self::CreateJobWithInput { reply, .. }
            | Self::CreateLocalJobWithInput { reply, .. }
            | Self::RetryJob { reply, .. } => refuse(reply, error),
            Self::StartJob { reply, .. }
            | Self::StartLocalJob { reply, .. }
            | Self::AssignJobExecutor { reply, .. }
            | Self::AssociateJobProject { reply, .. } => refuse(reply, error),
            Self::AppendJobOutput { reply, .. } => refuse(reply, error),
            Self::FinalizeJob { reply, .. } => refuse(reply, error),
            Self::CreateExecutor { reply, .. } => refuse(reply, error),
            Self::CreateProject { reply, .. } => refuse(reply, error),
            Self::CreateLlmProvider { reply, .. } => refuse(reply, error),
            Self::CreateProfile { reply, .. } => refuse(reply, error),
            Self::UpdateProfile { reply, .. } => refuse(reply, error),
            Self::CreateAgent { reply, .. } => refuse(reply, error),
            Self::CreateAttachment { reply, .. } => refuse(reply, error),
            Self::CleanupAttachment { reply, .. } => refuse(reply, error),
            Self::ConfirmJobStopped { reply, .. } => refuse(reply, error),
            Self::RecordMetrics { reply, .. } => refuse(reply, error),
            Self::RecordAvailabilityStat { reply, .. } => refuse(reply, error),
            Self::RecordToolDecision { reply, .. }
            | Self::RecordToolOutcome { reply, .. }
            | Self::ReconcileToolEffect { reply, .. } => refuse(reply, error),
            Self::CommitAvailabilityAggregation { reply, .. } => refuse(reply, error),
            Self::CommitTokenAggregation { reply, .. } => refuse(reply, error),
        }
    }
}

pub(super) fn refuse<T>(reply: Reply<T>, error: impl Fn() -> DatabaseError) {
    reply.send(Err(error())).ok();
}

pub(super) fn byte_count(length: usize) -> u64 {
    u64::try_from(length).unwrap_or(u64::MAX)
}

pub(super) fn text_bytes(text: &Option<String>) -> u64 {
    text.as_ref().map_or(0, |text| byte_count(text.len()))
}

pub(super) fn json_bytes(value: &Option<serde_json::Value>) -> u64 {
    value
        .as_ref()
        .map_or(0, |value| counted_json_bytes(value, ByteCounter::default()))
}

/// Measures the compact serialization of `value` on top of what `counter`
/// already holds, without retaining the serialized bytes. An overflow or an
/// unexpected serialization failure answers `u64::MAX`, never a short count.
pub(super) fn counted_json_bytes(value: &serde_json::Value, mut counter: ByteCounter) -> u64 {
    match serde_json::to_writer(&mut counter, value) {
        Ok(()) => counter.bytes,
        Err(_) => u64::MAX,
    }
}

/// A write sink that only counts the bytes written to it.
#[derive(Default)]
pub(super) struct ByteCounter {
    pub(super) bytes: u64,
}

impl Write for ByteCounter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.bytes = self
            .bytes
            .checked_add(byte_count(buf.len()))
            .ok_or_else(|| std::io::Error::other("serialized byte count overflowed"))?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(super) fn output_bytes(
    summary: &Option<String>,
    code: &Option<String>,
    value: &Option<serde_json::Value>,
) -> u64 {
    text_bytes(summary)
        .saturating_add(text_bytes(code))
        .saturating_add(json_bytes(value))
}
