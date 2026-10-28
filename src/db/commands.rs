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
    Recover,
    CompactStorage(flume::Sender<Result<CompactionOutcome, MaintenanceError>>),
}

impl Control {
    pub(super) fn refuse_policy(self, stopped: bool) {
        match self {
            Self::CompactStorage(reply) => {
                reply
                    .send(Err(if stopped {
                        MaintenanceError::WorkerStopped
                    } else {
                        MaintenanceError::ShuttingDown
                    }))
                    .ok();
            }
            Self::Recover => {}
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
    ClaimExecution {
        request: ClaimRequest,
        reply: Reply<Result<ClaimGrant, ClaimRefusal>>,
    },
    ClaimBatch {
        reply: Reply<Vec<JobId>>,
    },
    CreateExecutionPool {
        pool: ExecutionPool,
        reply: Reply<ExecutionPoolId>,
    },
    SetExecutionPoolEnabled {
        id: ExecutionPoolId,
        enabled: bool,
        reply: Reply<()>,
    },
    UpdateExecutionPool {
        id: ExecutionPoolId,
        config: ExecutionPoolConfig,
        reply: Reply<()>,
    },
    CreateJob {
        job: Job,
        reply: Reply<JobId>,
    },
    BenchCreateDependentTurn {
        job: Job,
        reply: Reply<JobId>,
    },
    CreateSubmittedJob {
        job: Job,
        submission: Submission,
        reply: Reply<JobId>,
    },
    DecideApproval {
        approval: ToolCallRef,
        decision: entities::ApprovalDecision,
        reply: Reply<Appended>,
    },
    ApplyJobTool {
        call: ToolCallRef,
        tool: entities::JobToolCall,
        reply: Reply<serde_json::Value>,
    },
    BoundaryB {
        clock: i64,
        reply: Reply<()>,
    },
    Open {
        parent: Option<JobId>,
        spec: JobGroup,
        first: Entry,
        configuration: ExecutionConfiguration,
        submission: Option<Submission>,
        format: Option<crate::db::entities::ResponseFormat>,
        reply: Reply<(JobId, Appended)>,
    },
    Append {
        conversation: JobId,
        entry: Entry,
        freeze: Option<ExecutionConfiguration>,
        submission: Option<Submission>,
        format: Option<crate::db::entities::ResponseFormat>,
        reply: Reply<Appended>,
    },
    AmendQueuedInput {
        conversation: JobId,
        turn: JobId,
        entry: Entry,
        reply: Reply<Appended>,
    },
    Steer {
        conversation: JobId,
        entry: Entry,
        freeze: Option<ExecutionConfiguration>,
        submission: Option<Submission>,
        format: Option<crate::db::entities::ResponseFormat>,
        automatic: bool,
        reply: Reply<(Appended, Vec<(JobId, crate::db::entities::StopCause)>)>,
    },
    /// Opens a system conversation with one typed decision turn. It is the
    /// same admission boundary as `Open`; only the execution class differs.
    SubmitDecision {
        parent: Option<JobId>,
        spec: JobGroup,
        first: Entry,
        configuration: ExecutionConfiguration,
        request: crate::db::entities::DecisionRequest,
        submission: Option<Submission>,
        reply: Reply<(JobId, Appended)>,
    },
    StopTurn {
        conversation: JobId,
        by: Actor,
        reply: Reply<JobAction>,
    },
    Close {
        conversation: JobId,
        by: Actor,
        reply: Reply<JobAction>,
    },
    Hold {
        conversation: JobId,
        by: Actor,
        reply: Reply<JobAction>,
    },
    Unhold {
        conversation: JobId,
        by: Actor,
        reply: Reply<JobAction>,
    },
    Retry {
        conversation: JobId,
        turn: JobId,
        by: Actor,
        reply: Reply<Appended>,
    },
    AppendJobOutput {
        job: JobId,
        output: JobOutput,
        reply: Reply<AppendedOutput>,
    },
    FinalizeJob {
        job: JobId,
        cause: Option<crate::db::entities::OutcomeCause>,
        ignore_stop_cause: bool,
        finalization: JobFinalization,
        evidence: Option<ProviderSettlementEvidence>,
        reply: Reply<Option<AppendedOutput>>,
    },
    CreateExecutor {
        executor: Executor,
        reply: Reply<ExecutorId>,
    },
    CreateProject {
        project: Project,
        reply: Reply<ProjectId>,
    },
    AssessCommandGrant {
        call: ToolCallRef,
        reply: Reply<Option<JobId>>,
    },
    ConfigureProjectDecision {
        id: ProjectId,
        expected: crate::db::entities::Timestamp,
        assignment: Option<crate::db::entities::ProjectDecisionConfig>,
        reply: Reply<crate::db::entities::ProjectRecord>,
    },
    UpdateProject {
        id: ProjectId,
        name: String,
        description: Option<String>,
        commands: Vec<ProjectCommand>,
        reply: Reply<()>,
    },
    SetConversation {
        root: JobId,
        edit: JobEdit,
        reply: Reply<()>,
    },
    /// Schedules optional, bounded shadow prediction after conversation activity.
    PromotePrediction {
        job: JobId,
        reply: Reply<Option<JobId>>,
    },
    PredictContext {
        conversation: JobId,
        reply: Reply<Option<JobId>>,
    },
    /// Requests one idempotent manual compaction using a generative model.
    /// A source that will not fit is refused.
    RequestCompaction {
        conversation: JobId,
        model: ConfigId,
        submission: Submission,
        reply: Reply<crate::db::entities::CompactionRequest>,
    },
    CreateConfig {
        config: Config,
        reply: Reply<ConfigId>,
    },
    RefreshModelMetadata {
        connection: crate::db::entities::ConnectionConfig,
        models: Vec<crate::db::entities::DiscoveredModel>,
        reply: Reply<()>,
    },
    ImportModels {
        connection: Config,
        models: Vec<crate::db::entities::DiscoveredModel>,
        capabilities: std::collections::BTreeSet<crate::db::entities::ModelCapability>,
        reply: Reply<(Vec<ConfigId>, usize)>,
    },
    SetGlobalCompactionSettings {
        settings: crate::db::entities::GlobalCompactionSettings,
        reply: Reply<()>,
    },
    ArchiveModel {
        id: ConfigId,
        reply: Reply<()>,
    },
    UpdateConfig {
        id: ConfigId,
        config: Config,
        reply: Reply<()>,
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
        proof: StopProof,
        reply: Reply<()>,
    },
    // Observations are boxed so they do not size every queued command.
    RecordMetrics {
        metrics: Box<Metrics>,
        reply: Reply<MetricsId>,
    },
    RecordSentRequest {
        sent: Box<SentRequest>,
        reply: Reply<()>,
    },
    RecordProviderAnswer {
        answer: Box<ProviderAnswer>,
        reply: Reply<()>,
    },
    RecordAvailabilityStat {
        stat: Box<AvailabilityStat>,
        reply: Reply<AvailabilityStatId>,
    },
    RecordToolDecision {
        evidence: Box<ToolEvidence>,
        reply: Reply<()>,
    },
    RecordToolOutcome {
        call: ToolCallRef,
        outcome: Box<ToolOutcome>,
        reply: Reply<()>,
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
            Self::ClaimExecution { request, reply } => {
                let mut refusal = ClaimRefusal::RecipeInvalid;
                let outcome =
                    redb::run_batch_declinable(database, operation.epoch, commit, |batch| {
                        match mutations::claim_execution(batch, request)? {
                            Ok(grant) => Ok(Some(grant)),
                            Err(reason) => {
                                refusal = reason;
                                Ok(None)
                            }
                        }
                    });
                let declined = matches!(outcome.result, Ok(None));
                let outcome = redb::BatchOutcome {
                    result: outcome.result.map(|grant| match grant {
                        Some(grant) => Ok(grant),
                        None => Err(refusal),
                    }),
                    storage_failure: outcome.storage_failure,
                    durable_commit_us: outcome.durable_commit_us,
                    scheduling_wake: outcome.scheduling_wake,
                    capacity_freed: outcome.capacity_freed,
                    catalog_changed: outcome.catalog_changed,
                    probe: outcome.probe,
                };
                settle(
                    operation,
                    commit,
                    Kind::ClaimExecution,
                    reply,
                    outcome,
                    declined,
                )
            }
            Self::ClaimBatch { reply } => run(
                database,
                operation,
                commit,
                Kind::ClaimExecution,
                reply,
                |batch| batch.claim_batch(),
            ),
            Self::CreateExecutionPool { pool, reply } => run(
                database,
                operation,
                commit,
                Kind::CreateExecutionPool,
                reply,
                |batch| mutations::create_execution_pool(batch, pool),
            ),
            Self::SetExecutionPoolEnabled { id, enabled, reply } => run(
                database,
                operation,
                commit,
                Kind::SetExecutionPoolEnabled,
                reply,
                |batch| mutations::set_execution_pool_enabled(batch, id, enabled),
            ),
            Self::UpdateExecutionPool { id, config, reply } => run(
                database,
                operation,
                commit,
                Kind::SetExecutionPoolProfile,
                reply,
                |batch| mutations::update_execution_pool(batch, id, config),
            ),
            Self::CreateJob { job, reply } => run(
                database,
                operation,
                commit,
                Kind::CreateJobWithInput,
                reply,
                |batch| mutations::create_job(batch, job),
            ),
            Self::BenchCreateDependentTurn { job, reply } => run(
                database,
                operation,
                commit,
                Kind::CreateJobWithInput,
                reply,
                |batch| mutations::bench_create_dependent_turn(batch, job),
            ),
            Self::CreateSubmittedJob {
                job,
                submission,
                reply,
            } => run(
                database,
                operation,
                commit,
                Kind::CreateJobWithInput,
                reply,
                |batch| mutations::create_submitted_job(batch, job, submission),
            ),
            Self::DecideApproval {
                approval,
                decision,
                reply,
            } => run(
                database,
                operation,
                commit,
                Kind::DecideApproval,
                reply,
                |batch| batch.decide(approval, decision),
            ),
            Self::ApplyJobTool { call, tool, reply } => run(
                database,
                operation,
                commit,
                Kind::ApplyJobTool,
                reply,
                |batch| batch.apply_job_tool(call, tool),
            ),
            Self::BoundaryB { clock, reply } => run(
                database,
                operation,
                commit,
                Kind::BoundaryB,
                reply,
                |batch| batch.boundary_b(clock),
            ),
            Self::Open {
                parent,
                spec,
                first,
                configuration,
                submission,
                format,
                reply,
            } => run(database, operation, commit, Kind::Open, reply, |batch| {
                batch.open_group_with_format(
                    parent,
                    spec,
                    first,
                    configuration,
                    submission,
                    Vec::new(),
                    format,
                )
            }),
            Self::Append {
                conversation,
                entry,
                freeze,
                submission,
                format,
                reply,
            } => run(database, operation, commit, Kind::Append, reply, |batch| {
                batch.require_agent_group(conversation)?;
                batch.append_with_format(conversation, entry, freeze, submission, false, format)
            }),
            Self::AmendQueuedInput {
                conversation,
                turn,
                entry,
                reply,
            } => run(database, operation, commit, Kind::Append, reply, |batch| {
                batch.amend_queued_input(conversation, turn, entry)
            }),
            Self::Steer {
                conversation,
                entry,
                freeze,
                submission,
                format,
                automatic,
                reply,
            } => run(database, operation, commit, Kind::Append, reply, |batch| {
                batch.steer_with_format(conversation, entry, freeze, submission, format, automatic)
            }),
            Self::SubmitDecision {
                parent,
                spec,
                first,
                configuration,
                request,
                submission,
                reply,
            } => run(database, operation, commit, Kind::Open, reply, |batch| {
                batch.open_decision(parent, spec, first, configuration, request, submission)
            }),
            Self::StopTurn {
                conversation,
                by,
                reply,
            } => run(
                database,
                operation,
                commit,
                Kind::StopTurn,
                reply,
                |batch| batch.stop_turn(conversation, by),
            ),
            Self::Close {
                conversation,
                by,
                reply,
            } => run(database, operation, commit, Kind::Close, reply, |batch| {
                batch.close(conversation, by)
            }),
            Self::Hold {
                conversation,
                by,
                reply,
            } => run(database, operation, commit, Kind::Hold, reply, |batch| {
                batch.hold(conversation, by, false)
            }),
            Self::Unhold {
                conversation,
                by,
                reply,
            } => run(database, operation, commit, Kind::Unhold, reply, |batch| {
                batch.hold(conversation, by, true)
            }),
            Self::Retry {
                conversation,
                turn,
                by,
                reply,
            } => run(database, operation, commit, Kind::Retry, reply, |batch| {
                batch.retry(conversation, turn, by)
            }),
            Self::AppendJobOutput { job, output, reply } => {
                let mut result = run(
                    database,
                    operation,
                    commit,
                    Kind::AppendJobOutput,
                    reply,
                    |batch| mutations::append_job_output(batch, job, output),
                );
                if result.committed {
                    result.output_changed = Some(job);
                }
                result
            }
            Self::FinalizeJob {
                job,
                finalization,
                cause,
                ignore_stop_cause,
                evidence,
                reply,
            } => {
                let mut changes = Vec::new();
                let mut result = run(
                    database,
                    operation,
                    commit,
                    Kind::FinalizeJob,
                    reply,
                    |batch| {
                        let provider_settlement = evidence.is_some();
                        let abandoned = provider_settlement
                            && batch
                                .require_job_state(job)?
                                .outcome
                                .is_some_and(|outcome| {
                                    outcome.cause == Some(entities::OutcomeCause::Abandoned)
                                });
                        let output = if abandoned {
                            None
                        } else {
                            mutations::finalize_job(
                                batch,
                                job,
                                finalization,
                                cause,
                                ignore_stop_cause,
                            )?
                        };
                        let mut probe = None;
                        match evidence {
                            None => {}
                            Some(ProviderSettlementEvidence::Observations(observations)) => {
                                for stat in observations {
                                    if stat.job != job {
                                        return batch.reject(Violation::InvalidValue(
                                            "job",
                                            "observation belongs to a different job",
                                        ));
                                    }
                                    let id = mutations::record_availability_stat(batch, stat)?;
                                    changes.push(ObservationChange::Availability(job, id));
                                    probe = Some(id);
                                }
                            }
                            Some(ProviderSettlementEvidence::Answer(answer)) => {
                                if answer.stat.job != job {
                                    return batch.reject(Violation::InvalidValue(
                                        "job",
                                        "answer belongs to a different job",
                                    ));
                                }
                                let (metrics, availability) =
                                    mutations::record_provider_answer(batch, *answer)?;
                                changes.push(ObservationChange::Metrics(job, metrics));
                                changes.push(ObservationChange::Availability(job, availability));
                                probe = Some(availability);
                            }
                        }
                        if provider_settlement {
                            batch.provider_settlement_probe(job, probe)?;
                        }
                        Ok(output)
                    },
                );
                if result.committed {
                    result.observations = changes;
                }
                result
            }
            Self::CreateExecutor { executor, reply } => run(
                database,
                operation,
                commit,
                Kind::CreateExecutor,
                reply,
                |batch| mutations::create_executor(batch, executor),
            ),
            Self::CreateProject { project, reply } => run(
                database,
                operation,
                commit,
                Kind::CreateProject,
                reply,
                |batch| mutations::create_project(batch, project),
            ),
            Self::AssessCommandGrant { call, reply } => run(
                database,
                operation,
                commit,
                Kind::UpdateProject,
                reply,
                |batch| batch.assess_command_grant(call),
            ),
            Self::ConfigureProjectDecision {
                id,
                expected,
                assignment,
                reply,
            } => run(
                database,
                operation,
                commit,
                Kind::UpdateProject,
                reply,
                |batch| mutations::configure_project_decision(batch, id, expected, assignment),
            ),
            Self::UpdateProject {
                id,
                name,
                description,
                commands,
                reply,
            } => run(
                database,
                operation,
                commit,
                Kind::UpdateProject,
                reply,
                |batch| mutations::update_project(batch, id, name, description, commands),
            ),
            Self::SetConversation { root, edit, reply } => run(
                database,
                operation,
                commit,
                Kind::SetConversation,
                reply,
                |batch| mutations::set_conversation(batch, root, edit),
            ),
            Self::PromotePrediction { job, reply } => {
                run(database, operation, commit, Kind::Open, reply, |batch| {
                    batch.promote_prediction(job)
                })
            }
            Self::PredictContext {
                conversation,
                reply,
            } => run(database, operation, commit, Kind::Open, reply, |batch| {
                batch.predict_context(conversation)
            }),
            Self::RequestCompaction {
                conversation,
                model,
                submission,
                reply,
            } => run(database, operation, commit, Kind::Open, reply, |batch| {
                batch.request_compaction(conversation, model, submission)
            }),
            Self::CreateConfig { config, reply } => run(
                database,
                operation,
                commit,
                Kind::CreateProfile,
                reply,
                |batch| mutations::create_config(batch, config),
            ),
            Self::RefreshModelMetadata {
                connection,
                models,
                reply,
            } => run(
                database,
                operation,
                commit,
                Kind::UpdateProfile,
                reply,
                |batch| batch.refresh_model_metadata(connection, models),
            ),
            Self::ImportModels {
                connection,
                models,
                capabilities,
                reply,
            } => run(
                database,
                operation,
                commit,
                Kind::CreateProfile,
                reply,
                |batch| mutations::import_models(batch, connection, models, capabilities),
            ),
            Self::SetGlobalCompactionSettings { settings, reply } => run(
                database,
                operation,
                commit,
                Kind::UpdateProfile,
                reply,
                |batch| batch.set_global_compaction_settings(settings),
            ),
            Self::ArchiveModel { id, reply } => run(
                database,
                operation,
                commit,
                Kind::UpdateProfile,
                reply,
                |batch| mutations::archive_model(batch, id),
            ),
            Self::UpdateConfig { id, config, reply } => run(
                database,
                operation,
                commit,
                Kind::UpdateProfile,
                reply,
                |batch| mutations::update_config(batch, id, config),
            ),
            Self::CreateAttachment { attachment, reply } => run(
                database,
                operation,
                commit,
                Kind::CreateAttachment,
                reply,
                |batch| mutations::create_attachment(batch, *attachment),
            ),
            Self::CleanupAttachment { id, kind, reply } => run(
                database,
                operation,
                commit,
                Kind::CleanupAttachment,
                reply,
                |batch| mutations::cleanup_attachment(batch, id, kind),
            ),
            Self::ConfirmJobStopped { job, proof, reply } => run(
                database,
                operation,
                commit,
                Kind::ConfirmJobStopped,
                reply,
                |batch| mutations::confirm_job_stopped(batch, job, proof),
            ),
            Self::RecordMetrics { metrics, reply } => {
                let job = metrics.job;
                let id = std::cell::Cell::new(None);
                let mut result = run(
                    database,
                    operation,
                    commit,
                    Kind::RecordMetrics,
                    reply,
                    |batch| {
                        let value = mutations::record_metrics(batch, *metrics)?;
                        id.set(Some(value));
                        Ok(value)
                    },
                );
                if let Some(id) = id.get() {
                    result
                        .observations
                        .push(ObservationChange::Metrics(job, id));
                }
                result
            }
            Self::RecordSentRequest { sent, reply } => run(
                database,
                operation,
                commit,
                Kind::RecordSentRequest,
                reply,
                |batch| mutations::record_sent_request(batch, *sent),
            ),
            Self::RecordProviderAnswer { answer, reply } => {
                let job = answer.stat.job;
                let ids = std::cell::Cell::new(None);
                let mut result = run(
                    database,
                    operation,
                    commit,
                    Kind::RecordProviderAnswer,
                    reply,
                    |batch| {
                        let value = mutations::record_provider_answer(batch, *answer)?;
                        ids.set(Some(value));
                        Ok(())
                    },
                );
                if let Some((metrics, availability)) = ids.get() {
                    result
                        .observations
                        .push(ObservationChange::Metrics(job, metrics));
                    result
                        .observations
                        .push(ObservationChange::Availability(job, availability));
                }
                result
            }
            Self::RecordAvailabilityStat { stat, reply } => {
                let job = stat.job;
                let id = std::cell::Cell::new(None);
                let mut result = run(
                    database,
                    operation,
                    commit,
                    Kind::RecordAvailabilityStat,
                    reply,
                    |batch| {
                        let value = mutations::record_availability_stat(batch, *stat)?;
                        id.set(Some(value));
                        Ok(value)
                    },
                );
                if let Some(id) = id.get() {
                    result
                        .observations
                        .push(ObservationChange::Availability(job, id));
                }
                result
            }
            Self::RecordToolDecision { evidence, reply } => run(
                database,
                operation,
                commit,
                Kind::RecordToolDecision,
                reply,
                |batch| mutations::record_tool_decision(batch, *evidence),
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
                |batch| mutations::record_tool_outcome(batch, call, *outcome),
            ),
        }
    }

    /// The space this command's transaction is allowed to need: twice its
    /// variable-length payload and search-index allowance, plus
    /// [`TRANSACTION_ALLOWANCE_BYTES`]. Saturates
    /// rather than wraps, so an absurd payload fails the space check.
    pub(super) fn expected_write_bytes(&self) -> u64 {
        let payload = match self {
            Self::ClaimExecution { request, .. } => byte_count(request.allowed_pools.len())
                .saturating_mul(8)
                .saturating_add(4096 * 512),
            Self::ClaimBatch { .. } => 2 * 4096 * 512,
            Self::BoundaryB { .. } => 4096,
            Self::DecideApproval { decision, .. } => match decision {
                entities::ApprovalDecision::Answered { text } => byte_count(text.len()),
                entities::ApprovalDecision::Approved
                | entities::ApprovalDecision::Rejected
                | entities::ApprovalDecision::Voided => 64,
            },
            Self::ApplyJobTool { tool, .. } => {
                use entities::JobToolCall;
                let search = match tool {
                    JobToolCall::JobPlan { steps, .. } => steps
                        .iter()
                        .map(|step| search_index_bytes(&step.objective).saturating_add(64 * 1024))
                        .sum(),
                    JobToolCall::JobAssign { objective, .. }
                    | JobToolCall::JobDispatch { objective, .. } => search_index_bytes(objective),
                    JobToolCall::JobMessage { text, .. } => search_index_bytes(text),
                    JobToolCall::JobLimits { .. }
                    | JobToolCall::JobAgents { .. }
                    | JobToolCall::JobWorkers { .. }
                    | JobToolCall::JobStop { .. }
                    | JobToolCall::JobClose { .. }
                    | JobToolCall::JobHold { .. }
                    | JobToolCall::JobUnhold { .. }
                    | JobToolCall::JobRetry { .. } => 0,
                };
                byte_count(format!("{tool:?}").len())
                    .saturating_add(4096)
                    .saturating_add(search)
                    .saturating_add(match tool {
                        JobToolCall::JobAssign { objective, .. }
                        | JobToolCall::JobDispatch { objective, .. }
                            if objective.len() <= 64 * 1024 =>
                        {
                            byte_count(objective.len())
                                .saturating_mul(6)
                                .saturating_add(64 * 1024)
                        }
                        JobToolCall::JobMessage { text, .. } if text.len() <= 64 * 1024 => {
                            byte_count(text.len())
                                .saturating_mul(6)
                                .saturating_add(64 * 1024)
                        }
                        _ => 0,
                    })
            }
            Self::CreateExecutionPool { pool, .. } => byte_count(pool.name.len()),
            Self::SetExecutionPoolEnabled { .. } | Self::UpdateExecutionPool { .. } => 64,
            Self::CreateJob { job, .. }
            | Self::BenchCreateDependentTurn { job, .. }
            | Self::CreateSubmittedJob { job, .. } => {
                text_bytes(&job.name).saturating_add(text_bytes(&job.input.objective))
            }
            Self::SubmitDecision { first, request, .. } => byte_count(format!("{first:?}").len())
                .saturating_add(byte_count(format!("{request:?}").len())),
            Self::Steer {
                entry: first,
                format,
                ..
            }
            | Self::Open { first, format, .. }
            | Self::Append {
                entry: first,
                format,
                ..
            } => byte_count(format!("{first:?}").len())
                .saturating_add(match first {
                    Entry::UserMessage { text, attachments } => search_index_bytes(text)
                        .saturating_add((attachments.len() as u64).saturating_mul(
                            3 * 64 * (crate::db::entities::MAX_ATTACHMENT_NAME_BYTES as u64 + 1),
                        )),
                    Entry::AgentMessage { text, .. } => search_index_bytes(text),
                    Entry::Notice { .. }
                    | Entry::Decision { .. }
                    | Entry::RetryOf { .. }
                    | Entry::Iteration { .. }
                    | Entry::UserMessageEdit { .. }
                    | Entry::UserMessageWithdraw { .. }
                    | Entry::ContextPreparation { .. }
                    | Entry::ContextUse { .. }
                    | Entry::ResponseFormat { .. } => 0,
                })
                .saturating_add(format.as_ref().map_or(0, |format| {
                    byte_count(redb::encode_response_format(format).len())
                }))
                .saturating_add(match first {
                    Entry::UserMessage { text, .. } | Entry::AgentMessage { text, .. }
                        if text.len() <= 64 * 1024 =>
                    {
                        byte_count(text.len())
                            .saturating_mul(6)
                            .saturating_add(64 * 1024)
                    }
                    _ => 0,
                }),
            Self::AmendQueuedInput { entry, .. } => byte_count(format!("{entry:?}").len())
                .saturating_add(match entry {
                    Entry::UserMessageEdit {
                        text, attachments, ..
                    } => search_index_bytes(text).saturating_add(
                        (attachments.len() as u64).saturating_mul(
                            3 * 64 * (crate::db::entities::MAX_ATTACHMENT_NAME_BYTES as u64 + 1),
                        ),
                    ),
                    Entry::UserMessage { .. }
                    | Entry::UserMessageWithdraw { .. }
                    | Entry::AgentMessage { .. }
                    | Entry::Notice { .. }
                    | Entry::Decision { .. }
                    | Entry::RetryOf { .. }
                    | Entry::Iteration { .. }
                    | Entry::ContextPreparation { .. }
                    | Entry::ContextUse { .. }
                    | Entry::ResponseFormat { .. } => 0,
                })
                .saturating_add(4096),
            Self::StopTurn { .. }
            | Self::Close { .. }
            | Self::Hold { .. }
            | Self::Unhold { .. }
            | Self::Retry { .. } => 4096,
            Self::ConfirmJobStopped { .. } => 0,
            // The record rewritten keeps its length; the allowance covers it.
            Self::CleanupAttachment { .. } => 0,
            Self::AppendJobOutput { output, .. } => {
                output_bytes(&output.summary, &output.code, &output.value)
            }
            Self::FinalizeJob {
                finalization,
                evidence,
                ..
            } => {
                let evidence_bytes = match evidence {
                    None => 0,
                    Some(ProviderSettlementEvidence::Answer(answer)) => {
                        byte_count(answer.response.byte_length())
                            .saturating_add(json_bytes(&answer.measurements.usage.raw))
                    }
                    Some(ProviderSettlementEvidence::Observations(stats)) => {
                        stats.iter().fold(0u64, |bytes, stat| {
                            let observation = &stat.observation;
                            let failure = observation.failure.as_ref().map_or(0, |failure| {
                                byte_count(failure.message.len())
                                    .saturating_add(text_bytes(&failure.provider_code))
                                    .saturating_add(text_bytes(&failure.response_body))
                                    .saturating_add(text_bytes(&failure.request_id))
                            });
                            bytes
                                .saturating_add(failure)
                                .saturating_add(text_bytes(&observation.raw_finish_reason))
                        })
                    }
                };
                evidence_bytes.saturating_add(match finalization {
                    JobFinalization::Completed(result)
                    | JobFinalization::Failed(result)
                    | JobFinalization::Cancelled(result) => {
                        let text = result
                            .value
                            .as_ref()
                            .and_then(|value| value.get("text"))
                            .and_then(serde_json::Value::as_str)
                            .or(result.summary.as_deref());
                        output_bytes(&result.summary, &result.code, &result.value)
                            .saturating_add(64 * 1024)
                            .saturating_add(text.map_or(0, search_index_bytes))
                    }
                    JobFinalization::Unknown => 0,
                })
            }
            Self::CreateExecutor { executor, .. } => byte_count(executor.name.len()),
            Self::CreateProject { project, .. } => byte_count(project.name.len())
                .saturating_add(text_bytes(&project.description))
                .saturating_add(byte_count(project.root.len()))
                .saturating_add(command_bytes(&project.commands)),
            Self::ConfigureProjectDecision { .. } => 32 * 1024,
            Self::AssessCommandGrant { .. } => 128 * 1024,
            Self::UpdateProject {
                name,
                description,
                commands,
                ..
            } => byte_count(name.len())
                .saturating_add(text_bytes(description))
                .saturating_add(command_bytes(commands)),
            Self::SetConversation { edit, .. } => match edit {
                JobEdit::Title(title) | JobEdit::SuggestedTitle(title) => byte_count(title.len()),
                JobEdit::Hide | JobEdit::Limits(_) => 16,
                JobEdit::CompactionPolicy(policy) => compaction_bytes(policy),
            },
            // The sealed source is already stored. This records one system
            // child and a short instruction, not a second copy of the source.
            Self::PromotePrediction { .. } => 4096 * 512,
            Self::PredictContext { .. } => 131072,
            Self::RequestCompaction { .. } => 4096 * 512,
            Self::SetGlobalCompactionSettings { settings, .. } => {
                compaction_bytes(&Some(settings.policy.clone())).saturating_add(64)
            }
            Self::RefreshModelMetadata {
                connection, models, ..
            } => text_bytes(&connection.endpoint)
                .saturating_add(text_bytes(&connection.credential_ref))
                .saturating_add(
                    models
                        .iter()
                        .map(|model| byte_count(model.id.len()).saturating_add(512))
                        .sum::<u64>(),
                ),
            Self::ArchiveModel { .. } => 16,
            Self::ImportModels {
                connection, models, ..
            } => {
                let base = match &connection.body {
                    ConfigBody::Connection(body) => {
                        text_bytes(&body.endpoint).saturating_add(text_bytes(&body.credential_ref))
                    }
                    ConfigBody::Model(_)
                    | ConfigBody::Agent(_)
                    | ConfigBody::Pool(_)
                    | ConfigBody::DecisionModel(_)
                    | ConfigBody::DecisionAgent(_) => 0,
                };
                models.iter().fold(
                    base.saturating_add(byte_count(connection.name.len())),
                    |bytes, model| {
                        bytes
                            .saturating_add(byte_count(model.id.len()))
                            .saturating_add(512)
                            .saturating_add(16)
                    },
                )
            }
            Self::CreateConfig { config, .. } | Self::UpdateConfig { config, .. } => {
                byte_count(config.name.len()).saturating_add(match &config.body {
                    ConfigBody::Agent(agent) => text_bytes(&agent.instructions)
                        .saturating_add(compaction_bytes(&agent.compaction_policy))
                        .saturating_add(byte_count(agent.models.len()).saturating_mul(8)),
                    ConfigBody::Model(model) => byte_count(model.model.len()),
                    ConfigBody::Connection(connection) => text_bytes(&connection.endpoint)
                        .saturating_add(text_bytes(&connection.credential_ref)),
                    ConfigBody::Pool(_) => 32,
                    ConfigBody::DecisionModel(dm) => byte_count(dm.model.len())
                        .saturating_add(2048)
                        .saturating_add(16)
                        .saturating_add(byte_count(dm.declared_capabilities.len()))
                        .saturating_add(32),
                    ConfigBody::DecisionAgent(da) => text_bytes(&da.instructions)
                        .saturating_add(byte_count(da.models.len().saturating_mul(16)))
                        .saturating_add(32),
                })
            }
            Self::CreateAttachment { attachment, .. } => byte_count(attachment.mime.len())
                .saturating_add(text_bytes(&attachment.display_name)),
            Self::RecordMetrics { metrics, .. } => json_bytes(&metrics.measurements.usage.raw),
            Self::RecordSentRequest { sent, .. } => sent.bodies.iter().fold(
                byte_count(sent.messages.len()).saturating_mul(32),
                |bytes, body| bytes.saturating_add(byte_count(body.byte_length())),
            ),
            Self::RecordProviderAnswer { answer, .. } => byte_count(answer.response.byte_length())
                .saturating_add(json_bytes(&answer.measurements.usage.raw)),
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
                match evidence.decision.stored_admission() {
                    None => identity,
                    Some(admission) => identity
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
        };
        payload
            .saturating_mul(2)
            .saturating_add(TRANSACTION_ALLOWANCE_BYTES)
    }

    /// Answers a command that will never run, so its caller is not left
    /// waiting and knows nothing was applied.
    pub(super) fn refuse(self, error: impl Fn() -> DatabaseError) {
        match self {
            Self::ClaimExecution { reply, .. } => refuse(reply, error),
            Self::ClaimBatch { reply, .. } => refuse(reply, error),
            Self::DecideApproval { reply, .. } => refuse(reply, error),
            Self::ApplyJobTool { reply, .. } => refuse(reply, error),
            Self::BoundaryB { reply, .. } => refuse(reply, error),
            Self::CreateExecutionPool { reply, .. } => refuse(reply, error),
            Self::SetExecutionPoolEnabled { reply, .. }
            | Self::UpdateExecutionPool { reply, .. } => refuse(reply, error),
            Self::CreateJob { reply, .. }
            | Self::BenchCreateDependentTurn { reply, .. }
            | Self::CreateSubmittedJob { reply, .. } => refuse(reply, error),
            Self::Open { reply, .. } | Self::SubmitDecision { reply, .. } => refuse(reply, error),
            Self::Steer { reply, .. } => refuse(reply, error),
            Self::AmendQueuedInput { reply, .. }
            | Self::Append { reply, .. }
            | Self::Retry { reply, .. } => refuse(reply, error),
            Self::StopTurn { reply, .. }
            | Self::Close { reply, .. }
            | Self::Hold { reply, .. }
            | Self::Unhold { reply, .. } => refuse(reply, error),
            Self::AppendJobOutput { reply, .. } => refuse(reply, error),
            Self::FinalizeJob { reply, .. } => refuse(reply, error),
            Self::CreateExecutor { reply, .. } => refuse(reply, error),
            Self::CreateProject { reply, .. } => refuse(reply, error),
            Self::ConfigureProjectDecision { reply, .. } => refuse(reply, error),
            Self::AssessCommandGrant { reply, .. } => refuse(reply, error),
            Self::UpdateProject { reply, .. } => refuse(reply, error),
            Self::SetConversation { reply, .. } => refuse(reply, error),
            Self::PromotePrediction { reply, .. } => refuse(reply, error),
            Self::PredictContext { reply, .. } => refuse(reply, error),
            Self::RequestCompaction { reply, .. } => refuse(reply, error),
            Self::CreateConfig { reply, .. } => refuse(reply, error),
            Self::UpdateConfig { reply, .. } => refuse(reply, error),
            Self::ImportModels { reply, .. } => refuse(reply, error),
            Self::RefreshModelMetadata { reply, .. } => refuse(reply, error),
            Self::SetGlobalCompactionSettings { reply, .. } => refuse(reply, error),
            Self::ArchiveModel { reply, .. } => refuse(reply, error),
            Self::CreateAttachment { reply, .. } => refuse(reply, error),
            Self::CleanupAttachment { reply, .. } => refuse(reply, error),
            Self::ConfirmJobStopped { reply, .. } => refuse(reply, error),
            Self::RecordMetrics { reply, .. } => refuse(reply, error),
            Self::RecordAvailabilityStat { reply, .. } => refuse(reply, error),
            Self::RecordSentRequest { reply, .. }
            | Self::RecordProviderAnswer { reply, .. }
            | Self::RecordToolDecision { reply, .. }
            | Self::RecordToolOutcome { reply, .. } => refuse(reply, error),
        }
    }
}

pub(super) fn refuse<T>(reply: Reply<T>, error: impl Fn() -> DatabaseError) {
    reply.send(Err(error())).ok();
}

// At most three gram keys per lowercased scalar; each 53-byte key has
// an additional allowance for its index entry. The command doubles this too.
fn search_index_bytes(text: &str) -> u64 {
    byte_count(text.chars().flat_map(char::to_lowercase).count())
        .saturating_mul(3)
        .saturating_mul(64)
}

fn compaction_bytes(policy: &Option<crate::db::entities::CompactionPolicy>) -> u64 {
    policy.as_ref().map_or(0, |policy| {
        256u64.saturating_add(
            policy
                .predictive
                .as_ref()
                .and_then(|prediction| prediction.acceptance.as_ref())
                .map_or(0, |pin| {
                    byte_count(pin.reported_model.len())
                        .saturating_add(byte_count(pin.confidence_semantics.len()))
                }),
        )
    })
}

pub(super) fn command_bytes(commands: &[ProjectCommand]) -> u64 {
    commands.iter().fold(0, |total, command| {
        command.argv.iter().fold(
            total
                .saturating_add(byte_count(command.name.len()))
                .saturating_add(19),
            |total, entry| total.saturating_add(byte_count(entry.len())),
        )
    })
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
