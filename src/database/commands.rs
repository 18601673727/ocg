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
    #[cfg(test)]
    CreateClaimedFixture {
        job: NewJob,
        executor: ExecutorId,
        reply: Reply<JobId>,
    },
    ClaimExecution {
        request: ClaimRequest,
        reply: Reply<Result<ClaimGrant, ClaimRefusal>>,
    },
    ClaimBatch {
        reply: Reply<Vec<JobId>>,
    },
    CreateExecutionPool {
        pool: NewExecutionPool,
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
        job: NewJob,
        reply: Reply<JobId>,
    },
    BenchCreateDependentTurn {
        job: NewJob,
        reply: Reply<JobId>,
    },
    CreateSubmittedJob {
        job: NewJob,
        submission: NewSubmission,
        reply: Reply<JobId>,
    },
    DecideApproval {
        approval: ToolCallRef,
        decision: persistence::ApprovalDecision,
        reply: Reply<Appended>,
    },
    ApplyJobTool {
        call: ToolCallRef,
        tool: crate::orchestration::job_tools::JobToolCall,
        max_result_bytes: usize,
        reply: Reply<serde_json::Value>,
    },
    BoundaryB {
        clock: i64,
        reply: Reply<()>,
    },
    Open {
        parent: Option<JobId>,
        spec: ConversationSpec,
        first: Entry,
        configuration: ExecutionConfiguration,
        submission: Option<NewSubmission>,
        format: Option<crate::persistence::ResponseFormat>,
        reply: Reply<(JobId, Appended)>,
    },
    Append {
        conversation: JobId,
        entry: Entry,
        freeze: Option<ExecutionConfiguration>,
        submission: Option<NewSubmission>,
        format: Option<crate::persistence::ResponseFormat>,
        reply: Reply<Appended>,
    },
    /// Opens a system conversation with one typed decision turn. It is the
    /// same admission boundary as `Open`; only the execution class differs.
    SubmitDecision {
        parent: Option<JobId>,
        spec: ConversationSpec,
        first: Entry,
        configuration: ExecutionConfiguration,
        request: crate::persistence::DecisionRequest,
        submission: Option<NewSubmission>,
        reply: Reply<(JobId, Appended)>,
    },
    StopTurn {
        conversation: JobId,
        by: Actor,
        reply: Reply<ConversationAction>,
    },
    Close {
        conversation: JobId,
        by: Actor,
        reply: Reply<ConversationAction>,
    },
    Hold {
        conversation: JobId,
        by: Actor,
        reply: Reply<ConversationAction>,
    },
    Unhold {
        conversation: JobId,
        by: Actor,
        reply: Reply<ConversationAction>,
    },
    Retry {
        conversation: JobId,
        turn: JobId,
        by: Actor,
        reply: Reply<Appended>,
    },
    AppendJobOutput {
        job: JobId,
        output: NewJobOutput,
        reply: Reply<AppendedOutput>,
    },
    FinalizeJob {
        job: JobId,
        cause: Option<crate::persistence::OutcomeCause>,
        ignore_stop_cause: bool,
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
    UpdateProject {
        id: ProjectId,
        name: String,
        description: Option<String>,
        commands: Vec<ProjectCommand>,
        reply: Reply<()>,
    },
    SetConversation {
        root: JobId,
        edit: ConversationEdit,
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
        submission: NewSubmission,
        reply: Reply<crate::persistence::CompactionRequest>,
    },
    CreateConfig {
        config: NewConfig,
        reply: Reply<ConfigId>,
    },
    ImportModels {
        connection: NewConfig,
        models: Vec<(String, Option<u32>)>,
        capabilities: std::collections::BTreeSet<crate::persistence::ModelCapability>,
        reply: Reply<(Vec<ConfigId>, usize)>,
    },
    ArchiveModel {
        id: ConfigId,
        reply: Reply<()>,
    },
    UpdateConfig {
        id: ConfigId,
        config: NewConfig,
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
        metrics: Box<NewMetrics>,
        reply: Reply<MetricsId>,
    },
    RecordSentRequest {
        sent: Box<NewSentRequest>,
        reply: Reply<()>,
    },
    RecordProviderAnswer {
        answer: Box<NewProviderAnswer>,
        reply: Reply<()>,
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
            #[cfg(test)]
            Self::CreateClaimedFixture {
                job,
                executor,
                reply,
            } => run(
                database,
                operation,
                commit,
                Kind::CreateJobWithInput,
                reply,
                |batch| {
                    let job = persistence::create_job(batch, job)?;
                    match persistence::claim_execution(
                        batch,
                        ClaimRequest {
                            job,
                            allowed_pools: std::collections::BTreeSet::from([executor.into()]),
                        },
                    )? {
                        Ok(_) => Ok(job),
                        Err(_) => batch.reject(Violation::InvalidValue(
                            "claim",
                            "fixture Job could not be claimed",
                        )),
                    }
                },
            ),
            Self::ClaimExecution { request, reply } => {
                let mut refusal = ClaimRefusal::RecipeInvalid;
                let outcome =
                    storage::run_batch_declinable(database, operation.epoch, commit, |batch| {
                        match persistence::claim_execution(batch, request)? {
                            Ok(grant) => Ok(Some(grant)),
                            Err(reason) => {
                                refusal = reason;
                                Ok(None)
                            }
                        }
                    });
                let declined = matches!(outcome.result, Ok(None));
                let outcome = storage::BatchOutcome {
                    result: outcome.result.map(|grant| match grant {
                        Some(grant) => Ok(grant),
                        None => Err(refusal),
                    }),
                    storage_failure: outcome.storage_failure,
                    scheduling_wake: outcome.scheduling_wake,
                    capacity_freed: outcome.capacity_freed,
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
                |batch| persistence::create_execution_pool(batch, pool),
            ),
            Self::SetExecutionPoolEnabled { id, enabled, reply } => run(
                database,
                operation,
                commit,
                Kind::SetExecutionPoolEnabled,
                reply,
                |batch| persistence::set_execution_pool_enabled(batch, id, enabled),
            ),
            Self::UpdateExecutionPool { id, config, reply } => run(
                database,
                operation,
                commit,
                Kind::SetExecutionPoolProfile,
                reply,
                |batch| persistence::update_execution_pool(batch, id, config),
            ),
            Self::CreateJob { job, reply } => run(
                database,
                operation,
                commit,
                Kind::CreateJobWithInput,
                reply,
                |batch| persistence::create_job(batch, job),
            ),
            Self::BenchCreateDependentTurn { job, reply } => run(
                database,
                operation,
                commit,
                Kind::CreateJobWithInput,
                reply,
                |batch| persistence::bench_create_dependent_turn(batch, job),
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
                |batch| persistence::create_submitted_job(batch, job, submission),
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
            Self::ApplyJobTool {
                call,
                tool,
                max_result_bytes,
                reply,
            } => run(
                database,
                operation,
                commit,
                Kind::ApplyJobTool,
                reply,
                |batch| batch.apply_job_tool(call, tool, max_result_bytes),
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
                batch.open_conversation_with_format(
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
                batch.require_user_conversation(conversation)?;
                batch.append_with_format(conversation, entry, freeze, submission, false, format)
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
                cause,
                ignore_stop_cause,
                reply,
            } => run(
                database,
                operation,
                commit,
                Kind::FinalizeJob,
                reply,
                |batch| {
                    persistence::finalize_job(batch, job, finalization, cause, ignore_stop_cause)
                },
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
                |batch| persistence::update_project(batch, id, name, description, commands),
            ),
            Self::SetConversation { root, edit, reply } => run(
                database,
                operation,
                commit,
                Kind::SetConversation,
                reply,
                |batch| persistence::set_conversation(batch, root, edit),
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
                |batch| persistence::create_config(batch, config),
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
                |batch| persistence::import_models(batch, connection, models, capabilities),
            ),
            Self::ArchiveModel { id, reply } => run(
                database,
                operation,
                commit,
                Kind::UpdateProfile,
                reply,
                |batch| persistence::archive_model(batch, id),
            ),
            Self::UpdateConfig { id, config, reply } => run(
                database,
                operation,
                commit,
                Kind::UpdateProfile,
                reply,
                |batch| persistence::update_config(batch, id, config),
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
            Self::ConfirmJobStopped { job, proof, reply } => run(
                database,
                operation,
                commit,
                Kind::ConfirmJobStopped,
                reply,
                |batch| persistence::confirm_job_stopped(batch, job, proof),
            ),
            Self::RecordMetrics { metrics, reply } => run(
                database,
                operation,
                commit,
                Kind::RecordMetrics,
                reply,
                |batch| persistence::record_metrics(batch, *metrics),
            ),
            Self::RecordSentRequest { sent, reply } => run(
                database,
                operation,
                commit,
                Kind::RecordSentRequest,
                reply,
                |batch| persistence::record_sent_request(batch, *sent),
            ),
            Self::RecordProviderAnswer { answer, reply } => run(
                database,
                operation,
                commit,
                Kind::RecordProviderAnswer,
                reply,
                |batch| persistence::record_provider_answer(batch, *answer),
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
        }
    }

    /// The space this command's transaction is allowed to need: twice its
    /// variable-length payload and search-index allowance, plus
    /// [`TRANSACTION_ALLOWANCE_BYTES`]. Saturates
    /// rather than wraps, so an absurd payload fails the space check.
    pub(super) fn expected_write_bytes(&self) -> u64 {
        let payload = match self {
            #[cfg(test)]
            Self::CreateClaimedFixture { job, .. } => {
                text_bytes(&job.name).saturating_add(text_bytes(&job.input.objective))
            }
            Self::ClaimExecution { request, .. } => byte_count(request.allowed_pools.len())
                .saturating_mul(8)
                .saturating_add(4096 * 512),
            Self::ClaimBatch { .. } => 2 * 4096 * 512,
            Self::BoundaryB { .. } => 4096,
            Self::DecideApproval { decision, .. } => match decision {
                persistence::ApprovalDecision::Answered { text } => byte_count(text.len()),
                persistence::ApprovalDecision::Approved
                | persistence::ApprovalDecision::Rejected
                | persistence::ApprovalDecision::Voided => 64,
            },
            Self::ApplyJobTool { tool, .. } => {
                use crate::orchestration::job_tools::JobToolCall;
                let search = match tool {
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
            Self::Open { first, format, .. }
            | Self::Append {
                entry: first,
                format,
                ..
            } => byte_count(format!("{first:?}").len())
                .saturating_add(match first {
                    Entry::UserMessage { text, .. } | Entry::AgentMessage { text, .. } => {
                        search_index_bytes(text)
                    }
                    Entry::Notice { .. }
                    | Entry::Decision { .. }
                    | Entry::RetryOf { .. }
                    | Entry::Iteration { .. }
                    | Entry::ContextPreparation { .. }
                    | Entry::ContextUse { .. }
                    | Entry::ResponseFormat { .. } => 0,
                })
                .saturating_add(format.as_ref().map_or(0, |format| {
                    byte_count(storage::encode_response_format(format).len())
                })),
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
            Self::FinalizeJob { finalization, .. } => match finalization {
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
                        .saturating_add(text.map_or(0, search_index_bytes))
                }
                JobFinalization::Unknown => 0,
            },
            Self::CreateExecutor { executor, .. } => byte_count(executor.name.len()),
            Self::CreateProject { project, .. } => byte_count(project.name.len())
                .saturating_add(text_bytes(&project.description))
                .saturating_add(byte_count(project.root.len()))
                .saturating_add(command_bytes(&project.commands)),
            Self::UpdateProject {
                name,
                description,
                commands,
                ..
            } => byte_count(name.len())
                .saturating_add(text_bytes(description))
                .saturating_add(command_bytes(commands)),
            Self::SetConversation { edit, .. } => match edit {
                ConversationEdit::Title(title) | ConversationEdit::SuggestedTitle(title) => {
                    byte_count(title.len())
                }
                ConversationEdit::Hide | ConversationEdit::Limits(_) => 16,
                ConversationEdit::CompactionPolicy(policy) => compaction_bytes(policy),
            },
            // The sealed source is already stored. This records one system
            // child and a short instruction, not a second copy of the source.
            Self::PromotePrediction { .. } => 4096 * 512,
            Self::PredictContext { .. } => 131072,
            Self::RequestCompaction { .. } => 4096 * 512,
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
                    |bytes, (model, _)| {
                        bytes
                            .saturating_add(byte_count(model.len()))
                            .saturating_add(16)
                    },
                )
            }
            Self::CreateConfig { config, .. } | Self::UpdateConfig { config, .. } => {
                byte_count(config.name.len()).saturating_add(match &config.body {
                    ConfigBody::Agent(agent) => text_bytes(&agent.instructions)
                        .saturating_add(compaction_bytes(&agent.compaction_policy))
                        .saturating_add(text_bytes(&agent.tool_grant))
                        .saturating_add(
                            byte_count(agent.models.len().saturating_add(agent.dispatch_to.len()))
                                .saturating_mul(8),
                        ),
                    ConfigBody::Model(model) => byte_count(model.model.len()),
                    ConfigBody::Connection(connection) => text_bytes(&connection.endpoint)
                        .saturating_add(text_bytes(&connection.credential_ref)),
                    ConfigBody::Pool(_) => 32,
                    ConfigBody::DecisionModel(dm) => byte_count(dm.model.len())
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
            #[cfg(test)]
            Self::CreateClaimedFixture { reply, .. } => refuse(reply, error),
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
            Self::Append { reply, .. } | Self::Retry { reply, .. } => refuse(reply, error),
            Self::StopTurn { reply, .. }
            | Self::Close { reply, .. }
            | Self::Hold { reply, .. }
            | Self::Unhold { reply, .. } => refuse(reply, error),
            Self::AppendJobOutput { reply, .. } => refuse(reply, error),
            Self::FinalizeJob { reply, .. } => refuse(reply, error),
            Self::CreateExecutor { reply, .. } => refuse(reply, error),
            Self::CreateProject { reply, .. } => refuse(reply, error),
            Self::UpdateProject { reply, .. } => refuse(reply, error),
            Self::SetConversation { reply, .. } => refuse(reply, error),
            Self::PromotePrediction { reply, .. } => refuse(reply, error),
            Self::PredictContext { reply, .. } => refuse(reply, error),
            Self::RequestCompaction { reply, .. } => refuse(reply, error),
            Self::CreateConfig { reply, .. } => refuse(reply, error),
            Self::UpdateConfig { reply, .. } => refuse(reply, error),
            Self::ImportModels { reply, .. } => refuse(reply, error),
            Self::ArchiveModel { reply, .. } => refuse(reply, error),
            Self::CreateAttachment { reply, .. } => refuse(reply, error),
            Self::CleanupAttachment { reply, .. } => refuse(reply, error),
            Self::ConfirmJobStopped { reply, .. } => refuse(reply, error),
            Self::RecordMetrics { reply, .. } => refuse(reply, error),
            Self::RecordAvailabilityStat { reply, .. } => refuse(reply, error),
            Self::RecordSentRequest { reply, .. }
            | Self::RecordProviderAnswer { reply, .. }
            | Self::RecordToolDecision { reply, .. }
            | Self::RecordToolOutcome { reply, .. }
            | Self::ReconcileToolEffect { reply, .. } => refuse(reply, error),
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

fn compaction_bytes(policy: &Option<crate::persistence::CompactionPolicy>) -> u64 {
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
            total.saturating_add(byte_count(command.name.len())),
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

#[cfg(test)]
mod decision_budget_tests {
    use super::*;
    #[test]
    fn message_and_result_estimates_cover_search_posting_amplification() {
        let text = (0x1000..0x3000)
            .filter_map(char::from_u32)
            .collect::<String>();
        let keys = text.chars().count() * 3 - 3;
        let postings = u64::try_from(keys * 53).unwrap();
        let (reply, _) = flume::bounded(1);
        let command = Command::Append {
            conversation: JobId::new(1),
            entry: Entry::UserMessage {
                text: text.clone(),
                attachments: vec![],
            },
            freeze: None,
            submission: None,
            format: None,
            reply,
        };
        assert!(command.expected_write_bytes() >= TRANSACTION_ALLOWANCE_BYTES + 2 * postings);
        let (reply, _) = flume::bounded(1);
        let command = Command::FinalizeJob {
            job: JobId::new(2),
            cause: None,
            ignore_stop_cause: false,
            finalization: JobFinalization::Completed(crate::persistence::FinalResult {
                summary: Some(text.clone()),
                code: None,
                goal_reached: None,
                value: None,
            }),
            reply,
        };
        assert!(command.expected_write_bytes() >= TRANSACTION_ALLOWANCE_BYTES + 2 * postings);
        let (reply, _) = flume::bounded(1);
        let command = Command::ApplyJobTool {
            call: ToolCallRef {
                job: JobId::new(2),
                request: RequestNumber::FIRST,
                index: 0,
            },
            tool: crate::orchestration::job_tools::JobToolCall::JobMessage {
                conversation: JobId::new(1).to_string(),
                text,
            },
            max_result_bytes: 1000,
            reply,
        };
        assert!(command.expected_write_bytes() >= TRANSACTION_ALLOWANCE_BYTES + 2 * postings);
        assert_eq!(search_index_bytes("İ"), 2 * 3 * 64);
    }

    #[test]
    fn decision_payload_estimate_counts_uuid_targets_and_variable_text() {
        use crate::persistence::{DecisionAgentConfig, WorkspaceAccess};
        let estimate = |instructions: &str, count: usize| {
            let (reply, _) = flume::bounded(1);
            Command::CreateConfig {
                config: NewConfig {
                    name: "decision".into(),
                    body: ConfigBody::DecisionAgent(DecisionAgentConfig {
                        instructions: Some(instructions.into()),
                        models: (0..count).map(|n| ConfigId::new(n as u128)).collect(),
                        default_model: None,
                        workspace_access: WorkspaceAccess::None,
                    }),
                },
                reply,
            }
            .expected_write_bytes()
        };
        assert_eq!(estimate("", 256) - estimate("", 0), 2 * 256 * 16);
        assert_eq!(estimate(&"x".repeat(65536), 0) - estimate("", 0), 2 * 65536);
    }
}
