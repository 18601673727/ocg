use super::*;
use crate::storage::JobRow;

pub(crate) type OpResult<T> = Result<T, BatchError>;

// ---- Jobs ---------------------------------------------------------------------------

/// The immutable spec of a Job about to be submitted: its input, its
/// relationships and the configuration it freezes.
fn spec_of(batch: &mut Batch, job: &NewJob, allow_stored: bool) -> Result<JobRow, BatchError> {
    let stored = job.parent.get() != 0 && batch.require_job(job.parent)?.kind.is_conversation();
    if stored && !allow_stored {
        return batch.reject(Violation::InvalidValue(
            "parent",
            "turns are created by append or open_conversation",
        ));
    }
    let frozen = match job.configuration {
        Some(configuration) => Some(batch.freeze_configuration(configuration)?),
        None => None,
    };
    if stored && job.eligible_at.is_some() {
        return batch.reject(Violation::InvalidValue(
            "eligible_at",
            "a stored turn cannot be delayed",
        ));
    }
    let Some(frozen) = frozen else {
        return batch.reject(Violation::InvalidValue(
            "execution",
            "a process turn requires execution",
        ));
    };
    if let Some(project) = job.project {
        batch.require_project(project)?;
    }
    if !stored && job.previous.is_some() {
        return batch.reject(Violation::InvalidValue(
            "previous",
            "a process turn has no conversation",
        ));
    }
    if (job.process_grant.is_some()
        || frozen
            .agent
            .as_ref()
            .is_some_and(|agent| agent.tool_grant.is_some()))
        && !stored
        && job.project.is_none()
    {
        return batch.reject(Violation::ProjectRequired);
    }
    Ok(JobRow {
        id: JobId::new(0),
        created_at: batch.now(),
        parent_id: (job.parent.get() != 0).then_some(job.parent),
        kind: JobKind::Turn {
            name: job.name.clone(),
            plan_source: if stored {
                PlanSource::Stored {
                    previous: job.previous,
                    dependencies: Vec::new(),
                    preparation: None,
                }
            } else {
                PlanSource::Process {
                    eligible_at: job.eligible_at,
                    objective: job.input.objective.clone(),
                    attachments: job.input.attachments.clone(),
                    project: job.project,
                    grant: job.process_grant.clone().or_else(|| {
                        frozen
                            .agent
                            .as_ref()
                            .and_then(|agent| agent.tool_grant.clone())
                    }),
                }
            },
            execution: TurnExecution::Frozen(frozen),
        },
    })
}

/// T1 constructs a spec directly. Interactive messages use append instead.
pub(crate) fn create_job(batch: &mut Batch, job: NewJob) -> OpResult<JobId> {
    submit(batch, job, None, false)
}

pub(crate) fn bench_create_dependent_turn(batch: &mut Batch, job: NewJob) -> OpResult<JobId> {
    submit(batch, job, None, true)
}

fn submit(
    batch: &mut Batch,
    job: NewJob,
    submission: Option<NewSubmission>,
    allow_stored: bool,
) -> Result<JobId, BatchError> {
    let spec = spec_of(batch, &job, allow_stored)?;
    let submission = submission.map(|submission| (submission.key, submission.fingerprint));
    let stored = spec.kind.is_stored();
    let turn = batch.t1_submit(submission, spec)?;
    if stored {
        batch.insert_turn_input(
            turn,
            Entry::UserMessage {
                text: job.input.objective.unwrap_or_default(),
                attachments: job.input.attachments,
            },
        )?;
    }
    Ok(turn)
}

pub(crate) fn create_submitted_job(
    batch: &mut Batch,
    job: NewJob,
    submission: NewSubmission,
) -> OpResult<JobId> {
    submit(batch, job, Some(submission), false)
}

/// T2 for one named Job: claims it into the first of `request.allowed_pools`
/// that can run it, within the same limits as the claim batch.
pub(crate) fn claim_execution(
    batch: &mut Batch,
    request: ClaimRequest,
) -> OpResult<Result<ClaimGrant, ClaimRefusal>> {
    let allowed = request
        .allowed_pools
        .iter()
        .map(|pool| (*pool).into())
        .collect();
    batch.claim_job(request.job, &allowed)
}

// ---- outputs and outcomes ------------------------------------------------------------

/// Intermediate outputs are refused once the Job has any outcome, unknown
/// included, so a final result stays the Job's newest output and an unknown
/// Job's recorded outputs stay as they were (I11).
pub(crate) fn append_job_output(
    batch: &mut Batch,
    job: JobId,
    output: NewJobOutput,
) -> OpResult<AppendedOutput> {
    let state = batch.require_job_state(job)?;
    if state.outcome.is_some() {
        return batch.reject(Violation::JobHasTerminalOutcome(job));
    }
    let (seq, output_id) = batch.append_output(
        job,
        output.kind.into(),
        output.summary,
        output.code,
        output.goal_reached,
        output.value,
    )?;
    Ok(AppendedOutput {
        output: output_id,
        seq: i64::from(seq),
    })
}

/// FINALIZE. A confirmed outcome of a claimed Job requires every direct child
/// to have settled and every admitted tool call to be resolved; otherwise the
/// outcome recorded is Unknown (I6). An Unknown outcome publishes no result.
/// A Job without a claim can only be cancelled.
///
/// Returns the Result output, when one was published.
pub(crate) fn finalize_job(
    batch: &mut Batch,
    job: JobId,
    finalization: JobFinalization,
    cause: Option<OutcomeCause>,
    ignore_stop_cause: bool,
) -> OpResult<Option<AppendedOutput>> {
    let (kind, result) = match finalization {
        JobFinalization::Unknown => (OutcomeKind::Unknown, None),
        JobFinalization::Completed(result) => (OutcomeKind::Completed, Some(result)),
        JobFinalization::Failed(result) => (OutcomeKind::Failed, Some(result)),
        JobFinalization::Cancelled(result) => (OutcomeKind::Cancelled, Some(result)),
    };
    let (_, appended) = batch.finalize(job, kind, cause, result, ignore_stop_cause)?;
    Ok(appended.map(|(seq, output)| AppendedOutput {
        output,
        seq: i64::from(seq),
    }))
}

/// Records that an unknown-outcome Job's execution has actually stopped,
/// which releases the slot it holds. Only the trusted execution path that ran
/// the Job may call this, and only once it has observed the termination; a
/// timeout, a cancel request, a disabled Pool or a restart is not such an
/// observation. The unknown outcome stays as it is. The stop is local: it
/// says nothing of an effect a tool call had outside OCG.
pub(crate) fn confirm_job_stopped(batch: &mut Batch, job: JobId, proof: StopProof) -> OpResult<()> {
    batch.confirm_stop(job, proof)
}

// ---- tool calls -------------------------------------------------------------------------

/// Records OCG's decision about a call a Provider requested, before
/// anything about the call can run: its refusal, or its admission with the
/// exact registration, arguments and authorization it runs under. The call
/// is identified by its Job's Provider request, which must have a recorded
/// observation, and its wire index there; the Provider's id for it must be
/// unused in the Job. A call has one decision, never replaced.
///
/// `evidence.spawns` says whether the handler can start an OS process; the
/// caller takes it from [`ToolAdmission::spawns`]. Only the trusted execution
/// path of the Job, while it executes it, may record a decision. An admission
/// is refused beyond the stored evidence limits:
/// [`MAX_STORED_TOOL_ARGUMENTS_BYTES`] per call, and
/// [`MAX_STORED_TOOL_ARGUMENTS_PER_JOB_BYTES`] and
/// [`MAX_STORED_TOOL_SCHEMAS_PER_JOB_BYTES`] per Job.
pub(crate) fn record_tool_decision(batch: &mut Batch, evidence: NewToolEvidence) -> OpResult<()> {
    let call = evidence.call;
    if evidence.tool_call_id.is_empty() || evidence.tool_call_id.len() > MAX_TOOL_CALL_ID_BYTES {
        return batch.reject(Violation::InvalidValue(
            "tool_call_id",
            "must be 1 to 256 bytes",
        ));
    }
    if !is_function_name(&evidence.name) {
        return batch.reject(Violation::InvalidValue("name", "is not a function name"));
    }
    if let Some(admission) = evidence.decision.stored_admission() {
        if !is_handler_version(&admission.handler_version) {
            return batch.reject(Violation::InvalidValue(
                "handler_version",
                "is not a handler version",
            ));
        }
        if serde_json::from_str::<serde_json::Value>(&admission.parameters).is_err() {
            return batch.reject(Violation::InvalidValue("parameters", "is not JSON"));
        }
        if !is_json_object(admission.arguments.as_str()) {
            return batch.reject(Violation::InvalidValue("arguments", "is not a JSON object"));
        }
        if admission.parameters.len() > MAX_TOOL_DEFINITION_BYTES {
            return batch.reject(Violation::ToolEvidenceLimit {
                call,
                limit: "the schema of one call",
            });
        }
        if admission.arguments.len() > MAX_STORED_TOOL_ARGUMENTS_BYTES {
            return batch.reject(Violation::ToolEvidenceLimit {
                call,
                limit: "the arguments of one call",
            });
        }
    }
    batch.tool_decide(
        call,
        &evidence.tool_call_id,
        &evidence.name,
        evidence.decision,
        evidence.spawns,
        evidence.approval,
    )
}

/// Records what is known of an admitted call's execution, once. Only the
/// trusted execution path of the Job, while it executes it, may record it,
/// and only after the call's admission. An uncertain outcome stays
/// uncertain: no later outcome replaces it.
pub(crate) fn record_tool_outcome(
    batch: &mut Batch,
    call: ToolCallRef,
    outcome: ToolOutcome,
) -> OpResult<()> {
    batch.tool_outcome(call, outcome)
}

/// Records a verified conclusion about an admitted call whose outcome is
/// uncertain, once, keeping the admission and the uncertain outcome as
/// they are. The Job must be locally stopped, so nothing still executes for
/// it; a descendant that still runs executes its own calls, not this Job's.
/// The reconciler, its evidence and its explanation must each be given.
pub(crate) fn reconcile_tool_effect(
    batch: &mut Batch,
    call: ToolCallRef,
    reconciliation: ToolReconciliation,
) -> OpResult<()> {
    if !batch.require_job_state(call.job)?.locally_stopped() {
        return batch.reject(Violation::ToolEffectStillExecuting(call.job));
    }
    let Some(mut record) = batch.tool_evidence(call)? else {
        return batch.reject(Violation::ToolCallNotFound(call));
    };
    if record.admission().is_none() {
        return batch.reject(Violation::ToolCallNotAdmitted(call));
    }
    if record.reconciliation.is_some() {
        return batch.reject(Violation::ToolEffectAlreadyReconciled(call));
    }
    if !record.is_unresolved() {
        return batch.reject(Violation::ToolEffectAlreadyResolved(call));
    }
    if reconciliation.reconciler.is_empty()
        || reconciliation.reconciler.len() > MAX_RECONCILER_BYTES
    {
        return batch.reject(Violation::InvalidValue(
            "reconciler",
            "must be 1 to 256 bytes",
        ));
    }
    for (field, text) in [
        ("evidence", &reconciliation.evidence),
        ("explanation", &reconciliation.explanation),
    ] {
        if text.is_empty() || text.len() > MAX_RECONCILIATION_TEXT_BYTES {
            return batch.reject(Violation::InvalidValue(field, "must be 1 to 4096 bytes"));
        }
    }
    record.reconciliation = Some(Recorded {
        fact: reconciliation,
        at: batch.now(),
    });
    batch.put_tool_evidence(&record)
}

/// Whether `version` can identify a handler's behavior: 1 to
/// [`MAX_HANDLER_VERSION_BYTES`] ASCII letters, digits or `.`, `-`, `_`, `+`.
pub fn is_handler_version(version: &str) -> bool {
    !version.is_empty()
        && version.len() <= MAX_HANDLER_VERSION_BYTES
        && version
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_' | b'+'))
}

/// Whether `text` is one JSON object.
pub(super) fn is_json_object(text: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(text).is_ok_and(|value| value.is_object())
}

// ---- configs ------------------------------------------------------------------------

/// Creates a config. Its references must name configs of the kinds its body
/// requires (I17).
pub(crate) fn create_config(batch: &mut Batch, config: NewConfig) -> OpResult<ConfigId> {
    batch.create_config(config.name, config.body)
}

pub(crate) fn import_models(
    batch: &mut Batch,
    connection: NewConfig,
    models: Vec<(String, Option<u32>)>,
    capabilities: std::collections::BTreeSet<crate::persistence::ModelCapability>,
) -> OpResult<(Vec<ConfigId>, usize)> {
    batch.import_models(connection, models, capabilities)
}

pub(crate) fn archive_model(batch: &mut Batch, id: ConfigId) -> OpResult<()> {
    batch.archive_model(id)
}

/// Edits a config in place: its name and body, with the same kind. The edit
/// counter rises by one; Jobs that froze the old values keep them.
pub(crate) fn update_config(batch: &mut Batch, id: ConfigId, config: NewConfig) -> OpResult<()> {
    batch.update_config(id, config.name, config.body)
}

/// Registers an Executor: a Pool that offers `LlmRequest` over each of the
/// transports its adapter implements.
pub(crate) fn create_executor(batch: &mut Batch, executor: NewExecutor) -> OpResult<ExecutorId> {
    let capabilities = executor
        .llm_transports
        .iter()
        .map(|transport| (ExecutionOperation::LlmRequest, Some(*transport)))
        .collect();
    let id = batch.create_config(
        executor.name,
        ConfigBody::Pool(PoolConfig {
            capabilities,
            concurrency_limit: executor.concurrency_limit.get(),
            enabled: true,
        }),
    )?;
    Ok(id.into())
}

pub(crate) fn create_execution_pool(
    batch: &mut Batch,
    pool: NewExecutionPool,
) -> OpResult<ExecutionPoolId> {
    let id = batch.create_config(pool.name, ConfigBody::Pool(pool.config.body(pool.enabled)))?;
    Ok(id.into())
}

/// Replaces a Pool's capabilities and limit. Whether it is enabled stays as it is.
pub(crate) fn update_execution_pool(
    batch: &mut Batch,
    id: ExecutionPoolId,
    config: ExecutionPoolConfig,
) -> OpResult<()> {
    let existing = batch.require_config_kind(id.into(), ConfigKind::Pool)?;
    let ConfigBody::Pool(body) = &existing.body else {
        return batch.reject(Violation::NotFound(RecordRef::ExecutionPool(id)));
    };
    batch.update_config(
        id.into(),
        existing.name.clone(),
        ConfigBody::Pool(config.body(body.enabled)),
    )
}

pub(crate) fn set_execution_pool_enabled(
    batch: &mut Batch,
    id: ExecutionPoolId,
    enabled: bool,
) -> OpResult<()> {
    let existing = batch.require_config_kind(id.into(), ConfigKind::Pool)?;
    let ConfigBody::Pool(mut body) = existing.body.clone() else {
        return batch.reject(Violation::NotFound(RecordRef::ExecutionPool(id)));
    };
    body.enabled = enabled;
    batch.update_config(id.into(), existing.name, ConfigBody::Pool(body))
}

// ---- attachments and projects ------------------------------------------------------------

/// A display name as OCG keeps it: the last component of a path, without
/// control characters or surrounding whitespace, cut on a character
/// boundary to [`MAX_ATTACHMENT_NAME_BYTES`]. `None` when nothing is left,
/// or when it is `.` or `..`.
#[must_use]
pub fn sanitize_display_name(name: &str) -> Option<String> {
    let last = name.rsplit(['/', '\\']).next().unwrap_or_default();
    let cleaned: String = last
        .chars()
        .filter(|character| !character.is_control())
        .collect();
    let mut cleaned = cleaned.trim();
    while cleaned.len() > MAX_ATTACHMENT_NAME_BYTES {
        let mut end = MAX_ATTACHMENT_NAME_BYTES;
        while !cleaned.is_char_boundary(end) {
            end -= 1;
        }
        cleaned = cleaned[..end].trim_end();
    }
    match cleaned {
        "" | "." | ".." => None,
        name => Some(name.to_owned()),
    }
}

/// Whether `mime` is a lowercase `type/subtype` of RFC 6838 restricted-name
/// characters, without parameters.
pub(super) fn is_mime_type(mime: &str) -> bool {
    let name = |part: &str| {
        (1..=127).contains(&part.len())
            && part.starts_with(|first: char| first.is_ascii_lowercase() || first.is_ascii_digit())
            && part.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"!#$&-^_.+".contains(&byte)
            })
    };
    mime.len() <= MAX_ATTACHMENT_MIME_BYTES
        && mime
            .split_once('/')
            .is_some_and(|(kind, subtype)| name(kind) && name(subtype))
}

/// Checks what [`create_attachment`] would refuse of `attachment`, so that
/// nothing is published for an Attachment that cannot be recorded.
pub(crate) fn check_new_attachment(attachment: &NewAttachment) -> Result<(), Violation> {
    if attachment.content.is_empty() {
        return Err(Violation::InvalidValue("content", "cannot be empty"));
    }
    if attachment.content.len() > MAX_ATTACHMENT_BYTES {
        return Err(Violation::ValueOutOfRange("content"));
    }
    if !is_mime_type(&attachment.mime) {
        return Err(Violation::InvalidValue(
            "mime",
            "must be a lowercase type/subtype without parameters",
        ));
    }
    Ok(())
}

/// What a new Attachment records of its content, once that content is
/// published under `sha256`.
pub(crate) struct PublishedAttachment {
    pub(crate) sha256: ContentDigest,
    pub(crate) byte_length: u64,
    pub(crate) mime: String,
    pub(crate) display_name: Option<String>,
    pub(crate) origin: AttachmentOrigin,
}

/// Records an Attachment whose content is already published. Its display
/// name is sanitized here.
pub(crate) fn create_attachment(
    batch: &mut Batch,
    attachment: PublishedAttachment,
) -> OpResult<AttachmentId> {
    if !is_mime_type(&attachment.mime) {
        return batch.reject(Violation::InvalidValue(
            "mime",
            "must be a lowercase type/subtype without parameters",
        ));
    }
    if attachment.byte_length == 0 {
        return batch.reject(Violation::InvalidValue("content", "cannot be empty"));
    }
    let id = AttachmentId(batch.next_id()?);
    batch.insert_attachment(&AttachmentRecord {
        id,
        sha256: attachment.sha256,
        byte_length: attachment.byte_length,
        mime: attachment.mime,
        display_name: attachment
            .display_name
            .as_deref()
            .and_then(sanitize_display_name),
        origin: attachment.origin,
        created_at: batch.now(),
        cleanup: None,
    })?;
    Ok(id)
}

/// Records that an available Attachment was cleaned up: `kind` and the
/// batch's time, in one write. Nothing else of it changes, nor any input
/// that names it. An Attachment already cleaned up keeps its cleanup, and
/// the batch is refused with [`Violation::AttachmentCleanedUp`].
pub(crate) fn cleanup_attachment(
    batch: &mut Batch,
    id: AttachmentId,
    kind: CleanupKind,
) -> OpResult<AttachmentRecord> {
    let mut record = batch.require_attachment(id)?;
    if !record.is_available() {
        return batch.reject(Violation::AttachmentCleanedUp(id));
    }
    record.cleanup = Some(AttachmentCleanup {
        kind,
        cleaned_at: batch.now(),
    });
    batch.insert_attachment(&record)?;
    Ok(record)
}

/// Creates a Project. Its root, which the caller resolved, may not equal,
/// contain or lie inside another Project's root.
pub(crate) fn create_project(batch: &mut Batch, project: NewProject) -> OpResult<ProjectId> {
    if let Err(reason) = validate_project_commands(&project.commands) {
        return batch.reject(Violation::InvalidValue("commands", reason));
    }
    let root = std::path::Path::new(&project.root);
    for (existing, other) in batch.project_roots()? {
        let other = std::path::Path::new(&other);
        if other.starts_with(root) || root.starts_with(other) {
            return batch.reject(Violation::ProjectRootOverlap { project: existing });
        }
    }
    let id = ProjectId(batch.next_id()?);
    let now = batch.now();
    batch.insert_project(&ProjectRecord {
        id,
        name: project.name,
        description: project.description,
        root: project.root,
        commands: project.commands,
        created_at: now,
        updated_at: now,
    })?;
    Ok(id)
}

/// Replaces a Project's name, description and commands. Its root stays.
pub(crate) fn update_project(
    batch: &mut Batch,
    id: ProjectId,
    name: String,
    description: Option<String>,
    commands: Vec<ProjectCommand>,
) -> OpResult<()> {
    if let Err(reason) = validate_project_commands(&commands) {
        return batch.reject(Violation::InvalidValue("commands", reason));
    }
    let mut record = batch.require_project_record(id)?;
    record.name = name;
    record.description = description;
    record.commands = commands;
    record.updated_at = batch.now();
    batch.insert_project(&record)
}

// ---- conversations --------------------------------------------------------------------------

/// Conversation edits apply only to specs of kind Conversation.
fn require_conversation(batch: &mut Batch, id: JobId) -> OpResult<()> {
    let row = match batch.job(id)? {
        Some(row) => row,
        None => return batch.reject(Violation::NotFound(RecordRef::Job(id))),
    };
    if row.kind.is_conversation() {
        Ok(())
    } else {
        batch.reject(Violation::NotFound(RecordRef::Job(id)))
    }
}

/// Edits conversation metadata or limits. Hiding twice changes nothing, and no
/// Job is touched.
pub(crate) fn set_conversation(
    batch: &mut Batch,
    root: JobId,
    edit: ConversationEdit,
) -> OpResult<()> {
    require_conversation(batch, root)?;
    let mut row = batch.conversation(root)?;
    match edit {
        ConversationEdit::Limits(limits) => {
            return batch.set_conversation_limits(root, limits, super::Actor::User);
        }
        ConversationEdit::Title(title) => row.title = Some(title),
        ConversationEdit::SuggestedTitle(_) if row.title.is_some() => return Ok(()),
        ConversationEdit::SuggestedTitle(title) => row.title = Some(title),
        ConversationEdit::Hide if row.hidden_at.is_some() => return Ok(()),
        ConversationEdit::Hide => row.hidden_at = Some(batch.now()),
    }
    batch.insert_conversation(root, &row)?;
    batch.refresh_recent(root)
}

// ---- request events -------------------------------------------------------------------------

/// Appends measurements for a Job as request events. Their LLM, if any, is
/// the one the Job froze; the Job itself is not changed.
///
/// A Token annotation must agree with the eligibility the request's
/// committed annotations state, so contradictory evidence is refused here, in
/// the transaction that would write it, rather than met later by a reader.
pub(crate) fn record_metrics(batch: &mut Batch, metrics: NewMetrics) -> OpResult<MetricsId> {
    let job = batch.require_job(metrics.job)?;
    if metrics.measurements.is_empty() && metrics.token.is_none() {
        return batch.reject(Violation::InvalidValue(
            "measurements",
            "records no measurement",
        ));
    }
    if let Some(token) = metrics.token {
        if job.kind.is_conversation() {
            return batch.reject(Violation::NotAnLlmExecution(metrics.job));
        }
        if token.eligibility == RequestEligibility::Excluded
            && (token.finality != UsageFinality::Cumulative
                || metrics.measurements.usage != NormalizedUsage::default())
        {
            return batch.reject(Violation::InvalidValue(
                "token",
                "a request that was never sent reports no usage",
            ));
        }
        if let Some(committed) = batch.request_token_eligibility(metrics.job, metrics.request)?
            && committed != token.eligibility
        {
            return batch.reject(Violation::ContradictoryTokenEligibility {
                job: metrics.job,
                request: metrics.request,
                committed,
                refused: token.eligibility,
            });
        }
    }
    let (old_sample, events) = batch.usage_sample(metrics.job, metrics.request)?;
    let mut id = None;
    let mut inserted = Vec::new();
    if !metrics.measurements.is_empty() {
        let event = batch.insert_request_event(
            metrics.job,
            metrics.request,
            metrics.observed_at,
            RequestPayload::Measurement {
                measurements: metrics.measurements,
            },
        )?;
        id = Some(event.id);
        inserted.push(event);
    }
    if let Some(token) = metrics.token {
        let event = batch.insert_request_event(
            metrics.job,
            metrics.request,
            metrics.observed_at,
            RequestPayload::Token {
                eligibility: token.eligibility,
                finality: token.finality,
            },
        )?;
        id.get_or_insert(event.id);
        inserted.push(event);
    }
    batch.update_usage_total(metrics.job, metrics.request, old_sample, events, inserted)?;
    Ok(MetricsId::new(id.map_or(0, |event| event.get())))
}

/// Appends one availability observation of an LLM request made by a Job
/// that was submitted with an LLM configuration. Neither the Job nor its
/// outcome is changed or consulted.
pub(crate) fn record_availability_stat(
    batch: &mut Batch,
    stat: NewAvailabilityStat,
) -> OpResult<AvailabilityStatId> {
    let job = batch.require_job(stat.job)?;
    if job.kind.is_conversation() {
        return batch.reject(Violation::NotAnLlmExecution(stat.job));
    }
    if stat.observation.is_empty() {
        return batch.reject(Violation::InvalidValue(
            "observation",
            "records no response or failure",
        ));
    }
    let event = batch.insert_request_event(
        stat.job,
        stat.request,
        stat.observed_at,
        RequestPayload::Availability {
            status_code: stat.observation.status_code,
            finish_reason: stat.observation.finish_reason,
            raw_finish_reason: stat.observation.raw_finish_reason,
            failure: stat.observation.failure,
        },
    )?;
    Ok(AvailabilityStatId::new(event.id.get()))
}
