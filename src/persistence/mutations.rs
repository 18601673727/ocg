use super::*;

pub(crate) type OpResult<T> = Result<(T, MarkerSubjects), BatchError>;

/// Creates and starts one cleanup Job under `owner`. The cleanup runs in this
/// process, so the Job is started as a local execution of the opening of
/// `epoch`; if the process ends before the Job is finalized, the next start
/// of the database settles it like any abandoned local work.
pub(crate) fn begin_core_log_cleanup(
    batch: &mut Batch,
    owner: JobId,
    executor: ExecutorId,
    started: NewJobOutput,
    epoch: u64,
) -> OpResult<crate::log_cleanup::CleanupRecords> {
    batch.require_core_executor(executor)?;
    let (input, _) = save_job_input(batch, None, JobInputDraft {
        objective: Some("Remove expired OCG runtime logs".into()),
        ..JobInputDraft::default()
    })?;
    let (job, _) = create_job_with_input(batch, NewJob {
        name: Some("__CORE__ log cleanup".into()),
        execution_mode: ExecutionMode::Once,
        parent: owner,
        projects: Vec::new(),
        configuration: None,
        assigned_executor: Some(executor),
        dependencies: Vec::new(),
        eligible_at: None,
        scheduled_at: None,
        next_due_at: None,
        next_iteration_no: None,
        iteration_no: None,
        previous: None,
        replaces: None,
    }, input)?;
    start_local_job(batch, job, epoch)?;
    publish_output(batch, job, started.kind.into(), started.summary, started.code, started.goal_reached, started.value)?;
    Ok((crate::log_cleanup::CleanupRecords { job },
        MarkerSubjects::created(RecordRef::Job(owner), RecordRef::Job(job))))
}

/// Without `existing`, creates a new unsubmitted JobInput. With `existing`,
/// replaces its editable fields, which is refused once the input is submitted.
pub(crate) fn save_job_input(
    batch: &mut Batch,
    existing: Option<JobInputId>,
    draft: JobInputDraft,
) -> OpResult<JobInputId> {
    let now = batch.now();
    for attachment in &draft.attachments {
        if !batch.require_attachment(*attachment)?.is_available() {
            return batch.reject(Violation::AttachmentCleanedUp(*attachment));
        }
    }
    let row = match existing {
        None => {
            let id = JobInputId(batch.next_id(crate::storage::Sequence::JobInput)?);
            let row = JobInputRow {
                id,
                submission_id: draft.submission_id,
                objective: draft.objective,
                max_depth: draft.max_depth,
                max_children_per_job: draft.max_children_per_job,
                max_total_descendants: draft.max_total_descendants,
                submitted_at: None,
                created_at: now,
                updated_at: now,
                configuration: None,
            };
            batch.insert_input(&row)?;
            batch.set_input_attachments(id, &draft.attachments)?;
            return Ok((id, MarkerSubjects::created(RecordRef::JobInput(id), RecordRef::JobInput(id))));
        }
        Some(id) => batch.require_input(id)?,
    };
    if row.submitted_at.is_some() {
        return batch.reject(Violation::InputAlreadySubmitted(row.id));
    }
    let row = JobInputRow {
        submission_id: draft.submission_id,
        objective: draft.objective,
        max_depth: draft.max_depth,
        max_children_per_job: draft.max_children_per_job,
        max_total_descendants: draft.max_total_descendants,
        updated_at: now,
        ..row
    };
    batch.put_input(&row)?;
    batch.set_input_attachments(row.id, &draft.attachments)?;
    Ok((row.id, MarkerSubjects::addressed(RecordRef::JobInput(row.id))))
}

/// The fields and relationships of a Job about to be admitted, from either a
/// submission or a retry.
pub(super) struct Admission {
    name: Option<String>,
    execution_mode: ExecutionMode,
    eligible_at: Option<Timestamp>,
    scheduled_at: Option<Timestamp>,
    next_due_at: Option<Timestamp>,
    next_iteration_no: Option<i64>,
    iteration_no: Option<i64>,
    parent: JobId,
    projects: Vec<ProjectId>,
    dependencies: Vec<(JobId, DependencyCondition)>,
    configuration: Option<ExecutionConfiguration>,
    /// The LLM configuration frozen for `configuration`, whose transport an
    /// assigned Executor must support.
    frozen_llm: Option<FrozenLlmConfig>,
    assigned_executor: Option<ExecutorId>,
    previous: Option<JobId>,
    replaces: Option<JobId>,
    retry_of: Option<JobId>,
}

/// An input that no Job uses yet; submitting a Job freezes it.
pub(super) fn require_unsubmitted_input(batch: &mut Batch, input: JobInputId) -> Result<JobInputRow, BatchError> {
    let row = batch.require_input(input)?;
    if row.submitted_at.is_some() || batch.input_has_jobs(input)? {
        return batch.reject(Violation::InputAlreadySubmitted(input));
    }
    Ok(row)
}

/// Freezes the input, together with the execution configuration of the Job
/// submitted with it.
pub(super) fn submit_input(batch: &mut Batch, mut row: JobInputRow, configuration: Option<FrozenExecutionConfig>) -> Result<(), BatchError> {
    let now = batch.now();
    row.submitted_at = Some(now);
    row.updated_at = now;
    row.configuration = configuration;
    batch.put_input(&row)
}

/// Creates the Job with every relationship fixed at submission, after
/// validating each endpoint and the combined waiting graph with all
/// proposed waiting edges included.
pub(super) fn admit_job(
    batch: &mut Batch,
    job: Admission,
    input: JobInputId,
) -> Result<JobId, BatchError> {
    let now = batch.now();
    let parent_row = batch.require_job(job.parent)?;
    if parent_row.any_terminal() || parent_row.sterilized_at.is_some() {
        return batch.reject(Violation::ParentClosed(job.parent));
    }
    for (index, project) in job.projects.iter().enumerate() {
        if job.projects[..index].contains(project) {
            return batch.reject(Violation::DuplicateReference(RecordRef::Project(*project)));
        }
        batch.require_project(*project)?;
    }
    let mut dependency_rows = Vec::with_capacity(job.dependencies.len());
    for (index, (dependency, _)) in job.dependencies.iter().enumerate() {
        if job.dependencies[..index].iter().any(|(earlier, _)| earlier == dependency) {
            return batch.reject(Violation::DuplicateDependency(*dependency));
        }
        dependency_rows.push(batch.require_job(*dependency)?);
    }
    if let Some(configuration) = job.configuration {
        batch.require_configuration(configuration)?;
    }
    if let Some(executor) = job.assigned_executor {
        batch.require_executor_for(executor, job.frozen_llm.as_ref())?;
    }
    for lineage in [job.previous, job.replaces, job.retry_of].into_iter().flatten() {
        batch.require_job(lineage)?;
    }

    let id = JobId(batch.next_id(crate::storage::Sequence::Job)?);
    let mut proposed = vec![(job.parent, id)];
    for row in &dependency_rows {
        if !batch.subtree_settled(row)? {
            proposed.push((id, row.id));
        }
    }
    batch.check_waiting(id, &proposed)?;

    batch.insert_job(&JobRow {
        id,
        name: job.name,
        execution_mode: job.execution_mode,
        eligible_at: job.eligible_at,
        started_at: None,
        paused_at: None,
        sterilized_at: None,
        completed_at: None,
        failed_at: None,
        cancelled_at: None,
        unknown_at: None,
        stop_confirmed_at: None,
        next_due_at: job.next_due_at,
        next_iteration_no: job.next_iteration_no,
        iteration_no: job.iteration_no,
        scheduled_at: job.scheduled_at,
        created_at: now,
        updated_at: now,
    })?;
    batch.link_input(id, input)?;
    batch.link_child(id, job.parent)?;
    for project in job.projects {
        batch.link_project(id, project)?;
    }
    for (dependency, condition) in &job.dependencies {
        batch.link_dependency(id, *dependency, *condition)?;
    }
    if let Some(configuration) = job.configuration {
        batch.link_configuration(id, configuration)?;
    }
    if let Some(executor) = job.assigned_executor {
        batch.set_assigned_executor(id, executor)?;
    }
    batch.link_lineage(id, job.previous, job.replaces)?;
    if let Some(original) = job.retry_of {
        batch.link_retry(id, original)?;
    }
    Ok(id)
}

/// Submits the input and creates its Job in one transaction. A configured
/// Job's configuration is read from its Profiles and frozen into the input.
pub(crate) fn create_job_with_input(batch: &mut Batch, job: NewJob, input: JobInputId) -> OpResult<JobId> {
    let input_row = require_unsubmitted_input(batch, input)?;
    let frozen = match job.configuration {
        Some(configuration) => Some(batch.freeze_configuration(configuration)?),
        None => None,
    };
    let id = admit_job(batch, Admission {
        name: job.name,
        execution_mode: job.execution_mode,
        eligible_at: job.eligible_at,
        scheduled_at: job.scheduled_at,
        next_due_at: job.next_due_at,
        next_iteration_no: job.next_iteration_no,
        iteration_no: job.iteration_no,
        parent: job.parent,
        projects: job.projects,
        dependencies: job.dependencies,
        configuration: job.configuration,
        frozen_llm: frozen.as_ref().map(|frozen| frozen.llm.clone()),
        assigned_executor: job.assigned_executor,
        previous: job.previous,
        replaces: job.replaces,
        retry_of: None,
    }, input)?;
    submit_input(batch, input_row, frozen)?;
    Ok((id, MarkerSubjects::created(RecordRef::JobInput(input), RecordRef::Job(id))))
}

/// Submits the input, creates its Job and starts it as a local execution of
/// the opening of `epoch`, all in one transaction. No committed Job of this
/// process's own execution is ever left holding its Profiles without the
/// claim by which a later opening settles it; see [`start_local_job`].
pub(crate) fn create_local_job_with_input(
    batch: &mut Batch,
    job: NewJob,
    input: JobInputId,
    epoch: u64,
) -> OpResult<JobId> {
    let (id, subjects) = create_job_with_input(batch, job, input)?;
    start_local_job(batch, id, epoch)?;
    Ok((id, subjects))
}

/// Like [`create_local_job_with_input`], and records `submission` as the
/// one submission that created the Job, in the same transaction. A key that
/// is already recorded refuses the whole creation, so a key never names two
/// Jobs, and a recorded key always names the Job created with it.
pub(crate) fn create_submitted_local_job(
    batch: &mut Batch,
    job: NewJob,
    input: JobInputId,
    epoch: u64,
    submission: NewSubmission,
) -> OpResult<JobId> {
    if let Some(recorded) = batch.submission(submission.key)? {
        return batch.reject(Violation::SubmissionKeyUsed {
            job: recorded.job,
            same_request: recorded.fingerprint == submission.fingerprint,
        });
    }
    let (id, subjects) = create_local_job_with_input(batch, job, input, epoch)?;
    let recorded_at = batch.now();
    batch.insert_submission(&SubmissionRecord {
        key: submission.key,
        fingerprint: submission.fingerprint,
        job: id,
        recorded_at,
    })?;
    Ok((id, subjects))
}

/// Creates a new Job that retries `retry.original` under the original's
/// parent. The original Job, its input and its outputs are not touched.
///
/// Only an original whose subtree has settled may be retried, and only
/// while none of its direct retries has an unsettled subtree; both are
/// checked in the transaction that creates the retry, so concurrent retries
/// cannot both become active. An original that stopped locally while a
/// descendant still runs is not retried yet. The original's tool effects
/// are checked in the same transaction, see [`require_repeatable_tool_effects`].
///
/// The retry's configuration is frozen from its Profiles as they are now. It
/// may share the original's input only when that equals what the input
/// froze; Profile IDs alone prove nothing once a Profile has been edited.
pub(crate) fn retry_job(batch: &mut Batch, retry: JobRetry) -> OpResult<JobId> {
    let original = retry.original;
    let original_row = batch.require_job(original)?;
    if batch.genesis()? == original {
        return batch.reject(Violation::GenesisProtected(original));
    }
    if !batch.subtree_settled(&original_row)? {
        return batch.reject(Violation::RetryOfUnsettledJob(original));
    }
    for existing in batch.retries(original)? {
        let existing_row = batch.require_job(existing)?;
        if !batch.subtree_settled(&existing_row)? {
            return batch.reject(Violation::ActiveRetry {
                original,
                retry: existing,
            });
        }
    }
    require_repeatable_tool_effects(batch, original, retry.acknowledge_repeated_effects)?;
    let parent = batch.parent_of(original)?;
    let frozen = match retry.configuration {
        Some(configuration) => Some(batch.freeze_configuration(configuration)?),
        None => None,
    };
    let (input, unsubmitted) = match retry.input {
        RetryInput::Unchanged => {
            let input = batch.input_of(original)?;
            if batch.require_input(input)?.configuration != frozen {
                return batch.reject(Violation::ConfigurationMismatch(input));
            }
            (input, None)
        }
        RetryInput::Changed(input) => (input, Some(require_unsubmitted_input(batch, input)?)),
    };
    let id = admit_job(batch, Admission {
        name: retry.name,
        execution_mode: retry.execution_mode,
        eligible_at: retry.eligible_at,
        scheduled_at: retry.scheduled_at,
        next_due_at: retry.next_due_at,
        next_iteration_no: retry.next_iteration_no,
        iteration_no: retry.iteration_no,
        parent,
        projects: retry.projects,
        dependencies: retry.dependencies,
        configuration: retry.configuration,
        frozen_llm: frozen.as_ref().map(|frozen| frozen.llm.clone()),
        assigned_executor: retry.assigned_executor,
        previous: None,
        replaces: None,
        retry_of: Some(original),
    }, input)?;
    if let Some(row) = unsubmitted {
        submit_input(batch, row, frozen)?;
    }
    Ok((id, MarkerSubjects::created(RecordRef::Job(original), RecordRef::Job(id))))
}

/// Records that the Job's execution began, which requires every explicit
/// dependency of the Job to be satisfied. A refused start changes nothing: a
/// Job still waiting may start later, and one whose dependency became
/// impossible stays as it is until it is finalized explicitly.
pub(crate) fn start_job(batch: &mut Batch, job: JobId) -> OpResult<()> {
    let now = batch.now();
    let mut row = batch.require_job(job)?;
    if row.any_terminal() {
        return batch.reject(Violation::JobHasTerminalOutcome(job));
    }
    if row.started_at.is_some() {
        return batch.reject(Violation::JobAlreadyStarted(job));
    }
    require_satisfied_dependencies(batch, job)?;
    row.started_at = Some(now);
    row.updated_at = now;
    batch.put_job(&row)?;
    Ok(((), MarkerSubjects::addressed(RecordRef::Job(job))))
}

/// Records that the Job's execution began in this process, under the
/// opening of `epoch`. Besides the start, it claims the execution for that
/// opening until the Job settles, so a later opening can tell that work this
/// opening left unsettled can no longer be running; see
/// [`reconcile_abandoned_job`]. The Genesis Job is never executed.
pub(crate) fn start_local_job(batch: &mut Batch, job: JobId, epoch: u64) -> OpResult<()> {
    if batch.genesis()? == job {
        return batch.reject(Violation::GenesisProtected(job));
    }
    let started = start_job(batch, job)?;
    batch.claim_local_execution(job, epoch)?;
    Ok(started)
}

/// Starts a Job locally as a database written before dependencies were
/// checked at the start could hold it: without that check.
#[cfg(test)]
pub(crate) fn start_preexisting_local_job(
    batch: &mut Batch,
    job: JobId,
    epoch: u64,
) -> OpResult<()> {
    let now = batch.now();
    let mut row = batch.require_job(job)?;
    row.started_at = Some(now);
    row.updated_at = now;
    batch.put_job(&row)?;
    batch.claim_local_execution(job, epoch)?;
    Ok(((), MarkerSubjects::addressed(RecordRef::Job(job))))
}

/// Settles a Job that the opening of `epoch` started as its own local
/// execution and left unsettled. Runs only when the database is opened, so
/// that opening is closed: nothing it ran can still execute or settle the
/// Job. An unsettled Job's outcome becomes unknown, never a claimed result,
/// and its stop is confirmed, which releases the Profiles it holds and
/// settles it. An unknown outcome only has its stop confirmed. Children are
/// not touched: a parent still needs each of them to be settled. Neither is
/// the Job's tool evidence: a call admitted without an outcome stays
/// unresolved, never taken as not applied.
///
/// Declines when the Job no longer has that claim, so running it again
/// changes nothing.
pub(crate) fn reconcile_abandoned_job(
    batch: &mut Batch,
    job: JobId,
    epoch: u64,
) -> Result<Option<((), MarkerSubjects)>, BatchError> {
    if batch.local_execution(job)? != Some(epoch) {
        return Ok(None);
    }
    let now = batch.now();
    let mut row = batch.require_job(job)?;
    if batch.genesis()? != job && row.holds_profiles() {
        row.unknown_at.get_or_insert(now);
        row.stop_confirmed_at = Some(now);
        row.updated_at = now;
        batch.put_job(&row)?;
    }
    batch.release_local_execution(job)?;
    Ok(Some(((), MarkerSubjects::addressed(RecordRef::Job(job)))))
}

/// Replaces the Job's main Executor; refused once the Job has started. The
/// Executor must support the transport of the LLM configuration the Job
/// froze, if any; the Job's Provider, model and parameters stay as frozen.
pub(crate) fn assign_job_executor(batch: &mut Batch, job: JobId, executor: ExecutorId) -> OpResult<()> {
    let row = batch.require_job(job)?;
    let frozen = batch.job_frozen_llm(job)?;
    batch.require_executor_for(executor, frozen.as_ref())?;
    if row.started_at.is_some() || row.any_terminal() {
        return batch.reject(Violation::ExecutorChangeAfterStart(job));
    }
    batch.set_assigned_executor(job, executor)?;
    Ok(((), MarkerSubjects::addressed(RecordRef::Job(job))))
}

/// Adds one more Project to the Job's Project set.
pub(crate) fn associate_job_project(batch: &mut Batch, job: JobId, project: ProjectId) -> OpResult<()> {
    batch.require_job(job)?;
    batch.require_project(project)?;
    batch.link_project(job, project)?;
    Ok(((), MarkerSubjects::addressed(RecordRef::Job(job))))
}

pub(super) fn seq_i64(batch: &mut Batch, seq: u64) -> Result<i64, BatchError> {
    match i64::try_from(seq) {
        Ok(seq) => Ok(seq),
        Err(_) => batch.reject(Violation::ValueOutOfRange("seq")),
    }
}

/// Intermediate outputs are refused once the Job has any terminal outcome,
/// unknown included, so a final result stays the Job's newest output and an
/// unknown Job's recorded outputs stay as they were.
pub(crate) fn append_job_output(batch: &mut Batch, job: JobId, output: NewJobOutput) -> OpResult<AppendedOutput> {
    let row = batch.require_job(job)?;
    if row.any_terminal() {
        return batch.reject(Violation::JobHasTerminalOutcome(job));
    }
    let appended = publish_output(batch, job, output.kind.into(), output.summary, output.code, output.goal_reached, output.value)?;
    let output_id = appended.output;
    Ok((appended, MarkerSubjects::created(RecordRef::Job(job), RecordRef::JobOutput(output_id))))
}

/// Shared by intermediate appends and finalization so that both publish an
/// output, its ownership edge, and the LATEST_OUTPUT pointer identically.
pub(super) fn publish_output(
    batch: &mut Batch,
    job: JobId,
    kind: OutputKind,
    summary: Option<String>,
    code: Option<String>,
    goal_reached: Option<bool>,
    value: Option<serde_json::Value>,
) -> Result<AppendedOutput, BatchError> {
    let seq = batch.last_output_seq(job)? + 1;
    let id = JobOutputId(batch.next_id(crate::storage::Sequence::JobOutput)?);
    let record = JobOutputRecord {
        id,
        job,
        seq: seq_i64(batch, seq)?,
        kind,
        summary,
        code,
        goal_reached,
        value,
        created_at: batch.now(),
    };
    batch.insert_output(&record)?;
    Ok(AppendedOutput { output: id, seq: record.seq })
}

/// A confirmed outcome requires the subtree of every child Job to be
/// settled: a child with an unknown outcome counts once its execution is
/// confirmed stopped and so is every descendant of it. A child's own
/// outcome does not decide the parent's; a required success is
/// an explicit Success dependency. Completion also requires every explicit
/// dependency of the Job to be satisfied, while failure and cancellation do
/// not. The Job is then sterilized, its final result published, and the
/// terminal timestamp set in one transaction.
///
/// A confirmed outcome also requires every tool call the Job admitted to
/// have a definite outcome or a reconciliation: a Job whose tool effects
/// are unresolved can only become unknown.
///
/// An unknown outcome publishes no result and claims nothing about children;
/// it stops further child creation.
///
/// The Genesis Job is never finalized: any terminal outcome would close the
/// instance's root to all further work.
pub(crate) fn finalize_job(batch: &mut Batch, job: JobId, finalization: JobFinalization) -> OpResult<Option<AppendedOutput>> {
    let now = batch.now();
    let mut row = batch.require_job(job)?;
    if batch.genesis()? == job {
        return batch.reject(Violation::GenesisProtected(job));
    }
    if row.any_terminal() {
        return batch.reject(Violation::JobAlreadyFinalized(job));
    }
    let completes = matches!(finalization, JobFinalization::Completed(_));
    let (outcome, result): (fn(&mut JobRow) -> &mut Option<Timestamp>, FinalResult) = match finalization {
        JobFinalization::Unknown => {
            row.unknown_at = Some(now);
            row.updated_at = now;
            batch.put_job(&row)?;
            return Ok((None, MarkerSubjects::addressed(RecordRef::Job(job))));
        }
        JobFinalization::Completed(result) => (|row| &mut row.completed_at, result),
        JobFinalization::Failed(result) => (|row| &mut row.failed_at, result),
        JobFinalization::Cancelled(result) => (|row| &mut row.cancelled_at, result),
    };
    for child in batch.children(job)? {
        let child_row = batch.require_job(child)?;
        if !batch.subtree_settled(&child_row)? {
            return batch.reject(Violation::UnfinishedChild(job));
        }
    }
    require_resolved_tool_effects(batch, job)?;
    if completes {
        require_satisfied_dependencies(batch, job)?;
    }
    row.sterilized_at.get_or_insert(now);
    let appended = publish_output(batch, job, OutputKind::Result, result.summary, result.code, result.goal_reached, result.value)?;
    *outcome(&mut row) = Some(now);
    row.updated_at = now;
    batch.put_job(&row)?;
    // A confirmed outcome settles the Job, so no opening owns its execution.
    batch.release_local_execution(job)?;
    let output_id = appended.output;
    Ok((Some(appended), MarkerSubjects::created(RecordRef::Job(job), RecordRef::JobOutput(output_id))))
}

/// Refuses unless every tool call the Job admitted has a definite outcome
/// or a reconciliation.
pub(super) fn require_resolved_tool_effects(
    batch: &mut Batch,
    job: JobId,
) -> Result<(), BatchError> {
    for record in batch.job_tool_evidence(job)? {
        if record.is_unresolved() {
            return batch.reject(Violation::UnresolvedToolEffect(record.call));
        }
    }
    Ok(())
}

/// Refuses a retry of `original` while a tool call of it, or of any Job it
/// retries in turn through RETRY_OF, that may change something outside OCG
/// is unresolved, whatever the retry acknowledges; and a retry that could
/// repeat a non-idempotent effect one of them is known to have had unless
/// this retry acknowledges that. An earlier retry's acknowledgement is its
/// own: a retry of a retry repeats the effects of the whole chain.
pub(super) fn require_repeatable_tool_effects(
    batch: &mut Batch,
    original: JobId,
    acknowledged: bool,
) -> Result<(), BatchError> {
    let mut unacknowledged = None;
    for job in batch.retry_lineage(original)? {
        for record in batch.job_tool_evidence(job)? {
            let Some(effect) = record.admission().map(|admission| admission.effect) else {
                continue;
            };
            if record.is_unresolved() && effect != ToolEffect::ReadOnly {
                return batch.reject(Violation::UnresolvedToolEffect(record.call));
            }
            if record.is_applied() && effect == ToolEffect::NonIdempotent && !acknowledged {
                unacknowledged.get_or_insert(record.call);
            }
        }
    }
    match unacknowledged {
        Some(call) => batch.reject(Violation::RepeatedEffectNotAcknowledged(call)),
        None => Ok(()),
    }
}

/// Refuses unless the Job is being executed by this process: started
/// locally, so it holds a local execution claim, and without any outcome.
pub(super) fn require_executing_locally(batch: &mut Batch, job: JobId) -> Result<(), BatchError> {
    let row = batch.require_job(job)?;
    if row.any_terminal() || batch.local_execution(job)?.is_none() {
        return batch.reject(Violation::NotExecutingLocally(job));
    }
    Ok(())
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

/// Records OCG's decision about a call a Provider requested, before
/// anything about the call can run: its refusal, or its admission with the
/// exact registration, arguments and authorization it runs under. The call
/// is identified by its Job's Provider request, which must have a recorded
/// observation, and its wire index there; the Provider's id for it must be
/// unused in the Job. A call has one decision, never replaced.
///
/// Only the trusted execution path of the Job, while it executes it, may
/// record one. An admission is refused beyond the stored evidence limits:
/// [`MAX_STORED_TOOL_ARGUMENTS_BYTES`] per call, and
/// [`MAX_STORED_TOOL_ARGUMENTS_PER_JOB_BYTES`] and
/// [`MAX_STORED_TOOL_SCHEMAS_PER_JOB_BYTES`] per Job.
pub(crate) fn record_tool_decision(batch: &mut Batch, evidence: NewToolEvidence) -> OpResult<()> {
    let call = evidence.call;
    require_executing_locally(batch, call.job)?;
    if evidence.tool_call_id.is_empty() || evidence.tool_call_id.len() > MAX_TOOL_CALL_ID_BYTES {
        return batch.reject(Violation::InvalidValue(
            "tool_call_id",
            "must be 1 to 256 bytes",
        ));
    }
    if !is_function_name(&evidence.name) {
        return batch.reject(Violation::InvalidValue("name", "is not a function name"));
    }
    if !batch.request_observed(call.job, call.request)? {
        return batch.reject(Violation::RequestNotObserved {
            job: call.job,
            request: call.request,
        });
    }
    if let ToolDecision::Admitted(admission) = &evidence.decision {
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
        let (mut arguments, mut schemas) = (admission.arguments.len(), admission.parameters.len());
        for record in batch.job_tool_evidence(call.job)? {
            if let Some(earlier) = record.admission() {
                arguments = arguments.saturating_add(earlier.arguments.len());
                schemas = schemas.saturating_add(earlier.parameters.len());
            }
        }
        if arguments > MAX_STORED_TOOL_ARGUMENTS_PER_JOB_BYTES {
            return batch.reject(Violation::ToolEvidenceLimit {
                call,
                limit: "the arguments of one Job",
            });
        }
        if schemas > MAX_STORED_TOOL_SCHEMAS_PER_JOB_BYTES {
            return batch.reject(Violation::ToolEvidenceLimit {
                call,
                limit: "the schemas of one Job",
            });
        }
    }
    let record = ToolEvidenceRecord {
        call,
        tool_call_id: evidence.tool_call_id,
        name: evidence.name,
        decision: Recorded {
            fact: evidence.decision,
            at: batch.now(),
        },
        outcome: None,
        reconciliation: None,
    };
    batch.insert_tool_evidence(&record)?;
    Ok(((), MarkerSubjects::addressed(RecordRef::Job(call.job))))
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
    require_executing_locally(batch, call.job)?;
    let Some(mut record) = batch.tool_evidence(call)? else {
        return batch.reject(Violation::ToolCallNotFound(call));
    };
    if record.admission().is_none() {
        return batch.reject(Violation::ToolCallNotAdmitted(call));
    }
    if record.outcome.is_some() {
        return batch.reject(Violation::ToolOutcomeAlreadyRecorded(call));
    }
    record.outcome = Some(Recorded {
        fact: outcome,
        at: batch.now(),
    });
    batch.put_tool_evidence(&record)?;
    Ok(((), MarkerSubjects::addressed(RecordRef::Job(call.job))))
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
    if !batch.require_job(call.job)?.locally_stopped() {
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
    batch.put_tool_evidence(&record)?;
    Ok(((), MarkerSubjects::addressed(RecordRef::Job(call.job))))
}

/// Refuses unless every explicit dependency of the Job is satisfied.
pub(super) fn require_satisfied_dependencies(
    batch: &mut Batch,
    job: JobId,
) -> Result<(), BatchError> {
    for (prerequisite, condition) in batch.dependencies(job)? {
        let row = batch.require_job(prerequisite)?;
        let subtree_settled =
            matches!(condition, DependencyCondition::Settled) && batch.subtree_settled(&row)?;
        let state = dependency_state_with(condition, &row, subtree_settled);
        if state != DependencyState::Satisfied {
            return batch.reject(Violation::UnsatisfiedDependency {
                job,
                prerequisite,
                state,
            });
        }
    }
    Ok(())
}

/// Registers an Executor with the LLM transports its adapter implements. It
/// names no Profile or model, so it holds no Profile lock and can run any
/// LLM configuration whose Provider speaks one of those transports.
pub(crate) fn create_executor(batch: &mut Batch, executor: NewExecutor) -> OpResult<ExecutorId> {
    let now = batch.now();
    if !executor.llm_transports.is_empty() && executor.executor_type != ExecutorType::Llm {
        return batch.reject(Violation::InvalidValue("llm_transports", "only an Llm Executor can run LLM transports"));
    }
    let id = ExecutorId(batch.next_id(crate::storage::Sequence::Executor)?);
    batch.insert_executor(&ExecutorRow {
        id,
        name: executor.name,
        executor_type: executor.executor_type,
        concurrency_limit: executor.concurrency_limit.get(),
        latest_enabled_at: None,
        latest_disabled_at: None,
        created_at: now,
        updated_at: now,
        llm_transports: executor.llm_transports,
    })?;
    Ok((id, MarkerSubjects::created(RecordRef::Executor(id), RecordRef::Executor(id))))
}

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
    let id = AttachmentId(batch.next_id(crate::storage::Sequence::Attachment)?);
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
    Ok((
        id,
        MarkerSubjects::created(RecordRef::Attachment(id), RecordRef::Attachment(id)),
    ))
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
    batch.put_attachment(&record)?;
    Ok((record, MarkerSubjects::addressed(RecordRef::Attachment(id))))
}

pub(crate) fn create_project(batch: &mut Batch, project: NewProject) -> OpResult<ProjectId> {
    let id = ProjectId(batch.next_id(crate::storage::Sequence::Project)?);
    batch.insert_project(&ProjectRecord {
        id,
        name: project.name,
        description: project.description,
        created_at: batch.now(),
    })?;
    Ok((id, MarkerSubjects::created(RecordRef::Project(id), RecordRef::Project(id))))
}

pub(crate) fn create_llm_provider(batch: &mut Batch, provider: NewLlmProvider) -> OpResult<LlmProviderId> {
    if provider.credential_ref.as_deref().is_some_and(str::is_empty) {
        return batch.reject(Violation::InvalidValue("credential_ref", "cannot be empty"));
    }
    let id = LlmProviderId(batch.next_id(crate::storage::Sequence::LlmProvider)?);
    batch.insert_llm_provider(&LlmProviderRecord {
        id,
        name: provider.name,
        transport: provider.transport,
        endpoint: provider.endpoint,
        credential_ref: provider.credential_ref,
        created_at: batch.now(),
    })?;
    Ok((id, MarkerSubjects::created(RecordRef::LlmProvider(id), RecordRef::LlmProvider(id))))
}

pub(super) fn validate_llm_config(batch: &mut Batch, config: &LlmConfig) -> Result<(), BatchError> {
    if config.model.is_empty() {
        return batch.reject(Violation::InvalidValue("model", "cannot be empty"));
    }
    let parameters = &config.parameters;
    for (field, value) in [
        ("temperature", parameters.temperature),
        ("top_p", parameters.top_p),
        ("presence_penalty", parameters.presence_penalty),
        ("frequency_penalty", parameters.frequency_penalty),
    ] {
        if value.is_some_and(|value| !value.is_finite()) {
            return batch.reject(Violation::InvalidValue(field, "must be a finite number"));
        }
    }
    Ok(())
}

/// Validates a Profile's payload and the references that define it.
pub(super) fn profile_parts(
    batch: &mut Batch,
    config: NewProfileConfig,
) -> Result<(ProfileConfig, ProfileLinks), BatchError> {
    Ok(match config {
        NewProfileConfig::ExecutionPool(config) => (
            ProfileConfig::ExecutionPool(config),
            ProfileLinks::ExecutionPool,
        ),
        NewProfileConfig::Agent { config, llm_targets } => {
            for (index, target) in llm_targets.iter().enumerate() {
                if llm_targets[..index].contains(target) {
                    return batch.reject(Violation::DuplicateReference(RecordRef::Profile(*target)));
                }
                batch.require_profile_kind(*target, ProfileKind::Llm)?;
            }
            (ProfileConfig::Agent(config), ProfileLinks::Agent { llm_targets })
        }
        NewProfileConfig::Llm { provider, config } => {
            validate_llm_config(batch, &config)?;
            batch.require_llm_provider(provider)?;
            (ProfileConfig::Llm(config), ProfileLinks::Llm { provider })
        }
    })
}

/// Stores either kind of Profile in the one Profile table, with the
/// references that define it: an LLM Profile's Provider, or an Agent
/// Profile's LLM targets.
pub(crate) fn create_profile(batch: &mut Batch, profile: NewProfile) -> OpResult<ProfileId> {
    let (config, links) = profile_parts(batch, profile.config)?;
    let id = ProfileId(batch.next_id(crate::storage::Sequence::Profile)?);
    let now = batch.now();
    batch.insert_profile(&ProfileRow { id, name: profile.name, config, created_at: now, updated_at: now }, &links)?;
    Ok((id, MarkerSubjects::created(RecordRef::Profile(id), RecordRef::Profile(id))))
}

/// Replaces a Profile's name, payload and defining references, keeping its
/// identity and kind. Refused while any submitted execution holds the
/// Profile; the check and the edit share this transaction, so no submission,
/// finalization or stop confirmation can come between them. Submitted Jobs
/// keep the configuration they froze.
pub(crate) fn update_profile(batch: &mut Batch, id: ProfileId, profile: NewProfile) -> OpResult<()> {
    let existing = batch.require_profile_row(id)?;
    let (config, links) = profile_parts(batch, profile.config)?;
    if config.kind() != existing.config.kind() {
        return batch.reject(WrongProfileKind { profile: id, expected: existing.config.kind(), found: config.kind() }.into());
    }
    let (blocking_jobs, _) = batch.profile_holders(id, LOCK_EXAMPLES)?;
    if !blocking_jobs.is_empty() {
        return batch.reject(Violation::ProfileLocked { profile: id, blocking_jobs });
    }
    let row = ProfileRow { id, name: profile.name, config, created_at: existing.created_at, updated_at: batch.now() };
    batch.replace_profile(&row, &links)?;
    Ok(((), MarkerSubjects::addressed(RecordRef::Profile(id))))
}

/// How many holding Jobs a lock refusal names.
pub(super) const LOCK_EXAMPLES: usize = 16;

pub(crate) fn create_agent(batch: &mut Batch, agent: NewAgent) -> OpResult<AgentId> {
    for (index, profile) in agent.profiles.iter().enumerate() {
        if agent.profiles[..index].contains(profile) {
            return batch.reject(Violation::DuplicateReference(RecordRef::Profile(*profile)));
        }
        batch.require_profile_kind(*profile, ProfileKind::Agent)?;
    }
    let id = AgentId(batch.next_id(crate::storage::Sequence::Agent)?);
    batch.insert_agent(&AgentRow { id, name: agent.name, created_at: batch.now() })?;
    for profile in agent.profiles {
        batch.link_agent_profile(id, profile)?;
    }
    Ok((id, MarkerSubjects::created(RecordRef::Agent(id), RecordRef::Agent(id))))
}

/// Records that an unknown-outcome Job's execution has actually stopped,
/// which releases the Profiles it holds. Only the trusted execution path
/// that ran the Job may call this, and only once it has observed the
/// termination; a timeout, a cancel request, a disabled Executor or a
/// restart is not such an observation. The unknown outcome stays as it is.
/// The stop is local: it says nothing of an effect a tool call had outside
/// OCG, and leaves the Job's tool evidence as it is.
pub(crate) fn confirm_job_stopped(batch: &mut Batch, job: JobId) -> OpResult<()> {
    let now = batch.now();
    let mut row = batch.require_job(job)?;
    if row.unknown_at.is_none() {
        return batch.reject(Violation::StopNeedsUnknownOutcome(job));
    }
    if row.stop_confirmed_at.is_some() {
        return batch.reject(Violation::StopAlreadyConfirmed(job));
    }
    row.stop_confirmed_at = Some(now);
    row.updated_at = now;
    batch.put_job(&row)?;
    batch.release_local_execution(job)?;
    Ok(((), MarkerSubjects::addressed(RecordRef::Job(job))))
}

/// Appends measurements for a Job. Their LLM, if any, is the one the Job
/// froze; the Job itself is not changed.
///
/// A Token annotation must agree with the eligibility the Job's committed
/// annotations state, so contradictory evidence is refused here, in the
/// transaction that would write it, rather than met later by aggregation.
pub(crate) fn record_metrics(batch: &mut Batch, metrics: NewMetrics) -> OpResult<MetricsId> {
    record_metrics_checked(batch, metrics, true)
}

/// Writes Metrics without comparing their eligibility with the Job's, as a
/// database written before that check could hold them.
#[cfg(test)]
pub(crate) fn record_preexisting_metrics(
    batch: &mut Batch,
    metrics: NewMetrics,
) -> OpResult<MetricsId> {
    record_metrics_checked(batch, metrics, false)
}

pub(super) fn record_metrics_checked(
    batch: &mut Batch,
    metrics: NewMetrics,
    consistent: bool,
) -> OpResult<MetricsId> {
    batch.require_job(metrics.job)?;
    if metrics.measurements.is_empty() && metrics.token.is_none() {
        return batch.reject(Violation::InvalidValue("measurements", "records no measurement"));
    }
    if let Some(token) = metrics.token {
        if batch.job_llm_profile(metrics.job)?.is_none() {
            return batch.reject(Violation::NotAnLlmExecution(metrics.job));
        }
        if token.eligibility == RequestEligibility::Excluded
            && (token.finality != UsageFinality::Cumulative || metrics.measurements.usage != NormalizedUsage::default())
        {
            return batch.reject(Violation::InvalidValue("token", "a request that was never sent reports no usage"));
        }
        if consistent
            && let Some(committed) = batch.request_token_eligibility(metrics.job, metrics.request)?
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
    let id = MetricsId(batch.next_id(crate::storage::Sequence::Metrics)?);
    batch.insert_metrics(
        &MetricsRow { id, request: metrics.request, observed_at: metrics.observed_at, measurements: metrics.measurements, token: metrics.token, recorded_at: batch.now() },
        metrics.job,
    )?;
    Ok((id, MarkerSubjects::created(RecordRef::Job(metrics.job), RecordRef::Metrics(id))))
}

/// Appends one availability observation of an LLM request made by a Job
/// that was submitted with an LLM configuration. Neither the Job nor its
/// outcome is changed or consulted.
pub(crate) fn record_availability_stat(batch: &mut Batch, stat: NewAvailabilityStat) -> OpResult<AvailabilityStatId> {
    batch.require_job(stat.job)?;
    if batch.job_llm_profile(stat.job)?.is_none() {
        return batch.reject(Violation::NotAnLlmExecution(stat.job));
    }
    if stat.observation.is_empty() {
        return batch.reject(Violation::InvalidValue("observation", "records no response or failure"));
    }
    let id = AvailabilityStatId(batch.next_id(crate::storage::Sequence::AvailabilityStat)?);
    batch.insert_availability_stat(
        &AvailabilityStatRow { id, request: stat.request, observed_at: stat.observed_at, observation: stat.observation, recorded_at: batch.now() },
        stat.job,
    )?;
    Ok((id, MarkerSubjects::created(RecordRef::Job(stat.job), RecordRef::AvailabilityStat(id))))
}

pub(crate) fn create_execution_pool(
    batch: &mut Batch,
    new: NewExecutionPool,
) -> OpResult<ExecutionPoolId> {
    batch.require_profile_kind(new.profile, ProfileKind::ExecutionPool)?;
    let id = ExecutionPoolId(batch.next_id(crate::storage::Sequence::ExecutionPool)?);
    let now = batch.now();
    batch.insert_execution_pool(&ExecutionPool {
        id,
        name: new.name,
        profile: new.profile,
        enabled: new.enabled,
        created_at: now,
        updated_at: now,
    })?;
    Ok((
        id,
        MarkerSubjects::created(RecordRef::ExecutionPool(id), RecordRef::ExecutionPool(id)),
    ))
}

pub(crate) fn set_execution_pool_enabled(
    batch: &mut Batch,
    id: ExecutionPoolId,
    enabled: bool,
) -> OpResult<()> {
    let mut pool = batch.require_execution_pool(id)?;
    pool.enabled = enabled;
    pool.updated_at = batch.now();
    batch.put_execution_pool(&pool)?;
    Ok(((), MarkerSubjects::addressed(RecordRef::ExecutionPool(id))))
}

pub(crate) fn set_execution_pool_profile(
    batch: &mut Batch,
    id: ExecutionPoolId,
    profile: ProfileId,
) -> OpResult<()> {
    let mut pool = batch.require_execution_pool(id)?;
    pool.profile = profile;
    pool.updated_at = batch.now();
    batch.put_execution_pool(&pool)?;
    Ok(((), MarkerSubjects::addressed(RecordRef::ExecutionPool(id))))
}
