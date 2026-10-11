use std::fmt;
use std::num::NonZeroU32;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::storage::{
    Batch, BatchError, ExecutorRow, JobInputRow, JobRow, JobStepRow, MarkerSubjects,
    dependency_state,
};

/// Microseconds since the Unix epoch, as recorded by the database worker.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(i64);

impl Timestamp {
    pub fn now() -> Self {
        let micros = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(elapsed) => i64::try_from(elapsed.as_micros()).unwrap_or(i64::MAX),
            Err(before) => i64::try_from(before.duration().as_micros()).map_or(i64::MIN, |micros| -micros),
        };
        Self(micros)
    }

    pub const fn from_unix_micros(micros: i64) -> Self {
        Self(micros)
    }

    pub const fn as_unix_micros(self) -> i64 {
        self.0
    }
}

macro_rules! record_id_type {
    ($name:ident, $table:literal) => {
        /// A stable business identifier allocated by the database. A committed
        /// identifier is never reused; one allocated by a transaction that
        /// rolled back may be allocated again by a later transaction.
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(u64);

        impl $name {
            pub const fn new(value: u64) -> Self {
                Self(value)
            }

            pub const fn get(self) -> u64 {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, concat!($table, ":{}"), self.0)
            }
        }
    };
}

record_id_type!(JobId, "job");
record_id_type!(JobInputId, "job_input");
record_id_type!(JobOutputId, "job_output");
record_id_type!(JobStepId, "job_step");
record_id_type!(ExecutorId, "executor");

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RecordRef {
    Job(JobId),
    JobInput(JobInputId),
    JobOutput(JobOutputId),
    JobStep(JobStepId),
    Executor(ExecutorId),
}

impl fmt::Display for RecordRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Job(id) => id.fmt(f),
            Self::JobInput(id) => id.fmt(f),
            Self::JobOutput(id) => id.fmt(f),
            Self::JobStep(id) => id.fmt(f),
            Self::Executor(id) => id.fmt(f),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JobType {
    Task,
    System,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecutionMode {
    Once,
    Continuous,
    Periodic,
    Container,
}

/// `result` is deliberately absent: a final result output is only published
/// by [`JobFinalization`], in the same transaction that finalizes the Job.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IntermediateOutputKind {
    Progress,
    Evaluation,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutputKind {
    Progress,
    Evaluation,
    Result,
}

impl From<IntermediateOutputKind> for OutputKind {
    fn from(kind: IntermediateOutputKind) -> Self {
        match kind {
            IntermediateOutputKind::Progress => Self::Progress,
            IntermediateOutputKind::Evaluation => Self::Evaluation,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EffectKind {
    Idempotent,
    StrictFenced,
    Reconcilable,
    NonRetryable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecutorType {
    System,
    Llm,
    Agent,
    Tool,
    Database,
    Frontend,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DependencyCondition {
    Success,
    Settled,
}

/// How a dependency currently stands. An unknown prerequisite outcome is not
/// settled, so it keeps the dependency waiting; a success dependency on a
/// failed or cancelled prerequisite can never be met and is not a wait.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DependencyState {
    Waiting,
    Satisfied,
    Impossible,
}

/// Holding the step's terminal outcome as one value makes a second terminal
/// timestamp unrepresentable.
#[derive(Clone, Debug, PartialEq)]
pub enum StepOutcome {
    Completed(Timestamp),
    Failed(Timestamp),
    Cancelled(Timestamp),
    Unknown(Timestamp),
}

#[derive(Clone, Debug, PartialEq)]
pub enum StepDecision {
    Approved(Timestamp),
    Rejected(Timestamp),
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct JobInputDraft {
    pub submission_id: Option<String>,
    pub objective: Option<String>,
    pub max_depth: Option<u32>,
    pub max_children_per_job: Option<u32>,
    pub max_total_descendants: Option<u32>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct NewJob {
    pub name: Option<String>,
    pub job_type: JobType,
    pub execution_mode: ExecutionMode,
    pub eligible_at: Option<Timestamp>,
    pub scheduled_at: Option<Timestamp>,
    pub next_due_at: Option<Timestamp>,
    pub next_iteration_no: Option<i64>,
    pub iteration_no: Option<i64>,
    pub parent: Option<JobId>,
    pub dependencies: Vec<(JobId, DependencyCondition)>,
    pub assigned_executor: Option<ExecutorId>,
    pub previous: Option<JobId>,
    pub replaces: Option<JobId>,
    pub retry_of: Option<JobId>,
}

/// Execution facts reported for a JobStep. Absent values never overwrite a
/// value that is already recorded, and `truncated` can only become true.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct StepFacts {
    pub code: Option<String>,
    pub message: Option<String>,
    pub http_status: Option<i64>,
    pub exit_code: Option<i64>,
    pub stdout: Option<String>,
    pub stderr: Option<String>,
    pub truncated: bool,
    pub structured_result: Option<serde_json::Map<String, serde_json::Value>>,
    pub diagnostic_details: Option<serde_json::Map<String, serde_json::Value>>,
    pub external_task_id: Option<String>,
    pub external_session_id: Option<String>,
    pub ttfb_ms: Option<u64>,
    pub reasoning_ttft_ms: Option<u64>,
    pub output_ttft_ms: Option<u64>,
    pub request_finished_in_ms: Option<u64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct NewJobStep {
    pub operation: String,
    pub protocol_call_id: Option<String>,
    pub effect_kind: EffectKind,
    pub executed_by: Option<ExecutorId>,
    pub confirmation_requested_at: Option<Timestamp>,
    pub decision: Option<StepDecision>,
    pub started_at: Option<Timestamp>,
    pub outcome: Option<StepOutcome>,
    pub facts: StepFacts,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StepSettlement {
    Completed,
    Failed,
    Cancelled,
    Unknown,
}

#[derive(Clone, Debug, PartialEq)]
pub struct NewJobOutput {
    pub kind: IntermediateOutputKind,
    pub summary: Option<String>,
    pub code: Option<String>,
    pub goal_reached: Option<bool>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FinalResult {
    pub summary: Option<String>,
    pub code: Option<String>,
    pub goal_reached: Option<bool>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum JobFinalization {
    Completed(FinalResult),
    Failed(FinalResult),
    /// A confirmed cancellation still publishes a final result, which may be
    /// no more than a short explanation of the cancellation.
    Cancelled(FinalResult),
    /// An unknown outcome is recorded without publishing a final result output.
    Unknown,
}

#[derive(Clone, Debug, PartialEq)]
pub struct NewExecutor {
    pub name: String,
    pub executor_type: ExecutorType,
    pub concurrency_limit: NonZeroU32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendedStep {
    pub step: JobStepId,
    pub seq: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendedOutput {
    pub output: JobOutputId,
    pub seq: i64,
}

/// A stored Job with the relationships fixed when it was created or assigned.
#[derive(Clone, Debug, PartialEq)]
pub struct JobRecord {
    pub id: JobId,
    pub name: Option<String>,
    pub job_type: JobType,
    pub execution_mode: ExecutionMode,
    pub eligible_at: Option<Timestamp>,
    pub started_at: Option<Timestamp>,
    pub paused_at: Option<Timestamp>,
    pub sterilized_at: Option<Timestamp>,
    pub completed_at: Option<Timestamp>,
    pub failed_at: Option<Timestamp>,
    pub cancelled_at: Option<Timestamp>,
    pub unknown_at: Option<Timestamp>,
    pub next_due_at: Option<Timestamp>,
    pub next_iteration_no: Option<i64>,
    pub iteration_no: Option<i64>,
    pub scheduled_at: Option<Timestamp>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    pub input: JobInputId,
    pub parent: Option<JobId>,
    pub assigned_executor: Option<ExecutorId>,
}

/// A published output. Outputs have no update path, so they carry no
/// `updated_at`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobOutputRecord {
    pub id: JobOutputId,
    pub job: JobId,
    pub seq: i64,
    pub kind: OutputKind,
    pub summary: Option<String>,
    pub code: Option<String>,
    pub goal_reached: Option<bool>,
    pub created_at: Timestamp,
}

/// Identifies one submitted command. `epoch` is allocated durably each time
/// the database is opened, so an identifier is never reused across openings,
/// even when its command never committed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OperationId {
    pub(crate) epoch: u64,
    pub(crate) seq: u64,
}

impl OperationId {
    pub const fn new(epoch: u64, seq: u64) -> Self {
        Self { epoch, seq }
    }

    pub const fn epoch(self) -> u64 {
        self.epoch
    }

    pub const fn seq(self) -> u64 {
        self.seq
    }
}

impl fmt::Display for OperationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "operation:{}.{}", self.epoch, self.seq)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OperationKind {
    SaveJobInput,
    CreateJobWithInput,
    AssignJobExecutor,
    AppendJobStep,
    StartJobStep,
    SettleJobStep,
    AppendJobOutput,
    FinalizeJob,
    CreateExecutor,
    BeginCoreLogCleanup,
}

/// Written in the same transaction as every accepted mutation, so its
/// presence proves the mutation committed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OperationMarker {
    pub operation: OperationId,
    pub kind: OperationKind,
    /// The record the command addressed.
    pub subject: RecordRef,
    /// The record the command created, when it created one.
    pub created: Option<RecordRef>,
    pub committed_at: Timestamp,
}

/// A business rule refused the command; nothing it did was committed.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Violation {
    #[error("{0} must be the __CORE__ Executor with type System")]
    NotCoreExecutor(ExecutorId),
    #[error("{0} not found")]
    NotFound(RecordRef),
    #[error("{0} already exists")]
    DuplicateId(RecordRef),
    #[error("{0} is submitted and therefore immutable")]
    InputAlreadySubmitted(JobInputId),
    #[error("{0} is already finalized")]
    JobAlreadyFinalized(JobId),
    #[error("{0} has a terminal outcome and accepts no more steps or outputs")]
    JobHasTerminalOutcome(JobId),
    #[error("the parent {0} no longer accepts children")]
    ParentClosed(JobId),
    #[error("ASSIGNED_TO cannot change after {0} starts")]
    ExecutorChangeAfterStart(JobId),
    #[error("{0} has already started")]
    StepAlreadyStarted(JobStepId),
    #[error("{0} is already settled")]
    StepAlreadySettled(JobStepId),
    #[error("cannot finalize {0}: a JobStep is unfinished or has an unknown outcome")]
    UnfinishedStep(JobId),
    #[error("cannot finalize {0}: a child Job is unfinished or has an unknown outcome")]
    UnfinishedChild(JobId),
    #[error("the dependency on {0} is listed more than once")]
    DuplicateDependency(JobId),
    #[error("the relationship would create a cycle in the waiting graph through {0:?}")]
    WaitingCycle(Vec<JobId>),
    #[error("waiting-graph validation reached its traversal limit ({max_layers} layers or {max_jobs} Jobs); the relationship was not created")]
    WaitingGraphLimit { max_layers: usize, max_jobs: usize },
    #[error("{0} exceeds the storable range")]
    ValueOutOfRange(&'static str),
}

pub(crate) type OpResult<T> = Result<(T, MarkerSubjects), BatchError>;

pub(crate) fn begin_core_log_cleanup(
    batch: &mut Batch,
    owner: JobId,
    executor: ExecutorId,
    facts: StepFacts,
) -> OpResult<crate::log_cleanup::CleanupRecords> {
    batch.require_core_executor(executor)?;
    batch.require_job(owner)?;
    let (input, _) = save_job_input(batch, None, JobInputDraft {
        objective: Some("Remove expired OCG runtime logs".into()),
        ..JobInputDraft::default()
    })?;
    let (job, _) = create_job_with_input(batch, NewJob {
        name: Some("__CORE__ log cleanup".into()),
        job_type: JobType::System,
        execution_mode: ExecutionMode::Once,
        parent: Some(owner),
        assigned_executor: Some(executor),
        dependencies: Vec::new(),
        eligible_at: None,
        scheduled_at: None,
        next_due_at: None,
        next_iteration_no: None,
        iteration_no: None,
        previous: None,
        replaces: None,
        retry_of: None,
    }, input)?;
    let mut row = batch.require_job(job)?;
    row.started_at = Some(batch.now());
    batch.put_job(&row)?;
    let (step, _) = append_job_step(batch, job, NewJobStep {
        operation: "__CORE__.cleanup_logs".into(),
        protocol_call_id: None,
        effect_kind: EffectKind::NonRetryable,
        executed_by: Some(executor),
        confirmation_requested_at: None,
        decision: None,
        started_at: Some(batch.now()),
        outcome: None,
        facts,
    })?;
    Ok((crate::log_cleanup::CleanupRecords { job, step: step.step },
        MarkerSubjects::created(RecordRef::Job(owner), RecordRef::Job(job))))
}

fn milliseconds(batch: &mut Batch, field: &'static str, value: Option<u64>) -> Result<Option<i64>, BatchError> {
    match value.map(i64::try_from).transpose() {
        Ok(value) => Ok(value),
        Err(_) => batch.reject(Violation::ValueOutOfRange(field)),
    }
}

/// Without `existing`, creates a new unsubmitted JobInput. With `existing`,
/// replaces its editable fields, which is refused once the input is submitted.
pub(crate) fn save_job_input(
    batch: &mut Batch,
    existing: Option<JobInputId>,
    draft: JobInputDraft,
) -> OpResult<JobInputId> {
    let now = batch.now();
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
            };
            batch.insert_input(&row)?;
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
    Ok((row.id, MarkerSubjects::addressed(RecordRef::JobInput(row.id))))
}

/// Creates the Job with every relationship fixed at submission and submits
/// the input, after validating the combined waiting graph with all proposed
/// waiting edges included.
pub(crate) fn create_job_with_input(batch: &mut Batch, job: NewJob, input: JobInputId) -> OpResult<JobId> {
    let now = batch.now();
    let mut input_row = batch.require_input(input)?;
    if input_row.submitted_at.is_some() || batch.input_job(input)?.is_some() {
        return batch.reject(Violation::InputAlreadySubmitted(input));
    }
    if let Some(parent) = job.parent {
        let parent_row = batch.require_job(parent)?;
        if parent_row.any_terminal() || parent_row.sterilized_at.is_some() {
            return batch.reject(Violation::ParentClosed(parent));
        }
    }
    let mut dependency_rows = Vec::with_capacity(job.dependencies.len());
    for (index, (dependency, condition)) in job.dependencies.iter().enumerate() {
        if job.dependencies[..index].iter().any(|(earlier, _)| earlier == dependency) {
            return batch.reject(Violation::DuplicateDependency(*dependency));
        }
        dependency_rows.push((batch.require_job(*dependency)?, *condition));
    }
    if let Some(executor) = job.assigned_executor {
        batch.require_executor(executor)?;
    }
    for lineage in [job.previous, job.replaces, job.retry_of].into_iter().flatten() {
        batch.require_job(lineage)?;
    }

    let id = JobId(batch.next_id(crate::storage::Sequence::Job)?);
    let mut proposed = Vec::new();
    if let Some(parent) = job.parent {
        proposed.push((parent, id));
    }
    for (row, condition) in &dependency_rows {
        if dependency_state(*condition, row) == DependencyState::Waiting {
            proposed.push((id, row.id));
        }
    }
    batch.check_waiting(&proposed)?;

    batch.insert_job(&JobRow {
        id,
        name: job.name,
        job_type: job.job_type,
        execution_mode: job.execution_mode,
        eligible_at: job.eligible_at,
        started_at: None,
        paused_at: None,
        sterilized_at: None,
        completed_at: None,
        failed_at: None,
        cancelled_at: None,
        unknown_at: None,
        next_due_at: job.next_due_at,
        next_iteration_no: job.next_iteration_no,
        iteration_no: job.iteration_no,
        scheduled_at: job.scheduled_at,
        created_at: now,
        updated_at: now,
    })?;
    batch.link_input(id, input)?;
    if let Some(parent) = job.parent {
        batch.link_child(id, parent)?;
    }
    for (dependency, condition) in &job.dependencies {
        batch.link_dependency(id, *dependency, *condition)?;
    }
    if let Some(executor) = job.assigned_executor {
        batch.set_assigned_executor(id, executor)?;
    }
    batch.link_lineage(id, job.previous, job.replaces, job.retry_of)?;

    input_row.submitted_at = Some(now);
    input_row.updated_at = now;
    batch.put_input(&input_row)?;
    Ok((id, MarkerSubjects::created(RecordRef::JobInput(input), RecordRef::Job(id))))
}

/// Replaces the Job's main Executor; refused once the Job has started.
pub(crate) fn assign_job_executor(batch: &mut Batch, job: JobId, executor: ExecutorId) -> OpResult<()> {
    let row = batch.require_job(job)?;
    batch.require_executor(executor)?;
    if row.started_at.is_some() || row.any_terminal() {
        return batch.reject(Violation::ExecutorChangeAfterStart(job));
    }
    batch.set_assigned_executor(job, executor)?;
    Ok(((), MarkerSubjects::addressed(RecordRef::Job(job))))
}

/// Steps are refused once the Job has any terminal outcome, unknown included.
pub(crate) fn append_job_step(batch: &mut Batch, job: JobId, step: NewJobStep) -> OpResult<AppendedStep> {
    let now = batch.now();
    let job_row = batch.require_job(job)?;
    if job_row.any_terminal() {
        return batch.reject(Violation::JobHasTerminalOutcome(job));
    }
    if let Some(executor) = step.executed_by {
        batch.require_executor(executor)?;
    }
    let (approved_at, rejected_at) = match step.decision {
        None => (None, None),
        Some(StepDecision::Approved(at)) => (Some(at), None),
        Some(StepDecision::Rejected(at)) => (None, Some(at)),
    };
    let (completed_at, failed_at, cancelled_at, unknown_at) = match step.outcome {
        None => (None, None, None, None),
        Some(StepOutcome::Completed(at)) => (Some(at), None, None, None),
        Some(StepOutcome::Failed(at)) => (None, Some(at), None, None),
        Some(StepOutcome::Cancelled(at)) => (None, None, Some(at), None),
        Some(StepOutcome::Unknown(at)) => (None, None, None, Some(at)),
    };
    let mut row = JobStepRow {
        id: JobStepId(0),
        job,
        seq: 0,
        operation: step.operation,
        protocol_call_id: step.protocol_call_id,
        effect_kind: step.effect_kind,
        confirmation_requested_at: step.confirmation_requested_at,
        approved_at,
        rejected_at,
        started_at: step.started_at,
        completed_at,
        failed_at,
        cancelled_at,
        unknown_at,
        code: None,
        message: None,
        http_status: None,
        exit_code: None,
        stdout: None,
        stderr: None,
        truncated: false,
        structured_result: None,
        diagnostic_details: None,
        external_task_id: None,
        external_session_id: None,
        ttfb_ms: None,
        reasoning_ttft_ms: None,
        output_ttft_ms: None,
        request_finished_in_ms: None,
        created_at: now,
        updated_at: now,
    };
    merge_step_facts(batch, &mut row, step.facts)?;
    row.seq = batch.last_step_seq(job)? + 1;
    row.id = JobStepId(batch.next_id(crate::storage::Sequence::JobStep)?);
    batch.insert_step(&row)?;
    if let Some(executor) = step.executed_by {
        batch.set_executed_by(row.id, executor)?;
    }
    let appended = AppendedStep { step: row.id, seq: seq_i64(batch, row.seq)? };
    Ok((appended, MarkerSubjects::created(RecordRef::Job(job), RecordRef::JobStep(row.id))))
}

/// Marks an unstarted step as started, first establishing or replacing its
/// executing Executor when one is given.
pub(crate) fn start_job_step(batch: &mut Batch, step: JobStepId, executed_by: Option<ExecutorId>) -> OpResult<()> {
    let now = batch.now();
    let mut row = batch.require_step(step)?;
    if row.started_at.is_some() {
        return batch.reject(Violation::StepAlreadyStarted(step));
    }
    if row.any_terminal() {
        return batch.reject(Violation::StepAlreadySettled(step));
    }
    if let Some(executor) = executed_by {
        batch.require_executor(executor)?;
        batch.set_executed_by(step, executor)?;
    }
    row.started_at = Some(now);
    row.updated_at = now;
    batch.put_step(&row)?;
    Ok(((), MarkerSubjects::addressed(RecordRef::JobStep(step))))
}

/// Records the step's single terminal outcome together with its facts.
/// Resolving an unknown outcome is not supported by this foundation.
pub(crate) fn settle_job_step(
    batch: &mut Batch,
    step: JobStepId,
    settlement: StepSettlement,
    facts: StepFacts,
) -> OpResult<()> {
    let now = batch.now();
    let mut row = batch.require_step(step)?;
    if row.any_terminal() {
        return batch.reject(Violation::StepAlreadySettled(step));
    }
    match settlement {
        StepSettlement::Completed => row.completed_at = Some(now),
        StepSettlement::Failed => row.failed_at = Some(now),
        StepSettlement::Cancelled => row.cancelled_at = Some(now),
        StepSettlement::Unknown => row.unknown_at = Some(now),
    }
    merge_step_facts(batch, &mut row, facts)?;
    row.updated_at = now;
    batch.put_step(&row)?;
    Ok(((), MarkerSubjects::addressed(RecordRef::JobStep(step))))
}

fn merge_step_facts(batch: &mut Batch, row: &mut JobStepRow, facts: StepFacts) -> Result<(), BatchError> {
    fn keep<T>(target: &mut Option<T>, value: Option<T>) {
        if value.is_some() {
            *target = value;
        }
    }
    let ttfb_ms = milliseconds(batch, "ttfb_ms", facts.ttfb_ms)?;
    let reasoning_ttft_ms = milliseconds(batch, "reasoning_ttft_ms", facts.reasoning_ttft_ms)?;
    let output_ttft_ms = milliseconds(batch, "output_ttft_ms", facts.output_ttft_ms)?;
    let request_finished_in_ms = milliseconds(batch, "request_finished_in_ms", facts.request_finished_in_ms)?;
    keep(&mut row.code, facts.code);
    keep(&mut row.message, facts.message);
    keep(&mut row.http_status, facts.http_status);
    keep(&mut row.exit_code, facts.exit_code);
    keep(&mut row.stdout, facts.stdout);
    keep(&mut row.stderr, facts.stderr);
    row.truncated |= facts.truncated;
    keep(&mut row.structured_result, facts.structured_result);
    keep(&mut row.diagnostic_details, facts.diagnostic_details);
    keep(&mut row.external_task_id, facts.external_task_id);
    keep(&mut row.external_session_id, facts.external_session_id);
    keep(&mut row.ttfb_ms, ttfb_ms);
    keep(&mut row.reasoning_ttft_ms, reasoning_ttft_ms);
    keep(&mut row.output_ttft_ms, output_ttft_ms);
    keep(&mut row.request_finished_in_ms, request_finished_in_ms);
    Ok(())
}

fn seq_i64(batch: &mut Batch, seq: u64) -> Result<i64, BatchError> {
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
    let appended = publish_output(batch, job, output.kind.into(), output.summary, output.code, output.goal_reached)?;
    let output_id = appended.output;
    Ok((appended, MarkerSubjects::created(RecordRef::Job(job), RecordRef::JobOutput(output_id))))
}

/// Shared by intermediate appends and finalization so that both publish an
/// output, its ownership edge, and the LATEST_OUTPUT pointer identically.
fn publish_output(
    batch: &mut Batch,
    job: JobId,
    kind: OutputKind,
    summary: Option<String>,
    code: Option<String>,
    goal_reached: Option<bool>,
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
        created_at: batch.now(),
    };
    batch.insert_output(&record)?;
    Ok(AppendedOutput { output: id, seq: record.seq })
}

/// A confirmed outcome requires every own step and child Job to have a
/// confirmed outcome; the Job is then sterilized, its final result published,
/// and the terminal timestamp set in one transaction.
///
/// An unknown outcome publishes no result and claims nothing about steps or
/// children; it stops further child creation.
pub(crate) fn finalize_job(batch: &mut Batch, job: JobId, finalization: JobFinalization) -> OpResult<Option<AppendedOutput>> {
    let now = batch.now();
    let mut row = batch.require_job(job)?;
    if row.any_terminal() {
        return batch.reject(Violation::JobAlreadyFinalized(job));
    }
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
    for step in batch.job_steps(job)? {
        if !step.confirmed_terminal() {
            return batch.reject(Violation::UnfinishedStep(job));
        }
    }
    for child in batch.children(job)? {
        if !batch.require_job(child)?.confirmed_terminal() {
            return batch.reject(Violation::UnfinishedChild(job));
        }
    }
    row.sterilized_at.get_or_insert(now);
    let appended = publish_output(batch, job, OutputKind::Result, result.summary, result.code, result.goal_reached)?;
    *outcome(&mut row) = Some(now);
    row.updated_at = now;
    batch.put_job(&row)?;
    let output_id = appended.output;
    Ok((Some(appended), MarkerSubjects::created(RecordRef::Job(job), RecordRef::JobOutput(output_id))))
}

pub(crate) fn create_executor(batch: &mut Batch, executor: NewExecutor) -> OpResult<ExecutorId> {
    let now = batch.now();
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
    })?;
    Ok((id, MarkerSubjects::created(RecordRef::Executor(id), RecordRef::Executor(id))))
}
