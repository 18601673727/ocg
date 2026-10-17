use super::*;

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

/// How a dependency currently stands. A prerequisite is settled once it has
/// a confirmed outcome, or an unknown outcome whose execution is confirmed
/// stopped; until then the dependency waits. A completed prerequisite
/// satisfies either condition. Any other settled prerequisite satisfies a
/// settled dependency, while a success dependency on it can never be met and
/// is not a wait.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DependencyState {
    Waiting,
    Satisfied,
    Impossible,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct JobInputDraft {
    pub submission_id: Option<String>,
    pub objective: Option<String>,
    pub max_depth: Option<u32>,
    pub max_children_per_job: Option<u32>,
    pub max_total_descendants: Option<u32>,
    /// The Attachments the input carries, in order, each named once. Each
    /// must exist and must not be cleaned up; see [`AttachmentRecord`].
    pub attachments: Vec<AttachmentId>,
}

/// Every relationship here is fixed once the Job is submitted. Project
/// associations are independent of `parent`: a child never inherits its
/// parent's Projects.
#[derive(Clone, Debug, PartialEq)]
pub struct NewJob {
    pub name: Option<String>,
    pub execution_mode: ExecutionMode,
    pub eligible_at: Option<Timestamp>,
    pub scheduled_at: Option<Timestamp>,
    pub next_due_at: Option<Timestamp>,
    pub next_iteration_no: Option<i64>,
    pub iteration_no: Option<i64>,
    /// Top-level work names the Genesis Job, see [`crate::database::DatabaseHandle::genesis`].
    pub parent: JobId,
    pub projects: Vec<ProjectId>,
    pub dependencies: Vec<(JobId, DependencyCondition)>,
    /// Present for an LLM execution child; absent for orchestrating or
    /// system work.
    pub configuration: Option<ExecutionConfiguration>,
    /// When `configuration` is given, must be an Llm Executor supporting the
    /// transport of the LLM configuration frozen for it.
    pub assigned_executor: Option<ExecutorId>,
    pub previous: Option<JobId>,
    pub replaces: Option<JobId>,
}

/// The configuration an LLM execution child Job is submitted with. Both
/// references are fixed at submission. Work that runs an LLM directly, such
/// as a fixed capability check, names no Agent Profile; when one is named,
/// `llm_profile` must be one of its configured LLM targets. The values the
/// Job executes with are frozen into its input at submission.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExecutionConfiguration {
    pub agent_profile: Option<ProfileId>,
    pub llm_profile: ProfileId,
}

/// Which input an explicit retry runs with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetryInput {
    /// The original Job's frozen input, shared rather than copied.
    Unchanged,
    /// A different, not yet submitted input; it is submitted by the retry.
    Changed(JobInputId),
}

/// An explicit retry creates a new Job. Its parent and its RETRY_OF edge are
/// derived from `original`, which is left exactly as it was.
#[derive(Clone, Debug, PartialEq)]
pub struct JobRetry {
    pub original: JobId,
    pub input: RetryInput,
    pub name: Option<String>,
    pub execution_mode: ExecutionMode,
    pub eligible_at: Option<Timestamp>,
    pub scheduled_at: Option<Timestamp>,
    pub next_due_at: Option<Timestamp>,
    pub next_iteration_no: Option<i64>,
    pub iteration_no: Option<i64>,
    pub projects: Vec<ProjectId>,
    pub dependencies: Vec<(JobId, DependencyCondition)>,
    /// Stated explicitly for the retry; never taken from the original, so a
    /// retry cannot silently switch model or Provider.
    pub configuration: Option<ExecutionConfiguration>,
    pub assigned_executor: Option<ExecutorId>,
    /// Accepts that the retry may repeat a non-idempotent tool effect the
    /// original, or any Job it retries in turn, is known to have had. It
    /// covers this retry only. It promises no deduplication, grants
    /// no tool anything, and never admits a retry over an effect that is
    /// still unresolved. Leave it `false` unless the user accepted that.
    pub acknowledge_repeated_effects: bool,
}

/// `value` carries a machine-readable payload, such as the conclusion of a
/// capability check. It is published as given and never changes the Job's
/// outcome.
#[derive(Clone, Debug, PartialEq)]
pub struct NewJobOutput {
    pub kind: IntermediateOutputKind,
    pub summary: Option<String>,
    pub code: Option<String>,
    pub goal_reached: Option<bool>,
    pub value: Option<serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FinalResult {
    pub summary: Option<String>,
    pub code: Option<String>,
    pub goal_reached: Option<bool>,
    pub value: Option<serde_json::Value>,
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

/// A Job's terminal outcome without its published result.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JobOutcome {
    Completed,
    Failed,
    Cancelled,
    Unknown,
}

impl JobOutcome {
    /// An unknown outcome publishes no result, so `result` is dropped for it.
    pub fn finalization(self, result: FinalResult) -> JobFinalization {
        match self {
            Self::Completed => JobFinalization::Completed(result),
            Self::Failed => JobFinalization::Failed(result),
            Self::Cancelled => JobFinalization::Cancelled(result),
            Self::Unknown => JobFinalization::Unknown,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct NewExecutor {
    pub name: String,
    pub executor_type: ExecutorType,
    pub concurrency_limit: NonZeroU32,
    /// The LLM transports this Executor's adapter implements, fixed at
    /// registration. Only an `ExecutorType::Llm` Executor may name any.
    pub llm_transports: BTreeSet<LlmTransport>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendedOutput {
    pub output: JobOutputId,
    pub seq: i64,
}
