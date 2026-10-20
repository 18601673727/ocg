use super::*;

/// `result` is deliberately absent: a final result output is only published
/// by [`JobFinalization`], in the same transaction that finalizes the Job.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IntermediateOutputKind {
    Progress,
    Evaluation,
    Reasoning,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutputKind {
    Progress,
    Evaluation,
    Reasoning,
    Result,
}

impl From<IntermediateOutputKind> for OutputKind {
    fn from(kind: IntermediateOutputKind) -> Self {
        match kind {
            IntermediateOutputKind::Progress => Self::Progress,
            IntermediateOutputKind::Evaluation => Self::Evaluation,
            IntermediateOutputKind::Reasoning => Self::Reasoning,
        }
    }
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

/// What a Job is asked to do: its objective, the Attachments it carries and
/// its limits on descendants. It becomes part of the Job's spec.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct JobInputDraft {
    pub objective: Option<String>,
    /// The Attachments the input carries, in order. Each must exist and must
    /// not be cleaned up; see [`AttachmentRecord`].
    pub attachments: Vec<AttachmentId>,
}

/// Every relationship here is fixed once the Job is submitted. The Project
/// is independent of `parent`: a child never inherits its parent's Project.
#[derive(Clone, Debug, PartialEq)]
pub struct NewJob {
    pub name: Option<String>,
    pub eligible_at: Option<Timestamp>,
    /// Top-level work names the Genesis Job (0), see
    /// [`crate::database::DatabaseHandle::genesis`].
    pub parent: JobId,
    pub project: Option<ProjectId>,
    /// The CLI grant stored with a Process plan. Stored turns use the frozen Agent.
    pub process_grant: Option<String>,
    pub input: JobInputDraft,
    /// Present for an LLM execution child; absent for orchestrating or
    /// system work.
    pub configuration: Option<ExecutionConfiguration>,
    /// The conversation tip this directly constructed turn follows.
    pub previous: Option<JobId>,
}

/// The configuration an LLM execution child Job is submitted with. Both
/// references are fixed at submission. Work that runs an LLM directly names
/// no Agent; when one is named, `llm_profile` must be one of its Models. The
/// values the Job executes with are frozen into its spec at submission.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExecutionConfiguration {
    pub agent_profile: Option<ProfileId>,
    pub llm_profile: ProfileId,
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

/// An Executor is a Pool config. It offers `LlmRequest` for each of the LLM
/// transports its adapter implements and nothing else, so one without
/// transports is never granted LLM work.
#[derive(Clone, Debug, PartialEq)]
pub struct NewExecutor {
    pub name: String,
    pub concurrency_limit: NonZeroU32,
    /// The LLM transports this Executor's adapter implements, fixed at
    /// registration.
    pub llm_transports: BTreeSet<LlmTransport>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendedOutput {
    pub output: JobOutputId,
    pub seq: i64,
}
