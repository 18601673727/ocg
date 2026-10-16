use super::*;

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
    AppendJobOutput,
    FinalizeJob,
    CreateExecutor,
    BeginCoreLogCleanup,
    StartJob,
    RetryJob,
    AssociateJobProject,
    CreateProject,
    CreateLlmProvider,
    CreateProfile,
    UpdateProfile,
    CreateAgent,
    ConfirmJobStopped,
    RecordMetrics,
    RecordAvailabilityStat,
    /// One bounded batch of AvailabilityStat aggregation. Its marker's
    /// subject is the last record the batch consumed.
    ConsumeAvailability,
    /// One bounded batch of Token aggregation. Its marker's subject is the
    /// last Metrics record the batch consumed.
    ConsumeTokenUsage,
    /// A start by this process's own runtime, which claims the Job's
    /// execution for the current opening of the database.
    StartLocalJob,
    /// The settlement, when the database is opened, of a Job that an
    /// earlier opening started locally and never settled.
    ReconcileAbandonedJob,
    /// OCG's refusal or admission of a tool call a Provider requested.
    RecordToolDecision,
    /// What is known of an admitted tool call's execution.
    RecordToolOutcome,
    /// A verified conclusion about an uncertain tool execution.
    ReconcileToolEffect,
    /// A new Attachment, recorded once its content was published.
    CreateAttachment,
    /// The cleanup of an available Attachment: its kind and time, together.
    CleanupAttachment,
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
    /// The wall-clock time at which the command's write transaction began,
    /// not when its commit completed; the same value as the timestamps the
    /// command wrote. Wall-clock time is not monotonic.
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
    #[error("{0} has a terminal outcome and accepts no more outputs")]
    JobHasTerminalOutcome(JobId),
    #[error("{0} has already started")]
    JobAlreadyStarted(JobId),
    #[error("the parent {0} no longer accepts children")]
    ParentClosed(JobId),
    #[error("ASSIGNED_TO cannot change after {0} starts")]
    ExecutorChangeAfterStart(JobId),
    #[error("cannot finalize {0}: a child Job is not settled")]
    UnfinishedChild(JobId),
    #[error(
        "{job} cannot start or complete: its dependency on {prerequisite} is {state:?}, not satisfied"
    )]
    UnsatisfiedDependency {
        job: JobId,
        prerequisite: JobId,
        state: DependencyState,
    },
    #[error("{0} is not settled, so it cannot be retried yet")]
    RetryOfUnsettledJob(JobId),
    #[error("{original} already has the unsettled direct retry {retry}")]
    ActiveRetry { original: JobId, retry: JobId },
    #[error("the dependency on {0} is listed more than once")]
    DuplicateDependency(JobId),
    #[error("the relationship would create a cycle in the waiting graph through {0:?}")]
    WaitingCycle(Vec<JobId>),
    #[error("waiting-graph validation reached its traversal limit ({max_layers} layers or {max_jobs} Jobs); the relationship was not created")]
    WaitingGraphLimit { max_layers: usize, max_jobs: usize },
    #[error("{0} exceeds the storable range")]
    ValueOutOfRange(&'static str),
    #[error("{0} is invalid: {1}")]
    InvalidValue(&'static str, &'static str),
    #[error("{0} is the Genesis Job, which ordinary work cannot finalize, cancel or retry")]
    GenesisProtected(JobId),
    #[error("only the Genesis Job is its own parent; {0} cannot be")]
    SelfParent(JobId),
    #[error("the parent of {0} was fixed when it was submitted")]
    ParentFixed(JobId),
    #[error("making {parent} the parent of {child} would create a parent cycle")]
    ParentCycle { child: JobId, parent: JobId },
    #[error("{job} is already associated with {project}")]
    AlreadyInProject { job: JobId, project: ProjectId },
    #[error("{0} is listed more than once")]
    DuplicateReference(RecordRef),
    #[error(transparent)]
    WrongProfileKind(#[from] WrongProfileKind),
    #[error("{llm_profile} is not a configured LLM target of {agent_profile}")]
    NotAConfiguredTarget { agent_profile: ProfileId, llm_profile: ProfileId },
    #[error("the configuration of {0} was fixed when it was submitted")]
    ConfigurationFixed(JobId),
    #[error("{executor} does not support the {transport:?} transport this work's LLM configuration uses")]
    ExecutorLacksTransport { executor: ExecutorId, transport: LlmTransport },
    #[error("{profile} is used by unfinished work, such as {blocking_jobs:?}, and cannot be edited")]
    ProfileLocked { profile: ProfileId, blocking_jobs: Vec<JobId> },
    #[error("the configuration frozen in {0} differs from the retry's; submit the retry with a new input")]
    ConfigurationMismatch(JobInputId),
    #[error("{executor} is a {found:?} Executor; running an LLM configuration needs an Llm Executor")]
    IncompatibleExecutorKind { executor: ExecutorId, found: ExecutorType },
    #[error("{0} has no unknown outcome, so it has no unresolved execution to confirm stopped")]
    StopNeedsUnknownOutcome(JobId),
    #[error("the execution of {0} was already confirmed stopped")]
    StopAlreadyConfirmed(JobId),
    #[error("{0} was submitted without an LLM configuration")]
    NotAnLlmExecution(JobId),
    /// Every Token annotation of one Provider request states the same
    /// eligibility: the request was either issued or not. The committed
    /// records keep theirs. Two requests of one Job may differ.
    #[error(
        "the Metrics of {request} of {job} already state that it is {committed:?}; an annotation stating {refused:?} contradicts them"
    )]
    ContradictoryTokenEligibility {
        job: JobId,
        request: RequestNumber,
        committed: RequestEligibility,
        refused: RequestEligibility,
    },
    #[error("{0} is not being executed by this process, so it records no tool evidence")]
    NotExecutingLocally(JobId),
    #[error("{request} of {job} has no recorded Provider observation")]
    RequestNotObserved { job: JobId, request: RequestNumber },
    #[error("{0} already has evidence")]
    DuplicateToolCall(ToolCallRef),
    #[error("the Provider id of {call} is already used by {existing}")]
    DuplicateToolCallId {
        call: ToolCallRef,
        existing: ToolCallRef,
    },
    #[error("{0} has no evidence")]
    ToolCallNotFound(ToolCallRef),
    #[error("{0} was not admitted")]
    ToolCallNotAdmitted(ToolCallRef),
    #[error("{0} already has a recorded outcome")]
    ToolOutcomeAlreadyRecorded(ToolCallRef),
    #[error("{0} already has a definite outcome")]
    ToolEffectAlreadyResolved(ToolCallRef),
    #[error("{0} was already reconciled")]
    ToolEffectAlreadyReconciled(ToolCallRef),
    #[error("{0} is not settled, so its tool executions cannot be reconciled yet")]
    ToolEffectStillExecuting(JobId),
    #[error("{call} exceeds the stored tool evidence limit on {limit}")]
    ToolEvidenceLimit {
        call: ToolCallRef,
        limit: &'static str,
    },
    /// An admitted call's effect is neither known nor reconciled.
    #[error("whether {0} had its effect is unresolved")]
    UnresolvedToolEffect(ToolCallRef),
    /// A retry could repeat an effect that already happened, and the retry
    /// did not acknowledge that.
    #[error(
        "{0} already had an effect that a retry could repeat, and the retry did not acknowledge it"
    )]
    RepeatedEffectNotAcknowledged(ToolCallRef),
    /// A new input may not carry an Attachment whose content is gone.
    #[error("{0} was cleaned up, so its content can no longer be used")]
    AttachmentCleanedUp(AttachmentId),
    /// A submission key names one Job forever; `same_request` says whether
    /// the refused creation was for the request that created it.
    #[error("the submission key already created {job}")]
    SubmissionKeyUsed { job: JobId, same_request: bool },
}
