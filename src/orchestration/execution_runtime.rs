//! Bounded scheduler-to-executor handoff for the 0.5 execution plane.
//!
//! The scheduler owns admission to the executor queue. The executor owns the
//! execution plan after dequeueing it and is the only worker that calls the
//! database handle. Every worker boundary is a bounded `flume` channel; the
//! database handle itself has a separate bounded command channel.

use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};
use std::time::Duration;

use ntex::util::{Either, select};

use crate::attachments::AttachmentError;
use crate::database::{CommandOutcome, DatabaseError, DatabaseHandle};
use crate::openai_compatible::image::{
    MAX_IMAGE_BYTES, MAX_REQUEST_IMAGE_BYTES, MAX_REQUEST_IMAGES,
};
use crate::openai_compatible::{
    ChatConversation, ChatMessage, ChatToolCallWire, CompletedToolCall, ImageError, ImageInput,
    ImageMime, InvalidToolCall, ToolCallRefusal, ToolWire,
};
use crate::orchestration::tools::{
    FailureCertainty, ToolFailure, ToolFuture, ToolOffer, ToolRegistryError,
};
use crate::persistence::{
    AppendedOutput, AttachmentId, AuthorizationBasis, ExecutorId, FinalResult,
    IntermediateOutputKind, JobFinalization, JobId, JobInputDraft, JobInputId, JobOutcome,
    MAX_STORED_TOOL_ARGUMENTS_BYTES, MAX_STORED_TOOL_ARGUMENTS_PER_JOB_BYTES, Measurements,
    NewAvailabilityStat, NewJob, NewJobOutput, NewMetrics, NewSubmission, NewToolEvidence,
    NotInvokedReason, RequestNumber, Timestamp, ToolAdmission, ToolCallRef, ToolDecision,
    ToolOutcome, ToolOutcomeKind, ToolPayload, UncertainCause, Violation, WithheldReason,
};
use crate::provider_execution::{
    self, CancellationToken, CredentialResolver, EnvironmentCredentialResolver, OutputSinkError,
    ProviderExecutionConfig, ProviderExecutionError, ProviderExecutionResult, ProviderFact,
    ProviderObservation, ProviderOutputSink,
};
use crate::token_usage::{TokenAnnotation, UsageFinality};

pub const SCHEDULER_QUEUE_CAPACITY: usize = 64;
pub const EXECUTOR_QUEUE_CAPACITY: usize = 64;

/// The durable work supplied by the scheduler to one registered executor.
///
/// Executor registration is a process or installation concern and must happen
/// through [`DatabaseHandle::create_executor`] before a plan is submitted. The
/// executor allocates only the Job lifecycle here: it creates the Job, starts
/// it as its own local execution in one transaction, and finalizes it with
/// its published result. Provider-specific execution can be inserted between
/// the start and `finalize_job` without changing the worker ownership model.
#[derive(Debug)]
pub struct ExecutionPlan {
    /// An existing durable Executor record. This is never registered as part
    /// of executing a plan, so repeated plans do not grow the executor table.
    /// When `job.configuration` names an LLM Profile, the Job is refused
    /// unless this is an Llm Executor supporting the transport of the
    /// configuration frozen for it; see [`DatabaseHandle::compatible_executors`].
    /// Choosing among compatible Executors is the caller's decision, and the
    /// choice never changes the frozen Provider, model or parameters.
    pub executor_id: ExecutorId,
    pub input: JobInputDraft,
    pub job: NewJob,
    pub outcome: JobOutcome,
    pub result: FinalResult,
}

/// A provider-backed execution request. Its LLM configuration is frozen when
/// the Job is admitted; execution reads that snapshot after the durable start.
///
/// The Attachments of `input` are sent as images with the objective, in
/// order, in its user message after its text; see
/// [`provider_execution::objective_conversation_with_images`]. Each is read
/// back from the database and the attachment store after the Job started,
/// and verified, see [`attachment_images`]; nothing of an image is held by
/// the plan. When any cannot be sent, the Job fails before any request.
pub struct ProviderExecutionPlan {
    pub executor_id: ExecutorId,
    pub input: JobInputDraft,
    pub job: NewJob,
    pub credential_resolver: Arc<dyn CredentialResolver>,
    pub provider_config: ProviderExecutionConfig,
    /// The OCG tools offered to the Provider, if any. `None`, the default,
    /// offers none, and the execution is the single request it always was.
    pub tools: Option<ToolExecution>,
}

/// Tools OCG offers one execution, and the limits of the tool loop. Only
/// OCG's own code can build one, from tools it registered and authorized;
/// a plan built outside the crate can only offer no tools.
#[derive(Clone, Debug)]
pub struct ToolExecution {
    pub(crate) offer: ToolOffer,
    pub(crate) limits: ToolLoopLimits,
}

/// Bounds of one execution's tool loop. Each is a refusal, never a
/// truncation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ToolLoopLimits {
    /// Provider requests one Job may send, its first included. Tools a
    /// response requests when no further request is allowed are not run.
    pub max_provider_requests: u32,
    /// Tool calls one Job may run. A response whose calls would exceed it
    /// runs none of them.
    pub max_tool_calls: usize,
    /// Largest serialized result of one call.
    pub max_tool_result_bytes: usize,
    /// Largest sum of the serialized results of one Job's calls.
    pub max_tool_results_bytes: usize,
    /// Longest one handler may run. A handler that never yields cannot be
    /// stopped by it.
    pub handler_timeout: Duration,
}

/// A [`ToolLoopLimits`] that cannot bound an execution as written.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ToolLimitsError {
    /// No Provider request is allowed, not even the first.
    #[error("the tool loop allows no Provider request")]
    NoProviderRequest,
    /// No handler is given any time, so every call would time out.
    #[error("the tool loop gives handlers no time")]
    NoHandlerTime,
}

impl ToolLoopLimits {
    /// Refuses limits that cannot bound an execution as written. A zero call
    /// limit or result limit stays valid: it refuses every call, or every
    /// result, explicitly.
    ///
    /// # Errors
    ///
    /// Returns which limit cannot be met.
    pub fn validate(&self) -> Result<(), ToolLimitsError> {
        if self.max_provider_requests == 0 {
            return Err(ToolLimitsError::NoProviderRequest);
        }
        if self.handler_timeout.is_zero() {
            return Err(ToolLimitsError::NoHandlerTime);
        }
        Ok(())
    }
}

impl Default for ToolLoopLimits {
    fn default() -> Self {
        Self {
            max_provider_requests: 4,
            max_tool_calls: 8,
            max_tool_result_bytes: 16 * 1024,
            max_tool_results_bytes: 64 * 1024,
            handler_timeout: Duration::from_secs(10),
        }
    }
}

/// Why a tool loop ended before a final answer, when the Provider did not
/// end it. None of these is a Provider failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ToolLoopStop {
    /// The limits cannot bound the execution, see
    /// [`ToolLoopLimits::validate`], so no Provider request was sent.
    InvalidLimits(ToolLimitsError),
    /// The Provider requested tools in the last request the limits allow.
    /// None of them ran.
    RequestLimit,
    /// The calls of a response would exceed the call limit. None of them ran.
    CallLimit,
    /// A call was refused at dispatch: OCG did not offer it, it is no longer
    /// the registration offered or no longer authorized, its id repeats an
    /// earlier call's, its registered schema refuses its arguments, or its
    /// arguments exceed what OCG records. It did not run; calls of the
    /// response before it may have, and their outcomes are recorded.
    Refused(InvalidToolCall),
    /// The database refused to record the admission of the call at this
    /// index, so its handler was not invoked.
    AdmissionRefused { index: u32, violation: Violation },
    /// The handler of the call at this index reported a failure, with
    /// evidence that its effect did not happen.
    HandlerFailed { index: u32, message: String },
    /// The handler of the call at this index reported a failure without
    /// evidence that its effect did not happen.
    HandlerUncertain { index: u32, message: String },
    /// The result of the call at this index exceeds the per-call or the
    /// accumulated limit. The handler completed; its result was not sent.
    ResultTooLarge { index: u32 },
    /// The result of the call at this index cannot be serialized. The
    /// handler completed; its result was not sent.
    ResultUnserializable { index: u32 },
    /// The handler of the call at this index did not finish within its
    /// limit. It was stopped; whatever it did is unknown.
    TimedOut { index: u32 },
    /// The execution was cancelled locally before every call was answered.
    Cancelled,
    /// The admission of the call at this index, or what its handler was
    /// observed to do, `outcome`, could not be recorded. Either way the
    /// call's effect is not known from what was recorded.
    EvidenceNotRecorded {
        index: u32,
        outcome: Option<ToolOutcomeKind>,
    },
}

impl ToolLoopStop {
    /// Whether the stop leaves the Job's outcome unknown rather than known
    /// to have failed: a handler stopped while it ran, reported an
    /// uncertain failure, or may have run without its evidence recorded.
    fn is_unknown(&self) -> bool {
        matches!(
            self,
            Self::TimedOut { .. }
                | Self::Cancelled
                | Self::HandlerUncertain { .. }
                | Self::EvidenceNotRecorded { .. }
        )
    }
}

impl std::fmt::Display for ToolLoopStop {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidLimits(error) => write!(formatter, "invalid tool loop limits: {error}"),
            Self::RequestLimit => {
                formatter.write_str("the Provider requested tools after the last allowed request")
            }
            Self::CallLimit => {
                formatter.write_str("the requested tool calls exceed the call limit")
            }
            Self::Refused(refusal) => write!(formatter, "a tool call was refused: {refusal}"),
            Self::AdmissionRefused { index, violation } => {
                write!(
                    formatter,
                    "the admission of tool call {index} was refused: {violation}"
                )
            }
            Self::HandlerFailed { index, message } => {
                write!(formatter, "tool call {index} failed: {message}")
            }
            Self::HandlerUncertain { index, message } => {
                write!(
                    formatter,
                    "tool call {index} failed and its effect is uncertain: {message}"
                )
            }
            Self::ResultTooLarge { index } => {
                write!(
                    formatter,
                    "the result of tool call {index} exceeds its limit"
                )
            }
            Self::ResultUnserializable { index } => {
                write!(
                    formatter,
                    "the result of tool call {index} cannot be serialized"
                )
            }
            Self::TimedOut { index } => write!(formatter, "tool call {index} timed out"),
            Self::Cancelled => formatter.write_str("the tool loop was cancelled"),
            Self::EvidenceNotRecorded { index, .. } => {
                write!(
                    formatter,
                    "the evidence of tool call {index} could not be recorded"
                )
            }
        }
    }
}

impl ProviderExecutionPlan {
    pub fn environment_credentials(
        executor_id: ExecutorId,
        input: JobInputDraft,
        job: NewJob,
    ) -> Self {
        Self {
            executor_id,
            input,
            job,
            credential_resolver: Arc::new(EnvironmentCredentialResolver),
            provider_config: ProviderExecutionConfig::default(),
            tools: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionReceipt {
    pub executor: ExecutorId,
    pub input: JobInputId,
    pub job: JobId,
    /// `None` for an unknown outcome, which publishes no result.
    pub result: Option<AppendedOutput>,
}

#[derive(Debug, thiserror::Error)]
pub enum ExecutionRuntimeError {
    #[error("scheduler queue is full")]
    SchedulerQueueFull,
    #[error("scheduler worker stopped")]
    SchedulerStopped,
    #[error("executor queue is full")]
    ExecutorQueueFull,
    #[error("executor worker stopped")]
    ExecutorStopped,
    #[error("database operation failed: {0}")]
    Database(#[from] DatabaseError),
    #[error("executor {0} is not registered")]
    ExecutorNotFound(ExecutorId),
    #[error("failed to start execution worker: {0}")]
    Startup(#[source] std::io::Error),
    #[error("execution worker panicked")]
    WorkerPanicked,
    /// Shutdown began before the plan started, so nothing of it, not even
    /// its input, was persisted.
    #[error(
        "the execution runtime is shutting down; the plan was not started and nothing was persisted"
    )]
    ShuttingDown,
    #[error("provider execution failed: {0}")]
    Provider(String),
    /// The Job's outcome is settled, as the receipt shows, but a Metrics or
    /// availability record could not be written. It is a local storage
    /// failure, not a Provider fault, and it changes nothing that settled.
    #[error("the Job was settled but an observation was not recorded: {source}")]
    ObservationsNotRecorded {
        receipt: Box<ExecutionReceipt>,
        source: DatabaseError,
    },
    /// The Provider answered successfully, but the Job could not complete
    /// because `source` refused it, so it was settled Failed, as the receipt
    /// shows. That is a local orchestration failure, not the Provider's: the
    /// answer is recorded as observed, apart from `observations`, the first
    /// observation that could not be written.
    #[error("the Provider answered, but the Job was settled failed: {source}")]
    CompletionRefused {
        receipt: Box<ExecutionReceipt>,
        source: Violation,
        observations: Option<Box<DatabaseError>>,
    },
    /// The tools selected for the plan are no longer registered as selected,
    /// or no longer authorized. Nothing of the plan was persisted.
    #[error("the plan's tools cannot be offered: {0}")]
    ToolsUnavailable(ToolRegistryError),
    /// The plan's tool loop limits cannot bound its execution. Nothing of
    /// the plan was persisted and nothing was sent.
    #[error("the plan's tool loop limits are invalid: {0}")]
    InvalidToolLimits(ToolLimitsError),
    /// The tool loop ended on this side before a final answer, as `stop`
    /// says, and the receipt shows how the Job settled: Failed when the stop
    /// is known, Unknown, with its stop confirmed, when a call's effect is
    /// uncertain. Every Provider request's own observations were recorded,
    /// apart from `observations`, the first that could not be written, and
    /// every call's evidence, apart from `evidence`, the write that failed.
    /// `stop_unconfirmed` is set when the outcome was recorded as unknown but
    /// the stop of the execution could not be confirmed; the Job then keeps
    /// its Profiles until a later start of the database settles it.
    #[error("the tool loop stopped: {stop}")]
    ToolLoopStopped {
        receipt: Box<ExecutionReceipt>,
        stop: ToolLoopStop,
        observations: Option<Box<DatabaseError>>,
        evidence: Option<Box<DatabaseError>>,
        stop_unconfirmed: Option<Box<DatabaseError>>,
    },
    /// The tool loop stopped, as `stop` says, but the Job's outcome could
    /// not be recorded, see [`ExecutionRuntimeError::NotSettled`]. `evidence`
    /// is the call evidence that could not be recorded, if any.
    #[error("the tool loop stopped ({stop}) but the Job was not settled: {source}")]
    ToolLoopNotSettled {
        stop: Box<ToolLoopStop>,
        source: DatabaseError,
        evidence: Option<Box<DatabaseError>>,
        observations: Option<Box<DatabaseError>>,
    },
    /// The Provider answered, but the Job's outcome could not be recorded.
    /// The Job is left as it was, started and unsettled, and keeps its Profiles
    /// and its local execution claim until it is settled, at the latest by a
    /// later start of the database; see [`DatabaseHandle::start_local_job`].
    /// What the Provider was observed to do is still recorded, apart from
    /// `observations`, the first observation that could not be written.
    #[error("the Provider answered but the Job was not settled: {source}")]
    NotSettled {
        source: DatabaseError,
        observations: Option<Box<DatabaseError>>,
    },
    /// The Job's outcome was recorded as unknown, as the receipt shows, but
    /// the stop of its local execution could not be recorded. The Job keeps
    /// its Profiles until a later start of the database settles it as
    /// abandoned; see [`DatabaseHandle::start_local_job`].
    #[error("the Job's outcome was recorded as unknown but its stop was not confirmed: {source}")]
    StopNotConfirmed {
        receipt: Box<ExecutionReceipt>,
        source: DatabaseError,
        /// The first observation that could not be recorded either, if any.
        observations: Option<Box<DatabaseError>>,
    },
}

/// Why a provider plan was not acknowledged as an admitted Job, see
/// [`SchedulerHandle::admit_provider`].
#[derive(Debug, thiserror::Error)]
pub enum AdmissionError {
    /// The plan never started and no Job was created for it: it was refused
    /// before its Job was created, or the creation is known not to have
    /// committed. An input it saved, if any, belongs to no Job.
    #[error("the plan was not admitted: {0}")]
    NotAdmitted(ExecutionRuntimeError),
    /// The creation and local start of the plan's Job was attempted, but
    /// whether it committed is unknown: a Job may exist, started and
    /// claimed by this runtime, which no longer executes it. It must not be
    /// submitted again as if nothing happened. A
    /// [`DatabaseError::OutcomeUnknown`] names the operation to reconcile;
    /// a Job left started is settled as abandoned by a later start of the
    /// database.
    #[error("whether the plan's Job was created is unknown: {0}")]
    Uncertain(ExecutionRuntimeError),
}

impl AdmissionError {
    /// The underlying error, as [`SchedulerHandle::schedule_provider`]
    /// reports it.
    pub fn into_inner(self) -> ExecutionRuntimeError {
        match self {
            Self::NotAdmitted(error) | Self::Uncertain(error) => error,
        }
    }
}

/// The Job of a provider plan, created and locally started.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct StartedJob {
    executor: ExecutorId,
    input: JobInputId,
    job: JobId,
}

type ExecutionReply = flume::Sender<Result<ExecutionReceipt, ExecutionRuntimeError>>;

/// Where the executor reports a provider plan: once when its Job has been
/// created and locally started, or why it was not; and once more, for a
/// started Job, when the execution has returned.
struct ProviderReply {
    started: flume::Sender<Result<StartedJob, AdmissionError>>,
    settled: ExecutionReply,
}

/// One submitted plan with its caller's reply. A provider plan may carry
/// the submission to record with its Job.
enum Submission {
    Plan(ExecutionPlan, ExecutionReply),
    Provider(ProviderExecutionPlan, Option<NewSubmission>, ProviderReply),
}

impl Submission {
    /// Answers a plan that will never start: nothing of it was persisted.
    fn refuse(self) {
        self.fail(ExecutionRuntimeError::ShuttingDown);
    }

    /// Answers a plan that never started, for `error`.
    fn fail(self, error: ExecutionRuntimeError) {
        match self {
            Self::Plan(_, reply) => {
                reply.send(Err(error)).ok();
            }
            Self::Provider(_, _, reply) => {
                reply
                    .started
                    .send(Err(AdmissionError::NotAdmitted(error)))
                    .ok();
            }
        }
    }
}

enum SchedulerMessage {
    Schedule(Box<Submission>),
    Shutdown,
}

/// What submitters, the workers and shutdown share. One lock covers every
/// field, so a plan either starts before shutdown begins, and then has its
/// cancellation registered for shutdown to cancel, or never starts; and a
/// plan is queued only while the scheduler will still take it from the queue.
#[derive(Default)]
struct RuntimeState {
    /// Set once shutdown begins. No plan starts or is queued after it.
    stopping: bool,
    /// Set once the scheduler stops taking plans, before it answers the
    /// plans still queued. No plan is queued after it.
    closed: bool,
    /// The cancellation of the provider execution in flight, if any.
    current: Option<CancellationToken>,
}

type SharedState = Arc<Mutex<RuntimeState>>;

fn lock(state: &SharedState) -> std::sync::MutexGuard<'_, RuntimeState> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A cloneable endpoint for scheduler submissions.
#[derive(Clone)]
pub struct SchedulerHandle {
    sender: flume::Sender<SchedulerMessage>,
    state: SharedState,
}

impl SchedulerHandle {
    /// Submit work without waiting for queue capacity.
    ///
    /// The returned future waits only for the executor's response. Queue
    /// admission remains explicit: a full queue returns an error immediately.
    pub async fn schedule(
        &self,
        plan: ExecutionPlan,
    ) -> Result<ExecutionReceipt, ExecutionRuntimeError> {
        let (reply, response) = flume::bounded(1);
        self.submit(Submission::Plan(plan, reply))?;
        response
            .recv_async()
            .await
            .map_err(|_| ExecutionRuntimeError::SchedulerStopped)?
    }

    /// Submits a provider execution and waits until it has settled: it is
    /// [`Self::admit_provider`] followed by [`AdmittedJob::settled`], with
    /// an admission failure reported as its underlying error. Cancelling
    /// `plan.provider_config.cancellation` stops the execution; shutdown of
    /// the runtime stops it too if it is executing then.
    pub async fn schedule_provider(
        &self,
        plan: ProviderExecutionPlan,
    ) -> Result<ExecutionReceipt, ExecutionRuntimeError> {
        self.admit_provider(plan)
            .await
            .map_err(AdmissionError::into_inner)?
            .settled()
            .await
    }

    /// Submits a provider execution and returns as soon as its one Job has
    /// been created with its input and locally started by the executor, in
    /// the one transaction that claims it, before anything is sent to the
    /// Provider. The executor then owns the execution: it runs, and settles
    /// the Job, whatever becomes of the returned [`AdmittedJob`]; dropping
    /// it, or the future of its settlement, neither stops nor cancels the
    /// Job. Its stop is requested through a cancellation of its own, a
    /// child of the plan's token: cancelling that token, or shutdown of the
    /// runtime, stops it too, but its stop reaches no other execution,
    /// whatever tokens the plans share.
    ///
    /// Admission is bounded as [`Self::schedule_provider`]'s is: a full
    /// queue, a stopped scheduler or executor, shutdown, an unregistered
    /// Executor, invalid tool limits, tools no longer offered, and a
    /// creation known not to have committed are each
    /// [`AdmissionError::NotAdmitted`]; a creation whose commit is unknown
    /// is [`AdmissionError::Uncertain`], never a refusal.
    ///
    /// # Errors
    ///
    /// Returns why no Job is known to have been admitted.
    pub async fn admit_provider(
        &self,
        plan: ProviderExecutionPlan,
    ) -> Result<AdmittedJob, AdmissionError> {
        self.admit(plan, None).await
    }

    /// Like [`Self::admit_provider`], and records `submission` with the
    /// plan's Job, in the transaction that creates and starts it, so the
    /// Job exists only with its submission, and the submission only with its
    /// Job. A key already recorded refuses the plan before its Job is
    /// created, as [`AdmissionError::NotAdmitted`] with
    /// [`DatabaseError::Rejected`] of [`Violation::SubmissionKeyUsed`],
    /// which names the Job the key created.
    ///
    /// # Errors
    ///
    /// As [`Self::admit_provider`].
    pub async fn admit_submitted_provider(
        &self,
        plan: ProviderExecutionPlan,
        submission: NewSubmission,
    ) -> Result<AdmittedJob, AdmissionError> {
        self.admit(plan, Some(submission)).await
    }

    async fn admit(
        &self,
        mut plan: ProviderExecutionPlan,
        submission: Option<NewSubmission>,
    ) -> Result<AdmittedJob, AdmissionError> {
        // The execution observes a cancellation of its own, which the
        // plan's token, however widely it is shared, still cancels.
        let cancellation = plan.provider_config.cancellation.child();
        plan.provider_config.cancellation = cancellation.clone();
        let (started, admission) = flume::bounded(1);
        let (settled, settlement) = flume::bounded(1);
        self.submit(Submission::Provider(
            plan,
            submission,
            ProviderReply { started, settled },
        ))
        .map_err(AdmissionError::NotAdmitted)?;
        // Every refusal is answered before the reply is dropped, so a
        // reply dropped unanswered means a worker stopped without knowing
        // whether the Job was created.
        let started = admission
            .recv_async()
            .await
            .map_err(|_| AdmissionError::Uncertain(ExecutionRuntimeError::SchedulerStopped))??;
        Ok(AdmittedJob {
            started,
            stop: JobStop {
                job: started.job,
                cancellation,
            },
            settlement,
        })
    }

    /// Once shutdown has begun, a plan is refused here without being queued.
    /// The check and the queueing happen under the state lock, which never
    /// waits for queue space, so a plan is either queued ahead of the
    /// scheduler's last drain or refused.
    fn submit(&self, submission: Submission) -> Result<(), ExecutionRuntimeError> {
        let state = lock(&self.state);
        if state.stopping {
            return Err(ExecutionRuntimeError::ShuttingDown);
        }
        if state.closed {
            return Err(ExecutionRuntimeError::SchedulerStopped);
        }
        self.sender
            .try_send(SchedulerMessage::Schedule(Box::new(submission)))
            .map_err(|error| match error {
                flume::TrySendError::Full(_) => ExecutionRuntimeError::SchedulerQueueFull,
                flume::TrySendError::Disconnected(_) => ExecutionRuntimeError::SchedulerStopped,
            })
    }
}

/// A provider plan's Job, created and locally started: an accepted,
/// running Job, not a settled one. Its identity is the one the execution,
/// its input, outputs, tool evidence and settlement are recorded under.
/// The runtime owns its execution; this only observes its settlement and
/// may request its stop. Dropping it changes nothing about the Job.
#[derive(Debug)]
pub struct AdmittedJob {
    started: StartedJob,
    stop: JobStop,
    settlement: flume::Receiver<Result<ExecutionReceipt, ExecutionRuntimeError>>,
}

impl AdmittedJob {
    pub fn job(&self) -> JobId {
        self.started.job
    }

    pub fn input(&self) -> JobInputId {
        self.started.input
    }

    pub fn executor(&self) -> ExecutorId {
        self.started.executor
    }

    /// A handle that requests the stop of this Job and of nothing else.
    pub fn stop_handle(&self) -> JobStop {
        self.stop.clone()
    }

    /// Requests the stop of this Job's execution, see [`JobStop::request`].
    pub fn request_stop(&self) {
        self.stop.request();
    }

    /// Waits until the execution has returned, and answers what
    /// [`SchedulerHandle::schedule_provider`] answers for it: the receipt
    /// of the Job, or why it settled, or failed to settle, as it did.
    ///
    /// # Errors
    ///
    /// As [`SchedulerHandle::schedule_provider`] once the Job was started.
    pub async fn settled(self) -> Result<ExecutionReceipt, ExecutionRuntimeError> {
        self.settlement
            .recv_async()
            .await
            .map_err(|_| ExecutionRuntimeError::SchedulerStopped)?
    }
}

/// Requests the stop of one admitted Job's execution. Cloneable; every
/// clone addresses the same Job.
#[derive(Clone, Debug)]
pub struct JobStop {
    job: JobId,
    cancellation: CancellationToken,
}

impl JobStop {
    pub fn job(&self) -> JobId {
        self.job
    }

    /// Cancels the Job's execution: the cancellation made for it at its
    /// admission, the one its execution observes, and nothing else. It
    /// cancels neither the token of the plan it was admitted from nor any
    /// other Job's execution, sharing that token or not. It is a request, not
    /// an outcome: the Job settles as the execution then ends, which is
    /// Unknown, not Cancelled, when a request may have reached the Provider
    /// or a call may have had an effect, and may still be Completed when
    /// the execution had already finished. Once the execution has returned
    /// it changes nothing.
    pub fn request(&self) {
        self.cancellation.cancel();
    }
}

/// Owns the scheduler and executor threads.
pub struct ExecutionRuntime {
    scheduler: SchedulerHandle,
    shutdown: CancellationToken,
    finished: Option<ntex::rt::JoinHandle<Result<(), ExecutionRuntimeError>>>,
}

impl ExecutionRuntime {
    pub fn start(database: DatabaseHandle) -> Result<Self, ExecutionRuntimeError> {
        #[cfg(test)]
        if ntex::rt::System::try_current().is_none() {
            return crate::test_support::on_system(move || async move { Self::start(database) });
        }

        let (sender, receiver) = flume::bounded(SCHEDULER_QUEUE_CAPACITY);
        let state = SharedState::default();
        let shutdown = CancellationToken::default();
        let finished = ntex::rt::spawn(executor_loop(
            database,
            receiver,
            state.clone(),
            shutdown.clone(),
        ));
        Ok(Self {
            scheduler: SchedulerHandle { sender, state },
            shutdown,
            finished: Some(finished),
        })
    }

    pub fn scheduler(&self) -> SchedulerHandle {
        self.scheduler.clone()
    }

    pub async fn shutdown(mut self) -> Result<(), ExecutionRuntimeError> {
        self.request_stop();
        if let Some(finished) = self.finished.take() {
            finished
                .await
                .map_err(|_| ExecutionRuntimeError::WorkerPanicked)??;
        }
        Ok(())
    }

    fn request_stop(&self) {
        let mut state = lock(&self.scheduler.state);
        state.stopping = true;
        if let Some(current) = &state.current {
            current.cancel();
        }
        self.shutdown.cancel();
        match self.scheduler.sender.try_send(SchedulerMessage::Shutdown) {
            Ok(())
            | Err(flume::TrySendError::Full(_))
            | Err(flume::TrySendError::Disconnected(_)) => {}
        }
    }
}

impl Drop for ExecutionRuntime {
    fn drop(&mut self) {
        self.request_stop();
    }
}

async fn executor_loop(
    database: DatabaseHandle,
    receiver: flume::Receiver<SchedulerMessage>,
    state: SharedState,
    shutdown: CancellationToken,
) -> Result<(), ExecutionRuntimeError> {
    while let Either::Right(Ok(message)) = select(shutdown.cancelled(), receiver.recv_async()).await
    {
        let submission = match message {
            SchedulerMessage::Schedule(submission) => *submission,
            SchedulerMessage::Shutdown => break,
        };
        {
            let mut state = lock(&state);
            if state.stopping {
                drop(state);
                submission.refuse();
                continue;
            }
            state.current = match &submission {
                Submission::Provider(plan, ..) => Some(plan.provider_config.cancellation.clone()),
                Submission::Plan(..) => None,
            };
        }
        match submission {
            Submission::Plan(plan, reply) => {
                let result = execute_plan(database.clone(), plan).await;
                lock(&state).current = None;
                reply.send(result).ok();
            }
            Submission::Provider(plan, submission, reply) => {
                let result =
                    execute_provider_plan(database.clone(), plan, submission, &reply.started).await;
                lock(&state).current = None;
                // A plan that never started has had its answer.
                if let Some(result) = result {
                    reply.settled.send(result).ok();
                }
            }
        }
    }
    lock(&state).closed = true;
    for message in receiver.drain() {
        if let SchedulerMessage::Schedule(submission) = message {
            submission.refuse();
        }
    }
    Ok(())
}

async fn execute_plan(
    database: DatabaseHandle,
    mut plan: ExecutionPlan,
) -> Result<ExecutionReceipt, ExecutionRuntimeError> {
    let executor = plan.executor_id;
    if database.executor(executor).await?.is_none() {
        return Err(ExecutionRuntimeError::ExecutorNotFound(executor));
    }
    plan.job.input = plan.input;
    let job = database.create_local_job(plan.job, executor).await?;
    let input = JobInputId::new(job.get());
    let result = database
        .finalize_job(job, plan.outcome.finalization(plan.result))
        .await?;
    let receipt = ExecutionReceipt {
        executor,
        input,
        job,
        result,
    };
    // Recording the outcome was all this runtime executed for the plan.
    if plan.outcome == JobOutcome::Unknown
        && let Err(source) = confirm_stopped(&database, job).await
    {
        return Err(ExecutionRuntimeError::StopNotConfirmed {
            receipt: Box::new(receipt),
            source,
            observations: None,
        });
    }
    Ok(receipt)
}

/// Records that this runtime's execution of a Job whose outcome was settled
/// unknown has stopped, which releases the Job's Profiles. It is called only
/// once the execution has returned: its future, its request and its
/// response have been dropped, so nothing in this process acts for the Job
/// any more. That says nothing about the Provider, and the outcome stays
/// unknown. A stop already confirmed counts as confirmed.
pub(crate) async fn confirm_stopped(
    database: &DatabaseHandle,
    job: JobId,
) -> Result<(), DatabaseError> {
    match database.confirm_job_stopped(job).await {
        Ok(()) | Err(DatabaseError::Rejected(Violation::StopAlreadyConfirmed(_))) => Ok(()),
        Err(error) => Err(error),
    }
}

/// Executes a provider plan: creates and locally starts its one Job,
/// reports that on `started`, and runs it to its settlement, which is
/// returned. A plan that does not start has why reported on `started`, and
/// returns `None`.
async fn execute_provider_plan(
    database: DatabaseHandle,
    mut plan: ProviderExecutionPlan,
    submission: Option<NewSubmission>,
    started: &flume::Sender<Result<StartedJob, AdmissionError>>,
) -> Option<Result<ExecutionReceipt, ExecutionRuntimeError>> {
    let offered = match start_provider_job(&database, &mut plan, submission).await {
        Ok((job, offered)) => {
            started.send(Ok(job)).ok();
            (job, offered)
        }
        Err(error) => {
            started.send(Err(error)).ok();
            return None;
        }
    };
    Some(run_started_provider_job(database, plan, offered).await)
}

/// Creates and locally starts the Job of a provider plan, once the plan is
/// checked, and returns it with the tool definitions to offer. Nothing is
/// persisted unless the Executor is registered, the tool limits are valid
/// and the tools can still be offered. A `submission` is recorded with the
/// Job in the transaction that creates it.
async fn start_provider_job(
    database: &DatabaseHandle,
    plan: &mut ProviderExecutionPlan,
    submission: Option<NewSubmission>,
) -> Result<(StartedJob, Option<Vec<ToolWire>>), AdmissionError> {
    let refused = AdmissionError::NotAdmitted;
    let executor = plan.executor_id;
    if database
        .executor(executor)
        .await
        .map_err(|error| refused(error.into()))?
        .is_none()
    {
        return Err(refused(ExecutionRuntimeError::ExecutorNotFound(executor)));
    }
    // The limits are checked, and the offer read from the registry, before
    // anything is persisted.
    if let Some(tools) = &plan.tools {
        tools
            .limits
            .validate()
            .map_err(|error| refused(ExecutionRuntimeError::InvalidToolLimits(error)))?;
    }
    let offered = plan
        .tools
        .as_ref()
        .map(|tools| tools.offer.wires())
        .transpose()
        .map_err(|error| refused(ExecutionRuntimeError::ToolsUnavailable(error)))?;
    plan.job.input = std::mem::take(&mut plan.input);
    #[cfg(test)]
    provider_seam::before_creation(plan.job.name.as_deref());
    let created = match submission {
        None => database.create_local_job(plan.job.clone(), executor).await,
        Some(submission) => {
            database
                .create_submitted_local_job(plan.job.clone(), executor, submission)
                .await
        }
    };
    let job = created.map_err(|error| match error.command_outcome() {
        Some(CommandOutcome::NotAccepted | CommandOutcome::NotCommitted) => refused(error.into()),
        Some(CommandOutcome::Unknown) | None => AdmissionError::Uncertain(error.into()),
    })?;
    Ok((
        StartedJob {
            executor,
            input: JobInputId::new(job.get()),
            job,
        },
        offered,
    ))
}

/// Runs a provider plan whose Job was started to its settlement.
async fn run_started_provider_job(
    database: DatabaseHandle,
    plan: ProviderExecutionPlan,
    (started, offered): (StartedJob, Option<Vec<ToolWire>>),
) -> Result<ExecutionReceipt, ExecutionRuntimeError> {
    let StartedJob {
        executor,
        input,
        job,
    } = started;

    // Settles the Job failed before anything was sent.
    let refused = async |summary: String| -> Result<ExecutionReceipt, ExecutionRuntimeError> {
        let failed = FinalResult {
            summary: Some(summary),
            code: None,
            goal_reached: Some(false),
            value: None,
        };
        let result = database
            .finalize_job(job, JobFinalization::Failed(failed))
            .await?;
        Ok(ExecutionReceipt {
            executor,
            input,
            job,
            result,
        })
    };
    let Some(job_record) = database.job(job).await? else {
        return refused("started Job record disappeared".to_owned()).await;
    };
    let Some(configuration) = database.job_configuration(job).await? else {
        return refused("provider execution requires frozen LLM configuration".to_owned()).await;
    };
    let Some(input_record) = database.job_input(input).await? else {
        return refused("submitted Job input disappeared".to_owned()).await;
    };
    // Nothing is sent unless every Attachment can be.
    let images = match attachment_images(&database, &input_record.attachments).await {
        Ok(images) => images,
        Err(AttachmentImageError::Attachment(AttachmentError::Database(error))) => {
            return Err(error.into());
        }
        Err(error) => {
            return refused(format!("the input's attachments cannot be sent: {error}")).await;
        }
    };
    let mut conversation = if let Some(first_previous) = job_record.previous {
        const MAX_CONVERSATION_TURNS: usize = 64;
        let genesis = database.genesis();
        let mut visited = std::collections::BTreeSet::new();
        visited.insert(job);
        let mut prior_turns = Vec::new();
        let mut cursor = Some(first_previous);
        while let Some(current) = cursor {
            if prior_turns.len() + 1 >= MAX_CONVERSATION_TURNS {
                return refused("conversation exceeds the maximum number of turns".to_owned())
                    .await;
            }
            if !visited.insert(current) {
                return refused("conversation contains a cycle of previous Jobs".to_owned()).await;
            }
            let Some(ancestor) = database.job(current).await? else {
                return refused(format!("previous Job {current} does not exist")).await;
            };
            if current == genesis
                || ancestor.parent != genesis
                || database.job_configuration(current).await?.is_none()
            {
                return refused(format!(
                    "previous Job {current} cannot be used as conversation context"
                ))
                .await;
            }
            if ancestor.completed_at.is_none() {
                return refused(format!("previous Job {current} has not completed")).await;
            }
            let Some(ancestor_input) = database.job_input_for_job(current).await? else {
                return refused(format!("previous Job {current} input disappeared")).await;
            };
            let ancestor_images =
                match attachment_images(&database, &ancestor_input.attachments).await {
                    Ok(imgs) => imgs,
                    Err(AttachmentImageError::Attachment(AttachmentError::Database(error))) => {
                        return Err(error.into());
                    }
                    Err(error) => {
                        return refused(format!(
                            "previous Job {current} attachments cannot be sent: {error}"
                        ))
                        .await;
                    }
                };
            let assistant_text = match database.latest_output(current).await? {
                Some(output) if output.summary.is_some() => output.summary.unwrap_or_default(),
                _ => {
                    let outputs = database.job_outputs(current, None, 1_000).await?;
                    outputs
                        .into_iter()
                        .filter(|output| output.kind == crate::persistence::OutputKind::Progress)
                        .filter_map(|output| output.summary)
                        .collect::<String>()
                }
            };
            prior_turns.push((ancestor_input.objective, ancestor_images, assistant_text));
            cursor = ancestor.previous;
        }
        prior_turns.reverse();
        let mut built = provider_execution::objective_conversation(&configuration.frozen, None);
        built.messages.pop();
        for (turn_objective, turn_images, assistant_text) in prior_turns {
            if let Some(user_msg) = provider_execution::objective_conversation_with_images(
                &configuration.frozen,
                turn_objective.as_deref(),
                &turn_images,
            )
            .messages
            .pop()
            {
                built.messages.push(user_msg);
            }
            built
                .messages
                .push(ChatMessage::assistant(Some(assistant_text), Vec::new()));
        }
        if let Some(user_msg) = provider_execution::objective_conversation_with_images(
            &configuration.frozen,
            input_record.objective.as_deref(),
            &images,
        )
        .messages
        .pop()
        {
            built.messages.push(user_msg);
        }
        built
    } else {
        provider_execution::objective_conversation_with_images(
            &configuration.frozen,
            input_record.objective.as_deref(),
            &images,
        )
    };
    conversation.tools = offered.unwrap_or_default();
    let frozen = &configuration.frozen;
    let resolver = plan.credential_resolver.as_ref();
    let config = &plan.provider_config;
    run_provider_job(
        ProviderRun {
            database,
            executor,
            input,
            job,
        },
        conversation,
        plan.tools.as_ref(),
        &config.cancellation,
        async |conversation: &ChatConversation, sink: &mut DatabaseOutputSink| {
            #[cfg(test)]
            if provider_seam::is_scripted(frozen) {
                return provider_seam::exchange(frozen, config, conversation, sink).await;
            }
            provider_execution::execute_conversation(
                frozen,
                conversation,
                resolver,
                config,
                Some(sink),
            )
            .await
        },
    )
    .await
}

/// A test stand-in for the Provider of a plan executed by the runtime, and a
/// hook before a provider plan's Job is created. Both are keyed, by the
/// Provider's endpoint and by the Job's name, so tests running at once do
/// not see each other's.
#[cfg(test)]
pub(crate) mod provider_seam {
    use std::collections::BTreeMap;
    use std::sync::{Mutex, PoisonError};
    use std::time::Instant;

    use ntex::util::select;

    use super::DatabaseOutputSink;
    use crate::openai_compatible::ChatConversation;
    use crate::persistence::FrozenExecutionConfig;
    use crate::provider_execution::scripted::{Step, payload};
    use crate::provider_execution::{
        self, ProviderExecutionConfig, ProviderExecutionError, ProviderExecutionResult,
        ProviderFact,
    };

    /// One scripted response: the steps of its body, read once `gate`
    /// yields, is dropped, or the execution is cancelled.
    pub(crate) struct Response {
        pub(crate) gate: Option<flume::Receiver<()>>,
        pub(crate) steps: Vec<Step>,
    }

    type Script = Box<dyn FnMut(&ChatConversation) -> Response + Send>;
    type Hook = Box<dyn FnMut() + Send>;

    static SCRIPTS: Mutex<BTreeMap<String, Script>> = Mutex::new(BTreeMap::new());
    static HOOKS: Mutex<BTreeMap<String, Hook>> = Mutex::new(BTreeMap::new());

    /// Answers every request to `endpoint` with what `script` gives, instead
    /// of sending it.
    pub(crate) fn install(
        endpoint: &str,
        script: impl FnMut(&ChatConversation) -> Response + Send + 'static,
    ) {
        SCRIPTS
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(endpoint.to_owned(), Box::new(script));
    }

    /// Runs `hook` immediately before the Job of a provider plan named
    /// `name` is created, once its input was saved.
    pub(crate) fn before_creation_of(name: &str, hook: impl FnMut() + Send + 'static) {
        HOOKS
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(name.to_owned(), Box::new(hook));
    }

    pub(super) fn before_creation(name: Option<&str>) {
        let Some(name) = name else { return };
        if let Some(hook) = HOOKS
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_mut(name)
        {
            hook();
        }
    }

    fn endpoint(frozen: &FrozenExecutionConfig) -> Option<&str> {
        frozen.llm.provider.endpoint.as_deref()
    }

    pub(super) fn is_scripted(frozen: &FrozenExecutionConfig) -> bool {
        endpoint(frozen).is_some_and(|endpoint| {
            SCRIPTS
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .contains_key(endpoint)
        })
    }

    /// Answers as the scripted Provider would a request it received; as
    /// the transport does, nothing is sent once the execution is cancelled.
    pub(super) async fn exchange(
        frozen: &FrozenExecutionConfig,
        config: &ProviderExecutionConfig,
        conversation: &ChatConversation,
        sink: &mut DatabaseOutputSink,
    ) -> Result<ProviderExecutionResult, ProviderExecutionError> {
        if config.cancellation.is_cancelled() {
            provider_execution::announce(Some(&mut *sink), ProviderFact::NotDelivered).await;
            return Err(ProviderExecutionError::NotDelivered {
                message: "provider execution was cancelled before sending".to_owned(),
            });
        }
        let response = {
            let mut scripts = SCRIPTS.lock().unwrap_or_else(PoisonError::into_inner);
            let script = endpoint(frozen)
                .and_then(|endpoint| scripts.get_mut(endpoint))
                .expect("a scripted Provider");
            script(conversation)
        };
        provider_execution::announce(Some(&mut *sink), ProviderFact::Delivered).await;
        if let Some(gate) = response.gate {
            select(config.cancellation.cancelled(), gate.recv_async()).await;
        }
        provider_execution::read_response(
            payload(response.steps, config.cancellation.clone()),
            200,
            Instant::now(),
            None,
            config,
            Some(sink),
            conversation,
        )
        .await
    }
}

/// Why an input's Attachments cannot be sent as images.
#[derive(Debug, thiserror::Error)]
pub enum AttachmentImageError {
    #[error(transparent)]
    Attachment(#[from] AttachmentError),
    #[error("a request may carry at most {MAX_REQUEST_IMAGES} images")]
    TooMany,
    #[error("{attachment} cannot be sent as an image: {error}")]
    NotAnImage {
        attachment: AttachmentId,
        error: ImageError,
    },
    #[error("the images may not exceed {MAX_REQUEST_IMAGE_BYTES} bytes together")]
    TooLarge,
}

/// The images of `attachments`, in order. Each Attachment must exist, be
/// available and be a PNG or JPEG within the image limits, as its record
/// says, before any content is read; then its content is read from the
/// attachment store, verified against the record's length and SHA-256, and
/// checked again as the image its record declares. Nothing is substituted
/// for an Attachment that fails, and nothing is skipped.
///
/// # Errors
///
/// Returns the first Attachment, or the limit, that fails.
pub async fn attachment_images(
    database: &DatabaseHandle,
    attachments: &[AttachmentId],
) -> Result<Vec<ImageInput>, AttachmentImageError> {
    if attachments.len() > MAX_REQUEST_IMAGES {
        return Err(AttachmentImageError::TooMany);
    }
    let mut declared = Vec::with_capacity(attachments.len());
    let mut total = 0u64;
    for &attachment in attachments {
        let record = database
            .attachment(attachment)
            .await
            .map_err(AttachmentError::Database)?
            .ok_or(AttachmentError::NotFound(attachment))?;
        if let Some(cleanup) = record.cleanup {
            return Err(AttachmentError::CleanedUp {
                id: attachment,
                cleanup,
            }
            .into());
        }
        let not_an_image = |error| AttachmentImageError::NotAnImage { attachment, error };
        let mime =
            ImageMime::from_mime(&record.mime).ok_or(not_an_image(ImageError::UnsupportedType))?;
        if record.byte_length > MAX_IMAGE_BYTES as u64 {
            return Err(not_an_image(ImageError::TooLarge));
        }
        total += record.byte_length;
        declared.push((attachment, mime));
    }
    if total > MAX_REQUEST_IMAGE_BYTES as u64 {
        return Err(AttachmentImageError::TooLarge);
    }
    let mut images = Vec::with_capacity(declared.len());
    for (attachment, mime) in declared {
        let content = database.attachment_content(attachment).await?;
        images.push(
            ImageInput::new(mime, content.bytes)
                .map_err(|error| AttachmentImageError::NotAnImage { attachment, error })?,
        );
    }
    Ok(images)
}

/// The Job a provider execution settles.
pub(crate) struct ProviderRun {
    pub(crate) database: DatabaseHandle,
    pub(crate) executor: ExecutorId,
    pub(crate) input: JobInputId,
    pub(crate) job: JobId,
}

/// Sends `conversation` with `exchange` and settles the Job from where it
/// ends. A response that requests no tool is the final answer candidate and
/// is settled by [`finish_provider_job`], as is any Provider failure. A
/// response that requests tools never completes the Job: its calls are run
/// by [`answer_calls`], each answered in order, and the assistant message
/// with every result is sent back as the next request, until a response
/// requests none or the loop stops, see [`ToolLoopStop`]. Each request is
/// numbered, and its observations are recorded as its own.
pub(crate) async fn run_provider_job(
    run: ProviderRun,
    mut conversation: ChatConversation,
    tools: Option<&ToolExecution>,
    cancellation: &CancellationToken,
    mut exchange: impl AsyncFnMut(
        &ChatConversation,
        &mut DatabaseOutputSink,
    ) -> Result<ProviderExecutionResult, ProviderExecutionError>,
) -> Result<ExecutionReceipt, ExecutionRuntimeError> {
    let mut sink = output_sink(run.database.clone(), run.job);
    if let Some(Err(error)) = tools.map(|tools| tools.limits.validate()) {
        // Nothing was sent, so nothing of a Provider is recorded.
        let stopped = Stopped::from(ToolLoopStop::InvalidLimits(error));
        return stop_tool_loop(run, sink, None, stopped).await;
    }
    let mut budget = ToolBudget::default();
    loop {
        let result = match exchange(&conversation, &mut sink).await {
            Ok(result) if result.requests_tools() => result,
            execution => {
                return finish_provider_job(
                    run.database,
                    run.executor,
                    run.input,
                    run.job,
                    sink,
                    execution,
                )
                .await;
            }
        };
        // A response that requests tools is no answer, but its facts are
        // this request's own.
        if let Some(error) = record_answer(&run.database, run.job, sink.request, &result).await {
            sink.gap.get_or_insert(error);
        }
        let next = sink.request.next().filter(|next| {
            tools.is_some_and(|tools| next.get() <= tools.limits.max_provider_requests)
        });
        let answered = match (tools, next) {
            (Some(tools), Some(_)) => {
                let site = CallSite {
                    database: &run.database,
                    job: run.job,
                    request: sink.request,
                };
                answer_calls(
                    tools,
                    &site,
                    &conversation,
                    &result.tool_calls,
                    &mut budget,
                    cancellation,
                )
                .await
            }
            (Some(_), None) => Err(ToolLoopStop::RequestLimit.into()),
            (None, _) => Err(ToolLoopStop::Refused(InvalidToolCall::new(
                ToolCallRefusal::Unauthorized,
                "the Provider requested tools that OCG did not offer",
            ))
            .into()),
        };
        let (answers, next) = match (answered, next) {
            (Ok(answers), Some(next)) => (answers, next),
            (Err(stopped), _) => {
                return stop_tool_loop(run, sink, Some(&result), stopped).await;
            }
            (Ok(_), None) => unreachable!("calls are answered only when a request may follow"),
        };
        let text = Some(result.text.clone()).filter(|text| !text.is_empty());
        conversation.messages.push(ChatMessage::assistant(
            text,
            result
                .tool_calls
                .iter()
                .map(ChatToolCallWire::from)
                .collect(),
        ));
        for (call, content) in result.tool_calls.iter().zip(answers) {
            conversation
                .messages
                .push(ChatMessage::tool(call.id.clone(), content));
        }
        sink.request = next;
    }
}

/// What one Job's calls have used of their limits.
#[derive(Default)]
struct ToolBudget {
    calls: usize,
    argument_bytes: usize,
    result_bytes: usize,
}

/// The Provider request whose calls are answered, and where their evidence
/// is recorded.
struct CallSite<'a> {
    database: &'a DatabaseHandle,
    job: JobId,
    request: RequestNumber,
}

impl CallSite<'_> {
    fn call(&self, call: &CompletedToolCall) -> ToolCallRef {
        ToolCallRef {
            job: self.job,
            request: self.request,
            index: call.index,
        }
    }
}

/// Why a tool loop stopped, with the call evidence that could not be
/// recorded, if any.
struct Stopped {
    stop: ToolLoopStop,
    evidence: Option<DatabaseError>,
    proven_stop: Option<ToolCallRef>,
}

impl From<ToolLoopStop> for Stopped {
    fn from(stop: ToolLoopStop) -> Self {
        Self {
            stop,
            evidence: None,
            proven_stop: None,
        }
    }
}

/// Test seams of the dispatch of tool calls.
#[cfg(test)]
pub(crate) mod dispatch_seam {
    use std::cell::RefCell;

    use crate::persistence::ToolCallRef;

    /// Where in a call's dispatch a seam runs.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(crate) enum Point {
        /// Its admission is about to be recorded.
        BeforeAdmission,
        /// Its admission is durable; its final authorization check and its
        /// handler have not run.
        Admitted,
        /// Its handler has returned or was stopped; its outcome is not
        /// recorded yet.
        Ran,
    }

    type Seam = Box<dyn FnMut(Point, ToolCallRef)>;

    thread_local! {
        static SEAM: RefCell<Option<Seam>> = const { RefCell::new(None) };
    }

    /// Runs `seam` at every dispatch point of this thread's tool loops.
    pub(crate) fn install(seam: impl FnMut(Point, ToolCallRef) + 'static) {
        SEAM.with(|installed| *installed.borrow_mut() = Some(Box::new(seam)));
    }

    pub(super) fn reach(point: Point, call: ToolCallRef) {
        let seam = SEAM.with(|installed| installed.borrow_mut().take());
        if let Some(mut seam) = seam {
            seam(point, call);
            SEAM.with(|installed| {
                installed.borrow_mut().get_or_insert(seam);
            });
        }
    }
}

/// Answers every call of one response, in order, or stops. The calls are
/// checked together before any runs: against the call limit, against the
/// ids already in the conversation, each against the registry and its
/// schema, and their arguments against what OCG records. A call refused
/// then, or when it is checked again before its admission, never runs, and
/// its refusal is recorded. Each call that passes is admitted durably
/// before anything of it runs; its authorization is then checked once more,
/// with nothing awaited between that check and the handler's invocation, so
/// a grant withdrawn while the admission was recorded refuses it. A handler
/// runs under the time limit and the execution's cancellation, and what is
/// known of its outcome is recorded before its result can be sent on. Its
/// result, serialized, is the answer, and it is never truncated.
async fn answer_calls(
    tools: &ToolExecution,
    site: &CallSite<'_>,
    conversation: &ChatConversation,
    calls: &[CompletedToolCall],
    budget: &mut ToolBudget,
    cancellation: &CancellationToken,
) -> Result<Vec<String>, Stopped> {
    let limits = &tools.limits;
    if cancellation.is_cancelled() {
        return Err(ToolLoopStop::Cancelled.into());
    }
    if budget.calls.saturating_add(calls.len()) > limits.max_tool_calls {
        return Err(ToolLoopStop::CallLimit.into());
    }
    let earlier = conversation
        .messages
        .iter()
        .filter_map(|message| message.tool_calls.as_deref())
        .flatten()
        .map(|call| call.id.as_str())
        .collect::<std::collections::HashSet<_>>();
    let mut argument_bytes = budget.argument_bytes;
    for call in calls {
        if earlier.contains(call.id.as_str()) {
            return Err(ToolLoopStop::Refused(InvalidToolCall::at(
                ToolCallRefusal::ConflictingIdentity,
                call.index,
                format!("tool call {} repeats the id of an earlier call", call.index),
            ))
            .into());
        }
        if let Err(refusal) = tools.offer.resolve(call) {
            return Err(refuse(site, call, refusal).await);
        }
        argument_bytes = argument_bytes.saturating_add(call.arguments.len());
        if call.arguments.len() > MAX_STORED_TOOL_ARGUMENTS_BYTES
            || argument_bytes > MAX_STORED_TOOL_ARGUMENTS_PER_JOB_BYTES
        {
            let refusal = InvalidToolCall::at(
                ToolCallRefusal::LimitExceeded,
                call.index,
                format!(
                    "tool call {} has arguments beyond what OCG records",
                    call.index
                ),
            );
            return Err(refuse(site, call, refusal).await);
        }
    }

    let mut answers = Vec::with_capacity(calls.len());
    for call in calls {
        if cancellation.is_cancelled() {
            return Err(ToolLoopStop::Cancelled.into());
        }
        let dispatch = match tools.offer.resolve(call) {
            Ok(dispatch) => dispatch,
            Err(refusal) => return Err(refuse(site, call, refusal).await),
        };
        let index = call.index;
        let reference = site.call(call);
        let decided = ToolAdmission {
            handler_version: dispatch.version,
            effect: dispatch.effect,
            parameters: dispatch.schema,
            arguments: ToolPayload::new(call.arguments.clone()),
            authorization: AuthorizationBasis::RegistryGrant,
        };
        let admission = NewToolEvidence {
            call: reference,
            tool_call_id: call.id.clone(),
            name: call.name.clone(),
            spawns: decided.spawns(),
            decision: ToolDecision::Admitted(decided),
        };
        #[cfg(test)]
        dispatch_seam::reach(dispatch_seam::Point::BeforeAdmission, reference);
        match site.database.record_tool_decision(admission).await {
            Ok(()) => {}
            // Nothing was committed, and nothing runs.
            Err(DatabaseError::Rejected(violation)) => {
                return Err(ToolLoopStop::AdmissionRefused { index, violation }.into());
            }
            // The admission may have committed, so the call is as uncertain
            // as one that ran, though nothing runs.
            Err(error) => {
                return Err(Stopped {
                    stop: ToolLoopStop::EvidenceNotRecorded {
                        index,
                        outcome: None,
                    },
                    evidence: Some(error),
                    proven_stop: Some(reference),
                });
            }
        }
        budget.calls += 1;
        budget.argument_bytes = budget.argument_bytes.saturating_add(call.arguments.len());
        #[cfg(test)]
        dispatch_seam::reach(dispatch_seam::Point::Admitted, reference);
        // The execution may have been cancelled while the admission was
        // recorded.
        if cancellation.is_cancelled() {
            return Err(cancelled_before_invocation(site, reference).await);
        }
        let dispatch = match tools.offer.resolve(call) {
            Ok(dispatch) => dispatch,
            Err(refusal) => {
                let outcome = ToolOutcome::NotInvoked {
                    reason: NotInvokedReason::Refused(refusal.kind),
                };
                return Err(
                    conclude(site, reference, outcome, ToolLoopStop::Refused(refusal)).await,
                );
            }
        };
        // The invocation boundary. A cancellation seen here keeps the handler
        // from being invoked; one that comes later, even before the handler
        // is first polled, leaves its outcome uncertain.
        if cancellation.is_cancelled() {
            return Err(cancelled_before_invocation(site, reference).await);
        }
        let handler = dispatch.handler;
        let spawns = dispatch.spawns;
        let arguments = dispatch.arguments;
        let stopping_handler = handler.clone();
        let invocation = CatchPanic(Box::pin(async move {
            handler.call_for(reference, arguments).await
        }));
        let ran = select(
            cancellation.cancelled(),
            ntex::time::timeout(limits.handler_timeout, invocation),
        )
        .await;
        #[cfg(test)]
        dispatch_seam::reach(dispatch_seam::Point::Ran, reference);
        let stopped = match &ran {
            Either::Left(()) | Either::Right(Err(())) | Either::Right(Ok(Err(_))) if spawns => {
                stopping_handler.stop(site.job).await
            }
            _ => Ok(()),
        };
        if let Err(failure) = stopped {
            return Err(conclude(
                site,
                reference,
                ToolOutcome::Uncertain {
                    cause: UncertainCause::HandlerReported,
                    message: Some(failure.message),
                },
                ToolLoopStop::HandlerUncertain {
                    index,
                    message: "the handler could not confirm its process cleanup".to_owned(),
                },
            )
            .await);
        }
        let (outcome, stop) = match ran {
            Either::Left(()) => (
                ToolOutcome::Uncertain {
                    cause: UncertainCause::Cancelled,
                    message: None,
                },
                ToolLoopStop::Cancelled,
            ),
            Either::Right(Err(())) => (
                ToolOutcome::Uncertain {
                    cause: UncertainCause::TimedOut,
                    message: None,
                },
                ToolLoopStop::TimedOut { index },
            ),
            Either::Right(Ok(Err(failure))) => match failure.certainty {
                FailureCertainty::NotApplied => (
                    ToolOutcome::NotApplied {
                        message: failure.message.clone(),
                    },
                    ToolLoopStop::HandlerFailed {
                        index,
                        message: failure.message,
                    },
                ),
                FailureCertainty::Uncertain => (
                    ToolOutcome::Uncertain {
                        cause: UncertainCause::HandlerReported,
                        message: Some(failure.message.clone()),
                    },
                    ToolLoopStop::HandlerUncertain {
                        index,
                        message: failure.message,
                    },
                ),
            },
            Either::Right(Ok(Ok(value))) => match serde_json::to_string(&value) {
                Err(_) => (
                    ToolOutcome::CompletedWithheld {
                        reason: WithheldReason::Unserializable,
                    },
                    ToolLoopStop::ResultUnserializable { index },
                ),
                Ok(content) => {
                    let total = budget.result_bytes.saturating_add(content.len());
                    if content.len() > limits.max_tool_result_bytes
                        || total > limits.max_tool_results_bytes
                    {
                        (
                            ToolOutcome::CompletedWithheld {
                                reason: WithheldReason::TooLarge,
                            },
                            ToolLoopStop::ResultTooLarge { index },
                        )
                    } else {
                        let outcome = ToolOutcome::Completed {
                            result: ToolPayload::new(content.clone()),
                        };
                        // The result is sent on only once it is recorded.
                        if let Err(error) =
                            site.database.record_tool_outcome(reference, outcome).await
                        {
                            return Err(Stopped {
                                stop: ToolLoopStop::EvidenceNotRecorded {
                                    index,
                                    outcome: Some(ToolOutcomeKind::Completed),
                                },
                                evidence: Some(error),
                                proven_stop: Some(reference),
                            });
                        }
                        budget.result_bytes = total;
                        answers.push(content);
                        continue;
                    }
                }
            },
        };
        let mut stopped = conclude(site, reference, outcome, stop).await;
        if spawns {
            stopped.proven_stop = Some(reference);
        }
        return Err(stopped);
    }
    Ok(answers)
}

/// A handler's call, created and polled so that a panic in the handler fails
/// the call as an uncertain failure instead of unwinding through the
/// executor worker. It only catches a panic: a handler that blocks without
/// yielding still blocks the worker, and nothing here can stop it.
struct CatchPanic(ToolFuture<'static>);

impl Future for CatchPanic {
    type Output = Result<serde_json::Value, ToolFailure>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let call = &mut self.0;
        match catch_unwind(AssertUnwindSafe(|| call.as_mut().poll(context))) {
            Ok(polled) => polled,
            // The call is not polled again: it is dropped with this future.
            Err(_) => Poll::Ready(Err(ToolFailure::uncertain("the tool handler panicked"))),
        }
    }
}

/// Records the refusal of a call that never ran, and stops on it. A
/// refusal that cannot be recorded changes nothing about the call, which
/// did not run, so the stop stays the refusal.
async fn refuse(
    site: &CallSite<'_>,
    call: &CompletedToolCall,
    refusal: InvalidToolCall,
) -> Stopped {
    let recorded = site
        .database
        .record_tool_decision(NewToolEvidence {
            call: site.call(call),
            tool_call_id: call.id.clone(),
            name: call.name.clone(),
            decision: ToolDecision::Refused(refusal.kind),
            spawns: false,
        })
        .await;
    Stopped {
        stop: ToolLoopStop::Refused(refusal),
        evidence: recorded.err(),
        proven_stop: None,
    }
}

/// Records that an admitted call's handler was never invoked because the
/// execution was cancelled first, and stops as a cancellation. If that
/// cannot be recorded, the call stays as uncertain as its admission left it.
async fn cancelled_before_invocation(site: &CallSite<'_>, call: ToolCallRef) -> Stopped {
    let outcome = ToolOutcome::NotInvoked {
        reason: NotInvokedReason::Cancelled,
    };
    conclude(site, call, outcome, ToolLoopStop::Cancelled).await
}

/// Records the outcome of an admitted call that stops the loop. When it
/// cannot be recorded, the call's effect is not known from what was
/// recorded, so a stop that would be known becomes an unknown one.
async fn conclude(
    site: &CallSite<'_>,
    call: ToolCallRef,
    outcome: ToolOutcome,
    stop: ToolLoopStop,
) -> Stopped {
    let kind = outcome.kind();
    let proven_stop = outcome.is_definite().then_some(call);
    let mut stopped = match site.database.record_tool_outcome(call, outcome).await {
        Ok(()) => stop.into(),
        Err(error) if stop.is_unknown() => Stopped {
            stop,
            evidence: Some(error),
            proven_stop: None,
        },
        Err(error) => Stopped {
            stop: ToolLoopStop::EvidenceNotRecorded {
                index: call.index,
                outcome: Some(kind),
            },
            evidence: Some(error),
            proven_stop: None,
        },
    };
    stopped.proven_stop = proven_stop;
    stopped
}

async fn tool_processes_stopped(
    database: &DatabaseHandle,
    job: JobId,
    proven_stop: Option<ToolCallRef>,
) -> Result<bool, DatabaseError> {
    let mut after = None;
    loop {
        let calls = database.tool_evidence(job, after, 1000).await?;
        for record in &calls {
            if record.spawns
                && proven_stop != Some(record.call)
                && !record
                    .outcome
                    .as_ref()
                    .is_some_and(|outcome| outcome.fact.is_definite())
            {
                return Ok(false);
            }
        }
        let Some(last) = calls.last() else {
            return Ok(true);
        };
        after = Some((last.call.request, last.call.index));
    }
}

/// Settles a Job whose tool loop stopped on this side. A known stop fails
/// the Job, a local orchestration failure that keeps the Provider's last
/// response, if there was one, as its value; a stop that leaves a call's
/// effect uncertain leaves the outcome unknown, and the stop of the
/// execution is then confirmed. No Provider failure is recorded for either.
async fn stop_tool_loop(
    run: ProviderRun,
    sink: DatabaseOutputSink,
    last: Option<&ProviderExecutionResult>,
    stopped: Stopped,
) -> Result<ExecutionReceipt, ExecutionRuntimeError> {
    let database = run.database;
    let job = run.job;
    let Stopped {
        stop,
        evidence,
        proven_stop,
    } = stopped;
    let evidence = evidence.map(Box::new);
    let finalization = if stop.is_unknown() {
        JobFinalization::Unknown
    } else {
        JobFinalization::Failed(FinalResult {
            summary: Some(stop.to_string()),
            code: None,
            goal_reached: Some(false),
            value: last.map(provider_execution::result_value),
        })
    };
    let observations = sink.gap.map(Box::new);
    let result = match database.finalize_job(job, finalization).await {
        Ok(result) => result,
        Err(source) => {
            return Err(ExecutionRuntimeError::ToolLoopNotSettled {
                stop: Box::new(stop),
                source,
                evidence,
                observations,
            });
        }
    };
    let receipt = Box::new(ExecutionReceipt {
        executor: run.executor,
        input: run.input,
        job,
        result,
    });
    let stop_unconfirmed = if stop.is_unknown() {
        match tool_processes_stopped(&database, job, proven_stop).await {
            Ok(true) => confirm_stopped(&database, job).await.err().map(Box::new),
            Ok(false) => None,
            Err(error) => Some(Box::new(error)),
        }
    } else {
        None
    };
    Err(ExecutionRuntimeError::ToolLoopStopped {
        receipt,
        stop,
        observations,
        evidence,
        stop_unconfirmed,
    })
}

/// Records the facts of one answered request: its timing and its
/// availability observation. Returns the first write that failed.
async fn record_answer(
    database: &DatabaseHandle,
    job: JobId,
    request: RequestNumber,
    result: &ProviderExecutionResult,
) -> Option<DatabaseError> {
    let timing = database
        .record_metrics(NewMetrics {
            job,
            request,
            observed_at: Timestamp::now(),
            measurements: provider_execution::metrics(result),
            token: None,
        })
        .await
        .map(|_| ());
    let availability = database
        .record_availability_stat(NewAvailabilityStat {
            job,
            request,
            observed_at: Timestamp::now(),
            observation: provider_execution::success_observation(result),
        })
        .await
        .map(|_| ());
    timing.err().or(availability.err())
}

/// Settles the Job from a finished execution, then records what the run
/// observed. Settlement comes first and is authoritative: a later write that
/// fails is reported as [`ExecutionRuntimeError::ObservationsNotRecorded`]
/// and undoes nothing. Every observation is still attempted, so one failed
/// write does not cost the others, and a successful execution's
/// observations are attempted even when its Job cannot be completed. A
/// completion refused for an unsatisfied dependency settles the Job Failed
/// instead, keeping the Provider's result, see
/// [`ExecutionRuntimeError::CompletionRefused`]; any other refusal or failure
/// leaves it unsettled, see [`ExecutionRuntimeError::NotSettled`].
pub(crate) async fn finish_provider_job(
    database: DatabaseHandle,
    executor: ExecutorId,
    input: JobInputId,
    job: JobId,
    sink: DatabaseOutputSink,
    execution: Result<ProviderExecutionResult, ProviderExecutionError>,
) -> Result<ExecutionReceipt, ExecutionRuntimeError> {
    let request = sink.request;
    let mut gap = sink.gap;
    let receipt = match execution {
        Ok(result) => {
            let mut refused = None;
            let settled = match database
                .finalize_job(
                    job,
                    JobFinalization::Completed(FinalResult {
                        summary: Some(result.text.clone()),
                        code: None,
                        goal_reached: None,
                        value: Some(provider_execution::result_value(&result)),
                    }),
                )
                .await
            {
                Err(DatabaseError::Rejected(
                    violation @ Violation::UnsatisfiedDependency { .. },
                )) => {
                    let settled = database
                        .finalize_job(
                            job,
                            JobFinalization::Failed(FinalResult {
                                summary: Some(format!(
                                    "the Provider answered, but the Job cannot complete: {violation}"
                                )),
                                code: None,
                                goal_reached: Some(false),
                                value: Some(provider_execution::result_value(&result)),
                            }),
                        )
                        .await;
                    refused = Some(violation);
                    settled
                }
                settled => settled,
            };
            if let Some(error) = record_answer(&database, job, request, &result).await {
                gap.get_or_insert(error);
            }
            let final_output = match settled {
                Ok(final_output) => final_output,
                Err(source) => {
                    return Err(ExecutionRuntimeError::NotSettled {
                        source,
                        observations: gap.map(Box::new),
                    });
                }
            };
            let receipt = ExecutionReceipt {
                executor,
                input,
                job,
                result: final_output,
            };
            if let Some(source) = refused {
                return Err(ExecutionRuntimeError::CompletionRefused {
                    receipt: Box::new(receipt),
                    source,
                    observations: gap.map(Box::new),
                });
            }
            receipt
        }
        Err(error) => {
            match settle_provider_error(database, executor, input, job, request, error).await {
                Ok(receipt) => receipt,
                Err(ExecutionRuntimeError::ObservationsNotRecorded { receipt, source }) => {
                    gap.get_or_insert(source);
                    *receipt
                }
                Err(ExecutionRuntimeError::StopNotConfirmed {
                    receipt,
                    source,
                    observations,
                }) => {
                    return Err(ExecutionRuntimeError::StopNotConfirmed {
                        receipt,
                        source,
                        observations: gap.map(Box::new).or(observations),
                    });
                }
                Err(error) => return Err(error),
            }
        }
    };
    match gap {
        None => Ok(receipt),
        Some(source) => Err(ExecutionRuntimeError::ObservationsNotRecorded {
            receipt: Box::new(receipt),
            source,
        }),
    }
}

/// The sink of one Job's execution: streamed output goes to the Job's
/// outputs, and Token facts go to annotated Metrics records, each awaited
/// before the stream moves on.
pub(crate) struct DatabaseOutputSink {
    database: DatabaseHandle,
    job: JobId,
    /// The Provider request whose observations the sink records now.
    request: RequestNumber,
    /// The first observation that could not be written. The execution goes on
    /// regardless, see [`ProviderOutputSink::observe`].
    gap: Option<DatabaseError>,
}

pub(crate) fn output_sink(database: DatabaseHandle, job: JobId) -> DatabaseOutputSink {
    DatabaseOutputSink {
        database,
        job,
        request: RequestNumber::FIRST,
        gap: None,
    }
}

impl ProviderOutputSink for DatabaseOutputSink {
    fn append<'a>(
        &'a mut self,
        text: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), OutputSinkError>> + 'a>>
    {
        Box::pin(async move {
            self.database
                .append_job_output(
                    self.job,
                    NewJobOutput {
                        kind: IntermediateOutputKind::Progress,
                        summary: Some(text.to_owned()),
                        code: None,
                        goal_reached: None,
                        value: None,
                    },
                )
                .await
                .map(|_| ())
                .map_err(|error| OutputSinkError {
                    message: format!("database could not persist provider output: {error}"),
                })
        })
    }

    fn observe<'a>(
        &'a mut self,
        observation: ProviderObservation,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'a>> {
        Box::pin(async move {
            let (measurements, token) = match observation.fact {
                ProviderFact::NotDelivered => {
                    (Measurements::default(), TokenAnnotation::excluded())
                }
                ProviderFact::Delivered => (
                    Measurements::default(),
                    TokenAnnotation::eligible(UsageFinality::Cumulative),
                ),
                ProviderFact::Usage { usage, finality } => (
                    Measurements {
                        usage,
                        ..Measurements::default()
                    },
                    TokenAnnotation::eligible(finality),
                ),
            };
            let recorded = self
                .database
                .record_metrics(NewMetrics {
                    job: self.job,
                    request: self.request,
                    observed_at: observation.observed_at,
                    measurements,
                    token: Some(token),
                })
                .await;
            if let Err(error) = recorded {
                self.gap.get_or_insert(error);
            }
        })
    }
}

/// Settles a failed provider execution. Its Job outcome follows the error
/// alone; its availability observations record only the facts the error
/// carries, see [`provider_execution::error_observations`]. Each is
/// attempted, and the first that could not be written is reported. The
/// execution has returned, so an unknown outcome also has its local stop
/// confirmed, but only once that outcome is committed: a failed settlement
/// confirms nothing.
pub(crate) async fn settle_provider_error(
    database: DatabaseHandle,
    executor: ExecutorId,
    input: JobInputId,
    job: JobId,
    request: RequestNumber,
    error: ProviderExecutionError,
) -> Result<ExecutionReceipt, ExecutionRuntimeError> {
    let unknown = matches!(
        error,
        ProviderExecutionError::Unknown { .. }
            | ProviderExecutionError::Incomplete { .. }
            | ProviderExecutionError::Interrupted { .. }
            | ProviderExecutionError::Stopped { .. }
            | ProviderExecutionError::OutputPersistence { .. }
    );
    let result = if unknown {
        database.finalize_job(job, JobFinalization::Unknown).await?
    } else {
        database
            .finalize_job(
                job,
                JobFinalization::Failed(FinalResult {
                    summary: Some(error.to_string()),
                    code: None,
                    goal_reached: Some(false),
                    value: None,
                }),
            )
            .await?
    };
    let receipt = ExecutionReceipt {
        executor,
        input,
        job,
        result,
    };
    let stop = if unknown {
        confirm_stopped(&database, job).await.err()
    } else {
        None
    };
    let mut gap = None;
    let observed_at = Timestamp::now();
    for observation in provider_execution::error_observations(&error) {
        let recorded = database
            .record_availability_stat(NewAvailabilityStat {
                job,
                request,
                observed_at,
                observation,
            })
            .await;
        if let Err(error) = recorded {
            gap.get_or_insert(error);
        }
    }
    match (stop, gap) {
        (Some(source), observations) => Err(ExecutionRuntimeError::StopNotConfirmed {
            receipt: Box::new(receipt),
            source,
            observations: observations.map(Box::new),
        }),
        (None, Some(source)) => Err(ExecutionRuntimeError::ObservationsNotRecorded {
            receipt: Box::new(receipt),
            source,
        }),
        (None, None) => Ok(receipt),
    }
}
