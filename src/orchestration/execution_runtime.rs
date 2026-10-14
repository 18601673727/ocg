//! Bounded scheduler-to-executor handoff for the 0.5 execution plane.
//!
//! The scheduler owns admission to the executor queue. The executor owns the
//! execution plan after dequeueing it and is the only worker that calls the
//! database handle. Every worker boundary is a bounded `flume` channel; the
//! database handle itself has a separate bounded command channel.

use std::sync::{Arc, Mutex, PoisonError};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use ntex::util::{Either, select};

use crate::database::{DatabaseError, DatabaseHandle};
use crate::openai_compatible::{
    ChatConversation, ChatMessage, ChatToolCallWire, CompletedToolCall, InvalidToolCall,
    ToolCallRefusal,
};
use crate::orchestration::tools::{FailureCertainty, ToolOffer, ToolRegistryError};
use crate::persistence::{
    AppendedOutput, AuthorizationBasis, ExecutorId, FinalResult, IntermediateOutputKind,
    JobFinalization, JobId, JobInputDraft, JobInputId, JobOutcome, MAX_STORED_TOOL_ARGUMENTS_BYTES,
    MAX_STORED_TOOL_ARGUMENTS_PER_JOB_BYTES, Measurements, NewAvailabilityStat, NewJob,
    NewJobOutput, NewMetrics, NewToolEvidence, RequestNumber, Timestamp, ToolAdmission,
    ToolCallRef, ToolDecision, ToolOutcome, ToolOutcomeKind, ToolPayload, UncertainCause,
    Violation, WithheldReason,
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

type ExecutionReply = flume::Sender<Result<ExecutionReceipt, ExecutionRuntimeError>>;

enum Work {
    Plan(ExecutionPlan),
    Provider(ProviderExecutionPlan),
}

/// One submitted plan with its caller's reply.
struct Submission {
    work: Work,
    reply: ExecutionReply,
}

impl Submission {
    /// Answers a plan that will never start: nothing of it was persisted.
    fn refuse(self) {
        self.reply
            .send(Err(ExecutionRuntimeError::ShuttingDown))
            .ok();
    }
}

enum SchedulerMessage {
    Schedule(Box<Submission>),
    Shutdown,
}

enum ExecutorMessage {
    Execute(Box<Submission>),
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
        self.submit(Work::Plan(plan)).await
    }

    /// Submits a provider execution. Shutdown of the runtime cancels
    /// `plan.provider_config.cancellation` if the plan is executing then.
    pub async fn schedule_provider(
        &self,
        plan: ProviderExecutionPlan,
    ) -> Result<ExecutionReceipt, ExecutionRuntimeError> {
        self.submit(Work::Provider(plan)).await
    }

    /// Once shutdown has begun, a plan is refused here without being queued.
    /// The check and the queueing happen under the state lock, which never
    /// waits for queue space, so a plan is either queued ahead of the
    /// scheduler's last drain or refused.
    async fn submit(&self, work: Work) -> Result<ExecutionReceipt, ExecutionRuntimeError> {
        let (reply, response) = flume::bounded(1);
        {
            let state = lock(&self.state);
            if state.stopping {
                return Err(ExecutionRuntimeError::ShuttingDown);
            }
            if state.closed {
                return Err(ExecutionRuntimeError::SchedulerStopped);
            }
            #[cfg(test)]
            tests::admitted();
            self.sender
                .try_send(SchedulerMessage::Schedule(Box::new(Submission {
                    work,
                    reply,
                })))
                .map_err(|error| match error {
                    flume::TrySendError::Full(_) => ExecutionRuntimeError::SchedulerQueueFull,
                    flume::TrySendError::Disconnected(_) => ExecutionRuntimeError::SchedulerStopped,
                })?;
        }
        response
            .recv_async()
            .await
            .map_err(|_| ExecutionRuntimeError::SchedulerStopped)?
    }
}

/// Owns the scheduler and executor threads.
pub struct ExecutionRuntime {
    scheduler: SchedulerHandle,
    scheduler_thread: Option<JoinHandle<Result<(), ExecutionRuntimeError>>>,
    executor_sender: flume::Sender<ExecutorMessage>,
    executor_thread: Option<JoinHandle<Result<(), ExecutionRuntimeError>>>,
}

impl ExecutionRuntime {
    pub fn start(database: DatabaseHandle) -> Result<Self, ExecutionRuntimeError> {
        let (scheduler_sender, scheduler_receiver) = flume::bounded(SCHEDULER_QUEUE_CAPACITY);
        let (executor_sender, executor_receiver) = flume::bounded(EXECUTOR_QUEUE_CAPACITY);
        let state = SharedState::default();

        let executor_state = state.clone();
        let executor_thread = thread::Builder::new()
            .name("ocg-executor".to_string())
            .spawn(move || {
                ntex::rt::System::new("ocg-executor", ntex::rt::DefaultRuntime)
                    .block_on(executor_loop(database, executor_receiver, executor_state))
            })
            .map_err(ExecutionRuntimeError::Startup)?;

        let scheduler_executor_sender = executor_sender.clone();
        let scheduler_state = state.clone();
        let scheduler_thread = match thread::Builder::new()
            .name("ocg-scheduler".to_string())
            .spawn(move || {
                scheduler_loop(
                    scheduler_receiver,
                    scheduler_executor_sender,
                    scheduler_state,
                )
            }) {
            Ok(thread) => thread,
            Err(error) => {
                executor_sender.send(ExecutorMessage::Shutdown).ok();
                executor_thread
                    .join()
                    .map_err(|_| ExecutionRuntimeError::WorkerPanicked)??;
                return Err(ExecutionRuntimeError::Startup(error));
            }
        };

        Ok(Self {
            scheduler: SchedulerHandle {
                sender: scheduler_sender,
                state,
            },
            scheduler_thread: Some(scheduler_thread),
            executor_sender,
            executor_thread: Some(executor_thread),
        })
    }

    pub fn scheduler(&self) -> SchedulerHandle {
        self.scheduler.clone()
    }

    /// Stops the runtime without waiting for queued work. From the moment it
    /// is called, no plan starts: every plan still queued, or submitted
    /// later, is refused as [`ExecutionRuntimeError::ShuttingDown`] with
    /// nothing persisted. A provider execution in flight is cancelled and
    /// settles like any cancelled execution; a plain plan in flight
    /// finishes its few database writes. Returns once both workers have
    /// exited, so no worker is left holding a database call.
    pub fn shutdown(mut self) -> Result<(), ExecutionRuntimeError> {
        self.stop_and_join()
    }

    fn stop_and_join(&mut self) -> Result<(), ExecutionRuntimeError> {
        {
            let mut state = lock(&self.scheduler.state);
            state.stopping = true;
            if let Some(current) = &state.current {
                current.cancel();
            }
        }
        if self.scheduler_thread.is_some() {
            self.scheduler.sender.send(SchedulerMessage::Shutdown).ok();
        }
        let scheduler_result = join_worker(self.scheduler_thread.take());

        // The executor refuses whatever is still queued ahead of this marker
        // without starting it, so the wait for queue space is short.
        self.executor_sender.send(ExecutorMessage::Shutdown).ok();
        let executor_result = join_worker(self.executor_thread.take());

        match (scheduler_result, executor_result) {
            // The scheduler stops because the executor did; report the cause.
            (Err(ExecutionRuntimeError::ExecutorStopped), Err(error)) => Err(error),
            (Err(error), _) | (Ok(()), Err(error)) => Err(error),
            (Ok(()), Ok(())) => Ok(()),
        }
    }
}

fn join_worker(
    thread: Option<JoinHandle<Result<(), ExecutionRuntimeError>>>,
) -> Result<(), ExecutionRuntimeError> {
    let Some(thread) = thread else {
        return Ok(());
    };
    thread
        .join()
        .map_err(|_| ExecutionRuntimeError::WorkerPanicked)?
}

impl Drop for ExecutionRuntime {
    fn drop(&mut self) {
        let _ = self.stop_and_join();
    }
}

/// Hands plans to the executor until shutdown or until the executor stops.
/// Either way, it then closes admission and answers every plan still queued,
/// none of which started.
fn scheduler_loop(
    receiver: flume::Receiver<SchedulerMessage>,
    executor_sender: flume::Sender<ExecutorMessage>,
    state: SharedState,
) -> Result<(), ExecutionRuntimeError> {
    let result = schedule(&receiver, &executor_sender, &state);
    // Nothing is queued once this is set, so the drain below leaves nothing.
    lock(&state).closed = true;
    for message in receiver.drain() {
        if let SchedulerMessage::Schedule(submission) = message {
            match result {
                Ok(()) => submission.refuse(),
                Err(_) => {
                    submission
                        .reply
                        .send(Err(ExecutionRuntimeError::ExecutorStopped))
                        .ok();
                }
            }
        }
    }
    result
}

fn schedule(
    receiver: &flume::Receiver<SchedulerMessage>,
    executor_sender: &flume::Sender<ExecutorMessage>,
    state: &SharedState,
) -> Result<(), ExecutionRuntimeError> {
    while let Ok(message) = receiver.recv() {
        let submission = match message {
            SchedulerMessage::Schedule(submission) => submission,
            SchedulerMessage::Shutdown => break,
        };
        if lock(state).stopping {
            submission.refuse();
            continue;
        }
        match executor_sender.try_send(ExecutorMessage::Execute(submission)) {
            Ok(()) => {}
            Err(flume::TrySendError::Full(ExecutorMessage::Execute(submission))) => {
                submission
                    .reply
                    .send(Err(ExecutionRuntimeError::ExecutorQueueFull))
                    .ok();
            }
            Err(flume::TrySendError::Disconnected(ExecutorMessage::Execute(submission))) => {
                submission
                    .reply
                    .send(Err(ExecutionRuntimeError::ExecutorStopped))
                    .ok();
                return Err(ExecutionRuntimeError::ExecutorStopped);
            }
            Err(
                flume::TrySendError::Full(ExecutorMessage::Shutdown)
                | flume::TrySendError::Disconnected(ExecutorMessage::Shutdown),
            ) => {
                unreachable!("the scheduler sends only executions")
            }
        }
    }
    Ok(())
}

async fn executor_loop(
    database: DatabaseHandle,
    receiver: flume::Receiver<ExecutorMessage>,
    state: SharedState,
) -> Result<(), ExecutionRuntimeError> {
    while let Ok(message) = receiver.recv_async().await {
        let Submission { work, reply } = match message {
            ExecutorMessage::Execute(submission) => *submission,
            ExecutorMessage::Shutdown => break,
        };
        {
            let mut state = lock(&state);
            if state.stopping {
                drop(state);
                Submission { work, reply }.refuse();
                continue;
            }
            state.current = match &work {
                Work::Provider(plan) => Some(plan.provider_config.cancellation.clone()),
                Work::Plan(_) => None,
            };
        }
        let result = match work {
            Work::Plan(plan) => execute_plan(database.clone(), plan).await,
            Work::Provider(plan) => execute_provider_plan(database.clone(), plan).await,
        };
        lock(&state).current = None;
        reply.send(result).ok();
    }
    for message in receiver.drain() {
        if let ExecutorMessage::Execute(submission) = message {
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
    plan.job.assigned_executor = Some(executor);

    // An input left unsubmitted by a refused creation holds nothing.
    let input = database.save_job_input(None, plan.input).await?;
    let job = database
        .create_local_job_with_input(plan.job, input)
        .await?;
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

async fn execute_provider_plan(
    database: DatabaseHandle,
    mut plan: ProviderExecutionPlan,
) -> Result<ExecutionReceipt, ExecutionRuntimeError> {
    let executor = plan.executor_id;
    if database.executor(executor).await?.is_none() {
        return Err(ExecutionRuntimeError::ExecutorNotFound(executor));
    }
    plan.job.assigned_executor = Some(executor);
    // The limits are checked, and the offer read from the registry, before
    // anything is persisted.
    if let Some(tools) = &plan.tools {
        tools
            .limits
            .validate()
            .map_err(ExecutionRuntimeError::InvalidToolLimits)?;
    }
    let offered = plan
        .tools
        .as_ref()
        .map(|tools| tools.offer.wires())
        .transpose()
        .map_err(ExecutionRuntimeError::ToolsUnavailable)?;
    let input = database.save_job_input(None, plan.input).await?;
    let job = database
        .create_local_job_with_input(plan.job, input)
        .await?;

    let configuration = match database.job_configuration(job).await? {
        Some(configuration) => configuration,
        None => {
            let result = database
                .finalize_job(
                    job,
                    JobFinalization::Failed(FinalResult {
                        summary: Some(
                            "provider execution requires frozen LLM configuration".to_owned(),
                        ),
                        code: None,
                        goal_reached: Some(false),
                        value: None,
                    }),
                )
                .await?;
            return Ok(ExecutionReceipt {
                executor,
                input,
                job,
                result,
            });
        }
    };
    let input_record = match database.job_input(input).await? {
        Some(input) => input,
        None => {
            let result = database
                .finalize_job(
                    job,
                    JobFinalization::Failed(FinalResult {
                        summary: Some("submitted Job input disappeared".to_owned()),
                        code: None,
                        goal_reached: Some(false),
                        value: None,
                    }),
                )
                .await?;
            return Ok(ExecutionReceipt {
                executor,
                input,
                job,
                result,
            });
        }
    };
    let mut conversation = provider_execution::objective_conversation(
        &configuration.frozen,
        input_record.objective.as_deref(),
    );
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
            (Err(stopped), _) => return stop_tool_loop(run, sink, Some(&result), stopped).await,
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
}

impl From<ToolLoopStop> for Stopped {
    fn from(stop: ToolLoopStop) -> Self {
        Self {
            stop,
            evidence: None,
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
        let admission = NewToolEvidence {
            call: reference,
            tool_call_id: call.id.clone(),
            name: call.name.clone(),
            decision: ToolDecision::Admitted(ToolAdmission {
                handler_version: dispatch.version,
                effect: dispatch.effect,
                parameters: dispatch.schema,
                arguments: ToolPayload::new(call.arguments.clone()),
                authorization: AuthorizationBasis::RegistryGrant,
            }),
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
                });
            }
        }
        budget.calls += 1;
        budget.argument_bytes = budget.argument_bytes.saturating_add(call.arguments.len());
        #[cfg(test)]
        dispatch_seam::reach(dispatch_seam::Point::Admitted, reference);
        let dispatch = match tools.offer.resolve(call) {
            Ok(dispatch) => dispatch,
            Err(refusal) => {
                let outcome = ToolOutcome::NotInvoked {
                    reason: refusal.kind,
                };
                return Err(
                    conclude(site, reference, outcome, ToolLoopStop::Refused(refusal)).await,
                );
            }
        };
        let ran = select(
            cancellation.cancelled(),
            ntex::time::timeout(
                limits.handler_timeout,
                dispatch.handler.call(dispatch.arguments),
            ),
        )
        .await;
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
                            });
                        }
                        budget.result_bytes = total;
                        answers.push(content);
                        continue;
                    }
                }
            },
        };
        return Err(conclude(site, reference, outcome, stop).await);
    }
    Ok(answers)
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
        })
        .await;
    Stopped {
        stop: ToolLoopStop::Refused(refusal),
        evidence: recorded.err(),
    }
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
    match site.database.record_tool_outcome(call, outcome).await {
        Ok(()) => stop.into(),
        Err(error) if stop.is_unknown() => Stopped {
            stop,
            evidence: Some(error),
        },
        Err(error) => Stopped {
            stop: ToolLoopStop::EvidenceNotRecorded {
                index: call.index,
                outcome: Some(kind),
            },
            evidence: Some(error),
        },
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
    let Stopped { stop, evidence } = stopped;
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
        confirm_stopped(&database, job).await.err().map(Box::new)
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

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::time::Instant;

    use super::*;
    use crate::persistence::ExecutionMode;
    use crate::test_support::{WAIT, wait};

    thread_local! {
        /// Runs on a submitting thread once its plan has passed the shutdown
        /// check, just before the plan is queued.
        static ADMITTED: RefCell<Option<Box<dyn FnOnce()>>> = const { RefCell::new(None) };
    }

    pub(super) fn admitted() {
        if let Some(hook) = ADMITTED.with(|hook| hook.borrow_mut().take()) {
            hook();
        }
    }

    fn on_admission(hook: impl FnOnce() + 'static) {
        ADMITTED.with(|admitted| *admitted.borrow_mut() = Some(Box::new(hook)));
    }

    /// Waits for `condition`, which is checked rather than timed, within [`WAIT`].
    fn until(mut condition: impl FnMut() -> bool) {
        let deadline = Instant::now() + WAIT;
        while !condition() {
            assert!(Instant::now() < deadline, "not reached within {WAIT:?}");
            thread::yield_now();
        }
    }

    /// Joins a thread that must finish within [`WAIT`].
    fn finished<T>(thread: JoinHandle<T>) -> T {
        until(|| thread.is_finished());
        thread.join().expect("the thread did not panic")
    }

    /// A plan that is never executed by these tests.
    fn plan() -> ExecutionPlan {
        ExecutionPlan {
            executor_id: ExecutorId::new(1),
            input: JobInputDraft::default(),
            job: NewJob {
                name: None,
                execution_mode: ExecutionMode::Once,
                eligible_at: None,
                scheduled_at: None,
                next_due_at: None,
                next_iteration_no: None,
                iteration_no: None,
                parent: JobId::new(1),
                projects: Vec::new(),
                dependencies: Vec::new(),
                configuration: None,
                assigned_executor: None,
                previous: None,
                replaces: None,
            },
            outcome: JobOutcome::Completed,
            result: FinalResult {
                summary: None,
                code: None,
                goal_reached: None,
                value: None,
            },
        }
    }

    fn queues() -> (
        SchedulerHandle,
        flume::Receiver<SchedulerMessage>,
        flume::Sender<ExecutorMessage>,
        flume::Receiver<ExecutorMessage>,
    ) {
        let (sender, receiver) = flume::bounded(SCHEDULER_QUEUE_CAPACITY);
        let (executor_sender, executor_receiver) = flume::bounded(EXECUTOR_QUEUE_CAPACITY);
        let scheduler = SchedulerHandle {
            sender,
            state: SharedState::default(),
        };
        (scheduler, receiver, executor_sender, executor_receiver)
    }

    /// The real scheduler, started only once shutdown has begun, so that
    /// every plan it finds was queued before and none is handed on.
    fn scheduler_after_stopping(
        receiver: flume::Receiver<SchedulerMessage>,
        executor_sender: flume::Sender<ExecutorMessage>,
        state: SharedState,
    ) -> JoinHandle<Result<(), ExecutionRuntimeError>> {
        thread::spawn(move || {
            until(|| lock(&state).stopping);
            scheduler_loop(receiver, executor_sender, state)
        })
    }

    /// Stands in for the executor: it answers what reaches it without
    /// running it, and stops at the shutdown marker.
    fn answering_executor(
        receiver: flume::Receiver<ExecutorMessage>,
    ) -> JoinHandle<Result<(), ExecutionRuntimeError>> {
        thread::spawn(move || {
            while let Ok(message) = receiver.recv() {
                match message {
                    ExecutorMessage::Execute(submission) => {
                        submission
                            .reply
                            .send(Err(ExecutionRuntimeError::Provider(
                                "the plan reached the executor".to_owned(),
                            )))
                            .ok();
                    }
                    ExecutorMessage::Shutdown => break,
                }
            }
            Ok(())
        })
    }

    #[test]
    fn a_plan_queued_as_shutdown_begins_is_refused_not_stranded() {
        let (scheduler, receiver, executor_sender, executor_receiver) = queues();
        // Keeps the queue connected after the scheduler's last drain, as it
        // is until the scheduler's receiver is dropped.
        let _connected = receiver.clone();
        let state = scheduler.state.clone();
        let runtime = ExecutionRuntime {
            scheduler: scheduler.clone(),
            scheduler_thread: Some(scheduler_after_stopping(
                receiver,
                executor_sender.clone(),
                state.clone(),
            )),
            executor_sender,
            executor_thread: Some(answering_executor(executor_receiver)),
        };
        let (stopper_sender, stopper) = flume::bounded(1);
        on_admission(move || {
            let (ran_sender, ran) = flume::bounded(1);
            let stopping = thread::spawn(move || {
                // Unless the check and the queueing are one step, shutdown
                // runs to its end between them.
                if state.try_lock().is_ok() {
                    let stopped = runtime.shutdown();
                    ran_sender.send(()).ok();
                    stopped
                } else {
                    ran_sender.send(()).ok();
                    runtime.shutdown()
                }
            });
            ran.recv_timeout(WAIT)
                .expect("shutdown either ran or waits for the plan to be queued");
            stopper_sender.send(stopping).ok();
        });

        let refused = wait(scheduler.schedule(plan()));
        assert!(
            matches!(refused, Err(ExecutionRuntimeError::ShuttingDown)),
            "a plan admitted before shutdown began is refused unstarted, got {refused:?}"
        );
        let stopping = stopper
            .try_recv()
            .expect("the plan passed the shutdown check");
        finished(stopping).expect("shutdown");
        assert!(
            matches!(
                wait(scheduler.schedule(plan())),
                Err(ExecutionRuntimeError::ShuttingDown)
            ),
            "a long-lived handle refuses once shutdown began"
        );
    }

    #[test]
    fn a_full_queue_refuses_at_once_and_shutdown_answers_every_queued_plan() {
        let (scheduler, receiver, executor_sender, executor_receiver) = queues();
        let state = scheduler.state.clone();
        let runtime = ExecutionRuntime {
            scheduler: scheduler.clone(),
            scheduler_thread: Some(scheduler_after_stopping(
                receiver,
                executor_sender.clone(),
                state.clone(),
            )),
            executor_sender,
            executor_thread: Some(answering_executor(executor_receiver)),
        };
        let submitters: Vec<_> = (0..SCHEDULER_QUEUE_CAPACITY)
            .map(|_| {
                let scheduler = scheduler.clone();
                thread::spawn(move || wait(scheduler.schedule(plan())))
            })
            .collect();
        until(|| scheduler.sender.len() == SCHEDULER_QUEUE_CAPACITY);
        assert!(matches!(
            wait(scheduler.schedule(plan())),
            Err(ExecutionRuntimeError::SchedulerQueueFull)
        ));

        let stopping = thread::spawn(move || runtime.shutdown());
        until(|| lock(&state).stopping);
        assert!(
            matches!(
                wait(scheduler.schedule(plan())),
                Err(ExecutionRuntimeError::ShuttingDown)
            ),
            "a plan submitted once shutdown began is refused"
        );
        finished(stopping).expect("shutdown");
        for submitter in submitters {
            let refused = finished(submitter);
            assert!(
                matches!(refused, Err(ExecutionRuntimeError::ShuttingDown)),
                "every queued plan is answered unstarted, got {refused:?}"
            );
        }
        assert!(scheduler.sender.is_empty());
    }

    #[test]
    fn plans_queued_when_the_executor_is_gone_are_answered_and_its_failure_reported() {
        let (scheduler, receiver, executor_sender, executor_receiver) = queues();
        // The executor failed before any plan reached it.
        drop(executor_receiver);
        let executor_thread = thread::spawn(|| -> Result<(), ExecutionRuntimeError> {
            std::panic::resume_unwind(Box::new("the executor failed"))
        });
        let submitters: Vec<_> = (0..3)
            .map(|_| {
                let scheduler = scheduler.clone();
                thread::spawn(move || wait(scheduler.schedule(plan())))
            })
            .collect();
        until(|| scheduler.sender.len() == 3);
        let state = scheduler.state.clone();
        let scheduler_executor_sender = executor_sender.clone();
        let runtime = ExecutionRuntime {
            scheduler: scheduler.clone(),
            scheduler_thread: Some(thread::spawn(move || {
                scheduler_loop(receiver, scheduler_executor_sender, state)
            })),
            executor_sender,
            executor_thread: Some(executor_thread),
        };

        for submitter in submitters {
            let answer = finished(submitter);
            assert!(
                matches!(answer, Err(ExecutionRuntimeError::ExecutorStopped)),
                "a queued plan is answered while handles live on, got {answer:?}"
            );
        }
        assert!(
            matches!(
                wait(scheduler.schedule(plan())),
                Err(ExecutionRuntimeError::SchedulerStopped)
            ),
            "a stopped scheduler refuses at once"
        );
        let stopped = runtime.shutdown();
        assert!(
            matches!(stopped, Err(ExecutionRuntimeError::WorkerPanicked)),
            "the executor's failure is reported, not its consequence: {stopped:?}"
        );
    }
}
