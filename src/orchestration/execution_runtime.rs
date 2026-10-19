use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use ntex::util::{Either, select};

use crate::attachments::AttachmentError;
use crate::database::{CommandOutcome, DatabaseError, DatabaseHandle};
use crate::openai_compatible::image::{
    MAX_IMAGE_BYTES, MAX_REQUEST_IMAGE_BYTES, MAX_REQUEST_IMAGES,
};
use crate::openai_compatible::{
    ChatConversation, ChatMessage, ChatRole, ChatToolCallWire, CompletedToolCall, ContentPartWire,
    ImageError, ImageInput, ImageMime, InvalidToolCall, MessageContentWire, ToolCallRefusal,
    ToolWire,
};
use crate::orchestration::blocking;
use crate::orchestration::tools::{
    FailureCertainty, ToolFailure, ToolFuture, ToolOffer, ToolRegistryError, ToolStart,
};
use crate::persistence::{
    AppendedOutput, AttachmentId, AuthorizationBasis, ExecutorId, FinalResult,
    FrozenExecutionConfig, IntermediateOutputKind, JobFinalization, JobId, JobInputDraft,
    JobInputId, JobOutcome, MAX_STORED_TOOL_ARGUMENTS_BYTES,
    MAX_STORED_TOOL_ARGUMENTS_PER_JOB_BYTES, Measurements, NewAvailabilityStat, NewJob,
    NewJobOutput, NewMetrics, NewSubmission, NewToolEvidence, NotInvokedReason, RequestNumber,
    Timestamp, ToolAdmission, ToolCallRef, ToolDecision, ToolOutcome, ToolOutcomeKind,
    ToolPayload, UncertainCause, Violation, WithheldReason,
};
use crate::provider_execution::{
    self, CancellationToken, CredentialResolver, EnvironmentCredentialResolver, OutputSinkError,
    ProviderExecutionConfig, ProviderExecutionError, ProviderExecutionResult, ProviderFact,
    ProviderObservation, ProviderOutputSink,
};
use crate::token_usage::{TokenAnnotation, UsageFinality};

/// Work admitted as a pending Job and finalized by a task after a committed
/// claim batch grants it capacity.
#[derive(Debug)]
pub struct ExecutionPlan {
    /// A registered Pool named by the submitter. The claim batch chooses
    /// the actual compatible Pool from those with available capacity.
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
    /// Provider rounds one Job may send, its first included; retries do not
    /// consume rounds. Tools requested in the last round are not run.
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
            max_provider_requests: 64,
            max_tool_calls: 256,
            max_tool_result_bytes: 16 * 1024,
            max_tool_results_bytes: 1024 * 1024,
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
    /// The conversation would exceed the model's context window.
    ContextWindow,
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
            Self::ContextWindow => {
                formatter.write_str("the conversation exceeds the model's context window")
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
    /// `None` at admission, or for an unknown outcome that publishes no result.
    pub result: Option<AppendedOutput>,
}

#[derive(Debug, thiserror::Error)]
pub enum ExecutionRuntimeError {
    #[error("the frozen Workspace tool grant is unsupported on this host")]
    WorkspaceGrantUnsupported,
    #[error("scheduler worker stopped")]
    SchedulerStopped,
    #[error("scheduler admission queue is full")]
    SchedulerQueueFull,
    #[error("claimed Job {0} has no frozen pool binding")]
    MissingPoolBinding(JobId),
    #[error("claimed Job {0} has no execution plan in this opening")]
    MissingExecutionPlan(JobId),
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
    #[error("the tool loop stopped: {stop}")]
    ToolLoopStopped {
        receipt: Box<ExecutionReceipt>,
        stop: ToolLoopStop,
        observations: Option<Box<DatabaseError>>,
        evidence: Option<Box<DatabaseError>>,
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
    /// later start of the database; see [`DatabaseHandle::abandoned_at_open`].
    /// What the Provider was observed to do is still recorded, apart from
    /// `observations`, the first observation that could not be written.
    #[error("the Provider answered but the Job was not settled: {source}")]
    NotSettled {
        source: DatabaseError,
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
    /// T1 was attempted but its commit is unknown. A pending Job may exist;
    /// reconcile the operation or submission before resubmitting it.
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

/// The committed identity of an admitted provider plan.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct StartedJob {
    executor: ExecutorId,
    input: JobInputId,
    job: JobId,
}

type ExecutionReply = flume::Sender<Result<ExecutionReceipt, ExecutionRuntimeError>>;

/// Admission reports the committed identity; settlement reports the result
/// after dispatch and execution, or cancellation while still pending.
struct ProviderReply {
    started: flume::Sender<Result<StartedJob, AdmissionError>>,
    settled: ExecutionReply,
}

/// One submitted plan with its caller's reply. A provider plan may carry
/// the submission to record with its Job.
enum Submission {
    Plan(ExecutionPlan, ExecutionReply),
    Provider(
        ProviderExecutionPlan,
        Option<NewSubmission>,
        Option<WorkspaceTools>,
        ProviderReply,
    ),
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
            Self::Provider(_, _, _, reply) => {
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
    Claim(flume::Sender<Result<super::scheduler::DispatchBatch<JobId>, DatabaseError>>),
    Grant(JobId),
    Stop(JobId),
    Finished(JobId),
    Reconcile,
}

#[derive(Default)]
struct RuntimeState {
    stopping: bool,
    closed: bool,
    current: BTreeMap<JobId, CancellationToken>,
}

struct PendingExecution {
    started: StartedJob,
    execution: PendingPlan,
    cancellation: CancellationToken,
    done: CancellationToken,
    reply: Option<ExecutionReply>,
}

enum PendingPlan {
    Plan(JobOutcome, FinalResult),
    Provider(ProviderTask, Option<Vec<ToolWire>>),
}

#[derive(Clone, Debug, Default)]
pub(crate) struct WorkspaceTools {
    #[cfg(unix)]
    workspace: Option<crate::orchestration::workspace_read::Workspace>,
}

impl WorkspaceTools {
    pub(crate) fn root(&self) -> Option<&std::path::Path> {
        #[cfg(unix)]
        {
            self.workspace.as_ref().map(|workspace| workspace.root())
        }
        #[cfg(not(unix))]
        {
            None
        }
    }

    pub(crate) fn from_environment(data_directory: &crate::data_directory::DataDirectory) -> Self {
        #[cfg(unix)]
        {
            use crate::orchestration::workspace_read::{WORKSPACE_ROOT_VARIABLE, Workspace};
            let workspace = std::env::var_os(WORKSPACE_ROOT_VARIABLE)
                .filter(|root| !root.is_empty())
                .and_then(|root| {
                    match Workspace::authorize(std::path::Path::new(&root), data_directory) {
                        Ok(workspace) => Some(workspace),
                        Err(error) => {
                            eprintln!("OCG Workspace is not configured: {error}");
                            None
                        }
                    }
                });
            Self { workspace }
        }
        #[cfg(not(unix))]
        {
            let _ = data_directory;
            Self::default()
        }
    }

    fn available(&self) -> bool {
        #[cfg(unix)]
        {
            self.workspace.is_some()
        }
        #[cfg(not(unix))]
        {
            false
        }
    }

    fn build(&self, grant: Option<&str>) -> Result<Option<ToolExecution>, ExecutionRuntimeError> {
        let Some(grant) = grant else {
            return Ok(None);
        };
        #[cfg(unix)]
        {
            use crate::orchestration::workspace_read;
            let workspace = self.workspace.clone().ok_or({
                ExecutionRuntimeError::Database(DatabaseError::Rejected(
                    Violation::WorkspaceNotConfigured,
                ))
            })?;
            let tools = match grant {
                "read" => workspace_read::read_file_tools(workspace),
                #[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
                "edit" => crate::orchestration::workspace_write::edit_file_tools(workspace),
                #[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
                "code" => crate::orchestration::workspace_command::code_tools(workspace),
                _ => return Err(ExecutionRuntimeError::WorkspaceGrantUnsupported),
            }
            .map_err(ExecutionRuntimeError::ToolsUnavailable)?;
            Ok(Some(tools))
        }
        #[cfg(not(unix))]
        {
            let _ = grant;
            Err(ExecutionRuntimeError::WorkspaceGrantUnsupported)
        }
    }
}

struct ProviderTask {
    workspace_tools: Option<WorkspaceTools>,
    credential_resolver: Arc<dyn CredentialResolver>,
    provider_config: ProviderExecutionConfig,
    tools: Option<ToolExecution>,
}

#[cfg(all(test, unix))]
mod grounding_tests {
    use super::*;
    use crate::database::DatabaseWorker;
    use crate::orchestration::workspace_read::Workspace;
    use crate::persistence::{AgentBehavior, AgentConfig, ConfigBody, NewConfig, NewExecutor};
    use crate::test_support::{
        data_directory, execution_configuration, on_system, temporary_directory, wait,
    };

    #[test]
    fn frozen_workspace_grants_ground_first_and_continued_chat_turns() {
        let (_data, directory) = data_directory("chat", "grounding");
        let (_root, root) = temporary_directory("chat", "grounding-workspace");
        let workspace = Workspace::authorize(&root, &directory).expect("authorize");
        let workspace_tools = WorkspaceTools {
            workspace: Some(workspace),
        };
        let worker = DatabaseWorker::start(&directory).expect("start");
        let handle = worker.handle();
        let mut configuration = execution_configuration(&handle);
        let model = wait(handle.config(configuration.llm_profile.into()))
            .expect("read")
            .expect("model");
        let ConfigBody::Model(model) = model.body else {
            panic!("model")
        };
        let endpoint = "https://chat-grounding.invalid/v1";
        wait(handle.update_config(
            model.connection_id,
            NewConfig {
                name: "grounding provider".into(),
                body: ConfigBody::Connection(crate::persistence::ConnectionConfig {
                    protocol: crate::persistence::LlmTransport::OpenAiChatCompletions,
                    endpoint: Some(endpoint.into()),
                    credential_ref: None,
                }),
            },
        ))
        .expect("provider endpoint");
        let agent_body = AgentConfig {
            instructions: Some("Agent instructions".into()),
            behavior: AgentBehavior::Chat,
            models: vec![configuration.llm_profile.into()],
            tool_grant: Some("read".into()),
            ..Default::default()
        };
        let agent = wait(handle.create_config(NewConfig {
            name: "chat".into(),
            body: ConfigBody::Agent(agent_body.clone()),
        }))
        .expect("agent");
        configuration.agent_profile = Some(agent.into());
        let executor = wait(handle.create_executor(NewExecutor {
            name: "local".into(),
            concurrency_limit: std::num::NonZeroU32::MIN,
            llm_transports: [crate::persistence::LlmTransport::OpenAiChatCompletions].into(),
        }))
        .expect("executor");
        let mut previous = None;
        for (index, granted) in [true, true, false].into_iter().enumerate() {
            let job = wait(handle.create_claimed_fixture(
                NewJob {
                    name: None,
                    eligible_at: None,
                    parent: handle.genesis(),
                    projects: vec![],
                    dependencies: vec![],
                    input: JobInputDraft {
                        objective: Some("objective".into()),
                        ..Default::default()
                    },
                    configuration: Some(configuration),
                    previous,
                },
                executor,
            ))
            .expect("claim");
            // Change the live grant after T1; execution must use the frozen one.
            let mut edited = agent_body.clone();
            edited.tool_grant = if granted { None } else { Some("read".into()) };
            wait(handle.update_config(
                agent,
                NewConfig {
                    name: "chat".into(),
                    body: ConfigBody::Agent(edited),
                },
            ))
            .expect("edit live agent");
            let tools = workspace_tools.clone();
            let run_handle = handle.clone();
            let expected_root = tools.root().expect("root").to_string_lossy().into_owned();
            provider_seam::install(endpoint, move |conversation| {
                let sent = serde_json::to_value(conversation).expect("json");
                let messages = sent["messages"].as_array().expect("messages");
                if granted {
                    assert_eq!(messages[0]["role"], "system");
                    let text = messages[0]["content"].as_str().expect("grounding");
                    assert!(text.contains(&expected_root));
                    assert!(text.contains("Every tool path is relative to this root"));
                    assert!(text.contains("cannot be reached"));
                    assert!(text.contains("instead of claiming you acted there"));
                    assert_eq!(messages[1]["content"], "Agent instructions");
                    assert!(!conversation.tools.is_empty());
                } else {
                    assert_eq!(messages[0]["content"], "Agent instructions");
                    assert!(conversation.tools.is_empty());
                }
                provider_seam::Response {
                    gate: None,
                    steps: vec![crate::provider_execution::scripted::Step::Chunk(
                        "data: {\"choices\":[{\"delta\":{\"content\":\"answer\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n",
                    )],
                }
            });
            on_system(move || async move {
                run_started_provider_job(
                    run_handle,
                    ProviderTask {
                        workspace_tools: Some(tools),
                        tools: None,
                        credential_resolver: Arc::new(EnvironmentCredentialResolver),
                        provider_config: ProviderExecutionConfig::default(),
                    },
                    (
                        StartedJob {
                            executor,
                            input: JobInputId::new(job.get()),
                            job,
                        },
                        None,
                    ),
                    &Cell::new(false),
                )
                .await
            })
            .expect("settled");
            previous = if index == 0 { Some(job) } else { None };
            wait(handle.update_config(
                agent,
                NewConfig {
                    name: "chat".into(),
                    body: ConfigBody::Agent(AgentConfig {
                        tool_grant: if index == 0 {
                            Some("read".into())
                        } else {
                            None
                        },
                        ..agent_body.clone()
                    }),
                },
            ))
            .expect("configure next turn");
        }
        worker.shutdown().expect("shutdown");
    }
}

impl From<ProviderExecutionPlan> for ProviderTask {
    fn from(plan: ProviderExecutionPlan) -> Self {
        Self {
            workspace_tools: None,
            credential_resolver: plan.credential_resolver,
            provider_config: plan.provider_config,
            tools: plan.tools,
        }
    }
}

type SharedState = Arc<Mutex<RuntimeState>>;

fn lock(state: &SharedState) -> std::sync::MutexGuard<'_, RuntimeState> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A cloneable endpoint for scheduler submissions.
#[derive(Clone)]
pub struct SchedulerHandle {
    sender: flume::Sender<Box<Submission>>,
    state: SharedState,
}

impl SchedulerHandle {
    /// Creates a pending Job through T1 and returns after that commit. Its
    /// receipt has no result yet; dispatch and finalization run separately.
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
    /// been committed as pending with its input, before any claim or Provider
    /// request. The runtime then owns the execution: it dispatches and settles
    /// the Job, whatever becomes of the returned [`AdmittedJob`]; dropping
    /// it, or the future of its settlement, neither stops nor cancels the
    /// Job. Its stop is requested through a cancellation of its own, a
    /// child of the plan's token: cancelling that token, or shutdown of the
    /// runtime, stops it too, but its stop reaches no other execution,
    /// whatever tokens the plans share.
    ///
    /// The pending cap, a stopped scheduler, shutdown, an unregistered
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
        self.admit(plan, None, None).await
    }

    /// Like [`Self::admit_provider`], and records `submission` with the
    /// plan's Job, in its T1 transaction, so the
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
        self.admit(plan, Some(submission), None).await
    }

    pub(crate) async fn admit_workspace_provider(
        &self,
        plan: ProviderExecutionPlan,
        submission: NewSubmission,
        workspace_tools: WorkspaceTools,
    ) -> Result<AdmittedJob, AdmissionError> {
        self.admit(plan, Some(submission), Some(workspace_tools))
            .await
    }

    async fn admit(
        &self,
        mut plan: ProviderExecutionPlan,
        submission: Option<NewSubmission>,
        workspace_tools: Option<WorkspaceTools>,
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
            workspace_tools,
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
            .try_send(Box::new(submission))
            .map_err(|error| match error {
                flume::TrySendError::Full(_) => ExecutionRuntimeError::SchedulerQueueFull,
                flume::TrySendError::Disconnected(_) => ExecutionRuntimeError::SchedulerStopped,
            })
    }
}

/// A provider plan's committed Job, initially pending. Its identity is the
/// one the execution,
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

    /// The submitter's registered Pool; the settlement receipt names the
    /// Pool actually selected by the claim batch.
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

/// Owns admission, the dispatcher, and every granted Job task.
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

        let wake = database
            .take_scheduler_wake()
            .ok_or(ExecutionRuntimeError::SchedulerStopped)?;
        let (sender, receiver) = flume::unbounded();
        let (admission, admissions) = flume::bounded(64);
        let state = SharedState::default();
        let shutdown = CancellationToken::default();
        let finished = ntex::rt::spawn(dispatch_runtime(
            database,
            wake,
            sender.clone(),
            receiver,
            admissions,
            state.clone(),
            shutdown.clone(),
        ));
        Ok(Self {
            scheduler: SchedulerHandle {
                sender: admission,
                state,
            },
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
        for cancellation in state.current.values() {
            cancellation.cancel();
        }
        self.shutdown.cancel();
    }
}

impl Drop for ExecutionRuntime {
    fn drop(&mut self) {
        self.request_stop();
    }
}

async fn dispatch_runtime(
    database: DatabaseHandle,
    wake: crate::database::SchedulerWakeSubscription,
    sender: flume::Sender<SchedulerMessage>,
    receiver: flume::Receiver<SchedulerMessage>,
    admissions: flume::Receiver<Box<Submission>>,
    state: SharedState,
    shutdown: CancellationToken,
) -> Result<(), ExecutionRuntimeError> {
    let dispatcher = ntex::rt::spawn({
        let sender = sender.clone();
        let database = database.clone();
        let shutdown = shutdown.clone();
        async move {
            super::scheduler::Scheduler::new(&database, wake, shutdown)
                .run(
                    || {
                        let sender = sender.clone();
                        async move {
                            let (reply, response) = flume::bounded(1);
                            sender
                                .send(SchedulerMessage::Claim(reply))
                                .map_err(|_| DatabaseError::ShuttingDown)?;
                            response
                                .recv_async()
                                .await
                                .map_err(|_| DatabaseError::ShuttingDown)?
                        }
                    },
                    |job| {
                        sender.send(SchedulerMessage::Grant(job)).ok();
                    },
                )
                .await;
        }
    });
    let mut pending = BTreeMap::new();
    let mut running = BTreeMap::new();
    let mut reconcile_at = Instant::now() + Duration::from_secs(30);
    let mut unconfirmed_stopped = BTreeSet::new();
    loop {
        let incoming = async {
            if Instant::now() >= reconcile_at {
                return Ok(SchedulerMessage::Reconcile);
            }
            match select(
                receiver.recv_async(),
                select(
                    admissions.recv_async(),
                    ntex::time::sleep(reconcile_at.saturating_duration_since(Instant::now())),
                ),
            )
            .await
            {
                Either::Left(message) => message,
                Either::Right(Either::Left(submission)) => {
                    submission.map(SchedulerMessage::Schedule)
                }
                Either::Right(Either::Right(())) => Ok(SchedulerMessage::Reconcile),
            }
        };
        let Either::Right(Ok(message)) = select(shutdown.cancelled(), incoming).await else {
            break;
        };
        match message {
            SchedulerMessage::Schedule(submission) => {
                if lock(&state).stopping {
                    submission.refuse();
                    continue;
                }
                let admitted = match *submission {
                    Submission::Plan(mut plan, reply) => {
                        let created = async {
                            if database.executor(plan.executor_id).await?.is_none() {
                                return Err(ExecutionRuntimeError::ExecutorNotFound(
                                    plan.executor_id,
                                ));
                            }
                            plan.job.input = std::mem::take(&mut plan.input);
                            Ok(database.create_job(plan.job.clone()).await?)
                        }
                        .await;
                        match created {
                            Ok(job) => {
                                let started = StartedJob {
                                    job,
                                    input: JobInputId::new(job.get()),
                                    executor: plan.executor_id,
                                };
                                reply
                                    .send(Ok(ExecutionReceipt {
                                        job,
                                        input: started.input,
                                        executor: started.executor,
                                        result: None,
                                    }))
                                    .ok();
                                Some(PendingExecution {
                                    started,
                                    execution: PendingPlan::Plan(plan.outcome, plan.result),
                                    reply: None,
                                    cancellation: CancellationToken::default(),
                                    done: CancellationToken::default(),
                                })
                            }
                            Err(error) => {
                                reply.send(Err(error)).ok();
                                None
                            }
                        }
                    }
                    Submission::Provider(mut plan, submission, workspace_tools, reply) => {
                        match admit_provider_job(
                            &database,
                            &mut plan,
                            submission,
                            workspace_tools.as_ref(),
                        )
                        .await
                        {
                            Ok((started, offered)) => {
                                let cancellation = plan.provider_config.cancellation.clone();
                                reply.started.send(Ok(started)).ok();
                                let mut task = ProviderTask::from(plan);
                                task.workspace_tools = workspace_tools;
                                Some(PendingExecution {
                                    started,
                                    execution: PendingPlan::Provider(task, offered),
                                    reply: Some(reply.settled),
                                    cancellation,
                                    done: CancellationToken::default(),
                                })
                            }
                            Err(error) => {
                                reply.started.send(Err(error)).ok();
                                None
                            }
                        }
                    }
                };
                if let Some(admitted) = admitted {
                    let job = admitted.started.job;
                    {
                        let mut state = lock(&state);
                        if state.stopping {
                            admitted.cancellation.cancel();
                        }
                        state.current.insert(job, admitted.cancellation.clone());
                    }
                    ntex::rt::spawn({
                        let sender = sender.clone();
                        let cancellation = admitted.cancellation.clone();
                        let done = admitted.done.clone();
                        async move {
                            if let Either::Left(()) =
                                select(cancellation.cancelled(), done.cancelled()).await
                            {
                                sender.send(SchedulerMessage::Stop(job)).ok();
                            }
                        }
                    });
                    pending.insert(job, admitted);
                }
            }
            SchedulerMessage::Reconcile => {
                reconcile_at = Instant::now() + Duration::from_secs(30);
                match ntex::rt::spawn_blocking(blocking::retry_unproven_groups).await {
                    Ok(recovered) => unconfirmed_stopped.extend(recovered),
                    Err(error) => {
                        log::warn!("failed to retry unproven groups on blocking worker: {error}");
                    }
                }
                let mut retry_next = BTreeSet::new();
                for job in std::mem::take(&mut unconfirmed_stopped) {
                    match database.confirm_job_stopped(job).await {
                        Ok(())
                        | Err(DatabaseError::Rejected(Violation::StopAlreadyConfirmed(_))) => {}
                        Err(DatabaseError::Rejected(Violation::StopNeedsUnknownOutcome(_))) => {
                            retry_next.insert(job);
                        }
                        Err(error) => {
                            log::warn!(
                                "failed to confirm job stop after process group cleanup: {error}"
                            );
                        }
                    }
                }
                unconfirmed_stopped = retry_next;
                match database
                    .terminal_jobs(pending.keys().copied().collect())
                    .await
                {
                    Ok(terminal) => {
                        for job in terminal {
                            if let Some(execution) = pending.remove(&job) {
                                execution.done.cancel();
                                lock(&state).current.remove(&job);
                                if let Some(reply) = execution.reply {
                                    reply
                                        .send(Ok(ExecutionReceipt {
                                            job,
                                            input: execution.started.input,
                                            executor: execution.started.executor,
                                            result: None,
                                        }))
                                        .ok();
                                }
                            }
                        }
                    }
                    Err(error) => log::warn!("pending settlement read failed: {error}"),
                }
            }
            SchedulerMessage::Claim(reply) => {
                // Admission and claim requests share this task so a committed
                // grant cannot overtake registration of its execution context.
                let batch = match database.claim_batch().await {
                    Ok(grants) => {
                        let next_due = match database.next_scheduled_delay().await {
                            Ok(due) => due,
                            Err(error) => {
                                log::warn!("dispatcher timer read failed: {error}");
                                None
                            }
                        };
                        Ok(super::scheduler::DispatchBatch { grants, next_due })
                    }
                    Err(error) => Err(error),
                };
                reply.send(batch).ok();
            }
            SchedulerMessage::Grant(job) => {
                spawn_grant(
                    job,
                    pending.remove(&job),
                    &database,
                    &sender,
                    &state,
                    &mut running,
                );
            }
            SchedulerMessage::Stop(job) => {
                stop_pending(&database, job, &mut pending, &state).await;
            }
            SchedulerMessage::Finished(job) => {
                if let Some(task) = running.remove(&job) {
                    finish_grant(&database, job, task).await;
                }
                lock(&state).current.remove(&job);
            }
        }
    }
    lock(&state).closed = true;
    for submission in admissions.drain() {
        submission.refuse();
    }
    for message in receiver.drain() {
        match message {
            SchedulerMessage::Claim(reply) => {
                reply.send(Err(DatabaseError::ShuttingDown)).ok();
            }
            SchedulerMessage::Schedule(submission) => submission.refuse(),
            SchedulerMessage::Grant(job) => spawn_grant(
                job,
                pending.remove(&job),
                &database,
                &sender,
                &state,
                &mut running,
            ),
            SchedulerMessage::Stop(_)
            | SchedulerMessage::Finished(_)
            | SchedulerMessage::Reconcile => {}
        }
    }
    dispatcher
        .await
        .map_err(|_| ExecutionRuntimeError::WorkerPanicked)?;
    for message in receiver.drain() {
        match message {
            SchedulerMessage::Schedule(submission) => submission.refuse(),
            SchedulerMessage::Claim(reply) => {
                reply.send(Err(DatabaseError::ShuttingDown)).ok();
            }
            SchedulerMessage::Grant(job) => spawn_grant(
                job,
                pending.remove(&job),
                &database,
                &sender,
                &state,
                &mut running,
            ),
            SchedulerMessage::Stop(_)
            | SchedulerMessage::Finished(_)
            | SchedulerMessage::Reconcile => {}
        }
    }
    for execution in pending.into_values() {
        cancel_pending(&database, &execution).await;
    }
    for (job, task) in running {
        finish_grant(&database, job, task).await;
    }
    lock(&state).current.clear();
    Ok(())
}

async fn stop_pending(
    database: &DatabaseHandle,
    job: JobId,
    pending: &mut BTreeMap<JobId, PendingExecution>,
    state: &SharedState,
) {
    if let Some(execution) = pending.get(&job) {
        execution.cancellation.cancel();
        match database.frozen_pool_binding(job).await {
            Ok(None) => {
                if cancel_pending(database, execution).await {
                    pending.remove(&job);
                    lock(state).current.remove(&job);
                }
            }
            Ok(Some(_)) => {} // Its grant will observe the cancelled token.
            Err(error) => log::error!("pending Job {job} stop read failed: {error}"),
        }
    }
}

async fn cancel_pending(database: &DatabaseHandle, execution: &PendingExecution) -> bool {
    let started = execution.started;
    match database
        .finalize_job(
            started.job,
            JobFinalization::Cancelled(FinalResult {
                summary: Some("stopped before execution".into()),
                code: None,
                goal_reached: None,
                value: None,
            }),
        )
        .await
    {
        Ok(result) => {
            execution.done.cancel();
            if let Some(reply) = &execution.reply {
                reply
                    .send(Ok(ExecutionReceipt {
                        executor: started.executor,
                        input: started.input,
                        job: started.job,
                        result,
                    }))
                    .ok();
            }
            true
        }
        Err(DatabaseError::Rejected(Violation::JobAlreadyFinalized(_))) => {
            execution.done.cancel();
            if let Some(reply) = &execution.reply {
                reply
                    .send(Ok(ExecutionReceipt {
                        executor: started.executor,
                        input: started.input,
                        job: started.job,
                        result: None,
                    }))
                    .ok();
            }
            true
        }
        Err(error) => {
            log::error!("pending Job {} could not settle: {error}", started.job);
            false
        }
    }
}

struct RunningExecution {
    task: ntex::rt::JoinHandle<Result<ExecutionReceipt, ExecutionRuntimeError>>,
    reply: Option<ExecutionReply>,
}

struct NotifyFinished {
    job: JobId,
    sender: flume::Sender<SchedulerMessage>,
}

impl Drop for NotifyFinished {
    fn drop(&mut self) {
        self.sender.send(SchedulerMessage::Finished(self.job)).ok();
    }
}

struct JobTask<F>(Pin<Box<F>>);

impl<F: Future<Output = Result<ExecutionReceipt, ExecutionRuntimeError>>> Future for JobTask<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match catch_unwind(AssertUnwindSafe(|| self.0.as_mut().poll(context))) {
            Ok(polled) => polled,
            Err(_) => Poll::Ready(Err(ExecutionRuntimeError::WorkerPanicked)),
        }
    }
}

async fn finish_grant(database: &DatabaseHandle, job: JobId, running: RunningExecution) {
    let result = match running.task.await {
        Ok(Err(ExecutionRuntimeError::WorkerPanicked)) | Err(_) => {
            log::error!("Job {job} task panicked");
            finalize_claim_error(database, job, &ExecutionRuntimeError::WorkerPanicked, true).await;
            Err(ExecutionRuntimeError::WorkerPanicked)
        }
        Ok(result) => result,
    };
    if let Some(reply) = running.reply {
        reply.send(result).ok();
    } else if let Err(error) = result {
        log::error!("granted Job {job} failed: {error}");
    }
}

async fn finalize_claim_error(
    database: &DatabaseHandle,
    job: JobId,
    error: &ExecutionRuntimeError,
    tool_may_have_run: bool,
) {
    if matches!(
        error,
        ExecutionRuntimeError::ObservationsNotRecorded { .. }
            | ExecutionRuntimeError::CompletionRefused { .. }
            | ExecutionRuntimeError::ToolLoopStopped { .. }
    ) {
        return;
    }
    let outcome = if tool_may_have_run {
        JobFinalization::Unknown
    } else {
        JobFinalization::Failed(FinalResult {
            summary: Some(error.to_string()),
            code: None,
            goal_reached: Some(false),
            value: None,
        })
    };
    match database.finalize_job(job, outcome).await {
        Ok(_) | Err(DatabaseError::Rejected(Violation::JobAlreadyFinalized(_))) => {}
        Err(failure) => log::error!(
            "Job {job} error ({error}) could not be finalized: {failure}; open reconcile will recover it"
        ),
    }
}

fn spawn_grant(
    job: JobId,
    mut execution: Option<PendingExecution>,
    database: &DatabaseHandle,
    sender: &flume::Sender<SchedulerMessage>,
    state: &SharedState,
    running: &mut BTreeMap<JobId, RunningExecution>,
) {
    let database = database.clone();
    let notify = NotifyFinished {
        job,
        sender: sender.clone(),
    };
    let reply = execution
        .as_mut()
        .and_then(|execution| execution.reply.take());
    if let Some(execution) = &execution {
        execution.done.cancel();
        if lock(state).stopping {
            execution.cancellation.cancel();
        }
    }
    let task = ntex::rt::spawn(async move {
        let _notify = notify;
        JobTask(Box::pin(async move {
            let tool_may_have_run = Cell::new(false);
            let result = async {
                let execution =
                    execution.ok_or(ExecutionRuntimeError::MissingExecutionPlan(job))?;
                let binding = database
                    .frozen_pool_binding(job)
                    .await?
                    .ok_or(ExecutionRuntimeError::MissingPoolBinding(job))?;
                let started = StartedJob {
                    job,
                    input: JobInputId::new(job.get()),
                    executor: binding.pool.into(),
                };
                if execution.cancellation.is_cancelled() {
                    let result = database
                        .finalize_job(
                            job,
                            JobFinalization::Cancelled(FinalResult {
                                summary: Some("stopped before execution".into()),
                                code: None,
                                goal_reached: None,
                                value: None,
                            }),
                        )
                        .await?;
                    return Ok(ExecutionReceipt {
                        job,
                        executor: started.executor,
                        input: started.input,
                        result,
                    });
                }
                match execution.execution {
                    PendingPlan::Plan(outcome, result) => {
                        let result = database
                            .finalize_job(job, outcome.finalization(result))
                            .await?;
                        Ok(ExecutionReceipt {
                            job,
                            executor: started.executor,
                            input: started.input,
                            result,
                        })
                    }
                    PendingPlan::Provider(plan, offered) => {
                        run_started_provider_job(
                            database.clone(),
                            plan,
                            (started, offered),
                            &tool_may_have_run,
                        )
                        .await
                    }
                }
            }
            .await;
            if let Err(error) = &result {
                finalize_claim_error(&database, job, error, tool_may_have_run.get()).await;
            }
            result
        }))
        .await
    });
    running.insert(job, RunningExecution { task, reply });
}

/// Admits the pending Job of a provider plan through T1, once the plan is
/// checked, and returns it with the tool definitions to offer. Nothing is
/// persisted unless the Executor is registered, the tool limits are valid
/// and the tools can still be offered. A `submission` is recorded with the
/// Job in the transaction that creates it.
async fn admit_provider_job(
    database: &DatabaseHandle,
    plan: &mut ProviderExecutionPlan,
    submission: Option<NewSubmission>,
    workspace_tools: Option<&WorkspaceTools>,
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
        None => database.create_job(plan.job.clone()).await,
        Some(submission) => match workspace_tools {
            Some(tools) => {
                database
                    .create_workspace_submitted_job(plan.job.clone(), submission, tools.available())
                    .await
            }
            None => {
                database
                    .create_submitted_job(plan.job.clone(), submission)
                    .await
            }
        },
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

pub(crate) const OMITTED_NOTICE: &str =
    "Earlier turns of this conversation were omitted to fit the model's context window.";

fn append_omitted_notice(conversation: &mut ChatConversation) {
    if let Some(sys_msg) = conversation
        .messages
        .iter_mut()
        .find(|m| m.role == ChatRole::System)
    {
        match &mut sys_msg.content {
            Some(MessageContentWire::Text(text)) => {
                if !text.is_empty() && !text.ends_with("\n\n") {
                    if text.ends_with('\n') {
                        text.push('\n');
                    } else {
                        text.push_str("\n\n");
                    }
                }
                text.push_str(OMITTED_NOTICE);
            }
            Some(MessageContentWire::Parts(parts)) => {
                parts.push(ContentPartWire::Text {
                    text: format!("\n\n{OMITTED_NOTICE}"),
                });
            }
            None => {
                sys_msg.content = Some(MessageContentWire::Text(OMITTED_NOTICE.to_owned()));
            }
        }
    } else {
        conversation
            .messages
            .insert(0, ChatMessage::system(OMITTED_NOTICE));
    }
}

pub(crate) fn assemble_conversation(
    frozen: &FrozenExecutionConfig,
    prior_turns: &[(Option<String>, Vec<ImageInput>, String)],
    omitted_notice: bool,
    current_objective: Option<&str>,
    current_images: &[ImageInput],
    workspace_root: Option<&std::path::Path>,
    tools: Vec<ToolWire>,
) -> ChatConversation {
    let mut conversation = provider_execution::objective_conversation(frozen, None);
    conversation.messages.pop();

    if omitted_notice {
        append_omitted_notice(&mut conversation);
    }

    for (turn_objective, turn_images, assistant_text) in prior_turns {
        if let Some(user_msg) = provider_execution::objective_conversation_with_images(
            frozen,
            turn_objective.as_deref(),
            turn_images,
        )
        .messages
        .pop()
        {
            conversation.messages.push(user_msg);
        }
        conversation
            .messages
            .push(ChatMessage::assistant(Some(assistant_text.clone()), Vec::new()));
    }

    if let Some(user_msg) = provider_execution::objective_conversation_with_images(
        frozen,
        current_objective,
        current_images,
    )
    .messages
    .pop()
    {
        conversation.messages.push(user_msg);
    }

    if let Some(root) = workspace_root {
        provider_execution::ground_workspace(&mut conversation, root);
    }
    conversation.tools = tools;
    conversation
}

/// Estimates the token count of a conversation request under conservative assumptions:
/// `ceil(UTF-8 bytes / 3)` over all message text, tool arguments, tool results, and
/// offered tool schemas, plus 1,600 tokens per image, plus `max_tokens` (or 4,096
/// when unset).
pub(crate) fn estimate_request_tokens(
    conversation: &ChatConversation,
    max_tokens: Option<u32>,
) -> u64 {
    let mut bytes: usize = 0;
    let mut images: usize = 0;

    for message in &conversation.messages {
        if let Some(content) = &message.content {
            match content {
                MessageContentWire::Text(text) => {
                    bytes = bytes.saturating_add(text.len());
                }
                MessageContentWire::Parts(parts) => {
                    for part in parts {
                        match part {
                            ContentPartWire::Text { text } => {
                                bytes = bytes.saturating_add(text.len());
                            }
                            ContentPartWire::ImageUrl { .. } => {
                                images = images.saturating_add(1);
                            }
                        }
                    }
                }
            }
        }
        if let Some(reasoning) = &message.reasoning_content {
            bytes = bytes.saturating_add(reasoning.len());
        }
        if let Some(calls) = &message.tool_calls {
            for call in calls {
                bytes = bytes.saturating_add(call.function.arguments.len());
            }
        }
    }

    for tool in &conversation.tools {
        if let Some(parameters) = &tool.function.parameters {
            bytes = bytes.saturating_add(parameters.to_string().len());
        }
    }

    let text_tokens = bytes.div_ceil(3) as u64;
    let image_tokens = (images as u64).saturating_mul(1600);
    let output_tokens = max_tokens.map_or(4096, u64::from);
    text_tokens
        .saturating_add(image_tokens)
        .saturating_add(output_tokens)
}

/// Runs a provider plan whose Job was started to its settlement.
async fn run_started_provider_job(
    database: DatabaseHandle,
    mut plan: ProviderTask,
    (started, mut offered): (StartedJob, Option<Vec<ToolWire>>),
    tool_may_have_run: &Cell<bool>,
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
    let mut workspace_root = None;
    if let Some(tools) = &plan.workspace_tools {
        let grant = configuration
            .frozen
            .agent
            .as_ref()
            .and_then(|agent| agent.tool_grant.as_deref());
        plan.tools = match tools.build(grant) {
            Ok(tools) => tools,
            Err(error) => return refused(error.to_string()).await,
        };
        if plan.tools.is_some() {
            workspace_root = tools.root();
        }
        offered = match plan
            .tools
            .as_ref()
            .map(|tools| tools.offer.wires())
            .transpose()
        {
            Ok(offered) => offered,
            Err(error) => return refused(error.to_string()).await,
        };
    }
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
    let mut prior_turns = Vec::new();
    if let Some(first_previous) = job_record.previous {
        const MAX_CONVERSATION_TURNS: usize = 64;
        let genesis = database.genesis();
        let mut visited = std::collections::BTreeSet::new();
        visited.insert(job);
        let mut cursor = Some(first_previous);
        while let Some(current) = cursor {
            if visited.len() >= MAX_CONVERSATION_TURNS {
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
            if !ancestor.settled() {
                return refused(format!("previous Job {current} has not finished")).await;
            }
            cursor = ancestor.previous;
            if ancestor.completed_at.is_none() {
                continue;
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
                Some(output) if output.kind == crate::persistence::OutputKind::Result => {
                    output.summary.unwrap_or_default()
                }
                _ => return refused(format!("previous Job {current} has no final answer")).await,
            };
            prior_turns.push((ancestor_input.objective, ancestor_images, assistant_text));
        }
        prior_turns.reverse();
    }

    let context_window = configuration.frozen.llm.config.context_window;
    let max_tokens = configuration.frozen.llm.config.parameters.max_tokens;
    let tools_wire = offered.unwrap_or_default();

    let conversation = if let Some(window) = context_window {
        let total_priors = prior_turns.len();
        let mut selected = None;

        let full = assemble_conversation(
            &configuration.frozen,
            &prior_turns,
            false,
            input_record.objective.as_deref(),
            &images,
            workspace_root,
            tools_wire.clone(),
        );
        if estimate_request_tokens(&full, max_tokens) <= window as u64 {
            selected = Some(full);
        } else {
            for drop_count in 1..=total_priors {
                let candidate = assemble_conversation(
                    &configuration.frozen,
                    &prior_turns[drop_count..],
                    true,
                    input_record.objective.as_deref(),
                    &images,
                    workspace_root,
                    tools_wire.clone(),
                );
                if estimate_request_tokens(&candidate, max_tokens) <= window as u64 {
                    selected = Some(candidate);
                    break;
                }
            }
        }

        match selected {
            Some(conv) => conv,
            None => {
                let failed = FinalResult {
                    summary: Some("the request exceeds the model's context window".to_owned()),
                    code: Some("context_window_exceeded".to_owned()),
                    goal_reached: Some(false),
                    value: None,
                };
                let result = database
                    .finalize_job(job, JobFinalization::Failed(failed))
                    .await?;
                return Ok(ExecutionReceipt {
                    executor,
                    input,
                    job,
                    result,
                });
            }
        }
    } else {
        assemble_conversation(
            &configuration.frozen,
            &prior_turns,
            false,
            input_record.objective.as_deref(),
            &images,
            workspace_root,
            tools_wire.clone(),
        )
    };

    let frozen = &configuration.frozen;
    let resolver = plan.credential_resolver.as_ref();
    let config = &plan.provider_config;
    run_provider_job_inner(
        ProviderRun {
            database,
            executor,
            input,
            job,
        },
        conversation,
        plan.tools.as_ref(),
        ContextLimits {
            context_window,
            max_tokens,
        },
        &config.cancellation,
        tool_may_have_run,
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

    type Script =
        Box<dyn FnMut(&ChatConversation) -> Result<Response, ProviderExecutionError> + Send>;
    type Hook = Box<dyn FnMut() + Send>;

    static SCRIPTS: Mutex<BTreeMap<String, Script>> = Mutex::new(BTreeMap::new());
    static HOOKS: Mutex<BTreeMap<String, Hook>> = Mutex::new(BTreeMap::new());

    /// Answers every request to `endpoint` with what `script` gives, instead
    /// of sending it.
    pub(crate) fn install(
        endpoint: &str,
        mut script: impl FnMut(&ChatConversation) -> Response + Send + 'static,
    ) {
        install_result(endpoint, move |conversation| Ok(script(conversation)));
    }

    pub(crate) fn install_result(
        endpoint: &str,
        script: impl FnMut(&ChatConversation) -> Result<Response, ProviderExecutionError>
        + Send
        + 'static,
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
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                provider_execution::announce(
                    Some(&mut *sink),
                    provider_execution::unannounced_fact(&error),
                )
                .await;
                return Err(error);
            }
        };
        provider_execution::announce(Some(&mut *sink), ProviderFact::Delivered).await;
        if let Some(gate) = response.gate {
            select(config.cancellation.cancelled(), gate.recv_async()).await;
        }
        let config = config.for_profile(frozen);
        provider_execution::read_response(
            payload(response.steps, config.cancellation.clone()),
            200,
            Instant::now(),
            None,
            &config,
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
#[cfg(test)]
pub(crate) async fn run_provider_job(
    run: ProviderRun,
    conversation: ChatConversation,
    tools: Option<&ToolExecution>,
    cancellation: &CancellationToken,
    exchange: impl AsyncFnMut(
        &ChatConversation,
        &mut DatabaseOutputSink,
    ) -> Result<ProviderExecutionResult, ProviderExecutionError>,
) -> Result<ExecutionReceipt, ExecutionRuntimeError> {
    run_provider_job_with_window(
        run,
        conversation,
        tools,
        None,
        None,
        cancellation,
        exchange,
    )
    .await
}

#[cfg(test)]
pub(crate) async fn run_provider_job_with_window(
    run: ProviderRun,
    conversation: ChatConversation,
    tools: Option<&ToolExecution>,
    context_window: Option<u32>,
    max_tokens: Option<u32>,
    cancellation: &CancellationToken,
    exchange: impl AsyncFnMut(
        &ChatConversation,
        &mut DatabaseOutputSink,
    ) -> Result<ProviderExecutionResult, ProviderExecutionError>,
) -> Result<ExecutionReceipt, ExecutionRuntimeError> {
    let database = run.database.clone();
    let job = run.job;
    let tool_may_have_run = Cell::new(false);
    let result = run_provider_job_inner(
        run,
        conversation,
        tools,
        ContextLimits {
            context_window,
            max_tokens,
        },
        cancellation,
        &tool_may_have_run,
        exchange,
    )
    .await;
    if let Err(error) = &result {
        finalize_claim_error(&database, job, error, tool_may_have_run.get()).await;
    }
    result
}

#[derive(Clone, Copy, Default)]
struct ContextLimits {
    context_window: Option<u32>,
    max_tokens: Option<u32>,
}

async fn run_provider_job_inner(
    run: ProviderRun,
    mut conversation: ChatConversation,
    tools: Option<&ToolExecution>,
    context_limits: ContextLimits,
    cancellation: &CancellationToken,
    tool_may_have_run: &Cell<bool>,
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
    if let Some(window) = context_limits.context_window
        && estimate_request_tokens(&conversation, context_limits.max_tokens) > window as u64
    {
        let stopped = Stopped::from(ToolLoopStop::ContextWindow);
        return stop_tool_loop(run, sink, None, stopped).await;
    }
    let mut budget = ToolBudget::default();
    let mut round = 1u32;
    loop {
        let mut retries = 0;
        let execution = loop {
            let execution = exchange(&conversation, &mut sink).await;
            let Err(error) = &execution else {
                break execution;
            };
            let Some((delay, next)) =
                retry_delay(error, retries, cancellation.is_cancelled()).zip(sink.request.next())
            else {
                break execution;
            };
            let observed_at = Timestamp::now();
            for observation in provider_execution::error_observations(error) {
                if let Err(error) = run
                    .database
                    .record_availability_stat(NewAvailabilityStat {
                        job: run.job,
                        request: sink.request,
                        observed_at,
                        observation,
                    })
                    .await
                {
                    sink.gap.get_or_insert(error);
                }
            }
            sink.request = next;
            retries += 1;
            if let ntex::util::Either::Left(()) =
                ntex::util::select(cancellation.cancelled(), ntex::time::sleep(delay)).await
            {
                provider_execution::announce(Some(&mut sink), ProviderFact::NotDelivered).await;
                break Err(ProviderExecutionError::NotDelivered {
                    message: "provider execution was cancelled before sending".to_owned(),
                });
            }
        };
        let result = match execution {
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
        let next = sink
            .request
            .next()
            .filter(|_| tools.is_some_and(|tools| round < tools.limits.max_provider_requests));
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
                    tool_may_have_run,
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
        if let Some(window) = context_limits.context_window
            && estimate_request_tokens(&conversation, context_limits.max_tokens) > window as u64
        {
            return stop_tool_loop(
                run,
                sink,
                Some(&result),
                Stopped::from(ToolLoopStop::ContextWindow),
            )
            .await;
        }
        sink.request = next;
        round += 1;
    }
}

fn retry_delay(
    error: &ProviderExecutionError,
    retries: u32,
    cancelled: bool,
) -> Option<std::time::Duration> {
    if cancelled || retries >= 3 {
        return None;
    }
    let milliseconds = match error {
        ProviderExecutionError::Rejected { failure } if failure.retryable => {
            failure.retry_after_ms.unwrap_or(1_000 << retries)
        }
        ProviderExecutionError::NotDelivered { .. } => 1_000 << retries,
        _ => return None,
    };
    (milliseconds <= 30_000).then(|| std::time::Duration::from_millis(milliseconds))
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
    tool_may_have_run: &Cell<bool>,
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
        tool_may_have_run.set(true);
        let handler = dispatch.handler;
        let spawns = dispatch.spawns;
        let arguments = dispatch.arguments;
        let stopping_handler = handler.clone();
        let start = ToolStart::default();
        let invocation_start = start.clone();
        let invocation = CatchPanic(Box::pin(async move {
            handler
                .call_with_start(reference, arguments, invocation_start)
                .await
        }));
        let timed = async {
            let expires = async {
                start.wait().await;
                ntex::time::sleep(start.remaining(limits.handler_timeout)).await;
            };
            match select(invocation, expires).await {
                Either::Left(result) => Ok(result),
                Either::Right(()) => Err(()),
            }
        };
        let ran = select(cancellation.cancelled(), timed).await;
        #[cfg(test)]
        dispatch_seam::reach(dispatch_seam::Point::Ran, reference);
        let stopped = match &ran {
            Either::Left(()) | Either::Right(Err(())) | Either::Right(Ok(Err(_))) if spawns => {
                stopping_handler.stop(site.job).await
            }
            _ => Ok(()),
        };
        if let Err(failure) = stopped {
            return Err(ToolLoopStop::HandlerUncertain {
                index,
                message: failure.message,
            }
            .into());
        }
        let (outcome, stop) = match ran {
            Either::Left(()) if !start.has_started() => (
                ToolOutcome::NotApplied {
                    message: "cancelled before the handler started".into(),
                },
                ToolLoopStop::Cancelled,
            ),
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
/// effect uncertain leaves the outcome unknown. No Provider failure is
/// recorded for either.
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
    Err(ExecutionRuntimeError::ToolLoopStopped {
        receipt,
        stop,
        observations,
        evidence,
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
            if text.is_empty() {
                return Ok(());
            }
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
/// execution has returned; finalization checks `no_live_process` and releases
/// capacity in the same commit when no process can remain.
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
    match gap {
        Some(source) => Err(ExecutionRuntimeError::ObservationsNotRecorded {
            receipt: Box::new(receipt),
            source,
        }),
        None => Ok(receipt),
    }
}

#[cfg(test)]
mod failure_tests {
    use super::*;
    use crate::database::{DatabaseWorker, READ_QUEUE_CAPACITY, READ_WORKERS};
    use crate::persistence::{LlmTransport, NewExecutor};
    use crate::test_support::{
        data_directory, execution_configuration, on_system, poll_once, wait,
    };

    #[test]
    fn provider_network_database_output_preserves_whitespace_flushes() {
        use crate::provider_execution::scripted::{Step, payload};
        let (_root, directory) = data_directory("provider-network", "whitespace");
        let worker = DatabaseWorker::start(&directory).expect("database");
        let database = worker.handle();
        let executor = executor(&database);
        let job = wait(database.create_claimed_fixture(spec(&database), executor)).expect("claim");
        on_system(move || async move {
            let config = ProviderExecutionConfig::default();
            let prefix = "x".repeat(4096);
            let first = Box::leak(
                format!("data: {{\"choices\":[{{\"delta\":{{\"content\":\"{prefix}\"}}}}]}}\n\n")
                    .into_boxed_str(),
            );
            let mut sink = output_sink(database.clone(), job);
            provider_execution::read_response(
                payload(vec![
                    Step::Chunk(first),
                    Step::Chunk("data: {\"choices\":[{\"delta\":{\"content\":\" \\n\\n\"},\"finish_reason\":\"stop\"}]}\n\n"),
                ], config.cancellation.clone()),
                200, Instant::now(), None, &config, Some(&mut sink),
                &ChatConversation { messages: vec![ChatMessage::user("hi")], ..Default::default() },
            ).await.expect("stream persisted");
            sink.append("").await.expect("skip empty");
            let outputs = database.job_outputs(job, None, 10).await.expect("outputs");
            assert_eq!(outputs.len(), 2);
            assert_eq!(outputs[0].summary.as_deref(), Some(prefix.as_str()));
            assert_eq!(outputs[1].summary.as_deref(), Some(" \n\n"));
        });
        worker.shutdown().expect("shutdown");
    }

    fn spec(database: &DatabaseHandle) -> NewJob {
        NewJob {
            name: Some("failure fixture".into()),
            eligible_at: None,
            parent: database.genesis(),
            projects: Vec::new(),
            dependencies: Vec::new(),
            input: JobInputDraft::default(),
            configuration: Some(execution_configuration(database)),
            previous: None,
        }
    }

    fn pending(job: JobId, executor: ExecutorId) -> PendingExecution {
        PendingExecution {
            started: StartedJob {
                job,
                input: JobInputId::new(job.get()),
                executor,
            },
            execution: PendingPlan::Plan(
                JobOutcome::Completed,
                FinalResult {
                    summary: Some("done".into()),
                    code: None,
                    goal_reached: None,
                    value: None,
                },
            ),
            cancellation: CancellationToken::default(),
            done: CancellationToken::default(),
            reply: None,
        }
    }

    fn executor(database: &DatabaseHandle) -> ExecutorId {
        wait(database.create_executor(NewExecutor {
            name: "failure worker".into(),
            concurrency_limit: std::num::NonZeroU32::new(1).expect("one"),
            llm_transports: std::collections::BTreeSet::from([LlmTransport::OpenAiChatCompletions]),
        }))
        .expect("executor")
    }

    #[cfg(unix)]
    #[test]
    fn provider_network_retries_preserve_requests_rounds_and_cancellation() {
        use crate::openai_compatible::ProviderFailure;
        use crate::provider_execution::scripted::Step;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let rejected = |delay| ProviderExecutionError::Rejected {
            failure: ProviderFailure {
                retryable: true,
                retry_after_ms: delay,
                ..Default::default()
            },
        };
        for (retry, seconds) in [(0, 1), (1, 2), (2, 4)] {
            assert_eq!(
                retry_delay(&rejected(None), retry, false),
                Some(Duration::from_secs(seconds))
            );
        }
        assert!(retry_delay(&rejected(None), 3, false).is_none());
        assert!(retry_delay(&rejected(Some(30_001)), 0, false).is_none());
        assert!(retry_delay(&rejected(None), 0, true).is_none());
        let facts = crate::provider_execution::ResponseFacts {
            status_code: 200,
            finish_reason: None,
            raw_finish_reason: None,
        };
        for error in [
            ProviderExecutionError::Rejected {
                failure: ProviderFailure::default(),
            },
            ProviderExecutionError::Unknown {
                message: "unknown".into(),
                facts: None,
                finished: false,
            },
            ProviderExecutionError::Incomplete {
                message: "incomplete".into(),
                facts: facts.clone(),
            },
            ProviderExecutionError::Interrupted {
                message: "interrupted".into(),
            },
            ProviderExecutionError::Stopped {
                message: "stopped".into(),
                facts,
            },
            ProviderExecutionError::OutputPersistence {
                message: "write".into(),
                stream: Box::new(rejected(None)),
            },
        ] {
            assert!(retry_delay(&error, 0, false).is_none());
        }
        assert_eq!(crate::provider_execution::retry_after_ms("2"), Some(2_000));
        assert_eq!(
            crate::provider_execution::retry_after_ms("Sun, 06 Nov 1994 08:49:37 GMT"),
            Some(0)
        );
        assert!(
            crate::provider_execution::retry_after_ms("Fri, 01 Jan 2100 00:00:00 GMT")
                .expect("date")
                > 30_000
        );
        assert_eq!(crate::provider_execution::retry_after_ms("invalid"), None);

        let (_root, directory) = data_directory("provider-network", "retries");
        let (_workspace, root) =
            crate::test_support::temporary_directory("provider-network", "retry-workspace");
        std::fs::write(root.join("input"), "read me").expect("file");
        let mut tools = crate::orchestration::workspace_read::read_file_tools(
            crate::orchestration::workspace_read::Workspace::authorize(&root, &directory)
                .expect("workspace"),
        )
        .expect("tools");
        tools.limits.max_provider_requests = 2;
        let worker = DatabaseWorker::start(&directory).expect("database");
        let database = worker.handle();
        for (scenario, expected) in [(0, 6), (1, 4), (2, 1), (3, 1), (4, 1)] {
            let executor = executor(&database);
            let job =
                wait(database.create_claimed_fixture(spec(&database), executor)).expect("claim");
            let mut frozen = wait(database.job_configuration(job))
                .expect("configuration")
                .expect("frozen")
                .frozen;
            let endpoint = format!("https://retries-{scenario}.example/v1");
            frozen.llm.provider.endpoint = Some(endpoint.clone());
            let attempts = Arc::new(AtomicUsize::new(0));
            let sent = attempts.clone();
            let cancellation = CancellationToken::default();
            let stop = cancellation.clone();
            provider_seam::install_result(&endpoint, move |conversation| {
                let attempt = sent.fetch_add(1, Ordering::SeqCst);
                if scenario == 3 {
                    let stop = stop.clone();
                    ntex::rt::spawn(async move {
                        ntex::time::sleep(Duration::from_millis(50)).await;
                        stop.cancel();
                    });
                }
                if scenario == 4 {
                    return Err(ProviderExecutionError::Unknown {
                        message: "unknown".into(),
                        facts: None,
                        finished: false,
                    });
                }
                if scenario == 0 && attempt == 1 {
                    return Err(ProviderExecutionError::NotDelivered {
                        message: "connection refused".into(),
                    });
                }
                if scenario != 0 || matches!(attempt, 0 | 2 | 4) {
                    return Err(ProviderExecutionError::Rejected {
                        failure: ProviderFailure {
                            status_code: Some(429),
                            message: "busy".into(),
                            retryable: true,
                            retry_after_ms: Some(if scenario == 2 {
                                31_000
                            } else if scenario == 3 {
                                30_000
                            } else {
                                0
                            }),
                            ..Default::default()
                        },
                    });
                }
                let chunk = if attempt == 3 {
                    assert_eq!(conversation.messages.len(), 1);
                    "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"read\",\"type\":\"function\",\"function\":{\"name\":\"workspace_read_file\",\"arguments\":\"{\\\"path\\\":\\\"input\\\"}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\ndata: [DONE]\n\n"
                } else {
                    assert_eq!(conversation.messages.len(), 3);
                    "data: {\"choices\":[{\"delta\":{\"content\":\"answer\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":2}}\n\ndata: [DONE]\n\n"
                };
                Ok(provider_seam::Response {
                    gate: None,
                    steps: vec![Step::Chunk(chunk)],
                })
            });
            let handle = database.clone();
            let tools = tools.clone();
            let elapsed = Instant::now();
            on_system(move || async move {
                let config = ProviderExecutionConfig {
                    cancellation: cancellation.clone(),
                    ..Default::default()
                };
                run_provider_job(
                    ProviderRun {
                        database: handle,
                        executor,
                        input: JobInputId::new(job.get()),
                        job,
                    },
                    ChatConversation {
                        messages: vec![ChatMessage::user("read")],
                        tools: tools.offer.wires().expect("offer"),
                        ..Default::default()
                    },
                    Some(&tools),
                    &cancellation,
                    async |conversation, sink| {
                        provider_seam::exchange(&frozen, &config, conversation, sink).await
                    },
                )
                .await
                .expect("settled");
            });
            assert_eq!(attempts.load(Ordering::SeqCst), expected);
            let availability =
                wait(database.job_availability(job, None, 20)).expect("availability");
            if scenario == 0 {
                assert_eq!(
                    availability
                        .iter()
                        .map(|stat| stat.request.get())
                        .collect::<Vec<_>>(),
                    [1, 3, 4, 5, 6]
                );
                let metrics = wait(database.job_metrics(job, None, 20)).expect("metrics");
                assert!(metrics.iter().any(|metric| metric.request.get() == 2
                    && metric.token == Some(TokenAnnotation::excluded())));
            }
            if scenario == 3 {
                assert!(elapsed.elapsed() < Duration::from_secs(2));
                assert_eq!(availability.len(), 1);
                assert_eq!(availability[0].request.get(), 1);
            }
        }
        worker.shutdown().expect("shutdown");
    }

    #[test]
    fn a_full_read_queue_after_claim_finalizes_failed_and_releases_capacity() {
        let (_root, directory) = data_directory("dispatcher-errors", "read-queue");
        let worker = DatabaseWorker::start(&directory).expect("database");
        let database = worker.handle();
        let executor = executor(&database);
        let job = wait(database.create_claimed_fixture(spec(&database), executor)).expect("claim");
        let stopped = wait(database.create_job(spec(&database))).expect("pending Job");
        on_system(move || async move {
            let (entered, started) = flume::unbounded();
            let (release, released) = flume::unbounded();
            let mut reads = Vec::new();
            for index in 0..READ_WORKERS + READ_QUEUE_CAPACITY {
                let entered = entered.clone();
                let released = released.clone();
                let mut read = Box::pin(database.with_database(move |_| {
                    if index < READ_WORKERS {
                        entered.send(()).expect("entered");
                        released
                            .recv_timeout(crate::test_support::WAIT)
                            .expect("release");
                    }
                    Ok(())
                }));
                assert!(poll_once(read.as_mut()).is_none());
                reads.push(read);
                if index + 1 == READ_WORKERS {
                    for _ in 0..READ_WORKERS {
                        started.recv_async().await.expect("held readers");
                    }
                }
            }
            let state = SharedState::default();
            let mut pending_jobs = BTreeMap::from([(stopped, pending(stopped, executor))]);
            stop_pending(&database, stopped, &mut pending_jobs, &state).await;
            let retained = pending_jobs
                .get(&stopped)
                .expect("failed read keeps the pending stop");
            assert!(retained.cancellation.is_cancelled());
            assert!(!retained.done.is_cancelled());
            let (sender, messages) = flume::unbounded();
            let mut running = BTreeMap::new();
            let (reply, response) = flume::bounded(1);
            let mut execution = pending(job, executor);
            execution.reply = Some(reply);
            spawn_grant(
                job,
                Some(execution),
                &database,
                &sender,
                &SharedState::default(),
                &mut running,
            );
            assert!(
                matches!(messages.recv_async().await, Ok(SchedulerMessage::Finished(id)) if id == job)
            );
            finish_grant(&database, job, running.remove(&job).expect("task")).await;
            assert!(matches!(
                response.recv_async().await,
                Ok(Err(ExecutionRuntimeError::Database(
                    DatabaseError::ReadQueueFull
                )))
            ));
            for _ in 0..READ_WORKERS {
                release.send(()).expect("release");
            }
            for read in reads {
                read.await.expect("read drained");
            }
            assert!(
                database
                    .job(job)
                    .await
                    .expect("read")
                    .expect("Job")
                    .failed_at
                    .is_some()
            );
            assert!(database.occupancy().await.expect("slots").is_empty());
            assert_eq!(
                database.claim_batch().await.expect("later claim"),
                vec![stopped]
            );
            spawn_grant(
                stopped,
                pending_jobs.remove(&stopped),
                &database,
                &sender,
                &state,
                &mut running,
            );
            messages.recv_async().await.expect("stopped task finished");
            finish_grant(&database, stopped, running.remove(&stopped).expect("task")).await;
            assert!(
                database
                    .job(stopped)
                    .await
                    .expect("read")
                    .expect("Job")
                    .cancelled_at
                    .is_some()
            );
            assert!(database.occupancy().await.expect("slots").is_empty());
        });
        worker.shutdown().expect("shutdown");
    }

    #[test]
    fn a_failed_pending_cancel_commit_keeps_its_context_and_cancelled_token() {
        use crate::disk_protection::{ProtectedArea, ProtectionReason, SpaceShortage};
        let (_root, directory) = data_directory("dispatcher-errors", "cancel-commit");
        let worker = DatabaseWorker::start(&directory).expect("database");
        let database = worker.handle();
        let executor = executor(&database);
        let job = wait(database.create_job(spec(&database))).expect("pending Job");
        assert!(
            database.activate_filesystem_protection(ProtectionReason::InsufficientSpace(
                SpaceShortage {
                    area: ProtectedArea::Database,
                    path: directory.database_path().to_path_buf(),
                    available_bytes: 0,
                    expected_write_bytes: 0,
                    threshold_bytes: 1,
                }
            ))
        );
        on_system(move || async move {
            let state = SharedState::default();
            let (reply, response) = flume::bounded(1);
            let mut execution = pending(job, executor);
            execution.reply = Some(reply);
            lock(&state)
                .current
                .insert(job, execution.cancellation.clone());
            let mut pending_jobs = BTreeMap::from([(job, execution)]);
            stop_pending(&database, job, &mut pending_jobs, &state).await;
            let retained = pending_jobs
                .get(&job)
                .expect("failed commit keeps its execution");
            assert!(retained.cancellation.is_cancelled());
            assert!(!retained.done.is_cancelled());
            assert!(response.try_recv().is_err());
            assert!(lock(&state).current.contains_key(&job));
            assert!(
                database
                    .job(job)
                    .await
                    .expect("read")
                    .expect("Job")
                    .outcome
                    .is_none()
            );
        });
        worker.shutdown().expect("shutdown");
    }

    #[test]
    fn a_grant_without_its_admitting_process_plan_is_failed_without_execution() {
        let (_root, directory) = data_directory("dispatcher-errors", "missing-plan");
        let worker = DatabaseWorker::start(&directory).expect("database");
        let database = worker.handle();
        let executor = executor(&database);
        let job = wait(database.create_claimed_fixture(spec(&database), executor)).expect("claim");
        on_system(move || async move {
            let (sender, messages) = flume::unbounded();
            let mut running = BTreeMap::new();
            spawn_grant(
                job,
                None,
                &database,
                &sender,
                &SharedState::default(),
                &mut running,
            );
            messages.recv_async().await.expect("finished");
            finish_grant(&database, job, running.remove(&job).expect("task")).await;
            assert!(
                database
                    .job(job)
                    .await
                    .expect("read")
                    .expect("Job")
                    .failed_at
                    .is_some()
            );
            assert!(database.occupancy().await.expect("slots").is_empty());
        });
        worker.shutdown().expect("shutdown");
    }

    #[test]
    fn admission_refuses_the_sixty_fifth_queued_plan_as_busy() {
        let (_root, directory) = data_directory("dispatcher-errors", "admission-bound");
        let worker = DatabaseWorker::start(&directory).expect("database");
        let database = worker.handle();
        let job = spec(&database);
        let (sender, receiver) = flume::bounded(64);
        let scheduler = SchedulerHandle {
            sender,
            state: SharedState::default(),
        };
        for index in 0..65 {
            let (reply, _) = flume::bounded(1);
            let submitted = scheduler.submit(Submission::Plan(
                ExecutionPlan {
                    executor_id: ExecutorId::new(1),
                    input: JobInputDraft::default(),
                    job: job.clone(),
                    outcome: JobOutcome::Completed,
                    result: FinalResult {
                        summary: None,
                        code: None,
                        goal_reached: None,
                        value: None,
                    },
                },
                reply,
            ));
            if index < 64 {
                submitted.expect("queue space");
            } else {
                assert!(matches!(
                    submitted,
                    Err(ExecutionRuntimeError::SchedulerQueueFull)
                ));
            }
        }
        assert_eq!(receiver.len(), 64);
        assert!(wait(database.jobs(None, 100)).expect("jobs").is_empty());
        worker.shutdown().expect("shutdown");
    }
}
