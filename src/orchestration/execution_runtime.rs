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
    AppendedOutput, ApproveClass, AttachmentId, AuthorizationBasis, ExecutorId, FinalResult,
    FrozenExecutionConfig, IntermediateOutputKind, JobFinalization, JobId, JobInputDraft,
    JobInputId, JobOutcome, JobRecord, MAX_STORED_TOOL_ARGUMENTS_BYTES,
    MAX_STORED_TOOL_ARGUMENTS_PER_JOB_BYTES, Measurements, NewAvailabilityStat, NewJob,
    NewJobOutput, NewMetrics, NewSubmission, NewToolEvidence, NotInvokedReason, ProjectCommand,
    ProjectId, ProjectRecord, RequestNumber, Timestamp, ToolAdmission, ToolCallRef, ToolDecision,
    ToolOutcome, ToolOutcomeKind, ToolPayload, UncertainCause, Violation, WithheldReason,
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
    pub(crate) approve: Vec<ApproveClass>,
    pub(crate) decider: bool,
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
    /// The Job's outcome could not be recorded. The runtime retries the same
    /// settlement once. If that also fails, open reconcile records
    /// Unknown/Abandoned at the next start of the database; see
    /// [`DatabaseHandle::abandoned_at_open`].
    /// What the Provider was observed to do is still recorded, apart from
    /// `observations`, the first observation that could not be written.
    #[error("the Job's outcome was not recorded: {source}")]
    NotSettled {
        source: DatabaseError,
        observations: Option<Box<DatabaseError>>,
        settlement: Box<JobFinalization>,
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
    stops: BTreeMap<JobId, Arc<Mutex<Option<crate::persistence::StopCause>>>>,
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

/// What a Provider execution needs to turn a Job's Project into tools: the
/// data directory its root must stay apart from. Nothing of a Project is
/// kept here; it is read again for every Job, see [`resolve_project`].
#[derive(Clone, Debug)]
pub(crate) struct WorkspaceTools {
    data_directory: Arc<crate::data_directory::DataDirectory>,
}

impl WorkspaceTools {
    pub(crate) fn new(data_directory: crate::data_directory::DataDirectory) -> Self {
        Self {
            data_directory: Arc::new(data_directory),
        }
    }

    pub(crate) fn data_directory(&self) -> &crate::data_directory::DataDirectory {
        &self.data_directory
    }
}

/// The tools `grant` allows in `workspace`, with the Project's `commands`.
#[cfg(unix)]
fn build_tools(
    workspace: &crate::orchestration::workspace_read::Workspace,
    grant: &str,
    commands: &[ProjectCommand],
) -> Result<ToolExecution, ExecutionRuntimeError> {
    use crate::orchestration::workspace_command::{self, Grant};
    let Some(grant) = Grant::parse(grant) else {
        return Err(ExecutionRuntimeError::WorkspaceGrantUnsupported);
    };
    workspace_command::workspace_tools(workspace.clone(), grant, commands)
        .map_err(ExecutionRuntimeError::ToolsUnavailable)
}

/// Why a Job's Project cannot be used to run it.
#[cfg(unix)]
#[derive(Debug, thiserror::Error)]
enum ProjectUnavailable {
    #[error("the Agent's Workspace tool grant needs a Project, and the Job has none")]
    Required,
    #[error("{0} no longer exists")]
    Missing(ProjectId),
    #[error("the Project's root no longer authorizes: {0}")]
    Root(#[from] crate::orchestration::workspace_read::WorkspaceError),
    #[error("the Project's root is no longer the directory it was created with")]
    RootChanged,
    #[error("the Project's root could not be checked: {0}")]
    Unchecked(&'static str),
    #[error(transparent)]
    Database(#[from] DatabaseError),
}

/// The Project of `job` as it is now, and the Workspace its root authorizes
/// now, or `None` for a Job without a Project. The root was resolved when the
/// Project was created; it is resolved again here, and a root that no longer
/// authorizes, or resolves anywhere else, is refused.
#[cfg(unix)]
async fn resolve_project(
    database: &DatabaseHandle,
    data_directory: &crate::data_directory::DataDirectory,
    job: &JobRecord,
) -> Result<
    Option<(
        ProjectRecord,
        crate::orchestration::workspace_read::Workspace,
    )>,
    ProjectUnavailable,
> {
    use crate::orchestration::blocking::{self, Kind};
    use crate::orchestration::workspace_read::Workspace;

    let Some(id) = database.turn_execution(job.id).await?.1 else {
        return Ok(None);
    };
    let project = database
        .project(id)
        .await?
        .ok_or(ProjectUnavailable::Missing(id))?;
    let root = std::path::PathBuf::from(&project.root);
    let data_directory = data_directory.clone();
    let workspace = blocking::run(Kind::File, move || {
        Workspace::authorize(&root, &data_directory)
    })
    .await
    .map_err(ProjectUnavailable::Unchecked)??;
    if workspace.root() != std::path::Path::new(&project.root) {
        return Err(ProjectUnavailable::RootChanged);
    }
    Ok(Some((project, workspace)))
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
    use crate::persistence::{
        AgentBehavior, AgentConfig, ConfigBody, NewConfig, NewExecutor, NewProject,
    };
    use crate::test_support::{
        data_directory, execution_configuration, on_system, temporary_directory, wait,
    };

    #[test]
    fn frozen_workspace_grants_ground_first_and_continued_chat_turns() {
        let (_data, directory) = data_directory("chat", "grounding");
        let (_root, root) = temporary_directory("chat", "grounding-workspace");
        let workspace = Workspace::authorize(&root, &directory).expect("authorize");
        let workspace_tools = WorkspaceTools::new(directory.clone());
        let worker = DatabaseWorker::start(&directory).expect("start");
        let handle = worker.handle();
        let project = wait(handle.create_project(NewProject {
            name: "grounding".into(),
            description: None,
            root: workspace.root().to_string_lossy().into_owned(),
            commands: Vec::new(),
        }))
        .expect("project");
        let mut configuration = execution_configuration(&handle);
        let model = wait(handle.config(configuration.llm_profile.into()))
            .expect("read")
            .expect("model");
        let ConfigBody::Model(mut model) = model.body else {
            panic!("model")
        };
        model
            .declared_capabilities
            .insert(crate::persistence::ModelCapability::Tools);
        wait(handle.update_config(
            configuration.llm_profile.into(),
            NewConfig {
                name: "fixture-model".into(),
                body: ConfigBody::Model(model.clone()),
            },
        ))
        .expect("model tools");
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
            dispatch_to: Vec::new(),
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
            let mut live = agent_body.clone();
            live.tool_grant = granted.then(|| "read".into());
            wait(handle.update_config(
                agent,
                NewConfig {
                    name: "chat".into(),
                    body: ConfigBody::Agent(live),
                },
            ))
            .expect("set grant before T1");
            let entry = crate::persistence::Entry::UserMessage {
                text: "objective".into(),
                attachments: Vec::new(),
            };
            let job = if let Some(previous) = previous {
                let conversation = wait(handle.job(previous))
                    .expect("read")
                    .expect("turn")
                    .conversation
                    .expect("conversation");
                wait(handle.append(conversation, entry, Some(configuration), None))
                    .expect("append")
                    .turn
            } else {
                wait(handle.open_conversation(
                    None,
                    crate::persistence::ConversationSpec {
                        name: None,
                        agent: Some(agent),
                        project: Some(project),
                        origin: crate::persistence::Origin::User,
                        keep_open: true,
                    },
                    entry,
                    configuration,
                    None,
                ))
                .expect("open")
                .1
                .turn
            };
            assert_eq!(
                wait(handle.job_configuration(job))
                    .expect("read frozen")
                    .expect("frozen")
                    .frozen
                    .agent
                    .as_ref()
                    .and_then(|agent| agent.tool_grant.as_deref()),
                granted.then_some("read")
            );
            wait(handle.claim_fixture(job, executor)).expect("claim");
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
            let expected_root = workspace.root().to_string_lossy().into_owned();
            provider_seam::install(endpoint, move |conversation| {
                let sent = serde_json::to_value(conversation).expect("json");
                let messages = sent["messages"].as_array().expect("messages");
                if granted {
                    assert_eq!(messages[0]["role"], "system");
                    let text = messages[0]["content"].as_str().expect("grounding");
                    assert!(text.contains(&expected_root));
                    assert!(text.contains("\"grounding\""));
                    assert!(text.contains("Every tool path is relative to this root"));
                    assert!(text.contains("cannot be reached"));
                    assert!(text.contains("instead of claiming you acted there"));
                    assert!(text.contains("Your grant allows: read."));
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
                    None,
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
                        dispatch_to: Vec::new(),
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

    #[test]
    fn a_job_whose_project_root_was_removed_fails_at_claim_and_runs_no_tool() {
        let (_data, directory) = data_directory("chat", "removed-root");
        let (_root, root) = temporary_directory("chat", "removed-root-workspace");
        let workspace = Workspace::authorize(&root, &directory).expect("authorize");
        let workspace_tools = WorkspaceTools::new(directory.clone());
        let worker = DatabaseWorker::start(&directory).expect("start");
        let handle = worker.handle();
        let project = wait(handle.create_project(NewProject {
            name: "removed".into(),
            description: None,
            root: workspace.root().to_string_lossy().into_owned(),
            commands: Vec::new(),
        }))
        .expect("project");
        let mut configuration = execution_configuration(&handle);
        let agent = wait(handle.create_config(NewConfig {
            name: "chat".into(),
            body: ConfigBody::Agent(AgentConfig {
                dispatch_to: Vec::new(),
                behavior: AgentBehavior::Chat,
                models: vec![configuration.llm_profile.into()],
                tool_grant: Some("read".into()),
                ..Default::default()
            }),
        }))
        .expect("agent");
        configuration.agent_profile = Some(agent.into());
        let executor = wait(handle.create_executor(NewExecutor {
            name: "local".into(),
            concurrency_limit: std::num::NonZeroU32::MIN,
            llm_transports: [crate::persistence::LlmTransport::OpenAiChatCompletions].into(),
        }))
        .expect("executor");
        let job = wait(handle.create_claimed_fixture(
            NewJob {
                process_grant: None,
                name: None,
                eligible_at: None,
                parent: handle.genesis(),
                project: Some(project),
                input: JobInputDraft {
                    objective: Some("objective".into()),
                    ..Default::default()
                },
                configuration: Some(configuration),
                previous: None,
            },
            executor,
        ))
        .expect("claim");
        std::fs::remove_dir_all(&root).expect("remove the root");

        let run_handle = handle.clone();
        let receipt = on_system(move || async move {
            run_started_provider_job(
                run_handle,
                ProviderTask {
                    workspace_tools: Some(workspace_tools),
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
                None,
            )
            .await
        })
        .expect("settled");
        assert_eq!(receipt.job, job);
        let record = wait(handle.job(job)).expect("read").expect("job");
        assert!(record.failed_at.is_some(), "the Job failed");
        assert!(
            wait(handle.tool_evidence(job, None, 10))
                .expect("evidence")
                .is_empty(),
            "no tool ran"
        );
        worker.shutdown().expect("shutdown");
    }

    #[test]
    fn grounding_includes_grant_and_agents_md_in_system_messages() {
        let (_data, directory) = data_directory("chat", "agents-md-grounding");
        let (_root, root) = temporary_directory("chat", "agents-md-workspace");
        std::fs::write(
            root.join("AGENTS.md"),
            "# Project Instructions\nFollow these rules.\n",
        )
        .expect("write AGENTS.md");
        let workspace = Workspace::authorize(&root, &directory).expect("authorize");
        let workspace_tools = WorkspaceTools::new(directory.clone());
        let worker = DatabaseWorker::start(&directory).expect("start");
        let handle = worker.handle();
        let project = wait(handle.create_project(NewProject {
            name: "agents-md".into(),
            description: None,
            root: workspace.root().to_string_lossy().into_owned(),
            commands: vec![ProjectCommand {
                name: "lint".into(),
                argv: vec!["cargo".into(), "clippy".into()],
            }],
        }))
        .expect("project");
        let mut configuration = execution_configuration(&handle);
        let model = wait(handle.config(configuration.llm_profile.into()))
            .expect("read")
            .expect("model");
        let ConfigBody::Model(model) = model.body else {
            panic!("model")
        };
        let endpoint = "https://agents-md-grounding.invalid/v1";
        wait(handle.update_config(
            model.connection_id,
            NewConfig {
                name: "agents-md provider".into(),
                body: ConfigBody::Connection(crate::persistence::ConnectionConfig {
                    protocol: crate::persistence::LlmTransport::OpenAiChatCompletions,
                    endpoint: Some(endpoint.into()),
                    credential_ref: None,
                }),
            },
        ))
        .expect("provider endpoint");

        for (grant, allowed_text) in [
            ("read", "read"),
            ("edit", "read and edit"),
            ("code", "read, edit and run commands"),
        ] {
            let agent_body = AgentConfig {
                dispatch_to: Vec::new(),
                instructions: Some("Agent instructions".into()),
                behavior: AgentBehavior::Chat,
                models: vec![configuration.llm_profile.into()],
                tool_grant: Some(grant.into()),
                ..Default::default()
            };
            let agent = wait(handle.create_config(NewConfig {
                name: format!("agent-{grant}"),
                body: ConfigBody::Agent(agent_body),
            }))
            .expect("agent");
            configuration.agent_profile = Some(agent.into());
            let executor = wait(handle.create_executor(NewExecutor {
                name: format!("executor-{grant}"),
                concurrency_limit: std::num::NonZeroU32::MIN,
                llm_transports: [crate::persistence::LlmTransport::OpenAiChatCompletions].into(),
            }))
            .expect("executor");
            let job = wait(handle.create_claimed_fixture(
                NewJob {
                    process_grant: None,
                    name: None,
                    eligible_at: None,
                    parent: handle.genesis(),
                    project: Some(project),
                    input: JobInputDraft {
                        objective: Some("test objective".into()),
                        ..Default::default()
                    },
                    configuration: Some(configuration),
                    previous: None,
                },
                executor,
            ))
            .expect("claim");
            let tools = workspace_tools.clone();
            let expected_root = workspace.root().to_string_lossy().into_owned();
            let run_handle = handle.clone();
            let allowed_text_owned = allowed_text.to_owned();
            provider_seam::install(endpoint, move |conversation| {
                let sent = serde_json::to_value(conversation).expect("json");
                let messages = sent["messages"].as_array().expect("messages");
                // 1. Grounding message
                assert_eq!(messages[0]["role"], "system");
                let grounding = messages[0]["content"].as_str().expect("grounding");
                assert!(grounding.contains(&expected_root));
                assert!(grounding.contains(&format!("Your grant allows: {allowed_text_owned}.")));
                assert!(grounding.contains("\"agents-md\""));
                // Only a grant that runs commands names them.
                for command in ["cargo_check", "cargo_test", "lint"] {
                    assert_eq!(
                        grounding.contains(command),
                        allowed_text_owned.contains("run")
                    );
                }
                // 2. Agent instructions
                assert_eq!(messages[1]["role"], "system");
                assert_eq!(messages[1]["content"], "Agent instructions");
                // 3. AGENTS.md as its own System message, never User
                assert_eq!(messages[2]["role"], "system");
                assert_eq!(
                    messages[2]["content"],
                    "# Project Instructions\nFollow these rules.\n"
                );
                // 4. User objective
                assert_eq!(messages[3]["role"], "user");
                assert_eq!(messages[3]["content"], "test objective");
                provider_seam::Response {
                    gate: None,
                    steps: vec![crate::provider_execution::scripted::Step::Chunk(
                        "data: {\"choices\":[{\"delta\":{\"content\":\"done\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n",
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
                    None,
                )
                .await
            })
            .expect("settled");
        }

        // Test AGENTS.md skip conditions: symlink, >32 KiB, non-UTF-8, NUL
        let test_workspace = workspace.clone();
        on_system(move || async move {
            // Valid content
            assert_eq!(
                read_agents_md(&test_workspace).await.as_deref(),
                Some("# Project Instructions\nFollow these rules.\n")
            );

            // NUL byte: skipped and logged
            std::fs::write(root.join("AGENTS.md"), "hello\0world").expect("write");
            assert_eq!(read_agents_md(&test_workspace).await, None);

            // Non-UTF-8: skipped and logged
            std::fs::write(root.join("AGENTS.md"), b"hello \xff\xfe world")
                .expect("write non-utf8");
            assert_eq!(read_agents_md(&test_workspace).await, None);
            std::fs::remove_file(root.join("AGENTS.md")).expect("remove non-utf8");

            // Symlink: skipped and logged
            #[cfg(unix)]
            {
                use std::os::unix::fs::symlink;
                std::fs::write(root.join("target.md"), "target").expect("write target");
                symlink(root.join("target.md"), root.join("AGENTS.md")).expect("symlink");
                assert_eq!(read_agents_md(&test_workspace).await, None);
                std::fs::remove_file(root.join("AGENTS.md")).expect("remove symlink");
                std::fs::remove_file(root.join("target.md")).expect("remove target");
            }

            // > 32 KiB: skipped and logged
            std::fs::write(root.join("AGENTS.md"), vec![b'a'; 33 * 1024]).expect("write large");
            assert_eq!(read_agents_md(&test_workspace).await, None);

            // Missing: skipped silently
            std::fs::remove_file(root.join("AGENTS.md")).expect("remove");
            assert_eq!(read_agents_md(&test_workspace).await, None);
        });

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
    notifications: flume::Sender<SchedulerMessage>,
    state: SharedState,
}

#[derive(Clone)]
pub(crate) struct StopHandle {
    notifications: flume::Sender<SchedulerMessage>,
    state: SharedState,
}
impl StopHandle {
    pub fn stop(&self, turn: JobId, cause: crate::persistence::StopCause) {
        let mut state = lock(&self.state);
        let stopped = state.stops.entry(turn).or_default();
        *stopped.lock().unwrap_or_else(PoisonError::into_inner) = Some(cause);
        if let Some(cancellation) = state.current.get(&turn) {
            cancellation.cancel();
        }
        drop(state);
        self.notifications.send(SchedulerMessage::Stop(turn)).ok();
    }
}

impl SchedulerHandle {
    #[cfg(test)]
    pub(crate) fn running_count(&self) -> usize {
        lock(&self.state).current.len()
    }
    pub fn stop(&self, turn: JobId, cause: crate::persistence::StopCause) {
        StopHandle {
            notifications: self.notifications.clone(),
            state: self.state.clone(),
        }
        .stop(turn, cause);
    }

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClaimScope {
    Stored,
    Process,
}

impl ExecutionRuntime {
    pub fn start(
        database: DatabaseHandle,
        scope: ClaimScope,
    ) -> Result<Self, ExecutionRuntimeError> {
        #[cfg(test)]
        if ntex::rt::System::try_current().is_none() {
            return crate::test_support::on_system(
                move || async move { Self::start(database, scope) },
            );
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
            scope,
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
                notifications: sender,
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

#[allow(clippy::too_many_arguments)]
async fn dispatch_runtime(
    database: DatabaseHandle,
    scope: ClaimScope,
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
                        match admit_provider_job(&database, &mut plan, submission).await {
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
                retry_unsettled(&database).await;
                match ntex::rt::spawn_blocking(blocking::retry_unproven_groups).await {
                    Ok(recovered) => unconfirmed_stopped.extend(recovered),
                    Err(error) => {
                        log::warn!("failed to retry unproven groups on blocking worker: {error}");
                    }
                }
                let mut retry_next = BTreeSet::new();
                for job in std::mem::take(&mut unconfirmed_stopped) {
                    let result = database
                        .confirm_job_stopped(job, crate::persistence::StopProof::Proof)
                        .await;
                    if retry_stop_confirmation(&result) {
                        retry_next.insert(job);
                    }
                    match result {
                        Ok(())
                        | Err(DatabaseError::Rejected(
                            Violation::StopAlreadyConfirmed(_)
                            | Violation::StopNeedsUnknownOutcome(_)
                            | Violation::StopNeedsOutcome(_),
                        )) => {}
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
                let claimed = if scope == ClaimScope::Stored {
                    async {
                        let clock = crate::storage::promotion_clock();
                        match database.recurrence_due(clock).await {
                            Ok(true) => {
                                if let Err(error) = database.boundary_b(clock).await {
                                    log::error!("recurrence promotion failed: {error}");
                                }
                            }
                            Ok(false) => {}
                            Err(error) => log::error!("recurrence due read failed: {error}"),
                        }
                        database.claim_batch().await
                    }
                    .await
                } else {
                    let mut grants = Vec::new();
                    let mut failure = None;
                    for execution in pending.values() {
                        match database
                            .claim_execution(crate::persistence::ClaimRequest {
                                job: execution.started.job,
                                allowed_pools: BTreeSet::from([execution.started.executor.into()]),
                            })
                            .await
                        {
                            Ok(Ok(_)) => grants.push(execution.started.job),
                            Ok(Err(_)) => {}
                            Err(error) => {
                                failure = Some(error);
                                break;
                            }
                        }
                    }
                    match failure {
                        // Granted claims are committed: dropping them would
                        // leave their Jobs claimed with no task.
                        Some(error) if !grants.is_empty() => {
                            log::warn!(
                                "process claim sweep interrupted after {} grants: {error}",
                                grants.len()
                            );
                            Ok(grants)
                        }
                        Some(error) => Err(error),
                        None => Ok(grants),
                    }
                };
                let batch = match claimed {
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
                // A committed Stop may reach us after Finished removed its task.
                // Keep the cause through a grant race, but do not retain terminal Jobs.
                if database
                    .job(job)
                    .await
                    .ok()
                    .flatten()
                    .is_some_and(|record| record.any_terminal())
                {
                    lock(&state).stops.remove(&job);
                }
            }
            SchedulerMessage::Finished(job) => {
                if let Some(task) = running.remove(&job) {
                    finish_grant(&database, job, task).await;
                }
                lock(&state).current.remove(&job);
                lock(&state).stops.remove(&job);
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
    retry_unsettled(&database).await;
    lock(&state).current.clear();
    lock(&state).stops.clear();
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

fn retry_stop_confirmation(result: &Result<(), DatabaseError>) -> bool {
    match result {
        Ok(()) => false,
        Err(DatabaseError::Rejected(violation)) => {
            matches!(violation, Violation::StopNeedsOutcome(_))
        }
        Err(_) => true,
    }
}

/// Codes on Failed results: provider_rejected (provider refusal), not_delivered
/// (delivery ruled out), build_failed (request construction), context_window_exceeded
/// (input budget), not_sent (pre-send refusal), tool_loop_stopped (known local stop),
/// completion_refused (local completion rejected), internal (other runtime failure).
const PROVIDER_REJECTED: &str = "provider_rejected";
const NOT_DELIVERED: &str = "not_delivered";
const BUILD_FAILED: &str = "build_failed";
const CONTEXT_WINDOW_EXCEEDED: &str = "context_window_exceeded";
const NOT_SENT: &str = "not_sent";
const TOOL_LOOP_STOPPED: &str = "tool_loop_stopped";
const COMPLETION_REFUSED: &str = "completion_refused";
const INTERNAL_FAILURE: &str = "internal";

pub(crate) fn claim_error_finalization(
    error: &ExecutionRuntimeError,
    tool_may_have_run: bool,
) -> JobFinalization {
    match error {
        ExecutionRuntimeError::NotSettled {
            source: DatabaseError::Rejected(violation),
            ..
        }
        | ExecutionRuntimeError::ToolLoopNotSettled {
            source: DatabaseError::Rejected(violation),
            ..
        } if !matches!(violation, Violation::JobAlreadyFinalized(_)) => {
            JobFinalization::Failed(FinalResult {
                summary: Some(violation.to_string()),
                code: Some(COMPLETION_REFUSED.to_owned()),
                goal_reached: Some(false),
                value: None,
            })
        }
        ExecutionRuntimeError::NotSettled { settlement, .. } => (**settlement).clone(),
        _ if tool_may_have_run => JobFinalization::Unknown,
        ExecutionRuntimeError::ToolLoopNotSettled { stop, .. } if stop.is_unknown() => {
            JobFinalization::Unknown
        }
        _ => JobFinalization::Failed(FinalResult {
            summary: Some(error.to_string()),
            code: Some(INTERNAL_FAILURE.to_owned()),
            goal_reached: Some(false),
            value: None,
        }),
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
    let outcome = claim_error_finalization(error, tool_may_have_run);
    let database = if matches!(
        error,
        ExecutionRuntimeError::NotSettled { source: DatabaseError::Rejected(violation), .. }
            | ExecutionRuntimeError::ToolLoopNotSettled { source: DatabaseError::Rejected(violation), .. }
            if !matches!(violation, Violation::JobAlreadyFinalized(_))
    ) {
        database.without_stop_cause()
    } else {
        database.clone()
    };
    match database.finalize_job(job, outcome.clone()).await {
        Ok(_) | Err(DatabaseError::Rejected(Violation::JobAlreadyFinalized(_))) => {}
        Err(failure) => {
            log::error!(
                "Job {job} error ({error}) could not be finalized: {failure}; the finalization is retained for retry"
            );
            retain_unsettled(database, job, outcome);
        }
    }
}

/// Finalizations that failed, kept until the database accepts them: a pure
/// low-space lock never reopens the database, so open reconcile would not
/// settle them.
static UNSETTLED: Mutex<Vec<(DatabaseHandle, JobId, JobFinalization)>> = Mutex::new(Vec::new());

fn retain_unsettled(database: DatabaseHandle, job: JobId, outcome: JobFinalization) {
    let mut unsettled = UNSETTLED.lock().unwrap_or_else(PoisonError::into_inner);
    if !unsettled
        .iter()
        .any(|(existing, retained, _)| existing.same_database(&database) && *retained == job)
    {
        unsettled.push((database, job, outcome));
    }
}

/// Retries every finalization retained for `owner`'s database once. A
/// rejected completion is downgraded to a failure, so a refused result
/// cannot be retried forever.
async fn retry_unsettled(owner: &DatabaseHandle) {
    let retained = {
        let mut unsettled = UNSETTLED.lock().unwrap_or_else(PoisonError::into_inner);
        let (mine, others) = std::mem::take(&mut *unsettled)
            .into_iter()
            .partition(|(database, _, _)| database.same_database(owner));
        *unsettled = others;
        mine
    };
    for (database, job, outcome) in retained {
        match database.finalize_job(job, outcome.clone()).await {
            Ok(_) | Err(DatabaseError::Rejected(Violation::JobAlreadyFinalized(_))) => {}
            Err(DatabaseError::ShuttingDown | DatabaseError::StorageUnavailable(_)) => {}
            Err(DatabaseError::Rejected(violation)) => {
                if matches!(outcome, JobFinalization::Completed(_)) {
                    let failed = JobFinalization::Failed(FinalResult {
                        summary: Some(violation.to_string()),
                        code: Some(COMPLETION_REFUSED.to_owned()),
                        goal_reached: Some(false),
                        value: None,
                    });
                    retain_unsettled(database.without_stop_cause(), job, failed);
                } else {
                    log::error!("Job {job} could not be finalized: {violation}");
                }
            }
            Err(failure) => {
                log::warn!("Job {job} finalization is still unsettled: {failure}");
                retain_unsettled(database, job, outcome);
            }
        }
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
    let cause = lock(state).stops.entry(job).or_default().clone();
    let database = database.with_stop_cause(cause.clone());
    if cause
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .is_some()
        && let Some(execution) = &execution
    {
        execution.cancellation.cancel();
    }
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
    let scheduler = StopHandle {
        notifications: sender.clone(),
        state: state.clone(),
    };
    let state = state.clone();
    let task = ntex::rt::spawn(async move {
        let _notify = notify;
        JobTask(Box::pin(async move {
            let tool_may_have_run = Cell::new(false);
            let result = async {
                let execution = match execution {
                    Some(execution) => execution,
                    None => {
                        let cancellation = CancellationToken::default();
                        {
                            let mut state = lock(&state);
                            if state.stopping
                                || state.stops.get(&job).is_some_and(|cause| {
                                    cause
                                        .lock()
                                        .unwrap_or_else(PoisonError::into_inner)
                                        .is_some()
                                })
                            {
                                cancellation.cancel();
                            }
                            state.current.insert(job, cancellation.clone());
                        }
                        let binding = database
                            .frozen_pool_binding(job)
                            .await?
                            .ok_or(ExecutionRuntimeError::MissingPoolBinding(job))?;
                        PendingExecution {
                            started: StartedJob {
                                job,
                                input: JobInputId::new(job.get()),
                                executor: binding.pool.into(),
                            },
                            execution: PendingPlan::Provider(
                                ProviderTask {
                                    workspace_tools: Some(WorkspaceTools::new(
                                        database.data_directory(),
                                    )),
                                    credential_resolver: database.credential_resolver(),
                                    provider_config: ProviderExecutionConfig {
                                        cancellation: cancellation.clone(),
                                        ..ProviderExecutionConfig::default()
                                    },
                                    tools: None,
                                },
                                None,
                            ),
                            cancellation,
                            done: CancellationToken::default(),
                            reply: None,
                        }
                    }
                };
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
                    let settlement = JobFinalization::Cancelled(FinalResult {
                        summary: Some("stopped before execution".into()),
                        code: None,
                        goal_reached: None,
                        value: None,
                    });
                    let result = database
                        .finalize_job(job, settlement.clone())
                        .await
                        .map_err(|source| ExecutionRuntimeError::NotSettled {
                            source,
                            observations: None,
                            settlement: Box::new(settlement),
                        })?;
                    return Ok(ExecutionReceipt {
                        job,
                        executor: started.executor,
                        input: started.input,
                        result,
                    });
                }
                match execution.execution {
                    PendingPlan::Plan(outcome, result) => {
                        let settlement = outcome.finalization(result);
                        let result = database
                            .finalize_job(job, settlement.clone())
                            .await
                            .map_err(|source| ExecutionRuntimeError::NotSettled {
                                source,
                                observations: None,
                                settlement: Box::new(settlement),
                            })?;
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
                            Some(scheduler),
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
        Some(submission) => {
            database
                .create_submitted_job(plan.job.clone(), submission)
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

pub(crate) const IMAGES_OMITTED_NOTICE: &str = "Images of earlier turns were omitted; a request carries at most 8 images and 16 MiB of images.";

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

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn assemble_conversation(
    frozen: &FrozenExecutionConfig,
    prior_turns: &[(Option<String>, Vec<ImageInput>, String)],
    omitted_notice: bool,
    current_objective: Option<&str>,
    current_images: &[ImageInput],
    project: Option<&ProjectRecord>,
    grant: Option<&str>,
    agents_md: Option<&str>,
    tools: Vec<ToolWire>,
) -> ChatConversation {
    let mut messages = system_messages(frozen, project, grant, agents_md, omitted_notice);

    for (turn_objective, turn_images, assistant_text) in prior_turns {
        if let Some(user_msg) = provider_execution::objective_conversation_with_images(
            frozen,
            turn_objective.as_deref(),
            turn_images,
        )
        .messages
        .pop()
        {
            messages.push(user_msg);
        }
        messages.push(ChatMessage::assistant(
            Some(assistant_text.clone()),
            Vec::new(),
        ));
    }

    if let Some(user_msg) = provider_execution::objective_conversation_with_images(
        frozen,
        current_objective,
        current_images,
    )
    .messages
    .pop()
    {
        messages.push(user_msg);
    }

    ChatConversation {
        messages,
        tools,
        ..Default::default()
    }
}

fn message_weights(message: &ChatMessage) -> (usize, usize) {
    let mut bytes: usize = 0;
    let mut images: usize = 0;

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
            bytes = bytes.saturating_add(call.function.name.len());
            bytes = bytes.saturating_add(call.function.arguments.len());
        }
    }

    (bytes, images)
}

fn tool_bytes(tool: &ToolWire) -> usize {
    serde_json::to_vec(tool).map_or(0, |v| v.len())
}

/// Estimates the token count of a conversation request under conservative assumptions:
/// `ceil(UTF-8 bytes / 3)` over all message text, tool arguments, tool results, and
/// offered tool definitions (serializing each [`ToolWire`]: name, description, and
/// parameters), plus each tool call's function name with its arguments, plus 1,600
/// tokens per image, plus `max_tokens` (or 4,096 when unset).
pub(crate) fn estimate_request_tokens(
    conversation: &ChatConversation,
    max_tokens: Option<u32>,
) -> u64 {
    let mut bytes: usize = 0;
    let mut images: usize = 0;

    for message in &conversation.messages {
        let (m_bytes, m_images) = message_weights(message);
        bytes = bytes.saturating_add(m_bytes);
        images = images.saturating_add(m_images);
    }

    for tool in &conversation.tools {
        bytes = bytes.saturating_add(tool_bytes(tool));
    }

    let text_tokens = bytes.div_ceil(3) as u64;
    let image_tokens = (images as u64).saturating_mul(1600);
    let output_tokens = max_tokens.map_or(4096, u64::from);
    text_tokens
        .saturating_add(image_tokens)
        .saturating_add(output_tokens)
}

pub(crate) const DEFAULT_CONTEXT_WINDOW: u32 = 128_000;

#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub(crate) struct ContextLimits {
    pub(crate) context_window: Option<u32>,
    pub(crate) max_tokens: Option<u32>,
}

impl ContextLimits {
    pub(crate) fn read_budget(self) -> usize {
        self.context_window
            .unwrap_or(DEFAULT_CONTEXT_WINDOW)
            .saturating_sub(self.max_tokens.unwrap_or(4096)) as usize
    }

    pub(crate) fn for_profile(config: &crate::persistence::LlmConfig) -> Self {
        Self {
            context_window: config.context_window,
            max_tokens: Some(
                config
                    .parameters
                    .max_completion_tokens
                    .or(config.parameters.max_tokens)
                    .unwrap_or(4096),
            ),
        }
    }
}

pub(crate) struct PriorTurnMessages {
    pub(crate) user: Option<ChatMessage>,
    pub(crate) assistant: Option<ChatMessage>,
}

#[cfg(unix)]
async fn read_agents_md(
    workspace: &crate::orchestration::workspace_read::Workspace,
) -> Option<String> {
    use crate::orchestration::blocking::{self, Kind};
    use crate::orchestration::workspace_read::{self, Refusal, Step};

    const MAX_AGENTS_MD_BYTES: usize = 32 * 1024;

    let workspace = workspace.clone();
    let result = blocking::run(Kind::File, move || -> Result<Option<String>, String> {
        let (parent, name) = match workspace.parent("AGENTS.md") {
            Ok(res) => res,
            Err(Refusal::NotFound) => return Ok(None),
            Err(e) => return Err(format!("failed to locate AGENTS.md: {e}")),
        };
        let file = match workspace_read::open_within(&parent, name, Step::File, MAX_AGENTS_MD_BYTES)
        {
            Ok(f) => f,
            Err(Refusal::NotFound) => return Ok(None),
            Err(e) => return Err(format!("failed to open AGENTS.md: {e}")),
        };
        let bytes = match workspace_read::read_bytes(&file, MAX_AGENTS_MD_BYTES) {
            Ok(b) => b,
            Err(Refusal::NotFound) => return Ok(None),
            Err(e) => return Err(format!("failed to read AGENTS.md: {e}")),
        };
        let text = match String::from_utf8(bytes) {
            Ok(t) => t,
            Err(e) => return Err(format!("AGENTS.md is not valid UTF-8: {e}")),
        };
        if text.contains('\0') {
            return Err("AGENTS.md contains a NUL byte".to_string());
        }
        Ok(Some(text))
    })
    .await;

    match result {
        Ok(Ok(content)) => content,
        Ok(Err(reason)) => {
            log::warn!("skipping AGENTS.md: {reason}");
            None
        }
        Err(e) => {
            log::warn!("skipping AGENTS.md: blocking task failed: {e}");
            None
        }
    }
}

fn system_messages(
    frozen: &FrozenExecutionConfig,
    project: Option<&ProjectRecord>,
    grant: Option<&str>,
    agents_md: Option<&str>,
    omitted_notice: bool,
) -> Vec<ChatMessage> {
    let mut messages = Vec::new();

    if let Some(project) = project {
        let command_names = crate::persistence::BUILT_IN_COMMANDS
            .into_iter()
            .chain(project.commands.iter().map(|command| command.name.as_str()))
            .collect::<Vec<_>>();
        messages.push(provider_execution::ground_project(
            &project.name,
            &project.root,
            grant,
            &command_names,
        ));
    }

    let mut agent_conv = provider_execution::objective_conversation(frozen, None);
    agent_conv.messages.pop();

    if omitted_notice {
        append_omitted_notice(&mut agent_conv);
    }

    messages.extend(agent_conv.messages);

    if let Some(agents_md) = agents_md.filter(|content| !content.is_empty()) {
        messages.push(ChatMessage::system(agents_md));
    }

    messages
}

fn assemble_final_conversation(
    system_messages: Vec<ChatMessage>,
    prior_turns: Vec<PriorTurnMessages>,
    current_message: Option<ChatMessage>,
    tools: Vec<ToolWire>,
) -> ChatConversation {
    let mut messages = system_messages;
    for turn in prior_turns {
        if let Some(user) = turn.user {
            messages.push(user);
        }
        if let Some(assistant) = turn.assistant {
            messages.push(assistant);
        }
    }
    if let Some(current) = current_message {
        messages.push(current);
    }
    ChatConversation {
        messages,
        tools,
        ..Default::default()
    }
}

/// What the context read already dropped or says, which the fitted request
/// must carry and count.
#[derive(Clone, Copy, Default)]
struct ContextNotices<'a> {
    images_omitted: bool,
    context_omitted: bool,
    tree_status: Option<&'a str>,
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn fit_conversation(
    frozen: &FrozenExecutionConfig,
    prior_turns: Vec<(Option<String>, Vec<ImageInput>, Option<String>)>,
    current_objective: Option<&str>,
    current_images: &[ImageInput],
    project: Option<&ProjectRecord>,
    grant: Option<&str>,
    agents_md: Option<&str>,
    tools: Vec<ToolWire>,
    context_limits: ContextLimits,
) -> Option<ChatConversation> {
    fit_with_notices(
        frozen,
        prior_turns,
        current_objective,
        current_images,
        project,
        grant,
        agents_md,
        tools,
        context_limits,
        ContextNotices::default(),
    )
}

#[allow(clippy::too_many_arguments)]
fn fit_with_notices(
    frozen: &FrozenExecutionConfig,
    prior_turns: Vec<(Option<String>, Vec<ImageInput>, Option<String>)>,
    current_objective: Option<&str>,
    current_images: &[ImageInput],
    project: Option<&ProjectRecord>,
    grant: Option<&str>,
    agents_md: Option<&str>,
    tools: Vec<ToolWire>,
    context_limits: ContextLimits,
    notices: ContextNotices<'_>,
) -> Option<ChatConversation> {
    // The system messages of the request, led by the tree status and the
    // images notice; the omitted notice is part of the system prompt.
    let with_prefix = |omitted: bool| {
        let mut system = Vec::new();
        if let Some(status) = notices.tree_status.filter(|status| !status.is_empty()) {
            system.push(ChatMessage::system(status));
        }
        if notices.images_omitted {
            system.push(ChatMessage::system(IMAGES_OMITTED_NOTICE));
        }
        system.extend(system_messages(frozen, project, grant, agents_md, omitted));
        system
    };
    // 1. Build messages once without per-attempt cloning.
    let prior_messages: Vec<PriorTurnMessages> = prior_turns
        .into_iter()
        .map(|(objective, images, assistant_text)| {
            let user = provider_execution::objective_conversation_with_images(
                frozen,
                objective.as_deref(),
                &images,
            )
            .messages
            .pop();
            let assistant =
                assistant_text.map(|text| ChatMessage::assistant(Some(text), Vec::new()));
            PriorTurnMessages { user, assistant }
        })
        .collect();

    let current_user_message = provider_execution::objective_conversation_with_images(
        frozen,
        current_objective,
        current_images,
    )
    .messages
    .pop();

    let Some(window) = context_limits.context_window else {
        return Some(assemble_final_conversation(
            with_prefix(notices.context_omitted),
            prior_messages,
            current_user_message,
            tools,
        ));
    };

    let system_without_notice = with_prefix(notices.context_omitted);
    let system_with_notice = with_prefix(true);

    // 2. Estimate token weights once for prior turns and the rest (system, notice, current turn, tools, max_tokens).
    let tools_bytes: usize = tools.iter().map(tool_bytes).sum();
    let (current_bytes, current_images_count) = current_user_message
        .as_ref()
        .map_or((0, 0), message_weights);

    let system_without_notice_bytes: usize = system_without_notice
        .iter()
        .map(|m| message_weights(m).0)
        .sum();
    let system_with_notice_bytes: usize = system_with_notice
        .iter()
        .map(|m| message_weights(m).0)
        .sum();

    let rest_without_notice_bytes = system_without_notice_bytes
        .saturating_add(current_bytes)
        .saturating_add(tools_bytes);
    let rest_with_notice_bytes = system_with_notice_bytes
        .saturating_add(current_bytes)
        .saturating_add(tools_bytes);

    let prior_weights: Vec<(usize, usize)> = prior_messages
        .iter()
        .map(|turn| {
            let (user_b, user_img) = turn.user.as_ref().map_or((0, 0), message_weights);
            let (asst_b, asst_img) = turn.assistant.as_ref().map_or((0, 0), message_weights);
            (
                user_b.saturating_add(asst_b),
                user_img.saturating_add(asst_img),
            )
        })
        .collect();

    let total_prior_bytes: usize = prior_weights.iter().map(|(b, _)| *b).sum();
    let total_prior_images: usize = prior_weights.iter().map(|(_, img)| *img).sum();

    let output_tokens = context_limits.max_tokens.map_or(4096, u64::from);

    // 3. Check if full conversation (no turns dropped, no notice) fits.
    let full_bytes = rest_without_notice_bytes.saturating_add(total_prior_bytes);
    let full_images = current_images_count.saturating_add(total_prior_images);
    let full_tokens = (full_bytes.div_ceil(3) as u64)
        .saturating_add((full_images as u64).saturating_mul(1600))
        .saturating_add(output_tokens);

    if full_tokens <= window as u64 {
        return Some(assemble_final_conversation(
            system_without_notice,
            prior_messages,
            current_user_message,
            tools,
        ));
    }

    // 4. Drop oldest turns until the sum fits.
    let total_priors = prior_messages.len();
    let mut remaining_prior_bytes = total_prior_bytes;
    let mut remaining_prior_images = total_prior_images;
    let mut selected_drop = None;

    for drop_count in 1..=total_priors {
        let (dropped_b, dropped_img) = prior_weights[drop_count - 1];
        remaining_prior_bytes = remaining_prior_bytes.saturating_sub(dropped_b);
        remaining_prior_images = remaining_prior_images.saturating_sub(dropped_img);

        let candidate_bytes = rest_with_notice_bytes.saturating_add(remaining_prior_bytes);
        let candidate_images = current_images_count.saturating_add(remaining_prior_images);
        let candidate_tokens = (candidate_bytes.div_ceil(3) as u64)
            .saturating_add((candidate_images as u64).saturating_mul(1600))
            .saturating_add(output_tokens);

        if candidate_tokens <= window as u64 {
            selected_drop = Some(drop_count);
            break;
        }
    }

    // 5. Assemble the final conversation once.
    let drop_count = selected_drop?;
    let mut kept_prior_messages = prior_messages;
    kept_prior_messages.drain(0..drop_count);

    let mut conversation = assemble_final_conversation(
        system_with_notice,
        kept_prior_messages,
        current_user_message,
        tools,
    );
    conversation.dropped_turns = drop_count as u32;
    Some(conversation)
}

/// Runs a provider plan whose Job was started to its settlement.
async fn run_started_provider_job(
    database: DatabaseHandle,
    mut plan: ProviderTask,
    (started, _offered): (StartedJob, Option<Vec<ToolWire>>),
    tool_may_have_run: &Cell<bool>,
    scheduler: Option<StopHandle>,
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
            code: Some(NOT_SENT.to_owned()),
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
    let mut project = None;
    let mut agents_md = None;
    let grant = configuration
        .frozen
        .agent
        .as_ref()
        .and_then(|agent| agent.tool_grant.as_deref());
    if let Some(tools) = &plan.workspace_tools {
        #[cfg(unix)]
        if grant.is_some() || job_record.kind.is_stored() {
            let resolved =
                match resolve_project(&database, tools.data_directory(), &job_record).await {
                    Ok(resolved) => resolved,
                    Err(ProjectUnavailable::Database(error)) => return Err(error.into()),
                    Err(error) => return refused(error.to_string()).await,
                };
            if let Some((record, workspace)) = resolved {
                if let Some(grant) = grant {
                    match build_tools(&workspace, grant, &record.commands) {
                        Ok(built) => plan.tools = Some(built),
                        Err(error) => return refused(error.to_string()).await,
                    }
                    agents_md = read_agents_md(&workspace).await;
                } else {
                    plan.tools = None;
                }
                project = grant.map(|_| record);
            } else if grant.is_some() {
                return refused(ProjectUnavailable::Required.to_string()).await;
            } else {
                plan.tools = None;
            }
        } else {
            plan.tools = None;
        }
        #[cfg(not(unix))]
        if grant.is_some() {
            return refused(ExecutionRuntimeError::WorkspaceGrantUnsupported.to_string()).await;
        }
    }
    let decider =
        job_record.kind.is_stored() && database.has_decider(job_record.conversation).await?;
    let dispatch = job_record.kind.is_stored()
        && configuration
            .frozen
            .agent
            .as_ref()
            .is_some_and(|agent| !agent.dispatch_to.is_empty());
    let ask_user = if let Some(conversation) = job_record.conversation {
        decider
            && database
                .job(conversation)
                .await?
                .is_some_and(|row| row.parent_id.is_some())
            && configuration
                .frozen
                .llm
                .config
                .declared_capabilities
                .contains(&crate::persistence::ModelCapability::Tools)
    } else {
        false
    };
    if dispatch || ask_user {
        plan.tools = match super::job_tools::add(plan.tools.take(), dispatch, ask_user) {
            Ok(tools) => Some(tools),
            Err(error) => return refused(error.to_string()).await,
        };
    }
    if let Some(tools) = &mut plan.tools {
        tools.approve = configuration
            .frozen
            .agent
            .as_ref()
            .map_or_else(Vec::new, |a| a.approve.clone());
        tools.decider = decider;
    }
    let offered = match plan
        .tools
        .as_ref()
        .map(|tools| tools.offer.wires())
        .transpose()
    {
        Ok(offered) => offered,
        Err(error) => return refused(error.to_string()).await,
    };
    let approved = match job_record.conversation {
        Some(conversation) => database.approved_calls(conversation).await?,
        None => Vec::new(),
    };
    let Some(input_record) = database.job_input(input).await? else {
        return refused("submitted Job input disappeared".to_owned()).await;
    };
    let context_limits = ContextLimits::for_profile(&configuration.frozen.llm.config);
    let mut prior_attachments = Vec::new();
    let mut prior_turns = Vec::new();
    let mut current_objective = input_record.objective.clone();
    let mut current_attachments = input_record.attachments.clone();
    let mut context_omitted = false;
    let mut images_omitted = false;
    let mut response_format = None;
    let mut tree_status = String::new();
    if job_record.conversation.is_some() {
        let budget = context_limits.read_budget();
        let mut context = database.read_turn_context(job, budget).await?;
        context_omitted = context.omitted;
        response_format = context.response_format;
        let mut bytes_left = crate::openai_compatible::image::MAX_REQUEST_IMAGE_BYTES as u64;
        tree_status = context.tree_status;
        if let Some(current) = context.turns.pop() {
            debug_assert_eq!(current.turn, job);
            current_objective = Some(format!("{}{}", context.attention, current.text));
            current_attachments = current.attachments;
            bytes_left = bytes_left.saturating_sub(current.attachment_bytes.iter().sum::<u64>());
        }
        let vision = configuration
            .frozen
            .llm
            .config
            .declared_capabilities
            .contains(&crate::persistence::ModelCapability::Vision);
        let mut images_left = if vision {
            crate::openai_compatible::image::MAX_REQUEST_IMAGES
                .saturating_sub(current_attachments.len())
        } else {
            0
        };
        let mut prior = Vec::new();
        for turn in context.turns.into_iter().rev() {
            let mut attachments = Vec::new();
            for (attachment, bytes) in turn.attachments.iter().zip(&turn.attachment_bytes).rev() {
                if images_left > 0 && *bytes <= bytes_left {
                    attachments.push(*attachment);
                    images_left -= 1;
                    bytes_left -= bytes;
                } else if vision {
                    images_omitted = true;
                }
            }
            attachments.reverse();
            prior.push((turn.text, attachments, turn.answer));
        }
        for (text, attachments, answer) in prior.into_iter().rev() {
            let images = attachment_images(&database, &attachments)
                .await
                .map_err(|error| ExecutionRuntimeError::Provider(error.to_string()))?;
            prior_attachments.push(attachments);
            prior_turns.push((Some(text), images, answer));
        }
    }
    let images = match attachment_images(&database, &current_attachments).await {
        Ok(images) => images,
        Err(error) => {
            return refused(format!("the input's attachments cannot be sent: {error}")).await;
        }
    };
    let context_limits = ContextLimits::for_profile(&configuration.frozen.llm.config);
    let tools_wire = offered.unwrap_or_default();

    let mut conversation = match fit_with_notices(
        &configuration.frozen,
        prior_turns,
        current_objective.as_deref(),
        &images,
        project.as_ref(),
        grant,
        agents_md.as_deref(),
        tools_wire,
        context_limits,
        ContextNotices {
            images_omitted,
            context_omitted,
            tree_status: Some(tree_status.as_str()),
        },
    ) {
        Some(conversation) => conversation,
        None => {
            let failed = FinalResult {
                summary: Some("the request exceeds the model's context window".to_owned()),
                code: Some(CONTEXT_WINDOW_EXCEEDED.to_owned()),
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
    };

    conversation.response_format = response_format.map(Into::into);
    let mut attachments = prior_attachments
        .into_iter()
        .skip(conversation.dropped_turns as usize)
        .chain([current_attachments]);
    for (index, message) in conversation.messages.iter().enumerate() {
        if message.role == ChatRole::User {
            conversation
                .image_attachments
                .insert(index, attachments.next().unwrap_or_default());
        }
    }
    let frozen = &configuration.frozen;
    let resolver = plan.credential_resolver.as_ref();
    let config = &plan.provider_config;
    run_provider_job_inner(
        ProviderRun {
            scheduler,
            database,
            executor,
            input,
            job,
        },
        conversation,
        plan.tools.as_ref(),
        &approved,
        context_limits,
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

    pub(crate) fn before_creation(name: Option<&str>) {
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
        let body = provider_execution::request_body(
            &frozen.llm.config.model,
            &frozen.llm.config.parameters,
            conversation,
        )?;
        super::ProviderOutputSink::sending(sink, &body)
            .await
            .map_err(|error| ProviderExecutionError::NotDelivered {
                message: error.message,
            })?;
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
    pub(crate) scheduler: Option<StopHandle>,
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
    run_provider_job_with_window(run, conversation, tools, None, None, cancellation, exchange).await
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
        &[],
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

#[allow(clippy::too_many_arguments)]
async fn run_provider_job_inner(
    run: ProviderRun,
    mut conversation: ChatConversation,
    tools: Option<&ToolExecution>,
    approved: &[crate::persistence::ToolEvidenceRecord],
    context_limits: ContextLimits,
    cancellation: &CancellationToken,
    tool_may_have_run: &Cell<bool>,
    mut exchange: impl AsyncFnMut(
        &ChatConversation,
        &mut DatabaseOutputSink,
    ) -> Result<ProviderExecutionResult, ProviderExecutionError>,
) -> Result<ExecutionReceipt, ExecutionRuntimeError> {
    let mut sink = output_sink(run.database.clone(), run.job);
    if conversation.response_format.is_none()
        && let Some(turn) = run.database.job(run.job).await?
        && turn.kind.is_stored()
        && turn.previous.is_none()
        && let Some(root) = turn.conversation
        && let Some(parent) = run.database.job(root).await?
        && parent.parent_id.is_none()
        && parent.name.is_none()
        && matches!(
            parent.kind,
            crate::persistence::JobKind::Conversation {
                origin: crate::persistence::Origin::User,
                ..
            }
        )
    {
        sink.title_conversation = Some(root);
        let before_user = conversation
            .messages
            .iter()
            .position(|message| message.role != ChatRole::System)
            .unwrap_or(conversation.messages.len());
        conversation.messages.insert(
            before_user,
            ChatMessage::system(super::conversation_title::INSTRUCTION),
        );
        conversation.image_attachments = conversation
            .image_attachments
            .into_iter()
            .map(|(index, attachments)| (index + usize::from(index >= before_user), attachments))
            .collect();
    }
    sink.recorder.image_attachments = std::mem::take(&mut conversation.image_attachments);
    sink.dropped_turns = conversation.dropped_turns;
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
    if !approved.is_empty() {
        let calls = approved
            .iter()
            .enumerate()
            .map(|(index, record)| CompletedToolCall {
                index: index as u32,
                id: format!(
                    "approval_{}_{}_{}",
                    record.call.job.get(),
                    record.call.request.get(),
                    record.call.index
                ),
                name: record.name.clone(),
                arguments: record
                    .decision
                    .fact
                    .stored_admission()
                    .map_or("{}", |a| a.arguments.as_str())
                    .to_owned(),
            })
            .collect::<Vec<_>>();
        let links = approved
            .iter()
            .map(|record| record.call)
            .collect::<Vec<_>>();
        let empty_tools = ToolExecution {
            offer: super::tools::ToolRegistry::default()
                .offer(&[])
                .map_err(ExecutionRuntimeError::ToolsUnavailable)?,
            approve: Vec::new(),
            decider: false,
            limits: ToolLoopLimits::default(),
        };
        let site = CallSite {
            scheduler: run.scheduler.as_ref(),
            database: &run.database,
            job: run.job,
            request: RequestNumber::FIRST,
        };
        let answers = match answer_calls(
            tools.unwrap_or(&empty_tools),
            &site,
            &conversation,
            &calls,
            &[],
            &links,
            &mut budget,
            cancellation,
            tool_may_have_run,
        )
        .await
        {
            Ok((answers, _)) => answers,
            Err(stopped) => return stop_tool_loop(run, sink, None, stopped).await,
        };
        conversation.messages.push(ChatMessage::assistant(
            None,
            calls.iter().map(ChatToolCallWire::from).collect(),
        ));
        for (call, answer) in calls.iter().zip(answers) {
            conversation
                .messages
                .push(ChatMessage::tool(call.id.clone(), answer));
        }
        sink.request = RequestNumber::FIRST
            .next()
            .ok_or(ExecutionRuntimeError::SchedulerStopped)?;
        if let Some(window) = context_limits.context_window
            && estimate_request_tokens(&conversation, context_limits.max_tokens) > window as u64
        {
            let stopped = Stopped::from(ToolLoopStop::ContextWindow);
            return stop_tool_loop(run, sink, None, stopped).await;
        }
    }
    let mut round = 1u32;
    loop {
        let mut retries = 0;
        let execution = loop {
            let execution = exchange(&conversation, &mut sink).await;
            let execution = if tools.is_none() {
                execution.and_then(ProviderExecutionResult::admitted)
            } else {
                execution
            };
            if let Ok(result) = &execution
                && let Some(root) = sink.title_conversation
                && let Some((title, _)) = super::conversation_title::split(&result.text)
                && let Err(error) = sink
                    .database
                    .set_conversation(
                        root,
                        crate::persistence::ConversationEdit::SuggestedTitle(title.to_owned()),
                    )
                    .await
            {
                sink.gap.get_or_insert(error);
            }
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
        let assistant = assistant_message(&result);
        record_answer(&mut sink, &result, &assistant).await;
        let next = sink
            .request
            .next()
            .filter(|_| tools.is_some_and(|tools| round < tools.limits.max_provider_requests));
        let awaits = tools.is_some_and(|tools| {
            tools.decider
                && result.tool_calls.iter().any(|call| {
                    !result
                        .refused
                        .iter()
                        .any(|refusal| refusal.index == Some(call.index))
                        && needs_decision(tools, &call.name)
                })
        });
        let answered = match tools {
            Some(tools) if next.is_some() || awaits => {
                let site = CallSite {
                    scheduler: run.scheduler.as_ref(),
                    database: &run.database,
                    job: run.job,
                    request: sink.request,
                };
                answer_calls(
                    tools,
                    &site,
                    &conversation,
                    &result.tool_calls,
                    &result.refused,
                    &[],
                    &mut budget,
                    cancellation,
                    tool_may_have_run,
                )
                .await
            }
            Some(_) => Err(ToolLoopStop::RequestLimit.into()),
            None => Err(ToolLoopStop::Refused(InvalidToolCall::new(
                ToolCallRefusal::Unauthorized,
                "the Provider requested tools that OCG did not offer",
            ))
            .into()),
        };
        let (answers, awaiting) = match answered {
            Ok(answered) => answered,
            Err(stopped) => {
                return stop_tool_loop(run, sink, Some(&result), stopped).await;
            }
        };
        if !awaiting.is_empty() {
            let mut summary = result.text.clone();
            for index in awaiting {
                if let Some(call) = result.tool_calls.iter().find(|call| call.index == index) {
                    summary.push('\n');
                    if call.name == "ask_user" {
                        let question = serde_json::from_str::<serde_json::Value>(&call.arguments)
                            .ok()
                            .and_then(|v| {
                                v.get("question")
                                    .and_then(|v| v.as_str())
                                    .map(str::to_owned)
                            })
                            .unwrap_or_default();
                        summary.push_str(&format!("Awaiting answer: {question}"));
                    } else {
                        summary.push_str(&format!(
                            "Awaiting approval: {} {}",
                            call.name, call.arguments
                        ));
                    }
                }
            }
            return complete_provider_job(
                run.database,
                run.executor,
                run.input,
                run.job,
                sink,
                &result,
                summary,
                None,
            )
            .await;
        }
        let Some(next) = next else {
            return stop_tool_loop(run, sink, Some(&result), ToolLoopStop::RequestLimit.into())
                .await;
        };
        conversation.messages.push(assistant);
        if let Some(digest) = sink.response_digest.take() {
            sink.recorder.remember_response(digest);
        }
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
    scheduler: Option<&'a StopHandle>,
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

fn needs_decision(tools: &ToolExecution, name: &str) -> bool {
    name == "ask_user" || ApproveClass::of(name).is_some_and(|class| tools.approve.contains(&class))
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
/// result, serialized, is the answer, and it is never truncated. A handler
/// failure known not to have applied is answered with its reason, so the
/// model may correct the call. Answerable provider refusals are recorded and
/// answered before continuing, count as calls, and are never resolved or
/// admitted. Failure to record a refusal stops before answering it.
#[allow(clippy::too_many_arguments)]
async fn answer_calls(
    tools: &ToolExecution,
    site: &CallSite<'_>,
    conversation: &ChatConversation,
    calls: &[CompletedToolCall],
    refused: &[InvalidToolCall],
    approvals: &[ToolCallRef],
    budget: &mut ToolBudget,
    cancellation: &CancellationToken,
    tool_may_have_run: &Cell<bool>,
) -> Result<(Vec<String>, Vec<u32>), Stopped> {
    let limits = &tools.limits;
    if cancellation.is_cancelled() {
        return Err(ToolLoopStop::Cancelled.into());
    }
    if approvals.is_empty() && budget.calls.saturating_add(calls.len()) > limits.max_tool_calls {
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
    for call in calls.iter().filter(|_| approvals.is_empty()) {
        if earlier.contains(call.id.as_str()) {
            return Err(ToolLoopStop::Refused(InvalidToolCall::at(
                ToolCallRefusal::ConflictingIdentity,
                call.index,
                format!("tool call {} repeats the id of an earlier call", call.index),
            ))
            .into());
        }
        if refused
            .iter()
            .any(|refusal| refusal.index == Some(call.index))
        {
            continue;
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
    let mut awaiting = Vec::new();
    for call in calls {
        if cancellation.is_cancelled() {
            return Err(ToolLoopStop::Cancelled.into());
        }
        if let Some(refusal) = refused
            .iter()
            .find(|refusal| refusal.index == Some(call.index))
        {
            record_refusal(site, call, refusal).await?;
            answers.push(
                serde_json::json!({"refused": refusal.kind.as_str(), "reason": refusal.message})
                    .to_string(),
            );
            budget.calls += 1;
            continue;
        }
        let approval = approvals.get(call.index as usize).copied();
        let resolved = if approval.is_some() && budget.calls >= limits.max_tool_calls {
            Err(InvalidToolCall::at(
                ToolCallRefusal::LimitExceeded,
                call.index,
                "tool call limit reached",
            ))
        } else if approval.is_some() && earlier.contains(call.id.as_str()) {
            Err(InvalidToolCall::at(
                ToolCallRefusal::ConflictingIdentity,
                call.index,
                "tool call id already used",
            ))
        } else if approval.is_some()
            && (call.arguments.len() > MAX_STORED_TOOL_ARGUMENTS_BYTES
                || budget.argument_bytes.saturating_add(call.arguments.len())
                    > MAX_STORED_TOOL_ARGUMENTS_PER_JOB_BYTES)
        {
            Err(InvalidToolCall::at(
                ToolCallRefusal::LimitExceeded,
                call.index,
                "arguments exceed the recording limit",
            ))
        } else {
            tools.offer.resolve(call)
        };
        let dispatch = match resolved {
            Ok(dispatch) => dispatch,
            Err(refusal) if approval.is_some() => {
                record_call_decision(site, call, ToolDecision::Refused(refusal.kind), approval)
                    .await?;
                answers.push(
                    serde_json::json!({"refused":refusal.kind.as_str(), "reason":refusal.message})
                        .to_string(),
                );
                continue;
            }
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
        if approval.is_none() && needs_decision(tools, &call.name) {
            if tools.decider {
                let decision = if call.name == "ask_user" {
                    ToolDecision::AwaitingAnswer(decided)
                } else {
                    ToolDecision::AwaitingApproval(decided)
                };
                record_call_decision(site, call, decision, None).await?;
                awaiting.push(call.index);
            } else {
                record_call_decision(
                    site,
                    call,
                    ToolDecision::Refused(ToolCallRefusal::NoDecider),
                    None,
                )
                .await?;
                answers.push(serde_json::json!({"refused":"no_decider", "reason":"the conversation has no deciding user"}).to_string());
            }
            budget.calls += 1;
            budget.argument_bytes = budget.argument_bytes.saturating_add(call.arguments.len());
            continue;
        }
        let admission = NewToolEvidence {
            approval,
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
                if approval.is_some() {
                    site.database
                        .record_tool_outcome(reference, outcome)
                        .await
                        .map_err(|error| Stopped {
                            stop: ToolLoopStop::EvidenceNotRecorded {
                                index,
                                outcome: Some(ToolOutcomeKind::NotInvoked),
                            },
                            evidence: Some(error),
                        })?;
                    answers.push(serde_json::json!({"refused":refusal.kind.as_str(), "reason":refusal.message}).to_string());
                    continue;
                }
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
        if dispatch.version.starts_with("job_") {
            let parsed = serde_json::from_value::<super::job_tools::JobToolCall>(
                serde_json::json!({"name":call.name,"arguments":dispatch.arguments}),
            );
            let applied = match parsed {
                Ok(tool) => {
                    let remaining = limits
                        .max_tool_results_bytes
                        .saturating_sub(budget.result_bytes);
                    site.database
                        .apply_job_tool_within(
                            reference,
                            tool,
                            remaining.min(limits.max_tool_result_bytes),
                        )
                        .await
                }
                Err(_) => Ok(
                    serde_json::json!({"refused":"invalid_arguments","reason":"job tool arguments do not match its schema"}),
                ),
            };
            let result = match applied {
                Ok(result) => result,
                Err(DatabaseError::Rejected(violation))
                    if !matches!(
                        violation,
                        Violation::NotExecutingLocally(_)
                            | Violation::ToolOutcomeAlreadyRecorded(_)
                            | Violation::ToolCallNotFound(_)
                            | Violation::ToolCallNotAdmitted(_)
                    ) =>
                {
                    let code = match &violation {
                        Violation::InvalidValue(code, _) => *code,
                        Violation::ParentClosed(_) => "parent_closed",
                        Violation::ToolsNotSupported => "tools_not_supported",
                        Violation::ProjectRequired => "project_required",
                        _ => "job_tool_refused",
                    };
                    serde_json::json!({"refused":code,"reason":violation.to_string()})
                }
                Err(error) => {
                    return Err(Stopped {
                        stop: ToolLoopStop::EvidenceNotRecorded {
                            index,
                            outcome: None,
                        },
                        evidence: Some(error),
                    });
                }
            };
            if let Some(reason) = result.get("refused").map(|_| result.to_string()) {
                if let Err(error) = site
                    .database
                    .record_tool_outcome(reference, ToolOutcome::NotApplied { message: reason })
                    .await
                {
                    return Err(Stopped {
                        stop: ToolLoopStop::EvidenceNotRecorded {
                            index,
                            outcome: None,
                        },
                        evidence: Some(error),
                    });
                }
            } else if let Some(scheduler) = site.scheduler
                && let Some(running) = result.get("running").and_then(serde_json::Value::as_array)
            {
                for turn in running {
                    if let Some(id) = turn
                        .as_str()
                        .and_then(|id| id.strip_prefix("job:"))
                        .and_then(|id| id.parse::<u64>().ok())
                    {
                        let cause = if call.name == "job_close" {
                            crate::persistence::StopCause::Closed
                        } else {
                            let Some(record) = result
                                .get("record_seq")
                                .and_then(serde_json::Value::as_u64)
                                .and_then(|record| u32::try_from(record).ok())
                            else {
                                continue;
                            };
                            crate::persistence::StopCause::Stopped(record)
                        };
                        scheduler.stop(JobId::new(id), cause);
                    }
                }
            }
            let answer = result.to_string();
            let total = budget.result_bytes.saturating_add(answer.len());
            if answer.len() > limits.max_tool_result_bytes || total > limits.max_tool_results_bytes
            {
                return Err(ToolLoopStop::ResultTooLarge { index }.into());
            }
            budget.result_bytes = total;
            answers.push(answer);
            continue;
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
                // A failure that changed nothing is the model's to correct:
                // its reason is the call's answer, and the loop goes on.
                FailureCertainty::NotApplied => {
                    let message = failure.message;
                    let outcome = ToolOutcome::NotApplied {
                        message: message.clone(),
                    };
                    // The reason is sent on only once it is recorded.
                    if let Err(error) = site.database.record_tool_outcome(reference, outcome).await
                    {
                        return Err(Stopped {
                            stop: ToolLoopStop::EvidenceNotRecorded {
                                index,
                                outcome: Some(ToolOutcomeKind::NotApplied),
                            },
                            evidence: Some(error),
                        });
                    }
                    answers.push(
                        serde_json::json!({"failed": "not_applied", "reason": message}).to_string(),
                    );
                    continue;
                }
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
    Ok((answers, awaiting))
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

async fn record_call_decision(
    site: &CallSite<'_>,
    call: &CompletedToolCall,
    decision: ToolDecision,
    approval: Option<ToolCallRef>,
) -> Result<(), Stopped> {
    let evidence = NewToolEvidence {
        call: site.call(call),
        approval,
        tool_call_id: call.id.clone(),
        name: call.name.clone(),
        spawns: false,
        decision,
    };
    match site.database.record_tool_decision(evidence).await {
        Ok(()) => Ok(()),
        Err(DatabaseError::Rejected(violation)) => Err(ToolLoopStop::AdmissionRefused {
            index: call.index,
            violation,
        }
        .into()),
        Err(error) => Err(Stopped {
            stop: ToolLoopStop::EvidenceNotRecorded {
                index: call.index,
                outcome: None,
            },
            evidence: Some(error),
        }),
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
    match record_refusal(site, call, &refusal).await {
        Ok(()) => ToolLoopStop::Refused(refusal).into(),
        Err(stopped) => stopped,
    }
}

async fn record_refusal(
    site: &CallSite<'_>,
    call: &CompletedToolCall,
    refusal: &InvalidToolCall,
) -> Result<(), Stopped> {
    site.database
        .record_tool_decision(NewToolEvidence {
            approval: None,
            call: site.call(call),
            tool_call_id: call.id.clone(),
            name: call.name.clone(),
            decision: ToolDecision::Refused(refusal.kind),
            spawns: false,
        })
        .await
        .map_err(|error| Stopped {
            stop: ToolLoopStop::Refused(refusal.clone()),
            evidence: Some(error),
        })
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
            code: Some(TOOL_LOOP_STOPPED.to_owned()),
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
fn assistant_message(result: &ProviderExecutionResult) -> ChatMessage {
    ChatMessage::assistant(
        Some(result.text.clone()).filter(|text| !text.is_empty()),
        result
            .tool_calls
            .iter()
            .map(ChatToolCallWire::from)
            .collect(),
    )
}

async fn record_answer(
    sink: &mut DatabaseOutputSink,
    result: &ProviderExecutionResult,
    assistant: &ChatMessage,
) {
    let response = match sink.recorder.response(assistant) {
        Ok(response) => response,
        Err(error) => {
            // ChatMessage serialization is infallible for the admitted wire types.
            unreachable!("assistant message could not be serialized: {error:?}");
        }
    };
    let digest = response.digest();
    let recorded = sink
        .database
        .record_provider_answer(crate::persistence::NewProviderAnswer {
            stat: NewAvailabilityStat {
                job: sink.job,
                request: sink.request,
                observed_at: Timestamp::now(),
                observation: provider_execution::success_observation(result),
            },
            measurements: provider_execution::metrics(result),
            response,
        })
        .await;
    match recorded {
        Ok(()) => sink.response_digest = Some(digest),
        Err(error) => {
            sink.gap.get_or_insert(error);
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn complete_provider_job(
    database: DatabaseHandle,
    executor: ExecutorId,
    input: JobInputId,
    job: JobId,
    mut sink: DatabaseOutputSink,
    result: &ProviderExecutionResult,
    summary: String,
    answer: Option<&ChatMessage>,
) -> Result<ExecutionReceipt, ExecutionRuntimeError> {
    let summary = if sink.title_conversation.is_some()
        && let Some((_, answer)) = super::conversation_title::split(&summary)
    {
        answer.to_owned()
    } else {
        summary
    };
    let mut gap = sink.gap.take();
    let mut refused = None;
    let mut settlement = JobFinalization::Completed(FinalResult {
        summary: Some(summary),
        code: None,
        goal_reached: None,
        value: Some(provider_execution::result_value(result)),
    });
    let settled = match database.finalize_job(job, settlement.clone()).await {
        Err(DatabaseError::Rejected(violation))
            if !matches!(violation, Violation::JobAlreadyFinalized(_)) =>
        {
            settlement = JobFinalization::Failed(FinalResult {
                summary: Some(format!(
                    "the Provider answered, but the Job cannot complete: {violation}"
                )),
                code: Some(COMPLETION_REFUSED.to_owned()),
                goal_reached: Some(false),
                value: Some(provider_execution::result_value(result)),
            });
            let settled = database
                .without_stop_cause()
                .finalize_job(job, settlement.clone())
                .await;
            refused = Some(violation);
            settled
        }
        settled => settled,
    };
    if let Some(answer) = answer {
        sink.database = database.clone();
        sink.job = job;
        record_answer(&mut sink, result, answer).await;
        if let Some(error) = sink.gap.take() {
            gap.get_or_insert(error);
        }
    }
    let final_output = match settled {
        Ok(final_output) => final_output,
        Err(source) => {
            return Err(ExecutionRuntimeError::NotSettled {
                source,
                observations: gap.map(Box::new),
                settlement: Box::new(settlement),
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
    match gap {
        None => Ok(receipt),
        Some(source) => Err(ExecutionRuntimeError::ObservationsNotRecorded {
            receipt: Box::new(receipt),
            source,
        }),
    }
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
    mut sink: DatabaseOutputSink,
    execution: Result<ProviderExecutionResult, ProviderExecutionError>,
) -> Result<ExecutionReceipt, ExecutionRuntimeError> {
    let request = sink.request;
    match execution {
        Ok(result) => {
            let answer = assistant_message(&result);
            complete_provider_job(
                database,
                executor,
                input,
                job,
                sink,
                &result,
                result.text.clone(),
                Some(&answer),
            )
            .await
        }
        Err(error) => {
            let mut gap = sink.gap.take();
            let receipt =
                match settle_provider_error(database, executor, input, job, request, error).await {
                    Ok(receipt) => receipt,
                    Err(ExecutionRuntimeError::ObservationsNotRecorded { receipt, source }) => {
                        gap.get_or_insert(source);
                        *receipt
                    }
                    Err(ExecutionRuntimeError::NotSettled {
                        source,
                        observations: None,
                        settlement,
                    }) => {
                        return Err(ExecutionRuntimeError::NotSettled {
                            source,
                            observations: gap.map(Box::new),
                            settlement,
                        });
                    }
                    Err(error) => return Err(error),
                };
            match gap {
                None => Ok(receipt),
                Some(source) => Err(ExecutionRuntimeError::ObservationsNotRecorded {
                    receipt: Box::new(receipt),
                    source,
                }),
            }
        }
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
    recorder: provider_execution::RequestRecorder,
    dropped_turns: u32,
    response_digest: Option<crate::persistence::ContentDigest>,
    title_conversation: Option<JobId>,
}

pub(crate) fn output_sink(database: DatabaseHandle, job: JobId) -> DatabaseOutputSink {
    DatabaseOutputSink {
        database,
        job,
        request: RequestNumber::FIRST,
        gap: None,
        recorder: provider_execution::RequestRecorder::default(),
        dropped_turns: 0,
        response_digest: None,
        title_conversation: None,
    }
}

impl ProviderOutputSink for DatabaseOutputSink {
    fn sending<'a>(
        &'a mut self,
        body: &'a serde_json::Value,
    ) -> Pin<Box<dyn Future<Output = Result<(), OutputSinkError>> + 'a>> {
        Box::pin(async move {
            let (sent, state) =
                self.recorder
                    .prepare(self.job, self.request, body, self.dropped_turns)?;
            if let Err(error) = self.database.record_sent_request(sent).await {
                let message = format!("database could not persist provider request: {error}");
                self.gap.get_or_insert(error);
                return Err(OutputSinkError { message });
            }
            self.recorder.remember_sent(state);
            Ok(())
        })
    }

    fn append<'a>(
        &'a mut self,
        kind: IntermediateOutputKind,
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
                        kind,
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
    let failure = match &error {
        ProviderExecutionError::Rejected { failure } => Some((
            PROVIDER_REJECTED,
            Some(serde_json::json!({
                "status_code": failure.status_code,
                "provider_code": failure.provider_code,
                "request_id": failure.request_id,
                "retryable": failure.retryable,
            })),
        )),
        ProviderExecutionError::NotDelivered { .. } => Some((NOT_DELIVERED, None)),
        ProviderExecutionError::Build { .. } => Some((BUILD_FAILED, None)),
        ProviderExecutionError::Unknown { .. }
        | ProviderExecutionError::Incomplete { .. }
        | ProviderExecutionError::Interrupted { .. }
        | ProviderExecutionError::Stopped { .. }
        | ProviderExecutionError::OutputPersistence { .. } => None,
    };
    let settlement = match failure {
        None => JobFinalization::Unknown,
        Some((code, value)) => JobFinalization::Failed(FinalResult {
            summary: Some(error.to_string()),
            code: Some(code.to_owned()),
            goal_reached: Some(false),
            value,
        }),
    };
    let observed_at = Timestamp::now();
    let mut gap = None;
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
    let result = match database.finalize_job(job, settlement.clone()).await {
        Ok(result) => result,
        Err(source) => {
            return Err(ExecutionRuntimeError::NotSettled {
                source,
                observations: None,
                settlement: Box::new(settlement),
            });
        }
    };
    let receipt = ExecutionReceipt {
        executor,
        input,
        job,
        result,
    };
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
    fn stop_confirmation_retries_only_pending_outcomes_and_database_failures() {
        let job = JobId::new(1);
        for (result, expected) in [
            (Ok(()), false),
            (
                Err(DatabaseError::Rejected(Violation::StopNeedsOutcome(job))),
                true,
            ),
            (
                Err(DatabaseError::Rejected(Violation::StopNeedsUnknownOutcome(
                    job,
                ))),
                false,
            ),
            (
                Err(DatabaseError::Rejected(Violation::StopAlreadyConfirmed(
                    job,
                ))),
                false,
            ),
            (Err(DatabaseError::QueueFull), true),
            (
                Err(DatabaseError::OutcomeUnknown {
                    operation: crate::persistence::OperationId::new(1, 1),
                    cause: crate::database::UnknownCause::WorkerStopped,
                }),
                true,
            ),
        ] {
            assert_eq!(retry_stop_confirmation(&result), expected, "{result:?}");
        }
    }

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
            sink.append(IntermediateOutputKind::Progress, "")
                .await
                .expect("skip empty");
            let outputs = database.job_outputs(job, None, 10).await.expect("outputs");
            assert_eq!(outputs.len(), 2);
            assert_eq!(outputs[0].summary.as_deref(), Some(prefix.as_str()));
            assert_eq!(outputs[1].summary.as_deref(), Some(" \n\n"));
        });
        worker.shutdown().expect("shutdown");
    }

    fn spec(database: &DatabaseHandle) -> NewJob {
        NewJob {
            process_grant: None,
            name: Some("failure fixture".into()),
            eligible_at: None,
            parent: database.genesis(),
            project: None,
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
        let mut tools = crate::orchestration::workspace_command::workspace_tools(
            crate::orchestration::workspace_read::Workspace::authorize(&root, &directory)
                .expect("workspace"),
            crate::orchestration::workspace_command::Grant::Read,
            &[],
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
                        scheduler: None,
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
    fn permanent_finalization_refusal_fails_once_and_releases_the_slot() {
        let (_root, directory) = data_directory("dispatcher-errors", "permanent-finalization");
        let worker = DatabaseWorker::start(&directory).expect("database");
        let database = worker.handle();
        let executor = executor(&database);
        let job = wait(database.create_claimed_fixture(spec(&database), executor)).expect("claim");
        on_system(move || async move {
            let error = ExecutionRuntimeError::NotSettled {
                source: DatabaseError::Rejected(Violation::InvalidValue(
                    "result",
                    "permanently invalid",
                )),
                observations: None,
                settlement: Box::new(JobFinalization::Completed(FinalResult {
                    summary: Some("invalid".into()),
                    code: None,
                    goal_reached: None,
                    value: None,
                })),
            };
            finalize_claim_error(&database, job, &error, false).await;
            assert!(
                database
                    .job(job)
                    .await
                    .expect("read")
                    .expect("job")
                    .failed_at
                    .is_some()
            );
            assert!(database.occupancy().await.expect("slots").is_empty());
            assert!(
                database
                    .job_outputs(job, None, 20)
                    .await
                    .expect("output")
                    .last()
                    .expect("result")
                    .summary
                    .as_ref()
                    .expect("summary")
                    .contains("permanently invalid")
            );
        });
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
            assert!(
                database
                    .claim_execution(crate::persistence::ClaimRequest {
                        job: stopped,
                        allowed_pools: BTreeSet::from([executor.into()])
                    })
                    .await
                    .expect("process claim")
                    .is_ok()
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
    fn k1_round2_joined_image_limit_and_nonvision_follow_up() {
        use crate::persistence::{
            ConfigBody, ConversationSpec, Entry, ModelCapability, NewAttachment, NewConfig, Origin,
        };
        let (_root, directory) = data_directory("dispatcher-errors", "round2-images");
        let worker = DatabaseWorker::start(&directory).expect("database");
        let database = worker.handle();
        let pool = executor(&database);
        let configuration = execution_configuration(&database);
        let ConfigBody::Model(mut model) = wait(database.config(configuration.llm_profile.into()))
            .expect("read")
            .expect("model")
            .body
        else {
            panic!("model")
        };
        let endpoint = "https://round2-images.invalid/v1";
        wait(database.update_config(
            model.connection_id,
            NewConfig {
                name: "test connection".into(),
                body: ConfigBody::Connection(crate::persistence::ConnectionConfig {
                    protocol: LlmTransport::OpenAiChatCompletions,
                    endpoint: Some(endpoint.into()),
                    credential_ref: None,
                }),
            },
        ))
        .expect("endpoint");
        model.declared_capabilities.insert(ModelCapability::Vision);
        wait(database.update_config(
            configuration.llm_profile.into(),
            NewConfig {
                name: "vision".into(),
                body: ConfigBody::Model(model.clone()),
            },
        ))
        .expect("vision");
        let attachment = wait(database.create_user_attachment(NewAttachment {
            content: crate::openai_compatible::image::tests::png(),
            mime: "image/png".into(),
            display_name: None,
        }))
        .expect("image");
        let (conversation, first) = wait(database.open_conversation(
            None,
            ConversationSpec {
                name: None,
                agent: None,
                project: None,
                origin: Origin::User,
                keep_open: true,
            },
            Entry::UserMessage {
                text: "images".into(),
                attachments: vec![attachment; 8],
            },
            configuration,
            None,
        ))
        .expect("open");
        assert!(matches!(
            wait(database.append(
                conversation,
                Entry::UserMessage {
                    text: "more images".into(),
                    attachments: vec![attachment; 8]
                },
                None,
                None
            )),
            Err(DatabaseError::Rejected(Violation::InvalidValue(
                "too_many_images",
                _
            )))
        ));
        wait(database.claim_fixture(first.turn, pool)).expect("claim");
        wait(database.finalize_job(
            first.turn,
            JobFinalization::Completed(FinalResult {
                summary: Some("first answer".into()),
                code: None,
                goal_reached: None,
                value: None,
            }),
        ))
        .expect("complete");
        model.declared_capabilities.clear();
        wait(database.update_config(
            configuration.llm_profile.into(),
            NewConfig {
                name: "text only".into(),
                body: ConfigBody::Model(model),
            },
        ))
        .expect("text model");
        let next = wait(database.append(
            conversation,
            Entry::UserMessage {
                text: "text follow up".into(),
                attachments: vec![],
            },
            Some(configuration),
            None,
        ))
        .expect("follow up");
        provider_seam::install(endpoint, |conversation| {
            assert!(
                conversation
                    .messages
                    .iter()
                    .all(|message| message_weights(message).1 == 0)
            );
            assert!(conversation.image_attachments.values().all(Vec::is_empty));
            provider_seam::Response {
                gate: None,
                steps: vec![crate::provider_execution::scripted::Step::Chunk(
                    "data: {\"choices\":[{\"delta\":{\"content\":\"text answer\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n",
                )],
            }
        });
        on_system(move || async move {
            let runtime =
                ExecutionRuntime::start(database.clone(), ClaimScope::Stored).expect("runtime");
            ntex::time::timeout(Duration::from_secs(5), async {
                while !database
                    .job(next.turn)
                    .await
                    .expect("read")
                    .expect("turn")
                    .any_terminal()
                {
                    ntex::time::sleep(Duration::from_millis(2)).await;
                }
            })
            .await
            .expect("finished");
            assert!(
                database
                    .job(next.turn)
                    .await
                    .expect("read")
                    .expect("turn")
                    .completed_at
                    .is_some()
            );
            runtime.shutdown().await.expect("shutdown");
        });
        worker.shutdown().expect("shutdown");
    }

    #[test]
    fn stored_turn_runs_after_reopen_and_process_scope_leaves_it_pending() {
        use crate::persistence::{ConversationSpec, Entry, NewProject, Origin};
        let (_root, directory) = data_directory("dispatcher-errors", "stored-restart");
        let (_workspace, root) =
            crate::test_support::temporary_directory("dispatcher-errors", "stored-project");
        let worker = DatabaseWorker::start(&directory).expect("database");
        let database = worker.handle();
        let pool = executor(&database);
        let configuration = execution_configuration(&database);
        let model = wait(database.config(configuration.llm_profile.into()))
            .expect("read")
            .expect("model");
        let crate::persistence::ConfigBody::Model(model) = model.body else {
            panic!("model");
        };
        wait(database.update_config(
            model.connection_id,
            crate::persistence::NewConfig {
                name: "stored provider".into(),
                body: crate::persistence::ConfigBody::Connection(
                    crate::persistence::ConnectionConfig {
                        protocol: crate::persistence::LlmTransport::OpenAiChatCompletions,
                        endpoint: Some("https://stored-restart.invalid/v1".into()),
                        credential_ref: None,
                    },
                ),
            },
        ))
        .expect("test endpoint");
        let project = wait(database.create_project(NewProject {
            name: "project".into(),
            description: None,
            root: root.to_string_lossy().into_owned(),
            commands: Vec::new(),
        }))
        .expect("project");
        let (_, appended) = wait(database.open_conversation(
            None,
            ConversationSpec {
                name: None,
                agent: None,
                project: Some(project),
                origin: Origin::User,
                keep_open: true,
            },
            Entry::UserMessage {
                text: "survive restart".into(),
                attachments: Vec::new(),
            },
            configuration,
            None,
        ))
        .expect("stored turn");
        let frozen = wait(database.job_configuration(appended.turn))
            .expect("configuration")
            .expect("frozen")
            .frozen;
        let endpoint = frozen
            .llm
            .provider
            .endpoint
            .unwrap_or_else(|| "https://api.openai.com/v1/chat/completions".into());
        worker.shutdown().expect("close before claim");
        let worker = DatabaseWorker::start(&directory).expect("reopen");
        let database = worker.handle();
        let process_database = database.clone();
        on_system(move || async move {
            let runtime = ExecutionRuntime::start(process_database.clone(), ClaimScope::Process)
                .expect("process runtime");
            let created = runtime
                .scheduler()
                .schedule(ExecutionPlan {
                    executor_id: pool,
                    input: JobInputDraft::default(),
                    job: spec(&process_database),
                    outcome: JobOutcome::Completed,
                    result: FinalResult {
                        summary: Some("process".into()),
                        code: None,
                        goal_reached: None,
                        value: None,
                    },
                })
                .await
                .expect("process admission");
            ntex::time::timeout(Duration::from_secs(5), async {
                while !process_database
                    .job(created.job)
                    .await
                    .expect("read")
                    .expect("process")
                    .any_terminal()
                {
                    ntex::time::sleep(Duration::from_millis(2)).await;
                }
            })
            .await
            .expect("process finished");
            assert!(
                process_database
                    .job(appended.turn)
                    .await
                    .expect("read")
                    .expect("stored")
                    .awaits_start()
            );
            runtime.shutdown().await.expect("shutdown process");
        });
        worker.shutdown().expect("reopen between scopes");
        let worker = DatabaseWorker::start(&directory).expect("stored reopen");
        let database = worker.handle();
        provider_seam::install(&endpoint, |conversation| {
            assert!(
                serde_json::to_string(conversation)
                    .expect("request")
                    .contains("survive restart")
            );
            provider_seam::Response {
                gate: None,
                steps: vec![crate::provider_execution::scripted::Step::Chunk(
                    "data: {\"choices\":[{\"delta\":{\"content\":\"restarted answer\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n",
                )],
            }
        });
        on_system(move || async move {
            let runtime = ExecutionRuntime::start(database.clone(), ClaimScope::Stored)
                .expect("stored runtime");
            ntex::time::timeout(Duration::from_secs(5), async {
                while !database
                    .job(appended.turn)
                    .await
                    .expect("read")
                    .expect("stored")
                    .any_terminal()
                {
                    ntex::time::sleep(Duration::from_millis(2)).await;
                }
            })
            .await
            .expect("stored finished");
            assert!(
                database
                    .job(appended.turn)
                    .await
                    .expect("read")
                    .expect("stored")
                    .completed_at
                    .is_some()
            );
            runtime.shutdown().await.expect("shutdown stored");
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
            notifications: flume::unbounded().0,
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
    #[test]
    fn admitted_dispatch_crash_reopens_not_applied_without_a_child() {
        use crate::persistence::{
            AgentConfig, ConfigBody, ConversationSpec, Entry, ModelCapability, NewConfig, Origin,
        };
        use crate::test_support::on_system;
        let (_root, directory) = data_directory("dispatcher-errors", "dispatch-crash");
        let worker = DatabaseWorker::start(&directory).unwrap();
        let database = worker.handle();
        let pool = executor(&database);
        let mut configuration = execution_configuration(&database);
        let record = wait(database.config(configuration.llm_profile.into()))
            .unwrap()
            .unwrap();
        let ConfigBody::Model(mut model) = record.body else {
            panic!("model");
        };
        model.declared_capabilities.insert(ModelCapability::Tools);
        let endpoint = "https://dispatch-crash.invalid/v1";
        wait(database.update_config(
            model.connection_id,
            NewConfig {
                name: "crash provider".into(),
                body: ConfigBody::Connection(crate::persistence::ConnectionConfig {
                    protocol: LlmTransport::OpenAiChatCompletions,
                    endpoint: Some(endpoint.into()),
                    credential_ref: None,
                }),
            },
        ))
        .unwrap();
        wait(database.update_config(
            record.id,
            NewConfig {
                name: "tool model".into(),
                body: ConfigBody::Model(model),
            },
        ))
        .unwrap();
        let target = wait(database.create_config(NewConfig {
            name: "target".into(),
            body: ConfigBody::Agent(AgentConfig {
                models: vec![configuration.llm_profile.into()],
                default_model: Some(configuration.llm_profile.into()),
                ..Default::default()
            }),
        }))
        .unwrap();
        let dispatcher = wait(database.create_config(NewConfig {
            name: "dispatcher".into(),
            body: ConfigBody::Agent(AgentConfig {
                models: vec![configuration.llm_profile.into()],
                dispatch_to: vec![target],
                ..Default::default()
            }),
        }))
        .unwrap();
        configuration.agent_profile = Some(dispatcher.into());
        let (conversation, appended) = wait(database.open_conversation(
            None,
            ConversationSpec {
                name: None,
                agent: Some(dispatcher),
                project: None,
                origin: Origin::User,
                keep_open: true,
            },
            Entry::UserMessage {
                text: "dispatch".into(),
                attachments: vec![],
            },
            configuration,
            None,
        ))
        .unwrap();
        wait(database.claim_fixture(appended.turn, pool)).unwrap();
        let chunk = format!(
            "data: {}\n\ndata: [DONE]\n\n",
            serde_json::json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"dispatch","type":"function","function":{"name":"job_dispatch","arguments":serde_json::json!({"agent":crate::persistence::ProfileId::from(target).to_string(),"objective":"child"}).to_string()}}]},"finish_reason":"tool_calls"}]})
        );
        let chunk: &'static str = Box::leak(chunk.into_boxed_str());
        provider_seam::install(endpoint, move |request| {
            assert!(
                request
                    .tools
                    .iter()
                    .any(|tool| tool.function.name == "job_dispatch")
            );
            provider_seam::Response {
                gate: None,
                steps: vec![crate::provider_execution::scripted::Step::Chunk(chunk)],
            }
        });
        let run_database = database.clone();
        on_system(move || async move {
            struct Crash<F>(Pin<Box<F>>);
            impl<F: Future> Future for Crash<F> {
                type Output = bool;
                fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<bool> {
                    match catch_unwind(AssertUnwindSafe(|| self.0.as_mut().poll(context))) {
                        Ok(Poll::Pending) => Poll::Pending,
                        Ok(Poll::Ready(_)) => Poll::Ready(false),
                        Err(_) => Poll::Ready(true),
                    }
                }
            }
            dispatch_seam::install(move |point, call| {
                if call.job == appended.turn && point == dispatch_seam::Point::Admitted {
                    panic!("host crash at admitted dispatch");
                }
            });
            let possible_effect = Cell::new(false);
            let crashed = Crash(Box::pin(run_started_provider_job(
                run_database,
                ProviderTask {
                    workspace_tools: None,
                    credential_resolver: Arc::new(EnvironmentCredentialResolver),
                    provider_config: ProviderExecutionConfig::default(),
                    tools: None,
                },
                (
                    StartedJob {
                        job: appended.turn,
                        input: JobInputId::new(appended.turn.get()),
                        executor: pool,
                    },
                    None,
                ),
                &possible_effect,
                None,
            )))
            .await;
            assert!(crashed);
        });
        let evidence = wait(database.tool_evidence(appended.turn, None, 10)).unwrap();
        assert_eq!(evidence.len(), 1);
        assert!(evidence[0].outcome.is_none());
        assert!(
            wait(database.conversation_children(conversation, None, 10))
                .unwrap()
                .is_empty()
        );
        worker.shutdown().unwrap();
        let worker = DatabaseWorker::start(&directory).unwrap();
        let database = worker.handle();
        let evidence = wait(database.tool_evidence(appended.turn, None, 10)).unwrap();
        let reconciliation = &evidence[0].reconciliation.as_ref().unwrap().fact;
        assert_eq!(
            reconciliation.conclusion,
            crate::persistence::EffectConclusion::NotApplied
        );
        assert_eq!(reconciliation.reconciler, "open");
        assert!(!evidence[0].is_unresolved());
        assert!(
            wait(database.conversation_children(conversation, None, 10))
                .unwrap()
                .is_empty()
        );
        worker.shutdown().unwrap();
    }
}

#[cfg(test)]
mod provider_record_tests {
    use super::*;
    use crate::database::DatabaseWorker;
    use crate::openai_compatible::{ChatFinishReason, NormalizedUsage};
    use crate::persistence::{ContentDigest, NewAttachment, NewJob};
    use crate::test_support::{data_directory, wait};
    use serde_json::json;

    fn claim(handle: &DatabaseHandle, job: JobId) {
        let executor = wait(handle.create_executor(crate::persistence::NewExecutor {
            name: "provider recorder".into(),
            concurrency_limit: std::num::NonZeroU32::new(1).expect("one"),
            llm_transports: std::collections::BTreeSet::from([
                crate::persistence::LlmTransport::OpenAiChatCompletions,
            ]),
        }))
        .expect("executor");
        wait(handle.claim_fixture(job, executor)).expect("claim");
    }

    fn job(handle: &DatabaseHandle) -> JobId {
        wait(handle.create_job(NewJob {
            process_grant: None,
            name: None,
            eligible_at: None,
            parent: handle.genesis(),
            project: None,
            input: Default::default(),
            previous: None,
            configuration: Some(crate::test_support::execution_configuration(handle)),
        }))
        .expect("job")
    }

    #[test]
    fn a_refused_sent_preserves_recorder_state_and_database_availability() {
        wait(async {
            let (_root, directory) = data_directory("provider-record", "failed-sent");
            let worker = DatabaseWorker::start(&directory).expect("start");
            let handle = worker.handle();
            let job = job(&handle);
            claim(&handle, job);
            let mut sink = output_sink(handle.clone(), job);
            let body = json!({"model":"mock", "stream":true, "tools":[{"type":"function"}],
                "messages":[{"role":"system","content":"instructions"},
                    {"role":"user","content":"question"}]});
            let (mut refused, _) = sink
                .recorder
                .prepare(job, sink.request, &body, 0)
                .expect("prepare");
            let expected = refused.clone();
            refused
                .bodies
                .retain(|body| body.digest() != refused.parameters);
            assert!(matches!(
                handle.record_sent_request(refused).await,
                Err(DatabaseError::Rejected(Violation::InvalidValue(
                    "messages",
                    _
                )))
            ));
            assert_eq!(handle.status(), crate::database::DatabaseStatus::Open);
            assert!(
                handle
                    .provider_request(job, sink.request)
                    .await
                    .expect("read")
                    .is_none()
            );
            let (retry, _) = sink
                .recorder
                .prepare(job, sink.request, &body, 0)
                .expect("retry");
            assert_eq!(retry.bodies, expected.bodies);
            assert_eq!(retry.messages, expected.messages);
            assert_eq!(retry.tools, expected.tools);
            assert_eq!(retry.parameters, expected.parameters);
            sink.sending(&body).await.expect("retry records all bodies");
            let recorded = handle
                .provider_request(job, sink.request)
                .await
                .expect("read")
                .expect("Sent");
            assert_eq!(
                recorded.messages,
                body["messages"].as_array().expect("messages").clone()
            );
            assert_eq!(recorded.tools, Some(body["tools"].clone()));
            assert_eq!(recorded.parameters, json!({"model":"mock", "stream":true}));
            assert_eq!(handle.status(), crate::database::DatabaseStatus::Open);
            worker.shutdown().expect("shutdown");
        });
    }

    #[test]
    fn sent_requires_a_claimed_running_job() {
        wait(async {
            let (_root, directory) = data_directory("provider-record", "sent-claim");
            let worker = DatabaseWorker::start(&directory).expect("start");
            let handle = worker.handle();
            let job = job(&handle);
            let mut sink = output_sink(handle.clone(), job);
            let body = json!({"model":"mock", "messages":[{"role":"user","content":"hi"}]});
            assert!(sink.sending(&body).await.is_err());
            assert!(
                matches!(sink.gap.take(), Some(DatabaseError::Rejected(Violation::NotExecutingLocally(id))) if id == job)
            );
            claim(&handle, job);
            sink.sending(&body).await.expect("claimed retry");
            handle
                .finalize_job(job, JobFinalization::Unknown)
                .await
                .expect("settle");
            sink.request = sink.request.next().expect("next");
            assert!(sink.sending(&body).await.is_err());
            assert!(
                matches!(sink.gap.take(), Some(DatabaseError::Rejected(Violation::NotExecutingLocally(id))) if id == job)
            );
            assert!(
                handle
                    .provider_request(job, sink.request)
                    .await
                    .expect("read")
                    .is_none()
            );
            assert_eq!(handle.status(), crate::database::DatabaseStatus::Open);
            worker.shutdown().expect("shutdown");
        });
    }

    #[test]
    fn sent_checks_only_the_digests_introduced_for_this_job() {
        wait(async {
            let (_root, directory) = data_directory("provider-record", "sent-suffix");
            let worker = DatabaseWorker::start(&directory).expect("start");
            let handle = worker.handle();
            let job = job(&handle);
            claim(&handle, job);
            let mut sink = output_sink(handle.clone(), job);
            let mut body =
                json!({"model":"mock", "tools":[], "messages":[{"role":"user","content":"hi"}]});
            sink.sending(&body).await.expect("first Sent");
            let response = sink
                .recorder
                .response(&ChatMessage::assistant(Some("answer".into()), vec![]))
                .expect("response");
            // A received body is acknowledged by the recorder only after the answer commits.
            let digest = response.digest();
            handle
                .record_provider_answer(crate::persistence::NewProviderAnswer {
                    stat: NewAvailabilityStat {
                        job,
                        request: sink.request,
                        observed_at: Timestamp::now(),
                        observation: crate::persistence::AvailabilityObservation {
                            status_code: Some(200),
                            finish_reason: Some(ChatFinishReason::Stop),
                            raw_finish_reason: Some("stop".into()),
                            failure: None,
                        },
                    },
                    measurements: Measurements {
                        request_finished_in_ms: Some(1),
                        ..Default::default()
                    },
                    response,
                })
                .await
                .expect("answer");
            sink.recorder.remember_response(digest);
            body["messages"].as_array_mut().expect("messages").extend([
                json!({"role":"assistant","content":"answer"}),
                json!({"role":"user","content":"next"}),
            ]);
            sink.request = sink.request.next().expect("next");
            let old = crate::persistence::MessageBody::new(&body["messages"][0]).expect("old body");
            let old_digest = old.digest();
            handle
                .with_database(move |database| {
                    let tx = database.begin_write()?;
                    tx.open_table(redb::TableDefinition::<&[u8], &[u8]>::new("message"))?
                        .remove(old_digest.as_bytes().as_slice())?;
                    tx.commit()?;
                    Ok(())
                })
                .await
                .expect("hide acknowledged history to detect redundant validation");
            let (sent, state) = sink
                .recorder
                .prepare(job, sink.request, &body, 0)
                .expect("prepare");
            assert_eq!(sent.required_messages, [sent.messages[2]]);
            assert_eq!(sent.bodies.len(), 1);
            handle.record_sent_request(sent).await.expect("new suffix");
            sink.recorder.remember_sent(state);
            handle
                .with_database(move |database| {
                    let tx = database.begin_write()?;
                    tx.open_table(redb::TableDefinition::<&[u8], &[u8]>::new("message"))?
                        .insert(old.digest.as_bytes().as_slice(), old.bytes.as_slice())?;
                    tx.commit()?;
                    Ok(())
                })
                .await
                .expect("restore history");
            let (retry, _) = sink
                .recorder
                .prepare(job, sink.request, &body, 0)
                .expect("retry");
            assert!(retry.required_messages.is_empty());
            assert!(retry.bodies.is_empty());
            let other_job = self::job(&handle);
            let (fresh, _) = provider_execution::RequestRecorder::default()
                .prepare(other_job, RequestNumber::FIRST, &body, 0)
                .expect("fresh recorder");
            assert_eq!(fresh.required_messages.len(), 5);
            worker.shutdown().expect("shutdown");
        });
    }

    #[test]
    fn sent_requests_are_content_addressed_and_reuse_received_messages() {
        wait(async {
            let (_root, directory) = data_directory("provider-record", "replay");
            let worker = DatabaseWorker::start(&directory).expect("start");
            let handle = worker.handle();
            let attachment = handle
                .create_user_attachment(NewAttachment {
                    content: crate::openai_compatible::image::tests::png(),
                    mime: "image/png".into(),
                    display_name: None,
                })
                .await
                .expect("attachment");
            let job = handle
                .create_job(NewJob {
                    process_grant: None,
                    name: None,
                    eligible_at: None,
                    parent: handle.genesis(),
                    project: None,
                    input: JobInputDraft {
                        attachments: vec![attachment],
                        ..Default::default()
                    },
                    previous: None,
                    configuration: Some(crate::test_support::execution_configuration(&handle)),
                })
                .await
                .expect("job");
            claim(&handle, job);
            let mut sink = output_sink(handle.clone(), job);
            sink.dropped_turns = 2;
            sink.recorder.image_attachments.insert(1, vec![attachment]);
            let mut body = json!({"model":"mock", "stream":true, "tools":[{"type":"function"}],
                "messages":[{"role":"user","content":"hello"},
                {"role":"user","content":[{"type":"image_url","image_url":{"url":"data:image/png;base64,AAAA"}}]}]});
            let pinned = crate::persistence::MessageBody::new(&body["messages"][0]).expect("body");
            assert_eq!(
                pinned.digest().to_string(),
                "f4f7e767b9a1966921d93f1818bb0238c1633ed715f29a789b4e3a624ab16512"
            );
            sink.sending(&body).await.expect("Sent 1");
            let first = handle
                .provider_request(job, RequestNumber::FIRST)
                .await
                .expect("read")
                .expect("Sent");
            assert!(
                first.response.is_none(),
                "a crash here has no invented response"
            );
            assert_eq!(first.dropped_turns, 2);
            assert_eq!(
                first.messages[1]["content"][0]["image_url"]["url"],
                json!({"attachment":attachment.to_string()})
            );
            sink.request = sink.request.next().expect("retry number");
            let (retry, state) = sink
                .recorder
                .prepare(job, sink.request, &body, 2)
                .expect("retry");
            assert!(
                retry.bodies.is_empty(),
                "retry hashes and sends no earlier body"
            );
            assert_eq!(retry.dropped_turns, None);
            handle.record_sent_request(retry).await.expect("Sent retry");
            sink.recorder.remember_sent(state);
            let result = ProviderExecutionResult {
                refused: Vec::new(),
                text: "answer".into(),
                reasoning: "private thinking".into(),
                tool_calls: vec![],
                usage: NormalizedUsage::default(),
                finish_reason: Some(ChatFinishReason::Stop),
                raw_finish_reason: Some("stop".into()),
                status_code: 200,
                request_id: None,
                ttfb: Duration::ZERO,
                finished_in: Duration::ZERO,
            };
            let assistant = assistant_message(&result);
            record_answer(&mut sink, &result, &assistant).await;
            assert!(sink.gap.is_none());
            sink.recorder
                .remember_response(sink.response_digest.take().expect("response digest"));
            body["messages"]
                .as_array_mut()
                .expect("messages")
                .push(serde_json::to_value(assistant).expect("assistant"));
            sink.request = sink.request.next().expect("third");
            let (next, state) = sink
                .recorder
                .prepare(job, sink.request, &body, 2)
                .expect("next");
            assert!(next.bodies.is_empty(), "the response is already stored");
            handle.record_sent_request(next).await.expect("Sent 3");
            sink.recorder.remember_sent(state);
            let received = handle
                .provider_request(job, RequestNumber::new(2).expect("two"))
                .await
                .expect("read")
                .expect("request");
            assert_eq!(
                received.response,
                Some(json!({"role":"assistant","content":"answer"}))
            );
            assert_eq!(received.parameters, json!({"model":"mock","stream":true}));
            assert_eq!(received.tools, Some(body["tools"].clone()));
            let third = handle
                .provider_request(job, sink.request)
                .await
                .expect("read")
                .expect("third");
            assert_eq!(third.messages[2], received.response.expect("response"));
            assert_eq!(third.dropped_turns, 2);
            handle
                .with_database(move |database| {
                    use redb::{ReadableDatabase, ReadableTable};
                    let tx = database.begin_read()?;
                    let table =
                        tx.open_table(redb::TableDefinition::<&[u8], &[u8]>::new("message"))?;
                    let mut count = 0;
                    for row in table.iter()? {
                        let (key, bytes) = row?;
                        assert_eq!(
                            ContentDigest::of(bytes.value()).as_bytes().as_slice(),
                            key.value()
                        );
                        assert!(!String::from_utf8_lossy(bytes.value()).contains("base64"));
                        count += 1;
                    }
                    assert_eq!(count, 5, "two messages, tools, parameters and response");
                    Ok(())
                })
                .await
                .expect("check rows");
            let replies = handle
                .provider_responses(job, None, 1)
                .await
                .expect("responses");
            assert_eq!(replies.len(), 1);
            assert_eq!(replies[0].request.get(), 2, "Sent alone is not a reply");
            assert_eq!(replies[0].text, "answer");
            assert!(!replies[0].requests_tools);
            handle.record_provider_answer(crate::persistence::NewProviderAnswer {
                stat: NewAvailabilityStat {
                    job, request: sink.request, observed_at: Timestamp::now(),
                    observation: provider_execution::success_observation(&result),
                },
                measurements: provider_execution::metrics(&result),
                response: crate::persistence::MessageBody::new(&json!({
                    "role":"assistant", "content":null,
                    "tool_calls":[{"id":"call-3", "type":"function", "function":{"name":"read", "arguments":"{}"}}]
                })).expect("tool reply"),
            }).await.expect("record tool reply");
            let tail = handle
                .provider_responses(job, Some(replies[0].request), 1)
                .await
                .expect("next page");
            assert_eq!(tail.len(), 1);
            assert_eq!(tail[0].request, sink.request);
            assert!(tail[0].text.is_empty());
            assert!(tail[0].requests_tools);
            assert!(
                handle
                    .provider_responses(job, Some(sink.request), 1)
                    .await
                    .expect("end")
                    .is_empty()
            );
            worker.shutdown().expect("close");
            let worker = DatabaseWorker::start(&directory).expect("reopen");
            assert_eq!(
                worker
                    .handle()
                    .provider_request(job, RequestNumber::FIRST)
                    .await
                    .expect("read"),
                Some(first)
            );
            let restored = worker
                .handle()
                .provider_responses(job, None, 10)
                .await
                .expect("restored replies");
            assert_eq!(restored.len(), 2);
            assert_eq!(restored[0].text, replies[0].text);
            assert_eq!(restored[1].request, tail[0].request);
            assert_eq!(restored[1].recorded_at, tail[0].recorded_at);
            worker.shutdown().expect("close");
        });
    }
}

#[cfg(all(test, unix))]
mod approval_runtime_tests {
    use super::*;
    use crate::database::DatabaseWorker;
    use crate::orchestration::workspace_command::{Grant, workspace_tools};
    use crate::orchestration::workspace_read::Workspace;
    use crate::persistence::*;
    use crate::test_support::{data_directory, on_system, temporary_directory, wait};
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn open(
        db: &DatabaseHandle,
        project: ProjectId,
        approve: ApproveClass,
    ) -> (JobId, JobId, ExecutorId) {
        let mut configuration = crate::test_support::execution_configuration(db);
        let ConfigBody::Model(mut model) = wait(db.config(configuration.llm_profile.into()))
            .unwrap()
            .unwrap()
            .body
        else {
            panic!("model");
        };
        model
            .declared_capabilities
            .extend([ModelCapability::Chat, ModelCapability::Tools]);
        wait(db.update_config(
            configuration.llm_profile.into(),
            NewConfig {
                name: "tool model".into(),
                body: ConfigBody::Model(model),
            },
        ))
        .unwrap();
        let agent = wait(db.create_config(NewConfig {
            name: "approval Agent".into(),
            body: ConfigBody::Agent(AgentConfig {
                models: vec![configuration.llm_profile.into()],
                tool_grant: Some("code".into()),
                approve: vec![approve],
                ..Default::default()
            }),
        }))
        .unwrap();
        configuration.agent_profile = Some(agent.into());
        let pool = wait(db.create_executor(NewExecutor {
            name: "worker".into(),
            concurrency_limit: std::num::NonZeroU32::new(1).unwrap(),
            llm_transports: BTreeSet::from([LlmTransport::OpenAiChatCompletions]),
        }))
        .unwrap();
        let (conversation, entry) = wait(db.open_conversation(
            None,
            ConversationSpec {
                name: None,
                agent: Some(agent),
                project: Some(project),
                origin: Origin::User,
                keep_open: true,
            },
            Entry::UserMessage {
                text: "objective".into(),
                attachments: vec![],
            },
            configuration,
            None,
        ))
        .unwrap();
        wait(db.claim_fixture(entry.turn, pool)).unwrap();
        (conversation, entry.turn, pool)
    }
    fn response(text: &str, calls: serde_json::Value) -> provider_seam::Response {
        let tools = calls.as_array().is_some_and(|calls| !calls.is_empty());
        let mut delta = json!({"content":text});
        if tools {
            delta["tool_calls"] = calls;
        }
        let chunk = format!(
            "data: {}\n\ndata: [DONE]\n\n",
            json!({"choices":[{"delta":delta,"finish_reason":if tools { "tool_calls" } else { "stop" }}]})
        );
        provider_seam::Response {
            gate: None,
            steps: vec![crate::provider_execution::scripted::Step::Chunk(Box::leak(
                chunk.into_boxed_str(),
            ))],
        }
    }
    async fn execute(
        db: DatabaseHandle,
        job: JobId,
        pool: ExecutorId,
        tools: &ToolExecution,
        approved: &[ToolEvidenceRecord],
        endpoint: &str,
        dropped_turns: u32,
    ) {
        let mut frozen = db.job_configuration(job).await.unwrap().unwrap().frozen;
        frozen.llm.provider.endpoint = Some(endpoint.into());
        let config = ProviderExecutionConfig::default();
        run_provider_job_inner(
            ProviderRun {
                scheduler: None,
                database: db,
                executor: pool,
                input: JobInputId::new(job.get()),
                job,
            },
            ChatConversation {
                messages: vec![ChatMessage::user("objective")],
                tools: tools.offer.wires().unwrap(),
                dropped_turns,
                ..Default::default()
            },
            Some(tools),
            approved,
            ContextLimits {
                context_window: None,
                max_tokens: None,
            },
            &config.cancellation,
            &Cell::new(false),
            async |conversation, sink| {
                provider_seam::exchange(&frozen, &config, conversation, sink).await
            },
        )
        .await
        .unwrap();
    }

    #[test]
    fn approval_runs_the_read_only_and_completes_without_a_second_request() {
        let (_data, directory) = data_directory("approvals", "mixed-response");
        let (_files, root) = temporary_directory("approvals", "mixed-files");
        std::fs::write(root.join("input"), "read me").unwrap();
        let workspace = Workspace::authorize(&root, &directory).unwrap();
        let mut tools = workspace_tools(workspace, Grant::Code, &[]).unwrap();
        tools.approve = vec![ApproveClass::Writes];
        tools.decider = true;
        let worker = DatabaseWorker::start(&directory).unwrap();
        let db = worker.handle();
        let project = wait(db.create_project(NewProject {
            name: "files".into(),
            description: None,
            root: root.to_string_lossy().into_owned(),
            commands: vec![],
        }))
        .unwrap();
        let (_, turn, pool) = open(&db, project, ApproveClass::Writes);
        let requests = Arc::new(AtomicUsize::new(0));
        let count = requests.clone();
        let endpoint = "https://approval-mixed.invalid/v1";
        provider_seam::install(endpoint, move |_| {
            assert_eq!(count.fetch_add(1, Ordering::SeqCst), 0);
            response(
                "Checking",
                json!([
                    {"index":0,"id":"write","type":"function","function":{"name":"workspace_write_file","arguments":"{\"operation\":\"create\",\"path\":\"output\",\"content\":\"written\"}"}},
                    {"index":1,"id":"read","type":"function","function":{"name":"workspace_read_file","arguments":"{\"path\":\"input\"}"}}
                ]),
            )
        });
        let handle = db.clone();
        on_system(move || async move {
            execute(handle, turn, pool, &tools, &[], endpoint, 0).await;
        });
        assert_eq!(requests.load(Ordering::SeqCst), 1);
        assert!(!root.join("output").exists());
        let calls = wait(db.tool_evidence(turn, None, 10)).unwrap();
        assert!(matches!(
            calls[0].decision.fact,
            ToolDecision::AwaitingApproval(_)
        ));
        assert!(calls[0].outcome.is_none());
        assert!(matches!(
            calls[1].outcome.as_ref().unwrap().fact,
            ToolOutcome::Completed { .. }
        ));
        assert!(wait(db.job(turn)).unwrap().unwrap().completed_at.is_some());
        assert!(
            wait(db.latest_output(turn))
                .unwrap()
                .unwrap()
                .summary
                .unwrap()
                .contains("Awaiting approval: workspace_write_file")
        );
        worker.shutdown().unwrap();
    }

    #[test]
    fn approval_on_the_last_allowed_round_completes_without_a_second_request() {
        let (_data, directory) = data_directory("approvals", "last-round");
        let (_files, root) = temporary_directory("approvals", "last-round-files");
        let workspace = Workspace::authorize(&root, &directory).unwrap();
        let mut tools = workspace_tools(workspace, Grant::Code, &[]).unwrap();
        tools.approve = vec![ApproveClass::Writes];
        tools.decider = true;
        tools.limits.max_provider_requests = 1;
        let worker = DatabaseWorker::start(&directory).unwrap();
        let db = worker.handle();
        let project = wait(db.create_project(NewProject {
            name: "files".into(),
            description: None,
            root: root.to_string_lossy().into_owned(),
            commands: vec![],
        }))
        .unwrap();
        let (_, turn, pool) = open(&db, project, ApproveClass::Writes);
        let requests = Arc::new(AtomicUsize::new(0));
        let count = requests.clone();
        let endpoint = "https://approval-last-round.invalid/v1";
        provider_seam::install(endpoint, move |_| {
            assert_eq!(count.fetch_add(1, Ordering::SeqCst), 0);
            response(
                "Checking",
                json!([
                    {"index":0,"id":"write","type":"function","function":{"name":"workspace_write_file","arguments":"{\"operation\":\"create\",\"path\":\"output\",\"content\":\"written\"}"}}
                ]),
            )
        });
        let handle = db.clone();
        on_system(move || async move {
            execute(handle, turn, pool, &tools, &[], endpoint, 0).await;
        });
        assert_eq!(requests.load(Ordering::SeqCst), 1);
        assert!(!root.join("output").exists());
        let calls = wait(db.tool_evidence(turn, None, 10)).unwrap();
        assert_eq!(calls.len(), 1);
        assert!(matches!(
            calls[0].decision.fact,
            ToolDecision::AwaitingApproval(_)
        ));
        assert!(calls[0].outcome.is_none());
        assert!(wait(db.job(turn)).unwrap().unwrap().completed_at.is_some());
        assert!(
            wait(db.latest_output(turn))
                .unwrap()
                .unwrap()
                .summary
                .unwrap()
                .contains("Awaiting approval: workspace_write_file")
        );
        worker.shutdown().unwrap();
    }

    #[test]
    fn a_child_turn_offers_ask_user_and_awaits_its_answer() {
        let (_data, directory) = data_directory("approvals", "ask-user");
        let worker = DatabaseWorker::start(&directory).unwrap();
        let db = worker.handle();
        let configuration = crate::test_support::execution_configuration(&db);
        let record = wait(db.config(configuration.llm_profile.into()))
            .unwrap()
            .unwrap();
        let ConfigBody::Model(mut model) = record.body else {
            panic!("model");
        };
        model
            .declared_capabilities
            .extend([ModelCapability::Chat, ModelCapability::Tools]);
        let endpoint = "https://approval-ask-user.invalid/v1";
        wait(db.update_config(
            model.connection_id,
            NewConfig {
                name: "ask user connection".into(),
                body: ConfigBody::Connection(crate::persistence::ConnectionConfig {
                    protocol: LlmTransport::OpenAiChatCompletions,
                    endpoint: Some(endpoint.into()),
                    credential_ref: None,
                }),
            },
        ))
        .unwrap();
        wait(db.update_config(
            record.id,
            NewConfig {
                name: "tool model".into(),
                body: ConfigBody::Model(model),
            },
        ))
        .unwrap();
        let agent = wait(db.create_config(NewConfig {
            name: "child Agent".into(),
            body: ConfigBody::Agent(AgentConfig {
                models: vec![configuration.llm_profile.into()],
                ..Default::default()
            }),
        }))
        .unwrap();
        let pool = wait(db.create_executor(NewExecutor {
            name: "worker".into(),
            concurrency_limit: std::num::NonZeroU32::new(1).unwrap(),
            llm_transports: BTreeSet::from([LlmTransport::OpenAiChatCompletions]),
        }))
        .unwrap();
        let (chat, _) = wait(db.open_conversation(
            None,
            ConversationSpec {
                name: None,
                agent: None,
                project: None,
                origin: Origin::User,
                keep_open: true,
            },
            Entry::UserMessage {
                text: "objective".into(),
                attachments: vec![],
            },
            configuration,
            None,
        ))
        .unwrap();
        let call = ToolCallRef {
            job: chat,
            request: RequestNumber::FIRST,
            index: 0,
        };
        let (_, child) = wait(db.open_conversation(
            Some(chat),
            ConversationSpec {
                name: None,
                agent: Some(agent),
                project: None,
                origin: Origin::Agent { call },
                keep_open: true,
            },
            Entry::AgentMessage {
                text: "objective".into(),
                call,
            },
            ExecutionConfiguration {
                agent_profile: Some(agent.into()),
                ..configuration
            },
            None,
        ))
        .unwrap();
        wait(db.claim_fixture(child.turn, pool)).unwrap();
        provider_seam::install(endpoint, move |request| {
            assert!(
                request
                    .tools
                    .iter()
                    .any(|tool| tool.function.name == "ask_user")
            );
            response(
                "Asking",
                json!([
                    {"index":0,"id":"ask","type":"function","function":{"name":"ask_user","arguments":"{\"question\":\"Which file?\"}"}}
                ]),
            )
        });
        let handle = db.clone();
        on_system(move || async move {
            let possible_effect = Cell::new(false);
            run_started_provider_job(
                handle,
                ProviderTask {
                    workspace_tools: None,
                    credential_resolver: Arc::new(EnvironmentCredentialResolver),
                    provider_config: ProviderExecutionConfig::default(),
                    tools: None,
                },
                (
                    StartedJob {
                        job: child.turn,
                        input: JobInputId::new(child.turn.get()),
                        executor: pool,
                    },
                    None,
                ),
                &possible_effect,
                None,
            )
            .await
            .unwrap();
        });
        let calls = wait(db.tool_evidence(child.turn, None, 10)).unwrap();
        assert_eq!(calls.len(), 1);
        assert!(matches!(
            calls[0].decision.fact,
            ToolDecision::AwaitingAnswer(_)
        ));
        assert!(calls[0].outcome.is_none());
        assert!(
            wait(db.job(child.turn))
                .unwrap()
                .unwrap()
                .completed_at
                .is_some()
        );
        assert!(
            wait(db.latest_output(child.turn))
                .unwrap()
                .unwrap()
                .summary
                .unwrap()
                .contains("Awaiting answer:")
        );
        worker.shutdown().unwrap();
    }

    #[test]
    fn approved_command_runs_once_under_request_one_before_the_first_provider_request() {
        let (_data, directory) = data_directory("approvals", "approved-runtime");
        let (_files, root) = temporary_directory("approvals", "approved-files");
        let command = ProjectCommand {
            name: "run".into(),
            argv: vec!["sh".into(), "-c".into(), "echo ran >> ran.txt".into()],
        };
        let workspace = Workspace::authorize(&root, &directory).unwrap();
        let mut tools =
            workspace_tools(workspace, Grant::Code, std::slice::from_ref(&command)).unwrap();
        tools.approve = vec![ApproveClass::Commands];
        tools.decider = true;
        let worker = DatabaseWorker::start(&directory).unwrap();
        let db = worker.handle();
        let project = wait(db.create_project(NewProject {
            name: "files".into(),
            description: None,
            root: root.to_string_lossy().into_owned(),
            commands: vec![command],
        }))
        .unwrap();
        let (conversation, first, pool) = open(&db, project, ApproveClass::Commands);
        let endpoint = "https://approval-execute.invalid/v1";
        let requests = Arc::new(AtomicUsize::new(0));
        let count = requests.clone();
        provider_seam::install(endpoint, move |_| {
            if count.fetch_add(1, Ordering::SeqCst) == 0 {
                response(
                    "Requesting",
                    json!([{"index":0,"id":"run","type":"function","function":{"name":"workspace_run_command","arguments":"{\"command\":\"run\"}"}}]),
                )
            } else {
                response("Done", json!([]))
            }
        });
        let handle = db.clone();
        let first_tools = tools.clone();
        on_system(move || async move {
            execute(handle, first, pool, &first_tools, &[], endpoint, 0).await;
        });
        let approval = wait(db.tool_evidence(first, None, 10)).unwrap()[0].call;
        let next = wait(db.decide_approval(approval, ApprovalDecision::Approved))
            .unwrap()
            .turn;
        wait(db.claim_fixture(next, pool)).unwrap();
        let approved = wait(db.approved_calls(conversation)).unwrap();
        let handle = db.clone();
        let next_tools = tools.clone();
        on_system(move || async move {
            execute(handle, next, pool, &next_tools, &approved, endpoint, 7).await;
        });
        let calls = wait(db.tool_evidence(next, None, 10)).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].call.request, RequestNumber::FIRST);
        assert_eq!(calls[0].approval, Some(approval));
        assert!(
            calls[0]
                .tool_call_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        );
        assert!(
            wait(db.provider_request(next, RequestNumber::FIRST))
                .unwrap()
                .is_none()
        );
        assert_eq!(
            wait(db.provider_request(next, RequestNumber::FIRST.next().unwrap()))
                .unwrap()
                .unwrap()
                .dropped_turns,
            7
        );
        let third = wait(db.append(
            conversation,
            Entry::UserMessage {
                text: "again".into(),
                attachments: vec![],
            },
            None,
            None,
        ))
        .unwrap()
        .turn;
        wait(db.claim_fixture(third, pool)).unwrap();
        assert!(wait(db.approved_calls(conversation)).unwrap().is_empty());
        let handle = db.clone();
        on_system(move || async move {
            execute(handle, third, pool, &tools, &[], endpoint, 0).await;
        });
        assert!(wait(db.tool_evidence(third, None, 10)).unwrap().is_empty());
        assert_eq!(
            std::fs::read_to_string(root.join("ran.txt")).unwrap(),
            "ran\n"
        );
        worker.shutdown().unwrap();
    }
}
