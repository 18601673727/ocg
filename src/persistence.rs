use std::collections::BTreeSet;
use std::fmt;
use std::num::NonZeroU32;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::openai_compatible::{ChatFinishReason, NormalizedUsage, TransportError};
use crate::storage::{
    AgentRow, Batch, BatchError, ExecutorRow, HealthCheckRow, JobInputRow, JobRow, MarkerSubjects,
    ProfileLinks, ProfileRow, dependency_state,
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
record_id_type!(ExecutorId, "executor");
record_id_type!(ProjectId, "project");
record_id_type!(ProfileId, "profile");
record_id_type!(AgentId, "agent");
record_id_type!(LlmProviderId, "llm_provider");
record_id_type!(HealthCheckId, "health_check");

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RecordRef {
    Job(JobId),
    JobInput(JobInputId),
    JobOutput(JobOutputId),
    Executor(ExecutorId),
    Project(ProjectId),
    Profile(ProfileId),
    Agent(AgentId),
    LlmProvider(LlmProviderId),
    HealthCheck(HealthCheckId),
}

impl fmt::Display for RecordRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Job(id) => id.fmt(f),
            Self::JobInput(id) => id.fmt(f),
            Self::JobOutput(id) => id.fmt(f),
            Self::Executor(id) => id.fmt(f),
            Self::Project(id) => id.fmt(f),
            Self::Profile(id) => id.fmt(f),
            Self::Agent(id) => id.fmt(f),
            Self::LlmProvider(id) => id.fmt(f),
            Self::HealthCheck(id) => id.fmt(f),
        }
    }
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

#[derive(Clone, Debug, Default, PartialEq)]
pub struct JobInputDraft {
    pub submission_id: Option<String>,
    pub objective: Option<String>,
    pub max_depth: Option<u32>,
    pub max_children_per_job: Option<u32>,
    pub max_total_descendants: Option<u32>,
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
    /// Must be able to serve `configuration`'s LLM Profile when one is given.
    pub assigned_executor: Option<ExecutorId>,
    pub previous: Option<JobId>,
    pub replaces: Option<JobId>,
}

/// The configuration an LLM execution child Job is submitted with. Both
/// references are fixed at submission, and `llm_profile` must be one of
/// `agent_profile`'s configured LLM targets. The effective model, Provider,
/// parameters and declared capabilities are read from these immutable
/// Profiles, never copied into the Job.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExecutionConfiguration {
    pub agent_profile: ProfileId,
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
    /// The LLM Profiles this Executor can serve, fixed at registration. Only
    /// an `ExecutorType::Llm` Executor may serve any.
    pub serves: Vec<ProfileId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendedOutput {
    pub output: JobOutputId,
    pub seq: i64,
}

/// A stored Job with the related IDs assembled from its relationship tables.
#[derive(Clone, Debug, PartialEq)]
pub struct JobRecord {
    pub id: JobId,
    pub name: Option<String>,
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
    /// The Genesis Job is its own parent; every other Job has a different one.
    pub parent: JobId,
    pub projects: Vec<ProjectId>,
    /// Assembled from APPLIES_PROFILE and RUNS_LLM_PROFILE.
    pub configuration: Option<ExecutionConfiguration>,
    pub assigned_executor: Option<ExecutorId>,
    pub previous: Option<JobId>,
    pub replaces: Option<JobId>,
    pub retry_of: Option<JobId>,
}

/// A stored JobInput.
#[derive(Clone, Debug, PartialEq)]
pub struct JobInputRecord {
    pub id: JobInputId,
    pub submission_id: Option<String>,
    pub objective: Option<String>,
    pub max_depth: Option<u32>,
    pub max_children_per_job: Option<u32>,
    pub max_total_descendants: Option<u32>,
    pub submitted_at: Option<Timestamp>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    /// What an LLM execution child submitted with this input was configured
    /// to execute, frozen with the input. `None` for unconfigured work and
    /// for an input not yet submitted.
    pub configuration: Option<FrozenExecutionConfig>,
}

/// An LLM Profile an Executor is registered to serve, with that Profile's
/// configuration as it was at registration. A later edit of the Profile does
/// not extend the registration to the new configuration.
#[derive(Clone, Debug, PartialEq)]
pub struct ServedConfiguration {
    pub profile: ProfileId,
    pub configuration: FrozenLlmConfig,
}

/// A stored Executor.
#[derive(Clone, Debug, PartialEq)]
pub struct ExecutorRecord {
    pub id: ExecutorId,
    pub name: String,
    pub executor_type: ExecutorType,
    pub concurrency_limit: u32,
    pub latest_enabled_at: Option<Timestamp>,
    pub latest_disabled_at: Option<Timestamp>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    /// Assembled from SERVES, in Profile ID order.
    pub serves: Vec<ServedConfiguration>,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewProject {
    pub name: String,
    pub description: Option<String>,
}

/// Projects have no update path, so they carry no `updated_at`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectRecord {
    pub id: ProjectId,
    pub name: String,
    pub description: Option<String>,
    pub created_at: Timestamp,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewLlmProvider {
    pub name: String,
    pub endpoint: Option<String>,
    /// The name of a stored credential, never the secret itself.
    pub credential_ref: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LlmProviderRecord {
    pub id: LlmProviderId,
    pub name: String,
    pub endpoint: Option<String>,
    pub credential_ref: Option<String>,
    pub created_at: Timestamp,
}

/// A capability a user declares for a model, or that a HealthCheck verifies.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ModelCapability {
    Chat,
    Tools,
    Vision,
    Reasoning,
}

/// Request parameters the OpenAI-compatible transport already forwards.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct LlmParameters {
    pub temperature: Option<f64>,
    pub top_p: Option<f64>,
    pub max_tokens: Option<u32>,
    pub presence_penalty: Option<f64>,
    pub frequency_penalty: Option<f64>,
}

/// The configuration of a Profile<LLM>. Its Provider is a relationship, not
/// a field, see [`NewProfileConfig::Llm`].
#[derive(Clone, Debug, PartialEq)]
pub struct LlmConfig {
    pub model: String,
    pub parameters: LlmParameters,
    /// What the user states the model supports. HealthChecks never change it.
    pub declared_capabilities: BTreeSet<ModelCapability>,
}

/// The configuration of a Profile<Agent>.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AgentConfig {
    pub instructions: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProfileKind {
    Agent,
    Llm,
}

impl fmt::Display for ProfileKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Agent => "Agent",
            Self::Llm => "LLM",
        })
    }
}

/// A Profile's configuration. The variant is the Profile's only kind tag.
#[derive(Clone, Debug, PartialEq)]
pub enum ProfileConfig {
    Agent(AgentConfig),
    Llm(LlmConfig),
}

impl ProfileConfig {
    pub fn kind(&self) -> ProfileKind {
        match self {
            Self::Agent(_) => ProfileKind::Agent,
            Self::Llm(_) => ProfileKind::Llm,
        }
    }
}

/// A Profile's payload together with the references that define it. Both
/// are fixed when the Profile is created.
#[derive(Clone, Debug, PartialEq)]
pub enum NewProfileConfig {
    /// `llm_targets` are the LLM Profiles this Agent Profile may run with.
    /// They form a set: no default or fallback order is implied.
    Agent { config: AgentConfig, llm_targets: Vec<ProfileId> },
    Llm { provider: LlmProviderId, config: LlmConfig },
}

#[derive(Clone, Debug, PartialEq)]
pub struct NewProfile {
    pub name: String,
    pub config: NewProfileConfig,
}

/// A stored Profile of either kind, as currently configured. It can be
/// edited while no unfinished Job uses it; submitted Jobs and HealthChecks
/// keep their own frozen copies of what they used.
#[derive(Clone, Debug, PartialEq)]
pub struct ProfileRecord {
    pub id: ProfileId,
    pub name: String,
    pub config: ProfileConfig,
    /// Assembled from USES_PROVIDER; present exactly for an LLM Profile.
    pub llm_provider: Option<LlmProviderId>,
    /// Assembled from TARGETS_LLM, in ID order; empty for an LLM Profile.
    pub llm_targets: Vec<ProfileId>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

/// Whether a Profile can be edited now. Every Job submitted with the
/// Profile that has no completed, failed or cancelled outcome holds it,
/// queued, paused and unknown-outcome Jobs included.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProfileEditability {
    /// The holding Jobs in ID order, at most 1,000 of them.
    pub blocking_jobs: Vec<JobId>,
    /// Set when more Jobs hold the Profile than `blocking_jobs` lists.
    pub more_blocking_jobs: bool,
}

impl ProfileEditability {
    pub fn editable(&self) -> bool {
        self.blocking_jobs.is_empty()
    }
}

/// A configuration type that one kind of Profile holds, with the related IDs
/// that kind's typed form assembles.
pub trait ProfileVariant: Sized {
    const KIND: ProfileKind;
    type Links: Clone + fmt::Debug + PartialEq;

    fn split(record: ProfileRecord) -> Option<(Self, Self::Links)>;
}

impl ProfileVariant for AgentConfig {
    const KIND: ProfileKind = ProfileKind::Agent;
    /// The configured LLM targets.
    type Links = Vec<ProfileId>;

    fn split(record: ProfileRecord) -> Option<(Self, Vec<ProfileId>)> {
        match record.config {
            ProfileConfig::Agent(config) => Some((config, record.llm_targets)),
            ProfileConfig::Llm(_) => None,
        }
    }
}

impl ProfileVariant for LlmConfig {
    const KIND: ProfileKind = ProfileKind::Llm;
    type Links = LlmProviderId;

    fn split(record: ProfileRecord) -> Option<(Self, LlmProviderId)> {
        match (record.config, record.llm_provider) {
            (ProfileConfig::Llm(config), Some(provider)) => Some((config, provider)),
            _ => None,
        }
    }
}

/// A Profile in its typed form: `Profile<AgentConfig>` is an Agent Profile
/// and `Profile<LlmConfig>` an LLM Profile.
#[derive(Clone, Debug, PartialEq)]
pub struct Profile<C: ProfileVariant> {
    pub id: ProfileId,
    pub name: String,
    pub config: C,
    pub links: C::Links,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

pub type AgentProfile = Profile<AgentConfig>;
pub type LlmProfile = Profile<LlmConfig>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{profile} is an {found} Profile, not an {expected} Profile")]
pub struct WrongProfileKind {
    pub profile: ProfileId,
    pub expected: ProfileKind,
    pub found: ProfileKind,
}

impl<C: ProfileVariant> TryFrom<ProfileRecord> for Profile<C> {
    type Error = WrongProfileKind;

    fn try_from(record: ProfileRecord) -> Result<Self, WrongProfileKind> {
        let (id, name, created_at, updated_at, found) = (record.id, record.name.clone(), record.created_at, record.updated_at, record.config.kind());
        let mismatch = WrongProfileKind { profile: id, expected: C::KIND, found };
        if found != C::KIND {
            return Err(mismatch);
        }
        let (config, links) = C::split(record).ok_or(mismatch)?;
        Ok(Self { id, name, config, links, created_at, updated_at })
    }
}

/// The Provider facts needed to interpret a request. The credential is
/// named, never copied.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderSnapshot {
    pub id: LlmProviderId,
    pub name: String,
    pub endpoint: Option<String>,
    pub credential_ref: Option<String>,
}

/// An LLM Profile's configuration as it was when frozen: model, request
/// parameters, declared capabilities and the Provider that serves them.
#[derive(Clone, Debug, PartialEq)]
pub struct FrozenLlmConfig {
    pub config: LlmConfig,
    pub provider: ProviderSnapshot,
}

/// What a submitted LLM execution child was configured to execute, frozen
/// in its JobInput when it was submitted.
#[derive(Clone, Debug, PartialEq)]
pub struct FrozenExecutionConfig {
    pub agent: AgentConfig,
    pub llm: FrozenLlmConfig,
}

/// A submitted Job's configuration: the Profiles it was submitted with, as
/// provenance, and the values it was frozen with, as the authority for what
/// it executes. Later Profile edits change neither.
#[derive(Clone, Debug, PartialEq)]
pub struct SubmittedConfiguration {
    pub agent_profile: ProfileId,
    pub llm_profile: ProfileId,
    pub frozen: FrozenExecutionConfig,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewAgent {
    pub name: String,
    /// Each must be an Agent Profile.
    pub profiles: Vec<ProfileId>,
}

/// Agents have no update path, so they carry no `updated_at`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentRecord {
    pub id: AgentId,
    pub name: String,
    /// Assembled from USES_PROFILE.
    pub profiles: Vec<ProfileId>,
    pub created_at: Timestamp,
}

/// What a HealthCheck observed. A failure keeps the transport's own error,
/// so a network, authentication, timeout or rate-limit failure is never
/// recorded as a missing capability.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HealthCheckOutcome {
    Passed,
    Failed(TransportError),
}

/// Times measured by the checker. Unmeasured durations stay `None`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HealthCheckTiming {
    pub started_at: Timestamp,
    pub finished_at: Timestamp,
    pub ttfb_ms: Option<u64>,
    pub reasoning_ttft_ms: Option<u64>,
    pub output_ttft_ms: Option<u64>,
    pub request_finished_in_ms: Option<u64>,
}

/// One verification of one capability of an LLM Profile. It is a fact of its
/// own: recording it never changes the Profile's declared capabilities.
#[derive(Clone, Debug, PartialEq)]
pub struct NewHealthCheck {
    /// The configuration that was actually checked, captured when the check
    /// started, for example with `DatabaseHandle::current_llm_configuration`.
    pub checked: FrozenLlmConfig,
    pub capability: ModelCapability,
    pub outcome: HealthCheckOutcome,
    /// Unreported counters stay `None`; they are never stored as zero.
    pub usage: NormalizedUsage,
    pub finish_reason: Option<ChatFinishReason>,
    pub timing: HealthCheckTiming,
}

#[derive(Clone, Debug, PartialEq)]
pub struct HealthCheckRecord {
    pub id: HealthCheckId,
    /// Assembled from CHECKS. Provenance only: the Profile may have been
    /// edited since, so `checked` says what was verified.
    pub profile: ProfileId,
    pub checked: FrozenLlmConfig,
    pub capability: ModelCapability,
    pub outcome: HealthCheckOutcome,
    pub usage: NormalizedUsage,
    pub finish_reason: Option<ChatFinishReason>,
    pub timing: HealthCheckTiming,
    pub recorded_at: Timestamp,
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
    RecordHealthCheck,
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
    #[error("{0} has a terminal outcome and accepts no more outputs")]
    JobHasTerminalOutcome(JobId),
    #[error("{0} has already started")]
    JobAlreadyStarted(JobId),
    #[error("the parent {0} no longer accepts children")]
    ParentClosed(JobId),
    #[error("ASSIGNED_TO cannot change after {0} starts")]
    ExecutorChangeAfterStart(JobId),
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
    #[error("{executor} is not registered to serve {llm_profile} as configured for this work")]
    ExecutorCannotServe { executor: ExecutorId, llm_profile: ProfileId },
    #[error("{profile} is used by unfinished work, such as {blocking_jobs:?}, and cannot be edited")]
    ProfileLocked { profile: ProfileId, blocking_jobs: Vec<JobId> },
    #[error("the configuration frozen in {0} differs from the retry's; submit the retry with a new input")]
    ConfigurationMismatch(JobInputId),
    #[error("{executor} is a {found:?} Executor; serving an LLM Profile needs an Llm Executor")]
    IncompatibleExecutorKind { executor: ExecutorId, found: ExecutorType },
}

pub(crate) type OpResult<T> = Result<(T, MarkerSubjects), BatchError>;

pub(crate) fn begin_core_log_cleanup(
    batch: &mut Batch,
    owner: JobId,
    executor: ExecutorId,
    started: NewJobOutput,
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
    start_job(batch, job)?;
    publish_output(batch, job, started.kind.into(), started.summary, started.code, started.goal_reached)?;
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

/// The fields and relationships of a Job about to be admitted, from either a
/// submission or a retry.
struct Admission {
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
    /// The LLM configuration frozen for `configuration`, which an assigned
    /// Executor must be registered for.
    frozen_llm: Option<FrozenLlmConfig>,
    assigned_executor: Option<ExecutorId>,
    previous: Option<JobId>,
    replaces: Option<JobId>,
    retry_of: Option<JobId>,
}

/// An input that no Job uses yet; submitting a Job freezes it.
fn require_unsubmitted_input(batch: &mut Batch, input: JobInputId) -> Result<JobInputRow, BatchError> {
    let row = batch.require_input(input)?;
    if row.submitted_at.is_some() || batch.input_has_jobs(input)? {
        return batch.reject(Violation::InputAlreadySubmitted(input));
    }
    Ok(row)
}

/// Freezes the input, together with the execution configuration of the Job
/// submitted with it.
fn submit_input(batch: &mut Batch, mut row: JobInputRow, configuration: Option<FrozenExecutionConfig>) -> Result<(), BatchError> {
    let now = batch.now();
    row.submitted_at = Some(now);
    row.updated_at = now;
    row.configuration = configuration;
    batch.put_input(&row)
}

/// Creates the Job with every relationship fixed at submission, after
/// validating each endpoint and the combined waiting graph with all
/// proposed waiting edges included.
fn admit_job(batch: &mut Batch, job: Admission, input: JobInputId) -> Result<JobId, BatchError> {
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
    for (index, (dependency, condition)) in job.dependencies.iter().enumerate() {
        if job.dependencies[..index].iter().any(|(earlier, _)| earlier == dependency) {
            return batch.reject(Violation::DuplicateDependency(*dependency));
        }
        dependency_rows.push((batch.require_job(*dependency)?, *condition));
    }
    if let Some(configuration) = job.configuration {
        batch.require_configuration(configuration)?;
    }
    if let Some(executor) = job.assigned_executor {
        let served = job.configuration.zip(job.frozen_llm.as_ref()).map(|(configuration, frozen)| (configuration.llm_profile, frozen));
        batch.require_executor_for(executor, served)?;
    }
    for lineage in [job.previous, job.replaces, job.retry_of].into_iter().flatten() {
        batch.require_job(lineage)?;
    }

    let id = JobId(batch.next_id(crate::storage::Sequence::Job)?);
    let mut proposed = vec![(job.parent, id)];
    for (row, condition) in &dependency_rows {
        if dependency_state(*condition, row) == DependencyState::Waiting {
            proposed.push((id, row.id));
        }
    }
    batch.check_waiting(&proposed)?;

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

/// Creates a new Job that retries `retry.original` under the original's
/// parent. The original Job, its input and its outputs are not touched.
///
/// The retry's configuration is frozen from its Profiles as they are now. It
/// may share the original's input only when that equals what the input
/// froze; Profile IDs alone prove nothing once a Profile has been edited.
pub(crate) fn retry_job(batch: &mut Batch, retry: JobRetry) -> OpResult<JobId> {
    let original = retry.original;
    batch.require_job(original)?;
    if batch.genesis()? == original {
        return batch.reject(Violation::GenesisProtected(original));
    }
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

/// Records that the Job's execution began.
pub(crate) fn start_job(batch: &mut Batch, job: JobId) -> OpResult<()> {
    let now = batch.now();
    let mut row = batch.require_job(job)?;
    if row.any_terminal() {
        return batch.reject(Violation::JobHasTerminalOutcome(job));
    }
    if row.started_at.is_some() {
        return batch.reject(Violation::JobAlreadyStarted(job));
    }
    row.started_at = Some(now);
    row.updated_at = now;
    batch.put_job(&row)?;
    Ok(((), MarkerSubjects::addressed(RecordRef::Job(job))))
}

/// Replaces the Job's main Executor; refused once the Job has started. The
/// Executor must be registered for the LLM configuration the Job froze, if any.
pub(crate) fn assign_job_executor(batch: &mut Batch, job: JobId, executor: ExecutorId) -> OpResult<()> {
    let row = batch.require_job(job)?;
    let served = batch.job_frozen_llm(job)?;
    batch.require_executor_for(executor, served.as_ref().map(|(profile, frozen)| (*profile, frozen)))?;
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

/// A confirmed outcome requires every child Job to have a confirmed outcome;
/// the Job is then sterilized, its final result published, and the terminal
/// timestamp set in one transaction.
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

/// Registers an Executor together with the LLM Profiles it can serve, each
/// frozen as configured now. Registrations hold no Profile lock.
pub(crate) fn create_executor(batch: &mut Batch, executor: NewExecutor) -> OpResult<ExecutorId> {
    let now = batch.now();
    if !executor.serves.is_empty() && executor.executor_type != ExecutorType::Llm {
        return batch.reject(Violation::InvalidValue("serves", "only an Llm Executor can serve LLM Profiles"));
    }
    let mut served = Vec::with_capacity(executor.serves.len());
    for (index, profile) in executor.serves.iter().enumerate() {
        if executor.serves[..index].contains(profile) {
            return batch.reject(Violation::DuplicateReference(RecordRef::Profile(*profile)));
        }
        served.push(ServedConfiguration { profile: *profile, configuration: batch.freeze_llm(*profile)? });
    }
    let id = ExecutorId(batch.next_id(crate::storage::Sequence::Executor)?);
    batch.insert_executor(&served, &ExecutorRow {
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
        endpoint: provider.endpoint,
        credential_ref: provider.credential_ref,
        created_at: batch.now(),
    })?;
    Ok((id, MarkerSubjects::created(RecordRef::LlmProvider(id), RecordRef::LlmProvider(id))))
}

fn validate_llm_config(batch: &mut Batch, config: &LlmConfig) -> Result<(), BatchError> {
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
fn profile_parts(batch: &mut Batch, config: NewProfileConfig) -> Result<(ProfileConfig, ProfileLinks), BatchError> {
    Ok(match config {
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
/// identity and kind. Refused while any unfinished Job holds the Profile;
/// the check and the edit share this transaction, so no submission or
/// finalization can come between them. Submitted Jobs and HealthChecks keep
/// the configuration they froze.
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
const LOCK_EXAMPLES: usize = 16;

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

/// Appends a verification fact for an LLM Profile, with the configuration
/// the checker actually checked. The Profile itself, its declared
/// capabilities included, is not touched, and is not read in place of the
/// checked configuration.
pub(crate) fn record_health_check(batch: &mut Batch, profile: ProfileId, check: NewHealthCheck) -> OpResult<HealthCheckId> {
    batch.require_profile_kind(profile, ProfileKind::Llm)?;
    validate_llm_config(batch, &check.checked.config)?;
    batch.require_llm_provider(check.checked.provider.id)?;
    let id = HealthCheckId(batch.next_id(crate::storage::Sequence::HealthCheck)?);
    batch.insert_health_check(&HealthCheckRow {
        id,
        checked: check.checked,
        capability: check.capability,
        outcome: check.outcome,
        usage: check.usage,
        finish_reason: check.finish_reason,
        timing: check.timing,
        recorded_at: batch.now(),
    })?;
    batch.link_check(id, profile)?;
    Ok((id, MarkerSubjects::created(RecordRef::Profile(profile), RecordRef::HealthCheck(id))))
}
