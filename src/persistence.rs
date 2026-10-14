use std::collections::BTreeSet;
use std::fmt;
use std::num::NonZeroU32;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::openai_compatible::{
    ChatFinishReason, MAX_TOOL_CALL_ID_BYTES, MAX_TOOL_DEFINITION_BYTES, NormalizedUsage,
    ProviderFailure, ToolCallRefusal, is_function_name,
};
use crate::storage::{
    AgentRow, AvailabilityStatRow, Batch, BatchError, ExecutorRow, JobInputRow, JobRow,
    MarkerSubjects, MetricsRow, ProfileLinks, ProfileRow, dependency_state,
};
use crate::token_usage::{RequestEligibility, TokenAnnotation, UsageFinality};

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
record_id_type!(MetricsId, "metrics");
record_id_type!(AvailabilityStatId, "availability_stat");
record_id_type!(AttachmentId, "attachment");

/// The number of one Provider request within the Job that sent it, from 1
/// in the order the Job sent them. A Job may send several requests, such as
/// the rounds of a tool loop; every Metrics and AvailabilityStat record
/// belongs to exactly one of them, and evidence is reduced per request,
/// never across the requests of a Job.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RequestNumber(u32);

impl RequestNumber {
    /// A Job's first request, and the only request of a Job recorded before
    /// requests were numbered.
    pub const FIRST: Self = Self(1);

    /// `None` for 0, which numbers no request.
    #[must_use]
    pub const fn new(value: u32) -> Option<Self> {
        if value == 0 { None } else { Some(Self(value)) }
    }

    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }

    /// The request after this one; `None` when numbers ran out.
    #[must_use]
    pub fn next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
}

impl fmt::Display for RequestNumber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "request {}", self.0)
    }
}

/// One Provider request: the Job that sent it and its number in that Job.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RequestRef {
    pub job: JobId,
    pub request: RequestNumber,
}

impl fmt::Display for RequestRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} of {}", self.request, self.job)
    }
}

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
    Metrics(MetricsId),
    AvailabilityStat(AvailabilityStatId),
    Attachment(AttachmentId),
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
            Self::Metrics(id) => id.fmt(f),
            Self::AvailabilityStat(id) => id.fmt(f),
            Self::Attachment(id) => id.fmt(f),
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
    /// When the trusted execution path confirmed that an unknown-outcome
    /// Job's execution has actually stopped. The outcome stays unknown.
    pub stop_confirmed_at: Option<Timestamp>,
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
    /// Assembled from RUNS_LLM_PROFILE and, when present, APPLIES_PROFILE.
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
    /// The Attachments the input carries, in the order they were given.
    /// Frozen with the input when it is submitted; a reference outlives the
    /// cleanup of its Attachment's content.
    pub attachments: Vec<AttachmentId>,
}

/// The SHA-256 of an Attachment's content. It names the immutable file
/// that holds the content, never the Attachment: several Attachments may
/// share one content file.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContentDigest([u8; 32]);

impl ContentDigest {
    /// The SHA-256 of `bytes`.
    #[must_use]
    pub fn of(bytes: &[u8]) -> Self {
        let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, bytes);
        Self(digest.as_ref().try_into().expect("SHA-256 has 32 bytes"))
    }

    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Lowercase hexadecimal, 64 characters: the name of the content file.
impl fmt::Display for ContentDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.iter().try_for_each(|byte| write!(f, "{byte:02x}"))
    }
}

impl fmt::Debug for ContentDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ContentDigest({self})")
    }
}

/// Where an Attachment's content came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttachmentOrigin {
    /// Given to OCG by its user, such as an image sent with an objective.
    UserProvided,
    /// Produced by a model. No Provider output creates one yet.
    ModelProduced,
}

/// Who removed an Attachment's content.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CleanupKind {
    /// The user asked for it.
    Manual,
    /// A retention policy did.
    Automatic,
}

/// That an Attachment's content was cleaned up: how, and when. Its kind and
/// time are recorded together or not at all. Cleanup removes availability,
/// never history: the Attachment, its metadata and every reference to it
/// remain.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AttachmentCleanup {
    pub kind: CleanupKind,
    pub cleaned_at: Timestamp,
}

/// A stored Attachment: a durable business entity with an identity of its
/// own, independent of its content. Two Attachments of identical bytes are
/// two Attachments with one [`ContentDigest`]; each keeps its own name,
/// origin and cleanup. Attachments have no update path apart from cleanup,
/// which no operation records yet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AttachmentRecord {
    pub id: AttachmentId,
    pub sha256: ContentDigest,
    /// The content's length in bytes, kept after its cleanup.
    pub byte_length: u64,
    /// A lowercase `type/subtype`, without parameters.
    pub mime: String,
    /// A sanitized file name, for display only; never a path.
    pub display_name: Option<String>,
    pub origin: AttachmentOrigin,
    pub created_at: Timestamp,
    /// `None` while the content is available.
    pub cleanup: Option<AttachmentCleanup>,
}

impl AttachmentRecord {
    /// Whether the Attachment's content may still be used. A file that is
    /// missing or does not match is refused when it is read, see
    /// [`crate::database::DatabaseHandle::attachment_content`].
    #[must_use]
    pub fn is_available(&self) -> bool {
        self.cleanup.is_none()
    }
}

/// Content to store as a new user-provided Attachment.
#[derive(Clone, PartialEq, Eq)]
pub struct NewAttachment {
    pub content: Vec<u8>,
    /// A lowercase `type/subtype`, without parameters.
    pub mime: String,
    /// A file name to display. It is sanitized, see
    /// [`sanitize_display_name`]; an empty result is no name.
    pub display_name: Option<String>,
}

/// Shows the content by its size only.
impl fmt::Debug for NewAttachment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NewAttachment")
            .field("content", &self.content.len())
            .field("mime", &self.mime)
            .field("display_name", &self.display_name)
            .finish()
    }
}

/// The largest Attachment content OCG stores.
pub const MAX_ATTACHMENT_BYTES: usize = 16 * 1024 * 1024;
/// The longest MIME type and display name, in bytes.
pub const MAX_ATTACHMENT_MIME_BYTES: usize = 255;
pub const MAX_ATTACHMENT_NAME_BYTES: usize = 255;

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
    pub llm_transports: BTreeSet<LlmTransport>,
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
    pub value: Option<serde_json::Value>,
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

/// The wire protocol an LLM request is sent with. Each variant is an adapter
/// OCG implements; a Provider states which one it speaks, and an Executor
/// which ones it can run. Compatibility is never inferred from a model name
/// or an endpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LlmTransport {
    /// [`crate::openai_compatible`] Chat Completions.
    OpenAiChatCompletions,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewLlmProvider {
    pub name: String,
    pub transport: LlmTransport,
    pub endpoint: Option<String>,
    /// The name of a stored credential, never the secret itself.
    pub credential_ref: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LlmProviderRecord {
    pub id: LlmProviderId,
    pub name: String,
    pub transport: LlmTransport,
    pub endpoint: Option<String>,
    pub credential_ref: Option<String>,
    pub created_at: Timestamp,
}

/// A capability a user declares for a model.
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
    /// What the user states the model supports. Observed capability check
    /// results never change it.
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
/// edited while no submitted execution holds it; submitted Jobs keep their
/// own frozen copies of what they used.
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
/// Profile holds it until the Job is completed, failed or cancelled, or, for
/// an unknown outcome, until its execution is confirmed stopped. Queued,
/// started and paused Jobs hold it.
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
    pub transport: LlmTransport,
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
/// in its JobInput when it was submitted. `agent` is absent for work
/// submitted without an Agent Profile.
#[derive(Clone, Debug, PartialEq)]
pub struct FrozenExecutionConfig {
    pub agent: Option<AgentConfig>,
    pub llm: FrozenLlmConfig,
}

/// A submitted Job's configuration: the Profiles it was submitted with, as
/// provenance, and the values it was frozen with, as the authority for what
/// it executes. Later Profile edits change neither.
#[derive(Clone, Debug, PartialEq)]
pub struct SubmittedConfiguration {
    pub agent_profile: Option<ProfileId>,
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

/// Measurements of one request. Every measurement is optional: one that was
/// not measured or reported stays `None` and is never stored as zero.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Measurements {
    pub ttfb_ms: Option<u64>,
    pub reasoning_ttft_ms: Option<u64>,
    pub output_ttft_ms: Option<u64>,
    pub request_finished_in_ms: Option<u64>,
    pub usage: NormalizedUsage,
}

impl Measurements {
    fn is_empty(&self) -> bool {
        let usage = &self.usage;
        [self.ttfb_ms, self.reasoning_ttft_ms, self.output_ttft_ms, self.request_finished_in_ms].iter().all(Option::is_none)
            && [usage.input_tokens, usage.output_tokens, usage.cache_read_tokens, usage.cache_write_tokens, usage.reasoning_tokens]
                .iter()
                .all(Option::is_none)
            && usage.raw.is_none()
    }
}

/// Measurements taken for a Job, such as one of its LLM requests. Any Job
/// may be measured; one that runs no LLM has no LLM identity. Recording
/// them never changes the Job, and a Job may have any number of them.
#[derive(Clone, Debug, PartialEq)]
pub struct NewMetrics {
    pub job: JobId,
    /// The Provider request of the Job the record measures.
    pub request: RequestNumber,
    /// When the measured activity happened, as its producer reports it.
    /// Observation queries filter and order by this time.
    pub observed_at: Timestamp,
    pub measurements: Measurements,
    /// The producer's Token facts about this record, when it states any. A
    /// record may be annotated without measurements: an eligible one then
    /// establishes a request whose usage is missing. `None` is no trusted
    /// Token contract, not Excluded and not Final. An annotation needs the
    /// Job's frozen LLM configuration, whose Provider and model are the
    /// request's identity; a caller cannot name another.
    pub token: Option<TokenAnnotation>,
}

/// The LLM a Job ran, as its submission froze it: the LLM Profile as
/// provenance and the configuration the Job actually used. Later Profile
/// edits change neither.
#[derive(Clone, Debug, PartialEq)]
pub struct LlmTarget {
    pub llm_profile: ProfileId,
    pub frozen: FrozenLlmConfig,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MetricsRecord {
    pub id: MetricsId,
    /// Assembled from MEASURES.
    pub job: JobId,
    /// See [`NewMetrics::request`].
    pub request: RequestNumber,
    /// Read from the Job's frozen input; `None` for a Job without an LLM.
    pub target: Option<LlmTarget>,
    /// The producer's time, see [`NewMetrics::observed_at`].
    pub observed_at: Timestamp,
    pub measurements: Measurements,
    /// The producer's Token facts, see [`NewMetrics::token`].
    pub token: Option<TokenAnnotation>,
    /// The wall-clock time at which the database's write transaction that
    /// stored the record began, not when its commit completed. Wall-clock
    /// time is not monotonic. It is never used in place of `observed_at`.
    pub recorded_at: Timestamp,
}

/// The facts observed about one request that was sent to an LLM Provider.
/// A request refused locally before it was sent has no observation. These
/// are raw facts; [`AvailabilityObservation::classification`] derives their
/// availability meaning on demand, and nothing is stored.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AvailabilityObservation {
    /// The HTTP status the Provider answered with, when one was seen.
    pub status_code: Option<u16>,
    /// How the response ended, when it reported a finish reason.
    pub finish_reason: Option<ChatFinishReason>,
    pub raw_finish_reason: Option<String>,
    /// The Provider or transport failure as the transport reported it. It
    /// may accompany a response that had already begun.
    pub failure: Option<ProviderFailure>,
}

/// The availability meaning of one observation, derived from its structured
/// facts. It is a view, never stored, and never changes the observation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AvailabilityClassification {
    /// HTTP 200 whose stream reached a normal terminal finish reason.
    Available,
    /// Provider evidence of unavailability.
    Unavailable(UnavailableCause),
    /// A definite refusal. It says nothing about availability and is kept
    /// apart from the availability denominator.
    Rejected(RejectionCause),
    /// No valid terminal evidence, or evidence this classification does not
    /// support. Reportable, but neither success nor failure.
    Unconfirmed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnavailableCause {
    /// The Provider answered with an HTTP 5xx status.
    ProviderServerError,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RejectionCause {
    /// HTTP 429.
    RateLimited,
    /// HTTP 401 or 403.
    AuthOrPermission,
    /// HTTP 400, 404 or 422.
    RequestOrTarget,
}

impl AvailabilityObservation {
    fn is_empty(&self) -> bool {
        self.status_code.is_none()
            && self.finish_reason.is_none()
            && self.raw_finish_reason.is_none()
            && self.failure.is_none()
    }

    /// Classifies this observation from its observed HTTP status and finish
    /// reason. The failure text never decides a class: a failure without an
    /// observed status is Unconfirmed, because a timeout, reset or incomplete
    /// exchange does not prove that the Provider was unavailable.
    #[must_use]
    pub fn classification(&self) -> AvailabilityClassification {
        match self.status_code {
            Some(200) if self.normal_termination().is_some() => {
                AvailabilityClassification::Available
            }
            status => classify_status(status),
        }
    }

    /// The normal terminal finish reason this observation witnesses by
    /// itself, whatever its HTTP status: Stop or Length, with no failure
    /// beside it and no raw reason that names another finish reason. Missing
    /// usage is not a contradiction.
    fn normal_termination(&self) -> Option<ChatFinishReason> {
        let finish = self.finish_reason?;
        let normal = matches!(finish, ChatFinishReason::Stop | ChatFinishReason::Length);
        // A failure beside a finish reason is conflicting evidence, and a raw
        // reason that names a different finish reason contradicts the typed one.
        (normal
            && self.failure.is_none()
            && self
                .raw_finish_reason
                .as_deref()
                .is_none_or(|raw| raw == finish.as_openai_str()))
        .then_some(finish)
    }
}

/// The class of an HTTP status alone. Only the statuses approved for a class
/// have one; every other status, including 200 without a normal finish, is
/// Unconfirmed.
fn classify_status(status_code: Option<u16>) -> AvailabilityClassification {
    use AvailabilityClassification::{Rejected, Unavailable, Unconfirmed};
    match status_code {
        Some(500..=599) => Unavailable(UnavailableCause::ProviderServerError),
        Some(429) => Rejected(RejectionCause::RateLimited),
        Some(401 | 403) => Rejected(RejectionCause::AuthOrPermission),
        Some(400 | 404 | 422) => Rejected(RejectionCause::RequestOrTarget),
        Some(_) | None => Unconfirmed,
    }
}

/// One request sample: the evidence of one Provider request, reduced to one
/// classification. It is derived on demand from committed AvailabilityStat
/// records. It is never stored, and no record is changed. Several records of
/// one request add facts, not requests; each request of a Job is a sample of
/// its own.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AvailabilityRequestSample {
    pub job: JobId,
    pub request: RequestNumber,
    /// The lowest committed AvailabilityStat ID of the request.
    pub first_record: AvailabilityStatId,
    /// `observed_at` of `first_record`. Later records never move it.
    pub observed_at: Timestamp,
    pub classification: AvailabilityClassification,
}

/// Records of two different requests, of one Job or of two, were given to
/// the reduction of one request.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("evidence mixes {first} and {other}")]
pub struct MixedRequests {
    pub first: RequestRef,
    pub other: RequestRef,
}

impl AvailabilityRequestSample {
    /// Reduces the records of one request, in any order. The result depends
    /// only on the set of records, so a replay yields the same sample. Empty
    /// input is no sample.
    ///
    /// Explicit contradictions always make the sample Unconfirmed. They are:
    /// two different supplied HTTP statuses, where a failure's status counts
    /// as supplied, so that an absent status conflicts with nothing; two
    /// records, or one record, observed different finish reasons; a record
    /// whose raw finish reason names a different finish reason than its typed
    /// one; and a record whose failure carries an HTTP status that differs
    /// from its own observation's status.
    ///
    /// Otherwise the sample is Available when its observed HTTP status is 200
    /// and some record witnesses a normal termination on its own. The status
    /// and the termination may come from different records; a record with no
    /// status, or no finish, does not erase the other. A failure without a
    /// status, a missing field, or an Unconfirmed record is not a contradiction
    /// and is never a witness. Without that combination the observed HTTP
    /// status decides, as for a single observation. A failure status never
    /// classifies, and neither does a raw finish reason without a typed one.
    pub fn from_records<'a>(
        records: impl IntoIterator<Item = &'a AvailabilityStatRecord>,
    ) -> Result<Option<Self>, MixedRequests> {
        let mut evidence = RequestEvidence::default();
        for record in records {
            evidence.add(record)?;
        }
        Ok(evidence.sample())
    }
}

/// The structured claims that the records of one request have made so far.
/// It keeps claims, not records, so replay in any order reaches the same state.
#[derive(Clone, Default)]
pub(crate) struct RequestEvidence {
    pub(crate) request: Option<RequestRef>,
    /// The lowest record ID seen, with its observed time.
    pub(crate) first: Option<(AvailabilityStatId, Timestamp)>,
    /// The first HTTP status a record observed. Only an observed status
    /// classifies the request.
    pub(crate) observed_status: Option<u16>,
    /// The first HTTP status any record supplied, observed or on a failure.
    pub(crate) supplied_status: Option<u16>,
    /// The first finish reason observed.
    pub(crate) finish: Option<ChatFinishReason>,
    /// Some record witnesses a normal termination by itself.
    pub(crate) witness: bool,
    /// An explicit contradiction has been seen; see `from_records`.
    pub(crate) contradictory: bool,
}

impl RequestEvidence {
    /// Adds one record's facts. The reducer and incremental aggregation both
    /// use it, so their classifications cannot differ.
    pub(crate) fn add(&mut self, record: &AvailabilityStatRecord) -> Result<(), MixedRequests> {
        let request = RequestRef {
            job: record.job,
            request: record.request,
        };
        match self.request {
            Some(first) if first != request => {
                return Err(MixedRequests {
                    first,
                    other: request,
                });
            }
            _ => self.request = Some(request),
        }
        self.first = match self.first {
            Some((id, observed_at)) if id <= record.id => Some((id, observed_at)),
            _ => Some((record.id, record.observed_at)),
        };

        let observation = &record.observation;
        self.witness |= observation.normal_termination().is_some();
        if let Some(status) = observation.status_code {
            self.observed_status.get_or_insert(status);
        }
        // A missing status is absence, so only supplied statuses are compared.
        let failure_status = observation
            .failure
            .as_ref()
            .and_then(|failure| failure.status_code);
        for status in observation.status_code.into_iter().chain(failure_status) {
            match self.supplied_status {
                Some(first) => self.contradictory |= first != status,
                None => self.supplied_status = Some(status),
            }
        }
        if let Some(finish) = observation.finish_reason {
            match self.finish {
                Some(first) => self.contradictory |= first != finish,
                None => self.finish = Some(finish),
            }
            if let Some(raw) = observation.raw_finish_reason.as_deref() {
                self.contradictory |= ChatFinishReason::from_openai_str(raw) != finish;
            }
        }
        Ok(())
    }

    pub(crate) fn sample(&self) -> Option<AvailabilityRequestSample> {
        let (request, (first_record, observed_at)) = (self.request?, self.first?);
        let classification = if self.contradictory {
            AvailabilityClassification::Unconfirmed
        } else if self.witness && self.observed_status == Some(200) {
            AvailabilityClassification::Available
        } else {
            classify_status(self.observed_status)
        };
        Some(AvailabilityRequestSample {
            job: request.job,
            request: request.request,
            first_record,
            observed_at,
            classification,
        })
    }
}

/// Microseconds in one aggregation minute.
const MICROS_PER_MINUTE: i64 = 60_000_000;

/// The one-minute bucket `at` falls in: whole minutes since the Unix epoch,
/// so a negative time has a negative bucket. A sample's bucket is fixed by
/// its first record's `observed_at`.
#[must_use]
pub fn minute_of(at: Timestamp) -> i64 {
    at.as_unix_micros().div_euclid(MICROS_PER_MINUTE)
}

/// Requests per classification in one (Provider, model, minute) bucket.
/// Every count is of Provider requests, each counted once whichever Job sent
/// it, never of raw observations.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AvailabilityCounts {
    pub available: u64,
    pub provider_server_error: u64,
    pub rate_limited: u64,
    pub auth_or_permission: u64,
    pub request_or_target: u64,
    pub unconfirmed: u64,
}

impl AvailabilityCounts {
    /// The counter a classification is counted in.
    pub(crate) fn counter(&mut self, class: AvailabilityClassification) -> &mut u64 {
        use AvailabilityClassification::{Available, Rejected, Unavailable, Unconfirmed};
        match class {
            Available => &mut self.available,
            Unavailable(UnavailableCause::ProviderServerError) => &mut self.provider_server_error,
            Rejected(RejectionCause::RateLimited) => &mut self.rate_limited,
            Rejected(RejectionCause::AuthOrPermission) => &mut self.auth_or_permission,
            Rejected(RejectionCause::RequestOrTarget) => &mut self.request_or_target,
            Unconfirmed => &mut self.unconfirmed,
        }
    }
}

/// One request sample as materialized: its Job's frozen target and the
/// sample it reduces to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestSampleRecord {
    pub job: JobId,
    pub request: RequestNumber,
    pub llm_profile: ProfileId,
    pub provider: LlmProviderId,
    pub model: String,
    pub sample: AvailabilityRequestSample,
}

/// How far aggregation has consumed AvailabilityStat records.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AvailabilityAggregationProgress {
    /// The last committed AvailabilityStat ID consumed. `None` before any.
    pub consumed_through: Option<AvailabilityStatId>,
    /// Wall-clock time of the last committed batch. It is activity metadata,
    /// not progress, and it covers no observed time.
    pub updated_at: Option<Timestamp>,
}

/// A read together with the aggregation progress of the same snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadSnapshot<T> {
    pub progress: AvailabilityAggregationProgress,
    pub value: T,
}

/// What one committed aggregation batch consumed and wrote.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AvailabilityBatchReport {
    pub consumed_records: usize,
    pub consumed_through: AvailabilityStatId,
    /// Request samples whose stored state changed.
    pub samples_written: usize,
    /// Minute buckets whose stored counts changed.
    pub buckets_written: usize,
    /// Preparations this call made, including the one that committed.
    pub attempts: u8,
}

/// The result of one bounded aggregation step.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AvailabilityBatch {
    /// No committed record was unconsumed, so nothing was written.
    Idle {
        consumed_through: Option<AvailabilityStatId>,
    },
    Consumed(AvailabilityBatchReport),
    /// Every preparation was overtaken by another commit, so none was applied.
    Contended {
        attempts: u8,
    },
}

/// One availability observation of an LLM request made by a Job. The LLM it
/// concerns is the one the Job froze at submission, never a caller-supplied
/// target. The Job's outcome is neither consulted nor changed, so an
/// observation may be recorded, and stays, whatever the Job's outcome.
#[derive(Clone, Debug, PartialEq)]
pub struct NewAvailabilityStat {
    pub job: JobId,
    /// The Provider request of the Job the record observes.
    pub request: RequestNumber,
    /// When the request was observed, as its producer reports it.
    /// Observation queries filter and order by this time.
    pub observed_at: Timestamp,
    pub observation: AvailabilityObservation,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AvailabilityStatRecord {
    pub id: AvailabilityStatId,
    /// Assembled from OBSERVES.
    pub job: JobId,
    /// See [`NewAvailabilityStat::request`].
    pub request: RequestNumber,
    /// Read from the Job's frozen input.
    pub target: LlmTarget,
    /// The producer's time, see [`NewAvailabilityStat::observed_at`].
    pub observed_at: Timestamp,
    pub observation: AvailabilityObservation,
    /// The wall-clock time at which the database's write transaction that
    /// stored the record began, not when its commit completed. Wall-clock
    /// time is not monotonic. It is never used in place of `observed_at`.
    pub recorded_at: Timestamp,
}

/// A half-open time range, `from` included and `until` excluded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimeRange {
    pub from: Timestamp,
    pub until: Timestamp,
}

/// Selects Metrics or AvailabilityStats. Each `Some` field must match; a
/// record without an LLM matches only when every LLM field is `None`.
/// Provider and model are compared with the target its Job froze, never with
/// the current contents of any Profile, so records from before a Profile
/// edit stay selectable by what they actually ran. Choosing records this way
/// decides nothing about grouping, current validity or availability.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObservationFilter {
    /// Compared with the producer's observation time; the time a record was
    /// committed cannot be filtered on.
    pub observed: TimeRange,
    pub llm_profile: Option<ProfileId>,
    pub provider: Option<LlmProviderId>,
    /// Compared exactly with the frozen model name.
    pub model: Option<String>,
}

/// A position in observation order, which is `observed_at` and then record
/// ID, so records with equal times keep a stable order. A page continues
/// strictly after it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObservationCursor<I> {
    pub observed_at: Timestamp,
    pub record: I,
}

/// One page of an observation query. `next` is `None` once nothing in the
/// filter's time range remains after this page. A page may be empty while
/// `next` is set: the bounded scan behind it found no match, and the query
/// continues from `next`.
#[derive(Clone, Debug, PartialEq)]
pub struct ObservationPage<T, I> {
    pub records: Vec<T>,
    pub next: Option<ObservationCursor<I>>,
}

/// Selects historical (Provider, model) choices: the frozen targets of LLM
/// Metrics and AvailabilityStats observed within `observed`, which is
/// compared with the producer's observation time like
/// [`ObservationFilter::observed`]. A configured model nothing observed has
/// no choice here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObservedModelFilter {
    pub observed: TimeRange,
    pub provider: Option<LlmProviderId>,
}

/// A Provider and an exact, case-sensitive model name that observations
/// were recorded against, as their Jobs froze them. Choices are ordered by
/// Provider ID and then by the model name's UTF-8 bytes, and a choice is
/// also the cursor that continues strictly after it.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObservedModel {
    pub provider: LlmProviderId,
    pub model: String,
}

/// Which records within the time range a choice comes from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObservedModelChoice {
    pub model: ObservedModel,
    pub has_metrics: bool,
    pub has_availability_stats: bool,
}

/// One page of choices. `next` is `None` once no later pair exists; a page
/// may be empty while `next` is set, when every pair it examined had no
/// record within the time range.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObservedModelPage {
    pub choices: Vec<ObservedModelChoice>,
    pub next: Option<ObservedModel>,
}

/// The largest arguments of one tool call that OCG records, and so runs.
/// It is stricter than the transport's own argument limit,
/// [`crate::openai_compatible::MAX_TOOL_ARGUMENTS_BYTES`].
pub const MAX_STORED_TOOL_ARGUMENTS_BYTES: usize = 64 * 1024;
/// The largest sum of the recorded arguments of one Job's admitted calls.
pub const MAX_STORED_TOOL_ARGUMENTS_PER_JOB_BYTES: usize = 256 * 1024;
/// The largest sum of the parameter schemas recorded with one Job's
/// admitted calls. Each admission records the schema its arguments were
/// checked against, so this bounds what repeated calls of a tool store.
pub const MAX_STORED_TOOL_SCHEMAS_PER_JOB_BYTES: usize = 1024 * 1024;
/// The longest handler version a tool may declare.
pub const MAX_HANDLER_VERSION_BYTES: usize = 64;
const MAX_RECONCILER_BYTES: usize = 256;
const MAX_RECONCILIATION_TEXT_BYTES: usize = 4 * 1024;

/// One tool call a Provider request asked for: the request of the Job that
/// sent it and the call's wire index in that request's response. It keys the
/// call's [`ToolEvidenceRecord`], so a call has at most one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ToolCallRef {
    pub job: JobId,
    pub request: RequestNumber,
    pub index: u32,
}

impl fmt::Display for ToolCallRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "tool call {} of {} of {}",
            self.index, self.request, self.job
        )
    }
}

/// What a tool's handler may change outside OCG, as the trusted code that
/// implements and registers it declares.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ToolEffect {
    /// Changes nothing outside OCG, so running it again repeats nothing.
    ReadOnly,
    /// Running it again with the same arguments leaves what one run left.
    /// This must hold without an idempotency key: nothing OCG could derive
    /// from a call stays the same in a retry, which is another Job.
    Idempotent,
    /// Running it again may repeat its effect.
    NonIdempotent,
}

/// Why OCG let an admitted call run. OCG has no persistent permission
/// system: the tool registry's explicit grant is the only basis there is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthorizationBasis {
    RegistryGrant,
}

/// Tool arguments or a tool result, exactly as they were sent. Its `Debug`
/// shows only its size, so that neither reaches a log or an error message
/// by accident.
#[derive(Clone, PartialEq, Eq)]
pub struct ToolPayload(String);

impl ToolPayload {
    pub fn new(text: impl Into<String>) -> Self {
        Self(text.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Debug for ToolPayload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ToolPayload({} bytes)", self.0.len())
    }
}

/// The first durable fact about a call the Provider requested: whether OCG
/// refused it or admitted it to run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ToolDecision {
    /// OCG did not authorize the call, and never ran it.
    Refused(ToolCallRefusal),
    /// OCG accepted the call for execution. The handler may have been
    /// invoked only once this was durable.
    Admitted(ToolAdmission),
}

/// What OCG accepted for one execution: the exact registration, the
/// arguments as checked and why it was authorized.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToolAdmission {
    /// The version the handler's registration declared.
    pub handler_version: String,
    pub effect: ToolEffect,
    /// The parameter schema the registration declared, serialized as it is
    /// offered. The arguments were checked against it.
    pub parameters: String,
    /// The arguments exactly as the Provider sent them.
    pub arguments: ToolPayload,
    pub authorization: AuthorizationBasis,
}

/// A call the Provider requested, as OCG identified it, with OCG's decision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewToolEvidence {
    pub call: ToolCallRef,
    /// The id the Provider gave the call; unique within the Job.
    pub tool_call_id: String,
    /// The tool the call named.
    pub name: String,
    pub decision: ToolDecision,
}

/// Why a completed handler's result was not sent to the Provider.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WithheldReason {
    TooLarge,
    Unserializable,
}

/// Why an admitted call's outcome cannot be established.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UncertainCause {
    /// The handler was stopped at its time limit.
    TimedOut,
    /// The execution was cancelled while the handler ran.
    Cancelled,
    /// The handler reported a failure without evidence that its effect
    /// did not happen.
    HandlerReported,
}

/// What is known of an admitted call's execution. Only `Uncertain` leaves
/// the effect open; a missing outcome means the same.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ToolOutcome {
    /// The handler returned this result, which is the one sent on.
    Completed { result: ToolPayload },
    /// The handler returned, so its effect happened, but its result was not
    /// sent on.
    CompletedWithheld { reason: WithheldReason },
    /// The handler established that its effect did not happen.
    NotApplied { message: String },
    /// OCG never invoked the handler, for `reason`, after the admission.
    NotInvoked { reason: NotInvokedReason },
    /// The handler may have run and may have had its effect.
    Uncertain {
        cause: UncertainCause,
        message: Option<String>,
    },
}

/// Why OCG never invoked an admitted call's handler.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NotInvokedReason {
    /// Its final authorization check refused the call.
    Refused(ToolCallRefusal),
    /// The execution was cancelled before the handler's invocation.
    Cancelled,
}

/// The kind of a [`ToolOutcome`], without what it carries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ToolOutcomeKind {
    Completed,
    CompletedWithheld,
    NotApplied,
    NotInvoked,
    Uncertain,
}

impl ToolOutcome {
    pub fn kind(&self) -> ToolOutcomeKind {
        match self {
            Self::Completed { .. } => ToolOutcomeKind::Completed,
            Self::CompletedWithheld { .. } => ToolOutcomeKind::CompletedWithheld,
            Self::NotApplied { .. } => ToolOutcomeKind::NotApplied,
            Self::NotInvoked { .. } => ToolOutcomeKind::NotInvoked,
            Self::Uncertain { .. } => ToolOutcomeKind::Uncertain,
        }
    }

    /// Whether the outcome settles whether the effect happened.
    pub fn is_definite(&self) -> bool {
        !matches!(self, Self::Uncertain { .. })
    }
}

/// What a reconciliation established about an uncertain execution.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EffectConclusion {
    Applied,
    NotApplied,
}

/// A verified conclusion about an admitted call whose outcome was left
/// uncertain. Only trusted OCG code that verified it against the external
/// system may record one; OCG has none yet, so nothing outside the crate
/// can record one and no uncertainty is resolved in production.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToolReconciliation {
    pub conclusion: EffectConclusion,
    /// The trusted reconciler that verified it.
    pub reconciler: String,
    /// What it was verified against, such as the external system's record.
    pub evidence: String,
    pub explanation: String,
}

/// A fact and when its transaction recorded it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Recorded<T> {
    pub fact: T,
    pub at: Timestamp,
}

/// Everything recorded about one call a Provider requested. Each part is
/// written once and never changed or removed, whatever the Job's outcome.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToolEvidenceRecord {
    pub call: ToolCallRef,
    pub tool_call_id: String,
    pub name: String,
    pub decision: Recorded<ToolDecision>,
    pub outcome: Option<Recorded<ToolOutcome>>,
    pub reconciliation: Option<Recorded<ToolReconciliation>>,
}

impl ToolEvidenceRecord {
    pub fn admission(&self) -> Option<&ToolAdmission> {
        match &self.decision.fact {
            ToolDecision::Admitted(admission) => Some(admission),
            ToolDecision::Refused(_) => None,
        }
    }

    /// Whether the call was admitted and whether its effect happened is
    /// still open: no definite outcome and no reconciliation.
    pub fn is_unresolved(&self) -> bool {
        self.admission().is_some()
            && self.reconciliation.is_none()
            && !self
                .outcome
                .as_ref()
                .is_some_and(|outcome| outcome.fact.is_definite())
    }

    /// Whether the call's effect is known to have happened.
    pub fn is_applied(&self) -> bool {
        let completed = self.outcome.as_ref().is_some_and(|outcome| {
            matches!(
                outcome.fact,
                ToolOutcome::Completed { .. } | ToolOutcome::CompletedWithheld { .. }
            )
        });
        let reconciled = self.reconciliation.as_ref().is_some_and(|reconciliation| {
            reconciliation.fact.conclusion == EffectConclusion::Applied
        });
        self.admission().is_some() && (completed || reconciled)
    }
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
}

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
    /// The LLM configuration frozen for `configuration`, whose transport an
    /// assigned Executor must support.
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
        batch.require_executor_for(executor, job.frozen_llm.as_ref())?;
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

/// Creates a new Job that retries `retry.original` under the original's
/// parent. The original Job, its input and its outputs are not touched.
///
/// Only a settled original may be retried, and only while none of its
/// direct retries is unsettled; both are checked in the transaction that
/// creates the retry, so concurrent retries cannot both become active. The
/// original's tool effects are checked in the same transaction, see
/// [`require_repeatable_tool_effects`].
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
    if !original_row.settled() {
        return batch.reject(Violation::RetryOfUnsettledJob(original));
    }
    for existing in batch.retries(original)? {
        if !batch.require_job(existing)?.settled() {
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
    let appended = publish_output(batch, job, output.kind.into(), output.summary, output.code, output.goal_reached, output.value)?;
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

/// A confirmed outcome requires every child Job to be settled: a child with
/// an unknown outcome counts once its execution is confirmed stopped. A
/// child's own outcome does not decide the parent's; a required success is
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
        if !batch.require_job(child)?.settled() {
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
fn require_resolved_tool_effects(batch: &mut Batch, job: JobId) -> Result<(), BatchError> {
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
fn require_repeatable_tool_effects(
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
fn require_executing_locally(batch: &mut Batch, job: JobId) -> Result<(), BatchError> {
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
fn is_json_object(text: &str) -> bool {
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
/// they are. The Job must be settled, so nothing still executes for it.
/// The reconciler, its evidence and its explanation must each be given.
pub(crate) fn reconcile_tool_effect(
    batch: &mut Batch,
    call: ToolCallRef,
    reconciliation: ToolReconciliation,
) -> OpResult<()> {
    if !batch.require_job(call.job)?.settled() {
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
fn require_satisfied_dependencies(batch: &mut Batch, job: JobId) -> Result<(), BatchError> {
    for (prerequisite, condition) in batch.dependencies(job)? {
        let state = dependency_state(condition, &batch.require_job(prerequisite)?);
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
fn is_mime_type(mime: &str) -> bool {
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

fn record_metrics_checked(
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

#[cfg(test)]
mod classification_tests {
    use super::*;

    fn observation(
        status_code: Option<u16>,
        finish_reason: Option<ChatFinishReason>,
        raw_finish_reason: Option<&str>,
        failure: Option<&str>,
    ) -> AvailabilityObservation {
        AvailabilityObservation {
            status_code,
            finish_reason,
            raw_finish_reason: raw_finish_reason.map(str::to_owned),
            failure: failure.map(|message| ProviderFailure {
                message: message.to_owned(),
                status_code,
                ..ProviderFailure::default()
            }),
        }
    }

    #[test]
    fn classification_follows_only_observed_status_and_finish_facts() {
        use AvailabilityClassification::{Available, Rejected, Unavailable, Unconfirmed};
        use ChatFinishReason::{ContentFilter, Error, Length, Other, Stop, ToolCalls};
        let cases = [
            // HTTP 200 needs a normal terminal finish reason.
            (observation(Some(200), None, None, None), Unconfirmed),
            (
                observation(Some(200), Some(Stop), Some("stop"), None),
                Available,
            ),
            (observation(Some(200), Some(Stop), None, None), Available),
            (
                observation(Some(200), Some(Length), Some("length"), None),
                Available,
            ),
            (
                observation(Some(201), Some(Stop), Some("stop"), None),
                Unconfirmed,
            ),
            (
                observation(Some(200), Some(ToolCalls), Some("tool_calls"), None),
                Unconfirmed,
            ),
            (
                observation(Some(200), Some(ContentFilter), Some("content_filter"), None),
                Unconfirmed,
            ),
            (
                observation(Some(200), Some(Error), Some("error"), None),
                Unconfirmed,
            ),
            (
                observation(Some(200), Some(Other), Some("other"), None),
                Unconfirmed,
            ),
            // Conflicting facts are not evidence of completion.
            (
                observation(Some(200), Some(Stop), Some("length"), None),
                Unconfirmed,
            ),
            (
                observation(Some(200), Some(Stop), Some("stop"), Some("stream reset")),
                Unconfirmed,
            ),
            (
                observation(Some(200), None, Some("stop"), None),
                Unconfirmed,
            ),
            (
                observation(None, Some(Stop), Some("stop"), None),
                Unconfirmed,
            ),
            // Provider HTTP 5xx is unavailability.
            (
                observation(Some(500), None, None, Some("server error")),
                Unavailable(UnavailableCause::ProviderServerError),
            ),
            (
                observation(Some(503), None, None, Some("overloaded")),
                Unavailable(UnavailableCause::ProviderServerError),
            ),
            (
                observation(Some(599), None, None, None),
                Unavailable(UnavailableCause::ProviderServerError),
            ),
            // Definite refusals are kept apart by cause.
            (
                observation(Some(429), None, None, Some("rate limited")),
                Rejected(RejectionCause::RateLimited),
            ),
            (
                observation(Some(401), None, None, Some("bad key")),
                Rejected(RejectionCause::AuthOrPermission),
            ),
            (
                observation(Some(403), None, None, Some("forbidden")),
                Rejected(RejectionCause::AuthOrPermission),
            ),
            (
                observation(Some(400), None, None, Some("bad body")),
                Rejected(RejectionCause::RequestOrTarget),
            ),
            (
                observation(Some(404), None, None, Some("no model")),
                Rejected(RejectionCause::RequestOrTarget),
            ),
            (
                observation(Some(422), None, None, Some("invalid")),
                Rejected(RejectionCause::RequestOrTarget),
            ),
            // Statuses with no approved mapping are not invented into a class.
            (
                observation(Some(408), None, None, Some("timeout")),
                Unconfirmed,
            ),
            (observation(Some(302), None, None, None), Unconfirmed),
            // A transport failure without an observed Provider status is not proof.
            (
                observation(None, None, None, Some("connection reset")),
                Unconfirmed,
            ),
            (observation(None, None, None, None), Unconfirmed),
        ];
        for (observation, expected) in cases {
            assert_eq!(
                observation.classification(),
                expected,
                "classifying {observation:?}"
            );
        }
    }
}
