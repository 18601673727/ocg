use super::*;

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

/// A position in the order of Jobs that have been submitted but have not
/// started or reached an outcome: creation time, then Job ID, so Jobs created
/// in the same microsecond keep a stable order. A creation time is the
/// database worker's wall clock, so it can run backwards across Job IDs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct PendingJobKey {
    pub created_at: Timestamp,
    pub job: JobId,
}

/// A bounded run of the pending Jobs in key order, read in one short
/// transaction. `visited` holds every index entry the read examined and each
/// references an existing Job that was pending in that transaction. Nothing
/// is filtered here, and a page never examines more entries than its limit.
/// `exhausted` is set only when the read went past the last entry; a page
/// that used its whole limit says nothing about what follows it, and the
/// next page starts strictly after `last_visited`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingJobsPage {
    pub visited: Vec<PendingJobKey>,
    pub last_visited: Option<PendingJobKey>,
    pub exhausted: bool,
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

/// What a client's idempotency key is recorded as: a SHA-256 digest of it,
/// computed by its caller. The key itself is never stored, and only the
/// first bytes of the digest are ever displayed.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SubmissionKey([u8; 32]);

impl SubmissionKey {
    #[must_use]
    pub const fn from_digest(digest: [u8; 32]) -> Self {
        Self(digest)
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for SubmissionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SubmissionKey(")?;
        self.0[..4]
            .iter()
            .try_for_each(|byte| write!(f, "{byte:02x}"))?;
        f.write_str("…)")
    }
}

/// A SHA-256 digest of a submission's normalized request, computed by its
/// caller, so a key reused for another request is told apart.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct RequestFingerprint([u8; 32]);

impl RequestFingerprint {
    #[must_use]
    pub const fn from_digest(digest: [u8; 32]) -> Self {
        Self(digest)
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for RequestFingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RequestFingerprint(")?;
        self.0[..4]
            .iter()
            .try_for_each(|byte| write!(f, "{byte:02x}"))?;
        f.write_str("…)")
    }
}

/// The idempotency of one submitted Job: recorded in the transaction that
/// creates the Job, and refused there when its key is already recorded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NewSubmission {
    pub key: SubmissionKey,
    pub fingerprint: RequestFingerprint,
}

/// A recorded submission: the one Job its key ever created, and the request
/// that created it. Never changed once committed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SubmissionRecord {
    pub key: SubmissionKey,
    pub fingerprint: RequestFingerprint,
    pub job: JobId,
    /// When the Job was created with it.
    pub recorded_at: Timestamp,
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
/// see [`crate::database::DatabaseHandle::cleanup_attachment`].
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
    pub(super) fn is_empty(&self) -> bool {
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
