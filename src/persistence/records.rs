use super::*;

/// A Job assembled from its immutable `job` row, its mutable `job_state` row
/// and the derived tables that describe it.
#[derive(Clone, Debug, PartialEq)]
pub struct JobRecord {
    pub id: JobId,
    pub name: Option<String>,
    /// When the Job's claim was made.
    pub started_at: Option<Timestamp>,
    pub completed_at: Option<Timestamp>,
    pub failed_at: Option<Timestamp>,
    pub cancelled_at: Option<Timestamp>,
    pub unknown_at: Option<Timestamp>,
    /// When the trusted execution path confirmed that an unknown-outcome
    /// Job's execution has actually stopped. The outcome stays unknown.
    pub stop_confirmed_at: Option<Timestamp>,
    pub stop_confirmed_by: Option<StopProof>,
    pub created_at: Timestamp,
    /// The latest time one of the Job's lifecycle facts was recorded.
    pub updated_at: Timestamp,
    /// The Job's own input, which its spec holds: the identity is the Job's.
    pub input: JobInputId,
    /// The parent Job; 0 for a top-level Job.
    pub parent: JobId,
    pub parent_id: Option<JobId>,
    pub kind: JobKind,
    pub conversation: Option<JobId>,
    pub objective: Option<String>,
    /// The Agent and Model configs the Job was submitted with, as provenance.
    pub configuration: Option<ExecutionConfiguration>,
    /// The Pool the Job was claimed into.
    pub assigned_executor: Option<ExecutorId>,
    pub previous: Option<JobId>,
    pub claim: Option<ClaimRecord>,
    pub outcome: Option<OutcomeRecord>,
    pub children_closed_at: Option<Timestamp>,
    /// Whether the Job has no `unsettled` row: it and every Job below it
    /// have stopped.
    pub subtree_settled: bool,
}

impl JobRecord {
    pub fn confirmed_terminal(&self) -> bool {
        self.completed_at.is_some() || self.failed_at.is_some() || self.cancelled_at.is_some()
    }

    pub fn any_terminal(&self) -> bool {
        self.confirmed_terminal() || self.unknown_at.is_some()
    }

    pub fn awaits_start(&self) -> bool {
        self.started_at.is_none() && !self.any_terminal()
    }

    pub fn locally_stopped(&self) -> bool {
        self.confirmed_terminal() || (self.unknown_at.is_some() && self.stop_confirmed_at.is_some())
    }

    pub fn settled(&self) -> bool {
        self.subtree_settled
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum OutcomeKind {
    Completed = 1,
    Failed = 2,
    Cancelled = 3,
    Unknown = 4,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OutcomeCause {
    Code(u8),
    Text(String),
    Abandoned,
    DependencyImpossible,
    Stopped { record: u32 },
    Closed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutcomeRecord {
    pub kind: OutcomeKind,
    pub at: Timestamp,
    pub cause: Option<OutcomeCause>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClaimRecord {
    pub started_at: Timestamp,
    pub pool_id: ConfigId,
    pub pool_edit_seq: u32,
    pub capabilities: BTreeSet<(ExecutionOperation, Option<LlmTransport>)>,
    pub epoch: u64,
    pub host_id: Option<String>,
    pub boot_id: Option<String>,
    pub notice_seq: u32,
}

/// A position in the order of Jobs that have been submitted but have not
/// started or reached an outcome: Job ID alone. IDs are allocated in the
/// single write transaction, so this order follows commits independently
/// of the worker's wall clock.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct PendingJobKey {
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
    pub last_visited: Option<JobId>,
    pub exhausted: bool,
}

/// A Job's input as its spec holds it: the objective, limits, Attachments and
/// frozen configuration. Its identity is the Job's.
#[derive(Clone, Debug, PartialEq)]
pub struct JobInputRecord {
    pub id: JobInputId,
    pub objective: Option<String>,
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

impl fmt::Display for SubmissionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in &self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
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
    pub seq: u32,
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

/// A Pool config seen as the Executor that runs work.
#[derive(Clone, Debug, PartialEq)]
pub struct ExecutorRecord {
    pub id: ExecutorId,
    pub name: String,
    pub concurrency_limit: u32,
    /// The LLM transports the Pool offers `LlmRequest` for.
    pub llm_transports: BTreeSet<LlmTransport>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
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

/// What the owner changes about a conversation, which is a chat thread
/// root. The Jobs of the conversation are never changed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConversationEdit {
    Title(String),
    SuggestedTitle(String),
    /// Hides it from the conversation list, for good.
    Hide,
}

/// A command a Project offers its Agents by name. `argv[0]` is a bare
/// program name or an absolute path; it runs in the Project's root with the
/// same filtered environment and timeout as every other command.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectCommand {
    pub name: String,
    pub argv: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewProject {
    pub name: String,
    pub description: Option<String>,
    /// The canonical, UTF-8 root the control plane resolved. It is fixed
    /// for the life of the Project.
    pub root: String,
    pub commands: Vec<ProjectCommand>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectRecord {
    pub id: ProjectId,
    pub name: String,
    pub description: Option<String>,
    pub root: String,
    /// In the order given, so that their encoding is stable.
    pub commands: Vec<ProjectCommand>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

/// The commands every Project offers with a `code` grant.
pub const BUILT_IN_COMMANDS: [&str; 2] = ["cargo_check", "cargo_test"];

/// The most commands a Project may have.
const MAX_PROJECT_COMMANDS: usize = 32;
/// The most the encoded commands may take, because they are sent to the
/// Provider with every request.
const MAX_PROJECT_COMMANDS_BYTES: usize = 16 * 1024;
const MAX_COMMAND_NAME_BYTES: usize = 64;
const MAX_ARGV_ENTRIES: usize = 64;
const MAX_ARGV_ENTRY_BYTES: usize = 4096;

/// Why `commands` cannot be a Project's. Create and update share it.
pub fn validate_project_commands(commands: &[ProjectCommand]) -> Result<(), &'static str> {
    if commands.len() > MAX_PROJECT_COMMANDS {
        return Err("a Project has at most 32 commands");
    }
    // As stored: a 4-byte count, then each string with a 4-byte length.
    let mut encoded = 4usize;
    for (index, command) in commands.iter().enumerate() {
        let mut name = command.name.bytes();
        let well_formed = command.name.len() <= MAX_COMMAND_NAME_BYTES
            && name.next().is_some_and(|byte| byte.is_ascii_lowercase())
            && name.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_');
        if !well_formed {
            return Err("a command name matches [a-z][a-z0-9_]{0,63}");
        }
        if BUILT_IN_COMMANDS.contains(&command.name.as_str()) {
            return Err("cargo_check and cargo_test are built in");
        }
        if commands[..index]
            .iter()
            .any(|earlier| earlier.name == command.name)
        {
            return Err("command names are unique");
        }
        if command.argv.is_empty() || command.argv.len() > MAX_ARGV_ENTRIES {
            return Err("a command has 1 to 64 argv entries");
        }
        if command.argv.iter().any(|entry| {
            entry.is_empty() || entry.len() > MAX_ARGV_ENTRY_BYTES || entry.contains('\0')
        }) {
            return Err("each argv entry is 1 to 4096 bytes without NUL");
        }
        let program = &command.argv[0];
        if program.contains('/') && !program.starts_with('/') {
            return Err("argv[0] is a bare program name or an absolute path");
        }
        encoded += 4 + command.name.len() + 4;
        encoded += command
            .argv
            .iter()
            .map(|entry| 4 + entry.len())
            .sum::<usize>();
    }
    if encoded > MAX_PROJECT_COMMANDS_BYTES {
        return Err("the commands take at most 16 KiB");
    }
    Ok(())
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
    pub reasoning_effort: Option<String>,
    pub max_completion_tokens: Option<u32>,
    pub presence_penalty: Option<f64>,
    pub frequency_penalty: Option<f64>,
}

/// What a Model config holds of the model itself; its Connection is a
/// reference, see [`ModelConfig::connection_id`].
#[derive(Clone, Debug, PartialEq)]
pub struct LlmConfig {
    pub model: String,
    pub context_window: Option<u32>,
    pub parameters: LlmParameters,
    /// What the user states the model supports. Observed capability check
    /// results never change it.
    pub declared_capabilities: BTreeSet<ModelCapability>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AgentBehavior {
    Chat = 1,
    General = 2,
}

/// The body of an Agent config.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentConfig {
    pub instructions: Option<String>,
    pub behavior: AgentBehavior,
    pub models: Vec<ConfigId>,
    pub default_model: Option<ConfigId>,
    pub dispatch_to: Vec<ConfigId>,
    pub approve: Vec<ApproveClass>,
    pub tool_grant: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ApproveClass {
    Commands,
    Writes,
    Dispatch,
    Close,
}

impl ApproveClass {
    pub fn of(tool_name: &str) -> Option<Self> {
        match tool_name {
            "workspace_run_command" => Some(Self::Commands),
            "workspace_write_file" => Some(Self::Writes),
            "job_dispatch" => Some(Self::Dispatch),
            "job_close" => Some(Self::Close),
            _ => None,
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Commands => "commands",
            Self::Writes => "writes",
            Self::Dispatch => "dispatch",
            Self::Close => "close",
        }
    }
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            instructions: None,
            behavior: AgentBehavior::General,
            models: Vec::new(),
            default_model: None,
            dispatch_to: Vec::new(),
            approve: Vec::new(),
            tool_grant: None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ConfigKind {
    Connection = 1,
    Model = 2,
    Agent = 3,
    Pool = 4,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConnectionConfig {
    pub protocol: LlmTransport,
    pub endpoint: Option<String>,
    pub credential_ref: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ModelConfig {
    pub connection_id: ConfigId,
    pub model: String,
    pub context_window: Option<u32>,
    pub parameters: LlmParameters,
    pub declared_capabilities: BTreeSet<ModelCapability>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PoolConfig {
    pub capabilities: BTreeSet<(ExecutionOperation, Option<LlmTransport>)>,
    pub concurrency_limit: u32,
    pub enabled: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ConfigBody {
    Connection(ConnectionConfig),
    Model(ModelConfig),
    Agent(AgentConfig),
    Pool(PoolConfig),
}

impl ConfigBody {
    pub fn kind(&self) -> ConfigKind {
        match self {
            Self::Connection(_) => ConfigKind::Connection,
            Self::Model(_) => ConfigKind::Model,
            Self::Agent(_) => ConfigKind::Agent,
            Self::Pool(_) => ConfigKind::Pool,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ConfigRecord {
    pub id: ConfigId,
    pub kind: ConfigKind,
    pub name: String,
    pub edit_seq: u32,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    pub archived_at: Option<Timestamp>,
    pub body: ConfigBody,
}

#[derive(Clone, Debug, PartialEq)]
pub struct NewConfig {
    pub name: String,
    pub body: ConfigBody,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RequestPayloadKind {
    Measurement = 1,
    Availability = 2,
    Token = 3,
    Sent = 4,
    Received = 5,
    ApprovedCalls = 6,
}

#[derive(Clone, Debug, PartialEq)]
pub enum RequestPayload {
    ApprovedCalls,
    Sent {
        messages: Vec<ContentDigest>,
        tools: Option<ContentDigest>,
        parameters: ContentDigest,
        dropped_turns: Option<u32>,
    },
    Received {
        digest: ContentDigest,
    },
    Measurement {
        measurements: Measurements,
    },
    Availability {
        status_code: Option<u16>,
        finish_reason: Option<ChatFinishReason>,
        raw_finish_reason: Option<String>,
        failure: Option<ProviderFailure>,
    },
    Token {
        eligibility: RequestEligibility,
        finality: UsageFinality,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct RequestEventRecord {
    pub job_id: JobId,
    pub request: RequestNumber,
    pub seq: u32,
    pub observed_at: Timestamp,
    pub recorded_at: Timestamp,
    pub payload: RequestPayload,
}

pub type ToolCallRecord = super::tool_evidence::ToolEvidenceRecord;

/// The Provider facts needed to interpret a request. The credential is
/// named, never copied.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderSnapshot {
    pub id: LlmProviderId,
    /// The Connection's edit counter when it was frozen.
    pub edit_seq: u32,
    pub name: String,
    pub transport: LlmTransport,
    pub endpoint: Option<String>,
    pub credential_ref: Option<String>,
}

/// A config row and the edit counter it had when a Job froze its values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConfigRef {
    pub id: ConfigId,
    pub edit_seq: u32,
}

/// A Model config as it was when frozen: model, request parameters,
/// declared capabilities and the Connection that serves them.
#[derive(Clone, Debug, PartialEq)]
pub struct FrozenLlmConfig {
    pub config: LlmConfig,
    pub provider: ProviderSnapshot,
    /// The Model config the values came from.
    pub model: ConfigRef,
}

/// What a submitted Job was configured to execute, frozen in its spec when
/// it was submitted. `agent` is absent for work submitted without an Agent.
#[derive(Clone, Debug, PartialEq)]
pub struct FrozenExecutionConfig {
    pub agent: Option<AgentConfig>,
    /// The Agent config `agent` came from.
    pub agent_ref: Option<ConfigRef>,
    pub llm: FrozenLlmConfig,
}

/// A submitted Job's configuration: the Agent and Model configs it was
/// submitted with, as provenance, and the values it was frozen with, as the
/// authority for what it executes. Later config edits change neither.
#[derive(Clone, Debug, PartialEq)]
pub struct SubmittedConfiguration {
    pub agent_profile: Option<ProfileId>,
    pub llm_profile: ProfileId,
    pub frozen: FrozenExecutionConfig,
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
        [
            self.ttfb_ms,
            self.reasoning_ttft_ms,
            self.output_ttft_ms,
            self.request_finished_in_ms,
        ]
        .iter()
        .all(Option::is_none)
            && [
                usage.input_tokens,
                usage.output_tokens,
                usage.cache_read_tokens,
                usage.cache_write_tokens,
                usage.reasoning_tokens,
            ]
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

/// The LLM a Job ran, as its submission froze it: the Model config as
/// provenance and the configuration the Job actually used. Later config
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ExecutionOperation {
    LlmRequest,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CapabilitySet {
    pub operations: BTreeSet<ExecutionOperation>,
    pub llm_protocols: BTreeSet<LlmTransport>,
}

impl CapabilitySet {
    pub fn supports(&self, operation: ExecutionOperation, protocol: Option<LlmTransport>) -> bool {
        self.operations.contains(&operation)
            && protocol.is_none_or(|protocol| self.llm_protocols.contains(&protocol))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionPoolConfig {
    pub concurrency_limit: NonZeroU32,
    pub capabilities: CapabilitySet,
}

impl ExecutionPoolConfig {
    /// The Pool body this configuration is stored as.
    pub fn body(&self, enabled: bool) -> PoolConfig {
        let mut capabilities = BTreeSet::new();
        for operation in &self.capabilities.operations {
            if self.capabilities.llm_protocols.is_empty() {
                capabilities.insert((*operation, None));
            }
            for protocol in &self.capabilities.llm_protocols {
                capabilities.insert((*operation, Some(*protocol)));
            }
        }
        PoolConfig {
            capabilities,
            concurrency_limit: self.concurrency_limit.get(),
            enabled,
        }
    }
}

impl PoolConfig {
    /// The capabilities of this Pool as operations and protocols.
    pub fn capability_set(&self) -> CapabilitySet {
        let mut set = CapabilitySet::default();
        for (operation, protocol) in &self.capabilities {
            set.operations.insert(*operation);
            if let Some(protocol) = protocol {
                set.llm_protocols.insert(*protocol);
            }
        }
        set
    }

    /// Whether this Pool offers `operation` for `protocol`. A capability
    /// without a protocol offers only work that needs none.
    pub fn offers(&self, operation: ExecutionOperation, protocol: Option<LlmTransport>) -> bool {
        self.capabilities.contains(&(operation, protocol))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewExecutionPool {
    pub name: String,
    pub config: ExecutionPoolConfig,
    pub enabled: bool,
}

/// A Pool config as the scheduler's callers see it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionPool {
    pub id: ExecutionPoolId,
    pub name: String,
    pub config: ExecutionPoolConfig,
    pub enabled: bool,
    pub edit_seq: u32,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

/// Aggregated statistics of one tool's admissions and execution outcomes.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ToolStatRecord {
    pub name: String,
    pub admitted: u64,
    pub refused_arguments: u64,
    pub completed: u64,
    pub failed: u64,
    pub duration_us_total: u64,
    pub duration_us_max: u64,
}

/// The width of one usage summary bucket: one hour. A summary rounds its
/// lower bound down to a bucket and its upper bound up, so the buckets it
/// covers are those an instant in `[from_us, until_us)` falls in.
pub const USAGE_SUMMARY_HOUR_MICROS: i64 = 3_600_000_000;

/// One aggregated row in a token usage summary breakdown. `agent` is 0 for
/// work submitted without an Agent; the route reports it as `profile:N` or
/// `null`.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UsageSummaryRow {
    pub agent: u64,
    pub provider: u64,
    pub model: Option<String>,
    pub requests: u64,
    pub complete_requests: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_write_tokens: u64,
    pub reasoning_tokens: u64,
}

/// The aggregated token usage summary response. `complete` says the scan
/// finished; `resume_from_us` is set exactly when it did not.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UsageSummaryResponse {
    pub from_us: i64,
    pub until_us: i64,
    pub complete: bool,
    pub resume_from_us: Option<i64>,
    pub rows: Vec<UsageSummaryRow>,
}

/// Optional filters for querying usage summaries.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UsageSummaryFilter {
    pub from_us: Option<i64>,
    pub until_us: Option<i64>,
    pub agent: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum UsageBucketWidth {
    #[default]
    Day,
    Week,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UsageBucketing {
    pub width: UsageBucketWidth,
    pub offset_minutes: i32,
}

impl UsageBucketing {
    pub fn start_of(self, us: i64) -> Option<i64> {
        const DAY: i64 = 86_400_000_000;
        let offset = i64::from(self.offset_minutes).checked_mul(60_000_000)?;
        let (width, anchor) = match self.width {
            UsageBucketWidth::Day => (DAY, 0),
            UsageBucketWidth::Week => (7 * DAY, 4 * DAY),
        };
        us.checked_add(offset)?
            .checked_sub(anchor)?
            .div_euclid(width)
            .checked_mul(width)?
            .checked_add(anchor)?
            .checked_sub(offset)
    }

    pub fn end_of(self, us: i64) -> Option<i64> {
        let width = match self.width {
            UsageBucketWidth::Day => 86_400_000_000,
            UsageBucketWidth::Week => 7 * 86_400_000_000,
        };
        self.start_of(us)?.checked_add(width)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UsageBucketRow {
    pub bucket_us: i64,
    pub row: UsageSummaryRow,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UsageBucketResponse {
    pub from_us: i64,
    pub until_us: i64,
    pub complete: bool,
    pub resume_from_us: Option<i64>,
    pub rows: Vec<UsageBucketRow>,
}
