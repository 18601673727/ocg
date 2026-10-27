use super::*;

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

/// The wire protocol an LLM request is sent with. Each variant is an adapter
/// OCG implements; a Provider states which one it speaks, and an Executor
/// which ones it can run. Compatibility is never inferred from a model name
/// or an endpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LlmTransport {
    /// [`crate::service::ai::providers::openai_compatible`] Chat Completions.
    OpenAiChatCompletions,
    OpenAiResponses,
    AnthropicMessages,
}

impl LlmTransport {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OpenAiChatCompletions => "openai_chat_completions",
            Self::OpenAiResponses => "openai_responses",
            Self::AnthropicMessages => "anthropic_messages",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "openai_chat_completions" => Some(Self::OpenAiChatCompletions),
            "openai_responses" => Some(Self::OpenAiResponses),
            "anthropic_messages" => Some(Self::AnthropicMessages),
            _ => None,
        }
    }
}

/// A capability a user declares for a model.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ModelCapability {
    Chat,
    Tools,
    Vision,
    Reasoning,
}

impl ModelCapability {
    pub fn code(self) -> u8 {
        match self {
            Self::Chat => 1,
            Self::Tools => 2,
            Self::Vision => 3,
            Self::Reasoning => 4,
        }
    }
    pub fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::Chat),
            2 => Some(Self::Tools),
            3 => Some(Self::Vision),
            4 => Some(Self::Reasoning),
            _ => None,
        }
    }
}

/// A capability a user declares for a decision model.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum DecisionCapability {
    Choice,
    OrdinalScore,
    BinaryProbability,
}

impl DecisionCapability {
    pub fn code(self) -> u8 {
        match self {
            Self::Choice => 1,
            Self::OrdinalScore => 2,
            Self::BinaryProbability => 3,
        }
    }
    pub fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::Choice),
            2 => Some(Self::OrdinalScore),
            3 => Some(Self::BinaryProbability),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionProtocol {
    /// TypeSafe System One REST JSON protocol.
    TypeSafeSystemOne,
}

impl DecisionProtocol {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TypeSafeSystemOne => "typesafe_systemone",
        }
    }
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "typesafe_systemone" => Some(Self::TypeSafeSystemOne),
            _ => None,
        }
    }
    pub fn code(self) -> u8 {
        match self {
            Self::TypeSafeSystemOne => 1,
        }
    }
    pub fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::TypeSafeSystemOne),
            _ => None,
        }
    }
}

/// Request parameters the OpenAI-compatible transport already forwards.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ReasoningEffort {
    None,
    Minimal,
    Low,
    Medium,
    High,
    Xhigh,
    Max,
}

impl ReasoningEffort {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "none" => Some(Self::None),
            "minimal" => Some(Self::Minimal),
            "low" => Some(Self::Low),
            "medium" => Some(Self::Medium),
            "high" => Some(Self::High),
            "xhigh" => Some(Self::Xhigh),
            "max" => Some(Self::Max),
            _ => None,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Minimal => "minimal",
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::Xhigh => "xhigh",
            Self::Max => "max",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiscoveredModel {
    pub id: String,
    pub context_window: Option<u32>,
    pub reasoning_efforts: Option<Vec<String>>,
}

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

#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    ts_rs::TS,
)]
#[serde(rename_all = "snake_case")]
pub enum AgentCapability {
    Chat,
    Coding,
    Decision,
}

impl AgentCapability {
    pub fn code(self) -> u8 {
        match self {
            Self::Chat => 1,
            Self::Coding => 2,
            Self::Decision => 3,
        }
    }
    pub fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::Chat),
            2 => Some(Self::Coding),
            3 => Some(Self::Decision),
            _ => None,
        }
    }
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "chat" => Some(Self::Chat),
            "coding" => Some(Self::Coding),
            "decision" => Some(Self::Decision),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentConfig {
    pub instructions: Option<String>,
    pub capabilities: BTreeSet<AgentCapability>,
    pub models: Vec<ConfigId>,
    pub compaction_policy: Option<CompactionPolicy>,
}

impl AgentConfig {
    pub fn default_model(&self) -> Option<ConfigId> {
        self.models.first().copied()
    }
    pub fn workspace_grant(&self) -> Option<&'static str> {
        self.capabilities
            .contains(&AgentCapability::Coding)
            .then_some("code")
    }
    pub fn manages_jobs(&self) -> bool {
        self.capabilities.contains(&AgentCapability::Chat)
    }
    pub fn supports_model(&self, body: &ConfigBody) -> bool {
        !self.capabilities.is_empty()
            && self.capabilities.iter().all(|capability| match body {
                ConfigBody::Model(model) => {
                    model.declared_capabilities.contains(&ModelCapability::Chat)
                        && match capability {
                            AgentCapability::Coding => model
                                .declared_capabilities
                                .contains(&ModelCapability::Tools),
                            AgentCapability::Chat => true,
                            AgentCapability::Decision => true,
                        }
                }
                ConfigBody::DecisionModel(model) => {
                    *capability == AgentCapability::Decision
                        && !model.declared_capabilities.is_empty()
                }
                ConfigBody::Agent(_)
                | ConfigBody::DecisionAgent(_)
                | ConfigBody::Connection(_)
                | ConfigBody::Pool(_) => false,
            })
    }
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            instructions: None,
            capabilities: BTreeSet::from([AgentCapability::Chat]),
            models: Vec::new(),
            compaction_policy: None,
        }
    }
}

/// Limits for a decision model.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionLimits {
    pub max_input_tokens_per_request: Option<u32>,
    pub max_output_tokens_per_request: Option<u32>,
    pub max_input_tokens_per_question: Option<u32>,
    pub max_output_tokens_per_question: Option<u32>,
}

/// The body of a Decision Model config.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionModelConfig {
    pub connection_id: ConfigId,
    pub model: String,
    pub protocol: DecisionProtocol,
    pub limits: DecisionLimits,
    pub declared_capabilities: BTreeSet<DecisionCapability>,
}

/// The body of a Decision Agent config.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionAgentConfig {
    pub instructions: Option<String>,
    pub models: Vec<ConfigId>,
    pub default_model: Option<ConfigId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ConfigKind {
    Connection = 1,
    Model = 2,
    Agent = 3,
    Pool = 4,
    DecisionModel = 5,
    DecisionAgent = 6,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConnectionConfig {
    pub protocol: LlmTransport,
    pub endpoint: Option<String>,
    pub credential_ref: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ModelConfig {
    pub reasoning_efforts: Option<Vec<String>>,
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
#[allow(clippy::large_enum_variant)] // Configuration snapshots remain inline, like JobExecution.
pub enum ConfigBody {
    Connection(ConnectionConfig),
    Model(ModelConfig),
    Agent(AgentConfig),
    Pool(PoolConfig),
    DecisionModel(DecisionModelConfig),
    DecisionAgent(DecisionAgentConfig),
}

impl ConfigBody {
    pub fn kind(&self) -> ConfigKind {
        match self {
            Self::Connection(_) => ConfigKind::Connection,
            Self::Model(_) => ConfigKind::Model,
            Self::Agent(_) => ConfigKind::Agent,
            Self::Pool(_) => ConfigKind::Pool,
            Self::DecisionModel(_) => ConfigKind::DecisionModel,
            Self::DecisionAgent(_) => ConfigKind::DecisionAgent,
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
pub struct Config {
    pub name: String,
    pub body: ConfigBody,
}

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
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ExecutionOperation {
    LlmRequest,
    /// One typed decision request over a Decision Model's native protocol.
    /// Pools offering it are candidates for ordinary decision Jobs.
    Decision,
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
            // Decision work names no LLM transport, so its Pool offers it
            // without one whether or not the same Pool serves generative
            // work over transports.
            if *operation == ExecutionOperation::Decision
                || self.capabilities.llm_protocols.is_empty()
            {
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
pub struct ExecutionPool {
    pub name: String,
    pub config: ExecutionPoolConfig,
    pub enabled: bool,
}

/// A Pool config as the scheduler's callers see it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionPoolRecord {
    pub id: ExecutionPoolId,
    pub name: String,
    pub config: ExecutionPoolConfig,
    pub enabled: bool,
    pub edit_seq: u32,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", content = "config", rename_all = "snake_case")]
pub enum FrozenDecisionAgent {
    Agent(Box<AgentConfig>),
    Profile(DecisionAgentConfig),
}

impl FrozenDecisionAgent {
    pub fn instructions(&self) -> Option<&str> {
        match self {
            Self::Agent(agent) => agent.instructions.as_deref(),
            Self::Profile(agent) => agent.instructions.as_deref(),
        }
    }
    pub fn models(&self) -> &[ConfigId] {
        match self {
            Self::Agent(agent) => &agent.models,
            Self::Profile(agent) => &agent.models,
        }
    }
    pub fn kind(&self) -> ConfigKind {
        match self {
            Self::Agent(_) => ConfigKind::Agent,
            Self::Profile(_) => ConfigKind::DecisionAgent,
        }
    }
    pub fn supports_decision(&self) -> bool {
        match self {
            Self::Agent(agent) => agent.capabilities.contains(&AgentCapability::Decision),
            Self::Profile(_) => true,
        }
    }
}

/// Tool-free snapshot for the typed decision runtime. References name the
/// versions captured in one database transaction; credentials remain references.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeDecisionConfig {
    pub agent: Option<FrozenDecisionAgent>,
    pub agent_ref: Option<ConfigRef>,
    pub model: DecisionModelConfig,
    pub model_ref: ConfigRef,
    pub connection_ref: ConfigRef,
    pub endpoint: Option<String>,
    pub credential_ref: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum FrozenDecisionConfig {
    Native(NativeDecisionConfig),
    Generative(
        FrozenExecutionConfig,
        Option<(DecisionAgentConfig, ConfigRef)>,
    ),
}

impl FrozenDecisionConfig {
    pub fn model_ref(&self) -> ConfigRef {
        match self {
            Self::Native(config) => config.model_ref,
            Self::Generative(config, _) => config.llm.model,
        }
    }
    pub fn agent_ref(&self) -> Option<ConfigRef> {
        match self {
            Self::Native(config) => config.agent_ref,
            Self::Generative(config, profile) => profile
                .as_ref()
                .map(|(_, reference)| *reference)
                .or(config.agent_ref),
        }
    }
    pub fn profile_ref(&self) -> ConfigRef {
        self.agent_ref().unwrap_or(self.model_ref())
    }
    pub fn connection_ref(&self) -> ConfigRef {
        match self {
            Self::Native(config) => config.connection_ref,
            Self::Generative(config, _) => ConfigRef {
                id: config.llm.provider.id.into(),
                edit_seq: config.llm.provider.edit_seq,
            },
        }
    }
    pub fn model_name(&self) -> &str {
        match self {
            Self::Native(config) => &config.model.model,
            Self::Generative(config, _) => &config.llm.config.model,
        }
    }
    pub fn capabilities(&self) -> BTreeSet<DecisionCapability> {
        match self {
            Self::Native(config) => config.model.declared_capabilities.clone(),
            Self::Generative(_, _) => BTreeSet::from([
                DecisionCapability::Choice,
                DecisionCapability::BinaryProbability,
                DecisionCapability::OrdinalScore,
            ]),
        }
    }
    pub fn confidence_semantics(&self) -> &'static str {
        match self {
            Self::Native(_) => "provider.typesafe.choice.confidence.v1",
            Self::Generative(_, _) => "model.self_reported.choice.confidence.v1",
        }
    }
    pub fn references(&self) -> impl Iterator<Item = ConfigRef> {
        self.agent_ref()
            .into_iter()
            .chain([self.model_ref(), self.connection_ref()])
    }
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.references().any(|reference| reference.edit_seq == 0) {
            return Err("decision work has invalid frozen identities");
        }
        match self {
            Self::Native(config)
                if config
                    .agent
                    .as_ref()
                    .is_some_and(|agent| !agent.supports_decision())
                    || config.model.connection_id != config.connection_ref.id
                    || config.agent.is_some() != config.agent_ref.is_some()
                    || config
                        .agent
                        .as_ref()
                        .is_some_and(|agent| !agent.models().contains(&config.model_ref.id)) =>
            {
                Err("decision work has inconsistent native configuration")
            }
            Self::Generative(config, Some((profile, _)))
                if config.agent.is_some()
                    || config.agent_ref.is_some()
                    || !profile.models.contains(&config.llm.model.id) =>
            {
                Err("decision work has inconsistent generative profile")
            }
            Self::Generative(config, _)
                if !config
                    .llm
                    .config
                    .declared_capabilities
                    .contains(&ModelCapability::Chat) =>
            {
                Err("decision work requires a chat-capable generative model")
            }
            Self::Native(_) | Self::Generative(_, _) => Ok(()),
        }
    }
}

/// Shared freeze result. A caller must dispatch on execution class; a decision
/// snapshot cannot be passed to a generative runner by structural coincidence.
#[derive(Clone, Debug, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum FrozenConfiguration {
    Generative(FrozenExecutionConfig),
    Decision(FrozenDecisionConfig),
}

/// An Executor is a Pool config. It offers `LlmRequest` for each of the LLM
/// transports its adapter implements and nothing else, so one without
/// transports is never granted LLM work.
#[derive(Clone, Debug, PartialEq)]
pub struct Executor {
    pub name: String,
    pub concurrency_limit: NonZeroU32,
    /// The LLM transports this Executor's adapter implements, fixed at
    /// registration.
    pub llm_transports: BTreeSet<LlmTransport>,
}
