use super::*;

// ---- explicit versioned encoding ----------------------------------------------

pub(super) const RECORD_VERSION: u8 = 1;
pub(super) const TAG_JOB: u8 = 1;
pub(super) const TAG_INPUT: u8 = 2;
pub(super) const TAG_OUTPUT: u8 = 3;
// Tag 4 was the retired JobStep record.
pub(super) const TAG_EXECUTOR: u8 = 5;
pub(super) const TAG_OPERATION: u8 = 6;
pub(super) const TAG_PROJECT: u8 = 7;
pub(super) const TAG_PROFILE: u8 = 8;
pub(super) const TAG_AGENT: u8 = 9;
pub(super) const TAG_LLM_PROVIDER: u8 = 10;
// Tags 11 and 12 were the retired HealthCheck and SERVES records.
pub(super) const TAG_METRICS: u8 = 13;
pub(super) const TAG_AVAILABILITY_STAT: u8 = 14;
pub(super) const TAG_REQUEST_SAMPLE: u8 = 15;
pub(super) const TAG_MINUTE_COUNTS: u8 = 16;
pub(super) const TAG_AGGREGATION_PROGRESS: u8 = 17;
pub(super) const TAG_TOKEN_REQUEST: u8 = 18;
pub(super) const TAG_TOKEN_MINUTE: u8 = 19;
pub(super) const TAG_TOOL_EVIDENCE: u8 = 20;
pub(super) const TAG_ATTACHMENT: u8 = 21;
pub(super) const TAG_SUBMISSION: u8 = 22;
pub(super) const TAG_EXECUTION_POOL: u8 = 23;
pub(super) const TAG_POOL_BINDING: u8 = 24;

pub(super) struct Enc(pub(super) Vec<u8>);

impl Enc {
    pub(super) fn new(tag: u8) -> Self {
        Enc(vec![tag, RECORD_VERSION])
    }
    pub(super) fn u8(&mut self, v: u8) {
        self.0.push(v);
    }
    pub(super) fn u16(&mut self, v: u16) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    pub(super) fn u32(&mut self, v: u32) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    pub(super) fn u64(&mut self, v: u64) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    pub(super) fn i64(&mut self, v: i64) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    pub(super) fn f64(&mut self, v: f64) {
        self.u64(v.to_bits());
    }
    pub(super) fn bool(&mut self, v: bool) {
        self.u8(u8::from(v));
    }
    pub(super) fn ts(&mut self, v: Timestamp) {
        self.i64(v.as_unix_micros());
    }
    pub(super) fn str(&mut self, v: &str) {
        self.u32(u32::try_from(v.len()).expect("strings above 4 GiB are refused by redb first"));
        self.0.extend_from_slice(v.as_bytes());
    }
    pub(super) fn opt<T>(&mut self, v: &Option<T>, mut put: impl FnMut(&mut Self, &T)) {
        match v {
            None => self.u8(0),
            Some(v) => {
                self.u8(1);
                put(self, v);
            }
        }
    }
    pub(super) fn ots(&mut self, v: Option<Timestamp>) {
        self.opt(&v, |e, v| e.ts(*v));
    }
    pub(super) fn ostr(&mut self, v: &Option<String>) {
        self.opt(v, |e, v| e.str(v));
    }
    pub(super) fn oi64(&mut self, v: Option<i64>) {
        self.opt(&v, |e, v| e.i64(*v));
    }
    pub(super) fn ou32(&mut self, v: Option<u32>) {
        self.opt(&v, |e, v| e.u32(*v));
    }
    pub(super) fn ou64(&mut self, v: Option<u64>) {
        self.opt(&v, |e, v| e.u64(*v));
    }
    pub(super) fn of64(&mut self, v: Option<f64>) {
        self.opt(&v, |e, v| e.f64(*v));
    }
    pub(super) fn ojson(&mut self, v: &Option<serde_json::Value>) {
        self.opt(v, |e, v| e.str(&v.to_string()));
    }
    pub(super) fn record_ref(&mut self, r: RecordRef) {
        let (tag, id) = match r {
            RecordRef::Job(id) => (1, id.get()),
            RecordRef::JobInput(id) => (2, id.get()),
            RecordRef::JobOutput(id) => (3, id.get()),
            // Tag 4 was the retired JobStep reference.
            RecordRef::Executor(id) => (5, id.get()),
            RecordRef::Project(id) => (6, id.get()),
            RecordRef::Profile(id) => (7, id.get()),
            RecordRef::Agent(id) => (8, id.get()),
            RecordRef::LlmProvider(id) => (9, id.get()),
            // Tag 10 was the retired HealthCheck reference.
            RecordRef::Metrics(id) => (11, id.get()),
            RecordRef::AvailabilityStat(id) => (12, id.get()),
            RecordRef::Attachment(id) => (13, id.get()),
            RecordRef::ExecutionPool(id) => (14, id.get()),
        };
        self.u8(tag);
        self.u64(id);
    }
    pub(super) fn profile_config(&mut self, config: &ProfileConfig) {
        match config {
            ProfileConfig::ExecutionPool(pool) => {
                self.u8(3);
                self.pool_config(pool);
            }
            ProfileConfig::Agent(agent) => {
                self.u8(1);
                self.agent_config(agent);
            }
            ProfileConfig::Llm(llm) => {
                self.u8(2);
                self.llm_config(llm);
            }
        }
    }
    pub(super) fn agent_config(&mut self, agent: &AgentConfig) {
        self.ostr(&agent.instructions);
    }
    pub(super) fn llm_config(&mut self, llm: &LlmConfig) {
        self.str(&llm.model);
        let parameters = &llm.parameters;
        self.of64(parameters.temperature);
        self.of64(parameters.top_p);
        self.ou32(parameters.max_tokens);
        self.of64(parameters.presence_penalty);
        self.of64(parameters.frequency_penalty);
        self.u32(u32::try_from(llm.declared_capabilities.len()).unwrap_or(u32::MAX));
        for capability in &llm.declared_capabilities {
            self.u8(capability.code());
        }
    }
    pub(super) fn frozen_llm(&mut self, frozen: &FrozenLlmConfig) {
        self.llm_config(&frozen.config);
        let provider = &frozen.provider;
        self.u64(provider.id.get());
        self.str(&provider.name);
        self.u8(provider.transport.code());
        self.ostr(&provider.endpoint);
        self.ostr(&provider.credential_ref);
    }
    pub(super) fn frozen_execution(&mut self, frozen: &FrozenExecutionConfig) {
        self.opt(&frozen.agent, Enc::agent_config);
        self.frozen_llm(&frozen.llm);
    }
    pub(super) fn transports(&mut self, transports: &BTreeSet<LlmTransport>) {
        self.u32(u32::try_from(transports.len()).unwrap_or(u32::MAX));
        for transport in transports {
            self.u8(transport.code());
        }
    }
    pub(super) fn provider_failure(&mut self, failure: &ProviderFailure) {
        self.str(&failure.message);
        self.opt(&failure.status_code, |e, v| e.u16(*v));
        self.ostr(&failure.provider_code);
        self.ostr(&failure.response_body);
        self.ostr(&failure.request_id);
        self.ou64(failure.retry_after_ms);
        self.bool(failure.retryable);
    }
    pub(super) fn usage(&mut self, usage: &NormalizedUsage) {
        for count in [
            usage.input_tokens,
            usage.output_tokens,
            usage.cache_read_tokens,
            usage.cache_write_tokens,
            usage.reasoning_tokens,
        ] {
            self.ou32(count);
        }
        self.ojson(&usage.raw);
    }
}

pub(super) struct Dec<'a> {
    bytes: &'a [u8],
    at: usize,
}

pub(super) type DecodeResult<T> = Result<T, StorageError>;

impl<'a> Dec<'a> {
    /// Refuses a record of another type or of an unsupported encoding version.
    pub(super) fn new(bytes: &'a [u8], tag: u8) -> DecodeResult<Self> {
        match bytes {
            [found, RECORD_VERSION, ..] if *found == tag => Ok(Dec { bytes, at: 2 }),
            [found, version, ..] if *found == tag => Err(StorageError::corrupted(format!(
                "record type {tag} uses unsupported encoding version {version}"
            ))),
            _ => Err(StorageError::corrupted(format!(
                "expected a record of type {tag}"
            ))),
        }
    }
    pub(super) fn take(&mut self, n: usize) -> DecodeResult<&'a [u8]> {
        let slice = self
            .bytes
            .get(self.at..self.at + n)
            .ok_or_else(|| StorageError::corrupted("truncated record"))?;
        self.at += n;
        Ok(slice)
    }
    pub(super) fn u8(&mut self) -> DecodeResult<u8> {
        Ok(self.take(1)?[0])
    }
    pub(super) fn u16(&mut self) -> DecodeResult<u16> {
        Ok(u16::from_le_bytes(
            self.take(2)?.try_into().expect("2 bytes"),
        ))
    }
    pub(super) fn u32(&mut self) -> DecodeResult<u32> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().expect("4 bytes"),
        ))
    }
    pub(super) fn u64(&mut self) -> DecodeResult<u64> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().expect("8 bytes"),
        ))
    }
    pub(super) fn i64(&mut self) -> DecodeResult<i64> {
        Ok(i64::from_le_bytes(
            self.take(8)?.try_into().expect("8 bytes"),
        ))
    }
    pub(super) fn f64(&mut self) -> DecodeResult<f64> {
        Ok(f64::from_bits(self.u64()?))
    }
    pub(super) fn bool(&mut self) -> DecodeResult<bool> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(StorageError::corrupted("invalid boolean")),
        }
    }
    /// A stored request number, which is never 0.
    pub(super) fn request(&mut self) -> DecodeResult<RequestNumber> {
        let number = self.u32()?;
        RequestNumber::new(number)
            .ok_or_else(|| StorageError::corrupted("a stored request number is 0"))
    }

    pub(super) fn ts(&mut self) -> DecodeResult<Timestamp> {
        Ok(Timestamp::from_unix_micros(self.i64()?))
    }
    pub(super) fn str(&mut self) -> DecodeResult<String> {
        let len = self.u32()? as usize;
        String::from_utf8(self.take(len)?.to_vec())
            .map_err(|_| StorageError::corrupted("invalid UTF-8"))
    }
    pub(super) fn opt<T>(
        &mut self,
        get: impl FnOnce(&mut Self) -> DecodeResult<T>,
    ) -> DecodeResult<Option<T>> {
        match self.u8()? {
            0 => Ok(None),
            1 => get(self).map(Some),
            _ => Err(StorageError::corrupted("invalid option flag")),
        }
    }
    pub(super) fn ots(&mut self) -> DecodeResult<Option<Timestamp>> {
        self.opt(Self::ts)
    }
    pub(super) fn ostr(&mut self) -> DecodeResult<Option<String>> {
        self.opt(Self::str)
    }
    pub(super) fn oi64(&mut self) -> DecodeResult<Option<i64>> {
        self.opt(Self::i64)
    }
    pub(super) fn ou32(&mut self) -> DecodeResult<Option<u32>> {
        self.opt(Self::u32)
    }
    pub(super) fn ou64(&mut self) -> DecodeResult<Option<u64>> {
        self.opt(Self::u64)
    }
    pub(super) fn of64(&mut self) -> DecodeResult<Option<f64>> {
        self.opt(Self::f64)
    }
    pub(super) fn ojson(&mut self) -> DecodeResult<Option<serde_json::Value>> {
        self.opt(|d| {
            serde_json::from_str(&d.str()?)
                .map_err(|_| StorageError::corrupted("invalid JSON value"))
        })
    }
    pub(super) fn code<T>(&mut self, decode: impl FnOnce(u8) -> Option<T>) -> DecodeResult<T> {
        let code = self.u8()?;
        decode(code).ok_or_else(|| StorageError::corrupted(format!("invalid enum code {code}")))
    }
    pub(super) fn record_ref(&mut self) -> DecodeResult<RecordRef> {
        let tag = self.u8()?;
        let id = self.u64()?;
        Ok(match tag {
            1 => RecordRef::Job(JobId::new(id)),
            2 => RecordRef::JobInput(JobInputId::new(id)),
            3 => RecordRef::JobOutput(JobOutputId::new(id)),
            5 => RecordRef::Executor(ExecutorId::new(id)),
            6 => RecordRef::Project(ProjectId::new(id)),
            7 => RecordRef::Profile(ProfileId::new(id)),
            8 => RecordRef::Agent(AgentId::new(id)),
            9 => RecordRef::LlmProvider(LlmProviderId::new(id)),
            11 => RecordRef::Metrics(MetricsId::new(id)),
            12 => RecordRef::AvailabilityStat(AvailabilityStatId::new(id)),
            13 => RecordRef::Attachment(AttachmentId::new(id)),
            14 => RecordRef::ExecutionPool(ExecutionPoolId::new(id)),
            _ => return Err(StorageError::corrupted("invalid record reference")),
        })
    }
    pub(super) fn profile_config(&mut self) -> DecodeResult<ProfileConfig> {
        Ok(match self.u8()? {
            1 => ProfileConfig::Agent(self.agent_config()?),
            2 => ProfileConfig::Llm(self.llm_config()?),
            3 => ProfileConfig::ExecutionPool(self.pool_config()?),
            _ => {
                return Err(StorageError::corrupted(
                    "invalid Profile configuration kind",
                ));
            }
        })
    }
    pub(super) fn agent_config(&mut self) -> DecodeResult<AgentConfig> {
        Ok(AgentConfig {
            instructions: self.ostr()?,
        })
    }
    pub(super) fn llm_config(&mut self) -> DecodeResult<LlmConfig> {
        let model = self.str()?;
        let parameters = LlmParameters {
            temperature: self.of64()?,
            top_p: self.of64()?,
            max_tokens: self.ou32()?,
            presence_penalty: self.of64()?,
            frequency_penalty: self.of64()?,
        };
        let mut declared_capabilities = BTreeSet::new();
        for _ in 0..self.u32()? {
            if !declared_capabilities.insert(self.code(ModelCapability::from_code)?) {
                return Err(StorageError::corrupted("repeated declared capability"));
            }
        }
        Ok(LlmConfig {
            model,
            parameters,
            declared_capabilities,
        })
    }
    pub(super) fn frozen_llm(&mut self) -> DecodeResult<FrozenLlmConfig> {
        let config = self.llm_config()?;
        let provider = ProviderSnapshot {
            id: LlmProviderId::new(self.u64()?),
            name: self.str()?,
            transport: self.code(LlmTransport::from_code)?,
            endpoint: self.ostr()?,
            credential_ref: self.ostr()?,
        };
        Ok(FrozenLlmConfig { config, provider })
    }
    pub(super) fn frozen_execution(&mut self) -> DecodeResult<FrozenExecutionConfig> {
        Ok(FrozenExecutionConfig {
            agent: self.opt(Self::agent_config)?,
            llm: self.frozen_llm()?,
        })
    }
    pub(super) fn transports(&mut self) -> DecodeResult<BTreeSet<LlmTransport>> {
        let mut transports = BTreeSet::new();
        for _ in 0..self.u32()? {
            if !transports.insert(self.code(LlmTransport::from_code)?) {
                return Err(StorageError::corrupted("repeated LLM transport"));
            }
        }
        Ok(transports)
    }
    pub(super) fn provider_failure(&mut self) -> DecodeResult<ProviderFailure> {
        Ok(ProviderFailure {
            message: self.str()?,
            status_code: self.opt(Self::u16)?,
            provider_code: self.ostr()?,
            response_body: self.ostr()?,
            request_id: self.ostr()?,
            retry_after_ms: self.ou64()?,
            retryable: self.bool()?,
        })
    }
    pub(super) fn usage(&mut self) -> DecodeResult<NormalizedUsage> {
        Ok(NormalizedUsage {
            input_tokens: self.ou32()?,
            output_tokens: self.ou32()?,
            cache_read_tokens: self.ou32()?,
            cache_write_tokens: self.ou32()?,
            reasoning_tokens: self.ou32()?,
            raw: self.ojson()?,
        })
    }
    pub(super) fn end(self) -> DecodeResult<()> {
        if self.at == self.bytes.len() {
            Ok(())
        } else {
            Err(StorageError::corrupted("trailing record bytes"))
        }
    }
}

macro_rules! codes {
    ($t:ty { $($variant:path = $code:literal),* $(,)? }) => {
        impl Code for $t {
            fn code(self) -> u8 { match self { $($variant => $code),* } }
            fn from_code(code: u8) -> Option<Self> { match code { $($code => Some($variant),)* _ => None } }
        }
    };
}

pub(super) trait Code: Sized + Copy {
    fn code(self) -> u8;
    fn from_code(code: u8) -> Option<Self>;
}

codes!(ExecutionMode { ExecutionMode::Once = 1, ExecutionMode::Continuous = 2, ExecutionMode::Periodic = 3, ExecutionMode::Container = 4 });
codes!(OutputKind { OutputKind::Progress = 1, OutputKind::Evaluation = 2, OutputKind::Result = 3 });
codes!(ExecutorType { ExecutorType::System = 1, ExecutorType::Llm = 2, ExecutorType::Agent = 3, ExecutorType::Tool = 4, ExecutorType::Database = 5, ExecutorType::Frontend = 6 });
codes!(DependencyCondition { DependencyCondition::Success = 1, DependencyCondition::Settled = 2 });
codes!(ModelCapability { ModelCapability::Chat = 1, ModelCapability::Tools = 2, ModelCapability::Vision = 3, ModelCapability::Reasoning = 4 });
codes!(LlmTransport { LlmTransport::OpenAiChatCompletions = 1 });
codes!(ChatFinishReason {
    ChatFinishReason::Stop = 1, ChatFinishReason::Length = 2, ChatFinishReason::ToolCalls = 3,
    ChatFinishReason::ContentFilter = 4, ChatFinishReason::Error = 5, ChatFinishReason::Other = 6,
});
// Codes 4, 5 and 6 were the retired JobStep commands; 18 recorded HealthChecks.
codes!(OperationKind {
    OperationKind::SaveJobInput = 1, OperationKind::CreateJobWithInput = 2, OperationKind::AssignJobExecutor = 3,
    OperationKind::AppendJobOutput = 7, OperationKind::FinalizeJob = 8, OperationKind::CreateExecutor = 9,
    OperationKind::BeginCoreLogCleanup = 10, OperationKind::StartJob = 11, OperationKind::RetryJob = 12,
    OperationKind::AssociateJobProject = 13, OperationKind::CreateProject = 14, OperationKind::CreateLlmProvider = 15,
    OperationKind::CreateProfile = 16, OperationKind::CreateAgent = 17, OperationKind::UpdateProfile = 19,
    OperationKind::ConfirmJobStopped = 20, OperationKind::RecordMetrics = 21, OperationKind::RecordAvailabilityStat = 22,
    OperationKind::ConsumeAvailability = 23, OperationKind::ConsumeTokenUsage = 24,
    OperationKind::StartLocalJob = 25, OperationKind::ReconcileAbandonedJob = 26,
    OperationKind::RecordToolDecision = 27, OperationKind::RecordToolOutcome = 28,
    OperationKind::ReconcileToolEffect = 29, OperationKind::CreateAttachment = 30,
    OperationKind::CleanupAttachment = 31,
    OperationKind::CreateExecutionPool = 32, OperationKind::SetExecutionPoolEnabled = 33,
    OperationKind::SetExecutionPoolProfile = 34,
    OperationKind::ClaimExecution = 35,
});
codes!(ToolCallRefusal {
    ToolCallRefusal::Malformed = 1, ToolCallRefusal::ConflictingIdentity = 2, ToolCallRefusal::AmbiguousIdentity = 3,
    ToolCallRefusal::LimitExceeded = 4, ToolCallRefusal::Incomplete = 5, ToolCallRefusal::Unauthorized = 6,
    ToolCallRefusal::MissingSchema = 7, ToolCallRefusal::UnsupportedSchema = 8, ToolCallRefusal::InvalidArguments = 9,
});
codes!(ToolEffect { ToolEffect::ReadOnly = 1, ToolEffect::Idempotent = 2, ToolEffect::NonIdempotent = 3 });
codes!(AuthorizationBasis { AuthorizationBasis::RegistryGrant = 1 });
codes!(WithheldReason { WithheldReason::TooLarge = 1, WithheldReason::Unserializable = 2 });
codes!(UncertainCause { UncertainCause::TimedOut = 1, UncertainCause::Cancelled = 2, UncertainCause::HandlerReported = 3 });
codes!(EffectConclusion { EffectConclusion::Applied = 1, EffectConclusion::NotApplied = 2 });
codes!(AttachmentOrigin { AttachmentOrigin::UserProvided = 1, AttachmentOrigin::ModelProduced = 2 });
codes!(CleanupKind { CleanupKind::Manual = 1, CleanupKind::Automatic = 2 });

impl Enc {
    pub(super) fn pool_config(&mut self, config: &ExecutionPoolConfig) {
        self.u32(config.concurrency_limit.get());
        self.u32(config.capabilities.operations.len() as u32);
        for operation in &config.capabilities.operations {
            self.u8(match operation {
                ExecutionOperation::LlmRequest => 1,
            });
        }
        self.transports(&config.capabilities.llm_protocols);
    }
}

impl Dec<'_> {
    pub(super) fn pool_config(&mut self) -> DecodeResult<ExecutionPoolConfig> {
        let concurrency_limit = std::num::NonZeroU32::new(self.u32()?)
            .ok_or_else(|| StorageError::corrupted("zero Pool concurrency"))?;
        let count = self.u32()?;
        if count > 1 {
            return Err(StorageError::corrupted("invalid Pool operation set"));
        }
        let mut operations = BTreeSet::new();
        for _ in 0..count {
            let operation = match self.u8()? {
                1 => ExecutionOperation::LlmRequest,
                _ => return Err(StorageError::corrupted("unknown Pool operation")),
            };
            if !operations.insert(operation) {
                return Err(StorageError::corrupted("duplicate Pool operation"));
            }
        }
        Ok(ExecutionPoolConfig {
            concurrency_limit,
            capabilities: CapabilitySet {
                operations,
                llm_protocols: self.transports()?,
            },
        })
    }
}
