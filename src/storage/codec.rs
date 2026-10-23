use super::*;
use crate::persistence::{ContextPreparation, ContextUse, PreparationStatus, SourceBoundary};

// ---- explicit versioned encoding ----------------------------------------------

pub(super) const RECORD_VERSION: u8 = 1;
pub(super) const TAG_CONFIG: u8 = 1;
pub(super) const TAG_JOB: u8 = 2;
pub(super) const TAG_JOB_STATE: u8 = 3;
pub(super) const TAG_OUTPUT: u8 = 4;
pub(super) const TAG_SUBMISSION: u8 = 5;
pub(super) const TAG_ATTACHMENT: u8 = 6;
pub(super) const TAG_TOOL_CALL: u8 = 7;
pub(super) const TAG_REQUEST_EVENT: u8 = 8;
pub(super) const TAG_PROJECT: u8 = 9;
pub(super) const TAG_CONVERSATION: u8 = 10;
pub(super) const TAG_TURN_INPUT: u8 = 11;
pub(super) const TAG_NOTICE: u8 = 12;
pub(super) const TAG_RECURRENCE: u8 = 13;
// 14 is unused.
pub(super) const TAG_APPROVAL: u8 = 15;

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
    pub(super) fn u128(&mut self, v: u128) {
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
    pub(super) fn ou32(&mut self, v: Option<u32>) {
        self.opt(&v, |e, v| e.u32(*v));
    }
    pub(super) fn ou64(&mut self, v: Option<u64>) {
        self.opt(&v, |e, v| e.u64(*v));
    }
    pub(super) fn ou128(&mut self, v: Option<u128>) {
        self.opt(&v, |e, v| e.u128(*v));
    }
    pub(super) fn of64(&mut self, v: Option<f64>) {
        self.opt(&v, |e, v| e.f64(*v));
    }
    pub(super) fn ojson(&mut self, v: &Option<serde_json::Value>) {
        self.opt(v, |e, v| e.str(&v.to_string()));
    }
    pub(super) fn agent_config(&mut self, agent: &AgentConfig) {
        self.ostr(&agent.instructions);
        self.u8(agent.behavior as u8);
        self.u32(u32::try_from(agent.models.len()).unwrap_or(u32::MAX));
        for model in &agent.models {
            self.u128(model.get());
        }
        self.u32(agent.approve.len() as u32);
        for class in &agent.approve {
            self.u8(class.code());
        }
        self.ou128(agent.default_model.map(ConfigId::get));
        self.ostr(&agent.tool_grant);
        self.u32(agent.dispatch_to.len() as u32);
        for agent in &agent.dispatch_to {
            self.u128(agent.get());
        }
    }
    pub(super) fn config_ref(&mut self, config: &ConfigRef) {
        self.u128(config.id.get());
        self.u32(config.edit_seq);
    }
    pub(super) fn llm_config(&mut self, llm: &LlmConfig) {
        self.str(&llm.model);
        self.ou32(llm.context_window);
        let parameters = &llm.parameters;
        self.of64(parameters.temperature);
        self.of64(parameters.top_p);
        self.ou32(parameters.max_tokens);
        self.ostr(&parameters.reasoning_effort);
        self.ou32(parameters.max_completion_tokens);
        self.of64(parameters.presence_penalty);
        self.of64(parameters.frequency_penalty);
        self.u32(u32::try_from(llm.declared_capabilities.len()).unwrap_or(u32::MAX));
        for capability in &llm.declared_capabilities {
            self.u8(capability.code());
        }
    }
    pub(super) fn frozen_llm(&mut self, frozen: &FrozenLlmConfig) {
        self.llm_config(&frozen.config);
        self.config_ref(&frozen.model);
        let provider = &frozen.provider;
        self.u128(provider.id.get());
        self.u32(provider.edit_seq);
        self.str(&provider.name);
        self.u8(provider.transport.code());
        self.ostr(&provider.endpoint);
        self.ostr(&provider.credential_ref);
    }
    pub(super) fn frozen_execution(&mut self, frozen: &FrozenExecutionConfig) {
        self.opt(&frozen.agent, Enc::agent_config);
        self.opt(&frozen.agent_ref, Enc::config_ref);
        self.frozen_llm(&frozen.llm);
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
    pub(super) fn u128(&mut self) -> DecodeResult<u128> {
        Ok(u128::from_le_bytes(
            self.take(16)?.try_into().expect("16 bytes"),
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
    pub(super) fn ou32(&mut self) -> DecodeResult<Option<u32>> {
        self.opt(Self::u32)
    }
    pub(super) fn ou64(&mut self) -> DecodeResult<Option<u64>> {
        self.opt(Self::u64)
    }
    pub(super) fn ou128(&mut self) -> DecodeResult<Option<u128>> {
        self.opt(Dec::u128)
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
    pub(super) fn agent_config(&mut self) -> DecodeResult<AgentConfig> {
        let instructions = self.ostr()?;
        let behavior = match self.u8()? {
            1 => AgentBehavior::Chat,
            2 => AgentBehavior::General,
            code => {
                return Err(StorageError::corrupted(format!(
                    "invalid agent behavior {code}"
                )));
            }
        };
        let mut models = Vec::new();
        for _ in 0..self.u32()? {
            models.push(ConfigId::new(self.u128()?));
        }
        let mut approve = Vec::new();
        for _ in 0..self.u32()? {
            approve.push(self.code(ApproveClass::from_code)?);
        }
        let default_model = self.ou128()?.map(ConfigId::new);
        let tool_grant = self.ostr()?;
        let mut dispatch_to = Vec::new();
        for _ in 0..self.u32()? {
            dispatch_to.push(ConfigId::new(self.u128()?));
        }
        Ok(AgentConfig {
            instructions,
            behavior,
            models,
            default_model,
            tool_grant,
            dispatch_to,
            approve,
        })
    }
    pub(super) fn config_ref(&mut self) -> DecodeResult<ConfigRef> {
        Ok(ConfigRef {
            id: ConfigId::new(self.u128()?),
            edit_seq: self.u32()?,
        })
    }
    pub(super) fn llm_config(&mut self) -> DecodeResult<LlmConfig> {
        let model = self.str()?;
        let context_window = self.ou32()?;
        let parameters = LlmParameters {
            temperature: self.of64()?,
            top_p: self.of64()?,
            max_tokens: self.ou32()?,
            reasoning_effort: self.ostr()?,
            max_completion_tokens: self.ou32()?,
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
            context_window,
            parameters,
            declared_capabilities,
        })
    }
    pub(super) fn frozen_llm(&mut self) -> DecodeResult<FrozenLlmConfig> {
        let config = self.llm_config()?;
        let model = self.config_ref()?;
        let provider = ProviderSnapshot {
            id: LlmProviderId::new(self.u128()?),
            edit_seq: self.u32()?,
            name: self.str()?,
            transport: self.code(LlmTransport::from_code)?,
            endpoint: self.ostr()?,
            credential_ref: self.ostr()?,
        };
        Ok(FrozenLlmConfig {
            config,
            provider,
            model,
        })
    }
    pub(super) fn frozen_execution(&mut self) -> DecodeResult<FrozenExecutionConfig> {
        Ok(FrozenExecutionConfig {
            agent: self.opt(Self::agent_config)?,
            agent_ref: self.opt(Self::config_ref)?,
            llm: self.frozen_llm()?,
        })
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

codes!(OutputKind { OutputKind::Progress = 1, OutputKind::Evaluation = 2, OutputKind::Result = 3, OutputKind::Reasoning = 4, OutputKind::Decision = 5, OutputKind::Summarization = 6 });
codes!(DependencyCondition { DependencyCondition::Success = 1, DependencyCondition::Settled = 2 });
codes!(ModelCapability { ModelCapability::Chat = 1, ModelCapability::Tools = 2, ModelCapability::Vision = 3, ModelCapability::Reasoning = 4 });
codes!(LlmTransport { LlmTransport::OpenAiChatCompletions = 1, LlmTransport::OpenAiResponses = 2 });
codes!(ExecutionOperation { ExecutionOperation::LlmRequest = 1 });
codes!(ChatFinishReason {
    ChatFinishReason::Stop = 1, ChatFinishReason::Length = 2, ChatFinishReason::ToolCalls = 3,
    ChatFinishReason::ContentFilter = 4, ChatFinishReason::Error = 5, ChatFinishReason::Other = 6,
});
codes!(ApproveClass { ApproveClass::Commands = 1, ApproveClass::Writes = 2, ApproveClass::Dispatch = 3, ApproveClass::Close = 4 });
codes!(ToolCallRefusal {
    ToolCallRefusal::Malformed = 1, ToolCallRefusal::ConflictingIdentity = 2, ToolCallRefusal::AmbiguousIdentity = 3,
    ToolCallRefusal::LimitExceeded = 4, ToolCallRefusal::Incomplete = 5, ToolCallRefusal::Unauthorized = 6,
    ToolCallRefusal::MissingSchema = 7, ToolCallRefusal::UnsupportedSchema = 8, ToolCallRefusal::InvalidArguments = 9, ToolCallRefusal::NoDecider = 10,
});
codes!(ToolEffect { ToolEffect::ReadOnly = 1, ToolEffect::Idempotent = 2, ToolEffect::NonIdempotent = 3 });
codes!(AuthorizationBasis { AuthorizationBasis::RegistryGrant = 1 });
codes!(WithheldReason { WithheldReason::TooLarge = 1, WithheldReason::Unserializable = 2 });
codes!(UncertainCause { UncertainCause::TimedOut = 1, UncertainCause::Cancelled = 2, UncertainCause::HandlerReported = 3 });
codes!(EffectConclusion { EffectConclusion::Applied = 1, EffectConclusion::NotApplied = 2 });
codes!(AttachmentOrigin { AttachmentOrigin::UserProvided = 1, AttachmentOrigin::ModelProduced = 2 });
codes!(CleanupKind { CleanupKind::Manual = 1, CleanupKind::Automatic = 2 });

impl Enc {
    pub(super) fn call_ref(&mut self, call: &crate::persistence::ToolCallRef) {
        self.u128(call.job.get());
        self.u32(call.request.get());
        self.u32(call.index);
    }
    pub(super) fn actor(&mut self, actor: &Actor) {
        match actor {
            Actor::Auto => self.u8(0),
            Actor::Agent { conversation } => {
                self.u8(1);
                self.u128(conversation.get());
            }
            Actor::User => self.u8(2),
            Actor::Loop => self.u8(3),
        }
    }
    pub(super) fn job_kind(&mut self, kind: &JobKind) {
        match kind {
            JobKind::Conversation {
                agent,
                project,
                origin,
                keep_open,
            } => {
                self.u8(0);
                self.ou128(agent.map(ConfigId::get));
                self.ou128(project.map(ProjectId::get));
                match origin {
                    Origin::User => self.u8(0),
                    Origin::Agent { call } => {
                        self.u8(1);
                        self.call_ref(call);
                    }
                    Origin::System => self.u8(2),
                };
                self.bool(*keep_open);
            }
            JobKind::Turn {
                name,
                execution,
                plan_source,
            } => {
                self.u8(1);
                self.ostr(name);
                match execution {
                    TurnExecution::Frozen(frozen) => {
                        self.u8(0);
                        self.frozen_execution(frozen);
                    }
                    TurnExecution::Ref(owner) => {
                        self.u8(1);
                        self.u128(owner.get());
                    }
                };
                match plan_source {
                    PlanSource::Stored {
                        previous,
                        dependencies,
                        preparation,
                    } => {
                        self.u8(0);
                        self.ou128(previous.map(JobId::get));
                        self.u32(dependencies.len() as u32);
                        for (job, condition) in dependencies {
                            self.u128(job.get());
                            self.u8(condition.code());
                        }
                        self.opt(preparation, |e, p| e.context_preparation(p));
                    }
                    PlanSource::Process {
                        project,
                        grant,
                        eligible_at,
                        objective,
                        attachments,
                    } => {
                        self.u8(1);
                        self.ou128(project.map(ProjectId::get));
                        self.ostr(grant);
                        self.ots(*eligible_at);
                        self.ostr(objective);
                        self.u32(attachments.len() as u32);
                        for attachment in attachments {
                            self.u128(attachment.get());
                        }
                    }
                }
            }
        }
    }
    pub(super) fn notice_event(&mut self, event: &NoticeEvent) {
        match event {
            NoticeEvent::ClosedSelf => self.u8(0),
            NoticeEvent::ClosedAbove => self.u8(1),
            NoticeEvent::StoppedAbove => self.u8(2),
            NoticeEvent::TurnEnded { kind } => {
                self.u8(3);
                self.u8(*kind as u8);
            }
            NoticeEvent::Requested => self.u8(4),
            NoticeEvent::Stalled => self.u8(5),
            NoticeEvent::ObjectiveCompleted => self.u8(6),
            NoticeEvent::WaitersBlocked { waiting } => {
                self.u8(7);
                self.u32(waiting.len() as u32);
                for conversation in waiting {
                    self.u128(conversation.get());
                }
            }
            NoticeEvent::IterationCompleted => self.u8(9),
            NoticeEvent::RecurrenceHalted => self.u8(10),
        }
    }
    pub(super) fn context_preparation(&mut self, prep: &ContextPreparation) {
        self.u8(prep.status.code());
        self.u32(prep.source_boundaries.len() as u32);
        for boundary in &prep.source_boundaries {
            self.u128(boundary.turn.get());
            self.u32(boundary.offset);
            self.u32(boundary.length);
        }
        self.opt(&prep.context_use, |e, use_| {
            e.u32(use_.input_tokens);
            e.u32(use_.retained_turns);
            e.u32(use_.summarized_turns);
        });
    }
    pub(super) fn response_format(&mut self, format: &ResponseFormat) {
        match format {
            ResponseFormat::JsonObject => self.u8(0),
            ResponseFormat::JsonSchema {
                name,
                schema,
                strict,
            } => {
                self.u8(1);
                self.str(name);
                self.str(&schema.to_string());
                self.opt(strict, |e, v| e.bool(*v));
            }
        }
    }
    pub(super) fn entry(&mut self, entry: &Entry) {
        match entry {
            Entry::UserMessage { text, attachments } => {
                self.u8(0);
                self.str(text);
                self.u32(attachments.len() as u32);
                for attachment in attachments {
                    self.u128(attachment.get());
                }
            }
            Entry::AgentMessage { text, call } => {
                self.u8(1);
                self.str(text);
                self.call_ref(call);
            }
            Entry::Notice {
                child,
                event,
                reference,
            } => {
                self.u8(2);
                self.u128(child.get());
                self.notice_event(event);
                self.u128(reference.get());
            }
            Entry::Decision {
                approval: (turn, request, index),
            } => {
                self.u8(3);
                self.u128(turn.get());
                self.u32(*request);
                self.u32(*index);
            }
            Entry::RetryOf { turn, by } => {
                self.u8(4);
                self.u128(turn.get());
                self.actor(by);
            }
            Entry::Iteration { n } => {
                self.u8(5);
                self.u64(*n);
            }
            Entry::ResponseFormat { format } => {
                self.u8(6);
                self.response_format(format);
            }
        }
    }
}
pub(crate) fn encode_response_format(format: &ResponseFormat) -> Vec<u8> {
    let mut e = Enc(Vec::new());
    e.response_format(format);
    e.0
}
impl Dec<'_> {
    fn context_preparation(&mut self) -> DecodeResult<ContextPreparation> {
        let status = self.code(PreparationStatus::from_code)?;
        let mut source_boundaries = Vec::new();
        for _ in 0..self.u32()? {
            source_boundaries.push(SourceBoundary {
                turn: JobId::new(self.u128()?),
                offset: self.u32()?,
                length: self.u32()?,
            });
        }
        let context_use = self.opt(|d| {
            Ok(ContextUse {
                input_tokens: d.u32()?,
                retained_turns: d.u32()?,
                summarized_turns: d.u32()?,
            })
        })?;
        Ok(ContextPreparation {
            status,
            source_boundaries,
            context_use,
        })
    }
    fn response_format(&mut self) -> DecodeResult<ResponseFormat> {
        Ok(match self.u8()? {
            0 => ResponseFormat::JsonObject,
            1 => ResponseFormat::JsonSchema {
                name: self.str()?,
                schema: serde_json::from_str(&self.str()?)
                    .map_err(|_| StorageError::corrupted("invalid response schema"))?,
                strict: self.opt(Self::bool)?,
            },
            _ => return Err(StorageError::corrupted("invalid response format")),
        })
    }
    pub(super) fn call_ref(&mut self) -> DecodeResult<crate::persistence::ToolCallRef> {
        Ok(crate::persistence::ToolCallRef {
            job: JobId::new(self.u128()?),
            request: RequestNumber::new(self.u32()?)
                .ok_or_else(|| StorageError::corrupted("invalid request"))?,
            index: self.u32()?,
        })
    }
    pub(super) fn actor(&mut self) -> DecodeResult<Actor> {
        Ok(match self.u8()? {
            0 => Actor::Auto,
            1 => Actor::Agent {
                conversation: JobId::new(self.u128()?),
            },
            2 => Actor::User,
            3 => Actor::Loop,
            _ => return Err(StorageError::corrupted("invalid actor")),
        })
    }
    pub(super) fn job_kind(&mut self) -> DecodeResult<JobKind> {
        Ok(match self.u8()? {
            0 => JobKind::Conversation {
                agent: self.ou128()?.map(ConfigId::new),
                project: self.ou128()?.map(ProjectId::new),
                origin: match self.u8()? {
                    0 => Origin::User,
                    1 => Origin::Agent {
                        call: self.call_ref()?,
                    },
                    2 => Origin::System,
                    _ => return Err(StorageError::corrupted("invalid origin")),
                },
                keep_open: self.bool()?,
            },
            1 => JobKind::Turn {
                name: self.ostr()?,
                execution: match self.u8()? {
                    0 => TurnExecution::Frozen(self.frozen_execution()?),
                    1 => TurnExecution::Ref(JobId::new(self.u128()?)),
                    _ => return Err(StorageError::corrupted("invalid execution")),
                },
                plan_source: match self.u8()? {
                    0 => {
                        let previous = self.ou128()?.map(JobId::new);
                        let mut dependencies = Vec::new();
                        for _ in 0..self.u32()? {
                            dependencies.push((
                                JobId::new(self.u128()?),
                                self.code(DependencyCondition::from_code)?,
                            ));
                        }
                        PlanSource::Stored {
                            previous,
                            dependencies,
                            preparation: self.opt(Dec::context_preparation)?,
                        }
                    }
                    1 => PlanSource::Process {
                        project: self.ou128()?.map(ProjectId::new),
                        grant: self.ostr()?,
                        eligible_at: self.ots()?,
                        objective: self.ostr()?,
                        attachments: {
                            let mut attachments = Vec::new();
                            for _ in 0..self.u32()? {
                                attachments.push(AttachmentId::new(self.u128()?));
                            }
                            attachments
                        },
                    },
                    _ => return Err(StorageError::corrupted("invalid plan source")),
                },
            },
            _ => return Err(StorageError::corrupted("invalid job kind")),
        })
    }
    pub(super) fn notice_event(&mut self) -> DecodeResult<NoticeEvent> {
        Ok(match self.u8()? {
            0 => NoticeEvent::ClosedSelf,
            1 => NoticeEvent::ClosedAbove,
            2 => NoticeEvent::StoppedAbove,
            3 => NoticeEvent::TurnEnded {
                kind: match self.u8()? {
                    1 => OutcomeKind::Completed,
                    2 => OutcomeKind::Failed,
                    3 => OutcomeKind::Cancelled,
                    4 => OutcomeKind::Unknown,
                    _ => return Err(StorageError::corrupted("invalid outcome")),
                },
            },
            4 => NoticeEvent::Requested,
            5 => NoticeEvent::Stalled,
            6 => NoticeEvent::ObjectiveCompleted,
            7 => {
                let mut waiting = Vec::new();
                for _ in 0..self.u32()? {
                    waiting.push(JobId::new(self.u128()?));
                }
                NoticeEvent::WaitersBlocked { waiting }
            }
            9 => NoticeEvent::IterationCompleted,
            10 => NoticeEvent::RecurrenceHalted,
            _ => return Err(StorageError::corrupted("invalid notice event")),
        })
    }
    pub(super) fn entry(&mut self) -> DecodeResult<Entry> {
        Ok(match self.u8()? {
            0 => {
                let text = self.str()?;
                let n = self.u32()?;
                let mut attachments = Vec::new();
                for _ in 0..n {
                    attachments.push(AttachmentId::new(self.u128()?));
                }
                Entry::UserMessage { text, attachments }
            }
            1 => Entry::AgentMessage {
                text: self.str()?,
                call: self.call_ref()?,
            },
            2 => Entry::Notice {
                child: JobId::new(self.u128()?),
                event: self.notice_event()?,
                reference: JobId::new(self.u128()?),
            },
            3 => Entry::Decision {
                approval: (JobId::new(self.u128()?), self.u32()?, self.u32()?),
            },
            4 => Entry::RetryOf {
                turn: JobId::new(self.u128()?),
                by: self.actor()?,
            },
            5 => Entry::Iteration { n: self.u64()? },
            6 => Entry::ResponseFormat {
                format: self.response_format()?,
            },
            _ => return Err(StorageError::corrupted("invalid entry")),
        })
    }
}

#[cfg(test)]
mod responses_protocol_tests {
    use super::*;
    #[test]
    fn responses_protocol_preserves_the_existing_chat_code() {
        assert_eq!(LlmTransport::OpenAiChatCompletions.code(), 1);
        assert_eq!(
            LlmTransport::from_code(1),
            Some(LlmTransport::OpenAiChatCompletions)
        );
        assert_eq!(LlmTransport::OpenAiResponses.code(), 2);
        assert_eq!(
            LlmTransport::from_code(2),
            Some(LlmTransport::OpenAiResponses)
        );
        assert_eq!(FORMAT_VERSION, 27);
    }
}
