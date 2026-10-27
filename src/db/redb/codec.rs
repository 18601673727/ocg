use super::*;
use crate::db::entities::{
    ChoiceOption, CommandAssessment, CompactionMode, CompactionPolicy, CompactionPreset,
    CustomPolicy, DecisionAgentConfig, DecisionAssignment, DecisionCapability, DecisionLimits,
    DecisionModelConfig, DecisionProtocol, DecisionQuestion, DecisionRequest, DecisionWork,
    FrozenDecisionAgent, FrozenDecisionConfig, NativeDecisionConfig, PredictionAcceptance,
    PredictionMode, PredictiveCompaction, ScoreLevel, SummarizationStage,
};

// ---- explicit versioned encoding ----------------------------------------------

pub(super) const RECORD_VERSION: u8 = 11;
pub(super) const TAG_CONFIG: u8 = 1;
pub(super) const TAG_JOB: u8 = 2;
pub(super) const TAG_JOB_STATE: u8 = 3;
pub(super) const TAG_OUTPUT: u8 = 4;
pub(super) const TAG_SUBMISSION: u8 = 5;
pub(super) const TAG_ATTACHMENT: u8 = 6;
pub(super) const TAG_TOOL_CALL: u8 = 7;
pub(super) const TAG_REQUEST_EVENT: u8 = 8;
pub(super) const TAG_PROJECT: u8 = 9;
pub(super) const TAG_JOB_CONTROL: u8 = 10;
pub(super) const TAG_JOB_INPUT: u8 = 11;
pub(super) const TAG_NOTICE: u8 = 12;
pub(super) const TAG_RECURRENCE: u8 = 13;
pub(super) const TAG_GLOBAL_COMPACTION: u8 = 14;
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
        self.u32(agent.capabilities.len() as u32);
        for capability in &agent.capabilities {
            self.u8(capability.code());
        }
        self.u32(u32::try_from(agent.models.len()).unwrap_or(u32::MAX));
        for model in &agent.models {
            self.u128(model.get());
        }

        self.opt(&agent.compaction_policy, Enc::compaction_policy);
    }
    pub(super) fn compaction_policy(&mut self, policy: &CompactionPolicy) {
        match policy.mode {
            CompactionMode::Off => self.u8(0),
            CompactionMode::Auto { preset } => {
                self.u8(1);
                self.u8(match preset {
                    CompactionPreset::Conservative => 0,
                    CompactionPreset::Balanced => 1,
                    CompactionPreset::Aggressive => 2,
                });
            }
            CompactionMode::Custom { policy } => {
                self.u8(2);
                for value in [
                    policy.trigger_tokens,
                    policy.target_tokens,
                    policy.reserved_output_tokens,
                    policy.minimum_source_tokens,
                    policy.minimum_savings_tokens,
                    policy.preserve_recent_turns,
                    policy.cooldown_seconds,
                ] {
                    self.u32(value);
                }
            }
        }
        self.ou128(policy.summarization_model.map(ConfigId::get));
        self.opt(&policy.predictive, |e, p| {
            e.u128(p.decision_agent.get());
            e.u32(p.minimum_interval_seconds);
            e.u32(p.max_preparations);
            e.u32(p.max_prediction_tokens);
            e.u32(p.max_preparation_tokens);
            e.u8(match p.mode {
                PredictionMode::Shadow => 0,
                PredictionMode::Prepare => 1,
            });
            e.opt(&p.acceptance, |e, acceptance| {
                e.config_ref(&acceptance.model);
                e.str(&acceptance.reported_model);
                e.str(&acceptance.confidence_semantics);
                e.u32(acceptance.minimum_probability_millionths);
                e.u32(acceptance.minimum_confidence_millionths);
            });
            e.u32(p.max_preparations_per_hour);
            e.u32(p.max_tokens_per_hour);
            e.u32(p.max_requests_per_hour);
        });
    }
    pub(super) fn decision_model(&mut self, model: &DecisionModelConfig) {
        self.u128(model.connection_id.get());
        self.str(&model.model);
        self.u8(model.protocol.code());
        for limit in [
            model.limits.max_input_tokens_per_request,
            model.limits.max_output_tokens_per_request,
            model.limits.max_input_tokens_per_question,
            model.limits.max_output_tokens_per_question,
        ] {
            self.ou32(limit);
        }
        self.u32(model.declared_capabilities.len() as u32);
        for cap in &model.declared_capabilities {
            self.u8(cap.code());
        }
    }
    pub(super) fn decision_agent(&mut self, agent: &DecisionAgentConfig) {
        self.ostr(&agent.instructions);
        self.u32(agent.models.len() as u32);
        for model in &agent.models {
            self.u128(model.get());
        }
        self.ou128(agent.default_model.map(ConfigId::get));
    }
    fn decision_work(&mut self, work: &DecisionWork) {
        match &work.frozen {
            FrozenDecisionConfig::Native(frozen) => {
                self.u8(0);
                self.opt(&frozen.agent, |encoder, agent| match agent {
                    FrozenDecisionAgent::Agent(agent) => {
                        encoder.u8(0);
                        encoder.agent_config(agent);
                    }
                    FrozenDecisionAgent::Profile(agent) => {
                        encoder.u8(1);
                        encoder.decision_agent(agent);
                    }
                });
                self.opt(&frozen.agent_ref, Self::config_ref);
                self.decision_model(&frozen.model);
                self.config_ref(&frozen.model_ref);
                self.config_ref(&frozen.connection_ref);
                self.ostr(&frozen.endpoint);
                self.ostr(&frozen.credential_ref);
            }
            FrozenDecisionConfig::Generative(frozen, profile) => {
                self.u8(1);
                self.frozen_execution(frozen);
                self.opt(profile, |encoder, (agent, reference)| {
                    encoder.decision_agent(agent);
                    encoder.config_ref(reference);
                });
            }
        }
        self.opt(&work.assignment, |encoder, assignment| {
            encoder.u128(assignment.project.get());
            encoder.u64(assignment.revision);
        });
        self.u32(work.request.schema_version);
        self.str(&work.request.state.to_string());
        match &work.request.question {
            DecisionQuestion::Choice { prompt, options } => {
                self.u8(0);
                self.str(prompt);
                self.u32(options.len() as u32);
                for option in options {
                    self.str(&option.id);
                    self.str(&option.description);
                }
            }
            DecisionQuestion::BinaryProbability { prompt } => {
                self.u8(1);
                self.str(prompt);
            }
            DecisionQuestion::OrdinalScore { prompt, levels } => {
                self.u8(2);
                self.str(prompt);
                self.u32(levels.len() as u32);
                for level in levels {
                    self.0.extend_from_slice(&level.value.to_le_bytes());
                    self.str(&level.description);
                }
            }
        }
        self.opt(&work.prediction, |encoder, binding| {
            encoder.u128(binding.conversation.get());
            encoder.u128(binding.activity_turn.get());
            encoder.u64(binding.policy_revision);
            encoder.opt(&binding.policy_profile, Self::config_ref);
            encoder.u32(binding.max_tokens);
            encoder.compaction_policy(&binding.policy);
        });
        self.opt(&work.commands, |encoder, binding| {
            encoder.u128(binding.approval.job.get());
            encoder.u32(binding.approval.request.get());
            encoder.u32(binding.approval.index);
            encoder.u32(u32::from(binding.minimum_confidence_millis));
        });
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
    pub(super) fn new(bytes: &'a [u8], tag: u8) -> DecodeResult<Self> {
        match bytes {
            [found, _, ..] if *found == tag => Ok(Dec { bytes, at: 2 }),
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
    pub(super) fn take_array<const N: usize>(&mut self) -> DecodeResult<[u8; N]> {
        <[u8; N]>::try_from(self.take(N)?).map_err(|_| StorageError::corrupted("truncated record"))
    }
    pub(super) fn u8(&mut self) -> DecodeResult<u8> {
        Ok(self.take(1)?[0])
    }
    pub(super) fn u16(&mut self) -> DecodeResult<u16> {
        Ok(u16::from_le_bytes(self.take_array()?))
    }
    pub(super) fn u32(&mut self) -> DecodeResult<u32> {
        Ok(u32::from_le_bytes(self.take_array()?))
    }
    pub(super) fn u64(&mut self) -> DecodeResult<u64> {
        Ok(u64::from_le_bytes(self.take_array()?))
    }
    pub(super) fn u128(&mut self) -> DecodeResult<u128> {
        Ok(u128::from_le_bytes(self.take_array()?))
    }
    pub(super) fn i64(&mut self) -> DecodeResult<i64> {
        Ok(i64::from_le_bytes(self.take_array()?))
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
        let mut capabilities = BTreeSet::new();
        let count = self.u32()?;
        if count == 0 || count > 3 {
            return Err(StorageError::corrupted("invalid Agent capability count"));
        }
        for _ in 0..count {
            if !capabilities.insert(self.code(AgentCapability::from_code)?) {
                return Err(StorageError::corrupted("duplicate Agent capability"));
            }
        }
        let mut models = Vec::new();
        for _ in 0..self.u32()? {
            models.push(ConfigId::new(self.u128()?));
        }
        Ok(AgentConfig {
            instructions,
            capabilities,
            models,
            compaction_policy: self.opt(Dec::compaction_policy)?,
        })
    }
    pub(super) fn compaction_policy(&mut self) -> DecodeResult<CompactionPolicy> {
        let mode = match self.u8()? {
            0 => CompactionMode::Off,
            1 => CompactionMode::Auto {
                preset: match self.u8()? {
                    0 => CompactionPreset::Conservative,
                    1 => CompactionPreset::Balanced,
                    2 => CompactionPreset::Aggressive,
                    _ => return Err(StorageError::corrupted("invalid compaction preset")),
                },
            },
            2 => CompactionMode::Custom {
                policy: CustomPolicy {
                    trigger_tokens: self.u32()?,
                    target_tokens: self.u32()?,
                    reserved_output_tokens: self.u32()?,
                    minimum_source_tokens: self.u32()?,
                    minimum_savings_tokens: self.u32()?,
                    preserve_recent_turns: self.u32()?,
                    cooldown_seconds: self.u32()?,
                },
            },
            _ => return Err(StorageError::corrupted("invalid compaction mode")),
        };
        let policy = CompactionPolicy {
            mode,
            summarization_model: self.ou128()?.map(ConfigId::new),
            predictive: self.opt(|d| {
                Ok(PredictiveCompaction {
                    decision_agent: ConfigId::new(d.u128()?),
                    minimum_interval_seconds: d.u32()?,
                    max_preparations: d.u32()?,
                    max_prediction_tokens: d.u32()?,
                    max_preparation_tokens: d.u32()?,
                    mode: match d.u8()? {
                        0 => PredictionMode::Shadow,
                        1 => PredictionMode::Prepare,
                        _ => return Err(StorageError::corrupted("invalid prediction mode")),
                    },
                    acceptance: d.opt(|d| {
                        Ok(PredictionAcceptance {
                            model: d.config_ref()?,
                            reported_model: d.str()?,
                            confidence_semantics: d.str()?,
                            minimum_probability_millionths: d.u32()?,
                            minimum_confidence_millionths: d.u32()?,
                        })
                    })?,
                    max_preparations_per_hour: d.u32()?,
                    max_tokens_per_hour: d.u32()?,
                    max_requests_per_hour: d.u32()?,
                })
            })?,
        };
        policy.validate().map_err(StorageError::corrupted)?;
        Ok(policy)
    }
    pub(super) fn decision_model(&mut self) -> DecodeResult<DecisionModelConfig> {
        let connection_id = ConfigId::new(self.u128()?);
        let model = self.str()?;
        let protocol = self.code(DecisionProtocol::from_code)?;
        let limits = DecisionLimits {
            max_input_tokens_per_request: self.ou32()?,
            max_output_tokens_per_request: self.ou32()?,
            max_input_tokens_per_question: self.ou32()?,
            max_output_tokens_per_question: self.ou32()?,
        };
        let count = self.u32()?;
        if count > 3 {
            return Err(StorageError::corrupted("invalid decision capability count"));
        }
        let mut declared_capabilities = BTreeSet::new();
        for _ in 0..count {
            let cap = self.code(DecisionCapability::from_code)?;
            if !declared_capabilities.insert(cap) {
                return Err(StorageError::corrupted("duplicate decision capability"));
            }
        }
        Ok(DecisionModelConfig {
            connection_id,
            model,
            protocol,
            limits,
            declared_capabilities,
        })
    }
    pub(super) fn decision_agent(&mut self) -> DecodeResult<DecisionAgentConfig> {
        let instructions = self.ostr()?;
        let count = self.u32()?;
        if count > 256 {
            return Err(StorageError::corrupted("too many decision targets"));
        }
        let mut models = Vec::new();
        for _ in 0..count {
            models.push(ConfigId::new(self.u128()?));
        }
        Ok(DecisionAgentConfig {
            instructions,
            models,
            default_model: self.ou128()?.map(ConfigId::new),
        })
    }
    fn decision_work(&mut self) -> DecodeResult<DecisionWork> {
        let frozen = match self.u8()? {
            0 => FrozenDecisionConfig::Native(NativeDecisionConfig {
                agent: self.opt(|decoder| match decoder.u8()? {
                    0 => decoder
                        .agent_config()
                        .map(|agent| FrozenDecisionAgent::Agent(Box::new(agent))),
                    1 => decoder.decision_agent().map(FrozenDecisionAgent::Profile),
                    _ => Err(StorageError::corrupted("invalid decision Agent kind")),
                })?,
                agent_ref: self.opt(Self::config_ref)?,
                model: self.decision_model()?,
                model_ref: self.config_ref()?,
                connection_ref: self.config_ref()?,
                endpoint: self.ostr()?,
                credential_ref: self.ostr()?,
            }),
            1 => FrozenDecisionConfig::Generative(
                self.frozen_execution()?,
                self.opt(|decoder| Ok((decoder.decision_agent()?, decoder.config_ref()?)))?,
            ),
            _ => return Err(StorageError::corrupted("invalid decision backend")),
        };
        let assignment = self.opt(|decoder| {
            Ok(DecisionAssignment {
                project: ProjectId::new(decoder.u128()?),
                revision: decoder.u64()?,
            })
        })?;
        let schema_version = self.u32()?;
        let state = serde_json::from_str(&self.str()?)
            .map_err(|_| StorageError::corrupted("invalid decision state"))?;
        let kind = self.u8()?;
        let prompt = self.str()?;
        let question = match kind {
            0 => {
                let count = self.u32()?;
                if count > 256 {
                    return Err(StorageError::corrupted("too many choice options"));
                }
                let mut options = Vec::new();
                for _ in 0..count {
                    options.push(ChoiceOption {
                        id: self.str()?,
                        description: self.str()?,
                    });
                }
                DecisionQuestion::Choice { prompt, options }
            }
            1 => DecisionQuestion::BinaryProbability { prompt },
            2 => {
                let count = self.u32()?;
                if count > 256 {
                    return Err(StorageError::corrupted("too many score levels"));
                }
                let mut levels = Vec::new();
                for _ in 0..count {
                    levels.push(ScoreLevel {
                        value: self.u32()? as i32,
                        description: self.str()?,
                    });
                }
                DecisionQuestion::OrdinalScore { prompt, levels }
            }
            _ => return Err(StorageError::corrupted("invalid decision primitive")),
        };
        let prediction = self.opt(|decoder| {
            Ok(crate::db::entities::PredictionBinding {
                conversation: JobId::new(decoder.u128()?),
                activity_turn: JobId::new(decoder.u128()?),
                policy_revision: decoder.u64()?,
                policy_profile: decoder.opt(Self::config_ref)?,
                max_tokens: decoder.u32()?,
                policy: decoder.compaction_policy()?,
            })
        })?;
        let commands = self.opt(|decoder| {
            Ok(CommandAssessment {
                approval: crate::db::entities::ToolCallRef {
                    job: JobId::new(decoder.u128()?),
                    request: RequestNumber::new(decoder.u32()?).ok_or_else(|| {
                        StorageError::corrupted("invalid command assessment request")
                    })?,
                    index: decoder.u32()?,
                },
                minimum_confidence_millis: u16::try_from(decoder.u32()?)
                    .map_err(|_| StorageError::corrupted("invalid command confidence"))?,
            })
        })?;
        let work = DecisionWork {
            assignment,
            commands,
            frozen,
            prediction,
            request: DecisionRequest {
                schema_version,
                state,
                question,
            },
        };
        work.validate().map_err(StorageError::corrupted)?;
        Ok(work)
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
codes!(LlmTransport { LlmTransport::OpenAiChatCompletions = 1, LlmTransport::OpenAiResponses = 2, LlmTransport::AnthropicMessages = 3 });
codes!(ExecutionOperation { ExecutionOperation::LlmRequest = 1, ExecutionOperation::Decision = 2 });
codes!(ChatFinishReason {
    ChatFinishReason::Stop = 1, ChatFinishReason::Length = 2, ChatFinishReason::ToolCalls = 3,
    ChatFinishReason::ContentFilter = 4, ChatFinishReason::Error = 5, ChatFinishReason::Other = 6,
});
codes!(ToolCallRefusal {
    ToolCallRefusal::Malformed = 1, ToolCallRefusal::ConflictingIdentity = 2, ToolCallRefusal::AmbiguousIdentity = 3,
    ToolCallRefusal::LimitExceeded = 4, ToolCallRefusal::Incomplete = 5, ToolCallRefusal::Unauthorized = 6,
    ToolCallRefusal::MissingSchema = 7, ToolCallRefusal::UnsupportedSchema = 8, ToolCallRefusal::InvalidArguments = 9, ToolCallRefusal::NoDecider = 10,
});
codes!(ToolEffect { ToolEffect::ReadOnly = 1, ToolEffect::Idempotent = 2, ToolEffect::NonIdempotent = 3 });
codes!(AuthorizationBasis { AuthorizationBasis::RegistryGrant = 1 });
codes!(WithheldReason { WithheldReason::Unserializable = 2 });
codes!(UncertainCause { UncertainCause::TimedOut = 1, UncertainCause::Cancelled = 2, UncertainCause::HandlerReported = 3 });
codes!(EffectConclusion { EffectConclusion::Applied = 1, EffectConclusion::NotApplied = 2 });
codes!(AttachmentOrigin { AttachmentOrigin::UserProvided = 1, AttachmentOrigin::ModelProduced = 2 });
codes!(CleanupKind { CleanupKind::Manual = 1, CleanupKind::Automatic = 2 });

impl Enc {
    pub(super) fn call_ref(&mut self, call: &crate::db::entities::ToolCallRef) {
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
            JobKind::Group {
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
            JobKind::Execution {
                name,
                execution,
                plan_source,
            } => {
                self.u8(1);
                self.ostr(name);
                match execution {
                    JobExecution::Frozen(frozen) => {
                        self.u8(0);
                        self.frozen_execution(frozen);
                    }
                    JobExecution::Summarization(work) => {
                        self.u8(3);
                        self.frozen_execution(&work.frozen);
                        self.0.extend_from_slice(work.source.as_bytes());
                        self.u32(work.target_tokens);
                        self.u32(work.minimum_savings_tokens);
                        self.opt(&work.policy_revision, |encoder, revision| {
                            encoder.u64(*revision)
                        });
                        self.opt(&work.policy_profile, Self::config_ref);
                        match work.stage {
                            SummarizationStage::Direct => self.u8(0),
                            SummarizationStage::Chunk {
                                start,
                                end,
                                content,
                            } => {
                                self.u8(1);
                                self.u64(start);
                                self.u64(end);
                                self.0.extend_from_slice(content.as_bytes());
                            }
                            SummarizationStage::Reduce => self.u8(2),
                        }
                        self.ou128(work.chain.map(JobId::get));
                        self.ou128(work.prediction.map(JobId::get));
                        self.u32(work.reserved_tokens);
                        self.u32(work.reserved_requests);
                    }
                    JobExecution::Decision(work) => {
                        self.u8(2);
                        self.decision_work(work);
                    }
                    JobExecution::Ref(owner) => {
                        self.u8(1);
                        self.u128(owner.get());
                    }
                };
                match plan_source {
                    PlanSource::Stored {
                        previous,
                        dependencies,
                    } => {
                        self.u8(0);
                        self.ou128(previous.map(JobId::get));
                        self.u32(dependencies.len() as u32);
                        for (job, condition) in dependencies {
                            self.u128(job.get());
                            self.u8(condition.code());
                        }
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
            Entry::UserMessageEdit {
                message,
                text,
                attachments,
            } => {
                self.u8(9);
                self.u32(*message);
                self.str(text);
                self.u32(attachments.len() as u32);
                for attachment in attachments {
                    self.u128(attachment.get());
                }
            }
            Entry::UserMessageWithdraw { message } => {
                self.u8(10);
                self.u32(*message);
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
            Entry::ContextPreparation {
                job,
                policy_revision,
            } => {
                self.u8(7);
                self.u128(job.get());
                self.u64(*policy_revision);
            }
            Entry::ContextUse { artifact } => {
                self.u8(8);
                self.ou128(artifact.map(JobId::get));
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
    pub(super) fn call_ref(&mut self) -> DecodeResult<crate::db::entities::ToolCallRef> {
        Ok(crate::db::entities::ToolCallRef {
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
            0 => JobKind::Group {
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
            1 => JobKind::Execution {
                name: self.ostr()?,
                execution: match self.u8()? {
                    0 => JobExecution::Frozen(self.frozen_execution()?),
                    1 => JobExecution::Ref(JobId::new(self.u128()?)),
                    2 => JobExecution::Decision(self.decision_work()?),
                    3 => {
                        let work = crate::db::entities::SummarizationWork {
                            frozen: self.frozen_execution()?,
                            source: ContentDigest::from_bytes(
                                self.take(32)?.try_into().map_err(|_| {
                                    StorageError::corrupted("invalid source digest")
                                })?,
                            ),
                            target_tokens: self.u32()?,
                            minimum_savings_tokens: self.u32()?,
                            policy_revision: self.opt(Self::u64)?,
                            policy_profile: self.opt(Self::config_ref)?,
                            stage: match self.u8()? {
                                0 => SummarizationStage::Direct,
                                1 => SummarizationStage::Chunk {
                                    start: self.u64()?,
                                    end: self.u64()?,
                                    content: ContentDigest::from_bytes(
                                        self.take(32)?.try_into().map_err(|_| {
                                            StorageError::corrupted("invalid chunk digest")
                                        })?,
                                    ),
                                },
                                2 => SummarizationStage::Reduce,
                                _ => return Err(StorageError::corrupted("invalid summary stage")),
                            },
                            chain: self.ou128()?.map(JobId::new),
                            prediction: self.ou128()?.map(JobId::new),
                            reserved_tokens: self.u32()?,
                            reserved_requests: self.u32()?,
                        };
                        work.validate_stage().map_err(StorageError::corrupted)?;
                        JobExecution::Summarization(work)
                    }
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
            7 => Entry::ContextPreparation {
                job: JobId::new(self.u128()?),
                policy_revision: self.u64()?,
            },
            8 => Entry::ContextUse {
                artifact: self.ou128()?.map(JobId::new),
            },
            9 => {
                let message = self.u32()?;
                let text = self.str()?;
                let n = self.u32()?;
                let mut attachments = Vec::new();
                for _ in 0..n {
                    attachments.push(AttachmentId::new(self.u128()?));
                }
                Entry::UserMessageEdit {
                    message,
                    text,
                    attachments,
                }
            }
            10 => Entry::UserMessageWithdraw {
                message: self.u32()?,
            },
            6 => Entry::ResponseFormat {
                format: self.response_format()?,
            },
            _ => return Err(StorageError::corrupted("invalid entry")),
        })
    }
}
