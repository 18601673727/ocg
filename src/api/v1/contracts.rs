use serde_json::Value;

pub(crate) fn envelope(mut payload: Value) -> Value {
    if let Some(fields) = payload.as_object_mut() {
        fields.insert("api_version".into(), Value::from(super::API_VERSION));
    }
    payload
}

#[derive(Clone, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CompactionPresetInput {
    Conservative,
    Balanced,
    Aggressive,
}

#[derive(Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CustomCompactionInput {
    pub trigger_tokens: u32,
    pub target_tokens: u32,
    pub reserved_output_tokens: u32,
    pub minimum_source_tokens: u32,
    pub minimum_savings_tokens: u32,
    pub preserve_recent_turns: u32,
    pub cooldown_seconds: u32,
}

#[derive(Clone, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum CompactionModeInput {
    Off,
    Auto { preset: CompactionPresetInput },
    Custom { policy: CustomCompactionInput },
}

#[derive(Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PredictionAcceptanceInput {
    pub model: String,
    pub model_edit_seq: u32,
    pub reported_model: String,
    pub confidence_semantics: String,
    pub minimum_probability_millionths: u32,
    pub minimum_confidence_millionths: u32,
}

#[derive(Clone, serde::Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PredictionModeInput {
    #[default]
    Shadow,
    Prepare,
}

#[derive(Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PredictiveCompactionInput {
    pub decision_agent: String,
    pub minimum_interval_seconds: u32,
    pub max_preparations: u32,
    pub max_prediction_tokens: u32,
    pub max_preparation_tokens: u32,
    #[serde(default)]
    pub mode: PredictionModeInput,
    #[serde(default)]
    pub acceptance: Option<PredictionAcceptanceInput>,
    #[serde(default)]
    pub max_preparations_per_hour: u32,
    #[serde(default)]
    pub max_tokens_per_hour: u32,
    #[serde(default)]
    pub max_requests_per_hour: u32,
}

#[derive(Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CompactionPolicyInput {
    pub mode: CompactionModeInput,
    pub summarization_model: Option<String>,
    pub predictive: Option<PredictiveCompactionInput>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GlobalCompactionSettingsInput {
    pub revision: u64,
    pub prediction_enabled: bool,
    pub policy: CompactionPolicyInput,
}

impl CompactionPolicyInput {
    pub(crate) fn into_policy(
        self,
    ) -> Result<crate::db::entities::CompactionPolicy, super::Refusal> {
        use crate::db::entities::{
            CompactionMode, CompactionPreset, CustomPolicy, PredictiveCompaction,
        };
        let id = |value: String| {
            super::profiles::profile_id(&value).map(crate::db::entities::ConfigId::from)
        };
        let policy = crate::db::entities::CompactionPolicy {
            mode: match self.mode {
                CompactionModeInput::Off => CompactionMode::Off,
                CompactionModeInput::Auto { preset } => CompactionMode::Auto {
                    preset: match preset {
                        CompactionPresetInput::Conservative => CompactionPreset::Conservative,
                        CompactionPresetInput::Balanced => CompactionPreset::Balanced,
                        CompactionPresetInput::Aggressive => CompactionPreset::Aggressive,
                    },
                },
                CompactionModeInput::Custom { policy } => CompactionMode::Custom {
                    policy: CustomPolicy {
                        trigger_tokens: policy.trigger_tokens,
                        target_tokens: policy.target_tokens,
                        reserved_output_tokens: policy.reserved_output_tokens,
                        minimum_source_tokens: policy.minimum_source_tokens,
                        minimum_savings_tokens: policy.minimum_savings_tokens,
                        preserve_recent_turns: policy.preserve_recent_turns,
                        cooldown_seconds: policy.cooldown_seconds,
                    },
                },
            },
            summarization_model: self.summarization_model.map(id).transpose()?,
            predictive: self
                .predictive
                .map(|prediction| {
                    Ok::<_, super::Refusal>(PredictiveCompaction {
                        decision_agent: id(prediction.decision_agent)?,
                        minimum_interval_seconds: prediction.minimum_interval_seconds,
                        max_preparations: prediction.max_preparations,
                        max_prediction_tokens: prediction.max_prediction_tokens,
                        max_preparation_tokens: prediction.max_preparation_tokens,
                        mode: match prediction.mode {
                            PredictionModeInput::Shadow => {
                                crate::db::entities::PredictionMode::Shadow
                            }
                            PredictionModeInput::Prepare => {
                                crate::db::entities::PredictionMode::Prepare
                            }
                        },
                        acceptance: prediction
                            .acceptance
                            .map(|acceptance| {
                                Ok::<_, super::Refusal>(crate::db::entities::PredictionAcceptance {
                                    model: crate::db::entities::ConfigRef {
                                        id: id(acceptance.model)?,
                                        edit_seq: acceptance.model_edit_seq,
                                    },
                                    reported_model: acceptance.reported_model,
                                    confidence_semantics: acceptance.confidence_semantics,
                                    minimum_probability_millionths: acceptance
                                        .minimum_probability_millionths,
                                    minimum_confidence_millionths: acceptance
                                        .minimum_confidence_millionths,
                                })
                            })
                            .transpose()?,
                        max_preparations_per_hour: prediction.max_preparations_per_hour,
                        max_tokens_per_hour: prediction.max_tokens_per_hour,
                        max_requests_per_hour: prediction.max_requests_per_hour,
                    })
                })
                .transpose()?,
        };
        policy.validate().map_err(|_| super::INVALID_REQUEST)?;
        Ok(policy)
    }
}

pub(crate) fn llm_parameters_json(value: &crate::db::entities::LlmParameters) -> Value {
    serde_json::json!({
        "temperature": value.temperature,
        "top_p": value.top_p,
        "max_tokens": value.max_tokens,
        "reasoning_effort": value.reasoning_effort.clone(),
        "max_completion_tokens": value.max_completion_tokens,
        "presence_penalty": value.presence_penalty,
        "frequency_penalty": value.frequency_penalty,
    })
}

pub(crate) fn compaction_policy_json(policy: crate::db::entities::CompactionPolicy) -> Value {
    use crate::db::entities::{CompactionMode, CompactionPreset, PredictionMode};

    let mode = match policy.mode {
        CompactionMode::Off => serde_json::json!({ "kind": "off" }),
        CompactionMode::Auto { preset } => serde_json::json!({
            "kind": "auto",
            "preset": match preset {
                CompactionPreset::Conservative => "conservative",
                CompactionPreset::Balanced => "balanced",
                CompactionPreset::Aggressive => "aggressive",
            },
        }),
        CompactionMode::Custom { policy } => serde_json::json!({
            "kind": "custom",
            "policy": {
                "trigger_tokens": policy.trigger_tokens,
                "target_tokens": policy.target_tokens,
                "reserved_output_tokens": policy.reserved_output_tokens,
                "minimum_source_tokens": policy.minimum_source_tokens,
                "minimum_savings_tokens": policy.minimum_savings_tokens,
                "preserve_recent_turns": policy.preserve_recent_turns,
                "cooldown_seconds": policy.cooldown_seconds,
            },
        }),
    };
    let predictive = policy.predictive.map(|prediction| {
        let acceptance = prediction.acceptance.map(|acceptance| {
            serde_json::json!({
                "model": acceptance.model.id.to_string(),
                "model_edit_seq": acceptance.model.edit_seq,
                "reported_model": acceptance.reported_model,
                "confidence_semantics": acceptance.confidence_semantics,
                "minimum_probability_millionths": acceptance.minimum_probability_millionths,
                "minimum_confidence_millionths": acceptance.minimum_confidence_millionths,
            })
        });
        serde_json::json!({
            "decision_agent": prediction.decision_agent.to_string(),
            "minimum_interval_seconds": prediction.minimum_interval_seconds,
            "max_preparations": prediction.max_preparations,
            "max_prediction_tokens": prediction.max_prediction_tokens,
            "max_preparation_tokens": prediction.max_preparation_tokens,
            "mode": match prediction.mode {
                PredictionMode::Shadow => "shadow",
                PredictionMode::Prepare => "prepare",
            },
            "acceptance": acceptance,
            "max_preparations_per_hour": prediction.max_preparations_per_hour,
            "max_tokens_per_hour": prediction.max_tokens_per_hour,
            "max_requests_per_hour": prediction.max_requests_per_hour,
        })
    });
    serde_json::json!({
        "mode": mode,
        "summarization_model": policy.summarization_model.map(|id| id.to_string()),
        "predictive": predictive,
    })
}
