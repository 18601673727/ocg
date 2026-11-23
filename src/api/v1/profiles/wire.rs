use super::super::contracts::{CompactionPolicyInput, OutboundHeaderInput, OutboundHeaderView};
use crate::db::entities::{AgentCapability, LlmParameters};
use serde::Deserialize;
use std::collections::BTreeSet;

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ParametersInput {
    #[serde(default)]
    pub(super) temperature: Option<f64>,
    #[serde(default)]
    pub(super) top_p: Option<f64>,
    #[serde(default)]
    pub(super) max_tokens: Option<u32>,
    #[serde(default)]
    pub(super) reasoning_effort: Option<String>,
    #[serde(default)]
    pub(super) max_completion_tokens: Option<u32>,
    #[serde(default)]
    pub(super) presence_penalty: Option<f64>,
    #[serde(default)]
    pub(super) frequency_penalty: Option<f64>,
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum ProfileRequest {
    Llm {
        name: String,
        model: String,
        #[serde(default)]
        context_window: Option<u32>,
        #[serde(default)]
        protocol: Option<String>,
        #[serde(default)]
        endpoint: Option<String>,
        #[serde(default)]
        credential_ref: Option<String>,
        #[serde(default)]
        credential: Option<String>,
        #[serde(default)]
        clear_credential: bool,
        #[serde(default)]
        declared_capabilities: Vec<String>,
        #[serde(default)]
        parameters: ParametersInput,
        #[serde(default)]
        headers: Option<Vec<OutboundHeaderInput>>,
        #[serde(default)]
        expected_edit_seq: Option<u32>,
    },
    Agent {
        name: String,
        #[serde(default)]
        instructions: Option<String>,
        #[serde(default)]
        capabilities: Vec<String>,
        #[serde(default)]
        llm_targets: Vec<String>,
        #[serde(default, deserialize_with = "optional_value")]
        compaction_policy: Option<Option<CompactionPolicyInput>>,
        #[serde(default)]
        expected_edit_seq: Option<u32>,
    },
    DecisionModel {
        name: String,
        model: String,
        #[serde(default)]
        protocol: Option<String>,
        #[serde(default)]
        endpoint: Option<String>,
        #[serde(default)]
        credential_ref: Option<String>,
        #[serde(default)]
        credential: Option<String>,
        #[serde(default)]
        clear_credential: bool,
        #[serde(default)]
        declared_capabilities: Vec<String>,
        #[serde(default)]
        limits: DecisionLimitInput,
        #[serde(default)]
        headers: Option<Vec<OutboundHeaderInput>>,
        #[serde(default)]
        expected_edit_seq: Option<u32>,
    },
    DecisionAgent {
        name: String,
        #[serde(default)]
        instructions: Option<String>,
        #[serde(default)]
        models: Vec<String>,
        #[serde(default, deserialize_with = "optional_value")]
        default_model: Option<Option<String>>,
        #[serde(default)]
        expected_edit_seq: Option<u32>,
    },
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub(super) struct DecisionLimitInput {
    #[serde(default)]
    pub(super) max_input_tokens_per_request: Option<u32>,
    #[serde(default)]
    pub(super) max_output_tokens_per_request: Option<u32>,
    #[serde(default)]
    pub(super) max_input_tokens_per_question: Option<u32>,
    #[serde(default)]
    pub(super) max_output_tokens_per_question: Option<u32>,
}

pub(crate) fn optional_value<'de, D, T>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer).map(Some)
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "LlmProfile")]
pub(crate) struct LlmProfileView {
    pub(super) edit_seq: u32,
    pub(super) enabled: bool,
    pub(super) reasoning_efforts: Option<Vec<String>>,
    pub(super) id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub(super) profile_type: Option<String>,
    pub(super) name: String,
    pub(super) model: String,
    pub(super) context_window: Option<u32>,
    pub(super) protocol: String,
    pub(super) endpoint: Option<String>,
    pub(super) credential_ref: Option<String>,
    pub(super) has_credential: bool,
    pub(super) headers: Vec<OutboundHeaderView>,
    pub(super) declared_capabilities: Vec<String>,
    pub(super) parameters: LlmParameters,
    pub(super) editable: bool,
    pub(super) blocking_jobs: Vec<String>,
    pub(super) more_blocking_jobs: bool,
    pub(super) created_at_us: i64,
    pub(super) updated_at_us: i64,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "ManagedAgent")]
pub(crate) struct ManagedAgentView {
    pub(super) id: String,
    pub(super) edit_seq: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub(super) profile_type: Option<String>,
    pub(super) name: String,
    pub(super) instructions: Option<String>,
    pub(super) capabilities: BTreeSet<AgentCapability>,
    pub(super) llm_targets: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub(super) compaction_policy: Option<CompactionPolicyInput>,
    pub(super) editable: bool,
    pub(super) blocking_jobs: Vec<String>,
    pub(super) more_blocking_jobs: bool,
    pub(super) created_at_us: i64,
    pub(super) updated_at_us: i64,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "DecisionLimits")]
pub(crate) struct DecisionLimitsView {
    pub(super) max_input_tokens_per_request: Option<u32>,
    pub(super) max_output_tokens_per_request: Option<u32>,
    pub(super) max_input_tokens_per_question: Option<u32>,
    pub(super) max_output_tokens_per_question: Option<u32>,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "DecisionProfile")]
pub(crate) struct DecisionProfileView {
    pub(super) id: String,
    pub(super) edit_seq: u32,
    pub(super) name: String,
    pub(super) model: String,
    pub(super) protocol: String,
    pub(super) endpoint: Option<String>,
    pub(super) credential_ref: Option<String>,
    pub(super) has_credential: bool,
    pub(super) headers: Vec<OutboundHeaderView>,
    pub(super) declared_capabilities: Vec<String>,
    pub(super) limits: DecisionLimitsView,
    pub(super) editable: bool,
    pub(super) created_at_us: i64,
    pub(super) updated_at_us: i64,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "DecisionAgentProfile")]
pub(crate) struct DecisionAgentProfileView {
    pub(super) id: String,
    pub(super) edit_seq: u32,
    pub(super) name: String,
    pub(super) instructions: Option<String>,
    pub(super) models: Vec<String>,
    pub(super) default_model: Option<String>,
    pub(super) editable: bool,
    pub(super) created_at_us: i64,
    pub(super) updated_at_us: i64,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "ProfileExecutor")]
pub(crate) struct ProfileExecutorView {
    pub(super) id: String,
    pub(super) protocol: String,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "Provider")]
pub(crate) struct ProviderView {
    pub(super) id: String,
    pub(super) name: String,
    pub(super) protocol: String,
    pub(super) endpoint: Option<String>,
    pub(super) credential_ref: Option<String>,
    pub(super) has_credential: bool,
    pub(super) headers: Vec<OutboundHeaderView>,
    pub(super) profile_ids: Vec<String>,
    pub(super) models: Vec<String>,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "Profiles")]
pub(crate) struct ProfilesView {
    pub(super) llm_profiles: Vec<LlmProfileView>,
    pub(super) agent_profiles: Vec<ManagedAgentView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub(super) decision_profiles: Option<Vec<DecisionProfileView>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub(super) decision_agent_profiles: Option<Vec<DecisionAgentProfileView>>,
    pub(super) executor_protocols: Vec<ProfileExecutorView>,
    pub(super) providers: Vec<ProviderView>,
    pub(super) has_more: bool,
    pub(super) next_before: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub(super) profile_id: Option<String>,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "ImportedModels")]
pub(crate) struct ImportedModelsView {
    pub(super) profile_ids: Vec<String>,
    pub(super) created: usize,
    pub(super) existing: usize,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "ProviderModels")]
pub(crate) struct ProviderModelsView {
    pub(super) models: Vec<crate::db::entities::DiscoveredModel>,
}
