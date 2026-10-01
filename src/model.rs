//! Model registry and role resolution.
//!
//! Roles are durable; providers and model ids are replaceable and live in
//! `config/models.yaml`. Routing roles are arbitrary: the code only assumes a
//! role maps to a model key.

use crate::error::{OcgError, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

pub fn providers(data: &Value) -> Option<&Map<String, Value>> {
    data.get("models")
        .and_then(|models| models.get("providers"))
        .and_then(Value::as_object)
}

pub fn model_registry(data: &Value) -> Option<&Map<String, Value>> {
    data.get("models")
        .and_then(|models| models.get("models"))
        .and_then(Value::as_object)
}

pub fn role_specs(data: &Value) -> Option<&Map<String, Value>> {
    data.get("routing")
        .and_then(|routing| routing.get("roles"))
        .and_then(Value::as_object)
}

pub fn provider_label(data: &Value, provider: &str) -> String {
    providers(data)
        .and_then(|map| map.get(provider))
        .and_then(Value::as_object)
        .and_then(|entry| entry.get("label"))
        .and_then(Value::as_str)
        .filter(|label| !label.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| provider.to_string())
}

pub fn model_entry<'a>(data: &'a Value, key: &str) -> Result<&'a Map<String, Value>> {
    model_registry(data)
        .and_then(|registry| registry.get(key))
        .and_then(Value::as_object)
        .ok_or_else(|| OcgError::config(format!("unknown model key: '{key}'")))
}

/// `(provider, "provider/model_id")` for a model key.
///
/// This resolves the registry entry only. Placeholder and runnability checks
/// belong to the *selection* paths (`Profile::select`, `lead_contract`,
/// provider configuration), so diagnostics, worker routing and validation
/// can still read every configured resource, including placeholders.
pub fn model_full_id(data: &Value, key: &str) -> Result<(String, String)> {
    let entry = model_entry(data, key)?;
    let provider = entry
        .get("provider")
        .and_then(Value::as_str)
        .filter(|provider| !provider.is_empty())
        .ok_or_else(|| OcgError::config(format!("model '{key}' has no provider")))?;
    let id = entry
        .get("id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .ok_or_else(|| OcgError::config(format!("model '{key}' has no id")))?;
    Ok((provider.to_string(), format!("{provider}/{id}")))
}

pub fn model_label(data: &Value, key: &str) -> String {
    match model_entry(data, key) {
        Ok(entry) => entry
            .get("label")
            .and_then(Value::as_str)
            .filter(|label| !label.is_empty())
            .or_else(|| entry.get("id").and_then(Value::as_str))
            .unwrap_or(key)
            .to_string(),
        Err(_) => key.to_string(),
    }
}

pub fn lead_agent_id(_model_key: &str) -> String {
    "lead".to_string()
}

pub fn worker_agent_id(role: &str) -> String {
    format!("{}{role}", crate::defaults::WORKER_AGENT_PREFIX)
}

/// The primary Lead request contract selected from the OCG Profile.
///
/// This value is resolved in Rust and exported to the launched runtime, so the
/// Lead is fixed after OCG has applied any sticky UI/session selection but
/// before the user message is saved or sent to a provider.
///
/// The Lead is provider-agnostic: `provider_id`/`model_id` come from the
/// configured registry, not from a hardcoded OpenAI assumption. A reasoning
/// `variant` is optional and provider-specific. When the resolved model declares
/// no variant, it is `None` and the request is left
/// at the provider default; it is never fabricated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeadContract {
    pub level: String,
    pub agent: String,
    pub provider_id: String,
    pub model_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variant: Option<String>,
}

impl LeadContract {
    pub fn full_model_id(&self) -> String {
        format!("{}/{}", self.provider_id, self.model_id)
    }
}

/// Resolve the runtime Lead contract for an open-ended Profile model key.
pub fn lead_contract(data: &Value, model_key: &str) -> Result<LeadContract> {
    let profile = crate::profile::Profile::from_ocg_config(data)?;
    profile.select(Some(model_key))?;
    let entry = model_entry(data, model_key)?;
    let provider_id = entry
        .get("provider")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| OcgError::config(format!("model '{model_key}' has no provider")))?;
    let model_id = entry
        .get("id")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| OcgError::config(format!("model '{model_key}' has no id")))?;
    let variant = entry
        .get("variant")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    Ok(LeadContract {
        level: model_key.to_string(),
        agent: lead_agent_id(model_key),
        provider_id: provider_id.to_string(),
        model_id: model_id.to_string(),
        variant,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelRequirementKind {
    Lead,
    Worker,
}

/// One provider/model exposed by the active execution configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelRequirement {
    pub label: String,
    pub agent: String,
    pub full_model_id: String,
    pub variant: Option<String>,
    pub kind: ModelRequirementKind,
}

/// The selected Lead and explicitly configured worker roles.
///
/// `selected` is the model key this invocation will actually use (`--model` or
/// `profile.defaultModel`). Probing only the Profile default would let a launch
/// select a different, unavailable model and proceed.
pub fn runtime_model_requirements(
    data: &Value,
    selected: Option<&str>,
) -> Result<Vec<ModelRequirement>> {
    let mut requirements = Vec::new();
    let profile = crate::profile::Profile::from_ocg_config(data)?;
    if let Some(choice) = selected.or(profile.default_model.as_deref()) {
        let contract = lead_contract(data, choice)?;
        let full_model_id = contract.full_model_id();
        requirements.push(ModelRequirement {
            label: contract.agent.clone(),
            agent: contract.agent,
            full_model_id,
            variant: contract.variant,
            kind: ModelRequirementKind::Lead,
        });
    }
    if let Some(roles) = role_specs(data) {
        for (role, spec) in roles {
            let key = spec.get("model").and_then(Value::as_str).ok_or_else(|| {
                OcgError::config(format!("routing role '{role}' is missing 'model'"))
            })?;
            requirements.push(ModelRequirement {
                label: role.clone(),
                agent: worker_agent_id(role),
                full_model_id: model_full_id(data, key)?.1,
                variant: spec
                    .get("variant")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                kind: ModelRequirementKind::Worker,
            });
        }
    }
    Ok(requirements)
}

/// Every model key referenced by the selected Lead, role, fallback or small_model.
fn referenced_model_keys(data: &Value, selected: Option<&str>) -> Vec<String> {
    let mut keys = Vec::new();
    if let Ok(profile) = crate::profile::Profile::from_ocg_config(data) {
        if let Some(choice) = selected.or(profile.default_model.as_deref()) {
            keys.push(choice.to_string());
        }
    }
    if let Some(roles) = role_specs(data) {
        for spec in roles.values() {
            if let Some(key) = spec.get("model").and_then(Value::as_str) {
                keys.push(key.to_string());
            }
            if let Some(fallbacks) = spec.get("fallback").and_then(Value::as_array) {
                for fallback in fallbacks {
                    if let Some(key) = fallback.get("model").and_then(Value::as_str) {
                        keys.push(key.to_string());
                    }
                }
            }
        }
    }
    if let Some(small) = data
        .get("routing")
        .and_then(|routing| routing.get("small_model"))
        .and_then(Value::as_str)
    {
        keys.push(small.to_string());
    }
    keys
}

/// Providers used by the resolved routing, in declaration order. Undeclared
/// but used providers are appended so nothing silently disappears.
pub fn enabled_provider_order(data: &Value, selected: Option<&str>) -> Vec<String> {
    let mut used: Vec<String> = Vec::new();
    for key in referenced_model_keys(data, selected) {
        let Ok((provider, _)) = model_full_id(data, &key) else {
            continue;
        };
        if !used.contains(&provider) {
            used.push(provider);
        }
    }
    let mut ordered: Vec<String> = Vec::new();
    if let Some(declared) = providers(data) {
        for name in declared.keys() {
            if used.contains(name) && !ordered.contains(name) {
                ordered.push(name.clone());
            }
        }
    }
    for name in &used {
        if !ordered.contains(name) {
            ordered.push(name.clone());
        }
    }
    ordered
}

/// `(role, agent, provider label, provider/model)` rows for reporting.
pub fn routing_rows(data: &Value) -> Result<Vec<(String, String, String, String)>> {
    let mut rows = Vec::new();
    if let Some(roles) = role_specs(data) {
        for (role, spec) in roles {
            let key = spec.get("model").and_then(Value::as_str).ok_or_else(|| {
                OcgError::config(format!("routing role '{role}' is missing 'model'"))
            })?;
            let (provider, full) = model_full_id(data, key)?;
            rows.push((
                role.clone(),
                worker_agent_id(role),
                provider_label(data, &provider),
                full,
            ));
        }
    }
    Ok(rows)
}
