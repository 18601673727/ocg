//! Deterministic OpenCode config generation.

use crate::config::Effective;
use crate::defaults::LEAD_ROLE;
use crate::error::{OcgError, Result};
use crate::json::{deep_merge, is_truthy};
use crate::model;
use crate::prompt;
use crate::validate;
use serde_json::{json, Map, Value};

/// Build compatibility configuration for one OCG Profile model key.
///
/// The result is validated, deterministic and independent of the current
/// environment: the same inputs always produce the same JSON. Callers that
/// already detected the runtime use [`build_opencode_config_for`] so the
/// delegation permission key matches the runtime family.
pub fn build_opencode_config(effective: &Effective, level: &str) -> Result<Value> {
    build_opencode_config_for(effective, level, crate::runtime::compat::v1_adapter())
}

/// Build compatibility configuration for a selected OCG Profile model.
///
/// No version conditional lives here: the adapter supplies the delegation
/// permission key.
pub fn build_opencode_config_for(
    effective: &Effective,
    level: &str,
    adapter: &dyn crate::runtime::compat::RuntimeAdapter,
) -> Result<Value> {
    validate::require_valid(effective)?;
    let data = &effective.data;

    let profile = crate::profile::Profile::from_ocg_config(data)?;
    profile.select(Some(level))?;

    let roles = model::role_specs(data).cloned().unwrap_or_default();
    let empty_permissions = Map::new();
    let permissions = data
        .get("permissions")
        .and_then(Value::as_object)
        .unwrap_or(&empty_permissions);
    let subagent = permissions
        .get("subagent")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let subagent_permission = subagent
        .get("permission")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let profiles = permissions
        .get("profiles")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let role_profiles = permissions
        .get("role_profiles")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let lead_permission = permissions
        .get("lead")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();

    let worker_agents: Vec<(String, String)> = roles
        .keys()
        .map(|role| (role.clone(), model::worker_agent_id(role)))
        .collect();

    let mut agent_config = Map::new();

    // One Lead contract for the explicitly selected Profile resource.
    let lead_template = prompt::read_prompt(effective, LEAD_ROLE)?.1;
    {
        let contract = model::lead_contract(data, level)?;
        let full = contract.full_model_id();
        let temperature = lead_permission
            .get("temperature")
            .cloned()
            .unwrap_or_else(|| json!(0.1));
        let rendered = prompt::render_lead_prompt(effective, level, &lead_template)?;

        let mut task = Map::new();
        task.insert(
            "*".to_string(),
            lead_permission
                .get("task_default")
                .cloned()
                .unwrap_or_else(|| json!("deny")),
        );
        for (_, agent) in &worker_agents {
            task.insert(agent.clone(), json!("allow"));
        }

        let mut lead_permissions = Map::new();
        lead_permissions.insert(adapter.task_key().to_string(), Value::Object(task));

        let mut lead_spec = Map::new();
        lead_spec.insert("mode".to_string(), json!("primary"));
        lead_spec.insert("model".to_string(), json!(full));
        lead_spec.insert("temperature".to_string(), temperature);
        lead_spec.insert("prompt".to_string(), json!(rendered));
        lead_spec.insert("permission".to_string(), Value::Object(lead_permissions));
        if let Some(variant) = contract.variant {
            lead_spec.insert("variant".to_string(), json!(variant));
        }
        agent_config.insert(model::lead_agent_id(level), Value::Object(lead_spec));
    }

    // Worker agents: only explicitly configured roles, independent of Lead selection.
    for (role, spec) in &roles {
        let model_key = spec
            .get("model")
            .and_then(Value::as_str)
            .ok_or_else(|| OcgError::config(format!("routing role '{role}' is missing 'model'")))?;
        let (_, full) = model::model_full_id(data, model_key)?;
        let (meta, body) = prompt::read_prompt(effective, role)?;

        let profile = role_profiles
            .get(role)
            .and_then(Value::as_str)
            .and_then(|name| profiles.get(name))
            .cloned()
            .unwrap_or_else(|| json!({}));
        let permission = with_task_key(
            deep_merge(&subagent_permission, &profile),
            adapter.task_key(),
        );

        let description = spec
            .get("description")
            .filter(|value| is_truthy(value))
            .cloned()
            .or_else(|| meta.get("description").map(|value| json!(value)))
            .unwrap_or_else(|| json!(role));

        let hidden = subagent.get("hidden").map(is_truthy).unwrap_or(true);

        let mut worker_spec = Map::new();
        worker_spec.insert("mode".to_string(), json!("subagent"));
        worker_spec.insert("model".to_string(), json!(full));
        worker_spec.insert("description".to_string(), description);
        worker_spec.insert("prompt".to_string(), json!(body));
        worker_spec.insert("hidden".to_string(), json!(hidden));
        worker_spec.insert("permission".to_string(), permission);
        if let Some(temperature) = meta.get("temperature") {
            let parsed: f64 = temperature.parse().map_err(|_| {
                OcgError::config(format!("{role} prompt temperature is not a number"))
            })?;
            worker_spec.insert("temperature".to_string(), json!(parsed));
        }
        if let Some(variant) = spec
            .get("variant")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
        {
            worker_spec.insert("variant".to_string(), json!(variant));
        }
        agent_config.insert(model::worker_agent_id(role), Value::Object(worker_spec));
    }

    let mut result = Map::new();
    result.insert(
        "default_agent".to_string(),
        json!(model::lead_agent_id(level)),
    );
    result.insert(
        "model".to_string(),
        json!(model::model_full_id(data, level)?.1),
    );
    result.insert(
        "enabled_providers".to_string(),
        json!(model::enabled_provider_order(data, Some(level))),
    );
    result.insert("agent".to_string(), Value::Object(agent_config));
    if let Some(small) = data
        .get("routing")
        .and_then(|routing| routing.get("small_model"))
        .and_then(Value::as_str)
    {
        result.insert(
            "small_model".to_string(),
            json!(model::model_full_id(data, small)?.1),
        );
    }

    let base = data.get("base").cloned().unwrap_or_else(|| json!({}));
    let mut merged = deep_merge(&base, &Value::Object(result));
    if let Some(extra) = data.get("opencode").filter(|value| value.is_object()) {
        merged = deep_merge(&merged, extra);
    }

    Ok(merged)
}

/// Rename the top-level `task` permission key to the runtime's delegation key.
///
/// OpenCode 1 uses `task`; OpenCode 2 renamed the tool to `subagent`. The
/// profiles and defaults stay written once, in the project-agnostic v1 shape.
fn with_task_key(permission: Value, task_key: &str) -> Value {
    if task_key == "task" {
        return permission;
    }
    let Value::Object(mut map) = permission else {
        return permission;
    };
    if let Some(task) = map.remove("task") {
        map.insert(task_key.to_string(), task);
    }
    Value::Object(map)
}
