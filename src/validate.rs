//! Whole-configuration validation.

use crate::config::Effective;
use crate::defaults::LEAD_ROLE;
use crate::error::{OcgError, Result};
use crate::json::{is_truthy, value_to_string};
use crate::model;
use crate::prompt;
use serde_json::{Map, Value};

/// Return every validation error. An empty vector means the config is valid.
pub fn validate(effective: &Effective) -> Vec<String> {
    let data = &effective.data;
    let mut errors = Vec::new();

    errors.extend(crate::runtime::policy::RuntimePolicy::validate(data));
    errors.extend(crate::context::ContextConfig::validate(data));
    errors.extend(crate::verification::Config::validate(data));
    errors.extend(crate::capabilities::CapabilityConfig::validate(data));
    errors.extend(crate::telemetry::TelemetryConfig::validate(data));
    errors.extend(crate::orchestration::OrchestrationConfig::validate(data));
    errors.extend(crate::orchestration::budget::BudgetConfig::validate(data));

    if let Err(error) = crate::profile::Profile::from_ocg_config(data) {
        errors.push(error.to_string());
    }

    let empty_roles = Map::new();
    let roles = data
        .get("routing")
        .and_then(|routing| routing.get("roles"))
        .and_then(Value::as_object)
        .unwrap_or(&empty_roles);
    for (role, spec) in roles {
        if spec.get("model").is_none() {
            errors.push(format!("routing role '{role}' is missing 'model'"));
            continue;
        }
        let model_key = spec.get("model").and_then(Value::as_str).unwrap_or("");
        if model::model_registry(data)
            .and_then(|registry| registry.get(model_key))
            .is_none()
        {
            errors.push(format!(
                "routing role '{role}' references unknown model '{model_key}'"
            ));
        }
        validate_variant(data, spec, &format!("routing role '{role}'"), &mut errors);
        if let Some(fallbacks) = spec.get("fallback").and_then(Value::as_array) {
            for fallback in fallbacks {
                if fallback.get("model").is_none() {
                    errors.push(format!(
                        "routing role '{role}' has a malformed fallback entry"
                    ));
                    continue;
                }
                let fallback_key = fallback.get("model").and_then(Value::as_str).unwrap_or("");
                if model::model_registry(data)
                    .and_then(|registry| registry.get(fallback_key))
                    .is_none()
                {
                    errors.push(format!(
                        "routing role '{role}' fallback references unknown model '{fallback_key}'"
                    ));
                }
                validate_variant(
                    data,
                    fallback,
                    &format!("routing role '{role}' fallback"),
                    &mut errors,
                );
            }
        }
    }

    if let Some(small) = data
        .get("routing")
        .and_then(|routing| routing.get("small_model"))
        .and_then(Value::as_str)
    {
        if model::model_registry(data)
            .and_then(|registry| registry.get(small))
            .is_none()
        {
            errors.push(format!(
                "routing.small_model references unknown model '{small}'"
            ));
        }
    }

    if let Some(registry) = model::model_registry(data) {
        for (key, entry) in registry {
            let Some(entry) = entry.as_object() else {
                errors.push(format!("model '{key}' must be an object"));
                continue;
            };
            let provider = entry.get("provider").and_then(Value::as_str).unwrap_or("");
            let declared = model::providers(data)
                .map(|providers| providers.contains_key(provider))
                .unwrap_or(false);
            if !declared {
                errors.push(format!(
                    "model '{key}' uses provider '{provider}', which is not declared in models.providers"
                ));
            }
            if !entry.get("id").map(is_truthy).unwrap_or(false) {
                errors.push(format!("model '{key}' is missing 'id'"));
            }
        }
    }

    let profiles = data
        .get("permissions")
        .and_then(|permissions| permissions.get("profiles"))
        .and_then(Value::as_object);
    if let Some(role_profiles) = data
        .get("permissions")
        .and_then(|permissions| permissions.get("role_profiles"))
        .and_then(Value::as_object)
    {
        for role in roles.keys() {
            if let Some(profile) = role_profiles.get(role).and_then(Value::as_str) {
                let known = profiles
                    .map(|profiles| profiles.contains_key(profile))
                    .unwrap_or(false);
                if !known {
                    errors.push(format!(
                        "permissions.role_profiles['{role}'] points at unknown profile '{profile}'"
                    ));
                }
            }
        }
    }

    for role in std::iter::once(LEAD_ROLE).chain(roles.keys().map(String::as_str)) {
        if let Err(error) = prompt::read_prompt(effective, role) {
            errors.push(error.to_string());
        }
    }

    errors
}

/// Validate and return a single error describing every problem.
pub fn require_valid(effective: &Effective) -> Result<()> {
    let errors = validate(effective);
    if errors.is_empty() {
        Ok(())
    } else {
        Err(OcgError::config(format!(
            "configuration is invalid:\n  - {}",
            errors.join("\n  - ")
        )))
    }
}

fn validate_variant(data: &Value, spec: &Value, location: &str, errors: &mut Vec<String>) {
    let Some(variant) = spec.get("variant") else {
        return;
    };
    // `null` and the empty string both mean "provider default": the variant is
    // absent, not invalid.
    if variant.is_null() {
        return;
    }
    let Some(variant) = variant.as_str() else {
        errors.push(format!("{location}: variant must be a string"));
        return;
    };
    if variant.is_empty() {
        return;
    }
    let model_key = spec.get("model").and_then(Value::as_str).unwrap_or("");
    let Some(entry) = model::model_registry(data)
        .and_then(|registry| registry.get(model_key))
        .and_then(Value::as_object)
    else {
        return; // unknown model already reported
    };
    if let Some(variants) = entry.get("variants").and_then(Value::as_array) {
        if !variants.iter().any(|value| value.as_str() == Some(variant)) {
            let available = variants
                .iter()
                .map(value_to_string)
                .collect::<Vec<_>>()
                .join(", ");
            errors.push(format!(
                "{location}: variant '{variant}' is not exposed by model '{model_key}' (available: {available})"
            ));
        }
    }
}
