//! Prompt loading, frontmatter, append semantics and Lead rendering.

use crate::config::Effective;
use crate::defaults::{embedded_prompt, EMBEDDED_PROMPT_PREFIX, PROMPT_APPEND_SEPARATOR};
use crate::error::{OcgError, Result};
use crate::json::value_to_string;
use crate::model;
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

/// Split optional `---` frontmatter from the prompt body.
///
/// Only simple `key: value` lines are supported, matching the shipped prompts.
pub fn split_frontmatter(text: &str) -> (BTreeMap<String, String>, String) {
    if text.starts_with("---") {
        let parts: Vec<&str> = text.splitn(3, "---").collect();
        if parts.len() == 3 {
            let mut meta = BTreeMap::new();
            for line in parts[1].trim().split('\n') {
                if let Some((key, value)) = line.split_once(':') {
                    meta.insert(key.trim().to_string(), value.trim().to_string());
                }
            }
            return (meta, parts[2].trim().to_string());
        }
    }
    (BTreeMap::new(), text.trim().to_string())
}

fn resolve_prompt_path(raw: &str, effective: &Effective) -> PathBuf {
    let candidate = crate::config::expand_tilde(raw, effective.home_dir.as_deref());
    if candidate.is_absolute() {
        return candidate;
    }
    let mut bases: Vec<PathBuf> = vec![effective.cwd.clone()];
    if let Some(home) = &effective.ocg_home {
        bases.push(home.join("config"));
    }
    for base in bases {
        let resolved = base.join(&candidate);
        if resolved.is_file() {
            return resolved.canonicalize().unwrap_or(resolved);
        }
    }
    effective.cwd.join(&candidate)
}

fn read_prompt_source_string(raw: &str, effective: &Effective, role: &str) -> Result<String> {
    if let Some(embedded) = raw.strip_prefix(EMBEDDED_PROMPT_PREFIX) {
        return embedded_prompt(embedded)
            .map(str::to_string)
            .ok_or_else(|| {
                OcgError::config(format!("prompt file for role '{role}' not found: {raw}"))
            });
    }
    let path = resolve_prompt_path(raw, effective);
    if !path.is_file() {
        return Err(OcgError::config(format!(
            "prompt file for role '{role}' not found: {}",
            path.display()
        )));
    }
    fs::read_to_string(&path).map_err(|error| OcgError::read(&path, error))
}

/// The configured source for a role, if any.
pub fn prompt_source<'a>(effective: &'a Effective, role: &str) -> Result<&'a Value> {
    effective
        .data
        .get("prompts")
        .and_then(|prompts| prompts.get(role))
        .ok_or_else(|| OcgError::config(format!("no prompt configured for role '{role}'")))
}

/// The unmodified OCG prompt for a role, independent of overrides.
pub fn default_prompt_text(effective: &Effective, role: &str) -> Result<String> {
    let raw = effective
        .data
        .get("_prompt_defaults")
        .and_then(|defaults| defaults.get(role))
        .and_then(Value::as_str);
    match raw {
        Some(value) => read_prompt_source_string(value, effective, role),
        None => {
            if let Some(home) = &effective.ocg_home {
                let path = home
                    .join("config")
                    .join("prompts")
                    .join(format!("{role}.md"));
                if path.is_file() {
                    return fs::read_to_string(&path).map_err(|error| OcgError::read(&path, error));
                }
                return Err(OcgError::config(format!(
                    "prompt file for role '{role}' not found: {}",
                    path.display()
                )));
            }
            Err(OcgError::config(format!(
                "no default prompt for role '{role}'"
            )))
        }
    }
}

fn read_prompt_source_text(source: &Value, effective: &Effective, role: &str) -> Result<String> {
    match source {
        Value::String(raw) => read_prompt_source_string(raw, effective, role),
        Value::Object(object) => {
            if let Some(text) = object.get("text") {
                return Ok(value_to_string(text));
            }
            if let Some(path) = object.get("path") {
                return read_prompt_source_string(&value_to_string(path), effective, role);
            }
            if object.contains_key("append") {
                return default_prompt_text(effective, role);
            }
            Err(prompt_shape_error(role))
        }
        _ => Err(prompt_shape_error(role)),
    }
}

fn prompt_shape_error(role: &str) -> OcgError {
    OcgError::config(format!(
        "prompt for role '{role}' must be a path, {{'path': ...}}, {{'text': ...}} or {{'append': [...]}}"
    ))
}

/// Load a role prompt: frontmatter metadata plus the assembled body.
///
/// Supported source forms: a path string, `{"path": ...}`, `{"text": ...}` or
/// `{"append": [...]}` (which extends the unmodified OCG prompt).
pub fn read_prompt(
    effective: &Effective,
    role: &str,
) -> Result<(BTreeMap<String, String>, String)> {
    let source = prompt_source(effective, role)?;
    let text = read_prompt_source_text(source, effective, role)?;
    let (meta, body) = split_frontmatter(&text);
    if body.trim().is_empty() {
        return Err(OcgError::config(format!(
            "prompt for role '{role}' is empty"
        )));
    }

    let mut assembled = body.trim().to_string();
    if let Value::Object(object) = source {
        match object.get("append") {
            None => {}
            Some(Value::Array(entries)) => {
                for entry in entries {
                    let extra_text = read_prompt_source_text(entry, effective, role)?;
                    let (_, extra) = split_frontmatter(&extra_text);
                    if extra.trim().is_empty() {
                        return Err(OcgError::config(format!(
                            "appended prompt for role '{role}' is empty"
                        )));
                    }
                    assembled = format!(
                        "{}{}{}",
                        assembled.trim(),
                        PROMPT_APPEND_SEPARATOR,
                        extra.trim()
                    );
                }
            }
            Some(_) => {
                return Err(OcgError::config(format!(
                    "prompt 'append' for role '{role}' must be a list"
                )))
            }
        }
    }
    Ok((meta, assembled.trim().to_string()))
}

/// The routing table injected into the Lead prompt.
pub fn routing_block(effective: &Effective) -> Result<String> {
    let data = &effective.data;
    let roles = model::role_specs(data).cloned().unwrap_or_default();

    let mut lines = vec![
        "## Configured Worker routing".to_string(),
        String::new(),
        "| Role | Agent | Provider / model |".to_string(),
        "| --- | --- | --- |".to_string(),
    ];
    for (role, spec) in &roles {
        let key = spec
            .get("model")
            .and_then(Value::as_str)
            .ok_or_else(|| OcgError::config(format!("routing role '{role}' is missing 'model'")))?;
        let (provider, _) = model::model_full_id(data, key)?;
        let mut rendered = model::model_label(data, key);
        if let Some(variant) = spec
            .get("variant")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
        {
            rendered = format!("{rendered} ({variant})");
        }
        lines.push(format!(
            "| {} | `{}` | {} / {} |",
            role.to_uppercase(),
            model::worker_agent_id(role),
            model::provider_label(data, &provider),
            rendered
        ));
    }

    let mut fallbacks: Vec<String> = Vec::new();
    for (role, spec) in &roles {
        if let Some(entries) = spec.get("fallback").and_then(Value::as_array) {
            for fallback in entries {
                let key = fallback.get("model").and_then(Value::as_str).unwrap_or("");
                let (provider, _) = model::model_full_id(data, key)?;
                fallbacks.push(format!(
                    "- {}: {} / {}",
                    role.to_uppercase(),
                    model::provider_label(data, &provider),
                    model::model_label(data, key)
                ));
            }
        }
    }
    if !fallbacks.is_empty() {
        lines.push(String::new());
        lines
            .push("Configured fallbacks (use only when the primary model fails or its".to_string());
        lines.push("quota is exhausted; never treat a fallback as the new default):".to_string());
        lines.extend(fallbacks);
    }

    lines.push(String::new());
    lines.push("Worker roles run only on the models listed above. The Lead is".to_string());
    lines.push("provider-agnostic; never substitute the Lead model or any other".to_string());
    lines.push("provider/model unless the user explicitly configures it.".to_string());
    Ok(lines.join("\n"))
}

/// Substitute role/selected-model/routing placeholders in the Lead prompt.
pub fn render_lead_prompt(effective: &Effective, level: &str, template: &str) -> Result<String> {
    let mut text = template.to_string();
    if let Some(roles) = model::role_specs(&effective.data) {
        for role in roles.keys() {
            let mut placeholder = String::from("{{");
            placeholder.push_str(&role.replace('-', "_"));
            placeholder.push_str("}}");
            text = text.replace(&placeholder, &model::worker_agent_id(role));
        }
    }
    text = text.replace("{{model_choice}}", level);
    text = text.replace("{{routing}}", &routing_block(effective)?);
    Ok(text)
}
