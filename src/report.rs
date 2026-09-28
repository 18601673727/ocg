//! Human-readable reporting for `status`, `routing` and `layers`.

use crate::config::Effective;
use crate::error::Result;
use crate::model;
use crate::observability;
use serde_json::Value;
use std::path::{Path, PathBuf};

pub fn routing_text(effective: &Effective) -> Result<String> {
    let mut lines = vec!["Worker router (role -> model):".to_string(), String::new()];
    for (role, agent, provider, full) in model::routing_rows(&effective.data)? {
        lines.push(format!("  {role:<13} {agent:<18} {provider:<24} {full}"));
    }
    if let Some(small) = effective
        .data
        .get("routing")
        .and_then(|routing| routing.get("small_model"))
        .and_then(Value::as_str)
    {
        let (provider, full) = model::model_full_id(&effective.data, small)?;
        lines.push(String::new());
        lines.push(format!(
            "  small_model   {} -> {}",
            model::provider_label(&effective.data, &provider),
            full
        ));
    }
    Ok(lines.join("\n"))
}

fn layer_mark(is_applied: bool) -> &'static str {
    if is_applied {
        "applied"
    } else {
        "not found"
    }
}

pub fn status_text(effective: &Effective, level: &str) -> Result<String> {
    let profile = crate::profile::Profile::from_ocg_config(&effective.data)?;
    let workers = model::role_specs(&effective.data)
        .map(|roles| roles.len())
        .unwrap_or(0);
    let selected = if level.is_empty() { "(none)" } else { level };
    let origin = match &profile.origin {
        crate::profile::Origin::New => "New".to_string(),
        crate::profile::Origin::Imported { source, scope, .. } => {
            format!("Imported {scope} from {source}")
        }
    };

    // A model that declares no variant is reported as `provider-default`; the
    // value is never invented.
    let lead = match model::lead_contract(&effective.data, level) {
        Ok(contract) => format!(
            "  lead model    {} (variant {})",
            contract.full_model_id(),
            contract.variant.as_deref().unwrap_or("provider-default")
        ),
        Err(error) => format!("  lead model    unavailable: {error}"),
    };
    let mut lines = vec![
        "OCG".to_string(),
        String::new(),
        format!("  Profile       {origin}"),
        format!("  model choice  {selected}"),
        format!("  default agent {}", model::lead_agent_id(level)),
        lead,
        format!("  provider count {}", profile.providers.len()),
        format!("  model count   {}", profile.models.len()),
        format!("  runnable models {}", profile.runnable_models().count()),
        format!(
            "  placeholder-only {}",
            profile.runnable_models().next().is_none()
        ),
        format!("  workers       {workers}"),
        format!("  cwd           {}", effective.cwd.display()),
        String::new(),
        "Config layers:".to_string(),
    ];

    let applied: Vec<&PathBuf> = effective.applied.iter().map(|(_, path)| path).collect();
    let (name, path) = ("global", &effective.user_path);
    let marked = applied.contains(&path);
    lines.push(format!(
        "  {name:<8} {}  [{}]",
        path.display(),
        layer_mark(marked)
    ));

    lines.push(String::new());
    lines.extend(
        effective
            .diagnostics
            .iter()
            .map(|item| format!("  migration: {item}")),
    );
    lines.push(routing_text(effective)?);
    Ok(lines.join("\n"))
}

/// `layers` output. `env_trace` is the environment-provided trace path.
pub fn layers_text(effective: &Effective, env_trace: Option<&Path>) -> String {
    let home = effective
        .ocg_home
        .as_ref()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "(embedded)".to_string());
    let mut lines = vec![format!("OCG home: {home}")];
    let (name, path) = ("global", &effective.user_path);
    let found = if path.is_file() { "found" } else { "not found" };
    lines.push(format!("{name:<8} {}  [{found}]", path.display()));
    let trace = observability::trace_path(effective, env_trace)
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "(disabled)".to_string());
    lines.push(format!("{:<8} {trace}", "trace"));
    lines.join("\n")
}
