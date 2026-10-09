//! Human-readable reporting for `status`, `routing` and `layers`.

use crate::config::Effective;
use crate::error::Result;
use crate::model;
use crate::observability;
use serde_json::Value;

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

pub fn status_text(effective: &Effective, level: &str) -> Result<String> {
    let profile = crate::profile::Profile::from_ocg_config(&effective.data)?;
    let workers = model::role_specs(&effective.data)
        .map(|roles| roles.len())
        .unwrap_or(0);
    let selected = if level.is_empty() { "(none)" } else { level };
    let origin = match &profile.origin {
        crate::profile::Origin::New => "New".to_string(),
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
        format!("  workers       {workers}"),
        format!("  cwd           {}", effective.cwd.display()),
        String::new(),
        "Configuration:".to_string(),
        format!("  database      {}", effective.store.display()),
        format!("  revision      {}", effective.revision),
        String::new(),
    ];
    lines.push(routing_text(effective)?);
    Ok(lines.join("\n"))
}

/// `layers` output: the one configuration authority and its active revision.
pub fn layers_text(effective: &Effective) -> String {
    let trace = observability::trace_path(effective)
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "(disabled)".to_string());
    [
        "defaults compiled into this binary".to_string(),
        format!(
            "{:<8} {}  [revision {}]",
            "core",
            effective.store.display(),
            effective.revision
        ),
        format!("{:<8} {trace}", "trace"),
    ]
    .join("\n")
}
