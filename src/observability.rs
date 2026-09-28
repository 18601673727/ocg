//! Opt-in, local-only routing trace. No prompts, no source, no network.

use crate::config::Effective;
use crate::json::is_truthy;
use crate::model;
use serde_json::{Map, Value};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

/// Resolve the trace path, or `None` when observability is disabled.
pub fn trace_path(effective: &Effective, env_trace: Option<&Path>) -> Option<PathBuf> {
    let config = effective
        .data
        .get("observability")
        .and_then(Value::as_object);
    let enabled = config
        .and_then(|config| config.get("enabled"))
        .map(is_truthy)
        .unwrap_or(false);
    if !enabled {
        return None;
    }
    if let Some(path) = env_trace {
        return Some(crate::config::expand_tilde(
            &path.to_string_lossy(),
            effective.home_dir.as_deref(),
        ));
    }
    if let Some(raw) = config
        .and_then(|config| config.get("path"))
        .and_then(Value::as_str)
    {
        return Some(crate::config::expand_tilde(
            raw,
            effective.home_dir.as_deref(),
        ));
    }
    let home = effective.home_dir.clone().unwrap_or_default();
    Some(
        home.join(".local")
            .join("state")
            .join("ocg")
            .join("events.jsonl"),
    )
}

/// Append one routing event. Errors are swallowed: tracing is best-effort and
/// must never block a launch.
pub fn record_event(
    effective: &Effective,
    event: &str,
    level: &str,
    env_trace: Option<&Path>,
) -> Option<PathBuf> {
    let path = trace_path(effective, env_trace)?;
    let data = &effective.data;
    crate::profile::Profile::from_ocg_config(data)
        .ok()?
        .select(Some(level))
        .ok()?;
    let (_, lead) = model::model_full_id(data, level).ok()?;

    let mut routing = Map::new();
    if let Some(roles) = model::role_specs(data) {
        for (role, spec) in roles {
            if let Some(key) = spec.get("model").and_then(Value::as_str) {
                if let Ok((_, full)) = model::model_full_id(data, key) {
                    routing.insert(role.clone(), Value::String(full));
                }
            }
        }
    }

    let record = serde_json::json!({
        "ts": now_iso(),
        "event": event,
        "model_choice": level,
        "default_agent": model::lead_agent_id(level),
        "lead": lead,
        "routing": Value::Object(routing),
    });

    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    let mut line = record.to_string();
    line.push('\n');
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .ok()?;
    file.write_all(line.as_bytes()).ok()?;
    Some(path)
}

/// UTC timestamp in the same shape the Python builder emitted.
pub fn now_iso() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let seconds = duration.as_secs() as i64;
    let days = seconds.div_euclid(86_400);
    let remainder = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = remainder / 3600;
    let minute = (remainder % 3600) / 60;
    let second = remainder % 60;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}+00:00")
}

/// Howard Hinnant's `civil_from_days` algorithm, in UTC.
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let day_of_era = z - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_pivot = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_pivot + 2) / 5 + 1;
    let month = if month_pivot < 10 {
        month_pivot + 3
    } else {
        month_pivot - 9
    };
    let year = if month <= 2 { year + 1 } else { year };
    (year, month, day)
}
