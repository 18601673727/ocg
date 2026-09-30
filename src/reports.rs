//! Raw persistence of the latest completed root Lead output and the stable
//! read-only latest execution report.
//!
//! This is deliberately not a report *subsystem*: it is one small, fixed
//! artifact. When OpenCode V2 finishes an assistant step in the current
//! Mission execution, the generated adapter reports the raw user-visible text
//! to the bridge and this module writes exactly that text — no headers,
//! timestamps, ids, metadata, summaries or wrapper prose — to
//! `<project>/.ocg/reports/latest-lead-output.md`.
//!
//! Rules:
//!
//! - only a *completed* assistant step from the Mission's current durable
//!   root execution is written (the OpenCode adapter decides completion and
//!   the bridge checks the current execution binding; a streaming partial, an
//!   errored/interrupted message, a stale execution and every worker session
//!   are never reported);
//! - the text is stored byte-verbatim, so a reader can diff it directly;
//! - the file is replaced atomically (temp file + rename) and an interrupted
//!   write never truncates the previous completed output;
//! - every failure is soft: a broken report must never break a session, so
//!   callers report the error and continue.
//!
//! The feature is configured by `reports.latestLeadOutput.enabled` (default
//! true). OpenCode V1 has no event stream for this, so the capture is V2-only;
//! on V1 the switch is inert and no file is produced.

use crate::error::{OcgError, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::{Path, PathBuf};

/// The fixed file name under `.ocg/reports/`.
pub const LATEST_LEAD_OUTPUT_FILE: &str = "latest-lead-output.md";

/// `reports.latestLeadOutput` policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct LatestLeadOutputConfig {
    /// Persist the latest completed root Lead output.
    pub enabled: bool,
}

impl Default for LatestLeadOutputConfig {
    fn default() -> Self {
        // Enabled by default: the artifact is local, ignored and bounded.
        Self { enabled: true }
    }
}

/// `reports` policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct ReportsConfig {
    pub latest_lead_output: LatestLeadOutputConfig,
}

impl ReportsConfig {
    /// Parse `data["reports"]`, falling back to the defaults when absent.
    pub fn from_config(data: &Value) -> Result<Self> {
        let Some(value) = data.get("reports") else {
            return Ok(Self::default());
        };
        if value.is_null() {
            return Ok(Self::default());
        }
        let object = value
            .as_object()
            .ok_or_else(|| OcgError::config("reports must be a JSON object"))?;
        let mut config = Self::default();
        if let Some(value) = object.get("latestLeadOutput") {
            if value.is_null() {
                return Ok(config);
            }
            let entry = value
                .as_object()
                .ok_or_else(|| OcgError::config("reports.latestLeadOutput must be an object"))?;
            if let Some(enabled) = entry.get("enabled") {
                config.latest_lead_output.enabled = enabled.as_bool().ok_or_else(|| {
                    OcgError::config("reports.latestLeadOutput.enabled must be a boolean")
                })?;
            }
        }
        Ok(config)
    }

    /// Collect every policy problem for whole-configuration validation.
    pub fn validate(data: &Value) -> Vec<String> {
        match Self::from_config(data) {
            Ok(_) => Vec::new(),
            Err(error) => vec![error.to_string()],
        }
    }
}

/// `<project>/.ocg/reports`.
pub fn reports_dir(root: &Path) -> PathBuf {
    root.join(".ocg").join("reports")
}

/// `<project>/.ocg/reports/latest-lead-output.md`.
pub fn latest_lead_output_path(root: &Path) -> PathBuf {
    reports_dir(root).join(LATEST_LEAD_OUTPUT_FILE)
}

/// Write the raw text atomically and return the path that was written.
///
/// The text is stored byte-verbatim. The previous file is only replaced once
/// the new content is fully on disk (temp file + rename in the same
/// directory), so a failed or interrupted write cannot leave a truncated
/// report behind.
pub fn write_latest_lead_output(root: &Path, text: &str) -> Result<PathBuf> {
    let dir = reports_dir(root);
    std::fs::create_dir_all(&dir)
        .map_err(|error| OcgError::io(format!("cannot create {}", dir.display()), error))?;
    let target = dir.join(LATEST_LEAD_OUTPUT_FILE);
    let temp = dir.join(format!(
        ".{LATEST_LEAD_OUTPUT_FILE}.tmp-{}",
        std::process::id()
    ));
    std::fs::write(&temp, text).map_err(|error| OcgError::write(&temp, error))?;
    std::fs::rename(&temp, &target).map_err(|error| {
        let _ = std::fs::remove_file(&temp);
        OcgError::write(&target, error)
    })?;
    Ok(target)
}
