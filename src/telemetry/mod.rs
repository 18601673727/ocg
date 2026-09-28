//! Local-only, inspectable telemetry.
//!
//! OCG records a small structured event for `ocg context` and
//! `ocg verify` so the local flows can be measured over time. The design keeps
//! three promises:
//!
//! - **Local only.** Events are appended to
//!   `<project>/.ocg/telemetry/events.jsonl`. There is no remote
//!   service, no upload, no model API and no network path.
//! - **Private by construction.** The schema has no field for prompts, source
//!   code, command strings, command output, headers, environment dumps or
//!   absolute paths. Metadata is redacted at the schema boundary.
//! - **Fail soft.** A telemetry failure warns and never blocks context,
//!   verification or an ordinary launch.
//!
//! The schema is deliberately rich enough to become the input of a future
//! budget controller or capability router, but nothing here makes a decision.

pub mod stats;
pub mod store;
pub mod task;
pub mod tokens;

pub use stats::{Aggregate, TelemetryStats};
pub use store::{EventLog, TelemetryStore, TELEMETRY_DIR, TELEMETRY_FILE};
pub use task::{
    ContextMetrics, Event, LogMetrics, OrchestrationMetrics, Outcome, RepoMetrics,
    VerificationMetrics, EVENT_SCHEMA_VERSION, REDACTED,
};
pub use tokens::{TokenCount, TokenSource};

use crate::error::{OcgError, Result};
use crate::verification::result::{Overall, VerificationReport};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::Path;

/// The `telemetry` policy. `localOnly` is always true: there is no remote mode
/// to opt into.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TelemetryConfig {
    pub enabled: bool,
    pub local_only: bool,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            local_only: true,
        }
    }
}

impl TelemetryConfig {
    /// A config that records nothing. Used when configuration is invalid so a
    /// telemetry problem can never block the command that owns it.
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            local_only: true,
        }
    }

    /// Parse `data["telemetry"]`, falling back to the enabled-by-default
    /// local-only policy when absent.
    pub fn from_config(data: &Value) -> Result<Self> {
        let Some(value) = data.get("telemetry") else {
            return Ok(Self::default());
        };
        if value.is_null() {
            return Ok(Self::default());
        }
        let object = value.as_object().ok_or_else(|| {
            OcgError::config("telemetry must be a JSON object with enabled and localOnly")
        })?;
        let mut config = Self::default();
        if let Some(enabled) = object.get("enabled") {
            config.enabled = enabled
                .as_bool()
                .ok_or_else(|| OcgError::config("telemetry.enabled must be a boolean"))?;
        }
        // `localOnly` is canonical; accept `local_only` as an alias.
        if let Some(local_only) = object.get("localOnly").or_else(|| object.get("local_only")) {
            config.local_only = local_only
                .as_bool()
                .ok_or_else(|| OcgError::config("telemetry.localOnly must be a boolean"))?;
        }
        config.validate_values()?;
        Ok(config)
    }

    /// Collect every policy problem for whole-configuration validation.
    pub fn validate(data: &Value) -> Vec<String> {
        match Self::from_config(data) {
            Ok(_) => Vec::new(),
            Err(error) => vec![error.to_string()],
        }
    }

    fn validate_values(&self) -> Result<()> {
        if !self.local_only {
            return Err(OcgError::config(
                "telemetry.localOnly=false is not supported: there is no remote telemetry mode; set it to true",
            ));
        }
        Ok(())
    }

    /// Apply the optional `OCG_TELEMETRY` override. Only an explicit
    /// on/off value changes the policy; anything else is ignored.
    pub fn with_env_override(mut self, raw: Option<&str>) -> Self {
        if let Some(raw) = raw {
            match raw.trim().to_ascii_lowercase().as_str() {
                "0" | "false" | "off" | "no" | "disabled" => self.enabled = false,
                "1" | "true" | "on" | "yes" | "enabled" => self.enabled = true,
                _ => {}
            }
        }
        self
    }
}

/// Append one event, returning human-readable warnings instead of failing the
/// caller. A disabled store silently records nothing.
pub fn record(root: &Path, config: &TelemetryConfig, event: Event) -> Vec<String> {
    let store = TelemetryStore::new(root, *config);
    match store.append(&event) {
        Ok(_) => Vec::new(),
        Err(error) => vec![format!("telemetry event was not recorded: {error}")],
    }
}

/// Verification attempt accounting derived from a report. No command string or
/// output is copied.
pub fn verification_metrics(report: &VerificationReport) -> VerificationMetrics {
    let attempts = report.results.len();
    let passed = report
        .results
        .iter()
        .filter(|result| result.success)
        .count();
    let failed = attempts - passed;
    let not_run = usize::from(!report.ran);
    VerificationMetrics {
        enabled: report.enabled,
        ran: report.ran,
        attempts,
        passed,
        failed,
        not_run,
        targeted_candidates: report
            .test_proposal
            .as_ref()
            .map(|proposal| proposal.candidates.len())
            .unwrap_or(0),
        stage: Some(report.stage.clone()),
    }
}

/// The deterministic outcome of a report.
pub fn verification_outcome(report: &VerificationReport) -> Outcome {
    match report.overall() {
        Overall::Passed => Outcome::Success,
        Overall::Failed => Outcome::Failure,
        Overall::NotRun => Outcome::Unknown,
    }
}

/// Raw-versus-distilled byte accounting for a report.
///
/// Raw bytes come from the stored raw log files; distilled bytes are the
/// deterministic JSON size of the distilled output. No log content is copied.
pub fn log_metrics(root: &Path, report: &VerificationReport) -> LogMetrics {
    let mut raw_bytes = 0u64;
    let mut distilled_bytes = 0u64;
    for result in &report.results {
        if let Some(relative) = &result.raw_log {
            if let Ok(metadata) = std::fs::metadata(root.join(relative)) {
                raw_bytes = raw_bytes.saturating_add(metadata.len());
            }
        }
        if let Ok(bytes) = serde_json::to_vec(&result.output) {
            distilled_bytes = distilled_bytes.saturating_add(bytes.len() as u64);
        }
    }
    LogMetrics::new(raw_bytes, distilled_bytes)
}
