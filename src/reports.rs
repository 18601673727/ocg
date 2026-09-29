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

use crate::orchestration::domain::DomainRepository;

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

/// A token field keeps both its value and its evidence. A missing value is
/// deliberately represented as `None`, rather than as zero.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReportUsage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub cached_tokens: Option<u64>,
    pub reasoning_tokens: Option<u64>,
    pub provenance: String,
}

impl Default for ReportUsage {
    fn default() -> Self {
        Self {
            input_tokens: None,
            output_tokens: None,
            cached_tokens: None,
            reasoning_tokens: None,
            provenance: "unknown".to_string(),
        }
    }
}

/// The latest durable execution, including incomplete and replaced attempts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LatestReport {
    pub source: String,
    pub mission_id: Option<String>,
    pub work_node_id: Option<usize>,
    pub work_identity: Option<String>,
    pub run_id: Option<usize>,
    pub state: String,
    pub executor: Option<String>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub role: Option<String>,
    pub started_at: Option<i64>,
    pub finished_at: Option<i64>,
    pub last_activity_at: Option<i64>,
    pub output: Option<String>,
    pub failure_reason: Option<String>,
    pub attempts: usize,
    pub replacements: usize,
    pub usage: ReportUsage,
}

impl LatestReport {
    /// Render a stable diagnostic view for internal callers and future Message
    /// projections. This is intentionally not a CLI contract.
    pub fn render(&self) -> String {
        let value = |value: Option<&str>| value.unwrap_or("Unknown").to_string();
        let number = |value: Option<i64>| {
            value
                .map(|v| v.to_string())
                .unwrap_or_else(|| "Unknown".to_string())
        };
        let usage = &self.usage;
        let token = |value: Option<u64>| {
            value
                .map(|v| v.to_string())
                .unwrap_or_else(|| "Unknown".to_string())
        };
        let mut out = String::new();
        out.push_str("Latest execution report\n\n");
        out.push_str(&format!("  source          {}\n", self.source));
        out.push_str(&format!(
            "  mission         {}\n",
            value(self.mission_id.as_deref())
        ));
        out.push_str(&format!(
            "  WorkNode        {} ({})\n",
            self.work_node_id
                .map(|v| v.to_string())
                .unwrap_or_else(|| "Unknown".to_string()),
            value(self.work_identity.as_deref())
        ));
        out.push_str(&format!(
            "  Run             {}\n",
            self.run_id
                .map(|v| v.to_string())
                .as_deref()
                .unwrap_or("Unknown")
        ));
        out.push_str(&format!("  state           {}\n", self.state));
        out.push_str(&format!(
            "  executor        {}\n",
            value(self.executor.as_deref())
        ));
        out.push_str(&format!(
            "  provider        {}\n",
            value(self.provider.as_deref())
        ));
        out.push_str(&format!(
            "  model           {}\n",
            value(self.model.as_deref())
        ));
        out.push_str(&format!(
            "  role            {}\n",
            value(self.role.as_deref())
        ));
        out.push_str(&format!("  started         {}\n", number(self.started_at)));
        out.push_str(&format!("  finished        {}\n", number(self.finished_at)));
        out.push_str(&format!(
            "  last activity   {}\n",
            number(self.last_activity_at)
        ));
        out.push_str(&format!("  attempts        {}\n", self.attempts));
        out.push_str(&format!("  replacements    {}\n", self.replacements));
        out.push_str(&format!(
            "  usage           input={} output={} cached={} reasoning={} ({})\n",
            token(usage.input_tokens),
            token(usage.output_tokens),
            token(usage.cached_tokens),
            token(usage.reasoning_tokens),
            usage.provenance
        ));
        if let Some(reason) = &self.failure_reason {
            out.push_str(&format!("  failure         {reason}\n"));
        }
        out.push_str("\noutput:\n");
        out.push_str(self.output.as_deref().unwrap_or("Unknown\n"));
        if !out.ends_with('\n') {
            out.push('\n');
        }
        out
    }
}

/// Read the latest canonical execution without contacting OpenCode or any
/// provider. A missing optional store is a normal Unknown result.
pub fn latest_report(root: &Path) -> Result<Option<LatestReport>> {
    let state_dir = crate::orchestration::state::state_dir(root);
    let substrate_path = state_dir.join("substrate.sqlite3");
    let report_path = latest_lead_output_path(root);
    if substrate_path.is_file() {
        let repository = DomainRepository::open(root)?;
        let project = repository.ensure_project(root)?;
        if let Some(job) = repository
            .jobs(&project.id)?
            .into_iter()
            .max_by_key(|job| (job.updated_at, job.id.clone()))
        {
            let attempts = repository.attempts_for_job(&job.id)?;
            if let Some(attempt) = attempts.last() {
                let calls = repository.calls_for_attempt(&attempt.id)?;
                let executor = repository.executor_for_attempt(&attempt.id)?;
                let latest_call = calls.last();
                let output = calls.iter().rev().find_map(|call| {
                    (call.state == "completed")
                        .then(|| call.response.clone())
                        .flatten()
                });
                let usage = latest_call
                    .and_then(|call| call.response.as_deref())
                    .and_then(|response| serde_json::from_str::<Value>(response).ok())
                    .map(|value| ReportUsage {
                        provenance: "canonical_call".to_string(),
                        input_tokens: value
                            .get("usage")
                            .and_then(|usage| usage.get("input_tokens"))
                            .and_then(Value::as_u64),
                        output_tokens: value
                            .get("usage")
                            .and_then(|usage| usage.get("output_tokens"))
                            .and_then(Value::as_u64),
                        cached_tokens: value
                            .get("usage")
                            .and_then(|usage| usage.get("cache_read_tokens"))
                            .and_then(Value::as_u64),
                        reasoning_tokens: value
                            .get("usage")
                            .and_then(|usage| usage.get("reasoning_tokens"))
                            .and_then(Value::as_u64),
                    })
                    .unwrap_or_default();
                return Ok(Some(LatestReport {
                    source: "canonical Project/Job/Attempt".to_string(),
                    mission_id: Some(job.id.clone()),
                    work_node_id: None,
                    work_identity: Some(job.payload.clone()),
                    run_id: None,
                    state: format!("{}", attempt.state),
                    executor: executor.as_ref().map(|executor| executor.kind.clone()),
                    provider: executor
                        .as_ref()
                        .and_then(|executor| provider_from_executor(&executor.kind)),
                    model: executor.as_ref().and_then(|executor| {
                        executor
                            .kind
                            .split_once('/')
                            .map(|(_, model)| model.to_string())
                    }),
                    role: None,
                    started_at: Some(attempt.created_at),
                    finished_at: attempt.finished_at,
                    last_activity_at: latest_call
                        .map(|call| call.finished_at.unwrap_or(call.created_at))
                        .or(Some(attempt.created_at)),
                    output,
                    failure_reason: latest_call
                        .filter(|call| call.state == "failed")
                        .and_then(|call| call.response.clone()),
                    attempts: attempts.len(),
                    replacements: attempts.len().saturating_sub(1),
                    usage,
                }));
            }
        }
    }

    // This is a compatibility capture, not a success inference: the writer's
    // contract only writes this artifact after a completed root step.
    if report_path.is_file() {
        let output = std::fs::read_to_string(&report_path)
            .map_err(|error| OcgError::io("cannot read latest execution output", error))?;
        return Ok(Some(LatestReport {
            source: "legacy completed-output capture".to_string(),
            mission_id: None,
            work_node_id: None,
            work_identity: None,
            run_id: None,
            state: "completed (legacy capture)".to_string(),
            executor: None,
            provider: None,
            model: None,
            role: None,
            started_at: None,
            finished_at: None,
            last_activity_at: None,
            output: Some(output),
            failure_reason: None,
            attempts: 1,
            replacements: 0,
            usage: ReportUsage::default(),
        }));
    }
    Ok(None)
}

fn provider_from_executor(executor: &str) -> Option<String> {
    executor
        .split_once('/')
        .map(|(provider, _)| provider.to_string())
}
