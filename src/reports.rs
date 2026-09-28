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

use crate::orchestration::replay::SnapshotService;
use crate::orchestration::substrate::{MissionId, RunState, SubstrateRepository};

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
        let mut repository = SubstrateRepository::open(root)?;
        let missions = repository.list_missions()?;
        if let Some((mission_id, _, _mission_state, completed_at)) = missions.into_iter().next() {
            let mission = MissionId::new(&mission_id)?;
            let state = repository.load(&mission)?.ok_or_else(|| {
                OcgError::config("latest canonical Mission disappeared while reading")
            })?;
            let run_entry = state
                .runs
                .iter_enumerated()
                .max_by_key(|(_, run)| (run.created_at, run.node_id.0))
                .map(|(id, run)| (id, run.clone()));
            if let Some((run_id, run)) = run_entry {
                let replacements = state
                    .runs
                    .iter()
                    .filter(|run| matches!(run.state, RunState::Fenced | RunState::Superseded))
                    .count();
                let node = state.work_nodes.get(run.node_id);
                let output = run.result.clone().or_else(|| {
                    state
                        .runs
                        .iter()
                        .rev()
                        .find_map(|candidate| candidate.result.clone())
                });
                let state_label = match run.state {
                    RunState::Active => "active".to_string(),
                    RunState::Completed => "completed".to_string(),
                    RunState::Failed => "failed".to_string(),
                    RunState::Cancelled => "cancelled".to_string(),
                    RunState::Superseded => "superseded".to_string(),
                    RunState::Fenced => "fenced".to_string(),
                };
                let usage = usage_from_replay(root, &mission_id, &run.runtime_execution_id);
                return Ok(Some(LatestReport {
                    source: "canonical substrate".to_string(),
                    mission_id: Some(mission_id),
                    work_node_id: Some(run.node_id.0),
                    work_identity: node.map(|node| node.payload.clone()),
                    run_id: Some(run_id.0),
                    state: state_label,
                    executor: Some(run.contract().executor.clone()),
                    provider: provider_from_executor(run.contract().executor.as_str()),
                    model: Some(run.contract().model.clone()),
                    role: Some(run.contract().role.clone()),
                    started_at: Some(run.created_at),
                    finished_at: run.finished_at.or(completed_at),
                    last_activity_at: run.finished_at.or(Some(run.created_at)),
                    output,
                    failure_reason: (run.state == RunState::Failed)
                        .then(|| run.result.clone())
                        .flatten(),
                    attempts: state.runs.len(),
                    replacements,
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

fn usage_from_replay(
    root: &Path,
    mission_id: &str,
    runtime_execution_id: &Option<String>,
) -> ReportUsage {
    let Ok(service) = SnapshotService::open(root) else {
        return ReportUsage::default();
    };
    let Ok(snapshot) = service.snapshot() else {
        return ReportUsage::default();
    };
    let dispatch = snapshot
        .dispatches
        .values()
        .filter(|dispatch| {
            dispatch.mission_id == mission_id
                && runtime_execution_id
                    .as_deref()
                    .is_none_or(|id| dispatch.execution_id == *id)
        })
        .max_by_key(|dispatch| (dispatch.updated_at, dispatch.id.as_str().to_string()));
    let Some(dispatch) = dispatch else {
        return ReportUsage::default();
    };
    let Some(usage) = &dispatch.usage else {
        return ReportUsage::default();
    };
    ReportUsage {
        input_tokens: usage.input_tokens,
        output_tokens: usage.output_tokens,
        cached_tokens: usage.cache_read_tokens,
        reasoning_tokens: usage.reasoning_tokens,
        provenance: usage.provenance.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn absent_reports_config_is_enabled_by_default() {
        let config = ReportsConfig::from_config(&json!({})).unwrap();
        assert!(config.latest_lead_output.enabled);
        assert_eq!(config, ReportsConfig::default());
    }

    #[test]
    fn the_switch_can_disable_the_capture() {
        let config = ReportsConfig::from_config(&json!({
            "reports": {"latestLeadOutput": {"enabled": false}}
        }))
        .unwrap();
        assert!(!config.latest_lead_output.enabled);
    }

    #[test]
    fn malformed_reports_config_is_rejected() {
        for bad in [
            json!({"reports": "on"}),
            json!({"reports": {"latestLeadOutput": true}}),
            json!({"reports": {"latestLeadOutput": {"enabled": "yes"}}}),
        ] {
            assert!(ReportsConfig::from_config(&bad).is_err(), "{bad}");
            assert!(!ReportsConfig::validate(&bad).is_empty(), "{bad}");
        }
        assert!(ReportsConfig::validate(&json!({})).is_empty());
    }

    #[test]
    fn the_path_is_fixed_under_the_ignored_state_directory() {
        let dir = tempfile::tempdir().unwrap();
        let path = latest_lead_output_path(dir.path());
        assert_eq!(
            path,
            dir.path()
                .join(".ocg")
                .join("reports")
                .join("latest-lead-output.md")
        );
    }

    #[test]
    fn the_text_is_stored_byte_verbatim() {
        let dir = tempfile::tempdir().unwrap();
        let text = "# Title\n\n- item\n\n```rust\nfn main() {}\n```\n";
        let path = write_latest_lead_output(dir.path(), text).unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), text);
        assert_eq!(std::fs::read(&path).unwrap(), text.as_bytes());
    }

    #[test]
    fn a_second_write_replaces_the_first_and_leaves_no_temp_file() {
        let dir = tempfile::tempdir().unwrap();
        write_latest_lead_output(dir.path(), "first\n").unwrap();
        let path = write_latest_lead_output(dir.path(), "second\n").unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "second\n");
        let entries: Vec<String> = std::fs::read_dir(reports_dir(dir.path()))
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(entries, vec![LATEST_LEAD_OUTPUT_FILE.to_string()]);
    }

    #[test]
    fn a_blocked_report_path_fails_without_touching_the_previous_output() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_latest_lead_output(dir.path(), "keep me\n").unwrap();
        // Replace the reports directory with a file: the next write must fail
        // soft and the previous output must survive byte-for-byte.
        std::fs::remove_file(&path).unwrap();
        std::fs::remove_dir(reports_dir(dir.path())).unwrap();
        std::fs::write(reports_dir(dir.path()), "not a directory").unwrap();
        assert!(write_latest_lead_output(dir.path(), "must not appear\n").is_err());
        assert_eq!(
            std::fs::read_to_string(reports_dir(dir.path())).unwrap(),
            "not a directory"
        );
    }

    fn live_repository(root: &Path, mission_id: &str) -> (SubstrateRepository, MissionId) {
        let mut repository = SubstrateRepository::open(root).unwrap();
        let mission = MissionId::new(mission_id).unwrap();
        repository
            .create_live_mission(
                &mission,
                "Investigate the latest execution",
                crate::orchestration::substrate::RunContract {
                    executor: "provider/model".to_string(),
                    model: "model".to_string(),
                    role: "worker".to_string(),
                },
                "runtime-1",
                100,
            )
            .unwrap();
        (repository, mission)
    }

    #[test]
    fn latest_report_exposes_a_successful_run_and_unknown_usage() {
        let dir = tempfile::tempdir().unwrap();
        let (mut repository, mission) = live_repository(dir.path(), "mission-success");
        repository
            .finish_run(
                &mission,
                crate::orchestration::substrate::WorkNodeId(0),
                crate::orchestration::substrate::RunId(0),
                RunState::Completed,
                Some("completed output"),
                120,
            )
            .unwrap();

        let report = latest_report(dir.path()).unwrap().unwrap();
        assert_eq!(report.state, "completed");
        assert_eq!(report.output.as_deref(), Some("completed output"));
        assert_eq!(report.provider.as_deref(), Some("provider"));
        assert_eq!(report.usage.input_tokens, None);
        assert_eq!(report.usage.provenance, "unknown");
    }

    #[test]
    fn latest_report_keeps_failure_state_and_reason() {
        let dir = tempfile::tempdir().unwrap();
        let (mut repository, mission) = live_repository(dir.path(), "mission-failure");
        repository
            .finish_run(
                &mission,
                crate::orchestration::substrate::WorkNodeId(0),
                crate::orchestration::substrate::RunId(0),
                RunState::Failed,
                Some("provider timed out"),
                130,
            )
            .unwrap();

        let report = latest_report(dir.path()).unwrap().unwrap();
        assert_eq!(report.state, "failed");
        assert_eq!(report.failure_reason.as_deref(), Some("provider timed out"));
    }

    #[test]
    fn latest_report_exposes_active_run() {
        let dir = tempfile::tempdir().unwrap();
        let _ = live_repository(dir.path(), "mission-active");

        let report = latest_report(dir.path()).unwrap().unwrap();
        assert_eq!(report.state, "active");
        assert_eq!(report.finished_at, None);
        assert_eq!(report.last_activity_at, Some(100));
    }

    #[test]
    fn latest_report_counts_replacements_and_selects_the_new_attempt() {
        let dir = tempfile::tempdir().unwrap();
        let (mut repository, mission) = live_repository(dir.path(), "mission-replaced");
        repository
            .replace_bound_run(
                &mission,
                crate::orchestration::substrate::WorkNodeId(0),
                crate::orchestration::substrate::RunId(0),
                crate::orchestration::substrate::RunContract {
                    executor: "provider/new-model".to_string(),
                    model: "new-model".to_string(),
                    role: "worker".to_string(),
                },
                "runtime-2",
                150,
            )
            .unwrap();

        let report = latest_report(dir.path()).unwrap().unwrap();
        assert_eq!(report.run_id, Some(1));
        assert_eq!(report.state, "active");
        assert_eq!(report.attempts, 2);
        assert_eq!(report.replacements, 1);
        assert_eq!(report.model.as_deref(), Some("new-model"));
    }
}
