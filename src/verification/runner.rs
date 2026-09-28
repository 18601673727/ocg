//! Verification execution: run a stage's trusted commands in order.

use crate::clock::Clock;
use crate::error::Result;
use crate::process::CaptureRunner;
use crate::verification::config::VerificationConfig;
use crate::verification::distill::distill;
use crate::verification::logs::{LogStore, RawLogInput};
use crate::verification::result::{VerificationReport, VerificationResult};
use crate::verification::select::TestProposal;
use crate::verification::{ENGINE_VERSION, REPORT_SCHEMA_VERSION};
use std::path::Path;

/// Everything a verification run needs. All process construction is delegated
/// to the injected [`CaptureRunner`], so tests never spawn anything.
pub struct VerifyRequest<'a> {
    pub root: &'a Path,
    pub config: &'a VerificationConfig,
    pub stage: String,
    pub runner: &'a dyn CaptureRunner,
    pub clock: &'a dyn Clock,
    /// An advisory proposal attached to the report; never executed here.
    pub test_proposal: Option<TestProposal>,
}

/// Run one stage and return a structured report. A failure never aborts the
/// reporting path: a command that cannot even spawn becomes a failed result.
pub fn execute(request: &VerifyRequest<'_>) -> Result<VerificationReport> {
    let config = request.config;
    let now = request.clock.now_unix();
    let mut notes = Vec::new();

    if !config.enabled {
        notes.push(
            "verification is disabled (verification.enabled=false); no command was run".to_string(),
        );
        return Ok(VerificationReport {
            schema_version: REPORT_SCHEMA_VERSION,
            engine_version: ENGINE_VERSION.to_string(),
            stage: request.stage.clone(),
            enabled: false,
            ran: false,
            stopped_early: false,
            results: Vec::new(),
            test_proposal: request.test_proposal.clone(),
            created_at: now,
            notes,
        });
    }

    let stage = config.stage(&request.stage)?;
    if stage.commands.is_empty() {
        notes.push(format!(
            "no commands are configured for the '{}' stage; nothing was run",
            request.stage
        ));
        return Ok(VerificationReport {
            schema_version: REPORT_SCHEMA_VERSION,
            engine_version: ENGINE_VERSION.to_string(),
            stage: request.stage.clone(),
            enabled: true,
            ran: false,
            stopped_early: false,
            results: Vec::new(),
            test_proposal: request.test_proposal.clone(),
            created_at: now,
            notes,
        });
    }

    let store = LogStore::new(request.root);
    let mut results: Vec<VerificationResult> = Vec::new();
    let mut stopped_early = false;

    for command in stage.commands.iter() {
        let captured = match request.runner.run(
            &command.program,
            &command.args,
            request.root,
            config.max_raw_log_bytes,
        ) {
            Ok(captured) => captured,
            Err(error) => {
                let message = format!("{} could not be run: {error}", command.display());
                notes.push(message.clone());
                let mut output = distill("", "", false);
                output.summary = vec![message];
                results.push(VerificationResult {
                    command: command.program.clone(),
                    args: command.args.clone(),
                    stage: request.stage.clone(),
                    exit: crate::process::ProcessExit::Unknown,
                    success: false,
                    duration_ms: 0,
                    output,
                    raw_log: None,
                    raw_truncated: false,
                    failed_tests: Vec::new(),
                    source_locations: Vec::new(),
                });
                if config.stop_on_failure {
                    stopped_early = true;
                    break;
                }
                continue;
            }
        };

        let stdout = captured.stdout_lossy();
        let stderr = captured.stderr_lossy();
        let mut output = distill(&stdout, &stderr, captured.success);
        if captured.truncated() {
            output.truncated = true;
            output.notes.push(
                "captured output was truncated: only the retained prefix was distilled; the command completed and its exit status is authoritative"
                    .to_string(),
            );
        }
        let raw = store.store(&RawLogInput {
            created_at: now,
            label: &command.display(),
            stdout: &captured.stdout,
            stderr: &captured.stderr,
            stdout_truncated: captured.stdout_truncated,
            stderr_truncated: captured.stderr_truncated,
            max_bytes: config.max_raw_log_bytes,
        })?;
        // Never let the total-size cap delete the log the report references.
        let keep = Path::new(&raw.path)
            .file_name()
            .map(|name| name.to_string_lossy().into_owned());
        if let Err(error) = store.prune(config.max_log_storage_bytes, keep.as_deref()) {
            notes.push(format!("raw log storage could not be pruned: {error}"));
        }

        results.push(VerificationResult {
            command: command.program.clone(),
            args: command.args.clone(),
            stage: request.stage.clone(),
            exit: captured.exit,
            success: captured.success,
            duration_ms: captured.duration_ms,
            failed_tests: output.failed_tests.clone(),
            source_locations: output.source_locations.clone(),
            output,
            raw_log: Some(raw.path),
            raw_truncated: raw.truncated || captured.truncated(),
        });

        if !captured.success && config.stop_on_failure {
            stopped_early = true;
            break;
        }
    }

    Ok(VerificationReport {
        schema_version: REPORT_SCHEMA_VERSION,
        engine_version: ENGINE_VERSION.to_string(),
        stage: request.stage.clone(),
        enabled: true,
        ran: true,
        stopped_early,
        results,
        test_proposal: request.test_proposal.clone(),
        created_at: now,
        notes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::FixedClock;
    use crate::process::{CapturedOutput, FakeCaptureRunner, ProcessExit};
    use serde_json::json;

    fn config() -> VerificationConfig {
        VerificationConfig::from_config(&json!({
            "verification": {
                "stages": {
                    "fast": {"commands": ["cargo fmt --check"]},
                    "normal": {"commands": ["cargo check", "cargo test"]}
                }
            }
        }))
        .unwrap()
    }

    fn request<'a>(
        root: &'a Path,
        config: &'a VerificationConfig,
        runner: &'a FakeCaptureRunner,
        clock: &'a FixedClock,
    ) -> VerifyRequest<'a> {
        VerifyRequest {
            root,
            config,
            stage: "normal".to_string(),
            runner,
            clock,
            test_proposal: None,
        }
    }

    #[test]
    fn disabled_config_runs_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = config();
        config.enabled = false;
        let runner = FakeCaptureRunner::new();
        let clock = FixedClock::new(5);
        let report = execute(&request(dir.path(), &config, &runner, &clock)).unwrap();
        assert!(!report.enabled);
        assert!(!report.ran);
        assert!(report.results.is_empty());
        assert!(runner.calls().is_empty());
        assert!(report.notes[0].contains("disabled"));
    }

    #[test]
    fn empty_stage_runs_nothing_and_reports_fallback() {
        let dir = tempfile::tempdir().unwrap();
        let config = config();
        let runner = FakeCaptureRunner::new();
        let clock = FixedClock::new(5);
        let mut request = request(dir.path(), &config, &runner, &clock);
        request.stage = "full".to_string();
        let report = execute(&request).unwrap();
        assert!(!report.ran);
        assert!(runner.calls().is_empty());
        assert!(report.notes[0].contains("no commands"));
    }

    #[test]
    fn runs_commands_in_order_and_writes_raw_logs() {
        let dir = tempfile::tempdir().unwrap();
        let config = config();
        let runner = FakeCaptureRunner::new()
            .with_success("cargo", &["check"], "Checking demo\n")
            .with_success("cargo", &["test"], "test result: ok. 2 passed; 0 failed\n");
        let clock = FixedClock::new(5);
        let report = execute(&request(dir.path(), &config, &runner, &clock)).unwrap();
        assert!(report.ran);
        assert!(report.passed());
        assert_eq!(report.results.len(), 2);
        assert_eq!(
            runner.calls(),
            vec![
                ("cargo".to_string(), vec!["check".to_string()]),
                ("cargo".to_string(), vec!["test".to_string()]),
            ]
        );
        assert!(report.results[0].raw_log.is_some());
        let logs = dir.path().join(".ocg/logs");
        assert_eq!(std::fs::read_dir(logs).unwrap().count(), 2);
    }

    #[test]
    fn stops_on_failure_by_default() {
        let dir = tempfile::tempdir().unwrap();
        let config = config();
        let runner = FakeCaptureRunner::new()
            .with_failure("cargo", &["check"], 101, "error[E0308]: bad\n")
            .with_success("cargo", &["test"], "never runs");
        let clock = FixedClock::new(5);
        let report = execute(&request(dir.path(), &config, &runner, &clock)).unwrap();
        assert!(report.failed());
        assert!(report.stopped_early);
        assert_eq!(report.results.len(), 1);
        assert_eq!(runner.calls().len(), 1);
    }

    #[test]
    fn continues_after_failure_when_stop_on_failure_is_off() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = config();
        config.stop_on_failure = false;
        let runner = FakeCaptureRunner::new()
            .with_failure("cargo", &["check"], 1, "error: bad\n")
            .with_success("cargo", &["test"], "ok");
        let clock = FixedClock::new(5);
        let report = execute(&request(dir.path(), &config, &runner, &clock)).unwrap();
        assert!(report.failed());
        assert!(!report.stopped_early);
        assert_eq!(report.results.len(), 2);
    }

    #[test]
    fn unspawnable_command_becomes_a_failed_result() {
        let dir = tempfile::tempdir().unwrap();
        let config = config();
        let runner = FakeCaptureRunner::new();
        let clock = FixedClock::new(5);
        let report = execute(&request(dir.path(), &config, &runner, &clock)).unwrap();
        assert!(report.failed());
        assert_eq!(report.results[0].exit, ProcessExit::Unknown);
        assert!(!report.results[0].success);
    }

    #[test]
    fn preserves_signal_exit_and_truncation() {
        let dir = tempfile::tempdir().unwrap();
        let config = config();
        let output = CapturedOutput {
            exit: ProcessExit::Signal(9),
            success: false,
            stdout: b"partial".to_vec(),
            stderr: Vec::new(),
            stdout_truncated: true,
            stderr_truncated: false,
            duration_ms: 42,
        };
        let runner = FakeCaptureRunner::new().with_response("cargo", &["check"], output);
        let clock = FixedClock::new(5);
        let report = execute(&request(dir.path(), &config, &runner, &clock)).unwrap();
        assert_eq!(report.results[0].exit, ProcessExit::Signal(9));
        assert_eq!(report.results[0].duration_ms, 42);
        assert!(report.results[0].raw_truncated);
        // The command's own exit stays authoritative and the truncation is
        // explicit rather than looking like a false failure.
        assert!(!report.results[0].success);
        assert!(report.results[0].output.truncated);
        assert!(report.results[0]
            .output
            .notes
            .iter()
            .any(|note| note.contains("only the retained prefix was distilled")));
    }

    #[test]
    fn truncated_but_successful_command_is_not_a_failure() {
        let dir = tempfile::tempdir().unwrap();
        let config = config();
        let output = CapturedOutput {
            exit: ProcessExit::Code(0),
            success: true,
            stdout: b"verbose".to_vec(),
            stderr: Vec::new(),
            stdout_truncated: true,
            stderr_truncated: false,
            duration_ms: 3,
        };
        let runner = FakeCaptureRunner::new()
            .with_response("cargo", &["check"], output)
            .with_success("cargo", &["test"], "ok");
        let clock = FixedClock::new(5);
        let report = execute(&request(dir.path(), &config, &runner, &clock)).unwrap();
        // Exit 0 stays a pass even though its log was bounded.
        assert!(report.passed(), "{report:?}");
        assert!(report.results[0].raw_truncated);
        assert!(report.results[0].output.truncated);
    }
}
