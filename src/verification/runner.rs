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
/// to the injected [`CaptureRunner`].
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
