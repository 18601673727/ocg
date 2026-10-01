//! Verification execution: run a stage's trusted commands in order.

use crate::clock::Clock;
use crate::compiler_feedback as compiler;
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

    // The previous compile's diagnostics per command. Loaded fail-soft: a
    // missing or corrupt state file simply means there is no baseline, which the
    // delta reports as `baseline_known: false` rather than as everything being
    // new.
    let loaded = crate::orchestration::state::load(request.root);
    let mut state = loaded.state;
    if loaded.corrupt {
        notes.push(
            "orchestration state could not be read; this run has no compiler diagnostic baseline"
                .to_string(),
        );
    }
    let mut live_commands: Vec<String> = Vec::new();

    for configured in stage.commands.iter() {
        // Only an explicitly enabled flag changes what a command prints, and
        // only for cargo subcommands that emit diagnostics.
        let command = if config.machine_readable_diagnostics {
            configured.with_machine_readable_diagnostics()
        } else {
            configured.clone()
        };
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
                    compiler: None,
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

        // Structured compiler feedback, when this run actually produced
        // machine-readable diagnostics. Detection is by evidence, not by command
        // name: cargo's message stream is proof, so a non-Rust command never
        // enters this path by accident.
        //
        // A clean compile still records a baseline. Leaving the previous one in
        // place would make the next compile report reintroduced diagnostics as
        // carried over rather than new.
        let key = command.display();
        let machine_readable =
            compiler::is_machine_readable_output(&stdout) || compiler::is_machine_readable_output(&stderr);
        let compiler_delta = if !machine_readable {
            None
        } else {
            let mut current = compiler::from_json_lines(&stdout);
            if current.is_empty() {
                current = compiler::from_json_lines(&stderr);
            }
            let previous = state.compiler_baseline(&key).map(|baseline| baseline.current.clone());
            let delta = compiler::delta(previous.as_ref(), current);
            live_commands.push(key.clone());
            state.record_compiler_baseline(&key, delta.clone(), now, &live_commands);
            // The distilled text for a JSON compile is the raw message stream,
            // which is exactly the re-sending this projection exists to stop. The
            // delta replaces it as the summary a reader sees; the captured
            // streams remain in the raw log below.
            output.summary = delta.observation().lines().map(str::to_string).collect();
            Some(delta)
        };

        let raw = store.store(&RawLogInput {
            created_at: now,
            label: &key,
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
            compiler: compiler_delta,
        });

        if !captured.success && config.stop_on_failure {
            stopped_early = true;
            break;
        }
    }

    // Persisting the baseline is best effort. Losing it only costs the next
    // compile its delta, so it never turns a verification run into a failure.
    if !live_commands.is_empty() {
        if let Err(error) = crate::orchestration::state::save(request.root, &state) {
            notes.push(format!(
                "compiler diagnostic baseline was not persisted: {error}"
            ));
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
