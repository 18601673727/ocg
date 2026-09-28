//! Verification results and reports.

use crate::process::ProcessExit;
use crate::verification::distill::{DistilledOutput, SourceLocation};
use crate::verification::select::TestProposal;
use serde::{Deserialize, Serialize};

/// One executed (or attempted) verification command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerificationResult {
    /// The program that was run.
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    pub stage: String,
    /// Exit code, signal or an explicit unknown — never guessed.
    pub exit: ProcessExit,
    pub success: bool,
    pub duration_ms: u64,
    pub output: DistilledOutput,
    /// Path of the raw log relative to the project root, when one was stored.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_log: Option<String>,
    /// True when the captured (and stored) raw output was cut by a cap.
    #[serde(default)]
    pub raw_truncated: bool,
    /// Convenience copy of the failed tests the distiller found.
    #[serde(default)]
    pub failed_tests: Vec<String>,
    /// Convenience copy of the source locations the distiller found.
    #[serde(default)]
    pub source_locations: Vec<SourceLocation>,
}

impl VerificationResult {
    /// A display form for the command line.
    pub fn display(&self) -> String {
        if self.args.is_empty() {
            return self.command.clone();
        }
        format!("{} {}", self.command, self.args.join(" "))
    }
}

/// The overall outcome of a stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Overall {
    Passed,
    Failed,
    NotRun,
}

impl Overall {
    pub fn as_str(self) -> &'static str {
        match self {
            Overall::Passed => "passed",
            Overall::Failed => "failed",
            Overall::NotRun => "not_run",
        }
    }
}

/// A structured, serializable verification report for one stage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerificationReport {
    pub schema_version: u32,
    pub engine_version: String,
    pub stage: String,
    /// False when `verification.enabled` is false: nothing was executed.
    pub enabled: bool,
    /// True when at least one command was executed.
    pub ran: bool,
    /// True when a failure stopped the stage before every command ran.
    pub stopped_early: bool,
    pub results: Vec<VerificationResult>,
    /// The advisory targeted-test proposal (never auto-run).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub test_proposal: Option<TestProposal>,
    pub created_at: i64,
    #[serde(default)]
    pub notes: Vec<String>,
}

impl VerificationReport {
    pub fn overall(&self) -> Overall {
        if !self.enabled || self.results.is_empty() {
            return Overall::NotRun;
        }
        if self.results.iter().all(|result| result.success) {
            Overall::Passed
        } else {
            Overall::Failed
        }
    }

    pub fn passed(&self) -> bool {
        self.overall() == Overall::Passed
    }

    pub fn failed(&self) -> bool {
        self.overall() == Overall::Failed
    }

    /// All failed tests across the stage, in command order.
    pub fn failed_tests(&self) -> Vec<String> {
        let mut out = Vec::new();
        for result in &self.results {
            for test in &result.failed_tests {
                if !out.contains(test) {
                    out.push(test.clone());
                }
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::verification::distill::distill;

    fn result(success: bool) -> VerificationResult {
        VerificationResult {
            command: "cargo".to_string(),
            args: vec!["test".to_string()],
            stage: "normal".to_string(),
            exit: ProcessExit::Code(if success { 0 } else { 101 }),
            success,
            duration_ms: 1,
            output: distill("", "", success),
            raw_log: None,
            raw_truncated: false,
            failed_tests: Vec::new(),
            source_locations: Vec::new(),
        }
    }

    #[test]
    fn overall_reflects_enabled_and_results() {
        let mut report = VerificationReport {
            schema_version: 1,
            engine_version: "test".to_string(),
            stage: "normal".to_string(),
            enabled: false,
            ran: false,
            stopped_early: false,
            results: Vec::new(),
            test_proposal: None,
            created_at: 0,
            notes: Vec::new(),
        };
        assert_eq!(report.overall(), Overall::NotRun);

        report.enabled = true;
        assert_eq!(report.overall(), Overall::NotRun);
        report.results.push(result(true));
        assert_eq!(report.overall(), Overall::Passed);
        assert!(report.passed());
        report.results.push(result(false));
        assert_eq!(report.overall(), Overall::Failed);
        assert!(report.failed());
    }
}
