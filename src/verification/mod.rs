//! Typed verification subsystem.
//!
//! Verification is **explicitly configured**: commands come only from trusted
//! defaults, project/user configuration or an `ocg verify` invocation. No
//! command is discovered from a manifest and no model output is ever executed.
//! All process construction is delegated to [`crate::process::CaptureRunner`].
//!
//! The subsystem owns four pieces:
//!
//! - [`command`] — the structured `program + args` command and its strict,
//!   shell-free parser;
//! - [`distill`] — deterministic compiler/test/build/lint log distillation;
//! - [`result`] / [`runner`] — the structured report and the stage runner;
//! - [`select`] — the conservative targeted-test proposal (advisory only).
//!
//! Raw logs are kept under `.ocg/logs/` and bounded by [`logs`].

pub mod command;
pub mod config;
pub mod distill;
pub mod logs;
pub mod result;
pub mod runner;
pub mod select;

use crate::verification::config::VerificationConfig;
use serde::{Deserialize, Serialize};

/// The verification report schema version.
pub const REPORT_SCHEMA_VERSION: u32 = 1;
/// The engine version stamped into reports.
pub const ENGINE_VERSION: &str = crate::cli::VERSION;

pub use command::CommandSpec;
pub use config::VerificationConfig as Config;
pub use result::{Overall, VerificationReport, VerificationResult};
pub use select::{TestCandidate, TestProposal};

/// A config-only summary of one stage, embedded in context plans.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageSummary {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub commands: Vec<String>,
}

/// The verification state known without running anything.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerificationState {
    pub enabled: bool,
    pub default_stage: String,
    pub stages: Vec<StageSummary>,
    pub note: String,
}

impl VerificationState {
    /// Summarize configured stages in the canonical fast, normal, full order.
    /// This never reads a log or runs a command.
    pub fn from_config(config: &VerificationConfig) -> Self {
        let stages: Vec<StageSummary> = config::STAGES
            .iter()
            .filter_map(|name| {
                config.stages.get(*name).map(|stage| StageSummary {
                    name: (*name).to_string(),
                    description: stage.description.clone(),
                    commands: stage
                        .commands
                        .iter()
                        .map(command::CommandSpec::display)
                        .collect(),
                })
            })
            .collect();
        let runnable = config
            .stages
            .values()
            .map(|stage| stage.commands.len())
            .sum::<usize>();
        let note = if !config.enabled {
            "verification is disabled".to_string()
        } else if runnable == 0 {
            "no verification commands are configured; no command will run".to_string()
        } else {
            format!("{runnable} configured command(s) across the fast/normal/full stages")
        };
        Self {
            enabled: config.enabled,
            default_stage: config.default_stage.clone(),
            stages,
            note,
        }
    }
}
