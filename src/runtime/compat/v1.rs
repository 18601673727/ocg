//! OpenCode 1.18.x adapter.
//!
//! This is the historical contract OCG has always shipped:
//!
//! - worker delegation uses the `task` tool/permission key,
//! - the Rust-resolved Lead contract is enforced on the mutable request
//!   message by the generated adapter (`chat.message`),
//! - OCG replaces its own process with `opencode` and passes
//!   `OPENCODE_CONFIG_CONTENT`.

use crate::runtime::compat::{LaunchMode, LeadSelectionMode, Major, RuntimeAdapter};

/// The OpenCode 1.18.x adapter.
#[derive(Debug, Default, Clone, Copy)]
pub struct V1Adapter;

impl RuntimeAdapter for V1Adapter {
    fn major(&self) -> Major {
        Major::V1
    }

    fn task_key(&self) -> &'static str {
        "task"
    }

    fn lead_selection(&self) -> LeadSelectionMode {
        LeadSelectionMode::RequestMessage
    }

    fn launch_mode(&self) -> LaunchMode {
        LaunchMode::Exec
    }

    fn is_task_tool(&self, tool: &str) -> bool {
        tool == "task"
    }
}
