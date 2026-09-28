//! OpenCode 1.18.x adapter.
//!
//! This is the historical contract OCG has always shipped:
//!
//! - the generated local plugin lives under the singular `plugin` array,
//! - worker delegation uses the `task` tool/permission key,
//! - the Rust-resolved Lead contract is enforced on the mutable request
//!   message by the generated adapter (`chat.message`),
//! - OCG replaces its own process with `opencode` and passes
//!   `OPENCODE_CONFIG_CONTENT`.

use crate::error::{OcgError, Result};
use crate::runtime::compat::{LaunchMode, LeadSelectionMode, Major, RuntimeAdapter};
use std::path::Path;

/// The OpenCode 1.18.x adapter.
#[derive(Debug, Default, Clone, Copy)]
pub struct V1Adapter;

impl RuntimeAdapter for V1Adapter {
    fn major(&self) -> Major {
        Major::V1
    }

    fn plugin_key(&self) -> &'static str {
        "plugin"
    }

    fn task_key(&self) -> &'static str {
        "task"
    }

    fn plugin_source(&self) -> &'static str {
        crate::orchestration::plugin::plugin_source()
    }

    fn local_plugin_uri(&self, root: &Path) -> Result<Option<String>> {
        crate::orchestration::plugin::plugin_uri(root)
            .map(Some)
            .ok_or_else(|| OcgError::config("cannot resolve the local OpenCode plugin path"))
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
