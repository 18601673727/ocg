//! OpenCode 2.x adapter.
//!
//! OpenCode 2 changes the surrounding shape, not OCG's policy:
//!
//! - the v1 `task` tool/permission key became `subagent`,
//! - the runtime is a daemon reached over HTTP/SSE; OCG selects the Lead on
//!   the *session* (create/resolve, switch agent/model/variant, verify) rather
//!   than by rewriting a request message,
//! - `OPENCODE_CONFIG_CONTENT` remains the supported way to hand OCG's
//!   generated config to the runtime.

use crate::runtime::compat::{LaunchMode, LeadSelectionMode, Major, RuntimeAdapter};

/// The OpenCode 2.x adapter.
#[derive(Debug, Default, Clone, Copy)]
pub struct V2Adapter;

impl RuntimeAdapter for V2Adapter {
    fn major(&self) -> Major {
        Major::V2
    }

    fn task_key(&self) -> &'static str {
        "subagent"
    }

    fn lead_selection(&self) -> LeadSelectionMode {
        LeadSelectionMode::Session
    }

    fn launch_mode(&self) -> LaunchMode {
        LaunchMode::Daemon
    }

    fn lifecycle_capabilities(&self) -> crate::runtime::lifecycle::RuntimeCapabilities {
        crate::runtime::lifecycle::RuntimeCapabilities::OPENCODE_V2
    }

    fn is_task_tool(&self, tool: &str) -> bool {
        tool == "subagent"
    }
}
