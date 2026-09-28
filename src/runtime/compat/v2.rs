//! OpenCode 2.x adapter.
//!
//! OpenCode 2 changes the surrounding shape, not OCG's policy:
//!
//! - local plugins are discovered from `OPENCODE_CONFIG_DIR/plugins`, not from
//!   a config array entry,
//! - the v1 `task` tool/permission key became `subagent`,
//! - the runtime is a daemon reached over HTTP/SSE; OCG selects the Lead on
//!   the *session* (create/resolve, switch agent/model/variant, verify) rather
//!   than by rewriting a request message,
//! - `OPENCODE_CONFIG_CONTENT` remains the supported way to hand OCG's
//!   generated config to the runtime.
//!
//! The generated adapter stays deliberately thin: it only transports dynamic
//! context and hand-offs. Session-level Lead policy, model choice and variant
//! resolution stay in Rust.

use crate::error::Result;
use crate::runtime::compat::{LaunchMode, LeadSelectionMode, Major, RuntimeAdapter};
use std::path::Path;

/// The OpenCode 2.x adapter.
#[derive(Debug, Default, Clone, Copy)]
pub struct V2Adapter;

impl RuntimeAdapter for V2Adapter {
    fn major(&self) -> Major {
        Major::V2
    }

    fn plugin_key(&self) -> &'static str {
        "plugin"
    }

    fn task_key(&self) -> &'static str {
        "subagent"
    }

    fn plugin_source(&self) -> &'static str {
        crate::orchestration::plugin::v2_plugin_source()
    }

    fn local_plugin_uri(&self, _root: &Path) -> Result<Option<String>> {
        Ok(None)
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
