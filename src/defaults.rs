//! Shipped defaults.
//!
//! The binary compiles in `config/*.yaml` and `config/prompts/*.md`. They are
//! the defaults the Core database's committed configuration overlays; nothing
//! reads them from disk at runtime.

use crate::error::Result;
use crate::yaml::parse_yaml_object;
use serde_json::{json, Map, Value};

/// The orchestrating role. There is exactly one Lead prompt.
pub const LEAD_ROLE: &str = "lead";

/// Shipped worker roles. Routing roles are otherwise open-ended; these are
/// only the roles that ship with a default prompt.
pub const WORKER_ROLES: [&str; 6] = [
    "explore",
    "explore-deep",
    "build",
    "verify",
    "debug",
    "docs",
];

/// Worker agent ids are namespaced so they cannot collide with a repository's
/// own agents.
pub const WORKER_AGENT_PREFIX: &str = "ocg-";

/// Inserted between the OCG prompt for a role and each appended project block.
pub const PROMPT_APPEND_SEPARATOR: &str = "\n\n---\n\n";

/// Internal marker for "use the embedded prompt for this role". A NUL byte
/// cannot appear in a real path, so it can never collide with a user file.
pub const EMBEDDED_PROMPT_PREFIX: &str = "\u{0}embedded:";

const BASE_YAML: &str = include_str!("../config/base.yaml");
const PERMISSIONS_YAML: &str = include_str!("../config/permissions.yaml");

const LEAD_PROMPT: &str = include_str!("../config/prompts/lead.md");
const EXPLORE_PROMPT: &str = include_str!("../config/prompts/explore.md");
const EXPLORE_DEEP_PROMPT: &str = include_str!("../config/prompts/explore-deep.md");
const BUILD_PROMPT: &str = include_str!("../config/prompts/build.md");
const VERIFY_PROMPT: &str = include_str!("../config/prompts/verify.md");
const DEBUG_PROMPT: &str = include_str!("../config/prompts/debug.md");
const DOCS_PROMPT: &str = include_str!("../config/prompts/docs.md");

/// Roles that ship with a default prompt, Lead first.
pub fn default_prompt_roles() -> Vec<&'static str> {
    let mut roles = Vec::with_capacity(1 + WORKER_ROLES.len());
    roles.push(LEAD_ROLE);
    roles.extend(WORKER_ROLES);
    roles
}

/// The embedded prompt for a role, if one is compiled in.
pub fn embedded_prompt(role: &str) -> Option<&'static str> {
    match role {
        "lead" => Some(LEAD_PROMPT),
        "explore" => Some(EXPLORE_PROMPT),
        "explore-deep" => Some(EXPLORE_DEEP_PROMPT),
        "build" => Some(BUILD_PROMPT),
        "verify" => Some(VERIFY_PROMPT),
        "debug" => Some(DEBUG_PROMPT),
        "docs" => Some(DOCS_PROMPT),
        _ => None,
    }
}

/// Build the base effective configuration, before the committed overlay.
pub fn load_defaults() -> Result<Value> {
    let permissions = parse_yaml_object("config/permissions.yaml", PERMISSIONS_YAML)?;
    let base = parse_yaml_object("config/base.yaml", BASE_YAML)?;
    let mut prompts = Map::new();
    for role in default_prompt_roles() {
        prompts.insert(
            role.to_string(),
            Value::String(format!("{EMBEDDED_PROMPT_PREFIX}{role}")),
        );
    }
    let prompt_defaults = prompts.clone();

    Ok(json!({
        "models": {"providers": {}, "models": {}},
        "routing": {"roles": {}},
        "permissions": permissions,
        "base": base,
        "prompts": prompts,
        "_prompt_defaults": prompt_defaults,
        "observability": {"enabled": false, "path": Value::Null},
        "runtime": default_runtime(),
        "context": default_context(),
        "verification": default_verification(),
        "capabilities": default_capabilities(),
        "telemetry": default_telemetry(),
        "orchestration": default_orchestration(),
        "storage": {"attachments": {"path": crate::attachments::DEFAULT_ROOT}},
    }))
}

/// The built-in context engine policy.
///
/// All context fields are optional.
pub fn default_context() -> Value {
    serde_json::to_value(crate::context::ContextConfig::default()).unwrap_or_else(|_| json!({}))
}

/// The built-in runtime policy.
pub fn default_runtime() -> Value {
    json!({
        "channel": "latest",

        "fallback": "project-local",
    })
}

/// The built-in verification policy.
///
/// The schedule is deliberately empty: no command runs merely because a
/// manifest exists.
pub fn default_verification() -> Value {
    serde_json::to_value(crate::verification::Config::default()).unwrap_or_else(|_| json!({}))
}

/// The built-in capability policy.
pub fn default_capabilities() -> Value {
    serde_json::to_value(crate::capabilities::CapabilityConfig::default())
        .unwrap_or_else(|_| json!({}))
}

/// The built-in telemetry policy: local-only and enabled by default, with no
/// remote mode to opt into.
pub fn default_telemetry() -> Value {
    serde_json::to_value(crate::telemetry::TelemetryConfig::default()).unwrap_or_else(|_| json!({}))
}

/// The built-in orchestration policy.
///
/// The defaults are conservative: enabled, two Build retries and one Debug hand-off.
pub fn default_orchestration() -> Value {
    serde_json::to_value(crate::orchestration::OrchestrationConfig::default())
        .unwrap_or_else(|_| json!({}))
}
