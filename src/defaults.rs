//! Shipped defaults.
//!
//! The release binary embeds `config/*.yaml` and `config/prompts/*.md` so it is
//! self-contained. For development and tests a OCG home directory with the
//! same layout can be supplied through `OCG_HOME` (legacy
//! `OCG_HOME`), and its files replace the embedded defaults.

use crate::error::Result;
use crate::yaml::{parse_yaml_object, read_yaml_object};
use serde_json::{json, Map, Value};
use std::path::PathBuf;

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

/// Where the default registries and prompts come from.
#[derive(Debug, Clone)]
pub enum OcgSource {
    /// The config compiled into the binary. Used by released binaries.
    Embedded,
    /// A OCG home containing `config/`, used for development, tests and
    /// power-user overrides.
    Dir(PathBuf),
}

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

/// Build the base effective configuration (before user/project overrides).
pub fn load_defaults(source: &OcgSource) -> Result<Value> {
    let (permissions, base, prompt_dir) = match source {
        OcgSource::Embedded => (
            parse_yaml_object("config/permissions.yaml", PERMISSIONS_YAML)?,
            parse_yaml_object("config/base.yaml", BASE_YAML)?,
            None,
        ),
        OcgSource::Dir(home) => {
            let config = home.join("config");
            (
                read_yaml_object(&config.join("permissions.yaml"))?,
                read_yaml_object(&config.join("base.yaml"))?,
                Some(config.join("prompts")),
            )
        }
    };

    let mut prompts = Map::new();
    let mut prompt_defaults = Map::new();
    for role in default_prompt_roles() {
        let value = match &prompt_dir {
            None => Value::String(format!("{EMBEDDED_PROMPT_PREFIX}{role}")),
            Some(dir) => Value::String(
                dir.join(format!("{role}.md"))
                    .to_string_lossy()
                    .into_owned(),
            ),
        };
        prompts.insert(role.to_string(), value.clone());
        prompt_defaults.insert(role.to_string(), value);
    }

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
    }))
}

/// The built-in context engine policy.
///
/// Kept in code (like `observability` and `runtime`) so a disk OCG home keeps
/// working without a new required file. All context fields are optional.
pub fn default_context() -> Value {
    serde_json::to_value(crate::context::ContextConfig::default()).unwrap_or_else(|_| json!({}))
}

/// The built-in runtime policy.
///
/// Kept in code (like `observability`) so a disk OCG home does not need a new
/// required file and existing OCG homes keep working.
pub fn default_runtime() -> Value {
    json!({
        "channel": "latest",
        "autoUpgrade": true,
        "checkIntervalHours": 24,
        "fallback": "project-local",
    })
}

/// The built-in verification policy.
///
/// Kept in code so a disk OCG home keeps working without a new required file.
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
/// Kept in code (like `context`, `verification` and `telemetry`) so a disk OCG
/// home keeps working without a new required file. The defaults are
/// conservative: enabled, two Build retries and one Debug hand-off.
pub fn default_orchestration() -> Value {
    serde_json::to_value(crate::orchestration::OrchestrationConfig::default())
        .unwrap_or_else(|_| json!({}))
}
