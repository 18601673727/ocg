//! Typed top-level `orchestration` policy.
//!
//! Orchestration is the layer that prepares dynamic, *bounded* context for a
//! running OpenCode session and moves a task through Explore → Build → Verify
//! (→ Debug). It is configured under its own top-level key, like `context` and
//! `verification`, so the raw `opencode` config is never overloaded.
//!
//! The policy is deliberately conservative:
//!
//! - it is enabled by default and activates at launch by materializing the
//!   generated plugin; context preparation is what remains explicit (a launch
//!   never builds the context index);
//! - retry budgets start at [[2]] build / [[1]] debug attempts;
//! - every hand-off capsule is bounded by a configurable runtime optimization
//!   envelope: an absolute byte cap and a fraction of the rich source context.
//!   The defaults (`16384` bytes / `60%`) are a size optimization, **not** a
//!   universal correctness rule. Required evidence (goal, hard constraints,
//!   critical findings, changed files, failing locations) is never dropped to
//!   satisfy the envelope; if it cannot fit, the overage is recorded.
//! - the deterministic release fixture deliberately configures a much tighter
//!   `4096` / `40%` gate to catch projection regressions.
//!
//! `OCG_ORCHESTRATION=0` is an explicit escape hatch that disables the
//! whole layer for one process. A disabled layer emits no plugin, writes no
//! state and makes no context decision.

use crate::error::{OcgError, Result};
use crate::orchestration::context_governor::ContextGovernorConfig;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Default number of Build retries after a failed verification.
pub const DEFAULT_MAX_BUILD_RETRIES: usize = 2;
/// Default number of Debug hand-offs after the Build budget is exhausted.
pub const DEFAULT_MAX_DEBUG_RETRIES: usize = 1;
/// Default absolute cap on a projected hand-off capsule. This is a **runtime
/// optimization envelope**, not a correctness rule: required evidence may
/// exceed it and is then recorded rather than dropped.
pub const DEFAULT_MAX_HANDOFF_BYTES: usize = 16_384;
/// Default cap on a hand-off as a percentage of the rich source context.
pub const DEFAULT_MAX_HANDOFF_RATIO_PERCENT: usize = 60;
/// Hard ceiling accepted for `maxBuildRetries`.
pub const MAX_BUILD_RETRIES_CEILING: usize = 10;
/// Hard ceiling accepted for `maxDebugRetries`.
pub const MAX_DEBUG_RETRIES_CEILING: usize = 5;
/// Smallest accepted `maxHandoffBytes`.
pub const MIN_HANDOFF_BYTES: usize = 512;
/// Hard ceiling accepted for `maxHandoffBytes`.
pub const MAX_HANDOFF_BYTES_CEILING: usize = 65_536;

/// The environment variable that force-disables orchestration for one process.
pub const ENV_ENABLED: &str = "OCG_ORCHESTRATION";

/// Parsed, validated orchestration policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct OrchestrationConfig {
    /// Allow orchestration. When false no plugin is emitted and no state is kept.
    pub enabled: bool,
    /// Build retries after a failed verification, before Debug is recommended.
    pub max_build_retries: usize,
    /// Debug hand-offs after the build budget is exhausted.
    pub max_debug_retries: usize,
    /// Absolute byte cap on a projected hand-off capsule.
    pub max_handoff_bytes: usize,
    /// Hand-off cap as a percentage of the rich source context.
    pub max_handoff_ratio_percent: usize,
    /// Conversation/context rollover policy.  It is intentionally separate
    /// from hand-off projection caps: projection bytes are not conversation
    /// token usage.
    #[serde(default)]
    pub context_governor: ContextGovernorConfig,
}

impl Default for OrchestrationConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_build_retries: DEFAULT_MAX_BUILD_RETRIES,
            max_debug_retries: DEFAULT_MAX_DEBUG_RETRIES,
            max_handoff_bytes: DEFAULT_MAX_HANDOFF_BYTES,
            max_handoff_ratio_percent: DEFAULT_MAX_HANDOFF_RATIO_PERCENT,
            context_governor: ContextGovernorConfig::default(),
        }
    }
}

impl OrchestrationConfig {
    /// Parse `data["orchestration"]`, falling back to the defaults when absent.
    pub fn from_config(data: &Value) -> Result<Self> {
        let Some(value) = data.get("orchestration") else {
            return Ok(Self::default());
        };
        if value.is_null() {
            return Ok(Self::default());
        }
        let object = value.as_object().ok_or_else(|| {
            OcgError::config("orchestration must be a JSON object with enabled and retry limits")
        })?;
        let mut config = Self::default();
        if let Some(enabled) = object.get("enabled") {
            config.enabled = enabled
                .as_bool()
                .ok_or_else(|| OcgError::config("orchestration.enabled must be a boolean"))?;
        }
        for (key, slot, label) in [
            (
                "maxBuildRetries",
                &mut config.max_build_retries,
                "orchestration.maxBuildRetries",
            ),
            (
                "maxDebugRetries",
                &mut config.max_debug_retries,
                "orchestration.maxDebugRetries",
            ),
            (
                "maxHandoffBytes",
                &mut config.max_handoff_bytes,
                "orchestration.maxHandoffBytes",
            ),
            (
                "maxHandoffRatioPercent",
                &mut config.max_handoff_ratio_percent,
                "orchestration.maxHandoffRatioPercent",
            ),
        ] {
            if let Some(value) = object.get(key) {
                *slot = value.as_u64().ok_or_else(|| {
                    OcgError::config(format!("{label} must be a positive integer"))
                })? as usize;
            }
        }
        if let Some(value) = object
            .get("contextGovernor")
            .or_else(|| object.get("context_governor"))
            .or_else(|| object.get("contextGovernance"))
            .or_else(|| object.get("context_governance"))
        {
            config.context_governor = ContextGovernorConfig::from_value(value)?;
        }
        config.validate_values()?;
        Ok(config)
    }

    /// Collect every policy problem for whole-configuration validation.
    pub fn validate(data: &Value) -> Vec<String> {
        match Self::from_config(data) {
            Ok(_) => Vec::new(),
            Err(error) => vec![error.to_string()],
        }
    }

    fn validate_values(&self) -> Result<()> {
        if self.max_build_retries > MAX_BUILD_RETRIES_CEILING {
            return Err(OcgError::config(format!(
                "orchestration.maxBuildRetries must not exceed {MAX_BUILD_RETRIES_CEILING}"
            )));
        }
        if self.max_debug_retries > MAX_DEBUG_RETRIES_CEILING {
            return Err(OcgError::config(format!(
                "orchestration.maxDebugRetries must not exceed {MAX_DEBUG_RETRIES_CEILING}"
            )));
        }
        if self.max_handoff_bytes < MIN_HANDOFF_BYTES
            || self.max_handoff_bytes > MAX_HANDOFF_BYTES_CEILING
        {
            return Err(OcgError::config(format!(
                "orchestration.maxHandoffBytes must be between {MIN_HANDOFF_BYTES} and {MAX_HANDOFF_BYTES_CEILING}"
            )));
        }
        if self.max_handoff_ratio_percent == 0 || self.max_handoff_ratio_percent > 100 {
            return Err(OcgError::config(
                "orchestration.maxHandoffRatioPercent must be between 1 and 100",
            ));
        }
        self.context_governor.validate_values()?;
        Ok(())
    }

    /// Apply the optional `OCG_ORCHESTRATION` override. Only an
    /// explicit on/off value changes the policy; anything else is ignored.
    pub fn with_env_override(mut self, raw: Option<&str>) -> Self {
        if let Some(enabled) = parse_env_enabled(raw) {
            self.enabled = enabled;
        }
        self
    }

    /// Apply the environment override to a raw effective config in place.
    ///
    /// This is how one process can force orchestration off without editing any
    /// on-disk configuration. An unrecognised value is ignored.
    pub fn apply_env_override(data: &mut Value, raw: Option<&str>) {
        let Some(enabled) = parse_env_enabled(raw) else {
            return;
        };
        if !data.is_object() {
            *data = serde_json::json!({});
        }
        if let Some(object) = data.as_object_mut() {
            let entry = object
                .entry("orchestration".to_string())
                .or_insert_with(|| serde_json::json!({}));
            if !entry.is_object() {
                *entry = serde_json::json!({});
            }
            if let Some(orchestration) = entry.as_object_mut() {
                orchestration.insert("enabled".to_string(), Value::Bool(enabled));
            }
        }
    }

    /// A stable fingerprint of the policy.
    pub fn fingerprint(&self) -> String {
        let value = serde_json::to_value(self).unwrap_or(Value::Null);
        crate::runtime::hash::sha256_hex(value.to_string().as_bytes())
    }
}

fn parse_env_enabled(raw: Option<&str>) -> Option<bool> {
    let raw = raw?;
    match raw.trim().to_ascii_lowercase().as_str() {
        "0" | "false" | "off" | "no" | "disabled" => Some(false),
        "1" | "true" | "on" | "yes" | "enabled" => Some(true),
        _ => None,
    }
}
