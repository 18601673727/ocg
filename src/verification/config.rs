//! Typed top-level `verification` policy.
//!
//! Like `context` and `runtime`, verification is configured under its own
//! top-level key. The policy is deliberately conservative: **no command runs
//! merely because a manifest exists**. Stages start empty, and commands only
//! come from trusted defaults, project/user configuration or an explicit
//! `ocg verify` invocation. Nothing in this module reads a model response.

use crate::error::{OcgError, Result};
use crate::verification::command::CommandSpec;
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;

/// The stages the verification runner understands, in order.
pub const STAGES: [&str; 3] = ["fast", "normal", "full"];

/// Default cap on captured raw bytes per stream.
pub const DEFAULT_MAX_RAW_LOG_BYTES: usize = 2_000_000;
/// Hard upper bound accepted for `maxRawLogBytes` (64 MiB).
pub const MAX_RAW_LOG_BYTES_CEILING: usize = 64 * 1024 * 1024;
/// Default cap on the total on-disk raw log directory (50 MiB).
pub const DEFAULT_MAX_LOG_STORAGE_BYTES: u64 = 50 * 1024 * 1024;
/// Hard upper bound accepted for `maxLogStorageBytes` (2 GiB).
pub const MAX_LOG_STORAGE_BYTES_CEILING: u64 = 2 * 1024 * 1024 * 1024;

/// One named stage: an ordered, trusted command list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Default)]
pub struct StageConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub commands: Vec<CommandSpec>,
}

/// Parsed, validated verification policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VerificationConfig {
    /// Allow `ocg verify` to run configured commands.
    pub enabled: bool,
    /// The stage used when the CLI does not name one.
    pub default_stage: String,
    /// Stop after the first failing command in a stage.
    pub stop_on_failure: bool,
    /// Bound for each captured stdout/stderr stream (bytes).
    pub max_raw_log_bytes: usize,
    /// Bound for the whole `.ocg/logs/` directory (bytes).
    pub max_log_storage_bytes: u64,
    /// Include the advisory targeted-test proposal in verification reports.
    pub include_test_proposal: bool,
    /// The three stages, always present.
    pub stages: BTreeMap<String, StageConfig>,
}

impl Default for VerificationConfig {
    fn default() -> Self {
        let mut stages = BTreeMap::new();
        for stage in STAGES {
            stages.insert(stage.to_string(), StageConfig::default());
        }
        Self {
            enabled: true,
            default_stage: "normal".to_string(),
            stop_on_failure: true,
            max_raw_log_bytes: DEFAULT_MAX_RAW_LOG_BYTES,
            max_log_storage_bytes: DEFAULT_MAX_LOG_STORAGE_BYTES,
            include_test_proposal: true,
            stages,
        }
    }
}

impl VerificationConfig {
    /// Parse `data["verification"]`, falling back to the defaults when absent.
    pub fn from_config(data: &Value) -> Result<Self> {
        let Some(verification) = data.get("verification") else {
            return Ok(Self::default());
        };
        if verification.is_null() {
            return Ok(Self::default());
        }
        let object = verification.as_object().ok_or_else(|| {
            OcgError::config("verification must be a JSON object with stages and limits")
        })?;
        let mut config = Self::default();

        if let Some(value) = object.get("enabled") {
            config.enabled = value
                .as_bool()
                .ok_or_else(|| OcgError::config("verification.enabled must be a boolean"))?;
        }
        if let Some(value) = object.get("defaultStage") {
            config.default_stage = value
                .as_str()
                .ok_or_else(|| OcgError::config("verification.defaultStage must be a string"))?
                .to_string();
        }
        if let Some(value) = object.get("stopOnFailure") {
            config.stop_on_failure = value
                .as_bool()
                .ok_or_else(|| OcgError::config("verification.stopOnFailure must be a boolean"))?;
        }
        if let Some(value) = object.get("includeTestProposal") {
            config.include_test_proposal = value.as_bool().ok_or_else(|| {
                OcgError::config("verification.includeTestProposal must be a boolean")
            })?;
        }
        if let Some(value) = object.get("maxRawLogBytes") {
            config.max_raw_log_bytes = value.as_u64().ok_or_else(|| {
                OcgError::config("verification.maxRawLogBytes must be a positive integer")
            })? as usize;
        }
        if let Some(value) = object.get("maxLogStorageBytes") {
            config.max_log_storage_bytes = value.as_u64().ok_or_else(|| {
                OcgError::config("verification.maxLogStorageBytes must be a positive integer")
            })?;
        }
        if let Some(value) = object.get("stages") {
            config.stages = parse_stages(value)?;
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
        if !STAGES.contains(&self.default_stage.as_str()) {
            return Err(OcgError::config(format!(
                "verification.defaultStage '{}' is not one of: {}",
                self.default_stage,
                STAGES.join(", ")
            )));
        }
        if self.max_raw_log_bytes == 0 {
            return Err(OcgError::config(
                "verification.maxRawLogBytes must be greater than zero",
            ));
        }
        if self.max_raw_log_bytes > MAX_RAW_LOG_BYTES_CEILING {
            return Err(OcgError::config(format!(
                "verification.maxRawLogBytes must not exceed {MAX_RAW_LOG_BYTES_CEILING}"
            )));
        }
        if self.max_log_storage_bytes == 0 {
            return Err(OcgError::config(
                "verification.maxLogStorageBytes must be greater than zero",
            ));
        }
        if self.max_log_storage_bytes > MAX_LOG_STORAGE_BYTES_CEILING {
            return Err(OcgError::config(format!(
                "verification.maxLogStorageBytes must not exceed {MAX_LOG_STORAGE_BYTES_CEILING}"
            )));
        }
        for stage in STAGES {
            if !self.stages.contains_key(stage) {
                return Err(OcgError::config(format!(
                    "verification.stages is missing the '{stage}' stage"
                )));
            }
        }
        Ok(())
    }

    /// The stage policy by name, or an error naming the supported stages.
    pub fn stage(&self, name: &str) -> Result<&StageConfig> {
        self.stages.get(name).ok_or_else(|| {
            OcgError::config(format!(
                "unknown verification stage '{name}' (expected: {})",
                STAGES.join(", ")
            ))
        })
    }

    pub fn command_count(&self, name: &str) -> usize {
        self.stages
            .get(name)
            .map(|stage| stage.commands.len())
            .unwrap_or(0)
    }

    /// A stable fingerprint of the policy, usable in cache keys.
    pub fn fingerprint(&self) -> String {
        let value = serde_json::to_value(self).unwrap_or(Value::Null);
        crate::runtime::hash::sha256_hex(value.to_string().as_bytes())
    }
}

fn parse_stages(value: &Value) -> Result<BTreeMap<String, StageConfig>> {
    let object = value
        .as_object()
        .ok_or_else(|| OcgError::config("verification.stages must be a JSON object"))?;
    let mut stages: BTreeMap<String, StageConfig> = BTreeMap::new();
    for stage in STAGES {
        stages.insert(stage.to_string(), StageConfig::default());
    }
    for (name, spec) in object {
        if !STAGES.contains(&name.as_str()) {
            return Err(OcgError::config(format!(
                "verification stage '{name}' is not supported (expected: {})",
                STAGES.join(", ")
            )));
        }
        let spec = spec.as_object().ok_or_else(|| {
            OcgError::config(format!("verification.stages.{name} must be an object"))
        })?;
        let mut stage = StageConfig::default();
        if let Some(description) = spec.get("description") {
            stage.description = Some(
                description
                    .as_str()
                    .ok_or_else(|| {
                        OcgError::config(format!(
                            "verification.stages.{name}.description must be a string"
                        ))
                    })?
                    .to_string(),
            );
        }
        if let Some(commands) = spec.get("commands") {
            let list = commands.as_array().ok_or_else(|| {
                OcgError::config(format!(
                    "verification.stages.{name}.commands must be a list"
                ))
            })?;
            for value in list {
                let command = CommandSpec::from_value(value).map_err(|error| {
                    OcgError::config(format!("verification.stages.{name}: {error}"))
                })?;
                stage.commands.push(command);
            }
        }
        stages.insert(name.clone(), stage);
    }
    Ok(stages)
}

/// Normalize a requested stage name, defaulting to the configured stage.
pub fn resolve_stage<'a>(
    config: &'a VerificationConfig,
    requested: Option<&'a str>,
) -> Result<(&'a str, &'a StageConfig)> {
    let name = requested.unwrap_or(&config.default_stage);
    if !STAGES.contains(&name) {
        return Err(OcgError::config(format!(
            "unknown verification stage '{name}' (expected: {})",
            STAGES.join(", ")
        )));
    }
    Ok((name, config.stage(name)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn absent_verification_uses_conservative_defaults() {
        let config = VerificationConfig::from_config(&json!({})).unwrap();
        assert_eq!(config, VerificationConfig::default());
        assert_eq!(
            VerificationConfig::from_config(&json!({"verification": null})).unwrap(),
            VerificationConfig::default()
        );
        for stage in STAGES {
            assert!(
                config.stage(stage).unwrap().commands.is_empty(),
                "stage {stage} must start empty"
            );
        }
    }

    #[test]
    fn partial_config_keeps_other_defaults() {
        let config = VerificationConfig::from_config(&json!({
            "verification": {"enabled": false, "maxRawLogBytes": 10}
        }))
        .unwrap();
        assert!(!config.enabled);
        assert_eq!(config.max_raw_log_bytes, 10);
        assert_eq!(config.default_stage, "normal");
        assert_eq!(config.max_log_storage_bytes, DEFAULT_MAX_LOG_STORAGE_BYTES);
    }

    #[test]
    fn parses_stage_commands_in_string_and_object_form() {
        let config = VerificationConfig::from_config(&json!({
            "verification": {
                "stages": {
                    "fast": {"description": "quick", "commands": ["cargo fmt --check"]},
                    "normal": {"commands": [{"program": "cargo", "args": ["check"]}]},
                }
            }
        }))
        .unwrap();
        assert_eq!(config.command_count("fast"), 1);
        assert_eq!(
            config.stage("fast").unwrap().commands[0].display(),
            "cargo fmt --check"
        );
        assert_eq!(
            config.stage("normal").unwrap().commands[0].display(),
            "cargo check"
        );
        assert_eq!(config.command_count("full"), 0);
    }

    #[test]
    fn rejects_invalid_configs() {
        for bad in [
            json!({"verification": "on"}),
            json!({"verification": {"enabled": "yes"}}),
            json!({"verification": {"defaultStage": "quick"}}),
            json!({"verification": {"maxRawLogBytes": 0}}),
            json!({"verification": {"maxRawLogBytes": 1_000_000_000_000u64}}),
            json!({"verification": {"maxLogStorageBytes": 0}}),
            json!({"verification": {"stages": []}}),
            json!({"verification": {"stages": {"turbo": {}}}}),
            json!({"verification": {"stages": {"fast": {"commands": "cargo check"}}}}),
            json!({"verification": {"stages": {"fast": {"commands": [1, 2]}}}}),
        ] {
            assert!(
                VerificationConfig::from_config(&bad).is_err(),
                "expected rejection for {bad}"
            );
            assert!(!VerificationConfig::validate(&bad).is_empty());
        }
    }

    #[test]
    fn rejects_shell_commands_in_stage_config() {
        let bad = json!({
            "verification": {"stages": {"fast": {"commands": ["cargo check; rm -rf ."]}}}
        });
        assert!(VerificationConfig::from_config(&bad).is_err());
    }

    #[test]
    fn resolve_stage_defaults_and_rejects_unknown() {
        let config = VerificationConfig::default();
        assert_eq!(resolve_stage(&config, None).unwrap().0, "normal");
        assert_eq!(resolve_stage(&config, Some("full")).unwrap().0, "full");
        assert!(resolve_stage(&config, Some("turbo")).is_err());
    }

    #[test]
    fn fingerprint_changes_with_values() {
        let a = VerificationConfig::default();
        let mut b = a.clone();
        b.enabled = !b.enabled;
        assert_ne!(a.fingerprint(), b.fingerprint());
    }
}
