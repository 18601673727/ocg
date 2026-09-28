//! Runtime policy: the small top-level `runtime` object.
//!
//! The existing top-level `opencode` key is raw generated OpenCode config and
//! must never be overloaded with OCG behaviour. Runtime policy therefore
//! lives in its own object, with defaults baked into the binary.

use crate::error::{OcgError, Result};
use semver::Version;
use serde_json::Value;

/// Default update-check interval.
pub const DEFAULT_CHECK_INTERVAL_HOURS: u64 = 24;

/// The single OpenCode version floor. `OPENCODE_CONFIG_CONTENT` is required to
/// pass the generated config, and that requires OpenCode >= 1.18.0.
pub fn min_opencode_version() -> Version {
    Version::new(1, 18, 0)
}

/// The only supported release channel today.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Channel {
    Latest,
}

impl Channel {
    pub fn as_str(self) -> &'static str {
        match self {
            Channel::Latest => "latest",
        }
    }
}

/// What to do when no runtime can be found or the system runtime is unusable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fallback {
    ProjectLocal,
}

impl Fallback {
    pub fn as_str(self) -> &'static str {
        match self {
            Fallback::ProjectLocal => "project-local",
        }
    }
}

/// Parsed, validated runtime policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimePolicy {
    pub channel: Channel,
    pub auto_upgrade: bool,
    pub check_interval_hours: u64,
    pub fallback: Fallback,
    /// Exact semver pin. When set, the managed runtime never advances.
    pub version: Option<Version>,
}

impl Default for RuntimePolicy {
    fn default() -> Self {
        Self {
            channel: Channel::Latest,
            auto_upgrade: true,
            check_interval_hours: DEFAULT_CHECK_INTERVAL_HOURS,
            fallback: Fallback::ProjectLocal,
            version: None,
        }
    }
}

impl RuntimePolicy {
    /// Parse `data["runtime"]`, falling back to the defaults when absent.
    pub fn from_config(data: &Value) -> Result<Self> {
        let Some(runtime) = data.get("runtime") else {
            return Ok(Self::default());
        };
        let Some(object) = runtime.as_object() else {
            return Err(OcgError::config(
                "runtime must be a JSON object with channel, autoUpgrade, checkIntervalHours, fallback and optional version",
            ));
        };

        let mut policy = Self::default();

        if let Some(channel) = object.get("channel") {
            match channel.as_str() {
                Some("latest") => policy.channel = Channel::Latest,
                _ => {
                    return Err(OcgError::config(format!(
                        "runtime.channel must be \"latest\", got {}",
                        compact(channel)
                    )))
                }
            }
        }

        if let Some(auto_upgrade) = object.get("autoUpgrade") {
            policy.auto_upgrade = auto_upgrade
                .as_bool()
                .ok_or_else(|| OcgError::config("runtime.autoUpgrade must be a boolean"))?;
        }

        if let Some(interval) = object.get("checkIntervalHours") {
            policy.check_interval_hours = parse_interval(interval)?;
        }

        if let Some(fallback) = object.get("fallback") {
            match fallback.as_str() {
                Some("project-local") => policy.fallback = Fallback::ProjectLocal,
                _ => {
                    return Err(OcgError::config(format!(
                        "runtime.fallback must be \"project-local\", got {}",
                        compact(fallback)
                    )))
                }
            }
        }

        if let Some(version) = object.get("version") {
            if !version.is_null() {
                let raw = version.as_str().ok_or_else(|| {
                    OcgError::config("runtime.version must be an exact semver string")
                })?;
                let parsed = parse_exact_version(raw).ok_or_else(|| {
                    OcgError::config(format!(
                        "runtime.version '{raw}' is not an exact semver (for example \"1.18.31\")"
                    ))
                })?;
                if parsed < min_opencode_version() {
                    return Err(OcgError::config(format!(
                        "runtime.version '{raw}' is below the required OpenCode {} floor",
                        min_opencode_version()
                    )));
                }
                policy.version = Some(parsed);
            }
        }

        Ok(policy)
    }

    /// Collect every policy problem for whole-configuration validation.
    pub fn validate(data: &Value) -> Vec<String> {
        match Self::from_config(data) {
            Ok(_) => Vec::new(),
            Err(error) => vec![error.to_string()],
        }
    }

    pub fn is_pinned(&self) -> bool {
        self.version.is_some()
    }

    /// Whether an existing compatible runtime may be advanced automatically.
    pub fn allows_auto_upgrade(&self) -> bool {
        self.auto_upgrade && !self.is_pinned()
    }
}

/// Parse an exact `major.minor.patch` semver, tolerating a leading `v`.
pub fn parse_exact_version(raw: &str) -> Option<Version> {
    let trimmed = raw.trim();
    let trimmed = trimmed.strip_prefix('v').unwrap_or(trimmed);
    Version::parse(trimmed).ok()
}

fn parse_interval(value: &Value) -> Result<u64> {
    let number = value
        .as_u64()
        .ok_or_else(|| OcgError::config("runtime.checkIntervalHours must be a positive integer"))?;
    if number == 0 {
        return Err(OcgError::config(
            "runtime.checkIntervalHours must be greater than zero",
        ));
    }
    Ok(number)
}

fn compact(value: &Value) -> String {
    match value {
        Value::String(text) => format!("\"{text}\""),
        other => other.to_string(),
    }
}
