//! Cached update checks.
//!
//! A due check records the last time we asked GitHub for the latest release
//! and, when known, the latest version. The cache lives in the platform cache
//! directory so a fresh install never networks on every launch.

use crate::clock::Clock;
use crate::error::{OcgError, Result};
use semver::Version;
use serde_json::{json, Value};
use std::fs;
use std::path::{Path, PathBuf};

/// One cached update-check result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheRecord {
    pub checked_at: i64,
    pub version: Option<Version>,
    /// A safe category for a failed check; never raw transport/process text.
    pub failure_reason: Option<String>,
}

impl CacheRecord {
    /// Read the cache from `dir`, or `None` when there is nothing usable.
    pub fn read(dir: &Path) -> Option<Self> {
        let text = fs::read_to_string(cache_file(dir)).ok()?;
        Self::parse(&text)
    }

    /// Parse a cache document. Malformed files are treated as absent.
    pub fn parse(text: &str) -> Option<Self> {
        let value: Value = serde_json::from_str(text).ok()?;
        let checked_at = value.get("checked_at").and_then(Value::as_i64)?;
        let version = value
            .get("version")
            .and_then(Value::as_str)
            .and_then(super::policy::parse_exact_version);
        let failure_reason = value
            .get("failure_reason")
            .and_then(Value::as_str)
            .filter(|reason| matches!(*reason, "rate_limited" | "failed"))
            .map(str::to_string);
        Some(Self {
            checked_at,
            version,
            failure_reason,
        })
    }

    pub fn to_json(&self) -> Value {
        json!({
            "checked_at": self.checked_at,
            "version": self.version.as_ref().map(ToString::to_string),
            "failure_reason": self.failure_reason,
        })
    }

    /// Write the cache atomically (temp sibling + rename).
    pub fn write(&self, dir: &Path) -> Result<()> {
        fs::create_dir_all(dir)
            .map_err(|error| OcgError::io(format!("cannot create {}", dir.display()), error))?;
        let target = cache_file(dir);
        let text = format!("{}\n", self.to_json());
        let parent = target.parent().unwrap_or(dir);
        let mut temporary = tempfile::Builder::new()
            .prefix(".update-check-")
            .tempfile_in(parent)
            .map_err(|error| OcgError::io(format!("cannot write {}", target.display()), error))?;
        use std::io::Write;
        temporary
            .write_all(text.as_bytes())
            .map_err(|error| OcgError::write(&target, error))?;
        temporary
            .persist(&target)
            .map_err(|error| OcgError::write(&target, error.error))?;
        Ok(())
    }

    /// Whether a check is due at `now`.
    pub fn is_due(&self, now: i64, interval_hours: u64) -> bool {
        due(Some(self), now, interval_hours, false)
    }
}

/// The cache file inside a cache directory.
pub fn cache_file(dir: &Path) -> PathBuf {
    dir.join("update-check.json")
}

/// Whether an update check should run.
pub fn due(record: Option<&CacheRecord>, now: i64, interval_hours: u64, force: bool) -> bool {
    if force {
        return true;
    }
    let Some(record) = record else {
        return true;
    };
    let interval = (interval_hours as i64).saturating_mul(3_600);
    now.saturating_sub(record.checked_at) >= interval
}

/// Whether a check is due given a clock and an optional cache directory.
pub fn is_due(
    cache_dir: Option<&Path>,
    clock: &dyn Clock,
    interval_hours: u64,
    force: bool,
) -> bool {
    let record = cache_dir.and_then(CacheRecord::read);
    due(record.as_ref(), clock.now_unix(), interval_hours, force)
}

/// The platform cache directory for OCG, when one can be determined.
pub fn platform_cache_dir() -> Option<PathBuf> {
    directories::BaseDirs::new().map(|dirs| dirs.cache_dir().join("ocg"))
}
