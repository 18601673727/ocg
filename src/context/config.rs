//! The small top-level `context` object.
//!
//! Like `runtime` and `observability`, the context engine is configured by its
//! own top-level key so provider configuration is never overloaded. All
//! fields are optional and the defaults are deliberately conservative: the
//! engine must be safe to run on every launch without a user writing config.

use crate::error::{OcgError, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Files larger than this are indexed by path metadata only, never read.
pub const DEFAULT_MAX_FILE_BYTES: u64 = 1_000_000;
/// Hard upper bound accepted for `maxFileBytes` (64 MiB).
pub const MAX_FILE_BYTES_CEILING: u64 = 64 * 1024 * 1024;
/// Default hard cap on files scanned into the repo map.
pub const DEFAULT_MAX_REPOSITORY_FILES: usize = 100_000;
/// Hard upper bound accepted for `maxRepositoryFiles` (5 million).
pub const MAX_REPOSITORY_FILES_CEILING: usize = 5_000_000;

/// Parsed, validated context engine policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ContextConfig {
    /// Allow context production. When false nothing is read, indexed or cached.
    pub enabled: bool,
    /// Read and write the local context cache.
    pub cache: bool,
    /// Source files above this size are never read or parsed.
    pub max_file_bytes: u64,
    /// Hard cap on files scanned/indexed; the map is marked truncated beyond it.
    pub max_repository_files: usize,
    /// Maximum ranked candidates in a plan.
    pub max_candidates: usize,
    /// Maximum files selected into a plan.
    pub max_files: usize,
    /// Maximum content slices selected into a plan.
    pub max_slices: usize,
    /// Maximum bytes of selected content slices.
    pub max_bytes: usize,
    /// Maximum bytes of git diff text retained.
    pub max_diff_bytes: usize,
    /// Maximum diff hunks retained.
    pub max_hunks: usize,
    /// Maximum symbols extracted per file.
    pub max_symbols_per_file: usize,
    /// Include untracked files in the git diff summary.
    pub include_untracked: bool,
}

impl Default for ContextConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            cache: true,
            max_file_bytes: DEFAULT_MAX_FILE_BYTES,
            max_repository_files: DEFAULT_MAX_REPOSITORY_FILES,
            max_candidates: 200,
            max_files: 24,
            max_slices: 48,
            max_bytes: 262_144,
            max_diff_bytes: 131_072,
            max_hunks: 40,
            max_symbols_per_file: 200,
            include_untracked: true,
        }
    }
}

impl ContextConfig {
    /// Parse `data["context"]`, falling back to the defaults when absent.
    pub fn from_config(data: &Value) -> Result<Self> {
        let Some(context) = data.get("context") else {
            return Ok(Self::default());
        };
        if context.is_null() {
            return Ok(Self::default());
        }
        if !context.is_object() {
            return Err(OcgError::config(
                "context must be a JSON object with enabled, cache and size limits",
            ));
        }
        let parsed: Self = serde_json::from_value(context.clone()).map_err(|error| {
            OcgError::config(format!("context is not a valid configuration: {error}"))
        })?;
        parsed.validate_values()?;
        Ok(parsed)
    }

    /// Collect every policy problem for whole-configuration validation.
    pub fn validate(data: &Value) -> Vec<String> {
        match Self::from_config(data) {
            Ok(_) => Vec::new(),
            Err(error) => vec![error.to_string()],
        }
    }

    fn validate_values(&self) -> Result<()> {
        if self.max_file_bytes == 0 {
            return Err(OcgError::config(
                "context.maxFileBytes must be greater than zero",
            ));
        }
        if self.max_file_bytes > MAX_FILE_BYTES_CEILING {
            return Err(OcgError::config(format!(
                "context.maxFileBytes must not exceed {MAX_FILE_BYTES_CEILING}"
            )));
        }
        if self.max_repository_files == 0 {
            return Err(OcgError::config(
                "context.maxRepositoryFiles must be greater than zero",
            ));
        }
        if self.max_repository_files > MAX_REPOSITORY_FILES_CEILING {
            return Err(OcgError::config(format!(
                "context.maxRepositoryFiles must not exceed {MAX_REPOSITORY_FILES_CEILING}"
            )));
        }
        for (name, value) in [
            ("context.maxCandidates", self.max_candidates),
            ("context.maxFiles", self.max_files),
            ("context.maxSlices", self.max_slices),
            ("context.maxBytes", self.max_bytes),
            ("context.maxDiffBytes", self.max_diff_bytes),
            ("context.maxHunks", self.max_hunks),
            ("context.maxSymbolsPerFile", self.max_symbols_per_file),
        ] {
            if value == 0 {
                return Err(OcgError::config(format!(
                    "{name} must be greater than zero"
                )));
            }
        }
        Ok(())
    }

    /// A stable fingerprint of the policy, used in cache keys.
    pub fn fingerprint(&self) -> String {
        let value = serde_json::to_value(self).unwrap_or(Value::Null);
        crate::hash::sha256_hex(value.to_string().as_bytes())
    }

    /// The limits carried into a plan for transparency.
    pub fn limits(&self) -> crate::context::ranking::ContextLimits {
        crate::context::ranking::ContextLimits {
            max_candidates: self.max_candidates,
            max_files: self.max_files,
            max_slices: self.max_slices,
            max_bytes: self.max_bytes,
            max_diff_bytes: self.max_diff_bytes,
            max_hunks: self.max_hunks,
            max_file_bytes: self.max_file_bytes,
            max_symbols_per_file: self.max_symbols_per_file,
            max_repository_files: self.max_repository_files,
        }
    }
}
