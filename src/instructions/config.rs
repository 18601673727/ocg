//! Policy for repository instruction discovery and projection.
//!
//! Separated from the algorithms in the sibling modules for the same reason
//! [`crate::compaction::config`] separates: each algorithm takes its policy by
//! reference, so a caller can construct one directly without going through
//! configuration parsing.
//!
//! The precedence chain, stated once:
//!
//! ```text
//! user-level          lowest precedence, applies everywhere
//! project root        highest precedence among directory-scoped files
//! ...                 each nested directory outranks its ancestors
//! working directory   the most specific file, and the last word
//! ```
//!
//! Precedence is expressed by **order**, not by a winner-take-one rule. Every
//! discovered file contributes, root first, so a nested file can refine an
//! inherited rule without erasing it. A file that wants to *cancel* an inherited
//! instruction has to say so in prose, because a silent structural override
//! would make an instruction disappear without the model ever being told.

use crate::error::{OcgError, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The canonical instruction filename.
pub const CANONICAL_FILENAME: &str = "AGENTS.md";

/// The per-directory override filename, tried before [`CANONICAL_FILENAME`].
///
/// One file per directory is read, not both. Two files in one directory would
/// make "which one wins" a question with no answer the model could check, and an
/// instruction the model cannot reason about is worse than no instruction.
pub const OVERRIDE_FILENAME: &str = "AGENTS.override.md";

/// Filenames tried per directory, in order.
pub const DEFAULT_FILENAMES: [&str; 2] = [OVERRIDE_FILENAME, CANONICAL_FILENAME];

/// Default cap on one instruction file, in bytes.
///
/// A root file that consumes the whole budget silently starves every nested
/// file, so the cap is per file and the total is capped separately. Truncation is
/// always recorded in the projection rather than being invisible.
pub const DEFAULT_MAX_FILE_BYTES: usize = 32 * 1024;

/// Default cap on the whole resolved chain, in bytes.
pub const DEFAULT_MAX_TOTAL_BYTES: usize = 64 * 1024;

/// Default cap on how many directories the upward walk inspects.
///
/// The walk is bounded by the project root as well, so this only bounds a
/// pathological root-to-cwd distance.
pub const DEFAULT_MAX_DEPTH: usize = 64;

/// Default cap on files kept from one chain.
pub const DEFAULT_MAX_FILES: usize = 32;

/// Hard ceiling on a single-file cap.
pub const MAX_FILE_BYTES_CEILING: usize = 1024 * 1024;

/// Hard ceiling on a total-budget cap.
pub const MAX_TOTAL_BYTES_CEILING: usize = 4 * 1024 * 1024;

/// `instructions`: the repository-instruction policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct InstructionsConfig {
    /// Master switch. When false nothing is read and no section is projected.
    pub enabled: bool,
    /// Filenames tried in each directory, in precedence order. The first
    /// existing file in a directory wins and the rest are ignored.
    pub filenames: Vec<String>,
    /// Read a user-level file that applies to every project.
    pub user_level: bool,
    /// Cap on one instruction file, in bytes.
    pub max_file_bytes: usize,
    /// Cap on the whole resolved chain, in bytes.
    pub max_total_bytes: usize,
    /// Cap on directories inspected by the upward walk.
    pub max_depth: usize,
    /// Cap on files kept from one chain.
    pub max_files: usize,
}

impl Default for InstructionsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            filenames: DEFAULT_FILENAMES.iter().map(|name| name.to_string()).collect(),
            user_level: true,
            max_file_bytes: DEFAULT_MAX_FILE_BYTES,
            max_total_bytes: DEFAULT_MAX_TOTAL_BYTES,
            max_depth: DEFAULT_MAX_DEPTH,
            max_files: DEFAULT_MAX_FILES,
        }
    }
}

impl InstructionsConfig {
    /// Parse the nested JSON object. Absent or null yields the defaults.
    pub fn from_value(value: &Value) -> Result<Self> {
        let Some(object) = as_object(value)? else {
            return Ok(Self::default());
        };
        let mut config = Self::default();
        if let Some(value) = object.get("enabled") {
            config.enabled = value
                .as_bool()
                .ok_or_else(|| invalid_err("enabled must be a boolean"))?;
        }
        if let Some(value) = first(object, &["filenames", "fileNames"]) {
            config.filenames = parse_filenames(value)?;
        }
        if let Some(value) = first(object, &["userLevel", "user_level"]) {
            config.user_level = value
                .as_bool()
                .ok_or_else(|| invalid_err("userLevel must be a boolean"))?;
        }
        for (keys, slot, label) in [
            (
                ["maxFileBytes", "max_file_bytes"].as_slice(),
                &mut config.max_file_bytes,
                "maxFileBytes",
            ),
            (
                ["maxTotalBytes", "max_total_bytes"].as_slice(),
                &mut config.max_total_bytes,
                "maxTotalBytes",
            ),
        ] {
            if let Some(value) = first(object, keys) {
                *slot = parse_positive_usize(value, label)?;
            }
        }
        if let Some(value) = first(object, &["maxDepth", "max_depth"]) {
            config.max_depth = parse_positive_usize(value, "maxDepth")?;
        }
        if let Some(value) = first(object, &["maxFiles", "max_files"]) {
            config.max_files = parse_positive_usize(value, "maxFiles")?;
        }
        config.validate_values()?;
        Ok(config)
    }

    /// Parse `data["instructions"]`.
    pub fn from_config(data: &Value) -> Result<Self> {
        let value = data.get("instructions").unwrap_or(&Value::Null);
        Self::from_value(value)
    }

    /// Collect every policy problem for whole-configuration validation.
    pub fn validate(data: &Value) -> Vec<String> {
        match Self::from_config(data) {
            Ok(_) => Vec::new(),
            Err(error) => vec![error.to_string()],
        }
    }

    pub fn validate_values(&self) -> Result<()> {
        if self.filenames.is_empty() {
            return Err(invalid_err(
                "filenames must name at least one instruction file",
            ));
        }
        for name in &self.filenames {
            validate_filename(name)?;
        }
        if self.max_file_bytes > MAX_FILE_BYTES_CEILING {
            return Err(invalid_err(format!(
                "maxFileBytes must not exceed {MAX_FILE_BYTES_CEILING}"
            )));
        }
        if self.max_total_bytes > MAX_TOTAL_BYTES_CEILING {
            return Err(invalid_err(format!(
                "maxTotalBytes must not exceed {MAX_TOTAL_BYTES_CEILING}"
            )));
        }
        if self.max_total_bytes < self.max_file_bytes {
            return Err(invalid_err(
                "maxTotalBytes must not be below maxFileBytes; one file must always fit",
            ));
        }
        Ok(())
    }

    /// A stable fingerprint of the policy, so a projection built under different
    /// settings is recognisable as such.
    pub fn fingerprint(&self) -> String {
        let value = serde_json::to_value(self).unwrap_or(Value::Null);
        crate::hash::sha256_hex(value.to_string().as_bytes())
    }
}

/// A filename that cannot be probed safely.
///
/// A name containing a separator or a NUL is rejected even though it would
/// "work" on most platforms. Resolving such a name against a remote or
/// sandboxed filesystem can be turned into a request for a different resource
/// than the name suggests, so the name space is kept to plain basenames.
fn validate_filename(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(invalid_err("filenames must not be empty"));
    }
    if name.contains('/') || name.contains('\\') || name.contains('\0') {
        return Err(invalid_err(format!(
            "filenames must be plain basenames, but '{name}' contains a path separator"
        )));
    }
    Ok(())
}

fn parse_filenames(value: &Value) -> Result<Vec<String>> {
    let array = value
        .as_array()
        .ok_or_else(|| invalid_err("filenames must be an array of strings"))?;
    let names: Vec<String> = array
        .iter()
        .map(|entry| {
            entry
                .as_str()
                .map(|name| name.to_string())
                .ok_or_else(|| invalid_err("filenames must be an array of strings"))
        })
        .collect::<Result<Vec<String>>>()?;
    if names.is_empty() {
        return Err(invalid_err(
            "filenames must name at least one instruction file",
        ));
    }
    for name in &names {
        validate_filename(name)?;
    }
    Ok(names)
}

fn as_object(value: &Value) -> Result<Option<&serde_json::Map<String, Value>>> {
    if value.is_null() {
        return Ok(None);
    }
    value
        .as_object()
        .map(Some)
        .ok_or_else(|| invalid_err("must be an object"))
}

fn first<'a>(object: &'a serde_json::Map<String, Value>, keys: &[&str]) -> Option<&'a Value> {
    keys.iter().find_map(|key| object.get(*key))
}

fn invalid_err(message: impl std::fmt::Display) -> OcgError {
    OcgError::config(format!("instructions.{message}"))
}

fn parse_positive_usize(value: &Value, label: &str) -> Result<usize> {
    value
        .as_u64()
        .filter(|number| *number > 0)
        .map(|number| number as usize)
        .ok_or_else(|| invalid_err(format!("{label} must be a positive integer")))
}
