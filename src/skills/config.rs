//! Skill policy: what a Skill is allowed to be, and how much of it may be paid
//! for.
//!
//! Separated from the algorithms for the same reason as every other policy in
//! OCG: each algorithm takes its policy by reference.
//!
//! ## The three-tier disclosure
//!
//! ```text
//! name + description   every discoverable skill, in every request
//! body                 one skill, once it is activated
//! resources            a file, once the model asks for it by relative path
//! ```
//!
//! Only the first tier is unconditional. That is the entire reason Skills are
//! cheap: a project with forty skills pays for forty names and descriptions, not
//! forty bodies.
//!
//! ## The caps are not one cap
//!
//! Three separate bounds, because they defend against three different failures:
//!
//! * `max_catalog_chars` — too many skills make the catalog itself the context.
//!   When it binds, entries are dropped and the drop is *counted in band*, so the
//!   model is told the list is incomplete rather than inferring it is complete.
//! * `max_body_chars` — one pathological skill must not evict the conversation.
//! * `max_active_body_chars` — several moderate skills must not collectively do
//!   the same. Activated bodies are re-materialized, not transcript content, so
//!   exceeding this is recoverable by dropping the least recently activated.
//!
//! ## A Skill never executes anything
//!
//! Nothing in this policy grants a capability. `required_tools` is a *declaration*
//! checked against the tools the request actually carries, and a mismatch is a
//! diagnostic. Granting a capability is [`crate::capabilities`] and execution
//! authority's job, and it stays there.

use crate::error::{OcgError, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The file that carries a skill's metadata and instructions.
pub const SKILL_FILENAME: &str = "SKILL.md";

/// Directory under `.ocg/` holding project-local skills.
pub const PROJECT_SKILL_DIR: &str = "skills";

/// Default cap on the catalog, in characters.
pub const DEFAULT_MAX_CATALOG_CHARS: usize = 8_000;

/// Default cap on one skill body, in characters.
pub const DEFAULT_MAX_BODY_CHARS: usize = 24_000;

/// Default cap on all activated bodies together, in characters.
pub const DEFAULT_MAX_ACTIVE_BODY_CHARS: usize = 48_000;

/// Default cap on supporting files listed per skill.
pub const DEFAULT_MAX_RESOURCE_FILES: usize = 32;

/// Default cap on skills that may be activated at once.
pub const DEFAULT_MAX_ACTIVE_SKILLS: usize = 8;

/// Hard ceiling on the catalog cap.
pub const MAX_CATALOG_CHARS_CEILING: usize = 262_144;

/// Hard ceiling on a body cap.
pub const MAX_BODY_CHARS_CEILING: usize = 1_048_576;

/// Hard ceiling on how many skills one registry may hold.
pub const MAX_SKILLS: usize = 4_096;

/// Hard ceiling on a skill name, in characters.
pub const MAX_NAME_CHARS: usize = 64;

/// Hard ceiling on a skill description, in characters.
pub const MAX_DESCRIPTION_CHARS: usize = 1_024;

/// `skills`: the Skill policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct SkillsConfig {
    /// Master switch. When false discovery reads nothing and no skill section
    /// is projected.
    pub enabled: bool,
    /// Discover skills under `<project>/.ocg/skills`.
    pub project_skills: bool,
    /// Discover skills under the OCG configuration directory.
    pub user_skills: bool,
    /// Extra directories to scan, in addition to the two standard locations.
    pub paths: Vec<String>,
    /// Cap on the catalog, in characters.
    pub max_catalog_chars: usize,
    /// Cap on one skill body, in characters.
    pub max_body_chars: usize,
    /// Cap on all activated bodies together, in characters.
    pub max_active_body_chars: usize,
    /// Cap on supporting files listed per skill.
    pub max_resource_files: usize,
    /// Cap on simultaneously active skills.
    pub max_active_skills: usize,
    /// Require a skill's name to match its directory name.
    ///
    /// On by default. A mismatch means two directories can claim one identity,
    /// and identity is what re-materialization after compaction depends on.
    pub require_directory_name: bool,
}

impl Default for SkillsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            project_skills: true,
            user_skills: true,
            paths: Vec::new(),
            max_catalog_chars: DEFAULT_MAX_CATALOG_CHARS,
            max_body_chars: DEFAULT_MAX_BODY_CHARS,
            max_active_body_chars: DEFAULT_MAX_ACTIVE_BODY_CHARS,
            max_resource_files: DEFAULT_MAX_RESOURCE_FILES,
            max_active_skills: DEFAULT_MAX_ACTIVE_SKILLS,
            require_directory_name: true,
        }
    }
}

impl SkillsConfig {
    /// Parse the nested JSON object. Absent or null yields the defaults.
    pub fn from_value(value: &Value) -> Result<Self> {
        let Some(object) = value.as_object() else {
            if value.is_null() {
                return Ok(Self::default());
            }
            return Err(invalid_err("must be an object"));
        };
        let mut config = Self::default();
        if let Some(value) = object.get("enabled") {
            config.enabled = value
                .as_bool()
                .ok_or_else(|| invalid_err("enabled must be a boolean"))?;
        }
        for (key, slot) in [
            ("projectSkills", &mut config.project_skills),
            ("userSkills", &mut config.user_skills),
            ("requireDirectoryName", &mut config.require_directory_name),
        ] {
            if let Some(value) = object.get(key) {
                *slot = value
                    .as_bool()
                    .ok_or_else(|| invalid_err(format!("{key} must be a boolean")))?;
            }
        }
        if let Some(value) = object.get("paths") {
            config.paths = parse_paths(value)?;
        }
        for (key, slot) in [
            ("maxCatalogChars", &mut config.max_catalog_chars),
            ("maxBodyChars", &mut config.max_body_chars),
            ("maxActiveBodyChars", &mut config.max_active_body_chars),
        ] {
            if let Some(value) = object.get(key) {
                *slot = parse_positive_usize(value, key)?;
            }
        }
        if let Some(value) = object.get("maxResourceFiles") {
            config.max_resource_files = parse_positive_usize(value, "maxResourceFiles")?;
        }
        if let Some(value) = object.get("maxActiveSkills") {
            config.max_active_skills = parse_positive_usize(value, "maxActiveSkills")?;
        }
        config.validate_values()?;
        Ok(config)
    }

    /// Parse `data["skills"]`.
    pub fn from_config(data: &Value) -> Result<Self> {
        let value = data.get("skills").unwrap_or(&Value::Null);
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
        if self.max_catalog_chars > MAX_CATALOG_CHARS_CEILING {
            return Err(invalid_err(format!(
                "maxCatalogChars must not exceed {MAX_CATALOG_CHARS_CEILING}"
            )));
        }
        if self.max_body_chars > MAX_BODY_CHARS_CEILING {
            return Err(invalid_err(format!(
                "maxBodyChars must not exceed {MAX_BODY_CHARS_CEILING}"
            )));
        }
        if self.max_active_body_chars < self.max_body_chars {
            return Err(invalid_err(
                "maxActiveBodyChars must not be below maxBodyChars; one skill must always fit",
            ));
        }
        for path in &self.paths {
            validate_relative_dir(path)?;
        }
        Ok(())
    }

    /// A stable fingerprint of the policy.
    pub fn fingerprint(&self) -> String {
        let value = serde_json::to_value(self).unwrap_or(Value::Null);
        crate::hash::sha256_hex(value.to_string().as_bytes())
    }
}

/// Validate a skill name.
///
/// The rules are the `SKILL.md` convention's, kept because identity depends on
/// them: a name is the re-materialization key after compaction, so two skills
/// that normalize to one name would silently share a body.
pub fn validate_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(invalid_err("a skill name must not be empty"));
    }
    if name.chars().count() > MAX_NAME_CHARS {
        return Err(invalid_err(format!(
            "a skill name must be at most {MAX_NAME_CHARS} characters"
        )));
    }
    if name.starts_with('-') || name.ends_with('-') || name.contains("--") {
        return Err(invalid_err(
            "a skill name must not start or end with '-' or contain '--'",
        ));
    }
    if !name
        .chars()
        .all(|character| character.is_ascii_lowercase() || character.is_ascii_digit() || character == '-')
    {
        return Err(invalid_err(
            "a skill name may contain only lowercase letters, digits and '-'",
        ));
    }
    Ok(())
}

/// Validate a skill description length.
pub fn validate_description(description: &str) -> Result<()> {
    if description.trim().is_empty() {
        return Err(invalid_err("a skill description must not be empty"));
    }
    if description.chars().count() > MAX_DESCRIPTION_CHARS {
        return Err(invalid_err(format!(
            "a skill description must be at most {MAX_DESCRIPTION_CHARS} characters"
        )));
    }
    Ok(())
}

/// An extra skill directory must be relative and must not climb.
///
/// Configured paths are held to the same rule as discovered ones. An absolute or
/// `..`-bearing path would let a configuration file pull a skill from outside the
/// directories OCG was asked to scan.
fn validate_relative_dir(path: &str) -> Result<()> {
    if path.is_empty() {
        return Err(invalid_err("skills.paths must not contain an empty path"));
    }
    if path.starts_with('/') || path.starts_with('\\') {
        return Err(invalid_err(format!(
            "skills.paths entries must be relative, but '{path}' is absolute"
        )));
    }
    if path.contains('\0') {
        return Err(invalid_err("skills.paths must not contain a NUL"));
    }
    if path
        .split(['/', '\\'])
        .any(|segment| segment == "..")
    {
        return Err(invalid_err(format!(
            "skills.paths entries must not escape upwards, but '{path}' contains '..'"
        )));
    }
    Ok(())
}

fn parse_paths(value: &Value) -> Result<Vec<String>> {
    let array = value
        .as_array()
        .ok_or_else(|| invalid_err("paths must be an array of strings"))?;
    let paths: Vec<String> = array
        .iter()
        .map(|entry| {
            entry
                .as_str()
                .map(|path| path.to_string())
                .ok_or_else(|| invalid_err("paths must be an array of strings"))
        })
        .collect::<Result<Vec<String>>>()?;
    for path in &paths {
        validate_relative_dir(path)?;
    }
    Ok(paths)
}

fn invalid_err(message: impl std::fmt::Display) -> OcgError {
    OcgError::config(format!("skills.{message}"))
}

fn parse_positive_usize(value: &Value, label: &str) -> Result<usize> {
    value
        .as_u64()
        .filter(|number| *number > 0)
        .map(|number| number as usize)
        .ok_or_else(|| invalid_err(format!("{label} must be a positive integer")))
}
