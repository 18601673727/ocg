//! The Skill package: metadata, instructions, supporting files, declared
//! dependencies.
//!
//! ## The division of responsibility
//!
//! ```text
//! AGENTS.md  = project- and directory-scoped standing instructions
//! Skill      = a discoverable, selectable capability description plus resources
//! Native Tool= something OCG actually executes
//! ```
//!
//! A Skill is **not** an execution authority. It contains prose and files. It
//! cannot call [`crate::native_tools`] functions, grant a
//! [`crate::capabilities::Capability`], or widen a permission. What it *can* do
//! is declare, in `required_tools`, that it expects certain tools to be present
//! — and that declaration is checked and reported, never assumed. A skill whose
//! declared tools are missing from the request is a diagnostic for the caller,
//! because silently dropping the requirement would produce a skill that
//! instructs the model to use a tool that is not there.
//!
//! ## Metadata is the interface
//!
//! Only `name` and `description` are read from the frontmatter. Unknown keys are
//! ignored rather than rejected, so a package written for a richer spec still
//! loads; the fields OCG does not understand cannot change its behaviour.
//!
//! `name` and `description` are the whole catalog. `description` is not
//! documentation — it is the selection surface, which is why it has a hard
//! length cap in [`crate::skills::config`] and why the catalog is budgeted
//! separately from bodies.
//!
//! ## Identity
//!
//! A skill's identity is its `name`, and re-materialization after compaction is
//! by `name` plus `revision`. That is why the directory-name check exists: two
//! directories claiming one name would make the identity ambiguous, and an
//! ambiguous identity cannot be re-materialized.

use crate::derived::DerivedRevision;
use crate::error::{OcgError, Result};
use crate::skills::config::{self, SKILL_FILENAME};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Where a skill was found.
///
/// The variant order *is* the precedence order: a later variant overrides an
/// earlier one with the same name. Project-local beats user-global, because a
/// project author is closer to the work than a user-level default; user-global
/// beats built-in, because an installed default is the weakest claim.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SkillSource {
    /// Compiled into OCG.
    BuiltIn,
    /// Under the user configuration directory.
    UserGlobal,
    /// Under `<project>/.ocg/skills`.
    ProjectLocal,
    /// Under an explicitly configured directory.
    Configured(String),
}

impl SkillSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            SkillSource::BuiltIn => "builtIn",
            SkillSource::UserGlobal => "user",
            SkillSource::ProjectLocal => "project",
            SkillSource::Configured(_) => "configured",
        }
    }

    /// A human-readable provenance label.
    pub fn describe(&self) -> String {
        match self {
            SkillSource::Configured(path) => format!("configured:{path}"),
            other => other.as_str().to_string(),
        }
    }
}

/// One skill's metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillMetadata {
    pub name: String,
    pub description: String,
    /// Declared tool dependencies. A declaration, not a grant.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_tools: Vec<String>,
    /// Declared OCG capabilities this skill expects. Also a declaration.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_capabilities: Vec<String>,
    /// Free-form key/values carried through for the caller's diagnostics.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extra: std::collections::BTreeMap<String, String>,
}

/// A supporting file inside a skill package.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillResource {
    /// Path relative to the skill directory, with `/` separators.
    pub path: String,
    pub bytes: u64,
}

/// A discovered, validated skill package.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillDefinition {
    pub metadata: SkillMetadata,
    /// The instruction body: `SKILL.md` with its frontmatter removed.
    pub instructions: String,
    /// Digest of the *rendered* body, so a whitespace-only edit does not
    /// invalidate anything.
    pub revision: DerivedRevision,
    /// Directory holding the package. `None` for a built-in.
    pub base_dir: Option<PathBuf>,
    /// The `SKILL.md` path, or `None` for a built-in.
    pub manifest_path: Option<PathBuf>,
    pub source: SkillSource,
    /// Supporting files, sorted by path so the listing is deterministic.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resources: Vec<SkillResource>,
    /// Whether the body was cut at the configured cap.
    #[serde(default)]
    pub truncated: bool,
}

impl SkillDefinition {
    /// A built-in skill, compiled in rather than read.
    pub fn built_in(
        name: &str,
        description: &str,
        instructions: &str,
        required_tools: Vec<String>,
    ) -> Result<Self> {
        config::validate_name(name)?;
        config::validate_description(description)?;
        let metadata = SkillMetadata {
            name: name.to_string(),
            description: description.to_string(),
            required_tools,
            required_capabilities: Vec::new(),
            extra: Default::default(),
        };
        let revision = DerivedRevision::of(instructions.as_bytes());
        Ok(Self {
            metadata,
            instructions: instructions.to_string(),
            revision,
            base_dir: None,
            manifest_path: None,
            source: SkillSource::BuiltIn,
            resources: Vec::new(),
            truncated: false,
        })
    }

    /// The skill's identity for re-materialization.
    pub fn id(&self) -> crate::derived::DerivedId {
        crate::derived::DerivedId::skill(&self.metadata.name)
    }

    /// Whether this skill's base directory is inside `root`.
    ///
    /// A configured skill directory that resolves outside the project is not a
    /// project skill. Checking the *canonicalized* directory rather than the
    /// configured spelling is what makes a symlinked path unable to smuggle a
    /// package in from elsewhere.
    pub fn is_inside(&self, root: &Path) -> bool {
        match &self.base_dir {
            None => false,
            Some(base) => {
                crate::project::canonicalize(base).starts_with(crate::project::canonicalize(root))
            }
        }
    }

    /// The digest of everything a request would carry for this skill.
    ///
    /// Metadata and body together, so a changed description invalidates the
    /// catalog entry and a changed body invalidates the activation — the two are
    /// projected at different times and must not share a revision.
    pub fn catalog_revision(&self) -> DerivedRevision {
        DerivedRevision::of_parts([
            self.metadata.name.as_bytes(),
            self.metadata.description.as_bytes(),
        ])
    }
}

/// Split YAML frontmatter from a body.
///
/// Returns `None` when the text has no frontmatter, which is legal: a skill with
/// no metadata is a skill OCG cannot put in a catalog, and the caller reports
/// that as a rejection rather than inventing a name from the directory.
pub fn split_frontmatter(text: &str) -> Option<(String, String)> {
    let text = text.strip_prefix('\u{feff}').unwrap_or(text);
    let rest = text.strip_prefix("---")?;
    // The opening delimiter must be a line of its own, otherwise a body that
    // merely begins with a dash-dash-dash would be misread as frontmatter.
    let rest = rest
        .strip_prefix('\n')
        .or_else(|| rest.strip_prefix("\r\n"))?;
    let end = rest
        .find("\n---")
        .map(|index| (index, 4))
        .or_else(|| rest.find("\r\n---").map(|index| (index, 5)))?;
    let (front, remainder) = rest.split_at(end.0);
    let after = &remainder[end.1..];
    let body = after
        .strip_prefix('\n')
        .or_else(|| after.strip_prefix("\r\n"))
        .unwrap_or(after);
    Some((
        front.to_string(),
        body.trim_start_matches(['\r', '\n']).to_string(),
    ))
}

/// Read the frontmatter fields OCG understands.
///
/// Unknown keys are ignored, which is deliberate: a package authored against a
/// richer specification must still load here, and a field OCG cannot interpret
/// cannot change what OCG does.
pub fn parse_metadata(front: &str) -> SkillMetadata {
    let value: serde_json::Value =
        serde_yaml_ng::from_str(front).unwrap_or(serde_json::Value::Null);
    let object = match value.as_object() {
        Some(object) => object,
        None => {
            return SkillMetadata {
                name: String::new(),
                description: String::new(),
                required_tools: Vec::new(),
                required_capabilities: Vec::new(),
                extra: Default::default(),
            }
        }
    };
    let mut extra = std::collections::BTreeMap::new();
    for (key, entry) in object {
        if matches!(
            key.as_str(),
            "name" | "description" | "required-tools" | "requiredTools"
        ) {
            continue;
        }
        if let Some(text) = entry.as_str() {
            extra.insert(key.clone(), text.to_string());
        }
    }
    SkillMetadata {
        name: string_field(object, "name"),
        description: string_field(object, "description"),
        required_tools: list_field(object, "required-tools", "requiredTools"),
        required_capabilities: list_field(object, "required-capabilities", "requiredCapabilities"),
        extra,
    }
}

fn string_field(object: &serde_json::Map<String, serde_json::Value>, key: &str) -> String {
    object
        .get(key)
        .and_then(|value| value.as_str())
        .unwrap_or_default()
        .trim()
        .to_string()
}

fn list_field(
    object: &serde_json::Map<String, serde_json::Value>,
    primary: &str,
    alternate: &str,
) -> Vec<String> {
    let value = object.get(primary).or_else(|| object.get(alternate));
    match value {
        // A space-separated string is the documented form for `allowed-tools`
        // style fields, and is accepted for the same reason.
        Some(serde_json::Value::String(text)) => text
            .split_whitespace()
            .map(|entry| entry.to_string())
            .collect(),
        Some(serde_json::Value::Array(items)) => items
            .iter()
            .filter_map(|item| item.as_str())
            .map(|item| item.trim().to_string())
            .filter(|item| !item.is_empty())
            .collect(),
        _ => Vec::new(),
    }
}

/// Why a discovered package was not registered.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillRejection {
    pub path: String,
    pub reason: String,
}

/// Read and validate one skill package.
///
/// `boundary_root` is the project root; a package whose directory resolves
/// outside it is rejected when the source is project-local. `None` disables that
/// check, which is only correct for user-global and built-in sources.
pub fn load(
    manifest: &Path,
    source: SkillSource,
    policy: &config::SkillsConfig,
    boundary_root: Option<&Path>,
) -> std::result::Result<SkillDefinition, SkillRejection> {
    let reject = |path: &Path, reason: String| SkillRejection {
        path: path.to_string_lossy().to_string(),
        reason,
    };
    let base_dir = manifest.parent().unwrap_or(manifest).to_path_buf();
    if let Some(root) = boundary_root {
        if !crate::project::canonicalize(&base_dir).starts_with(crate::project::canonicalize(root))
        {
            return Err(reject(
                manifest,
                "the package directory resolves outside the project root".to_string(),
            ));
        }
    }
    let text = std::fs::read_to_string(manifest)
        .map_err(|error| reject(manifest, format!("could not be read: {error}")))?;
    let Some((front, body)) = split_frontmatter(&text) else {
        return Err(reject(
            manifest,
            format!("{SKILL_FILENAME} has no YAML frontmatter, so it has no name to be catalogued under"),
        ));
    };
    let mut metadata = parse_metadata(&front);
    config::validate_name(&metadata.name).map_err(|error| reject(manifest, strip_prefix(error)))?;
    config::validate_description(&metadata.description)
        .map_err(|error| reject(manifest, strip_prefix(error)))?;
    if policy.require_directory_name {
        if let Some(directory) = base_dir.file_name().and_then(|name| name.to_str()) {
            if directory != metadata.name {
                metadata
                    .extra
                    .insert("directoryNameMismatch".to_string(), directory.to_string());
                return Err(reject(
                    manifest,
                    format!(
                        "the skill is named '{}' but its directory is '{directory}'; the name is the re-materialization key after compaction, so it must match",
                        metadata.name
                    ),
                ));
            }
        }
    }
    let (instructions, truncated) = bound(&body, policy.max_body_chars);
    let resources = list_resources(&base_dir, policy.max_resource_files);
    let revision = DerivedRevision::of(instructions.as_bytes());
    Ok(SkillDefinition {
        metadata,
        instructions,
        revision,
        base_dir: Some(base_dir),
        manifest_path: Some(manifest.to_path_buf()),
        source,
        resources,
        truncated,
    })
}

/// Cut a body at the cap, on a character boundary and at a line boundary where
/// one is available.
///
/// Cutting mid-line would leave an instruction that reads as complete, so the
/// last newline inside the retained region is preferred.
fn bound(body: &str, cap: usize) -> (String, bool) {
    if body.chars().count() <= cap {
        return (body.to_string(), false);
    }
    let cut: String = body.chars().take(cap).collect();
    let end = cut.rfind('\n').map(|index| index + 1).unwrap_or(cut.len());
    (cut[..end].to_string(), true)
}

/// List supporting files, excluding the manifest itself.
///
/// Sorted by path so two runs over the same tree produce the same listing. The
/// listing is a *pointer*, not content: the model reads a resource with the
/// normal read tool, which keeps resource bytes out of every request.
fn list_resources(base: &Path, limit: usize) -> Vec<SkillResource> {
    let mut found = Vec::new();
    collect(base, base, limit, &mut found);
    found.sort_by(|left, right| left.path.cmp(&right.path));
    found.truncate(limit);
    found
}

fn collect(base: &Path, directory: &Path, limit: usize, out: &mut Vec<SkillResource>) {
    if out.len() >= limit {
        return;
    }
    let Ok(entries) = std::fs::read_dir(directory) else {
        return;
    };
    let mut names: Vec<PathBuf> = entries
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .collect();
    names.sort();
    for path in names {
        if out.len() >= limit {
            return;
        }
        let Ok(metadata) = std::fs::symlink_metadata(&path) else {
            continue;
        };
        if metadata.file_type().is_symlink() {
            // A symlinked resource could point anywhere, including outside the
            // package. It is not listed; the model can still read it by name if
            // it asks, and the read goes through the normal boundary check.
            continue;
        }
        if metadata.is_dir() {
            collect(base, &path, limit, out);
            continue;
        }
        let relative = path
            .strip_prefix(base)
            .map(|relative| relative.to_string_lossy().replace('\\', "/"))
            .unwrap_or_default();
        if relative.is_empty() || relative == SKILL_FILENAME {
            continue;
        }
        out.push(SkillResource {
            path: relative,
            bytes: metadata.len(),
        });
    }
}

/// `OcgError::config` messages carry the `skills.` prefix added by the policy
/// layer; a rejection reason reads better without it.
fn strip_prefix(error: OcgError) -> String {
    let text = error.to_string();
    text.strip_prefix("skills.").unwrap_or(&text).to_string()
}

/// Report declared dependencies that the request does not carry.
///
/// This is a diagnostic, never an authorization decision. It exists so a caller
/// can see that a skill was activated for a tool that is not on the request,
/// instead of discovering it from a model error three turns later.
pub fn unsatisfied_dependencies(
    definition: &SkillDefinition,
    available_tools: &[String],
    available_capabilities: &[String],
) -> Vec<String> {
    let mut missing = Vec::new();
    for tool in &definition.metadata.required_tools {
        if !available_tools.iter().any(|available| available == tool) {
            missing.push(format!("tool:{tool}"));
        }
    }
    for capability in &definition.metadata.required_capabilities {
        if !available_capabilities
            .iter()
            .any(|available| available == capability)
        {
            missing.push(format!("capability:{capability}"));
        }
    }
    missing
}

/// Resolve a resource path inside a skill package.
///
/// Rejects `..`, absolute paths, and anything that canonicalizes outside the
/// package directory. The model supplies these paths, so the check is on the
/// resolved result and not on the spelling.
pub fn resolve_resource(base: &Path, relative: &str) -> Result<PathBuf> {
    if relative.is_empty() || relative.contains('\0') {
        return Err(OcgError::config("a skill resource path must not be empty"));
    }
    let candidate = Path::new(relative);
    if candidate.is_absolute() {
        return Err(OcgError::config(format!(
            "a skill resource path must be relative to the skill directory, but '{relative}' is absolute"
        )));
    }
    let base = crate::project::canonicalize(base);
    let resolved = crate::project::canonicalize(&base.join(candidate));
    if !resolved.starts_with(&base) {
        return Err(OcgError::config(format!(
            "'{relative}' resolves outside the skill directory"
        )));
    }
    Ok(resolved)
}
