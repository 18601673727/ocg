//! The Skill registry: discovery, precedence, and collision handling.
//!
//! ## Precedence
//!
//! ```text
//! BuiltIn        registered first, so a project skill can override it
//! UserGlobal     from the OCG configuration directory
//! ProjectLocal   from <project>/.ocg/skills
//! Configured     from explicitly configured directories, in order
//! ```
//!
//! A later source replaces an earlier one with the same name, and the replaced
//! definition is *recorded* rather than dropped silently. A project that shadows
//! a user-level skill is an intentional act; a project that shadows one by
//! accident should be visible.
//!
//! ## Determinism
//!
//! Directory entries are sorted before they are read, so two runs over the same
//! tree produce the same registry. Without that, two skills in one directory
//! could win or lose a name collision by filesystem order.
//!
//! ## Discovery is fail-soft and bounded
//!
//! A package that fails validation becomes a [`SkillRejection`], never an error:
//! one malformed skill must not stop a session. The scan is bounded in both
//! directory depth and package count, and exceeding either is recorded.

use crate::derived::{DerivedBindings, DerivedId, DerivedKind, DerivedRevision};
use crate::error::Result;
use crate::skills::config::{self, SkillsConfig, MAX_SKILLS};
use crate::skills::definition::{self, SkillDefinition, SkillRejection, SkillSource};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// One name claimed by more than one source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillCollision {
    pub name: String,
    /// The source that won.
    pub winner: String,
    /// The source that was replaced.
    pub loser: String,
}

/// Every skill OCG knows about, keyed by name.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillRegistry {
    skills: BTreeMap<String, SkillDefinition>,
    collisions: Vec<SkillCollision>,
    rejections: Vec<SkillRejection>,
    /// Whether the built-ins were registered. Recorded so a caller can tell an
    /// empty registry from a disabled one.
    pub builtins_registered: bool,
}

impl SkillRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Every skill, ordered by name.
    pub fn list(&self) -> Vec<&SkillDefinition> {
        self.skills.values().collect()
    }

    pub fn len(&self) -> usize {
        self.skills.len()
    }

    pub fn is_empty(&self) -> bool {
        self.skills.is_empty()
    }

    pub fn get(&self, name: &str) -> Option<&SkillDefinition> {
        self.skills.get(name)
    }

    pub fn collisions(&self) -> &[SkillCollision] {
        &self.collisions
    }

    pub fn rejections(&self) -> &[SkillRejection] {
        &self.rejections
    }

    /// Register a definition, applying precedence.
    ///
    /// Returns the replaced definition when one existed. A definition from a
    /// *lower*-precedence source does not replace a higher one, so scan order
    /// cannot change the outcome.
    pub fn register(&mut self, definition: SkillDefinition) -> Option<SkillDefinition> {
        let name = definition.metadata.name.clone();
        if let Some(existing) = self.skills.get(&name) {
            if existing.source >= definition.source {
                self.rejections.push(SkillRejection {
                    path: definition
                        .manifest_path
                        .as_ref()
                        .map(|path| path.to_string_lossy().to_string())
                        .unwrap_or_else(|| definition.source.describe()),
                    reason: format!(
                        "a skill named '{name}' from a higher-precedence source ({}) is already registered",
                        existing.source.describe()
                    ),
                });
                return None;
            }
            self.collisions.push(SkillCollision {
                name: name.clone(),
                winner: definition.source.describe(),
                loser: existing.source.describe(),
            });
            let replaced = self.skills.insert(name, definition);
            return replaced;
        }
        if self.skills.len() >= MAX_SKILLS {
            self.rejections.push(SkillRejection {
                path: name,
                reason: format!("the registry already holds the maximum of {MAX_SKILLS} skills"),
            });
            return None;
        }
        self.skills.insert(name, definition);
        None
    }

    /// Record a package that failed to load.
    pub fn reject(&mut self, rejection: SkillRejection) {
        if !self.rejections.contains(&rejection) {
            self.rejections.push(rejection);
        }
    }

    /// Record a name collision.
    pub fn note_collision(&mut self, collision: SkillCollision) {
        if !self.collisions.contains(&collision) {
            self.collisions.push(collision);
        }
    }

    /// The catalog revision: a digest over every skill's name and description.
    ///
    /// This is what a request records so an unchanged catalog costs nothing. It
    /// deliberately excludes bodies: a body edit must not invalidate the catalog,
    /// because the two are projected at different times.
    pub fn catalog_revision(&self) -> DerivedRevision {
        let parts: Vec<Vec<u8>> = self
            .skills
            .values()
            .map(|skill| {
                let mut buffer = Vec::new();
                buffer.extend_from_slice(skill.metadata.name.as_bytes());
                buffer.push(0);
                buffer.extend_from_slice(skill.metadata.description.as_bytes());
                buffer
            })
            .collect();
        let borrowed: Vec<&[u8]> = parts.iter().map(|part| part.as_slice()).collect();
        DerivedRevision::of_parts(borrowed)
    }

    /// The single binding a request records for the catalog.
    pub fn catalog_binding(&self) -> DerivedBindings {
        let mut bindings = DerivedBindings::new();
        bindings.bind(crate::derived::DerivedBinding::new(
            DerivedId::skill_catalog(),
            DerivedKind::SkillCatalog,
            self.catalog_revision(),
            "skill registry",
            0,
        ));
        bindings
    }

    /// A stable fingerprint of the whole registry, bodies included.
    pub fn fingerprint(&self) -> DerivedRevision {
        let parts: Vec<Vec<u8>> = self
            .skills
            .values()
            .map(|skill| {
                let mut buffer = Vec::new();
                buffer.extend_from_slice(skill.metadata.name.as_bytes());
                buffer.push(0);
                buffer.extend_from_slice(skill.instructions.as_bytes());
                buffer
            })
            .collect();
        let borrowed: Vec<&[u8]> = parts.iter().map(|part| part.as_slice()).collect();
        DerivedRevision::of_parts(borrowed)
    }
}

/// Build a registry from the filesystem.
///
/// * `project_root` is OCG's existing [`crate::project::ProjectBoundary`] root. It
///   is used for the project skill directory and as the boundary check; this
///   module does not re-derive where a project begins.
/// * `config_home` is the OCG configuration directory, used for user-global
///   skills. Resolving it is [`crate::config`]'s job.
/// * `builtins` are compiled-in skills, registered first so a project skill can
///   override them.
pub fn discover(
    project_root: Option<&Path>,
    config_home: Option<&Path>,
    builtins: &[SkillDefinition],
    policy: &SkillsConfig,
) -> SkillRegistry {
    let mut registry = SkillRegistry::new();
    if !policy.enabled {
        return registry;
    }
    for definition in builtins {
        let _ = registry.register(definition.clone());
    }
    registry.builtins_registered = !builtins.is_empty();

    if policy.user_skills {
        if let Some(home) = config_home {
            let dir = home.join(config::PROJECT_SKILL_DIR);
            scan(&dir, SkillSource::UserGlobal, None, policy, &mut registry);
        }
    }
    if policy.project_skills {
        if let Some(root) = project_root {
            let dir = root
                .join(crate::project::MARKER)
                .join(config::PROJECT_SKILL_DIR);
            scan(
                &dir,
                SkillSource::ProjectLocal,
                Some(root),
                policy,
                &mut registry,
            );
        }
    }
    for relative in &policy.paths {
        // A configured path is relative to the project root. When there is no
        // project there is nothing for it to be relative to, and guessing a base
        // would make the same configuration mean different things in different
        // directories.
        let Some(root) = project_root else {
            registry.reject(SkillRejection {
                path: relative.clone(),
                reason: "a configured skill path needs a project root to be relative to"
                    .to_string(),
            });
            continue;
        };
        let dir = root.join(relative);
        scan(
            &dir,
            SkillSource::Configured(relative.clone()),
            Some(root),
            policy,
            &mut registry,
        );
    }
    registry
}

/// Scan one directory for packages.
///
/// Two levels only. A package is a directory holding a `SKILL.md`, and nesting
/// packages inside packages is not part of the convention — a skill that needs
/// another skill's material lists it as a resource.
fn scan(
    root: &Path,
    source: SkillSource,
    boundary_root: Option<&Path>,
    policy: &SkillsConfig,
    registry: &mut SkillRegistry,
) {
    if !root.is_dir() {
        return;
    }
    let Ok(entries) = std::fs::read_dir(root) else {
        registry.reject(SkillRejection {
            path: root.to_string_lossy().to_string(),
            reason: "the skill directory could not be read".to_string(),
        });
        return;
    };
    let mut directories: Vec<PathBuf> = entries
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.is_dir())
        .collect();
    directories.sort();
    for directory in directories {
        if registry.len() >= MAX_SKILLS {
            registry.reject(SkillRejection {
                path: directory.to_string_lossy().to_string(),
                reason: format!("the registry reached the maximum of {MAX_SKILLS} skills"),
            });
            return;
        }
        let manifest = directory.join(config::SKILL_FILENAME);
        if !manifest.is_file() {
            continue;
        }
        match definition::load(&manifest, source.clone(), policy, boundary_root) {
            Ok(definition) => {
                let _ = registry.register(definition);
            }
            Err(rejection) => registry.reject(rejection),
        }
    }
}

/// Resolve a skill name a caller named explicitly.
///
/// Ambiguity is resolved rather than guessed: when the caller passes a path it
/// is used directly, and when they pass a bare name it must match exactly one
/// registered skill. Names are already unique in the registry, so the only
/// ambiguity is a caller-supplied path, which is checked against the boundary.
pub fn resolve_named<'a>(
    registry: &'a SkillRegistry,
    name: &str,
    boundary_root: Option<&Path>,
) -> Result<&'a SkillDefinition> {
    if name.contains('/') || name.contains('\\') {
        let path = Path::new(name);
        if let Some(root) = boundary_root {
            definition::resolve_resource(root, name)?;
        }
        let canonical = crate::project::canonicalize(path);
        return registry
            .list()
            .into_iter()
            .find(|skill| {
                skill
                    .manifest_path
                    .as_ref()
                    .is_some_and(|manifest| crate::project::canonicalize(manifest) == canonical)
            })
            .ok_or_else(|| {
                crate::error::OcgError::config(format!("no skill is registered at {name}"))
            });
    }
    registry
        .get(name)
        .ok_or_else(|| crate::error::OcgError::config(format!("no skill is named '{name}'")))
}
