//! Discovery of the `AGENTS.md` chain, and the resolution of that chain into an
//! ordered, scoped, provenance-carrying instruction set.
//!
//! ## Discovery
//!
//! ```text
//! working directory
//!      │  walk upward, one directory at a time
//!      ▼
//! project root        ← the walk stops here, always
//! ```
//!
//! The project root is OCG's existing [`ProjectBoundary`], not a marker invented
//! here. Instruction discovery has no authority of its own: it cannot decide
//! where a project begins, only read the files inside the boundary that
//! [`crate::project`] already established.
//!
//! Two properties of the walk are worth stating because both are load-bearing:
//!
//! * **It stops at the project root.** A parent directory above the boundary is
//!   not part of this project, and its `AGENTS.md` — if it has one — belongs to
//!   whatever project that directory actually is. Reading it would let an
//!   unrelated ancestor's instructions silently govern this project.
//!
//! * **It walks upward, then emits root-first.** The walk direction is an
//!   implementation detail; the *emission* order is the precedence rule. A file
//!   found closer to the working directory is emitted later, and later text is
//!   what a model reads as the more specific refinement.
//!
//! ## Symlinks and boundary escape
//!
//! Every candidate path is canonicalized before it is compared to the root, and
//! a canonicalized path outside the root is rejected. Comparing the spelling
//! instead would let `dir/link -> /elsewhere` place an instruction file from
//! outside the project inside the project's instruction chain, which is the
//! filesystem-boundary escape this module exists to prevent.
//!
//! ## One file per directory
//!
//! The first configured filename found in a directory wins and the remaining
//! names are not probed for that directory. Two files in one directory would
//! make precedence unobservable to the model.

use crate::derived::{DerivedBindings, DerivedId, DerivedRevision};
use crate::error::{OcgError, Result};
use crate::instructions::config::InstructionsConfig;
use crate::project::{self, ProjectBoundary};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

/// Which boundary an instruction file was found under.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum InstructionScope {
    /// A user-level file outside any project, applying to every project.
    UserLevel,
    /// The project root's own instruction file.
    ProjectRoot,
    /// A file in a directory strictly between the root and the working
    /// directory. `depth` counts directories below the root, so a larger depth
    /// is a more specific scope.
    Nested { depth: usize },
}

impl InstructionScope {
    /// A stable label used in rendered provenance.
    pub fn as_str(self) -> &'static str {
        match self {
            InstructionScope::UserLevel => "user",
            InstructionScope::ProjectRoot => "project",
            InstructionScope::Nested { .. } => "directory",
        }
    }

    /// How specific this scope is. Higher wins.
    pub fn specificity(self) -> u32 {
        match self {
            InstructionScope::UserLevel => 0,
            InstructionScope::ProjectRoot => 1,
            InstructionScope::Nested { depth } => 1_000 + depth as u32,
        }
    }
}

/// One discovered instruction file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InstructionFile {
    /// Project-relative path with `/` separators, or the absolute path for a
    /// user-level file that has no project-relative spelling.
    pub source: String,
    /// Absolute path, as canonicalized during discovery.
    pub path: PathBuf,
    pub scope: InstructionScope,
    /// Bytes on disk.
    pub bytes: u64,
    /// Retained content, already bounded by the per-file cap.
    pub content: String,
    /// Whether `content` is shorter than the file on disk.
    pub truncated: bool,
    /// Digest of `content`, so an edit that does not change the retained bytes
    /// does not invalidate anything.
    pub revision: DerivedRevision,
}

impl InstructionFile {
    fn retained_chars(&self) -> usize {
        self.content.chars().count()
    }
}

/// Something the discovery pass deliberately did not do.
///
/// Recorded rather than swallowed: an instruction file that exists but was
/// skipped is a fact the operator needs, because "my instructions are not being
/// applied" is otherwise indistinguishable from "I have no instructions".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscoveryNote {
    pub source: String,
    pub reason: String,
}

/// The resolved instruction set for one working directory.
///
/// Ordered lowest precedence first, so the caller can render the array in order
/// and get the correct refinement semantics for free.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InstructionSet {
    /// One project root, or `None` when discovery was disabled.
    pub root: Option<PathBuf>,
    /// Files in precedence order, lowest first.
    pub files: Vec<InstructionFile>,
    /// Files that existed but were not included, with the reason.
    pub skipped: Vec<DiscoveryNote>,
}

impl InstructionSet {
    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }

    pub fn len(&self) -> usize {
        self.files.len()
    }

    /// Total retained characters across the chain.
    pub fn chars(&self) -> usize {
        self.files.iter().map(InstructionFile::retained_chars).sum()
    }

    /// The chain's combined revision.
    pub fn revision(&self) -> DerivedRevision {
        let parts: Vec<Vec<u8>> = self
            .files
            .iter()
            .map(|file| file.content.as_bytes().to_vec())
            .collect();
        let borrowed: Vec<&[u8]> = parts.iter().map(|part| part.as_slice()).collect();
        DerivedRevision::of_parts(borrowed)
    }

    /// One binding per file, for durable comparison against a later request.
    pub fn bindings(&self) -> DerivedBindings {
        let mut bindings = DerivedBindings::new();
        for file in &self.files {
            bindings.bind(crate::derived::DerivedBinding::new(
                DerivedId::instruction_file(&file.source),
                crate::derived::DerivedKind::InstructionFile,
                file.revision.clone(),
                file.source.clone(),
                file.retained_chars(),
            ));
        }
        bindings
    }

    /// The most specific file that governs `path`.
    ///
    /// Used to decide whether a nested instruction file applies to the file
    /// being worked on. Returns `None` when no discovered scope contains the
    /// path, which is a real answer: a path outside the project has no project
    /// instructions.
    pub fn governing(&self, path: &Path) -> Option<&InstructionFile> {
        let canonical = resolve_query(path);
        self.files.iter().rev().find(|file| match file.scope {
            InstructionScope::UserLevel => true,
            _ => file
                .path
                .parent()
                .is_some_and(|directory| canonical.starts_with(directory)),
        })
    }

    /// Every file whose directory contains `path`, in precedence order.
    pub fn applicable_to(&self, path: &Path) -> Vec<&InstructionFile> {
        let canonical = resolve_query(path);
        self.files
            .iter()
            .filter(|file| match file.scope {
                InstructionScope::UserLevel => true,
                _ => file
                    .path
                    .parent()
                    .is_some_and(|directory| canonical.starts_with(directory)),
            })
            .collect()
    }
}

/// Discover the instruction chain for `cwd` inside `boundary`.
///
/// `user_level` is the path of a user-level instruction file, already resolved
/// by the caller. It is a parameter rather than something looked up here because
/// the user config location is owned by [`crate::config`]; this module has no
/// opinion about where a user's own configuration lives.
///
/// Discovery is fail-soft by design. A file that cannot be read produces a
/// [`DiscoveryNote`], not an error, because instruction discovery is never the
/// reason a request should fail.
pub fn discover(
    boundary: &ProjectBoundary,
    cwd: &Path,
    config: &InstructionsConfig,
    user_level: Option<&Path>,
) -> InstructionSet {
    let mut set = InstructionSet {
        root: Some(boundary.root().to_path_buf()),
        ..InstructionSet::default()
    };
    if !config.enabled {
        set.root = None;
        return set;
    }

    let root = project::canonicalize(boundary.root());
    let cwd = project::canonicalize(cwd);

    if config.user_level {
        if let Some(path) = user_level {
            match load(path, &root, InstructionScope::UserLevel, config, &mut set) {
                Ok(Some(file)) => set.files.push(file),
                Ok(None) => {}
                Err(note) => set.skipped.push(note),
            }
        }
    }

    // Collect the directories from cwd up to the root, then reverse so the
    // chain is emitted root-first. The depth cap bounds the walk independently
    // of the boundary check, so a caller that supplies an unrelated `cwd`
    // degrades to "no instructions" instead of walking to the filesystem root.
    let mut directories: Vec<PathBuf> = Vec::new();
    let mut current = cwd.clone();
    let mut steps = 0usize;
    while current.starts_with(&root) {
        if steps >= config.max_depth {
            set.skipped.push(DiscoveryNote {
                source: display_path(&current, &root),
                reason: format!(
                    "the upward walk stopped at the configured depth limit of {}",
                    config.max_depth
                ),
            });
            break;
        }
        directories.push(current.clone());
        if current == root {
            break;
        }
        match current.parent() {
            Some(parent) => current = parent.to_path_buf(),
            None => break,
        }
        steps += 1;
    }
    directories.reverse();

    // Depth is assigned after the reversal so it counts directories *below the
    // root*, which is what makes it comparable across working directories: a
    // file two levels under the root has depth 2 whether the walk started there
    // or ten levels beneath it.
    for (position, directory) in directories.into_iter().enumerate() {
        let scope = if directory == root {
            InstructionScope::ProjectRoot
        } else {
            InstructionScope::Nested { depth: position }
        };
        let Some(path) = first_existing(&directory, &config.filenames) else {
            continue;
        };
        match load(&path, &root, scope, config, &mut set) {
            Ok(Some(file)) => {
                if set.files.len() >= config.max_files {
                    set.skipped.push(DiscoveryNote {
                        source: file.source,
                        reason: format!(
                            "the chain already holds the configured maximum of {} files",
                            config.max_files
                        ),
                    });
                } else {
                    set.files.push(file);
                }
            }
            Ok(None) => {}
            Err(note) => set.skipped.push(note),
        }
    }

    apply_total_budget(&mut set, config);
    set
}

/// Canonicalize a path that may not exist yet.
///
/// A scope query names a file the model is *about to* touch, which usually does
/// not exist. [`project::canonicalize`] falls back to a lexical normalization in
/// that case, and a lexical result does not resolve the symlinked ancestor that
/// every real project root sits behind — on macOS `/var` is a link to
/// `/private/var`, so a lexical query never matches a canonical root and every
/// scope test silently fails. Canonicalizing the deepest existing ancestor and
/// re-appending the remainder fixes that without loosening the boundary: the
/// ancestor is the part that decides containment.
fn resolve_query(path: &Path) -> PathBuf {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map(|cwd| cwd.join(path))
            .unwrap_or_else(|_| path.to_path_buf())
    };
    let mut prefix = absolute.as_path();
    let mut suffix: Vec<std::ffi::OsString> = Vec::new();
    loop {
        if prefix.exists() {
            let canonical = project::canonicalize(prefix);
            let mut resolved = canonical;
            for segment in suffix.iter().rev() {
                resolved.push(segment);
            }
            return resolved;
        }
        match (prefix.parent(), prefix.file_name()) {
            (Some(parent), Some(name)) => {
                suffix.push(name.to_os_string());
                prefix = parent;
            }
            _ => return absolute,
        }
    }
}

/// The first configured filename that exists as a regular file.
fn first_existing(directory: &Path, filenames: &[String]) -> Option<PathBuf> {
    filenames
        .iter()
        .map(|name| directory.join(name))
        .find(|candidate| candidate.is_file())
}

/// Read one instruction file, applying the per-file cap and the boundary check.
///
/// `Ok(None)` means the path held no readable content worth projecting, which is
/// not the same as a file that does not exist: an empty `AGENTS.md` is a fact,
/// but an empty instruction has nothing to say and no reason to occupy the
/// projection.
fn load(
    path: &Path,
    root: &Path,
    scope: InstructionScope,
    config: &InstructionsConfig,
    set: &mut InstructionSet,
) -> std::result::Result<Option<InstructionFile>, DiscoveryNote> {
    let canonical = project::canonicalize(path);
    if scope != InstructionScope::UserLevel && !canonical.starts_with(root) {
        return Err(DiscoveryNote {
            source: display_path(path, root),
            reason: "the path resolves outside the project root and was rejected".to_string(),
        });
    }
    let bytes = match fs::read(&canonical) {
        Ok(bytes) => bytes,
        Err(error) => {
            return Err(DiscoveryNote {
                source: display_path(&canonical, root),
                reason: format!("the file could not be read: {error}"),
            })
        }
    };
    let total = bytes.len();
    let retained = &bytes[..total.min(config.max_file_bytes)];
    let truncated = total > config.max_file_bytes;
    let content = String::from_utf8_lossy(retained).into_owned();
    if content.trim().is_empty() {
        return Ok(None);
    }
    let revision = DerivedRevision::of(content.as_bytes());
    let source = display_path(&canonical, root);
    let _ = set;
    Ok(Some(InstructionFile {
        source,
        path: canonical,
        scope,
        bytes: total as u64,
        content,
        truncated,
        revision,
    }))
}

/// Enforce the whole-chain budget, dropping the least specific files first.
///
/// The order matters. A root file is inherited by everything below it, so it is
/// the cheapest thing to lose; a nested file is the only statement of the rules
/// for its own subtree. Truncation within a file is worse than dropping a whole
/// low-precedence file, because it leaves an instruction that ends mid-sentence
/// and looks complete.
fn apply_total_budget(set: &mut InstructionSet, config: &InstructionsConfig) {
    let mut total: usize = set.files.iter().map(|file| file.content.len()).sum();
    if total <= config.max_total_bytes {
        return;
    }
    let order: Vec<usize> = (0..set.files.len()).collect();
    for index in order {
        if total <= config.max_total_bytes {
            break;
        }
        let source = set.files[index].source.clone();
        let size = set.files[index].content.len();
        set.skipped.push(DiscoveryNote {
            source,
            reason: format!(
                "the chain exceeded maxTotalBytes of {} and this is the least specific file",
                config.max_total_bytes
            ),
        });
        total = total.saturating_sub(size);
        set.files.remove(index);
    }
}

/// A project-relative spelling with `/` separators, falling back to the absolute
/// path.
fn display_path(path: &Path, root: &Path) -> String {
    path.strip_prefix(root)
        .map(|relative| relative.to_string_lossy().replace('\\', "/"))
        .unwrap_or_else(|_| path.to_string_lossy().replace('\\', "/"))
}

/// Reject a candidate that a caller proposed directly rather than discovered.
///
/// Exposed so an integration point that accepts a caller-supplied instruction
/// path can apply the same boundary rule the walk applies, instead of
/// re-deriving a weaker one.
pub fn require_inside_root(root: &Path, path: &Path) -> Result<()> {
    let canonical = project::canonicalize(path);
    if canonical.starts_with(project::canonicalize(root)) {
        Ok(())
    } else {
        Err(OcgError::config(format!(
            "{} resolves outside the project root and cannot be used as a project instruction",
            path.display()
        )))
    }
}
