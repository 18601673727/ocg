//! Deterministic project-boundary resolution.
//!
//! A project is *initialized* by the presence of an OCG state directory
//! (`.ocg/`) in a directory. The project root is the nearest
//! ancestor of the invocation directory that contains the marker. Every piece
//! of project-scoped state (context index/cache, orchestration state,
//! checkpoints, verification logs and local telemetry)
//! is placed under that resolved root, never under an arbitrary working
//! directory. Two sibling projects therefore can never share state, and a
//! parent directory that is not itself a project can never become a boundary
//! for a child project.
//!
//! Resolution is purely local and deterministic: the filesystem is walked
//! upwards and paths are canonicalized before any comparison. No workspace
//! database, registry or cache is introduced. There is no global fallback that
//! could merge unrelated projects.

use crate::error::{OcgError, Result};
use std::path::{Component, Path, PathBuf};

/// The state directory marker that initializes a project boundary.
pub const MARKER: &str = ".ocg";

/// The resolved project boundary for one invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectBoundary {
    root: PathBuf,
    /// A marker was found at `root` or at a nearest ancestor.
    marker: bool,
    /// The caller named the boundary explicitly (`--project` / `--cwd`).
    explicit: bool,
}

impl ProjectBoundary {
    /// The directory that owns all project-scoped state.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Whether an `.ocg` marker defines this boundary.
    pub fn has_marker(&self) -> bool {
        self.marker
    }

    /// Whether the caller named this boundary explicitly.
    pub fn is_explicit(&self) -> bool {
        self.explicit
    }

    /// Whether project-scoped state may be placed at [`Self::root`]. An
    /// explicit `--project` declaration always allows it; otherwise a marker
    /// must have been found by the ancestor walk.
    pub fn allows_project_state(&self) -> bool {
        self.marker || self.explicit
    }

    /// The project-local OCG state directory inside the boundary.
    pub fn config_path(&self) -> PathBuf {
        self.root.join(MARKER)
    }

    /// Reject an operation that needs a declared boundary when the invocation
    /// sits outside any initialized project.
    pub fn require(&self, invocation_dir: &Path) -> Result<()> {
        if self.allows_project_state() {
            return Ok(());
        }
        Err(OcgError::config(format!(
            "this command needs an initialized project: no {MARKER} was found in {} or any parent directory\nrun `mkdir .ocg` in the project root, or pass --project DIR",
            invocation_dir.display()
        )))
    }
}

/// Canonicalize a path before it takes part in any comparison.
///
/// Symlinked working directories and `..` segments must resolve to the same
/// boundary, otherwise a second state directory could silently appear for one
/// project. When the path cannot be canonicalized (for example a platform
/// where it does not exist yet) a lexical normalization is used so the result
/// stays deterministic and absolute.
pub fn canonicalize(path: &Path) -> PathBuf {
    match path.canonicalize() {
        Ok(canonical) => canonical,
        Err(_) => normalize_lexically(path),
    }
}

fn normalize_lexically(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                if !out.pop() {
                    out.push("..");
                }
            }
            other => out.push(other.as_os_str()),
        }
    }
    if out.as_os_str().is_empty() {
        out.push(".");
    }
    out
}

/// Resolve the nearest ancestor of `start` that contains the marker, or fall
/// back to the canonical `start` directory with no marker.
pub fn resolve(start: &Path) -> ProjectBoundary {
    let start = canonicalize(start);
    let mut current = start.clone();
    loop {
        if current.join(MARKER).is_dir() {
            return ProjectBoundary {
                root: current,
                marker: true,
                explicit: false,
            };
        }
        match current.parent() {
            Some(parent) => current = parent.to_path_buf(),
            None => break,
        }
    }
    ProjectBoundary {
        root: start,
        marker: false,
        explicit: false,
    }
}

/// Use an explicitly named root as the boundary. The directory does not need a
/// marker: naming it is itself the declaration. The root is still
/// canonicalized so a symlinked spelling cannot create a second boundary.
pub fn explicit(root: &Path) -> ProjectBoundary {
    let root = canonicalize(root);
    let marker = root.join(MARKER).is_dir();
    ProjectBoundary {
        root,
        marker,
        explicit: true,
    }
}
