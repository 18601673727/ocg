//! Deterministic project-boundary resolution.
//!
//! A Project is a directory registered in the OCG database; registering a root
//! is what declares it. The boundary of an invocation is the registered
//! Project whose root is the invocation directory or its nearest ancestor.
//! OCG never marks a Project on disk: nothing is written into the directory
//! to declare it, and nothing found in it is taken as a declaration. Paths are
//! canonicalized before any comparison, so a symlinked spelling resolves to
//! the same Project, and a parent directory that is not itself registered can
//! never become the boundary of a child Project.

use crate::error::{OcgError, Result};
use std::path::{Component, Path, PathBuf};

/// The resolved project boundary for one invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectBoundary {
    root: PathBuf,
    /// `root` is a registered Project.
    registered: bool,
    /// The caller named the boundary explicitly (`--project` / `--cwd`).
    explicit: bool,
}

impl ProjectBoundary {
    /// The Project's root directory.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Whether `root` is a registered Project.
    pub fn is_registered(&self) -> bool {
        self.registered
    }

    /// Whether the caller named this boundary explicitly.
    pub fn is_explicit(&self) -> bool {
        self.explicit
    }

    /// Whether Project-scoped work may use [`Self::root`]. An explicit
    /// `--project` declaration always allows it; otherwise the root must be a
    /// registered Project.
    pub fn allows_project_state(&self) -> bool {
        self.registered || self.explicit
    }

    /// Reject an operation that needs a declared boundary when the invocation
    /// sits outside any registered Project.
    pub fn require(&self, invocation_dir: &Path) -> Result<()> {
        if self.allows_project_state() {
            return Ok(());
        }
        Err(OcgError::config(format!(
            "this command needs a registered Project: neither {} nor any parent directory is one\nregister the Project from the OCG control surface, or pass --project DIR",
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

/// Resolve the registered Project containing `start`, or fall back to the
/// canonical `start` directory, unregistered.
pub fn resolve(start: &Path) -> ProjectBoundary {
    let start = canonicalize(start);
    let registered = crate::core_store::CoreStore::locate()
        .and_then(|store| store.project_containing(&start))
        .ok()
        .flatten();
    match registered {
        Some(project) => ProjectBoundary {
            root: PathBuf::from(project.root),
            registered: true,
            explicit: false,
        },
        None => ProjectBoundary {
            root: start,
            registered: false,
            explicit: false,
        },
    }
}

/// Use an explicitly named root as the boundary. Naming it is itself the
/// declaration. The root is still canonicalized so a symlinked spelling
/// cannot create a second boundary.
pub fn explicit(root: &Path) -> ProjectBoundary {
    let root = canonicalize(root);
    let registered = crate::core_store::CoreStore::locate()
        .and_then(|store| store.project_containing(&root))
        .ok()
        .flatten()
        .is_some_and(|project| Path::new(&project.root) == root);
    ProjectBoundary {
        root,
        registered,
        explicit: true,
    }
}
