//! Error type shared by the whole application.

use std::path::Path;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpawnRefusalReason {
    ParentAuthorityStale,
    SpawnKeyConflict,
    CallMismatch,
    DepthLimit,
    ChildLimit,
    DescendantLimit,
    DependencyCycle,
}

impl SpawnRefusalReason {
    pub const fn code(self) -> &'static str {
        match self {
            Self::ParentAuthorityStale => "spawn_parent_authority_stale",
            Self::SpawnKeyConflict => "spawn_key_conflict",
            Self::CallMismatch => "spawn_call_mismatch",
            Self::DepthLimit => "spawn_depth_limit_reached",
            Self::ChildLimit => "spawn_child_limit_reached",
            Self::DescendantLimit => "spawn_descendant_limit_reached",
            Self::DependencyCycle => "spawn_dependency_cycle",
        }
    }
}

/// A problem the user has to fix: bad configuration, a missing file, invalid
/// JSON, or a process that could not be launched.
#[derive(Debug, Error)]
pub enum OcgError {
    /// A recursive child Job was refused by a durable invariant.
    #[error("spawn refused ({reason:?}): {message}")]
    SpawnRefused {
        reason: SpawnRefusalReason,
        message: String,
    },
    /// A configuration problem. The message is already user-facing.
    #[error("{0}")]
    Config(String),
    /// An I/O problem with the path it applied to.
    #[error("{context}: {source}")]
    Io {
        context: String,
        #[source]
        source: std::io::Error,
    },
}

impl OcgError {
    pub fn spawn_refused(reason: SpawnRefusalReason, message: impl Into<String>) -> Self {
        Self::SpawnRefused {
            reason,
            message: message.into(),
        }
    }

    pub fn config(message: impl Into<String>) -> Self {
        Self::Config(message.into())
    }

    pub fn io(context: impl Into<String>, source: std::io::Error) -> Self {
        Self::Io {
            context: context.into(),
            source,
        }
    }

    pub fn read(path: &Path, source: std::io::Error) -> Self {
        Self::io(format!("cannot read {}", path.display()), source)
    }

    pub fn write(path: &Path, source: std::io::Error) -> Self {
        Self::io(format!("cannot write {}", path.display()), source)
    }
}

pub type Result<T> = std::result::Result<T, OcgError>;
