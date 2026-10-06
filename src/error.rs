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
    StorageProtection,
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
            Self::StorageProtection => "spawn_storage_protection",
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
    /// The host has no usable disk space left for durable execution state.
    /// This is storage safety, never a provider, Placement, rate or health
    /// failure: callers must not retry it as one.
    #[error("storage full ({context}): {message}")]
    StorageFull { context: String, message: String },
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

    pub fn storage_full(context: impl Into<String>, message: impl Into<String>) -> Self {
        Self::StorageFull {
            context: context.into(),
            message: message.into(),
        }
    }

    /// True when this error means the local filesystem cannot take more
    /// durable state: either an explicit storage-full report or an I/O
    /// failure with `ENOSPC` (or a read-only mount, which cannot take durable
    /// state either). There is always a race with other processes, so
    /// preflight checks can never make this impossible.
    pub fn is_storage_full(&self) -> bool {
        match self {
            Self::StorageFull { .. } => true,
            Self::Io { source, .. } => {
                let code = source.raw_os_error();
                code == Some(libc::ENOSPC) || code == Some(libc::EROFS)
            }
            _ => false,
        }
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
