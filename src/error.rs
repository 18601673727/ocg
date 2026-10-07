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

/// How a failed HTTP attempt relates to repeating the identical request.
///
/// The verdict is taken where the transport's own error type is still
/// available, from typed variants and [`std::io::ErrorKind`] alone, and then
/// carried on [`OcgError::Transport`]. It is never re-derived from an error
/// message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportFault {
    /// The network interrupted the exchange: the same bytes can plausibly be
    /// accepted on a later attempt.
    Reconnectable,
    /// Repeating the identical request cannot change the outcome: a rejected
    /// or malformed request, a certificate that does not validate, a protocol
    /// the peer or OCG decoded wrongly, or a local limit.
    Deterministic,
}

impl TransportFault {
    pub const fn code(self) -> &'static str {
        match self {
            Self::Reconnectable => "reconnectable",
            Self::Deterministic => "deterministic",
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
    /// The OS credential store did not answer within the caller's bound,
    /// usually because it is waiting for a person to answer an approval
    /// prompt. The lookup keeps running; a later retry observes its outcome.
    #[error("{0}")]
    CredentialStorePending(String),
    /// A configuration problem. The message is already user-facing.
    #[error("{0}")]
    Config(String),
    /// A failure raised at the HTTP boundary, carrying the verdict the
    /// transport's own error type supported at the moment it was made.
    #[error("{message}")]
    Transport {
        fault: TransportFault,
        message: String,
    },
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

    pub fn transport(fault: TransportFault, message: impl Into<String>) -> Self {
        Self::Transport {
            fault,
            message: message.into(),
        }
    }

    /// The verdict this error's transport produced.
    ///
    /// Every error that did not come from the HTTP boundary is deterministic by
    /// definition: a rejected payload, a poisoned decoder or a local failure is
    /// never repeated.
    pub fn transport_fault(&self) -> TransportFault {
        match self {
            Self::Transport { fault, .. } => *fault,
            _ => TransportFault::Deterministic,
        }
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

pub(crate) fn sqlite_mapping_error(error: serde_rusqlite::Error) -> rusqlite::Error {
    match error {
        serde_rusqlite::Error::Rusqlite(error) => error,
        error => rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Null,
            Box::new(error),
        ),
    }
}
