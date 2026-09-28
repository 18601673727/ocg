//! Error type shared by the whole application.

use std::path::Path;
use thiserror::Error;

/// A problem the user has to fix: bad configuration, a missing file, invalid
/// JSON, or a process that could not be launched.
#[derive(Debug, Error)]
pub enum OcgError {
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
