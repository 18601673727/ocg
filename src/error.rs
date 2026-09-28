//! Error type shared by the whole application.

use std::fmt;
use std::path::Path;

/// A problem the user has to fix: bad configuration, a missing file, invalid
/// JSON, or a process that could not be launched.
#[derive(Debug)]
pub enum OcgError {
    /// A configuration problem. The message is already user-facing.
    Config(String),
    /// An I/O problem with the path it applied to.
    Io {
        context: String,
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

impl fmt::Display for OcgError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(message) => write!(formatter, "{message}"),
            Self::Io { context, source } => write!(formatter, "{context}: {source}"),
        }
    }
}

impl std::error::Error for OcgError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Config(_) => None,
        }
    }
}

pub type Result<T> = std::result::Result<T, OcgError>;
