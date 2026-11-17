pub mod blocking;
pub(crate) mod command;
#[cfg(unix)]
pub mod files;
#[cfg(unix)]
pub(crate) mod process;
pub mod search;

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ExecutionFailure {
    #[error("{0}")]
    Refused(String),
    #[error("{0}")]
    Uncertain(String),
}

impl ExecutionFailure {
    pub(crate) fn not_applied(message: impl Into<String>) -> Self {
        Self::Refused(message.into())
    }

    pub(crate) fn uncertain(message: impl Into<String>) -> Self {
        Self::Uncertain(message.into())
    }
}
