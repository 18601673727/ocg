use std::fmt;

use crate::db::entities::ProviderFailure;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportError {
    InvalidRequest { field: String, message: String },
    Unsupported { field: String, message: String },
    Build { message: String },
    Provider(ProviderFailure),
}

impl TransportError {
    #[must_use]
    pub fn invalid(field: impl Into<String>, message: impl Into<String>) -> Self {
        Self::InvalidRequest {
            field: field.into(),
            message: message.into(),
        }
    }

    #[must_use]
    pub fn unsupported(field: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Unsupported {
            field: field.into(),
            message: message.into(),
        }
    }

    #[must_use]
    pub fn build(message: impl Into<String>) -> Self {
        Self::Build {
            message: message.into(),
        }
    }

    #[must_use]
    pub fn status_code(&self) -> Option<u16> {
        match self {
            Self::Provider(failure) => failure.status_code,
            Self::InvalidRequest { .. } | Self::Unsupported { .. } | Self::Build { .. } => None,
        }
    }

    #[must_use]
    pub fn provider_code(&self) -> Option<&str> {
        match self {
            Self::Provider(failure) => failure.provider_code.as_deref(),
            Self::InvalidRequest { .. } | Self::Unsupported { .. } | Self::Build { .. } => None,
        }
    }
}

impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest { field, message } => {
                write!(f, "invalid chat request field `{field}`: {message}")
            }
            Self::Unsupported { field, message } => {
                write!(f, "unsupported chat request field `{field}`: {message}")
            }
            Self::Build { message } => write!(f, "could not build provider request: {message}"),
            Self::Provider(failure) => write!(f, "provider call failed: {failure}"),
        }
    }
}

impl std::error::Error for TransportError {}
