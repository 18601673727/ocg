//! OpenAI-compatible transport error types.

use std::fmt;

/// Provider-side failure detail, preserving the observable facts without
/// interpreting them into a routing decision.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProviderFailure {
    pub message: String,
    pub status_code: Option<u16>,
    pub provider_code: Option<String>,
    pub response_body: Option<String>,
    pub request_id: Option<String>,
    pub retry_after_ms: Option<u64>,
    pub retryable: bool,
}

impl fmt::Display for ProviderFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.status_code {
            Some(status) => write!(f, "HTTP {status}: {}", self.message),
            None => f.write_str(&self.message),
        }
    }
}

impl std::error::Error for ProviderFailure {}

/// Typed transport error.
///
/// The gateway classifies dispatch/settlement from this; the transport itself
/// never decides whether to retry, reroute or settle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportError {
    /// The client request is structurally invalid.
    InvalidRequest { field: String, message: String },
    /// The client request uses a field or feature this transport does not
    /// forward. Rejected before any dispatch.
    Unsupported { field: String, message: String },
    /// The request could not be translated into a provider dispatch, or the
    /// transport configuration is unusable.
    Build { message: String },
    /// The upstream provider or transport failed.
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

    /// Whether the underlying provider error was marked retryable.
    /// OCG owns the decision to act on this.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Provider(failure) if failure.retryable)
    }

    /// The observed HTTP status, when one was seen.
    #[must_use]
    pub fn status_code(&self) -> Option<u16> {
        match self {
            Self::Provider(failure) => failure.status_code,
            _ => None,
        }
    }

    /// The provider's machine-readable code, when one was reported.
    #[must_use]
    pub fn provider_code(&self) -> Option<&str> {
        match self {
            Self::Provider(failure) => failure.provider_code.as_deref(),
            _ => None,
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
