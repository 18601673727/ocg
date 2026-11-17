#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unsupported chat request field `{field}`: {message}")]
pub struct TransportError {
    field: String,
    message: String,
}

impl TransportError {
    pub(crate) fn unsupported(field: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            message: message.into(),
        }
    }
}
