//! OpenAI-compatible stream normalization.
//!
//! Content part types for the OpenAI Chat Completions wire format.

/// A content part in an OpenAI-compatible message.
#[derive(Debug, Clone, PartialEq)]
pub enum ContentPart {
    Text { text: String },
}

impl ContentPart {
    #[must_use]
    pub fn text(text: impl Into<String>) -> Self {
        Self::Text { text: text.into() }
    }
}
