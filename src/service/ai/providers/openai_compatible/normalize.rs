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
