//! OpenAI-compatible stream normalization.
//!
//! Tool-call index assignment and content part types for the OpenAI
//! Chat Completions wire format.

use std::collections::HashMap;

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

/// Assigns stable OpenAI `tool_calls[].index` values to tool-call
/// fragments and keeps interleaved fragments attached to the right call.
#[derive(Debug, Clone, Default)]
pub struct ToolCallNormalizer {
    slots: Vec<ToolCallSlot>,
    by_id: HashMap<String, usize>,
}

#[derive(Debug, Clone, Default)]
struct ToolCallSlot {
    name: String,
}

impl ToolCallNormalizer {
    /// Resolve (or assign) the index for a tool call id. `name` is recorded
    /// when a start or complete event supplies it.
    pub fn index_for(&mut self, id: &str, name: Option<&str>) -> u32 {
        if let Some(&position) = self.by_id.get(id) {
            if let Some(name) = name {
                if !name.is_empty() {
                    self.slots[position].name = name.to_string();
                }
            }
            return u32::try_from(position).unwrap_or(u32::MAX);
        }
        let position = self.slots.len();
        self.slots.push(ToolCallSlot {
            name: name.unwrap_or_default().to_string(),
        });
        self.by_id.insert(id.to_string(), position);
        u32::try_from(position).unwrap_or(u32::MAX)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }
}
