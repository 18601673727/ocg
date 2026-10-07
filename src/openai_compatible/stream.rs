//! OpenAI-compatible stream events and usage normalization.
//!
//! Normalized streaming events for the OpenAI Chat Completions wire format.

use serde_json::Value;

/// OpenAI-compatible finish reason for SSE framing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChatFinishReason {
    Stop,
    Length,
    ToolCalls,
    ContentFilter,
    Error,
    Other,
}

impl ChatFinishReason {
    /// The OpenAI `finish_reason` string for this reason.
    #[must_use]
    pub fn as_openai_str(self) -> &'static str {
        match self {
            Self::Stop => "stop",
            Self::Length => "length",
            Self::ToolCalls => "tool_calls",
            Self::ContentFilter => "content_filter",
            Self::Error => "error",
            Self::Other => "other",
        }
    }
}

/// Normalized token usage. Unreported counters stay `None`; they are never
/// fabricated as zero.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct NormalizedUsage {
    pub input_tokens: Option<u32>,
    pub output_tokens: Option<u32>,
    pub cache_read_tokens: Option<u32>,
    pub cache_write_tokens: Option<u32>,
    pub reasoning_tokens: Option<u32>,
    /// The provider's original `usage` object, when reported.
    pub raw: Option<Value>,
}

impl NormalizedUsage {
    /// Sum of the reported input and output totals, when both are present.
    #[must_use]
    pub fn total_tokens(&self) -> Option<u32> {
        match (self.input_tokens, self.output_tokens) {
            (Some(input), Some(output)) => Some(input.saturating_add(output)),
            _ => None,
        }
    }
}

/// One normalized provider event.
///
/// Tool-call events carry the OpenAI `index` so interleaved fragments from
/// multiple calls reconstruct without re-deriving order.
#[derive(Debug, Clone, PartialEq)]
pub enum ChatStreamEvent {
    Image {
        url: String,
    },
    TextDelta {
        delta: String,
    },
    ReasoningDelta {
        delta: String,
    },
    ToolCallStart {
        index: u32,
        id: String,
        name: String,
    },
    ToolCallArgumentsDelta {
        index: u32,
        id: String,
        delta: String,
    },
    ToolCallComplete {
        index: u32,
        id: String,
        name: String,
        /// JSON-encoded arguments, as on the OpenAI wire.
        arguments: String,
    },
    Metadata {
        id: Option<String>,
        model: Option<String>,
        timestamp: Option<String>,
    },
    /// A logical provider round began. Everything the provider emits between
    /// this marker and the next one is this round's provisional output, so a
    /// replaced physical attempt can be dropped without touching the rounds
    /// that already completed.
    RoundBegan,
    /// The provisional output of the current round was invalidated: a
    /// transient transport failure ended a physical attempt that had already
    /// produced user-visible output, and a replacement attempt will produce it
    /// again. Consumers drop back to the last [`ChatStreamEvent::RoundBegan`].
    RoundReset,
    Finish {
        reason: ChatFinishReason,
        raw_reason: Option<String>,
        usage: NormalizedUsage,
    },
    Error(super::error::TransportError),
}

/// A completed tool call reconstructed from a normalized stream.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompletedToolCall {
    pub index: u32,
    pub id: String,
    pub name: String,
    /// JSON-encoded arguments, as on the OpenAI wire.
    pub arguments: String,
}

/// The accumulated result of a normalized stream, suitable for usage
/// settlement. Fragment order is not preserved here; the streaming
/// events are the source of truth for reconstruction.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ChatStreamSummary {
    pub images: Vec<String>,
    pub text: String,
    pub reasoning: String,
    pub tool_calls: Vec<CompletedToolCall>,
    pub usage: NormalizedUsage,
    pub finish_reason: Option<ChatFinishReason>,
    pub raw_finish_reason: Option<String>,
}

impl ChatStreamSummary {
    /// Fold one normalized event into the summary.
    pub fn apply(&mut self, event: &ChatStreamEvent) {
        match event {
            ChatStreamEvent::Image { url } => {
                if !self.images.contains(url) {
                    self.images.push(url.clone());
                }
            }
            ChatStreamEvent::TextDelta { delta } => self.text.push_str(delta),
            ChatStreamEvent::ReasoningDelta { delta } => self.reasoning.push_str(delta),
            ChatStreamEvent::ToolCallStart { index, id, name } => {
                let slot = self.slot(*index);
                // Compatible providers may repeat call metadata with argument
                // fragments. Only a different call replaces accumulated JSON.
                if !slot.id.is_empty() && slot.id != *id {
                    slot.arguments.clear();
                }
                slot.id.clone_from(id);
                slot.name.clone_from(name);
            }
            ChatStreamEvent::ToolCallArgumentsDelta { index, id, delta } => {
                let slot = self.slot(*index);
                if slot.id.is_empty() {
                    slot.id.clone_from(id);
                }
                slot.arguments.push_str(delta);
            }
            ChatStreamEvent::ToolCallComplete {
                index,
                id,
                name,
                arguments,
            } => {
                let slot = self.slot(*index);
                slot.id.clone_from(id);
                slot.name.clone_from(name);
                slot.arguments.clone_from(arguments);
            }
            ChatStreamEvent::Finish {
                reason,
                raw_reason,
                usage,
            } => {
                self.usage.clone_from(usage);
                self.finish_reason = Some(*reason);
                self.raw_finish_reason.clone_from(raw_reason);
            }
            ChatStreamEvent::Metadata { .. }
            | ChatStreamEvent::Error(_)
            | ChatStreamEvent::RoundBegan
            | ChatStreamEvent::RoundReset => {}
        }
    }

    fn slot(&mut self, index: u32) -> &mut CompletedToolCall {
        match self
            .tool_calls
            .binary_search_by_key(&index, |call| call.index)
        {
            Ok(position) => &mut self.tool_calls[position],
            Err(position) => {
                self.tool_calls.insert(
                    position,
                    CompletedToolCall {
                        index,
                        ..CompletedToolCall::default()
                    },
                );
                &mut self.tool_calls[position]
            }
        }
    }
}
