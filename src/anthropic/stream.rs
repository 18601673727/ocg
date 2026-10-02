//! Anthropic Messages stream decoding.
//!
//! Decodes the Anthropic Messages SSE event sequence incrementally into OCG's
//! canonical [`ChatStreamEvent`] and [`ChatStreamSummary`]. The canonical
//! structures are provider-neutral, so nothing here leaks the Messages event
//! names, content-block indices or stop-reason spellings past this module.

use std::collections::BTreeMap;

use serde_json::Value;

use crate::error::{OcgError, Result};
use crate::openai_compatible::error::{ProviderFailure, TransportError};
use crate::openai_compatible::stream::{
    ChatFinishReason, ChatStreamEvent, ChatStreamSummary, NormalizedUsage,
};

use super::error::{anthropic_failure, describe};

/// One Anthropic content block OCG is currently accumulating.
#[derive(Debug, Clone, Default)]
struct BlockSlot {
    /// Canonical tool-call ordinal this block maps to, for `tool_use` blocks.
    tool_index: Option<u32>,
    id: String,
    name: String,
    /// The argument fragments streamed so far, concatenated as they arrived.
    arguments: String,
}

/// Incremental Anthropic Messages decoder for one provider round.
///
/// Each SSE event is folded as it arrives; the response body is never buffered
/// and replayed. The state is owned behind a lock by the HTTP chunk callback,
/// which runs inside the transport's async read.
#[derive(Debug, Default)]
pub struct AnthropicStreamState {
    blocks: BTreeMap<u64, BlockSlot>,
    next_tool_index: u32,
    usage: Option<Value>,
    stop_reason: Option<String>,
    message_id: Option<String>,
    model: Option<String>,
    /// An `error` event seen on the stream. It terminates the round: the Call
    /// fails rather than being folded into a successful assistant message.
    failure: Option<ProviderFailure>,
    closed: bool,
}

impl AnthropicStreamState {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether the Messages sequence reached a terminal event, so the transport
    /// can stop reading instead of holding the connection open.
    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.closed
    }

    /// Fold one `data:` payload from the stream. Returns every canonical event
    /// it produced, in order, plus whether reading should continue.
    pub fn consume(&mut self, event: &Value) -> Result<(bool, Vec<ChatStreamEvent>)> {
        let mut emitted = Vec::new();
        if self.closed {
            return Ok((false, emitted));
        }
        // `ping` and any event OCG does not decode carry no assistant content.
        // Ignoring them keeps an unrecognised keep-alive from ending a live round.
        let Some(kind) = event.get("type").and_then(Value::as_str) else {
            return Ok((!self.closed, emitted));
        };
        match kind {
            "message_start" => {
                let message = event.get("message");
                self.message_id = message
                    .and_then(|message| message.get("id"))
                    .and_then(Value::as_str)
                    .map(str::to_string);
                self.model = message
                    .and_then(|message| message.get("model"))
                    .and_then(Value::as_str)
                    .map(str::to_string);
                self.usage = message
                    .and_then(|message| message.get("usage"))
                    .cloned()
                    .or_else(|| event.get("usage").cloned());
                emitted.push(ChatStreamEvent::Metadata {
                    id: self.message_id.clone(),
                    model: self.model.clone(),
                    timestamp: None,
                });
            }
            "content_block_start" => {
                self.start_block(block_index(event), event, &mut emitted)?;
            }
            "content_block_delta" => {
                self.delta_block(block_index(event), event, &mut emitted);
            }
            "content_block_stop" => {
                self.stop_block(block_index(event), &mut emitted);
            }
            "message_delta" => {
                if let Some(reason) = event
                    .get("delta")
                    .and_then(|delta| delta.get("stop_reason"))
                    .and_then(Value::as_str)
                {
                    self.stop_reason = Some(reason.to_string());
                }
                // `message_start` states the input total and `message_delta`
                // states the output total; merging keeps both instead of letting
                // the later, partial statement erase the earlier one.
                if let Some(usage) = event.get("usage") {
                    let mut merged = self
                        .usage
                        .clone()
                        .unwrap_or_else(|| Value::Object(Default::default()));
                    merge_usage(&mut merged, usage);
                    self.usage = Some(merged);
                }
            }
            "message_stop" => {
                self.closed = true;
            }
            "error" => {
                let failure = anthropic_failure(Some(event), None);
                emitted.push(ChatStreamEvent::Error(TransportError::Provider(
                    failure.clone(),
                )));
                self.failure = Some(failure);
                self.closed = true;
            }
            _ => {}
        }
        Ok((!self.closed, emitted))
    }

    /// Close the round: record usage and the stop reason on the canonical
    /// summary, or fail the round when the stream reported an error.
    pub fn finish(&mut self, summary: &mut ChatStreamSummary) -> Result<()> {
        if let Some(failure) = self.failure.take() {
            return Err(OcgError::config(describe(&failure)));
        }
        if !self.closed || self.stop_reason.is_none() || !self.blocks.is_empty() {
            return Err(OcgError::config(
                "Anthropic provider stream ended before completion",
            ));
        }
        if let Some(usage) = self.usage.as_ref() {
            summary.usage = parse_usage(usage);
        }
        summary.raw_finish_reason = self.stop_reason.clone();
        summary.finish_reason = Some(map_stop_reason(self.stop_reason.as_deref()));
        Ok(())
    }

    fn start_block(
        &mut self,
        index: u64,
        event: &Value,
        emitted: &mut Vec<ChatStreamEvent>,
    ) -> Result<()> {
        let Some(block) = event.get("content_block") else {
            return Ok(());
        };
        if block.get("type").and_then(Value::as_str) != Some("tool_use") {
            return Ok(());
        }
        // The Messages block index counts every block, text included. OCG's
        // canonical tool index is a dense tool-call ordinal, so each `tool_use`
        // block takes the next one and the mapping is remembered.
        let tool_index = self.next_tool_index;
        self.next_tool_index = tool_index
            .checked_add(1)
            .ok_or_else(|| OcgError::config("Anthropic provider returned too many tool calls"))?;
        let id = block
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let name = block
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        // `input` arrives complete rather than as fragments, so a non-empty
        // object is the canonical argument blob and no delta is needed.
        let arguments = match block.get("input") {
            Some(Value::Object(fields)) if !fields.is_empty() => {
                serde_json::to_string(&Value::Object(fields.clone())).unwrap_or_default()
            }
            _ => String::new(),
        };
        let slot = BlockSlot {
            tool_index: Some(tool_index),
            id: id.clone(),
            name: name.clone(),
            arguments: arguments.clone(),
        };
        self.blocks.insert(index, slot);
        emitted.push(ChatStreamEvent::ToolCallStart {
            index: tool_index,
            id: id.clone(),
            name,
        });
        if !arguments.is_empty() {
            emitted.push(ChatStreamEvent::ToolCallArgumentsDelta {
                index: tool_index,
                id: id.clone(),
                delta: arguments,
            });
        }
        Ok(())
    }

    fn delta_block(&mut self, index: u64, event: &Value, emitted: &mut Vec<ChatStreamEvent>) {
        let Some(delta) = event.get("delta") else {
            return;
        };
        match delta.get("type").and_then(Value::as_str) {
            Some("text_delta") => {
                if let Some(text) = delta.get("text").and_then(Value::as_str) {
                    if !text.is_empty() {
                        emitted.push(ChatStreamEvent::TextDelta {
                            delta: text.to_string(),
                        });
                    }
                }
            }
            Some("thinking_delta") => {
                if let Some(text) = delta.get("thinking").and_then(Value::as_str) {
                    if !text.is_empty() {
                        emitted.push(ChatStreamEvent::ReasoningDelta {
                            delta: text.to_string(),
                        });
                    }
                }
            }
            Some("input_json_delta") => {
                let Some(slot) = self.blocks.get_mut(&index) else {
                    return;
                };
                let Some(tool_index) = slot.tool_index else {
                    return;
                };
                if let Some(partial) = delta.get("partial_json").and_then(Value::as_str) {
                    if partial.is_empty() {
                        return;
                    }
                    slot.arguments.push_str(partial);
                    emitted.push(ChatStreamEvent::ToolCallArgumentsDelta {
                        index: tool_index,
                        id: slot.id.clone(),
                        delta: partial.to_string(),
                    });
                }
            }
            // `signature_delta` authenticates a thinking block rather than
            // extending reasoning text, and `citations_delta` carries a citation
            // OCG's canonical assistant message cannot carry. Neither is
            // reasoning output.
            _ => {}
        }
    }

    fn stop_block(&mut self, index: u64, emitted: &mut Vec<ChatStreamEvent>) {
        let Some(slot) = self.blocks.remove(&index) else {
            return;
        };
        let Some(tool_index) = slot.tool_index else {
            return;
        };
        // The canonical argument blob is the accumulated fragment string, which
        // is the only place the fragments exist before they are parsed into the
        // Call payload.
        let arguments = if slot.arguments.trim().is_empty() {
            "{}".to_string()
        } else {
            slot.arguments
        };
        emitted.push(ChatStreamEvent::ToolCallComplete {
            index: tool_index,
            id: slot.id,
            name: slot.name,
            arguments,
        });
    }
}

fn block_index(event: &Value) -> u64 {
    event.get("index").and_then(Value::as_u64).unwrap_or(0)
}

/// Fold one complete (non-streamed) Messages response into the summary.
///
/// A non-streamed response has no incremental truth to reconstruct, so nothing
/// is emitted here — the canonical events a Chat surface renders are the stream,
/// and replaying a whole body as deltas would put a summary round's text in the
/// user's transcript. This mirrors the non-streamed OpenAI completion path.
pub fn apply_message(summary: &mut ChatStreamSummary, message: &Value) -> Result<()> {
    if let Some(failure) = message.get("error") {
        let Some(text) = failure.get("message").and_then(Value::as_str) else {
            return Err(OcgError::config(
                "Anthropic provider returned an error response".to_string(),
            ));
        };
        return Err(OcgError::config(format!(
            "Anthropic provider returned an error: {text}"
        )));
    }
    if message.get("type").and_then(Value::as_str) != Some("message")
        || message.get("content").and_then(Value::as_array).is_none()
        || message.get("stop_reason").and_then(Value::as_str).is_none()
    {
        return Err(OcgError::config("invalid Anthropic Messages response"));
    }
    let blocks = message
        .get("content")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();
    for (index, block) in blocks.iter().enumerate() {
        let index = u32::try_from(index).unwrap_or(u32::MAX);
        match block.get("type").and_then(Value::as_str) {
            Some("text") => {
                if let Some(text) = block.get("text").and_then(Value::as_str) {
                    summary.text.push_str(text);
                }
            }
            Some("tool_use") => {
                let id = block
                    .get("id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                let name = block
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                let arguments = match block.get("input") {
                    Some(Value::Object(fields)) => {
                        serde_json::to_string(&Value::Object(fields.clone())).unwrap_or_default()
                    }
                    _ => "{}".to_string(),
                };
                summary.apply(&ChatStreamEvent::ToolCallComplete {
                    index,
                    id,
                    name,
                    arguments,
                });
            }
            // `thinking` and `redacted_thinking` are replayed into the request
            // only with their signature, which the canonical assistant message
            // does not carry; the reasoning is still surfaced as canonical
            // reasoning output below.
            Some("thinking") => {
                if let Some(text) = block.get("thinking").and_then(Value::as_str) {
                    summary.reasoning.push_str(text);
                }
            }
            _ => {}
        }
    }
    summary.raw_finish_reason = message
        .get("stop_reason")
        .and_then(Value::as_str)
        .map(str::to_string);
    summary.finish_reason = Some(map_stop_reason(summary.raw_finish_reason.as_deref()));
    if let Some(usage) = message.get("usage") {
        summary.usage = parse_usage(usage);
    }
    Ok(())
}

/// Map an Anthropic stop reason onto the canonical finish reason.
///
/// `stop_sequence` has no canonical counterpart and is a normal completion, so
/// it maps to `Stop` with the original reason preserved on the summary.
fn map_stop_reason(raw: Option<&str>) -> ChatFinishReason {
    match raw {
        Some("end_turn") | Some("stop_sequence") | None => ChatFinishReason::Stop,
        Some("max_tokens") => ChatFinishReason::Length,
        Some("tool_use") => ChatFinishReason::ToolCalls,
        Some("refusal") => ChatFinishReason::ContentFilter,
        Some(_) => ChatFinishReason::Other,
    }
}

/// Map Anthropic's cache vocabulary onto OCG's.
///
/// Anthropic separates cache reads from cache *creation*; OCG's
/// [`NormalizedUsage`] separates cache reads from cache *writes*. A cache
/// creation is the write it is, so it is mapped rather than dropped: dropping
/// it would understate a settled Call.
#[must_use]
pub fn parse_usage(value: &Value) -> NormalizedUsage {
    NormalizedUsage {
        input_tokens: counter(value, "input_tokens"),
        output_tokens: counter(value, "output_tokens"),
        cache_read_tokens: counter(value, "cache_read_input_tokens"),
        cache_write_tokens: counter(value, "cache_creation_input_tokens"),
        // Anthropic reports thinking tokens inside `output_tokens` and does not
        // break them out, so no reasoning total is fabricated here.
        reasoning_tokens: None,
        raw: Some(value.clone()),
    }
}

fn counter(value: &Value, field: &str) -> Option<u32> {
    value
        .get(field)
        .and_then(Value::as_u64)
        .map(|count| u32::try_from(count).unwrap_or(u32::MAX))
}

/// Merge a later partial `usage` statement over an earlier one.
fn merge_usage(into: &mut Value, later: &Value) {
    let (Some(into), Some(later)) = (into.as_object_mut(), later.as_object()) else {
        *into = later.clone();
        return;
    };
    for (field, value) in later {
        into.insert(field.clone(), value.clone());
    }
}

/// Ensure the tool-call arguments this round accumulated are parseable JSON
/// objects.
///
/// Anthropic streams a tool call's arguments as JSON fragments, so a truncated
/// stream can leave a fragment that is not JSON. The Call payload is validated
/// against the tool's own schema before dispatch, but a fragment that is not
/// JSON at all is a provider protocol failure, not a tool error the model could
/// repair, so it fails the round here.
pub fn validate_tool_arguments(summary: &ChatStreamSummary) -> Result<()> {
    for call in &summary.tool_calls {
        if call.arguments.trim().is_empty() {
            return Err(OcgError::config(format!(
                "provider tool call {} produced no arguments",
                call.id
            )));
        }
        match serde_json::from_str::<Value>(&call.arguments) {
            Ok(Value::Object(_)) => {}
            Ok(_) => {
                return Err(OcgError::config(format!(
                    "provider tool call {} arguments are not a JSON object",
                    call.id
                )))
            }
            Err(error) => {
                return Err(OcgError::config(format!(
                    "provider tool call {} arguments are not valid JSON: {error}",
                    call.id
                )))
            }
        }
    }
    Ok(())
}
