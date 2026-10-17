//! OpenAI-compatible stream events and usage normalization.
//!
//! Normalized streaming events for the OpenAI Chat Completions wire format.

use serde::de::IgnoredAny;
use serde_json::{Map, Value};

use super::request::{
    MAX_FUNCTION_NAME_BYTES, MAX_TOOL_ARGUMENTS_BYTES, MAX_TOOL_CALL_ID_BYTES, MAX_TOOL_CALLS,
    is_function_name,
};

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

    /// The finish reason a raw OpenAI `finish_reason` string names. Any string
    /// without a known name is `Other`, exactly as the stream parser reads it.
    #[must_use]
    pub fn from_openai_str(raw: &str) -> Self {
        match raw {
            "stop" => Self::Stop,
            "length" => Self::Length,
            "tool_calls" => Self::ToolCalls,
            "content_filter" => Self::ContentFilter,
            "error" => Self::Error,
            _ => Self::Other,
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

/// Why a tool call a Provider sent was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolCallRefusal {
    /// A fragment or field the wire contract does not allow.
    Malformed,
    /// A call changed its id, or one id was sent for two calls.
    ConflictingIdentity,
    /// A call sent a function name more than once, so the function it names
    /// cannot be told for certain.
    AmbiguousIdentity,
    /// A call, or the response, exceeded a size or count limit.
    LimitExceeded,
    /// A call, or the finish beside it, never completed.
    Incomplete,
    /// The call names a function that its request did not allow.
    Unauthorized,
    /// The function was offered without a parameter schema.
    MissingSchema,
    /// The function's parameter schema uses something that is not enforced.
    UnsupportedSchema,
    /// The arguments are not one strict JSON object satisfying the schema.
    InvalidArguments,
}

impl ToolCallRefusal {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Malformed => "malformed",
            Self::ConflictingIdentity => "conflicting identity",
            Self::AmbiguousIdentity => "ambiguous identity",
            Self::LimitExceeded => "limit exceeded",
            Self::Incomplete => "incomplete",
            Self::Unauthorized => "unauthorized",
            Self::MissingSchema => "missing parameter schema",
            Self::UnsupportedSchema => "unsupported parameter schema",
            Self::InvalidArguments => "invalid arguments",
        }
    }
}

impl std::fmt::Display for ToolCallRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Tool-call output a Provider sent that is refused: it cannot be taken as
/// a complete, well-formed call, or its request does not admit it. The
/// message names no argument value.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid provider tool call ({kind}): {message}")]
pub struct InvalidToolCall {
    pub kind: ToolCallRefusal,
    /// The wire index of the refused call, when one call is at fault.
    pub index: Option<u32>,
    pub message: String,
}

impl InvalidToolCall {
    /// A refusal of the response rather than of one call.
    #[must_use]
    pub fn new(kind: ToolCallRefusal, message: impl Into<String>) -> Self {
        Self {
            kind,
            index: None,
            message: message.into(),
        }
    }

    /// A refusal of the call at wire index `index`.
    #[must_use]
    pub fn at(kind: ToolCallRefusal, index: u32, message: impl Into<String>) -> Self {
        Self {
            kind,
            index: Some(index),
            message: message.into(),
        }
    }
}

/// Reconstructs the streamed `delta.tool_calls` of one choice. Fragments are
/// keyed by their wire `index`, never by arrival order, so interleaved calls
/// stay apart. Argument fragments are concatenated exactly as sent. A call
/// names its function in exactly one non-empty name field: a second one is
/// refused, whether it repeats the name or continues it, because the wire
/// cannot tell an echo from a fragment and the name is the call's identity.
/// An id may repeat, but a different id at one index, or one id at two
/// indexes, is a conflict and is refused rather than merged.
#[derive(Debug, Clone, Default)]
pub struct ToolCallAssembler {
    /// Ordered by `index`. An empty id or name has not been sent yet.
    calls: Vec<CompletedToolCall>,
    bytes: usize,
}

impl ToolCallAssembler {
    /// Folds one `delta.tool_calls` value. `null` carries nothing.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidToolCall`] for a malformed fragment, a conflicting
    /// identity, or a fragment beyond the per-call or per-response limits.
    pub fn apply(&mut self, deltas: &Value) -> Result<(), InvalidToolCall> {
        let deltas = match deltas {
            Value::Null => return Ok(()),
            Value::Array(deltas) => deltas,
            _ => {
                return Err(InvalidToolCall::new(
                    ToolCallRefusal::Malformed,
                    "`tool_calls` is not an array",
                ));
            }
        };
        for delta in deltas {
            self.apply_one(delta)?;
        }
        Ok(())
    }

    fn apply_one(&mut self, delta: &Value) -> Result<(), InvalidToolCall> {
        let fields = delta.as_object().ok_or_else(|| {
            InvalidToolCall::new(
                ToolCallRefusal::Malformed,
                "a tool call delta is not an object",
            )
        })?;
        let index = fields
            .get("index")
            .and_then(Value::as_u64)
            .and_then(|index| u32::try_from(index).ok())
            .ok_or_else(|| {
                InvalidToolCall::new(
                    ToolCallRefusal::Malformed,
                    "a tool call delta has no valid `index`",
                )
            })?;
        let id = fragment(fields, "id", index)?;
        if let Some(kind) = fragment(fields, "type", index)?
            && kind != "function"
        {
            return Err(InvalidToolCall::at(
                ToolCallRefusal::Malformed,
                index,
                format!("tool call {index} has unsupported type `{kind}`"),
            ));
        }
        let function = match fields.get("function") {
            None | Some(Value::Null) => None,
            Some(Value::Object(function)) => Some(function),
            Some(_) => {
                return Err(InvalidToolCall::at(
                    ToolCallRefusal::Malformed,
                    index,
                    format!("tool call {index} has a `function` that is not an object"),
                ));
            }
        };
        let name = function
            .map(|function| fragment(function, "name", index))
            .transpose()?
            .flatten();
        let arguments = function
            .map(|function| fragment(function, "arguments", index))
            .transpose()?
            .flatten();

        if let Some(id) = id
            && self
                .calls
                .iter()
                .any(|call| call.index != index && call.id == id)
        {
            return Err(InvalidToolCall::at(
                ToolCallRefusal::ConflictingIdentity,
                index,
                format!("tool call id `{id}` was sent for two different indexes"),
            ));
        }
        let mut added = 0;
        let call = self.slot(index)?;
        if let Some(id) = id {
            if call.id.is_empty() {
                if id.len() > MAX_TOOL_CALL_ID_BYTES {
                    return Err(InvalidToolCall::at(
                        ToolCallRefusal::LimitExceeded,
                        index,
                        format!(
                            "tool call {index} has an id longer than {MAX_TOOL_CALL_ID_BYTES} bytes"
                        ),
                    ));
                }
                call.id.push_str(id);
                added += id.len();
            } else if call.id != id {
                return Err(InvalidToolCall::at(
                    ToolCallRefusal::ConflictingIdentity,
                    index,
                    format!(
                        "tool call {index} changed its id from `{}` to `{id}`",
                        call.id
                    ),
                ));
            }
        }
        if let Some(name) = name {
            // `ab` then `ab` may mean `ab` echoed or `abab` fragmented; no
            // second name is taken, so neither is guessed.
            if !call.name.is_empty() {
                return Err(InvalidToolCall::at(
                    ToolCallRefusal::AmbiguousIdentity,
                    index,
                    format!(
                        "tool call {index} sent a second function name; a call names its function in exactly one field"
                    ),
                ));
            }
            if name.len() > MAX_FUNCTION_NAME_BYTES {
                return Err(InvalidToolCall::at(
                    ToolCallRefusal::LimitExceeded,
                    index,
                    format!(
                        "tool call {index} has a function name longer than {MAX_FUNCTION_NAME_BYTES} bytes"
                    ),
                ));
            }
            call.name.push_str(name);
            added += name.len();
        }
        if let Some(arguments) = arguments {
            if call.arguments.len().saturating_add(arguments.len()) > MAX_TOOL_ARGUMENTS_BYTES {
                return Err(InvalidToolCall::at(
                    ToolCallRefusal::LimitExceeded,
                    index,
                    format!(
                        "tool call {index} has arguments longer than {MAX_TOOL_ARGUMENTS_BYTES} bytes"
                    ),
                ));
            }
            call.arguments.push_str(arguments);
            added += arguments.len();
        }
        self.bytes = self.bytes.saturating_add(added);
        Ok(())
    }

    fn slot(&mut self, index: u32) -> Result<&mut CompletedToolCall, InvalidToolCall> {
        let position = match self.calls.binary_search_by_key(&index, |call| call.index) {
            Ok(position) => position,
            Err(position) => {
                if self.calls.len() >= MAX_TOOL_CALLS {
                    return Err(InvalidToolCall::new(
                        ToolCallRefusal::LimitExceeded,
                        format!("the response has more than {MAX_TOOL_CALLS} tool calls"),
                    ));
                }
                self.calls.insert(
                    position,
                    CompletedToolCall {
                        index,
                        ..CompletedToolCall::default()
                    },
                );
                position
            }
        };
        Ok(&mut self.calls[position])
    }

    /// The bytes of ids, names and arguments taken so far.
    #[must_use]
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Whether no tool call fragment has been taken.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.calls.is_empty()
    }

    /// The completed tool calls of a stream that ended with `finish`, ordered
    /// by their wire index. A call is complete only with an id, a valid
    /// function name and arguments that are one JSON object; the arguments
    /// are returned exactly as sent. Tool calls are complete only beside a
    /// `tool_calls` or `stop` finish (a forced function choice finishes with
    /// `stop`), and a `tool_calls` finish without any call is incomplete.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidToolCall`] when any call, or the finish, is not
    /// complete. Nothing partial is returned.
    pub fn finish(
        self,
        finish: Option<ChatFinishReason>,
    ) -> Result<Vec<CompletedToolCall>, InvalidToolCall> {
        if self.calls.is_empty() {
            if finish == Some(ChatFinishReason::ToolCalls) {
                return Err(InvalidToolCall::new(
                    ToolCallRefusal::Incomplete,
                    "the response finished with `tool_calls` but sent no tool call",
                ));
            }
            return Ok(self.calls);
        }
        if !matches!(
            finish,
            Some(ChatFinishReason::ToolCalls | ChatFinishReason::Stop)
        ) {
            return Err(InvalidToolCall::new(
                ToolCallRefusal::Incomplete,
                format!(
                    "tool calls ended with finish reason `{}`",
                    finish.map_or("none", ChatFinishReason::as_openai_str)
                ),
            ));
        }
        for call in &self.calls {
            let index = call.index;
            if call.id.trim().is_empty() {
                return Err(InvalidToolCall::at(
                    ToolCallRefusal::Incomplete,
                    index,
                    format!("tool call {index} has no id"),
                ));
            }
            if !is_function_name(&call.name) {
                let kind = if call.name.is_empty() {
                    ToolCallRefusal::Incomplete
                } else {
                    ToolCallRefusal::Malformed
                };
                return Err(InvalidToolCall::at(
                    kind,
                    index,
                    format!("tool call {index} has no valid function name"),
                ));
            }
            if !is_json_object(&call.arguments) {
                return Err(InvalidToolCall::at(
                    ToolCallRefusal::InvalidArguments,
                    index,
                    format!("tool call {index} arguments are not one JSON object"),
                ));
            }
        }
        Ok(self.calls)
    }
}

/// An optional string field of a tool call delta. Absent, `null` and empty
/// carry nothing; any other non-string is malformed.
fn fragment<'a>(
    fields: &'a Map<String, Value>,
    key: &str,
    index: u32,
) -> Result<Option<&'a str>, InvalidToolCall> {
    match fields.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.as_str()).filter(|value| !value.is_empty())),
        Some(_) => Err(InvalidToolCall::at(
            ToolCallRefusal::Malformed,
            index,
            format!("tool call {index} has a `{key}` that is not a string"),
        )),
    }
}

/// Whether `text` is exactly one JSON object, checked without building it.
fn is_json_object(text: &str) -> bool {
    text.trim_start().starts_with('{') && serde_json::from_str::<IgnoredAny>(text).is_ok()
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
