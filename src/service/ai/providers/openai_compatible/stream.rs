use serde::de::IgnoredAny;
use serde_json::{Map, Value};

use super::request::{
    MAX_FUNCTION_NAME_BYTES, MAX_TOOL_ARGUMENTS_BYTES, MAX_TOOL_CALL_ID_BYTES, MAX_TOOL_CALLS,
    is_function_name,
};

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

#[derive(Debug, Clone, Default, PartialEq)]
pub struct NormalizedUsage {
    pub input_tokens: Option<u32>,
    pub output_tokens: Option<u32>,
    pub cache_read_tokens: Option<u32>,
    pub cache_write_tokens: Option<u32>,
    pub reasoning_tokens: Option<u32>,
    pub raw: Option<Value>,
}

impl NormalizedUsage {
    #[must_use]
    pub fn total_tokens(&self) -> Option<u32> {
        match (self.input_tokens, self.output_tokens) {
            (Some(input), Some(output)) => Some(input.saturating_add(output)),
            _ => None,
        }
    }
}

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
        arguments: String,
    },
    Metadata {
        id: Option<String>,
        model: Option<String>,
        timestamp: Option<String>,
    },
    RoundBegan,
    RoundReset,
    Finish {
        reason: ChatFinishReason,
        raw_reason: Option<String>,
        usage: NormalizedUsage,
    },
    Error(super::error::TransportError),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompletedToolCall {
    pub index: u32,
    pub id: String,
    pub name: String,
    pub arguments: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolCallRefusal {
    Malformed,
    ConflictingIdentity,
    AmbiguousIdentity,
    LimitExceeded,
    Incomplete,
    Unauthorized,
    MissingSchema,
    UnsupportedSchema,
    InvalidArguments,
    NoDecider,
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
            Self::NoDecider => "no decider",
        }
    }
}

impl std::fmt::Display for ToolCallRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid provider tool call ({kind}): {message}")]
pub struct InvalidToolCall {
    pub kind: ToolCallRefusal,
    pub index: Option<u32>,
    pub message: String,
}

impl InvalidToolCall {
    #[must_use]
    pub fn new(kind: ToolCallRefusal, message: impl Into<String>) -> Self {
        Self {
            kind,
            index: None,
            message: message.into(),
        }
    }

    #[must_use]
    pub fn at(kind: ToolCallRefusal, index: u32, message: impl Into<String>) -> Self {
        Self {
            kind,
            index: Some(index),
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ToolCallAssembler {
    calls: Vec<CompletedToolCall>,
    bytes: usize,
    inferred_index: bool,
    empty_arguments: Vec<u32>,
}

impl ToolCallAssembler {
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
        if deltas.len() > 1 && deltas.iter().any(|delta| delta.get("index").is_none()) {
            return Err(InvalidToolCall::new(
                ToolCallRefusal::Malformed,
                "a tool call delta has no valid `index` in a multiple-call response",
            ));
        }
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
        let inferred = match fields.get("index") {
            None if self.calls.len() <= 1 => {
                self.inferred_index = true;
                Some(self.calls.first().map_or(0, |call| call.index))
            }
            _ => None,
        };
        let index = fields
            .get("index")
            .and_then(Value::as_u64)
            .and_then(|index| u32::try_from(index).ok())
            .or(inferred)
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

        if function
            .is_some_and(|function| function.get("arguments").and_then(Value::as_str) == Some(""))
            && !self.empty_arguments.contains(&index)
        {
            self.empty_arguments.push(index);
        }
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
        if let Some(name) = name.filter(|name| *name != call.name) {
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
                if self.inferred_index && !self.calls.is_empty() {
                    return Err(InvalidToolCall::new(
                        ToolCallRefusal::Malformed,
                        "a tool call delta has no valid `index` in a multiple-call response",
                    ));
                }
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

    #[must_use]
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.calls.is_empty()
    }

    pub fn finish(
        mut self,
        finish: Option<ChatFinishReason>,
    ) -> Result<(Vec<CompletedToolCall>, Vec<InvalidToolCall>), InvalidToolCall> {
        if self.calls.is_empty() {
            if finish == Some(ChatFinishReason::ToolCalls) {
                return Err(InvalidToolCall::new(
                    ToolCallRefusal::Incomplete,
                    "the response finished with `tool_calls` but sent no tool call",
                ));
            }
            return Ok((self.calls, Vec::new()));
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
        let mut refused = Vec::new();
        for call in &mut self.calls {
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
            if call.arguments.is_empty() && self.empty_arguments.contains(&index) {
                call.arguments.push_str("{}");
            }
            if !is_json_object(&call.arguments) {
                refused.push(InvalidToolCall::at(
                    ToolCallRefusal::InvalidArguments,
                    index,
                    format!("tool call {index} arguments are not one JSON object"),
                ));
            }
        }
        Ok((self.calls, refused))
    }
}

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

fn is_json_object(text: &str) -> bool {
    text.trim_start().starts_with('{') && serde_json::from_str::<IgnoredAny>(text).is_ok()
}

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
