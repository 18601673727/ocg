//! OpenAI Chat Completions request parsing.
//!
//! Strict request validation for the OpenAI Chat Completions wire format.
//! Unknown fields are rejected on deserialization.

use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;

use super::error::TransportError;

/// An OpenAI Chat Completions request, restricted to the fields this transport
/// explicitly forwards. Unknown fields are rejected on deserialization.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChatRequest {
    pub model: String,
    pub messages: Vec<ChatMessage>,

    #[serde(default)]
    pub stream: Option<bool>,
    #[serde(default)]
    pub stream_options: Option<StreamOptions>,
    #[serde(default)]
    pub store: Option<bool>,

    #[serde(default)]
    pub temperature: Option<f64>,
    #[serde(default)]
    pub top_p: Option<f64>,
    #[serde(default)]
    pub top_k: Option<f64>,
    #[serde(default)]
    pub max_tokens: Option<u32>,
    #[serde(default)]
    pub max_completion_tokens: Option<u32>,
    #[serde(default)]
    pub stop: Option<StopSequences>,
    #[serde(default)]
    pub presence_penalty: Option<f64>,
    #[serde(default)]
    pub frequency_penalty: Option<f64>,
    #[serde(default)]
    pub logit_bias: Option<BTreeMap<String, f64>>,
    #[serde(default)]
    pub logprobs: Option<bool>,
    #[serde(default)]
    pub top_logprobs: Option<u32>,
    #[serde(default)]
    pub n: Option<u32>,
    #[serde(default)]
    pub seed: Option<u64>,
    #[serde(default)]
    pub user: Option<String>,
    #[serde(default)]
    pub metadata: Option<BTreeMap<String, Value>>,
    #[serde(default)]
    pub response_format: Option<ResponseFormatWire>,
    #[serde(default)]
    pub tools: Option<Vec<ToolWire>>,
    #[serde(default)]
    pub tool_choice: Option<ToolChoiceWire>,
    #[serde(default)]
    pub parallel_tool_calls: Option<bool>,
    #[serde(default)]
    pub service_tier: Option<String>,
    #[serde(default)]
    pub reasoning_effort: Option<String>,
}

impl ChatRequest {
    /// Deserialize a request body. Performs no network I/O.
    ///
    /// # Errors
    ///
    /// Returns [`TransportError::InvalidRequest`] when the body does not match
    /// the supported OpenAI Chat Completions contract.
    pub fn from_json(body: Value) -> Result<Self, TransportError> {
        serde_json::from_value(body).map_err(|error| {
            TransportError::invalid("request", format!("could not parse chat request: {error}"))
        })
    }

    /// Deserialize a request body from bytes. Performs no network I/O.
    ///
    /// # Errors
    ///
    /// See [`ChatRequest::from_json`].
    pub fn from_slice(body: &[u8]) -> Result<Self, TransportError> {
        serde_json::from_slice(body).map_err(|error| {
            TransportError::invalid("request", format!("could not parse chat request: {error}"))
        })
    }

    /// Validate every field before dispatch.
    ///
    /// # Errors
    ///
    /// Returns a typed [`TransportError`] for the first unsupported or invalid
    /// field.
    pub fn validate(&self) -> Result<(), TransportError> {
        if self.model.trim().is_empty() {
            return Err(TransportError::invalid("model", "model must not be empty"));
        }
        if self.messages.is_empty() {
            return Err(TransportError::invalid(
                "messages",
                "at least one message is required",
            ));
        }
        if self.stream == Some(false) {
            return Err(TransportError::unsupported(
                "stream",
                "only streaming chat completions are supported",
            ));
        }
        if let Some(options) = &self.stream_options {
            if options.include_usage == Some(false) {
                return Err(TransportError::unsupported(
                    "stream_options.include_usage",
                    "usage must be included for settlement",
                ));
            }
        }
        if let Some(n) = self.n {
            if n != 1 {
                return Err(TransportError::unsupported("n", "only n = 1 is supported"));
            }
        }
        if self.max_tokens.is_some() && self.max_completion_tokens.is_some() {
            return Err(TransportError::invalid(
                "max_tokens",
                "max_tokens and max_completion_tokens are mutually exclusive",
            ));
        }
        if let Some(effort) = &self.reasoning_effort {
            parse_reasoning_effort(effort)?;
        }
        if let Some(format) = &self.response_format {
            format.validate()?;
        }
        if let Some(tool_choice) = &self.tool_choice {
            validate_tool_choice(tool_choice)?;
        }
        if let Some(tools) = &self.tools {
            for (index, tool) in tools.iter().enumerate() {
                tool.validate(index)?;
            }
        }
        for (index, message) in self.messages.iter().enumerate() {
            message.validate(index)?;
        }
        Ok(())
    }
}

/// OpenAI `stream_options`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamOptions {
    #[serde(default)]
    pub include_usage: Option<bool>,
}

/// OpenAI `stop`: either one string or an array of strings.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum StopSequences {
    One(String),
    Many(Vec<String>),
}

impl StopSequences {
    #[must_use]
    pub fn to_vec(&self) -> Vec<String> {
        match self {
            Self::One(one) => vec![one.clone()],
            Self::Many(many) => many.clone(),
        }
    }
}

/// One message in the request.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChatMessage {
    pub role: ChatRole,
    #[serde(default)]
    pub content: Option<MessageContentWire>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub tool_calls: Option<Vec<ChatToolCallWire>>,
    #[serde(default)]
    pub tool_call_id: Option<String>,
    #[serde(default)]
    pub reasoning_content: Option<String>,
    #[serde(default)]
    pub refusal: Option<String>,
}

impl ChatMessage {
    fn validate(&self, index: usize) -> Result<(), TransportError> {
        let field = |suffix: &str| format!("messages[{index}].{suffix}");
        if let Some(name) = &self.name {
            return Err(TransportError::unsupported(
                field("name"),
                format!("per-message names are not forwarded (`{name}`)"),
            ));
        }
        if let Some(refusal) = &self.refusal {
            return Err(TransportError::unsupported(
                field("refusal"),
                format!("assistant refusals are not forwarded (`{refusal}`)"),
            ));
        }
        match self.role {
            ChatRole::Function => Err(TransportError::unsupported(
                field("role"),
                "the legacy `function` role is not supported",
            )),
            ChatRole::System | ChatRole::Developer => {
                self.require_text_content(&field("content"))?;
                self.reject_tool_fields(index)?;
                Ok(())
            }
            ChatRole::User => {
                self.require_text_content(&field("content"))?;
                self.reject_tool_fields(index)?;
                Ok(())
            }
            ChatRole::Assistant => {
                if let Some(calls) = &self.tool_calls {
                    for (call_index, call) in calls.iter().enumerate() {
                        call.validate(&field(&format!("tool_calls[{call_index}]")))?;
                    }
                }
                let has_parts = self.content.is_some()
                    || self
                        .tool_calls
                        .as_ref()
                        .is_some_and(|calls| !calls.is_empty())
                    || self
                        .reasoning_content
                        .as_ref()
                        .is_some_and(|reasoning| !reasoning.is_empty());
                if !has_parts {
                    return Err(TransportError::invalid(
                        field("content"),
                        "assistant messages must carry content, reasoning or tool_calls",
                    ));
                }
                if self.tool_call_id.is_some() {
                    return Err(TransportError::invalid(
                        field("tool_call_id"),
                        "assistant messages must not carry tool_call_id",
                    ));
                }
                Ok(())
            }
            ChatRole::Tool => {
                if self.tool_call_id.is_none() {
                    return Err(TransportError::invalid(
                        field("tool_call_id"),
                        "tool messages require tool_call_id",
                    ));
                }
                if self.tool_calls.is_some() {
                    return Err(TransportError::invalid(
                        field("tool_calls"),
                        "tool messages must not carry tool_calls",
                    ));
                }
                self.require_text_content(&field("content"))?;
                Ok(())
            }
        }
    }

    fn reject_tool_fields(&self, index: usize) -> Result<(), TransportError> {
        let field = |suffix: &str| format!("messages[{index}].{suffix}");
        if self.tool_calls.is_some() {
            return Err(TransportError::invalid(
                field("tool_calls"),
                "tool_calls are only valid on assistant messages",
            ));
        }
        if self.tool_call_id.is_some() {
            return Err(TransportError::invalid(
                field("tool_call_id"),
                "tool_call_id is only valid on tool messages",
            ));
        }
        if self.reasoning_content.is_some() {
            return Err(TransportError::invalid(
                field("reasoning_content"),
                "reasoning_content is only valid on assistant messages",
            ));
        }
        Ok(())
    }

    fn require_text_content(
        &self,
        field: &str,
    ) -> Result<Vec<super::normalize::ContentPart>, TransportError> {
        let content = self.content.as_ref().ok_or_else(|| {
            TransportError::invalid(field.to_string(), "message content is required")
        })?;
        content.to_text_parts(field)
    }
}

/// Message role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChatRole {
    System,
    User,
    Assistant,
    Tool,
    Developer,
    Function,
}

/// Message content: a string or an array of content parts.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum MessageContentWire {
    Text(String),
    Parts(Vec<ContentPartWire>),
}

impl MessageContentWire {
    fn to_text_parts(
        &self,
        field: &str,
    ) -> Result<Vec<super::normalize::ContentPart>, TransportError> {
        let parts: Vec<super::normalize::ContentPart> = match self {
            Self::Text(text) => vec![super::normalize::ContentPart::text(text.clone())],
            Self::Parts(parts) => {
                if parts.is_empty() {
                    return Err(TransportError::invalid(
                        field.to_string(),
                        "content parts must not be empty",
                    ));
                }
                parts
                    .iter()
                    .map(|part| match part {
                        ContentPartWire::Text { text } => {
                            super::normalize::ContentPart::text(text.clone())
                        }
                    })
                    .collect()
            }
        };
        Ok(parts)
    }
}

/// A supported content part. Non-text parts (images, audio, files) are
/// rejected during deserialization.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ContentPartWire {
    Text { text: String },
}

/// One tool definition.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolWire {
    #[serde(rename = "type")]
    pub tool_type: String,
    pub function: FunctionDefinitionWire,
}

impl ToolWire {
    fn validate(&self, index: usize) -> Result<(), TransportError> {
        if self.tool_type != "function" {
            return Err(TransportError::unsupported(
                format!("tools[{index}].type"),
                format!("unsupported tool type `{}`", self.tool_type),
            ));
        }
        if self.function.name.trim().is_empty() {
            return Err(TransportError::invalid(
                format!("tools[{index}].function.name"),
                "tool name must not be empty",
            ));
        }
        Ok(())
    }
}

/// A function tool definition.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FunctionDefinitionWire {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub parameters: Option<Value>,
    #[serde(default)]
    pub strict: Option<bool>,
}

/// One tool call in an assistant message.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChatToolCallWire {
    pub id: String,
    #[serde(rename = "type", default)]
    pub tool_type: Option<String>,
    pub function: ChatFunctionCallWire,
}

impl ChatToolCallWire {
    fn validate(&self, field: &str) -> Result<(), TransportError> {
        if self.id.trim().is_empty() {
            return Err(TransportError::invalid(
                format!("{field}.id"),
                "tool call id must not be empty",
            ));
        }
        if let Some(kind) = &self.tool_type {
            if kind != "function" {
                return Err(TransportError::unsupported(
                    format!("{field}.type"),
                    format!("unsupported tool call type `{kind}`"),
                ));
            }
        }
        if self.function.name.trim().is_empty() {
            return Err(TransportError::invalid(
                format!("{field}.function.name"),
                "tool name must not be empty",
            ));
        }
        parse_tool_arguments(
            &self.function.arguments,
            &format!("{field}.function.arguments"),
        )?;
        Ok(())
    }
}

/// A function call in an assistant message.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChatFunctionCallWire {
    pub name: String,
    #[serde(default)]
    pub arguments: String,
}

fn parse_tool_arguments(raw: &str, field: &str) -> Result<Value, TransportError> {
    if raw.is_empty() {
        return Ok(Value::Object(serde_json::Map::new()));
    }
    serde_json::from_str(raw).map_err(|error| {
        TransportError::invalid(
            field.to_string(),
            format!("invalid tool arguments JSON: {error}"),
        )
    })
}

fn parse_reasoning_effort(effort: &str) -> Result<(), TransportError> {
    match effort {
        "none" | "minimal" | "low" | "medium" | "high" | "xhigh" => Ok(()),
        other => Err(TransportError::unsupported(
            "reasoning_effort",
            format!("unsupported reasoning effort `{other}`"),
        )),
    }
}

fn validate_tool_choice(choice: &ToolChoiceWire) -> Result<(), TransportError> {
    match choice {
        ToolChoiceWire::Mode(mode) => match mode.as_str() {
            "auto" | "none" | "required" => Ok(()),
            other => Err(TransportError::invalid(
                "tool_choice",
                format!("unknown tool_choice `{other}`"),
            )),
        },
        ToolChoiceWire::Named(named) => {
            if named.kind != "function" {
                return Err(TransportError::unsupported(
                    "tool_choice.type",
                    format!("unsupported tool_choice type `{}`", named.kind),
                ));
            }
            if named.function.name.trim().is_empty() {
                Err(TransportError::invalid(
                    "tool_choice.function.name",
                    "tool choice function name must not be empty",
                ))
            } else {
                Ok(())
            }
        }
    }
}

/// OpenAI `tool_choice`.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum ToolChoiceWire {
    Mode(String),
    Named(NamedToolChoiceWire),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamedToolChoiceWire {
    #[serde(rename = "type")]
    pub kind: String,
    pub function: NamedFunctionWire,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamedFunctionWire {
    pub name: String,
}

/// OpenAI `response_format`.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResponseFormatWire {
    Text {},
    JsonObject {},
    JsonSchema { json_schema: JsonSchemaWire },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JsonSchemaWire {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub schema: Option<Value>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub strict: Option<bool>,
}

impl ResponseFormatWire {
    fn validate(&self) -> Result<(), TransportError> {
        match self {
            ResponseFormatWire::Text {} | ResponseFormatWire::JsonObject {} => Ok(()),
            ResponseFormatWire::JsonSchema { json_schema } => {
                if json_schema.schema.is_none() {
                    return Err(TransportError::invalid(
                        "response_format.json_schema.schema",
                        "a JSON schema is required for response_format type json_schema",
                    ));
                }
                Ok(())
            }
        }
    }
}
