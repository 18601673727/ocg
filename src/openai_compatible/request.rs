//! OpenAI Chat Completions request parsing.
//!
//! Strict request validation for the OpenAI Chat Completions wire format.
//! Unknown fields are rejected on deserialization.

use jsonschema::Validator;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashSet};

use super::admission;
use super::error::TransportError;
use super::image::{self, MAX_REQUEST_IMAGE_BYTES, MAX_REQUEST_IMAGES};
use super::stream::{CompletedToolCall, InvalidToolCall, ToolCallRefusal};

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
        if let Some(options) = &self.stream_options
            && options.include_usage == Some(false)
        {
            return Err(TransportError::unsupported(
                "stream_options.include_usage",
                "usage must be included for settlement",
            ));
        }
        if let Some(n) = self.n
            && n != 1
        {
            return Err(TransportError::unsupported("n", "only n = 1 is supported"));
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
        let mut images = ImageTally::default();
        for (index, message) in self.messages.iter().enumerate() {
            message.validate(index, &mut images)?;
        }
        Ok(())
    }
}

/// The conversation part of one outgoing Chat Completions request: its
/// messages, in order, and the function tools offered with them. The model
/// and sampling parameters are deliberately absent; they come from the
/// frozen execution configuration and nothing here can change them.
#[derive(Debug, Clone, Default, Serialize)]
pub struct ChatConversation {
    #[serde(skip)]
    pub dropped_turns: u32,
    #[serde(skip)]
    pub image_attachments: BTreeMap<usize, Vec<crate::persistence::AttachmentId>>,
    pub messages: Vec<ChatMessage>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<ToolWire>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<ToolChoiceWire>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parallel_tool_calls: Option<bool>,
    /// The requested response shape for this turn; model text is kept verbatim.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_format: Option<ResponseFormatWire>,
}

impl ChatConversation {
    /// Validates the conversation before it is sent: every message and tool
    /// definition, their limits, its images and their total, and the
    /// tool-call structure. Each assistant
    /// tool call has a unique id and is answered by exactly one tool message,
    /// and those answers follow it before any other message.
    ///
    /// # Errors
    ///
    /// Returns a typed [`TransportError`] for the first invalid or
    /// unsupported field.
    pub fn validate(&self) -> Result<(), TransportError> {
        if self.messages.is_empty() {
            return Err(TransportError::invalid(
                "messages",
                "at least one message is required",
            ));
        }
        if self.tools.len() > MAX_TOOLS {
            return Err(TransportError::invalid(
                "tools",
                format!("at most {MAX_TOOLS} tools may be offered"),
            ));
        }
        for (index, tool) in self.tools.iter().enumerate() {
            tool.validate(index)?;
            if self.tools[..index]
                .iter()
                .any(|earlier| earlier.function.name == tool.function.name)
            {
                return Err(TransportError::invalid(
                    format!("tools[{index}].function.name"),
                    format!("tool `{}` is offered twice", tool.function.name),
                ));
            }
        }
        if self.tools.is_empty() {
            if self.tool_choice.is_some() {
                return Err(TransportError::invalid(
                    "tool_choice",
                    "tool_choice requires tools",
                ));
            }
            if self.parallel_tool_calls.is_some() {
                return Err(TransportError::invalid(
                    "parallel_tool_calls",
                    "parallel_tool_calls requires tools",
                ));
            }
        }
        if let Some(choice) = &self.tool_choice {
            validate_tool_choice(choice)?;
            if let ToolChoiceWire::Named(named) = choice
                && !self.offers(&named.function.name)
            {
                return Err(TransportError::invalid(
                    "tool_choice.function.name",
                    format!("tool `{}` is not offered", named.function.name),
                ));
            }
        }

        let mut issued = HashSet::new();
        let mut pending: Vec<&str> = Vec::new();
        let mut images = ImageTally::default();
        for (index, message) in self.messages.iter().enumerate() {
            message.validate(index, &mut images)?;
            if message.role == ChatRole::Tool {
                let id = message.tool_call_id.as_deref().unwrap_or_default();
                let Some(position) = pending.iter().position(|pending| *pending == id) else {
                    return Err(TransportError::invalid(
                        format!("messages[{index}].tool_call_id"),
                        format!("`{id}` does not answer an unanswered tool call just before it"),
                    ));
                };
                pending.swap_remove(position);
                continue;
            }
            if let Some(id) = pending.first() {
                return Err(TransportError::invalid(
                    format!("messages[{index}]"),
                    format!("tool call `{id}` must be answered before the next message"),
                ));
            }
            let Some(calls) = &message.tool_calls else {
                continue;
            };
            if calls.is_empty() || calls.len() > MAX_TOOL_CALLS {
                return Err(TransportError::invalid(
                    format!("messages[{index}].tool_calls"),
                    format!("an assistant message carries 1 to {MAX_TOOL_CALLS} tool calls"),
                ));
            }
            for (call_index, call) in calls.iter().enumerate() {
                if !issued.insert(call.id.as_str()) {
                    return Err(TransportError::invalid(
                        format!("messages[{index}].tool_calls[{call_index}].id"),
                        format!("tool call id `{}` is used twice", call.id),
                    ));
                }
                pending.push(&call.id);
            }
        }
        if let Some(id) = pending.first() {
            return Err(TransportError::invalid(
                "messages",
                format!("tool call `{id}` is never answered"),
            ));
        }
        Ok(())
    }

    fn offers(&self, name: &str) -> bool {
        self.tools.iter().any(|tool| tool.function.name == name)
    }

    /// Whether any message carries an image part. A conversation that once
    /// did keeps doing so in every continuation that resends it, tool turns
    /// included.
    #[must_use]
    pub fn carries_images(&self) -> bool {
        self.messages.iter().any(|message| {
            matches!(&message.content, Some(MessageContentWire::Parts(parts))
                if parts.iter().any(|part| matches!(part, ContentPartWire::ImageUrl { .. })))
        })
    }

    /// Admits the completed tool calls a Provider returned for this
    /// conversation only as far as this request allowed them. Each call must
    /// name exactly one function tool offered here, under that exact name;
    /// none is admitted with `tool_choice: none`, only the named function
    /// when one was forced, and one call at most when parallel calls were
    /// turned off. Its arguments must then be one strict JSON object that
    /// satisfies the parameter schema offered for that function; a function
    /// offered without one admits no call. Nothing the Provider sent, not a
    /// name, a definition or an argument, widens what is admitted, and no
    /// call is changed to fit.
    ///
    /// Admission is scoped to this request and is not permission to execute
    /// a call: whatever executes one must still check it against OCG's own
    /// tool registry, policies and effective permissions.
    ///
    /// # Errors
    ///
    /// Returns every answerable refusal of the response, one per call,
    /// ordered by index. An invalid offer, schema or response-wide
    /// constraint stays an error.
    pub fn admit_tool_calls(
        &self,
        calls: &[CompletedToolCall],
        assembled: Vec<InvalidToolCall>,
    ) -> Result<Vec<InvalidToolCall>, InvalidToolCall> {
        if self.parallel_tool_calls == Some(false) && calls.len() > 1 {
            return Err(InvalidToolCall::new(
                ToolCallRefusal::Unauthorized,
                format!(
                    "{} tool calls were returned with parallel_tool_calls off",
                    calls.len()
                ),
            ));
        }
        // The assembler's per-call refusals, by wire index. A name this
        // request did not allow replaces one; an offered and chosen call
        // keeps its own.
        let mut assembled: std::collections::BTreeMap<u32, InvalidToolCall> = assembled
            .into_iter()
            .filter_map(|refusal| refusal.index.map(|index| (index, refusal)))
            .collect();
        // Each offered function's schema is compiled once, when a call first
        // names it.
        let mut validators: Vec<Option<Validator>> = self.tools.iter().map(|_| None).collect();
        let mut refused = Vec::new();
        for call in calls {
            let index = call.index;
            let assembled = assembled.remove(&index);
            let unauthorized = || {
                InvalidToolCall::at(
                    ToolCallRefusal::Unauthorized,
                    index,
                    format!(
                        "tool call {index} names `{}`, which this request did not allow",
                        call.name
                    ),
                )
            };
            let chosen = match &self.tool_choice {
                Some(ToolChoiceWire::Mode(mode)) => match mode.as_str() {
                    "auto" | "required" => true,
                    "none" => false,
                    _ => return Err(unauthorized()),
                },
                Some(ToolChoiceWire::Named(named)) => named.function.name == call.name,
                None => true,
            };
            let mut offered = self
                .tools
                .iter()
                .enumerate()
                .filter(|(_, tool)| tool.function.name == call.name);
            let (position, tool) = match (offered.next(), offered.next()) {
                (Some(offered), None) if offered.1.tool_type == "function" => offered,
                (None, None) => {
                    refused.push(unauthorized());
                    continue;
                }
                _ => return Err(unauthorized()),
            };
            if !chosen {
                refused.push(unauthorized());
                continue;
            }
            if let Some(refusal) = assembled {
                refused.push(refusal);
                continue;
            }
            let validator = match &mut validators[position] {
                Some(validator) => validator,
                slot => slot.insert(admission::parameter_validator(
                    index,
                    &tool.function.name,
                    tool.function.parameters.as_ref(),
                )?),
            };
            if let Err(refusal) =
                admission::admit_arguments(index, &call.name, &call.arguments, validator)
            {
                refused.push(refusal);
            }
        }
        Ok(refused)
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
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ChatMessage {
    pub role: ChatRole,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<MessageContentWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<ChatToolCallWire>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    /// Forwarded only when set: some Providers require an assistant turn's
    /// reasoning back with its tool calls, others refuse it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_content: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refusal: Option<String>,
}

impl ChatMessage {
    fn with_role(role: ChatRole) -> Self {
        Self {
            role,
            content: None,
            name: None,
            tool_calls: None,
            tool_call_id: None,
            reasoning_content: None,
            refusal: None,
        }
    }

    #[must_use]
    pub fn system(content: impl Into<String>) -> Self {
        Self {
            content: Some(MessageContentWire::Text(content.into())),
            ..Self::with_role(ChatRole::System)
        }
    }

    #[must_use]
    pub fn user(content: impl Into<String>) -> Self {
        Self {
            content: Some(MessageContentWire::Text(content.into())),
            ..Self::with_role(ChatRole::User)
        }
    }

    /// A user turn of content parts, in order: text, images, or both.
    #[must_use]
    pub fn user_parts(parts: Vec<ContentPartWire>) -> Self {
        Self {
            content: Some(MessageContentWire::Parts(parts)),
            ..Self::with_role(ChatRole::User)
        }
    }

    /// An assistant turn, as a Provider returned it: its text, if any, and
    /// the tool calls it made, if any.
    #[must_use]
    pub fn assistant(content: Option<String>, tool_calls: Vec<ChatToolCallWire>) -> Self {
        Self {
            content: content.map(MessageContentWire::Text),
            tool_calls: Some(tool_calls).filter(|calls| !calls.is_empty()),
            ..Self::with_role(ChatRole::Assistant)
        }
    }

    /// The result of the tool call `tool_call_id`.
    #[must_use]
    pub fn tool(tool_call_id: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            content: Some(MessageContentWire::Text(content.into())),
            tool_call_id: Some(tool_call_id.into()),
            ..Self::with_role(ChatRole::Tool)
        }
    }
}

/// The images a request's messages carry so far.
#[derive(Default)]
struct ImageTally {
    count: usize,
    bytes: usize,
}

impl ChatMessage {
    fn validate(&self, index: usize, images: &mut ImageTally) -> Result<(), TransportError> {
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
                self.require_user_content(&field("content"), images)?;
                self.reject_tool_fields(index)?;
                Ok(())
            }
            ChatRole::Assistant => {
                if let Some(MessageContentWire::Parts(parts)) = &self.content
                    && parts
                        .iter()
                        .any(|part| matches!(part, ContentPartWire::ImageUrl { .. }))
                {
                    return Err(TransportError::unsupported(
                        field("content"),
                        "images are accepted in user messages only",
                    ));
                }
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

    /// A user message's content: text, or parts of text and images. Each
    /// image is checked, and counted toward the request's image limits.
    fn require_user_content(
        &self,
        field: &str,
        images: &mut ImageTally,
    ) -> Result<(), TransportError> {
        let content = self.content.as_ref().ok_or_else(|| {
            TransportError::invalid(field.to_string(), "message content is required")
        })?;
        let parts = match content {
            MessageContentWire::Text(_) => return Ok(()),
            MessageContentWire::Parts(parts) => parts,
        };
        if parts.is_empty() {
            return Err(TransportError::invalid(
                field.to_string(),
                "content parts must not be empty",
            ));
        }
        for (position, part) in parts.iter().enumerate() {
            let ContentPartWire::ImageUrl { image_url } = part else {
                continue;
            };
            let field = format!("{field}[{position}].image_url.url");
            let bytes = image::check_data_url(&image_url.url).map_err(|error| match error {
                image::ImageError::UnsupportedType | image::ImageError::NotDataUrl => {
                    TransportError::unsupported(field.clone(), error.to_string())
                }
                _ => TransportError::invalid(field.clone(), error.to_string()),
            })?;
            images.count += 1;
            images.bytes = images.bytes.saturating_add(bytes);
            if images.count > MAX_REQUEST_IMAGES {
                return Err(TransportError::invalid(
                    field,
                    format!("a request may carry at most {MAX_REQUEST_IMAGES} images"),
                ));
            }
            if images.bytes > MAX_REQUEST_IMAGE_BYTES {
                return Err(TransportError::invalid(
                    field,
                    format!(
                        "a request's images may not exceed {MAX_REQUEST_IMAGE_BYTES} bytes together"
                    ),
                ));
            }
        }
        Ok(())
    }
}

/// Message role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
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
#[derive(Debug, Clone, Deserialize, Serialize)]
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
                            Ok(super::normalize::ContentPart::text(text.clone()))
                        }
                        ContentPartWire::ImageUrl { .. } => Err(TransportError::unsupported(
                            field.to_string(),
                            "images are accepted in user messages only",
                        )),
                    })
                    .collect::<Result<_, _>>()?
            }
        };
        Ok(parts)
    }
}

/// A supported content part: text, or, in a user message, an image as an
/// inline `data:` URL, see [`super::image`]. Other parts (audio, files) are
/// rejected during deserialization.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ContentPartWire {
    Text { text: String },
    ImageUrl { image_url: ImageUrlWire },
}

/// The image of an `image_url` part. Only its URL is forwarded; its `Debug`
/// never shows the URL, which carries the whole image.
#[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ImageUrlWire {
    pub url: String,
}

impl std::fmt::Debug for ImageUrlWire {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let header = self
            .url
            .split_once(',')
            .map_or("", |(header, _)| header)
            .strip_prefix("data:")
            .unwrap_or("");
        formatter
            .debug_struct("ImageUrlWire")
            .field(
                "url",
                &format_args!("data:{header},<{} bytes>", self.url.len()),
            )
            .finish()
    }
}

/// One tool definition.
#[derive(Debug, Clone, Deserialize, Serialize)]
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
        require_function_name(&self.function.name, &format!("tools[{index}]"))?;
        if let Some(parameters) = &self.function.parameters
            && !parameters.is_object()
        {
            return Err(TransportError::invalid(
                format!("tools[{index}].function.parameters"),
                "function parameters must be a JSON Schema object",
            ));
        }
        if !serializes_within(&self.function, MAX_TOOL_DEFINITION_BYTES) {
            return Err(TransportError::invalid(
                format!("tools[{index}].function"),
                format!("a tool definition must not exceed {MAX_TOOL_DEFINITION_BYTES} bytes"),
            ));
        }
        Ok(())
    }
}

/// Most tools one request may offer.
pub const MAX_TOOLS: usize = 128;
/// Most tool calls one assistant message or one streamed response may carry.
pub const MAX_TOOL_CALLS: usize = 128;
/// Longest function name, as the OpenAI function-calling contract allows.
pub const MAX_FUNCTION_NAME_BYTES: usize = 64;
/// Longest tool call id.
pub const MAX_TOOL_CALL_ID_BYTES: usize = 256;
/// Largest compact serialization of one function definition.
pub const MAX_TOOL_DEFINITION_BYTES: usize = 64 * 1024;
/// Largest serialized arguments of one tool call.
pub const MAX_TOOL_ARGUMENTS_BYTES: usize = 1024 * 1024;

/// Whether `name` is a function name of the OpenAI contract: 1 to 64 ASCII
/// letters, digits, underscores or dashes.
#[must_use]
pub fn is_function_name(name: &str) -> bool {
    (1..=MAX_FUNCTION_NAME_BYTES).contains(&name.len())
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

/// Requires a valid function name. `field` names the owning tool entry
/// and is used to build the error path `{field}.function.name`.
fn require_function_name(name: &str, field: &str) -> Result<(), TransportError> {
    if !is_function_name(name) {
        return Err(TransportError::invalid(
            format!("{field}.function.name"),
            format!(
                "a function name is 1 to {MAX_FUNCTION_NAME_BYTES} ASCII letters, digits, `_` or `-`"
            ),
        ));
    }
    Ok(())
}

/// Whether the compact JSON of `value` fits in `limit` bytes. Counting stops
/// at the limit, so an oversized value is never serialized in full.
fn serializes_within(value: &impl Serialize, limit: usize) -> bool {
    struct Budget(usize);

    impl std::io::Write for Budget {
        fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_sub(buffer.len())
                .ok_or_else(|| std::io::Error::other("over budget"))?;
            Ok(buffer.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    serde_json::to_writer(Budget(limit), value).is_ok()
}

/// A function tool definition.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FunctionDefinitionWire {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parameters: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub strict: Option<bool>,
}

/// One tool call in an assistant message.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ChatToolCallWire {
    pub id: String,
    /// Optional when read; validation admits only `function`, which is
    /// always written, as the outgoing contract requires it.
    #[serde(rename = "type", default, serialize_with = "function_type")]
    pub tool_type: Option<String>,
    pub function: ChatFunctionCallWire,
}

fn function_type<S: serde::Serializer>(
    _: &Option<String>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_str("function")
}

impl From<&CompletedToolCall> for ChatToolCallWire {
    /// The call as an assistant message carries it back, arguments unchanged.
    fn from(call: &CompletedToolCall) -> Self {
        Self {
            id: call.id.clone(),
            tool_type: Some("function".to_owned()),
            function: ChatFunctionCallWire {
                name: call.name.clone(),
                arguments: call.arguments.clone(),
            },
        }
    }
}

impl ChatToolCallWire {
    fn validate(&self, field: &str) -> Result<(), TransportError> {
        if self.id.trim().is_empty() {
            return Err(TransportError::invalid(
                format!("{field}.id"),
                "tool call id must not be empty",
            ));
        }
        if self.id.len() > MAX_TOOL_CALL_ID_BYTES {
            return Err(TransportError::invalid(
                format!("{field}.id"),
                format!("a tool call id must not exceed {MAX_TOOL_CALL_ID_BYTES} bytes"),
            ));
        }
        if self.function.arguments.len() > MAX_TOOL_ARGUMENTS_BYTES {
            return Err(TransportError::invalid(
                format!("{field}.function.arguments"),
                format!("tool arguments must not exceed {MAX_TOOL_ARGUMENTS_BYTES} bytes"),
            ));
        }
        if let Some(kind) = &self.tool_type
            && kind != "function"
        {
            return Err(TransportError::unsupported(
                format!("{field}.type"),
                format!("unsupported tool call type `{kind}`"),
            ));
        }
        require_function_name(&self.function.name, field)?;
        parse_tool_arguments(
            &self.function.arguments,
            &format!("{field}.function.arguments"),
        )?;
        Ok(())
    }
}

/// A function call in an assistant message. `arguments` is the serialized
/// JSON exactly as it was received, never re-encoded.
#[derive(Debug, Clone, Deserialize, Serialize)]
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

pub(crate) fn parse_reasoning_effort(effort: &str) -> Result<(), TransportError> {
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
            require_function_name(&named.function.name, "tool_choice")
        }
    }
}

/// OpenAI `tool_choice`.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum ToolChoiceWire {
    Mode(String),
    Named(NamedToolChoiceWire),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NamedToolChoiceWire {
    #[serde(rename = "type")]
    pub kind: String,
    pub function: NamedFunctionWire,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NamedFunctionWire {
    pub name: String,
}

/// OpenAI `response_format`.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResponseFormatWire {
    Text {},
    JsonObject {},
    JsonSchema { json_schema: JsonSchemaWire },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JsonSchemaWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub strict: Option<bool>,
}

impl From<crate::persistence::ResponseFormat> for ResponseFormatWire {
    fn from(format: crate::persistence::ResponseFormat) -> Self {
        match format {
            crate::persistence::ResponseFormat::JsonObject => Self::JsonObject {},
            crate::persistence::ResponseFormat::JsonSchema {
                name,
                schema,
                strict,
            } => Self::JsonSchema {
                json_schema: JsonSchemaWire {
                    name: Some(name),
                    schema: Some(schema),
                    strict,
                    description: None,
                },
            },
        }
    }
}

impl ResponseFormatWire {
    pub(crate) fn validate(&self) -> Result<(), TransportError> {
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::openai_compatible::image::tests::{PNG_BASE64, jpeg, png};
    use crate::openai_compatible::image::{ImageInput, ImageMime, MAX_IMAGE_BYTES};

    fn png_part() -> ContentPartWire {
        ImageInput::new(ImageMime::Png, png())
            .expect("a PNG")
            .part()
    }

    fn jpeg_part() -> ContentPartWire {
        ImageInput::new(ImageMime::Jpeg, jpeg())
            .expect("a JPEG")
            .part()
    }

    fn text(text: &str) -> ContentPartWire {
        ContentPartWire::Text {
            text: text.to_owned(),
        }
    }

    fn image_url(url: &str) -> ContentPartWire {
        ContentPartWire::ImageUrl {
            image_url: ImageUrlWire {
                url: url.to_owned(),
            },
        }
    }

    fn conversation(messages: Vec<ChatMessage>) -> ChatConversation {
        ChatConversation {
            messages,
            ..ChatConversation::default()
        }
    }

    #[test]
    fn a_text_message_serializes_as_it_always_did() {
        assert_eq!(
            serde_json::to_value(ChatMessage::user("Describe it.")).expect("JSON"),
            json!({"role": "user", "content": "Describe it."})
        );
        let parts = ChatMessage::user_parts(vec![text("a"), text("b")]);
        assert_eq!(
            serde_json::to_value(&parts).expect("JSON"),
            json!({"role": "user", "content": [
                {"type": "text", "text": "a"},
                {"type": "text", "text": "b"},
            ]})
        );
        conversation(vec![parts]).validate().expect("valid");
    }

    #[test]
    fn text_and_images_keep_their_order_as_real_image_parts() {
        let message = ChatMessage::user_parts(vec![
            text("Compare these."),
            png_part(),
            text("and"),
            jpeg_part(),
            png_part(),
        ]);
        let jpeg_url = format!(
            "data:image/jpeg;base64,{}",
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, jpeg())
        );
        let png_url = format!("data:image/png;base64,{PNG_BASE64}");
        assert_eq!(
            serde_json::to_value(&message).expect("JSON"),
            json!({"role": "user", "content": [
                {"type": "text", "text": "Compare these."},
                {"type": "image_url", "image_url": {"url": png_url}},
                {"type": "text", "text": "and"},
                {"type": "image_url", "image_url": {"url": jpeg_url}},
                {"type": "image_url", "image_url": {"url": png_url}},
            ]})
        );
        conversation(vec![message]).validate().expect("valid");
        conversation(vec![ChatMessage::user_parts(vec![png_part()])])
            .validate()
            .expect("an image alone is a valid user message");
    }

    #[test]
    fn images_are_accepted_in_user_messages_only() {
        let system = ChatMessage {
            content: Some(MessageContentWire::Parts(vec![png_part()])),
            ..ChatMessage::system("")
        };
        let assistant = ChatMessage {
            content: Some(MessageContentWire::Parts(vec![text("x"), png_part()])),
            ..ChatMessage::assistant(None, Vec::new())
        };
        let tool_call = ChatToolCallWire {
            id: "call_1".to_owned(),
            tool_type: Some("function".to_owned()),
            function: ChatFunctionCallWire {
                name: "f".to_owned(),
                arguments: "{}".to_owned(),
            },
        };
        let tool = ChatMessage {
            content: Some(MessageContentWire::Parts(vec![png_part()])),
            ..ChatMessage::tool("call_1", "")
        };
        let offered = ChatConversation {
            tools: vec![ToolWire {
                tool_type: "function".to_owned(),
                function: FunctionDefinitionWire {
                    name: "f".to_owned(),
                    description: None,
                    parameters: Some(json!({"type": "object"})),
                    strict: None,
                },
            }],
            ..ChatConversation::default()
        };
        for (case, messages) in [
            ("system", vec![system, ChatMessage::user("q")]),
            ("assistant", vec![ChatMessage::user("q"), assistant]),
            (
                "tool",
                vec![
                    ChatMessage::user("q"),
                    ChatMessage::assistant(None, vec![tool_call]),
                    tool,
                ],
            ),
        ] {
            let error = ChatConversation {
                messages,
                ..offered.clone()
            }
            .validate()
            .expect_err(case);
            assert!(
                error.to_string().contains("user messages only"),
                "{case}: {error}"
            );
        }
    }

    #[test]
    fn every_image_is_checked_and_no_refusal_repeats_it() {
        let data = &PNG_BASE64;
        for (url, problem) in [
            ("https://example.com/a.png".to_owned(), "never a remote URL"),
            (
                format!("data:image/bmp;base64,{data}"),
                "only image/png, image/jpeg, image/gif and image/webp",
            ),
            (format!("data:image/png;base64,{data}!"), "canonical base64"),
            (
                format!("data:image/jpeg;base64,{data}"),
                "not the image/jpeg",
            ),
        ] {
            let error = conversation(vec![ChatMessage::user_parts(vec![
                text("look"),
                image_url(&url),
            ])])
            .validate()
            .expect_err(problem);
            let message = error.to_string();
            assert!(message.contains(problem), "{message}");
            assert!(
                message.contains("messages[0].content[1].image_url.url"),
                "{message}"
            );
            assert!(!message.contains(&data[..12]), "{message}");
        }
        assert!(
            conversation(vec![ChatMessage::user_parts(Vec::new())])
                .validate()
                .is_err(),
            "content parts must not be empty"
        );
    }

    #[test]
    fn a_request_carries_a_bounded_number_and_size_of_images() {
        let many = (0..=MAX_REQUEST_IMAGES).map(|_| png_part()).collect();
        let error = conversation(vec![ChatMessage::user_parts(many)])
            .validate()
            .expect_err("too many images");
        assert!(error.to_string().contains("at most 8 images"), "{error}");

        let mut largest = png();
        largest.resize(MAX_IMAGE_BYTES, 0);
        let large = ImageInput::new(ImageMime::Png, largest)
            .expect("a PNG at the limit")
            .part();
        let within = MAX_REQUEST_IMAGE_BYTES / MAX_IMAGE_BYTES;
        let messages = (0..within)
            .map(|_| ChatMessage::user_parts(vec![large.clone()]))
            .collect::<Vec<_>>();
        conversation(messages.clone())
            .validate()
            .expect("the total is at its limit");
        let mut over = messages;
        over.push(ChatMessage::user_parts(vec![png_part()]));
        let error = conversation(over)
            .validate()
            .expect_err("the total is over its limit");
        assert!(error.to_string().contains("together"), "{error}");
    }

    #[test]
    fn an_inbound_request_may_carry_images_under_the_same_rules() {
        let url = format!("data:image/png;base64,{PNG_BASE64}");
        let request = ChatRequest::from_json(json!({
            "model": "m",
            "messages": [{"role": "user", "content": [
                {"type": "text", "text": "what is this?"},
                {"type": "image_url", "image_url": {"url": url}},
            ]}],
        }))
        .expect("parsed");
        request.validate().expect("valid");
        let remote = ChatRequest::from_json(json!({
            "model": "m",
            "messages": [{"role": "user", "content": [
                {"type": "image_url", "image_url": {"url": "https://example.com/a.png"}},
            ]}],
        }))
        .expect("parsed");
        assert!(remote.validate().is_err());
        for refused in [
            json!({"type": "image_url", "image_url": {"url": url, "detail": "high"}}),
            json!({"type": "input_audio", "input_audio": {"data": "", "format": "wav"}}),
            json!({"type": "file", "file": {"file_data": ""}}),
        ] {
            assert!(
                ChatRequest::from_json(json!({
                    "model": "m",
                    "messages": [{"role": "user", "content": [refused]}],
                }))
                .is_err(),
                "{refused:.80}"
            );
        }
    }
}
