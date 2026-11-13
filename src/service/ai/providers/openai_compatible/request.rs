use jsonschema::Validator;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashSet};

use super::admission;
use super::error::TransportError;
use super::image::{self, MAX_REQUEST_IMAGE_BYTES, MAX_REQUEST_IMAGES};
use super::stream::{CompletedToolCall, InvalidToolCall, ToolCallRefusal};

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
    pub fn from_json(body: Value) -> Result<Self, TransportError> {
        serde_json::from_value(body).map_err(|error| {
            TransportError::invalid("request", format!("could not parse chat request: {error}"))
        })
    }

    pub fn from_slice(body: &[u8]) -> Result<Self, TransportError> {
        serde_json::from_slice(body).map_err(|error| {
            TransportError::invalid("request", format!("could not parse chat request: {error}"))
        })
    }

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

#[derive(Debug, Clone, Default, Serialize)]
pub struct ChatConversation {
    #[serde(skip)]
    pub response_items: BTreeMap<usize, Vec<Value>>,
    #[serde(skip)]
    pub dropped_turns: u32,
    #[serde(skip)]
    pub image_attachments: BTreeMap<usize, Vec<crate::db::entities::AttachmentId>>,
    pub messages: Vec<ChatMessage>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<ToolWire>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<ToolChoiceWire>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parallel_tool_calls: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_format: Option<ResponseFormatWire>,
}

impl ChatConversation {
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

    #[must_use]
    pub fn carries_images(&self) -> bool {
        self.messages.iter().any(|message| {
            matches!(&message.content, Some(MessageContentWire::Parts(parts))
                if parts.iter().any(|part| matches!(part, ContentPartWire::ImageUrl { .. })))
        })
    }

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
        let mut assembled: std::collections::BTreeMap<u32, InvalidToolCall> = assembled
            .into_iter()
            .filter_map(|refusal| refusal.index.map(|index| (index, refusal)))
            .collect();
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

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamOptions {
    #[serde(default)]
    pub include_usage: Option<bool>,
}

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

    #[must_use]
    pub fn user_parts(parts: Vec<ContentPartWire>) -> Self {
        Self {
            content: Some(MessageContentWire::Parts(parts)),
            ..Self::with_role(ChatRole::User)
        }
    }

    #[must_use]
    pub fn assistant(content: Option<String>, tool_calls: Vec<ChatToolCallWire>) -> Self {
        Self {
            content: content.map(MessageContentWire::Text),
            tool_calls: Some(tool_calls).filter(|calls| !calls.is_empty()),
            ..Self::with_role(ChatRole::Assistant)
        }
    }

    #[must_use]
    pub fn tool(tool_call_id: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            content: Some(MessageContentWire::Text(content.into())),
            tool_call_id: Some(tool_call_id.into()),
            ..Self::with_role(ChatRole::Tool)
        }
    }
}

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
                image::ImageError::MalformedBase64
                | image::ImageError::Empty
                | image::ImageError::TooLarge
                | image::ImageError::Mismatch(_) => {
                    TransportError::invalid(field.clone(), error.to_string())
                }
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

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ContentPartWire {
    Text { text: String },
    ImageUrl { image_url: ImageUrlWire },
}

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

pub const MAX_TOOLS: usize = 128;
pub const MAX_TOOL_CALLS: usize = 128;
pub const MAX_FUNCTION_NAME_BYTES: usize = 64;
pub const MAX_TOOL_CALL_ID_BYTES: usize = 256;
pub const MAX_TOOL_DEFINITION_BYTES: usize = 64 * 1024;
pub const MAX_TOOL_ARGUMENTS_BYTES: usize = 1024 * 1024;

#[must_use]
pub fn is_function_name(name: &str) -> bool {
    (1..=MAX_FUNCTION_NAME_BYTES).contains(&name.len())
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

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

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ChatToolCallWire {
    pub id: String,
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
        "none" | "minimal" | "low" | "medium" | "high" | "xhigh" | "max" => Ok(()),
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

impl From<crate::db::entities::ResponseFormat> for ResponseFormatWire {
    fn from(format: crate::db::entities::ResponseFormat) -> Self {
        match format {
            crate::db::entities::ResponseFormat::JsonObject => Self::JsonObject {},
            crate::db::entities::ResponseFormat::JsonSchema {
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
