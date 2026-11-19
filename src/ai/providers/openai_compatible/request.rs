use jsonschema::Validator;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

use super::admission;
use super::error::TransportError;
use super::stream::{CompletedToolCall, InvalidToolCall};
use crate::db::entities::ToolCallRefusal;

#[derive(Debug, Clone, Default, Serialize)]
pub struct ChatConversation {
    #[serde(skip)]
    pub dropped_turns: u32,
    #[serde(skip)]
    pub image_attachments: BTreeMap<usize, Vec<crate::db::entities::AttachmentId>>,
    pub messages: Vec<ChatMessage>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<ToolWire>,
}

impl ChatConversation {
    pub fn validate(&self) -> Result<(), TransportError> {
        if self.messages.is_empty() {
            return Err(TransportError::unsupported(
                "messages",
                "at least one message is required",
            ));
        }
        if self.tools.len() > MAX_TOOLS {
            return Err(TransportError::unsupported(
                "tools",
                format!("at most {MAX_TOOLS} tools may be offered"),
            ));
        }
        for (index, tool) in self.tools.iter().enumerate() {
            if tool.tool_type != "function" || tool.function.name.trim().is_empty() {
                return Err(TransportError::unsupported(
                    format!("tools[{index}]"),
                    "invalid tool definition",
                ));
            }
            if self.tools[..index]
                .iter()
                .any(|earlier| earlier.function.name == tool.function.name)
            {
                return Err(TransportError::unsupported(
                    format!("tools[{index}].function.name"),
                    format!("tool `{}` is offered twice", tool.function.name),
                ));
            }
        }
        let mut image_count = 0usize;
        let mut image_bytes = 0usize;
        for (index, message) in self.messages.iter().enumerate() {
            if let Some(MessageContentWire::Parts(parts)) = &message.content {
                for part in parts {
                    if let ContentPartWire::ImageUrl { image_url } = part {
                        let bytes =
                            super::image::check_data_url(&image_url.url).map_err(|error| {
                                TransportError::unsupported(
                                    format!("messages[{index}].content"),
                                    error.to_string(),
                                )
                            })?;
                        image_count = image_count.saturating_add(1);
                        image_bytes = image_bytes.saturating_add(bytes);
                        if image_count > super::image::MAX_REQUEST_IMAGES
                            || image_bytes > super::image::MAX_REQUEST_IMAGE_BYTES
                        {
                            return Err(TransportError::unsupported(
                                format!("messages[{index}].content"),
                                "request image count or byte limit exceeded",
                            ));
                        }
                    }
                }
            }
        }
        Ok(())
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
    #[serde(skip)]
    pub(crate) response_items: Option<Vec<Value>>,
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
            response_items: None,
        }
    }

    #[must_use]
    pub fn system(content: impl Into<String>) -> Self {
        Self {
            content: Some(MessageContentWire::Text(content.into())),
            ..Self::with_role(ChatRole::System)
        }
    }

    pub(crate) fn developer(content: impl Into<String>) -> Self {
        Self {
            content: Some(MessageContentWire::Text(content.into())),
            ..Self::with_role(ChatRole::Developer)
        }
    }

    pub(crate) fn responses(items: Vec<Value>) -> Self {
        Self {
            response_items: Some(items),
            ..Self::with_role(ChatRole::Assistant)
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

pub const MAX_TOOLS: usize = 128;
pub const MAX_TOOL_CALLS: usize = 128;
pub const MAX_TOOL_ARGUMENTS_BYTES: usize = 1024 * 1024;

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

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ChatFunctionCallWire {
    pub name: String,
    #[serde(default)]
    pub arguments: String,
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
