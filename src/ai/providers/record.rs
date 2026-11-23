use std::collections::BTreeSet;

use super::openai_compatible::ChatConversation;
use super::{OutputSinkError, Timestamp, Value, json};
use crate::db::entities::{
    AttachmentId, ContentDigest, JobId, MessageBody, RequestNumber, SentRequest,
};

#[derive(Default)]
pub struct RequestRecorder {
    messages: Vec<ContentDigest>,
    stored: BTreeSet<ContentDigest>,
    sent: bool,
    anthropic: bool,
}

pub struct SentRequestState {
    messages: Vec<ContentDigest>,
    stored: Vec<ContentDigest>,
}

impl RequestRecorder {
    pub(crate) fn for_transport(transport: Option<crate::db::entities::LlmTransport>) -> Self {
        Self {
            anthropic: transport == Some(crate::db::entities::LlmTransport::AnthropicMessages),
            ..Self::default()
        }
    }

    pub fn prepare(
        &self,
        job: JobId,
        request: RequestNumber,
        body: &Value,
        conversation: &ChatConversation,
        dropped_turns: u32,
    ) -> Result<(SentRequest, SentRequestState), OutputSinkError> {
        let mut bodies = Vec::new();
        let mut pending = BTreeSet::new();
        let responses = body.get("input").is_some();
        let messages = body
            .get(if responses { "input" } else { "messages" })
            .and_then(Value::as_array)
            .ok_or_else(|| record_error("request has no messages"))?;
        if !self.anthropic && conversation.messages.len() < self.messages.len() {
            return Err(record_error("recorded messages must only be appended"));
        }
        let mut digests = Vec::with_capacity(conversation.messages.len());
        let mut keep_message = |value: &Value| -> Result<(), OutputSinkError> {
            digests.push(self.keep(value, &mut bodies, &mut pending)?);
            Ok(())
        };
        if self.anthropic {
            record_anthropic_messages(messages, conversation, keep_message)?;
        } else {
            record_chat_messages(messages, conversation, responses, &mut keep_message)?;
        }
        if (responses || self.anthropic) && !digests.starts_with(&self.messages) {
            return Err(record_error(
                "recorded request must preserve its input prefix",
            ));
        }
        let tools = body
            .get("tools")
            .map(|value| self.keep(value, &mut bodies, &mut pending))
            .transpose()?;
        let parameters = {
            let parameters = body
                .as_object()
                .ok_or_else(|| record_error("request is not an object"))?
                .iter()
                .filter(|(key, _)| !matches!(key.as_str(), "messages" | "input" | "tools"))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect();
            self.keep(&Value::Object(parameters), &mut bodies, &mut pending)?
        };
        let required_messages = digests
            .iter()
            .copied()
            .chain(tools)
            .chain([parameters])
            .filter(|digest| !self.stored.contains(digest))
            .collect();
        let state = SentRequestState {
            messages: digests.clone(),
            stored: pending.into_iter().collect(),
        };
        Ok((
            SentRequest {
                job,
                request,
                observed_at: Timestamp::now(),
                messages: digests,
                tools,
                parameters,
                dropped_turns: (!self.sent).then_some(dropped_turns),
                bodies,
                required_messages,
            },
            state,
        ))
    }

    pub fn remember_sent(&mut self, state: SentRequestState) {
        self.messages = state.messages;
        self.stored.extend(state.stored);
        self.sent = true;
    }

    fn keep(
        &self,
        value: &Value,
        bodies: &mut Vec<MessageBody>,
        pending: &mut BTreeSet<ContentDigest>,
    ) -> Result<ContentDigest, OutputSinkError> {
        let body =
            MessageBody::new_exact(value).map_err(|error| record_error(error.to_string()))?;
        let digest = body.digest();
        if !self.stored.contains(&digest) && pending.insert(digest) {
            bodies.push(body);
        }
        Ok(digest)
    }

    pub fn remember_response(&mut self, digest: ContentDigest) {
        self.messages.push(digest);
        self.stored.insert(digest);
    }
}

fn record_chat_messages(
    messages: &[Value],
    conversation: &ChatConversation,
    responses: bool,
    mut keep_message: impl FnMut(&Value) -> Result<(), OutputSinkError>,
) -> Result<(), OutputSinkError> {
    let mut wire = messages;
    for (index, message) in conversation.messages.iter().enumerate() {
        let attachments = conversation
            .image_attachments
            .get(&index)
            .map_or(&[][..], Vec::as_slice);
        let group = message.response_items.as_deref().filter(|_| responses);
        let count = group.map_or(1, <[Value]>::len);
        let (items, remaining) = wire
            .split_at_checked(count)
            .ok_or_else(|| record_error("request input differs from its conversation"))?;
        wire = remaining;
        let value = if let Some(group) = group {
            if group != items || !attachments.is_empty() {
                return Err(record_error(
                    "request replay differs from its recorded output",
                ));
            }
            json!({"output": items})
        } else {
            let mut value = items[0].clone();
            replace_images(&mut value, attachments)?;
            value
        };
        keep_message(&value)?;
    }
    if !wire.is_empty() {
        return Err(record_error(
            "recorded request must preserve its input prefix",
        ));
    }
    Ok(())
}

fn record_anthropic_messages(
    messages: &[Value],
    conversation: &ChatConversation,
    mut keep_message: impl FnMut(&Value) -> Result<(), OutputSinkError>,
) -> Result<(), OutputSinkError> {
    use super::openai_compatible::{ContentPartWire, MessageContentWire};
    let mut attachments = conversation.image_attachments.values().flatten();
    let mut images = conversation
        .messages
        .iter()
        .filter_map(|message| match &message.content {
            Some(MessageContentWire::Parts(parts)) => Some(parts.iter()),
            Some(MessageContentWire::Text(_)) | None => None,
        })
        .flatten()
        .filter_map(|part| match part {
            ContentPartWire::ImageUrl { image_url } => Some(image_url.url.as_str()),
            ContentPartWire::Text { .. } => None,
        });
    for message in messages {
        let mut message = message.clone();
        let parts = message["content"]
            .as_array_mut()
            .ok_or_else(|| record_error("Anthropic message has no content blocks"))?;
        for part in parts.iter_mut().filter(|part| part["type"] == "image") {
            let source = &mut part["source"];
            let image = images
                .next()
                .and_then(|url| url.strip_prefix("data:"))
                .and_then(|url| url.split_once(";base64,"));
            let media_type = source["media_type"].as_str();
            if source["type"] != "base64"
                || image.is_none()
                || media_type.zip(source["data"].as_str()) != image
            {
                return Err(record_error(
                    "Anthropic image differs from its prepared Attachment",
                ));
            }
            let attachment = attachments
                .next()
                .ok_or_else(|| record_error("request image has no stored Attachment"))?;
            source["data"] = json!({"attachment": attachment.to_string()});
        }
        keep_message(&message)?;
    }
    if attachments.next().is_some() || images.next().is_some() {
        return Err(record_error(
            "request does not carry all its stored Attachments",
        ));
    }
    Ok(())
}

fn record_error(message: impl Into<String>) -> OutputSinkError {
    OutputSinkError {
        message: message.into(),
    }
}

fn replace_images(value: &mut Value, attachments: &[AttachmentId]) -> Result<(), OutputSinkError> {
    let Some(parts) = value.get_mut("content").and_then(Value::as_array_mut) else {
        return if attachments.is_empty() {
            Ok(())
        } else {
            Err(record_error(
                "request message has Attachments but no content parts",
            ))
        };
    };
    let mut attachments = attachments.iter();
    for part in parts {
        let url = match part.get("type").and_then(Value::as_str) {
            Some("image_url") => part
                .get_mut("image_url")
                .and_then(|image| image.get_mut("url")),
            Some("input_image") => part.get_mut("image_url"),
            _ => continue,
        };
        let Some(url) = url else {
            return Err(record_error("request image has no URL"));
        };
        if !url
            .as_str()
            .is_some_and(|url| url.starts_with("data:image/"))
        {
            return Err(record_error("request image is not a data URL"));
        }
        let attachment = attachments
            .next()
            .ok_or_else(|| record_error("request image has no stored Attachment"))?;
        *url = json!({"attachment": attachment.to_string()});
    }
    if attachments.next().is_some() {
        return Err(record_error(
            "request does not carry all its stored Attachments",
        ));
    }
    Ok(())
}
