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
}

pub struct SentRequestState {
    messages: Vec<ContentDigest>,
    stored: Vec<ContentDigest>,
}

impl RequestRecorder {
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
        if conversation.messages.len() < self.messages.len() {
            return Err(record_error("recorded messages must only be appended"));
        }
        let mut digests = Vec::with_capacity(conversation.messages.len());
        let mut wire = messages.as_slice();
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
            let body =
                MessageBody::new_exact(&value).map_err(|error| record_error(error.to_string()))?;
            digests.push(body.digest());
            if !self.stored.contains(&body.digest()) && pending.insert(body.digest()) {
                bodies.push(body);
            }
        }
        if !wire.is_empty() || (responses && !digests.starts_with(&self.messages)) {
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
