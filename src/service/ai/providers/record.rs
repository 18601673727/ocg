use std::collections::{BTreeMap, BTreeSet};

use super::*;
use crate::db::entities::{
    AttachmentId, ContentDigest, JobId, MessageBody, RequestNumber, SentRequest,
};

#[derive(Default)]
pub struct RequestRecorder {
    responses: bool,
    input_len: usize,
    response_groups: BTreeMap<usize, (usize, ContentDigest)>,
    pending_response_items: usize,
    messages: Vec<ContentDigest>,
    stored: BTreeSet<ContentDigest>,
    /// Whether a request for this Job has already been recorded. Only the
    /// first records the dropped-turn count, which describes the whole history.
    sent: bool,
    pub(crate) image_attachments: BTreeMap<usize, Vec<AttachmentId>>,
}

pub struct SentRequestState {
    responses: bool,
    input_len: usize,
    messages: Vec<ContentDigest>,
    stored: Vec<ContentDigest>,
}

impl RequestRecorder {
    pub fn prepare(
        &self,
        job: JobId,
        request: RequestNumber,
        body: &Value,
        dropped_turns: u32,
    ) -> Result<(SentRequest, SentRequestState), OutputSinkError> {
        let mut bodies = Vec::new();
        let mut pending = BTreeSet::new();
        let responses = body.get("input").is_some();
        let messages = body
            .get(if responses { "input" } else { "messages" })
            .and_then(Value::as_array)
            .ok_or_else(|| record_error("request has no messages"))?;
        let input_len = messages.len();
        let mut grouped = Vec::new();
        let mut index = 0;
        while index < messages.len() {
            if responses && let Some((count, digest)) = self.response_groups.get(&index) {
                let items = messages
                    .get(index..index.saturating_add(*count))
                    .ok_or_else(|| record_error("Responses input omitted a previous output"))?;
                let value = json!({"output": items});
                let body =
                    MessageBody::new(&value).map_err(|error| record_error(error.to_string()))?;
                if body.digest() != *digest {
                    return Err(record_error("Responses input changed a previous output"));
                }
                grouped.push(std::borrow::Cow::Owned(value));
                index += count;
            } else {
                grouped.push(std::borrow::Cow::Borrowed(&messages[index]));
                index += 1;
            }
        }
        let messages = grouped;
        if messages.len() < self.messages.len() {
            return Err(record_error("recorded messages must only be appended"));
        }
        let mut digests = Vec::with_capacity(messages.len());
        let mut user_index = 0;
        let mut image_index = 0;
        for (index, value) in messages.iter().enumerate() {
            let attachments = if value
                .get("content")
                .and_then(Value::as_array)
                .is_some_and(|parts| parts.iter().any(|part| part["type"] == "image"))
            {
                let attachments = self
                    .image_attachments
                    .values()
                    .filter(|attachments| !attachments.is_empty())
                    .nth(image_index)
                    .map_or(&[][..], Vec::as_slice);
                image_index += 1;
                attachments
            } else if responses {
                if value.get("role").and_then(Value::as_str) == Some("user") {
                    let attachments = self
                        .image_attachments
                        .values()
                        .nth(user_index)
                        .map_or(&[][..], Vec::as_slice);
                    user_index += 1;
                    attachments
                } else {
                    &[]
                }
            } else {
                self.image_attachments
                    .get(&index)
                    .map_or(&[][..], Vec::as_slice)
            };
            let mut value = value.as_ref().clone();
            replace_images(&mut value, attachments)?;
            let body = MessageBody::new(&value).map_err(|error| record_error(error.to_string()))?;
            digests.push(body.digest());
            if !self.stored.contains(&body.digest()) && pending.insert(body.digest()) {
                bodies.push(body);
            }
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
            responses,
            input_len,
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
        self.responses = state.responses;
        self.input_len = state.input_len;
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
        let body = MessageBody::new(value).map_err(|error| record_error(error.to_string()))?;
        let digest = body.digest();
        if !self.stored.contains(&digest) && pending.insert(digest) {
            bodies.push(body);
        }
        Ok(digest)
    }

    pub fn response(&mut self, message: &ChatMessage) -> Result<MessageBody, OutputSinkError> {
        let value =
            serde_json::to_value(message).map_err(|error| record_error(error.to_string()))?;
        MessageBody::new(&value).map_err(|error| record_error(error.to_string()))
    }

    pub fn response_with_items(
        &mut self,
        message: &ChatMessage,
        items: &[Value],
    ) -> Result<MessageBody, OutputSinkError> {
        self.pending_response_items = items.len();
        if items.is_empty() {
            return self.response(message);
        }
        MessageBody::new(&json!({"output": items})).map_err(|error| record_error(error.to_string()))
    }

    pub fn remember_response(&mut self, digest: ContentDigest) {
        if self.responses && self.pending_response_items > 0 {
            self.response_groups
                .insert(self.input_len, (self.pending_response_items, digest));
        }
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
        return Ok(());
    };
    let mut attachments = attachments.iter();
    for part in parts {
        if part["type"] == "image" {
            if part["source"]["type"] != "base64" || !part["source"]["data"].is_string() {
                return Err(record_error("request image is not base64"));
            }
            let attachment = attachments
                .next()
                .ok_or_else(|| record_error("request image has no stored Attachment"))?;
            part["source"] = json!({"type":"attachment","attachment":attachment.to_string()});
            continue;
        }
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
    Ok(())
}
