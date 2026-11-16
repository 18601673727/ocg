use std::collections::{BTreeMap, BTreeSet};

use super::{OutputSinkError, Timestamp, Value, json};
use crate::db::entities::{
    AttachmentId, ContentDigest, JobId, MessageBody, RequestNumber, SentRequest,
};

#[derive(Default)]
pub struct RequestRecorder {
    messages: Vec<ContentDigest>,
    stored: BTreeSet<ContentDigest>,
    sent: bool,
    pub(crate) image_attachments: BTreeMap<usize, Vec<AttachmentId>>,
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
        dropped_turns: u32,
    ) -> Result<(SentRequest, SentRequestState), OutputSinkError> {
        let mut bodies = Vec::new();
        let mut pending = BTreeSet::new();
        let messages = body
            .get("messages")
            .and_then(Value::as_array)
            .ok_or_else(|| record_error("request has no messages"))?;
        if messages.len() < self.messages.len() {
            return Err(record_error("recorded messages must only be appended"));
        }
        let mut digests = Vec::with_capacity(messages.len());
        for (index, value) in messages.iter().enumerate() {
            let attachments = self
                .image_attachments
                .get(&index)
                .map_or(&[][..], Vec::as_slice);
            let mut value = value.clone();
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
        let body = MessageBody::new(value).map_err(|error| record_error(error.to_string()))?;
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
        return Ok(());
    };
    let mut attachments = attachments.iter();
    for part in parts {
        if part.get("type").and_then(Value::as_str) != Some("image_url") {
            continue;
        }
        let url = part
            .get_mut("image_url")
            .and_then(|image| image.get_mut("url"));
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
