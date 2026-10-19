use std::collections::{BTreeMap, BTreeSet};

use super::*;
use crate::persistence::{
    AttachmentId, ContentDigest, JobId, MessageBody, NewSentRequest, RequestNumber,
};

#[derive(Default)]
pub struct RequestRecorder {
    messages: Vec<ContentDigest>,
    stored: BTreeSet<ContentDigest>,
    tools: Option<Option<ContentDigest>>,
    parameters: Option<ContentDigest>,
    pub(crate) image_attachments: BTreeMap<usize, Vec<AttachmentId>>,
}

impl RequestRecorder {
    pub fn prepare(
        &mut self,
        job: JobId,
        request: RequestNumber,
        body: &Value,
        dropped_turns: u32,
    ) -> Result<NewSentRequest, OutputSinkError> {
        let mut bodies = Vec::new();
        let messages = body
            .get("messages")
            .and_then(Value::as_array)
            .ok_or_else(|| record_error("request has no messages"))?;
        if messages.len() < self.messages.len() {
            return Err(record_error("recorded messages must only be appended"));
        }
        for (index, value) in messages.iter().enumerate().skip(self.messages.len()) {
            let mut value = value.clone();
            replace_images(
                &mut value,
                self.image_attachments
                    .get(&index)
                    .map_or(&[], Vec::as_slice),
            )?;
            let body = MessageBody::new(&value).map_err(|error| record_error(error.to_string()))?;
            self.messages.push(body.digest());
            if self.stored.insert(body.digest()) {
                bodies.push(body);
            }
        }
        let tools = match self.tools {
            Some(tools) => tools,
            None => {
                let tools = body
                    .get("tools")
                    .map(|value| self.keep(value, &mut bodies))
                    .transpose()?;
                self.tools = Some(tools);
                tools
            }
        };
        let parameters = match self.parameters {
            Some(parameters) => parameters,
            None => {
                let parameters = body.as_object()
                    .ok_or_else(|| record_error("request is not an object"))?
                    .iter().filter(|(key, _)| !matches!(key.as_str(), "messages" | "tools"))
                    .map(|(key, value)| (key.clone(), value.clone())).collect();
                let digest = self.keep(&Value::Object(parameters), &mut bodies)?;
                self.parameters = Some(digest);
                digest
            }
        };
        Ok(NewSentRequest {
            job,
            request,
            observed_at: Timestamp::now(),
            messages: self.messages.clone(),
            tools,
            parameters,
            dropped_turns: (request == RequestNumber::FIRST).then_some(dropped_turns),
            bodies,
        })
    }

    fn keep(
        &mut self,
        value: &Value,
        bodies: &mut Vec<MessageBody>,
    ) -> Result<ContentDigest, OutputSinkError> {
        let body = MessageBody::new(value).map_err(|error| record_error(error.to_string()))?;
        let digest = body.digest();
        if self.stored.insert(digest) {
            bodies.push(body);
        }
        Ok(digest)
    }

    pub fn response(&mut self, message: &ChatMessage) -> Result<MessageBody, OutputSinkError> {
        let value =
            serde_json::to_value(message).map_err(|error| record_error(error.to_string()))?;
        MessageBody::new(&value).map_err(|error| record_error(error.to_string()))
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
        let Some(url) = part
            .get_mut("image_url")
            .and_then(|image| image.get_mut("url"))
        else {
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
