use std::collections::BTreeMap;

use super::*;

fn build(message: &str) -> ProviderExecutionError {
    ProviderExecutionError::Build {
        message: message.to_owned(),
    }
}

pub(crate) fn request_body(
    mut chat: Value,
    conversation: &ChatConversation,
) -> Result<Value, ProviderExecutionError> {
    let body = chat
        .as_object_mut()
        .ok_or_else(|| build("request is not an object"))?;
    for key in ["presence_penalty", "frequency_penalty"] {
        if body.contains_key(key) {
            return Err(build(
                "Responses does not support presence_penalty or frequency_penalty",
            ));
        }
    }
    let messages = body
        .remove("messages")
        .ok_or_else(|| build("request has no messages"))?;
    let messages = messages
        .as_array()
        .ok_or_else(|| build("request messages are not an array"))?;
    let mut input = Vec::new();
    for (index, message) in messages.iter().enumerate() {
        if let Some(items) = conversation.response_items.get(&index) {
            input.extend(items.iter().cloned());
            continue;
        }
        let role = message
            .get("role")
            .and_then(Value::as_str)
            .ok_or_else(|| build("message has no role"))?;
        if role == "tool" {
            input.push(json!({"type":"function_call_output", "call_id": message["tool_call_id"], "output": message["content"]}));
            continue;
        }
        if let Some(content) = message.get("content").filter(|v| !v.is_null()) {
            let content = match content.as_array() {
                None => content.clone(),
                Some(parts) => Value::Array(
                    parts
                        .iter()
                        .map(|part| match part["type"].as_str() {
                            Some("text") => Ok(json!({"type":"input_text","text":part["text"]})),
                            Some("image_url") => Ok(
                                json!({"type":"input_image","image_url":part["image_url"]["url"]}),
                            ),
                            _ => Err(build("unsupported Responses input content")),
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                ),
            };
            input.push(json!({"role":role,"content":content}));
        }
        if let Some(calls) = message.get("tool_calls").and_then(Value::as_array) {
            for call in calls {
                input.push(json!({"type":"function_call","call_id":call["id"],"name":call["function"]["name"],"arguments":call["function"]["arguments"]}));
            }
        }
    }
    body.insert("input".into(), Value::Array(input));
    body.remove("stream_options");
    body.insert("store".into(), json!(false));
    body.insert("include".into(), json!(["reasoning.encrypted_content"]));
    if body.contains_key("max_tokens") && body.contains_key("max_completion_tokens") {
        return Err(build(
            "max_tokens and max_completion_tokens are mutually exclusive",
        ));
    }
    if let Some(limit) = body
        .remove("max_tokens")
        .or_else(|| body.remove("max_completion_tokens"))
    {
        body.insert("max_output_tokens".into(), limit);
    }
    if let Some(effort) = body.remove("reasoning_effort") {
        body.insert(
            "reasoning".into(),
            json!({"effort":effort,"summary":"auto"}),
        );
    }
    if let Some(tools) = body.get_mut("tools").and_then(Value::as_array_mut) {
        for tool in tools {
            let mut function = tool
                .get("function")
                .and_then(Value::as_object)
                .cloned()
                .ok_or_else(|| build("invalid function tool"))?;
            function.insert("type".into(), json!("function"));
            function.entry("strict").or_insert(json!(false));
            *tool = Value::Object(function);
        }
    }
    if let Some(choice) = body
        .get_mut("tool_choice")
        .filter(|choice| choice.is_object())
    {
        *choice = json!({"type":"function","name":choice["function"]["name"]});
    }
    if let Some(mut format) = body.remove("response_format") {
        if format["type"] == "json_schema" {
            let mut schema = format
                .get("json_schema")
                .and_then(Value::as_object)
                .cloned()
                .ok_or_else(|| build("invalid response schema"))?;
            schema.insert("type".into(), json!("json_schema"));
            format = Value::Object(schema);
        }
        body.insert("text".into(), json!({"format":format}));
    }
    Ok(chat)
}

#[derive(Default)]
struct Item {
    id: Option<String>,
    kind: Option<String>,
    call_id: Option<String>,
    name: Option<String>,
    arguments: String,
    text: BTreeMap<(String, u64), String>,
    done: bool,
}

#[derive(Default)]
pub(crate) struct ResponseStream {
    items: BTreeMap<u32, Item>,
    pub(crate) output: Vec<Value>,
    pub(crate) terminal: bool,
    bytes: usize,
}

impl ResponseStream {
    fn index(event: &Value) -> Result<u32, &'static str> {
        event
            .get("output_index")
            .and_then(Value::as_u64)
            .and_then(|v| u32::try_from(v).ok())
            .ok_or("Responses event has no valid output_index")
    }

    fn item(&mut self, index: u32) -> Result<&mut Item, &'static str> {
        if !self.items.contains_key(&index) && self.items.len() >= 128 {
            return Err("Responses output has too many items");
        }
        Ok(self.items.entry(index).or_default())
    }

    fn identity(slot: &mut Option<String>, value: &Value) -> Result<bool, &'static str> {
        let Some(value) = value.as_str().filter(|s| !s.is_empty()) else {
            return Ok(false);
        };
        if value.len() > 1024 {
            return Err("Responses output identity exceeded its limit");
        }
        if slot.as_deref().is_some_and(|old| old != value) {
            return Err("Responses output changed identity");
        }
        if slot.is_none() {
            *slot = Some(value.to_owned());
            return Ok(true);
        }
        Ok(false)
    }

    fn suffix(previous: &mut String, value: &str, snapshot: bool) -> String {
        if snapshot {
            let suffix = value
                .strip_prefix(previous.as_str())
                .unwrap_or("")
                .to_owned();
            previous.clear();
            previous.push_str(value);
            suffix
        } else {
            previous.push_str(value);
            value.to_owned()
        }
    }

    fn text(
        &mut self,
        index: u32,
        part: u64,
        field: &str,
        value: &str,
        snapshot: bool,
        delta: &mut Value,
    ) -> Result<(), &'static str> {
        let item = self.item(index)?;
        if item.done && !snapshot {
            return Err("Responses text changed after its item completed");
        }
        let text = item.text.entry((field.to_owned(), part)).or_default();
        let previous_len = text.len();
        let suffix = Self::suffix(text, value, snapshot);
        let added = suffix.len().max(text.len().saturating_sub(previous_len));
        self.bytes = self.bytes.saturating_add(added);
        let old = delta[field].as_str().unwrap_or("");
        delta[field] = json!(format!("{old}{suffix}"));
        Ok(())
    }

    fn arguments(
        &mut self,
        index: u32,
        value: &str,
        snapshot: bool,
        delta: &mut Value,
    ) -> Result<(), &'static str> {
        let item = self.item(index)?;
        if item.done && !snapshot {
            return Err("Responses arguments changed after its item completed");
        }
        let previous_len = item.arguments.len();
        let suffix = Self::suffix(&mut item.arguments, value, snapshot);
        let added = suffix
            .len()
            .max(item.arguments.len().saturating_sub(previous_len));
        self.bytes = self.bytes.saturating_add(added);
        if !suffix.is_empty() {
            Self::call_delta(
                delta,
                json!({"index":index,"function":{"arguments":suffix}}),
            );
        }
        Ok(())
    }

    fn call_delta(delta: &mut Value, call: Value) {
        if let Some(calls) = delta.get_mut("tool_calls").and_then(Value::as_array_mut) {
            calls.push(call);
        } else {
            delta["tool_calls"] = json!([call]);
        }
    }

    fn snapshot(
        &mut self,
        index: u32,
        value: &Value,
        done: bool,
        delta: &mut Value,
    ) -> Result<(), &'static str> {
        let kind = value["type"].as_str().ok_or("Responses item has no type")?;
        let item = self.item(index)?;
        Self::identity(&mut item.kind, &value["type"])?;
        Self::identity(&mut item.id, &value["id"])?;
        let mut expected_text = BTreeMap::new();
        if done
            && value
                .get("status")
                .and_then(Value::as_str)
                .is_some_and(|status| status != "completed")
        {
            return Err("Responses completed item has an invalid status");
        }
        match kind {
            "function_call" => {
                if done
                    && ["call_id", "name", "arguments"]
                        .iter()
                        .any(|key| value[*key].as_str().is_none())
                {
                    return Err("Responses completed function call is missing fields");
                }
                let mut call = json!({"index":index,"type":"function","function":{}});
                let id_new = Self::identity(&mut item.call_id, &value["call_id"])?;
                let name_new = Self::identity(&mut item.name, &value["name"])?;
                if id_new {
                    call["id"] = value["call_id"].clone();
                }
                if name_new {
                    call["function"]["name"] = value["name"].clone();
                }
                if id_new || name_new {
                    Self::call_delta(delta, call);
                }
                if let Some(arguments) = value["arguments"].as_str() {
                    self.arguments(index, arguments, true, delta)?;
                }
            }
            "message" => {
                if let Some(parts) = value["content"].as_array() {
                    for (part, content) in parts.iter().enumerate() {
                        let text = match content["type"].as_str() {
                            Some("output_text") => content["text"].as_str(),
                            Some("refusal") => content["refusal"].as_str(),
                            _ => return Err("unsupported Responses message content"),
                        }
                        .ok_or("Responses content is not text")?;
                        expected_text.insert(("content".to_owned(), part as u64), text.to_owned());
                        self.text(index, part as u64, "content", text, true, delta)?;
                    }
                }
            }
            "reasoning" => {
                if let Some(summary) = value["summary"].as_array() {
                    for (part, content) in summary.iter().enumerate() {
                        let text = content["text"]
                            .as_str()
                            .ok_or("Responses reasoning summary is not text")?;
                        expected_text.insert(
                            ("reasoning_content".to_owned(), part as u64),
                            text.to_owned(),
                        );
                        self.text(index, part as u64, "reasoning_content", text, true, delta)?;
                    }
                }
            }
            _ => return Err("unsupported Responses output item"),
        }
        if done {
            let item = self.item(index)?;
            if item.id.is_none()
                || item.text != expected_text
                || (kind != "function_call" && !item.arguments.is_empty())
            {
                return Err("Responses snapshot omitted streamed output");
            }
            item.done = true;
        }
        Ok(())
    }

    pub(crate) fn event(&mut self, event: &Value, limit: usize) -> Result<Value, &'static str> {
        let kind = event["type"]
            .as_str()
            .ok_or("Responses event has no type")?;
        let mut delta = json!({});
        let mut finish = None;
        match kind {
            "response.output_item.added" | "response.output_item.done" => {
                self.snapshot(
                    Self::index(event)?,
                    &event["item"],
                    kind.ends_with(".done"),
                    &mut delta,
                )?;
            }
            "response.output_text.delta"
            | "response.refusal.delta"
            | "response.reasoning_summary_text.delta" => {
                let index = Self::index(event)?;
                let item = self.item(index)?;
                Self::identity(&mut item.id, &event["item_id"])?;
                let part = event
                    .get("content_index")
                    .or_else(|| event.get("summary_index"))
                    .and_then(Value::as_u64)
                    .ok_or("Responses delta has no content index")?;
                if part > 1024 {
                    return Err("Responses content index exceeds its limit");
                }
                let text = event["delta"]
                    .as_str()
                    .ok_or("Responses text delta is not text")?;
                let field = if kind == "response.reasoning_summary_text.delta" {
                    "reasoning_content"
                } else {
                    "content"
                };
                self.text(index, part, field, text, false, &mut delta)?;
            }
            "response.function_call_arguments.delta" | "response.function_call_arguments.done" => {
                let index = Self::index(event)?;
                Self::identity(&mut self.item(index)?.id, &event["item_id"])?;
                let snapshot = kind.ends_with(".done");
                let text = event[if snapshot { "arguments" } else { "delta" }]
                    .as_str()
                    .ok_or("Responses arguments are not text")?;
                self.arguments(index, text, snapshot, &mut delta)?;
            }
            "response.completed" => {
                if event["response"]["status"] != "completed" {
                    return Err("Responses completion has an invalid status");
                }
                let output = event["response"]["output"]
                    .as_array()
                    .ok_or("Responses completion has no output")?;
                if output.len() > 128 {
                    return Err("Responses output has too many items");
                }
                let mut completed = Self::default();
                for (index, item) in output.iter().enumerate() {
                    self.snapshot(index as u32, item, true, &mut json!({}))?;
                    completed.snapshot(index as u32, item, true, &mut delta)?;
                }
                if self.items.len() != output.len() {
                    return Err("Responses completion omitted streamed items");
                }
                self.bytes = self.bytes.max(completed.bytes);
                // The terminal snapshot bounds encrypted reasoning and other retained metadata too.
                if output.iter().map(|v| v.to_string().len()).sum::<usize>()
                    > limit.saturating_mul(6).saturating_add(MAX_SSE_LINE_BYTES)
                {
                    return Err("Responses output metadata exceeded its limit");
                }
                self.output = output.clone();
                finish = Some(
                    if self
                        .items
                        .values()
                        .any(|item| item.kind.as_deref() == Some("function_call"))
                    {
                        "tool_calls"
                    } else {
                        "stop"
                    },
                );
                self.terminal = true;
            }
            "response.failed" | "response.incomplete" => {
                return Err("Responses request failed or ended incomplete");
            }
            "error" => return Ok(json!({"error":event})),
            _ => {}
        }
        if self.bytes > limit {
            return Err("Responses output exceeded its limit");
        }
        Ok(json!({"choices":[{"delta":delta,"finish_reason":finish}]}))
    }
}
