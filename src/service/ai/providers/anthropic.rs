use std::collections::BTreeMap;

use super::*;

pub(crate) const DEFAULT_ENDPOINT: &str = "https://api.anthropic.com/v1";
pub(crate) const API_VERSION: &str = "2023-06-01";

fn build(message: &str) -> ProviderExecutionError {
    ProviderExecutionError::Build {
        message: message.to_owned(),
    }
}

pub(crate) fn api_key(secret: &str) -> Result<HeaderValue, ProviderExecutionError> {
    let mut value = HeaderValue::try_from(secret)
        .map_err(|_| build("the resolved credential is not a valid HTTP header value"))?;
    value.set_sensitive(true);
    Ok(value)
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
                "Anthropic does not support presence_penalty or frequency_penalty",
            ));
        }
    }
    if body.contains_key("max_tokens") && body.contains_key("max_completion_tokens") {
        return Err(build(
            "max_tokens and max_completion_tokens are mutually exclusive",
        ));
    }
    let max_tokens = body
        .remove("max_tokens")
        .or_else(|| body.remove("max_completion_tokens"))
        .unwrap_or(json!(4096));
    body.insert("max_tokens".into(), max_tokens);
    body.remove("stream_options");
    let source = body
        .remove("messages")
        .ok_or_else(|| build("request has no messages"))?;
    let source = source
        .as_array()
        .ok_or_else(|| build("request messages are not an array"))?;
    let mut system = Vec::new();
    let mut messages = Vec::new();
    for (index, message) in source.iter().enumerate() {
        let role = message["role"]
            .as_str()
            .ok_or_else(|| build("message has no role"))?;
        if role == "system" || role == "developer" {
            if let Some(text) = message["content"].as_str().filter(|text| !text.is_empty()) {
                system.push(json!({"type":"text","text":text}));
            }
            continue;
        }
        let mut content = Vec::new();
        if let Some(items) = conversation.response_items.get(&index) {
            for item in items {
                let blocks = item["content"]
                    .as_array()
                    .ok_or_else(|| build("invalid Anthropic response history"))?;
                content.extend(blocks.iter().cloned());
            }
        } else if role == "tool" {
            content.push(json!({"type":"tool_result","tool_use_id":message["tool_call_id"],"content":message["content"]}));
        } else {
            match &message["content"] {
                Value::String(text) if !text.is_empty() => {
                    content.push(json!({"type":"text","text":text}))
                }
                Value::Array(parts) => {
                    for part in parts {
                        match part["type"].as_str() {
                            Some("text") => {
                                if part["text"].as_str().is_some_and(|text| !text.is_empty()) {
                                    content.push(part.clone());
                                }
                            }
                            Some("image_url") => {
                                let url = part["image_url"]["url"]
                                    .as_str()
                                    .ok_or_else(|| build("image has no URL"))?;
                                let (media_type, data) = url
                                    .strip_prefix("data:")
                                    .and_then(|url| url.split_once(";base64,"))
                                    .ok_or_else(|| {
                                        build("Anthropic images must be base64 data URLs")
                                    })?;
                                content.push(json!({"type":"image","source":{"type":"base64","media_type":media_type,"data":data}}));
                            }
                            _ => return Err(build("unsupported Anthropic message content")),
                        }
                    }
                }
                _ => {}
            }
            if let Some(calls) = message["tool_calls"].as_array() {
                for call in calls {
                    let input: Value =
                        serde_json::from_str(call["function"]["arguments"].as_str().unwrap_or(""))
                            .map_err(|_| build("invalid tool arguments in Anthropic history"))?;
                    content.push(json!({"type":"tool_use","id":call["id"],"name":call["function"]["name"],"input":input}));
                }
            }
        }
        if content.is_empty() {
            return Err(build("Anthropic messages must have non-empty content"));
        }
        messages.push(json!({"role":if role == "tool" { "user" } else { role },"content":content}));
    }
    body.insert("messages".into(), Value::Array(messages));
    if !system.is_empty() {
        body.insert("system".into(), Value::Array(system));
    }
    if let Some(tools) = body.get_mut("tools").and_then(Value::as_array_mut) {
        for tool in tools {
            let function = &tool["function"];
            let mut native = json!({"name":function["name"],"input_schema":function["parameters"]});
            if let Some(description) = function.get("description") {
                native["description"] = description.clone();
            }
            if let Some(strict) = function.get("strict") {
                native["strict"] = strict.clone();
            }
            *tool = native;
        }
    }
    if let Some(choice) = body.get_mut("tool_choice") {
        *choice = match choice.as_str() {
            Some("auto") => json!({"type":"auto"}),
            Some("none") => json!({"type":"none"}),
            Some("required") => json!({"type":"any"}),
            _ => json!({"type":"tool","name":choice["function"]["name"]}),
        };
    }
    if let Some(parallel) = body.remove("parallel_tool_calls") {
        let choice = body.entry("tool_choice").or_insert(json!({"type":"auto"}));
        choice["disable_parallel_tool_use"] = json!(parallel == false);
    }
    if let Some(format) = body.remove("response_format") {
        match format["type"].as_str() {
            Some("text") => {}
            Some("json_schema") => {
                body.insert("output_config".into(), json!({"format":{"type":"json_schema","schema":format["json_schema"]["schema"]}}));
            }
            Some("json_object") => {
                return Err(build(
                    "Anthropic requires json_schema instead of json_object",
                ));
            }
            _ => return Err(build("unsupported Anthropic response format")),
        }
    }
    if let Some(effort) = body.remove("reasoning_effort") {
        match effort.as_str() {
            Some("none") => {
                body.insert("thinking".into(), json!({"type":"disabled"}));
            }
            Some("minimal" | "low" | "medium" | "high" | "xhigh" | "max") => {
                body.insert("thinking".into(), json!({"type":"adaptive"}));
                let output = body.entry("output_config").or_insert(json!({}));
                output["effort"] = if effort == "minimal" {
                    json!("low")
                } else {
                    effort
                };
            }
            _ => {
                return Err(build(
                    "Anthropic reasoning_effort must be none, minimal, low, medium, high, xhigh or max",
                ));
            }
        }
    }
    Ok(chat)
}

struct Block {
    content: Value,
    arguments: String,
    done: bool,
}

#[derive(Default)]
pub(crate) struct MessageStream {
    blocks: BTreeMap<u32, Block>,
    started: bool,
    pub(crate) terminal: bool,
    pub(crate) stop_reason: Option<String>,
    usage: serde_json::Map<String, Value>,
    bytes: usize,
}

impl MessageStream {
    fn index(event: &Value) -> Result<u32, &'static str> {
        event["index"]
            .as_u64()
            .and_then(|value| u32::try_from(value).ok())
            .ok_or("Anthropic event has no valid block index")
    }

    pub(crate) fn output(&self) -> Vec<Value> {
        vec![
            json!({"type":"message","role":"assistant","content":self.blocks.values().map(|block| block.content.clone()).collect::<Vec<_>>()}),
        ]
    }

    pub(crate) fn reported_usage(&self) -> Option<usage::ReportedUsage> {
        if self.usage.is_empty() {
            return None;
        }
        let count = |key: &str| {
            self.usage
                .get(key)
                .and_then(Value::as_u64)
                .and_then(|value| u32::try_from(value).ok())
        };
        let cache_read_tokens = count("cache_read_input_tokens");
        let cache_write_tokens = count("cache_creation_input_tokens");
        Some(usage::ReportedUsage {
            usage: NormalizedUsage {
                input_tokens: count("input_tokens").and_then(|count| {
                    count
                        .checked_add(cache_read_tokens.unwrap_or(0))?
                        .checked_add(cache_write_tokens.unwrap_or(0))
                }),
                output_tokens: count("output_tokens"),
                cache_read_tokens,
                cache_write_tokens,
                reasoning_tokens: None,
                raw: Some(Value::Object(self.usage.clone())),
            },
            finality: if self.terminal {
                UsageFinality::Final
            } else {
                UsageFinality::Cumulative
            },
        })
    }

    pub(crate) fn event(&mut self, event: &Value, limit: usize) -> Result<Value, &'static str> {
        let kind = event["type"]
            .as_str()
            .ok_or("Anthropic event has no type")?;
        if kind == "ping" {
            return Ok(json!({}));
        }
        if kind == "error" {
            return Ok(event.clone());
        }
        if self.terminal {
            return Err("Anthropic event followed message_stop");
        }
        if kind == "message_start" {
            if self.started {
                return Err("Anthropic message_start repeated");
            }
            self.started = true;
            if let Some(usage) = event["message"]["usage"].as_object() {
                self.usage.extend(usage.clone());
            }
            return Ok(json!({}));
        }
        if !self.started {
            return Err("Anthropic stream omitted message_start");
        }
        let mut delta = json!({});
        let mut finish = Value::Null;
        match kind {
            "content_block_start" => {
                let index = Self::index(event)?;
                if self.blocks.len() >= 128 || self.blocks.contains_key(&index) {
                    return Err("Anthropic content block repeated or exceeded its limit");
                }
                let content = event["content_block"].clone();
                self.bytes = self.bytes.saturating_add(
                    ["text", "thinking", "signature", "data", "id", "name"]
                        .iter()
                        .filter_map(|key| content.get(key).and_then(Value::as_str))
                        .map(str::len)
                        .sum::<usize>(),
                );
                match content["type"].as_str() {
                    Some("text") => {
                        delta["content"] = content["text"].clone();
                    }
                    Some("thinking") => {
                        delta["reasoning_content"] = content["thinking"].clone();
                    }
                    Some("redacted_thinking") => {}
                    Some("tool_use") => {
                        delta["tool_calls"] = json!([{"index":index,"id":content["id"],"type":"function","function":{"name":content["name"],"arguments":""}}]);
                    }
                    _ => return Err("unsupported Anthropic content block"),
                }
                self.blocks.insert(
                    index,
                    Block {
                        content,
                        arguments: String::new(),
                        done: false,
                    },
                );
            }
            "content_block_delta" => {
                let index = Self::index(event)?;
                let block = self
                    .blocks
                    .get_mut(&index)
                    .filter(|block| !block.done)
                    .ok_or("Anthropic delta has no open content block")?;
                let change = &event["delta"];
                let (field, wire, expected) = match change["type"].as_str() {
                    Some("text_delta") => ("text", "content", "text"),
                    Some("thinking_delta") => ("thinking", "reasoning_content", "thinking"),
                    Some("signature_delta") => ("signature", "", "thinking"),
                    Some("input_json_delta") => ("partial_json", "", "tool_use"),
                    _ => return Ok(json!({})),
                };
                if block.content["type"] != expected {
                    return Err("Anthropic delta changed content block type");
                }
                let text = change[field]
                    .as_str()
                    .ok_or("Anthropic delta is not text")?;
                self.bytes = self.bytes.saturating_add(text.len());
                if field == "partial_json" {
                    block.arguments.push_str(text);
                    delta["tool_calls"] = json!([{"index":index,"function":{"arguments":text}}]);
                } else {
                    let fields = block
                        .content
                        .as_object_mut()
                        .ok_or("Anthropic content block is not an object")?;
                    let previous = fields.entry(field).or_insert(json!(""));
                    let Value::String(previous) = previous else {
                        return Err("Anthropic content field is not text");
                    };
                    previous.push_str(text);
                    if !wire.is_empty() {
                        delta[wire] = json!(text);
                    }
                }
            }
            "content_block_stop" => {
                let index = Self::index(event)?;
                let block = self
                    .blocks
                    .get_mut(&index)
                    .filter(|block| !block.done)
                    .ok_or("Anthropic stop has no open content block")?;
                block.done = true;
                if block.content["type"] == "tool_use" {
                    if block.arguments.is_empty() {
                        let input = block
                            .content
                            .get("input")
                            .filter(|input| input.is_object())
                            .ok_or("Anthropic tool input is not an object")?;
                        delta["tool_calls"] =
                            json!([{"index":index,"function":{"arguments":input.to_string()}}]);
                    } else {
                        block.content["input"] = serde_json::from_str::<Value>(&block.arguments)
                            .map_err(|_| "Anthropic tool input is incomplete JSON")?;
                    }
                }
            }
            "message_delta" => {
                if let Some(reason) = event["delta"]["stop_reason"].as_str() {
                    if self.stop_reason.as_deref().is_some_and(|old| old != reason) {
                        return Err("Anthropic stop reason changed");
                    }
                    self.stop_reason = Some(reason.to_owned());
                }
                if let Some(usage) = event["usage"].as_object() {
                    self.usage.extend(usage.clone());
                }
            }
            "message_stop" => {
                if self.blocks.values().any(|block| !block.done) {
                    return Err("Anthropic message stopped with an open content block");
                }
                finish = json!(match self
                    .stop_reason
                    .as_deref()
                    .ok_or("Anthropic message has no stop reason")?
                {
                    "end_turn" | "stop_sequence" => "stop",
                    "tool_use" => "tool_calls",
                    "max_tokens" | "model_context_window_exceeded" => "length",
                    "refusal" => "content_filter",
                    _ => "other",
                });
                self.terminal = true;
            }
            _ => {}
        }
        if self.bytes > limit {
            return Err("Anthropic output exceeded its limit");
        }
        Ok(json!({"choices":[{"delta":delta,"finish_reason":finish}]}))
    }
}
