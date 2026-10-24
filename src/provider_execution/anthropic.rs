use std::collections::BTreeMap;

use super::*;

pub(super) const DEFAULT_ENDPOINT: &str = "https://api.anthropic.com/v1";
pub(super) const API_VERSION: &str = "2023-06-01";

fn build(message: &str) -> ProviderExecutionError {
    ProviderExecutionError::Build {
        message: message.to_owned(),
    }
}

pub(super) fn api_key(secret: &str) -> Result<HeaderValue, ProviderExecutionError> {
    let mut value = HeaderValue::try_from(secret)
        .map_err(|_| build("the resolved credential is not a valid HTTP header value"))?;
    value.set_sensitive(true);
    Ok(value)
}

pub(super) fn request_body(
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
            Some("minimal" | "low" | "medium" | "high" | "xhigh") => {
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
                    "Anthropic reasoning_effort must be none, minimal, low, medium, high or xhigh",
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
pub(super) struct MessageStream {
    blocks: BTreeMap<u32, Block>,
    started: bool,
    pub(super) terminal: bool,
    pub(super) stop_reason: Option<String>,
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

    pub(super) fn output(&self) -> Vec<Value> {
        vec![
            json!({"type":"message","role":"assistant","content":self.blocks.values().map(|block| block.content.clone()).collect::<Vec<_>>()}),
        ]
    }

    pub(super) fn reported_usage(&self) -> Option<usage::ReportedUsage> {
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

    pub(super) fn event(&mut self, event: &Value, limit: usize) -> Result<Value, &'static str> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::{AttachmentId, JobId, LlmTransport, RequestNumber};
    use crate::provider_execution::scripted::{Step, payload};

    fn start() -> Value {
        json!({"type":"message_start","message":{"role":"assistant","content":[],"usage":{"input_tokens":10,"output_tokens":0,"cache_read_input_tokens":3,"cache_creation_input_tokens":2}}})
    }
    fn block(index: u32, content: Value) -> Value {
        json!({"type":"content_block_start","index":index,"content_block":content})
    }
    fn delta(index: u32, delta: Value) -> Value {
        json!({"type":"content_block_delta","index":index,"delta":delta})
    }
    fn stop(index: u32) -> Value {
        json!({"type":"content_block_stop","index":index})
    }
    fn finish(reason: &str) -> Vec<Value> {
        vec![
            json!({"type":"message_delta","delta":{"stop_reason":reason},"usage":{"output_tokens":7}}),
            json!({"type":"message_stop"}),
        ]
    }
    fn steps(events: Vec<Value>) -> Vec<Step> {
        events
            .into_iter()
            .map(|event| {
                Step::Chunk(Box::leak(
                    format!(
                        "event: {}\r\ndata: {event}\r\n\r\n",
                        event["type"].as_str().unwrap()
                    )
                    .into_boxed_str(),
                ))
            })
            .collect()
    }
    fn parse(
        events: Vec<Value>,
        conversation: &ChatConversation,
    ) -> Result<ProviderExecutionResult, ProviderExecutionError> {
        let config = ProviderExecutionConfig {
            transport: LlmTransport::AnthropicMessages,
            ..Default::default()
        };
        crate::test_support::wait(stream::read_response(
            payload(steps(events), config.cancellation.clone()),
            200,
            Instant::now(),
            Some("request-1".into()),
            &config,
            None,
            conversation,
        ))
    }

    #[test]
    fn anthropic_request_maps_native_messages_images_tools_formats_and_parameters() {
        let conversation: ChatConversation = ChatConversation {
            messages: serde_json::from_value(json!([
                {"role":"system","content":"instructions"},
                {"role":"user","content":[{"type":"text","text":"look"},{"type":"image_url","image_url":{"url":"data:image/png;base64,AAAA"}}]},
                {"role":"assistant","content":null,"tool_calls":[{"id":"c","type":"function","function":{"name":"f","arguments":"{}"}}]},
                {"role":"tool","tool_call_id":"c","content":"done"}
            ])).unwrap(),
            tools: serde_json::from_value(json!([{"type":"function","function":{"name":"f","description":"function","parameters":{"type":"object"},"strict":true}}])).unwrap(),
            tool_choice: Some(serde_json::from_value(json!("required")).unwrap()),
            parallel_tool_calls: Some(false),
            response_format: Some(serde_json::from_value(json!({"type":"json_schema","json_schema":{"name":"answer","schema":{"type":"object","properties":{},"additionalProperties":false},"strict":true}})).unwrap()),
            ..Default::default()
        };
        let parameters = LlmParameters {
            max_completion_tokens: Some(2048),
            reasoning_effort: Some("high".into()),
            ..Default::default()
        };
        let body = request_body(
            super::super::request_body("claude-test", &parameters, &conversation).unwrap(),
            &conversation,
        )
        .unwrap();
        assert_eq!(body["system"][0]["text"], "instructions");
        assert_eq!(
            body["messages"][0]["content"][1],
            json!({"type":"image","source":{"type":"base64","media_type":"image/png","data":"AAAA"}})
        );
        assert_eq!(
            body["messages"][1]["content"][0],
            json!({"type":"tool_use","id":"c","name":"f","input":{}})
        );
        assert_eq!(
            body["messages"][2]["content"][0],
            json!({"type":"tool_result","tool_use_id":"c","content":"done"})
        );
        assert_eq!(body["tools"][0]["input_schema"]["type"], "object");
        assert_eq!(
            body["tool_choice"],
            json!({"type":"any","disable_parallel_tool_use":true})
        );
        assert_eq!(body["output_config"]["format"]["schema"]["type"], "object");
        assert_eq!(body["output_config"]["effort"], "high");
        assert_eq!(body["thinking"]["type"], "adaptive");
        assert_eq!(body["max_tokens"], 2048);
        assert!(body.get("stream_options").is_none());
        let mut recorder = RequestRecorder::default();
        recorder
            .image_attachments
            .insert(1, vec![AttachmentId::new(9)]);
        let (sent, _) = recorder
            .prepare(JobId::new(1), RequestNumber::FIRST, &body, 0)
            .unwrap();
        let recorded = sent
            .bodies
            .iter()
            .map(|body| String::from_utf8_lossy(&body.bytes).into_owned())
            .collect::<Vec<_>>()
            .join("");
        assert!(!recorded.contains("AAAA"));
        assert!(recorded.contains("attachment"));
    }

    #[test]
    fn anthropic_large_signed_events_obey_the_configured_output_budget() {
        let signature = "s".repeat(70000);
        let text = "\0".repeat(12000);
        let mut events = vec![
            start(),
            block(
                0,
                json!({"type":"thinking","thinking":"why","signature":""}),
            ),
            delta(0, json!({"type":"signature_delta","signature":signature})),
            stop(0),
            block(1, json!({"type":"text","text":text})),
            stop(1),
        ];
        events.extend(finish("end_turn"));
        let result = parse(events.clone(), &ChatConversation::default()).unwrap();
        assert_eq!(result.text, text);
        assert_eq!(
            result.response_items[0]["content"][0]["signature"],
            signature
        );
        let config = ProviderExecutionConfig {
            transport: LlmTransport::AnthropicMessages,
            max_output_bytes: 65536,
            ..Default::default()
        };
        let error = crate::test_support::wait(stream::read_response(
            payload(steps(events), config.cancellation.clone()),
            200,
            Instant::now(),
            None,
            &config,
            None,
            &ChatConversation::default(),
        ))
        .unwrap_err();
        assert!(matches!(error, ProviderExecutionError::Stopped { .. }));
    }

    #[test]
    fn anthropic_stream_preserves_signed_thinking_text_and_cache_usage() {
        let mut events = vec![
            start(),
            json!({"type":"future_event"}),
            block(0, json!({"type":"thinking","thinking":"","signature":""})),
            delta(0, json!({"type":"thinking_delta","thinking":"why"})),
            delta(0, json!({"type":"signature_delta","signature":"signed"})),
            stop(0),
            block(1, json!({"type":"text","text":"hi"})),
            delta(1, json!({"type":"text_delta","text":" 世界"})),
            stop(1),
        ];
        events.extend(finish("end_turn"));
        let result = parse(events, &ChatConversation::default()).unwrap();
        assert_eq!(result.text, "hi 世界");
        assert_eq!(result.reasoning, "why");
        assert_eq!(result.raw_finish_reason.as_deref(), Some("end_turn"));
        assert_eq!(result.usage.input_tokens, Some(15));
        assert_eq!(result.usage.output_tokens, Some(7));
        assert_eq!(result.usage.cache_read_tokens, Some(3));
        assert_eq!(result.usage.cache_write_tokens, Some(2));
        assert_eq!(
            result.response_items[0]["content"][0]["signature"],
            "signed"
        );
        let mut conversation = ChatConversation {
            messages: vec![
                ChatMessage::user("task"),
                ChatMessage::assistant(Some(result.text), vec![]),
            ],
            ..Default::default()
        };
        conversation.response_items.insert(1, result.response_items);
        let body = request_body(
            super::super::request_body("m", &LlmParameters::default(), &conversation).unwrap(),
            &conversation,
        )
        .unwrap();
        assert_eq!(body["messages"][1]["content"][0]["signature"], "signed");
        assert!(body["messages"][1].get("reasoning_content").is_none());
    }

    #[test]
    fn anthropic_tool_arguments_are_admitted_only_after_a_complete_message() {
        let mut events = vec![
            start(),
            block(
                0,
                json!({"type":"tool_use","id":"call","name":"f","input":{}}),
            ),
            delta(
                0,
                json!({"type":"input_json_delta","partial_json":"{\"value\":"}),
            ),
            delta(0, json!({"type":"input_json_delta","partial_json":"1}"})),
            stop(0),
        ];
        events.extend(finish("tool_use"));
        let conversation = ChatConversation { tools:serde_json::from_value(json!([{"type":"function","function":{"name":"f","parameters":{"type":"object","properties":{"value":{"type":"integer"}},"required":["value"]}}}])).unwrap(),..Default::default() };
        let result = parse(events.clone(), &conversation).unwrap();
        assert!(result.refused.is_empty());
        assert_eq!(result.tool_calls[0].arguments, "{\"value\":1}");
        assert_eq!(result.finish_reason, Some(ChatFinishReason::ToolCalls));
        let refused = parse(events.clone(), &ChatConversation::default()).unwrap();
        assert_eq!(refused.tool_calls.len(), 1);
        assert_eq!(refused.refused.len(), 1);
        events.pop();
        assert!(matches!(
            parse(events, &conversation),
            Err(ProviderExecutionError::Incomplete { .. })
        ));
    }

    #[test]
    fn anthropic_rejects_incomplete_or_conflicting_block_sequences() {
        for events in [
            vec![block(0, json!({"type":"text","text":"x"}))],
            vec![
                start(),
                block(0, json!({"type":"text","text":"x"})),
                block(0, json!({"type":"text","text":"y"})),
            ],
            vec![
                start(),
                block(0, json!({"type":"text","text":"x"})),
                json!({"type":"message_delta","delta":{"stop_reason":"end_turn"}}),
                json!({"type":"message_stop"}),
            ],
            vec![
                start(),
                block(0, json!({"type":"text","text":"x"})),
                delta(0, json!({"type":"thinking_delta","thinking":"wrong"})),
            ],
            vec![start(), stop(0)],
        ] {
            assert!(matches!(
                parse(events, &ChatConversation::default()),
                Err(ProviderExecutionError::Incomplete { .. })
            ));
        }
    }

    #[test]
    fn anthropic_usage_is_final_only_at_message_stop_and_unknown_counters_stay_unknown() {
        let mut stream = MessageStream::default();
        stream.event(&start(), 1000).unwrap();
        assert_eq!(
            stream.reported_usage().unwrap().finality,
            UsageFinality::Cumulative
        );
        stream.event(&json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":7}}),1000).unwrap();
        assert_eq!(
            stream.reported_usage().unwrap().finality,
            UsageFinality::Cumulative
        );
        stream.event(&json!({"type":"message_stop"}), 1000).unwrap();
        assert_eq!(
            stream.reported_usage().unwrap().finality,
            UsageFinality::Final
        );
        let mut missing = MessageStream::default();
        missing
            .event(
                &json!({"type":"message_start","message":{"usage":{"output_tokens":0}}}),
                1000,
            )
            .unwrap();
        assert_eq!(missing.reported_usage().unwrap().usage.input_tokens, None);
    }

    #[test]
    fn anthropic_parameters_fail_before_sending_and_headers_are_sensitive() {
        let conversation = ChatConversation {
            messages: vec![ChatMessage::user("task")],
            ..Default::default()
        };
        for parameters in [
            LlmParameters {
                presence_penalty: Some(0.0),
                ..Default::default()
            },
            LlmParameters {
                max_tokens: Some(10),
                max_completion_tokens: Some(10),
                ..Default::default()
            },
        ] {
            assert!(matches!(
                request_body(
                    super::super::request_body("m", &parameters, &conversation).unwrap(),
                    &conversation
                ),
                Err(ProviderExecutionError::Build { .. })
            ));
        }
        assert!(api_key("test-only-key").unwrap().is_sensitive());
        assert!(api_key("bad\nkey").is_err());
        assert_eq!(
            exchange::endpoint("https://api.anthropic.com/v1/messages", "models").unwrap(),
            "https://api.anthropic.com/v1/models"
        );
    }
}

#[cfg(test)]
mod http_tests {
    use super::*;
    use crate::persistence::*;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};

    fn request(socket: &mut TcpStream) -> (String, Value) {
        socket
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut raw = Vec::new();
        loop {
            let mut chunk = [0; 8192];
            let read = socket.read(&mut chunk).unwrap();
            assert_ne!(read, 0);
            raw.extend_from_slice(&chunk[..read]);
            if let Some(offset) = raw.windows(4).position(|window| window == b"\r\n\r\n") {
                let head = String::from_utf8(raw[..offset].to_vec()).unwrap();
                let size = head
                    .lines()
                    .find_map(|line| {
                        line.split_once(':')
                            .filter(|(key, _)| key.eq_ignore_ascii_case("content-length"))
                            .map(|(_, value)| value.trim().parse::<usize>().unwrap())
                    })
                    .unwrap_or(0);
                if raw.len() >= offset + 4 + size {
                    let body = if size == 0 {
                        Value::Null
                    } else {
                        serde_json::from_slice(&raw[offset + 4..offset + 4 + size]).unwrap()
                    };
                    return (head, body);
                }
            }
        }
    }
    fn respond(socket: &mut TcpStream, content_type: &str, body: &str) {
        write!(socket,"HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nrequest-id: anthropic-request\r\nConnection: close\r\n\r\n{body}",body.len()).unwrap();
    }
    struct Key;
    impl CredentialResolver for Key {
        fn resolve(&self, _: Option<&str>) -> Result<Option<String>, ProviderExecutionError> {
            Ok(Some("test-only-key".into()))
        }
    }

    #[test]
    fn anthropic_http_execution_authenticates_and_replays_signed_tool_rounds() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}/v1/messages", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            for round in 0..2 {
                let (mut socket, _) = listener.accept().unwrap();
                let (head, body) = request(&mut socket);
                let lower = head.to_ascii_lowercase();
                assert!(head.starts_with("POST /v1/messages HTTP/"));
                assert!(lower.contains("x-api-key: test-only-key"));
                assert!(lower.contains("anthropic-version: 2023-06-01"));
                assert!(!lower.contains("authorization:"));
                assert_eq!(body["system"][0]["text"], "instructions");
                assert_eq!(body["max_tokens"], 4096);
                let content = if round == 0 {
                    vec![
                        json!({"type":"thinking","thinking":"why","signature":"signed"}),
                        json!({"type":"tool_use","id":"call","name":"f","input":{}}),
                        json!({"type":"tool_use","id":"call-2","name":"f","input":{}}),
                    ]
                } else {
                    assert_eq!(body["messages"][1]["content"][0]["signature"], "signed");
                    assert_eq!(body["messages"][1]["content"][1]["id"], "call");
                    assert_eq!(
                        body["messages"][2]["content"][0],
                        json!({"type":"tool_result","tool_use_id":"call","content":"done"})
                    );
                    assert_eq!(body["messages"][1]["content"][2]["id"], "call-2");
                    assert_eq!(
                        body["messages"][3]["content"][0],
                        json!({"type":"tool_result","tool_use_id":"call-2","content":"also done"})
                    );
                    vec![json!({"type":"text","text":"finished"})]
                };
                let mut events = vec![
                    json!({"type":"message_start","message":{"content":[],"usage":{"input_tokens":10,"output_tokens":0}}}),
                ];
                for (index, content) in content.into_iter().enumerate() {
                    events.push(
                        json!({"type":"content_block_start","index":index,"content_block":content}),
                    );
                    events.push(json!({"type":"content_block_stop","index":index}));
                }
                events.push(json!({"type":"message_delta","delta":{"stop_reason":if round == 0 { "tool_use" } else { "end_turn" }},"usage":{"output_tokens":4}}));
                events.push(json!({"type":"message_stop"}));
                let body = events
                    .into_iter()
                    .map(|event| format!("data: {event}\n\n"))
                    .collect::<String>();
                respond(&mut socket, "text/event-stream", &body);
            }
        });
        crate::test_support::on_system(|| async move {
            let frozen = FrozenExecutionConfig {
                agent: None,
                agent_ref: None,
                llm: FrozenLlmConfig {
                    config: LlmConfig {
                        model: "claude-test".into(),
                        context_window: None,
                        parameters: LlmParameters::default(),
                        declared_capabilities: [ModelCapability::Chat, ModelCapability::Tools]
                            .into(),
                    },
                    provider: ProviderSnapshot {
                        id: LlmProviderId::new(1),
                        edit_seq: 1,
                        name: "test".into(),
                        transport: LlmTransport::AnthropicMessages,
                        endpoint: Some(endpoint),
                        credential_ref: None,
                    },
                    model: ConfigRef {
                        id: ConfigId::new(1),
                        edit_seq: 1,
                    },
                },
            };
            let mut conversation = ChatConversation { messages:vec![ChatMessage::system("instructions"),ChatMessage::user("task")],tools:serde_json::from_value(json!([{"type":"function","function":{"name":"f","parameters":{"type":"object","properties":{}}}}])).unwrap(),..Default::default() };
            let result = execute_conversation(
                &frozen,
                &conversation,
                &Key,
                &ProviderExecutionConfig::default(),
                None,
            )
            .await
            .unwrap()
            .admitted()
            .unwrap();
            assert_eq!(result.tool_calls.len(), 2);
            assert_eq!(result.request_id.as_deref(), Some("anthropic-request"));
            conversation.response_items.insert(2, result.response_items);
            conversation.messages.push(ChatMessage::assistant(
                None,
                result
                    .tool_calls
                    .iter()
                    .map(crate::openai_compatible::ChatToolCallWire::from)
                    .collect(),
            ));
            conversation
                .messages
                .push(ChatMessage::tool("call", "done"));
            conversation
                .messages
                .push(ChatMessage::tool("call-2", "also done"));
            let result = execute_conversation(
                &frozen,
                &conversation,
                &Key,
                &ProviderExecutionConfig::default(),
                None,
            )
            .await
            .unwrap();
            assert_eq!(result.text, "finished");
        });
        server.join().unwrap();
    }

    #[test]
    fn anthropic_model_discovery_authenticates_and_follows_native_cursors() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}/v1/messages", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            for round in 0..2 {
                let (mut socket, _) = listener.accept().unwrap();
                let (head, _) = request(&mut socket);
                assert!(
                    head.to_ascii_lowercase()
                        .contains("x-api-key: test-only-key")
                );
                assert!(
                    head.to_ascii_lowercase()
                        .contains("anthropic-version: 2023-06-01")
                );
                if round == 0 {
                    assert!(head.starts_with("GET /v1/models?limit=1000 HTTP/"));
                } else {
                    assert!(head.starts_with("GET /v1/models?limit=1000&after_id=model%2Fa HTTP/"));
                }
                let body = if round == 0 {
                    json!({"data":[{"id":"model/a","max_input_tokens":200000}],"has_more":true,"last_id":"model/a"})
                } else {
                    json!({"data":[{"id":"model-b"}],"has_more":false,"last_id":"model-b"})
                };
                respond(&mut socket, "application/json", &body.to_string());
            }
        });
        crate::test_support::on_system(|| async move {
            let models = match list_models(
                &endpoint,
                Some("test-only-key"),
                LlmTransport::AnthropicMessages,
            )
            .await
            {
                Ok(models) => models,
                Err(_) => panic!("native model listing failed"),
            };
            assert_eq!(models.len(), 2);
            assert!(
                models
                    .iter()
                    .any(|model| model.id == "model/a" && model.context_window == Some(200000))
            );
        });
        server.join().unwrap();
    }
}
