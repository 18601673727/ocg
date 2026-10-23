use std::collections::BTreeMap;

use super::*;

fn build(message: &str) -> ProviderExecutionError {
    ProviderExecutionError::Build {
        message: message.to_owned(),
    }
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
pub(super) struct ResponseStream {
    items: BTreeMap<u32, Item>,
    pub(super) output: Vec<Value>,
    pub(super) terminal: bool,
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

    fn suffix(previous: &mut String, value: &str, snapshot: bool) -> Result<String, &'static str> {
        if snapshot {
            let suffix = value
                .strip_prefix(previous.as_str())
                .ok_or("Responses snapshot conflicts with streamed output")?
                .to_owned();
            previous.push_str(&suffix);
            Ok(suffix)
        } else {
            previous.push_str(value);
            Ok(value.to_owned())
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
        if item.done {
            let previous = item.text.get(&(field.to_owned(), part));
            if !snapshot || previous.is_none_or(|previous| previous != value) {
                return Err("Responses text changed after its item completed");
            }
        }
        let text = item.text.entry((field.to_owned(), part)).or_default();
        let suffix = Self::suffix(text, value, snapshot)?;
        self.bytes = self.bytes.saturating_add(suffix.len());
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
        if item.done && (!snapshot || item.arguments != value) {
            return Err("Responses arguments changed after its item completed");
        }
        let suffix = Self::suffix(&mut item.arguments, value, snapshot)?;
        self.bytes = self.bytes.saturating_add(suffix.len());
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

    pub(super) fn event(&mut self, event: &Value, limit: usize) -> Result<Value, &'static str> {
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
                for (index, item) in output.iter().enumerate() {
                    self.snapshot(index as u32, item, true, &mut delta)?;
                }
                if self.items.len() != output.len() {
                    return Err("Responses completion omitted streamed items");
                }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::*;
    use crate::provider_execution::scripted::{Step, payload};

    fn message(text: &str) -> Value {
        json!({"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[{"type":"output_text","text":text,"annotations":[]}]})
    }
    fn complete(output: Vec<Value>) -> Value {
        json!({"type":"response.completed","response":{"status":"completed","output":output,"usage":{"input_tokens":12,"output_tokens":4,"input_tokens_details":{"cached_tokens":3},"output_tokens_details":{"reasoning_tokens":2}}}})
    }
    fn events(events: Vec<Value>) -> Vec<Step> {
        events
            .into_iter()
            .map(|event| {
                Step::Chunk(Box::leak(
                    format!(
                        "event: {}\ndata: {event}\n\n",
                        event["type"].as_str().unwrap()
                    )
                    .into_boxed_str(),
                ))
            })
            .collect()
    }
    fn config() -> ProviderExecutionConfig {
        ProviderExecutionConfig {
            transport: LlmTransport::OpenAiResponses,
            ..Default::default()
        }
    }
    fn read(
        events: Vec<Value>,
        max: usize,
    ) -> Result<ProviderExecutionResult, ProviderExecutionError> {
        let config = ProviderExecutionConfig {
            max_output_bytes: max,
            ..config()
        };
        crate::test_support::wait(stream::read_response(
            payload(self::events(events), config.cancellation.clone()),
            200,
            Instant::now(),
            None,
            &config,
            None,
            &ChatConversation::default(),
        ))
    }
    fn text_delta(text: &str) -> Value {
        json!({"type":"response.output_text.delta","output_index":0,"content_index":0,"item_id":"msg_1","delta":text})
    }

    #[test]
    fn responses_text_refusals_usage_and_terminal_event() {
        ntex::rt::System::new("responses-stream", ntex::rt::DefaultRuntime).block_on(async {
            let cfg = config();
            let mut steps = events(vec![text_delta("hello"), complete(vec![message("hello")])]);
            steps.push(Step::Stall);
            let result = stream::read_response(
                payload(steps, cfg.cancellation.clone()),
                200,
                Instant::now(),
                None,
                &cfg,
                None,
                &ChatConversation::default(),
            )
            .await
            .unwrap();
            assert_eq!(result.text, "hello");
            assert_eq!(result.raw_finish_reason.as_deref(), Some("completed"));
            assert_eq!(result.usage.input_tokens, Some(12));
            assert_eq!(result.usage.cache_read_tokens, Some(3));
            assert_eq!(result.usage.reasoning_tokens, Some(2));
            assert_eq!(result.usage.raw.as_ref().unwrap()["input_tokens"], 12);
            let refusal = json!({"type":"message","id":"msg_1","role":"assistant","content":[{"type":"refusal","refusal":"Cannot comply"}]});
            assert_eq!(
                read(vec![complete(vec![refusal])], 1024).unwrap().text,
                "Cannot comply"
            );
        });
    }

    #[test]
    fn responses_reject_conflicts_truncation_failure_and_output_limits() {
        assert!(matches!(
            read(vec![text_delta("abc")], 1024),
            Err(ProviderExecutionError::Incomplete { .. })
        ));
        assert!(matches!(
            read(
                vec![text_delta("abc"), complete(vec![message("different")])],
                1024
            ),
            Err(ProviderExecutionError::Incomplete { .. })
        ));
        assert!(matches!(
            read(vec![text_delta("abc")], 2),
            Err(ProviderExecutionError::Stopped { .. })
        ));
        for kind in ["response.incomplete", "response.failed"] {
            assert!(read(vec![json!({"type":kind,"response":{"error":{"message":"failed"},"incomplete_details":{"reason":"max_output_tokens"}}})],1024).is_err());
        }
        assert!(
            read(
                vec![json!({"type":"response.output_text.delta","delta":"missing indexes"})],
                1024
            )
            .is_err()
        );
    }

    #[test]
    fn responses_function_calls_and_reasoning_are_carried_into_next_input() {
        let reasoning = json!({"type":"reasoning","id":"rs_1","summary":[{"type":"summary_text","text":"Thinking"}],"encrypted_content":"opaque"});
        let call = json!({"type":"function_call","id":"fc_1","call_id":"call_1","name":"f","arguments":"{}","status":"completed"});
        let cfg = config();
        let received = vec![
            json!({"type":"response.output_item.added","output_index":0,"item":{"type":"reasoning","id":"rs_1","summary":[]}}),
            json!({"type":"response.reasoning_summary_text.delta","output_index":0,"summary_index":0,"item_id":"rs_1","delta":"Thinking"}),
            json!({"type":"response.output_item.done","output_index":0,"item":reasoning}),
            json!({"type":"response.output_item.added","output_index":1,"item":{"type":"function_call","id":"fc_1","call_id":"call_1","name":"f","arguments":""}}),
            json!({"type":"response.function_call_arguments.delta","output_index":1,"item_id":"fc_1","delta":"{"}),
            json!({"type":"response.function_call_arguments.delta","output_index":1,"item_id":"fc_1","delta":"}"}),
            json!({"type":"response.output_item.done","output_index":1,"item":call}),
            complete(vec![reasoning.clone(), call.clone()]),
        ];
        let result = crate::test_support::wait(stream::parse_stream(
            payload(events(received), cfg.cancellation.clone()),
            200,
            Instant::now(),
            None,
            &cfg,
            None,
            &mut UsageCapture::default(),
        ))
        .unwrap();
        assert_eq!(result.reasoning, "Thinking");
        assert_eq!(result.tool_calls[0].arguments, "{}");
        assert_eq!(result.tool_calls[0].id, "call_1");
        let mut conversation = ChatConversation {
            messages: vec![
                ChatMessage::user("task"),
                ChatMessage::assistant(None, vec![]),
                ChatMessage::tool("call_1", "done"),
            ],
            ..Default::default()
        };
        conversation.response_items.insert(1, result.response_items);
        let body = request_body(
            super::super::request_body("m", &LlmParameters::default(), &conversation).unwrap(),
            &conversation,
        )
        .unwrap();
        assert_eq!(body["input"][1], reasoning);
        assert_eq!(body["input"][2], call);
        assert_eq!(body["input"][3]["type"], "function_call_output");
        assert_eq!(body["input"][3]["call_id"], "call_1");
    }

    #[test]
    fn responses_request_maps_tools_images_formats_and_parameters() {
        let chat = json!({"model":"m","messages":[{"role":"system","content":"system"},{"role":"user","content":[{"type":"text","text":"look"},{"type":"image_url","image_url":{"url":"data:image/png;base64,AAAA"}}]}],"stream":true,"stream_options":{"include_usage":true},"max_completion_tokens":100,"reasoning_effort":"high","tools":[{"type":"function","function":{"name":"f","parameters":{"type":"object"}}}],"tool_choice":{"type":"function","function":{"name":"f"}},"response_format":{"type":"json_schema","json_schema":{"name":"result","schema":{"type":"object"},"strict":true}}});
        let body = request_body(chat, &ChatConversation::default()).unwrap();
        assert_eq!(body["input"][1]["content"][1]["type"], "input_image");
        assert_eq!(body["max_output_tokens"], 100);
        assert_eq!(body["reasoning"]["effort"], "high");
        assert_eq!(body["tools"][0]["name"], "f");
        assert_eq!(body["tools"][0]["strict"], false);
        assert_eq!(body["tool_choice"]["name"], "f");
        assert_eq!(body["text"]["format"]["name"], "result");
        assert_eq!(body["store"], false);
        assert!(body.get("messages").is_none());
        assert!(body.get("stream_options").is_none());
        assert!(
            request_body(
                json!({"messages":[],"presence_penalty":0.5}),
                &ChatConversation::default()
            )
            .is_err()
        );
    }

    #[test]
    fn responses_recorder_keeps_native_input_without_duplicate_replies_or_image_data() {
        let mut recorder = RequestRecorder::default();
        recorder
            .image_attachments
            .insert(2, vec![AttachmentId::new(42)]);
        let first = json!({"model":"m","input":[{"role":"user","content":[{"type":"input_image","image_url":"data:image/png;base64,AAAA"}]}],"store":false});
        let (sent, state) = recorder
            .prepare(JobId::new(1), RequestNumber::FIRST, &first, 0)
            .unwrap();
        assert_eq!(sent.messages.len(), 1);
        let image: Value = serde_json::from_slice(&sent.bodies[0].bytes).unwrap();
        assert!(!image.to_string().contains("AAAA"));
        recorder.remember_sent(state);
        let response = recorder
            .response_with_items(
                &ChatMessage::assistant(Some("reply".into()), vec![]),
                &[message("reply")],
            )
            .unwrap();
        recorder.remember_response(response.digest());
        let mut next = first.clone();
        next["input"]
            .as_array_mut()
            .unwrap()
            .extend([message("reply"), json!({"role":"user","content":"again"})]);
        let (sent, _) = recorder
            .prepare(
                JobId::new(1),
                RequestNumber::FIRST.next().unwrap(),
                &next,
                0,
            )
            .unwrap();
        assert_eq!(sent.messages.len(), 3);
        assert_eq!(sent.bodies.len(), 1);
    }
}

#[cfg(test)]
mod http_tests {
    use super::*;
    use crate::persistence::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;

    #[test]
    fn responses_http_endpoint_and_tool_continuation_use_native_payloads() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}/v1/responses", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            let mut requests = Vec::new();
            for round in 0..2 {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut raw = Vec::new();
                let mut buffer = [0; 4096];
                let (head, body) = loop {
                    let n = socket.read(&mut buffer).unwrap();
                    assert_ne!(n, 0);
                    raw.extend_from_slice(&buffer[..n]);
                    if let Some(offset) = raw.windows(4).position(|v| v == b"\r\n\r\n") {
                        let head = String::from_utf8(raw[..offset].to_vec()).unwrap();
                        let size: usize = head
                            .lines()
                            .find_map(|line| {
                                let (name, value) = line.split_once(':')?;
                                name.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse().unwrap())
                            })
                            .unwrap();
                        if raw.len() >= offset + 4 + size {
                            break (
                                head,
                                serde_json::from_slice::<Value>(
                                    &raw[offset + 4..offset + 4 + size],
                                )
                                .unwrap(),
                            );
                        }
                    }
                };
                assert!(head.starts_with("POST /v1/responses HTTP/"));
                assert!(head.to_ascii_lowercase().contains("user-agent: ocg/"));
                assert!(body.get("messages").is_none());
                assert_eq!(body["store"], false);
                let output = if round == 0 {
                    json!([{"type":"function_call","id":"fc_1","call_id":"call_1","name":"f","arguments":"{}","status":"completed"}])
                } else {
                    assert_eq!(body["input"][1]["call_id"], "call_1");
                    assert_eq!(
                        body["input"][2],
                        json!({"type":"function_call_output","call_id":"call_1","output":"done"})
                    );
                    json!([{"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[{"type":"output_text","text":"finished","annotations":[]}]}])
                };
                requests.push(body);
                let event = json!({"type":"response.completed","response":{"status":"completed","output":output,"usage":{"input_tokens":10,"output_tokens":2}}});
                let body = format!("event: response.completed\ndata: {event}\n\n");
                write!(socket,"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).unwrap();
            }
            requests
        });
        ntex::rt::System::new("responses-http", ntex::rt::DefaultRuntime).block_on(async move {
            let frozen = FrozenExecutionConfig {
                agent: None,
                agent_ref: None,
                llm: FrozenLlmConfig {
                    config: LlmConfig {
                        model: "m".into(),
                        context_window: None,
                        parameters: LlmParameters::default(),
                        declared_capabilities: [ModelCapability::Chat, ModelCapability::Tools]
                            .into(),
                    },
                    provider: ProviderSnapshot {
                        id: LlmProviderId::new(1),
                        edit_seq: 1,
                        name: "test".into(),
                        transport: LlmTransport::OpenAiResponses,
                        endpoint: Some(endpoint),
                        credential_ref: None,
                    },
                    model: ConfigRef {
                        id: ConfigId::new(1),
                        edit_seq: 1,
                    },
                },
            };
            let mut conversation = ChatConversation { messages: vec![ChatMessage::user("task")], tools: serde_json::from_value(json!([{"type":"function","function":{"name":"f","parameters":{"type":"object","properties":{}}}}])).unwrap(), ..Default::default() };
            let result = execute_conversation(
                &frozen,
                &conversation,
                &EnvironmentCredentialResolver,
                &ProviderExecutionConfig::default(),
                None,
            )
            .await
            .unwrap()
            .admitted()
            .unwrap();
            assert_eq!(result.tool_calls.len(), 1);
            conversation.response_items.insert(1, result.response_items);
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
                .push(ChatMessage::tool("call_1", "done"));
            let result = execute_conversation(
                &frozen,
                &conversation,
                &EnvironmentCredentialResolver,
                &ProviderExecutionConfig::default(),
                None,
            )
            .await
            .unwrap();
            assert_eq!(result.text, "finished");
        });
        assert_eq!(server.join().unwrap().len(), 2);
    }
}
