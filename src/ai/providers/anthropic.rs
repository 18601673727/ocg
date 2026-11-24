use ntex::http::header::HeaderValue;
use serde_json::{Value, json};

use super::openai_compatible::{
    ChatConversation, ChatFunctionCallWire, ChatMessage, ChatRole, ChatToolCallWire,
    CompletedToolCall, ContentPartWire, MessageContentWire, ToolCallAssembler,
};
use super::{
    ProviderExecutionError, ProviderFact, ProviderOutputSink, ResponseFacts, announce,
    incomplete_tool_calls,
};
use crate::db::entities::{
    ChatFinishReason, FrozenLlmConfig, IntermediateOutputKind, LlmParameters, NormalizedUsage,
    ProviderFailure, UsageFinality,
};

pub(crate) const API_VERSION: &str = "2023-06-01";
pub(crate) const DEFAULT_ENDPOINT: &str = "https://api.anthropic.com/v1";
pub(crate) const MAX_TEMPERATURE: f64 = 1.0;
pub(crate) const DEFAULT_MAX_TOKENS: u32 = 16_384;

pub(crate) fn max_tokens(parameters: &LlmParameters) -> u32 {
    parameters
        .max_tokens
        .or(parameters.max_completion_tokens)
        .unwrap_or(DEFAULT_MAX_TOKENS)
}

pub(crate) fn parameter_refusal(
    parameters: &LlmParameters,
) -> Option<(&'static str, &'static str)> {
    const UNSUPPORTED: &str = "Anthropic Messages does not support this setting";
    if parameters.reasoning_effort.is_some() {
        return Some(("reasoning_effort", UNSUPPORTED));
    }
    if parameters.presence_penalty.is_some() {
        return Some(("presence_penalty", UNSUPPORTED));
    }
    if parameters.frequency_penalty.is_some() {
        return Some(("frequency_penalty", UNSUPPORTED));
    }
    if parameters
        .temperature
        .is_some_and(|value| !(0.0..=MAX_TEMPERATURE).contains(&value))
    {
        return Some((
            "temperature",
            "Anthropic Messages requires temperature in 0..=1",
        ));
    }
    if parameters.temperature.is_some() && parameters.top_p.is_some() {
        return Some((
            "temperature",
            "Anthropic Messages does not allow temperature and top_p together",
        ));
    }
    None
}

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
    frozen: &FrozenLlmConfig,
    conversation: &ChatConversation,
) -> Result<Value, ProviderExecutionError> {
    let parameters = &frozen.config.parameters;
    if let Some((setting, reason)) = parameter_refusal(parameters) {
        return Err(build(&format!("{setting}: {reason}")));
    }

    let mut system = Vec::new();
    let mut turns: Vec<(&str, Vec<Value>)> = Vec::new();
    for message in &conversation.messages {
        let role = match message.role {
            ChatRole::System | ChatRole::Developer => {
                match &message.content {
                    Some(MessageContentWire::Text(text)) => system.push(text.as_str()),
                    Some(MessageContentWire::Parts(parts)) => {
                        for part in parts {
                            let ContentPartWire::Text { text } = part else {
                                return Err(build("Anthropic system instructions must be text"));
                            };
                            system.push(text);
                        }
                    }
                    None => {}
                }
                continue;
            }
            ChatRole::User | ChatRole::Tool => "user",
            ChatRole::Assistant => "assistant",
            ChatRole::Function => return Err(build("unsupported Anthropic function message")),
        };
        let blocks = message_blocks(message)?;
        if blocks.is_empty() {
            continue;
        }
        match turns.last_mut() {
            Some((last, content)) if *last == role => content.extend(blocks),
            _ => turns.push((role, blocks)),
        }
    }
    if !matches!(turns.first(), Some(("user", _))) {
        return Err(build("Anthropic Messages requires an initial user message"));
    }

    let messages: Vec<_> = turns
        .into_iter()
        .map(|(role, content)| json!({ "role": role, "content": content }))
        .collect();
    let mut body = json!({
        "model": frozen.config.model,
        "messages": messages,
        "stream": true,
        "max_tokens": max_tokens(parameters),
    });
    if !system.is_empty() {
        body["system"] = Value::String(system.join("\n\n"));
    }
    if let Some(temperature) = parameters.temperature {
        body["temperature"] = json!(temperature);
    }
    if let Some(top_p) = parameters.top_p {
        body["top_p"] = json!(top_p);
    }
    if !conversation.tools.is_empty() {
        body["tools"] = Value::Array(
            conversation
                .tools
                .iter()
                .map(|tool| {
                    let function = &tool.function;
                    let mut schema = function
                        .parameters
                        .clone()
                        .unwrap_or_else(|| json!({"type":"object"}));
                    // Messages rejects these combinators at the root. Admission still uses the full schema.
                    if let Some(object) = schema.as_object_mut() {
                        for keyword in ["oneOf", "anyOf", "allOf"] {
                            object.remove(keyword);
                        }
                    }
                    let mut tool = json!({"name": function.name, "input_schema": schema});
                    if let Some(description) = &function.description {
                        tool["description"] = json!(description);
                    }
                    tool
                })
                .collect(),
        );
    }
    Ok(body)
}

fn message_blocks(message: &ChatMessage) -> Result<Vec<Value>, ProviderExecutionError> {
    if let Some(blocks) = &message.response_items {
        return Ok(blocks.clone());
    }
    let mut blocks = Vec::new();
    if message.role == ChatRole::Tool {
        let (Some(call_id), Some(MessageContentWire::Text(text))) =
            (&message.tool_call_id, &message.content)
        else {
            return Err(build(
                "Anthropic tool results require a tool call ID and text",
            ));
        };
        blocks.push(json!({"type": "tool_result", "tool_use_id": call_id, "content": text}));
        return Ok(blocks);
    }
    let text_block = |text: &str| json!({"type":"text","text":text});
    match &message.content {
        Some(MessageContentWire::Text(text)) if !text.is_empty() => blocks.push(text_block(text)),
        Some(MessageContentWire::Parts(parts)) => {
            for part in parts {
                match part {
                    ContentPartWire::Text { text } => {
                        if !text.is_empty() {
                            blocks.push(text_block(text));
                        }
                    }
                    ContentPartWire::ImageUrl { image_url } => {
                        let (media_type, data) = image_url
                            .url
                            .strip_prefix("data:")
                            .and_then(|url| url.split_once(";base64,"))
                            .ok_or_else(|| build("Anthropic images require a base64 data URL"))?;
                        blocks.push(json!({"type":"image", "source": {"type": "base64", "media_type": media_type, "data":data}}));
                    }
                }
            }
        }
        Some(MessageContentWire::Text(_)) | None => {}
    }
    for call in message.tool_calls.iter().flatten() {
        let input = serde_json::from_str::<Value>(&call.function.arguments)
            .ok()
            .filter(Value::is_object)
            .ok_or_else(|| build("Anthropic tool input must be a JSON object"))?;
        blocks.push(
            json!({"type": "tool_use", "id": call.id, "name": call.function.name, "input": input}),
        );
    }
    Ok(blocks)
}

#[derive(Clone, Copy)]
enum Block {
    Text,
    Thinking,
    Tool { call: u32, complete_input: bool },
    Other,
}

pub(crate) enum Event {
    Ignore,
    Progress,
    Stop,
}

pub(crate) struct Stream<'a> {
    status: u16,
    request_id: Option<&'a str>,
    limit: usize,
    started: bool,
    last_index: Option<u32>,
    active: Option<Block>,
    next_call: u32,
    replay_bytes: usize,
    pub(crate) blocks: Vec<Value>,
    pub(crate) calls: ToolCallAssembler,
    pub(crate) text: String,
    pub(crate) usage: NormalizedUsage,
    pub(crate) reason: Option<(ChatFinishReason, String)>,
}

impl<'a> Stream<'a> {
    pub(crate) fn new(status: u16, request_id: Option<&'a str>, limit: usize) -> Self {
        Self {
            status,
            request_id,
            limit,
            started: false,
            last_index: None,
            active: None,
            next_call: 0,
            replay_bytes: 0,
            blocks: Vec::new(),
            calls: ToolCallAssembler::default(),
            text: String::new(),
            usage: NormalizedUsage::default(),
            reason: None,
        }
    }

    fn facts(&self) -> ResponseFacts {
        ResponseFacts {
            status_code: self.status,
            finish_reason: None,
            raw_finish_reason: None,
        }
    }

    fn malformed(&self) -> ProviderExecutionError {
        ProviderExecutionError::Incomplete {
            message: "malformed or incomplete Anthropic Messages response".to_owned(),
            facts: self.facts(),
        }
    }

    fn error(&self, error: &Value) -> ProviderExecutionError {
        ProviderExecutionError::StreamFailure {
            failure: Box::new(ProviderFailure {
                message: error["message"]
                    .as_str()
                    .unwrap_or("Anthropic request failed")
                    .to_owned(),
                provider_code: error["type"].as_str().map(str::to_owned),
                request_id: self.request_id.map(str::to_owned),
                ..ProviderFailure::default()
            }),
            facts: self.facts(),
            finished: false,
        }
    }

    fn count_bytes(&mut self, adding: usize) -> Result<(), ProviderExecutionError> {
        self.replay_bytes = self.replay_bytes.saturating_add(adding);
        if self.replay_bytes > self.limit {
            Err(ProviderExecutionError::Stopped {
                message: "provider output exceeded its limit".to_owned(),
                facts: self.facts(),
            })
        } else {
            Ok(())
        }
    }

    fn extend_block(
        &mut self,
        field: &str,
        fragment: &Value,
    ) -> Result<(), ProviderExecutionError> {
        let fragment = fragment.as_str().ok_or_else(|| self.malformed())?;
        self.count_bytes(fragment.len())?;
        let Some(block) = self.blocks.last_mut() else {
            return Err(self.malformed());
        };
        if block[field].is_null() {
            block[field] = json!("");
        }
        let Value::String(field) = &mut block[field] else {
            return Err(self.malformed());
        };
        field.push_str(fragment);
        Ok(())
    }

    fn apply_call(&mut self, delta: Value) -> Result<(), ProviderExecutionError> {
        self.calls
            .apply(&Value::Array(vec![delta]))
            .map_err(|e| incomplete_tool_calls(e, self.facts()))
    }

    async fn append(
        &mut self,
        kind: IntermediateOutputKind,
        text: &Value,
        sink: &mut dyn ProviderOutputSink,
    ) -> Result<Event, ProviderExecutionError> {
        let text = text.as_str().ok_or_else(|| self.malformed())?;
        if text.is_empty() {
            return Ok(Event::Ignore);
        }
        if kind == IntermediateOutputKind::Progress {
            self.text.push_str(text);
        }
        let index = self
            .blocks
            .len()
            .checked_sub(1)
            .and_then(|i| u32::try_from(i).ok())
            .ok_or_else(|| self.malformed())?;
        sink.append(kind, (0, index), text).await.map_err(|error| {
            ProviderExecutionError::Stopped {
                message: error.message,
                facts: self.facts(),
            }
        })?;
        Ok(Event::Progress)
    }

    async fn start_block(
        &mut self,
        index: u32,
        block: &Value,
        sink: &mut dyn ProviderOutputSink,
    ) -> Result<Event, ProviderExecutionError> {
        if self.active.is_some()
            || self.last_index.is_some_and(|last| index <= last)
            || self.reason.is_some()
        {
            return Err(self.malformed());
        }
        self.count_bytes(crate::db::serialized_len(block).unwrap_or(usize::MAX))?;
        self.blocks.push(block.clone());
        self.last_index = Some(index);
        let (active, event) = match block["type"].as_str().ok_or_else(|| self.malformed())? {
            "text" => (
                Block::Text,
                self.append(IntermediateOutputKind::Progress, &block["text"], sink)
                    .await?,
            ),
            "thinking" => (
                Block::Thinking,
                self.append(IntermediateOutputKind::Reasoning, &block["thinking"], sink)
                    .await?,
            ),
            "tool_use" => {
                let call = self.next_call;
                let input = block["input"].as_object().ok_or_else(|| self.malformed())?;
                let complete_input = !input.is_empty();
                let arguments = if complete_input {
                    block["input"].to_string()
                } else {
                    String::new()
                };
                self.apply_call(json!({"index":call, "id":block["id"], "function":{"name":block["name"], "arguments": arguments}}))?;
                self.next_call += 1;
                (
                    Block::Tool {
                        call,
                        complete_input,
                    },
                    Event::Progress,
                )
            }
            _ => (Block::Other, Event::Ignore),
        };
        self.active = Some(active);
        Ok(event)
    }

    async fn delta(
        &mut self,
        index: u32,
        delta: &Value,
        sink: &mut dyn ProviderOutputSink,
    ) -> Result<Event, ProviderExecutionError> {
        let active = self
            .active
            .filter(|_| self.last_index == Some(index))
            .ok_or_else(|| self.malformed())?;
        let kind = delta["type"].as_str().ok_or_else(|| self.malformed())?;
        match (active, kind) {
            (Block::Text, "text_delta") => {
                self.extend_block("text", &delta["text"])?;
                self.append(IntermediateOutputKind::Progress, &delta["text"], sink)
                    .await
            }
            (Block::Thinking, "thinking_delta") => {
                self.extend_block("thinking", &delta["thinking"])?;
                self.append(IntermediateOutputKind::Reasoning, &delta["thinking"], sink)
                    .await
            }
            (
                Block::Tool {
                    call,
                    complete_input: false,
                },
                "input_json_delta",
            ) => {
                let part = delta["partial_json"]
                    .as_str()
                    .ok_or_else(|| self.malformed())?;
                self.count_bytes(part.len())?;
                self.apply_call(json!({"index":call,"function":{"arguments":part}}))?;
                Ok(Event::Progress)
            }
            (Block::Thinking, "signature_delta") => {
                self.extend_block("signature", &delta["signature"])?;
                Ok(Event::Ignore)
            }
            (Block::Text, "citations_delta") => {
                let citation = delta.get("citation").ok_or_else(|| self.malformed())?;
                self.count_bytes(crate::db::serialized_len(citation).unwrap_or(usize::MAX))?;
                let Some(block) = self.blocks.last_mut() else {
                    return Err(self.malformed());
                };
                if block["citations"].is_null() {
                    block["citations"] = json!([]);
                }
                let Some(citations) = block["citations"].as_array_mut() else {
                    return Err(self.malformed());
                };
                citations.push(citation.clone());
                Ok(Event::Ignore)
            }
            _ => Err(self.malformed()),
        }
    }

    fn set_reason(&mut self, raw: &Value) -> Result<(), ProviderExecutionError> {
        let raw = raw
            .as_str()
            .filter(|raw| !raw.is_empty() && raw.len() <= 256)
            .ok_or_else(|| self.malformed())?;
        let reason = match raw {
            "end_turn" | "stop_sequence" => ChatFinishReason::Stop,
            "tool_use" => ChatFinishReason::ToolCalls,
            "max_tokens" | "model_context_window_exceeded" => ChatFinishReason::Length,
            "refusal" => ChatFinishReason::ContentFilter,
            _ => ChatFinishReason::Other,
        };
        if !self.calls.is_empty() && reason == ChatFinishReason::Stop {
            return Err(self.malformed());
        }
        self.reason = Some((reason, raw.to_owned()));
        Ok(())
    }

    fn merge_usage(&mut self, value: &Value) -> bool {
        let Some(reported) = value.as_object() else {
            return false;
        };
        let raw = self.usage.raw.get_or_insert_with(|| json!({}));
        let mut changed = false;
        for key in [
            "input_tokens",
            "output_tokens",
            "cache_read_input_tokens",
            "cache_creation_input_tokens",
        ] {
            if let Some(count) = reported
                .get(key)
                .and_then(Value::as_u64)
                .and_then(|v| u32::try_from(v).ok())
            {
                changed |= raw[key].as_u64() != Some(u64::from(count));
                raw[key] = json!(count);
            }
        }
        let count = |key: &str| raw[key].as_u64().and_then(|v| u32::try_from(v).ok());
        self.usage.cache_read_tokens = count("cache_read_input_tokens");
        self.usage.cache_write_tokens = count("cache_creation_input_tokens");
        self.usage.input_tokens = count("input_tokens").and_then(|base| {
            base.checked_add(self.usage.cache_read_tokens.unwrap_or(0))?
                .checked_add(self.usage.cache_write_tokens.unwrap_or(0))
        });
        self.usage.output_tokens = count("output_tokens");
        changed
    }

    async fn report_usage(&self, finality: UsageFinality, sink: &mut dyn ProviderOutputSink) {
        if self.usage.has_counts() {
            announce(
                Some(sink),
                ProviderFact::Usage {
                    usage: self.usage.clone(),
                    finality,
                },
            )
            .await;
        }
    }

    pub(crate) async fn event(
        &mut self,
        event: &Value,
        sink: &mut dyn ProviderOutputSink,
    ) -> Result<Event, ProviderExecutionError> {
        let index = || event["index"].as_u64().and_then(|i| u32::try_from(i).ok());
        match event["type"].as_str().ok_or_else(|| self.malformed())? {
            "message_start" => {
                if self.started || event["message"]["type"] != "message" {
                    return Err(self.malformed());
                }
                self.started = true;
                if self.merge_usage(&event["message"]["usage"]) {
                    self.report_usage(UsageFinality::Cumulative, sink).await;
                }
            }
            "content_block_start" if self.started => {
                return self
                    .start_block(
                        index().ok_or_else(|| self.malformed())?,
                        &event["content_block"],
                        sink,
                    )
                    .await;
            }
            "content_block_delta" if self.started => {
                return self
                    .delta(
                        index().ok_or_else(|| self.malformed())?,
                        &event["delta"],
                        sink,
                    )
                    .await;
            }
            "content_block_stop" if self.active.is_some() && index() == self.last_index => {
                self.active = None
            }
            "message_delta" if self.started && self.active.is_none() => {
                if !event["delta"]["stop_reason"].is_null() {
                    if self.reason.is_some() {
                        return Err(self.malformed());
                    }
                    self.set_reason(&event["delta"]["stop_reason"])?;
                }
                if self.merge_usage(&event["usage"]) {
                    self.report_usage(UsageFinality::Cumulative, sink).await;
                }
            }
            "message_stop" if self.reason.is_some() && self.active.is_none() => {
                self.report_usage(UsageFinality::Final, sink).await;
                return Ok(Event::Stop);
            }
            "content_block_start"
            | "content_block_delta"
            | "content_block_stop"
            | "message_delta"
            | "message_stop" => return Err(self.malformed()),
            "error" => return Err(self.error(&event["error"])),
            _ => {}
        }
        Ok(Event::Ignore)
    }

    pub(crate) async fn json(
        &mut self,
        message: &Value,
        sink: &mut dyn ProviderOutputSink,
    ) -> Result<(), ProviderExecutionError> {
        if message["type"] == "error" {
            return Err(self.error(&message["error"]));
        }
        if message["type"] != "message" {
            return Err(self.malformed());
        }
        for (index, block) in message["content"]
            .as_array()
            .ok_or_else(|| self.malformed())?
            .iter()
            .enumerate()
        {
            self.start_block(
                u32::try_from(index).map_err(|_| self.malformed())?,
                block,
                sink,
            )
            .await?;
            self.active = None;
        }
        self.set_reason(&message["stop_reason"])?;
        self.merge_usage(&message["usage"]);
        self.report_usage(UsageFinality::Final, sink).await;
        Ok(())
    }
}

pub(crate) fn complete_blocks(
    mut blocks: Vec<Value>,
    calls: &[CompletedToolCall],
    facts: ResponseFacts,
) -> Result<super::responses::ResponseOutput, ProviderExecutionError> {
    // Streaming delivers tool inputs as fragments. Only a complete object can be replayed.
    for (block, call) in blocks
        .iter_mut()
        .filter(|block| block["type"] == "tool_use")
        .zip(calls)
    {
        block["input"] = serde_json::from_str::<Value>(&call.arguments)
            .ok()
            .filter(Value::is_object)
            .ok_or_else(|| ProviderExecutionError::Incomplete {
                message: "Anthropic tool input was not a complete JSON object".to_owned(),
                facts: facts.clone(),
            })?;
    }
    Ok(super::responses::ResponseOutput {
        items: blocks,
        failure: None,
    })
}

pub(crate) fn assistant_content(blocks: &[Value]) -> Value {
    json!({"role":"assistant", "content":blocks})
}

pub(crate) fn replay(
    message: &Value,
    into: &mut Vec<ChatMessage>,
) -> Result<(), ProviderExecutionError> {
    let parts = message["content"]
        .as_array()
        .ok_or_else(|| build("missing recorded Anthropic content"))?;
    match message["role"].as_str() {
        Some("assistant") => {
            let mut calls = Vec::new();
            for block in parts.iter().filter(|part| part["type"] == "tool_use") {
                let (Some(id), Some(name), true) = (
                    block["id"].as_str(),
                    block["name"].as_str(),
                    block["input"].is_object(),
                ) else {
                    return Err(build("malformed recorded Anthropic tool call"));
                };
                calls.push(ChatToolCallWire {
                    id: id.to_owned(),
                    tool_type: Some("function".to_owned()),
                    function: ChatFunctionCallWire {
                        name: name.to_owned(),
                        arguments: block["input"].to_string(),
                    },
                });
            }
            let mut assistant = ChatMessage::assistant(None, calls);
            assistant.response_items = Some(parts.clone());
            into.push(assistant);
        }
        Some("user") => {
            for part in parts {
                let (Some(id), Some(text)) =
                    (part["tool_use_id"].as_str(), part["content"].as_str())
                else {
                    return Err(build("malformed recorded Anthropic tool result"));
                };
                if part["type"] != "tool_result" {
                    return Err(build("unexpected Anthropic replay block"));
                }
                into.push(ChatMessage::tool(id, text));
            }
        }
        _ => return Err(build("unexpected Anthropic replay role")),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ai::generative_model::execute_conversation;
    use crate::ai::providers::{
        EnvironmentCredentialResolver, ProviderExecutionConfig, ProviderExecutionResult,
    };
    use crate::db::entities::{
        ConfigId, ConfigRef, FrozenExecutionConfig, LlmConfig, LlmProviderId, LlmTransport,
        ProviderSnapshot,
    };
    use ntex::web::{self, App, HttpRequest, HttpResponse};

    fn model(endpoint: Option<String>) -> FrozenLlmConfig {
        FrozenLlmConfig {
            config: LlmConfig {
                model: "test-model".into(),
                context_window: Some(8192),
                parameters: Default::default(),
                declared_capabilities: Default::default(),
            },
            model: ConfigRef {
                id: ConfigId::new(1),
                edit_seq: 1,
            },
            provider: ProviderSnapshot {
                id: LlmProviderId::new(2),
                name: "local fixture".into(),
                edit_seq: 1,
                transport: LlmTransport::AnthropicMessages,
                endpoint,
                credential_ref: None,
                headers: Default::default(),
            },
        }
    }

    fn conversation() -> ChatConversation {
        ChatConversation {
            messages: vec![ChatMessage::user("question")],
            tools: vec![serde_json::from_value(json!({
                "type":"function", "function":{"name":"lookup","description":"Look up a value",
                    "parameters":{"type":"object","properties":{"key":{"type":"string"}},"required":["key"]}}
            })).unwrap()],
            ..Default::default()
        }
    }

    #[test]
    fn builds_alternating_messages_and_replays_signed_tool_round_trip() {
        let mut chat = conversation();
        chat.messages.insert(0, ChatMessage::system("system rules"));
        chat.messages
            .insert(1, ChatMessage::developer("developer rules"));
        chat.messages.push(ChatMessage::user("detail"));
        let blocks = vec![
            json!({"type":"thinking","thinking":"thought","signature":"opaque signed bytes"}),
            json!({"type":"tool_use","id":"call_1","name":"lookup","input":{"key":"abc"}}),
        ];
        replay(&assistant_content(&blocks), &mut chat.messages).unwrap();
        let result = json!({"type":"tool_result","tool_use_id":"call_1","content":"answer"});
        replay(
            &json!({"role":"user","content":[result]}),
            &mut chat.messages,
        )
        .unwrap();
        chat.messages.push(ChatMessage::user("continue"));
        let body = request_body(&model(None), &chat).unwrap();
        let text = |value: &str| json!({"type":"text","text":value});
        assert_eq!(body["model"], "test-model");
        assert_eq!(body["stream"], true);
        assert_eq!(body["max_tokens"], DEFAULT_MAX_TOKENS);
        assert_eq!(body["system"], "system rules\n\ndeveloper rules");
        assert_eq!(
            body["messages"],
            json!([
                {"role":"user","content":[text("question"),text("detail")]},
                {"role":"assistant","content":blocks},
                {"role":"user","content":[result,text("continue")]},
            ])
        );
        assert_eq!(
            body["tools"],
            json!([{
                "name":"lookup", "description":"Look up a value",
                "input_schema":chat.tools[0].function.parameters
            }])
        );
        let call = &chat.messages[4].tool_calls.as_ref().unwrap()[0];
        assert_eq!((&*call.id, &*call.function.name), ("call_1", "lookup"));
        assert_eq!(
            serde_json::from_str::<Value>(&call.function.arguments).unwrap(),
            json!({"key":"abc"})
        );
    }

    #[test]
    fn credential_debug_and_invalid_header_errors_redact_the_key() {
        let secret = "unit-test-private-value";
        let value = api_key(secret).unwrap();
        assert_eq!(value.to_str().unwrap(), secret);
        assert!(value.is_sensitive());
        assert!(!format!("{value:?}").contains(secret));
        let invalid = api_key(&format!("{secret}\ninvalid")).unwrap_err();
        assert!(!format!("{invalid:?} {invalid}").contains(secret));
    }

    async fn messages(request: HttpRequest) -> HttpResponse {
        let events = [
            json!({"type":"message_start","message":{"type":"message","usage":{"input_tokens":4,"cache_read_input_tokens":2,"cache_creation_input_tokens":1}}}),
            json!({"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"consider"}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"sig"}}),
            json!({"type":"content_block_stop","index":0}),
            json!({"type":"content_block_start","index":1,"content_block":{"type":"text","text":"Looking"}}),
            json!({"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":" up"}}),
            json!({"type":"content_block_stop","index":1}),
            json!({"type":"content_block_start","index":2,"content_block":{"type":"tool_use","id":"call_1","name":"lookup","input":{}}}),
            json!({"type":"content_block_delta","index":2,"delta":{"type":"input_json_delta","partial_json":"{\"key\":"}}),
            json!({"type":"content_block_delta","index":2,"delta":{"type":"input_json_delta","partial_json":"\"abc\"}"}}),
            json!({"type":"content_block_stop","index":2}),
            json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":3}}),
            json!({"type":"message_stop"}),
        ];
        let mut body = String::new();
        for event in events {
            if request.path() == "/before-stop/messages" && event["type"] == "message_stop" {
                break;
            }
            body.push_str(&format!("data: {event}\n\n"));
            if request.path() == "/partial-input/messages"
                && event["delta"]["partial_json"] == "{\"key\":"
            {
                break;
            }
        }
        HttpResponse::Ok()
            .content_type("text/event-stream")
            .body(body)
    }

    async fn exchange(endpoint: String) -> Result<ProviderExecutionResult, ProviderExecutionError> {
        let llm = model(Some(endpoint));
        let chat = conversation();
        let body = request_body(&llm, &chat).unwrap();
        execute_conversation(
            &FrozenExecutionConfig {
                agent: None,
                agent_ref: None,
                llm,
            },
            &chat,
            &body,
            &EnvironmentCredentialResolver,
            &ProviderExecutionConfig::default(),
            None,
        )
        .await
    }

    #[ntex::test]
    async fn sse_assembles_signed_blocks_tool_arguments_and_usage_only_after_message_stop() {
        let server = web::test::server(async || {
            App::new().service(web::resource("/{case}/messages").route(web::post().to(messages)))
        })
        .await;
        let result = exchange(server.url("/complete"))
            .await
            .unwrap()
            .admitted()
            .unwrap();
        assert_eq!(result.text, "Looking up");
        assert_eq!(result.finish_reason, Some(ChatFinishReason::ToolCalls));
        assert_eq!(result.raw_finish_reason.as_deref(), Some("tool_use"));
        assert_eq!(result.usage.input_tokens, Some(7));
        assert_eq!(result.usage.cache_read_tokens, Some(2));
        assert_eq!(result.usage.cache_write_tokens, Some(1));
        assert_eq!(result.usage.output_tokens, Some(3));
        assert_eq!(
            result.tool_calls,
            [CompletedToolCall {
                index: 0,
                id: "call_1".into(),
                name: "lookup".into(),
                arguments: r#"{"key":"abc"}"#.into()
            }]
        );
        assert_eq!(
            result.response_output.unwrap().items,
            [
                json!({"type":"thinking","thinking":"consider","signature":"sig"}),
                json!({"type":"text","text":"Looking up"}),
                json!({"type":"tool_use","id":"call_1","name":"lookup","input":{"key":"abc"}}),
            ]
        );
        for path in ["/partial-input", "/before-stop"] {
            let error = exchange(server.url(path)).await.unwrap_err();
            assert!(
                matches!(error, ProviderExecutionError::Unknown { ref message, finished:false, .. } if message.contains("before message_stop")),
                "{error:?}"
            );
        }
    }
}
