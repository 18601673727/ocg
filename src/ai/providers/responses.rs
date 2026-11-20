use std::collections::BTreeMap;

use serde_json::{Value, json};

use super::openai_compatible::{
    ChatConversation, ChatMessage, ChatRole, ContentPartWire, MessageContentWire, ToolCallAssembler,
};
use super::{ProviderExecutionError, ProviderExecutionResult, ResponseFacts, openai_compatible};
use crate::db::entities::{
    ChatFinishReason, FinalResult, FrozenLlmConfig, IntermediateOutputKind, ModelCapability,
    ProviderFailure,
};

#[derive(Debug, Clone)]
pub(crate) struct ResponseOutput {
    pub(crate) items: Vec<Value>,
    pub(crate) failure: Option<ProviderFailure>,
}

pub(crate) fn terminal_failure(result: &ProviderExecutionResult) -> Option<FinalResult> {
    let failure = result.response_output.as_ref()?.failure.as_ref()?;
    Some(FinalResult {
        summary: Some(failure.message.clone()),
        code: Some("responses_terminal_failure".to_owned()),
        goal_reached: Some(false),
        value: Some(json!({
            "text": result.text,
            "finish_reason": result.raw_finish_reason,
            "provider_code": failure.provider_code,
            "request_id": failure.request_id,
        })),
    })
}

pub(crate) fn request_body(
    frozen: &FrozenLlmConfig,
    conversation: &ChatConversation,
) -> Result<Value, ProviderExecutionError> {
    let model = &frozen.config.model;
    let parameters = &frozen.config.parameters;
    let endpoint = frozen
        .provider
        .endpoint
        .as_deref()
        .unwrap_or(openai_compatible::DEFAULT_ENDPOINT);
    let direct_openai = super::provider_uri(endpoint).is_some_and(|uri| {
        uri.scheme_str() == Some("https")
            && matches!(
                uri.host().map(str::to_ascii_lowercase).as_deref(),
                Some("api.openai.com" | "us.api.openai.com" | "eu.api.openai.com")
            )
            && uri.port_u16().unwrap_or(443) == 443
    });
    let gpt_62 = ["gpt-6.2-astra", "gpt-6.2-sol"]
        .into_iter()
        .any(|family| matches_model(model, family));
    let cache_options = direct_openai
        && (gpt_62
            || [
                "gpt-5.6",
                "gpt-6-astra",
                "gpt-6-sol",
                "gpt-6-luna",
                "gpt-6.1-sol",
                "gpt-6.1-terra",
            ]
            .into_iter()
            .any(|family| matches_model(model, family)));

    let mut input = Vec::new();
    for message in &conversation.messages {
        if let Some(items) = &message.response_items {
            input.extend(items.iter().cloned());
        } else {
            input.push(input_message(message)?);
        }
    }
    let mut body = json!({"model": model, "input": input, "store": false, "stream": true});
    if !conversation.tools.is_empty() {
        body["tools"] = Value::Array(
            conversation
                .tools
                .iter()
                .map(|tool| {
                    let function = &tool.function;
                    let mut definition = json!({
                        "type": "function",
                        "name": function.name,
                        "strict": function.strict.unwrap_or(false),
                    });
                    if let Some(description) = &function.description {
                        definition["description"] = json!(description);
                    }
                    if let Some(parameters) = &function.parameters {
                        definition["parameters"] = parameters.clone();
                    }
                    definition
                })
                .collect(),
        );
    }
    if let Some(max) = parameters.max_completion_tokens.or(parameters.max_tokens) {
        body["max_output_tokens"] = json!(max);
    }
    if let Some(effort) = parameters.reasoning_effort.as_deref() {
        openai_compatible::request::parse_reasoning_effort(effort)
            .map_err(|error| build(error.to_string()))?;
        let effort = match (gpt_62, effort) {
            (true, "none" | "minimal") => "low",
            _ => effort,
        };
        body["reasoning"] = json!({"effort": effort});
    }
    if !gpt_62 {
        if let Some(value) = parameters.temperature {
            body["temperature"] = json!(value);
        }
        if let Some(value) = parameters.top_p {
            body["top_p"] = json!(value);
        }
    }
    if cache_options {
        body["prompt_cache_options"] = json!({"mode": "implicit", "ttl": "30m"});
        body["reasoning"]["context"] = json!("all_turns");
    }
    if gpt_62
        || cache_options
        || frozen
            .config
            .declared_capabilities
            .contains(&ModelCapability::Reasoning)
        || parameters
            .reasoning_effort
            .as_deref()
            .is_some_and(|effort| effort != "none")
        || (direct_openai && model.starts_with("gpt-5"))
    {
        body["include"] = json!(["reasoning.encrypted_content"]);
    }
    Ok(body)
}

fn matches_model(model: &str, family: &str) -> bool {
    model == family
        || model.strip_prefix(family).is_some_and(|suffix| {
            suffix.len() == 11
                && suffix.bytes().enumerate().all(|(index, byte)| {
                    if matches!(index, 0 | 5 | 8) {
                        byte == b'-'
                    } else {
                        byte.is_ascii_digit()
                    }
                })
        })
}

fn build(message: impl Into<String>) -> ProviderExecutionError {
    ProviderExecutionError::Build {
        message: message.into(),
    }
}

fn input_message(message: &ChatMessage) -> Result<Value, ProviderExecutionError> {
    if message.role == ChatRole::Tool {
        let (Some(call_id), Some(MessageContentWire::Text(text))) =
            (&message.tool_call_id, &message.content)
        else {
            return Err(build(
                "Responses function output requires a call ID and text",
            ));
        };
        return Ok(json!({"type": "function_call_output", "call_id": call_id, "output": text}));
    }
    let role = match message.role {
        ChatRole::System => "system",
        ChatRole::Developer => "developer",
        ChatRole::User => "user",
        ChatRole::Assistant => "assistant",
        ChatRole::Tool | ChatRole::Function => {
            return Err(build("unsupported Responses message role"));
        }
    };
    let content = match &message.content {
        Some(MessageContentWire::Text(text)) => json!(text),
        Some(MessageContentWire::Parts(parts)) => Value::Array(
            parts
                .iter()
                .map(|part| match part {
                    ContentPartWire::Text { text } => json!({"type": "input_text", "text": text}),
                    ContentPartWire::ImageUrl { image_url } => {
                        json!({"type": "input_image", "image_url": image_url.url, "detail": "auto"})
                    }
                })
                .collect(),
        ),
        None => return Err(build("Responses message has no content")),
    };
    Ok(json!({"role": role, "content": content}))
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum PartKind {
    Text,
    Refusal,
    Summary,
    Arguments,
}

type PartKey = (u32, PartKind, u32);

#[derive(Default)]
pub(crate) struct Stream {
    parts: BTreeMap<PartKey, String>,
    bytes: usize,
}

pub(crate) enum Event {
    Delta(IntermediateOutputKind, (u32, u32), String),
    Progress,
    Terminal(Value),
    Ignore,
}

type Delta = (IntermediateOutputKind, (u32, u32), String);

pub(crate) struct Terminal {
    pub(crate) output: ResponseOutput,
    pub(crate) text: String,
    pub(crate) remaining: Vec<Delta>,
    pub(crate) calls: ToolCallAssembler,
    pub(crate) finish: ChatFinishReason,
    pub(crate) detail: String,
}

impl Stream {
    pub(crate) fn event(
        &mut self,
        event: Value,
        status_code: u16,
        request_id: Option<&str>,
        limit: usize,
    ) -> Result<Event, ProviderExecutionError> {
        let kind = event
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| incomplete(status_code, "Responses SSE event has no type"))?;
        let part = match kind {
            "response.output_text.delta" => PartKind::Text,
            "response.refusal.delta" => PartKind::Refusal,
            "response.reasoning_summary_text.delta" => PartKind::Summary,
            "response.function_call_arguments.delta" => PartKind::Arguments,
            "response.completed"
            | "response.incomplete"
            | "response.failed"
            | "response.cancelled" => {
                let expected = kind.trim_start_matches("response.");
                let response = &event["response"];
                if response["status"].as_str() != Some(expected) {
                    return Err(incomplete(
                        status_code,
                        "Responses terminal event disagrees with its status",
                    ));
                }
                return Ok(Event::Terminal(response.clone()));
            }
            "error" => {
                return Err(ProviderExecutionError::StreamFailure {
                    failure: Box::new(failure(event.get("error").unwrap_or(&event), request_id)),
                    facts: facts(status_code),
                    finished: false,
                });
            }
            _ => return Ok(Event::Ignore),
        };
        let delta = event["delta"]
            .as_str()
            .ok_or_else(|| incomplete(status_code, "Responses delta has no text"))?;
        let index = |key: &str| {
            event[key]
                .as_u64()
                .and_then(|index| u32::try_from(index).ok())
                .ok_or_else(|| incomplete(status_code, "Responses delta has no valid index"))
        };
        let output_index = index("output_index")?;
        let part_index = match part {
            PartKind::Text | PartKind::Refusal => index("content_index")?,
            PartKind::Summary => index("summary_index")?,
            PartKind::Arguments => 0,
        };
        if delta.is_empty() {
            return Ok(Event::Ignore);
        }
        self.bytes = self.bytes.saturating_add(delta.len());
        if self.bytes > limit
            || self.parts.len() >= 4096
                && !self.parts.contains_key(&(output_index, part, part_index))
        {
            return Err(output_limit(status_code));
        }
        let buffered = self
            .parts
            .entry((output_index, part, part_index))
            .or_default();
        if part == PartKind::Arguments
            && buffered.len().saturating_add(delta.len())
                > openai_compatible::MAX_TOOL_ARGUMENTS_BYTES
        {
            return Err(output_limit(status_code));
        }
        buffered.push_str(delta);
        Ok(match output_kind(part) {
            Some(kind) => Event::Delta(kind, (output_index, part_index), delta.to_owned()),
            None => Event::Progress,
        })
    }

    pub(crate) fn terminal(
        mut self,
        mut response: Value,
        status_code: u16,
        request_id: Option<&str>,
        limit: usize,
    ) -> Result<Terminal, ProviderExecutionError> {
        let status = response["status"]
            .as_str()
            .ok_or_else(|| incomplete(status_code, "Responses response has no status"))?;
        let (mut finish, detail, failure) = match status {
            "completed" => (ChatFinishReason::Stop, None, None),
            "incomplete" => {
                let reason = response["incomplete_details"]["reason"]
                    .as_str()
                    .unwrap_or("incomplete");
                let finish = match reason {
                    "max_output_tokens" => ChatFinishReason::Length,
                    "content_filter" => ChatFinishReason::ContentFilter,
                    _ => ChatFinishReason::Other,
                };
                (
                    finish,
                    Some(reason.to_owned()),
                    Some(ProviderFailure {
                        message: "provider returned an incomplete response".to_owned(),
                        provider_code: Some(reason.to_owned()),
                        request_id: request_id.map(str::to_owned),
                        ..ProviderFailure::default()
                    }),
                )
            }
            "failed" | "cancelled" => {
                let failure = failure(&response["error"], request_id);
                (
                    ChatFinishReason::Error,
                    Some(status.to_owned()),
                    Some(failure),
                )
            }
            _ => {
                return Err(incomplete(
                    status_code,
                    "Responses response has no terminal status",
                ));
            }
        };
        let items = match response.get_mut("output").map(Value::take) {
            Some(Value::Array(items)) => items,
            None | Some(Value::Null) if failure.is_some() => Vec::new(),
            _ => {
                return Err(incomplete(
                    status_code,
                    "Responses response has no output array",
                ));
            }
        };
        if items.len() > 4096
            || crate::db::serialized_len(&items).map_or(true, |bytes| bytes > limit)
        {
            return Err(output_limit(status_code));
        }
        let mut calls = ToolCallAssembler::default();
        let mut text = String::new();
        let mut remaining = Vec::new();
        for (index, item) in (0u32..).zip(&items) {
            if failure.is_none() && !matches!(item["status"].as_str(), None | Some("completed")) {
                return Err(incomplete(
                    status_code,
                    "completed Responses response contains an unfinished item",
                ));
            }
            match item["type"].as_str() {
                Some("message") if item["role"] == "assistant" => {
                    let parts = item["content"].as_array().ok_or_else(|| {
                        incomplete(status_code, "Responses message has no content array")
                    })?;
                    for (part_index, part) in (0u32..).zip(parts) {
                        let (kind, field) = match part["type"].as_str() {
                            Some("output_text") => (PartKind::Text, "text"),
                            Some("refusal") => (PartKind::Refusal, "refusal"),
                            _ => {
                                return Err(incomplete(
                                    status_code,
                                    "unsupported Responses message content",
                                ));
                            }
                        };
                        let value = part[field].as_str().ok_or_else(|| {
                            incomplete(status_code, "Responses message part has no text")
                        })?;
                        self.reconcile(
                            (index, kind, part_index),
                            value,
                            status_code,
                            &mut remaining,
                        )?;
                        text.push_str(value);
                    }
                }
                Some("reasoning") => {
                    if let Some(parts) = item["summary"].as_array() {
                        for (part_index, part) in (0u32..).zip(parts) {
                            if let Some(value) = part["text"].as_str() {
                                self.reconcile(
                                    (index, PartKind::Summary, part_index),
                                    value,
                                    status_code,
                                    &mut remaining,
                                )?;
                            }
                        }
                    }
                }
                Some("function_call") if failure.is_none() => {
                    if !matches!(item["caller"]["type"].as_str(), None | Some("direct"))
                        || item["namespace"]
                            .as_str()
                            .is_some_and(|name| !name.is_empty())
                    {
                        return Err(incomplete(
                            status_code,
                            "unsupported Responses function call scope",
                        ));
                    }
                    let arguments = item["arguments"].as_str().ok_or_else(|| {
                        incomplete(status_code, "Responses function call has no arguments")
                    })?;
                    self.reconcile(
                        (index, PartKind::Arguments, 0),
                        arguments,
                        status_code,
                        &mut remaining,
                    )?;
                    calls
                        .apply(&json!([{
                            "index": index,
                            "type": "function",
                            "id": item["call_id"],
                        "function": {
                            "name": item["name"],
                            "arguments": (!arguments.is_empty()).then_some(arguments),
                        },
                        }]))
                        .map_err(|error| super::incomplete_tool_calls(error, facts(status_code)))?;
                }
                _ if failure.is_some() => {}
                _ => return Err(incomplete(status_code, "unsupported Responses output item")),
            }
        }
        if failure.is_none() {
            if !self.parts.is_empty() || items.is_empty() {
                return Err(incomplete(
                    status_code,
                    "Responses terminal output is missing streamed items",
                ));
            }
            if !calls.is_empty() {
                finish = ChatFinishReason::ToolCalls;
            }
        }
        Ok(Terminal {
            output: ResponseOutput { items, failure },
            text,
            remaining,
            calls,
            finish,
            detail: detail.unwrap_or_else(|| finish.as_str().to_owned()),
        })
    }

    fn reconcile(
        &mut self,
        key: PartKey,
        value: &str,
        status_code: u16,
        remaining: &mut Vec<Delta>,
    ) -> Result<(), ProviderExecutionError> {
        let prior = self.parts.remove(&key).unwrap_or_default();
        let tail = value.strip_prefix(&prior).ok_or_else(|| {
            incomplete(
                status_code,
                "Responses terminal output disagrees with streamed deltas",
            )
        })?;
        if !tail.is_empty()
            && let Some(kind) = output_kind(key.1)
        {
            remaining.push((kind, (key.0, key.2), tail.to_owned()));
        }
        Ok(())
    }
}

fn output_kind(part: PartKind) -> Option<IntermediateOutputKind> {
    match part {
        PartKind::Text | PartKind::Refusal => Some(IntermediateOutputKind::Progress),
        PartKind::Summary => Some(IntermediateOutputKind::Reasoning),
        PartKind::Arguments => None,
    }
}

fn failure(error: &Value, request_id: Option<&str>) -> ProviderFailure {
    ProviderFailure {
        message: error["message"]
            .as_str()
            .unwrap_or("provider terminated the Responses request")
            .to_owned(),
        provider_code: error["code"].as_str().map(str::to_owned),
        request_id: request_id.map(str::to_owned),
        ..ProviderFailure::default()
    }
}

fn facts(status_code: u16) -> ResponseFacts {
    ResponseFacts {
        status_code,
        finish_reason: None,
        raw_finish_reason: None,
    }
}

pub(crate) fn incomplete(status_code: u16, message: &str) -> ProviderExecutionError {
    ProviderExecutionError::Incomplete {
        message: message.to_owned(),
        facts: facts(status_code),
    }
}

fn output_limit(status_code: u16) -> ProviderExecutionError {
    ProviderExecutionError::Stopped {
        message: "provider output exceeded its limit".to_owned(),
        facts: facts(status_code),
    }
}
