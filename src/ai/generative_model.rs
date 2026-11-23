use std::time::{Duration, Instant};
use std::{
    future::{Future, poll_fn},
    pin::{Pin, pin},
    task::Poll,
};

use ntex::http::{Payload, StatusCode};
use ntex::util::{Either, select};
use serde_json::{Value, json};

use super::providers::observe::{self, trim_ascii};
use super::providers::openai_compatible::{
    ChatConversation, ChatMessage, ContentPartWire, ImageInput, InvalidToolCall, ToolCallAssembler,
};
use super::providers::{
    CredentialResolver, MAX_SSE_LINE_BYTES, OutputSinkError, ProviderExecutionConfig,
    ProviderExecutionError, ProviderExecutionResult, ProviderFact, ProviderObservation,
    ProviderOutputSink, ResponseFacts, UsageCapture, announce, anthropic, endpoint,
    incomplete_tool_calls, openai_compatible, responses, unannounced_fact,
};
use crate::db::entities::{ChatFinishReason, NormalizedUsage, ProviderFailure};
use crate::db::entities::{
    FrozenExecutionConfig, FrozenLlmConfig, IntermediateOutputKind, LlmTransport,
};

const STREAM_OUTPUT_BUFFER_BYTES: usize = 4 * 1024;
const STREAM_OUTPUT_FLUSH_INTERVAL: Duration = Duration::from_millis(5);

pub(crate) const CHART_INSTRUCTIONS: &str = "For line, bar, pie, or combo charts, always use `ocg-chart` instead of Mermaid or `xychart-beta`; reserve Mermaid for other diagram types. \
     An `ocg-chart` fenced code block contains only JSON with this shape: \
     {\"type\":\"line|bar|pie|combo\",\"labels\":[\"label\"],\"series\":[{\"label\":\"series\",\"data\":[1],\"type\":\"line|bar\",\"axis\":\"left|right\"}],\"ariaLabel\":\"accessible description\"}. \
     Series `type` and `axis` are optional; pie requires exactly one series.";

pub(crate) fn ground_project(
    name: &str,
    root: &str,
    grant: Option<&str>,
    command_names: &[&str],
) -> ChatMessage {
    let allows = match grant {
        Some("code") => "read, edit and run commands",
        Some("edit") => "read and edit",
        _ => "read",
    };
    let commands = if grant == Some("code") {
        format!(" The commands you may run: {}.", command_names.join(", "))
    } else {
        String::new()
    };
    ChatMessage::system(format!(
        "You work in the Project {name:?}. Its Workspace's absolute root is {root}. Your grant allows: {allows}.{commands} Every tool path is relative to this root. \
         Paths outside this Workspace cannot be reached. If asked to act outside it, \
         say that you cannot reach that path instead of claiming you acted there."
    ))
}

#[must_use]
pub fn objective_conversation(
    frozen: &FrozenExecutionConfig,
    objective: Option<&str>,
) -> ChatConversation {
    let mut messages = Vec::new();
    if let Some(instructions) = frozen
        .agent
        .as_ref()
        .and_then(|agent| agent.instructions.as_deref())
        .filter(|instructions| !instructions.is_empty())
    {
        messages.push(
            if frozen.llm.provider.transport == LlmTransport::OpenAiResponses {
                ChatMessage::developer(instructions)
            } else {
                ChatMessage::system(instructions)
            },
        );
    }
    messages.push(ChatMessage::user(objective.unwrap_or("")));
    ChatConversation {
        messages,
        ..ChatConversation::default()
    }
}

#[must_use]
pub fn objective_conversation_with_images(
    frozen: &FrozenExecutionConfig,
    objective: Option<&str>,
    images: &[ImageInput],
) -> ChatConversation {
    let mut conversation = objective_conversation(frozen, objective);
    if images.is_empty() {
        return conversation;
    }
    let text = objective
        .filter(|objective| !objective.is_empty())
        .map(|text| ContentPartWire::Text {
            text: text.to_owned(),
        });
    let parts = text
        .into_iter()
        .chain(images.iter().map(ImageInput::part))
        .collect();
    if let Some(user) = conversation.messages.last_mut() {
        *user = ChatMessage::user_parts(parts);
    }
    conversation
}

pub(crate) fn request_body(
    frozen: &FrozenLlmConfig,
    conversation: &ChatConversation,
) -> Result<Value, ProviderExecutionError> {
    conversation
        .validate()
        .map_err(|error| ProviderExecutionError::Build {
            message: error.to_string(),
        })?;
    match frozen.provider.transport {
        LlmTransport::OpenAiResponses => return responses::request_body(frozen, conversation),
        LlmTransport::OpenAiChatCompletions => {}
        LlmTransport::AnthropicMessages => return anthropic::request_body(frozen, conversation),
    }
    let model = &frozen.config.model;
    let parameters = &frozen.config.parameters;
    let mut body =
        serde_json::to_value(conversation).map_err(|error| ProviderExecutionError::Build {
            message: format!("could not encode the chat request: {error}"),
        })?;
    let Some(object) = body.as_object_mut() else {
        return Err(ProviderExecutionError::Build {
            message: "the chat request did not encode as a JSON object".to_owned(),
        });
    };
    object.insert("model".to_owned(), json!(model));
    object.insert("stream".to_owned(), json!(true));
    object.insert("stream_options".to_owned(), json!({"include_usage": true}));
    if let Some(value) = parameters.temperature {
        object.insert("temperature".to_owned(), json!(value));
    }
    if let Some(value) = parameters.top_p {
        object.insert("top_p".to_owned(), json!(value));
    }
    if let Some(value) = parameters.max_tokens {
        object.insert("max_tokens".to_owned(), json!(value));
    }
    if let Some(value) = &parameters.reasoning_effort {
        openai_compatible::request::parse_reasoning_effort(value).map_err(|error| {
            ProviderExecutionError::Build {
                message: error.to_string(),
            }
        })?;
        object.insert("reasoning_effort".to_owned(), json!(value));
    }
    if let Some(value) = parameters.max_completion_tokens {
        object.insert("max_completion_tokens".to_owned(), json!(value));
    }
    if let Some(value) = parameters.presence_penalty {
        object.insert("presence_penalty".to_owned(), json!(value));
    }
    if let Some(value) = parameters.frequency_penalty {
        object.insert("frequency_penalty".to_owned(), json!(value));
    }
    Ok(body)
}

pub(crate) async fn execute_conversation(
    frozen: &FrozenExecutionConfig,
    conversation: &ChatConversation,
    body: &Value,
    resolver: &dyn CredentialResolver,
    config: &ProviderExecutionConfig,
    mut sink: Option<&mut dyn ProviderOutputSink>,
) -> Result<ProviderExecutionResult, ProviderExecutionError> {
    let mut announced = false;
    let result = if let Some(reason) = frozen.llm.provider.transport.execution_refusal() {
        Err(ProviderExecutionError::NotDelivered {
            message: reason.to_owned(),
        })
    } else {
        let config = config.for_profile(frozen);
        exchange(
            frozen,
            conversation,
            body,
            resolver,
            &config,
            &mut sink,
            &mut announced,
        )
        .await
    };
    if let Err(error) = &result
        && !announced
    {
        announce(borrow_output_sink(&mut sink), unannounced_fact(error)).await;
    }
    result
}

fn borrow_output_sink<'a>(
    sink: &'a mut Option<&mut dyn ProviderOutputSink>,
) -> Option<&'a mut dyn ProviderOutputSink> {
    match sink {
        Some(sink) => Some(&mut **sink),
        None => None,
    }
}

async fn exchange(
    frozen: &FrozenExecutionConfig,
    conversation: &ChatConversation,
    body: &Value,
    resolver: &dyn CredentialResolver,
    config: &ProviderExecutionConfig,
    sink: &mut Option<&mut dyn ProviderOutputSink>,
    announced: &mut bool,
) -> Result<ProviderExecutionResult, ProviderExecutionError> {
    let provider = &frozen.llm.provider;
    let (base, resource) = match provider.transport {
        LlmTransport::OpenAiChatCompletions => {
            (openai_compatible::DEFAULT_ENDPOINT, "chat/completions")
        }
        LlmTransport::OpenAiResponses => (openai_compatible::DEFAULT_ENDPOINT, "responses"),
        LlmTransport::AnthropicMessages => (anthropic::DEFAULT_ENDPOINT, "messages"),
    };
    let endpoint = endpoint(provider.endpoint.as_deref().unwrap_or(base), resource)?;
    let (response, started, redactor) = super::providers::send(
        endpoint,
        (provider.credential_ref.as_deref(), &provider.headers),
        resolver,
        config,
        body,
        borrow_output_sink(sink),
        Some(provider.transport),
    )
    .await?;
    *announced = true;
    announce(borrow_output_sink(sink), ProviderFact::Delivered).await;
    let status = response.status().as_u16();
    let request_id = if provider.transport == LlmTransport::AnthropicMessages {
        response
            .header("request-id")
            .and_then(|value| value.to_str().ok())
            .map(|id| redactor.text(id))
            .or_else(|| redactor.request_id(&response))
    } else {
        redactor.request_id(&response)
    };
    if response.status() != StatusCode::OK {
        return Err(observe::reject_response(
            response,
            request_id,
            config,
            &redactor,
            conversation.carries_images(),
        )
        .await);
    }

    let json = response
        .header("content-type")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|mime| {
            let mime = mime.trim().to_ascii_lowercase();
            mime == "application/json" || mime.ends_with("+json")
        });
    let format = match provider.transport {
        LlmTransport::OpenAiChatCompletions => ResponseFormat::ChatSse,
        LlmTransport::OpenAiResponses if json => ResponseFormat::ResponsesJson,
        LlmTransport::OpenAiResponses => ResponseFormat::ResponsesSse,
        LlmTransport::AnthropicMessages if json => ResponseFormat::AnthropicJson,
        LlmTransport::AnthropicMessages => ResponseFormat::AnthropicSse,
    };
    read_response(
        (response.take_payload(), format),
        status,
        started,
        request_id,
        config,
        borrow_output_sink(sink),
        conversation,
    )
    .await
    .map(|mut result| {
        if provider.transport != LlmTransport::OpenAiChatCompletions {
            result.raw_finish_reason = result
                .raw_finish_reason
                .as_deref()
                .map(|reason| redactor.text(reason));
        }
        if let Some(output) = &mut result.response_output
            && let Some(mut failure) = output.failure.take()
        {
            if conversation.carries_images() {
                failure.message = "provider terminated the image request".to_owned();
            }
            output.failure = Some(redactor.failure(failure));
        }
        result
    })
    .map_err(|error| redactor.error(error))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ResponseFormat {
    ChatSse,
    ResponsesSse,
    ResponsesJson,
    AnthropicSse,
    AnthropicJson,
}

async fn read_response(
    input: (Payload, ResponseFormat),
    status_code: u16,
    started: Instant,
    request_id: Option<String>,
    config: &ProviderExecutionConfig,
    sink: Option<&mut dyn ProviderOutputSink>,
    conversation: &ChatConversation,
) -> Result<ProviderExecutionResult, ProviderExecutionError> {
    let mut capture = UsageCapture::default();
    let mut result = parse_stream(
        input,
        status_code,
        started,
        request_id,
        config,
        sink,
        &mut capture,
    )
    .await
    .map_err(|error| {
        if !conversation.carries_images() {
            return error;
        }
        fn redact_image_error(error: ProviderExecutionError) -> ProviderExecutionError {
            match error {
                ProviderExecutionError::StreamFailure {
                    mut failure,
                    facts,
                    finished,
                } => {
                    failure.message =
                        "provider failed while processing an image request".to_owned();
                    failure.response_body = None;
                    ProviderExecutionError::StreamFailure {
                        failure,
                        facts,
                        finished,
                    }
                }
                ProviderExecutionError::Rejected { failure } => ProviderExecutionError::Rejected {
                    failure: ProviderFailure {
                        message: "provider rejected image request".to_owned(),
                        response_body: None,
                        ..failure
                    },
                },
                ProviderExecutionError::OutputPersistence { message, stream } => {
                    ProviderExecutionError::OutputPersistence {
                        message,
                        stream: Box::new(redact_image_error(*stream)),
                    }
                }
                other @ (ProviderExecutionError::Build { .. }
                | ProviderExecutionError::NotDelivered { .. }
                | ProviderExecutionError::Unknown { .. }
                | ProviderExecutionError::Incomplete { .. }
                | ProviderExecutionError::Interrupted { .. }
                | ProviderExecutionError::Stopped { .. }
                | ProviderExecutionError::CompletionUnpersisted { .. }) => other,
            }
        }
        redact_image_error(error)
    })?;
    let assembled = std::mem::take(&mut result.refused);
    let refused = conversation
        .admit_tool_calls(&result.tool_calls, assembled)
        .map_err(|error| {
            incomplete_tool_calls(
                error,
                ResponseFacts {
                    status_code: result.status_code,
                    finish_reason: result.finish_reason,
                    raw_finish_reason: result.raw_finish_reason.clone(),
                },
            )
        })?;
    result.refused = refused;
    Ok(result)
}

async fn parse_stream(
    input: (Payload, ResponseFormat),
    status_code: u16,
    started: Instant,
    request_id: Option<String>,
    config: &ProviderExecutionConfig,
    sink: Option<&mut dyn ProviderOutputSink>,
    capture: &mut UsageCapture,
) -> Result<ProviderExecutionResult, ProviderExecutionError> {
    let mut output = StreamOutput::new(sink);
    let result = parse_stream_body(
        input,
        status_code,
        started,
        request_id,
        config,
        capture,
        &mut output,
    )
    .await;
    capture.finish(&mut output, false).await;
    let flushed = output.flush().await;
    match (result, flushed) {
        (result, Ok(())) => result,
        (Ok(result), Err(error)) => Err(ProviderExecutionError::CompletionUnpersisted {
            message: error.message,
            result: Box::new(result),
        }),
        (Err(stream), Err(error)) => Err(ProviderExecutionError::OutputPersistence {
            message: error.message,
            stream: Box::new(stream),
        }),
    }
}

struct StreamOutput<'a> {
    sink: Option<&'a mut dyn ProviderOutputSink>,
    text: String,
    kind: IntermediateOutputKind,
    part: (u32, u32),
    started: Option<Instant>,
}

impl<'a> StreamOutput<'a> {
    fn new(sink: Option<&'a mut dyn ProviderOutputSink>) -> Self {
        Self {
            sink,
            text: String::with_capacity(STREAM_OUTPUT_BUFFER_BYTES),
            kind: IntermediateOutputKind::Progress,
            part: (0, 0),
            started: None,
        }
    }

    fn is_pending(&self) -> bool {
        !self.text.is_empty()
    }

    fn flush_delay(&self) -> Duration {
        self.started
            .map_or(STREAM_OUTPUT_FLUSH_INTERVAL, |started| {
                STREAM_OUTPUT_FLUSH_INTERVAL.saturating_sub(started.elapsed())
            })
    }

    async fn buffer(
        &mut self,
        kind: IntermediateOutputKind,
        part: (u32, u32),
        mut delta: &str,
    ) -> Result<(), OutputSinkError> {
        if delta.is_empty() || self.sink.is_none() {
            return Ok(());
        }
        if self.kind != kind || self.part != part {
            self.flush().await?;
            self.kind = kind;
            self.part = part;
        }
        while !delta.is_empty() {
            if self.text.is_empty() {
                self.started = Some(Instant::now());
            }
            let available = STREAM_OUTPUT_BUFFER_BYTES - self.text.len();
            let split = floor_char_boundary(delta, available);
            if split == 0 {
                self.flush().await?;
                continue;
            }
            self.text.push_str(&delta[..split]);
            delta = &delta[split..];
            if self.text.len() == STREAM_OUTPUT_BUFFER_BYTES || !delta.is_empty() {
                self.flush().await?;
            }
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), OutputSinkError> {
        if self.text.is_empty() {
            return Ok(());
        }
        self.started = None;
        struct ClearOnDrop<'a>(&'a mut String);
        impl Drop for ClearOnDrop<'_> {
            fn drop(&mut self) {
                self.0.clear();
            }
        }
        let text = ClearOnDrop(&mut self.text);
        match self.sink.as_deref_mut() {
            Some(sink) => sink.append(self.kind, self.part, text.0.as_str()).await,
            None => Ok(()),
        }
    }
}

impl ProviderOutputSink for StreamOutput<'_> {
    fn append<'a>(
        &'a mut self,
        kind: IntermediateOutputKind,
        part: (u32, u32),
        text: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), OutputSinkError>> + 'a>> {
        Box::pin(async move { self.buffer(kind, part, text).await })
    }

    fn observe<'a>(
        &'a mut self,
        observation: ProviderObservation,
    ) -> Pin<Box<dyn Future<Output = ()> + 'a>> {
        Box::pin(async move {
            if let Some(sink) = self.sink.as_deref_mut() {
                sink.observe(observation).await;
            }
        })
    }
}

fn floor_char_boundary(text: &str, maximum: usize) -> usize {
    let mut split = text.len().min(maximum);
    while split > 0 && !text.is_char_boundary(split) {
        split -= 1;
    }
    split
}

enum StreamWait {
    Cancelled,
    Flush,
    Idle,
    Chunk(Option<Result<ntex::util::Bytes, ntex::http::error::PayloadError>>),
}

async fn next_stream_event(
    payload: &mut Payload,
    config: &ProviderExecutionConfig,
    flush_delay: Option<Duration>,
    idle_remaining: Duration,
) -> StreamWait {
    let chunk = ntex::time::timeout(idle_remaining, payload.recv());
    if let Some(delay) = flush_delay {
        match select(
            config.cancellation.cancelled(),
            select(ntex::time::sleep(delay), chunk),
        )
        .await
        {
            Either::Left(()) => StreamWait::Cancelled,
            Either::Right(Either::Left(())) => StreamWait::Flush,
            Either::Right(Either::Right(Ok(chunk))) => StreamWait::Chunk(chunk),
            Either::Right(Either::Right(Err(()))) => StreamWait::Idle,
        }
    } else {
        match select(config.cancellation.cancelled(), chunk).await {
            Either::Left(()) => StreamWait::Cancelled,
            Either::Right(Ok(chunk)) => StreamWait::Chunk(chunk),
            Either::Right(Err(())) => StreamWait::Idle,
        }
    }
}

async fn parse_stream_body(
    (mut payload, format): (Payload, ResponseFormat),
    status_code: u16,
    started: Instant,
    request_id: Option<String>,
    config: &ProviderExecutionConfig,
    capture: &mut UsageCapture,
    output: &mut StreamOutput<'_>,
) -> Result<ProviderExecutionResult, ProviderExecutionError> {
    let mut buffer = Vec::with_capacity(MAX_SSE_LINE_BYTES);
    let mut frame = Vec::new();
    let mut responses_stream = responses::Stream::default();
    let mut anthropic_stream =
        anthropic::Stream::new(status_code, request_id.as_deref(), config.max_output_bytes);
    let wire_limit = if format == ResponseFormat::ChatSse {
        MAX_SSE_LINE_BYTES
    } else {
        config
            .max_output_bytes
            .saturating_mul(2)
            .saturating_add(2 * 1024 * 1024)
    };
    let mut terminal = None;
    let mut response = StreamedResponse::default();
    let mut saw_event = false;
    let mut first_byte = None;
    let mut idle_waited = Duration::ZERO;
    let mut saw_delta = false;

    'stream: loop {
        let remaining = if !saw_delta {
            config
                .first_delta_timeout
                .map(|timeout| timeout.saturating_sub(started.elapsed()))
        } else {
            None
        }
        .unwrap_or_else(|| config.timeout.saturating_sub(idle_waited));
        if remaining.is_zero() {
            return Err(response.unknown(
                "provider stream was idle after it may have been delivered",
                status_code,
            ));
        }
        let wait_started = Instant::now();
        let flush_delay = output.is_pending().then(|| output.flush_delay());
        let event = {
            let mut next = pin!(next_stream_event(
                &mut payload,
                config,
                flush_delay,
                remaining
            ));
            match poll_fn(|cx| Poll::Ready(next.as_mut().poll(cx))).await {
                Poll::Ready(event) => event,
                Poll::Pending => {
                    capture.checkpoint(output).await;
                    next.await
                }
            }
        };
        let chunk = match event {
            StreamWait::Cancelled => {
                return Err(local_stop(
                    "provider execution was cancelled while streaming",
                    status_code,
                    response.finish_reason,
                    response.raw_finish_reason.as_deref(),
                ));
            }
            StreamWait::Flush => {
                idle_waited = idle_waited.saturating_add(wait_started.elapsed());
                output.flush().await.map_err(|error| {
                    local_stop(
                        &error.message,
                        status_code,
                        response.finish_reason,
                        response.raw_finish_reason.as_deref(),
                    )
                })?;
                continue;
            }
            StreamWait::Idle => {
                return Err(response.unknown(
                    "provider stream was idle after it may have been delivered",
                    status_code,
                ));
            }
            StreamWait::Chunk(None) => break,
            StreamWait::Chunk(Some(chunk)) => {
                idle_waited = Duration::ZERO;
                chunk
            }
        };
        if config.cancellation.is_cancelled() {
            return Err(local_stop(
                "provider execution was cancelled while streaming",
                status_code,
                response.finish_reason,
                response.raw_finish_reason.as_deref(),
            ));
        }
        let chunk = chunk.map_err(|error| response.unknown(error.to_string(), status_code))?;
        first_byte.get_or_insert_with(Instant::now);
        if matches!(
            format,
            ResponseFormat::ResponsesJson | ResponseFormat::AnthropicJson
        ) {
            if buffer.len().saturating_add(chunk.len()) > wire_limit {
                return Err(
                    response.incomplete("provider JSON exceeded its wire limit", status_code)
                );
            }
            buffer.extend_from_slice(&chunk);
            continue;
        }
        for byte in chunk {
            if buffer.len() == wire_limit {
                return Err(
                    response.incomplete("provider SSE line exceeded its limit", status_code)
                );
            }
            buffer.push(byte);
            if byte != b'\n' {
                continue;
            }
            let line = buffer.strip_suffix(b"\n").unwrap_or(&buffer);
            let line = line.strip_suffix(b"\r").unwrap_or(line);
            if matches!(
                format,
                ResponseFormat::ResponsesSse | ResponseFormat::AnthropicSse
            ) {
                if let Some(data) = line.strip_prefix(b"data:") {
                    if frame.len().saturating_add(data.len()).saturating_add(1) > wire_limit {
                        return Err(response.incomplete(
                            "provider SSE event exceeded its wire limit",
                            status_code,
                        ));
                    }
                    frame.extend_from_slice(data.strip_prefix(b" ").unwrap_or(data));
                    frame.push(b'\n');
                } else if line.is_empty() && !frame.is_empty() {
                    let data = trim_ascii(&frame);
                    if data == b"[DONE]" {
                        break 'stream;
                    }
                    let event = serde_json::from_slice(data).map_err(|_| {
                        response.incomplete("malformed provider SSE event", status_code)
                    })?;
                    frame.clear();
                    if format == ResponseFormat::AnthropicSse {
                        match anthropic_stream.event(&event, output).await? {
                            anthropic::Event::Ignore => {}
                            anthropic::Event::Progress => saw_delta = true,
                            anthropic::Event::Stop => {
                                saw_event = true;
                                buffer.clear();
                                break 'stream;
                            }
                        }
                    } else {
                        match responses_stream.event(
                            event,
                            status_code,
                            request_id.as_deref(),
                            config.max_output_bytes,
                        )? {
                            responses::Event::Terminal(value) => {
                                terminal = Some(value);
                                break 'stream;
                            }
                            responses::Event::Progress => saw_delta = true,
                            responses::Event::Delta(kind, part, delta) => {
                                saw_delta = true;
                                output.buffer(kind, part, &delta).await.map_err(|error| {
                                    local_stop(&error.message, status_code, None, None)
                                })?;
                            }
                            responses::Event::Ignore => {}
                        }
                    }
                }
                buffer.clear();
                continue;
            }
            if line.len() > MAX_SSE_LINE_BYTES {
                return Err(
                    response.incomplete("provider SSE line exceeded its limit", status_code)
                );
            }
            let Some(data) = line.strip_prefix(b"data:") else {
                buffer.clear();
                continue;
            };
            let data = trim_ascii(data);
            if data == b"[DONE]" {
                buffer.clear();
                break 'stream;
            }
            let event: Value = serde_json::from_slice(data).map_err(|error| {
                response.incomplete(
                    format!("malformed provider SSE event: {error}"),
                    status_code,
                )
            })?;
            buffer.clear();
            saw_event = true;
            capture.observe(&event, &mut response.usage, output).await;
            if let Some(error) = event.get("error").filter(|error| error.is_object()) {
                let (message, provider_code) = observe::parse_provider_error(&event.to_string())
                    .unwrap_or_else(|| ("provider request failed".to_owned(), None));
                let rejected_status = ["status", "status_code", "code"]
                    .iter()
                    .filter_map(|key| error.get(key))
                    .filter_map(|status| status.as_u64().or_else(|| status.as_str()?.parse().ok()))
                    .find(|status| (400..500).contains(status));
                return Err(match rejected_status {
                    Some(status) => ProviderExecutionError::Rejected {
                        failure: ProviderFailure {
                            message,
                            status_code: Some(status as u16),
                            provider_code,
                            request_id: request_id.clone(),
                            ..ProviderFailure::default()
                        },
                    },
                    None => ProviderExecutionError::StreamFailure {
                        failure: Box::new(ProviderFailure {
                            message,
                            provider_code,
                            request_id: request_id.clone(),
                            ..ProviderFailure::default()
                        }),
                        facts: response.facts(status_code),
                        finished: response.finish_reason.is_some()
                            && response.tool_calls.is_empty(),
                    },
                });
            }
            let delta = consume_event(&event, &mut response, config.max_output_bytes).map_err(
                |refused| match refused {
                    Refused::ToolCall(error) => {
                        incomplete_tool_calls(error, response.facts(status_code))
                    }
                    Refused::OutputLimit => local_stop(
                        "provider output exceeded its limit",
                        status_code,
                        response.finish_reason,
                        response.raw_finish_reason.as_deref(),
                    ),
                },
            )?;
            if event
                .get("choices")
                .and_then(Value::as_array)
                .and_then(|choices| choices.first())
                .and_then(|choice| choice.get("delta"))
                .is_some_and(|delta| {
                    ["content", "reasoning_content", "reasoning"]
                        .iter()
                        .any(|key| {
                            delta
                                .get(key)
                                .and_then(Value::as_str)
                                .is_some_and(|text| !text.is_empty())
                        })
                        || delta
                            .get("tool_calls")
                            .and_then(Value::as_array)
                            .is_some_and(|calls| !calls.is_empty())
                })
            {
                saw_delta = true;
            }
            for (kind, delta) in delta {
                output.buffer(kind, (0, 0), &delta).await.map_err(|error| {
                    local_stop(
                        &error.message,
                        status_code,
                        response.finish_reason,
                        response.raw_finish_reason.as_deref(),
                    )
                })?;
            }
        }
    }
    let mut response_output = None;
    let mut anthropic_blocks = None;
    let terminal = match format {
        ResponseFormat::ResponsesJson => Some(serde_json::from_slice(&buffer).map_err(|_| {
            response.incomplete("malformed or incomplete Responses JSON", status_code)
        })?),
        ResponseFormat::ResponsesSse => Some(terminal.ok_or_else(|| {
            response.unknown(
                "Responses stream ended before a terminal event",
                status_code,
            )
        })?),
        ResponseFormat::AnthropicJson | ResponseFormat::AnthropicSse => {
            if format == ResponseFormat::AnthropicJson {
                let message = serde_json::from_slice(&buffer).map_err(|_| {
                    response.incomplete("malformed or incomplete Anthropic JSON", status_code)
                })?;
                anthropic_stream.json(&message, output).await?;
                buffer.clear();
            } else if !saw_event {
                return Err(
                    response.unknown("Anthropic stream ended before message_stop", status_code)
                );
            }
            let (finish, raw) = anthropic_stream
                .reason
                .ok_or_else(|| response.incomplete("missing Anthropic stop reason", status_code))?;
            response.finish_reason = Some(finish);
            response.raw_finish_reason = Some(raw);
            response.text = anthropic_stream.text;
            response.tool_calls = anthropic_stream.calls;
            anthropic_blocks = Some(anthropic_stream.blocks);
            response.usage = anthropic_stream.usage;
            saw_event = true;
            None
        }
        ResponseFormat::ChatSse => None,
    };
    if let Some(terminal) = terminal {
        response.usage = UsageCapture::responses_terminal(&terminal, output).await;
        let terminal = responses_stream.terminal(
            terminal,
            status_code,
            request_id.as_deref(),
            config.max_output_bytes,
        )?;
        response.text = terminal.text;
        response.tool_calls = terminal.calls;
        response.finish_reason = Some(terminal.finish);
        response.raw_finish_reason = Some(terminal.detail);
        response_output = Some(terminal.output);
        for (kind, part, delta) in terminal.remaining {
            output.buffer(kind, part, &delta).await.map_err(|error| {
                local_stop(
                    &error.message,
                    status_code,
                    response.finish_reason,
                    response.raw_finish_reason.as_deref(),
                )
            })?;
        }
        buffer.clear();
        saw_event = true;
    }
    let cut_short = !buffer.is_empty() || !saw_event || response.finish_reason.is_none();
    if cut_short && config.cancellation.is_cancelled() {
        return Err(local_stop(
            "provider execution was cancelled before the stream finished",
            status_code,
            response.finish_reason,
            response.raw_finish_reason.as_deref(),
        ));
    }
    if !buffer.is_empty() {
        return Err(response.incomplete(
            "provider stream ended with an incomplete SSE frame",
            status_code,
        ));
    }
    if !saw_event || response.finish_reason.is_none() {
        return Err(
            response.incomplete("provider stream ended without a finish event", status_code)
        );
    }
    capture.finish(output, true).await;
    let (tool_calls, refused) =
        response
            .tool_calls
            .finish(response.finish_reason)
            .map_err(|error| {
                let facts = ResponseFacts {
                    status_code,
                    finish_reason: response.finish_reason,
                    raw_finish_reason: response.raw_finish_reason.clone(),
                };
                incomplete_tool_calls(error, facts)
            })?;
    if let Some(blocks) = anthropic_blocks {
        response_output = Some(anthropic::complete_blocks(
            blocks,
            &tool_calls,
            ResponseFacts {
                status_code,
                finish_reason: response.finish_reason,
                raw_finish_reason: response.raw_finish_reason.clone(),
            },
        )?);
    }
    let finished_in = started.elapsed();
    Ok(ProviderExecutionResult {
        text: response.text,
        tool_calls,
        refused,
        usage: response.usage,
        finish_reason: response.finish_reason,
        raw_finish_reason: response.raw_finish_reason,
        status_code,
        request_id,
        ttfb: first_byte.map_or(finished_in, |first| first.duration_since(started)),
        finished_in,
        response_output,
    })
}

fn local_stop(
    message: &str,
    status_code: u16,
    finish_reason: Option<ChatFinishReason>,
    raw_finish_reason: Option<&str>,
) -> ProviderExecutionError {
    ProviderExecutionError::Stopped {
        message: message.to_owned(),
        facts: ResponseFacts {
            status_code,
            finish_reason,
            raw_finish_reason: raw_finish_reason.map(str::to_owned),
        },
    }
}

#[derive(Default)]
struct StreamedResponse {
    text: String,
    reasoning_bytes: usize,
    tool_calls: ToolCallAssembler,
    usage: NormalizedUsage,
    finish_reason: Option<ChatFinishReason>,
    raw_finish_reason: Option<String>,
}

impl StreamedResponse {
    fn output_bytes(&self) -> usize {
        self.text
            .len()
            .saturating_add(self.reasoning_bytes)
            .saturating_add(self.tool_calls.bytes())
    }

    fn facts(&self, status_code: u16) -> ResponseFacts {
        ResponseFacts {
            status_code,
            finish_reason: self.finish_reason,
            raw_finish_reason: self.raw_finish_reason.clone(),
        }
    }

    fn incomplete(&self, message: impl Into<String>, status_code: u16) -> ProviderExecutionError {
        ProviderExecutionError::Incomplete {
            message: message.into(),
            facts: self.facts(status_code),
        }
    }

    fn unknown(&self, message: impl Into<String>, status_code: u16) -> ProviderExecutionError {
        ProviderExecutionError::Unknown {
            message: message.into(),
            facts: Some(self.facts(status_code)),
            finished: self.finish_reason.is_some() && self.tool_calls.is_empty(),
        }
    }
}

enum Refused {
    ToolCall(InvalidToolCall),
    OutputLimit,
}

fn consume_event(
    event: &Value,
    response: &mut StreamedResponse,
    max_output_bytes: usize,
) -> Result<Vec<(IntermediateOutputKind, String)>, Refused> {
    let Some(choice) = event
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|choices| choices.first())
    else {
        return Ok(Vec::new());
    };
    let mut deltas = Vec::new();
    if let Some(delta) = choice.get("delta") {
        if let Some(deltas) = delta.get("tool_calls") {
            response
                .tool_calls
                .apply(deltas)
                .map_err(Refused::ToolCall)?;
            if response.output_bytes() > max_output_bytes {
                return Err(Refused::OutputLimit);
            }
        }
        if let Some(value) = delta
            .get("reasoning_content")
            .and_then(Value::as_str)
            .or_else(|| delta.get("reasoning").and_then(Value::as_str))
        {
            if response.output_bytes().saturating_add(value.len()) > max_output_bytes {
                return Err(Refused::OutputLimit);
            }
            response.reasoning_bytes += value.len();
            deltas.push((IntermediateOutputKind::Reasoning, value.to_owned()));
        }
        if let Some(value) = delta.get("content").and_then(Value::as_str) {
            if response.output_bytes().saturating_add(value.len()) > max_output_bytes {
                return Err(Refused::OutputLimit);
            }
            response.text.push_str(value);
            deltas.push((IntermediateOutputKind::Progress, value.to_owned()));
        }
    }
    if let Some(raw) = choice.get("finish_reason").and_then(Value::as_str)
        && let Some(reason) = openai_compatible::stream::parse_finish_reason(raw)
    {
        response.raw_finish_reason = Some(raw.to_owned());
        response.finish_reason = Some(reason);
    }
    Ok(deltas)
}
