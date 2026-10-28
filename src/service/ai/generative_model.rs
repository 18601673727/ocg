use std::time::{Duration, Instant};
use std::{future::Future, pin::Pin};

use ntex::http::header::{ACCEPT, CONTENT_TYPE};
use ntex::http::{Payload, StatusCode};
use ntex::util::{Either, select};
use serde_json::{Value, json};

use super::providers::observe::{self, Redactor, bounded_body, classify_client_error, trim_ascii};
use super::providers::openai_compatible::{
    ChatConversation, ChatFinishReason, ChatMessage, ContentPartWire, ImageInput, InvalidToolCall,
    NormalizedUsage, ProviderFailure, ToolCallAssembler,
};
use super::providers::usage::{self, reported_usage};
use super::providers::{
    CredentialResolver, MAX_PROVIDER_ERROR_BYTES, MAX_SSE_LINE_BYTES, OutputSinkError,
    ProviderExecutionConfig, ProviderExecutionError, ProviderExecutionResult, ProviderFact,
    ProviderObservation, ProviderOutputSink, ResponseFacts, UsageCapture, announce, anthropic,
    endpoint, incomplete_tool_calls, openai_compatible, rejected_failure, responses,
    unannounced_fact,
};
use crate::db::entities::{FrozenExecutionConfig, IntermediateOutputKind, LlmParameters};
use crate::system::http::{authorization, provider_client};

const STREAM_OUTPUT_BUFFER_BYTES: usize = 4 * 1024;
const STREAM_OUTPUT_FLUSH_INTERVAL: Duration = Duration::from_millis(250);

/// Tells the Provider which Project it works in, where its root is, what
/// its grant allows and, for a grant that runs commands, which command names
/// it may run.
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

/// The conversation a Job's objective is sent as: the frozen Agent
/// instructions, when there are any, then the objective. It offers no tools.
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
        messages.push(ChatMessage::system(instructions));
    }
    messages.push(ChatMessage::user(objective.unwrap_or("")));
    ChatConversation {
        messages,
        ..ChatConversation::default()
    }
}

/// [`objective_conversation`] with `images` in the objective's user message:
/// its text first, when there is any, then each image as an `image_url`
/// part, in order. Without images it is exactly [`objective_conversation`].
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

/// The request body: the conversation as given, with the frozen model and
/// parameters. Streaming with usage is always requested.
pub(crate) fn request_body(
    model: &str,
    parameters: &LlmParameters,
    conversation: &ChatConversation,
) -> Result<Value, ProviderExecutionError> {
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

/// Sends one validated conversation, with the tools it offers, to the
/// frozen Provider and model with the frozen parameters, and reads the
/// streamed response. The conversation is the whole message list sent.
/// Calls come back in wire order; a complete call this request did not
/// admit (a name not offered or not chosen, arguments not one JSON object
/// or refused by the schema) is in `tool_calls` as sent and in `refused`
/// with its reason, inside `Ok`. A caller with no tool loop applies
/// [`ProviderExecutionResult::admitted`], as `run_provider_job_inner` does
/// inside its retry loop when no tools are offered. A cut-off or malformed
/// stream and response-wide refusals are still `Err`.
/// No tool is ever run here.
pub async fn execute_conversation(
    frozen: &FrozenExecutionConfig,
    conversation: &ChatConversation,
    resolver: &dyn CredentialResolver,
    config: &ProviderExecutionConfig,
    mut sink: Option<&mut dyn ProviderOutputSink>,
) -> Result<ProviderExecutionResult, ProviderExecutionError> {
    let config = config.for_profile(frozen);
    let mut announced = false;
    let result = exchange(
        frozen,
        conversation,
        resolver,
        &config,
        &mut sink,
        &mut announced,
    )
    .await;
    // An error before any response head is classified by what it proves about
    // delivery; a response head has already announced the request.
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
    resolver: &dyn CredentialResolver,
    config: &ProviderExecutionConfig,
    sink: &mut Option<&mut dyn ProviderOutputSink>,
    announced: &mut bool,
) -> Result<ProviderExecutionResult, ProviderExecutionError> {
    conversation
        .validate()
        .map_err(|error| ProviderExecutionError::Build {
            message: error.to_string(),
        })?;
    let provider = &frozen.llm.provider;
    let anthropic = config.transport == crate::db::entities::LlmTransport::AnthropicMessages;
    let endpoint = endpoint(
        provider.endpoint.as_deref().unwrap_or(if anthropic {
            anthropic::DEFAULT_ENDPOINT
        } else {
            openai_compatible::DEFAULT_ENDPOINT
        }),
        match config.transport {
            crate::db::entities::LlmTransport::OpenAiChatCompletions => "chat/completions",
            crate::db::entities::LlmTransport::OpenAiResponses => "responses",
            crate::db::entities::LlmTransport::AnthropicMessages => "messages",
        },
    )?;

    let credential = resolver.resolve(provider.credential_ref.as_deref())?;
    let redactor = Redactor::new(credential.as_deref());
    let authorization = credential
        .as_deref()
        .map(if anthropic {
            anthropic::api_key
        } else {
            authorization
        })
        .transpose()?;
    if config.cancellation.is_cancelled() {
        return Err(ProviderExecutionError::NotDelivered {
            message: "provider execution was cancelled before sending".to_owned(),
        });
    }
    let chat_body = request_body(
        &frozen.llm.config.model,
        &frozen.llm.config.parameters,
        conversation,
    )?;
    let body = match config.transport {
        crate::db::entities::LlmTransport::OpenAiChatCompletions => chat_body,
        crate::db::entities::LlmTransport::AnthropicMessages => {
            anthropic::request_body(chat_body, conversation)?
        }
        crate::db::entities::LlmTransport::OpenAiResponses => {
            responses::request_body(chat_body, conversation)?
        }
    };
    let head_timeout = config.first_delta_timeout.unwrap_or(config.timeout);
    let client = provider_client(head_timeout).await?;
    let session = endpoint
        .parse::<ntex::http::Uri>()
        .ok()
        .filter(|uri| {
            uri.host()
                .is_some_and(|host| host.eq_ignore_ascii_case("opencode.ai"))
        })
        .and(config.session_id)
        .map(|id| format!("ocg-{}", id.get()));

    let request = client
        .post(endpoint)
        .set_header(CONTENT_TYPE, "application/json")
        .set_header(ACCEPT, "text/event-stream")
        .set_header("Cache-Control", "no-cache")
        .if_some(session, |session, request| {
            request.set_header("x-opencode-session", session)
        })
        .if_some(authorization, |authorization, request| {
            request.set_header(
                if anthropic {
                    "x-api-key"
                } else {
                    "authorization"
                },
                authorization,
            )
        });
    let request = if anthropic {
        request.set_header("anthropic-version", anthropic::API_VERSION)
    } else {
        request
    };

    if let Some(sink) = sink.as_deref_mut() {
        sink.sending(&body)
            .await
            .map_err(|error| ProviderExecutionError::NotDelivered {
                message: error.message,
            })?;
    }
    let started = Instant::now();
    let sent = select(
        config.cancellation.cancelled(),
        ntex::time::timeout(head_timeout, request.send_json(&body)),
    )
    .await;
    let response = match sent {
        // The request future is dropped here, with whatever it had sent.
        Either::Left(()) => {
            return Err(ProviderExecutionError::Interrupted {
                message: "provider execution was cancelled before a response arrived".to_owned(),
            });
        }
        Either::Right(sent) => sent,
    }
    .map_err(|_| ProviderExecutionError::Unknown {
        message: "provider request timed out after it may have been delivered".to_owned(),
        facts: None,
        finished: false,
    })?
    .map_err(classify_client_error)?;
    // A response head proves the request reached a Provider, whatever it says.
    *announced = true;
    announce(borrow_output_sink(sink), ProviderFact::Delivered).await;
    let status = response.status().as_u16();
    let request_id = response
        .header(if anthropic {
            "request-id"
        } else {
            "x-request-id"
        })
        .and_then(|value| value.to_str().ok())
        .map(|value| redactor.text(value));
    let retry_after_ms = response
        .header("retry-after")
        .and_then(|value| value.to_str().ok())
        .and_then(observe::retry_after_ms);
    if response.status() != StatusCode::OK {
        // Nothing of a refused image-bearing request's body is kept, so it is
        // not read; see `rejected_failure`.
        if conversation.carries_images() {
            return Err(ProviderExecutionError::Rejected {
                failure: observe::with_retry_after(
                    redactor.failure(rejected_failure(conversation, status, request_id, None)),
                    retry_after_ms,
                ),
            });
        }
        // A cancelled read keeps the status the Provider answered with.
        let body = match select(
            config.cancellation.cancelled(),
            ntex::time::timeout(
                config.timeout,
                bounded_body(response, MAX_PROVIDER_ERROR_BYTES),
            ),
        )
        .await
        {
            Either::Left(()) => None,
            Either::Right(Ok(body)) => body,
            Either::Right(Err(())) => None,
        };
        // The raw body is redacted before it is parsed, so the message and
        // code read from it cannot carry the credential either.
        let body = body.map(|body| redactor.body(&body));
        return Err(ProviderExecutionError::Rejected {
            failure: observe::with_retry_after(
                redactor.failure(rejected_failure(conversation, status, request_id, body)),
                retry_after_ms,
            ),
        });
    }

    let payload = response.take_payload();
    read_response(
        payload,
        status,
        started,
        request_id,
        config,
        borrow_output_sink(sink),
        conversation,
    )
    .await
    .map_err(|error| redactor.error(error))
}

const MAX_NATIVE_SSE_LINE_BYTES: usize = 16 * 1024 * 1024;

/// Reads one response stream, then admits its tool calls only as far as
/// `conversation` allowed them. Text, reasoning and every completed call
/// are kept as sent; a complete call this request did not admit is in
/// `tool_calls` as sent and in `refused` with its reason. The configured
/// timeout applies only while waiting for the next provider chunk; parsing
/// and persistence are outside that interval.
pub(crate) async fn read_response(
    payload: Payload,
    status_code: u16,
    started: Instant,
    request_id: Option<String>,
    config: &ProviderExecutionConfig,
    sink: Option<&mut dyn ProviderOutputSink>,
    conversation: &ChatConversation,
) -> Result<ProviderExecutionResult, ProviderExecutionError> {
    let mut capture = UsageCapture::default();
    let mut result = parse_stream(
        payload,
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
                    failure.provider_code = None;
                    failure.request_id = None;
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
                        status_code: failure.status_code,
                        retryable: failure.retryable,
                        ..ProviderFailure::default()
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

pub(super) async fn parse_stream(
    payload: Payload,
    status_code: u16,
    started: Instant,
    request_id: Option<String>,
    config: &ProviderExecutionConfig,
    sink: Option<&mut dyn ProviderOutputSink>,
    capture: &mut UsageCapture,
) -> Result<ProviderExecutionResult, ProviderExecutionError> {
    let mut output = StreamOutput::new(sink);
    let result = parse_stream_body(
        payload,
        status_code,
        started,
        request_id,
        config,
        capture,
        &mut output,
    )
    .await;
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
    started: Option<Instant>,
}

impl<'a> StreamOutput<'a> {
    fn new(sink: Option<&'a mut dyn ProviderOutputSink>) -> Self {
        Self {
            sink,
            text: String::with_capacity(STREAM_OUTPUT_BUFFER_BYTES),
            kind: IntermediateOutputKind::Progress,
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
        mut delta: &str,
    ) -> Result<(), OutputSinkError> {
        if delta.is_empty() || self.sink.is_none() {
            return Ok(());
        }
        if self.kind != kind {
            self.flush().await?;
            self.kind = kind;
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
        let text = std::mem::take(&mut self.text);
        self.started = None;
        let Some(sink) = self.sink.as_deref_mut() else {
            return Ok(());
        };
        sink.append(self.kind, &text).await
    }
}

impl ProviderOutputSink for StreamOutput<'_> {
    fn append<'a>(
        &'a mut self,
        kind: IntermediateOutputKind,
        text: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), OutputSinkError>> + 'a>> {
        Box::pin(async move { self.buffer(kind, text).await })
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
    output: &StreamOutput<'_>,
    idle_remaining: Duration,
) -> StreamWait {
    let chunk = ntex::time::timeout(idle_remaining, payload.recv());
    if output.is_pending() {
        match select(
            config.cancellation.cancelled(),
            select(ntex::time::sleep(output.flush_delay()), chunk),
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
    mut payload: Payload,
    status_code: u16,
    started: Instant,
    request_id: Option<String>,
    config: &ProviderExecutionConfig,
    capture: &mut UsageCapture,
    output: &mut StreamOutput<'_>,
) -> Result<ProviderExecutionResult, ProviderExecutionError> {
    let mut buffer = Vec::with_capacity(MAX_SSE_LINE_BYTES);
    let mut response = StreamedResponse::default();
    let native = config.transport == crate::db::entities::LlmTransport::OpenAiResponses;
    let mut responses = responses::ResponseStream::default();
    let anthropic = config.transport == crate::db::entities::LlmTransport::AnthropicMessages;
    let mut messages = anthropic::MessageStream::default();
    let line_limit = if native || anthropic {
        config
            .max_output_bytes
            .saturating_mul(6)
            .saturating_add(MAX_SSE_LINE_BYTES)
            .min(MAX_NATIVE_SSE_LINE_BYTES)
    } else {
        MAX_SSE_LINE_BYTES
    };
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
        let chunk = match next_stream_event(&mut payload, config, output, remaining).await {
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
        for byte in chunk {
            if buffer.len() == line_limit {
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
            if line.len() > line_limit {
                return Err(
                    response.incomplete("provider SSE line exceeded its limit", status_code)
                );
            }
            let Some(data) = line.strip_prefix(b"data:") else {
                buffer.clear();
                continue;
            };
            let data = trim_ascii(data);
            // `[DONE]` ends the response whether or not the connection
            // closes; whatever follows it is not part of the response.
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
            // The usage is taken before anything else of the event, so
            // neither a refused part of the event nor a failed delta write
            // loses its counters.
            let event = if anthropic {
                messages
                    .event(&event, config.max_output_bytes)
                    .map_err(|message| {
                        if message == "Anthropic output exceeded its limit" {
                            local_stop(
                                message,
                                status_code,
                                response.finish_reason,
                                response.raw_finish_reason.as_deref(),
                            )
                        } else {
                            response.incomplete(message, status_code)
                        }
                    })?
            } else {
                event
            };
            let reported = if anthropic {
                messages.reported_usage()
            } else if native {
                usage::reported_responses_usage(&event)
            } else {
                reported_usage(&event)
            };
            if let Some(reported) = reported {
                response.usage = reported.usage.clone();
                capture.record(Some(output), reported).await;
            }
            if native && event["type"] == "response.failed" {
                let (message, provider_code) =
                    observe::parse_provider_error(&event["response"]["error"].to_string())
                        .unwrap_or_else(|| {
                            ("Responses provider failed after delivery".to_owned(), None)
                        });
                return Err(ProviderExecutionError::StreamFailure {
                    failure: Box::new(ProviderFailure {
                        message,
                        provider_code,
                        request_id: request_id.clone(),
                        ..ProviderFailure::default()
                    }),
                    facts: response.facts(status_code),
                    finished: response.finish_reason.is_some() && response.tool_calls.is_empty(),
                });
            }
            if native && event["type"] == "response.incomplete" {
                let reason = event["response"]["incomplete_details"]["reason"]
                    .as_str()
                    .unwrap_or("unspecified");
                return Err(response.incomplete(
                    format!("Responses request ended incomplete: {reason}"),
                    status_code,
                ));
            }
            let event = if native {
                responses
                    .event(&event, config.max_output_bytes)
                    .map_err(|message| {
                        if message == "Responses output exceeded its limit" {
                            local_stop(
                                message,
                                status_code,
                                response.finish_reason,
                                response.raw_finish_reason.as_deref(),
                            )
                        } else {
                            response.incomplete(message, status_code)
                        }
                    })?
            } else {
                event
            };
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
            if native && responses.terminal {
                // The completed native snapshot owns the final answer and calls;
                // already published deltas remain progress in the output history.
                response = StreamedResponse {
                    usage: std::mem::take(&mut response.usage),
                    ..Default::default()
                };
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
            if native && responses.terminal {
                response.raw_finish_reason = Some("completed".to_owned());
                break 'stream;
            }
            if anthropic && messages.terminal {
                response.raw_finish_reason = messages.stop_reason.clone();
            }
            for (kind, delta) in delta {
                output.buffer(kind, &delta).await.map_err(|error| {
                    local_stop(
                        &error.message,
                        status_code,
                        response.finish_reason,
                        response.raw_finish_reason.as_deref(),
                    )
                })?;
            }
            if anthropic && messages.terminal {
                break 'stream;
            }
        }
    }
    // A stream cut short after cancellation is the local stop, not the provider's incomplete stream.
    let cut_short = !buffer.is_empty() || !saw_event || response.finish_reason.is_none();
    if cut_short && config.cancellation.is_cancelled() {
        return Err(local_stop(
            "provider execution was cancelled before the stream finished",
            status_code,
            response.finish_reason,
            response.raw_finish_reason.as_deref(),
        ));
    }
    if anthropic && !messages.terminal {
        return Err(response.incomplete("Anthropic stream ended without message_stop", status_code));
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
    let finished_in = started.elapsed();
    Ok(ProviderExecutionResult {
        response_items: if anthropic {
            messages.output()
        } else {
            responses.output
        },
        text: response.text,
        reasoning: response.reasoning,
        tool_calls,
        refused,
        usage: response.usage,
        finish_reason: response.finish_reason,
        raw_finish_reason: response.raw_finish_reason,
        status_code,
        request_id,
        ttfb: first_byte.map_or(finished_in, |first| first.duration_since(started)),
        finished_in,
    })
}

/// A stop on this side after the response head arrived, keeping the facts seen so far.
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

/// What a response stream has produced so far.
#[derive(Default)]
struct StreamedResponse {
    text: String,
    reasoning: String,
    tool_calls: ToolCallAssembler,
    usage: NormalizedUsage,
    finish_reason: Option<ChatFinishReason>,
    raw_finish_reason: Option<String>,
}

impl StreamedResponse {
    /// The bytes the response limit covers: text, reasoning and tool calls.
    fn output_bytes(&self) -> usize {
        self.text
            .len()
            .saturating_add(self.reasoning.len())
            .saturating_add(self.tool_calls.bytes())
    }

    /// The facts accepted so far, under the status of the response head.
    fn facts(&self, status_code: u16) -> ResponseFacts {
        ResponseFacts {
            status_code,
            finish_reason: self.finish_reason,
            raw_finish_reason: self.raw_finish_reason.clone(),
        }
    }

    /// A protocol fault of the response, with the facts accepted before it.
    fn incomplete(&self, message: impl Into<String>, status_code: u16) -> ProviderExecutionError {
        ProviderExecutionError::Incomplete {
            message: message.into(),
            facts: self.facts(status_code),
        }
    }

    /// A transport failure or Provider silence after the response head,
    /// with the facts accepted before it.
    fn unknown(&self, message: impl Into<String>, status_code: u16) -> ProviderExecutionError {
        ProviderExecutionError::Unknown {
            message: message.into(),
            facts: Some(self.facts(status_code)),
            finished: self.finish_reason.is_some() && self.tool_calls.is_empty(),
        }
    }
}

/// Why one stream event was refused.
enum Refused {
    /// A tool call fragment the Provider sent is malformed or conflicting.
    ToolCall(InvalidToolCall),
    /// Taking the event would exceed the configured output limit, a local
    /// protection rather than a fault of the Provider.
    OutputLimit,
}

/// Folds the choice of one event into `response` and returns its ordered deltas.
/// The event's usage is not read here, see [`reported_usage`]. An invalid
/// tool-call fragment rejects the whole choice, so none of its text and no
/// finish reason of it is taken either.
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
            response.reasoning.push_str(value);
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
    if let Some(raw) = choice.get("finish_reason").and_then(Value::as_str) {
        response.raw_finish_reason = Some(raw.to_owned());
        response.finish_reason = Some(ChatFinishReason::from_openai_str(raw));
    }
    Ok(deltas)
}
