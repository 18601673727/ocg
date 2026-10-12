//! OpenAI-compatible provider execution for an admitted Job.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};
use std::{future::Future, pin::Pin};

use ntex::client::{Client, ClientResponse, error::ClientError};
use ntex::http::StatusCode;
use ntex::http::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use ntex::time::Millis;
use serde_json::{Value, json};

use crate::openai_compatible::{ChatFinishReason, NormalizedUsage, ProviderFailure};
use crate::persistence::{FrozenExecutionConfig, LlmParameters};

const DEFAULT_ENDPOINT: &str = "https://api.openai.com/v1/chat/completions";
const MAX_SSE_LINE_BYTES: usize = 64 * 1024;
const MAX_PROVIDER_ERROR_BYTES: usize = 64 * 1024;
const MAX_OUTPUT_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(120);

/// The temporary bootstrap credential boundary. The name is persisted, while
/// this resolver returns the secret only to the request builder.
pub trait CredentialResolver: Send + Sync {
    fn resolve(
        &self,
        credential_ref: Option<&str>,
    ) -> Result<Option<String>, ProviderExecutionError>;
}

/// Receives text deltas while the provider stream is still active. The
/// runtime uses this to publish bounded intermediate outputs through the
/// authoritative database before final settlement.
pub trait ProviderOutputSink {
    fn append<'a>(
        &'a mut self,
        text: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), ProviderExecutionError>> + 'a>>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct EnvironmentCredentialResolver;

impl CredentialResolver for EnvironmentCredentialResolver {
    fn resolve(
        &self,
        credential_ref: Option<&str>,
    ) -> Result<Option<String>, ProviderExecutionError> {
        let Some(name) = credential_ref else {
            return Ok(None);
        };
        std::env::var(name).map(Some).map_err(|error| match error {
            std::env::VarError::NotPresent => ProviderExecutionError::Build {
                message: format!("credential reference `{name}` is not present"),
            },
            std::env::VarError::NotUnicode(_) => ProviderExecutionError::Build {
                message: format!("credential reference `{name}` is not valid UTF-8"),
            },
        })
    }
}

#[derive(Debug, Clone)]
pub struct ProviderExecutionConfig {
    pub timeout: Duration,
    pub max_output_bytes: usize,
    pub cancellation: CancellationToken,
}

impl Default for ProviderExecutionConfig {
    fn default() -> Self {
        Self {
            timeout: DEFAULT_TIMEOUT,
            max_output_bytes: MAX_OUTPUT_BYTES,
            cancellation: CancellationToken::default(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct CancellationToken(Arc<AtomicBool>);

impl CancellationToken {
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

#[derive(Debug, Clone)]
pub struct ProviderExecutionResult {
    pub text: String,
    pub usage: NormalizedUsage,
    pub finish_reason: Option<ChatFinishReason>,
    pub raw_finish_reason: Option<String>,
    pub status_code: u16,
    pub request_id: Option<String>,
    pub ttfb: Duration,
    pub finished_in: Duration,
}

#[derive(Debug, thiserror::Error)]
pub enum ProviderExecutionError {
    #[error("provider request could not be built: {message}")]
    Build { message: String },
    #[error("provider request failed before delivery: {message}")]
    NotDelivered { message: String },
    #[error("provider response failed after possible delivery: {message}")]
    Unknown { message: String },
    #[error("provider rejected the request: {failure}")]
    Rejected { failure: ProviderFailure },
    #[error("provider stream was incomplete: {message}")]
    Incomplete { message: String },
}

impl ProviderExecutionError {
    pub fn failure(&self) -> Option<&ProviderFailure> {
        match self {
            Self::Rejected { failure } => Some(failure),
            _ => None,
        }
    }
}

pub async fn execute_chat(
    frozen: &FrozenExecutionConfig,
    objective: Option<&str>,
    resolver: &dyn CredentialResolver,
    config: &ProviderExecutionConfig,
) -> Result<ProviderExecutionResult, ProviderExecutionError> {
    execute_chat_inner(frozen, objective, resolver, config, None).await
}

async fn execute_chat_inner(
    frozen: &FrozenExecutionConfig,
    objective: Option<&str>,
    resolver: &dyn CredentialResolver,
    config: &ProviderExecutionConfig,
    sink: Option<&mut dyn ProviderOutputSink>,
) -> Result<ProviderExecutionResult, ProviderExecutionError> {
    let provider = &frozen.llm.provider;
    let endpoint = provider.endpoint.as_deref().unwrap_or(DEFAULT_ENDPOINT);
    let endpoint = endpoint.trim_end_matches('/');
    let endpoint = if endpoint.ends_with("/chat/completions") {
        endpoint.to_owned()
    } else {
        format!("{endpoint}/chat/completions")
    };
    if !endpoint.starts_with("https://") {
        return Err(ProviderExecutionError::Build {
            message: "provider endpoint must use https".to_owned(),
        });
    }

    let credential = resolver.resolve(provider.credential_ref.as_deref())?;
    if config.cancellation.is_cancelled() {
        return Err(ProviderExecutionError::NotDelivered {
            message: "provider execution was cancelled before sending".to_owned(),
        });
    }
    let body = request_body(
        &frozen.llm.config.model,
        &frozen.llm.config.parameters,
        frozen
            .agent
            .as_ref()
            .and_then(|agent| agent.instructions.as_deref()),
        objective,
    );
    let client = Client::builder()
        .disable_redirects()
        .response_timeout(Millis(
            config.timeout.as_millis().min(u128::from(u32::MAX)) as u32
        ))
        .response_payload_timeout(Millis(
            config.timeout.as_millis().min(u128::from(u32::MAX)) as u32
        ))
        .build(ntex::SharedCfg::default())
        .await
        .map_err(|error| ProviderExecutionError::Build {
            message: error.to_string(),
        })?;

    let request = client
        .post(endpoint)
        .set_header(CONTENT_TYPE, "application/json")
        .set_header(ACCEPT, "text/event-stream")
        .set_header("Cache-Control", "no-cache")
        .if_some(credential.as_deref(), |credential, request| {
            request.set_header(AUTHORIZATION, format!("Bearer {credential}"))
        });

    let started = Instant::now();
    let response = ntex::time::timeout(config.timeout, request.send_json(&body))
        .await
        .map_err(|_| ProviderExecutionError::Unknown {
            message: "provider request timed out after it may have been delivered".to_owned(),
        })?
        .map_err(classify_client_error)?;
    let status = response.status().as_u16();
    let request_id = response
        .header("x-request-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    if response.status() != StatusCode::OK {
        let body = bounded_body(response, MAX_PROVIDER_ERROR_BYTES).await;
        return Err(ProviderExecutionError::Rejected {
            failure: provider_failure(status, request_id, body),
        });
    }

    ntex::time::timeout(
        config.timeout,
        parse_stream(
            response,
            started,
            request_id,
            config.max_output_bytes,
            config.cancellation.clone(),
            sink,
        ),
    )
    .await
    .map_err(|_| ProviderExecutionError::Unknown {
        message: "provider stream timed out after it may have been delivered".to_owned(),
    })?
}

pub async fn execute_chat_with_sink(
    frozen: &FrozenExecutionConfig,
    objective: Option<&str>,
    resolver: &dyn CredentialResolver,
    config: &ProviderExecutionConfig,
    sink: &mut dyn ProviderOutputSink,
) -> Result<ProviderExecutionResult, ProviderExecutionError> {
    execute_chat_inner(frozen, objective, resolver, config, Some(sink)).await
}

fn request_body(
    model: &str,
    parameters: &LlmParameters,
    instructions: Option<&str>,
    objective: Option<&str>,
) -> Value {
    let mut messages = Vec::new();
    if let Some(instructions) = instructions.filter(|value| !value.is_empty()) {
        messages.push(json!({"role": "system", "content": instructions}));
    }
    messages.push(json!({"role": "user", "content": objective.unwrap_or("")}));
    let mut body = json!({"model": model, "messages": messages, "stream": true, "stream_options": {"include_usage": true}});
    let Some(object) = body.as_object_mut() else {
        return json!({"model": model, "messages": messages, "stream": true});
    };
    if let Some(value) = parameters.temperature {
        object.insert("temperature".to_owned(), json!(value));
    }
    if let Some(value) = parameters.top_p {
        object.insert("top_p".to_owned(), json!(value));
    }
    if let Some(value) = parameters.max_tokens {
        object.insert("max_tokens".to_owned(), json!(value));
    }
    if let Some(value) = parameters.presence_penalty {
        object.insert("presence_penalty".to_owned(), json!(value));
    }
    if let Some(value) = parameters.frequency_penalty {
        object.insert("frequency_penalty".to_owned(), json!(value));
    }
    body
}

async fn parse_stream(
    response: ClientResponse,
    started: Instant,
    request_id: Option<String>,
    max_output_bytes: usize,
    cancellation: CancellationToken,
    mut sink: Option<&mut dyn ProviderOutputSink>,
) -> Result<ProviderExecutionResult, ProviderExecutionError> {
    let mut payload = response.take_payload();
    let mut buffer = Vec::with_capacity(MAX_SSE_LINE_BYTES);
    let mut text = String::new();
    let mut usage = NormalizedUsage::default();
    let mut finish_reason = None;
    let mut raw_finish_reason = None;
    let mut saw_event = false;
    let mut first_byte = None;
    let status_code = response.status().as_u16();

    while let Some(chunk) = payload.recv().await {
        if cancellation.is_cancelled() {
            return Err(ProviderExecutionError::Unknown {
                message: "provider execution was cancelled while streaming".to_owned(),
            });
        }
        let chunk = chunk.map_err(|error| ProviderExecutionError::Unknown {
            message: error.to_string(),
        })?;
        first_byte.get_or_insert_with(Instant::now);
        for byte in chunk {
            buffer.push(byte);
            if buffer.len() > MAX_SSE_LINE_BYTES {
                return Err(ProviderExecutionError::Incomplete {
                    message: "provider SSE line exceeded its limit".to_owned(),
                });
            }
            if byte != b'\n' {
                continue;
            }
            let line = std::mem::take(&mut buffer);
            let line = line.strip_suffix(b"\n").unwrap_or(&line);
            let line = line.strip_suffix(b"\r").unwrap_or(line);
            if line.len() > MAX_SSE_LINE_BYTES {
                return Err(ProviderExecutionError::Incomplete {
                    message: "provider SSE line exceeded its limit".to_owned(),
                });
            }
            let Some(data) = line.strip_prefix(b"data:") else {
                continue;
            };
            let data = trim_ascii(data);
            if data == b"[DONE]" {
                continue;
            }
            let event: Value = serde_json::from_slice(data).map_err(|error| {
                ProviderExecutionError::Incomplete {
                    message: format!("malformed provider SSE event: {error}"),
                }
            })?;
            saw_event = true;
            let delta = consume_event(
                &event,
                &mut text,
                &mut usage,
                &mut finish_reason,
                &mut raw_finish_reason,
                max_output_bytes,
            )?;
            if let Some(delta) = delta.as_deref()
                && let Some(sink) = sink.as_deref_mut()
            {
                sink.append(delta).await?;
            }
        }
    }
    if !buffer.is_empty() {
        return Err(ProviderExecutionError::Incomplete {
            message: "provider stream ended with an incomplete SSE frame".to_owned(),
        });
    }
    if !saw_event || finish_reason.is_none() {
        return Err(ProviderExecutionError::Incomplete {
            message: "provider stream ended without a finish event".to_owned(),
        });
    }
    let finished_in = started.elapsed();
    Ok(ProviderExecutionResult {
        text,
        usage,
        finish_reason,
        raw_finish_reason,
        status_code,
        request_id,
        ttfb: first_byte.map_or(finished_in, |first| first.duration_since(started)),
        finished_in,
    })
}

fn consume_event(
    event: &Value,
    text: &mut String,
    usage: &mut NormalizedUsage,
    finish_reason: &mut Option<ChatFinishReason>,
    raw_finish_reason: &mut Option<String>,
    max_output_bytes: usize,
) -> Result<Option<String>, ProviderExecutionError> {
    if let Some(value) = event.get("usage") {
        usage.raw = Some(value.clone());
        usage.input_tokens = value
            .get("prompt_tokens")
            .and_then(Value::as_u64)
            .and_then(|value| u32::try_from(value).ok());
        usage.output_tokens = value
            .get("completion_tokens")
            .and_then(Value::as_u64)
            .and_then(|value| u32::try_from(value).ok());
        usage.cache_read_tokens = value
            .get("prompt_tokens_details")
            .and_then(|details| details.get("cached_tokens"))
            .and_then(Value::as_u64)
            .and_then(|value| u32::try_from(value).ok());
        usage.reasoning_tokens = value
            .get("completion_tokens_details")
            .and_then(|details| details.get("reasoning_tokens"))
            .and_then(Value::as_u64)
            .and_then(|value| u32::try_from(value).ok());
    }
    let Some(choice) = event
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|choices| choices.first())
    else {
        return Ok(None);
    };
    let mut text_delta = None;
    if let Some(delta) = choice.get("delta")
        && let Some(value) = delta.get("content").and_then(Value::as_str)
    {
        if text.len().saturating_add(value.len()) > max_output_bytes {
            return Err(ProviderExecutionError::Incomplete {
                message: "provider output exceeded its limit".to_owned(),
            });
        }
        text.push_str(value);
        text_delta = Some(value.to_owned());
    }
    if let Some(raw) = choice.get("finish_reason").and_then(Value::as_str) {
        *raw_finish_reason = Some(raw.to_owned());
        *finish_reason = Some(match raw {
            "stop" => ChatFinishReason::Stop,
            "length" => ChatFinishReason::Length,
            "tool_calls" => ChatFinishReason::ToolCalls,
            "content_filter" => ChatFinishReason::ContentFilter,
            "error" => ChatFinishReason::Error,
            _ => ChatFinishReason::Other,
        });
    }
    Ok(text_delta)
}

async fn bounded_body(response: ClientResponse, limit: usize) -> Option<String> {
    response
        .body()
        .limit(limit)
        .await
        .ok()
        .and_then(|body| String::from_utf8(body.to_vec()).ok())
}

fn provider_failure(
    status_code: u16,
    request_id: Option<String>,
    response_body: Option<String>,
) -> ProviderFailure {
    let (message, provider_code) = response_body
        .as_deref()
        .and_then(parse_provider_error)
        .unwrap_or_else(|| (format!("provider returned HTTP {status_code}"), None));
    ProviderFailure {
        message,
        status_code: Some(status_code),
        provider_code,
        response_body,
        request_id,
        retry_after_ms: None,
        retryable: matches!(status_code, 408 | 409 | 425 | 429 | 500..=599),
    }
}

fn parse_provider_error(body: &str) -> Option<(String, Option<String>)> {
    let value: Value = serde_json::from_str(body).ok()?;
    let error = value.get("error")?;
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("provider request failed")
        .to_owned();
    let code = error.get("code").and_then(Value::as_str).map(str::to_owned);
    Some((message, code))
}

fn classify_client_error(error: ClientError) -> ProviderExecutionError {
    let message = error.to_string();
    match error {
        ClientError::Url(_) | ClientError::Connect(_) => {
            ProviderExecutionError::NotDelivered { message }
        }
        ClientError::Request(_) => ProviderExecutionError::NotDelivered { message },
        ClientError::Send(_) => ProviderExecutionError::Unknown { message },
        ClientError::Timeout
        | ClientError::Response(_)
        | ClientError::Http(_)
        | ClientError::H2(_)
        | ClientError::TunnelNotSupported
        | ClientError::Error(_) => ProviderExecutionError::Unknown { message },
    }
}

fn trim_ascii(value: &[u8]) -> &[u8] {
    let start = value
        .iter()
        .position(|byte| !byte.is_ascii_whitespace())
        .unwrap_or(value.len());
    let end = value
        .iter()
        .rposition(|byte| !byte.is_ascii_whitespace())
        .map_or(start, |index| index + 1);
    &value[start..end]
}

pub fn metrics(result: &ProviderExecutionResult) -> crate::persistence::Measurements {
    crate::persistence::Measurements {
        ttfb_ms: Some(result.ttfb.as_millis().min(u128::from(u64::MAX)) as u64),
        request_finished_in_ms: Some(
            result.finished_in.as_millis().min(u128::from(u64::MAX)) as u64
        ),
        usage: result.usage.clone(),
        ..crate::persistence::Measurements::default()
    }
}

pub fn error_observation(
    error: &ProviderExecutionError,
) -> crate::persistence::AvailabilityObservation {
    crate::persistence::AvailabilityObservation {
        status_code: error.failure().and_then(|failure| failure.status_code),
        finish_reason: None,
        raw_finish_reason: None,
        failure: error.failure().cloned().or_else(|| {
            Some(ProviderFailure {
                message: error.to_string(),
                ..ProviderFailure::default()
            })
        }),
    }
}

pub fn success_observation(
    result: &ProviderExecutionResult,
) -> crate::persistence::AvailabilityObservation {
    crate::persistence::AvailabilityObservation {
        status_code: Some(result.status_code),
        finish_reason: result.finish_reason,
        raw_finish_reason: result.raw_finish_reason.clone(),
        failure: None,
    }
}

pub fn result_value(result: &ProviderExecutionResult) -> Value {
    let mut value = serde_json::Map::new();
    value.insert("text".to_owned(), Value::String(result.text.clone()));
    if let Some(reason) = result.raw_finish_reason.as_deref() {
        value.insert("finish_reason".to_owned(), Value::String(reason.to_owned()));
    }
    Value::Object(value)
}
