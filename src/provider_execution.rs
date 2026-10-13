//! OpenAI-compatible provider execution for an admitted Job.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};
use std::{future::Future, pin::Pin};

use ntex::client::{Client, ClientResponse, error::ClientError};
use ntex::http::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use ntex::http::{Payload, StatusCode};
use ntex::time::Millis;
use serde_json::{Value, json};

use crate::openai_compatible::{ChatFinishReason, NormalizedUsage, ProviderFailure};
use crate::persistence::{FrozenExecutionConfig, LlmParameters, Timestamp};
use crate::token_usage::UsageFinality;

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
///
/// It also receives the Token facts of the request as they become known, see
/// [`ProviderObservation`]. Each is awaited, so the producer never runs ahead
/// of what the sink has taken.
pub trait ProviderOutputSink {
    fn append<'a>(
        &'a mut self,
        text: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), OutputSinkError>> + 'a>>;

    /// Takes one Token fact. It cannot fail the request: a sink that cannot
    /// keep a fact owns that gap, and the provider outcome stays what the
    /// provider and transport made it.
    fn observe<'a>(
        &'a mut self,
        _observation: ProviderObservation,
    ) -> Pin<Box<dyn Future<Output = ()> + 'a>> {
        Box::pin(async {})
    }
}

/// A Token fact of one request, with the time the producer learned it.
#[derive(Debug, Clone, PartialEq)]
pub struct ProviderObservation {
    pub observed_at: Timestamp,
    pub fact: ProviderFact,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProviderFact {
    /// The request was positively known not to have been sent: it failed
    /// while being built, or before any byte could leave. No Provider saw it.
    NotDelivered,
    /// The request was issued or may have been delivered: a response head
    /// arrived, or the send failed in a way that cannot rule delivery out.
    /// It is announced only once delivery is no longer excluded, so a
    /// request that can still be classified NotDelivered never has it.
    Delivered,
    /// One usage object the provider reported. Its counters are normalized
    /// only where valid, and the object is kept raw.
    Usage {
        usage: NormalizedUsage,
        finality: UsageFinality,
    },
}

/// A local failure to persist a streamed delta. It is a fact about this
/// runtime, not about the provider, so it never becomes a provider failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputSinkError {
    pub message: String,
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

/// The facts of a provider response observed before execution stopped on
/// this side. The status is always present: a stop happens only after the
/// response head arrived.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResponseFacts {
    pub status_code: u16,
    pub finish_reason: Option<ChatFinishReason>,
    pub raw_finish_reason: Option<String>,
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
    /// Execution stopped on this side after the provider had answered: the
    /// caller cancelled, or the runtime could not persist streamed output.
    /// Neither is a provider or transport fault, so only the response facts
    /// already observed are kept.
    #[error("provider execution stopped locally: {message}")]
    Stopped {
        message: String,
        facts: ResponseFacts,
    },
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
    mut sink: Option<&mut (dyn ProviderOutputSink + '_)>,
) -> Result<ProviderExecutionResult, ProviderExecutionError> {
    let mut announced = false;
    let result = exchange(
        frozen,
        objective,
        resolver,
        config,
        sink.as_deref_mut(),
        &mut announced,
    )
    .await;
    // An error before any response head is classified by what it proves about
    // delivery; a response head has already announced the request.
    if let Err(error) = &result
        && !announced
    {
        announce(sink, unannounced_fact(error)).await;
    }
    result
}

/// What an error before any response head proves about delivery. Only a
/// failure that positively happened before the request could leave is
/// NotDelivered; anything that may have reached a Provider is Delivered,
/// whatever the Job's outcome becomes.
fn unannounced_fact(error: &ProviderExecutionError) -> ProviderFact {
    match error {
        ProviderExecutionError::Build { .. } | ProviderExecutionError::NotDelivered { .. } => {
            ProviderFact::NotDelivered
        }
        ProviderExecutionError::Unknown { .. }
        | ProviderExecutionError::Rejected { .. }
        | ProviderExecutionError::Incomplete { .. }
        | ProviderExecutionError::Stopped { .. } => ProviderFact::Delivered,
    }
}

/// Hands one fact to the sink with the current time.
pub(crate) async fn announce(sink: Option<&mut (dyn ProviderOutputSink + '_)>, fact: ProviderFact) {
    announce_at(sink, Timestamp::now(), fact).await;
}

/// Hands one fact to the sink with the time the producer learned it. The time
/// is fixed by the caller before this awaits, so a slow sink cannot move it.
async fn announce_at(
    sink: Option<&mut (dyn ProviderOutputSink + '_)>,
    observed_at: Timestamp,
    fact: ProviderFact,
) {
    if let Some(sink) = sink {
        sink.observe(ProviderObservation { observed_at, fact })
            .await;
    }
}

async fn exchange(
    frozen: &FrozenExecutionConfig,
    objective: Option<&str>,
    resolver: &dyn CredentialResolver,
    config: &ProviderExecutionConfig,
    mut sink: Option<&mut (dyn ProviderOutputSink + '_)>,
    announced: &mut bool,
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
    // A response head proves the request reached a Provider, whatever it says.
    *announced = true;
    announce(sink.as_deref_mut(), ProviderFact::Delivered).await;
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

    let payload = response.take_payload();
    read_response(payload, status, started, request_id, config, sink).await
}

/// Reads one response stream under the outer timeout. Every usage object is
/// handed to the sink, awaited, when it is parsed, so the timeout drops
/// nothing that was handed over. The one object being awaited when the
/// timeout fires may or may not be stored: its write was started and is
/// neither retried nor reported.
pub(crate) async fn read_response(
    payload: Payload,
    status_code: u16,
    started: Instant,
    request_id: Option<String>,
    config: &ProviderExecutionConfig,
    sink: Option<&mut (dyn ProviderOutputSink + '_)>,
) -> Result<ProviderExecutionResult, ProviderExecutionError> {
    let mut capture = UsageCapture::default();
    let outcome = ntex::time::timeout(
        config.timeout,
        parse_stream(
            payload,
            status_code,
            started,
            request_id,
            config,
            sink,
            &mut capture,
        ),
    )
    .await;
    outcome.unwrap_or_else(|_| {
        Err(ProviderExecutionError::Unknown {
            message: "provider stream timed out after it may have been delivered".to_owned(),
        })
    })
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
    mut payload: Payload,
    status_code: u16,
    started: Instant,
    request_id: Option<String>,
    config: &ProviderExecutionConfig,
    mut sink: Option<&mut (dyn ProviderOutputSink + '_)>,
    capture: &mut UsageCapture,
) -> Result<ProviderExecutionResult, ProviderExecutionError> {
    let mut buffer = Vec::with_capacity(MAX_SSE_LINE_BYTES);
    let mut text = String::new();
    let mut usage = NormalizedUsage::default();
    let mut finish_reason = None;
    let mut raw_finish_reason = None;
    let mut saw_event = false;
    let mut first_byte = None;

    while let Some(chunk) = payload.recv().await {
        if config.cancellation.is_cancelled() {
            return Err(local_stop(
                "provider execution was cancelled while streaming",
                status_code,
                finish_reason,
                raw_finish_reason.as_deref(),
            ));
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
            let consumed = consume_event(
                &event,
                &mut text,
                &mut usage,
                &mut finish_reason,
                &mut raw_finish_reason,
                config.max_output_bytes,
            )?;
            // The usage is taken before the delta, so a failed delta write
            // loses none of the event's counters.
            if let Some(reported) = consumed.usage {
                capture.record(sink.as_deref_mut(), reported).await;
            }
            if let Some(delta) = consumed.delta.as_deref()
                && let Some(sink) = sink.as_deref_mut()
            {
                // The failed delta's event is already applied, so its finish reason is kept.
                sink.append(delta).await.map_err(|error| {
                    local_stop(
                        &error.message,
                        status_code,
                        finish_reason,
                        raw_finish_reason.as_deref(),
                    )
                })?;
            }
        }
    }
    // A stream cut short after cancellation is the local stop, not the provider's incomplete stream.
    let cut_short = !buffer.is_empty() || !saw_event || finish_reason.is_none();
    if cut_short && config.cancellation.is_cancelled() {
        return Err(local_stop(
            "provider execution was cancelled before the stream finished",
            status_code,
            finish_reason,
            raw_finish_reason.as_deref(),
        ));
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

/// What one stream event contributed.
struct Consumed {
    delta: Option<String>,
    usage: Option<ReportedUsage>,
}

/// A valid, non-null usage object of one event, and what its position says
/// about its finality.
#[derive(Clone, Debug, PartialEq)]
struct ReportedUsage {
    usage: NormalizedUsage,
    finality: UsageFinality,
}

fn counter(value: Option<&Value>) -> Option<u32> {
    value
        .and_then(Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
}

/// The usage object of an event. `usage: null` and any other non-object is
/// absence: it reports nothing and erases nothing.
///
/// Final is the documented `stream_options.include_usage` chunk: usage with
/// an explicit empty `choices` array carries the whole request's usage. Usage
/// beside choices (or with no `choices` at all) is a running total and stays
/// cumulative. A finish reason, `[DONE]`, the end of the stream or the Job's
/// completion never promotes anything to final.
fn reported_usage(event: &Value) -> Option<ReportedUsage> {
    let value = event.get("usage").filter(|value| value.is_object())?;
    let usage_only = event
        .get("choices")
        .and_then(Value::as_array)
        .is_some_and(Vec::is_empty);
    Some(ReportedUsage {
        usage: NormalizedUsage {
            input_tokens: counter(value.get("prompt_tokens")),
            output_tokens: counter(value.get("completion_tokens")),
            cache_read_tokens: counter(
                value
                    .get("prompt_tokens_details")
                    .and_then(|details| details.get("cached_tokens")),
            ),
            cache_write_tokens: None,
            reasoning_tokens: counter(
                value
                    .get("completion_tokens_details")
                    .and_then(|details| details.get("reasoning_tokens")),
            ),
            raw: Some(value.clone()),
        },
        finality: if usage_only {
            UsageFinality::Final
        } else {
            UsageFinality::Cumulative
        },
    })
}

fn consume_event(
    event: &Value,
    text: &mut String,
    usage: &mut NormalizedUsage,
    finish_reason: &mut Option<ChatFinishReason>,
    raw_finish_reason: &mut Option<String>,
    max_output_bytes: usize,
) -> Result<Consumed, ProviderExecutionError> {
    let reported = reported_usage(event);
    if let Some(reported) = &reported {
        *usage = reported.usage.clone();
    }
    let mut consumed = Consumed {
        delta: None,
        usage: reported,
    };
    let Some(choice) = event
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|choices| choices.first())
    else {
        return Ok(consumed);
    };
    if let Some(delta) = choice.get("delta")
        && let Some(value) = delta.get("content").and_then(Value::as_str)
    {
        if text.len().saturating_add(value.len()) > max_output_bytes {
            return Err(ProviderExecutionError::Incomplete {
                message: "provider output exceeded its limit".to_owned(),
            });
        }
        text.push_str(value);
        consumed.delta = Some(value.to_owned());
    }
    if let Some(raw) = choice.get("finish_reason").and_then(Value::as_str) {
        *raw_finish_reason = Some(raw.to_owned());
        *finish_reason = Some(ChatFinishReason::from_openai_str(raw));
    }
    Ok(consumed)
}

/// Hands each usage object a stream reports to the sink as it is parsed. It
/// keeps no history, no count and no merged total: the only state is the last
/// cumulative object, to skip an identical repeat, and the clock. Memory is
/// one bounded SSE object plus that one, whatever the stream reports, and each
/// object is awaited before the next is read, so a slow sink slows the read.
pub(crate) struct UsageCapture {
    last_cumulative: Option<NormalizedUsage>,
    /// When the producer observes an object. It is read when the object is
    /// parsed, before the sink is awaited.
    clock: fn() -> Timestamp,
}

impl Default for UsageCapture {
    fn default() -> Self {
        Self::with_clock(Timestamp::now)
    }
}

impl UsageCapture {
    pub(crate) fn with_clock(clock: fn() -> Timestamp) -> Self {
        Self {
            last_cumulative: None,
            clock,
        }
    }

    /// Hands one reported object over as it is, with its own normalization,
    /// its finality and the time it was parsed. Distinct objects are never
    /// merged or dropped, however many there are; per-field maxima belong to
    /// the reducer. Only an identical consecutive cumulative repeat, which
    /// carries nothing new, is skipped.
    async fn record(
        &mut self,
        sink: Option<&mut (dyn ProviderOutputSink + '_)>,
        reported: ReportedUsage,
    ) {
        let Some(sink) = sink else { return };
        let observed_at = (self.clock)();
        if reported.finality == UsageFinality::Cumulative {
            if self.last_cumulative.as_ref() == Some(&reported.usage) {
                return;
            }
            self.last_cumulative = Some(reported.usage.clone());
        }
        announce_at(
            Some(sink),
            observed_at,
            ProviderFact::Usage {
                usage: reported.usage,
                finality: reported.finality,
            },
        )
        .await;
    }
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

/// The timing of a finished request, and nothing else. Its usage is in the
/// result and in the annotated Metrics records captured while it streamed.
pub fn metrics(result: &ProviderExecutionResult) -> crate::persistence::Measurements {
    crate::persistence::Measurements {
        ttfb_ms: Some(result.ttfb.as_millis().min(u128::from(u64::MAX)) as u64),
        request_finished_in_ms: Some(
            result.finished_in.as_millis().min(u128::from(u64::MAX)) as u64
        ),
        ..crate::persistence::Measurements::default()
    }
}

/// The availability observation an error proves, if it proves one. Only what
/// the provider or transport actually reported becomes a failure: a refusal
/// before delivery proves nothing, and a local stop keeps just the response
/// facts it already had. Nothing is inferred from an unknown outcome.
pub fn error_observation(
    error: &ProviderExecutionError,
) -> Option<crate::persistence::AvailabilityObservation> {
    use crate::persistence::AvailabilityObservation;
    match error {
        ProviderExecutionError::Build { .. } | ProviderExecutionError::NotDelivered { .. } => None,
        ProviderExecutionError::Stopped { facts, .. } => Some(AvailabilityObservation {
            status_code: Some(facts.status_code),
            finish_reason: facts.finish_reason,
            raw_finish_reason: facts.raw_finish_reason.clone(),
            failure: None,
        }),
        ProviderExecutionError::Rejected { failure } => Some(AvailabilityObservation {
            status_code: failure.status_code,
            finish_reason: None,
            raw_finish_reason: None,
            failure: Some(failure.clone()),
        }),
        ProviderExecutionError::Unknown { .. } | ProviderExecutionError::Incomplete { .. } => {
            Some(AvailabilityObservation {
                status_code: None,
                finish_reason: None,
                raw_finish_reason: None,
                failure: Some(ProviderFailure {
                    message: error.to_string(),
                    ..ProviderFailure::default()
                }),
            })
        }
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

/// A scripted response body, for tests that drive the real parser.
#[cfg(test)]
pub(crate) mod scripted {
    use std::collections::VecDeque;
    use std::io;

    use ntex::http::Payload;
    use ntex::http::error::PayloadError;
    use ntex::util::{Bytes, Stream};

    use super::CancellationToken;

    /// One scripted step of a response body.
    pub(crate) enum Step {
        Chunk(&'static str),
        Fail,
        /// Cancels the execution, then yields the next chunk so the parser sees it.
        Cancel,
        /// Yields nothing, ever: only a timeout ends the read.
        Stall,
    }

    struct Body {
        steps: VecDeque<Step>,
        cancellation: CancellationToken,
    }

    impl Stream for Body {
        type Item = Result<Bytes, PayloadError>;

        fn poll_next(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            use std::task::Poll;
            while let Some(step) = self.steps.pop_front() {
                match step {
                    Step::Cancel => self.cancellation.cancel(),
                    Step::Chunk(text) => {
                        return Poll::Ready(Some(Ok(Bytes::from_static(text.as_bytes()))));
                    }
                    Step::Fail => {
                        return Poll::Ready(Some(Err(PayloadError::Incomplete(Some(
                            io::Error::other("connection reset"),
                        )))));
                    }
                    Step::Stall => {
                        // Stays at the front, so every poll stalls again.
                        self.steps.push_front(Step::Stall);
                        return Poll::Pending;
                    }
                }
            }
            Poll::Ready(None)
        }
    }

    pub(crate) fn payload(steps: Vec<Step>, cancellation: CancellationToken) -> Payload {
        Payload::from_stream(Body {
            steps: steps.into(),
            cancellation,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::scripted::{Step, payload};
    use super::*;
    use crate::test_support::wait;

    const CONTENT: &str = "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n";
    const STOP: &str = "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n";

    #[derive(Default)]
    struct RecordingSink {
        deltas: Vec<String>,
        fail: Option<&'static str>,
        observations: Vec<ProviderObservation>,
    }

    impl RecordingSink {
        /// The usage facts received, with the finality each was given.
        fn usages(&self) -> Vec<(UsageFinality, NormalizedUsage)> {
            self.observations
                .iter()
                .filter_map(|observation| match &observation.fact {
                    ProviderFact::Usage { usage, finality } => Some((*finality, usage.clone())),
                    _ => None,
                })
                .collect()
        }
    }

    impl ProviderOutputSink for RecordingSink {
        fn append<'a>(
            &'a mut self,
            text: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<(), OutputSinkError>> + 'a>> {
            Box::pin(async move {
                match self.fail {
                    Some(message) => Err(OutputSinkError {
                        message: message.to_owned(),
                    }),
                    None => {
                        self.deltas.push(text.to_owned());
                        Ok(())
                    }
                }
            })
        }

        fn observe<'a>(
            &'a mut self,
            observation: ProviderObservation,
        ) -> Pin<Box<dyn Future<Output = ()> + 'a>> {
            Box::pin(async move { self.observations.push(observation) })
        }
    }

    fn parse(
        steps: Vec<Step>,
        sink: Option<&mut dyn ProviderOutputSink>,
    ) -> Result<ProviderExecutionResult, ProviderExecutionError> {
        let config = ProviderExecutionConfig::default();
        wait(parse_stream(
            payload(steps, config.cancellation.clone()),
            200,
            Instant::now(),
            None,
            &config,
            sink,
            &mut UsageCapture::default(),
        ))
    }

    fn facts(finish_reason: Option<ChatFinishReason>, raw: Option<&str>) -> ResponseFacts {
        ResponseFacts {
            status_code: 200,
            finish_reason,
            raw_finish_reason: raw.map(str::to_owned),
        }
    }

    #[test]
    fn a_complete_stream_streams_each_delta_to_the_sink() {
        let mut sink = RecordingSink::default();
        let result = parse(
            vec![Step::Chunk(CONTENT), Step::Chunk(STOP)],
            Some(&mut sink),
        )
        .expect("a finished stream succeeds");
        assert_eq!(result.text, "hi");
        assert_eq!(sink.deltas, ["hi"]);
    }

    #[test]
    fn a_cancelled_stream_keeps_the_response_facts_it_already_had() {
        let error = parse(
            vec![
                Step::Chunk(CONTENT),
                Step::Chunk(STOP),
                Step::Cancel,
                Step::Chunk(CONTENT),
            ],
            None,
        )
        .expect_err("a cancelled stream stops");
        match error {
            ProviderExecutionError::Stopped { facts, .. } => {
                assert_eq!(
                    facts,
                    self::facts(Some(ChatFinishReason::Stop), Some("stop"))
                )
            }
            other => panic!("cancellation after send is a local stop, got {other:?}"),
        }
    }

    #[test]
    fn a_cancelled_stream_that_ends_early_is_a_local_stop_not_an_incomplete_stream() {
        let error = parse(vec![Step::Chunk(CONTENT), Step::Cancel], None)
            .expect_err("a cancelled stream stops");
        match error {
            ProviderExecutionError::Stopped { facts, .. } => {
                assert_eq!(facts, self::facts(None, None))
            }
            other => panic!("a cut-short stream after cancellation is local, got {other:?}"),
        }
    }

    #[test]
    fn a_cancellation_before_the_finish_keeps_only_the_status() {
        let error = parse(
            vec![Step::Chunk(CONTENT), Step::Cancel, Step::Chunk(STOP)],
            None,
        )
        .expect_err("a cancelled stream stops");
        match error {
            ProviderExecutionError::Stopped { facts, .. } => {
                assert_eq!(facts, self::facts(None, None))
            }
            other => panic!("cancellation after send is a local stop, got {other:?}"),
        }
    }

    #[test]
    fn a_local_output_failure_keeps_the_facts_of_the_event_that_failed() {
        let mut sink = RecordingSink {
            fail: Some("database could not persist provider output"),
            ..RecordingSink::default()
        };
        let error = parse(
            vec![Step::Chunk(
                "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"},\"finish_reason\":\"stop\"}]}\n\n",
            )],
            Some(&mut sink),
        )
        .expect_err("a sink failure stops the stream");
        match error {
            ProviderExecutionError::Stopped { message, facts } => {
                assert_eq!(message, "database could not persist provider output");
                assert_eq!(
                    facts,
                    self::facts(Some(ChatFinishReason::Stop), Some("stop"))
                );
            }
            other => panic!("a local write failure is a local stop, got {other:?}"),
        }
    }

    #[test]
    fn a_transport_failure_mid_stream_stays_an_unknown_provider_outcome() {
        let error =
            parse(vec![Step::Chunk(CONTENT), Step::Fail], None).expect_err("a broken body fails");
        assert!(
            matches!(error, ProviderExecutionError::Unknown { .. }),
            "a transport failure is reported as it occurred, got {error:?}"
        );
    }

    #[test]
    fn a_stream_without_a_finish_event_stays_incomplete() {
        let error = parse(vec![Step::Chunk(CONTENT)], None).expect_err("no finish event");
        assert!(matches!(error, ProviderExecutionError::Incomplete { .. }));
    }

    #[test]
    fn only_provider_and_transport_facts_become_availability_observations() {
        let stopped = ProviderExecutionError::Stopped {
            message: "cancelled".to_owned(),
            facts: facts(Some(ChatFinishReason::Stop), Some("stop")),
        };
        assert_eq!(
            error_observation(&stopped),
            Some(crate::persistence::AvailabilityObservation {
                status_code: Some(200),
                finish_reason: Some(ChatFinishReason::Stop),
                raw_finish_reason: Some("stop".to_owned()),
                failure: None,
            }),
            "a local stop keeps the response facts and no failure"
        );
        assert_eq!(
            error_observation(&ProviderExecutionError::Build {
                message: "bad".to_owned()
            }),
            None
        );
        assert_eq!(
            error_observation(&ProviderExecutionError::NotDelivered {
                message: "refused".to_owned()
            }),
            None
        );

        let unknown = ProviderExecutionError::Unknown {
            message: "reset".to_owned(),
        };
        let observation = error_observation(&unknown).expect("a transport fact");
        assert_eq!(observation.status_code, None);
        assert_eq!(
            observation.failure.map(|failure| failure.message),
            Some(unknown.to_string())
        );
    }

    const CUMULATIVE_10: &str = "data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":1}}\n\n";
    const CUMULATIVE_25: &str = "data: {\"choices\":[{\"delta\":{\"content\":\"b\"}}],\"usage\":{\"prompt_tokens\":25,\"completion_tokens\":2}}\n\n";
    const NULL_USAGE: &str =
        "data: {\"choices\":[{\"delta\":{\"content\":\"c\"}}],\"usage\":null}\n\n";
    const FINAL_40_60: &str = "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":40,\"completion_tokens\":60,\"prompt_tokens_details\":{\"cached_tokens\":0}}}\n\n";
    const FINAL_40_61: &str =
        "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":40,\"completion_tokens\":61}}\n\n";
    const FINAL_INPUT_ONLY: &str = "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":40}}\n\n";
    const FINAL_ZERO_OUTPUT: &str =
        "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":0}}\n\n";
    const DONE: &str = "data: [DONE]\n\n";

    fn recorded(
        steps: Vec<Step>,
    ) -> (
        Result<ProviderExecutionResult, ProviderExecutionError>,
        RecordingSink,
    ) {
        let mut sink = RecordingSink::default();
        let result = parse(steps, Some(&mut sink));
        (result, sink)
    }

    fn counters(usage: &NormalizedUsage) -> [Option<u32>; 3] {
        [
            usage.input_tokens,
            usage.output_tokens,
            usage.cache_read_tokens,
        ]
    }

    #[test]
    fn usage_null_is_absence_and_erases_nothing() {
        let (result, sink) = recorded(vec![
            Step::Chunk(CUMULATIVE_10),
            Step::Chunk(NULL_USAGE),
            Step::Chunk(STOP),
        ]);
        let result = result.expect("a finished stream");
        assert_eq!(
            sink.usages().len(),
            1,
            "a null usage reports nothing, so nothing is recorded for it"
        );
        assert_eq!(result.usage.input_tokens, Some(10), "earlier counters stay");
        assert!(result.usage.raw.is_some(), "and so does the raw object");
    }

    #[test]
    fn usage_beside_choices_is_cumulative_and_only_the_empty_choices_chunk_is_final() {
        let (result, sink) = recorded(vec![
            Step::Chunk(CUMULATIVE_10),
            Step::Chunk(CUMULATIVE_25),
            Step::Chunk(STOP),
            Step::Chunk(FINAL_40_60),
            Step::Chunk(DONE),
        ]);
        result.expect("a finished stream");
        let usages = sink.usages();
        assert_eq!(
            usages
                .iter()
                .map(|(finality, usage)| (*finality, counters(usage)))
                .collect::<Vec<_>>(),
            [
                (UsageFinality::Cumulative, [Some(10), Some(1), None]),
                (UsageFinality::Cumulative, [Some(25), Some(2), None]),
                (UsageFinality::Final, [Some(40), Some(60), Some(0)]),
            ]
        );
        assert!(
            usages[2].1.raw.is_some(),
            "each reported object is kept raw"
        );
    }

    #[test]
    fn a_normal_finish_end_of_stream_and_done_never_make_usage_final() {
        let (result, sink) = recorded(vec![
            Step::Chunk(CUMULATIVE_25),
            Step::Chunk(STOP),
            Step::Chunk(DONE),
        ]);
        result.expect("a finished stream");
        let usages = sink.usages();
        assert_eq!(usages.len(), 1);
        assert_eq!(usages[0].0, UsageFinality::Cumulative);
    }

    #[test]
    fn a_final_reports_only_its_own_fields_and_zero_is_not_missing() {
        let (_, sink) = recorded(vec![
            Step::Chunk(CUMULATIVE_25),
            Step::Chunk(STOP),
            Step::Chunk(FINAL_INPUT_ONLY),
            Step::Chunk(FINAL_ZERO_OUTPUT),
        ]);
        let usages = sink.usages();
        assert_eq!(
            counters(&usages[1].1),
            [Some(40), None, None],
            "an omitted field is missing, never zero and never the earlier value"
        );
        assert_eq!(
            counters(&usages[2].1),
            [Some(3), Some(0), None],
            "a real zero is a reported count"
        );
    }

    #[test]
    fn duplicate_and_conflicting_finals_all_reach_the_sink() {
        let (_, sink) = recorded(vec![
            Step::Chunk(FINAL_40_60),
            Step::Chunk(FINAL_40_60),
            Step::Chunk(FINAL_40_61),
            Step::Chunk(STOP),
        ]);
        assert_eq!(
            sink.usages()
                .iter()
                .map(|(finality, usage)| (*finality, usage.output_tokens))
                .collect::<Vec<_>>(),
            [
                (UsageFinality::Final, Some(60)),
                (UsageFinality::Final, Some(60)),
                (UsageFinality::Final, Some(61)),
            ]
        );
    }

    #[test]
    fn a_usage_reported_before_each_kind_of_stop_is_not_lost_with_the_parser() {
        let cases: [(&str, Vec<Step>); 3] = [
            (
                "transport failure",
                vec![Step::Chunk(CUMULATIVE_25), Step::Fail],
            ),
            ("stream without a finish", vec![Step::Chunk(CUMULATIVE_25)]),
            (
                "cancellation",
                vec![
                    Step::Chunk(CUMULATIVE_25),
                    Step::Cancel,
                    Step::Chunk(CONTENT),
                ],
            ),
        ];
        for (case, steps) in cases {
            let (result, sink) = recorded(steps);
            assert!(result.is_err(), "{case}");
            assert_eq!(
                sink.usages()
                    .iter()
                    .map(|(finality, usage)| (*finality, counters(usage)))
                    .collect::<Vec<_>>(),
                [(UsageFinality::Cumulative, [Some(25), Some(2), None])],
                "{case}: the usage reached the sink before the error"
            );
        }
    }

    #[test]
    fn a_failed_delta_write_keeps_the_counters_of_its_own_event() {
        let mut sink = RecordingSink {
            fail: Some("database could not persist provider output"),
            ..RecordingSink::default()
        };
        let error = parse(vec![Step::Chunk(CUMULATIVE_25)], Some(&mut sink))
            .expect_err("a sink failure stops the stream");
        assert!(matches!(error, ProviderExecutionError::Stopped { .. }));
        assert_eq!(sink.usages().len(), 1);
    }

    #[test]
    fn only_a_failure_before_the_request_could_leave_is_not_delivered() {
        let message = || "x".to_owned();
        assert_eq!(
            unannounced_fact(&ProviderExecutionError::Build { message: message() }),
            ProviderFact::NotDelivered
        );
        assert_eq!(
            unannounced_fact(&ProviderExecutionError::NotDelivered { message: message() }),
            ProviderFact::NotDelivered
        );
        for error in [
            ProviderExecutionError::Unknown { message: message() },
            ProviderExecutionError::Incomplete { message: message() },
            ProviderExecutionError::Rejected {
                failure: ProviderFailure::default(),
            },
        ] {
            assert_eq!(unannounced_fact(&error), ProviderFact::Delivered, "{error}");
        }
    }

    fn leaked(chunk: String) -> &'static str {
        Box::leak(chunk.into_boxed_str())
    }

    /// One content-free chunk reporting `usage` beside choices.
    fn running_total(usage: &str) -> &'static str {
        leaked(format!(
            "data: {{\"choices\":[{{\"delta\":{{}}}}],\"usage\":{usage}}}\n\n"
        ))
    }

    /// One usage-only chunk, the protocol's final position.
    fn usage_only(usage: &str) -> &'static str {
        leaked(format!("data: {{\"choices\":[],\"usage\":{usage}}}\n\n"))
    }

    #[test]
    fn every_distinct_usage_object_is_handed_over_as_reported_however_many_there_are() {
        // Non-monotonic fields, so no maximum or latest object can stand in for them.
        let objects: Vec<String> = (0..40u32)
            .map(|n| {
                let input = (n * 7) % 13 + 1;
                let output = (n * 5) % 11;
                match n % 3 {
                    0 => format!("{{\"prompt_tokens\":{input},\"completion_tokens\":{output}}}"),
                    1 => format!("{{\"completion_tokens\":{output},\"extra\":{n}}}"),
                    _ => format!(
                        "{{\"prompt_tokens\":{input},\"prompt_tokens_details\":{{\"cached_tokens\":{}}}}}",
                        n % 4
                    ),
                }
            })
            .collect();
        let mut steps: Vec<Step> = Vec::new();
        for object in &objects {
            steps.push(Step::Chunk(running_total(object)));
        }
        // An identical consecutive repeat carries nothing new.
        steps.push(Step::Chunk(running_total(&objects[39])));
        steps.push(Step::Chunk(STOP));
        let (result, sink) = recorded(steps);
        result.expect("a finished stream");
        let usages = sink.usages();
        assert_eq!(
            usages.len(),
            40,
            "no object was merged, dropped or repeated"
        );
        for ((finality, usage), source) in usages.iter().zip(&objects) {
            assert_eq!(*finality, UsageFinality::Cumulative);
            let raw: Value = serde_json::from_str(source).expect("source object");
            assert_eq!(usage.raw.as_ref(), Some(&raw), "the source object is kept");
            assert_eq!(
                counters(usage),
                [
                    raw.get("prompt_tokens")
                        .and_then(Value::as_u64)
                        .map(|v| v as u32),
                    raw.get("completion_tokens")
                        .and_then(Value::as_u64)
                        .map(|v| v as u32),
                    raw.pointer("/prompt_tokens_details/cached_tokens")
                        .and_then(Value::as_u64)
                        .map(|v| v as u32),
                ],
                "its normalization matches that object alone"
            );
        }
    }

    #[test]
    fn many_final_objects_neither_stop_the_stream_nor_get_dropped() {
        let mut steps: Vec<Step> = (0..20)
            .map(|_| {
                Step::Chunk(usage_only(
                    "{\"prompt_tokens\":40,\"completion_tokens\":60}",
                ))
            })
            .collect();
        for n in 0..20 {
            steps.push(Step::Chunk(usage_only(&format!(
                "{{\"prompt_tokens\":40,\"completion_tokens\":{}}}",
                100 + n
            ))));
        }
        steps.push(Step::Chunk(STOP));
        steps.push(Step::Chunk(CONTENT));
        let (result, sink) = recorded(steps);
        let result = result.expect("a stream with many finals still completes");
        assert_eq!(result.text, "hi", "the response was read to its end");
        let usages = sink.usages();
        assert_eq!(usages.len(), 40);
        assert!(
            usages
                .iter()
                .all(|(finality, _)| *finality == UsageFinality::Final)
        );
        assert_eq!(
            usages
                .iter()
                .map(|(_, usage)| usage.output_tokens)
                .collect::<Vec<_>>()[18..22],
            [Some(60), Some(60), Some(100), Some(101)],
            "identical and distinct finals are all kept, in order"
        );
    }

    static TICKS: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);

    fn tick() -> Timestamp {
        Timestamp::from_unix_micros(TICKS.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1)
    }

    /// A sink that takes its own turn on the clock before it keeps an
    /// observation, as a slow sink would let time pass.
    struct SlowSink {
        kept: Vec<(Timestamp, Timestamp)>,
    }

    impl ProviderOutputSink for SlowSink {
        fn append<'a>(
            &'a mut self,
            _text: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<(), OutputSinkError>> + 'a>> {
            Box::pin(async { Ok(()) })
        }

        fn observe<'a>(
            &'a mut self,
            observation: ProviderObservation,
        ) -> Pin<Box<dyn Future<Output = ()> + 'a>> {
            Box::pin(async move {
                let late = tick();
                self.kept.push((observation.observed_at, late));
            })
        }
    }

    #[test]
    fn an_observation_keeps_the_time_it_was_parsed_not_the_time_the_sink_took_it() {
        let config = ProviderExecutionConfig::default();
        let mut sink = SlowSink { kept: Vec::new() };
        let steps = vec![
            Step::Chunk(running_total("{\"prompt_tokens\":1}")),
            Step::Chunk(running_total("{\"prompt_tokens\":2}")),
            Step::Chunk(usage_only("{\"prompt_tokens\":3}")),
            Step::Chunk(STOP),
        ];
        wait(parse_stream(
            payload(steps, config.cancellation.clone()),
            200,
            Instant::now(),
            None,
            &config,
            Some(&mut sink),
            &mut UsageCapture::with_clock(tick),
        ))
        .expect("a finished stream");
        assert_eq!(sink.kept.len(), 3);
        for (observed, taken) in &sink.kept {
            assert_eq!(
                observed.as_unix_micros() + 1,
                taken.as_unix_micros(),
                "the object was timed on the clock just before the sink took its turn"
            );
        }
        let times: Vec<_> = sink.kept.iter().map(|(observed, _)| *observed).collect();
        assert!(
            times.windows(2).all(|pair| pair[0] < pair[1]),
            "each later object is timed after the earlier sink wait"
        );
    }

    /// Keeps observations, but its `stall`th never completes.
    struct StallingSink {
        kept: Vec<ProviderObservation>,
        stall: usize,
    }

    impl ProviderOutputSink for StallingSink {
        fn append<'a>(
            &'a mut self,
            _text: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<(), OutputSinkError>> + 'a>> {
            Box::pin(async { Ok(()) })
        }

        fn observe<'a>(
            &'a mut self,
            observation: ProviderObservation,
        ) -> Pin<Box<dyn Future<Output = ()> + 'a>> {
            Box::pin(async move {
                self.kept.push(observation);
                if self.kept.len() == self.stall {
                    std::future::pending::<()>().await;
                }
            })
        }
    }

    #[test]
    fn the_outer_timeout_keeps_what_was_handed_over_and_invents_no_flush() {
        let steps = vec![
            Step::Chunk(running_total("{\"prompt_tokens\":1}")),
            Step::Chunk(running_total("{\"prompt_tokens\":2}")),
            Step::Chunk(running_total("{\"prompt_tokens\":3}")),
            Step::Stall,
        ];
        let config = ProviderExecutionConfig {
            timeout: Duration::from_millis(100),
            ..ProviderExecutionConfig::default()
        };
        let (result, sink) = ntex::rt::System::new("timeout-test", ntex::rt::DefaultRuntime)
            .block_on(async move {
                let mut sink = StallingSink {
                    kept: Vec::new(),
                    stall: 2,
                };
                let result = read_response(
                    payload(steps, config.cancellation.clone()),
                    200,
                    Instant::now(),
                    None,
                    &config,
                    Some(&mut sink),
                )
                .await;
                (result, sink)
            });
        assert!(matches!(
            result,
            Err(ProviderExecutionError::Unknown { .. })
        ));
        let inputs: Vec<_> = sink
            .kept
            .iter()
            .map(|observation| match &observation.fact {
                ProviderFact::Usage { usage, .. } => usage.input_tokens,
                other => panic!("only usage was observed, got {other:?}"),
            })
            .collect();
        assert_eq!(
            inputs,
            [Some(1), Some(2)],
            "the second write was interrupted and nothing was resent or merged; the third was never read"
        );
    }
}
