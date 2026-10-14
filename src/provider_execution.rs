//! OpenAI-compatible provider execution for an admitted Job.

use std::sync::{
    Arc, Mutex, PoisonError, Weak,
    atomic::{AtomicBool, Ordering},
};
use std::task::{Poll, Waker};
use std::time::{Duration, Instant};
use std::{future::Future, pin::Pin};

use ntex::client::{Client, ClientResponse, error::ClientError};
use ntex::http::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderValue};
use ntex::http::{Payload, StatusCode};
use ntex::time::Millis;
use ntex::util::{Either, select};
use serde_json::{Value, json};

use crate::openai_compatible::{
    ChatConversation, ChatFinishReason, ChatMessage, CompletedToolCall, ContentPartWire,
    ImageInput, InvalidToolCall, NormalizedUsage, ProviderFailure, ToolCallAssembler,
};
use crate::persistence::{FrozenExecutionConfig, LlmParameters, Timestamp};
use crate::token_usage::UsageFinality;

const DEFAULT_ENDPOINT: &str = "https://api.openai.com/v1/chat/completions";
const MAX_SSE_LINE_BYTES: usize = 64 * 1024;
const MAX_PROVIDER_ERROR_BYTES: usize = 64 * 1024;
const MAX_OUTPUT_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(120);
const STREAM_OUTPUT_BUFFER_BYTES: usize = 4 * 1024;
const STREAM_OUTPUT_FLUSH_INTERVAL: Duration = Duration::from_millis(250);

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

/// A cancellation shared by an execution and whoever may stop it. Cancelling
/// wakes every task waiting in [`CancellationToken::cancelled`], so a wait on
/// a stalled Provider ends at once instead of at its timeout. Clones share
/// one cancellation; [`CancellationToken::child`] makes a distinct one that
/// its parent's cancellation reaches.
#[derive(Clone, Debug, Default)]
pub struct CancellationToken(Arc<Cancellation>);

#[derive(Debug, Default)]
struct Cancellation {
    cancelled: AtomicBool,
    waiters: Mutex<Vec<Waker>>,
    /// The children made since the last cancellation, held weakly so a
    /// long-lived parent keeps none that was dropped. Registration and
    /// cancellation both take this lock, so a child is either registered
    /// before the parent's cancellation takes the list, and cancelled with
    /// it, or sees the parent already cancelled.
    children: Mutex<Vec<Weak<Cancellation>>>,
}

impl CancellationToken {
    /// Cancels this token and every child made from it, and theirs.
    pub fn cancel(&self) {
        self.0.cancelled.store(true, Ordering::Release);
        let waiters = std::mem::take(
            &mut *self
                .0
                .waiters
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        );
        for waiter in waiters {
            waiter.wake();
        }
        let children = std::mem::take(
            &mut *self
                .0
                .children
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        );
        for child in children.iter().filter_map(Weak::upgrade) {
            Self(child).cancel();
        }
    }

    /// A new cancellation of its own, cancelled when this one is, already
    /// if this one is now. Cancelling the child cancels neither this token
    /// nor any other child of it.
    pub fn child(&self) -> Self {
        let child = Self::default();
        let mut children = self
            .0
            .children
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if self.is_cancelled() {
            drop(children);
            child.cancel();
            return child;
        }
        children.retain(|registered| registered.strong_count() > 0);
        children.push(Arc::downgrade(&child.0));
        child
    }

    /// How many children this token still registers, live or not yet
    /// pruned.
    #[cfg(test)]
    pub(crate) fn registered_children(&self) -> usize {
        self.0
            .children
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.cancelled.load(Ordering::Acquire)
    }

    /// Resolves once the token is cancelled. Needs no particular async runtime.
    pub async fn cancelled(&self) {
        std::future::poll_fn(|cx| {
            if self.is_cancelled() {
                return Poll::Ready(());
            }
            {
                let mut waiters = self
                    .0
                    .waiters
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                if !waiters.iter().any(|waiter| waiter.will_wake(cx.waker())) {
                    waiters.push(cx.waker().clone());
                }
            }
            // Checked again after registering, so a cancellation between the
            // first check and the registration is not missed.
            if self.is_cancelled() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await
    }
}

/// One finished Provider exchange. It may be assistant text, tool calls, or
/// both. Tool calls are requests the Provider made, complete and admitted by
/// the request that produced them, see [`ChatConversation::admit_tool_calls`];
/// none of them has been run, and a result that carries them is not a
/// finished answer. Admission is not permission to execute a call.
#[derive(Debug, Clone)]
pub struct ProviderExecutionResult {
    pub text: String,
    /// The streamed `reasoning_content`, kept apart from the text and never
    /// published as output.
    pub reasoning: String,
    /// Ordered by their wire index, with ids, names and arguments as sent.
    pub tool_calls: Vec<CompletedToolCall>,
    pub usage: NormalizedUsage,
    pub finish_reason: Option<ChatFinishReason>,
    pub raw_finish_reason: Option<String>,
    pub status_code: u16,
    pub request_id: Option<String>,
    pub ttfb: Duration,
    pub finished_in: Duration,
}

/// The facts of a provider response observed before execution ended
/// abnormally. The status is always present: these facts exist only once
/// the response head arrived. A finish reason is the one an accepted event
/// reported; it is a raw observation, and whether it witnesses a normal
/// termination is decided only where it is classified.
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
    /// The transport failed, or the Provider fell silent, after the request
    /// may have been delivered.
    #[error("provider response failed after possible delivery: {message}")]
    Unknown {
        message: String,
        /// What the response had shown before the failure; `None` when no
        /// response head arrived.
        facts: Option<ResponseFacts>,
        /// Whether the response had already finished before the failure: its
        /// finish reason was accepted and no tool call of it is left
        /// unchecked. Its facts then stand apart from the later failure.
        finished: bool,
    },
    #[error("provider rejected the request: {failure}")]
    Rejected { failure: ProviderFailure },
    /// The response broke the protocol, or what it sent was refused, such
    /// as a malformed or unadmitted tool call. The refusal is about the
    /// response itself, so its facts never stand apart from it.
    #[error("provider stream was incomplete: {message}")]
    Incomplete {
        message: String,
        facts: ResponseFacts,
    },
    /// Execution was cancelled on this side before any response head
    /// arrived, after the request may have been sent. Delivery cannot be
    /// ruled out, and nothing about the Provider was observed: it is neither
    /// a Provider nor a transport fault.
    #[error("provider execution was interrupted locally: {message}")]
    Interrupted { message: String },
    /// Execution stopped on this side after the provider had answered: the
    /// caller cancelled, the response reached the local output limit, or the
    /// runtime could not persist streamed output. None is a provider or
    /// transport fault, so only the response facts already observed are kept.
    #[error("provider execution stopped locally: {message}")]
    Stopped {
        message: String,
        facts: ResponseFacts,
    },
    #[error("{stream}; additionally, streamed output could not be persisted: {message}")]
    OutputPersistence {
        message: String,
        stream: Box<ProviderExecutionError>,
    },
}

impl ProviderExecutionResult {
    /// Whether the Provider asked for tools to be run before it can answer.
    #[must_use]
    pub fn requests_tools(&self) -> bool {
        !self.tool_calls.is_empty()
    }
}

impl ProviderExecutionError {
    pub fn failure(&self) -> Option<&ProviderFailure> {
        match self {
            Self::Rejected { failure } => Some(failure),
            Self::OutputPersistence { stream, .. } => stream.failure(),
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
    let conversation = objective_conversation(frozen, objective);
    execute_conversation(frozen, &conversation, resolver, config, None).await
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

/// Sends one validated conversation, with the tools it offers, to the
/// frozen Provider and model with the frozen parameters, and reads the
/// streamed response. The conversation is the whole message list sent.
/// Tool calls in the response come back typed in the result once they are
/// complete and allowed; anything else about them is an incomplete stream.
/// No tool is ever run here.
pub async fn execute_conversation(
    frozen: &FrozenExecutionConfig,
    conversation: &ChatConversation,
    resolver: &dyn CredentialResolver,
    config: &ProviderExecutionConfig,
    mut sink: Option<&mut dyn ProviderOutputSink>,
) -> Result<ProviderExecutionResult, ProviderExecutionError> {
    let mut announced = false;
    let result = exchange(
        frozen,
        conversation,
        resolver,
        config,
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
        | ProviderExecutionError::Interrupted { .. }
        | ProviderExecutionError::Stopped { .. } => ProviderFact::Delivered,
        ProviderExecutionError::OutputPersistence { stream, .. } => unannounced_fact(stream),
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
    let authorization = credential.as_deref().map(authorization).transpose()?;
    if config.cancellation.is_cancelled() {
        return Err(ProviderExecutionError::NotDelivered {
            message: "provider execution was cancelled before sending".to_owned(),
        });
    }
    let body = request_body(
        &frozen.llm.config.model,
        &frozen.llm.config.parameters,
        conversation,
    )?;
    let client = Client::builder()
        .disable_redirects()
        .response_timeout(Millis(
            config.timeout.as_millis().min(u128::from(u32::MAX)) as u32
        ))
        // The pinned client's payload timeout covers the whole body, not an
        // idle interval. Streaming applies its own timeout to each chunk wait.
        .response_payload_timeout(Millis::ZERO)
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
        .if_some(authorization, |authorization, request| {
            request.set_header(AUTHORIZATION, authorization)
        });

    let started = Instant::now();
    let sent = select(
        config.cancellation.cancelled(),
        ntex::time::timeout(config.timeout, request.send_json(&body)),
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
        .header("x-request-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    if response.status() != StatusCode::OK {
        // Nothing of a refused image-bearing request's body is kept, so it is
        // not read; see `rejected_failure`.
        if conversation.carries_images() {
            return Err(ProviderExecutionError::Rejected {
                failure: rejected_failure(conversation, status, request_id, None),
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
        return Err(ProviderExecutionError::Rejected {
            failure: rejected_failure(conversation, status, request_id, body),
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
}

/// Reads one response stream, then admits its tool calls only as far as
/// `conversation` allowed them. The configured timeout applies only while
/// waiting for the next provider chunk; parsing and persistence are outside
/// that interval.
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
    let result = parse_stream(
        payload,
        status_code,
        started,
        request_id,
        config,
        sink,
        &mut capture,
    )
    .await?;
    if let Err(error) = conversation.admit_tool_calls(&result.tool_calls) {
        return Err(incomplete_tool_calls(
            error,
            ResponseFacts {
                status_code: result.status_code,
                finish_reason: result.finish_reason,
                raw_finish_reason: result.raw_finish_reason,
            },
        ));
    }
    Ok(result)
}

fn incomplete_tool_calls(error: InvalidToolCall, facts: ResponseFacts) -> ProviderExecutionError {
    ProviderExecutionError::Incomplete {
        message: error.to_string(),
        facts,
    }
}

pub async fn execute_chat_with_sink(
    frozen: &FrozenExecutionConfig,
    objective: Option<&str>,
    resolver: &dyn CredentialResolver,
    config: &ProviderExecutionConfig,
    sink: &mut dyn ProviderOutputSink,
) -> Result<ProviderExecutionResult, ProviderExecutionError> {
    let conversation = objective_conversation(frozen, objective);
    execute_conversation(frozen, &conversation, resolver, config, Some(sink)).await
}

/// The Authorization header of a request. It is marked sensitive, so no
/// dump of the request's headers, such as the HTTP client's own request
/// tracing, prints the credential. A credential that is no valid header
/// value is refused without being named.
fn authorization(credential: &str) -> Result<HeaderValue, ProviderExecutionError> {
    let mut value = HeaderValue::try_from(format!("Bearer {credential}")).map_err(|_| {
        ProviderExecutionError::Build {
            message: "the resolved credential is not a valid HTTP header value".to_owned(),
        }
    })?;
    value.set_sensitive(true);
    Ok(value)
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
    if let Some(value) = parameters.presence_penalty {
        object.insert("presence_penalty".to_owned(), json!(value));
    }
    if let Some(value) = parameters.frequency_penalty {
        object.insert("frequency_penalty".to_owned(), json!(value));
    }
    Ok(body)
}

async fn parse_stream(
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
        (Ok(result), Err(error)) => Err(local_stop(
            &error.message,
            status_code,
            result.finish_reason,
            result.raw_finish_reason.as_deref(),
        )),
        (Err(stream), Err(error)) => Err(ProviderExecutionError::OutputPersistence {
            message: error.message,
            stream: Box::new(stream),
        }),
    }
}

struct StreamOutput<'a> {
    sink: Option<&'a mut dyn ProviderOutputSink>,
    text: String,
    started: Option<Instant>,
}

impl<'a> StreamOutput<'a> {
    fn new(sink: Option<&'a mut dyn ProviderOutputSink>) -> Self {
        Self {
            sink,
            text: String::with_capacity(STREAM_OUTPUT_BUFFER_BYTES),
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

    async fn buffer(&mut self, mut delta: &str) -> Result<(), OutputSinkError> {
        if self.sink.is_none() {
            return Ok(());
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
        sink.append(&text).await
    }
}

impl ProviderOutputSink for StreamOutput<'_> {
    fn append<'a>(
        &'a mut self,
        text: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), OutputSinkError>> + 'a>> {
        Box::pin(async move { self.buffer(text).await })
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
    #[cfg(test)]
    if ntex::rt::System::try_current().is_none() {
        return match select(config.cancellation.cancelled(), payload.recv()).await {
            Either::Left(()) => StreamWait::Cancelled,
            Either::Right(chunk) => StreamWait::Chunk(chunk),
        };
    }
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
    let mut saw_event = false;
    let mut first_byte = None;
    let mut idle_waited = Duration::ZERO;

    'stream: loop {
        if idle_waited >= config.timeout {
            return Err(response.unknown(
                "provider stream was idle after it may have been delivered",
                status_code,
            ));
        }
        let wait_started = Instant::now();
        let chunk =
            match next_stream_event(&mut payload, config, output, config.timeout - idle_waited)
                .await
            {
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
            buffer.push(byte);
            if buffer.len() > MAX_SSE_LINE_BYTES {
                return Err(
                    response.incomplete("provider SSE line exceeded its limit", status_code)
                );
            }
            if byte != b'\n' {
                continue;
            }
            let line = std::mem::take(&mut buffer);
            let line = line.strip_suffix(b"\n").unwrap_or(&line);
            let line = line.strip_suffix(b"\r").unwrap_or(line);
            if line.len() > MAX_SSE_LINE_BYTES {
                return Err(
                    response.incomplete("provider SSE line exceeded its limit", status_code)
                );
            }
            let Some(data) = line.strip_prefix(b"data:") else {
                continue;
            };
            let data = trim_ascii(data);
            // `[DONE]` ends the response whether or not the connection
            // closes; whatever follows it is not part of the response.
            if data == b"[DONE]" {
                break 'stream;
            }
            let event: Value = serde_json::from_slice(data).map_err(|error| {
                response.incomplete(
                    format!("malformed provider SSE event: {error}"),
                    status_code,
                )
            })?;
            saw_event = true;
            // The usage is taken before anything else of the event, so
            // neither a refused part of the event nor a failed delta write
            // loses its counters.
            if let Some(reported) = reported_usage(&event) {
                response.usage = reported.usage.clone();
                capture.record(Some(output), reported).await;
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
            if let Some(delta) = delta.as_deref() {
                output.buffer(delta).await.map_err(|error| {
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
    let tool_calls = response
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
        text: response.text,
        reasoning: response.reasoning,
        tool_calls,
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

/// What a response stream has produced so far. Only the text is streamed
/// out; reasoning and tool-call fragments wait for the end of the stream.
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

/// Folds the choice of one event into `response` and returns its text delta.
/// The event's usage is not read here, see [`reported_usage`]. An invalid
/// tool-call fragment rejects the whole choice, so none of its text and no
/// finish reason of it is taken either.
fn consume_event(
    event: &Value,
    response: &mut StreamedResponse,
    max_output_bytes: usize,
) -> Result<Option<String>, Refused> {
    let Some(choice) = event
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|choices| choices.first())
    else {
        return Ok(None);
    };
    let mut text = None;
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
        if let Some(value) = delta.get("reasoning_content").and_then(Value::as_str) {
            if response.output_bytes().saturating_add(value.len()) > max_output_bytes {
                return Err(Refused::OutputLimit);
            }
            response.reasoning.push_str(value);
        }
        if let Some(value) = delta.get("content").and_then(Value::as_str) {
            if response.output_bytes().saturating_add(value.len()) > max_output_bytes {
                return Err(Refused::OutputLimit);
            }
            response.text.push_str(value);
            text = Some(value.to_owned());
        }
    }
    if let Some(raw) = choice.get("finish_reason").and_then(Value::as_str) {
        response.raw_finish_reason = Some(raw.to_owned());
        response.finish_reason = Some(ChatFinishReason::from_openai_str(raw));
    }
    Ok(text)
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

/// What OCG keeps of a non-200 response to `conversation`. A Provider or
/// gateway may echo the request it refuses, and an image-bearing request
/// carries whole images, so of a refusal of one only the HTTP status, and
/// the retryability it implies, is kept: never its body, nor the message or
/// code read from it, nor its request id header. Nothing is redacted; all
/// of it is dropped. A text-only refusal is kept as the Provider gave it.
pub(crate) fn rejected_failure(
    conversation: &ChatConversation,
    status_code: u16,
    request_id: Option<String>,
    response_body: Option<String>,
) -> ProviderFailure {
    if !conversation.carries_images() {
        return provider_failure(status_code, request_id, response_body);
    }
    ProviderFailure {
        message: format!("provider rejected image request (HTTP {status_code})"),
        status_code: Some(status_code),
        retryable: retryable_status(status_code),
        ..ProviderFailure::default()
    }
}

/// Whether a refusal with this status may succeed when sent again.
fn retryable_status(status_code: u16) -> bool {
    matches!(status_code, 408 | 409 | 425 | 429 | 500..=599)
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
        retryable: retryable_status(status_code),
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

/// A failure before any response head: it observed no response fact.
fn classify_client_error(error: ClientError) -> ProviderExecutionError {
    let message = error.to_string();
    match error {
        ClientError::Url(_) | ClientError::Connect(_) => {
            ProviderExecutionError::NotDelivered { message }
        }
        ClientError::Request(_) => ProviderExecutionError::NotDelivered { message },
        ClientError::Send(_)
        | ClientError::Timeout
        | ClientError::Response(_)
        | ClientError::Http(_)
        | ClientError::H2(_)
        | ClientError::TunnelNotSupported
        | ClientError::Error(_) => ProviderExecutionError::Unknown {
            message,
            facts: None,
            finished: false,
        },
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

/// The availability observations an error proves, in the order they were
/// observed; at most two. Only what the provider or transport actually
/// reported becomes a failure: a refusal before delivery proves nothing, and
/// a local stop keeps just the response facts it already had. A failure keeps
/// the response facts observed before it. When the response had finished
/// before an independent transport failure, its facts and the failure are
/// two observations, so the failure is not recorded as part of the finish.
/// Nothing is inferred from an unknown outcome.
pub fn error_observations(
    error: &ProviderExecutionError,
) -> Vec<crate::persistence::AvailabilityObservation> {
    use crate::persistence::AvailabilityObservation;
    let observed =
        |facts: &ResponseFacts, failure: Option<ProviderFailure>| AvailabilityObservation {
            status_code: Some(facts.status_code),
            finish_reason: facts.finish_reason,
            raw_finish_reason: facts.raw_finish_reason.clone(),
            failure,
        };
    let failure = || ProviderFailure {
        message: error.to_string(),
        ..ProviderFailure::default()
    };
    // A failure without any response fact beside it.
    let failure_only = || AvailabilityObservation {
        status_code: None,
        finish_reason: None,
        raw_finish_reason: None,
        failure: Some(failure()),
    };
    match error {
        // A local interruption before any response head observed nothing
        // about the Provider.
        ProviderExecutionError::Build { .. }
        | ProviderExecutionError::NotDelivered { .. }
        | ProviderExecutionError::Interrupted { .. } => Vec::new(),
        ProviderExecutionError::Stopped { facts, .. } => vec![observed(facts, None)],
        ProviderExecutionError::Rejected { failure } => vec![AvailabilityObservation {
            status_code: failure.status_code,
            finish_reason: None,
            raw_finish_reason: None,
            failure: Some(failure.clone()),
        }],
        ProviderExecutionError::Unknown { facts: None, .. } => vec![failure_only()],
        ProviderExecutionError::Unknown {
            facts: Some(facts),
            finished: true,
            ..
        } => vec![observed(facts, None), failure_only()],
        ProviderExecutionError::Unknown {
            facts: Some(facts),
            finished: false,
            ..
        }
        | ProviderExecutionError::Incomplete { facts, .. } => {
            vec![observed(facts, Some(failure()))]
        }
        ProviderExecutionError::OutputPersistence { stream, .. } => error_observations(stream),
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
    use std::pin::Pin;
    use std::time::Duration;

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
        /// Waits before yielding the next scripted step.
        Quiet(Duration),
        /// Yields nothing, ever: only a timeout ends the read.
        Stall,
    }

    struct Body {
        steps: VecDeque<Step>,
        cancellation: CancellationToken,
        quiet: Option<Pin<Box<dyn Future<Output = ()>>>>,
    }

    impl Stream for Body {
        type Item = Result<Bytes, PayloadError>;

        fn poll_next(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            use std::task::Poll;
            if let Some(quiet) = &mut self.quiet {
                if quiet.as_mut().poll(cx).is_pending() {
                    return Poll::Pending;
                }
                self.quiet = None;
            }
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
                    Step::Quiet(duration) => {
                        self.quiet = Some(Box::pin(ntex::time::sleep(duration)));
                        if let Some(quiet) = &mut self.quiet
                            && quiet.as_mut().poll(cx).is_pending()
                        {
                            return Poll::Pending;
                        }
                        self.quiet = None;
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
            quiet: None,
        })
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::time::Instant;

    use super::scripted::{Step, payload};
    use super::*;
    use crate::persistence::{AvailabilityClassification, AvailabilityObservation};
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
    fn a_complete_stream_coalesces_deltas_and_reconstructs_exact_text() {
        let mut sink = RecordingSink::default();
        let result = parse(
            vec![
                Step::Chunk(CONTENT),
                Step::Chunk("data: {\"choices\":[{\"delta\":{\"content\":\" 世界\"}}]}\n\n"),
                Step::Chunk(STOP),
            ],
            Some(&mut sink),
        )
        .expect("a finished stream succeeds");
        assert_eq!(result.text, "hi 世界");
        assert_eq!(sink.deltas, ["hi 世界"]);
    }

    #[test]
    fn a_large_delta_is_split_at_utf8_boundaries_with_a_strict_bound() {
        let content = "世".repeat(STREAM_OUTPUT_BUFFER_BYTES);
        let event = leaked(format!(
            "data: {{\"choices\":[{{\"delta\":{{\"content\":{}}}}}]}}\n\n",
            serde_json::to_string(&content).expect("JSON string")
        ));
        let mut sink = RecordingSink::default();
        let result = parse(vec![Step::Chunk(event), Step::Chunk(STOP)], Some(&mut sink))
            .expect("a finished stream succeeds");
        assert_eq!(sink.deltas.concat(), content);
        assert_eq!(result.text, content);
        assert!(
            sink.deltas
                .iter()
                .all(|delta| delta.len() <= STREAM_OUTPUT_BUFFER_BYTES)
        );
        assert!(sink.deltas.len() > 1);
    }

    #[test]
    fn flush_boundaries_preserve_order() {
        let first = "a".repeat(STREAM_OUTPUT_BUFFER_BYTES - 1);
        let event = |text: &str| {
            leaked(format!(
                "data: {{\"choices\":[{{\"delta\":{{\"content\":{}}}}}]}}\n\n",
                serde_json::to_string(text).expect("JSON string")
            ))
        };
        let mut sink = RecordingSink::default();
        let result = parse(
            vec![
                Step::Chunk(event(&first)),
                Step::Chunk(event("世界")),
                Step::Chunk(event("z")),
                Step::Chunk(STOP),
            ],
            Some(&mut sink),
        )
        .expect("a finished stream succeeds");
        assert_eq!(sink.deltas.concat(), result.text);
        assert_eq!(result.text, format!("{first}世界z"));
        assert!(sink.deltas.len() >= 2);
    }

    #[test]
    fn a_quiet_stream_flushes_pending_output_before_the_next_chunk() {
        let sink =
            ntex::rt::System::new("quiet-flush-test", ntex::rt::DefaultRuntime).block_on(async {
                let config = ProviderExecutionConfig {
                    timeout: Duration::from_secs(2),
                    ..ProviderExecutionConfig::default()
                };
                let mut sink = RecordingSink::default();
                parse_stream(
                    payload(
                        vec![
                            Step::Chunk(CONTENT),
                            Step::Quiet(STREAM_OUTPUT_FLUSH_INTERVAL + Duration::from_millis(100)),
                            Step::Cancel,
                        ],
                        config.cancellation.clone(),
                    ),
                    200,
                    Instant::now(),
                    None,
                    &config,
                    Some(&mut sink),
                    &mut UsageCapture::default(),
                )
                .await
                .expect_err("the scripted cancellation stops the stream");
                sink
            });
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
        let mut sink = RecordingSink::default();
        let error = parse(vec![Step::Chunk(CONTENT), Step::Fail], Some(&mut sink))
            .expect_err("a broken body fails");
        assert!(
            matches!(error, ProviderExecutionError::Unknown { .. }),
            "a transport failure is reported as it occurred, got {error:?}"
        );
        assert_eq!(sink.deltas, ["hi"], "parsed output is flushed on failure");
    }

    #[test]
    fn a_stream_without_a_finish_event_stays_incomplete() {
        let mut sink = RecordingSink::default();
        let error =
            parse(vec![Step::Chunk(CONTENT)], Some(&mut sink)).expect_err("no finish event");
        assert!(matches!(error, ProviderExecutionError::Incomplete { .. }));
        assert_eq!(sink.deltas, ["hi"]);
    }

    #[test]
    fn malformed_sse_flushes_valid_prior_output_without_changing_the_outcome() {
        let mut sink = RecordingSink::default();
        let error = parse(
            vec![Step::Chunk(CONTENT), Step::Chunk("data: {nope}\n\n")],
            Some(&mut sink),
        )
        .expect_err("malformed SSE is incomplete");
        assert!(matches!(error, ProviderExecutionError::Incomplete { .. }));
        assert_eq!(sink.deltas, ["hi"]);
    }

    #[test]
    fn a_flush_failure_reports_both_the_stream_outcome_and_local_persistence_gap() {
        let mut sink = RecordingSink {
            fail: Some("unknown durable outcome"),
            ..RecordingSink::default()
        };
        let error = parse(vec![Step::Chunk(CONTENT), Step::Fail], Some(&mut sink))
            .expect_err("transport and persistence both fail");
        match error {
            ProviderExecutionError::OutputPersistence { message, stream } => {
                assert_eq!(message, "unknown durable outcome");
                assert!(matches!(*stream, ProviderExecutionError::Unknown { .. }));
            }
            other => panic!("both outcomes remain visible, got {other:?}"),
        }
        assert!(
            sink.deltas.is_empty(),
            "the uncertain write is never retried"
        );
    }

    #[test]
    fn output_limit_termination_flushes_only_the_accepted_prefix() {
        let config = ProviderExecutionConfig {
            max_output_bytes: 2,
            ..ProviderExecutionConfig::default()
        };
        let mut sink = RecordingSink::default();
        let error = wait(parse_stream(
            payload(
                vec![
                    Step::Chunk(CONTENT),
                    Step::Chunk("data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n"),
                ],
                config.cancellation.clone(),
            ),
            200,
            Instant::now(),
            None,
            &config,
            Some(&mut sink),
            &mut UsageCapture::default(),
        ))
        .expect_err("the output limit stops the stream");
        assert!(
            matches!(&error, ProviderExecutionError::Stopped { message, facts: seen } if message.contains("exceeded its limit") && *seen == facts(None, None)),
            "the output limit is a local stop, got {error:?}"
        );
        assert_eq!(sink.deltas, ["hi"]);
    }

    #[test]
    fn usage_in_the_event_that_reaches_the_output_limit_is_still_observed() {
        let usage = "{\"prompt_tokens\":9,\"completion_tokens\":3,\"extra\":[1,\"a\"]}";
        let cases = [
            ("text", r#"{"content":"x"}"#),
            ("reasoning", r#"{"reasoning_content":"x"}"#),
            ("tool call", r#"{"tool_calls":[{"index":0,"id":"c"}]}"#),
        ];
        for (case, delta) in cases {
            let config = ProviderExecutionConfig {
                max_output_bytes: 2,
                ..ProviderExecutionConfig::default()
            };
            let mut sink = RecordingSink::default();
            let trigger = leaked(format!(
                "data: {{\"choices\":[{{\"delta\":{delta}}}],\"usage\":{usage}}}\n\n"
            ));
            let error = wait(parse_stream(
                payload(
                    vec![
                        Step::Chunk(CONTENT),
                        Step::Chunk(trigger),
                        Step::Chunk(STOP),
                    ],
                    config.cancellation.clone(),
                ),
                200,
                Instant::now(),
                None,
                &config,
                Some(&mut sink),
                &mut UsageCapture::default(),
            ))
            .expect_err("the output limit stops the stream");
            assert!(
                matches!(&error, ProviderExecutionError::Stopped { facts: seen, .. } if *seen == facts(None, None)),
                "{case}: a local stop that invents no finish, got {error:?}"
            );
            assert_eq!(sink.deltas, ["hi"], "{case}: the accepted prefix is kept");
            let raw: Value = serde_json::from_str(usage).expect("usage object");
            assert_eq!(
                sink.usages(),
                [(
                    UsageFinality::Cumulative,
                    NormalizedUsage {
                        input_tokens: Some(9),
                        output_tokens: Some(3),
                        raw: Some(raw),
                        ..NormalizedUsage::default()
                    }
                )],
                "{case}: the usage of the refused event is observed as reported"
            );
            assert_eq!(
                error_observations(&error)
                    .iter()
                    .map(|observation| observation.failure.is_some())
                    .collect::<Vec<_>>(),
                [false],
                "{case}: a local stop is no Provider failure"
            );
        }
    }

    #[test]
    fn failures_after_the_head_keep_the_facts_they_observed() {
        let tool_call =
            tool_delta(r#"[{"index":0,"id":"c","function":{"name":"f","arguments":"{}"}}]"#);
        let length = "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"length\"}]}\n\n";
        let stop_facts = || Some(facts(Some(ChatFinishReason::Stop), Some("stop")));
        let unknown = |result: Result<ProviderExecutionResult, ProviderExecutionError>| match result
        {
            Err(ProviderExecutionError::Unknown {
                facts, finished, ..
            }) => (facts, finished),
            other => panic!("expected an unknown outcome, got {other:?}"),
        };
        assert_eq!(
            unknown(parse(
                vec![Step::Chunk(CONTENT), Step::Chunk(STOP), Step::Fail],
                None
            )),
            (stop_facts(), true),
            "a reset after the finish keeps the finish, which stands apart"
        );
        assert_eq!(
            unknown(parse(vec![Step::Chunk(CONTENT), Step::Fail], None)),
            (Some(facts(None, None)), false),
            "a reset before the finish keeps only the status"
        );
        assert_eq!(
            unknown(parse(
                vec![Step::Chunk(tool_call), Step::Chunk(STOP), Step::Fail],
                None
            )),
            (stop_facts(), false),
            "a finish beside unchecked tool calls does not stand apart"
        );

        let incomplete =
            |result: Result<ProviderExecutionResult, ProviderExecutionError>| match result {
                Err(ProviderExecutionError::Incomplete { facts, .. }) => facts,
                other => panic!("expected an incomplete stream, got {other:?}"),
            };
        assert_eq!(
            incomplete(parse(
                vec![
                    Step::Chunk(CONTENT),
                    Step::Chunk(STOP),
                    Step::Chunk("data: {nope}\n\n")
                ],
                None
            )),
            facts(Some(ChatFinishReason::Stop), Some("stop")),
            "malformed SSE after the finish keeps the finish"
        );
        assert_eq!(
            incomplete(parse(
                vec![
                    Step::Chunk(CONTENT),
                    Step::Chunk(length),
                    Step::Chunk("data: {")
                ],
                None
            )),
            facts(Some(ChatFinishReason::Length), Some("length")),
            "a cut frame keeps the facts accepted before it"
        );
        assert_eq!(
            incomplete(parse(vec![Step::Chunk(CONTENT)], None)),
            facts(None, None),
            "a stream without a finish invents none"
        );
    }

    #[test]
    fn an_idle_stream_after_its_finish_keeps_the_finish_apart_from_the_silence() {
        let result = ntex::rt::System::new("finished-idle-test", ntex::rt::DefaultRuntime)
            .block_on(async {
                let config = ProviderExecutionConfig {
                    timeout: Duration::from_millis(100),
                    ..ProviderExecutionConfig::default()
                };
                parse_stream(
                    payload(
                        vec![Step::Chunk(CONTENT), Step::Chunk(STOP), Step::Stall],
                        config.cancellation.clone(),
                    ),
                    200,
                    Instant::now(),
                    None,
                    &config,
                    None,
                    &mut UsageCapture::default(),
                )
                .await
            });
        assert!(
            matches!(
                &result,
                Err(ProviderExecutionError::Unknown { facts: Some(seen), finished: true, .. })
                    if *seen == facts(Some(ChatFinishReason::Stop), Some("stop"))
            ),
            "got {result:?}"
        );
    }

    #[test]
    fn a_failure_before_any_response_head_observes_no_response_fact() {
        for error in [ClientError::Timeout, ClientError::TunnelNotSupported] {
            let error = classify_client_error(error);
            assert!(
                matches!(
                    error,
                    ProviderExecutionError::Unknown {
                        facts: None,
                        finished: false,
                        ..
                    }
                ),
                "got {error:?}"
            );
            assert_eq!(
                error_observations(&error)
                    .iter()
                    .map(|observation| (
                        observation.status_code,
                        observation.finish_reason,
                        observation.raw_finish_reason.clone()
                    ))
                    .collect::<Vec<_>>(),
                [(None, None, None)]
            );
        }
    }

    #[test]
    fn only_provider_and_transport_facts_become_availability_observations() {
        let stopped = ProviderExecutionError::Stopped {
            message: "cancelled".to_owned(),
            facts: facts(Some(ChatFinishReason::Stop), Some("stop")),
        };
        assert_eq!(
            error_observations(&stopped),
            [AvailabilityObservation {
                status_code: Some(200),
                finish_reason: Some(ChatFinishReason::Stop),
                raw_finish_reason: Some("stop".to_owned()),
                failure: None,
            }],
            "a local stop keeps the response facts and no failure"
        );
        assert_eq!(
            error_observations(&ProviderExecutionError::Build {
                message: "bad".to_owned()
            }),
            []
        );
        assert_eq!(
            error_observations(&ProviderExecutionError::NotDelivered {
                message: "refused".to_owned()
            }),
            []
        );

        let failed = |error: &ProviderExecutionError| ProviderFailure {
            message: error.to_string(),
            ..ProviderFailure::default()
        };
        let before_head = ProviderExecutionError::Unknown {
            message: "reset".to_owned(),
            facts: None,
            finished: false,
        };
        assert_eq!(
            error_observations(&before_head),
            [AvailabilityObservation {
                status_code: None,
                finish_reason: None,
                raw_finish_reason: None,
                failure: Some(failed(&before_head)),
            }],
            "a failure before any response head invents no response fact"
        );

        let unfinished = ProviderExecutionError::Unknown {
            message: "reset".to_owned(),
            facts: Some(facts(None, None)),
            finished: false,
        };
        assert_eq!(
            error_observations(&unfinished),
            [AvailabilityObservation {
                status_code: Some(200),
                finish_reason: None,
                raw_finish_reason: None,
                failure: Some(failed(&unfinished)),
            }],
            "a failure after the head keeps the status it observed"
        );

        let after_finish = ProviderExecutionError::Unknown {
            message: "reset".to_owned(),
            facts: Some(facts(Some(ChatFinishReason::Stop), Some("stop"))),
            finished: true,
        };
        let observations = error_observations(&after_finish);
        assert_eq!(
            observations,
            [
                AvailabilityObservation {
                    status_code: Some(200),
                    finish_reason: Some(ChatFinishReason::Stop),
                    raw_finish_reason: Some("stop".to_owned()),
                    failure: None,
                },
                AvailabilityObservation {
                    status_code: None,
                    finish_reason: None,
                    raw_finish_reason: None,
                    failure: Some(failed(&after_finish)),
                },
            ],
            "a finish observed before a later transport failure is not rewritten to contain it"
        );

        let refused = ProviderExecutionError::Incomplete {
            message: "invalid provider tool call: tool call 0 has no id".to_owned(),
            facts: facts(Some(ChatFinishReason::Stop), Some("stop")),
        };
        let observations = error_observations(&refused);
        assert_eq!(
            observations,
            [AvailabilityObservation {
                status_code: Some(200),
                finish_reason: Some(ChatFinishReason::Stop),
                raw_finish_reason: Some("stop".to_owned()),
                failure: Some(failed(&refused)),
            }],
            "a refused response keeps its facts beside the refusal"
        );
        assert_eq!(
            observations[0].classification(),
            AvailabilityClassification::Unconfirmed,
            "and is never a normal-termination witness"
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
        assert!(matches!(
            error,
            ProviderExecutionError::OutputPersistence {
                stream,
                ..
            } if matches!(*stream, ProviderExecutionError::Incomplete { .. })
        ));
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
            ProviderExecutionError::Unknown {
                message: message(),
                facts: None,
                finished: false,
            },
            ProviderExecutionError::Incomplete {
                message: message(),
                facts: facts(None, None),
            },
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

    struct SlowOutputSink {
        deltas: Vec<String>,
    }

    impl ProviderOutputSink for SlowOutputSink {
        fn append<'a>(
            &'a mut self,
            text: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<(), OutputSinkError>> + 'a>> {
            Box::pin(async move {
                ntex::time::sleep(Duration::from_millis(250)).await;
                self.deltas.push(text.to_owned());
                Ok(())
            })
        }

        fn observe<'a>(
            &'a mut self,
            _observation: ProviderObservation,
        ) -> Pin<Box<dyn Future<Output = ()> + 'a>> {
            Box::pin(async {})
        }
    }

    #[test]
    fn local_persistence_time_is_not_provider_idle_time() {
        let steps = vec![Step::Chunk(CONTENT), Step::Chunk(STOP)];
        let config = ProviderExecutionConfig {
            timeout: Duration::from_millis(100),
            ..ProviderExecutionConfig::default()
        };
        let started = Instant::now();
        let (result, sink) = ntex::rt::System::new("timeout-test", ntex::rt::DefaultRuntime)
            .block_on(async move {
                let mut sink = SlowOutputSink { deltas: Vec::new() };
                let result = parse_stream(
                    payload(steps, config.cancellation.clone()),
                    200,
                    Instant::now(),
                    None,
                    &config,
                    Some(&mut sink),
                    &mut UsageCapture::default(),
                )
                .await;
                (result, sink)
            });
        assert!(
            result.is_ok(),
            "local persistence does not trip provider idle"
        );
        assert!(started.elapsed() >= Duration::from_millis(200));
        assert_eq!(sink.deltas, ["hi"]);
    }

    #[test]
    fn a_silent_stream_exceeding_the_idle_timeout_is_unknown_and_flushes_pending_output() {
        let (result, sink) = ntex::rt::System::new("idle-timeout-test", ntex::rt::DefaultRuntime)
            .block_on(async {
                let config = ProviderExecutionConfig {
                    timeout: Duration::from_millis(100),
                    ..ProviderExecutionConfig::default()
                };
                let mut sink = RecordingSink::default();
                let result = parse_stream(
                    payload(
                        vec![Step::Chunk(CONTENT), Step::Stall],
                        config.cancellation.clone(),
                    ),
                    200,
                    Instant::now(),
                    None,
                    &config,
                    Some(&mut sink),
                    &mut UsageCapture::default(),
                )
                .await;
                (result, sink)
            });
        assert!(matches!(
            result,
            Err(ProviderExecutionError::Unknown { .. })
        ));
        assert_eq!(sink.deltas, ["hi"]);
    }

    #[test]
    fn repeated_chunks_within_the_idle_limit_can_exceed_the_old_total_deadline() {
        let (result, sink) = ntex::rt::System::new("long-stream-test", ntex::rt::DefaultRuntime)
            .block_on(async {
                let config = ProviderExecutionConfig {
                    timeout: Duration::from_millis(100),
                    ..ProviderExecutionConfig::default()
                };
                let mut sink = RecordingSink::default();
                let result = parse_stream(
                    payload(
                        vec![
                            Step::Chunk(CONTENT),
                            Step::Quiet(Duration::from_millis(70)),
                            Step::Chunk(CONTENT),
                            Step::Quiet(Duration::from_millis(70)),
                            Step::Chunk(STOP),
                        ],
                        config.cancellation.clone(),
                    ),
                    200,
                    Instant::now(),
                    None,
                    &config,
                    Some(&mut sink),
                    &mut UsageCapture::default(),
                )
                .await;
                (result, sink)
            });
        let result = result.expect("each provider interval stayed within the idle limit");
        assert_eq!(result.text, "hihi");
        assert_eq!(sink.deltas.concat(), "hihi");
    }

    /// Reads a scripted body with the real reader, a short idle timeout and
    /// timers running, so a stream that is never ended is reported as idle.
    fn read_with_timers(
        steps: Vec<Step>,
        conversation: ChatConversation,
    ) -> (
        Result<ProviderExecutionResult, ProviderExecutionError>,
        RecordingSink,
    ) {
        ntex::rt::System::new("done-test", ntex::rt::DefaultRuntime).block_on(async move {
            let config = ProviderExecutionConfig {
                timeout: Duration::from_millis(100),
                ..ProviderExecutionConfig::default()
            };
            let mut sink = RecordingSink::default();
            let result = read_response(
                payload(steps, config.cancellation.clone()),
                200,
                Instant::now(),
                None,
                &config,
                Some(&mut sink),
                &conversation,
            )
            .await;
            (result, sink)
        })
    }

    #[test]
    fn done_ends_the_response_though_the_connection_stays_open() {
        let (result, sink) = read_with_timers(
            vec![
                Step::Chunk(CONTENT),
                Step::Chunk(STOP),
                Step::Chunk(FINAL_40_60),
                Step::Chunk(DONE),
                Step::Stall,
            ],
            ChatConversation::default(),
        );
        let result = result.expect("the response ended at [DONE]");
        assert_eq!(result.text, "hi");
        assert_eq!(result.finish_reason, Some(ChatFinishReason::Stop));
        assert_eq!(result.usage.input_tokens, Some(40));
        assert_eq!(sink.deltas, ["hi"], "the output is flushed");
        assert_eq!(
            sink.usages()
                .iter()
                .map(|(finality, _)| *finality)
                .collect::<Vec<_>>(),
            [UsageFinality::Final]
        );

        // `[DONE]` inside a transport chunk, and split across two; nothing
        // after it is read.
        for steps in [
            vec![
                Step::Chunk(CONTENT),
                Step::Chunk(leaked(format!("{STOP}{DONE}data: {{nope}}\n\n"))),
                Step::Stall,
            ],
            vec![
                Step::Chunk(CONTENT),
                Step::Chunk(STOP),
                Step::Chunk("data: [DO"),
                Step::Chunk("NE]\n\ndata: {nope}\n\n"),
                Step::Stall,
            ],
        ] {
            let (result, sink) = read_with_timers(steps, ChatConversation::default());
            let result = result.expect("the response ended at [DONE]");
            assert_eq!(result.text, "hi");
            assert_eq!(sink.deltas, ["hi"]);
        }

        let (result, _) = read_with_timers(
            vec![
                Step::Chunk(tool_delta(
                    r#"[{"index":0,"id":"c","function":{"name":"f","arguments":"{}"}}]"#,
                )),
                Step::Chunk(TOOL_CALLS),
                Step::Chunk(DONE),
                Step::Stall,
            ],
            offering(&["f"]),
        );
        assert_eq!(
            result.expect("tool calls ended at [DONE]").tool_calls,
            [call(0, "c", "f", "{}")]
        );

        // Without `[DONE]` the end of the stream still ends the response.
        let (result, _) = read_with_timers(
            vec![Step::Chunk(CONTENT), Step::Chunk(STOP)],
            ChatConversation::default(),
        );
        assert_eq!(result.expect("a finished stream").text, "hi");
    }

    #[test]
    fn done_completes_nothing_the_response_did_not() {
        let partial =
            tool_delta(r#"[{"index":0,"id":"c","function":{"name":"f","arguments":"{\"a\":"}}]"#);
        let cases: [(&str, Vec<Step>, &str); 3] = [
            (
                "no finish event",
                vec![Step::Chunk(CONTENT), Step::Chunk(DONE), Step::Stall],
                "without a finish event",
            ),
            (
                "an incomplete tool call",
                vec![
                    Step::Chunk(partial),
                    Step::Chunk(TOOL_CALLS),
                    Step::Chunk(DONE),
                    Step::Stall,
                ],
                "not one JSON object",
            ),
            (
                "a tool that was not offered",
                vec![
                    Step::Chunk(tool_delta(
                        r#"[{"index":0,"id":"c","function":{"name":"g","arguments":"{}"}}]"#,
                    )),
                    Step::Chunk(TOOL_CALLS),
                    Step::Chunk(DONE),
                    Step::Stall,
                ],
                "invalid provider tool call",
            ),
        ];
        for (case, steps, expected) in cases {
            let (result, _) = read_with_timers(steps, offering(&["f"]));
            let message = incomplete_message(result);
            assert!(message.contains(expected), "{case}: {message}");
        }

        let (result, sink) = read_with_timers(
            vec![Step::Chunk(CONTENT), Step::Fail, Step::Chunk(DONE)],
            ChatConversation::default(),
        );
        assert!(
            matches!(result, Err(ProviderExecutionError::Unknown { .. })),
            "a transport failure before [DONE] stays unknown, got {result:?}"
        );
        assert_eq!(sink.deltas, ["hi"]);

        let (result, _) = read_with_timers(
            vec![
                Step::Chunk(CONTENT),
                Step::Chunk(STOP),
                Step::Cancel,
                Step::Chunk(DONE),
            ],
            ChatConversation::default(),
        );
        assert!(
            matches!(&result, Err(ProviderExecutionError::Stopped { facts: seen, .. }) if *seen == facts(Some(ChatFinishReason::Stop), Some("stop"))),
            "a cancellation before [DONE] is a local stop, got {result:?}"
        );
    }

    #[test]
    fn a_child_is_cancelled_by_its_parent_and_by_nothing_else() {
        let parent = CancellationToken::default();
        let (first, second) = (parent.child(), parent.child());
        let grandchild = first.child();
        first.cancel();
        assert!(first.is_cancelled() && grandchild.is_cancelled());
        assert!(
            !parent.is_cancelled() && !second.is_cancelled(),
            "a child's cancellation reaches neither its parent nor a sibling"
        );
        let waiter = second.clone();
        let waiting = std::thread::spawn(move || wait(waiter.cancelled()));
        parent.cancel();
        assert!(second.is_cancelled());
        waiting.join().expect("the child's waiter was woken");
        assert!(
            parent.child().is_cancelled(),
            "a child of a cancelled parent starts cancelled"
        );
        assert_eq!(parent.registered_children(), 0);
    }

    #[test]
    fn a_cancellation_racing_child_creation_reaches_every_child() {
        for _ in 0..200 {
            let parent = CancellationToken::default();
            let start = Arc::new(std::sync::Barrier::new(2));
            let (making, maker_start) = (parent.clone(), start.clone());
            let making = std::thread::spawn(move || {
                maker_start.wait();
                (0..50).map(|_| making.child()).collect::<Vec<_>>()
            });
            start.wait();
            parent.cancel();
            let children = making.join().expect("children made");
            assert!(
                children.iter().all(CancellationToken::is_cancelled),
                "no child escapes a cancellation made while it was created"
            );
        }
    }

    #[test]
    fn a_parent_keeps_no_child_that_was_dropped() {
        let parent = CancellationToken::default();
        for _ in 0..1_000 {
            drop(parent.child());
        }
        let live = parent.child();
        assert_eq!(
            parent.registered_children(),
            1,
            "dropped children are pruned when another registers"
        );
        let weak = Arc::downgrade(&live.0);
        drop(live);
        assert!(
            weak.upgrade().is_none(),
            "the parent holds its children weakly"
        );
        parent.cancel();
        assert_eq!(parent.registered_children(), 0);
    }

    #[test]
    fn a_cancellation_wakes_every_waiter_at_once() {
        let token = CancellationToken::default();
        let waiter = token.clone();
        let waiting = std::thread::spawn(move || wait(waiter.cancelled()));
        std::thread::sleep(Duration::from_millis(20));
        assert!(!waiting.is_finished(), "nothing was cancelled yet");
        token.cancel();
        waiting.join().expect("the waiter woke");
        assert!(token.is_cancelled());
        wait(token.cancelled());
    }

    #[test]
    fn a_cancellation_ends_a_stalled_stream_at_once_with_the_facts_it_had() {
        let config = ProviderExecutionConfig::default();
        assert!(config.timeout >= Duration::from_secs(60));
        let cancellation = config.cancellation.clone();
        let canceller = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            cancellation.cancel();
        });
        let started = Instant::now();
        let mut sink = RecordingSink::default();
        let result = wait(parse_stream(
            payload(
                vec![Step::Chunk(CONTENT), Step::Stall],
                config.cancellation.clone(),
            ),
            200,
            Instant::now(),
            None,
            &config,
            Some(&mut sink),
            &mut UsageCapture::default(),
        ));
        canceller.join().expect("cancelled");
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the stalled read did not wait for its timeout"
        );
        assert!(
            matches!(&result, Err(ProviderExecutionError::Stopped { facts: seen, .. }) if *seen == facts(None, None)),
            "a cancelled stream is a local stop, got {result:?}"
        );
        assert_eq!(
            sink.deltas,
            ["hi"],
            "what arrived before the stall was kept"
        );
    }

    #[test]
    fn an_interruption_before_any_response_observes_nothing_but_cannot_rule_out_delivery() {
        let interrupted = ProviderExecutionError::Interrupted {
            message: "provider execution was cancelled before a response arrived".into(),
        };
        assert_eq!(error_observations(&interrupted), []);
        assert_eq!(unannounced_fact(&interrupted), ProviderFact::Delivered);
    }

    #[test]
    fn the_authorization_header_is_sensitive_and_no_header_dump_prints_it() {
        let value = authorization("sk-test-secret").expect("a valid credential");
        assert!(value.is_sensitive());
        assert_eq!(value.to_str().ok(), Some("Bearer sk-test-secret"));
        let mut headers = ntex::http::HeaderMap::new();
        headers.insert(AUTHORIZATION, value);
        let dumped = format!("{headers:?}");
        assert!(!dumped.contains("sk-test-secret"), "{dumped}");

        match authorization("sk-test\nsecret") {
            Err(ProviderExecutionError::Build { message }) => {
                assert!(
                    !message.contains("sk-test"),
                    "the credential is not named: {message}"
                )
            }
            other => panic!("an invalid credential is refused, got {other:?}"),
        }
    }

    // ---- Tool calling ----------------------------------------------------

    use crate::openai_compatible::{
        ChatToolCallWire, FunctionDefinitionWire, MAX_TOOL_ARGUMENTS_BYTES, MAX_TOOL_CALLS,
        MAX_TOOLS, NamedFunctionWire, NamedToolChoiceWire, ToolChoiceWire, ToolWire,
    };

    const TOOL_CALLS: &str =
        "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n";

    /// One chunk whose delta carries the given `tool_calls` array.
    fn tool_delta(calls: &str) -> &'static str {
        leaked(format!(
            "data: {{\"choices\":[{{\"delta\":{{\"tool_calls\":{calls}}}}}]}}\n\n"
        ))
    }

    fn tool(name: &str) -> ToolWire {
        ToolWire {
            tool_type: "function".to_owned(),
            function: FunctionDefinitionWire {
                name: name.to_owned(),
                description: Some(format!("the {name} tool")),
                parameters: Some(json!({"type": "object", "properties": {}})),
                strict: None,
            },
        }
    }

    /// A conversation offering the named tools.
    fn offering(names: &[&str]) -> ChatConversation {
        ChatConversation {
            messages: vec![ChatMessage::user("weather?")],
            tools: names.iter().map(|name| tool(name)).collect(),
            ..ChatConversation::default()
        }
    }

    /// Reads a scripted body for `conversation` with the real reader.
    fn read(
        steps: Vec<Step>,
        conversation: &ChatConversation,
        sink: Option<&mut dyn ProviderOutputSink>,
    ) -> Result<ProviderExecutionResult, ProviderExecutionError> {
        let config = ProviderExecutionConfig::default();
        wait(read_response(
            payload(steps, config.cancellation.clone()),
            200,
            Instant::now(),
            None,
            &config,
            sink,
            conversation,
        ))
    }

    fn call(index: u32, id: &str, name: &str, arguments: &str) -> CompletedToolCall {
        CompletedToolCall {
            index,
            id: id.to_owned(),
            name: name.to_owned(),
            arguments: arguments.to_owned(),
        }
    }

    fn incomplete_message(
        result: Result<ProviderExecutionResult, ProviderExecutionError>,
    ) -> String {
        match result {
            Err(ProviderExecutionError::Incomplete { message, .. }) => message,
            other => panic!("expected an incomplete stream, got {other:?}"),
        }
    }

    #[test]
    fn a_text_only_response_carries_no_tool_calls_or_reasoning() {
        let mut sink = RecordingSink::default();
        let result = read(
            vec![Step::Chunk(CONTENT), Step::Chunk(STOP)],
            &offering(&["get_weather"]),
            Some(&mut sink),
        )
        .expect("a finished text stream");
        assert_eq!(result.text, "hi");
        assert_eq!(sink.deltas, ["hi"]);
        assert!(result.tool_calls.is_empty());
        assert!(!result.requests_tools());
        assert_eq!(result.reasoning, "");
        assert_eq!(
            result_value(&result),
            json!({"text": "hi", "finish_reason": "stop"})
        );
    }

    #[test]
    fn a_single_tool_call_is_reconstructed_with_its_wire_identity() {
        let mut sink = RecordingSink::default();
        let result = read(
            vec![
                Step::Chunk(tool_delta(
                    r#"[{"index":0,"id":"call_1","type":"function","function":{"name":"get_weather","arguments":""}}]"#,
                )),
                Step::Chunk(tool_delta(
                    r#"[{"index":0,"function":{"arguments":"{\"city\":"}}]"#,
                )),
                Step::Chunk(tool_delta(
                    r#"[{"index":0,"function":{"arguments":" \"Paris\"}"}}]"#,
                )),
                Step::Chunk(TOOL_CALLS),
            ],
            &offering(&["get_weather"]),
            Some(&mut sink),
        )
        .expect("a complete tool call");
        assert_eq!(
            result.tool_calls,
            [call(0, "call_1", "get_weather", "{\"city\": \"Paris\"}")],
            "the arguments are kept exactly as sent, spacing included"
        );
        assert!(result.requests_tools());
        assert_eq!(result.text, "");
        assert!(
            sink.deltas.is_empty(),
            "no tool fragment is published as output"
        );
        assert_eq!(result.finish_reason, Some(ChatFinishReason::ToolCalls));
        assert_eq!(result.raw_finish_reason.as_deref(), Some("tool_calls"));
    }

    #[test]
    fn interleaved_calls_are_kept_apart_by_their_wire_index() {
        let result = read(
            vec![
                Step::Chunk(tool_delta(
                    r#"[{"index":1,"id":"call_b","function":{"name":"get_time","arguments":"{\"tz\""}}]"#,
                )),
                Step::Chunk(tool_delta(
                    r#"[{"index":0,"id":"call_a","function":{"name":"get_weather","arguments":"{\"city\""}}]"#,
                )),
                Step::Chunk(tool_delta(
                    r#"[{"index":1,"function":{"arguments":":\"UTC\"}"}},{"index":0,"function":{"arguments":":\"Oslo\"}"}}]"#,
                )),
                Step::Chunk(TOOL_CALLS),
            ],
            &offering(&["get_weather", "get_time"]),
            None,
        )
        .expect("two complete tool calls");
        assert_eq!(
            result.tool_calls,
            [
                call(0, "call_a", "get_weather", "{\"city\":\"Oslo\"}"),
                call(1, "call_b", "get_time", "{\"tz\":\"UTC\"}"),
            ],
            "ordered by index, not by arrival"
        );
    }

    #[test]
    fn late_metadata_reconstructs_one_call_named_in_one_field() {
        let result = read(
            vec![
                // Arguments arrive before the call's id and name.
                Step::Chunk(tool_delta(r#"[{"index":0,"function":{"arguments":"{\"q\":"}}]"#)),
                Step::Chunk(tool_delta(
                    r#"[{"index":0,"id":"call_9","type":"function","function":{"name":"web_search"}}]"#,
                )),
                // Continuations repeat the id, and carry an empty name or none.
                Step::Chunk(tool_delta(
                    r#"[{"index":0,"id":"call_9","function":{"name":"","arguments":"\"世界\""}}]"#,
                )),
                Step::Chunk(tool_delta(r#"[{"index":0,"function":{"name":null,"arguments":"}"}}]"#)),
                Step::Chunk(TOOL_CALLS),
            ],
            &offering(&["web_search"]),
            None,
        )
        .expect("a complete tool call");
        assert_eq!(
            result.tool_calls,
            [call(0, "call_9", "web_search", "{\"q\":\"世界\"}")]
        );
    }

    #[test]
    fn a_second_name_field_is_an_ambiguous_identity_and_admits_nothing() {
        let named = |name: &str, arguments: &str| {
            tool_delta(leaked(format!(
                r#"[{{"index":0,"id":"call_1","function":{{"name":"{name}","arguments":{}}}}}]"#,
                serde_json::to_string(arguments).expect("JSON string")
            )))
        };
        let cases: [(&str, Vec<Step>, &[&str]); 5] = [
            (
                "`ab` then `ab`, with `ab` and `abab` both offered",
                vec![Step::Chunk(named("ab", "")), Step::Chunk(named("ab", "{}"))],
                &["ab", "abab"],
            ),
            (
                "a complete name echoed beside every argument fragment",
                vec![
                    Step::Chunk(named("get_weather", "")),
                    Step::Chunk(named("get_weather", "{\"city\":")),
                    Step::Chunk(named("get_weather", "\"Paris\"}")),
                ],
                &["get_weather"],
            ),
            (
                "a name sent in two fragments",
                vec![
                    Step::Chunk(named("web_", "")),
                    Step::Chunk(named("search", "{}")),
                ],
                &["web_", "search", "web_search"],
            ),
            (
                "another complete name",
                vec![
                    Step::Chunk(named("get_weather", "{}")),
                    Step::Chunk(named("get_time", "")),
                ],
                &["get_weather", "get_time"],
            ),
            (
                "two name fields for one index in one delta",
                vec![Step::Chunk(tool_delta(
                    r#"[{"index":0,"id":"c","function":{"name":"f","arguments":"{}"}},{"index":0,"function":{"name":"f"}}]"#,
                ))],
                &["f"],
            ),
        ];
        for (case, mut steps, offered) in cases {
            steps.push(Step::Chunk(TOOL_CALLS));
            let message = incomplete_message(read(steps, &offering(offered), None));
            assert!(
                message.contains("(ambiguous identity)") && message.contains("tool call 0"),
                "{case}: {message}"
            );
        }

        // Each index names its function once; interleaved calls stay apart.
        let result = read(
            vec![
                Step::Chunk(tool_delta(
                    r#"[{"index":1,"id":"b","function":{"name":"abab","arguments":"{"}},{"index":0,"id":"a","function":{"name":"ab","arguments":"{"}}]"#,
                )),
                Step::Chunk(tool_delta(
                    r#"[{"index":0,"id":"a","function":{"arguments":"}"}},{"index":1,"id":"b","function":{"arguments":"}"}}]"#,
                )),
                Step::Chunk(TOOL_CALLS),
            ],
            &offering(&["ab", "abab"]),
            None,
        )
        .expect("two unambiguous calls");
        assert_eq!(
            result.tool_calls,
            [call(0, "a", "ab", "{}"), call(1, "b", "abab", "{}")]
        );
    }

    #[test]
    fn malformed_and_boundary_names_fail_closed() {
        let longest = "n".repeat(64);
        let result = read(
            vec![
                Step::Chunk(tool_delta(leaked(format!(
                    r#"[{{"index":0,"id":"c","function":{{"name":"{longest}","arguments":"{{}}"}}}}]"#
                )))),
                Step::Chunk(TOOL_CALLS),
            ],
            &offering(&[&longest]),
            None,
        )
        .expect("a name at the limit");
        assert_eq!(result.tool_calls, [call(0, "c", &longest, "{}")]);

        let cases = [
            (
                "a name that is not a string",
                r#"[{"index":0,"id":"c","function":{"name":7,"arguments":"{}"}}]"#,
                "(malformed)",
            ),
            (
                "a name outside the function-name alphabet",
                r#"[{"index":0,"id":"c","function":{"name":"get weather","arguments":"{}"}}]"#,
                "(malformed)",
            ),
            (
                "a name longer than the limit",
                r#"[{"index":0,"id":"c","function":{"name":"nnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn","arguments":"{}"}}]"#,
                "(limit exceeded)",
            ),
            (
                "no name at all",
                r#"[{"index":0,"id":"c","function":{"arguments":"{}"}}]"#,
                "(incomplete)",
            ),
            (
                "a name that was not offered",
                r#"[{"index":0,"id":"c","function":{"name":"get_time","arguments":"{}"}}]"#,
                "(unauthorized)",
            ),
            (
                "an offered name with different case",
                r#"[{"index":0,"id":"c","function":{"name":"Get_Weather","arguments":"{}"}}]"#,
                "(unauthorized)",
            ),
        ];
        for (case, calls, expected) in cases {
            let message = incomplete_message(read(
                vec![Step::Chunk(tool_delta(calls)), Step::Chunk(TOOL_CALLS)],
                &offering(&["get_weather"]),
                None,
            ));
            assert!(message.contains(expected), "{case}: {message}");
            assert!(!message.contains("get weather"), "{case}: {message}");
        }
    }

    /// A conversation offering one function `name` with `parameters`.
    fn offering_schema(name: &str, parameters: Option<Value>) -> ChatConversation {
        ChatConversation {
            tools: vec![ToolWire {
                function: FunctionDefinitionWire {
                    parameters,
                    ..tool(name).function
                },
                ..tool(name)
            }],
            ..offering(&[])
        }
    }

    /// One complete call to `name` with `arguments`, finished with `tool_calls`.
    fn calling(name: &str, arguments: &str) -> Vec<Step> {
        vec![
            Step::Chunk(tool_delta(leaked(format!(
                r#"[{{"index":0,"id":"c","function":{{"name":"{name}","arguments":{}}}}}]"#,
                serde_json::to_string(arguments).expect("JSON string")
            )))),
            Step::Chunk(TOOL_CALLS),
        ]
    }

    #[test]
    fn arguments_must_satisfy_the_offered_parameter_schema() {
        let booking = offering_schema(
            "book",
            Some(json!({
                "type": "object",
                "properties": {
                    "city": {"type": "string", "minLength": 1},
                    "nights": {"type": "integer", "minimum": 1, "maximum": 30},
                    "class": {"enum": ["economy", "business"]},
                    "guests": {
                        "type": "array",
                        "maxItems": 4,
                        "items": {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string"},
                                "age": {"type": "integer", "minimum": 0}
                            },
                            "required": ["name"],
                            "additionalProperties": false
                        }
                    }
                },
                "required": ["city", "nights"],
                "additionalProperties": false
            })),
        );
        // Kept exactly as sent: spacing and key order included.
        let valid = r#"{ "nights": 2, "city": "Oslo", "class": "economy", "guests": [{"name": "Ada", "age": 36}] }"#;
        let result = read(calling("book", valid), &booking, None).expect("valid arguments");
        assert_eq!(result.tool_calls, [call(0, "c", "book", valid)]);

        let secret = "sk-live-not-to-be-echoed";
        let cases = [
            (
                "a missing required property",
                r#"{"city":"Oslo"}"#,
                "/required",
            ),
            (
                "a wrong type",
                r#"{"city":"Oslo","nights":"2"}"#,
                "/properties/nights/type",
            ),
            (
                "a string where a number is required",
                r#"{"city":"Oslo","nights":"two"}"#,
                "/type",
            ),
            (
                "a value out of range",
                r#"{"city":"Oslo","nights":31}"#,
                "/maximum",
            ),
            (
                "a value outside an enum",
                r#"{"city":"Oslo","nights":2,"class":"first"}"#,
                "/enum",
            ),
            (
                "an unexpected property",
                leaked(format!(
                    r#"{{"city":"Oslo","nights":2,"token":"{secret}"}}"#
                )),
                "/additionalProperties",
            ),
            (
                "a nested object missing its required property",
                r#"{"city":"Oslo","nights":2,"guests":[{"age":3}]}"#,
                "/guests/items/required",
            ),
            (
                "a nested unexpected property",
                leaked(format!(
                    r#"{{"city":"Oslo","nights":2,"guests":[{{"name":"Ada","pin":"{secret}"}}]}}"#
                )),
                "/guests/items/additionalProperties",
            ),
            (
                "a nested wrong type",
                r#"{"city":"Oslo","nights":2,"guests":[{"name":"Ada","age":-1}]}"#,
                "/age/minimum",
            ),
            (
                "too many array items",
                r#"{"city":"Oslo","nights":2,"guests":[{"name":"a"},{"name":"b"},{"name":"c"},{"name":"d"},{"name":"e"}]}"#,
                "/maxItems",
            ),
            ("an array, not an object", "[1]", "not one JSON object"),
            ("malformed JSON", r#"{"city":"#, "not one JSON object"),
        ];
        for (case, arguments, expected) in cases {
            let error = read(calling("book", arguments), &booking, None)
                .expect_err("invalid arguments are refused");
            let ProviderExecutionError::Incomplete {
                message,
                facts: seen,
            } = &error
            else {
                panic!("{case}: expected an incomplete stream, got {error:?}");
            };
            assert!(
                message.contains("(invalid arguments)") && message.contains("tool call 0"),
                "{case}: {message}"
            );
            assert!(message.contains(expected), "{case}: {message}");
            assert!(
                !message.contains(secret),
                "{case}: an argument value is echoed: {message}"
            );
            assert_eq!(
                *seen,
                facts(Some(ChatFinishReason::ToolCalls), Some("tool_calls")),
                "{case}: the response facts are kept"
            );
            let observations = error_observations(&error);
            assert_eq!(observations.len(), 1, "{case}");
            assert_eq!(
                observations[0].classification(),
                AvailabilityClassification::Unconfirmed,
                "{case}: a refused call is neither available evidence nor a Provider failure"
            );
        }
    }

    #[test]
    fn arguments_are_admitted_only_as_strict_i_json() {
        let open = offering_schema("f", Some(json!({"type": "object"})));
        for arguments in [
            "{}",
            r#"{"a":0.1,"b":-0,"c":1e2,"d":2.50,"e":-1.5E-7}"#,
            r#"{"s":"\"}{\\","n":[1,{"m":2}]}"#,
            // The safe-integer bounds, in every notation.
            r#"{"a":9007199254740991,"b":-9007199254740991}"#,
            r#"{"a":9007199254740991.0,"b":-9007199254740991.0}"#,
            r#"{"a":9.007199254740991e15,"b":-9.007199254740991E+15,"c":90071992547409910e-1}"#,
        ] {
            let result = read(calling("f", arguments), &open, None)
                .unwrap_or_else(|error| panic!("{arguments}: {error:?}"));
            assert_eq!(result.tool_calls[0].arguments, arguments);
        }
        let cases = [
            ("a duplicate key", r#"{"a":1,"a":2}"#, "key twice"),
            (
                "a nested duplicate key",
                r#"{"a":{"b":1,"b":1}}"#,
                "key twice",
            ),
            (
                "an integer beyond u64",
                r#"{"a":18446744073709551616}"#,
                "not exactly",
            ),
            (
                "the integer just past the safe range",
                r#"{"a":9007199254740992}"#,
                "not exactly",
            ),
            (
                "the negative integer just past the safe range",
                r#"{"a":-9007199254740992}"#,
                "not exactly",
            ),
            (
                "an integer the Provider meant past 2^53",
                r#"{"a":9007199254740993}"#,
                "not exactly",
            ),
            (
                "the largest u64",
                r#"{"a":18446744073709551615}"#,
                "not exactly",
            ),
            (
                "the smallest i64",
                r#"{"a":-9223372036854775808}"#,
                "not exactly",
            ),
            (
                "the first unsafe integer in decimal notation",
                r#"{"a":9007199254740992.0}"#,
                "not exactly",
            ),
            (
                "the first unsafe integer in exponent notation",
                r#"{"a":9.007199254740992e15}"#,
                "not exactly",
            ),
            (
                "a negative unsafe integer in exponent notation",
                r#"{"a":-9.007199254740992E15}"#,
                "not exactly",
            ),
            (
                "an exactly representable large double",
                r#"{"a":1e300}"#,
                "not exactly",
            ),
            (
                "an integer beyond 2^53 read as a double",
                r#"{"a":9007199254740993.0}"#,
                "not exactly",
            ),
            (
                "more digits than a double keeps",
                r#"{"a":100.00000000000000001}"#,
                "not exactly",
            ),
            (
                "a number underflowing to zero",
                r#"{"a":1e-400}"#,
                "not exactly",
            ),
            (
                "a number overflowing a double",
                r#"{"a":1e400}"#,
                "not valid JSON",
            ),
        ];
        for (case, arguments, expected) in cases {
            let message = incomplete_message(read(calling("f", arguments), &open, None));
            assert!(
                message.contains("(invalid arguments)") && message.contains(expected),
                "{case}: {message}"
            );
        }
    }

    #[test]
    fn large_integers_are_admitted_only_as_strings_a_schema_allows() {
        let counter = offering_schema(
            "count",
            Some(json!({
                "type": "object",
                "properties": {
                    "id": {"type": "string", "pattern": "^[0-9]+$"},
                    "n": {"type": "integer"}
                },
                "required": ["id", "n"],
                "additionalProperties": false
            })),
        );
        let valid = r#"{"id":"18446744073709551616","n":9007199254740991}"#;
        let result = read(calling("count", valid), &counter, None).expect("a safe call");
        assert_eq!(result.tool_calls, [call(0, "c", "count", valid)]);

        // The schema allows any integer, but not every reader takes this one exactly.
        let error = read(
            calling("count", r#"{"id":"1","n":9007199254740993}"#),
            &counter,
            None,
        )
        .expect_err("an unsafe integer is refused");
        let ProviderExecutionError::Incomplete {
            message,
            facts: seen,
        } = &error
        else {
            panic!("expected an incomplete stream, got {error:?}");
        };
        assert!(
            message.contains("(invalid arguments)") && message.contains("not exactly"),
            "{message}"
        );
        assert!(!message.contains("9007199254740993"), "{message}");
        assert_eq!(
            *seen,
            facts(Some(ChatFinishReason::ToolCalls), Some("tool_calls"))
        );
        assert_eq!(
            error_observations(&error)[0].classification(),
            AvailabilityClassification::Unconfirmed
        );
        // A string the schema does not allow is still refused by the schema.
        assert!(
            incomplete_message(read(
                calling("count", r#"{"id":"0x1F","n":1}"#),
                &counter,
                None
            ))
            .contains("/properties/id/pattern")
        );
    }

    #[test]
    fn only_an_explicit_enforceable_parameter_schema_admits_a_call() {
        let parameterless =
            json!({"type": "object", "properties": {}, "additionalProperties": false});
        let conversation = offering_schema("ping", Some(parameterless));
        read(calling("ping", "{}"), &conversation, None).expect("no arguments");
        assert!(
            incomplete_message(read(calling("ping", r#"{"x":1}"#), &conversation, None))
                .contains("/additionalProperties")
        );
        let local = offering_schema(
            "f",
            Some(json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "$defs": {"city": {"type": "string", "pattern": "^[A-Z][a-z]+$"}},
                "type": "object",
                "properties": {"city": {"$ref": "#/$defs/city"}, "on": {"type": "string", "format": "date"}}
            })),
        );
        read(
            calling("f", r#"{"city":"Oslo","on":"2026-10-14"}"#),
            &local,
            None,
        )
        .expect("a local reference, a pattern and a format");
        for (case, arguments) in [
            ("a referenced pattern", r#"{"city":"oslo"}"#),
            ("an asserted format", r#"{"on":"14 October"}"#),
        ] {
            assert!(
                incomplete_message(read(calling("f", arguments), &local, None))
                    .contains("(invalid arguments)"),
                "{case}"
            );
        }

        let cases: [(&str, Option<Value>, &str); 13] = [
            ("no schema", None, "(missing parameter schema)"),
            (
                "a remote reference",
                Some(json!({"$ref": "https://example.com/s.json"})),
                "leaves the schema",
            ),
            (
                "a file reference",
                Some(json!({"properties": {"a": {"$ref": "file:///etc/passwd"}}})),
                "leaves the schema",
            ),
            (
                "a relative reference",
                Some(json!({"$ref": "other.json#/a"})),
                "leaves the schema",
            ),
            (
                "a base URI",
                Some(json!({"$id": "https://example.com/s"})),
                "`/$id`",
            ),
            (
                "an unknown keyword",
                Some(json!({"type": "object", "requird": ["a"]})),
                "`/requird`",
            ),
            (
                "a draft-07 container",
                Some(json!({"definitions": {"a": true}})),
                "`/definitions`",
            ),
            (
                "a content annotation",
                Some(json!({"properties": {"a": {"contentMediaType": "application/json"}}})),
                "contentMediaType",
            ),
            (
                "another dialect",
                Some(json!({"$schema": "http://json-schema.org/draft-07/schema#"})),
                "dialect",
            ),
            (
                "an unknown format",
                Some(json!({"properties": {"a": {"format": "bogus"}}})),
                "cannot be enforced",
            ),
            (
                "an unenforced IDN format",
                Some(json!({"properties": {"a": {"format": "idn-hostname"}}})),
                "cannot be enforced",
            ),
            (
                "a backtracking pattern",
                Some(json!({"properties": {"a": {"pattern": "(a)\\1"}}})),
                "cannot be enforced",
            ),
            (
                "a schema that is no schema",
                Some(json!({"properties": {"a": 7}})),
                "not one",
            ),
        ];
        for (case, parameters, expected) in cases {
            let message = incomplete_message(read(
                calling("f", "{}"),
                &offering_schema("f", parameters),
                None,
            ));
            assert!(
                (message.contains("(unsupported parameter schema)")
                    || message.contains("(missing parameter schema)"))
                    && message.contains(expected),
                "{case}: {message}"
            );
        }
        let mut deep = json!({"type": "object"});
        for _ in 0..40 {
            deep = json!({"not": deep});
        }
        assert!(
            incomplete_message(read(
                calling("f", "{}"),
                &offering_schema("f", Some(deep)),
                None
            ))
            .contains("nests deeper")
        );
    }

    #[test]
    fn admission_follows_only_the_request_that_produced_the_response() {
        let two = || offering(&["f", "g"]);
        let forced = ChatConversation {
            tool_choice: Some(ToolChoiceWire::Named(NamedToolChoiceWire {
                kind: "function".to_owned(),
                function: NamedFunctionWire {
                    name: "f".to_owned(),
                },
            })),
            ..two()
        };
        read(calling("f", "{}"), &forced, None).expect("the forced function");
        let required = ChatConversation {
            tool_choice: Some(ToolChoiceWire::Mode("required".to_owned())),
            ..two()
        };
        read(calling("g", "{}"), &required, None).expect("any offered function");
        let serial = ChatConversation {
            parallel_tool_calls: Some(false),
            ..two()
        };
        read(calling("g", "{}"), &serial, None).expect("one call");

        // Conversations admission never trusts to have been validated.
        let mut duplicated = offering(&["f"]);
        duplicated.tools.push(ToolWire {
            function: FunctionDefinitionWire {
                parameters: Some(json!(true)),
                ..tool("f").function
            },
            ..tool("f")
        });
        let named_unoffered = ChatConversation {
            tool_choice: Some(ToolChoiceWire::Named(NamedToolChoiceWire {
                kind: "function".to_owned(),
                function: NamedFunctionWire {
                    name: "h".to_owned(),
                },
            })),
            ..two()
        };
        let unknown_mode = ChatConversation {
            tool_choice: Some(ToolChoiceWire::Mode("anything".to_owned())),
            ..two()
        };
        let not_a_function = ChatConversation {
            tools: vec![ToolWire {
                tool_type: "code_interpreter".to_owned(),
                ..tool("f")
            }],
            ..offering(&[])
        };
        let cases: [(&str, ChatConversation, &str); 6] = [
            ("another function than the forced one", forced, "g"),
            (
                "tool_choice none",
                ChatConversation {
                    tool_choice: Some(ToolChoiceWire::Mode("none".to_owned())),
                    ..two()
                },
                "f",
            ),
            ("a function offered twice", duplicated, "f"),
            (
                "a forced function that was never offered",
                named_unoffered,
                "h",
            ),
            ("an unknown tool_choice mode", unknown_mode, "f"),
            ("a tool that is not a function", not_a_function, "f"),
        ];
        for (case, conversation, name) in cases {
            let message = incomplete_message(read(calling(name, "{}"), &conversation, None));
            assert!(message.contains("(unauthorized)"), "{case}: {message}");
        }
        let message = incomplete_message(read(
            vec![
                Step::Chunk(tool_delta(
                    r#"[{"index":0,"id":"a","function":{"name":"f","arguments":"{}"}},{"index":1,"id":"b","function":{"name":"g","arguments":"{}"}}]"#,
                )),
                Step::Chunk(TOOL_CALLS),
            ],
            &ChatConversation {
                parallel_tool_calls: Some(false),
                ..two()
            },
            None,
        ));
        assert!(
            message.contains("(unauthorized)") && message.contains("parallel"),
            "{message}"
        );
    }

    #[test]
    fn conflicting_or_malformed_fragments_are_refused_not_merged() {
        let cases = [
            (
                "a different id at one index",
                r#"[{"index":0,"id":"call_a","function":{"name":"f","arguments":"{}"}},{"index":0,"id":"call_b"}]"#,
                "changed its id",
            ),
            (
                "one id at two indexes",
                r#"[{"index":0,"id":"call_a","function":{"name":"f","arguments":"{}"}},{"index":1,"id":"call_a","function":{"name":"f","arguments":"{}"}}]"#,
                "two different indexes",
            ),
            (
                "no index",
                r#"[{"id":"call_a","function":{"name":"f","arguments":"{}"}}]"#,
                "no valid `index`",
            ),
            (
                "a negative index",
                r#"[{"index":-1,"id":"call_a"}]"#,
                "no valid `index`",
            ),
            (
                "a non-function type",
                r#"[{"index":0,"id":"call_a","type":"code_interpreter"}]"#,
                "unsupported type",
            ),
            (
                "arguments that are not a string",
                r#"[{"index":0,"id":"call_a","function":{"name":"f","arguments":{"a":1}}}]"#,
                "not a string",
            ),
            (
                "tool_calls that is not an array",
                r#"{"index":0}"#,
                "not an array",
            ),
        ];
        for (case, calls, expected) in cases {
            let message = incomplete_message(read(
                vec![Step::Chunk(tool_delta(calls)), Step::Chunk(TOOL_CALLS)],
                &offering(&["f"]),
                None,
            ));
            assert!(message.contains(expected), "{case}: {message}");
        }
    }

    #[test]
    fn missing_or_malformed_arguments_are_never_completed() {
        let cases = [
            (
                "no arguments",
                r#"[{"index":0,"id":"c","function":{"name":"f"}}]"#,
            ),
            (
                "truncated JSON",
                r#"[{"index":0,"id":"c","function":{"name":"f","arguments":"{\"a\":"}}]"#,
            ),
            (
                "a JSON array",
                r#"[{"index":0,"id":"c","function":{"name":"f","arguments":"[1]"}}]"#,
            ),
            (
                "trailing data",
                r#"[{"index":0,"id":"c","function":{"name":"f","arguments":"{} {}"}}]"#,
            ),
        ];
        for (case, calls) in cases {
            let message = incomplete_message(read(
                vec![Step::Chunk(tool_delta(calls)), Step::Chunk(TOOL_CALLS)],
                &offering(&["f"]),
                None,
            ));
            assert!(message.contains("not one JSON object"), "{case}: {message}");
        }
        // Arguments that a later call never had are not borrowed from an earlier one.
        let message = incomplete_message(read(
            vec![
                Step::Chunk(tool_delta(
                    r#"[{"index":0,"id":"c0","function":{"name":"f","arguments":"{\"a\":1}"}},{"index":1,"id":"c1","function":{"name":"f"}}]"#,
                )),
                Step::Chunk(TOOL_CALLS),
            ],
            &offering(&["f"]),
            None,
        ));
        assert!(message.contains("tool call 1"), "{message}");
    }

    #[test]
    fn incomplete_tool_call_streams_stay_incomplete_and_keep_their_evidence() {
        let partial =
            tool_delta(r#"[{"index":0,"id":"c","function":{"name":"f","arguments":"{\"a\":"}}]"#);
        let complete =
            tool_delta(r#"[{"index":0,"id":"c","function":{"name":"f","arguments":"{}"}}]"#);
        let length = "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"length\"}]}\n\n";
        let cases: [(&str, Vec<Step>, &str); 5] = [
            (
                "a stream cut before its finish",
                vec![Step::Chunk(CONTENT), Step::Chunk(partial)],
                "without a finish event",
            ),
            (
                "a tool_calls finish without a call",
                vec![Step::Chunk(CONTENT), Step::Chunk(TOOL_CALLS)],
                "sent no tool call",
            ),
            (
                "a call truncated by the length limit",
                vec![
                    Step::Chunk(CONTENT),
                    Step::Chunk(complete),
                    Step::Chunk(length),
                ],
                "finish reason `length`",
            ),
            (
                "a call without an id",
                vec![
                    Step::Chunk(CONTENT),
                    Step::Chunk(tool_delta(
                        r#"[{"index":0,"function":{"name":"f","arguments":"{}"}}]"#,
                    )),
                    Step::Chunk(TOOL_CALLS),
                ],
                "has no id",
            ),
            (
                "a call without a name",
                vec![
                    Step::Chunk(CONTENT),
                    Step::Chunk(tool_delta(
                        r#"[{"index":0,"id":"c","function":{"arguments":"{}"}}]"#,
                    )),
                    Step::Chunk(TOOL_CALLS),
                ],
                "no valid function name",
            ),
        ];
        for (case, steps, expected) in cases {
            let mut sink = RecordingSink::default();
            let message = incomplete_message(read(steps, &offering(&["f"]), Some(&mut sink)));
            assert!(message.contains(expected), "{case}: {message}");
            assert_eq!(sink.deltas, ["hi"], "{case}: the streamed text is kept");
        }
    }

    #[test]
    fn a_cancellation_during_argument_reconstruction_is_a_local_stop_without_tool_calls() {
        let mut sink = RecordingSink::default();
        let error = read(
            vec![
                Step::Chunk(tool_delta(
                    r#"[{"index":0,"id":"c","function":{"name":"f","arguments":"{\"a\":"}}]"#,
                )),
                Step::Cancel,
                Step::Chunk(tool_delta(r#"[{"index":0,"function":{"arguments":"1}"}}]"#)),
                Step::Chunk(TOOL_CALLS),
            ],
            &offering(&["f"]),
            Some(&mut sink),
        )
        .expect_err("a cancelled stream stops");
        assert!(
            matches!(&error, ProviderExecutionError::Stopped { facts: seen, .. } if *seen == facts(None, None)),
            "got {error:?}"
        );
        assert!(sink.deltas.is_empty());
    }

    #[test]
    fn a_stalled_stream_during_argument_reconstruction_is_unknown() {
        let result =
            ntex::rt::System::new("tool-idle-test", ntex::rt::DefaultRuntime).block_on(async {
                let config = ProviderExecutionConfig {
                    timeout: Duration::from_millis(100),
                    ..ProviderExecutionConfig::default()
                };
                read_response(
                    payload(
                        vec![
                            Step::Chunk(tool_delta(
                                r#"[{"index":0,"id":"c","function":{"name":"f","arguments":"{"}}]"#,
                            )),
                            Step::Stall,
                        ],
                        config.cancellation.clone(),
                    ),
                    200,
                    Instant::now(),
                    None,
                    &config,
                    None,
                    &offering(&["f"]),
                )
                .await
            });
        assert!(
            matches!(result, Err(ProviderExecutionError::Unknown { .. })),
            "got {result:?}"
        );
    }

    #[test]
    fn text_reasoning_and_tool_calls_in_one_response_are_each_kept() {
        let mut sink = RecordingSink::default();
        let result = read(
            vec![
                Step::Chunk(
                    "data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"need \"}}]}\n\n",
                ),
                Step::Chunk(
                    "data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"weather\"}}]}\n\n",
                ),
                Step::Chunk("data: {\"choices\":[{\"delta\":{\"content\":\"Checking.\"}}]}\n\n"),
                Step::Chunk(tool_delta(
                    r#"[{"index":0,"id":"c","function":{"name":"get_weather","arguments":"{}"}}]"#,
                )),
                Step::Chunk(TOOL_CALLS),
            ],
            &offering(&["get_weather"]),
            Some(&mut sink),
        )
        .expect("a mixed response");
        assert_eq!(result.text, "Checking.");
        assert_eq!(sink.deltas, ["Checking."], "only the text is published");
        assert_eq!(result.reasoning, "need weather");
        assert_eq!(result.tool_calls, [call(0, "c", "get_weather", "{}")]);
        assert!(result.requests_tools());
    }

    #[test]
    fn a_tool_call_exchange_is_neither_a_final_answer_nor_available_evidence() {
        let forced = ChatConversation {
            tool_choice: Some(ToolChoiceWire::Named(NamedToolChoiceWire {
                kind: "function".to_owned(),
                function: NamedFunctionWire {
                    name: "f".to_owned(),
                },
            })),
            ..offering(&["f", "g"])
        };
        // A forced function finishes with `stop` on the OpenAI wire.
        let result = read(
            vec![
                Step::Chunk(tool_delta(
                    r#"[{"index":0,"id":"c","function":{"name":"f","arguments":"{}"}}]"#,
                )),
                Step::Chunk(STOP),
            ],
            &forced,
            None,
        )
        .expect("a forced call");
        assert_eq!(result.raw_finish_reason.as_deref(), Some("stop"));
        assert!(
            result.requests_tools(),
            "a stop beside tool calls still asks for tools"
        );

        let result = read(
            vec![
                Step::Chunk(tool_delta(
                    r#"[{"index":0,"id":"c","function":{"name":"f","arguments":"{}"}}]"#,
                )),
                Step::Chunk(TOOL_CALLS),
            ],
            &offering(&["f"]),
            None,
        )
        .expect("a tool call");
        assert_ne!(
            success_observation(&result).classification(),
            crate::persistence::AvailabilityClassification::Available,
            "a tool_calls finish is not a normal terminal answer"
        );
        assert_eq!(
            result_value(&result),
            json!({"text": "", "finish_reason": "tool_calls"}),
            "nothing claims a tool ran or produced output"
        );
    }

    #[test]
    fn tool_calls_beyond_what_the_request_allowed_are_refused() {
        let call_f =
            tool_delta(r#"[{"index":0,"id":"c","function":{"name":"f","arguments":"{}"}}]"#);
        let none = ChatConversation {
            tool_choice: Some(ToolChoiceWire::Mode("none".to_owned())),
            ..offering(&["f"])
        };
        let serial = ChatConversation {
            parallel_tool_calls: Some(false),
            ..offering(&["f"])
        };
        let cases: [(&str, ChatConversation, &'static str); 4] = [
            (
                "a Job objective offers no tools",
                ChatConversation::default(),
                call_f,
            ),
            ("an undeclared tool", offering(&["g"]), call_f),
            ("tool_choice none", none, call_f),
            (
                "parallel calls turned off",
                serial,
                tool_delta(
                    r#"[{"index":0,"id":"a","function":{"name":"f","arguments":"{}"}},{"index":1,"id":"b","function":{"name":"f","arguments":"{}"}}]"#,
                ),
            ),
        ];
        for (case, conversation, calls) in cases {
            let message = incomplete_message(read(
                vec![Step::Chunk(calls), Step::Chunk(TOOL_CALLS)],
                &conversation,
                None,
            ));
            assert!(
                message.contains("invalid provider tool call"),
                "{case}: {message}"
            );
        }
    }

    #[test]
    fn usage_beside_a_refused_tool_fragment_is_observed_and_the_call_is_not() {
        let usage = r#"{"prompt_tokens":7,"completion_tokens":2,"prompt_tokens_details":{"cached_tokens":1},"vendor":{"n":1}}"#;
        let refused = leaked(format!(
            "data: {{\"choices\":[{{\"delta\":{{\"content\":\"lost\",\"tool_calls\":[{{\"index\":0,\"id\":\"c\",\"type\":\"code_interpreter\"}}]}}}}],\"usage\":{usage}}}\n\n"
        ));
        let mut sink = RecordingSink::default();
        let error = read(
            vec![
                Step::Chunk(CONTENT),
                Step::Chunk(refused),
                Step::Chunk(TOOL_CALLS),
            ],
            &offering(&["f"]),
            Some(&mut sink),
        )
        .expect_err("a malformed tool call is refused");
        match &error {
            ProviderExecutionError::Incomplete {
                message,
                facts: seen,
            } => {
                assert!(message.contains("unsupported type"), "{message}");
                assert_eq!(*seen, facts(None, None));
            }
            other => panic!("expected an incomplete stream, got {other:?}"),
        }
        assert_eq!(
            sink.deltas,
            ["hi"],
            "no text of the refused event is published"
        );
        let raw: Value = serde_json::from_str(usage).expect("usage object");
        assert_eq!(
            sink.usages(),
            [(
                UsageFinality::Cumulative,
                NormalizedUsage {
                    input_tokens: Some(7),
                    output_tokens: Some(2),
                    cache_read_tokens: Some(1),
                    raw: Some(raw),
                    ..NormalizedUsage::default()
                }
            )]
        );
    }

    #[test]
    fn refused_tool_calls_keep_the_facts_they_observed_and_stay_unconfirmed() {
        let call_g =
            tool_delta(r#"[{"index":0,"id":"c","function":{"name":"g","arguments":"{}"}}]"#);
        let no_id = tool_delta(r#"[{"index":0,"function":{"name":"f","arguments":"{}"}}]"#);
        let forced = ChatConversation {
            tool_choice: Some(ToolChoiceWire::Named(NamedToolChoiceWire {
                kind: "function".to_owned(),
                function: NamedFunctionWire {
                    name: "f".to_owned(),
                },
            })),
            ..offering(&["f"])
        };
        let cases: [(&str, Vec<Step>, ChatConversation, ResponseFacts); 3] = [
            (
                "an undeclared tool",
                vec![Step::Chunk(call_g), Step::Chunk(TOOL_CALLS)],
                offering(&["f"]),
                facts(Some(ChatFinishReason::ToolCalls), Some("tool_calls")),
            ),
            (
                "a forced call to another tool, finished with stop",
                vec![Step::Chunk(call_g), Step::Chunk(STOP)],
                forced,
                facts(Some(ChatFinishReason::Stop), Some("stop")),
            ),
            (
                "a call without an id, finished with stop",
                vec![Step::Chunk(no_id), Step::Chunk(STOP)],
                offering(&["f"]),
                facts(Some(ChatFinishReason::Stop), Some("stop")),
            ),
        ];
        for (case, steps, conversation, expected) in cases {
            let error = read(steps, &conversation, None).expect_err(case);
            assert!(
                matches!(&error, ProviderExecutionError::Incomplete { facts: seen, .. } if *seen == expected),
                "{case}: got {error:?}"
            );
            let observations = error_observations(&error);
            assert_eq!(observations.len(), 1, "{case}");
            assert_eq!(observations[0].status_code, Some(200), "{case}");
            assert_eq!(
                observations[0].finish_reason, expected.finish_reason,
                "{case}"
            );
            assert!(observations[0].failure.is_some(), "{case}");
            assert_eq!(
                observations[0].classification(),
                AvailabilityClassification::Unconfirmed,
                "{case}: a refused tool response is never available evidence"
            );
        }
    }

    #[test]
    fn streamed_tool_calls_are_bounded_per_call_and_per_response() {
        let oversized = leaked(format!(
            "[{{\"index\":0,\"id\":\"c\",\"function\":{{\"name\":\"f\",\"arguments\":\"{}\"}}}}]",
            "a".repeat(60 * 1024)
        ));
        let mut steps = vec![Step::Chunk(tool_delta(oversized))];
        let more = leaked(format!(
            "[{{\"index\":0,\"function\":{{\"arguments\":\"{}\"}}}}]",
            "a".repeat(60 * 1024)
        ));
        steps.extend(
            (0..MAX_TOOL_ARGUMENTS_BYTES / (60 * 1024)).map(|_| Step::Chunk(tool_delta(more))),
        );
        steps.push(Step::Chunk(TOOL_CALLS));
        let message = incomplete_message(read(steps, &offering(&["f"]), None));
        assert!(message.contains("arguments longer than"), "{message}");

        let calls: Vec<String> = (0..=MAX_TOOL_CALLS)
            .map(|index| format!("{{\"index\":{index},\"id\":\"c{index}\"}}"))
            .collect();
        let message = incomplete_message(read(
            vec![Step::Chunk(tool_delta(leaked(format!(
                "[{}]",
                calls.join(",")
            ))))],
            &offering(&["f"]),
            None,
        ));
        assert!(message.contains("more than"), "{message}");

        let long_name = leaked(format!(
            "[{{\"index\":0,\"id\":\"c\",\"function\":{{\"name\":\"{}\"}}}}]",
            "n".repeat(65)
        ));
        let message = incomplete_message(read(
            vec![Step::Chunk(tool_delta(long_name))],
            &offering(&["f"]),
            None,
        ));
        assert!(message.contains("function name longer"), "{message}");

        // Text, reasoning and tool calls share one response limit: the two
        // text bytes fit, the four bytes of the call that follows do not.
        let config = ProviderExecutionConfig {
            max_output_bytes: 5,
            ..ProviderExecutionConfig::default()
        };
        let mut sink = RecordingSink::default();
        let error = wait(read_response(
            payload(
                vec![
                    Step::Chunk(CONTENT),
                    Step::Chunk(tool_delta(
                        r#"[{"index":0,"id":"c","function":{"name":"f","arguments":"{}"}}]"#,
                    )),
                    Step::Chunk(TOOL_CALLS),
                ],
                config.cancellation.clone(),
            ),
            200,
            Instant::now(),
            None,
            &config,
            Some(&mut sink),
            &offering(&["f"]),
        ))
        .expect_err("the response limit stops the stream");
        assert!(
            matches!(&error, ProviderExecutionError::Stopped { message, .. } if message.contains("exceeded its limit")),
            "got {error:?}"
        );
        assert_eq!(sink.deltas, ["hi"]);
    }

    #[test]
    fn usage_in_a_tool_call_stream_is_observed_exactly_as_in_a_text_stream() {
        let steps = |middle: &'static str, finish: &'static str| {
            vec![
                Step::Chunk(running_total(
                    "{\"prompt_tokens\":5,\"completion_tokens\":1}",
                )),
                Step::Chunk(middle),
                Step::Chunk(running_total(
                    "{\"prompt_tokens\":5,\"completion_tokens\":3}",
                )),
                Step::Chunk(finish),
                Step::Chunk(usage_only("{\"prompt_tokens\":5,\"completion_tokens\":4}")),
                Step::Chunk(DONE),
            ]
        };
        let mut text_sink = RecordingSink::default();
        let text = read(
            steps(CONTENT, STOP),
            &offering(&["f"]),
            Some(&mut text_sink),
        )
        .expect("a text stream");
        let mut tool_sink = RecordingSink::default();
        let tools = read(
            steps(
                tool_delta(r#"[{"index":0,"id":"c","function":{"name":"f","arguments":"{}"}}]"#),
                TOOL_CALLS,
            ),
            &offering(&["f"]),
            Some(&mut tool_sink),
        )
        .expect("a tool call stream");
        assert_eq!(tool_sink.usages(), text_sink.usages());
        assert_eq!(tool_sink.usages().len(), 3);
        assert_eq!(tools.usage, text.usage);
    }

    #[test]
    fn the_objective_request_body_is_unchanged() {
        let parameters = LlmParameters {
            temperature: Some(0.5),
            max_tokens: Some(64),
            ..LlmParameters::default()
        };
        let conversation = ChatConversation {
            messages: vec![ChatMessage::system("be brief"), ChatMessage::user("hi")],
            ..ChatConversation::default()
        };
        assert_eq!(
            request_body("model-a", &parameters, &conversation).expect("a body"),
            json!({
                "model": "model-a",
                "messages": [
                    {"role": "system", "content": "be brief"},
                    {"role": "user", "content": "hi"},
                ],
                "stream": true,
                "stream_options": {"include_usage": true},
                "temperature": 0.5,
                "max_tokens": 64,
            })
        );
    }

    #[test]
    fn an_assistant_tool_turn_and_its_results_serialize_in_order_with_exact_arguments() {
        let arguments = "{ \"city\" : \"Paris\",\"units\":\"c\" }";
        let mut assistant = ChatMessage::assistant(
            None,
            vec![ChatToolCallWire::from(&call(
                0,
                "call_1",
                "get_weather",
                arguments,
            ))],
        );
        assistant.reasoning_content = Some("need weather".to_owned());
        let conversation = ChatConversation {
            messages: vec![
                ChatMessage::user("weather?"),
                assistant,
                ChatMessage::tool("call_1", "18C"),
            ],
            tool_choice: Some(ToolChoiceWire::Mode("auto".to_owned())),
            parallel_tool_calls: Some(true),
            ..offering(&["get_weather"])
        };
        conversation.validate().expect("a valid conversation");
        let body =
            request_body("model-a", &LlmParameters::default(), &conversation).expect("a body");
        assert_eq!(
            body["messages"],
            json!([
                {"role": "user", "content": "weather?"},
                {
                    "role": "assistant",
                    "reasoning_content": "need weather",
                    "tool_calls": [{
                        "id": "call_1",
                        "type": "function",
                        "function": {"name": "get_weather", "arguments": arguments},
                    }],
                },
                {"role": "tool", "tool_call_id": "call_1", "content": "18C"},
            ])
        );
        assert_eq!(
            body["tools"],
            json!([{
                "type": "function",
                "function": {
                    "name": "get_weather",
                    "description": "the get_weather tool",
                    "parameters": {"type": "object", "properties": {}},
                },
            }])
        );
        assert_eq!(body["tool_choice"], json!("auto"));
        assert_eq!(body["parallel_tool_calls"], json!(true));
        assert_eq!(body["model"], json!("model-a"), "the frozen model is sent");
    }

    #[test]
    fn invalid_or_unsupported_conversations_are_refused_before_sending() {
        let answered = |id: &str| {
            vec![
                ChatMessage::user("q"),
                ChatMessage::assistant(None, vec![ChatToolCallWire::from(&call(0, id, "f", "{}"))]),
                ChatMessage::tool(id, "r"),
            ]
        };
        let mut unsupported_type = tool("f");
        unsupported_type.tool_type = "retrieval".to_owned();
        let mut non_object_parameters = tool("f");
        non_object_parameters.function.parameters = Some(json!("string"));
        let mut duplicate_ids = answered("x");
        duplicate_ids.extend(answered("x").into_iter().skip(1));
        let cases: Vec<(&str, ChatConversation, &str)> = vec![
            ("no messages", ChatConversation::default(), "messages"),
            (
                "an unsupported tool type",
                ChatConversation {
                    tools: vec![unsupported_type],
                    ..offering(&[])
                },
                "tools[0].type",
            ),
            (
                "an invalid tool name",
                offering(&["get weather"]),
                "tools[0].function.name",
            ),
            (
                "a tool offered twice",
                offering(&["f", "f"]),
                "tools[1].function.name",
            ),
            (
                "non-object parameters",
                ChatConversation {
                    tools: vec![non_object_parameters],
                    ..offering(&[])
                },
                "tools[0].function.parameters",
            ),
            (
                "tool_choice without tools",
                ChatConversation {
                    tool_choice: Some(ToolChoiceWire::Mode("auto".to_owned())),
                    ..offering(&[])
                },
                "tool_choice",
            ),
            (
                "a forced tool that is not offered",
                ChatConversation {
                    tool_choice: Some(ToolChoiceWire::Named(NamedToolChoiceWire {
                        kind: "function".to_owned(),
                        function: NamedFunctionWire {
                            name: "g".to_owned(),
                        },
                    })),
                    ..offering(&["f"])
                },
                "tool_choice.function.name",
            ),
            (
                "an unknown tool_choice mode",
                ChatConversation {
                    tool_choice: Some(ToolChoiceWire::Mode("sometimes".to_owned())),
                    ..offering(&["f"])
                },
                "tool_choice",
            ),
            (
                "an unanswered tool call",
                ChatConversation {
                    messages: answered("x")[..2].to_vec(),
                    ..offering(&["f"])
                },
                "never answered",
            ),
            (
                "a tool result for an unknown call",
                ChatConversation {
                    messages: vec![ChatMessage::user("q"), ChatMessage::tool("x", "r")],
                    ..offering(&["f"])
                },
                "messages[1].tool_call_id",
            ),
            (
                "a message between a call and its result",
                ChatConversation {
                    messages: vec![
                        answered("x")[0].clone(),
                        answered("x")[1].clone(),
                        ChatMessage::user("interrupt"),
                        answered("x")[2].clone(),
                    ],
                    ..offering(&["f"])
                },
                "messages[2]",
            ),
            (
                "a tool call id used twice",
                ChatConversation {
                    messages: duplicate_ids,
                    ..offering(&["f"])
                },
                "messages[3].tool_calls[0].id",
            ),
            (
                "an empty tool_calls array",
                ChatConversation {
                    messages: vec![ChatMessage::user("q"), {
                        let mut message = ChatMessage::assistant(Some("a".to_owned()), Vec::new());
                        message.tool_calls = Some(Vec::new());
                        message
                    }],
                    ..offering(&["f"])
                },
                "messages[1].tool_calls",
            ),
            (
                "malformed tool call arguments",
                ChatConversation {
                    messages: vec![
                        ChatMessage::user("q"),
                        ChatMessage::assistant(
                            None,
                            vec![ChatToolCallWire::from(&call(0, "x", "f", "{oops"))],
                        ),
                        ChatMessage::tool("x", "r"),
                    ],
                    ..offering(&["f"])
                },
                "tool_calls[0].function.arguments",
            ),
        ];
        for (case, conversation, field) in cases {
            let error = conversation.validate().expect_err(case);
            assert!(error.to_string().contains(field), "{case}: {error}");
        }
        answered("x");
        ChatConversation {
            messages: answered("x"),
            ..offering(&["f"])
        }
        .validate()
        .expect("an answered call is valid");
    }

    #[test]
    fn tool_definitions_and_arguments_are_size_limited() {
        let mut huge = tool("f");
        huge.function.description = Some("d".repeat(64 * 1024));
        let names: Vec<String> = (0..=MAX_TOOLS).map(|index| format!("t{index}")).collect();
        let too_many = offering(&names.iter().map(String::as_str).collect::<Vec<_>>());
        let oversized_arguments = format!("{{\"a\":\"{}\"}}", "a".repeat(MAX_TOOL_ARGUMENTS_BYTES));
        let long_id = "i".repeat(257);
        let cases: Vec<(&str, ChatConversation, &str)> = vec![
            (
                "an oversized definition",
                ChatConversation {
                    tools: vec![huge],
                    ..offering(&[])
                },
                "tools[0].function",
            ),
            ("too many tools", too_many, "tools"),
            (
                "oversized arguments",
                ChatConversation {
                    messages: vec![
                        ChatMessage::user("q"),
                        ChatMessage::assistant(
                            None,
                            vec![ChatToolCallWire::from(&call(
                                0,
                                "x",
                                "f",
                                &oversized_arguments,
                            ))],
                        ),
                        ChatMessage::tool("x", "r"),
                    ],
                    ..offering(&["f"])
                },
                "function.arguments",
            ),
            (
                "an oversized id",
                ChatConversation {
                    messages: vec![
                        ChatMessage::user("q"),
                        ChatMessage::assistant(
                            None,
                            vec![ChatToolCallWire::from(&call(0, &long_id, "f", "{}"))],
                        ),
                        ChatMessage::tool(long_id.clone(), "r"),
                    ],
                    ..offering(&["f"])
                },
                "tool_calls[0].id",
            ),
        ];
        for (case, conversation, field) in cases {
            let error = conversation.validate().expect_err(case);
            assert!(error.to_string().contains(field), "{case}: {error}");
        }
    }

    #[test]
    fn an_invalid_conversation_is_never_sent_and_is_known_not_delivered() {
        let frozen = FrozenExecutionConfig {
            agent: None,
            llm: crate::persistence::FrozenLlmConfig {
                config: crate::persistence::LlmConfig {
                    model: "model-a".to_owned(),
                    parameters: LlmParameters::default(),
                    declared_capabilities: Default::default(),
                },
                provider: crate::persistence::ProviderSnapshot {
                    id: crate::persistence::LlmProviderId::new(1),
                    name: "local".to_owned(),
                    transport: crate::persistence::LlmTransport::OpenAiChatCompletions,
                    // Nothing listens here; validation must refuse first.
                    endpoint: Some("https://127.0.0.1:9/v1".to_owned()),
                    credential_ref: None,
                },
            },
        };
        let conversation = ChatConversation {
            messages: vec![ChatMessage::user("q"), ChatMessage::tool("x", "r")],
            ..offering(&["f"])
        };
        let mut sink = RecordingSink::default();
        let error = wait(execute_conversation(
            &frozen,
            &conversation,
            &EnvironmentCredentialResolver,
            &ProviderExecutionConfig::default(),
            Some(&mut sink),
        ))
        .expect_err("an invalid conversation is refused");
        assert!(
            matches!(&error, ProviderExecutionError::Build { message } if message.contains("tool_call_id")),
            "got {error:?}"
        );
        assert_eq!(
            sink.observations
                .iter()
                .map(|observation| observation.fact.clone())
                .collect::<Vec<_>>(),
            [ProviderFact::NotDelivered]
        );
        assert_eq!(error_observations(&error), []);

        let objective = objective_conversation(
            &FrozenExecutionConfig {
                agent: Some(crate::persistence::AgentConfig {
                    instructions: Some("be brief".to_owned()),
                }),
                ..frozen
            },
            Some("hi"),
        );
        objective
            .validate()
            .expect("an objective is a valid conversation");
        assert!(objective.tools.is_empty() && objective.tool_choice.is_none());
        assert_eq!(
            serde_json::to_value(&objective.messages).expect("messages"),
            json!([
                {"role": "system", "content": "be brief"},
                {"role": "user", "content": "hi"},
            ])
        );
    }

    /// A frozen configuration whose endpoint nothing listens on.
    fn unreachable_frozen() -> FrozenExecutionConfig {
        FrozenExecutionConfig {
            agent: None,
            llm: crate::persistence::FrozenLlmConfig {
                config: crate::persistence::LlmConfig {
                    model: "model-v".to_owned(),
                    parameters: LlmParameters::default(),
                    declared_capabilities: Default::default(),
                },
                provider: crate::persistence::ProviderSnapshot {
                    id: crate::persistence::LlmProviderId::new(1),
                    name: "local".to_owned(),
                    transport: crate::persistence::LlmTransport::OpenAiChatCompletions,
                    endpoint: Some("https://127.0.0.1:9/v1".to_owned()),
                    credential_ref: None,
                },
            },
        }
    }

    fn body_of(frozen: &FrozenExecutionConfig, conversation: &ChatConversation) -> Value {
        let config = &frozen.llm.config;
        request_body(&config.model, &config.parameters, conversation).expect("a body")
    }

    #[test]
    fn images_reach_the_request_body_as_real_parts_and_text_alone_is_unchanged() {
        use crate::openai_compatible::image::tests::{PNG_BASE64, jpeg, png};
        use crate::openai_compatible::{ImageInput, ImageMime};

        let frozen = unreachable_frozen();
        let text_only = body_of(&frozen, &objective_conversation(&frozen, Some("hi")));
        assert_eq!(
            text_only,
            json!({
                "messages": [{"role": "user", "content": "hi"}],
                "model": "model-v",
                "stream": true,
                "stream_options": {"include_usage": true},
            })
        );
        assert_eq!(
            body_of(
                &frozen,
                &objective_conversation_with_images(&frozen, Some("hi"), &[])
            ),
            text_only,
            "no image, no change"
        );

        let images = [
            ImageInput::new(ImageMime::Png, png()).expect("a PNG"),
            ImageInput::new(ImageMime::Jpeg, jpeg()).expect("a JPEG"),
        ];
        let conversation =
            objective_conversation_with_images(&frozen, Some("Read the image."), &images);
        conversation.validate().expect("valid");
        let jpeg_url = format!(
            "data:image/jpeg;base64,{}",
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, jpeg())
        );
        assert_eq!(
            body_of(&frozen, &conversation),
            json!({
                "messages": [{"role": "user", "content": [
                    {"type": "text", "text": "Read the image."},
                    {"type": "image_url", "image_url": {
                        "url": format!("data:image/png;base64,{PNG_BASE64}")
                    }},
                    {"type": "image_url", "image_url": {"url": jpeg_url}},
                ]}],
                "model": "model-v",
                "stream": true,
                "stream_options": {"include_usage": true},
            })
        );
        let image_only = objective_conversation_with_images(&frozen, Some(""), &images[..1]);
        image_only.validate().expect("an image alone is valid");
        assert_eq!(
            body_of(&frozen, &image_only)["messages"],
            json!([{"role": "user", "content": [{"type": "image_url", "image_url": {
                "url": format!("data:image/png;base64,{PNG_BASE64}")
            }}]}])
        );
    }

    #[test]
    fn an_invalid_image_is_never_sent_and_is_known_not_delivered() {
        use crate::openai_compatible::image::tests::PNG_BASE64;
        use crate::openai_compatible::{ContentPartWire, ImageUrlWire};

        let conversation = ChatConversation {
            messages: vec![ChatMessage::user_parts(vec![ContentPartWire::ImageUrl {
                image_url: ImageUrlWire {
                    url: format!("data:image/jpeg;base64,{PNG_BASE64}"),
                },
            }])],
            ..ChatConversation::default()
        };
        let mut sink = RecordingSink::default();
        let error = wait(execute_conversation(
            &unreachable_frozen(),
            &conversation,
            &EnvironmentCredentialResolver,
            &ProviderExecutionConfig::default(),
            Some(&mut sink),
        ))
        .expect_err("a mislabelled image is refused");
        assert!(
            matches!(&error, ProviderExecutionError::Build { message }
                if message.contains("not the image/jpeg") && !message.contains(&PNG_BASE64[..12])),
            "got {error:?}"
        );
        assert_eq!(
            sink.observations
                .iter()
                .map(|observation| observation.fact.clone())
                .collect::<Vec<_>>(),
            [ProviderFact::NotDelivered]
        );
    }

    /// Conversations whose refusals must keep nothing the Provider said:
    /// one PNG, a PNG and a JPEG, and an image in an earlier turn followed
    /// by a tool call and its answer.
    pub(crate) fn image_conversations() -> Vec<(&'static str, ChatConversation)> {
        use crate::openai_compatible::image::tests::{jpeg, png};
        use crate::openai_compatible::{ChatFunctionCallWire, ChatToolCallWire};
        use crate::openai_compatible::{ImageInput, ImageMime};

        let png = ImageInput::new(ImageMime::Png, png()).expect("a PNG");
        let jpeg = ImageInput::new(ImageMime::Jpeg, jpeg()).expect("a JPEG");
        let text = |text: &str| ContentPartWire::Text {
            text: text.to_owned(),
        };
        let call = ChatToolCallWire {
            id: "call_1".to_owned(),
            tool_type: Some("function".to_owned()),
            function: ChatFunctionCallWire {
                name: "f".to_owned(),
                arguments: "{}".to_owned(),
            },
        };
        let user = |parts| ChatConversation {
            messages: vec![ChatMessage::user_parts(parts)],
            ..ChatConversation::default()
        };
        vec![
            ("one PNG", user(vec![text("Read it."), png.part()])),
            (
                "a PNG and a JPEG",
                user(vec![png.part(), text("and"), jpeg.part()]),
            ),
            (
                "an image before a tool continuation",
                ChatConversation {
                    messages: vec![
                        ChatMessage::user_parts(vec![text("Read it."), jpeg.part()]),
                        ChatMessage::assistant(None, vec![call]),
                        ChatMessage::tool("call_1", "{\"ok\":true}"),
                        ChatMessage::user("Go on."),
                    ],
                    ..offering(&["f"])
                },
            ),
        ]
    }

    /// The `data:` URLs of a conversation's images.
    pub(crate) fn image_urls(conversation: &ChatConversation) -> Vec<String> {
        conversation
            .messages
            .iter()
            .filter_map(|message| match &message.content {
                Some(crate::openai_compatible::MessageContentWire::Parts(parts)) => Some(parts),
                _ => None,
            })
            .flatten()
            .filter_map(|part| match part {
                ContentPartWire::ImageUrl { image_url } => Some(image_url.url.clone()),
                ContentPartWire::Text { .. } => None,
            })
            .collect()
    }

    /// A refusal that echoes every image of the request: in its message,
    /// its code and an extra field, raw and with JSON's optional escapes.
    pub(crate) fn echoing_body(urls: &[String]) -> String {
        let all = urls.join(" ");
        let escaped = all.replace('/', "\\/").replace('+', "\\u002b");
        format!(
            r#"{{"error":{{"message":"invalid image: {all}","code":"{escaped}","type":"invalid_request_error","param":"{escaped}"}}}}"#
        )
    }

    /// Whether `text` holds any part of an image's base64, written raw or
    /// with JSON's escapes.
    pub(crate) fn discloses(text: &str, urls: &[String]) -> bool {
        let text = text.replace("\\/", "/").replace("\\u002b", "+");
        urls.iter().any(|url| {
            let (_, data) = url.split_once(',').expect("a data URL");
            data.as_bytes()
                .chunks(16)
                .filter(|window| window.len() == 16)
                .any(|window| text.contains(std::str::from_utf8(window).expect("ASCII")))
        })
    }

    #[test]
    fn a_refused_image_request_keeps_only_its_status() {
        for (case, conversation) in image_conversations() {
            assert!(conversation.carries_images(), "{case}");
            conversation.validate().expect(case);
            let urls = image_urls(&conversation);
            let body = echoing_body(&urls);
            assert!(discloses(&body, &urls), "the fixture echoes the images");
            for (status, retryable) in [
                (400, false),
                (401, false),
                (413, false),
                (408, true),
                (429, true),
                (500, true),
                (503, true),
            ] {
                let failure = rejected_failure(
                    &conversation,
                    status,
                    Some(urls.join(",")),
                    Some(body.clone()),
                );
                assert_eq!(
                    failure,
                    ProviderFailure {
                        message: format!("provider rejected image request (HTTP {status})"),
                        status_code: Some(status),
                        provider_code: None,
                        response_body: None,
                        request_id: None,
                        retry_after_ms: None,
                        retryable,
                    },
                    "{case}, HTTP {status}"
                );
                let error = ProviderExecutionError::Rejected { failure };
                assert!(!discloses(&error.to_string(), &urls), "{case}");
                for observation in error_observations(&error) {
                    assert!(!discloses(&format!("{observation:?}"), &urls), "{case}");
                }
            }
        }
    }

    #[test]
    fn a_refused_text_request_keeps_what_the_provider_said() {
        let body =
            r#"{"error":{"message":"The model `m` does not exist","code":"model_not_found"}}"#;
        for conversation in [
            ChatConversation {
                messages: vec![ChatMessage::user("hi")],
                ..ChatConversation::default()
            },
            ChatConversation {
                messages: vec![ChatMessage::user_parts(vec![ContentPartWire::Text {
                    text: "hi".to_owned(),
                }])],
                ..ChatConversation::default()
            },
        ] {
            assert!(!conversation.carries_images());
            for (status, retryable) in [(404, false), (503, true)] {
                assert_eq!(
                    rejected_failure(
                        &conversation,
                        status,
                        Some("req_123".to_owned()),
                        Some(body.to_owned())
                    ),
                    ProviderFailure {
                        message: "The model `m` does not exist".to_owned(),
                        status_code: Some(status),
                        provider_code: Some("model_not_found".to_owned()),
                        response_body: Some(body.to_owned()),
                        request_id: Some("req_123".to_owned()),
                        retry_after_ms: None,
                        retryable,
                    }
                );
            }
            assert_eq!(
                rejected_failure(
                    &conversation,
                    502,
                    None,
                    Some("<html>bad gateway</html>".into())
                ),
                ProviderFailure {
                    message: "provider returned HTTP 502".to_owned(),
                    status_code: Some(502),
                    response_body: Some("<html>bad gateway</html>".to_owned()),
                    retryable: true,
                    ..ProviderFailure::default()
                }
            );
        }
    }
}
