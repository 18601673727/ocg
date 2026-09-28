//! OCG-owned provider transport for explicitly migrated OpenAI-compatible
//! routes.
//!
//! This module is the narrow seam between the OCG Provider Gateway (owned by
//! the orchestration/control plane) and the fixed `aimux` 0.3.x transport
//! substrate:
//!
//! ```text
//! gateway (routing, SSE framing, dispatch authority)
//!         │  ChatRequest  ──▶  ProviderTransport::prepare   (pure, no network)
//!         │                     ProviderTransport::stream     (network starts here)
//!         ▼
//!   ChatEventStream  (normalized, index-stable streaming events + usage)
//!         │
//!   aimux 0.3.x  ──▶  upstream OpenAI-compatible provider
//! ```
//!
//! Design rules held by this module:
//!
//! - **No network until `stream`.** `prepare` only validates and translates, so
//!   the gateway can reject an unsupported request *before* dispatch and
//!   reserve spend without side effects.
//! - **Explicit or rejected.** [`ChatRequest`] uses `deny_unknown_fields` and a
//!   semantic `validate` pass. A field the transport does not forward is a
//!   pre-dispatch error, never a silent drop.
//! - **Stable tool-call indices.** aimux keys tool-call fragments by call id;
//!   [`ToolCallNormalizer`] assigns the OpenAI `tool_calls[].index` in first-seen
//!   order and preserves interleaved argument fragments.
//! - **Retries are off.** Retry identity belongs to OCG dispatch, not the
//!   transport, so both the provider config and the per-call option set
//!   `max_retries = 0`.
//! - **No credentials leave this module.** The API key is only placed into the
//!   aimux provider config at call time and is never formatted into an error.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::pin::Pin;

use aimux_core::content::ContentPart;
use aimux_core::error::{AiMuxError, ApiCallError};
use aimux_core::generate::{stream_text, GenerateTextOptions};
use aimux_core::message::{MessageContent, ModelMessage, ModelPrompt, Role};
use aimux_core::options::{ResponseFormat, ToolChoice};
use aimux_core::stream_part::StreamPart;
use aimux_core::tool::{FunctionTool, Tool};
use aimux_core::types::{FinishReasonUnified, ReasoningEffort, Usage};
use aimux_provider_utils::RetryConfig;
use aimux_providers::openai::{OpenAIConfig, OpenAIProvider};
use futures::{Stream, StreamExt};
use serde::Deserialize;
use serde_json::{json, Map, Value};

/// A normalized stream of provider events.
pub type ChatEventStream =
    Pin<Box<dyn Stream<Item = Result<ChatStreamEvent, TransportError>> + Send>>;

/// The aimux stream this module consumes. Kept as a private alias so the public
/// signature stays readable.
type AimuxStream = Pin<Box<dyn Stream<Item = Result<StreamPart, AiMuxError>> + Send>>;

// ─────────────────────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────────────────────

/// Configuration for one migrated OpenAI-compatible route.
///
/// Construction is network-free; the base URL and credentials are only used
/// when [`ProviderTransport::stream`] is called.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderTransportConfig {
    /// Provider API base, e.g. `https://api.openai.com/v1`. A trailing slash is
    /// normalized away when the aimux client is built.
    pub base_url: String,
    /// Bearer credential for the upstream provider.
    pub api_key: String,
    /// aimux provider identity. `openai` selects the full OpenAI-compatible
    /// profile; other registry names are accepted verbatim.
    pub provider: String,
    /// Extra headers merged into every upstream request.
    pub headers: BTreeMap<String, String>,
    /// Optional `OpenAI-Organization` header.
    pub organization: Option<String>,
    /// Optional `OpenAI-Project` header.
    pub project: Option<String>,
}

impl ProviderTransportConfig {
    /// Create a config with the full OpenAI-compatible profile and no extra
    /// headers.
    #[must_use]
    pub fn new(base_url: impl Into<String>, api_key: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            api_key: api_key.into(),
            provider: "openai".to_string(),
            headers: BTreeMap::new(),
            organization: None,
            project: None,
        }
    }

    #[must_use]
    pub fn with_provider(mut self, provider: impl Into<String>) -> Self {
        self.provider = provider.into();
        self
    }

    #[must_use]
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(name.into(), value.into());
        self
    }

    #[must_use]
    pub fn with_organization(mut self, organization: impl Into<String>) -> Self {
        self.organization = Some(organization.into());
        self
    }

    #[must_use]
    pub fn with_project(mut self, project: impl Into<String>) -> Self {
        self.project = Some(project.into());
        self
    }

    /// Validate fields that must be present before any dispatch.
    ///
    /// # Errors
    ///
    /// Returns [`TransportError::Build`] when `base_url` is empty.
    pub fn validate(&self) -> Result<(), TransportError> {
        if self.base_url.trim().is_empty() {
            return Err(TransportError::build("provider base_url must not be empty"));
        }
        Ok(())
    }
}

/// A stateless provider transport. Cloning shares nothing mutable; each call
/// builds its own aimux model.
#[derive(Debug, Clone)]
pub struct ProviderTransport {
    config: ProviderTransportConfig,
}

impl ProviderTransport {
    #[must_use]
    pub fn new(config: ProviderTransportConfig) -> Self {
        Self { config }
    }

    #[must_use]
    pub fn config(&self) -> &ProviderTransportConfig {
        &self.config
    }

    /// Validate and translate an OpenAI Chat Completions request into an
    /// aimux dispatch. Performs no network I/O.
    ///
    /// # Errors
    ///
    /// Returns a typed [`TransportError`] for an invalid request, an
    /// unsupported field, or an unusable transport configuration.
    pub fn prepare(&self, request: ChatRequest) -> Result<PreparedChat, TransportError> {
        self.config.validate()?;
        let prepared = request.into_prepared()?;
        Ok(prepared)
    }

    /// Parse, validate and translate a JSON body. Performs no network I/O.
    ///
    /// # Errors
    ///
    /// See [`ProviderTransport::prepare`] and [`ChatRequest::from_json`].
    pub fn prepare_json(&self, body: Value) -> Result<PreparedChat, TransportError> {
        self.prepare(ChatRequest::from_json(body)?)
    }

    /// Dispatch a prepared request and return normalized streaming events.
    ///
    /// This is the first and only method that performs network I/O.
    ///
    /// # Errors
    ///
    /// Returns a [`TransportError`] when the request cannot be built or the
    /// provider connection fails before the stream is established. Mid-stream
    /// failures surface as [`ChatStreamEvent::Error`] items.
    pub async fn stream(&self, prepared: PreparedChat) -> Result<ChatEventStream, TransportError> {
        let mut config = OpenAIConfig::new(self.config.api_key.clone())
            .with_base_url(self.config.base_url.clone())
            .with_provider(self.config.provider.clone())
            // Retry identity is OCG's; never retry inside the transport.
            .with_retry_config(RetryConfig {
                max_retries: 0,
                ..RetryConfig::default()
            });
        if let Some(organization) = &self.config.organization {
            config = config.with_org_id(organization.clone());
        }
        if let Some(project) = &self.config.project {
            config = config.with_project(project.clone());
        }
        if !self.config.headers.is_empty() {
            config = config.with_headers(self.config.headers.clone().into_iter().collect());
        }

        let provider = OpenAIProvider::new(config);
        let model = provider.model(prepared.model_id());
        let result = stream_text(&model, prepared.prompt, prepared.options).await?;
        Ok(normalize_stream(result.stream))
    }

    /// Convenience: prepare then dispatch.
    ///
    /// # Errors
    ///
    /// See [`ProviderTransport::prepare`] and [`ProviderTransport::stream`].
    pub async fn stream_chat(
        &self,
        request: ChatRequest,
    ) -> Result<ChatEventStream, TransportError> {
        let prepared = self.prepare(request)?;
        self.stream(prepared).await
    }
}

/// A validated, translated request ready for network dispatch.
#[derive(Debug, Clone)]
pub struct PreparedChat {
    model_id: String,
    prompt: ModelPrompt,
    options: GenerateTextOptions,
}

impl PreparedChat {
    #[must_use]
    pub fn model_id(&self) -> &str {
        &self.model_id
    }

    #[must_use]
    pub fn prompt(&self) -> &ModelPrompt {
        &self.prompt
    }

    /// The translated aimux options. Exposed as the translation seam for the
    /// gateway and for focused tests.
    #[must_use]
    pub fn options(&self) -> &GenerateTextOptions {
        &self.options
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Errors
// ─────────────────────────────────────────────────────────────────────────────

/// Provider-side failure detail, preserving the observable facts without
/// interpreting them into a routing decision.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProviderFailure {
    pub message: String,
    pub status_code: Option<u16>,
    pub provider_code: Option<String>,
    pub response_body: Option<String>,
    pub request_id: Option<String>,
    pub retry_after_ms: Option<u64>,
    pub retryable: bool,
}

impl fmt::Display for ProviderFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.status_code {
            Some(status) => write!(f, "HTTP {status}: {}", self.message),
            None => f.write_str(&self.message),
        }
    }
}

impl std::error::Error for ProviderFailure {}

/// Typed transport error.
///
/// The gateway classifies dispatch/settlement from this; the transport itself
/// never decides whether to retry, reroute or settle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportError {
    /// The client request is structurally invalid.
    InvalidRequest { field: String, message: String },
    /// The client request uses a field or feature this transport does not
    /// forward. Rejected before any dispatch.
    Unsupported { field: String, message: String },
    /// The request could not be translated into an aimux dispatch, or the
    /// transport configuration is unusable.
    Build { message: String },
    /// The upstream provider or transport failed.
    Provider(ProviderFailure),
}

impl TransportError {
    #[must_use]
    pub fn invalid(field: impl Into<String>, message: impl Into<String>) -> Self {
        Self::InvalidRequest {
            field: field.into(),
            message: message.into(),
        }
    }

    #[must_use]
    pub fn unsupported(field: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Unsupported {
            field: field.into(),
            message: message.into(),
        }
    }

    #[must_use]
    pub fn build(message: impl Into<String>) -> Self {
        Self::Build {
            message: message.into(),
        }
    }

    /// Whether the underlying provider error was marked retryable.
    /// OCG owns the decision to act on this.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Provider(failure) if failure.retryable)
    }

    /// The observed HTTP status, when one was seen.
    #[must_use]
    pub fn status_code(&self) -> Option<u16> {
        match self {
            Self::Provider(failure) => failure.status_code,
            _ => None,
        }
    }

    /// The provider's machine-readable code, when one was reported.
    #[must_use]
    pub fn provider_code(&self) -> Option<&str> {
        match self {
            Self::Provider(failure) => failure.provider_code.as_deref(),
            _ => None,
        }
    }
}

impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest { field, message } => {
                write!(f, "invalid chat request field `{field}`: {message}")
            }
            Self::Unsupported { field, message } => {
                write!(f, "unsupported chat request field `{field}`: {message}")
            }
            Self::Build { message } => write!(f, "could not build provider request: {message}"),
            Self::Provider(failure) => write!(f, "provider call failed: {failure}"),
        }
    }
}

impl std::error::Error for TransportError {}

impl From<AiMuxError> for TransportError {
    fn from(error: AiMuxError) -> Self {
        match error {
            AiMuxError::ApiCall(detail) => {
                let ApiCallError {
                    status_code,
                    provider_code,
                    message,
                    response_body,
                    request_id,
                    retry_after_ms,
                    is_retryable,
                } = detail;
                Self::Provider(ProviderFailure {
                    message,
                    status_code,
                    provider_code,
                    response_body,
                    request_id,
                    retry_after_ms,
                    retryable: is_retryable,
                })
            }
            AiMuxError::InvalidArgument(message) | AiMuxError::InvalidPrompt(message) => {
                Self::Build { message }
            }
            AiMuxError::TokenExpired(message) => Self::Provider(ProviderFailure {
                message,
                status_code: Some(401),
                ..ProviderFailure::default()
            }),
            AiMuxError::Timeout(message) => Self::Provider(ProviderFailure {
                message,
                ..ProviderFailure::default()
            }),
            AiMuxError::Aborted => Self::Provider(ProviderFailure {
                message: "provider request aborted".to_string(),
                ..ProviderFailure::default()
            }),
            other => {
                let retryable = other.is_retryable();
                Self::Provider(ProviderFailure {
                    message: other.to_string(),
                    retryable,
                    ..ProviderFailure::default()
                })
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Normalized stream events and usage
// ─────────────────────────────────────────────────────────────────────────────

/// Normalized token usage. Unreported counters stay `None`; they are never
/// fabricated as zero.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct NormalizedUsage {
    pub input_tokens: Option<u32>,
    pub output_tokens: Option<u32>,
    pub cache_read_tokens: Option<u32>,
    pub cache_write_tokens: Option<u32>,
    pub reasoning_tokens: Option<u32>,
    /// The provider's original `usage` object, when reported.
    pub raw: Option<Value>,
}

impl NormalizedUsage {
    /// Sum of the reported input and output totals, when both are present.
    #[must_use]
    pub fn total_tokens(&self) -> Option<u32> {
        match (self.input_tokens, self.output_tokens) {
            (Some(input), Some(output)) => Some(input.saturating_add(output)),
            _ => None,
        }
    }

    fn from_aimux(usage: &Usage) -> Self {
        Self {
            input_tokens: usage.input_tokens.total,
            output_tokens: usage.output_tokens.total,
            cache_read_tokens: usage.input_tokens.cache_read,
            cache_write_tokens: usage.input_tokens.cache_write,
            reasoning_tokens: usage.output_tokens.reasoning,
            raw: usage.raw.clone(),
        }
    }
}

/// OpenAI-compatible finish reason for the gateway's SSE framing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChatFinishReason {
    Stop,
    Length,
    ToolCalls,
    ContentFilter,
    Error,
    Other,
}

impl ChatFinishReason {
    /// The OpenAI `finish_reason` string for this reason.
    #[must_use]
    pub fn as_openai_str(self) -> &'static str {
        match self {
            Self::Stop => "stop",
            Self::Length => "length",
            Self::ToolCalls => "tool_calls",
            Self::ContentFilter => "content_filter",
            Self::Error => "error",
            Self::Other => "other",
        }
    }

    #[must_use]
    pub fn from_unified(reason: FinishReasonUnified) -> Self {
        match reason {
            FinishReasonUnified::Stop => Self::Stop,
            FinishReasonUnified::Length => Self::Length,
            FinishReasonUnified::ToolCalls => Self::ToolCalls,
            FinishReasonUnified::ContentFilter => Self::ContentFilter,
            FinishReasonUnified::Error => Self::Error,
            FinishReasonUnified::Other => Self::Other,
        }
    }
}

/// One normalized provider event.
///
/// Tool-call events carry the OpenAI `index` so interleaved fragments from
/// multiple calls reconstruct in the gateway without re-deriving order.
#[derive(Debug, Clone, PartialEq)]
pub enum ChatStreamEvent {
    TextDelta {
        delta: String,
    },
    ReasoningDelta {
        delta: String,
    },
    ToolCallStart {
        index: u32,
        id: String,
        name: String,
    },
    ToolCallArgumentsDelta {
        index: u32,
        id: String,
        delta: String,
    },
    ToolCallComplete {
        index: u32,
        id: String,
        name: String,
        /// JSON-encoded arguments, as on the OpenAI wire.
        arguments: String,
    },
    Metadata {
        id: Option<String>,
        model: Option<String>,
        timestamp: Option<String>,
    },
    Finish {
        reason: ChatFinishReason,
        raw_reason: Option<String>,
        usage: NormalizedUsage,
    },
    Error(TransportError),
}

/// A completed tool call reconstructed from a normalized stream.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompletedToolCall {
    pub index: u32,
    pub id: String,
    pub name: String,
    /// JSON-encoded arguments, as on the OpenAI wire.
    pub arguments: String,
}

/// The accumulated result of a normalized stream, suitable for usage
/// settlement or tests. Fragment order is not preserved here; the streaming
/// events are the source of truth for reconstruction.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ChatStreamSummary {
    pub text: String,
    pub reasoning: String,
    pub tool_calls: Vec<CompletedToolCall>,
    pub usage: NormalizedUsage,
    pub finish_reason: Option<ChatFinishReason>,
    pub raw_finish_reason: Option<String>,
}

impl ChatStreamSummary {
    /// Fold one normalized event into the summary.
    pub fn apply(&mut self, event: &ChatStreamEvent) {
        match event {
            ChatStreamEvent::TextDelta { delta } => self.text.push_str(delta),
            ChatStreamEvent::ReasoningDelta { delta } => self.reasoning.push_str(delta),
            ChatStreamEvent::ToolCallStart { index, id, name } => {
                let slot = self.slot(*index);
                slot.id.clone_from(id);
                slot.name.clone_from(name);
                slot.arguments.clear();
            }
            ChatStreamEvent::ToolCallArgumentsDelta { index, id, delta } => {
                let slot = self.slot(*index);
                if slot.id.is_empty() {
                    slot.id.clone_from(id);
                }
                slot.arguments.push_str(delta);
            }
            ChatStreamEvent::ToolCallComplete {
                index,
                id,
                name,
                arguments,
            } => {
                let slot = self.slot(*index);
                slot.id.clone_from(id);
                slot.name.clone_from(name);
                slot.arguments.clone_from(arguments);
            }
            ChatStreamEvent::Finish {
                reason,
                raw_reason,
                usage,
            } => {
                self.usage.clone_from(usage);
                self.finish_reason = Some(*reason);
                self.raw_finish_reason.clone_from(raw_reason);
            }
            ChatStreamEvent::Metadata { .. } | ChatStreamEvent::Error(_) => {}
        }
    }

    fn slot(&mut self, index: u32) -> &mut CompletedToolCall {
        match self
            .tool_calls
            .binary_search_by_key(&index, |call| call.index)
        {
            Ok(position) => &mut self.tool_calls[position],
            Err(position) => {
                self.tool_calls.insert(
                    position,
                    CompletedToolCall {
                        index,
                        ..CompletedToolCall::default()
                    },
                );
                &mut self.tool_calls[position]
            }
        }
    }
}

/// Drain a normalized stream into a [`ChatStreamSummary`].
///
/// # Errors
///
/// Returns the first [`TransportError`] item in the stream.
pub async fn collect_events(
    mut stream: ChatEventStream,
) -> Result<ChatStreamSummary, TransportError> {
    let mut summary = ChatStreamSummary::default();
    while let Some(item) = stream.next().await {
        match item {
            Ok(event) => summary.apply(&event),
            Err(error) => return Err(error),
        }
    }
    Ok(summary)
}

/// Assigns stable OpenAI `tool_calls[].index` values to aimux tool-call
/// fragments and keeps interleaved fragments attached to the right call.
#[derive(Debug, Clone, Default)]
pub struct ToolCallNormalizer {
    slots: Vec<ToolCallSlot>,
    by_id: HashMap<String, usize>,
}

#[derive(Debug, Clone, Default)]
struct ToolCallSlot {
    name: String,
}

impl ToolCallNormalizer {
    /// Resolve (or assign) the index for a tool call id. `name` is recorded
    /// when a start or complete event supplies it.
    pub fn index_for(&mut self, id: &str, name: Option<&str>) -> u32 {
        if let Some(&position) = self.by_id.get(id) {
            if let Some(name) = name {
                if !name.is_empty() {
                    self.slots[position].name = name.to_string();
                }
            }
            return u32::try_from(position).unwrap_or(u32::MAX);
        }
        let position = self.slots.len();
        self.slots.push(ToolCallSlot {
            name: name.unwrap_or_default().to_string(),
        });
        self.by_id.insert(id.to_string(), position);
        u32::try_from(position).unwrap_or(u32::MAX)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }
}

/// Translate one aimux stream part into a normalized event, if it maps to one.
/// Parts already represented by the normalized vocabulary (text/reasoning
/// segment boundaries, raw chunks, sources, files) are dropped.
pub fn translate_stream_part(
    part: StreamPart,
    normalizer: &mut ToolCallNormalizer,
) -> Option<ChatStreamEvent> {
    match part {
        StreamPart::TextDelta { delta, .. } => Some(ChatStreamEvent::TextDelta { delta }),
        StreamPart::ReasoningDelta { delta, .. } => Some(ChatStreamEvent::ReasoningDelta { delta }),
        StreamPart::ToolInputStart { id, tool_name, .. } => {
            let index = normalizer.index_for(&id, Some(&tool_name));
            Some(ChatStreamEvent::ToolCallStart {
                index,
                id,
                name: tool_name,
            })
        }
        StreamPart::ToolInputDelta { id, delta, .. } => {
            let index = normalizer.index_for(&id, None);
            Some(ChatStreamEvent::ToolCallArgumentsDelta { index, id, delta })
        }
        StreamPart::ToolCall {
            tool_call_id,
            tool_name,
            input,
            ..
        } => {
            let index = normalizer.index_for(&tool_call_id, Some(&tool_name));
            let arguments = serde_json::to_string(&input).unwrap_or_else(|_| input.to_string());
            Some(ChatStreamEvent::ToolCallComplete {
                index,
                id: tool_call_id,
                name: tool_name,
                arguments,
            })
        }
        StreamPart::Finish {
            finish_reason,
            usage,
            ..
        } => {
            let unified = finish_reason.unified;
            Some(ChatStreamEvent::Finish {
                reason: ChatFinishReason::from_unified(unified),
                raw_reason: finish_reason.raw,
                usage: NormalizedUsage::from_aimux(&usage),
            })
        }
        StreamPart::Error { error } => Some(ChatStreamEvent::Error(error.into())),
        StreamPart::ResponseMetadata {
            id,
            timestamp,
            model_id,
        } => Some(ChatStreamEvent::Metadata {
            id,
            model: model_id,
            timestamp,
        }),
        _ => None,
    }
}

fn normalize_stream(inner: AimuxStream) -> ChatEventStream {
    let state = (inner, ToolCallNormalizer::default());
    Box::pin(futures::stream::unfold(
        state,
        |(mut inner, mut normalizer)| async move {
            loop {
                match inner.next().await {
                    Some(Ok(part)) => {
                        if let Some(event) = translate_stream_part(part, &mut normalizer) {
                            return Some((Ok(event), (inner, normalizer)));
                        }
                    }
                    Some(Err(error)) => {
                        return Some((Err(error.into()), (inner, normalizer)));
                    }
                    None => return None,
                }
            }
        },
    ))
}

// ─────────────────────────────────────────────────────────────────────────────
// OpenAI Chat Completions request contract
// ─────────────────────────────────────────────────────────────────────────────

/// An OpenAI Chat Completions request, restricted to the fields this transport
/// explicitly forwards. Unknown fields are rejected on deserialization.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChatRequest {
    pub model: String,
    pub messages: Vec<ChatMessage>,

    #[serde(default)]
    pub stream: Option<bool>,
    #[serde(default)]
    pub stream_options: Option<StreamOptions>,
    #[serde(default)]
    pub store: Option<bool>,

    #[serde(default)]
    pub temperature: Option<f64>,
    #[serde(default)]
    pub top_p: Option<f64>,
    #[serde(default)]
    pub top_k: Option<f64>,
    #[serde(default)]
    pub max_tokens: Option<u32>,
    #[serde(default)]
    pub max_completion_tokens: Option<u32>,
    #[serde(default)]
    pub stop: Option<StopSequences>,
    #[serde(default)]
    pub presence_penalty: Option<f64>,
    #[serde(default)]
    pub frequency_penalty: Option<f64>,
    #[serde(default)]
    pub logit_bias: Option<BTreeMap<String, f64>>,
    #[serde(default)]
    pub logprobs: Option<bool>,
    #[serde(default)]
    pub top_logprobs: Option<u32>,
    #[serde(default)]
    pub n: Option<u32>,
    #[serde(default)]
    pub seed: Option<u64>,
    #[serde(default)]
    pub user: Option<String>,
    #[serde(default)]
    pub metadata: Option<BTreeMap<String, Value>>,
    #[serde(default)]
    pub response_format: Option<ResponseFormatWire>,
    #[serde(default)]
    pub tools: Option<Vec<ToolWire>>,
    #[serde(default)]
    pub tool_choice: Option<ToolChoiceWire>,
    #[serde(default)]
    pub parallel_tool_calls: Option<bool>,
    #[serde(default)]
    pub service_tier: Option<String>,
    #[serde(default)]
    pub reasoning_effort: Option<String>,
}

impl ChatRequest {
    /// Deserialize a request body. Performs no network I/O.
    ///
    /// # Errors
    ///
    /// Returns [`TransportError::InvalidRequest`] when the body does not match
    /// the supported OpenAI Chat Completions contract.
    pub fn from_json(body: Value) -> Result<Self, TransportError> {
        serde_json::from_value(body).map_err(|error| {
            TransportError::invalid("request", format!("could not parse chat request: {error}"))
        })
    }

    /// Deserialize a request body from bytes. Performs no network I/O.
    ///
    /// # Errors
    ///
    /// See [`ChatRequest::from_json`].
    pub fn from_slice(body: &[u8]) -> Result<Self, TransportError> {
        serde_json::from_slice(body).map_err(|error| {
            TransportError::invalid("request", format!("could not parse chat request: {error}"))
        })
    }

    /// Validate every field before dispatch.
    ///
    /// # Errors
    ///
    /// Returns a typed [`TransportError`] for the first unsupported or invalid
    /// field.
    pub fn validate(&self) -> Result<(), TransportError> {
        if self.model.trim().is_empty() {
            return Err(TransportError::invalid("model", "model must not be empty"));
        }
        if self.messages.is_empty() {
            return Err(TransportError::invalid(
                "messages",
                "at least one message is required",
            ));
        }
        if self.stream == Some(false) {
            return Err(TransportError::unsupported(
                "stream",
                "only streaming chat completions are supported",
            ));
        }
        if let Some(options) = &self.stream_options {
            if options.include_usage == Some(false) {
                return Err(TransportError::unsupported(
                    "stream_options.include_usage",
                    "usage must be included for settlement",
                ));
            }
        }
        if let Some(n) = self.n {
            if n != 1 {
                return Err(TransportError::unsupported("n", "only n = 1 is supported"));
            }
        }
        if self.max_tokens.is_some() && self.max_completion_tokens.is_some() {
            return Err(TransportError::invalid(
                "max_tokens",
                "max_tokens and max_completion_tokens are mutually exclusive",
            ));
        }
        if let Some(effort) = &self.reasoning_effort {
            parse_reasoning_effort(effort)?;
        }
        if let Some(format) = &self.response_format {
            format.validate()?;
        }
        if let Some(tool_choice) = &self.tool_choice {
            validate_tool_choice(tool_choice)?;
        }
        if let Some(tools) = &self.tools {
            for (index, tool) in tools.iter().enumerate() {
                tool.validate(index)?;
            }
        }
        for (index, message) in self.messages.iter().enumerate() {
            message.validate(index)?;
        }
        Ok(())
    }

    fn into_prepared(self) -> Result<PreparedChat, TransportError> {
        self.validate()?;
        let model_id = self.model.clone();
        let prompt = self.build_prompt()?;
        let options = self.build_options()?;
        Ok(PreparedChat {
            model_id,
            prompt,
            options,
        })
    }

    fn build_prompt(&self) -> Result<ModelPrompt, TransportError> {
        let messages = self
            .messages
            .iter()
            .enumerate()
            .map(|(index, message)| message.to_model_message(index))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ModelPrompt::Messages(messages))
    }

    fn build_options(&self) -> Result<GenerateTextOptions, TransportError> {
        let mut overrides = Map::new();

        // The capture contract: stream with usage, store disabled by default.
        overrides.insert("stream".to_string(), json!(true));
        overrides.insert(
            "stream_options".to_string(),
            json!({ "include_usage": true }),
        );
        overrides.insert("store".to_string(), json!(self.store.unwrap_or(false)));

        // Preserve the exact max-token key the client chose and delete the
        // other one, which aimux may have inserted by model-capability guess.
        if let Some(max_tokens) = self.max_tokens {
            overrides.insert("max_tokens".to_string(), json!(max_tokens));
            overrides.insert("max_completion_tokens".to_string(), Value::Null);
        }
        if let Some(max_completion_tokens) = self.max_completion_tokens {
            overrides.insert(
                "max_completion_tokens".to_string(),
                json!(max_completion_tokens),
            );
            overrides.insert("max_tokens".to_string(), Value::Null);
        }

        // Fields aimux models only through provider options; forward them as
        // explicit body overrides so nothing is silently dropped.
        if let Some(parallel) = self.parallel_tool_calls {
            overrides.insert("parallel_tool_calls".to_string(), json!(parallel));
        }
        if let Some(logit_bias) = &self.logit_bias {
            overrides.insert("logit_bias".to_string(), json!(logit_bias));
        }
        if let Some(user) = &self.user {
            overrides.insert("user".to_string(), json!(user));
        }
        if let Some(metadata) = &self.metadata {
            overrides.insert("metadata".to_string(), json!(metadata));
        }
        if let Some(service_tier) = &self.service_tier {
            overrides.insert("service_tier".to_string(), json!(service_tier));
        }
        if let Some(logprobs) = self.logprobs {
            overrides.insert("logprobs".to_string(), json!(logprobs));
        }
        if let Some(top_logprobs) = self.top_logprobs {
            overrides.insert("top_logprobs".to_string(), json!(top_logprobs));
        }
        if let Some(format) = &self.response_format {
            // aimux hardcodes `strict: true` for JSON schema on non-Groq
            // providers, so re-assert the exact wire shape the client sent.
            overrides.insert("response_format".to_string(), format.to_openai_json());
        }

        let response_format = self
            .response_format
            .as_ref()
            .map(ResponseFormatWire::to_aimux)
            .transpose()?;
        let tools = self
            .tools
            .as_ref()
            .map(|tools| build_tools(tools))
            .transpose()?;
        let tool_choice = self
            .tool_choice
            .as_ref()
            .map(ToolChoiceWire::to_aimux)
            .transpose()?;
        let reasoning = self
            .reasoning_effort
            .as_deref()
            .map(parse_reasoning_effort)
            .transpose()?;
        let stop_sequences = self.stop.as_ref().map(StopSequences::to_vec);

        Ok(GenerateTextOptions {
            max_output_tokens: self.max_tokens.or(self.max_completion_tokens),
            temperature: self.temperature,
            stop_sequences,
            top_p: self.top_p,
            top_k: self.top_k,
            presence_penalty: self.presence_penalty,
            frequency_penalty: self.frequency_penalty,
            response_format,
            seed: self.seed,
            tools,
            tool_choice,
            reasoning,
            // OCG owns retry identity; disable aimux retries at the call too.
            max_retries: Some(0),
            body_overrides: Some(Value::Object(overrides)),
            ..GenerateTextOptions::default()
        })
    }
}

/// OpenAI `stream_options`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamOptions {
    #[serde(default)]
    pub include_usage: Option<bool>,
}

/// OpenAI `stop`: either one string or an array of strings.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum StopSequences {
    One(String),
    Many(Vec<String>),
}

impl StopSequences {
    #[must_use]
    pub fn to_vec(&self) -> Vec<String> {
        match self {
            Self::One(one) => vec![one.clone()],
            Self::Many(many) => many.clone(),
        }
    }
}

/// One message in the request.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChatMessage {
    pub role: ChatRole,
    #[serde(default)]
    pub content: Option<MessageContentWire>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub tool_calls: Option<Vec<ChatToolCallWire>>,
    #[serde(default)]
    pub tool_call_id: Option<String>,
    #[serde(default)]
    pub reasoning_content: Option<String>,
    #[serde(default)]
    pub refusal: Option<String>,
}

impl ChatMessage {
    fn validate(&self, index: usize) -> Result<(), TransportError> {
        let field = |suffix: &str| format!("messages[{index}].{suffix}");
        if let Some(name) = &self.name {
            return Err(TransportError::unsupported(
                field("name"),
                format!("per-message names are not forwarded (`{name}`)"),
            ));
        }
        if let Some(refusal) = &self.refusal {
            return Err(TransportError::unsupported(
                field("refusal"),
                format!("assistant refusals are not forwarded (`{refusal}`)"),
            ));
        }
        match self.role {
            ChatRole::Function => Err(TransportError::unsupported(
                field("role"),
                "the legacy `function` role is not supported",
            )),
            ChatRole::System | ChatRole::Developer => {
                self.require_text_content(&field("content"))?;
                self.reject_tool_fields(index)?;
                Ok(())
            }
            ChatRole::User => {
                self.require_text_content(&field("content"))?;
                self.reject_tool_fields(index)?;
                Ok(())
            }
            ChatRole::Assistant => {
                if let Some(calls) = &self.tool_calls {
                    for (call_index, call) in calls.iter().enumerate() {
                        call.validate(&field(&format!("tool_calls[{call_index}]")))?;
                    }
                }
                let has_parts = self.content.is_some()
                    || self
                        .tool_calls
                        .as_ref()
                        .is_some_and(|calls| !calls.is_empty())
                    || self
                        .reasoning_content
                        .as_ref()
                        .is_some_and(|reasoning| !reasoning.is_empty());
                if !has_parts {
                    return Err(TransportError::invalid(
                        field("content"),
                        "assistant messages must carry content, reasoning or tool_calls",
                    ));
                }
                if self.tool_call_id.is_some() {
                    return Err(TransportError::invalid(
                        field("tool_call_id"),
                        "assistant messages must not carry tool_call_id",
                    ));
                }
                Ok(())
            }
            ChatRole::Tool => {
                if self.tool_call_id.is_none() {
                    return Err(TransportError::invalid(
                        field("tool_call_id"),
                        "tool messages require tool_call_id",
                    ));
                }
                if self.tool_calls.is_some() {
                    return Err(TransportError::invalid(
                        field("tool_calls"),
                        "tool messages must not carry tool_calls",
                    ));
                }
                self.require_text_content(&field("content"))?;
                Ok(())
            }
        }
    }

    fn reject_tool_fields(&self, index: usize) -> Result<(), TransportError> {
        let field = |suffix: &str| format!("messages[{index}].{suffix}");
        if self.tool_calls.is_some() {
            return Err(TransportError::invalid(
                field("tool_calls"),
                "tool_calls are only valid on assistant messages",
            ));
        }
        if self.tool_call_id.is_some() {
            return Err(TransportError::invalid(
                field("tool_call_id"),
                "tool_call_id is only valid on tool messages",
            ));
        }
        if self.reasoning_content.is_some() {
            return Err(TransportError::invalid(
                field("reasoning_content"),
                "reasoning_content is only valid on assistant messages",
            ));
        }
        Ok(())
    }

    fn require_text_content(&self, field: &str) -> Result<Vec<ContentPart>, TransportError> {
        let content = self.content.as_ref().ok_or_else(|| {
            TransportError::invalid(field.to_string(), "message content is required")
        })?;
        content.to_text_parts(field)
    }

    fn to_model_message(&self, index: usize) -> Result<ModelMessage, TransportError> {
        let field = |suffix: &str| format!("messages[{index}].{suffix}");
        match self.role {
            ChatRole::System | ChatRole::Developer => {
                let parts = self.require_text_content(&field("content"))?;
                Ok(ModelMessage {
                    role: Role::System,
                    content: MessageContent::Parts(parts),
                })
            }
            ChatRole::User => {
                let parts = self.require_text_content(&field("content"))?;
                Ok(ModelMessage {
                    role: Role::User,
                    content: MessageContent::Parts(parts),
                })
            }
            ChatRole::Assistant => {
                let mut parts = Vec::new();
                if let Some(reasoning) = &self.reasoning_content {
                    if !reasoning.is_empty() {
                        parts.push(ContentPart::reasoning(reasoning.clone()));
                    }
                }
                if let Some(content) = &self.content {
                    parts.extend(content.to_text_parts(&field("content"))?);
                }
                if let Some(calls) = &self.tool_calls {
                    for (call_index, call) in calls.iter().enumerate() {
                        let call_field = field(&format!("tool_calls[{call_index}]"));
                        if let Some(kind) = &call.tool_type {
                            if kind != "function" {
                                return Err(TransportError::unsupported(
                                    format!("{call_field}.type"),
                                    format!("unsupported tool call type `{kind}`"),
                                ));
                            }
                        }
                        let input = parse_tool_arguments(
                            &call.function.arguments,
                            &format!("{call_field}.function.arguments"),
                        )?;
                        parts.push(ContentPart::tool_call(
                            call.id.clone(),
                            call.function.name.clone(),
                            input,
                        ));
                    }
                }
                Ok(ModelMessage {
                    role: Role::Assistant,
                    content: MessageContent::Parts(parts),
                })
            }
            ChatRole::Tool => {
                let tool_call_id = self.tool_call_id.clone().ok_or_else(|| {
                    TransportError::invalid(
                        field("tool_call_id"),
                        "tool messages require tool_call_id",
                    )
                })?;
                let parts = self.require_text_content(&field("content"))?;
                let result = collect_text(parts);
                Ok(ModelMessage {
                    role: Role::Tool,
                    content: MessageContent::Parts(vec![ContentPart::tool_result(
                        tool_call_id,
                        Value::String(result),
                    )]),
                })
            }
            ChatRole::Function => Err(TransportError::unsupported(
                field("role"),
                "the legacy `function` role is not supported",
            )),
        }
    }
}

fn collect_text(parts: Vec<ContentPart>) -> String {
    let mut text = String::new();
    for part in parts {
        if let ContentPart::Text { text: value, .. } = part {
            text.push_str(&value);
        }
    }
    text
}

/// Message role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChatRole {
    System,
    User,
    Assistant,
    Tool,
    Developer,
    Function,
}

/// Message content: a string or an array of content parts.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum MessageContentWire {
    Text(String),
    Parts(Vec<ContentPartWire>),
}

impl MessageContentWire {
    fn to_text_parts(&self, field: &str) -> Result<Vec<ContentPart>, TransportError> {
        let parts: Vec<ContentPart> = match self {
            Self::Text(text) => vec![ContentPart::text(text.clone())],
            Self::Parts(parts) => {
                if parts.is_empty() {
                    return Err(TransportError::invalid(
                        field.to_string(),
                        "content parts must not be empty",
                    ));
                }
                parts
                    .iter()
                    .map(|part| match part {
                        ContentPartWire::Text { text } => ContentPart::text(text.clone()),
                    })
                    .collect()
            }
        };
        Ok(parts)
    }
}

/// A supported content part. Non-text parts (images, audio, files) are
/// rejected during deserialization.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ContentPartWire {
    Text { text: String },
}

/// One tool definition.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolWire {
    #[serde(rename = "type")]
    pub tool_type: String,
    pub function: FunctionDefinitionWire,
}

impl ToolWire {
    fn validate(&self, index: usize) -> Result<(), TransportError> {
        if self.tool_type != "function" {
            return Err(TransportError::unsupported(
                format!("tools[{index}].type"),
                format!("unsupported tool type `{}`", self.tool_type),
            ));
        }
        if self.function.name.trim().is_empty() {
            return Err(TransportError::invalid(
                format!("tools[{index}].function.name"),
                "tool name must not be empty",
            ));
        }
        Ok(())
    }
}

/// A function tool definition.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FunctionDefinitionWire {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub parameters: Option<Value>,
    #[serde(default)]
    pub strict: Option<bool>,
}

fn build_tools(tools: &[ToolWire]) -> Result<Vec<Tool>, TransportError> {
    tools
        .iter()
        .enumerate()
        .map(|(index, tool)| {
            tool.validate(index)?;
            let schema = tool
                .function
                .parameters
                .clone()
                .unwrap_or_else(|| json!({ "type": "object" }));
            let mut function = FunctionTool::new(tool.function.name.clone(), schema);
            function.description.clone_from(&tool.function.description);
            // Preserve an explicit `strict: false`; it is meaningful.
            function.strict = tool.function.strict;
            Ok(Tool::Function(function))
        })
        .collect()
}

/// One tool call in an assistant message.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChatToolCallWire {
    pub id: String,
    #[serde(rename = "type", default)]
    pub tool_type: Option<String>,
    pub function: ChatFunctionCallWire,
}

impl ChatToolCallWire {
    fn validate(&self, field: &str) -> Result<(), TransportError> {
        if self.id.trim().is_empty() {
            return Err(TransportError::invalid(
                format!("{field}.id"),
                "tool call id must not be empty",
            ));
        }
        if let Some(kind) = &self.tool_type {
            if kind != "function" {
                return Err(TransportError::unsupported(
                    format!("{field}.type"),
                    format!("unsupported tool call type `{kind}`"),
                ));
            }
        }
        if self.function.name.trim().is_empty() {
            return Err(TransportError::invalid(
                format!("{field}.function.name"),
                "tool name must not be empty",
            ));
        }
        parse_tool_arguments(
            &self.function.arguments,
            &format!("{field}.function.arguments"),
        )?;
        Ok(())
    }
}

/// A function call in an assistant message.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChatFunctionCallWire {
    pub name: String,
    #[serde(default)]
    pub arguments: String,
}

fn parse_tool_arguments(raw: &str, field: &str) -> Result<Value, TransportError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(Value::Object(Map::new()));
    }
    serde_json::from_str::<Value>(trimmed).map_err(|error| {
        TransportError::invalid(
            field.to_string(),
            format!("tool arguments must be valid JSON: {error}"),
        )
    })
}

/// Tool selection.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum ToolChoiceWire {
    Mode(String),
    Named(NamedToolChoiceWire),
}

/// Named tool selection object.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamedToolChoiceWire {
    #[serde(rename = "type")]
    pub kind: String,
    pub function: NamedFunctionWire,
}

/// Named function reference in a tool choice.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamedFunctionWire {
    pub name: String,
}

fn validate_tool_choice(choice: &ToolChoiceWire) -> Result<(), TransportError> {
    match choice {
        ToolChoiceWire::Mode(mode) => match mode.as_str() {
            "auto" | "none" | "required" => Ok(()),
            other => Err(TransportError::invalid(
                "tool_choice",
                format!("unknown tool_choice `{other}`"),
            )),
        },
        ToolChoiceWire::Named(named) => {
            if named.kind != "function" {
                return Err(TransportError::unsupported(
                    "tool_choice.type",
                    format!("unsupported tool_choice type `{}`", named.kind),
                ));
            }
            if named.function.name.trim().is_empty() {
                return Err(TransportError::invalid(
                    "tool_choice.function.name",
                    "tool_choice function name must not be empty",
                ));
            }
            Ok(())
        }
    }
}

impl ToolChoiceWire {
    fn to_aimux(&self) -> Result<ToolChoice, TransportError> {
        validate_tool_choice(self)?;
        Ok(match self {
            Self::Mode(mode) => match mode.as_str() {
                "auto" => ToolChoice::Auto,
                "none" => ToolChoice::None,
                "required" => ToolChoice::Required,
                other => {
                    return Err(TransportError::invalid(
                        "tool_choice",
                        format!("unknown tool_choice `{other}`"),
                    ));
                }
            },
            Self::Named(named) => ToolChoice::Tool {
                tool_name: named.function.name.clone(),
            },
        })
    }
}

/// Response format.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResponseFormatWire {
    Text {},
    JsonObject {},
    JsonSchema { json_schema: JsonSchemaWire },
}

impl ResponseFormatWire {
    fn validate(&self) -> Result<(), TransportError> {
        if let Self::JsonSchema { json_schema } = self {
            if json_schema.schema.is_none() {
                return Err(TransportError::invalid(
                    "response_format.json_schema.schema",
                    "a JSON schema is required for response_format type json_schema",
                ));
            }
        }
        Ok(())
    }

    fn to_aimux(&self) -> Result<ResponseFormat, TransportError> {
        self.validate()?;
        Ok(match self {
            Self::Text {} => ResponseFormat::Text,
            Self::JsonObject {} => ResponseFormat::Json {
                schema: None,
                name: None,
                description: None,
            },
            Self::JsonSchema { json_schema } => ResponseFormat::Json {
                schema: json_schema.schema.clone(),
                name: json_schema.name.clone(),
                description: json_schema.description.clone(),
            },
        })
    }

    fn to_openai_json(&self) -> Value {
        match self {
            Self::Text {} => json!({ "type": "text" }),
            Self::JsonObject {} => json!({ "type": "json_object" }),
            Self::JsonSchema { json_schema } => {
                let mut wire = Map::new();
                if let Some(name) = &json_schema.name {
                    wire.insert("name".to_string(), json!(name));
                }
                if let Some(description) = &json_schema.description {
                    wire.insert("description".to_string(), json!(description));
                }
                if let Some(schema) = &json_schema.schema {
                    wire.insert("schema".to_string(), schema.clone());
                }
                if let Some(strict) = json_schema.strict {
                    wire.insert("strict".to_string(), json!(strict));
                }
                json!({ "type": "json_schema", "json_schema": Value::Object(wire) })
            }
        }
    }
}

/// A structured-output schema.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JsonSchemaWire {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub schema: Option<Value>,
    #[serde(default)]
    pub strict: Option<bool>,
}

fn parse_reasoning_effort(effort: &str) -> Result<ReasoningEffort, TransportError> {
    match effort {
        "none" => Ok(ReasoningEffort::None),
        "minimal" => Ok(ReasoningEffort::Minimal),
        "low" => Ok(ReasoningEffort::Low),
        "medium" => Ok(ReasoningEffort::Medium),
        "high" => Ok(ReasoningEffort::High),
        "xhigh" => Ok(ReasoningEffort::Xhigh),
        other => Err(TransportError::invalid(
            "reasoning_effort",
            format!("unknown reasoning_effort `{other}`"),
        )),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Focused translation tests (no network)
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use aimux_core::tool::Tool;
    use aimux_core::types::{FinishReason, FinishReasonUnified, TokenUsage, Usage};

    fn transport() -> ProviderTransport {
        ProviderTransport::new(ProviderTransportConfig::new(
            "http://127.0.0.1:1/v1",
            "test-key",
        ))
    }

    fn request(body: Value) -> ChatRequest {
        ChatRequest::from_json(body).expect("request should parse")
    }

    fn start(id: &str, name: &str) -> StreamPart {
        StreamPart::ToolInputStart {
            id: id.to_string(),
            tool_name: name.to_string(),
            provider_executed: None,
            dynamic: None,
            title: None,
            provider_metadata: None,
        }
    }

    fn delta(id: &str, fragment: &str) -> StreamPart {
        StreamPart::ToolInputDelta {
            id: id.to_string(),
            delta: fragment.to_string(),
            provider_metadata: None,
        }
    }

    fn translate(parts: Vec<StreamPart>) -> Vec<ChatStreamEvent> {
        let mut normalizer = ToolCallNormalizer::default();
        parts
            .into_iter()
            .filter_map(|part| translate_stream_part(part, &mut normalizer))
            .collect()
    }

    fn full_request() -> Value {
        json!({
            "model": "test-model",
            "stream": true,
            "stream_options": { "include_usage": true },
            "store": false,
            "reasoning_effort": "high",
            "temperature": 0.2,
            "top_p": 0.9,
            "max_completion_tokens": 256,
            "seed": 7,
            "stop": ["STOP"],
            "metadata": { "trace": "abc" },
            "parallel_tool_calls": false,
            "tools": [{
                "type": "function",
                "function": {
                    "name": "tool_a",
                    "description": "a tool",
                    "parameters": { "type": "object", "properties": { "x": { "type": "integer" } } },
                    "strict": false
                }
            }],
            "tool_choice": { "type": "function", "function": { "name": "tool_a" } },
            "messages": [
                { "role": "system", "content": "sys" },
                { "role": "user", "content": "hi" },
                {
                    "role": "assistant",
                    "content": null,
                    "tool_calls": [
                        { "id": "call_a", "type": "function", "function": { "name": "tool_a", "arguments": "{\"x\":1}" } }
                    ]
                },
                { "role": "tool", "tool_call_id": "call_a", "content": "42" }
            ]
        })
    }

    #[test]
    fn prepares_full_openai_chat_request() {
        let parsed = request(full_request());
        let prepared = transport().prepare(parsed).expect("prepare should succeed");

        assert_eq!(prepared.model_id(), "test-model");
        let options = prepared.options();
        assert_eq!(options.temperature, Some(0.2));
        assert_eq!(options.top_p, Some(0.9));
        assert_eq!(options.max_output_tokens, Some(256));
        assert_eq!(options.seed, Some(7));
        assert_eq!(options.stop_sequences, Some(vec!["STOP".to_string()]));
        assert_eq!(options.reasoning, Some(ReasoningEffort::High));
        assert_eq!(options.max_retries, Some(0));
        assert_eq!(
            options.tool_choice,
            Some(ToolChoice::Tool {
                tool_name: "tool_a".to_string()
            })
        );

        let tools = options.tools.as_ref().expect("tools translated");
        match &tools[0] {
            Tool::Function(function) => {
                assert_eq!(function.name, "tool_a");
                assert_eq!(function.strict, Some(false));
                assert_eq!(function.description.as_deref(), Some("a tool"));
                assert_eq!(function.input_schema["type"], json!("object"));
            }
            other => panic!("expected function tool, got {other:?}"),
        }

        let overrides = options.body_overrides.as_ref().expect("body overrides");
        assert_eq!(overrides["stream"], json!(true));
        assert_eq!(
            overrides["stream_options"],
            json!({ "include_usage": true })
        );
        assert_eq!(overrides["store"], json!(false));
        assert_eq!(overrides["parallel_tool_calls"], json!(false));
        // `reasoning_effort` is carried by the typed `GenerateTextOptions::reasoning`
        // and mapped by aimux; it must not be duplicated as a body override.
        assert!(overrides.get("reasoning_effort").is_none());
        assert!(overrides["max_tokens"].is_null());
        assert_eq!(overrides["max_completion_tokens"], json!(256));

        let ModelPrompt::Messages(messages) = prepared.prompt() else {
            panic!("expected messages prompt");
        };
        assert_eq!(messages.len(), 4);
        assert_eq!(messages[0].role, Role::System);
        assert_eq!(messages[1].role, Role::User);
        assert_eq!(messages[2].role, Role::Assistant);
        assert_eq!(messages[3].role, Role::Tool);
        let MessageContent::Parts(assistant_parts) = &messages[2].content else {
            panic!("assistant content should be multi-part");
        };
        assert!(matches!(
            assistant_parts.as_slice(),
            [ContentPart::ToolCall { tool_name, .. }] if tool_name == "tool_a"
        ));
        let MessageContent::Parts(tool_parts) = &messages[3].content else {
            panic!("tool content should be multi-part");
        };
        assert!(matches!(
            tool_parts.as_slice(),
            [ContentPart::ToolResult { tool_call_id, .. }] if tool_call_id == "call_a"
        ));
    }

    #[test]
    fn defaults_stream_usage_and_store_pre_dispatch() {
        let body = json!({
            "model": "m",
            "messages": [{ "role": "user", "content": "hi" }]
        });
        let prepared = transport().prepare(request(body)).unwrap();
        let overrides = prepared.options().body_overrides.as_ref().unwrap();
        assert_eq!(
            overrides["stream_options"],
            json!({ "include_usage": true })
        );
        assert_eq!(overrides["store"], json!(false));
        assert_eq!(overrides["stream"], json!(true));
    }

    #[test]
    fn rejects_unknown_top_level_field_pre_dispatch() {
        let body = json!({
            "model": "m",
            "messages": [{ "role": "user", "content": "hi" }],
            "functions": []
        });
        assert!(matches!(
            ChatRequest::from_json(body),
            Err(TransportError::InvalidRequest { .. })
        ));
    }

    #[test]
    fn rejects_unsupported_content_parts() {
        let body = json!({
            "model": "m",
            "messages": [{
                "role": "user",
                "content": [{ "type": "image_url", "image_url": { "url": "http://x" } }]
            }]
        });
        assert!(matches!(
            ChatRequest::from_json(body),
            Err(TransportError::InvalidRequest { .. })
        ));
    }

    #[test]
    fn rejects_non_streaming_and_usage_opt_out() {
        let non_streaming = json!({
            "model": "m",
            "stream": false,
            "messages": [{ "role": "user", "content": "hi" }]
        });
        let error = transport().prepare(request(non_streaming)).unwrap_err();
        assert!(matches!(error, TransportError::Unsupported { .. }));

        let no_usage = json!({
            "model": "m",
            "stream_options": { "include_usage": false },
            "messages": [{ "role": "user", "content": "hi" }]
        });
        let error = transport().prepare(request(no_usage)).unwrap_err();
        assert!(matches!(error, TransportError::Unsupported { .. }));
    }

    #[test]
    fn rejects_invalid_options() {
        let both_max = json!({
            "model": "m",
            "max_tokens": 1,
            "max_completion_tokens": 2,
            "messages": [{ "role": "user", "content": "hi" }]
        });
        assert!(matches!(
            transport().prepare(request(both_max)).unwrap_err(),
            TransportError::InvalidRequest { .. }
        ));

        let multi_n = json!({
            "model": "m",
            "n": 2,
            "messages": [{ "role": "user", "content": "hi" }]
        });
        assert!(matches!(
            transport().prepare(request(multi_n)).unwrap_err(),
            TransportError::Unsupported { .. }
        ));

        let bad_effort = json!({
            "model": "m",
            "reasoning_effort": "ultra",
            "messages": [{ "role": "user", "content": "hi" }]
        });
        assert!(matches!(
            transport().prepare(request(bad_effort)).unwrap_err(),
            TransportError::InvalidRequest { .. }
        ));
    }

    #[test]
    fn stop_accepts_a_string_or_an_array() {
        let one = json!({
            "model": "m",
            "stop": "END",
            "messages": [{ "role": "user", "content": "hi" }]
        });
        assert_eq!(
            transport()
                .prepare(request(one))
                .unwrap()
                .options()
                .stop_sequences,
            Some(vec!["END".to_string()])
        );

        let many = json!({
            "model": "m",
            "stop": ["A", "B"],
            "messages": [{ "role": "user", "content": "hi" }]
        });
        assert_eq!(
            transport()
                .prepare(request(many))
                .unwrap()
                .options()
                .stop_sequences,
            Some(vec!["A".to_string(), "B".to_string()])
        );
    }

    #[test]
    fn preserves_json_schema_strict_false_override() {
        let body = json!({
            "model": "m",
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "name": "answer",
                    "schema": { "type": "object" },
                    "strict": false
                }
            },
            "messages": [{ "role": "user", "content": "hi" }]
        });
        let prepared = transport().prepare(request(body)).unwrap();
        let overrides = prepared.options().body_overrides.as_ref().unwrap();
        assert_eq!(
            overrides["response_format"],
            json!({
                "type": "json_schema",
                "json_schema": { "name": "answer", "schema": { "type": "object" }, "strict": false }
            })
        );
        assert!(matches!(
            prepared.options().response_format,
            Some(ResponseFormat::Json { .. })
        ));
    }

    #[test]
    fn same_frame_multiple_tool_calls_keep_stable_indices() {
        let events = translate(vec![
            start("call_a", "tool_a"),
            start("call_b", "tool_b"),
            delta("call_a", "{\"x\":"),
            delta("call_b", "{\"y\":"),
            delta("call_a", "1}"),
            delta("call_b", "2}"),
        ]);

        assert_eq!(
            events,
            vec![
                ChatStreamEvent::ToolCallStart {
                    index: 0,
                    id: "call_a".into(),
                    name: "tool_a".into()
                },
                ChatStreamEvent::ToolCallStart {
                    index: 1,
                    id: "call_b".into(),
                    name: "tool_b".into()
                },
                ChatStreamEvent::ToolCallArgumentsDelta {
                    index: 0,
                    id: "call_a".into(),
                    delta: "{\"x\":".into()
                },
                ChatStreamEvent::ToolCallArgumentsDelta {
                    index: 1,
                    id: "call_b".into(),
                    delta: "{\"y\":".into()
                },
                ChatStreamEvent::ToolCallArgumentsDelta {
                    index: 0,
                    id: "call_a".into(),
                    delta: "1}".into()
                },
                ChatStreamEvent::ToolCallArgumentsDelta {
                    index: 1,
                    id: "call_b".into(),
                    delta: "2}".into()
                },
            ]
        );
    }

    #[test]
    fn interleaved_fragments_across_frames_reconstruct_calls() {
        let events = translate(vec![
            start("call_a", "tool_a"),
            delta("call_a", "{\"x\":"),
            start("call_b", "tool_b"),
            delta("call_b", "{\"y\":"),
            delta("call_a", "1}"),
            delta("call_b", "2}"),
            StreamPart::ToolCall {
                tool_call_id: "call_a".into(),
                tool_name: "tool_a".into(),
                input: json!({ "x": 1 }),
                provider_executed: None,
                dynamic: None,
                thought_signature: None,
                provider_metadata: None,
            },
            StreamPart::ToolCall {
                tool_call_id: "call_b".into(),
                tool_name: "tool_b".into(),
                input: json!({ "y": 2 }),
                provider_executed: None,
                dynamic: None,
                thought_signature: None,
                provider_metadata: None,
            },
        ]);

        let mut summary = ChatStreamSummary::default();
        for event in &events {
            summary.apply(event);
        }

        assert_eq!(
            summary.tool_calls,
            vec![
                CompletedToolCall {
                    index: 0,
                    id: "call_a".into(),
                    name: "tool_a".into(),
                    arguments: "{\"x\":1}".into(),
                },
                CompletedToolCall {
                    index: 1,
                    id: "call_b".into(),
                    name: "tool_b".into(),
                    arguments: "{\"y\":2}".into(),
                },
            ]
        );
    }

    #[test]
    fn finish_and_usage_are_normalized() {
        let usage = Usage {
            input_tokens: TokenUsage {
                total: Some(100),
                cache_read: Some(20),
                cache_write: Some(5),
                ..TokenUsage::default()
            },
            output_tokens: TokenUsage {
                total: Some(40),
                reasoning: Some(10),
                ..TokenUsage::default()
            },
            raw: Some(json!({ "prompt_tokens": 100 })),
        };
        let part = StreamPart::Finish {
            finish_reason: FinishReason {
                unified: FinishReasonUnified::ToolCalls,
                raw: Some("tool_calls".into()),
            },
            usage,
            provider_metadata: None,
        };

        let events = translate(vec![part]);
        let ChatStreamEvent::Finish {
            reason,
            raw_reason,
            usage,
        } = &events[0]
        else {
            panic!("expected finish event");
        };
        assert_eq!(*reason, ChatFinishReason::ToolCalls);
        assert_eq!(reason.as_openai_str(), "tool_calls");
        assert_eq!(raw_reason.as_deref(), Some("tool_calls"));
        assert_eq!(usage.input_tokens, Some(100));
        assert_eq!(usage.output_tokens, Some(40));
        assert_eq!(usage.cache_read_tokens, Some(20));
        assert_eq!(usage.cache_write_tokens, Some(5));
        assert_eq!(usage.reasoning_tokens, Some(10));
        assert_eq!(usage.total_tokens(), Some(140));
        assert_eq!(usage.raw, Some(json!({ "prompt_tokens": 100 })));
    }

    #[test]
    fn aimux_errors_keep_classification() {
        let error = AiMuxError::ApiCall(ApiCallError {
            status_code: Some(429),
            provider_code: Some("rate_limit_exceeded".into()),
            message: "slow down".into(),
            retry_after_ms: Some(250),
            is_retryable: true,
            ..ApiCallError::default()
        });
        let transport_error: TransportError = error.into();
        assert!(transport_error.is_retryable());
        assert_eq!(transport_error.status_code(), Some(429));
        assert_eq!(transport_error.provider_code(), Some("rate_limit_exceeded"));
    }

    #[test]
    fn missing_reasoning_effort_is_provider_default() {
        let body = json!({
            "model": "m",
            "messages": [{ "role": "user", "content": "hi" }]
        });
        assert_eq!(
            transport()
                .prepare(request(body))
                .unwrap()
                .options()
                .reasoning,
            None
        );
    }
}
