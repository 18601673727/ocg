//! OCG-owned OpenAI-compatible provider loop.
//!
//! Provider wire handling ends at normalized assistant text/tool calls. Native
//! Tool authority stays in `native_tools`, and every tool invocation is
//! admitted and completed as a canonical Call before its tool message is sent
//! back to the provider.

use crate::call_recovery as recovery;
use crate::error::{OcgError, Result};
use crate::http::{BoxFuture, HttpTransport};
use crate::native_tools::{
    openai_projection::OpenAiToolProjection, NativeToolRegistry, PermissionPolicy,
};
use crate::openai_compatible::{
    ChatFinishReason, ChatStreamEvent, ChatStreamSummary, CompletedToolCall, NormalizedUsage,
};
use crate::orchestration::budget::{BudgetConfig, QuotaFacts};
use crate::orchestration::domain::{
    AccountingAuthority, AttemptAuthority, DispatchAccounting, DomainRepository, EffectIntentKind,
    EffectIntentState,
};
use crate::orchestration::execution_dispatch::{
    admit_call, BoundedDispatcher, CallCancellation, ExecutionEnvelope, ExecutionEvent,
};
use crate::provider_protocol::ProviderProtocol;
use serde_json::{json, Value};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

#[cfg(test)]
mod tests;

pub const MAX_PROVIDER_ROUNDS: usize = 32;

#[derive(Debug, Clone, PartialEq)]
pub struct ProviderRound {
    pub assistant: Value,
    pub summary: ChatStreamSummary,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProviderFinalResponse {
    pub content: String,
    pub reasoning: String,
    pub rounds: usize,
}

/// One protocol implementation of a provider round.
///
/// The provider loop is protocol-agnostic above this trait: it hands a canonical
/// provider request in and receives canonical assistant text, reasoning, tool
/// calls and usage out. Each protocol owns only its own wire encoding, so a new
/// protocol is a new implementation here rather than a new execution path.
pub trait ProviderClient: Send + Sync {
    /// Run one provider round.
    ///
    /// A round is awaited on the ntex runtime the provider worker already owns,
    /// so the transport performs its I/O on that ambient execution context
    /// rather than blocking the worker thread on a runtime of its own.
    fn complete(&self, request: &Value) -> BoxFuture<'_, Result<ProviderRound>>;
}

/// The decoding state of one streamed round, whatever protocol produced it.
///
/// This is what lets chunk delivery, cancellation and terminalization stay in
/// one place: a protocol contributes only how bytes become canonical events.
trait ProviderRoundState: Send {
    /// Fold one raw body chunk. Returns `Ok(false)` when the stream is finished
    /// and reading should stop, plus every canonical event it produced.
    fn consume(&mut self, chunk: &[u8]) -> Result<(bool, Vec<ChatStreamEvent>)>;

    /// Close the round and produce its canonical result.
    fn finish(&mut self) -> Result<ProviderRound>;
}

/// Shared, cancellation-aware state for one streamed provider round. The
/// `on_chunk` callback runs inside the HTTP transport's async read, so the
/// state lives behind an [`Arc`]/[`Mutex`] the callback can own.
struct StreamedRoundState {
    /// Undecoded bytes carried across chunks for UTF-8 / SSE framing.
    accumulator: SseAccumulator,
    /// Whole-body JSON buffer for a non-streamed completion response.
    json_buffer: Vec<u8>,
    mode: StreamMode,
    summary: ChatStreamSummary,
    done: bool,
}

impl StreamedRoundState {
    fn new() -> Self {
        Self {
            accumulator: SseAccumulator::new(),
            json_buffer: Vec::new(),
            mode: StreamMode::Undetermined,
            summary: ChatStreamSummary::default(),
            done: false,
        }
    }

    /// Feed one raw chunk. Returns `Ok(false)` when the stream is finished
    /// (`[DONE]` or a complete JSON body) and reading should stop.
    fn consume(&mut self, chunk: &[u8]) -> Result<(bool, Vec<ChatStreamEvent>)> {
        if self.mode == StreamMode::Undetermined {
            self.mode = detect_stream_mode(chunk);
        }
        match self.mode {
            StreamMode::Sse => {
                let mut emitted = Vec::new();
                for event in self.accumulator.consume(chunk)? {
                    emitted.extend(apply_chunk_json(&mut self.summary, &event)?);
                }
                self.done = self.accumulator.finished();
                Ok((!self.done, emitted))
            }
            StreamMode::CompletionJson => {
                self.json_buffer.extend_from_slice(chunk);
                Ok((true, Vec::new()))
            }
            StreamMode::Undetermined => Ok((true, Vec::new())),
        }
    }

    /// Called once the transport finished delivering chunks.
    fn finish(&mut self) -> Result<ProviderRound> {
        if self.mode == StreamMode::Sse {
            for event in self.accumulator.finish()? {
                let _ = apply_chunk_json(&mut self.summary, &event)?;
            }
        } else {
            let value: Value = serde_json::from_slice(&self.json_buffer).map_err(|error| {
                OcgError::config(format!("invalid provider completion JSON: {error}"))
            })?;
            apply_completion_json(&mut self.summary, &value)?;
        }
        Ok(ProviderRound {
            assistant: build_assistant_message(&self.summary),
            summary: self.summary.clone(),
        })
    }
}

impl ProviderRoundState for StreamedRoundState {
    fn consume(&mut self, chunk: &[u8]) -> Result<(bool, Vec<ChatStreamEvent>)> {
        StreamedRoundState::consume(self, chunk)
    }

    fn finish(&mut self) -> Result<ProviderRound> {
        StreamedRoundState::finish(self)
    }
}

/// Which wire shape the response uses. Decided lazily from the first chunk so
/// both `stream: true` (SSE) and `stream: false` (one JSON completion) decode
/// through the same incremental path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StreamMode {
    Undetermined,
    Sse,
    CompletionJson,
}

pub(crate) fn detect_stream_mode(first_chunk: &[u8]) -> StreamMode {
    let trimmed = first_chunk
        .iter()
        .copied()
        .skip_while(|byte| byte.is_ascii_whitespace())
        .collect::<Vec<_>>();
    match trimmed.first() {
        Some(b'{') | Some(b'[') => StreamMode::CompletionJson,
        _ => StreamMode::Sse,
    }
}

/// Incremental SSE frame decoder over raw response chunks. It preserves the
/// undecoded byte remainder across chunks (a multi-byte UTF-8 codepoint can
/// span chunks), buffers partial lines, and only emits an event once its
/// terminating blank line has arrived, so events, JSON objects, and
/// multi-`data:` payloads are never delivered partially.
pub(crate) struct SseAccumulator {
    /// Bytes not yet decodable as UTF-8 (partial codepoint at chunk boundary).
    raw: Vec<u8>,
    /// Decoded text not yet terminated by a newline.
    line: String,
    /// `data:` payloads of the event currently being assembled.
    data: Vec<String>,
    /// `[DONE]` was received.
    done: bool,
}

impl SseAccumulator {
    pub(crate) fn new() -> Self {
        Self {
            raw: Vec::new(),
            line: String::new(),
            data: Vec::new(),
            done: false,
        }
    }

    fn finished(&self) -> bool {
        self.done
    }

    /// Feed one chunk and return every SSE event completed by it.
    pub(crate) fn consume(&mut self, chunk: &[u8]) -> Result<Vec<Value>> {
        self.raw.extend_from_slice(chunk);
        let (decoded, consumed) = decode_utf8_prefix(&self.raw)?;
        self.raw.drain(..consumed);
        self.line.push_str(&decoded);
        self.drain_complete_lines(false)
    }

    /// Flush any event left without a terminating blank line at end of stream.
    pub(crate) fn finish(&mut self) -> Result<Vec<Value>> {
        self.drain_complete_lines(true)
    }

    fn drain_complete_lines(&mut self, flush: bool) -> Result<Vec<Value>> {
        let mut events = Vec::new();
        while let Some(position) = self.line.find('\n') {
            let text = self.line[..position].trim_end_matches('\r').to_string();
            self.line.drain(..=position);
            self.handle_line(&text, &mut events)?;
        }
        if flush {
            // No trailing newline: treat any residue as a final line, then
            // emit any pending event that never got its blank line.
            let residue = std::mem::take(&mut self.line);
            if !residue.is_empty() {
                let text = residue.trim_end_matches('\r').to_string();
                self.handle_line(&text, &mut events)?;
            }
            self.end_event(&mut events)?;
        }
        Ok(events)
    }

    fn handle_line(&mut self, line: &str, events: &mut Vec<Value>) -> Result<()> {
        if line.is_empty() {
            // A blank line terminates the current event.
            return self.end_event(events);
        }
        if let Some(data) = line.strip_prefix("data:") {
            self.data
                .push(data.strip_prefix(' ').unwrap_or(data).to_string());
        }
        // Comment lines (`:`) and `event:`/`id:`/`retry:` fields are ignored.
        Ok(())
    }

    fn end_event(&mut self, events: &mut Vec<Value>) -> Result<()> {
        if self.data.is_empty() {
            return Ok(());
        }
        let payload = self.data.join("\n");
        self.data.clear();
        if payload.trim() == "[DONE]" {
            self.done = true;
            return Ok(());
        }
        let value: Value = serde_json::from_str(&payload)
            .map_err(|error| OcgError::config(format!("invalid provider SSE JSON: {error}")))?;
        events.push(value);
        Ok(())
    }
}

/// Decode the longest valid UTF-8 prefix of `raw`, returning the decoded text
/// and the number of input bytes consumed. Incomplete trailing codepoints stay
/// in `raw` for the next chunk; invalid bytes are dropped so the remainder can
/// never grow without bound.
fn decode_utf8_prefix(raw: &[u8]) -> Result<(String, usize)> {
    match std::str::from_utf8(raw) {
        Ok(text) => Ok((text.to_string(), raw.len())),
        Err(error) => {
            let valid = error.valid_up_to();
            let skip = error.error_len().unwrap_or(0);
            let decoded = String::from_utf8_lossy(&raw[..valid]).into_owned();
            Ok((decoded, valid + skip))
        }
    }
}

fn parse_finish_reason(raw: &str) -> ChatFinishReason {
    match raw {
        "stop" => ChatFinishReason::Stop,
        "length" => ChatFinishReason::Length,
        "tool_calls" => ChatFinishReason::ToolCalls,
        "content_filter" => ChatFinishReason::ContentFilter,
        "error" => ChatFinishReason::Error,
        _ => ChatFinishReason::Other,
    }
}

fn parse_usage(value: &Value) -> NormalizedUsage {
    let mut usage = NormalizedUsage {
        raw: Some(value.clone()),
        ..NormalizedUsage::default()
    };
    usage.input_tokens = value
        .get("prompt_tokens")
        .and_then(Value::as_u64)
        .map(|v| v as u32);
    usage.output_tokens = value
        .get("completion_tokens")
        .and_then(Value::as_u64)
        .map(|v| v as u32);
    usage
}

/// Fold one streamed chat-completion chunk into the summary. Tool-call
/// argument deltas accumulate by the OpenAI `index` even when a fragment
/// carries no `id` or `name`, matching [`ChatStreamSummary::apply`].
fn apply_chunk_json(
    summary: &mut ChatStreamSummary,
    value: &Value,
) -> Result<Vec<ChatStreamEvent>> {
    let mut emitted = Vec::new();
    if let Some(choices) = value.get("choices").and_then(Value::as_array) {
        if let Some(first) = choices.first() {
            if let Some(delta) = first.get("delta") {
                if let Some(content) = delta.get("content").and_then(Value::as_str) {
                    if !content.is_empty() {
                        let event = ChatStreamEvent::TextDelta {
                            delta: content.to_string(),
                        };
                        summary.apply(&event);
                        emitted.push(event);
                    }
                }
                if let Some(reasoning) = delta.get("reasoning_content").and_then(Value::as_str) {
                    if !reasoning.is_empty() {
                        let event = ChatStreamEvent::ReasoningDelta {
                            delta: reasoning.to_string(),
                        };
                        summary.apply(&event);
                        emitted.push(event);
                    }
                }
                if let Some(tool_calls) = delta.get("tool_calls").and_then(Value::as_array) {
                    for call in tool_calls {
                        let index = call.get("index").and_then(Value::as_u64).unwrap_or(0) as u32;
                        let id = call.get("id").and_then(Value::as_str).unwrap_or("");
                        if let Some(function) = call.get("function") {
                            if let (Some(id), Some(name)) = (
                                call.get("id").and_then(Value::as_str),
                                function.get("name").and_then(Value::as_str),
                            ) {
                                let event = ChatStreamEvent::ToolCallStart {
                                    index,
                                    id: id.to_string(),
                                    name: name.to_string(),
                                };
                                summary.apply(&event);
                                emitted.push(event);
                            }
                            if let Some(arguments) =
                                function.get("arguments").and_then(Value::as_str)
                            {
                                if !arguments.is_empty() {
                                    let event = ChatStreamEvent::ToolCallArgumentsDelta {
                                        index,
                                        id: id.to_string(),
                                        delta: arguments.to_string(),
                                    };
                                    summary.apply(&event);
                                    emitted.push(event);
                                }
                            }
                        }
                    }
                }
            }
            if let Some(finish_reason) = first.get("finish_reason").and_then(Value::as_str) {
                summary.finish_reason = Some(parse_finish_reason(finish_reason));
                summary.raw_finish_reason = Some(finish_reason.to_string());
            }
        }
    }
    if let Some(usage) = value.get("usage") {
        summary.usage = parse_usage(usage);
    }
    Ok(emitted)
}

/// Fold one complete (non-streamed) chat-completion response into the summary.
fn apply_completion_json(summary: &mut ChatStreamSummary, value: &Value) -> Result<()> {
    if let Some(choices) = value.get("choices").and_then(Value::as_array) {
        if let Some(first) = choices.first() {
            if let Some(message) = first.get("message") {
                if let Some(content) = message.get("content").and_then(Value::as_str) {
                    summary.text.push_str(content);
                }
                if let Some(reasoning) = message.get("reasoning_content").and_then(Value::as_str) {
                    summary.reasoning.push_str(reasoning);
                }
                if let Some(tool_calls) = message.get("tool_calls").and_then(Value::as_array) {
                    for (position, call) in tool_calls.iter().enumerate() {
                        let index = call
                            .get("index")
                            .and_then(Value::as_u64)
                            .map(|v| v as u32)
                            .unwrap_or(position as u32);
                        if let (Some(id), Some(function)) =
                            (call.get("id").and_then(Value::as_str), call.get("function"))
                        {
                            if let (Some(name), Some(arguments)) = (
                                function.get("name").and_then(Value::as_str),
                                function.get("arguments").and_then(Value::as_str),
                            ) {
                                summary.apply(&ChatStreamEvent::ToolCallComplete {
                                    index,
                                    id: id.to_string(),
                                    name: name.to_string(),
                                    arguments: arguments.to_string(),
                                });
                            }
                        }
                    }
                }
            }
            if let Some(finish_reason) = first.get("finish_reason").and_then(Value::as_str) {
                summary.finish_reason = Some(parse_finish_reason(finish_reason));
                summary.raw_finish_reason = Some(finish_reason.to_string());
            }
        }
    }
    if let Some(usage) = value.get("usage") {
        summary.usage = parse_usage(usage);
    }
    Ok(())
}

/// Build the assistant message that goes back on the wire for the next round.
fn build_assistant_message(summary: &ChatStreamSummary) -> Value {
    let tool_calls: Vec<Value> = summary
        .tool_calls
        .iter()
        .map(|call| {
            json!({"id": call.id, "type": "function", "function": {"name": call.name, "arguments": call.arguments}})
        })
        .collect();
    let mut message = if tool_calls.is_empty() {
        json!({"role": "assistant", "content": summary.text})
    } else {
        json!({"role": "assistant", "content": summary.text, "tool_calls": tool_calls})
    };
    if !summary.reasoning.is_empty() {
        message["reasoning_content"] = json!(summary.reasoning);
    }
    message
}

/// A provider client using the repository's native ntex HTTP surface.
/// It decodes OpenAI-compatible response JSON/SSE incrementally through the
/// existing `ChatStreamEvent`/`ChatStreamSummary` path and uses the existing
/// `ToolCallNormalizer` semantics (tool-call argument deltas accumulate by
/// `index`).
pub struct NativeOpenAiCompatibleProvider<'a> {
    transport: &'a dyn HttpTransport,
    endpoint: String,
    bearer: Option<String>,
    upstream_model_id: String,
    cancelled: CallCancellation,
    events: Option<flume::Sender<crate::orchestration::execution_dispatch::ExecutionEvent>>,
}

impl<'a> NativeOpenAiCompatibleProvider<'a> {
    pub fn new(
        transport: &'a dyn HttpTransport,
        endpoint: impl Into<String>,
        bearer: Option<String>,
        upstream_model_id: impl Into<String>,
        cancelled: CallCancellation,
        events: Option<flume::Sender<crate::orchestration::execution_dispatch::ExecutionEvent>>,
    ) -> Self {
        Self {
            transport,
            endpoint: endpoint.into(),
            bearer,
            upstream_model_id: upstream_model_id.into(),
            cancelled,
            events,
        }
    }
}

/// Configuration for the canonical provider handler. The handler owns the
/// provider transport and project boundary, while each envelope supplies the
/// Attempt/Call identity and frozen provider configuration that must be
/// revalidated before execution.
pub struct ProviderHandlerConfig {
    pub transport: Arc<dyn HttpTransport>,
    pub project_root: std::path::PathBuf,
    pub permission_policy: PermissionPolicy,
    pub cancelled: Arc<AtomicBool>,
    pub native_tool_dispatcher: BoundedDispatcher,
}

pub struct CanonicalProviderCallHandler {
    config: Arc<ProviderHandlerConfig>,
}

impl Clone for CanonicalProviderCallHandler {
    fn clone(&self) -> Self {
        Self {
            config: Arc::clone(&self.config),
        }
    }
}

impl CanonicalProviderCallHandler {
    pub fn new(config: ProviderHandlerConfig) -> Self {
        Self {
            config: Arc::new(config),
        }
    }

    async fn execute_validated(&self, envelope: ExecutionEnvelope) -> Result<Value> {
        let config = Arc::clone(&self.config);
        if config.cancelled.load(Ordering::SeqCst) || envelope.cancelled.is_cancelled() {
            fail_authoritative_provider_call(
                &config.project_root,
                &envelope,
                "cancelled before provider execution",
                false,
            );
            return Err(OcgError::config("provider Call cancelled before execution"));
        }
        let mut domain = DomainRepository::open(&config.project_root)?;

        let authority = domain
            .authority(&envelope.attempt_id)?
            .filter(|authority| {
                authority.job_id == envelope.job_id && authority.generation == envelope.generation
            })
            .ok_or_else(|| OcgError::config("provider Call has stale Attempt authority"))?;
        let call = domain.call(&envelope.call_id)?;
        if call.attempt_id != envelope.attempt_id || call.generation != envelope.generation {
            return Err(OcgError::config(
                "provider Call has stale envelope identity",
            ));
        }
        // Redelivery is not a failure of the actor that already claimed this Call.
        if matches!(call.state.as_str(), "running" | "completed") {
            return Err(OcgError::config("provider Call has already been delivered"));
        }
        if call.state != "created" {
            let message = "provider Call is not executable";
            fail_authoritative_provider_call(&config.project_root, &envelope, message, false);
            return Err(OcgError::config(message));
        }
        if call.executor_id != envelope.executor_id || call.request != envelope.payload {
            let message = "provider envelope differs from durable Call";
            fail_authoritative_provider_call(&config.project_root, &envelope, message, false);
            return Err(OcgError::config(message));
        }

        // Verify economic authority before execution
        let intent = domain.dispatch_intent(&envelope.call_id)?.ok_or_else(|| {
            fail_authoritative_provider_call(
                &config.project_root,
                &envelope,
                "provider Call has no durable dispatch intent",
                false,
            );
            OcgError::config("provider Call has no durable dispatch intent")
        })?;
        if !intent.budget_admitted {
            fail_authoritative_provider_call(
                &config.project_root,
                &envelope,
                "provider Call has no economic admission",
                false,
            );
            return Err(OcgError::config(
                "provider Call lacks economic admission authority",
            ));
        }

        let input: Value = match serde_json::from_str(&envelope.payload) {
            Ok(input) => input,
            Err(error) => {
                let message = format!("invalid provider Call payload: {error}");
                fail_authoritative_provider_call(&config.project_root, &envelope, &message, false);
                return Err(OcgError::config(message));
            }
        };
        let mut request = match input.get("arguments").cloned() {
            Some(request) => request,
            None => {
                let message = "provider Call payload missing 'arguments'";
                fail_authoritative_provider_call(&config.project_root, &envelope, message, false);
                return Err(OcgError::config(message));
            }
        };
        // The protocol was frozen into this payload when the Call was admitted,
        // so recovery and re-execution replay it rather than re-reading the
        // Profile: a dispatch never silently switches wire formats.
        let protocol = match input.get("provider_protocol") {
            None => ProviderProtocol::default(),
            Some(value) => match serde_json::from_value::<ProviderProtocol>(value.clone()) {
                Ok(protocol) => protocol,
                Err(error) => {
                    let message = format!("invalid frozen provider protocol: {error}");
                    fail_authoritative_provider_call(
                        &config.project_root,
                        &envelope,
                        &message,
                        false,
                    );
                    return Err(OcgError::config(message));
                }
            },
        };
        let executor = match domain.executor(envelope.executor_id.as_deref().unwrap_or("")) {
            Ok(Some(executor)) => executor,
            resolved => {
                let error = match resolved {
                    Err(error) => error,
                    Ok(None) => OcgError::config("provider Call executor not found"),
                    Ok(Some(_)) => unreachable!("resolved above"),
                };
                fail_authoritative_provider_call(
                    &config.project_root,
                    &envelope,
                    &error.to_string(),
                    false,
                );
                return Err(error);
            }
        };

        // Resolve provider configuration from envelope. This is the frozen
        // durable identity the Call was admitted with; it is revalidated, never
        // re-resolved, so recovery and re-execution use the same provider,
        // model, endpoint, and credential reference.
        let provider_config = envelope.provider_config.as_ref().ok_or_else(|| {
            fail_authoritative_provider_call(
                &config.project_root,
                &envelope,
                "provider Call missing provider_config",
                false,
            );
            OcgError::config("provider Call missing provider_config")
        })?;

        if intent.job_id != envelope.job_id
            || intent.attempt_id != envelope.attempt_id
            || intent.generation != envelope.generation
            || intent.executor_id != envelope.executor_id
            || intent.request != envelope.payload
            || intent.provider_key.as_deref() != Some(provider_config.provider_key.as_str())
            || intent.model.as_deref() != Some(provider_config.model.as_str())
            || intent.upstream_model_id.as_deref()
                != Some(provider_config.upstream_model_id.as_str())
            || intent.endpoint.as_deref() != Some(provider_config.endpoint.as_str())
            || intent.credential_ref != provider_config.credential_ref
        {
            fail_authoritative_provider_call(
                &config.project_root,
                &envelope,
                "provider envelope differs from frozen dispatch intent",
                false,
            );
            return Err(OcgError::config(
                "provider envelope differs from frozen dispatch intent",
            ));
        }
        if !matches!(intent.state.as_str(), "pending" | "queued")
            || intent.effect_state != EffectIntentState::NotStarted
        {
            let message = "provider dispatch intent is not executable";
            fail_authoritative_provider_call(&config.project_root, &envelope, message, false);
            return Err(OcgError::config(message));
        }
        if let Err(error) = crate::orchestration::call_schema::validate_input(&input) {
            fail_authoritative_provider_call(
                &config.project_root,
                &envelope,
                &error.to_string(),
                false,
            );
            return Err(error);
        }

        // The wire model id is the frozen upstream model id, not the Profile
        // model key. It is set here, before any tool injection, so every round
        // uses the frozen identity.
        if let Some(object) = request.as_object_mut() {
            object.insert(
                "model".to_string(),
                Value::String(provider_config.upstream_model_id.clone()),
            );
        }

        // Resolve credential from the user-global Vault at execution time,
        // immediately before the side effect. `None` means no Authorization
        // header; `Some(ref)` missing from the Vault fails closed. The raw
        // token is held only in `bearer` and never persisted or logged.
        let bearer = match (|| -> Result<Option<String>> {
            let vault = crate::vault::Vault::user_global()?;
            match &provider_config.credential_ref {
                Some(credential_ref) => vault.get(credential_ref)?.map(Some).ok_or_else(|| {
                    OcgError::config("provider credential not found in the user-global Vault")
                }),
                None => Ok(None),
            }
        })() {
            Ok(bearer) => bearer,
            Err(error) => {
                fail_authoritative_provider_call(
                    &config.project_root,
                    &envelope,
                    &error.to_string(),
                    false,
                );
                return Err(error);
            }
        };

        if let Err(error) =
            domain.start_call(&envelope.call_id, &envelope.attempt_id, envelope.generation)
        {
            // Re-read authority and Call state, not the ambiguous claim error:
            // replacement, cancellation and an existing claimant are all no-ops.
            fail_authoritative_provider_call(
                &config.project_root,
                &envelope,
                &error.to_string(),
                false,
            );
            return Err(error);
        }
        drop(domain);
        let provider = provider_client(
            ProviderBinding::of(protocol),
            config.transport.as_ref(),
            provider_config,
            bearer,
            envelope.cancelled.clone(),
            Some(envelope.events.clone()),
        );
        let task = input
            .get("context_task")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .unwrap_or_else(|| {
                objective_of(
                    request
                        .get("messages")
                        .and_then(Value::as_array)
                        .map(Vec::as_slice)
                        .unwrap_or_default(),
                )
            });
        let response = match execute_provider_loop(
            provider.as_ref(),
            &config.project_root,
            &envelope,
            &mut request,
            ProviderBinding::of(protocol),
            ProviderAdmission {
                authority: &authority,
                executor_id: &executor.id,
                permission_policy: config.permission_policy,
                shutdown: &config.cancelled,
                native_tool_dispatcher: &config.native_tool_dispatcher,
                task: &task,
            },
        )
        .await
        {
            Ok(response) => response,
            Err(error) => {
                // Cancellation is not a provider failure. The cancellation
                // lifecycle already terminalized this Attempt and its Job, so
                // settling it again here as a failure would overwrite `cancelled`
                // with `failed`. The error is still returned so the worker logs
                // the reason and moves on to the next bounded envelope.
                if envelope.cancelled.is_cancelled() {
                    return Err(error);
                }
                // A failed provider round is a terminal Call failure, never a
                // completion. This worker still holds the Attempt authority it
                // validated above, so the failure settles the whole execution
                // rather than only the Call.
                fail_authoritative_provider_call(
                    &config.project_root,
                    &envelope,
                    &error.to_string(),
                    true,
                );
                return Err(error);
            }
        };
        let mut domain = DomainRepository::open(&config.project_root)?;
        let serialized = serde_json::to_string(&json!({
            "content": response.content,
            "reasoning": response.reasoning,
            "rounds": response.rounds
        }))
        .map_err(|error| OcgError::config(format!("serialize provider response: {error}")))?;
        domain.finish_call(
            &envelope.call_id,
            &envelope.attempt_id,
            envelope.generation,
            &serialized,
        )?;
        // This provider round was this Attempt's last outstanding Call: the loop
        // above returned a final answer, and every native tool Call it admitted
        // was completed before it did. `finish_attempt` is the existing
        // settlement authority for that fact, and it decides on its own terms
        // whether the Attempt may close — it refuses unless every Call of this
        // Attempt reached `completed`, fences anything still outstanding, moves
        // the Attempt's Executors to the same terminal state, revokes the
        // Attempt's authority, and carries the Job to the same terminal state in
        // one transaction. Nothing here re-derives that rule.
        domain.finish_attempt(&authority.attempt_id, true)?;
        let _ = envelope
            .events
            .send(crate::orchestration::execution_dispatch::ExecutionEvent::Finished);
        Ok(json!({
            "result": {
                "content": response.content,
                "reasoning": response.reasoning,
                "rounds": response.rounds
            }
        }))
    }
}

/// Admit one provider request through canonical economic admission before
/// placing it on the bounded dispatcher. The returned Call is the durable owner
/// of the provider request; a queue item alone never authorizes execution.
///
/// Economic admission must succeed before the Call enters the bounded queue.
/// If queue handoff fails after successful admission, the reservation is
/// released as NotDispatched.
pub fn admit_provider_call(
    admission: ProviderCallAdmission<'_>,
) -> Result<crate::orchestration::domain::Call> {
    admit_provider_call_with_events(admission, None, CallCancellation::new()).map(|(call, _)| call)
}

/// Everything a provider Call is admitted with.
///
/// One value rather than a long argument list, so the frozen protocol, the
/// frozen provider configuration and the queue the Call is handed to are
/// established together and cannot be mixed from two different admissions.
pub struct ProviderCallAdmission<'a> {
    pub domain: &'a mut DomainRepository,
    pub authority: &'a AttemptAuthority,
    pub executor_id: &'a str,
    pub request: Value,
    pub config: &'a BudgetConfig,
    pub quota: QuotaFacts,
    pub dispatcher: &'a BoundedDispatcher,
    pub provider_config: crate::orchestration::execution_dispatch::ProviderExecutionConfig,
    pub protocol: ProviderProtocol,
}

/// Admit a provider Call and retain its live event receiver/cancellation token
/// for a product chat stream. The durable Call remains the sole execution
/// authority; these handles are only transport observability.
pub fn admit_provider_call_with_events(
    admission: ProviderCallAdmission<'_>,
    event_sender: Option<flume::Sender<ExecutionEvent>>,
    cancelled: CallCancellation,
) -> Result<(crate::orchestration::domain::Call, CallCancellation)> {
    let ProviderCallAdmission {
        domain,
        authority,
        executor_id,
        request,
        config,
        quota,
        dispatcher,
        provider_config,
        protocol,
    } = admission;
    let job = domain
        .job(&authority.job_id)?
        .ok_or_else(|| OcgError::config("provider Call Job no longer exists"))?;
    let context_task = serde_json::from_str::<Value>(&job.spec)
        .ok()
        .and_then(|spec| {
            spec.get("objective")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_else(|| {
            objective_of(
                request
                    .get("messages")
                    .and_then(Value::as_array)
                    .map(Vec::as_slice)
                    .unwrap_or_default(),
            )
        });
    let payload = json!({
        "executor_transport": "provider",
        // The protocol travels in the durable Call payload. It is a property of
        // the dispatch, not of the conversation, and the payload is already the
        // frozen record this Call is revalidated against — so a recovered
        // provider Call replays the protocol it was admitted under instead of
        // re-deriving one from configuration that may since have changed.
        "provider_protocol": protocol.as_str(),
        "context_task": context_task,
        "arguments": request
    });

    // Create Call and DispatchIntent without queueing
    let call = domain.create_call_with_effect(
        &authority.attempt_id,
        Some(executor_id),
        authority.generation,
        EffectIntentKind::StrictFenced,
        &payload.to_string(),
    )?;

    // Freeze provider execution configuration for this Call
    domain.set_provider_config(
        &call.id,
        &provider_config.provider_key,
        &provider_config.model,
        &provider_config.upstream_model_id,
        &provider_config.endpoint,
        provider_config.credential_ref.as_deref(),
    )?;

    // Resolve canonical Project identity from Job ownership
    let project_id = &job.project_id;

    // Execute true economic admission through canonical DomainRepository API
    let assessment =
        domain.admit_dispatch(project_id, authority.generation, &call.id, config, quota)?;

    // Fail closed: denied admission terminates the Call before queue/execution
    if !assessment.is_allowed() {
        let failure = format!("economic_admission_denied: {}", assessment.reason_code);
        domain.fail_call(
            &call.id,
            &authority.attempt_id,
            authority.generation,
            &failure,
        )?;
        return Err(OcgError::config(format!(
            "provider Call denied by economic admission: {}",
            assessment.reason
        )));
    }

    // Economic admission succeeded; now queue
    domain.mark_dispatch_queued(&call.id)?;
    domain.accept_chat_turn(&authority.attempt_id)?;
    let (default_events, _receiver) = flume::unbounded();
    if let Err(error) = dispatcher.send(ExecutionEnvelope {
        call_id: call.id.clone(),
        job_id: authority.job_id.clone(),
        attempt_id: authority.attempt_id.clone(),
        executor_id: Some(executor_id.to_string()),
        generation: authority.generation,
        payload: payload.to_string(),
        dispatch_id: None,
        events: event_sender.unwrap_or(default_events),
        provider_config: Some(provider_config),
        cancelled: cancelled.clone(),
    }) {
        // Queue handoff failed after successful economic admission.
        // The provider request never left OCG, so release the reservation
        // via NotDispatched disposition.
        domain.fail_call(
            &call.id,
            &authority.attempt_id,
            authority.generation,
            "bounded_dispatch_disconnected",
        )?;
        let claim = AccountingAuthority {
            attempt_id: authority.attempt_id.clone(),
            generation: authority.generation,
        };
        let _ =
            domain.settle_dispatch_accounting(&call.id, &claim, &DispatchAccounting::NotDispatched);
        return Err(error);
    }

    Ok((call, cancelled))
}

/// Requeue one recovered provider DispatchIntent back to the bounded
/// dispatcher. The Call and DispatchIntent already exist; this only
/// redelivers the execution envelope.
pub fn requeue_recovered_provider_call(
    domain: &mut DomainRepository,
    intent: &crate::orchestration::domain::DispatchIntent,
    dispatcher: &BoundedDispatcher,
) -> Result<()> {
    // Revalidate current Attempt authority
    let authority = domain
        .authority(&intent.attempt_id)?
        .filter(|authority| {
            authority.job_id == intent.job_id && authority.generation == intent.generation
        })
        .ok_or_else(|| OcgError::config("recovered provider Call has stale Attempt authority"))?;

    // Requeue using existing Call identity. The frozen provider configuration
    // is reused exactly as admitted; the credential reference is optional and
    // re-read from the user-global Vault at execution time.
    let provider_config = if let (Some(pk), Some(m), Some(ep), Some(umi)) = (
        intent.provider_key.as_ref(),
        intent.model.as_ref(),
        intent.endpoint.as_ref(),
        intent.upstream_model_id.as_ref(),
    ) {
        Some(
            crate::orchestration::execution_dispatch::ProviderExecutionConfig {
                provider_key: pk.clone(),
                model: m.clone(),
                upstream_model_id: umi.clone(),
                endpoint: ep.clone(),
                credential_ref: intent.credential_ref.clone(),
            },
        )
    } else {
        None
    };

    // The protocol is not re-resolved here: it is frozen in `intent.request` and
    // read back by the handler from that same payload, so a recovered dispatch
    // replays the protocol it was admitted under rather than one the Profile
    // happens to declare now.
    domain.accept_chat_turn(&authority.attempt_id)?;
    let (events, _receiver) = flume::unbounded();
    dispatcher.send(ExecutionEnvelope {
        call_id: intent.call_id.clone(),
        job_id: intent.job_id.clone(),
        attempt_id: intent.attempt_id.clone(),
        executor_id: intent.executor_id.clone(),
        generation: authority.generation,
        payload: intent.request.clone(),
        dispatch_id: None,
        events,
        provider_config,
        cancelled: CallCancellation::new(),
    })?;

    Ok(())
}

/// Run provider envelopes through the bounded execution worker. This
/// is the production handoff from canonical admission to provider/tool work.
///
/// On startup, this recovers any incomplete provider dispatches from prior
/// crashes or interruptions by fencing them as unknown, then requeues safe
/// recovered intents after the consumer starts.
pub fn run_provider_dispatcher(
    project_root: &Path,
    dispatcher: &BoundedDispatcher,
    config: ProviderHandlerConfig,
) -> Result<()> {
    // Recover provider dispatches: fence unsafe ones, collect ready ones
    let recovered = {
        let mut domain = DomainRepository::open(project_root)?;
        let ready = domain.recover_provider_dispatches()?;
        if !ready.is_empty() {
            eprintln!(
                "Provider dispatcher found {} recoverable call(s) from previous run",
                ready.len()
            );
        }
        ready
    };

    let handler = CanonicalProviderCallHandler::new(config);
    let dispatcher_clone = dispatcher.clone();
    let project_root_clone = project_root.to_path_buf();

    // Use a channel to coordinate consumer startup with recovery producer
    let (ready_tx, ready_rx) = flume::bounded::<()>(1);

    // Start bounded consumer in separate thread
    let consumer_thread = std::thread::Builder::new()
        .name("provider-consumer".to_string())
        .spawn(move || {
            // Signal that consumer is ready to receive
            let _ = ready_tx.send(());
            run_provider_worker(&dispatcher_clone, &handler)
        })
        .map_err(|error| OcgError::config(format!("spawn consumer thread: {error}")))?;

    // Wait for consumer to signal readiness before starting recovery producer
    let _ = ready_rx.recv();

    // Start recovery producer in separate thread with explicit ownership
    let recovery_dispatcher = dispatcher.clone();
    let recovery_thread = std::thread::Builder::new()
        .name("provider-recovery".to_string())
        .spawn(move || -> Result<()> {
            let mut domain = DomainRepository::open(&project_root_clone)?;
            for intent in &recovered {
                requeue_recovered_provider_call(&mut domain, intent, &recovery_dispatcher)?;
            }
            Ok(())
        })
        .map_err(|error| OcgError::config(format!("spawn recovery thread: {error}")))?;

    // Join recovery thread and propagate errors
    let recovery_result = recovery_thread.join();
    let recovery_error = match recovery_result {
        Ok(result) => result.err(),
        Err(panic) => {
            // Recovery thread panicked; propagate it
            std::panic::resume_unwind(panic);
        }
    };

    // Join consumer thread and propagate errors
    let consumer_result = consumer_thread.join();
    let consumer_error = match consumer_result {
        Ok(result) => result.err(),
        Err(panic) => {
            // Consumer thread panicked; propagate it
            std::panic::resume_unwind(panic);
        }
    };

    // Return first error encountered, if any
    if let Some(error) = recovery_error {
        return Err(error);
    }
    if let Some(error) = consumer_error {
        return Err(error);
    }

    Ok(())
}

/// Run the provider worker loop using ntex runtime for async execution.
pub fn run_provider_worker(
    dispatcher: &BoundedDispatcher,
    handler: &CanonicalProviderCallHandler,
) -> Result<()> {
    loop {
        let Some(envelope) = dispatcher.recv()? else {
            return Ok(());
        };
        if let Err(error) = execute_provider_envelope_sync(handler, envelope) {
            tracing::error!(error = %error, "canonical provider Call execution failed; continuing with next bounded item");
        }
    }
}

/// Execute one provider envelope on the runtime that owns this worker thread.
///
/// This is the only runtime boundary on the provider path. The `System` is
/// entered once per Call and everything inside it — the provider round, its
/// HTTP/streaming I/O, and any summarizing round — is awaited on that runtime.
/// Nothing inside may stand up another runtime or block on one.
fn execute_provider_envelope_sync(
    handler: &CanonicalProviderCallHandler,
    envelope: ExecutionEnvelope,
) -> Result<()> {
    let handler = handler.clone();
    let runtime = ntex::rt::System::new("ocg-provider", ntex::rt::DefaultRuntime);
    runtime.block_on(async move {
        let output = handler.execute_validated(envelope).await?;
        crate::orchestration::call_schema::validate_output(&output)?;
        Ok(())
    })
}

/// Run the native tool worker loop synchronously without an async runtime.
pub fn run_native_tool_worker(
    dispatcher: &BoundedDispatcher,
    handler: &crate::native_tools::NativeToolCallHandler,
) -> Result<()> {
    loop {
        let Some(envelope) = dispatcher.recv()? else {
            return Ok(());
        };
        if let Err(error) = execute_native_tool_envelope_sync(handler, envelope) {
            tracing::error!(error = %error, "canonical native tool Call execution failed; continuing with next bounded item");
        }
    }
}

/// Execute one native tool envelope synchronously.
fn execute_native_tool_envelope_sync(
    handler: &crate::native_tools::NativeToolCallHandler,
    envelope: ExecutionEnvelope,
) -> Result<()> {
    let input: serde_json::Value = serde_json::from_str(&envelope.payload)
        .map_err(|error| OcgError::config(format!("invalid Call input JSON: {error}")))?;
    crate::orchestration::call_schema::validate_input(&input)?;
    let output = handler.execute_validated_sync(envelope)?;
    crate::orchestration::call_schema::validate_output(&output)?;
    Ok(())
}

/// The provider Call's admission facts the provider loop needs.
///
/// Grouping them keeps the protocol and the credential that were frozen together
/// at admission in the same value through execution, so neither can be
/// substituted for another on the way to the wire.
struct ProviderAdmission<'a> {
    authority: &'a AttemptAuthority,
    executor_id: &'a str,
    permission_policy: PermissionPolicy,
    shutdown: &'a AtomicBool,
    native_tool_dispatcher: &'a BoundedDispatcher,
    task: &'a str,
}

async fn execute_provider_loop(
    provider: &dyn ProviderClient,
    project_root: &Path,
    envelope: &ExecutionEnvelope,
    request: &mut Value,
    binding: ProviderBinding,
    admission: ProviderAdmission<'_>,
) -> Result<ProviderFinalResponse> {
    let ProviderAdmission {
        authority,
        executor_id,
        permission_policy: _permission_policy,
        shutdown,
        native_tool_dispatcher,
        task,
    } = admission;
    let protocol = binding.protocol;
    let projection = protocol
        .is_openai_chat_completions()
        .then(OpenAiToolProjection::from_registry)
        .transpose()?;
    let model = envelope
        .provider_config
        .as_ref()
        .map(|config| config.upstream_model_id.clone())
        .unwrap_or_default();

    // Inject native tools into request
    let object = request
        .as_object_mut()
        .ok_or_else(|| OcgError::config("provider request must be an object"))?;
    let tools = match &projection {
        Some(projection) => projection.tools(),
        None => NativeToolRegistry::definitions()
            .into_iter()
            .map(|definition| {
                json!({
                    "type": "function",
                    "function": {
                        "name": wire_name_of(definition.name),
                        "description": definition.description,
                        "parameters": definition.parameters,
                    },
                })
            })
            .collect(),
    };
    object.insert("tools".to_string(), Value::Array(tools));

    // Assemble the active context once, before any round: the structured
    // projection and the compacted conversation together form the request this
    // provider Call carries. Every later round appends to that same request
    // rather than re-deciding what context it has.
    apply_active_context(
        request,
        project_root,
        &authority.attempt_id,
        provider,
        &model,
        task,
    )
    .await?;

    for round in 0..MAX_PROVIDER_ROUNDS {
        if shutdown.load(Ordering::SeqCst) || envelope.cancelled.is_cancelled() {
            return Err(OcgError::config("provider loop cancelled"));
        }
        if let Some(tools) = request.get("tools").and_then(Value::as_array) {
            if tools.is_empty() {
                request.as_object_mut().and_then(|obj| obj.remove("tools"));
            }
        }

        let round_response = provider.complete(request).await?;

        if round_response.summary.finish_reason == Some(ChatFinishReason::Length) {
            return Err(OcgError::config("provider exceeded token limit"));
        }
        if round_response.summary.finish_reason == Some(ChatFinishReason::Stop)
            || round_response.summary.tool_calls.is_empty()
        {
            return Ok(ProviderFinalResponse {
                content: round_response.summary.text,
                reasoning: round_response.summary.reasoning,
                rounds: round + 1,
            });
        }
        request
            .get_mut("messages")
            .and_then(Value::as_array_mut)
            .ok_or_else(|| OcgError::config("provider request messages are missing"))?
            .push(round_response.assistant);
        for call in &round_response.summary.tool_calls {
            // A tool call is admitted, executed and completed as a canonical
            // Call. That authority is unchanged here; what changes is what
            // happens when the Runtime can already tell the Call is invalid.
            // Recovery decides, and only a Call that genuinely needs new
            // reasoning is reported as such. See `crate::call_recovery`.
            match resolve_call(project_root, authority, call, projection.as_ref())? {
                ResolvedCall::Admitted {
                    name: canonical_name,
                    arguments,
                    permission,
                    repairs,
                } => {
                    for repair in &repairs {
                        tracing::debug!(
                            tool_call_id = %call.id,
                            ?repair,
                            "deterministically recovered native tool Call before dispatch"
                        );
                    }
                    // Admit and queue Native Tool Call to separate bounded
                    // dispatcher.
                    let payload = json!({
                        "kind": "native_tool",
                        "tool_call_id": call.id,
                        "name": canonical_name,
                        "arguments": arguments
                    });
                    let mut domain = DomainRepository::open(project_root)?;
                    let side_effect = permission != crate::native_tools::PermissionClass::ReadOnly;

                    let tool_call = admit_call(
                        &mut domain,
                        authority,
                        executor_id,
                        side_effect,
                        &payload.to_string(),
                        native_tool_dispatcher,
                    )?;

                    drop(domain);

                    // Wait for child Call completion by polling (blocking is
                    // acceptable here because provider and native tool use
                    // separate dispatchers).
                    let result = wait_for_call_completion(project_root, &tool_call.id)?;

                    push_tool_message(request, call, result)?;
                }
                ResolvedCall::Rejected(content) => {
                    // The Call failed before dispatch. It is reported as a
                    // corrective tool result carrying a compact observation, so
                    // the model can repair in place rather than the Runtime
                    // discarding the round and paying for a fresh one. The full
                    // arguments stay in the provider Call record and journal.
                    push_tool_message(request, call, content)?;
                }
            }
        }
    }
    Err(OcgError::config(format!(
        "provider exceeded the {MAX_PROVIDER_ROUNDS}-round native tool loop limit"
    )))
}

/// What recovery decided about one provider-emitted tool call.
enum ResolvedCall {
    /// Dispatchable, with any deterministic repairs already applied.
    Admitted {
        name: &'static str,
        arguments: Value,
        permission: crate::native_tools::PermissionClass,
        repairs: Vec<crate::call_recovery::Repair>,
    },
    /// Not dispatchable. The payload is the compact observation to feed back.
    Rejected(String),
}

/// Run one tool call through schema validation and recovery.
///
/// This is the single point where a provider tool call becomes a canonical Call,
/// so it is the only place a dispatch failure can be characterized before any
/// side effect is possible.
fn resolve_call(
    project_root: &Path,
    authority: &AttemptAuthority,
    call: &CompletedToolCall,
    projection: Option<&OpenAiToolProjection>,
) -> Result<ResolvedCall> {
    let reference = format!("tool_call {}", call.id);
    let bindings = recovery::TargetBindings::from_calls(
        &DomainRepository::open(project_root)?.calls_for_attempt(&authority.attempt_id)?,
    );

    // Parse first. A malformed argument blob is not a schema failure and has no
    // field to repair, so it never enters the recovery flow.
    let wire_arguments: Value = match serde_json::from_str(&call.arguments) {
        Ok(value) => value,
        Err(error) => {
            let failure = recovery::CallFailure::MalformedArguments {
                tool: call.name.clone(),
                reason: format!("arguments are not valid JSON: {error}"),
            };
            return Ok(ResolvedCall::Rejected(
                failure.compact_observation(&reference),
            ));
        }
    };

    // `confidence` is deliberately not consumed here: recovery records the match
    // kind on the repair it emits, so the dispatch path stays the single
    // authority on why a Call was allowed through.
    let Some((definition, _confidence)) = recovery::resolve_tool(&call.name) else {
        let failure = recovery::CallFailure::UnknownTool {
            requested: call.name.clone(),
            candidates: recovery::tool_candidates(&call.name),
        };
        return Ok(ResolvedCall::Rejected(
            failure.compact_observation(&reference),
        ));
    };

    // A Chat Completions provider was offered the OpenAI wire name and a
    // strict-schema wire shape, not the canonical schema. Undoing that widening
    // happens before preflight so the failure the model sees names canonical
    // fields, and strict validation still runs first: normalization on its own
    // would hide a wire violation, because dropping a null optional field makes
    // the canonical form valid.
    //
    // A protocol that carries the canonical schema on the wire has nothing to
    // undo and nothing to widen, so its arguments are already canonical and
    // preflight validates them against the tool's own schema.
    let canonical = if let Some(projection) = projection {
        match normalize_wire_arguments(projection, &call.name, &definition, &wire_arguments) {
            Ok(canonical) => canonical,
            Err(message) => {
                // The validator's own error text interpolates the offending
                // value, so it is never forwarded to the model: a wrongly-typed
                // `oldString` would otherwise drag its own body back into the
                // context that already holds it. The classified observation
                // replaces it, and the validator text stays in the trace.
                let failure = recovery::CallFailure::InvalidArguments(wire_failure(
                    &definition,
                    &wire_arguments,
                ));
                tracing::debug!(
                    tool = %call.name,
                    tool_call_id = %call.id,
                    %message,
                    "native tool wire arguments rejected before dispatch"
                );
                return Ok(ResolvedCall::Rejected(
                    failure.compact_observation(&reference),
                ));
            }
        }
    } else {
        wire_arguments
    };

    // Recovery is registry-driven: the tool name is resolved and the arguments
    // validated against the tool's own schema, then only what Runtime state can
    // settle is settled here. Everything still outstanding becomes a field-scoped
    // repair request rather than a fresh round.
    match recovery::recover(&call.name, &canonical, &bindings, &reference) {
        recovery::RecoveryAction::Dispatch {
            definition,
            arguments,
            repairs,
        } => {
            let permission =
                crate::native_tools::tool_permission_for(definition.name).ok_or_else(|| {
                    OcgError::config(format!("unknown tool permission: {}", definition.name))
                })?;
            Ok(ResolvedCall::Admitted {
                name: definition.name,
                arguments,
                permission,
                repairs,
            })
        }
        recovery::RecoveryAction::ConstrainedRepair(repair) => {
            Ok(ResolvedCall::Rejected(repair.instruction()))
        }
        recovery::RecoveryAction::NeedsReasoning(failure) => Ok(ResolvedCall::Rejected(
            failure.compact_observation(&reference),
        )),
    }
}

/// Classify a wire-schema rejection without echoing the offending values.
///
/// The provider is held to the strict schema it was offered, which lists every
/// property as required and widens optional ones to nullable. A field sent as
/// `null` is therefore a wire defect, not a canonical one, and is reported
/// against the canonical schema the tool actually owns.
fn wire_failure(
    definition: &crate::native_tools::NativeToolDefinition,
    wire_arguments: &Value,
) -> recovery::InvalidArguments {
    // Validate against the canonical schema after stripping the widening nulls,
    // so the reported field names and types are the ones a repair must satisfy.
    let canonical = match recovery::preflight(definition, wire_arguments) {
        Ok(Some(recovery::CallFailure::InvalidArguments(invalid))) => invalid,
        _ => recovery::InvalidArguments {
            tool: definition.name.to_string(),
            missing: Vec::new(),
            invalid: Vec::new(),
            unexpected: wire_unexpected(definition, wire_arguments),
        },
    };
    recovery::InvalidArguments {
        tool: definition.name.to_string(),
        missing: canonical.missing,
        invalid: canonical.invalid,
        unexpected: if canonical.unexpected.is_empty() {
            wire_unexpected(definition, wire_arguments)
        } else {
            canonical.unexpected
        },
    }
}

/// Fields the provider sent that the strict schema does not declare.
fn wire_unexpected(
    definition: &crate::native_tools::NativeToolDefinition,
    wire_arguments: &Value,
) -> Vec<recovery::FieldIssue> {
    let Some(declared) = definition
        .parameters
        .get("properties")
        .and_then(Value::as_object)
    else {
        return Vec::new();
    };
    wire_arguments
        .as_object()
        .map(|fields| {
            fields
                .keys()
                .filter(|name| !declared.contains_key(name.as_str()))
                .map(|name| recovery::FieldIssue {
                    field: name.clone(),
                    instance_path: String::new(),
                    defect: recovery::ArgumentDefect::Unexpected,
                    observed: None,
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Undo strict-mode widening after the wire schema is satisfied.
///
/// The projection owns this transform for the exact wire name the provider was
/// given. An alias-resolved Call did not arrive under a wire name the projection
/// offered, so its wire shape is the canonical one with nulls already dropped by
/// the provider's own emission — nothing to undo.
fn normalize_wire_arguments(
    projection: &OpenAiToolProjection,
    requested: &str,
    definition: &crate::native_tools::NativeToolDefinition,
    wire_arguments: &Value,
) -> std::result::Result<Value, String> {
    if requested == wire_name_of(definition.name) {
        return Ok(projection
            .resolve(requested)
            .map_err(|error| error.to_string())?
            .canonical_arguments(wire_arguments));
    }
    // An alias Call still has to satisfy the strict schema of the tool it
    // resolved to, otherwise a name substitution would be a way to bypass
    // validation.
    let tool = projection
        .resolve(&wire_name_of(definition.name))
        .map_err(|error| error.to_string())?;
    if let Err(error) = tool.validate_wire_arguments(wire_arguments) {
        return Err(error.to_string());
    }
    Ok(tool.canonical_arguments(wire_arguments))
}

fn wire_name_of(canonical_name: &str) -> String {
    canonical_name.replace('.', "_")
}

/// Replace the request's messages with the assembled active context.
///
/// This is the single boundary at which context enters a provider request. The
/// Context Engine, verification and Call recovery do not each reach into the
/// request; they are read here and rendered once, by
/// [`crate::provider_context`]. A projection that cannot be assembled leaves the
/// conversation exactly as the caller built it, because losing context must not
/// cost the Call.
///
/// `provider` and `model` are handed to the projection so a compaction that
/// needs a summary can reach a model. The summarizing call runs under the
/// authority of the provider Call being executed: it is one step in assembling
/// that Call's context, so it is not a separate execution and gets no admission
/// of its own.
async fn apply_active_context<'p>(
    request: &mut Value,
    project_root: &Path,
    attempt_id: &str,
    provider: &'p dyn ProviderClient,
    model: &str,
    task: &str,
) -> Result<()> {
    let conversation = request
        .get("messages")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let model = model.to_owned();
    // The summarizing call is awaited on the same runtime as the round itself,
    // so it reuses that execution context instead of blocking for a new one.
    let summarize = move |prompt: String, max_tokens: u64| -> BoxFuture<'p, Result<String>> {
        let summary_request = json!({
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": max_tokens,
            "stream": false,
        });
        Box::pin(async move { Ok(provider.complete(&summary_request).await?.summary.text) })
    };
    let assembled = match crate::provider_context::assemble(
        project_root,
        crate::provider_context::ContextInputs {
            task,
            attempt_id,
            tools: request.get("tools"),
            conversation: conversation.clone(),
            summarize: Some(Box::new(summarize)),
        },
    )
    .await
    {
        Ok(assembled) => assembled,
        Err(error) => {
            tracing::warn!(%error, "active context projection failed; sending the conversation as assembled");
            return Ok(());
        }
    };
    tracing::debug!(
        sources = ?assembled.sources,
        projection_bytes = assembled.bytes,
        conversation_messages = assembled.messages.len(),
        "provider request context assembled"
    );
    let mut messages = Vec::with_capacity(assembled.messages.len() + 1);
    messages.extend(assembled.system);
    messages.extend(assembled.messages);
    request
        .as_object_mut()
        .ok_or_else(|| OcgError::config("provider request must be an object"))?
        .insert("messages".to_string(), Value::Array(messages));
    Ok(())
}

/// The task text a context plan is ranked against.
///
/// Compatibility fallback for Calls admitted without a frozen task.
fn objective_of(conversation: &[Value]) -> String {
    conversation
        .iter()
        .rev()
        .find(|message| message.get("role").and_then(Value::as_str) == Some("user"))
        .and_then(|message| message.get("content"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

/// Append a tool result to the request.
fn push_tool_message(request: &mut Value, call: &CompletedToolCall, content: String) -> Result<()> {
    request
        .get_mut("messages")
        .and_then(Value::as_array_mut)
        .ok_or_else(|| OcgError::config("provider request messages are missing"))?
        .push(json!({
            "role": "tool",
            "tool_call_id": call.id,
            "content": content
        }));
    Ok(())
}

/// Poll for Call completion. This blocks the provider handler but does not
/// block the native tool consumer since they use separate dispatchers.
///
/// A failed Call is returned as its recorded result rather than raised as an
/// error. A tool that failed — a stale revision, an ambiguous target, a denied
/// permission — is a fact the model needs to correct, and raising it aborted the
/// entire provider Call, discarding the round and every fact in it.
fn wait_for_call_completion(project_root: &Path, call_id: &str) -> Result<String> {
    for _ in 0..600 {
        std::thread::sleep(std::time::Duration::from_millis(100));
        let domain = DomainRepository::open(project_root)?;
        let call = domain.call(call_id)?;
        match call.state.as_str() {
            "completed" => {
                return call.response.ok_or_else(|| {
                    OcgError::config("completed native tool Call has no response")
                })
            }
            "failed" => {
                return Ok(call.response.unwrap_or_else(|| {
                    json!({"success": false, "error": {"kind": "unknown", "message": "native tool Call failed"}}).to_string()
                }))
            }
            "fenced" => {
                return Ok(json!({
                    "success": false,
                    "error": {
                        "kind": "fenced",
                        "message": "native tool Call was fenced; its effect is unknown and it will not be retried automatically"
                    }
                })
                .to_string())
            }
            _ => continue,
        }
    }
    Err(OcgError::config("native tool Call timeout"))
}

impl ProviderClient for NativeOpenAiCompatibleProvider<'_> {
    fn complete(&self, request: &Value) -> BoxFuture<'_, Result<ProviderRound>> {
        // The frozen upstream model id is authoritative on the wire.
        let mut body = request.clone();
        if let Some(object) = body.as_object_mut() {
            object.insert(
                "model".to_string(),
                Value::String(self.upstream_model_id.clone()),
            );
        }
        // Default to streaming so the success path is incremental.
        let streaming = body.get("stream").and_then(Value::as_bool).unwrap_or(true);
        if body.get("stream").is_none() {
            body["stream"] = Value::Bool(true);
        }

        let mut headers = vec![
            ("Content-Type".to_string(), "application/json".to_string()),
            ("Accept".to_string(), accept_for(streaming)),
        ];
        // The Vault stores the raw token; the header value is built only here.
        if let Some(token) = self.bearer.as_deref() {
            headers.push(("Authorization".to_string(), format!("Bearer {token}")));
        }

        // The round owns its request and headers, so the future borrows only
        // `self` and runs on the runtime the provider worker already owns. The
        // chunk callback is unchanged: the transport still delivers each chunk
        // as it arrives, so this stays an incremental stream rather than a
        // buffered body handed to the parser at the end.
        Box::pin(async move {
            run_streamed_round(
                StreamedRoundState::new(),
                self.transport,
                ProviderRequest {
                    endpoint: &self.endpoint,
                    headers: &headers,
                    body: &body,
                    secret: self.bearer.as_deref(),
                    on_http_failure: openai_http_failure,
                },
                self.events.clone(),
                &self.cancelled,
            )
            .await
        })
    }
}

/// A provider client speaking the Anthropic Messages protocol natively.
///
/// It encodes the canonical provider request into a Messages request, reads the
/// Messages SSE stream incrementally through the existing ntex transport, and
/// decodes it into the same canonical provider events every other protocol
/// produces. Anthropic is never reached through an OpenAI-compatible
/// translation, and this is not a second execution path: the Call lifecycle,
/// cancellation, admission and settlement below the protocol boundary are the
/// same ones the OpenAI providers use.
pub struct NativeAnthropicProvider<'a> {
    transport: &'a dyn HttpTransport,
    endpoint: String,
    /// The Anthropic `x-api-key`, resolved from the Vault immediately before the
    /// request. Held only in memory; never persisted or logged.
    api_key: Option<String>,
    upstream_model_id: String,
    cancelled: CallCancellation,
    events: Option<flume::Sender<ExecutionEvent>>,
}

impl<'a> NativeAnthropicProvider<'a> {
    pub fn new(
        transport: &'a dyn HttpTransport,
        endpoint: impl Into<String>,
        api_key: Option<String>,
        upstream_model_id: impl Into<String>,
        cancelled: CallCancellation,
        events: Option<flume::Sender<ExecutionEvent>>,
    ) -> Self {
        Self {
            transport,
            endpoint: endpoint.into(),
            api_key,
            upstream_model_id: upstream_model_id.into(),
            cancelled,
            events,
        }
    }
}

/// The decoding state of one Anthropic provider round.
///
/// Anthropic's SSE terminates on `message_stop` rather than a `[DONE]`
/// sentinel, so the terminal condition is the decoded event sequence itself.
/// A non-streamed Messages body is decoded on the same state, because the
/// summarizing round the Context Engine runs is explicitly non-streamed.
struct AnthropicRoundState {
    accumulator: SseAccumulator,
    decoder: crate::anthropic::stream::AnthropicStreamState,
    summary: ChatStreamSummary,
    json_buffer: Vec<u8>,
    mode: StreamMode,
}

impl AnthropicRoundState {
    fn new() -> Self {
        Self {
            accumulator: SseAccumulator::new(),
            decoder: crate::anthropic::stream::AnthropicStreamState::new(),
            summary: ChatStreamSummary::default(),
            json_buffer: Vec::new(),
            mode: StreamMode::Undetermined,
        }
    }

    fn apply(&mut self, event: &Value) -> Result<(bool, Vec<ChatStreamEvent>)> {
        let (keep_reading, emitted) = self.decoder.consume(event)?;
        for event in &emitted {
            self.summary.apply(event);
        }
        Ok((keep_reading, emitted))
    }
}

impl ProviderRoundState for AnthropicRoundState {
    fn consume(&mut self, chunk: &[u8]) -> Result<(bool, Vec<ChatStreamEvent>)> {
        if self.mode == StreamMode::Undetermined {
            self.mode = detect_stream_mode(chunk);
        }
        match self.mode {
            StreamMode::Sse => {
                let mut emitted = Vec::new();
                let mut keep_reading = true;
                for event in self.accumulator.consume(chunk)? {
                    let (reading, produced) = self.apply(&event)?;
                    emitted.extend(produced);
                    keep_reading = reading;
                }
                // Anthropic has no `[DONE]` sentinel, so the decoded terminal
                // event is what ends the read.
                if self.decoder.is_finished() {
                    keep_reading = false;
                }
                Ok((keep_reading, emitted))
            }
            StreamMode::CompletionJson => {
                self.json_buffer.extend_from_slice(chunk);
                Ok((true, Vec::new()))
            }
            StreamMode::Undetermined => Ok((true, Vec::new())),
        }
    }

    fn finish(&mut self) -> Result<ProviderRound> {
        if self.mode == StreamMode::Sse {
            for event in self.accumulator.finish()? {
                // Events left unflushed at end of stream still have to reach the
                // decoder; a terminal `error` event is reported from here.
                self.apply(&event)?;
            }
            self.decoder.finish(&mut self.summary)?;
        } else {
            let message: Value = serde_json::from_slice(&self.json_buffer).map_err(|error| {
                OcgError::config(format!("invalid Anthropic Messages response: {error}"))
            })?;
            crate::anthropic::stream::apply_message(&mut self.summary, &message)?;
        }
        // Anthropic streams tool-call arguments as JSON fragments. A fragment
        // that never became a JSON object is a provider protocol failure, not a
        // tool error the model could repair, so the round fails here instead of
        // becoming a rejected tool Call.
        crate::anthropic::stream::validate_tool_arguments(&self.summary)?;
        Ok(ProviderRound {
            assistant: build_assistant_message(&self.summary),
            summary: self.summary.clone(),
        })
    }
}

impl ProviderClient for NativeAnthropicProvider<'_> {
    fn complete(&self, request: &Value) -> BoxFuture<'_, Result<ProviderRound>> {
        // Default to streaming so the success path is incremental.
        let streaming = request
            .get("stream")
            .and_then(Value::as_bool)
            .unwrap_or(true);
        // A canonical request OCG cannot encode is a round that never dispatches.
        // The error travels inside the future so it settles on the same failure
        // path as a transport error, rather than unwinding in the caller.
        let encoded =
            crate::anthropic::request::build_request(request, &self.upstream_model_id, streaming);

        let mut headers = vec![
            ("Content-Type".to_string(), "application/json".to_string()),
            ("Accept".to_string(), accept_for(streaming)),
            (
                "anthropic-version".to_string(),
                crate::anthropic::ANTHROPIC_VERSION.to_string(),
            ),
        ];
        // The Vault value is Anthropic's API key verbatim. The header value is
        // built here and nowhere else.
        if let Some(api_key) = self.api_key.as_deref() {
            headers.push(("x-api-key".to_string(), api_key.to_string()));
        }

        Box::pin(async move {
            let body = encoded?;
            run_streamed_round(
                AnthropicRoundState::new(),
                self.transport,
                ProviderRequest {
                    endpoint: &self.endpoint,
                    headers: &headers,
                    body: &body,
                    secret: self.api_key.as_deref(),
                    on_http_failure: anthropic_http_failure,
                },
                self.events.clone(),
                &self.cancelled,
            )
            .await
        })
    }
}

/// The wire protocol frozen for one provider Call.
///
/// The failure normalization each protocol needs travels with the protocol
/// client it selected, so the two cannot be swapped independently: `provider_client`
/// and `execute_provider_loop` are both driven from this one value, and the
/// provider reads the same two functions when it builds its request.
struct ProviderBinding {
    protocol: ProviderProtocol,
}

impl ProviderBinding {
    /// The binding for a protocol.
    const fn of(protocol: ProviderProtocol) -> Self {
        Self { protocol }
    }
}

/// Build the provider client a frozen protocol dispatches to.
///
/// One dispatch point, so adding a protocol cannot leave a route that silently
/// speaks another protocol's wire format. Credential resolution has already
/// happened upstream: the caller passes the Vault value, never a reference.
fn provider_client<'a>(
    binding: ProviderBinding,
    transport: &'a dyn HttpTransport,
    config: &crate::orchestration::execution_dispatch::ProviderExecutionConfig,
    credential: Option<String>,
    cancelled: CallCancellation,
    events: Option<flume::Sender<ExecutionEvent>>,
) -> Box<dyn ProviderClient + 'a> {
    match binding.protocol {
        ProviderProtocol::Anthropic => Box::new(NativeAnthropicProvider::new(
            transport,
            config.endpoint.clone(),
            credential,
            config.upstream_model_id.clone(),
            cancelled,
            events,
        )),
        ProviderProtocol::OpenAi | ProviderProtocol::OpenAiCompatible => {
            Box::new(NativeOpenAiCompatibleProvider::new(
                transport,
                config.endpoint.clone(),
                credential,
                config.upstream_model_id.clone(),
                cancelled,
                events,
            ))
        }
    }
}

fn accept_for(streaming: bool) -> String {
    if streaming {
        "text/event-stream".to_string()
    } else {
        "application/json".to_string()
    }
}

/// Normalize a non-2xx OpenAI-compatible response into a canonical failure.
fn openai_http_failure(status: u16, body: &[u8]) -> OcgError {
    let excerpt = String::from_utf8_lossy(body);
    OcgError::config(format!(
        "OpenAI-compatible provider returned HTTP {status}: {excerpt}"
    ))
}

/// Normalize a non-2xx Anthropic response into a canonical failure, keeping the
/// provider's own error type alongside the HTTP status.
fn anthropic_http_failure(status: u16, body: &[u8]) -> OcgError {
    let failure = crate::anthropic::error::anthropic_failure_from_body(status, body);
    OcgError::config(crate::anthropic::describe(&failure))
}

/// Drive one streamed provider round.
///
/// Chunk delivery, backpressure, cancellation racing and terminalization are
/// protocol-independent: a protocol supplies the decoding state, the headers and
/// how a non-2xx body becomes a failure. The transport delivers each chunk as it
/// arrives and the sink is called before the next chunk is read, so this stays
/// an incremental stream rather than a buffered body handed to a parser at the
/// end.
/// The endpoint, headers and body one protocol put on the wire, plus the
/// credential that must never appear in a failure message.
struct ProviderRequest<'a> {
    endpoint: &'a str,
    headers: &'a [(String, String)],
    body: &'a Value,
    secret: Option<&'a str>,
    on_http_failure: fn(u16, &[u8]) -> OcgError,
}

async fn run_streamed_round<S: ProviderRoundState + 'static>(
    state: S,
    transport: &dyn HttpTransport,
    wire: ProviderRequest<'_>,
    events: Option<flume::Sender<ExecutionEvent>>,
    cancelled: &CallCancellation,
) -> Result<ProviderRound> {
    let secret = wire.secret.filter(|secret| !secret.is_empty());
    run_streamed_round_inner(state, transport, wire, events, cancelled)
        .await
        .map_err(|error| {
            let message = error.to_string();
            match secret {
                Some(secret) if message.contains(secret) => {
                    OcgError::config(message.replace(secret, "<redacted>"))
                }
                _ => error,
            }
        })
}

fn redact_provider_event(event: &mut ChatStreamEvent, secret: Option<&str>) {
    use crate::openai_compatible::error::TransportError;

    let Some(secret) = secret.filter(|secret| !secret.is_empty()) else {
        return;
    };
    let ChatStreamEvent::Error(error) = event else {
        return;
    };
    match error {
        TransportError::Provider(failure) => {
            failure.message = failure.message.replace(secret, "<redacted>");
            for value in [
                &mut failure.provider_code,
                &mut failure.response_body,
                &mut failure.request_id,
            ]
            .into_iter()
            .flatten()
            {
                *value = value.replace(secret, "<redacted>");
            }
        }
        TransportError::InvalidRequest { field, message }
        | TransportError::Unsupported { field, message } => {
            *field = field.replace(secret, "<redacted>");
            *message = message.replace(secret, "<redacted>");
        }
        TransportError::Build { message } => {
            *message = message.replace(secret, "<redacted>");
        }
    }
}

async fn run_streamed_round_inner<S: ProviderRoundState + 'static>(
    state: S,
    transport: &dyn HttpTransport,
    wire: ProviderRequest<'_>,
    events: Option<flume::Sender<ExecutionEvent>>,
    cancelled: &CallCancellation,
) -> Result<ProviderRound> {
    let state = Arc::new(Mutex::new(state));
    let callback_state = Arc::clone(&state);
    let callback_events = events.clone();
    let callback_cancelled = cancelled.clone();
    let callback_secret = wire.secret.map(str::to_owned);
    let on_chunk: crate::http::ChunkSink = Box::new(move |chunk: &[u8]| -> Result<bool> {
        // Cancellation is checked during consumption; stop reading and let the
        // caller close the Call as cancelled, never completed.
        if callback_cancelled.is_cancelled() {
            return Ok(false);
        }
        let mut guard = callback_state
            .lock()
            .map_err(|_| OcgError::config("provider stream state poisoned"))?;
        let (keep_reading, emitted) = guard.consume(chunk)?;
        if let Some(sender) = &callback_events {
            for mut event in emitted {
                redact_provider_event(&mut event, callback_secret.as_deref());
                if let Err(error) = sender.send(ExecutionEvent::Provider(event)) {
                    tracing::debug!(%error, "provider event receiver disconnected");
                }
            }
        }
        Ok(keep_reading)
    });

    let header_refs = wire
        .headers
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .collect::<Vec<_>>();
    // The read is raced against this Call's cancellation token, so a turn can be
    // ended even when the upstream has gone silent and no further byte will ever
    // arrive. The chunk callback already stops at the next chunk; this ends the
    // read when there is no next chunk. Losing the race drops the read future,
    // which drops the response and closes the connection, so the worker returns
    // and the next bounded envelope can start.
    let response = match ntex::util::select(
        transport.post_json_stream_in_runtime(wire.endpoint, &header_refs, wire.body, on_chunk),
        cancelled.cancelled(),
    )
    .await
    {
        ntex::util::Either::Left(response) => response?,
        ntex::util::Either::Right(()) => {
            // Cancellation owns terminalization; this is not a provider
            // failure and must not be reported as one.
            return Err(OcgError::config("provider Call cancelled during streaming"));
        }
    };

    if !response.is_success() {
        // Bounded, redacted diagnostic. The provider's error body is small and
        // any occurrence of the credential is removed before the message leaves
        // this function, so a credential echoed back by an endpoint can never
        // reach the Call record, the journal or the Chat surface.
        let body_limit = response
            .body
            .len()
            .min(crate::http::MAX_PROVIDER_ERROR_BODY);
        let mut excerpt = String::from_utf8_lossy(&response.body[..body_limit]).into_owned();
        if let Some(secret) = wire.secret {
            excerpt = excerpt.replace(secret, "<redacted>");
        }
        return Err((wire.on_http_failure)(response.status, excerpt.as_bytes()));
    }

    // If cancellation interrupted the stream, this round is not a completion.
    if cancelled.is_cancelled() {
        return Err(OcgError::config("provider Call cancelled during streaming"));
    }

    let mut state = state
        .lock()
        .map_err(|_| OcgError::config("provider stream state poisoned"))?;
    state.finish()
}

fn fail_provider_envelope(
    domain: &mut DomainRepository,
    envelope: &ExecutionEnvelope,
    reason: &str,
    call_claimed: bool,
) -> Result<bool> {
    if call_claimed {
        domain.fail_call(
            &envelope.call_id,
            &envelope.attempt_id,
            envelope.generation,
            reason,
        )?;
    } else if !domain.fail_unclaimed_call(
        &envelope.call_id,
        &envelope.attempt_id,
        envelope.generation,
        reason,
    )? {
        return Ok(false);
    }
    if let Err(error) = envelope
        .events
        .send(crate::orchestration::execution_dispatch::ExecutionEvent::Failed(reason.to_string()))
    {
        tracing::debug!(error = %error, "provider failure receiver closed");
    }
    Ok(true)
}

/// Settle a provider execution that genuinely failed while this worker still
/// held its Attempt authority.
///
/// It first settles the Call and its DispatchIntent exactly as
/// [`fail_provider_envelope`] does, then closes the Attempt with the domain's
/// existing failure settlement authority, which fences anything still
/// outstanding, moves the Attempt's Executors to `failed`, revokes the
/// Attempt's authority and carries the Job to `failed` in one transaction.
///
/// The authority is re-read immediately before settling, on the same terms the
/// success path validates: the Attempt must still be authoritative, its Job must
/// still point at it, and this envelope must still be that generation's work. A
/// stale or replaced envelope is a durable no-op. Before a claim, an existing
/// claimant is also left alone; only this worker's claimed Call may be failed
/// while running. A signalled but still-current queued Call uses cancellation,
/// not failure settlement.
fn fail_authoritative_provider_call(
    project_root: &Path,
    envelope: &ExecutionEnvelope,
    reason: &str,
    call_claimed: bool,
) {
    if let Err(error) = (|| -> Result<()> {
        let mut domain = DomainRepository::open(project_root)?;
        let held = domain
            .authority(&envelope.attempt_id)?
            .filter(|authority| {
                authority.job_id == envelope.job_id && authority.generation == envelope.generation
            })
            .is_some();
        if !held {
            return Ok(());
        }
        let call = domain.call(&envelope.call_id)?;
        if call.attempt_id != envelope.attempt_id || call.generation != envelope.generation {
            return Ok(());
        }
        // Before this worker claims the Call, running/completed means another
        // actor owns it. A terminal unsuccessful Call has no actor left to settle
        // its still-authoritative Attempt; it must not be mistaken for a claimant.
        if !call_claimed && matches!(call.state.as_str(), "running" | "completed") {
            return Ok(());
        }
        if envelope.cancelled.is_cancelled() {
            domain.request_cancel(&envelope.attempt_id)?;
            return domain.confirm_cancel(&envelope.attempt_id, true);
        }
        if matches!(call.state.as_str(), "created" | "running") {
            if !fail_provider_envelope(&mut domain, envelope, reason, call_claimed)? {
                // A concurrent claimant owns running/completed, but a concurrent
                // Call failure may still have left this Attempt without an owner.
                let current = domain.call(&envelope.call_id)?;
                if current.attempt_id != envelope.attempt_id
                    || current.generation != envelope.generation
                    || !matches!(current.state.as_str(), "failed" | "unknown")
                    || domain
                        .authority(&envelope.attempt_id)?
                        .is_none_or(|authority| {
                            authority.job_id != envelope.job_id
                                || authority.generation != envelope.generation
                        })
                {
                    return Ok(());
                }
            }
        } else if let Err(error) = envelope.events.send(
            crate::orchestration::execution_dispatch::ExecutionEvent::Failed(reason.to_string()),
        ) {
            tracing::debug!(error = %error, "provider failure receiver closed");
        }
        domain.finish_attempt(&envelope.attempt_id, false)
    })() {
        tracing::error!(error = %error, call_id = %envelope.call_id, "provider Attempt failure could not be settled");
    }
}
