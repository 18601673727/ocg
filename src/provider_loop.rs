//! OCG-owned OpenAI-compatible provider loop.
//!
//! Provider wire handling ends at normalized assistant text/tool calls. Native
//! Tool authority stays in `native_tools`, and every tool invocation is
//! admitted and completed as a canonical Call before its tool message is sent
//! back to the provider.

use crate::error::{OcgError, Result};
use crate::http::HttpTransport;
use crate::native_tools::{
    execute_canonical_tool_call, tool_permission_for, NativeCallRequest, NativeToolRegistry,
    PermissionPolicy,
};
use crate::openai_compatible::{
    ChatFinishReason, ChatStreamEvent, ChatStreamSummary, NormalizedUsage, ToolCallNormalizer,
};
use crate::orchestration::domain::{AttemptAuthority, DomainRepository, Executor};
use crate::orchestration::execution_dispatch::{
    admit_call, BoundedDispatcher, CompioExecutor, ExecutionEnvelope, ValidatedCompioCallHandler,
};
use serde_json::{json, Value};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

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

pub trait OpenAiCompatibleProvider: Send + Sync {
    fn complete(&self, request: &Value) -> Result<ProviderRound>;
}

/// A provider client using the repository's native ntex/Compio HTTP surface.
/// It decodes OpenAI-compatible response JSON/SSE and uses the existing
/// `ToolCallNormalizer`/`ChatStreamSummary` path for tool-call assembly.
pub struct NativeOpenAiCompatibleProvider<'a> {
    transport: &'a dyn HttpTransport,
    endpoint: String,
    bearer: Option<String>,
}

impl<'a> NativeOpenAiCompatibleProvider<'a> {
    pub fn new(
        transport: &'a dyn HttpTransport,
        endpoint: impl Into<String>,
        bearer: Option<String>,
    ) -> Self {
        Self {
            transport,
            endpoint: endpoint.into(),
            bearer,
        }
    }
}

/// Configuration for the canonical provider handler. The handler owns the
/// provider transport and project boundary, while each envelope supplies the
/// Attempt/Call identity that must be revalidated before execution.
pub struct ProviderHandlerConfig {
    pub transport: Arc<dyn HttpTransport>,
    pub endpoint: String,
    pub bearer: Option<String>,
    pub project_root: std::path::PathBuf,
    pub permission_policy: PermissionPolicy,
    pub cancelled: Arc<AtomicBool>,
}

pub struct CanonicalProviderCallHandler {
    config: Arc<ProviderHandlerConfig>,
}

impl CanonicalProviderCallHandler {
    pub fn new(config: ProviderHandlerConfig) -> Self {
        Self {
            config: Arc::new(config),
        }
    }
}

/// Admit one provider request into the same bounded dispatcher used by other
/// canonical Calls. The returned Call is the durable owner of the provider
/// request; a queue item alone never authorizes execution.
///
/// Budget admission is marked immediately after Call creation, ensuring provider
/// usage goes through economic admission before execution.
pub fn admit_provider_call(
    domain: &mut DomainRepository,
    authority: &AttemptAuthority,
    executor_id: &str,
    request: Value,
    dispatcher: &BoundedDispatcher,
) -> Result<crate::orchestration::domain::Call> {
    let payload = json!({
        "executor_transport": "provider",
        "arguments": request
    });
    let call = admit_call(
        domain,
        authority,
        executor_id,
        true,
        &payload.to_string(),
        dispatcher,
    )?;
    
    // Mark budget admitted before execution. Provider calls have metered usage
    // and must go through economic admission before dispatch.
    domain.mark_budget_admitted(&call.id)?;
    
    Ok(call)
}

/// Run provider envelopes through the existing bounded Compio executor. This
/// is the production handoff from canonical admission to provider/tool work.
/// 
/// On startup, this recovers any incomplete provider dispatches from prior
/// crashes or interruptions by fencing them as unknown.
pub fn run_provider_dispatcher(
    project_root: &Path,
    dispatcher: &BoundedDispatcher,
    config: ProviderHandlerConfig,
) -> Result<()> {
    // Recover any incomplete provider calls from previous runs before starting
    // the dispatcher. This ensures provider intents left in 'running' state
    // are properly fenced as unknown.
    {
        let mut domain = DomainRepository::open(project_root)?;
        let recovered = domain.recover_provider_dispatches()?;
        if !recovered.is_empty() {
            eprintln!(
                "Provider dispatcher recovered {} incomplete call(s) from previous run",
                recovered.len()
            );
        }
    } // Release domain lock before starting executor
    
    let handler = CanonicalProviderCallHandler::new(config);
    CompioExecutor::run(dispatcher, &handler)
}

impl ValidatedCompioCallHandler for CanonicalProviderCallHandler {
    fn execute_validated(
        &self,
        envelope: ExecutionEnvelope,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value>> + Send + '_>> {
        let config = Arc::clone(&self.config);
        Box::pin(async move {
            if config.cancelled.load(Ordering::SeqCst) {
                fail_provider_envelope(
                    &config.project_root,
                    &envelope,
                    "cancelled before provider execution",
                );
                return Err(OcgError::config("provider Call cancelled before execution"));
            }
            let mut domain = DomainRepository::open(&config.project_root)?;
            let authority = domain
                .authority(&envelope.attempt_id)?
                .filter(|authority| {
                    authority.job_id == envelope.job_id
                        && authority.generation == envelope.generation
                })
                .ok_or_else(|| {
                    fail_provider_envelope(
                        &config.project_root,
                        &envelope,
                        "stale Attempt authority",
                    );
                    OcgError::config("provider Call has stale Attempt authority")
                })?;
            if let Err(error) =
                domain.start_call(&envelope.call_id, &envelope.attempt_id, envelope.generation)
            {
                fail_provider_envelope(&config.project_root, &envelope, &error.to_string());
                return Err(error);
            }
            let input: Value = match serde_json::from_str(&envelope.payload) {
                Ok(input) => input,
                Err(error) => {
                    let message = format!("invalid provider Call payload: {error}");
                    fail_provider_envelope(&config.project_root, &envelope, &message);
                    return Err(OcgError::config(message));
                }
            };
            let mut request = match input.get("arguments").cloned() {
                Some(request) => request,
                None => {
                    let message = "provider Call payload has no arguments";
                    fail_provider_envelope(&config.project_root, &envelope, message);
                    return Err(OcgError::config(message));
                }
            };
            if let Err(error) = inject_native_tools(&mut request) {
                let message = error.to_string();
                fail_provider_envelope(&config.project_root, &envelope, &message);
                return Err(error);
            }
            let executor = match envelope.executor_id.as_deref() {
                Some(id) => domain
                    .executor(id)?
                    .ok_or_else(|| OcgError::config("provider Call executor no longer exists"))?,
                None => {
                    let message = "provider Call has no Executor";
                    fail_provider_envelope(&config.project_root, &envelope, message);
                    return Err(OcgError::config(message));
                }
            };
            let provider = NativeOpenAiCompatibleProvider::new(
                config.transport.as_ref(),
                config.endpoint.clone(),
                config.bearer.clone(),
            );
            let mut loop_runner = ProviderToolLoop {
                provider,
                domain: &mut domain,
                authority,
                executor,
                project_root: &config.project_root,
                permission_policy: config.permission_policy,
                cancelled: config.cancelled.as_ref(),
            };
            let final_response = match loop_runner.run(request) {
                Ok(response) => response,
                Err(error) => {
                    fail_provider_envelope(&config.project_root, &envelope, &error.to_string());
                    return Err(error);
                }
            };
            let result = json!({
                "content": final_response.content,
                "reasoning": final_response.reasoning,
                "rounds": final_response.rounds
            });
            domain.finish_call(
                &envelope.call_id,
                &envelope.attempt_id,
                envelope.generation,
                &result.to_string(),
            )?;
            Ok(json!({"result":result}))
        })
    }
}

fn fail_provider_envelope(root: &Path, envelope: &ExecutionEnvelope, message: &str) {
    let bounded = if message.len() > 4096 {
        &message[..message
            .char_indices()
            .take_while(|(index, _)| *index < 4096)
            .last()
            .map(|(index, character)| index + character.len_utf8())
            .unwrap_or(4096)]
    } else {
        message
    };
    let Ok(mut domain) = DomainRepository::open(root) else {
        return;
    };
    if domain
        .fail_call(
            &envelope.call_id,
            &envelope.attempt_id,
            envelope.generation,
            bounded,
        )
        .is_err()
    {
        let _ = domain.fence_dispatch_intent(&envelope.call_id, bounded);
    }
}

fn inject_native_tools(request: &mut Value) -> Result<()> {
    let object = request
        .as_object_mut()
        .ok_or_else(|| OcgError::config("provider request must be an object"))?;
    object.insert(
        "tools".to_string(),
        Value::Array(NativeToolRegistry::openai_tools()),
    );
    Ok(())
}

impl OpenAiCompatibleProvider for NativeOpenAiCompatibleProvider<'_> {
    fn complete(&self, request: &Value) -> Result<ProviderRound> {
        let body = if request.get("stream").is_none() {
            let mut body = request.clone();
            body["stream"] = Value::Bool(true);
            body
        } else {
            request.clone()
        };
        let bearer = self.bearer.as_deref().unwrap_or("");
        let mut headers = vec![("Content-Type", "application/json")];
        if !bearer.is_empty() {
            headers.push(("Authorization", bearer));
        }
        let response = self.transport.post_json(&self.endpoint, &headers, &body)?;
        if !response.is_success() {
            return Err(OcgError::config(format!(
                "OpenAI-compatible provider returned HTTP {}: {}",
                response.status,
                bounded_message(&response.body)
            )));
        }
        decode_provider_response(&response.body)
    }
}

pub struct ProviderToolLoop<'a, P> {
    pub provider: P,
    pub domain: &'a mut DomainRepository,
    pub authority: AttemptAuthority,
    pub executor: Executor,
    pub project_root: &'a Path,
    pub permission_policy: PermissionPolicy,
    pub cancelled: &'a AtomicBool,
}

impl<'a, P: OpenAiCompatibleProvider> ProviderToolLoop<'a, P> {
    /// Run until a normal assistant response is returned. Tool calls are
    /// executed in provider order, preserving ids and order in the next
    /// request. Each tool call gets its own canonical Call.
    pub fn run(&mut self, mut request: Value) -> Result<ProviderFinalResponse> {
        for round in 0..MAX_PROVIDER_ROUNDS {
            if self.cancelled.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(OcgError::config("provider tool loop cancelled"));
            }
            let provider_round = self.provider.complete(&request)?;
            if provider_round.summary.tool_calls.is_empty() {
                return Ok(ProviderFinalResponse {
                    content: provider_round.summary.text,
                    reasoning: provider_round.summary.reasoning,
                    rounds: round + 1,
                });
            }
            let calls = &provider_round.summary.tool_calls;
            request
                .get_mut("messages")
                .and_then(Value::as_array_mut)
                .ok_or_else(|| OcgError::config("provider request messages are missing"))?
                .push(provider_round.assistant.clone());
            for call in calls {
                let permission = tool_permission_for(&call.name).ok_or_else(|| {
                    OcgError::config(format!(
                        "provider requested unknown native tool '{}'",
                        call.name
                    ))
                })?;
                let arguments: Value = serde_json::from_str(&call.arguments).map_err(|error| {
                    OcgError::config(format!(
                        "tool call '{}' has invalid arguments: {error}",
                        call.name
                    ))
                })?;
                let result = execute_canonical_tool_call(
                    self.domain,
                    &self.authority,
                    &self.executor.id,
                    self.project_root,
                    NativeCallRequest {
                        tool_call_id: call.id.clone(),
                        name: call.name.clone(),
                        arguments,
                        permission,
                    },
                    self.permission_policy,
                    self.cancelled,
                )?;
                request
                    .get_mut("messages")
                    .and_then(Value::as_array_mut)
                    .ok_or_else(|| OcgError::config("provider request messages are missing"))?
                    .push(json!({
                        "role":"tool",
                        "tool_call_id":call.id,
                        "content":result.tool_message_content()
                    }));
            }
        }
        Err(OcgError::config(format!(
            "provider exceeded the {MAX_PROVIDER_ROUNDS}-round native tool loop limit"
        )))
    }
}

fn decode_provider_response(body: &[u8]) -> Result<ProviderRound> {
    let text = String::from_utf8_lossy(body);
    let mut summary = ChatStreamSummary::default();
    let mut normalizer = ToolCallNormalizer::default();
    let mut assistant_tool_calls = Vec::new();
    let mut assistant_content = String::new();
    let mut assistant_reasoning = String::new();
    if text.lines().any(|line| line.starts_with("data:")) {
        for line in text.lines().filter_map(|line| line.strip_prefix("data:")) {
            let payload = line.trim();
            if payload == "[DONE]" || payload.is_empty() {
                continue;
            }
            let value: Value = serde_json::from_str(payload)
                .map_err(|error| OcgError::config(format!("invalid provider SSE JSON: {error}")))?;
            fold_chunk(&mut summary, &mut normalizer, &value);
        }
        assistant_content.clone_from(&summary.text);
        assistant_reasoning.clone_from(&summary.reasoning);
        assistant_tool_calls.extend(summary.tool_calls.iter().map(|call| {
            json!({"id":call.id,"type":"function","function":{"name":call.name,"arguments":call.arguments}})
        }));
    } else {
        let value: Value = serde_json::from_slice(body).map_err(|error| {
            OcgError::config(format!("invalid provider JSON response: {error}"))
        })?;
        let choice = value
            .get("choices")
            .and_then(Value::as_array)
            .and_then(|choices| choices.first())
            .and_then(|choice| choice.get("message"))
            .ok_or_else(|| OcgError::config("provider response has no assistant message"))?;
        assistant_content = choice
            .get("content")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        assistant_reasoning = choice
            .get("reasoning_content")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if let Some(calls) = choice.get("tool_calls").and_then(Value::as_array) {
            for call in calls {
                let id = call.get("id").and_then(Value::as_str).unwrap_or("");
                let function = call.get("function").unwrap_or(&Value::Null);
                let name = function.get("name").and_then(Value::as_str).unwrap_or("");
                let arguments = function
                    .get("arguments")
                    .and_then(Value::as_str)
                    .unwrap_or("{}");
                let index = normalizer.index_for(id, Some(name));
                summary
                    .tool_calls
                    .push(crate::openai_compatible::CompletedToolCall {
                        index,
                        id: id.to_string(),
                        name: name.to_string(),
                        arguments: arguments.to_string(),
                    });
                assistant_tool_calls.push(call.clone());
            }
        }
        summary.text = assistant_content.clone();
        summary.reasoning = assistant_reasoning.clone();
        summary.finish_reason = value
            .get("choices")
            .and_then(Value::as_array)
            .and_then(|choices| choices.first())
            .and_then(|choice| choice.get("finish_reason"))
            .and_then(Value::as_str)
            .map(|reason| {
                if reason == "tool_calls" {
                    ChatFinishReason::ToolCalls
                } else {
                    ChatFinishReason::Stop
                }
            });
    }
    let assistant = json!({"role":"assistant","content":if assistant_content.is_empty(){Value::Null}else{Value::String(assistant_content)},"reasoning_content":if assistant_reasoning.is_empty(){Value::Null}else{Value::String(assistant_reasoning)},"tool_calls":if assistant_tool_calls.is_empty(){Value::Null}else{Value::Array(assistant_tool_calls)}});
    Ok(ProviderRound { assistant, summary })
}

fn fold_chunk(summary: &mut ChatStreamSummary, normalizer: &mut ToolCallNormalizer, chunk: &Value) {
    let choice = chunk
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|choices| choices.first());
    let Some(choice) = choice else { return };
    let delta = choice.get("delta").unwrap_or(&Value::Null);
    if let Some(text) = delta.get("content").and_then(Value::as_str) {
        summary.apply(&ChatStreamEvent::TextDelta {
            delta: text.to_string(),
        });
    }
    if let Some(text) = delta.get("reasoning_content").and_then(Value::as_str) {
        summary.apply(&ChatStreamEvent::ReasoningDelta {
            delta: text.to_string(),
        });
    }
    if let Some(calls) = delta.get("tool_calls").and_then(Value::as_array) {
        for call in calls {
            let id = call.get("id").and_then(Value::as_str).unwrap_or("");
            let index = call
                .get("index")
                .and_then(Value::as_u64)
                .map(|index| index as u32)
                .unwrap_or_else(|| normalizer.index_for(id, None));
            let function = call.get("function").unwrap_or(&Value::Null);
            let name = function.get("name").and_then(Value::as_str).unwrap_or("");
            let arguments = function
                .get("arguments")
                .and_then(Value::as_str)
                .unwrap_or("");
            let index = if id.is_empty() {
                index
            } else {
                normalizer.index_for(id, Some(name))
            };
            let existing = summary
                .tool_calls
                .iter()
                .find(|existing| existing.index == index)
                .cloned();
            if existing.is_none() {
                summary.apply(&ChatStreamEvent::ToolCallStart {
                    index,
                    id: id.to_string(),
                    name: name.to_string(),
                });
            } else if let Some(existing) = existing.as_ref() {
                if !name.is_empty() && existing.name.is_empty() {
                    summary.apply(&ChatStreamEvent::ToolCallComplete {
                        index,
                        id: if id.is_empty() {
                            existing.id.clone()
                        } else {
                            id.to_string()
                        },
                        name: name.to_string(),
                        arguments: existing.arguments.clone(),
                    });
                }
            }
            if !arguments.is_empty() {
                summary.apply(&ChatStreamEvent::ToolCallArgumentsDelta {
                    index,
                    id: id.to_string(),
                    delta: arguments.to_string(),
                });
            }
        }
    }
    if let Some(reason) = choice.get("finish_reason").and_then(Value::as_str) {
        let finish_reason = if reason == "tool_calls" {
            ChatFinishReason::ToolCalls
        } else {
            ChatFinishReason::Stop
        };
        summary.apply(&ChatStreamEvent::Finish {
            reason: finish_reason,
            raw_reason: Some(reason.to_string()),
            usage: NormalizedUsage::default(),
        });
    }
}

fn bounded_message(body: &[u8]) -> String {
    String::from_utf8_lossy(&body[..body.len().min(4096)]).into_owned()
}
