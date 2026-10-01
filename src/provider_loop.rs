//! OCG-owned OpenAI-compatible provider loop.
//!
//! Provider wire handling ends at normalized assistant text/tool calls. Native
//! Tool authority stays in `native_tools`, and every tool invocation is
//! admitted and completed as a canonical Call before its tool message is sent
//! back to the provider.

use crate::error::{OcgError, Result};
use crate::http::HttpTransport;
use crate::native_tools::{
    openai_projection::OpenAiToolProjection, tool_permission_for, PermissionPolicy,
};
use crate::openai_compatible::{
    ChatFinishReason, ChatStreamSummary, CompletedToolCall,
};
use crate::orchestration::budget::{BudgetConfig, QuotaFacts};
use crate::orchestration::domain::{
    AccountingAuthority, AttemptAuthority, DispatchAccounting, DomainRepository, EffectIntentKind, Executor,
};
use crate::orchestration::execution_dispatch::{
    admit_call, BoundedDispatcher, CompioExecutor, ExecutionEnvelope,
    ValidatedCompioCallHandler,
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
    pub native_tool_dispatcher: BoundedDispatcher,
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

/// Admit one provider request through canonical economic admission before
/// placing it on the bounded dispatcher. The returned Call is the durable owner
/// of the provider request; a queue item alone never authorizes execution.
///
/// Economic admission must succeed before the Call enters the bounded queue.
/// If queue handoff fails after successful admission, the reservation is
/// released as NotDispatched.
pub fn admit_provider_call(
    domain: &mut DomainRepository,
    authority: &AttemptAuthority,
    executor_id: &str,
    request: Value,
    config: &BudgetConfig,
    quota: QuotaFacts,
    dispatcher: &BoundedDispatcher,
) -> Result<crate::orchestration::domain::Call> {
    let payload = json!({
        "executor_transport": "provider",
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

    // Resolve canonical Project identity from Job ownership
    let job = domain
        .job(&authority.job_id)?
        .ok_or_else(|| OcgError::config("provider Call Job no longer exists"))?;
    let project_id = &job.project_id;

    // Execute true economic admission through canonical DomainRepository API
    let assessment = domain.admit_dispatch(
        project_id,
        authority.generation,
        &call.id,
        config,
        quota,
    )?;

    // Fail closed: denied admission terminates the Call before queue/execution
    if !assessment.is_allowed() {
        let failure = format!("economic_admission_denied: {}", assessment.reason_code);
        domain.fail_call(&call.id, &authority.attempt_id, authority.generation, &failure)?;
        return Err(OcgError::config(format!(
            "provider Call denied by economic admission: {}",
            assessment.reason
        )));
    }

    // Economic admission succeeded; now queue
    domain.mark_dispatch_queued(&call.id)?;
    let (events, _receiver) = flume::unbounded();
    if let Err(error) = dispatcher.send(ExecutionEnvelope {
        call_id: call.id.clone(),
        job_id: authority.job_id.clone(),
        attempt_id: authority.attempt_id.clone(),
        executor_id: Some(executor_id.to_string()),
        generation: authority.generation,
        payload: payload.to_string(),
        dispatch_id: None,
        events,
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
        let _ = domain.settle_dispatch_accounting(
            &call.id,
            &claim,
            &DispatchAccounting::NotDispatched,
        );
        return Err(error);
    }

    Ok(call)
}

/// Requeue one recovered provider DispatchIntent back to the bounded
/// dispatcher. The Call and DispatchIntent already exist; this only
/// redelivers the execution envelope.
fn requeue_recovered_provider_call(
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
        .ok_or_else(|| {
            OcgError::config("recovered provider Call has stale Attempt authority")
        })?;

    // Requeue using existing Call identity
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
    })?;

    Ok(())
}

/// Start the complete execution runtime with provider and native tool dispatchers.
/// This creates two bounded dispatchers under the same execution authority,
/// starts both consumers, and handles recovery.
pub fn run_execution_runtime(
    project_root: &Path,
    provider_capacity: usize,
    native_tool_capacity: usize,
    transport: Arc<dyn HttpTransport>,
    endpoint: String,
    bearer: Option<String>,
    permission_policy: PermissionPolicy,
    cancelled: Arc<AtomicBool>,
) -> Result<()> {
    let provider_dispatcher = BoundedDispatcher::new(provider_capacity)?;
    let native_tool_dispatcher = BoundedDispatcher::new(native_tool_capacity)?;

    let provider_config = ProviderHandlerConfig {
        transport,
        endpoint,
        bearer,
        project_root: project_root.to_path_buf(),
        permission_policy,
        cancelled: cancelled.clone(),
        native_tool_dispatcher: native_tool_dispatcher.clone(),
    };

    let native_tool_handler = crate::native_tools::NativeToolCallHandler::new(
        project_root.to_path_buf(),
        permission_policy,
        cancelled,
    );

    // Start native tool consumer in separate thread
    let native_tool_dispatcher_clone = native_tool_dispatcher.clone();
    let native_tool_thread = std::thread::spawn(move || {
        CompioExecutor::run(&native_tool_dispatcher_clone, &native_tool_handler)
    });

    // Run provider dispatcher in main thread (with recovery)
    let result = run_provider_dispatcher(project_root, &provider_dispatcher, provider_config);

    // Shutdown dispatcher to unblock native tool consumer
    provider_dispatcher.shutdown();
    native_tool_dispatcher.shutdown();

    // Wait for native tool thread and propagate any panic
    let native_result = native_tool_thread.join();
    if let Err(panic) = native_result {
        std::panic::resume_unwind(panic);
    }

    result
}

/// Run provider envelopes through the existing bounded Compio executor. This
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
            CompioExecutor::run(&dispatcher_clone, &handler)
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

            // Verify economic authority before execution
            let intent = domain.dispatch_intent(&envelope.call_id)?
                .ok_or_else(|| {
                    fail_provider_envelope(
                        &config.project_root,
                        &envelope,
                        "provider Call has no durable dispatch intent",
                    );
                    OcgError::config("provider Call has no durable dispatch intent")
                })?;
            if !intent.budget_admitted {
                fail_provider_envelope(
                    &config.project_root,
                    &envelope,
                    "provider Call has no economic admission",
                );
                return Err(OcgError::config("provider Call lacks economic admission authority"));
            }

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
                    let message = "provider Call payload missing 'arguments'";
                    fail_provider_envelope(&config.project_root, &envelope, message);
                    return Err(OcgError::config(message));
                }
            };
            let executor = domain.executor(envelope.executor_id.as_deref().unwrap_or(""))?
                .ok_or_else(|| OcgError::config("provider Call executor not found"))?;
            drop(domain);
            let provider = NativeOpenAiCompatibleProvider::new(
                config.transport.as_ref(),
                config.endpoint.clone(),
                config.bearer.clone(),
            );
            let response = execute_provider_loop(
                &provider,
                &config.project_root,
                &envelope,
                &authority,
                &executor,
                &mut request,
                config.permission_policy,
                &config.cancelled,
                &config.native_tool_dispatcher,
            )?;
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
            Ok(json!({"content": response.content, "reasoning": response.reasoning, "rounds": response.rounds}))
        })
    }
}

fn execute_provider_loop(
    provider: &dyn OpenAiCompatibleProvider,
    project_root: &Path,
    _envelope: &ExecutionEnvelope,
    authority: &AttemptAuthority,
    executor: &Executor,
    request: &mut Value,
    _permission_policy: PermissionPolicy,
    cancelled: &AtomicBool,
    native_tool_dispatcher: &BoundedDispatcher,
) -> Result<ProviderFinalResponse> {
    let projection = OpenAiToolProjection::from_registry()?;

    // Inject native tools into request
    let object = request
        .as_object_mut()
        .ok_or_else(|| OcgError::config("provider request must be an object"))?;
    object.insert("tools".to_string(), Value::Array(projection.tools()));
    for round in 0..MAX_PROVIDER_ROUNDS {
        if cancelled.load(Ordering::SeqCst) {
            return Err(OcgError::config("provider loop cancelled"));
        }
        if let Some(tools) = request.get("tools").and_then(Value::as_array) {
            if tools.is_empty() {
                request.as_object_mut().and_then(|obj| obj.remove("tools"));
            }
        }
        let round_response = provider.complete(request)?;
        if round_response.summary.finish_reason == Some(ChatFinishReason::Stop)
            || round_response.summary.tool_calls.is_empty()
        {
            return Ok(ProviderFinalResponse {
                content: round_response.summary.text,
                reasoning: round_response.summary.reasoning,
                rounds: round + 1,
            });
        }
        if round_response.summary.finish_reason == Some(ChatFinishReason::Length) {
            return Err(OcgError::config("provider exceeded token limit"));
        }
        request
            .get_mut("messages")
            .and_then(Value::as_array_mut)
            .ok_or_else(|| OcgError::config("provider request messages are missing"))?
            .push(round_response.assistant);
        for call in &round_response.summary.tool_calls {
            let tool = projection.resolve(&call.name)?;
            let canonical_name = tool.canonical_name();
            let wire_arguments: Value = serde_json::from_str(&call.arguments)
                .map_err(|error| OcgError::config(format!("invalid tool arguments JSON: {error}")))?;
            let permission = tool_permission_for(canonical_name)
                .ok_or_else(|| OcgError::config(format!("unknown tool permission: {canonical_name}")))?;
            tool.validate_wire_arguments(&wire_arguments)?;
            let arguments = tool.canonical_arguments(&wire_arguments);

            // Admit and queue Native Tool Call to separate bounded dispatcher
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
                &executor.id,
                side_effect,
                &payload.to_string(),
                native_tool_dispatcher,
            )?;

            drop(domain);

            // Wait for child Call completion by polling (blocking is acceptable
            // here because provider and native tool use separate dispatchers)
            let result = wait_for_call_completion(project_root, &tool_call.id)?;

            request
                .get_mut("messages")
                .and_then(Value::as_array_mut)
                .ok_or_else(|| OcgError::config("provider request messages are missing"))?
                .push(json!({
                    "role": "tool",
                    "tool_call_id": call.id,
                    "content": result
                }));
        }
    }
    Err(OcgError::config(format!(
        "provider exceeded the {MAX_PROVIDER_ROUNDS}-round native tool loop limit"
    )))
}

/// Poll for Call completion. This blocks the provider handler but does not
/// block the native tool consumer since they use separate dispatchers.
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
                return Err(OcgError::config(format!(
                    "native tool Call failed: {}",
                    call.response.unwrap_or_else(|| "unknown error".to_string())
                )))
            }
            "fenced" => {
                return Err(OcgError::config("native tool Call fenced"))
            }
            _ => continue,
        }
    }
    Err(OcgError::config("native tool Call timeout"))
}

fn decode_provider_response(body: &[u8]) -> Result<ProviderRound> {
    let text = String::from_utf8_lossy(body);
    let mut summary = ChatStreamSummary::default();
    let mut assistant_tool_calls = Vec::new();
    let mut assistant_content = String::new();
    let mut assistant_reasoning = String::new();
    if text.lines().any(|line| line.starts_with("data:")) {
        // SSE format - parse manually
        for line in text.lines().filter_map(|line| line.strip_prefix("data:")) {
            let payload = line.trim();
            if payload == "[DONE]" || payload.is_empty() {
                continue;
            }
            let value: Value = serde_json::from_str(payload)
                .map_err(|error| OcgError::config(format!("invalid provider SSE JSON: {error}")))?;

            // Manually extract content and tool calls from delta
            if let Some(choices) = value.get("choices").and_then(Value::as_array) {
                if let Some(first) = choices.first() {
                    if let Some(delta) = first.get("delta") {
                        if let Some(content) = delta.get("content").and_then(Value::as_str) {
                            assistant_content.push_str(content);
                            summary.text.push_str(content);
                        }
                        if let Some(reasoning) = delta.get("reasoning_content").and_then(Value::as_str) {
                            assistant_reasoning.push_str(reasoning);
                            summary.reasoning.push_str(reasoning);
                        }
                        if let Some(tool_calls) = delta.get("tool_calls").and_then(Value::as_array) {
                            for call in tool_calls {
                                if let Some(function) = call.get("function") {
                                    if let (Some(id), Some(name)) = (
                                        call.get("id").and_then(Value::as_str),
                                        function.get("name").and_then(Value::as_str),
                                    ) {
                                        // Check if this tool call already exists
                                        if !summary.tool_calls.iter().any(|tc| tc.id == id) {
                                            summary.tool_calls.push(CompletedToolCall {
                                                index: summary.tool_calls.len() as u32,
                                                id: id.to_string(),
                                                name: name.to_string(),
                                                arguments: String::new(),
                                            });
                                        }
                                    }
                                    if let (Some(id), Some(arguments)) = (
                                        call.get("id").and_then(Value::as_str),
                                        function.get("arguments").and_then(Value::as_str),
                                    ) {
                                        if let Some(tc) = summary.tool_calls.iter_mut().find(|tc| tc.id == id) {
                                            tc.arguments.push_str(arguments);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if let Some(finish_reason) = first.get("finish_reason").and_then(Value::as_str) {
                        summary.finish_reason = match finish_reason {
                            "stop" => Some(ChatFinishReason::Stop),
                            "length" => Some(ChatFinishReason::Length),
                            "tool_calls" => Some(ChatFinishReason::ToolCalls),
                            _ => Some(ChatFinishReason::Stop),
                        };
                    }
                }
            }
        }
        assistant_tool_calls.extend(summary.tool_calls.iter().map(|call| {
            json!({"id":call.id,"type":"function","function":{"name":call.name,"arguments":call.arguments}})
        }));
    } else {
        let value: Value = serde_json::from_slice(body).map_err(|error| {
            OcgError::config(format!("invalid provider non-SSE JSON: {error}"))
        })?;
        if let Some(choices) = value.get("choices").and_then(Value::as_array) {
            if let Some(first) = choices.first() {
                if let Some(message) = first.get("message") {
                    if let Some(content) = message.get("content").and_then(Value::as_str) {
                        assistant_content = content.to_string();
                    }
                    if let Some(reasoning_content) = message
                        .get("reasoning_content")
                        .and_then(Value::as_str)
                    {
                        assistant_reasoning = reasoning_content.to_string();
                    }
                    if let Some(tool_calls) = message.get("tool_calls").and_then(Value::as_array) {
                        for call in tool_calls {
                            if let (Some(id), Some(function)) = (
                                call.get("id").and_then(Value::as_str),
                                call.get("function"),
                            ) {
                                if let (Some(name), Some(arguments)) = (
                                    function.get("name").and_then(Value::as_str),
                                    function.get("arguments").and_then(Value::as_str),
                                ) {
                                    summary.tool_calls.push(
                                        crate::openai_compatible::stream::CompletedToolCall {
                                            index: 0,
                                            id: id.to_string(),
                                            name: name.to_string(),
                                            arguments: arguments.to_string(),
                                        },
                                    );
                                    assistant_tool_calls.push(call.clone());
                                }
                            }
                        }
                    }
                }
                if let Some(finish_reason) = first
                    .get("finish_reason")
                    .and_then(Value::as_str)
                {
                    summary.finish_reason = match finish_reason {
                        "stop" => Some(ChatFinishReason::Stop),
                        "length" => Some(ChatFinishReason::Length),
                        "tool_calls" => Some(ChatFinishReason::ToolCalls),
                        _ => Some(ChatFinishReason::Stop),
                    };
                }
            }
        }
        summary.text = assistant_content.clone();
        summary.reasoning = assistant_reasoning.clone();
    }
    let assistant = if !assistant_tool_calls.is_empty() {
        let mut message = json!({"role":"assistant","content":assistant_content,"tool_calls":assistant_tool_calls});
        if !assistant_reasoning.is_empty() {
            message["reasoning_content"] = json!(assistant_reasoning);
        }
        message
    } else {
        let mut message = json!({"role":"assistant","content":assistant_content});
        if !assistant_reasoning.is_empty() {
            message["reasoning_content"] = json!(assistant_reasoning);
        }
        message
    };
    Ok(ProviderRound { assistant, summary })
}

impl OpenAiCompatibleProvider for NativeOpenAiCompatibleProvider<'_> {
    fn complete(&self, request: &Value) -> Result<ProviderRound> {
        // Inject stream: true if not already present
        let body = if request.get("stream").is_none() {
            let mut body = request.clone();
            body["stream"] = Value::Bool(true);
            body
        } else {
            request.clone()
        };

        let accept = if body.get("stream").and_then(Value::as_bool) == Some(true) {
            "text/event-stream"
        } else {
            "application/json"
        };
        let mut headers = vec![("Content-Type", "application/json"), ("Accept", accept)];

        // Vault stores the raw token; the header value is built only here.
        let authorization = self.bearer.as_ref().map(|token| format!("Bearer {token}"));
        if let Some(value) = &authorization {
            headers.push(("Authorization", value.as_str()));
        }

        let response = self.transport.post_json(&self.endpoint, &headers, &body)?;

        if !response.is_success() {
            const MAX_ERROR_BODY: usize = 2048;
            let excerpt = &response.body[..response.body.len().min(MAX_ERROR_BODY)];
            return Err(OcgError::config(format!(
                "OpenAI-compatible provider returned HTTP {}: {}",
                response.status,
                String::from_utf8_lossy(excerpt)
            )));
        }

        decode_provider_response(&response.body)
    }
}

fn fail_provider_envelope(project_root: &Path, envelope: &ExecutionEnvelope, reason: &str) {
    let _ = (|| -> Result<()> {
        let mut domain = DomainRepository::open(project_root)?;
        domain.fail_call(
            &envelope.call_id,
            &envelope.attempt_id,
            envelope.generation,
            reason,
        )?;
        Ok(())
    })();
}
