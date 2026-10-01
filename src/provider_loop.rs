//! OCG-owned OpenAI-compatible provider loop.
//!
//! Provider wire handling ends at normalized assistant text/tool calls. Native
//! Tool authority stays in `native_tools`, and every tool invocation is
//! admitted and completed as a canonical Call before its tool message is sent
//! back to the provider.

use crate::call_recovery as recovery;
use crate::error::{OcgError, Result};
use crate::http::HttpTransport;
use crate::native_tools::{
    openai_projection::OpenAiToolProjection, PermissionPolicy,
};
use crate::openai_compatible::{
    ChatFinishReason, ChatStreamSummary, CompletedToolCall,
};
use crate::orchestration::budget::{BudgetConfig, QuotaFacts};
use crate::orchestration::domain::{
    AccountingAuthority, AttemptAuthority, DispatchAccounting, DomainRepository, EffectIntentKind, Executor,
};
use crate::orchestration::execution_dispatch::{
    admit_call, BoundedDispatcher, ExecutionEnvelope,
};
use serde_json::{json, Value};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub const MAX_PROVIDER_ROUNDS: usize = 32;

/// Events emitted during provider streaming.
#[derive(Debug, Clone)]
pub enum ProviderStreamEvent {
    /// Text delta from the assistant
    TextDelta(String),
    /// Reasoning content delta
    ReasoningDelta(String),
    /// A tool call has started (id and name known)
    ToolCallStart { id: String, name: String, index: u32 },
    /// Tool call arguments delta
    ToolCallArgumentsDelta { id: String, arguments: String, index: u32 },
    /// Finish reason received
    FinishReason(ChatFinishReason),
}

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

/// A provider client using the repository's native ntex HTTP surface.
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
        
        // Resolve provider configuration from envelope
        let provider_config = envelope.provider_config.as_ref().ok_or_else(|| {
            fail_provider_envelope(&config.project_root, &envelope, "provider Call missing provider_config");
            OcgError::config("provider Call missing provider_config")
        })?;
        
        // Resolve credential from the user-global Vault at execution time, the
        // same store `ocg auth` writes and the same one admission validated the
        // reference against. The project root is a directory and is not a
        // credential store.
        let vault = crate::vault::Vault::user_global()?;
        let bearer = vault.get(&provider_config.credential_ref)?.ok_or_else(|| {
            fail_provider_envelope(&config.project_root, &envelope, "credential not found in Vault");
            OcgError::config(format!("credential not found: {}", provider_config.credential_ref))
        })?;
        
        drop(domain);
        let provider = NativeOpenAiCompatibleProvider::new(
            config.transport.as_ref(),
            provider_config.endpoint.clone(),
            Some(bearer),
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
    provider_config: crate::orchestration::execution_dispatch::ProviderExecutionConfig,
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

    // Freeze provider execution configuration for this Call
    domain.set_provider_config(
        &call.id,
        &provider_config.provider_key,
        &provider_config.model,
        &provider_config.endpoint,
        &provider_config.credential_ref,
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
        provider_config: Some(provider_config),
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
        .ok_or_else(|| {
            OcgError::config("recovered provider Call has stale Attempt authority")
        })?;

    // Requeue using existing Call identity
    let provider_config = if let (Some(pk), Some(m), Some(ep), Some(cr)) = (
        intent.provider_key.as_ref(),
        intent.model.as_ref(),
        intent.endpoint.as_ref(),
        intent.credential_ref.as_ref(),
    ) {
        Some(crate::orchestration::execution_dispatch::ProviderExecutionConfig {
            provider_key: pk.clone(),
            model: m.clone(),
            endpoint: ep.clone(),
            credential_ref: cr.clone(),
        })
    } else {
        None
    };
    
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
pub fn run_provider_worker(dispatcher: &BoundedDispatcher, handler: &CanonicalProviderCallHandler) -> Result<()> {
    loop {
        let Some(envelope) = dispatcher.recv()? else {
            return Ok(());
        };
        if let Err(error) = execute_provider_envelope_sync(handler, envelope) {
            tracing::error!(error = %error, "canonical provider Call execution failed; continuing with next bounded item");
        }
    }
}

/// Execute one provider envelope synchronously by creating a runtime per request.
/// This matches the pattern used in http.rs for native HTTP transport.
fn execute_provider_envelope_sync(handler: &CanonicalProviderCallHandler, envelope: ExecutionEnvelope) -> Result<()> {
    let handler = handler.clone();
    let runtime = ntex::rt::System::new("ocg-provider", ntex::rt::DefaultRuntime);
    runtime.block_on(async move {
        let input: serde_json::Value = serde_json::from_str(&envelope.payload)
            .map_err(|error| OcgError::config(format!("invalid Call input JSON: {error}")))?;
        crate::orchestration::call_schema::validate_input(&input)?;
        let output = handler.execute_validated(envelope).await?;
        crate::orchestration::call_schema::validate_output(&output)?;
        Ok(())
    })
}

/// Run the native tool worker loop synchronously without an async runtime.
pub fn run_native_tool_worker(dispatcher: &BoundedDispatcher, handler: &crate::native_tools::NativeToolCallHandler) -> Result<()> {
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
fn execute_native_tool_envelope_sync(handler: &crate::native_tools::NativeToolCallHandler, envelope: ExecutionEnvelope) -> Result<()> {
    let input: serde_json::Value = serde_json::from_str(&envelope.payload)
        .map_err(|error| OcgError::config(format!("invalid Call input JSON: {error}")))?;
    crate::orchestration::call_schema::validate_input(&input)?;
    let output = handler.execute_validated_sync(envelope)?;
    crate::orchestration::call_schema::validate_output(&output)?;
    Ok(())
}

fn execute_provider_loop(
    provider: &dyn OpenAiCompatibleProvider,
    project_root: &Path,
    envelope: &ExecutionEnvelope,
    authority: &AttemptAuthority,
    executor: &Executor,
    request: &mut Value,
    _permission_policy: PermissionPolicy,
    cancelled: &AtomicBool,
    native_tool_dispatcher: &BoundedDispatcher,
) -> Result<ProviderFinalResponse> {
    let projection = OpenAiToolProjection::from_registry()?;
    let model = envelope
        .provider_config
        .as_ref()
        .map(|config| config.model.clone())
        .unwrap_or_default();

    // Inject native tools into request
    let object = request
        .as_object_mut()
        .ok_or_else(|| OcgError::config("provider request must be an object"))?;
    object.insert("tools".to_string(), Value::Array(projection.tools()));

    // Assemble the active context once, before any round: the structured
    // projection and the compacted conversation together form the request this
    // provider Call carries. Every later round appends to that same request
    // rather than re-deciding what context it has.
    apply_active_context(request, project_root, &authority.attempt_id, provider, &model)?;
    
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
            // A tool call is admitted, executed and completed as a canonical
            // Call. That authority is unchanged here; what changes is what
            // happens when the Runtime can already tell the Call is invalid.
            // Recovery decides, and only a Call that genuinely needs new
            // reasoning is reported as such. See `crate::call_recovery`.
            match resolve_call(project_root, authority, call, &projection)? {
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
                        &executor.id,
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
    projection: &OpenAiToolProjection,
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
        return Ok(ResolvedCall::Rejected(failure.compact_observation(&reference)));
    };

    // The provider was offered the OpenAI wire name, and a strict-schema wire
    // shape, not the canonical schema. Undoing that widening happens before
    // preflight so the failure the model sees names canonical fields. Strict
    // validation still runs first: normalization on its own would hide a wire
    // violation, because dropping a null optional field makes the canonical form
    // valid.
    let canonical =
        match normalize_wire_arguments(projection, &call.name, &definition, &wire_arguments) {
            Ok(canonical) => canonical,
            Err(message) => {
                // The validator's own error text interpolates the offending
                // value, so it is never forwarded to the model: a wrongly-typed
                // `oldString` would otherwise drag its own body back into the
                // context that already holds it. The classified observation
                // replaces it, and the validator text stays in the trace.
                let failure = recovery::CallFailure::InvalidArguments(
                    wire_failure(&definition, &wire_arguments),
                );
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
            let permission = crate::native_tools::tool_permission_for(definition.name).ok_or_else(
                || {
                    OcgError::config(format!("unknown tool permission: {}", definition.name))
                },
            )?;
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
        recovery::RecoveryAction::NeedsReasoning(failure) => {
            Ok(ResolvedCall::Rejected(failure.compact_observation(&reference)))
        }
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
    let Some(declared) = definition.parameters.get("properties").and_then(Value::as_object) else {
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
fn apply_active_context(
    request: &mut Value,
    project_root: &Path,
    attempt_id: &str,
    provider: &dyn OpenAiCompatibleProvider,
    model: &str,
) -> Result<()> {
    let conversation = request
        .get("messages")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let task = objective_of(&conversation);
    let summarize = |prompt: &str, max_tokens: u64| -> Result<String> {
        let summary_request = json!({
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": max_tokens,
            "stream": false,
        });
        Ok(provider.complete(&summary_request)?.summary.text)
    };
    let assembled = match crate::provider_context::assemble(
        project_root,
        crate::provider_context::ContextInputs {
            task: &task,
            attempt_id,
            tools: request.get("tools"),
            conversation: conversation.clone(),
            summarize: Some(&summarize),
        },
    ) {
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
/// The objective is the opening user message, which is where
/// `canonical_control` puts it. Only that message is read: the rest of the
/// conversation is work in progress, not a description of the task.
fn objective_of(conversation: &[Value]) -> String {
    conversation
        .iter()
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

/// Stateful SSE chunk parser that handles cross-chunk boundaries.
struct SseChunkParser {
    buffer: String,
}

impl SseChunkParser {
    fn new() -> Self {
        Self {
            buffer: String::new(),
        }
    }

    /// Parse a chunk and invoke callback for each complete SSE event.
    /// Handles partial UTF-8, partial SSE lines, and partial JSON.
    fn parse_chunk<F>(&mut self, chunk: &[u8], callback: &mut F) -> Result<()>
    where
        F: FnMut(ProviderStreamEvent) -> Result<()>,
    {
        // Append chunk to buffer (may contain partial UTF-8)
        match std::str::from_utf8(chunk) {
            Ok(text) => self.buffer.push_str(text),
            Err(error) => {
                // Partial UTF-8 at end of chunk - buffer it
                let valid_up_to = error.valid_up_to();
                if valid_up_to > 0 {
                    self.buffer.push_str(&String::from_utf8_lossy(&chunk[..valid_up_to]));
                }
                // The rest will be completed in the next chunk
                return Ok(());
            }
        }

        // Process complete lines
        while let Some(newline_pos) = self.buffer.find('\n') {
            let line = self.buffer[..newline_pos].trim_end_matches('\r').to_string();
            self.buffer.drain(..=newline_pos);

            if let Some(data) = line.strip_prefix("data:") {
                let payload = data.trim();
                if payload == "[DONE]" || payload.is_empty() {
                    continue;
                }

                // Parse JSON and emit events
                match serde_json::from_str::<Value>(payload) {
                    Ok(value) => {
                        self.parse_sse_event(&value, callback)?;
                    }
                    Err(_) => {
                        // Incomplete JSON - put the line back and wait for more data
                        self.buffer.insert_str(0, &format!("{}\n", line));
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    fn parse_sse_event<F>(&self, value: &Value, callback: &mut F) -> Result<()>
    where
        F: FnMut(ProviderStreamEvent) -> Result<()>,
    {
        if let Some(choices) = value.get("choices").and_then(Value::as_array) {
            if let Some(first) = choices.first() {
                if let Some(delta) = first.get("delta") {
                    // Text content delta
                    if let Some(content) = delta.get("content").and_then(Value::as_str) {
                        if !content.is_empty() {
                            callback(ProviderStreamEvent::TextDelta(content.to_string()))?;
                        }
                    }

                    // Reasoning content delta
                    if let Some(reasoning) = delta.get("reasoning_content").and_then(Value::as_str) {
                        if !reasoning.is_empty() {
                            callback(ProviderStreamEvent::ReasoningDelta(reasoning.to_string()))?;
                        }
                    }

                    // Tool calls
                    if let Some(tool_calls) = delta.get("tool_calls").and_then(Value::as_array) {
                        for call in tool_calls {
                            let index = call.get("index").and_then(Value::as_u64).unwrap_or(0) as u32;

                            if let Some(function) = call.get("function") {
                                // Tool call start (id and name present)
                                if let (Some(id), Some(name)) = (
                                    call.get("id").and_then(Value::as_str),
                                    function.get("name").and_then(Value::as_str),
                                ) {
                                    callback(ProviderStreamEvent::ToolCallStart {
                                        id: id.to_string(),
                                        name: name.to_string(),
                                        index,
                                    })?;
                                }

                                // Tool call arguments delta
                                if let (Some(id), Some(arguments)) = (
                                    call.get("id").and_then(Value::as_str),
                                    function.get("arguments").and_then(Value::as_str),
                                ) {
                                    if !arguments.is_empty() {
                                        callback(ProviderStreamEvent::ToolCallArgumentsDelta {
                                            id: id.to_string(),
                                            arguments: arguments.to_string(),
                                            index,
                                        })?;
                                    }
                                }
                            }
                        }
                    }
                }

                // Finish reason
                if let Some(finish_reason) = first.get("finish_reason").and_then(Value::as_str) {
                    let reason = match finish_reason {
                        "stop" => ChatFinishReason::Stop,
                        "length" => ChatFinishReason::Length,
                        "tool_calls" => ChatFinishReason::ToolCalls,
                        "content_filter" => ChatFinishReason::ContentFilter,
                        "error" => ChatFinishReason::Error,
                        _ => ChatFinishReason::Other,
                    };
                    callback(ProviderStreamEvent::FinishReason(reason))?;
                }
            }
        }

        Ok(())
    }
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
