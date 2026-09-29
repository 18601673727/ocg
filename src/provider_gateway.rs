//! Invocation-scoped OpenAI Chat Completions gateway for explicitly migrated routes.
use crate::error::{OcgError, Result};
use crate::orchestration::budget::{BudgetConfig, QuotaFacts};
use crate::orchestration::domain::{DomainRepository, EffectIntentState};
use crate::orchestration::execution_dispatch::{
    BoundedDispatcher, CompioCallHandler, CompioExecutor, ExecutionEnvelope, ExecutionEvent,
};
use crate::provider_transport::{
    ChatStreamEvent, NormalizedUsage, ProviderTransport, ProviderTransportConfig,
};
use crate::runtime::compat::v2_client::{ServiceRegistration, V2SessionClient};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const MAX_HEADERS: usize = 16 * 1024;
const MAX_BODY: usize = 4 * 1024 * 1024;

#[derive(Clone)]
pub struct GatewayRoute {
    pub provider: String,
    pub model: String,
    pub upstream: ProviderTransportConfig,
}

pub struct ProviderGateway {
    _execution_lock: std::fs::File,
    listener: Option<JoinHandle<()>>,
    executor: Option<JoinHandle<()>>,
    dispatcher: BoundedDispatcher,
    stop: Arc<AtomicBool>,
    registration: Arc<Mutex<Option<ServiceRegistration>>>,
    url: String,
    token: String,
    invocation: String,
    provider_id: String,
}

struct GatewayContext {
    project: PathBuf,
    directory: String,
    registration: Arc<Mutex<Option<ServiceRegistration>>>,
    route: GatewayRoute,
    budget: BudgetConfig,
    token: String,
    invocation: String,
    dispatcher: BoundedDispatcher,
}

struct ProviderCallHandler {
    project: PathBuf,
    route: GatewayRoute,
}

impl CompioCallHandler for ProviderCallHandler {
    fn execute(
        &self,
        envelope: ExecutionEnvelope,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async move {
            let mut domain = DomainRepository::open(&self.project)?;
            let authority = crate::orchestration::domain::AttemptAuthority {
                attempt_id: envelope.attempt_id.clone(),
                job_id: envelope.job_id.clone(),
                generation: envelope.generation,
            };
            match domain.claim_call(
                &envelope.call_id,
                &authority.attempt_id,
                authority.generation,
            ) {
                Ok(true) => {}
                Ok(false) => {
                    if let Err(error) = envelope
                        .events
                        .send(ExecutionEvent::Failed("duplicate_delivery".into()))
                    {
                        tracing::debug!(%error, "duplicate delivery receiver disconnected");
                    }
                    return Ok(());
                }
                Err(error) => {
                    domain.fence_dispatch_intent(&envelope.call_id, "stale_attempt_authority")?;
                    if let Err(send_error) = envelope
                        .events
                        .send(ExecutionEvent::Failed(error.to_string()))
                    {
                        tracing::debug!(%send_error, "stale delivery receiver disconnected");
                    }
                    return Ok(());
                }
            }
            let input: Value = serde_json::from_str(&envelope.payload)
                .map_err(|error| OcgError::config(format!("invalid queued Call input: {error}")))?;
            let arguments = input
                .get("arguments")
                .cloned()
                .ok_or_else(|| OcgError::config("queued Call is missing arguments"))?;
            let transport = ProviderTransport::new(self.route.upstream.clone());
            let prepared = match transport.prepare_json(arguments) {
                Ok(prepared) => prepared,
                Err(error) => {
                    let reason = format!("provider_prepare: {error}");
                    let _ = domain.fail_call(
                        &envelope.call_id,
                        &authority.attempt_id,
                        authority.generation,
                        &reason,
                    );
                    let _ = domain.finish_dispatch_intent(
                        &envelope.call_id,
                        "failed",
                        EffectIntentState::NotStarted,
                        Some("provider_prepare"),
                    );
                    let _ = domain.settle_dispatch_budget(&envelope.call_id, "not_dispatched");
                    let _ = envelope.events.send(ExecutionEvent::Failed(reason));
                    return Ok(());
                }
            };
            let _ = envelope.events.send(ExecutionEvent::Started);
            let mut stream = match transport.stream(prepared).await {
                Ok(stream) => stream,
                Err(error) => {
                    let reason = format!("provider_start: {error}");
                    let _ = domain.fail_call(
                        &envelope.call_id,
                        &authority.attempt_id,
                        authority.generation,
                        &reason,
                    );
                    let _ = domain.finish_dispatch_intent(
                        &envelope.call_id,
                        "failed",
                        EffectIntentState::Unknown,
                        Some("provider_start"),
                    );
                    let _ = domain.settle_dispatch_budget(&envelope.call_id, "failed");
                    let _ = envelope.events.send(ExecutionEvent::Failed(reason));
                    return Ok(());
                }
            };
            let mut usage = None;
            let mut finished = false;
            let mut finish_event = None;
            while let Some(event) = futures::StreamExt::next(&mut stream).await {
                match event {
                    Ok(event @ crate::provider_transport::ChatStreamEvent::Finish { .. }) => {
                        if let crate::provider_transport::ChatStreamEvent::Finish {
                            usage: reported,
                            ..
                        } = &event
                        {
                            usage = Some(reported.clone());
                        }
                        finish_event = Some(event);
                        finished = true;
                    }
                    Ok(crate::provider_transport::ChatStreamEvent::Error(error)) => {
                        let reason = format!("provider_stream: {error}");
                        let _ = envelope.events.send(ExecutionEvent::Provider(
                            crate::provider_transport::ChatStreamEvent::Error(error),
                        ));
                        let _ = domain.fail_call(
                            &envelope.call_id,
                            &authority.attempt_id,
                            authority.generation,
                            &reason,
                        );
                        let _ = domain.finish_dispatch_intent(
                            &envelope.call_id,
                            "failed",
                            EffectIntentState::Unknown,
                            Some("provider_stream"),
                        );
                        let _ = domain.settle_dispatch_budget(&envelope.call_id, "failed");
                        let _ = envelope.events.send(ExecutionEvent::Failed(reason));
                        return Ok(());
                    }
                    Ok(other) => {
                        let _ = envelope.events.send(ExecutionEvent::Provider(other));
                    }
                    Err(error) => {
                        let reason = format!("provider_stream: {error}");
                        let _ = domain.fail_call(
                            &envelope.call_id,
                            &authority.attempt_id,
                            authority.generation,
                            &reason,
                        );
                        let _ = domain.finish_dispatch_intent(
                            &envelope.call_id,
                            "failed",
                            EffectIntentState::Unknown,
                            Some("provider_stream"),
                        );
                        let _ = domain.settle_dispatch_budget(&envelope.call_id, "failed");
                        let _ = envelope.events.send(ExecutionEvent::Failed(reason));
                        return Ok(());
                    }
                }
            }
            if finished {
                let response =
                    json!({"usage": usage.map(|value| value.raw), "provider": self.route.provider})
                        .to_string();
                if domain
                    .finish_call(
                        &envelope.call_id,
                        &authority.attempt_id,
                        authority.generation,
                        &response,
                    )
                    .is_ok()
                {
                    let _ = domain.finish_dispatch_intent(
                        &envelope.call_id,
                        "completed",
                        EffectIntentState::Settled,
                        None,
                    );
                    let _ = domain.settle_dispatch_budget(&envelope.call_id, "completed");
                    if let Some(event) = finish_event {
                        let _ = envelope.events.send(ExecutionEvent::Provider(event));
                    }
                    let _ = envelope.events.send(ExecutionEvent::Finished);
                } else {
                    let _ = domain.fence_dispatch_intent(&envelope.call_id, "late_result");
                    let _ = domain.settle_dispatch_budget(&envelope.call_id, "fenced");
                    let _ = envelope
                        .events
                        .send(ExecutionEvent::Failed("late_result".into()));
                }
            } else {
                let _ = domain.fail_call(
                    &envelope.call_id,
                    &authority.attempt_id,
                    authority.generation,
                    "provider_disconnect",
                );
                let _ = domain.finish_dispatch_intent(
                    &envelope.call_id,
                    "failed",
                    EffectIntentState::Unknown,
                    Some("provider_disconnect"),
                );
                let _ = domain.settle_dispatch_budget(&envelope.call_id, "failed");
                let _ = envelope
                    .events
                    .send(ExecutionEvent::Failed("provider_disconnect".into()));
            }
            Ok(())
        })
    }
}

impl ProviderGateway {
    pub fn start(
        project: PathBuf,
        directory: String,
        route: GatewayRoute,
        budget: BudgetConfig,
    ) -> Result<Self> {
        let mut domain = DomainRepository::open(&project)?;
        let execution_lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(crate::orchestration::state::state_dir(&project).join("provider-executor.lock"))
            .map_err(|error| OcgError::io("open provider executor ownership lock", error))?;
        fs2::FileExt::try_lock_exclusive(&execution_lock)
            .map_err(|error| OcgError::io("canonical provider executor is already owned", error))?;
        let recovered = domain.recover_provider_dispatches()?;
        let socket = TcpListener::bind("127.0.0.1:0")
            .map_err(|e| OcgError::io("cannot bind private provider gateway", e))?;
        socket
            .set_nonblocking(true)
            .map_err(|e| OcgError::io("cannot configure provider gateway", e))?;
        let url = format!(
            "http://{}/v1",
            socket
                .local_addr()
                .map_err(|e| OcgError::io("cannot identify provider gateway", e))?
        );
        // tempfile's random path suffix supplies invocation entropy without a
        // persisted token or an extra credential dependency.
        let entropy = tempfile::Builder::new()
            .prefix("gateway-")
            .tempfile()
            .map_err(|e| OcgError::io("cannot initialize gateway identity", e))?;
        let seed = format!(
            "{}:{}:{}",
            entropy.path().display(),
            std::process::id(),
            now()
        );
        let token = crate::runtime::hash::sha256_hex(seed.as_bytes());
        let invocation = token[..24].to_string();
        let registration = Arc::new(Mutex::new(None));
        let provider_id = route.provider.clone();
        let dispatcher = BoundedDispatcher::new(32)?;
        let worker_dispatcher = dispatcher.clone();
        let worker_project = project.clone();
        let worker_route = route.clone();
        let executor = std::thread::Builder::new()
            .name("ocg-compio-provider-executor".to_string())
            .spawn(move || {
                let handler = ProviderCallHandler {
                    project: worker_project,
                    route: worker_route,
                };
                if let Err(error) = CompioExecutor::run(&worker_dispatcher, &handler) {
                    tracing::error!(error = %error, "canonical provider executor stopped");
                }
            })
            .map_err(|e| OcgError::io("cannot start canonical provider executor", e))?;
        recover_dispatches(&project, &dispatcher, recovered, &route);
        let context = Arc::new(GatewayContext {
            project,
            directory,
            registration: registration.clone(),
            route,
            budget,
            token: token.clone(),
            invocation: invocation.clone(),
            dispatcher: dispatcher.clone(),
        });
        let stop = Arc::new(AtomicBool::new(false));
        let flag = stop.clone();
        let listener = std::thread::Builder::new()
            .name("ocg-provider-gateway".to_string())
            .spawn(move || {
                while !flag.load(Ordering::Relaxed) {
                    match socket.accept() {
                        Ok((stream, _)) => {
                            let ctx = context.clone();
                            let _ = std::thread::Builder::new()
                                .name("ocg-provider-request".into())
                                .spawn(move || handle(stream, &ctx));
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(Duration::from_millis(20))
                        }
                        Err(_) => break,
                    }
                }
            })
            .map_err(|e| OcgError::io("cannot start provider gateway", e))?;
        Ok(Self {
            _execution_lock: execution_lock,
            listener: Some(listener),
            executor: Some(executor),
            dispatcher,
            stop,
            registration,
            url,
            token,
            invocation,
            provider_id,
        })
    }

    pub fn attach_runtime(&self, registration: ServiceRegistration) -> Result<()> {
        let mut slot = self
            .registration
            .lock()
            .map_err(|_| OcgError::config("gateway runtime registration poisoned"))?;
        if slot.is_some() {
            return Err(OcgError::config("gateway runtime already attached"));
        }
        *slot = Some(registration);
        Ok(())
    }

    pub fn url(&self) -> &str {
        &self.url
    }
    pub fn token(&self) -> &str {
        &self.token
    }
    pub fn invocation(&self) -> &str {
        &self.invocation
    }
    pub fn provider_id(&self) -> &str {
        &self.provider_id
    }
}

impl Drop for ProviderGateway {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.listener.take() {
            let _ = handle.join();
        }
        let mut dispatcher = self.dispatcher.clone();
        let _ = dispatcher.close();
        if let Some(handle) = self.executor.take() {
            let _ = handle.join();
        }
    }
}

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

/// Recover only work whose external effect is known not to have started.
/// Queued work can be safely re-admitted; running work is fenced because a
/// process restart cannot prove whether the provider accepted the request.
fn recover_dispatches(
    project: &Path,
    dispatcher: &BoundedDispatcher,
    intents: Vec<crate::orchestration::domain::DispatchIntent>,
    route: &GatewayRoute,
) {
    let recover = || -> Result<()> {
        let mut domain = DomainRepository::open(project)?;
        for intent in intents {
            let input: Value = serde_json::from_str(&intent.request).map_err(|error| {
                OcgError::config(format!("invalid durable Call input: {error}"))
            })?;
            let provider = input.get("provider_id").and_then(Value::as_str);
            let model = input.get("model_id").and_then(Value::as_str);
            if provider.is_none() || model.is_none() {
                domain
                    .fence_dispatch_intent(&intent.call_id, "restart_missing_provider_binding")?;
                continue;
            }
            if provider != Some(route.provider.as_str()) || model != Some(route.model.as_str()) {
                continue;
            }
            let Some(authority) = domain.authority(&intent.attempt_id)? else {
                domain.fence_dispatch_intent(&intent.call_id, "restart_stale_authority")?;
                continue;
            };
            let call = domain.call(&intent.call_id)?;
            let receiver = crate::orchestration::execution_dispatch::queue_call(
                &mut domain,
                &call,
                &authority,
                &intent.request,
                Some(intent.id),
                dispatcher,
            )?;
            drop(receiver);
        }
        Ok(())
    };
    if let Err(error) = recover() {
        tracing::error!(%error, "canonical dispatch recovery failed; durable pending intents retained");
    }
}

fn respond(socket: &mut TcpStream, code: u16, message: &str) {
    let body = json!({"error":{"message":message,"type":"invalid_request_error"}}).to_string();
    let header = format!("HTTP/1.1 {code} Error\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len());
    let _ = socket.write_all(header.as_bytes());
    let _ = socket.write_all(body.as_bytes());
}

fn read_request(
    stream: &mut TcpStream,
) -> std::result::Result<(BTreeMap<String, String>, Value), &'static str> {
    stream
        .set_read_timeout(Some(Duration::from_secs(15)))
        .map_err(|_| "read timeout setup failed")?;
    let mut bytes = Vec::new();
    let mut chunk = [0; 4096];
    let end = loop {
        if bytes.len() > MAX_HEADERS {
            return Err("request headers exceed limit");
        }
        let count = stream.read(&mut chunk).map_err(|_| "request read failed")?;
        if count == 0 {
            return Err("request ended before headers");
        }
        bytes.extend_from_slice(&chunk[..count]);
        if let Some(i) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
            break i + 4;
        }
    };
    let header = std::str::from_utf8(&bytes[..end]).map_err(|_| "invalid request headers")?;
    let mut lines = header.split("\r\n");
    if lines.next() != Some("POST /v1/chat/completions HTTP/1.1") {
        return Err("only POST /v1/chat/completions is supported");
    }
    let mut headers = BTreeMap::new();
    for line in lines.filter(|l| !l.is_empty()) {
        let (name, value) = line.split_once(':').ok_or("malformed header")?;
        let key = name.trim().to_ascii_lowercase();
        if headers.insert(key, value.trim().to_string()).is_some() {
            return Err("duplicate header");
        }
    }
    if headers.contains_key("transfer-encoding") {
        return Err("chunked requests are unsupported");
    }
    let length: usize = headers
        .get("content-length")
        .ok_or("content-length required")?
        .parse()
        .map_err(|_| "invalid content-length")?;
    if length == 0 || length > MAX_BODY {
        return Err("request body exceeds limit");
    }
    while bytes.len() - end < length {
        let count = stream
            .read(&mut chunk)
            .map_err(|_| "request body read failed")?;
        if count == 0 {
            return Err("request body incomplete");
        }
        bytes.extend_from_slice(&chunk[..count]);
    }
    if bytes.len() - end != length {
        return Err("request body length mismatch");
    }
    let body = serde_json::from_slice(&bytes[end..]).map_err(|_| "invalid chat request JSON")?;
    Ok((headers, body))
}

fn handle(mut socket: TcpStream, ctx: &GatewayContext) {
    let (headers, body) = match read_request(&mut socket) {
        Ok(request) => request,
        Err(message) => {
            respond(&mut socket, 400, message);
            return;
        }
    };
    if headers.get("authorization").map(String::as_str)
        != Some(format!("Bearer {}", ctx.token).as_str())
        || headers.get("x-ocg-invocation").map(String::as_str) != Some(ctx.invocation.as_str())
    {
        respond(
            &mut socket,
            403,
            "provider gateway invocation is not authorized",
        );
        return;
    }
    let Some(session) = headers.get("x-ocg-session") else {
        respond(
            &mut socket,
            403,
            "provider request lacks execution correlation",
        );
        return;
    };
    let kind = headers
        .get("x-ocg-request-kind")
        .map(String::as_str)
        .unwrap_or("unknown");
    if !matches!(
        kind,
        "primary" | "title" | "summary" | "compaction" | "generate"
    ) {
        respond(&mut socket, 403, "unknown provider request ownership kind");
        return;
    }
    if body.get("model").and_then(Value::as_str) != Some(ctx.route.model.as_str()) {
        respond(
            &mut socket,
            400,
            "model is not configured for this migrated route",
        );
        return;
    }
    let call_request = json!({"arguments": body,"executor_transport":"provider","provider_id":ctx.route.provider,"model_id":ctx.route.model}).to_string();
    let registration = match ctx.registration.lock() {
        Ok(slot) => slot.clone(),
        Err(_) => None,
    };
    let Some(registration) = registration else {
        respond(&mut socket, 503, "gateway runtime not yet ready");
        return;
    };
    let _client = match V2SessionClient::connect(&registration, ctx.directory.clone()) {
        Ok(client) => client,
        Err(_) => {
            respond(&mut socket, 503, "runtime lineage unavailable");
            return;
        }
    };
    // Provider ownership is canonical SQLite state. Runtime/session data is
    // only used to establish the transport connection; it cannot authorize a
    // provider Call or revive a stale Attempt.
    let mut domain = match crate::orchestration::domain::DomainRepository::open(&ctx.project) {
        Ok(domain) => domain,
        Err(_) => {
            respond(
                &mut socket,
                503,
                "canonical execution authority unavailable",
            );
            return;
        }
    };
    let project = match domain.ensure_project(&ctx.project) {
        Ok(project) => project,
        Err(_) => {
            respond(&mut socket, 503, "canonical Project unavailable");
            return;
        }
    };
    let authority = match domain.authority_for_binding(&project.id, session) {
        Ok(Some(authority)) => authority,
        Ok(None) => {
            respond(
                &mut socket,
                403,
                "no current canonical Attempt owns execution",
            );
            return;
        }
        Err(_) => {
            respond(&mut socket, 503, "canonical Attempt lookup failed");
            return;
        }
    };
    let executor = match domain.executor_for_attempt(&authority.attempt_id) {
        Ok(Some(executor)) => executor,
        Ok(None) => {
            respond(&mut socket, 403, "canonical Attempt has no Executor");
            return;
        }
        Err(_) => {
            respond(&mut socket, 503, "canonical Executor lookup failed");
            return;
        }
    };
    if domain.mark_attempt_running(&authority).is_err() {
        respond(
            &mut socket,
            403,
            "canonical Attempt is no longer executable",
        );
        return;
    }
    let call = match domain.create_call_with_effect(
        &authority.attempt_id,
        Some(&executor.id),
        authority.generation,
        crate::orchestration::domain::EffectIntentKind::StrictFenced,
        &call_request,
    ) {
        Ok(call) => call,
        Err(_) => {
            respond(&mut socket, 403, "provider Call admission rejected");
            return;
        }
    };
    let quota = if ctx.budget.require_quota {
        crate::orchestration::budget::quota_facts(
            &ctx.project,
            &crate::resources::ResourceIdentity::for_model(&ctx.route.provider, &ctx.route.model)
                .with_runtime_family("opencode", "v2"),
            now(),
        )
    } else {
        QuotaFacts::unknown()
    };
    let assessment = match domain.admit_dispatch(
        &project.id,
        authority.generation,
        &call.id,
        &ctx.budget,
        quota,
    ) {
        Ok(result) => result,
        Err(_) => {
            let _ = domain.fail_call(
                &call.id,
                &authority.attempt_id,
                authority.generation,
                "dispatch_reservation",
            );
            respond(&mut socket, 503, "dispatch reservation failed");
            return;
        }
    };
    if !assessment.is_allowed() {
        let _ = domain.fail_call(
            &call.id,
            &authority.attempt_id,
            authority.generation,
            "economic_admission",
        );
        respond(
            &mut socket,
            403,
            "canonical economic admission blocked provider dispatch",
        );
        return;
    }
    if domain
        .attach_dispatch_reservation(&call.id, assessment.reservation_id.as_deref())
        .is_err()
    {
        let _ = domain.fail_call(
            &call.id,
            &authority.attempt_id,
            authority.generation,
            "dispatch_reservation_attach",
        );
        let _ = domain.settle_dispatch_budget(&call.id, "not_dispatched");
        respond(
            &mut socket,
            503,
            "canonical dispatch reservation could not be attached",
        );
        return;
    }
    let events = match crate::orchestration::execution_dispatch::queue_call(
        &mut domain,
        &call,
        &authority,
        &call_request,
        Some(call.id.clone()),
        &ctx.dispatcher,
    ) {
        Ok(events) => events,
        Err(_) => {
            let _ = domain.fail_call(
                &call.id,
                &authority.attempt_id,
                authority.generation,
                "bounded_dispatch",
            );
            let _ = domain.settle_dispatch_budget(&call.id, "not_dispatched");
            respond(&mut socket, 503, "canonical bounded dispatcher unavailable");
            return;
        }
    };
    if socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: close\r\n\r\n").is_err() {
        return;
    }
    let stream_id = format!("chatcmpl-{}", call.id);
    let _ = send(
        &mut socket,
        &stream_id,
        &ctx.route.model,
        json!({"role":"assistant"}),
        None,
        None,
    );
    let mut finished = false;
    let mut usage = None;
    while let Ok(event) = events.recv() {
        let result = match event {
            ExecutionEvent::Started => Ok(()),
            ExecutionEvent::Provider(event) => match event {
                ChatStreamEvent::TextDelta { delta } => send(
                    &mut socket,
                    &stream_id,
                    &ctx.route.model,
                    json!({"content":delta}),
                    None,
                    None,
                ),
                ChatStreamEvent::ReasoningDelta { delta } => send(
                    &mut socket,
                    &stream_id,
                    &ctx.route.model,
                    json!({"reasoning_content":delta}),
                    None,
                    None,
                ),
                ChatStreamEvent::ToolCallStart { index, id, name } => send(
                    &mut socket,
                    &stream_id,
                    &ctx.route.model,
                    json!({"tool_calls":[{"index":index,"id":id,"type":"function","function":{"name":name,"arguments":""}}]}),
                    None,
                    None,
                ),
                ChatStreamEvent::ToolCallArgumentsDelta { index, delta, .. } => send(
                    &mut socket,
                    &stream_id,
                    &ctx.route.model,
                    json!({"tool_calls":[{"index":index,"function":{"arguments":delta}}]}),
                    None,
                    None,
                ),
                ChatStreamEvent::Finish {
                    reason,
                    usage: reported,
                    ..
                } => {
                    usage = Some(reported);
                    finished = true;
                    send(
                        &mut socket,
                        &stream_id,
                        &ctx.route.model,
                        json!({}),
                        Some(reason.as_openai_str()),
                        None,
                    )
                }
                ChatStreamEvent::Error(_) => Err(std::io::Error::other("provider stream error")),
                ChatStreamEvent::Metadata { .. } | ChatStreamEvent::ToolCallComplete { .. } => {
                    Ok(())
                }
            },
            ExecutionEvent::Failed(_) => Err(std::io::Error::other("provider execution failed")),
            ExecutionEvent::Finished => {
                finished = true;
                Ok(())
            }
        };
        if result.is_err() {
            break;
        }
    }
    if finished {
        if let Some(tokens) = usage.as_ref() {
            let _ = send_usage(&mut socket, &stream_id, &ctx.route.model, tokens);
        }
        let _ = socket.write_all(b"data: [DONE]\n\n");
    }
}

fn send(
    socket: &mut TcpStream,
    id: &str,
    model: &str,
    delta: Value,
    finish: Option<&str>,
    usage: Option<Value>,
) -> std::io::Result<()> {
    let choices = if usage.is_some() {
        json!([])
    } else {
        json!([{"index":0,"delta":delta,"finish_reason":finish}])
    };
    let mut chunk = json!({"id":id,"object":"chat.completion.chunk","created":now(),"model":model,"choices":choices});
    if let Some(usage) = usage {
        chunk["usage"] = usage;
    }
    socket.write_all(format!("data: {chunk}\n\n").as_bytes())
}

fn send_usage(
    socket: &mut TcpStream,
    id: &str,
    model: &str,
    usage: &NormalizedUsage,
) -> std::io::Result<()> {
    let value = usage.raw.clone().unwrap_or_else(|| {
        json!({
            "prompt_tokens": usage.input_tokens, "completion_tokens": usage.output_tokens,
            "total_tokens": usage.total_tokens(),
        })
    });
    send(socket, id, model, json!({}), None, Some(value))
}
