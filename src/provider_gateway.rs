//! Invocation-scoped OpenAI Chat Completions gateway for explicitly migrated routes.
use crate::error::{OcgError, Result};
use crate::orchestration::budget::{BudgetConfig, QuotaFacts};
use crate::orchestration::dispatch::{DispatchId, DispatchRecord, DispatchState, DispatchUsage};
use crate::orchestration::mission;
use crate::orchestration::replay::SnapshotService;
use crate::provider_transport::{
    ChatStreamEvent, NormalizedUsage, ProviderTransport, ProviderTransportConfig,
};
use crate::runtime::compat::v2_client::{ServiceRegistration, V2SessionClient};
use crate::runtime::lifecycle::RuntimeExecutionId;
use futures::StreamExt;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Mutex,
};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const MAX_HEADERS: usize = 16 * 1024;
const MAX_BODY: usize = 4 * 1024 * 1024;
static NEXT_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Clone)]
pub struct GatewayRoute {
    pub provider: String,
    pub model: String,
    pub upstream: ProviderTransportConfig,
}

pub struct ProviderGateway {
    listener: Option<JoinHandle<()>>,
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
}

impl ProviderGateway {
    pub fn start(
        project: PathBuf,
        directory: String,
        route: GatewayRoute,
        budget: BudgetConfig,
    ) -> Result<Self> {
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
        let context = Arc::new(GatewayContext {
            project,
            directory,
            registration: registration.clone(),
            route,
            budget,
            token: token.clone(),
            invocation: invocation.clone(),
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
            listener: Some(listener),
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
    }
}

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
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
    let transport = ProviderTransport::new(ctx.route.upstream.clone());
    let prepared = match transport.prepare_json(body) {
        Ok(prepared) => prepared,
        Err(_) => {
            respond(&mut socket, 400, "unsupported provider request semantics");
            return;
        }
    };
    let registration = match ctx.registration.lock() {
        Ok(slot) => slot.clone(),
        Err(_) => None,
    };
    let Some(registration) = registration else {
        respond(&mut socket, 503, "gateway runtime not yet ready");
        return;
    };
    let client = match V2SessionClient::connect(&registration, ctx.directory.clone()) {
        Ok(client) => client,
        Err(_) => {
            respond(&mut socket, 503, "runtime lineage unavailable");
            return;
        }
    };
    let (lineage, owner) = match mission::owner_for_execution(
        &ctx.project,
        &client,
        &RuntimeExecutionId::new(session),
    ) {
        Ok(pair) => pair,
        Err(_) => {
            respond(&mut socket, 503, "runtime lineage unavailable");
            return;
        }
    };
    let Some(mission) = owner else {
        respond(&mut socket, 403, "no current Mission owns execution");
        return;
    };
    let service = match SnapshotService::open(&ctx.project) {
        Ok(service) => service,
        Err(_) => {
            respond(&mut socket, 503, "dispatch authority unavailable");
            return;
        }
    };
    let seq = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let seed = format!("{}:{}:{seq}:{}", ctx.invocation, now(), session);
    let id = match DispatchId::new(format!(
        "dsp-{}",
        &crate::runtime::hash::sha256_hex(seed.as_bytes())[..32]
    )) {
        Ok(id) => id,
        Err(_) => {
            respond(&mut socket, 503, "dispatch identity unavailable");
            return;
        }
    };
    let record = DispatchRecord {
        id: id.clone(),
        mission_id: mission.mission_id.clone(),
        generation: mission.generation,
        logical_operation: headers
            .get("x-ocg-logical-operation")
            .filter(|v| v.len() <= 80)
            .cloned()
            .unwrap_or_else(|| id.as_str().to_string()),
        execution_id: session.clone(),
        root_id: lineage.root_id.to_string(),
        provider: ctx.route.provider.clone(),
        model: ctx.route.model.clone(),
        reservation_id: None,
        state: DispatchState::Reserved,
        created_at: now(),
        updated_at: now(),
        usage: None,
        cost_provenance: "unknown".into(),
        failure_class: None,
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
    let (assessment, _) = match service.reserve_dispatch(record, &ctx.budget, quota) {
        Ok(result) => result,
        Err(_) => {
            respond(&mut socket, 503, "dispatch reservation failed");
            return;
        }
    };
    if !assessment.is_allowed() {
        respond(
            &mut socket,
            403,
            "mandatory Mission economic admission blocked provider dispatch",
        );
        return;
    }
    if service.start_dispatch(&id, now()).is_err() {
        respond(&mut socket, 503, "dispatch start could not be persisted");
        return;
    }
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(_) => {
            let _ = service.finish_dispatch(
                &id,
                DispatchState::KnownNotDispatched,
                None,
                Some("local_runtime"),
                now(),
            );
            respond(&mut socket, 503, "provider runtime unavailable");
            return;
        }
    };
    runtime.block_on(async {
        let mut events = match transport.stream(prepared).await {
            Ok(events) => events,
            Err(error) => {
                let _ = service.finish_dispatch(&id, DispatchState::Unresolved, None, Some("provider_start_failure"), now());
                respond(&mut socket, error.status_code().unwrap_or(502), "upstream provider request failed"); return;
            }
        };
        if socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: close\r\n\r\n").is_err() {
            let _ = service.finish_dispatch(&id, DispatchState::Unresolved, None, Some("client_disconnect"), now()); return;
        }
        let mut usage = None;
        let mut finished = false;
        let stream_id = format!("chatcmpl-{}", id.as_str());
        if send(&mut socket, &stream_id, &ctx.route.model, json!({"role":"assistant"}), None, None).is_err() {
            let _ = service.finish_dispatch(&id, DispatchState::Unresolved, None, Some("client_disconnect"), now()); return;
        }
        while let Some(event) = events.next().await {
            let result = match event {
                Ok(ChatStreamEvent::TextDelta { delta }) => send(&mut socket, &stream_id, &ctx.route.model, json!({"content":delta}), None, None),
                Ok(ChatStreamEvent::ReasoningDelta { delta }) => send(&mut socket, &stream_id, &ctx.route.model, json!({"reasoning_content":delta}), None, None),
                Ok(ChatStreamEvent::ToolCallStart { index, id, name }) => send(&mut socket, &stream_id, &ctx.route.model,
                    json!({"tool_calls":[{"index":index,"id":id,"type":"function","function":{"name":name,"arguments":""}}]}), None, None),
                Ok(ChatStreamEvent::ToolCallArgumentsDelta { index, delta, .. }) => send(&mut socket, &stream_id, &ctx.route.model,
                    json!({"tool_calls":[{"index":index,"function":{"arguments":delta}}]}), None, None),
                Ok(ChatStreamEvent::Finish { reason, usage: reported, .. }) => {
                    usage = Some(reported);
                    finished = true;
                    send(&mut socket, &stream_id, &ctx.route.model, json!({}), Some(reason.as_openai_str()), None)
                },
                Ok(ChatStreamEvent::Error(_)) | Err(_) => { break; }
                Ok(ChatStreamEvent::Metadata { .. }) | Ok(ChatStreamEvent::ToolCallComplete { .. }) => Ok(()),
            };
            if result.is_err() { break; }
        }
        let evidence = usage.as_ref().map(|v| DispatchUsage {
            input_tokens: v.input_tokens.map(u64::from), output_tokens: v.output_tokens.map(u64::from),
            reasoning_tokens: v.reasoning_tokens.map(u64::from),
            cache_read_tokens: v.cache_read_tokens.map(u64::from),
            cache_write_tokens: v.cache_write_tokens.map(u64::from),
            provenance: if v.raw.is_some() { "provider_reported" } else { "unknown" }.into(),
        });
        let state = if finished { DispatchState::Settled } else { DispatchState::Unresolved };
        // Never claim success on the client until durable settlement succeeds.
        if service.finish_dispatch(&id, state, evidence, (!finished).then_some("midstream_disconnect"), now()).is_err() {
            return;
        }
        if finished {
            if let Some(tokens) = usage.as_ref() {
                let _ = send_usage(&mut socket, &stream_id, &ctx.route.model, tokens);
            }
            let _ = socket.write_all(b"data: [DONE]\n\n");
        }
    });
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
