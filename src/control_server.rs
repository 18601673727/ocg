//! Loopback-only HTTP/1.1 + SSE control server.
//!
//! The server is a thin transport over [`ControlService`]. It is deliberately
//! not a general web framework:
//!
//! - It binds only a loopback address (`127.0.0.0/8` or `::1`); a wildcard or
//!   routable address is refused before the socket is created.
//! - It accepts a bounded number of concurrent clients and gives every socket a
//!   read and a write timeout, so a slow or stalled consumer is disconnected
//!   instead of pinning a connection.
//! - It caps the request head, the header count and the body, and rejects
//!   malformed, oversized and unsupported requests with a typed error envelope.
//! - It always closes the connection after a response, so there is no keep-alive
//!   state machine to get wrong.
//! - `GET /api/v1/events` first replays the durable journal from the requested
//!   cursor and then tails new events by polling the same authoritative reader.
//!   Event ids are `epoch:seq`; heartbeats are SSE comments and never carry an
//!   id, so they can never advance a client's resume position.
//!
//! No authentication, CORS, frontend or WebSocket support is provided. This is
//! local operator tooling, not a network service.

use crate::contracts::{
    ApiErrorBody, ApiErrorEnvelope, CanonicalConfigurationEnvelope, CanonicalEventsEnvelope,
    CanonicalJobConfigEnvelope, CanonicalProjectsResponse, ProfileView,
};
use crate::error::{OcgError, Result};
use crate::orchestration::checkpoint::is_safe_id;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Upper bound on the request head (request line + headers).
pub const MAX_HEADER_BYTES: usize = 16 * 1024;
/// Upper bound on the number of request headers.
pub const MAX_HEADERS: usize = 64;
/// Upper bound on a request body.
pub const MAX_BODY_BYTES: usize = 256 * 1024;
/// Upper bound on one serialized non-streaming response.
pub const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
/// Default bound on concurrently handled clients.
pub const DEFAULT_MAX_CLIENTS: usize = 16;
/// Default bound on concurrently handled long-lived chat SSE streams. Streams
/// are admitted from their own pool so a live stream can never occupy one of
/// the short-request client slots.
pub const DEFAULT_MAX_STREAMS: usize = 8;
/// Default journal poll interval for an SSE tail.
pub const DEFAULT_POLL_INTERVAL_MS: u64 = 200;
/// Default SSE heartbeat interval.
pub const DEFAULT_HEARTBEAT_MS: u64 = 10_000;
/// Default per-socket write timeout.
pub const DEFAULT_WRITE_TIMEOUT_MS: u64 = 15_000;
/// Default per-socket read timeout.
pub const DEFAULT_READ_TIMEOUT_MS: u64 = 10_000;
/// How long a Profile read waits on the OS credential store, which may be
/// holding an approval prompt nobody is answering, before it reports
/// `credential_store_pending` instead.
const PROFILE_READ_CREDENTIAL_WAIT: Duration = Duration::from_secs(5);

/// Server limits and timings. Every field has a bounded default.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Maximum number of clients handled at once. Further connections receive a
    /// `503` and are closed.
    pub max_clients: usize,
    /// Maximum number of concurrent long-lived chat SSE streams. Streams draw
    /// from their own pool, so a full stream pool leaves every short-request
    /// client slot untouched.
    pub max_streams: usize,
    /// Replay journal retention used by the service this server opens.
    /// How often an SSE tail polls the authoritative journal.
    pub poll_interval: Duration,
    /// How often an idle SSE tail emits a comment heartbeat.
    pub heartbeat: Duration,
    /// Per-socket write timeout; a slow consumer is dropped on expiry.
    pub write_timeout: Duration,
    /// Per-socket read timeout; a stalled request is dropped on expiry.
    pub read_timeout: Duration,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            max_clients: DEFAULT_MAX_CLIENTS,
            max_streams: DEFAULT_MAX_STREAMS,
            poll_interval: Duration::from_millis(DEFAULT_POLL_INTERVAL_MS),
            heartbeat: Duration::from_millis(DEFAULT_HEARTBEAT_MS),
            write_timeout: Duration::from_millis(DEFAULT_WRITE_TIMEOUT_MS),
            read_timeout: Duration::from_millis(DEFAULT_READ_TIMEOUT_MS),
        }
    }
}

impl ServerConfig {
    fn validate(&self) -> Result<()> {
        if self.max_clients == 0 || self.max_clients > 1024 {
            return Err(OcgError::config(
                "control server max_clients must be between 1 and 1024",
            ));
        }
        if self.max_streams == 0 || self.max_streams > 1024 {
            return Err(OcgError::config(
                "control server max_streams must be between 1 and 1024",
            ));
        }
        if self.poll_interval < Duration::from_millis(10) {
            return Err(OcgError::config(
                "control server poll_interval must be at least 10ms",
            ));
        }
        if self.heartbeat < self.poll_interval {
            return Err(OcgError::config(
                "control server heartbeat must not be shorter than its poll interval",
            ));
        }
        if self.write_timeout.is_zero() || self.read_timeout.is_zero() {
            return Err(OcgError::config(
                "control server socket timeouts must be greater than zero",
            ));
        }
        Ok(())
    }
}

/// A bound control server. Call [`ControlServer::serve`] to run its accept loop.
pub struct ControlServer {
    listener: TcpListener,
    addr: SocketAddr,
    /// Backend-backed canonical control surface with process-scoped metadata.
    canonical: Option<crate::orchestration::canonical_control::CanonicalControlService>,
    profile: crate::profile::ProfileService,
    config: ServerConfig,
    active: Arc<AtomicUsize>,
    /// Long-lived chat SSE streams admitted from their own bounded pool.
    active_streams: Arc<AtomicUsize>,
    /// Process-owned registry of lazily activated Project execution workers.
    /// Only present when canonical control service is available.
    execution_runtimes:
        Option<Arc<crate::orchestration::execution_runtime::ProjectRuntimeRegistry>>,
    /// The canonical Project identity resolved and registered by this launch.
    /// `None` when the launch resolved no registered Project boundary.
    startup_project: Option<String>,
    security: Arc<RequestSecurity>,
}

impl ControlServer {
    /// Validate the address and diagnostics, bind the listener, and open the
    /// authoritative service. Only loopback addresses are accepted.
    pub fn bind(addr: &str, root: &Path, config: ServerConfig) -> Result<Self> {
        let home = std::env::var_os("HOME")
            .or_else(|| std::env::var_os("USERPROFILE"))
            .map(PathBuf::from);
        let profile = crate::config::user_config_path(
            None,
            std::env::var_os("OCG_USER_CONFIG")
                .as_deref()
                .map(Path::new),
            std::env::var_os("XDG_CONFIG_HOME")
                .as_deref()
                .map(Path::new),
            home.as_deref(),
        );
        Self::bind_with_profile(addr, root, &profile, config, false)
    }

    /// Bind using a user-global profile while keeping durable project state at
    /// `root`.
    pub fn bind_with_profile(
        addr: &str,
        root: &Path,
        profile_path: &Path,
        config: ServerConfig,
        disable_proxy: bool,
    ) -> Result<Self> {
        let authentication = crate::control_security::Authentication::from_env()?;
        let requested = parse_loopback_addr(addr)?;
        config.validate()?;
        let listener = TcpListener::bind(requested).map_err(|error| {
            OcgError::io(
                format!("cannot bind the control server to {requested}"),
                error,
            )
        })?;
        let bound = listener
            .local_addr()
            .map_err(|error| OcgError::io("cannot read the control server address", error))?;
        // Defense in depth: even if the OS remapped the address, it must be
        // loopback.
        if !bound.ip().is_loopback() {
            return Err(OcgError::config(format!(
                "control server bound a non-loopback address {bound}; refusing to serve"
            )));
        }

        let service =
            crate::orchestration::canonical_control::CanonicalControlService::open_process(
                root,
                profile_path,
            )?;
        let security = Arc::new(RequestSecurity {
            authentication,
            ownership: service.security_store()?,
            bound,
        });
        let boundary = crate::project::resolve(root);
        // The launch Project is registered here, so its canonical identity is
        // already known to this process. It is handed to the UI as the
        // explicit Project of this launch instead of leaving the UI to pick one
        // from a persisted browser selection or a list ordering.
        let startup_project = if boundary.has_marker() {
            let registered =
                service.register_project("startup-register", boundary.root(), now_unix())?;
            Some(registered.project.project_id)
        } else {
            None
        };

        // Share process transport policy; activate bounded workers lazily per Project.
        let selection = crate::proxy::resolve(
            disable_proxy,
            &crate::proxy::SystemProxyEnv,
            &crate::process::SystemStaticProxy,
        );
        let transport = Arc::new(crate::http::NativeHttp::with_policy(
            selection.plan(),
            None,
        )?);
        let registry = Arc::new(
            crate::orchestration::execution_runtime::ProjectRuntimeRegistry::new(
                transport,
                crate::native_tools::PermissionPolicy::default(),
                16,
                16,
            ),
        );
        let (canonical, execution_runtimes) = if security.authentication.is_remote() {
            (Some(service), None)
        } else {
            (
                Some(service.with_runtime_registry(registry.clone())),
                Some(registry),
            )
        };

        Ok(Self {
            listener,
            addr: bound,
            canonical,
            profile: crate::profile::ProfileService::with_workspace(profile_path, root),
            config,
            active: Arc::new(AtomicUsize::new(0)),
            active_streams: Arc::new(AtomicUsize::new(0)),
            execution_runtimes,
            startup_project,
            security,
        })
    }

    /// The canonical Project identity this launch registered, when the launch
    /// resolved a Project boundary. The frontend treats it as the explicit
    /// Project of the launch; it is never a UI-side guess.
    pub fn startup_project(&self) -> Option<&str> {
        self.startup_project.as_deref()
    }

    /// The address the server is actually bound to.
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// A human-readable base URL for the bound loopback address.
    pub fn base_url(&self) -> String {
        match self.addr {
            SocketAddr::V4(_) => format!("http://{}", self.addr),
            SocketAddr::V6(_) => format!("http://[{}]:{}", self.addr.ip(), self.addr.port()),
        }
    }

    /// Run the accept loop until `stop` is set.
    pub fn serve(self, stop: Arc<AtomicBool>) -> Result<()> {
        let _job_admission = if self.security.authentication.is_remote() {
            None
        } else {
            self.canonical
                .as_ref()
                .map(|service| service.start_job_admission_worker())
                .transpose()?
        };
        self.listener.set_nonblocking(true).map_err(|error| {
            OcgError::io(
                "cannot put the control server listener in nonblocking mode",
                error,
            )
        })?;
        loop {
            if stop.load(Ordering::SeqCst) {
                return Ok(());
            }
            match self.listener.accept() {
                Ok((stream, _peer)) => self.dispatch(stream),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(5));
                }
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(error) => {
                    return Err(OcgError::io("control server accept failed", error));
                }
            }
        }
    }

    fn dispatch(&self, mut stream: TcpStream) {
        let current = self.active.fetch_add(1, Ordering::SeqCst);
        if current >= self.config.max_clients {
            self.active.fetch_sub(1, Ordering::SeqCst);
            let _ = stream.set_write_timeout(Some(self.config.write_timeout));
            let _ = write_api_error(
                &mut stream,
                &ApiError::new(
                    503,
                    "overloaded",
                    "the control server is at its concurrent client limit",
                ),
            );
            return;
        }
        // Admission is optimistic on the client lane; a long-lived chat SSE
        // tail switches to its own bounded stream lane inside the handler, so
        // it never occupies a short-request slot for its lifetime.
        let guard = ClientGuard(Arc::clone(&self.active));
        let canonical = self.canonical.clone();
        let profile = self.profile.clone();
        let config = self.config.clone();
        let security = self.security.clone();
        let active_streams = Arc::clone(&self.active_streams);
        let spawned = thread::Builder::new()
            .name("ocg-control-client".to_string())
            .spawn(move || {
                handle_client(
                    stream,
                    canonical.as_ref(),
                    &profile,
                    &config,
                    &security,
                    guard,
                    active_streams,
                );
            });
        if spawned.is_err() {
            // The closure (and its guard) is dropped, releasing the slot.
        }
    }
}

/// Decrements whichever lane counter it was created with, exactly once, even
/// on panic.
struct ClientGuard(Arc<AtomicUsize>);

impl Drop for ClientGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Drop for ControlServer {
    fn drop(&mut self) {
        if let Some(registry) = self.execution_runtimes.take() {
            if let Err(error) = registry.shutdown() {
                eprintln!("ocg: Project runtime shutdown failed: {error}");
            }
        }
    }
}

/// Parse and validate a numeric loopback bind address.
pub fn parse_loopback_addr(raw: &str) -> Result<SocketAddr> {
    let trimmed = raw.trim();
    let socket: SocketAddr = trimmed.parse().map_err(|_| {
        OcgError::config(format!(
            "control server address '{raw}' must be a numeric loopback IP and port, for example \
             127.0.0.1:0 or [::1]:8765"
        ))
    })?;
    if !socket.ip().is_loopback() {
        return Err(OcgError::config(format!(
            "control server refuses to bind {socket}: only loopback addresses are allowed"
        )));
    }
    Ok(socket)
}

/// A typed transport error rendered as the shared JSON envelope.
#[derive(Debug)]
struct ApiError {
    status: u16,
    body: Value,
    allow: Option<&'static str>,
}

impl ApiError {
    fn new(status: u16, code: &'static str, message: impl Into<String>) -> Self {
        // The declared envelope from `src/contracts.rs`, so the PWA's error
        // type is the shape the server actually emits.
        let body = serde_json::to_value(ApiErrorEnvelope {
            error: ApiErrorBody {
                code: code.to_string(),
                message: bounded(message.into()),
            },
        })
        .unwrap_or_else(|_| json!({ "error": { "code": "internal", "message": "unavailable" } }));
        Self {
            status,
            body,
            allow: None,
        }
    }
    fn method_not_allowed(allow: &'static str) -> Self {
        let mut error = Self::new(
            405,
            "method_not_allowed",
            format!("this route only allows {allow}"),
        );
        error.allow = Some(allow);
        error
    }

    fn to_json(&self) -> Value {
        self.body.clone()
    }
}

fn api_error_for(error: &OcgError) -> ApiError {
    match error {
        OcgError::SpawnRefused { reason, message } => {
            ApiError::new(409, reason.code(), message.clone())
        }
        _ => ApiError::new(400, "invalid_request", error.to_string()),
    }
}

fn bounded(message: String) -> String {
    let redacted = crate::telemetry::task::redact(&message);
    const MAX: usize = 400;
    if redacted.len() <= MAX {
        return redacted;
    }
    let mut end = MAX;
    while end > 0 && !redacted.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &redacted[..end])
}

/// One parsed HTTP request. Only the fields this server uses are retained.
struct Request {
    method: String,
    path: String,
    query: HashMap<String, String>,
    headers: HashMap<String, String>,
    body: Vec<u8>,
    approved_origin: Option<String>,
    identity: Option<crate::control_security::Identity>,
    ownership: Option<crate::control_security::OwnershipStore>,
}

struct RequestSecurity {
    authentication: crate::control_security::Authentication,
    ownership: crate::control_security::OwnershipStore,
    bound: SocketAddr,
}

fn handle_security(
    stream: &mut TcpStream,
    canonical: Option<&crate::orchestration::canonical_control::CanonicalControlService>,
    security: &RequestSecurity,
    request: &Request,
) -> bool {
    let result = match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/api/v1/auth/session") => {
            let session = crate::contracts::AuthenticationSession {
                mode: if request.identity.is_some() {
                    crate::contracts::AuthenticationMode::CloudflareAccess
                } else {
                    crate::contracts::AuthenticationMode::Local
                },
                user_id: request
                    .identity
                    .as_ref()
                    .map(|identity| identity.user_id.clone()),
                expires_at: request
                    .identity
                    .as_ref()
                    .map(|identity| identity.expires_at),
                remote_execution: false,
            };
            match serde_json::to_value(session) {
                Ok(value) => {
                    write_json_with_origin(stream, 200, &value, request.approved_origin.as_deref())
                }
                Err(_) => write_api_error(
                    stream,
                    &ApiError::new(500, "internal", "cannot encode authentication session"),
                ),
            }
        }
        ("POST", "/api/v1/auth/logout") => {
            if let Some(identity) = &request.identity {
                match security.ownership.revoke(identity, now_unix()) {
                    Ok(()) => write_response(
                        stream,
                        303,
                        "application/json",
                        b"{}",
                        &[("Location", "/cdn-cgi/access/logout".to_string())],
                    ),
                    Err(_) => write_api_error(
                        stream,
                        &ApiError::new(503, "security_unavailable", "cannot revoke session"),
                    ),
                }
            } else {
                write_json_with_origin(
                    stream,
                    200,
                    &json!({"mode": "local"}),
                    request.approved_origin.as_deref(),
                )
            }
        }
        ("POST", "/api/v1/auth/ownership") if !security.authentication.is_remote() => {
            let migrate = || -> std::result::Result<Value, ApiError> {
                let body = request.json_body()?;
                let migration: crate::contracts::ProjectOwnershipRequest =
                    serde_json::from_value(body).map_err(|_| {
                        ApiError::new(
                            400,
                            "invalid_request",
                            "explicit project_id, issuer and subject are required",
                        )
                    })?;
                let project = migration.project_id.as_str();
                let issuer = migration.issuer.as_str();
                let subject = migration.subject.as_str();
                let registered = canonical
                    .ok_or_else(|| {
                        ApiError::new(503, "security_unavailable", "Project registry unavailable")
                    })?
                    .projects()
                    .map_err(|_| {
                        ApiError::new(503, "security_unavailable", "Project registry unavailable")
                    })?;
                if !registered.iter().any(|record| record.project_id == project) {
                    return Err(ApiError::new(
                        404,
                        "project_unavailable",
                        "Project unavailable",
                    ));
                }
                let user = security
                    .ownership
                    .assign_legacy(project, issuer, subject)
                    .map_err(|error| ApiError::new(409, "migration_refused", error.to_string()))?;
                serde_json::to_value(crate::contracts::ProjectOwnershipResponse {
                    project_id: project.to_string(),
                    user_id: user,
                })
                .map_err(|_| ApiError::new(500, "internal", "cannot encode migration result"))
            };
            match migrate() {
                Ok(value) => {
                    write_json_with_origin(stream, 200, &value, request.approved_origin.as_deref())
                }
                Err(error) => write_api_error(stream, &error),
            }
        }
        (_, path) if path.starts_with("/api/v1/auth/") => write_api_error(
            stream,
            &ApiError::new(
                403,
                "operation_refused",
                "authentication operation unavailable",
            ),
        ),
        _ => return false,
    };
    if let Err(error) = result {
        tracing::debug!(%error, "security response connection closed");
    }
    true
}

fn authorize_remote(
    canonical: Option<&crate::orchestration::canonical_control::CanonicalControlService>,
    route: &Route,
    request: &Request,
) -> std::result::Result<(), ApiError> {
    let Some(identity) = &request.identity else {
        return Ok(());
    };
    if matches!(
        route,
        Route::Product { .. } | Route::CanonicalProjects | Route::CanonicalPreflight
    ) {
        return Ok(());
    }
    if request.method != "GET" {
        return Err(ApiError::new(
            403,
            "remote_execution_blocked",
            "remote mutations require isolated executors and filesystem roots",
        ));
    }
    let project = match route {
        Route::ChatImageGet { project, .. } => Some(project.clone()),
        Route::ChatStream => canonical.and_then(|service| {
            service.chat_project_for(
                request.query.get("session_id")?,
                request.query.get("job_id")?,
            )
        }),
        Route::CanonicalSnapshot
        | Route::CanonicalEvents
        | Route::CanonicalDashboard
        | Route::ProjectUsage
        | Route::ConversationUsage
        | Route::JobUsage
        | Route::ChatConversations
        | Route::ChatMessages => request.query.get("project_id").cloned(),
        // Profile, Vault, configuration, setup and global probes are process
        // authority. A Project owner must never acquire that authority.
        _ => {
            return Err(ApiError::new(
                403,
                "operator_required",
                "this operation requires a local operator",
            ))
        }
    };
    let allowed = project.as_deref().is_some_and(|project| {
        request
            .ownership
            .as_ref()
            .is_some_and(|ownership| ownership.owns(&identity.user_id, project).unwrap_or(false))
    });
    if allowed {
        Ok(())
    } else {
        Err(ApiError::new(
            403,
            "resource_denied",
            "resource unavailable to this user",
        ))
    }
}

impl Request {
    fn json_body(&self) -> std::result::Result<Value, ApiError> {
        if self.body.is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_slice(&self.body)
            .map_err(|_| ApiError::new(400, "malformed_json", "the request body is not valid JSON"))
    }
}

fn handle_client(
    mut stream: TcpStream,
    canonical: Option<&crate::orchestration::canonical_control::CanonicalControlService>,
    profile: &crate::profile::ProfileService,
    config: &ServerConfig,
    security: &RequestSecurity,
    guard: ClientGuard,
    active_streams: Arc<AtomicUsize>,
) {
    let mut lane_guard = Some(guard);
    let _ = stream.set_nonblocking(false);
    let _ = stream.set_read_timeout(Some(config.read_timeout));
    let _ = stream.set_write_timeout(Some(config.write_timeout));
    let _ = stream.set_nodelay(true);

    let mut request = match read_request(&mut stream) {
        Ok(request) => request,
        Err(error) => {
            let _ = write_api_error(&mut stream, &error);
            return;
        }
    };

    // The Host check precedes routing, so no handler (read or mutation) ever
    // sees a request addressed to a non-local authority.
    if let Err(error) = check_host(&request, security.bound.port()) {
        let _ = write_api_error(&mut stream, &error);
        return;
    }

    if let Some(origin) = request.headers.get("origin") {
        if !security.authentication.accepts_origin(origin) {
            let _ = write_api_error(
                &mut stream,
                &ApiError::new(403, "origin_refused", "browser origin is not trusted"),
            );
            return;
        }
        request.approved_origin = Some(origin.clone());
    }
    if request
        .headers
        .get("sec-fetch-site")
        .is_some_and(|site| site == "cross-site")
    {
        let _ = write_api_error(
            &mut stream,
            &ApiError::new(403, "origin_refused", "cross-site requests are refused"),
        );
        return;
    }
    request.identity = match security.authentication.authenticate(
        &request.headers,
        &security.ownership,
        now_unix(),
    ) {
        Ok(identity) => identity,
        Err(_) => {
            let _ = write_api_error(
                &mut stream,
                &ApiError::new(
                    401,
                    "authentication_required",
                    "a valid Access session is required",
                ),
            );
            return;
        }
    };
    request.ownership = Some(security.ownership.clone());
    if handle_security(&mut stream, canonical, security, &request) {
        return;
    }

    let route = match classify(&request) {
        Ok(route) => route,
        Err(error) => {
            let _ = write_api_error(&mut stream, &error);
            return;
        }
    };

    if let Err(error) = authorize_remote(canonical, &route, &request) {
        let _ = write_api_error(&mut stream, &error);
        return;
    }

    // The request head is parsed on this per-connection thread, never on the
    // accept loop. Only an actual GET stream switches lanes; OPTIONS and all
    // ordinary control requests remain in the short-request pool.
    if request.method == "GET" && matches!(route, Route::ChatStream) {
        let current = active_streams.fetch_add(1, Ordering::SeqCst);
        if current >= config.max_streams {
            active_streams.fetch_sub(1, Ordering::SeqCst);
            let _ = write_api_error(
                &mut stream,
                &ApiError::new(
                    503,
                    "overloaded",
                    "the control server is at its concurrent chat stream limit",
                ),
            );
            return;
        }
        let _ = lane_guard.replace(ClientGuard(active_streams));
    }

    if matches!(
        route,
        Route::ProfileGet
            | Route::ProfileBootstrap
            | Route::ProfilePut
            | Route::ProfileCredential
            | Route::ProfilePreflight
    ) {
        if request.method == "OPTIONS" {
            let _ = write_preflight(&mut stream, &request);
        } else {
            let _ = handle_profile(&mut stream, profile, &route, &request);
        }
        return;
    }
    if matches!(
        route,
        Route::SetupProviderConnect
            | Route::SetupProviderRefresh
            | Route::SetupModelsSave
            | Route::SetupBrowse
            | Route::SetupProjectInit
    ) {
        if request.method == "OPTIONS" {
            let _ = write_preflight(&mut stream, &request);
        } else {
            let _ = handle_setup(&mut stream, profile, canonical, &route, &request);
        }
        return;
    }
    if let Route::Product { path } = &route {
        let _ = serve_product(&mut stream, &request, path);
        return;
    }
    if let Some(canonical) = canonical {
        // The OCG PWA is served from a different loopback origin than this
        // server, so the browser needs an explicit preflight answer. Only
        // loopback origins are ever echoed, only for the canonical control
        // surface, and credentials are never allowed.
        if request.method == "OPTIONS" {
            let _ = write_preflight(&mut stream, &request);
            return;
        }
        if let Some(outcome) = handle_canonical(&mut stream, canonical, &route, &request) {
            let _ = outcome;
            return;
        }
    }
    let _ = write_api_error(
        &mut stream,
        &ApiError::new(
            409,
            "boundary_required",
            "canonical control needs an initialized Project boundary",
        ),
    );
}

/// Reject any request whose Host is not this server's own loopback authority.
/// HTTP/1.1 requires a Host header (RFC 7230 section 5.4) and HTTP/1.0 does
/// not, but a missing Host cannot be checked, so it is refused for both.
fn check_host(request: &Request, bound_port: u16) -> std::result::Result<(), ApiError> {
    let Some(host) = request.headers.get("host") else {
        return Err(ApiError::new(
            400,
            "malformed_request",
            "the Host header is required",
        ));
    };
    if is_local_host(host, bound_port) {
        Ok(())
    } else {
        Err(ApiError::new(
            403,
            "host_refused",
            "only a loopback Host may use the control server",
        ))
    }
}

/// True when `value` is `<host>` or `<host>:<port>`, where `<host>` is either
/// the name `localhost` or a numeric loopback IP (any `127.0.0.0/8` IPv4 or
/// `::1`, bracketed when IPv6), and the port is exactly `bound_port`. A bare
/// host stands for port 80, the default an HTTP client omits, so it is accepted
/// only when that is the bound port. The host is split off structurally, so
/// suffixes such as `localhost.evil.com`, `127.0.0.2.evil.com` or
/// `localhost@evil.com` cannot pass, and no DNS lookup is ever made.
fn is_local_host(value: &str, bound_port: u16) -> bool {
    let value = value.to_ascii_lowercase();
    let (host, rest) = if value.starts_with('[') {
        let Some(end) = value.find(']') else {
            return false;
        };
        (&value[..=end], &value[end + 1..])
    } else {
        match value.find(':') {
            Some(index) => (&value[..index], &value[index..]),
            None => (value.as_str(), ""),
        }
    };
    let port = if rest.is_empty() {
        None
    } else if let Some(port) = rest.strip_prefix(':') {
        Some(port)
    } else {
        return false;
    };
    let host_is_loopback = if let Some(literal) = host.strip_prefix('[') {
        literal
            .strip_suffix(']')
            .and_then(|inner| inner.parse::<Ipv6Addr>().ok())
            .is_some_and(|addr| addr.is_loopback())
    } else if host == "localhost" {
        true
    } else {
        host.parse::<Ipv4Addr>()
            .is_ok_and(|addr| addr.is_loopback())
    };
    host_is_loopback
        && match port {
            Some(port) => port == bound_port.to_string(),
            None => bound_port == 80,
        }
}

/// The CORS allowlist: only a loopback HTTP origin may call the canonical
/// control surface. Credentials are never allowed, so an allowlist of concrete
/// origins (never `*`) is both correct and required.
fn allowed_cors_origin(request: &Request) -> Option<String> {
    request.approved_origin.clone()
}

fn cors_headers(origin: Option<&str>, methods: &str) -> Vec<(&'static str, String)> {
    let mut headers: Vec<(&'static str, String)> =
        vec![("Access-Control-Allow-Methods", methods.to_string())];
    if let Some(origin) = origin {
        headers.push(("Access-Control-Allow-Origin", origin.to_string()));
        headers.push(("Vary", "Origin".to_string()));
    }
    headers
}

fn write_preflight(stream: &mut TcpStream, request: &Request) -> std::io::Result<()> {
    let origin = allowed_cors_origin(request);
    if origin.is_none() {
        return write_api_error(
            stream,
            &ApiError::new(
                403,
                "origin_refused",
                "only a loopback origin may use the canonical control surface",
            ),
        );
    }
    let owned = canonical_segments(request);
    let borrowed: Vec<&str> = owned.iter().map(String::as_str).collect();
    let methods = allowed_methods(&borrowed).unwrap_or("GET, PUT, POST, OPTIONS");
    let body = b"{}";
    let mut headers = cors_headers(origin.as_deref(), &format!("{methods}, OPTIONS"));
    headers.push((
        "Access-Control-Allow-Headers",
        "content-type, last-event-id".to_string(),
    ));
    headers.push(("Access-Control-Max-Age", "600".to_string()));
    write_response(stream, 204, "application/json", body, &headers)
}

fn canonical_segments(request: &Request) -> Vec<String> {
    request
        .path
        .trim_start_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .map(str::to_string)
        .collect()
}

fn write_json_with_origin(
    stream: &mut TcpStream,
    status: u16,
    value: &Value,
    origin: Option<&str>,
) -> std::io::Result<()> {
    let body = serde_json::to_vec(value).unwrap_or_else(|_| b"{}".to_vec());
    let headers = origin
        .map(|origin| vec![("Access-Control-Allow-Origin", origin.to_string())])
        .unwrap_or_default();
    write_response(stream, status, "application/json", &body, &headers)
}

/// Configuration/onboarding reads and writes share the project YAML with the
/// CLI. The service does not select an external candidate on the user's behalf.
fn handle_profile(
    stream: &mut TcpStream,
    service: &crate::profile::ProfileService,
    route: &Route,
    request: &Request,
) -> std::io::Result<()> {
    if request.headers.contains_key("origin") && allowed_cors_origin(request).is_none() {
        return write_api_error(
            stream,
            &ApiError::new(
                403,
                "origin_refused",
                "Profile API accepts only loopback browser origins",
            ),
        );
    }
    // `Ok(None)` means this handler does not own the route.
    let operation = || -> Result<Option<Value>> {
        // A read never waits on the OS credential store past a bound; the
        // mutations keep their existing wait, which follows a committed edit.
        let view = |wait: Option<Duration>| -> Result<Value> {
            let current = service.current()?;
            // A declared struct, so the generated TypeScript describes the
            // response the PWA actually receives.
            serde_json::to_value(ProfileView {
                api_version: crate::profile::PROVIDER_PROFILE_API_VERSION.to_string(),
                profile: current.as_ref().map(|(profile, _)| profile.clone()),
                revision: current.as_ref().map(|(_, revision)| revision.clone()),
                runnable_choices: service.runnable_choices(wait)?,
            })
            .map_err(|error| OcgError::config(error.to_string()))
        };
        let value = match route {
            Route::ProfileGet => view(Some(PROFILE_READ_CREDENTIAL_WAIT)),
            Route::ProfileBootstrap => {
                let body = request
                    .json_body()
                    .map_err(|_| OcgError::config("invalid Profile bootstrap body"))?;
                let choice = body
                    .get("choice")
                    .and_then(Value::as_str)
                    .ok_or_else(|| OcgError::config("explicit choice is required: new"))?;
                match choice {
                    "new" => {
                        service.bootstrap()?;
                    }
                    _ => return Err(OcgError::config("choice must be new")),
                }
                view(None)
            }
            Route::ProfilePut => {
                let body = request
                    .json_body()
                    .map_err(|_| OcgError::config("invalid Profile edit body"))?;
                let expected = body
                    .get("revision")
                    .and_then(Value::as_str)
                    .ok_or_else(|| OcgError::config("expected Profile revision is required"))?;
                let profile: crate::profile::Profile = serde_json::from_value(
                    body.get("profile")
                        .cloned()
                        .ok_or_else(|| OcgError::config("edited Profile is required"))?,
                )
                .map_err(|error| OcgError::config(format!("invalid edited Profile: {error}")))?;
                service.replace(expected, &profile)?;
                view(None)
            }
            Route::ProfileCredential => {
                // The secret goes to the Vault only. The response is the
                // recomputed ProfileView with fresh readiness; it never
                // contains the secret.
                let request: crate::contracts::ProfileCredentialRequest = serde_json::from_value(
                    request
                        .json_body()
                        .map_err(|_| OcgError::config("invalid Profile credential body"))?,
                )
                .map_err(|error| {
                    OcgError::config(format!("invalid Profile credential: {error}"))
                })?;
                let vault = crate::vault::Vault::user_global()?;
                vault.set(&request.name, &request.value)?;
                view(None)
            }
            _ => return Ok(None),
        }?;
        Ok(Some(value))
    };
    let origin = allowed_cors_origin(request);
    match operation() {
        Ok(Some(value)) => write_json_with_origin(stream, 200, &value, origin.as_deref()),
        Ok(None) => write_api_error(
            stream,
            &ApiError::new(404, "not_found", "no such control route"),
        ),
        Err(OcgError::CredentialStorePending(message)) => write_json_with_origin(
            stream,
            503,
            &ApiError::new(503, "credential_store_pending", message).to_json(),
            origin.as_deref(),
        ),
        Err(error) => write_json_with_origin(
            stream,
            400,
            &ApiError::new(400, "invalid_profile", error.to_string()).to_json(),
            origin.as_deref(),
        ),
    }
}

/// Handle setup/first-run endpoints: provider discovery, model selection, filesystem browse, project init.
fn handle_setup(
    stream: &mut TcpStream,
    service: &crate::profile::ProfileService,
    canonical: Option<&crate::orchestration::canonical_control::CanonicalControlService>,
    route: &Route,
    request: &Request,
) -> std::io::Result<()> {
    if request.headers.contains_key("origin") && allowed_cors_origin(request).is_none() {
        return write_api_error(
            stream,
            &ApiError::new(
                403,
                "origin_refused",
                "Setup API accepts only loopback browser origins",
            ),
        );
    }

    // `Ok(None)` means this handler does not own the route.
    let operation = || -> Result<Option<Value>> {
        let value = match route {
            Route::SetupProviderConnect => {
                let body: crate::contracts::SetupConnectRequest = serde_json::from_value(
                    request
                        .json_body()
                        .map_err(|_| OcgError::config("invalid setup connect body"))?,
                )
                .map_err(|_| OcgError::config("invalid setup connect request"))?;
                let name = body.name.trim();
                if name.is_empty() {
                    return Err(OcgError::config("provider name is required"));
                }
                if body.api_key.trim().is_empty() {
                    return Err(OcgError::config("api key is required"));
                }

                // Both URLs are derived from the one operator-supplied value, so
                // discovery and dispatch can never disagree about the base path.
                let chat_endpoint = crate::setup::chat_endpoint_from_base(&body.endpoint)?;

                // Discover before writing anything: a provider that cannot be
                // reached must not leave a half-configured Profile behind.
                let http = crate::http::NativeHttp::new()?;
                let catalog = crate::setup::discover_catalog(&http, &body.endpoint, &body.api_key)?;

                // Keys are chosen against the Profile and Vault as committed
                // now, so concurrent connects add distinct providers exactly
                // as sequential connects would.
                let vault = crate::vault::Vault::user_global()?;
                let (new_revision, provider_key) = service.edit_with_credential(
                    &vault,
                    body.api_key.trim(),
                    |profile, stored| {
                        let (provider_key, credential_ref) = {
                            let existing_keys: Vec<&str> =
                                profile.providers.keys().map(String::as_str).collect();
                            let existing_refs: Vec<&str> = profile
                                .providers
                                .values()
                                .filter_map(|p| p.credential_ref.as_deref())
                                .chain(stored.iter().map(String::as_str))
                                .collect();
                            (
                                crate::setup::normalize_provider_key(name, &existing_keys),
                                crate::setup::normalize_credential_ref(name, &existing_refs),
                            )
                        };
                        profile.providers.insert(
                            provider_key.clone(),
                            crate::profile::Provider {
                                label: name.to_string(),
                                endpoint: Some(chat_endpoint),
                                credential_ref: Some(credential_ref.clone()),
                                protocol: Some(
                                    crate::provider_protocol::ProviderProtocol::OpenAiCompatible,
                                ),
                                catalog: Some(catalog.clone()),
                            },
                        );
                        Ok((credential_ref, provider_key))
                    },
                )?;
                let models = crate::setup::catalog_models(&provider_key, &catalog);

                Ok(
                    serde_json::to_value(crate::contracts::SetupConnectResponse {
                        api_version: crate::profile::PROVIDER_PROFILE_API_VERSION.to_string(),
                        provider_key,
                        models,
                        revision: new_revision,
                    })
                    .map_err(|error| OcgError::config(error.to_string()))?,
                )
            }

            Route::SetupProviderRefresh => {
                let body: crate::contracts::SetupRefreshRequest = serde_json::from_value(
                    request
                        .json_body()
                        .map_err(|_| OcgError::config("invalid catalog refresh body"))?,
                )
                .map_err(|_| OcgError::config("invalid catalog refresh request"))?;
                let (mut profile, revision) = service
                    .current()?
                    .ok_or_else(|| OcgError::config("Profile is missing"))?;
                if revision != body.revision {
                    return Err(OcgError::config(
                        "Profile changed; refresh before discovering models",
                    ));
                }
                let provider = profile
                    .providers
                    .get_mut(&body.provider_key)
                    .ok_or_else(|| OcgError::config("provider is not configured"))?;
                if !provider.wire_protocol().is_openai_chat_completions() {
                    return Err(OcgError::config(
                        "model catalog discovery requires an OpenAI Chat Completions provider",
                    ));
                }
                let endpoint = provider
                    .endpoint
                    .as_deref()
                    .ok_or_else(|| OcgError::config("provider endpoint is missing"))?;
                let vault = crate::vault::Vault::user_global()?;
                let secret = match provider.credential_ref.as_deref() {
                    Some(reference) => vault
                        .get(reference)?
                        .ok_or_else(|| OcgError::config("provider credential is missing"))?,
                    None => String::new(),
                };
                // Setup discovery has no Project; scheduled discovery can invoke this same operation.
                let catalog = crate::setup::discover_catalog(
                    &crate::http::NativeHttp::new()?,
                    endpoint,
                    &secret,
                )?;
                let models = crate::setup::catalog_models(&body.provider_key, &catalog);
                provider.catalog = Some(catalog.clone());
                // Enabled models follow the provider's current label and
                // metadata, and keep the user's variant while it is still
                // offered. A model the provider no longer lists stays in place
                // rather than being removed silently; the UI reports it.
                for model in profile
                    .models
                    .values_mut()
                    .filter(|model| model.provider == body.provider_key)
                {
                    let Some(current) = catalog.models.iter().find(|entry| entry.id == model.id)
                    else {
                        continue;
                    };
                    model.label = Some(current.label.clone());
                    model.variants = current.metadata.variants.clone().unwrap_or_default();
                    model.metadata = Some(current.metadata.clone());
                    let still_offered = model.variant.as_ref().is_some_and(|variant| {
                        model.variants.is_empty() || model.variants.contains(variant)
                    });
                    if !still_offered {
                        model.variant = current.metadata.variant.clone();
                    }
                }
                profile.validate()?;
                let revision = service.replace(&body.revision, &profile)?;
                Ok(
                    serde_json::to_value(crate::contracts::SetupConnectResponse {
                        api_version: crate::profile::PROVIDER_PROFILE_API_VERSION.to_string(),
                        provider_key: body.provider_key,
                        models,
                        revision,
                    })
                    .map_err(|error| OcgError::config(error.to_string()))?,
                )
            }

            Route::SetupModelsSave => {
                let body: crate::contracts::SetupModelsRequest = serde_json::from_value(
                    request
                        .json_body()
                        .map_err(|_| OcgError::config("invalid setup models body"))?,
                )
                .map_err(|error| OcgError::config(format!("invalid setup models: {error}")))?;

                // Get current profile
                let current = service.current()?;
                let (mut profile, _revision) = current
                    .as_ref()
                    .ok_or_else(|| OcgError::config("profile must be bootstrapped first"))?
                    .clone();

                // Ensure provider exists
                if !profile.providers.contains_key(&body.provider_key) {
                    return Err(OcgError::config(format!(
                        "provider '{}' not found",
                        body.provider_key
                    )));
                }

                if body.models.is_empty() {
                    return Err(OcgError::config("select at least one model"));
                }

                let provider_key = body.provider_key.clone();
                if !profile.providers.contains_key(&provider_key) {
                    return Err(OcgError::config(format!(
                        "provider '{provider_key}' is not configured"
                    )));
                }
                let catalog = profile
                    .providers
                    .get(&provider_key)
                    .and_then(|provider| provider.catalog.as_ref())
                    .ok_or_else(|| OcgError::config("refresh provider models before selecting"))?
                    .clone();
                let mut selected = std::collections::BTreeSet::new();
                for selection in &body.models {
                    if !selected.insert(&selection.key)
                        || !catalog.models.iter().any(|model| {
                            model.id == selection.id
                                && crate::setup::model_key(&provider_key, &model.id)
                                    == selection.key
                        })
                    {
                        return Err(OcgError::config(
                            "selected model does not belong to the current provider catalog",
                        ));
                    }
                }
                if let Some(default_model) = &body.default_model {
                    if !body.models.iter().any(|s| &s.key == default_model) {
                        return Err(OcgError::config(
                            "the default model must be one of the selected models",
                        ));
                    }
                }

                // Replace this provider's models wholesale; keep other providers.
                // A variant the user already chose survives re-saving while
                // the provider still offers it.
                let previous_variants: std::collections::BTreeMap<String, String> = profile
                    .models
                    .iter()
                    .filter(|(_, model)| model.provider == provider_key)
                    .filter_map(|(key, model)| Some((key.clone(), model.variant.clone()?)))
                    .collect();
                profile.models.retain(|_, m| m.provider != provider_key);
                for selection in &body.models {
                    let discovered = catalog
                        .models
                        .iter()
                        .find(|model| model.id == selection.id)
                        .ok_or_else(|| {
                            OcgError::config("selected model is no longer in the catalog")
                        })?;
                    let variants = discovered.metadata.variants.clone().unwrap_or_default();
                    let variant = previous_variants
                        .get(&selection.key)
                        .filter(|variant| variants.is_empty() || variants.contains(variant))
                        .cloned()
                        .or_else(|| discovered.metadata.variant.clone());
                    profile.models.insert(
                        selection.key.clone(),
                        crate::profile::Model {
                            provider: provider_key.clone(),
                            id: selection.id.clone(),
                            variant,
                            variants,
                            label: Some(discovered.label.clone()),
                            metadata: Some(discovered.metadata.clone()),
                        },
                    );
                }

                // Without an explicit default, keep the current one if it
                // survived this provider's model replacement.
                let default_model = body
                    .default_model
                    .clone()
                    .or_else(|| {
                        profile
                            .default_model
                            .clone()
                            .filter(|key| profile.models.contains_key(key))
                    })
                    .ok_or_else(|| OcgError::config("choose a default model"))?;
                profile.default_model = Some(default_model.clone());
                profile.validate()?;

                // Backend readiness is the only authority on whether setup may
                // continue. Anything less than one runnable choice is reported
                // as a failure instead of a silent success.
                let runnable = profile.executable_choices(&crate::vault::Vault::user_global()?);
                if !runnable.contains(&default_model) {
                    return Err(OcgError::config(
                        "selected default model is not executable: check provider configuration",
                    ));
                }

                let new_revision = service.replace(&body.revision, &profile)?;

                Ok(serde_json::to_value(crate::contracts::SetupModelsResponse {
                    api_version: crate::profile::PROVIDER_PROFILE_API_VERSION.to_string(),
                    selected_models: selected.into_iter().cloned().collect(),
                    default_model,
                    runnable_choices: runnable,
                    revision: new_revision.clone(),
                })
                .map_err(|error| OcgError::config(error.to_string()))?)
            }

            Route::SetupBrowse => {
                let body: crate::contracts::SetupBrowseRequest = serde_json::from_value(
                    request
                        .json_body()
                        .map_err(|_| OcgError::config("invalid setup browse body"))?,
                )
                .map_err(|error| OcgError::config(format!("invalid setup browse: {error}")))?;

                let path = body
                    .path
                    .as_ref()
                    .map(Path::new)
                    .unwrap_or_else(|| Path::new(""));

                let listing = crate::setup::browse_directory(path)?;

                Ok(serde_json::to_value(crate::contracts::SetupBrowseResponse {
                    current: listing.current,
                    parent: listing.parent,
                    entries: listing
                        .entries
                        .into_iter()
                        .map(|e| crate::contracts::SetupDirectoryEntry {
                            name: e.name,
                            path: e.path,
                            is_dir: e.is_dir,
                        })
                        .collect(),
                })
                .map_err(|error| OcgError::config(error.to_string()))?)
            }

            Route::SetupProjectInit => {
                let body: crate::contracts::SetupProjectRequest = serde_json::from_value(
                    request
                        .json_body()
                        .map_err(|_| OcgError::config("invalid setup project body"))?,
                )
                .map_err(|error| OcgError::config(format!("invalid setup project: {error}")))?;

                let root = Path::new(&body.root);

                // Initialize the .ocg marker if it doesn't exist
                let canonical_root = crate::setup::initialize_project_marker(root)?;

                let canonical = canonical
                    .ok_or_else(|| OcgError::config("Project registry is not available"))?;

                let now = now_unix();
                let response =
                    canonical.register_project(&body.command_id, &canonical_root, now)?;

                // Extract project name from root path
                let name = canonical_root
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_else(|| "Project".to_string());

                Ok(
                    serde_json::to_value(crate::contracts::SetupProjectResponse {
                        api_version:
                            crate::orchestration::canonical_control::CANONICAL_CONTROL_API_VERSION
                                .to_string(),
                        project_id: response.project.project_id,
                        name,
                        root: canonical_root.display().to_string(),
                    })
                    .map_err(|error| OcgError::config(error.to_string()))?,
                )
            }

            _ => return Ok(None),
        }?;
        Ok(Some(value))
    };

    let origin = allowed_cors_origin(request);
    match operation() {
        Ok(Some(value)) => write_json_with_origin(stream, 200, &value, origin.as_deref()),
        Ok(None) => write_api_error(
            stream,
            &ApiError::new(404, "not_found", "no such control route"),
        ),
        Err(error) => write_json_with_origin(
            stream,
            400,
            &ApiError::new(400, "invalid_setup", error.to_string()).to_json(),
            origin.as_deref(),
        ),
    }
}

/// Dispatch backend-backed canonical control commands. A command id is
/// required for every mutation, and the acknowledgement carries it unchanged.
fn handle_canonical(
    stream: &mut TcpStream,
    service: &crate::orchestration::canonical_control::CanonicalControlService,
    route: &Route,
    request: &Request,
) -> Option<std::io::Result<()>> {
    use crate::orchestration::canonical_control::{
        CanonicalEventTail, GlobalConfiguration, CANONICAL_CONTROL_API_VERSION,
    };
    let allowed_origin = allowed_cors_origin(request);
    let respond = |stream: &mut TcpStream, result: Result<Value>| match result {
        Ok(value) => write_json_with_origin(stream, 200, &value, allowed_origin.as_deref()),
        Err(error) => {
            let error = api_error_for(&error);
            write_json_with_origin(stream, error.status, &error.body, allowed_origin.as_deref())
        }
    };
    let query = |key: &str| -> Result<String> {
        request
            .query
            .get(key)
            .cloned()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| OcgError::config(format!("{key} is required")))
    };
    let body = || -> Result<Value> {
        request.json_body().map_err(|error| {
            let message = error
                .body
                .get("error")
                .and_then(Value::as_object)
                .and_then(|inner| inner.get("message"))
                .and_then(Value::as_str)
                .unwrap_or("invalid request body")
                .to_string();
            OcgError::config(message)
        })
    };
    let command_id = |body: &Value| -> Result<String> {
        body.get("command_id")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .ok_or_else(|| OcgError::config("command_id is required"))
    };
    // The operation is the single decision of which canonical routes this
    // function answers: `Ok(None)` is returned for every route it does not own.
    let operation = || -> Result<Option<Value>> {
        let now = now_unix();
        // Every canonical answer is built from a declared response struct in
        // `src/contracts.rs`, not an anonymous literal. The generated
        // TypeScript describes these structs, so the PWA's contract is a
        // projection of what the server actually sends.
        macro_rules! answer {
            ($value:expr) => {
                serde_json::to_value($value).map_err(|error| OcgError::config(error.to_string()))
            };
        }
        let value = match route {
            Route::CanonicalProjects => answer!(CanonicalProjectsResponse {
                api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
                projects: service
                    .projects()?
                    .into_iter()
                    .filter(|project| {
                        request.identity.as_ref().is_none_or(|identity| {
                            request.ownership.as_ref().is_some_and(|ownership| {
                                ownership
                                    .owns(&identity.user_id, &project.project_id)
                                    .unwrap_or(false)
                            })
                        })
                    })
                    .collect(),
            }),
            Route::CanonicalProjectImport => {
                let body = body()?;
                let id = command_id(&body)?;
                let root = body
                    .get("root")
                    .and_then(Value::as_str)
                    .ok_or_else(|| OcgError::config("root is required"))?;
                answer!(service.register_project(&id, Path::new(root), now)?)
            }
            Route::CanonicalProjectsGet { project } => answer!(CanonicalConfigurationEnvelope {
                api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
                configuration: service.configuration(project)?,
            }),
            Route::CanonicalConfigurationGet => {
                let project = query("project_id")?;
                answer!(CanonicalConfigurationEnvelope {
                    api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
                    configuration: service.configuration(&project)?,
                })
            }
            Route::CanonicalConfigurationPut => {
                let body = body()?;
                let id = command_id(&body)?;
                let config: GlobalConfiguration = serde_json::from_value(
                    body.get("configuration")
                        .cloned()
                        .ok_or_else(|| OcgError::config("configuration is required"))?,
                )
                .map_err(|error| OcgError::config(error.to_string()))?;
                answer!(service.set_global_configuration(&id, config, now)?)
            }
            Route::CanonicalConfigurationProjectPut { project } => {
                let body = body()?;
                let id = command_id(&body)?;
                let defaults = body
                    .get("defaults")
                    .cloned()
                    .ok_or_else(|| OcgError::config("defaults are required"))?;
                answer!(service.set_project_defaults(&id, project, defaults, now)?)
            }
            Route::CanonicalJobConfigGet { job } => {
                let stored = service.job_configuration(job)?;
                let (configuration, revision) = stored.unwrap_or((Value::Null, 0));
                answer!(CanonicalJobConfigEnvelope {
                    api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
                    job_id: job.to_string(),
                    configuration,
                    revision,
                })
            }
            Route::CanonicalJobConfigPut { job } => {
                let body = body()?;
                let id = command_id(&body)?;
                let config = body
                    .get("configuration")
                    .cloned()
                    .ok_or_else(|| OcgError::config("configuration is required"))?;
                answer!(service.set_job_configuration(&id, job, config, now)?)
            }
            Route::CanonicalJobLaunch => {
                let body = body()?;
                let request: crate::contracts::JobLaunchRequest = serde_json::from_value(body)
                    .map_err(|error| OcgError::config(error.to_string()))?;
                answer!(service.launch_job(request, now)?)
            }
            Route::HealthProbeLaunch => {
                let body = body()?;
                let request: crate::contracts::HealthProbeRequest = serde_json::from_value(body)
                    .map_err(|error| OcgError::config(error.to_string()))?;
                answer!(service.launch_health_probe(request, now)?)
            }
            Route::HealthProbeQuery => {
                let target: crate::contracts::HealthProbeTarget = serde_json::from_value(json!({
                    "provider": query("provider")?,
                    "model": query("model")?,
                    "effort": request.query.get("effort").cloned(),
                }))
                .map_err(|error| OcgError::config(error.to_string()))?;
                answer!(service.health_probe(crate::contracts::HealthProbeQuery {
                    project_id: query("project_id")?,
                    target,
                })?)
            }
            Route::CanonicalJobSpawn { job } => {
                let request: crate::contracts::CanonicalJobSpawnRequest =
                    serde_json::from_value(body()?)
                        .map_err(|error| OcgError::config(error.to_string()))?;
                answer!(service.spawn_job(job, request)?)
            }
            Route::CanonicalJobCancel { job } | Route::CanonicalJobRetry { job } => {
                let request: crate::contracts::CanonicalJobOperationRequest =
                    serde_json::from_value(body()?)
                        .map_err(|error| OcgError::config(error.to_string()))?;
                if matches!(route, Route::CanonicalJobCancel { .. }) {
                    answer!(service.cancel_job(job, request.expected_generation)?)
                } else {
                    answer!(service.retry_job(job, request.expected_generation)?)
                }
            }
            Route::ChatSend => {
                let body = body()?;
                let request: crate::contracts::ChatSendRequest = serde_json::from_value(body)
                    .map_err(|error| OcgError::config(error.to_string()))?;
                answer!(service.launch_chat_with_images(
                    request.launch,
                    request.selection,
                    &request.image_ids,
                    now
                )?)
            }
            Route::ChatImageUpload => {
                let request: crate::contracts::ChatImageUploadRequest =
                    serde_json::from_value(body()?)
                        .map_err(|error| OcgError::config(error.to_string()))?;
                answer!(service.upload_chat_image(&request)?)
            }
            Route::ChatConversations => {
                answer!(service.chat_conversations(&query("project_id")?)?)
            }
            Route::ChatConversationDelete => {
                answer!(service
                    .delete_chat_conversation(&query("project_id")?, &query("session_id")?)?)
            }
            Route::ChatMessages => {
                answer!(service.chat_messages(&query("project_id")?, &query("session_id")?)?)
            }
            Route::ChatCancel => {
                let body = body()?;
                let session_id = body
                    .get("session_id")
                    .and_then(Value::as_str)
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| OcgError::config("session_id is required"))?;
                let cancelled = service.cancel_chat_in_project(
                    body.get("project_id").and_then(Value::as_str),
                    session_id,
                )?;
                answer!(
                    crate::orchestration::canonical_control::ChatCancelResponse {
                        api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
                        session_id: session_id.to_string(),
                        cancelled,
                    }
                )
            }
            Route::CanonicalSnapshot => {
                let project = query("project_id")?;
                let job = query("job_id")?;
                answer!(service.canonical_snapshot(&project, &job)?)
            }
            Route::ProjectUsage => {
                let window = request
                    .query
                    .get("window")
                    .map(String::as_str)
                    .unwrap_or("all");
                let window = serde_json::from_value::<crate::contracts::UsageWindow>(
                    Value::String(window.to_string()),
                )
                .map_err(|_| OcgError::config("invalid usage window"))?;
                answer!(service.project_usage(&query("project_id")?, window)?)
            }
            Route::ConversationUsage => {
                answer!(service.conversation_usage(&query("project_id")?, &query("session_id")?)?)
            }
            Route::JobUsage => {
                answer!(service.job_usage(&query("project_id")?, &query("job_id")?)?)
            }
            Route::CanonicalDashboard => {
                let project = query("project_id")?;
                let job = request.query.get("job_id").map(String::as_str);
                answer!(service.dashboard(&project, job)?)
            }
            _ => return Ok(None),
        }?;
        Ok(Some(value))
    };
    if matches!(route, Route::ChatImageUpload | Route::ChatImageGet { .. })
        && request.headers.contains_key("origin")
        && allowed_origin.is_none()
    {
        return Some(write_api_error(
            stream,
            &ApiError::new(
                403,
                "origin_refused",
                "Chat images accept only loopback browser origins",
            ),
        ));
    }
    if let Route::ChatImageGet { project, image } = route {
        return Some(match service.read_chat_image(project, image) {
            Ok((media_type, bytes)) => {
                let mut headers = vec![
                    ("Cache-Control", "private, no-store".to_string()),
                    ("X-Content-Type-Options", "nosniff".to_string()),
                ];
                if let Some(origin) = allowed_origin.as_ref() {
                    headers.push(("Access-Control-Allow-Origin", origin.clone()));
                }
                write_response(stream, 200, &media_type, &bytes, &headers)
            }
            Err(error) => write_api_error(
                stream,
                &ApiError::new(404, "image_unavailable", error.to_string()),
            ),
        });
    }
    // The chat tail streams live provider events as SSE. The durable Call
    // remains the execution authority; this channel is transport observability
    // only.
    if let Route::ChatStream = route {
        return Some(handle_chat_stream(stream, service, request));
    }
    // The event tail is answered outside the generic operation above, because a
    // resume position the journal can no longer serve is not a bad request. It
    // gets its own status and code so the client can distinguish "refetch the
    // snapshot" from "your request was malformed" and from "nothing is new".
    if let Route::CanonicalEvents = route {
        let (project, job, after) = match (
            request.query.get("project_id").cloned(),
            request.query.get("job_id").cloned(),
            request
                .query
                .get("after")
                .and_then(|raw| raw.parse::<u64>().ok())
                .unwrap_or(0),
        ) {
            (Some(project), Some(job), after) if !project.is_empty() && !job.is_empty() => {
                (project, job, after)
            }
            _ => {
                return Some(write_api_error(
                    stream,
                    &ApiError::new(400, "invalid_request", "project_id and job_id are required"),
                ))
            }
        };
        let outcome = match service.canonical_event_tail(&project, &job, after) {
            Ok(outcome) => outcome,
            Err(error) => {
                return Some(write_api_error(
                    stream,
                    &ApiError::new(400, "invalid_request", error.to_string()),
                ))
            }
        };
        let answer = match outcome {
            CanonicalEventTail::Events(events) => CanonicalEventsEnvelope {
                api_version: CANONICAL_CONTROL_API_VERSION.to_string(),
                project_id: project,
                job_id: job,
                events,
            },
            CanonicalEventTail::ResyncRequired {
                requested,
                floor_cursor,
                head_cursor,
            } => {
                return Some(write_api_error(
                    stream,
                    &ApiError::new(
                        409,
                        "resync_required",
                        format!(
                            "cursor {requested} is below the retained execution journal floor \
                             {floor_cursor} (head {head_cursor}); refetch the canonical snapshot \
                             and continue from its cursor"
                        ),
                    ),
                ))
            }
            CanonicalEventTail::InvalidCursor {
                requested,
                head_cursor,
            } => {
                return Some(write_api_error(
                    stream,
                    &ApiError::new(
                        400,
                        "invalid_cursor",
                        format!(
                            "cursor {requested} is ahead of the execution journal head \
                             {head_cursor}"
                        ),
                    ),
                ))
            }
        };
        return Some(respond(
            stream,
            serde_json::to_value(answer).map_err(|error| OcgError::config(error.to_string())),
        ));
    }
    operation()
        .transpose()
        .map(|result| respond(stream, result))
}

/// Stream one live chat turn as SSE from the real provider path.
///
/// The durable Call owns execution; this channel only carries normalized
/// provider deltas for transport observability. Text deltas are forwarded,
/// terminal `Finished`/`Failed` close the stream, and heartbeats keep the
/// loopback socket inside its write timeout.
fn handle_chat_stream(
    stream: &mut TcpStream,
    service: &crate::orchestration::canonical_control::CanonicalControlService,
    request: &Request,
) -> std::io::Result<()> {
    let (session_id, job_id) = match (
        request.query.get("session_id").cloned(),
        request.query.get("job_id").cloned(),
    ) {
        (Some(session), Some(job)) if !session.is_empty() && !job.is_empty() => (session, job),
        _ => {
            return write_api_error(
                stream,
                &ApiError::new(400, "invalid_request", "session_id and job_id are required"),
            )
        }
    };
    let Some((buffer, started_at)) = service.chat_buffer_for(&session_id, &job_id) else {
        return write_api_error(
            stream,
            &ApiError::new(404, "unknown_chat", "chat stream unavailable"),
        );
    };
    let origin = allowed_cors_origin(request);
    let mut head = String::from(
        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-store\r\nConnection: close\r\n",
    );
    if let Some(origin) = origin.as_deref() {
        head.push_str(&format!(
            "Access-Control-Allow-Origin: {origin}\r\nVary: Origin\r\n"
        ));
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes())?;
    stream.flush()?;

    // The execution deadline belongs to the turn, not to this connection:
    // `started_at` was fixed when the ActiveChat was created, so every
    // attach and reconnect shares the same deadline and a reconnect can
    // never extend the execution lifetime.
    let overall = crate::orchestration::canonical_control::CHAT_EXECUTION_TIMEOUT;
    let deadline = started_at + overall;
    let mut last_heartbeat = std::time::Instant::now();
    let heartbeat = Duration::from_secs(10);
    let poll = Duration::from_millis(200);
    // Replay what arrived before attach, then go live. The provider body is
    // never buffered whole: buffered deltas are forwarded immediately in
    // order, and the tail continues incrementally. A reconnecting browser
    // sends the standard `Last-Event-ID`; each buffered event has a stable
    // append-only sequence (its position + 1), so replay resumes exactly at
    // the first unconsumed event and never duplicates a delivered delta.
    let resume_after: u64 = request
        .headers
        .get("last-event-id")
        .and_then(|raw| raw.trim().parse().ok())
        .unwrap_or(0);
    let mut index = resume_after as usize;
    {
        let guard = buffer
            .state
            .lock()
            .map_err(|_| std::io::Error::other("chat buffer poisoned"))?;
        if index > guard.events.len() {
            index = guard.events.len();
        }
    }
    let mut finished = false;
    let mut disconnected = false;
    while !finished {
        if let Some(identity) = &request.identity {
            let authorized = request.ownership.as_ref().is_some_and(|ownership| {
                identity.expires_at > now_unix()
                    && ownership
                        .revoked(&identity.session_id)
                        .is_ok_and(|revoked| !revoked)
                    && service
                        .chat_project_for(&session_id, &job_id)
                        .is_some_and(|project| {
                            ownership.owns(&identity.user_id, &project).unwrap_or(false)
                        })
            });
            if !authorized {
                stream.write_all(b"event: authentication_required\ndata: {}\n\n")?;
                stream.flush()?;
                disconnected = true;
                break;
            }
        }
        if std::time::Instant::now() >= deadline {
            let payload = json!({"error": "chat stream timed out"}).to_string();
            let _ = stream.write_all(format!("data: {payload}\n\n").as_bytes());
            let _ = stream.flush();
            // The timeout must enter the canonical cancellation path so the
            // Attempt/Call/provider execution is actually revoked. It runs
            // even when the frontend socket write above failed, and it precedes
            // any ActiveChat removal so the cancel state is still findable.
            let _ = service.cancel_chat_turn(&session_id, &job_id);
            break;
        }
        let (base_index, pending): (
            usize,
            Vec<crate::orchestration::execution_dispatch::ExecutionEvent>,
        ) = {
            let guard = buffer
                .state
                .lock()
                .map_err(|_| std::io::Error::other("chat buffer poisoned"))?;
            if index < guard.events.len() {
                let base = index;
                let pending = guard.events[index..].to_vec();
                index = guard.events.len();
                (base, pending)
            } else {
                if guard.terminal {
                    break;
                }
                drop(guard);
                let guard = buffer
                    .cvar
                    .wait_timeout(
                        buffer
                            .state
                            .lock()
                            .map_err(|_| std::io::Error::other("chat buffer poisoned"))?,
                        poll,
                    )
                    .map_err(|_| std::io::Error::other("chat buffer poisoned"))?
                    .0;
                if index < guard.events.len() {
                    let base = index;
                    let pending = guard.events[index..].to_vec();
                    index = guard.events.len();
                    (base, pending)
                } else {
                    if guard.terminal {
                        break;
                    }
                    drop(guard);
                    if last_heartbeat.elapsed() >= heartbeat {
                        if stream.write_all(b": ping\n\n").is_err() {
                            disconnected = true;
                            break;
                        }
                        if stream.flush().is_err() {
                            disconnected = true;
                            break;
                        }
                        last_heartbeat = std::time::Instant::now();
                    }
                    continue;
                }
            }
        };
        for (offset, event) in pending.into_iter().enumerate() {
            // Every buffered event carries its stable sequence as the SSE
            // `id:`; reconnects resume from the next unconsumed sequence.
            let seq = base_index as u64 + offset as u64 + 1;
            use crate::orchestration::execution_dispatch::ExecutionEvent as Live;
            match event {
                Live::Started => {}
                Live::Provider(provider_event) => {
                    use crate::openai_compatible::stream::ChatStreamEvent as Stream;
                    let payload = match provider_event {
                        Stream::Image { url } => Some(
                            match service.receive_chat_image_for_turn(&session_id, &job_id, &url) {
                                Ok(image) => json!({"image": image}),
                                Err(error) => json!({"error": error.to_string()}),
                            }
                            .to_string(),
                        ),
                        Stream::TextDelta { delta } => Some(json!({"delta": delta}).to_string()),
                        Stream::ReasoningDelta { delta } => {
                            Some(json!({"reasoning": delta}).to_string())
                        }
                        // The round boundaries carry no provider content. They are
                        // ordered like every other buffered event, so a
                        // reconnecting EventSource replays a reset before the
                        // replacement deltas that follow it.
                        Stream::RoundBegan => Some(json!({"round_begin": true}).to_string()),
                        Stream::RoundReset => Some(json!({"round_reset": true}).to_string()),
                        Stream::ToolCallStart { .. }
                        | Stream::ToolCallArgumentsDelta { .. }
                        | Stream::ToolCallComplete { .. }
                        | Stream::Metadata { .. }
                        | Stream::Finish { .. } => None,
                        Stream::Error(error) => {
                            Some(json!({"error": error.to_string()}).to_string())
                        }
                    };
                    if let Some(payload) = payload {
                        if stream
                            .write_all(format!("id: {seq}\ndata: {payload}\n\n").as_bytes())
                            .is_err()
                        {
                            disconnected = true;
                            finished = true;
                            break;
                        }
                        if stream.flush().is_err() {
                            disconnected = true;
                            finished = true;
                            break;
                        }
                        // A provider transport error terminates the tail.
                        if payload.contains("\"error\"") {
                            finished = true;
                            break;
                        }
                    }
                }
                Live::Failed(message) => {
                    let payload = json!({"error": message}).to_string();
                    if stream
                        .write_all(format!("id: {seq}\ndata: {payload}\n\n").as_bytes())
                        .is_err()
                    {
                        // A failed terminal write is a disconnect: keep the
                        // retained buffer for a later replay.
                        disconnected = true;
                        finished = true;
                        break;
                    }
                    if stream.flush().is_err() {
                        disconnected = true;
                        finished = true;
                        break;
                    }
                    finished = true;
                    break;
                }
                Live::Finished => {
                    let payload = json!({"done": true}).to_string();
                    if stream
                        .write_all(format!("id: {seq}\ndata: {payload}\n\n").as_bytes())
                        .is_err()
                    {
                        disconnected = true;
                        finished = true;
                        break;
                    }
                    if stream.flush().is_err() {
                        disconnected = true;
                        finished = true;
                        break;
                    }
                    finished = true;
                    break;
                }
            }
        }
    }
    // A broken socket keeps the retained tail for a retry; only a consumed
    // terminal (or timeout/error sent above) removes the entry.
    if !disconnected {
        service.finish_chat(&session_id, &job_id, &buffer);
    }
    let _ = stream.flush();
    Ok(())
}

fn write_api_error(stream: &mut TcpStream, error: &ApiError) -> std::io::Result<()> {
    let body = serde_json::to_vec(&error.to_json()).unwrap_or_else(|_| b"{}".to_vec());
    let extra: Vec<(&str, String)> = error
        .allow
        .map(|allow| vec![("Allow", allow.to_string())])
        .unwrap_or_default();
    write_response(stream, error.status, "application/json", &body, &extra)
}

fn write_response(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &[u8],
    extra: &[(&str, String)],
) -> std::io::Result<()> {
    let mut head = format!(
        "HTTP/1.1 {status} {}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n\
         Cache-Control: no-store\r\nConnection: close\r\n",
        reason(status),
        body.len()
    );
    for (name, value) in extra {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes())?;
    stream.write_all(body)?;
    stream.flush()
}

fn read_request(stream: &mut TcpStream) -> std::result::Result<Request, ApiError> {
    let mut buffer: Vec<u8> = Vec::with_capacity(1024);
    let head_end = loop {
        if let Some(index) = find_head_end(&buffer) {
            if index > MAX_HEADER_BYTES {
                return Err(ApiError::new(
                    431,
                    "headers_too_large",
                    format!("the request head exceeds {MAX_HEADER_BYTES} bytes"),
                ));
            }
            break index;
        }
        if buffer.len() > MAX_HEADER_BYTES {
            return Err(ApiError::new(
                431,
                "headers_too_large",
                format!("the request head exceeds {MAX_HEADER_BYTES} bytes"),
            ));
        }
        let mut chunk = [0u8; 4096];
        let read = stream.read(&mut chunk).map_err(|_| {
            ApiError::new(400, "malformed_request", "the request could not be read")
        })?;
        if read == 0 {
            return Err(ApiError::new(
                400,
                "malformed_request",
                "the connection closed before the request head was complete",
            ));
        }
        buffer.extend_from_slice(&chunk[..read]);
        if buffer.len() > MAX_HEADER_BYTES + MAX_BODY_BYTES {
            return Err(ApiError::new(
                413,
                "payload_too_large",
                "the request is larger than the accepted maximum",
            ));
        }
    };

    let head = std::str::from_utf8(&buffer[..head_end]).map_err(|_| {
        ApiError::new(
            400,
            "malformed_request",
            "the request head is not valid UTF-8",
        )
    })?;
    let mut lines = head.split("\r\n");
    let request_line = lines
        .next()
        .ok_or_else(|| ApiError::new(400, "malformed_request", "the request line is missing"))?;
    let mut parts = request_line.split(' ');
    let method = parts.next().unwrap_or_default().to_ascii_uppercase();
    let target = parts.next().unwrap_or_default().to_string();
    let version = parts.next().unwrap_or_default();
    if parts.next().is_some() || target.is_empty() || version.is_empty() {
        return Err(ApiError::new(
            400,
            "malformed_request",
            "the request line is malformed",
        ));
    }
    if version != "HTTP/1.1" && version != "HTTP/1.0" {
        return Err(ApiError::new(
            505,
            "http_version_not_supported",
            "only HTTP/1.0 and HTTP/1.1 are supported",
        ));
    }
    // OPTIONS is accepted only so a browser preflight for the canonical
    // control surface can be answered; it is never a routable method.
    if !matches!(
        method.as_str(),
        "GET" | "POST" | "PUT" | "DELETE" | "OPTIONS"
    ) {
        return Err(ApiError::method_not_allowed(
            "GET, POST, PUT, DELETE, OPTIONS",
        ));
    }

    let mut headers = HashMap::new();
    let mut count = 0usize;
    for line in lines {
        if line.is_empty() {
            continue;
        }
        if line.starts_with(' ') || line.starts_with('\t') {
            return Err(ApiError::new(
                400,
                "malformed_request",
                "folded request headers are not accepted",
            ));
        }
        count += 1;
        if count > MAX_HEADERS {
            return Err(ApiError::new(
                431,
                "headers_too_large",
                format!("the request has more than {MAX_HEADERS} headers"),
            ));
        }
        let Some((name, value)) = line.split_once(':') else {
            return Err(ApiError::new(
                400,
                "malformed_request",
                "a request header is missing its colon",
            ));
        };
        let name = name.trim().to_ascii_lowercase();
        if name.is_empty()
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte))
        {
            return Err(ApiError::new(
                400,
                "malformed_request",
                "a request header has an invalid name",
            ));
        }
        if headers.insert(name, value.trim().to_string()).is_some() {
            return Err(ApiError::new(
                400,
                "malformed_request",
                "duplicate request headers are not accepted",
            ));
        }
    }

    if headers.contains_key("transfer-encoding") {
        return Err(ApiError::new(
            400,
            "unsupported_transfer_encoding",
            "chunked request bodies are not supported",
        ));
    }

    let content_length = match headers.get("content-length") {
        Some(value) => value.trim().parse::<usize>().map_err(|_| {
            ApiError::new(
                400,
                "malformed_request",
                "Content-Length must be a non-negative integer",
            )
        })?,
        None => 0,
    };
    let (path, query) = parse_target(&target)?;
    let max_body_bytes = if method == "POST" && path == "/api/v1/canonical/chat/images" {
        crate::chat_images::MAX_UPLOAD_BODY_BYTES
    } else {
        MAX_BODY_BYTES
    };
    if content_length > max_body_bytes {
        return Err(ApiError::new(
            413,
            "payload_too_large",
            format!("the request body exceeds the {max_body_bytes} byte limit"),
        ));
    }

    let body_start = head_end + 4;
    let mut body = buffer[body_start..].to_vec();
    if body.len() > content_length {
        return Err(ApiError::new(
            400,
            "malformed_request",
            "bytes after the declared request body are not accepted",
        ));
    }
    while body.len() < content_length {
        let remaining = content_length - body.len();
        let mut chunk = [0u8; 4096];
        let take = remaining.min(chunk.len());
        let read = stream.read(&mut chunk[..take]).map_err(|_| {
            ApiError::new(
                400,
                "malformed_request",
                "the request body could not be read",
            )
        })?;
        if read == 0 {
            return Err(ApiError::new(
                400,
                "malformed_request",
                "the request body ended before Content-Length bytes arrived",
            ));
        }
        body.extend_from_slice(&chunk[..read]);
    }

    Ok(Request {
        method,
        path,
        query,
        headers,
        body,
        approved_origin: None,
        identity: None,
        ownership: None,
    })
}

fn find_head_end(buffer: &[u8]) -> Option<usize> {
    buffer.windows(4).position(|window| window == b"\r\n\r\n")
}

fn parse_target(target: &str) -> std::result::Result<(String, HashMap<String, String>), ApiError> {
    let (raw_path, raw_query) = match target.split_once('?') {
        Some((path, query)) => (path, Some(query)),
        None => (target, None),
    };
    if !raw_path.starts_with('/') {
        return Err(ApiError::new(
            400,
            "malformed_request",
            "the request target must be an absolute path",
        ));
    }
    let path = percent_decode(raw_path);
    let mut query = HashMap::new();
    if let Some(raw_query) = raw_query {
        for pair in raw_query.split('&').filter(|pair| !pair.is_empty()) {
            let (key, value) = match pair.split_once('=') {
                Some((key, value)) => (percent_decode(key), percent_decode(value)),
                None => (percent_decode(pair), String::new()),
            };
            query.insert(key, value);
        }
    }
    Ok((path, query))
}

fn percent_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            let high = (bytes[index + 1] as char).to_digit(16);
            let low = (bytes[index + 2] as char).to_digit(16);
            if let (Some(high), Some(low)) = (high, low) {
                out.push((high * 16 + low) as u8);
                index += 3;
                continue;
            }
        }
        out.push(bytes[index]);
        index += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The classified route for a request.
enum Route {
    ProfileGet,
    ProfileBootstrap,
    ProfilePut,
    ProfileCredential,
    ProfilePreflight,
    // Setup / first-run
    SetupProviderConnect,
    SetupProviderRefresh,
    SetupModelsSave,
    SetupBrowse,
    SetupProjectInit,
    // Canonical Job/Attempt control surface (project import, configuration,
    // pre-attempt Job configuration, snapshots and the event tail).
    CanonicalProjects,
    CanonicalProjectImport,
    CanonicalProjectsGet {
        project: String,
    },
    CanonicalConfigurationGet,
    CanonicalConfigurationPut,
    CanonicalConfigurationProjectPut {
        project: String,
    },
    CanonicalJobConfigGet {
        job: String,
    },
    CanonicalJobConfigPut {
        job: String,
    },
    CanonicalJobLaunch,
    /// Health Probe: launch a probe Job for one Provider x Model x Effort tuple,
    /// and read the latest canonical evidence for one. Explicitly operator- and
    /// client-driven; there is no scheduled probing anywhere in this service.
    HealthProbeLaunch,
    HealthProbeQuery,
    CanonicalJobCancel {
        job: String,
    },
    CanonicalJobRetry {
        job: String,
    },
    CanonicalJobSpawn {
        job: String,
    },
    CanonicalSnapshot,
    CanonicalEvents,
    CanonicalDashboard,
    ProjectUsage,
    ConversationUsage,
    JobUsage,
    ChatImageUpload,
    ChatImageGet {
        project: String,
        image: String,
    },
    ChatSend,
    ChatConversations,
    ChatConversationDelete,
    ChatMessages,
    ChatStream,
    ChatCancel,
    CanonicalPreflight,
    Product {
        path: String,
    },
}

fn classify(request: &Request) -> std::result::Result<Route, ApiError> {
    let segments: Vec<&str> = request
        .path
        .trim_start_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();

    // The path's method table is the only method policy. OPTIONS is never
    // refused here: it is the CORS preflight, which every known path answers.
    if let Some(methods) = allowed_methods(&segments) {
        if request.method != "OPTIONS"
            && !methods.split(", ").any(|method| method == request.method)
        {
            return Err(ApiError::method_not_allowed(methods));
        }
    }

    match (request.method.as_str(), segments.as_slice()) {
        ("GET", ["api", "v1", "profile"]) => Ok(Route::ProfileGet),
        ("POST", ["api", "v1", "profile", "bootstrap"]) => Ok(Route::ProfileBootstrap),
        ("PUT", ["api", "v1", "profile"]) => Ok(Route::ProfilePut),
        ("POST", ["api", "v1", "profile", "credentials"]) => Ok(Route::ProfileCredential),
        ("OPTIONS", ["api", "v1", "profile", ..]) => Ok(Route::ProfilePreflight),
        // Setup routes
        ("POST", ["api", "v1", "setup", "connect"]) => Ok(Route::SetupProviderConnect),
        ("POST", ["api", "v1", "setup", "refresh"]) => Ok(Route::SetupProviderRefresh),
        ("POST", ["api", "v1", "setup", "models"]) => Ok(Route::SetupModelsSave),
        ("POST", ["api", "v1", "setup", "browse"]) => Ok(Route::SetupBrowse),
        ("POST", ["api", "v1", "setup", "project"]) => Ok(Route::SetupProjectInit),
        ("OPTIONS", ["api", "v1", "setup", ..]) => Ok(Route::ProfilePreflight),
        ("GET", ["api", "v1", "canonical", "projects"]) => Ok(Route::CanonicalProjects),
        ("POST", ["api", "v1", "canonical", "projects", "import"]) => {
            Ok(Route::CanonicalProjectImport)
        }
        ("GET", ["api", "v1", "canonical", "projects", project]) => {
            Ok(Route::CanonicalProjectsGet {
                project: safe_id(project)?,
            })
        }
        ("GET", ["api", "v1", "canonical", "configuration"]) => {
            Ok(Route::CanonicalConfigurationGet)
        }
        ("PUT", ["api", "v1", "canonical", "configuration"]) => {
            Ok(Route::CanonicalConfigurationPut)
        }
        ("PUT", ["api", "v1", "canonical", "configuration", "projects", project]) => {
            Ok(Route::CanonicalConfigurationProjectPut {
                project: safe_id(project)?,
            })
        }
        ("GET", ["api", "v1", "canonical", "jobs", job, "configuration"]) => {
            Ok(Route::CanonicalJobConfigGet { job: safe_id(job)? })
        }
        ("PUT", ["api", "v1", "canonical", "jobs", job, "configuration"]) => {
            Ok(Route::CanonicalJobConfigPut { job: safe_id(job)? })
        }
        ("POST", ["api", "v1", "canonical", "jobs", "launch"]) => Ok(Route::CanonicalJobLaunch),
        ("GET", ["api", "v1", "canonical", "jobs", "health-probe"]) => Ok(Route::HealthProbeQuery),
        ("POST", ["api", "v1", "canonical", "jobs", "health-probe"]) => {
            Ok(Route::HealthProbeLaunch)
        }
        ("POST", ["api", "v1", "canonical", "jobs", job, "spawn"]) => {
            Ok(Route::CanonicalJobSpawn { job: safe_id(job)? })
        }
        ("POST", ["api", "v1", "canonical", "jobs", job, "cancel"]) => {
            Ok(Route::CanonicalJobCancel { job: safe_id(job)? })
        }
        ("POST", ["api", "v1", "canonical", "jobs", job, "retry"]) => {
            Ok(Route::CanonicalJobRetry { job: safe_id(job)? })
        }
        ("GET", ["api", "v1", "canonical", "jobs"]) => Ok(Route::CanonicalSnapshot),
        ("GET", ["api", "v1", "canonical", "jobs", "events"]) => Ok(Route::CanonicalEvents),
        ("GET", ["api", "v1", "canonical", "usage"]) => Ok(Route::ProjectUsage),
        ("GET", ["api", "v1", "canonical", "chat", "usage"]) => Ok(Route::ConversationUsage),
        ("GET", ["api", "v1", "canonical", "jobs", "usage"]) => Ok(Route::JobUsage),
        ("GET", ["api", "v1", "canonical", "dashboard"]) => Ok(Route::CanonicalDashboard),
        ("POST", ["api", "v1", "canonical", "chat", "images"]) => Ok(Route::ChatImageUpload),
        ("GET", ["api", "v1", "canonical", "chat", "images", project, image]) => {
            Ok(Route::ChatImageGet {
                project: (*project).to_string(),
                image: (*image).to_string(),
            })
        }
        ("POST", ["api", "v1", "canonical", "chat", "send"]) => Ok(Route::ChatSend),
        ("GET", ["api", "v1", "canonical", "chat", "stream"]) => Ok(Route::ChatStream),
        ("GET", ["api", "v1", "canonical", "chat", "conversations"]) => {
            Ok(Route::ChatConversations)
        }
        ("DELETE", ["api", "v1", "canonical", "chat", "conversations"]) => {
            Ok(Route::ChatConversationDelete)
        }
        ("GET", ["api", "v1", "canonical", "chat", "messages"]) => Ok(Route::ChatMessages),
        ("POST", ["api", "v1", "canonical", "chat", "cancel"]) => Ok(Route::ChatCancel),
        ("OPTIONS", ["api", "v1", "canonical", ..]) => Ok(Route::CanonicalPreflight),
        ("GET", _) if !crate::ui_assets::is_control_path(&request.path) => Ok(Route::Product {
            path: request.path.clone(),
        }),
        _ => Err(ApiError::new(404, "not_found", "no such control route")),
    }
}

fn serve_product(stream: &mut TcpStream, request: &Request, path: &str) -> std::io::Result<()> {
    if request.method != "GET" {
        return write_api_error(stream, &ApiError::method_not_allowed("GET"));
    }
    if !crate::ui_assets::is_packaged() {
        return write_api_error(
            stream,
            &ApiError::new(
                503,
                "ui_not_packaged",
                format!(
                    "the OCG product UI is not embedded in this binary: {}",
                    crate::ui_assets::build_note()
                ),
            ),
        );
    }
    let Some(asset) = crate::ui_assets::resolve(path) else {
        return write_api_error(
            stream,
            &ApiError::new(404, "not_found", "no such product asset"),
        );
    };
    let cache = if crate::ui_assets::is_immutable_asset(asset.path) {
        "public, max-age=31536000, immutable".to_string()
    } else {
        "no-cache".to_string()
    };
    let headers = vec![
        ("Cache-Control", cache),
        (
            "X-OCG-UI-Version",
            crate::ui_assets::ui_version().to_string(),
        ),
        (
            "X-OCG-Backend-Version",
            crate::ui_assets::backend_version().to_string(),
        ),
    ];
    if asset.body.len() > MAX_RESPONSE_BYTES {
        return write_api_error(
            stream,
            &ApiError::new(
                503,
                "asset_too_large",
                "the UI asset exceeds the response limit",
            ),
        );
    }
    write_response(stream, 200, asset.content_type, asset.body, &headers)
}

fn allowed_methods(segments: &[&str]) -> Option<&'static str> {
    match segments {
        ["api", "v1", "profile"] => Some("GET, PUT"),
        ["api", "v1", "profile", "bootstrap"] => Some("POST"),
        ["api", "v1", "profile", "credentials"] => Some("POST"),
        ["api", "v1", "setup", "connect"] => Some("POST"),
        ["api", "v1", "setup", "models"] => Some("POST"),
        ["api", "v1", "setup", "browse"] => Some("POST"),
        ["api", "v1", "setup", "project"] => Some("POST"),
        ["api", "v1", "canonical", "configuration"] => Some("GET, PUT"),
        ["api", "v1", "canonical", "configuration", "projects", _] => Some("PUT"),
        ["api", "v1", "canonical", "jobs", _, "configuration"] => Some("GET, PUT"),
        ["api", "v1", "canonical", "jobs"]
        | ["api", "v1", "canonical", "jobs", "events"]
        | ["api", "v1", "canonical", "dashboard"]
        | ["api", "v1", "canonical", "usage"]
        | ["api", "v1", "canonical", "jobs", "usage"]
        | ["api", "v1", "canonical", "chat", "usage"]
        | ["api", "v1", "canonical", "chat", "stream"]
        | ["api", "v1", "canonical", "chat", "messages"]
        | ["api", "v1", "canonical", "projects"] => Some("GET"),
        ["api", "v1", "canonical", "chat", "conversations"] => Some("GET, DELETE"),
        ["api", "v1", "canonical", "projects", "import"] => Some("POST"),
        ["api", "v1", "canonical", "chat", "images", _, _] => Some("GET"),
        ["api", "v1", "canonical", "chat", "images"] => Some("POST"),
        ["api", "v1", "canonical", "jobs", _, "cancel" | "retry" | "spawn"] => Some("POST"),
        ["api", "v1", "canonical", "jobs", "health-probe"] => Some("GET, POST"),
        ["api", "v1", "canonical", "jobs", "launch"]
        | ["api", "v1", "canonical", "chat", "send"]
        | ["api", "v1", "canonical", "chat", "cancel"] => Some("POST"),
        ["api", "v1", "canonical", "projects", _] => Some("GET"),
        _ => None,
    }
}

fn safe_id(value: &str) -> std::result::Result<String, ApiError> {
    if !is_safe_id(value) {
        return Err(ApiError::new(
            400,
            "invalid_id",
            format!("'{value}' is not a safe identifier"),
        ));
    }
    Ok(value.to_string())
}

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or(0)
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        303 => "See Other",
        401 => "Unauthorized",
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        410 => "Gone",
        411 => "Length Required",
        413 => "Payload Too Large",
        422 => "Unprocessable Entity",
        431 => "Request Header Fields Too Large",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        505 => "HTTP Version Not Supported",
        _ => "Error",
    }
}
