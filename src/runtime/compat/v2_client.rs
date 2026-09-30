//! OpenCode 2.x local session transport.
//!
//! [`V2SessionClient`] is the first concrete runtime lifecycle adapter. It
//! implements the runtime-neutral contract in
//! [`crate::runtime::lifecycle::RuntimeAdapter`] while retaining the existing
//! OpenCode compatibility traits for launch/effective-state callers. Every
//! HTTP route, JSON shape and discovery detail lives here, so Job-facing
//! orchestration does not parse V2 responses.
//!
//! The routes below were read from the running OpenCode 2.0.x server's own
//! OpenAPI document (`GET /openapi.json`) and exercised live; they are not
//! inferred:
//!
//! ```text
//! POST /api/session                          {location:{directory}}          -> {data:{id}}
//! GET  /api/session?directory=&limit=1       (newest root sessions first)   -> {data:[{id},...], cursor}
//! POST /api/session/{id}/agent               {agent}                        -> 204
//! POST /api/session/{id}/model               {model:{id, providerID[, variant]}} -> 204
//! GET  /api/session/{id}                     -> {data:{id[, parentID], agent, model:{id, providerID[, variant]}, ...}}
//! GET  /api/session/{id}/context             -> {data:[message records]}
//! GET  /api/model?directory=...               -> {data:[{providerID,id,limit:{context,input,output}}]}
//! POST /api/session/{id}/synthetic            {id,text,description,metadata,resume} -> {data:{id}}
//! ```
//!
//! The variant key is written **only** when the Rust-resolved contract carries
//! one; a `None` variant omits it completely rather than sending `null`, an
//! empty string or a fabricated default.
//!
//! Discovery can use OpenCode's own background-service registration at
//! `$XDG_STATE_HOME/opencode/service.json` (default
//! `~/.local/state/opencode/service.json`), which carries the loopback URL and
//! the local Basic-auth password. That password is a local service credential:
//! it is held in a [`Secret`] and never rendered, logged or attached to an
//! error. OCG never reads OpenCode's provider credential store (`auth.json`).
//!
//! Production launch creates an invocation-scoped `opencode serve` child and
//! passes its ephemeral registration directly to this client. Discovery remains
//! available for diagnostics, but production never restarts or reconfigures a
//! user's background service.

use crate::error::{OcgError, Result};
use crate::http::Secret;
use crate::runtime::compat::{
    select_existing_session_lead, BridgeRuntimeClient, EffectiveLead, LeadSelection, SessionClient,
    SessionLifecycleClient,
};
use crate::runtime::lifecycle::{
    RuntimeAdapter, RuntimeCapabilities, RuntimeContextEvent, RuntimeContextObservation,
    RuntimeContextUsage, RuntimeContinuation, RuntimeError, RuntimeErrorKind, RuntimeExecution,
    RuntimeExecutionId, RuntimeIdentity, RuntimeModelMetadata, RuntimeProfile, RuntimeProvenance,
    RuntimeRecoveryKey, RuntimeResult,
};
use serde_json::{json, Map, Value};
use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// The username OpenCode's local daemon expects for its Basic-auth challenge.
/// Only the password is per-service; the username is fixed by the runtime.
const LOCAL_SERVICE_USERNAME: &str = "opencode";

/// The default request timeout for a local daemon call.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// A running OpenCode 2 background service, as registered by OpenCode itself.
///
/// The registration file is OpenCode's supported discovery mechanism and also
/// carries the loopback Basic-auth password. That password is a credential and
/// is kept in a [`Secret`]: it can only be reached through
/// [`ServiceRegistration::password`] and is redacted from every rendering.
#[derive(Clone)]
pub struct ServiceRegistration {
    url: String,
    password: Secret,
}

impl ServiceRegistration {
    pub fn new(url: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            url: normalize_base_url(url.into()),
            password: Secret::new(password),
        }
    }

    /// The loopback base URL (no trailing slash).
    pub fn url(&self) -> &str {
        &self.url
    }

    /// The local service password. Never log, print or persist the result.
    pub fn password(&self) -> &Secret {
        &self.password
    }

    /// Parse a registration document. Missing or empty `url`/`password` is a
    /// deterministic error; the raw document is never echoed.
    pub fn from_json(text: &str) -> Result<Self> {
        let value: Value = serde_json::from_str(text).map_err(|error| {
            OcgError::config(format!(
                "OpenCode service registration is not valid JSON: {error}"
            ))
        })?;
        let url = value
            .get("url")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim();
        let password = value.get("password").and_then(Value::as_str).unwrap_or("");
        if url.is_empty() || password.is_empty() {
            return Err(OcgError::config(
                "OpenCode service registration is missing the local service url or password",
            ));
        }
        Ok(Self::new(url, password))
    }

    /// Read a registration file.
    pub fn from_file(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path).map_err(|error| OcgError::read(path, error))?;
        Self::from_json(&text)
    }

    /// Discover the running OpenCode 2 service through its own registration
    /// file. A missing file means "no service is running", not a parse problem.
    pub fn discover() -> Result<Self> {
        let base = directories::BaseDirs::new().ok_or_else(|| {
            OcgError::config("cannot determine the home directory for OpenCode service discovery")
        })?;
        let state_home = base.state_dir().map(Path::to_path_buf);
        let path = service_registration_path(state_home.as_deref(), base.home_dir());
        if !path.is_file() {
            return Err(OcgError::config(format!(
                "no running OpenCode V2 service registration was found at {}; start it with `opencode service start`",
                path.display()
            )));
        }
        Self::from_file(&path)
    }
}

impl fmt::Debug for ServiceRegistration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServiceRegistration")
            .field("url", &"<redacted>")
            .field("password", &"<redacted>")
            .finish()
    }
}

/// Resolve the registration path from an optional state directory and a home
/// directory. Pure so the layout is tested without touching the environment.
fn service_registration_path(state_home: Option<&Path>, home: &Path) -> PathBuf {
    let state = state_home
        .map(Path::to_path_buf)
        .unwrap_or_else(|| home.join(".local").join("state"));
    state.join("opencode").join("service.json")
}

fn normalize_base_url(url: String) -> String {
    url.trim().trim_end_matches('/').to_string()
}

/// One request against the local daemon.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V2Request {
    pub method: &'static str,
    pub url: String,
    /// `None` sends an empty body; the body never contains credentials.
    pub body: Option<Value>,
}

/// One raw response from the local daemon.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V2Response {
    pub status: u16,
    pub body: String,
}

/// The result of one readiness observation. See
/// [`V2SessionClient::readiness`] for the exact mapping from HTTP to state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum V2Readiness {
    /// The runtime answered the capability probe successfully.
    Ready,
    /// The runtime is not serving the API yet (or not yet reachable).
    Starting,
    /// The runtime answered but cannot serve OCG: retrying cannot help.
    Unusable(String),
}

/// Poll `probe` until it reports [`V2Readiness::Ready`].
///
/// `pause` runs only *between* attempts, so a runtime that is already ready is
/// never delayed by a fixed sleep. `attempts` bounds the wait, so this can
/// neither poll forever nor accept a runtime that never becomes ready. The
/// first [`V2Readiness::Unusable`] observation fails immediately: a rejected
/// credential or a wrong route is not a startup race.
pub fn wait_ready<F, P>(mut probe: F, mut pause: P, attempts: u32) -> Result<()>
where
    F: FnMut() -> V2Readiness,
    P: FnMut(),
{
    let attempts = attempts.max(1);
    for attempt in 0..attempts {
        match probe() {
            V2Readiness::Ready => return Ok(()),
            V2Readiness::Unusable(reason) => {
                return Err(OcgError::config(format!(
                    "the OpenCode V2 runtime is reachable but cannot serve OCG: {reason}"
                )))
            }
            V2Readiness::Starting => {
                if attempt + 1 < attempts {
                    pause();
                }
            }
        }
    }
    Err(OcgError::config(format!(
        "the OpenCode V2 runtime did not become ready after {attempts} bounded readiness probe(s)"
    )))
}

/// The transport seam. Production binds [`ReqwestV2Transport`]; tests bind a
/// deterministic in-memory fake, exactly like the other runtime boundaries.
pub trait V2Transport: Send + Sync {
    fn send(&self, request: V2Request) -> Result<V2Response>;
}

/// The production transport: a blocking `reqwest` client that authenticates
/// with the local service password and never uses an ambient proxy.
pub struct ReqwestV2Transport {
    client: reqwest::blocking::Client,
    password: Secret,
}

impl ReqwestV2Transport {
    pub fn new(registration: &ServiceRegistration) -> Result<Self> {
        let client = reqwest::blocking::Client::builder()
            .user_agent(concat!("ocg/", env!("CARGO_PKG_VERSION")))
            .timeout(REQUEST_TIMEOUT)
            .no_proxy()
            .build()
            .map_err(|error| {
                OcgError::config(format!(
                    "cannot build the OpenCode V2 session client: {error}"
                ))
            })?;
        Ok(Self {
            client,
            password: registration.password().clone(),
        })
    }
}

impl V2Transport for ReqwestV2Transport {
    fn send(&self, request: V2Request) -> Result<V2Response> {
        let method = reqwest::Method::from_bytes(request.method.as_bytes())
            .map_err(|_| OcgError::config(format!("unsupported HTTP method {}", request.method)))?;
        let mut builder = self
            .client
            .request(method, &request.url)
            .basic_auth(LOCAL_SERVICE_USERNAME, Some(self.password.expose()));
        if let Some(body) = &request.body {
            let encoded = serde_json::to_string(body).map_err(|error| {
                OcgError::config(format!(
                    "cannot encode the OpenCode V2 session request: {error}"
                ))
            })?;
            builder = builder
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(encoded);
        }
        let response = builder.send().map_err(|error| {
            let detail = error
                .to_string()
                .replace(&request.url, "<redacted-endpoint>")
                .replace(self.password.expose(), "<redacted-credential>");
            OcgError::config(format!(
                "cannot reach the local OpenCode V2 endpoint: {detail}"
            ))
        })?;
        let status = response.status().as_u16();
        let body = response.text().map_err(|error| {
            let detail = error
                .to_string()
                .replace(&request.url, "<redacted-endpoint>")
                .replace(self.password.expose(), "<redacted-credential>");
            OcgError::config(format!(
                "cannot read the local OpenCode V2 response: {detail}"
            ))
        })?;
        Ok(V2Response { status, body })
    }
}

/// The OpenCode 2 [`SessionClient`] backed by the local daemon.
pub struct V2SessionClient {
    base_url: String,
    directory: String,
    transport: Box<dyn V2Transport>,
}

impl V2SessionClient {
    /// Connect to a discovered service and scope sessions to `directory`.
    pub fn connect(
        registration: &ServiceRegistration,
        directory: impl Into<String>,
    ) -> Result<Self> {
        let transport = ReqwestV2Transport::new(registration)?;
        Ok(Self::with_transport(
            registration.url().to_string(),
            directory,
            Box::new(transport),
        ))
    }

    /// Build a client over an injected transport. This is the deterministic
    /// seam used by tests and by callers that supply their own HTTP stack.
    pub fn with_transport(
        base_url: impl Into<String>,
        directory: impl Into<String>,
        transport: Box<dyn V2Transport>,
    ) -> Self {
        Self {
            base_url: normalize_base_url(base_url.into()),
            directory: directory.into(),
            transport,
        }
    }

    fn endpoint(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    /// Probe whether this runtime is actually ready to serve OCG's session
    /// capability.
    ///
    /// A bound socket is not readiness: the process may have printed its
    /// startup lines, bound a port, and still not be serving the API (or may
    /// have died). This performs a real authenticated request against the
    /// runtime's own config route, which is the cheapest evidence that the API
    /// is up and that it accepted the credentials OCG registered. It never
    /// sleeps and never retries: callers bound the wait through [`wait_ready`].
    ///
    /// - `2xx` -> [`V2Readiness::Ready`]
    /// - a connection/transport failure or `5xx` -> [`V2Readiness::Starting`]
    ///   (the port may simply not be bound yet)
    /// - `401`/`403` or any other `4xx` -> [`V2Readiness::Unusable`], because
    ///   retrying cannot repair a rejected credential or a wrong route
    pub fn readiness(&self) -> V2Readiness {
        let url = self.endpoint("/api/config");
        match self.transport.send(V2Request {
            method: "GET",
            url,
            body: None,
        }) {
            Ok(response) => match response.status {
                status if (200..300).contains(&status) => V2Readiness::Ready,
                401 | 403 => V2Readiness::Unusable(
                    "the runtime rejected OCG's local service credentials".to_string(),
                ),
                status if (500..600).contains(&status) => V2Readiness::Starting,
                status => V2Readiness::Unusable(format!(
                    "the runtime API answered the readiness probe with HTTP {status}"
                )),
            },
            Err(_) => V2Readiness::Starting,
        }
    }

    /// Send one request, mapping transport failure, non-success status and
    /// non-JSON bodies to deterministic [`OcgError`]s. A successful empty body
    /// (for example a `204`) yields `Ok(None)`.
    fn send(&self, method: &'static str, path: &str, body: Option<Value>) -> Result<Option<Value>> {
        let url = self.endpoint(path);
        let response = self
            .transport
            .send(V2Request {
                method,
                url: url.clone(),
                body,
            })
            .map_err(|error| {
                OcgError::config(format!(
                    "OpenCode V2 session transport failed: {}",
                    error.to_string().replace(&url, "<redacted-endpoint>")
                ))
            })?;
        if !(200..300).contains(&response.status) {
            return Err(non_success(method, path, response.status, &response.body));
        }
        let text = response.body.trim();
        if text.is_empty() {
            return Ok(None);
        }
        let value = serde_json::from_str(text).map_err(|error| {
            OcgError::config(format!(
                "OpenCode V2 session response from {path} is not valid JSON: {error}"
            ))
        })?;
        Ok(Some(value))
    }
    /// Create a genuinely fresh root session for a rollover.  This is not
    /// `resolve_session`: the latter intentionally reuses an existing project
    /// session, which is the wrong operation for a bounded replacement.
    pub fn create_fresh_session(&self) -> Result<String> {
        let created = self
            .send(
                "POST",
                "/api/session",
                Some(json!({"location": {"directory": self.directory}})),
            )?
            .ok_or_else(|| malformed("/api/session", "empty response"))?;
        created
            .get("data")
            .and_then(|data| data.get("id"))
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| {
                OcgError::config(
                    "OpenCode V2 fresh session create response did not contain a session id",
                )
            })
    }

    /// Read the runtime's active-context message projection. The response is
    /// intentionally retained as a value at the runtime boundary; the governor
    /// decides which fields are safe to interpret and records provenance.
    pub fn context_messages(&self, session: &str) -> Result<Vec<Value>> {
        let path = format!("/api/session/{}/context", encode_component(session));
        let body = self
            .send("GET", &path, None)?
            .ok_or_else(|| malformed(&path, "empty response"))?;
        body.get("data")
            .and_then(Value::as_array)
            .cloned()
            .ok_or_else(|| malformed(&path, "missing the `data` array"))
    }

    /// Read the full session record for model and aggregate-token observation.
    pub fn session_info(&self, session: &str) -> Result<Value> {
        let path = format!("/api/session/{}", encode_component(session));
        let body = self
            .send("GET", &path, None)?
            .ok_or_else(|| malformed(&path, "empty response"))?;
        body.get("data")
            .and_then(Value::as_object)
            .cloned()
            .map(Value::Object)
            .ok_or_else(|| malformed(&path, "missing the `data` object"))
    }

    /// Read the model catalogue entry and its runtime-reported limits.
    pub fn model_metadata(
        &self,
        provider_id: Option<&str>,
        model_id: Option<&str>,
    ) -> Result<RuntimeModelMetadata> {
        let path = format!("/api/model?directory={}", encode_component(&self.directory));
        let body = self
            .send("GET", &path, None)?
            .ok_or_else(|| malformed(&path, "empty response"))?;
        let data = body
            .get("data")
            .and_then(Value::as_array)
            .ok_or_else(|| malformed(&path, "missing the `data` array"))?;
        let entry = data.iter().find(|entry| {
            let entry_provider = entry.get("providerID").and_then(Value::as_str);
            let entry_model = entry
                .get("id")
                .or_else(|| entry.get("modelID"))
                .and_then(Value::as_str);
            provider_id.is_none_or(|wanted| entry_provider == Some(wanted))
                && model_id.is_none_or(|wanted| entry_model == Some(wanted))
        });
        let Some(entry) = entry else {
            // A valid catalogue response with no matching model is an
            // observation with unknown limits, not permission to invent one.
            return Ok(RuntimeModelMetadata {
                source: Some("opencode-v2:/api/model".to_string()),
                ..Default::default()
            });
        };
        Ok(model_metadata_from_v2(entry))
    }

    /// Inject a durable synthetic continuation. The normal prompt route is
    /// intentionally not used: it is a user-task admission boundary and could
    /// re-admit the task under a new identity.
    pub fn inject_continuation(
        &self,
        session: &str,
        message_id: &str,
        text: &str,
        description: &str,
        metadata: &Value,
    ) -> Result<()> {
        self.inject_continuation_with_resume(session, message_id, text, description, metadata, true)
    }

    fn inject_continuation_with_resume(
        &self,
        session: &str,
        message_id: &str,
        text: &str,
        description: &str,
        metadata: &Value,
        resume: bool,
    ) -> Result<()> {
        let path = format!("/api/session/{}/synthetic", encode_component(session));
        self.send(
            "POST",
            &path,
            Some(json!({
                "id": message_id,
                "text": text,
                "description": description,
                "metadata": metadata,
                "resume": resume,
            })),
        )?;
        Ok(())
    }
}

impl SessionLifecycleClient for V2SessionClient {
    fn create_fresh_session(&self) -> Result<String> {
        Self::create_fresh_session(self)
    }

    fn context_messages(&self, session: &str) -> Result<Vec<Value>> {
        Self::context_messages(self, session)
    }

    fn session_info(&self, session: &str) -> Result<Value> {
        Self::session_info(self, session)
    }

    fn model_metadata(
        &self,
        provider_id: Option<&str>,
        model_id: Option<&str>,
    ) -> Result<RuntimeModelMetadata> {
        Self::model_metadata(self, provider_id, model_id)
    }

    fn inject_continuation(
        &self,
        session: &str,
        message_id: &str,
        text: &str,
        description: &str,
        metadata: &Value,
    ) -> Result<()> {
        Self::inject_continuation(self, session, message_id, text, description, metadata)
    }

    fn stage_continuation(
        &self,
        session: &str,
        message_id: &str,
        text: &str,
        description: &str,
        metadata: &Value,
    ) -> Result<()> {
        self.inject_continuation_with_resume(
            session,
            message_id,
            text,
            description,
            metadata,
            false,
        )
    }

    fn resume_continuation(
        &self,
        session: &str,
        message_id: &str,
        text: &str,
        description: &str,
        metadata: &Value,
    ) -> Result<()> {
        self.inject_continuation_with_resume(session, message_id, text, description, metadata, true)
    }
}

impl SessionClient for V2SessionClient {
    /// Resolve the newest root session for this project directory, creating one
    /// only when the directory has no session yet.
    fn resolve_session(&mut self) -> Result<String> {
        let list_path = format!(
            "/api/session?directory={}&parentID=null&order=desc&limit=1",
            encode_component(&self.directory)
        );
        let listed = self
            .send("GET", &list_path, None)?
            .ok_or_else(|| malformed(&list_path, "empty response"))?;
        let data = listed
            .get("data")
            .and_then(Value::as_array)
            .ok_or_else(|| malformed(&list_path, "missing the `data` array"))?;
        if let Some(id) = data
            .first()
            .and_then(|session| session.get("id"))
            .and_then(Value::as_str)
        {
            return Ok(id.to_string());
        }

        let created = self
            .send(
                "POST",
                "/api/session",
                Some(json!({"location": {"directory": self.directory}})),
            )?
            .ok_or_else(|| malformed("/api/session", "empty response"))?;
        created
            .get("data")
            .and_then(|data| data.get("id"))
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| {
                OcgError::config("OpenCode V2 session create response did not contain a session id")
            })
    }

    fn select_agent(&mut self, session: &str, agent: &str) -> Result<()> {
        let path = format!("/api/session/{}/agent", encode_component(session));
        self.send("POST", &path, Some(json!({ "agent": agent })))?;
        Ok(())
    }

    fn select_model(
        &mut self,
        session: &str,
        provider_id: &str,
        model_id: &str,
        variant: Option<&str>,
    ) -> Result<()> {
        let mut model = Map::new();
        model.insert("id".to_string(), Value::String(model_id.to_string()));
        model.insert(
            "providerID".to_string(),
            Value::String(provider_id.to_string()),
        );
        // Omitted entirely when there is no resolved variant. Never `null`,
        // never an empty string, never a fabricated default.
        if let Some(variant) = variant {
            model.insert("variant".to_string(), Value::String(variant.to_string()));
        }
        let path = format!("/api/session/{}/model", encode_component(session));
        self.send(
            "POST",
            &path,
            Some(json!({ "model": Value::Object(model) })),
        )?;
        Ok(())
    }

    /// Read the session's effective Lead back. A response without an agent or
    /// model is an error: OCG refuses to rely on state it cannot verify.
    fn effective_lead(&self, session: &str) -> Result<EffectiveLead> {
        let path = format!("/api/session/{}", encode_component(session));
        let body = self
            .send("GET", &path, None)?
            .ok_or_else(|| malformed(&path, "empty response"))?;
        let data = body
            .get("data")
            .and_then(Value::as_object)
            .ok_or_else(|| malformed(&path, "missing the `data` object"))?;

        let agent = data
            .get("agent")
            .and_then(Value::as_str)
            .map(str::to_string);
        let model = data.get("model").and_then(Value::as_object);
        let provider_id = model
            .and_then(|model| model.get("providerID"))
            .and_then(Value::as_str)
            .map(str::to_string);
        let model_id = model
            .and_then(|model| model.get("id"))
            .and_then(Value::as_str)
            .map(str::to_string);
        let variant = model
            .and_then(|model| model.get("variant"))
            .and_then(Value::as_str)
            .map(str::to_string);

        if agent.is_none() || provider_id.is_none() || model_id.is_none() {
            return Err(OcgError::config(format!(
                "OpenCode V2 session {session} did not report an effective agent/model"
            )));
        }
        Ok(EffectiveLead {
            agent,
            provider_id,
            model_id,
            variant,
        })
    }
}

impl BridgeRuntimeClient for V2SessionClient {
    fn as_lifecycle(&mut self) -> &mut dyn RuntimeAdapter {
        self
    }

    fn as_session(&mut self) -> &mut dyn SessionClient {
        self
    }
}

fn runtime_error(error: OcgError, default_kind: RuntimeErrorKind) -> RuntimeError {
    let detail = error.to_string();
    let lower = detail.to_ascii_lowercase();
    let kind = if lower.contains("http 401")
        || lower.contains("http 403")
        || lower.contains("credential")
        || lower.contains("unauthorized")
    {
        RuntimeErrorKind::Authentication
    } else if lower.contains("transport")
        || lower.contains("connection")
        || lower.contains("timed out")
        || lower.contains("timeout")
    {
        RuntimeErrorKind::Transport
    } else if default_kind == RuntimeErrorKind::ExecutionMissing
        && (lower.contains("http 404") || lower.contains("http 410"))
    {
        // Only an explicit not-found response proves execution absence. A
        // server error or an unclassified failure must remain an observation
        // failure/unavailable state, never authorize replacement.
        RuntimeErrorKind::ExecutionMissing
    } else if lower.contains("http 5")
        || lower.contains("internal error")
        || lower.contains("service unavailable")
    {
        RuntimeErrorKind::Unavailable
    } else if lower.contains("malformed")
        || lower.contains("valid json")
        || lower.contains("missing")
        || lower.contains("did not contain")
        || lower.contains("different session")
    {
        RuntimeErrorKind::InvalidResponse
    } else if default_kind == RuntimeErrorKind::ExecutionMissing {
        RuntimeErrorKind::ObservationFailed
    } else {
        default_kind
    };
    let safe_detail = sanitize_runtime_detail(&detail, kind);
    RuntimeError::new(kind, safe_detail)
}

fn sanitize_runtime_detail(detail: &str, kind: RuntimeErrorKind) -> String {
    let mut safe = detail
        .replace("OpenCode V2 session request", "runtime request")
        .replace("OpenCode V2 session response", "runtime response")
        .replace("OpenCode V2 session", "runtime")
        .replace("OpenCode", "runtime")
        .replace("/api/", "<runtime-route>/")
        .replace("/openapi", "<runtime-route>")
        .replace("http://", "<runtime-endpoint>")
        .replace("https://", "<runtime-endpoint>");
    if safe.trim().is_empty() {
        safe = match kind {
            RuntimeErrorKind::Unavailable => {
                "runtime lifecycle operation was unavailable".to_string()
            }
            RuntimeErrorKind::Unsupported => "runtime capability is unsupported".to_string(),
            RuntimeErrorKind::ExecutionMissing => "runtime execution was not found".to_string(),
            RuntimeErrorKind::Authentication => "runtime authentication was rejected".to_string(),
            RuntimeErrorKind::Transport => "runtime transport was unavailable".to_string(),
            RuntimeErrorKind::InvalidResponse => "runtime returned an invalid response".to_string(),
            RuntimeErrorKind::ObservationFailed => {
                "runtime observation was indeterminate".to_string()
            }
            RuntimeErrorKind::ProfileSelection => {
                "runtime profile selection or verification failed".to_string()
            }
            RuntimeErrorKind::ProviderCompletion => {
                "runtime continuation could not be completed".to_string()
            }
        };
    }
    safe
}

fn profile_to_lead(profile: &RuntimeProfile) -> RuntimeResult<LeadSelection> {
    if profile.is_empty() {
        return Err(RuntimeError::new(
            RuntimeErrorKind::ProfileSelection,
            "runtime profile is missing its profile id or model selector",
        ));
    }
    let (provider_id, model_id) = profile.model_selector.split_once('/').ok_or_else(|| {
        RuntimeError::new(
            RuntimeErrorKind::ProfileSelection,
            "runtime profile model selector must be provider/model",
        )
    })?;
    if provider_id.is_empty() || model_id.is_empty() {
        return Err(RuntimeError::new(
            RuntimeErrorKind::ProfileSelection,
            "runtime profile model selector is incomplete",
        ));
    }
    Ok(LeadSelection {
        level: "runtime".to_string(),
        agent: profile.profile_id.clone(),
        provider_id: provider_id.to_string(),
        model_id: model_id.to_string(),
        variant: profile.variant.clone(),
    })
}

fn model_metadata_from_v2(value: &Value) -> RuntimeModelMetadata {
    let limit = value.get("limit");
    RuntimeModelMetadata {
        provider_id: value
            .get("providerID")
            .and_then(Value::as_str)
            .map(str::to_string),
        model_id: value
            .get("id")
            .or_else(|| value.get("modelID"))
            .and_then(Value::as_str)
            .map(str::to_string),
        context_limit: limit
            .and_then(|limit| limit.get("context"))
            .and_then(Value::as_u64)
            .filter(|limit| *limit > 0),
        input_limit: limit
            .and_then(|limit| limit.get("input"))
            .and_then(Value::as_u64),
        output_limit: limit
            .and_then(|limit| limit.get("output"))
            .and_then(Value::as_u64),
        effective_limit: limit
            .and_then(|limit| limit.get("context"))
            .and_then(Value::as_u64)
            .filter(|limit| *limit > 0),
        source: Some("opencode-v2:/api/model".to_string()),
    }
}

fn runtime_context_limit(info: &Value, model: Option<&Map<String, Value>>) -> Option<u64> {
    info.get("limit")
        .and_then(|limit| limit.get("context").or_else(|| limit.get("contextLimit")))
        .and_then(Value::as_u64)
        .or_else(|| {
            model.and_then(|model| {
                model
                    .get("limit")
                    .and_then(|limit| limit.get("context").or_else(|| limit.get("contextLimit")))
                    .and_then(Value::as_u64)
            })
        })
        .or_else(|| info.get("contextLimit").and_then(Value::as_u64))
        .or_else(|| model.and_then(|model| model.get("contextLimit").and_then(Value::as_u64)))
        .filter(|limit| *limit > 0)
}

impl RuntimeAdapter for V2SessionClient {
    fn identity(&self) -> RuntimeIdentity {
        RuntimeIdentity::new("opencode", "v2", "invocation")
    }

    fn capabilities(&self) -> RuntimeCapabilities {
        RuntimeCapabilities::OPENCODE_V2
    }

    fn execution_parent(
        &self,
        execution_id: &RuntimeExecutionId,
    ) -> RuntimeResult<Option<RuntimeExecutionId>> {
        let info = self
            .session_info(execution_id.as_str())
            .map_err(|error| runtime_error(error, RuntimeErrorKind::ExecutionMissing))?;
        if info.get("id").and_then(Value::as_str) != Some(execution_id.as_str()) {
            return Err(RuntimeError::new(
                RuntimeErrorKind::InvalidResponse,
                "runtime lineage lookup returned a different execution identity",
            ));
        }
        // V2 omits parentID for a root; a child has a nonempty string.
        match info.get("parentID") {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(parent)) if !parent.is_empty() => {
                Ok(Some(RuntimeExecutionId::new(parent.clone())))
            }
            _ => Err(RuntimeError::new(
                RuntimeErrorKind::InvalidResponse,
                "runtime lineage response has an invalid parent identity",
            )),
        }
    }

    fn resolve_execution(&mut self) -> RuntimeResult<RuntimeExecutionId> {
        SessionClient::resolve_session(self)
            .map(RuntimeExecutionId::new)
            .map_err(|error| runtime_error(error, RuntimeErrorKind::Unavailable))
    }

    fn create_execution(&mut self) -> RuntimeResult<RuntimeExecutionId> {
        V2SessionClient::create_fresh_session(self)
            .map(RuntimeExecutionId::new)
            .map_err(|error| runtime_error(error, RuntimeErrorKind::Unavailable))
    }

    fn recover_execution(
        &mut self,
        key: &RuntimeRecoveryKey,
    ) -> RuntimeResult<Option<RuntimeExecution>> {
        let path = format!(
            "/api/session?directory={}&parentID=null&order=desc&limit=100",
            encode_component(&self.directory)
        );
        let listed = self
            .send("GET", &path, None)
            .map_err(|error| runtime_error(error, RuntimeErrorKind::Unavailable))?;
        let data = listed
            .as_ref()
            .and_then(|value| value.get("data"))
            .and_then(Value::as_array)
            .ok_or_else(|| {
                RuntimeError::new(
                    RuntimeErrorKind::InvalidResponse,
                    "runtime recovery listing did not contain a session array",
                )
            })?;
        let mut matches = Vec::new();
        for item in data {
            let Some(id) = item.get("id").and_then(Value::as_str) else {
                continue;
            };
            let messages = self
                .context_messages(id)
                .map_err(|error| runtime_error(error, RuntimeErrorKind::Transport))?;
            if messages.iter().any(|message| {
                let Some(metadata) = message.get("metadata") else {
                    return false;
                };
                metadata.get("marker").and_then(Value::as_str) == Some("OCG_RECONCILIATION")
                    && metadata.get("job_id").and_then(Value::as_str)
                        == Some(key.job_id.as_str())
                    && metadata.get("generation").and_then(Value::as_u64)
                        == Some(key.generation as u64)
                    && metadata.get("operation_id").and_then(Value::as_str)
                        == Some(key.operation_id.as_str())
            }) {
                matches.push(id.to_string());
            }
        }
        match matches.as_slice() {
            [] => Err(RuntimeError::new(
                RuntimeErrorKind::ObservationFailed,
                "interrupted execution creation has no authoritative recovery marker",
            )),
            [id] => Ok(Some(RuntimeExecution {
                id: RuntimeExecutionId::new(id.clone()),
                profile: None,
            })),
            _ => Err(RuntimeError::new(
                RuntimeErrorKind::InvalidResponse,
                "runtime recovery marker matched more than one root execution",
            )),
        }
    }

    fn inspect_execution(
        &self,
        execution_id: &RuntimeExecutionId,
    ) -> RuntimeResult<RuntimeExecution> {
        let info = self
            .session_info(execution_id.as_str())
            .map_err(|error| runtime_error(error, RuntimeErrorKind::ExecutionMissing))?;
        let reported = info.get("id").and_then(Value::as_str).ok_or_else(|| {
            RuntimeError::new(
                RuntimeErrorKind::InvalidResponse,
                "runtime execution response did not report an id",
            )
        })?;
        if reported != execution_id.as_str() {
            return Err(RuntimeError::new(
                RuntimeErrorKind::ExecutionMissing,
                format!(
                    "runtime reported execution {reported}, expected {}",
                    execution_id.as_str()
                ),
            ));
        }
        Ok(RuntimeExecution {
            id: execution_id.clone(),
            profile: None,
        })
    }

    fn prepare_execution(
        &mut self,
        execution_id: &RuntimeExecutionId,
        profile: &RuntimeProfile,
    ) -> RuntimeResult<RuntimeExecution> {
        let lead = profile_to_lead(profile)?;
        select_existing_session_lead(self, execution_id.as_str(), &lead)
            .map_err(|error| runtime_error(error, RuntimeErrorKind::ProfileSelection))?;
        Ok(RuntimeExecution {
            id: execution_id.clone(),
            profile: Some(profile.clone()),
        })
    }

    fn observe_context(
        &self,
        event: &RuntimeContextEvent,
    ) -> RuntimeResult<RuntimeContextObservation> {
        let messages = self
            .context_messages(event.execution_id.as_str())
            .map_err(|error| runtime_error(error, RuntimeErrorKind::Unavailable))?;
        let info = self
            .session_info(event.execution_id.as_str())
            .map_err(|error| runtime_error(error, RuntimeErrorKind::Unavailable))?;
        if info.get("id").and_then(Value::as_str) != Some(event.execution_id.as_str()) {
            return Err(RuntimeError::new(
                RuntimeErrorKind::InvalidResponse,
                "runtime context query returned a different execution identity",
            ));
        }
        let model = info.get("model").and_then(Value::as_object);
        let reported_profile = event.reported_profile.as_ref();
        let provider_id = reported_profile
            .and_then(|profile| profile.model_selector.split_once('/'))
            .map(|(provider, _)| provider.to_string())
            .or_else(|| {
                model
                    .and_then(|model| model.get("providerID"))
                    .and_then(Value::as_str)
                    .map(str::to_string)
            });
        let model_id = reported_profile
            .and_then(|profile| profile.model_selector.split_once('/'))
            .map(|(_, model)| model.to_string())
            .or_else(|| {
                model
                    .and_then(|model| model.get("id").or_else(|| model.get("modelID")))
                    .and_then(Value::as_str)
                    .map(str::to_string)
            });
        let mut metadata = if provider_id.is_some() || model_id.is_some() {
            self.model_metadata(provider_id.as_deref(), model_id.as_deref())
                .map_err(|error| runtime_error(error, RuntimeErrorKind::InvalidResponse))?
        } else {
            RuntimeModelMetadata::default()
        };
        if let Some(limit) = runtime_context_limit(&info, model) {
            metadata.effective_limit = Some(
                metadata
                    .effective_limit
                    .map(|current| current.min(limit))
                    .unwrap_or(limit),
            );
            metadata.source = Some("opencode-v2:/api/session+model".to_string());
        }
        let mut usage = event.reported_usage.clone().unwrap_or_default();
        let mut assistant_message_id = event.assistant_message_id.clone();
        if usage.is_empty() {
            for message in messages.iter().rev() {
                if message.get("type").and_then(Value::as_str) == Some("assistant") {
                    usage = RuntimeContextUsage::from_value(message.get("tokens"));
                    assistant_message_id = message
                        .get("id")
                        .and_then(Value::as_str)
                        .map(str::to_string)
                        .or(assistant_message_id);
                    if !usage.is_empty() {
                        break;
                    }
                }
            }
        }
        let used_tokens = usage.active_context_tokens();
        let overflow = usage.input.is_some() && usage.cache_read.is_some() && used_tokens.is_none();
        let usage_provenance = if usage.is_empty() {
            RuntimeProvenance::Unknown
        } else {
            RuntimeProvenance::Exact
        };
        let context_provenance = if used_tokens.is_some() {
            RuntimeProvenance::Estimated
        } else {
            RuntimeProvenance::Unknown
        };
        Ok(RuntimeContextObservation {
            execution_id: event.execution_id.clone(),
            event_id: event.event_id.clone(),
            observed_at: event.observed_at,
            assistant_message_id,
            finish: event.finish.clone(),
            safe_boundary: event.safe_boundary,
            usage,
            used_tokens,
            limit_tokens: metadata.effective_limit,
            model: metadata,
            message_count: messages.len(),
            compaction_count: messages
                .iter()
                .filter(|message| message.get("type").and_then(Value::as_str) == Some("compaction"))
                .count(),
            usage_provenance,
            context_provenance,
            note: overflow.then(|| {
                "runtime context token projection overflowed; no comparison was attempted"
                    .to_string()
            }),
        })
    }

    fn stage_runtime_continuation(
        &self,
        execution_id: &RuntimeExecutionId,
        continuation: &RuntimeContinuation,
    ) -> RuntimeResult<()> {
        SessionLifecycleClient::stage_continuation(
            self,
            execution_id.as_str(),
            &continuation.id,
            &continuation.text,
            &continuation.description,
            &continuation.metadata,
        )
        .map_err(|error| runtime_error(error, RuntimeErrorKind::Transport))
    }

    fn resume_runtime_continuation(
        &self,
        execution_id: &RuntimeExecutionId,
        continuation: &RuntimeContinuation,
    ) -> RuntimeResult<()> {
        SessionLifecycleClient::resume_continuation(
            self,
            execution_id.as_str(),
            &continuation.id,
            &continuation.text,
            &continuation.description,
            &continuation.metadata,
        )
        .map_err(|error| runtime_error(error, RuntimeErrorKind::ProviderCompletion))
    }
}

fn malformed(path: &str, reason: &str) -> OcgError {
    OcgError::config(format!(
        "OpenCode V2 session response from {path} is malformed: {reason}"
    ))
}

/// A non-success status. Only the server's stable `_tag` discriminator (when
/// present and sanitized) is echoed; the response body is never rendered.
fn non_success(method: &str, path: &str, status: u16, body: &str) -> OcgError {
    match safe_tag(body) {
        Some(tag) => OcgError::config(format!(
            "OpenCode V2 session request {method} {path} failed with HTTP {status} ({tag})"
        )),
        None => OcgError::config(format!(
            "OpenCode V2 session request {method} {path} failed with HTTP {status}"
        )),
    }
}

fn safe_tag(body: &str) -> Option<String> {
    let tag = serde_json::from_str::<Value>(body)
        .ok()?
        .get("_tag")?
        .as_str()?
        .to_string();
    let sanitized: String = tag
        .chars()
        .filter(|character| character.is_ascii_alphanumeric() || *character == '_')
        .take(64)
        .collect();
    (!sanitized.is_empty()).then_some(sanitized)
}

/// Percent-encode one path segment or query value. Only RFC 3986 unreserved
/// characters survive, so project paths and session ids are always safe.
fn encode_component(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                encoded.push(byte as char)
            }
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}
