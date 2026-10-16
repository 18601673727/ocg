//! The local Control Plane: an opt-in, authenticated HTTP listener on the
//! loopback interface that reads persisted Job facts for a client on the
//! same machine, submits Provider Jobs to the host's own execution runtime
//! and requests their stop. It reads through the host's own
//! [`DatabaseHandle`] and submits through the host's own
//! [`SchedulerHandle`]; it keeps no copy of any Job, and the runtime, not
//! the listener or a request, owns every Job it submits.
//!
//! It is enabled only when the operator sets both [`PORT_VARIABLE`] and
//! [`TOKEN_VARIABLE`]; setting one without the other, or a token that is
//! too short or plainly not random, is a configuration error and the
//! process does not start. The host starts it only once its own services
//! run, and stops it before anything else, so a request never outlives the
//! database it reads. Its socket is bound by the host before it starts; it
//! then serves on a thread of its own, with a runtime of its own, so it
//! never touches the host's signal handling; the host stops it and joins
//! that thread.
//!
//! Every request must:
//!
//! - use `GET`, or `POST` or `DELETE` on the routes that take them;
//! - name the listener exactly in `Host`: `127.0.0.1:<port>` or
//!   `localhost:<port>` with the bound port, so a page served from any
//!   other name, rebound to this address, is refused;
//! - when it carries `Origin`, name the same origin over `http`, and when
//!   it carries `Sec-Fetch-Site`, be `same-origin` or `none`;
//! - to reach the API, be authenticated as one of two kinds of client: a
//!   programmatic client carrying `Authorization: Bearer <token>` with
//!   the configured token, or the operator's browser carrying its session
//!   cookie, whose state-changing requests must also carry that session's
//!   CSRF token in `X-OCG-CSRF`. A request carrying both kinds of
//!   credential, or neither, is refused;
//! - carry no body, except a submission or a sign-in, whose body must be
//!   JSON, declared `application/json` with its one `Content-Length`, at
//!   most [`MAX_SUBMISSION_BYTES`] or [`browser::MAX_LOGIN_BYTES`].
//!
//! The connection's loopback peer authorizes nothing. No response carries
//! a CORS header or the token; only a sign-in and its sign-out set a
//! cookie, and only the routes under `/api/` are API. When the operator
//! named an exported frontend in [`UI_VARIABLE`], any other `GET` is
//! answered with that frontend's files from that directory alone, never
//! from anywhere else on disk; see [`assets`].
//!
//! Routes, all versioned under `/api/v1`:
//!
//! - `GET /api/v1/health`: what the host can establish: it is running, and
//!   whether the database reads and accepts commands. Nothing about
//!   Providers or credentials.
//! - `GET /api/v1/jobs/{job}`: the Job's persisted lifecycle, with `job`
//!   written as OCG writes it, `job:<n>`.
//! - `GET /api/v1/jobs/{job}/input`: the Job's frozen input as stored: its
//!   objective as text, which the Job record names only by identity, its
//!   Attachments by identity, and the Profiles and model it was frozen with.
//!   Nothing of a Provider's endpoint or credential. See [`catalog`].
//! - `GET /api/v1/agent-profiles?before=profile:N&limit=M`: Agent Profiles,
//!   newest first, each with the LLM Profiles it may run with and the
//!   Executors that can run each now. A page reads `limit` Profiles of either
//!   kind, so it may list fewer; `next_before` continues it. See [`catalog`].
//! - `GET /api/v1/jobs/{job}/outputs?after_seq=N&limit=M`: the Job's
//!   outputs as stored, in `seq` order, strictly after `after_seq`, at most
//!   `limit` ([`MAX_OUTPUT_PAGE`]) of them, with the cursor of the next page.
//! - `GET /api/v1/jobs/{job}/events`: the same outputs as Server-Sent
//!   Events, those stored strictly after the cursor the request names with
//!   `Last-Event-ID` or `?after_seq=N`, then each one as it is stored, then
//!   how the Job settled. A browser's `EventSource` cannot send the token;
//!   a client reads it with an authenticated `fetch`. See [`events`].
//! - `POST /api/v1/jobs`: submits one Provider Job, under the request's one
//!   `Idempotency-Key`, of `{"llm_profile": "profile:<n>", "executor":
//!   "executor:<n>", "objective": "...", "name": "...", "agent_profile":
//!   "profile:<n>"}`, `name` and `agent_profile` optional: an existing LLM
//!   Profile and an Executor that can run it, and an Agent Profile that
//!   targets that LLM Profile, all checked here and again when the Job is
//!   created. No tool is offered, and
//!   nothing of a Provider, credential or command is accepted. The answer
//!   is `{"admission": {"status", "job"}}`: `201` when this request created
//!   the Job, `200` when the key already named it, `202` with `pending`
//!   when the admission did not end within a bounded wait; it runs on, and
//!   the key is looked up later. The answer never waits for the Provider.
//!   A key used for another request is `409`; a Job that may exist but
//!   cannot be confirmed is `500` `admission_uncertain`, and the key is not
//!   submitted again while that lasts. See [`submissions`].
//! - `GET /api/v1/admissions`: what the request's `Idempotency-Key`
//!   created: `{"admission": {"status", "job"}}`, with `status` one of
//!   `admitted` (with its Job), `pending`, `uncertain` or `not_found`.
//! - `POST /api/v1/jobs/{job}/stop`: requests the stop of a Job this
//!   listener admitted: `202` `requested` once the request reached its
//!   execution, `200` `already_settled` once its outcome is recorded, `409`
//!   `not_stoppable` for a Job with no execution here, such as one of an
//!   earlier process. A requested stop is not an outcome: the Job's record
//!   says how it settled.
//! - `POST /api/v1/session`: signs the operator's browser in under the
//!   pairing code this run printed for its operator, and sets its session
//!   cookie; `GET /api/v1/session` reports that session and the CSRF
//!   token its requests carry; `DELETE /api/v1/session` revokes it and
//!   clears the cookie. The browser's alone: a bearer token reaches none
//!   of them. See [`browser`].
//!
//! A request is answered with a small JSON body, an error with a fixed
//! code and message that repeats nothing the request or the database
//! said.

use std::fmt;
use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use aws_lc_rs::constant_time::verify_slices_are_equal;
use aws_lc_rs::digest::{SHA256, digest};
use ntex::http::header::{self, HeaderMap, HeaderValue};
use ntex::http::{HttpServiceConfig, Method, StatusCode};
use ntex::io::IoConfig;
use ntex::server::Server;
use ntex::service::cfg::SharedCfg;
use ntex::time::Seconds;
use ntex::web::{self, App, HttpRequest, HttpResponse, HttpServer};
use serde_json::{Value, json};

use crate::database::{DatabaseError, DatabaseHandle, DatabaseStatus};
use crate::orchestration::execution_runtime::SchedulerHandle;
use crate::persistence::{ExecutionMode, JobId, JobOutputRecord, JobRecord, OutputKind, ProfileId};

mod assets;
mod browser;
mod catalog;
mod config;
mod events;
mod listener;
mod submissions;

#[cfg(test)]
pub(crate) use events::events_seam;
#[cfg(test)]
pub(crate) use submissions::resolution_seam;

pub use config::{
    API_VERSION, ConfigError, ControlPlaneConfig, MAX_OUTPUT_PAGE, MAX_SUBMISSION_BYTES,
    MIN_TOKEN_CHARS, PORT_VARIABLE, TOKEN_VARIABLE, UI_VARIABLE,
};
use config::{
    DEFAULT_OUTPUT_PAGE, MAX_HEADER_BYTES, MAX_HEADERS, MAX_PATH_BYTES, MAX_QUERY_BYTES, Token,
};
pub use listener::{ControlPlane, StartError, StopError};

#[cfg(test)]
impl ControlPlaneConfig {
    #[cfg(test)]
    pub(crate) fn with_response_bytes(mut self, bytes: usize) -> Self {
        self.response_bytes = bytes;
        self
    }

    #[cfg(test)]
    pub(crate) fn with_admission_wait(mut self, wait: Duration) -> Self {
        self.admission_wait = wait;
        self
    }

    /// At most `streams` event streams, each reading again every `poll`,
    /// silent at most `heartbeat`, disconnected once its client took
    /// nothing for `stall`, and lasting at most `lifetime`.
    #[cfg(test)]
    pub(crate) fn with_streams(
        mut self,
        streams: usize,
        poll: Duration,
        heartbeat: Duration,
        stall: Duration,
        lifetime: Duration,
    ) -> Self {
        self.stream_limits = events::StreamLimits {
            streams,
            poll,
            heartbeat,
            stall,
            lifetime,
        };
        self
    }
}

#[cfg(test)]
impl ControlPlane {
    /// How many Jobs it admitted whose execution has not returned, and how
    /// many keys it holds unresolved.
    #[cfg(test)]
    pub(crate) fn held(&self) -> (usize, usize) {
        (
            self.shared.submissions.running(),
            self.shared.submissions.unresolved(),
        )
    }

    /// How many event streams are open, and how many outputs they have
    /// read in all.
    #[cfg(test)]
    pub(crate) fn streams(&self) -> (usize, usize) {
        (
            self.shared.streams.load(Ordering::SeqCst),
            self.shared.fetched.load(Ordering::SeqCst),
        )
    }

    /// Counts what still holds the listener's shared state, the database
    /// handle among it, after the listener is gone.
    #[cfg(test)]
    pub(crate) fn holders(&self) -> impl Fn() -> usize + Send + 'static {
        let shared = Arc::downgrade(&self.shared);
        move || shared.strong_count()
    }
}

/// What every request shares.
struct Shared {
    database: DatabaseHandle,
    scheduler: SchedulerHandle,
    submissions: submissions::Registry,
    token: Token,
    port: u16,
    response_bytes: usize,
    admission_wait: Duration,
    stream_limits: events::StreamLimits,
    /// The exported frontend, when one is served.
    assets: Option<assets::UiAssets>,
    /// The browser's sign-in and its one session, with the frontend.
    browser: Option<browser::State>,
    /// How many event streams are open.
    streams: AtomicUsize,
    /// Set once the listener stops.
    closing: AtomicBool,
    #[cfg(test)]
    fetched: AtomicUsize,
}

/// A refused or failed request: its status and a fixed code and message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Refusal {
    status: StatusCode,
    code: &'static str,
    message: &'static str,
    /// The methods a `405` names.
    allow: &'static str,
}

impl Refusal {
    const fn new(status: StatusCode, code: &'static str, message: &'static str) -> Self {
        Self {
            status,
            code,
            message,
            allow: "GET",
        }
    }

    fn response(&self) -> HttpResponse {
        let mut response = json_response(
            self.status,
            &json!({
                "api_version": API_VERSION,
                "error": {"code": self.code, "message": self.message},
            }),
        );
        if self.status == StatusCode::UNAUTHORIZED {
            response
                .headers_mut()
                .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
        }
        if self.status == StatusCode::METHOD_NOT_ALLOWED {
            response
                .headers_mut()
                .insert(header::ALLOW, HeaderValue::from_static(self.allow));
        }
        response
    }
}

/// An answered request: its status and JSON body, and a cookie when the
/// answer signs a browser in or out.
struct Reply {
    status: StatusCode,
    body: Value,
    cookie: Option<String>,
}

impl Reply {
    fn new(status: StatusCode, body: Value) -> Self {
        Self {
            status,
            body,
            cookie: None,
        }
    }

    fn ok(body: Value) -> Self {
        Self::new(StatusCode::OK, body)
    }

    fn with_cookie(status: StatusCode, body: Value, cookie: String) -> Self {
        Self {
            status,
            body,
            cookie: Some(cookie),
        }
    }
}

const INVALID_REQUEST: Refusal = Refusal::new(
    StatusCode::BAD_REQUEST,
    "invalid_request",
    "the request's path or query is not one this API accepts",
);
const UNAUTHENTICATED: Refusal = Refusal::new(
    StatusCode::UNAUTHORIZED,
    "unauthenticated",
    "a valid bearer token is required",
);
const FORBIDDEN_HOST: Refusal = Refusal::new(
    StatusCode::FORBIDDEN,
    "forbidden_host",
    "the Host header does not name this listener",
);
const FORBIDDEN_ORIGIN: Refusal = Refusal::new(
    StatusCode::FORBIDDEN,
    "forbidden_origin",
    "cross-origin requests are refused",
);
const NOT_FOUND: Refusal = Refusal::new(StatusCode::NOT_FOUND, "not_found", "no such resource");
const JOB_NOT_FOUND: Refusal = Refusal::new(StatusCode::NOT_FOUND, "job_not_found", "no such Job");
const METHOD_NOT_ALLOWED: Refusal = Refusal::new(
    StatusCode::METHOD_NOT_ALLOWED,
    "method_not_allowed",
    "only GET is accepted",
);
const ONLY_POST: Refusal = Refusal {
    message: "only POST is accepted",
    allow: "POST",
    ..METHOD_NOT_ALLOWED
};
const SESSION_METHODS: Refusal = Refusal {
    message: "only GET, POST or DELETE is accepted",
    allow: "GET, POST, DELETE",
    ..METHOD_NOT_ALLOWED
};
const URI_TOO_LONG: Refusal = Refusal::new(
    StatusCode::URI_TOO_LONG,
    "uri_too_long",
    "the request's path or query is too long",
);
const HEADERS_TOO_LARGE: Refusal = Refusal::new(
    StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
    "headers_too_large",
    "the request's headers are too large",
);
const BODY_NOT_ACCEPTED: Refusal = Refusal::new(
    StatusCode::PAYLOAD_TOO_LARGE,
    "body_not_accepted",
    "a request may not carry a body",
);
const BODY_TOO_LARGE: Refusal = Refusal::new(
    StatusCode::PAYLOAD_TOO_LARGE,
    "body_too_large",
    "the body is larger than a submission may be",
);
const LENGTH_REQUIRED: Refusal = Refusal::new(
    StatusCode::LENGTH_REQUIRED,
    "length_required",
    "a submission must declare its one Content-Length and no Transfer-Encoding",
);
const UNSUPPORTED_MEDIA_TYPE: Refusal = Refusal::new(
    StatusCode::UNSUPPORTED_MEDIA_TYPE,
    "unsupported_media_type",
    "a submission must be application/json",
);
const OUTPUT_TOO_LARGE: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "output_too_large",
    "the next output is larger than a response may be; it is not truncated",
);
const DATABASE_BUSY: Refusal = Refusal::new(
    StatusCode::SERVICE_UNAVAILABLE,
    "database_busy",
    "the database is busy; retry later",
);
const DATABASE_UNAVAILABLE: Refusal = Refusal::new(
    StatusCode::SERVICE_UNAVAILABLE,
    "database_unavailable",
    "the database cannot be read now",
);
const SHUTTING_DOWN: Refusal = Refusal::new(
    StatusCode::SERVICE_UNAVAILABLE,
    "shutting_down",
    "OCG is shutting down",
);
const INCONSISTENT_RECORD: Refusal = Refusal::new(
    StatusCode::INTERNAL_SERVER_ERROR,
    "inconsistent_record",
    "the Job's record cannot be described truthfully",
);
const INTERNAL: Refusal = Refusal::new(
    StatusCode::INTERNAL_SERVER_ERROR,
    "internal",
    "the request could not be answered",
);

impl From<DatabaseError> for Refusal {
    /// A failed read never says what failed inside, and never becomes a
    /// Job state.
    fn from(error: DatabaseError) -> Self {
        match error {
            DatabaseError::ReadQueueFull => DATABASE_BUSY,
            DatabaseError::ReadsClosed => SHUTTING_DOWN,
            DatabaseError::ReadsStorageFailed(_)
            | DatabaseError::ReadInterrupted
            | DatabaseError::ReadPoolStopped
            | DatabaseError::Read(_) => DATABASE_UNAVAILABLE,
            _ => INTERNAL,
        }
    }
}

fn json_response(status: StatusCode, body: &Value) -> HttpResponse {
    HttpResponse::build(status)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CACHE_CONTROL, "no-store")
        .header(header::X_CONTENT_TYPE_OPTIONS, "nosniff")
        .body(body.to_string())
}

/// Answers one request: refuses it unless it passes every check, in order,
/// then routes it.
async fn answer(
    request: HttpRequest,
    payload: web::types::Payload,
    shared: web::types::State<Arc<Shared>>,
) -> HttpResponse {
    match admit(&request, &shared) {
        Err(refusal) => refusal.response(),
        Ok(Destination::Asset) => match shared
            .assets
            .as_ref()
            .map(|assets| assets.serve(request.path()))
            .unwrap_or(Err(NOT_FOUND))
        {
            Ok(asset) => assets::response(asset),
            Err(refusal) => refusal.response(),
        },
        Ok(Destination::Api(caller)) => {
            match route(&request).and_then(|route| authorize(&caller, route)) {
                Ok(Route::Events { job, after_seq }) => {
                    events::open(&shared, request.io().cloned(), job, after_seq)
                        .await
                        .unwrap_or_else(|refusal| refusal.response())
                }
                Ok(route) => match respond(route, &caller, &request, payload, &shared).await {
                    Ok(reply) => {
                        let mut response = json_response(reply.status, &reply.body);
                        if let Some(cookie) = &reply.cookie {
                            match HeaderValue::from_str(cookie) {
                                Ok(value) => {
                                    response.headers_mut().insert(header::SET_COOKIE, value);
                                }
                                Err(_) => return INTERNAL.response(),
                            }
                        }
                        response
                    }
                    Err(refusal) => refusal.response(),
                },
                Err(refusal) => refusal.response(),
            }
        }
    }
}

/// Whether `path` names a route that is submitted to with `POST`: Job
/// submission and a Job's stop.
fn takes_post(path: &str) -> bool {
    match path.strip_prefix("/api/v1/") {
        Some("jobs") => true,
        Some(rest) => rest
            .strip_prefix("jobs/")
            .and_then(|rest| rest.strip_suffix("/stop"))
            .is_some_and(|job| !job.contains('/')),
        None => false,
    }
}

/// Where an admitted request is answered from.
enum Destination {
    /// The exported frontend's files.
    Asset,
    /// An API route, for one caller.
    Api(Caller),
}

/// Who an API request is.
#[derive(Clone)]
enum Caller {
    /// A programmatic client carrying the bearer token.
    Programmatic,
    /// The operator's browser, within its session.
    Operator(browser::Session),
    /// No one yet: only the sign-in, which needs this run's pairing code
    /// and nothing else.
    Anonymous,
}

/// Checks everything about a request but its route: its shape, that it
/// names this listener and its own origin, who it is, and what it may
/// carry.
fn admit(request: &HttpRequest, shared: &Shared) -> Result<Destination, Refusal> {
    let headers = request.headers();
    let header_bytes = headers
        .iter()
        .map(|(name, value)| name.as_str().len() + value.len())
        .sum::<usize>();
    if headers.len() > MAX_HEADERS || header_bytes > MAX_HEADER_BYTES {
        return Err(HEADERS_TOO_LARGE);
    }
    if request.path().len() > MAX_PATH_BYTES || request.query_string().len() > MAX_QUERY_BYTES {
        return Err(URI_TOO_LONG);
    }
    let path = request.path();
    let login = path == "/api/v1/session";
    let api = login || path.starts_with("/api/");
    let takes_post = takes_post(path);
    let method = request.method();
    let allowed = if login {
        *method == Method::GET || *method == Method::POST || *method == Method::DELETE
    } else if api {
        *method == Method::GET || (takes_post && *method == Method::POST)
    } else {
        *method == Method::GET
    };
    if !allowed {
        return Err(if login {
            SESSION_METHODS
        } else if takes_post {
            ONLY_POST
        } else {
            METHOD_NOT_ALLOWED
        });
    }
    let Some(host) = names_listener(headers, shared.port) else {
        return Err(FORBIDDEN_HOST);
    };
    if !same_origin(headers, &host) {
        return Err(FORBIDDEN_ORIGIN);
    }
    if !api {
        // The frontend's files are the same shell for every reader on this
        // machine: all application data and every command is API, and
        // refused there without its credential.
        return Ok(Destination::Asset);
    }
    let caller = if login && method == Method::POST {
        // A sign-in carries its own credential, this run's pairing code;
        // the bearer API has no use for it.
        if headers.get_all(header::AUTHORIZATION).count() != 0 {
            return Err(NOT_FOUND);
        }
        Caller::Anonymous
    } else {
        authenticate(headers, shared)?
    };
    if matches!(caller, Caller::Operator(_)) && method != Method::GET {
        let Caller::Operator(session) = &caller else {
            return Err(INTERNAL);
        };
        if !shared
            .browser
            .as_ref()
            .is_some_and(|browser| browser.csrf_matches(session, headers))
        {
            return Err(browser::FORBIDDEN_CSRF);
        }
    }
    if method == Method::POST && path == "/api/v1/jobs" {
        return submission_body(headers).map(|()| Destination::Api(caller));
    }
    if login && method == Method::POST {
        return browser::login_body(headers).map(|()| Destination::Api(caller));
    }
    let length = headers.get_all(header::CONTENT_LENGTH).collect::<Vec<_>>();
    if headers.contains_key(header::TRANSFER_ENCODING)
        || length.len() > 1
        || length
            .first()
            .is_some_and(|length| length.as_bytes() != b"0")
    {
        return Err(BODY_NOT_ACCEPTED);
    }
    Ok(Destination::Api(caller))
}

/// The request's one credential, or refusal: exactly one of the bearer
/// token and a live session cookie. A request with both, with a
/// duplicated header, or with a cookie that names no live session, is
/// refused rather than believed in part.
fn authenticate(headers: &HeaderMap, shared: &Shared) -> Result<Caller, Refusal> {
    let credentials = headers.get_all(header::AUTHORIZATION).count();
    let presented = browser::presented(headers);
    match (credentials, &presented) {
        (0, browser::Presented::None) => Err(UNAUTHENTICATED),
        (1, browser::Presented::None) if authenticated(headers, &shared.token) => {
            Ok(Caller::Programmatic)
        }
        (0, browser::Presented::One(_)) => shared
            .browser
            .as_ref()
            .and_then(|browser| browser.session(&presented))
            .map(Caller::Operator)
            .ok_or(UNAUTHENTICATED),
        _ => Err(UNAUTHENTICATED),
    }
}

/// A submission's body is declared JSON, with exactly one length, and no
/// larger than a submission may be, before any of it is read.
fn submission_body(headers: &HeaderMap) -> Result<(), Refusal> {
    let mut lengths = headers.get_all(header::CONTENT_LENGTH);
    let length = match (lengths.next(), lengths.next()) {
        (Some(length), None) if !headers.contains_key(header::TRANSFER_ENCODING) => length,
        _ => return Err(LENGTH_REQUIRED),
    };
    let length = length
        .to_str()
        .ok()
        .and_then(decimal::<usize>)
        .ok_or(LENGTH_REQUIRED)?;
    if length > MAX_SUBMISSION_BYTES {
        return Err(BODY_TOO_LARGE);
    }
    let json = single(headers, &header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            let mut parts = value.split(';').map(str::trim);
            parts
                .next()
                .is_some_and(|media| media.eq_ignore_ascii_case("application/json"))
                && parts.all(|parameter| parameter.eq_ignore_ascii_case("charset=utf-8"))
        });
    if json {
        Ok(())
    } else {
        Err(UNSUPPORTED_MEDIA_TYPE)
    }
}

/// The body, whole, refused once it exceeds `limit` bytes.
async fn read_body(mut payload: web::types::Payload, limit: usize) -> Result<Vec<u8>, Refusal> {
    let mut body = Vec::new();
    while let Some(chunk) = payload.recv().await {
        let chunk = chunk.map_err(|_| submissions::INVALID_BODY)?;
        if body.len() + chunk.len() > limit {
            return Err(BODY_TOO_LARGE);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// The only value of `name`, if it has exactly one.
fn single<'a>(headers: &'a HeaderMap, name: &header::HeaderName) -> Option<&'a HeaderValue> {
    let mut values = headers.get_all(name);
    let value = values.next()?;
    values.next().is_none().then_some(value)
}

/// The request's one `Host`, lowercased, if it names this listener exactly.
fn names_listener(headers: &HeaderMap, port: u16) -> Option<String> {
    let host = single(headers, &header::HOST)?
        .to_str()
        .ok()?
        .to_ascii_lowercase();
    (host == format!("127.0.0.1:{port}") || host == format!("localhost:{port}")).then_some(host)
}

/// Whether the request says nothing of an origin, or its one `Origin` is
/// the `http` origin of the `host` it names.
fn same_origin(headers: &HeaderMap, host: &str) -> bool {
    let origin_allowed = match headers.get_all(header::ORIGIN).count() {
        0 => true,
        1 => single(headers, &header::ORIGIN)
            .and_then(|origin| origin.to_str().ok())
            .map(str::to_ascii_lowercase)
            .is_some_and(|origin| origin == format!("http://{host}")),
        _ => false,
    };
    let site_allowed = match headers.get_all("sec-fetch-site").count() {
        0 => true,
        1 => single(headers, &header::HeaderName::from_static("sec-fetch-site"))
            .is_some_and(|site| site.as_bytes() == b"same-origin" || site.as_bytes() == b"none"),
        _ => false,
    };
    origin_allowed && site_allowed
}

/// Whether the request's one `Authorization` is the bearer token.
fn authenticated(headers: &HeaderMap, token: &Token) -> bool {
    let Some(value) = single(headers, &header::AUTHORIZATION) else {
        return false;
    };
    let value = value.as_bytes();
    let Some((scheme, presented)) = value.split_at_checked(7) else {
        return false;
    };
    scheme.eq_ignore_ascii_case(b"bearer ") && token.matches(presented)
}

/// A request's route, once admitted.
enum Route {
    Health,
    Job(JobId),
    JobInput(JobId),
    AgentProfiles {
        before: Option<ProfileId>,
        limit: usize,
    },
    Outputs {
        job: JobId,
        after_seq: Option<i64>,
        limit: usize,
    },
    Events {
        job: JobId,
        after_seq: Option<i64>,
    },
    Submit,
    Admission,
    Stop(JobId),
    SessionCreate,
    SessionStatus,
    SessionRevoke,
}

/// Whether `caller` may use `route`: the API routes a signed-in browser
/// may use are named one by one, and everything unnamed is refused to it.
/// The session routes are the browser's alone.
fn authorize(caller: &Caller, route: Route) -> Result<Route, Refusal> {
    let allowed = match (caller, &route) {
        (Caller::Anonymous, Route::SessionCreate) => true,
        (
            Caller::Programmatic,
            Route::SessionCreate | Route::SessionStatus | Route::SessionRevoke,
        ) => false,
        (Caller::Programmatic, _) => true,
        (
            Caller::Operator(_),
            Route::Health
            | Route::Job(_)
            | Route::JobInput(_)
            | Route::AgentProfiles { .. }
            | Route::Outputs { .. }
            | Route::Events { .. }
            | Route::Admission
            | Route::Submit
            | Route::Stop(_)
            | Route::SessionStatus
            | Route::SessionRevoke,
        ) => true,
        (Caller::Operator(_) | Caller::Anonymous, _) => false,
    };
    if allowed { Ok(route) } else { Err(NOT_FOUND) }
}

fn route(request: &HttpRequest) -> Result<Route, Refusal> {
    let path = request.path();
    let query = request.query_string();
    let segments = path
        .strip_prefix("/api/v1/")
        .ok_or(NOT_FOUND)?
        .split('/')
        .collect::<Vec<_>>();
    if request.method() == Method::POST {
        return match segments.as_slice() {
            ["jobs"] => no_query(query).map(|()| Route::Submit),
            ["jobs", job, "stop"] => {
                let job = job_id(job)?;
                no_query(query).map(|()| Route::Stop(job))
            }
            ["session"] => no_query(query).map(|()| Route::SessionCreate),
            _ => Err(NOT_FOUND),
        };
    }
    if request.method() == Method::DELETE {
        return match segments.as_slice() {
            ["session"] => no_query(query).map(|()| Route::SessionRevoke),
            _ => Err(NOT_FOUND),
        };
    }
    match segments.as_slice() {
        ["health"] => no_query(query).map(|()| Route::Health),
        ["admissions"] => no_query(query).map(|()| Route::Admission),
        ["session"] => no_query(query).map(|()| Route::SessionStatus),
        ["jobs", job] => {
            let job = job_id(job)?;
            no_query(query).map(|()| Route::Job(job))
        }
        ["jobs", job, "input"] => {
            let job = job_id(job)?;
            no_query(query).map(|()| Route::JobInput(job))
        }
        ["agent-profiles"] => catalog::agent_profile_query(query)
            .map(|(before, limit)| Route::AgentProfiles { before, limit }),
        ["jobs", job, "outputs"] => {
            let job = job_id(job)?;
            let (after_seq, limit) = output_query(query)?;
            Ok(Route::Outputs {
                job,
                after_seq,
                limit,
            })
        }
        ["jobs", job, "events"] => {
            let job = job_id(job)?;
            let after_seq = events::cursor(query, request.headers())?;
            Ok(Route::Events { job, after_seq })
        }
        _ => Err(NOT_FOUND),
    }
}

fn no_query(query: &str) -> Result<(), Refusal> {
    if query.is_empty() {
        Ok(())
    } else {
        Err(INVALID_REQUEST)
    }
}

/// A non-negative decimal number without sign, leading zero or more digits
/// than `T` holds.
fn decimal<T: std::str::FromStr>(text: &str) -> Option<T> {
    let canonical = !text.is_empty()
        && text.bytes().all(|byte| byte.is_ascii_digit())
        && (text == "0" || !text.starts_with('0'));
    canonical.then(|| text.parse().ok()).flatten()
}

/// A Job written as OCG writes it: `job:<n>`.
fn job_id(text: &str) -> Result<JobId, Refusal> {
    text.strip_prefix("job:")
        .and_then(decimal::<u64>)
        .map(JobId::new)
        .ok_or(INVALID_REQUEST)
}

/// `after_seq` and `limit`, each at most once, and nothing else.
fn output_query(query: &str) -> Result<(Option<i64>, usize), Refusal> {
    let (mut after_seq, mut limit) = (None, None);
    if !query.is_empty() {
        for pair in query.split('&') {
            let (key, value) = pair.split_once('=').ok_or(INVALID_REQUEST)?;
            let slot = match key {
                "after_seq" if after_seq.is_none() => {
                    after_seq = Some(decimal::<i64>(value).ok_or(INVALID_REQUEST)?);
                    continue;
                }
                "limit" if limit.is_none() => &mut limit,
                _ => return Err(INVALID_REQUEST),
            };
            *slot = Some(
                decimal::<usize>(value)
                    .filter(|limit| (1..=MAX_OUTPUT_PAGE).contains(limit))
                    .ok_or(INVALID_REQUEST)?,
            );
        }
    }
    Ok((after_seq, limit.unwrap_or(DEFAULT_OUTPUT_PAGE)))
}

async fn respond(
    route: Route,
    caller: &Caller,
    request: &HttpRequest,
    payload: web::types::Payload,
    shared: &Arc<Shared>,
) -> Result<Reply, Refusal> {
    let database = &shared.database;
    match route {
        Route::Health => Ok(Reply::ok(health(database).await)),
        Route::Job(job) => {
            let record = database.job(job).await?.ok_or(JOB_NOT_FOUND)?;
            Ok(Reply::ok(
                json!({"api_version": API_VERSION, "job": job_json(&record)?}),
            ))
        }
        Route::JobInput(job) => catalog::job_input(database, job).await,
        Route::AgentProfiles { before, limit } => {
            catalog::agent_profiles(database, before, limit).await
        }
        Route::Outputs {
            job,
            after_seq,
            limit,
        } => outputs(shared, job, after_seq, limit).await.map(Reply::ok),
        Route::Submit => {
            let key = submissions::idempotency_key(request.headers())?;
            let body = read_body(payload, MAX_SUBMISSION_BYTES).await?;
            let job = submissions::JobRequest::parse(&body)?;
            submissions::submit(shared, key, job).await
        }
        Route::Admission => {
            let key = submissions::idempotency_key(request.headers())?;
            submissions::lookup(shared, key).await
        }
        Route::Stop(job) => submissions::stop(shared, job).await,
        Route::SessionCreate => {
            let body = read_body(payload, browser::MAX_LOGIN_BYTES).await?;
            let browser = shared.browser.as_ref().ok_or(NOT_FOUND)?;
            browser.login(&body)
        }
        Route::SessionStatus => match caller {
            Caller::Operator(session) => Ok(Reply::ok(browser::described(session))),
            _ => Err(NOT_FOUND),
        },
        Route::SessionRevoke => match caller {
            Caller::Operator(session) => {
                let browser = shared.browser.as_ref().ok_or(NOT_FOUND)?;
                Ok(browser.logout(session))
            }
            _ => Err(NOT_FOUND),
        },
        // Answered as a stream, before any reply.
        Route::Events { .. } => Err(INTERNAL),
    }
}

/// What the host can establish: it runs, as the listener answers only
/// while it does, and what the database says of itself now.
async fn health(database: &DatabaseHandle) -> Value {
    let status = match database.status() {
        DatabaseStatus::Open => "open",
        DatabaseStatus::Protected => "protected",
        DatabaseStatus::ShuttingDown => "shutting_down",
        DatabaseStatus::WorkerStopped => "stopped",
        DatabaseStatus::Closed => "closed",
    };
    let readable = database.job(database.genesis()).await.is_ok();
    json!({
        "api_version": API_VERSION,
        "host": "running",
        "database": {
            "status": status,
            "accepting_commands": status == "open",
            "readable": readable,
        },
    })
}

fn micros(timestamp: Option<crate::persistence::Timestamp>) -> Value {
    timestamp.map_or(Value::Null, |timestamp| json!(timestamp.as_unix_micros()))
}

/// The Job's settlement: the one outcome its record holds, or none yet; a
/// record holding more than one is not described at all.
fn settlement_json(record: &JobRecord) -> Result<Value, Refusal> {
    let outcomes = [
        ("completed", record.completed_at),
        ("failed", record.failed_at),
        ("cancelled", record.cancelled_at),
        ("unknown", record.unknown_at),
    ]
    .into_iter()
    .filter_map(|(state, at)| at.map(|at| (state, at)))
    .collect::<Vec<_>>();
    match outcomes.as_slice() {
        [] => Ok(json!({"state": "unsettled", "settled_at_us": null})),
        [(state, at)] => Ok(json!({"state": state, "settled_at_us": at.as_unix_micros()})),
        _ => Err(INCONSISTENT_RECORD),
    }
}

/// The Job's lifecycle as recorded, with its [`settlement_json`].
fn job_json(record: &JobRecord) -> Result<Value, Refusal> {
    let settlement = settlement_json(record)?;
    let execution_mode = match record.execution_mode {
        ExecutionMode::Once => "once",
        ExecutionMode::Continuous => "continuous",
        ExecutionMode::Periodic => "periodic",
        ExecutionMode::Container => "container",
    };
    Ok(json!({
        "id": record.id.to_string(),
        "name": record.name,
        "execution_mode": execution_mode,
        "input": record.input.to_string(),
        "assigned_executor": record.assigned_executor.map(|executor| executor.to_string()),
        "created_at_us": record.created_at.as_unix_micros(),
        "updated_at_us": record.updated_at.as_unix_micros(),
        "started_at_us": micros(record.started_at),
        "paused_at_us": micros(record.paused_at),
        "sterilized_at_us": micros(record.sterilized_at),
        "settlement": settlement,
        // Recorded only for an Unknown Job whose local execution was
        // confirmed stopped; it says nothing about the Provider.
        "stop_confirmed_at_us": micros(record.stop_confirmed_at),
    }))
}

fn output_json(record: &JobOutputRecord) -> Value {
    let kind = match record.kind {
        OutputKind::Progress => "progress",
        OutputKind::Evaluation => "evaluation",
        OutputKind::Result => "result",
    };
    json!({
        "id": record.id.to_string(),
        "seq": record.seq,
        "kind": kind,
        "summary": record.summary,
        "code": record.code,
        "goal_reached": record.goal_reached,
        "value": record.value,
        "created_at_us": record.created_at.as_unix_micros(),
    })
}

/// A page of the Job's outputs, exactly as stored and in `seq` order: as
/// many of the first `limit` after `after_seq` as fit the response, whole.
/// `next_after_seq` is the cursor of the next page, `after_seq` again when
/// none was returned; `has_more` says whether a later output was stored
/// when the page was read.
async fn outputs(
    shared: &Shared,
    job: JobId,
    after_seq: Option<i64>,
    limit: usize,
) -> Result<Value, Refusal> {
    let database = &shared.database;
    if database.job(job).await?.is_none() {
        return Err(JOB_NOT_FOUND);
    }
    let records = database.job_outputs(job, after_seq, limit + 1).await?;
    let envelope = |outputs: Vec<Value>, next: Option<i64>, more: bool| {
        json!({
            "api_version": API_VERSION,
            "job": job.to_string(),
            "after_seq": after_seq,
            "outputs": outputs,
            "next_after_seq": next,
            "has_more": more,
        })
    };
    let mut size = envelope(Vec::new(), Some(i64::MIN), true).to_string().len();
    let mut page = Vec::new();
    let mut next = after_seq;
    let mut more = records.len() > limit;
    for record in records.iter().take(limit) {
        let output = output_json(record);
        let cost = output.to_string().len() + 1;
        if size + cost > shared.response_bytes {
            if page.is_empty() {
                return Err(OUTPUT_TOO_LARGE);
            }
            more = true;
            break;
        }
        size += cost;
        page.push(output);
        next = Some(record.seq);
    }
    Ok(envelope(page, next, more))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source<'a>(
        values: &'a [(&'a str, &'a str)],
    ) -> impl Fn(&str) -> Option<Result<String, std::ffi::OsString>> + 'a {
        move |name| {
            values
                .iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| Ok((*value).to_owned()))
        }
    }

    const TOKEN: &str = "c2VjcmV0LXRva2VuLWZvci10ZXN0cy0wMTIzNDU2Nzg5";

    #[test]
    fn the_control_plane_is_off_unless_both_variables_are_set() {
        assert!(matches!(
            ControlPlaneConfig::from_source(source(&[])),
            Ok(None)
        ));
        assert_eq!(
            ControlPlaneConfig::from_source(source(&[(PORT_VARIABLE, "7840")])).err(),
            Some(ConfigError::TokenMissing)
        );
        assert_eq!(
            ControlPlaneConfig::from_source(source(&[(TOKEN_VARIABLE, TOKEN)])).err(),
            Some(ConfigError::PortMissing)
        );
        let config = ControlPlaneConfig::from_source(source(&[
            (PORT_VARIABLE, "7840"),
            (TOKEN_VARIABLE, TOKEN),
        ]))
        .expect("valid")
        .expect("enabled");
        assert_eq!(config.port(), 7840);
        assert!(
            !format!("{config:?}").contains(TOKEN),
            "the token is redacted"
        );
    }

    #[test]
    fn invalid_ports_and_weak_tokens_are_refused() {
        for port in ["0", "", "-1", "65536", "80a", " 80", "0x50"] {
            assert_eq!(
                ControlPlaneConfig::from_source(source(&[
                    (PORT_VARIABLE, port),
                    (TOKEN_VARIABLE, TOKEN),
                ]))
                .err(),
                Some(ConfigError::InvalidPort),
                "{port:?}"
            );
        }
        let short = &TOKEN[..MIN_TOKEN_CHARS - 1];
        let repeated = "ab".repeat(MIN_TOKEN_CHARS);
        let spaced = format!("{TOKEN} x");
        let long = "a1b2c3d4e5".repeat(60);
        for token in [
            "",
            short,
            &repeated,
            &spaced,
            &long,
            "ä".repeat(40).as_str(),
        ] {
            let refused = ControlPlaneConfig::from_source(source(&[
                (PORT_VARIABLE, "7840"),
                (TOKEN_VARIABLE, token),
            ]));
            assert_eq!(refused.err(), Some(ConfigError::InvalidToken), "{token:?}");
        }
        let error = ConfigError::InvalidToken.to_string();
        assert!(!error.contains(TOKEN));
        let not_unicode = ControlPlaneConfig::from_source(|name| {
            (name == PORT_VARIABLE)
                .then(|| Ok("7840".to_owned()))
                .or_else(|| Some(Err(std::ffi::OsString::from("x"))))
        });
        assert_eq!(not_unicode.err(), Some(ConfigError::InvalidToken));
    }

    #[test]
    fn tokens_are_compared_whole() {
        let token = Token(TOKEN.into());
        assert!(token.matches(TOKEN.as_bytes()));
        assert!(!token.matches(&TOKEN.as_bytes()[1..]));
        assert!(!token.matches(format!("{TOKEN}x").as_bytes()));
        assert!(!token.matches(b""));
    }

    #[test]
    fn ids_and_queries_are_parsed_strictly() {
        assert_eq!(job_id("job:7").ok(), Some(JobId::new(7)));
        for text in [
            "7", "job:", "job:07", "job:+7", "job:-7", "Job:7", "job:7 ", "job%3A7",
        ] {
            assert!(job_id(text).is_err(), "{text:?}");
        }
        assert!(job_id(&format!("job:{}0", u64::MAX)).is_err());
        assert_eq!(output_query("").ok(), Some((None, DEFAULT_OUTPUT_PAGE)));
        assert_eq!(
            output_query("after_seq=3&limit=10").ok(),
            Some((Some(3), 10))
        );
        assert_eq!(
            output_query("limit=100&after_seq=0").ok(),
            Some((Some(0), 100))
        );
        for query in [
            "limit=0",
            "limit=101",
            "limit=",
            "after_seq=-1",
            "after_seq=1&after_seq=2",
            "limit=1&limit=2",
            "token=abc",
            "access_token=abc",
            "after_seq",
            "after_seq=1&",
        ] {
            assert!(output_query(query).is_err(), "{query:?}");
        }
    }
}
