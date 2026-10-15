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
//! - use `GET`, or `POST` on a route that submits or stops;
//! - name the listener exactly in `Host`: `127.0.0.1:<port>` or
//!   `localhost:<port>` with the bound port, so a page served from any
//!   other name, rebound to this address, is refused;
//! - when it carries `Origin`, name the same origin over `http`, and when
//!   it carries `Sec-Fetch-Site`, be `same-origin` or `none`;
//! - carry `Authorization: Bearer <token>` with the configured token;
//! - carry no body, except a submission, whose body must be JSON, declared
//!   `application/json` with its one `Content-Length`, at most
//!   [`MAX_SUBMISSION_BYTES`].
//!
//! The connection's loopback peer authorizes nothing. No response carries
//! a CORS header, a cookie or the token, and no route serves a file. A
//! request is answered with a small JSON body, an error with a fixed code
//! and message that repeats nothing the request or the database said.
//!
//! Routes, all versioned under `/api/v1`:
//!
//! - `GET /api/v1/health`: what the host can establish: it is running, and
//!   whether the database reads and accepts commands. Nothing about
//!   Providers or credentials.
//! - `GET /api/v1/jobs/{job}`: the Job's persisted lifecycle, with `job`
//!   written as OCG writes it, `job:<n>`.
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
//!   "executor:<n>", "objective": "...", "name": "..."}`, `name` optional:
//!   an existing LLM Profile and an Executor that can run it, both checked
//!   here and again when the Job is created. No tool is offered, and
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

use std::fmt;
use std::net::{Ipv4Addr, SocketAddr, TcpListener};
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
use crate::persistence::{ExecutionMode, JobId, JobOutputRecord, JobRecord, OutputKind};

mod events;
mod submissions;

#[cfg(test)]
pub(crate) use events::events_seam;
#[cfg(test)]
pub(crate) use submissions::resolution_seam;

/// The loopback TCP port the listener binds, from 1 to 65535.
pub const PORT_VARIABLE: &str = "OCG_CONTROL_PORT";
/// The bearer token every request must present. It is never logged.
pub const TOKEN_VARIABLE: &str = "OCG_CONTROL_TOKEN";

/// The version of the JSON this listener answers.
pub const API_VERSION: u32 = 1;

/// The shortest token accepted, in characters.
pub const MIN_TOKEN_CHARS: usize = 32;
/// The longest token accepted, in characters.
const MAX_TOKEN_CHARS: usize = 512;
/// The fewest distinct characters a token may have.
const MIN_TOKEN_DISTINCT: usize = 8;

/// The largest submission body, in bytes.
pub const MAX_SUBMISSION_BYTES: usize = 64 * 1024;

/// The most outputs one page returns, and the default.
pub const MAX_OUTPUT_PAGE: usize = 100;
const DEFAULT_OUTPUT_PAGE: usize = 50;

/// The longest path and query a request may have, in bytes.
const MAX_PATH_BYTES: usize = 256;
const MAX_QUERY_BYTES: usize = 128;
/// The most headers, and header bytes, a request may have.
const MAX_HEADERS: usize = 32;
const MAX_HEADER_BYTES: usize = 8 * 1024;

/// The largest response body, by default.
const RESPONSE_BYTES: usize = 1024 * 1024;

/// How long a stop waits for requests in flight before it closes them.
const STOP_TIMEOUT: Seconds = Seconds(5);

/// How long a submission waits for its admission before it answers
/// `pending`. The admission runs on either way.
const ADMISSION_WAIT: Duration = Duration::from_secs(3);

/// The bearer token. Its value is never displayed.
#[derive(Clone)]
struct Token(Arc<str>);

impl fmt::Debug for Token {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Token(<redacted>)")
    }
}

impl Token {
    /// Whether `presented` is the token, compared in constant time over
    /// digests, so neither its content nor its length is timed.
    fn matches(&self, presented: &[u8]) -> bool {
        let expected = digest(&SHA256, self.0.as_bytes());
        let presented = digest(&SHA256, presented);
        verify_slices_are_equal(expected.as_ref(), presented.as_ref()).is_ok()
    }
}

/// Why the Control Plane configuration is refused. It never repeats the
/// token.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ConfigError {
    #[error("{PORT_VARIABLE} is set but {TOKEN_VARIABLE} is not; the control plane needs both")]
    TokenMissing,
    #[error("{TOKEN_VARIABLE} is set but {PORT_VARIABLE} is not; the control plane needs both")]
    PortMissing,
    #[error("{PORT_VARIABLE} must be a TCP port from 1 to 65535")]
    InvalidPort,
    #[error(
        "{TOKEN_VARIABLE} must be a random token of {MIN_TOKEN_CHARS} to {MAX_TOKEN_CHARS} characters of A-Z, a-z, 0-9, '-', '.', '_', '~', '+', '/' or '='"
    )]
    InvalidToken,
}

/// What the operator configured for the Control Plane.
#[derive(Clone, Debug)]
pub struct ControlPlaneConfig {
    port: u16,
    token: Token,
    /// The largest response body.
    response_bytes: usize,
    /// How long a submission waits for its admission.
    admission_wait: Duration,
    /// The bounds of event streams.
    stream_limits: events::StreamLimits,
}

impl ControlPlaneConfig {
    /// The configuration in OCG's environment: `None` when neither variable
    /// is set, which leaves the Control Plane off.
    ///
    /// # Errors
    ///
    /// Returns why a configuration that was given is refused.
    pub fn from_environment() -> Result<Option<Self>, ConfigError> {
        Self::from_source(|name| std::env::var_os(name).map(|value| value.into_string()))
    }

    /// The configuration in the variables `source` gives; a value that is
    /// not Unicode is `Some(Err(_))`.
    ///
    /// # Errors
    ///
    /// As [`Self::from_environment`].
    pub fn from_source(
        source: impl Fn(&str) -> Option<Result<String, std::ffi::OsString>>,
    ) -> Result<Option<Self>, ConfigError> {
        match (source(PORT_VARIABLE), source(TOKEN_VARIABLE)) {
            (None, None) => Ok(None),
            (Some(_), None) => Err(ConfigError::TokenMissing),
            (None, Some(_)) => Err(ConfigError::PortMissing),
            (Some(port), Some(token)) => {
                let port = port
                    .ok()
                    .and_then(|port| port.parse::<u16>().ok())
                    .filter(|port| *port != 0)
                    .ok_or(ConfigError::InvalidPort)?;
                let token = token.map_err(|_| ConfigError::InvalidToken)?;
                Self::new(port, &token).map(Some)
            }
        }
    }

    /// A configuration of `port` and `token`, checked as the environment's
    /// is, except that port 0, any free port, is allowed.
    ///
    /// # Errors
    ///
    /// [`ConfigError::InvalidToken`] for a token that is refused.
    pub fn new(port: u16, token: &str) -> Result<Self, ConfigError> {
        let length = token.chars().count();
        let distinct = token
            .chars()
            .collect::<std::collections::BTreeSet<_>>()
            .len();
        if !(MIN_TOKEN_CHARS..=MAX_TOKEN_CHARS).contains(&length)
            || distinct < MIN_TOKEN_DISTINCT
            || !token
                .chars()
                .all(|character| character.is_ascii_alphanumeric() || "-._~+/=".contains(character))
        {
            return Err(ConfigError::InvalidToken);
        }
        Ok(Self {
            port,
            token: Token(token.into()),
            response_bytes: RESPONSE_BYTES,
            admission_wait: ADMISSION_WAIT,
            stream_limits: events::StreamLimits::default(),
        })
    }

    /// The configured port; 0 for any free one.
    pub fn port(&self) -> u16 {
        self.port
    }

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

/// Why the listener did not start.
#[derive(Debug, thiserror::Error)]
pub enum StartError {
    #[error("the control plane cannot listen on 127.0.0.1:{port}: {source}")]
    Bind {
        port: u16,
        #[source]
        source: std::io::Error,
    },
    #[error("the control plane could not start serving: {0}")]
    Serve(#[source] std::io::Error),
}

/// Why the listener did not stop cleanly.
#[derive(Debug, thiserror::Error)]
pub enum StopError {
    #[error("the control plane thread panicked")]
    Panicked,
}

/// A running listener. Only [`ControlPlane::stop`] stops it.
pub struct ControlPlane {
    address: SocketAddr,
    server: Server,
    thread: std::thread::JoinHandle<()>,
    shared: Arc<Shared>,
}

impl ControlPlane {
    /// Binds `127.0.0.1` at the configured port, then starts answering on
    /// a thread of its own. Nothing is answered unless the bind succeeded.
    /// Jobs are submitted to `scheduler`, which must be the host's own.
    ///
    /// # Errors
    ///
    /// Returns why the listener cannot be bound.
    pub fn start(
        config: ControlPlaneConfig,
        database: DatabaseHandle,
        scheduler: SchedulerHandle,
    ) -> Result<Self, StartError> {
        let bind = |source| StartError::Bind {
            port: config.port,
            source,
        };
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, config.port)).map_err(bind)?;
        let address = listener.local_addr().map_err(bind)?;
        let shared = Arc::new(Shared {
            database,
            scheduler,
            submissions: submissions::Registry::default(),
            token: config.token,
            port: address.port(),
            response_bytes: config.response_bytes,
            admission_wait: config.admission_wait,
            stream_limits: config.stream_limits,
            streams: AtomicUsize::new(0),
            closing: AtomicBool::new(false),
            #[cfg(test)]
            fetched: AtomicUsize::new(0),
        });
        let kept = shared.clone();
        let (started, starting) = flume::bounded(1);
        let thread = std::thread::Builder::new()
            .name("ocg-control-plane".to_owned())
            .spawn(move || {
                // A runtime of its own, without signals: the server's
                // signal settings apply to this runtime only.
                ntex::rt::System::new("ocg-control-plane", ntex::rt::DefaultRuntime).block_on(
                    async move {
                        let server = HttpServer::new(async move || {
                            App::new()
                                .state(shared.clone())
                                .default_service(web::to(answer))
                        })
                        .workers(1)
                        .maxconn(64)
                        .backlog(64)
                        .disable_signals()
                        .shutdown_timeout(STOP_TIMEOUT)
                        .config(
                            SharedCfg::new("OCG-CONTROL").add(IoConfig::new()).add(
                                HttpServiceConfig::new()
                                    .set_max_headers(MAX_HEADERS)
                                    .set_max_buf_size(16 * 1024),
                            ),
                        )
                        .listen(listener);
                        match server {
                            Ok(server) => {
                                let server = server.run();
                                started.send(Ok(server.clone())).ok();
                                // Resolves once the server has stopped.
                                server.await.ok();
                            }
                            Err(error) => {
                                started.send(Err(error)).ok();
                            }
                        }
                    },
                );
            })
            .map_err(StartError::Serve)?;
        let server = match starting.recv() {
            Ok(Ok(server)) => server,
            Ok(Err(error)) => {
                thread.join().ok();
                return Err(StartError::Serve(error));
            }
            Err(_) => {
                thread.join().ok();
                return Err(StartError::Serve(std::io::Error::other(
                    "the control plane thread stopped before serving",
                )));
            }
        };
        Ok(Self {
            address,
            server,
            thread,
            shared: kept,
        })
    }

    /// The address it listens on.
    pub fn address(&self) -> SocketAddr {
        self.address
    }

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

    /// Stops accepting, lets requests in flight finish within a bounded
    /// time, then closes them and the listening socket, and joins the
    /// listener's thread. Event streams end at their next read, without a
    /// settlement, for their clients to resume. Admissions still running
    /// are dropped with it: none of them owns its Job, which the execution
    /// runtime settles or refuses as it stops, and whose record a later
    /// lookup reads.
    ///
    /// # Errors
    ///
    /// Returns [`StopError::Panicked`] when the listener's thread panicked.
    pub async fn stop(self) -> Result<(), StopError> {
        self.shared.closing.store(true, Ordering::SeqCst);
        self.server.stop(true).await;
        self.thread.join().map_err(|_| StopError::Panicked)
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

/// An answered request: its status and JSON body.
struct Reply {
    status: StatusCode,
    body: Value,
}

impl Reply {
    fn new(status: StatusCode, body: Value) -> Self {
        Self { status, body }
    }

    fn ok(body: Value) -> Self {
        Self::new(StatusCode::OK, body)
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
    match admit(&request, &shared).and_then(|()| route(&request)) {
        Ok(Route::Events { job, after_seq }) => {
            events::open(&shared, request.io().cloned(), job, after_seq)
                .await
                .unwrap_or_else(|refusal| refusal.response())
        }
        Ok(route) => match respond(route, &request, payload, &shared).await {
            Ok(reply) => json_response(reply.status, &reply.body),
            Err(refusal) => refusal.response(),
        },
        Err(refusal) => refusal.response(),
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

/// Checks everything about a request but its route.
fn admit(request: &HttpRequest, shared: &Shared) -> Result<(), Refusal> {
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
    let takes_post = takes_post(request.path());
    let method = request.method();
    if method != Method::GET && !(takes_post && method == Method::POST) {
        return Err(if takes_post {
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
    if !authenticated(headers, &shared.token) {
        return Err(UNAUTHENTICATED);
    }
    if method == Method::POST && request.path() == "/api/v1/jobs" {
        return submission_body(headers);
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
    Ok(())
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
            _ => Err(NOT_FOUND),
        };
    }
    match segments.as_slice() {
        ["health"] => no_query(query).map(|()| Route::Health),
        ["admissions"] => no_query(query).map(|()| Route::Admission),
        ["jobs", job] => {
            let job = job_id(job)?;
            no_query(query).map(|()| Route::Job(job))
        }
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
