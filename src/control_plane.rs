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
//! - `GET /api/v1/health`: what the host can establish: it is running,
//!   whether the database reads and accepts commands, database vitals,
//!   claim timing, queue lengths, and process memory. Nothing about
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
//! - `GET /api/v1/projects`, `POST /api/v1/projects` and
//!   `POST /api/v1/projects/{project}`: Projects, each a root and the commands
//!   an Agent may run in it. See [`projects`].
//! - `GET /api/v1/pools` and `POST /api/v1/pools/{execution_pool}`: every
//!   Pool, and its concurrency limit or enabled state. See [`pools`].
//! - `POST /api/v1/conversations/{job}`: renames or hides a conversation
//!   whose root Job is `job`. See [`catalog`].
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
//!   created. Workspace tools follow the frozen Agent grant and the optional
//!   `"project": "project:<n>"`, which an Agent with a grant requires and a
//!   follow-up takes from its previous turn; nothing of a Provider,
//!   credential or command is accepted. The answer
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
//! - `POST /api/v1/conversations/{conversation}/{stop|close|hold|unhold|retry}`:
//!   manages the durable conversation; Stop cancels its current and waiting
//!   turns, while Close closes the live subtree. Turns and notices are paged
//!   through `GET /api/v1/conversations/{conversation}/turns` and `/notices`.
//! - `POST /api/v1/session`: signs the operator's browser in under the
//!   pairing code this run printed for a headless listener, and sets its
//!   session cookie; `GET /api/v1/session` reports that session and the CSRF
//!   token its requests carry, and, when a frontend is served, begins the
//!   session itself so the page needs no code; `DELETE /api/v1/session`
//!   revokes it and clears the cookie. The browser's alone: a bearer token
//!   reaches none of them. See [`browser`].
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
use crate::persistence::{
    AttachmentId, ExecutionPoolId, JobId, JobOutputRecord, JobRecord, NotInvokedReason, OutputKind,
    ProfileId, ProjectId, RequestNumber, ToolDecision, ToolEvidenceRecord, ToolOutcome,
    ToolOutcomeKind, UncertainCause, WithheldReason,
};

mod assets;
mod attachments;
mod browser;
mod catalog;
mod config;
mod dashboard;
mod events;
mod listener;
mod pools;
mod profiles;
mod projection;
mod projects;
mod routing;
mod security;
mod submissions;
mod wire;

#[cfg(test)]
pub(crate) use events::events_seam;
#[cfg(test)]
pub(crate) use submissions::resolution_seam;

pub use config::{
    API_VERSION, ConfigError, ControlPlaneConfig, MAX_ATTACHMENT_UPLOAD_BYTES, MAX_OUTPUT_PAGE,
    MAX_SUBMISSION_BYTES, MIN_TOKEN_CHARS, PORT_VARIABLE, TOKEN_VARIABLE, UI_VARIABLE,
};
use config::{
    DEFAULT_OUTPUT_PAGE, MAX_HEADER_BYTES, MAX_HEADERS, MAX_PATH_BYTES, MAX_QUERY_BYTES, Token,
};
pub use listener::{ControlPlane, StartError, StopError};
use projection::{health, job_json, micros, output_json, outputs, settlement_json};
use routing::{Route, authorize, decimal, respond, route};
#[cfg(test)]
use routing::{job_id, output_query};
use security::{Caller, Destination, admit};
use wire::{
    BODY_NOT_ACCEPTED, BODY_TOO_LARGE, DATABASE_BUSY, DATABASE_UNAVAILABLE, FORBIDDEN_HOST,
    FORBIDDEN_ORIGIN, HEADERS_TOO_LARGE, INCONSISTENT_RECORD, INTERNAL, INVALID_REQUEST,
    JOB_NOT_FOUND, LENGTH_REQUIRED, METHOD_NOT_ALLOWED, NOT_FOUND, ONLY_POST, OUTPUT_TOO_LARGE,
    Refusal, Reply, SESSION_METHODS, SHUTTING_DOWN, UNAUTHENTICATED, UNSUPPORTED_MEDIA_TYPE,
    URI_TOO_LONG, answer, read_body, single, submission_body,
};

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

    #[cfg(test)]
    pub(crate) fn with_port(mut self, port: u16) -> Self {
        self.port = port;
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
            self.shared.scheduler.running_count(),
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
    workspace_tools: crate::orchestration::execution_runtime::WorkspaceTools,
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
