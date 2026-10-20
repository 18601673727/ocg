//! The local operator's browser session: how a browser on this machine
//! signs in, stays signed in and signs out.
//!
//! A browser never handles [`super::TOKEN_VARIABLE`]'s token: it signs in
//! once with the pairing code this run printed to its own operator, and
//! from then on carries one session cookie. The session exists only in
//! this process, holds at most one at a time, expires after
//! [`SESSION_LIFETIME`], and dies with the process: a run that cannot
//! validate a session answers as unauthenticated, never as the operator.
//! The bearer token and a session are separate credentials; neither
//! grants the other.
//!
//! A sign-in is bounded: the pairing code is drawn from the system's
//! random source when the listener starts, is compared without leaking
//! where it stopped matching, and is never sent back to any client. After
//! [`MAX_PAIRING_FAILURES`] failed sign-ins the code is closed for the
//! rest of the run. The code is not the token and reveals nothing of it.
//!
//! Every state-changing request of a session must also carry that
//! session's CSRF token in [`CSRF_HEADER`], a header a cross-site request
//! cannot set: together with the `SameSite=Strict` cookie and the
//! listener's own Host and Origin checks, a page from any other origin
//! can neither use nor forge the session.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use aws_lc_rs::constant_time::verify_slices_are_equal;
use aws_lc_rs::digest::{Context, SHA256, digest};
use aws_lc_rs::rand::{SecureRandom, SystemRandom};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ntex::http::StatusCode;
use ntex::http::header::{self, HeaderMap};
use serde::Deserialize;
use serde_json::{Value, json};

use super::{
    API_VERSION, BODY_TOO_LARGE, INTERNAL, LENGTH_REQUIRED, Refusal, Reply, UNSUPPORTED_MEDIA_TYPE,
    decimal, single,
};
use crate::persistence::Timestamp;

/// The session cookie a signed-in browser carries.
pub(super) const COOKIE: &str = "ocg_session";
/// The header a state-changing request carries its session's CSRF token in.
pub(super) const CSRF_HEADER: &str = "x-ocg-csrf";

/// How long one session is authorized; a sign-in is explicit again after.
const SESSION_LIFETIME: Duration = Duration::from_secs(8 * 60 * 60);
/// The most failed sign-ins before the pairing code is closed for the run.
const MAX_PAIRING_FAILURES: usize = 10;
/// The random bytes behind a session id, a CSRF token and the code.
const ID_BYTES: usize = 32;
const PAIRING_BYTES: usize = 24;
/// The largest sign-in body, in bytes.
pub(super) const MAX_LOGIN_BYTES: usize = 1024;

const SESSION_DOMAIN: &[u8] = b"ocg.control-plane.browser-session.v1\n";

pub(super) const LOGIN_NOT_ACCEPTED: Refusal = Refusal {
    message: "a sign-in must declare its one Content-Length and no Transfer-Encoding",
    ..LENGTH_REQUIRED
};
pub(super) const LOGIN_TOO_LARGE: Refusal = Refusal {
    message: "the body is larger than a sign-in may be",
    ..BODY_TOO_LARGE
};
pub(super) const LOGIN_MEDIA_TYPE: Refusal = Refusal {
    message: "a sign-in must be application/json",
    ..UNSUPPORTED_MEDIA_TYPE
};
const INVALID_LOGIN: Refusal = Refusal::new(
    StatusCode::BAD_REQUEST,
    "invalid_body",
    "the body is not a sign-in this API accepts",
);
const INVALID_PAIRING: Refusal = Refusal::new(
    StatusCode::UNAUTHORIZED,
    "invalid_pairing_code",
    "the pairing code is not the one this run printed",
);
const PAIRING_DISABLED: Refusal = Refusal::new(
    StatusCode::FORBIDDEN,
    "pairing_disabled",
    "too many sign-ins failed; signing in is closed for this run",
);
pub(super) const FORBIDDEN_CSRF: Refusal = Refusal::new(
    StatusCode::FORBIDDEN,
    "forbidden_csrf",
    "the request does not carry its session's CSRF token",
);

/// A signed-in session: what authorizes its requests until it expires.
#[derive(Clone)]
pub(super) struct Session {
    /// The CSRF token its state-changing requests must present.
    csrf: String,
    /// The digest of its cookie value: how it is kept and removed.
    key: [u8; 32],
    expires_at_us: i64,
}

/// Everything the listener holds about browser sign-in: one pairing code,
/// and the one session a single operator may hold.
pub(super) struct State {
    pairing: String,
    failed: AtomicUsize,
    sessions: Mutex<HashMap<[u8; 32], Session>>,
}

/// What the request's cookie says, before any of it is believed.
pub(super) enum Presented {
    /// No session cookie.
    None,
    /// One session cookie value.
    One(String),
    /// More than one: refused rather than guessed at.
    Ambiguous,
}

impl State {
    /// A fresh pairing code, for this run only.
    pub(super) fn start() -> Result<Self, Refusal> {
        Ok(Self {
            pairing: random_token(PAIRING_BYTES)?,
            failed: AtomicUsize::new(0),
            sessions: Mutex::new(HashMap::new()),
        })
    }

    /// The code its operator signs in with. It is shown to the operator
    /// alone, and is not the bearer token.
    pub(super) fn pairing_code(&self) -> &str {
        &self.pairing
    }

    /// The live session the request's cookie names, if any: an absent,
    /// malformed, ambiguous, unknown or expired cookie is answered as
    /// unauthenticated.
    pub(super) fn session(&self, presented: &Presented) -> Option<Session> {
        let Presented::One(id) = presented else {
            return None;
        };
        if URL_SAFE_NO_PAD.decode(id).ok()?.len() != ID_BYTES {
            return None;
        }
        let key = digest_key(id.as_bytes());
        let mut sessions = self.sessions.lock().unwrap_or_else(PoisonError::into_inner);
        let now = Timestamp::now().as_unix_micros();
        sessions.retain(|_, session| session.expires_at_us > now);
        // The key is the digest of the cookie value: a found entry is the
        // one session that value named.
        sessions.get(&key).cloned()
    }

    /// Whether the request carries `session`'s CSRF token, once, in its
    /// header: the one thing a page of another origin cannot present.
    pub(super) fn csrf_matches(&self, session: &Session, headers: &HeaderMap) -> bool {
        single(headers, &header::HeaderName::from_static(CSRF_HEADER))
            .and_then(|value| value.to_str().ok())
            .is_some_and(|presented| matches_secret(&session.csrf, presented))
    }

    /// Signs one browser in under this run's pairing code: the one session
    /// this operator holds, replacing any earlier one. Its cookie value
    /// and its CSRF token are drawn from the system's random source and
    /// kept only in this process.
    pub(super) fn login(&self, body: &[u8]) -> Result<Reply, Refusal> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Login {
            pairing_code: String,
        }
        let request: Login = serde_json::from_slice(body).map_err(|_| INVALID_LOGIN)?;
        if self.failed.load(Ordering::SeqCst) >= MAX_PAIRING_FAILURES {
            return Err(PAIRING_DISABLED);
        }
        if !matches_secret(&self.pairing, &request.pairing_code) {
            self.failed.fetch_add(1, Ordering::SeqCst);
            return Err(INVALID_PAIRING);
        }
        self.begin_session()
    }

    /// Starts the one session this operator holds without a pairing code.
    /// Only the web surface uses it: the frontend that this listener serves
    /// signs itself in, and the code remains for a remote control client of
    /// a headless listener.
    pub(super) fn begin_session(&self) -> Result<Reply, Refusal> {
        let id = random_token(ID_BYTES)?;
        let csrf = random_token(ID_BYTES)?;
        let expires_at_us = Timestamp::now().as_unix_micros()
            + i64::try_from(SESSION_LIFETIME.as_micros()).unwrap_or(i64::MAX);
        let session = Session {
            csrf,
            key: digest_key(id.as_bytes()),
            expires_at_us,
        };
        let cookie = format!(
            "{COOKIE}={id}; Path=/; HttpOnly; SameSite=Strict; Max-Age={}",
            SESSION_LIFETIME.as_secs()
        );
        let mut sessions = self.sessions.lock().unwrap_or_else(PoisonError::into_inner);
        // Explicitly single-operator: one live session at a time.
        sessions.clear();
        sessions.insert(session.key, session.clone());
        Ok(Reply::with_cookie(
            StatusCode::CREATED,
            described(&session),
            cookie,
        ))
    }

    /// Signs the session out: its cookie stops authorizing at once, in
    /// this process and in the browser.
    pub(super) fn logout(&self, session: &Session) -> Reply {
        self.sessions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&session.key);
        Reply::with_cookie(
            StatusCode::OK,
            json!({"api_version": API_VERSION, "session": {"authenticated": false}}),
            format!("{COOKIE}=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0"),
        )
    }
}

/// What a signed-in request may read of its own session.
pub(super) fn described(session: &Session) -> Value {
    json!({
        "api_version": API_VERSION,
        "session": {
            "authenticated": true,
            "csrf_token": session.csrf,
            "expires_at_us": session.expires_at_us,
        },
    })
}

/// A sign-in's body is declared JSON, with exactly one length, and no
/// larger than a sign-in may be, before any of it is read.
pub(super) fn login_body(headers: &HeaderMap) -> Result<(), Refusal> {
    let mut lengths = headers.get_all(header::CONTENT_LENGTH);
    let length = match (lengths.next(), lengths.next()) {
        (Some(length), None) if !headers.contains_key(header::TRANSFER_ENCODING) => length,
        _ => return Err(LOGIN_NOT_ACCEPTED),
    };
    let length = length
        .to_str()
        .ok()
        .and_then(decimal::<usize>)
        .ok_or(LOGIN_NOT_ACCEPTED)?;
    if length > MAX_LOGIN_BYTES {
        return Err(LOGIN_TOO_LARGE);
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
    if json { Ok(()) } else { Err(LOGIN_MEDIA_TYPE) }
}

/// The one session cookie the request carries, before it is checked.
pub(super) fn presented(headers: &HeaderMap) -> Presented {
    let mut found: Option<String> = None;
    let cookies = headers.get_all(header::COOKIE);
    for cookie in cookies {
        let Ok(cookie) = cookie.to_str() else {
            return Presented::Ambiguous;
        };
        for pair in cookie.split(';') {
            let pair = pair.trim();
            let Some((name, value)) = pair.split_once('=') else {
                continue;
            };
            if name.trim() != COOKIE {
                continue;
            }
            if found.is_some() {
                return Presented::Ambiguous;
            }
            found = Some(value.trim().to_owned());
        }
    }
    found.map_or(Presented::None, Presented::One)
}

/// Whether `presented` is `expected`, compared in constant time over
/// digests, so neither its content nor its length is timed.
fn matches_secret(expected: &str, presented: &str) -> bool {
    let expected = digest(&SHA256, expected.as_bytes());
    let presented = digest(&SHA256, presented.as_bytes());
    verify_slices_are_equal(expected.as_ref(), presented.as_ref()).is_ok()
}

/// How a session is found: the digest of its cookie value, with the
/// session's domain, never the value itself.
fn digest_key(value: &[u8]) -> [u8; 32] {
    let mut context = Context::new(&SHA256);
    context.update(SESSION_DOMAIN);
    context.update(value);
    context
        .finish()
        .as_ref()
        .try_into()
        .expect("SHA-256 has 32 bytes")
}

/// `bytes` random bytes, URL-safe: a cookie value, a CSRF token or the
/// pairing code. A system that cannot draw them refuses to sign anyone in.
fn random_token(bytes: usize) -> Result<String, Refusal> {
    let mut buffer = vec![0u8; bytes];
    SystemRandom::new()
        .fill(&mut buffer)
        .map_err(|_| INTERNAL)?;
    Ok(URL_SAFE_NO_PAD.encode(buffer))
}
