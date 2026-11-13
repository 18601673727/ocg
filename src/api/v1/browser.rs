use super::contracts::*;

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
use serde_json::Value;

use super::{
    BODY_TOO_LARGE, INTERNAL, LENGTH_REQUIRED, Refusal, Reply, UNSUPPORTED_MEDIA_TYPE, decimal,
    single,
};
use crate::db::entities::Timestamp;

pub(super) const COOKIE: &str = "ocg_session";
pub(super) const CSRF_HEADER: &str = "x-ocg-csrf";

const SESSION_LIFETIME: Duration = Duration::from_secs(8 * 60 * 60);
const MAX_SESSIONS: usize = 256;
const MAX_PAIRING_FAILURES: usize = 10;
const ID_BYTES: usize = 32;
const PAIRING_BYTES: usize = 24;
pub(super) const MAX_LOGIN_BYTES: usize = 1024;

const SESSION_DOMAIN: &[u8] = b"ocg.control-plane.browser-session.v1\n";

pub(super) const LOGIN_NOT_ACCEPTED: Refusal = LENGTH_REQUIRED
    .with_message("a sign-in must declare its one Content-Length and no Transfer-Encoding");
pub(super) const LOGIN_TOO_LARGE: Refusal =
    BODY_TOO_LARGE.with_message("the body is larger than a sign-in may be");
pub(super) const LOGIN_MEDIA_TYPE: Refusal =
    UNSUPPORTED_MEDIA_TYPE.with_message("a sign-in must be application/json");
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

#[derive(Clone)]
pub(super) struct Session {
    csrf: String,
    key: [u8; 32],
    expires_at_us: i64,
    last_seen_us: Option<i64>,
}

pub(super) struct State {
    pairing: String,
    failed: AtomicUsize,
    sessions: Mutex<HashMap<[u8; 32], Session>>,
}

pub(super) enum Presented {
    None,
    One(String),
    Ambiguous,
}

impl State {
    pub(super) fn start() -> Result<Self, Refusal> {
        Ok(Self {
            pairing: random_token(PAIRING_BYTES)?,
            failed: AtomicUsize::new(0),
            sessions: Mutex::new(HashMap::new()),
        })
    }

    pub(super) fn pairing_code(&self) -> &str {
        &self.pairing
    }

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
        let session = sessions.get_mut(&key)?;
        session.last_seen_us = Some(now);
        Some(session.clone())
    }

    pub(super) fn csrf_matches(&self, session: &Session, headers: &HeaderMap) -> bool {
        single(headers, &header::HeaderName::from_static(CSRF_HEADER))
            .and_then(|value| value.to_str().ok())
            .is_some_and(|presented| matches_secret(&session.csrf, presented))
    }

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

    pub(super) fn begin_session(&self) -> Result<Reply, Refusal> {
        let id = random_token(ID_BYTES)?;
        let csrf = random_token(ID_BYTES)?;
        let now = Timestamp::now().as_unix_micros();
        let expires_at_us = now + i64::try_from(SESSION_LIFETIME.as_micros()).unwrap_or(i64::MAX);
        let session = Session {
            csrf,
            key: digest_key(id.as_bytes()),
            expires_at_us,
            last_seen_us: None,
        };
        let cookie = format!(
            "{COOKIE}={id}; Path=/; HttpOnly; SameSite=Strict; Max-Age={}",
            SESSION_LIFETIME.as_secs()
        );
        let mut sessions = self.sessions.lock().unwrap_or_else(PoisonError::into_inner);
        sessions.retain(|_, session| session.expires_at_us > now);
        while sessions.len() >= MAX_SESSIONS {
            let Some((&evict, _)) = sessions
                .iter()
                .min_by_key(|(_, session)| (session.last_seen_us, session.expires_at_us))
            else {
                break;
            };
            sessions.remove(&evict);
        }
        sessions.insert(session.key, session.clone());
        Ok(Reply::with_cookie(
            StatusCode::CREATED,
            described(&session),
            cookie,
        ))
    }

    pub(super) fn logout(&self, session: &Session) -> Reply {
        self.sessions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&session.key);
        Reply::with_cookie(
            StatusCode::OK,
            envelope(serde_json::json!(SessionResponseView {
                session: SessionView::SignedOut(SignedOutView {
                    authenticated: false,
                }),
            })),
            format!("{COOKIE}=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0"),
        )
    }
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "SignedOut")]
pub(crate) struct SignedOutView {
    #[ts(type = "false")]
    authenticated: bool,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "SignedIn")]
pub(crate) struct SignedInView {
    #[ts(type = "true")]
    authenticated: bool,
    csrf_token: String,
    expires_at_us: i64,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[serde(untagged)]
#[ts(rename = "Session")]
pub(crate) enum SessionView {
    SignedOut(SignedOutView),
    SignedIn(SignedInView),
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "SessionResponse")]
pub(crate) struct SessionResponseView {
    session: SessionView,
}

pub(super) fn described(session: &Session) -> Value {
    envelope(serde_json::json!(SessionResponseView {
        session: SessionView::SignedIn(SignedInView {
            authenticated: true,
            csrf_token: session.csrf.clone(),
            expires_at_us: session.expires_at_us,
        }),
    }))
}

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

fn matches_secret(expected: &str, presented: &str) -> bool {
    let expected = digest(&SHA256, expected.as_bytes());
    let presented = digest(&SHA256, presented.as_bytes());
    verify_slices_are_equal(expected.as_ref(), presented.as_ref()).is_ok()
}

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

fn random_token(bytes: usize) -> Result<String, Refusal> {
    let mut buffer = vec![0u8; bytes];
    SystemRandom::new()
        .fill(&mut buffer)
        .map_err(|_| INTERNAL)?;
    Ok(URL_SAFE_NO_PAD.encode(buffer))
}
