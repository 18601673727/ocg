use super::*;

/// The collections whose members are changed with `POST`.
const MEMBER_COLLECTIONS: [&str; 4] = ["profiles", "projects", "pools", "conversations"];

/// Whether `rest` names one member of `collection`.
fn member(rest: &str, collection: &str) -> bool {
    rest.strip_prefix(collection)
        .and_then(|rest| rest.strip_prefix('/'))
        .is_some_and(|id| !id.is_empty() && !id.contains('/'))
}

fn conversation_action(rest: &str) -> bool {
    let mut segments = rest.split('/');
    matches!((segments.next(), segments.next(), segments.next(), segments.next()), (Some("conversations"), Some(id), Some("stop" | "close" | "hold" | "unhold" | "retry"), None) if !id.is_empty())
}

/// Whether `path` names a route that is submitted to with `POST`: Job
/// submission, a Job's stop, a Profile, Project, Pool or conversation
/// change, or an image upload.
fn takes_post(path: &str) -> bool {
    match path.strip_prefix("/api/v1/") {
        Some("jobs" | "profiles" | "projects" | "provider-models" | "attachments") => true,
        Some(rest) => {
            conversation_action(rest)
                || MEMBER_COLLECTIONS
                    .iter()
                    .any(|collection| member(rest, collection))
        }
        None => false,
    }
}

/// The largest JSON body `path` accepts with `POST`, or `None` when it
/// accepts none.
fn json_post_limit(path: &str) -> Option<usize> {
    match path.strip_prefix("/api/v1/") {
        Some("attachments") => Some(MAX_ATTACHMENT_UPLOAD_BYTES),
        Some("jobs" | "profiles" | "projects" | "provider-models") => Some(MAX_SUBMISSION_BYTES),
        Some(rest) => (conversation_action(rest)
            || MEMBER_COLLECTIONS
                .iter()
                .any(|collection| member(rest, collection)))
        .then_some(MAX_SUBMISSION_BYTES),
        None => None,
    }
}

/// Where an admitted request is answered from.
pub(super) enum Destination {
    /// The exported frontend's files.
    Asset,
    /// An API route, for one caller.
    Api(Caller),
}

/// Who an API request is.
#[derive(Clone)]
pub(super) enum Caller {
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
pub(super) fn admit(request: &HttpRequest, shared: &Shared) -> Result<Destination, Refusal> {
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
    } else if login
        && *method == Method::GET
        && shared.assets.is_some()
        && headers.get_all(header::AUTHORIZATION).count() == 0
    {
        // The frontend this listener serves signs itself in: a browser with
        // no live session, or one from an earlier run, is answered as
        // anonymous here so the route can begin one. The pairing code stays
        // for a headless listener's remote control client.
        authenticate(headers, shared).unwrap_or(Caller::Anonymous)
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
    if method == Method::POST
        && let Some(limit) = json_post_limit(path)
    {
        return submission_body(headers, limit).map(|()| Destination::Api(caller));
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
