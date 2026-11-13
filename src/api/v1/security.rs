use super::*;

const MEMBER_COLLECTIONS: [&str; 5] = [
    "profiles",
    "projects",
    "pools",
    "executors",
    "conversations",
];

fn member(rest: &str, collection: &str) -> bool {
    rest.strip_prefix(collection)
        .and_then(|rest| rest.strip_prefix('/'))
        .is_some_and(|id| !id.is_empty() && !id.contains('/'))
}

fn executor_archive(rest: &str) -> bool {
    let segments: Vec<_> = rest.split('/').collect();
    matches!(segments.as_slice(), ["executors", id, "archive"] if !id.is_empty())
}

fn pool_archive(rest: &str) -> bool {
    let segments: Vec<_> = rest.split('/').collect();
    matches!(segments.as_slice(), ["pools", id, "archive"] if !id.is_empty())
}

fn decision(rest: &str) -> bool {
    let segments: Vec<_> = rest.split('/').collect();
    matches!(segments.as_slice(), ["projects", id, "decision"] if !id.is_empty())
}

fn profile_availability(rest: &str) -> bool {
    let segments: Vec<_> = rest.split('/').collect();
    matches!(segments.as_slice(), ["profiles", id, "availability"] if !id.is_empty())
}

fn conversation_action(rest: &str) -> bool {
    let mut segments = rest.split('/');
    matches!((segments.next(), segments.next(), segments.next(), segments.next()), (Some("conversations"), Some(id), Some("stop" | "close" | "hold" | "unhold" | "retry" | "compact" | "queue"), None) if !id.is_empty())
}

fn approval_route(rest: &str) -> bool {
    let segments = rest.split('/').collect::<Vec<_>>();
    match segments.as_slice() {
        ["approvals", job, request, index] => {
            !job.is_empty() && !request.is_empty() && !index.is_empty()
        }
        _ => false,
    }
}

fn mutable_job_write(rest: &str) -> bool {
    let segments = rest.split('/').collect::<Vec<_>>();
    matches!(
        segments.as_slice(),
        ["jobs"]
            | ["jobs", _]
            | ["jobs", _, "submit"]
            | ["jobs", _, "cancel-unsupported"]
            | ["jobs", _, "stop"]
    )
}

fn takes_post(path: &str) -> bool {
    if path == "/api/v1/executors" || path == "/api/v1/pools" {
        return true;
    }
    match path.strip_prefix("/api/v1/") {
        Some(
            "update"
            | "jobs"
            | "decisions"
            | "profiles"
            | "projects"
            | "provider-models"
            | "attachments"
            | "settings/compaction"
            | "settings/outbound-headers",
        ) => true,
        Some(rest) => {
            mutable_job_write(rest)
                || conversation_action(rest)
                || executor_archive(rest)
                || pool_archive(rest)
                || decision(rest)
                || profile_availability(rest)
                || approval_route(rest)
                || MEMBER_COLLECTIONS
                    .iter()
                    .any(|collection| member(rest, collection))
        }
        None => false,
    }
}

fn takes_delete(path: &str) -> bool {
    path.strip_prefix("/api/v1/").is_some_and(|rest| {
        member(rest, "profiles") || member(rest, "providers") || member(rest, "jobs")
    })
}

fn json_post_limit(path: &str) -> Option<usize> {
    if path == "/api/v1/executors" || path == "/api/v1/pools" {
        return Some(MAX_SUBMISSION_BYTES);
    }
    match path.strip_prefix("/api/v1/") {
        Some("attachments") => Some(MAX_ATTACHMENT_UPLOAD_BYTES),
        Some("update") => Some(update::UPDATE_BODY_LIMIT),
        Some(
            "jobs"
            | "decisions"
            | "profiles"
            | "projects"
            | "provider-models"
            | "settings/compaction"
            | "settings/outbound-headers",
        ) => Some(MAX_SUBMISSION_BYTES),
        Some(rest) => (mutable_job_write(rest)
            || executor_archive(rest)
            || pool_archive(rest)
            || decision(rest)
            || profile_availability(rest)
            || conversation_action(rest)
            || approval_route(rest)
            || MEMBER_COLLECTIONS
                .iter()
                .any(|collection| member(rest, collection)))
        .then_some(MAX_SUBMISSION_BYTES),
        None => None,
    }
}

pub(super) enum Destination {
    Asset,
    Api(Caller),
}

#[derive(Clone)]
pub(super) enum Caller {
    Programmatic,
    Operator(browser::Session),
    Anonymous,
}

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
    let takes_delete = takes_delete(path);
    let method = request.method();
    let allowed = if login {
        *method == Method::GET || *method == Method::POST || *method == Method::DELETE
    } else if api {
        *method == Method::GET
            || (takes_post && *method == Method::POST)
            || (takes_delete && *method == Method::DELETE)
    } else {
        *method == Method::GET
    };
    if !allowed {
        return Err(if login {
            SESSION_METHODS
        } else if takes_post {
            if takes_delete {
                PROFILE_METHODS
            } else {
                ONLY_POST
            }
        } else {
            METHOD_NOT_ALLOWED
        });
    }
    let Some(host) = names_listener(headers, shared.port) else {
        return Err(FORBIDDEN_HOST);
    };
    if api && !same_origin(headers, &host) {
        return Err(FORBIDDEN_ORIGIN);
    }
    if !api {
        return Ok(Destination::Asset);
    }
    let caller = if login && method == Method::POST {
        if headers.get_all(header::AUTHORIZATION).count() != 0 {
            return Err(NOT_FOUND);
        }
        Caller::Anonymous
    } else if login
        && *method == Method::GET
        && shared.assets.is_some()
        && headers.get_all(header::AUTHORIZATION).count() == 0
    {
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

fn names_listener(headers: &HeaderMap, port: u16) -> Option<String> {
    let host = single(headers, &header::HOST)?
        .to_str()
        .ok()?
        .to_ascii_lowercase();
    (host == format!("127.0.0.1:{port}") || host == format!("localhost:{port}")).then_some(host)
}

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
