use super::contracts::*;
use super::*;

/// A refused or failed request: its status and a fixed code and message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Refusal {
    pub(super) status: StatusCode,
    pub(super) code: &'static str,
    pub(super) message: &'static str,
    /// The methods a `405` names.
    pub(super) allow: &'static str,
}

impl Refusal {
    pub(super) const fn new(status: StatusCode, code: &'static str, message: &'static str) -> Self {
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
            &envelope(WireErrorResponse {
                error: WireError {
                    code: self.code.to_owned(),
                    message: self.message.to_owned(),
                },
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
pub(super) struct Reply {
    pub(super) status: StatusCode,
    pub(super) body: Value,
    cookie: Option<String>,
}

impl Reply {
    pub(super) fn new(status: StatusCode, body: Value) -> Self {
        Self {
            status,
            body,
            cookie: None,
        }
    }

    pub(super) fn ok(body: Value) -> Self {
        Self::new(StatusCode::OK, body)
    }

    pub(super) fn with_cookie(status: StatusCode, body: Value, cookie: String) -> Self {
        Self {
            status,
            body,
            cookie: Some(cookie),
        }
    }
}

pub(super) const INVALID_REQUEST: Refusal = Refusal::new(
    StatusCode::BAD_REQUEST,
    "invalid_request",
    "the request's path or query is not one this API accepts",
);
pub(super) const UNAUTHENTICATED: Refusal = Refusal::new(
    StatusCode::UNAUTHORIZED,
    "unauthenticated",
    "a valid bearer token is required",
);
pub(super) const FORBIDDEN_HOST: Refusal = Refusal::new(
    StatusCode::FORBIDDEN,
    "forbidden_host",
    "the Host header does not name this listener",
);
pub(super) const FORBIDDEN_ORIGIN: Refusal = Refusal::new(
    StatusCode::FORBIDDEN,
    "forbidden_origin",
    "cross-origin requests are refused",
);
pub(super) const NOT_FOUND: Refusal =
    Refusal::new(StatusCode::NOT_FOUND, "not_found", "no such resource");
pub(super) const JOB_NOT_FOUND: Refusal =
    Refusal::new(StatusCode::NOT_FOUND, "job_not_found", "no such Job");
pub(super) const METHOD_NOT_ALLOWED: Refusal = Refusal::new(
    StatusCode::METHOD_NOT_ALLOWED,
    "method_not_allowed",
    "only GET is accepted",
);
pub(super) const ONLY_POST: Refusal = Refusal {
    message: "only POST is accepted",
    allow: "POST",
    ..METHOD_NOT_ALLOWED
};
pub(super) const SESSION_METHODS: Refusal = Refusal {
    message: "only GET, POST or DELETE is accepted",
    allow: "GET, POST, DELETE",
    ..METHOD_NOT_ALLOWED
};
pub(super) const URI_TOO_LONG: Refusal = Refusal::new(
    StatusCode::URI_TOO_LONG,
    "uri_too_long",
    "the request's path or query is too long",
);
pub(super) const HEADERS_TOO_LARGE: Refusal = Refusal::new(
    StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
    "headers_too_large",
    "the request's headers are too large",
);
pub(super) const BODY_NOT_ACCEPTED: Refusal = Refusal::new(
    StatusCode::PAYLOAD_TOO_LARGE,
    "body_not_accepted",
    "a request may not carry a body",
);
pub(super) const BODY_TOO_LARGE: Refusal = Refusal::new(
    StatusCode::PAYLOAD_TOO_LARGE,
    "body_too_large",
    "the body is larger than a submission may be",
);
pub(super) const LENGTH_REQUIRED: Refusal = Refusal::new(
    StatusCode::LENGTH_REQUIRED,
    "length_required",
    "a submission must declare its one Content-Length and no Transfer-Encoding",
);
pub(super) const UNSUPPORTED_MEDIA_TYPE: Refusal = Refusal::new(
    StatusCode::UNSUPPORTED_MEDIA_TYPE,
    "unsupported_media_type",
    "a submission must be application/json",
);
pub(super) const OUTPUT_TOO_LARGE: Refusal = Refusal::new(
    StatusCode::UNPROCESSABLE_ENTITY,
    "output_too_large",
    "the next output is larger than a response may be; it is not truncated",
);
pub(super) const DATABASE_BUSY: Refusal = Refusal::new(
    StatusCode::SERVICE_UNAVAILABLE,
    "database_busy",
    "the database is busy; retry later",
);
pub(super) const DATABASE_UNAVAILABLE: Refusal = Refusal::new(
    StatusCode::SERVICE_UNAVAILABLE,
    "database_unavailable",
    "the database cannot be read now",
);
pub(super) const SHUTTING_DOWN: Refusal = Refusal::new(
    StatusCode::SERVICE_UNAVAILABLE,
    "shutting_down",
    "OCG is shutting down",
);
pub(super) const INCONSISTENT_RECORD: Refusal = Refusal::new(
    StatusCode::INTERNAL_SERVER_ERROR,
    "inconsistent_record",
    "the Job's record cannot be described truthfully",
);
pub(super) const INTERNAL: Refusal = Refusal::new(
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
pub(super) async fn answer(
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
                Ok(Route::AttachmentContent(attachment)) => {
                    attachments::content(&shared.database, attachment)
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

/// A submission's body is declared JSON, with exactly one length, and no
/// larger than `limit`, before any of it is read.
pub(super) fn submission_body(headers: &HeaderMap, limit: usize) -> Result<(), Refusal> {
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
    if length > limit {
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
pub(super) async fn read_body(
    mut payload: web::types::Payload,
    limit: usize,
) -> Result<Vec<u8>, Refusal> {
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
pub(super) fn single<'a>(
    headers: &'a HeaderMap,
    name: &header::HeaderName,
) -> Option<&'a HeaderValue> {
    let mut values = headers.get_all(name);
    let value = values.next()?;
    values.next().is_none().then_some(value)
}
