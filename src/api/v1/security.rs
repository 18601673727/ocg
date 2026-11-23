use super::config::UiAuthMode;
use super::{
    ApiRequest, BODY_NOT_ACCEPTED, FORBIDDEN_HOST, FORBIDDEN_ORIGIN, HEADERS_TOO_LARGE, HeaderMap,
    HttpRequest, MAX_HEADER_BYTES, MAX_HEADERS, MAX_PATH_BYTES, MAX_QUERY_BYTES,
    METHOD_NOT_ALLOWED, Method, Refusal, Token, UNAUTHENTICATED, URI_TOO_LONG, browser, header,
    route, single, submission_body,
};

#[derive(Clone)]
pub(super) enum Caller {
    Programmatic,
    Operator(browser::Session),
    BeginSession,
    Anonymous,
}

pub(super) fn admit(
    request: &HttpRequest,
    port: u16,
    token: &Token,
    browser: &browser::State,
    has_assets: bool,
) -> Result<Option<ApiRequest>, Refusal> {
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
    if !request.path().starts_with("/api/") {
        if request.method() != Method::GET {
            return Err(METHOD_NOT_ALLOWED);
        }
        names_listener(headers, port).ok_or(FORBIDDEN_HOST)?;
        return Ok(None);
    }
    route(request, |session_route, post_limit| {
        let host = names_listener(headers, port).ok_or(FORBIDDEN_HOST)?;
        if !same_origin(headers, &host) {
            return Err(FORBIDDEN_ORIGIN);
        }
        admit_caller(
            request,
            token,
            browser,
            has_assets,
            session_route,
            post_limit,
        )
    })
    .map(Some)
}

fn admit_caller(
    request: &HttpRequest,
    token: &Token,
    browser: &browser::State,
    has_assets: bool,
    session_route: bool,
    post_limit: Option<usize>,
) -> Result<Caller, Refusal> {
    let method = request.method();
    let headers = request.headers();
    let caller = if session_route
        && *method == Method::GET
        && (has_assets || token.ui_auth_mode == UiAuthMode::Token)
    {
        match authenticate(headers, token, browser) {
            Ok(Caller::Programmatic) if token.ui_auth_mode == UiAuthMode::Token => {
                Caller::BeginSession
            }
            Err(_) if !headers.contains_key(header::AUTHORIZATION) => match token.ui_auth_mode {
                UiAuthMode::Auto => Caller::BeginSession,
                UiAuthMode::Token => Caller::Anonymous,
            },
            result => result?,
        }
    } else {
        authenticate(headers, token, browser)?
    };
    if let Caller::Operator(session) = &caller
        && method != Method::GET
        && !browser.csrf_matches(session, headers)
    {
        return Err(browser::FORBIDDEN_CSRF);
    }
    if let Some(limit) = post_limit {
        submission_body(headers, limit)?;
        return Ok(caller);
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
    Ok(caller)
}

fn authenticate(
    headers: &HeaderMap,
    token: &Token,
    browser: &browser::State,
) -> Result<Caller, Refusal> {
    let credentials = headers.get_all(header::AUTHORIZATION).count();
    let presented = browser::presented(headers);
    match (credentials, &presented) {
        (0, browser::Presented::None) => Err(UNAUTHENTICATED),
        (1, browser::Presented::None) if authenticated(headers, token) => Ok(Caller::Programmatic),
        (0, browser::Presented::One(_)) => browser
            .session(&presented)
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

#[cfg(test)]
mod tests {
    use super::super::{
        INVALID_REQUEST, MAX_ATTACHMENT_UPLOAD_BYTES, MAX_SUBMISSION_BYTES, NOT_FOUND, ONLY_POST,
        PROFILE_METHODS, Route, SESSION_METHODS, authorize, config::ServerConfig, update, wire,
    };
    use super::*;
    use ntex::web::test::TestRequest;

    const SECRET: &str = "test-token-0123456789-abcdef-OCG-1234";
    const HOST: &str = "localhost:18080";

    struct Listener {
        token: Token,
        browser: browser::State,
    }

    impl Listener {
        fn new(mode: &str) -> Self {
            let config = ServerConfig::from_source(|name| match name {
                "OCG_API_PORT" => Some(Ok("18080".into())),
                "OCG_API_TOKEN" => Some(Ok(SECRET.into())),
                "OCG_UI_AUTH_MODE" => Some(Ok(mode.into())),
                _ => None,
            })
            .unwrap()
            .unwrap();
            Self {
                token: config.token,
                browser: browser::State::default(),
            }
        }

        fn admit(&self, request: TestRequest) -> Result<Option<ApiRequest>, Refusal> {
            admit(
                &request.to_http_request(),
                18080,
                &self.token,
                &self.browser,
                false,
            )
        }
    }

    fn request(method: Method, path: &str) -> TestRequest {
        TestRequest::with_uri(path)
            .method(method)
            .header(header::HOST, HOST)
    }

    fn bearer(request: TestRequest) -> TestRequest {
        request.header(header::AUTHORIZATION, format!("Bearer {SECRET}"))
    }

    fn json(request: TestRequest, bytes: usize) -> TestRequest {
        request
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::CONTENT_LENGTH, bytes.to_string())
    }

    #[test]
    fn host_and_origin_checks_precede_authentication() {
        let listener = Listener::new("token");
        let path = "/api/v1/health";
        for host in ["example.test:18080", "localhost:80", "127.0.0.1"] {
            assert_eq!(
                listener
                    .admit(TestRequest::with_uri(path).header(header::HOST, host))
                    .err(),
                Some(FORBIDDEN_HOST)
            );
        }
        assert_eq!(
            listener
                .admit(request(Method::GET, path).header(header::HOST, HOST))
                .err(),
            Some(FORBIDDEN_HOST)
        );
        for (name, value) in [
            ("origin", "http://127.0.0.1:18080"),
            ("origin", "https://localhost:18080"),
            ("sec-fetch-site", "same-site"),
        ] {
            assert_eq!(
                listener
                    .admit(request(Method::GET, path).header(name, value))
                    .err(),
                Some(FORBIDDEN_ORIGIN)
            );
        }
        assert_eq!(
            listener
                .admit(
                    request(Method::GET, path)
                        .header(header::ORIGIN, "http://localhost:18080")
                        .header(header::ORIGIN, "http://localhost:18080")
                )
                .err(),
            Some(FORBIDDEN_ORIGIN)
        );
        assert_eq!(
            listener
                .admit(
                    request(Method::GET, path)
                        .header(header::ORIGIN, "http://LOCALHOST:18080")
                        .header("sec-fetch-site", "same-origin")
                )
                .err(),
            Some(UNAUTHENTICATED)
        );
        assert!(matches!(
            listener.admit(request(Method::GET, "/").header("origin", "null")),
            Ok(None)
        ));
    }

    #[test]
    fn method_refusals_and_unknown_paths_preserve_precedence() {
        let listener = Listener::new("token");
        for (method, path, error) in [
            (Method::POST, "/api/v1/session", SESSION_METHODS),
            (Method::PUT, "/api/v1/profiles/import", PROFILE_METHODS),
            (Method::DELETE, "/api/v1/jobs/", ONLY_POST),
            (Method::DELETE, "/api/v1/attachments", ONLY_POST),
            (
                Method::POST,
                "/api/v1/conversations//compact",
                METHOD_NOT_ALLOWED,
            ),
            (Method::POST, "/api/v1/approvals/a//0", METHOD_NOT_ALLOWED),
            (Method::POST, "/api/v1/providers/a", METHOD_NOT_ALLOWED),
            (Method::POST, "/", METHOD_NOT_ALLOWED),
        ] {
            // With no Host or credentials, these requests still receive their method-specific 405.
            assert_eq!(
                listener
                    .admit(TestRequest::with_uri(path).method(method))
                    .err(),
                Some(error),
                "{path}"
            );
        }
        for path in ["/api/v1/missing", "/api/v2/health"] {
            assert_eq!(
                listener.admit(request(Method::GET, path)).err(),
                Some(UNAUTHENTICATED)
            );
            assert_eq!(
                listener
                    .admit(bearer(request(Method::GET, path)).header(header::CONTENT_LENGTH, "1"))
                    .err(),
                Some(BODY_NOT_ACCEPTED)
            );
            assert_eq!(
                listener.admit(bearer(request(Method::GET, path))).err(),
                Some(NOT_FOUND)
            );
        }
        for path in [
            "/api/v1/jobs/current",
            "/api/v1/conversations/search",
            "/api/v1/conversations/archived",
            "/api/v1/jobs//stop",
        ] {
            assert_eq!(
                listener.admit(request(Method::POST, path)).err(),
                Some(UNAUTHENTICATED)
            );
            assert_eq!(
                listener.admit(bearer(request(Method::POST, path))).err(),
                Some(wire::LENGTH_REQUIRED)
            );
            assert_eq!(
                listener
                    .admit(json(bearer(request(Method::POST, path)), 0))
                    .err(),
                Some(INVALID_REQUEST)
            );
        }
    }

    #[test]
    fn route_limit_governs_admission_and_the_dispatched_body() {
        let listener = Listener::new("token");
        for (path, limit) in [
            ("/api/v1/update", update::UPDATE_BODY_LIMIT),
            ("/api/v1/attachments", MAX_ATTACHMENT_UPLOAD_BYTES),
            ("/api/v1/jobs", MAX_SUBMISSION_BYTES),
        ] {
            assert_eq!(
                listener
                    .admit(json(bearer(request(Method::POST, path)), limit + 1))
                    .err(),
                Some(wire::BODY_TOO_LARGE),
                "{path}"
            );
            let api = listener
                .admit(json(bearer(request(Method::POST, path)), limit))
                .unwrap()
                .unwrap();
            assert_eq!(api.body_limit, limit, "{path}");
            assert_eq!(
                listener
                    .admit(
                        json(bearer(request(Method::POST, path)), 1)
                            .header(header::TRANSFER_ENCODING, "chunked")
                    )
                    .err(),
                Some(wire::LENGTH_REQUIRED)
            );
        }
        let path = "/api/v1/jobs/invalid/submit?unexpected=1";
        assert_eq!(
            listener
                .admit(json(
                    bearer(request(Method::POST, path)),
                    MAX_SUBMISSION_BYTES + 1
                ))
                .err(),
            Some(wire::BODY_TOO_LARGE)
        );
        assert_eq!(
            listener
                .admit(bearer(request(
                    Method::GET,
                    "/api/v1/jobs/invalid/outputs/latest?unexpected=1"
                )))
                .err(),
            Some(NOT_FOUND)
        );
    }

    #[test]
    fn token_session_credentials_and_mutations_are_scoped_to_their_caller() {
        let listener = Listener::new("token");
        let session = "/api/v1/session";
        let anonymous = listener
            .admit(request(Method::GET, session))
            .unwrap()
            .unwrap();
        assert!(matches!(anonymous.caller, Caller::Anonymous));
        assert!(matches!(
            authorize(&anonymous.caller, anonymous.route),
            Ok(Route::SessionStatus)
        ));
        assert!(
            matches!(authorize(&anonymous.caller, Route::Health(None)), Err(error) if error == NOT_FOUND)
        );
        assert_eq!(
            listener.admit(request(Method::GET, "/api/v1/health")).err(),
            Some(UNAUTHENTICATED)
        );
        assert_eq!(
            listener
                .admit(
                    request(Method::GET, session).header(header::AUTHORIZATION, "Bearer invalid")
                )
                .err(),
            Some(UNAUTHENTICATED)
        );
        assert!(matches!(
            listener
                .admit(bearer(request(Method::GET, session)))
                .unwrap()
                .unwrap()
                .caller,
            Caller::BeginSession
        ));
        let auto = Listener::new("auto");
        assert_eq!(
            auto.admit(request(Method::GET, session)).err(),
            Some(UNAUTHENTICATED)
        );
        assert!(matches!(
            admit(
                &request(Method::GET, session).to_http_request(),
                18080,
                &auto.token,
                &auto.browser,
                true
            )
            .unwrap()
            .unwrap()
            .caller,
            Caller::BeginSession
        ));

        let reply = listener.browser.begin_session().unwrap();
        let csrf = reply.body["session"]["csrf_token"]
            .as_str()
            .unwrap()
            .to_owned();
        let response = reply.into_response(4096);
        let cookie = response
            .headers()
            .get(header::SET_COOKIE)
            .unwrap()
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap();
        let signed_in = || request(Method::GET, session).header(header::COOKIE, cookie);
        let logout = || request(Method::DELETE, session).header(header::COOKIE, cookie);
        assert!(matches!(
            listener.admit(signed_in()).unwrap().unwrap().caller,
            Caller::Operator(_)
        ));
        assert_eq!(
            listener.admit(bearer(signed_in())).err(),
            Some(UNAUTHENTICATED)
        );
        assert_eq!(
            listener.admit(logout()).err(),
            Some(browser::FORBIDDEN_CSRF)
        );
        assert_eq!(
            listener
                .admit(logout().header(browser::CSRF_HEADER, "mismatched"))
                .err(),
            Some(browser::FORBIDDEN_CSRF)
        );
        let api = listener
            .admit(logout().header(browser::CSRF_HEADER, csrf))
            .unwrap()
            .unwrap();
        assert!(matches!(
            authorize(&api.caller, api.route),
            Ok(Route::SessionRevoke)
        ));
        let api = listener
            .admit(bearer(request(Method::DELETE, session)))
            .unwrap()
            .unwrap();
        assert!(matches!(authorize(&api.caller, api.route), Err(error) if error == NOT_FOUND));
    }
}
