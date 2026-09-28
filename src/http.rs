//! HTTP/download abstraction.
//!
//! The runtime only ever needs two things over the network: a JSON release
//! document and a release archive. Both go through [`HttpTransport`] so tests
//! can run the whole runtime pipeline without touching the network.
//!
//! Responses keep their status and a small, safe projection of the GitHub
//! rate-limit headers so a refused request produces a clear message instead of
//! a bare HTTP code. A GitHub token, when available, is attached only to
//! requests whose host is exactly `api.github.com`; it never enters a log,
//! a warning or a serialized artifact.

use crate::error::{OcgError, Result};
use crate::proxy::{proxy_builder_ops, ProxyBuilderOp, ProxyPlan, ProxyScheme};
use std::collections::HashMap;
use std::fmt;
use std::io::Read;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Upper bound on a single response body (512 MiB).
///
/// Release archives are tens of megabytes; the cap protects against a hostile
/// or broken server that streams without a `Content-Length`.
pub const MAX_BODY_BYTES: u64 = 512 * 1024 * 1024;

/// A value that must never be rendered. `Debug` and `Display` redact it.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    pub fn new(raw: impl Into<String>) -> Self {
        Self(raw.into())
    }

    /// The raw value. Never log, print or persist the result.
    pub fn expose(&self) -> &str {
        &self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<redacted>")
    }
}

impl fmt::Display for Secret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<redacted>")
    }
}

/// A GitHub token plus the variable it came from. The source name is safe to
/// report; the token value is not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GithubToken {
    secret: Secret,
    source: &'static str,
}

impl GithubToken {
    /// The raw token for an `Authorization` header. Never render it.
    pub fn expose(&self) -> &str {
        self.secret.expose()
    }

    pub fn source(&self) -> &'static str {
        self.source
    }
}

impl fmt::Display for GithubToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<redacted>")
    }
}

/// The only environment access the HTTP layer needs. Injectable for tests.
pub trait HttpEnv: Send + Sync {
    fn var(&self, name: &str) -> Option<String>;
}

/// The real process environment.
#[derive(Debug, Default, Clone, Copy)]
pub struct ProcessHttpEnv;

impl HttpEnv for ProcessHttpEnv {
    fn var(&self, name: &str) -> Option<String> {
        std::env::var(name).ok().filter(|value| !value.is_empty())
    }
}

/// Resolve the GitHub token with `GH_TOKEN` before `GITHUB_TOKEN`.
pub fn github_token_from_env(env: &dyn HttpEnv) -> Option<GithubToken> {
    for source in ["GH_TOKEN", "GITHUB_TOKEN"] {
        if let Some(raw) = env.var(source) {
            if !raw.trim().is_empty() {
                return Some(GithubToken {
                    secret: Secret::new(raw.trim().to_string()),
                    source,
                });
            }
        }
    }
    None
}

/// True only for the exact public GitHub API host over TLS on its default port.
///
/// The scheme is checked so a token can never be attached to a cleartext URL,
/// even if a caller bypasses the transport's HTTPS guard.
pub fn is_github_api_url(url: &str) -> bool {
    let Some((scheme, rest)) = url.split_once("://") else {
        return false;
    };
    if !scheme.eq_ignore_ascii_case("https") {
        return false;
    }
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    let authority = authority.rsplit('@').next().unwrap_or("");
    if authority.starts_with('[') {
        return false;
    }
    let (host, port) = match authority.rsplit_once(':') {
        Some((host, port)) => (host, Some(port)),
        None => (authority, None),
    };
    host.eq_ignore_ascii_case("api.github.com") && matches!(port, None | Some("443"))
}

/// Which GitHub API headers a request should carry. Presence only; the token
/// value never appears here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct GithubHeaders {
    pub accept: bool,
    pub api_version: bool,
    pub authorization: bool,
}

/// Decide the GitHub headers for a URL. GitHub API headers are attached only
/// for the exact `api.github.com` host, and authorization only when a token is
/// available.
pub fn github_headers_for(url: &str, has_token: bool) -> GithubHeaders {
    if !is_github_api_url(url) {
        return GithubHeaders::default();
    }
    GithubHeaders {
        accept: true,
        api_version: true,
        authorization: has_token,
    }
}

/// The safe GitHub rate-limit projection of a response.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RateLimit {
    pub limit: Option<u64>,
    pub remaining: Option<u64>,
    pub used: Option<u64>,
    pub reset: Option<u64>,
    pub retry_after: Option<u64>,
    pub resource: Option<String>,
}

impl RateLimit {
    pub fn from_pairs<'a, I>(pairs: I) -> Self
    where
        I: IntoIterator<Item = (&'a str, &'a str)>,
    {
        let mut rate = RateLimit::default();
        for (name, value) in pairs {
            match name.to_ascii_lowercase().as_str() {
                "x-ratelimit-limit" => rate.limit = parse_u64(value),
                "x-ratelimit-remaining" => rate.remaining = parse_u64(value),
                "x-ratelimit-used" => rate.used = parse_u64(value),
                "x-ratelimit-reset" => rate.reset = parse_u64(value),
                "x-ratelimit-resource" => rate.resource = Some(value.to_string()),
                "retry-after" => rate.retry_after = parse_u64(value),
                _ => {}
            }
        }
        rate
    }

    /// Whether the headers carry rate-limit evidence.
    pub fn has_evidence(&self) -> bool {
        self.limit.is_some()
            || self.remaining.is_some()
            || self.reset.is_some()
            || self.retry_after.is_some()
    }

    /// A safe, token-free description for diagnostics.
    pub fn describe(&self) -> String {
        let mut parts = Vec::new();
        if let Some(resource) = &self.resource {
            parts.push(format!("resource={resource}"));
        }
        if let Some(limit) = self.limit {
            parts.push(format!("limit={limit}"));
        }
        if let Some(remaining) = self.remaining {
            parts.push(format!("remaining={remaining}"));
        }
        if let Some(used) = self.used {
            parts.push(format!("used={used}"));
        }
        if let Some(reset) = self.reset {
            parts.push(format!("reset={reset}"));
        }
        if let Some(retry) = self.retry_after {
            parts.push(format!("retry-after={retry}s"));
        }
        if parts.is_empty() {
            "no rate-limit headers".to_string()
        } else {
            parts.join(", ")
        }
    }
}

fn parse_u64(value: &str) -> Option<u64> {
    value.trim().parse::<u64>().ok()
}

/// A structured HTTP response. `status` and [`RateLimit`] are preserved so a
/// caller can diagnose a refusal without losing the body.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HttpResponse {
    pub status: u16,
    pub rate_limit: RateLimit,
    pub body: Vec<u8>,
}

impl HttpResponse {
    pub fn ok(body: impl Into<Vec<u8>>) -> Self {
        Self {
            status: 200,
            rate_limit: RateLimit::default(),
            body: body.into(),
        }
    }

    pub fn with_status(status: u16, body: impl Into<Vec<u8>>) -> Self {
        Self {
            status,
            rate_limit: RateLimit::default(),
            body: body.into(),
        }
    }

    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }

    pub fn body_text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

/// Turn a non-success response into a diagnostic. A generic 403 stays generic;
/// only GitHub rate-limit evidence produces a rate-limit message.
pub fn http_failure(url: &str, response: &HttpResponse) -> OcgError {
    if response.status == 403 && response.rate_limit.remaining == Some(0) {
        return OcgError::config(format!(
            "request to {url} was refused because the GitHub API rate limit is exhausted ({})",
            response.rate_limit.describe()
        ));
    }
    if response.status == 429 && response.rate_limit.has_evidence() {
        return OcgError::config(format!(
            "request to {url} was throttled by the GitHub API ({})",
            response.rate_limit.describe()
        ));
    }
    OcgError::config(format!(
        "request to {url} failed with HTTP {}",
        response.status
    ))
}

/// A minimal blocking HTTP client.
pub trait HttpTransport: Send + Sync {
    /// Fetch a URL and return the raw body, erroring on non-success.
    fn get(&self, url: &str) -> Result<Vec<u8>>;

    /// Fetch a URL and decode the body as UTF-8.
    fn get_text(&self, url: &str) -> Result<String> {
        let bytes = self.get(url)?;
        String::from_utf8(bytes).map_err(|error| {
            OcgError::config(format!("response from {url} is not valid UTF-8: {error}"))
        })
    }

    /// Fetch a URL and return the structured response, including status and
    /// safe rate-limit headers, without converting a non-success into an
    /// error. The default derives from [`HttpTransport::get`].
    fn get_response(&self, url: &str) -> Result<HttpResponse> {
        Ok(HttpResponse::ok(self.get(url)?))
    }
}

/// The production transport: one `reqwest` blocking client with rustls.
///
/// Automatic (hidden) proxy discovery is always disabled first; only the
/// typed endpoints of the resolved [`ProxyPlan`] are installed. Only
/// `https://` URLs are accepted, and bodies are capped at [`MAX_BODY_BYTES`].
pub struct ReqwestHttp {
    client: reqwest::blocking::Client,
    token: Option<GithubToken>,
}

impl ReqwestHttp {
    /// A client with no proxy and no GitHub token.
    pub fn new() -> Result<Self> {
        Self::with_policy(&ProxyPlan::default(), None)
    }

    /// A client that uses exactly the resolved proxy plan and GitHub token.
    pub fn with_policy(proxy: &ProxyPlan, token: Option<GithubToken>) -> Result<Self> {
        // Always disable hidden automatic proxy discovery first, then install
        // only the typed resolved endpoints. `proxy_builder_ops` is the
        // inspectable contract for that ordering.
        let ops = proxy_builder_ops(proxy);
        debug_assert_eq!(
            ops.first(),
            Some(&ProxyBuilderOp::DisableAutomaticDiscovery)
        );

        let mut builder = reqwest::blocking::Client::builder()
            .user_agent(concat!("ocg/", env!("CARGO_PKG_VERSION")))
            .timeout(Duration::from_secs(120))
            .no_proxy();

        let exceptions = if proxy.no_proxy().is_empty() {
            None
        } else {
            reqwest::NoProxy::from_string(&proxy.no_proxy().join(","))
        };
        for endpoint in proxy.endpoints() {
            let configured = match endpoint.scheme() {
                ProxyScheme::Http => reqwest::Proxy::http(endpoint.expose()),
                ProxyScheme::Https => reqwest::Proxy::https(endpoint.expose()),
                ProxyScheme::All => reqwest::Proxy::all(endpoint.expose()),
            }
            .map_err(|_| OcgError::config("cannot configure an HTTP proxy endpoint"))?;
            builder = builder.proxy(configured.no_proxy(exceptions.clone()));
        }

        let client = builder
            .build()
            .map_err(|error| OcgError::config(format!("cannot build the HTTP client: {error}")))?;
        Ok(Self { client, token })
    }
}

impl HttpTransport for ReqwestHttp {
    fn get(&self, url: &str) -> Result<Vec<u8>> {
        let response = self.get_response(url)?;
        if !response.is_success() {
            return Err(http_failure(url, &response));
        }
        Ok(response.body)
    }

    fn get_response(&self, url: &str) -> Result<HttpResponse> {
        if !url.starts_with("https://") {
            return Err(OcgError::config(format!("refusing non-HTTPS URL: {url}")));
        }
        let mut request = self.client.get(url);
        let headers = github_headers_for(url, self.token.is_some());
        if headers.authorization {
            if let Some(token) = &self.token {
                request = request.bearer_auth(token.expose());
            }
        }
        if headers.accept {
            request = request.header(reqwest::header::ACCEPT, "application/vnd.github+json");
        }
        if headers.api_version {
            request = request.header("X-GitHub-Api-Version", "2022-11-28");
        }

        let response = request
            .send()
            .map_err(|error| OcgError::config(format!("request to {url} failed: {error}")))?;
        let status = response.status().as_u16();
        let rate_limit = RateLimit::from_headers(response.headers());
        if let Some(length) = response.content_length() {
            if length > MAX_BODY_BYTES {
                return Err(OcgError::config(format!(
                    "response from {url} is {length} bytes, over the {MAX_BODY_BYTES} byte limit"
                )));
            }
        }
        let mut bytes = Vec::new();
        // `take` also enforces the cap when no Content-Length was sent.
        let mut reader = response.take(MAX_BODY_BYTES + 1);
        reader.read_to_end(&mut bytes).map_err(|error| {
            OcgError::config(format!("cannot read the response from {url}: {error}"))
        })?;
        if bytes.len() as u64 > MAX_BODY_BYTES {
            return Err(OcgError::config(format!(
                "response from {url} exceeded the {MAX_BODY_BYTES} byte limit"
            )));
        }
        Ok(HttpResponse {
            status,
            rate_limit,
            body: bytes,
        })
    }
}

impl RateLimit {
    fn from_headers(headers: &reqwest::header::HeaderMap) -> Self {
        RateLimit::from_pairs(
            headers.iter().filter_map(|(name, value)| {
                value.to_str().ok().map(|value| (name.as_str(), value))
            }),
        )
    }
}

/// A transport that always fails. Used by non-mutating commands (`version`,
/// `doctor`) that must never touch the network.
#[doc(hidden)]
#[derive(Debug, Default, Clone, Copy)]
pub struct NoHttp;

impl HttpTransport for NoHttp {
    fn get(&self, url: &str) -> Result<Vec<u8>> {
        Err(OcgError::config(format!(
            "network access is disabled for this command ({url})"
        )))
    }
}

/// An in-memory transport for tests and offline tooling. It records every
/// requested URL so tests can prove that a cached check makes no request.
#[doc(hidden)]
#[derive(Debug, Default, Clone)]
pub struct MemoryHttp {
    responses: HashMap<String, HttpResponse>,
    requests: Arc<Mutex<Vec<String>>>,
}

impl MemoryHttp {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with(mut self, url: &str, body: impl Into<Vec<u8>>) -> Self {
        self.insert(url, body);
        self
    }

    pub fn insert(&mut self, url: &str, body: impl Into<Vec<u8>>) {
        self.responses
            .insert(url.to_string(), HttpResponse::ok(body));
    }

    /// Register a structured response with a status and safe headers.
    pub fn with_status(
        mut self,
        url: &str,
        status: u16,
        headers: &[(&str, &str)],
        body: impl Into<Vec<u8>>,
    ) -> Self {
        self.responses.insert(
            url.to_string(),
            HttpResponse {
                status,
                rate_limit: RateLimit::from_pairs(headers.iter().copied()),
                body: body.into(),
            },
        );
        self
    }

    fn response(&self, url: &str) -> Result<HttpResponse> {
        self.requests
            .lock()
            .expect("fake requests")
            .push(url.to_string());
        self.responses
            .get(url)
            .cloned()
            .ok_or_else(|| OcgError::config(format!("no fixture registered for {url}")))
    }

    /// URLs requested so far, in order.
    pub fn requests(&self) -> Vec<String> {
        self.requests.lock().expect("fake requests").clone()
    }
}

impl HttpTransport for MemoryHttp {
    fn get(&self, url: &str) -> Result<Vec<u8>> {
        let response = self.response(url)?;
        if !response.is_success() {
            return Err(http_failure(url, &response));
        }
        Ok(response.body)
    }

    fn get_response(&self, url: &str) -> Result<HttpResponse> {
        self.response(url)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct MapHttpEnv(Vec<(&'static str, &'static str)>);

    impl HttpEnv for MapHttpEnv {
        fn var(&self, name: &str) -> Option<String> {
            self.0
                .iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| value.to_string())
        }
    }

    #[test]
    fn memory_transport_serves_registered_bodies() {
        let http = MemoryHttp::new().with("https://example.test/a", b"hello".to_vec());
        assert_eq!(http.get("https://example.test/a").unwrap(), b"hello");
        assert_eq!(http.get_text("https://example.test/a").unwrap(), "hello");
        assert!(http.get("https://example.test/missing").is_err());
        assert_eq!(
            http.requests(),
            vec![
                "https://example.test/a".to_string(),
                "https://example.test/a".to_string(),
                "https://example.test/missing".to_string(),
            ]
        );
    }

    #[test]
    fn production_transport_rejects_non_https() {
        let http = ReqwestHttp::new().unwrap();
        assert!(http.get("http://example.test/a").is_err());
        assert!(http.get("file:///etc/hosts").is_err());
    }

    #[test]
    fn structured_response_keeps_status_and_rate_limit() {
        let http = MemoryHttp::new().with_status(
            "https://api.github.com/repos/a/b/releases/latest",
            403,
            &[
                ("x-ratelimit-limit", "60"),
                ("x-ratelimit-remaining", "0"),
                ("x-ratelimit-used", "60"),
                ("x-ratelimit-reset", "1700000000"),
                ("x-ratelimit-resource", "core"),
            ],
            b"{}".to_vec(),
        );
        let response = http
            .get_response("https://api.github.com/repos/a/b/releases/latest")
            .unwrap();
        assert_eq!(response.status, 403);
        assert_eq!(response.rate_limit.limit, Some(60));
        assert_eq!(response.rate_limit.remaining, Some(0));
        assert_eq!(response.rate_limit.resource.as_deref(), Some("core"));
    }

    #[test]
    fn generic_403_stays_generic() {
        let url = "https://api.github.com/repos/a/b";
        let response = HttpResponse::with_status(403, Vec::new());
        let message = http_failure(url, &response).to_string();
        assert!(message.contains("HTTP 403"));
        assert!(!message.contains("rate limit"));
    }

    #[test]
    fn exhausted_403_reports_the_rate_limit() {
        let url = "https://api.github.com/repos/a/b";
        let mut response = HttpResponse::with_status(403, Vec::new());
        response.rate_limit = RateLimit {
            limit: Some(60),
            remaining: Some(0),
            used: Some(60),
            reset: Some(1_700_000_000),
            retry_after: Some(30),
            resource: Some("core".to_string()),
        };
        let message = http_failure(url, &response).to_string();
        assert!(message.contains("rate limit"));
        assert!(message.contains("remaining=0"));
        assert!(message.contains("retry-after=30s"));
    }

    #[test]
    fn github_429_with_evidence_reports_retry_after() {
        let url = "https://api.github.com/repos/a/b";
        let response = HttpResponse {
            status: 429,
            rate_limit: RateLimit {
                retry_after: Some(12),
                ..RateLimit::default()
            },
            body: Vec::new(),
        };
        let message = http_failure(url, &response).to_string();
        assert!(message.contains("throttled"));
        assert!(message.contains("retry-after=12s"));
    }

    #[test]
    fn generic_429_without_evidence_stays_generic() {
        let url = "https://example.test/a";
        let response = HttpResponse::with_status(429, Vec::new());
        let message = http_failure(url, &response).to_string();
        assert!(message.contains("HTTP 429"));
        assert!(!message.contains("throttled"));
    }

    #[test]
    fn token_precedence_prefers_gh_token() {
        let env = MapHttpEnv(vec![("GH_TOKEN", "primary"), ("GITHUB_TOKEN", "secondary")]);
        let token = github_token_from_env(&env).unwrap();
        assert_eq!(token.expose(), "primary");
        assert_eq!(token.source(), "GH_TOKEN");

        let fallback = MapHttpEnv(vec![("GITHUB_TOKEN", "secondary")]);
        let token = github_token_from_env(&fallback).unwrap();
        assert_eq!(token.expose(), "secondary");
        assert_eq!(token.source(), "GITHUB_TOKEN");

        assert!(github_token_from_env(&MapHttpEnv::default()).is_none());
    }

    #[test]
    fn token_rendering_is_redacted() {
        let token =
            github_token_from_env(&MapHttpEnv(vec![("GH_TOKEN", "super-secret-value")])).unwrap();
        assert!(!format!("{token:?}").contains("super-secret-value"));
        assert!(!format!("{token}").contains("super-secret-value"));
        assert!(!format!("{:?}", token.secret).contains("super-secret-value"));
    }

    #[test]
    fn github_headers_are_api_only() {
        assert!(github_headers_for("https://api.github.com/repos/a/b", true).authorization);
        assert!(github_headers_for("https://api.github.com/repos/a/b", false).accept);
        assert!(!github_headers_for("https://api.github.com/repos/a/b", false).authorization);

        for foreign in [
            "https://github.com/a/b",
            "https://api.github.com.evil.test/a",
            "https://evil.test/api.github.com/a",
            "https://objects.githubusercontent.com/a",
            "https://api.github.com:8443/a",
            "https://api.github.com.:443/a",
            "https://[::1]/a",
        ] {
            assert_eq!(
                github_headers_for(foreign, true),
                GithubHeaders::default(),
                "{foreign} must not receive GitHub API headers"
            );
            assert!(!is_github_api_url(foreign), "{foreign}");
        }
        // Accepted forms: upper-case host and an explicit :443.
        for accepted in [
            "https://api.github.com/repos/a/b",
            "https://API.GITHUB.COM/repos/a/b",
            "https://api.github.com:443/repos/a/b",
        ] {
            assert!(is_github_api_url(accepted), "{accepted}");
        }
        // Userinfo is stripped before the host comparison.
        let with_userinfo = format!("https://{}@api.github.com/repos/a/b", "user");
        assert!(is_github_api_url(&with_userinfo), "{with_userinfo}");
        // Fail-closed forms must never be treated as the GitHub API.
        for rejected in [
            "https://api.github.com:0443/a",
            "https://api.github.com.evil.test/a",
            "http://api.github.com/a",
        ] {
            assert!(!is_github_api_url(rejected), "{rejected}");
        }
    }

    #[test]
    fn rate_limit_projects_from_a_reqwest_header_map() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("x-ratelimit-limit", "60".parse().unwrap());
        headers.insert("x-ratelimit-remaining", "0".parse().unwrap());
        headers.insert("x-ratelimit-used", "60".parse().unwrap());
        headers.insert("x-ratelimit-reset", "1700000000".parse().unwrap());
        headers.insert("x-ratelimit-resource", "core".parse().unwrap());
        headers.insert("retry-after", "30".parse().unwrap());
        let rate = RateLimit::from_headers(&headers);
        assert_eq!(rate.limit, Some(60));
        assert_eq!(rate.remaining, Some(0));
        assert_eq!(rate.used, Some(60));
        assert_eq!(rate.reset, Some(1_700_000_000));
        assert_eq!(rate.retry_after, Some(30));
        assert_eq!(rate.resource.as_deref(), Some("core"));
        assert!(rate.has_evidence());
        let described = rate.describe();
        assert!(described.contains("remaining=0"), "{described}");
        assert!(described.contains("retry-after=30s"), "{described}");
    }

    #[test]
    fn response_headers_never_leak_into_the_error() {
        let url = "https://api.github.com/repos/a/b";
        let response = HttpResponse {
            status: 403,
            rate_limit: RateLimit {
                remaining: Some(0),
                ..RateLimit::default()
            },
            body: b"authorization: secret-value-here".to_vec(),
        };
        let message = http_failure(url, &response).to_string();
        assert!(!message.contains("secret-value-here"));
    }

    #[test]
    fn reqwest_client_builds_with_resolved_proxies_without_network() {
        let env = crate::proxy::MapProxyEnv::new()
            .with("HTTPS_PROXY", "http://proxy-internal:3128")
            .with("NO_PROXY", "localhost,169.254/16");
        let selection = crate::proxy::resolve(false, &env, &crate::proxy::NoStaticProxy);
        let http = ReqwestHttp::with_policy(selection.plan(), None).unwrap();
        // The client is built entirely offline; only the HTTPS guard fires.
        assert!(http.get("http://example.test/a").is_err());
    }

    #[test]
    fn disabled_proxy_client_installs_no_endpoint_and_stays_direct() {
        let env =
            crate::proxy::MapProxyEnv::new().with("HTTPS_PROXY", "http://proxy-internal:3128");
        let selection = crate::proxy::resolve(true, &env, &crate::proxy::NoStaticProxy);
        assert!(selection.is_disabled());
        assert!(selection.plan().endpoints().is_empty());
        assert_eq!(
            crate::proxy::proxy_builder_ops(selection.plan()),
            vec![crate::proxy::ProxyBuilderOp::DisableAutomaticDiscovery],
            "a disabled policy must only disable hidden automatic discovery"
        );
        // The client builds offline and never installs the ambient proxy.
        let http = ReqwestHttp::with_policy(selection.plan(), None).unwrap();
        assert!(http.get("http://example.test/a").is_err());
    }
}
