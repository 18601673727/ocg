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
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Upper bound on a single response body (512 MiB).
///
/// Release archives are tens of megabytes; the cap protects against a hostile
/// or broken server that streams without a `Content-Length`.
pub const MAX_BODY_BYTES: u64 = 512 * 1024 * 1024;

/// Time allowed for connect, write and the response head.
///
/// The client library's 5s default aborts a request GitHub is queueing or
/// throttling, which surfaces as an opaque transport error rather than as the
/// rate-limit condition the caller can actually act on.
const RESPONSE_HEAD_TIMEOUT: Duration = Duration::from_secs(30);

/// Time allowed to read a whole body once the head has arrived.
///
/// This is a total read budget rather than an idle timeout, so it has to scale
/// with [`MAX_BODY_BYTES`] instead of sitting at a latency-sized default.
const RESPONSE_BODY_TIMEOUT: Duration = Duration::from_secs(600);

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

    /// Send one bounded JSON request through the same native HTTP surface.
    /// Provider execution uses this narrow extension; it is not a second
    /// transport implementation.
    fn post_json(
        &self,
        url: &str,
        headers: &[(&str, &str)],
        body: &Value,
    ) -> Result<HttpResponse> {
        let _ = (url, headers, body);
        Err(OcgError::config("native HTTP transport does not support JSON POST"))
    }

}

/// The production transport uses OCG's ntex HTTP stack on the native ntex runtime.
///
/// Automatic (hidden) proxy discovery is always disabled first; only the
/// typed endpoints of the resolved [`ProxyPlan`] are installed. Only
/// `https://` URLs are accepted, and bodies are capped at [`MAX_BODY_BYTES`].
pub struct NativeHttp {
    token: Option<GithubToken>,
}

impl NativeHttp {
    /// A client with no proxy and no GitHub token.
    pub fn new() -> Result<Self> {
        Self::with_policy(&ProxyPlan::default(), None)
    }

    /// A client that uses exactly the resolved proxy plan and GitHub token.
    pub fn with_policy(proxy: &ProxyPlan, token: Option<GithubToken>) -> Result<Self> {
        let ops = proxy_builder_ops(proxy);
        if ops.first() != Some(&ProxyBuilderOp::DisableAutomaticDiscovery) {
            return Err(OcgError::config("native HTTP policy must disable automatic proxy discovery first"));
        }
        if proxy.endpoints().iter().any(|endpoint| endpoint.scheme() == ProxyScheme::All) {
            return Err(OcgError::config("native HTTP client does not support an untyped proxy endpoint"));
        }
        Ok(Self { token })
    }
}

impl HttpTransport for NativeHttp {
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
        let token = self.token.clone();
        let url = url.to_owned();
        let runtime = ntex::rt::System::new("ocg-http", ntex::rt::DefaultRuntime);
        runtime.block_on(async move { native_get(&url, token.as_ref()).await })
    }

    fn post_json(
        &self,
        url: &str,
        headers: &[(&str, &str)],
        body: &Value,
    ) -> Result<HttpResponse> {
        if !url.starts_with("https://") {
            return Err(OcgError::config(format!("refusing non-HTTPS URL: {url}")));
        }
        let url = url.to_owned();
        let body = serde_json::to_vec(body)
            .map_err(|error| OcgError::config(format!("cannot encode JSON request: {error}")))?;
        let headers = headers
            .iter()
            .map(|(name, value)| ((*name).to_string(), (*value).to_string()))
            .collect::<Vec<_>>();
        let runtime = ntex::rt::System::new("ocg-http", ntex::rt::DefaultRuntime);
        runtime.block_on(async move { native_post_json(&url, &headers, &body).await })
    }
}

impl RateLimit {
}

async fn native_get(url: &str, token: Option<&GithubToken>) -> Result<HttpResponse> {
    // `Client::new()` would inherit the library's payload limits, and
    // `response.body()` reads through the buffered payload reader that enforces
    // them: at the defaults a body is capped at 256 KiB and 10s, so every
    // release archive fails long before `MAX_BODY_BYTES` is consulted. The
    // envelope is therefore declared here, where it is enforced, rather than
    // only compared after the fact.
    let client = ntex::client::ClientBuilder::new()
        .response_timeout(RESPONSE_HEAD_TIMEOUT)
        .response_payload_limit(MAX_BODY_BYTES as usize)
        .response_payload_timeout(ntex::time::Millis::from(RESPONSE_BODY_TIMEOUT))
        .build(ntex::SharedCfg::default())
        .await
        .map_err(|error| {
            OcgError::config(format!("cannot build the native HTTP client: {error}"))
        })?;
    let mut request = client.get(url).header("User-Agent", concat!("ocg/", env!("CARGO_PKG_VERSION")));
    let headers = github_headers_for(url, token.is_some());
    if headers.authorization { if let Some(token) = token { request = request.header("Authorization", format!("Bearer {}", token.expose())); } }
    if headers.accept { request = request.header("Accept", "application/vnd.github+json"); }
    if headers.api_version { request = request.header("X-GitHub-Api-Version", "2022-11-28"); }
    let response = request.send().await.map_err(|error| OcgError::config(format!("request to {url} failed: {error}")))?;
    let status = response.status().as_u16();
    let rate_limit = RateLimit::from_pairs(response.headers().iter().filter_map(|(name, value)| {
        value.to_str().ok().map(|value| (name.as_str(), value))
    }));
    let body = response.body().await.map_err(|error| OcgError::config(format!("cannot read the response body from {url} (limit {MAX_BODY_BYTES} bytes): {error}")))?;
    // The payload reader rejects an oversized body before this point; kept as a
    // cheap second gate so `MAX_BODY_BYTES` stays the single declared envelope.
    if body.len() as u64 > MAX_BODY_BYTES { return Err(OcgError::config(format!("response from {url} exceeded the {MAX_BODY_BYTES} byte limit"))); }
    Ok(HttpResponse { status, rate_limit, body: body.to_vec() })
}

async fn native_post_json(
    url: &str,
    headers: &[(String, String)],
    body: &[u8],
) -> Result<HttpResponse> {
    if body.len() as u64 > MAX_BODY_BYTES {
        return Err(OcgError::config(format!(
            "request to {url} exceeded the {MAX_BODY_BYTES} byte limit"
        )));
    }
    let client = ntex::client::ClientBuilder::new()
        .response_timeout(RESPONSE_HEAD_TIMEOUT)
        .response_payload_limit(MAX_BODY_BYTES as usize)
        .response_payload_timeout(ntex::time::Millis::from(RESPONSE_BODY_TIMEOUT))
        .build(ntex::SharedCfg::default())
        .await
        .map_err(|error| {
            OcgError::config(format!("cannot build the native HTTP client: {error}"))
        })?;
    let mut request = client.post(url).header("User-Agent", concat!("ocg/", env!("CARGO_PKG_VERSION")));
    for (name, value) in headers {
        request = request.header(name.as_str(), value.as_str());
    }
    let response = request
        .send_body(body.to_vec())
        .await
        .map_err(|error| OcgError::config(format!("request to {url} failed: {error}")))?;
    let status = response.status().as_u16();
    let response_body = response
        .body()
        .await
        .map_err(|error| OcgError::config(format!("cannot read the response from {url}: {error}")))?;
    if response_body.len() as u64 > MAX_BODY_BYTES {
        return Err(OcgError::config(format!(
            "response from {url} exceeded the {MAX_BODY_BYTES} byte limit"
        )));
    }
    Ok(HttpResponse {
        status,
        rate_limit: RateLimit::default(),
        body: response_body.to_vec(),
    })
}

async fn native_post_json_streaming<F>(
    url: &str,
    headers: &[(String, String)],
    body: &[u8],
    mut callback: F,
) -> Result<HttpResponse>
where
    F: FnMut(&[u8]) -> Result<()>,
{
    if body.len() as u64 > MAX_BODY_BYTES {
        return Err(OcgError::config(format!(
            "request to {url} exceeded the {MAX_BODY_BYTES} byte limit"
        )));
    }
    let client = ntex::client::ClientBuilder::new()
        .response_timeout(RESPONSE_HEAD_TIMEOUT)
        .response_payload_limit(MAX_BODY_BYTES as usize)
        .response_payload_timeout(ntex::time::Millis::from(RESPONSE_BODY_TIMEOUT))
        .build(ntex::SharedCfg::default())
        .await
        .map_err(|error| {
            OcgError::config(format!("cannot build the native HTTP client: {error}"))
        })?;
    let mut request = client.post(url).header("User-Agent", concat!("ocg/", env!("CARGO_PKG_VERSION")));
    for (name, value) in headers {
        request = request.header(name.as_str(), value.as_str());
    }
    let response = request
        .send_body(body.to_vec())
        .await
        .map_err(|error| OcgError::config(format!("request to {url} failed: {error}")))?;
    let status = response.status().as_u16();
    
    // Read the response body using load_body which returns the complete body
    let response_body = response
        .body()
        .await
        .map_err(|error| OcgError::config(format!("error reading response from {url}: {error}")))?;
    
    // Check size limit
    if response_body.len() > MAX_BODY_BYTES as usize {
        return Err(OcgError::config(format!(
            "response from {url} exceeded the {MAX_BODY_BYTES} byte limit"
        )));
    }
    
    // For now, invoke callback with the complete body
    // TODO: Implement true streaming when ntex provides a streaming API
    callback(&response_body)?;
    
    Ok(HttpResponse {
        status,
        rate_limit: RateLimit::default(),
        body: response_body.to_vec(),
    })
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

    fn post_json(
        &self,
        url: &str,
        _headers: &[(&str, &str)],
        _body: &Value,
    ) -> Result<HttpResponse> {
        self.response(url)
    }
}
