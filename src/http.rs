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
use ntex::util::Stream;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;
use std::future::poll_fn;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Upper bound on a single response body (512 MiB).
///
/// Release archives are tens of megabytes; the cap protects against a hostile
/// or broken server that streams without a `Content-Length`.
pub const MAX_BODY_BYTES: u64 = 512 * 1024 * 1024;

/// Upper bound on a non-2xx provider diagnostic body (4 KiB).
///
/// A refused provider request yields only this much of its body for the error
/// message; it is small, and the caller redacts it before reporting.
pub const MAX_PROVIDER_ERROR_BODY: usize = 4096;

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

    /// Send one JSON request and deliver the response body incrementally.
    ///
    /// `on_chunk` runs once per received chunk, in order, before the next
    /// chunk is read: there is no intermediate queue, so consumption is
    /// backpressured by the caller. Returning `Ok(false)` stops the read early
    /// (used for cancellation); returning `Err` aborts the request. The
    /// default implementation buffers once via [`HttpTransport::post_json`]
    /// and delivers the whole body in a single call, which is correct for
    /// non-provider transports that do not stream.
    ///
    /// On success the returned [`HttpResponse`] carries the status and safe
    /// headers with an empty `body`; the chunks already reached `on_chunk`. A
    /// non-2xx response is never delivered to `on_chunk`; its bounded body is
    /// returned on the [`HttpResponse`] so the caller can render a redacted
    /// diagnostic.
    fn post_json_stream(
        &self,
        url: &str,
        headers: &[(&str, &str)],
        body: &Value,
        mut on_chunk: Box<dyn FnMut(&[u8]) -> Result<bool> + Send>,
    ) -> Result<HttpResponse> {
        let response = self.post_json(url, headers, body)?;
        on_chunk(&response.body)?;
        Ok(response)
    }
}

/// The production transport uses OCG's ntex HTTP stack on the native ntex runtime.
///
/// Automatic (hidden) proxy discovery is always disabled first; only the
/// typed endpoints of the resolved [`ProxyPlan`] are installed. Only
/// `https://` URLs are accepted, and bodies are capped at [`MAX_BODY_BYTES`].
pub struct NativeHttp {
    token: Option<GithubToken>,
    /// The one proxy plan resolved at startup. The client never re-reads the
    /// ambient proxy environment; this plan decides every route.
    proxy: ProxyPlan,
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
        Ok(Self { token, proxy: proxy.clone() })
    }

    /// Decide the outbound route for one URL from the stored [`ProxyPlan`].
    ///
    /// A host covered by the resolved exception list connects directly. A host
    /// the plan routes through a proxy tunnels via an async HTTP CONNECT
    /// carried on ntex I/O (`TCP -> proxy -> CONNECT target -> 200 ->
    /// TLS(target SNI) -> ntex HTTP`); the upper layer keeps the original
    /// target URL. Every other host is direct. The ambient proxy environment
    /// is never consulted again.
    fn proxy_endpoint_for(&self, url: &str) -> Result<Option<String>> {
        let host = url_host(url)?;
        if self.proxy.matches_no_proxy(&host) {
            return Ok(None);
        }
        let scheme = url.split_once("://").map(|(scheme, _)| scheme).unwrap_or("");
        if let Some(endpoint) = self.proxy.endpoint_for(scheme) {
            return Ok(Some(endpoint.expose().to_string()));
        }
        Ok(None)
    }
}

/// One parsed `http://` proxy endpoint. The raw URL and credential never
/// leave this struct except as a `Proxy-Authorization` header value.
struct ProxyDial {
    host: String,
    port: u16,
    auth: Option<Secret>,
}

fn parse_proxy_dial(raw: &str) -> Result<ProxyDial> {
    let raw = raw.trim();
    let (scheme, rest) = raw
        .split_once("://")
        .ok_or_else(|| OcgError::config("proxy endpoint has no scheme"))?;
    if !scheme.eq_ignore_ascii_case("http") {
        return Err(OcgError::config(
            "proxy endpoint uses an unsupported scheme; only http:// proxies tunnel CONNECT",
        ));
    }
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    if authority.is_empty() {
        return Err(OcgError::config("proxy endpoint has no authority"));
    }
    let (userinfo, hostport) = match authority.rsplit_once('@') {
        Some((user, host)) => (Some(user), host),
        None => (None, authority),
    };
    if hostport.is_empty() {
        return Err(OcgError::config("proxy endpoint has no host"));
    }
    let (host, port) = if let Some(inner) = hostport.strip_prefix('[') {
        let end = inner.find(']').ok_or_else(|| OcgError::config("proxy endpoint has a malformed IPv6 host"))?;
        let host = inner[..end].to_string();
        let port = inner[end + 1..].strip_prefix(':').and_then(|p| p.parse::<u16>().ok()).unwrap_or(80);
        (host, port)
    } else if hostport.matches(':').count() == 1 {
        let (host, port) = hostport.split_once(':').unwrap_or((hostport, ""));
        let port = port.parse::<u16>().map_err(|_| OcgError::config("proxy endpoint has an invalid port"))?;
        (host.to_string(), port)
    } else {
        (hostport.to_string(), 80)
    };
    if host.is_empty() {
        return Err(OcgError::config("proxy endpoint has no host"));
    }
    let auth = match userinfo {
        Some(credentials) if !credentials.is_empty() => {
            use base64::Engine;
            let encoded = base64::engine::general_purpose::STANDARD.encode(credentials);
            Some(Secret::new(format!("Basic {encoded}")))
        }
        _ => None,
    };
    Ok(ProxyDial { host, port, auth })
}

fn proxy_tls_config() -> Result<std::sync::Arc<rustls::ClientConfig>> {
    let store = rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let mut config = rustls::ClientConfig::builder()
        .with_root_certificates(store)
        .with_no_client_auth();
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(std::sync::Arc::new(config))
}

/// Secure connector that tunnels TLS to the original target through an
/// `http://` proxy: plain TCP to the proxy, `CONNECT target:port`,
/// expect `200`, then TLS with the target host as SNI. The ntex HTTP
/// client above keeps the original target URL; only this connector dials
/// the proxy.
#[derive(Debug, Clone)]
struct ProxySecureConnector {
    proxy_host: String,
    proxy_port: u16,
    proxy_auth: Option<Secret>,
    tls_config: std::sync::Arc<rustls::ClientConfig>,
}

#[derive(Debug, Clone)]
struct ProxySecureService {
    proxy_host: String,
    proxy_port: u16,
    proxy_auth: Option<Secret>,
    tls_config: std::sync::Arc<rustls::ClientConfig>,
}

impl ntex::service::ServiceFactory<ntex::connect::Connect<ntex::http::Uri>, ntex::SharedCfg>
    for ProxySecureConnector
{
    type Response = ntex::io::IoBoxed;
    type Error = ntex::connect::ConnectError;
    type Service = ProxySecureService;
    type InitError = std::convert::Infallible;

    async fn create(&self, _cfg: ntex::SharedCfg) -> std::result::Result<Self::Service, Self::InitError> {
        Ok(ProxySecureService {
            proxy_host: self.proxy_host.clone(),
            proxy_port: self.proxy_port,
            proxy_auth: self.proxy_auth.clone(),
            tls_config: self.tls_config.clone(),
        })
    }
}

impl ntex::service::Service<ntex::connect::Connect<ntex::http::Uri>> for ProxySecureService {
    type Response = ntex::io::IoBoxed;
    type Error = ntex::connect::ConnectError;

    async fn call(
        &self,
        req: ntex::connect::Connect<ntex::http::Uri>,
        _ctx: ntex::service::ServiceCtx<'_, Self>,
    ) -> std::result::Result<Self::Response, Self::Error> {
        let target_host = req.host().to_string();
        let target_port = req.port();
        if target_host.is_empty() || target_port == 0 {
            return Err(ntex::connect::ConnectError::InvalidInput);
        }
        let io_err = |message: String| {
            ntex::connect::ConnectError::Io(std::io::Error::other(message))
        };
        // Plain TCP to the proxy; the target URL is untouched above.
        let proxy_msg = ntex::connect::Connect::new(self.proxy_host.clone()).set_port(self.proxy_port);
        let io = ntex::connect::connect(proxy_msg).await?;
        // Minimal CONNECT. Only `Proxy-Authorization` is added when the
        // proxy URL carried userinfo; the credential never enters a log.
        let authority = format!("{target_host}:{target_port}");
        let mut request = format!("CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\n");
        if let Some(auth) = &self.proxy_auth {
            request.push_str(&format!("Proxy-Authorization: {}\r\n", auth.expose()));
        }
        request.push_str("\r\n");
        io.encode_slice(request.as_bytes())
            .map_err(ntex::connect::ConnectError::Io)?;
        io.flush(true).await.map_err(ntex::connect::ConnectError::Io)?;
        // Read until the end of the proxy response head.
        let mut head: Vec<u8> = Vec::new();
        loop {
            if head.len() > 16 * 1024 {
                return Err(io_err(format!("proxy CONNECT response too large for {target_host}")));
            }
            let chunk = io
                .recv(&ntex::codec::BytesCodec)
                .await
                .map_err(|error| io_err(format!("proxy CONNECT read failed for {target_host}: {error:?}")))?;
            let Some(bytes) = chunk else {
                return Err(io_err(format!("proxy closed CONNECT for {target_host}")));
            };
            head.extend_from_slice(&bytes);
            if find_connect_head_end(&head).is_some() {
                break;
            }
        }
        let end = find_connect_head_end(&head)
            .ok_or_else(|| io_err(format!("proxy CONNECT response incomplete for {target_host}")))?;
        let header = String::from_utf8_lossy(&head[..end]);
        let status = header
            .split("\r\n")
            .next()
            .unwrap_or("")
            .split_whitespace()
            .nth(1)
            .and_then(|code| code.parse::<u16>().ok())
            .unwrap_or(0);
        if status != 200 {
            return Err(io_err(format!("proxy CONNECT refused with HTTP {status} for {target_host}")));
        }
        if head.len() != end + 4 {
            return Err(io_err(format!("proxy sent unexpected bytes after CONNECT for {target_host}")));
        }
        // TLS to the original target with its host as SNI, over the tunnel.
        let domain = rustls::pki_types::ServerName::try_from(target_host.clone())
            .map_err(|_| io_err(format!("invalid TLS server name for {target_host}")))?;
        let tls = ntex::connect::rustls::TlsClientFilter::create(io, self.tls_config.clone(), domain)
            .await
            .map_err(ntex::connect::ConnectError::Io)?;
        Ok(tls.boxed())
    }
}

fn find_connect_head_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|window| window == b"\r\n\r\n")
}

/// The host of an absolute URL, lowercased and without userinfo, brackets or
/// port. Only used to consult the stored proxy plan, never to attach secrets.
fn url_host(url: &str) -> Result<String> {
    let rest = url
        .split_once("://")
        .map(|(_, rest)| rest)
        .ok_or_else(|| OcgError::config(format!("cannot determine the host of URL: {url}")))?;
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    let authority = authority.rsplit('@').next().unwrap_or("");
    let host = if let Some(inner) = authority.strip_prefix('[') {
        inner.split(']').next().unwrap_or("").to_string()
    } else {
        authority.split(':').next().unwrap_or("").to_string()
    };
    if host.is_empty() {
        return Err(OcgError::config(format!("URL has no host: {url}")));
    }
    Ok(host.to_ascii_lowercase())
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
        let proxy = self.proxy_endpoint_for(url)?;
        let token = self.token.clone();
        let url = url.to_owned();
        let runtime = ntex::rt::System::new("ocg-http", ntex::rt::DefaultRuntime);
        runtime.block_on(async move {
            match proxy {
                None => native_get(&url, token.as_ref()).await,
                Some(endpoint) => native_get_via_proxy(&url, token.as_ref(), &endpoint).await,
            }
        })
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
        let proxy = self.proxy_endpoint_for(url)?;
        let url = url.to_owned();
        let body = serde_json::to_vec(body)
            .map_err(|error| OcgError::config(format!("cannot encode JSON request: {error}")))?;
        let headers = headers
            .iter()
            .map(|(name, value)| ((*name).to_string(), (*value).to_string()))
            .collect::<Vec<_>>();
        let runtime = ntex::rt::System::new("ocg-http", ntex::rt::DefaultRuntime);
        runtime.block_on(async move {
            match proxy {
                None => native_post_json(&url, &headers, &body).await,
                Some(endpoint) => native_post_json_via_proxy(&url, &headers, &body, &endpoint).await,
            }
        })
    }

    fn post_json_stream(
        &self,
        url: &str,
        headers: &[(&str, &str)],
        body: &Value,
        on_chunk: Box<dyn FnMut(&[u8]) -> Result<bool> + Send>,
    ) -> Result<HttpResponse> {
        if !url.starts_with("https://") {
            return Err(OcgError::config(format!("refusing non-HTTPS URL: {url}")));
        }
        let proxy = self.proxy_endpoint_for(url)?;
        let url = url.to_owned();
        let body = serde_json::to_vec(body)
            .map_err(|error| OcgError::config(format!("cannot encode JSON request: {error}")))?;
        let headers = headers
            .iter()
            .map(|(name, value)| ((*name).to_string(), (*value).to_string()))
            .collect::<Vec<_>>();
        let runtime = ntex::rt::System::new("ocg-http", ntex::rt::DefaultRuntime);
        runtime.block_on(async move {
            match proxy {
                None => native_post_json_stream(&url, &headers, &body, on_chunk).await,
                Some(endpoint) => {
                    native_post_json_stream_via_proxy(&url, &headers, &body, &endpoint, on_chunk).await
                }
            }
        })
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

/// Stream a JSON POST response chunk by chunk, delivering each to `on_chunk`.
///
/// This is the only provider execution path: the success body is never
/// buffered whole. The ntex response payload is polled as a [`Stream`]; each
/// chunk is handed to `on_chunk` synchronously before the next is requested,
/// so the caller's consumption rate applies backpressure with no intermediate
/// queue. A non-2xx status instead reads a small bounded body for diagnostics
/// and returns it on the [`HttpResponse`].
async fn native_post_json_stream(
    url: &str,
    headers: &[(String, String)],
    body: &[u8],
    mut on_chunk: Box<dyn FnMut(&[u8]) -> Result<bool> + Send>,
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
    let mut response = Box::pin(response);

    if !(200..300).contains(&status) {
        // Non-2xx: read a small bounded diagnostic body. It is never streamed
        // to on_chunk and never carries OCG-attached headers back out.
        let mut diagnostic = Vec::new();
        while let Some(chunk) = poll_fn(|cx| response.as_mut().poll_next(cx)).await {
            let chunk = chunk.map_err(|error| {
                OcgError::config(format!("cannot read the error response from {url}: {error}"))
            })?;
            if diagnostic.len() + chunk.len() > MAX_PROVIDER_ERROR_BODY {
                break;
            }
            diagnostic.extend_from_slice(&chunk);
        }
        return Ok(HttpResponse {
            status,
            rate_limit: RateLimit::default(),
            body: diagnostic,
        });
    }

    // 2xx success: incremental delivery with backpressure. The total byte
    // count is enforced here because polling the raw payload stream bypasses
    // the buffered reader that applies the configured payload limit.
    let mut total: u64 = 0;
    while let Some(chunk) = poll_fn(|cx| response.as_mut().poll_next(cx)).await {
        let chunk = chunk.map_err(|error| {
            OcgError::config(format!("error reading streamed response from {url}: {error}"))
        })?;
        total += chunk.len() as u64;
        if total > MAX_BODY_BYTES {
            return Err(OcgError::config(format!(
                "streamed response from {url} exceeded the {MAX_BODY_BYTES} byte limit"
            )));
        }
        if !on_chunk(&chunk)? {
            // The consumer asked to stop (cancellation): stop reading and let
            // the caller treat the partial stream as not completed.
            break;
        }
    }
    Ok(HttpResponse {
        status,
        rate_limit: RateLimit::default(),
        body: Vec::new(),
    })
}

/// Build an ntex client whose secure connections tunnel through the given
/// `http://` proxy endpoint. The caller keeps the original target URL; only
/// this connector dials the proxy and performs `CONNECT`.
async fn proxy_client_for(endpoint: &str) -> Result<ntex::client::Client> {
    let dial = parse_proxy_dial(endpoint)?;
    let tls_config = proxy_tls_config()?;
    let connector = ProxySecureConnector {
        proxy_host: dial.host,
        proxy_port: dial.port,
        proxy_auth: dial.auth,
        tls_config,
    };
    let connector = ntex::client::Connector::default().secure_connector(connector);
    ntex::client::ClientBuilder::new()
        .response_timeout(RESPONSE_HEAD_TIMEOUT)
        .response_payload_limit(MAX_BODY_BYTES as usize)
        .response_payload_timeout(ntex::time::Millis::from(RESPONSE_BODY_TIMEOUT))
        .connector::<()>(connector)
        .build(ntex::SharedCfg::default())
        .await
        .map_err(|error| {
            OcgError::config(format!("cannot build the proxied HTTP client: {error}"))
        })
}

async fn native_get_via_proxy(url: &str, token: Option<&GithubToken>, endpoint: &str) -> Result<HttpResponse> {
    let client = proxy_client_for(endpoint).await?;
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
    if body.len() as u64 > MAX_BODY_BYTES { return Err(OcgError::config(format!("response from {url} exceeded the {MAX_BODY_BYTES} byte limit"))); }
    Ok(HttpResponse { status, rate_limit, body: body.to_vec() })
}

async fn native_post_json_via_proxy(
    url: &str,
    headers: &[(String, String)],
    body: &[u8],
    endpoint: &str,
) -> Result<HttpResponse> {
    if body.len() as u64 > MAX_BODY_BYTES {
        return Err(OcgError::config(format!(
            "request to {url} exceeded the {MAX_BODY_BYTES} byte limit"
        )));
    }
    let client = proxy_client_for(endpoint).await?;
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

async fn native_post_json_stream_via_proxy(
    url: &str,
    headers: &[(String, String)],
    body: &[u8],
    endpoint: &str,
    mut on_chunk: Box<dyn FnMut(&[u8]) -> Result<bool> + Send>,
) -> Result<HttpResponse> {
    if body.len() as u64 > MAX_BODY_BYTES {
        return Err(OcgError::config(format!(
            "request to {url} exceeded the {MAX_BODY_BYTES} byte limit"
        )));
    }
    let client = proxy_client_for(endpoint).await?;
    let mut request = client.post(url).header("User-Agent", concat!("ocg/", env!("CARGO_PKG_VERSION")));
    for (name, value) in headers {
        request = request.header(name.as_str(), value.as_str());
    }
    let response = request
        .send_body(body.to_vec())
        .await
        .map_err(|error| OcgError::config(format!("request to {url} failed: {error}")))?;
    let status = response.status().as_u16();
    let mut response = Box::pin(response);

    if !(200..300).contains(&status) {
        let mut diagnostic = Vec::new();
        while let Some(chunk) = poll_fn(|cx| response.as_mut().poll_next(cx)).await {
            let chunk = chunk.map_err(|error| {
                OcgError::config(format!("cannot read the error response from {url}: {error}"))
            })?;
            if diagnostic.len() + chunk.len() > MAX_PROVIDER_ERROR_BODY {
                break;
            }
            diagnostic.extend_from_slice(&chunk);
        }
        return Ok(HttpResponse {
            status,
            rate_limit: RateLimit::default(),
            body: diagnostic,
        });
    }

    let mut total: u64 = 0;
    while let Some(chunk) = poll_fn(|cx| response.as_mut().poll_next(cx)).await {
        let chunk = chunk.map_err(|error| {
            OcgError::config(format!("error reading streamed response from {url}: {error}"))
        })?;
        total += chunk.len() as u64;
        if total > MAX_BODY_BYTES {
            return Err(OcgError::config(format!(
                "streamed response from {url} exceeded the {MAX_BODY_BYTES} byte limit"
            )));
        }
        if !on_chunk(&chunk)? {
            break;
        }
    }
    Ok(HttpResponse {
        status,
        rate_limit: RateLimit::default(),
        body: Vec::new(),
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
