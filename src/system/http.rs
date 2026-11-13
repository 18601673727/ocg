use std::ffi::OsString;
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use base64::{Engine, engine::general_purpose::STANDARD};
use ntex::client::{Client, ClientRequest, Connector};
use ntex::connect::{Connect, ConnectError, Connector as TcpConnector};
use ntex::http::Uri;
use ntex::http::header::{HeaderName, HeaderValue};
use ntex::io::{Io, IoBoxed};
use ntex::service::apply_fn_factory;
use ntex::time::{Millis, Seconds};

use crate::service::ai::providers::ProviderExecutionError;

static PROXY: OnceLock<Result<Option<Proxy>, String>> = OnceLock::new();

pub(crate) fn direct_diagnostic_loopback(endpoint: &str) -> bool {
    use crate::system::execution_controls::{ControlState, ExecutionControl};
    match ExecutionControl::ExternalProviderTransport.state() {
        ControlState::Disabled => literal_direct_loopback(endpoint),
        ControlState::Enabled => false,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LoopbackEndpointClass {
    LiteralDirect,
    LiteralProxied,
    LiteralMalformed,
    LocalhostName,
    Ipv6Loopback,
    External,
    Unparseable,
}

impl LoopbackEndpointClass {
    pub(crate) const fn name(self) -> &'static str {
        match self {
            LoopbackEndpointClass::LiteralDirect => "literal_direct_loopback",
            LoopbackEndpointClass::LiteralProxied => "literal_loopback_proxied",
            LoopbackEndpointClass::LiteralMalformed => "literal_loopback_malformed",
            LoopbackEndpointClass::LocalhostName => "localhost_name",
            LoopbackEndpointClass::Ipv6Loopback => "ipv6_loopback",
            LoopbackEndpointClass::External => "external",
            LoopbackEndpointClass::Unparseable => "unparseable",
        }
    }
}

pub(crate) fn classify_loopback_endpoint(endpoint: &str) -> LoopbackEndpointClass {
    let Ok(uri) = endpoint.parse::<Uri>() else {
        return LoopbackEndpointClass::Unparseable;
    };
    match uri.host() {
        Some("127.0.0.1") => {
            if literal_direct_loopback(endpoint) {
                LoopbackEndpointClass::LiteralDirect
            } else if uri.scheme_str() == Some("http")
                && uri
                    .authority()
                    .is_some_and(|authority| !authority.as_str().contains('@'))
                && uri.query().is_none()
                && uri.port_u16().is_some_and(|port| port != 0)
            {
                LoopbackEndpointClass::LiteralProxied
            } else {
                LoopbackEndpointClass::LiteralMalformed
            }
        }
        Some("localhost") => LoopbackEndpointClass::LocalhostName,
        Some("::1" | "[::1]") => LoopbackEndpointClass::Ipv6Loopback,
        Some(_) | None => LoopbackEndpointClass::External,
    }
}

fn literal_direct_loopback(endpoint: &str) -> bool {
    let Ok(uri) = endpoint.parse::<Uri>() else {
        return false;
    };
    if uri.scheme_str() != Some("http")
        || uri.host() != Some("127.0.0.1")
        || uri
            .authority()
            .is_none_or(|authority| authority.as_str().contains('@'))
        || uri.query().is_some()
    {
        return false;
    }
    let Some(port) = uri.port_u16().filter(|port| *port != 0) else {
        return false;
    };
    match PROXY.get_or_init(load) {
        Ok(None) => true,
        Ok(Some(proxy)) => proxy.bypasses("127.0.0.1", port),
        Err(_) => false,
    }
}

const TUNNEL_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) fn authorization(credential: &str) -> Result<HeaderValue, ProviderExecutionError> {
    let mut value = HeaderValue::try_from(format!("Bearer {credential}")).map_err(|_| {
        ProviderExecutionError::Build {
            message: "the resolved credential is not a valid HTTP header value".to_owned(),
        }
    })?;
    value.set_sensitive(true);
    Ok(value)
}

#[derive(Default)]
pub(crate) struct RequestHeaders {
    fields: Vec<(HeaderName, HeaderValue)>,
    secrets: Vec<String>,
}

impl std::fmt::Debug for RequestHeaders {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_list()
            .entries(self.fields.iter().map(|(name, _)| name.as_str()))
            .finish()
    }
}

impl Drop for RequestHeaders {
    fn drop(&mut self) {
        self.secrets
            .iter_mut()
            .for_each(crate::system::secrets::wipe_string);
    }
}

impl RequestHeaders {
    pub(crate) fn apply(&self, mut request: ClientRequest) -> ClientRequest {
        for (name, value) in &self.fields {
            request = request.set_header(name.clone(), value.clone());
        }
        request
    }

    pub(crate) fn secrets(&self) -> &[String] {
        &self.secrets
    }
}

pub(crate) async fn resolve_request_headers(
    frozen: &crate::db::entities::FrozenHeaders,
    resolver: &dyn crate::service::ai::providers::CredentialResolver,
) -> Result<RequestHeaders, ProviderExecutionError> {
    use crate::db::entities::{HeaderScope, OutboundHeaderValue, validate_fields};
    let invalid = || ProviderExecutionError::Build {
        message: "the custom request headers are invalid".to_owned(),
    };
    validate_fields(&frozen.fields, HeaderScope::Connection).map_err(|_| invalid())?;
    let mut resolved = RequestHeaders::default();
    for field in &frozen.fields {
        let name = HeaderName::try_from(field.name.as_str()).map_err(|_| invalid())?;
        let (text, secret) = match &field.value {
            OutboundHeaderValue::Plain(value) => (value.clone(), false),
            OutboundHeaderValue::Secret(reference) => {
                let value = resolver
                    .resolve(Some(reference.as_str()))
                    .await?
                    .ok_or_else(|| ProviderExecutionError::Build {
                        message: "a secret request header cannot be resolved".to_owned(),
                    })?;
                (value, true)
            }
        };
        let value = HeaderValue::try_from(text.as_str()).map(|mut value| {
            value.set_sensitive(secret);
            value
        });
        if secret {
            resolved.secrets.push(text);
        }
        resolved.fields.push((name, value.map_err(|_| invalid())?));
    }
    Ok(resolved)
}

async fn standard_client(timeout: Duration) -> Result<Client, ProviderExecutionError> {
    Client::builder()
        .header("User-Agent", concat!("ocg/", env!("CARGO_PKG_VERSION")))
        .connector::<()>(connector()?)
        .disable_redirects()
        .response_timeout(Millis(timeout.as_millis().min(u128::from(u32::MAX)) as u32))
        .response_payload_timeout(Millis::ZERO)
        .build(
            ntex::SharedCfg::new("provider")
                .add(ntex_h2::ServiceConfig::new().set_ping_timeout(Seconds::ZERO)),
        )
        .await
        .map_err(|_| ProviderExecutionError::Build {
            message: "provider HTTP client could not be initialized".to_owned(),
        })
}

pub(crate) async fn provider_client_for(
    timeout: Duration,
    destination: &crate::service::ai::providers::DestinationPolicy,
    endpoint: &str,
) -> Result<Client, ProviderExecutionError> {
    use crate::service::ai::providers::DestinationPolicy;
    match destination {
        DestinationPolicy::Standard => {
            if !direct_diagnostic_loopback(endpoint) {
                return Err(ProviderExecutionError::Build {
                    message:
                        "this build sends Provider requests to a literal loopback endpoint only"
                            .to_owned(),
                });
            }
            standard_client(timeout).await
        }
        DestinationPolicy::ApprovedHttps {
            host,
            port,
            route,
            authority,
        } => {
            if !authority.control().is_enabled() {
                return Err(ProviderExecutionError::Build {
                    message: authority.control().refusal(),
                });
            }
            if !authority.admits_route(route) {
                return Err(ProviderExecutionError::Build {
                    message: format!("{} requires a trusted proxy route", authority.name()),
                });
            }
            Client::builder()
                .header("User-Agent", concat!("ocg/", env!("CARGO_PKG_VERSION")))
                .connector::<()>(approved_connector(host, *port, route)?)
                .disable_redirects()
                .response_timeout(Millis(timeout.as_millis().min(u128::from(u32::MAX)) as u32))
                .response_payload_timeout(Millis::ZERO)
                .build(
                    ntex::SharedCfg::new("provider-approved")
                        .add(ntex_h2::ServiceConfig::new().set_ping_timeout(Seconds::ZERO)),
                )
                .await
                .map_err(|_| ProviderExecutionError::Build {
                    message: "provider HTTP client could not be initialized".to_owned(),
                })
        }
    }
}

pub(crate) enum DiscoveryClientError {
    NotAuthorized,
    Unavailable,
}

pub(crate) fn discovery_permitted(endpoint: &str) -> bool {
    use crate::system::execution_controls::{ControlState, ExecutionControl};
    match ExecutionControl::ProviderDiscovery.state() {
        ControlState::Enabled => true,
        ControlState::Disabled => literal_direct_loopback(endpoint),
    }
}

pub(crate) async fn discovery_client(
    timeout: Duration,
    endpoint: &str,
) -> Result<Client, DiscoveryClientError> {
    if !discovery_permitted(endpoint) {
        return Err(DiscoveryClientError::NotAuthorized);
    }
    standard_client(timeout)
        .await
        .map_err(|_| DiscoveryClientError::Unavailable)
}

pub(crate) async fn release_client() -> Result<Client, ProviderExecutionError> {
    Client::builder()
        .header("User-Agent", concat!("ocg/", env!("CARGO_PKG_VERSION")))
        .connector::<()>(connector()?)
        .disable_redirects()
        .response_timeout(Millis(30_000))
        .response_payload_timeout(Millis(30_000))
        .build(ntex::SharedCfg::new("release"))
        .await
        .map_err(|_| ProviderExecutionError::Build {
            message: "release HTTP client could not be initialized".to_owned(),
        })
}

pub(crate) async fn web_client(
    timeout: Duration,
    non_public: Arc<AtomicBool>,
) -> Result<Client, ProviderExecutionError> {
    Client::builder()
        .header("User-Agent", concat!("ocg/", env!("CARGO_PKG_VERSION")))
        .connector::<()>(web_connector(non_public)?)
        .disable_redirects()
        .response_timeout(Millis(timeout.as_millis().min(u128::from(u32::MAX)) as u32))
        .response_payload_timeout(Millis::ZERO)
        .build(
            ntex::SharedCfg::new("web")
                .add(ntex_h2::ServiceConfig::new().set_ping_timeout(Seconds::ZERO)),
        )
        .await
        .map_err(|_| ProviderExecutionError::Build {
            message: "web HTTP client could not be initialized".to_owned(),
        })
}

struct Proxy {
    uri: Uri,
    authorization: Option<HeaderValue>,
    exclusions: Vec<Exclusion>,
    tls: Arc<rustls::ClientConfig>,
}

struct Exclusion {
    host: String,
    port: Option<u16>,
}

impl Proxy {
    fn parse(url: &str, exclusions: &str) -> Result<Self, String> {
        let invalid =
            || "provider proxy must be an http:// URL with a host and valid port".to_owned();
        let (scheme, rest) = url.split_once("://").ok_or_else(invalid)?;
        if scheme != "http" {
            return Err("provider proxy refused: only http:// proxies are supported (including HTTPS via CONNECT)".to_owned());
        }
        let rest = rest.strip_suffix('/').unwrap_or(rest);
        let (credentials, authority) = rest
            .rsplit_once('@')
            .map_or((None, rest), |(credentials, host)| {
                (Some(credentials), host)
            });
        let uri: Uri = format!("http://{authority}")
            .parse()
            .map_err(|_| invalid())?;
        if uri.host().is_none_or(str::is_empty)
            || uri
                .authority()
                .is_none_or(|value| value.as_str() != authority)
            || uri.port().is_some_and(|port| port.as_u16() == 0)
        {
            return Err(invalid());
        }
        let authorization = credentials
            .map(|credentials| {
                let decoded = decode_credentials(credentials).ok_or_else(invalid)?;
                let decoded = if decoded.contains(':') {
                    decoded
                } else {
                    format!("{decoded}:")
                };
                let mut header =
                    HeaderValue::try_from(format!("Basic {}", STANDARD.encode(decoded)))
                        .map_err(|_| invalid())?;
                header.set_sensitive(true);
                Ok::<_, String>(header)
            })
            .transpose()?;
        Ok(Self {
            uri,
            authorization,
            exclusions: exclusions.split(',').filter_map(parse_exclusion).collect(),
            tls: tunnel_tls(),
        })
    }

    fn tls(&self) -> Arc<rustls::ClientConfig> {
        self.tls.clone()
    }

    fn bypasses(&self, host: &str, port: u16) -> bool {
        let host = bare_host(host);
        self.exclusions.iter().any(|exclusion| {
            exclusion.port.is_none_or(|only| only == port)
                && (exclusion.host == "*"
                    || host == exclusion.host
                    || host
                        .strip_suffix(&exclusion.host)
                        .is_some_and(|prefix| prefix.ends_with('.')))
        })
    }
}

fn parse_exclusion(value: &str) -> Option<Exclusion> {
    let value = value
        .trim()
        .trim_start_matches('.')
        .trim_end_matches('.')
        .to_ascii_lowercase();
    if value.is_empty() {
        return None;
    }
    let (host, port) = if let Some(rest) = value.strip_prefix('[') {
        let (host, rest) = rest.split_once(']')?;
        let port = match rest {
            "" => None,
            rest => Some(rest.strip_prefix(':')?.parse().ok()?),
        };
        (host, port)
    } else if value.matches(':').count() == 1 {
        let (host, port) = value.split_once(':')?;
        (host, Some(port.parse().ok()?))
    } else {
        (value.as_str(), None)
    };
    (!host.is_empty()).then(|| Exclusion {
        host: host.to_owned(),
        port,
    })
}

pub(crate) fn bare_host(host: &str) -> String {
    let host = host.trim();
    let host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    host.trim_end_matches('.').to_ascii_lowercase()
}

fn tunnel_tls() -> Arc<rustls::ClientConfig> {
    let roots = rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let mut tls = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    tls.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Arc::new(tls)
}

fn decode_credentials(value: &str) -> Option<String> {
    let mut bytes = value.bytes();
    let mut decoded = Vec::new();
    while let Some(byte) = bytes.next() {
        decoded.push(if byte == b'%' {
            let high = char::from(bytes.next()?).to_digit(16)?;
            let low = char::from(bytes.next()?).to_digit(16)?;
            (high * 16 + low) as u8
        } else {
            byte
        });
    }
    String::from_utf8(decoded).ok()
}

fn load() -> Result<Option<Proxy>, String> {
    load_from(|name| std::env::var_os(name), system_proxy)
}

fn load_from(
    lookup: impl Fn(&str) -> Option<OsString>,
    system: impl FnOnce() -> Result<Option<String>, String>,
) -> Result<Option<Proxy>, String> {
    let proxy = ["HTTPS_PROXY", "https_proxy", "ALL_PROXY", "all_proxy"]
        .into_iter()
        .find_map(|name| lookup(name).filter(|value| !value.is_empty()))
        .map(|value| {
            value
                .into_string()
                .map_err(|_| "provider proxy is not valid UTF-8".to_owned())
        })
        .transpose()?;
    let proxy = match proxy {
        Some(proxy) => Some(proxy),
        None => system()?,
    };
    let exclusions = ["NO_PROXY", "no_proxy"]
        .into_iter()
        .filter_map(|name| lookup(name).filter(|value| !value.is_empty()))
        .find_map(|value| value.into_string().ok())
        .unwrap_or_default();
    proxy
        .map(|proxy| Proxy::parse(&proxy, &exclusions))
        .transpose()
}

#[cfg(target_os = "macos")]
fn system_proxy() -> Result<Option<String>, String> {
    let output = std::process::Command::new("/usr/sbin/scutil")
        .arg("--proxy")
        .output()
        .map_err(|_| "could not read the macOS HTTPS proxy".to_owned())?;
    if !output.status.success() {
        return Err("could not read the macOS HTTPS proxy".to_owned());
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let field = |name: &str| {
        text.lines()
            .filter_map(|line| line.trim().split_once(" : "))
            .find_map(|(key, value)| (key == name).then_some(value))
    };
    if field("HTTPSEnable") != Some("1") {
        return Ok(None);
    }
    match (field("HTTPSProxy"), field("HTTPSPort")) {
        (Some(host), Some(port)) => Ok(Some(format!("http://{host}:{port}"))),
        _ => Err("macOS HTTPS proxy is enabled but has no host or port".to_owned()),
    }
}

#[cfg(not(target_os = "macos"))]
fn system_proxy() -> Result<Option<String>, String> {
    Ok(None)
}

pub(crate) fn initialize() -> Result<(), String> {
    match PROXY.get_or_init(load) {
        Ok(_) => Ok(()),
        Err(error) => {
            eprintln!("{error}");
            Err(error.clone())
        }
    }
}

pub(crate) fn log_configuration() {
    if let Some(Ok(Some(proxy))) = PROXY.get() {
        log::info!(
            "provider HTTPS proxy: {}:{}",
            proxy.uri.host().unwrap_or_default(),
            proxy.uri.port_u16().unwrap_or(80)
        );
    }
}

async fn wrap_tls(
    io: Io,
    host: &str,
    tls: Arc<rustls::ClientConfig>,
) -> Result<IoBoxed, ConnectError> {
    let name = rustls::pki_types::ServerName::try_from(host.trim_matches(['[', ']']).to_owned())
        .map_err(|_| ConnectError::InvalidInput)?;
    ntex::connect::rustls::TlsClientFilter::create(io, tls, name)
        .await
        .map(IoBoxed::from)
        .map_err(ConnectError::from)
}

async fn open_proxy_io(
    proxy: &Proxy,
    request: Connect<Uri>,
    default_port: u16,
    call: impl AsyncFnOnce(Connect<Uri>) -> Result<Io, ConnectError>,
) -> Result<(Io, String), ConnectError> {
    let target = request.get_ref().clone();
    let host = target.host().ok_or(ConnectError::InvalidInput)?.to_owned();
    let port = target.port_u16().unwrap_or(default_port);
    let tunnel = !proxy.bypasses(&host, port);
    let io = call(if tunnel {
        Connect::new(proxy.uri.clone())
    } else {
        request
    })
    .await?;
    if tunnel {
        connect_tunnel_with_timeout(
            &io,
            &host,
            port,
            proxy.authorization.as_ref(),
            TUNNEL_TIMEOUT,
        )
        .await?;
    }
    Ok((io, host))
}

fn connector() -> Result<Connector, ProviderExecutionError> {
    let proxy =
        PROXY
            .get_or_init(load)
            .as_ref()
            .map_err(|message| ProviderExecutionError::Build {
                message: message.clone(),
            })?;
    let Some(proxy) = proxy else {
        return Ok(Connector::default());
    };
    let tls = proxy.tls();
    Ok(Connector::default()
        .connector(apply_fn_factory(
            TcpConnector::<Uri>::new(),
            async move |request: Connect<Uri>, service| {
                open_proxy_io(proxy, request, 80, |req| service.call(req))
                    .await
                    .map(|(io, _)| io)
            },
        ))
        .secure_connector(apply_fn_factory(
            TcpConnector::<Uri>::new(),
            async move |request: Connect<Uri>, service| {
                let (io, host) =
                    open_proxy_io(proxy, request, 443, |req| service.call(req)).await?;
                wrap_tls(io, &host, tls.clone()).await
            },
        )))
}

fn approved_connector(
    host: &str,
    port: u16,
    route: &crate::service::ai::providers::ApprovedRoute,
) -> Result<Connector, ProviderExecutionError> {
    use crate::service::ai::providers::ApprovedRoute;
    let tls = tunnel_tls();
    let approved = Arc::new((bare_host(host), port));
    let proxy = match route {
        ApprovedRoute::Direct => None,
        ApprovedRoute::TrustedProxy(proxy) => {
            let addr = proxy.address();
            let uri: Uri =
                format!("http://{addr}")
                    .parse()
                    .map_err(|_| ProviderExecutionError::Build {
                        message: "the trusted proxy address is not usable".to_owned(),
                    })?;
            Some((addr, uri))
        }
    };
    Ok(Connector::default()
        .connector(apply_fn_factory(
            TcpConnector::<Uri>::new(),
            async move |_request: Connect<Uri>, _service| {
                Err::<Io, ConnectError>(ConnectError::InvalidInput)
            },
        ))
        .secure_connector(apply_fn_factory(
            TcpConnector::<Uri>::new(),
            async move |request: Connect<Uri>, service| {
                let target = request.get_ref().clone();
                let host = target.host().ok_or(ConnectError::InvalidInput)?;
                let port = target.port_u16().unwrap_or(443);
                if bare_host(host) != approved.0 || port != approved.1 {
                    return Err(ConnectError::InvalidInput);
                }
                let io = if let Some((addr, uri)) = &proxy {
                    let io = service
                        .call(Connect::new(uri.clone()).set_addrs(vec![*addr]))
                        .await?;
                    connect_tunnel_with_timeout(&io, host, port, None, TUNNEL_TIMEOUT).await?;
                    io
                } else {
                    let refused = Arc::new(AtomicBool::new(false));
                    let addrs = resolve_approved_target(host, port, &refused).await?;
                    service.call(request.set_addrs(addrs)).await?
                };
                wrap_tls(io, host, tls.clone()).await
            },
        )))
}

async fn open_web_io(
    proxy: Option<&Proxy>,
    request: Connect<Uri>,
    default_port: u16,
    non_public: &Arc<AtomicBool>,
    call: impl AsyncFnOnce(Connect<Uri>) -> Result<Io, ConnectError>,
) -> Result<(Io, String), ConnectError> {
    let target = request.get_ref().clone();
    let host = target.host().ok_or(ConnectError::InvalidInput)?.to_owned();
    let port = target.port_u16().unwrap_or(default_port);
    let tunnel = proxy.is_some_and(|proxy| !proxy.bypasses(&host, port));
    let io = if let (true, Some(proxy)) = (tunnel, proxy) {
        check_proxied_web_target(&host, port, non_public).await?;
        let io = call(Connect::new(proxy.uri.clone())).await?;
        connect_tunnel_with_timeout(
            &io,
            &host,
            port,
            proxy.authorization.as_ref(),
            TUNNEL_TIMEOUT,
        )
        .await?;
        io
    } else {
        let addrs = resolve_direct_web_target(&host, port, non_public).await?;
        call(request.set_addrs(addrs)).await?
    };
    Ok((io, host))
}

fn web_connector(non_public: Arc<AtomicBool>) -> Result<Connector, ProviderExecutionError> {
    let proxy =
        PROXY
            .get_or_init(load)
            .as_ref()
            .map_err(|message| ProviderExecutionError::Build {
                message: message.clone(),
            })?;
    let tls = proxy.as_ref().map_or_else(tunnel_tls, Proxy::tls);
    let http_non_public = non_public.clone();
    Ok(Connector::default()
        .connector(apply_fn_factory(
            TcpConnector::<Uri>::new(),
            async move |request: Connect<Uri>, service| {
                open_web_io(proxy.as_ref(), request, 80, &http_non_public, |req| {
                    service.call(req)
                })
                .await
                .map(|(io, _)| io)
            },
        ))
        .secure_connector(apply_fn_factory(
            TcpConnector::<Uri>::new(),
            async move |request: Connect<Uri>, service| {
                let (io, host) = open_web_io(proxy.as_ref(), request, 443, &non_public, |req| {
                    service.call(req)
                })
                .await?;
                wrap_tls(io, &host, tls.clone()).await
            },
        )))
}

async fn resolve_approved_target(
    host: &str,
    port: u16,
    non_public: &Arc<AtomicBool>,
) -> Result<Vec<SocketAddr>, ConnectError> {
    let addrs = resolve_direct_web_target(host, port, non_public).await?;
    let substituted = addrs.iter().any(|addr| match addr.ip() {
        IpAddr::V4(v4) => is_fake_ip_ipv4(v4),
        IpAddr::V6(v6) => embedded_ipv4(v6.segments()).is_some_and(is_fake_ip_ipv4),
    });
    if substituted {
        non_public.store(true, Ordering::SeqCst);
        return Err(ConnectError::Unresolved);
    }
    Ok(addrs)
}

async fn resolve_direct_web_target(
    host: &str,
    port: u16,
    non_public: &Arc<AtomicBool>,
) -> Result<Vec<SocketAddr>, ConnectError> {
    if is_non_public_host_literal(host) {
        non_public.store(true, Ordering::SeqCst);
        return Err(ConnectError::Unresolved);
    }
    let lookup_host = host.trim_matches(['[', ']']).to_owned();
    let resolved = ntex::rt::spawn_blocking(move || (lookup_host.as_str(), port).to_socket_addrs())
        .await
        .map_err(|_| ConnectError::Unresolved)?
        .map_err(|_| ConnectError::Unresolved)?;
    let addrs: Vec<SocketAddr> = resolved.collect();
    if addrs.is_empty() {
        return Err(ConnectError::Unresolved);
    }
    if addrs.iter().any(|addr| !is_public_ip(addr.ip())) {
        non_public.store(true, Ordering::SeqCst);
        return Err(ConnectError::Unresolved);
    }
    Ok(addrs)
}

async fn check_proxied_web_target(
    host: &str,
    port: u16,
    non_public: &Arc<AtomicBool>,
) -> Result<(), ConnectError> {
    if is_non_public_host_literal(host) {
        non_public.store(true, Ordering::SeqCst);
        return Err(ConnectError::Unresolved);
    }
    let lookup_host = host.trim_matches(['[', ']']).to_owned();
    if let Ok(Ok(addrs)) =
        ntex::rt::spawn_blocking(move || (lookup_host.as_str(), port).to_socket_addrs()).await
        && addrs.into_iter().any(|addr| !is_public_ip(addr.ip()))
    {
        non_public.store(true, Ordering::SeqCst);
        return Err(ConnectError::Unresolved);
    }
    Ok(())
}

pub(crate) fn host_may_be_external(host: &str) -> bool {
    !is_non_public_host_literal(host)
}

fn is_non_public_host_literal(host: &str) -> bool {
    let bare = bare_host(host);
    if bare == "localhost" || bare.ends_with(".localhost") {
        return true;
    }
    let unzoned = bare.split_once('%').map_or(bare.as_str(), |(ip, _)| ip);
    if let Ok(ipv6) = unzoned.parse::<Ipv6Addr>() {
        return !is_public_ipv6(ipv6)
            || embedded_ipv4(ipv6.segments()).is_some_and(is_fake_ip_ipv4);
    }
    if host.trim().starts_with('[') {
        return true;
    }
    if let Some(ipv4) = parse_inet_aton(&bare) {
        return !is_public_ipv4(ipv4) || is_fake_ip_ipv4(ipv4);
    }
    !bare.contains('.')
        || [
            ".internal",
            ".local",
            ".lan",
            ".home.arpa",
            ".corp",
            ".intranet",
        ]
        .iter()
        .any(|suffix| bare.ends_with(suffix))
}

fn parse_inet_aton_part(part: &str) -> Option<u32> {
    if part.is_empty() || part.starts_with('+') || part.starts_with('-') {
        return None;
    }
    if let Some(hex) = part.strip_prefix("0x").or_else(|| part.strip_prefix("0X")) {
        u32::from_str_radix(hex, 16).ok()
    } else if part.len() > 1 && part.starts_with('0') {
        u32::from_str_radix(part, 8).ok()
    } else {
        part.parse::<u32>().ok()
    }
}

fn parse_inet_aton(host: &str) -> Option<Ipv4Addr> {
    let parts: Vec<&str> = host.split('.').collect();
    if parts.is_empty() || parts.len() > 4 {
        return None;
    }
    let mut values = Vec::with_capacity(parts.len());
    for part in parts {
        values.push(parse_inet_aton_part(part)?);
    }
    let address = match values.as_slice() {
        [a] => *a,
        [a, b] if *a <= 0xff && *b <= 0x00ff_ffff => (*a << 24) | *b,
        [a, b, c] if *a <= 0xff && *b <= 0xff && *c <= 0xffff => (*a << 24) | (*b << 16) | *c,
        [a, b, c, d] if *a <= 0xff && *b <= 0xff && *c <= 0xff && *d <= 0xff => {
            (*a << 24) | (*b << 16) | (*c << 8) | *d
        }
        _ => return None,
    };
    Some(Ipv4Addr::from(address))
}

fn is_public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_public_ipv4(v4),
        IpAddr::V6(v6) => is_public_ipv6(v6),
    }
}

fn is_public_ipv4(ip: Ipv4Addr) -> bool {
    let [a, b, c, d] = ip.octets();
    !(a == 0
        || a == 10
        || (a == 100 && (b & 0xc0) == 64)
        || a == 127
        || (a == 169 && b == 254)
        || (a == 172 && (b & 0xf0) == 16)
        || (a == 192 && b == 0 && (c == 0 || c == 2))
        || (a == 192 && b == 168)
        || (a == 198 && b == 51 && c == 100)
        || (a == 203 && b == 0 && c == 113)
        || (a & 0xf0) == 224
        || (a & 0xf0) == 240
        || (a == 255 && b == 255 && c == 255 && d == 255))
}

fn embedded_ipv4(s: [u16; 8]) -> Option<Ipv4Addr> {
    let from_tail = || Ipv4Addr::new((s[6] >> 8) as u8, s[6] as u8, (s[7] >> 8) as u8, s[7] as u8);
    if s[0..6] == [0, 0, 0, 0, 0, 0xffff]
        || s[0..6] == [0x0064, 0xff9b, 0, 0, 0, 0]
        || s[0..6] == [0, 0, 0, 0, 0xffff, 0]
        || s[0..6] == [0; 6]
    {
        Some(from_tail())
    } else if s[0] == 0x2002 {
        Some(Ipv4Addr::new(
            (s[1] >> 8) as u8,
            s[1] as u8,
            (s[2] >> 8) as u8,
            s[2] as u8,
        ))
    } else {
        None
    }
}

fn is_fake_ip_ipv4(ip: Ipv4Addr) -> bool {
    let [a, b, _, _] = ip.octets();
    a == 198 && (b & 0xfe) == 18
}

fn is_public_ipv6(ip: Ipv6Addr) -> bool {
    let s = ip.segments();
    if s[0] == 0x0064 && s[1] == 0xff9b && s[2] == 1 {
        return false;
    }
    let embedded_v4 = embedded_ipv4(s);
    if let Some(v4) = embedded_v4 {
        return is_public_ipv4(v4);
    }
    !(s == [0, 0, 0, 0, 0, 0, 0, 0]
        || s == [0, 0, 0, 0, 0, 0, 0, 1]
        || (s[0] & 0xfe00) == 0xfc00
        || (s[0] & 0xffc0) == 0xfe80
        || (s[0] & 0xffc0) == 0xfec0
        || (s[0] & 0xff00) == 0xff00
        || s[0..4] == [0x0100, 0, 0, 0]
        || (s[0] == 0x2001 && s[1] == 0x0db8)
        || (s[0] == 0x2001 && (s[1] & 0xfe00) == 0))
}

async fn connect_tunnel_with_timeout(
    io: &Io,
    host: &str,
    port: u16,
    authorization: Option<&HeaderValue>,
    timeout: Duration,
) -> io::Result<()> {
    match ntex::time::timeout(timeout, connect_tunnel(io, host, port, authorization)).await {
        Ok(result) => result,
        Err(_) => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "provider proxy CONNECT timed out",
        )),
    }
}

async fn connect_tunnel(
    io: &Io,
    host: &str,
    port: u16,
    authorization: Option<&HeaderValue>,
) -> io::Result<()> {
    let authority = format!("{host}:{port}");
    io.with_write_buf(|buffer| {
        buffer.extend_from_slice(
            format!("CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\n").as_bytes(),
        );
        if let Some(authorization) = authorization {
            buffer.extend_from_slice(b"Proxy-Authorization: ");
            buffer.extend_from_slice(authorization.as_bytes());
            buffer.extend_from_slice(b"\r\n");
        }
        buffer.extend_from_slice(b"\r\n");
    })?;
    io.flush(true).await?;
    let mut head = Vec::new();
    while head.len() < 16 * 1024 {
        let mut byte = [0];
        io.read(&mut byte).await?;
        head.push(byte[0]);
        if head.ends_with(b"\r\n\r\n") {
            let status = std::str::from_utf8(&head).ok().and_then(|head| {
                let mut words = head.lines().next()?.split_whitespace();
                matches!(words.next()?, "HTTP/1.1" | "HTTP/1.0").then_some(())?;
                words.next()?.parse::<u16>().ok()
            });
            return if status.is_some_and(|status| (200..300).contains(&status)) {
                Ok(())
            } else {
                Err(io::Error::other("provider proxy refused CONNECT"))
            };
        }
    }
    Err(io::Error::other(
        "provider proxy CONNECT response exceeded its limit",
    ))
}
