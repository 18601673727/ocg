use std::ffi::OsString;
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use base64::{Engine, engine::general_purpose::STANDARD};
use ntex::client::{Client, Connector};
use ntex::connect::{Connect, ConnectError, Connector as TcpConnector};
use ntex::http::Uri;
use ntex::http::header::HeaderValue;
use ntex::io::{Io, IoBoxed};
use ntex::service::apply_fn_factory;
use ntex::time::{Millis, Seconds};

use crate::service::ai::providers::ProviderExecutionError;

static PROXY: OnceLock<Result<Option<Proxy>, String>> = OnceLock::new();

const TUNNEL_TIMEOUT: Duration = Duration::from_secs(30);

/// The Authorization header of a request. It is marked sensitive, so no
/// dump of the request's headers, such as the HTTP client's own request
/// tracing, prints the credential. A credential that is no valid header
/// value is refused without being named.
pub(crate) fn authorization(credential: &str) -> Result<HeaderValue, ProviderExecutionError> {
    let mut value = HeaderValue::try_from(format!("Bearer {credential}")).map_err(|_| {
        ProviderExecutionError::Build {
            message: "the resolved credential is not a valid HTTP header value".to_owned(),
        }
    })?;
    value.set_sensitive(true);
    Ok(value)
}

pub(crate) async fn provider_client(timeout: Duration) -> Result<Client, ProviderExecutionError> {
    Client::builder()
        .header("User-Agent", concat!("ocg/", env!("CARGO_PKG_VERSION")))
        .connector::<()>(connector()?)
        .disable_redirects()
        .response_timeout(Millis(timeout.as_millis().min(u128::from(u32::MAX)) as u32))
        .response_payload_timeout(Millis::ZERO)
        // Stream idle deadlines bound stalled requests without terminating
        // long replies when an intermediary does not answer HTTP/2 PINGs.
        .build(
            ntex::SharedCfg::new("provider")
                .add(ntex_h2::ServiceConfig::new().set_ping_timeout(Seconds::ZERO)),
        )
        .await
        .map_err(|_| ProviderExecutionError::Build {
            message: "provider HTTP client could not be initialized".to_owned(),
        })
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

/// Reads one `NO_PROXY` entry: a host or IPv6 address, optionally followed by
/// a port that the entry then applies to alone.
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
        // No colon, or several (an unbracketed IPv6 address): no port.
        (value.as_str(), None)
    };
    (!host.is_empty()).then(|| Exclusion {
        host: host.to_owned(),
        port,
    })
}

fn bare_host(host: &str) -> String {
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
                let target = request.get_ref().clone();
                let host = target.host().ok_or(ConnectError::InvalidInput)?;
                let port = target.port_u16().unwrap_or(80);
                let tunnel = !proxy.bypasses(host, port);
                let io = service
                    .call(if tunnel {
                        Connect::new(proxy.uri.clone())
                    } else {
                        request
                    })
                    .await?;
                if tunnel {
                    connect_tunnel_with_timeout(
                        &io,
                        host,
                        port,
                        proxy.authorization.as_ref(),
                        TUNNEL_TIMEOUT,
                    )
                    .await?;
                }
                Ok::<_, ConnectError>(io)
            },
        ))
        .secure_connector(apply_fn_factory(
            TcpConnector::<Uri>::new(),
            async move |request: Connect<Uri>, service| {
                let target = request.get_ref().clone();
                let host = target.host().ok_or(ConnectError::InvalidInput)?;
                let port = target.port_u16().unwrap_or(443);
                let tunnel = !proxy.bypasses(host, port);
                let io = service
                    .call(if tunnel {
                        Connect::new(proxy.uri.clone())
                    } else {
                        request
                    })
                    .await?;
                if tunnel {
                    connect_tunnel_with_timeout(
                        &io,
                        host,
                        port,
                        proxy.authorization.as_ref(),
                        TUNNEL_TIMEOUT,
                    )
                    .await?;
                }
                let name = rustls::pki_types::ServerName::try_from(
                    host.trim_matches(['[', ']']).to_owned(),
                )
                .map_err(|_| ConnectError::InvalidInput)?;
                ntex::connect::rustls::TlsClientFilter::create(io, tls.clone(), name)
                    .await
                    .map(IoBoxed::from)
                    .map_err(ConnectError::from)
            },
        )))
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
                let target = request.get_ref().clone();
                let host = target.host().ok_or(ConnectError::InvalidInput)?;
                let port = target.port_u16().unwrap_or(80);
                let tunnel = proxy
                    .as_ref()
                    .is_some_and(|proxy| !proxy.bypasses(host, port));
                if let (true, Some(proxy)) = (tunnel, proxy.as_ref()) {
                    check_proxied_web_target(host, port, &http_non_public).await?;
                    let io = service.call(Connect::new(proxy.uri.clone())).await?;
                    connect_tunnel_with_timeout(
                        &io,
                        host,
                        port,
                        proxy.authorization.as_ref(),
                        TUNNEL_TIMEOUT,
                    )
                    .await?;
                    Ok::<_, ConnectError>(io)
                } else {
                    let addrs = resolve_direct_web_target(host, port, &http_non_public).await?;
                    service.call(request.set_addrs(addrs)).await
                }
            },
        ))
        .secure_connector(apply_fn_factory(
            TcpConnector::<Uri>::new(),
            async move |request: Connect<Uri>, service| {
                let target = request.get_ref().clone();
                let host = target.host().ok_or(ConnectError::InvalidInput)?;
                let port = target.port_u16().unwrap_or(443);
                let tunnel = proxy
                    .as_ref()
                    .is_some_and(|proxy| !proxy.bypasses(host, port));
                let io = if let (true, Some(proxy)) = (tunnel, proxy.as_ref()) {
                    check_proxied_web_target(host, port, &non_public).await?;
                    let io = service.call(Connect::new(proxy.uri.clone())).await?;
                    connect_tunnel_with_timeout(
                        &io,
                        host,
                        port,
                        proxy.authorization.as_ref(),
                        TUNNEL_TIMEOUT,
                    )
                    .await?;
                    io
                } else {
                    let addrs = resolve_direct_web_target(host, port, &non_public).await?;
                    service.call(request.set_addrs(addrs)).await?
                };
                let name = rustls::pki_types::ServerName::try_from(
                    host.trim_matches(['[', ']']).to_owned(),
                )
                .map_err(|_| ConnectError::InvalidInput)?;
                ntex::connect::rustls::TlsClientFilter::create(io, tls.clone(), name)
                    .await
                    .map(IoBoxed::from)
                    .map_err(ConnectError::from)
            },
        )))
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

/// Checks a target before opening a CONNECT tunnel through the proxy. The
/// proxy's own address is never checked (it is often 127.0.0.1), and local DNS
/// is not required because proxy-only networks may not have working local
/// resolution; DNS rebinding through the proxy is out of scope. Intranet-style
/// names (no dot, or `.local`, `.corp`, ...) are refused by literal check even
/// when local DNS cannot resolve them; other names that local DNS cannot
/// resolve are still allowed through the proxy.
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
    // 198.18.0.0/15 is intentionally allowed for resolved names: Clash/Surge
    // fake-IP DNS hands out these addresses for every name, so refusing them
    // would break web tools for those users. Only URL host literals in that
    // range are refused (see `is_non_public_host_literal`).
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
    // Local-use NAT64 64:ff9b:1::/48.
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
