use std::ffi::OsString;
use std::io;
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
