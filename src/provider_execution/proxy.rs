use std::ffi::OsString;
use std::io;
use std::sync::OnceLock;

use base64::{Engine, engine::general_purpose::STANDARD};
use ntex::client::Connector;
use ntex::connect::{Connect, ConnectError, Connector as TcpConnector};
use ntex::http::Uri;
use ntex::io::{Io, IoBoxed};
use ntex::service::apply_fn_factory;

use super::*;

static PROXY: OnceLock<Result<Option<Proxy>, String>> = OnceLock::new();

const TUNNEL_TIMEOUT: Duration = Duration::from_secs(30);

pub(super) struct Proxy {
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

pub(super) fn connector() -> Result<Connector, ProviderExecutionError> {
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
    Ok(Connector::default().secure_connector(apply_fn_factory(
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
            let name =
                rustls::pki_types::ServerName::try_from(host.trim_matches(['[', ']']).to_owned())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_network_proxy_connect_authentication_and_exclusions() {
        let proxy = Proxy::parse(
            "http://alice:p%40ss@proxy.example:8080/",
            ".internal.example,localhost",
        )
        .expect("proxy");
        assert_eq!(proxy.uri.host(), Some("proxy.example"));
        assert_eq!(proxy.uri.port_u16(), Some(8080));
        assert!(proxy.bypasses("api.internal.example", 443));
        assert!(proxy.bypasses("internal.example", 443));
        assert!(!proxy.bypasses("notinternal.example", 443));
        assert!(!proxy.bypasses("provider.example", 443));
        assert!(
            Proxy::parse("http://proxy", "*")
                .expect("proxy")
                .bypasses("anything", 443)
        );
        for url in [
            "socks5://proxy:1080",
            "https://proxy",
            "file:///proxy",
            "http://proxy/path",
            "http://proxy:0",
        ] {
            assert!(Proxy::parse(url, "").is_err(), "{url}");
        }
        let authorization = proxy.authorization.expect("authorization");
        assert_eq!(authorization.as_bytes(), b"Basic YWxpY2U6cEBzcw==");
        assert!(authorization.is_sensitive());
        crate::test_support::on_system(move || async move {
            for (response, accepted) in [
                ("HTTP/1.1 200 Connection established\r\n\r\n", true),
                ("HTTP/1.1 407 Proxy authentication required\r\n\r\n", false),
            ] {
                let (client, server) = ntex::testing::IoTest::create();
                server.remote_buffer_cap(65_536);
                server.write(response);
                let io = Io::new(client, ntex::SharedCfg::default());
                let result =
                    connect_tunnel(&io, "provider.example", 443, Some(&authorization)).await;
                assert_eq!(result.is_ok(), accepted);
                let request = server.read().await.expect("CONNECT request");
                assert_eq!(request.as_ref(), b"CONNECT provider.example:443 HTTP/1.1\r\nHost: provider.example:443\r\nProxy-Authorization: Basic YWxpY2U6cEBzcw==\r\n\r\n");
            }
        });
    }

    #[test]
    fn provider_network_proxy_load_skips_variables_that_are_set_but_empty() {
        let vars = std::collections::BTreeMap::from([
            ("HTTPS_PROXY", ""),
            ("https_proxy", "http://lower.example:8080"),
            ("NO_PROXY", ""),
            ("no_proxy", "excluded.example"),
        ]);
        let proxy = load_from(
            |name| vars.get(name).map(|value| OsString::from(*value)),
            || panic!("a non-empty proxy variable must win over the system proxy"),
        )
        .expect("proxy")
        .expect("configured proxy");
        assert_eq!(proxy.uri.host(), Some("lower.example"));
        assert!(proxy.bypasses("api.excluded.example", 443));

        let called = std::cell::Cell::new(false);
        let proxy = load_from(
            |_| Some(OsString::new()),
            || {
                called.set(true);
                Ok(Some("http://system.example:8080".to_owned()))
            },
        )
        .expect("system proxy")
        .expect("configured proxy");
        assert!(
            called.get(),
            "empty proxy variables fall through to the system proxy"
        );
        assert_eq!(proxy.uri.host(), Some("system.example"));
    }

    #[test]
    fn provider_network_proxy_exclusions_match_ports_and_unbracketed_ipv6() {
        let proxy = Proxy::parse(
            "http://proxy.example:8080",
            "api.example:8443,example.com,[::1],::2",
        )
        .expect("proxy");
        assert!(proxy.bypasses("api.example", 8443));
        assert!(!proxy.bypasses("api.example", 443));
        assert!(proxy.bypasses("example.com", 443));
        assert!(proxy.bypasses("deep.example.com", 443));
        assert!(proxy.bypasses("::1", 443));
        assert!(proxy.bypasses("[::1]", 443));
        assert!(proxy.bypasses("::2", 443));
        assert!(!proxy.bypasses("::3", 443));
    }

    #[test]
    fn provider_network_proxy_connect_has_its_own_timeout() {
        assert_eq!(TUNNEL_TIMEOUT, Duration::from_secs(30));
        crate::test_support::on_system(|| async {
            let (client, _server) = ntex::testing::IoTest::create();
            let io = Io::new(client, ntex::SharedCfg::default());
            let error = connect_tunnel_with_timeout(
                &io,
                "provider.example",
                443,
                None,
                Duration::from_millis(50),
            )
            .await
            .expect_err("a silent proxy times out the tunnel");
            assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        });
    }

    #[test]
    fn provider_network_proxy_builds_tunnel_tls_at_initialization() {
        let proxy = Proxy::parse("http://proxy.example:8080", "").expect("proxy");
        assert!(
            Arc::ptr_eq(&proxy.tls(), &proxy.tls()),
            "the tunnel TLS config is built with the proxy and reused"
        );
        assert_eq!(
            proxy.tls().alpn_protocols,
            vec![b"h2".to_vec(), b"http/1.1".to_vec()]
        );
    }
}
