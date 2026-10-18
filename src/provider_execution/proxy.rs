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

pub(super) struct Proxy {
    uri: Uri,
    authorization: Option<HeaderValue>,
    exclusions: Vec<String>,
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
            exclusions: exclusions
                .split(',')
                .map(|host| host.trim().trim_start_matches('.').to_ascii_lowercase())
                .filter(|host| !host.is_empty())
                .collect(),
        })
    }

    fn bypasses(&self, host: &str) -> bool {
        let host = host.trim_end_matches('.').to_ascii_lowercase();
        self.exclusions.iter().any(|suffix| {
            suffix == "*"
                || host == *suffix
                || host
                    .strip_suffix(suffix)
                    .is_some_and(|prefix| prefix.ends_with('.'))
        })
    }
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
    let proxy = ["HTTPS_PROXY", "https_proxy", "ALL_PROXY", "all_proxy"]
        .into_iter()
        .find_map(std::env::var_os)
        .map(|value| {
            value
                .into_string()
                .map_err(|_| "provider proxy is not valid UTF-8".to_owned())
        })
        .transpose()?;
    let proxy = match proxy {
        Some(proxy) => Some(proxy),
        None => system_proxy()?,
    };
    let exclusions = std::env::var("NO_PROXY")
        .or_else(|_| std::env::var("no_proxy"))
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
    let roots = rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let mut tls = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    tls.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    let tls = Arc::new(tls);
    Ok(Connector::default().secure_connector(apply_fn_factory(
        TcpConnector::<Uri>::new(),
        async move |request: Connect<Uri>, service| {
            let target = request.get_ref().clone();
            let host = target.host().ok_or(ConnectError::InvalidInput)?;
            let tunnel = !proxy.bypasses(host);
            let io = service
                .call(if tunnel {
                    Connect::new(proxy.uri.clone())
                } else {
                    request
                })
                .await?;
            if tunnel {
                connect_tunnel(
                    &io,
                    host,
                    target.port_u16().unwrap_or(443),
                    proxy.authorization.as_ref(),
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
        assert!(proxy.bypasses("api.internal.example"));
        assert!(proxy.bypasses("internal.example"));
        assert!(!proxy.bypasses("notinternal.example"));
        assert!(!proxy.bypasses("provider.example"));
        assert!(
            Proxy::parse("http://proxy", "*")
                .expect("proxy")
                .bypasses("anything")
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
}
