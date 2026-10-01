//! Proxy policy: resolve one effective proxy configuration, hand it to the
//! HTTP client, and describe exactly what a child process should inherit.
//!
//! Resolution is deterministic and injectable. The precedence is:
//!
//! 1. `--disable-proxy` on the command line,
//! 2. a truthy `OCG_DISABLE_PROXY`,
//! 3. any non-empty standard proxy variable (upper- or lower-case),
//! 4. static macOS discovery via `/usr/sbin/scutil --proxy`,
//! 5. direct.
//!
//! The module never renders a proxy URL or credential. [`SecretUrl`] and the
//! redacting `Debug` implementations are the only way those values travel.

use std::fmt;
use std::process::Command;

/// Every proxy variable spelling OCG recognizes.
pub const PROXY_ENV_VARS: [&str; 8] = [
    "HTTP_PROXY",
    "http_proxy",
    "HTTPS_PROXY",
    "https_proxy",
    "ALL_PROXY",
    "all_proxy",
    "NO_PROXY",
    "no_proxy",
];

/// The single opt-out variable. Canonical name only.
pub const DISABLE_ENV: &str = "OCG_DISABLE_PROXY";

/// Read-only access to the ambient environment. Injectable so resolution
/// tests never depend on the host.
pub trait ProxyEnv: Send + Sync {
    /// A non-empty value for `name`, or `None`.
    fn var(&self, name: &str) -> Option<String>;
}

/// The real process environment.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemProxyEnv;

impl ProxyEnv for SystemProxyEnv {
    fn var(&self, name: &str) -> Option<String> {
        std::env::var(name).ok().filter(|value| !value.is_empty())
    }
}

/// A fixed environment for tests and offline tooling.
#[derive(Default, Clone)]
pub struct MapProxyEnv {
    vars: Vec<(String, String)>,
}

impl fmt::Debug for MapProxyEnv {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MapProxyEnv")
            .field("variables", &self.vars.len())
            .finish()
    }
}

impl MapProxyEnv {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with(mut self, name: &str, value: &str) -> Self {
        self.vars.push((name.to_string(), value.to_string()));
        self
    }
}

impl ProxyEnv for MapProxyEnv {
    fn var(&self, name: &str) -> Option<String> {
        self.vars
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.clone())
            .filter(|value| !value.is_empty())
    }
}

/// Supplies the raw `/usr/sbin/scutil --proxy` output. The system
/// implementation runs the command on macOS; tests inject fixed text.
pub trait StaticProxyProvider: Send + Sync {
    fn raw_scutil(&self) -> Option<String>;
}

/// A provider that never reports a static system proxy.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoStaticProxy;

impl StaticProxyProvider for NoStaticProxy {
    fn raw_scutil(&self) -> Option<String> {
        None
    }
}

/// Which proxy protocol a resolved endpoint speaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProxyScheme {
    Http,
    Https,
    All,
}

/// A URL that must never be rendered. Only [`SecretUrl::expose`] returns the
/// raw value, and only the HTTP client and the child environment use it.
#[derive(Clone, PartialEq, Eq)]
pub struct SecretUrl(String);

impl SecretUrl {
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

impl fmt::Debug for SecretUrl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<redacted>")
    }
}

impl fmt::Display for SecretUrl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<redacted>")
    }
}

/// One typed proxy endpoint.
#[derive(Clone, PartialEq, Eq)]
pub struct ProxyEndpoint {
    scheme: ProxyScheme,
    url: SecretUrl,
}

impl ProxyEndpoint {
    pub fn new(scheme: ProxyScheme, url: impl Into<String>) -> Self {
        Self {
            scheme,
            url: SecretUrl::new(url),
        }
    }

    pub fn scheme(&self) -> ProxyScheme {
        self.scheme
    }

    /// The raw URL for the transport and the child environment.
    pub fn expose(&self) -> &str {
        self.url.expose()
    }
}

impl fmt::Debug for ProxyEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProxyEndpoint")
            .field("scheme", &self.scheme)
            .field("url", &self.url)
            .finish()
    }
}

/// A fully resolved proxy plan. `Debug` reports only shape, never values.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct ProxyPlan {
    endpoints: Vec<ProxyEndpoint>,
    no_proxy: Vec<String>,
    pac: Option<SecretUrl>,
    /// Proxy variables OCG cannot interpret (SOCKS) that the user explicitly
    /// configured. They are preserved verbatim for child processes so an
    /// existing SOCKS setup keeps working, and are never used by OCG's own
    /// client. Values are [`SecretUrl`], so they never render.
    passthrough: Vec<(&'static str, SecretUrl)>,
}

impl ProxyPlan {
    pub fn endpoints(&self) -> &[ProxyEndpoint] {
        &self.endpoints
    }

    /// Safe exception hosts parsed from `NO_PROXY` or scutil `ExceptionsList`.
    pub fn no_proxy(&self) -> &[String] {
        &self.no_proxy
    }

    /// A PAC URL was detected. It is reported, never interpreted.
    pub fn has_pac(&self) -> bool {
        self.pac.is_some()
    }

    /// Unsupported proxy variables preserved for child processes.
    pub fn passthrough(&self) -> &[(&'static str, SecretUrl)] {
        &self.passthrough
    }

    pub fn is_empty(&self) -> bool {
        self.endpoints.is_empty()
            && self.no_proxy.is_empty()
            && self.pac.is_none()
            && self.passthrough.is_empty()
    }

    /// The endpoint the transport would use for a URL with this scheme, if any.
    /// An untyped `All` endpoint matches any scheme.
    pub fn endpoint_for(&self, scheme: &str) -> Option<&ProxyEndpoint> {
        let wanted = match scheme {
            "http" => ProxyScheme::Http,
            "https" => ProxyScheme::Https,
            _ => return None,
        };
        self.endpoints
            .iter()
            .find(|endpoint| endpoint.scheme() == wanted)
            .or_else(|| {
                self.endpoints
                    .iter()
                    .find(|endpoint| endpoint.scheme() == ProxyScheme::All)
            })
    }

    /// Whether `host` is covered by the resolved exception list (`NO_PROXY` or
    /// the static system exceptions). Matching is the minimal safe subset of
    /// `NO_PROXY` semantics; see [`no_proxy_matches`].
    pub fn matches_no_proxy(&self, host: &str) -> bool {
        no_proxy_matches(&self.no_proxy, host)
    }
}

/// Minimal `NO_PROXY` matcher. Recognizes `*` (everything), a leading `.`
/// suffix (`example.com` and its subdomains), a leading `*.` wildcard
/// (`example.com` and its subdomains), and an exact host. A `:port` on either
/// side is stripped before comparison. CIDR entries only ever match an exact
/// host string; they are never interpreted as networks.
pub fn no_proxy_matches(entries: &[String], host: &str) -> bool {
    let host = strip_no_proxy_port(host.trim().to_ascii_lowercase());
    if host.is_empty() {
        return false;
    }
    entries.iter().any(|entry| {
        let entry = strip_no_proxy_port(entry.trim().to_ascii_lowercase());
        exception_entry_matches(&entry, &host)
    })
}

fn exception_entry_matches(entry: &str, host: &str) -> bool {
    if entry == "*" {
        return true;
    }
    if let Some(suffix) = entry.strip_prefix("*.").or_else(|| entry.strip_prefix('.')) {
        return host == suffix || host.ends_with(&format!(".{suffix}"));
    }
    entry == host
}

/// Drop a trailing `:port` from a host or exception entry. Bracketed IPv6
/// literals keep their brackets and drop the port; anything malformed is
/// returned unchanged rather than rejected.
fn strip_no_proxy_port(mut value: String) -> String {
    if value.starts_with('[') {
        if let Some(end) = value.find(']') {
            value.truncate(end + 1);
        }
        return value;
    }
    if value.matches(':').count() == 1 {
        if let Some((host, _)) = value.split_once(':') {
            return host.to_string();
        }
    }
    value
}

impl fmt::Debug for ProxyPlan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProxyPlan")
            .field("endpoints", &self.endpoints.len())
            .field("no_proxy", &self.no_proxy.len())
            .field("pac", &self.pac.is_some())
            .field("passthrough", &self.passthrough.len())
            .finish()
    }
}

/// Where the effective proxy came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProxySource {
    CliDisabled,
    EnvDisabled,
    Environment,
    System,
    Direct,
}

/// One deterministic proxy decision, plus non-fatal notes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxySelection {
    source: ProxySource,
    plan: ProxyPlan,
    warnings: Vec<String>,
}

impl ProxySelection {
    pub fn source(&self) -> ProxySource {
        self.source
    }

    pub fn plan(&self) -> &ProxyPlan {
        &self.plan
    }

    pub fn warnings(&self) -> &[String] {
        &self.warnings
    }

    /// True when the user explicitly disabled proxy use.
    pub fn is_disabled(&self) -> bool {
        matches!(
            self.source,
            ProxySource::CliDisabled | ProxySource::EnvDisabled
        )
    }

    /// The exact environment a child process must receive.
    ///
    /// Every recognized spelling is cleared first, then the resolved endpoints
    /// are exported under both the upper- and lower-case names. Clients differ
    /// in which spelling they read, so exporting both keeps the resolved
    /// policy authoritative for either convention. Disabling the proxy exports
    /// nothing and still clears all eight spellings.
    pub fn child_env(&self) -> ChildProxyEnv {
        let mut env = ChildProxyEnv::default();
        env.remove.extend(PROXY_ENV_VARS);
        for endpoint in &self.plan.endpoints {
            let names: [&'static str; 2] = match endpoint.scheme {
                ProxyScheme::Http => ["HTTP_PROXY", "http_proxy"],
                ProxyScheme::Https => ["HTTPS_PROXY", "https_proxy"],
                ProxyScheme::All => ["ALL_PROXY", "all_proxy"],
            };
            for name in names {
                env.set.push((name, endpoint.expose().to_string()));
            }
        }
        if !self.plan.no_proxy.is_empty() {
            let exceptions = self.plan.no_proxy.join(",");
            env.set.push(("NO_PROXY", exceptions.clone()));
            env.set.push(("no_proxy", exceptions));
        }
        // Unsupported (SOCKS) values the user configured are restored verbatim
        // under their original name so an existing setup is not broken.
        for (name, value) in &self.plan.passthrough {
            env.set.push((name, value.expose().to_string()));
        }
        env
    }
}

/// Resolve the effective proxy from the injected environment and static
/// provider. Never touches the network and never reads the host directly.
pub fn resolve(
    cli_disable: bool,
    env: &dyn ProxyEnv,
    static_proxy: &dyn StaticProxyProvider,
) -> ProxySelection {
    if cli_disable {
        return ProxySelection {
            source: ProxySource::CliDisabled,
            plan: ProxyPlan::default(),
            warnings: Vec::new(),
        };
    }

    if let Some(raw) = env.var(DISABLE_ENV) {
        if env_truthy(&raw) {
            return ProxySelection {
                source: ProxySource::EnvDisabled,
                plan: ProxyPlan::default(),
                warnings: Vec::new(),
            };
        }
    }

    if PROXY_ENV_VARS.iter().any(|name| env.var(name).is_some()) {
        let (plan, warnings) = plan_from_env(env);
        return ProxySelection {
            source: ProxySource::Environment,
            plan,
            warnings,
        };
    }

    if let Some(raw) = static_proxy.raw_scutil() {
        let parsed = parse_scutil_output(&raw);
        let mut warnings = Vec::new();
        if parsed.pac.is_some() {
            warnings.push(
                "the system proxy is configured with a PAC file; OCG does not interpret PAC"
                    .to_string(),
            );
        }
        if !parsed.endpoints.is_empty() || parsed.pac.is_some() {
            return ProxySelection {
                source: ProxySource::System,
                plan: ProxyPlan {
                    endpoints: parsed.endpoints,
                    no_proxy: parsed.no_proxy,
                    pac: parsed.pac,
                    passthrough: Vec::new(),
                },
                warnings,
            };
        }
    }

    ProxySelection {
        source: ProxySource::Direct,
        plan: ProxyPlan::default(),
        warnings: Vec::new(),
    }
}

/// Truthy values used by `OCG_DISABLE_PROXY`.
pub fn env_truthy(raw: &str) -> bool {
    matches!(
        raw.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "on" | "yes" | "enabled"
    )
}

fn plan_from_env(env: &dyn ProxyEnv) -> (ProxyPlan, Vec<String>) {
    let mut plan = ProxyPlan::default();
    let mut warnings = Vec::new();
    for (scheme, upper, lower) in [
        (ProxyScheme::Http, "HTTP_PROXY", "http_proxy"),
        (ProxyScheme::Https, "HTTPS_PROXY", "https_proxy"),
        (ProxyScheme::All, "ALL_PROXY", "all_proxy"),
    ] {
        let (name, raw) = match env.var(upper).map(|value| (upper, value)) {
            Some(found) => found,
            None => match env.var(lower).map(|value| (lower, value)) {
                Some(found) => found,
                None => continue,
            },
        };
        match parse_proxy_url(&raw) {
            Some(url) => plan.endpoints.push(ProxyEndpoint::new(scheme, url)),
            None => {
                // A SOCKS value is a real, working configuration that OCG's
                // transport cannot speak. Preserve it for child processes
                // rather than silently removing a proxy that used to work.
                if is_socks_scheme(&raw) {
                    warnings.push(format!(
                        "{upper} uses a SOCKS scheme that OCG does not interpret; it is preserved for child processes"
                    ));
                    plan.passthrough.push((name, SecretUrl::new(raw)));
                } else {
                    warnings.push(format!("ignoring an unusable {upper} value"));
                }
            }
        }
    }
    if let Some(raw) = env.var("NO_PROXY").or_else(|| env.var("no_proxy")) {
        plan.no_proxy = parse_no_proxy(&raw);
    }
    (plan, warnings)
}

/// A SOCKS-family proxy scheme. Recognized so it can be preserved verbatim for
/// child processes; deliberately not implemented for OCG's own HTTP client.
fn is_socks_scheme(raw: &str) -> bool {
    match raw.trim().split_once("://") {
        Some((scheme, rest)) => {
            !rest.is_empty()
                && matches!(
                    scheme.to_ascii_lowercase().as_str(),
                    "socks" | "socks4" | "socks4a" | "socks5" | "socks5h"
                )
        }
        None => false,
    }
}

/// Validate a proxy URL without printing it. Only `http` and `https` proxy
/// schemes are accepted; anything else (including SOCKS) is ignored rather
/// than passed to the transport.
fn parse_proxy_url(raw: &str) -> Option<String> {
    let raw = raw.trim();
    if raw.is_empty() || raw.chars().any(|c| c.is_control() || c.is_whitespace()) {
        return None;
    }
    let (scheme, rest) = raw.split_once("://")?;
    if !matches!(scheme.to_ascii_lowercase().as_str(), "http" | "https") {
        return None;
    }
    if rest.is_empty() || rest.starts_with('/') {
        return None;
    }
    Some(raw.to_string())
}

fn parse_no_proxy(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|entry| is_safe_exception(entry))
        .map(str::to_string)
        .collect()
}

/// Accept host / domain / CIDR / wildcard exception entries only.
fn is_safe_exception(entry: &str) -> bool {
    !entry.is_empty()
        && entry.chars().all(|c| {
            c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_' | '*' | ':' | '/' | '[' | ']')
        })
}

/// The parsed parts of a static macOS proxy configuration.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StaticProxyConfig {
    pub endpoints: Vec<ProxyEndpoint>,
    pub no_proxy: Vec<String>,
    pub pac: Option<SecretUrl>,
}

/// Parse `scutil --proxy` output. Malformed lines are ignored, partial output
/// is accepted, and PAC is detected but never interpreted.
pub fn parse_scutil_output(text: &str) -> StaticProxyConfig {
    let mut fields: Vec<(String, String)> = Vec::new();
    let mut exceptions: Vec<String> = Vec::new();
    let mut in_exceptions = false;

    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if in_exceptions {
            if line == "}" {
                in_exceptions = false;
                continue;
            }
            if let Some((_, value)) = line.split_once(':') {
                let value = value.trim();
                if is_safe_exception(value) {
                    exceptions.push(value.to_string());
                }
            }
            continue;
        }
        if line.starts_with("ExceptionsList") && line.contains("<array>") {
            in_exceptions = true;
            continue;
        }
        if let Some((key, value)) = line.split_once(':') {
            let key = key.trim();
            let value = value.trim();
            if key.is_empty() || value.is_empty() {
                continue;
            }
            fields.push((key.to_string(), value.to_string()));
        }
    }

    let field = |name: &str| -> Option<&str> {
        fields
            .iter()
            .rev()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    };

    let mut config = StaticProxyConfig {
        no_proxy: exceptions,
        ..StaticProxyConfig::default()
    };

    if field("ProxyAutoConfigEnable")
        .map(scutil_enabled)
        .unwrap_or(false)
    {
        config.pac = field("ProxyAutoConfigURLString").map(|raw| SecretUrl::new(raw.to_string()));
    }

    for (scheme, enable, host, port) in [
        (ProxyScheme::Http, "HTTPEnable", "HTTPProxy", "HTTPPort"),
        (ProxyScheme::Https, "HTTPSEnable", "HTTPSProxy", "HTTPSPort"),
    ] {
        // `scutil --proxy` can retain stale host/port keys while the proxy is
        // disabled. Require the corresponding enable flag instead of treating
        // a partial pair as active.
        let enabled = field(enable).map(scutil_enabled).unwrap_or(false);
        if !enabled {
            continue;
        }
        let (Some(host), Some(port)) = (field(host), field(port)) else {
            continue;
        };
        if !is_safe_exception(host) || host.contains('/') || host.contains('@') {
            continue;
        }
        let Ok(port) = port.parse::<u16>() else {
            continue;
        };
        if port == 0 {
            continue;
        }
        config
            .endpoints
            .push(ProxyEndpoint::new(scheme, format!("http://{host}:{port}")));
    }

    config
}

fn scutil_enabled(value: &str) -> bool {
    matches!(value.trim(), "1" | "true" | "TRUE" | "yes" | "YES")
}

/// The ordered operations the HTTP client builder must perform. The first
/// operation always disables hidden automatic proxy discovery; the rest
/// install only resolved, typed endpoints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProxyBuilderOp {
    DisableAutomaticDiscovery,
    Install(ProxyScheme),
}

/// Pure builder plan so the ordering is inspectable in tests without a live
/// network or a client instance.
pub fn proxy_builder_ops(plan: &ProxyPlan) -> Vec<ProxyBuilderOp> {
    let mut ops = vec![ProxyBuilderOp::DisableAutomaticDiscovery];
    ops.extend(
        plan.endpoints
            .iter()
            .map(|endpoint| ProxyBuilderOp::Install(endpoint.scheme)),
    );
    ops
}

/// Exactly what a child process must receive. `Debug` reports counts only.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct ChildProxyEnv {
    remove: Vec<&'static str>,
    set: Vec<(&'static str, String)>,
}

impl ChildProxyEnv {
    /// Variables to remove before the child starts.
    pub fn removals(&self) -> &[&'static str] {
        &self.remove
    }

    /// Variables to set, in order.
    pub fn assignments(&self) -> &[(&'static str, String)] {
        &self.set
    }

    /// Apply the policy to a command. Removal happens before assignment so the
    /// resolved value always wins.
    pub fn apply(&self, command: &mut Command) {
        for name in &self.remove {
            command.env_remove(name);
        }
        for (name, value) in &self.set {
            command.env(name, value);
        }
    }
}

impl fmt::Debug for ChildProxyEnv {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChildProxyEnv")
            .field("remove", &self.remove.len())
            .field("set", &self.set.len())
            .finish()
    }
}
