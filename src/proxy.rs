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

/// Supplies the raw `/usr/sbin/scutil --proxy` output. The real provider only
/// runs the command on macOS; tests inject fixed text.
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
    /// (curl and parts of the OpenCode runtime read the lower-case spelling,
    /// others only the upper-case one), so exporting both keeps the resolved
    /// policy authoritative for any convention. Disabling the proxy exports
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
                        "{upper} uses a SOCKS scheme that OCG does not interpret; it is preserved for child OpenCode processes"
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
/// children; deliberately not implemented for OCG's own HTTP client.
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

#[cfg(test)]
mod tests {
    use super::*;

    fn resolve_env(env: &MapProxyEnv) -> ProxySelection {
        resolve(false, env, &NoStaticProxy)
    }

    #[test]
    fn cli_disable_beats_every_source() {
        let env = MapProxyEnv::new()
            .with("HTTP_PROXY", "http://proxy-internal:8080")
            .with(DISABLE_ENV, "1");
        let selection = resolve(true, &env, &NoStaticProxy);
        assert_eq!(selection.source(), ProxySource::CliDisabled);
        assert!(selection.is_disabled());
        assert!(selection.plan().endpoints().is_empty());
    }

    #[test]
    fn env_truthy_disables_and_falsey_does_not() {
        for truthy in ["1", "true", "TRUE", " on ", "yes", "enabled"] {
            let env = MapProxyEnv::new().with(DISABLE_ENV, truthy);
            assert!(resolve_env(&env).is_disabled(), "{truthy} must disable");
        }
        for falsey in ["0", "false", "off", "", "maybe"] {
            let env = MapProxyEnv::new().with(DISABLE_ENV, falsey);
            assert!(
                !resolve_env(&env).is_disabled(),
                "{falsey} must not disable"
            );
        }
    }

    #[test]
    fn uppercase_and_lowercase_environment_are_both_read() {
        let upper = resolve_env(&MapProxyEnv::new().with("HTTPS_PROXY", "http://proxy-a:3128"));
        assert_eq!(upper.source(), ProxySource::Environment);
        assert_eq!(upper.plan().endpoints().len(), 1);

        let lower = resolve_env(
            &MapProxyEnv::new()
                .with("https_proxy", "http://proxy-b:3128")
                .with("no_proxy", "localhost, 169.254/16"),
        );
        assert_eq!(lower.source(), ProxySource::Environment);
        assert_eq!(lower.plan().endpoints().len(), 1);
        assert_eq!(lower.plan().no_proxy(), &["localhost", "169.254/16"]);
    }

    #[test]
    fn environment_beats_static_system_configuration() {
        let env = MapProxyEnv::new().with("HTTP_PROXY", "http://proxy-env:8080");
        let selection = resolve(
            false,
            &env,
            &FakeStatic("HTTPEnable : 1\nHTTPProxy : proxy-os\nHTTPPort : 9999\n"),
        );
        assert_eq!(selection.source(), ProxySource::Environment);
        assert_eq!(selection.plan().endpoints().len(), 1);
    }

    #[test]
    fn only_no_proxy_selects_the_environment_branch() {
        let env = MapProxyEnv::new().with("no_proxy", ".internal");
        let selection = resolve(
            false,
            &env,
            &FakeStatic("HTTPEnable : 1\nHTTPProxy : proxy-os\nHTTPPort : 9999\n"),
        );
        assert_eq!(selection.source(), ProxySource::Environment);
        assert!(selection.plan().endpoints().is_empty());
        assert_eq!(selection.plan().no_proxy(), &[".internal"]);
    }

    #[test]
    fn static_system_http_and_https_are_typed_and_exceptions_are_safe() {
        let selection = resolve(
            false,
            &MapProxyEnv::new(),
            &FakeStatic(
                "<dictionary> {\n  ExceptionsList : <array> {\n    0 : *.local\n    1 : 169.254/16\n  }\n  HTTPEnable : 1\n  HTTPPort : 3128\n  HTTPProxy : proxy-internal\n  HTTPSEnable : 1\n  HTTPSPort : 3129\n  HTTPSProxy : proxy-internal\n}\n",
            ),
        );
        assert_eq!(selection.source(), ProxySource::System);
        let schemes: Vec<ProxyScheme> = selection
            .plan()
            .endpoints()
            .iter()
            .map(ProxyEndpoint::scheme)
            .collect();
        assert_eq!(schemes, vec![ProxyScheme::Http, ProxyScheme::Https]);
        assert_eq!(selection.plan().no_proxy(), &["*.local", "169.254/16"]);
    }

    #[test]
    fn malformed_and_partial_scutil_is_ignored_safely() {
        let parsed = parse_scutil_output(
            "garbage line without colon\nHTTPEnable : 1\nHTTPProxy : proxy-a\nHTTPPort : not-a-port\nHTTPSProxy : proxy-b\nHTTPSPort : 0\n: : :\n",
        );
        assert!(parsed.endpoints.is_empty(), "invalid ports must be dropped");
        assert!(parsed.pac.is_none());
    }

    #[test]
    fn scutil_partial_configuration_requires_an_enable_flag() {
        let parsed = parse_scutil_output("HTTPProxy : proxy-a\nHTTPPort : 8080\n");
        assert!(parsed.endpoints.is_empty());

        let enabled = parse_scutil_output("HTTPEnable : 1\nHTTPProxy : proxy-a\nHTTPPort : 8080\n");
        assert_eq!(enabled.endpoints.len(), 1);
        assert_eq!(enabled.endpoints[0].scheme(), ProxyScheme::Http);
    }

    #[test]
    fn scutil_pac_is_detected_but_not_interpreted() {
        let selection = resolve(
            false,
            &MapProxyEnv::new(),
            &FakeStatic(
                "ProxyAutoConfigEnable : 1\nProxyAutoConfigURLString : http://wpad.invalid/proxy.pac\n",
            ),
        );
        assert_eq!(selection.source(), ProxySource::System);
        assert!(selection.plan().has_pac());
        assert!(selection.plan().endpoints().is_empty());
        assert!(!selection.warnings().is_empty());
        // The PAC URL never appears in a rendering of the plan.
        let rendered = format!("{:?}", selection.plan());
        assert!(!rendered.contains("wpad"));
    }

    #[test]
    fn unsupported_socks_values_are_preserved_for_children_without_leaking() {
        let selection = resolve_env(
            &MapProxyEnv::new().with("ALL_PROXY", "socks5h://alice:secret@proxy-internal:1080"),
        );
        assert_eq!(selection.source(), ProxySource::Environment);
        // OCG's own transport never installs a SOCKS endpoint.
        assert!(selection.plan().endpoints().is_empty());
        assert!(selection
            .warnings()
            .iter()
            .any(|w| w.contains("ALL_PROXY") && w.contains("preserved for child")));
        assert!(!format!("{:?}", selection).contains("proxy-internal"));
        assert!(!format!("{:?}", selection).contains("secret"));
        assert_eq!(selection.plan().passthrough().len(), 1);
        assert!(!format!("{:?}", selection.plan().passthrough()).contains("secret"));

        // The child keeps the user's working SOCKS value verbatim.
        let child = selection.child_env();
        let restored = child
            .assignments()
            .iter()
            .find(|(name, _)| *name == "ALL_PROXY")
            .map(|(_, value)| value.clone());
        assert_eq!(
            restored,
            Some("socks5h://alice:secret@proxy-internal:1080".to_string())
        );
        // ...but a disabled policy drops it entirely.
        let disabled = resolve(true, &MapProxyEnv::new(), &NoStaticProxy);
        assert!(disabled
            .child_env()
            .assignments()
            .iter()
            .all(|(name, _)| *name != "ALL_PROXY"));
    }

    #[test]
    fn malformed_proxy_values_are_not_preserved_for_children() {
        let selection = resolve_env(&MapProxyEnv::new().with("ALL_PROXY", "://not-a-proxy"));
        assert!(selection.plan().passthrough().is_empty());
        assert!(selection
            .child_env()
            .assignments()
            .iter()
            .all(|(name, _)| !name.contains("ALL_PROXY") && !name.contains("all_proxy")));
    }

    #[test]
    fn debug_and_display_never_render_credentials() {
        let env =
            MapProxyEnv::new().with("HTTPS_PROXY", "http://alice:wonderland@proxy-internal:3128");
        assert!(!format!("{env:?}").contains("wonderland"));
        let selection = resolve_env(&env);
        let endpoint = &selection.plan().endpoints()[0];
        let debug = format!("{endpoint:?}");
        let plan_debug = format!("{:?}", selection.plan());
        assert!(!debug.contains("wonderland"));
        assert!(!debug.contains("alice"));
        assert!(!plan_debug.contains("wonderland"));
        assert!(!format!("{}", SecretUrl::new("hunter2")).contains("hunter2"));
        assert!(!format!("{:?}", SecretUrl::new("hunter2")).contains("hunter2"));
    }

    #[test]
    fn disabled_child_env_strips_all_eight_forms() {
        let selection = resolve(true, &MapProxyEnv::new(), &NoStaticProxy);
        let child = selection.child_env();
        let mut removed: Vec<&str> = child.removals().to_vec();
        removed.sort_unstable();
        let mut expected = PROXY_ENV_VARS.to_vec();
        expected.sort_unstable();
        assert_eq!(removed, expected);
        assert!(child.assignments().is_empty());
    }

    #[test]
    fn enabled_child_env_propagates_canonical_names() {
        let selection = resolve_env(
            &MapProxyEnv::new()
                .with("https_proxy", "http://proxy-internal:3128")
                .with("no_proxy", ".internal,localhost"),
        );
        let child = selection.child_env();
        assert_eq!(child.removals(), PROXY_ENV_VARS);
        for name in ["HTTPS_PROXY", "https_proxy"] {
            let value = child
                .assignments()
                .iter()
                .find(|(assigned, _)| *assigned == name)
                .map(|(_, value)| value.clone());
            assert_eq!(
                value,
                Some("http://proxy-internal:3128".to_string()),
                "{name}"
            );
        }
        for name in ["NO_PROXY", "no_proxy"] {
            let value = child
                .assignments()
                .iter()
                .find(|(assigned, _)| *assigned == name)
                .map(|(_, value)| value.clone());
            assert_eq!(value, Some(".internal,localhost".to_string()), "{name}");
        }
        // Only the resolved schemes are exported.
        assert!(child
            .assignments()
            .iter()
            .all(|(name, _)| !name.contains("ALL_PROXY") && !name.contains("all_proxy")));
        assert!(!format!("{child:?}").contains("proxy-internal"));
    }

    #[test]
    fn builder_ops_disable_automatic_discovery_first() {
        let selection =
            resolve_env(&MapProxyEnv::new().with("HTTPS_PROXY", "http://proxy-internal:3128"));
        let ops = proxy_builder_ops(selection.plan());
        assert_eq!(ops[0], ProxyBuilderOp::DisableAutomaticDiscovery);
        assert_eq!(ops.len(), 2);
        assert_eq!(ops[1], ProxyBuilderOp::Install(ProxyScheme::Https));

        let direct = proxy_builder_ops(&ProxyPlan::default());
        assert_eq!(direct, vec![ProxyBuilderOp::DisableAutomaticDiscovery]);
    }

    #[derive(Clone)]
    struct FakeStatic(&'static str);

    impl StaticProxyProvider for FakeStatic {
        fn raw_scutil(&self) -> Option<String> {
            Some(self.0.to_string())
        }
    }
}
