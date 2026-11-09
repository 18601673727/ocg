//! The approval of one manually invoked external exchange: one explicitly
//! named, bounded, tool-free Job sent to one operator-approved HTTPS endpoint
//! with one operator-approved credential reference.
//!
//! Two authorities use it, each with its own control and its own entry point:
//! the external trial ([`crate::system::external_trial`], under
//! [`ExecutionControl::ManualExternalTrial`]) and ordinary manual external
//! execution ([`crate::system::manual_external`], under
//! [`ExecutionControl::ManualExternalExecution`]). An approval names the
//! authority that formed it; the authority's control is asked again by the
//! shared runner below, by the endpoint policy of the exchange and by the
//! Provider client itself, so neither control can carry the other's approval
//! to the network. Neither is manual loopback execution, whose endpoint guard
//! this leaves untouched, and neither is general external transport,
//! discovery, autonomous scheduling or recovery, which stay disabled.
//!
//! An approval names, for this one invocation: the endpoint, the credential
//! reference, optionally (the trial) or necessarily (ordinary execution) the
//! Model, the route and the limits. They are assertions against the Job's
//! frozen configuration, never overrides: the Job must already name exactly
//! the approved endpoint, credential and Model, carry no secret header, and
//! bound its own output, or the exchange is refused before anything is
//! reserved.
//!
//! Every deterministic refusal happens before the reservation. The exact
//! request is built without an Allocation (a read-only preview), the route is
//! checked, and the request is measured against the approved limits, so a
//! limit that is too small, a destination or proxy that is not usable, or a
//! request the approval does not cover leaves the submitted Job untouched. The
//! same facts are checked again on the exact prepared request inside the
//! start, against the real reservation, so a Job, credential or configuration
//! that changed in between still fails closed.
//!
//! The connection is direct (trial only) or through one named trusted
//! loopback HTTP proxy. Either way the ambient proxy (environment or system)
//! is never consulted. Direct: OCG resolves the name, checks every address,
//! and connects to exactly those. Through the named proxy: OCG does not look
//! the name up at all; the proxy resolves it and chooses the route, which OCG
//! cannot validate, and there is no fallback to a direct connection.
//!
//! There is one dispatch attempt. The single-use handoff is consumed by one
//! exchange; there is no retry, and an exchange that may have reached the
//! Provider is never settled as unsent. Stopping the local transport proves
//! nothing about the Provider: such an exchange leaves the Allocation held as
//! Uncertain.

use std::time::Duration;

use crate::db::entities::{FrozenExecutionConfig, LlmTransport, MutableFrozenConfiguration};
use crate::db::{DatabaseHandle, DevelopmentPreviewError};
use crate::service::ai::providers::{
    ApprovedRoute, CancellationToken, DestinationPolicy, ExternalAuthority, LoopbackProxy,
    ProviderExecutionConfig,
};
use crate::system::execution_controls::ExecutionControl;
use crate::system::manual_execution::{
    ExecutionReport, Stop, Target, checked_start, read_submitted_job, report,
    require_free_executor, reserve_named_job, within_deadline,
};

/// Hard ceilings, whatever the operator asks for.
pub(crate) const MAX_REQUEST_BYTES_CEILING: usize = 1 << 20;
pub(crate) const MAX_OUTPUT_TOKENS_CEILING: u32 = 4096;
pub(crate) const MAX_TIMEOUT_CEILING: Duration = Duration::from_secs(600);
/// The response bytes an exchange reads before it gives up.
const MAX_RESPONSE_BYTES: usize = 1 << 20;

/// Why an approval cannot be formed or an exchange was refused. `code` is
/// stable.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{code}: {detail}")]
pub(crate) struct ApprovalRefusal {
    pub(crate) code: &'static str,
    pub(crate) detail: String,
}

pub(crate) fn refuse(code: &'static str, detail: impl Into<String>) -> ApprovalRefusal {
    ApprovalRefusal {
        code,
        detail: detail.into(),
    }
}

impl From<ApprovalRefusal> for Stop {
    fn from(refusal: ApprovalRefusal) -> Self {
        Stop::Refused {
            code: refusal.code,
            detail: refusal.detail,
        }
    }
}

/// An HTTPS endpoint that may be approved: a public DNS name or public
/// address literal, an explicit or default port, no credentials, query or
/// fragment, and a plain path. Two spellings of the same endpoint are equal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ApprovedEndpoint {
    /// Lower-case host without brackets.
    host: String,
    port: u16,
    request_url: String,
}

impl ApprovedEndpoint {
    pub(crate) fn parse(text: &str) -> Result<Self, ApprovalRefusal> {
        let invalid = |detail: &str| refuse("endpoint_not_approvable", detail);
        if text.is_empty()
            || text.len() > 2048
            || !text.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
        {
            return Err(invalid(
                "the endpoint must be 1..=2048 visible ASCII characters",
            ));
        }
        let uri: ntex::http::Uri = text
            .parse()
            .map_err(|_| invalid("the endpoint is not a valid URL"))?;
        if uri.scheme_str() != Some("https") {
            return Err(invalid("the endpoint must use https"));
        }
        let Some(host) = uri.host().filter(|host| !host.is_empty()) else {
            return Err(invalid("the endpoint needs a host"));
        };
        if uri
            .authority()
            .is_none_or(|authority| authority.as_str().contains('@'))
            || uri.query().is_some()
            || text.contains('#')
        {
            return Err(invalid(
                "the endpoint must carry no credentials, query or fragment",
            ));
        }
        if !crate::system::http::host_may_be_external(host) {
            return Err(invalid(
                "the host must be a public DNS name or a public address, not localhost, an intranet name or a private address",
            ));
        }
        let port = uri.port_u16().unwrap_or(443);
        if port == 0 {
            return Err(invalid("the port must not be 0"));
        }
        let path = uri.path().trim_end_matches('/');
        let path = path.strip_suffix("/chat/completions").unwrap_or(path);
        // The request URL is assembled from the frozen endpoint by
        // `providers::endpoint`, which strips exactly one of these resource
        // names. A path that still ends in another would be rewritten there,
        // so the URL sent would not be the URL approved.
        if [
            "/responses",
            "/messages",
            "/systemone",
            "/models",
            "/chat/completions",
        ]
        .iter()
        .any(|resource| path.ends_with(resource))
        {
            return Err(invalid(
                "the endpoint path must be the API base or end in /chat/completions once",
            ));
        }
        if !path
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._~/-".contains(&byte))
            || path
                .split('/')
                .skip(1)
                .any(|segment| matches!(segment, "" | "." | ".."))
        {
            return Err(invalid("the endpoint path must be plain and normalized"));
        }
        let host = crate::system::http::bare_host(host);
        // An IPv6 literal is bracketed in a URL.
        let host_text = if host.contains(':') {
            format!("[{host}]")
        } else {
            host.clone()
        };
        let authority = if port == 443 {
            host_text
        } else {
            format!("{host_text}:{port}")
        };
        Ok(Self {
            request_url: format!("https://{authority}{path}/chat/completions"),
            host,
            port,
        })
    }

    pub(crate) fn host(&self) -> &str {
        &self.host
    }

    pub(crate) fn port(&self) -> u16 {
        self.port
    }

    /// Whether a frozen endpoint names exactly this one.
    pub(crate) fn permits(&self, frozen: &str) -> bool {
        Self::parse(frozen).is_ok_and(|frozen| frozen == *self)
    }
}

/// What one exchange may spend. Every field is required of the operator and
/// capped by a ceiling.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ApprovalLimits {
    pub(crate) max_request_bytes: usize,
    pub(crate) max_output_tokens: u32,
    pub(crate) timeout: Duration,
}

impl ApprovalLimits {
    pub(crate) fn new(
        max_request_bytes: usize,
        max_output_tokens: u32,
        timeout: Duration,
    ) -> Result<Self, ApprovalRefusal> {
        if max_request_bytes == 0
            || max_request_bytes > MAX_REQUEST_BYTES_CEILING
            || max_output_tokens == 0
            || max_output_tokens > MAX_OUTPUT_TOKENS_CEILING
            || timeout.is_zero()
            || timeout > MAX_TIMEOUT_CEILING
        {
            return Err(refuse(
                "limit_out_of_range",
                format!(
                    "request bytes 1..={MAX_REQUEST_BYTES_CEILING}, output tokens 1..={MAX_OUTPUT_TOKENS_CEILING} and timeout 1..={} s are required",
                    MAX_TIMEOUT_CEILING.as_secs()
                ),
            ));
        }
        Ok(Self {
            max_request_bytes,
            max_output_tokens,
            timeout,
        })
    }
}

/// Parses the loopback proxy an operator names: `http://127.0.0.1:<port>` or
/// `http://[::1]:<port>`, nothing else. A name such as `localhost`, another
/// scheme, a remote or non-loopback address, credentials, a query, a fragment,
/// a path beyond `/` and a missing or zero port are all refused.
pub(crate) fn parse_trusted_proxy(text: &str) -> Result<LoopbackProxy, ApprovalRefusal> {
    let invalid = |detail: &str| refuse("proxy_not_approvable", detail);
    if text.is_empty() || text.len() > 64 || !text.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
    {
        return Err(invalid("the proxy must be 1..=64 visible ASCII characters"));
    }
    let Some(rest) = text.strip_prefix("http://") else {
        return Err(invalid(
            "the proxy must be an http:// URL (HTTP CONNECT); other schemes, including socks5, are not supported",
        ));
    };
    let authority = rest.strip_suffix('/').unwrap_or(rest);
    if authority.contains(['@', '?', '#', '/']) {
        return Err(invalid(
            "the proxy URL must be a bare address and port: no credentials, path, query or fragment",
        ));
    }
    let address: std::net::SocketAddr = authority.parse().map_err(|_| {
        invalid("the proxy must be a literal loopback address with a port, such as http://127.0.0.1:6152")
    })?;
    if address.to_string() != authority {
        return Err(invalid("the proxy address must be written canonically"));
    }
    LoopbackProxy::new(address).ok_or_else(|| {
        invalid("only a literal loopback proxy address with a nonzero port is eligible")
    })
}

/// A Model id the operator may name: 1..=128 visible ASCII characters.
fn valid_model_id(model: &str) -> Result<(), ApprovalRefusal> {
    if model.is_empty()
        || model.len() > 128
        || !model.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
    {
        return Err(refuse(
            "model_invalid",
            "the model id must be 1..=128 visible ASCII characters",
        ));
    }
    Ok(())
}

/// Thinking rules of the Kimi Code `k3.1-pro` family: thinking cannot be
/// disabled, and the supported `reasoning_effort` values are low, high and
/// max. The documented default is not stated consistently, so the effort must
/// be named rather than left to the Provider. Other models carry no rule here.
fn check_thinking(model: &str, effort: Option<&str>) -> Result<(), ApprovalRefusal> {
    if !model.starts_with("k3.1-pro") {
        return Ok(());
    }
    match effort {
        Some("low" | "high" | "max") => Ok(()),
        _ => Err(refuse(
            "thinking_effort_not_supported",
            format!(
                "{model} requires thinking: freeze reasoning_effort as exactly low, high or max (none, minimal, medium, xhigh or no value are not accepted here)"
            ),
        )),
    }
}

/// The operator's approval for one invocation, under one authority.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ExternalApproval {
    authority: ExternalAuthority,
    endpoint: ApprovedEndpoint,
    credential_ref: String,
    limits: ApprovalLimits,
    route: ApprovedRoute,
    /// The one model id the Job may name. The trial may leave it open;
    /// ordinary manual execution always names it.
    model: Option<String>,
}

fn approved_credential_ref(credential_ref: &str) -> Result<String, ApprovalRefusal> {
    if !crate::db::redb::valid_credential_ref(credential_ref) {
        return Err(refuse(
            "credential_ref_invalid",
            "the credential reference must be 1..=128 characters without control characters",
        ));
    }
    Ok(credential_ref.to_owned())
}

impl ExternalApproval {
    /// An external-trial approval: direct unless
    /// [`Self::with_trusted_proxy`], any Model unless [`Self::with_model`].
    pub(crate) fn trial(
        endpoint: &str,
        credential_ref: &str,
        limits: ApprovalLimits,
    ) -> Result<Self, ApprovalRefusal> {
        let credential_ref = approved_credential_ref(credential_ref)?;
        Ok(Self {
            authority: ExternalAuthority::ManualExternalTrial,
            endpoint: ApprovedEndpoint::parse(endpoint)?,
            credential_ref,
            limits,
            route: ApprovedRoute::Direct,
            model: None,
        })
    }

    /// An ordinary manual external-execution approval. Every identity is
    /// named and the route is the named trusted proxy; there is no default
    /// and no direct route.
    pub(crate) fn manual_execution(
        endpoint: &str,
        credential_ref: &str,
        model: &str,
        proxy: LoopbackProxy,
        limits: ApprovalLimits,
    ) -> Result<Self, ApprovalRefusal> {
        let credential_ref = approved_credential_ref(credential_ref)?;
        valid_model_id(model)?;
        Ok(Self {
            authority: ExternalAuthority::ManualExternalExecution,
            endpoint: ApprovedEndpoint::parse(endpoint)?,
            credential_ref,
            limits,
            route: ApprovedRoute::TrustedProxy(proxy),
            model: Some(model.to_owned()),
        })
    }

    /// Connects through the named loopback proxy instead of directly.
    pub(crate) fn with_trusted_proxy(mut self, proxy: LoopbackProxy) -> Self {
        self.route = ApprovedRoute::TrustedProxy(proxy);
        self
    }

    /// Requires the Job's frozen model to be exactly `model`.
    pub(crate) fn with_model(mut self, model: &str) -> Result<Self, ApprovalRefusal> {
        valid_model_id(model)?;
        self.model = Some(model.to_owned());
        Ok(self)
    }

    pub(crate) fn authority(&self) -> ExternalAuthority {
        self.authority
    }

    pub(crate) fn route(&self) -> &ApprovedRoute {
        &self.route
    }

    /// The approved destination as `host:port`, for reports.
    pub(crate) fn destination_text(&self) -> String {
        if self.endpoint.host().contains(':') {
            format!("[{}]:{}", self.endpoint.host(), self.endpoint.port())
        } else {
            format!("{}:{}", self.endpoint.host(), self.endpoint.port())
        }
    }

    pub(crate) fn limits(&self) -> ApprovalLimits {
        self.limits
    }

    pub(crate) fn destination(&self) -> DestinationPolicy {
        DestinationPolicy::ApprovedHttps {
            host: self.endpoint.host().to_owned(),
            port: self.endpoint.port(),
            route: self.route.clone(),
            authority: self.authority,
        }
    }

    /// Whether this approval may act at all: its authority's control is
    /// enabled and the authority admits its route.
    fn authorized(&self) -> Result<(), ApprovalRefusal> {
        let control = self.authority.control();
        if !control.is_enabled() {
            return Err(refuse("control_disabled", control.refusal()));
        }
        if !self.authority.admits_route(&self.route) {
            return Err(refuse(
                "proxy_required",
                format!(
                    "{} connects only through a named trusted HTTP proxy",
                    self.authority.name()
                ),
            ));
        }
        Ok(())
    }

    /// Whether `endpoint`, as frozen into a Job, is the approved one, and the
    /// approval's authority may act.
    pub(crate) fn permits_endpoint(&self, endpoint: &str) -> bool {
        self.authorized().is_ok() && self.endpoint.permits(endpoint)
    }

    /// The provider configuration of the one exchange: the approved
    /// destination only, the approved per-chunk timeout, a bounded response.
    pub(crate) fn provider_config(
        &self,
        cancellation: CancellationToken,
    ) -> ProviderExecutionConfig {
        ProviderExecutionConfig {
            timeout: self.limits.timeout,
            first_delta_timeout: Some(self.limits.timeout),
            max_output_bytes: MAX_RESPONSE_BYTES,
            cancellation,
            destination: self.destination(),
            ..ProviderExecutionConfig::default()
        }
    }

    /// Whether the approved route is usable from here, without sending
    /// anything to the destination. Direct: the approved name resolves to
    /// public addresses only (a lookup, not a connection; the connector
    /// resolves and checks again and connects to exactly the addresses it
    /// checked). Trusted proxy: something accepts connections at the proxy
    /// address; the Provider name is not looked up and no CONNECT is sent.
    pub(crate) async fn check_destination(&self) -> Result<(), ApprovalRefusal> {
        use crate::system::http::{DestinationFault, check_direct_destination};
        match &self.route {
            ApprovedRoute::TrustedProxy(proxy) => probe_proxy(*proxy).await,
            ApprovedRoute::Direct => {
                match check_direct_destination(self.endpoint.host(), self.endpoint.port()).await {
                    Ok(_) => Ok(()),
                    Err(DestinationFault::Unresolvable) => Err(refuse(
                        "destination_unresolvable",
                        "the approved host does not resolve from this machine; nothing was reserved or sent",
                    )),
                    Err(DestinationFault::NonPublic) => Err(refuse(
                        "destination_not_public",
                        "the approved host resolves to an address that is not public; nothing was reserved or sent",
                    )),
                }
            }
        }
    }

    /// What the Job's frozen configuration must be, checked before anything
    /// is reserved.
    pub(crate) fn check_frozen(
        &self,
        frozen: &MutableFrozenConfiguration,
    ) -> Result<(), ApprovalRefusal> {
        match frozen {
            MutableFrozenConfiguration::Generative(configuration) => {
                self.check_generative(configuration)
            }
            MutableFrozenConfiguration::Decision(_) => Err(refuse(
                "decision_not_supported",
                "one Generative request is run here; a Decision has no bound on its output",
            )),
        }
    }

    fn check_generative(&self, frozen: &FrozenExecutionConfig) -> Result<(), ApprovalRefusal> {
        let provider = &frozen.llm.provider;
        if provider.transport != LlmTransport::OpenAiChatCompletions {
            return Err(refuse(
                "transport_not_supported",
                "only the OpenAI chat completions transport is supported",
            ));
        }
        match provider.endpoint.as_deref() {
            Some(endpoint) if self.endpoint.permits(endpoint) => {}
            _ => {
                return Err(refuse(
                    "endpoint_not_approved",
                    "the Job's frozen endpoint is not the endpoint approved for this invocation",
                ));
            }
        }
        if provider.credential_ref.as_deref() != Some(self.credential_ref.as_str()) {
            return Err(refuse(
                "credential_not_approved",
                "the Job's frozen credential reference is not the one approved for this invocation",
            ));
        }
        if provider.headers.secret_refs().next().is_some() {
            return Err(refuse(
                "secret_header_not_approved",
                "a custom header carries a secret; only the approved credential may leave the process",
            ));
        }
        let model = frozen.llm.config.model.as_str();
        if self
            .model
            .as_deref()
            .is_some_and(|approved| approved != model)
        {
            return Err(refuse(
                "model_not_approved",
                "the Job's frozen model is not the model approved for this invocation",
            ));
        }
        if let Some(reason) = frozen.single_request_refusal() {
            return Err(refuse("not_single_request_generative", reason));
        }
        let parameters = &frozen.llm.config.parameters;
        check_thinking(model, parameters.reasoning_effort.as_deref())?;
        let bounds: Vec<u32> = [parameters.max_tokens, parameters.max_completion_tokens]
            .into_iter()
            .flatten()
            .collect();
        if bounds.is_empty()
            || bounds
                .iter()
                .any(|bound| *bound > self.limits.max_output_tokens)
        {
            return Err(refuse(
                "output_bound_missing_or_too_large",
                format!(
                    "the Job must freeze max_tokens or max_completion_tokens, none above {}",
                    self.limits.max_output_tokens
                ),
            ));
        }
        Ok(())
    }

    /// The same facts on the exact prepared request, before the start is
    /// submitted: the frozen configuration, the credentials the handoff holds
    /// (only the approved one), the request's size and its own output bound.
    pub(crate) fn check_prepared(
        &self,
        prepared: &crate::db::PreparedDevelopmentHandoff,
    ) -> Result<(), ApprovalRefusal> {
        let Some(frozen) = prepared.generative_configuration() else {
            return Err(refuse(
                "decision_not_supported",
                "a Generative Job is required",
            ));
        };
        self.check_generative(frozen)?;
        if prepared.credential_references() != [self.credential_ref.as_str()] {
            return Err(refuse(
                "credential_not_approved",
                "the handoff would carry a credential other than the approved one",
            ));
        }
        let request = prepared.request();
        let size = serde_json::to_vec(request).map_or(usize::MAX, |bytes| bytes.len());
        if size > self.limits.max_request_bytes {
            return Err(refuse(
                "request_too_large",
                format!(
                    "the request is {size} bytes; at most {} are approved",
                    self.limits.max_request_bytes
                ),
            ));
        }
        let model = request.get("model").and_then(serde_json::Value::as_str);
        let effort = request
            .get("reasoning_effort")
            .and_then(serde_json::Value::as_str);
        if request.get("thinking").is_some()
            || model.is_none_or(|model| check_thinking(model, effort).is_err())
            || self
                .model
                .as_deref()
                .is_some_and(|approved| model != Some(approved))
        {
            return Err(refuse(
                "request_not_bounded",
                "the prepared request must name the approved model and, for a thinking-only model, a supported reasoning_effort, with no thinking override",
            ));
        }
        let bound = |name: &str| request.get(name).and_then(serde_json::Value::as_u64);
        let bounded = [bound("max_tokens"), bound("max_completion_tokens")];
        if bounded.iter().all(Option::is_none)
            || bounded
                .iter()
                .flatten()
                .any(|bound| *bound > u64::from(self.limits.max_output_tokens))
            || request.get("tools").is_some()
            || request.get("tool_choice").is_some()
            || request.get("stream") != Some(&serde_json::Value::Bool(true))
        {
            return Err(refuse(
                "request_not_bounded",
                "the prepared request must stream, offer no tools, and bound its output",
            ));
        }
        Ok(())
    }
}

/// Runs the one approved exchange of `target`. Everything before the
/// reservation is a read; the reservation, the checked start and the exchange
/// each happen at most once. `cancellation` is the operator's stop; the
/// approved timeout cancels it too. Neither is a claim about the Provider.
///
/// Reached only through the entry point of the approval's authority, which
/// has already asked its own control; the control is asked again here.
pub(crate) async fn run_approved_external(
    database: &DatabaseHandle,
    target: Target,
    approval: &ExternalApproval,
    cancellation: CancellationToken,
) -> Result<ExecutionReport, Stop> {
    use crate::system::executor::execution_runtime::{
        EndpointPolicy, supervise_development_generative,
    };
    approval.authorized()?;

    // 1. Reads only: the Job, its frozen configuration and its standing.
    let record = read_submitted_job(database, target.job).await?;
    if let Some(frozen) = record.job.frozen.as_ref() {
        approval.check_frozen(frozen)?;
    }
    require_free_executor(database, target.executor).await?;

    // 1b. Everything else that is predictable, before anything is reserved:
    // the route, and the exact request built without an Allocation. A
    // refusal here changes nothing; the Job stays submitted.
    approval.check_destination().await?;
    let refusal = std::cell::RefCell::new(None);
    match database
        .preview_development_handoff(target.job, target.pool, target.executor, |prepared| {
            approval.check_prepared(prepared).map_err(|refusal_found| {
                let code = refusal_found.code;
                *refusal.borrow_mut() = Some(refusal_found);
                code
            })
        })
        .await
    {
        Ok(()) => {}
        Err(DevelopmentPreviewError::Refused(code)) => {
            return Err(refusal
                .into_inner()
                .unwrap_or_else(|| {
                    refuse(code, "the prepared request is not covered by the approval")
                })
                .into());
        }
        Err(DevelopmentPreviewError::Unavailable(error)) => {
            return Err(refuse(
                "preflight_unavailable",
                format!(
                    "the request could not be prepared; nothing was reserved or sent, and the Job is unchanged ({error})"
                ),
            )
            .into());
        }
    }

    // 2. One reservation, of this Job only.
    let allocation = reserve_named_job(database, target).await?;

    // 3. The checked start, then 4. the one exchange.
    let handoff = checked_start(database, allocation.token, |prepared| {
        approval
            .check_prepared(prepared)
            .map_err(|refusal| refusal.code)
    })
    .await?;
    let config = approval.provider_config(cancellation.clone());
    let exchange = within_deadline(
        &cancellation,
        approval.limits().timeout,
        supervise_development_generative(
            database,
            handoff,
            &config,
            EndpointPolicy::ApprovedExternal(approval),
        ),
    )
    .await;

    // 5. What the database now shows is the report.
    report(database, target.job, &exchange).await
}

/// A bare TCP connection to the loopback proxy, before anything is reserved.
/// It sends no byte and names no destination.
async fn probe_proxy(proxy: LoopbackProxy) -> Result<(), ApprovalRefusal> {
    let address = proxy.address();
    let reachable = ntex::rt::spawn_blocking(move || {
        std::net::TcpStream::connect_timeout(&address, Duration::from_secs(2)).is_ok()
    })
    .await
    .unwrap_or(false);
    if reachable {
        Ok(())
    } else {
        Err(refuse(
            "proxy_unreachable",
            format!(
                "nothing accepts connections at the trusted proxy {address}; nothing was reserved or sent"
            ),
        ))
    }
}

/// The control a caller names must be the one its approval was formed under.
pub(crate) fn require_authority(
    approval: &ExternalApproval,
    control: ExecutionControl,
) -> Result<(), ApprovalRefusal> {
    if approval.authority().control() != control {
        return Err(refuse(
            "authority_mismatch",
            format!(
                "the approval was formed under {}, not {}",
                approval.authority().name(),
                control.name()
            ),
        ));
    }
    Ok(())
}
