use std::time::Duration;

use crate::db::entities::{FrozenExecutionConfig, LlmTransport};
use crate::service::ai::providers::{
    ApprovedRoute, CancellationToken, DestinationPolicy, ExternalAuthority, LoopbackProxy,
    ProviderExecutionConfig,
};

pub(crate) const MAX_REQUEST_BYTES_CEILING: usize = 1 << 20;
pub(crate) const MAX_OUTPUT_TOKENS_CEILING: u32 = 4096;
pub(crate) const MAX_TIMEOUT_CEILING: Duration = Duration::from_secs(600);
const MAX_RESPONSE_BYTES: usize = 1 << 20;

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

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ApprovedEndpoint {
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

    pub(crate) fn request_url(&self) -> &str {
        &self.request_url
    }

    pub(crate) fn port(&self) -> u16 {
        self.port
    }

    pub(crate) fn permits(&self, frozen: &str) -> bool {
        Self::parse(frozen).is_ok_and(|frozen| frozen == *self)
    }
}

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

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ExternalApproval {
    authority: ExternalAuthority,
    endpoint: ApprovedEndpoint,
    credential_ref: String,
    limits: ApprovalLimits,
    route: ApprovedRoute,
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
    pub(crate) fn scheduled_external(
        endpoint: &str,
        credential_ref: &str,
        model: &str,
        proxy: LoopbackProxy,
        limits: ApprovalLimits,
    ) -> Result<Self, ApprovalRefusal> {
        let credential_ref = approved_credential_ref(credential_ref)?;
        valid_model_id(model)?;
        Ok(Self {
            authority: ExternalAuthority::ScheduledExternalTransport,
            endpoint: ApprovedEndpoint::parse(endpoint)?,
            credential_ref,
            limits,
            route: ApprovedRoute::TrustedProxy(proxy),
            model: Some(model.to_owned()),
        })
    }

    pub(crate) fn limits(&self) -> ApprovalLimits {
        self.limits
    }

    pub(crate) fn credential_ref(&self) -> &str {
        &self.credential_ref
    }

    pub(crate) fn names_model(&self, model: &str) -> bool {
        self.model.as_deref() == Some(model)
    }

    pub(crate) fn destination(&self) -> DestinationPolicy {
        DestinationPolicy::ApprovedHttps {
            host: self.endpoint.host().to_owned(),
            port: self.endpoint.port(),
            route: self.route.clone(),
            authority: self.authority,
        }
    }

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

    pub(crate) fn permits_endpoint(&self, endpoint: &str) -> bool {
        self.authorized().is_ok() && self.endpoint.permits(endpoint)
    }

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
        let refusal = if self.authority == ExternalAuthority::ScheduledExternalTransport {
            frozen.native_execution_refusal()
        } else {
            frozen.single_request_refusal()
        };
        if let Some(reason) = refusal {
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
        if prepared.is_summary_producer()
            && self.authority == ExternalAuthority::ScheduledExternalTransport
        {
            if let Some(reason) = frozen.provider_refusal() {
                return Err(refuse("not_single_request_generative", reason));
            }
        } else {
            self.check_generative(frozen)?;
        }
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
