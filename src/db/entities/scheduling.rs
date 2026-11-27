use crate::db::entities::identity::{
    ExecutionPoolId, ExecutorId, JobId, ReservationRequestId, ScheduledExternalPolicyId,
};
use crate::db::entities::{
    ExecutionOperation, FrozenExecutionConfig, LlmTransport, OutcomeKind, StopProof, Timestamp,
    valid_credential_ref,
};
use crate::network::http::LoopbackProxy;
use std::collections::BTreeSet;
use std::num::NonZeroU32;
use std::time::Duration;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AllocationPool {
    pub id: ExecutionPoolId,
    pub name: String,
    pub capacity: NonZeroU32,
    pub enabled: bool,
    pub archived: bool,
    pub revision: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AllocationExecutor {
    pub id: ExecutorId,
    pub pool: ExecutionPoolId,
    pub name: String,
    pub enabled: bool,
    pub archived: bool,
    pub revision: u32,
    pub capabilities: BTreeSet<(ExecutionOperation, Option<LlmTransport>)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecutorUpdate {
    Enabled(bool),
    Name(String),
    Capabilities(BTreeSet<(ExecutionOperation, Option<LlmTransport>)>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AllocationToken {
    pub job: JobId,
    pub pool: ExecutionPoolId,
    pub executor: ExecutorId,
    pub generation: NonZeroU32,
    pub epoch: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AllocationPhase {
    Reserved,
    Running,
    StopRequested,
    Uncertain,
    Released,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PoolAllocation {
    pub token: AllocationToken,
    pub request_id: ReservationRequestId,
    pub request_after: Option<DevelopmentReadyCursor>,
    pub job_revision: u64,
    pub phase: AllocationPhase,
    pub claimed_at: Timestamp,
    pub started_at: Option<Timestamp>,
    pub host_id: Option<String>,
    pub boot_id: Option<String>,
    pub pool_revision: u32,
    pub executor_revision: u32,
    pub operation: ExecutionOperation,
    pub protocol: Option<LlmTransport>,
    pub stop_confirmed_at: Option<Timestamp>,
    pub stop_proof: Option<StopProof>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnstartedCause {
    FrozenSecretUnavailable,
    InputUnavailable,
    InputContextTooLarge,
    StartRefused,
    ExecutorUnavailable,
    Cancelled,
    HostShutdown,
    DeadlineExceeded,
    PreparationDeadlineExceeded,
    OfflineReconciled,
}

impl UnstartedCause {
    pub(crate) fn outcome(self) -> OutcomeKind {
        match self {
            Self::Cancelled => OutcomeKind::Cancelled,
            Self::FrozenSecretUnavailable
            | Self::InputUnavailable
            | Self::InputContextTooLarge
            | Self::StartRefused
            | Self::ExecutorUnavailable
            | Self::HostShutdown
            | Self::DeadlineExceeded
            | Self::PreparationDeadlineExceeded
            | Self::OfflineReconciled => OutcomeKind::Failed,
        }
    }

    pub fn code(self) -> &'static str {
        match self {
            Self::FrozenSecretUnavailable => "frozen_secret_unavailable",
            Self::InputUnavailable => "input_unavailable",
            Self::InputContextTooLarge => "input_context_too_large_before_start",
            Self::StartRefused => "start_refused",
            Self::ExecutorUnavailable => "executor_unavailable_before_start",
            Self::Cancelled => "cancelled_before_start",
            Self::HostShutdown => "host_shutdown_before_start",
            Self::DeadlineExceeded => "deadline_exceeded_before_start",
            Self::PreparationDeadlineExceeded => "preparation_deadline_exceeded_before_start",
            Self::OfflineReconciled => "reconciled_unstarted_reservation",
        }
    }

    pub(crate) const CONTEXT_BUDGET_DETAIL: &str =
        "selected execution context exceeds estimated model token budget";

    pub(crate) fn routed_recovery(code: &str) -> Option<Self> {
        [
            Self::OfflineReconciled,
            Self::FrozenSecretUnavailable,
            Self::InputContextTooLarge,
            Self::PreparationDeadlineExceeded,
        ]
        .into_iter()
        .find(|cause| cause.code() == code)
    }

    pub fn summary(self) -> &'static str {
        match self {
            Self::FrozenSecretUnavailable => {
                "A credential this Job was submitted with is no longer available, so it did not start. Nothing was sent to the Provider."
            }
            Self::InputUnavailable => {
                "This Job's input could not be prepared safely, so it did not start. Nothing was sent to the Provider."
            }
            Self::InputContextTooLarge => {
                "The selected context exceeds the Model's input budget. Nothing was sent to the Provider."
            }
            Self::StartRefused => {
                "This Job could not be started safely. Nothing was sent to the Provider."
            }
            Self::ExecutorUnavailable => {
                "The selected Executor became unavailable before execution started. Nothing was sent to the Provider."
            }
            Self::Cancelled => {
                "This execution was cancelled before it started. Nothing was sent to the Provider."
            }
            Self::HostShutdown => {
                "The host shut down before this Job started. Nothing was sent to the Provider."
            }
            Self::DeadlineExceeded => {
                "The dispatch deadline expired before this Job started. Nothing was sent to the Provider."
            }
            Self::PreparationDeadlineExceeded => {
                "Preparation timed out before this Job started. Nothing was sent to the Provider."
            }
            Self::OfflineReconciled => {
                "OCG stopped after reserving capacity for this Job and before it started; an offline reconciliation ended it. Nothing was sent to the Provider."
            }
        }
    }
}

impl PoolAllocation {
    pub fn holds_capacity(&self) -> bool {
        self.phase != AllocationPhase::Released
    }

    pub(crate) fn matches_request(&self, request: DevelopmentReservationRequest) -> bool {
        self.request_id == request.id
            && self.token.pool == request.pool
            && self.token.executor == request.executor
            && self.request_after == request.after
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DevelopmentReadyCursor(pub(crate) (u8, i64, u128, u16));

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DevelopmentReservationRequest {
    pub id: ReservationRequestId,
    pub pool: ExecutionPoolId,
    pub executor: ExecutorId,
    pub after: Option<DevelopmentReadyCursor>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct DevelopmentFreshReservation {
    request: DevelopmentReservationRequest,
}

impl DevelopmentFreshReservation {
    pub(crate) fn new(
        pool: ExecutionPoolId,
        executor: ExecutorId,
        after: Option<DevelopmentReadyCursor>,
    ) -> Self {
        Self {
            request: DevelopmentReservationRequest {
                id: ReservationRequestId::new(uuid::Uuid::now_v7().as_u128()),
                pool,
                executor,
                after,
            },
        }
    }

    pub(crate) fn request(&self) -> DevelopmentReservationRequest {
        self.request
    }
}

pub const STOP_JOB_NOT_RUNNING: &str = "the Job is not running";
pub const STOP_JOB_ENDED: &str = "the Job has already ended";
pub const STOPPED_BEFORE_SEND: &str = "a stop was requested before the request was sent";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeStopRequest {
    EndedBeforeStart {
        token: Option<AllocationToken>,
        outcome: OutcomeKind,
    },
    Requested(AllocationToken),
    AlreadyRequested(AllocationToken),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum DevelopmentReservation {
    Reserved(Box<PoolAllocation>),
    Replayed,
    Continue(DevelopmentReadyCursor),
    Exhausted,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct PoolOccupancy {
    pub held: u64,
    pub reserved: u64,
    pub running: u64,
    pub stop_requested: u64,
    pub uncertain: u64,
}

pub(crate) struct PoolAllocationPage {
    pub pool: crate::db::entities::AllocationPool,
    pub occupancy: Option<PoolOccupancy>,
    pub allocations: Vec<crate::db::entities::PoolAllocation>,
    pub next: Option<(u128, u128)>,
}

pub(crate) struct UncertainAllocation {
    pub allocation: PoolAllocation,
    pub since: Timestamp,
    pub allocation_bytes: Vec<u8>,
    pub commands: Vec<Option<super::CommandGroup>>,
    pub command_calls: Vec<super::ToolCallRef>,
    pub evidence: String,
}

pub(crate) type AllocationCursor = (u128, u128, u128);
pub(crate) type UncertainPage = super::CursorPage<UncertainAllocation, AllocationCursor>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScheduledExternalPolicyRecord {
    pub id: ScheduledExternalPolicyId,
    pub endpoint: String,
    pub credential_ref: String,
    pub model: String,
    pub proxy: String,
    pub max_request_bytes: u32,
    pub max_output_tokens: u32,
    pub timeout_secs: u32,
    pub enabled: bool,
    pub revoked: bool,
    pub revision: u32,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

impl ScheduledExternalPolicyRecord {
    pub const fn is_live(&self) -> bool {
        self.enabled && !self.revoked
    }

    pub(in crate::db) fn admits(&self, frozen: &FrozenExecutionConfig) -> bool {
        ScheduledExternalPolicyConfig::from_live_record(self).is_ok_and(|config| {
            ScheduledExternalTarget::new(frozen).is_some_and(|target| target.matches(&config))
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScheduledExternalBinding {
    pub job: JobId,
    pub policy: ScheduledExternalPolicyId,
    pub policy_revision: u32,
    pub allocation_epoch: u64,
    pub allocation_generation: NonZeroU32,
}

const MAX_REQUEST_BYTES_CEILING: usize = 1 << 20;
const MAX_OUTPUT_TOKENS_CEILING: u32 = 4096;
pub(crate) const SCHEDULED_EXTERNAL_MAX_TIMEOUT: Duration = Duration::from_secs(600);

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{code}: {detail}")]
pub(crate) struct ScheduledExternalRefusal {
    pub(crate) code: &'static str,
    pub(crate) detail: String,
}

impl ScheduledExternalRefusal {
    pub(crate) fn new(code: &'static str, detail: impl Into<String>) -> Self {
        Self {
            code,
            detail: detail.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ApprovedEndpoint {
    host: String,
    port: u16,
    request_url: String,
}

impl ApprovedEndpoint {
    fn parse(text: &str) -> Result<Self, ScheduledExternalRefusal> {
        let invalid =
            |detail: &str| ScheduledExternalRefusal::new("endpoint_not_approvable", detail);
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
        if !crate::network::http::host_may_be_external(host) {
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
        let host = crate::network::http::bare_host(host);
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

    fn permits(&self, frozen: &str) -> bool {
        Self::parse(frozen).is_ok_and(|frozen| frozen == *self)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ScheduledExternalLimits {
    pub(crate) max_request_bytes: usize,
    pub(crate) max_output_tokens: u32,
    pub(crate) timeout: Duration,
}

impl ScheduledExternalLimits {
    fn new(
        max_request_bytes: usize,
        max_output_tokens: u32,
        timeout: Duration,
    ) -> Result<Self, ScheduledExternalRefusal> {
        if max_request_bytes == 0
            || max_request_bytes > MAX_REQUEST_BYTES_CEILING
            || max_output_tokens == 0
            || max_output_tokens > MAX_OUTPUT_TOKENS_CEILING
            || timeout.is_zero()
            || timeout > SCHEDULED_EXTERNAL_MAX_TIMEOUT
        {
            return Err(ScheduledExternalRefusal::new(
                "limit_out_of_range",
                format!(
                    "request bytes 1..={MAX_REQUEST_BYTES_CEILING}, output tokens 1..={MAX_OUTPUT_TOKENS_CEILING} and timeout 1..={} s are required",
                    SCHEDULED_EXTERNAL_MAX_TIMEOUT.as_secs()
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

fn parse_trusted_proxy(text: &str) -> Result<LoopbackProxy, ScheduledExternalRefusal> {
    let invalid = |detail: &str| ScheduledExternalRefusal::new("proxy_not_approvable", detail);
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
        return Err(invalid(
            "the proxy address must be written in normalized form",
        ));
    }
    LoopbackProxy::new(address).ok_or_else(|| {
        invalid("only a literal loopback proxy address with a nonzero port is eligible")
    })
}

fn valid_model_id(model: &str) -> Result<(), ScheduledExternalRefusal> {
    if model.is_empty()
        || model.len() > 128
        || !model.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
    {
        return Err(ScheduledExternalRefusal::new(
            "model_invalid",
            "the model id must be 1..=128 visible ASCII characters",
        ));
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ScheduledExternalPolicyConfig {
    endpoint: ApprovedEndpoint,
    credential_ref: String,
    model: String,
    proxy: LoopbackProxy,
    limits: ScheduledExternalLimits,
}

impl ScheduledExternalPolicyConfig {
    pub(crate) fn new(
        endpoint: &str,
        credential_ref: &str,
        model: &str,
        proxy: &str,
        max_request_bytes: u32,
        max_output_tokens: u32,
        timeout_secs: u32,
    ) -> Result<Self, ScheduledExternalRefusal> {
        let endpoint = ApprovedEndpoint::parse(endpoint)?;
        if !valid_credential_ref(credential_ref) {
            return Err(ScheduledExternalRefusal::new(
                "scheduled_external_credential_ref_invalid",
                "the credential reference must be 1..=128 characters without control characters",
            ));
        }
        let proxy = parse_trusted_proxy(proxy)?;
        let limits = ScheduledExternalLimits::new(
            usize::try_from(max_request_bytes).map_err(|_| {
                ScheduledExternalRefusal::new(
                    "scheduled_external_limit_out_of_range",
                    "the request byte limit is out of range",
                )
            })?,
            max_output_tokens,
            Duration::from_secs(u64::from(timeout_secs)),
        )?;
        valid_model_id(model)?;
        // The canonical request URL must also satisfy the endpoint limits.
        ApprovedEndpoint::parse(&endpoint.request_url)?;
        Ok(Self {
            endpoint,
            credential_ref: credential_ref.to_owned(),
            model: model.to_owned(),
            proxy,
            limits,
        })
    }

    pub(crate) fn from_live_record(
        record: &ScheduledExternalPolicyRecord,
    ) -> Result<Self, ScheduledExternalRefusal> {
        if record.revoked {
            return Err(ScheduledExternalRefusal::new(
                "scheduled_external_policy_revoked",
                "the scheduled external policy was revoked",
            ));
        }
        if !record.enabled {
            return Err(ScheduledExternalRefusal::new(
                "scheduled_external_policy_disabled",
                "the scheduled external policy is not enabled",
            ));
        }
        if !(1..=MAX_REQUEST_BYTES_CEILING as u32).contains(&record.max_request_bytes)
            || !(1..=MAX_OUTPUT_TOKENS_CEILING).contains(&record.max_output_tokens)
            || record.timeout_secs == 0
            || Duration::from_secs(u64::from(record.timeout_secs)) > SCHEDULED_EXTERNAL_MAX_TIMEOUT
        {
            return Err(ScheduledExternalRefusal::new(
                "scheduled_external_limit_out_of_range",
                "the stored limits are out of range",
            ));
        }
        if !valid_credential_ref(&record.credential_ref) {
            return Err(ScheduledExternalRefusal::new(
                "scheduled_external_credential_ref_invalid",
                "the stored credential reference is invalid",
            ));
        }
        let proxy = parse_trusted_proxy(&format!("http://{}", record.proxy))?;
        let limits = ScheduledExternalLimits::new(
            usize::try_from(record.max_request_bytes).map_err(|_| {
                ScheduledExternalRefusal::new(
                    "scheduled_external_limit_out_of_range",
                    "the stored request byte limit is out of range",
                )
            })?,
            record.max_output_tokens,
            Duration::from_secs(u64::from(record.timeout_secs)),
        )?;
        valid_model_id(&record.model)?;
        let endpoint = ApprovedEndpoint::parse(&record.endpoint)?;
        Ok(Self {
            endpoint,
            credential_ref: record.credential_ref.clone(),
            model: record.model.clone(),
            proxy,
            limits,
        })
    }

    pub(in crate::db) fn match_key(&self) -> (&str, &str, &str) {
        (self.endpoint(), self.credential_ref(), self.model())
    }

    pub(crate) fn endpoint(&self) -> &str {
        &self.endpoint.request_url
    }

    pub(crate) fn endpoint_origin(&self) -> (&str, u16) {
        (&self.endpoint.host, self.endpoint.port)
    }

    pub(crate) fn permits_endpoint(&self, endpoint: &str) -> bool {
        self.endpoint.permits(endpoint)
    }

    pub(crate) fn credential_ref(&self) -> &str {
        &self.credential_ref
    }

    pub(crate) fn model(&self) -> &str {
        &self.model
    }

    pub(crate) fn proxy(&self) -> LoopbackProxy {
        self.proxy
    }

    pub(crate) fn limits(&self) -> ScheduledExternalLimits {
        self.limits
    }
}

pub(crate) struct ScheduledExternalTarget<'a> {
    endpoint: ApprovedEndpoint,
    credential_ref: &'a str,
    model: &'a str,
    max_output_tokens: u32,
}

impl<'a> ScheduledExternalTarget<'a> {
    pub(in crate::db) fn new(frozen: &'a FrozenExecutionConfig) -> Option<Self> {
        let provider = &frozen.llm.provider;
        if frozen.native_execution_refusal().is_some()
            || provider.transport != LlmTransport::OpenAiChatCompletions
            || provider.headers.secret_refs().next().is_some()
        {
            return None;
        }
        let parameters = &frozen.llm.config.parameters;
        Some(Self {
            endpoint: ApprovedEndpoint::parse(provider.endpoint.as_deref()?).ok()?,
            credential_ref: provider.credential_ref.as_deref()?,
            model: &frozen.llm.config.model,
            max_output_tokens: parameters
                .max_tokens
                .max(parameters.max_completion_tokens)?,
        })
    }

    pub(in crate::db) fn match_key(&self) -> (&str, &str, &str) {
        (&self.endpoint.request_url, self.credential_ref, self.model)
    }

    pub(in crate::db) fn matches(&self, policy: &ScheduledExternalPolicyConfig) -> bool {
        self.match_key() == policy.match_key()
            && self.max_output_tokens <= policy.limits().max_output_tokens
    }
}
