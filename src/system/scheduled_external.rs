//! The durable authorization for one scheduled external transport, and the
//! runtime view one stored policy yields.
//!
//! The native scheduler ([`crate::system::native_scheduler`]) selects native
//! Generative Jobs without a per-Job invocation. A literal loopback endpoint
//! is admitted exactly as before. An external configuration is admitted only
//! by a stored [`crate::db::entities::ScheduledExternalPolicyRecord`], and only
//! when
//! [`crate::system::execution_controls::ExecutionControl::ScheduledExternalTransport`]
//! is enabled. Neither condition alone is enough: this build keeps the control
//! disabled, so [`grant`] refuses every row and no external dispatch exists.
//!
//! The policy names exactly one endpoint, one credential reference, one exact
//! Model, one trusted loopback proxy and the limits of one exchange. It stores
//! no plaintext credential and no mutable Provider configuration. A
//! [`ScheduledExternalGrant`] is the runtime view formed from one row; it is
//! deliberately not convertible from
//! [`crate::service::ai::providers::ExternalAuthority`], so a manual external
//! approval cannot authorize a scheduled exchange.
//!
//! The three authorization points are separate:
//!
//! 1. selection and reservation, in one writer transaction, require a live
//!    policy that binds the Job's whole frozen configuration, and record the
//!    policy identity and revision in a durable binding;
//! 2. the checked start revalidates that binding and policy revision in the
//!    start transaction;
//! 3. the send-evidence transaction revalidates them again, atomically with
//!    recording that the request was sent, so a revocation that lands
//!    beforehand prevents the request from being sent at all.
//!
//! A missing, revoked, disabled or stale policy refuses; none is inferred from
//! the Job's frozen configuration or from another authority.
//!
//! The send-evidence commit is the durable send boundary, not a lock on the
//! network. The HTTP request is written only after that commit, so a revocation
//! committed before it prevents the send; a revocation committed after it
//! cannot unsend the request, and the exchange keeps its possibly-sent
//! semantics (held as Uncertain unless a response proves how it ended). No
//! database transaction and network I/O are atomic, and nothing here claims
//! otherwise.

use crate::db::entities::{
    FrozenExecutionConfig, LlmTransport, ScheduledExternalPolicyId, ScheduledExternalPolicyRecord,
};
use crate::service::ai::providers::{
    CancellationToken, DestinationPolicy, ProviderExecutionConfig,
};
use crate::system::execution_controls::ExecutionControl;
use crate::system::external_approval::{
    ApprovalLimits, ApprovalRefusal, ApprovedEndpoint, ExternalApproval, MAX_OUTPUT_TOKENS_CEILING,
    MAX_REQUEST_BYTES_CEILING, MAX_TIMEOUT_CEILING, parse_trusted_proxy,
};

/// Why a scheduled external policy cannot be formed, bind a Job, or authorize
/// a send. `code` is stable and every case is a deterministic refusal.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{code}: {detail}")]
pub(crate) struct ScheduledExternalRefusal {
    pub(crate) code: &'static str,
    pub(crate) detail: String,
}

impl ScheduledExternalRefusal {
    fn new(code: &'static str, detail: impl Into<String>) -> Self {
        Self {
            code,
            detail: detail.into(),
        }
    }
}

impl From<ApprovalRefusal> for ScheduledExternalRefusal {
    fn from(refusal: ApprovalRefusal) -> Self {
        Self::new(refusal.code, refusal.detail)
    }
}

/// An operator's validated input for one policy. Its fields are private and
/// it is only produced by [`Self::new`], so an unvalidated endpoint, credential
/// reference, Model, proxy or limit never reaches storage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ScheduledExternalPolicyDraft {
    endpoint: String,
    credential_ref: String,
    model: String,
    proxy: String,
    limits: ApprovalLimits,
}

impl ScheduledExternalPolicyDraft {
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
        if !crate::db::redb::valid_credential_ref(credential_ref) {
            return Err(ScheduledExternalRefusal::new(
                "scheduled_external_credential_ref_invalid",
                "the credential reference must be 1..=128 characters without control characters",
            ));
        }
        let proxy = parse_trusted_proxy(proxy)?;
        let limits = ApprovalLimits::new(
            usize::try_from(max_request_bytes).map_err(|_| {
                ScheduledExternalRefusal::new(
                    "scheduled_external_limit_out_of_range",
                    "the request byte limit is out of range",
                )
            })?,
            max_output_tokens,
            std::time::Duration::from_secs(u64::from(timeout_secs)),
        )?;
        // Delegates the exact Model check to the same validator a manual
        // approval uses, without constructing an authority of its own.
        ExternalApproval::scheduled_external(
            endpoint.request_url(),
            credential_ref,
            model,
            proxy,
            limits,
        )?;
        Ok(Self {
            endpoint: endpoint.request_url().to_owned(),
            credential_ref: credential_ref.to_owned(),
            model: model.to_owned(),
            proxy: proxy.address().to_string(),
            limits,
        })
    }

    pub(crate) fn endpoint(&self) -> &str {
        &self.endpoint
    }
    pub(crate) fn credential_ref(&self) -> &str {
        &self.credential_ref
    }
    pub(crate) fn model(&self) -> &str {
        &self.model
    }
    pub(crate) fn proxy(&self) -> &str {
        &self.proxy
    }
    pub(crate) fn limits(&self) -> ApprovalLimits {
        self.limits
    }
}

/// The runtime view of one live stored policy, under the build's control.
/// Nothing forms one from an approval, a Job's frozen configuration or a
/// request: [`Self::from_record`] is the only constructor and it refuses a
/// disabled control, a disabled or revoked row, and every non-canonical field.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ScheduledExternalGrant {
    pub(crate) policy: ScheduledExternalPolicyId,
    pub(crate) policy_revision: u32,
    approval: ExternalApproval,
}

impl ScheduledExternalGrant {
    pub(crate) fn from_record(
        record: &ScheduledExternalPolicyRecord,
    ) -> Result<Self, ScheduledExternalRefusal> {
        if !ExecutionControl::ScheduledExternalTransport.is_enabled() {
            return Err(ScheduledExternalRefusal::new(
                "scheduled_external_control_disabled",
                ExecutionControl::ScheduledExternalTransport.refusal(),
            ));
        }
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
            || std::time::Duration::from_secs(u64::from(record.timeout_secs)) > MAX_TIMEOUT_CEILING
        {
            return Err(ScheduledExternalRefusal::new(
                "scheduled_external_limit_out_of_range",
                "the stored limits are out of range",
            ));
        }
        if !crate::db::redb::valid_credential_ref(&record.credential_ref) {
            return Err(ScheduledExternalRefusal::new(
                "scheduled_external_credential_ref_invalid",
                "the stored credential reference is invalid",
            ));
        }
        let proxy = parse_trusted_proxy(&format!("http://{}", record.proxy))?;
        let limits = ApprovalLimits::new(
            usize::try_from(record.max_request_bytes).map_err(|_| {
                ScheduledExternalRefusal::new(
                    "scheduled_external_limit_out_of_range",
                    "the stored request byte limit is out of range",
                )
            })?,
            record.max_output_tokens,
            std::time::Duration::from_secs(u64::from(record.timeout_secs)),
        )?;
        let approval = ExternalApproval::scheduled_external(
            &record.endpoint,
            &record.credential_ref,
            &record.model,
            proxy,
            limits,
        )?;
        Ok(Self {
            policy: record.id,
            policy_revision: record.revision,
            approval,
        })
    }

    /// Whether this grant admits exactly the Job's frozen configuration. The
    /// endpoint, credential reference and Model are assertions on the frozen
    /// values, never overrides; the Job must also be the checked single
    /// request, carry no secret header, and bound its own output within the
    /// policy's limits.
    pub(crate) fn admits(
        &self,
        frozen: &FrozenExecutionConfig,
    ) -> Result<(), ScheduledExternalRefusal> {
        if let Some(reason) = frozen.single_request_refusal() {
            return Err(ScheduledExternalRefusal::new(
                "scheduled_external_not_single_request",
                reason,
            ));
        }
        let provider = &frozen.llm.provider;
        if provider.transport != LlmTransport::OpenAiChatCompletions {
            return Err(ScheduledExternalRefusal::new(
                "scheduled_external_transport_not_supported",
                "only the OpenAI chat completions transport is supported",
            ));
        }
        let endpoint = provider.endpoint.as_deref().ok_or_else(|| {
            ScheduledExternalRefusal::new(
                "scheduled_external_endpoint_missing",
                "the Job's frozen configuration names no endpoint",
            )
        })?;
        if !self.approval.permits_endpoint(endpoint) {
            return Err(ScheduledExternalRefusal::new(
                "scheduled_external_endpoint_mismatch",
                "the Job's frozen endpoint is not the one the scheduled policy names",
            ));
        }
        let credential_ref = provider.credential_ref.as_deref().ok_or_else(|| {
            ScheduledExternalRefusal::new(
                "scheduled_external_credential_missing",
                "the Job's frozen configuration names no credential reference",
            )
        })?;
        if credential_ref != self.approval.credential_ref() {
            return Err(ScheduledExternalRefusal::new(
                "scheduled_external_credential_mismatch",
                "the Job's frozen credential reference is not the one the scheduled policy names",
            ));
        }
        if provider.headers.secret_refs().next().is_some() {
            return Err(ScheduledExternalRefusal::new(
                "scheduled_external_secret_header_not_approved",
                "a custom header carries a secret; only the policy's credential may leave the process",
            ));
        }
        if !self.approval.names_model(&frozen.llm.config.model) {
            return Err(ScheduledExternalRefusal::new(
                "scheduled_external_model_mismatch",
                "the Job's frozen Model is not the exact Model the scheduled policy names",
            ));
        }
        let parameters = &frozen.llm.config.parameters;
        let bounds: Vec<u32> = [parameters.max_tokens, parameters.max_completion_tokens]
            .into_iter()
            .flatten()
            .collect();
        if bounds.is_empty()
            || bounds
                .iter()
                .any(|bound| *bound > self.approval.limits().max_output_tokens)
            || self.approval.limits().max_request_bytes == 0
            || self.approval.limits().timeout.is_zero()
        {
            return Err(ScheduledExternalRefusal::new(
                "scheduled_external_not_bounded",
                "the Job must freeze an output bound within the scheduled limits, and the limits must be nonzero",
            ));
        }
        Ok(())
    }

    /// The exact prepared request, before the start is submitted.
    pub(crate) fn check_prepared(
        &self,
        prepared: &crate::db::PreparedDevelopmentHandoff,
    ) -> Result<(), ApprovalRefusal> {
        self.approval.check_prepared(prepared)
    }

    /// The one exchange's Provider configuration: this destination, this
    /// timeout, a bounded response.
    pub(crate) fn provider_config(
        &self,
        cancellation: CancellationToken,
    ) -> ProviderExecutionConfig {
        self.approval.provider_config(cancellation)
    }

    pub(crate) fn destination(&self) -> DestinationPolicy {
        self.approval.destination()
    }

    pub(crate) fn permits_endpoint(&self, endpoint: &str) -> bool {
        self.approval.permits_endpoint(endpoint)
    }
}

/// Whether one stored row, read without a grant, admits this frozen
/// configuration. It is used inside the reservation, start and send
/// transactions; every case refuses when anything is missing or inconsistent.
pub(crate) fn record_admits(
    record: &ScheduledExternalPolicyRecord,
    frozen: &FrozenExecutionConfig,
) -> bool {
    ScheduledExternalGrant::from_record(record).is_ok_and(|grant| grant.admits(frozen).is_ok())
}
