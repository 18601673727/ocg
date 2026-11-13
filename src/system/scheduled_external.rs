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

    pub(crate) fn admits(
        &self,
        frozen: &FrozenExecutionConfig,
    ) -> Result<(), ScheduledExternalRefusal> {
        if let Some(reason) = frozen.native_execution_refusal() {
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

    pub(crate) fn check_prepared(
        &self,
        prepared: &crate::db::PreparedDevelopmentHandoff,
    ) -> Result<(), ApprovalRefusal> {
        self.approval.check_prepared(prepared)
    }

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

    pub(crate) fn limits(&self) -> ApprovalLimits {
        self.approval.limits()
    }
}

pub(crate) fn record_admits(
    record: &ScheduledExternalPolicyRecord,
    frozen: &FrozenExecutionConfig,
) -> bool {
    ScheduledExternalGrant::from_record(record).is_ok_and(|grant| grant.admits(frozen).is_ok())
}
