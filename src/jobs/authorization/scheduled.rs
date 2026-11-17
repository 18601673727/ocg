use crate::ai::providers::{CancellationToken, DestinationPolicy, ProviderExecutionConfig};
use crate::db::entities::{
    FrozenExecutionConfig, JobId, LlmTransport, ScheduledExternalLimits,
    ScheduledExternalPolicyConfig, ScheduledExternalRefusal, Violation,
};
use crate::db::{DatabaseError, DatabaseHandle, PreparedDevelopmentHandoff};

const MAX_RESPONSE_BYTES: usize = 1 << 20;

pub(crate) struct ScheduledExternalGrant {
    policy: ScheduledExternalPolicyConfig,
}

impl ScheduledExternalGrant {
    pub(crate) async fn load(
        database: &DatabaseHandle,
        job: JobId,
    ) -> Result<Option<Self>, DatabaseError> {
        let Some(record) = database.scheduled_external_bound_policy(job).await? else {
            return Ok(None);
        };
        let policy =
            ScheduledExternalPolicyConfig::from_live_record(&record).map_err(|refusal| {
                DatabaseError::Rejected(Violation::InvalidValue(
                    "scheduled_external_policy",
                    refusal.code,
                ))
            })?;
        Ok(Some(Self { policy }))
    }

    pub(crate) fn provider_config(
        &self,
        cancellation: CancellationToken,
    ) -> ProviderExecutionConfig {
        let timeout = self.policy.limits().timeout;
        ProviderExecutionConfig {
            timeout,
            first_delta_timeout: Some(timeout),
            max_output_bytes: MAX_RESPONSE_BYTES,
            cancellation,
            destination: self.destination(),
            ..ProviderExecutionConfig::default()
        }
    }

    pub(crate) fn destination(&self) -> DestinationPolicy {
        let (host, port) = self.policy.endpoint_origin();
        DestinationPolicy::ApprovedHttps {
            host: host.to_owned(),
            port,
            proxy: self.policy.proxy(),
        }
    }

    pub(crate) fn permits_endpoint(&self, endpoint: &str) -> bool {
        self.policy.permits_endpoint(endpoint)
    }

    pub(crate) fn limits(&self) -> ScheduledExternalLimits {
        self.policy.limits()
    }

    fn check_generative(
        &self,
        frozen: &FrozenExecutionConfig,
    ) -> Result<(), ScheduledExternalRefusal> {
        let provider = &frozen.llm.provider;
        if provider.transport != LlmTransport::OpenAiChatCompletions {
            return Err(ScheduledExternalRefusal::new(
                "transport_not_supported",
                "only the OpenAI chat completions transport is supported",
            ));
        }
        match provider.endpoint.as_deref() {
            Some(endpoint) if self.policy.permits_endpoint(endpoint) => {}
            _ => {
                return Err(ScheduledExternalRefusal::new(
                    "endpoint_not_approved",
                    "the Job's frozen endpoint is not the endpoint approved for this invocation",
                ));
            }
        }
        if provider.credential_ref.as_deref() != Some(self.policy.credential_ref()) {
            return Err(ScheduledExternalRefusal::new(
                "credential_not_approved",
                "the Job's frozen credential reference is not the one approved for this invocation",
            ));
        }
        if provider.headers.secret_refs().next().is_some() {
            return Err(ScheduledExternalRefusal::new(
                "secret_header_not_approved",
                "a custom header carries a secret; only the approved credential may leave the process",
            ));
        }
        let model = frozen.llm.config.model.as_str();
        if self.policy.model() != model {
            return Err(ScheduledExternalRefusal::new(
                "model_not_approved",
                "the Job's frozen model is not the model approved for this invocation",
            ));
        }
        if let Some(reason) = frozen.native_execution_refusal() {
            return Err(ScheduledExternalRefusal::new(
                "not_single_request_generative",
                reason,
            ));
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
                .any(|bound| *bound > self.policy.limits().max_output_tokens)
        {
            return Err(ScheduledExternalRefusal::new(
                "output_bound_missing_or_too_large",
                format!(
                    "the Job must freeze max_tokens or max_completion_tokens, none above {}",
                    self.policy.limits().max_output_tokens
                ),
            ));
        }
        Ok(())
    }

    pub(crate) fn check_prepared(
        &self,
        prepared: &PreparedDevelopmentHandoff,
    ) -> Result<(), ScheduledExternalRefusal> {
        let Some(frozen) = prepared.generative_configuration() else {
            return Err(ScheduledExternalRefusal::new(
                "decision_not_supported",
                "a Generative Job is required",
            ));
        };
        if prepared.is_summary_producer() {
            if let Some(reason) = frozen.provider_refusal() {
                return Err(ScheduledExternalRefusal::new(
                    "not_single_request_generative",
                    reason,
                ));
            }
        } else {
            self.check_generative(frozen)?;
        }
        if prepared.credential_references() != [self.policy.credential_ref()] {
            return Err(ScheduledExternalRefusal::new(
                "credential_not_approved",
                "the handoff would carry a credential other than the approved one",
            ));
        }
        let request = prepared.request();
        let size = serde_json::to_vec(request).map_or(usize::MAX, |bytes| bytes.len());
        if size > self.policy.limits().max_request_bytes {
            return Err(ScheduledExternalRefusal::new(
                "request_too_large",
                format!(
                    "the request is {size} bytes; at most {} are approved",
                    self.policy.limits().max_request_bytes
                ),
            ));
        }
        let model = request.get("model").and_then(serde_json::Value::as_str);
        let effort = request
            .get("reasoning_effort")
            .and_then(serde_json::Value::as_str);
        if request.get("thinking").is_some()
            || model.is_none_or(|model| check_thinking(model, effort).is_err())
            || model != Some(self.policy.model())
        {
            return Err(ScheduledExternalRefusal::new(
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
                .any(|bound| *bound > u64::from(self.policy.limits().max_output_tokens))
            || request.get("tools").is_some()
            || request.get("tool_choice").is_some()
            || request.get("stream") != Some(&serde_json::Value::Bool(true))
        {
            return Err(ScheduledExternalRefusal::new(
                "request_not_bounded",
                "the prepared request must stream, offer no tools, and bound its output",
            ));
        }
        Ok(())
    }
}

fn check_thinking(model: &str, effort: Option<&str>) -> Result<(), ScheduledExternalRefusal> {
    if !model.starts_with("k3.1-pro") {
        return Ok(());
    }
    match effort {
        Some("low" | "high" | "max") => Ok(()),
        _ => Err(ScheduledExternalRefusal::new(
            "thinking_effort_not_supported",
            format!(
                "{model} requires thinking: freeze reasoning_effort as exactly low, high or max (none, minimal, medium, xhigh or no value are not accepted here)"
            ),
        )),
    }
}
