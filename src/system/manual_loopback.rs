use std::time::Duration;

use crate::db::entities::MutableFrozenConfiguration;
use crate::db::{DatabaseHandle, DevelopmentPreviewError, PreparedDevelopmentHandoff};
use crate::service::ai::providers::{CancellationToken, ProviderExecutionConfig};
use crate::system::execution_controls::ExecutionControl;
use crate::system::executor::execution_runtime::{
    EndpointPolicy, supervise_development_generative,
};
use crate::system::http::direct_diagnostic_loopback;
use crate::system::manual_execution::{
    ExecutionReport, Stop, Target, checked_start, read_submitted_job, refuse, report,
    require_free_executor, reserve_named_job, within_deadline,
};

pub(crate) const MAX_TIMEOUT_CEILING: Duration = Duration::from_secs(600);
const MAX_RESPONSE_BYTES: usize = 1 << 20;

#[derive(Clone, Copy, Debug)]
pub(crate) struct LoopbackRequest {
    pub(crate) target: Target,
    pub(crate) timeout: Duration,
}

impl LoopbackRequest {
    pub(crate) fn new(target: Target, timeout: Duration) -> Result<Self, Stop> {
        if timeout.is_zero() || timeout > MAX_TIMEOUT_CEILING {
            return Err(refuse(
                "timeout_invalid",
                format!(
                    "the timeout must be 1..={} seconds",
                    MAX_TIMEOUT_CEILING.as_secs()
                ),
            ));
        }
        Ok(Self { target, timeout })
    }
}

pub(crate) fn check_prepared(prepared: &PreparedDevelopmentHandoff) -> Result<(), &'static str> {
    check_prepared_with(prepared, false)
}

pub(crate) fn check_prepared_native(
    prepared: &PreparedDevelopmentHandoff,
) -> Result<(), &'static str> {
    if let Some(decision) = prepared.decision_configuration() {
        if !decision
            .endpoint
            .as_deref()
            .is_some_and(direct_diagnostic_loopback)
        {
            return Err("endpoint_not_loopback");
        }
        return Ok(());
    }
    check_prepared_with(prepared, true)
}

fn check_prepared_with(
    prepared: &PreparedDevelopmentHandoff,
    tools: bool,
) -> Result<(), &'static str> {
    let Some(frozen) = prepared.generative_configuration() else {
        return Err("generative_required");
    };
    let refusal = if tools {
        frozen.native_execution_refusal()
    } else {
        frozen.single_request_refusal()
    };
    if refusal.is_some() {
        return Err("not_single_request_generative");
    }
    if !frozen
        .llm
        .provider
        .endpoint
        .as_deref()
        .is_some_and(direct_diagnostic_loopback)
    {
        return Err("endpoint_not_loopback");
    }
    let request = prepared.request();
    if (!tools && request.get("tools").is_some())
        || request.get("tool_choice").is_some()
        || request.get("stream") != Some(&serde_json::Value::Bool(true))
    {
        return Err("request_not_tool_free_stream");
    }
    Ok(())
}

fn detail(code: &str) -> &'static str {
    match code {
        "generative_required" => "a Generative Job is required",
        "not_single_request_generative" => {
            "the Job's frozen configuration cannot run as a single tool-free Generative request"
        }
        "endpoint_not_loopback" => {
            "the Job's frozen endpoint is not a literal loopback endpoint this build may reach"
        }
        _ => "the prepared request must stream and offer no tools",
    }
}

pub(crate) async fn run_loopback_job(
    database: &DatabaseHandle,
    request: LoopbackRequest,
    cancellation: CancellationToken,
) -> Result<ExecutionReport, Stop> {
    if !ExecutionControl::ManualLoopbackExecution.is_enabled() {
        return Err(refuse(
            "control_disabled",
            ExecutionControl::ManualLoopbackExecution.refusal(),
        ));
    }
    let target = request.target;

    let record = read_submitted_job(database, target.job).await?;
    match record.job.frozen.as_ref() {
        Some(MutableFrozenConfiguration::Generative(frozen)) => {
            if let Some(reason) = frozen.single_request_refusal() {
                return Err(refuse("not_single_request_generative", reason));
            }
            if !frozen
                .llm
                .provider
                .endpoint
                .as_deref()
                .is_some_and(direct_diagnostic_loopback)
            {
                return Err(refuse(
                    "endpoint_not_loopback",
                    detail("endpoint_not_loopback"),
                ));
            }
        }
        Some(MutableFrozenConfiguration::Decision(_)) | None => {
            return Err(refuse(
                "decision_not_supported",
                "a Generative Job is required; a Decision is not run here",
            ));
        }
    }
    require_free_executor(database, target.executor).await?;

    match database
        .preview_development_handoff(target.job, target.pool, target.executor, check_prepared)
        .await
    {
        Ok(()) => {}
        Err(DevelopmentPreviewError::Refused(code)) => {
            return Err(refuse(code, detail(code)));
        }
        Err(DevelopmentPreviewError::Unavailable(error)) => {
            return Err(refuse(
                "preflight_unavailable",
                format!(
                    "the request could not be prepared; nothing was reserved or sent, and the Job is unchanged ({error})"
                ),
            ));
        }
    }

    let allocation = reserve_named_job(database, target).await?;
    let handoff = checked_start(database, allocation.token, check_prepared).await?;
    let config = ProviderExecutionConfig {
        timeout: request.timeout,
        first_delta_timeout: Some(request.timeout),
        max_output_bytes: MAX_RESPONSE_BYTES,
        cancellation: cancellation.clone(),
        ..ProviderExecutionConfig::default()
    };
    let exchange = within_deadline(
        &cancellation,
        request.timeout,
        supervise_development_generative(
            database,
            handoff,
            &config,
            EndpointPolicy::Loopback,
            None,
        ),
    )
    .await;

    report(database, target.job, &exchange).await
}
