//! Manual loopback execution: one explicitly named native Generative Job,
//! run once against a literal loopback Provider, by an operator.
//!
//! This is [`ExecutionControl::ManualLoopbackExecution`] and nothing more. It
//! is not an external trial ([`crate::system::external_trial`] keeps its own
//! approval, control and destination policy), not general external
//! transport, not Provider discovery, not autonomous scheduling and not
//! recovery. The exchange runs under [`EndpointPolicy::Loopback`], so the
//! Job's own frozen endpoint must be a literal `127.0.0.1` HTTP endpoint with
//! an explicit port, and a configured proxy that does not bypass it refuses
//! the exchange rather than tunnelling it; there is no endpoint override.
//!
//! Everything predictable is refused before anything is reserved, so a
//! refusal leaves the submitted Job untouched. The Job must be tool-free:
//! the shared single-request predicate (a chat Agent without a Workspace
//! grant, the OpenAI Chat Completions protocol, a declared context window)
//! is the one that admission, handoff and the start transaction ask, and the
//! exact request built without an Allocation must stream and offer no tools.
//! The reservation, the checked start and the exchange then each happen at
//! most once; see [`crate::system::manual_execution`]. An exchange that may
//! have reached the Provider leaves its Allocation held; nothing here
//! retries, reconciles or releases it.

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

/// The longest exchange the operator may allow, whatever they ask for.
pub(crate) const MAX_TIMEOUT_CEILING: Duration = Duration::from_secs(600);
/// The response bytes the exchange reads before it gives up.
const MAX_RESPONSE_BYTES: usize = 1 << 20;

/// What one invocation names. The endpoint is never named here: it is the
/// Job's frozen one.
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

/// Why the exact prepared request is not a tool-free loopback exchange. The
/// same facts are asked before the reservation and again inside the start.
/// The native scheduler's loopback dispatch asks it inside its start too; see
/// [`crate::system::native_scheduler`].
pub(crate) fn check_prepared(prepared: &PreparedDevelopmentHandoff) -> Result<(), &'static str> {
    let Some(frozen) = prepared.generative_configuration() else {
        return Err("generative_required");
    };
    if frozen.single_request_refusal().is_some() {
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
    if request.get("tools").is_some()
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

/// Runs the one loopback execution. Everything before the reservation is a
/// read. `cancellation` is the operator's stop; the timeout cancels it too.
/// Neither is a claim about the Provider.
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

    // 1. Reads only: the Job, its frozen configuration and its standing.
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

    // 1b. The exact request, built without an Allocation. A refusal here
    // changes nothing; the Job stays submitted.
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

    // 2. One reservation, of this Job only. 3. The checked start. 4. The one
    // exchange, to the Job's loopback endpoint and nowhere else.
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
        supervise_development_generative(database, handoff, &config, EndpointPolicy::Loopback),
    )
    .await;

    // 5. What the database now shows is the report.
    report(database, target.job, &exchange).await
}
