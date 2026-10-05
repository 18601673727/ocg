//! Health Probe: can OCG execute this Provider x Model x Effort combination?
//!
//! Health is evidence about an executable placement target, not a mutable flag.
//! A probe therefore is a real canonical Job. It declares its target on the Job
//! specification ([`super::domain::JobSpec::health_probe`]), is admitted through
//! the ordinary Job -> Attempt -> Executor -> Call -> DispatchIntent path, runs
//! one real provider round through the same
//! [`crate::provider_loop::CanonicalProviderCallHandler`] every other Job uses,
//! and settles under the ordinary terminal-state rules.
//!
//! Two properties are deliberate:
//!
//! * Nothing here schedules. A probe runs when something asks for one. There is
//!   no timer, no cron entry and no background sweep.
//! * Health is derived on read from the durable Job/Attempt/Call/DispatchIntent
//!   rows. There is no stored health row that can drift from execution history.
//!
//! A probe stops after the first successful provider round. Context assembly,
//! native-tool dispatch and compaction are agent behaviour: they cost tokens and
//! can fail for reasons unrelated to executability, so a probe does not run them.
//! It sends the frozen request over the real transport, with the real
//! credential, against the real endpoint — which is the question being asked.

use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::core_contract::{Failure, FailureClass};
use crate::error::{OcgError, Result};
use crate::http::{BoxFuture, ChunkSink, HttpResponse, HttpTransport};

use super::domain::{job_failure, AttemptId, AttemptState, JobId, JobState};

/// The exact executable placement target a probe answers for.
///
/// This is the health identity: the tuple Placement will later ask about. It is
/// stored on the Job specification, so it is durable, journaled with the Job,
/// and reconstructable from the canonical post-image.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
pub struct HealthProbeIntent {
    /// Profile provider key.
    pub provider: String,
    /// Profile model key, not the upstream model id.
    pub model: String,
    /// Reasoning effort under test; `None` means the tuple carries no effort.
    #[serde(default)]
    pub effort: Option<String>,
}

/// The Job objective recorded for a probe Job.
///
/// A probe has no task. This keeps the Job identifiable in ordinary Job
/// projections without pretending to be work.
pub const PROBE_OBJECTIVE: &str = "health probe";

/// The entire prompt a probe sends.
///
/// The probe asks whether the tuple executes, not what the model can do, so the
/// request must be the smallest one the wire formats accept and must not invite
/// tool use. `reasoning_effort` is the field under test; a probe that omits it
/// proves nothing about Effort.
const PROBE_PROMPT: &str = "Reply with the single word: ok";

/// The reasoning-effort values OCG sends on the wire.
///
/// Mirrors the ladder the canonical launch path accepts, so a probe validates
/// exactly what a real Job would send.
pub const PROBE_EFFORTS: [&str; 6] = ["none", "minimal", "low", "medium", "high", "xhigh"];

/// The provider request a probe Call freezes.
///
/// No `max_tokens` is sent: the probe deliberately supplies no output bound of
/// its own, because an artificial cap turns a reasoning model that is slow to
/// start into a false "not executable" verdict. Each protocol applies the same
/// default it applies for any other Job, and the shortest possible answer is
/// the cheapest possible round.
pub fn probe_request(model: &str, effort: Option<&str>) -> Value {
    let mut request = json!({
        "model": model,
        "messages": [{"role": "user", "content": PROBE_PROMPT}],
        "stream": true,
    });
    if let Some(effort) = effort {
        request["reasoning_effort"] = json!(effort);
    }
    request
}

/// What the transport actually saw for one probe Call.
///
/// The provider loop renders a non-2xx response into a diagnostic string. A
/// probe cannot classify health from prose, so it observes the status where it
/// is still structured, and the rest of the loop is left untouched.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeHttpOutcome {
    /// The round ended without a non-2xx response.
    Completed,
    /// The provider answered with a non-2xx status.
    Status(u16),
    /// The request produced no response at all: connection, DNS, TLS or
    /// transport timeout.
    Unreachable,
}

/// Captures the provider's real HTTP outcome for a single probe Call.
///
/// This is a transport decorator, not a second transport: it forwards every
/// call and remembers only whether a response arrived and what status it
/// carried. It is installed only for probe Calls, so ordinary Jobs keep the
/// exact behaviour they had.
pub struct ProbeOutcomeTransport<'a> {
    inner: &'a dyn HttpTransport,
    outcome: Arc<Mutex<Option<ProbeHttpOutcome>>>,
}

impl<'a> ProbeOutcomeTransport<'a> {
    pub fn new(inner: &'a dyn HttpTransport) -> Self {
        Self {
            inner,
            outcome: Arc::new(Mutex::new(None)),
        }
    }

    /// A handle the executing task can read after the round returns.
    pub fn observer(&self) -> ProbeOutcomeObserver {
        ProbeOutcomeObserver {
            outcome: Arc::clone(&self.outcome),
        }
    }
}

/// Reads the captured HTTP outcome of a probe Call.
#[derive(Clone)]
pub struct ProbeOutcomeObserver {
    outcome: Arc<Mutex<Option<ProbeHttpOutcome>>>,
}

impl ProbeOutcomeObserver {
    pub fn record(&self, outcome: ProbeHttpOutcome) {
        if let Ok(mut slot) = self.outcome.lock() {
            // The first verdict is the one that matters: a probe never retries.
            slot.get_or_insert(outcome);
        }
    }

    pub fn outcome(&self) -> Option<ProbeHttpOutcome> {
        self.outcome.lock().ok().and_then(|slot| *slot)
    }
}

impl HttpTransport for ProbeOutcomeTransport<'_> {
    fn get(&self, url: &str) -> Result<Vec<u8>> {
        self.inner.get(url)
    }

    fn post_json_stream_in_runtime(
        &self,
        url: &str,
        headers: &[(&str, &str)],
        body: &Value,
        on_chunk: ChunkSink,
    ) -> BoxFuture<'static, Result<HttpResponse>> {
        let observer = self.observer();
        let future = self
            .inner
            .post_json_stream_in_runtime(url, headers, body, on_chunk);
        Box::pin(async move {
            match future.await {
                Ok(response) => {
                    if response.status >= 400 {
                        observer.record(ProbeHttpOutcome::Status(response.status));
                    }
                    Ok(response)
                }
                Err(error) => {
                    observer.record(ProbeHttpOutcome::Unreachable);
                    Err(error)
                }
            }
        })
    }
}

/// Run exactly one provider round for a probe Call.
///
/// The round is the real one: same transport, same protocol decoder, same
/// cancellation racing, same credential. What is skipped is everything that is
/// about doing work rather than about whether the tuple executes.
pub async fn run_probe_round(
    provider: &dyn crate::provider_loop::ProviderClient,
    project_root: &Path,
    envelope: &crate::orchestration::execution_dispatch::ExecutionEnvelope,
    request: &Value,
    shutdown: &AtomicBool,
) -> Result<()> {
    crate::provider_loop::ensure_provider_active(project_root, envelope, shutdown)?;
    let round = provider.complete(request).await?;
    crate::provider_loop::ensure_provider_active(project_root, envelope, shutdown)?;
    // The same terminal rules the ordinary loop applies: a truncated answer and
    // an empty one are not evidence that the tuple executes.
    if round.summary.finish_reason
        == Some(crate::openai_compatible::stream::ChatFinishReason::Length)
    {
        return Err(OcgError::config("provider exceeded token limit"));
    }
    if round.summary.text.trim().is_empty() && round.summary.images.is_empty() {
        return Err(OcgError::config(
            "provider returned no user-visible assistant content",
        ));
    }
    Ok(())
}

/// Normalize what a probe round proved into OCG's own failure vocabulary.
///
/// [`FailureClass`] is already the canonical taxonomy, so health evidence
/// reuses it instead of minting a parallel one: `Authentication` and
/// `Authorization` cover credential and permission failures, `Validation` and
/// `Capability` cover an unsupported model/effort or a rejected request,
/// `Timeout` covers a provider that gave up, `Cancelled` covers a revoked
/// authority, and `Provider` covers everything that reached or failed to reach
/// the provider. The HTTP status is the one provider-specific fact worth
/// keeping, so it is carried in `details`.
pub fn classify_failure(
    outcome: Option<ProbeHttpOutcome>,
    cancelled: bool,
    reason: &str,
) -> Failure {
    if cancelled {
        return with_status(
            job_failure(
                "health_probe_cancelled",
                FailureClass::Cancelled,
                reason,
                true,
            ),
            None,
        );
    }
    let (class, code, retryable) = match outcome {
        Some(ProbeHttpOutcome::Status(status)) => status_class(status),
        Some(ProbeHttpOutcome::Unreachable) => {
            (FailureClass::Provider, "health_probe_unreachable", true)
        }
        Some(ProbeHttpOutcome::Completed) | None => {
            (FailureClass::Provider, "health_probe_provider_error", true)
        }
    };
    let status = match outcome {
        Some(ProbeHttpOutcome::Status(status)) => Some(status),
        _ => None,
    };
    with_status(job_failure(code, class, reason, retryable), status)
}

/// Map a non-2xx provider status onto the canonical failure taxonomy.
fn status_class(status: u16) -> (FailureClass, &'static str, bool) {
    match status {
        401 => (
            FailureClass::Authentication,
            "health_probe_authentication",
            false,
        ),
        403 => (
            FailureClass::Authorization,
            "health_probe_authorization",
            false,
        ),
        408 | 504 => (FailureClass::Timeout, "health_probe_timeout", true),
        429 => (FailureClass::RateLimit, "health_probe_rate_limited", true),
        // A model, effort or request the provider will not serve is not a
        // transport problem, and retrying it unchanged cannot help.
        400..=499 => (FailureClass::Validation, "health_probe_unsupported", false),
        // A 5xx is the provider failing, not the request.
        500..=599 => (FailureClass::Provider, "health_probe_provider_error", true),
        _ => (FailureClass::Unknown, "health_probe_unknown", false),
    }
}

fn with_status(mut failure: Failure, status: Option<u16>) -> Failure {
    if let Some(status) = status {
        failure.details = Some(json!({ "http_status": status }));
    }
    failure
}

/// The rejection for a probe target OCG cannot even form a request for.
///
/// Raised before any I/O, but recorded on a real probe Job, so "this model or
/// effort is not supported" is durable evidence rather than a transient string
/// on an HTTP response.
/// The rejection for a probe target OCG cannot even form a request for.
///
/// This is not a provider round: it is OCG's own configuration refusing to
/// dispatch. An unsupported effort is a capability gap in the Profile, a missing
/// endpoint or credential is a validation gap, and a missing provider/model is
/// nothing more than a target that does not exist. None of them is worth
/// retrying unchanged, so all of them are non-retryable.
pub fn unsupported_target(code: &str, message: String) -> Failure {
    let class = match code {
        "health_probe_unsupported_effort" | "health_probe_unsupported_protocol" => {
            FailureClass::Capability
        }
        _ => FailureClass::Validation,
    };
    job_failure(code, class, &message, false)
}

/// The canonical rows one probe left behind, read back for the health
/// projection.
///
/// Every field here is durable execution fact. Nothing in this struct is
/// computed from a stored health value, because there is no stored health
/// value: the projection is this row set, shaped.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HealthProbeOutcome {
    pub project_id: String,
    pub intent: HealthProbeIntent,
    pub job_id: JobId,
    pub job_state: JobState,
    /// The highest-generation Attempt the Job published.
    pub attempt_id: Option<AttemptId>,
    pub attempt_generation: Option<u64>,
    pub attempt_state: Option<AttemptState>,
    /// The provider Call, when the probe reached dispatch.
    pub call_id: Option<String>,
    pub dispatch_intent_id: Option<String>,
    /// The upstream model id the frozen DispatchIntent actually dispatched.
    pub upstream_model_id: Option<String>,
    pub started_at: Option<i64>,
    pub completed_at: Option<i64>,
    /// Wall-clock seconds between the canonical start and finish stamps. The
    /// canonical domain stores whole seconds, so a sub-second probe reports `0`
    /// rather than a fabricated precision.
    pub latency_seconds: Option<i64>,
    /// The canonical terminal failure, when the probe did not execute.
    pub failure: Option<Failure>,
}

/// The wire projection: the latest usable health evidence for one candidate.
///
/// This is the shape later Placement work asks its question against. It is
/// derived on read; nothing here is stored.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ts_rs::TS)]
pub struct HealthProbeObservation {
    pub project_id: String,
    pub provider: String,
    pub model: String,
    pub effort: Option<String>,
    pub job_id: String,
    pub job_state: String,
    pub attempt_id: Option<String>,
    pub attempt_generation: Option<u64>,
    pub attempt_state: Option<String>,
    pub call_id: Option<String>,
    pub dispatch_intent_id: Option<String>,
    pub upstream_model_id: Option<String>,
    pub started_at: Option<i64>,
    pub completed_at: Option<i64>,
    pub latency_seconds: Option<i64>,
    /// `true` only when a probe Job completed a real provider round. This is
    /// the single answer to "reachable and executable"; every other state is
    /// described by `failure`.
    pub executable: bool,
    pub failure: Option<Failure>,
}

impl From<HealthProbeOutcome> for HealthProbeObservation {
    fn from(outcome: HealthProbeOutcome) -> Self {
        let executable = outcome.job_state == JobState::Completed && outcome.failure.is_none();
        Self {
            project_id: outcome.project_id,
            provider: outcome.intent.provider,
            model: outcome.intent.model,
            effort: outcome.intent.effort,
            job_id: outcome.job_id,
            job_state: outcome.job_state.to_string(),
            attempt_id: outcome.attempt_id,
            attempt_generation: outcome.attempt_generation,
            attempt_state: outcome.attempt_state.map(|state| state.to_string()),
            call_id: outcome.call_id,
            dispatch_intent_id: outcome.dispatch_intent_id,
            upstream_model_id: outcome.upstream_model_id,
            started_at: outcome.started_at,
            completed_at: outcome.completed_at,
            latency_seconds: outcome.latency_seconds,
            executable,
            failure: outcome.failure,
        }
    }
}
