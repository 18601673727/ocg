//! Canonical admission for every executable Job.
//!
//! Job creation, Placement, Admission, Dispatch and provider payload construction
//! stay distinguishable. Origin-specific callers prepare a Job and a payload;
//! this module is the only place that decides whether a Job may be reserved for
//! canonical execution, and the only place that publishes the Attempt, Executor
//! and provider Call that reservation owns.
//!
//! Placement is a typed policy seam, not a second scheduler:
//!
//! * [`AdmissionTarget::SelectCandidate`] asks Placement to choose a runnable
//!   Provider × Model. Ordinary Chat and generic Jobs use this.
//! * [`AdmissionTarget::Exact`] freezes a target the caller already named.
//!   A Health Probe uses this so Placement cannot substitute a different tuple.
//!
//! Pickup is the other seam. A Job that still needs candidate selection is
//! automatically admissible once its dependencies are satisfied. A Job whose
//! exact target is already frozen waits for an explicit admission action, so a
//! rejected probe cannot be picked up and routed through Placement.

use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::core_contract::{Failure, FailureClass};
use crate::error::{OcgError, Result};
use crate::profile::Profile;
use crate::provider_protocol::ProviderProtocol;

use super::budget::BudgetConfig;
use super::domain::{job_failure, Attempt, DomainRepository, Executor, Job, JobSpec, JobState};
use super::execution_dispatch::{CallCancellation, ExecutionEvent, ProviderExecutionConfig};
use super::placement::{self, ProviderChoice};

fn invalid(message: impl Into<String>) -> OcgError {
    OcgError::config(message.into())
}

/// How Admission obtains the executable target.
///
/// This is the only distinction between "Placement must choose" and "the target
/// is already frozen". Both modes enter the same reservation and dispatch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AdmissionTarget {
    /// Placement ranks configured candidates and may substitute.
    SelectCandidate,
    /// The caller already named the exact Provider × Model × Effort. Placement
    /// must not choose a different one.
    Exact(ExactTarget),
}

/// A target that Admission must execute as named.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ExactTarget {
    pub provider: String,
    pub model: String,
    pub effort: Option<String>,
}

impl ExactTarget {
    pub(crate) fn choice(&self) -> ProviderChoice {
        ProviderChoice {
            provider: self.provider.clone(),
            model: self.model.clone(),
        }
    }
}

/// Whether the automatic admission worker may pick this Job up.
///
/// Explicit admission is not "inadmissible". It means only an origin that
/// already knows the target may reserve the Job. Terminal and dependency-blocked
/// Jobs are not admissible at all; that is expressed by Job state, not by this
/// policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AdmissionPickup {
    /// Dependencies satisfied and no exact target yet: the worker may admit it.
    Automatic,
    /// An exact target is already frozen. Only an explicit admission action
    /// may reserve this Job.
    Explicit,
}

/// The frozen admission decision, durable on the Job so a crash between
/// reservation and the first Call resumes the same target.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct AdmissionReservation {
    pub choice: ProviderChoice,
    pub pickup: AdmissionPickup,
    /// Present only when the caller froze an exact target. Placement never
    /// writes this, so a resumed probe cannot be mistaken for a selected
    /// candidate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exact: Option<ExactTarget>,
}

/// What an origin may vary without owning a scheduler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PayloadMode {
    /// Ordinary provider request, including native-tool projection.
    Agent,
    /// Executability probe: empty tool projection, no conversation assembly.
    Probe,
}

/// Origin-specific preparation that Admission does not own.
///
/// Chat supplies a conversation. A generic Job supplies its objective. A probe
/// supplies the frozen probe request and must not be rebuilt here.
pub(crate) struct PreparedExecution {
    pub request: Value,
    pub payload: PayloadMode,
    pub event_sender: Option<flume::Sender<ExecutionEvent>>,
    pub cancelled: CallCancellation,
}

/// The facts Admission resolved before reserving an Attempt.
pub(crate) struct ResolvedTarget {
    pub protocol: ProviderProtocol,
    pub config: ProviderExecutionConfig,
    pub effort: Option<String>,
    pub reservation: AdmissionReservation,
}

/// Why Admission refused to reserve a Job.
pub(crate) struct AdmissionRefusal {
    pub failure: Failure,
    pub message: String,
}

enum PreparedTarget {
    Resolved(ResolvedTarget),
    Refused(AdmissionRefusal),
}

/// Inputs shared by every executable origin.
pub(crate) struct AdmissionContext<'a> {
    pub domain: &'a mut DomainRepository,
    pub profile: &'a Profile,
    pub root: &'a Path,
    pub project_id: &'a str,
    pub budget: &'a BudgetConfig,
    pub concurrency: Option<usize>,
    pub governor: Option<&'a super::governor::Governor>,
    pub now: i64,
}

/// Resolve the executable target without reserving an Attempt.
///
/// A refusal is a decision, not a panic: the caller records it on the Job when
/// a Job already exists. No Attempt is created here.
pub(crate) fn resolve_target(
    context: &mut AdmissionContext<'_>,
    job: Option<&Job>,
    spec: &JobSpec,
    target: &AdmissionTarget,
    requirements: placement::Requirements<'_>,
    preferred_provider: Option<&str>,
    preferred_model: Option<&str>,
) -> Result<std::result::Result<ResolvedTarget, AdmissionRefusal>> {
    match prepare_target(
        context,
        job,
        spec,
        target,
        requirements,
        preferred_provider,
        preferred_model,
    )? {
        PreparedTarget::Resolved(resolved) => Ok(Ok(resolved)),
        PreparedTarget::Refused(refusal) => Ok(Err(refusal)),
    }
}

fn prepare_target(
    context: &mut AdmissionContext<'_>,
    job: Option<&Job>,
    spec: &JobSpec,
    target: &AdmissionTarget,
    requirements: placement::Requirements<'_>,
    preferred_provider: Option<&str>,
    preferred_model: Option<&str>,
) -> Result<PreparedTarget> {
    if let Some(job) = job {
        if let Some(existing) = context.domain.admission_reservation(&job.id)? {
            return materialize_reservation(context.profile, existing);
        }
    }
    match target {
        AdmissionTarget::SelectCandidate => {
            match placement::choose(
                context.domain,
                placement::PolicyInput {
                    profile: context.profile,
                    root: context.root,
                    project_id: context.project_id,
                    job_id: job.map(|job| job.id.as_str()),
                    spec,
                    requirements,
                    preferred_provider,
                    preferred_model,
                    budget: context.budget,
                    concurrency: context.concurrency,
                    governor: context.governor,
                    now: context.now,
                },
            )? {
                Ok(result) => {
                    // Store placement evidence if we have a job
                    if let Some(job) = job {
                        context
                            .domain
                            .record_placement_evidence(&job.id, &result.evidence)?;
                    }
                    materialize_choice(
                        context.profile,
                        result.choice,
                        None,
                        AdmissionPickup::Automatic,
                    )
                }
                Err(failure) => Ok(PreparedTarget::Refused(AdmissionRefusal {
                    message: format!("{}: {}", failure.code, failure.message),
                    failure,
                })),
            }
        }
        AdmissionTarget::Exact(exact) => match validate_exact(context.profile, exact)? {
            Ok(()) => materialize_choice(
                context.profile,
                exact.choice(),
                Some(exact.clone()),
                AdmissionPickup::Explicit,
            ),
            Err(failure) => Ok(PreparedTarget::Refused(AdmissionRefusal {
                message: failure.message.clone(),
                failure,
            })),
        },
    }
}

fn materialize_reservation(
    profile: &Profile,
    reservation: AdmissionReservation,
) -> Result<PreparedTarget> {
    materialize_choice(
        profile,
        reservation.choice.clone(),
        reservation.exact.clone(),
        reservation.pickup,
    )
}

fn materialize_choice(
    profile: &Profile,
    choice: ProviderChoice,
    exact: Option<ExactTarget>,
    pickup: AdmissionPickup,
) -> Result<PreparedTarget> {
    let Some(provider) = profile.providers.get(&choice.provider) else {
        return Ok(PreparedTarget::Refused(AdmissionRefusal {
            message: format!("provider not found: {}", choice.provider),
            failure: job_failure(
                "provider_not_found",
                FailureClass::Validation,
                &format!("provider not found: {}", choice.provider),
                false,
            ),
        }));
    };
    let Some(entry) = profile.models.get(&choice.model) else {
        return Ok(PreparedTarget::Refused(AdmissionRefusal {
            message: format!("model not found: {}", choice.model),
            failure: job_failure(
                "model_not_found",
                FailureClass::Validation,
                &format!("model not found: {}", choice.model),
                false,
            ),
        }));
    };
    if entry.provider != choice.provider || entry.id.is_empty() {
        return Ok(PreparedTarget::Refused(AdmissionRefusal {
            message: format!(
                "model {} is not runnable for provider {}",
                choice.model, choice.provider
            ),
            failure: job_failure(
                "model_provider_mismatch",
                FailureClass::Validation,
                &format!(
                    "model {} is not runnable for provider {}",
                    choice.model, choice.provider
                ),
                false,
            ),
        }));
    }
    let Some(endpoint) = provider
        .endpoint
        .as_deref()
        .filter(|endpoint| !endpoint.is_empty())
        .map(str::to_owned)
    else {
        return Ok(PreparedTarget::Refused(AdmissionRefusal {
            message: format!("provider {} has no endpoint configured", choice.provider),
            failure: job_failure(
                "missing_endpoint",
                FailureClass::Validation,
                &format!("provider {} has no endpoint configured", choice.provider),
                false,
            ),
        }));
    };
    if endpoint_has_userinfo(&endpoint) {
        return Ok(PreparedTarget::Refused(AdmissionRefusal {
            message: format!(
                "provider {} endpoint must not contain userinfo",
                choice.provider
            ),
            failure: job_failure(
                "invalid_endpoint",
                FailureClass::Validation,
                &format!(
                    "provider {} endpoint must not contain userinfo",
                    choice.provider
                ),
                false,
            ),
        }));
    }
    if let Some(reference) = provider.credential_ref.as_deref() {
        let vault = crate::vault::Vault::user_global()?;
        if vault.get(reference)?.is_none() {
            return Ok(PreparedTarget::Refused(AdmissionRefusal {
                message: format!("credential not found: {reference}"),
                failure: job_failure(
                    "missing_credential",
                    FailureClass::Validation,
                    &format!("credential not found: {reference}"),
                    false,
                ),
            }));
        }
    }
    Ok(PreparedTarget::Resolved(ResolvedTarget {
        protocol: provider.wire_protocol(),
        config: ProviderExecutionConfig {
            provider_key: choice.provider.clone(),
            model: choice.model.clone(),
            upstream_model_id: entry.id.clone(),
            endpoint,
            credential_ref: provider.credential_ref.clone(),
        },
        effort: exact.as_ref().and_then(|exact| exact.effort.clone()),
        reservation: AdmissionReservation {
            choice,
            pickup,
            exact,
        },
    }))
}

/// Validate an exact target the way a probe must be validated: the named tuple
/// is either executable as specified, or it is not. Placement is not consulted.
fn validate_exact(
    profile: &Profile,
    exact: &ExactTarget,
) -> Result<std::result::Result<(), Failure>> {
    let Some(provider) = profile.providers.get(&exact.provider) else {
        return Ok(Err(super::health_probe::unsupported_target(
            "health_probe_unknown_provider",
            format!("provider not found: {}", exact.provider),
        )));
    };
    if !provider.wire_protocol().is_openai_chat_completions() {
        return Ok(Err(super::health_probe::unsupported_target(
            "health_probe_unsupported_protocol",
            format!(
                "provider {} does not speak the OpenAI chat-completions protocol",
                exact.provider
            ),
        )));
    }
    let Some(entry) = profile.models.get(&exact.model) else {
        return Ok(Err(super::health_probe::unsupported_target(
            "health_probe_unknown_model",
            format!("model not found: {}", exact.model),
        )));
    };
    if entry.provider != exact.provider {
        return Ok(Err(super::health_probe::unsupported_target(
            "health_probe_model_provider_mismatch",
            format!(
                "model {} is not runnable for provider {}",
                exact.model, exact.provider
            ),
        )));
    }
    if let Some(effort) = exact.effort.as_deref() {
        if !placement::supports_effort(entry, effort) {
            return Ok(Err(super::health_probe::unsupported_target(
                "health_probe_unsupported_effort",
                format!(
                    "model {} does not support reasoning effort {}",
                    exact.model, effort
                ),
            )));
        }
        if !super::health_probe::PROBE_EFFORTS.contains(&effort) {
            return Ok(Err(super::health_probe::unsupported_target(
                "health_probe_unsupported_effort",
                format!("unsupported reasoning effort: {effort}"),
            )));
        }
    }
    let Some(endpoint) = provider
        .endpoint
        .as_deref()
        .filter(|endpoint| !endpoint.is_empty())
    else {
        return Ok(Err(super::health_probe::unsupported_target(
            "health_probe_missing_endpoint",
            format!("provider {} has no endpoint configured", exact.provider),
        )));
    };
    if endpoint_has_userinfo(endpoint) {
        return Ok(Err(super::health_probe::unsupported_target(
            "health_probe_missing_endpoint",
            format!(
                "provider {} endpoint must not contain userinfo",
                exact.provider
            ),
        )));
    }
    if let Some(reference) = provider.credential_ref.as_deref() {
        let vault = crate::vault::Vault::user_global()?;
        if vault.get(reference)?.is_none() {
            return Ok(Err(super::health_probe::unsupported_target(
                "health_probe_missing_credential",
                format!("credential not found: {reference}"),
            )));
        }
    }
    Ok(Ok(()))
}

fn endpoint_has_userinfo(endpoint: &str) -> bool {
    endpoint.split_once("://").is_some_and(|(_, rest)| {
        rest.split(['/', '?', '#'])
            .next()
            .unwrap_or("")
            .contains('@')
    })
}

/// Reserve an eligible Job: freeze the target and claim its Attempt and Executor.
///
/// This is the transition from "a Job exists" to "this Job has been validly
/// reserved". Payload construction happens after this returns, because Chat
/// needs the Attempt identity to persist its turn. [`publish_call`] is the only
/// way that reserved Attempt becomes a provider Call.
pub(crate) fn reserve(
    context: &mut AdmissionContext<'_>,
    job: &Job,
    resolved: &ResolvedTarget,
) -> Result<std::result::Result<ReservedExecution, AdmissionRefusal>> {
    if resolved.reservation.pickup == AdmissionPickup::Explicit
        && resolved.reservation.exact.is_none()
    {
        return Err(invalid("explicit admission requires a frozen exact target"));
    }
    if job.state == JobState::Running {
        let reserved = resume_reserved(context.domain, job)?;
        // A crash between reservation and the first Call must resume that
        // Attempt, not prepare a second payload for it.
        let calls = context.domain.calls_for_attempt(&reserved.attempt.id)?;
        return Ok(Ok(ReservedExecution {
            attempt: reserved.attempt,
            executor: reserved.executor,
            existing_call: !calls.is_empty(),
        }));
    }
    let quota = placement::quota(
        context.root,
        context.profile,
        &resolved.reservation.choice,
        context.now,
    );
    let placement = match (resolved.reservation.pickup, context.concurrency) {
        // Candidate selection reserves Project capacity in the same transaction
        // that claims the Attempt. Chat still acquires its execution slot in
        // the provider worker, so a missing limit means "do not reserve here",
        // not "admission failed". An exact probe never reserves capacity: it
        // queues behind real work instead of displacing it.
        (AdmissionPickup::Automatic, Some(limit)) => Some(super::domain::AdmissionPlacement {
            limit,
            config: context.budget,
            quota,
        }),
        (AdmissionPickup::Automatic, None) | (AdmissionPickup::Explicit, _) => None,
    };
    match context
        .domain
        .dispatch_job_for_admission(&job.id, &resolved.reservation, placement)
    {
        Ok((attempt, executor)) => Ok(Ok(ReservedExecution {
            attempt,
            executor,
            existing_call: false,
        })),
        Err(error) => Ok(Err(AdmissionRefusal {
            message: error.to_string(),
            failure: job_failure(
                "admission_reservation_failed",
                FailureClass::Unknown,
                &error.to_string(),
                true,
            ),
        })),
    }
}

/// Publish the provider Call for an Attempt that [`reserve`] already owns.
pub(crate) fn publish_call(
    context: &mut AdmissionContext<'_>,
    job: &Job,
    resolved: &ResolvedTarget,
    reserved: &ReservedExecution,
    prepared: PreparedExecution,
    runtime: &crate::orchestration::execution_runtime::ExecutionRuntimeHandle,
) -> Result<std::result::Result<AdmittedExecution, AdmissionRefusal>> {
    let attempt = &reserved.attempt;
    let executor = &reserved.executor;
    let quota = placement::quota(
        context.root,
        context.profile,
        &resolved.reservation.choice,
        context.now,
    );
    let authority = context
        .domain
        .authority(&attempt.id)?
        .ok_or_else(|| invalid("attempt authority disappeared"))?;
    let previous = context
        .domain
        .calls_for_attempt(&attempt.id)?
        .into_iter()
        .next();
    let resume = match previous.as_ref() {
        Some(call) if call.state == "created" => {
            let intent = context
                .domain
                .dispatch_intent(&call.id)?
                .ok_or_else(|| invalid("automatic DispatchIntent disappeared"))?;
            !intent.budget_admitted
        }
        _ => false,
    };
    let mut request = prepared.request;
    if let Some(effort) = resolved
        .reservation
        .exact
        .as_ref()
        .and_then(|exact| exact.effort.as_deref())
        .or(resolved.effort.as_deref())
    {
        if request.get("reasoning_effort").is_none() {
            request["reasoning_effort"] = json!(effort);
        }
    }
    let admission = crate::provider_loop::ProviderCallAdmission {
        domain: context.domain,
        authority: &authority,
        executor_id: &executor.id,
        request,
        config: context.budget,
        quota,
        dispatcher: runtime.provider_dispatcher(),
        provider_config: resolved.config.clone(),
        protocol: resolved.protocol,
    };
    let admitted = if let Some(call) = previous {
        if resume {
            crate::provider_loop::resume_provider_call_admission(admission, call)
        } else if call.state == "failed" {
            Err(invalid(
                "automatic provider Call admission previously failed",
            ))
        } else {
            Ok(call)
        }
    } else {
        match prepared.payload {
            PayloadMode::Agent => crate::provider_loop::admit_provider_call_with_events(
                admission,
                prepared.event_sender,
                prepared.cancelled,
            )
            .map(|(call, _)| call),
            PayloadMode::Probe => crate::provider_loop::admit_health_probe_call(admission),
        }
    };
    match admitted {
        Ok(call) => {
            context.domain.complete_automatic_admission(&job.id)?;
            Ok(Ok(AdmittedExecution { call_id: call.id }))
        }
        Err(error) => {
            context.domain.discard_unaccepted_chat_turn(&attempt.id)?;
            if context.domain.authority(&attempt.id)?.is_some() {
                let code = match prepared.payload {
                    PayloadMode::Probe => "health_probe_economic_admission",
                    PayloadMode::Agent => "economic_admission_failed",
                };
                let class = match prepared.payload {
                    PayloadMode::Probe => FailureClass::Budget,
                    PayloadMode::Agent => FailureClass::Unknown,
                };
                context.domain.fail_attempt(
                    &attempt.id,
                    &job_failure(code, class, &error.to_string(), true),
                )?;
            }
            Ok(Err(AdmissionRefusal {
                message: format!("economic admission failed: {error}"),
                failure: job_failure(
                    "economic_admission_failed",
                    FailureClass::Budget,
                    &error.to_string(),
                    true,
                ),
            }))
        }
    }
}

pub(crate) struct ReservedExecution {
    pub attempt: Attempt,
    pub executor: Executor,
    /// True when this Attempt already owns a Call. The origin must not prepare
    /// another payload; [`publish_call`] resumes the existing one.
    pub existing_call: bool,
}

pub(crate) struct AdmittedExecution {
    pub call_id: String,
}

fn resume_reserved(domain: &DomainRepository, job: &Job) -> Result<ReservedExecution> {
    let attempt_id = job
        .authoritative_attempt_id
        .as_deref()
        .ok_or_else(|| invalid("automatic admission lost Attempt authority"))?;
    let attempt = domain
        .attempt(attempt_id)?
        .ok_or_else(|| invalid("automatic Attempt disappeared"))?;
    let executor = domain
        .executor_for_attempt(attempt_id)?
        .ok_or_else(|| invalid("automatic Executor disappeared"))?;
    if executor.kind != "provider" {
        domain.complete_automatic_admission(&job.id)?;
        return Err(invalid("Job was admitted by another executor"));
    }
    Ok(ReservedExecution {
        attempt,
        executor,
        existing_call: false,
    })
}
