//! Fleet Watchdog: one reconciliation pass over the existing execution ontology.
//!
//! Watchdog is not a scheduler, a heartbeat, or a second owner of work. It
//! reads non-terminal Jobs, classifies them from durable authority and live
//! process ownership, and recovers only by calling the operations that already
//! fence an Attempt. Elapsed time is never a stall. A provider Call that this
//! process still owns is alive for as long as the HTTP transport deadline
//! allows, even when no byte has arrived.
//!
//! Rate and capacity denial are Governor facts. They stay on the Governor and
//! are never rewritten here, and they are never treated as a disappeared
//! executor.

use crate::core_contract::{Actor, Failure, FailureClass};
use crate::error::{OcgError, Result};
use crate::orchestration::domain::{
    job_failure, AttemptState, DispatchIntent, DomainRepository, EffectIntentState, JobState,
    WatchdogActionRecord, WatchdogObservation,
};
use crate::orchestration::execution_runtime::ExecutionRuntimeHandle;
use serde_json::{json, Value};

/// How often one runtime reconciles. This is deliberately slower than admission
/// pickup: Watchdog must not become a second 100ms scheduler.
pub(crate) const WATCHDOG_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

/// A classification that does not mutate anything.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WatchdogClass {
    /// The provider worker still holds this Attempt's cancellation token.
    Alive { attempt_id: String },
    /// Admission has not finished publishing the Call. That is the admission
    /// worker's crash window, not a disappeared executor.
    AdmissionPending { attempt_id: String },
    /// Governor denied rate or capacity. The denial is already recorded; this
    /// pass must not invent a cooldown or edit a counter.
    GovernorDenied {
        attempt_id: String,
        decision: String,
    },
    /// This process no longer owns a Call whose external effect may have started.
    Disappeared {
        attempt_id: String,
        generation: u64,
        call_id: Option<String>,
        executor_id: Option<String>,
    },
    /// Cancellation was requested and this process can no longer signal a stop.
    CancelUnconfirmed { attempt_id: String },
    /// Nothing actionable. Includes terminal rows and work still queued for an
    /// owner that has not disappeared.
    Healthy,
}

/// What recovery did, after classification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WatchdogRecovery {
    None,
    Fenced {
        attempt_id: String,
    },
    /// The Attempt was fenced and the Job returned to admission. No replacement
    /// Attempt exists yet; admission creates one when it places the Job.
    Readmitted {
        attempt_id: String,
    },
    CancelConfirmed {
        attempt_id: String,
        stopped: bool,
    },
}

/// One observed Job and the classification that was computed without mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WatchdogFinding {
    pub job_id: String,
    pub class: WatchdogClass,
}

/// Classify one observation.
///
/// `owned` is true only when this process's provider dispatcher still holds a
/// live cancellation token for the authoritative Attempt. A missing token is
/// disappearance, not a timeout. A present token is life, not progress.
pub(crate) fn classify(observation: &WatchdogObservation, owned: bool) -> WatchdogClass {
    let Some(attempt) = observation.attempt.as_ref() else {
        return WatchdogClass::Healthy;
    };
    if !matches!(
        attempt.state,
        AttemptState::Queued | AttemptState::Running | AttemptState::Cancelling
    ) {
        return WatchdogClass::Healthy;
    }
    if attempt.state == AttemptState::Cancelling || observation.job.state == JobState::Cancelling {
        return if owned {
            WatchdogClass::Alive {
                attempt_id: attempt.id.clone(),
            }
        } else {
            WatchdogClass::CancelUnconfirmed {
                attempt_id: attempt.id.clone(),
            }
        };
    }
    if owned {
        // A live token is ownership. Rate or capacity denial recorded against
        // that Attempt is a Governor fact, not a stall, and it is not edited here.
        if let Some(decision) = governor_denial(observation) {
            return WatchdogClass::GovernorDenied {
                attempt_id: attempt.id.clone(),
                decision,
            };
        }
        return WatchdogClass::Alive {
            attempt_id: attempt.id.clone(),
        };
    }
    let live_call = observation.calls.iter().find(|call| {
        matches!(call.state.as_str(), "created" | "running")
            && call.generation == attempt.generation
    });
    let live_intent = observation.intents.iter().find(|intent| {
        intent.attempt_id == attempt.id
            && intent.generation == attempt.generation
            && matches!(intent.state.as_str(), "pending" | "queued" | "running")
    });
    let effect_started = live_intent.is_some_and(intent_effect_may_have_started)
        || live_call.is_some_and(|call| call.state == "running");
    if !effect_started {
        if let Some(decision) = governor_denial(observation) {
            return WatchdogClass::GovernorDenied {
                attempt_id: attempt.id.clone(),
                decision,
            };
        }
    }
    if effect_started {
        // The external effect may already have left this process, and nothing
        // here still owns the Call. That is disappearance. Elapsed time is not
        // consulted: a live token above already returned Alive.
        return WatchdogClass::Disappeared {
            attempt_id: attempt.id.clone(),
            generation: attempt.generation,
            call_id: live_call
                .map(|call| call.id.clone())
                .or_else(|| live_intent.map(|intent| intent.call_id.clone())),
            executor_id: observation
                .executor
                .as_ref()
                .map(|executor| executor.id.clone()),
        };
    }
    // A crash between claiming the Attempt and publishing its Call, or a Call
    // whose effect has not started, belongs to admission and restart recovery.
    // Replacing it here would race the worker that already owns that window.
    if observation.automatic_admission || live_call.is_some() || live_intent.is_some() {
        return WatchdogClass::AdmissionPending {
            attempt_id: attempt.id.clone(),
        };
    }
    WatchdogClass::Healthy
}

fn governor_denial(observation: &WatchdogObservation) -> Option<String> {
    let outcome = observation.acquisition.as_ref()?;
    if outcome.success {
        return None;
    }
    let decision = outcome.governor_decision.as_str();
    if decision.starts_with("rate_limited") || decision == "capacity_unavailable" {
        Some(decision.to_string())
    } else {
        None
    }
}

fn intent_effect_may_have_started(intent: &DispatchIntent) -> bool {
    intent.state == "running" || intent.effect_state != EffectIntentState::NotStarted
}

/// Apply one classification through the existing fences.
///
/// Replacement is idempotent on the expected Attempt: if authority already
/// moved, the domain rejects the replacement and this pass records that fact
/// instead of retrying. Exact probe targets are not re-placed here. Candidate
/// Jobs are fenced and left eligible for the admission worker, which is the
/// only component that runs Placement.
pub(crate) fn recover(
    domain: &mut DomainRepository,
    observation: &WatchdogObservation,
    class: &WatchdogClass,
    runtime: Option<&ExecutionRuntimeHandle>,
) -> Result<WatchdogRecovery> {
    match class {
        WatchdogClass::Alive { .. }
        | WatchdogClass::AdmissionPending { .. }
        | WatchdogClass::GovernorDenied { .. }
        | WatchdogClass::Healthy => Ok(WatchdogRecovery::None),
        WatchdogClass::CancelUnconfirmed { attempt_id } => {
            let stopped = runtime.is_some_and(|runtime| {
                runtime
                    .provider_dispatcher()
                    .cancel_attempt(attempt_id)
                    .unwrap_or(false)
            });
            // `confirm_cancel` is the existing two-phase settlement. A missing
            // token cannot prove the external effect stopped, so an unsignalled
            // cancel stays `unknown` rather than `cancelled`.
            domain.confirm_cancel(attempt_id, stopped)?;
            Ok(WatchdogRecovery::CancelConfirmed {
                attempt_id: attempt_id.clone(),
                stopped,
            })
        }
        WatchdogClass::Disappeared {
            attempt_id,
            generation: _,
            call_id,
            executor_id: _,
        } => fence_disappeared(domain, observation, attempt_id, call_id.as_deref()),
    }
}

fn fence_disappeared(
    domain: &mut DomainRepository,
    observation: &WatchdogObservation,
    attempt_id: &str,
    call_id: Option<&str>,
) -> Result<WatchdogRecovery> {
    let exact = observation
        .reservation
        .as_ref()
        .is_some_and(|reservation| reservation.exact.is_some())
        || observation.job.spec.health_probe.is_some();
    if exact {
        // An exact probe target is frozen. Fence the in-flight Call first so a
        // late result cannot settle it, then fail the Attempt. Operator retry
        // republishes the frozen target; Watchdog does not select a substitute.
        if let Some(call_id) = call_id {
            if let Err(error) =
                domain.fence_dispatch_intent(call_id, "watchdog_executor_disappeared")
            {
                tracing::debug!(%error, call_id, "watchdog fence found the Call already settled");
            }
        }
        domain.fail_attempt(
            attempt_id,
            &failure(
                "watchdog_executor_disappeared",
                "Provider executor disappeared while an exact target was in flight; the frozen target was preserved",
            ),
        )?;
        return Ok(WatchdogRecovery::Fenced {
            attempt_id: attempt_id.to_string(),
        });
    }
    let current = domain
        .authority(attempt_id)?
        .filter(|authority| authority.job_id == observation.job.id);
    if current.is_none() {
        // Authority already moved. Fence any Call this observation still names
        // so a late result cannot settle the disappeared generation.
        if let Some(call_id) = call_id {
            let _ = domain.fence_dispatch_intent(call_id, "watchdog_executor_disappeared");
        }
        return Ok(WatchdogRecovery::Fenced {
            attempt_id: attempt_id.to_string(),
        });
    }
    if domain.chat_session_for_job(&observation.job.id)?.is_some() {
        // A Chat turn runs again only through Chat retry, which replays its
        // frozen request with the Conversation's context; Placement would
        // rebuild it from the objective alone. Its effect may already have
        // left this process, so the Attempt settles as unknown, not failed,
        // and the Job waits for that explicit retry.
        if let Some(call_id) = call_id {
            if let Err(error) =
                domain.fence_dispatch_intent(call_id, "watchdog_executor_disappeared")
            {
                tracing::debug!(%error, call_id, "watchdog fence found the Call already settled");
            }
        }
        match domain.settle_attempt_unknown(
            attempt_id,
            &failure(
                "watchdog_executor_disappeared",
                "Provider executor disappeared during this Chat turn; whether its effect completed is unknown. Retry the turn to run it again",
            ),
        ) {
            Ok(()) => {}
            Err(error) if authority_moved(&error) => {}
            Err(error) => return Err(error),
        }
        return Ok(WatchdogRecovery::Fenced {
            attempt_id: attempt_id.to_string(),
        });
    }
    // Candidate work may be placed again, but only by admission. Fencing this
    // generation and clearing the authority pointer returns the Job to
    // eligibility. The admission worker is the only component that runs
    // Placement. Exact targets are rejected by the domain and fail above.
    domain.complete_automatic_admission(&observation.job.id)?;
    match domain.fence_attempt_for_readmission(&observation.job.id, attempt_id) {
        Ok(()) => Ok(WatchdogRecovery::Readmitted {
            attempt_id: attempt_id.to_string(),
        }),
        Err(error) if authority_moved(&error) => Ok(WatchdogRecovery::Fenced {
            attempt_id: attempt_id.to_string(),
        }),
        Err(error) => Err(error),
    }
}

fn authority_moved(error: &OcgError) -> bool {
    let message = error.to_string();
    message.contains("authority changed")
        || message.contains("already terminal")
        || message.contains("no longer authoritative")
}

fn failure(code: &str, message: &str) -> Failure {
    let mut failure = job_failure(code, FailureClass::Unknown, message, true);
    failure.source = Actor::Core;
    failure
}

/// Observe every non-terminal Job in one repository and recover what classification requires.
pub(crate) fn reconcile_repository(
    domain: &mut DomainRepository,
    runtime: Option<&ExecutionRuntimeHandle>,
) -> Result<Vec<(WatchdogFinding, WatchdogRecovery)>> {
    let observations = domain.watchdog_observations()?;
    let mut results = Vec::new();
    for observation in observations {
        let owned = observation
            .attempt
            .as_ref()
            .and_then(|attempt| {
                runtime.map(|runtime| runtime.provider_dispatcher().owns_attempt(&attempt.id))
            })
            .transpose()?
            .unwrap_or(false);
        let class = classify(&observation, owned);
        let recovery = recover(domain, &observation, &class, runtime)?;
        record(domain, &observation, &class, &recovery)?;
        results.push((
            WatchdogFinding {
                job_id: observation.job.id.clone(),
                class,
            },
            recovery,
        ));
    }
    Ok(results)
}

fn record(
    domain: &mut DomainRepository,
    observation: &WatchdogObservation,
    class: &WatchdogClass,
    recovery: &WatchdogRecovery,
) -> Result<()> {
    let (classification, action, attempt_id, generation, call_id, executor_id, replacement) =
        match (class, recovery) {
            (
                WatchdogClass::Healthy
                | WatchdogClass::Alive { .. }
                | WatchdogClass::GovernorDenied { .. },
                WatchdogRecovery::None,
            ) => return Ok(()),
            (WatchdogClass::AdmissionPending { attempt_id }, WatchdogRecovery::None) => (
                "admission_pending",
                "observe".to_string(),
                Some(attempt_id.clone()),
                observation
                    .attempt
                    .as_ref()
                    .map(|attempt| attempt.generation),
                None,
                observation
                    .executor
                    .as_ref()
                    .map(|executor| executor.id.clone()),
                None,
            ),
            (
                WatchdogClass::CancelUnconfirmed { attempt_id },
                WatchdogRecovery::CancelConfirmed { stopped, .. },
            ) => (
                "cancel_unconfirmed",
                if *stopped {
                    "confirm_stopped"
                } else {
                    "confirm_unknown"
                }
                .to_string(),
                Some(attempt_id.clone()),
                observation
                    .attempt
                    .as_ref()
                    .map(|attempt| attempt.generation),
                None,
                None,
                None,
            ),
            (
                WatchdogClass::Disappeared {
                    attempt_id,
                    generation,
                    call_id,
                    executor_id,
                },
                WatchdogRecovery::Readmitted { .. },
            ) => (
                "executor_disappeared",
                "readmit".to_string(),
                Some(attempt_id.clone()),
                Some(*generation),
                call_id.clone(),
                executor_id.clone(),
                None,
            ),
            (
                WatchdogClass::Disappeared {
                    attempt_id,
                    generation,
                    call_id,
                    executor_id,
                },
                WatchdogRecovery::Fenced { .. },
            ) => (
                "executor_disappeared",
                "fence_attempt".to_string(),
                Some(attempt_id.clone()),
                Some(*generation),
                call_id.clone(),
                executor_id.clone(),
                None,
            ),
            _ => return Ok(()),
        };
    let evidence = evidence_of(observation, class);
    domain.record_watchdog_action(&WatchdogActionRecord {
        job_id: observation.job.id.clone(),
        attempt_id,
        generation,
        call_id,
        executor_id,
        classification: classification.to_string(),
        action: action.to_string(),
        evidence,
        outcome: outcome_of(recovery),
        replacement_attempt_id: replacement,
        created_at: 0,
    })?;
    Ok(())
}

fn evidence_of(observation: &WatchdogObservation, class: &WatchdogClass) -> Value {
    json!({
        "job_state": observation.job.state.to_string(),
        "attempt_state": observation.attempt.as_ref().map(|attempt| attempt.state.to_string()),
        "automatic_admission": observation.automatic_admission,
        "exact_target": observation.reservation.as_ref().and_then(|reservation| reservation.exact.as_ref()).map(|exact| json!({
            "provider": exact.provider,
            "model": exact.model,
            "effort": exact.effort,
        })),
        "health_probe": observation.job.spec.health_probe.is_some(),
        "classification": format!("{class:?}"),
        "governor_decision": observation.acquisition.as_ref().map(|outcome| outcome.governor_decision.clone()),
    })
}

fn outcome_of(recovery: &WatchdogRecovery) -> String {
    match recovery {
        WatchdogRecovery::None => "observed".to_string(),
        WatchdogRecovery::Fenced { .. } => "fenced".to_string(),
        WatchdogRecovery::Readmitted { .. } => "readmitted".to_string(),
        WatchdogRecovery::CancelConfirmed { stopped: true, .. } => "cancel_stopped".to_string(),
        WatchdogRecovery::CancelConfirmed { stopped: false, .. } => "cancel_unknown".to_string(),
    }
}
