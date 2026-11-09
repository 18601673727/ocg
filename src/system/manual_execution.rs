//! The lifecycle shared by the manually initiated, single-request executions
//! of one named native Job: [`crate::system::manual_loopback`] and
//! [`crate::system::external_trial`].
//!
//! Each of them owns only what makes it a different authority: which control
//! permits it, which endpoint it may reach, and which facts it checks before
//! anything is reserved. This module owns the part that must not diverge:
//! reading the Job's standing, reserving exactly the named Job, the checked
//! start, the bounded exchange and the report of what the database shows
//! afterwards. It grants no authority of its own and is reached only through
//! those two callers.
//!
//! There is one reservation, one checked start and one exchange, and nothing
//! here retries, reconciles or releases. An exchange that may have reached
//! the Provider leaves its Allocation held; see
//! [`crate::system::executor::execution_runtime`].

use std::future::Future;
use std::time::Duration;

use crate::db::entities::{
    AllocationPhase, AllocationToken, AppendedOutput, DevelopmentFreshReservation,
    DevelopmentReservation, DevelopmentReservationAttempt, ExecutionPoolId, ExecutorId, JobId,
    MutableJobPhase, MutableJobRecord, OutcomeKind, PoolAllocation, ReadySelection,
};
use crate::db::{DatabaseError, DatabaseHandle, DevelopmentHandoff, PreparedDevelopmentHandoff};
use crate::service::ai::providers::CancellationToken;

/// Ready-index pages one reservation may walk to find its one Job.
const MAX_RESERVATION_PAGES: usize = 64;

/// The Job, Pool and Executor one invocation names. Nothing is selected for
/// the operator.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Target {
    pub(crate) job: JobId,
    pub(crate) pool: ExecutionPoolId,
    pub(crate) executor: ExecutorId,
}

/// Where an invocation stopped, and what that leaves.
#[derive(Debug)]
pub(crate) enum Stop {
    /// Refused before anything was reserved. Nothing changed. `code` is
    /// stable.
    Refused { code: &'static str, detail: String },
    /// A reservation or start failed in a way that does not prove nothing
    /// happened. Ownership may be held; inspect the database with
    /// `ocg inspect-held` before anything else.
    Held { stage: &'static str, detail: String },
}

pub(crate) fn refuse(code: &'static str, detail: impl Into<String>) -> Stop {
    Stop::Refused {
        code,
        detail: detail.into(),
    }
}

pub(crate) fn held(stage: &'static str, error: impl std::fmt::Display) -> Stop {
    Stop::Held {
        stage,
        detail: error.to_string(),
    }
}

/// What the database shows after the exchange returned.
#[derive(Debug)]
pub(crate) struct ExecutionReport {
    pub(crate) job: JobId,
    pub(crate) allocation_phase: AllocationPhase,
    pub(crate) started: bool,
    pub(crate) outcome: Option<OutcomeKind>,
    pub(crate) outcome_code: Option<String>,
    /// Whether this process saw the exchange return a supported result.
    pub(crate) exchange_returned_result: bool,
    pub(crate) exchange_detail: String,
}

/// Reads the named Job, which must be submitted, queued, without an outcome
/// and without an Allocation, and must carry its frozen configuration. Reads
/// only.
pub(crate) async fn read_submitted_job(
    database: &DatabaseHandle,
    job: JobId,
) -> Result<MutableJobRecord, Stop> {
    let record = match database
        .development_trial_job(job)
        .await
        .map_err(|error| held("read_job", error))?
    {
        Some((record, None)) => record,
        Some((_, Some(_))) => {
            return Err(refuse(
                "job_has_allocation",
                "the Job already has an Allocation; a manual execution never adopts or repeats one",
            ));
        }
        None => return Err(refuse("job_not_found", "no such native Job")),
    };
    if record.outcome.is_some()
        || record.job.phase != MutableJobPhase::Queued
        || record.job.submitted_at.is_none()
    {
        return Err(refuse(
            "job_not_submitted_queued",
            "the Job must be submitted, queued and without an outcome",
        ));
    }
    if record.job.frozen.is_none() {
        return Err(refuse(
            "job_not_frozen",
            "the Job has no frozen configuration",
        ));
    }
    Ok(record)
}

/// The Executor must have no owner: there is no competing execution to wait
/// for, and waiting is never an option here.
pub(crate) async fn require_free_executor(
    database: &DatabaseHandle,
    executor: ExecutorId,
) -> Result<(), Stop> {
    if database
        .development_executor_allocation(executor)
        .await
        .map_err(|error| held("read_executor_owner", error))?
        .is_some()
    {
        return Err(refuse(
            "executor_has_owner",
            "the Executor already holds an Allocation; there is no competing execution owner to wait for",
        ));
    }
    Ok(())
}

/// One reservation, of the named Job only: a ready entry of any other Job is
/// never reserved, so the invocation reserves the one Job it names or none.
pub(crate) async fn reserve_named_job(
    database: &DatabaseHandle,
    target: Target,
) -> Result<PoolAllocation, Stop> {
    let mut after = None;
    let mut allocation = None;
    for _ in 0..MAX_RESERVATION_PAGES {
        let fresh = DevelopmentFreshReservation::new(target.pool, target.executor, after);
        let reservation = database
            .reserve_development_job(
                fresh.request(),
                DevelopmentReservationAttempt::New(fresh),
                ReadySelection::Only(target.job),
            )
            .await;
        match reservation {
            Ok(DevelopmentReservation::Reserved(reserved)) => {
                allocation = Some(reserved);
                break;
            }
            Ok(DevelopmentReservation::Continue(cursor)) => after = Some(cursor),
            Ok(DevelopmentReservation::Exhausted) => {
                return Err(refuse(
                    "job_not_reservable",
                    "the Job is not ready for this Pool and Executor",
                ));
            }
            Ok(DevelopmentReservation::Replayed(_) | DevelopmentReservation::Unresolved) => {
                return Err(held("reserve", "an unexpected reservation reply"));
            }
            Err(crate::db::DevelopmentReservationError::DefinitelyNotCommitted {
                error, ..
            }) => {
                return Err(refuse("reservation_refused", error.to_string()));
            }
            Err(crate::db::DevelopmentReservationError::Other(error)) => {
                return Err(held("reserve", error));
            }
        }
    }
    let Some(allocation) = allocation else {
        return Err(refuse(
            "job_not_reservable",
            "the Job was not found in the ready index",
        ));
    };
    if allocation.token.job != target.job {
        return Err(held(
            "reserve",
            "the reservation names another Job; nothing was started",
        ));
    }
    Ok(allocation)
}

/// The checked start, after a last look at the exact prepared request against
/// the real reservation. Past this point a refusal consumes the Job: the
/// caller's preflight is what keeps predictable refusals out of it. A start
/// refused before any request ends the reservation and fails the Job.
pub(crate) async fn checked_start(
    database: &DatabaseHandle,
    token: AllocationToken,
    guard: impl FnOnce(&PreparedDevelopmentHandoff) -> Result<(), &'static str>,
) -> Result<DevelopmentHandoff, Stop> {
    match database
        .checked_development_handoff_guarded(token, guard)
        .await
    {
        Ok(handoff) => Ok(handoff),
        Err(crate::db::DevelopmentHandoffError::NotStarted { cause, error }) => {
            let settlement = database
                .fail_unstarted_development_reservation(token, cause)
                .await;
            Err(held(
                "start",
                format!(
                    "the start was refused before any request ({error}); the reservation was {}",
                    match settlement {
                        Ok(_) => "ended, and the Job failed",
                        Err(_) => "not ended and is still held",
                    }
                ),
            ))
        }
        Err(error) => Err(held("start", format!("{error:?}"))),
    }
}

/// Runs `exchange` with `timeout` as a cancellation of `cancellation`: the
/// deadline is a local stop like the operator's, and either leaves a possibly
/// sent request Uncertain, never released. Neither is a claim about the
/// Provider.
pub(crate) async fn within_deadline<F: Future>(
    cancellation: &CancellationToken,
    timeout: Duration,
    exchange: F,
) -> F::Output {
    let done = CancellationToken::default();
    let timer = ntex::rt::spawn({
        let cancellation = cancellation.clone();
        let done = done.clone();
        async move {
            use ntex::util::{Either, select};
            if let Either::Right(()) = select(done.cancelled(), ntex::time::sleep(timeout)).await {
                cancellation.cancel();
            }
        }
    });
    let output = exchange.await;
    done.cancel();
    drop(timer);
    output
}

/// What the database now shows is the report; the exchange result only
/// explains it.
pub(crate) async fn report(
    database: &DatabaseHandle,
    job: JobId,
    exchange: &Result<AppendedOutput, DatabaseError>,
) -> Result<ExecutionReport, Stop> {
    let (exchange_returned_result, exchange_detail) = match exchange {
        Ok(_) => (
            true,
            "the exchange returned and was recorded; OCG cannot prove the Provider did no other work or incurred no other charge".to_owned(),
        ),
        Err(error) => (false, error.to_string()),
    };
    let (record, allocation) = database
        .development_trial_job(job)
        .await
        .map_err(|error| held("read_result", error))?
        .ok_or_else(|| held("read_result", "the Job vanished"))?;
    let allocation = allocation.ok_or_else(|| held("read_result", "the Allocation vanished"))?;
    Ok(ExecutionReport {
        job,
        allocation_phase: allocation.phase,
        started: allocation.started_at.is_some(),
        outcome: record.outcome.as_ref().map(|outcome| outcome.kind),
        outcome_code: record
            .outcome
            .and_then(|outcome| outcome.cause)
            .map(|cause| format!("{cause:?}")),
        exchange_returned_result,
        exchange_detail,
    })
}

pub(crate) fn phase_name(phase: AllocationPhase) -> &'static str {
    match phase {
        AllocationPhase::Reserved => "reserved",
        AllocationPhase::Running => "running",
        AllocationPhase::StopRequested => "stop_requested",
        AllocationPhase::Uncertain => "uncertain",
        AllocationPhase::Released => "released",
    }
}

pub(crate) fn outcome_name(kind: Option<OutcomeKind>) -> &'static str {
    match kind {
        None => "none",
        Some(OutcomeKind::Completed) => "completed",
        Some(OutcomeKind::Failed) => "failed",
        Some(OutcomeKind::Cancelled) => "cancelled",
        Some(OutcomeKind::Unknown) => "unknown",
    }
}
