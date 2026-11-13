use std::future::Future;
use std::time::Duration;

use crate::db::entities::{
    AllocationPhase, AllocationToken, AppendedOutput, DevelopmentFreshReservation,
    DevelopmentReservation, DevelopmentReservationAttempt, ExecutionPoolId, ExecutorId, JobId,
    MutableJobPhase, MutableJobRecord, OutcomeKind, PoolAllocation, ReadySelection,
};
use crate::db::{DatabaseError, DatabaseHandle, DevelopmentHandoff, PreparedDevelopmentHandoff};
use crate::service::ai::providers::CancellationToken;

const MAX_RESERVATION_PAGES: usize = 64;

#[derive(Clone, Copy, Debug)]
pub(crate) struct Target {
    pub(crate) job: JobId,
    pub(crate) pool: ExecutionPoolId,
    pub(crate) executor: ExecutorId,
}

#[derive(Debug)]
pub(crate) enum Stop {
    Refused { code: &'static str, detail: String },
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

#[derive(Debug)]
pub(crate) struct ExecutionReport {
    pub(crate) job: JobId,
    pub(crate) allocation_phase: AllocationPhase,
    pub(crate) started: bool,
    pub(crate) outcome: Option<OutcomeKind>,
    pub(crate) outcome_code: Option<String>,
    pub(crate) exchange_returned_result: bool,
    pub(crate) exchange_detail: String,
}

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

pub(crate) async fn checked_start(
    database: &DatabaseHandle,
    token: AllocationToken,
    guard: impl FnOnce(&PreparedDevelopmentHandoff) -> Result<(), &'static str>,
) -> Result<DevelopmentHandoff, Stop> {
    match database
        .checked_development_handoff_guarded(token, &crate::db::NoTooling, guard)
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
