use super::{StorageError, TxRead, execution, jobs};
use crate::db::entities::{
    AllocationPhase, DependencyState, ExecutionEvent, ExecutionEventKind, ExecutionSnapshot,
    JobClosing, MutableJobPhase, OutcomeCause, OutcomeRecord, PoolAllocation, ToolCallRef,
    UncertaintyReason,
};
use crate::db::entities::{
    DependencyCondition, ExecutionHistoryItem, ExecutionHistoryPage, JobId, JobState, OutcomeKind,
    RequestNumber, StopProof, Timestamp,
};
use crate::db::redb::batch::{Batch, BatchError};
use crate::db::redb::codec::{Code, Dec, DecodeResult, Enc};
use crate::db::redb::reads::read;
use crate::db::redb::rows::tool_call_key;
use crate::db::redb::schema::{
    EXECUTION_COMMITS, JOB_EVENTS, JOB_INPUTS, JOB_OUTPUTS, JOBS, MAX_PAGE, REQUEST_EVENTS,
    SEQUENCES, TOOL_CALLS,
};
use crate::db::redb::{allocations, reads};
use ::redb::{Database, ReadTransaction, ReadableTable};
use std::collections::BTreeSet;

pub(super) const TAG_EXECUTION_EVENT: u8 = 55;
pub(super) const EVENT_SEQUENCE_KEY: &str = "execution_event";

fn phase_code(phase: MutableJobPhase) -> u8 {
    match phase {
        MutableJobPhase::Unsubmitted => 0,
        MutableJobPhase::Queued => 1,
        MutableJobPhase::Executing => 2,
    }
}

fn decode_phase(value: u8) -> DecodeResult<MutableJobPhase> {
    match value {
        0 => Ok(MutableJobPhase::Unsubmitted),
        1 => Ok(MutableJobPhase::Queued),
        2 => Ok(MutableJobPhase::Executing),
        _ => Err(StorageError::corrupted("invalid execution Job phase")),
    }
}

fn job_phase_transition_allowed(from: MutableJobPhase, to: MutableJobPhase) -> bool {
    match (from, to) {
        (MutableJobPhase::Unsubmitted, MutableJobPhase::Queued)
        | (MutableJobPhase::Queued, MutableJobPhase::Executing) => true,
        (
            MutableJobPhase::Unsubmitted | MutableJobPhase::Queued | MutableJobPhase::Executing,
            MutableJobPhase::Unsubmitted | MutableJobPhase::Queued | MutableJobPhase::Executing,
        ) => false,
    }
}

fn allocation_phase_code(phase: AllocationPhase) -> u8 {
    match phase {
        AllocationPhase::Reserved => 0,
        AllocationPhase::Running => 1,
        AllocationPhase::StopRequested => 2,
        AllocationPhase::Uncertain => 3,
        AllocationPhase::Released => 4,
    }
}

fn decode_allocation_phase(value: u8) -> DecodeResult<AllocationPhase> {
    match value {
        0 => Ok(AllocationPhase::Reserved),
        1 => Ok(AllocationPhase::Running),
        2 => Ok(AllocationPhase::StopRequested),
        3 => Ok(AllocationPhase::Uncertain),
        4 => Ok(AllocationPhase::Released),
        _ => Err(StorageError::corrupted(
            "invalid execution Allocation phase",
        )),
    }
}

fn encode_outcome(e: &mut Enc, outcome: &OutcomeRecord) {
    e.u8(outcome.kind.code());
    e.ts(outcome.at);
    e.opt(outcome.cause.as_ref(), |e, cause| match cause {
        OutcomeCause::Code(value) => {
            e.u8(1);
            e.u8(*value);
        }
        OutcomeCause::Text(value) => {
            e.u8(2);
            e.str(value);
        }
        OutcomeCause::Abandoned => e.u8(3),
        OutcomeCause::DependencyImpossible => e.u8(4),
        OutcomeCause::Stopped { record } => {
            e.u8(5);
            e.u32(*record);
        }
        OutcomeCause::Closed => e.u8(6),
        OutcomeCause::Uncertain(reason) => {
            e.u8(7);
            e.u8(reason.code());
        }
    });
}

fn decode_outcome(d: &mut Dec<'_>) -> DecodeResult<OutcomeRecord> {
    let kind = d.code(OutcomeKind::from_code)?;
    let at = d.ts()?;
    let cause = d.opt(|d| {
        Ok(match d.u8()? {
            1 => OutcomeCause::Code(d.u8()?),
            2 => OutcomeCause::Text(d.str()?),
            3 => OutcomeCause::Abandoned,
            4 => OutcomeCause::DependencyImpossible,
            5 => OutcomeCause::Stopped { record: d.u32()? },
            6 => OutcomeCause::Closed,
            7 => OutcomeCause::Uncertain(d.code(UncertaintyReason::from_code)?),
            _ => return Err(StorageError::corrupted("invalid execution outcome cause")),
        })
    })?;
    Ok(OutcomeRecord { kind, at, cause })
}

fn encode_event(event: &ExecutionEvent) -> DecodeResult<Vec<u8>> {
    let mut e = Enc::new(TAG_EXECUTION_EVENT);
    e.u128(event.job.get());
    e.u64(event.seq);
    e.u64(event.previous_seq);
    e.u64(event.transaction);
    e.ts(event.at);
    match &event.kind {
        ExecutionEventKind::Created { phase } => {
            e.u8(0);
            e.opt(phase.as_ref(), |e, phase| e.u8(phase_code(*phase)));
        }
        ExecutionEventKind::PhaseChanged { phase } => {
            e.u8(1);
            e.u8(phase_code(*phase));
        }
        ExecutionEventKind::AllocationReserved { allocation } => {
            e.u8(2);
            let bytes = allocations::encode_pool_allocation(allocation);
            e.u32(
                u32::try_from(bytes.len())
                    .map_err(|_| StorageError::corrupted("Job reservation record is too large"))?,
            );
            e.0.extend_from_slice(&bytes);
        }
        ExecutionEventKind::AllocationStarted => e.u8(3),
        ExecutionEventKind::AllocationPhaseChanged { phase } => {
            e.u8(4);
            e.u8(allocation_phase_code(*phase));
        }
        ExecutionEventKind::AllocationStopConfirmed { at, proof } => {
            e.u8(5);
            e.ts(*at);
            e.u8(proof.code());
        }
        ExecutionEventKind::JobOutcome { outcome } => {
            e.u8(6);
            encode_outcome(&mut e, outcome);
        }
        ExecutionEventKind::Closing { at, by, running } => {
            e.u8(8);
            e.ts(*at);
            e.actor(by);
            e.bool(*running);
        }
        ExecutionEventKind::DependencyAdded {
            prerequisite,
            condition,
        } => {
            e.u8(9);
            e.u128(prerequisite.get());
            e.u8(condition.code());
        }
        ExecutionEventKind::DependencyResolved {
            prerequisite,
            state,
        } => {
            e.u8(10);
            e.u128(prerequisite.get());
            e.u8(match state {
                DependencyState::Waiting => 0,
                DependencyState::Satisfied => 1,
                DependencyState::Impossible => 2,
            });
        }
        ExecutionEventKind::RequestRecorded { request, seq } => {
            e.u8(11);
            e.u32(request.get());
            e.u32(*seq);
        }
        ExecutionEventKind::ToolRecorded { call, completed } => {
            e.u8(12);
            e.u128(call.job.get());
            e.u32(call.request.get());
            e.u32(call.index);
            e.bool(*completed);
        }
        ExecutionEventKind::OutputRecorded { seq } => {
            e.u8(13);
            e.u32(*seq);
        }
        ExecutionEventKind::ClosingRunningChanged { running } => {
            e.u8(14);
            e.bool(*running);
        }
        ExecutionEventKind::HoldChanged { reason } => {
            e.u8(15);
            e.ou32(*reason);
        }
        ExecutionEventKind::VisibilityChanged { at } => {
            e.u8(16);
            e.opt(at.as_ref(), |e, at| e.ts(*at));
        }
        ExecutionEventKind::PrerequisitesChanged { after } => {
            e.u8(17);
            e.u8(u8::try_from(after.len())
                .map_err(|_| StorageError::corrupted("too many execution prerequisites"))?);
            for (job, condition) in after {
                e.u128(job.get());
                e.u8(condition.code());
            }
        }
        ExecutionEventKind::InputRevisionRecorded { revision } => {
            e.u8(18);
            e.u64(*revision);
        }
    }
    Ok(e.0)
}

fn reservation_is_valid(allocation: &PoolAllocation, job: JobId, at: Timestamp) -> bool {
    allocation.token.job == job
        && allocation.phase == AllocationPhase::Reserved
        && allocation.claimed_at == at
        && allocation.started_at.is_none()
        && allocation.stop_confirmed_at.is_none()
        && allocation.stop_proof.is_none()
}

pub(crate) fn decode_event(key: (u128, u64), bytes: &[u8]) -> DecodeResult<ExecutionEvent> {
    let mut d = Dec::new(bytes, TAG_EXECUTION_EVENT)?;
    let job = JobId::new(d.u128()?);
    let seq = d.u64()?;
    let previous_seq = d.u64()?;
    let transaction = d.u64()?;
    let at = d.ts()?;
    let kind = match d.u8()? {
        0 => ExecutionEventKind::Created {
            phase: d.opt(|d| decode_phase(d.u8()?))?,
        },
        1 => ExecutionEventKind::PhaseChanged {
            phase: decode_phase(d.u8()?)?,
        },
        2 => {
            let length = d.u32()? as usize;
            let allocation = allocations::decode_pool_allocation(job.get(), d.take(length)?)?;
            if !reservation_is_valid(&allocation, job, at) {
                return Err(StorageError::corrupted("invalid Job reservation event"));
            }
            ExecutionEventKind::AllocationReserved {
                allocation: Box::new(allocation),
            }
        }
        3 => ExecutionEventKind::AllocationStarted,
        4 => ExecutionEventKind::AllocationPhaseChanged {
            phase: decode_allocation_phase(d.u8()?)?,
        },
        5 => ExecutionEventKind::AllocationStopConfirmed {
            at: d.ts()?,
            proof: d.code(StopProof::from_code)?,
        },
        6 => ExecutionEventKind::JobOutcome {
            outcome: decode_outcome(&mut d)?,
        },
        8 => ExecutionEventKind::Closing {
            at: d.ts()?,
            by: d.actor()?,
            running: d.bool()?,
        },
        9 => ExecutionEventKind::DependencyAdded {
            prerequisite: JobId::new(d.u128()?),
            condition: d.code(DependencyCondition::from_code)?,
        },
        10 => ExecutionEventKind::DependencyResolved {
            prerequisite: JobId::new(d.u128()?),
            state: match d.u8()? {
                0 => DependencyState::Waiting,
                1 => DependencyState::Satisfied,
                2 => DependencyState::Impossible,
                _ => {
                    return Err(StorageError::corrupted(
                        "invalid execution dependency resolution",
                    ));
                }
            },
        },
        11 => ExecutionEventKind::RequestRecorded {
            request: RequestNumber::new(d.u32()?)
                .ok_or_else(|| StorageError::corrupted("zero execution request"))?,
            seq: d.u32()?,
        },
        12 => ExecutionEventKind::ToolRecorded {
            call: ToolCallRef {
                job: JobId::new(d.u128()?),
                request: RequestNumber::new(d.u32()?)
                    .ok_or_else(|| StorageError::corrupted("zero execution tool request"))?,
                index: d.u32()?,
            },
            completed: d.bool()?,
        },
        13 => ExecutionEventKind::OutputRecorded { seq: d.u32()? },
        14 => ExecutionEventKind::ClosingRunningChanged { running: d.bool()? },
        15 => ExecutionEventKind::HoldChanged { reason: d.ou32()? },
        16 => ExecutionEventKind::VisibilityChanged {
            at: d.opt(Dec::ts)?,
        },
        17 => {
            let mut after = Vec::new();
            for _ in 0..d.u8()? {
                after.push((
                    JobId::new(d.u128()?),
                    d.code(DependencyCondition::from_code)?,
                ));
            }
            ExecutionEventKind::PrerequisitesChanged { after }
        }
        18 => ExecutionEventKind::InputRevisionRecorded { revision: d.u64()? },
        _ => return Err(StorageError::corrupted("invalid execution event kind")),
    };
    d.end()?;
    if job.get() != key.0
        || seq != key.1
        || seq == 0
        || previous_seq >= seq
        || transaction == 0
        || transaction > seq
    {
        return Err(StorageError::corrupted(
            "invalid execution event identity or order",
        ));
    }
    Ok(ExecutionEvent {
        job,
        seq,
        previous_seq,
        transaction,
        at,
        kind,
    })
}

pub(crate) fn latest_event_seq(
    events: &impl ReadableTable<(u128, u64), &'static [u8]>,
    job: JobId,
) -> DecodeResult<u64> {
    events
        .range((job.get(), 0)..=(job.get(), u64::MAX))?
        .next_back()
        .transpose()?
        .map(|(key, value)| decode_event(key.value(), value.value()).map(|event| event.seq))
        .transpose()
        .map(|seq| seq.unwrap_or(0))
}

pub(crate) fn commit_end(tx: &impl TxRead, transaction: u64) -> DecodeResult<u64> {
    let commits = tx.table(EXECUTION_COMMITS)?;
    let end = commits
        .get(transaction)?
        .ok_or_else(|| StorageError::corrupted("execution event has no committed transaction"))?
        .value();
    if end < transaction {
        return Err(StorageError::corrupted(
            "invalid execution transaction boundary",
        ));
    }
    Ok(end)
}

pub(crate) fn latest_seq(tx: &impl TxRead, job: JobId) -> DecodeResult<u64> {
    let events = tx.table(JOB_EVENTS)?;
    let last = events
        .range((job.get(), 0)..=(job.get(), u64::MAX))?
        .next_back()
        .transpose()?;
    match last {
        Some((key, value)) => {
            let event = decode_event(key.value(), value.value())?;
            let end = commit_end(tx, event.transaction)?;
            if event.seq > end {
                return Err(StorageError::corrupted(
                    "execution event exceeds its transaction",
                ));
            }
            Ok(end)
        }
        None => Ok(0),
    }
}

fn allocation_phase_transition_allowed(from: AllocationPhase, to: AllocationPhase) -> bool {
    match (from, to) {
        (AllocationPhase::Reserved, AllocationPhase::Released | AllocationPhase::Uncertain)
        | (
            AllocationPhase::Running,
            AllocationPhase::StopRequested | AllocationPhase::Released | AllocationPhase::Uncertain,
        )
        | (
            AllocationPhase::StopRequested,
            AllocationPhase::Released | AllocationPhase::Uncertain,
        )
        | (AllocationPhase::Uncertain, AllocationPhase::Released) => true,
        (
            AllocationPhase::Reserved
            | AllocationPhase::Running
            | AllocationPhase::StopRequested
            | AllocationPhase::Uncertain
            | AllocationPhase::Released,
            AllocationPhase::Reserved
            | AllocationPhase::Running
            | AllocationPhase::StopRequested
            | AllocationPhase::Uncertain
            | AllocationPhase::Released,
        ) => false,
    }
}

fn apply_allocation_event(
    allocation: &mut PoolAllocation,
    event: &ExecutionEvent,
) -> DecodeResult<()> {
    if event.job != allocation.token.job {
        return Err(StorageError::corrupted(
            "Allocation event names another Job",
        ));
    }
    match &event.kind {
        ExecutionEventKind::AllocationStarted => {
            if allocation.phase != AllocationPhase::Reserved || allocation.started_at.is_some() {
                return Err(StorageError::corrupted(
                    "Job execution started more than once",
                ));
            }
            allocation.started_at = Some(event.at);
            allocation.phase = AllocationPhase::Running;
        }
        ExecutionEventKind::AllocationPhaseChanged { phase } => {
            if !allocation_phase_transition_allowed(allocation.phase, *phase) {
                return Err(StorageError::corrupted(
                    "invalid historical Allocation transition",
                ));
            }
            allocation.phase = *phase;
        }
        ExecutionEventKind::AllocationStopConfirmed { at, proof } => {
            if allocation.phase != AllocationPhase::Released || allocation.stop_proof.is_some() {
                return Err(StorageError::corrupted(
                    "invalid historical Allocation stop proof",
                ));
            }
            allocation.stop_confirmed_at = Some(*at);
            allocation.stop_proof = Some(*proof);
        }
        ExecutionEventKind::Created { .. }
        | ExecutionEventKind::PhaseChanged { .. }
        | ExecutionEventKind::InputRevisionRecorded { .. }
        | ExecutionEventKind::AllocationReserved { .. }
        | ExecutionEventKind::JobOutcome { .. }
        | ExecutionEventKind::Closing { .. }
        | ExecutionEventKind::ClosingRunningChanged { .. }
        | ExecutionEventKind::HoldChanged { .. }
        | ExecutionEventKind::VisibilityChanged { .. }
        | ExecutionEventKind::PrerequisitesChanged { .. }
        | ExecutionEventKind::DependencyAdded { .. }
        | ExecutionEventKind::DependencyResolved { .. }
        | ExecutionEventKind::RequestRecorded { .. }
        | ExecutionEventKind::ToolRecorded { .. }
        | ExecutionEventKind::OutputRecorded { .. } => {}
    }
    Ok(())
}

pub(crate) fn replay_job(
    tx: &impl TxRead,
    job: JobId,
    at_seq: Option<u64>,
) -> DecodeResult<Option<ExecutionSnapshot>> {
    let mut snapshot = ExecutionSnapshot::new(job);
    let mut previous = 0u64;
    let cutoff = at_seq.unwrap_or(u64::MAX);
    for entry in tx
        .table(JOB_EVENTS)?
        .range((job.get(), 0)..=(job.get(), cutoff))?
    {
        let (key, value) = entry?;
        let event = decode_event(key.value(), value.value())?;
        let end = commit_end(tx, event.transaction)?;
        if end > cutoff {
            break;
        }
        if event.seq > end || event.previous_seq != previous {
            return Err(StorageError::corrupted("broken execution history chain"));
        }
        if previous == 0 && !matches!(event.kind, ExecutionEventKind::Created { .. }) {
            return Err(StorageError::corrupted(
                "execution history has no creation event",
            ));
        }
        match &event.kind {
            ExecutionEventKind::Created { phase } => {
                if previous != 0 || *phase == Some(MutableJobPhase::Executing) {
                    return Err(StorageError::corrupted("duplicate Job creation event"));
                }
                snapshot.phase = *phase;
            }
            ExecutionEventKind::PhaseChanged { phase } => {
                if snapshot.outcome.is_some()
                    || snapshot
                        .phase
                        .is_none_or(|previous| !job_phase_transition_allowed(previous, *phase))
                {
                    return Err(StorageError::corrupted(
                        "Job phase changed after settlement",
                    ));
                }
                snapshot.phase = Some(*phase);
            }
            ExecutionEventKind::InputRevisionRecorded { revision } => {
                let expected = snapshot
                    .input_revision
                    .map_or(Some(1), |value| value.checked_add(1));
                if snapshot.phase.is_none()
                    || snapshot.outcome.is_some()
                    || snapshot.allocation.is_some()
                    || expected != Some(*revision)
                {
                    return Err(StorageError::corrupted("invalid execution input revision"));
                }
                let jobs = tx.table(JOBS)?;
                let row = jobs
                    .get(job.get())?
                    .ok_or_else(|| StorageError::corrupted("input revision has no Job"))?;
                let current = jobs::decode_mutable_job(job.get(), row.value())?;
                if current.revision < *revision {
                    return Err(StorageError::corrupted(
                        "input revision exceeds its current Job",
                    ));
                }
                if current.revision != *revision {
                    let revision = u32::try_from(*revision).map_err(|_| {
                        StorageError::corrupted("historical input revision overflow")
                    })?;
                    let inputs = tx.table(JOB_INPUTS)?;
                    let input = inputs.get((job.get(), revision))?.ok_or_else(|| {
                        StorageError::corrupted("historical input revision is missing")
                    })?;
                    jobs::decode_mutable_input_revision(job, revision, input.value())?;
                }
                snapshot.input_revision = Some(*revision);
            }
            ExecutionEventKind::AllocationReserved { allocation } => {
                if snapshot.outcome.is_some()
                    || snapshot.phase != Some(MutableJobPhase::Queued)
                    || !snapshot.dependencies.is_empty()
                    || snapshot.allocation.is_some()
                    || !reservation_is_valid(allocation, job, event.at)
                    || snapshot.input_revision != Some(allocation.job_revision)
                {
                    return Err(StorageError::corrupted(
                        "Job reservation conflicts with its history",
                    ));
                }
                snapshot.allocation = Some(allocation.as_ref().clone());
            }
            ExecutionEventKind::AllocationStarted
            | ExecutionEventKind::AllocationPhaseChanged { .. }
            | ExecutionEventKind::AllocationStopConfirmed { .. } => {
                if matches!(event.kind, ExecutionEventKind::AllocationStarted)
                    && snapshot.outcome.is_some()
                {
                    return Err(StorageError::corrupted("settled Job cannot start"));
                }
                let allocation = snapshot.allocation.as_mut().ok_or_else(|| {
                    StorageError::corrupted("Allocation transition has no Job reservation")
                })?;
                apply_allocation_event(allocation, &event)?;
            }
            ExecutionEventKind::JobOutcome { outcome } => {
                if snapshot.outcome.is_some() {
                    return Err(StorageError::corrupted(
                        "Job outcome recorded more than once",
                    ));
                }
                snapshot.outcome = Some(outcome.clone());
            }
            ExecutionEventKind::Closing { at, by, running } => {
                if snapshot.closing.is_some() {
                    return Err(StorageError::corrupted(
                        "Job closing recorded more than once",
                    ));
                }
                snapshot.closing = Some(JobClosing {
                    at: *at,
                    by: by.clone(),
                    running: *running,
                });
            }
            ExecutionEventKind::ClosingRunningChanged { running } => {
                let closing = snapshot.closing.as_mut().ok_or_else(|| {
                    StorageError::corrupted("closing state changed before closing")
                })?;
                if closing.running == *running {
                    return Err(StorageError::corrupted("duplicate closing state change"));
                }
                closing.running = *running;
            }
            ExecutionEventKind::HoldChanged { reason } => {
                if snapshot.on_hold == *reason {
                    return Err(StorageError::corrupted("duplicate Job hold change"));
                }
                snapshot.on_hold = *reason;
            }
            ExecutionEventKind::VisibilityChanged { at } => {
                if snapshot.hidden_at == *at {
                    return Err(StorageError::corrupted("duplicate Job visibility change"));
                }
                snapshot.hidden_at = *at;
            }
            ExecutionEventKind::PrerequisitesChanged { after } => {
                let mut seen = BTreeSet::new();
                if snapshot.prerequisites == *after
                    || after.iter().any(|(id, _)| *id == job || !seen.insert(*id))
                {
                    return Err(StorageError::corrupted("invalid prerequisite change"));
                }
                snapshot.prerequisites = after.clone();
            }
            ExecutionEventKind::DependencyAdded {
                prerequisite,
                condition,
            } => {
                if *prerequisite == job
                    || snapshot
                        .dependencies
                        .iter()
                        .any(|(id, _)| id == prerequisite)
                {
                    return Err(StorageError::corrupted(
                        "invalid execution dependency event",
                    ));
                }
                snapshot.dependencies.push((*prerequisite, *condition));
            }
            ExecutionEventKind::DependencyResolved {
                prerequisite,
                state,
            } => {
                if *state == DependencyState::Waiting {
                    return Err(StorageError::corrupted(
                        "waiting dependency cannot be resolved",
                    ));
                }
                let position = snapshot
                    .dependencies
                    .iter()
                    .position(|(id, _)| id == prerequisite)
                    .ok_or_else(|| StorageError::corrupted("resolution has no dependency"))?;
                snapshot.dependencies.remove(position);
            }
            ExecutionEventKind::RequestRecorded { request, seq } => {
                if snapshot.outcome.is_some()
                    || snapshot.allocation.as_ref().is_none_or(|allocation| {
                        !matches!(
                            allocation.phase,
                            AllocationPhase::Running | AllocationPhase::StopRequested
                        )
                    })
                    || *seq == 0
                    || tx
                        .table(REQUEST_EVENTS)?
                        .get((job.get(), request.get(), *seq))?
                        .is_none()
                {
                    return Err(StorageError::corrupted(
                        "request event has no open Job or request fact",
                    ));
                }
            }
            ExecutionEventKind::ToolRecorded { call, .. } => {
                if snapshot.outcome.is_some()
                    || snapshot.allocation.as_ref().is_none_or(|allocation| {
                        !matches!(
                            allocation.phase,
                            AllocationPhase::Running | AllocationPhase::StopRequested
                        )
                    })
                    || call.job != job
                    || tx.table(TOOL_CALLS)?.get(tool_call_key(*call))?.is_none()
                {
                    return Err(StorageError::corrupted(
                        "tool event has no open Job or tool fact",
                    ));
                }
            }
            ExecutionEventKind::OutputRecorded { seq } => {
                if *seq == 0 || tx.table(JOB_OUTPUTS)?.get((job.get(), *seq))?.is_none() {
                    return Err(StorageError::corrupted("missing execution output"));
                }
            }
        }
        previous = event.seq;
        snapshot.seq = end;
    }
    Ok((previous != 0).then_some(snapshot))
}

impl Batch {
    pub(super) fn record_execution_event(
        &mut self,
        job: JobId,
        kind: ExecutionEventKind,
    ) -> Result<u64, BatchError> {
        let now = self.now;
        let transaction = self.execution_transaction;
        let (seq, first) = self.step(|tx| {
            let previous_seq = latest_event_seq(&tx.open_table(JOB_EVENTS)?, job)?;
            if (previous_seq == 0) != matches!(kind, ExecutionEventKind::Created { .. }) {
                return Err(StorageError::corrupted(
                    "execution creation event ordering is invalid",
                ));
            }
            let mut sequences = tx.open_table(SEQUENCES)?;
            let current = sequences
                .get(EVENT_SEQUENCE_KEY)?
                .map(|row| row.value())
                .unwrap_or(0);
            let seq = u64::try_from(current)
                .ok()
                .and_then(|value| value.checked_add(1))
                .ok_or_else(|| StorageError::corrupted("execution event sequence exhausted"))?;
            let first = transaction.unwrap_or(seq);
            let event = ExecutionEvent {
                job,
                seq,
                previous_seq,
                transaction: first,
                at: now,
                kind,
            };
            let mut events = tx.open_table(JOB_EVENTS)?;
            if events
                .insert((job.get(), seq), encode_event(&event)?.as_slice())?
                .is_some()
            {
                return Err(StorageError::corrupted(
                    "execution event would overwrite history",
                ));
            }
            tx.open_table(EXECUTION_COMMITS)?.insert(first, seq)?;
            sequences.insert(EVENT_SEQUENCE_KEY, u128::from(seq))?;
            Ok((seq, first))
        })?;
        self.execution_transaction = Some(first);
        Ok(seq)
    }

    pub(super) fn record_allocation_transition(
        &mut self,
        previous: Option<&PoolAllocation>,
        next: &PoolAllocation,
    ) -> Result<(), BatchError> {
        let job = next.token.job;
        let Some(previous) = previous else {
            if !reservation_is_valid(next, job, self.now) {
                return self.corrupted("invalid initial Job reservation".into());
            }
            self.record_execution_event(
                job,
                ExecutionEventKind::AllocationReserved {
                    allocation: Box::new(next.clone()),
                },
            )?;
            return Ok(());
        };
        if previous.token != next.token
            || previous.request_id != next.request_id
            || previous.request_after != next.request_after
            || previous.job_revision != next.job_revision
            || previous.claimed_at != next.claimed_at
            || previous.host_id != next.host_id
            || previous.boot_id != next.boot_id
            || previous.pool_revision != next.pool_revision
            || previous.executor_revision != next.executor_revision
            || previous.operation != next.operation
            || previous.protocol != next.protocol
        {
            return self.corrupted("Job reservation identity cannot be changed".into());
        }
        if previous.started_at != next.started_at {
            if previous.phase != AllocationPhase::Reserved
                || previous.started_at.is_some()
                || next.started_at != Some(self.now)
                || next.phase != AllocationPhase::Running
            {
                return self.corrupted("Job start evidence cannot be overwritten".into());
            }
            self.record_execution_event(job, ExecutionEventKind::AllocationStarted)?;
        } else if previous.phase != next.phase {
            if !allocation_phase_transition_allowed(previous.phase, next.phase) {
                return self.corrupted("invalid Job Allocation transition".into());
            }
            self.record_execution_event(
                job,
                ExecutionEventKind::AllocationPhaseChanged { phase: next.phase },
            )?;
        }
        if previous.stop_confirmed_at != next.stop_confirmed_at
            || previous.stop_proof != next.stop_proof
        {
            let (Some(at), Some(proof)) = (next.stop_confirmed_at, next.stop_proof) else {
                return self.corrupted("Allocation stop proof cannot be removed".into());
            };
            if previous.stop_proof.is_some() || next.phase != AllocationPhase::Released {
                return self.corrupted("Allocation stop proof cannot be overwritten".into());
            }
            self.record_execution_event(
                job,
                ExecutionEventKind::AllocationStopConfirmed { at, proof },
            )?;
        }
        Ok(())
    }

    pub(super) fn record_job_phase(
        &mut self,
        job: JobId,
        previous: Option<MutableJobPhase>,
        next: MutableJobPhase,
    ) -> Result<(), BatchError> {
        if previous == Some(next) {
            return Ok(());
        }
        if previous.is_some_and(|previous| !job_phase_transition_allowed(previous, next))
            || (previous.is_none() && next == MutableJobPhase::Executing)
        {
            return self.corrupted("invalid execution Job phase transition".into());
        }
        let kind = if previous.is_none() {
            ExecutionEventKind::Created { phase: Some(next) }
        } else {
            ExecutionEventKind::PhaseChanged { phase: next }
        };
        self.record_execution_event(job, kind)?;
        Ok(())
    }

    pub(super) fn record_mutable_input_revision(
        &mut self,
        job: JobId,
        previous: Option<u64>,
        revision: u64,
    ) -> Result<(), BatchError> {
        if previous == Some(revision) {
            return Ok(());
        }
        if previous.map_or(Some(1), |value| value.checked_add(1)) != Some(revision) {
            return self.corrupted("Job input revision is not consecutive".into());
        }
        self.record_execution_event(job, ExecutionEventKind::InputRevisionRecorded { revision })?;
        Ok(())
    }

    pub(super) fn record_job_state(
        &mut self,
        previous: Option<&JobState>,
        next: &JobState,
    ) -> Result<(), BatchError> {
        let job = next.job_id;
        if self.step(|tx| latest_event_seq(&tx.open_table(JOB_EVENTS)?, job))? == 0 {
            self.record_execution_event(job, ExecutionEventKind::Created { phase: None })?;
        }
        if previous.and_then(|state| state.outcome.as_ref()) != next.outcome.as_ref() {
            let Some(outcome) = &next.outcome else {
                return self.corrupted("Job outcome cannot be removed".into());
            };
            if previous.is_some_and(|state| state.outcome.is_some()) {
                return self.corrupted("Job outcome cannot be overwritten".into());
            }
            self.record_execution_event(
                job,
                ExecutionEventKind::JobOutcome {
                    outcome: outcome.clone(),
                },
            )?;
        }
        Ok(())
    }
}

fn execution_job_visible(
    rx: &ReadTransaction,
    job: JobId,
    context: Option<crate::db::entities::ComposerContextId>,
) -> Result<bool, StorageError> {
    if reads::jobs::read_conversation_row(rx, job)?.is_some() {
        return Ok(true);
    }
    let jobs = rx.open_table(JOBS)?;
    let Some(row) = jobs.get(job.get())? else {
        return Ok(false);
    };
    let record = crate::db::redb::jobs::decode_mutable_job(job.get(), row.value())?;
    Ok(
        !context.is_some_and(|context| context != record.composer_context)
            && (record.phase != crate::db::entities::MutableJobPhase::Unsubmitted
                || context.is_some()),
    )
}

pub(crate) fn read_execution_history(
    database: &Database,
    job: JobId,
    after: Option<u64>,
    limit: usize,
    context: Option<crate::db::entities::ComposerContextId>,
) -> Result<Option<ExecutionHistoryPage>, StorageError> {
    let rx = read(database)?;
    if !execution_job_visible(&rx, job, context)? {
        return Ok(None);
    }
    let table = rx.open_table(JOB_EVENTS)?;
    let limit = limit.clamp(1, MAX_PAGE);
    let mut events = Vec::new();
    let mut more = false;
    let mut transaction_end = None;
    for entry in table.range((
        std::ops::Bound::Excluded((job.get(), after.unwrap_or(0))),
        std::ops::Bound::Included((job.get(), u64::MAX)),
    ))? {
        if events.len() == limit {
            more = true;
            break;
        }
        let (key, value) = entry?;
        let event = execution::decode_event(key.value(), value.value())?;
        let committed_seq = match transaction_end {
            Some((transaction, end)) if transaction == event.transaction => end,
            _ => {
                let end = execution::commit_end(&rx, event.transaction)?;
                transaction_end = Some((event.transaction, end));
                end
            }
        };
        events.push(ExecutionHistoryItem {
            event,
            committed_seq,
        });
    }
    let next_after = events.last().map(|item| item.event.seq).filter(|_| more);
    Ok(Some(ExecutionHistoryPage {
        events,
        next_after,
        head_seq: execution::latest_seq(&rx, job)?,
    }))
}

pub(crate) fn read_execution_snapshot(
    database: &Database,
    job: JobId,
    at_seq: Option<u64>,
    context: Option<crate::db::entities::ComposerContextId>,
) -> Result<Option<crate::db::entities::ExecutionSnapshot>, StorageError> {
    let rx = read(database)?;
    if !execution_job_visible(&rx, job, context)? {
        return Ok(None);
    }
    execution::replay_job(&rx, job, at_seq)
}
