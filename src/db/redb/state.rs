use super::{StorageError, TxRead};
use crate::db::entities::{
    DependencyCondition, JobId, JobLifecycle, JobLimits, JobState, OutcomeKind,
};
use crate::db::entities::{
    JobClosing, OutcomeCause, OutcomeRecord, PoolAllocation, UncertaintyReason,
};
use crate::db::redb::codec::{Code, Dec, DecodeResult, Enc, TAG_JOB_RUNTIME};
use crate::db::redb::schema::JOB_RUNTIME;
use ::redb::{ReadableTable, WriteTransaction};

/// Mutable facts for one Job, kept together under its JobId.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct JobRuntime {
    pub state: JobState,
    pub lifecycle: JobLifecycle,
    pub pending: Option<(u32, u32)>,
    pub last_result: Option<JobId>,
}

pub(super) fn has_open_membership(
    outcome: Option<&OutcomeRecord>,
    allocation: Option<&PoolAllocation>,
) -> bool {
    outcome.is_none_or(|outcome| {
        outcome.kind == OutcomeKind::Unknown
            && allocation.is_some_and(PoolAllocation::holds_capacity)
    })
}

impl JobRuntime {
    pub(super) fn record_result(&mut self, job: JobId) {
        self.last_result = Some(self.last_result.map_or(job, |old| old.max(job)));
    }

    pub(super) fn new(job: JobId) -> Self {
        Self {
            state: JobState::new(job),
            lifecycle: JobLifecycle::default(),
            pending: None,
            last_result: None,
        }
    }

    pub(super) fn encode(&self) -> Vec<u8> {
        let mut e = Enc::new(TAG_JOB_RUNTIME);
        e.opt(self.state.outcome.as_ref(), |e, outcome| {
            e.u8(outcome.kind.code());
            e.ts(outcome.at);
            e.opt(outcome.cause.as_ref(), |e, cause| match cause {
                OutcomeCause::Code(code) => {
                    e.u8(1);
                    e.u8(*code);
                }
                OutcomeCause::Text(text) => {
                    e.u8(2);
                    e.str(text);
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
        });
        let row = &self.lifecycle;
        e.ostr(row.title.as_deref());
        e.ots(row.hidden_at);
        e.ou32(row.on_hold);
        e.u8(row.loop_count);
        e.opt(row.closing.as_ref(), |e, closing| {
            e.ts(closing.at);
            e.actor(&closing.by);
            e.bool(closing.running);
        });
        e.opt(row.limits.as_ref(), |e, limits| {
            e.u8(limits.max_depth);
            e.u8(limits.max_auto_turns);
            e.u32(limits.max_iterations);
        });
        e.opt(row.compaction_policy.as_ref(), Enc::compaction_policy);
        e.u64(row.compaction_revision);
        e.u32(row.after.len() as u32);
        for (job, condition) in &row.after {
            e.u128(job.get());
            e.u8(match condition {
                DependencyCondition::Success => 1,
                DependencyCondition::Settled => 2,
            });
        }
        e.ou128(row.reported.map(JobId::get));
        e.opt(self.pending.as_ref(), |e, (dependencies, children)| {
            e.u32(*dependencies);
            e.u32(*children);
        });
        e.ou128(self.last_result.map(JobId::get));
        e.0
    }

    pub(super) fn decode_at(key: u128, bytes: &[u8]) -> DecodeResult<Self> {
        let mut d = Dec::new(bytes, TAG_JOB_RUNTIME)?;
        let (state, mut lifecycle) = Self::decode_state_and_lifecycle(key, &mut d)?;
        lifecycle.reported = d.ou128()?.map(JobId::new);
        let row = Self {
            state,
            lifecycle,
            pending: d.opt(|d| Ok((d.u32()?, d.u32()?)))?,
            last_result: d.ou128()?.map(JobId::new),
        };
        d.end()?;
        Ok(row)
    }

    pub(super) fn decode_state_and_lifecycle(
        key: u128,
        d: &mut Dec<'_>,
    ) -> DecodeResult<(JobState, JobLifecycle)> {
        let outcome = d.opt(|d| {
            let kind = d.code_with(OutcomeKind::from_code, |_| {
                StorageError::corrupted("invalid outcome")
            })?;
            let at = d.ts()?;
            let cause = d.opt(|d| match d.u8()? {
                1 => Ok(OutcomeCause::Code(d.u8()?)),
                2 => Ok(OutcomeCause::Text(d.str()?)),
                3 => Ok(OutcomeCause::Abandoned),
                4 => Ok(OutcomeCause::DependencyImpossible),
                5 => Ok(OutcomeCause::Stopped { record: d.u32()? }),
                6 => Ok(OutcomeCause::Closed),
                7 => d
                    .code_with(UncertaintyReason::from_code, |_| {
                        StorageError::corrupted("invalid uncertainty reason")
                    })
                    .map(OutcomeCause::Uncertain),
                _ => Err(StorageError::corrupted("invalid outcome cause")),
            })?;
            Ok(OutcomeRecord { kind, at, cause })
        })?;
        let state = JobState {
            job_id: JobId::new(key),
            outcome,
        };
        let lifecycle = JobLifecycle {
            title: d.ostr()?,
            hidden_at: d.ots()?,
            on_hold: d.ou32()?,
            loop_count: d.u8()?,
            closing: d.opt(|d| {
                Ok(JobClosing {
                    at: d.ts()?,
                    by: d.actor()?,
                    running: d.bool()?,
                })
            })?,
            limits: d.opt(|d| {
                Ok(JobLimits {
                    max_depth: d.u8()?,
                    max_auto_turns: d.u8()?,
                    max_iterations: d.u32()?,
                })
            })?,
            compaction_policy: d.opt(Dec::compaction_policy)?,
            compaction_revision: d.u64()?,
            after: {
                let mut items = Vec::new();
                for _ in 0..d.u32()? {
                    let job = JobId::new(d.u128()?);
                    let condition = match d.u8()? {
                        1 => DependencyCondition::Success,
                        2 => DependencyCondition::Settled,
                        _ => return Err(StorageError::corrupted("invalid dependency condition")),
                    };
                    items.push((job, condition));
                }
                items
            },
            reported: None,
        };
        Ok((state, lifecycle))
    }
}

pub(super) fn read(tx: &impl TxRead, job: JobId) -> Result<Option<JobRuntime>, StorageError> {
    tx.table(JOB_RUNTIME)?
        .get(job.get())?
        .map(|bytes| JobRuntime::decode_at(job.get(), bytes.value()))
        .transpose()
}

pub(super) fn decode_state(key: u128, bytes: &[u8]) -> DecodeResult<JobState> {
    JobRuntime::decode_at(key, bytes).map(|row| row.state)
}

pub(super) fn require(tx: &impl TxRead, job: JobId) -> Result<JobRuntime, StorageError> {
    read(tx, job)?.ok_or_else(|| StorageError::corrupted(format!("{job} has no runtime row")))
}

pub(super) fn write(tx: &WriteTransaction, row: &JobRuntime) -> Result<(), StorageError> {
    tx.open_table(JOB_RUNTIME)?
        .insert(row.state.job_id.get(), row.encode().as_slice())?;
    Ok(())
}

pub(super) fn pending(tx: &impl TxRead, job: JobId) -> Result<Option<(u32, u32)>, StorageError> {
    Ok(require(tx, job)?.pending)
}

pub(super) fn set_pending(
    tx: &WriteTransaction,
    job: JobId,
    pending: Option<(u32, u32)>,
) -> Result<(), StorageError> {
    let mut row = require(tx, job)?;
    row.pending = pending;
    write(tx, &row)
}
