//! Durable quarantine of the execution capacity an unresolved external
//! execution may still occupy.
//!
//! An OCG opening that starts a Job claims it in [`LOCAL_EXECUTIONS`] under its
//! own epoch. When a later opening finds that claim, the earlier opening is
//! gone: nothing in this process can still drive the Job. That establishes
//! *local* stop, and nothing more. The earlier supervisor's OS descendants are
//! not its epoch, and may still be running. Treating the claim as a stop
//! confirmation would release a Pool slot back into admission while the work
//! that held it is alive, which is exactly the reallocation this module exists
//! to prevent.
//!
//! So an abandoned claim is not confirmed stopped. It is *quarantined*: the Job
//! takes an unknown outcome, keeps every claim and occupancy it held, and
//! records why in [`UNRESOLVED_EXECUTIONS`] so the protection is durable and
//! survives any number of later restarts.
//!
//! Nothing here observes a process, and nothing here needs to. A PID is not
//! recorded because a PID's presence proves nothing about the descendants that
//! mattered and its absence does not prove they are gone; storing it would add
//! metadata that looks like evidence without being any. The quarantine is
//! released only by a confirmation that already had to be backed by evidence
//! of termination, which no such metadata could produce.

use super::*;
use redb::ReadableTableMetadata;

/// The binding decoder, so the quarantine can check its own domain against
/// the authoritative one rather than a second reading of it.
use super::execution_claim::decode_frozen_binding;

/// What one quarantined Job's unresolved external execution leaves occupied.
///
/// Only the minimum Option A needs: which opening's death left the stop
/// unconfirmed, when that was noticed, and which capacity domain the
/// unresolved work may still hold.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UnresolvedExecution {
    /// The opening whose death left the Job's stop unconfirmed. Provenance
    /// only; never evidence that anything stopped.
    pub epoch: u64,
    /// When the opening that found it abandoned recorded the unresolved state.
    pub unresolved_at: Timestamp,
    /// The Pool whose slot the unresolved work may still occupy, when the
    /// Job's frozen binding established the domain. `None` when no binding did:
    /// the affected domain could not be established reliably, so the global
    /// domain is charged and no individual Pool is charged a slot it may not
    /// be holding. Guessing a Pool here would quarantine capacity whose safety
    /// can be established.
    pub pool: Option<ExecutionPoolId>,
}

impl UnresolvedExecution {
    pub(super) fn encode(&self) -> Vec<u8> {
        let mut e = Enc::new(codec::TAG_QUARANTINE);
        e.u64(self.epoch);
        e.ts(self.unresolved_at);
        e.opt(&self.pool, |e, pool| e.u64(pool.get()));
        e.0
    }

    pub(super) fn decode(bytes: &[u8]) -> DecodeResult<Self> {
        let mut d = Dec::new(bytes, codec::TAG_QUARANTINE)?;
        let record = UnresolvedExecution {
            epoch: d.u64()?,
            unresolved_at: d.ts()?,
            pool: d.opt(|d| Ok(ExecutionPoolId::new(d.u64()?)))?,
        };
        d.end()?;
        Ok(record)
    }
}

impl Batch {
    /// Records that `job`'s execution is unresolved and holds the capacity
    /// `record` names. Written once: a Job that already has an entry keeps it,
    /// so a repeated restart cannot weaken a protection an earlier one set.
    pub(crate) fn quarantine_execution(
        &mut self,
        job: JobId,
        record: UnresolvedExecution,
    ) -> Result<(), BatchError> {
        self.step(|tx| {
            let mut table = tx.open_table(UNRESOLVED_EXECUTIONS)?;
            if table.get(job.get())?.is_some() {
                return Err(StorageError::corrupted(format!(
                    "{job} already carries an unresolved execution"
                )));
            }
            table.insert(job.get(), record.encode().as_slice())?;
            Ok(())
        })
    }

    /// The Job's unresolved execution, while it has one.
    pub(crate) fn unresolved_execution(
        &mut self,
        job: JobId,
    ) -> Result<Option<UnresolvedExecution>, BatchError> {
        let bytes = self.step(|tx| {
            Ok(tx
                .open_table(UNRESOLVED_EXECUTIONS)?
                .get(job.get())?
                .map(|value| value.value().to_vec()))
        })?;
        let Some(bytes) = bytes else {
            return Ok(None);
        };
        Ok(Some(self.decoded(UnresolvedExecution::decode(&bytes))?))
    }

    /// Removes the Job's unresolved execution. Only a confirmed stop calls
    /// this; every other path leaves the quarantine in place.
    pub(crate) fn release_quarantine(&mut self, job: JobId) -> Result<(), BatchError> {
        self.step(|tx| {
            tx.open_table(UNRESOLVED_EXECUTIONS)?.remove(job.get())?;
            Ok(())
        })
    }
}

/// Every quarantined execution, in Job order.
pub(crate) fn read_unresolved_executions(
    database: &Database,
) -> Result<Vec<(JobId, UnresolvedExecution)>, StorageError> {
    let transaction = read(database)?;
    let table = transaction.open_table(UNRESOLVED_EXECUTIONS)?;
    let mut records = Vec::new();
    records
        .try_reserve_exact(table.len()? as usize)
        .map_err(execution_claim::allocation_error)?;
    for entry in table.iter()? {
        let (job, bytes) = entry?;
        records.push((
            JobId::new(job.value()),
            UnresolvedExecution::decode(bytes.value())?,
        ));
    }
    Ok(records)
}

/// The global execution capacity held by quarantined work that no
/// [`OCCUPANCY`] row represents.
///
/// A quarantined Job with a known Pool keeps its [`OCCUPANCY`] entry, so
/// [`OCCUPANCY`] already counts it and this must not count it again. A Job
/// whose domain could not be established has no Pool to charge and so no
/// occupancy row; it still holds a global slot, and this is that slot. The two
/// sets are disjoint by the invariant [`verify_quarantine`] enforces, which is
/// what keeps claim retention and quarantine from charging one occupation
/// twice.
///
/// Bounded: past `GLOBAL_EXECUTION_LIMIT` the global domain is full whatever
/// the exact count is, so a scan that stops there answers every question the
/// capacity check asks.
pub(super) fn unattributed_quarantine(tx: &WriteTransaction) -> Result<u64, StorageError> {
    let table = tx.open_table(UNRESOLVED_EXECUTIONS)?;
    let mut count = 0;
    for entry in table.iter()? {
        let (_, bytes) = entry?;
        if UnresolvedExecution::decode(bytes.value())?.pool.is_some() {
            continue;
        }
        count += 1;
        if count > u64::from(GLOBAL_EXECUTION_LIMIT) {
            break;
        }
    }
    Ok(count)
}

/// The global execution domain's occupancy: every [`OCCUPANCY`] entry, plus
/// the quarantined work that holds a global slot without one.
pub(super) fn global_occupancy(tx: &WriteTransaction) -> Result<u64, StorageError> {
    let occupied = tx.open_table(OCCUPANCY)?.len()?;
    Ok(occupied + unattributed_quarantine(tx)?)
}

/// Checks that the durable quarantine agrees with the rest of the durable
/// capacity state, and fails closed when it does not.
///
/// A quarantine that cannot be reconciled with the occupancy and bindings it
/// claims to protect is not a safe base to admit from: it may be understating
/// what is still held. Every disagreement is refused at open, before any
/// admission, rather than repaired by guessing which side is right.
///
/// The two directions are checked because either can be wrong on its own.
/// From the quarantine side, each entry must name a Job that still needs
/// protection, with a domain its frozen binding agrees with. From the
/// occupancy side, every occupancy row whose Job is quarantined must be the
/// entry that quarantine claims, which is what rules out charging one
/// occupation twice.
///
/// The occupancy side is the check the binding side cannot make. `rebuild`
/// has just derived the occupancy from the bindings, so a disagreement it
/// finds is one no writer of this build could have produced; it is stated
/// rather than assumed, because this runs on every open and the cost of
/// catching it here, before admission, is the whole point.
pub(super) fn verify_quarantine(tx: &WriteTransaction) -> Result<(), StorageError> {
    let quarantine = tx.open_table(UNRESOLVED_EXECUTIONS)?;
    let bindings = tx.open_table(FROZEN_POOL_BINDINGS)?;
    let occupancy = tx.open_table(OCCUPANCY)?;
    let jobs = tx.open_table(JOBS)?;
    for entry in quarantine.iter()? {
        let (job, bytes) = entry?;
        let job = JobId::new(job.value());
        let record = UnresolvedExecution::decode(bytes.value())?;
        let Some(row) = jobs.get(job.get())? else {
            return Err(StorageError::corrupted(format!(
                "the unresolved execution of {job} names a missing Job"
            )));
        };
        // A confirmed stop releases the quarantine in the same transaction, so
        // an entry beside a confirmed stop is a claim to protect capacity that
        // has already been handed back.
        if JobRow::decode(row.value())?.locally_stopped() {
            return Err(StorageError::corrupted(format!(
                "{job} is quarantined but its execution is already confirmed stopped"
            )));
        }
        let bound = bindings
            .get(job.get())?
            .map(|value| decode_frozen_binding(value.value()));
        match (record.pool, bound) {
            (Some(quarantined), Some(Ok(binding))) if binding.pool != quarantined => {
                return Err(StorageError::corrupted(format!(
                    "{job} quarantines {} but is bound to {}",
                    binding.pool, quarantined
                )));
            }
            // A Job with a binding always keeps that Pool's occupancy, and one
            // without a binding never has any; either disagreement means the
            // record and the capacity it names describe different worlds.
            (Some(quarantined), None) => {
                return Err(StorageError::corrupted(format!(
                    "{job} quarantines {quarantined} but has no frozen binding"
                )));
            }
            (Some(_), Some(Err(error))) | (None, Some(Err(error))) => return Err(error),
            // Agreement is checked from the occupancy side below, where a
            // missing row can be seen.
            (Some(_), Some(Ok(_))) | (None, None) => {}
            (None, Some(Ok(binding))) => {
                return Err(StorageError::corrupted(format!(
                    "{job} has no quarantined domain but is bound to {}",
                    binding.pool
                )));
            }
        }
    }
    for entry in occupancy.iter()? {
        let (key, _) = entry?;
        let (pool, job) = key.value();
        let Some(record) = quarantine.get(job)?.map(|value| value.value().to_vec()) else {
            continue;
        };
        let record = UnresolvedExecution::decode(&record)?;
        if record.pool != Some(ExecutionPoolId::new(pool)) {
            return Err(StorageError::corrupted(format!(
                "the occupancy of {} by {} is not the domain its unresolved execution names",
                ExecutionPoolId::new(pool),
                JobId::new(job)
            )));
        }
    }
    drop(quarantine);
    drop(bindings);
    drop(occupancy);
    drop(jobs);
    if global_occupancy(tx)? > u64::from(GLOBAL_EXECUTION_LIMIT) {
        return Err(StorageError::corrupted(
            "quarantined and occupied execution capacity exceeds the global limit",
        ));
    }
    Ok(())
}
