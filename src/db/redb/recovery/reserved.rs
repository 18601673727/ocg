use super::{
    ResidualOwnership, check_identity, check_records, residual_ownership_in,
    verify_reserved_residual,
};
use crate::db::entities::{JobId, JobSpec, OutcomeKind};
use crate::db::redb::execution;
use crate::db::redb::recovery::audit::{
    audit_delta, verify_active_allocation_delta, verify_reserved_output_delta,
    verify_reserved_settlement_delta, verify_unrelated_recovery_rows,
};
use crate::db::redb::recovery::offline::{
    apply_reconciliation, finalization_copy, with_reconciliation_copy,
    with_verified_finalization_copy,
};
use crate::db::redb::schema::{
    APPROVALS, CONVERSATIONS, EXECUTORS, JOB_OBSERVATIONS, JOB_OUTPUTS, JOB_RUNTIME, LIVE_CHILDREN,
    META, POOL_ALLOCATIONS, POOLS, REQUEST_EVENTS, TOOL_CALLS,
};
use crate::db::redb::state::JobRuntime;
use crate::db::redb::{StorageError, StorageErrorKind, allocations, reads};
use crate::host::data_directory::{current_boot_id, current_host_id};
use ::redb::ReadableDatabase;

#[derive(Debug)]
pub(crate) struct ReservedRefusal {
    pub(crate) code: &'static str,
    pub(crate) detail: String,
}

impl ReservedRefusal {
    pub(crate) fn new(code: &'static str, detail: impl Into<String>) -> Self {
        Self {
            code,
            detail: detail.into(),
        }
    }
}

impl From<StorageError> for ReservedRefusal {
    fn from(error: StorageError) -> Self {
        let code = match error.kind {
            StorageErrorKind::Corrupted => "inconsistent_or_corrupt",
            StorageErrorKind::Io(_)
            | StorageErrorKind::PreviousIo
            | StorageErrorKind::AlreadyOpen
            | StorageErrorKind::Closed
            | StorageErrorKind::ValueTooLarge
            | StorageErrorKind::Other => "unverifiable",
        };
        Self::new(code, error.detail)
    }
}

impl From<::redb::DatabaseError> for ReservedRefusal {
    fn from(error: ::redb::DatabaseError) -> Self {
        StorageError::from(error).into()
    }
}

fn reserved_preflight_in(
    database: &impl ReadableDatabase,
    job: JobId,
) -> Result<
    (
        crate::db::entities::AllocationToken,
        serde_json::Value,
        ResidualOwnership,
    ),
    ReservedRefusal,
> {
    use crate::db::entities::{AllocationPhase, MutableJobPhase};
    use serde_json::json;

    let tx = database.begin_read().map_err(StorageError::from)?;
    let meta = tx.open_table(META).map_err(StorageError::from)?;
    check_identity(&meta)?;
    let epoch = meta
        .get("open_epoch")
        .map_err(StorageError::from)?
        .and_then(|value| <[u8; 8]>::try_from(value.value()).ok())
        .map(u64::from_le_bytes)
        .ok_or_else(|| ReservedRefusal::new("inconsistent_or_corrupt", "missing open epoch"))?;
    let clean_shutdown = meta
        .get("clean_shutdown")
        .map_err(StorageError::from)?
        .map(|value| value.value() == [1]);
    drop(meta);
    check_records(&tx)?;

    let allocations = tx
        .open_table(POOL_ALLOCATIONS)
        .map_err(StorageError::from)?;
    let row = allocations
        .get(job.get())
        .map_err(StorageError::from)?
        .ok_or_else(|| ReservedRefusal::new("allocation_not_found", "the Job has no Allocation"))?;
    let allocation = allocations::decode_pool_allocation(job.get(), row.value())?;
    drop(row);
    match allocation.phase {
        AllocationPhase::Reserved => {}
        AllocationPhase::Released => {
            return Err(ReservedRefusal::new(
                "already_released",
                "the Allocation is already released; there is nothing to reconcile",
            ));
        }
        AllocationPhase::Running => {
            return Err(ReservedRefusal::new(
                "running",
                "the Allocation is Running: the Job started, and a restart is not evidence its work stopped",
            ));
        }
        AllocationPhase::StopRequested => {
            return Err(ReservedRefusal::new(
                "stop_requested",
                "a stop was requested of a started Job; its stop is not confirmed",
            ));
        }
        AllocationPhase::Uncertain => {
            return Err(ReservedRefusal::new(
                "uncertain",
                "the Allocation is Uncertain; its outcome is unknown, not unstarted",
            ));
        }
    }
    if allocation.started_at.is_some() {
        return Err(ReservedRefusal::new(
            "started",
            "a start was committed for this Allocation",
        ));
    }
    if allocation.stop_confirmed_at.is_some() || allocation.stop_proof.is_some() {
        return Err(ReservedRefusal::new(
            "stop_evidence",
            "the Allocation carries stop evidence, so it is not an untouched reservation",
        ));
    }
    let token = allocation.token;
    let recorded = execution::replay_job(&tx, job, None)?.and_then(|snapshot| snapshot.allocation);
    if recorded.as_ref() != Some(&allocation) {
        return Err(audit_delta(
            "the reserved Allocation differs from its Job history",
        ));
    }
    if token.epoch != epoch {
        return Err(ReservedRefusal::new(
            "epoch_mismatch",
            format!(
                "the Allocation was made in epoch {} but the database was last opened in epoch {epoch}; the intervening history is not visible",
                token.epoch
            ),
        ));
    }
    let current_host = current_host_id();
    if current_host.is_none() || allocation.host_id != current_host {
        return Err(ReservedRefusal::new(
            "other_host",
            "the Allocation was not claimed by this host, or this host cannot be identified; a lock here says nothing about another machine",
        ));
    }
    let same_boot = allocation.boot_id.is_some() && allocation.boot_id == current_boot_id();

    drop(allocations);
    let ownership = residual_ownership_in(&tx).map_err(|error| match error.kind {
        StorageErrorKind::Corrupted => ReservedRefusal::new("ownership_inconsistent", error.detail),
        StorageErrorKind::Io(_)
        | StorageErrorKind::PreviousIo
        | StorageErrorKind::AlreadyOpen
        | StorageErrorKind::Closed
        | StorageErrorKind::ValueTooLarge
        | StorageErrorKind::Other => error.into(),
    })?;
    if !ownership.held.contains(&job.get()) {
        return Err(ReservedRefusal::new(
            "inconsistent_or_corrupt",
            "the Reserved Allocation is not classified as held",
        ));
    }
    let mut residual = ownership.clone();
    residual.held.remove(&job.get());

    let record = reads::jobs::mutable_job_record_in(&tx, job)?
        .ok_or_else(|| ReservedRefusal::new("job_missing", "the reserved Job does not exist"))?;
    if let Some(outcome) = &record.outcome {
        return Err(ReservedRefusal::new(
            if outcome.kind == OutcomeKind::Unknown {
                "outcome_unknown"
            } else {
                "job_has_outcome"
            },
            "the Job already has an outcome",
        ));
    }
    if record.job.phase != MutableJobPhase::Queued
        || record.job.revision != allocation.job_revision
        || record.job.frozen.is_none()
    {
        return Err(ReservedRefusal::new(
            "job_not_prestart",
            "the Job is not the frozen, queued revision that was reserved",
        ));
    }

    let pools = tx.open_table(POOLS).map_err(StorageError::from)?;
    let executors = tx.open_table(EXECUTORS).map_err(StorageError::from)?;
    let pool = match pools.get(token.pool.get()).map_err(StorageError::from)? {
        Some(bytes) => Some(allocations::decode_allocation_pool(
            token.pool.get(),
            bytes.value(),
        )?),
        None => None,
    };
    let executor = match executors
        .get(token.executor.get())
        .map_err(StorageError::from)?
    {
        Some(bytes) => Some(allocations::decode_allocation_executor(
            token.executor.get(),
            bytes.value(),
        )?),
        None => None,
    };
    if pool.as_ref().is_none_or(|pool| pool.id != token.pool)
        || executor
            .as_ref()
            .is_none_or(|executor| executor.id != token.executor || executor.pool != token.pool)
    {
        return Err(ReservedRefusal::new(
            "binding_inconsistent",
            "the reserved Pool or Executor is missing or no longer bound to the Allocation",
        ));
    }

    let id = job.get();
    let mut evidence = Vec::new();
    if tx
        .open_table(JOB_OUTPUTS)
        .map_err(StorageError::from)?
        .range((id, 0)..=(id, u32::MAX))
        .map_err(StorageError::from)?
        .next()
        .is_some()
    {
        evidence.push("output");
    }
    if tx
        .open_table(REQUEST_EVENTS)
        .map_err(StorageError::from)?
        .range((id, 0, 0)..=(id, u32::MAX, u32::MAX))
        .map_err(StorageError::from)?
        .next()
        .is_some()
    {
        evidence.push("request");
    }
    if tx
        .open_table(TOOL_CALLS)
        .map_err(StorageError::from)?
        .range((id, 0, 0)..=(id, u32::MAX, u32::MAX))
        .map_err(StorageError::from)?
        .next()
        .is_some()
    {
        evidence.push("tool");
    }
    if tx
        .open_table(APPROVALS)
        .map_err(StorageError::from)?
        .range((id, 0, 0)..=(id, u32::MAX, u32::MAX))
        .map_err(StorageError::from)?
        .next()
        .is_some()
    {
        evidence.push("approval");
    }
    if tx
        .open_table(JOB_OBSERVATIONS)
        .map_err(StorageError::from)?
        .range((id, 0, 0)..=(id, u8::MAX, u128::MAX))
        .map_err(StorageError::from)?
        .next()
        .is_some()
    {
        evidence.push("observation");
    }
    if tx
        .open_table(LIVE_CHILDREN)
        .map_err(StorageError::from)?
        .range((id, 0)..=(id, u128::MAX))
        .map_err(StorageError::from)?
        .next()
        .is_some()
    {
        evidence.push("child");
    }
    if !evidence.is_empty() {
        return Err(ReservedRefusal::new(
            "evidence_present",
            format!(
                "the reserved Job has {} evidence; an unstarted reservation has none",
                evidence.join(", ")
            ),
        ));
    }

    let facts = json!({
        "job": job.to_string(),
        "pool": token.pool.to_string(),
        "executor": token.executor.to_string(),
        "generation": token.generation.get(),
        "allocation_epoch": token.epoch,
        "open_epoch": epoch,
        "clean_shutdown": clean_shutdown,
        "claimed_at_unix_micros": allocation.claimed_at.as_unix_micros(),
        "job_revision": allocation.job_revision,
        "claimed_by_this_host": true,
        "claimed_in_this_boot": same_boot,
        "held_allocations": ownership.held.len(),
        "unknown_outcomes": ownership.retained_unknown.len() + ownership.resolved_unknown.len(),
        "residual_ownership": residual.facts(),
    });
    Ok((token, facts, residual))
}

fn verify_reserved_residual_against(
    database: &impl ReadableDatabase,
    expected: &ResidualOwnership,
) -> Result<serde_json::Value, ReservedRefusal> {
    let verified = verify_reserved_residual(database)?;
    if &verified.ownership != expected {
        return Err(audit_delta(
            "the residual ownership differs from the rollback source without the target",
        ));
    }
    Ok(serde_json::json!({
        "residual_ownership": verified.ownership.facts(),
        "normal_startup": if verified.normal_startup_accepts {
            "accepted: no Allocation holds capacity and no Unknown outcome is unresolved; full startup verification passed"
        } else {
            "refused: residual held ownership remains, as normal startup requires"
        },
    }))
}

pub(crate) fn reconcile_reserved_preflight(
    file: std::fs::File,
    job: JobId,
) -> Result<serde_json::Value, ReservedRefusal> {
    with_reconciliation_copy(file, |database| {
        reserved_preflight_in(&*database, job).map(|(_, facts, _)| facts)
    })
}

pub(crate) fn reconcile_reserved_apply(
    file: std::fs::File,
    job: JobId,
) -> Result<serde_json::Value, ReservedRefusal> {
    use crate::db::entities::{AllocationPhase, UnstartedCause};
    with_reconciliation_copy(file, |database| {
        let (token, mut facts, residual) = reserved_preflight_in(&*database, job)?;
        apply_reconciliation(database, token.epoch, "reconciled", |batch| {
            batch.fail_unstarted_reservation(token, UnstartedCause::OfflineReconciled)
        })?;
        let residual = verify_reserved_residual_against(&*database, &residual)?;
        let tx = database.begin_read().map_err(StorageError::from)?;
        let allocations = tx
            .open_table(POOL_ALLOCATIONS)
            .map_err(StorageError::from)?;
        let row = allocations
            .get(job.get())
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                ReservedRefusal::new("inconsistent_or_corrupt", "Allocation vanished")
            })?;
        let released = allocations::decode_pool_allocation(job.get(), row.value())?;
        let record = reads::jobs::mutable_job_record_in(&tx, job)?;
        let failed = record
            .as_ref()
            .and_then(|record| record.outcome.as_ref())
            .is_some_and(|outcome| outcome.kind == OutcomeKind::Failed);
        if released.phase != AllocationPhase::Released || released.started_at.is_some() || !failed {
            return Err(ReservedRefusal::new(
                "inconsistent_or_corrupt",
                "the reconciled copy does not show a Released Allocation and a Failed Job",
            ));
        }
        facts["result"] = serde_json::json!({
            "allocation_phase": "released",
            "job_outcome": "failed",
            "failure_code": UnstartedCause::OfflineReconciled.code(),
            "page_checksums": "every page checksum verified",
            "logical_checks": "every record header, derived index and ownership row verified",
            "residual": residual,
        });
        Ok(facts)
    })
}

pub(crate) fn verify_reserved_publication_delta(
    source: &std::fs::File,
    published: &std::fs::File,
    job: JobId,
) -> Result<serde_json::Value, ReservedRefusal> {
    use crate::db::entities::{AllocationPhase, UnstartedCause};
    use serde_json::json;

    let source = finalization_copy(source)?;
    let published = finalization_copy(published)?;
    with_verified_finalization_copy(source, "rollback", |source_database| {
        let (_, mut facts, residual) = reserved_preflight_in(&*source_database, job)?;
        with_verified_finalization_copy(published, "published", |database| {
            let residual = verify_reserved_residual_against(&*database, &residual)?;
            let source_tx = source_database.begin_read().map_err(StorageError::from)?;
            let source_record = reads::jobs::mutable_job_record_in(&source_tx, job)?
                .ok_or_else(|| audit_delta("the rollback source Job vanished"))?;
            let parent = source_record.job.parent_id;
            let parent_row = source_tx
                .open_table(CONVERSATIONS)
                .map_err(StorageError::from)?
                .get(parent.get())
                .map_err(StorageError::from)?
                .ok_or_else(|| audit_delta("the rollback parent Group vanished"))?;
            let standalone = JobSpec::decode_at(parent.get(), parent_row.value())?
                .is_standalone_decision_group();
            drop(parent_row);
            verify_unrelated_recovery_rows(
                &*source_database,
                &*database,
                &std::collections::BTreeSet::from([job.get()]),
                Some((parent, standalone)),
            )?;
            let source_allocation_row = source_tx
                .open_table(POOL_ALLOCATIONS)
                .map_err(StorageError::from)?
                .get(job.get())
                .map_err(StorageError::from)?
                .ok_or_else(|| audit_delta("the rollback source Allocation vanished"))?;
            let source_allocation =
                allocations::decode_pool_allocation(job.get(), source_allocation_row.value())?;
            drop(source_allocation_row);
            let source_state_row = source_tx
                .open_table(JOB_RUNTIME)
                .map_err(StorageError::from)?
                .get(job.get())
                .map_err(StorageError::from)?
                .ok_or_else(|| audit_delta("the rollback source Job state vanished"))?;
            let source_runtime = JobRuntime::decode_at(job.get(), source_state_row.value())?;
            drop(source_state_row);

            let tx = database.begin_read().map_err(StorageError::from)?;
            let row = tx
                .open_table(POOL_ALLOCATIONS)
                .map_err(StorageError::from)?
                .get(job.get())
                .map_err(StorageError::from)?
                .ok_or_else(|| {
                    ReservedRefusal::new("inconsistent_or_corrupt", "Allocation vanished")
                })?;
            let released = allocations::decode_pool_allocation(job.get(), row.value())?;
            drop(row);
            let state_row = tx
                .open_table(JOB_RUNTIME)
                .map_err(StorageError::from)?
                .get(job.get())
                .map_err(StorageError::from)?
                .ok_or_else(|| audit_delta("the published Job state vanished"))?;
            let published_runtime = JobRuntime::decode_at(job.get(), state_row.value())?;
            drop(state_row);
            let outcome =
                published_runtime.state.outcome.as_ref().ok_or_else(|| {
                    audit_delta("the published Job has no OfflineReconciled outcome")
                })?;
            let mut expected_allocation = source_allocation;
            expected_allocation.phase = AllocationPhase::Released;
            let mut expected_runtime = source_runtime;
            expected_runtime.state.outcome = published_runtime.state.outcome.clone();
            expected_runtime.pending = None;
            if released != expected_allocation
                || published_runtime != expected_runtime
                || outcome.kind != OutcomeKind::Failed
                || outcome.cause.is_some()
            {
                return Err(audit_delta(
                    "the published target rows are not the exact authorized Released Allocation and Failed Job",
                ));
            }
            let output = verify_reserved_output_delta(&source_tx, &tx, job)?;
            if output.created_at != outcome.at {
                return Err(audit_delta(
                    "the Result and Job outcome do not share the transaction timestamp",
                ));
            }
            verify_active_allocation_delta(
                &source_tx,
                &tx,
                &std::collections::BTreeSet::from([(
                    released.token.pool.get(),
                    released.token.executor.get(),
                    job.get(),
                )]),
            )?;
            verify_reserved_settlement_delta(&source_tx, &tx, job, parent, standalone, outcome)?;
            facts["result"] = json!({
                "allocation_phase": "released",
                "job_outcome": "failed",
                "failure_code": UnstartedCause::OfflineReconciled.code(),
                "result_output_sequence": output.seq,
                "result_output_id": output.id.to_string(),
                "settlement_state": "exact Job settlement, parent child decrement and result advance",
                "page_checksums": "every page checksum verified",
                "logical_checks": "every record header, derived index and ownership row verified",
                "unrelated_allocations_and_jobs": "byte-identical to the rollback source",
                "preserved_evidence_and_derived_rows": "byte-identical to the rollback source except the authorized Result, sequence, settlement and active-index deltas",
                "reconstructed_without_reapply": true,
                "residual": residual,
            });
            Ok(facts)
        })
    })
}
