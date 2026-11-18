use super::{
    check_identity, check_records, refuse_unresolved_ownership, resolved_unknown_ownership,
    verify_recovered,
};
use crate::db::entities::{ContentDigest, OutcomeKind, OutputKind, StopProof};
use crate::db::redb::StorageError;
use crate::db::redb::recovery::audit::{
    verify_held_publication_preservation, verify_unrelated_recovery_rows,
};
use crate::db::redb::recovery::offline::{
    apply_reconciliation, finalization_copy, with_reconciliation_copy,
    with_verified_finalization_copy,
};
use crate::db::redb::recovery::reserved::ReservedRefusal;
use crate::db::redb::schema::FORMAT_VERSION;
use crate::db::redb::schema::{
    ACTIVE_ALLOCATIONS, ALLOCATION_REQUESTS, EXECUTORS, JOB_OUTPUTS, JOB_RUNTIME, META,
    MUTABLE_READY, PENDING, POOL_ALLOCATIONS, POOLS, TOOL_CALLS,
};
use crate::db::redb::state::JobRuntime;
use crate::db::redb::{allocations, reads, rows, state};
use crate::host::identity::{current_boot_id, current_host_id};
use ::redb::{ReadTransaction, ReadableDatabase, ReadableTable};

#[derive(Clone, Debug)]
pub(crate) struct RecoveryAction {
    pub(crate) token: crate::db::entities::AllocationToken,
    pub(crate) phase: crate::db::entities::AllocationPhase,
    pub(crate) proof: StopProof,
    pub(crate) reason: crate::db::entities::UncertaintyReason,
    pub(crate) preserve_unknown: bool,
    pub(crate) expected_allocation: Vec<u8>,
    pub(crate) expected_runtime: Vec<u8>,
}

pub(crate) struct HeldRecoveryPlan {
    pub(crate) evidence: serde_json::Value,
    pub(crate) digest: String,
    actions: Vec<RecoveryAction>,
    epoch: u64,
}

fn recovery_phase_name(phase: crate::db::entities::AllocationPhase) -> &'static str {
    use crate::db::entities::AllocationPhase;
    match phase {
        AllocationPhase::Reserved => "reserved",
        AllocationPhase::Running => "running",
        AllocationPhase::StopRequested => "stop_requested",
        AllocationPhase::Uncertain => "uncertain",
        AllocationPhase::Released => "released",
    }
}

fn recovery_proof_name(proof: StopProof) -> &'static str {
    match proof {
        StopProof::Proof => "proof",
        StopProof::Reboot => "reboot",
        StopProof::Settled => "settled",
        StopProof::User => "user",
        StopProof::Operator => "operator",
    }
}

fn sha256_json(value: &serde_json::Value) -> Result<String, ReservedRefusal> {
    let bytes = serde_json::to_vec(value).map_err(|error| {
        ReservedRefusal::new(
            "unverifiable",
            format!("cannot encode the recovery plan: {error}"),
        )
    })?;
    Ok(ContentDigest::of(&bytes).to_string())
}

fn held_recovery_plan_in(
    database: &impl ReadableDatabase,
    source_sha256: &str,
) -> Result<HeldRecoveryPlan, ReservedRefusal> {
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
    let clean = meta
        .get("clean_shutdown")
        .map_err(StorageError::from)?
        .is_some_and(|value| value.value() == [1]);
    drop(meta);
    check_records(&tx)?;

    let mut held = Vec::new();
    for entry in tx
        .open_table(POOL_ALLOCATIONS)
        .map_err(StorageError::from)?
        .iter()
        .map_err(StorageError::from)?
    {
        let (key, value) = entry.map_err(StorageError::from)?;
        let allocation = allocations::decode_pool_allocation(key.value(), value.value())?;
        if allocation.holds_capacity() {
            held.push((allocation, value.value().to_vec()));
        }
    }
    if clean && !held.is_empty() {
        return Err(ReservedRefusal::new(
            "unguarded_source",
            "a clean-shutdown database with held Allocations is not a supported recovery source",
        ));
    }

    let current_host = current_host_id();
    let current_boot = current_boot_id();
    let mut actions = Vec::new();
    let mut action_facts = Vec::new();
    let mut refusals = Vec::new();
    for (allocation, allocation_bytes) in held {
        let job = allocation.token.job;
        let state_bytes = match tx
            .open_table(JOB_RUNTIME)
            .map_err(StorageError::from)?
            .get(job.get())
            .map_err(StorageError::from)?
        {
            Some(row) => row.value().to_vec(),
            None => {
                return Err(ReservedRefusal::new(
                    "inconsistent_or_corrupt",
                    format!("{job} has no Job state"),
                ));
            }
        };
        let planned_revision = allocation.job_revision;
        let planned_phase = allocation.phase;
        match recovery_action_in(
            &tx,
            epoch,
            current_host.as_deref(),
            current_boot.as_deref(),
            allocation,
            allocation_bytes,
            state_bytes,
        )? {
            Ok(action) => {
                action_facts.push(json!({
                    "job": job.to_string(),
                    "pool": action.token.pool.to_string(),
                    "executor": action.token.executor.to_string(),
                    "generation": action.token.generation.get(),
                    "epoch": action.token.epoch,
                    "job_revision": planned_revision,
                    "from_phase": recovery_phase_name(action.phase),
                    "to_phase": "released",
                    "stop_proof": recovery_proof_name(action.proof),
                    "unknown": if action.preserve_unknown { "preserve_exact" } else { "record_unknown" },
                    "uncertainty_reason": action.reason.as_str(),
                    "allocation_sha256": sha256_bytes(&action.expected_allocation),
                    "job_runtime_sha256": sha256_bytes(&action.expected_runtime),
                }));
                actions.push(action);
            }
            Err((code, detail)) => refusals.push(json!({
                "job": job.to_string(),
                "phase": recovery_phase_name(planned_phase),
                "code": code,
                "detail": detail,
            })),
        }
    }
    let evidence = json!({
        "kind": "ocg-held-recovery-plan-v1",
        "database_format": FORMAT_VERSION,
        "record_version": FORMAT_VERSION,
        "source_sha256": source_sha256,
        "open_epoch": epoch,
        "clean_shutdown": clean,
        "current_host_id": current_host,
        "current_boot_id": current_boot,
        "actions": action_facts,
        "refused": refusals,
    });
    let digest = sha256_json(&evidence)?;
    Ok(HeldRecoveryPlan {
        evidence,
        digest,
        actions,
        epoch,
    })
}

fn sha256_bytes(bytes: &[u8]) -> String {
    ContentDigest::of(bytes).to_string()
}

fn recovery_action_in(
    tx: &ReadTransaction,
    epoch: u64,
    current_host: Option<&str>,
    current_boot: Option<&str>,
    allocation: crate::db::entities::PoolAllocation,
    allocation_bytes: Vec<u8>,
    state_bytes: Vec<u8>,
) -> Result<Result<RecoveryAction, (&'static str, String)>, ReservedRefusal> {
    use crate::db::entities::{
        AllocationPhase, ExecutionOperation, MutableFrozenConfiguration, MutableJobPhase,
        OutcomeCause, UncertaintyReason,
    };

    macro_rules! ineligible {
        ($code:expr, $detail:expr) => {
            return Ok(Err(($code, $detail.to_owned())))
        };
    }

    let job_id = allocation.token.job;
    if allocation.phase == AllocationPhase::Reserved {
        ineligible!(
            "reserved",
            "an unstarted Reservation belongs to `ocg reconcile-reserved`; it is left untouched"
        );
    }
    if allocation.token.epoch != epoch {
        ineligible!(
            "epoch_mismatch",
            "the database has intervening open history that this recovery cannot see"
        );
    }
    if current_host.is_none() || allocation.host_id.as_deref() != current_host {
        ineligible!(
            "other_host",
            "the Allocation is not owned by this identified host"
        );
    }
    let same_boot = current_boot.is_some() && allocation.boot_id.as_deref() == current_boot;
    let boot_changed = current_boot.is_some()
        && allocation.boot_id.is_some()
        && allocation.boot_id.as_deref() != current_boot;
    if allocation.started_at.is_none()
        || allocation.stop_confirmed_at.is_some()
        || allocation.stop_proof.is_some()
    {
        ineligible!(
            "allocation_inconsistent",
            "the held Allocation lacks a started, unconfirmed shape"
        );
    }
    let Some(record) = reads::jobs::mutable_job_record_in(tx, job_id)? else {
        ineligible!("job_not_native", "a native mutable Job is required");
    };
    if record.job.phase != MutableJobPhase::Executing
        || record.job.revision != allocation.job_revision
    {
        ineligible!(
            "job_revision",
            "the Job is not the executing revision named by the Allocation"
        );
    }
    let expected_operation = match record.job.frozen.as_ref() {
        Some(MutableFrozenConfiguration::Generative(frozen)) => Some((
            ExecutionOperation::LlmRequest,
            Some(frozen.llm.provider.transport),
        )),
        Some(MutableFrozenConfiguration::Decision(_)) => Some((ExecutionOperation::Decision, None)),
        None => None,
    };
    if expected_operation != Some((allocation.operation, allocation.protocol)) {
        ineligible!(
            "allocation_inconsistent",
            "the Allocation operation or protocol disagrees with the frozen Job"
        );
    }
    let pool = tx
        .open_table(POOLS)
        .map_err(StorageError::from)?
        .get(allocation.token.pool.get())
        .map_err(StorageError::from)?
        .map(|row| allocations::decode_allocation_pool(allocation.token.pool.get(), row.value()))
        .transpose()?;
    let executor = tx
        .open_table(EXECUTORS)
        .map_err(StorageError::from)?
        .get(allocation.token.executor.get())
        .map_err(StorageError::from)?
        .map(|row| {
            allocations::decode_allocation_executor(allocation.token.executor.get(), row.value())
        })
        .transpose()?;
    if pool
        .as_ref()
        .is_none_or(|pool| pool.id != allocation.token.pool)
        || executor.as_ref().is_none_or(|executor| {
            executor.id != allocation.token.executor
                || executor.pool != allocation.token.pool
                || !executor
                    .capabilities
                    .contains(&(allocation.operation, allocation.protocol))
        })
    {
        ineligible!(
            "binding_changed",
            "the Pool or Executor binding no longer proves this Allocation"
        );
    }
    for entry in tx
        .open_table(JOB_OUTPUTS)
        .map_err(StorageError::from)?
        .range((job_id.get(), 0)..=(job_id.get(), u32::MAX))
        .map_err(StorageError::from)?
    {
        let (_, value) = entry.map_err(StorageError::from)?;
        if rows::output_kind(value.value())? == OutputKind::Result {
            ineligible!(
                "result_present",
                "a final Result already exists and will not be replaced"
            );
        }
    }
    for entry in tx
        .open_table(MUTABLE_READY)
        .map_err(StorageError::from)?
        .iter()
        .map_err(StorageError::from)?
    {
        if entry.map_err(StorageError::from)?.0.value().2 == job_id.get() {
            ineligible!("already_queued", "the executing Job is unexpectedly ready");
        }
    }
    if tx
        .open_table(PENDING)
        .map_err(StorageError::from)?
        .get(job_id.get())
        .map_err(StorageError::from)?
        .is_some()
        || state::pending(tx, job_id)? != Some((0, 0))
    {
        ineligible!(
            "derived_state",
            "the executing Job's pending or unsettled rows are inconsistent"
        );
    }

    let (proof, reason, preserve_unknown) = match allocation.phase {
        AllocationPhase::Running => {
            if record.outcome.is_some() {
                ineligible!(
                    "outcome_present",
                    "a Running Allocation cannot have a final outcome"
                );
            }
            if !boot_changed {
                ineligible!(
                    "same_boot_running",
                    "Running recovery requires a genuine, identified Boot-ID change"
                );
            }
            (StopProof::Reboot, UncertaintyReason::Unclassified, false)
        }
        AllocationPhase::StopRequested => {
            if record.outcome.is_some() {
                ineligible!(
                    "outcome_present",
                    "a StopRequested Allocation cannot have a final outcome"
                );
            }
            if !boot_changed {
                ineligible!(
                    "same_boot_stop_requested",
                    "StopRequested recovery requires a genuine, identified Boot-ID change"
                );
            }
            (
                StopProof::Reboot,
                UncertaintyReason::StopRequestedWithoutConfirmedTermination,
                false,
            )
        }
        AllocationPhase::Uncertain => {
            let Some(outcome) = record.outcome.as_ref() else {
                ineligible!(
                    "outcome_missing",
                    "an Uncertain Allocation must preserve an Unknown outcome"
                );
            };
            if outcome.kind != OutcomeKind::Unknown {
                ineligible!(
                    "outcome_not_unknown",
                    "an Uncertain Allocation cannot have a known outcome"
                );
            }
            if boot_changed {
                (StopProof::Reboot, UncertaintyReason::Unclassified, true)
            } else if same_boot {
                let returned = outcome.cause.as_ref().is_some_and(|cause| {
                    matches!(
                        cause,
                        OutcomeCause::Uncertain(
                            UncertaintyReason::RequestPossiblySent
                                | UncertaintyReason::ConnectionInterrupted
                                | UncertaintyReason::ResponseIncompleteOrInvalid
                                | UncertaintyReason::StopRequestedWithoutConfirmedTermination
                        )
                    ) || matches!(cause, OutcomeCause::Text(detail) if matches!(
                        detail.as_str(),
                        crate::db::entities::UNCERTAIN_HOST_SHUTDOWN | crate::db::entities::UNCERTAIN_DEADLINE
                    ))
                });
                if !returned {
                    ineligible!(
                        "local_return_unproven",
                        "same-Boot Uncertain recovery requires a durable typed local-return reason"
                    );
                }
                (StopProof::Proof, UncertaintyReason::Unclassified, true)
            } else {
                ineligible!(
                    "boot_unproven",
                    "neither a genuine Boot-ID change nor same-Boot local return is proved"
                );
            }
        }
        AllocationPhase::Reserved | AllocationPhase::Released => {
            return Err(ReservedRefusal::new(
                "inconsistent_or_corrupt",
                "unreachable held Allocation phase",
            ));
        }
    };
    if proof == StopProof::Proof {
        for entry in tx
            .open_table(TOOL_CALLS)
            .map_err(StorageError::from)?
            .range((job_id.get(), 0, 0)..=(job_id.get(), u32::MAX, u32::MAX))
            .map_err(StorageError::from)?
        {
            let (key, row) = entry.map_err(StorageError::from)?;
            let tool = rows::decode_tool_evidence(rows::tool_call_ref(key.value())?, row.value())?;
            if tool.admission().is_some()
                && tool.reconciliation.is_none()
                && tool
                    .outcome
                    .as_ref()
                    .is_none_or(|outcome| !outcome.fact.is_definite())
            {
                ineligible!(
                    "tool_effect_unproven",
                    "an admitted tool effect is unconfirmed and a local process may still be running on this boot"
                );
            }
        }
    }
    Ok(Ok(RecoveryAction {
        token: allocation.token,
        phase: allocation.phase,
        proof,
        reason,
        preserve_unknown,
        expected_allocation: allocation_bytes,
        expected_runtime: state_bytes,
    }))
}

pub(crate) fn reconcile_held_preflight(
    file: std::fs::File,
    source_sha256: &str,
) -> Result<HeldRecoveryPlan, ReservedRefusal> {
    with_reconciliation_copy(file, |database| {
        held_recovery_plan_in(&*database, source_sha256)
    })
}

pub(crate) fn reconcile_held_apply(
    file: std::fs::File,
    source_sha256: &str,
    expected_digest: &str,
) -> Result<serde_json::Value, ReservedRefusal> {
    use serde_json::json;
    with_reconciliation_copy(file, |database| {
        let plan = held_recovery_plan_in(&*database, source_sha256)?;
        if plan.digest != expected_digest {
            return Err(ReservedRefusal::new(
                "plan_digest_mismatch",
                "the source or its recovery decisions changed; repeat the dry-run",
            ));
        }
        if plan.actions.is_empty() {
            return Err(ReservedRefusal::new(
                "no_eligible_allocations",
                "the plan contains no Allocation this command may release",
            ));
        }
        apply_reconciliation(database, plan.epoch, "recovered", |batch| {
            for action in &plan.actions {
                batch.reconcile_stopped_allocation(action)?;
            }
            batch.step(|tx| {
                tx.open_table(META)?
                    .insert("clean_shutdown", [0u8].as_slice())?;
                Ok(())
            })
        })?;
        let audit = audit_held_recovery(&*database, &plan)?;
        Ok(json!({
            "plan": plan.evidence,
            "plan_digest": plan.digest,
            "result": audit,
            "page_checksums": "every page checksum verified",
            "clean_shutdown": false,
        }))
    })
}

fn audit_held_recovery(
    database: &impl ReadableDatabase,
    plan: &HeldRecoveryPlan,
) -> Result<serde_json::Value, ReservedRefusal> {
    use crate::db::entities::{AllocationPhase, OutcomeCause};
    use serde_json::json;

    let tx = database.begin_read().map_err(StorageError::from)?;
    check_records(&tx)?;
    let mut recovered = Vec::new();
    for action in &plan.actions {
        let job = action.token.job;
        let allocation_row = tx
            .open_table(POOL_ALLOCATIONS)
            .map_err(StorageError::from)?
            .get(job.get())
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                ReservedRefusal::new("inconsistent_or_corrupt", "recovered Allocation vanished")
            })?;
        let allocation = allocations::decode_pool_allocation(job.get(), allocation_row.value())?;
        drop(allocation_row);
        let state_row = tx
            .open_table(JOB_RUNTIME)
            .map_err(StorageError::from)?
            .get(job.get())
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                ReservedRefusal::new("inconsistent_or_corrupt", "recovered Job state vanished")
            })?;
        let runtime = JobRuntime::decode_at(job.get(), state_row.value())?;
        let state = &runtime.state;
        let exact_state = if action.preserve_unknown {
            state_row.value() == action.expected_runtime
        } else {
            let mut expected = JobRuntime::decode_at(job.get(), &action.expected_runtime)?;
            expected.state.outcome = state.outcome.clone();
            expected == runtime
                && state
                    .outcome
                    .as_ref()
                    .is_some_and(|outcome| Some(outcome.at) == allocation.stop_confirmed_at)
        };
        drop(state_row);
        let outcome_valid = state.outcome.as_ref().is_some_and(|outcome| {
            outcome.kind == OutcomeKind::Unknown
                && (action.preserve_unknown
                    || outcome.cause == Some(OutcomeCause::Uncertain(action.reason)))
        });
        let mut expected_allocation =
            allocations::decode_pool_allocation(job.get(), &action.expected_allocation)?;
        expected_allocation.phase = AllocationPhase::Released;
        expected_allocation.stop_confirmed_at = allocation.stop_confirmed_at;
        expected_allocation.stop_proof = Some(action.proof);
        let mut ready = false;
        for entry in tx
            .open_table(MUTABLE_READY)
            .map_err(StorageError::from)?
            .iter()
            .map_err(StorageError::from)?
        {
            if entry.map_err(StorageError::from)?.0.value().2 == job.get() {
                ready = true;
                break;
            }
        }
        let mut result_present = false;
        for entry in tx
            .open_table(JOB_OUTPUTS)
            .map_err(StorageError::from)?
            .range((job.get(), 0)..=(job.get(), u32::MAX))
            .map_err(StorageError::from)?
        {
            let (_, value) = entry.map_err(StorageError::from)?;
            if rows::output_kind(value.value())? == OutputKind::Result {
                result_present = true;
                break;
            }
        }
        if allocation != expected_allocation
            || allocation.stop_confirmed_at.is_none()
            || !outcome_valid
            || !exact_state
            || ready
            || result_present
            || !resolved_unknown_ownership(&tx, state)?
        {
            return Err(ReservedRefusal::new(
                "audit_failed",
                format!("{job} does not have the exact recovered Unknown ownership"),
            ));
        }
        let active_key = (
            action.token.pool.get(),
            action.token.executor.get(),
            job.get(),
        );
        if tx
            .open_table(ACTIVE_ALLOCATIONS)
            .map_err(StorageError::from)?
            .get(active_key)
            .map_err(StorageError::from)?
            .is_some()
            || tx
                .open_table(ALLOCATION_REQUESTS)
                .map_err(StorageError::from)?
                .get(allocation.request_id.get())
                .map_err(StorageError::from)?
                .map(|row| row.value())
                != Some(job.get())
            || tx
                .open_table(PENDING)
                .map_err(StorageError::from)?
                .get(job.get())
                .map_err(StorageError::from)?
                .is_some()
            || runtime.pending != Some((0, 0))
        {
            return Err(ReservedRefusal::new(
                "audit_failed",
                format!("{job} has inconsistent recovered derived indexes"),
            ));
        }
        recovered.push(json!({
            "job": job.to_string(),
            "allocation_phase": "released",
            "job_outcome": "unknown",
            "stop_proof": recovery_proof_name(action.proof),
            "unknown_preserved_exactly": action.preserve_unknown,
            "dispatchable": false,
            "original_evidence_preserved": true,
        }));
    }
    let mut held = 0u64;
    for entry in tx
        .open_table(POOL_ALLOCATIONS)
        .map_err(StorageError::from)?
        .iter()
        .map_err(StorageError::from)?
    {
        let (key, value) = entry.map_err(StorageError::from)?;
        if allocations::decode_pool_allocation(key.value(), value.value())?.holds_capacity() {
            held += 1;
        }
    }
    let mut unresolved_unknown = 0u64;
    let mut resolved_unknown = 0u64;
    for entry in tx
        .open_table(JOB_RUNTIME)
        .map_err(StorageError::from)?
        .iter()
        .map_err(StorageError::from)?
    {
        let (key, value) = entry.map_err(StorageError::from)?;
        let state = state::decode_state(key.value(), value.value())?;
        if state
            .outcome
            .as_ref()
            .is_some_and(|outcome| outcome.kind == OutcomeKind::Unknown)
        {
            if resolved_unknown_ownership(&tx, &state)? {
                resolved_unknown += 1;
            } else {
                unresolved_unknown += 1;
                let retained = tx
                    .open_table(POOL_ALLOCATIONS)
                    .map_err(StorageError::from)?
                    .get(key.value())
                    .map_err(StorageError::from)?
                    .map(|row| allocations::decode_pool_allocation(key.value(), row.value()))
                    .transpose()?
                    .is_some_and(|allocation| allocation.holds_capacity());
                if !retained {
                    return Err(ReservedRefusal::new(
                        "audit_failed",
                        format!(
                            "{} is unresolved Unknown without a retained held Allocation",
                            state.job_id
                        ),
                    ));
                }
            }
        }
    }
    if held == 0 && unresolved_unknown == 0 {
        refuse_unresolved_ownership(&tx)?;
    }
    let clean = tx
        .open_table(META)
        .map_err(StorageError::from)?
        .get("clean_shutdown")
        .map_err(StorageError::from)?
        .is_some_and(|value| value.value() == [0]);
    if !clean {
        return Err(ReservedRefusal::new(
            "audit_failed",
            "recovery did not leave clean_shutdown = 0",
        ));
    }
    Ok(json!({
        "recovered": recovered,
        "remaining_held_allocations": held,
        "remaining_unresolved_unknown": unresolved_unknown,
        "resolved_unknown": resolved_unknown,
        "allocation_indexes": "consistent with authoritative rows",
        "ready_or_pending_recovered_jobs": 0,
        "logical_checks": "every record header and native Allocation invariant verified",
    }))
}

pub(crate) fn verify_held_publication_delta(
    source: &std::fs::File,
    published: &std::fs::File,
    source_sha256: &str,
    expected_plan: &serde_json::Value,
    expected_digest: &str,
) -> Result<serde_json::Value, ReservedRefusal> {
    use serde_json::json;

    let source = finalization_copy(source)?;
    let published = finalization_copy(published)?;
    with_verified_finalization_copy(source, "rollback", |source_database| {
        let plan = held_recovery_plan_in(&*source_database, source_sha256)?;
        if plan.digest != expected_digest || &plan.evidence != expected_plan {
            return Err(ReservedRefusal::new(
                "plan_digest_mismatch",
                "the prepared plan is not the exact plan authorized by the rollback source",
            ));
        }
        with_verified_finalization_copy(published, "published", |database| {
            verify_recovered(&*database)?;
            let targets = plan
                .actions
                .iter()
                .map(|action| action.token.job.get())
                .collect::<std::collections::BTreeSet<_>>();
            let released = plan
                .actions
                .iter()
                .map(|action| {
                    (
                        action.token.pool.get(),
                        action.token.executor.get(),
                        action.token.job.get(),
                    )
                })
                .collect::<std::collections::BTreeSet<_>>();
            verify_unrelated_recovery_rows(&*source_database, &*database, &targets, None)?;
            verify_held_publication_preservation(&*source_database, &*database, &released)?;
            let mut audit = audit_held_recovery(&*database, &plan)?;
            audit["unrelated_allocations_and_jobs"] =
                json!("byte-identical to the rollback source");
            audit["preserved_evidence_and_derived_rows"] = json!(
                "byte-identical to the rollback source except authorized target and active-index deltas"
            );
            Ok(json!({
                "plan": plan.evidence,
                "plan_digest": plan.digest,
                "result": audit,
                "page_checksums": "every page checksum verified",
                "clean_shutdown": false,
                "reconstructed_without_reapply": true,
            }))
        })
    })
}
