use ::redb::ReadableTableMetadata;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use super::{
    StorageError, execution,
    scheduled_external::{ScheduledPolicies, ScheduledPolicyMatch},
};
use crate::db::entities::{
    AllocationPhase, DevelopmentCandidate, ExecutionOperation, FrozenConfiguration,
    FrozenDecisionConfig, FrozenExecutionConfig, JobId, LlmTransport, MutableJobPhase,
    OutcomeCause, OutcomeKind, OutcomeRecord, RecordRef, Violation,
};
use crate::db::redb::allocations::{
    STOPPED_BEFORE_START_CODE, STOPPED_BEFORE_START_SUMMARY, active_allocations,
    allocation_by_request, decode_allocation_executor, decode_allocation_pool,
    decode_pool_allocation, encode_pool_allocation, valid_reservation_request_id,
};
use crate::db::redb::batch::{Batch, BatchError, Failure, Probe};
use crate::db::redb::codec::{Code, encode_scheduled_external_binding};
use crate::db::redb::jobs::{decode_mutable_job, mutable_priority};
use crate::db::redb::reads::read;
use crate::db::redb::rows::{decode_attachment, decode_output};
use crate::db::redb::schema::{
    ACTIVE_ALLOCATIONS, ALLOCATION_REQUESTS, ATTACHMENTS, CREDENTIALS, DATABASE_FORMAT_KEY,
    EXECUTORS, FORMAT_VERSION, JOB_EVENTS, JOB_OUTPUTS, JOB_RUNTIME, JOBS, META, MUTABLE_READY,
    PENDING, POOL_ALLOCATIONS, POOLS, RECURRENCE_DUE, REQUEST_EVENTS, SCHEDULED_EXTERNAL_BINDING,
    TOOL_CALLS,
};
use crate::db::redb::{allocations, reads, state};
use crate::host::identity::{current_boot_id, current_host_id};
use ::redb::{Database, ReadableTable};

static PROMOTION_CLOCK_BASE: OnceLock<(i64, Instant)> = OnceLock::new();

pub(super) fn init_promotion_clock(wall_time_at_open: i64) {
    let _ = PROMOTION_CLOCK_BASE.set((wall_time_at_open, Instant::now()));
}

static PROMOTION_HIGH_WATER: AtomicI64 = AtomicI64::new(i64::MIN);

fn promotion_clock() -> i64 {
    let wall = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_micros()).unwrap_or(i64::MAX))
        .unwrap_or(0);
    let reading =
        match PROMOTION_CLOCK_BASE.get() {
            Some((wall_base, instant_base)) => wall.max(wall_base.saturating_add(
                i64::try_from(instant_base.elapsed().as_micros()).unwrap_or(i64::MAX),
            )),
            None => wall,
        };
    PROMOTION_HIGH_WATER
        .fetch_max(reading, Ordering::SeqCst)
        .max(reading)
}

pub(crate) struct QueueCounts {
    pub(crate) pending: u64,
    pub(crate) ready: u64,
    pub(crate) delayed: u64,
    pub(crate) running: u64,
    pub(crate) held: u64,
}

pub(crate) fn queue_counts(
    database: &Database,
    for_update: bool,
) -> Result<QueueCounts, StorageError> {
    let rx = read(database)?;
    let pending_table = rx.open_table(PENDING)?;
    let pending = pending_table.len()?;
    let schedules = rx.open_table(RECURRENCE_DUE)?;
    let delayed = if for_update {
        u64::from(
            schedules
                .first()?
                .is_some_and(|(key, _)| key.value().0 <= promotion_clock()),
        )
    } else {
        schedules.len()?
    };
    let ready = rx.open_table(MUTABLE_READY)?.len()?;
    let allocations = rx.open_table(POOL_ALLOCATIONS)?;
    let (mut running, mut held) = (0, 0);
    for entry in rx.open_table(ACTIVE_ALLOCATIONS)?.iter()? {
        let (_, _, job) = entry?.0.value();
        let bytes = allocations.get(job)?.ok_or_else(|| {
            StorageError::corrupted("active Allocation index references a missing Allocation")
        })?;
        let allocation = allocations::decode_pool_allocation(job, bytes.value())?;
        match allocation.phase {
            AllocationPhase::Running | AllocationPhase::StopRequested => running += 1,
            AllocationPhase::Reserved | AllocationPhase::Uncertain => {}
            AllocationPhase::Released => {
                return Err(StorageError::corrupted(
                    "active Allocation index references a released Allocation",
                ));
            }
        }
        held += 1;
    }
    Ok(QueueCounts {
        pending,
        ready,
        delayed,
        running,
        held,
    })
}

impl Batch {
    pub(crate) fn reserve_next_development_job(
        &mut self,
        request: crate::db::entities::DevelopmentReservationRequest,
        eligible: fn(&FrozenConfiguration, bool) -> bool,
    ) -> Result<crate::db::entities::DevelopmentReservation, BatchError> {
        use crate::db::entities::{
            DevelopmentReadyCursor, DevelopmentReservation, MutableJobPhase,
        };
        use std::ops::Bound::{Excluded, Unbounded};

        const SCAN_BUDGET: usize = 128;
        self.require_allocation_durability()?;
        if !valid_reservation_request_id(request.id) {
            return self.reject(Violation::InvalidValue(
                "reservation_request",
                "expected a UUIDv7 request identity",
            ));
        }
        let existing = self.step(|tx| {
            allocation_by_request(
                &tx.open_table(ALLOCATION_REQUESTS)?,
                &tx.open_table(POOL_ALLOCATIONS)?,
                request.id,
            )
        })?;
        if let Some(allocation) = existing {
            if !allocation.matches_request(request) {
                return self.reject(Violation::InvalidValue(
                    "reservation_request",
                    "request identity has different parameters",
                ));
            }
            self.probe = Some(Probe::PoolAllocation(allocation.token));
            return Ok(DevelopmentReservation::Replayed);
        }
        let pool_id = request.pool;
        let executor_id = request.executor;
        let after = request.after;
        let Some(pool) = self.get(POOLS, pool_id.get(), |bytes| {
            decode_allocation_pool(pool_id.get(), bytes)
        })?
        else {
            return self.reject(Violation::InvalidValue("pool", "Pool does not exist"));
        };
        let Some(executor) = self.get(EXECUTORS, executor_id.get(), |bytes| {
            decode_allocation_executor(executor_id.get(), bytes)
        })?
        else {
            return self.reject(Violation::InvalidValue(
                "executor",
                "Executor does not exist",
            ));
        };
        if !pool.enabled
            || pool.archived
            || !executor.enabled
            || executor.archived
            || executor.pool != pool_id
        {
            return self.reject(Violation::InvalidValue(
                "executor",
                "Executor is not eligible",
            ));
        }
        let room = self.step(|tx| {
            let active = tx.open_table(ACTIVE_ALLOCATIONS)?;
            let mut occupied = 0;
            for entry in
                active.range((pool_id.get(), 0, 0)..=(pool_id.get(), u128::MAX, u128::MAX))?
            {
                let (key, _) = entry?;
                occupied += 1;
                if key.value().1 == executor_id.get() || occupied >= pool.capacity.get() {
                    return Ok(false);
                }
            }
            Ok(true)
        })?;
        if !room {
            return self.reject(Violation::InvalidValue(
                "allocation",
                "the Executor or its Pool has no room",
            ));
        }

        let keys = self.step(|tx| {
            let ready = tx.open_table(MUTABLE_READY)?;
            let start = after.map_or(Unbounded, |cursor| Excluded(cursor.0));
            ready
                .range((start, Unbounded))?
                .take(SCAN_BUDGET)
                .map(|entry| Ok(entry?.0.value()))
                .collect::<Result<Vec<_>, StorageError>>()
        })?;
        let mut policies: Option<ScheduledPolicies> = None;
        for key in &keys {
            let id = JobId::new(key.2);
            let Some(mut job) =
                self.get(JOBS, id.get(), |bytes| decode_mutable_job(id.get(), bytes))?
            else {
                return self.corrupted("ready entry references a missing Job".to_owned());
            };
            let state = self.require_job_state(id)?;
            let previous = self.pool_allocation(id)?;
            let submitted = job.submitted_at.ok_or_else(|| {
                BatchError(Failure::Storage(StorageError::corrupted(
                    "ready Job has no submission time",
                )))
            })?;
            if job.phase != MutableJobPhase::Queued
                || state.outcome.is_some()
                || key.0 != mutable_priority(job.input.priority)
                || key.1 != submitted.as_unix_micros()
                || previous
                    .as_ref()
                    .is_some_and(|allocation| allocation.holds_capacity())
            {
                return self.corrupted("invalid mutable ready entry".to_owned());
            }
            if !self.mutable_predecessor_ready(&job)? || self.mutable_job_blocked(&job)? {
                continue;
            }
            if previous.is_none()
                && job.context_selection
                    == crate::db::entities::MutableContextSelection::FullHistory
                && job.previous.is_some()
                && let Some((producer, source)) = self.latest_verified_summary(job.parent_id, id)?
            {
                job.context_selection =
                    crate::db::entities::MutableContextSelection::AdoptedSummary {
                        producer,
                        source,
                    };
            }
            if !self.mutable_context_valid(&job, job.context_selection)? {
                continue;
            }
            if matches!(
                &job.input.content,
                crate::db::entities::MutableJobContent::Generative { .. }
            ) {
                match self.require_generative_predecessors(&job) {
                    Ok(()) => {}
                    Err(BatchError(Failure::Rejected(_))) => continue,
                    Err(error) => return Err(error),
                }
            }
            let summary_work = self.is_summary_work(&job)?;
            if !summary_work
                && matches!(&job.input.content, crate::db::entities::MutableJobContent::Generative { configuration, .. } if configuration.is_none_or(|config| config.agent_profile.is_none()))
            {
                continue;
            }
            if self.step(|tx| Ok(state::pending(tx, id)?.is_some_and(|counts| counts.0 > 0)))? {
                continue;
            }
            let mut attachments_available = true;
            for attachment in job.input.attachments() {
                let record = self.get(ATTACHMENTS, attachment.get(), |bytes| {
                    decode_attachment(attachment.get(), bytes)
                })?;
                if !record.is_some_and(|record| record.is_available()) {
                    attachments_available = false;
                    break;
                }
            }
            if !attachments_available {
                continue;
            }
            let frozen = self.frozen_configuration(&job)?;
            if self.unsupported_start_context(&job)?.is_some() || !eligible(&frozen, summary_work) {
                continue;
            }
            let scheduled_external = match &frozen {
                FrozenConfiguration::Generative(target) => {
                    let policies = match &mut policies {
                        Some(policies) => policies,
                        slot @ None => slot.insert(self.scheduled_external_policies()?),
                    };
                    match policies.matching(target) {
                        ScheduledPolicyMatch::One { id, revision } => Some((id, revision)),
                        ScheduledPolicyMatch::Ambiguous => continue,
                        ScheduledPolicyMatch::None => None,
                    }
                }
                FrozenConfiguration::Decision(_) => None,
            };
            let (operation, protocol) = match &frozen {
                FrozenConfiguration::Generative(frozen) => (
                    ExecutionOperation::LlmRequest,
                    Some(frozen.llm.provider.transport),
                ),
                FrozenConfiguration::Decision(FrozenDecisionConfig::Native(_)) => {
                    (ExecutionOperation::Decision, None)
                }
                FrozenConfiguration::Decision(FrozenDecisionConfig::Generative(frozen, _)) => (
                    ExecutionOperation::LlmRequest,
                    Some(frozen.llm.provider.transport),
                ),
            };
            if key.3 != requirement(Some(operation), protocol)
                || !executor.capabilities.contains(&(operation, protocol))
            {
                continue;
            }
            return self
                .reserve_pool_allocation(
                    &pool,
                    &executor,
                    &job,
                    *key,
                    request,
                    operation,
                    protocol,
                    scheduled_external,
                    previous.as_ref(),
                )
                .map(|allocation| DevelopmentReservation::Reserved(Box::new(allocation)));
        }
        if keys.len() == SCAN_BUDGET
            && let Some(key) = keys.last()
        {
            return Ok(DevelopmentReservation::Continue(DevelopmentReadyCursor(
                *key,
            )));
        }
        Ok(DevelopmentReservation::Exhausted)
    }

    #[allow(clippy::too_many_arguments)]
    fn reserve_pool_allocation(
        &mut self,
        pool: &crate::db::entities::AllocationPool,
        executor: &crate::db::entities::AllocationExecutor,
        row: &crate::db::entities::MutableJob,
        ready_key: (u8, i64, u128, u16),
        request: crate::db::entities::DevelopmentReservationRequest,
        operation: ExecutionOperation,
        protocol: Option<LlmTransport>,
        scheduled_external: Option<(crate::db::entities::ScheduledExternalPolicyId, u32)>,
        previous: Option<&crate::db::entities::PoolAllocation>,
    ) -> Result<crate::db::entities::PoolAllocation, BatchError> {
        use crate::db::entities::{AllocationPhase, AllocationToken, PoolAllocation};
        let snapshot = self
            .step(|tx| execution::replay_job(tx, row.id, None))?
            .ok_or_else(|| {
                BatchError(Failure::Storage(StorageError::corrupted(
                    "ready Job has no execution history",
                )))
            })?;
        if snapshot.outcome.is_some()
            || snapshot.phase != Some(MutableJobPhase::Queued)
            || snapshot.input_revision != Some(row.revision)
            || !snapshot.dependencies.is_empty()
            || snapshot.allocation.as_ref() != previous
            || previous.is_some()
        {
            return self
                .corrupted("ready Job disagrees with authoritative execution history".to_owned());
        }
        let generation = std::num::NonZeroU32::MIN;
        let allocation = PoolAllocation {
            request_id: request.id,
            request_after: request.after,
            token: AllocationToken {
                job: row.id,
                pool: pool.id,
                executor: executor.id,
                generation,
                epoch: self.epoch,
            },
            job_revision: row.revision,
            phase: AllocationPhase::Reserved,
            claimed_at: self.now,
            started_at: None,
            host_id: current_host_id(),
            boot_id: current_boot_id(),
            pool_revision: pool.revision,
            executor_revision: executor.revision,
            operation,
            protocol,
            stop_confirmed_at: None,
            stop_proof: None,
        };
        self.put_pool_allocation(&allocation, previous)?;
        self.step(|tx| {
            tx.open_table(MUTABLE_READY)?.remove(ready_key)?;
            tx.open_table(PENDING)?.remove(row.id.get())?;
            let mut bindings = tx.open_table(SCHEDULED_EXTERNAL_BINDING)?;
            match scheduled_external {
                Some((policy, policy_revision)) => {
                    let binding = crate::db::entities::ScheduledExternalBinding {
                        job: row.id,
                        policy,
                        policy_revision,
                        allocation_epoch: allocation.token.epoch,
                        allocation_generation: allocation.token.generation,
                    };
                    bindings.insert(
                        row.id.get(),
                        encode_scheduled_external_binding(&binding).as_slice(),
                    )?;
                }
                None => {
                    bindings.remove(row.id.get())?;
                }
            }
            Ok(())
        })?;
        self.probe = Some(Probe::PoolAllocation(allocation.token));
        Ok(allocation)
    }

    fn allocation_for_token(
        &mut self,
        token: crate::db::entities::AllocationToken,
    ) -> Result<crate::db::entities::PoolAllocation, BatchError> {
        let Some(allocation) = self.get(POOL_ALLOCATIONS, token.job.get(), |bytes| {
            decode_pool_allocation(token.job.get(), bytes)
        })?
        else {
            return self.reject(Violation::InvalidValue(
                "allocation",
                "allocation does not exist",
            ));
        };
        if allocation.token != token {
            return self.reject(Violation::InvalidValue(
                "allocation",
                "stale allocation token",
            ));
        }
        Ok(allocation)
    }

    pub(crate) fn pool_allocation(
        &mut self,
        job: JobId,
    ) -> Result<Option<crate::db::entities::PoolAllocation>, BatchError> {
        self.get(POOL_ALLOCATIONS, job.get(), |bytes| {
            decode_pool_allocation(job.get(), bytes)
        })
    }

    pub(super) fn revalidate_development_snapshot(
        &mut self,
        candidate: &DevelopmentCandidate,
    ) -> Result<(), BatchError> {
        use crate::db::entities::{AllocationPhase, MutableJobPhase};
        let (allocation, job, pool, executor, conversation, lifecycle, project) = match candidate {
            DevelopmentCandidate::Generative(context) => (
                &context.allocation,
                &context.job,
                &context.pool,
                &context.executor,
                &context.conversation,
                &context.conversation_lifecycle,
                &context.project,
            ),
            DevelopmentCandidate::Decision(context) => (
                &context.allocation,
                &context.job,
                &context.pool,
                &context.executor,
                &context.conversation,
                &context.conversation_lifecycle,
                &context.project,
            ),
        };
        let stale = || {
            BatchError(Failure::Rejected(Violation::InvalidValue(
                "handoff",
                "execution snapshot changed",
            )))
        };
        let token = allocation.token;
        let metadata_matches = self.step(|tx| {
            let meta = tx.open_table(META)?;
            Ok(meta
                .get(DATABASE_FORMAT_KEY)?
                .is_some_and(|value| value.value() == FORMAT_VERSION.to_le_bytes())
                && meta
                    .get("open_epoch")?
                    .is_some_and(|value| value.value() == token.epoch.to_le_bytes()))
        })?;
        if !metadata_matches {
            return Err(stale());
        }
        let bounded = self.step(|tx| {
            Ok(tx
                .open_table(JOBS)?
                .get(job.id.get())?
                .is_some_and(|value| value.value().len() <= 8 * 1024 * 1024))
        })?;
        if !bounded {
            return Err(stale());
        }
        let state = self.require_job_state(job.id)?;
        if token.epoch != self.epoch
            || allocation.phase != AllocationPhase::Reserved
            || self.pool_allocation(token.job)?.as_ref() != Some(allocation)
            || self.mutable_job(token.job)?.as_ref() != Some(job)
            || job.phase != MutableJobPhase::Queued
            || job.revision != allocation.job_revision
            || state.outcome.is_some()
        {
            return Err(stale());
        }
        let current_pool = self.get(POOLS, token.pool.get(), |bytes| {
            decode_allocation_pool(token.pool.get(), bytes)
        })?;
        let current_executor = self.get(EXECUTORS, token.executor.get(), |bytes| {
            decode_allocation_executor(token.executor.get(), bytes)
        })?;
        if current_pool.as_ref().is_none_or(|current| {
            !current.enabled
                || current.archived
                || current.id != token.pool
                || current.id != pool.id
                || current.revision < allocation.pool_revision
                || current.revision < pool.revision
        }) || current_executor.as_ref().is_none_or(|current| {
            !current.enabled
                || current.archived
                || current.id != token.executor
                || current.id != executor.id
                || current.pool != token.pool
                || current.revision < allocation.executor_revision
                || current.revision < executor.revision
                || !current
                    .capabilities
                    .contains(&(allocation.operation, allocation.protocol))
        }) || !pool.enabled
            || pool.archived
            || !executor.enabled
            || executor.archived
            || executor.pool != pool.id
            || pool.id != token.pool
            || executor.id != token.executor
            || !executor
                .capabilities
                .contains(&(allocation.operation, allocation.protocol))
        {
            return Err(stale());
        }
        let occupancy_valid = self.step(|tx| {
            let held =
                active_allocations(&tx.open_table(ACTIVE_ALLOCATIONS)?, token.pool.get(), None)?;
            let executor_exclusive = held
                .iter()
                .filter(|(owner, _)| *owner == token.executor.get())
                .all(|(_, owner_job)| *owner_job == token.job.get());
            let own = held.iter().any(|(owner, owner_job)| {
                *owner == token.executor.get() && *owner_job == token.job.get()
            });
            Ok(executor_exclusive && own)
        })?;
        if !occupancy_valid {
            return Err(stale());
        }
        if self.require_mutable_conversation(job.parent_id)? != *conversation
            || self.job_lifecycle(job.parent_id)? != *lifecycle
        {
            return Err(stale());
        }
        if let Some(project) = project
            && self.require_project_record(project.id)? != *project
        {
            return Err(stale());
        }
        let no_execution_evidence = self.step(|tx| {
            Ok(tx
                .open_table(JOB_OUTPUTS)?
                .range((job.id.get(), 0)..=(job.id.get(), u32::MAX))?
                .next()
                .transpose()?
                .is_none()
                && tx
                    .open_table(REQUEST_EVENTS)?
                    .range((job.id.get(), 0, 0)..=(job.id.get(), u32::MAX, u32::MAX))?
                    .next()
                    .transpose()?
                    .is_none()
                && tx
                    .open_table(TOOL_CALLS)?
                    .range((job.id.get(), 0, 0)..=(job.id.get(), u32::MAX, u32::MAX))?
                    .next()
                    .transpose()?
                    .is_none())
        })?;
        if !no_execution_evidence {
            return Err(stale());
        }
        let frozen = self.frozen_configuration(job)?;
        if mutable_configuration_requirement(&frozen)
            != requirement(Some(allocation.operation), allocation.protocol)
        {
            return Err(stale());
        }
        match (candidate, frozen) {
            (
                DevelopmentCandidate::Decision(context),
                FrozenConfiguration::Decision(configuration),
            ) => {
                if context.configuration != configuration {
                    return Err(stale());
                }
            }
            (
                DevelopmentCandidate::Generative(context),
                FrozenConfiguration::Generative(configuration),
            ) => {
                if context.configuration != configuration {
                    return Err(stale());
                }
                let compaction = self
                    .step(|tx| reads::catalog::global_compaction_settings(&tx.open_table(META)?))?;
                if context.compaction != compaction {
                    return Err(stale());
                }
                if let Some(summary) = &context.adopted_summary {
                    let crate::db::entities::MutableContextSelection::AdoptedSummary {
                        producer,
                        source,
                    } = job.context_selection
                    else {
                        return Err(stale());
                    };
                    if source != summary.summary.source {
                        return Err(stale());
                    }
                    // A prepared handoff is a durability promise: if its source
                    // can no longer be verified the preparation is unusable, and
                    // the damage must surface instead of being reported as staleness.
                    let current = self.step(|tx| {
                        reads::execution_context::verify_native_summary_in(
                            tx,
                            job.parent_id,
                            job.id,
                            producer,
                            source,
                        )
                        .map_err(reads::execution_context::SummarySourceError::into_storage)
                    })?;
                    let Some(current) = current else {
                        return Err(stale());
                    };
                    if current.manifest != summary.manifest || current.summary != summary.summary {
                        return Err(stale());
                    }
                } else if !matches!(
                    job.context_selection,
                    crate::db::entities::MutableContextSelection::FullHistory
                ) {
                    return Err(stale());
                }
                let mut previous = job.previous;
                for turn in context.history.iter().rev() {
                    let bounded = self.step(|tx| {
                        Ok(tx
                            .open_table(JOBS)?
                            .get(turn.record.job.id.get())?
                            .is_some_and(|value| value.value().len() <= 8 * 1024 * 1024))
                    })?;
                    if !bounded {
                        return Err(stale());
                    }
                    if previous != Some(turn.record.job.id)
                        || self.read_mutable_job(turn.record.job.id)?.as_ref() != Some(&turn.record)
                        || turn.record.job.input.attachments()
                            != turn
                                .attachments
                                .iter()
                                .map(|attachment| attachment.id)
                                .collect::<Vec<_>>()
                    {
                        return Err(stale());
                    }
                    let outputs = self.step(|tx| {
                        let mut outputs = Vec::new();
                        let mut counted = std::collections::BTreeSet::new();
                        let mut bytes = 0usize;
                        let table = tx.open_table(JOB_OUTPUTS)?;
                        for entry in table
                            .range(
                                (turn.record.job.id.get(), 0)
                                    ..=(turn.record.job.id.get(), u32::MAX),
                            )?
                            .take(turn.outputs.len().saturating_add(1))
                        {
                            let (key, value) = entry?;
                            let row = decode_output(key.value(), value.value())?;
                            bytes =
                                bytes.saturating_add(super::observations::context_output_bytes(
                                    tx,
                                    &row,
                                    value.value().len(),
                                    &mut counted,
                                )?);
                            if bytes > 8 * 1024 * 1024 {
                                return Ok(None);
                            }
                            outputs.push(row);
                        }
                        Ok(Some(outputs))
                    })?;
                    let answer = self.step(|tx| {
                        reads::execution_context::projected_answer(
                            tx,
                            &turn.record,
                            turn.outputs.last(),
                        )
                    })?;
                    if answer != turn.answer_text || outputs.as_ref() != Some(&turn.outputs) {
                        return Err(stale());
                    }
                    previous = turn.record.job.previous;
                }
                if job.summary_source.is_none() {
                    for input in context.covered_inputs.iter().rev() {
                        if previous != Some(input.job) {
                            return Err(stale());
                        }
                        let bounded = self.step(|tx| {
                            Ok(tx
                                .open_table(JOBS)?
                                .get(input.job.get())?
                                .is_some_and(|value| value.value().len() <= 8 * 1024 * 1024))
                        })?;
                        if !bounded {
                            return Err(stale());
                        }
                        let row = self.read_mutable_job(input.job)?.ok_or_else(stale)?;
                        if row.job.input.text() != input.text
                            || row.job.input.attachments()
                                != input
                                    .attachments
                                    .iter()
                                    .map(|attachment| attachment.id)
                                    .collect::<Vec<_>>()
                        {
                            return Err(stale());
                        }
                        previous = row.job.previous;
                    }
                }
                if previous.is_some() {
                    return Err(stale());
                }
                if job.input.attachments()
                    != context
                        .attachments
                        .iter()
                        .map(|attachment| attachment.id)
                        .collect::<Vec<_>>()
                {
                    return Err(stale());
                }
                for attachment in context
                    .covered_inputs
                    .iter()
                    .flat_map(|input| &input.attachments)
                    .chain(context.history.iter().flat_map(|turn| &turn.attachments))
                    .chain(&context.attachments)
                {
                    let current = self.get(ATTACHMENTS, attachment.id.get(), |bytes| {
                        decode_attachment(attachment.id.get(), bytes)
                    })?;
                    if current.as_ref() != Some(attachment) || !attachment.is_available() {
                        return Err(stale());
                    }
                }
            }
            _ => return Err(stale()),
        }
        Ok(())
    }

    pub(crate) fn start_mutable_job(
        &mut self,
        prepared: &crate::db::handoff::PreparedDevelopmentHandoff,
    ) -> Result<crate::db::entities::MutableJob, BatchError> {
        use crate::db::entities::{AllocationPhase, MutableJobPhase};
        self.require_allocation_durability()?;
        if !prepared.has_bounded_inputs() {
            return self.reject(Violation::InvalidValue(
                "handoff",
                "prepared inputs are incomplete",
            ));
        }
        let candidate = &prepared.candidate;
        self.revalidate_development_snapshot(candidate)?;
        let job_id = match candidate {
            DevelopmentCandidate::Generative(context) => context.job.id,
            DevelopmentCandidate::Decision(context) => context.job.id,
        };
        self.scheduled_external_authorization(job_id)?;
        let (snapshot, allocation) = match candidate {
            DevelopmentCandidate::Generative(context) => {
                let refusal = if self.is_summary_work(&context.job)? {
                    context.configuration.provider_refusal()
                } else {
                    context.configuration.native_execution_refusal()
                };
                if refusal.is_some() {
                    return self.reject(Violation::InvalidValue(
                        "handoff",
                        "execution policy or provider requires unsupported preparation",
                    ));
                }
                (&context.job, &context.allocation)
            }
            DevelopmentCandidate::Decision(context) => (&context.job, &context.allocation),
        };
        if !self.mutable_predecessor_ready(snapshot)? {
            return self.reject(Violation::InvalidValue(
                "handoff",
                "conversation predecessor is not settled",
            ));
        }
        if self.mutable_job_blocked(snapshot)? {
            return self.reject(Violation::InvalidValue(
                "handoff",
                "conversation is not eligible",
            ));
        }
        let expected = prepared.credential_references();
        let evidence: Vec<(&str, &[u8])> = prepared.credential_evidence().collect();
        let mut credential_matches = expected.len() == evidence.len()
            && expected
                .iter()
                .all(|reference| evidence.iter().any(|(held, _)| held == reference));
        if credential_matches {
            for (reference, encrypted) in &evidence {
                let unchanged = self.step(|tx| {
                    Ok(tx
                        .open_table(CREDENTIALS)?
                        .get(*reference)?
                        .is_some_and(|row| row.value() == *encrypted))
                })?;
                if !unchanged {
                    credential_matches = false;
                    break;
                }
            }
        }
        if !credential_matches {
            return self.reject(Violation::InvalidValue(
                "handoff",
                "credential changed or is unavailable",
            ));
        }
        if let Some(reason) = self.unsupported_start_context(snapshot)? {
            return self.reject(Violation::InvalidValue("handoff", reason));
        }
        if snapshot.frozen.is_none() {
            return self.reject(Violation::InvalidValue(
                "handoff",
                "the Job has no frozen configuration",
            ));
        }
        let mut job = snapshot.clone();
        let mut allocation = allocation.clone();
        job.phase = MutableJobPhase::Executing;
        allocation.phase = AllocationPhase::Running;
        allocation.started_at = Some(self.now);
        self.store_mutable_job(&job)?;
        let previous = self.allocation_for_token(allocation.token)?;
        self.put_pool_allocation(&allocation, Some(&previous))?;
        self.catalog_changed = true;
        self.probe = Some(Probe::MutableJobStarted(allocation.token, job.revision));
        Ok(job)
    }

    pub(super) fn require_development_write_for(
        &mut self,
        proof: &crate::db::handoff::DevelopmentWriteProof,
        operation: Option<ExecutionOperation>,
    ) -> Result<crate::db::entities::MutableJob, BatchError> {
        use crate::db::entities::{AllocationPhase, MutableFrozenConfiguration, MutableJobPhase};
        self.require_allocation_durability()?;
        let token = proof.token();
        let rejected =
            || Violation::InvalidValue("execution", "stale or unauthorized execution ownership");
        let allocation = self.pool_allocation(token.job)?;
        let job = self.mutable_job(token.job)?;
        let Some(allocation) = allocation else {
            return self.reject(rejected());
        };
        let Some(job) = job else {
            return self.reject(rejected());
        };
        let state = self.require_job_state(token.job)?;
        if token.epoch != self.epoch
            || allocation.token != token
            || !matches!(
                allocation.phase,
                AllocationPhase::Running | AllocationPhase::StopRequested
            )
            || operation.is_some_and(|operation| allocation.operation != operation)
            || allocation.job_revision != proof.revision()
            || allocation.stop_confirmed_at.is_some()
            || allocation.stop_proof.is_some()
            || job.id != token.job
            || job.revision != proof.revision()
            || job.phase != MutableJobPhase::Executing
            || !matches!(
                (allocation.operation, &job.frozen),
                (
                    ExecutionOperation::LlmRequest,
                    Some(MutableFrozenConfiguration::Generative(_))
                ) | (
                    ExecutionOperation::Decision,
                    Some(MutableFrozenConfiguration::Decision(_))
                )
            )
            || state.outcome.is_some()
            || (self.job_lifecycle(job.parent_id)?.closing.is_some()
                && allocation.phase != AllocationPhase::StopRequested)
        {
            return self.reject(rejected());
        }
        Ok(job)
    }
}

impl Batch {
    pub(super) fn end_untouched_reservation(
        &mut self,
        token: crate::db::entities::AllocationToken,
        kind: OutcomeKind,
        result: crate::db::entities::FinalResult,
    ) -> Result<
        (
            crate::db::entities::MutableJob,
            crate::db::entities::AppendedOutput,
        ),
        BatchError,
    > {
        use crate::db::entities::{AllocationPhase, MutableJobPhase};
        self.require_allocation_durability()?;
        let refused = |reason: &'static str| {
            BatchError(Failure::Rejected(Violation::InvalidValue(
                "reservation",
                reason,
            )))
        };
        let mut allocation = self.allocation_for_token(token)?;
        if token.epoch != self.epoch {
            return Err(refused("the Allocation belongs to another database epoch"));
        }
        if allocation.phase != AllocationPhase::Reserved
            || allocation.stop_confirmed_at.is_some()
            || allocation.stop_proof.is_some()
        {
            return Err(refused("the Allocation is not an untouched reservation"));
        }
        let Some(job) = self.mutable_job(token.job)? else {
            return Err(refused("the reserved Job does not exist"));
        };
        let state = self.require_job_state(token.job)?;
        if job.id != token.job
            || job.phase != MutableJobPhase::Queued
            || job.revision != allocation.job_revision
            || job.frozen.is_none()
            || state.outcome.is_some()
        {
            return Err(refused(
                "the Job is not the queued revision that was reserved",
            ));
        }
        let (seq, id) = self.settle_mutable_outcome(token, &job, kind, result)?;
        let previous = allocation.clone();
        allocation.phase = AllocationPhase::Released;
        self.put_pool_allocation(&allocation, Some(&previous))?;
        self.settle_standalone_decision(&job)?;
        self.freed_capacity = true;
        self.probe = Some(Probe::Outcome(token.job));
        Ok((
            job,
            crate::db::entities::AppendedOutput {
                output: id,
                seq: i64::from(seq),
            },
        ))
    }

    pub(crate) fn recover_interrupted(
        &mut self,
        allocation: crate::db::entities::PoolAllocation,
        proof: Option<crate::db::entities::StopProof>,
    ) -> Result<(), BatchError> {
        if allocation.token.epoch != self.epoch {
            return self.corrupted("interrupted allocation epoch mismatch".to_owned());
        }
        let job = allocation.token.job;
        if allocation.phase == AllocationPhase::Reserved {
            self.fail_unstarted_reservation(
                allocation.token,
                crate::db::entities::UnstartedCause::HostShutdown,
            )?;
            self.child_turn_settled(job)?;
            return Ok(());
        }
        if allocation.phase != AllocationPhase::Uncertain {
            let mut state = self.require_job_state(job)?;
            if state.outcome.is_some() {
                return self.corrupted("interrupted Job already settled".to_owned());
            }
            state.outcome = Some(OutcomeRecord {
                kind: OutcomeKind::Unknown,
                at: self.now,
                cause: Some(OutcomeCause::Uncertain(
                    crate::db::entities::UncertaintyReason::HostExited,
                )),
            });
            self.put_job_state(job, &state)?;
        }
        if let Some(proof) = proof {
            return self.release_with_proof(allocation, proof);
        }
        if allocation.phase != AllocationPhase::Uncertain {
            let mut next = allocation.clone();
            next.phase = AllocationPhase::Uncertain;
            self.put_pool_allocation(&next, Some(&allocation))?;
        }
        self.child_turn_settled(job)?;
        Ok(())
    }

    pub(crate) fn release_uncertain(
        &mut self,
        token: crate::db::entities::AllocationToken,
        revision: u64,
        proof: crate::db::entities::StopProof,
        expected: Option<&[u8]>,
    ) -> Result<bool, BatchError> {
        self.require_allocation_durability()?;
        if let Some(expected) = expected
            && !self.step(|tx| {
                Ok(tx
                    .open_table(POOL_ALLOCATIONS)?
                    .get(token.job.get())?
                    .is_some_and(|row| row.value() == expected))
            })?
        {
            return Ok(false);
        }
        let allocation = self.allocation_for_token(token)?;
        if allocation.job_revision != revision {
            return self.reject(Violation::InvalidValue(
                "allocation",
                "the allocation revision does not match the interrupted job",
            ));
        }
        if allocation.phase == AllocationPhase::Released {
            return self.reject(Violation::StopAlreadyConfirmed(token.job));
        }
        if allocation.phase != AllocationPhase::Uncertain || allocation.stop_proof.is_some() {
            return self.reject(Violation::InvalidValue(
                "allocation",
                "requires an interrupted, uncertain allocation at this revision",
            ));
        }
        self.release_with_proof(allocation, proof)?;
        Ok(true)
    }

    fn release_with_proof(
        &mut self,
        allocation: crate::db::entities::PoolAllocation,
        proof: crate::db::entities::StopProof,
    ) -> Result<(), BatchError> {
        let mut next = allocation.clone();
        next.phase = AllocationPhase::Released;
        next.stop_confirmed_at = Some(self.now);
        next.stop_proof = Some(proof);
        self.put_pool_allocation(&next, Some(&allocation))?;
        let id = allocation.token.job;
        let Some(job) = self.mutable_job(id)? else {
            return self.corrupted(format!("released Unknown Allocation has no Job {id}"));
        };
        self.finish_job_membership(&job, false)?;
        self.child_turn_settled(id)?;
        self.freed_capacity = true;
        Ok(())
    }

    pub(crate) fn reconcile_stopped_allocation(
        &mut self,
        action: &super::recovery::held::RecoveryAction,
    ) -> Result<(), BatchError> {
        use crate::db::entities::{AllocationPhase, MutableJobPhase, OutcomeRecord};

        self.require_allocation_durability()?;
        if action.token.epoch != self.epoch
            && !(action.phase == AllocationPhase::Uncertain && action.token.epoch < self.epoch)
        {
            return self.reject(Violation::InvalidValue(
                "allocation",
                "stale offline recovery epoch",
            ));
        }
        let unchanged = self.step(|tx| {
            let allocation = tx
                .open_table(POOL_ALLOCATIONS)?
                .get(action.token.job.get())?
                .is_some_and(|row| row.value() == action.expected_allocation);
            let state = tx
                .open_table(JOB_RUNTIME)?
                .get(action.token.job.get())?
                .is_some_and(|row| row.value() == action.expected_runtime);
            Ok(allocation && state)
        })?;
        if !unchanged {
            return self.reject(Violation::InvalidValue(
                "allocation",
                "offline recovery source rows changed",
            ));
        }
        let allocation = self.allocation_for_token(action.token)?;
        if allocation.phase != action.phase
            || allocation.stop_confirmed_at.is_some()
            || allocation.stop_proof.is_some()
        {
            return self.reject(Violation::InvalidValue(
                "allocation",
                "offline recovery phase changed",
            ));
        }
        let Some(job) = self.mutable_job(action.token.job)? else {
            return self.reject(Violation::NotFound(RecordRef::Job(action.token.job)));
        };
        if job.phase != MutableJobPhase::Executing || job.revision != allocation.job_revision {
            return self.reject(Violation::InvalidValue(
                "allocation",
                "offline recovery Job revision changed",
            ));
        }
        let mut state = self.require_job_state(action.token.job)?;
        match (&state.outcome, action.preserve_unknown) {
            (Some(outcome), true) if outcome.kind == OutcomeKind::Unknown => {}
            (None, false) => {
                state.outcome = Some(OutcomeRecord {
                    kind: OutcomeKind::Unknown,
                    at: self.now,
                    cause: Some(crate::db::entities::OutcomeCause::Uncertain(action.reason)),
                });
                self.put_job_state(action.token.job, &state)?;
            }
            _ => {
                return self.reject(Violation::InvalidValue(
                    "outcome",
                    "offline recovery would change an existing outcome",
                ));
            }
        }
        self.release_with_proof(allocation, action.proof)
    }

    pub(crate) fn request_native_stop(
        &mut self,
        job: JobId,
    ) -> Result<crate::db::entities::NativeStopRequest, BatchError> {
        use crate::db::entities::{AllocationPhase, NativeStopRequest, UnstartedCause};
        self.require_allocation_durability()?;
        let Some(row) = self.mutable_job(job)? else {
            return self.reject(Violation::NotFound(RecordRef::Job(job)));
        };
        let ended = Violation::InvalidValue("stop", crate::db::entities::STOP_JOB_ENDED);
        if self.require_job_state(job)?.outcome.is_some() {
            return self.reject(ended);
        }
        let allocation = self.pool_allocation(job)?;
        if allocation.is_none() && row.phase == MutableJobPhase::Queued {
            self.end_queued_native_job(
                &row,
                OutcomeCause::Text(STOPPED_BEFORE_START_SUMMARY.to_owned()),
            )?;
            return Ok(NativeStopRequest::EndedBeforeStart {
                token: None,
                outcome: OutcomeKind::Cancelled,
            });
        }
        let Some(allocation) = allocation else {
            return self.reject(Violation::InvalidValue(
                "stop",
                crate::db::entities::STOP_JOB_NOT_RUNNING,
            ));
        };
        if allocation.job_revision != row.revision {
            return self.corrupted(format!(
                "Job {job} and its Allocation disagree on the revision"
            ));
        }
        if matches!(
            allocation.phase,
            AllocationPhase::Running | AllocationPhase::StopRequested
        ) && row.phase != MutableJobPhase::Executing
        {
            return self.corrupted(format!(
                "Job {job} is not executing with its active Allocation"
            ));
        }
        let token = allocation.token;
        match allocation.phase {
            AllocationPhase::Reserved => {
                let expired = self
                    .native_stops
                    .as_ref()
                    .is_some_and(|stops| stops.deadline_has_precedence(token));
                let (outcome, summary, code) = if expired {
                    let deadline = UnstartedCause::DeadlineExceeded;
                    (deadline.outcome(), deadline.summary(), deadline.code())
                } else {
                    (
                        OutcomeKind::Cancelled,
                        STOPPED_BEFORE_START_SUMMARY,
                        STOPPED_BEFORE_START_CODE,
                    )
                };
                let (job, _) = self.end_untouched_reservation(
                    token,
                    outcome,
                    crate::db::entities::FinalResult {
                        summary: Some(summary.to_owned()),
                        code: Some(code.to_owned()),
                        goal_reached: Some(false),
                        value: None,
                    },
                )?;
                self.settle_summary_membership(&job)?;
                Ok(NativeStopRequest::EndedBeforeStart {
                    token: Some(token),
                    outcome,
                })
            }
            AllocationPhase::Running => {
                self.transition_pool_allocation(token, AllocationPhase::StopRequested, None)?;
                let seq =
                    self.step(|tx| execution::latest_event_seq(&tx.open_table(JOB_EVENTS)?, job))?;
                self.probe = Some(Probe::ExecutionEvent(job, seq));
                Ok(NativeStopRequest::Requested(token))
            }
            AllocationPhase::StopRequested => Ok(NativeStopRequest::AlreadyRequested(token)),
            AllocationPhase::Uncertain | AllocationPhase::Released => self.reject(ended),
        }
    }

    pub(super) fn transition_pool_allocation(
        &mut self,
        token: crate::db::entities::AllocationToken,
        next: crate::db::entities::AllocationPhase,
        uncertainty: Option<crate::db::entities::UncertaintyReason>,
    ) -> Result<(), BatchError> {
        use crate::db::entities::AllocationPhase;
        self.require_allocation_durability()?;
        if (next == AllocationPhase::Uncertain) != uncertainty.is_some() {
            return self.reject(Violation::InvalidValue(
                "allocation",
                "an Uncertain transition carries exactly one reason",
            ));
        }
        let mut allocation = self.allocation_for_token(token)?;
        if token.epoch != self.epoch {
            return self.reject(Violation::InvalidValue(
                "allocation",
                "stale database epoch",
            ));
        }
        if !matches!(
            allocation.phase,
            AllocationPhase::Running | AllocationPhase::StopRequested
        ) || !execution::allocation_phase_transition_allowed(allocation.phase, next)
        {
            return self.reject(Violation::InvalidValue(
                "allocation",
                "invalid allocation transition",
            ));
        }
        let mut state = self.require_job_state(token.job)?;
        if next == AllocationPhase::Released {
            if !state.confirmed_terminal() {
                return self.reject(Violation::InvalidValue(
                    "allocation",
                    "Job has no confirmed outcome",
                ));
            }
        } else if state.outcome.is_some() {
            return self.reject(Violation::InvalidValue(
                "allocation",
                "Job already has an outcome",
            ));
        } else if let Some(reason) = uncertainty {
            state.outcome = Some(OutcomeRecord {
                kind: OutcomeKind::Unknown,
                at: self.now,
                cause: Some(OutcomeCause::Uncertain(reason)),
            });
            self.put_job_state(token.job, &state)?;
        }
        let previous = allocation.clone();
        allocation.phase = next;
        self.put_pool_allocation(&allocation, Some(&previous))
    }

    pub(super) fn put_pool_allocation(
        &mut self,
        allocation: &crate::db::entities::PoolAllocation,
        previous: Option<&crate::db::entities::PoolAllocation>,
    ) -> Result<(), BatchError> {
        let token = allocation.token;
        let active_key = (token.pool.get(), token.executor.get(), token.job.get());
        let new_reservation = previous.is_none();
        if let Some(previous) = previous
            && (previous.token != token
                || previous.request_id != allocation.request_id
                || !previous.holds_capacity())
        {
            return self.corrupted(
                "Job Allocation identity changed or a released allocation was rewritten".to_owned(),
            );
        }
        let outcome = self.step(|tx| {
            let mut allocations = tx.open_table(POOL_ALLOCATIONS)?;
            let mut requests = tx.open_table(ALLOCATION_REQUESTS)?;
            let mut active = tx.open_table(ACTIVE_ALLOCATIONS)?;
            let stored = allocations
                .get(token.job.get())?
                .map(|row| decode_pool_allocation(token.job.get(), row.value()))
                .transpose()?;
            if stored.as_ref() != previous {
                return Ok(Some("Allocation presence disagrees with the writer's view"));
            }
            if new_reservation {
                if requests
                    .insert(allocation.request_id.get(), token.job.get())?
                    .is_some()
                {
                    return Ok(Some("duplicate reservation request identity"));
                }
            } else if requests
                .get(allocation.request_id.get())?
                .is_none_or(|job| job.value() != token.job.get())
            {
                return Ok(Some("reservation request index is missing or wrong"));
            }
            let indexed = active.get(active_key)?.is_some();
            match (new_reservation, allocation.holds_capacity()) {
                (true, true) => {
                    if indexed {
                        return Ok(Some("active Allocation index already has this Job"));
                    }
                    active.insert(active_key, ())?;
                }
                (true, false) => return Ok(Some("a new Allocation cannot be Released")),
                (false, true) => {
                    if !indexed {
                        return Ok(Some("active Allocation index is missing a holding row"));
                    }
                }
                (false, false) => {
                    if !indexed {
                        return Ok(Some("active Allocation index is missing a holding row"));
                    }
                    active.remove(active_key)?;
                }
            }
            allocations.insert(
                token.job.get(),
                encode_pool_allocation(allocation).as_slice(),
            )?;
            Ok(None)
        })?;
        match outcome {
            Some(detail) => self.corrupted(detail.to_owned()),
            None => {
                self.record_allocation_transition(previous, allocation)?;
                Ok(())
            }
        }
    }
}

pub(super) fn requirement(
    operation: Option<ExecutionOperation>,
    protocol: Option<LlmTransport>,
) -> u16 {
    let operation = operation.map_or(0, |operation| u16::from(operation.code()));
    let protocol = protocol.map_or(0, |protocol| u16::from(protocol.code()));
    (operation << 8) | protocol
}

pub(super) fn execution_requirement(execution: &FrozenExecutionConfig) -> u16 {
    requirement(
        Some(ExecutionOperation::LlmRequest),
        Some(execution.llm.provider.transport),
    )
}

pub(super) fn mutable_configuration_requirement(configuration: &FrozenConfiguration) -> u16 {
    match configuration {
        FrozenConfiguration::Generative(execution) => execution_requirement(execution),
        FrozenConfiguration::Decision(decision) => decision_work_requirement(decision),
    }
}

pub(super) fn decision_work_requirement(frozen: &FrozenDecisionConfig) -> u16 {
    match frozen {
        FrozenDecisionConfig::Native(_) => decision_requirement(),
        FrozenDecisionConfig::Generative(frozen, _) => execution_requirement(frozen),
    }
}

pub(super) fn decision_requirement() -> u16 {
    requirement(Some(ExecutionOperation::Decision), None)
}
