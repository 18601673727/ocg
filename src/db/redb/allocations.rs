use super::{StorageError, StorageErrorKind, TxRead};
use crate::db::entities::{
    AllocationExecutor, AllocationPhase, AllocationPool, CursorPage, ExecutionOperation,
    ExecutionPoolId, ExecutorId, JobId, LlmTransport, OutcomeKind, PoolAllocationPage,
    PoolOccupancy, RecordRef, StopProof, UncertainAllocation, Violation,
};
use crate::db::redb::batch::{Batch, BatchError, Failure, NativeResourceDelta};
use crate::db::redb::codec::{Code, Dec, DecodeResult, Enc};
use crate::db::redb::reads::jobs::mutable_job_record_in;
use crate::db::redb::reads::read;
use crate::db::redb::schema::{
    ACTIVE_ALLOCATIONS, CONFIGS, EXECUTORS, MAX_PAGE, POOL_ALLOCATIONS, POOL_EXECUTORS, POOLS,
    SEQUENCES,
};
use ::redb::{Database, ReadableTable};
use std::collections::BTreeSet;
use std::ops::Bound;

pub(super) const TAG_ALLOCATION_POOL: u8 = 40;
pub(super) const TAG_ALLOCATION_EXECUTOR: u8 = 41;

pub(super) fn encode_allocation_pool(pool: &crate::db::entities::AllocationPool) -> Vec<u8> {
    let mut e = Enc::new(TAG_ALLOCATION_POOL);
    e.u128(pool.id.get());
    e.str(&pool.name);
    e.u32(pool.capacity.get());
    e.bool(pool.enabled);
    e.bool(pool.archived);
    e.u32(pool.revision);
    e.0
}

pub(super) fn decode_allocation_pool(
    key: u128,
    bytes: &[u8],
) -> DecodeResult<crate::db::entities::AllocationPool> {
    let mut d = Dec::new(bytes, TAG_ALLOCATION_POOL)?;
    let id = ExecutionPoolId::new(d.u128()?);
    let name = d.str()?;
    let capacity = std::num::NonZeroU32::new(d.u32()?)
        .ok_or_else(|| StorageError::corrupted("zero Pool capacity"))?;
    let enabled = d.bool()?;
    let archived = d.bool()?;
    let revision = d.u32()?;
    d.end()?;
    if id.get() != key || name.is_empty() || name.len() > 255 || revision == 0 {
        return Err(StorageError::corrupted("invalid allocation Pool"));
    }
    Ok(crate::db::entities::AllocationPool {
        id,
        name,
        capacity,
        enabled,
        archived,
        revision,
    })
}

pub(super) fn encode_allocation_executor(
    executor: &crate::db::entities::AllocationExecutor,
) -> Vec<u8> {
    let mut e = Enc::new(TAG_ALLOCATION_EXECUTOR);
    e.u128(executor.id.get());
    e.u128(executor.pool.get());
    e.str(&executor.name);
    e.bool(executor.enabled);
    e.bool(executor.archived);
    e.u32(executor.revision);
    e.u32(executor.capabilities.len() as u32);
    for (operation, protocol) in &executor.capabilities {
        e.u8(operation.code());
        e.opt(protocol.as_ref(), |e, protocol| e.u8(protocol.code()));
    }
    e.0
}

pub(super) fn decode_allocation_executor(
    key: u128,
    bytes: &[u8],
) -> DecodeResult<crate::db::entities::AllocationExecutor> {
    let mut d = Dec::new(bytes, TAG_ALLOCATION_EXECUTOR)?;
    let id = ExecutorId::new(d.u128()?);
    let pool = ExecutionPoolId::new(d.u128()?);
    let name = d.str()?;
    let enabled = d.bool()?;
    let archived = d.bool()?;
    let revision = d.u32()?;
    let length = d.u32()?;
    if length > 8 {
        return Err(StorageError::corrupted("too many Executor capabilities"));
    }
    let mut capabilities = BTreeSet::new();
    for _ in 0..length {
        let operation = d.code(ExecutionOperation::from_code)?;
        let protocol = d.opt(|d| d.code(LlmTransport::from_code))?;
        if !capabilities.insert((operation, protocol)) {
            return Err(StorageError::corrupted("duplicate Executor capability"));
        }
    }
    d.end()?;
    if id.get() != key
        || id.get() == pool.get()
        || name.is_empty()
        || name.len() > 255
        || revision == 0
    {
        return Err(StorageError::corrupted("invalid allocation Executor"));
    }
    Ok(crate::db::entities::AllocationExecutor {
        id,
        pool,
        name,
        enabled,
        archived,
        revision,
        capabilities,
    })
}

pub(super) fn encode_pool_allocation(allocation: &crate::db::entities::PoolAllocation) -> Vec<u8> {
    use crate::db::entities::AllocationPhase;
    let mut e = Enc::new(42);
    e.u128(allocation.token.job.get());
    e.u128(allocation.token.pool.get());
    e.u128(allocation.token.executor.get());
    e.u32(allocation.token.generation.get());
    e.u64(allocation.token.epoch);
    e.u64(allocation.job_revision);
    e.u8(match allocation.phase {
        AllocationPhase::Reserved => 1,
        AllocationPhase::Running => 2,
        AllocationPhase::StopRequested => 3,
        AllocationPhase::Uncertain => 4,
        AllocationPhase::Released => 5,
    });
    e.ts(allocation.claimed_at);
    e.ostr(allocation.host_id.as_deref());
    e.ostr(allocation.boot_id.as_deref());
    e.u32(allocation.pool_revision);
    e.u32(allocation.executor_revision);
    e.u8(allocation.operation.code());
    e.opt(allocation.protocol.as_ref(), |e, p| e.u8(p.code()));
    e.ots(allocation.stop_confirmed_at);
    e.opt(allocation.stop_proof.as_ref(), |e, p| e.u8(p.code()));
    e.u128(allocation.request_id.get());
    e.opt(allocation.request_after.as_ref(), |e, cursor| {
        e.u8(cursor.0.0);
        e.i64(cursor.0.1);
        e.u128(cursor.0.2);
        e.u16(cursor.0.3);
    });
    e.ots(allocation.started_at);
    e.0
}

pub(super) fn decode_pool_allocation(
    key: u128,
    bytes: &[u8],
) -> DecodeResult<crate::db::entities::PoolAllocation> {
    use crate::db::entities::{AllocationPhase, AllocationToken, PoolAllocation};
    let mut d = Dec::new(bytes, 42)?;
    let job = JobId::new(d.u128()?);
    let pool = ExecutionPoolId::new(d.u128()?);
    let executor = ExecutorId::new(d.u128()?);
    let generation = std::num::NonZeroU32::new(d.u32()?)
        .ok_or_else(|| StorageError::corrupted("zero allocation generation"))?;
    let epoch = d.u64()?;
    let job_revision = d.u64()?;
    let phase = match d.u8()? {
        1 => AllocationPhase::Reserved,
        2 => AllocationPhase::Running,
        3 => AllocationPhase::StopRequested,
        4 => AllocationPhase::Uncertain,
        5 => AllocationPhase::Released,
        _ => return Err(StorageError::corrupted("invalid allocation phase")),
    };
    let claimed_at = d.ts()?;
    let host_id = d.ostr()?;
    let boot_id = d.ostr()?;
    let pool_revision = d.u32()?;
    let executor_revision = d.u32()?;
    let operation = d.code(ExecutionOperation::from_code)?;
    let protocol = d.opt(|d| d.code(LlmTransport::from_code))?;
    let stop_confirmed_at = d.ots()?;
    let stop_proof = d.opt(|d| d.code(StopProof::from_code))?;
    let request_id = crate::db::entities::ReservationRequestId::new(d.u128()?);
    let request_after = d.opt(|d| {
        Ok(crate::db::entities::DevelopmentReadyCursor((
            d.u8()?,
            d.i64()?,
            d.u128()?,
            d.u16()?,
        )))
    })?;
    let started_at = d.ots()?;
    d.end()?;
    if job.get() != key
        || pool.get() == executor.get()
        || epoch == 0
        || job_revision == 0
        || pool_revision == 0
        || executor_revision == 0
        || !valid_reservation_request_id(request_id)
        || request_after.is_some_and(|cursor| cursor.0.0 > 2)
        || stop_confirmed_at.is_some() != stop_proof.is_some()
        || (phase != AllocationPhase::Released && stop_proof.is_some())
        || (phase == AllocationPhase::Reserved && started_at.is_some())
        || (phase == AllocationPhase::Running && started_at.is_none())
        || started_at.is_some_and(|at| at < claimed_at)
    {
        return Err(StorageError::corrupted("invalid Pool allocation"));
    }
    Ok(PoolAllocation {
        request_id,
        request_after,
        token: AllocationToken {
            job,
            pool,
            executor,
            generation,
            epoch,
        },
        job_revision,
        phase,
        claimed_at,
        started_at,
        host_id,
        boot_id,
        pool_revision,
        executor_revision,
        operation,
        protocol,
        stop_confirmed_at,
        stop_proof,
    })
}

pub(crate) fn valid_reservation_request_id(id: crate::db::entities::ReservationRequestId) -> bool {
    let value = uuid::Uuid::from_u128(id.get());
    value.get_version_num() == 7 && value.get_variant() == uuid::Variant::RFC4122
}

pub(super) fn allocation_by_request(
    requests: &impl ReadableTable<u128, u128>,
    allocations: &impl ReadableTable<u128, &'static [u8]>,
    id: crate::db::entities::ReservationRequestId,
) -> Result<Option<crate::db::entities::PoolAllocation>, StorageError> {
    let Some(job) = requests.get(id.get())?.map(|job| job.value()) else {
        return Ok(None);
    };
    let Some(bytes) = allocations.get(job)? else {
        return Err(StorageError::corrupted(
            "reservation request index references a missing Allocation",
        ));
    };
    let allocation = decode_pool_allocation(job, bytes.value())?;
    if allocation.request_id != id {
        return Err(StorageError::corrupted(
            "reservation request index disagrees with its Allocation",
        ));
    }
    Ok(Some(allocation))
}

pub(super) fn active_allocations(
    active: &impl ReadableTable<(u128, u128, u128), ()>,
    pool: u128,
    executor: Option<u128>,
) -> Result<Vec<(u128, u128)>, StorageError> {
    let (low, high) = match executor {
        Some(executor) => ((pool, executor, 0), (pool, executor, u128::MAX)),
        None => ((pool, 0, 0), (pool, u128::MAX, u128::MAX)),
    };
    active
        .range(low..=high)?
        .map(|entry| {
            let (key, _) = entry?;
            let (_, executor, job) = key.value();
            Ok((executor, job))
        })
        .collect()
}

pub(super) const STOPPED_BEFORE_START_CODE: &str = "stopped_before_start";
pub(super) const STOPPED_BEFORE_START_SUMMARY: &str =
    "Stopped before it started. Nothing was sent to the Provider.";

impl Batch {
    pub(crate) fn register_allocation_pool(
        &mut self,
        pool: crate::db::entities::AllocationPool,
    ) -> Result<(), BatchError> {
        self.require_allocation_durability()?;
        if pool.id.get() == 0
            || crate::db::entities::parse_entity_id(&pool.id.to_string()).is_none()
            || pool.name.trim().is_empty()
            || pool.name.chars().any(char::is_control)
            || pool.name.len() > 255
            || pool.revision != 1
        {
            return self.reject(Violation::InvalidValue(
                "pool",
                "invalid Pool configuration",
            ));
        }
        let existing = self.resource_identity_exists(pool.id.get())?;
        if existing {
            return self.reject(Violation::InvalidValue(
                "pool",
                "configuration identity already exists",
            ));
        }
        self.observe_configuration_id(pool.id.get())?;
        self.insert(
            POOLS,
            pool.id.get(),
            encode_allocation_pool(&pool).as_slice(),
        )?;
        self.resource_deltas.push(NativeResourceDelta::Pool(pool));
        Ok(())
    }

    pub(crate) fn register_allocation_executor(
        &mut self,
        executor: crate::db::entities::AllocationExecutor,
    ) -> Result<(), BatchError> {
        self.require_allocation_durability()?;
        if executor.id.get() == 0
            || executor.id.get() == executor.pool.get()
            || executor.name.trim().is_empty()
            || executor.name.chars().any(char::is_control)
            || executor.name.len() > 255
            || executor.revision != 1
            || executor.capabilities.len() > 8
            || executor.capabilities.iter().any(|(operation, protocol)| {
                !matches!(
                    (operation, protocol),
                    (ExecutionOperation::LlmRequest, Some(_))
                        | (ExecutionOperation::Decision, None)
                )
            })
            || crate::db::entities::parse_entity_id(&executor.id.to_string()).is_none()
        {
            return self.reject(Violation::InvalidValue(
                "executor",
                "invalid Executor configuration",
            ));
        }
        if let Some(reason) = executor
            .capabilities
            .iter()
            .find_map(|(_, protocol)| protocol.and_then(LlmTransport::execution_refusal))
        {
            return self.reject(Violation::InvalidValue("executor.protocol", reason));
        }
        let pool = self.get(POOLS, executor.pool.get(), |bytes| {
            decode_allocation_pool(executor.pool.get(), bytes)
        })?;
        if pool.is_none_or(|pool| pool.archived) {
            return self.reject(Violation::InvalidValue("executor", "Pool does not exist"));
        }
        let existing = self.resource_identity_exists(executor.id.get())?;
        if existing {
            return self.reject(Violation::InvalidValue(
                "executor",
                "configuration identity already exists",
            ));
        }
        self.observe_configuration_id(executor.id.get())?;
        self.insert(
            EXECUTORS,
            executor.id.get(),
            encode_allocation_executor(&executor).as_slice(),
        )?;
        self.insert(POOL_EXECUTORS, (executor.pool.get(), executor.id.get()), ())?;
        self.resource_deltas
            .push(NativeResourceDelta::Executor(executor));
        Ok(())
    }

    fn resource_identity_exists(&mut self, id: u128) -> Result<bool, BatchError> {
        self.step(|tx| {
            Ok(tx.open_table(POOLS)?.get(id)?.is_some()
                || tx.open_table(EXECUTORS)?.get(id)?.is_some()
                || tx.open_table(CONFIGS)?.get(id)?.is_some())
        })
    }

    fn observe_configuration_id(&mut self, id: u128) -> Result<(), BatchError> {
        self.step(|tx| {
            let mut sequence = tx.open_table(SEQUENCES)?;
            if sequence
                .get("entity")?
                .is_none_or(|previous| previous.value() < id)
            {
                sequence.insert("entity", id)?;
            }
            Ok(())
        })
    }

    fn mutate_native_pool(
        &mut self,
        id: ExecutionPoolId,
        expected_revision: u32,
        archived_violation: Violation,
        apply: impl FnOnce(
            &mut Self,
            &mut crate::db::entities::AllocationPool,
        ) -> Result<(), BatchError>,
    ) -> Result<crate::db::entities::AllocationPool, BatchError> {
        self.require_allocation_durability()?;
        let Some(mut pool) = self.get(POOLS, id.get(), |bytes| {
            decode_allocation_pool(id.get(), bytes)
        })?
        else {
            return self.reject(Violation::NotFound(RecordRef::ExecutionPool(id)));
        };
        if pool.archived {
            return self.reject(archived_violation);
        }
        if pool.revision != expected_revision {
            return self.reject(Violation::InvalidValue("pool", "revision conflict"));
        }
        apply(self, &mut pool)?;
        pool.revision = pool
            .revision
            .checked_add(1)
            .ok_or(BatchError(Failure::Rejected(Violation::ValueOutOfRange(
                "revision",
            ))))?;
        self.insert(POOLS, id.get(), encode_allocation_pool(&pool).as_slice())?;
        self.resource_deltas
            .push(NativeResourceDelta::Pool(pool.clone()));
        Ok(pool)
    }

    pub(crate) fn update_native_pool(
        &mut self,
        id: ExecutionPoolId,
        expected_revision: u32,
        capacity: Option<std::num::NonZeroU32>,
        enabled: Option<bool>,
    ) -> Result<crate::db::entities::AllocationPool, BatchError> {
        self.mutate_native_pool(
            id,
            expected_revision,
            Violation::NotFound(RecordRef::ExecutionPool(id)),
            |_, pool| {
                if let Some(capacity) = capacity {
                    pool.capacity = capacity;
                }
                if let Some(enabled) = enabled {
                    pool.enabled = enabled;
                }
                Ok(())
            },
        )
    }

    fn mutate_native_executor(
        &mut self,
        id: ExecutorId,
        expected_revision: u32,
        archived_violation: Violation,
        apply: impl FnOnce(
            &mut Self,
            &mut crate::db::entities::AllocationExecutor,
        ) -> Result<(), BatchError>,
    ) -> Result<crate::db::entities::AllocationExecutor, BatchError> {
        self.require_allocation_durability()?;
        let Some(mut executor) = self.native_executor_row(id)? else {
            return self.reject(Violation::NotFound(RecordRef::Executor(id)));
        };
        if executor.archived {
            return self.reject(archived_violation);
        }
        if executor.revision != expected_revision {
            return self.reject(Violation::InvalidValue("executor", "revision conflict"));
        }
        apply(self, &mut executor)?;
        executor.revision =
            executor
                .revision
                .checked_add(1)
                .ok_or(BatchError(Failure::Rejected(Violation::ValueOutOfRange(
                    "revision",
                ))))?;
        self.insert(
            EXECUTORS,
            id.get(),
            encode_allocation_executor(&executor).as_slice(),
        )?;
        self.resource_deltas
            .push(NativeResourceDelta::Executor(executor.clone()));
        Ok(executor)
    }

    pub(crate) fn update_native_executor(
        &mut self,
        id: ExecutorId,
        expected_revision: u32,
        update: crate::db::entities::ExecutorUpdate,
    ) -> Result<crate::db::entities::AllocationExecutor, BatchError> {
        use crate::db::entities::ExecutorUpdate;
        self.mutate_native_executor(
            id,
            expected_revision,
            Violation::NotFound(RecordRef::Executor(id)),
            |batch, executor| match update {
                ExecutorUpdate::Enabled(enabled) => {
                    executor.enabled = enabled;
                    Ok(())
                }
                ExecutorUpdate::Name(name) => {
                    if name.trim().is_empty()
                        || name.chars().any(char::is_control)
                        || name.len() > 255
                    {
                        return batch.reject(Violation::InvalidValue(
                            "executor",
                            "invalid Executor configuration",
                        ));
                    }
                    executor.name = name;
                    Ok(())
                }
                ExecutorUpdate::Capabilities(capabilities) => {
                    if capabilities.len() > 8
                        || capabilities.iter().any(|(operation, protocol)| {
                            !matches!(
                                (operation, protocol),
                                (ExecutionOperation::LlmRequest, Some(_))
                                    | (ExecutionOperation::Decision, None)
                            )
                        })
                    {
                        return batch.reject(Violation::InvalidValue(
                            "executor",
                            "invalid Executor configuration",
                        ));
                    }
                    // A settings edit may round-trip stored capabilities. Only admit new ones
                    // the runtime can execute.
                    if let Some(reason) =
                        capabilities
                            .difference(&executor.capabilities)
                            .find_map(|(_, protocol)| {
                                protocol.and_then(LlmTransport::execution_refusal)
                            })
                    {
                        return batch.reject(Violation::InvalidValue("executor.protocol", reason));
                    }
                    executor.capabilities = capabilities;
                    Ok(())
                }
            },
        )
    }

    fn native_executor_row(
        &mut self,
        id: ExecutorId,
    ) -> Result<Option<crate::db::entities::AllocationExecutor>, BatchError> {
        self.get(EXECUTORS, id.get(), |bytes| {
            decode_allocation_executor(id.get(), bytes)
        })
    }

    pub(crate) fn archive_native_executor(
        &mut self,
        id: ExecutorId,
        expected_revision: u32,
    ) -> Result<crate::db::entities::AllocationExecutor, BatchError> {
        self.mutate_native_executor(
            id,
            expected_revision,
            Violation::InvalidValue("executor", "already archived"),
            |batch, executor| {
                let pool = executor.pool.get();
                let held = batch.step(|tx| {
                    let active = tx.open_table(ACTIVE_ALLOCATIONS)?;
                    let allocations = tx.open_table(POOL_ALLOCATIONS)?;
                    let entries = active_allocations(&active, pool, Some(id.get()))?;
                    for &(executor, job) in &entries {
                        match indexed_allocation(&allocations, pool, executor, job) {
                            Ok(_) => {}
                            Err(error) if error.kind == StorageErrorKind::Corrupted => {
                                return Ok(None);
                            }
                            Err(error) => return Err(error),
                        }
                    }
                    Ok(match entries.len() {
                        0 | 1 => Some(!entries.is_empty()),
                        _ => None,
                    })
                })?;
                match held {
                    Some(false) => {}
                    Some(true) => {
                        return batch.reject(Violation::InvalidValue(
                            "executor",
                            "holds an active Allocation",
                        ));
                    }
                    None => {
                        return batch.reject(Violation::InvalidValue(
                            "executor",
                            "active Allocation index is inconsistent",
                        ));
                    }
                }
                executor.archived = true;
                executor.enabled = false;
                Ok(())
            },
        )
    }

    pub(crate) fn archive_native_pool(
        &mut self,
        id: ExecutionPoolId,
        expected_revision: u32,
    ) -> Result<crate::db::entities::AllocationPool, BatchError> {
        self.mutate_native_pool(
            id,
            expected_revision,
            Violation::InvalidValue("pool", "already archived"),
            |batch, pool| {
                let pool_key = id.get();
                let has_allocation = batch.step(|tx| {
                    let active = tx.open_table(ACTIVE_ALLOCATIONS)?;
                    Ok(active
                        .range((pool_key, 0, 0)..=(pool_key, u128::MAX, u128::MAX))?
                        .next()
                        .is_some())
                })?;
                if has_allocation {
                    return batch.reject(Violation::InvalidValue(
                        "pool",
                        "holds an active Allocation",
                    ));
                }
                let has_unarchived_executor = batch.step(|tx| {
                    let executors = tx.open_table(EXECUTORS)?;
                    let members = tx.open_table(POOL_EXECUTORS)?;
                    for entry in members.range((pool_key, 0)..=(pool_key, u128::MAX))? {
                        let (_, executor_id) = entry?.0.value();
                        let value = executors.get(executor_id)?.ok_or_else(|| {
                            StorageError::corrupted("Pool membership references a missing Executor")
                        })?;
                        let executor = decode_allocation_executor(executor_id, value.value())?;
                        if executor.pool.get() != pool_key {
                            return Err(StorageError::corrupted(
                                "Pool membership names another Pool",
                            ));
                        }
                        if !executor.archived {
                            return Ok(true);
                        }
                    }
                    Ok(false)
                })?;
                if has_unarchived_executor {
                    return batch.reject(Violation::InvalidValue(
                        "pool",
                        "has an unarchived Executor",
                    ));
                }
                pool.archived = true;
                pool.enabled = false;
                Ok(())
            },
        )
    }

    pub(crate) fn require_native_capability(
        &mut self,
        operation: ExecutionOperation,
        protocol: Option<LlmTransport>,
    ) -> Result<(), BatchError> {
        if !self.has_native_capability(operation, protocol)? {
            return self.reject(Violation::InvalidValue(
                "executor",
                "no unarchived native Executor supports this operation and protocol",
            ));
        }
        Ok(())
    }

    pub(super) fn has_native_capability(
        &mut self,
        operation: ExecutionOperation,
        protocol: Option<LlmTransport>,
    ) -> Result<bool, BatchError> {
        self.step(|tx| {
            let pools = tx.open_table(POOLS)?;
            let executors = tx.open_table(EXECUTORS)?;
            for entry in executors.iter()? {
                let (key, value) = entry?;
                let executor = decode_allocation_executor(key.value(), value.value())?;
                let pool_bytes = pools
                    .get(executor.pool.get())?
                    .ok_or_else(|| StorageError::corrupted("Executor references a missing Pool"))?;
                let pool = decode_allocation_pool(executor.pool.get(), pool_bytes.value())?;
                if !pool.archived
                    && !executor.archived
                    && executor.capabilities.contains(&(operation, protocol))
                {
                    return Ok(true);
                }
            }
            Ok(false)
        })
    }
}

pub(crate) fn read_pool(
    database: &Database,
    id: ExecutionPoolId,
) -> Result<Option<AllocationPool>, StorageError> {
    read(database)?
        .open_table(POOLS)?
        .get(id.get())?
        .map(|row| decode_allocation_pool(id.get(), row.value()))
        .transpose()
}

pub(crate) fn read_pools(
    database: &Database,
    before: Option<ExecutionPoolId>,
    limit: usize,
) -> Result<CursorPage<AllocationPool, ExecutionPoolId>, StorageError> {
    let tx = read(database)?;
    resource_page(
        &tx.open_table(POOLS)?,
        before.map(ExecutionPoolId::get),
        limit,
        ExecutionPoolId::new,
        |id, bytes| {
            let pool = decode_allocation_pool(id, bytes)?;
            Ok((!pool.archived).then_some(pool))
        },
    )
}

pub(crate) fn read_executors(
    database: &Database,
    before: Option<ExecutorId>,
    limit: usize,
) -> Result<CursorPage<AllocationExecutor, ExecutorId>, StorageError> {
    let tx = read(database)?;
    let pools = tx.open_table(POOLS)?;
    resource_page(
        &tx.open_table(EXECUTORS)?,
        before.map(ExecutorId::get),
        limit,
        ExecutorId::new,
        |id, bytes| {
            let executor = decode_allocation_executor(id, bytes)?;
            let pool = pools
                .get(executor.pool.get())?
                .ok_or_else(|| StorageError::corrupted("Executor references a missing Pool"))?;
            decode_allocation_pool(executor.pool.get(), pool.value())?;
            Ok((!executor.archived).then_some(executor))
        },
    )
}

fn resource_page<T, C>(
    table: &impl ReadableTable<u128, &'static [u8]>,
    before: Option<u128>,
    limit: usize,
    cursor: impl Fn(u128) -> C,
    decode: impl Fn(u128, &[u8]) -> Result<Option<T>, StorageError>,
) -> Result<CursorPage<T, C>, StorageError> {
    let end = before.unwrap_or(u128::MAX);
    let limit = limit.clamp(1, MAX_PAGE);
    let mut records = Vec::new();
    let mut last = None;
    for (scanned, entry) in table.range(..end)?.rev().enumerate() {
        if records.len() >= limit || scanned == MAX_PAGE {
            return Ok(CursorPage {
                records,
                next: last,
            });
        }
        let (key, bytes) = entry?;
        let id = key.value();
        last = Some(cursor(id));
        if let Some(record) = decode(id, bytes.value())? {
            records.push(record);
        }
    }
    Ok(CursorPage {
        records,
        next: None,
    })
}

pub(crate) const MAX_POOL_OCCUPANCY_ENTRIES: usize = 4096;

pub(crate) fn indexed_allocation(
    allocations: &impl ReadableTable<u128, &'static [u8]>,
    pool: u128,
    executor: u128,
    job: u128,
) -> Result<crate::db::entities::PoolAllocation, StorageError> {
    let bytes = allocations.get(job)?.ok_or_else(|| {
        StorageError::corrupted("active Allocation index references a missing Allocation")
    })?;
    let allocation = decode_pool_allocation(job, bytes.value())?;
    if allocation.token.job.get() != job
        || allocation.token.pool.get() != pool
        || allocation.token.executor.get() != executor
    {
        return Err(StorageError::corrupted(
            "active Allocation index disagrees with its Allocation's identity",
        ));
    }
    if !allocation.holds_capacity() {
        return Err(StorageError::corrupted(
            "active Allocation index references a released Allocation",
        ));
    }
    Ok(allocation)
}

pub(crate) fn read_pool_allocations(
    database: &Database,
    pool_id: crate::db::entities::ExecutionPoolId,
    after: Option<(u128, u128)>,
    limit: usize,
) -> Result<Option<PoolAllocationPage>, StorageError> {
    use crate::db::entities::AllocationPhase;
    let tx = read(database)?;
    let pool = {
        let pools = tx.open_table(POOLS)?;
        let Some(bytes) = pools.get(pool_id.get())? else {
            return Ok(None);
        };
        decode_allocation_pool(pool_id.get(), bytes.value())?
    };
    let active = tx.open_table(ACTIVE_ALLOCATIONS)?;
    let allocations = tx.open_table(POOL_ALLOCATIONS)?;
    let pool_key = pool_id.get();
    let high = (pool_key, u128::MAX, u128::MAX);

    let limit = limit.clamp(1, MAX_PAGE);
    let start = match after {
        Some((executor, job)) => Bound::Excluded((pool_key, executor, job)),
        None => Bound::Included((pool_key, 0, 0)),
    };
    let mut page = Vec::new();
    let mut next = None;
    for entry in active.range((start, Bound::Included(high)))? {
        let (_, executor, job) = entry?.0.value();
        if page.len() == limit {
            next = page
                .last()
                .map(|last: &crate::db::entities::PoolAllocation| {
                    (last.token.executor.get(), last.token.job.get())
                });
            break;
        }
        page.push(indexed_allocation(&allocations, pool_key, executor, job)?);
    }

    let mut occupancy = PoolOccupancy::default();
    let mut counted = Some(());
    for (scanned, entry) in active
        .range((Bound::Included((pool_key, 0, 0)), Bound::Included(high)))?
        .enumerate()
    {
        let (_, executor, job) = entry?.0.value();
        if scanned == MAX_POOL_OCCUPANCY_ENTRIES {
            counted = None;
            break;
        }
        let allocation = indexed_allocation(&allocations, pool_key, executor, job)?;
        occupancy.held += 1;
        match allocation.phase {
            AllocationPhase::Reserved => occupancy.reserved += 1,
            AllocationPhase::Running => occupancy.running += 1,
            AllocationPhase::StopRequested => occupancy.stop_requested += 1,
            AllocationPhase::Uncertain => occupancy.uncertain += 1,
            AllocationPhase::Released => {
                return Err(StorageError::corrupted(
                    "active Allocation index references a released Allocation",
                ));
            }
        }
    }
    Ok(Some(PoolAllocationPage {
        pool,
        occupancy: counted.map(|()| occupancy),
        allocations: page,
        next,
    }))
}

pub(crate) fn pools_offering_capability(
    rx: &impl TxRead,
    operation: ExecutionOperation,
    protocol: Option<LlmTransport>,
) -> Result<Vec<ExecutorId>, StorageError> {
    if protocol.and_then(LlmTransport::execution_refusal).is_some() {
        return Ok(Vec::new());
    }
    let pools = rx.table(POOLS)?;
    let table = rx.table(EXECUTORS)?;
    let mut executors = Vec::new();
    for entry in table.iter()? {
        let (key, bytes) = entry?;
        let executor = decode_allocation_executor(key.value(), bytes.value())?;
        let pool = pools
            .get(executor.pool.get())?
            .ok_or_else(|| StorageError::corrupted("Executor references a missing Pool"))?;
        let pool = decode_allocation_pool(executor.pool.get(), pool.value())?;
        if executor.enabled
            && !executor.archived
            && pool.enabled
            && !pool.archived
            && executor.capabilities.contains(&(operation, protocol))
        {
            executors.push(executor.id);
            if executors.len() >= MAX_PAGE {
                break;
            }
        }
    }
    Ok(executors)
}

#[derive(Clone, Debug)]
struct NativeExecutorEntry {
    pool: ExecutionPoolId,
    archived: bool,
    capabilities: BTreeSet<(ExecutionOperation, Option<LlmTransport>)>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct NativeResourceCatalog {
    pools: std::collections::HashMap<ExecutionPoolId, bool>,
    executors: std::collections::HashMap<ExecutorId, NativeExecutorEntry>,
}

impl NativeResourceCatalog {
    pub(crate) fn apply(&mut self, delta: super::NativeResourceDelta) {
        match delta {
            super::NativeResourceDelta::Pool(pool) => {
                self.pools.insert(pool.id, pool.archived);
            }
            super::NativeResourceDelta::Executor(executor) => {
                self.executors.insert(
                    executor.id,
                    NativeExecutorEntry {
                        pool: executor.pool,
                        archived: executor.archived,
                        capabilities: executor.capabilities,
                    },
                );
            }
        }
    }

    pub(crate) fn has_unarchived_capability(
        &self,
        operation: ExecutionOperation,
        protocol: Option<LlmTransport>,
    ) -> bool {
        self.executors.values().any(|entry| {
            !entry.archived
                && entry.capabilities.contains(&(operation, protocol))
                && self
                    .pools
                    .get(&entry.pool)
                    .is_some_and(|pool_archived| !pool_archived)
        })
    }
}

pub(crate) fn read_native_resource_catalog(
    database: &Database,
) -> Result<NativeResourceCatalog, StorageError> {
    let rx = read(database)?;
    let mut catalog = NativeResourceCatalog::default();
    for entry in rx.open_table(POOLS)?.iter()? {
        let (key, bytes) = entry?;
        let pool = decode_allocation_pool(key.value(), bytes.value())?;
        catalog.apply(super::NativeResourceDelta::Pool(pool));
    }
    for entry in rx.open_table(EXECUTORS)?.iter()? {
        let (key, bytes) = entry?;
        let executor = decode_allocation_executor(key.value(), bytes.value())?;
        catalog.apply(super::NativeResourceDelta::Executor(executor));
    }
    for entry in catalog.executors.values() {
        if !catalog.pools.contains_key(&entry.pool) {
            return Err(StorageError::corrupted(
                "Executor references a missing Pool",
            ));
        }
    }
    Ok(catalog)
}

pub(crate) fn read_uncertain_allocations(
    database: &Database,
) -> Result<Vec<UncertainAllocation>, StorageError> {
    use crate::db::entities::MutableJobPhase;
    let rx = read(database)?;
    let active = rx.open_table(ACTIVE_ALLOCATIONS)?;
    let allocations = rx.open_table(POOL_ALLOCATIONS)?;
    let mut uncertain = Vec::new();
    for entry in active.iter()? {
        let (pool, executor, job) = entry?.0.value();
        let allocation = indexed_allocation(&allocations, pool, executor, job)?;
        if allocation.phase != AllocationPhase::Uncertain {
            continue;
        }
        let record = mutable_job_record_in(&rx, JobId::new(job))?
            .ok_or_else(|| StorageError::corrupted("an Uncertain Allocation has no Job"))?;
        let started = record.job.phase == MutableJobPhase::Executing;
        let shaped = allocation.job_revision == record.job.revision
            && allocation.stop_confirmed_at.is_none()
            && allocation.stop_proof.is_none()
            && allocation.started_at.is_some() == started
            && (started || record.job.phase == MutableJobPhase::Queued);
        if !shaped {
            return Err(StorageError::corrupted(
                "an Uncertain Allocation contradicts its Job",
            ));
        }
        let Some(outcome) = record
            .outcome
            .filter(|outcome| outcome.kind == OutcomeKind::Unknown)
        else {
            return Err(StorageError::corrupted(
                "an Uncertain Allocation has no Unknown outcome",
            ));
        };
        uncertain.push(UncertainAllocation {
            job: JobId::new(job),
            pool: allocation.token.pool,
            epoch: allocation.token.epoch,
            since: outcome.at,
        });
    }
    Ok(uncertain)
}
