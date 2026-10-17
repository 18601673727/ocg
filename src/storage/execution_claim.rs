use super::*;
use redb::ReadableTableMetadata;

fn encode_binding(binding: FrozenPoolBinding) -> Vec<u8> {
    let mut encoder = Enc::new(codec::TAG_POOL_BINDING);
    encoder.u64(binding.pool.get());
    encoder.u64(binding.pool_profile.get());
    encoder.0
}

fn decode_binding(bytes: &[u8]) -> Result<FrozenPoolBinding, StorageError> {
    let mut decoder = Dec::new(bytes, codec::TAG_POOL_BINDING)?;
    let binding = FrozenPoolBinding {
        pool: ExecutionPoolId::new(decoder.u64()?),
        pool_profile: ProfileId::new(decoder.u64()?),
    };
    decoder.end()?;
    Ok(binding)
}

pub(crate) fn read_pool_binding(
    database: &Database,
    job: JobId,
) -> Result<Option<FrozenPoolBinding>, StorageError> {
    read(database)?
        .open_table(FROZEN_POOL_BINDINGS)?
        .get(job.get())?
        .map(|value| decode_binding(value.value()))
        .transpose()
}

pub(crate) fn read_occupancy(
    database: &Database,
) -> Result<Vec<(ExecutionPoolId, JobId)>, StorageError> {
    let transaction = read(database)?;
    let table = transaction.open_table(OCCUPANCY)?;
    let mut result = Vec::new();
    result
        .try_reserve_exact(table.len()? as usize)
        .map_err(allocation_error)?;
    for entry in table.iter()? {
        let (pool, job) = entry?.0.value();
        result.push((ExecutionPoolId::new(pool), JobId::new(job)));
    }
    Ok(result)
}

pub(super) fn allocation_error(error: std::collections::TryReserveError) -> StorageError {
    StorageError {
        kind: StorageErrorKind::Other,
        os_error: None,
        detail: format!("allocation exhausted: {error}"),
    }
}

impl Batch {
    pub(crate) fn claim_execution(
        &mut self,
        request: ClaimRequest,
        epoch: u64,
    ) -> Result<Result<ClaimGrant, ClaimRefusal>, BatchError> {
        let job = request.job;
        let Some(mut row) = self.job(job)? else {
            return Ok(Err(ClaimRefusal::NotFound));
        };
        if self.genesis()? == job {
            return Ok(Err(ClaimRefusal::Genesis));
        }
        if !row.awaits_start() {
            return Ok(Err(ClaimRefusal::NotPending));
        }
        if row.paused_at.is_some() {
            return Ok(Err(ClaimRefusal::Paused));
        }
        if let Some(eligible_at) = row.eligible_at
            && eligible_at > self.now()
        {
            return Ok(Err(ClaimRefusal::NotYetEligible { eligible_at }));
        }
        let occupied = self.step(|transaction| Ok(transaction.open_table(OCCUPANCY)?.len()?))?;
        if occupied >= u64::from(GLOBAL_EXECUTION_LIMIT) {
            return Ok(Err(ClaimRefusal::NoCapacity(CapacityScope::Global)));
        }
        if request.allowed_pools.is_empty() {
            return Ok(Err(ClaimRefusal::EmptyAllowedPools));
        }
        let frozen = self.job_frozen_llm(job)?;
        let protocol = frozen.as_ref().map(|frozen| frozen.provider.transport);
        let mut compatible = BTreeSet::new();
        let mut selected = None;
        for id in request.allowed_pools {
            let Some(bytes) = self.get_bytes(EXECUTION_POOLS, id.get())? else {
                continue;
            };
            let pool = self.decoded(pools::decode(&bytes))?;
            if !pool.enabled {
                continue;
            }
            let profile = self.require_profile_row(pool.profile)?;
            let ProfileConfig::ExecutionPool(config) = profile.config else {
                return self.corrupted(format!("{} has a non-Pool Profile", pool.id));
            };
            if !config
                .capabilities
                .supports(ExecutionOperation::LlmRequest, protocol)
            {
                continue;
            }
            compatible.insert(id);
            if selected.is_none() {
                let count = self.step(|transaction| {
                    let occupancy = transaction.open_table(OCCUPANCY)?;
                    let mut count = 0;
                    for entry in occupancy.range((id.get(), 0)..=(id.get(), u64::MAX))? {
                        entry?;
                        count += 1;
                    }
                    Ok(count)
                })?;
                if count < config.concurrency_limit.get() {
                    selected = Some(FrozenPoolBinding {
                        pool: id,
                        pool_profile: pool.profile,
                    });
                }
            }
        }
        if compatible.is_empty() {
            return Ok(Err(ClaimRefusal::NoCompatiblePool));
        }
        let Some(binding) = selected else {
            return Ok(Err(ClaimRefusal::NoCapacity(CapacityScope::Pools(
                compatible,
            ))));
        };
        // A missing durable LLM recipe cannot be reconstructed from a legacy plan.
        if frozen.is_none() {
            return Ok(Err(ClaimRefusal::RecipeInvalid));
        }
        let certified = self.step(closure_certified)?;
        let dependencies = self.dependencies(job)?;
        for (prerequisite, condition) in dependencies {
            let prerequisite_row = self.require_job(prerequisite)?;
            let settlement = if condition == DependencyCondition::Settled {
                self.step(|transaction| {
                    WaitingTables::open(transaction, certified, None)?
                        .subtree_settlement(&prerequisite_row)
                })?
            } else {
                Settlement::Unsettled
            };
            if settlement == Settlement::Unbounded {
                return Ok(Err(ClaimRefusal::Unbounded { prerequisite }));
            }
            let state = dependency_state_with(
                condition,
                &prerequisite_row,
                settlement == Settlement::Settled,
            );
            if state != DependencyState::Satisfied {
                return Ok(Err(ClaimRefusal::DependencyUnsatisfied {
                    prerequisite,
                    state,
                }));
            }
        }
        if self.get_bytes(FROZEN_POOL_BINDINGS, job.get())?.is_some() {
            return self.corrupted(format!("pending {job} already has a frozen Pool binding"));
        }
        self.step(|transaction| {
            let bytes = encode_binding(binding);
            transaction
                .open_table(FROZEN_POOL_BINDINGS)?
                .insert(job.get(), bytes.as_slice())?;
            transaction
                .open_table(ASSIGNED_POOL)?
                .insert(job.get(), binding.pool.get())?;
            transaction
                .open_table(POOL_PROFILE_JOBS)?
                .insert((binding.pool_profile.get(), job.get()), ())?;
            Ok(())
        })?;
        row.started_at = Some(self.now());
        row.updated_at = self.now();
        self.put_job(&row)?;
        self.claim_local_execution(job, epoch)?;
        Ok(Ok(ClaimGrant {
            job,
            binding,
            epoch,
            global_free_after: GLOBAL_EXECUTION_LIMIT - occupied as u8 - 1,
        }))
    }

    pub(super) fn index_occupancy(&mut self, row: &JobRow) -> Result<(), BatchError> {
        self.step(|transaction| {
            let bindings = transaction.open_table(FROZEN_POOL_BINDINGS)?;
            let Some(bytes) = bindings.get(row.id.get())? else {
                return Ok(());
            };
            let binding = decode_binding(bytes.value())?;
            let mut occupancy = transaction.open_table(OCCUPANCY)?;
            let key = (binding.pool.get(), row.id.get());
            if row.started_at.is_some() && !row.locally_stopped() {
                occupancy.insert(key, ())?;
            } else {
                occupancy.remove(key)?;
            }
            Ok(())
        })
    }

    pub(crate) fn reconcile_unexecuted_claim(
        &mut self,
        job: JobId,
        operation: OperationId,
    ) -> Result<Option<((), MarkerSubjects)>, BatchError> {
        let marker = self.step(|transaction| {
            transaction
                .open_table(OPERATIONS)?
                .get((operation.epoch(), operation.seq()))?
                .map(|value| decode_marker(operation, value.value()))
                .transpose()
        })?;
        let Some(marker) = marker else {
            return Ok(None);
        };
        if marker.kind != OperationKind::ClaimExecution || marker.subject != RecordRef::Job(job) {
            return self.reject(Violation::InvalidValue(
                "claim operation",
                "does not name this Job's execution grant",
            ));
        }
        crate::persistence::reconcile_abandoned_job(self, job, operation.epoch())
    }
}

pub(super) fn rebuild_execution_indexes(
    transaction: &WriteTransaction,
) -> Result<(), StorageError> {
    transaction.delete_table(ASSIGNED_POOL)?;
    transaction.delete_table(OCCUPANCY)?;
    transaction.delete_table(POOL_PROFILE_JOBS)?;
    let mut assigned = transaction.open_table(ASSIGNED_POOL)?;
    let mut occupancy = transaction.open_table(OCCUPANCY)?;
    let mut profiles = transaction.open_table(POOL_PROFILE_JOBS)?;
    let bindings = transaction.open_table(FROZEN_POOL_BINDINGS)?;
    let jobs = transaction.open_table(JOBS)?;
    let pools = transaction.open_table(EXECUTION_POOLS)?;
    let profile_rows = transaction.open_table(PROFILES)?;
    for entry in bindings.iter()? {
        let (job, bytes) = entry?;
        let binding = decode_binding(bytes.value())?;
        let row = jobs
            .get(job.value())?
            .ok_or_else(|| StorageError::corrupted("Pool binding names missing Job"))?;
        let row = JobRow::decode(row.value())?;
        if row.id.get() != job.value()
            || row.started_at.is_none()
            || row.id == genesis_id(&transaction.open_table(META)?)?
        {
            return Err(StorageError::corrupted("invalid frozen Pool binding owner"));
        }
        if pools.get(binding.pool.get())?.is_none() {
            return Err(StorageError::corrupted("frozen binding names missing Pool"));
        }
        let profile = profile_rows
            .get(binding.pool_profile.get())?
            .ok_or_else(|| StorageError::corrupted("binding names missing Profile"))?;
        if !matches!(
            ProfileRow::decode(profile.value())?.config,
            ProfileConfig::ExecutionPool(_)
        ) {
            return Err(StorageError::corrupted(
                "frozen binding has wrong Profile kind",
            ));
        }
        assigned.insert(job.value(), binding.pool.get())?;
        profiles.insert((binding.pool_profile.get(), job.value()), ())?;
        if !row.locally_stopped() {
            occupancy.insert((binding.pool.get(), job.value()), ())?;
        }
    }
    if occupancy.len()? > u64::from(GLOBAL_EXECUTION_LIMIT) {
        return Err(StorageError::corrupted(
            "execution occupancy exceeds global limit",
        ));
    }
    Ok(())
}
