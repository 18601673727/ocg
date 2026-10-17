use super::codec::TAG_EXECUTION_POOL;
use super::*;

fn encode(pool: &ExecutionPool) -> Vec<u8> {
    let mut encoder = Enc::new(TAG_EXECUTION_POOL);
    encoder.u64(pool.id.get());
    encoder.str(&pool.name);
    encoder.u64(pool.profile.get());
    encoder.bool(pool.enabled);
    encoder.ts(pool.created_at);
    encoder.ts(pool.updated_at);
    encoder.0
}

pub(super) fn decode(bytes: &[u8]) -> DecodeResult<ExecutionPool> {
    let mut decoder = Dec::new(bytes, TAG_EXECUTION_POOL)?;
    let pool = ExecutionPool {
        id: ExecutionPoolId::new(decoder.u64()?),
        name: decoder.str()?,
        profile: ProfileId::new(decoder.u64()?),
        enabled: decoder.bool()?,
        created_at: decoder.ts()?,
        updated_at: decoder.ts()?,
    };
    decoder.end()?;
    Ok(pool)
}

impl Batch {
    pub(crate) fn require_execution_pool(
        &mut self,
        id: ExecutionPoolId,
    ) -> Result<ExecutionPool, BatchError> {
        let Some(bytes) = self.get_bytes(EXECUTION_POOLS, id.get())? else {
            return self.reject(Violation::NotFound(RecordRef::ExecutionPool(id)));
        };
        self.decoded(decode(&bytes))
    }

    pub(crate) fn insert_execution_pool(&mut self, pool: &ExecutionPool) -> Result<(), BatchError> {
        self.require_profile_kind(pool.profile, ProfileKind::ExecutionPool)?;
        self.insert_new(
            EXECUTION_POOLS,
            pool.id.get(),
            encode(pool),
            RecordRef::ExecutionPool(pool.id),
        )?;
        self.refresh_pool_compatibility()
    }

    pub(crate) fn put_execution_pool(&mut self, pool: &ExecutionPool) -> Result<(), BatchError> {
        self.require_execution_pool(pool.id)?;
        self.require_profile_kind(pool.profile, ProfileKind::ExecutionPool)?;
        self.overwrite(EXECUTION_POOLS, pool.id.get(), encode(pool))?;
        self.refresh_pool_compatibility()
    }
}

pub(crate) fn read_execution_pool(
    database: &Database,
    id: ExecutionPoolId,
) -> Result<Option<ExecutionPool>, StorageError> {
    let transaction = read(database)?;
    transaction
        .open_table(EXECUTION_POOLS)?
        .get(id.get())?
        .map(|value| decode(value.value()))
        .transpose()
}

pub(crate) fn read_execution_pools(
    database: &Database,
    before: Option<ExecutionPoolId>,
    limit: usize,
) -> Result<Vec<ExecutionPool>, StorageError> {
    reads::newest(
        &read(database)?,
        EXECUTION_POOLS,
        before.map(ExecutionPoolId::get),
        limit,
        decode,
    )
}

pub(super) fn compatibility_key(protocol: Option<LlmTransport>) -> (u8, u8) {
    (1, protocol.map_or(0, Code::code))
}

pub(super) fn decode_compatible(bytes: &[u8]) -> Result<BTreeSet<ExecutionPoolId>, StorageError> {
    if !bytes.len().is_multiple_of(8) {
        return Err(StorageError::corrupted(
            "invalid compatible Pool projection",
        ));
    }
    let mut pools = BTreeSet::new();
    for chunk in bytes.as_chunks::<8>().0 {
        let id = u64::from_le_bytes(*chunk);
        if id == 0 || !pools.insert(ExecutionPoolId::new(id)) {
            return Err(StorageError::corrupted(
                "duplicate or invalid compatible Pool identity",
            ));
        }
    }
    Ok(pools)
}

pub(super) fn rebuild_compatibility(transaction: &WriteTransaction) -> Result<(), StorageError> {
    transaction.delete_table(COMPATIBLE_POOLS)?;
    let pools = transaction.open_table(EXECUTION_POOLS)?;
    let profiles = transaction.open_table(PROFILES)?;
    let mut projection: BTreeMap<(u8, u8), Vec<u8>> = BTreeMap::new();
    for entry in pools.iter()? {
        let (id, bytes) = entry?;
        let pool = decode(bytes.value())?;
        if pool.id.get() != id.value() {
            return Err(StorageError::corrupted("Pool identity mismatch"));
        }
        let profile = profiles
            .get(pool.profile.get())?
            .ok_or_else(|| StorageError::corrupted("Pool Profile missing"))?;
        let ProfileConfig::ExecutionPool(config) = ProfileRow::decode(profile.value())?.config
        else {
            return Err(StorageError::corrupted("Pool Profile kind mismatch"));
        };
        if !pool.enabled
            || !config
                .capabilities
                .operations
                .contains(&ExecutionOperation::LlmRequest)
        {
            continue;
        }
        for protocol in
            std::iter::once(None).chain(config.capabilities.llm_protocols.into_iter().map(Some))
        {
            let packed = projection.entry(compatibility_key(protocol)).or_default();
            packed
                .try_reserve(8)
                .map_err(execution_claim::allocation_error)?;
            packed.extend_from_slice(&pool.id.get().to_le_bytes());
        }
    }
    let mut table = transaction.open_table(COMPATIBLE_POOLS)?;
    for (key, bytes) in projection {
        table.insert(key, bytes.as_slice())?;
    }
    Ok(())
}

impl Batch {
    pub(crate) fn refresh_pool_compatibility(&mut self) -> Result<(), BatchError> {
        self.step(rebuild_compatibility)
    }
}
