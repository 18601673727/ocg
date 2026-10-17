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

fn decode(bytes: &[u8]) -> DecodeResult<ExecutionPool> {
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
        )
    }

    pub(crate) fn put_execution_pool(&mut self, pool: &ExecutionPool) -> Result<(), BatchError> {
        self.require_execution_pool(pool.id)?;
        self.require_profile_kind(pool.profile, ProfileKind::ExecutionPool)?;
        self.overwrite(EXECUTION_POOLS, pool.id.get(), encode(pool))
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
