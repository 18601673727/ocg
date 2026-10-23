use super::*;

pub const GLOBAL_EXECUTION_LIMIT: u8 = 16;

/// The Pool a Job was claimed into and what the Pool was when it was.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FrozenPoolBinding {
    pub pool: ExecutionPoolId,
    pub edit_seq: u32,
    pub capabilities: BTreeSet<(ExecutionOperation, Option<LlmTransport>)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClaimRequest {
    pub job: JobId,
    pub allowed_pools: BTreeSet<ExecutionPoolId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClaimGrant {
    pub job: JobId,
    pub binding: FrozenPoolBinding,
    pub epoch: u64,
    pub global_free_after: u8,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CapacityScope {
    Global,
    Pools(BTreeSet<ExecutionPoolId>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClaimRefusal {
    EmptyAllowedPools,
    NotFound,
    Genesis,
    NotPending,
    Paused,
    NotYetEligible {
        eligible_at: Timestamp,
    },
    NoCapacity(CapacityScope),
    NoCompatiblePool,
    DependencyUnsatisfied {
        prerequisite: JobId,
        state: DependencyState,
    },
    Unbounded {
        prerequisite: JobId,
    },
    RecipeInvalid,
}
