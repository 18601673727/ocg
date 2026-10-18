use super::*;
use std::num::NonZeroUsize;
use std::time::Duration;

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PendingFence {
    pub len: u64,
    pub high_water: JobId,
    pub end: Option<PendingJobKey>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PendingIndexFault {
    MissingJob,
    WrongJobIdentity,
    NotPending,
    Genesis,
    KeyMismatch,
    JobDecode,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PendingReadFault {
    IndexEntry {
        fence: PendingFence,
        key: PendingJobKey,
        kind: PendingIndexFault,
        service_time: Duration,
    },
    Allocation {
        fence: PendingFence,
        service_time: Duration,
    },
    InvalidFence,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingCapturePage {
    pub fence: PendingFence,
    pub page: PendingJobsPage,
    pub service_time: Duration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingSnapshot {
    pub fence: PendingFence,
    pub keys: Vec<PendingJobKey>,
    pub complete: bool,
    pub resume_after: Option<JobId>,
    pub service_time: Duration,
}

pub(crate) fn bounded_page(limit: NonZeroUsize) -> usize {
    limit.get().min(1_000)
}
