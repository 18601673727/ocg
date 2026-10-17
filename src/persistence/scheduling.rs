use super::*;
use std::num::{NonZeroU32, NonZeroUsize};
use std::time::Duration;

pub const GLOBAL_EXECUTION_LIMIT: u8 = 16;
pub const EVALUATION_BUDGET: u32 = 32_768;
pub const EVALUATION_CANDIDATES: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrozenPoolBinding {
    pub pool: ExecutionPoolId,
    pub pool_profile: ProfileId,
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
    pub resume_after: Option<PendingJobKey>,
    pub service_time: Duration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EvalRequest {
    pub candidates: Vec<JobId>,
    pub budget: NonZeroU32,
    pub continuation: Option<EvalContinuation>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EvalContinuation {
    pub job: JobId,
    pub after_prerequisite: JobId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EvalPage {
    pub verdicts: Vec<(JobId, Readiness)>,
    pub continuation: Option<EvalContinuation>,
    pub work_used: u32,
    pub service_time: Duration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Readiness {
    Ready {
        compatible_pools: BTreeSet<ExecutionPoolId>,
    },
    Blocked(Block),
    Indeterminate(Why),
    BudgetExhausted,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Block {
    NotPending,
    Genesis,
    Paused,
    NotYetEligible { eligible_at: Timestamp },
    DependencyWaiting,
    DependencyImpossible,
    NoCompatiblePool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Why {
    Unbounded,
    MissingJob,
    MissingPrerequisite(JobId),
    Corrupt,
}

pub(crate) fn bounded_page(limit: NonZeroUsize) -> usize {
    limit.get().min(1_000)
}
