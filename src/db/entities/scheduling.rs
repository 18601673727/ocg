use super::*;

pub const GLOBAL_EXECUTION_LIMIT: u8 = 16;

/// A Pool configuration for the independent allocation model. The legacy
/// scheduler does not use this type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AllocationPool {
    pub id: ExecutionPoolId,
    pub name: String,
    pub capacity: NonZeroU32,
    pub enabled: bool,
    pub archived: bool,
    pub revision: u32,
}

/// An execution resource belongs to one Pool and can hold one allocation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AllocationExecutor {
    pub id: ExecutorId,
    pub pool: ExecutionPoolId,
    pub name: String,
    pub enabled: bool,
    pub archived: bool,
    pub revision: u32,
    pub capabilities: BTreeSet<(ExecutionOperation, Option<LlmTransport>)>,
}

/// A generation identifies ownership; it is not a separate executable unit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AllocationToken {
    pub job: JobId,
    pub pool: ExecutionPoolId,
    pub executor: ExecutorId,
    pub generation: NonZeroU32,
    pub epoch: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AllocationPhase {
    Reserved,
    Running,
    StopRequested,
    Uncertain,
    Released,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PoolAllocation {
    pub token: AllocationToken,
    pub request_id: ReservationRequestId,
    pub request_after: Option<DevelopmentReadyCursor>,
    pub job_revision: u64,
    pub phase: AllocationPhase,
    /// When the Executor was reserved for the Job. Not the start of execution.
    pub claimed_at: Timestamp,
    /// When the checked start committed, the only moment an Allocation becomes
    /// Running. Absent while Reserved, and for a Reservation that ended or
    /// became Uncertain without a start.
    pub started_at: Option<Timestamp>,
    pub host_id: Option<String>,
    pub boot_id: Option<String>,
    pub pool_revision: u32,
    pub executor_revision: u32,
    pub operation: ExecutionOperation,
    pub protocol: Option<LlmTransport>,
    pub stop_confirmed_at: Option<Timestamp>,
    pub stop_proof: Option<StopProof>,
}

/// Why a Job was ended while its Allocation was still only Reserved, before
/// any execution authority existed. Nothing was sent to a Provider.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnstartedCause {
    /// A credential the Job's frozen configuration names was deleted, or
    /// replaced since the Job was read for handoff.
    FrozenSecretUnavailable,
    /// The Job's input, attachments or context cannot be prepared safely.
    InputUnavailable,
    /// The checked start refused the Job for another reason.
    StartRefused,
    /// The process that reserved the Job stopped before the checked start
    /// committed, and an offline reconciliation ended the untouched
    /// reservation.
    OfflineReconciled,
}

impl UnstartedCause {
    /// The typed failure code stored with the Job's Result.
    pub fn code(self) -> &'static str {
        match self {
            Self::FrozenSecretUnavailable => "frozen_secret_unavailable",
            Self::InputUnavailable => "input_unavailable",
            Self::StartRefused => "start_refused",
            Self::OfflineReconciled => "reconciled_unstarted_reservation",
        }
    }

    pub fn summary(self) -> &'static str {
        match self {
            Self::FrozenSecretUnavailable => {
                "A credential this Job was submitted with is no longer available, so it did not start. Nothing was sent to the Provider."
            }
            Self::InputUnavailable => {
                "This Job's input could not be prepared safely, so it did not start. Nothing was sent to the Provider."
            }
            Self::StartRefused => {
                "This Job could not be started safely. Nothing was sent to the Provider."
            }
            Self::OfflineReconciled => {
                "OCG stopped after reserving capacity for this Job and before it started; an offline reconciliation ended it. Nothing was sent to the Provider."
            }
        }
    }
}

impl PoolAllocation {
    pub fn holds_capacity(&self) -> bool {
        self.phase != AllocationPhase::Released
    }

    pub(crate) fn matches_request(&self, request: DevelopmentReservationRequest) -> bool {
        self.request_id == request.id
            && self.token.pool == request.pool
            && self.token.executor == request.executor
            && self.request_after == request.after
    }
}

/// An opaque continuation through the development model's ordered ready index.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DevelopmentReadyCursor(pub(crate) (u8, i64, u128, u16));

/// An internal allocator request. Its ID is correlation data, not a credential.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DevelopmentReservationRequest {
    pub id: ReservationRequestId,
    pub pool: ExecutionPoolId,
    pub executor: ExecutorId,
    pub after: Option<DevelopmentReadyCursor>,
}

/// A non-cloneable first-attempt permit binds the request before submission.
/// It is not an authorization credential.
#[derive(Debug, PartialEq, Eq)]
pub struct DevelopmentFreshReservation {
    request: DevelopmentReservationRequest,
}

#[allow(dead_code)]
impl DevelopmentFreshReservation {
    pub(crate) fn new(
        pool: ExecutionPoolId,
        executor: ExecutorId,
        after: Option<DevelopmentReadyCursor>,
    ) -> Self {
        Self {
            request: DevelopmentReservationRequest {
                id: ReservationRequestId::new(uuid::Uuid::now_v7().as_u128()),
                pool,
                executor,
                after,
            },
        }
    }

    /// Persist this request before consuming the permit if it must survive a crash.
    pub(crate) fn request(&self) -> DevelopmentReservationRequest {
        self.request
    }

    pub(crate) fn matches(&self, request: DevelopmentReservationRequest) -> bool {
        self.request == request
    }
}

/// A caller must use `Replay` after any uncertain reservation result.
#[derive(Debug, PartialEq, Eq)]
pub enum DevelopmentReservationAttempt {
    /// A first-attempt permit can be consumed only once.
    New(DevelopmentFreshReservation),
    /// Resolve an earlier attempt without creating an Allocation.
    Replay,
}

/// The result of one bounded, manually requested reservation pass.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DevelopmentReservation {
    Reserved(PoolAllocation),
    /// Stored evidence only; it does not authorize dispatch or recovery.
    Replayed(PoolAllocation),
    /// No durable Allocation bears the requested identity.
    Unresolved,
    Continue(DevelopmentReadyCursor),
    Exhausted,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DevelopmentReservationReconciliation {
    /// Stored evidence only; it does not authorize dispatch or recovery.
    Found(Box<PoolAllocation>),
    Conflict,
    /// No durable record was found; absence alone does not prove non-commit.
    Unresolved,
}

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
