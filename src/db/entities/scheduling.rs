use super::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AllocationPool {
    pub id: ExecutionPoolId,
    pub name: String,
    pub capacity: NonZeroU32,
    pub enabled: bool,
    pub archived: bool,
    pub revision: u32,
}

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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecutorUpdate {
    Enabled(bool),
    Name(String),
    Capabilities(BTreeSet<(ExecutionOperation, Option<LlmTransport>)>),
}

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
    pub claimed_at: Timestamp,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnstartedCause {
    FrozenSecretUnavailable,
    InputUnavailable,
    StartRefused,
    OfflineReconciled,
}

impl UnstartedCause {
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DevelopmentReadyCursor(pub(crate) (u8, i64, u128, u16));

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DevelopmentReservationRequest {
    pub id: ReservationRequestId,
    pub pool: ExecutionPoolId,
    pub executor: ExecutorId,
    pub after: Option<DevelopmentReadyCursor>,
}

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

    pub(crate) fn request(&self) -> DevelopmentReservationRequest {
        self.request
    }

    pub(crate) fn matches(&self, request: DevelopmentReservationRequest) -> bool {
        self.request == request
    }
}

#[derive(Clone, Copy, Debug)]
pub enum ReadySelection {
    Any,
    Only(JobId),
    Scheduled {
        generative_policy: fn(&FrozenExecutionConfig) -> bool,
        decision_policy: fn(&NativeDecisionConfig) -> bool,
        workspace_permit: bool,
    },
}

pub const STOP_JOB_NOT_RUNNING: &str = "the Job is not running";
pub const STOP_JOB_ENDED: &str = "the Job has already ended";
pub const STOPPED_BEFORE_SEND: &str = "a stop was requested before the request was sent";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeStopRequest {
    EndedBeforeStart(AllocationToken),
    Requested(AllocationToken),
    AlreadyRequested(AllocationToken),
}

#[derive(Debug, PartialEq, Eq)]
pub enum DevelopmentReservationAttempt {
    New(DevelopmentFreshReservation),
    Replay,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DevelopmentReservation {
    Reserved(PoolAllocation),
    Replayed(PoolAllocation),
    Unresolved,
    Continue(DevelopmentReadyCursor),
    Exhausted,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DevelopmentReservationReconciliation {
    Found(Box<PoolAllocation>),
    Conflict,
    Unresolved,
}
