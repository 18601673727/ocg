use super::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ExecutionEvent {
    pub job: JobId,
    pub seq: u64,
    pub previous_seq: u64,
    pub transaction: u64,
    pub at: Timestamp,
    pub kind: ExecutionEventKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ExecutionEventKind {
    Created {
        phase: Option<MutableJobPhase>,
    },
    PhaseChanged {
        phase: MutableJobPhase,
    },
    InputRevisionRecorded {
        revision: u64,
    },
    AllocationReserved {
        allocation: Box<PoolAllocation>,
    },
    AllocationStarted,
    AllocationPhaseChanged {
        phase: AllocationPhase,
    },
    AllocationStopConfirmed {
        at: Timestamp,
        proof: StopProof,
    },
    JobOutcome {
        outcome: OutcomeRecord,
    },
    Closing {
        at: Timestamp,
        by: Actor,
        running: bool,
    },
    ClosingRunningChanged {
        running: bool,
    },
    HoldChanged {
        reason: Option<u32>,
    },
    VisibilityChanged {
        at: Option<Timestamp>,
    },
    PrerequisitesChanged {
        after: Vec<(JobId, DependencyCondition)>,
    },
    DependencyAdded {
        prerequisite: JobId,
        condition: DependencyCondition,
    },
    DependencyResolved {
        prerequisite: JobId,
        state: DependencyState,
    },
    RequestRecorded {
        request: RequestNumber,
        seq: u32,
    },
    ToolRecorded {
        call: ToolCallRef,
        completed: bool,
    },
    OutputRecorded {
        seq: u32,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ExecutionSnapshot {
    pub job: JobId,
    pub seq: u64,
    pub phase: Option<MutableJobPhase>,
    pub input_revision: Option<u64>,
    pub outcome: Option<OutcomeRecord>,
    pub allocation: Option<PoolAllocation>,
    pub closing: Option<JobClosing>,
    pub on_hold: Option<u32>,
    pub hidden_at: Option<Timestamp>,
    pub prerequisites: Vec<(JobId, DependencyCondition)>,
    pub dependencies: Vec<(JobId, DependencyCondition)>,
}

impl ExecutionSnapshot {
    pub(crate) fn new(job: JobId) -> Self {
        Self {
            job,
            seq: 0,
            phase: None,
            input_revision: None,
            outcome: None,
            allocation: None,
            closing: None,
            on_hold: None,
            hidden_at: None,
            prerequisites: Vec::new(),
            dependencies: Vec::new(),
        }
    }
}
