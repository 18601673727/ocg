use crate::db::entities::identity::JobId;
use crate::db::entities::{
    Actor, AllocationExecutor, AllocationPhase, AllocationPool, AttachmentRecord,
    DependencyCondition, DependencyState, FrozenDecisionConfig, FrozenExecutionConfig,
    GlobalCompactionSettings, JobClosing, JobLifecycle, JobOutputRecord, JobSpec, MutableJob,
    MutableJobPhase, MutableJobRecord, NativeSummaryManifest, OutcomeRecord, PoolAllocation,
    ProjectRecord, RequestNumber, StopProof, SummarizationResult, Timestamp, ToolCallRef,
};

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

pub(crate) struct ExecutionHistoryItem {
    pub event: ExecutionEvent,
    pub committed_seq: u64,
}

pub(crate) struct ExecutionHistoryPage {
    pub events: Vec<ExecutionHistoryItem>,
    pub next_after: Option<u64>,
    pub head_seq: u64,
}

pub(crate) trait DevelopmentContextRequirements: Send + Sync {
    fn max_images(&self) -> usize;
    fn prompt_bytes(
        &self,
        configuration: &FrozenExecutionConfig,
        project: Option<&ProjectRecord>,
        summary: Option<&VerifiedNativeSummary>,
    ) -> Result<usize, serde_json::Error>;
    fn admits_tokens(
        &self,
        model: &crate::db::entities::LlmConfig,
        selected_bytes: usize,
        image_count: usize,
    ) -> bool;
}

pub(crate) struct DevelopmentExecutionContext {
    pub allocation: PoolAllocation,
    pub job: MutableJob,
    pub pool: AllocationPool,
    pub executor: AllocationExecutor,
    pub configuration: FrozenExecutionConfig,
    pub conversation: JobSpec,
    pub conversation_lifecycle: JobLifecycle,
    pub plan_step: bool,
    pub project: Option<ProjectRecord>,
    pub compaction: GlobalCompactionSettings,
    pub attachments: Vec<AttachmentRecord>,
    pub adopted_summary: Option<VerifiedNativeSummary>,
    pub covered_inputs: Vec<DevelopmentRetainedInput>,
    pub history: Vec<DevelopmentContextTurn>,
}

impl DevelopmentExecutionContext {
    pub(crate) fn is_summary_work(&self) -> bool {
        self.job.summary_source.is_some() || self.conversation.is_chunk_group()
    }
}

pub(crate) struct DevelopmentDecisionContext {
    pub allocation: PoolAllocation,
    pub job: MutableJob,
    pub pool: AllocationPool,
    pub executor: AllocationExecutor,
    pub configuration: FrozenDecisionConfig,
    pub conversation: JobSpec,
    pub conversation_lifecycle: JobLifecycle,
    pub project: Option<ProjectRecord>,
}

#[allow(clippy::large_enum_variant)]
pub(crate) enum DevelopmentCandidate {
    Generative(DevelopmentExecutionContext),
    Decision(DevelopmentDecisionContext),
}

impl DevelopmentCandidate {
    pub(crate) fn allocation(&self) -> &PoolAllocation {
        match self {
            Self::Generative(context) => &context.allocation,
            Self::Decision(context) => &context.allocation,
        }
    }
}

pub(crate) struct DevelopmentRetainedInput {
    pub job: JobId,
    pub text: String,
    pub attachments: Vec<AttachmentRecord>,
}

pub(crate) struct DevelopmentContextTurn {
    pub record: MutableJobRecord,
    pub attachments: Vec<AttachmentRecord>,
    pub outputs: Vec<JobOutputRecord>,
}

#[derive(Clone, Debug)]
pub(crate) struct VerifiedNativeSummary {
    pub manifest: NativeSummaryManifest,
    pub summary: SummarizationResult,
}
