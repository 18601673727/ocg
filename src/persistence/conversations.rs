use super::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Origin {
    User,
    Agent { call: ToolCallRef },
    System,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Actor {
    Auto,
    Agent { conversation: JobId },
    User,
    Loop,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StopProof {
    Proof,
    Reboot,
    Settled,
    User,
    Operator,
}

#[derive(Clone, Debug, PartialEq)]
#[allow(clippy::large_enum_variant)] // Keep the public spec value inline, as Frozen carries its snapshot.
pub enum TurnExecution {
    Frozen(FrozenExecutionConfig),
    Ref(JobId),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlanSource {
    Stored {
        previous: Option<JobId>,
        dependencies: Vec<(JobId, DependencyCondition)>,
    },
    Process {
        project: Option<ProjectId>,
        grant: Option<String>,
        eligible_at: Option<Timestamp>,
        objective: Option<String>,
        attachments: Vec<AttachmentId>,
    },
}

#[derive(Clone, Debug, PartialEq)]
#[allow(clippy::large_enum_variant)] // Mirrors the stored spec without a second allocation.
pub enum JobKind {
    Conversation {
        agent: Option<ConfigId>,
        project: Option<ProjectId>,
        origin: Origin,
        keep_open: bool,
    },
    Turn {
        name: Option<String>,
        execution: TurnExecution,
        plan_source: PlanSource,
    },
}

impl JobKind {
    pub fn is_conversation(&self) -> bool {
        matches!(self, Self::Conversation { .. })
    }
    pub fn is_stored(&self) -> bool {
        matches!(
            self,
            Self::Turn {
                plan_source: PlanSource::Stored { .. },
                ..
            }
        )
    }
    pub fn previous(&self) -> Option<JobId> {
        match self {
            Self::Turn {
                plan_source: PlanSource::Stored { previous, .. },
                ..
            } => *previous,
            Self::Conversation { .. }
            | Self::Turn {
                plan_source: PlanSource::Process { .. },
                ..
            } => None,
        }
    }
    pub fn dependencies(&self) -> &[(JobId, DependencyCondition)] {
        match self {
            Self::Turn {
                plan_source: PlanSource::Stored { dependencies, .. },
                ..
            } => dependencies,
            Self::Conversation { .. }
            | Self::Turn {
                plan_source: PlanSource::Process { .. },
                ..
            } => &[],
        }
    }
    pub fn eligible_at(&self) -> Option<Timestamp> {
        match self {
            Self::Turn {
                plan_source: PlanSource::Process { eligible_at, .. },
                ..
            } => *eligible_at,
            Self::Conversation { .. }
            | Self::Turn {
                plan_source: PlanSource::Stored { .. },
                ..
            } => None,
        }
    }
    pub fn objective(&self) -> Option<&str> {
        match self {
            Self::Turn {
                plan_source: PlanSource::Process { objective, .. },
                ..
            } => objective.as_deref(),
            Self::Conversation { .. }
            | Self::Turn {
                plan_source: PlanSource::Stored { .. },
                ..
            } => None,
        }
    }
    pub fn attachments(&self) -> &[AttachmentId] {
        match self {
            Self::Turn {
                plan_source: PlanSource::Process { attachments, .. },
                ..
            } => attachments,
            Self::Conversation { .. }
            | Self::Turn {
                plan_source: PlanSource::Stored { .. },
                ..
            } => &[],
        }
    }
    pub fn name(&self) -> Option<&str> {
        match self {
            Self::Turn { name, .. } => name.as_deref(),
            Self::Conversation { .. } => None,
        }
    }
    pub(crate) fn waits_on(&self) -> impl Iterator<Item = (JobId, DependencyCondition)> + '_ {
        self.dependencies().iter().copied().chain(
            self.previous()
                .map(|previous| (previous, DependencyCondition::Settled)),
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConversationSpec {
    pub name: Option<String>,
    pub agent: Option<ConfigId>,
    pub project: Option<ProjectId>,
    pub origin: Origin,
    pub keep_open: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum EntryKind {
    UserMessage,
    AgentMessage,
    Notice,
    Decision,
    RetryOf,
    Iteration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Entry {
    UserMessage {
        text: String,
        attachments: Vec<AttachmentId>,
    },
    AgentMessage {
        text: String,
        call: ToolCallRef,
    },
    Notice {
        child: JobId,
        event: NoticeEvent,
        reference: JobId,
    },
    Decision {
        approval: (JobId, u32, u32),
    },
    RetryOf {
        turn: JobId,
        by: Actor,
    },
    Iteration {
        n: u64,
    },
}
impl Entry {
    pub fn kind(&self) -> EntryKind {
        match self {
            Self::UserMessage { .. } => EntryKind::UserMessage,
            Self::AgentMessage { .. } => EntryKind::AgentMessage,
            Self::Notice { .. } => EntryKind::Notice,
            Self::Decision { .. } => EntryKind::Decision,
            Self::RetryOf { .. } => EntryKind::RetryOf,
            Self::Iteration { .. } => EntryKind::Iteration,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TurnEntry {
    pub entry: Entry,
    pub at: Timestamp,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Appended {
    pub turn: JobId,
    pub seq: u32,
    pub joined: bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NoticeClass {
    Attention,
    Progress,
    Record,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordEvent {
    Hold,
    Unhold,
    Stop,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NoticeEvent {
    ClosedSelf,
    ClosedAbove,
    StoppedAbove,
    TurnEnded { kind: OutcomeKind },
    Requested,
    Stalled,
    ObjectiveCompleted,
    WaitersBlocked { waiting: Vec<JobId> },
    IterationCompleted,
    RecurrenceHalted,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NoticeBody {
    Event {
        child: JobId,
        event: NoticeEvent,
        reference: JobId,
    },
    Record {
        event: RecordEvent,
        by: Actor,
        at: Timestamp,
    },
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Notice {
    pub class: NoticeClass,
    pub body: NoticeBody,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConversationAction {
    pub conversation: JobId,
    pub record_seq: Option<u32>,
    pub running: Vec<JobId>,
    pub already: bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StopCause {
    Stopped(u32),
    Closed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecurrencePhase {
    Active,
    WaitingDue { due: i64 },
    Halted { reason: RecurrenceHalt },
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecurrenceHalt {
    Closed,
    ConfigUnavailable,
    Refused,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Recurrence {
    pub phase: RecurrencePhase,
    pub n: u64,
    pub current_turn: JobId,
    pub previous_turn: Option<JobId>,
    pub interval_s: u32,
}
