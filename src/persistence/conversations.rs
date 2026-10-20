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
    Stored,
    Process {
        project: Option<ProjectId>,
        grant: Option<String>,
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
                plan_source: PlanSource::Stored,
                ..
            }
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
    MessageCompleted,
    AgentTurnCompleted,
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
