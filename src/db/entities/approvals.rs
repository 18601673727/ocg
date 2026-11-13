use super::*;

pub const MAX_STORED_TOOL_ARGUMENTS_BYTES: usize = 64 * 1024;
pub const MAX_HANDLER_VERSION_BYTES: usize = 64;

pub fn is_handler_version(version: &str) -> bool {
    !version.is_empty()
        && version.len() <= MAX_HANDLER_VERSION_BYTES
        && version
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_' | b'+'))
}

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(deny_unknown_fields)]
pub struct ToolCallRef {
    pub job: JobId,
    pub request: RequestNumber,
    pub index: u32,
}

impl fmt::Display for ToolCallRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "tool call {} of {} of {}",
            self.index, self.request, self.job
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ToolEffect {
    ReadOnly,
    Idempotent,
    NonIdempotent,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthorizationBasis {
    RegistryGrant,
}

#[derive(Clone, PartialEq, Eq)]
pub struct ToolPayload(String);

impl ToolPayload {
    pub fn new(text: impl Into<String>) -> Self {
        Self(text.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Debug for ToolPayload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ToolPayload({} bytes)", self.0.len())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ToolDecision {
    Refused(ToolCallRefusal),
    Admitted(ToolAdmission),
    AwaitingApproval(ToolAdmission),
    AwaitingAnswer(ToolAdmission),
}

impl ToolDecision {
    pub fn stored_admission(&self) -> Option<&ToolAdmission> {
        match self {
            Self::Admitted(a) | Self::AwaitingApproval(a) | Self::AwaitingAnswer(a) => Some(a),
            Self::Refused(_) => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ApprovalDecision {
    Approved,
    Rejected,
    Answered { text: String },
    Voided,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApprovalRecord {
    pub decision: ApprovalDecision,
    pub by: super::Actor,
    pub at: Timestamp,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToolAdmission {
    pub handler_version: String,
    pub effect: ToolEffect,
    pub parameters: String,
    pub arguments: ToolPayload,
    pub authorization: AuthorizationBasis,
}

impl ToolAdmission {
    pub fn spawns(&self) -> bool {
        matches!(
            self.handler_version.as_str(),
            "workspace_run_command-1"
                | "workspace_run_command-2"
                | "workspace_run_command-3"
                | "workspace_run_command-4"
                | "workspace_run_command-5"
                | "workspace_run_command-6"
                | "workspace_run_command-7"
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToolEvidence {
    pub call: ToolCallRef,
    pub approval: Option<ToolCallRef>,
    pub tool_call_id: String,
    pub name: String,
    pub decision: ToolDecision,
    pub spawns: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WithheldReason {
    Unserializable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UncertainCause {
    TimedOut,
    Cancelled,
    HandlerReported,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ToolOutcome {
    Completed {
        result: ToolPayload,
    },
    CompletedWithheld {
        reason: WithheldReason,
    },
    CompletedElided {
        bytes: u64,
        digest: ContentDigest,
    },
    NotApplied {
        message: String,
    },
    NotInvoked {
        reason: NotInvokedReason,
    },
    Uncertain {
        cause: UncertainCause,
        message: Option<String>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NotInvokedReason {
    Refused(ToolCallRefusal),
    Cancelled,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, ts_rs::TS)]
#[serde(rename_all = "snake_case")]
pub enum ToolOutcomeKind {
    Completed,
    CompletedWithheld,
    CompletedElided,
    NotApplied,
    NotInvoked,
    Uncertain,
}

impl ToolOutcome {
    pub fn kind(&self) -> ToolOutcomeKind {
        match self {
            Self::Completed { .. } => ToolOutcomeKind::Completed,
            Self::CompletedWithheld { .. } => ToolOutcomeKind::CompletedWithheld,
            Self::CompletedElided { .. } => ToolOutcomeKind::CompletedElided,
            Self::NotApplied { .. } => ToolOutcomeKind::NotApplied,
            Self::NotInvoked { .. } => ToolOutcomeKind::NotInvoked,
            Self::Uncertain { .. } => ToolOutcomeKind::Uncertain,
        }
    }

    pub fn is_definite(&self) -> bool {
        !matches!(self, Self::Uncertain { .. })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EffectConclusion {
    Applied,
    NotApplied,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToolReconciliation {
    pub conclusion: EffectConclusion,
    pub reconciler: String,
    pub evidence: String,
    pub explanation: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Recorded<T> {
    pub fact: T,
    pub at: Timestamp,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToolEvidenceRecord {
    pub call: ToolCallRef,
    pub approval: Option<ToolCallRef>,
    pub tool_call_id: String,
    pub name: String,
    pub spawns: bool,
    pub decision: Recorded<ToolDecision>,
    pub outcome: Option<Recorded<ToolOutcome>>,
    pub reconciliation: Option<Recorded<ToolReconciliation>>,
}

impl ToolEvidenceRecord {
    pub fn admission(&self) -> Option<&ToolAdmission> {
        match &self.decision.fact {
            ToolDecision::Admitted(admission) => Some(admission),
            ToolDecision::Refused(_)
            | ToolDecision::AwaitingApproval(_)
            | ToolDecision::AwaitingAnswer(_) => None,
        }
    }
}
