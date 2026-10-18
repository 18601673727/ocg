use super::*;

/// The largest arguments of one tool call that OCG records, and so runs.
/// It is stricter than the transport's own argument limit,
/// [`crate::openai_compatible::MAX_TOOL_ARGUMENTS_BYTES`].
pub const MAX_STORED_TOOL_ARGUMENTS_BYTES: usize = 64 * 1024;
/// The largest sum of the recorded arguments of one Job's admitted calls.
pub const MAX_STORED_TOOL_ARGUMENTS_PER_JOB_BYTES: usize = 256 * 1024;
/// The largest sum of the parameter schemas recorded with one Job's
/// admitted calls. Each admission records the schema its arguments were
/// checked against, so this bounds what repeated calls of a tool store.
pub const MAX_STORED_TOOL_SCHEMAS_PER_JOB_BYTES: usize = 1024 * 1024;
/// The longest handler version a tool may declare.
pub const MAX_HANDLER_VERSION_BYTES: usize = 64;
pub(super) const MAX_RECONCILER_BYTES: usize = 256;
pub(super) const MAX_RECONCILIATION_TEXT_BYTES: usize = 4 * 1024;

/// One tool call a Provider request asked for: the request of the Job that
/// sent it and the call's wire index in that request's response. It keys the
/// call's [`ToolEvidenceRecord`], so a call has at most one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
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

/// What a tool's handler may change outside OCG, as the trusted code that
/// implements and registers it declares.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ToolEffect {
    /// Changes nothing outside OCG, so running it again repeats nothing.
    ReadOnly,
    /// Running it again with the same arguments leaves what one run left.
    /// This must hold without an idempotency key: nothing OCG could derive
    /// from a call stays the same in a retry, which is another Job.
    Idempotent,
    /// Running it again may repeat its effect.
    NonIdempotent,
}

/// Why OCG let an admitted call run. OCG has no persistent permission
/// system: the tool registry's explicit grant is the only basis there is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthorizationBasis {
    RegistryGrant,
}

/// Tool arguments or a tool result, exactly as they were sent. Its `Debug`
/// shows only its size, so that neither reaches a log or an error message
/// by accident.
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

/// The first durable fact about a call the Provider requested: whether OCG
/// refused it or admitted it to run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ToolDecision {
    /// OCG did not authorize the call, and never ran it.
    Refused(ToolCallRefusal),
    /// OCG accepted the call for execution. The handler may have been
    /// invoked only once this was durable.
    Admitted(ToolAdmission),
}

/// What OCG accepted for one execution: the exact registration, the
/// arguments as checked and why it was authorized.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToolAdmission {
    /// The version the handler's registration declared.
    pub handler_version: String,
    pub effect: ToolEffect,
    /// The parameter schema the registration declared, serialized as it is
    /// offered. The arguments were checked against it.
    pub parameters: String,
    /// The arguments exactly as the Provider sent them.
    pub arguments: ToolPayload,
    pub authorization: AuthorizationBasis,
}

impl ToolAdmission {
    /// Whether the handler registered under this version can start an OS
    /// process. The caller of a tool decision passes this as `spawns`.
    pub fn spawns(&self) -> bool {
        matches!(
            self.handler_version.as_str(),
            "workspace_run_command-1" | "workspace_run_command-2"
        )
    }
}

/// A call the Provider requested, as OCG identified it, with OCG's decision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewToolEvidence {
    pub call: ToolCallRef,
    /// The id the Provider gave the call; unique within the Job.
    pub tool_call_id: String,
    /// The tool the call named.
    pub name: String,
    pub decision: ToolDecision,
    /// Whether the handler can start an OS process; see
    /// [`ToolAdmission::spawns`]. It decides whether a crash leaves the call's
    /// slot quarantined.
    pub spawns: bool,
}

/// Why a completed handler's result was not sent to the Provider.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WithheldReason {
    TooLarge,
    Unserializable,
}

/// Why an admitted call's outcome cannot be established.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UncertainCause {
    /// The handler was stopped at its time limit.
    TimedOut,
    /// The execution was cancelled while the handler ran.
    Cancelled,
    /// The handler reported a failure without evidence that its effect
    /// did not happen.
    HandlerReported,
}

/// What is known of an admitted call's execution. Only `Uncertain` leaves
/// the effect open; a missing outcome means the same.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ToolOutcome {
    /// The handler returned this result, which is the one sent on.
    Completed { result: ToolPayload },
    /// The handler returned, so its effect happened, but its result was not
    /// sent on.
    CompletedWithheld { reason: WithheldReason },
    /// The handler established that its effect did not happen.
    NotApplied { message: String },
    /// OCG never invoked the handler, for `reason`, after the admission.
    NotInvoked { reason: NotInvokedReason },
    /// The handler may have run and may have had its effect.
    Uncertain {
        cause: UncertainCause,
        message: Option<String>,
    },
}

/// Why OCG never invoked an admitted call's handler.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NotInvokedReason {
    /// Its final authorization check refused the call.
    Refused(ToolCallRefusal),
    /// The execution was cancelled before the handler's invocation.
    Cancelled,
}

/// The kind of a [`ToolOutcome`], without what it carries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ToolOutcomeKind {
    Completed,
    CompletedWithheld,
    NotApplied,
    NotInvoked,
    Uncertain,
}

impl ToolOutcome {
    pub fn kind(&self) -> ToolOutcomeKind {
        match self {
            Self::Completed { .. } => ToolOutcomeKind::Completed,
            Self::CompletedWithheld { .. } => ToolOutcomeKind::CompletedWithheld,
            Self::NotApplied { .. } => ToolOutcomeKind::NotApplied,
            Self::NotInvoked { .. } => ToolOutcomeKind::NotInvoked,
            Self::Uncertain { .. } => ToolOutcomeKind::Uncertain,
        }
    }

    /// Whether the outcome settles whether the effect happened.
    pub fn is_definite(&self) -> bool {
        !matches!(self, Self::Uncertain { .. })
    }
}

/// What a reconciliation established about an uncertain execution.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EffectConclusion {
    Applied,
    NotApplied,
}

/// A verified conclusion about an admitted call whose outcome was left
/// uncertain. Only trusted OCG code that verified it against the external
/// system may record one; OCG has none yet, so nothing outside the crate
/// can record one and no uncertainty is resolved in production.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToolReconciliation {
    pub conclusion: EffectConclusion,
    /// The trusted reconciler that verified it.
    pub reconciler: String,
    /// What it was verified against, such as the external system's record.
    pub evidence: String,
    pub explanation: String,
}

/// A fact and when its transaction recorded it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Recorded<T> {
    pub fact: T,
    pub at: Timestamp,
}

/// Everything recorded about one call a Provider requested. Each part is
/// written once and never changed or removed, whatever the Job's outcome.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToolEvidenceRecord {
    pub call: ToolCallRef,
    pub tool_call_id: String,
    pub name: String,
    /// Whether the admitted handler can start an OS process, as the caller of
    /// the decision stated it. `false` for a refusal.
    pub spawns: bool,
    pub decision: Recorded<ToolDecision>,
    pub outcome: Option<Recorded<ToolOutcome>>,
    pub reconciliation: Option<Recorded<ToolReconciliation>>,
}

impl ToolEvidenceRecord {
    pub fn admission(&self) -> Option<&ToolAdmission> {
        match &self.decision.fact {
            ToolDecision::Admitted(admission) => Some(admission),
            ToolDecision::Refused(_) => None,
        }
    }

    /// Whether the call was admitted and whether its effect happened is
    /// still open: no definite outcome and no reconciliation.
    pub fn is_unresolved(&self) -> bool {
        self.admission().is_some()
            && self.reconciliation.is_none()
            && !self
                .outcome
                .as_ref()
                .is_some_and(|outcome| outcome.fact.is_definite())
    }

    /// Whether the call's effect is known to have happened.
    pub fn is_applied(&self) -> bool {
        let completed = self.outcome.as_ref().is_some_and(|outcome| {
            matches!(
                outcome.fact,
                ToolOutcome::Completed { .. } | ToolOutcome::CompletedWithheld { .. }
            )
        });
        let reconciled = self.reconciliation.as_ref().is_some_and(|reconciliation| {
            reconciliation.fact.conclusion == EffectConclusion::Applied
        });
        self.admission().is_some() && (completed || reconciled)
    }
}
