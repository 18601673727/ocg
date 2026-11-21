use crate::db::derived::JobActivity;
use crate::db::entities::identity::{
    AttachmentId, ComposerContextId, ConfigId, JobId, JobOutputId, ProfileId, ProjectId,
};
use crate::db::entities::{
    Actor, CompactionPolicy, ConfigRef, DecisionCapability, FrozenDecisionConfig,
    FrozenExecutionConfig, JobLimits, JobUsage, NativeDecisionConfig, Origin, Recurrence,
    RequestRef, Timestamp,
};
use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IntermediateOutputKind {
    Progress,
    Evaluation,
    Reasoning,
    Decision,
    Summarization,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, ts_rs::TS)]
#[serde(rename_all = "snake_case")]
pub enum OutputKind {
    Progress,
    Evaluation,
    Reasoning,
    Result,
    Decision,
    Summarization,
}

impl From<IntermediateOutputKind> for OutputKind {
    fn from(kind: IntermediateOutputKind) -> Self {
        match kind {
            IntermediateOutputKind::Progress => Self::Progress,
            IntermediateOutputKind::Evaluation => Self::Evaluation,
            IntermediateOutputKind::Reasoning => Self::Reasoning,
            IntermediateOutputKind::Decision => Self::Decision,
            IntermediateOutputKind::Summarization => Self::Summarization,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DependencyCondition {
    Success,
    Settled,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DependencyState {
    Waiting,
    Satisfied,
    Impossible,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExecutionConfiguration {
    pub reasoning_effort: Option<super::ReasoningEffort>,
    pub agent_profile: Option<ProfileId>,
    pub llm_profile: ProfileId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MutableJobInput {
    pub priority: JobQueuePriority,
    pub content: MutableJobContent,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MutableJobContent {
    Generative {
        text: String,
        attachments: Vec<AttachmentId>,
        configuration: Option<ExecutionConfiguration>,
    },
    Decision {
        request: DecisionRequest,
        profile: ConfigId,
        references: Vec<ConfigRef>,
    },
}

impl MutableJobInput {
    pub fn size_hint(&self) -> usize {
        match &self.content {
            MutableJobContent::Generative {
                text, attachments, ..
            } => text
                .len()
                .saturating_add(attachments.len().saturating_mul(16)),
            MutableJobContent::Decision { request, .. } => {
                crate::db::serialized_len(request).unwrap_or(usize::MAX)
            }
        }
    }

    pub fn text(&self) -> &str {
        match &self.content {
            MutableJobContent::Generative { text, .. } => text,
            MutableJobContent::Decision { .. } => "",
        }
    }

    pub fn attachments(&self) -> &[AttachmentId] {
        match &self.content {
            MutableJobContent::Generative { attachments, .. } => attachments,
            MutableJobContent::Decision { .. } => &[],
        }
    }

    pub fn configuration(&self) -> Option<ExecutionConfiguration> {
        match &self.content {
            MutableJobContent::Generative { configuration, .. } => *configuration,
            MutableJobContent::Decision { .. } => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MutableContextSelection {
    FullHistory,
    AdoptedSummary {
        producer: JobId,
        source: super::ContentDigest,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JobQueuePriority {
    Foreground,
    Automatic,
    Speculative,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MutableJobPhase {
    Unsubmitted,
    Queued,
    Executing,
}

#[derive(Clone, Debug, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum MutableFrozenConfiguration {
    Generative(FrozenExecutionConfig),
    Decision(NativeDecisionConfig),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobRetry {
    pub original: JobId,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MutableJob {
    pub id: JobId,
    pub parent_id: JobId,
    pub previous: Option<JobId>,
    pub retry: Option<JobRetry>,
    pub submitted_by: Actor,
    pub composer_context: ComposerContextId,
    pub created_at: Timestamp,
    pub submitted_at: Option<Timestamp>,
    pub revision: u64,
    pub phase: MutableJobPhase,
    pub input: MutableJobInput,
    pub context_selection: MutableContextSelection,
    pub summary_source: Option<super::ContentDigest>,
    pub frozen: Option<MutableFrozenConfiguration>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MutableJobRecord {
    pub job: MutableJob,
    pub outcome: Option<OutcomeRecord>,
    pub project: Option<ProjectId>,
    pub event_seq: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct JobOutput {
    pub part: Option<(u32, u32, u32)>,
    pub kind: IntermediateOutputKind,
    pub summary: Option<String>,
    pub code: Option<String>,
    pub goal_reached: Option<bool>,
    pub value: Option<serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FinalResult {
    pub summary: Option<String>,
    pub code: Option<String>,
    pub goal_reached: Option<bool>,
    pub value: Option<serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum JobFinalization {
    Completed(FinalResult),
    Failed(FinalResult),
    Cancelled(FinalResult),
}

impl JobFinalization {
    pub(crate) fn result(&self) -> &FinalResult {
        match self {
            Self::Completed(result) | Self::Failed(result) | Self::Cancelled(result) => result,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendedOutput {
    pub output: JobOutputId,
    pub seq: i64,
}

pub const MAX_DECISION_REQUEST_BYTES: usize = 1024 * 1024;
pub const MAX_DECISION_OPTIONS: usize = 256;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChoiceOption {
    pub id: String,
    pub description: String,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScoreLevel {
    pub value: i32,
    pub description: String,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum DecisionQuestion {
    Choice {
        prompt: String,
        options: Vec<ChoiceOption>,
    },
    BinaryProbability {
        prompt: String,
    },
    OrdinalScore {
        prompt: String,
        levels: Vec<ScoreLevel>,
    },
}

impl DecisionQuestion {
    pub fn prompt(&self) -> &str {
        match self {
            Self::Choice { prompt, .. }
            | Self::BinaryProbability { prompt }
            | Self::OrdinalScore { prompt, .. } => prompt,
        }
    }

    pub fn capability(&self) -> DecisionCapability {
        match self {
            Self::Choice { .. } => DecisionCapability::Choice,
            Self::BinaryProbability { .. } => DecisionCapability::BinaryProbability,
            Self::OrdinalScore { .. } => DecisionCapability::OrdinalScore,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionRequest {
    pub state: serde_json::Value,
    pub question: DecisionQuestion,
}

impl DecisionRequest {
    pub fn validate(
        &self,
        capabilities: &BTreeSet<DecisionCapability>,
    ) -> Result<(), &'static str> {
        if !capabilities.contains(&self.question.capability()) {
            return Err("decision primitive is not supported by the model");
        }
        if crate::db::serialized_len(self).map_err(|_| "invalid decision request")?
            > MAX_DECISION_REQUEST_BYTES
        {
            return Err("decision request exceeds 1 MiB");
        }
        let prompt = match &self.question {
            DecisionQuestion::Choice { prompt, options } => {
                if !(2..=MAX_DECISION_OPTIONS).contains(&options.len()) {
                    return Err("choice requires 2 to 256 options");
                }
                let mut ids = BTreeSet::new();
                for option in options {
                    if option.id.trim().is_empty()
                        || option.id.len() > 128
                        || !ids.insert(&option.id)
                    {
                        return Err(
                            "choice option IDs must be nonempty, unique and at most 128 bytes",
                        );
                    }
                    if option.description.trim().is_empty() {
                        return Err("choice options require descriptions");
                    }
                }
                prompt
            }
            DecisionQuestion::OrdinalScore { prompt, levels } => {
                if !(2..=MAX_DECISION_OPTIONS).contains(&levels.len())
                    || levels.windows(2).any(|pair| pair[0].value >= pair[1].value)
                    || levels
                        .iter()
                        .any(|level| level.description.trim().is_empty())
                {
                    return Err("score requires 2 to 256 increasing, described levels");
                }
                prompt
            }
            DecisionQuestion::BinaryProbability { prompt } => prompt,
        };
        if prompt.trim().is_empty() {
            return Err("decision prompt must not be empty");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandGrantInput {
    pub command: super::ProjectCommand,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionConfidence {
    pub value: f64,
    pub semantics: String,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum DecisionAnswer {
    Choice {
        selected: String,
        distribution: Option<std::collections::BTreeMap<String, f64>>,
        confidence: Option<DecisionConfidence>,
    },
    BinaryProbability {
        true_probability: f64,
    },
    OrdinalScore {
        expectation: f64,
        distribution: Option<Vec<f64>>,
        confidence: Option<DecisionConfidence>,
    },
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum DecisionOutcome {
    Answer { answer: DecisionAnswer },
    Uncertain { reason: String },
    Unusable { reason: String },
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionResult {
    pub outcome: DecisionOutcome,
    pub requested_model: String,
    pub reported_model: Option<String>,
    pub model_version: Option<String>,
    pub model: ConfigRef,
    pub agent: Option<ConfigRef>,
    pub connection: ConfigRef,
    pub request: RequestRef,
}

impl DecisionResult {
    pub fn validate(
        &self,
        input: &DecisionRequest,
        frozen: &FrozenDecisionConfig,
        request: RequestRef,
    ) -> Result<(), &'static str> {
        input.validate(&frozen.capabilities())?;
        if self.request != request
            || self.model != frozen.model_ref()
            || self.agent != frozen.agent_ref()
            || self.connection != frozen.connection_ref()
            || self.requested_model != frozen.model_name()
        {
            return Err("decision result provenance does not match the frozen request");
        }
        if [&self.reported_model, &self.model_version]
            .into_iter()
            .any(|identity| {
                identity
                    .as_ref()
                    .is_some_and(|value| value.is_empty() || value.len() > 512)
            })
        {
            return Err("reported model identities must be nonempty and at most 512 bytes");
        }
        let answer = match &self.outcome {
            DecisionOutcome::Answer { answer } => answer,
            DecisionOutcome::Uncertain { reason } | DecisionOutcome::Unusable { reason } => {
                return if reason.trim().is_empty() || reason.len() > 4096 {
                    Err("unusable predictions require a bounded reason")
                } else {
                    Ok(())
                };
            }
        };
        let confidence = match (&input.question, answer) {
            (
                DecisionQuestion::Choice { options, .. },
                DecisionAnswer::Choice {
                    selected,
                    distribution,
                    confidence,
                },
            ) => {
                if !options.iter().any(|option| option.id == *selected) {
                    return Err("unknown choice selection");
                }
                if let Some(distribution) = distribution {
                    if distribution.len() != options.len()
                        || options
                            .iter()
                            .any(|option| !distribution.contains_key(&option.id))
                    {
                        return Err(
                            "choice distribution does not cover exactly the requested options",
                        );
                    }
                    validate_distribution(distribution.values().copied())?;
                }
                confidence
            }
            (
                DecisionQuestion::BinaryProbability { .. },
                DecisionAnswer::BinaryProbability { true_probability },
            ) => {
                if !probability(*true_probability) {
                    return Err("invalid binary probability");
                }
                return Ok(());
            }
            (
                DecisionQuestion::OrdinalScore { levels, .. },
                DecisionAnswer::OrdinalScore {
                    expectation,
                    distribution,
                    confidence,
                },
            ) => {
                let low = f64::from(levels[0].value);
                let high = f64::from(levels[levels.len() - 1].value);
                if !expectation.is_finite() || !(low..=high).contains(expectation) {
                    return Err("invalid score expectation");
                }
                if let Some(distribution) = distribution {
                    if distribution.len() != levels.len() {
                        return Err("score distribution does not cover requested levels");
                    }
                    validate_distribution(distribution.iter().copied())?;
                    let derived: f64 = levels
                        .iter()
                        .zip(distribution)
                        .map(|(level, p)| f64::from(level.value) * p)
                        .sum();
                    if (derived - expectation).abs() > 1e-6 * (1.0 + low.abs().max(high.abs())) {
                        return Err("score expectation disagrees with distribution");
                    }
                }
                confidence
            }
            _ => return Err("decision result primitive differs from request"),
        };
        if confidence.as_ref().is_some_and(|confidence| {
            !probability(confidence.value)
                || confidence.semantics.trim().is_empty()
                || confidence.semantics.len() > 256
        }) {
            return Err("confidence requires a finite value and bounded semantic identifier");
        }
        Ok(())
    }
}

fn probability(value: f64) -> bool {
    value.is_finite() && (0.0..=1.0).contains(&value)
}

fn validate_distribution(values: impl Iterator<Item = f64>) -> Result<(), &'static str> {
    let mut sum = 0.0;
    for value in values {
        if !probability(value) {
            return Err("invalid distribution probability");
        }
        sum += value;
    }
    if (sum - 1.0).abs() > 1e-6 {
        return Err("distribution must sum to one within 1e-6");
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum OutcomeKind {
    Completed = 1,
    Failed = 2,
    Cancelled = 3,
    Unknown = 4,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OutcomeCause {
    Code(u8),
    Text(String),
    Abandoned,
    DependencyImpossible,
    Stopped { record: u32 },
    Closed,
    Uncertain(UncertaintyReason),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum UncertaintyReason {
    LocalExecutionDropped,
    RequestPossiblySent,
    ConnectionInterrupted,
    ResponseIncompleteOrInvalid,
    WorkerAckPendingOrAmbiguous,
    StopRequestedWithoutConfirmedTermination,
    Unclassified,
    HostShutdown,
    DeadlineExceeded,
}

impl UncertaintyReason {
    pub const fn code(self) -> u8 {
        match self {
            Self::LocalExecutionDropped => 1,
            Self::RequestPossiblySent => 2,
            Self::ConnectionInterrupted => 3,
            Self::ResponseIncompleteOrInvalid => 4,
            Self::WorkerAckPendingOrAmbiguous => 5,
            Self::StopRequestedWithoutConfirmedTermination => 6,
            Self::Unclassified => 7,
            Self::HostShutdown => 8,
            Self::DeadlineExceeded => 9,
        }
    }

    pub const fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::LocalExecutionDropped),
            2 => Some(Self::RequestPossiblySent),
            3 => Some(Self::ConnectionInterrupted),
            4 => Some(Self::ResponseIncompleteOrInvalid),
            5 => Some(Self::WorkerAckPendingOrAmbiguous),
            6 => Some(Self::StopRequestedWithoutConfirmedTermination),
            7 => Some(Self::Unclassified),
            8 => Some(Self::HostShutdown),
            9 => Some(Self::DeadlineExceeded),
            _ => None,
        }
    }

    pub const fn description(self) -> &'static str {
        match self {
            Self::LocalExecutionDropped => {
                "The local execution task was lost before it returned; whether the Provider received the request is unknown."
            }
            Self::RequestPossiblySent => {
                "The request may have reached the Provider and no response was seen; the Provider may still be working or may have charged."
            }
            Self::ConnectionInterrupted => {
                "The connection failed after the request may have been delivered; the Provider may still be working or may have charged."
            }
            Self::ResponseIncompleteOrInvalid => {
                "A response arrived but was incomplete or not provably a refusal; the Provider may have done work or charged."
            }
            Self::WorkerAckPendingOrAmbiguous => {
                "A worker acknowledgement is pending or ambiguous; whether the work stopped is unknown."
            }
            Self::StopRequestedWithoutConfirmedTermination => {
                "A stop was requested locally but termination was never confirmed."
            }
            Self::Unclassified => "The reason the outcome is uncertain could not be established.",
            Self::HostShutdown => {
                "The host shut down after external work may have started. Termination was not confirmed."
            }
            Self::DeadlineExceeded => {
                "The dispatch deadline expired after external work may have started. Termination was not confirmed."
            }
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LocalExecutionDropped => "local_execution_dropped",
            Self::RequestPossiblySent => "request_possibly_sent",
            Self::ConnectionInterrupted => "connection_interrupted",
            Self::ResponseIncompleteOrInvalid => "response_incomplete_or_invalid",
            Self::WorkerAckPendingOrAmbiguous => "worker_ack_pending_or_ambiguous",
            Self::StopRequestedWithoutConfirmedTermination => {
                "stop_requested_without_confirmed_termination"
            }
            Self::Unclassified => "unclassified",
            Self::HostShutdown => "host_shutdown",
            Self::DeadlineExceeded => "deadline_exceeded",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutcomeRecord {
    pub kind: OutcomeKind,
    pub at: Timestamp,
    pub cause: Option<OutcomeCause>,
}

#[derive(Clone, Debug, PartialEq, Eq, ts_rs::TS)]
pub struct JobOutputRecord {
    #[ts(skip)]
    pub part: Option<(u32, u32, u32)>,
    #[ts(skip)]
    pub span: Option<(u32, u32)>,
    #[ts(skip)]
    pub answer_request: Option<u32>,
    pub id: JobOutputId,
    pub job: JobId,
    pub seq: i64,
    pub kind: OutputKind,
    pub summary: Option<String>,
    pub code: Option<String>,
    pub goal_reached: Option<bool>,
    pub value: Option<serde_json::Value>,
    pub created_at: Timestamp,
}

impl JobOutputRecord {
    pub fn model_text(&self) -> Option<&str> {
        self.value.as_ref()?.get("text")?.as_str()
    }

    pub fn display_text(&self) -> Option<String> {
        match (
            self.model_text().filter(|text| !text.is_empty()),
            self.summary.as_deref().filter(|text| !text.is_empty()),
        ) {
            (Some(text), Some(summary)) => Some(format!("{text}\n{summary}")),
            (Some(text), None) | (None, Some(text)) => Some(text.to_owned()),
            (None, None) => None,
        }
    }
}

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(
    tag = "name",
    content = "arguments",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum JobToolCall {
    JobPlan {
        steps: Vec<JobPlanStep>,
    },
    JobLimits {
        conversation: Option<String>,
        max_depth: Option<u8>,
        max_auto_turns: Option<u8>,
        max_iterations: Option<u32>,
    },
    JobAgents {
        after: Option<String>,
        limit: Option<u32>,
    },
    JobWorkers {
        after: Option<String>,
        limit: Option<u32>,
    },
    JobAssign {
        conversation: String,
        objective: String,
    },
    JobDispatch {
        project: Option<String>,
        #[serde(default)]
        projects: Vec<String>,
        objective: String,
        #[serde(default)]
        keep_open: bool,
        every_seconds: Option<u32>,
        #[serde(default)]
        depends_on: Vec<JobDependency>,
    },
    JobMessage {
        conversation: String,
        text: String,
    },
    JobStop {
        conversation: String,
    },
    JobClose {
        conversation: String,
    },
    JobHold {
        conversation: String,
    },
    JobUnhold {
        conversation: String,
    },
    JobRetry {
        turn: String,
    },
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DispatchSelection {
    pub agent: Option<String>,
    pub model: String,
    pub projects: Vec<String>,
}

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobDependency {
    pub conversation: String,
    pub condition: String,
}

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobPlanStep {
    pub title: String,
    pub objective: String,
    pub depends_on: Vec<usize>,
}

impl JobToolCall {
    pub(crate) fn parse(name: &str, arguments: &str) -> Option<Self> {
        let arguments: serde_json::Value = serde_json::from_str(arguments).ok()?;
        serde_json::from_value(serde_json::json!({"name": name, "arguments": arguments})).ok()
    }

    pub(crate) fn is_job_tool(name: &str) -> bool {
        matches!(
            name,
            "job_plan"
                | "job_limits"
                | "job_agents"
                | "job_workers"
                | "job_assign"
                | "job_dispatch"
                | "job_message"
                | "job_stop"
                | "job_close"
                | "job_hold"
                | "job_unhold"
                | "job_retry"
        )
    }

    pub(crate) fn handler_version(&self) -> String {
        let version = match self {
            Self::JobPlan { .. } => 6,
            Self::JobDispatch { .. } | Self::JobAssign { .. } => 2,
            Self::JobLimits { .. }
            | Self::JobAgents { .. }
            | Self::JobWorkers { .. }
            | Self::JobMessage { .. }
            | Self::JobStop { .. }
            | Self::JobClose { .. }
            | Self::JobHold { .. }
            | Self::JobUnhold { .. }
            | Self::JobRetry { .. } => 1,
        };
        format!("{}-{version}", self.name())
    }

    pub(crate) fn name(&self) -> &'static str {
        match self {
            Self::JobPlan { .. } => "job_plan",
            Self::JobLimits { .. } => "job_limits",
            Self::JobAgents { .. } => "job_agents",
            Self::JobWorkers { .. } => "job_workers",
            Self::JobAssign { .. } => "job_assign",
            Self::JobDispatch { .. } => "job_dispatch",
            Self::JobMessage { .. } => "job_message",
            Self::JobStop { .. } => "job_stop",
            Self::JobClose { .. } => "job_close",
            Self::JobHold { .. } => "job_hold",
            Self::JobUnhold { .. } => "job_unhold",
            Self::JobRetry { .. } => "job_retry",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct JobSpec {
    pub(crate) id: JobId,
    pub(crate) created_at: Timestamp,
    pub(crate) parent_id: Option<JobId>,
    pub(crate) agent: Option<ConfigId>,
    pub(crate) project: Option<ProjectId>,
    pub(crate) origin: Origin,
    pub(crate) keep_open: bool,
}

impl JobSpec {
    pub(crate) fn is_chunk_group(&self) -> bool {
        self.parent_id.is_some() && self.origin == Origin::System && !self.keep_open
    }

    pub(crate) fn is_standalone_decision_group(&self) -> bool {
        self.parent_id.is_none() && self.origin == Origin::System && !self.keep_open
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct JobState {
    pub(crate) job_id: JobId,
    pub(crate) outcome: Option<OutcomeRecord>,
}

impl JobState {
    pub(crate) fn new(job_id: JobId) -> Self {
        Self {
            job_id,
            ..Self::default()
        }
    }

    pub(crate) fn confirmed_terminal(&self) -> bool {
        self.outcome
            .as_ref()
            .is_some_and(|outcome| match outcome.kind {
                OutcomeKind::Completed | OutcomeKind::Failed | OutcomeKind::Cancelled => true,
                OutcomeKind::Unknown => false,
            })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct JobLifecycle {
    pub(crate) limits: Option<JobLimits>,
    pub(crate) title: Option<String>,
    pub(crate) hidden_at: Option<Timestamp>,
    pub(crate) on_hold: Option<u32>,
    pub(crate) loop_count: u8,
    pub(crate) closing: Option<JobClosing>,
    pub(crate) compaction_policy: Option<CompactionPolicy>,
    pub(crate) compaction_revision: u64,
    pub(crate) after: Vec<(JobId, DependencyCondition)>,
    pub(crate) reported: Option<JobId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct JobClosing {
    pub(crate) at: Timestamp,
    pub(crate) by: Actor,
    pub(crate) running: bool,
}

pub(crate) struct NativeJobItem {
    pub record: crate::db::entities::MutableJobRecord,
    pub allocation: Option<crate::db::entities::PoolAllocation>,
    pub updated_at_us: i64,
}

pub(crate) struct JobSearchPage {
    pub items: Vec<NativeJobItem>,
    pub next_before: Option<JobId>,
    pub has_more: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub enum TreeFacts {
    Conversation {
        title: String,
        state: &'static str,
        on_hold: Option<Actor>,
        origin: &'static str,
        keep_open: bool,
        active_turn: Option<(JobId, ConfigId, String)>,
        recurrence: Option<Recurrence>,
        depends_on: Vec<(JobId, DependencyCondition, DependencyState)>,
    },
    Turn {
        title: String,
        state: &'static str,
        conversation: Option<JobId>,
        previous: Option<JobId>,
        model: (ConfigId, String),
        depends_on: Vec<(JobId, DependencyCondition, DependencyState)>,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct TreeNode {
    pub id: JobId,
    pub parent_id: Option<JobId>,
    pub created_at: Timestamp,
    pub facts: TreeFacts,
    pub agent: Option<(ConfigId, Option<String>)>,
    pub children: Option<Vec<usize>>,
    pub has_more_children: bool,
    pub next_after: Option<JobId>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct JobTree {
    pub ancestors: Vec<JobId>,
    pub nodes: Vec<TreeNode>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CursorPage<T, C> {
    pub records: Vec<T>,
    pub next: Option<C>,
}

pub(crate) struct MutableJobDetails {
    pub record: crate::db::entities::MutableJobRecord,
    pub allocation: Option<crate::db::entities::PoolAllocation>,
    pub result: Option<JobOutputRecord>,
    pub usage: Option<JobUsage>,
    pub activity: JobActivity,
    pub unknown_reason: Option<String>,
    pub configuration: Option<MutableConfiguration>,
    pub unsupported_decision_history: Option<Result<bool, &'static str>>,
}

pub(crate) struct MutableConfiguration {
    pub selection: ExecutionConfiguration,
    pub model: String,
    pub context_window: Option<u32>,
}
