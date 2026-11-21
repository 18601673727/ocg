use crate::db::entities::identity::{ConfigId, JobId, ProjectId};
use crate::db::entities::{
    AllocationToken, ConfigRef, ContentDigest, FrozenDecisionConfig, FrozenLlmConfig,
    JobOutputRecord, MutableJobDetails, Timestamp, ToolCallRef, ToolEvidenceRecord,
};

pub const MAX_JOB_DEPTH: u8 = 16;

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobLimits {
    pub max_depth: u8,
    pub max_auto_turns: u8,
    pub max_iterations: u32,
}

impl Default for JobLimits {
    fn default() -> Self {
        Self {
            max_depth: 3,
            max_auto_turns: 8,
            max_iterations: 100,
        }
    }
}

impl JobLimits {
    pub const CEILING: Self = Self {
        max_depth: MAX_JOB_DEPTH,
        max_auto_turns: u8::MAX,
        max_iterations: 10_000,
    };

    pub fn intersect(self, other: Self) -> Self {
        Self {
            max_depth: self.max_depth.min(other.max_depth),
            max_auto_turns: self.max_auto_turns.min(other.max_auto_turns),
            max_iterations: self.max_iterations.min(other.max_iterations),
        }
    }

    pub fn within(self, ceiling: Self) -> bool {
        self.intersect(ceiling) == self
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompactionPreset {
    Conservative,
    Balanced,
    Aggressive,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CustomPolicy {
    pub trigger_tokens: u32,
    pub target_tokens: u32,
    pub reserved_output_tokens: u32,
    pub minimum_source_tokens: u32,
    pub minimum_savings_tokens: u32,
    pub preserve_recent_turns: u32,
    pub cooldown_seconds: u32,
}

impl CustomPolicy {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.trigger_tokens == 0
            || self.target_tokens == 0
            || self.target_tokens >= self.trigger_tokens
            || self.reserved_output_tokens == 0
            || self.minimum_source_tokens == 0
            || self.minimum_savings_tokens == 0
            || self.minimum_savings_tokens > self.trigger_tokens - self.target_tokens
            || self.preserve_recent_turns == 0
            || self.cooldown_seconds == 0
        {
            return Err(
                "custom policy requires positive token budgets, savings, recent turns and cooldown; target must be below trigger",
            );
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CompactionMode {
    Off,
    Auto { preset: CompactionPreset },
    Custom { policy: CustomPolicy },
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PredictiveCompaction {
    pub decision_agent: ConfigId,
    pub minimum_interval_seconds: u32,
    pub max_preparations: u32,
    pub max_prediction_tokens: u32,
    pub max_preparation_tokens: u32,
    #[serde(default)]
    pub mode: PredictionMode,
    #[serde(default)]
    pub acceptance: Option<PredictionAcceptance>,
    #[serde(default)]
    pub max_preparations_per_hour: u32,
    #[serde(default)]
    pub max_tokens_per_hour: u32,
    #[serde(default)]
    pub max_requests_per_hour: u32,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PredictionMode {
    #[default]
    Shadow,
    Prepare,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PredictionAcceptance {
    pub model: ConfigRef,
    pub reported_model: String,
    pub confidence_semantics: String,
    pub minimum_probability_millionths: u32,
    pub minimum_confidence_millionths: u32,
}

impl PredictionAcceptance {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.model.edit_seq == 0
            || self.reported_model.len() > 512
            || self.confidence_semantics.trim().is_empty()
            || self.confidence_semantics.len() > 256
            || !(1..=1_000_000).contains(&self.minimum_probability_millionths)
            || !(1..=1_000_000).contains(&self.minimum_confidence_millionths)
        {
            return Err(
                "prediction acceptance requires an exact model version, explicit confidence semantics and positive thresholds at most one million",
            );
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompactionPolicy {
    pub mode: CompactionMode,
    pub summarization_model: Option<ConfigId>,
    pub predictive: Option<PredictiveCompaction>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GlobalCompactionSettings {
    pub revision: u64,
    pub prediction_enabled: bool,
    pub policy: CompactionPolicy,
}

impl Default for GlobalCompactionSettings {
    fn default() -> Self {
        Self {
            revision: 0,
            prediction_enabled: true,
            policy: CompactionPolicy {
                mode: CompactionMode::Auto {
                    preset: CompactionPreset::Balanced,
                },
                summarization_model: None,
                predictive: None,
            },
        }
    }
}

impl PredictiveCompaction {
    pub fn automatic(decision_agent: ConfigId, model: ConfigRef) -> Self {
        Self {
            decision_agent,
            minimum_interval_seconds: 60,
            max_preparations: 1,
            max_prediction_tokens: 8192,
            max_preparation_tokens: 16384,
            mode: PredictionMode::Prepare,
            acceptance: Some(PredictionAcceptance {
                model,
                reported_model: String::new(),
                confidence_semantics: "provider.typesafe.choice.confidence.v1".to_owned(),
                minimum_probability_millionths: 800_000,
                minimum_confidence_millionths: 800_000,
            }),
            max_preparations_per_hour: 6,
            max_requests_per_hour: 24,
            max_tokens_per_hour: 98_304,
        }
    }
}

impl Default for CompactionPolicy {
    fn default() -> Self {
        Self {
            mode: CompactionMode::Off,
            summarization_model: None,
            predictive: None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResolvedCompaction {
    pub mode: &'static str,
    pub trigger: Option<u32>,
    pub target: u32,
    pub headroom: u32,
    pub minimum_source: u32,
    pub minimum_savings: u32,
    pub preserve_recent_turns: u32,
    pub cooldown_seconds: u32,
    pub summarization_model: Option<ConfigId>,
    pub uncalibrated: bool,
}

impl CompactionPolicy {
    pub(in crate::db) fn bind_prediction(&mut self, decision: &FrozenDecisionConfig) {
        let mut binding = self.predictive.take().unwrap_or_else(|| {
            PredictiveCompaction::automatic(decision.profile_ref().id, decision.model_ref())
        });
        binding.decision_agent = decision.profile_ref().id;
        binding.mode = PredictionMode::Prepare;
        let acceptance = binding.acceptance.get_or_insert(PredictionAcceptance {
            model: decision.model_ref(),
            reported_model: String::new(),
            confidence_semantics: decision.confidence_semantics().into(),
            minimum_probability_millionths: 800_000,
            minimum_confidence_millionths: 800_000,
        });
        acceptance.model = decision.model_ref();
        acceptance.reported_model.clear();
        acceptance.confidence_semantics = decision.confidence_semantics().into();
        self.predictive = Some(binding);
    }

    pub fn recipe(
        &self,
        window: Option<u32>,
        output_reserve: u32,
        safety: u32,
        headroom: u32,
    ) -> Result<ResolvedCompaction, &'static str> {
        let disabled = ResolvedCompaction {
            mode: "disabled",
            trigger: None,
            target: 0,
            headroom,
            minimum_source: 0,
            minimum_savings: 0,
            preserve_recent_turns: 0,
            cooldown_seconds: 0,
            summarization_model: self.summarization_model,
            uncalibrated: true,
        };
        if matches!(self.mode, CompactionMode::Off) {
            return Ok(disabled);
        }
        let Some(window) = window else {
            return Err("automatic compaction requires a known context window");
        };
        let output_reserve = match self.mode {
            CompactionMode::Custom { policy } => output_reserve.max(policy.reserved_output_tokens),
            CompactionMode::Off | CompactionMode::Auto { .. } => output_reserve,
        };
        let budget = window
            .checked_sub(output_reserve)
            .and_then(|value| value.checked_sub(safety))
            .ok_or("context window cannot hold the output reserve and safety allowance")?;
        let headroom = headroom.max(output_reserve);
        let (trigger_gap, target_gap, preserve, cooldown) = match self.mode {
            CompactionMode::Off => return Ok(disabled),
            CompactionMode::Auto {
                preset: CompactionPreset::Conservative,
            } => (1, 2, 4, 600),
            CompactionMode::Auto {
                preset: CompactionPreset::Balanced,
            } => (2, 3, 2, 300),
            CompactionMode::Auto {
                preset: CompactionPreset::Aggressive,
            } => (3, 4, 1, 120),
            CompactionMode::Custom { policy } => {
                policy.validate()?;
                if policy.trigger_tokens > budget || policy.target_tokens > budget {
                    return Err("custom compaction budgets exceed the effective model window");
                }
                return Ok(ResolvedCompaction {
                    mode: "custom",
                    trigger: Some(policy.trigger_tokens),
                    target: policy.target_tokens,
                    headroom,
                    minimum_source: policy.minimum_source_tokens,
                    minimum_savings: policy.minimum_savings_tokens,
                    preserve_recent_turns: policy.preserve_recent_turns,
                    cooldown_seconds: policy.cooldown_seconds,
                    summarization_model: self.summarization_model,
                    uncalibrated: true,
                });
            }
        };
        let trigger = budget
            .checked_sub(headroom.saturating_mul(trigger_gap))
            .ok_or("preset trigger does not fit the known window")?;
        let target = budget
            .checked_sub(headroom.saturating_mul(target_gap))
            .ok_or("preset target does not fit the known window")?;
        if target >= trigger {
            return Err("preset target is not below its trigger");
        }
        Ok(ResolvedCompaction {
            mode: match self.mode {
                CompactionMode::Auto {
                    preset: CompactionPreset::Conservative,
                } => "conservative",
                CompactionMode::Auto {
                    preset: CompactionPreset::Balanced,
                } => "balanced",
                CompactionMode::Auto {
                    preset: CompactionPreset::Aggressive,
                } => "aggressive",
                CompactionMode::Off | CompactionMode::Custom { .. } => "disabled",
            },
            trigger: Some(trigger),
            target,
            headroom,
            minimum_source: headroom.saturating_mul(2),
            minimum_savings: headroom,
            preserve_recent_turns: preserve,
            cooldown_seconds: cooldown,
            summarization_model: self.summarization_model,
            uncalibrated: true,
        })
    }

    pub fn validate(&self) -> Result<(), &'static str> {
        if let CompactionMode::Custom { policy } = self.mode {
            policy.validate()?;
        }
        if self.predictive.as_ref().is_some_and(|p| {
            p.minimum_interval_seconds == 0
                || p.max_preparations != 1
                || p.max_prediction_tokens == 0
                || p.max_preparation_tokens == 0
        }) {
            return Err(
                "predictive preparation requires positive budgets and interval, with at most one preparation",
            );
        }
        if let Some(predictive) = &self.predictive {
            if let Some(acceptance) = &predictive.acceptance {
                acceptance.validate()?;
            }
            if predictive.mode == PredictionMode::Prepare
                && (matches!(self.mode, CompactionMode::Off)
                    || predictive.acceptance.is_none()
                    || predictive.max_preparations_per_hour == 0
                    || predictive.max_requests_per_hour == 0
                    || predictive.max_tokens_per_hour < predictive.max_preparation_tokens)
            {
                return Err(
                    "predictive preparation requires enabled automatic compaction, explicit acceptance and positive hourly budgets",
                );
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceBoundary {
    pub turn: JobId,
    pub input_through: u32,
    pub output_through: u32,
    pub content: ContentDigest,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeSummaryBoundary {
    pub job: JobId,
    pub revision: u64,
    pub output_through: u32,
    pub content: ContentDigest,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeSummaryManifest {
    pub conversation: JobId,
    pub through_job: JobId,
    pub sources: Vec<NativeSummaryBoundary>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum NativeSummaryResultLink {
    Received {
        source: ContentDigest,
        producer_job: ContentDigest,
        answer_request: crate::db::entities::RequestNumber,
    },
    Payload {
        source: ContentDigest,
        producer_job: ContentDigest,
        payload: ContentDigest,
    },
}

impl NativeSummaryResultLink {
    pub(crate) fn source(self) -> ContentDigest {
        match self {
            Self::Received { source, .. } | Self::Payload { source, .. } => source,
        }
    }
}

impl NativeSummaryManifest {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.sources.is_empty()
            || self.sources.len() > 4096
            || self.sources.last().map(|source| source.job) != Some(self.through_job)
            || self
                .sources
                .iter()
                .any(|source| source.revision == 0 || source.output_through == 0)
            || self
                .sources
                .windows(2)
                .any(|pair| pair[0].job >= pair[1].job)
        {
            return Err("invalid native summary manifest");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CompactionRequest {
    Prepared {
        job: JobId,
        conversation: JobId,
        through: JobId,
        subsequent: bool,
        already: bool,
    },
    Noop {
        reason: &'static str,
    },
}

pub const SUMMARIZER_OUTPUT_RESERVE: u32 = 4096;

fn deserialize_content_digest<'de, D>(deserializer: D) -> Result<ContentDigest, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct DigestVisitor;

    impl<'de> serde::de::Visitor<'de> for DigestVisitor {
        type Value = ContentDigest;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a 64-character hex SHA-256 digest or 32-byte array")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            let trimmed = value.trim();
            if trimmed.len() != 64 {
                return Err(E::invalid_length(trimmed.len(), &self));
            }
            let mut bytes = [0u8; 32];
            for (index, slot) in bytes.iter_mut().enumerate() {
                let pair = trimmed
                    .get(index * 2..index * 2 + 2)
                    .ok_or_else(|| E::custom("source digest must be ASCII hex"))?;
                *slot = u8::from_str_radix(pair, 16).map_err(E::custom)?;
            }
            Ok(ContentDigest::from_bytes(bytes))
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            let mut bytes = [0u8; 32];
            for (index, slot) in bytes.iter_mut().enumerate() {
                *slot = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(index, &self))?;
            }
            if seq.next_element::<u8>()?.is_some() {
                return Err(serde::de::Error::invalid_length(33, &self));
            }
            Ok(ContentDigest::from_bytes(bytes))
        }
    }

    deserializer.deserialize_any(DigestVisitor)
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SummarizationResult {
    #[serde(deserialize_with = "deserialize_content_digest")]
    pub source: ContentDigest,
    pub summary: String,
    pub omitted_details: Vec<String>,
}

fn summary_context_text(summary: &str, omissions: &[String]) -> String {
    format!(
        "{summary}\nThis generated summary may omit assistant details. Originals remain available. Declared omissions (not guaranteed complete): {}",
        omissions.join("; ")
    )
}
fn validate_summary(summary: &str, omissions: &[String]) -> Result<(), &'static str> {
    if summary.trim().is_empty()
        || summary.len() > 1024 * 1024
        || omissions.len() > 256
        || omissions.iter().any(|detail| detail.len() > 4096)
    {
        return Err("invalid summarization result");
    }
    Ok(())
}
impl SummarizationResult {
    pub fn context_text(&self) -> String {
        summary_context_text(&self.summary, &self.omitted_details)
    }
    pub fn validate(&self) -> Result<(), &'static str> {
        validate_summary(&self.summary, &self.omitted_details)
    }
}
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
    HostExit,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SummarizationStage {
    Direct,
    Chunk {
        start: u64,
        end: u64,
        content: ContentDigest,
    },
    Reduce,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Appended {
    pub turn: JobId,
    pub seq: u32,
    pub joined: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobAction {
    pub conversation: JobId,
    pub record_seq: Option<u32>,
    pub running: Vec<AllocationToken>,
    pub ended: Vec<JobId>,
    pub already: bool,
    pub settled: bool,
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
    LimitReached,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Recurrence {
    pub phase: RecurrencePhase,
    pub n: u64,
    pub current_turn: JobId,
    pub previous_turn: Option<JobId>,
    pub interval_s: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JobEdit {
    Limits(super::JobLimits),
    CompactionPolicy(Option<super::CompactionPolicy>),
    Title(String),
    Hide,
    Unhide,
}

pub(in crate::db) fn summary_chunks(
    text: &str,
    capacity: usize,
) -> Option<Vec<crate::db::entities::SummarizationStage>> {
    if capacity == 0 {
        return None;
    }
    let mut ranges = Vec::new();
    let mut start = 0usize;
    while start < text.len() {
        if ranges.len() == 7 {
            return None;
        }
        let mut end = start.saturating_add(capacity).min(text.len());
        while end > start && !text.is_char_boundary(end) {
            end -= 1;
        }
        if end == start {
            return None;
        }
        ranges.push(crate::db::entities::SummarizationStage::Chunk {
            start: start as u64,
            end: end as u64,
            content: crate::db::entities::ContentDigest::of(&text.as_bytes()[start..end]),
        });
        start = end;
    }
    (!ranges.is_empty()).then_some(ranges)
}

pub(crate) struct MutableConversationPage {
    pub turns: Vec<MutableConversationTurn>,
    pub has_more: bool,
}

pub(crate) struct MutableConversationTurn {
    pub planning_target: Option<JobId>,
    pub details: MutableJobDetails,
    pub outputs: Vec<JobOutputRecord>,
    pub outputs_truncated: bool,
    pub evidence: Vec<ToolEvidenceRecord>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ConversationEntry {
    pub root_job: JobId,
    pub tip_job: JobId,
    pub agent: Option<ConfigId>,
    pub title: String,
    pub project: Option<ProjectId>,
    pub state: String,
    pub on_hold: Option<Actor>,
    pub tip_settlement_state: String,
    pub tip_created_at: Timestamp,
    pub job_state: String,
    pub has_children: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ConversationsPage {
    pub conversations: Vec<ConversationEntry>,
    pub next_before: Option<JobId>,
    pub has_more: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ChildConversationEntry {
    pub conversation: ConversationEntry,
    pub origin: Origin,
    pub keep_open: bool,
    pub recurrence: Option<Recurrence>,
}

pub(crate) struct ContextWorkState {
    pub job: JobId,
    pub conversation: JobId,
    pub state: &'static str,
    pub details: ContextWorkDetails,
}

pub(crate) enum ContextWorkDetails {
    Summary {
        summarizer: ConfigId,
        failure: Option<String>,
        automatic: bool,
        stage: &'static str,
        chain: Option<JobId>,
        prediction: Option<JobId>,
        reserved_tokens: u32,
        reserved_requests: u32,
        source: ContentDigest,
        first: crate::db::entities::SourceBoundary,
        through: crate::db::entities::SourceBoundary,
        turn_count: usize,
    },
}

pub(crate) struct ContextState {
    pub policy: crate::db::entities::CompactionPolicy,
    pub stored_override: Option<crate::db::entities::CompactionPolicy>,
    pub policy_source: &'static str,
    pub revision: u64,
    pub model: Option<FrozenLlmConfig>,
    pub estimated_tokens: Option<u64>,
    pub growth: u32,
    pub predictor: Option<ConfigId>,
    pub predictor_model: Option<ConfigId>,
    pub prediction_unavailable: Option<String>,
    pub last_use: Option<(JobId, u32, Option<JobId>)>,
    pub steer_blocked_by: Option<&'static str>,
    pub work: Vec<ContextWorkState>,
    pub next_before: Option<JobId>,
}
