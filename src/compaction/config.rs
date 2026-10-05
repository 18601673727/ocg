//! Compaction policy: every knob in one place, and no algorithm in this file.
//!
//! The policies here are deliberately *separate* from the algorithms in the
//! sibling modules. Each algorithm takes its own policy struct by reference, so
//! a caller can construct a policy directly (tests, a CLI flag, an extracted
//! build) without going through configuration parsing.
//!
//! Threshold relationships, stated once so they are not rediscovered per
//! component:
//!
//! ```text
//! prompt_window   = min(context, input)          // never ignore `context`
//! reserve         = response + reasoning + safety
//! available       = prompt_window - reserve      // the compaction budget
//! reduce at       = reduce_percent  of available
//! compact at      = compact_percent of available
//! tail budget     = clamp(ratio_percent of available, min, max)
//! ```
//!
//! `reduce_percent` < `compact_percent` is a hard invariant: the deterministic
//! layer must always get a chance to run before the semantic layer, so that the
//! cheapest reduction wins first and a summary is only paid for when reduction
//! cannot make room.

use crate::error::{OcgError, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Default fraction of the available prompt budget at which deterministic
/// reduction runs. Reduction is cheap and loses no meaning, so it runs early.
pub const DEFAULT_REDUCE_PERCENT: u8 = 60;
/// Default fraction of the available prompt budget at which semantic
/// compaction runs.
pub const DEFAULT_COMPACT_PERCENT: u8 = 85;
/// Default ceiling for the *assumed* next-response reserve, in tokens, used
/// only when the provider exposes no output limit.
pub const DEFAULT_MAX_RESPONSE_RESERVE: u64 = 16_384;
/// Default next-response reserve when the provider exposes no output limit.
pub const DEFAULT_ASSUMED_RESPONSE_RESERVE: u64 = 4_096;
/// Default reasoning-token reserve when neither the provider nor the operator
/// declares one. Zero by default: it is deliberately not invented.
pub const DEFAULT_REASONING_RESERVE: u64 = 0;
/// Default safety buffer in tokens.
///
/// The buffer is the catch-all for everything that cannot be measured exactly:
/// tokenizer estimation error (OCG estimates bytes/4), provider framing
/// overhead, and reasoning allowance a provider declined to expose.
pub const DEFAULT_SAFETY_BUFFER: u64 = 4_096;
/// Default ceiling on an *assumed* reserve, as a percentage of the prompt
/// window. A declared provider limit is evidence and is never capped by this;
/// only an assumed value is, so a small window cannot be reserved into
/// uselessness.
pub const DEFAULT_MAX_ASSUMED_RESERVE_PERCENT: u8 = 40;

/// Default minimum recent-tail budget, in tokens.
pub const DEFAULT_TAIL_MIN_TOKENS: u64 = 2_000;
/// Default maximum recent-tail budget, in tokens.
pub const DEFAULT_TAIL_MAX_TOKENS: u64 = 15_000;
/// Default recent-tail budget as a percentage of the available budget.
pub const DEFAULT_TAIL_RATIO_PERCENT: u8 = 25;
/// Default cap on how many messages the tail scan may inspect. The scan stops as
/// soon as the budget is filled, so this only bounds pathological inputs.
pub const DEFAULT_TAIL_SCAN_LIMIT: usize = 512;

/// Default bound on one tool result in the active context, in characters.
pub const DEFAULT_MAX_TOOL_RESULT_CHARS: usize = 4_096;
/// Default bound on a failed tool result. Failures are kept longer than
/// successes because a failure is continuation-critical evidence.
pub const DEFAULT_MAX_ERROR_CHARS: usize = 8_192;
/// Default bound on one assistant text part in the active context.
pub const DEFAULT_MAX_ASSISTANT_CHARS: usize = 4_096;
/// Default bound on one assistant reasoning part in the active context.
///
/// Reasoning is bounded because an unbounded reasoning blob can make a
/// compaction *larger* than the history it replaced: the provider may omit
/// reasoning from live requests while the transcript retains it verbatim.
pub const DEFAULT_MAX_REASONING_CHARS: usize = 2_048;
/// Default number of trailing messages that deterministic reduction never
/// touches.
pub const DEFAULT_PROTECT_RECENT_MESSAGES: usize = 2;
/// Default characters of a collapsed repeat/superseded result that are kept.
pub const DEFAULT_DIGEST_PREVIEW_CHARS: usize = 512;

/// Default bound on one tool result inside the summary transcript, in
/// characters. The summarizer reasons about outcomes, not byte payloads.
pub const DEFAULT_TRANSCRIPT_TOOL_CHARS: usize = 2_000;
/// Default bound on one assistant text part inside the summary transcript.
pub const DEFAULT_TRANSCRIPT_ASSISTANT_CHARS: usize = 4_000;
/// Default bound on one assistant reasoning part inside the summary transcript.
pub const DEFAULT_TRANSCRIPT_REASONING_CHARS: usize = 1_000;
/// Default cap on tool calls serialized into the summary transcript.
pub const DEFAULT_TRANSCRIPT_MAX_TOOL_CALLS: usize = 256;

/// Default ceiling on the tokens the summary response may claim.
pub const DEFAULT_MAX_SUMMARY_TOKENS: u64 = 4_096;
/// Default ceiling on the characters of a summary, independent of the provider
/// token limit.
pub const DEFAULT_MAX_SUMMARY_CHARS: usize = 24_000;
/// Default number of canonical Calls rendered into the summary prompt.
pub const DEFAULT_SUMMARY_MAX_CALLS: usize = 64;

/// Smallest accepted threshold percentage.
pub const MIN_THRESHOLD_PERCENT: u8 = 1;
/// Hard ceiling on a tool-result bound, in characters.
pub const MAX_TOOL_RESULT_CHARS_CEILING: usize = 262_144;
/// Hard ceiling on the retained-tail budget, in tokens.
pub const MAX_TAIL_TOKENS_CEILING: u64 = 262_144;

/// `orchestration.compaction`: the compaction policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct CompactionConfig {
    /// Master switch. When false no decision is made and the durable history is
    /// projected verbatim.
    pub enabled: bool,
    /// Utilization of the available budget at which deterministic reduction runs.
    pub reduce_percent: u8,
    /// Utilization of the available budget at which semantic compaction runs.
    pub compact_percent: u8,
    /// Absolute token count at which reduction runs, used when the provider
    /// exposes no trustworthy window. An internal safety budget, never a claim
    /// about the provider's context window.
    pub absolute_reduce_tokens: Option<u64>,
    /// Absolute token count at which semantic compaction runs.
    pub absolute_compact_tokens: Option<u64>,
    /// Absolute override for the next-response reserve.
    pub response_reserve_tokens: Option<u64>,
    /// Reasoning-token reserve used when the provider exposes no reasoning
    /// allowance. Defaults to zero; the safety buffer is the catch-all.
    pub reasoning_reserve_tokens: u64,
    /// Safety buffer in tokens.
    pub safety_buffer_tokens: u64,
    /// Ceiling on an assumed reserve, as a percentage of the prompt window.
    pub max_assumed_reserve_percent: u8,
    /// Recent-tail retention policy.
    pub tail: TailPolicy,
    /// Deterministic reduction policy.
    pub reduce: ReducePolicy,
    /// Summary-transcript serialization policy.
    pub transcript: TranscriptPolicy,
    /// Semantic summary policy.
    pub summary: SummaryPolicy,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            reduce_percent: DEFAULT_REDUCE_PERCENT,
            compact_percent: DEFAULT_COMPACT_PERCENT,
            absolute_reduce_tokens: None,
            absolute_compact_tokens: None,
            response_reserve_tokens: None,
            reasoning_reserve_tokens: DEFAULT_REASONING_RESERVE,
            safety_buffer_tokens: DEFAULT_SAFETY_BUFFER,
            max_assumed_reserve_percent: DEFAULT_MAX_ASSUMED_RESERVE_PERCENT,
            tail: TailPolicy::default(),
            reduce: ReducePolicy::default(),
            transcript: TranscriptPolicy::default(),
            summary: SummaryPolicy::default(),
        }
    }
}

impl CompactionConfig {
    /// Parse the nested JSON object. Absent or null yields the defaults.
    pub fn from_value(value: &Value) -> Result<Self> {
        if value.is_null() {
            return Ok(Self::default());
        }
        let object = value
            .as_object()
            .ok_or_else(|| invalid_err("must be an object"))?;
        let mut config = Self::default();
        if let Some(value) = object.get("enabled") {
            config.enabled = value
                .as_bool()
                .ok_or_else(|| invalid_err("enabled must be a boolean"))?;
        }
        if let Some(value) = first(object, &["reducePercent", "reduce_percent"]) {
            config.reduce_percent = parse_percent(value, "reducePercent")?;
        }
        if let Some(value) = first(object, &["compactPercent", "compact_percent"]) {
            config.compact_percent = parse_percent(value, "compactPercent")?;
        }
        for (keys, slot, label) in [
            (
                ["absoluteReduceTokens", "absolute_reduce_tokens"].as_slice(),
                &mut config.absolute_reduce_tokens,
                "absoluteReduceTokens",
            ),
            (
                ["absoluteCompactTokens", "absolute_compact_tokens"].as_slice(),
                &mut config.absolute_compact_tokens,
                "absoluteCompactTokens",
            ),
            (
                ["responseReserveTokens", "response_reserve_tokens"].as_slice(),
                &mut config.response_reserve_tokens,
                "responseReserveTokens",
            ),
        ] {
            if let Some(value) = first(object, keys) {
                if !value.is_null() {
                    *slot = Some(parse_positive_u64(value, label)?);
                }
            }
        }
        if let Some(value) = first(
            object,
            &["reasoningReserveTokens", "reasoning_reserve_tokens"],
        ) {
            config.reasoning_reserve_tokens =
                parse_non_negative_u64(value, "reasoningReserveTokens")?;
        }
        if let Some(value) = first(object, &["safetyBufferTokens", "safety_buffer_tokens"]) {
            config.safety_buffer_tokens = parse_non_negative_u64(value, "safetyBufferTokens")?;
        }
        if let Some(value) = first(
            object,
            &["maxAssumedReservePercent", "max_assumed_reserve_percent"],
        ) {
            config.max_assumed_reserve_percent = parse_percent(value, "maxAssumedReservePercent")?;
        }
        config.tail = TailPolicy::from_value(pick(object, &["tail"]))?;
        config.reduce = ReducePolicy::from_value(pick(object, &["reduce", "reduction"]))?;
        config.transcript = TranscriptPolicy::from_value(pick(object, &["transcript"]))?;
        config.summary = SummaryPolicy::from_value(pick(object, &["summary"]))?;
        config.validate_values()?;
        Ok(config)
    }

    /// Collect every policy problem for whole-configuration validation.
    pub fn validate(data: &Value) -> Vec<String> {
        match Self::from_config(data) {
            Ok(_) => Vec::new(),
            Err(error) => vec![error.to_string()],
        }
    }

    /// Parse `data["orchestration"]["compaction"]`.
    pub fn from_config(data: &Value) -> Result<Self> {
        let Some(orchestration) = data.get("orchestration") else {
            return Ok(Self::default());
        };
        if orchestration.is_null() {
            return Ok(Self::default());
        }
        let object = orchestration
            .as_object()
            .ok_or_else(|| invalid_err("must be an object"))?;
        Self::from_value(pick(object, &["compaction"]))
    }

    pub fn validate_values(&self) -> Result<()> {
        if self.compact_percent <= self.reduce_percent {
            return Err(invalid_err(
                "compactPercent must be greater than reducePercent; deterministic reduction runs first",
            ));
        }
        if self.max_assumed_reserve_percent == 0 || self.max_assumed_reserve_percent > 100 {
            return Err(invalid_err(
                "maxAssumedReservePercent must be between 1 and 100",
            ));
        }
        self.tail.validate_values()?;
        self.reduce.validate_values()?;
        self.transcript.validate_values()?;
        self.summary.validate_values()?;
        Ok(())
    }

    /// A stable fingerprint of the policy. A changed policy changes the
    /// fingerprint, so a checkpoint written under a different policy is
    /// recognisable as such.
    pub fn fingerprint(&self) -> String {
        let value = serde_json::to_value(self).unwrap_or(Value::Null);
        crate::hash::sha256_hex(value.to_string().as_bytes())
    }
}

/// Recent-tail retention policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct TailPolicy {
    /// Absolute tail budget in tokens. When set it replaces the derived budget.
    pub budget_tokens: Option<u64>,
    /// Floor for a derived tail budget.
    pub min_budget_tokens: u64,
    /// Ceiling for a derived tail budget.
    pub max_budget_tokens: u64,
    /// Derived tail budget as a percentage of the available budget.
    pub ratio_percent: u8,
    /// Cap on how many recent turns are considered. `None` means no cap.
    pub max_turns: Option<usize>,
    /// Cap on how many messages the tail scan may inspect.
    pub scan_limit: usize,
}

impl Default for TailPolicy {
    fn default() -> Self {
        Self {
            budget_tokens: None,
            min_budget_tokens: DEFAULT_TAIL_MIN_TOKENS,
            max_budget_tokens: DEFAULT_TAIL_MAX_TOKENS,
            ratio_percent: DEFAULT_TAIL_RATIO_PERCENT,
            max_turns: None,
            scan_limit: DEFAULT_TAIL_SCAN_LIMIT,
        }
    }
}

impl TailPolicy {
    fn from_value(value: &Value) -> Result<Self> {
        let Some(object) = as_object(value, "compaction.tail")? else {
            return Ok(Self::default());
        };
        let mut policy = Self::default();
        if let Some(value) = first(object, &["budgetTokens", "budget_tokens"]) {
            if !value.is_null() {
                policy.budget_tokens = Some(parse_positive_u64(value, "budgetTokens")?);
            }
        }
        for (keys, slot, label) in [
            (
                ["minBudgetTokens", "min_budget_tokens"].as_slice(),
                &mut policy.min_budget_tokens,
                "minBudgetTokens",
            ),
            (
                ["maxBudgetTokens", "max_budget_tokens"].as_slice(),
                &mut policy.max_budget_tokens,
                "maxBudgetTokens",
            ),
        ] {
            if let Some(value) = first(object, keys) {
                *slot = parse_positive_u64(value, label)?;
            }
        }
        if let Some(value) = first(object, &["ratioPercent", "ratio_percent"]) {
            policy.ratio_percent = parse_percent(value, "ratioPercent")?;
        }
        if let Some(value) = first(object, &["maxTurns", "max_turns"]) {
            if !value.is_null() {
                policy.max_turns =
                    Some(
                        value.as_u64().filter(|number| *number > 0).ok_or_else(|| {
                            invalid_err("tail.maxTurns must be a positive integer")
                        })? as usize,
                    );
            }
        }
        if let Some(value) = first(object, &["scanLimit", "scan_limit"]) {
            policy.scan_limit = value
                .as_u64()
                .filter(|number| *number > 0)
                .ok_or_else(|| invalid_err("tail.scanLimit must be a positive integer"))?
                as usize;
        }
        policy.validate_values()?;
        Ok(policy)
    }

    pub fn validate_values(&self) -> Result<()> {
        if self.max_budget_tokens < self.min_budget_tokens {
            return Err(invalid_err(
                "tail.maxBudgetTokens must not be below minBudgetTokens",
            ));
        }
        if self.max_budget_tokens > MAX_TAIL_TOKENS_CEILING {
            return Err(invalid_err(format!(
                "tail.maxBudgetTokens must not exceed {MAX_TAIL_TOKENS_CEILING}"
            )));
        }
        if self.ratio_percent == 0 || self.ratio_percent > 100 {
            return Err(invalid_err("tail.ratioPercent must be between 1 and 100"));
        }
        if self.scan_limit == 0 {
            return Err(invalid_err("tail.scanLimit must be a positive integer"));
        }
        Ok(())
    }
}

/// Deterministic reduction policy.
///
/// Every value here bounds *representation*. Reduction never deletes a message
/// and never changes an authoritative status: it replaces a payload with a
/// bounded, self-describing stand-in.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ReducePolicy {
    /// Bound on one tool result.
    pub max_tool_result_chars: usize,
    /// Bound on one failed tool result.
    pub max_error_chars: usize,
    /// Bound on one assistant text part.
    pub max_assistant_chars: usize,
    /// Bound on one assistant reasoning part.
    pub max_reasoning_chars: usize,
    /// Number of trailing messages reduction never touches.
    pub protect_recent_messages: usize,
    /// Collapse repeated read-only observation results onto their latest
    /// occurrence.
    pub collapse_repeat_reads: bool,
    /// Collapse a read result that a later operation on the same path made
    /// stale.
    pub collapse_superseded_reads: bool,
    /// Characters of a collapsed result kept as a preview.
    pub digest_preview_chars: usize,
}

impl Default for ReducePolicy {
    fn default() -> Self {
        Self {
            max_tool_result_chars: DEFAULT_MAX_TOOL_RESULT_CHARS,
            max_error_chars: DEFAULT_MAX_ERROR_CHARS,
            max_assistant_chars: DEFAULT_MAX_ASSISTANT_CHARS,
            max_reasoning_chars: DEFAULT_MAX_REASONING_CHARS,
            protect_recent_messages: DEFAULT_PROTECT_RECENT_MESSAGES,
            collapse_repeat_reads: true,
            collapse_superseded_reads: true,
            digest_preview_chars: DEFAULT_DIGEST_PREVIEW_CHARS,
        }
    }
}

impl ReducePolicy {
    fn from_value(value: &Value) -> Result<Self> {
        let Some(object) = as_object(value, "compaction.reduce")? else {
            return Ok(Self::default());
        };
        let mut policy = Self::default();
        for (keys, slot, label) in [
            (
                ["maxToolResultChars", "max_tool_result_chars"].as_slice(),
                &mut policy.max_tool_result_chars,
                "maxToolResultChars",
            ),
            (
                ["maxErrorChars", "max_error_chars"].as_slice(),
                &mut policy.max_error_chars,
                "maxErrorChars",
            ),
            (
                ["maxAssistantChars", "max_assistant_chars"].as_slice(),
                &mut policy.max_assistant_chars,
                "maxAssistantChars",
            ),
            (
                ["maxReasoningChars", "max_reasoning_chars"].as_slice(),
                &mut policy.max_reasoning_chars,
                "maxReasoningChars",
            ),
        ] {
            if let Some(value) = first(object, keys) {
                *slot = parse_positive_usize(value, label)?;
            }
        }
        if let Some(value) = first(
            object,
            &["protectRecentMessages", "protect_recent_messages"],
        ) {
            policy.protect_recent_messages =
                parse_non_negative_usize(value, "protectRecentMessages")?;
        }
        for (keys, slot, label) in [
            (
                ["collapseRepeatReads", "collapse_repeat_reads"].as_slice(),
                &mut policy.collapse_repeat_reads,
                "collapseRepeatReads",
            ),
            (
                ["collapseSupersededReads", "collapse_superseded_reads"].as_slice(),
                &mut policy.collapse_superseded_reads,
                "collapseSupersededReads",
            ),
        ] {
            if let Some(value) = first(object, keys) {
                *slot = value
                    .as_bool()
                    .ok_or_else(|| invalid_err(format!("reduce.{label} must be a boolean")))?;
            }
        }
        if let Some(value) = first(object, &["digestPreviewChars", "digest_preview_chars"]) {
            policy.digest_preview_chars = parse_positive_usize(value, "digestPreviewChars")?;
        }
        policy.validate_values()?;
        Ok(policy)
    }

    pub fn validate_values(&self) -> Result<()> {
        for (value, label) in [
            (self.max_tool_result_chars, "maxToolResultChars"),
            (self.max_error_chars, "maxErrorChars"),
            (self.max_assistant_chars, "maxAssistantChars"),
            (self.max_reasoning_chars, "maxReasoningChars"),
        ] {
            if value == 0 || value > MAX_TOOL_RESULT_CHARS_CEILING {
                return Err(invalid_err(format!(
                    "reduce.{label} must be between 1 and {MAX_TOOL_RESULT_CHARS_CEILING}"
                )));
            }
        }
        if self.digest_preview_chars > self.max_tool_result_chars {
            return Err(invalid_err(
                "reduce.digestPreviewChars must not exceed maxToolResultChars",
            ));
        }
        Ok(())
    }
}

/// Summary-transcript serialization policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct TranscriptPolicy {
    /// Bound on one tool result in the transcript.
    pub max_tool_result_chars: usize,
    /// Bound on one assistant text part in the transcript.
    pub max_assistant_chars: usize,
    /// Bound on one assistant reasoning part in the transcript.
    pub max_reasoning_chars: usize,
    /// Cap on tool calls serialized into the transcript.
    pub max_tool_calls: usize,
}

impl Default for TranscriptPolicy {
    fn default() -> Self {
        Self {
            max_tool_result_chars: DEFAULT_TRANSCRIPT_TOOL_CHARS,
            max_assistant_chars: DEFAULT_TRANSCRIPT_ASSISTANT_CHARS,
            max_reasoning_chars: DEFAULT_TRANSCRIPT_REASONING_CHARS,
            max_tool_calls: DEFAULT_TRANSCRIPT_MAX_TOOL_CALLS,
        }
    }
}

impl TranscriptPolicy {
    fn from_value(value: &Value) -> Result<Self> {
        let Some(object) = as_object(value, "compaction.transcript")? else {
            return Ok(Self::default());
        };
        let mut policy = Self::default();
        for (keys, slot, label) in [
            (
                ["maxToolResultChars", "max_tool_result_chars"].as_slice(),
                &mut policy.max_tool_result_chars,
                "maxToolResultChars",
            ),
            (
                ["maxAssistantChars", "max_assistant_chars"].as_slice(),
                &mut policy.max_assistant_chars,
                "maxAssistantChars",
            ),
            (
                ["maxReasoningChars", "max_reasoning_chars"].as_slice(),
                &mut policy.max_reasoning_chars,
                "maxReasoningChars",
            ),
            (
                ["maxToolCalls", "max_tool_calls"].as_slice(),
                &mut policy.max_tool_calls,
                "maxToolCalls",
            ),
        ] {
            if let Some(value) = first(object, keys) {
                *slot = parse_positive_usize(value, label)?;
            }
        }
        policy.validate_values()?;
        Ok(policy)
    }

    pub fn validate_values(&self) -> Result<()> {
        for (value, label) in [
            (self.max_tool_result_chars, "maxToolResultChars"),
            (self.max_assistant_chars, "maxAssistantChars"),
            (self.max_reasoning_chars, "maxReasoningChars"),
        ] {
            if value == 0 || value > MAX_TOOL_RESULT_CHARS_CEILING {
                return Err(invalid_err(format!(
                    "transcript.{label} must be between 1 and {MAX_TOOL_RESULT_CHARS_CEILING}"
                )));
            }
        }
        if self.max_tool_calls == 0 {
            return Err(invalid_err(
                "transcript.maxToolCalls must be a positive integer",
            ));
        }
        Ok(())
    }
}

/// Semantic summary policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct SummaryPolicy {
    /// Ceiling on the tokens the summary response may claim.
    pub max_summary_tokens: u64,
    /// Ceiling on summary characters, independent of the token ceiling.
    pub max_summary_chars: usize,
    /// Number of canonical Calls rendered into the summary prompt.
    pub max_calls: usize,
    /// Require every mandatory section before the boundary is allowed to
    /// advance. When true an incomplete summary is refused rather than accepted
    /// as success, because a silently short summary loses history permanently.
    pub require_all_sections: bool,
    /// Append a synthetic continuation message after a successful compaction.
    pub continue_after_compaction: bool,
    /// Redact secret-shaped free text in the summary and continuation.
    pub redact_free_text: bool,
}

impl Default for SummaryPolicy {
    fn default() -> Self {
        Self {
            max_summary_tokens: DEFAULT_MAX_SUMMARY_TOKENS,
            max_summary_chars: DEFAULT_MAX_SUMMARY_CHARS,
            max_calls: DEFAULT_SUMMARY_MAX_CALLS,
            require_all_sections: true,
            continue_after_compaction: true,
            redact_free_text: true,
        }
    }
}

impl SummaryPolicy {
    fn from_value(value: &Value) -> Result<Self> {
        let Some(object) = as_object(value, "compaction.summary")? else {
            return Ok(Self::default());
        };
        let mut policy = Self::default();
        if let Some(value) = first(object, &["maxSummaryTokens", "max_summary_tokens"]) {
            policy.max_summary_tokens = parse_positive_u64(value, "maxSummaryTokens")?;
        }
        if let Some(value) = first(object, &["maxSummaryChars", "max_summary_chars"]) {
            policy.max_summary_chars = parse_positive_usize(value, "maxSummaryChars")?;
        }
        if let Some(value) = first(object, &["maxCalls", "max_calls"]) {
            policy.max_calls = parse_positive_usize(value, "maxCalls")?;
        }
        for (keys, slot, label) in [
            (
                ["requireAllSections", "require_all_sections"].as_slice(),
                &mut policy.require_all_sections,
                "requireAllSections",
            ),
            (
                ["continueAfterCompaction", "continue_after_compaction"].as_slice(),
                &mut policy.continue_after_compaction,
                "continueAfterCompaction",
            ),
            (
                ["redactFreeText", "redact_free_text"].as_slice(),
                &mut policy.redact_free_text,
                "redactFreeText",
            ),
        ] {
            if let Some(value) = first(object, keys) {
                *slot = value
                    .as_bool()
                    .ok_or_else(|| invalid_err(format!("summary.{label} must be a boolean")))?;
            }
        }
        policy.validate_values()?;
        Ok(policy)
    }

    pub fn validate_values(&self) -> Result<()> {
        if self.max_summary_tokens == 0 {
            return Err(invalid_err(
                "summary.maxSummaryTokens must be a positive integer",
            ));
        }
        if self.max_summary_chars == 0 {
            return Err(invalid_err(
                "summary.maxSummaryChars must be a positive integer",
            ));
        }
        if self.max_calls == 0 {
            return Err(invalid_err("summary.maxCalls must be a positive integer"));
        }
        Ok(())
    }
}

/// Look up the first present key, tolerating both naming conventions.
/// Look up the first present key in an object, tolerating both naming
/// conventions. An absent key is `Value::Null`, which every nested parser treats
/// as "use the defaults".
fn pick<'a>(object: &'a serde_json::Map<String, Value>, keys: &[&str]) -> &'a Value {
    first(object, keys).unwrap_or(&Value::Null)
}

fn first<'a>(object: &'a serde_json::Map<String, Value>, keys: &[&str]) -> Option<&'a Value> {
    keys.iter().find_map(|key| object.get(*key))
}

fn as_object<'a>(
    value: &'a Value,
    label: &str,
) -> Result<Option<&'a serde_json::Map<String, Value>>> {
    if value.is_null() {
        return Ok(None);
    }
    match value.as_object() {
        Some(object) => Ok(Some(object)),
        None => Err(invalid_err(format!("{label} must be an object"))),
    }
}

/// Build a configuration error prefixed with the key path.
///
/// Every message names its full key so an operator can find it in a deeply
/// nested config without guessing which branch rejected the value.
fn invalid_err(message: impl std::fmt::Display) -> OcgError {
    OcgError::config(format!("orchestration.compaction.{message}"))
}

fn parse_percent(value: &Value, label: &str) -> Result<u8> {
    let value = value
        .as_u64()
        .ok_or_else(|| invalid_err(format!("{label} must be an integer")))?;
    u8::try_from(value)
        .ok()
        .filter(|percent| *percent >= MIN_THRESHOLD_PERCENT)
        .ok_or_else(|| {
            invalid_err(format!(
                "{label} must be between {MIN_THRESHOLD_PERCENT} and 100"
            ))
        })
}

fn parse_positive_u64(value: &Value, label: &str) -> Result<u64> {
    value
        .as_u64()
        .filter(|number| *number > 0)
        .ok_or_else(|| invalid_err(format!("{label} must be a positive integer")))
}

fn parse_non_negative_u64(value: &Value, label: &str) -> Result<u64> {
    value
        .as_u64()
        .ok_or_else(|| invalid_err(format!("{label} must be a non-negative integer")))
}

fn parse_positive_usize(value: &Value, label: &str) -> Result<usize> {
    let value = parse_positive_u64(value, label)?;
    usize::try_from(value).map_err(|_| invalid_err(format!("{label} is too large")))
}

fn parse_non_negative_usize(value: &Value, label: &str) -> Result<usize> {
    let value = parse_non_negative_u64(value, label)?;
    usize::try_from(value).map_err(|_| invalid_err(format!("{label} is too large")))
}
