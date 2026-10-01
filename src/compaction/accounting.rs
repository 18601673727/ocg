//! Token accounting: what the window actually contains, and how much is left.
//!
//! This module never guesses a denominator and never counts a token it cannot
//! justify. It separates two questions that are usually conflated:
//!
//! 1. *What does the outgoing request cost?* [`RequestFootprint`] measures the
//!    request that is about to be sent — system prompt, tool definitions,
//!    messages, and the tool-result share of those messages.
//! 2. *What room is left in the window?* [`TokenBudget`] decomposes the
//!    provider window into named, individually auditable reservations.
//!
//! The decomposition is deliberately verbose. A single `usable()` number hides
//! the very mistakes this module exists to prevent:
//!
//! - A model whose output limit exceeds its context window. Reserving the full
//!   output against a smaller window leaves zero usable budget, which reads as
//!   "always overflow" and compacts after every step forever. The output
//!   reservation is therefore capped at the context window, and the reserve is
//!   always checked against the window it is subtracted from.
//! - A provider-declared *input* limit larger than the context window. When
//!   both are known the prompt window is `min(context, input)`; an input limit
//!   never widens the window.
//! - Reasoning tokens that a provider reports but does not declare a ceiling
//!   for. An undeclared reasoning allowance is carried as its own line and is
//!   not silently folded into output.
//! - A safety buffer that exists precisely because the footprint is *measured*
//!   rather than exact: OCG estimates from serialized bytes, and the provider
//!   adds framing, role delimiters and tool-encoding overhead that a byte count
//!   does not see.
//!
//! Every limit is `Option`. A missing limit is `None`, never zero, and a
//! zero-valued limit from a provider is normalized to `None`: a reported zero
//! window means "unknown", not "no tokens allowed".

use crate::compaction::config::CompactionConfig;
use crate::error::Result;
use crate::telemetry::tokens::TokenCount;
use serde::{Deserialize, Serialize};

/// Provider-declared model window, normalized.
///
/// All fields are optional because a provider may declare any subset. A
/// declared zero is normalized to `None` on construction.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ModelLimits {
    /// Total context window, input plus the largest possible response.
    pub context: Option<u64>,
    /// Provider-declared prompt-side ceiling, when the API distinguishes one
    /// from the context window. It can only ever narrow the window.
    pub input: Option<u64>,
    /// Provider-declared maximum response size.
    pub output: Option<u64>,
    /// Provider-declared reasoning allowance, when the provider exposes one.
    /// This is charged against the same window as output on most providers, so
    /// it is reserved separately rather than assumed to be included.
    pub reasoning: Option<u64>,
    /// Where these numbers came from, for diagnostics only. Never a raw
    /// provider response.
    pub source: Option<String>,
}

impl ModelLimits {
    /// Build normalized limits: a declared zero becomes unknown.
    pub fn new(context: Option<u64>, input: Option<u64>, output: Option<u64>) -> Self {
        Self {
            context: positive(context),
            input: positive(input),
            output: positive(output),
            reasoning: None,
            source: None,
        }
    }

    /// Attach a reasoning allowance and a provenance label.
    pub fn with_reasoning(mut self, reasoning: Option<u64>) -> Self {
        self.reasoning = positive(reasoning);
        self
    }

    /// The prompt-side ceiling: `min(context, input)`.
    ///
    /// An input limit never widens the window, so when both are declared the
    /// smaller one wins. When only one is declared that one is the ceiling.
    pub fn prompt_ceiling(&self) -> Option<u64> {
        match (self.context, self.input) {
            (Some(context), Some(input)) => Some(context.min(input)),
            (Some(context), None) => Some(context),
            (None, Some(input)) => Some(input),
            (None, None) => None,
        }
    }

    /// Whether a trustworthy prompt ceiling exists.
    pub fn is_trustworthy(&self) -> bool {
        self.prompt_ceiling().is_some()
    }

    /// Whether the declared limits are mutually consistent.
    ///
    /// A context window smaller than the declared output limit means at least
    /// one number is wrong. The window is the more fundamental claim, so the
    /// output limit is treated as undeclared rather than allowed to drive the
    /// budget into a permanently overflowing state.
    pub fn is_consistent(&self) -> bool {
        match (self.context, self.output) {
            (Some(context), Some(output)) => context >= output,
            _ => true,
        }
    }
}

fn positive(value: Option<u64>) -> Option<u64> {
    value.filter(|number| *number > 0)
}

/// The measured size of one outgoing provider request.
///
/// The four components are reported separately because they have different
/// remedies: an oversized system prompt is a prompt problem, oversized tool
/// definitions are a schema problem, and oversized tool results are the one
/// component deterministic reduction can fix without semantic loss.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct RequestFootprint {
    /// Tokens attributed to the system prompt.
    pub system_prompt: u64,
    /// Tokens attributed to the tool definition array.
    pub tool_definitions: u64,
    /// Tokens attributed to the message array, inclusive of tool results.
    pub messages: u64,
    /// The subset of `messages` that is tool results. Reported for diagnosis,
    /// never added again to the total.
    pub tool_results: u64,
    /// The provider-reported prompt count for the previous request, when one is
    /// available. Used only to sanity-check the estimate.
    pub reported_prompt: Option<u64>,
}

impl RequestFootprint {
    /// Total prompt-side tokens: the three components that are actually sent.
    ///
    /// `tool_results` is a subset of `messages` and is never added here, so a
    /// request cannot be double-counted against its own window.
    pub fn prompt_tokens(&self) -> u64 {
        self.system_prompt
            .saturating_add(self.tool_definitions)
            .saturating_add(self.messages)
    }

    /// Fraction of the footprint that is tool results, in basis points.
    pub fn tool_result_share_bp(&self) -> u64 {
        let messages = self.messages;
        if messages == 0 {
            return 0;
        }
        ((self.tool_results as u128 * 10_000) / messages as u128) as u64
    }

    /// Whether a provider-reported count contradicts the estimate.
    ///
    /// A report that exceeds the estimate by more than [`Self::REPORT_SLACK_BP`]
    /// is retained as reported, because the provider is authoritative. A report
    /// that *understates* the measured request is discarded: the measurement is
    /// the conservative side and a smaller number must never authorize
    /// proceeding past a limit.
    pub fn reported_disagrees(&self) -> bool {
        let Some(reported) = self.reported_prompt else {
            return false;
        };
        let measured = self.prompt_tokens();
        if measured == 0 {
            return false;
        }
        let slack = (measured as u128 * Self::REPORT_SLACK_BP as u128) / 10_000;
        let floor = (measured as u128).saturating_sub(slack);
        (reported as u128) < floor
    }

    /// Basis-point slack allowed between an estimate and a provider report.
    pub const REPORT_SLACK_BP: u64 = 2_500;

    /// The count a decision should use.
    ///
    /// A provider report wins when it is at least as large as the measurement.
    /// Otherwise the measurement stands and the disagreement is recorded by
    /// [`Self::reported_disagrees`]. This keeps the larger, safer number
    /// authoritative without letting an understated report reopen a budget that
    /// the measured request has already consumed.
    pub fn effective_prompt_tokens(&self) -> u64 {
        match self.reported_prompt {
            Some(reported) if !self.reported_disagrees() => reported.max(self.prompt_tokens()),
            _ => self.prompt_tokens(),
        }
    }

    /// Attach a provider-reported prompt count.
    pub fn with_reported(mut self, count: &TokenCount) -> Self {
        self.reported_prompt = count.total;
        self
    }

    /// Attach a provider-reported prompt count directly.
    pub fn with_reported_tokens(mut self, tokens: Option<u64>) -> Self {
        self.reported_prompt = positive(tokens);
        self
    }
}

/// How much of the prompt window is reserved, and for what.
///
/// Every line is explicit so a budget can be explained without re-deriving it.
/// The sum is what leaves the window, and it is always bounded by the window so
/// a pathological declaration cannot produce a negative budget.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct TokenBudget {
    /// The prompt-side ceiling the reservations are subtracted from.
    pub prompt_ceiling: Option<u64>,
    /// Reserved for the next response's visible output.
    pub output_reserve: u64,
    /// Reserved for reasoning the provider may emit and count against the
    /// window. Zero when neither the provider nor the operator declared one.
    pub reasoning_reserve: u64,
    /// Reserved for estimation error and provider framing overhead.
    pub safety_buffer: u64,
}

impl TokenBudget {
    /// Total reserved tokens.
    pub fn total_reserve(&self) -> u64 {
        self.output_reserve
            .saturating_add(self.reasoning_reserve)
            .saturating_add(self.safety_buffer)
    }

    /// Tokens available for prompt-side content after reservations.
    ///
    /// `None` when the window is unknown: absence is never zero, and a caller
    /// must not treat an unknown window as a full or an empty one.
    pub fn available(&self) -> Option<u64> {
        self.prompt_ceiling
            .map(|ceiling| ceiling.saturating_sub(self.total_reserve()))
    }

    /// Whether a footprint of `prompt_tokens` still fits.
    ///
    /// An unknown ceiling never blocks. The alternative — treating unknown as
    /// zero — compacts an unbounded request against an imaginary limit, and
    /// treating it as unlimited hides a genuine overflow from a provider that
    /// simply declined to publish its window.
    pub fn admits(&self, prompt_tokens: u64) -> bool {
        match self.available() {
            Some(available) => prompt_tokens <= available,
            None => true,
        }
    }

    /// Utilization of the available budget in basis points.
    ///
    /// `None` when either side is unknown or the available budget is zero,
    /// rather than a fabricated percentage.
    pub fn utilization_bp(&self, prompt_tokens: u64) -> Option<u64> {
        let available = self.available()?;
        if available == 0 {
            return None;
        }
        Some(((prompt_tokens as u128 * 10_000) / available as u128) as u64)
    }

    /// Tokens still free, or `None` when unknown.
    pub fn headroom(&self, prompt_tokens: u64) -> Option<u64> {
        self.available()
            .map(|available| available.saturating_sub(prompt_tokens))
    }
}

/// Derive the reservations for one model.
///
/// The output reservation is bounded three ways, and each bound exists for a
/// stated reason:
///
/// 1. by the provider's declared output limit, which is the real ceiling on a
///    single response;
/// 2. by [`CompactionConfig::response_reserve_tokens`] when an operator wants a
///    concrete number rather than the model ceiling;
/// 3. by the prompt ceiling itself, so an inconsistent declaration degrades to
///    "the whole window is reserved" — loudly, with an available budget of
///    zero — instead of wrapping into a large spurious budget.
///
/// Reasoning uses the provider's declared allowance when present and the
/// configured reserve otherwise. The configured default is zero because an
/// undeclared reasoning allowance should be visible as absent rather than
/// quietly padded.
pub fn budget_for(limits: &ModelLimits, config: &CompactionConfig) -> TokenBudget {
    let ceiling = limits.prompt_ceiling();
    let declared_output = if limits.is_consistent() {
        limits.output
    } else {
        None
    };
    let output_reserve = config
        .response_reserve_tokens
        .or(declared_output)
        .or(Some(crate::compaction::config::DEFAULT_ASSUMED_RESPONSE_RESERVE))
        .unwrap_or(0);
    let output_reserve = match ceiling {
        Some(ceiling) => output_reserve.min(ceiling),
        None => output_reserve,
    };
    let reasoning_reserve = limits
        .reasoning
        .unwrap_or(config.reasoning_reserve_tokens)
        .min(match ceiling {
            Some(ceiling) => ceiling.saturating_sub(output_reserve),
            None => u64::MAX,
        });
    let safety_buffer = config.safety_buffer_tokens.min(match ceiling {
        Some(ceiling) => ceiling
            .saturating_sub(output_reserve)
            .saturating_sub(reasoning_reserve),
        None => u64::MAX,
    });
    TokenBudget {
        prompt_ceiling: ceiling,
        output_reserve,
        reasoning_reserve,
        safety_buffer,
    }
}

/// A pure decision about what to do with the active context.
///
/// This is the whole trigger surface: a band, the reasons behind it, and the
/// measurements that produced it. It performs no I/O and creates nothing, so a
/// caller can evaluate it on every turn and act only when it says so.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionDecision {
    /// Which band the active context is in.
    pub state: CompactionState,
    /// Whether semantic compaction should be attempted before the next model
    /// request.
    pub required: bool,
    /// The budget the decision was made against.
    pub budget: TokenBudget,
    /// The measured request the decision was made from.
    pub footprint: RequestFootprint,
    /// Utilization of the available budget in basis points, when known.
    pub utilization_bp: Option<u64>,
    /// Tokens still free, when known.
    pub headroom: Option<u64>,
    /// A human-readable explanation. It states the measured components so a
    /// surprising compaction can be diagnosed from the reason alone.
    pub reason: String,
}

/// Context bands, ordered by how much room is left.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionState {
    /// Compaction is switched off; history is projected verbatim.
    Disabled,
    /// The context fits, or the window is unknown and nothing is claimed.
    Normal,
    /// The context is inside the configured thresholds.
    Approaching,
    /// Semantic compaction is required before the next request.
    RolloverRequired,
    /// The window could not be established, or the configured absolute cap was
    /// reached.
    Unknown,
}

impl CompactionState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Normal => "normal",
            Self::Approaching => "approaching",
            Self::RolloverRequired => "rollover_required",
            Self::Unknown => "unknown",
        }
    }
}

/// Decide whether the active context needs to be compacted.
///
/// The trigger is deliberately *not* a fixed percentage of the context window.
/// The window is the wrong denominator: it includes room the prompt can never
/// use, because the response reservation and the safety buffer come out of it.
/// Utilization is therefore measured against the budget that is actually
/// available for prompt-side content, and compared against configurable
/// thresholds on that budget.
pub fn decide(
    config: &CompactionConfig,
    limits: &ModelLimits,
    footprint: &RequestFootprint,
) -> CompactionDecision {
    let budget = budget_for(limits, config);
    let trusted_window = budget.is_trustworthy_window();
    // Everything derived from the budget is computed before the decision is built,
    // so the borrow ends before the budget is moved into it.
    let used = footprint.effective_prompt_tokens();
    let available = budget.available();
    let utilization_bp = budget.utilization_bp(used);
    let headroom = budget.headroom(used);
    let mut decision = CompactionDecision {
        state: CompactionState::Normal,
        required: false,
        utilization_bp,
        headroom,
        reason: String::new(),
        budget,
        footprint: *footprint,
    };
    if !config.enabled {
        decision.state = CompactionState::Disabled;
        decision.reason = "compaction is disabled".to_string();
        return decision;
    }

    // An explicit absolute cap is usable even without a provider window, but it
    // is never applied to an absent token count.
    let absolute = config
        .absolute_compact_tokens
        .filter(|cap| *cap > 0)
        .is_some_and(|cap| footprint.effective_prompt_tokens() >= cap);

    if !trusted_window {
        if absolute {
            decision.state = CompactionState::RolloverRequired;
            decision.required = true;
            decision.reason = format!(
                "the configured absolute cap of {} tokens was reached; no provider window was trusted",
                config
                    .absolute_compact_tokens
                    .unwrap_or_default()
            );
        } else {
            decision.state = CompactionState::Unknown;
            decision.reason =
                "no trustworthy context window was reported; compaction was not attempted".to_string();
        }
        return decision;
    }

    // Overflow is decided by headroom, not by utilization. Utilization is a ratio,
// so it is undefined when the available budget is zero — and a zero available
// budget is the *most* over-budget state there is, not an unknown one. Deciding
// by ratio would silently skip compaction in exactly the case where it is
// mandatory, which is how a session ends up looping on a provider rejection.
let over_budget = available.is_some_and(|available| used > available);
    let state = if absolute || over_budget
        || decision
            .utilization_bp
            .is_some_and(|bp| bp >= config.compact_percent as u64 * 100)
    {
        CompactionState::RolloverRequired
    } else if decision
        .utilization_bp
        .is_some_and(|bp| bp >= config.reduce_percent as u64 * 100)
    {
        CompactionState::Approaching
    } else {
        CompactionState::Normal
    };
    decision.state = state;
    decision.required = state == CompactionState::RolloverRequired;
    decision.reason = describe(state, &decision.budget, used, &decision.footprint);
    decision
}

impl TokenBudget {
    /// Whether a usable prompt ceiling exists.
    pub fn is_trustworthy_window(&self) -> bool {
        self.prompt_ceiling.is_some()
    }
}

fn describe(
    state: CompactionState,
    budget: &TokenBudget,
    used: u64,
    footprint: &RequestFootprint,
) -> String {
    // Every component is named in the reason, so a surprising compaction can be
    // diagnosed without re-deriving the budget by hand.
    let mut reason = format!(
        "prompt {used} tokens (system {}, tools {}, messages {}, tool results {}) against an available {} of {} after reserving {} for output, {} for reasoning and {} for safety",
        footprint.system_prompt,
        footprint.tool_definitions,
        footprint.messages,
        footprint.tool_results,
        budget.available().unwrap_or_default(),
        budget.prompt_ceiling.unwrap_or_default(),
        budget.output_reserve,
        budget.reasoning_reserve,
        budget.safety_buffer
    );
    match state {
        CompactionState::Disabled => reason.push_str("; compaction is disabled"),
        CompactionState::RolloverRequired => reason.push_str("; compaction is required"),
        CompactionState::Approaching => reason.push_str("; deterministic reduction is advised"),
        CompactionState::Normal => reason.push_str("; no action required"),
        CompactionState::Unknown => reason.push_str("; the window is unknown"),
    }
    if footprint.reported_disagrees() {
        reason.push_str("; the provider reported a smaller prompt count than measured, so the measurement was used");
    }
    reason
}

/// Estimated tokens for a serialized request component.
///
/// OCG estimates from bytes rather than shipping a tokenizer; this is the one
/// place that estimate is defined for compaction, so every component is
/// measured the same way. The safety buffer in the budget exists to absorb this
/// approximation.
pub fn estimate_tokens(text: &str) -> u64 {
    (text.len() / 4) as u64
}

/// Estimate the tokens of a serialized JSON value.
pub fn estimate_value_tokens(value: &serde_json::Value) -> u64 {
    estimate_tokens(&value.to_string())
}

/// Measure a request body into a footprint.
///
/// Every field is optional on the wire, and an absent component contributes
/// zero rather than a fabricated estimate.
pub fn measure(
    system_prompt: Option<&str>,
    tools: Option<&serde_json::Value>,
    messages: &[serde_json::Value],
) -> RequestFootprint {
    let system_prompt = system_prompt.map(estimate_tokens).unwrap_or(0);
    let tool_definitions = tools.map(estimate_value_tokens).unwrap_or(0);
    let mut messages_tokens = 0u64;
    let mut tool_results = 0u64;
    for message in messages {
        let tokens = estimate_value_tokens(message);
        messages_tokens = messages_tokens.saturating_add(tokens);
        if crate::compaction::reduction::is_tool_result(message) {
            tool_results = tool_results.saturating_add(tokens);
        }
    }
    RequestFootprint {
        system_prompt,
        tool_definitions,
        messages: messages_tokens,
        tool_results,
        reported_prompt: None,
    }
}

/// Build a footprint from a full request body and an optional provider report.
pub fn measure_request(
    system_prompt: Option<&str>,
    tools: Option<&serde_json::Value>,
    messages: &[serde_json::Value],
    reported_prompt: Option<u64>,
) -> Result<RequestFootprint> {
    Ok(measure(system_prompt, tools, messages).with_reported_tokens(reported_prompt))
}