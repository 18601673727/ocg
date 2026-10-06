//! Recent-tail retention: what survives compaction verbatim.
//!
//! A summary is lossy by construction, so the messages immediately before the
//! compaction point are kept verbatim. How many is a budget decision, not a
//! count of messages: the same tail that is ample for a small model is a
//! significant fraction of a large one.
//!
//! The budget is derived from the *available* budget rather than the context
//! window, clamped between a floor and a ceiling:
//!
//! ```text
//! tail = clamp(available * ratio_percent / 100, min_budget, max_budget)
//! ```
//!
//! Retention is *turn*-aligned. A turn is a user message and everything the
//! model produced in response to it. Splitting mid-turn is what produces the
//! characteristic post-compaction failure of an assistant message whose tool
//! calls have lost their results: the protocol breaks, and the model either
//! stops calling tools or emits calls as literal text. So the tail always ends
//! at a turn boundary, and a turn that does not fit whole is not partially
//! retained — it is summarized along with the head.

use crate::compaction::config::TailPolicy;
use serde::{Deserialize, Serialize};

/// One user-initiated turn: `[start, end)` message indices.
///
/// A turn starts at a user message. A leading system message is not a turn: it
/// is request framing, not conversation, and it is re-sent every request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Turn {
    pub start: usize,
    pub end: usize,
}

impl Turn {
    pub fn len(&self) -> usize {
        self.end.saturating_sub(self.start)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Identify the turns in a history.
///
/// Consecutive user messages each begin a turn, so a user message that follows
/// a tool result continues the previous turn rather than starting one. The
/// result is always non-empty for a non-empty history, and the turns tile the
/// history from the first non-system message onward.
pub fn turns(messages: &[serde_json::Value]) -> Vec<Turn> {
    let first = messages
        .iter()
        .position(|message| role_of(message) != "system")
        .unwrap_or(messages.len());
    let mut starts = Vec::new();
    for (index, message) in messages.iter().enumerate().skip(first) {
        if role_of(message) == "user" {
            starts.push(index);
        }
    }
    if starts.is_empty() {
        // A history with no user message is one implicit turn, so the boundary
        // logic below still has something to align to.
        return if first < messages.len() {
            vec![Turn {
                start: first,
                end: messages.len(),
            }]
        } else {
            Vec::new()
        };
    }
    starts
        .iter()
        .enumerate()
        .map(|(position, start)| Turn {
            start: *start,
            end: starts.get(position + 1).copied().unwrap_or(messages.len()),
        })
        .collect()
}

fn role_of(message: &serde_json::Value) -> &str {
    message
        .get("role")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
}

/// The tail of a history that is retained verbatim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TailSelection {
    /// First retained message index. Messages before it are summarized.
    pub start: usize,
    /// Number of messages retained verbatim.
    pub len: usize,
    /// Number of complete turns retained.
    pub turns: usize,
    /// The budget the selection was made against.
    pub budget_tokens: u64,
    /// Whether the newest turn alone exceeded the budget and had to be
    /// summarized. Recorded because it is the case where the tail carries the
    /// least information.
    pub newest_turn_exceeds_budget: bool,
}

/// Derive the tail budget from the available budget.
///
/// An absent available budget yields the floor: retaining *something* recent is
/// more useful than retaining nothing, and the floor is small enough to be safe
/// against an unknown window.
pub fn tail_budget(policy: &TailPolicy, available: Option<u64>) -> u64 {
    if let Some(budget) = policy.budget_tokens.filter(|budget| *budget > 0) {
        return budget.min(policy.max_budget_tokens);
    }
    match available {
        Some(available) => {
            let ratio = (available as u128 * policy.ratio_percent as u128 / 100) as u64;
            ratio.clamp(policy.min_budget_tokens, policy.max_budget_tokens)
        }
        None => policy.min_budget_tokens,
    }
}

/// Select the verbatim tail.
///
/// Walks turns newest-first, accumulating until the budget is exhausted, then
/// stops at the turn boundary. `max_turns` bounds how many are considered, which
/// keeps the scan proportional to the retained tail rather than to the whole
/// history — a long history is exactly the case where compaction runs.
///
/// The measurement uses the same estimator as the rest of compaction, so one
/// token budget means one thing across the module.
pub fn select_tail(
    messages: &[serde_json::Value],
    policy: &TailPolicy,
    budget_tokens: u64,
) -> TailSelection {
    let turns = turns(messages);
    if messages.is_empty() || budget_tokens == 0 {
        return TailSelection {
            start: messages.len(),
            len: 0,
            turns: 0,
            budget_tokens,
            newest_turn_exceeds_budget: !messages.is_empty(),
        };
    }
    let Some((&newest, _)) = turns.split_last() else {
        // No turn boundary at all: keep everything, because there is no point at
        // which cutting would preserve a turn's protocol shape.
        return TailSelection {
            start: 0,
            len: messages.len(),
            turns: 0,
            budget_tokens,
            newest_turn_exceeds_budget: false,
        };
    };
    let newest_exceeds = estimate_span(messages, newest) > budget_tokens;
    // A `max_turns` cap keeps the scan proportional to the retained tail.
    let considered = match policy.max_turns {
        Some(max) => turns.len().saturating_sub(max),
        None => 0,
    };
    let mut spent = 0u64;
    let mut start = messages.len();
    let mut retained = 0usize;
    for turn in turns[considered..].iter().rev() {
        let tokens = estimate_span(messages, *turn);
        if spent.saturating_add(tokens) > budget_tokens {
            break;
        }
        spent = spent.saturating_add(tokens);
        retained += turn.len();
        start = turn.start;
    }
    if retained == 0 {
        // The newest turn does not fit. Retaining it verbatim would exceed the
        // budget, and a partial turn would strand assistant tool calls without
        // their results, so it is summarized with the head instead. Recorded on
        // the selection because this is the case where the tail carries the
        // least information and a caller may want to know.
        return TailSelection {
            start: messages.len(),
            len: 0,
            turns: 0,
            budget_tokens,
            newest_turn_exceeds_budget: true,
        };
    }
    TailSelection {
        start,
        len: messages.len().saturating_sub(start),
        turns: turns
            .iter()
            .rev()
            .take_while(|turn| turn.start >= start)
            .count(),
        budget_tokens,
        newest_turn_exceeds_budget: newest_exceeds,
    }
}

/// Estimate the tokens of a message span.
fn estimate_span(messages: &[serde_json::Value], turn: Turn) -> u64 {
    messages[turn.start..turn.end]
        .iter()
        .map(crate::compaction::accounting::estimate_value_tokens)
        .sum()
}
