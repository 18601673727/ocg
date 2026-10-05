//! The active-context projection: the boundary between durable history and the
//! messages a request actually carries.
//!
//! This module owns the ordering problem that makes compaction hard. Given a
//! durable history and a list of compaction points, it decides, for the next
//! request, which messages to send and in what order. Three properties make that
//! decidable:
//!
//! - **Positions, not indices.** A compaction point records transcript
//!   *positions*. The summary does not sit where the messages it replaced were,
//!   so array index in the projection never implies chronology. A caller that
//!   needs chronology reads the recorded positions.
//! - **The durable history is authoritative.** Nothing here mutates or deletes
//!   it. Every projection is a pure function of (history, compaction points,
//!   canonical state, config), so identical inputs rebuild an identical active
//!   context — which is what makes a compaction auditable after the fact.
//! - **Reduction precedes any size decision.** An over-budget context that
//!   reduction can shrink is returned as `Ready` and never reaches the
//!   summarizing model. A summary costs a call and loses meaning, so it is
//!   never paid for while a free fix is available.

use crate::compaction::accounting::{
    budget_for, decide, measure, CompactionState, ModelLimits, RequestFootprint,
};
use crate::compaction::canonical::CanonicalBlock;
use crate::compaction::config::CompactionConfig;
use crate::compaction::reduction::{reduce, ReductionReport};
use crate::compaction::summary::{self, SummaryInput};
use crate::compaction::tail::{select_tail, tail_budget, TailSelection};
use crate::error::{OcgError, Result};
use serde::{Deserialize, Serialize};

/// The durable transcript of one session.
///
/// `messages` is chronological and is never pruned by compaction: this type has
/// no removal operation, only projection. Append-only is what lets a compaction
/// be undone or redone without loss.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct History {
    pub messages: Vec<serde_json::Value>,
}

impl History {
    pub fn new(messages: Vec<serde_json::Value>) -> Self {
        Self { messages }
    }

    pub fn len(&self) -> usize {
        self.messages.len()
    }

    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    /// Append one message to the durable history.
    pub fn push(&mut self, message: serde_json::Value) {
        self.messages.push(message);
    }

    /// The messages after a transcript position.
    pub fn after(&self, position: usize) -> &[serde_json::Value] {
        self.messages.get(position..).unwrap_or(&[])
    }
}

/// A durable compaction point.
///
/// `through` is the exclusive transcript position the summary represents; the
/// summary stands in for every message before it. `tail_start` is the first
/// position retained verbatim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CompactionPoint {
    /// Stable id, derived from the boundary, so replanning the same boundary
    /// produces the same point rather than a duplicate event.
    pub id: String,
    /// The summary standing in for everything before `through`.
    pub summary: String,
    /// Exclusive transcript position the summary covers.
    pub through: usize,
    /// First transcript position retained verbatim.
    pub tail_start: usize,
    /// Canonical references this summary was verified to carry verbatim.
    pub canonical_refs: Vec<String>,
    /// Unix seconds.
    pub created_at: i64,
    /// Whether this compaction followed a provider overflow rejection rather
    /// than a local budget decision.
    pub overflow: bool,
}

impl CompactionPoint {
    /// Whether this point applies to a history of `len` messages.
    ///
    /// A point recorded against a shorter history is not trusted: its positions
    /// would refer to different messages. Rejected points are skipped, not fatal,
    /// so a truncated history can still be served from an earlier point.
    pub fn applies_to(&self, len: usize) -> bool {
        self.through <= len && self.tail_start <= len && self.tail_start >= self.through
    }

    /// Whether the point leaves any verbatim tail.
    pub fn has_tail(&self) -> bool {
        self.tail_start < self.through
    }
}

/// The split of a history into a summarizable head and a verbatim tail.
#[derive(Debug, Clone, PartialEq)]
pub struct Transcript {
    /// Messages the summary will represent.
    pub head: Vec<serde_json::Value>,
    /// Messages retained verbatim.
    pub tail: Vec<serde_json::Value>,
    /// Transcript position where the tail begins.
    pub tail_start: usize,
}

impl Transcript {
    /// The bounded head text for a summarizing call, folded with `previous`.
    pub fn head_text(&self, previous: Option<&str>, config: &CompactionConfig) -> String {
        summary::transcript(&self.head, previous, &config.transcript)
    }
}

/// What the next request should carry.
#[derive(Debug, Clone, PartialEq)]
pub enum CompactionOutcome {
    /// The context fits, or reduction made it fit. Send `messages`.
    Ready {
        messages: Vec<serde_json::Value>,
        footprint: RequestFootprint,
        /// Whether the context is inside the configured warning band.
        approaching: bool,
        /// What deterministic reduction reclaimed, for logs.
        report: ReductionReport,
    },
    /// A summary is required. Issue `prompt` to the summarizing model, then
    /// persist the point returned by [`Context::accept`].
    Compact {
        /// The pending point, with an empty summary until accepted.
        request: CompactionPoint,
        /// Prompt for the summarizing call.
        prompt: String,
        /// Token ceiling for the summary response.
        max_summary_tokens: u64,
        /// The retention decision, for logs.
        tail: TailSelection,
        /// The split the summary will be built from.
        transcript: Transcript,
        /// Why compaction was chosen.
        reason: String,
        /// What deterministic reduction reclaimed before giving up.
        report: ReductionReport,
    },
}

/// The projecting view of one session.
///
/// Holds borrowed state, so it cannot outlive or mutate the history, the
/// recorded points, or the canonical state.
pub struct Context<'a> {
    history: &'a History,
    points: &'a [CompactionPoint],
    canonical: &'a CanonicalBlock,
    config: &'a CompactionConfig,
    limits: &'a ModelLimits,
}

impl<'a> Context<'a> {
    pub fn new(
        history: &'a History,
        points: &'a [CompactionPoint],
        canonical: &'a CanonicalBlock,
        config: &'a CompactionConfig,
        limits: &'a ModelLimits,
    ) -> Self {
        Self {
            history,
            points,
            canonical,
            config,
            limits,
        }
    }

    /// The newest compaction point that applies to the current history.
    ///
    /// Newest-first: a later compaction already folded an earlier one in, so
    /// only the last valid point governs the projection.
    pub fn active_point(&self) -> Option<&'a CompactionPoint> {
        self.points
            .iter()
            .rev()
            .find(|point| point.applies_to(self.history.len()))
    }

    /// Evaluate the context and decide what the next request carries.
    ///
    /// The single entry point a caller needs: measure, decide, reduce, and
    /// either return the messages to send or the summarizing request to issue.
    pub fn evaluate(
        &self,
        system_prompt: Option<&str>,
        tools: Option<&serde_json::Value>,
        reported_prompt: Option<u64>,
    ) -> Result<CompactionOutcome> {
        let point = self.active_point();
        let (messages, report) = self.project(point)?;
        let footprint =
            measure(system_prompt, tools, &messages).with_reported_tokens(reported_prompt);
        let decision = decide(self.config, self.limits, &footprint);
        let approaching = decision.state == CompactionState::Approaching;

        if !decision.required {
            return Ok(CompactionOutcome::Ready {
                messages,
                footprint,
                approaching,
                report,
            });
        }

        // Reduction already ran during projection. If the context still does not
        // fit, the remaining option is a summary — there is no second free fix
        // to try, and looping on it would compact a history that is already as
        // small as reduction can make it.
        self.plan(decision.reason, report)
    }

    /// Project the active context: canonical block, then summary, then tail.
    ///
    /// The canonical block comes first so it is read as current state, and the
    /// summary follows it as the model-visible account of the work. The tail is
    /// last, unchanged, so the most recent exchange is still the most recent
    /// thing in the request.
    fn project(
        &self,
        point: Option<&CompactionPoint>,
    ) -> Result<(Vec<serde_json::Value>, ReductionReport)> {
        let refs = self.canonical.references();
        let mut messages = vec![self.canonical.to_message()];
        // Reduction must never rewrite the two messages this module constructed:
        // the canonical block is authority, and the summary is the compaction
        // product. Both are protected by counting them in the recent window.
        let mut protected_prefix = 1usize;
        if let Some(point) = point {
            messages.push(summary_message(point));
            protected_prefix += 1;
            messages.extend_from_slice(self.history.after(point.tail_start));
        } else {
            messages.extend(self.history.messages.iter().cloned());
        }
        let (reduced, report) =
            reduce_within(&messages, &self.config.reduce, &refs, protected_prefix)?;
        Ok((reduced, report))
    }

    /// Build the summarizing request.
    fn plan(&self, reason: String, report: ReductionReport) -> Result<CompactionOutcome> {
        let point = self.active_point();
        let previous = point.map(|point| point.summary.as_str());
        // A rolling compaction summarizes only what is new: the previous point's
        // `through` is where the last summary stopped.
        let already_summarized = point.map(|point| point.through).unwrap_or(0);
        let budget = budget_for(self.limits, self.config);
        let tail_budget = tail_budget(&self.config.tail, budget.available());
        let pending = self.history.after(already_summarized);
        let selection = select_tail(pending, &self.config.tail, tail_budget);
        let tail_start = already_summarized.saturating_add(selection.start);

        // There is nothing left to summarize when the pending range is empty.
        // The previous summary plus the retained tail already exceed the budget,
        // and summarizing again would only discard the summary. Reported rather
        // than attempted, because retrying cannot succeed.
        if tail_start <= already_summarized {
            return Err(OcgError::config(
                "compaction is required but no history remains to summarize; the previous \
                 summary and retained tail already exceed the budget",
            ));
        }

        let transcript = Transcript {
            head: pending[..selection.start].to_vec(),
            tail: pending[selection.start..].to_vec(),
            tail_start,
        };
        let refs = self.canonical.references();
        let transcript_text = transcript.head_text(previous, self.config);
        let overflow = point.map(|point| point.overflow).unwrap_or(false);
        let prompt = summary::build_prompt(&SummaryInput {
            previous_summary: previous,
            transcript: &transcript_text,
            canonical: &self.canonical.render(),
            canonical_refs: &refs,
            overflow,
        });
        let request = CompactionPoint {
            id: compaction_id(self.history.len(), tail_start, &self.config.fingerprint()),
            summary: String::new(),
            through: tail_start,
            tail_start,
            canonical_refs: refs,
            created_at: now(),
            overflow,
        };
        Ok(CompactionOutcome::Compact {
            request,
            prompt,
            max_summary_tokens: self.config.summary.max_summary_tokens,
            tail: selection,
            transcript,
            reason,
            report,
        })
    }

    /// Validate a candidate summary and return the point to persist.
    ///
    /// Validation lives here rather than at the send site so a malformed or
    /// lossy summary can never advance the durable boundary. Both checks matter:
    /// the section contract catches a truncated response, and the canonical
    /// check catches a summary that dropped or renamed a reference.
    pub fn accept(&self, request: &CompactionPoint, candidate: &str) -> Result<CompactionPoint> {
        let text = summary::accept(
            candidate,
            self.config.summary.max_summary_chars,
            &request.canonical_refs,
        )?;
        self.canonical.verify_summary(&text)?;
        let mut accepted = request.clone();
        accepted.summary = text;
        Ok(accepted)
    }

    /// The continuation message to append after a successful compaction.
    pub fn continuation(&self, overflow: bool) -> serde_json::Value {
        serde_json::json!({
            "role": "user",
            "content": [{
                "type": "text",
                "text": summary::continuation_message(overflow),
            }],
        })
    }
}

fn summary_message(point: &CompactionPoint) -> serde_json::Value {
    serde_json::json!({
        "role": "user",
        "content": [{
            "type": "text",
            "text": format!(
                "Summary of the work before this point. The full transcript is retained \
                 durably and can be re-read if a specific detail is missing here:\n\n{}",
                point.summary
            ),
        }],
        "ocg_compaction_id": point.id,
    })
}

/// Reduce a projected context while protecting a leading window.
///
/// `protect_prefix` messages at the front are never rewritten: the caller
/// constructed them and they are authoritative. The policy's own recent-message
/// window still applies to the tail of the projection, so the most recent
/// exchange is also untouched.
fn reduce_within(
    messages: &[serde_json::Value],
    policy: &crate::compaction::config::ReducePolicy,
    refs: &[String],
    protect_prefix: usize,
) -> Result<(Vec<serde_json::Value>, ReductionReport)> {
    let (reducible, rest) = messages.split_at(protect_prefix.min(messages.len()));
    let (reduced_rest, report) = reduce(rest, policy, refs)?;
    let mut out = reducible.to_vec();
    out.extend(reduced_rest);
    Ok((out, report))
}

/// A stable id for a compaction boundary.
///
/// Derived from the boundary and the policy fingerprint, so replanning the same
/// boundary yields the same id. That makes a compaction deduplicable instead of
/// a fresh event every time it is planned.
fn compaction_id(len: usize, tail_start: usize, policy_fingerprint: &str) -> String {
    let digest = crate::hash::sha256_hex(
        format!("ocg-compaction-v1|{len}|{tail_start}|{policy_fingerprint}").as_bytes(),
    );
    format!("cmp-{}", &digest[..24])
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs() as i64)
        .unwrap_or_default()
}
