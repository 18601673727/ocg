//! The semantic compaction layer: rolling summary construction and validation.
//!
//! This is the only layer that asks a model to summarize. Everything it needs
//! is assembled here, and everything it produces is checked here before the
//! durable state advances.
//!
//! ## The rolling contract
//!
//! Compaction is a *fold*, not a series of independent summaries. Each new
//! summary combines two inputs: the previous summary and the messages produced
//! since it. The previous summary is **not** preserved and the new one does not
//! supersede it by editing — the new summary replaces it wholesale, so anything
//! the model fails to carry forward is lost from the active context.
//!
//! That asymmetry is why the combination instructions are explicit about
//! carrying objectives, constraints and directives forward even when the new
//! messages do not mention them, and why the conversation wins on conflict: it
//! is newer than the summary it corrects.
//!
//! ## The canonical boundary
//!
//! Canonical facts — the Job id, generation, Call ids, settled budget, file
//! revisions — are never carried in summary prose. They are re-rendered from
//! canonical state on every request (see [`crate::compaction::canonical`]). The
//! summary prompt therefore includes the canonical block *and* instructs the
//! model to reference it rather than restate it. A summary that renames a
//! canonical identifier is rejected, because a renamed id is not a recoverable
//! reference — the canonical state is the only place the true value exists.

use crate::compaction::config::TranscriptPolicy;
use crate::error::{OcgError, Result};

/// Sections the summary must contain.
///
/// A summary that omits a section cannot be validated as complete, and an
/// incomplete summary that is accepted as success loses history silently. The
/// section list is therefore a contract: validation requires every one.
pub const REQUIRED_SECTIONS: [&str; 8] = [
    "## Objective",
    "## User Constraints",
    "## Completed Work",
    "## Current Work State",
    "## Unresolved",
    "## Next Move",
    "## Relevant Files",
    "## Failures And Causes",
];

/// The template given to the summarizing model.
///
/// It restates the retention priorities in the model's own terms: what must
/// survive for a continuation to make progress.
pub const SUMMARY_TEMPLATE: &str = "\
## Objective
- [what the user is trying to accomplish, in one or two sentences]

## User Constraints
- [explicit constraints, forbidden changes, and required invariants the user stated, or \"(none)\"]

## Completed Work
- [finished and verified work, or \"(none)\"]

## Current Work State
- [what is in progress right now: partial edits, unfinished reasoning, the exact point of interruption, or \"(none)\"]

## Unresolved
- [open questions, unknowns, and unverified assumptions, or \"(none)\"]

## Next Move
- [the immediate concrete next action]

## Relevant Files
- [path: why it matters, with revision when known]

## Failures And Causes
- [important failures and their causes, or \"(none)\"]";

/// The system prompt for the summarization call.
pub const SUMMARY_SYSTEM_PROMPT: &str = "\
You are a context summarization agent. You are given a conversation between a user \
and a coding agent. Your goal is to produce a structured summary matching the format \
specified so another coding agent can continue the work.

Always follow the exact output structure requested. Keep every section, preserve \
exact file paths, identifiers, command lines and error strings when known, and prefer \
terse bullets over paragraphs.

Do not continue the conversation. Do not answer any question in it. Output only the \
structured summary, and nothing about the summarization process itself.";

/// Instructions for folding a previous summary into a new one.
pub const SUMMARY_UPDATE_INSTRUCTIONS: &str = "\
The <prior-summary> summarizes everything before the <conversation>. Produce one new \
summary that combines both. The <prior-summary> is then discarded: anything you do \
not carry into the new summary is lost from the working context.

When combining:
- Carry forward objectives, user constraints, explicit directives, decisions and \
parallel workstreams from the <prior-summary> even when the <conversation> does not \
mention them. Drop only what is finished and no longer needed.
- The <conversation> is more recent than the <prior-summary>. Where they conflict, \
the conversation wins: state the corrected fact and drop the stale claim.
- Add new progress, decisions, constraints and findings from the <conversation>.
- Move work from \"Current Work State\" to \"Completed Work\" when it finishes, and \
the reverse when earlier completion was wrong.
- Update \"Objective\" and \"Next Move\" to reflect the current work state, not the \
state at the start.
- Record every failure that still constrains what can be tried next, with its cause.";

/// Instructions that keep canonical facts out of the summary prose.
pub const CANONICAL_REFERENCE_INSTRUCTIONS: &str = "\
The <canonical> block is OCG's authoritative execution state. It is regenerated \
from canonical state on every request and survives every compaction.

- Reference canonical identifiers by name (job id, generation, Call id, budget \
figures, file revision). Do not restate their values, and never invent, rename or \
recompute one.
- If a value you believe about a canonical fact differs from the <canonical> block, \
the <canonical> block is correct. Note the discrepancy under \"Unresolved\" instead \
of asserting the other value.";

/// Inputs to the summarization prompt.
pub struct SummaryInput<'a> {
    /// The prior summary, when this is a rolling update.
    pub previous_summary: Option<&'a str>,
    /// The serialized messages since the prior summary.
    pub transcript: &'a str,
    /// The canonical state block.
    pub canonical: &'a str,
    /// Names of canonical facts referenced by this compaction.
    pub canonical_refs: &'a [String],
    /// Whether the compaction was triggered by a provider rejection rather than
    /// by the local budget.
    pub overflow: bool,
}

/// Assemble the user prompt for one summarization call.
///
/// The order is deliberate: the canonical block precedes the transcript so it is
/// read as current state rather than as something the conversation claims, and
/// the template comes last so it is the final shape instruction.
pub fn build_prompt(input: &SummaryInput<'_>) -> String {
    let mut prompt = String::new();
    if input.overflow {
        prompt.push_str(
            "The previous request was rejected because it exceeded the provider's context \
limit. Reduce aggressively and prefer concrete facts over narrative.\n\n",
        );
    }
    if !input.canonical.trim().is_empty() {
        prompt.push_str("Here is OCG's authoritative execution state:\n\n<canonical>\n");
        prompt.push_str(input.canonical.trim());
        prompt.push_str("\n</canonical>\n\n");
        prompt.push_str(CANONICAL_REFERENCE_INSTRUCTIONS);
        prompt.push_str("\n\n");
    }
    prompt.push_str("Here is the conversation so far:\n\n<conversation>\n");
    prompt.push_str(input.transcript.trim());
    prompt.push_str("\n</conversation>\n\n");
    match input.previous_summary {
        Some(previous) if !previous.trim().is_empty() => {
            prompt.push_str(
                "Here is the summary of everything before the <conversation> above:\n\n\
                 <prior-summary>\n",
            );
            prompt.push_str(previous.trim());
            prompt.push_str("\n</prior-summary>\n\n");
            prompt.push_str(SUMMARY_UPDATE_INSTRUCTIONS);
            prompt.push_str("\n\n");
        }
        _ => {
            prompt.push_str(
                "Create a new summary from the <conversation> above so another coding \
                 agent can continue the work.\n\n",
            );
        }
    }
    if !input.canonical_refs.is_empty() {
        prompt.push_str("Canonical identifiers referenced by this compaction:\n");
        for reference in input.canonical_refs {
            prompt.push_str("- ");
            prompt.push_str(reference);
            prompt.push('\n');
        }
        prompt.push('\n');
    }
    prompt.push_str(
        "Output exactly the Markdown structure below, keeping the section order. Do not \
         include the template tags.\n\n<template>\n",
    );
    prompt.push_str(SUMMARY_TEMPLATE);
    prompt.push_str("\n</template>");
    prompt
}

/// Why a candidate summary was rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SummaryRejection {
    /// The response was empty or whitespace.
    Empty,
    /// Mandatory sections were missing.
    MissingSections(Vec<&'static str>),
    /// The response exceeded its configured character bound.
    TooLong { chars: usize, max: usize },
    /// A canonical reference was rewritten rather than carried verbatim.
    RewrittenCanonical { reference: String },
}

impl SummaryRejection {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Empty => "empty",
            Self::MissingSections(_) => "missing_sections",
            Self::TooLong { .. } => "too_long",
            Self::RewrittenCanonical { .. } => "rewritten_canonical",
        }
    }
}

impl std::fmt::Display for SummaryRejection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => write!(formatter, "the summary response was empty"),
            Self::MissingSections(sections) => write!(
                formatter,
                "the summary is missing required sections: {}",
                sections.join(", ")
            ),
            Self::TooLong { chars, max } => write!(
                formatter,
                "the summary is {chars} characters, over the {max} character bound"
            ),
            Self::RewrittenCanonical { reference } => write!(
                formatter,
                "the summary did not carry the canonical reference `{reference}` verbatim"
            ),
        }
    }
}

/// Validate a candidate summary before the compaction point advances.
///
/// Rejecting here is the difference between a recoverable failure — the caller
/// retries, or falls back to deterministic reduction — and a silent one, where a
/// truncated summary is accepted as success and the history it failed to carry
/// is gone from the active context for good.
pub fn validate(
    summary: &str,
    max_chars: usize,
    canonical_refs: &[String],
) -> std::result::Result<(), SummaryRejection> {
    if summary.trim().is_empty() {
        return Err(SummaryRejection::Empty);
    }
    if summary.chars().count() > max_chars {
        return Err(SummaryRejection::TooLong {
            chars: summary.chars().count(),
            max: max_chars,
        });
    }
    let missing = missing_sections(summary);
    if !missing.is_empty() {
        return Err(SummaryRejection::MissingSections(missing));
    }
    // A canonical reference that was named must still appear verbatim. A model
    // that paraphrases an id has produced a reference that resolves to nothing.
    for reference in canonical_refs {
        if !summary.contains(reference.as_str()) {
            return Err(SummaryRejection::RewrittenCanonical {
                reference: reference.clone(),
            });
        }
    }
    Ok(())
}

/// The required sections absent from a summary, in template order.
pub fn missing_sections(summary: &str) -> Vec<&'static str> {
    let present = present_sections(summary);
    REQUIRED_SECTIONS
        .iter()
        .copied()
        .filter(|section| !present.contains(&normalize_heading(section)))
        .collect()
}

fn present_sections(summary: &str) -> Vec<String> {
    summary
        .lines()
        // A heading is a line whose first non-space characters are `##`. The
        // marker is detected before any trimming of the *content*, because a
        // model legitimately writes `##Objective` without the space that
        // `strip_prefix("## ")` would require.
        .filter_map(|line| {
            let trimmed = line.trim_start();
            let rest = trimmed.strip_prefix("##")?;
            // `###` is a sub-heading, not a section heading.
            if rest.starts_with('#') {
                return None;
            }
            Some(normalize_heading(rest))
        })
        .collect()
}

/// Normalize a heading for comparison.
///
/// Models vary in casing and whitespace around headings (`##Objective`,
/// `##  Next Move `). Comparing normalized forms keeps validation from failing
/// on typography while still failing on a genuinely absent section.
fn normalize_heading(heading: &str) -> String {
    heading
        .trim()
        .trim_start_matches('#')
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

/// Validate and return a summary, converting a rejection into an [`OcgError`].
pub fn accept(summary: &str, max_chars: usize, canonical_refs: &[String]) -> Result<String> {
    validate(summary, max_chars, canonical_refs).map_err(|rejection| {
        OcgError::config(format!("compaction summary rejected: {rejection}"))
    })?;
    Ok(summary.to_string())
}

/// The continuation message injected after a successful compaction.
///
/// It states what changed and what did not, so the model does not treat the
/// summary as a fresh request or re-derive canonical state.
pub fn continuation_message(overflow: bool) -> String {
    let mut text = String::new();
    if overflow {
        text.push_str(
            "The previous request was rejected because it exceeded the provider's context \
             limit, so the working context has been compacted.\n\n",
        );
    } else {
        text.push_str(
            "The working context has been compacted to stay within the model's context \
             limit. Earlier messages are now represented by a structured summary.\n\n",
        );
    }
    text.push_str(
        "The canonical execution state below is authoritative and was not rewritten. \
         Continue from \"Next Move\" in the summary. If a canonical value contradicts \
         something you believe, trust the canonical state and say so.",
    );
    text
}

/// Serialize messages into the bounded transcript the summarizer reads.
///
/// This is not the wire format. The summarizer needs a legible narrative, so
/// messages are labelled and every payload is bounded, including assistant
/// reasoning — unbounded reasoning is a known way for a compaction transcript
/// to end up larger than the history it replaced.
pub fn transcript(
    messages: &[serde_json::Value],
    previous_summary: Option<&str>,
    policy: &TranscriptPolicy,
) -> String {
    let mut out = String::new();
    if let Some(previous) = previous_summary.filter(|text| !text.trim().is_empty()) {
        out.push_str("[Prior summary]\n");
        out.push_str(previous.trim());
        out.push_str("\n\n");
    }
    let mut tool_calls = 0usize;
    for message in messages {
        let role = message
            .get("role")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        match role {
            "system" => continue,
            "user" => {
                out.push_str("[User]\n");
                if let Some(text) = text_of(message, "content", policy.max_assistant_chars) {
                    out.push_str(&text);
                }
                out.push_str("\n\n");
            }
            "assistant" => {
                if let Some(text) =
                    text_of(message, "reasoning_content", policy.max_reasoning_chars)
                {
                    out.push_str("[Assistant reasoning]\n");
                    out.push_str(&text);
                    out.push_str("\n\n");
                }
                if let Some(calls) = message
                    .get("tool_calls")
                    .and_then(serde_json::Value::as_array)
                {
                    for call in calls {
                        if tool_calls >= policy.max_tool_calls {
                            out.push_str("[Tool calls elided: transcript tool-call cap reached]\n");
                            break;
                        }
                        tool_calls += 1;
                        out.push_str("[Assistant tool call]\n");
                        out.push_str(&call.to_string());
                        out.push_str("\n\n");
                    }
                }
                if let Some(text) = text_of(message, "content", policy.max_assistant_chars) {
                    out.push_str("[Assistant]\n");
                    out.push_str(&text);
                    out.push_str("\n\n");
                }
            }
            "tool" => {
                out.push_str("[Tool result]\n");
                if let Some(text) = text_of(message, "content", policy.max_tool_result_chars) {
                    out.push_str(&text);
                }
                out.push_str("\n\n");
            }
            other => {
                out.push_str(&format!("[{other}]\n"));
                if let Some(text) = text_of(message, "content", policy.max_assistant_chars) {
                    out.push_str(&text);
                }
                out.push_str("\n\n");
            }
        }
    }
    out.trim_end().to_string()
}

fn text_of(message: &serde_json::Value, field: &str, cap: usize) -> Option<String> {
    let text = match message.get(field)? {
        serde_json::Value::String(text) => text.clone(),
        serde_json::Value::Null => return None,
        other => other.to_string(),
    };
    if text.trim().is_empty() {
        return None;
    }
    Some(crate::compaction::reduction::bound_text(&text, cap, field))
}
