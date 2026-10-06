//! Session compaction: keeping the active context inside the model's window
//! without deleting the durable record of what happened.
//!
//! ## The separation this module rests on
//!
//! There are two different stores and they are not the same thing:
//!
//! ```text
//! durable history  ── append-only, never rewritten, never pruned by compaction
//!        │
//!        │  compaction changes only this projection
//!        ▼
//! active context   ── what the next request actually carries
//! ```
//!
//! Compaction never deletes a message from the durable history. It chooses a
//! *boundary* and produces the messages below it: a semantic summary for
//! everything before the boundary, the recent tail verbatim after it, and a
//! regenerated canonical state block. Because the history is intact, a
//! compaction can be redone, audited, or discarded without loss.
//!
//! ## The two layers, and why there are two
//!
//! Reduction runs first and is deterministic. It bounds payloads, collapses
//! repeated reads, and drops read results that a later edit superseded. It costs
//! no model call and loses no meaning, so it must get the chance to run before
//! anything irreversible.
//!
//! Semantic compaction runs only when reduction has been given a chance and the
//! context still does not fit. It costs a model call and it is lossy by
//! construction, so it is the last resort rather than the first move.
//!
//! ```text
//! measure ──► decide ──► reduce (deterministic) ──► still too large?
//!                                                        │
//!                                                  no ───┴─── yes
//!                                                       │
//!                                            summarize head, keep recent tail
//! ```
//!
//! ## What compaction is not allowed to do
//!
//! It is not allowed to be the only place a canonical fact exists. Project, Job,
//! Attempt, Executor, Call, budget, Call result and file revision live in
//! canonical state and are re-rendered on every request (see
//! [`canonical`]). The summary references them; it never carries them. That is
//! what makes a lossy layer safe to run repeatedly.
//!
//! ## The boundary
//!
//! A compaction point is durable and ordered. It records the transcript
//! position it covers, the tail boundary, and the summary that represents
//! everything before it. The next compaction folds the previous summary
//! forward, so summaries accumulate rather than restart — see [`summary`] for
//! the rolling contract and the reason the previous summary is discarded rather
//! than kept alongside.

pub mod accounting;
pub mod canonical;
pub mod config;
pub mod projection;
pub mod reduction;
pub mod summary;
pub mod tail;

pub use accounting::{
    budget_for, decide, estimate_tokens, estimate_value_tokens, measure, measure_request,
    CompactionDecision, CompactionState, ModelLimits, RequestFootprint, TokenBudget,
};
pub use canonical::{CanonicalBlock, CanonicalBudget, CanonicalCall, CanonicalRevision};
pub use config::{
    CompactionConfig, ReducePolicy, SummaryPolicy, TailPolicy, TranscriptPolicy,
    DEFAULT_COMPACT_PERCENT, DEFAULT_MAX_SUMMARY_TOKENS, DEFAULT_REDUCE_PERCENT,
    DEFAULT_SAFETY_BUFFER, DEFAULT_TAIL_MAX_TOKENS, DEFAULT_TAIL_MIN_TOKENS,
};
pub use projection::{CompactionOutcome, CompactionPoint, Context, History, Transcript};
pub use reduction::{reduce, ReductionKind, ReductionRecord, ReductionReport};
pub use summary::{build_prompt, transcript, SummaryInput, SummaryRejection};
pub use tail::{select_tail, tail_budget, TailSelection, Turn};
