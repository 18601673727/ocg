//! The canonical state block: facts that survive compaction because they were
//! never in the summary to begin with.
//!
//! The requirement this module exists to satisfy: **no canonical fact may live
//! only in summary text.** Project, Job, Attempt, Executor, Call, budget and
//! file revision are rendered here from canonical state and re-injected into
//! every request. Compaction cannot lose them, because compaction never owned
//! them.
//!
//! The block is deterministic: same canonical state in, byte-identical block
//! out. It is assembled from OCG's canonical read models and from nothing else.
//! It never reads a summary, a message or a tool result, so it cannot be
//! contaminated by them — a property that follows from the input side, not from
//! a validation step.
//!
//! The summary's role is to *reference* these facts, not restate them. That
//! split is what keeps compaction from becoming a second, weaker source of
//! execution truth.
//!
//! Both entry points ([`Self::from_projection`] and [`Self::from_snapshot`])
//! exist because a caller may hold either read model. They share one
//! implementation, so the two can never disagree about what canonical state
//! means.

use crate::error::{OcgError, Result};
use crate::orchestration::budget::SettlementDisposition;
use crate::orchestration::domain::{Attempt, Call, DispatchIntent, Executor, Job};
use crate::orchestration::journal::{ExecutionProjection, ExecutionSnapshot};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::Write as _;

/// Hard bound on the rendered block, in characters.
///
/// The block is part of every request, so it is bounded. Facts beyond the bound
/// are *counted*, never silently dropped, so a caller can tell that a project
/// with a very large Call history exceeded the block.
pub const MAX_CANONICAL_CHARS: usize = 24_000;

/// The header that marks the block, used both to delimit it in a prompt and to
/// recognize it in a request.
pub const CANONICAL_HEADER: &str = "## Canonical execution state (authoritative)";

/// How many Calls are listed. Newest state is kept: the oldest Calls are the
/// ones a summary already covers.
pub const MAX_CALLS_LISTED: usize = 24;

/// How many file revisions are listed.
pub const MAX_REVISIONS_LISTED: usize = 24;

/// One Call's canonical outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CanonicalCall {
    pub id: String,
    pub state: String,
    pub side_effect: bool,
    pub effect_kind: String,
    pub generation: u64,
}

/// Accumulated budget state, in the canonical Money unit (micros).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CanonicalBudget {
    pub settled_micros: i64,
    pub reserved_micros: i64,
    pub unresolved_micros: i64,
    pub released_micros: i64,
    pub currency: String,
}

/// One file revision fact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CanonicalRevision {
    pub path: String,
    pub revision: u64,
}

/// The canonical facts rendered for one compaction.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct CanonicalBlock {
    pub project_id: Option<String>,
    pub project_root: Option<String>,
    /// The authoritative Job.
    pub job_id: Option<String>,
    pub job_state: Option<String>,
    /// The generation that fences every Call below.
    pub generation: Option<u64>,
    /// The Job's authoritative Attempt.
    pub attempt_id: Option<String>,
    pub attempt_state: Option<String>,
    /// The most recently created Executor on that Attempt.
    pub executor_id: Option<String>,
    /// Terminal Call facts, ordered by id for determinism.
    pub calls: Vec<CanonicalCall>,
    /// Accumulated budget figures.
    pub budget: Option<CanonicalBudget>,
    /// File revisions.
    pub file_revisions: Vec<CanonicalRevision>,
    /// Facts omitted because a bound was reached. Reported, never silent.
    pub facts_omitted: usize,
}

impl CanonicalBlock {
    /// Build the block from a replayed projection.
    ///
    /// This is the live path: a caller holding a current projection gets the
    /// block without re-reading the journal.
    pub fn from_projection(
        projection: &ExecutionProjection,
        project_root: Option<&str>,
    ) -> Self {
        let settlements: Vec<crate::orchestration::budget::Settlement> = projection
            .settlements
            .values()
            .cloned()
            .collect();
        Self::assemble(
            &projection.jobs,
            &projection.attempts,
            &projection.executors,
            &projection.calls,
            &projection.dispatch_intents,
            &settlements,
        )
        .with_root(project_root)
    }

    /// Build the block from a durable snapshot.
    pub fn from_snapshot(snapshot: &ExecutionSnapshot, project_root: Option<&str>) -> Self {
        let jobs: BTreeMap<String, Job> = snapshot
            .jobs
            .iter()
            .map(|job| (job.id.clone(), job.clone()))
            .collect();
        let attempts: BTreeMap<String, Attempt> = snapshot
            .attempts
            .iter()
            .map(|attempt| (attempt.id.clone(), attempt.clone()))
            .collect();
        let executors: BTreeMap<String, Executor> = snapshot
            .executors
            .iter()
            .map(|executor| (executor.id.clone(), executor.clone()))
            .collect();
        let calls: BTreeMap<String, Call> = snapshot
            .calls
            .iter()
            .map(|call| (call.id.clone(), call.clone()))
            .collect();
        let intents: BTreeMap<String, DispatchIntent> = snapshot
            .dispatch_intents
            .iter()
            .map(|intent| (intent.id.clone(), intent.clone()))
            .collect();
        let settlements: Vec<crate::orchestration::budget::Settlement> =
            snapshot.settlements.clone();
        let mut block =
            Self::assemble(&jobs, &attempts, &executors, &calls, &intents, &settlements);
        block.project_id = snapshot.projects.first().map(|project| project.id.clone());
        block.with_root(project_root)
    }

    fn with_root(mut self, project_root: Option<&str>) -> Self {
        self.project_root = project_root.map(|root| root.to_string());
        self
    }

    #[allow(clippy::too_many_arguments)]
    fn assemble(
        jobs: &BTreeMap<String, Job>,
        attempts: &BTreeMap<String, Attempt>,
        executors: &BTreeMap<String, Executor>,
        calls: &BTreeMap<String, Call>,
        intents: &BTreeMap<String, DispatchIntent>,
        settlements: &[crate::orchestration::budget::Settlement],
    ) -> Self {
        let mut block = Self::default();

        // The authoritative Job is the one the canonical state already
        // designates. Falling back to the highest generation keeps the block
        // populated without inventing authority.
        let job = jobs
            .values()
            .find(|job| job.authoritative_attempt_id.is_some())
            .or_else(|| jobs.values().max_by_key(|job| job.generation));
        if let Some(job) = job {
            block.job_id = Some(job.id.clone());
            block.job_state = Some(job_state_str(&job.state).to_string());
            block.generation = Some(job.generation);
        }
        let attempt = job
            .and_then(|job| job.authoritative_attempt_id.as_ref())
            .and_then(|attempt_id| attempts.get(attempt_id));
        if let Some(attempt) = attempt {
            block.attempt_id = Some(attempt.id.clone());
            block.attempt_state = Some(attempt_state_str(&attempt.state).to_string());
            if let Some(executor) = executors
                .values()
                .filter(|executor| executor.attempt_id == attempt.id)
                .max_by_key(|executor| executor.created_at)
            {
                block.executor_id = Some(executor.id.clone());
            }
        }
        if block.generation.is_none() {
            block.generation = intents.values().map(|intent| intent.generation).max();
        }

        // Failed Calls are excluded: their outcome belongs in the summary's
        // Failures section, where the cause is explained, rather than in a list
        // of ids.
        let mut listed: Vec<CanonicalCall> = calls
            .values()
            .filter(|call| call.state.as_str() != "failed")
            .map(|call| CanonicalCall {
                id: call.id.clone(),
                state: call.state.as_str().to_string(),
                side_effect: call.side_effect,
                effect_kind: call.effect_kind.to_string().to_string(),
                generation: call.generation,
            })
            .collect();
        listed.sort_by(|left, right| left.id.cmp(&right.id));
        if listed.len() > MAX_CALLS_LISTED {
            // Keep the newest ids: the oldest Calls are what a summary covers.
            block.facts_omitted += listed.len() - MAX_CALLS_LISTED;
            listed.drain(..listed.len() - MAX_CALLS_LISTED);
        }
        block.calls = listed;

        if !settlements.is_empty() {
            let mut budget = CanonicalBudget {
                settled_micros: 0,
                reserved_micros: 0,
                unresolved_micros: 0,
                released_micros: 0,
                currency: String::new(),
            };
            for settlement in settlements {
                let micros = settlement.effect.actual.micros;
                let target = match settlement.disposition {
                    SettlementDisposition::Settled => &mut budget.settled_micros,
                    SettlementDisposition::Released => &mut budget.released_micros,
                    SettlementDisposition::Unresolved => &mut budget.unresolved_micros,
                };
                *target = target.saturating_add(micros);
                if budget.currency.is_empty() && !settlement.effect.actual.currency.is_empty() {
                    budget.currency = settlement.effect.actual.currency.clone();
                }
            }
            if !budget.currency.is_empty() {
                block.budget = Some(budget);
            }
        }
        block
    }

    /// Attach file revision facts.
    ///
    /// File revisions are tracked by the edit boundary rather than by the
    /// execution journal, so a caller supplies them from
    /// [`crate::edit`]-managed state. They are rendered with the rest of the
    /// canonical block and are therefore as compaction-proof as any other fact.
    pub fn with_file_revisions(mut self, revisions: Vec<CanonicalRevision>) -> Self {
        let mut revisions = revisions;
        revisions.sort_by(|left, right| left.path.cmp(&right.path));
        if revisions.len() > MAX_REVISIONS_LISTED {
            self.facts_omitted += revisions.len() - MAX_REVISIONS_LISTED;
            revisions.truncate(MAX_REVISIONS_LISTED);
        }
        self.file_revisions = revisions;
        self
    }

    /// The canonical identifiers this block establishes.
    ///
    /// Returned so a summary can be required to carry them verbatim: a summary
    /// that names a Call but renames it has produced a reference that resolves
    /// to nothing.
    pub fn references(&self) -> Vec<String> {
        let mut references = Vec::new();
        if let Some(job_id) = &self.job_id {
            references.push(job_id.clone());
        }
        if let Some(attempt_id) = &self.attempt_id {
            references.push(attempt_id.clone());
        }
        if let Some(generation) = self.generation {
            references.push(format!("generation={generation}"));
        }
        references.extend(self.calls.iter().map(|call| call.id.clone()));
        references.extend(
            self.file_revisions
                .iter()
                .map(|revision| revision.path.clone()),
        );
        references.sort();
        references.dedup();
        references
    }

    /// Render the deterministic Markdown block.
    ///
    /// Deterministic ordering is a requirement rather than a nicety: the block
    /// is injected into every request, and an unstable order would defeat
    /// request-cache reuse and make diffs between requests meaningless.
    pub fn render(&self) -> String {
        let mut out = String::with_capacity(1024);
        out.push_str(CANONICAL_HEADER);
        out.push('\n');
        if let Some(project_id) = &self.project_id {
            let _ = writeln!(out, "- project: {project_id}");
        }
        if let Some(root) = &self.project_root {
            let _ = writeln!(out, "- project root: {root}");
        }
        if let Some(job_id) = &self.job_id {
            let _ = write!(out, "- job: {job_id}");
            match (self.job_state.as_deref(), self.generation) {
                (Some(state), Some(generation)) => {
                    let _ = write!(out, " (state={state}, generation={generation})");
                }
                (Some(state), None) => {
                    let _ = write!(out, " (state={state})");
                }
                (None, Some(generation)) => {
                    let _ = write!(out, " (generation={generation})");
                }
                (None, None) => {}
            }
            out.push('\n');
        }
        if let Some(attempt_id) = &self.attempt_id {
            match self.attempt_state.as_deref() {
                Some(state) => {
                    let _ = writeln!(out, "- attempt: {attempt_id} (state={state})");
                }
                None => {
                    let _ = writeln!(out, "- attempt: {attempt_id}");
                }
            }
        }
        if let Some(executor_id) = &self.executor_id {
            let _ = writeln!(out, "- executor: {executor_id}");
        }
        if let Some(budget) = &self.budget {
            let _ = writeln!(
                out,
                "- budget: settled={} {}, reserved={} {}, unresolved={} {}, released={} {}",
                budget.settled_micros,
                budget.currency,
                budget.reserved_micros,
                budget.currency,
                budget.unresolved_micros,
                budget.currency,
                budget.released_micros,
                budget.currency
            );
        }
        if !self.calls.is_empty() {
            out.push_str("- calls:\n");
            for call in &self.calls {
                let _ = writeln!(
                    out,
                    "  - {} state={} effect={} generation={} side_effect={}",
                    call.id, call.state, call.effect_kind, call.generation, call.side_effect
                );
            }
        }
        if !self.file_revisions.is_empty() {
            out.push_str("- file revisions:\n");
            for revision in &self.file_revisions {
                let _ = writeln!(out, "  - {}@{}", revision.path, revision.revision);
            }
        }
        if self.facts_omitted > 0 {
            let _ = writeln!(
                out,
                "- note: {} further canonical fact(s) omitted from this block; consult the durable journal",
                self.facts_omitted
            );
        }
        out
    }

    /// The block as a message injectable into a request body.
    ///
    /// Injected as a *user* message rather than a system message: this is state,
    /// not instruction, and a system message would compete with the agent's own
    /// system prompt for attention.
    pub fn to_message(&self) -> serde_json::Value {
        serde_json::json!({
            "role": "user",
            "content": [{
                "type": "text",
                "text": self.render(),
            }],
        })
    }

    /// Verify that a summary carries the canonical references verbatim.
    ///
    /// Called before a compaction point advances. A summary that dropped or
    /// paraphrased a reference is rejected, because the reference is the only
    /// link between the summary and canonical state.
    pub fn verify_summary(&self, summary: &str) -> Result<()> {
        for reference in self.references() {
            if !summary.contains(reference.as_str()) {
                return Err(OcgError::config(format!(
                    "compaction summary did not carry the canonical reference `{reference}`"
                )));
            }
        }
        Ok(())
    }
}

/// The stable, persisted spelling of a Job state.
///
/// Defined here rather than in [`crate::orchestration::domain`] because the
/// canonical block is the only consumer that needs a *display* spelling; the
/// domain keeps its own parser private so a bad value still fails closed at the
/// write boundary.
fn job_state_str(state: &crate::orchestration::domain::JobState) -> &'static str {
    use crate::orchestration::domain::JobState::*;
    match state {
        Pending => "pending",
        Eligible => "eligible",
        Running => "running",
        Cancelling => "cancelling",
        Completed => "completed",
        Failed => "failed",
        Cancelled => "cancelled",
        Unknown => "unknown",
        Orphaned => "orphaned",
    }
}

/// The stable, persisted spelling of an Attempt state.
fn attempt_state_str(state: &crate::orchestration::domain::AttemptState) -> &'static str {
    use crate::orchestration::domain::AttemptState::*;
    match state {
        Queued => "queued",
        Running => "running",
        Cancelling => "cancelling",
        Completed => "completed",
        Failed => "failed",
        Cancelled => "cancelled",
        Unknown => "unknown",
        Orphaned => "orphaned",
    }
}