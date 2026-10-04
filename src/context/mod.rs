//! Deterministic, local, project-agnostic context engine.
//!
//! The engine prepares a conservative repository context for OCG:
//! a repo map, an incremental symbol index, a git diff summary, a ranked
//! context plan and a versioned task capsule. It is:
//!
//! - **local only** — no network, no telemetry, no paid APIs;
//! - **deterministic** — stable ordering, integer scores, no randomness;
//! - **conservative** — sensitive files are never read, missing data is an
//!   explicit state, and git is optional;
//! - **fail-soft** — a cache or capture failure warns and keeps the computed
//!   plan; it never blocks the explicit `ocg context` path.

pub mod cache;
pub mod capsule;
pub mod classify;
pub mod config;
pub mod engine;
pub mod freshness;
pub mod fulltext;
pub mod gitdiff;
pub mod index;
pub mod ranking;
pub mod repomap;
pub mod symbols;

pub use config::ContextConfig;
pub use engine::{ContextEngine, PlanOutcome, Prepared};

use crate::clock::{Clock, SystemClock};
use crate::config::Effective;
use crate::process::{GitHost, SystemGitHost};
use std::path::Path;

/// Explicit context preparation helper for callers that want to warm the index
/// (the library API and tests). It is deliberately **not** called by an
/// ordinary `ocg` / `ocg run` launch, which never builds the index; optional
/// orchestration answers bridge calls from its own plan without warming it.
///
/// Returns human-readable warnings. A broken or disabled context subsystem only
/// produces a warning (or nothing at all when it is disabled).
pub fn prepare_explicit(root: &Path, effective: &Effective) -> Vec<String> {
    prepare_explicit_with(root, effective, &SystemGitHost, &SystemClock)
}

/// Injectable variant used by tests.
pub fn prepare_explicit_with(
    root: &Path,
    effective: &Effective,
    git: &dyn GitHost,
    clock: &dyn Clock,
) -> Vec<String> {
    let config = match ContextConfig::from_config(&effective.data) {
        Ok(config) => config,
        Err(error) => return vec![format!("context disabled: {error}")],
    };
    if !config.enabled {
        return Vec::new();
    }
    let engine = ContextEngine::new(root, config, git, clock);
    match engine.prepare() {
        Ok(_) => Vec::new(),
        Err(error) => vec![format!("context preparation skipped: {error}")],
    }
}

/// A deterministic, human-readable plan renderer used by `ocg context`.
pub fn plan_text(plan: &ranking::ContextPlan) -> String {
    let mut out = String::new();
    out.push_str("OCG context plan\n");
    out.push_str(&format!("task:        {}\n", plan.task));
    if let Some(role) = &plan.role {
        out.push_str(&format!("role:        {role}\n"));
    }
    if plan.git.is_repo {
        out.push_str(&format!(
            "git:         {} ({}), {} changed path(s)\n",
            plan.git.branch.as_deref().unwrap_or("detached"),
            plan.git
                .head
                .as_deref()
                .map(|head| head.chars().take(12).collect::<String>())
                .unwrap_or_else(|| "no commits".to_string()),
            plan.changed_paths.len()
        ));
    } else {
        out.push_str("git:         not a git repository\n");
        out.push_str(&format!(
            "changed:     {} path(s)\n",
            plan.changed_paths.len()
        ));
    }
    out.push_str(&format!(
        "candidates:  {} ranked, {} selected\n",
        plan.candidates.len(),
        plan.selected_files.len()
    ));
    out.push_str(&format!(
        "content:     {} bytes ({} estimated tokens, estimate only)\n",
        plan.selected_bytes, plan.estimated_tokens
    ));
    out.push_str(&format!(
        "cache:       {}\n",
        match (plan.provenance.validated, plan.provenance.stale) {
            (false, _) => "unvalidated",
            (true, false) => "fresh (validated)",
            (true, true) => "stale",
        }
    ));
    if plan.truncated {
        out.push_str("truncated:   yes (see notes)\n");
    }
    out.push_str("\nselected files:\n");
    for path in &plan.selected_files {
        out.push_str(&format!("  {path}\n"));
    }
    if !plan.candidates.is_empty() {
        out.push_str("\ntop candidates:\n");
        for candidate in plan.candidates.iter().take(10) {
            let reasons = if candidate.reasons.is_empty() {
                "-".to_string()
            } else {
                candidate.reasons.join(",")
            };
            out.push_str(&format!(
                "  {:>4}  {}{}  [{}]\n",
                candidate.score,
                candidate.path,
                if candidate.selected { " *" } else { "" },
                reasons
            ));
        }
    }
    if !plan.policy.files.is_empty() {
        out.push_str(&format!(
            "\nproject policy: {}\n",
            plan.policy.files.join(", ")
        ));
    }
    if !plan.capabilities.enabled {
        out.push_str("capabilities: disabled\n");
    } else if !plan.capabilities.capabilities.is_empty() {
        let allowed: Vec<String> = plan
            .capabilities
            .capabilities
            .iter()
            .map(|entry| entry.capability.name())
            .collect();
        out.push_str(&format!("capabilities: {}\n", allowed.join(", ")));
    }
    if let Some(proposal) = &plan.test_proposal {
        out.push_str(&format!(
            "targeted tests: {} candidate(s), complete=false, fallback={}\n",
            proposal.candidates.len(),
            proposal.fallback.as_deref().unwrap_or("none")
        ));
    }
    out.push_str(&format!(
        "verification: stage={} ({})\n",
        plan.verification.default_stage, plan.verification.note
    ));
    if !plan.notes.is_empty() {
        out.push_str("\nnotes:\n");
        for note in &plan.notes {
            out.push_str(&format!("  {note}\n"));
        }
    }
    out
}
