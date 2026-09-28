//! Deterministic role projections from the rich context to a hand-off capsule.
//!
//! The projection is the security and size boundary: it is the last place that
//! sees the rich state before a model does. It therefore:
//!
//! - sanitizes **every** field (task, goal, constraints, findings, files,
//!   symbols, decisions, verification, failures, evidence, refs and git
//!   strings), dropping secret-shaped content and sensitive paths;
//! - drops a selected source slice whose *content* looks secret-shaped even if
//!   its path is innocuous;
//! - projects per destination: fields the destination role does not need are
//!   cleared deterministically, so an Explore hand-off cannot leak verification
//!   failures and a Debug hand-off cannot leak exploratory narrative;
//! - keeps required fields (goal, hard constraints, critical findings, changed
//!   files, relevant symbols, failing locations) at every size level;
//! - reduces optional material in a fixed order until the capsule fits both the
//!   absolute byte cap and the configured fraction of the rich source; and
//! - records what it dropped so the compaction is never silent.

use crate::context::capsule::CapsuleFile;
use crate::context::classify;
use crate::context::ranking::terms;
use crate::context::symbols::SymbolRef;
use crate::orchestration::config::OrchestrationConfig;
use crate::orchestration::handoff::{
    projection_id, HandoffFinding, HandoffVerification, ModelHandoffCapsule, ProjectionInput, Role,
    Severity, Transition, HANDOFF_SCHEMA_VERSION,
};
use crate::telemetry::task::{is_secret_like, redact};
use crate::verification::distill::SourceLocation;
use std::path::Path;

/// The size limits a projection must satisfy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProjectionLimits {
    pub max_bytes: usize,
    pub ratio_percent: usize,
}

impl ProjectionLimits {
    pub fn from_config(config: &OrchestrationConfig) -> Self {
        Self {
            max_bytes: config.max_handoff_bytes,
            ratio_percent: config.max_handoff_ratio_percent,
        }
    }

    /// The effective cap for a rich source of `rich_bytes`. Never larger than
    /// the absolute cap; `rich_bytes == 0` disables only the ratio part.
    pub fn effective_cap(&self, rich_bytes: usize) -> usize {
        if rich_bytes == 0 {
            return self.max_bytes;
        }
        let ratio = rich_bytes.saturating_mul(self.ratio_percent) / 100;
        ratio.min(self.max_bytes)
    }
}

/// The named schema for one role transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionSchema {
    pub source: Role,
    pub destination: Role,
    pub required: Vec<&'static str>,
    pub optional: Vec<&'static str>,
}

impl ProjectionSchema {
    pub fn id(&self) -> String {
        projection_id(self.source, self.destination)
    }

    pub fn has_required(&self, field: &str) -> bool {
        self.required.contains(&field)
    }
}

/// The projection schema for a transition. The names are stable and part of the
/// contract, not decoration.
pub fn projection_schema(source: Role, destination: Role) -> ProjectionSchema {
    use Role::*;
    let (required, optional): (Vec<&'static str>, Vec<&'static str>) = match (source, destination) {
        (Lead, Explore | ExploreDeep) => (
            vec![
                "task",
                "goal",
                "changed_files",
                "relevant_symbols",
                "critical_findings",
            ],
            vec!["constraints", "findings", "files", "symbols"],
        ),
        (Explore | ExploreDeep, Build) => (
            vec![
                "task",
                "goal",
                "hard_constraints",
                "critical_findings",
                "changed_files",
                "relevant_symbols",
            ],
            vec!["findings", "files", "symbols", "decisions", "verification"],
        ),
        (Build, Verify) => (
            vec!["task", "goal", "verification", "changed_files"],
            vec!["files", "symbols", "evidence"],
        ),
        (_, Debug) => (
            vec![
                "task",
                "failures",
                "evidence",
                "raw_log_refs",
                "verification",
            ],
            vec!["diff_ref"],
        ),
        (_, Docs) => (
            vec![
                "task",
                "goal",
                "findings",
                "files",
                "decisions",
                "verification",
            ],
            vec![],
        ),
        _ => (
            vec!["task", "goal"],
            vec!["constraints", "findings", "files", "symbols"],
        ),
    };
    ProjectionSchema {
        source,
        destination,
        required,
        optional,
    }
}

/// The transition implied by a source and destination role.
pub fn transition_for(source: Role, destination: Role) -> Transition {
    use Role::*;
    match (source, destination) {
        (Lead, _) => Transition::LeadToRole,
        (Explore | ExploreDeep, Build) => Transition::ExploreToBuild,
        (Build, Verify) => Transition::BuildToVerify,
        (Debug, Build) => Transition::DebugToBuild,
        (_, Debug) => Transition::VerifyToDebug,
        (_, Lead) => Transition::ToLead,
        _ => Transition::LeadToRole,
    }
}

/// Which fields a destination role is allowed to see. Everything not allowed is
/// cleared before any size reduction, so a role can never receive another
/// role's narrative or verification residue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FieldMask {
    goal: bool,
    constraints: bool,
    findings: bool,
    files: bool,
    symbols: bool,
    decisions: bool,
    verification: bool,
    failures: bool,
    evidence: bool,
    raw_log_refs: bool,
    diff_ref: bool,
    diff_context: bool,
}

/// The deterministic field mask for a destination.
fn destination_mask(destination: Role) -> FieldMask {
    use Role::*;
    match destination {
        // The Lead is the orchestrator and sees planning state, not raw
        // verification residue.
        Lead => FieldMask {
            goal: true,
            constraints: true,
            findings: true,
            files: true,
            symbols: true,
            decisions: true,
            verification: false,
            failures: false,
            evidence: false,
            raw_log_refs: false,
            diff_ref: false,
            diff_context: false,
        },
        // Explore maps the repository; it never sees verification failures or
        // raw logs (nothing has run yet, and they are not reconnaissance).
        Explore | ExploreDeep => FieldMask {
            goal: true,
            constraints: true,
            findings: true,
            files: true,
            symbols: true,
            decisions: false,
            verification: false,
            failures: false,
            evidence: true,
            raw_log_refs: false,
            diff_ref: false,
            diff_context: false,
        },
        // Build sees high-confidence findings and only the fix feedback it
        // needs (verification + failures). Raw logs stay out.
        Build => FieldMask {
            goal: true,
            constraints: true,
            findings: true,
            files: true,
            symbols: true,
            decisions: true,
            verification: true,
            failures: true,
            evidence: false,
            raw_log_refs: false,
            diff_ref: true,
            diff_context: true,
        },
        // Verify reviews the change against acceptance: goal, constraints,
        // changed files, symbols, diff and verification. No exploratory
        // narrative (findings/decisions) and no duplicated failure list.
        Verify => FieldMask {
            goal: true,
            constraints: true,
            findings: false,
            files: true,
            symbols: true,
            decisions: false,
            verification: true,
            failures: false,
            evidence: true,
            raw_log_refs: false,
            diff_ref: true,
            diff_context: true,
        },
        // Debug is deliberately narrow: failures, evidence, relevant diff,
        // verification and raw-log references only.
        Debug => FieldMask {
            goal: false,
            constraints: false,
            findings: false,
            files: false,
            symbols: false,
            decisions: false,
            verification: true,
            failures: true,
            evidence: true,
            raw_log_refs: true,
            diff_ref: true,
            diff_context: true,
        },
        // Docs closes the work out with the goal, findings, files, decisions and
        // verification summary.
        Docs => FieldMask {
            goal: true,
            constraints: false,
            findings: true,
            files: true,
            symbols: false,
            decisions: true,
            verification: true,
            failures: false,
            evidence: false,
            raw_log_refs: false,
            diff_ref: false,
            diff_context: false,
        },
    }
}

fn safe_path(path: &str) -> bool {
    !classify::classify(Path::new(path)).sensitive && !is_secret_like(path)
}

fn drop_secret(value: Option<String>, omitted: &mut Vec<String>, label: &str) -> Option<String> {
    let value = value?;
    if is_secret_like(&value) {
        omitted.push(format!("omitted a secret-shaped {label}"));
        None
    } else {
        Some(value)
    }
}

fn retain_safe(values: Vec<String>, omitted: &mut Vec<String>, label: &str) -> Vec<String> {
    let mut kept = Vec::with_capacity(values.len());
    let mut dropped = 0usize;
    for value in values {
        if is_secret_like(&value) {
            dropped += 1;
        } else {
            kept.push(value);
        }
    }
    if dropped > 0 {
        omitted.push(format!("omitted {dropped} secret-shaped {label}(s)"));
    }
    kept
}

fn retain_safe_paths(values: Vec<String>, omitted: &mut Vec<String>, label: &str) -> Vec<String> {
    let mut kept = Vec::with_capacity(values.len());
    let mut dropped = 0usize;
    for value in values {
        if safe_path(&value) {
            kept.push(value);
        } else {
            dropped += 1;
        }
    }
    if dropped > 0 {
        omitted.push(format!("omitted {dropped} unsafe {label}(s)"));
    }
    kept
}

/// Drop secret-shaped lines (and diff headers naming a sensitive path) from a
/// rendered diff block. The byte count of dropped lines is reported, never the
/// content.
fn sanitize_diff_block(text: &str) -> (String, usize) {
    let mut out = String::new();
    let mut dropped = 0usize;
    for line in text.lines() {
        if is_secret_like(line) || diff_header_is_sensitive(line) {
            dropped += 1;
            continue;
        }
        out.push_str(line);
        out.push('\n');
    }
    (out, dropped)
}

fn diff_header_is_sensitive(line: &str) -> bool {
    let trimmed = line.trim_start();
    let path = trimmed
        .strip_prefix("+++ ")
        .or_else(|| trimmed.strip_prefix("--- "))
        .or_else(|| trimmed.strip_prefix("diff --git "));
    let Some(path) = path else {
        return false;
    };
    // `diff --git a/x b/x` and `+++ b/x` both end with the real path.
    let candidate = path.split_whitespace().last().unwrap_or(path);
    let candidate = candidate
        .strip_prefix("a/")
        .or_else(|| candidate.strip_prefix("b/"))
        .unwrap_or(candidate);
    !candidate.is_empty() && !safe_path(candidate)
}

fn scrub_git(state: &mut crate::context::gitdiff::GitState) {
    for field in [&mut state.root, &mut state.head, &mut state.branch] {
        if let Some(value) = field.as_ref() {
            if is_secret_like(value) {
                *field = None;
            }
        }
    }
}

/// Remove secret-shaped strings and sensitive paths from a projection input.
/// Returns the cleaned input and one note per dropped item. The notes never
/// contain the dropped value.
pub fn sanitize(input: &ProjectionInput) -> (ProjectionInput, Vec<String>) {
    let mut omitted = Vec::new();
    let mut cleaned = input.clone();

    // The task is required: redact it rather than drop the whole hand-off.
    if is_secret_like(&cleaned.task) {
        cleaned.task = redact(&cleaned.task);
        omitted.push("redacted a secret-shaped task".to_string());
    }
    cleaned.goal = drop_secret(cleaned.goal, &mut omitted, "goal");
    cleaned.constraints = retain_safe(cleaned.constraints, &mut omitted, "hard constraint");
    cleaned.decisions = retain_safe(cleaned.decisions, &mut omitted, "decision");
    cleaned.failures = retain_safe(cleaned.failures, &mut omitted, "failure");
    cleaned.evidence = retain_safe(cleaned.evidence, &mut omitted, "evidence");
    cleaned.diff_ref = drop_secret(cleaned.diff_ref, &mut omitted, "diff reference");
    if !cleaned.diff_context.is_empty() {
        let (block, dropped) = sanitize_diff_block(&cleaned.diff_context);
        cleaned.diff_context = block;
        if dropped > 0 {
            omitted.push(format!("omitted {dropped} unsafe diff line(s)"));
        }
    }
    cleaned.raw_log_refs =
        retain_safe_paths(cleaned.raw_log_refs, &mut omitted, "raw-log reference");

    cleaned.findings.retain(|finding| {
        if safe_path_maybe(&finding.source)
            && !is_secret_like(&finding.summary)
            && finding
                .detail
                .as_deref()
                .map(|d| !is_secret_like(d))
                .unwrap_or(true)
            && finding
                .source
                .as_deref()
                .map(|s| !is_secret_like(s))
                .unwrap_or(true)
        {
            true
        } else {
            omitted.push("omitted a secret-shaped finding".to_string());
            false
        }
    });
    cleaned.files.retain(|file| {
        if safe_path(&file.path) {
            true
        } else {
            omitted.push("omitted a sensitive-path file".to_string());
            false
        }
    });
    cleaned.symbols.retain(|symbol| {
        if safe_path(&symbol.path) && !is_secret_like(&symbol.name) {
            true
        } else {
            omitted.push("omitted a sensitive-path symbol".to_string());
            false
        }
    });
    if let Some(verification) = cleaned.verification.as_mut() {
        if is_secret_like(&verification.stage) {
            verification.stage = redact(&verification.stage);
            omitted.push("redacted a secret-shaped verification stage".to_string());
        }
        if is_secret_like(&verification.outcome) {
            verification.outcome = redact(&verification.outcome);
            omitted.push("redacted a secret-shaped verification outcome".to_string());
        }
        verification.failed_commands = retain_safe(
            std::mem::take(&mut verification.failed_commands),
            &mut omitted,
            "failed command",
        );
        verification.failed_tests = retain_safe(
            std::mem::take(&mut verification.failed_tests),
            &mut omitted,
            "failed test",
        );
        verification.distilled = retain_safe(
            std::mem::take(&mut verification.distilled),
            &mut omitted,
            "distilled line",
        );
        verification.raw_log_refs = retain_safe_paths(
            std::mem::take(&mut verification.raw_log_refs),
            &mut omitted,
            "verification raw-log reference",
        );
        verification
            .locations
            .retain(|location| safe_path(&location.path) && !is_secret_like(&location.display()));
    }
    scrub_git(&mut cleaned.git);

    // Selected source slices travel separately in the dynamic context, but they
    // are still model-facing. Drop a slice whose content looks secret-shaped
    // even when its path is innocuous.
    let mut dropped_slices = 0usize;
    cleaned.slices.retain(|slice| {
        if classify::classify(Path::new(&slice.path)).sensitive || is_secret_like(&slice.content) {
            dropped_slices += 1;
            false
        } else {
            true
        }
    });
    if dropped_slices > 0 {
        omitted.push(format!(
            "omitted {dropped_slices} source slice(s) that were not safe to include"
        ));
    }

    omitted.sort();
    omitted.dedup();
    (cleaned, omitted)
}

fn safe_path_maybe(path: &Option<String>) -> bool {
    path.as_deref().map(safe_path).unwrap_or(true)
}

fn sort_findings(findings: &mut [HandoffFinding]) {
    findings.sort_by(|a, b| {
        b.severity
            .cmp(&a.severity)
            .then_with(|| a.summary.cmp(&b.summary))
    });
}

fn sort_files(files: &mut [CapsuleFile]) {
    files.sort_by(|a, b| b.changed.cmp(&a.changed).then_with(|| a.path.cmp(&b.path)));
}

fn sort_symbols(symbols: &mut [SymbolRef], task_terms: &[String]) {
    symbols.sort_by(|a, b| {
        let a_match = symbol_matches(a, task_terms);
        let b_match = symbol_matches(b, task_terms);
        b_match
            .cmp(&a_match)
            .then_with(|| a.path.cmp(&b.path))
            .then_with(|| a.start_line.cmp(&b.start_line))
            .then_with(|| a.name.cmp(&b.name))
    });
}

fn symbol_matches(symbol: &SymbolRef, task_terms: &[String]) -> bool {
    let name = symbol.name.to_ascii_lowercase();
    task_terms.iter().any(|term| name.contains(term))
}

fn sort_locations(locations: &mut [SourceLocation]) {
    locations.sort();
}

fn dedup_sorted(values: &mut Vec<String>) {
    values.sort();
    values.dedup();
}

/// The optional-item budget for one reduction level. Level 0 keeps the most;
/// the final levels keep only what the schema requires.
fn level_budgets(level: usize) -> (usize, usize, usize, usize, usize) {
    match level {
        0 => (200, 64, 64, 32, 20),
        1 => (64, 32, 32, 16, 12),
        2 => (24, 16, 16, 8, 8),
        3 => (12, 8, 8, 4, 4),
        4 => (8, 6, 6, 2, 2),
        _ => (4, 4, 4, 0, 1),
    }
}

fn build_at_level(
    input: &ProjectionInput,
    source: Role,
    destination: Role,
    task_id: &str,
    session_id: &str,
    level: usize,
) -> ModelHandoffCapsule {
    let (max_symbols, max_files, max_findings, max_evidence, max_distilled) = level_budgets(level);
    let task_terms = terms(&input.task);

    let mut capsule = ModelHandoffCapsule {
        schema_version: HANDOFF_SCHEMA_VERSION,
        transition: transition_for(source, destination),
        source,
        destination,
        task_id: task_id.to_string(),
        session_id: session_id.to_string(),
        task: input.task.clone(),
        goal: input.goal.clone(),
        hard_constraints: {
            let mut constraints = input.constraints.clone();
            dedup_sorted(&mut constraints);
            constraints
        },
        findings: input.findings.clone(),
        files: input.files.clone(),
        symbols: input.symbols.clone(),
        decisions: {
            let mut decisions = input.decisions.clone();
            dedup_sorted(&mut decisions);
            decisions
        },
        verification: input.verification.clone(),
        failures: {
            let mut failures = input.failures.clone();
            dedup_sorted(&mut failures);
            failures
        },
        evidence: {
            let mut evidence = input.evidence.clone();
            dedup_sorted(&mut evidence);
            evidence
        },
        diff_ref: input.diff_ref.clone(),
        diff_context: if input.diff_context.trim().is_empty() {
            None
        } else {
            Some(input.diff_context.clone())
        },
        raw_log_refs: {
            let mut refs = input.raw_log_refs.clone();
            dedup_sorted(&mut refs);
            refs
        },
        git: input.git.clone(),
        omitted: Vec::new(),
        projection: projection_id(source, destination),
    };

    // The role mask is applied before ordering and truncation so a field can
    // never be reintroduced later.
    let mask = destination_mask(destination);
    if !mask.goal {
        capsule.goal = None;
    }
    if !mask.constraints {
        capsule.hard_constraints.clear();
    }
    if !mask.findings {
        capsule.findings.clear();
    }
    if !mask.files {
        capsule.files.clear();
    }
    if !mask.symbols {
        capsule.symbols.clear();
    }
    if !mask.decisions {
        capsule.decisions.clear();
    }
    if !mask.verification {
        capsule.verification = None;
    }
    if !mask.failures {
        capsule.failures.clear();
    }
    if !mask.evidence {
        capsule.evidence.clear();
    }
    if !mask.raw_log_refs {
        capsule.raw_log_refs.clear();
    }
    if !mask.diff_ref {
        capsule.diff_ref = None;
    }
    if !mask.diff_context {
        capsule.diff_context = None;
    }

    sort_findings(&mut capsule.findings);
    sort_files(&mut capsule.files);
    sort_symbols(&mut capsule.symbols, &task_terms);
    capsule.files.dedup_by(|a, b| a.path == b.path);
    capsule
        .symbols
        .dedup_by(|a, b| a.path == b.path && a.name == b.name && a.start_line == b.start_line);
    capsule
        .findings
        .dedup_by(|a, b| a.summary == b.summary && a.severity == b.severity);

    // Destination-specific confidence / relevance filters.
    match destination {
        Role::Build => {
            // Only high-confidence findings are fix-relevant.
            capsule
                .findings
                .retain(|finding| finding.severity >= Severity::Warning);
        }
        Role::Verify => {
            // Review only what actually changed.
            capsule.files.retain(|file| file.changed);
        }
        _ => {}
    }

    if let Some(verification) = capsule.verification.as_mut() {
        sort_locations(&mut verification.locations);
    }

    // Truncate optional material. Critical findings survive at every level.
    let keep_critical = level >= 4;
    if keep_critical && destination != Role::Build {
        capsule
            .findings
            .retain(|finding| finding.severity >= Severity::Warning);
    }
    truncate(
        &mut capsule.findings,
        max_findings,
        "finding",
        &mut capsule.omitted,
    );
    truncate(&mut capsule.files, max_files, "file", &mut capsule.omitted);
    truncate(
        &mut capsule.symbols,
        max_symbols,
        "symbol",
        &mut capsule.omitted,
    );
    truncate(
        &mut capsule.decisions,
        max_evidence,
        "decision",
        &mut capsule.omitted,
    );
    truncate(
        &mut capsule.evidence,
        max_evidence,
        "evidence",
        &mut capsule.omitted,
    );
    if let Some(verification) = capsule.verification.as_mut() {
        truncate(
            &mut verification.distilled,
            max_distilled,
            "distilled line",
            &mut capsule.omitted,
        );
    }
    truncate(
        &mut capsule.raw_log_refs,
        max_distilled,
        "raw-log reference",
        &mut capsule.omitted,
    );
    capsule
}

fn truncate<T>(values: &mut Vec<T>, max: usize, label: &str, omitted: &mut Vec<String>) {
    if values.len() > max {
        let removed = values.len() - max;
        values.truncate(max);
        omitted.push(format!("dropped {removed} {label}(s)"));
    }
}

/// Project the rich input to the compact capsule for one transition.
///
/// The result never exceeds `limits.max_bytes`, and never exceeds
/// `ratio_percent` of the rich source when the rich source is non-empty,
/// unless even the required fields alone exceed that budget. That exception is
/// recorded in `omitted` rather than hidden.
pub fn project(
    input: &ProjectionInput,
    source: Role,
    destination: Role,
    task_id: &str,
    session_id: &str,
    limits: ProjectionLimits,
) -> ModelHandoffCapsule {
    let rich = input.rich_bytes();
    project_with_rich(
        input,
        source,
        destination,
        task_id,
        session_id,
        limits,
        rich,
    )
}

/// Like [`project`], but with an explicit rich-context reference for the ratio
/// cap. Used when the input is deliberately narrow (for example the Debug
/// hand-off) but the task as a whole has a much larger rich context.
#[allow(clippy::too_many_arguments)]
pub fn project_with_rich(
    input: &ProjectionInput,
    source: Role,
    destination: Role,
    task_id: &str,
    session_id: &str,
    limits: ProjectionLimits,
    rich_reference_bytes: usize,
) -> ModelHandoffCapsule {
    let (clean, mut omitted) = sanitize(input);
    let rich_bytes = clean.rich_bytes().max(rich_reference_bytes);
    let cap = limits.effective_cap(rich_bytes);
    let mut last = build_at_level(&clean, source, destination, task_id, session_id, 5);
    for level in 0..=5 {
        let capsule = build_at_level(&clean, source, destination, task_id, session_id, level);
        if capsule.measured_bytes() <= cap {
            let mut capsule = capsule;
            capsule.omitted.append(&mut omitted);
            capsule.omitted.sort();
            capsule.omitted.dedup();
            return capsule;
        }
        last = capsule;
    }
    let mut capsule = last;
    capsule.omitted.append(&mut omitted);
    capsule.omitted.push(format!(
        "required fields exceed the {} byte hand-off cap (rich source {} bytes)",
        cap, rich_bytes
    ));
    capsule.omitted.sort();
    capsule.omitted.dedup();
    capsule
}

/// Build a hand-off from an explicit verification block only. Used by the
/// Debug transition, which must not carry the rest of the rich state.
#[allow(clippy::too_many_arguments)]
pub fn debug_projection(
    task: &str,
    task_id: &str,
    session_id: &str,
    failures: Vec<String>,
    evidence: Vec<String>,
    raw_log_refs: Vec<String>,
    diff_ref: Option<String>,
    verification: Option<HandoffVerification>,
    git: crate::context::gitdiff::GitState,
    limits: ProjectionLimits,
) -> ModelHandoffCapsule {
    let input = ProjectionInput {
        task: task.to_string(),
        failures,
        evidence,
        raw_log_refs,
        diff_ref,
        verification,
        git,
        ..ProjectionInput::default()
    };
    project(
        &input,
        Role::Verify,
        Role::Debug,
        task_id,
        session_id,
        limits,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::capsule::CapsuleFile;
    use crate::context::symbols::SymbolKind;
    use crate::orchestration::handoff::Severity;

    fn symbol(path: &str, name: &str) -> SymbolRef {
        SymbolRef {
            path: path.to_string(),
            name: name.to_string(),
            kind: SymbolKind::Function,
            start_line: 1,
            end_line: 2,
        }
    }

    fn finding(summary: &str, severity: Severity) -> HandoffFinding {
        HandoffFinding {
            summary: summary.to_string(),
            detail: None,
            source: None,
            severity,
        }
    }

    fn rich() -> ProjectionInput {
        let mut input = ProjectionInput {
            task: "update module_1 parse_1 parser".to_string(),
            goal: Some("update the parser safely".to_string()),
            constraints: vec!["never break the public API".to_string()],
            findings: vec![
                finding("the parser is at src/module_1.rs", Severity::Critical),
                finding("a helper exists", Severity::Info),
            ],
            files: vec![CapsuleFile {
                path: "src/module_1.rs".to_string(),
                reason: Some("changed".to_string()),
                changed: true,
            }],
            symbols: vec![symbol("src/module_1.rs", "parse_1")],
            decisions: vec!["keep the function name".to_string()],
            failures: vec!["error[E0308] at src/module_1.rs:4:5".to_string()],
            evidence: vec!["exit 101".to_string()],
            verification: Some(HandoffVerification {
                stage: "normal".to_string(),
                outcome: "failed".to_string(),
                failed_commands: vec!["cargo test".to_string()],
                failed_tests: vec!["parse_1".to_string()],
                locations: vec![SourceLocation {
                    path: "src/module_1.rs".to_string(),
                    line: Some(4),
                    column: Some(5),
                }],
                raw_log_refs: vec![".ocg/logs/abc.txt".to_string()],
                distilled: vec!["error[E0308]".to_string()],
            }),
            diff_ref: Some("sha256:diff".to_string()),
            diff_context: "diff --git a b".to_string(),
            raw_log_refs: vec![".ocg/logs/abc.txt".to_string()],
            provenance: Default::default(),
            git: Default::default(),
            slices: vec![crate::context::ranking::ContextSlice {
                path: "src/module_1.rs".to_string(),
                start_line: 1,
                end_line: 2,
                content: "pub fn parse_1() {}\n".to_string(),
                bytes: 20,
                estimated_tokens: 5,
            }],
        };
        for index in 0..200 {
            input.files.push(CapsuleFile {
                path: format!("src/module_{index}.rs"),
                reason: Some("ranked".to_string()),
                changed: false,
            });
            input.symbols.push(symbol(
                &format!("src/module_{index}.rs"),
                &format!("helper_{index}"),
            ));
        }
        input
    }

    fn limits() -> ProjectionLimits {
        ProjectionLimits {
            max_bytes: 4096,
            ratio_percent: 40,
        }
    }

    #[test]
    fn required_fields_survive_every_projection() {
        let input = rich();
        let capsule = project(
            &input,
            Role::Explore,
            Role::Build,
            "task-1",
            "sess-1",
            limits(),
        );
        assert!(
            capsule.measured_bytes() <= 4096,
            "{}",
            capsule.measured_bytes()
        );
        assert!(
            capsule.measured_bytes() <= input.rich_bytes() * 40 / 100,
            "capsule {} rich {}",
            capsule.measured_bytes(),
            input.rich_bytes()
        );
        assert_eq!(capsule.goal.as_deref(), Some("update the parser safely"));
        assert!(capsule
            .hard_constraints
            .iter()
            .any(|constraint| constraint == "never break the public API"));
        assert!(capsule
            .symbols
            .iter()
            .any(|symbol| symbol.name == "parse_1"));
        assert!(capsule
            .findings
            .iter()
            .any(|finding| finding.severity == Severity::Critical));
        // A failing location must never be dropped from a Debug projection.
        let debug = project(
            &input,
            Role::Verify,
            Role::Debug,
            "task-1",
            "sess-1",
            limits(),
        );
        assert_eq!(
            debug.first_failing_location().map(|l| l.display()),
            Some("src/module_1.rs:4:5".to_string())
        );
    }

    #[test]
    fn projections_are_deterministic() {
        let input = rich();
        let a = project(&input, Role::Explore, Role::Build, "t", "s", limits());
        let b = project(&input, Role::Explore, Role::Build, "t", "s", limits());
        assert_eq!(a, b);
        assert_eq!(a.measured_bytes(), b.measured_bytes());
    }

    #[test]
    fn destinations_clear_irrelevant_fields() {
        let input = rich();
        let limits = limits();

        let explore = project(&input, Role::Lead, Role::Explore, "t", "s", limits);
        assert!(explore.goal.is_some());
        assert!(
            explore.verification.is_none(),
            "Explore must not carry verification"
        );
        assert!(
            explore.failures.is_empty(),
            "Explore must not carry failures"
        );
        assert!(
            explore.raw_log_refs.is_empty(),
            "Explore must not carry raw logs"
        );
        assert!(
            explore.decisions.is_empty(),
            "Explore must not carry decisions"
        );
        assert!(!explore.findings.is_empty());
        assert!(!explore.files.is_empty());
        assert!(
            explore.diff_context.is_none(),
            "Explore must not carry the diff"
        );

        let build = project(&input, Role::Explore, Role::Build, "t", "s", limits);
        assert!(
            build.verification.is_some(),
            "Build must carry fix feedback"
        );
        assert!(!build.failures.is_empty(), "Build must carry failures");
        assert!(
            build.raw_log_refs.is_empty(),
            "Build must not carry raw logs"
        );
        assert!(build.diff_context.is_some(), "Build must carry the diff");
        assert!(
            build
                .findings
                .iter()
                .all(|f| f.severity >= Severity::Warning),
            "Build only keeps high-confidence findings"
        );

        let verify = project(&input, Role::Build, Role::Verify, "t", "s", limits);
        assert!(
            verify.findings.is_empty(),
            "Verify must not carry exploratory findings"
        );
        assert!(
            verify.decisions.is_empty(),
            "Verify must not carry decisions"
        );
        assert!(
            verify.failures.is_empty(),
            "Verify must not duplicate failure lists"
        );
        assert!(verify.verification.is_some());
        assert!(verify.diff_ref.is_some());
        assert!(
            verify.files.iter().all(|file| file.changed),
            "Verify only sees changed files"
        );

        let debug = project(&input, Role::Verify, Role::Debug, "t", "s", limits);
        assert!(
            debug.goal.is_none(),
            "Debug must not carry the goal narrative"
        );
        assert!(debug.hard_constraints.is_empty());
        assert!(debug.findings.is_empty());
        assert!(debug.files.is_empty());
        assert!(debug.symbols.is_empty());
        assert!(debug.decisions.is_empty());
        assert!(!debug.failures.is_empty());
        assert!(debug.verification.is_some());
        assert!(!debug.raw_log_refs.is_empty());

        let docs = project(&input, Role::Verify, Role::Docs, "t", "s", limits);
        assert!(docs.goal.is_some());
        assert!(!docs.findings.is_empty());
        assert!(!docs.files.is_empty());
        assert!(!docs.decisions.is_empty());
        assert!(docs.verification.is_some());
        assert!(docs.hard_constraints.is_empty());
        assert!(docs.symbols.is_empty());
        assert!(docs.failures.is_empty());
    }

    #[test]
    fn secret_shaped_content_is_dropped_from_every_field() {
        let secret = format!("{}{}", concat!("sk", "-"), "A".repeat(40));
        let auth = format!("Authorization: Bearer {}", "x".repeat(24));
        let mut input = rich();
        input.goal = Some(secret.clone());
        input.constraints = vec![secret.clone()];
        input.decisions = vec![auth.clone()];
        input.evidence = vec![secret.clone()];
        input.diff_ref = Some(secret.clone());
        input.diff_context = format!("diff src/a.rs\n  + let key = \"{secret}\";\n");
        input.raw_log_refs = vec![".env".to_string(), secret.clone()];
        input.findings.push(HandoffFinding {
            summary: secret.clone(),
            detail: Some(secret.clone()),
            source: Some(secret.clone()),
            severity: Severity::Critical,
        });
        if let Some(verification) = input.verification.as_mut() {
            verification.stage = secret.clone();
            verification.distilled = vec![secret.clone(), auth.clone()];
            verification.failed_commands = vec![secret.clone()];
        }
        let capsule = project(&input, Role::Verify, Role::Debug, "t", "s", limits());
        let text = serde_json::to_string(&capsule).unwrap();
        assert!(!text.contains(&secret), "{text}");
        assert!(!text.contains(&auth), "{text}");
        assert!(!text.contains(".env"), "{text}");
        // Redaction, not leakage, is what remains.
        assert!(text.contains("[redacted]"), "{text}");
    }

    #[test]
    fn secret_shaped_source_slice_is_dropped_by_content() {
        let secret = format!("{}{}", concat!("sk", "-"), "A".repeat(40));
        let mut input = rich();
        input.slices = vec![crate::context::ranking::ContextSlice {
            path: "src/innocuous.rs".to_string(),
            start_line: 1,
            end_line: 3,
            content: format!("const KEY: &str = \"{secret}\";\n"),
            bytes: 60,
            estimated_tokens: 15,
        }];
        let (clean, omitted) = sanitize(&input);
        // The slice must be dropped if (and only if) it carries a secret.
        assert!(
            clean.slices.is_empty(),
            "a secret-shaped source slice must be dropped entirely"
        );
        assert!(omitted
            .iter()
            .any(|note| note.contains("source slice") && !note.contains(secret.as_str())));
        // The note never echoes the secret.
        assert!(!omitted.join("\n").contains(&secret));
    }

    #[test]
    fn sensitive_paths_are_dropped() {
        let input = ProjectionInput {
            task: "t".to_string(),
            files: vec![
                CapsuleFile {
                    path: ".env.production".to_string(),
                    reason: None,
                    changed: true,
                },
                CapsuleFile {
                    path: "certs/server.pem".to_string(),
                    reason: None,
                    changed: false,
                },
                CapsuleFile {
                    path: "credentials.json".to_string(),
                    reason: None,
                    changed: false,
                },
                CapsuleFile {
                    path: "src/token_store.rs".to_string(),
                    reason: None,
                    changed: false,
                },
                CapsuleFile {
                    path: "src/main.rs".to_string(),
                    reason: None,
                    changed: false,
                },
            ],
            ..ProjectionInput::default()
        };
        let (clean, _) = sanitize(&input);
        let paths: Vec<&str> = clean.files.iter().map(|file| file.path.as_str()).collect();
        assert_eq!(paths, vec!["src/main.rs"]);
    }

    #[test]
    fn schema_names_are_stable() {
        let schema = projection_schema(Role::Explore, Role::Build);
        assert_eq!(schema.id(), "explore-to-build");
        assert!(schema.has_required("hard_constraints"));
        assert!(!schema.has_required("raw_log_refs"));
        let debug = projection_schema(Role::Verify, Role::Debug);
        assert!(debug.has_required("failures"));
        assert!(debug.has_required("raw_log_refs"));
    }

    #[test]
    fn effective_cap_never_exceeds_absolute() {
        let limits = ProjectionLimits {
            max_bytes: 4096,
            ratio_percent: 40,
        };
        assert_eq!(limits.effective_cap(0), 4096);
        assert_eq!(limits.effective_cap(100_000), 4096);
        assert_eq!(limits.effective_cap(1_000), 400);
    }
}
