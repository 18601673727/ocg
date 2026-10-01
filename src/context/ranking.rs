//! Deterministic context ranking and plan assembly.
//!
//! Ranking is transparent: every candidate carries the integer reasons that
//! produced its score, so two runs over the same tree produce byte-identical
//! plans. No model, no network and no randomness is involved. Token counts are
//! always clearly estimates (`bytes / 4`), never exact.

use crate::context::config::ContextConfig;
use crate::context::freshness::{Provenance, SourceFingerprint, ENGINE_VERSION, SCHEMA_VERSION};
use crate::context::gitdiff::DiffSummary;
use crate::context::index::ContextIndex;
use crate::context::repomap::RepoMap;
use crate::error::{OcgError, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
/// The selected per-plan limits, echoed for transparency.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextLimits {
    pub max_candidates: usize,
    pub max_files: usize,
    pub max_slices: usize,
    pub max_bytes: usize,
    pub max_diff_bytes: usize,
    pub max_hunks: usize,
    pub max_file_bytes: u64,
    pub max_symbols_per_file: usize,
    /// Hard cap on files scanned into the repo map.
    pub max_repository_files: usize,
}

/// A named, ordered plan section. The order is a stable hook, not a layout.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanSection {
    pub name: String,
    pub order: u32,
}

/// The deterministic OCG-instruction block of a plan. It lists only stable
/// OCG commands; it does not inject provider execution configuration.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct GearInstructions {
    pub engine_version: String,
    pub commands: Vec<String>,
    pub note: String,
}

/// Repository instruction / policy files detected at the project root.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct PolicyState {
    pub files: Vec<String>,
    pub present: bool,
    pub note: String,
}

impl PolicyState {
    /// Repository policy files, in a stable order. Only existence is checked.
    pub fn detect(root: &Path) -> Self {
        const CANDIDATES: [&str; 8] = [
            "AGENTS.md",
            "CLAUDE.md",
            "CONTRIBUTING.md",
            "CONTRIBUTING.rst",
            "CODE_OF_CONDUCT.md",
            ".github/CONTRIBUTING.md",
            "docs/CONTRIBUTING.md",
            "docs/ARCHITECTURE.md",
        ];
        let files: Vec<String> = CANDIDATES
            .iter()
            .filter(|candidate| root.join(candidate).is_file())
            .map(|candidate| candidate.to_string())
            .collect();
        let present = !files.is_empty();
        let note = if present {
            "the repository is authoritative for its own conventions and safety rules".to_string()
        } else {
            "no standard repository instruction file was detected".to_string()
        };
        Self {
            files,
            present,
            note,
        }
    }
}

/// The computed capability/verification/test-proposal context attached to a
/// plan. Kept separate from ranking so the ranking functions stay pure.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct PlanEnrichment {
    #[serde(default)]
    pub capabilities: crate::capabilities::CapabilityPlan,
    #[serde(default)]
    pub verification: crate::verification::VerificationState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub test_proposal: Option<crate::verification::TestProposal>,
}

/// One ranked file candidate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Candidate {
    pub path: String,
    pub score: u64,
    pub reasons: Vec<String>,
    pub selected: bool,
    pub bytes: usize,
    /// Estimated tokens for this candidate (bytes / 4).
    pub estimated_tokens: usize,
}

/// A bounded content slice taken from a selected file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextSlice {
    pub path: String,
    pub start_line: u32,
    pub end_line: u32,
    pub content: String,
    pub bytes: usize,
    /// Estimated tokens (bytes / 4); never presented as exact.
    pub estimated_tokens: usize,
}

/// The complete, deterministic context plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextPlan {
    pub schema_version: u32,
    pub engine_version: String,
    pub task: String,
    pub role: Option<String>,
    pub repo_id: String,
    pub root: String,
    pub git: crate::context::gitdiff::GitState,
    pub sections: Vec<PlanSection>,
    pub candidates: Vec<Candidate>,
    pub selected_files: Vec<String>,
    pub changed_paths: Vec<String>,
    /// The bounded, real diff summary the plan was ranked against. Kept in the
    /// plan so downstream role hand-offs can render an actual diff instead of a
    /// fabricated reference. Optional for cache compatibility with plans written
    /// before the field existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diff: Option<DiffSummary>,
    pub slices: Vec<ContextSlice>,
    pub candidate_bytes: usize,
    pub selected_bytes: usize,
    /// Estimated only: `selected_bytes / 4`.
    pub estimated_tokens: usize,
    pub limits: ContextLimits,
    pub provenance: Provenance,
    pub sensitive_excluded: usize,
    pub truncated: bool,
    pub notes: Vec<String>,
    /// Stable conceptual sections: instructions, policy, map, capabilities,
    /// capsule, symbols/source, git diff, verification state.
    #[serde(default)]
    pub instructions: GearInstructions,
    #[serde(default)]
    pub policy: PolicyState,
    #[serde(default)]
    pub capabilities: crate::capabilities::CapabilityPlan,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capsule: Option<crate::context::capsule::TaskCapsule>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub test_proposal: Option<crate::verification::TestProposal>,
    #[serde(default)]
    pub verification: crate::verification::VerificationState,
}

impl ContextPlan {
    /// Whether the plan contains content that must never reach the cache.
    pub fn cacheable(&self) -> bool {
        !self
            .slices
            .iter()
            .any(|slice| crate::context::classify::classify(Path::new(&slice.path)).sensitive)
    }
}

/// The stable conceptual section order of a plan. This is the exact ordering
/// contract: instructions, policy, repository map, capabilities, task capsule,
/// symbols/source, git diff, verification state.
pub fn sections() -> Vec<PlanSection> {
    [
        "gear_instructions",
        "project_policy",
        "repository_map",
        "capabilities",
        "task_capsule",
        "symbols_source",
        "git_diff",
        "verification_state",
    ]
    .iter()
    .enumerate()
    .map(|(order, name)| PlanSection {
        name: name.to_string(),
        order: order as u32,
    })
    .collect()
}

/// Estimated tokens from a byte count. Clearly an estimate, never exact.
pub fn estimated_tokens(bytes: usize) -> usize {
    bytes.div_ceil(4)
}

/// Split a task into stable, lowercase search terms.
pub fn terms(task: &str) -> Vec<String> {
    let mut out = Vec::new();
    for raw in task.split(|ch: char| !ch.is_alphanumeric()) {
        if raw.len() < 2 {
            continue;
        }
        let term = raw.to_ascii_lowercase();
        if !out.contains(&term) {
            out.push(term);
        }
    }
    out
}

/// Score one file and return the human-readable reasons.
pub fn score(
    map: &RepoMap,
    index: &ContextIndex,
    diff: &DiffSummary,
    file_path: &str,
    task_terms: &[String],
) -> (u64, Vec<String>) {
    let mut score = 0u64;
    let mut reasons = Vec::new();
    let lower = file_path.to_ascii_lowercase();

    for term in task_terms {
        if lower.contains(term) {
            score += 40;
            reasons.push(format!("path:{term}"));
        }
    }
    if diff.entries.iter().any(|entry| entry.path == file_path) {
        score += 50;
        reasons.push("changed".to_string());
    }
    if crate::context::gitdiff::is_test_path(file_path) {
        score += 10;
        reasons.push("test".to_string());
    }
    if let Some(file) = index.file(file_path) {
        for symbol in &file.symbols {
            let symbol_lower = symbol.name.to_ascii_lowercase();
            for term in task_terms {
                if symbol_lower.contains(term) {
                    score += 30;
                    reasons.push(format!("symbol:{}", symbol.name));
                    break;
                }
            }
        }
        for symbol in &file.symbols {
            if symbol.kind == crate::context::symbols::SymbolKind::Import {
                for term in task_terms {
                    if symbol.name.to_ascii_lowercase().contains(term) {
                        score += 15;
                        reasons.push(format!("import:{}", symbol.name));
                        break;
                    }
                }
            }
        }
    }
    if let Some(entry) = map.file(file_path) {
        match entry.kind {
            crate::context::repomap::FileKind::Entrypoint => {
                score += 10;
                reasons.push("entrypoint".to_string());
            }
            crate::context::repomap::FileKind::Source => {
                score += 5;
            }
            _ => {}
        }
    }
    reasons.sort();
    reasons.dedup();
    (score, reasons)
}

/// Rank every file in the map. Deterministic: score desc, then path asc.
pub fn rank(
    map: &RepoMap,
    index: &ContextIndex,
    diff: &DiffSummary,
    task: &str,
    config: &ContextConfig,
) -> Vec<Candidate> {
    let terms = terms(task);
    let mut candidates: Vec<Candidate> = map
        .files
        .iter()
        .map(|file| {
            let (score, reasons) = score(map, index, diff, &file.path, &terms);
            Candidate {
                path: file.path.clone(),
                score,
                reasons,
                selected: false,
                bytes: file.size as usize,
                estimated_tokens: estimated_tokens(file.size as usize),
            }
        })
        .collect();
    candidates.sort_by(|a, b| b.score.cmp(&a.score).then_with(|| a.path.cmp(&b.path)));
    candidates.truncate(config.max_candidates);
    candidates
}

/// Select slices for the highest-ranked files, respecting the byte and slice
/// limits. Reads happen lazily and never touch sensitive files.
pub fn select_slices(
    root: &Path,
    map: &RepoMap,
    index: &ContextIndex,
    diff: &DiffSummary,
    task: &str,
    ranked: &mut [Candidate],
    config: &ContextConfig,
) -> (Vec<ContextSlice>, usize, bool) {
    let terms = terms(task);
    let mut slices = Vec::new();
    let mut total_bytes = 0usize;
    let mut truncated = false;
    let mut selected = 0usize;

    for candidate in ranked.iter_mut() {
        if selected >= config.max_files {
            break;
        }
        let Some(entry) = map.file(&candidate.path) else {
            continue;
        };
        if !entry.supported || entry.sensitive || entry.binary || entry.huge {
            continue;
        }
        let content = match fs::read_to_string(root.join(&candidate.path)) {
            Ok(content) => content,
            Err(_) => continue,
        };
        if (content.len() as u64) > config.max_file_bytes {
            continue;
        }
        let lines: Vec<&str> = content.lines().collect();
        let file_symbols = index
            .file(&candidate.path)
            .map(|file| file.symbols.clone())
            .unwrap_or_default();
        let mut ranges: Vec<(u32, u32)> = file_symbols
            .iter()
            .filter(|symbol| {
                terms
                    .iter()
                    .any(|term| symbol.name.to_ascii_lowercase().contains(term))
                    || diff
                        .changed_lines
                        .get(&candidate.path)
                        .map(|changed| {
                            changed.iter().any(|range| {
                                symbol.start_line <= range.end && symbol.end_line >= range.start
                            })
                        })
                        .unwrap_or(false)
            })
            .map(|symbol| (symbol.start_line, symbol.end_line))
            .collect();
        if ranges.is_empty() {
            ranges = file_symbols
                .iter()
                .take(3)
                .map(|symbol| (symbol.start_line, symbol.end_line))
                .collect();
        }
        if ranges.is_empty() {
            // No symbols: a short head of the file is still useful.
            ranges.push((1, lines.len().min(40) as u32));
        }
        candidate.selected = true;
        selected += 1;
        for (start, end) in ranges {
            if slices.len() >= config.max_slices {
                truncated = true;
                break;
            }
            let start_index = start.saturating_sub(1) as usize;
            let end_index = (end as usize).min(lines.len());
            if start_index >= lines.len() || start_index >= end_index {
                continue;
            }
            let body = lines[start_index..end_index].join("\n");
            let bytes = body.len();
            if total_bytes + bytes > config.max_bytes {
                truncated = true;
                break;
            }
            total_bytes += bytes;
            slices.push(ContextSlice {
                path: candidate.path.clone(),
                start_line: start,
                end_line: end.min(lines.len() as u32),
                bytes,
                estimated_tokens: estimated_tokens(bytes),
                content: body,
            });
        }
    }
    slices.sort_by(|a, b| (&a.path, a.start_line).cmp(&(&b.path, b.start_line)));
    (slices, total_bytes, truncated)
}

/// Build the provenance record for a plan.
///
/// Freshness fingerprints are always content SHA-256, never Git blob ids, so
/// they can be revalidated without invoking git. Sensitive files are skipped.
pub fn provenance(
    root: &Path,
    index: &ContextIndex,
    slices: &[ContextSlice],
    task: &str,
    now: i64,
) -> Provenance {
    let mut sources: Vec<SourceFingerprint> = slices
        .iter()
        .filter(|slice| !crate::context::classify::classify(Path::new(&slice.path)).sensitive)
        .filter_map(|slice| {
            let bytes = fs::read(root.join(&slice.path)).ok()?;
            Some(SourceFingerprint {
                path: slice.path.clone(),
                fingerprint: format!("sha256:{}", crate::hash::sha256_hex(&bytes)),
                size: bytes.len() as u64,
            })
        })
        .collect();
    sources.sort_by(|a, b| a.path.cmp(&b.path));
    sources.dedup_by(|a, b| a.path == b.path);
    Provenance {
        engine_version: ENGINE_VERSION.to_string(),
        schema_version: SCHEMA_VERSION,
        repo_id: index.repo_id.clone(),
        git_head: None,
        git_dirty: false,
        generated_at: now,
        sources,
        stale: false,
        validated: true,
        notes: vec![format!("task fingerprint: {}", fingerprint_text(task))],
    }
}

/// A stable, short fingerprint of any text.
pub fn fingerprint_text(text: &str) -> String {
    format!(
        "sha256:{}",
        crate::hash::sha256_hex(text.as_bytes())
    )
}

/// Assemble the plan from already-computed pieces.
#[allow(clippy::too_many_arguments)]
pub fn assemble(
    root: &Path,
    task: &str,
    role: Option<&str>,
    map: &RepoMap,
    index: &ContextIndex,
    diff: &DiffSummary,
    mut ranked: Vec<Candidate>,
    config: &ContextConfig,
    enrichment: PlanEnrichment,
    now: i64,
) -> Result<ContextPlan> {
    if !root.is_dir() {
        return Err(OcgError::config(format!(
            "{} is not a directory",
            root.display()
        )));
    }
    let (slices, selected_bytes, truncated) =
        select_slices(root, map, index, diff, task, &mut ranked, config);
    let selected_files: Vec<String> = ranked
        .iter()
        .filter(|candidate| candidate.selected)
        .map(|candidate| candidate.path.clone())
        .collect();
    let candidate_bytes = ranked.iter().map(|candidate| candidate.bytes).sum();
    let sensitive_excluded = map.files.iter().filter(|file| file.sensitive).count();
    let mut provenance = provenance(root, index, &slices, task, now);
    provenance.git_head = diff.state.head.clone();
    provenance.git_dirty = diff.state.dirty;
    // One human-facing notes field: `ContextPlan.notes`.
    let mut notes = Vec::new();
    if truncated {
        notes.push("content slices were truncated by the configured limits".to_string());
    }
    if diff.truncated {
        notes.push("git diff hunks were truncated by the configured limits".to_string());
    }
    if diff.capture_truncated {
        notes.push("git diff capture was truncated before parsing".to_string());
    }
    if !diff.unsourced_paths.is_empty() {
        notes.push(format!(
            "deleted paths have no current source: {}",
            diff.unsourced_paths.join(", ")
        ));
    }
    if map.truncated || index.truncated {
        notes.push(
            "repository file scan stopped at context.maxRepositoryFiles; the repo map and index are incomplete"
                .to_string(),
        );
    }
    if selected_files.len()
        < ranked
            .iter()
            .filter(|c| candidate_would_select(c, map))
            .count()
    {
        notes
            .push("some eligible files were not selected because maxFiles was reached".to_string());
    }
    let repository_truncated = map.truncated || index.truncated;
    let changed_paths = diff.changed_paths();
    let policy = PolicyState::detect(root);
    let instructions = GearInstructions {
        engine_version: ENGINE_VERSION.to_string(),
        commands: vec![
            "ocg context <task>".to_string(),
            "ocg verify <fast|normal|full>".to_string(),
            "ocg tools <task>".to_string(),
            "ocg checkpoint list".to_string(),
        ],
        note: "OCG produces deterministic plans and artifacts; OCG owns execution, conversation, provider and tool semantics.".to_string(),
    };
    let capsule = build_capsule(task, index, &ranked, diff, &enrichment, provenance.clone());
    Ok(ContextPlan {
        schema_version: SCHEMA_VERSION,
        engine_version: ENGINE_VERSION.to_string(),
        task: task.to_string(),
        role: role.map(str::to_string),
        repo_id: index.repo_id.clone(),
        root: root.to_string_lossy().into_owned(),
        git: diff.state.clone(),
        sections: sections(),
        candidates: ranked,
        selected_files,
        changed_paths,
        diff: Some(diff.clone()),
        slices,
        candidate_bytes,
        selected_bytes,
        estimated_tokens: estimated_tokens(selected_bytes),
        limits: config.limits(),
        provenance,
        sensitive_excluded,
        truncated: truncated || diff.truncated || diff.capture_truncated || repository_truncated,
        notes,
        instructions,
        policy,
        capabilities: enrichment.capabilities,
        capsule,
        test_proposal: enrichment.test_proposal,
        verification: enrichment.verification,
    })
}

/// Build the task capsule embedded in a plan. It reflects what the plan knows
/// and nothing more: no fabricated goal, decision or verification result.
fn build_capsule(
    task: &str,
    index: &ContextIndex,
    ranked: &[Candidate],
    diff: &DiffSummary,
    enrichment: &PlanEnrichment,
    provenance: Provenance,
) -> Option<crate::context::capsule::TaskCapsule> {
    let mut capsule = crate::context::capsule::TaskCapsule::new(task);
    let changed: std::collections::BTreeSet<String> = diff.changed_paths().into_iter().collect();
    for candidate in ranked.iter().filter(|candidate| candidate.selected) {
        capsule.files.push(crate::context::capsule::CapsuleFile {
            path: candidate.path.clone(),
            reason: Some(candidate.reasons.join(",")),
            changed: changed.contains(&candidate.path),
        });
    }
    capsule.files.sort_by(|a, b| a.path.cmp(&b.path));
    let mut symbols = Vec::new();
    for candidate in ranked.iter().filter(|candidate| candidate.selected) {
        if let Some(file) = index.file(&candidate.path) {
            for symbol in &file.symbols {
                if symbol.kind == crate::context::symbols::SymbolKind::Import {
                    continue;
                }
                symbols.push(crate::context::symbols::SymbolRef::new(
                    &candidate.path,
                    symbol,
                ));
                if symbols.len() >= 200 {
                    break;
                }
            }
        }
        if symbols.len() >= 200 {
            break;
        }
    }
    capsule.symbols = symbols;
    capsule.git = diff.state.clone();
    for command in enrichment
        .verification
        .stages
        .iter()
        .find(|stage| stage.name == enrichment.verification.default_stage)
        .map(|stage| stage.commands.clone())
        .unwrap_or_default()
    {
        capsule
            .verification
            .push(crate::context::capsule::Verification {
                check: command,
                status: "not_run".to_string(),
            });
    }
    capsule.provenance = provenance;
    capsule.recompute_size();
    Some(capsule)
}

fn candidate_would_select(candidate: &Candidate, map: &RepoMap) -> bool {
    map.file(&candidate.path)
        .map(|entry| entry.supported && !entry.sensitive && !entry.binary && !entry.huge)
        .unwrap_or(false)
}
