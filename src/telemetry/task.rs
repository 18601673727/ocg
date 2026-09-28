//! The telemetry event schema and its metadata sanitizer.
//!
//! One event describes one local OCG flow (a context plan or a
//! verification stage). The schema is deliberately wide enough to become the
//! input of a future budget controller or capability router, but nothing in
//! this repository makes a decision from it yet.
//!
//! Privacy is enforced at the schema boundary:
//!
//! - a task id is a caller-supplied safe id or a deterministic hash, never a
//!   raw prompt or task description;
//! - every free-text metadata field is passed through [`redact`], which replaces
//!   a secret-shaped value with `[redacted]` before it can be written;
//! - the store re-checks the serialized line and refuses to write one that still
//!   looks secret-like.
//!
//! Source code, prompts, command strings, command output, headers, environment
//! dumps and absolute paths are never part of the schema.

use crate::telemetry::tokens::TokenCount;
use serde::{Deserialize, Serialize};

/// The event schema version. Bump when a field's meaning changes.
pub const EVENT_SCHEMA_VERSION: u32 = 1;

/// Placement the redactor writes over a secret-shaped value.
pub const REDACTED: &str = "[redacted]";

/// A deterministic, non-reversible outcome label. `Unknown` is used when the
/// flow did not produce a defensible success/failure verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Success,
    Failure,
    #[default]
    Unknown,
}

impl Outcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Outcome::Success => "success",
            Outcome::Failure => "failure",
            Outcome::Unknown => "unknown",
        }
    }
}

/// Byte accounting for one context plan. `reduction_bytes` is
/// `candidate_bytes - selected_bytes` (saturating), so the selected context is
/// never claimed to be larger than the candidates it came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ContextMetrics {
    pub candidate_bytes: u64,
    pub selected_bytes: u64,
    pub capsule_bytes: u64,
    pub reduction_bytes: u64,
}

impl ContextMetrics {
    /// Build the metrics from the plan byte counts.
    pub fn new(candidate_bytes: u64, selected_bytes: u64, capsule_bytes: u64) -> Self {
        Self {
            candidate_bytes,
            selected_bytes,
            capsule_bytes,
            reduction_bytes: candidate_bytes.saturating_sub(selected_bytes),
        }
    }
}

/// Repo-map, symbol-index and context-cache hit accounting. `cache_hit` is
/// `None` for a flow that did not look at the context cache (for example a
/// verification run), so "not applicable" is never counted as a miss.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct RepoMetrics {
    pub files: usize,
    pub symbols: usize,
    pub index_reused: usize,
    pub index_updated: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_hit: Option<bool>,
}

/// Verification attempt accounting. A command that was attempted but could not
/// even spawn counts as an attempt and a failure, never as a pass.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct VerificationMetrics {
    pub enabled: bool,
    pub ran: bool,
    pub attempts: usize,
    pub passed: usize,
    pub failed: usize,
    pub not_run: usize,
    pub targeted_candidates: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stage: Option<String>,
}

/// Orchestration accounting for one phase transition. All byte counts are byte
/// counts; no prompt, source slice or raw log content is copied. Every field is
/// optional/defaulted so an older event without orchestration still parses.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct OrchestrationMetrics {
    /// The controller phase (`idle`, `explore`, `build`, `verify`, `debug`,
    /// `done`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    /// The role that produced the context (`lead`, `explore`, ...).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// The role the context was projected for.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination: Option<String>,
    /// The build/verify/debug attempt index (1-based) when applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt: Option<usize>,
    /// The retry index (0 for the first attempt) when applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry: Option<usize>,
    /// The byte size of the rich source context before projection.
    pub rich_capsule_bytes: u64,
    /// The byte size of the projected hand-off capsule.
    pub handoff_capsule_bytes: u64,
    /// The byte size of the selected source slices (kept outside the capsule).
    pub selected_source_bytes: u64,
    /// The byte size of the bounded git diff context.
    pub diff_context_bytes: u64,
    /// The byte size of the verification block attached to the hand-off.
    pub verification_context_bytes: u64,
    /// The byte size of the dynamic context suffix actually sent to the model.
    pub model_dynamic_context_bytes: u64,
    /// Context-cache hits observed in this transition.
    pub cache_hits: usize,
    /// Symbol-index reuse hits observed in this transition.
    pub index_hits: usize,
    /// Verification outcome (`passed`, `failed`, `not_run`) when applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verification_outcome: Option<String>,
    /// The explainable reason a Debug hand-off was recommended.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub debug_reason: Option<String>,
}

/// Raw-versus-distilled log byte accounting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct LogMetrics {
    pub raw_bytes: u64,
    pub distilled_bytes: u64,
    pub reduction_bytes: u64,
}

impl LogMetrics {
    /// Build the metrics from raw and distilled byte sizes.
    pub fn new(raw_bytes: u64, distilled_bytes: u64) -> Self {
        Self {
            raw_bytes,
            distilled_bytes,
            reduction_bytes: raw_bytes.saturating_sub(distilled_bytes),
        }
    }
}

/// One local telemetry event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    pub schema_version: u32,
    /// Unix seconds.
    pub timestamp: i64,
    /// A safe id or a deterministic hash, never a raw prompt.
    pub task_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default)]
    pub input_tokens: TokenCount,
    #[serde(default)]
    pub output_tokens: TokenCount,
    #[serde(default)]
    pub duration_ms: u64,
    #[serde(default)]
    pub context: ContextMetrics,
    #[serde(default)]
    pub repo: RepoMetrics,
    #[serde(default)]
    pub verification: VerificationMetrics,
    #[serde(default)]
    pub logs: LogMetrics,
    #[serde(default)]
    pub orchestration: OrchestrationMetrics,
    /// Selected capability names, never capability arguments or tool output.
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub outcome: Outcome,
}

impl Event {
    /// A new event with empty metrics and explicit unknown tokens.
    pub fn new(task_id: impl Into<String>, timestamp: i64) -> Self {
        Self {
            schema_version: EVENT_SCHEMA_VERSION,
            timestamp,
            task_id: task_id.into(),
            session_id: None,
            task_type: None,
            role: None,
            provider: None,
            model: None,
            input_tokens: TokenCount::unknown(),
            output_tokens: TokenCount::unknown(),
            duration_ms: 0,
            context: ContextMetrics::default(),
            repo: RepoMetrics::default(),
            verification: VerificationMetrics::default(),
            logs: LogMetrics::default(),
            orchestration: OrchestrationMetrics::default(),
            capabilities: Vec::new(),
            outcome: Outcome::Unknown,
        }
    }

    /// A stable, non-reversible task id from an arbitrary seed.
    pub fn hashed_task_id(seed: &str) -> String {
        safe_task_id(seed)
    }

    /// Replace secret-shaped metadata and normalise the task id. Runs before
    /// serialization so a bad value can never reach the store.
    pub fn sanitize(mut self) -> Self {
        self.task_id = sanitize_task_id(&self.task_id);
        self.session_id = redact_optional(self.session_id);
        self.task_type = redact_optional(self.task_type);
        self.role = redact_optional(self.role);
        self.provider = redact_optional(self.provider);
        self.model = redact_optional(self.model);
        self.capabilities = self
            .capabilities
            .into_iter()
            .map(|name| redact(&name))
            .collect();
        if let Some(stage) = self.verification.stage.take() {
            self.verification.stage = Some(redact(&stage));
        }
        self.orchestration.phase = redact_optional(self.orchestration.phase);
        self.orchestration.source = redact_optional(self.orchestration.source);
        self.orchestration.destination = redact_optional(self.orchestration.destination);
        self.orchestration.verification_outcome =
            redact_optional(self.orchestration.verification_outcome);
        self.orchestration.debug_reason = redact_optional(self.orchestration.debug_reason);
        self
    }
}

/// A deterministic task id derived from a seed (usually task text plus role).
/// The seed itself is never stored.
pub fn safe_task_id(seed: &str) -> String {
    let digest = crate::runtime::hash::sha256_hex(seed.as_bytes());
    let short = digest.get(..16).unwrap_or(&digest);
    format!("task-{short}")
}

/// Whether a candidate task/session id is safe to store verbatim: bounded, no
/// path separators, no secret shape.
pub fn is_safe_id(candidate: &str) -> bool {
    !candidate.is_empty()
        && candidate.len() <= 96
        && candidate
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.')
        && !is_secret_like(candidate)
}

/// Keep a caller-supplied safe id; hash anything else so raw text never lands
/// in the store.
pub fn sanitize_task_id(candidate: &str) -> String {
    if is_safe_id(candidate) {
        candidate.to_string()
    } else {
        safe_task_id(candidate)
    }
}

fn redact_optional(value: Option<String>) -> Option<String> {
    let value = value?;
    let value = redact(&value);
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

/// Replace a secret-shaped value with [`REDACTED`]; leave ordinary metadata
/// untouched.
pub fn redact(value: &str) -> String {
    if is_secret_like(value) {
        REDACTED.to_string()
    } else {
        value.to_string()
    }
}

/// Whether a string looks like a credential, private key or authorization
/// header. Deliberately conservative: a false positive only redacts a metadata
/// field.
pub fn is_secret_like(text: &str) -> bool {
    if text.contains(REDACTED) {
        return false;
    }
    if contains_run(text, "sk-", 20, |c| {
        c.is_ascii_alphanumeric() || c == '-' || c == '_'
    }) || contains_run(text, "ghp_", 36, |c| c.is_ascii_alphanumeric())
        || contains_run(text, "xoxb-", 20, |c| {
            c.is_ascii_alphanumeric() || c == '-' || c == '_'
        })
        || contains_run(text, "AIza", 30, |c| {
            c.is_ascii_alphanumeric() || c == '-' || c == '_'
        })
        || contains_run(text, "AKIA", 16, |c| {
            c.is_ascii_uppercase() || c.is_ascii_digit()
        })
    {
        return true;
    }
    if text.contains("PRIVATE KEY") || text.to_ascii_lowercase().contains("private key") {
        return true;
    }
    // `Authorization: Bearer <token>` in any casing/separator.
    let lower = text.to_ascii_lowercase();
    if lower.contains("bearer ")
        || lower.contains("authorization:")
        || lower.contains("authorization=")
    {
        return true;
    }
    // Key=value / key: value credential assignments.
    let keywords = [
        "password",
        "passwd",
        "secret",
        "api_key",
        "api-key",
        "apikey",
        "access_key",
        "access-key",
        "client_secret",
        "client-secret",
        "private_token",
        "auth_token",
        "credential",
        "token",
    ];
    keywords
        .iter()
        .any(|keyword| contains_assignment(&lower, keyword))
}

/// Whether `text` contains `keyword` followed (after optional spaces) by `=` or
/// `:` and then at least 8 value characters.
fn contains_assignment(text: &str, keyword: &str) -> bool {
    let mut search = text;
    while let Some(index) = search.find(keyword) {
        let rest = &search[index + keyword.len()..];
        let rest = rest.trim_start_matches([' ', '\t', '"', '\'']);
        if let Some(value) = rest.strip_prefix('=').or_else(|| rest.strip_prefix(':')) {
            let value = value.trim_start_matches([' ', '\t', '"', '\'']);
            let count = value
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || "._+/=~-".contains(*c))
                .count();
            if count >= 8 {
                return true;
            }
        }
        search = &search[index + keyword.len()..];
    }
    false
}

/// Whether `text` contains `marker` followed by at least `minimum` characters
/// accepted by `allowed`.
fn contains_run(text: &str, marker: &str, minimum: usize, allowed: impl Fn(char) -> bool) -> bool {
    let mut search = text;
    while let Some(index) = search.find(marker) {
        let rest = &search[index + marker.len()..];
        let count = rest.chars().take_while(|c| allowed(*c)).count();
        if count >= minimum {
            return true;
        }
        search = &search[index + marker.len()..];
    }
    false
}
