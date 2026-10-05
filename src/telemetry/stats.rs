//! Read-only aggregation of the local telemetry JSONL.
//!
//! `ocg stats` uses this module to turn events into one deterministic project
//! aggregate plus a latest-event summary. It performs no network access, makes
//! no decisions and never changes a stored value. Estimated token counts stay
//! labelled as estimates and an unavailable count stays explicit `null`.

use crate::telemetry::store::{EventLog, TelemetryStore};
use crate::telemetry::task::{
    ContextMetrics, Event, LogMetrics, OrchestrationMetrics, Outcome, RepoMetrics,
    VerificationMetrics,
};
use crate::telemetry::tokens::{TokenCount, TokenSource};
use serde::Serialize;
use std::collections::BTreeMap;

/// Round a reduction to one decimal place. Never negative.
pub fn reduction_percent(reduced: u64, total: u64) -> f64 {
    if total == 0 {
        return 0.0;
    }
    let percent = (reduced as f64) * 100.0 / (total as f64);
    (percent * 10.0).round() / 10.0
}

/// Token totals, split by the exact source that supplied them.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct TokenTotals {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_reported: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated: Option<u64>,
    /// Events whose count was explicitly unknown.
    pub unknown_events: usize,
}

impl TokenTotals {
    fn add(&mut self, count: TokenCount) {
        match count.source {
            TokenSource::ProviderReported => {
                if let Some(total) = count.total {
                    let sum = self.provider_reported.unwrap_or(0).saturating_add(total);
                    self.provider_reported = Some(sum);
                }
            }
            TokenSource::Estimated => {
                if let Some(total) = count.total {
                    let sum = self.estimated.unwrap_or(0).saturating_add(total);
                    self.estimated = Some(sum);
                }
            }
            TokenSource::Unknown => self.unknown_events += 1,
        }
    }

    /// Whether any count at all was recorded.
    pub fn has_any(&self) -> bool {
        self.provider_reported.is_some() || self.estimated.is_some() || self.unknown_events > 0
    }
}

/// Outcome counters.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Outcomes {
    pub success: usize,
    pub failure: usize,
    pub unknown: usize,
}

/// Context byte totals.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ContextTotals {
    pub candidate_bytes: u64,
    pub selected_bytes: u64,
    pub capsule_bytes: u64,
    pub reduction_bytes: u64,
}

impl ContextTotals {
    fn add(&mut self, metrics: ContextMetrics) {
        self.candidate_bytes = self.candidate_bytes.saturating_add(metrics.candidate_bytes);
        self.selected_bytes = self.selected_bytes.saturating_add(metrics.selected_bytes);
        self.capsule_bytes = self.capsule_bytes.saturating_add(metrics.capsule_bytes);
        self.reduction_bytes = self.reduction_bytes.saturating_add(metrics.reduction_bytes);
    }

    /// The reduction percentage over the recorded candidates.
    pub fn reduction_percent(&self) -> f64 {
        reduction_percent(self.reduction_bytes, self.candidate_bytes)
    }
}

/// Repo-map, symbol-index and cache-hit totals.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct RepoTotals {
    pub files: u64,
    pub symbols: u64,
    pub index_reused: u64,
    pub index_updated: u64,
    pub cache_hits: usize,
    pub cache_misses: usize,
}

impl RepoTotals {
    fn add(&mut self, metrics: RepoMetrics) {
        self.files = self.files.saturating_add(metrics.files as u64);
        self.symbols = self.symbols.saturating_add(metrics.symbols as u64);
        self.index_reused = self
            .index_reused
            .saturating_add(metrics.index_reused as u64);
        self.index_updated = self
            .index_updated
            .saturating_add(metrics.index_updated as u64);
        match metrics.cache_hit {
            Some(true) => self.cache_hits += 1,
            Some(false) => self.cache_misses += 1,
            None => {}
        }
    }
}

/// Verification attempt totals.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct VerificationTotals {
    pub attempts: u64,
    pub passed: u64,
    pub failed: u64,
    pub not_run: u64,
    pub targeted_candidates: u64,
}

impl VerificationTotals {
    fn add(&mut self, metrics: VerificationMetrics) {
        self.attempts = self.attempts.saturating_add(metrics.attempts as u64);
        self.passed = self.passed.saturating_add(metrics.passed as u64);
        self.failed = self.failed.saturating_add(metrics.failed as u64);
        self.not_run = self.not_run.saturating_add(metrics.not_run as u64);
        self.targeted_candidates = self
            .targeted_candidates
            .saturating_add(metrics.targeted_candidates as u64);
    }
}

/// Orchestration transition totals. Byte counts are summed; phase and
/// debug-reason maps are keyed by redacted label so the aggregate stays safe.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct OrchestrationTotals {
    pub transitions: u64,
    pub rich_capsule_bytes: u64,
    pub handoff_capsule_bytes: u64,
    pub selected_source_bytes: u64,
    pub diff_context_bytes: u64,
    pub verification_context_bytes: u64,
    pub model_dynamic_context_bytes: u64,
    pub build_attempts: u64,
    pub verify_attempts: u64,
    pub debug_attempts: u64,
    pub retries: u64,
    pub cache_hits: u64,
    pub index_hits: u64,
    pub phases: BTreeMap<String, usize>,
    pub debug_reasons: BTreeMap<String, usize>,
}

impl OrchestrationTotals {
    fn add(&mut self, metrics: OrchestrationMetrics) {
        self.transitions += 1;
        self.rich_capsule_bytes = self
            .rich_capsule_bytes
            .saturating_add(metrics.rich_capsule_bytes);
        self.handoff_capsule_bytes = self
            .handoff_capsule_bytes
            .saturating_add(metrics.handoff_capsule_bytes);
        self.selected_source_bytes = self
            .selected_source_bytes
            .saturating_add(metrics.selected_source_bytes);
        self.diff_context_bytes = self
            .diff_context_bytes
            .saturating_add(metrics.diff_context_bytes);
        self.verification_context_bytes = self
            .verification_context_bytes
            .saturating_add(metrics.verification_context_bytes);
        self.model_dynamic_context_bytes = self
            .model_dynamic_context_bytes
            .saturating_add(metrics.model_dynamic_context_bytes);
        self.cache_hits = self.cache_hits.saturating_add(metrics.cache_hits as u64);
        self.index_hits = self.index_hits.saturating_add(metrics.index_hits as u64);
        if let Some(phase) = metrics.phase.as_deref() {
            *self.phases.entry(phase.to_string()).or_insert(0) += 1;
            match phase {
                "build" => self.build_attempts += 1,
                "verify" => self.verify_attempts += 1,
                "debug" => self.debug_attempts += 1,
                _ => {}
            }
        }
        if let Some(attempt) = metrics.attempt {
            self.retries = self
                .retries
                .saturating_add(attempt.saturating_sub(1) as u64);
        }
        if let Some(reason) = metrics.debug_reason {
            *self.debug_reasons.entry(reason).or_insert(0) += 1;
        }
    }
}

/// Raw-versus-distilled log totals.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct LogTotals {
    pub raw_bytes: u64,
    pub distilled_bytes: u64,
    pub reduction_bytes: u64,
}

impl LogTotals {
    fn add(&mut self, metrics: LogMetrics) {
        self.raw_bytes = self.raw_bytes.saturating_add(metrics.raw_bytes);
        self.distilled_bytes = self.distilled_bytes.saturating_add(metrics.distilled_bytes);
        self.reduction_bytes = self.reduction_bytes.saturating_add(metrics.reduction_bytes);
    }

    pub fn reduction_percent(&self) -> f64 {
        reduction_percent(self.reduction_bytes, self.raw_bytes)
    }
}
/// The project aggregate.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Aggregate {
    pub events: usize,
    pub outcomes: Outcomes,
    pub input_tokens: TokenTotals,
    pub output_tokens: TokenTotals,
    pub context: ContextTotals,
    pub repo: RepoTotals,
    pub verification: VerificationTotals,
    pub logs: LogTotals,
    pub orchestration: OrchestrationTotals,
    pub capabilities: BTreeMap<String, usize>,
}

impl Aggregate {
    /// Aggregate a slice of events. Order only affects the capability map's
    /// stable ordering, which is a `BTreeMap`.
    pub fn from_events(events: &[Event]) -> Self {
        let mut aggregate = Self::default();
        for event in events {
            aggregate.events += 1;
            match event.outcome {
                Outcome::Success => aggregate.outcomes.success += 1,
                Outcome::Failure => aggregate.outcomes.failure += 1,
                Outcome::Unknown => aggregate.outcomes.unknown += 1,
            }
            aggregate.input_tokens.add(event.input_tokens);
            aggregate.output_tokens.add(event.output_tokens);
            aggregate.context.add(event.context);
            aggregate.repo.add(event.repo);
            aggregate.verification.add(event.verification.clone());
            aggregate.logs.add(event.logs);
            aggregate.orchestration.add(event.orchestration.clone());
            for name in &event.capabilities {
                *aggregate.capabilities.entry(name.clone()).or_insert(0) += 1;
            }
        }
        aggregate
    }
}

/// A compact view of the newest event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EventSummary {
    pub timestamp: i64,
    pub task_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub destination: Option<String>,
    pub duration_ms: u64,
    pub outcome: Outcome,
}

impl EventSummary {
    fn from_event(event: &Event) -> Self {
        Self {
            timestamp: event.timestamp,
            task_id: event.task_id.clone(),
            session_id: event.session_id.clone(),
            task_type: event.task_type.clone(),
            role: event.role.clone(),
            provider: event.provider.clone(),
            model: event.model.clone(),
            phase: event.orchestration.phase.clone(),
            source: event.orchestration.source.clone(),
            destination: event.orchestration.destination.clone(),
            duration_ms: event.duration_ms,
            outcome: event.outcome,
        }
    }
}

/// Everything `ocg stats` reports, in a stable field order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TelemetryStats {
    pub enabled: bool,
    pub local_only: bool,
    pub path: String,
    pub exists: bool,
    pub bytes: u64,
    pub events: usize,
    pub corrupt_lines: usize,
    pub unsupported_lines: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oldest: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub newest: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest: Option<EventSummary>,
    pub aggregate: Aggregate,
}

impl TelemetryStats {
    /// Read the store without creating it and aggregate every parseable event.
    pub fn collect(store: &TelemetryStore) -> Self {
        let log = store.read();
        let config = *store.config();
        Self::from_log(&config, store.path(), store.exists(), store.bytes(), log)
    }

    /// Build stats from an already-read log. Exposed for tests.
    pub fn from_log(
        config: &crate::telemetry::TelemetryConfig,
        path: &std::path::Path,
        exists: bool,
        bytes: u64,
        log: EventLog,
    ) -> Self {
        let aggregate = Aggregate::from_events(&log.events);
        let mut oldest: Option<i64> = None;
        let mut newest: Option<i64> = None;
        let mut latest: Option<&Event> = None;
        for event in &log.events {
            oldest = Some(match oldest {
                Some(current) if current <= event.timestamp => current,
                _ => event.timestamp,
            });
            newest = Some(match newest {
                Some(current) if current >= event.timestamp => current,
                _ => event.timestamp,
            });
            if latest
                .map(|current| event.timestamp >= current.timestamp)
                .unwrap_or(true)
            {
                latest = Some(event);
            }
        }
        Self {
            enabled: config.enabled,
            local_only: config.local_only,
            path: path.to_string_lossy().into_owned(),
            exists,
            bytes,
            events: log.events.len(),
            corrupt_lines: log.corrupt_lines,
            unsupported_lines: log.unsupported_lines,
            oldest,
            newest,
            latest: latest.map(EventSummary::from_event),
            aggregate,
        }
    }

    /// The stable human-readable rendering used by `ocg stats`.
    pub fn render(&self) -> String {
        let mut out = String::new();
        out.push_str("OCG telemetry (local only)\n");
        out.push_str(&format!("  enabled:     {}\n", yes_no(self.enabled)));
        out.push_str(&format!("  local-only:  {}\n", yes_no(self.local_only)));
        out.push_str(&format!("  path:        {}\n", self.path));
        out.push_str(&format!(
            "  file:        {}\n",
            if self.exists {
                "present"
            } else {
                "not present"
            }
        ));
        out.push_str(&format!("  events:      {}\n", self.events));
        out.push_str(&format!(
            "  corrupt:     {} line(s) skipped\n",
            self.corrupt_lines
        ));
        if self.unsupported_lines > 0 {
            out.push_str(&format!(
                "  unsupported: {} line(s) from a newer schema skipped\n",
                self.unsupported_lines
            ));
        }
        if self.exists {
            out.push_str(&format!("  bytes:       {}\n", self.bytes));
        }
        match (self.oldest, self.newest) {
            (Some(oldest), Some(newest)) => {
                out.push_str(&format!(
                    "  window:      {oldest} .. {newest} (unix seconds)\n"
                ));
            }
            _ => out.push_str("  window:      -\n"),
        }

        if self.events == 0 {
            out.push_str("\nno telemetry events recorded yet.\n");
            if !self.enabled {
                out.push_str(
                    "telemetry is disabled; set \"telemetry\": {\"enabled\": true} to collect.\n",
                );
            }
            return out;
        }

        if let Some(latest) = &self.latest {
            out.push_str("\nlatest event:\n");
            out.push_str(&format!(
                "  time:        {} (unix seconds)\n",
                latest.timestamp
            ));
            out.push_str(&format!("  task:        {}\n", latest.task_id));
            out.push_str(&format!(
                "  session:     {}\n",
                latest.session_id.as_deref().unwrap_or("-")
            ));
            out.push_str(&format!(
                "  type:        {}\n",
                latest.task_type.as_deref().unwrap_or("-")
            ));
            out.push_str(&format!(
                "  role:        {}\n",
                latest.role.as_deref().unwrap_or("-")
            ));
            out.push_str(&format!(
                "  provider:    {}\n",
                latest.provider.as_deref().unwrap_or("-")
            ));
            out.push_str(&format!(
                "  model:       {}\n",
                latest.model.as_deref().unwrap_or("-")
            ));
            out.push_str(&format!(
                "  phase:       {}\n",
                latest.phase.as_deref().unwrap_or("-")
            ));
            out.push_str(&format!(
                "  route:       {} -> {}\n",
                latest.source.as_deref().unwrap_or("-"),
                latest.destination.as_deref().unwrap_or("-")
            ));
            out.push_str(&format!("  duration:    {} ms\n", latest.duration_ms));
            out.push_str(&format!("  outcome:     {}\n", latest.outcome.as_str()));
        }

        let aggregate = &self.aggregate;
        out.push_str("\ntokens (source labelled; estimates are not exact):\n");
        out.push_str(&render_tokens("input", &aggregate.input_tokens));
        out.push_str(&render_tokens("output", &aggregate.output_tokens));

        out.push_str("\ncontext (bytes):\n");
        out.push_str(&format!(
            "  candidate:   {}\n",
            aggregate.context.candidate_bytes
        ));
        out.push_str(&format!(
            "  selected:    {}\n",
            aggregate.context.selected_bytes
        ));
        out.push_str(&format!(
            "  capsule:     {}\n",
            aggregate.context.capsule_bytes
        ));
        out.push_str(&format!(
            "  reduction:   {} ({:.1}%)\n",
            aggregate.context.reduction_bytes,
            aggregate.context.reduction_percent()
        ));

        out.push_str("\nrepo / index / cache:\n");
        out.push_str(&format!("  files:       {}\n", aggregate.repo.files));
        out.push_str(&format!("  symbols:     {}\n", aggregate.repo.symbols));
        out.push_str(&format!(
            "  index reused:  {}\n",
            aggregate.repo.index_reused
        ));
        out.push_str(&format!(
            "  index updated: {}\n",
            aggregate.repo.index_updated
        ));
        out.push_str(&format!("  cache hits:  {}\n", aggregate.repo.cache_hits));
        out.push_str(&format!(
            "  cache misses: {}\n",
            aggregate.repo.cache_misses
        ));

        out.push_str("\nverification:\n");
        out.push_str(&format!(
            "  attempts:    {}\n",
            aggregate.verification.attempts
        ));
        out.push_str(&format!(
            "  passed:      {}\n",
            aggregate.verification.passed
        ));
        out.push_str(&format!(
            "  failed:      {}\n",
            aggregate.verification.failed
        ));
        out.push_str(&format!(
            "  not run:     {}\n",
            aggregate.verification.not_run
        ));
        out.push_str(&format!(
            "  targeted candidates: {}\n",
            aggregate.verification.targeted_candidates
        ));

        out.push_str("\nlogs (bytes):\n");
        out.push_str(&format!("  raw:         {}\n", aggregate.logs.raw_bytes));
        out.push_str(&format!(
            "  distilled:   {}\n",
            aggregate.logs.distilled_bytes
        ));
        out.push_str(&format!(
            "  reduction:   {} ({:.1}%)\n",
            aggregate.logs.reduction_bytes,
            aggregate.logs.reduction_percent()
        ));

        out.push_str("\norchestration (bytes unless noted):\n");
        out.push_str(&format!(
            "  transitions: {} (build {}, verify {}, debug {}, retries {})\n",
            aggregate.orchestration.transitions,
            aggregate.orchestration.build_attempts,
            aggregate.orchestration.verify_attempts,
            aggregate.orchestration.debug_attempts,
            aggregate.orchestration.retries,
        ));
        out.push_str(&format!(
            "  rich capsule:   {}\n",
            aggregate.orchestration.rich_capsule_bytes
        ));
        out.push_str(&format!(
            "  handoff capsule: {}\n",
            aggregate.orchestration.handoff_capsule_bytes
        ));
        out.push_str(&format!(
            "  selected source: {}\n",
            aggregate.orchestration.selected_source_bytes
        ));
        out.push_str(&format!(
            "  diff context:    {}\n",
            aggregate.orchestration.diff_context_bytes
        ));
        out.push_str(&format!(
            "  verification:    {}\n",
            aggregate.orchestration.verification_context_bytes
        ));
        out.push_str(&format!(
            "  model dynamic:   {}\n",
            aggregate.orchestration.model_dynamic_context_bytes
        ));
        out.push_str(&format!(
            "  cache hits:      {}\n",
            aggregate.orchestration.cache_hits
        ));
        out.push_str(&format!(
            "  index hits:      {}\n",
            aggregate.orchestration.index_hits
        ));
        if !aggregate.orchestration.phases.is_empty() {
            out.push_str("  phases:\n");
            for (phase, count) in &aggregate.orchestration.phases {
                out.push_str(&format!("    {phase}: {count}\n"));
            }
        }
        if !aggregate.orchestration.debug_reasons.is_empty() {
            out.push_str("  debug reasons:\n");
            for (reason, count) in &aggregate.orchestration.debug_reasons {
                out.push_str(&format!("    {reason}: {count}\n"));
            }
        }

        out.push_str("\noutcomes:\n");
        out.push_str(&format!("  success:     {}\n", aggregate.outcomes.success));
        out.push_str(&format!("  failure:     {}\n", aggregate.outcomes.failure));
        out.push_str(&format!("  unknown:     {}\n", aggregate.outcomes.unknown));

        out.push_str(&format!(
            "\ncapabilities ({}):\n",
            aggregate.capabilities.len()
        ));
        if aggregate.capabilities.is_empty() {
            out.push_str("  (none recorded)\n");
        } else {
            for (name, count) in &aggregate.capabilities {
                out.push_str(&format!("  {name}: {count}\n"));
            }
        }
        out
    }
}

fn yes_no(value: bool) -> &'static str {
    if value {
        "yes"
    } else {
        "no"
    }
}

fn render_tokens(label: &str, totals: &TokenTotals) -> String {
    let mut out = String::new();
    out.push_str(&format!("  {label}:\n"));
    out.push_str(&format!(
        "    provider_reported: {}\n",
        match totals.provider_reported {
            Some(total) => format!("{total} (exact)"),
            None => "- (unavailable)".to_string(),
        }
    ));
    out.push_str(&format!(
        "    estimated:         {}\n",
        match totals.estimated {
            Some(total) => format!("{total} (estimate only)"),
            None => "- (unavailable)".to_string(),
        }
    ));
    out.push_str(&format!(
        "    unknown:           {} event(s)\n",
        totals.unknown_events
    ));
    out
}
