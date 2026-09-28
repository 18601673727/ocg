//! Context telemetry and the conservative session-rollover governor.
//!
//! This module deliberately separates *observation* from *action*. A runtime
//! adapter can report a token count for one model request, but that number is
//! not by itself proof of the active context window. The governor keeps the
//! normalized usage fields, records their provenance, and only compares a
//! measured value with a limit that the adapter actually reported. No
//! denominator or percentage is fabricated when either side is unavailable.
//!
//! The durable telemetry types here are local and deliberately boring. Runtime
//! adapters normalize their engine-specific response shapes before conversion
//! through `ContextObservation::from_runtime`; network/API details remain in
//! the concrete OpenCode adapter.

use crate::error::{OcgError, Result};
use crate::runtime::lifecycle::{
    RuntimeContextObservation, RuntimeContextUsage, RuntimeModelMetadata, RuntimeProvenance,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::{Path, PathBuf};

/// Default warning threshold, expressed as a percentage of a trustworthy model
/// context limit.
pub const DEFAULT_APPROACHING_PERCENT: u8 = 70;
/// Default rollover threshold, expressed as a percentage of a trustworthy
/// model context limit.
pub const DEFAULT_ROLLOVER_PERCENT: u8 = 80;
/// Default maximum size of a continuation packet.
pub const DEFAULT_MAX_CONTINUATION_BYTES: usize = 16_384;
/// Default delay before a failed rollover is attempted again.
pub const DEFAULT_RETRY_COOLDOWN_SECONDS: i64 = 60;
/// Current artifact schema version.
pub const CONTEXT_ARTIFACT_SCHEMA_VERSION: u32 = 1;
/// Maximum number of local context observations retained for diagnostics.
pub const MAX_CONTEXT_OBSERVATIONS: usize = 128;

/// How much confidence a context measurement has.
///
/// `Exact` means the value was directly reported for the observed operation.
/// `Estimated` means the value is a conservative projection of an observed
/// message-level value. `Unknown` deliberately carries no number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TelemetryProvenance {
    Exact,
    Estimated,
    #[default]
    Unknown,
}

impl TelemetryProvenance {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Exact => "exact",
            Self::Estimated => "estimated",
            Self::Unknown => "unknown",
        }
    }

    pub fn is_usable(self) -> bool {
        matches!(self, Self::Exact | Self::Estimated)
    }
}

/// A configured action for one context band.
///
/// `Rollover` is the only action that can create a replacement session.  It is
/// honored only at a verified safe boundary; an unsafe crossing is recorded as
/// pending and never acted on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GovernorAction {
    #[default]
    Continue,
    Warn,
    Rollover,
}

impl GovernorAction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Continue => "continue",
            Self::Warn => "warn",
            Self::Rollover => "rollover",
        }
    }
}

/// Which context band an observation falls into.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GovernorState {
    Disabled,
    #[default]
    Normal,
    Approaching,
    RolloverRequired,
    Unknown,
}

impl GovernorState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Normal => "normal",
            Self::Approaching => "approaching",
            Self::RolloverRequired => "rollover_required",
            Self::Unknown => "unknown",
        }
    }
}

/// The small policy nested under `orchestration.contextGovernor`.
///
/// Threshold values are ordered during configuration parsing.  An absolute cap
/// is optional and is used when the model limit is unavailable; it is an
/// internal safety budget, not a claim about the provider's context window.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ContextGovernorConfig {
    pub enabled: bool,
    pub approaching_percent: u8,
    pub rollover_percent: u8,
    pub absolute_cap_tokens: Option<u64>,
    pub normal: GovernorAction,
    pub approaching: GovernorAction,
    pub rollover_required: GovernorAction,
    pub unknown: GovernorAction,
    pub max_continuation_bytes: usize,
    pub retry_cooldown_seconds: i64,
}

impl Default for ContextGovernorConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            approaching_percent: DEFAULT_APPROACHING_PERCENT,
            rollover_percent: DEFAULT_ROLLOVER_PERCENT,
            absolute_cap_tokens: None,
            normal: GovernorAction::Continue,
            approaching: GovernorAction::Warn,
            rollover_required: GovernorAction::Rollover,
            unknown: GovernorAction::Warn,
            max_continuation_bytes: DEFAULT_MAX_CONTINUATION_BYTES,
            retry_cooldown_seconds: DEFAULT_RETRY_COOLDOWN_SECONDS,
        }
    }
}

impl ContextGovernorConfig {
    /// Parse the nested JSON object.  A few descriptive aliases are accepted
    /// so configuration can use either `approaching` or
    /// `approachingBehavior` without proliferating policy fields.
    pub fn from_value(value: &Value) -> Result<Self> {
        if value.is_null() {
            return Ok(Self::default());
        }
        let object = value
            .as_object()
            .ok_or_else(|| OcgError::config("orchestration.contextGovernor must be an object"))?;
        let mut config = Self::default();
        if let Some(value) = object.get("enabled") {
            config.enabled = value.as_bool().ok_or_else(|| {
                OcgError::config("orchestration.contextGovernor.enabled must be a boolean")
            })?;
        }
        if let Some(value) = first(
            object,
            &[
                "approachingPercent",
                "approaching_percent",
                "approachingThresholdPercent",
            ],
        ) {
            config.approaching_percent = parse_percent(value, "approachingPercent")?;
        }
        if let Some(value) = first(
            object,
            &[
                "rolloverPercent",
                "rollover_percent",
                "rolloverThresholdPercent",
            ],
        ) {
            config.rollover_percent = parse_percent(value, "rolloverPercent")?;
        }
        if let Some(value) = object
            .get("absoluteCapTokens")
            .or_else(|| object.get("absolute_cap_tokens"))
        {
            if !value.is_null() {
                config.absolute_cap_tokens = Some(parse_positive_u64(
                    value,
                    "orchestration.contextGovernor.absoluteCapTokens",
                )?);
            }
        }
        if let Some(value) = first(object, &["normal", "normalBehavior", "normal_behavior"]) {
            config.normal = parse_action(value, "normal")?;
        }
        if let Some(value) = first(
            object,
            &["approaching", "approachingBehavior", "approaching_behavior"],
        ) {
            config.approaching = parse_action(value, "approaching")?;
        }
        if let Some(value) = first(
            object,
            &[
                "rolloverRequired",
                "rolloverRequiredBehavior",
                "rollover_required_behavior",
            ],
        ) {
            config.rollover_required = parse_action(value, "rolloverRequired")?;
        }
        if let Some(value) = first(object, &["unknown", "unknownBehavior", "unknown_behavior"]) {
            config.unknown = parse_action(value, "unknown")?;
        }
        if let Some(value) = first(object, &["maxContinuationBytes", "max_continuation_bytes"]) {
            config.max_continuation_bytes =
                parse_positive_usize(value, "orchestration.contextGovernor.maxContinuationBytes")?;
        }
        if let Some(value) = first(object, &["retryCooldownSeconds", "retry_cooldown_seconds"]) {
            let seconds = value.as_i64().ok_or_else(|| {
                OcgError::config(
                    "orchestration.contextGovernor.retryCooldownSeconds must be a non-negative integer",
                )
            })?;
            if seconds < 0 {
                return Err(OcgError::config(
                    "orchestration.contextGovernor.retryCooldownSeconds must be non-negative",
                ));
            }
            config.retry_cooldown_seconds = seconds;
        }
        config.validate_values()?;
        Ok(config)
    }

    pub fn validate_values(&self) -> Result<()> {
        if self.approaching_percent == 0 || self.approaching_percent > 100 {
            return Err(OcgError::config(
                "orchestration.contextGovernor.approachingPercent must be between 1 and 100",
            ));
        }
        if self.rollover_percent <= self.approaching_percent || self.rollover_percent > 100 {
            return Err(OcgError::config(
                "orchestration.contextGovernor.rolloverPercent must be greater than approachingPercent and at most 100",
            ));
        }
        if self.max_continuation_bytes == 0 || self.max_continuation_bytes > 1_048_576 {
            return Err(OcgError::config(
                "orchestration.contextGovernor.maxContinuationBytes must be between 1 and 1048576",
            ));
        }
        if self.retry_cooldown_seconds < 0 {
            return Err(OcgError::config(
                "orchestration.contextGovernor.retryCooldownSeconds must be non-negative",
            ));
        }
        if matches!(self.unknown, GovernorAction::Rollover) {
            return Err(OcgError::config(
                "orchestration.contextGovernor.unknown must be continue or warn; unknown telemetry is non-destructive",
            ));
        }
        Ok(())
    }
}

fn first<'a>(object: &'a serde_json::Map<String, Value>, keys: &[&str]) -> Option<&'a Value> {
    keys.iter().find_map(|key| object.get(*key))
}

fn parse_percent(value: &Value, label: &str) -> Result<u8> {
    let value = value.as_u64().ok_or_else(|| {
        OcgError::config(format!(
            "orchestration.contextGovernor.{label} must be an integer"
        ))
    })?;
    u8::try_from(value).map_err(|_| {
        OcgError::config(format!(
            "orchestration.contextGovernor.{label} must be between 1 and 100"
        ))
    })
}

fn parse_positive_u64(value: &Value, label: &str) -> Result<u64> {
    let value = value
        .as_u64()
        .filter(|number| *number > 0)
        .ok_or_else(|| OcgError::config(format!("{label} must be a positive integer")))?;
    Ok(value)
}

fn parse_positive_usize(value: &Value, label: &str) -> Result<usize> {
    let value = parse_positive_u64(value, label)?;
    usize::try_from(value).map_err(|_| OcgError::config(format!("{label} is too large")))
}

fn parse_action(value: &Value, label: &str) -> Result<GovernorAction> {
    match value.as_str().unwrap_or("") {
        "continue" | "none" => Ok(GovernorAction::Continue),
        "warn" | "diagnostic" => Ok(GovernorAction::Warn),
        "rollover" | "replace" => Ok(GovernorAction::Rollover),
        _ => Err(OcgError::config(format!(
            "orchestration.contextGovernor.{label} must be continue, warn, or rollover"
        ))),
    }
}

/// Token fields normalized from one runtime observation.
///
/// The active-context projection uses one message's `input + cache.read`.
/// Cache writes remain separate, and a transcript is never summed into an
/// active-context measurement.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct TokenUsage {
    pub input: Option<u64>,
    pub output: Option<u64>,
    pub reasoning: Option<u64>,
    pub cache_read: Option<u64>,
    pub cache_write: Option<u64>,
}

impl From<RuntimeContextUsage> for TokenUsage {
    fn from(usage: RuntimeContextUsage) -> Self {
        Self {
            input: usage.input,
            output: usage.output,
            reasoning: usage.reasoning,
            cache_read: usage.cache_read,
            cache_write: usage.cache_write,
        }
    }
}

impl From<RuntimeProvenance> for TelemetryProvenance {
    fn from(provenance: RuntimeProvenance) -> Self {
        match provenance {
            RuntimeProvenance::Exact => Self::Exact,
            RuntimeProvenance::Estimated => Self::Estimated,
            RuntimeProvenance::Unknown => Self::Unknown,
        }
    }
}

impl From<RuntimeModelMetadata> for ModelMetadata {
    fn from(model: RuntimeModelMetadata) -> Self {
        Self {
            provider_id: model.provider_id,
            model_id: model.model_id,
            context_limit: model.context_limit,
            input_limit: model.input_limit,
            output_limit: model.output_limit,
            effective_limit: model.effective_limit,
            source: model.source,
        }
    }
}

impl TokenUsage {
    pub fn is_empty(&self) -> bool {
        self.input.is_none()
            && self.output.is_none()
            && self.reasoning.is_none()
            && self.cache_read.is_none()
            && self.cache_write.is_none()
    }

    /// Project the active context carried by one V2 message.
    ///
    /// The runtime's message accounting may split the request into `input`
    /// and `cache.read`; the effective request context is their sum. Cache
    /// writes are deliberately excluded: they account for newly written cache
    /// entries, not an additional active-prompt item. The addition is checked
    /// so malformed or future values cannot wrap into a small, safe number.
    pub fn active_context_tokens(&self) -> Result<Option<u64>> {
        match (self.input, self.cache_read) {
            (None, None) => Ok(None),
            (Some(input), None) => Ok(Some(input)),
            (None, Some(cache_read)) => Ok(Some(cache_read)),
            (Some(input), Some(cache_read)) => {
                input.checked_add(cache_read).map(Some).ok_or_else(|| {
                    OcgError::config(
                        "OpenCode V2 context token projection overflowed; telemetry is unknown",
                    )
                })
            }
        }
    }

    /// Same projection as [`Self::active_context_tokens`], with overflow
    /// represented as an absent value for callers that must never fail open
    /// with an invented number.
    pub fn active_context_tokens_lossless(&self) -> Option<u64> {
        self.active_context_tokens().ok().flatten()
    }
}

/// Model limits normalized by the runtime adapter.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ModelMetadata {
    pub provider_id: Option<String>,
    pub model_id: Option<String>,
    pub context_limit: Option<u64>,
    pub input_limit: Option<u64>,
    pub output_limit: Option<u64>,
    /// The limit OCG uses for the active-context comparison.  It is present
    /// only when the runtime supplied a real model limit.
    pub effective_limit: Option<u64>,
    pub source: Option<String>,
}

impl ModelMetadata {
    pub fn is_trustworthy_limit(&self) -> bool {
        self.effective_limit.filter(|limit| *limit > 0).is_some()
    }
}

/// A compact, privacy-safe observation of one runtime step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ContextObservation {
    pub schema_version: u32,
    pub session_id: String,
    pub event_id: String,
    pub observed_at: i64,
    pub assistant_message_id: Option<String>,
    pub finish: Option<String>,
    pub safe_boundary: bool,
    pub usage: TokenUsage,
    /// The value used for the comparison: one runtime message's
    /// `input + cache.read` projection. Cache writes are retained separately
    /// in [`Self::usage`] and never added to this value.
    pub used_tokens: Option<u64>,
    pub limit_tokens: Option<u64>,
    pub model: ModelMetadata,
    pub message_count: usize,
    pub compaction_count: usize,
    /// Provenance of the raw usage fields.
    pub usage_provenance: TelemetryProvenance,
    /// Provenance of the active-context projection. A present value is
    /// conservatively marked estimated when the runtime reports message-level
    /// usage rather than a separate active-context counter.
    pub context_provenance: TelemetryProvenance,
    pub note: Option<String>,
}

impl Default for ContextObservation {
    fn default() -> Self {
        Self {
            schema_version: CONTEXT_ARTIFACT_SCHEMA_VERSION,
            session_id: String::new(),
            event_id: String::new(),
            observed_at: 0,
            assistant_message_id: None,
            finish: None,
            safe_boundary: false,
            usage: TokenUsage::default(),
            used_tokens: None,
            limit_tokens: None,
            model: ModelMetadata::default(),
            message_count: 0,
            compaction_count: 0,
            usage_provenance: TelemetryProvenance::Unknown,
            context_provenance: TelemetryProvenance::Unknown,
            note: None,
        }
    }
}

impl ContextObservation {
    /// Convert the runtime-neutral observation into the durable telemetry
    /// shape. No OpenCode response fields cross this boundary.
    pub fn from_runtime(observation: RuntimeContextObservation) -> Self {
        Self {
            schema_version: CONTEXT_ARTIFACT_SCHEMA_VERSION,
            session_id: observation.execution_id.to_string(),
            event_id: observation.event_id,
            observed_at: observation.observed_at,
            assistant_message_id: observation.assistant_message_id,
            finish: observation.finish,
            safe_boundary: observation.safe_boundary,
            usage: observation.usage.into(),
            used_tokens: observation.used_tokens,
            limit_tokens: observation.limit_tokens,
            model: observation.model.into(),
            message_count: observation.message_count,
            compaction_count: observation.compaction_count,
            usage_provenance: observation.usage_provenance.into(),
            context_provenance: observation.context_provenance.into(),
            note: observation.note,
        }
    }

    pub fn unknown(session_id: &str, event_id: &str, now: i64, note: impl Into<String>) -> Self {
        Self {
            session_id: session_id.to_string(),
            event_id: event_id.to_string(),
            observed_at: now,
            note: Some(crate::telemetry::task::redact(&note.into())),
            ..Self::default()
        }
    }
}

/// The result of evaluating one observation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GovernorDecision {
    pub state: GovernorState,
    pub action: GovernorAction,
    /// Integer floor percentage, present only when both a usable numerator and
    /// a trustworthy denominator existed.
    pub utilization_percent: Option<u64>,
    pub rollover_allowed: bool,
    pub deferred_for_boundary: bool,
    pub reason: String,
}

impl GovernorDecision {
    fn new(state: GovernorState, action: GovernorAction, reason: impl Into<String>) -> Self {
        Self {
            state,
            action,
            utilization_percent: None,
            rollover_allowed: false,
            deferred_for_boundary: false,
            reason: reason.into(),
        }
    }
}

/// Evaluate an observation without performing I/O or creating a session.
pub fn decide(
    config: &ContextGovernorConfig,
    observation: &ContextObservation,
) -> GovernorDecision {
    if !config.enabled {
        return GovernorDecision::new(
            GovernorState::Disabled,
            GovernorAction::Continue,
            "disabled",
        );
    }
    let provenance = observation.context_provenance;
    let used = observation.used_tokens;
    let limit = observation.limit_tokens.filter(|limit| *limit > 0);
    let cap = config.absolute_cap_tokens.filter(|cap| *cap > 0);

    // An explicit cap is useful even when the model catalogue did not expose
    // a denominator.  It is still never applied to an unknown token number.
    if used.is_none() || !provenance.is_usable() {
        return GovernorDecision::new(
            GovernorState::Unknown,
            config.unknown,
            "context telemetry is unknown; no rollover was attempted",
        );
    }
    let used = used.expect("checked above");
    let ratio = limit.map(|limit| ((used as u128 * 10_000) / limit as u128) as u64);
    let percent = limit.map(|limit| ((used as u128 * 100) / limit as u128) as u64);
    let absolute = cap.is_some_and(|cap| used >= cap);
    // A raw usage number without a trustworthy denominator is not evidence of
    // a normal context window. Keep it visible as unknown unless the operator
    // supplied an explicit absolute safety cap.
    if limit.is_none() && !absolute {
        return GovernorDecision::new(
            GovernorState::Unknown,
            config.unknown,
            "context usage is known, but no trustworthy context limit is available",
        );
    }
    let (state, action) =
        if absolute || ratio.is_some_and(|ratio| ratio >= config.rollover_percent as u64 * 100) {
            (GovernorState::RolloverRequired, config.rollover_required)
        } else if ratio.is_some_and(|ratio| ratio >= config.approaching_percent as u64 * 100) {
            (GovernorState::Approaching, config.approaching)
        } else {
            (GovernorState::Normal, config.normal)
        };
    let reason = if absolute && limit.is_none() {
        "the configured absolute token cap was reached; no provider limit was trusted".to_string()
    } else if absolute {
        "the configured absolute token cap was reached".to_string()
    } else if state == GovernorState::RolloverRequired {
        "the context rollover threshold was reached".to_string()
    } else if state == GovernorState::Approaching {
        "the context approaching threshold was reached".to_string()
    } else {
        "context usage is within the configured thresholds".to_string()
    };
    let mut decision = GovernorDecision::new(state, action, reason);
    decision.utilization_percent = percent;
    decision.rollover_allowed = action == GovernorAction::Rollover;
    decision.deferred_for_boundary = decision.rollover_allowed && !observation.safe_boundary;
    if decision.deferred_for_boundary {
        decision
            .reason
            .push_str("; waiting for a safe semantic boundary");
    }
    decision
}

/// The directory containing context/rollover artifacts.
/// Persist one bounded context observation atomically. Telemetry is an
/// artifact, not a second state authority: it contains no transcript and is
/// never consulted to reconstruct a Mission.
pub fn save_observation(root: &Path, observation: &ContextObservation) -> Result<PathBuf> {
    if observation.schema_version != CONTEXT_ARTIFACT_SCHEMA_VERSION {
        return Err(OcgError::config(format!(
            "context observation has unsupported schema_version {}",
            observation.schema_version
        )));
    }
    if observation.event_id.is_empty() {
        return Err(OcgError::config(
            "context observation requires a deterministic event id",
        ));
    }
    if observation.event_id.len() > 256
        || crate::telemetry::task::is_secret_like(&observation.event_id)
    {
        return Err(OcgError::config(
            "context observation event id is not safe for durable telemetry",
        ));
    }
    let id = safe_artifact_id(&observation.event_id);
    crate::runtime::install::ensure_gitignore(root)?;
    let path = telemetry_dir(root).join(format!("{id}.json"));
    if path.is_file() {
        let existing = load_observation(root, &observation.event_id)?;
        if existing.as_ref() != Some(observation) {
            return Err(OcgError::config(
                "context telemetry event identity is already persisted with a different payload",
            ));
        }
        return Ok(path);
    }
    let value = serde_json::to_value(observation).map_err(|error| {
        OcgError::config(format!("cannot serialize context observation: {error}"))
    })?;
    crate::runtime::install::write_json_atomic(&path, &value)?;
    prune_observations(&telemetry_dir(root), &path);
    Ok(path)
}

/// Alias with a descriptive persistence verb for callers that treat telemetry
/// as an append-only observation store.
pub fn persist_observation(root: &Path, observation: &ContextObservation) -> Result<PathBuf> {
    save_observation(root, observation)
}

pub fn load_observation(root: &Path, event_id: &str) -> Result<Option<ContextObservation>> {
    let hashed_id = safe_artifact_id(event_id);
    let direct_id =
        crate::orchestration::checkpoint::is_safe_id(event_id).then(|| event_id.to_string());
    let candidates = direct_id
        .into_iter()
        .map(|id| (id, false))
        .chain(std::iter::once((hashed_id, true)))
        .collect::<Vec<_>>();
    let Some((path_id, hashed_path)) = candidates
        .into_iter()
        .find(|(id, _)| telemetry_dir(root).join(format!("{id}.json")).is_file())
    else {
        return Ok(None);
    };
    let path = telemetry_dir(root).join(format!("{path_id}.json"));
    if !path.is_file() {
        return Ok(None);
    }
    let text = std::fs::read_to_string(&path).map_err(|error| OcgError::read(&path, error))?;
    let observation: ContextObservation = serde_json::from_str(&text).map_err(|error| {
        OcgError::config(format!("context telemetry artifact is corrupt: {error}"))
    })?;
    if observation.schema_version != CONTEXT_ARTIFACT_SCHEMA_VERSION {
        return Err(OcgError::config(format!(
            "context telemetry artifact has unsupported schema_version {}",
            observation.schema_version
        )));
    }
    let identity_matches = if hashed_path {
        safe_artifact_id(&observation.event_id) == path_id
    } else {
        observation.event_id == event_id
    };
    if observation.event_id.is_empty() || !identity_matches {
        return Err(OcgError::config(
            "context telemetry artifact has a mismatched or empty event id",
        ));
    }
    Ok(Some(observation))
}

fn prune_observations(directory: &Path, keep: &Path) {
    let Ok(entries) = std::fs::read_dir(directory) else {
        return;
    };
    let mut files = entries
        .flatten()
        .filter_map(|entry| {
            let path = entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some("json") {
                return None;
            }
            let modified = entry
                .metadata()
                .and_then(|metadata| metadata.modified())
                .ok()?;
            Some((modified, path))
        })
        .collect::<Vec<_>>();
    if files.len() <= MAX_CONTEXT_OBSERVATIONS {
        return;
    }
    files.sort_by_key(|(modified, _)| *modified);
    let mut remaining = files.len();
    for (_, path) in files {
        if remaining <= MAX_CONTEXT_OBSERVATIONS {
            break;
        }
        if path == keep {
            continue;
        }
        if std::fs::remove_file(&path).is_ok() {
            remaining = remaining.saturating_sub(1);
        }
    }
}

pub fn context_artifacts_dir(root: &Path) -> PathBuf {
    crate::orchestration::state::state_dir(root).join("context")
}

/// The directory containing telemetry artifacts.
pub fn telemetry_dir(root: &Path) -> PathBuf {
    context_artifacts_dir(root).join("telemetry")
}

/// The directory containing rollover artifacts.
pub fn rollover_dir(root: &Path) -> PathBuf {
    context_artifacts_dir(root).join("rollovers")
}

/// The directory containing continuation packets.
pub fn continuation_dir(root: &Path) -> PathBuf {
    context_artifacts_dir(root).join("continuations")
}

pub fn safe_artifact_id(id: &str) -> String {
    let digest = crate::runtime::hash::sha256_hex(id.as_bytes());
    format!("a-{}", digest.get(..24).unwrap_or(&digest))
}

/// Build a deterministic event identity when the runtime did not provide one.
pub fn event_identity(
    session_id: &str,
    message_id: Option<&str>,
    finish: Option<&str>,
    created: Option<i64>,
) -> String {
    let raw = format!(
        "context-v1|{session_id}|{}|{}|{}",
        message_id.unwrap_or(""),
        finish.unwrap_or(""),
        created.map(|value| value.to_string()).unwrap_or_default()
    );
    safe_artifact_id(&raw)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::lifecycle::{
        RuntimeContextObservation, RuntimeContextUsage, RuntimeExecutionId, RuntimeProvenance,
    };
    use serde_json::json;

    fn observation(
        used: Option<u64>,
        limit: Option<u64>,
        provenance: TelemetryProvenance,
        safe: bool,
    ) -> ContextObservation {
        ContextObservation {
            used_tokens: used,
            limit_tokens: limit,
            context_provenance: provenance,
            safe_boundary: safe,
            ..ContextObservation::default()
        }
    }

    #[test]
    fn thresholds_are_ordered_and_unknown_cannot_rollover() {
        let mut config = ContextGovernorConfig::default();
        config.rollover_percent = config.approaching_percent;
        assert!(config.validate_values().is_err());
        config.unknown = GovernorAction::Rollover;
        assert!(config.validate_values().is_err());
    }

    #[test]
    fn normal_approaching_and_required_are_deterministic() {
        let config = ContextGovernorConfig::default();
        let normal = decide(
            &config,
            &observation(Some(10), Some(100), TelemetryProvenance::Estimated, true),
        );
        assert_eq!(normal.state, GovernorState::Normal);
        assert_eq!(normal.action, GovernorAction::Continue);
        let approaching = decide(
            &config,
            &observation(Some(70), Some(100), TelemetryProvenance::Exact, true),
        );
        assert_eq!(approaching.state, GovernorState::Approaching);
        let required = decide(
            &config,
            &observation(Some(90), Some(100), TelemetryProvenance::Exact, true),
        );
        assert_eq!(required.state, GovernorState::RolloverRequired);
        assert_eq!(required.utilization_percent, Some(90));
    }

    #[test]
    fn unsafe_required_boundary_is_deferred() {
        let config = ContextGovernorConfig::default();
        let decision = decide(
            &config,
            &observation(Some(99), Some(100), TelemetryProvenance::Exact, false),
        );
        assert_eq!(decision.state, GovernorState::RolloverRequired);
        assert!(decision.rollover_allowed);
        assert!(decision.deferred_for_boundary);
    }

    #[test]
    fn unknown_never_gets_a_percentage_or_rollover_action() {
        let config = ContextGovernorConfig::default();
        let decision = decide(
            &config,
            &observation(None, Some(100), TelemetryProvenance::Unknown, true),
        );
        assert_eq!(decision.state, GovernorState::Unknown);
        assert_eq!(decision.action, GovernorAction::Warn);
        assert_eq!(decision.utilization_percent, None);
        assert!(!decision.rollover_allowed);
    }

    #[test]
    fn absolute_cap_works_without_a_provider_denominator() {
        let config = ContextGovernorConfig {
            absolute_cap_tokens: Some(500),
            ..ContextGovernorConfig::default()
        };
        let decision = decide(
            &config,
            &observation(Some(500), None, TelemetryProvenance::Exact, true),
        );
        assert_eq!(decision.state, GovernorState::RolloverRequired);
        assert_eq!(decision.utilization_percent, None);
        assert!(decision.rollover_allowed);
    }

    #[test]
    fn usage_without_a_limit_or_cap_is_unknown_not_normal() {
        let decision = decide(
            &ContextGovernorConfig::default(),
            &observation(Some(12), None, TelemetryProvenance::Exact, true),
        );
        assert_eq!(decision.state, GovernorState::Unknown);
        assert_eq!(decision.action, GovernorAction::Warn);
        assert_eq!(decision.utilization_percent, None);
        assert!(!decision.rollover_allowed);
    }

    #[test]
    fn runtime_message_usage_is_not_summed_as_a_cumulative_context_total() {
        let result = ContextObservation::from_runtime(RuntimeContextObservation {
            execution_id: RuntimeExecutionId::new("s"),
            event_id: "e".to_string(),
            observed_at: 1,
            assistant_message_id: Some("m2".to_string()),
            finish: Some("stop".to_string()),
            safe_boundary: true,
            usage: RuntimeContextUsage {
                input: Some(150),
                output: Some(7),
                cache_read: Some(25),
                cache_write: Some(9),
                ..RuntimeContextUsage::default()
            },
            used_tokens: Some(175),
            limit_tokens: None,
            model: Default::default(),
            message_count: 2,
            compaction_count: 0,
            usage_provenance: RuntimeProvenance::Exact,
            context_provenance: RuntimeProvenance::Estimated,
            note: None,
        });
        assert_eq!(result.used_tokens, Some(175));
        assert_eq!(result.usage.cache_write, Some(9));
        assert_eq!(result.usage_provenance, TelemetryProvenance::Exact);
        assert_eq!(result.context_provenance, TelemetryProvenance::Estimated);
    }

    #[test]
    fn projection_overflow_is_unknown_instead_of_wrapping() {
        let usage = TokenUsage {
            input: Some(u64::MAX),
            cache_read: Some(1),
            ..TokenUsage::default()
        };
        assert!(usage.active_context_tokens().is_err());
        assert_eq!(usage.active_context_tokens_lossless(), None);
    }

    #[test]
    fn telemetry_round_trips_and_rejects_identity_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let value = ContextObservation {
            session_id: "ses-1".to_string(),
            event_id: "event-1".to_string(),
            used_tokens: Some(12),
            limit_tokens: Some(100),
            context_provenance: TelemetryProvenance::Exact,
            ..ContextObservation::default()
        };
        let path = save_observation(dir.path(), &value).unwrap();
        assert!(path.is_file());
        assert_eq!(
            load_observation(dir.path(), "event-1").unwrap(),
            Some(value.clone())
        );
        let generated = event_identity("ses-1", Some("m1"), Some("stop"), Some(1));
        let generated_value = ContextObservation {
            event_id: generated.clone(),
            ..value.clone()
        };
        save_observation(dir.path(), &generated_value).unwrap();
        assert_eq!(
            load_observation(dir.path(), &generated).unwrap(),
            Some(generated_value.clone())
        );
        let mut changed = value.clone();
        changed.used_tokens = Some(99);
        assert!(save_observation(dir.path(), &changed).is_err());
        let text = std::fs::read_to_string(&path).unwrap();
        let mut corrupt: serde_json::Value = serde_json::from_str(&text).unwrap();
        corrupt["event_id"] = json!("different-event");
        std::fs::write(path, serde_json::to_vec(&corrupt).unwrap()).unwrap();
        assert!(load_observation(dir.path(), "event-1").is_err());
    }

    #[test]
    fn config_aliases_and_caps_are_validated() {
        let value = json!({
            "approachingThresholdPercent": 60,
            "rolloverThresholdPercent": 80,
            "unknownBehavior": "diagnostic",
            "absoluteCapTokens": 1234,
            "maxContinuationBytes": 2048
        });
        let config = ContextGovernorConfig::from_value(&value).unwrap();
        assert_eq!(config.approaching_percent, 60);
        assert_eq!(config.rollover_percent, 80);
        assert_eq!(config.unknown, GovernorAction::Warn);
        assert_eq!(config.absolute_cap_tokens, Some(1234));
        assert_eq!(config.max_continuation_bytes, 2048);
    }
}
