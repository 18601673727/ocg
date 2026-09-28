//! Durable, bounded rollover artifacts and continuation packets.
//!
//! A rollover artifact is an *intent and recovery record*, not a second source
//! of Mission truth.  The Mission remains authoritative for identity,
//! generation, progress and terminal state.  The artifact records the bounded
//! continuation packet and the exact sequence of runtime operations so a crash
//! can be diagnosed or resumed without copying the OpenCode transcript.

use crate::error::{OcgError, Result};
use crate::orchestration::context_governor::{
    context_artifacts_dir, safe_artifact_id, ContextGovernorConfig, ContextObservation,
    TelemetryProvenance,
};
use crate::orchestration::handoff::{HandoffFinding, HandoffVerification};
use crate::orchestration::mission::{Mission, NextAction};
use crate::orchestration::state::{Attempts, OrchestrationPhase};
use crate::runtime::lifecycle::RuntimeProfile;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

/// Current rollover artifact schema version.
pub const ROLLOVER_SCHEMA_VERSION: u32 = 1;
/// The stable marker used in continuation prompts.  It is deliberately plain
/// text so a prompt remains inspectable even when metadata is ignored by a
/// runtime.
pub const CONTINUATION_MARKER: &str = "OCG_CONTINUATION";
/// Maximum number of collection entries copied into a packet before the byte
/// budget is applied.  This is a second bound beside the byte cap.
pub const MAX_CONTINUATION_ITEMS: usize = 64;
/// Maximum number of rollover artifacts retained per project.
pub const MAX_ROLLOVER_ARTIFACTS: usize = 64;

/// The state of one prepared session replacement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RolloverStatus {
    /// The continuation is durable; the old Mission binding is unchanged.
    #[default]
    Prepared,
    /// A fresh target session exists and its Lead was verified.
    TargetReady,
    /// The target and expected owner are recorded; cutover has not started.
    CutoverIntent,
    /// Mission ownership points at the target, but continuation is not yet
    /// acknowledged by the target dispatch.
    Active,
    /// The target received the continuation packet.
    Applied,
    /// A safe retry is possible after the configured cooldown.
    Failed,
    /// The expected owner/revision no longer matches; no automatic overwrite.
    Conflict,
    /// A user or an operator explicitly abandoned this artifact.
    Aborted,
}

impl RolloverStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Prepared => "prepared",
            Self::TargetReady => "target_ready",
            Self::CutoverIntent => "cutover_intent",
            Self::Active => "active",
            Self::Applied => "applied",
            Self::Failed => "failed",
            Self::Conflict => "conflict",
            Self::Aborted => "aborted",
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Applied | Self::Aborted)
    }
}

/// Legacy sanitized Lead projection retained for artifacts written before the
/// runtime-neutral profile. It contains no service URL, password, provider
/// credential or raw runtime response.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct LeadBinding {
    pub level: Option<String>,
    pub agent: String,
    pub provider_id: String,
    pub model_id: String,
    pub variant: Option<String>,
}

impl LeadBinding {
    /// Compatibility projection for artifacts written before the runtime
    /// lifecycle profile was introduced.
    pub fn from_runtime_profile(profile: &RuntimeProfile) -> Self {
        let (provider_id, model_id) = profile
            .model_selector
            .split_once('/')
            .map(|(provider, model)| (provider.to_string(), model.to_string()))
            .unwrap_or_else(|| (String::new(), profile.model_selector.clone()));
        Self {
            level: profile.level.clone(),
            agent: profile.profile_id.clone(),
            provider_id,
            model_id,
            variant: profile.variant.clone(),
        }
    }

    pub fn to_runtime_profile(&self) -> Option<RuntimeProfile> {
        if self.agent.is_empty() || self.provider_id.is_empty() || self.model_id.is_empty() {
            return None;
        }
        let mut profile = RuntimeProfile::new(
            self.agent.clone(),
            format!("{}/{}", self.provider_id, self.model_id),
            self.variant.clone(),
        );
        if let Some(level) = &self.level {
            profile = profile.with_level(level.clone());
        }
        Some(profile)
    }
}

/// A bounded, task-scoped continuation packet derived only from Mission state.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ContinuationPacket {
    pub schema_version: u32,
    pub mission_id: String,
    pub generation: u32,
    pub task: Option<String>,
    pub goal: Option<String>,
    pub constraints: Vec<String>,
    pub findings: Vec<HandoffFinding>,
    pub files: Vec<String>,
    pub symbols: Vec<String>,
    pub failures: Vec<String>,
    pub evidence: Vec<String>,
    pub verification: Option<HandoffVerification>,
    pub checkpoints: Vec<String>,
    pub phase: OrchestrationPhase,
    pub attempts: Attempts,
    pub next_action: NextActionValue,
    pub digest: String,
    pub bytes: usize,
    pub omitted: Vec<String>,
}

/// A serializable spelling of [`NextAction`].  Keeping this local avoids
/// exposing a controller-only enum in the artifact schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NextActionValue {
    #[default]
    Lead,
    Explore,
    Build,
    Verify,
    Debug,
    Escalate,
    Complete,
    Failed,
    Cancelled,
}

impl From<NextAction> for NextActionValue {
    fn from(value: NextAction) -> Self {
        match value {
            NextAction::Lead => Self::Lead,
            NextAction::Explore => Self::Explore,
            NextAction::Build => Self::Build,
            NextAction::Verify => Self::Verify,
            NextAction::Debug => Self::Debug,
            NextAction::Escalate => Self::Escalate,
            NextAction::Complete => Self::Complete,
            NextAction::Failed => Self::Failed,
            NextAction::Cancelled => Self::Cancelled,
        }
    }
}

impl ContinuationPacket {
    /// Build a packet from the durable Mission, never from a transcript.
    pub fn from_mission(
        mission: &Mission,
        max_debug_retries: usize,
        max_bytes: usize,
    ) -> Result<Self> {
        if mission.mission_id.is_empty() {
            return Err(OcgError::config(
                "cannot build a continuation packet for an empty Mission",
            ));
        }
        let mut packet = Self {
            schema_version: ROLLOVER_SCHEMA_VERSION,
            mission_id: mission.mission_id.clone(),
            generation: mission.generation,
            task: safe_text(mission.task.as_deref()),
            goal: safe_text(mission.goal.as_deref()),
            constraints: safe_texts(&mission.constraints),
            findings: mission
                .findings
                .iter()
                .take(MAX_CONTINUATION_ITEMS)
                .map(sanitize_finding)
                .collect(),
            files: safe_paths(&mission.files),
            symbols: safe_texts(&mission.symbols),
            failures: safe_texts(&mission.failures),
            evidence: safe_texts(&mission.evidence),
            verification: mission.last_verification.clone().map(sanitize_verification),
            checkpoints: mission
                .checkpoints
                .iter()
                .take(MAX_CONTINUATION_ITEMS)
                .cloned()
                .collect(),
            phase: mission.phase,
            attempts: mission.attempts,
            next_action: mission.next_action(max_debug_retries).into(),
            digest: String::new(),
            bytes: 0,
            omitted: Vec::new(),
        };
        packet.fit(max_bytes)?;
        Ok(packet)
    }

    /// The prompt text used when the target session is resumed.  The marker and
    /// identity are outside the JSON body so a runtime that strips metadata still
    /// carries the crucial Mission/generation identity to the model.
    pub fn render(&self) -> String {
        let body = serde_json::to_string(self).unwrap_or_else(|_| "{}".to_string());
        format!(
            "{CONTINUATION_MARKER} mission_id={} generation={} digest={}\n\n{body}",
            self.mission_id, self.generation, self.digest
        )
    }

    fn fit(&mut self, max_bytes: usize) -> Result<()> {
        if max_bytes == 0 {
            return Err(OcgError::config(
                "continuation max bytes must be greater than zero",
            ));
        }
        // Required identity and state stay. Optional evidence is reduced in a
        // deterministic order until the serialized packet fits.
        let mut omitted = Vec::new();
        if self.serialized_len() > max_bytes {
            self.findings.truncate(8);
            omitted.push("findings reduced to eight".to_string());
        }
        if self.serialized_len() > max_bytes {
            self.evidence.clear();
            self.failures.truncate(4);
            omitted.push("optional failures/evidence reduced".to_string());
        }
        if self.serialized_len() > max_bytes {
            self.files.truncate(8);
            self.symbols.truncate(8);
            self.checkpoints.truncate(16);
            omitted.push("file/symbol/checkpoint references reduced".to_string());
        }
        if self.serialized_len() > max_bytes {
            self.task = self.task.take().map(|value| truncate_text(&value, 512));
            self.goal = self.goal.take().map(|value| truncate_text(&value, 512));
            self.constraints.truncate(8);
            omitted.push("task text reduced".to_string());
        }
        if self.serialized_len() > max_bytes {
            // Last resort: retain a deterministic, bounded textual summary of
            // the goal and references rather than emitting an invalid artifact.
            self.findings.clear();
            self.evidence.clear();
            self.failures.clear();
            self.verification = None;
            omitted.push("optional findings/evidence omitted to fit byte budget".to_string());
        }
        if self.serialized_len() > max_bytes {
            return Err(OcgError::config(format!(
                "Mission {} continuation cannot fit the configured {max_bytes} byte budget",
                self.mission_id
            )));
        }
        self.omitted = omitted;
        self.refresh_digest_and_bytes();
        if self.serialized_len() > max_bytes {
            return Err(OcgError::config(format!(
                "Mission {} continuation cannot fit the configured {max_bytes} byte budget",
                self.mission_id
            )));
        }
        Ok(())
    }

    fn serialized_len(&self) -> usize {
        serde_json::to_vec(self)
            .map(|bytes| bytes.len())
            .unwrap_or(usize::MAX)
    }

    fn refresh_digest_and_bytes(&mut self) {
        self.digest = String::new();
        self.bytes = serde_json::to_vec(self)
            .map(|bytes| bytes.len())
            .unwrap_or(0);
        let canonical = serde_json::to_vec(self).unwrap_or_default();
        self.digest = format!("sha256-{}", crate::runtime::hash::sha256_hex(&canonical));
        // `bytes` is the size of the packet without the self-referential digest
        // and byte field. Recompute the final serialized length for consumers.
        self.bytes = serde_json::to_vec(self)
            .map(|bytes| bytes.len())
            .unwrap_or(0);
    }
}

/// One durable rollover intent/recovery artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct RolloverArtifact {
    pub schema_version: u32,
    pub artifact_id: String,
    pub mission_id: String,
    pub generation: u32,
    pub source_session_id: String,
    pub target_session_id: Option<String>,
    pub status: RolloverStatus,
    pub reason: String,
    pub observation: ContextObservation,
    pub continuation: ContinuationPacket,
    pub continuation_digest: String,
    pub continuation_bytes: usize,
    /// Runtime-neutral profile used to prepare/recover the target. `lead` is
    /// retained for compatibility with artifacts written before this field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_profile: Option<RuntimeProfile>,
    /// Legacy OpenCode-shaped projection. New code reads `runtime_profile`
    /// first and migrates old records through `runtime_profile()`.
    pub lead: Option<LeadBinding>,
    /// Mission `updated_at` observed before any target cutover.  It is a cheap
    /// optimistic-concurrency witness; the Mission itself remains authoritative.
    pub base_mission_updated_at: i64,
    /// Monotonic Mission revision paired with the timestamp witness.  A
    /// rollover never overwrites a Mission that changed after preparation.
    pub base_mission_revision: u64,
    pub created_at: i64,
    pub updated_at: i64,
    pub last_attempt_at: Option<i64>,
    pub retry_after: Option<i64>,
    pub retry_count: u32,
    pub last_error: Option<String>,
}

impl Default for RolloverArtifact {
    fn default() -> Self {
        Self {
            schema_version: ROLLOVER_SCHEMA_VERSION,
            artifact_id: String::new(),
            mission_id: String::new(),
            generation: 1,
            source_session_id: String::new(),
            target_session_id: None,
            status: RolloverStatus::Prepared,
            reason: String::new(),
            observation: ContextObservation::default(),
            continuation: ContinuationPacket::default(),
            continuation_digest: String::new(),
            continuation_bytes: 0,
            runtime_profile: None,
            lead: None,
            base_mission_updated_at: 0,
            base_mission_revision: 0,
            created_at: 0,
            updated_at: 0,
            last_attempt_at: None,
            retry_after: None,
            retry_count: 0,
            last_error: None,
        }
    }
}

impl RolloverArtifact {
    pub fn prepare(
        mission: &Mission,
        source_session_id: &str,
        observation: ContextObservation,
        reason: impl Into<String>,
        lead: Option<LeadBinding>,
        now: i64,
        config: &ContextGovernorConfig,
    ) -> Result<Self> {
        Self::prepare_with_debug_retries(
            mission,
            source_session_id,
            observation,
            reason,
            lead,
            now,
            config,
            1,
        )
    }

    /// Prepare an artifact while preserving the controller's configured Debug
    /// retry budget in the derived `next_action` field. The original `prepare`
    /// API remains a conservative compatibility default.
    #[allow(clippy::too_many_arguments)]
    pub fn prepare_with_debug_retries(
        mission: &Mission,
        source_session_id: &str,
        observation: ContextObservation,
        reason: impl Into<String>,
        lead: Option<LeadBinding>,
        now: i64,
        config: &ContextGovernorConfig,
        max_debug_retries: usize,
    ) -> Result<Self> {
        if mission.is_terminal() {
            return Err(OcgError::config(format!(
                "Mission {} is terminal and cannot be rolled over",
                mission.mission_id
            )));
        }
        if source_session_id.is_empty() {
            return Err(OcgError::config("rollover source session is empty"));
        }
        if mission.session_id.as_deref() != Some(source_session_id) {
            return Err(OcgError::config(format!(
                "rollover source session {source_session_id} is not the current Mission owner"
            )));
        }
        let continuation = ContinuationPacket::from_mission(
            mission,
            max_debug_retries,
            config.max_continuation_bytes,
        )?;
        let reason =
            safe_text(Some(&reason.into())).unwrap_or_else(|| "context_pressure".to_string());
        let artifact_id = safe_artifact_id(&format!(
            "rollover-v1|{}|{}|{}|{}",
            mission.mission_id, mission.generation, source_session_id, observation.event_id
        ));
        let continuation_digest = continuation.digest.clone();
        let continuation_bytes = continuation.bytes;
        let mut artifact = Self {
            schema_version: ROLLOVER_SCHEMA_VERSION,
            artifact_id,
            mission_id: mission.mission_id.clone(),
            generation: mission.generation,
            source_session_id: source_session_id.to_string(),
            target_session_id: None,
            status: RolloverStatus::Prepared,
            reason,
            observation,
            continuation,
            continuation_digest,
            continuation_bytes,
            runtime_profile: None,
            lead,
            base_mission_updated_at: mission.updated_at,
            base_mission_revision: mission.revision,
            created_at: now,
            updated_at: now,
            last_attempt_at: Some(now),
            retry_after: None,
            retry_count: 0,
            last_error: None,
        };
        artifact.updated_at = now;
        Ok(artifact)
    }

    /// Refresh the optimistic Mission witness after the preparation transition
    /// has itself been durably recorded.
    pub fn set_base_mission(&mut self, mission: &Mission) {
        self.base_mission_updated_at = mission.updated_at;
        self.base_mission_revision = mission.revision;
    }

    /// Return the neutral profile, accepting the legacy Lead projection from
    /// artifacts written by the previous durable implementation.
    pub fn runtime_profile(&self) -> Option<RuntimeProfile> {
        self.runtime_profile
            .clone()
            .or_else(|| self.lead.as_ref().and_then(LeadBinding::to_runtime_profile))
    }

    pub fn mark_target_ready(
        &mut self,
        target_session_id: &str,
        lead: LeadBinding,
        now: i64,
    ) -> Result<()> {
        if target_session_id.is_empty()
            || (target_session_id == self.source_session_id
                && self.target_session_id.as_deref() != Some(target_session_id))
        {
            return Err(OcgError::config(
                "rollover target must be a non-empty session different from its source",
            ));
        }
        if !matches!(
            self.status,
            RolloverStatus::Prepared | RolloverStatus::Failed
        ) {
            return Err(OcgError::config(format!(
                "rollover artifact {} is {} and cannot accept a target",
                self.artifact_id,
                self.status.as_str()
            )));
        }
        if lead.agent.is_empty() || lead.provider_id.is_empty() || lead.model_id.is_empty() {
            return Err(OcgError::config(
                "rollover target Lead binding is incomplete",
            ));
        }
        self.target_session_id = Some(target_session_id.to_string());
        self.lead = Some(lead);
        self.status = RolloverStatus::TargetReady;
        self.updated_at = now;
        self.last_error = None;
        Ok(())
    }

    pub fn mark_cutover_intent(&mut self, now: i64) -> Result<()> {
        if self.target_session_id.is_none() {
            return Err(OcgError::config(
                "cannot record cutover intent before a target session is ready",
            ));
        }
        if self.status == RolloverStatus::CutoverIntent {
            self.updated_at = now;
            return Ok(());
        }
        if self.status != RolloverStatus::TargetReady {
            return Err(OcgError::config(format!(
                "rollover artifact {} must be target_ready before cutover intent (is {})",
                self.artifact_id,
                self.status.as_str()
            )));
        }
        self.status = RolloverStatus::CutoverIntent;
        self.updated_at = now;
        Ok(())
    }

    pub fn mark_active(&mut self, now: i64) -> Result<()> {
        if self.target_session_id.is_none() {
            return Err(OcgError::config(
                "cannot activate a rollover without a target",
            ));
        }
        if !matches!(
            self.status,
            RolloverStatus::CutoverIntent | RolloverStatus::Active | RolloverStatus::Failed
        ) {
            return Err(OcgError::config(format!(
                "rollover artifact {} must be cutover_intent before activation (is {})",
                self.artifact_id,
                self.status.as_str()
            )));
        }
        self.status = RolloverStatus::Active;
        self.updated_at = now;
        self.last_error = None;
        Ok(())
    }

    pub fn mark_applied(&mut self, now: i64) -> Result<()> {
        if self.target_session_id.is_none() {
            return Err(OcgError::config("cannot apply a rollover without a target"));
        }
        if self.status != RolloverStatus::Active {
            return Err(OcgError::config(format!(
                "rollover artifact {} must be active before acknowledgement (is {})",
                self.artifact_id,
                self.status.as_str()
            )));
        }
        self.status = RolloverStatus::Applied;
        self.updated_at = now;
        self.retry_after = None;
        self.last_error = None;
        Ok(())
    }

    pub fn mark_failed(&mut self, error: &str, now: i64, retry_after: Option<i64>) {
        if matches!(
            self.status,
            RolloverStatus::Applied | RolloverStatus::Conflict | RolloverStatus::Aborted
        ) {
            return;
        }
        self.status = RolloverStatus::Failed;
        self.updated_at = now;
        self.last_attempt_at = Some(now);
        self.retry_count = self.retry_count.saturating_add(1);
        self.retry_after = retry_after;
        self.last_error =
            Some(safe_text(Some(error)).unwrap_or_else(|| "rollover failed".to_string()));
    }

    pub fn mark_conflict(&mut self, reason: &str, now: i64) {
        if matches!(
            self.status,
            RolloverStatus::Applied | RolloverStatus::Conflict | RolloverStatus::Aborted
        ) {
            return;
        }
        self.status = RolloverStatus::Conflict;
        self.updated_at = now;
        self.last_error =
            Some(safe_text(Some(reason)).unwrap_or_else(|| "rollover conflict".to_string()));
    }

    pub fn can_retry(&self, now: i64) -> bool {
        matches!(self.status, RolloverStatus::Failed)
            && self.retry_after.map(|at| now >= at).unwrap_or(true)
    }

    /// The deterministic prompt identity used by a V2 continuation request.
    pub fn prompt_id(&self) -> String {
        format!("msg_ocg_rollover_{}", self.artifact_id.replace('-', "_"))
    }
}

fn safe_text(value: Option<&str>) -> Option<String> {
    value
        .filter(|value| !value.trim().is_empty())
        .map(crate::telemetry::task::redact)
}

fn truncate_text(value: &str, max_bytes: usize) -> String {
    let value = crate::telemetry::task::redact(value);
    if value.len() <= max_bytes {
        return value;
    }
    let mut end = max_bytes;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &value[..end])
}

fn safe_texts(values: &[String]) -> Vec<String> {
    values
        .iter()
        .take(MAX_CONTINUATION_ITEMS)
        .filter_map(|value| safe_text(Some(value)))
        .collect()
}

fn sanitize_finding(finding: &HandoffFinding) -> HandoffFinding {
    HandoffFinding {
        summary: crate::telemetry::task::redact(&finding.summary),
        detail: finding
            .detail
            .as_deref()
            .map(crate::telemetry::task::redact),
        source: finding.source.as_deref().map(|value| {
            let normalized = value.replace('\\', "/");
            if crate::context::classify::classify(std::path::Path::new(&normalized)).sensitive {
                "redacted-sensitive-path".to_string()
            } else {
                normalized
                    .rsplit('/')
                    .next()
                    .unwrap_or(&normalized)
                    .to_string()
            }
        }),
        severity: finding.severity,
    }
}

fn safe_paths(values: &[String]) -> Vec<String> {
    values
        .iter()
        .take(MAX_CONTINUATION_ITEMS)
        .filter_map(|value| {
            let value = value.replace('\\', "/");
            if crate::context::classify::classify(std::path::Path::new(&value)).sensitive {
                return None;
            }
            let value = value.rsplit('/').next().unwrap_or(&value).to_string();
            (!value.is_empty()).then_some(value)
        })
        .collect()
}

fn sanitize_verification(mut verification: HandoffVerification) -> HandoffVerification {
    verification.stage = crate::telemetry::task::redact(&verification.stage);
    verification.outcome = crate::telemetry::task::redact(&verification.outcome);
    verification.failed_commands = safe_texts(&verification.failed_commands);
    verification.failed_tests = safe_texts(&verification.failed_tests);
    verification.raw_log_refs = safe_paths(&verification.raw_log_refs);
    verification.distilled = safe_texts(&verification.distilled);
    verification
}

/// The rollover artifact directory for one project.
pub fn rollover_dir(root: &Path) -> PathBuf {
    context_artifacts_dir(root).join("rollovers")
}

/// The continuation packet directory for one project.
pub fn continuation_dir(root: &Path) -> PathBuf {
    context_artifacts_dir(root).join("continuations")
}

/// Validate an artifact id before it becomes a path component.
pub fn artifact_path(root: &Path, artifact_id: &str) -> Result<PathBuf> {
    if !crate::orchestration::checkpoint::is_safe_id(artifact_id) {
        return Err(OcgError::config(format!(
            "unsafe rollover artifact id '{artifact_id}'"
        )));
    }
    Ok(rollover_dir(root).join(format!("{artifact_id}.json")))
}

/// The path of the bounded continuation packet for one rollover artifact.
pub fn continuation_path(root: &Path, artifact_id: &str) -> Result<PathBuf> {
    if !crate::orchestration::checkpoint::is_safe_id(artifact_id) {
        return Err(OcgError::config(format!(
            "unsafe continuation artifact id '{artifact_id}'"
        )));
    }
    Ok(continuation_dir(root).join(format!("{artifact_id}.json")))
}

/// Persist a continuation packet separately from the rollover state machine.
/// The packet is also embedded in the artifact for recovery, but this sidecar
/// makes the bounded prompt independently inspectable and replaceable.
pub fn save_continuation(
    root: &Path,
    artifact_id: &str,
    packet: &ContinuationPacket,
) -> Result<PathBuf> {
    if packet.schema_version != ROLLOVER_SCHEMA_VERSION
        || packet.mission_id.is_empty()
        || packet.generation == 0
    {
        return Err(OcgError::config(
            "continuation packet has an invalid schema or identity",
        ));
    }
    crate::runtime::install::ensure_gitignore(root)?;
    let path = continuation_path(root, artifact_id)?;
    let value = serde_json::to_value(packet).map_err(|error| {
        OcgError::config(format!("cannot serialize continuation packet: {error}"))
    })?;
    crate::runtime::install::write_json_atomic(&path, &value)?;
    prune_continuations(&continuation_dir(root), &path);
    Ok(path)
}

/// Load a separately persisted continuation packet, if one exists.
pub fn load_continuation(root: &Path, artifact_id: &str) -> Result<Option<ContinuationPacket>> {
    let path = continuation_path(root, artifact_id)?;
    if !path.is_file() {
        return Ok(None);
    }
    let text = fs::read_to_string(&path).map_err(|error| OcgError::read(&path, error))?;
    let packet: ContinuationPacket = serde_json::from_str(&text).map_err(|error| {
        OcgError::config(format!(
            "continuation artifact {artifact_id} is corrupt: {error}"
        ))
    })?;
    if packet.schema_version != ROLLOVER_SCHEMA_VERSION {
        return Err(OcgError::config(format!(
            "continuation artifact {artifact_id} has unsupported schema_version {}",
            packet.schema_version
        )));
    }
    Ok(Some(packet))
}

/// Persist an artifact atomically, ensuring the state tree is ignored. The
/// embedded packet and its inspectable sidecar are written together; recovery
/// still has the embedded copy if a sidecar write is interrupted.
pub fn save(root: &Path, artifact: &RolloverArtifact) -> Result<PathBuf> {
    validate_artifact(artifact)?;
    crate::runtime::install::ensure_gitignore(root)?;
    let path = artifact_path(root, &artifact.artifact_id)?;
    // Write the sidecar first. If its directory is unavailable, the prior
    // artifact remains untouched; if activation fails, the next load reports
    // the explicit sidecar/artifact mismatch instead of silently adopting it.
    save_continuation(root, &artifact.artifact_id, &artifact.continuation)?;
    let value = serde_json::to_value(artifact).map_err(|error| {
        OcgError::config(format!("cannot serialize rollover artifact: {error}"))
    })?;
    crate::runtime::install::write_json_atomic(&path, &value)?;
    prune_artifacts(path.parent().unwrap_or(&continuation_dir(root)), &path);
    Ok(path)
}

fn prune_artifacts(directory: &Path, keep: &Path) {
    let Ok(entries) = fs::read_dir(directory) else {
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
    if files.len() <= MAX_ROLLOVER_ARTIFACTS {
        return;
    }
    let mut protected = HashSet::new();
    for (_, path) in &files {
        let terminal = fs::read_to_string(path)
            .ok()
            .and_then(|text| serde_json::from_str::<Value>(&text).ok())
            .and_then(|value| {
                value
                    .get("status")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
            .is_some_and(|status| matches!(status.as_str(), "applied" | "aborted"));
        if !terminal {
            protected.insert(path.clone());
        }
    }
    files.sort_by_key(|(modified, _)| *modified);
    let mut remaining = files.len();
    for (_, path) in files {
        if remaining <= MAX_ROLLOVER_ARTIFACTS {
            break;
        }
        if path == keep || protected.contains(&path) {
            continue;
        }
        if fs::remove_file(&path).is_ok() {
            remaining = remaining.saturating_sub(1);
        }
    }
}

fn prune_continuations(directory: &Path, keep: &Path) {
    let Ok(entries) = fs::read_dir(directory) else {
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
    if files.len() <= MAX_ROLLOVER_ARTIFACTS {
        return;
    }
    files.sort_by_key(|(modified, _)| *modified);
    let mut remaining = files.len();
    for (_, path) in files {
        if remaining <= MAX_ROLLOVER_ARTIFACTS {
            break;
        }
        if path == keep {
            continue;
        }
        if fs::remove_file(&path).is_ok() {
            remaining = remaining.saturating_sub(1);
        }
    }
}

/// Load and schema-check one artifact. Unlike disposable session state, an
/// invalid rollover record is never silently treated as absent: callers receive
/// an explicit error and can leave the old Mission binding untouched.
pub fn load(root: &Path, artifact_id: &str) -> Result<Option<RolloverArtifact>> {
    let path = artifact_path(root, artifact_id)?;
    if !path.is_file() {
        return Ok(None);
    }
    let text = fs::read_to_string(&path).map_err(|error| OcgError::read(&path, error))?;
    let artifact: RolloverArtifact = serde_json::from_str(&text).map_err(|error| {
        OcgError::config(format!(
            "rollover artifact {artifact_id} is corrupt: {error}"
        ))
    })?;
    if artifact.schema_version != ROLLOVER_SCHEMA_VERSION {
        return Err(OcgError::config(format!(
            "rollover artifact {artifact_id} has unsupported schema_version {} (expected {ROLLOVER_SCHEMA_VERSION})",
            artifact.schema_version
        )));
    }
    if artifact.artifact_id != artifact_id {
        return Err(OcgError::config(format!(
            "rollover artifact {artifact_id} contains mismatched identity {}",
            artifact.artifact_id
        )));
    }
    validate_artifact(&artifact)?;
    if let Some(continuation) = load_continuation(root, artifact_id)? {
        if continuation != artifact.continuation {
            return Err(OcgError::config(format!(
                "continuation artifact {artifact_id} does not match its rollover packet"
            )));
        }
    }
    Ok(Some(artifact))
}

fn validate_artifact(artifact: &RolloverArtifact) -> Result<()> {
    if artifact.schema_version != ROLLOVER_SCHEMA_VERSION {
        return Err(OcgError::config(format!(
            "rollover artifact {} has unsupported schema_version {}",
            artifact.artifact_id, artifact.schema_version
        )));
    }
    if !crate::orchestration::checkpoint::is_safe_id(&artifact.artifact_id)
        || !crate::orchestration::checkpoint::is_safe_id(&artifact.mission_id)
    {
        return Err(OcgError::config(
            "rollover artifact contains an unsafe identity",
        ));
    }
    if artifact.generation == 0 || artifact.source_session_id.is_empty() {
        return Err(OcgError::config(
            "rollover artifact requires a positive generation and source session",
        ));
    }
    if artifact.continuation.mission_id != artifact.mission_id
        || artifact.continuation.generation != artifact.generation
    {
        return Err(OcgError::config(
            "rollover continuation identity does not match its artifact",
        ));
    }
    if artifact.continuation_digest != artifact.continuation.digest
        || artifact.continuation_bytes != artifact.continuation.bytes
    {
        return Err(OcgError::config(
            "rollover continuation digest or byte witness does not match its packet",
        ));
    }
    if artifact
        .target_session_id
        .as_deref()
        .is_some_and(|target| target == artifact.source_session_id)
    {
        return Err(OcgError::config(
            "rollover target must differ from its source session",
        ));
    }
    Ok(())
}

/// Find the newest artifact for one Mission, if any.
pub fn latest_for_mission(root: &Path, mission_id: &str) -> Result<Option<RolloverArtifact>> {
    let dir = rollover_dir(root);
    let Ok(entries) = fs::read_dir(dir) else {
        return Ok(None);
    };
    let mut newest: Option<(i64, RolloverArtifact)> = None;
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        let Some(name) = path.file_stem().and_then(|value| value.to_str()) else {
            continue;
        };
        let artifact = load(root, name)?.ok_or_else(|| {
            OcgError::config(format!(
                "rollover artifact {name} disappeared while listing"
            ))
        })?;
        if artifact.mission_id == mission_id
            && newest
                .as_ref()
                .map(|(at, _)| artifact.updated_at >= *at)
                .unwrap_or(true)
        {
            newest = Some((artifact.updated_at, artifact));
        }
    }
    Ok(newest.map(|(_, artifact)| artifact))
}

/// Whether a provenance value is safe to include in a diagnostic.
pub fn provenance_label(value: TelemetryProvenance) -> &'static str {
    value.as_str()
}

/// Convert a packet to a compact JSON value for a V2 prompt metadata field.
pub fn continuation_metadata(packet: &ContinuationPacket) -> Value {
    serde_json::json!({
        "marker": CONTINUATION_MARKER,
        "mission_id": packet.mission_id,
        "generation": packet.generation,
        "digest": packet.digest,
        "bytes": packet.bytes,
        "schema_version": packet.schema_version,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestration::context_governor::{ContextGovernorConfig, ContextObservation};
    use crate::orchestration::mission::Mission;

    fn mission() -> Mission {
        let mut mission = Mission::admit("task-0123456789abcdef", "do work", "session-old", 1);
        mission.goal = Some("finish".to_string());
        mission.findings.push(HandoffFinding {
            summary: "one".to_string(),
            detail: None,
            source: Some("src/a.rs".to_string()),
            severity: crate::orchestration::handoff::Severity::Warning,
        });
        mission.checkpoints.push("cp-1".to_string());
        mission
    }

    #[test]
    fn packet_is_bounded_digestible_and_derived_from_mission() {
        let packet = ContinuationPacket::from_mission(&mission(), 1, 2048).unwrap();
        assert!(packet.bytes <= 2048);
        assert!(packet.digest.starts_with("sha256-"));
        assert_eq!(packet.generation, 1);
        assert_eq!(packet.checkpoints, vec!["cp-1"]);
        assert!(packet.render().contains("mission_id=task-0123456789abcdef"));
    }

    #[test]
    fn continuation_sanitizes_finding_text_and_sensitive_paths() {
        let mut source = mission();
        let secret = format!("sk-{}", "A".repeat(40));
        source.findings.push(HandoffFinding {
            summary: secret.clone(),
            detail: Some(secret),
            source: Some("config/auth.json".to_string()),
            severity: crate::orchestration::handoff::Severity::Critical,
        });
        let packet = ContinuationPacket::from_mission(&source, 1, 4096).unwrap();
        assert!(!serde_json::to_string(&packet)
            .unwrap()
            .contains(&format!("sk-{}", "A".repeat(40))));
        assert!(packet
            .findings
            .iter()
            .all(|finding| finding.source.as_deref() != Some("config/auth.json")));
    }

    #[test]
    fn an_impossible_continuation_budget_fails_instead_of_writing_an_oversize_packet() {
        assert!(ContinuationPacket::from_mission(&mission(), 1, 1).is_err());
    }

    #[test]
    fn artifact_preserves_generation_and_does_not_change_owner_until_cutover() {
        let mut artifact = RolloverArtifact::prepare(
            &mission(),
            "session-old",
            ContextObservation::default(),
            "context budget",
            None,
            2,
            &ContextGovernorConfig::default(),
        )
        .unwrap();
        assert_eq!(artifact.generation, 1);
        assert_eq!(artifact.status, RolloverStatus::Prepared);
        assert!(artifact.target_session_id.is_none());
        artifact
            .mark_target_ready(
                "session-new",
                LeadBinding {
                    agent: "lead-high".to_string(),
                    provider_id: "p".to_string(),
                    model_id: "m".to_string(),
                    ..LeadBinding::default()
                },
                3,
            )
            .unwrap();
        assert_eq!(artifact.source_session_id, "session-old");
        assert_eq!(artifact.target_session_id.as_deref(), Some("session-new"));
        assert_eq!(artifact.generation, 1);
    }

    #[test]
    fn legacy_lead_binding_projects_to_a_runtime_profile() {
        let binding = LeadBinding {
            level: Some("high".to_string()),
            agent: "lead-high".to_string(),
            provider_id: "provider".to_string(),
            model_id: "model".to_string(),
            variant: Some("high".to_string()),
        };
        let profile = binding.to_runtime_profile().unwrap();
        assert_eq!(profile.profile_id, "lead-high");
        assert_eq!(profile.model_selector, "provider/model");
        assert_eq!(profile.level.as_deref(), Some("high"));

        let mut artifact = RolloverArtifact::prepare(
            &mission(),
            "session-old",
            ContextObservation::default(),
            "context budget",
            Some(binding),
            2,
            &ContextGovernorConfig::default(),
        )
        .unwrap();
        artifact.runtime_profile = None;
        let mut value = serde_json::to_value(&artifact).unwrap();
        value.as_object_mut().unwrap().remove("runtime_profile");
        let decoded: RolloverArtifact = serde_json::from_value(value).unwrap();
        assert_eq!(decoded.runtime_profile().unwrap(), profile);
    }

    #[test]
    fn artifact_round_trips_and_rejects_bad_identity() {
        let dir = tempfile::tempdir().unwrap();
        let artifact = RolloverArtifact::prepare(
            &mission(),
            "session-old",
            ContextObservation::default(),
            "context budget",
            None,
            2,
            &ContextGovernorConfig::default(),
        )
        .unwrap();
        let id = artifact.artifact_id.clone();
        save(dir.path(), &artifact).unwrap();
        assert!(continuation_path(dir.path(), &id).unwrap().is_file());
        assert_eq!(load(dir.path(), &id).unwrap().unwrap(), artifact);
        assert!(load(dir.path(), "../escape").is_err());
        let continuation = continuation_path(dir.path(), &id).unwrap();
        std::fs::write(continuation, b"{").unwrap();
        assert!(load(dir.path(), &id).is_err());
    }
}
