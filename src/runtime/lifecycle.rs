//! Runtime-neutral execution lifecycle vocabulary.
//!
//! This is the small boundary between durable OCG control-plane semantics and
//! a concrete execution engine. It contains only operations that the current
//! controller and bridge already need. OpenCode HTTP, session routes, agent
//! names, and plugin event shapes belong to the concrete adapter, not here.

use crate::telemetry::task::redact;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;
use std::fmt;

/// An opaque execution identity owned by a runtime adapter.
///
/// It is intentionally not a Mission identity. A Mission may outlive many
/// execution objects, and a rollover replaces this value without changing the
/// Mission ID or generation.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RuntimeExecutionId(String);

impl RuntimeExecutionId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RuntimeExecutionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl From<String> for RuntimeExecutionId {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<&str> for RuntimeExecutionId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

/// Maximum number of parent edges accepted from a runtime. A longer chain
/// cannot be used as evidence of Mission ownership.
pub const MAX_EXECUTION_LINEAGE_DEPTH: usize = 32;

/// Verified, runtime-neutral ancestry for a single execution.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeExecutionLineage {
    pub execution_id: RuntimeExecutionId,
    pub parent_id: Option<RuntimeExecutionId>,
    pub root_id: RuntimeExecutionId,
    pub depth: usize,
}

/// Walk the runtime's durable immediate-parent relation. Each visited object,
/// including the root, must exist; no result is cached across dispatches.
pub fn resolve_execution_lineage(
    adapter: &dyn RuntimeAdapter,
    execution_id: &RuntimeExecutionId,
) -> RuntimeResult<RuntimeExecutionLineage> {
    if execution_id.as_str().is_empty() {
        return Err(RuntimeError::new(
            RuntimeErrorKind::InvalidResponse,
            "empty execution identity",
        ));
    }
    let mut current = execution_id.clone();
    let mut visited = HashSet::new();
    let mut parent_id = None;
    let mut depth = 0;
    loop {
        if !visited.insert(current.clone()) {
            return Err(RuntimeError::new(
                RuntimeErrorKind::InvalidResponse,
                "execution lineage cycle",
            ));
        }
        let parent = adapter.execution_parent(&current)?;
        if depth == 0 {
            parent_id = parent.clone();
        }
        match parent {
            None => {
                return Ok(RuntimeExecutionLineage {
                    execution_id: execution_id.clone(),
                    parent_id,
                    root_id: current,
                    depth,
                })
            }
            Some(next) if next.as_str().is_empty() => {
                return Err(RuntimeError::new(
                    RuntimeErrorKind::InvalidResponse,
                    "empty execution lineage parent",
                ))
            }
            Some(_) if depth == MAX_EXECUTION_LINEAGE_DEPTH => {
                return Err(RuntimeError::new(
                    RuntimeErrorKind::InvalidResponse,
                    "execution lineage exceeds depth limit",
                ))
            }
            Some(next) => {
                depth += 1;
                current = next;
            }
        }
    }
}

/// A stable, credential-free identity for one durable control-plane recovery
/// operation.
///
/// The key is deliberately separate from [`RuntimeExecutionId`]: it names the
/// logical create/reuse attempt, not the runtime object that the attempt may
/// create.  Adapters that can recover an interrupted create can use this key
/// to find the already-created object; adapters that cannot must return an
/// explicit unsupported/unknown error rather than guessing from an agent name
/// or selecting an unrelated root session.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RuntimeRecoveryKey {
    pub mission_id: String,
    pub generation: u32,
    pub operation_id: String,
}

impl RuntimeRecoveryKey {
    pub fn new(
        mission_id: impl Into<String>,
        generation: u32,
        operation_id: impl Into<String>,
    ) -> Self {
        Self {
            mission_id: mission_id.into(),
            generation,
            operation_id: operation_id.into(),
        }
    }
}

/// A logical execution profile selected by policy and applied by an adapter.
///
/// `profile_id` and `model_selector` are opaque to the control plane. The
/// OpenCode adapter currently maps them to an agent and provider/model; another
/// adapter may use the same fields for a different profile mechanism.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeProfile {
    pub profile_id: String,
    pub model_selector: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub level: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variant: Option<String>,
}

impl RuntimeProfile {
    pub fn new(
        profile_id: impl Into<String>,
        model_selector: impl Into<String>,
        variant: Option<String>,
    ) -> Self {
        Self {
            profile_id: profile_id.into(),
            model_selector: model_selector.into(),
            level: None,
            variant,
        }
    }

    pub fn with_level(mut self, level: impl Into<String>) -> Self {
        self.level = Some(level.into());
        self
    }

    pub fn is_empty(&self) -> bool {
        self.profile_id.trim().is_empty() || self.model_selector.trim().is_empty()
    }
}

/// A credential-free identity for the runtime implementation behind an
/// adapter. It describes the engine/family, never a service endpoint or secret.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeIdentity {
    pub runtime: String,
    pub family: String,
    pub instance: String,
}

impl RuntimeIdentity {
    pub fn new(
        runtime: impl Into<String>,
        family: impl Into<String>,
        instance: impl Into<String>,
    ) -> Self {
        Self {
            runtime: runtime.into(),
            family: family.into(),
            instance: instance.into(),
        }
    }
}

/// Capabilities are facts about an adapter, not policy or placement decisions.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RuntimeCapabilities {
    pub resolve_execution: bool,
    pub create_execution: bool,
    /// Whether the adapter can authoritatively recover an interrupted create
    /// through [`RuntimeAdapter::recover_execution`]. It is separate from
    /// [`RuntimeCapability::RecoverExecution`], which names the operation.
    pub recover_execution: bool,
    pub inspect_execution: bool,
    pub execution_lineage: bool,
    pub select_profile: bool,
    pub observe_context: bool,
    pub stage_continuation: bool,
    pub resume_continuation: bool,
}

impl RuntimeCapabilities {
    pub const NONE: Self = Self {
        resolve_execution: false,
        create_execution: false,
        recover_execution: false,
        inspect_execution: false,
        execution_lineage: false,
        select_profile: false,
        observe_context: false,
        stage_continuation: false,
        resume_continuation: false,
    };

    pub const OPENCODE_V2: Self = Self {
        resolve_execution: true,
        create_execution: true,
        recover_execution: true,
        inspect_execution: true,
        execution_lineage: true,
        select_profile: true,
        observe_context: true,
        stage_continuation: true,
        resume_continuation: true,
    };
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuntimeCapability {
    ResolveExecution,
    CreateExecution,
    RecoverExecution,
    InspectExecution,
    ExecutionLineage,
    SelectProfile,
    ObserveContext,
    StageContinuation,
    ResumeContinuation,
}

impl RuntimeCapability {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ResolveExecution => "resolve_execution",
            Self::CreateExecution => "create_execution",
            Self::RecoverExecution => "recover_execution",
            Self::InspectExecution => "inspect_execution",
            Self::ExecutionLineage => "execution_lineage",
            Self::SelectProfile => "select_profile",
            Self::ObserveContext => "observe_context",
            Self::StageContinuation => "stage_continuation",
            Self::ResumeContinuation => "resume_continuation",
        }
    }
}

/// The small set of runtime failure classifications needed by control-plane
/// callers. Details remain available after secret redaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuntimeErrorKind {
    Unavailable,
    Unsupported,
    ExecutionMissing,
    Authentication,
    Transport,
    InvalidResponse,
    ObservationFailed,
    ProfileSelection,
    ProviderCompletion,
}

impl RuntimeErrorKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unavailable => "unavailable",
            Self::Unsupported => "unsupported",
            Self::ExecutionMissing => "execution_missing",
            Self::Authentication => "authentication",
            Self::Transport => "transport",
            Self::InvalidResponse => "invalid_response",
            Self::ObservationFailed => "observation_failed",
            Self::ProfileSelection => "profile_selection",
            Self::ProviderCompletion => "provider_completion",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeError {
    kind: RuntimeErrorKind,
    detail: String,
}

impl RuntimeError {
    pub fn new(kind: RuntimeErrorKind, detail: impl Into<String>) -> Self {
        Self {
            kind,
            detail: redact(&detail.into()),
        }
    }

    pub fn unsupported(capability: RuntimeCapability) -> Self {
        Self::new(
            RuntimeErrorKind::Unsupported,
            format!("runtime does not support {}", capability.as_str()),
        )
    }

    pub fn kind(&self) -> RuntimeErrorKind {
        self.kind
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "runtime {}: {}", self.kind.as_str(), self.detail)
    }
}

impl std::error::Error for RuntimeError {}

pub type RuntimeResult<T> = std::result::Result<T, RuntimeError>;

/// A verified execution object returned by an adapter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeExecution {
    pub id: RuntimeExecutionId,
    pub profile: Option<RuntimeProfile>,
}

/// Neutral token usage reported by a runtime.
///
/// Cache reads and writes remain separate fields. The active-context
/// projection is one message's input plus cache-read count, never a sum over a
/// transcript.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct RuntimeContextUsage {
    pub input: Option<u64>,
    pub output: Option<u64>,
    pub reasoning: Option<u64>,
    pub cache_read: Option<u64>,
    pub cache_write: Option<u64>,
}

impl RuntimeContextUsage {
    pub fn is_empty(&self) -> bool {
        self.input.is_none()
            && self.output.is_none()
            && self.reasoning.is_none()
            && self.cache_read.is_none()
            && self.cache_write.is_none()
    }

    /// Parse the common runtime token shape without turning missing values into
    /// zero. The concrete adapter remains responsible for choosing this data.
    pub fn from_value(value: Option<&Value>) -> Self {
        let Some(value) = value else {
            return Self::default();
        };
        let cache = value.get("cache");
        Self {
            input: value.get("input").and_then(Value::as_u64),
            output: value.get("output").and_then(Value::as_u64),
            reasoning: value.get("reasoning").and_then(Value::as_u64),
            cache_read: cache
                .and_then(|value| value.get("read"))
                .and_then(Value::as_u64),
            cache_write: cache
                .and_then(|value| value.get("write"))
                .and_then(Value::as_u64),
        }
    }

    pub fn active_context_tokens(&self) -> Option<u64> {
        match (self.input, self.cache_read) {
            (None, None) => None,
            (Some(input), None) => Some(input),
            (None, Some(cache_read)) => Some(cache_read),
            (Some(input), Some(cache_read)) => input.checked_add(cache_read),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeProvenance {
    Exact,
    Estimated,
    #[default]
    Unknown,
}

/// Model limits and identity after an adapter has normalized its transport
/// response. The source is a diagnostic label, never a raw API response.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct RuntimeModelMetadata {
    pub provider_id: Option<String>,
    pub model_id: Option<String>,
    pub context_limit: Option<u64>,
    pub input_limit: Option<u64>,
    pub output_limit: Option<u64>,
    pub effective_limit: Option<u64>,
    pub source: Option<String>,
}

/// A safe boundary event supplied by a plugin/bridge and interpreted by the
/// runtime adapter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeContextEvent {
    pub execution_id: RuntimeExecutionId,
    pub event_id: String,
    pub observed_at: i64,
    pub assistant_message_id: Option<String>,
    pub finish: Option<String>,
    pub safe_boundary: bool,
    pub reported_usage: Option<RuntimeContextUsage>,
    pub reported_profile: Option<RuntimeProfile>,
}

/// Runtime-neutral context observation consumed by the context governor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeContextObservation {
    pub execution_id: RuntimeExecutionId,
    pub event_id: String,
    pub observed_at: i64,
    pub assistant_message_id: Option<String>,
    pub finish: Option<String>,
    pub safe_boundary: bool,
    pub usage: RuntimeContextUsage,
    pub used_tokens: Option<u64>,
    pub limit_tokens: Option<u64>,
    pub model: RuntimeModelMetadata,
    pub message_count: usize,
    pub compaction_count: usize,
    pub usage_provenance: RuntimeProvenance,
    pub context_provenance: RuntimeProvenance,
    pub note: Option<String>,
}

impl RuntimeContextObservation {
    pub fn unknown(event: &RuntimeContextEvent, note: impl Into<String>) -> Self {
        Self {
            execution_id: event.execution_id.clone(),
            event_id: event.event_id.clone(),
            observed_at: event.observed_at,
            assistant_message_id: event.assistant_message_id.clone(),
            finish: event.finish.clone(),
            safe_boundary: false,
            usage: RuntimeContextUsage::default(),
            used_tokens: None,
            limit_tokens: None,
            model: RuntimeModelMetadata::default(),
            message_count: 0,
            compaction_count: 0,
            usage_provenance: RuntimeProvenance::Unknown,
            context_provenance: RuntimeProvenance::Unknown,
            note: Some(redact(&note.into())),
        }
    }
}

/// A continuation is an execution request, not a Mission identity transition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeContinuation {
    pub id: String,
    pub text: String,
    pub description: String,
    pub metadata: Value,
}

impl RuntimeContinuation {
    pub fn new(
        id: impl Into<String>,
        text: impl Into<String>,
        description: impl Into<String>,
        metadata: Value,
    ) -> Self {
        Self {
            id: id.into(),
            text: text.into(),
            description: description.into(),
            metadata,
        }
    }
}

/// The lifecycle operations currently consumed by OCG orchestration.
///
/// Optional operations are checked through [`RuntimeCapabilities`] and return
/// [`RuntimeErrorKind::Unsupported`] explicitly. Implementations must not
/// silently fall back to a different execution semantic.
pub trait RuntimeAdapter {
    fn identity(&self) -> RuntimeIdentity;
    fn capabilities(&self) -> RuntimeCapabilities;

    /// Read one authoritative parent link, including confirmation that the
    /// requested execution exists. `None` means a verified root, not an
    /// unavailable or missing execution.
    fn execution_parent(
        &self,
        _execution_id: &RuntimeExecutionId,
    ) -> RuntimeResult<Option<RuntimeExecutionId>> {
        Err(RuntimeError::unsupported(
            RuntimeCapability::ExecutionLineage,
        ))
    }

    fn resolve_execution(&mut self) -> RuntimeResult<RuntimeExecutionId> {
        Err(RuntimeError::unsupported(
            RuntimeCapability::ResolveExecution,
        ))
    }

    fn create_execution(&mut self) -> RuntimeResult<RuntimeExecutionId> {
        Err(RuntimeError::unsupported(
            RuntimeCapability::CreateExecution,
        ))
    }

    /// Find an execution previously created for `key`, if the adapter can
    /// prove the association. `Ok(None)` means the adapter authoritatively
    /// proved that no such side effect exists; an unsupported or indeterminate
    /// lookup must return an error. The default is intentionally unsupported:
    /// selecting a newest root session would risk promoting an unrelated
    /// execution.
    fn recover_execution(
        &mut self,
        _key: &RuntimeRecoveryKey,
    ) -> RuntimeResult<Option<RuntimeExecution>> {
        Err(RuntimeError::unsupported(
            RuntimeCapability::RecoverExecution,
        ))
    }

    fn inspect_execution(
        &self,
        _execution_id: &RuntimeExecutionId,
    ) -> RuntimeResult<RuntimeExecution> {
        Err(RuntimeError::unsupported(
            RuntimeCapability::InspectExecution,
        ))
    }

    fn prepare_execution(
        &mut self,
        _execution_id: &RuntimeExecutionId,
        _profile: &RuntimeProfile,
    ) -> RuntimeResult<RuntimeExecution> {
        Err(RuntimeError::unsupported(RuntimeCapability::SelectProfile))
    }

    fn observe_context(
        &self,
        _event: &RuntimeContextEvent,
    ) -> RuntimeResult<RuntimeContextObservation> {
        Err(RuntimeError::unsupported(RuntimeCapability::ObserveContext))
    }

    fn stage_runtime_continuation(
        &self,
        _execution_id: &RuntimeExecutionId,
        _continuation: &RuntimeContinuation,
    ) -> RuntimeResult<()> {
        Err(RuntimeError::unsupported(
            RuntimeCapability::StageContinuation,
        ))
    }

    fn resume_runtime_continuation(
        &self,
        _execution_id: &RuntimeExecutionId,
        _continuation: &RuntimeContinuation,
    ) -> RuntimeResult<()> {
        Err(RuntimeError::unsupported(
            RuntimeCapability::ResumeContinuation,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    struct EmptyAdapter;

    struct LineageAdapter(HashMap<String, Option<String>>);

    impl RuntimeAdapter for LineageAdapter {
        fn identity(&self) -> RuntimeIdentity {
            RuntimeIdentity::new("lineage", "test", "unit")
        }

        fn capabilities(&self) -> RuntimeCapabilities {
            RuntimeCapabilities {
                execution_lineage: true,
                ..RuntimeCapabilities::NONE
            }
        }

        fn execution_parent(
            &self,
            id: &RuntimeExecutionId,
        ) -> RuntimeResult<Option<RuntimeExecutionId>> {
            self.0
                .get(id.as_str())
                .map(|parent| parent.as_deref().map(RuntimeExecutionId::new))
                .ok_or_else(|| {
                    RuntimeError::new(
                        RuntimeErrorKind::ExecutionMissing,
                        "missing execution or parent",
                    )
                })
        }
    }

    fn lineage_adapter(edges: &[(&str, Option<&str>)]) -> LineageAdapter {
        LineageAdapter(
            edges
                .iter()
                .map(|(id, parent)| (id.to_string(), parent.map(str::to_string)))
                .collect(),
        )
    }

    #[test]
    fn root_direct_child_and_nested_lineage() {
        let adapter = lineage_adapter(&[
            ("root", None),
            ("child", Some("root")),
            ("nested", Some("child")),
        ]);
        for (id, parent, depth) in [
            ("root", None, 0),
            ("child", Some("root"), 1),
            ("nested", Some("child"), 2),
        ] {
            let lineage = resolve_execution_lineage(&adapter, &id.into()).unwrap();
            assert_eq!(lineage.execution_id.as_str(), id);
            assert_eq!(
                lineage.parent_id.as_ref().map(RuntimeExecutionId::as_str),
                parent
            );
            assert_eq!(lineage.root_id.as_str(), "root");
            assert_eq!(lineage.depth, depth);
        }
    }

    #[test]
    fn lineage_rejects_missing_execution_parent_cycles_and_excess_depth() {
        let adapter = lineage_adapter(&[("child", Some("absent"))]);
        assert_eq!(
            resolve_execution_lineage(&adapter, &"child".into())
                .unwrap_err()
                .kind(),
            RuntimeErrorKind::ExecutionMissing
        );
        assert_eq!(
            resolve_execution_lineage(&adapter, &"absent".into())
                .unwrap_err()
                .kind(),
            RuntimeErrorKind::ExecutionMissing
        );
        let adapter = lineage_adapter(&[("a", Some("b")), ("b", Some("a"))]);
        assert_eq!(
            resolve_execution_lineage(&adapter, &"a".into())
                .unwrap_err()
                .kind(),
            RuntimeErrorKind::InvalidResponse
        );
        let edges: Vec<_> = (0..=MAX_EXECUTION_LINEAGE_DEPTH + 1)
            .map(|index| {
                (
                    format!("node-{index}"),
                    (index <= MAX_EXECUTION_LINEAGE_DEPTH).then(|| format!("node-{}", index + 1)),
                )
            })
            .collect();
        let adapter = LineageAdapter(edges.into_iter().collect());
        assert_eq!(
            resolve_execution_lineage(&adapter, &"node-0".into())
                .unwrap_err()
                .kind(),
            RuntimeErrorKind::InvalidResponse
        );
        let adapter = lineage_adapter(&[("child", Some(""))]);
        assert_eq!(
            resolve_execution_lineage(&adapter, &"child".into())
                .unwrap_err()
                .kind(),
            RuntimeErrorKind::InvalidResponse
        );
    }

    impl RuntimeAdapter for EmptyAdapter {
        fn identity(&self) -> RuntimeIdentity {
            RuntimeIdentity::new("empty", "test", "unit")
        }

        fn capabilities(&self) -> RuntimeCapabilities {
            RuntimeCapabilities::NONE
        }
    }

    #[test]
    fn execution_identity_is_not_mission_identity() {
        let execution = RuntimeExecutionId::new("execution-a");
        assert_ne!(execution.as_str(), "task-mission");
        assert_eq!(execution.to_string(), "execution-a");
    }

    #[test]
    fn context_projection_does_not_sum_transcripts() {
        let usage = RuntimeContextUsage {
            input: Some(40),
            cache_read: Some(2),
            cache_write: Some(100),
            ..RuntimeContextUsage::default()
        };
        assert_eq!(usage.active_context_tokens(), Some(42));
    }

    #[test]
    fn unsupported_capabilities_are_explicit() {
        let error = RuntimeError::unsupported(RuntimeCapability::ResumeContinuation);
        assert_eq!(error.kind(), RuntimeErrorKind::Unsupported);
        assert!(error.to_string().contains("resume_continuation"));
    }

    #[test]
    fn empty_adapter_reports_unsupported_operations() {
        let mut adapter = EmptyAdapter;
        let error = adapter.create_execution().unwrap_err();
        assert_eq!(error.kind(), RuntimeErrorKind::Unsupported);
        assert_eq!(
            resolve_execution_lineage(&adapter, &"root".into())
                .unwrap_err()
                .kind(),
            RuntimeErrorKind::Unsupported
        );
    }

    #[test]
    fn runtime_errors_are_classified_and_redacted() {
        let error = RuntimeError::new(
            RuntimeErrorKind::Authentication,
            "Authorization: Bearer super-secret-token-value",
        );
        assert_eq!(error.kind(), RuntimeErrorKind::Authentication);
        assert!(!error.detail().contains("super-secret-token-value"));
    }
}
