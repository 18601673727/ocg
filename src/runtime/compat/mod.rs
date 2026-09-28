//! OpenCode runtime compatibility boundary.
//!
//! OCG supports two structurally different OpenCode major families:
//!
//! ```text
//! runtime/common contract
//! ├── v1 adapter   (1.18.x: `plugin`, `task`, request-scoped Lead)
//! └── v2 adapter   (2.0.x: `plugin`, `subagent`, session-scoped Lead)
//! ```
//!
//! Every version-specific decision lives here. No unrelated module may branch
//! on the OpenCode major version: callers ask for the detected [`Major`], get an
//! adapter from [`adapter_for`], and use the adapter's contract. The two
//! adapters are deliberately thin data+behaviour; all policy (which model, which
//! variant, who may run what) stays in Rust.
//!
//! Detection is explicit. [`detect`] parses an `opencode --version` string and
//! fails clearly for a structurally incompatible major (for example `3.x` or a
//! `1.x` below the [`v1_floor`]). A runtime whose version genuinely cannot be
//! determined is *unclassified* and callers fall back to the v1 contract to
//! preserve the historical 1.18.x launch path.

pub mod v1;
pub mod v2;
pub mod v2_client;
#[cfg(unix)]
pub mod v2_rendezvous;
pub mod v2_server;

use crate::error::{OcgError, Result};
use crate::model::LeadContract;
use crate::process::ProcessHost;
use crate::runtime::lifecycle;
use semver::Version;
use std::path::Path;

/// The oldest OpenCode release OCG supports. `OPENCODE_CONFIG_CONTENT` and the
/// orchestration adapter require at least this version.
pub fn v1_floor() -> Version {
    Version::new(1, 18, 0)
}

/// The oldest OpenCode 2 release OCG supports.
pub fn v2_floor() -> Version {
    Version::new(2, 0, 0)
}

/// The OpenCode 2 release OCG is currently verified against.
pub fn v2_verified_baseline() -> Version {
    Version::new(2, 0, 11)
}

/// A supported OpenCode major family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Major {
    V1,
    V2,
}

impl Major {
    pub fn as_str(self) -> &'static str {
        match self {
            Major::V1 => "v1",
            Major::V2 => "v2",
        }
    }
}

/// A detected OpenCode release, already classified into a supported family.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeVersion {
    version: Version,
    major: Major,
}

impl RuntimeVersion {
    pub fn version(&self) -> &Version {
        &self.version
    }

    pub fn major(&self) -> Major {
        self.major
    }
}

/// Parse a raw `opencode --version` string and classify it.
///
/// The output is tolerated to be either a bare semver (`1.18.31`) or a
/// decorated one (`opencode 1.18.31`). Every whitespace token is considered and
/// the first parseable semver wins. A parseable but unsupported major, or a
/// `1.x` below [`v1_floor`], is a clear error; an unparseable string is also an
/// error so callers can decide whether to fall back.
pub fn detect(raw: &str) -> Result<RuntimeVersion> {
    let version = parse_version_token(raw).ok_or_else(|| {
        OcgError::config(format!(
            "cannot determine the OpenCode version from `{raw}`; OCG requires OpenCode {} or a 2.x release",
            v1_floor()
        ))
    })?;
    classify(version)
}

/// Classify an already parsed semver into a supported family, failing clearly
/// for an unsupported major or a release below the family floor.
pub fn classify(version: Version) -> Result<RuntimeVersion> {
    let major = match version.major {
        1 => {
            if version < v1_floor() {
                return Err(OcgError::config(format!(
                    "OpenCode {version} is below the required {} floor",
                    v1_floor()
                )));
            }
            Major::V1
        }
        2 => {
            if version < v2_floor() {
                return Err(OcgError::config(format!(
                    "OpenCode {version} is below the required {} floor",
                    v2_floor()
                )));
            }
            Major::V2
        }
        other => {
            return Err(OcgError::config(format!(
                "OpenCode {version} belongs to unsupported major version {other}; OCG supports the 1.18.x (v1) and 2.x (v2) families"
            )))
        }
    };

    Ok(RuntimeVersion { version, major })
}

/// Probe a runtime explicitly and classify it. This is the only place the
/// runtime major is established for a launch.
pub fn detect_from_host(host: &dyn ProcessHost, program: &Path) -> Result<RuntimeVersion> {
    let raw = host.version(program)?;
    detect(&raw)
}

/// The adapter for a detected family.
pub fn adapter_for(version: &RuntimeVersion) -> &'static dyn RuntimeAdapter {
    match version.major() {
        Major::V1 => v1_adapter(),
        Major::V2 => v2_adapter(),
    }
}

/// The v1 (OpenCode 1.18.x) contract.
pub fn v1_adapter() -> &'static dyn RuntimeAdapter {
    &v1::V1Adapter
}

/// The v2 (OpenCode 2.x) contract.
pub fn v2_adapter() -> &'static dyn RuntimeAdapter {
    &v2::V2Adapter
}

pub(crate) fn parse_version_token(raw: &str) -> Option<Version> {
    raw.split_whitespace().find_map(|token| {
        let token =
            token.trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '.' && c != '-');
        let token = token.strip_prefix('v').unwrap_or(token);
        Version::parse(token).ok()
    })
}

/// How a runtime applies the Rust-resolved Lead contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeadSelectionMode {
    /// The adapter rewrites the mutable request message before OpenCode saves
    /// it (`chat.message`); the contract wins over sticky session/UI state.
    RequestMessage,
    /// The Lead is selected on the session itself: resolve a session, switch
    /// its agent/model/variant, then read the effective Lead back.
    Session,
}

/// How OCG reaches the running OpenCode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LaunchMode {
    /// Replace the process with `opencode` (interactive CLI).
    Exec,
    /// OpenCode 2 runs a daemon; the client talks HTTP/SSE to it.
    Daemon,
}

/// The narrow, version-specific contract every other module depends on.
pub trait RuntimeAdapter: Send + Sync {
    /// The family this adapter implements.
    fn major(&self) -> Major;

    /// The generated-config array key for package plugins.
    fn plugin_key(&self) -> &'static str;

    /// The OpenCode permission/tool key for worker delegation.
    fn task_key(&self) -> &'static str;

    /// The generated plugin source for this runtime.
    fn plugin_source(&self) -> &'static str;

    /// The `file://` URL for a local adapter when this runtime supports local
    /// adapters in its config array. Runtimes which discover local plugins via
    /// a config directory return `Ok(None)`.
    fn local_plugin_uri(&self, path: &Path) -> Result<Option<String>>;

    /// How the Lead contract is applied.
    fn lead_selection(&self) -> LeadSelectionMode;

    /// How a launch reaches the runtime.
    fn launch_mode(&self) -> LaunchMode;

    /// Lifecycle capabilities exposed by the concrete OpenCode family.
    /// V1 intentionally returns the empty set: its request-scoped Lead path
    /// does not provide the V2 session lifecycle contract.
    fn lifecycle_capabilities(&self) -> crate::runtime::lifecycle::RuntimeCapabilities {
        crate::runtime::lifecycle::RuntimeCapabilities::NONE
    }

    /// Whether a tool event names this runtime's delegation tool.
    fn is_task_tool(&self, tool: &str) -> bool;
}

/// The exact Lead selection OCG resolved for one selected Profile model, in the
/// shape
/// a runtime adapter needs.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LeadSelection {
    pub level: String,
    pub agent: String,
    pub provider_id: String,
    pub model_id: String,
    /// Present only when the Lead contract declares one. Never fabricated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variant: Option<String>,
}

impl LeadSelection {
    pub fn from_contract(contract: &LeadContract) -> Self {
        Self {
            level: contract.level.clone(),
            agent: contract.agent.clone(),
            provider_id: contract.provider_id.clone(),
            model_id: contract.model_id.clone(),
            variant: contract.variant.clone(),
        }
    }

    pub fn full_model_id(&self) -> String {
        format!("{}/{}", self.provider_id, self.model_id)
    }

    /// Convert the policy-resolved OpenCode contract into the runtime-neutral
    /// lifecycle profile consumed by orchestration. OpenCode field names stay
    /// on this compatibility side of the boundary.
    pub fn runtime_profile(&self) -> crate::runtime::lifecycle::RuntimeProfile {
        crate::runtime::lifecycle::RuntimeProfile::new(
            self.agent.clone(),
            self.full_model_id(),
            self.variant.clone(),
        )
        .with_level(self.level.clone())
    }
}

/// The Lead a session actually reports after selection. Fields are optional so
/// an unavailable observation is distinct from a contradictory one.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct EffectiveLead {
    pub agent: Option<String>,
    pub provider_id: Option<String>,
    pub model_id: Option<String>,
    pub variant: Option<String>,
}

/// Legacy OpenCode session lifecycle seam used by the compatibility layer.
///
/// New orchestration code uses [`crate::runtime::lifecycle::RuntimeAdapter`].
/// This trait remains for the existing V2 client contract and older callers;
/// its JSON-shaped methods are not part of the Mission-facing boundary.
/// Implementations are invocation-scoped; no operation here restarts a
/// runtime or reads provider credentials.
pub trait SessionLifecycleClient {
    /// Create a new root session in the already configured project directory.
    fn create_fresh_session(&self) -> Result<String>;
    /// Read the active context messages exposed by the runtime.
    fn context_messages(&self, session: &str) -> Result<Vec<serde_json::Value>>;
    /// Read the complete session record, including the runtime model/token
    /// aggregate when that version exposes it.
    fn session_info(&self, session: &str) -> Result<serde_json::Value>;
    /// Read model limits from the runtime catalogue. Missing model data is
    /// represented by a metadata value with no denominator, not by an invented
    /// limit.
    fn model_metadata(
        &self,
        provider_id: Option<&str>,
        model_id: Option<&str>,
    ) -> Result<crate::runtime::lifecycle::RuntimeModelMetadata>;
    /// Add a durable synthetic continuation message. Synthetic input is used
    /// instead of a normal prompt so it cannot be mistaken for a new user task
    /// and cannot reset Mission identity.
    fn inject_continuation(
        &self,
        session: &str,
        message_id: &str,
        text: &str,
        description: &str,
        metadata: &serde_json::Value,
    ) -> Result<()>;

    /// Stage a synthetic continuation without scheduling model execution.
    /// Implementations predating the explicit staging contract may use the
    /// ordinary injection method as a compatibility fallback.
    fn stage_continuation(
        &self,
        session: &str,
        message_id: &str,
        text: &str,
        description: &str,
        metadata: &serde_json::Value,
    ) -> Result<()> {
        self.inject_continuation(session, message_id, text, description, metadata)
    }

    /// Resume a previously staged synthetic continuation. A client that has no
    /// separate staging operation may make this a no-op after its compatibility
    /// injection has already scheduled the message.
    fn resume_continuation(
        &self,
        _session: &str,
        _message_id: &str,
        _text: &str,
        _description: &str,
        _metadata: &serde_json::Value,
    ) -> Result<()> {
        Ok(())
    }
}

pub trait SessionClient {
    /// Create a fresh session or resolve the project's active one.
    fn resolve_session(&mut self) -> Result<String>;
    /// Select or switch the session's Lead agent.
    fn select_agent(&mut self, session: &str, agent: &str) -> Result<()>;
    /// Select or switch the session's model; a `variant` is only ever supplied
    /// when the Rust-resolved contract has one.
    fn select_model(
        &mut self,
        session: &str,
        provider_id: &str,
        model_id: &str,
        variant: Option<&str>,
    ) -> Result<()>;
    /// Read the effective Lead back. Unavailable is an error: OCG refuses to
    /// rely on a Lead it cannot verify.
    fn effective_lead(&self, session: &str) -> Result<EffectiveLead>;
}

/// Legacy composition retained for compatibility callers. Mission-facing
/// orchestration now depends on [`crate::runtime::lifecycle::RuntimeAdapter`].
pub trait RolloverRuntime: SessionClient + SessionLifecycleClient {}

impl<T> RolloverRuntime for T where T: SessionClient + SessionLifecycleClient {}

/// The invocation-scoped client surface required by the orchestration bridge.
///
/// A concrete V2 client implements all three interfaces, but Rust does not
/// allow a `dyn A + B + C` object. Keeping this named composition trait gives
/// the bridge one shared client without cloning or moving the transport into
/// separate trait objects.
pub trait BridgeRuntimeClient:
    lifecycle::RuntimeAdapter + SessionClient + SessionLifecycleClient
{
    fn as_lifecycle(&mut self) -> &mut dyn lifecycle::RuntimeAdapter;
    fn as_session(&mut self) -> &mut dyn SessionClient;
}

/// Apply and verify a Lead contract to an already-created session.
///
/// Rollover must never call `resolve_session` after creating a target: that
/// operation is allowed to return an older/newest unrelated session. Keeping
/// this operation explicit makes the target identity part of the rollover
/// contract.
pub fn select_existing_session_lead(
    client: &mut dyn SessionClient,
    session_id: &str,
    lead: &LeadSelection,
) -> Result<SessionLeadSelection> {
    if session_id.is_empty() {
        return Err(OcgError::config(
            "cannot select a Lead for an empty session id",
        ));
    }
    client.select_agent(session_id, &lead.agent)?;
    client.select_model(
        session_id,
        &lead.provider_id,
        &lead.model_id,
        lead.variant.as_deref(),
    )?;
    let effective = client.effective_lead(session_id)?;
    verify_effective_lead(lead, &effective)?;
    Ok(SessionLeadSelection {
        session_id: session_id.to_string(),
        lead: lead.clone(),
        steps: vec![
            format!("select_agent={}", lead.agent),
            format!("select_model={}", lead.full_model_id()),
            format!(
                "variant={}",
                lead.variant.as_deref().unwrap_or("provider-default")
            ),
            "verify_effective_lead".to_string(),
        ],
    })
}

/// Idempotent Lead enforcement for prompt authority gate.
///
/// Reads the effective Lead first. If it already matches the resolved contract,
/// no mutation calls are made. If it differs, applies select_agent + select_model
/// and re-verifies. This is the single canonical path for "desired == effective"
/// used by both launch and prompt admission.
pub fn ensure_existing_session_lead(
    client: &mut dyn SessionClient,
    session_id: &str,
    lead: &LeadSelection,
) -> Result<SessionLeadSelection> {
    if session_id.is_empty() {
        return Err(OcgError::config(
            "cannot ensure Lead for an empty session id",
        ));
    }

    // 1. Read effective Lead first
    let effective = client.effective_lead(session_id)?;

    // 2. Canonical verification - if already matches, no-op
    if verify_effective_lead(lead, &effective).is_ok() {
        return Ok(SessionLeadSelection {
            session_id: session_id.to_string(),
            lead: lead.clone(),
            steps: vec!["verify_effective_lead: already matches".to_string()],
        });
    }

    // 3. Mismatch: apply contract, then re-verify
    client.select_agent(session_id, &lead.agent)?;
    client.select_model(
        session_id,
        &lead.provider_id,
        &lead.model_id,
        lead.variant.as_deref(),
    )?;
    let effective = client.effective_lead(session_id)?;
    verify_effective_lead(lead, &effective)?;
    Ok(SessionLeadSelection {
        session_id: session_id.to_string(),
        lead: lead.clone(),
        steps: vec![
            format!("select_agent={}", lead.agent),
            format!("select_model={}", lead.full_model_id()),
            format!(
                "variant={}",
                lead.variant.as_deref().unwrap_or("provider-default")
            ),
            "verify_effective_lead".to_string(),
        ],
    })
}

/// A deterministic, offline [`SessionClient`] used by tests and by callers that
/// need to rehearse a session selection without a live daemon.
///
/// Also implements [`RuntimeAdapter`] and [`SessionLifecycleClient`] so that
/// tests can wrap it in [`Rc<RefCell<MemorySessionClient>>`](std::rc::Rc) and
/// use it as the invocation-scoped runtime client for [`BridgeContext`].
#[derive(Debug, Clone, Default)]
pub struct MemorySessionClient {
    session_id: String,
    parent_session_id: Option<String>,
    selected_agent: Option<String>,
    selected_model: Option<(String, String, Option<String>)>,
    forced_effective: Option<EffectiveLead>,
    resolve_error: Option<String>,
    effective_error: Option<String>,
    calls: Vec<String>,
}

impl MemorySessionClient {
    pub fn new() -> Self {
        Self {
            session_id: "ocg-session".to_string(),
            ..Self::default()
        }
    }

    pub fn with_session_id(mut self, session_id: impl Into<String>) -> Self {
        self.session_id = session_id.into();
        self
    }

    /// Model a runtime worker execution whose durable parent is the root
    /// execution. Used by prompt-authority regression tests.
    pub fn with_parent_session(mut self, parent: impl Into<String>) -> Self {
        self.parent_session_id = Some(parent.into());
        self
    }

    /// Force a specific effective Lead, simulating a session that refused or
    /// silently overrode OCG's selection.
    pub fn with_effective(mut self, effective: EffectiveLead) -> Self {
        self.forced_effective = Some(effective);
        self
    }

    pub fn failing_resolve(mut self, message: impl Into<String>) -> Self {
        self.resolve_error = Some(message.into());
        self
    }

    pub fn failing_effective(mut self, message: impl Into<String>) -> Self {
        self.effective_error = Some(message.into());
        self
    }

    /// The ordered transcript of session operations, for determinism tests.
    pub fn calls(&self) -> &[String] {
        &self.calls
    }
}

impl RuntimeAdapter for MemorySessionClient {
    fn major(&self) -> Major {
        Major::V2
    }

    fn plugin_key(&self) -> &'static str {
        "plugin"
    }

    fn task_key(&self) -> &'static str {
        "subagent"
    }

    fn plugin_source(&self) -> &'static str {
        ""
    }

    fn local_plugin_uri(&self, _path: &Path) -> Result<Option<String>> {
        Ok(None)
    }

    fn lead_selection(&self) -> LeadSelectionMode {
        LeadSelectionMode::Session
    }

    fn launch_mode(&self) -> LaunchMode {
        LaunchMode::Daemon
    }

    fn lifecycle_capabilities(&self) -> crate::runtime::lifecycle::RuntimeCapabilities {
        crate::runtime::lifecycle::RuntimeCapabilities::OPENCODE_V2
    }

    fn is_task_tool(&self, tool: &str) -> bool {
        tool == "subagent"
    }
}

impl lifecycle::RuntimeAdapter for MemorySessionClient {
    fn identity(&self) -> lifecycle::RuntimeIdentity {
        lifecycle::RuntimeIdentity::new("memory", "v2", "test")
    }

    fn capabilities(&self) -> lifecycle::RuntimeCapabilities {
        lifecycle::RuntimeCapabilities::OPENCODE_V2
    }

    fn execution_parent(
        &self,
        execution_id: &lifecycle::RuntimeExecutionId,
    ) -> lifecycle::RuntimeResult<Option<lifecycle::RuntimeExecutionId>> {
        if execution_id.as_str().is_empty() {
            Err(lifecycle::RuntimeError::new(
                lifecycle::RuntimeErrorKind::ExecutionMissing,
                "memory execution is unknown",
            ))
        } else {
            Ok(self
                .parent_session_id
                .as_deref()
                .map(lifecycle::RuntimeExecutionId::new))
        }
    }

    fn inspect_execution(
        &self,
        execution_id: &lifecycle::RuntimeExecutionId,
    ) -> lifecycle::RuntimeResult<lifecycle::RuntimeExecution> {
        if execution_id.as_str().is_empty() {
            return Err(lifecycle::RuntimeError::new(
                lifecycle::RuntimeErrorKind::ExecutionMissing,
                "memory execution is unknown",
            ));
        }
        Ok(lifecycle::RuntimeExecution {
            id: execution_id.clone(),
            profile: None,
        })
    }
}

impl BridgeRuntimeClient for MemorySessionClient {
    fn as_lifecycle(&mut self) -> &mut dyn lifecycle::RuntimeAdapter {
        self
    }

    fn as_session(&mut self) -> &mut dyn SessionClient {
        self
    }
}

impl SessionLifecycleClient for MemorySessionClient {
    fn create_fresh_session(&self) -> Result<String> {
        Ok(self.session_id.clone())
    }

    fn context_messages(&self, _session: &str) -> Result<Vec<serde_json::Value>> {
        Ok(vec![])
    }

    fn session_info(&self, session: &str) -> Result<serde_json::Value> {
        let agent = self
            .selected_agent
            .clone()
            .unwrap_or_else(|| "unknown".to_string());
        Ok(serde_json::json!({
            "id": session,
            "agent": agent,
        }))
    }

    fn model_metadata(
        &self,
        _provider_id: Option<&str>,
        _model_id: Option<&str>,
    ) -> Result<crate::runtime::lifecycle::RuntimeModelMetadata> {
        Ok(crate::runtime::lifecycle::RuntimeModelMetadata::default())
    }

    fn inject_continuation(
        &self,
        _session: &str,
        _message_id: &str,
        _text: &str,
        _description: &str,
        _metadata: &serde_json::Value,
    ) -> Result<()> {
        Ok(())
    }
}

impl SessionClient for MemorySessionClient {
    fn resolve_session(&mut self) -> Result<String> {
        self.calls.push("resolve_session".to_string());
        if let Some(message) = &self.resolve_error {
            return Err(OcgError::config(message.clone()));
        }
        let session = if self.session_id.is_empty() {
            "ocg-session".to_string()
        } else {
            self.session_id.clone()
        };
        Ok(session)
    }

    fn select_agent(&mut self, session: &str, agent: &str) -> Result<()> {
        self.calls.push(format!("select_agent({session},{agent})"));
        self.selected_agent = Some(agent.to_string());
        Ok(())
    }

    fn select_model(
        &mut self,
        session: &str,
        provider_id: &str,
        model_id: &str,
        variant: Option<&str>,
    ) -> Result<()> {
        self.calls.push(format!(
            "select_model({session},{provider_id},{model_id},{})",
            variant.unwrap_or("")
        ));
        self.selected_model = Some((
            provider_id.to_string(),
            model_id.to_string(),
            variant.map(str::to_string),
        ));
        Ok(())
    }

    fn effective_lead(&self, _session: &str) -> Result<EffectiveLead> {
        if let Some(message) = &self.effective_error {
            return Err(OcgError::config(message.clone()));
        }
        if let Some(forced) = &self.forced_effective {
            return Ok(forced.clone());
        }
        let (provider_id, model_id, variant) = self.selected_model.clone().unwrap_or_default();
        Ok(EffectiveLead {
            agent: self.selected_agent.clone(),
            provider_id: (!provider_id.is_empty()).then_some(provider_id),
            model_id: (!model_id.is_empty()).then_some(model_id),
            variant,
        })
    }
}

/// The verified result of session-level Lead selection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionLeadSelection {
    pub session_id: String,
    pub lead: LeadSelection,
    /// Deterministic transcript of the operations performed.
    pub steps: Vec<String>,
}

/// Perform v2 session-level Lead selection:
///
/// 1. create/resolve the session,
/// 2. select/switch the Lead agent,
/// 3. select/switch the model, applying the reasoning variant only when the
///    Rust-resolved contract has one,
/// 4. read the effective Lead back and verify it before relying on it.
///
/// A selection that cannot be read back, or that differs from the resolved
/// contract, is a hard error: OCG never proceeds on a guessed Lead.
pub fn select_session_lead(
    client: &mut dyn SessionClient,
    lead: &LeadSelection,
) -> Result<SessionLeadSelection> {
    let session_id = client.resolve_session()?;
    client.select_agent(&session_id, &lead.agent)?;
    client.select_model(
        &session_id,
        &lead.provider_id,
        &lead.model_id,
        lead.variant.as_deref(),
    )?;
    let effective = client.effective_lead(&session_id)?;
    verify_effective_lead(lead, &effective)?;
    Ok(SessionLeadSelection {
        session_id,
        lead: lead.clone(),
        steps: vec![
            "resolve_session".to_string(),
            format!("select_agent={}", lead.agent),
            format!("select_model={}", lead.full_model_id()),
            format!(
                "variant={}",
                lead.variant.as_deref().unwrap_or("provider-default")
            ),
            "verify_effective_lead".to_string(),
        ],
    })
}

/// Verify that the effective session Lead matches the Rust-resolved contract.
///
/// The reasoning variant is only checked when the contract declares one; a
/// provider-default contract is satisfied by any provider variant, and OCG
/// never invents one.
pub fn verify_effective_lead(expected: &LeadSelection, effective: &EffectiveLead) -> Result<()> {
    let agent = effective
        .agent
        .as_deref()
        .ok_or_else(|| unavailable(expected))?;
    let provider = effective
        .provider_id
        .as_deref()
        .ok_or_else(|| unavailable(expected))?;
    let model = effective
        .model_id
        .as_deref()
        .ok_or_else(|| unavailable(expected))?;

    if agent != expected.agent || provider != expected.provider_id || model != expected.model_id {
        return Err(OcgError::config(format!(
            "contradictory active OpenCode Lead: resolved {} on {}/{}, but the session reports {} on {}/{}",
            expected.agent,
            expected.provider_id,
            expected.model_id,
            agent,
            provider,
            model
        )));
    }

    if let Some(variant) = &expected.variant {
        match effective.variant.as_deref() {
            Some(actual) if actual == variant => {}
            _ => {
                return Err(OcgError::config(format!(
                    "contradictory active OpenCode Lead: resolved variant '{variant}', but the session reports '{}'",
                    effective.variant.as_deref().unwrap_or("none")
                )))
            }
        }
    }
    Ok(())
}

fn unavailable(expected: &LeadSelection) -> OcgError {
    OcgError::config(format!(
        "active OpenCode Lead is unavailable: the session did not report an effective agent/model for {} on {}",
        expected.agent,
        expected.full_model_id()
    ))
}

/// A non-authoritative observation. Optional catalogue/debug probes may be
/// unavailable; OCG warns and continues rather than failing a launch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Observation<T> {
    Observed(T),
    Unavailable(String),
}

impl<T> Observation<T> {
    pub fn observed(self) -> Option<T> {
        match self {
            Observation::Observed(value) => Some(value),
            Observation::Unavailable(_) => None,
        }
    }

    pub fn unavailable_reason(&self) -> Option<&str> {
        match self {
            Observation::Observed(_) => None,
            Observation::Unavailable(reason) => Some(reason),
        }
    }
}

/// Fold an optional probe result into a fail-soft observation, pushing a
/// warning on failure. This is the shared shape for catalogue/debug probes and
/// for a v2 session transport that is not configured.
pub fn observe_optional<T>(
    result: Result<T>,
    label: &str,
    warnings: &mut Vec<String>,
) -> Observation<T> {
    match result {
        Ok(value) => Observation::Observed(value),
        Err(error) => {
            warnings.push(format!("optional {label} probe unavailable: {error}"));
            Observation::Unavailable(error.to_string())
        }
    }
}
