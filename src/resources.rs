//! The durable, descriptive Resource Registry.
//!
//! The registry answers *what execution resources does OCG know about, what
//! facts are known about each, where did those facts come from, how fresh are
//! they, and what capabilities and availability are known*. It deliberately
//! does **not** answer *which resource should a Mission use*: that is future
//! Policy / Placement work. Nothing here selects, ranks, scores, rotates or
//! fails over a resource.
//!
//! Three identities stay mechanically distinct:
//!
//! ```text
//! MissionId          durable work identity        (orchestration::mission)
//! ResourceId         execution-capable resource   (this module)
//! RuntimeExecutionId one concrete execution       (runtime::lifecycle)
//! ```
//!
//! Unknown is a first-class state. When OCG has no authoritative source for a
//! value the fact stays [`ResourceProvenance::Unknown`] with no value; missing
//! evidence is never turned into an optimistic `0`, `unlimited`, `available`
//! or `healthy`. The four states Configured / Resolved / Effective / Observed
//! are kept apart instead of being collapsed because they currently agree.
//!
//! Persistence is a single bounded, atomically written JSON document under
//! `<project>/.ocg/resources/registry.json`. Only dynamic
//! observations are persisted; configured facts are re-derived from the
//! effective configuration on every load, so configuration is never duplicated
//! as stale durable truth. A corrupt record is reported and skipped without
//! poisoning unrelated resources, and never silently becomes "resource absent".

use crate::error::{OcgError, Result};
use crate::model;
use crate::runtime::lifecycle::{
    RuntimeAdapter, RuntimeCapabilities, RuntimeError, RuntimeErrorKind, RuntimeIdentity,
    RuntimeModelMetadata, RuntimeProvenance,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

/// Resource registry schema version.
pub const RESOURCE_SCHEMA_VERSION: u32 = 1;
/// Directory under `.ocg/` holding the registry.
pub const RESOURCE_DIR: &str = "resources";
/// The single registry document.
pub const REGISTRY_FILE: &str = "registry.json";
/// Upper bound on retained resources so the document cannot grow without limit.
pub const MAX_RESOURCES: usize = 64;
/// Upper bound on configured uses retained for one resource.
pub const MAX_CONFIGURED_USES: usize = 16;
/// Default age after which a dynamic observation is reported as stale.
pub const DEFAULT_STALE_AFTER_SECONDS: i64 = 3_600;

/// Where a resource fact came from.
///
/// This is the *source* dimension, distinct from the existing
/// [`RuntimeProvenance`]/`TelemetryProvenance`, which describe how exact a token
/// measurement is. The two are not competing: a telemetry value converts into a
/// source when it enters the registry.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceProvenance {
    /// Declared by OCG configuration (shipped defaults, user or project layer).
    StaticConfig,
    /// Reported by the runtime/provider catalogue as an authoritative fact.
    ProviderReported,
    /// Observed from a live runtime (inspection, activation, telemetry).
    RuntimeObserved,
    /// Declared by a runtime adapter (identity, capabilities).
    RuntimeReported,
    /// A conservative projection of an observed value.
    Estimated,
    /// No source. Never carries a value.
    #[default]
    Unknown,
}

impl ResourceProvenance {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::StaticConfig => "static_config",
            Self::ProviderReported => "provider_reported",
            Self::RuntimeObserved => "runtime_observed",
            Self::RuntimeReported => "runtime_reported",
            Self::Estimated => "estimated",
            Self::Unknown => "unknown",
        }
    }

    /// Relative strength. A stronger known source is never silently replaced by
    /// a weaker one; equal-strength sources defer to recency.
    pub fn rank(self) -> u8 {
        match self {
            Self::StaticConfig => 5,
            Self::ProviderReported | Self::RuntimeObserved => 4,
            Self::RuntimeReported => 3,
            Self::Estimated => 2,
            Self::Unknown => 1,
        }
    }
}

impl From<RuntimeProvenance> for ResourceProvenance {
    fn from(provenance: RuntimeProvenance) -> Self {
        match provenance {
            RuntimeProvenance::Exact => Self::RuntimeObserved,
            RuntimeProvenance::Estimated => Self::Estimated,
            RuntimeProvenance::Unknown => Self::Unknown,
        }
    }
}

/// A value plus the evidence for it. `value: None` is an explicit Unknown.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Fact<T> {
    pub value: Option<T>,
    pub provenance: ResourceProvenance,
    pub observed_at: Option<i64>,
}

impl<T> Default for Fact<T> {
    fn default() -> Self {
        Self {
            value: None,
            provenance: ResourceProvenance::Unknown,
            observed_at: None,
        }
    }
}

impl<T> Fact<T> {
    pub fn unknown() -> Self {
        Self::default()
    }

    pub fn known(value: T, provenance: ResourceProvenance, observed_at: Option<i64>) -> Self {
        Self {
            value: Some(value),
            provenance,
            observed_at,
        }
    }

    pub fn is_known(&self) -> bool {
        self.value.is_some()
    }

    /// Merge an incoming fact without ever erasing a known value with an
    /// unknown one. A stronger source wins; equal sources defer to recency.
    pub fn merge_preferred(&mut self, incoming: Fact<T>) {
        if incoming.value.is_none() {
            return;
        }
        let take = if self.value.is_none() {
            true
        } else {
            incoming.provenance.rank() > self.provenance.rank()
                || (incoming.provenance.rank() == self.provenance.rank()
                    && incoming.observed_at.unwrap_or(i64::MIN)
                        >= self.observed_at.unwrap_or(i64::MIN))
        };
        if take {
            *self = incoming;
        }
    }
}

/// A deterministic, filesystem-safe resource identifier.
///
/// It is derived from the known identity dimensions, never from a Mission,
/// execution or OpenCode session id, and never from a raw agent label.
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ResourceId(String);

impl ResourceId {
    /// Derive the stable id from the identity dimensions that are known.
    pub fn derive(identity: &ResourceIdentity) -> Self {
        let digest = crate::runtime::hash::sha256_hex(identity.canonical_key().as_bytes());
        Self(format!("res-{}", digest.get(..16).unwrap_or(&digest)))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ResourceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// The dimensions OCG can reason about when naming an execution resource.
///
/// `account_profile` and `protocol` are part of the model so that two accounts
/// or protocols under the same provider/model can never be conflated, but no
/// current OCG source populates them; they stay `None` (Unknown) rather than
/// pretending multiple accounts are known. Raw OpenCode agent names are a
/// descriptive *effective* fact and are never part of identity.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ResourceIdentity {
    /// Runtime engine name (for example `opencode`).
    pub runtime: Option<String>,
    /// Runtime version family (for example `v2`). Never the process instance.
    pub family: Option<String>,
    pub provider: Option<String>,
    pub model: Option<String>,
    /// Non-secret account/profile label, when known. Currently always Unknown.
    pub account_profile: Option<String>,
    /// Protocol kind, when known. Currently always Unknown.
    pub protocol: Option<String>,
}

impl ResourceIdentity {
    /// A resource named only by provider/model.
    pub fn for_model(provider: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            provider: Some(provider.into()),
            model: Some(model.into()),
            ..Self::default()
        }
    }

    /// A resource named only by the runtime engine/family (no model yet).
    pub fn for_runtime(runtime: &RuntimeIdentity) -> Self {
        Self {
            runtime: Some(runtime.runtime.clone()),
            family: Some(runtime.family.clone()),
            ..Self::default()
        }
    }

    pub fn with_runtime(mut self, runtime: &RuntimeIdentity) -> Self {
        self.runtime = Some(runtime.runtime.clone());
        self.family = Some(runtime.family.clone());
        self
    }

    pub fn with_runtime_family(
        mut self,
        runtime: impl Into<String>,
        family: impl Into<String>,
    ) -> Self {
        self.runtime = Some(runtime.into());
        self.family = Some(family.into());
        self
    }

    pub fn with_optional_runtime(self, runtime: Option<&RuntimeIdentity>) -> Self {
        match runtime {
            Some(runtime) => self.with_runtime(runtime),
            None => self,
        }
    }

    /// The canonical, unknown-skipping key the id is derived from.
    pub fn canonical_key(&self) -> String {
        let mut parts = Vec::new();
        for (name, value) in [
            ("runtime", &self.runtime),
            ("family", &self.family),
            ("provider", &self.provider),
            ("model", &self.model),
            ("account", &self.account_profile),
            ("protocol", &self.protocol),
        ] {
            if let Some(value) = value
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                parts.push(format!("{name}={value}"));
            }
        }
        parts.join("|")
    }

    pub fn is_empty(&self) -> bool {
        self.canonical_key().is_empty()
    }

    /// A short non-secret description for reports.
    pub fn describe(&self) -> String {
        let mut parts = Vec::new();
        if let (Some(runtime), Some(family)) = (&self.runtime, &self.family) {
            parts.push(format!("{runtime}/{family}"));
        } else if let Some(runtime) = &self.runtime {
            parts.push(runtime.clone());
        } else if let Some(family) = &self.family {
            parts.push(format!("family {family}"));
        }
        match (&self.provider, &self.model) {
            (Some(provider), Some(model)) => parts.push(format!("{provider}/{model}")),
            (None, Some(model)) => parts.push(model.clone()),
            (Some(provider), None) => parts.push(provider.clone()),
            (None, None) => {}
        }
        parts.push(format!(
            "account {}",
            self.account_profile.as_deref().unwrap_or("unknown")
        ));
        parts.push(format!(
            "protocol {}",
            self.protocol.as_deref().unwrap_or("unknown")
        ));
        parts.join(" ")
    }
}

/// A serializable mirror of [`RuntimeCapabilities`] plus the recover fact.
///
/// Capabilities are facts about the adapter, never a placement decision.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct CapabilityFacts {
    pub resolve_execution: bool,
    pub create_execution: bool,
    pub recover_execution: bool,
    pub inspect_execution: bool,
    pub select_profile: bool,
    pub observe_context: bool,
    pub stage_continuation: bool,
    pub resume_continuation: bool,
}

impl CapabilityFacts {
    pub fn from_runtime(capabilities: RuntimeCapabilities) -> Self {
        Self {
            resolve_execution: capabilities.resolve_execution,
            create_execution: capabilities.create_execution,
            recover_execution: capabilities.recover_execution,
            inspect_execution: capabilities.inspect_execution,
            select_profile: capabilities.select_profile,
            observe_context: capabilities.observe_context,
            stage_continuation: capabilities.stage_continuation,
            resume_continuation: capabilities.resume_continuation,
        }
    }

    /// Every supported capability name, in a stable order.
    pub fn listed(&self) -> Vec<&'static str> {
        let mut names = Vec::new();
        for (supported, name) in [
            (self.resolve_execution, "resolve_execution"),
            (self.create_execution, "create_execution"),
            (self.recover_execution, "recover_execution"),
            (self.inspect_execution, "inspect_execution"),
            (self.select_profile, "select_profile"),
            (self.observe_context, "observe_context"),
            (self.stage_continuation, "stage_continuation"),
            (self.resume_continuation, "resume_continuation"),
        ] {
            if supported {
                names.push(name);
            }
        }
        names
    }
}

/// One configured use of a resource (a selected Lead or a routing role).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ConfiguredResource {
    /// The selected Lead model key or routing role (worker use).
    pub role: Option<String>,
    /// The agent identity the configuration targets (descriptive, not identity).
    pub agent: Option<String>,
    pub variant: Option<String>,
}

/// Whether the resolved runtime catalogue currently exposes the model.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CatalogueEvidence {
    Available,
    MissingProvider,
    MissingModel,
    /// A runtime was probed but no trustworthy catalogue was collected.
    Unverified,
    /// No catalogue probe was attempted.
    #[default]
    Unknown,
}

impl CatalogueEvidence {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Available => "available",
            Self::MissingProvider => "missing_provider",
            Self::MissingModel => "missing_model",
            Self::Unverified => "unverified",
            Self::Unknown => "unknown",
        }
    }
}

/// What a live runtime session reported as effective.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct EffectiveFacts {
    pub agent: Option<String>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub variant: Option<String>,
}

/// Factual availability. Deliberately not a score and not a routing input.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceHealth {
    Available,
    /// A transient or partial failure; the resource is not proven dead.
    Degraded,
    Unavailable,
    #[default]
    Unknown,
}

impl ResourceHealth {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Available => "available",
            Self::Degraded => "degraded",
            Self::Unavailable => "unavailable",
            Self::Unknown => "unknown",
        }
    }

    /// Severity for deterministic aggregation of several observations.
    pub fn severity(self) -> u8 {
        match self {
            Self::Unavailable => 3,
            Self::Degraded => 2,
            Self::Available => 1,
            Self::Unknown => 0,
        }
    }
}

/// Evidence for the current availability of a resource.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct HealthFacts {
    pub state: ResourceHealth,
    /// A bounded, secret-free reason. Never a provider transcript.
    pub reason: Option<String>,
    pub provenance: ResourceProvenance,
    pub observed_at: Option<i64>,
}

impl HealthFacts {
    pub fn available(reason: impl Into<String>, now: i64) -> Self {
        Self {
            state: ResourceHealth::Available,
            reason: Some(reason.into()),
            provenance: ResourceProvenance::RuntimeObserved,
            observed_at: Some(now),
        }
    }

    /// Classify a runtime error without turning a transient failure into
    /// permanent absence and without claiming success from an unknown state.
    pub fn from_error(error: &RuntimeError, now: i64) -> Self {
        let (state, reason) = match error.kind() {
            RuntimeErrorKind::Unavailable => {
                (ResourceHealth::Unavailable, "runtime is unavailable")
            }
            RuntimeErrorKind::Authentication => (
                ResourceHealth::Unavailable,
                "runtime rejected its credentials or configuration",
            ),
            RuntimeErrorKind::Transport => {
                (ResourceHealth::Degraded, "transient transport failure")
            }
            RuntimeErrorKind::ProviderCompletion => {
                (ResourceHealth::Degraded, "provider completion failed")
            }
            RuntimeErrorKind::InvalidResponse => (
                ResourceHealth::Degraded,
                "runtime returned an untrustworthy response",
            ),
            RuntimeErrorKind::ObservationFailed => {
                (ResourceHealth::Degraded, "runtime observation failed")
            }
            RuntimeErrorKind::ProfileSelection => {
                (ResourceHealth::Degraded, "profile selection failed")
            }
            RuntimeErrorKind::Unsupported => (
                ResourceHealth::Degraded,
                "capability unsupported by the runtime",
            ),
            RuntimeErrorKind::ExecutionMissing => (
                ResourceHealth::Available,
                "runtime is reachable but the execution is missing",
            ),
        };
        Self {
            state,
            reason: Some(reason.to_string()),
            provenance: ResourceProvenance::RuntimeObserved,
            observed_at: Some(now),
        }
    }
}

/// A bounded quota window, when an authoritative source exists.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct QuotaValue {
    pub window: Option<String>,
    pub remaining: Option<u64>,
    pub limit: Option<u64>,
    /// When the window is expected to reset, if the source reports it. Additive
    /// to the schema; old records deserialize as `None`. It is retained because
    /// an exhausted quota must be able to report its reset time honestly.
    pub reset_at: Option<i64>,
}

/// Pricing metadata, when an authoritative source exists. Never invented.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct CostValue {
    pub currency: Option<String>,
    pub input_per_million: Option<f64>,
    pub output_per_million: Option<f64>,
}

/// The durable, dynamic observation overlay for one resource.
///
/// This is the only part of a resource that is persisted. Configured facts and
/// capabilities are re-derived; identity is carried here so a record can be
/// reconstructed after a restart.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ResourceObservation {
    pub resource_id: ResourceId,
    pub identity: ResourceIdentity,
    /// Runtime engine/family, from the adapter (not part of identity metadata).
    pub runtime: Fact<RuntimeIdentity>,
    pub capabilities: Fact<CapabilityFacts>,
    /// Catalogue evidence that the runtime currently exposes the model.
    pub resolved: Fact<CatalogueEvidence>,
    /// What a live session reported (Effective).
    pub effective: Fact<EffectiveFacts>,
    pub health: HealthFacts,
    /// A trustworthy context limit, when the runtime reported a real one.
    pub context_limit: Fact<u64>,
    /// Execution slots. Always Unknown today; no fabricated capacity.
    pub capacity: Fact<u64>,
    /// Quota window/remaining/limit. Always Unknown today.
    pub quota: Fact<QuotaValue>,
    /// Pricing metadata. Always Unknown today.
    pub cost: Fact<CostValue>,
    pub first_seen_at: i64,
    pub updated_at: i64,
}

impl ResourceObservation {
    pub fn for_identity(identity: &ResourceIdentity, now: i64) -> Self {
        Self {
            resource_id: ResourceId::derive(identity),
            identity: identity.clone(),
            first_seen_at: now,
            updated_at: now,
            ..Self::default()
        }
    }

    /// Merge a newer observation without erasing stronger facts.
    pub fn merge(&mut self, incoming: ResourceObservation, now: i64) {
        if self.resource_id.as_str().is_empty() {
            self.resource_id = incoming.resource_id;
        }
        if self.identity.is_empty() {
            self.identity = incoming.identity;
        }
        self.runtime.merge_preferred(incoming.runtime);
        self.capabilities.merge_preferred(incoming.capabilities);
        self.resolved.merge_preferred(incoming.resolved);
        self.effective.merge_preferred(incoming.effective);
        self.context_limit.merge_preferred(incoming.context_limit);
        self.capacity.merge_preferred(incoming.capacity);
        self.quota.merge_preferred(incoming.quota);
        self.cost.merge_preferred(incoming.cost);
        self.health = merge_health(&self.health, &incoming.health);
        if self.first_seen_at == 0
            || (incoming.first_seen_at != 0 && incoming.first_seen_at < self.first_seen_at)
        {
            self.first_seen_at = incoming.first_seen_at;
        }
        self.updated_at = self.updated_at.max(incoming.updated_at).max(now);
    }
}

/// Recency-based health merge. An unknown observation never erases a known
/// state; otherwise the newest observation wins.
fn merge_health(current: &HealthFacts, incoming: &HealthFacts) -> HealthFacts {
    if incoming.state == ResourceHealth::Unknown && current.state != ResourceHealth::Unknown {
        return current.clone();
    }
    match (current.observed_at, incoming.observed_at) {
        (Some(current_at), Some(incoming_at)) if incoming_at < current_at => current.clone(),
        _ => incoming.clone(),
    }
}

/// The merged, queryable view of one resource.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ResourceRecord {
    pub resource_id: ResourceId,
    pub identity: ResourceIdentity,
    /// Every configured use that resolves to this resource (Configured).
    pub configured: Vec<ConfiguredResource>,
    /// Runtime engine/family (RuntimeReported).
    pub runtime: Fact<RuntimeIdentity>,
    /// Lifecycle capabilities (RuntimeReported).
    pub capabilities: Fact<CapabilityFacts>,
    /// Catalogue evidence (Resolved).
    pub resolved: Fact<CatalogueEvidence>,
    /// Live session report (Effective).
    pub effective: Fact<EffectiveFacts>,
    pub health: HealthFacts,
    pub context_limit: Fact<u64>,
    pub capacity: Fact<u64>,
    pub quota: Fact<QuotaValue>,
    pub cost: Fact<CostValue>,
    pub first_seen_at: i64,
    pub updated_at: i64,
}

impl ResourceRecord {
    /// The most recent dynamic observation time, if any.
    pub fn observed_at(&self) -> Option<i64> {
        [
            self.runtime.observed_at,
            self.capabilities.observed_at,
            self.resolved.observed_at,
            self.effective.observed_at,
            self.health.observed_at,
            self.context_limit.observed_at,
            self.capacity.observed_at,
            self.quota.observed_at,
            self.cost.observed_at,
        ]
        .into_iter()
        .flatten()
        .max()
    }

    /// Whether the newest observation is older than `stale_after`. A resource
    /// with no observation time is not called stale; it is simply unobserved.
    pub fn is_stale(&self, now: i64, stale_after: i64) -> bool {
        match self.observed_at() {
            Some(at) => now.saturating_sub(at) > stale_after,
            None => false,
        }
    }
}

/// One configured use of a resource, derived from the effective configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfiguredEntry {
    pub identity: ResourceIdentity,
    pub configured: ConfiguredResource,
}

/// Derive the configured resources from the effective configuration.
///
/// The selected Lead and every explicit routing role are derived. `runtime` should be
/// the resolved runtime identity when known so configured and observed facts
/// attach to the same resource; it is never invented.
pub fn configured_entries(
    data: &Value,
    runtime: Option<&RuntimeIdentity>,
) -> Result<Vec<ConfiguredEntry>> {
    let mut entries = Vec::new();
    let profile = crate::profile::Profile::from_ocg_config(data)?;
    if let Some(selected) = profile.default_model.as_deref() {
        if profile.select(Some(selected)).is_ok() {
            let contract = model::lead_contract(data, selected)?;
            let identity = ResourceIdentity::for_model(&contract.provider_id, &contract.model_id)
                .with_optional_runtime(runtime);
            entries.push(ConfiguredEntry {
                identity,
                configured: ConfiguredResource {
                    role: Some("lead".to_string()),
                    agent: Some(contract.agent),
                    variant: contract.variant,
                },
            });
        }
    }
    if let Some(roles) = model::role_specs(data) {
        for (role, spec) in roles {
            let Some(key) = spec.get("model").and_then(Value::as_str) else {
                continue;
            };
            let entry = model::model_entry(data, key)?;
            let (Some(provider), Some(model_id)) = (
                entry.get("provider").and_then(Value::as_str),
                entry.get("id").and_then(Value::as_str),
            ) else {
                continue;
            };
            let identity =
                ResourceIdentity::for_model(provider, model_id).with_optional_runtime(runtime);
            entries.push(ConfiguredEntry {
                identity,
                configured: ConfiguredResource {
                    role: Some(role.clone()),
                    agent: Some(model::worker_agent_id(role)),
                    variant: spec
                        .get("variant")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                },
            });
        }
    }
    Ok(entries)
}

/// The in-memory registry: dynamic observations plus derived configured facts.
#[derive(Clone, Debug, PartialEq)]
pub struct ResourceRegistry {
    schema_version: u32,
    updated_at: i64,
    observations: BTreeMap<String, ResourceObservation>,
    /// Configured uses, keyed by resource id. Derived, never persisted.
    configured: BTreeMap<String, Vec<ConfiguredResource>>,
}

impl Default for ResourceRegistry {
    fn default() -> Self {
        Self::new(0)
    }
}

impl ResourceRegistry {
    pub fn new(now: i64) -> Self {
        Self {
            schema_version: RESOURCE_SCHEMA_VERSION,
            updated_at: now,
            observations: BTreeMap::new(),
            configured: BTreeMap::new(),
        }
    }

    pub fn schema_version(&self) -> u32 {
        self.schema_version
    }

    pub fn updated_at(&self) -> i64 {
        self.updated_at
    }

    pub fn is_empty(&self) -> bool {
        self.observations.is_empty()
    }

    pub fn len(&self) -> usize {
        self.observations.len()
    }

    /// Register a configured use. Configured facts are re-derived and never
    /// persisted, so they cannot become stale durable truth.
    pub fn register_configured(
        &mut self,
        identity: &ResourceIdentity,
        configured: ConfiguredResource,
        now: i64,
    ) -> ResourceId {
        let id = ResourceId::derive(identity);
        let uses = self.configured.entry(id.as_str().to_string()).or_default();
        if !uses.contains(&configured) {
            uses.push(configured);
            if uses.len() > MAX_CONFIGURED_USES {
                let excess = uses.len() - MAX_CONFIGURED_USES;
                uses.drain(0..excess);
            }
        }
        self.merge_observation(ResourceObservation::for_identity(identity, now), now);
        id
    }

    /// Ingest an adapter's identity and lifecycle capabilities.
    ///
    /// This is the RuntimeLifecycleAdapter normalization seam: it consumes only
    /// neutral lifecycle types, never an OpenCode transport or response object.
    pub fn observe_adapter(&mut self, adapter: &dyn RuntimeAdapter, now: i64) -> ResourceId {
        self.observe_runtime_capabilities(adapter.identity(), adapter.capabilities(), now)
    }

    /// Ingest a runtime identity and its lifecycle capabilities directly.
    ///
    /// Used where a trait object is not available but the runtime family and
    /// its `lifecycle_capabilities()` are known.
    pub fn observe_runtime_capabilities(
        &mut self,
        runtime: RuntimeIdentity,
        capabilities: RuntimeCapabilities,
        now: i64,
    ) -> ResourceId {
        let identity = ResourceIdentity::for_runtime(&runtime);
        let mut observation = ResourceObservation::for_identity(&identity, now);
        observation.runtime = Fact::known(runtime, ResourceProvenance::RuntimeReported, Some(now));
        observation.capabilities = Fact::known(
            CapabilityFacts::from_runtime(capabilities),
            ResourceProvenance::RuntimeReported,
            Some(now),
        );
        let id = observation.resource_id.clone();
        self.merge_observation(observation, now);
        id
    }

    /// Record a factual health observation.
    ///
    /// The reason is redacted on ingestion so no credential-shaped text can
    /// reach the on-disk registry.
    pub fn observe_health(
        &mut self,
        identity: &ResourceIdentity,
        mut health: HealthFacts,
        now: i64,
    ) -> ResourceId {
        health.reason = health
            .reason
            .map(|reason| crate::telemetry::task::redact(&reason));
        let mut observation = ResourceObservation::for_identity(identity, now);
        observation.health = health;
        let id = observation.resource_id.clone();
        self.merge_observation(observation, now);
        id
    }

    /// Record a runtime error's availability classification.
    pub fn observe_error(
        &mut self,
        identity: &ResourceIdentity,
        error: &RuntimeError,
        now: i64,
    ) -> ResourceId {
        self.observe_health(identity, HealthFacts::from_error(error, now), now)
    }

    /// Record a successful observation.
    pub fn observe_available(
        &mut self,
        identity: &ResourceIdentity,
        reason: impl Into<String>,
        now: i64,
    ) -> ResourceId {
        self.observe_health(identity, HealthFacts::available(reason, now), now)
    }

    /// Record what a live session reported as effective.
    pub fn observe_effective(
        &mut self,
        identity: &ResourceIdentity,
        effective: EffectiveFacts,
        now: i64,
    ) -> ResourceId {
        let mut observation = ResourceObservation::for_identity(identity, now);
        observation.effective =
            Fact::known(effective, ResourceProvenance::RuntimeObserved, Some(now));
        let id = observation.resource_id.clone();
        self.merge_observation(observation, now);
        id
    }

    /// Record resolved catalogue evidence.
    pub fn observe_resolved(
        &mut self,
        identity: &ResourceIdentity,
        evidence: CatalogueEvidence,
        provenance: ResourceProvenance,
        now: i64,
    ) -> ResourceId {
        let mut observation = ResourceObservation::for_identity(identity, now);
        observation.resolved = Fact::known(evidence, provenance, Some(now));
        let id = observation.resource_id.clone();
        self.merge_observation(observation, now);
        id
    }

    /// Record a trustworthy context limit and effective model identity from
    /// runtime model metadata. Missing limits stay Unknown.
    pub fn observe_model_metadata(
        &mut self,
        identity: &ResourceIdentity,
        metadata: &RuntimeModelMetadata,
        now: i64,
    ) -> ResourceId {
        let mut observation = ResourceObservation::for_identity(identity, now);
        if let Some(limit) = metadata
            .effective_limit
            .or(metadata.context_limit)
            .filter(|limit| *limit > 0)
        {
            observation.context_limit =
                Fact::known(limit, ResourceProvenance::RuntimeObserved, Some(now));
        }
        if metadata.provider_id.is_some() || metadata.model_id.is_some() {
            observation.effective = Fact::known(
                EffectiveFacts {
                    agent: None,
                    provider: metadata.provider_id.clone(),
                    model: metadata.model_id.clone(),
                    variant: None,
                },
                ResourceProvenance::RuntimeObserved,
                Some(now),
            );
        }
        let id = observation.resource_id.clone();
        self.merge_observation(observation, now);
        id
    }

    fn merge_observation(&mut self, incoming: ResourceObservation, now: i64) {
        let key = incoming.resource_id.as_str().to_string();
        if key.is_empty() {
            return;
        }
        match self.observations.get_mut(&key) {
            Some(existing) => existing.merge(incoming, now),
            None => {
                let mut fresh = incoming;
                if fresh.first_seen_at == 0 {
                    fresh.first_seen_at = now;
                }
                fresh.updated_at = fresh.updated_at.max(now);
                self.observations.insert(key, fresh);
            }
        }
        self.updated_at = self.updated_at.max(now);
        self.enforce_bound();
    }

    fn enforce_bound(&mut self) {
        if self.observations.len() <= MAX_RESOURCES {
            return;
        }
        let mut entries: Vec<(String, i64)> = self
            .observations
            .iter()
            .map(|(key, observation)| (key.clone(), observation.updated_at))
            .collect();
        entries.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| b.0.cmp(&a.0)));
        for (key, _) in entries.into_iter().skip(MAX_RESOURCES) {
            self.observations.remove(&key);
            self.configured.remove(&key);
        }
    }

    /// The raw persisted observation for one id, if present.
    pub fn observation(&self, id: &ResourceId) -> Option<&ResourceObservation> {
        self.observations.get(id.as_str())
    }

    /// The complete durable observation set, keyed by resource id.
    ///
    /// This is the exact normalized domain that the replay authority owns; the
    /// configured uses are derived and never durable.
    pub(crate) fn observations(&self) -> &BTreeMap<String, ResourceObservation> {
        &self.observations
    }

    /// Rebuild the durable registry from an authoritative observation set.
    /// Configured uses are derived elsewhere and are never supplied here.
    pub(crate) fn from_observations(
        updated_at: i64,
        observations: BTreeMap<String, ResourceObservation>,
    ) -> Self {
        let mut registry = Self {
            schema_version: RESOURCE_SCHEMA_VERSION,
            updated_at,
            observations,
            configured: BTreeMap::new(),
        };
        registry.enforce_bound();
        registry
    }

    /// Build the merged view for one id.
    pub fn resource(&self, id: &ResourceId) -> Option<ResourceRecord> {
        let observation = self.observations.get(id.as_str())?;
        Some(self.record(id.as_str(), observation))
    }

    /// Every known resource, ordered by id.
    pub fn list(&self) -> Vec<ResourceRecord> {
        self.observations
            .iter()
            .map(|(key, observation)| self.record(key, observation))
            .collect()
    }

    fn record(&self, key: &str, observation: &ResourceObservation) -> ResourceRecord {
        ResourceRecord {
            resource_id: observation.resource_id.clone(),
            identity: observation.identity.clone(),
            configured: self.configured.get(key).cloned().unwrap_or_default(),
            runtime: observation.runtime.clone(),
            capabilities: observation.capabilities.clone(),
            resolved: observation.resolved.clone(),
            effective: observation.effective.clone(),
            health: observation.health.clone(),
            context_limit: observation.context_limit.clone(),
            capacity: observation.capacity.clone(),
            quota: observation.quota.clone(),
            cost: observation.cost.clone(),
            first_seen_at: observation.first_seen_at,
            updated_at: observation.updated_at,
        }
    }
}

/// The atomic on-disk document. Only dynamic observations are stored.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
struct RegistryDocument {
    schema_version: u32,
    updated_at: i64,
    resources: BTreeMap<String, ResourceObservation>,
}

impl Default for RegistryDocument {
    fn default() -> Self {
        Self {
            schema_version: RESOURCE_SCHEMA_VERSION,
            updated_at: 0,
            resources: BTreeMap::new(),
        }
    }
}

/// One corrupt or rejected record discovered while loading.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResourceIssue {
    pub resource: String,
    pub detail: String,
}

/// A load result that keeps corruption explicit.
#[derive(Clone, Debug)]
pub struct LoadedRegistry {
    pub registry: ResourceRegistry,
    /// The whole document was unreadable or had an unsupported schema.
    pub corrupt: bool,
    pub exists: bool,
    /// Records that were individually rejected. They are surfaced, never
    /// silently treated as absent, and never poison the other resources.
    pub issues: Vec<ResourceIssue>,
}

/// The registry directory.
pub fn registry_dir(root: &Path) -> PathBuf {
    root.join(crate::context::repomap::OCG_DIR)
        .join(RESOURCE_DIR)
}

/// The registry document path.
pub fn registry_path(root: &Path) -> PathBuf {
    registry_dir(root).join(REGISTRY_FILE)
}

fn is_safe_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 96
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
}

/// A strict raw read of the legacy projection, without consulting the replay
/// authority. Used to bootstrap the authority and to read projections before
/// the first initialization.
///
/// - A missing registry is `Ok(empty)` with `exists == false`.
/// - An unreadable document (I/O) is `Err`.
/// - A malformed or unsupported document is `Ok(corrupt == true)` so the
///   caller can fail closed with the registry's own representation.
pub(crate) fn load_raw(root: &Path) -> Result<LoadedRegistry> {
    let path = registry_path(root);
    let text = match fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(LoadedRegistry {
                registry: ResourceRegistry::new(0),
                corrupt: false,
                exists: false,
                issues: Vec::new(),
            })
        }
        Err(error) => return Err(OcgError::read(&path, error)),
    };
    let value: Value = match serde_json::from_str(&text) {
        Ok(value) => value,
        Err(error) => {
            return Ok(corrupt(file_issue(
                &path,
                &format!("registry document is not valid JSON: {error}"),
            )))
        }
    };
    let raw_schema_version = value
        .get("schema_version")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let schema_version = match u32::try_from(raw_schema_version) {
        Ok(version) => version,
        Err(_) => {
            return Ok(corrupt(file_issue(
                &path,
                &format!("registry has an out-of-range schema_version {raw_schema_version}"),
            )))
        }
    };
    if schema_version != RESOURCE_SCHEMA_VERSION {
        return Ok(corrupt(file_issue(
            &path,
            &format!("registry has unsupported schema_version {schema_version}"),
        )));
    }
    let Some(updated_at) = value.get("updated_at").and_then(Value::as_i64) else {
        return Ok(corrupt(file_issue(
            &path,
            "registry is missing a valid updated_at",
        )));
    };
    if value.get("resources").and_then(Value::as_object).is_none() {
        return Ok(corrupt(file_issue(
            &path,
            "registry is missing a valid resources object",
        )));
    }
    let mut registry = ResourceRegistry::new(updated_at);
    let mut issues = Vec::new();
    if let Some(resources) = value.get("resources").and_then(Value::as_object) {
        for (key, record) in resources {
            match serde_json::from_value::<ResourceObservation>(record.clone()) {
                Ok(observation) => match validate_observation(key, &observation) {
                    Ok(()) => {
                        registry.observations.insert(key.clone(), observation);
                    }
                    Err(detail) => issues.push(ResourceIssue {
                        resource: key.clone(),
                        detail,
                    }),
                },
                Err(error) => issues.push(ResourceIssue {
                    resource: key.clone(),
                    detail: format!("corrupt resource record: {error}"),
                }),
            }
        }
    }
    registry.enforce_bound();
    Ok(LoadedRegistry {
        registry,
        corrupt: false,
        exists: true,
        issues,
    })
}

/// Load the authoritative durable registry.
///
/// Once the replay authority is initialized it is the read source; only before
/// first initialization is the legacy projection consulted for migration
/// bootstrap. An initialized-but-unreadable authority fails closed with the
/// registry's existing `corrupt` representation.
pub fn load(root: &Path) -> LoadedRegistry {
    match crate::orchestration::replay::read_authoritative_snapshot(root) {
        Ok(Some(snapshot)) => {
            let updated_at = snapshot
                .resources
                .values()
                .map(|observation| observation.updated_at)
                .max()
                .unwrap_or(0);
            LoadedRegistry {
                registry: ResourceRegistry::from_observations(updated_at, snapshot.resources),
                corrupt: false,
                exists: true,
                issues: Vec::new(),
            }
        }
        Ok(None) => match load_raw(root) {
            Ok(loaded) => loaded,
            Err(error) => corrupt(file_issue(
                &registry_path(root),
                &format!("registry projection could not be read: {error}"),
            )),
        },
        Err(error) => corrupt(file_issue(
            &crate::orchestration::replay::state_path(root),
            &format!("replay authority is unreadable: {error}"),
        )),
    }
}

fn corrupt(issue: ResourceIssue) -> LoadedRegistry {
    LoadedRegistry {
        registry: ResourceRegistry::new(0),
        corrupt: true,
        exists: true,
        issues: vec![issue],
    }
}

fn file_issue(path: &Path, detail: &str) -> ResourceIssue {
    ResourceIssue {
        resource: path.display().to_string(),
        detail: detail.to_string(),
    }
}

pub(crate) fn validate_observation(
    key: &str,
    observation: &ResourceObservation,
) -> std::result::Result<(), String> {
    if !is_safe_id(key) {
        return Err("resource id is not a safe identifier".to_string());
    }
    if observation.resource_id.as_str() != key {
        return Err(format!(
            "resource record identity '{}' does not match its key",
            observation.resource_id
        ));
    }
    if !observation.identity.is_empty() && ResourceId::derive(&observation.identity).as_str() != key
    {
        return Err("resource identity does not derive to its key".to_string());
    }
    Ok(())
}

/// Persist the registry atomically. Only dynamic observations are written.
pub fn save(root: &Path, registry: &ResourceRegistry) -> Result<PathBuf> {
    crate::runtime::install::ensure_gitignore(root)?;
    // The replay authority is committed before the compatibility projection.
    crate::orchestration::replay::record_resource_updates(root, registry)?;
    let path = registry_path(root);
    let document = RegistryDocument {
        schema_version: RESOURCE_SCHEMA_VERSION,
        updated_at: registry.updated_at,
        resources: registry.observations.clone(),
    };
    let value = serde_json::to_value(&document).map_err(|error| {
        OcgError::config(format!("cannot serialize resource registry: {error}"))
    })?;
    crate::runtime::install::write_json_atomic(&path, &value)?;
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FakeAdapter;

    impl RuntimeAdapter for FakeAdapter {
        fn identity(&self) -> RuntimeIdentity {
            RuntimeIdentity::new("opencode", "v2", "invocation")
        }

        fn capabilities(&self) -> RuntimeCapabilities {
            RuntimeCapabilities::OPENCODE_V2
        }
    }

    #[test]
    fn identity_is_not_model_only_and_not_an_execution_id() {
        let plain = ResourceIdentity::for_model("openai", "gpt");
        let with_runtime = plain.clone().with_runtime_family("opencode", "v2");
        assert_ne!(
            ResourceId::derive(&plain),
            ResourceId::derive(&with_runtime)
        );

        let account = ResourceIdentity {
            account_profile: Some("team-a".to_string()),
            ..plain.clone()
        };
        assert_ne!(ResourceId::derive(&plain), ResourceId::derive(&account));
        assert_ne!(
            ResourceId::derive(&plain).as_str(),
            crate::runtime::lifecycle::RuntimeExecutionId::new("res-x").as_str()
        );
    }

    #[test]
    fn fact_merge_never_erases_a_known_value_with_an_unknown() {
        let mut fact = Fact::known(10u64, ResourceProvenance::RuntimeObserved, Some(5));
        fact.merge_preferred(Fact::unknown());
        assert_eq!(fact.value, Some(10));
        fact.merge_preferred(Fact::known(20, ResourceProvenance::Estimated, Some(9)));
        assert_eq!(fact.value, Some(10), "a weaker estimate must not win");
        fact.merge_preferred(Fact::known(
            30,
            ResourceProvenance::RuntimeObserved,
            Some(9),
        ));
        assert_eq!(fact.value, Some(30), "an equal, newer observation wins");
    }

    #[test]
    fn adapter_normalization_records_runtime_and_capabilities() {
        let mut registry = ResourceRegistry::new(1);
        let id = registry.observe_adapter(&FakeAdapter, 7);
        let record = registry.resource(&id).unwrap();
        assert_eq!(
            record.runtime.value.as_ref().unwrap().family,
            "v2".to_string()
        );
        assert!(record.capabilities.value.unwrap().recover_execution);
        assert_eq!(record.updated_at, 7);
    }
}
