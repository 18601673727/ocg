//! Policy / Admission Engine.
//!
//! The Policy layer answers exactly one question:
//!
//! ```text
//! given this Mission,
//!       this proposed control-plane action,
//!       these current durable/runtime/resource facts,
//! is the action allowed to proceed?
//! ```
//!
//! It does **not** answer *which resource should be used*. That is future
//! Placement / Resource Broker work and deliberately not implemented here.
//! Policy evaluates the action's admissibility for the **currently associated**
//! resource; it never enumerates, ranks, scores, rotates or fails over.
//!
//! The decision vocabulary is small and typed:
//!
//! ```text
//! Allow            proceed with the planned action
//! Defer            not currently safe; keep durable state, retry later
//! RequireApproval  needs an explicit, generation-bound approval
//! Deny             never proceed for this action/context
//! ```
//!
//! Rules are evaluated in a deterministic order and aggregated by an explicit
//! precedence (`Deny > RequireApproval > Defer > Allow`). The winning rule is
//! recorded, so a caller can always answer *why was this allowed/blocked?*.
//!
//! Policy is transport-neutral: it consumes only durable Mission metadata,
//! runtime-neutral capability facts, registry facts and a pre-resolved approval
//! view. No OpenCode HTTP/event type enters this module.
//!
//! Unknown is respected. A rule declares which facts it actually requires, and
//! an unknown required fact is never interpreted optimistically:
//! `unknown quota != unlimited`, `unknown capacity != available`,
//! `unknown cost != free`, `unknown health != healthy`, and a stale fact is not
//! treated as a current fact. A rule that does not require a fact is never
//! blocked by it being unknown.

use crate::error::{OcgError, Result};
use crate::orchestration::mission::{MissionReconcileStatus, MissionStatus};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservationStatus {
    Exists,
    Missing,
    Unbound,
    ObservationFailed,
    RuntimeUnavailable,
    AuthenticationFailure,
    TransientTransportFailure,
    Unsupported,
}

impl ObservationStatus {
    pub fn is_failure(self) -> bool {
        !matches!(self, Self::Exists | Self::Missing | Self::Unbound)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Exists => "exists",
            Self::Missing => "missing",
            Self::Unbound => "unbound",
            Self::ObservationFailed => "observation_failed",
            Self::RuntimeUnavailable => "runtime_unavailable",
            Self::AuthenticationFailure => "authentication_failure",
            Self::TransientTransportFailure => "transient_transport_failure",
            Self::Unsupported => "unsupported",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReconcileAction {
    Noop,
    Wait,
    RecoverRollover,
    RecoverExecution,
    EnsureExecution,
    ContinueExecution,
    Escalate,
    Blocked,
}

impl ReconcileAction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Noop => "noop",
            Self::Wait => "wait",
            Self::RecoverRollover => "recover_rollover",
            Self::RecoverExecution => "recover_execution",
            Self::EnsureExecution => "ensure_execution",
            Self::ContinueExecution => "continue_execution",
            Self::Escalate => "escalate",
            Self::Blocked => "blocked",
        }
    }
}
use crate::resources::{
    CostValue, Fact, QuotaValue, ResourceHealth, ResourceId, ResourceIdentity, ResourceProvenance,
    ResourceRecord,
};
use crate::runtime::lifecycle::{RuntimeCapabilities, RuntimeExecutionId, RuntimeProfile};
use crate::telemetry::task::redact;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

/// Schema version for a durable approval record.
pub const APPROVAL_SCHEMA_VERSION: u32 = 1;
/// Directory (under the orchestration state dir) holding approval records.
pub const APPROVALS_DIR: &str = "approvals";
/// Upper bound on retained approval records.
pub const MAX_APPROVALS: usize = 64;
/// Upper bound on a bounded human-readable policy reason.
pub const MAX_REASON_BYTES: usize = 240;
/// Age after which a resource health fact is treated as stale by Policy. It
/// mirrors the registry's own default so the two do not drift silently.
pub const RESOURCE_FACT_MAX_AGE_SECONDS: i64 = crate::resources::DEFAULT_STALE_AFTER_SECONDS;

/// A proposed consequential action that may require admission.
///
/// This is the consequential subset of [`ReconcileAction`]. Pure observation,
/// `noop`, `wait`, `escalate` and `blocked` actions never pass through Policy:
/// they have no costly or destructive side effect to admit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyAction {
    /// Recover an incomplete durable rollover.
    RecoverRollover,
    /// Create/recover the exact durable execution the Mission owns.
    RecoverExecution,
    /// Create and bind the first execution for an unbound Mission.
    EnsureExecution,
    /// Continue an already-claimed same-generation recovery.
    ContinueExecution,
}

impl PolicyAction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RecoverRollover => "recover_rollover",
            Self::RecoverExecution => "recover_execution",
            Self::EnsureExecution => "ensure_execution",
            Self::ContinueExecution => "continue_execution",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value.trim() {
            "recover_rollover" => Some(Self::RecoverRollover),
            "recover_execution" => Some(Self::RecoverExecution),
            "ensure_execution" => Some(Self::EnsureExecution),
            "continue_execution" => Some(Self::ContinueExecution),
            _ => None,
        }
    }

    /// The equivalent action for a planned reconcile decision, if it is
    /// consequential. Non-consequential actions return `None`.
    pub fn from_reconcile(action: ReconcileAction) -> Option<Self> {
        match action {
            ReconcileAction::RecoverRollover => Some(Self::RecoverRollover),
            ReconcileAction::RecoverExecution => Some(Self::RecoverExecution),
            ReconcileAction::EnsureExecution => Some(Self::EnsureExecution),
            ReconcileAction::ContinueExecution => Some(Self::ContinueExecution),
            ReconcileAction::Noop
            | ReconcileAction::Wait
            | ReconcileAction::Escalate
            | ReconcileAction::Blocked => None,
        }
    }

    /// Whether this action creates or replaces the current execution. Such an
    /// action is only admissible from an authoritative absence observation.
    pub fn is_replacement(self) -> bool {
        matches!(self, Self::EnsureExecution | Self::RecoverExecution)
    }

    /// Every action name, in a stable order.
    pub fn all() -> [Self; 4] {
        [
            Self::RecoverRollover,
            Self::RecoverExecution,
            Self::EnsureExecution,
            Self::ContinueExecution,
        ]
    }
}

/// The typed policy decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyDecision {
    /// The action may proceed.
    Allow,
    /// The action is not currently safe; retain durable state and retry.
    Defer,
    /// The action needs an explicit approval bound to this exact generation
    /// and action.
    RequireApproval,
    /// The action must never proceed for this action/context.
    Deny,
}

impl PolicyDecision {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::Defer => "defer",
            Self::RequireApproval => "require_approval",
            Self::Deny => "deny",
        }
    }

    /// The aggregation precedence. A strictly higher value wins regardless of
    /// rule order, so multiple rules cannot produce order-dependent behavior.
    pub fn precedence(self) -> u8 {
        match self {
            Self::Allow => 0,
            Self::Defer => 1,
            Self::RequireApproval => 2,
            Self::Deny => 3,
        }
    }
}

/// How well a fact a rule requires is actually known.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FactStatus {
    /// A known, fresh, authoritative value.
    Known,
    /// No value, or only a non-authoritative value. Never treated as a value.
    Unknown,
    /// A value exists but is older than the accepted freshness window. It is
    /// not used as a current fact.
    Stale,
}

impl FactStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Known => "known",
            Self::Unknown => "unknown",
            Self::Stale => "stale",
        }
    }

    /// The conservative interpretation of this status when a rule maps it to a
    /// decision. A known fact requires no gate; unknown and stale facts are
    /// explicitly handled by the requiring rule.
    pub fn is_authoritative(self) -> bool {
        self == Self::Known
    }
}

/// One fact a rule actually used, with its status. Presented for inspection
/// and persisted (bounded) in a receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactProbe {
    pub kind: String,
    pub status: FactStatus,
    pub detail: String,
}

impl FactProbe {
    pub fn new(kind: impl Into<String>, status: FactStatus, detail: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            status,
            detail: bounded(&detail.into()),
        }
    }

    /// A bounded `kind=status` summary used in receipts.
    pub fn summary(&self) -> String {
        format!("{}={}", self.kind, self.status.as_str())
    }
}

/// A compact, factual projection of the currently associated resource.
///
/// This is deliberately not a `ResourceRecord` clone: Policy reads the facts
/// it can reason about and keeps unknown values unknown.
#[derive(Debug, Clone, PartialEq)]
pub struct ResourceFacts {
    pub identity: ResourceIdentity,
    /// Whether a registry record was found for the current identity.
    pub found: bool,
    pub health: ResourceHealth,
    pub health_observed_at: Option<i64>,
    pub health_provenance: ResourceProvenance,
    /// Execution slots. Always Unknown today; never fabricated capacity.
    pub capacity: Fact<u64>,
    /// Quota window. Always Unknown today.
    pub quota: Fact<QuotaValue>,
    /// Pricing metadata. Always Unknown today.
    pub cost: Fact<CostValue>,
}

impl ResourceFacts {
    /// Facts for a resource with no registry record. Every fact is Unknown;
    /// nothing is optimistically defaulted.
    pub fn unknown(identity: ResourceIdentity) -> Self {
        Self {
            identity,
            found: false,
            health: ResourceHealth::Unknown,
            health_observed_at: None,
            health_provenance: ResourceProvenance::Unknown,
            capacity: Fact::unknown(),
            quota: Fact::unknown(),
            cost: Fact::unknown(),
        }
    }

    pub fn from_record(record: &ResourceRecord) -> Self {
        Self {
            identity: record.identity.clone(),
            found: true,
            health: record.health.state,
            health_observed_at: record.health.observed_at,
            health_provenance: record.health.provenance,
            capacity: record.capacity.clone(),
            quota: record.quota.clone(),
            cost: record.cost.clone(),
        }
    }

    /// Freshness of the health fact. `Unavailable` recorded an hour ago is not
    /// a current fact.
    pub fn health_status(&self, now: i64, max_age_seconds: i64) -> FactStatus {
        if !self.found || self.health == ResourceHealth::Unknown {
            return FactStatus::Unknown;
        }
        if self.health_provenance.rank() < ResourceProvenance::RuntimeReported.rank() {
            return FactStatus::Unknown;
        }
        match self.health_observed_at {
            None => FactStatus::Unknown,
            Some(at) if now.saturating_sub(at) > max_age_seconds => FactStatus::Stale,
            Some(_) => FactStatus::Known,
        }
    }

    /// Whether a numeric/serializable fact carries an authoritative value.
    /// Never treats a missing value as zero.
    fn fact_status<T>(fact: &Fact<T>) -> FactStatus {
        if fact.value.is_some() && fact.provenance != ResourceProvenance::Unknown {
            FactStatus::Known
        } else {
            FactStatus::Unknown
        }
    }

    pub fn capacity_status(&self) -> FactStatus {
        Self::fact_status(&self.capacity)
    }

    pub fn quota_status(&self) -> FactStatus {
        Self::fact_status(&self.quota)
    }

    pub fn cost_status(&self) -> FactStatus {
        Self::fact_status(&self.cost)
    }
}

/// Durable approval lifecycle. A stale approval cannot authorize a later
/// unrelated operation because every approval is bound to a Mission generation
/// and an exact proposed action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalStatus {
    Pending,
    Approved,
    Rejected,
}

impl ApprovalStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Approved => "approved",
            Self::Rejected => "rejected",
        }
    }

    pub fn is_pending(self) -> bool {
        self == Self::Pending
    }
}

/// The pre-resolved approval state for the evaluated action. Resolving it is
/// the caller's job, so Policy performs no I/O.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ApprovalView {
    /// Whether the active configuration requires an explicit approval.
    pub required: bool,
    /// The durable status, when a record exists.
    pub status: Option<ApprovalStatus>,
    /// A durable approval record exists but is unreadable or unsupported.
    pub corrupt: bool,
}

impl ApprovalView {
    pub fn not_required() -> Self {
        Self::default()
    }
}

/// The exact approval request Policy would like a human to resolve. The id is
/// deterministic over `(mission_id, generation, action, current_execution_id)`,
/// so re-evaluating an unchanged action never creates a new approval.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalRequest {
    pub approval_id: String,
    pub mission_id: String,
    pub generation: u32,
    pub action: PolicyAction,
    pub current_execution_id: Option<String>,
    pub requested_at: i64,
}

/// A durable, generation-bound approval record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ApprovalRecord {
    pub schema_version: u32,
    pub approval_id: String,
    pub mission_id: String,
    pub generation: u32,
    pub action: String,
    pub current_execution_id: Option<String>,
    pub status: ApprovalStatus,
    pub requested_at: i64,
    pub resolved_at: Option<i64>,
    pub note: Option<String>,
}

impl Default for ApprovalRecord {
    fn default() -> Self {
        Self {
            schema_version: APPROVAL_SCHEMA_VERSION,
            approval_id: String::new(),
            mission_id: String::new(),
            generation: 0,
            action: String::new(),
            current_execution_id: None,
            status: ApprovalStatus::Pending,
            requested_at: 0,
            resolved_at: None,
            note: None,
        }
    }
}

impl ApprovalRecord {
    pub fn request(request: &ApprovalRequest) -> Self {
        Self {
            schema_version: APPROVAL_SCHEMA_VERSION,
            approval_id: request.approval_id.clone(),
            mission_id: request.mission_id.clone(),
            generation: request.generation,
            action: request.action.as_str().to_string(),
            current_execution_id: request.current_execution_id.clone(),
            status: ApprovalStatus::Pending,
            requested_at: request.requested_at,
            resolved_at: None,
            note: None,
        }
    }

    /// Whether this record authorizes exactly this generation and action.
    pub fn authorizes(&self, mission_id: &str, generation: u32, action: PolicyAction) -> bool {
        self.status == ApprovalStatus::Approved
            && self.mission_id == mission_id
            && self.generation == generation
            && self.action == action.as_str()
    }
}

/// A read-only issue discovered while enumerating approval records.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalIssue {
    pub file: String,
    pub detail: String,
}

/// The result of one bounded approval scan.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LoadedApprovals {
    pub approvals: Vec<ApprovalRecord>,
    pub issues: Vec<ApprovalIssue>,
}

/// The compact context Policy evaluates. It is derived from authoritative
/// Mission/runtime/registry facts by the caller; Policy itself performs no I/O.
#[derive(Debug, Clone)]
pub struct PolicyContext {
    pub mission_id: String,
    pub generation: u32,
    pub mission_status: MissionStatus,
    pub action: PolicyAction,
    pub current_execution_id: Option<RuntimeExecutionId>,
    pub observation: ObservationStatus,
    pub reconcile_status: MissionReconcileStatus,
    pub capabilities: RuntimeCapabilities,
    pub resource: ResourceFacts,
    pub approval: ApprovalView,
    pub evaluated_at: i64,
}

impl PolicyContext {
    /// The capability a proposed action requires, as a `(supported, name)`
    /// pair. Unsupported never triggers a fake fallback: the admission is
    /// deferred.
    pub fn required_capability(&self) -> (bool, &'static str) {
        match self.action {
            PolicyAction::RecoverRollover => (self.capabilities.observe_context, "observe_context"),
            PolicyAction::RecoverExecution | PolicyAction::EnsureExecution => {
                (self.capabilities.create_execution, "create_execution")
            }
            PolicyAction::ContinueExecution => match self.reconcile_status {
                MissionReconcileStatus::Created
                | MissionReconcileStatus::Bound
                | MissionReconcileStatus::Preparing => {
                    (self.capabilities.select_profile, "select_profile")
                }
                MissionReconcileStatus::Prepared | MissionReconcileStatus::Staging => {
                    (self.capabilities.stage_continuation, "stage_continuation")
                }
                _ => (self.capabilities.resume_continuation, "resume_continuation"),
            },
        }
    }
}

/// The outcome of one rule. `Allow` from a rule means "this rule does not
/// restrict the action", not necessarily "the effective decision is Allow".
#[derive(Debug, Clone)]
struct RuleOutcome {
    rule: &'static str,
    decision: PolicyDecision,
    reason_code: &'static str,
    reason: String,
    facts: Vec<FactProbe>,
    approval: Option<ApprovalRequest>,
}

impl RuleOutcome {
    fn allow(rule: &'static str, code: &'static str, reason: impl Into<String>) -> Self {
        Self::simple(rule, PolicyDecision::Allow, code, reason)
    }

    fn simple(
        rule: &'static str,
        decision: PolicyDecision,
        code: &'static str,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            rule,
            decision,
            reason_code: code,
            reason: bounded(&reason.into()),
            facts: Vec::new(),
            approval: None,
        }
    }

    fn with_facts(mut self, facts: Vec<FactProbe>) -> Self {
        self.facts = facts;
        self
    }
}

/// The complete, inspectable result of one policy evaluation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyAssessment {
    pub decision: PolicyDecision,
    /// The rule that caused the effective decision (`policy.default_allow`
    /// when every applicable rule allowed the action).
    pub rule: String,
    pub reason_code: String,
    pub reason: String,
    pub evaluated_at: i64,
    pub action: PolicyAction,
    pub mission_id: String,
    pub generation: u32,
    pub current_execution_id: Option<String>,
    /// The facts the winning rule actually used.
    pub required_facts: Vec<FactProbe>,
    /// Present when the effective decision is `RequireApproval`.
    pub approval: Option<ApprovalRequest>,
    /// Bounded `rule=decision` summaries for every non-allow co-firing rule.
    /// This retains the full blocking picture instead of only the winning rule,
    /// so a hard cap and an exhausted quota are both visible.
    pub blocking: Vec<String>,
}

impl PolicyAssessment {
    pub fn summary(&self) -> PolicySummary {
        PolicySummary::from_assessment(self)
    }
}

/// A bounded, serializable projection of an assessment for receipts and CLI
/// output. It never carries a secret or a raw runtime response.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct PolicySummary {
    pub decision: String,
    pub rule: String,
    pub reason_code: String,
    pub reason: String,
    pub evaluated_at: i64,
    pub action: String,
    pub approval_id: Option<String>,
    pub required_facts: Vec<String>,
    /// Bounded `rule=decision` summaries of every non-allow co-firing rule.
    pub blocking: Vec<String>,
}

impl PolicySummary {
    pub fn from_assessment(assessment: &PolicyAssessment) -> Self {
        Self {
            decision: assessment.decision.as_str().to_string(),
            rule: assessment.rule.clone(),
            reason_code: assessment.reason_code.clone(),
            reason: assessment.reason.clone(),
            evaluated_at: assessment.evaluated_at,
            action: assessment.action.as_str().to_string(),
            approval_id: assessment
                .approval
                .as_ref()
                .map(|request| request.approval_id.clone()),
            required_facts: bounded_facts(&assessment.required_facts),
            blocking: assessment.blocking.iter().take(8).cloned().collect(),
        }
    }
}

/// Declarative Policy configuration.
///
/// The defaults preserve current OCG behavior exactly: Policy is enabled, it
/// makes the admission boundary explicit, and no action requires approval
/// until an operator asks for one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct PolicyConfig {
    /// Whether the admission boundary is evaluated at all. When false the
    /// Reconciler behaves exactly as before Policy existed.
    pub enabled: bool,
    /// Actions that require an explicit, generation-bound approval. Empty by
    /// default, so no workflow is changed unless configured.
    pub require_approval_for: Vec<String>,
}

impl Default for PolicyConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            require_approval_for: Vec::new(),
        }
    }
}

impl PolicyConfig {
    /// Parse the top-level `policy` section, falling back to the defaults.
    pub fn from_config(data: &serde_json::Value) -> Result<Self> {
        let Some(value) = data.get("policy") else {
            return Ok(Self::default());
        };
        if value.is_null() {
            return Ok(Self::default());
        }
        let object = value.as_object().ok_or_else(|| {
            OcgError::config("policy must be a JSON object with enabled and requireApprovalFor")
        })?;
        let mut config = Self::default();
        if let Some(enabled) = object.get("enabled") {
            config.enabled = enabled
                .as_bool()
                .ok_or_else(|| OcgError::config("policy.enabled must be a boolean"))?;
        }
        if let Some(value) = object.get("requireApprovalFor") {
            let array = value
                .as_array()
                .ok_or_else(|| OcgError::config("policy.requireApprovalFor must be an array"))?;
            for entry in array {
                let name = entry.as_str().ok_or_else(|| {
                    OcgError::config("policy.requireApprovalFor entries must be action names")
                })?;
                if PolicyAction::parse(name).is_none() {
                    return Err(OcgError::config(format!(
                        "policy.requireApprovalFor names unknown action '{name}'"
                    )));
                }
                if !config
                    .require_approval_for
                    .iter()
                    .any(|known| known == name)
                {
                    config.require_approval_for.push(name.to_string());
                }
            }
        }
        config.validate_values()?;
        Ok(config)
    }

    /// Collect every policy problem for whole-configuration validation.
    pub fn validate(data: &serde_json::Value) -> Vec<String> {
        match Self::from_config(data) {
            Ok(_) => Vec::new(),
            Err(error) => vec![error.to_string()],
        }
    }

    fn validate_values(&self) -> Result<()> {
        for name in &self.require_approval_for {
            if PolicyAction::parse(name).is_none() {
                return Err(OcgError::config(format!(
                    "policy.requireApprovalFor names unknown action '{name}'"
                )));
            }
        }
        Ok(())
    }

    pub fn requires_approval(&self, action: PolicyAction) -> bool {
        self.require_approval_for
            .iter()
            .any(|name| name == action.as_str())
    }

    /// A stable fingerprint of the policy.
    pub fn fingerprint(&self) -> String {
        let value = serde_json::to_value(self).unwrap_or(serde_json::Value::Null);
        crate::runtime::hash::sha256_hex(value.to_string().as_bytes())
    }
}

/// Evaluate every applicable rule and aggregate their decisions.
///
/// The evaluation is pure and total: for a fully typed context it cannot fail,
/// so there is no "policy engine crashed into Allow" path. Rule order only
/// breaks ties at the same precedence; the effective decision is the maximum
/// precedence, so it is independent of any accidental ordering.
pub fn evaluate(context: &PolicyContext) -> PolicyAssessment {
    let outcomes = [
        rule_integrity(context),
        rule_terminal(context),
        rule_capability(context),
        rule_observation(context),
        rule_resource(context),
        rule_approval(context),
    ];

    let winner = outcomes
        .iter()
        .filter(|outcome| outcome.decision != PolicyDecision::Allow)
        .max_by_key(|outcome| outcome.decision.precedence());

    let (decision, rule, reason_code, reason, facts, approval) = match winner {
        Some(outcome) => (
            outcome.decision,
            outcome.rule.to_string(),
            outcome.reason_code.to_string(),
            outcome.reason.clone(),
            outcome.facts.clone(),
            outcome.approval.clone(),
        ),
        None => {
            let facts = outcomes
                .iter()
                .flat_map(|outcome| outcome.facts.clone())
                .collect();
            (
                PolicyDecision::Allow,
                "policy.default_allow".to_string(),
                "default_allow".to_string(),
                "no applicable policy rule restricted the action".to_string(),
                facts,
                None,
            )
        }
    };

    let blocking: Vec<String> = outcomes
        .iter()
        .filter(|outcome| outcome.decision != PolicyDecision::Allow)
        .take(8)
        .map(|outcome| format!("{}={}", outcome.rule, outcome.decision.as_str()))
        .collect();

    PolicyAssessment {
        decision,
        rule,
        reason_code,
        reason,
        evaluated_at: context.evaluated_at,
        action: context.action,
        mission_id: context.mission_id.clone(),
        generation: context.generation,
        current_execution_id: context
            .current_execution_id
            .as_ref()
            .map(|id| id.as_str().to_string()),
        required_facts: facts,
        approval,
        blocking,
    }
}

/// Fail closed when the input identity Policy depends on is not usable.
fn rule_integrity(context: &PolicyContext) -> RuleOutcome {
    if context.mission_id.trim().is_empty() || context.generation == 0 {
        return RuleOutcome::simple(
            "policy.integrity",
            PolicyDecision::Deny,
            "invalid_mission_identity",
            "Mission identity or generation is missing; policy fails closed",
        );
    }
    RuleOutcome::allow(
        "policy.integrity",
        "identity_ok",
        "Mission identity and generation are present",
    )
}

/// A terminal Mission is permanently inert. This is primarily owned by the
/// Mission/planner; Policy reflects it defensively so a direct caller cannot
/// obtain admission for a consequential action on a terminal Mission.
fn rule_terminal(context: &PolicyContext) -> RuleOutcome {
    if context.mission_status.is_terminal() {
        return RuleOutcome::simple(
            "mission.terminal",
            PolicyDecision::Deny,
            "terminal_mission",
            format!(
                "terminal Mission ({}) may not execute a consequential action",
                context.mission_status.as_str()
            ),
        );
    }
    RuleOutcome::allow(
        "mission.terminal",
        "mission_active",
        "the Mission is not terminal",
    )
}

/// A proposed action whose capability the current adapter explicitly does not
/// support is not admissible now. No fake fallback is attempted.
fn rule_capability(context: &PolicyContext) -> RuleOutcome {
    let (supported, capability) = context.required_capability();
    let status = if supported {
        FactStatus::Known
    } else {
        // The fact is known: the adapter told us the capability is absent.
        FactStatus::Known
    };
    let fact = FactProbe::new(
        format!("capability.{capability}"),
        status,
        if supported {
            "adapter reports the capability".to_string()
        } else {
            "adapter does not support the capability".to_string()
        },
    );
    if supported {
        RuleOutcome::allow(
            "runtime.capability",
            "capability_supported",
            format!("the runtime supports {capability}"),
        )
        .with_facts(vec![fact])
    } else {
        RuleOutcome::simple(
            "runtime.capability",
            PolicyDecision::Defer,
            "capability_unsupported",
            format!(
                "the proposed action requires {capability}, which the current runtime adapter does not support"
            ),
        )
        .with_facts(vec![fact])
    }
}

/// A create/replace action is only admissible from an authoritative absence.
/// A merely failed observation never authorizes a replacement.
fn rule_observation(context: &PolicyContext) -> RuleOutcome {
    if !context.action.is_replacement() {
        return RuleOutcome::allow(
            "observation.authoritative_absence",
            "not_a_replacement",
            "the action does not create or replace an execution",
        );
    }
    // A durable create intent already owns this invariant: the runtime recovery
    // lookup (not a fresh observation) decides whether the claimed create
    // reached the runtime. Enforcing a fresh authoritative absence here would
    // turn an intentional same-generation retry into a defer and change the
    // existing convergence behavior. Only an *unclaimed* create (Idle/Applied)
    // must be authorized by an authoritative absence.
    if !matches!(
        context.reconcile_status,
        MissionReconcileStatus::Idle | MissionReconcileStatus::Applied
    ) {
        return RuleOutcome::allow(
            "observation.authoritative_absence",
            "durable_create_intent",
            "a durable create intent already exists for this generation; the runtime recovery lookup owns the absence invariant",
        );
    }
    let expected = match context.action {
        PolicyAction::EnsureExecution => ObservationStatus::Unbound,
        _ => ObservationStatus::Missing,
    };
    let authoritative = context.observation == expected;
    let fact = FactProbe::new(
        "observation",
        if authoritative {
            FactStatus::Known
        } else {
            FactStatus::Unknown
        },
        context.observation.as_str().to_string(),
    );
    if authoritative {
        RuleOutcome::allow(
            "observation.authoritative_absence",
            "absence_authoritative",
            "the current execution absence is authoritative",
        )
        .with_facts(vec![fact])
    } else {
        RuleOutcome::simple(
            "observation.authoritative_absence",
            PolicyDecision::Defer,
            "absence_not_authoritative",
            format!(
                "the action would create or replace an execution, but the current observation is '{}' rather than an authoritative absence",
                context.observation.as_str()
            ),
        )
        .with_facts(vec![fact])
    }
}

/// The currently associated resource must not be factually unavailable. This is
/// availability, never capacity: a healthy resource is not thereby proven able
/// to accept new work, and `ResourceHealth::Available` is not treated as a
/// capacity fact.
fn rule_resource(context: &PolicyContext) -> RuleOutcome {
    let status = context
        .resource
        .health_status(context.evaluated_at, RESOURCE_FACT_MAX_AGE_SECONDS);
    let fact = FactProbe::new(
        "resource.health",
        status,
        format!(
            "{} ({})",
            context.resource.health.as_str(),
            context.resource.health_provenance.as_str()
        ),
    );
    match status {
        FactStatus::Unknown => RuleOutcome::allow(
            "resource.availability",
            "resource_health_unknown",
            "no fresh, authoritative resource health fact is known; availability is not admission capacity, so this rule does not block",
        )
        .with_facts(vec![fact]),
        FactStatus::Stale => RuleOutcome::allow(
            "resource.availability",
            "resource_health_stale",
            "the latest resource health fact is stale and is not used as a current fact",
        )
        .with_facts(vec![fact]),
        FactStatus::Known => match context.resource.health {
            ResourceHealth::Unavailable => RuleOutcome::simple(
                "resource.availability",
                PolicyDecision::Defer,
                "resource_unavailable",
                "the associated resource is factually unavailable; the action is not currently safe",
            )
            .with_facts(vec![fact]),
            ResourceHealth::Degraded => RuleOutcome::allow(
                "resource.availability",
                "resource_degraded",
                "the associated resource is degraded but not proven unavailable (advisory)",
            )
            .with_facts(vec![fact]),
            ResourceHealth::Available | ResourceHealth::Unknown => RuleOutcome::allow(
                "resource.availability",
                "resource_reachable",
                "the associated resource is factually reachable; this is not a capacity claim",
            )
            .with_facts(vec![fact]),
        },
    }
}

/// The explicit approval hook. Only active when the configuration requires an
/// approval for this exact action.
fn rule_approval(context: &PolicyContext) -> RuleOutcome {
    if !context.approval.required {
        return RuleOutcome::allow(
            "approval",
            "approval_not_required",
            "no explicit approval is required for this action",
        );
    }
    let request = ApprovalRequest {
        approval_id: approval_id(
            &context.mission_id,
            context.generation,
            context.action,
            context.current_execution_id.as_ref(),
        ),
        mission_id: context.mission_id.clone(),
        generation: context.generation,
        action: context.action,
        current_execution_id: context
            .current_execution_id
            .as_ref()
            .map(|id| id.as_str().to_string()),
        requested_at: context.evaluated_at,
    };
    let fact = FactProbe::new(
        "approval",
        FactStatus::Known,
        match context.approval.status {
            Some(status) => status.as_str().to_string(),
            None => "none".to_string(),
        },
    );
    if context.approval.corrupt {
        return RuleOutcome::simple(
            "approval",
            PolicyDecision::Defer,
            "approval_state_corrupt",
            "the durable approval record for this action is corrupt; refusing to proceed",
        )
        .with_facts(vec![fact]);
    }
    match context.approval.status {
        Some(ApprovalStatus::Approved) => RuleOutcome::allow(
            "approval",
            "approval_granted",
            "an explicit approval for this exact generation and action is recorded",
        )
        .with_facts(vec![fact]),
        Some(ApprovalStatus::Rejected) => RuleOutcome::simple(
            "approval",
            PolicyDecision::Deny,
            "approval_rejected",
            "an explicit approval for this exact generation and action was rejected",
        )
        .with_facts(vec![fact]),
        Some(ApprovalStatus::Pending) | None => {
            let mut outcome = RuleOutcome::simple(
                "approval",
                PolicyDecision::RequireApproval,
                "approval_required",
                "this action requires an explicit approval before it may proceed",
            )
            .with_facts(vec![fact]);
            outcome.approval = Some(request);
            outcome
        }
    }
}

/// Deterministic approval identity. The same Mission generation and action
/// always map to the same approval, so repeated ticks never create duplicates.
pub fn approval_id(
    mission_id: &str,
    generation: u32,
    action: PolicyAction,
    current_execution_id: Option<&RuntimeExecutionId>,
) -> String {
    let key = format!(
        "ocg-approval-v1|{mission_id}|{generation}|{}|{}",
        action.as_str(),
        current_execution_id.map(|id| id.as_str()).unwrap_or("-")
    );
    let digest = crate::runtime::hash::sha256_hex(key.as_bytes());
    format!("apr-{}", digest.get(..16).unwrap_or(&digest))
}

/// The approval directory.
pub fn approval_dir(root: &Path) -> PathBuf {
    crate::orchestration::state::state_dir(root).join(APPROVALS_DIR)
}

/// The path for one approval record.
pub fn approval_path(root: &Path, approval_id: &str) -> Result<PathBuf> {
    if !crate::orchestration::checkpoint::is_safe_id(approval_id) {
        return Err(OcgError::config(format!(
            "unsafe approval id '{approval_id}'"
        )));
    }
    Ok(approval_dir(root).join(format!("{approval_id}.json")))
}

/// A strict raw read of the legacy projection, without consulting the replay
/// authority.
pub(crate) fn load_approval_raw(root: &Path, approval_id: &str) -> Result<Option<ApprovalRecord>> {
    let path = approval_path(root, approval_id)?;
    let text = match fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(OcgError::read(&path, error)),
    };
    let record: ApprovalRecord = serde_json::from_str(&text)
        .map_err(|error| OcgError::config(format!("approval {approval_id} is corrupt: {error}")))?;
    validate_approval(approval_id, &record)?;
    Ok(Some(record))
}

/// Load one approval record.
///
/// Once the replay authority is initialized it is the read source; only before
/// first initialization is the legacy projection consulted. A missing record
/// is `Ok(None)`; a corrupt/unsupported record or an unreadable authority is
/// an explicit error, never a silent reset.
pub fn load_approval(root: &Path, approval_id: &str) -> Result<Option<ApprovalRecord>> {
    match crate::orchestration::replay::read_authoritative_snapshot(root)? {
        Some(snapshot) => Ok(snapshot.approvals.get(approval_id).cloned()),
        None => load_approval_raw(root, approval_id),
    }
}

/// Persist one approval record atomically, then enforce bounded retention.
pub fn save_approval(root: &Path, record: &ApprovalRecord) -> Result<PathBuf> {
    validate_approval(&record.approval_id, record)?;
    crate::runtime::install::ensure_gitignore(root)?;
    // The replay authority is committed before the compatibility projection.
    crate::orchestration::replay::record_approval_update(root, record)?;
    let path = approval_path(root, &record.approval_id)?;
    let value = serde_json::to_value(record)
        .map_err(|error| OcgError::config(format!("cannot serialize approval record: {error}")))?;
    crate::runtime::install::write_json_atomic(&path, &value)?;
    let _ = prune_approvals(root);
    Ok(path)
}

/// Idempotently ensure a pending approval exists for a request. An existing
/// record (pending, approved or rejected) is returned unchanged.
pub fn ensure_pending(root: &Path, request: &ApprovalRequest) -> Result<ApprovalRecord> {
    if let Some(existing) = load_approval(root, &request.approval_id)? {
        return Ok(existing);
    }
    let record = ApprovalRecord::request(request);
    save_approval(root, &record)?;
    Ok(record)
}

/// Resolve an approval. Returns the updated record.
pub fn resolve_approval(
    root: &Path,
    approval_id: &str,
    status: ApprovalStatus,
    note: Option<String>,
    now: i64,
) -> Result<ApprovalRecord> {
    let Some(mut record) = load_approval(root, approval_id)? else {
        return Err(OcgError::config(format!(
            "approval {approval_id} does not exist"
        )));
    };
    record.status = status;
    record.resolved_at = Some(now);
    record.note = note.map(|note| redact(&bounded(&note)));
    save_approval(root, &record)?;
    Ok(record)
}

/// A strict raw scan of the legacy projection, without consulting the replay
/// authority. A missing directory is empty; a directory read error propagates.
/// Per-record corruption is surfaced as an issue, never silently dropped.
pub(crate) fn list_approvals_raw(root: &Path) -> Result<LoadedApprovals> {
    let mut loaded = LoadedApprovals::default();
    let dir = approval_dir(root);
    let entries = match fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(loaded),
        Err(error) => {
            return Err(OcgError::io(
                format!("cannot read approval directory {}", dir.display()),
                error,
            ))
        }
    };
    for entry in entries {
        let entry = entry.map_err(|error| {
            OcgError::io(
                format!(
                    "cannot read an approval directory entry in {}",
                    dir.display()
                ),
                error,
            )
        })?;
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        let Some(name) = path.file_stem().and_then(|value| value.to_str()) else {
            continue;
        };
        match load_approval_raw(root, name) {
            Ok(Some(record)) => loaded.approvals.push(record),
            Ok(None) => {}
            Err(error) => loaded.issues.push(ApprovalIssue {
                file: format!("{name}.json"),
                detail: redact(&error.to_string()),
            }),
        }
    }
    sort_approvals(&mut loaded.approvals);
    Ok(loaded)
}

fn sort_approvals(approvals: &mut [ApprovalRecord]) {
    approvals.sort_by(|left, right| {
        right
            .requested_at
            .cmp(&left.requested_at)
            .then_with(|| right.approval_id.cmp(&left.approval_id))
    });
}

/// List every readable approval record, newest request first.
///
/// Once the replay authority is initialized it is the read source. An
/// initialized-but-unreadable authority fails closed by surfacing a single
/// blocking issue rather than an empty list of approvals. Corrupt legacy
/// projections are surfaced the same way before initialization.
pub fn list_approvals(root: &Path) -> LoadedApprovals {
    match crate::orchestration::replay::read_authoritative_snapshot(root) {
        Ok(Some(snapshot)) => {
            let mut approvals: Vec<ApprovalRecord> = snapshot.approvals.into_values().collect();
            sort_approvals(&mut approvals);
            LoadedApprovals {
                approvals,
                issues: Vec::new(),
            }
        }
        Ok(None) => match list_approvals_raw(root) {
            Ok(loaded) => loaded,
            Err(error) => LoadedApprovals {
                approvals: Vec::new(),
                issues: vec![ApprovalIssue {
                    file: approval_dir(root).display().to_string(),
                    detail: redact(&format!("approval projection could not be read: {error}")),
                }],
            },
        },
        Err(error) => LoadedApprovals {
            approvals: Vec::new(),
            issues: vec![ApprovalIssue {
                file: crate::orchestration::replay::state_path(root)
                    .display()
                    .to_string(),
                detail: redact(&format!("replay authority is unreadable: {error}")),
            }],
        },
    }
}

/// Keep only the newest [`MAX_APPROVALS`] records, never dropping a pending
/// one while a resolved record could be pruned instead. This prunes the
/// compatibility projection on disk; the authority applies the same rule when
/// it applies an approval upsert.
fn prune_approvals(root: &Path) -> Result<()> {
    let loaded = match list_approvals_raw(root) {
        Ok(loaded) => loaded,
        Err(_) => return Ok(()),
    };
    if loaded.approvals.len() <= MAX_APPROVALS {
        return Ok(());
    }
    let mut resolved: Vec<&ApprovalRecord> = loaded
        .approvals
        .iter()
        .filter(|record| !record.status.is_pending())
        .collect();
    resolved.sort_by(|left, right| {
        left.requested_at
            .cmp(&right.requested_at)
            .then_with(|| left.approval_id.cmp(&right.approval_id))
    });
    let mut excess = loaded.approvals.len() - MAX_APPROVALS;
    for record in resolved {
        if excess == 0 {
            break;
        }
        if let Ok(path) = approval_path(root, &record.approval_id) {
            let _ = fs::remove_file(path);
            excess -= 1;
        }
    }
    Ok(())
}

pub(crate) fn validate_approval(expected_id: &str, record: &ApprovalRecord) -> Result<()> {
    if record.schema_version != APPROVAL_SCHEMA_VERSION {
        return Err(OcgError::config(format!(
            "approval {} has unsupported schema_version {}",
            record.approval_id, record.schema_version
        )));
    }
    if record.approval_id != expected_id
        || !crate::orchestration::checkpoint::is_safe_id(&record.approval_id)
        || record.mission_id.trim().is_empty()
        || record.generation == 0
        || record.action.trim().is_empty()
    {
        return Err(OcgError::config(
            "approval record contains an unsafe or incomplete identity",
        ));
    }
    if record
        .note
        .as_deref()
        .is_some_and(|note| note.len() > MAX_REASON_BYTES * 2)
    {
        return Err(OcgError::config("approval note is not bounded"));
    }
    Ok(())
}

/// Build the associated resource identity from a runtime profile. The model
/// selector is split into provider/model; the runtime engine/family is added
/// when an adapter identity is known. Raw agent labels are never part of this
/// identity.
pub fn resource_identity_for_profile(
    profile: &RuntimeProfile,
    runtime: Option<&crate::runtime::lifecycle::RuntimeIdentity>,
) -> ResourceIdentity {
    let selector = profile.model_selector.trim();
    let identity = match selector.split_once('/') {
        Some((provider, model)) if !provider.is_empty() && !model.is_empty() => {
            ResourceIdentity::for_model(provider, model)
        }
        _ => ResourceIdentity::for_model("", selector),
    };
    match runtime {
        Some(runtime) => {
            identity.with_runtime_family(runtime.runtime.clone(), runtime.family.clone())
        }
        None => identity,
    }
}

/// The opaque resource id for an identity. It is used only for a direct lookup
/// of the *currently associated* resource, never as a durable policy key: a
/// future identity enrichment would re-key it, so Policy never matches rules on
/// it.
pub fn associated_resource_id(identity: &ResourceIdentity) -> ResourceId {
    ResourceId::derive(identity)
}

fn bounded(text: &str) -> String {
    let redacted = redact(text);
    if redacted.len() <= MAX_REASON_BYTES {
        return redacted;
    }
    let mut end = MAX_REASON_BYTES;
    while end > 0 && !redacted.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &redacted[..end])
}

fn bounded_facts(facts: &[FactProbe]) -> Vec<String> {
    facts.iter().take(16).map(FactProbe::summary).collect()
}
