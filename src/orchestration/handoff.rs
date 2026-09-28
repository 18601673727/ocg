//! Typed model hand-off capsules.
//!
//! The rich [`crate::context::capsule::TaskCapsule`] is the full, inspectable
//! planning artifact. By itself it carries structured planning state (goal,
//! findings, files, symbols, decisions, provenance) and **not** source slices.
//! [`ProjectionInput`] combines that capsule with selected source slices, a
//! bounded diff summary and an optional verification block. A **hand-off
//! capsule** is a different, much smaller thing: the exact typed projection of
//! that rich input that a role transition gets to see.
//!
//! ```text
//! ProjectionInput = rich TaskCapsule + source slices + diff + verification
//!         │  deterministic, role-masked projection
//!         ▼
//! ModelHandoffCapsule  (typed, bounded, secret-checked, no source slices)
//! ```
//!
//! Invariants:
//!
//! - It is typed. A role is an enum, a transition is an enum and the required
//!   projection fields are named, not implied.
//! - It is deterministic. Every list is sorted and every omission is counted,
//!   so the same rich state always yields the same bytes.
//! - It is bounded. The projection drops optional material, never the required
//!   goal, hard constraints, critical findings, changed files or failing
//!   locations, until it fits `max_handoff_bytes` and the ratio cap.
//! - It is secret-checked at the boundary. Secret-shaped text is dropped and
//!   sensitive paths never enter a capsule.
//! - It never carries source slices. Selected source is a separate dynamic
//!   context block, so the compact hand-off stays compact.

use crate::context::capsule::CapsuleFile;
use crate::context::freshness::Provenance;
use crate::context::gitdiff::GitState;
use crate::context::ranking::ContextSlice;
use crate::context::symbols::SymbolRef;
use crate::verification::distill::SourceLocation;
use serde::{Deserialize, Serialize};

/// The hand-off schema version. Bumping it invalidates old state.
pub const HANDOFF_SCHEMA_VERSION: u32 = 1;

/// The seven orchestration roles. `ExploreDeep` is distinct from `Explore`
/// because its model and its budget are different, not because it is a variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Lead,
    Explore,
    ExploreDeep,
    Build,
    Verify,
    Debug,
    Docs,
}

impl Role {
    /// Every role, in a stable order.
    pub fn all() -> [Role; 7] {
        [
            Role::Lead,
            Role::Explore,
            Role::ExploreDeep,
            Role::Build,
            Role::Verify,
            Role::Debug,
            Role::Docs,
        ]
    }

    /// The canonical name used in state, telemetry and hand-offs.
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Lead => "lead",
            Role::Explore => "explore",
            Role::ExploreDeep => "explore_deep",
            Role::Build => "build",
            Role::Verify => "verify",
            Role::Debug => "debug",
            Role::Docs => "docs",
        }
    }

    /// The routing role name (also the prompt file stem).
    pub fn routing_role(self) -> &'static str {
        match self {
            Role::Lead => "lead",
            Role::Explore => "explore",
            Role::ExploreDeep => "explore-deep",
            Role::Build => "build",
            Role::Verify => "verify",
            Role::Debug => "debug",
            Role::Docs => "docs",
        }
    }

    /// The generated OpenCode worker agent for this role. The Lead is not a
    /// worker route, so it returns `None`.
    pub fn agent(self) -> Option<String> {
        match self {
            Role::Lead => None,
            other => Some(format!(
                "{}{}",
                crate::defaults::WORKER_AGENT_PREFIX,
                other.routing_role()
            )),
        }
    }

    /// Parse a canonical name, a routing name or an `ocg-` agent id.
    pub fn parse(text: &str) -> Option<Role> {
        let normalized = text
            .trim()
            .to_ascii_lowercase()
            .replace('-', "_")
            .trim_start_matches("ocg_")
            .to_string();
        match normalized.as_str() {
            "lead" => Some(Role::Lead),
            "explore" => Some(Role::Explore),
            "explore_deep" | "exploredeep" => Some(Role::ExploreDeep),
            "build" => Some(Role::Build),
            "verify" => Some(Role::Verify),
            "debug" => Some(Role::Debug),
            "docs" | "doc" => Some(Role::Docs),
            _ => None,
        }
    }
}

/// A phase transition. The transitions mirror the checkpoint phases plus the
/// Lead→role entry transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Transition {
    LeadToRole,
    ExploreToBuild,
    BuildToVerify,
    VerifyToDebug,
    DebugToBuild,
    ToLead,
}

impl Transition {
    pub fn as_str(self) -> &'static str {
        match self {
            Transition::LeadToRole => "lead_to_role",
            Transition::ExploreToBuild => "explore_to_build",
            Transition::BuildToVerify => "build_to_verify",
            Transition::VerifyToDebug => "verify_to_debug",
            Transition::DebugToBuild => "debug_to_build",
            Transition::ToLead => "to_lead",
        }
    }
}

/// How strongly a finding must survive projection. `Critical` items are never
/// dropped; the level is only assigned from explicit evidence, never inferred.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    #[default]
    Info,
    Warning,
    Critical,
}

/// One projected finding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HandoffFinding {
    pub summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default)]
    pub severity: Severity,
}

/// The verification state carried across Build→Verify and Verify→Debug.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HandoffVerification {
    /// The stage that ran.
    pub stage: String,
    /// `passed` / `failed` / `not_run`. Never guessed.
    pub outcome: String,
    #[serde(default)]
    pub failed_commands: Vec<String>,
    #[serde(default)]
    pub failed_tests: Vec<String>,
    /// Failing source locations (`path:line:column`).
    #[serde(default)]
    pub locations: Vec<SourceLocation>,
    /// Raw-log references, relative to the project root.
    #[serde(default)]
    pub raw_log_refs: Vec<String>,
    /// A bounded set of distilled summary lines.
    #[serde(default)]
    pub distilled: Vec<String>,
}

impl HandoffVerification {
    pub fn passed(&self) -> bool {
        self.outcome == "passed"
    }
}

/// The rich projection input. This is the *source* context: it still contains
/// selected source slices and the full diff text. It is never handed to a model
/// directly; [`ModelHandoffCapsule`] is.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ProjectionInput {
    pub task: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub goal: Option<String>,
    #[serde(default)]
    pub constraints: Vec<String>,
    #[serde(default)]
    pub findings: Vec<HandoffFinding>,
    #[serde(default)]
    pub files: Vec<CapsuleFile>,
    #[serde(default)]
    pub symbols: Vec<SymbolRef>,
    #[serde(default)]
    pub decisions: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verification: Option<HandoffVerification>,
    #[serde(default)]
    pub failures: Vec<String>,
    #[serde(default)]
    pub evidence: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diff_ref: Option<String>,
    #[serde(default)]
    pub diff_context: String,
    #[serde(default)]
    pub raw_log_refs: Vec<String>,
    #[serde(default)]
    pub git: GitState,
    #[serde(default)]
    pub provenance: Provenance,
    /// Selected source slices. Kept separate from the hand-off capsule.
    #[serde(default)]
    pub slices: Vec<ContextSlice>,
}

impl ProjectionInput {
    /// The byte size of the rich source context. Used for the ratio cap.
    pub fn rich_bytes(&self) -> usize {
        serde_json::to_vec(self)
            .map(|bytes| bytes.len())
            .unwrap_or(0)
    }

    /// The byte size of the selected source slices alone.
    pub fn selected_source_bytes(&self) -> usize {
        self.slices.iter().map(|slice| slice.bytes).sum()
    }

    /// The byte size of the diff context alone.
    pub fn diff_context_bytes(&self) -> usize {
        self.diff_context.len()
    }

    /// The byte size of the verification block alone.
    pub fn verification_context_bytes(&self) -> usize {
        self.verification
            .as_ref()
            .and_then(|verification| serde_json::to_vec(verification).ok())
            .map(|bytes| bytes.len())
            .unwrap_or(0)
    }
}

/// The compact, typed projection that a role transition receives.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelHandoffCapsule {
    pub schema_version: u32,
    pub transition: Transition,
    pub source: Role,
    pub destination: Role,
    pub task_id: String,
    pub session_id: String,
    pub task: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub goal: Option<String>,
    #[serde(default)]
    pub hard_constraints: Vec<String>,
    #[serde(default)]
    pub findings: Vec<HandoffFinding>,
    #[serde(default)]
    pub files: Vec<CapsuleFile>,
    #[serde(default)]
    pub symbols: Vec<SymbolRef>,
    #[serde(default)]
    pub decisions: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verification: Option<HandoffVerification>,
    #[serde(default)]
    pub failures: Vec<String>,
    #[serde(default)]
    pub evidence: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diff_ref: Option<String>,
    /// A bounded, deterministic rendering of the real diff (changed paths, kept
    /// hunks, structural truncation). Present only for roles that review the
    /// change (Build/Verify/Debug).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diff_context: Option<String>,
    #[serde(default)]
    pub raw_log_refs: Vec<String>,
    #[serde(default)]
    pub git: GitState,
    /// Human-readable notes about what the projection dropped to fit its caps.
    #[serde(default)]
    pub omitted: Vec<String>,
    /// The projection schema version stamped for traceability.
    #[serde(default)]
    pub projection: String,
}

impl Default for ModelHandoffCapsule {
    fn default() -> Self {
        Self {
            schema_version: HANDOFF_SCHEMA_VERSION,
            transition: Transition::LeadToRole,
            source: Role::Lead,
            destination: Role::Explore,
            task_id: String::new(),
            session_id: String::new(),
            task: String::new(),
            goal: None,
            hard_constraints: Vec::new(),
            findings: Vec::new(),
            files: Vec::new(),
            symbols: Vec::new(),
            decisions: Vec::new(),
            verification: None,
            failures: Vec::new(),
            evidence: Vec::new(),
            diff_ref: None,
            diff_context: None,
            raw_log_refs: Vec::new(),
            git: GitState::default(),
            omitted: Vec::new(),
            projection: String::new(),
        }
    }
}

impl ModelHandoffCapsule {
    /// The authoritative compact size. No `bytes` field is stored, so the value
    /// is stable and cannot disagree with the serialized form.
    pub fn measured_bytes(&self) -> usize {
        serde_json::to_vec(self)
            .map(|bytes| bytes.len())
            .unwrap_or(0)
    }

    /// The first failing location, if any. Used by tests and by Debug framing.
    pub fn first_failing_location(&self) -> Option<&SourceLocation> {
        self.verification
            .as_ref()
            .and_then(|verification| verification.locations.first())
    }
}

/// A stable fingerprint of the destination projection schema.
pub fn projection_id(source: Role, destination: Role) -> String {
    format!("{}-to-{}", source.as_str(), destination.as_str())
}
