//! Placement Decision Evidence and Projection.
//!
//! This module records and projects the durable facts behind each Placement decision:
//! which candidates were considered, why they were rejected or selected, and what
//! the final acquisition outcome was.
//!
//! Placement evidence is captured at decision time and remains immutable, so
//! historical inspection does not re-run current Placement logic.
//!
//! The Governor evaluation API is non-consuming: it inspects eligibility without
//! acquiring resources. The Governor acquisition API remains the authoritative
//! race-safe mechanism that actually reserves execution capacity.

use serde::{Deserialize, Serialize};

use crate::resources::{ResourceHealth, ResourceProvenance};

use super::budget::SpendDecision;
use super::governor::{Governor, GovernorDecision, GovernorScope};

/// Why a candidate was rejected during placement, or what outcome occurred.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PlacementReason {
    /// Candidate does not satisfy required capabilities (tools, images, effort).
    CapabilityRejected { detail: String },

    /// Candidate rejected due to health evidence showing Unavailable state.
    HealthRejected { health_state: String },

    /// Candidate rejected due to budget/economic constraints.
    BudgetRejected {
        reason_code: String,
        decision: String, // SpendDecision serialized as string
    },

    /// Candidate failed non-consuming rate evaluation.
    RateEvaluationUnavailable {
        #[serde(skip_serializing_if = "Option::is_none")]
        retry_after_millis: Option<u64>,
    },

    /// Candidate failed non-consuming capacity evaluation.
    CapacityEvaluationUnavailable,

    /// Candidate was eligible but lost in ranking to a higher-priority candidate.
    RankingLoss {
        health_rank: u32,
        model_preference_mismatch: bool,
        provider_preference_mismatch: bool,
    },

    /// Candidate was selected as best by placement ranking.
    Selected {
        health_rank: u32,
        model_preference_mismatch: bool,
        provider_preference_mismatch: bool,
    },

    /// Selected candidate lost final atomic resource acquisition.
    ReservationLost { reason: String },

    /// Successfully reserved and dispatched.
    Dispatched,
}

/// Evidence about a candidate's health state during placement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlacementHealthEvidence {
    pub state: ResourceHealth,
    pub provenance: ResourceProvenance,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_at: Option<i64>,
}

/// Evidence for one candidate considered during placement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CandidateEvidence {
    pub provider: String,
    pub model: String,

    /// Health evidence observed during placement.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub health: Option<PlacementHealthEvidence>,

    /// Outcome of non-consuming governor evaluation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub governor_evaluation: Option<GovernorEvaluation>,

    /// Ranking inputs if candidate reached ranking.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ranking_tuple: Option<(u32, bool, bool)>,

    /// Why this candidate was rejected, selected, or what outcome occurred.
    pub reason: PlacementReason,
}

/// Non-consuming evaluation of governor state for a candidate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GovernorEvaluation {
    pub rate_available: bool,
    pub capacity_available: bool,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_retry_after_millis: Option<u64>,
}

/// The mode of target selection used.
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum PlacementMode {
    /// Placement ranked candidates and selected one.
    SelectCandidate {
        candidates_considered: Vec<CandidateEvidence>,
        selected_provider: String,
        selected_model: String,
    },

    /// Exact target was specified; no candidate selection occurred.
    Exact {
        provider: String,
        model: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        effort: Option<String>,
    },
}

/// Complete durable record of a placement decision.
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlacementDecision {
    /// Which Job this decision belongs to.
    pub job_id: String,

    /// Mode of target selection.
    pub mode: PlacementMode,

    /// When the decision was made (Unix timestamp millis).
    pub decided_at: i64,

    /// Final acquisition outcome after selection (if applicable).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub acquisition_outcome: Option<AcquisitionOutcome>,
}

/// Outcome of final atomic resource acquisition.
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcquisitionOutcome {
    pub success: bool,
    pub governor_decision: String, // GovernorDecision serialized

    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<String>,
}

/// Non-consuming Governor evaluation API.
///
/// This inspects current rate and capacity state without consuming resources.
/// Rate evaluation does not append to the sliding window.
/// Capacity evaluation does not acquire a permit.
pub fn evaluate_governor(
    governor: &Governor,
    scope: &GovernorScope,
) -> crate::error::Result<GovernorEvaluation> {
    let (rate_available, rate_retry_after_millis) = evaluate_rate(governor, scope)?;
    let capacity_available = evaluate_capacity(governor, scope)?;

    Ok(GovernorEvaluation {
        rate_available,
        capacity_available,
        rate_retry_after_millis,
    })
}

fn evaluate_rate(
    governor: &Governor,
    scope: &GovernorScope,
) -> crate::error::Result<(bool, Option<u64>)> {
    // Access internal state to evaluate without consuming.
    // This requires exposing a read-only evaluation method on Governor.
    // For now, we'll add this capability to Governor itself.
    governor.evaluate_rate(scope)
}

fn evaluate_capacity(governor: &Governor, scope: &GovernorScope) -> crate::error::Result<bool> {
    governor.evaluate_capacity(scope)
}

/// Serialize SpendDecision for durable storage.
pub(crate) fn serialize_spend_decision(decision: SpendDecision) -> String {
    match decision {
        SpendDecision::Allow => "allow".to_string(),
        SpendDecision::Deny => "deny".to_string(),
        SpendDecision::Defer => "defer".to_string(),
    }
}

/// Serialize GovernorDecision for durable storage.
#[allow(dead_code)]
pub(crate) fn serialize_governor_decision(decision: &GovernorDecision) -> String {
    match decision {
        GovernorDecision::AllowedNow => "allowed_now".to_string(),
        GovernorDecision::RateLimited { retry_after } => {
            format!("rate_limited:{}", retry_after.as_millis())
        }
        GovernorDecision::CapacityUnavailable => "capacity_unavailable".to_string(),
    }
}
