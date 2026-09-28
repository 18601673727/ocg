//! Configured / Resolved / Effective runtime state.
//!
//! An OCG configuration saying that a provider/model is selected does not prove
//! that the runtime executing a task is using it. OCG therefore keeps three
//! distinct states apart instead of collapsing them into "the model is set":
//!
//! - **Configured** — what the OCG Profile requests. This is exactly
//!   [`crate::model::lead_contract`] read over the effective layers.
//! - **Resolved** — what OCG's own validation accepts, plus the runtime
//!   catalogue evidence that the requested provider/model is actually exposed.
//! - **Effective** — what a live runtime session reports after OCG activates
//!   the resolved contract on it.
//!
//! The three can diverge, and this module never fabricates one from another.
//! In particular, the OpenCode 2 session API accepts any provider/model id
//! without validating it (confirmed against a live 2.0.11 server), so a
//! successful session read-back proves *intent* while the catalogue probe
//! proves *availability*. [`RuntimeState`] records both, and the reporting
//! helpers keep the failure classes distinct.
//!
//! The production observation path — start an OCG-owned server, activate the
//! resolved contract, read the session back — lives here too, so `launch`,
//! `ocg config lead` and `ocg doctor --effective` all verify the same way.

use crate::error::Result;
use crate::preflight::{Availability, ModelPreflight};
use crate::proxy::ChildProxyEnv;
use crate::runtime::compat::v2_client::V2SessionClient;
use crate::runtime::compat::v2_server::{OwnedV2Server, RuntimeIdentity};
use crate::runtime::compat::{EffectiveLead, LeadSelection, SessionClient};
use crate::runtime::lifecycle::RuntimeAdapter as RuntimeLifecycleAdapter;
use std::ffi::OsString;
use std::path::Path;

/// Runtime catalogue evidence for the configured model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelEvidence {
    /// The runtime catalogue currently exposes the exact provider/model.
    Available,
    /// The runtime exists but does not expose the provider at all.
    MissingProvider,
    /// The provider is exposed but not this model.
    MissingModel,
    /// No catalogue evidence could be collected (no runtime, or the probe
    /// failed). Deliberately distinct from "missing".
    Unverified(String),
}

impl ModelEvidence {
    /// `(status, detail)` for a diagnostic line.
    pub fn describe(&self, full_model_id: &str, variant: Option<&str>) -> (&'static str, String) {
        let variant = variant
            .map(|v| format!(" (variant {v})"))
            .unwrap_or_default();
        match self {
            ModelEvidence::Available => (
                "ok",
                format!("{full_model_id}{variant} is exposed by the resolved runtime catalogue"),
            ),
            ModelEvidence::MissingProvider => (
                "error",
                format!(
                    "{full_model_id}{variant} — the resolved runtime does not expose this provider; authenticate/configure it or adjust OCG configuration"
                ),
            ),
            ModelEvidence::MissingModel => (
                "error",
                format!(
                    "{full_model_id}{variant} — the resolved runtime exposes this provider but not this model; adjust OCG configuration or the provider catalogue"
                ),
            ),
            ModelEvidence::Unverified(reason) => (
                "warn",
                format!("{full_model_id}{variant} was not checked against the runtime: {reason}"),
            ),
        }
    }
}

/// Live-runtime evidence for the effective Lead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EffectiveEvidence {
    /// A live runtime session was activated and reports this exact Lead.
    Observed {
        session_id: String,
        lead: EffectiveLead,
    },
    /// A runtime was reached but activation or read-back failed.
    Unavailable(String),
    /// No live runtime could be observed (for example a v1 runtime, which has
    /// no session-level Lead, or no resolvable runtime at all).
    NotObserved(String),
}

/// One level's Configured / Resolved / Effective state.
#[derive(Debug, Clone)]
pub struct RuntimeState {
    pub level: String,
    /// What the configuration requests (Configured).
    pub configured: LeadSelection,
    /// Whether OCG's own static validation accepted that configuration
    /// (Resolved, part 1).
    pub static_errors: Vec<String>,
    /// Runtime catalogue evidence for the configured model (Resolved, part 2).
    pub model: ModelEvidence,
    /// What the live session reports (Effective).
    pub effective: EffectiveEvidence,
    /// Which runtime endpoint OCG talked to, when one was reached.
    pub identity: Option<RuntimeIdentity>,
}

impl RuntimeState {
    /// Build the catalogue evidence for the configured agent/model from a
    /// reduced runtime preflight. `None` (no probe attempted) is `Unverified`,
    /// never `Available`.
    pub fn model_evidence(
        preflight: Option<&ModelPreflight>,
        configured: &LeadSelection,
    ) -> ModelEvidence {
        let Some(preflight) = preflight else {
            return ModelEvidence::Unverified(
                "no OpenCode runtime could be resolved for a catalogue probe".to_string(),
            );
        };
        match preflight {
            ModelPreflight::Unavailable { reason } => ModelEvidence::Unverified(reason.clone()),
            ModelPreflight::Complete { checks } => {
                let full = configured.full_model_id();
                match checks
                    .iter()
                    .find(|check| {
                        check.requirement.agent == configured.agent
                            && check.requirement.full_model_id == full
                    })
                    .map(|check| check.availability)
                {
                    Some(Availability::MissingProvider) => ModelEvidence::MissingProvider,
                    Some(Availability::MissingModel) => ModelEvidence::MissingModel,
                    // An available entry, or a level with no catalogue
                    // requirement at all, is as verified as the catalogue gets.
                    _ => ModelEvidence::Available,
                }
            }
        }
    }

    /// Whether the configured model is statically valid and (when evidence
    /// exists) available.
    pub fn resolved(&self) -> bool {
        self.static_errors.is_empty()
            && matches!(
                self.model,
                ModelEvidence::Available | ModelEvidence::Unverified(_)
            )
    }

    /// Compare the effective observation with the configured contract.
    ///
    /// `None` means the effective state could not be observed, so no comparison
    /// is possible. The variant is only compared when the configuration
    /// declares one, mirroring [`crate::runtime::compat::verify_effective_lead`]:
    /// a provider-default contract is satisfied by any provider variant.
    pub fn effective_matches(&self) -> Option<bool> {
        let EffectiveEvidence::Observed { lead, .. } = &self.effective else {
            return None;
        };
        let expected = &self.configured;
        let same = lead.agent.as_deref() == Some(expected.agent.as_str())
            && lead.provider_id.as_deref() == Some(expected.provider_id.as_str())
            && lead.model_id.as_deref() == Some(expected.model_id.as_str())
            && match &expected.variant {
                Some(variant) => lead.variant.as_deref() == Some(variant.as_str()),
                None => true,
            };
        Some(same)
    }

    /// `(status, detail)` for the Effective diagnostic line. Distinguishes
    /// "not observed", "observed and matching" and "contradictory", and never
    /// upgrades an unobserved state to a verified one.
    pub fn effective_line(&self) -> (&'static str, String) {
        match &self.effective {
            EffectiveEvidence::Observed { session_id, lead } => {
                let rendered = render_effective(lead);
                match self.effective_matches() {
                    Some(true) => (
                        "ok",
                        format!(
                            "session {session_id} on {} reports {rendered}",
                            self.identity
                                .as_ref()
                                .map(RuntimeIdentity::describe)
                                .unwrap_or_else(|| "the runtime".to_string())
                        ),
                    ),
                    Some(false) => (
                        "error",
                        format!(
                            "session {session_id} reports {rendered}, which contradicts the configured {} on {}",
                            self.configured.agent,
                            self.configured.full_model_id()
                        ),
                    ),
                    // `Observed` always carries both comparisons; this arm is
                    // unreachable but keeps the reporting total.
                    None => ("error", format!("session {session_id} reports {rendered}")),
                }
            }
            EffectiveEvidence::Unavailable(reason) => (
                "error",
                format!(
                    "the runtime was reached but the effective Lead could not be verified: {reason}"
                ),
            ),
            EffectiveEvidence::NotObserved(reason) => (
                "info",
                format!("the effective runtime Lead was not observed: {reason}"),
            ),
        }
    }
}

/// Render an effective Lead for a report. A runtime-reported `default` variant
/// is the provider default, not a fabricated value.
pub fn render_effective(lead: &EffectiveLead) -> String {
    let agent = lead.agent.as_deref().unwrap_or("(unknown agent)");
    let provider = lead.provider_id.as_deref().unwrap_or("(unknown provider)");
    let model = lead.model_id.as_deref().unwrap_or("(unknown model)");
    let variant = match lead.variant.as_deref() {
        None | Some("") | Some("default") => "provider-default".to_string(),
        Some(variant) => format!("variant {variant}"),
    };
    format!("{agent} on {provider}/{model} ({variant})")
}

/// The verified result of activating one resolved Lead on an owned runtime.
#[derive(Debug, Clone)]
pub struct ObservedActivation {
    pub identity: RuntimeIdentity,
    pub session_id: String,
    pub effective: EffectiveLead,
}

/// Start an OCG-owned OpenCode 2 runtime, activate the resolved Lead on a
/// session, verify it and read the effective state back.
///
/// This is the strongest evidence OCG can produce: the exact server OCG would
/// launch accepted the exact generated configuration, became ready, and its
/// session reports the exact resolved agent/provider/model/variant. The server
/// is terminated on return; nothing is persisted.
pub fn observe_owned_v2(
    program: &Path,
    config_content: &str,
    extra_env: &[(OsString, OsString)],
    proxy: &ChildProxyEnv,
    lead: &LeadSelection,
    directory: &str,
) -> Result<ObservedActivation> {
    let server = OwnedV2Server::start(program, config_content, extra_env, proxy)?;
    let mut client = V2SessionClient::connect(server.registration(), directory)?;
    let profile = lead.runtime_profile();
    let session_id = RuntimeLifecycleAdapter::resolve_execution(&mut client)
        .map_err(|error| crate::error::OcgError::config(error.to_string()))?;
    RuntimeLifecycleAdapter::prepare_execution(&mut client, &session_id, &profile)
        .map_err(|error| crate::error::OcgError::config(error.to_string()))?;
    let effective = client.effective_lead(session_id.as_str())?;
    Ok(ObservedActivation {
        identity: server.identity().clone(),
        session_id: session_id.to_string(),
        effective,
    })
}
