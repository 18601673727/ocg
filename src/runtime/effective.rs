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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{self, ModelRequirement, ModelRequirementKind};
    use crate::preflight::{ModelCheck, ModelPreflight};
    use crate::runtime::compat::LeadSelection;
    use serde_json::Value;

    fn configured(variant: Option<&str>) -> LeadSelection {
        LeadSelection {
            level: "high".to_string(),
            agent: "lead-high".to_string(),
            provider_id: "openai".to_string(),
            model_id: "gpt-6-astra".to_string(),
            variant: variant.map(str::to_string),
        }
    }

    fn requirement(variant: Option<&str>) -> ModelRequirement {
        ModelRequirement {
            label: "lead-high".to_string(),
            agent: "lead-high".to_string(),
            full_model_id: "openai/gpt-6-astra".to_string(),
            variant: variant.map(str::to_string),
            kind: ModelRequirementKind::Lead,
        }
    }

    fn complete(availability: Availability) -> ModelPreflight {
        ModelPreflight::Complete {
            checks: vec![ModelCheck {
                requirement: requirement(None),
                availability,
            }],
        }
    }

    #[test]
    fn missing_provider_and_missing_model_stay_distinct() {
        let lead = configured(None);
        assert_eq!(
            RuntimeState::model_evidence(Some(&complete(Availability::MissingProvider)), &lead),
            ModelEvidence::MissingProvider
        );
        assert_eq!(
            RuntimeState::model_evidence(Some(&complete(Availability::MissingModel)), &lead),
            ModelEvidence::MissingModel
        );
        assert_eq!(
            RuntimeState::model_evidence(Some(&complete(Availability::Available)), &lead),
            ModelEvidence::Available
        );
        assert!(matches!(
            RuntimeState::model_evidence(None, &lead),
            ModelEvidence::Unverified(_)
        ));
        assert!(matches!(
            RuntimeState::model_evidence(
                Some(&ModelPreflight::Unavailable {
                    reason: "probe failed".to_string()
                }),
                &lead
            ),
            ModelEvidence::Unverified(reason) if reason == "probe failed"
        ));
    }

    #[test]
    fn an_unobserved_effective_state_is_never_reported_as_matching() {
        let state = RuntimeState {
            level: "high".to_string(),
            configured: configured(None),
            static_errors: Vec::new(),
            model: ModelEvidence::Available,
            effective: EffectiveEvidence::NotObserved("no v2 runtime".to_string()),
            identity: None,
        };
        assert_eq!(state.effective_matches(), None);
        let (status, detail) = state.effective_line();
        assert_eq!(status, "info");
        assert!(detail.contains("not observed"), "{detail}");
    }

    #[test]
    fn an_observed_matching_lead_is_ok_and_normalizes_the_default_variant() {
        let state = RuntimeState {
            level: "high".to_string(),
            configured: configured(None),
            static_errors: Vec::new(),
            model: ModelEvidence::Available,
            effective: EffectiveEvidence::Observed {
                session_id: "ses_1".to_string(),
                lead: EffectiveLead {
                    agent: Some("lead-high".to_string()),
                    provider_id: Some("openai".to_string()),
                    model_id: Some("gpt-6-astra".to_string()),
                    variant: Some("default".to_string()),
                },
            },
            identity: None,
        };
        assert_eq!(state.effective_matches(), Some(true));
        let (status, detail) = state.effective_line();
        assert_eq!(status, "ok");
        assert!(detail.contains("provider-default"), "{detail}");
        assert!(detail.contains("openai/gpt-6-astra"), "{detail}");
    }

    #[test]
    fn a_declared_variant_must_be_observed_exactly() {
        let observed = |variant: Option<&str>| EffectiveLead {
            agent: Some("lead-high".to_string()),
            provider_id: Some("openai".to_string()),
            model_id: Some("gpt-6-astra".to_string()),
            variant: variant.map(str::to_string),
        };
        let state_with = |effective: EffectiveLead| RuntimeState {
            level: "high".to_string(),
            configured: configured(Some("high")),
            static_errors: Vec::new(),
            model: ModelEvidence::Available,
            effective: EffectiveEvidence::Observed {
                session_id: "ses_1".to_string(),
                lead: effective,
            },
            identity: None,
        };

        assert_eq!(
            state_with(observed(Some("high"))).effective_matches(),
            Some(true)
        );
        // `default` is not the declared variant and must not satisfy it.
        assert_eq!(
            state_with(observed(Some("default"))).effective_matches(),
            Some(false)
        );
        assert_eq!(state_with(observed(None)).effective_matches(), Some(false));

        let (status, detail) = state_with(observed(Some("low"))).effective_line();
        assert_eq!(status, "error");
        assert!(detail.contains("contradicts"), "{detail}");
    }

    #[test]
    fn a_contradictory_observed_lead_is_an_error_not_a_success() {
        let state = RuntimeState {
            level: "high".to_string(),
            configured: configured(None),
            static_errors: Vec::new(),
            model: ModelEvidence::Available,
            effective: EffectiveEvidence::Observed {
                session_id: "ses_1".to_string(),
                lead: EffectiveLead {
                    agent: Some("lead-high".to_string()),
                    provider_id: Some("openai".to_string()),
                    model_id: Some("gpt-5-other".to_string()),
                    variant: None,
                },
            },
            identity: None,
        };
        assert_eq!(state.effective_matches(), Some(false));
        assert_eq!(state.effective_line().0, "error");
    }

    #[test]
    fn a_failed_observation_is_reported_distinctly_from_an_absent_one() {
        let unavailable = RuntimeState {
            level: "high".to_string(),
            configured: configured(None),
            static_errors: Vec::new(),
            model: ModelEvidence::Available,
            effective: EffectiveEvidence::Unavailable("runtime not ready".to_string()),
            identity: None,
        };
        let (status, detail) = unavailable.effective_line();
        assert_eq!(status, "error");
        assert!(detail.contains("could not be verified"), "{detail}");
        assert_eq!(unavailable.effective_matches(), None);
    }

    #[test]
    fn static_errors_make_the_state_unresolved() {
        let state = RuntimeState {
            level: "high".to_string(),
            configured: configured(None),
            static_errors: vec!["unknown variant".to_string()],
            model: ModelEvidence::Available,
            effective: EffectiveEvidence::NotObserved("not probed".to_string()),
            identity: None,
        };
        assert!(!state.resolved());
    }

    #[test]
    fn a_configured_contract_is_read_from_the_same_resolution_as_launch() {
        // Guard the Configured state against drifting from launch's resolution.
        let mut data: Value = crate::defaults::load_defaults(&crate::defaults::OcgSource::Embedded)
            .expect("embedded defaults");
        data["profile"] = serde_json::json!({"origin":"new", "defaultModel":"chosen"});
        data["models"] = serde_json::json!({"providers":{"local":{"label":"Local","placeholder":false}},
            "models":{"chosen":{"provider":"local","id":"model","placeholder":false}}});
        let contract = model::lead_contract(&data, "chosen").expect("lead contract");
        let selection = LeadSelection::from_contract(&contract);
        assert_eq!(selection.level, "chosen");
        assert_eq!(selection.agent, model::lead_agent_id("chosen"));
        assert_eq!(selection.full_model_id(), "local/model");
    }
}
