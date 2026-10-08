use crate::core_contract::{Failure, FailureClass};
use crate::error::Result;
use crate::profile::{Model, Profile};
use crate::resources::{ResourceHealth, ResourceId, ResourceIdentity, ResourceProvenance};
use serde::{Deserialize, Serialize};
use std::path::Path;

use super::budget::{BudgetConfig, QuotaFacts, SpendDecision};
use super::domain::{job_failure, DomainRepository, JobSpec};
use super::governor::Governor;
use super::placement_projection::{
    evaluate_governor, serialize_spend_decision, CandidateEvidence, PlacementHealthEvidence,
    PlacementReason,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ProviderChoice {
    pub provider: String,
    pub model: String,
}

pub(crate) struct Requirements<'a> {
    pub images: bool,
    pub effort: Option<&'a str>,
}

pub(crate) fn supports_effort(model: &Model, effort: &str) -> bool {
    let metadata = model.metadata.as_ref();
    metadata
        .and_then(|metadata| metadata.efforts.as_ref())
        .is_some_and(|efforts| efforts.iter().any(|candidate| candidate == effort))
        || model.variants.iter().any(|candidate| candidate == effort)
        || metadata
            .and_then(|metadata| metadata.variants.as_ref())
            .is_some_and(|variants| variants.iter().any(|candidate| candidate == effort))
}

pub(crate) struct PolicyInput<'a> {
    pub profile: &'a Profile,
    pub root: &'a Path,
    pub project_id: &'a str,
    pub job_id: Option<&'a str>,
    pub spec: &'a JobSpec,
    pub requirements: Requirements<'a>,
    pub preferred_provider: Option<&'a str>,
    pub preferred_model: Option<&'a str>,
    pub budget: &'a BudgetConfig,
    pub concurrency: Option<usize>,
    pub governor: Option<&'a Governor>,
    pub now: i64,
}

pub(crate) struct PlacementRefusal {
    pub failure: Failure,
    pub profile_incompatible: bool,
}

/// Result of placement candidate selection with evidence.
pub(crate) struct PlacementResult {
    pub choice: ProviderChoice,
    pub evidence: Vec<CandidateEvidence>,
}

// This decision is transient. Job/Attempt/DispatchIntent remain the durable
// authorities; a replay reads the same configuration and persisted facts.
pub(crate) fn choose(
    domain: &DomainRepository,
    input: PolicyInput<'_>,
) -> Result<std::result::Result<PlacementResult, PlacementRefusal>> {
    let mut executable = None;
    let mut compatible = false;
    let loaded = crate::resources::load(input.root);
    if loaded.corrupt || !loaded.issues.is_empty() {
        return Ok(Err(PlacementRefusal {
            profile_incompatible: false,
            failure: job_failure(
                "placement_health_unknown",
                FailureClass::Provider,
                "Resource Registry contains unreadable availability facts",
                true,
            ),
        }));
    }

    let mut all_evidence = Vec::new();
    let mut candidates = Vec::new();
    let mut unavailable = false;
    let mut budget_failure = None;

    for (key, model) in input.profile.runnable_models() {
        if input
            .spec
            .provider
            .as_ref()
            .is_some_and(|provider| provider != &model.provider)
            || input
                .spec
                .model
                .as_ref()
                .is_some_and(|selected| selected != key)
        {
            continue;
        }
        let metadata = model.metadata.as_ref();
        let Some(provider) = input.profile.providers.get(&model.provider) else {
            continue;
        };
        let reported = provider
            .catalog
            .as_ref()
            .and_then(|catalog| catalog.models.iter().find(|entry| entry.id == model.id))
            .map(|entry| &entry.metadata);
        let tools = metadata
            .and_then(|metadata| metadata.tools)
            .or_else(|| reported.and_then(|metadata| metadata.tools));
        let images = metadata
            .and_then(|metadata| metadata.images)
            .or_else(|| reported.and_then(|metadata| metadata.images));
        let multimodal = metadata
            .and_then(|metadata| metadata.multimodal)
            .or_else(|| reported.and_then(|metadata| metadata.multimodal));

        // Capability check - record rejections
        if tools == Some(false) {
            all_evidence.push(CandidateEvidence {
                provider: model.provider.clone(),
                model: key.clone(),
                health: None,
                governor_evaluation: None,
                ranking_tuple: None,
                reason: PlacementReason::CapabilityRejected {
                    detail: "tools not supported".to_string(),
                },
            });
            continue;
        }
        if input.requirements.images
            && (images == Some(false) || (images.is_none() && multimodal == Some(false)))
        {
            all_evidence.push(CandidateEvidence {
                provider: model.provider.clone(),
                model: key.clone(),
                health: None,
                governor_evaluation: None,
                ranking_tuple: None,
                reason: PlacementReason::CapabilityRejected {
                    detail: "images not supported".to_string(),
                },
            });
            continue;
        }
        if input.requirements.effort.is_some_and(|effort| {
            !provider.wire_protocol().is_openai_chat_completions()
                || !matches!(
                    effort,
                    "none" | "minimal" | "low" | "medium" | "high" | "xhigh"
                )
                || !supports_effort(model, effort)
        }) {
            all_evidence.push(CandidateEvidence {
                provider: model.provider.clone(),
                model: key.clone(),
                health: None,
                governor_evaluation: None,
                ranking_tuple: None,
                reason: PlacementReason::CapabilityRejected {
                    detail: "reasoning effort not supported".to_string(),
                },
            });
            continue;
        }

        compatible = true;
        // Credential readiness can change independently of the Profile. Only
        // structural rejection can be deferred until the Profile changes.
        if executable.is_none() {
            let vault = crate::vault::Vault::user_global()?;
            executable = Some(input.profile.executable_choices(&vault));
        }
        if !executable
            .as_ref()
            .is_some_and(|choices| choices.contains(key))
        {
            continue;
        }

        // Health check
        let identity = ResourceIdentity::for_model(&model.provider, &model.id);
        let record = loaded.registry.resource(&ResourceId::derive(&identity));
        let health_record = record.as_ref().map(|record| &record.health);

        if health_record.is_some_and(|health| health.state == ResourceHealth::Unavailable) {
            let health_evidence = health_record.map(|h| PlacementHealthEvidence {
                state: h.state,
                provenance: h.provenance,
                observed_at: h.observed_at,
            });
            all_evidence.push(CandidateEvidence {
                provider: model.provider.clone(),
                model: key.clone(),
                health: health_evidence,
                governor_evaluation: None,
                ranking_tuple: None,
                reason: PlacementReason::HealthRejected {
                    health_state: "unavailable".to_string(),
                },
            });
            unavailable = true;
            continue;
        }

        let health_filtered =
            health_record.filter(|health| health.provenance != ResourceProvenance::Unknown);
        let health = health_filtered
            .map(|health| health.state)
            .unwrap_or(ResourceHealth::Unknown);

        let health_evidence = health_filtered.map(|h| PlacementHealthEvidence {
            state: h.state,
            provenance: h.provenance,
            observed_at: h.observed_at,
        });

        // Budget check
        let quota = super::budget::quota_facts(input.root, &identity, input.now);
        let assessment = domain.preview_provider_admission(
            input.project_id,
            input.job_id,
            input.spec,
            input.budget,
            quota,
        )?;

        if !assessment.is_allowed() {
            all_evidence.push(CandidateEvidence {
                provider: model.provider.clone(),
                model: key.clone(),
                health: health_evidence,
                governor_evaluation: None,
                ranking_tuple: None,
                reason: PlacementReason::BudgetRejected {
                    reason_code: assessment.reason_code.clone(),
                    decision: serialize_spend_decision(assessment.decision),
                },
            });
            budget_failure.get_or_insert_with(|| {
                job_failure(
                    &assessment.reason_code,
                    FailureClass::Budget,
                    &assessment.reason,
                    assessment.decision == SpendDecision::Defer,
                )
            });
            continue;
        }

        // Governor evaluation (non-consuming)
        let governor_eval = if let Some(governor) = input.governor {
            let scope =
                super::governor::GovernorScope::provider_model(model.provider.clone(), key.clone());
            let eval = evaluate_governor(governor, &scope)?;

            // Reject if governor says unavailable
            if !eval.rate_available {
                all_evidence.push(CandidateEvidence {
                    provider: model.provider.clone(),
                    model: key.clone(),
                    health: health_evidence,
                    governor_evaluation: Some(eval.clone()),
                    ranking_tuple: None,
                    reason: PlacementReason::RateEvaluationUnavailable {
                        retry_after_millis: eval.rate_retry_after_millis,
                    },
                });
                continue;
            }
            if !eval.capacity_available {
                all_evidence.push(CandidateEvidence {
                    provider: model.provider.clone(),
                    model: key.clone(),
                    health: health_evidence,
                    governor_evaluation: Some(eval),
                    ranking_tuple: None,
                    reason: PlacementReason::CapacityEvaluationUnavailable,
                });
                continue;
            }

            Some(eval)
        } else {
            None
        };

        // This candidate is eligible for ranking
        let health_rank = match health {
            ResourceHealth::Available => 0,
            ResourceHealth::Unknown => 1,
            ResourceHealth::Degraded => 2,
            ResourceHealth::Unavailable => continue,
        };

        let model_mismatch = input.preferred_model != Some(key.as_str());
        let provider_mismatch = input.preferred_provider != Some(model.provider.as_str());

        candidates.push((
            health_rank,
            model_mismatch,
            provider_mismatch,
            model.provider.clone(),
            key.clone(),
            health_evidence,
            governor_eval,
        ));
    }

    if candidates.is_empty() {
        return Ok(Err(PlacementRefusal {
            profile_incompatible: !compatible,
            failure: budget_failure.unwrap_or_else(|| {
                if unavailable {
                    job_failure(
                        "placement_unavailable",
                        FailureClass::Provider,
                        "All compatible execution resources are known unavailable",
                        true,
                    )
                } else {
                    job_failure(
                        "placement_incompatible",
                        FailureClass::Capability,
                        "No executable configured Provider/Model satisfies the explicit constraints and runtime capabilities",
                        false,
                    )
                }
            }),
        }));
    }

    // Project capacity check (not per-candidate)
    if let Some(limit) = input.concurrency {
        if !domain.provider_capacity_available(input.project_id, input.job_id, limit)? {
            return Ok(Err(PlacementRefusal {
                profile_incompatible: false,
                failure: job_failure(
                    "placement_capacity",
                    FailureClass::Concurrency,
                    "Project provider capacity is reserved by active Attempts",
                    true,
                ),
            }));
        }
    }

    // Sort by ranking criteria only, then extract winner
    candidates.sort_by_key(|(health, model_mis, prov_mis, provider, model, _, _)| {
        (
            *health,
            *model_mis,
            *prov_mis,
            provider.clone(),
            model.clone(),
        )
    });

    let Some((
        winner_health,
        winner_model_mis,
        winner_prov_mis,
        winner_provider,
        winner_model,
        winner_health_ev,
        winner_gov,
    )) = candidates.into_iter().next()
    else {
        return Ok(Err(PlacementRefusal {
            profile_incompatible: false,
            failure: job_failure(
                "placement_incompatible",
                FailureClass::Capability,
                "No candidate remains",
                false,
            ),
        }));
    };

    all_evidence.retain(|candidate| {
        executable
            .as_ref()
            .is_some_and(|choices| choices.contains(&candidate.model))
    });

    // Record winner as Selected
    all_evidence.push(CandidateEvidence {
        provider: winner_provider.clone(),
        model: winner_model.clone(),
        health: winner_health_ev,
        governor_evaluation: winner_gov,
        ranking_tuple: Some((winner_health, winner_model_mis, winner_prov_mis)),
        reason: PlacementReason::Selected {
            health_rank: winner_health,
            model_preference_mismatch: winner_model_mis,
            provider_preference_mismatch: winner_prov_mis,
        },
    });

    Ok(Ok(PlacementResult {
        choice: ProviderChoice {
            provider: winner_provider,
            model: winner_model,
        },
        evidence: all_evidence,
    }))
}

pub(crate) fn quota(
    root: &Path,
    profile: &Profile,
    choice: &ProviderChoice,
    now: i64,
) -> QuotaFacts {
    profile
        .models
        .get(&choice.model)
        .map(|model| {
            super::budget::quota_facts(
                root,
                &ResourceIdentity::for_model(&choice.provider, &model.id),
                now,
            )
        })
        .unwrap_or_else(QuotaFacts::unknown)
}
