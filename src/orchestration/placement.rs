use crate::core_contract::{Failure, FailureClass};
use crate::error::Result;
use crate::profile::{Model, Profile};
use crate::resources::{ResourceHealth, ResourceId, ResourceIdentity, ResourceProvenance};
use serde::{Deserialize, Serialize};
use std::path::Path;

use super::budget::{BudgetConfig, QuotaFacts, SpendDecision};
use super::domain::{job_failure, DomainRepository, JobSpec};

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
    pub now: i64,
}

// This decision is transient. Job/Attempt/DispatchIntent remain the durable
// authorities; a replay reads the same configuration and persisted facts.
pub(crate) fn choose(
    domain: &DomainRepository,
    input: PolicyInput<'_>,
) -> Result<std::result::Result<ProviderChoice, Failure>> {
    let vault = crate::vault::Vault::user_global()?;
    let executable = input.profile.executable_choices(&vault);
    let loaded = crate::resources::load(input.root);
    if loaded.corrupt || !loaded.issues.is_empty() {
        return Ok(Err(job_failure(
            "placement_health_unknown",
            FailureClass::Provider,
            "Resource Registry contains unreadable availability facts",
            true,
        )));
    }
    let mut candidates = Vec::new();
    let mut unavailable = false;
    let mut budget_failure = None;
    for (key, model) in input.profile.runnable_models() {
        if !executable.contains(key)
            || input
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
        // The existing provider loop always supplies native tools. Unknown
        // capability metadata stays unknown; only an explicit denial excludes it.
        if tools == Some(false)
            || (input.requirements.images
                && (images == Some(false) || (images.is_none() && multimodal == Some(false))))
            || input.requirements.effort.is_some_and(|effort| {
                !provider.wire_protocol().is_openai_chat_completions()
                    || !matches!(
                        effort,
                        "none" | "minimal" | "low" | "medium" | "high" | "xhigh"
                    )
                    || !supports_effort(model, effort)
            })
        {
            continue;
        }
        let identity = ResourceIdentity::for_model(&model.provider, &model.id);
        let record = loaded.registry.resource(&ResourceId::derive(&identity));
        let health = record.as_ref().map(|record| &record.health);
        if health.is_some_and(|health| health.state == ResourceHealth::Unavailable) {
            unavailable = true;
            continue;
        }
        let health = health.filter(|health| health.provenance != ResourceProvenance::Unknown);
        // Rank the last persisted observation, not an invented live probe or
        // clock-dependent score. Missing evidence never becomes Available.
        let health = health
            .map(|health| health.state)
            .unwrap_or(ResourceHealth::Unknown);
        let quota = super::budget::quota_facts(input.root, &identity, input.now);
        let assessment = domain.preview_provider_admission(
            input.project_id,
            input.job_id,
            input.spec,
            input.budget,
            quota,
        )?;
        if !assessment.is_allowed() {
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
        let health_rank = match health {
            ResourceHealth::Available => 0,
            ResourceHealth::Unknown => 1,
            ResourceHealth::Degraded => 2,
            ResourceHealth::Unavailable => continue,
        };
        candidates.push((
            health_rank,
            input.preferred_model != Some(key.as_str()),
            input.preferred_provider != Some(model.provider.as_str()),
            model.provider.clone(),
            key.clone(),
        ));
    }
    if candidates.is_empty() {
        return Ok(Err(budget_failure.unwrap_or_else(|| {
            if unavailable {
                job_failure("placement_unavailable", FailureClass::Provider,
                    "All compatible execution resources are known unavailable", true)
            } else {
                job_failure("placement_incompatible", FailureClass::Capability,
                    "No executable configured Provider/Model satisfies the explicit constraints and runtime capabilities", false)
            }
        })));
    }
    if let Some(limit) = input.concurrency {
        if !domain.provider_capacity_available(input.project_id, input.job_id, limit)? {
            return Ok(Err(job_failure(
                "placement_capacity",
                FailureClass::Concurrency,
                "Project provider capacity is reserved by active Attempts",
                true,
            )));
        }
    }
    candidates.sort();
    let Some((_, _, _, provider, model)) = candidates.into_iter().next() else {
        return Ok(Err(job_failure(
            "placement_incompatible",
            FailureClass::Capability,
            "No candidate remains",
            false,
        )));
    };
    Ok(Ok(ProviderChoice { provider, model }))
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
