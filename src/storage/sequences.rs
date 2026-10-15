use super::*;

#[derive(Clone, Copy, Debug)]
pub(crate) enum Sequence {
    Job,
    JobInput,
    JobOutput,
    Executor,
    Project,
    Profile,
    Agent,
    LlmProvider,
    Metrics,
    AvailabilityStat,
    Attachment,
}

impl Sequence {
    pub(super) fn key(self) -> &'static str {
        match self {
            Self::Job => "job",
            Self::JobInput => "job_input",
            Self::JobOutput => "job_output",
            Self::Executor => "executor",
            Self::Project => "project",
            Self::Profile => "profile",
            Self::Agent => "agent",
            Self::LlmProvider => "llm_provider",
            Self::Metrics => "metrics",
            Self::AvailabilityStat => "availability_stat",
            Self::Attachment => "attachment",
        }
    }
}

pub(super) const OPEN_EPOCH: &str = "open_epoch";

/// A set of observation records with an index range of its own. Every
/// record is in `All`; one whose Job ran an LLM is also in the four scopes
/// of the target that Job froze.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Scope<'a> {
    All,
    LlmProfile(ProfileId),
    Provider(LlmProviderId),
    Model(&'a str),
    ProviderModel(LlmProviderId, &'a str),
}

impl Scope<'_> {
    /// A tag byte, then the scope's identity: IDs as 8 big-endian bytes and
    /// the model name as its UTF-8 bytes. Each tag's encoding is injective.
    pub(super) fn key(&self) -> Vec<u8> {
        let mut key = Vec::new();
        match self {
            Self::All => key.push(0),
            Self::LlmProfile(profile) => {
                key.push(1);
                key.extend_from_slice(&profile.get().to_be_bytes());
            }
            Self::Provider(provider) => {
                key.push(2);
                key.extend_from_slice(&provider.get().to_be_bytes());
            }
            Self::Model(model) => {
                key.push(3);
                key.extend_from_slice(model.as_bytes());
            }
            Self::ProviderModel(provider, model) => {
                key.push(4);
                key.extend_from_slice(&provider.get().to_be_bytes());
                key.extend_from_slice(model.as_bytes());
            }
        }
        key
    }

    pub(super) fn of(target: Option<&LlmTarget>) -> Vec<Scope<'_>> {
        let mut scopes = vec![Scope::All];
        if let Some(target) = target {
            let (provider, model) = (
                target.frozen.provider.id,
                target.frozen.config.model.as_str(),
            );
            scopes.extend([
                Scope::LlmProfile(target.llm_profile),
                Scope::Provider(provider),
                Scope::Model(model),
                Scope::ProviderModel(provider, model),
            ]);
        }
        scopes
    }

    /// The scope a query scans: the LLM Profile when one is given, since a
    /// Profile's history may span several targets, and otherwise the
    /// narrowest scope the filter names. The filter is still checked
    /// against every record read.
    pub(super) fn for_filter(filter: &ObservationFilter) -> Scope<'_> {
        match (filter.llm_profile, filter.provider, filter.model.as_deref()) {
            (Some(profile), _, _) => Scope::LlmProfile(profile),
            (None, Some(provider), Some(model)) => Scope::ProviderModel(provider, model),
            (None, Some(provider), None) => Scope::Provider(provider),
            (None, None, Some(model)) => Scope::Model(model),
            (None, None, None) => Scope::All,
        }
    }
}

pub(super) fn filter_matches(filter: &ObservationFilter, target: Option<&LlmTarget>) -> bool {
    match target {
        Some(target) => {
            filter
                .llm_profile
                .is_none_or(|profile| profile == target.llm_profile)
                && filter
                    .provider
                    .is_none_or(|provider| provider == target.frozen.provider.id)
                && filter
                    .model
                    .as_deref()
                    .is_none_or(|model| model == target.frozen.config.model)
        }
        None => filter.llm_profile.is_none() && filter.provider.is_none() && filter.model.is_none(),
    }
}
