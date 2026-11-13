use super::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AvailabilityObservation {
    pub status_code: Option<u16>,
    pub finish_reason: Option<ChatFinishReason>,
    pub raw_finish_reason: Option<String>,
    pub failure: Option<ProviderFailure>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AvailabilityClassification {
    Available,
    Unavailable(UnavailableCause),
    Rejected(RejectionCause),
    Unconfirmed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnavailableCause {
    ProviderServerError,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RejectionCause {
    RateLimited,
    AuthOrPermission,
    RequestOrTarget,
}

impl AvailabilityObservation {
    pub(in crate::db) fn is_empty(&self) -> bool {
        self.status_code.is_none()
            && self.finish_reason.is_none()
            && self.raw_finish_reason.is_none()
            && self.failure.is_none()
    }

    #[must_use]
    pub fn classification(&self) -> AvailabilityClassification {
        match self.status_code {
            Some(200) if self.normal_termination().is_some() => {
                AvailabilityClassification::Available
            }
            status => classify_status(status),
        }
    }

    fn normal_termination(&self) -> Option<ChatFinishReason> {
        let finish = self.finish_reason?;
        let normal = matches!(finish, ChatFinishReason::Stop | ChatFinishReason::Length);
        (normal
            && self.failure.is_none()
            && self
                .raw_finish_reason
                .as_deref()
                .is_none_or(|raw| raw == finish.as_openai_str()))
        .then_some(finish)
    }
}

pub(in crate::db) fn classify_status(status_code: Option<u16>) -> AvailabilityClassification {
    use AvailabilityClassification::{Rejected, Unavailable, Unconfirmed};
    match status_code {
        Some(500..=599) => Unavailable(UnavailableCause::ProviderServerError),
        Some(429) => Rejected(RejectionCause::RateLimited),
        Some(401 | 403) => Rejected(RejectionCause::AuthOrPermission),
        Some(400 | 404 | 422) => Rejected(RejectionCause::RequestOrTarget),
        Some(_) | None => Unconfirmed,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AvailabilityRequestSample {
    pub job: JobId,
    pub request: RequestNumber,
    pub first_record: AvailabilityStatId,
    pub observed_at: Timestamp,
    pub classification: AvailabilityClassification,
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("evidence mixes {first} and {other}")]
pub struct MixedRequests {
    pub first: RequestRef,
    pub other: RequestRef,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AvailabilityStat {
    pub job: JobId,
    pub request: RequestNumber,
    pub observed_at: Timestamp,
    pub observation: AvailabilityObservation,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecisionTarget {
    pub provider: ConfigRef,
    pub model: ConfigRef,
    pub model_name: String,
    pub provider_name: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AvailabilityStatRecord {
    pub id: AvailabilityStatId,
    pub job: JobId,
    pub request: RequestNumber,
    pub target: Option<LlmTarget>,
    pub decision_target: Option<DecisionTarget>,
    pub observed_at: Timestamp,
    pub observation: AvailabilityObservation,
    pub recorded_at: Timestamp,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimeRange {
    pub from: Timestamp,
    pub until: Timestamp,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObservationFilter {
    pub observed: TimeRange,
    pub llm_profile: Option<ProfileId>,
    pub provider: Option<LlmProviderId>,
    pub model: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ObservationCursor {
    pub observed_at: Timestamp,
    pub job: JobId,
    pub request: RequestNumber,
    pub seq: u32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ObservationPage<T> {
    pub records: Vec<T>,
    pub next: Option<ObservationCursor>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ToolStatRecord {
    pub name: String,
    pub admitted: u64,
    pub refused_arguments: u64,
    pub completed: u64,
    pub failed: u64,
    pub duration_us_total: u64,
    pub duration_us_max: u64,
}
