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

impl AvailabilityRequestSample {
    pub fn from_records<'a>(
        records: impl IntoIterator<Item = &'a AvailabilityStatRecord>,
    ) -> Result<Option<Self>, MixedRequests> {
        let mut evidence = RequestEvidence::default();
        for record in records {
            evidence.add(record)?;
        }
        Ok(evidence.sample())
    }
}

#[derive(Clone, Default)]
pub(crate) struct RequestEvidence {
    pub(crate) request: Option<RequestRef>,
    pub(crate) first: Option<(AvailabilityStatId, Timestamp)>,
    pub(crate) observed_status: Option<u16>,
    pub(crate) supplied_status: Option<u16>,
    pub(crate) finish: Option<ChatFinishReason>,
    pub(crate) witness: bool,
    pub(crate) contradictory: bool,
}

impl RequestEvidence {
    pub(crate) fn add(&mut self, record: &AvailabilityStatRecord) -> Result<(), MixedRequests> {
        let request = RequestRef {
            job: record.job,
            request: record.request,
        };
        match self.request {
            Some(first) if first != request => {
                return Err(MixedRequests {
                    first,
                    other: request,
                });
            }
            _ => self.request = Some(request),
        }
        self.first = match self.first {
            Some((id, observed_at)) if id <= record.id => Some((id, observed_at)),
            _ => Some((record.id, record.observed_at)),
        };

        let observation = &record.observation;
        self.witness |= observation.normal_termination().is_some();
        if let Some(status) = observation.status_code {
            self.observed_status.get_or_insert(status);
        }
        let failure_status = observation
            .failure
            .as_ref()
            .and_then(|failure| failure.status_code);
        for status in observation.status_code.into_iter().chain(failure_status) {
            match self.supplied_status {
                Some(first) => self.contradictory |= first != status,
                None => self.supplied_status = Some(status),
            }
        }
        if let Some(finish) = observation.finish_reason {
            match self.finish {
                Some(first) => self.contradictory |= first != finish,
                None => self.finish = Some(finish),
            }
            if let Some(raw) = observation.raw_finish_reason.as_deref() {
                self.contradictory |= ChatFinishReason::from_openai_str(raw) != finish;
            }
        }
        Ok(())
    }

    pub(crate) fn sample(&self) -> Option<AvailabilityRequestSample> {
        let (request, (first_record, observed_at)) = (self.request?, self.first?);
        let classification = if self.contradictory {
            AvailabilityClassification::Unconfirmed
        } else if self.witness && self.observed_status == Some(200) {
            AvailabilityClassification::Available
        } else {
            classify_status(self.observed_status)
        };
        Some(AvailabilityRequestSample {
            job: request.job,
            request: request.request,
            first_record,
            observed_at,
            classification,
        })
    }
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
