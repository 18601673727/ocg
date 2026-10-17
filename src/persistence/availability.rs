use super::*;

/// The facts observed about one request that was sent to an LLM Provider.
/// A request refused locally before it was sent has no observation. These
/// are raw facts; [`AvailabilityObservation::classification`] derives their
/// availability meaning on demand, and nothing is stored.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AvailabilityObservation {
    /// The HTTP status the Provider answered with, when one was seen.
    pub status_code: Option<u16>,
    /// How the response ended, when it reported a finish reason.
    pub finish_reason: Option<ChatFinishReason>,
    pub raw_finish_reason: Option<String>,
    /// The Provider or transport failure as the transport reported it. It
    /// may accompany a response that had already begun.
    pub failure: Option<ProviderFailure>,
}

/// The availability meaning of one observation, derived from its structured
/// facts. It is a view, never stored, and never changes the observation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AvailabilityClassification {
    /// HTTP 200 whose stream reached a normal terminal finish reason.
    Available,
    /// Provider evidence of unavailability.
    Unavailable(UnavailableCause),
    /// A definite refusal. It says nothing about availability and is kept
    /// apart from the availability denominator.
    Rejected(RejectionCause),
    /// No valid terminal evidence, or evidence this classification does not
    /// support. Reportable, but neither success nor failure.
    Unconfirmed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnavailableCause {
    /// The Provider answered with an HTTP 5xx status.
    ProviderServerError,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RejectionCause {
    /// HTTP 429.
    RateLimited,
    /// HTTP 401 or 403.
    AuthOrPermission,
    /// HTTP 400, 404 or 422.
    RequestOrTarget,
}

impl AvailabilityObservation {
    pub(super) fn is_empty(&self) -> bool {
        self.status_code.is_none()
            && self.finish_reason.is_none()
            && self.raw_finish_reason.is_none()
            && self.failure.is_none()
    }

    /// Classifies this observation from its observed HTTP status and finish
    /// reason. The failure text never decides a class: a failure without an
    /// observed status is Unconfirmed, because a timeout, reset or incomplete
    /// exchange does not prove that the Provider was unavailable.
    #[must_use]
    pub fn classification(&self) -> AvailabilityClassification {
        match self.status_code {
            Some(200) if self.normal_termination().is_some() => {
                AvailabilityClassification::Available
            }
            status => classify_status(status),
        }
    }

    /// The normal terminal finish reason this observation witnesses by
    /// itself, whatever its HTTP status: Stop or Length, with no failure
    /// beside it and no raw reason that names another finish reason. Missing
    /// usage is not a contradiction.
    fn normal_termination(&self) -> Option<ChatFinishReason> {
        let finish = self.finish_reason?;
        let normal = matches!(finish, ChatFinishReason::Stop | ChatFinishReason::Length);
        // A failure beside a finish reason is conflicting evidence, and a raw
        // reason that names a different finish reason contradicts the typed one.
        (normal
            && self.failure.is_none()
            && self
                .raw_finish_reason
                .as_deref()
                .is_none_or(|raw| raw == finish.as_openai_str()))
        .then_some(finish)
    }
}

/// The class of an HTTP status alone. Only the statuses approved for a class
/// have one; every other status, including 200 without a normal finish, is
/// Unconfirmed.
pub(super) fn classify_status(status_code: Option<u16>) -> AvailabilityClassification {
    use AvailabilityClassification::{Rejected, Unavailable, Unconfirmed};
    match status_code {
        Some(500..=599) => Unavailable(UnavailableCause::ProviderServerError),
        Some(429) => Rejected(RejectionCause::RateLimited),
        Some(401 | 403) => Rejected(RejectionCause::AuthOrPermission),
        Some(400 | 404 | 422) => Rejected(RejectionCause::RequestOrTarget),
        Some(_) | None => Unconfirmed,
    }
}

/// One request sample: the evidence of one Provider request, reduced to one
/// classification. It is derived on demand from committed AvailabilityStat
/// records. It is never stored, and no record is changed. Several records of
/// one request add facts, not requests; each request of a Job is a sample of
/// its own.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AvailabilityRequestSample {
    pub job: JobId,
    pub request: RequestNumber,
    /// The lowest committed AvailabilityStat ID of the request.
    pub first_record: AvailabilityStatId,
    /// `observed_at` of `first_record`. Later records never move it.
    pub observed_at: Timestamp,
    pub classification: AvailabilityClassification,
}

/// Records of two different requests, of one Job or of two, were given to
/// the reduction of one request.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("evidence mixes {first} and {other}")]
pub struct MixedRequests {
    pub first: RequestRef,
    pub other: RequestRef,
}

impl AvailabilityRequestSample {
    /// Reduces the records of one request, in any order. The result depends
    /// only on the set of records, so a replay yields the same sample. Empty
    /// input is no sample.
    ///
    /// Explicit contradictions always make the sample Unconfirmed. They are:
    /// two different supplied HTTP statuses, where a failure's status counts
    /// as supplied, so that an absent status conflicts with nothing; two
    /// records, or one record, observed different finish reasons; a record
    /// whose raw finish reason names a different finish reason than its typed
    /// one; and a record whose failure carries an HTTP status that differs
    /// from its own observation's status.
    ///
    /// Otherwise the sample is Available when its observed HTTP status is 200
    /// and some record witnesses a normal termination on its own. The status
    /// and the termination may come from different records; a record with no
    /// status, or no finish, does not erase the other. A failure without a
    /// status, a missing field, or an Unconfirmed record is not a contradiction
    /// and is never a witness. Without that combination the observed HTTP
    /// status decides, as for a single observation. A failure status never
    /// classifies, and neither does a raw finish reason without a typed one.
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

/// The structured claims that the records of one request have made so far.
/// It keeps claims, not records, so replay in any order reaches the same state.
#[derive(Clone, Default)]
pub(crate) struct RequestEvidence {
    pub(crate) request: Option<RequestRef>,
    /// The lowest record ID seen, with its observed time.
    pub(crate) first: Option<(AvailabilityStatId, Timestamp)>,
    /// The first HTTP status a record observed. Only an observed status
    /// classifies the request.
    pub(crate) observed_status: Option<u16>,
    /// The first HTTP status any record supplied, observed or on a failure.
    pub(crate) supplied_status: Option<u16>,
    /// The first finish reason observed.
    pub(crate) finish: Option<ChatFinishReason>,
    /// Some record witnesses a normal termination by itself.
    pub(crate) witness: bool,
    /// An explicit contradiction has been seen; see `from_records`.
    pub(crate) contradictory: bool,
}

impl RequestEvidence {
    /// Adds one record's facts. The reducer and incremental aggregation both
    /// use it, so their classifications cannot differ.
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
        // A missing status is absence, so only supplied statuses are compared.
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

/// Microseconds in one aggregation minute.
pub(super) const MICROS_PER_MINUTE: i64 = 60_000_000;

/// The one-minute bucket `at` falls in: whole minutes since the Unix epoch,
/// so a negative time has a negative bucket. A sample's bucket is fixed by
/// its first record's `observed_at`.
#[must_use]
pub fn minute_of(at: Timestamp) -> i64 {
    at.as_unix_micros().div_euclid(MICROS_PER_MINUTE)
}

/// Requests per classification in one (Provider, model, minute) bucket.
/// Every count is of Provider requests, each counted once whichever Job sent
/// it, never of raw observations.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AvailabilityCounts {
    pub available: u64,
    pub provider_server_error: u64,
    pub rate_limited: u64,
    pub auth_or_permission: u64,
    pub request_or_target: u64,
    pub unconfirmed: u64,
}

impl AvailabilityCounts {
    /// The counter a classification is counted in.
    pub(crate) fn counter(&mut self, class: AvailabilityClassification) -> &mut u64 {
        use AvailabilityClassification::{Available, Rejected, Unavailable, Unconfirmed};
        match class {
            Available => &mut self.available,
            Unavailable(UnavailableCause::ProviderServerError) => &mut self.provider_server_error,
            Rejected(RejectionCause::RateLimited) => &mut self.rate_limited,
            Rejected(RejectionCause::AuthOrPermission) => &mut self.auth_or_permission,
            Rejected(RejectionCause::RequestOrTarget) => &mut self.request_or_target,
            Unconfirmed => &mut self.unconfirmed,
        }
    }
}

/// One request sample as materialized: its Job's frozen target and the
/// sample it reduces to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestSampleRecord {
    pub job: JobId,
    pub request: RequestNumber,
    pub llm_profile: ProfileId,
    pub provider: LlmProviderId,
    pub model: String,
    pub sample: AvailabilityRequestSample,
}

/// How far aggregation has consumed AvailabilityStat records.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AvailabilityAggregationProgress {
    /// The last committed AvailabilityStat ID consumed. `None` before any.
    pub consumed_through: Option<AvailabilityStatId>,
    /// Wall-clock time of the last committed batch. It is activity metadata,
    /// not progress, and it covers no observed time.
    pub updated_at: Option<Timestamp>,
}

/// A read together with the aggregation progress of the same snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadSnapshot<T> {
    pub progress: AvailabilityAggregationProgress,
    pub value: T,
}

/// What one committed aggregation batch consumed and wrote.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AvailabilityBatchReport {
    pub consumed_records: usize,
    pub consumed_through: AvailabilityStatId,
    /// Request samples whose stored state changed.
    pub samples_written: usize,
    /// Minute buckets whose stored counts changed.
    pub buckets_written: usize,
    /// Preparations this call made, including the one that committed.
    pub attempts: u8,
}

/// The result of one bounded aggregation step.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AvailabilityBatch {
    /// No committed record was unconsumed, so nothing was written.
    Idle {
        consumed_through: Option<AvailabilityStatId>,
    },
    Consumed(AvailabilityBatchReport),
    /// Every preparation was overtaken by another commit, so none was applied.
    Contended {
        attempts: u8,
    },
}

/// One availability observation of an LLM request made by a Job. The LLM it
/// concerns is the one the Job froze at submission, never a caller-supplied
/// target. The Job's outcome is neither consulted nor changed, so an
/// observation may be recorded, and stays, whatever the Job's outcome.
#[derive(Clone, Debug, PartialEq)]
pub struct NewAvailabilityStat {
    pub job: JobId,
    /// The Provider request of the Job the record observes.
    pub request: RequestNumber,
    /// When the request was observed, as its producer reports it.
    /// Observation queries filter and order by this time.
    pub observed_at: Timestamp,
    pub observation: AvailabilityObservation,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AvailabilityStatRecord {
    pub id: AvailabilityStatId,
    /// Assembled from OBSERVES.
    pub job: JobId,
    /// See [`NewAvailabilityStat::request`].
    pub request: RequestNumber,
    /// Read from the Job's frozen input.
    pub target: LlmTarget,
    /// The producer's time, see [`NewAvailabilityStat::observed_at`].
    pub observed_at: Timestamp,
    pub observation: AvailabilityObservation,
    /// The wall-clock time at which the database's write transaction that
    /// stored the record began, not when its commit completed. Wall-clock
    /// time is not monotonic. It is never used in place of `observed_at`.
    pub recorded_at: Timestamp,
}

/// A half-open time range, `from` included and `until` excluded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimeRange {
    pub from: Timestamp,
    pub until: Timestamp,
}

/// Selects Metrics or AvailabilityStats. Each `Some` field must match; a
/// record without an LLM matches only when every LLM field is `None`.
/// Provider and model are compared with the target its Job froze, never with
/// the current contents of any Profile, so records from before a Profile
/// edit stay selectable by what they actually ran. Choosing records this way
/// decides nothing about grouping, current validity or availability.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObservationFilter {
    /// Compared with the producer's observation time; the time a record was
    /// committed cannot be filtered on.
    pub observed: TimeRange,
    pub llm_profile: Option<ProfileId>,
    pub provider: Option<LlmProviderId>,
    /// Compared exactly with the frozen model name.
    pub model: Option<String>,
}

/// A position in observation order, which is `observed_at` and then record
/// ID, so records with equal times keep a stable order. A page continues
/// strictly after it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObservationCursor<I> {
    pub observed_at: Timestamp,
    pub record: I,
}

/// One page of an observation query. `next` is `None` once nothing in the
/// filter's time range remains after this page. A page may be empty while
/// `next` is set: the bounded scan behind it found no match, and the query
/// continues from `next`.
#[derive(Clone, Debug, PartialEq)]
pub struct ObservationPage<T, I> {
    pub records: Vec<T>,
    pub next: Option<ObservationCursor<I>>,
}

/// Selects historical (Provider, model) choices: the frozen targets of LLM
/// Metrics and AvailabilityStats observed within `observed`, which is
/// compared with the producer's observation time like
/// [`ObservationFilter::observed`]. A configured model nothing observed has
/// no choice here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObservedModelFilter {
    pub observed: TimeRange,
    pub provider: Option<LlmProviderId>,
}

/// A Provider and an exact, case-sensitive model name that observations
/// were recorded against, as their Jobs froze them. Choices are ordered by
/// Provider ID and then by the model name's UTF-8 bytes, and a choice is
/// also the cursor that continues strictly after it.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObservedModel {
    pub provider: LlmProviderId,
    pub model: String,
}

/// Which records within the time range a choice comes from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObservedModelChoice {
    pub model: ObservedModel,
    pub has_metrics: bool,
    pub has_availability_stats: bool,
}

/// One page of choices. `next` is `None` once no later pair exists; a page
/// may be empty while `next` is set, when every pair it examined had no
/// record within the time range.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObservedModelPage {
    pub choices: Vec<ObservedModelChoice>,
    pub next: Option<ObservedModel>,
}
