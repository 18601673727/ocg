//! Request-level Token usage evidence: a pure reducer from Metrics records,
//! plus two facts those records do not store, to one derived sample per
//! request.
//!
//! Nothing here is scheduled or read from the database. The reducer needs, for
//! every record, whether its request was ever issued ([`RequestEligibility`])
//! and whether its counters are cumulative or final ([`UsageFinality`]).
//! Neither can be inferred from a Job's outcome, its Profile, an HTTP status,
//! a finish reason or an availability class. A producer states them in a
//! [`TokenAnnotation`] stored with the Metrics record; a record without one
//! carries no trusted Token contract and is never treated as eligible, excluded
//! or final.
//!
//! The evidence keeps one small record per Token field (a cumulative maximum
//! and a final candidate), never the observation history, so replay in any
//! order, or with duplicates, reaches the same sample.

use super::{JobId, MetricsId, MetricsRecord, MixedRequests, RequestNumber, RequestRef, Timestamp};

/// Whether a request was ever issued to an LLM Provider. It is a trusted
/// input of the caller. It is per request, so the records of one request
/// may not disagree about it; two requests of one Job may.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestEligibility {
    /// The request was issued or may have been delivered. It stays eligible
    /// if it later timed out, was cancelled or has an Unknown Job outcome.
    Eligible,
    /// Known pre-send rejection or local-only work: no request was sent.
    Excluded,
}

/// What a record's counters mean. It is a trusted input of the caller and is
/// per record: every counter the record reports has this meaning.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UsageFinality {
    /// Running totals so far, which a later report may supersede.
    Cumulative,
    /// The request's final counts for each counter the record reports. A
    /// counter the record omits is not reported, not zero and not final.
    Final,
}

/// The Token facts a producer states about one Metrics record, stored with
/// it. They are the producer's trusted knowledge of delivery and of what its
/// counters mean, never derived from a Job's outcome or any response fact.
///
/// An [`Excluded`](RequestEligibility::Excluded) annotation says the request
/// was positively known never to have been sent. It reports no usage and its
/// finality is always [`Cumulative`](UsageFinality::Cumulative), so one fact
/// has one representation; [`TokenAnnotation::excluded`] builds it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TokenAnnotation {
    pub eligibility: RequestEligibility,
    /// What every counter this record reports means.
    pub finality: UsageFinality,
}

impl TokenAnnotation {
    /// The request was positively known not to have been sent.
    #[must_use]
    pub const fn excluded() -> Self {
        Self {
            eligibility: RequestEligibility::Excluded,
            finality: UsageFinality::Cumulative,
        }
    }

    /// The request was issued or may have been delivered, and the record's
    /// counters, if any, have the given finality.
    #[must_use]
    pub const fn eligible(finality: UsageFinality) -> Self {
        Self {
            eligibility: RequestEligibility::Eligible,
            finality,
        }
    }
}

/// One Metrics record with the facts it does not store. The record is only
/// read; its raw values are not copied.
#[derive(Clone, Copy, Debug)]
pub struct TokenObservation<'a> {
    pub metrics: &'a MetricsRecord,
    pub eligibility: RequestEligibility,
    pub finality: UsageFinality,
}

impl<'a> TokenObservation<'a> {
    /// The observation a stored record states; none for a record without a
    /// Token annotation.
    #[must_use]
    pub fn from_record(metrics: &'a MetricsRecord) -> Option<Self> {
        let annotation = metrics.token?;
        Some(Self {
            metrics,
            eligibility: annotation.eligibility,
            finality: annotation.finality,
        })
    }
}

/// Why records could not be reduced to one request.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum TokenEvidenceError {
    #[error("{0}")]
    MixedRequests(MixedRequests),
    /// Records of one request disagree whether it was issued. No delivery
    /// report is preferred over another.
    #[error("records of {0} disagree on whether it was issued")]
    ContradictoryEligibility(RequestRef),
}

/// What is known of one Token counter of a request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TokenCount {
    /// No record reported the counter.
    Missing,
    /// No final value yet: the largest cumulative value reported.
    Provisional(u32),
    /// A final value. It wins over every cumulative value, larger or later.
    Final(u32),
    /// Two different final values were reported. The counter has no value;
    /// neither candidate nor the cumulative maximum stands in for it.
    Conflicted,
}

impl TokenCount {
    /// The count when one is known, final or provisional.
    #[must_use]
    pub fn value(self) -> Option<u32> {
        match self {
            Self::Provisional(count) | Self::Final(count) => Some(count),
            Self::Missing | Self::Conflicted => None,
        }
    }
}

/// How much of a request's core usage (input and output) is established.
///
/// It is separate from the counters' own states: a conflicted counter is
/// visible there, and this only says the core is not yet Complete. Detail
/// counters (cache, reasoning) never affect it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TokenUsageState {
    /// No counter of any kind was reported.
    Missing,
    /// Something was reported, but input and output are not both final.
    /// That covers cumulative-only values, an absent core counter, a core
    /// conflict, and detail counters alone.
    Incomplete,
    /// Input and output both have a final, non-conflicting count.
    Complete,
}

/// The evidence of one counter. Two distinct finals collapse into a conflict
/// that later repetitions cannot clear.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct FieldEvidence {
    pub(crate) cumulative_max: Option<u32>,
    pub(crate) finals: Finals,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum Finals {
    #[default]
    None,
    One(u32),
    Conflicted,
}

impl FieldEvidence {
    fn add(&mut self, value: Option<u32>, finality: UsageFinality) {
        let Some(value) = value else { return };
        match finality {
            UsageFinality::Cumulative => {
                self.cumulative_max = self.cumulative_max.max(Some(value));
            }
            UsageFinality::Final => {
                self.finals = match self.finals {
                    Finals::None => Finals::One(value),
                    Finals::One(first) if first == value => Finals::One(first),
                    Finals::One(_) | Finals::Conflicted => Finals::Conflicted,
                };
            }
        }
    }

    fn count(self) -> TokenCount {
        match (self.finals, self.cumulative_max) {
            (Finals::One(value), _) => TokenCount::Final(value),
            (Finals::Conflicted, _) => TokenCount::Conflicted,
            (Finals::None, Some(value)) => TokenCount::Provisional(value),
            (Finals::None, None) => TokenCount::Missing,
        }
    }
}

/// The claims the observations of one request have made so far. It keeps
/// claims, not observations, so the order and repetition of the feed do not
/// matter.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TokenEvidence {
    request: Option<(RequestRef, RequestEligibility)>,
    /// The lowest Metrics ID of an eligible record, with its observed time.
    /// An eligible record anchors it even when it reports no usage.
    first: Option<(MetricsId, Timestamp)>,
    counters: [FieldEvidence; 5],
}

impl TokenEvidence {
    /// Adds one observation. On an error nothing is added; the feed is then
    /// not one request and the caller must not use the evidence.
    pub fn add(&mut self, observation: TokenObservation<'_>) -> Result<(), TokenEvidenceError> {
        let record = observation.metrics;
        let request = RequestRef {
            job: record.job,
            request: record.request,
        };
        if let Some((first, eligibility)) = self.request {
            if first != request {
                return Err(TokenEvidenceError::MixedRequests(MixedRequests {
                    first,
                    other: request,
                }));
            }
            if eligibility != observation.eligibility {
                return Err(TokenEvidenceError::ContradictoryEligibility(request));
            }
        }
        self.request = Some((request, observation.eligibility));
        if observation.eligibility == RequestEligibility::Excluded {
            return Ok(());
        }

        let anchor = (record.id, record.observed_at);
        self.first = Some(self.first.map_or(anchor, |first| first.min(anchor)));
        let usage = &record.measurements.usage;
        let counters = [
            usage.input_tokens,
            usage.output_tokens,
            usage.cache_read_tokens,
            usage.cache_write_tokens,
            usage.reasoning_tokens,
        ];
        for (evidence, count) in self.counters.iter_mut().zip(counters) {
            evidence.add(count, observation.finality);
        }
        Ok(())
    }

    /// The request's sample; none when no eligible record was added.
    #[must_use]
    pub fn sample(&self) -> Option<TokenRequestSample> {
        let (request, _) = self.request?;
        let (first_metrics, observed_at) = self.first?;
        let counts = self.counters.map(FieldEvidence::count);
        let [input, output, cache_read, cache_write, reasoning] = counts;
        let usage = if counts.iter().all(|count| *count == TokenCount::Missing) {
            TokenUsageState::Missing
        } else if matches!(
            (input, output),
            (TokenCount::Final(_), TokenCount::Final(_))
        ) {
            TokenUsageState::Complete
        } else {
            TokenUsageState::Incomplete
        };
        let total = match (input.value(), output.value()) {
            (Some(input), Some(output)) => Some(u64::from(input) + u64::from(output)),
            _ => None,
        };
        Some(TokenRequestSample {
            job: request.job,
            request: request.request,
            first_metrics,
            observed_at,
            usage,
            input,
            output,
            cache_read,
            cache_write,
            reasoning,
            total,
        })
    }
}

/// The derived view of one eligible Provider request. It is computed, never
/// stored.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TokenRequestSample {
    pub job: JobId,
    pub request: RequestNumber,
    /// The lowest Metrics ID among the request's eligible records.
    pub first_metrics: MetricsId,
    /// `observed_at` of that record: the sample's time. Later or backdated
    /// records of the request do not move it, and neither does `recorded_at`.
    pub observed_at: Timestamp,
    pub usage: TokenUsageState,
    pub input: TokenCount,
    pub output: TokenCount,
    pub cache_read: TokenCount,
    pub cache_write: TokenCount,
    pub reasoning: TokenCount,
    /// Input plus output, exact, when both have a value (final or
    /// provisional); otherwise unknown. Detail counts are not included. A
    /// total from provisional values says nothing about completeness: see
    /// [`usage`](Self::usage).
    pub total: Option<u64>,
}

impl TokenRequestSample {
    /// Reduces the stored Metrics records of one request, in any order.
    /// Records without a Token annotation carry no trusted contract and are
    /// skipped, so they neither make nor exclude a request. Records of
    /// several requests are refused; see [`Self::per_request`].
    pub fn from_metrics<'a>(
        records: impl IntoIterator<Item = &'a MetricsRecord>,
    ) -> Result<Option<Self>, TokenEvidenceError> {
        Self::from_observations(
            records
                .into_iter()
                .filter_map(TokenObservation::from_record),
        )
    }

    /// Reduces the observations of one request, in any order. The result
    /// depends only on the set of observations. Empty input, and input with
    /// no eligible observation, is no sample.
    pub fn from_observations<'a>(
        observations: impl IntoIterator<Item = TokenObservation<'a>>,
    ) -> Result<Option<Self>, TokenEvidenceError> {
        let mut evidence = TokenEvidence::default();
        for observation in observations {
            evidence.add(observation)?;
        }
        Ok(evidence.sample())
    }

    /// Reduces the stored Metrics records of one Job's requests, in any
    /// order, to one sample per eligible request, ordered by request. Each
    /// request is reduced on its own; nothing crosses from one to another.
    pub fn per_request<'a>(
        records: impl IntoIterator<Item = &'a MetricsRecord>,
    ) -> Result<Vec<Self>, TokenEvidenceError> {
        let mut requests: std::collections::BTreeMap<RequestRef, TokenEvidence> =
            std::collections::BTreeMap::new();
        for observation in records
            .into_iter()
            .filter_map(TokenObservation::from_record)
        {
            let request = RequestRef {
                job: observation.metrics.job,
                request: observation.metrics.request,
            };
            if let Some((first, _)) = requests.first_key_value()
                && first.job != request.job
            {
                return Err(TokenEvidenceError::MixedRequests(MixedRequests {
                    first: *first,
                    other: request,
                }));
            }
            requests.entry(request).or_default().add(observation)?;
        }
        Ok(requests
            .values()
            .filter_map(TokenEvidence::sample)
            .collect())
    }

    /// The input plus output of one Job: the sum of its requests' own
    /// totals, known only when every one of them is. No counter is taken
    /// from one request for another, and none is a maximum across them.
    #[must_use]
    pub fn job_total(samples: &[Self]) -> Option<u64> {
        samples
            .iter()
            .try_fold(0_u64, |sum, sample| sum.checked_add(sample.total?))
    }
}

/// The width of one usage summary bucket: one hour. A summary rounds its
/// lower bound down to a bucket and its upper bound up, so the buckets it
/// covers are those an instant in `[from_us, until_us)` falls in.
pub const USAGE_SUMMARY_HOUR_MICROS: i64 = 3_600_000_000;

/// One aggregated row in a token usage summary breakdown. `agent` is 0 for
/// work submitted without an Agent; the route reports it as `<profile-uuid>` or
/// `null`.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UsageSummaryRow {
    pub agent: u128,
    pub provider: u128,
    pub model: Option<String>,
    pub requests: u64,
    pub complete_requests: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_write_tokens: u64,
    pub reasoning_tokens: u64,
}

/// One aggregated row in a usage summary retry breakdown. The grouping
/// matches [`UsageSummaryRow`]: `agent` is 0 for work submitted without an
/// Agent, and `bucket_us` is the requested day or week bucket start. A page
/// can return the same key more than once across pages, so a consumer sums
/// `retries` by `(bucket_us, agent, provider, model)` to obtain the exact
/// count.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UsageRetryRow {
    pub bucket_us: i64,
    pub agent: u128,
    pub provider: u128,
    pub model: Option<String>,
    pub retries: u64,
}

/// A position in the usage totals: the last source row a page examined.
/// Continuing strictly after it reads every remaining row exactly once, even
/// inside one dense hour or chart bucket.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct UsageCursor {
    pub hour: i64,
    pub agent: u128,
    pub provider: u128,
    pub model_hash: [u8; 32],
}

/// The aggregated token usage summary response. `complete` says the scan
/// reached the end of the requested range; `resume_after` is set exactly
/// when it did not. A page can hold partial aggregates for groups that the
/// next page continues, so a consumer merges rows by their group key.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UsageSummaryResponse {
    pub from_us: i64,
    pub until_us: i64,
    pub complete: bool,
    pub resume_after: Option<UsageCursor>,
    pub rows: Vec<UsageSummaryRow>,
}

/// Optional filters for querying usage summaries.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UsageSummaryFilter {
    pub from_us: Option<i64>,
    pub until_us: Option<i64>,
    pub agent: Option<u128>,
    #[serde(default)]
    pub generative_only: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum UsageBucketWidth {
    #[default]
    Day,
    Week,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UsageBucketing {
    pub width: UsageBucketWidth,
    pub offset_minutes: i32,
}

impl UsageBucketing {
    pub fn start_of(self, us: i64) -> Option<i64> {
        const DAY: i64 = 86_400_000_000;
        let offset = i64::from(self.offset_minutes).checked_mul(60_000_000)?;
        let (width, anchor) = match self.width {
            UsageBucketWidth::Day => (DAY, 0),
            UsageBucketWidth::Week => (7 * DAY, 4 * DAY),
        };
        us.checked_add(offset)?
            .checked_sub(anchor)?
            .div_euclid(width)
            .checked_mul(width)?
            .checked_add(anchor)?
            .checked_sub(offset)
    }

    pub fn end_of(self, us: i64) -> Option<i64> {
        let width = match self.width {
            UsageBucketWidth::Day => 86_400_000_000,
            UsageBucketWidth::Week => 7 * 86_400_000_000,
        };
        self.start_of(us)?.checked_add(width)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UsageBucketRow {
    pub bucket_us: i64,
    pub row: UsageSummaryRow,
}

/// One page of chart buckets. As with [`UsageSummaryResponse`], a page can
/// hold partial aggregates and `resume_after` continues strictly after the
/// last source row examined.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UsageBucketResponse {
    pub from_us: i64,
    pub until_us: i64,
    pub complete: bool,
    pub resume_after: Option<UsageCursor>,
    pub rows: Vec<UsageBucketRow>,
}

#[derive(serde::Serialize, ts_rs::TS, Default)]
pub(crate) struct JobUsage {
    pub(in crate::db) requests: u64,
    pub(in crate::db) complete_requests: u64,
    pub(in crate::db) input_tokens: u64,
    pub(in crate::db) output_tokens: u64,
    pub(in crate::db) cache_read_tokens: u64,
    pub(in crate::db) cache_write_tokens: u64,
    pub(in crate::db) reasoning_tokens: u64,
    pub(in crate::db) truncated: bool,
}
