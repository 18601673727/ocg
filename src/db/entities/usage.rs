use super::{JobId, MetricsId, MetricsRecord, MixedRequests, RequestNumber, RequestRef, Timestamp};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestEligibility {
    Eligible,
    Excluded,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UsageFinality {
    Cumulative,
    Final,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TokenAnnotation {
    pub eligibility: RequestEligibility,
    pub finality: UsageFinality,
}

impl TokenAnnotation {
    #[must_use]
    pub const fn excluded() -> Self {
        Self {
            eligibility: RequestEligibility::Excluded,
            finality: UsageFinality::Cumulative,
        }
    }

    #[must_use]
    pub const fn eligible(finality: UsageFinality) -> Self {
        Self {
            eligibility: RequestEligibility::Eligible,
            finality,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct TokenObservation<'a> {
    pub metrics: &'a MetricsRecord,
    pub eligibility: RequestEligibility,
    pub finality: UsageFinality,
}

impl<'a> TokenObservation<'a> {
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

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum TokenEvidenceError {
    #[error("{0}")]
    MixedRequests(MixedRequests),
    #[error("records of {0} disagree on whether it was issued")]
    ContradictoryEligibility(RequestRef),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TokenCount {
    Missing,
    Provisional(u32),
    Final(u32),
    Conflicted,
}

impl TokenCount {
    #[must_use]
    pub fn value(self) -> Option<u32> {
        match self {
            Self::Provisional(count) | Self::Final(count) => Some(count),
            Self::Missing | Self::Conflicted => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TokenUsageState {
    Missing,
    Incomplete,
    Complete,
}

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

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TokenEvidence {
    request: Option<(RequestRef, RequestEligibility)>,
    first: Option<(MetricsId, Timestamp)>,
    counters: [FieldEvidence; 5],
}

impl TokenEvidence {
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TokenRequestSample {
    pub job: JobId,
    pub request: RequestNumber,
    pub first_metrics: MetricsId,
    pub observed_at: Timestamp,
    pub usage: TokenUsageState,
    pub input: TokenCount,
    pub output: TokenCount,
    pub cache_read: TokenCount,
    pub cache_write: TokenCount,
    pub reasoning: TokenCount,
    pub total: Option<u64>,
}

impl TokenRequestSample {
    pub fn from_metrics<'a>(
        records: impl IntoIterator<Item = &'a MetricsRecord>,
    ) -> Result<Option<Self>, TokenEvidenceError> {
        Self::from_observations(
            records
                .into_iter()
                .filter_map(TokenObservation::from_record),
        )
    }

    pub fn from_observations<'a>(
        observations: impl IntoIterator<Item = TokenObservation<'a>>,
    ) -> Result<Option<Self>, TokenEvidenceError> {
        let mut evidence = TokenEvidence::default();
        for observation in observations {
            evidence.add(observation)?;
        }
        Ok(evidence.sample())
    }

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
}

pub const USAGE_SUMMARY_HOUR_MICROS: i64 = 3_600_000_000;

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

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UsageRetryRow {
    pub bucket_us: i64,
    pub agent: u128,
    pub provider: u128,
    pub model: Option<String>,
    pub retries: u64,
}

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct UsageCursor {
    pub hour: i64,
    pub agent: u128,
    pub provider: u128,
    pub model_hash: [u8; 32],
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UsageSummaryResponse {
    pub from_us: i64,
    pub until_us: i64,
    pub complete: bool,
    pub resume_after: Option<UsageCursor>,
    pub rows: Vec<UsageSummaryRow>,
}

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

impl JobUsage {
    pub(in crate::db) fn add_sample(&mut self, sample: &TokenRequestSample) {
        self.requests = self.requests.saturating_add(1);
        self.complete_requests = self
            .complete_requests
            .saturating_add(u64::from(sample.usage == TokenUsageState::Complete));
        self.input_tokens = self
            .input_tokens
            .saturating_add(u64::from(sample.input.value().unwrap_or(0)));
        self.output_tokens = self
            .output_tokens
            .saturating_add(u64::from(sample.output.value().unwrap_or(0)));
        self.cache_read_tokens = self
            .cache_read_tokens
            .saturating_add(u64::from(sample.cache_read.value().unwrap_or(0)));
        self.cache_write_tokens = self
            .cache_write_tokens
            .saturating_add(u64::from(sample.cache_write.value().unwrap_or(0)));
        self.reasoning_tokens = self
            .reasoning_tokens
            .saturating_add(u64::from(sample.reasoning.value().unwrap_or(0)));
    }
}
