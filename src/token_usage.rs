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

use crate::persistence::{
    JobId, MetricsId, MetricsRecord, MixedRequests, RequestNumber, RequestRef, Timestamp,
};

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
    request: Option<RequestRef>,
    eligibility: Option<RequestEligibility>,
    /// The lowest Metrics ID of an eligible record, with its observed time.
    /// An eligible record anchors it even when it reports no usage.
    first: Option<(MetricsId, Timestamp)>,
    input: FieldEvidence,
    output: FieldEvidence,
    cache_read: FieldEvidence,
    cache_write: FieldEvidence,
    reasoning: FieldEvidence,
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
        if let Some(first) = self.request
            && first != request
        {
            return Err(TokenEvidenceError::MixedRequests(MixedRequests {
                first,
                other: request,
            }));
        }
        if self
            .eligibility
            .is_some_and(|known| known != observation.eligibility)
        {
            return Err(TokenEvidenceError::ContradictoryEligibility(request));
        }
        self.request = Some(request);
        self.eligibility = Some(observation.eligibility);
        if observation.eligibility == RequestEligibility::Excluded {
            return Ok(());
        }

        let anchor = (record.id, record.observed_at);
        self.first = Some(self.first.map_or(anchor, |first| first.min(anchor)));
        let usage = &record.measurements.usage;
        let finality = observation.finality;
        self.input.add(usage.input_tokens, finality);
        self.output.add(usage.output_tokens, finality);
        self.cache_read.add(usage.cache_read_tokens, finality);
        self.cache_write.add(usage.cache_write_tokens, finality);
        self.reasoning.add(usage.reasoning_tokens, finality);
        Ok(())
    }

    /// The request's sample; none when no eligible record was added.
    #[must_use]
    pub fn sample(&self) -> Option<TokenRequestSample> {
        let (request, (first_metrics, observed_at)) = (self.request?, self.first?);
        let counts = [
            self.input.count(),
            self.output.count(),
            self.cache_read.count(),
            self.cache_write.count(),
            self.reasoning.count(),
        ];
        let usage = if counts.iter().all(|count| *count == TokenCount::Missing) {
            TokenUsageState::Missing
        } else if matches!(
            (counts[0], counts[1]),
            (TokenCount::Final(_), TokenCount::Final(_))
        ) {
            TokenUsageState::Complete
        } else {
            TokenUsageState::Incomplete
        };
        let total = match (counts[0].value(), counts[1].value()) {
            (Some(input), Some(output)) => Some(u64::from(input) + u64::from(output)),
            _ => None,
        };
        Some(TokenRequestSample {
            job: request.job,
            request: request.request,
            first_metrics,
            observed_at,
            usage,
            input: counts[0],
            output: counts[1],
            cache_read: counts[2],
            cache_write: counts[3],
            reasoning: counts[4],
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

#[cfg(test)]
mod tests {
    use super::RequestEligibility::{Eligible, Excluded};
    use super::UsageFinality::{Cumulative, Final};
    use super::*;
    use crate::openai_compatible::NormalizedUsage;
    use crate::persistence::Measurements;

    const JOB: JobId = JobId::new(7);

    /// [input, output, cache read, cache write, reasoning]
    type Counters = [Option<u32>; 5];
    const NONE: Counters = [None; 5];

    fn metrics(id: u64, job: JobId, observed_micros: i64, counters: Counters) -> MetricsRecord {
        let [input, output, cache_read, cache_write, reasoning] = counters;
        MetricsRecord {
            id: MetricsId::new(id),
            job,
            request: RequestNumber::FIRST,
            target: None,
            observed_at: Timestamp::from_unix_micros(observed_micros),
            measurements: Measurements {
                usage: NormalizedUsage {
                    input_tokens: input,
                    output_tokens: output,
                    cache_read_tokens: cache_read,
                    cache_write_tokens: cache_write,
                    reasoning_tokens: reasoning,
                    raw: None,
                },
                ..Measurements::default()
            },
            token: None,
            // Never the sample's time, whatever it holds.
            recorded_at: Timestamp::from_unix_micros(i64::MAX),
        }
    }

    fn io(input: u32, output: u32) -> Counters {
        [Some(input), Some(output), None, None, None]
    }

    fn reduce(
        records: &[(&MetricsRecord, RequestEligibility, UsageFinality)],
    ) -> Result<Option<TokenRequestSample>, TokenEvidenceError> {
        TokenRequestSample::from_observations(records.iter().map(
            |&(metrics, eligibility, finality)| TokenObservation {
                metrics,
                eligibility,
                finality,
            },
        ))
    }

    fn sample(
        records: &[(&MetricsRecord, RequestEligibility, UsageFinality)],
    ) -> TokenRequestSample {
        reduce(records).expect("one Job").expect("a sample")
    }

    /// Every ordering of the records reduces to the same sample, and so does
    /// feeding the whole set twice.
    fn assert_order_and_repetition_invariant(
        records: &[(&MetricsRecord, RequestEligibility, UsageFinality)],
    ) -> TokenRequestSample {
        fn permute<T: Copy>(items: &mut Vec<T>, start: usize, visit: &mut impl FnMut(&[T])) {
            if start == items.len() {
                visit(items);
                return;
            }
            for next in start..items.len() {
                items.swap(start, next);
                permute(items, start + 1, visit);
                items.swap(start, next);
            }
        }
        let expected = sample(records);
        let mut items = records.to_vec();
        permute(&mut items, 0, &mut |order| {
            assert_eq!(sample(order), expected, "feed order must not matter");
            let twice: Vec<_> = order.iter().chain(order).copied().collect();
            assert_eq!(sample(&twice), expected, "a repeated feed must not matter");
        });
        expected
    }

    #[test]
    fn excluded_work_and_empty_input_make_no_sample() {
        assert_eq!(reduce(&[]), Ok(None));
        let local = metrics(1, JOB, 100, io(10, 20));
        assert_eq!(
            reduce(&[(&local, Excluded, Final), (&local, Excluded, Cumulative)]),
            Ok(None),
            "a request that was never sent has no Token sample, whatever it reports"
        );
    }

    #[test]
    fn an_eligible_record_without_usage_is_a_missing_usage_sample_with_a_time() {
        let silent = metrics(4, JOB, 400, NONE);
        let found = sample(&[(&silent, Eligible, Final)]);
        assert_eq!(
            (found.job, found.first_metrics, found.observed_at),
            (JOB, MetricsId::new(4), Timestamp::from_unix_micros(400))
        );
        assert_eq!(found.usage, TokenUsageState::Missing);
        assert_eq!(found.total, None);
        assert_eq!(found.input, TokenCount::Missing);
    }

    #[test]
    fn records_of_different_jobs_are_not_one_request() {
        let (a, b) = (
            metrics(1, JobId::new(1), 0, io(1, 1)),
            metrics(2, JobId::new(2), 0, io(1, 1)),
        );
        for records in [
            [(&a, Eligible, Final), (&b, Eligible, Final)],
            [(&b, Eligible, Final), (&a, Eligible, Final)],
        ] {
            assert!(matches!(
                reduce(&records),
                Err(TokenEvidenceError::MixedRequests(_))
            ));
        }
        let excluded = metrics(3, JobId::new(2), 0, NONE);
        assert!(
            matches!(
                reduce(&[(&a, Eligible, Final), (&excluded, Excluded, Final)]),
                Err(TokenEvidenceError::MixedRequests(_))
            ),
            "excluded records still belong to a Job"
        );
    }

    #[test]
    fn disagreeing_delivery_reports_are_refused_in_either_order() {
        let (sent, local) = (metrics(1, JOB, 0, io(1, 1)), metrics(2, JOB, 0, io(1, 1)));
        for records in [
            [(&sent, Eligible, Final), (&local, Excluded, Final)],
            [(&local, Excluded, Final), (&sent, Eligible, Final)],
        ] {
            assert_eq!(
                reduce(&records),
                Err(TokenEvidenceError::ContradictoryEligibility(RequestRef {
                    job: JOB,
                    request: RequestNumber::FIRST
                }))
            );
        }
    }

    #[test]
    fn a_final_value_replaces_the_cumulative_ones_and_nothing_is_summed() {
        let (a, b, c) = (
            metrics(1, JOB, 100, io(10, 10)),
            metrics(2, JOB, 200, io(25, 25)),
            metrics(3, JOB, 300, io(40, 40)),
        );
        let found = assert_order_and_repetition_invariant(&[
            (&a, Eligible, Cumulative),
            (&b, Eligible, Cumulative),
            (&c, Eligible, Final),
        ]);
        assert_eq!(
            (found.input, found.output, found.total),
            (TokenCount::Final(40), TokenCount::Final(40), Some(80)),
            "cumulative 10, 25 then final 40 is 40, never 75"
        );
        assert_eq!(found.usage, TokenUsageState::Complete);
    }

    #[test]
    fn without_a_final_the_largest_cumulative_value_is_provisional() {
        let (a, b) = (
            metrics(1, JOB, 100, io(25, 5)),
            metrics(2, JOB, 200, io(10, 9)),
        );
        let found = assert_order_and_repetition_invariant(&[
            (&a, Eligible, Cumulative),
            (&b, Eligible, Cumulative),
        ]);
        assert_eq!(
            (found.input, found.output, found.total),
            (
                TokenCount::Provisional(25),
                TokenCount::Provisional(9),
                Some(34)
            ),
            "each counter takes its own maximum"
        );
        assert_eq!(found.usage, TokenUsageState::Incomplete);
    }

    #[test]
    fn a_final_wins_over_larger_or_later_intermediate_values() {
        let (early, last, larger_later, final_lower) = (
            metrics(1, JOB, 100, io(90, 90)),
            metrics(2, JOB, 200, io(30, 30)),
            metrics(3, JOB, 300, io(500, 500)),
            metrics(4, JOB, 50, io(30, 30)),
        );
        let found = assert_order_and_repetition_invariant(&[
            (&early, Eligible, Cumulative),
            (&last, Eligible, Final),
            (&larger_later, Eligible, Cumulative),
        ]);
        assert_eq!(
            (found.input, found.output),
            (TokenCount::Final(30), TokenCount::Final(30)),
            "a final below the cumulative maximum is still the final value"
        );
        assert_eq!(
            sample(&[
                (&final_lower, Eligible, Final),
                (&early, Eligible, Cumulative)
            ])
            .input,
            TokenCount::Final(30)
        );
    }

    #[test]
    fn a_missing_field_keeps_the_evidence_of_the_others() {
        let (input_only, output_only) = (
            metrics(1, JOB, 100, [Some(12), None, None, None, None]),
            metrics(2, JOB, 200, [None, Some(8), None, None, None]),
        );
        let found = assert_order_and_repetition_invariant(&[
            (&input_only, Eligible, Cumulative),
            (&output_only, Eligible, Cumulative),
        ]);
        assert_eq!(
            (found.input, found.output, found.cache_read),
            (
                TokenCount::Provisional(12),
                TokenCount::Provisional(8),
                TokenCount::Missing
            )
        );
    }

    #[test]
    fn a_final_that_omits_a_core_field_does_not_promote_its_earlier_value() {
        let (cumulative, partial_final, empty_final) = (
            metrics(1, JOB, 100, io(40, 60)),
            metrics(2, JOB, 200, [Some(40), None, None, None, None]),
            metrics(3, JOB, 300, NONE),
        );
        let found = assert_order_and_repetition_invariant(&[
            (&cumulative, Eligible, Cumulative),
            (&partial_final, Eligible, Final),
            (&empty_final, Eligible, Final),
        ]);
        assert_eq!(found.input, TokenCount::Final(40));
        assert_eq!(
            found.output,
            TokenCount::Provisional(60),
            "output stays provisional"
        );
        assert_eq!(found.usage, TokenUsageState::Incomplete);
        assert_eq!(
            found.total,
            Some(100),
            "a provisional total does not make the usage Complete"
        );
    }

    #[test]
    fn compatible_finals_in_separate_observations_complete_the_core_usage() {
        let (input, output) = (
            metrics(1, JOB, 100, [Some(40), None, None, None, None]),
            metrics(2, JOB, 200, [None, Some(60), None, None, None]),
        );
        let found = assert_order_and_repetition_invariant(&[
            (&input, Eligible, Final),
            (&output, Eligible, Final),
        ]);
        assert_eq!(found.usage, TokenUsageState::Complete);
        assert_eq!(found.total, Some(100));
    }

    #[test]
    fn a_conflicting_final_makes_only_its_field_and_the_total_unknown() {
        let (input, output_a, output_b, repeat_a, cumulative) = (
            metrics(1, JOB, 100, [Some(40), Some(60), None, None, None]),
            metrics(2, JOB, 200, [None, Some(61), None, None, None]),
            metrics(3, JOB, 300, [None, Some(62), None, None, None]),
            metrics(4, JOB, 400, [None, Some(61), None, None, None]),
            metrics(5, JOB, 500, [None, Some(999), None, None, None]),
        );
        let found = assert_order_and_repetition_invariant(&[
            (&input, Eligible, Final),
            (&output_a, Eligible, Final),
            (&output_b, Eligible, Final),
            (&repeat_a, Eligible, Final),
            (&cumulative, Eligible, Cumulative),
        ]);
        assert_eq!(
            found.output,
            TokenCount::Conflicted,
            "repeating one candidate does not clear the conflict, and the cumulative maximum does not replace it"
        );
        assert_eq!(found.input, TokenCount::Final(40));
        assert_eq!(found.total, None);
        assert_eq!(
            found.usage,
            TokenUsageState::Incomplete,
            "a core conflict is not Complete, and the field state tells it from a mere absence"
        );
    }

    #[test]
    fn detail_counters_never_decide_whether_the_core_usage_is_complete() {
        let (core, detail_a, detail_b) = (
            metrics(1, JOB, 100, io(40, 60)),
            metrics(2, JOB, 200, [None, None, Some(5), None, Some(1)]),
            metrics(3, JOB, 300, [None, None, Some(6), None, None]),
        );
        let found = assert_order_and_repetition_invariant(&[
            (&core, Eligible, Final),
            (&detail_a, Eligible, Final),
            (&detail_b, Eligible, Final),
        ]);
        assert_eq!(found.usage, TokenUsageState::Complete);
        assert_eq!(found.cache_read, TokenCount::Conflicted);
        assert_eq!(found.cache_write, TokenCount::Missing);
        assert_eq!(found.reasoning, TokenCount::Final(1));
        assert_eq!(found.total, Some(100), "detail counts are not in the total");
    }

    #[test]
    fn detail_counters_are_kept_when_the_core_usage_is_not() {
        let detail = metrics(1, JOB, 100, [None, None, Some(5), Some(2), Some(3)]);
        let found = sample(&[(&detail, Eligible, Final)]);
        assert_eq!(found.usage, TokenUsageState::Incomplete);
        assert_eq!(
            (found.cache_read, found.cache_write, found.reasoning),
            (
                TokenCount::Final(5),
                TokenCount::Final(2),
                TokenCount::Final(3)
            )
        );
        assert_eq!(found.total, None);
    }

    #[test]
    fn a_real_zero_is_a_known_count_and_missing_is_not() {
        let zero = metrics(1, JOB, 100, [Some(0), Some(0), Some(0), None, None]);
        let found = sample(&[(&zero, Eligible, Final)]);
        assert_eq!(found.usage, TokenUsageState::Complete);
        assert_eq!(
            (found.input, found.cache_read, found.cache_write),
            (
                TokenCount::Final(0),
                TokenCount::Final(0),
                TokenCount::Missing
            )
        );
        assert_eq!(found.total, Some(0));

        let missing_output = metrics(2, JOB, 100, [Some(7), None, None, None, None]);
        let found = sample(&[(&missing_output, Eligible, Final)]);
        assert_eq!(found.total, None, "a missing output is not a zero");
        assert_eq!(
            found.input,
            TokenCount::Final(7),
            "known parts stay visible"
        );
    }

    #[test]
    fn the_total_needs_both_components_and_is_not_limited_to_u32() {
        let (big, conflicted_a, conflicted_b) = (
            metrics(1, JOB, 100, io(u32::MAX, u32::MAX)),
            metrics(2, JOB, 200, [None, Some(1), None, None, None]),
            metrics(3, JOB, 300, [None, Some(2), None, None, None]),
        );
        let found = sample(&[(&big, Eligible, Final)]);
        assert_eq!(
            found.total,
            Some(2 * u64::from(u32::MAX)),
            "the sum above u32::MAX is exact, not saturated"
        );
        let found = sample(&[
            (&big, Eligible, Cumulative),
            (&conflicted_a, Eligible, Final),
            (&conflicted_b, Eligible, Final),
        ]);
        assert_eq!(found.input, TokenCount::Provisional(u32::MAX));
        assert_eq!(found.total, None, "a conflicted component leaves no total");
    }

    #[test]
    fn the_sample_time_is_the_lowest_metrics_id_not_the_earliest_observation() {
        let (low_id_late, high_id_early, higher_id_earliest) = (
            metrics(10, JOB, 5_000, io(1, 1)),
            metrics(11, JOB, 1_000, io(2, 2)),
            metrics(12, JOB, -9_000, io(3, 3)),
        );
        let found = assert_order_and_repetition_invariant(&[
            (&high_id_early, Eligible, Cumulative),
            (&low_id_late, Eligible, Cumulative),
            (&higher_id_earliest, Eligible, Cumulative),
        ]);
        assert_eq!(found.first_metrics, MetricsId::new(10));
        assert_eq!(found.observed_at, Timestamp::from_unix_micros(5_000));
    }

    #[test]
    fn an_initial_record_without_usage_anchors_the_time_for_later_usage() {
        let (silent, later) = (metrics(1, JOB, 700, NONE), metrics(2, JOB, 100, io(30, 40)));
        let found = assert_order_and_repetition_invariant(&[
            (&silent, Eligible, Cumulative),
            (&later, Eligible, Final),
        ]);
        assert_eq!(found.observed_at, Timestamp::from_unix_micros(700));
        assert_eq!(found.usage, TokenUsageState::Complete);
    }

    #[test]
    fn a_retry_of_turn_is_a_separate_usage_sample() {
        use crate::database::DatabaseWorker;
        use crate::persistence::{Actor, ConversationSpec, Entry, JobFinalization, Origin};
        use crate::test_support::{data_directory, execution_configuration, wait};
        let (_root, directory) = data_directory("token-usage", "retry-of");
        let worker = DatabaseWorker::start(&directory).expect("database");
        let database = worker.handle();
        let (conversation, first) = wait(database.open_conversation(
            None,
            ConversationSpec {
                name: None,
                agent: None,
                project: None,
                origin: Origin::User,
                keep_open: true,
            },
            Entry::UserMessage {
                text: "original".into(),
                attachments: vec![],
            },
            execution_configuration(&database),
            None,
        ))
        .expect("open");
        let executor = wait(database.create_executor(crate::persistence::NewExecutor {
            name: "retry worker".into(),
            concurrency_limit: std::num::NonZeroU32::MIN,
            llm_transports: [crate::persistence::LlmTransport::OpenAiChatCompletions].into(),
        }))
        .expect("executor");
        wait(database.claim_fixture(first.turn, executor)).expect("claim");
        wait(database.finalize_job(first.turn, JobFinalization::Unknown)).expect("unknown");
        let next = wait(database.retry(conversation, first.turn, Actor::User)).expect("retry");
        assert!(
            matches!(wait(database.turn_entries(next.turn)).expect("entries")[0].1.entry, Entry::RetryOf { turn, .. } if turn == first.turn)
        );
        let (first_try, retry) = (
            metrics(1, first.turn, 100, io(10, 10)),
            metrics(2, next.turn, 200, io(10, 10)),
        );
        let (a, b) = (
            sample(&[(&first_try, Eligible, Final)]),
            sample(&[(&retry, Eligible, Final)]),
        );
        assert_ne!(a.job, b.job);
        assert_eq!((a.total, b.total), (Some(20), Some(20)));
        assert!(
            reduce(&[(&first_try, Eligible, Final), (&retry, Eligible, Final)]).is_err(),
            "the two Jobs are never reduced together"
        );
        worker.shutdown().expect("shutdown");
    }

    #[test]
    fn reducing_changes_no_source_record() {
        let record = metrics(1, JOB, 100, io(10, 20));
        let before = record.clone();
        let _ = sample(&[(&record, Eligible, Final)]);
        assert_eq!(record, before);
    }
}
