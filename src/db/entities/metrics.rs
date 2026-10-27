use super::*;

/// Measurements of one request. Every measurement is optional: one that was
/// not measured or reported stays `None` and is never stored as zero.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Measurements {
    pub ttfb_ms: Option<u64>,
    pub reasoning_ttft_ms: Option<u64>,
    pub output_ttft_ms: Option<u64>,
    pub request_finished_in_ms: Option<u64>,
    pub usage: NormalizedUsage,
}

impl Measurements {
    pub(in crate::db) fn is_empty(&self) -> bool {
        let usage = &self.usage;
        [
            self.ttfb_ms,
            self.reasoning_ttft_ms,
            self.output_ttft_ms,
            self.request_finished_in_ms,
        ]
        .iter()
        .all(Option::is_none)
            && [
                usage.input_tokens,
                usage.output_tokens,
                usage.cache_read_tokens,
                usage.cache_write_tokens,
                usage.reasoning_tokens,
            ]
            .iter()
            .all(Option::is_none)
            && usage.raw.is_none()
    }
}

/// Measurements taken for a Job, such as one of its LLM requests. Any Job
/// may be measured; one that runs no LLM has no LLM identity. Recording
/// them never changes the Job, and a Job may have any number of them.
#[derive(Clone, Debug, PartialEq)]
pub struct Metrics {
    pub job: JobId,
    /// The Provider request of the Job the record measures.
    pub request: RequestNumber,
    /// When the measured activity happened, as its producer reports it.
    /// Observation queries filter and order by this time.
    pub observed_at: Timestamp,
    pub measurements: Measurements,
    /// The producer's Token facts about this record, when it states any. A
    /// record may be annotated without measurements: an eligible one then
    /// establishes a request whose usage is missing. `None` is no trusted
    /// Token contract, not Excluded and not Final. An annotation needs the
    /// Job's frozen LLM configuration, whose Provider and model are the
    /// request's identity; a caller cannot name another.
    pub token: Option<TokenAnnotation>,
}

/// The LLM a Job ran, as its submission froze it: the Model config as
/// provenance and the configuration the Job actually used. Later config
/// edits change neither.
#[derive(Clone, Debug, PartialEq)]
pub struct LlmTarget {
    pub llm_profile: ProfileId,
    pub frozen: FrozenLlmConfig,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MetricsRecord {
    pub id: MetricsId,
    /// Assembled from MEASURES.
    pub job: JobId,
    /// See [`Metrics::request`].
    pub request: RequestNumber,
    /// Read from the Job's frozen input; `None` for a Job without an LLM.
    pub target: Option<LlmTarget>,
    /// The producer's time, see [`Metrics::observed_at`].
    pub observed_at: Timestamp,
    pub measurements: Measurements,
    /// The producer's Token facts, see [`Metrics::token`].
    pub token: Option<TokenAnnotation>,
    /// The wall-clock time at which the database's write transaction that
    /// stored the record began, not when its commit completed. Wall-clock
    /// time is not monotonic. It is never used in place of `observed_at`.
    pub recorded_at: Timestamp,
}

/// The Metrics records of one Job's events. A Token event that directly
/// follows the Measurement event written with it, in the same transaction,
/// is that record's annotation; any other Token event is a record of its
/// own, without measurements.
fn metrics_of(
    job: JobId,
    target: &Option<LlmTarget>,
    events: &[RequestEventRecord],
) -> Vec<MetricsRecord> {
    let mut records: Vec<MetricsRecord> = Vec::new();
    let mut last_measurement: Option<(RequestNumber, u32, Timestamp, Timestamp)> = None;
    for event in events {
        match &event.payload {
            RequestPayload::Measurement { measurements } => {
                last_measurement = Some((
                    event.request,
                    event.seq,
                    event.observed_at,
                    event.recorded_at,
                ));
                records.push(MetricsRecord {
                    id: MetricsId::new(event.id.get()),
                    job,
                    request: event.request,
                    target: target.clone(),
                    observed_at: event.observed_at,
                    measurements: measurements.clone(),
                    token: None,
                    recorded_at: event.recorded_at,
                });
            }
            RequestPayload::Token {
                eligibility,
                finality,
            } => {
                let annotation = TokenAnnotation {
                    eligibility: *eligibility,
                    finality: *finality,
                };
                let pairs =
                    last_measurement.is_some_and(|(request, seq, observed_at, recorded_at)| {
                        request == event.request
                            && seq + 1 == event.seq
                            && observed_at == event.observed_at
                            && recorded_at == event.recorded_at
                    });
                match records.last_mut() {
                    Some(record) if pairs => record.token = Some(annotation),
                    _ => records.push(MetricsRecord {
                        id: MetricsId::new(event.id.get()),
                        job,
                        request: event.request,
                        target: target.clone(),
                        observed_at: event.observed_at,
                        measurements: Measurements::default(),
                        token: Some(annotation),
                        recorded_at: event.recorded_at,
                    }),
                }
                last_measurement = None;
            }
            RequestPayload::Availability { .. }
            | RequestPayload::Sent { .. }
            | RequestPayload::Received { .. }
            | RequestPayload::ApprovedCalls => last_measurement = None,
        }
    }
    records
}

/// Reduces one request's events to its Token sample. An evidence error drops
/// only this request, and is logged once with the Job, the request and the
/// error; no sample is then returned. Both the mutation and the rebuild path
/// use this rule, so they cannot disagree about a request.
pub(in crate::db) fn usage_sample_of(
    job: JobId,
    request: RequestNumber,
    target: &Option<LlmTarget>,
    events: &[RequestEventRecord],
) -> Option<TokenRequestSample> {
    let records = metrics_of(job, target, events);
    match TokenRequestSample::from_metrics(&records) {
        Ok(sample) => sample,
        Err(error) => {
            log::warn!(
                "dropping Token usage of {job} request {}: {error}",
                request.get()
            );
            None
        }
    }
}
