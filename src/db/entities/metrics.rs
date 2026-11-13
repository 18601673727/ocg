use super::*;

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

#[derive(Clone, Debug, PartialEq)]
pub struct Metrics {
    pub job: JobId,
    pub request: RequestNumber,
    pub observed_at: Timestamp,
    pub measurements: Measurements,
    pub token: Option<TokenAnnotation>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct LlmTarget {
    pub llm_profile: ProfileId,
    pub frozen: FrozenLlmConfig,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MetricsRecord {
    pub id: MetricsId,
    pub job: JobId,
    pub request: RequestNumber,
    pub target: Option<LlmTarget>,
    pub decision_target: Option<DecisionTarget>,
    pub observed_at: Timestamp,
    pub measurements: Measurements,
    pub token: Option<TokenAnnotation>,
    pub recorded_at: Timestamp,
}

fn metrics_of(job: JobId, events: &[RequestEventRecord]) -> Vec<MetricsRecord> {
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
                    target: None,
                    decision_target: None,
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
                        target: None,
                        decision_target: None,
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

pub(in crate::db) fn usage_sample_of(
    job: JobId,
    request: RequestNumber,
    _target: Option<&LlmTarget>,
    events: &[RequestEventRecord],
) -> Option<TokenRequestSample> {
    let records = metrics_of(job, events);
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
