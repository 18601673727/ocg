use std::collections::HashMap;
use std::ops::Bound;
use std::sync::{Arc, Mutex, OnceLock};

use super::rows::decode_request_event;
use super::{StorageError, TxRead, jobs, state};
use crate::db::entities::{
    AvailabilityObservation, AvailabilityStatRecord, DecisionTarget, MetricsRecord,
    ObservationCursor, ObservationFilter, ObservationPage, RequestEventRecord, RequestPayload,
    TokenAnnotation,
};
use crate::db::entities::{
    AvailabilityStatId, ContentDigest, JobId, LlmTarget, Measurements, MetricsId, ProviderResponse,
    ProviderResponsesPage, RequestNumber, Timestamp,
};
use crate::db::redb::reads::read;
use crate::db::redb::rows::ConfigRow;
use crate::db::redb::schema::{
    CONFIGS, CONVERSATIONS, JOB_OBSERVATIONS, JOB_RUNTIME, JOBS, MAX_PAGE, MESSAGES,
    REQUEST_EVENTS, REQUEST_TIMELINE, RequestTimelineKey,
};
use ::redb::{Database, ReadTransaction, ReadableTable};
use serde_json::Value;

#[derive(Clone)]
enum ObservationTarget {
    Llm(Box<LlmTarget>),
    Decision(DecisionTarget),
}

fn frozen_observation_target(
    rx: &ReadTransaction,
    job: JobId,
) -> Result<Option<ObservationTarget>, StorageError> {
    use crate::db::entities::MutableFrozenConfiguration;
    let jobs = rx.open_table(JOBS)?;
    let Some(bytes) = jobs.get(job.get())? else {
        return Ok(None);
    };
    let row = jobs::decode_mutable_job(job.get(), bytes.value())?;
    match row.frozen {
        Some(MutableFrozenConfiguration::Generative(frozen)) => {
            Ok(Some(ObservationTarget::Llm(Box::new(LlmTarget {
                llm_profile: frozen.llm.model.id.into(),
                frozen: frozen.llm,
            }))))
        }
        Some(MutableFrozenConfiguration::Decision(frozen)) => {
            let provider = frozen.connection_ref;
            let provider_name = rx
                .open_table(CONFIGS)?
                .get(provider.id.get())?
                .map(|bytes| ConfigRow::decode_at(provider.id.get(), bytes.value()))
                .transpose()?
                .map(|row| row.name)
                .unwrap_or_else(|| provider.id.to_string());
            Ok(Some(ObservationTarget::Decision(DecisionTarget {
                provider,
                model: frozen.model_ref,
                model_name: frozen.model.model,
                provider_name,
            })))
        }
        None => Ok(None),
    }
}

fn event_at_key(
    table: &impl ReadableTable<(u128, u32, u32), &'static [u8]>,
    job: JobId,
    request: RequestNumber,
    seq: u32,
) -> Result<Option<RequestEventRecord>, StorageError> {
    table
        .get((job.get(), request.get(), seq))?
        .map(|bytes| decode_request_event(job, request, seq, bytes.value()))
        .transpose()
}

fn paired(measurement: &RequestEventRecord, token: &RequestEventRecord) -> bool {
    matches!(measurement.payload, RequestPayload::Measurement { .. })
        && matches!(token.payload, RequestPayload::Token { .. })
        && measurement.request == token.request
        && measurement.seq.checked_add(1) == Some(token.seq)
        && measurement.observed_at == token.observed_at
        && measurement.recorded_at == token.recorded_at
}

pub(super) fn observation_kind(
    table: &impl ReadableTable<(u128, u32, u32), &'static [u8]>,
    event: &RequestEventRecord,
) -> Result<Option<u8>, StorageError> {
    match event.payload {
        RequestPayload::Measurement { .. } => Ok(Some(1)),
        RequestPayload::Availability { .. } => Ok(Some(2)),
        RequestPayload::Token { .. } => {
            let previous = event
                .seq
                .checked_sub(1)
                .map(|seq| event_at_key(table, event.job_id, event.request, seq))
                .transpose()?
                .flatten();
            Ok((!previous
                .as_ref()
                .is_some_and(|previous| paired(previous, event)))
            .then_some(1))
        }
        RequestPayload::Sent { .. } | RequestPayload::Received { .. } => Ok(None),
    }
}

fn metric_record_with_target(
    table: &impl ReadableTable<(u128, u32, u32), &'static [u8]>,
    target: Option<&ObservationTarget>,
    event: &RequestEventRecord,
) -> Result<Option<MetricsRecord>, StorageError> {
    let (llm_target, decision_target) = match target {
        Some(ObservationTarget::Llm(llm)) => (Some(llm.as_ref()), None),
        Some(ObservationTarget::Decision(decision)) => (None, Some(decision)),
        None => (None, None),
    };
    let mut record = metric_record(table, llm_target, event)?;
    if let Some(record) = &mut record {
        record.decision_target = decision_target.cloned();
    }
    Ok(record)
}

pub(super) fn metric_record(
    table: &impl ReadableTable<(u128, u32, u32), &'static [u8]>,
    target: Option<&LlmTarget>,
    event: &RequestEventRecord,
) -> Result<Option<MetricsRecord>, StorageError> {
    let (measurements, token) = match &event.payload {
        RequestPayload::Measurement { measurements } => {
            let next = event
                .seq
                .checked_add(1)
                .map(|seq| event_at_key(table, event.job_id, event.request, seq))
                .transpose()?
                .flatten();
            let token =
                next.filter(|next| paired(event, next))
                    .and_then(|next| match next.payload {
                        RequestPayload::Token {
                            eligibility,
                            finality,
                        } => Some(TokenAnnotation {
                            eligibility,
                            finality,
                        }),
                        RequestPayload::Measurement { .. }
                        | RequestPayload::Availability { .. }
                        | RequestPayload::Sent { .. }
                        | RequestPayload::Received { .. } => None,
                    });
            (measurements.clone(), token)
        }
        RequestPayload::Token {
            eligibility,
            finality,
        } => {
            if observation_kind(table, event)? != Some(1) {
                return Ok(None);
            }
            (
                Measurements::default(),
                Some(TokenAnnotation {
                    eligibility: *eligibility,
                    finality: *finality,
                }),
            )
        }
        RequestPayload::Availability { .. }
        | RequestPayload::Sent { .. }
        | RequestPayload::Received { .. } => return Ok(None),
    };
    Ok(Some(MetricsRecord {
        id: MetricsId::new(event.id.get()),
        job: event.job_id,
        request: event.request,
        target: target.cloned(),
        decision_target: None,
        observed_at: event.observed_at,
        measurements,
        token,
        recorded_at: event.recorded_at,
    }))
}

pub(super) use crate::db::entities::usage_sample_of;

fn availability_of(
    job: JobId,
    target: ObservationTarget,
    event: RequestEventRecord,
) -> Option<AvailabilityStatRecord> {
    let RequestPayload::Availability {
        status_code,
        finish_reason,
        raw_finish_reason,
        failure,
    } = event.payload
    else {
        return None;
    };
    let (target, decision_target) = match target {
        ObservationTarget::Llm(llm) => (Some(*llm), None),
        ObservationTarget::Decision(decision) => (None, Some(decision)),
    };
    Some(AvailabilityStatRecord {
        id: AvailabilityStatId::new(event.id.get()),
        job,
        request: event.request,
        target,
        decision_target,
        observed_at: event.observed_at,
        observation: AvailabilityObservation {
            status_code,
            finish_reason,
            raw_finish_reason,
            failure,
        },
        recorded_at: event.recorded_at,
    })
}

fn read_job_observations<T>(
    database: &Database,
    job: JobId,
    kind: u8,
    after: u128,
    limit: usize,
    mut build: impl FnMut(
        &redb::ReadOnlyTable<(u128, u32, u32), &'static [u8]>,
        Option<&ObservationTarget>,
        RequestEventRecord,
    ) -> Result<T, StorageError>,
) -> Result<Vec<T>, StorageError> {
    let rx = read(database)?;
    let target = frozen_observation_target(&rx, job)?;
    let events = rx.open_table(REQUEST_EVENTS)?;
    let index = rx.open_table(JOB_OBSERVATIONS)?;
    let mut records = Vec::new();
    for entry in index
        .range((
            Bound::Excluded((job.get(), kind, after)),
            Bound::Included((job.get(), kind, u128::MAX)),
        ))?
        .take(limit.min(MAX_PAGE))
    {
        let (_, position) = entry?;
        let (request, seq) = position.value();
        let request = RequestNumber::new(request)
            .ok_or_else(|| StorageError::corrupted("invalid observation request"))?;
        let event = event_at_key(&events, job, request, seq)?
            .ok_or_else(|| StorageError::corrupted("observation event missing"))?;
        records.push(build(&events, target.as_ref(), event)?);
    }
    Ok(records)
}

pub(crate) fn read_job_metrics(
    database: &Database,
    job: JobId,
    after: Option<MetricsId>,
    limit: usize,
) -> Result<Vec<MetricsRecord>, StorageError> {
    read_job_observations(
        database,
        job,
        1,
        after.map_or(0, MetricsId::get),
        limit,
        |events, target, event| {
            metric_record_with_target(events, target, &event)?
                .ok_or_else(|| StorageError::corrupted("invalid metrics index"))
        },
    )
}

pub(crate) fn read_job_availability(
    database: &Database,
    job: JobId,
    after: Option<AvailabilityStatId>,
    limit: usize,
) -> Result<Vec<AvailabilityStatRecord>, StorageError> {
    read_job_observations(
        database,
        job,
        2,
        after.map_or(0, AvailabilityStatId::get),
        limit,
        |_, target, event| {
            let target = target
                .cloned()
                .ok_or_else(|| StorageError::corrupted("invalid availability index"))?;
            availability_of(job, target, event)
                .ok_or_else(|| StorageError::corrupted("invalid availability index"))
        },
    )
}

fn cursor_key(cursor: &ObservationCursor) -> RequestTimelineKey {
    (
        cursor.observed_at.as_unix_micros(),
        cursor.job.get(),
        cursor.request.get(),
        cursor.seq,
    )
}

fn key_cursor((observed_at, job, request, seq): RequestTimelineKey) -> ObservationCursor {
    ObservationCursor {
        observed_at: Timestamp::from_unix_micros(observed_at),
        job: JobId::new(job),
        request: RequestNumber::new(request).unwrap_or(RequestNumber::FIRST),
        seq,
    }
}

fn walk_timeline(
    rx: &ReadTransaction,
    filter: &ObservationFilter,
    after: Option<&ObservationCursor>,
    max_scan: usize,
    mut visit: impl FnMut(RequestTimelineKey) -> Result<bool, StorageError>,
) -> Result<Option<RequestTimelineKey>, StorageError> {
    let table = rx.open_table(REQUEST_TIMELINE)?;
    let from_micros = filter.observed.from.as_unix_micros();
    let until_micros = filter.observed.until.as_unix_micros();
    let first = (from_micros, 0, 0, 0);
    let upper = after
        .map(cursor_key)
        .unwrap_or((until_micros, 0, 0, 0))
        .min((until_micros, 0, 0, 0));
    if from_micros >= until_micros || max_scan == 0 || upper <= first {
        return Ok(None);
    }

    let range = table.range((Bound::Included(first), Bound::Excluded(upper)))?;
    let mut examined = 0usize;
    for entry in range.rev() {
        let key = entry?.0.value();
        examined += 1;
        if !visit(key)? || examined >= max_scan {
            return Ok(Some(key));
        }
    }
    Ok(None)
}

fn target_matches(
    rx: &ReadTransaction,
    filter: &ObservationFilter,
    job: JobId,
    cache: &mut HashMap<JobId, Option<ObservationTarget>>,
) -> Result<Option<ObservationTarget>, StorageError> {
    let target = match cache.get(&job) {
        Some(target) => target.clone(),
        None => {
            let target = frozen_observation_target(rx, job)?;
            cache.insert(job, target.clone());
            target
        }
    };
    Ok(target.filter(|target| match target {
        ObservationTarget::Llm(target) => {
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
        ObservationTarget::Decision(decision) => {
            filter.llm_profile.is_none()
                && filter
                    .provider
                    .is_none_or(|provider| provider == decision.provider.id.into())
                && filter
                    .model
                    .as_deref()
                    .is_none_or(|model| model == decision.model_name)
        }
    }))
}

fn event_at_parts(
    rx: &ReadTransaction,
    job: u128,
    request: u32,
    seq: u32,
) -> Result<Option<RequestEventRecord>, StorageError> {
    let Some(bytes) = rx.open_table(REQUEST_EVENTS)?.get((job, request, seq))? else {
        return Ok(None);
    };
    let request = RequestNumber::new(request)
        .ok_or_else(|| StorageError::corrupted("a stored request number is 0"))?;
    Ok(Some(decode_request_event(
        JobId::new(job),
        request,
        seq,
        bytes.value(),
    )?))
}

fn event_at_timeline(
    rx: &ReadTransaction,
    key: RequestTimelineKey,
) -> Result<Option<RequestEventRecord>, StorageError> {
    let (_, job, request, seq) = key;
    event_at_parts(rx, job, request, seq)
}

fn read_observation_page<T>(
    database: &Database,
    filter: &ObservationFilter,
    cursor: Option<ObservationCursor>,
    limit: usize,
    max_scan: usize,
    mut project: impl FnMut(
        &ReadTransaction,
        JobId,
        ObservationTarget,
        RequestEventRecord,
    ) -> Result<Option<T>, StorageError>,
) -> Result<ObservationPage<T>, StorageError> {
    let rx = read(database)?;
    let limit = limit.clamp(1, MAX_PAGE);
    let mut cache = HashMap::new();
    let mut records = Vec::new();
    let stopped = walk_timeline(&rx, filter, cursor.as_ref(), max_scan, |key| {
        let job = JobId::new(key.1);
        let Some(target) = target_matches(&rx, filter, job, &mut cache)? else {
            return Ok(true);
        };
        if let Some(event) = event_at_timeline(&rx, key)?
            && let Some(record) = project(&rx, job, target, event)?
        {
            records.push(record);
        }
        Ok(records.len() < limit)
    })?;
    Ok(ObservationPage {
        records,
        next: stopped.map(key_cursor),
    })
}

pub(crate) fn read_metrics(
    database: &Database,
    filter: &ObservationFilter,
    cursor: Option<ObservationCursor>,
    limit: usize,
    max_scan: usize,
) -> Result<ObservationPage<MetricsRecord>, StorageError> {
    read_observation_page(
        database,
        filter,
        cursor,
        limit,
        max_scan,
        |rx, _, target, event| {
            let ObservationTarget::Llm(target) = target else {
                return Ok(None);
            };
            if !matches!(
                event.payload,
                RequestPayload::Measurement { .. } | RequestPayload::Token { .. }
            ) {
                return Ok(None);
            }
            metric_record(&rx.open_table(REQUEST_EVENTS)?, Some(&target), &event)
        },
    )
}

pub(crate) fn read_availability_stats(
    database: &Database,
    filter: &ObservationFilter,
    cursor: Option<ObservationCursor>,
    limit: usize,
    max_scan: usize,
) -> Result<ObservationPage<AvailabilityStatRecord>, StorageError> {
    read_observation_page(
        database,
        filter,
        cursor,
        limit,
        max_scan,
        |_, job, target, event| Ok(availability_of(job, target, event)),
    )
}

#[derive(Clone)]
struct DecodedMessage {
    text: Arc<str>,
    requests_tools: bool,
    escaped_len: usize,
}

impl DecodedMessage {
    fn answer(&self) -> Option<&str> {
        (!self.requests_tools).then_some(self.text.as_ref())
    }
}

pub(super) fn answer_text(
    tx: &impl TxRead,
    id: JobId,
    output: &crate::db::entities::JobOutputRecord,
) -> Result<Option<String>, StorageError> {
    if output.answer_request.is_none() {
        return Ok(None);
    }
    let jobs = tx.table(JOBS)?;
    let states = tx.table(JOB_RUNTIME)?;
    let conversations = tx.table(CONVERSATIONS)?;
    let events = tx.table(REQUEST_EVENTS)?;
    let messages = tx.table(MESSAGES)?;
    answer_text_in(
        &jobs,
        &states,
        &conversations,
        &events,
        &messages,
        id,
        output,
    )
}

pub(super) fn project_answer(
    tx: &impl TxRead,
    id: JobId,
    output: &mut crate::db::entities::JobOutputRecord,
) -> Result<(), StorageError> {
    if let Some(text) = answer_text(tx, id, output)? {
        let value = output.value.get_or_insert_with(|| serde_json::json!({}));
        let object = value
            .as_object_mut()
            .ok_or_else(|| StorageError::corrupted("answer Result value is not an object"))?;
        object.insert("text".to_owned(), serde_json::Value::String(text));
    } else if output.kind == crate::db::entities::OutputKind::Result
        && output.summary.is_none()
        && let Some(value) = output
            .value
            .as_ref()
            .filter(|v| v.get("producer_job").is_some())
    {
        let link = serde_json::from_value(value.clone())
            .map_err(|_| StorageError::corrupted("invalid summary result link"))?;
        let summary = linked_summary(tx, id, link)?.summary;
        output.summary = Some(crate::db::entities::output_filter::text(&summary).into_owned());
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn answer_text_in(
    jobs: &impl ReadableTable<u128, &'static [u8]>,
    states: &impl ReadableTable<u128, &'static [u8]>,
    conversations: &impl ReadableTable<u128, &'static [u8]>,
    events: &impl ReadableTable<(u128, u32, u32), &'static [u8]>,
    messages: &impl ReadableTable<&'static [u8], &'static [u8]>,
    id: JobId,
    output: &crate::db::entities::JobOutputRecord,
) -> Result<Option<String>, StorageError> {
    use crate::db::entities::{JobSpec, MutableJobContent, OutcomeKind, OutputKind};
    if output.kind != OutputKind::Result
        || output.code.is_some()
        || output
            .value
            .as_ref()
            .and_then(|value| value.get("finish_reason"))
            .and_then(|value| value.as_str())
            == Some("tool_calls")
    {
        return Ok(None);
    }
    let Some(row) = jobs.get(id.get())? else {
        return Ok(None);
    };
    let job = jobs::decode_mutable_job(id.get(), row.value())?;
    if job.summary_source.is_some()
        || !matches!(job.input.content, MutableJobContent::Generative { .. })
    {
        return Ok(None);
    }
    let Some(parent) = conversations.get(job.parent_id.get())? else {
        return Ok(None);
    };
    if JobSpec::decode_at(job.parent_id.get(), parent.value())?.is_chunk_group() {
        return Ok(None);
    }
    let Some(runtime) = states.get(id.get())? else {
        return Ok(None);
    };
    if !state::decode_state(id.get(), runtime.value())?
        .outcome
        .is_some_and(|outcome| outcome.kind == OutcomeKind::Completed)
    {
        return Ok(None);
    }
    let Some(number) = output.answer_request else {
        return Ok(None);
    };
    if latest_request(events, id)? != Some(number) {
        return Ok(None);
    }
    let Some(digest) = received_digest(events, id, number)? else {
        return Ok(None);
    };
    let body = messages
        .get(digest.as_bytes().as_slice())?
        .ok_or_else(|| StorageError::corrupted("response references a missing message"))?;
    let decoded = decode_message(digest, body.value())?;
    Ok(decoded.answer().map(str::to_owned))
}

pub(super) fn latest_request(
    events: &impl ReadableTable<(u128, u32, u32), &'static [u8]>,
    id: JobId,
) -> Result<Option<u32>, StorageError> {
    Ok(events
        .range((id.get(), 0, 0)..=(id.get(), u32::MAX, u32::MAX))?
        .next_back()
        .transpose()?
        .map(|(key, _)| key.value().1))
}

fn received_digest(
    events: &impl ReadableTable<(u128, u32, u32), &'static [u8]>,
    id: JobId,
    number: u32,
) -> Result<Option<ContentDigest>, StorageError> {
    let request =
        RequestNumber::new(number).ok_or_else(|| StorageError::corrupted("zero request number"))?;
    for entry in events
        .range((id.get(), number, 0)..=(id.get(), number, u32::MAX))?
        .rev()
    {
        let (key, value) = entry?;
        if let RequestPayload::Received { digest } =
            decode_request_event(id, request, key.value().2, value.value())?.payload
        {
            return Ok(Some(digest));
        }
    }
    Ok(None)
}

pub(super) fn received_body(
    tx: &impl TxRead,
    job: JobId,
    number: u32,
) -> Result<Option<Value>, StorageError> {
    let Some(digest) = received_digest(&tx.table(REQUEST_EVENTS)?, job, number)? else {
        return Ok(None);
    };
    let messages = tx.table(MESSAGES)?;
    let body = messages
        .get(digest.as_bytes().as_slice())?
        .ok_or_else(|| StorageError::corrupted("response references a missing message"))?;
    serde_json::from_slice(body.value())
        .map(Some)
        .map_err(|e| StorageError::corrupted(format!("invalid response JSON: {e}")))
}

fn raw_answer(body: &Value) -> Option<String> {
    if let Some(items) = body["output"].as_array() {
        let mut text = String::new();
        for item in items
            .iter()
            .filter(|item| item["type"] == "message" && item["role"] == "assistant")
        {
            for part in item["content"].as_array()? {
                text.push_str(
                    part[if part["type"] == "refusal" {
                        "refusal"
                    } else {
                        "text"
                    }]
                    .as_str()?,
                );
            }
        }
        Some(text)
    } else if body["role"] == "assistant" {
        body["content"].as_str().map(str::to_owned)
    } else {
        None
    }
}

pub(super) fn received_part(
    body: &Value,
    kind: crate::db::entities::OutputKind,
    output: u32,
    part: u32,
) -> Option<&str> {
    use crate::db::entities::OutputKind;
    if let Some(items) = body["output"].as_array() {
        let item = items.get(output as usize)?;
        match kind {
            OutputKind::Progress if item["type"] == "message" && item["role"] == "assistant" => {
                let part = &item["content"][part as usize];
                match part["type"].as_str()? {
                    "output_text" => part["text"].as_str(),
                    "refusal" => part["refusal"].as_str(),
                    _ => None,
                }
            }
            OutputKind::Reasoning if item["type"] == "reasoning" => {
                item["summary"][part as usize]["text"].as_str()
            }
            _ => None,
        }
    } else if kind == OutputKind::Progress
        && (output, part) == (0, 0)
        && body["role"] == "assistant"
    {
        body["content"].as_str()
    } else {
        None
    }
}

pub(super) fn context_output_bytes(
    tx: &impl TxRead,
    row: &crate::db::entities::JobOutputRecord,
    stored_len: usize,
    counted: &mut std::collections::BTreeSet<u32>,
) -> Result<usize, StorageError> {
    let request = row
        .answer_request
        .or_else(|| row.span.and(row.part).map(|part| part.0));
    let Some(n) = request.filter(|n| counted.insert(*n)) else {
        return Ok(stored_len);
    };
    let digest = received_digest(&tx.table(REQUEST_EVENTS)?, row.job, n)?
        .ok_or_else(|| StorageError::corrupted("answered request has no response"))?;
    let messages = tx.table(MESSAGES)?;
    let body = messages
        .get(digest.as_bytes().as_slice())?
        .ok_or_else(|| StorageError::corrupted("answered request has no message"))?;
    let decoded = decode_message(digest, body.value())?;
    Ok(stored_len.saturating_add(decoded.answer().map_or(0, str::len)))
}

pub(super) fn received_summary(
    tx: &impl TxRead,
    job: JobId,
    request: RequestNumber,
    source: ContentDigest,
) -> Result<Option<crate::db::entities::SummarizationResult>, StorageError> {
    let Some(body) = received_body(tx, job, request.get())? else {
        return Ok(None);
    };
    if requests_tools(&body) {
        return Ok(None);
    }
    let summary = raw_answer(&body)
        .and_then(|text| {
            serde_json::from_str::<crate::db::entities::SummarizationResult>(&text).ok()
        })
        .filter(|summary| summary.source == source && summary.validate().is_ok());
    Ok(summary)
}

pub(super) fn linked_summary(
    tx: &impl TxRead,
    job: JobId,
    link: crate::db::entities::NativeSummaryResultLink,
) -> Result<crate::db::entities::SummarizationResult, StorageError> {
    use crate::db::entities::NativeSummaryResultLink;
    match link {
        NativeSummaryResultLink::Received {
            source,
            answer_request,
            ..
        } => received_summary(tx, job, answer_request, source)?
            .ok_or_else(|| StorageError::corrupted("invalid or missing recorded summary answer")),
        NativeSummaryResultLink::Payload {
            source, payload, ..
        } => {
            let messages = tx.table(MESSAGES)?;
            let bytes = messages
                .get(payload.as_bytes().as_slice())?
                .ok_or_else(|| StorageError::corrupted("summary payload missing"))?;
            let summary: crate::db::entities::SummarizationResult =
                serde_json::from_slice(bytes.value())
                    .map_err(|_| StorageError::corrupted("malformed summary payload"))?;
            summary.validate().map_err(StorageError::corrupted)?;
            if summary.source != source {
                return Err(StorageError::corrupted(
                    "summary payload names another source",
                ));
            }
            Ok(summary)
        }
    }
}

#[derive(Default)]
pub(super) struct PieceReader {
    filters: std::collections::BTreeMap<
        (crate::db::entities::OutputKind, (u32, u32, u32)),
        crate::db::entities::output_filter::TextFilter,
    >,
    bodies: HashMap<u32, Value>,
}

impl PieceReader {
    pub(super) fn project(
        &mut self,
        tx: &impl TxRead,
        row: &mut crate::db::entities::JobOutputRecord,
    ) -> Result<(), StorageError> {
        use crate::db::redb::{rows::decode_output, schema::JOB_OUTPUTS};
        let Some(part @ (number, output, index)) = row.part else {
            return Ok(());
        };
        let job = row.job;
        let key = (row.kind, part);
        let seq = u32::try_from(row.seq)
            .map_err(|_| StorageError::corrupted("invalid output sequence"))?;
        let first = !self.filters.contains_key(&key);
        let filter = self.filters.entry(key).or_default();
        let text = if let Some((offset, len)) = row.span {
            let body = match self.bodies.entry(number) {
                std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(received_body(tx, job, number)?.ok_or_else(|| {
                        StorageError::corrupted("piece references missing response")
                    })?)
                }
            };
            let part_text = received_part(body, row.kind, output, index)
                .ok_or_else(|| StorageError::corrupted("piece references missing response part"))?;
            let end = offset
                .checked_add(len)
                .ok_or_else(|| StorageError::corrupted("piece offset overflow"))?
                as usize;
            let offset = offset as usize;
            let piece = part_text
                .get(offset..end)
                .ok_or_else(|| StorageError::corrupted("piece outside response part"))?;
            if first && offset > 0 {
                filter.consume(&part_text[..offset], false);
            }
            filter.consume(piece, end == part_text.len())
        } else {
            let table = tx.table(JOB_OUTPUTS)?;
            if first && seq > 1 {
                for entry in table.range((job.get(), 0)..(job.get(), seq))? {
                    let (k, v) = entry?;
                    let prior = decode_output(k.value(), v.value())?;
                    if (prior.kind, prior.part) == (row.kind, Some(part)) {
                        filter.consume(
                            prior
                                .summary
                                .as_deref()
                                .ok_or_else(|| StorageError::corrupted("piece text missing"))?,
                            false,
                        );
                    }
                }
            }
            let raw = row
                .summary
                .as_deref()
                .ok_or_else(|| StorageError::corrupted("piece text missing"))?;
            let mut text = filter.consume(raw, false);
            if filter.has_pending() {
                let mut finished = received_digest(&tx.table(REQUEST_EVENTS)?, job, number)?
                    .is_some()
                    || state::read(tx, job)?.is_some_and(|state| state.state.outcome.is_some());
                if finished {
                    for entry in table.range((
                        Bound::Excluded((job.get(), seq)),
                        Bound::Included((job.get(), u32::MAX)),
                    ))? {
                        let (k, v) = entry?;
                        let later = decode_output(k.value(), v.value())?;
                        if (later.kind, later.part) == (row.kind, Some(part)) {
                            finished = false;
                            break;
                        }
                    }
                    if finished {
                        text.push_str(&filter.consume("", true));
                    }
                }
            }
            text
        };
        row.summary = Some(text);
        Ok(())
    }
}

const MESSAGE_CACHE_BYTES: usize = 8 * 1024 * 1024;
const MESSAGE_CACHE_ENTRIES: usize = 1_024;

#[derive(Default)]
struct MessageCache {
    entries: HashMap<[u8; 32], (u64, DecodedMessage)>,
    bytes: usize,
    clock: u64,
}

fn message_cache() -> &'static Mutex<MessageCache> {
    static CACHE: OnceLock<Mutex<MessageCache>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(MessageCache::default()))
}

fn lock_message_cache() -> std::sync::MutexGuard<'static, MessageCache> {
    message_cache()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn evict_least_recently_used(cache: &mut MessageCache) -> bool {
    let Some(key) = cache
        .entries
        .iter()
        .min_by_key(|(_, (used, _))| *used)
        .map(|(key, _)| *key)
    else {
        return false;
    };
    if let Some((_, decoded)) = cache.entries.remove(&key) {
        cache.bytes = cache.bytes.saturating_sub(decoded.text.len());
        true
    } else {
        false
    }
}

fn requests_tools(message: &Value) -> bool {
    message["output"].as_array().is_some_and(|items| {
        items.iter().any(|item| {
            item["type"] == "function_call"
                || item["content"]
                    .as_array()
                    .is_some_and(|parts| parts.iter().any(|part| part["type"] == "tool_use"))
        })
    }) || message
        .get("tool_calls")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|calls| !calls.is_empty())
}

fn decode_message(digest: ContentDigest, body: &[u8]) -> Result<DecodedMessage, StorageError> {
    {
        let mut cache = lock_message_cache();
        let clock = cache.clock.wrapping_add(1);
        cache.clock = clock;
        if let Some((used, decoded)) = cache.entries.get_mut(digest.as_bytes()) {
            *used = clock;
            return Ok(decoded.clone());
        }
    }

    let message: serde_json::Value = serde_json::from_slice(body)
        .map_err(|error| StorageError::corrupted(format!("invalid response JSON: {error}")))?;
    let text = match message.get("content") {
        None | Some(serde_json::Value::Null) => String::new(),
        Some(serde_json::Value::String(text)) => text.clone(),
        Some(_) => return Err(StorageError::corrupted("response content is not text")),
    };
    let output = message.get("output").and_then(serde_json::Value::as_array);
    let text = if let Some(output) = output {
        let mut text = String::new();
        for item in output {
            if item["type"] == "message"
                && item["role"] == "assistant"
                && let Some(parts) = item["content"].as_array()
            {
                for part in parts {
                    let value = if part["type"] == "refusal" {
                        part["refusal"].as_str()
                    } else {
                        part["text"].as_str()
                    };
                    if let Some(value) = value {
                        text.push_str(value);
                    }
                }
            }
        }
        text
    } else {
        text
    };
    let text = match crate::db::entities::output_filter::text(&text) {
        std::borrow::Cow::Borrowed(_) => text,
        std::borrow::Cow::Owned(filtered) => filtered,
    };
    let decoded = DecodedMessage {
        escaped_len: text
            .chars()
            .map(crate::db::entities::escaped_json_char_cost)
            .sum(),
        text: Arc::from(text),
        requests_tools: requests_tools(&message),
    };

    let mut cache = lock_message_cache();

    let clock = cache.clock.wrapping_add(1);
    cache.clock = clock;
    if let Some((used, existing)) = cache.entries.get_mut(digest.as_bytes()) {
        *used = clock;
        return Ok(existing.clone());
    }
    let size = decoded.text.len();
    if size > MESSAGE_CACHE_BYTES {
        cache.entries.clear();
        cache.bytes = 0;
    } else {
        while cache.bytes + size > MESSAGE_CACHE_BYTES
            || cache.entries.len() >= MESSAGE_CACHE_ENTRIES
        {
            if !evict_least_recently_used(&mut cache) {
                break;
            }
        }
    }
    cache
        .entries
        .insert(*digest.as_bytes(), (clock, decoded.clone()));
    cache.bytes += size;
    Ok(decoded)
}

pub(crate) fn read_provider_responses(
    database: &Database,
    job: JobId,
    after: Option<RequestNumber>,
    limit: usize,
    max_text_bytes: usize,
) -> Result<ProviderResponsesPage, StorageError> {
    let rx = read(database)?;
    let events = rx.open_table(REQUEST_EVENTS)?;
    let messages = rx.open_table(MESSAGES)?;
    let bounds = (
        Bound::Excluded((job.get(), after.map_or(0, RequestNumber::get), u32::MAX)),
        Bound::Included((job.get(), u32::MAX, u32::MAX)),
    );
    let mut responses = Vec::new();
    let mut text_bytes = 0;
    let mut has_more = false;
    for entry in events.range(bounds)? {
        let (key, bytes) = entry?;
        let (_, number, seq) = key.value();
        let request = RequestNumber::new(number)
            .ok_or_else(|| StorageError::corrupted("zero request number"))?;
        let event = decode_request_event(job, request, seq, bytes.value())?;
        let RequestPayload::Received { digest } = event.payload else {
            continue;
        };
        if responses.len() == limit || text_bytes >= max_text_bytes {
            has_more = true;
            break;
        }
        let body = messages
            .get(digest.as_bytes().as_slice())?
            .ok_or_else(|| StorageError::corrupted("response references a missing message"))?;
        let message = decode_message(digest, body.value())?;
        text_bytes += message.escaped_len;
        responses.push(ProviderResponse {
            request,
            text: message.text,
            requests_tools: message.requests_tools,
            recorded_at: event.recorded_at,
        });
    }
    Ok(ProviderResponsesPage {
        responses,
        has_more,
    })
}

pub(crate) fn read_provider_request(
    database: &Database,
    job: JobId,
    request: RequestNumber,
) -> Result<Option<crate::db::entities::ProviderRequestRecord>, StorageError> {
    let rx = read(database)?;
    let events = rx.open_table(REQUEST_EVENTS)?;
    let messages = rx.open_table(MESSAGES)?;
    let value = |digest: ContentDigest| -> Result<serde_json::Value, StorageError> {
        let bytes = messages
            .get(digest.as_bytes().as_slice())?
            .ok_or_else(|| StorageError::corrupted("request references a missing message"))?;
        serde_json::from_slice(bytes.value())
            .map_err(|error| StorageError::corrupted(format!("invalid message JSON: {error}")))
    };
    let mut sent = None;
    let mut response = None;
    let mut dropped_turns = 0;
    for entry in
        events.range((job.get(), request.get(), 0)..=(job.get(), request.get(), u32::MAX))?
    {
        let (key, bytes) = entry?;
        let event = decode_request_event(job, request, key.value().2, bytes.value())?;
        match event.payload {
            RequestPayload::Sent {
                messages,
                tools,
                parameters,
                dropped_turns: dropped,
            } => {
                if sent.is_some() {
                    return Err(StorageError::corrupted("request has multiple Sent events"));
                }
                dropped_turns = dropped.unwrap_or_default();
                let request_parameters = value(parameters)?;
                let request_messages = messages
                    .into_iter()
                    .map(&value)
                    .collect::<Result<Vec<_>, _>>()?;
                let request_tools = tools.map(value).transpose()?;
                sent = Some((request_messages, request_tools, request_parameters));
            }
            RequestPayload::Received { digest } => response = Some(value(digest)?),
            RequestPayload::Measurement { .. }
            | RequestPayload::Availability { .. }
            | RequestPayload::Token { .. } => {}
        }
    }
    let Some((messages, tools, parameters)) = sent else {
        return Ok(None);
    };
    for entry in events.range((job.get(), 0, 0)..=(job.get(), u32::MAX, u32::MAX))? {
        let (key, bytes) = entry?;
        let (_, number, seq) = key.value();
        let number = RequestNumber::new(number)
            .ok_or_else(|| StorageError::corrupted("zero request number"))?;
        if let RequestPayload::Sent {
            dropped_turns: dropped,
            ..
        } = decode_request_event(job, number, seq, bytes.value())?.payload
        {
            dropped_turns = dropped.unwrap_or_default();
            break;
        }
    }
    Ok(Some(crate::db::entities::ProviderRequestRecord {
        messages,
        tools,
        parameters,
        dropped_turns,
        response,
    }))
}

pub(super) fn read_responses_replay(
    rx: &::redb::ReadTransaction,
    job: JobId,
    include_last_answer: bool,
    budget: &mut usize,
) -> Result<Vec<Value>, StorageError> {
    let events = rx.open_table(REQUEST_EVENTS)?;
    let messages = rx.open_table(MESSAGES)?;
    let mut first: Option<Vec<ContentDigest>> = None;
    let mut tail = Vec::new();
    let mut answer = None;
    for entry in events.range((job.get(), 1, 0)..=(job.get(), u32::MAX, u32::MAX))? {
        let (key, bytes) = entry?;
        let (_, number, seq) = key.value();
        let request = RequestNumber::new(number)
            .ok_or_else(|| StorageError::corrupted("zero request number"))?;
        match decode_request_event(job, request, seq, bytes.value())?.payload {
            RequestPayload::Sent { messages, .. } => {
                answer = None;
                if let Some(prefix) = &first {
                    tail = messages
                        .strip_prefix(prefix.as_slice())
                        .ok_or_else(|| {
                            StorageError::corrupted("Responses request changed its initial input")
                        })?
                        .to_vec();
                } else {
                    first = Some(messages);
                }
            }
            RequestPayload::Received { digest } => answer = Some(digest),
            RequestPayload::Availability { .. }
            | RequestPayload::Measurement { .. }
            | RequestPayload::Token { .. } => {}
        }
    }
    if include_last_answer {
        tail.extend(answer);
    }
    tail.into_iter()
        .map(|digest| {
            let body = messages.get(digest.as_bytes().as_slice())?.ok_or_else(|| {
                StorageError::corrupted("Responses history references a missing message")
            })?;
            *budget = budget.saturating_add(body.value().len());
            if *budget > 8 * 1024 * 1024 {
                return Err(StorageError {
                    kind: super::StorageErrorKind::Other,
                    os_error: None,
                    detail: "selected Responses history exceeds execution context limit".to_owned(),
                });
            }
            serde_json::from_slice(body.value()).map_err(|error| {
                StorageError::corrupted(format!("invalid Responses history JSON: {error}"))
            })
        })
        .collect()
}
