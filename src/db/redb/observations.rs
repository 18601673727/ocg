//! Reads of the raw request events a Job recorded, plus scoped model queries
//! and globally time-ordered observation pages.

use std::collections::HashMap;
use std::ops::Bound;
use std::sync::{Arc, Mutex, OnceLock};

use super::rows::decode_request_event;
use super::*;
use crate::db::entities::{
    AvailabilityObservation, AvailabilityStatRecord, MetricsRecord, ObservationCursor,
    ObservationFilter, ObservationPage, RequestEventRecord, RequestPayload, TokenAnnotation,
};

pub(super) fn frozen_llm_target(
    rx: &ReadTransaction,
    job: JobId,
) -> Result<Option<LlmTarget>, StorageError> {
    let Some(row) = rx
        .open_table(JOBS)?
        .get(job.get())?
        .map(|bytes| JobSpec::decode_at(job.get(), bytes.value()))
        .transpose()?
    else {
        return Ok(None);
    };
    reader::turn_target(rx, &row)
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
        RequestPayload::Sent { .. }
        | RequestPayload::Received { .. }
        | RequestPayload::ApprovedCalls => Ok(None),
    }
}

pub(super) fn metric_record(
    table: &impl ReadableTable<(u128, u32, u32), &'static [u8]>,
    target: &Option<LlmTarget>,
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
                        | RequestPayload::Received { .. }
                        | RequestPayload::ApprovedCalls => None,
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
        | RequestPayload::Received { .. }
        | RequestPayload::ApprovedCalls => return Ok(None),
    };
    Ok(Some(MetricsRecord {
        id: MetricsId::new(event.id.get()),
        job: event.job_id,
        request: event.request,
        target: target.clone(),
        observed_at: event.observed_at,
        measurements,
        token,
        recorded_at: event.recorded_at,
    }))
}

pub(super) use crate::db::entities::usage_sample_of;

fn availability_of(
    job: JobId,
    target: &LlmTarget,
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
    Some(AvailabilityStatRecord {
        id: AvailabilityStatId::new(event.id.get()),
        job,
        request: event.request,
        target: target.clone(),
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

pub(crate) fn read_job_metrics(
    database: &Database,
    job: JobId,
    after: Option<MetricsId>,
    limit: usize,
) -> Result<Vec<MetricsRecord>, StorageError> {
    let rx = read(database)?;
    let target = frozen_llm_target(&rx, job)?;
    let events = rx.open_table(REQUEST_EVENTS)?;
    let index = rx.open_table(JOB_OBSERVATIONS)?;
    let mut records = Vec::new();
    for entry in index
        .range((
            Bound::Excluded((job.get(), 1, after.map_or(0, MetricsId::get))),
            Bound::Included((job.get(), 1, u128::MAX)),
        ))?
        .take(limit.min(MAX_PAGE))
    {
        let (_, position) = entry?;
        let (request, seq) = position.value();
        let request = RequestNumber::new(request)
            .ok_or_else(|| StorageError::corrupted("invalid observation request"))?;
        let event = event_at_key(&events, job, request, seq)?
            .ok_or_else(|| StorageError::corrupted("observation event missing"))?;
        records.push(
            metric_record(&events, &target, &event)?
                .ok_or_else(|| StorageError::corrupted("invalid metrics index"))?,
        );
    }
    Ok(records)
}

pub(crate) fn read_job_availability(
    database: &Database,
    job: JobId,
    after: Option<AvailabilityStatId>,
    limit: usize,
) -> Result<Vec<AvailabilityStatRecord>, StorageError> {
    let rx = read(database)?;
    let Some(target) = frozen_llm_target(&rx, job)? else {
        return Ok(Vec::new());
    };
    let events = rx.open_table(REQUEST_EVENTS)?;
    let index = rx.open_table(JOB_OBSERVATIONS)?;
    let mut records = Vec::new();
    for entry in index
        .range((
            Bound::Excluded((job.get(), 2, after.map_or(0, AvailabilityStatId::get))),
            Bound::Included((job.get(), 2, u128::MAX)),
        ))?
        .take(limit.min(MAX_PAGE))
    {
        let (_, position) = entry?;
        let (request, seq) = position.value();
        let request = RequestNumber::new(request)
            .ok_or_else(|| StorageError::corrupted("invalid observation request"))?;
        let event = event_at_key(&events, job, request, seq)?
            .ok_or_else(|| StorageError::corrupted("observation event missing"))?;
        records.push(
            availability_of(job, &target, event)
                .ok_or_else(|| StorageError::corrupted("invalid availability index"))?,
        );
    }
    Ok(records)
}

// ---- queries over the scope index ---------------------------------------------------

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

/// Walks the global timeline newest first, strictly older than `after`.
/// Examines at most `max_scan` entries and returns the position to continue
/// from if the walk stopped before the beginning of the time range.
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

/// Whether a Job froze the target `filter` names (its Model config).
fn target_matches(
    rx: &ReadTransaction,
    filter: &ObservationFilter,
    job: JobId,
    cache: &mut HashMap<JobId, Option<LlmTarget>>,
) -> Result<Option<LlmTarget>, StorageError> {
    let target = match cache.get(&job) {
        Some(target) => target.clone(),
        None => {
            let target = frozen_llm_target(rx, job)?;
            cache.insert(job, target.clone());
            target
        }
    };
    Ok(target.filter(|target| {
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
        LlmTarget,
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
            if !matches!(
                event.payload,
                RequestPayload::Measurement { .. } | RequestPayload::Token { .. }
            ) {
                return Ok(None);
            }
            metric_record(&rx.open_table(REQUEST_EVENTS)?, &Some(target), &event)
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
        |_, job, target, event| Ok(availability_of(job, &target, event)),
    )
}

pub(crate) struct ProviderResponse {
    pub request: RequestNumber,
    /// The decoded reply, shared with the transient cache: a continuation
    /// clones this handle instead of the text.
    pub text: Arc<str>,
    pub requests_tools: bool,
    pub recorded_at: Timestamp,
}

pub(crate) struct ProviderResponsesPage {
    pub responses: Vec<ProviderResponse>,
    pub has_more: bool,
}

/// A decoded response body. Messages are content-addressed and immutable, so
/// the cache needs no invalidation; a missing message is still reported from
/// the `MESSAGES` table before the cache is consulted.
#[derive(Clone)]
struct DecodedMessage {
    text: Arc<str>,
    requests_tools: bool,
    escaped_len: usize,
}

/// The cache holds decoded replies so a page continuation does not parse the
/// whole reply again. It is transient: an empty cache (restart, eviction)
/// costs one parse and then behaves the same.
const MESSAGE_CACHE_BYTES: usize = 8 * 1024 * 1024;
const MESSAGE_CACHE_ENTRIES: usize = 1_024;

#[derive(Default)]
struct MessageCache {
    /// Digest -> (last use, decoded message).
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

/// Decodes `body` once per digest. A reply larger than the cache budget is
/// held alone so paging it never re-parses it; the next message evicts it.
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
    let decoded = DecodedMessage {
        escaped_len: text.chars().map(super::escaped_json_char_cost).sum(),
        text: Arc::from(text),
        requests_tools: output.is_some_and(|items| {
            items.iter().any(|item| {
                item["type"] == "function_call"
                    || item["content"]
                        .as_array()
                        .is_some_and(|parts| parts.iter().any(|part| part["type"] == "tool_use"))
            })
        }) || message
            .get("tool_calls")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|calls| !calls.is_empty()),
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
            _ => {}
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
