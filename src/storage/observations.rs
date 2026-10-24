//! Reads of the raw request events a Job recorded, and the queries over the
//! `request_scope` index: events by Connection and model.

use std::collections::HashMap;
use std::ops::Bound;
use std::sync::{Arc, Mutex, OnceLock};

use super::rows::decode_request_event;
use super::*;
use crate::persistence::{
    AvailabilityObservation, AvailabilityStatRecord, MetricsRecord, ObservationCursor,
    ObservationFilter, ObservationPage, ObservedModel, ObservedModelChoice, ObservedModelCursor,
    ObservedModelFilter, ObservedModelPage, RequestEventRecord, RequestPayload,
};
use crate::token_usage::TokenAnnotation;

pub(super) fn frozen_llm_target(
    rx: &ReadTransaction,
    job: JobId,
) -> Result<Option<LlmTarget>, StorageError> {
    let Some(row) = rx
        .open_table(JOBS)?
        .get(job.get())?
        .map(|bytes| JobRow::decode_at(job.get(), bytes.value()))
        .transpose()?
    else {
        return Ok(None);
    };
    reads::turn_target(rx, &row)
}

fn job_events(rx: &ReadTransaction, job: JobId) -> Result<Vec<RequestEventRecord>, StorageError> {
    let table = rx.open_table(REQUEST_EVENTS)?;
    let mut events = Vec::new();
    for entry in table.range((job.get(), 0, 0)..=(job.get(), u32::MAX, u32::MAX))? {
        let (key, bytes) = entry?;
        let (_, request, seq) = key.value();
        let request = RequestNumber::new(request)
            .ok_or_else(|| StorageError::corrupted("a stored request number is 0"))?;
        events.push(decode_request_event(job, request, seq, bytes.value())?);
    }
    Ok(events)
}

/// The Metrics records of one Job's events. A Token event that directly
/// follows the Measurement event written with it, in the same transaction,
/// is that record's annotation; any other Token event is a record of its
/// own, without measurements.
pub(super) fn metrics_of(
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
pub(super) fn usage_sample_of(
    job: JobId,
    request: RequestNumber,
    target: &Option<LlmTarget>,
    events: &[RequestEventRecord],
) -> Option<crate::token_usage::TokenRequestSample> {
    let records = metrics_of(job, target, events);
    match crate::token_usage::TokenRequestSample::from_metrics(&records) {
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
    let records = metrics_of(job, &target, &job_events(&rx, job)?);
    Ok(records
        .into_iter()
        .filter(|record| after.is_none_or(|after| record.id > after))
        .take(limit.min(MAX_PAGE))
        .collect())
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
    Ok(job_events(&rx, job)?
        .into_iter()
        .filter_map(|event| availability_of(job, &target, event))
        .filter(|record| after.is_none_or(|after| record.id > after))
        .take(limit.min(MAX_PAGE))
        .collect())
}

// ---- queries over the scope index ---------------------------------------------------

type ScopeKey = (u128, [u8; 32], i64, u128, u32, u32);

fn model_hash(model: &str) -> [u8; 32] {
    *ContentDigest::of(model.as_bytes()).as_bytes()
}

fn cursor_key(cursor: &ObservationCursor) -> ScopeKey {
    (
        cursor.provider.get(),
        cursor.model_hash,
        cursor.observed_at.as_unix_micros(),
        cursor.job.get(),
        cursor.request.get(),
        cursor.seq,
    )
}

fn key_cursor((connection, hash, observed_at, job, request, seq): ScopeKey) -> ObservationCursor {
    ObservationCursor {
        provider: LlmProviderId::new(connection),
        model_hash: hash,
        observed_at: Timestamp::from_unix_micros(observed_at),
        job: JobId::new(job),
        request: RequestNumber::new(request).unwrap_or(RequestNumber::FIRST),
        seq,
    }
}

/// Walks the scope entries `filter` can select, in scope order, strictly
/// after `after`. Examines at most `max_scan` entries and passes each
/// entry the time range and the Provider and model constraints allow to
/// `visit`, which returns whether it wants more. Returns the position to
/// continue from, if the walk stopped before the end.
fn walk_scope(
    rx: &ReadTransaction,
    filter: &ObservationFilter,
    after: Option<&ObservationCursor>,
    max_scan: usize,
    mut visit: impl FnMut(ScopeKey) -> Result<bool, StorageError>,
) -> Result<Option<ScopeKey>, StorageError> {
    let table = rx.open_table(REQUEST_SCOPES)?;
    let from_micros = filter.observed.from.as_unix_micros();
    let until_micros = filter.observed.until.as_unix_micros();
    if from_micros >= until_micros || max_scan == 0 {
        return Ok(None);
    }

    let model = filter.model.as_deref().map(model_hash);
    let min_provider = filter.provider.map_or(0, |provider| provider.get());
    let max_provider = filter.provider.map_or(u128::MAX, |provider| provider.get());
    let high = (
        max_provider,
        model.unwrap_or([0xff; 32]),
        i64::MAX,
        u128::MAX,
        u32::MAX,
        u32::MAX,
    );

    let after_key = after.map(cursor_key);
    let mut next_bound: Bound<ScopeKey> = match after_key {
        Some(ak) => {
            if ak >= high {
                return Ok(None);
            }
            if ak.0 < min_provider {
                Bound::Included((min_provider, model.unwrap_or([0; 32]), from_micros, 0, 0, 0))
            } else if ak.2 < from_micros {
                Bound::Included((ak.0, ak.1, from_micros, 0, 0, 0))
            } else if ak.2 >= until_micros {
                Bound::Excluded((ak.0, ak.1, i64::MAX, u128::MAX, u32::MAX, u32::MAX))
            } else {
                Bound::Excluded(ak)
            }
        }
        None => Bound::Included((min_provider, model.unwrap_or([0; 32]), from_micros, 0, 0, 0)),
    };

    let mut examined = 0usize;

    'outer: loop {
        match next_bound {
            Bound::Included(k) if k > high => break 'outer,
            Bound::Excluded(k) if k >= high => break 'outer,
            _ => {}
        }

        let mut range = table.range((next_bound, Bound::Included(high)))?;
        let Some(first_entry) = range.next() else {
            break;
        };
        let mut key = first_entry?.0.value();

        loop {
            if filter.provider.is_some_and(|p| key.0 != p.get()) {
                if key.0 > max_provider {
                    break 'outer;
                }
                next_bound =
                    Bound::Included((min_provider, model.unwrap_or([0; 32]), from_micros, 0, 0, 0));
                break;
            }

            if let Some(target_hash) = model {
                if key.1 < target_hash {
                    next_bound = Bound::Included((key.0, target_hash, from_micros, 0, 0, 0));
                    break;
                } else if key.1 > target_hash {
                    let next_p = match key.0.checked_add(1) {
                        Some(p) => p,
                        None => break 'outer,
                    };
                    if next_p > max_provider {
                        break 'outer;
                    }
                    next_bound = Bound::Included((next_p, target_hash, from_micros, 0, 0, 0));
                    break;
                }
            }

            let observed_at = key.2;
            if observed_at < from_micros {
                next_bound = Bound::Included((key.0, key.1, from_micros, 0, 0, 0));
                break;
            }

            if observed_at >= until_micros {
                next_bound =
                    Bound::Excluded((key.0, key.1, i64::MAX, u128::MAX, u32::MAX, u32::MAX));
                break;
            }

            examined += 1;
            if !visit(key)? || examined >= max_scan {
                return Ok(Some(key));
            }

            match range.next() {
                Some(next_entry) => {
                    key = next_entry?.0.value();
                }
                None => {
                    break 'outer;
                }
            }
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

fn event_at(
    rx: &ReadTransaction,
    key: ScopeKey,
) -> Result<Option<RequestEventRecord>, StorageError> {
    let (_, _, _, job, request, seq) = key;
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

pub(crate) fn read_metrics(
    database: &Database,
    filter: &ObservationFilter,
    cursor: Option<ObservationCursor>,
    limit: usize,
    max_scan: usize,
) -> Result<ObservationPage<MetricsRecord>, StorageError> {
    let rx = read(database)?;
    let limit = limit.clamp(1, MAX_PAGE);
    let mut cache = HashMap::new();
    let mut records = Vec::new();
    let mut jobs: HashMap<JobId, Vec<MetricsRecord>> = HashMap::new();
    let stopped = walk_scope(&rx, filter, cursor.as_ref(), max_scan, |key| {
        let job = JobId::new(key.3);
        if target_matches(&rx, filter, job, &mut cache)?.is_none() {
            return Ok(true);
        }
        let Some(event) = event_at(&rx, key)? else {
            return Ok(true);
        };
        if !matches!(
            event.payload,
            RequestPayload::Measurement { .. } | RequestPayload::Token { .. }
        ) {
            return Ok(true);
        }
        let all = match jobs.get(&job) {
            Some(all) => all,
            None => {
                let target = cache.get(&job).cloned().flatten();
                jobs.entry(job)
                    .or_insert(metrics_of(job, &target, &job_events(&rx, job)?))
            }
        };
        let id = MetricsId::new(event.id.get());
        if let Some(record) = all.iter().find(|record| record.id == id) {
            records.push(record.clone());
        }
        Ok(records.len() < limit)
    })?;
    Ok(ObservationPage {
        records,
        next: stopped.map(key_cursor),
    })
}

pub(crate) fn read_availability_stats(
    database: &Database,
    filter: &ObservationFilter,
    cursor: Option<ObservationCursor>,
    limit: usize,
    max_scan: usize,
) -> Result<ObservationPage<AvailabilityStatRecord>, StorageError> {
    let rx = read(database)?;
    let limit = limit.clamp(1, MAX_PAGE);
    let mut cache = HashMap::new();
    let mut records = Vec::new();
    let stopped = walk_scope(&rx, filter, cursor.as_ref(), max_scan, |key| {
        let job = JobId::new(key.3);
        let Some(target) = target_matches(&rx, filter, job, &mut cache)? else {
            return Ok(true);
        };
        if let Some(event) = event_at(&rx, key)?
            && let Some(record) = availability_of(job, &target, event)
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

pub(crate) fn read_observed_models(
    database: &Database,
    filter: &ObservedModelFilter,
    after: Option<&ObservedModelCursor>,
    limit: usize,
    scan_limit: usize,
) -> Result<ObservedModelPage, StorageError> {
    let rx = read(database)?;
    let limit = limit.clamp(1, MAX_PAGE);
    let table = rx.open_table(REQUEST_SCOPES)?;
    let from = filter.observed.from.as_unix_micros();
    let until = filter.observed.until.as_unix_micros();
    if from >= until || scan_limit == 0 {
        return Ok(ObservedModelPage {
            choices: Vec::new(),
            next: None,
        });
    }
    let min_provider = filter.provider.map_or(0, |provider| provider.get());
    let max_provider = filter.provider.map_or(u128::MAX, |provider| provider.get());
    let high = (
        max_provider,
        [0xff; 32],
        i64::MAX,
        u128::MAX,
        u32::MAX,
        u32::MAX,
    );
    let group_end = |provider, hash| (provider, hash, i64::MAX, u128::MAX, u32::MAX, u32::MAX);
    let group_high = |provider, hash| {
        (
            provider,
            hash,
            until.saturating_sub(1),
            u128::MAX,
            u32::MAX,
            u32::MAX,
        )
    };

    let mut choices = Vec::new();
    let mut examined = 0usize;
    let mut flags = (false, false);
    let (mut current, mut start, mut bound) = match after {
        Some(cursor) => {
            let provider = cursor.model.provider.get();
            let hash = model_hash(&cursor.model.model);
            let key = (
                provider,
                hash,
                cursor.observed_at.as_unix_micros(),
                cursor.job.get(),
                cursor.request.get(),
                cursor.seq,
            );
            if cursor.complete {
                // The pair's choice was returned; continue after its whole group.
                (None, None, Bound::Excluded(group_end(provider, hash)))
            } else {
                // Continue the pair with the flags already seen in it.
                flags = (cursor.has_metrics, cursor.has_availability_stats);
                (
                    Some((provider, hash, cursor.model.clone(), key)),
                    Some(Bound::Excluded(key)),
                    Bound::Excluded(key),
                )
            }
        }
        None => (
            None,
            None,
            Bound::Included((min_provider, [0; 32], from, 0, 0, 0)),
        ),
    };

    loop {
        let resumed = current.is_some();
        let (provider, hash, model, discovered) = match current.take() {
            Some(current) => current,
            None => {
                let mut range = table.range((bound, Bound::Included(high)))?;
                let Some(entry) = range.next() else {
                    break;
                };
                let key = entry?.0.value();
                if key.0 > max_provider {
                    break;
                }
                let (provider, hash) = (key.0, key.1);
                let target = frozen_llm_target(&rx, JobId::new(key.3))?.ok_or_else(|| {
                    StorageError::corrupted("observed request has no frozen model")
                })?;
                flags = (false, false);
                (
                    provider,
                    hash,
                    ObservedModel {
                        provider: LlmProviderId::new(provider),
                        model: target.frozen.config.model,
                    },
                    key,
                )
            }
        };
        let low = start
            .take()
            .unwrap_or(Bound::Included((provider, hash, from, 0, 0, 0)));
        let mut last = None;
        let mut saw_event = resumed;
        let mut complete = true;
        for entry in table.range((low, Bound::Included(group_high(provider, hash))))? {
            if examined >= scan_limit {
                complete = false;
                break;
            }
            let key = entry?.0.value();
            if let Some(event) = event_at(&rx, key)? {
                match event.payload {
                    RequestPayload::Measurement { .. } | RequestPayload::Token { .. } => {
                        flags.0 = true;
                    }
                    RequestPayload::Availability { .. } => flags.1 = true,
                    RequestPayload::Sent { .. }
                    | RequestPayload::Received { .. }
                    | RequestPayload::ApprovedCalls => {}
                }
                saw_event = true;
            }
            examined += 1;
            last = Some(key);
        }
        if !complete {
            let Some(last) = last else {
                bound = Bound::Excluded(group_end(provider, hash));
                continue;
            };
            return Ok(ObservedModelPage {
                choices,
                next: Some(observed_cursor(model, flags, false, last)),
            });
        }
        bound = Bound::Excluded(group_end(provider, hash));
        if !saw_event {
            examined += 1;
            if examined >= scan_limit {
                return Ok(ObservedModelPage {
                    choices,
                    next: Some(observed_cursor(model, flags, true, discovered)),
                });
            }
            continue;
        }
        choices.push(ObservedModelChoice {
            model: model.clone(),
            has_metrics: flags.0,
            has_availability_stats: flags.1,
        });
        if choices.len() >= limit || examined >= scan_limit {
            let Some(last) = last else {
                continue;
            };
            return Ok(ObservedModelPage {
                choices,
                next: Some(observed_cursor(model, flags, true, last)),
            });
        }
    }
    Ok(ObservedModelPage {
        choices,
        next: None,
    })
}

fn observed_cursor(
    model: ObservedModel,
    flags: (bool, bool),
    complete: bool,
    (_, _, observed_at, job, request, seq): ScopeKey,
) -> ObservedModelCursor {
    ObservedModelCursor {
        model,
        has_metrics: flags.0,
        has_availability_stats: flags.1,
        complete,
        observed_at: Timestamp::from_unix_micros(observed_at),
        job: JobId::new(job),
        request: RequestNumber::new(request).unwrap_or(RequestNumber::FIRST),
        seq,
    }
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
    #[cfg(test)]
    parses: HashMap<[u8; 32], usize>,
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
    #[cfg(test)]
    {
        *cache.parses.entry(*digest.as_bytes()).or_default() += 1;
    }
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

#[cfg(test)]
pub(crate) fn message_parse_count(digest: ContentDigest) -> usize {
    lock_message_cache()
        .parses
        .get(digest.as_bytes())
        .copied()
        .unwrap_or(0)
}

#[cfg(test)]
pub(crate) fn message_cache_state() -> (usize, usize) {
    let cache = lock_message_cache();
    (cache.entries.len(), cache.bytes)
}

#[cfg(test)]
pub(crate) fn clear_message_cache() {
    let mut cache = lock_message_cache();
    cache.entries.clear();
    cache.bytes = 0;
    cache.parses.clear();
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
) -> Result<Option<crate::persistence::ProviderRequestRecord>, StorageError> {
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
    Ok(Some(crate::persistence::ProviderRequestRecord {
        messages,
        tools,
        parameters,
        dropped_turns,
        response,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn body(text: &str) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "content": text,
            "tool_calls": [{ "id": "call" }],
        }))
        .expect("JSON")
    }

    #[test]
    fn one_stored_reply_is_decoded_once_across_continuations() {
        let text = "é\"\\\n".repeat(300_000);
        assert!(text.len() > 1024 * 1024);
        let bytes = body(&text);
        let digest = ContentDigest::of(&bytes);

        clear_message_cache();
        let first = decode_message(digest, &bytes).expect("first decode");
        assert_eq!(message_parse_count(digest), 1);
        assert!(first.requests_tools);
        assert_eq!(&*first.text, text);
        assert_eq!(
            first.escaped_len,
            text.chars()
                .map(crate::storage::escaped_json_char_cost)
                .sum::<usize>()
        );
        for _ in 0..4 {
            let again = decode_message(digest, &bytes).expect("cached decode");
            assert!(Arc::ptr_eq(&first.text, &again.text));
            assert_eq!(&*again.text, text);
        }
        assert_eq!(
            message_parse_count(digest),
            1,
            "a continuation reuses the decoded reply"
        );

        // A restart or eviction empties the cache: the next access parses once
        // and the following ones reuse it again.
        clear_message_cache();
        let after_restart = decode_message(digest, &bytes).expect("decode after restart");
        assert_eq!(message_parse_count(digest), 1);
        assert_eq!(&*after_restart.text, text);
        let _ = decode_message(digest, &bytes).expect("cached after restart");
        assert_eq!(message_parse_count(digest), 1);
        clear_message_cache();
    }

    #[test]
    fn the_message_cache_stays_within_its_byte_budget() {
        clear_message_cache();
        let first_text = "a".repeat(5 * 1024 * 1024);
        let first_body = body(&first_text);
        let first = ContentDigest::of(&first_body);
        decode_message(first, &first_body).expect("first");

        let second_text = "b".repeat(5 * 1024 * 1024);
        let second_body = body(&second_text);
        let second = ContentDigest::of(&second_body);
        decode_message(second, &second_body).expect("second");
        assert!(
            message_cache_state().1 <= MESSAGE_CACHE_BYTES,
            "two replies larger than the budget evict down to the budget"
        );

        // One reply larger than the whole budget is held alone so paging it
        // never re-parses it; a later message evicts it.
        let oversized_text = "c".repeat(9 * 1024 * 1024);
        let oversized_body = body(&oversized_text);
        let oversized = ContentDigest::of(&oversized_body);
        decode_message(oversized, &oversized_body).expect("oversized");
        decode_message(oversized, &oversized_body).expect("oversized cached");
        assert_eq!(message_parse_count(oversized), 1);
        clear_message_cache();
    }
}

#[cfg(test)]
mod responses_projection_tests {
    use super::*;

    #[test]
    fn native_responses_output_projects_text_refusals_and_tools() {
        let value = serde_json::json!({"output":[
            {"type":"reasoning","summary":[{"type":"summary_text","text":"private"}]},
            {"type":"message","content":[{"type":"output_text","text":"visible"},{"type":"refusal","refusal":" refusal"}]},
            {"type":"function_call","call_id":"c","name":"f","arguments":"{}"}
        ]});
        let body = serde_json::to_vec(&value).unwrap();
        let decoded = decode_message(ContentDigest::of(&body), &body).unwrap();
        assert_eq!(&*decoded.text, "visible refusal");
        assert!(decoded.requests_tools);
    }
}
