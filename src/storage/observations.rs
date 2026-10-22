//! Reads of the raw request events a Job recorded, and the queries over the
//! `request_scope` index: events by Connection and model.

use std::collections::{HashMap, HashSet};
use std::ops::Bound;

use super::rows::decode_request_event;
use super::*;
use crate::persistence::{
    AvailabilityObservation, AvailabilityStatRecord, MetricsRecord, ObservationCursor,
    ObservationFilter, ObservationPage, ObservedModel, ObservedModelChoice, ObservedModelFilter,
    ObservedModelPage, RequestEventRecord, RequestPayload,
};
use crate::token_usage::TokenAnnotation;

/// The identity of a record within its Job: the request number and the
/// event's number in it.
fn pair(request: RequestNumber, seq: u32) -> u64 {
    (u64::from(request.get()) << 32) | u64::from(seq)
}

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
    if row.kind.is_conversation() {
        return Ok(None);
    }
    let execution = reads::turn_execution(&rx.open_table(JOBS)?, &row)?.0;
    Ok(Some(LlmTarget {
        llm_profile: execution.llm.model.id.into(),
        frozen: execution.llm,
    }))
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
                    id: MetricsId::new(pair(event.request, event.seq)),
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
                        id: MetricsId::new(pair(event.request, event.seq)),
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
        id: AvailabilityStatId::new(pair(event.request, event.seq)),
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

type ScopeKey = (u64, [u8; 32], i64, u64, u32, u32);

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
    let max_provider = filter.provider.map_or(u64::MAX, |provider| provider.get());
    let high = (
        max_provider,
        model.unwrap_or([0xff; 32]),
        i64::MAX,
        u64::MAX,
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
                Bound::Excluded((ak.0, ak.1, i64::MAX, u64::MAX, u32::MAX, u32::MAX))
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
                    Bound::Excluded((key.0, key.1, i64::MAX, u64::MAX, u32::MAX, u32::MAX));
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
        let id = MetricsId::new(pair(event.request, event.seq));
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
    after: Option<&ObservedModel>,
    limit: usize,
    scan_limit: usize,
) -> Result<ObservedModelPage, StorageError> {
    let rx = read(database)?;
    let limit = limit.clamp(1, MAX_PAGE);
    let range = ObservationFilter {
        observed: filter.observed,
        llm_profile: None,
        provider: filter.provider,
        model: None,
    };
    // One entry per (Provider, model) the range holds, with what it holds.
    let mut choices: BTreeMap<ObservedModel, (bool, bool)> = BTreeMap::new();
    let mut groups: HashMap<(u64, [u8; 32]), ObservedModel> = HashMap::new();
    // Groups at or before the caller's cursor, which the page never holds.
    let mut passed: HashSet<(u64, [u8; 32])> = HashSet::new();
    walk_scope(&rx, &range, None, usize::MAX, |key| {
        let group = (key.0, key.1);
        if passed.contains(&group) {
            return Ok(true);
        }
        match groups.get(&group) {
            Some(observed) if choices.get(observed) == Some(&(true, true)) => return Ok(true),
            Some(_) => {}
            None if groups.len() >= scan_limit => return Ok(false),
            None => {}
        }
        let Some(event) = event_at(&rx, key)? else {
            return Ok(true);
        };
        let observed = match groups.get(&group) {
            Some(observed) => observed.clone(),
            None => {
                let Some(target) = frozen_llm_target(&rx, JobId::new(key.3))? else {
                    return Ok(true);
                };
                let observed = ObservedModel {
                    provider: target.frozen.provider.id,
                    model: target.frozen.config.model,
                };
                if after.is_some_and(|after| observed <= *after) {
                    passed.insert(group);
                    return Ok(true);
                }
                groups.insert(group, observed.clone());
                observed
            }
        };
        let flags = choices.entry(observed).or_default();
        match event.payload {
            RequestPayload::Measurement { .. } | RequestPayload::Token { .. } => flags.0 = true,
            RequestPayload::Availability { .. } => flags.1 = true,
            RequestPayload::Sent { .. }
            | RequestPayload::Received { .. }
            | RequestPayload::ApprovedCalls => {}
        }
        Ok(true)
    })?;
    let mut page = Vec::new();
    let mut next = None;
    for (model, (has_metrics, has_availability_stats)) in choices {
        if after.is_some_and(|after| model <= *after) {
            continue;
        }
        if page.len() >= limit {
            next = page
                .last()
                .map(|choice: &ObservedModelChoice| choice.model.clone());
            break;
        }
        page.push(ObservedModelChoice {
            model,
            has_metrics,
            has_availability_stats,
        });
    }
    Ok(ObservedModelPage {
        choices: page,
        next,
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
