use std::io::{Read, Seek, SeekFrom};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;

use super::contracts::*;
use super::*;
use crate::db::ObservationChange;
use crate::db::entities::{
    AvailabilityClassification, LlmProviderId, ObservationCursor, ObservationFilter, RequestNumber,
    TimeRange, Timestamp, UsageCursor,
};
use crate::db::entities::{AvailabilityStatId, AvailabilityStatRecord, MetricsId, MetricsRecord};
use crate::db::entities::{RequestEligibility, UsageFinality};

fn pairs(query: &str) -> Result<std::collections::BTreeMap<&str, &str>, Refusal> {
    let mut values = std::collections::BTreeMap::new();
    if !query.is_empty() {
        for pair in query.split('&') {
            let (key, value) = pair.split_once('=').ok_or(INVALID_REQUEST)?;
            if value.is_empty() || values.insert(key, value).is_some() {
                return Err(INVALID_REQUEST);
            }
        }
    }
    Ok(values)
}

fn page_limit(value: Option<&str>) -> Result<usize, Refusal> {
    value.map_or(Ok(50), |value| {
        decimal(value)
            .filter(|limit| (1..=100).contains(limit))
            .ok_or(INVALID_REQUEST)
    })
}

pub(super) fn jobs_query(query: &str) -> Result<(Option<JobId>, usize, Vec<String>), Refusal> {
    let mut values = pairs(query)?;
    let before = values
        .remove("before")
        .map(super::routing::job_id)
        .transpose()?;
    let limit = page_limit(values.remove("limit"))?;
    let terms = match values.remove("query") {
        Some(query) => catalog::conversation_search_query(&format!("query={query}"))?.0,
        None => Vec::new(),
    };
    if !values.is_empty() {
        return Err(INVALID_REQUEST);
    }
    Ok((before, limit, terms))
}

pub(super) fn bounded(shared: &Shared, value: Value) -> Result<Reply, Refusal> {
    if value.to_string().len() > shared.response_bytes {
        return Err(OUTPUT_TOO_LARGE);
    }
    Ok(Reply::ok(value))
}

pub(super) async fn jobs(
    shared: &Shared,
    before: Option<JobId>,
    limit: usize,
    terms: Vec<String>,
) -> Result<Reply, Refusal> {
    if !terms.is_empty() {
        let page = shared.database.search_jobs(terms, before, limit).await?;
        let jobs = page
            .records
            .iter()
            .map(super::jobs::job_view)
            .collect::<Result<Vec<_>, _>>()?;
        return bounded(
            shared,
            envelope(serde_json::json!(super::jobs::JobsView {
                jobs,
                next_before: page.next_before.map(|id| id.to_string()),
                has_more: page.has_more,
            })),
        );
    }
    let records = shared.database.jobs(before, limit + 1).await?;
    let page = records
        .iter()
        .take(limit)
        .map(super::jobs::job_view)
        .collect::<Result<Vec<_>, _>>()?;
    let more = records.len() > limit;
    let next = more
        .then(|| records.get(limit - 1).map(|record| record.id.to_string()))
        .flatten();
    bounded(
        shared,
        envelope(serde_json::json!(super::jobs::JobsView {
            jobs: page,
            next_before: next,
            has_more: more,
        })),
    )
}

type CursorParts = (i64, u128, u32, u32);

fn cursor(text: &str) -> Result<ObservationCursor, Refusal> {
    let bytes = URL_SAFE_NO_PAD.decode(text).map_err(|_| INVALID_REQUEST)?;
    let (observed, job, request, seq): CursorParts =
        serde_json::from_slice(&bytes).map_err(|_| INVALID_REQUEST)?;
    Ok(ObservationCursor {
        observed_at: Timestamp::from_unix_micros(observed),
        job: JobId::new(job),
        request: RequestNumber::new(request).ok_or(INVALID_REQUEST)?,
        seq,
    })
}

fn next_cursor(cursor: Option<ObservationCursor>) -> Result<Option<String>, Refusal> {
    cursor
        .map(|cursor| {
            let parts: CursorParts = (
                cursor.observed_at.as_unix_micros(),
                cursor.job.get(),
                cursor.request.get(),
                cursor.seq,
            );
            serde_json::to_vec(&parts)
                .map(|bytes| URL_SAFE_NO_PAD.encode(bytes))
                .map_err(|_| INTERNAL)
        })
        .transpose()
}

#[derive(Clone)]
pub(super) struct ObservationQuery {
    filter: ObservationFilter,
    after: Option<ObservationCursor>,
    limit: usize,
}

impl ObservationQuery {
    pub(super) fn page(&self, after: Option<String>) -> Result<Self, Refusal> {
        let mut page = self.clone();
        page.after = after.map(|value| cursor(&value)).transpose()?;
        Ok(page)
    }

    pub(super) fn parse(query: &str) -> Result<Self, Refusal> {
        let mut values = pairs(query)?;
        let from = values
            .remove("from_us")
            .map(|value| decimal::<i64>(value).ok_or(INVALID_REQUEST))
            .transpose()?
            .unwrap_or(0);
        let until = values
            .remove("until_us")
            .map(|value| decimal::<i64>(value).ok_or(INVALID_REQUEST))
            .transpose()?
            .unwrap_or(i64::MAX);
        if from >= until {
            return Err(INVALID_REQUEST);
        }
        let after = values.remove("after").map(cursor).transpose()?;
        let limit = page_limit(values.remove("limit"))?;
        let llm_profile = values
            .remove("llm_profile")
            .map(super::profiles::profile_id)
            .transpose()?;
        if !values.is_empty() {
            return Err(INVALID_REQUEST);
        }
        Ok(Self {
            filter: ObservationFilter {
                observed: TimeRange {
                    from: Timestamp::from_unix_micros(from),
                    until: Timestamp::from_unix_micros(until),
                },
                llm_profile,
                provider: None,
                model: None,
            },
            after,
            limit,
        })
    }
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "UsageObservation")]
pub(crate) struct UsageObservationView {
    llm_profile: Option<String>,
    input_tokens: Option<u32>,
    output_tokens: Option<u32>,
    cache_read_tokens: Option<u32>,
    cache_write_tokens: Option<u32>,
    reasoning_tokens: Option<u32>,
    ttfb_ms: Option<u64>,
    finished_in_ms: Option<u64>,
    eligibility: Option<String>,
    finality: Option<String>,
    id: String,
    job: String,
    request: u32,
    observed_at_us: i64,
    model: Option<String>,
    provider: Option<String>,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "Usage")]
pub(crate) struct UsageView {
    observations: Vec<UsageObservationView>,
    next_after: Option<String>,
}

pub(super) async fn usage(shared: &Shared, query: ObservationQuery) -> Result<Reply, Refusal> {
    let page = shared
        .database
        .metrics(query.filter, query.after, query.limit)
        .await?;
    bounded(
        shared,
        envelope(serde_json::json!(UsageView {
            observations: page.records.iter().map(usage_record).collect(),
            next_after: next_cursor(page.next)?,
        })),
    )
}

fn usage_record(record: &MetricsRecord) -> UsageObservationView {
    let measurements = &record.measurements;
    let usage = &measurements.usage;
    UsageObservationView {
        llm_profile: record
            .target
            .as_ref()
            .map(|target| target.llm_profile.to_string()),
        input_tokens: usage.input_tokens,
        output_tokens: usage.output_tokens,
        cache_read_tokens: usage.cache_read_tokens,
        cache_write_tokens: usage.cache_write_tokens,
        reasoning_tokens: usage.reasoning_tokens,
        ttfb_ms: measurements.ttfb_ms,
        finished_in_ms: measurements.request_finished_in_ms,
        eligibility: record.token.map(|token| {
            match token.eligibility {
                RequestEligibility::Eligible => "eligible",
                RequestEligibility::Excluded => "excluded",
            }
            .to_owned()
        }),
        finality: record.token.map(|token| {
            match token.finality {
                UsageFinality::Final => "final",
                UsageFinality::Cumulative => "cumulative",
            }
            .to_owned()
        }),
        id: record.id.to_string(),
        job: record.job.to_string(),
        request: record.request.get(),
        observed_at_us: record.observed_at.as_unix_micros(),
        model: record
            .target
            .as_ref()
            .map(|target| target.frozen.config.model.clone()),
        provider: record
            .target
            .as_ref()
            .map(|target| target.frozen.provider.name.clone()),
    }
}

pub(super) async fn tool_stats(shared: &Shared) -> Result<Reply, Refusal> {
    let stats = shared.database.tool_stats().await?;
    let tools = stats
        .into_iter()
        .map(|stat| {
            let finished = stat.completed.saturating_add(stat.failed);
            let duration_us_mean = stat.duration_us_total.checked_div(finished);
            serde_json::json!({
                "name": stat.name,
                "admitted": stat.admitted,
                "refused_arguments": stat.refused_arguments,
                "completed": stat.completed,
                "failed": stat.failed,
                "open": stat
                .admitted
                .saturating_sub(stat.completed)
                .saturating_sub(stat.failed),
                "duration_us_mean": duration_us_mean,
                "duration_us_max": stat.duration_us_max,
            })
        })
        .collect::<Vec<_>>();
    bounded(
        shared,
        envelope(serde_json::json!({
            "tools": tools,
        })),
    )
}

const MAX_USAGE_BUCKETS: i64 = 400;

pub(super) struct UsageSummaryQuery {
    filter: crate::db::entities::UsageSummaryFilter,
    bucketing: Option<crate::db::entities::UsageBucketing>,
    after: Option<UsageCursor>,
}

impl UsageSummaryQuery {
    pub(super) fn parse(query: &str) -> Result<Self, Refusal> {
        let mut values = pairs(query)?;
        let generative_only = match values.remove("scope") {
            None | Some("all") => false,
            Some("generative") => true,
            Some(_) => return Err(INVALID_REQUEST),
        };
        let rest = values
            .into_iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join("&");
        let (mut filter, bucketing, after) = usage_summary_query(&rest)?;
        filter.generative_only = generative_only;
        Ok(Self {
            filter,
            bucketing,
            after,
        })
    }
}

pub(super) fn usage_summary_query(
    query: &str,
) -> Result<
    (
        crate::db::entities::UsageSummaryFilter,
        Option<crate::db::entities::UsageBucketing>,
        Option<UsageCursor>,
    ),
    Refusal,
> {
    use crate::db::entities::{
        USAGE_SUMMARY_HOUR_MICROS as HOUR, UsageBucketWidth, UsageBucketing,
    };
    let mut values = pairs(query)?;
    let from_us = values
        .remove("from_us")
        .map(|value| decimal::<i64>(value).ok_or(INVALID_REQUEST))
        .transpose()?;
    let until_us = values
        .remove("until_us")
        .map(|value| decimal::<i64>(value).ok_or(INVALID_REQUEST))
        .transpose()?;
    if let (Some(from), Some(until)) = (from_us, until_us)
        && from >= until
    {
        return Err(INVALID_REQUEST);
    }
    // The bounds are rounded to the hour buckets the summary scans; a bound
    // whose bucket is not an `i64` is refused here rather than overflowing.
    if let Some(from) = from_us
        && from.div_euclid(HOUR).checked_mul(HOUR).is_none()
    {
        return Err(INVALID_REQUEST);
    }
    if let Some(until) = until_us {
        let hour = until.div_euclid(HOUR);
        let end_hour = if until.rem_euclid(HOUR) == 0 {
            Some(hour)
        } else {
            hour.checked_add(1)
        };
        if end_hour.and_then(|hour| hour.checked_mul(HOUR)).is_none() {
            return Err(INVALID_REQUEST);
        }
    }
    let bucket = values
        .remove("bucket")
        .map(|value| match value {
            "day" => Ok(UsageBucketWidth::Day),
            "week" => Ok(UsageBucketWidth::Week),
            _ => Err(INVALID_REQUEST),
        })
        .transpose()?;
    let offset = values
        .remove("tz_offset_minutes")
        .map(|value| {
            let offset = if let Some(value) = value.strip_prefix('-') {
                decimal::<i32>(value)
                    .filter(|offset| *offset != 0)
                    .map(|offset| -offset)
            } else {
                decimal::<i32>(value)
            };
            offset
                .filter(|offset| (-840..=840).contains(offset) && offset % 60 == 0)
                .ok_or(INVALID_REQUEST)
        })
        .transpose()?;
    let bucketing = match bucket {
        Some(width) => {
            let bucketing = UsageBucketing {
                width,
                offset_minutes: offset.unwrap_or(0),
            };
            let from = from_us
                .and_then(|us| bucketing.start_of(us))
                .ok_or(INVALID_REQUEST)?;
            let until = until_us
                .and_then(|us| us.checked_sub(1))
                .and_then(|us| bucketing.end_of(us))
                .ok_or(INVALID_REQUEST)?;
            let width = bucketing
                .end_of(from)
                .and_then(|end| end.checked_sub(from))
                .ok_or(INVALID_REQUEST)?;
            let count = until.checked_sub(from).ok_or(INVALID_REQUEST)? / width;
            if count > MAX_USAGE_BUCKETS {
                return Err(INVALID_REQUEST);
            }
            Some(bucketing)
        }
        None => {
            if offset.is_some() {
                return Err(INVALID_REQUEST);
            }
            None
        }
    };
    let agent = values
        .remove("agent")
        .map(|value| super::profiles::profile_id(value).map(|id| id.get()))
        .transpose()?;
    let after = values.remove("after").map(usage_cursor).transpose()?;
    if !values.is_empty() {
        return Err(INVALID_REQUEST);
    }
    Ok((
        crate::db::entities::UsageSummaryFilter {
            generative_only: false,
            from_us,
            until_us,
            agent,
        },
        bucketing,
        after,
    ))
}

/// Decodes the opaque usage continuation: a source row position.
fn usage_cursor(text: &str) -> Result<UsageCursor, Refusal> {
    let bytes = URL_SAFE_NO_PAD.decode(text).map_err(|_| INVALID_REQUEST)?;
    let (hour, agent, provider, model_hash): (i64, u128, u128, [u8; 32]) =
        serde_json::from_slice(&bytes).map_err(|_| INVALID_REQUEST)?;
    Ok(UsageCursor {
        hour,
        agent,
        provider,
        model_hash,
    })
}

fn usage_next_cursor(cursor: Option<UsageCursor>) -> Result<Option<String>, Refusal> {
    cursor
        .map(|cursor| {
            serde_json::to_vec(&(
                cursor.hour,
                cursor.agent,
                cursor.provider,
                cursor.model_hash,
            ))
            .map(|bytes| URL_SAFE_NO_PAD.encode(bytes))
            .map_err(|_| INVALID_REQUEST)
        })
        .transpose()
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "UsageSummaryRow")]
pub(crate) struct UsageSummaryRowView {
    agent: Option<String>,
    provider: String,
    model: Option<String>,
    requests: u64,
    complete_requests: u64,
    input_tokens: u64,
    output_tokens: u64,
    cache_read_tokens: u64,
    cache_write_tokens: u64,
    reasoning_tokens: u64,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "UsageBucket")]
pub(crate) struct UsageBucketView {
    bucket_us: i64,
    agent: Option<String>,
    provider: String,
    model: Option<String>,
    requests: u64,
    complete_requests: u64,
    input_tokens: u64,
    output_tokens: u64,
    cache_read_tokens: u64,
    cache_write_tokens: u64,
    reasoning_tokens: u64,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "UsageSummary")]
pub(crate) struct UsageSummaryView {
    from_us: i64,
    until_us: i64,
    complete: bool,
    resume_after: Option<String>,
    rows: Vec<UsageSummaryRowView>,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "UsageBuckets")]
pub(crate) struct UsageBucketsView {
    from_us: i64,
    until_us: i64,
    complete: bool,
    resume_after: Option<String>,
    rows: Vec<UsageBucketView>,
    bucket: String,
    tz_offset_minutes: i32,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "UsageRetryRow")]
pub(crate) struct UsageRetryRowView {
    agent: Option<String>,
    provider: String,
    model: Option<String>,
    retries: u64,
    bucket_us: i64,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "UsageRetries")]
pub(crate) struct UsageRetriesView {
    retry_rows: Vec<UsageRetryRowView>,
    next_after: Option<String>,
}

fn usage_summary_row(row: &crate::db::entities::UsageSummaryRow) -> UsageSummaryRowView {
    UsageSummaryRowView {
        agent: (row.agent != 0).then(|| ProfileId::new(row.agent).to_string()),
        provider: LlmProviderId::new(row.provider).to_string(),
        model: row.model.clone(),
        requests: row.requests,
        complete_requests: row.complete_requests,
        input_tokens: row.input_tokens,
        output_tokens: row.output_tokens,
        cache_read_tokens: row.cache_read_tokens,
        cache_write_tokens: row.cache_write_tokens,
        reasoning_tokens: row.reasoning_tokens,
    }
}

fn usage_bucket_row(bucket: &crate::db::entities::UsageBucketRow) -> UsageBucketView {
    let row = usage_summary_row(&bucket.row);
    UsageBucketView {
        bucket_us: bucket.bucket_us,
        agent: row.agent,
        provider: row.provider,
        model: row.model,
        requests: row.requests,
        complete_requests: row.complete_requests,
        input_tokens: row.input_tokens,
        output_tokens: row.output_tokens,
        cache_read_tokens: row.cache_read_tokens,
        cache_write_tokens: row.cache_write_tokens,
        reasoning_tokens: row.reasoning_tokens,
    }
}

fn usage_retry_row(row: &crate::db::entities::UsageRetryRow) -> UsageRetryRowView {
    UsageRetryRowView {
        agent: (row.agent != 0).then(|| ProfileId::new(row.agent).to_string()),
        provider: LlmProviderId::new(row.provider).to_string(),
        model: row.model.clone(),
        retries: row.retries,
        bucket_us: row.bucket_us,
    }
}

pub(super) async fn usage_summary(
    shared: &Shared,
    query: UsageSummaryQuery,
) -> Result<Reply, Refusal> {
    let UsageSummaryQuery {
        filter,
        bucketing,
        after,
    } = query;
    let val = match bucketing {
        None => {
            let summary = shared.database.usage_summary(filter, after).await?;
            envelope(serde_json::json!(UsageSummaryView {
                from_us: summary.from_us,
                until_us: summary.until_us,
                complete: summary.complete,
                resume_after: usage_next_cursor(summary.resume_after)?,
                rows: summary.rows.iter().map(usage_summary_row).collect(),
            }))
        }
        Some(bucketing) => {
            let summary = shared
                .database
                .usage_buckets(filter, bucketing, after)
                .await?;
            let bucket = match bucketing.width {
                crate::db::entities::UsageBucketWidth::Day => "day",
                crate::db::entities::UsageBucketWidth::Week => "week",
            };
            envelope(serde_json::json!(UsageBucketsView {
                from_us: summary.from_us,
                until_us: summary.until_us,
                complete: summary.complete,
                resume_after: usage_next_cursor(summary.resume_after)?,
                rows: summary.rows.iter().map(usage_bucket_row).collect(),
                bucket: bucket.to_owned(),
                tz_offset_minutes: bucketing.offset_minutes,
            }))
        }
    };
    bounded(shared, val)
}

pub(super) struct UsageRetriesQuery {
    filter: crate::db::entities::UsageSummaryFilter,
    bucketing: crate::db::entities::UsageBucketing,
    after: Option<(u128, u128)>,
    limit: usize,
}

impl UsageRetriesQuery {
    pub(super) fn parse(query: &str) -> Result<Self, Refusal> {
        let mut values = pairs(query)?;
        let after = values
            .remove("after")
            .map(|cursor| {
                let (original, retry) = cursor.split_once('.').ok_or(INVALID_REQUEST)?;
                let positive = |value| {
                    crate::db::entities::parse_entity_id(value)
                        .filter(|id| *id > 0)
                        .ok_or(INVALID_REQUEST)
                };
                Ok::<_, Refusal>((positive(original)?, positive(retry)?))
            })
            .transpose()?;
        let limit = page_limit(values.remove("limit"))?;
        let rest = values
            .into_iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join("&");
        let (mut filter, bucketing, _) = usage_summary_query(&rest)?;
        let bucketing = bucketing.ok_or(INVALID_REQUEST)?;
        filter.from_us = filter.from_us.and_then(|us| bucketing.start_of(us));
        filter.until_us = filter
            .until_us
            .and_then(|us| us.checked_sub(1))
            .and_then(|us| bucketing.end_of(us));
        Ok(Self {
            filter,
            bucketing,
            after,
            limit,
        })
    }
}

fn retry_cursor((original, retry): (u128, u128)) -> String {
    format!("{}.{}", JobId::new(original), JobId::new(retry))
}

pub(super) async fn usage_retries(
    shared: &Shared,
    query: UsageRetriesQuery,
) -> Result<Reply, Refusal> {
    let page = shared
        .database
        .usage_retries(query.filter, query.bucketing, query.after, query.limit)
        .await?;
    bounded(
        shared,
        envelope(serde_json::json!(UsageRetriesView {
            retry_rows: page.rows.iter().map(usage_retry_row).collect(),
            next_after: page.next.map(retry_cursor),
        })),
    )
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "AvailabilityObservation")]
pub(crate) struct AvailabilityObservationView {
    status_code: Option<u16>,
    finish_reason: Option<String>,
    classification: String,
    id: String,
    job: String,
    request: u32,
    observed_at_us: i64,
    model: Option<String>,
    provider: Option<String>,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "Availability")]
pub(crate) struct AvailabilityView {
    observations: Vec<AvailabilityObservationView>,
    next_after: Option<String>,
}

pub(super) async fn availability(
    shared: &Shared,
    query: ObservationQuery,
) -> Result<Reply, Refusal> {
    let page = shared
        .database
        .availability_stats(query.filter, query.after, query.limit)
        .await?;
    bounded(
        shared,
        envelope(serde_json::json!(AvailabilityView {
            observations: page.records.iter().map(availability_record).collect(),
            next_after: next_cursor(page.next)?,
        })),
    )
}

fn availability_record(record: &AvailabilityStatRecord) -> AvailabilityObservationView {
    let observation = &record.observation;
    let classification = match observation.classification() {
        AvailabilityClassification::Available => "available",
        AvailabilityClassification::Unavailable(_) => "unavailable",
        AvailabilityClassification::Rejected(_) => "rejected",
        AvailabilityClassification::Unconfirmed => "unconfirmed",
    };
    AvailabilityObservationView {
        status_code: observation.status_code,
        finish_reason: observation
            .finish_reason
            .map(|reason| reason.as_openai_str().to_owned()),
        classification: classification.to_owned(),
        id: record.id.to_string(),
        job: record.job.to_string(),
        request: record.request.get(),
        observed_at_us: record.observed_at.as_unix_micros(),
        model: Some(record.target.frozen.config.model.clone()),
        provider: Some(record.target.frozen.provider.name.clone()),
    }
}

pub(super) async fn changed_observation(
    shared: &Shared,
    change: ObservationChange,
) -> Result<Option<Value>, Refusal> {
    match change {
        ObservationChange::Metrics(job, id) => {
            let Some(previous) = id.get().checked_sub(1) else {
                return Err(INTERNAL);
            };
            let records = shared
                .database
                .job_metrics(job, Some(MetricsId::new(previous)), 1)
                .await?;
            let Some(record) = records.first().filter(|record| record.id == id) else {
                return Ok(None);
            };
            Ok(record
                .target
                .as_ref()
                .map(|_| serde_json::json!(usage_record(record))))
        }
        ObservationChange::Availability(job, id) => {
            let Some(previous) = id.get().checked_sub(1) else {
                return Err(INTERNAL);
            };
            let records = shared
                .database
                .job_availability(job, Some(AvailabilityStatId::new(previous)), 1)
                .await?;
            let Some(record) = records.first().filter(|record| record.id == id) else {
                return Ok(None);
            };
            Ok(Some(serde_json::json!(availability_record(record))))
        }
    }
}

#[derive(Clone)]
pub(super) struct LogQuery {
    file: Option<String>,
    limit: usize,
    before: Option<u64>,
}

impl LogQuery {
    pub(super) fn parse(query: &str) -> Result<Self, Refusal> {
        let mut values = pairs(query)?;
        let file = values.remove("file").map(str::to_owned);
        if file
            .as_ref()
            .is_some_and(|file| !crate::system::logger::is_runtime_file_name(file.as_ref()))
        {
            return Err(INVALID_REQUEST);
        }
        let limit = page_limit(values.remove("limit"))?;
        let before = values
            .remove("before")
            .map(|value| {
                decimal::<u64>(value)
                    .filter(|offset| *offset <= 9_007_199_254_740_991)
                    .ok_or(INVALID_REQUEST)
            })
            .transpose()?;
        if before.is_some() && file.is_none() {
            return Err(INVALID_REQUEST);
        }
        if !values.is_empty() {
            return Err(INVALID_REQUEST);
        }
        Ok(Self {
            file,
            limit,
            before,
        })
    }
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "LogEntry")]
pub(crate) struct LogEntryView {
    offset: u64,
    line: String,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "Logs")]
pub(crate) struct LogsView {
    files: Vec<String>,
    file: Option<String>,
    entries: Vec<LogEntryView>,
    next_before: Option<u64>,
    truncated: bool,
    files_truncated: bool,
}

pub(super) async fn logs(shared: &Shared, query: LogQuery) -> Result<Reply, Refusal> {
    let directory = shared.database.log_directory().to_path_buf();
    let value = ntex::rt::spawn_blocking(move || log_tail(&directory, query))
        .await
        .map_err(|_| INTERNAL)??;
    bounded(shared, value)
}

fn log_tail(directory: &std::path::Path, query: LogQuery) -> Result<Value, Refusal> {
    const MAX_SCAN: usize = 4096;
    const MAX_BYTES: u64 = 64 * 1024;
    let unavailable = Refusal::new(
        StatusCode::SERVICE_UNAVAILABLE,
        "logs_unavailable",
        "runtime logs could not be read",
    );
    let entries = std::fs::read_dir(directory).map_err(|_| unavailable.clone())?;
    let mut files = std::collections::BTreeSet::new();
    let mut scanned = 0;
    let mut matched = 0;
    for entry in entries {
        scanned += 1;
        if scanned > MAX_SCAN {
            return Err(unavailable);
        }
        let entry = entry.map_err(|_| unavailable.clone())?;
        if crate::system::logger::is_runtime_file_name(&entry.file_name())
            && entry
                .file_type()
                .map_err(|_| unavailable.clone())?
                .is_file()
        {
            matched += 1;
            files.insert(entry.file_name().to_string_lossy().into_owned());
            if files.len() > 100 {
                files.pop_first();
            }
        }
    }
    let selected = query.file.or_else(|| files.last().cloned());
    let mut entries = Vec::new();
    let mut next_before = None;
    let mut truncated = false;
    if let Some(name) = &selected {
        if !files.contains(name) {
            return Err(NOT_FOUND);
        }
        let path = directory.join(name);
        #[cfg(unix)]
        let mut file = std::fs::File::from(
            rustix::fs::open(
                &path,
                rustix::fs::OFlags::RDONLY
                    | rustix::fs::OFlags::NOFOLLOW
                    | rustix::fs::OFlags::CLOEXEC,
                rustix::fs::Mode::empty(),
            )
            .map_err(|_| unavailable.clone())?,
        );
        #[cfg(not(unix))]
        let mut file = std::fs::File::open(&path).map_err(|_| unavailable.clone())?;
        let metadata = file.metadata().map_err(|_| unavailable.clone())?;
        if !metadata.is_file() {
            return Err(NOT_FOUND);
        }
        let end = query.before.unwrap_or(metadata.len()).min(metadata.len());
        let start = end.saturating_sub(MAX_BYTES);
        file.seek(SeekFrom::Start(start))
            .map_err(|_| unavailable.clone())?;
        let mut bytes = Vec::new();
        file.take(end - start)
            .read_to_end(&mut bytes)
            .map_err(|_| unavailable.clone())?;
        let mut position = if start > 0 {
            bytes
                .iter()
                .position(|byte| *byte == b'\n')
                .map_or(bytes.len(), |index| index + 1)
        } else {
            0
        };
        let mut found = Vec::new();
        while position < bytes.len() {
            let Some(length) = bytes[position..].iter().position(|byte| *byte == b'\n') else {
                break;
            };
            let line = String::from_utf8_lossy(&bytes[position..position + length])
                .trim_end_matches('\r')
                .to_owned();
            found.push((start + position as u64, line));
            position += length + 1;
        }
        let skip = found.len().saturating_sub(query.limit);
        let remaining = &found[skip..];
        next_before = remaining
            .first()
            .map(|(offset, _)| *offset)
            .or((start > 0).then_some(start))
            .filter(|offset| *offset > 0);
        entries = remaining
            .iter()
            .map(|(offset, line)| LogEntryView {
                offset: *offset,
                line: line.clone(),
            })
            .collect();
        truncated = next_before.is_some();
    }
    Ok(envelope(serde_json::json!(LogsView {
        files: files.into_iter().rev().collect(),
        file: selected,
        entries,
        next_before,
        truncated,
        files_truncated: matched > 100,
    })))
}
