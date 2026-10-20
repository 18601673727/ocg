use std::io::{Read, Seek, SeekFrom};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;

use super::*;
use crate::persistence::{
    AvailabilityClassification, LlmProviderId, ObservationCursor, ObservationFilter, RequestNumber,
    TimeRange, Timestamp,
};
use crate::token_usage::{RequestEligibility, UsageFinality};

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

pub(super) fn jobs_query(query: &str) -> Result<(Option<JobId>, usize), Refusal> {
    let mut values = pairs(query)?;
    let before = values
        .remove("before")
        .map(super::routing::job_id)
        .transpose()?;
    let limit = page_limit(values.remove("limit"))?;
    if !values.is_empty() {
        return Err(INVALID_REQUEST);
    }
    Ok((before, limit))
}

fn bounded(shared: &Shared, value: Value) -> Result<Reply, Refusal> {
    if value.to_string().len() > shared.response_bytes {
        return Err(OUTPUT_TOO_LARGE);
    }
    Ok(Reply::ok(value))
}

pub(super) async fn jobs(
    shared: &Shared,
    before: Option<JobId>,
    limit: usize,
) -> Result<Reply, Refusal> {
    let records = shared.database.jobs(before, limit + 1).await?;
    let page = records
        .iter()
        .take(limit)
        .map(job_json)
        .collect::<Result<Vec<_>, _>>()?;
    let more = records.len() > limit;
    let next = more
        .then(|| records.get(limit - 1).map(|record| record.id.to_string()))
        .flatten();
    bounded(
        shared,
        json!({"api_version": API_VERSION, "jobs": page, "next_before": next, "has_more": more}),
    )
}

type CursorParts = (u64, [u8; 32], i64, u64, u32, u32);

fn cursor(text: &str) -> Result<ObservationCursor, Refusal> {
    let bytes = URL_SAFE_NO_PAD.decode(text).map_err(|_| INVALID_REQUEST)?;
    let (provider, model_hash, observed, job, request, seq): CursorParts =
        serde_json::from_slice(&bytes).map_err(|_| INVALID_REQUEST)?;
    Ok(ObservationCursor {
        provider: LlmProviderId::new(provider),
        model_hash,
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
                cursor.provider.get(),
                cursor.model_hash,
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

pub(super) struct ObservationQuery {
    filter: ObservationFilter,
    after: Option<ObservationCursor>,
    limit: usize,
}

impl ObservationQuery {
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

pub(super) async fn usage(shared: &Shared, query: ObservationQuery) -> Result<Reply, Refusal> {
    let page = shared
        .database
        .metrics(query.filter, query.after, query.limit)
        .await?;
    let rows = page.records.iter().map(|record| {
        let measurements = &record.measurements;
        let usage = &measurements.usage;
        json!({
            "id": record.id.to_string(), "job": record.job.to_string(), "request": record.request.get(),
            "observed_at_us": record.observed_at.as_unix_micros(),
            "model": record.target.as_ref().map(|target| &target.frozen.config.model),
            "provider": record.target.as_ref().map(|target| &target.frozen.provider.name),
            "llm_profile": record.target.as_ref().map(|target| target.llm_profile.to_string()),
            "input_tokens": usage.input_tokens, "output_tokens": usage.output_tokens,
            "cache_read_tokens": usage.cache_read_tokens, "cache_write_tokens": usage.cache_write_tokens,
            "reasoning_tokens": usage.reasoning_tokens,
            "ttfb_ms": measurements.ttfb_ms, "finished_in_ms": measurements.request_finished_in_ms,
            "eligibility": record.token.map(|token| match token.eligibility {
                RequestEligibility::Eligible => "eligible", RequestEligibility::Excluded => "excluded",
            }),
            "finality": record.token.map(|token| match token.finality {
                UsageFinality::Final => "final", UsageFinality::Cumulative => "cumulative",
            }),
        })
    }).collect::<Vec<_>>();
    bounded(
        shared,
        json!({"api_version": API_VERSION, "observations": rows, "next_after": next_cursor(page.next)?}),
    )
}

pub(super) async fn tool_stats(shared: &Shared) -> Result<Reply, Refusal> {
    let stats = shared.database.tool_stats().await?;
    let tools = stats
        .into_iter()
        .map(|stat| {
            let finished = stat.completed.saturating_add(stat.failed);
            let duration_us_mean = stat.duration_us_total.checked_div(finished);
            json!({
                "name": stat.name,
                "admitted": stat.admitted,
                "refused_arguments": stat.refused_arguments,
                "completed": stat.completed,
                "failed": stat.failed,
                "open": stat.admitted.saturating_sub(stat.completed).saturating_sub(stat.failed),
                "duration_us_mean": duration_us_mean,
                "duration_us_max": stat.duration_us_max,
            })
        })
        .collect::<Vec<_>>();
    bounded(
        shared,
        json!({
            "api_version": API_VERSION,
            "tools": tools,
        }),
    )
}

pub(super) fn usage_summary_query(
    query: &str,
) -> Result<crate::token_usage::UsageSummaryFilter, Refusal> {
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
    let agent = values
        .remove("agent")
        .map(|value| {
            super::profiles::profile_id(value)
                .map(|id| id.get())
                .or_else(|_| decimal::<u64>(value).ok_or(INVALID_REQUEST))
        })
        .transpose()?;
    if !values.is_empty() {
        return Err(INVALID_REQUEST);
    }
    Ok(crate::token_usage::UsageSummaryFilter {
        from_us,
        until_us,
        agent,
    })
}

pub(super) async fn usage_summary(
    shared: &Shared,
    filter: crate::token_usage::UsageSummaryFilter,
) -> Result<Reply, Refusal> {
    let summary = shared.database.usage_summary(filter).await?;
    let val = json!({
        "api_version": API_VERSION,
        "from_us": summary.from_us,
        "until_us": summary.until_us,
        "complete": summary.complete,
        "resume_from_us": summary.resume_from_us,
        "rows": summary.rows,
    });
    bounded(shared, val)
}

pub(super) async fn availability(
    shared: &Shared,
    query: ObservationQuery,
) -> Result<Reply, Refusal> {
    let page = shared
        .database
        .availability_stats(query.filter, query.after, query.limit)
        .await?;
    let rows = page.records.iter().map(|record| {
        let observation = &record.observation;
        let classification = match observation.classification() {
            AvailabilityClassification::Available => "available",
            AvailabilityClassification::Unavailable(_) => "unavailable",
            AvailabilityClassification::Rejected(_) => "rejected",
            AvailabilityClassification::Unconfirmed => "unconfirmed",
        };
        json!({
            "id": record.id.to_string(), "job": record.job.to_string(), "request": record.request.get(),
            "observed_at_us": record.observed_at.as_unix_micros(),
            "model": record.target.frozen.config.model, "provider": record.target.frozen.provider.name,
            "status_code": observation.status_code,
            "finish_reason": observation.finish_reason.map(|finish| finish.as_openai_str()),
            "classification": classification,
        })
    }).collect::<Vec<_>>();
    bounded(
        shared,
        json!({"api_version": API_VERSION, "observations": rows, "next_after": next_cursor(page.next)?}),
    )
}

pub(super) struct LogQuery {
    file: Option<String>,
    limit: usize,
}

impl LogQuery {
    pub(super) fn parse(query: &str) -> Result<Self, Refusal> {
        let mut values = pairs(query)?;
        let file = values.remove("file").map(str::to_owned);
        if file
            .as_ref()
            .is_some_and(|file| !crate::logger::is_runtime_file_name(file.as_ref()))
        {
            return Err(INVALID_REQUEST);
        }
        let limit = page_limit(values.remove("limit"))?;
        if !values.is_empty() {
            return Err(INVALID_REQUEST);
        }
        Ok(Self { file, limit })
    }
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
    let entries = std::fs::read_dir(directory).map_err(|_| unavailable)?;
    let mut files = std::collections::BTreeSet::new();
    let mut scanned = 0;
    let mut matched = 0;
    for entry in entries {
        scanned += 1;
        if scanned > MAX_SCAN {
            return Err(unavailable);
        }
        let entry = entry.map_err(|_| unavailable)?;
        if crate::logger::is_runtime_file_name(&entry.file_name())
            && entry.file_type().map_err(|_| unavailable)?.is_file()
        {
            matched += 1;
            files.insert(entry.file_name().to_string_lossy().into_owned());
            if files.len() > 100 {
                files.pop_first();
            }
        }
    }
    let selected = query.file.or_else(|| files.last().cloned());
    let mut lines = Vec::new();
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
            .map_err(|_| unavailable)?,
        );
        #[cfg(not(unix))]
        let mut file = std::fs::File::open(&path).map_err(|_| unavailable)?;
        let metadata = file.metadata().map_err(|_| unavailable)?;
        if !metadata.is_file() {
            return Err(NOT_FOUND);
        }
        let start = metadata.len().saturating_sub(MAX_BYTES);
        file.seek(SeekFrom::Start(start)).map_err(|_| unavailable)?;
        let mut bytes = Vec::new();
        file.take(MAX_BYTES)
            .read_to_end(&mut bytes)
            .map_err(|_| unavailable)?;
        let text = String::from_utf8_lossy(&bytes);
        let complete = if start > 0 {
            text.split_once('\n').map_or("", |(_, rest)| rest)
        } else {
            &text
        };
        // The last record may still be being written when the snapshot is read.
        let complete = complete
            .rsplit_once('\n')
            .map_or("", |(complete, _)| complete);
        lines = complete
            .lines()
            .rev()
            .take(query.limit)
            .map(str::to_owned)
            .collect();
        lines.reverse();
        truncated = start > 0 || complete.lines().count() > query.limit;
    }
    Ok(
        json!({"api_version": API_VERSION, "files": files.into_iter().rev().collect::<Vec<_>>(),
        "file": selected, "lines": lines, "truncated": truncated, "files_truncated": matched > 100}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn usage_summary_query_parsing() {
        let filter = usage_summary_query("").expect("empty query");
        assert_eq!(filter.from_us, None);
        assert_eq!(filter.until_us, None);
        assert_eq!(filter.agent, None);

        let filter = usage_summary_query("from_us=100&until_us=200&agent=profile:42")
            .expect("profile query");
        assert_eq!(filter.from_us, Some(100));
        assert_eq!(filter.until_us, Some(200));
        assert_eq!(filter.agent, Some(42));

        let filter = usage_summary_query("agent=42").expect("decimal agent");
        assert_eq!(filter.agent, Some(42));

        assert!(usage_summary_query("from_us=200&until_us=100").is_err());
        assert!(usage_summary_query("model=gpt-4o").is_err());
        assert!(usage_summary_query("unknown=1").is_err());
    }
}
