use std::ops::Bound;

use super::*;
use crate::db::entities::ConfigKind;

pub(crate) fn read_tool_stats(
    database: &Database,
) -> Result<Vec<crate::db::entities::ToolStatRecord>, StorageError> {
    let rx = read(database)?;
    let table = rx.open_table(TOOL_STATS)?;
    let mut stats = Vec::new();
    for entry in table.iter()? {
        let (key, value) = entry?;
        let (admitted, refused_arguments, completed, failed, duration_us_total, duration_us_max) =
            value.value();
        stats.push(crate::db::entities::ToolStatRecord {
            name: key.value().to_owned(),
            admitted,
            refused_arguments,
            completed,
            failed,
            duration_us_total,
            duration_us_max,
        });
    }
    Ok(stats)
}

pub const MAX_USAGE_SCAN: usize = 100_000;

const MAX_USAGE_PAGE_GROUPS: usize = 512;

fn resolve_model_name(
    rx: &ReadTransaction,
    provider: u128,
    model_hash: [u8; 32],
) -> Result<Option<String>, StorageError> {
    let Some(job) = rx
        .open_table(USAGE_MODEL)?
        .get((provider, model_hash))?
        .map(|job| JobId::new(job.value()))
    else {
        return Ok(None);
    };
    {
        let jobs = rx.open_table(JOBS)?;
        let value = jobs
            .get(job.get())?
            .ok_or_else(|| StorageError::corrupted("usage model references a missing Job"))?;
        let row = crate::db::redb::jobs::decode_mutable_job(job.get(), value.value())?;
        let (actual_provider, model) = match row.frozen {
            Some(crate::db::entities::MutableFrozenConfiguration::Generative(frozen)) => {
                (frozen.llm.provider.id.get(), frozen.llm.config.model)
            }
            Some(crate::db::entities::MutableFrozenConfiguration::Decision(frozen)) => {
                (frozen.connection_ref.id.get(), frozen.model.model)
            }
            None => return Err(StorageError::corrupted("usage model has no frozen target")),
        };
        if actual_provider != provider
            || ContentDigest::of(model.as_bytes()).as_bytes() != &model_hash
        {
            return Err(StorageError::corrupted(
                "usage model references a different identity",
            ));
        }
        Ok(Some(model))
    }
}

type UsageGroupKey = (i64, u128, u128, [u8; 32]);

type UsageCounters = (u64, u64, u64, u64, u64, u64, u64);

pub(crate) fn read_usage_summary(
    database: &Database,
    filter: &crate::db::entities::UsageSummaryFilter,
    after: Option<crate::db::entities::UsageCursor>,
) -> Result<crate::db::entities::UsageSummaryResponse, StorageError> {
    let (row_budget, group_budget) = (MAX_USAGE_SCAN, MAX_USAGE_PAGE_GROUPS);
    use crate::db::entities::{USAGE_SUMMARY_HOUR_MICROS as HOUR, UsageSummaryResponse};

    let from_hour = filter
        .from_us
        .map(|us| us.div_euclid(HOUR))
        .unwrap_or(i64::MIN);
    let until_end = match filter.until_us {
        None => None,
        Some(us) => {
            let hour = us.div_euclid(HOUR);
            Some(if us.rem_euclid(HOUR) == 0 {
                hour
            } else {
                hour.checked_add(1).ok_or_else(|| {
                    StorageError::corrupted("usage summary until_us overflows its hour bucket")
                })?
            })
        }
    };
    let until_end_us = match until_end {
        Some(hour) => Some(hour.checked_mul(HOUR).ok_or_else(|| {
            StorageError::corrupted("usage summary until_us overflows its hour bucket")
        })?),
        None => None,
    };
    let until_hour = match until_end {
        Some(hour) => hour - 1,
        None => i64::MAX,
    };

    let requested_from_us = match filter.from_us {
        Some(us) => Some(us.div_euclid(HOUR).checked_mul(HOUR).ok_or_else(|| {
            StorageError::corrupted("usage summary from_us underflows its hour bucket")
        })?),
        None => None,
    };

    if from_hour > until_hour {
        return Ok(UsageSummaryResponse {
            from_us: requested_from_us.unwrap_or(0),
            until_us: until_end_us.unwrap_or(0),
            complete: true,
            resume_after: None,
            rows: Vec::new(),
        });
    }

    let UsageScan {
        rows,
        earliest_hour,
        latest_hour,
        next,
    } = scan_usage_totals(
        database,
        filter,
        from_hour,
        until_hour,
        None,
        after,
        UsageBudget {
            rows: row_budget,
            groups: group_budget,
        },
    )?;
    let rows = rows.into_iter().map(|bucket| bucket.row).collect();

    let effective_from_us = requested_from_us
        .or_else(|| earliest_hour.map(|hour| hour.saturating_mul(HOUR)))
        .unwrap_or(0);

    let effective_until_us = match until_end_us {
        Some(until) => until,
        None => latest_hour
            .and_then(|hour| hour.checked_add(1))
            .map_or(0, |hour| hour.saturating_mul(HOUR)),
    };

    Ok(UsageSummaryResponse {
        from_us: effective_from_us,
        until_us: effective_until_us,
        complete: next.is_none(),
        resume_after: next,
        rows,
    })
}

pub(crate) fn read_usage_buckets(
    database: &Database,
    filter: &crate::db::entities::UsageSummaryFilter,
    bucketing: &crate::db::entities::UsageBucketing,
    after: Option<crate::db::entities::UsageCursor>,
) -> Result<crate::db::entities::UsageBucketResponse, StorageError> {
    let (row_budget, group_budget) = (MAX_USAGE_SCAN, MAX_USAGE_PAGE_GROUPS);
    use crate::db::entities::{USAGE_SUMMARY_HOUR_MICROS as HOUR, UsageBucketResponse};

    if bucketing.offset_minutes % 60 != 0 || !(-840..=840).contains(&bucketing.offset_minutes) {
        return Err(StorageError::corrupted("invalid usage bucket offset"));
    }
    let from_us = filter
        .from_us
        .and_then(|us| bucketing.start_of(us))
        .ok_or_else(|| StorageError::corrupted("usage buckets require a representable from_us"))?;
    let until_us = filter
        .until_us
        .and_then(|us| us.checked_sub(1))
        .and_then(|us| bucketing.end_of(us))
        .ok_or_else(|| StorageError::corrupted("usage buckets require a representable until_us"))?;
    let scan = scan_usage_totals(
        database,
        filter,
        from_us.div_euclid(HOUR),
        until_us.div_euclid(HOUR) - 1,
        Some(bucketing),
        after,
        UsageBudget {
            rows: row_budget,
            groups: group_budget,
        },
    )?;
    Ok(UsageBucketResponse {
        from_us,
        until_us,
        complete: scan.next.is_none(),
        resume_after: scan.next,
        rows: scan.rows,
    })
}

#[derive(Clone, Copy)]
struct UsageBudget {
    rows: usize,
    groups: usize,
}

#[derive(Default)]
struct UsageScan {
    rows: Vec<crate::db::entities::UsageBucketRow>,
    earliest_hour: Option<i64>,
    latest_hour: Option<i64>,
    next: Option<crate::db::entities::UsageCursor>,
}

fn scan_usage_totals(
    database: &Database,
    filter: &crate::db::entities::UsageSummaryFilter,
    from_hour: i64,
    until_hour: i64,
    bucketing: Option<&crate::db::entities::UsageBucketing>,
    after: Option<crate::db::entities::UsageCursor>,
    budget: UsageBudget,
) -> Result<UsageScan, StorageError> {
    use crate::db::entities::{
        USAGE_SUMMARY_HOUR_MICROS as HOUR, UsageBucketRow, UsageCursor, UsageSummaryRow,
    };

    let rx = read(database)?;
    let table = rx.open_table(USAGE_TOTALS)?;
    if from_hour > until_hour || budget.rows == 0 || budget.groups == 0 {
        return Ok(UsageScan::default());
    }
    let range_start = (from_hour, 0, 0, [0u8; 32]);
    let end = (until_hour, u128::MAX, u128::MAX, [0xffu8; 32]);

    let configs = rx.open_table(CONFIGS)?;
    let mut agents = std::collections::HashMap::new();
    let mut matches_agent = |agent: u128| -> Result<bool, StorageError> {
        if filter.agent.is_some_and(|target| agent != target) {
            return Ok(false);
        }
        if !filter.generative_only || agent == 0 {
            return Ok(true);
        }
        if let Some(generative) = agents.get(&agent) {
            return Ok(*generative);
        }
        let bytes = configs
            .get(agent)?
            .ok_or_else(|| StorageError::corrupted("usage Agent missing"))?;
        let generative = match ConfigRow::decode_at(agent, bytes.value())?.kind {
            ConfigKind::Agent => true,
            ConfigKind::DecisionAgent => false,
            ConfigKind::Connection | ConfigKind::Model | ConfigKind::DecisionModel => {
                return Err(StorageError::corrupted(
                    "usage Agent has an incompatible kind",
                ));
            }
        };
        agents.insert(agent, generative);
        Ok(generative)
    };
    let mut find_hour = |rev: bool| -> Result<Option<i64>, StorageError> {
        let mut iter = table.range(range_start..=end)?;
        while let Some(entry) = if rev { iter.next_back() } else { iter.next() } {
            let key = entry?.0.value();
            if matches_agent(key.1)? {
                return Ok(Some(key.0));
            }
        }
        Ok(None)
    };
    let earliest_hour = if filter.from_us.is_none() {
        find_hour(false)?
    } else {
        None
    };
    let latest_hour = if filter.until_us.is_none() {
        find_hour(true)?
    } else {
        None
    };

    let start = match after {
        Some(cursor) => {
            let key = (
                cursor.hour,
                cursor.agent,
                cursor.provider,
                cursor.model_hash,
            );
            if key < range_start {
                Bound::Included(range_start)
            } else {
                Bound::Excluded(key)
            }
        }
        None => Bound::Included(range_start),
    };

    let mut grouped: std::collections::BTreeMap<UsageGroupKey, UsageCounters> =
        std::collections::BTreeMap::new();
    let mut last = None;
    let mut stopped = false;

    for (examined, entry) in table.range((start, Bound::Included(end)))?.enumerate() {
        if examined >= budget.rows {
            stopped = true;
            break;
        }
        let (key, value) = entry?;
        let (hour, agent, provider, model_hash) = key.value();
        let cursor = UsageCursor {
            hour,
            agent,
            provider,
            model_hash,
        };
        if !matches_agent(agent)? {
            last = Some(cursor);
            continue;
        }

        let bucket_us = match bucketing {
            Some(bucketing) => hour
                .checked_mul(HOUR)
                .and_then(|us| bucketing.start_of(us))
                .ok_or_else(|| StorageError::corrupted("usage bucket overflows"))?,
            None => hour,
        };
        let group = (
            if bucketing.is_some() { bucket_us } else { 0 },
            agent,
            provider,
            model_hash,
        );
        if !grouped.contains_key(&group) && grouped.len() >= budget.groups {
            stopped = true;
            break;
        }

        last = Some(cursor);

        let (reqs, comp, inp, out, cr, cw, reas) = value.value();
        let slot = grouped.entry(group).or_insert((0, 0, 0, 0, 0, 0, 0));
        slot.0 = slot.0.saturating_add(reqs);
        slot.1 = slot.1.saturating_add(comp);
        slot.2 = slot.2.saturating_add(inp);
        slot.3 = slot.3.saturating_add(out);
        slot.4 = slot.4.saturating_add(cr);
        slot.5 = slot.5.saturating_add(cw);
        slot.6 = slot.6.saturating_add(reas);
    }

    let next = if stopped {
        match last {
            Some(cursor) => {
                let key = (
                    cursor.hour,
                    cursor.agent,
                    cursor.provider,
                    cursor.model_hash,
                );
                table
                    .range((Bound::Excluded(key), Bound::Included(end)))?
                    .next()
                    .transpose()?
                    .map(|_| cursor)
            }
            None => None,
        }
    } else {
        None
    };

    let mut model_cache: std::collections::HashMap<(u128, [u8; 32]), Option<String>> =
        std::collections::HashMap::new();
    let mut rows = Vec::with_capacity(grouped.len());

    for ((bucket_us, agent, provider, model_hash), (reqs, comp, inp, out, cr, cw, reas)) in grouped
    {
        let model = match model_cache.entry((provider, model_hash)) {
            std::collections::hash_map::Entry::Occupied(e) => e.get().clone(),
            std::collections::hash_map::Entry::Vacant(e) => {
                let resolved = resolve_model_name(&rx, provider, model_hash)?;
                e.insert(resolved.clone());
                resolved
            }
        };
        rows.push(UsageBucketRow {
            bucket_us,
            row: UsageSummaryRow {
                agent,
                provider,
                model,
                requests: reqs,
                complete_requests: comp,
                input_tokens: inp,
                output_tokens: out,
                cache_read_tokens: cr,
                cache_write_tokens: cw,
                reasoning_tokens: reas,
            },
        });
    }

    Ok(UsageScan {
        rows,
        earliest_hour,
        latest_hour,
        next,
    })
}

pub(crate) fn read_usage_retries(
    database: &Database,
    filter: &crate::db::entities::UsageSummaryFilter,
    bucketing: crate::db::entities::UsageBucketing,
    after: Option<(u128, u128)>,
    limit: usize,
) -> Result<UsageRetriesPage, StorageError> {
    let rx = read(database)?;
    let retries = rx.open_table(RETRY_JOBS)?;
    let jobs = rx.open_table(JOBS)?;
    let start = after.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
    let mut entries = retries.range((start, std::ops::Bound::Unbounded))?;
    let mut grouped: BTreeMap<UsageGroupKey, (String, u64)> = BTreeMap::new();
    let mut last = None;
    for _ in 0..limit.clamp(1, 100) {
        let Some(entry) = entries.next() else { break };
        let key = entry?.0.value();
        last = Some(key);
        let retry = jobs
            .get(key.1)?
            .map(|row| crate::db::redb::jobs::decode_mutable_job(key.1, row.value()))
            .transpose()?
            .ok_or_else(|| StorageError::corrupted("retry has no Job row"))?;
        let at = retry.created_at.as_unix_micros();
        if filter.from_us.is_some_and(|from| at < from)
            || filter.until_us.is_some_and(|until| at >= until)
        {
            continue;
        }
        let original = jobs
            .get(key.0)?
            .map(|row| crate::db::redb::jobs::decode_mutable_job(key.0, row.value()))
            .transpose()?
            .ok_or_else(|| StorageError::corrupted("retry names a missing Job"))?;
        crate::db::redb::jobs::validate_retry_source(&retry, &original)?;
        let Some(crate::db::entities::MutableFrozenConfiguration::Generative(frozen)) =
            retry.frozen
        else {
            return Err(StorageError::corrupted(
                "retry has no generative configuration",
            ));
        };
        let agent = frozen.agent_ref.map_or(0, |reference| reference.id.get());
        if filter.agent.is_some_and(|target| agent != target) {
            continue;
        }
        let config = frozen.llm;
        let bucket = bucketing
            .start_of(at)
            .ok_or_else(|| StorageError::corrupted("retry bucket overflows"))?;
        let model = config.config.model;
        let hash = *ContentDigest::of(model.as_bytes()).as_bytes();
        let slot = grouped
            .entry((bucket, agent, config.provider.id.get(), hash))
            .or_insert((model, 0));
        slot.1 += 1;
    }
    let more = entries.next().transpose()?.is_some();
    Ok(UsageRetriesPage {
        rows: grouped
            .into_iter()
            .map(|((bucket_us, agent, provider, _), (model, retries))| {
                crate::db::entities::UsageRetryRow {
                    bucket_us,
                    agent,
                    provider,
                    model: Some(model),
                    retries,
                }
            })
            .collect(),
        next: if more { last } else { None },
    })
}
