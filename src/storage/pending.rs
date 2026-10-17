use super::*;
use redb::ReadableTableMetadata;
use std::num::NonZeroUsize;
use std::ops::Bound;
use std::time::Instant;

fn key(tuple: (i64, u64)) -> PendingJobKey {
    PendingJobKey {
        created_at: Timestamp::from_unix_micros(tuple.0),
        job: JobId::new(tuple.1),
    }
}

fn fence(transaction: &ReadTransaction) -> Result<PendingFence, StorageError> {
    genesis_id(&transaction.open_table(META)?)?;
    let sequences = transaction.open_table(SEQUENCES)?;
    let high_water = sequences
        .get(Sequence::Job.key())?
        .ok_or_else(|| StorageError::corrupted("missing Job high water"))?
        .value();
    let jobs = transaction.open_table(JOBS)?;
    if high_water == 0
        || jobs
            .last()?
            .is_none_or(|entry| entry.0.value() != high_water)
    {
        return Err(StorageError::corrupted("invalid Job high water"));
    }
    let index = transaction.open_table(PENDING_JOBS)?;
    Ok(PendingFence {
        len: index.len()?,
        high_water: JobId::new(high_water),
        end: index.last()?.map(|entry| key(entry.0.value())),
    })
}

pub(crate) fn read_pending_capture_page(
    database: &Database,
    after: Option<PendingJobKey>,
    limit: NonZeroUsize,
) -> Result<Result<PendingCapturePage, PendingReadFault>, StorageError> {
    pending_page(database, after, limit, true)
}

pub(crate) fn read_pending_keys(
    database: &Database,
    after: Option<PendingJobKey>,
    limit: NonZeroUsize,
) -> Result<Result<PendingCapturePage, PendingReadFault>, StorageError> {
    pending_page(database, after, limit, false)
}

fn pending_page(
    database: &Database,
    after: Option<PendingJobKey>,
    limit: NonZeroUsize,
    verify: bool,
) -> Result<Result<PendingCapturePage, PendingReadFault>, StorageError> {
    let began = Instant::now();
    let transaction = read(database)?;
    let fence = fence(&transaction)?;
    let genesis = genesis_id(&transaction.open_table(META)?)?;
    let index = transaction.open_table(PENDING_JOBS)?;
    let jobs = transaction.open_table(JOBS)?;
    let mut visited = Vec::new();
    let limit = crate::persistence::bounded_page(limit);
    if visited.try_reserve_exact(limit).is_err() {
        return Ok(Err(PendingReadFault::Allocation {
            fence,
            service_time: began.elapsed(),
        }));
    }
    let start = after.map_or(Bound::Unbounded, |key| {
        Bound::Excluded((key.created_at.as_unix_micros(), key.job.get()))
    });
    let mut entries = index.range((start, Bound::Unbounded))?;
    let mut exhausted = false;
    while visited.len() < limit {
        let Some(entry) = entries.next() else {
            exhausted = true;
            break;
        };
        let tuple = entry?.0.value();
        let key = key(tuple);
        if verify {
            let defect = match jobs.get(key.job.get())? {
                None => Some(PendingIndexFault::MissingJob),
                Some(value) => match JobRow::decode(value.value()) {
                    Err(_) => Some(PendingIndexFault::JobDecode),
                    Ok(row) if row.id != key.job => Some(PendingIndexFault::WrongJobIdentity),
                    Ok(row) if row.id == genesis => Some(PendingIndexFault::Genesis),
                    Ok(row) if !row.awaits_start() => Some(PendingIndexFault::NotPending),
                    Ok(row) if row.pending_key() != tuple => Some(PendingIndexFault::KeyMismatch),
                    Ok(_) => None,
                },
            };
            if let Some(kind) = defect {
                return Ok(Err(PendingReadFault::IndexEntry {
                    fence,
                    key,
                    kind,
                    service_time: began.elapsed(),
                }));
            }
        }
        visited.push(key);
    }
    let last_visited = visited.last().copied().or(after);
    Ok(Ok(PendingCapturePage {
        fence,
        page: PendingJobsPage {
            visited,
            last_visited,
            exhausted,
        },
        service_time: began.elapsed(),
    }))
}

pub(crate) fn read_pending_snapshot(
    database: &Database,
    budget: NonZeroUsize,
) -> Result<Result<PendingSnapshot, PendingReadFault>, StorageError> {
    let began = Instant::now();
    let transaction = read(database)?;
    let fence = fence(&transaction)?;
    let limit = budget.get().min(MAX_PENDING_JOBS as usize);
    let mut keys = Vec::new();
    let reserve = usize::try_from(fence.len).unwrap_or(usize::MAX).min(limit);
    if keys.try_reserve_exact(reserve).is_err() {
        return Ok(Err(PendingReadFault::Allocation {
            fence,
            service_time: began.elapsed(),
        }));
    }
    let index = transaction.open_table(PENDING_JOBS)?;
    for entry in index.iter()?.take(limit) {
        keys.push(key(entry?.0.value()));
    }
    let complete = fence.len <= limit as u64;
    let resume_after = keys.last().copied();
    Ok(Ok(PendingSnapshot {
        fence,
        keys,
        complete,
        resume_after,
        service_time: began.elapsed(),
    }))
}
