use super::*;
use redb::ReadableTableMetadata;
use std::num::NonZeroUsize;
use std::ops::Bound;
use std::time::Instant;

fn key(id: u64) -> PendingJobKey {
    PendingJobKey {
        job: JobId::new(id),
    }
}

fn fence(transaction: &ReadTransaction) -> Result<PendingFence, StorageError> {
    let index = transaction.open_table(PENDING)?;
    let high_water = transaction
        .open_table(SEQUENCES)?
        .get(Sequence::Job.key())?
        .map(|v| v.value())
        .unwrap_or(0);
    Ok(PendingFence {
        len: index.len()?,
        high_water: JobId::new(high_water),
        end: index.last()?.map(|entry| key(entry.0.value())),
    })
}

pub(crate) fn read_pending_capture_page(
    database: &Database,
    after: Option<JobId>,
    limit: NonZeroUsize,
) -> Result<Result<PendingCapturePage, PendingReadFault>, StorageError> {
    pending_page(database, after, limit, true)
}

pub(crate) fn read_pending_keys(
    database: &Database,
    after: Option<JobId>,
    limit: NonZeroUsize,
) -> Result<Result<PendingCapturePage, PendingReadFault>, StorageError> {
    pending_page(database, after, limit, false)
}

fn pending_page(
    database: &Database,
    after: Option<JobId>,
    limit: NonZeroUsize,
    _verify: bool,
) -> Result<Result<PendingCapturePage, PendingReadFault>, StorageError> {
    let began = Instant::now();
    let transaction = read(database)?;
    let fence = fence(&transaction)?;
    let index = transaction.open_table(PENDING)?;
    let mut visited = Vec::new();
    let limit = crate::persistence::bounded_page(limit);
    if visited.try_reserve_exact(limit).is_err() {
        return Ok(Err(PendingReadFault::Allocation {
            fence,
            service_time: began.elapsed(),
        }));
    }
    let start = after.map_or(Bound::Unbounded, |job| Bound::Excluded(job.get()));
    let mut entries = index.range((start, Bound::Unbounded))?;
    let mut exhausted = false;
    while visited.len() < limit {
        let Some(entry) = entries.next() else {
            exhausted = true;
            break;
        };
        let key = key(entry?.0.value());
        visited.push(key);
    }
    let last_visited = visited.last().map(|key| key.job).or(after);
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
    let index = transaction.open_table(PENDING)?;
    for entry in index.iter()?.take(limit) {
        keys.push(key(entry?.0.value()));
    }
    let complete = fence.len <= limit as u64;
    let resume_after = keys.last().map(|key| key.job);
    Ok(Ok(PendingSnapshot {
        fence,
        keys,
        complete,
        resume_after,
        service_time: began.elapsed(),
    }))
}
