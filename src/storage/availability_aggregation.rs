//! Incremental aggregation of AvailabilityStat records into request samples
//! and sparse one-minute counts.
//!
//! One batch is prepared from one read snapshot: the progress, at most
//! [`MAX_AGGREGATION_BATCH`] committed records after it, and the stored state
//! of every Job, bucket and progress row the batch touches. The replacements
//! are computed outside the write transaction. The commit re-reads each of
//! those rows, applies the replacements only if every one still holds the
//! bytes the snapshot read, and writes the progress with the batch. A batch
//! that finds its snapshot overtaken declines, so a stale preparation never
//! applies a delta. Raw records are never read for writing or changed.
//!
//! A request's classification comes only from [`RequestEvidence`], the same
//! reducer the pure sample uses, so replay and incremental processing agree.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;

use super::*;
use crate::persistence::{
    AvailabilityAggregationProgress, AvailabilityBatchReport, AvailabilityClassification,
    AvailabilityCounts, ReadSnapshot, RequestEvidence, RequestSampleRecord, minute_of,
};

/// The most committed records one aggregation batch consumes.
pub(crate) const MAX_AGGREGATION_BATCH: usize = 1_000;
/// How many preparations one call makes before it reports contention.
pub(crate) const MAX_AGGREGATION_ATTEMPTS: u8 = 3;
/// The one progress row of the AvailabilityStat stream.
const PROGRESS_KEY: &str = "availability";

/// A request sample as materialized: its Job's frozen target and the evidence
/// its records have added so far.
#[derive(Clone)]
struct SampleRow {
    llm_profile: ProfileId,
    provider: LlmProviderId,
    model: String,
    evidence: RequestEvidence,
}

impl SampleRow {
    fn new(target: &LlmTarget) -> Self {
        Self {
            llm_profile: target.llm_profile,
            provider: target.frozen.provider.id,
            model: target.frozen.config.model.clone(),
            evidence: RequestEvidence::default(),
        }
    }

    fn has_target(&self, target: &LlmTarget) -> bool {
        self.llm_profile == target.llm_profile
            && self.provider == target.frozen.provider.id
            && self.model == target.frozen.config.model
    }

    /// The class this sample is counted in, and the bucket it is counted in.
    /// The bucket is fixed by the sample's lowest record.
    fn counted(&self) -> Result<(AvailabilityClassification, Vec<u8>), StorageError> {
        let sample = self
            .evidence
            .sample()
            .ok_or_else(|| StorageError::corrupted("a stored request sample has no records"))?;
        Ok((
            sample.classification,
            minute_key(self.provider, &self.model, minute_of(sample.observed_at)),
        ))
    }

    fn encode(&self) -> Vec<u8> {
        let mut e = Enc::new(TAG_REQUEST_SAMPLE);
        e.u64(self.llm_profile.get());
        e.u64(self.provider.get());
        e.str(&self.model);
        let evidence = &self.evidence;
        e.opt(&evidence.first, |e, (id, observed_at)| {
            e.u64(id.get());
            e.ts(*observed_at);
        });
        e.opt(&evidence.observed_status, |e, status| e.u16(*status));
        e.opt(&evidence.supplied_status, |e, status| e.u16(*status));
        e.opt(&evidence.finish, |e, finish| e.u8(finish.code()));
        e.bool(evidence.witness);
        e.bool(evidence.contradictory);
        e.0
    }

    fn decode(job: JobId, bytes: &[u8]) -> DecodeResult<Self> {
        let mut d = Dec::new(bytes, TAG_REQUEST_SAMPLE)?;
        let row = Self {
            llm_profile: ProfileId::new(d.u64()?),
            provider: LlmProviderId::new(d.u64()?),
            model: d.str()?,
            evidence: RequestEvidence {
                job: Some(job),
                first: d.opt(|d| Ok((AvailabilityStatId::new(d.u64()?), d.ts()?)))?,
                observed_status: d.opt(|d| d.u16())?,
                supplied_status: d.opt(|d| d.u16())?,
                finish: d.opt(|d| d.code(ChatFinishReason::from_code))?,
                witness: d.bool()?,
                contradictory: d.bool()?,
            },
        };
        d.end()?;
        Ok(row)
    }
}

/// The bucket key: the Provider, then the minute with its sign bit flipped so
/// negative minutes sort first, then the exact, case-sensitive model name.
pub(super) fn minute_key(provider: LlmProviderId, model: &str, minute: i64) -> Vec<u8> {
    let mut key = Vec::with_capacity(16 + model.len());
    key.extend_from_slice(&provider.get().to_be_bytes());
    key.extend_from_slice(&((minute as u64) ^ (1 << 63)).to_be_bytes());
    key.extend_from_slice(model.as_bytes());
    key
}

fn encode_counts(counts: &AvailabilityCounts) -> Vec<u8> {
    let mut e = Enc::new(TAG_MINUTE_COUNTS);
    for count in [
        counts.available,
        counts.provider_server_error,
        counts.rate_limited,
        counts.auth_or_permission,
        counts.request_or_target,
        counts.unconfirmed,
    ] {
        e.u64(count);
    }
    e.0
}

fn decode_counts(bytes: &[u8]) -> DecodeResult<AvailabilityCounts> {
    let mut d = Dec::new(bytes, TAG_MINUTE_COUNTS)?;
    let counts = AvailabilityCounts {
        available: d.u64()?,
        provider_server_error: d.u64()?,
        rate_limited: d.u64()?,
        auth_or_permission: d.u64()?,
        request_or_target: d.u64()?,
        unconfirmed: d.u64()?,
    };
    d.end()?;
    Ok(counts)
}

/// The stored progress as (last consumed ID or 0, update time). Nothing stored
/// means nothing consumed.
pub(super) fn decode_progress(bytes: Option<&[u8]>) -> DecodeResult<(u64, Option<Timestamp>)> {
    let Some(bytes) = bytes else {
        return Ok((0, None));
    };
    let mut d = Dec::new(bytes, TAG_AGGREGATION_PROGRESS)?;
    let consumed = d.u64()?;
    let updated_at = d.ts()?;
    d.end()?;
    Ok((consumed, Some(updated_at)))
}

pub(super) fn encode_progress(consumed_through: u64, updated_at: Timestamp) -> Vec<u8> {
    let mut e = Enc::new(TAG_AGGREGATION_PROGRESS);
    e.u64(consumed_through);
    e.ts(updated_at);
    e.0
}

fn progress_in(rx: &ReadTransaction) -> Result<AvailabilityAggregationProgress, StorageError> {
    let bytes = rx
        .open_table(AGGREGATION_PROGRESS)?
        .get(PROGRESS_KEY)?
        .map(|v| v.value().to_vec());
    let (consumed, updated_at) = decode_progress(bytes.as_deref())?;
    Ok(AvailabilityAggregationProgress {
        consumed_through: (consumed != 0).then(|| AvailabilityStatId::new(consumed)),
        updated_at,
    })
}

/// One stored sample as the batch read it, and as the batch leaves it.
struct SampleState {
    expected: Option<Vec<u8>>,
    before: Option<SampleRow>,
    after: SampleRow,
}

fn load_sample(
    rx: &ReadTransaction,
    job: JobId,
    target: &LlmTarget,
) -> Result<SampleState, StorageError> {
    let expected = rx
        .open_table(AVAILABILITY_SAMPLES)?
        .get(job.get())?
        .map(|v| v.value().to_vec());
    let before = match &expected {
        Some(bytes) => Some(SampleRow::decode(job, bytes)?),
        None => None,
    };
    let after = match &before {
        Some(row) if !row.has_target(target) => {
            return Err(StorageError::corrupted(format!(
                "the frozen LLM target of {job} changed"
            )));
        }
        Some(row) => row.clone(),
        None => SampleRow::new(target),
    };
    Ok(SampleState {
        expected,
        before,
        after,
    })
}

/// One bucket's stored counts as the batch read them, and after the batch.
struct BucketState {
    expected: Option<Vec<u8>>,
    counts: AvailabilityCounts,
}

fn load_bucket(rx: &ReadTransaction, key: &[u8]) -> Result<BucketState, StorageError> {
    let expected = rx
        .open_table(AVAILABILITY_MINUTES)?
        .get(key)?
        .map(|v| v.value().to_vec());
    let counts = match &expected {
        Some(bytes) => decode_counts(bytes)?,
        None => AvailabilityCounts::default(),
    };
    Ok(BucketState { expected, counts })
}

/// Moves one request into or out of a bucket, with checked counts.
fn adjust(
    counts: &mut AvailabilityCounts,
    class: AvailabilityClassification,
    add: bool,
) -> Result<(), StorageError> {
    let counter = counts.counter(class);
    let next = if add {
        counter.checked_add(1)
    } else {
        counter.checked_sub(1)
    };
    *counter = next.ok_or_else(|| {
        StorageError::corrupted(
            "an aggregation count would leave its range; the stored state is inconsistent",
        )
    })?;
    Ok(())
}

struct SampleWrite {
    job: JobId,
    expected: Option<Vec<u8>>,
    row: Vec<u8>,
    /// Whether the row differs from `expected`. An unchanged row is still
    /// guarded by `expected` at the commit but is not written.
    changed: bool,
}

struct BucketWrite {
    key: Vec<u8>,
    expected: Option<Vec<u8>>,
    /// `None` removes the bucket, which holds no requests.
    value: Option<Vec<u8>>,
    /// Whether `value` differs from `expected`. An unchanged bucket is still
    /// guarded by `expected` at the commit but is neither written nor removed.
    changed: bool,
}

/// Test probe: the derived-row writes a commit really performed.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct WriteProbe {
    pub(crate) sample_inserts: std::sync::atomic::AtomicUsize,
    pub(crate) bucket_inserts: std::sync::atomic::AtomicUsize,
    pub(crate) bucket_removes: std::sync::atomic::AtomicUsize,
}

/// One batch, prepared from one snapshot and ready to commit. The commit
/// applies it only if the rows it read are still the same bytes.
pub(crate) struct AvailabilityPlan {
    expected_progress: Option<Vec<u8>>,
    /// The progress after the batch. It equals the progress read when nothing
    /// was consumed.
    consumed_through: u64,
    consumed_records: usize,
    samples: Vec<SampleWrite>,
    buckets: Vec<BucketWrite>,
    /// Test fault: fail the commit after its samples and buckets are written
    /// and before its progress is, so the rollback can be observed.
    #[cfg(test)]
    pub(crate) inject_failure: bool,
    /// Test probe: counts the sample and bucket writes the commit performs.
    #[cfg(test)]
    pub(crate) probe: Option<std::sync::Arc<WriteProbe>>,
}

impl AvailabilityPlan {
    pub(crate) fn consumed_through(&self) -> Option<AvailabilityStatId> {
        (self.consumed_through != 0).then(|| AvailabilityStatId::new(self.consumed_through))
    }

    /// What the batch consumed and writes, when it consumes anything.
    pub(crate) fn report(&self, attempts: u8) -> Option<AvailabilityBatchReport> {
        if self.consumed_records == 0 {
            return None;
        }
        Some(AvailabilityBatchReport {
            consumed_records: self.consumed_records,
            consumed_through: self.consumed_through()?,
            samples_written: self.samples_written(),
            buckets_written: self.buckets_written(),
            attempts,
        })
    }

    /// The sample rows the commit inserts or replaces: those whose bytes change.
    pub(crate) fn samples_written(&self) -> usize {
        self.samples.iter().filter(|write| write.changed).count()
    }

    /// The bucket rows the commit inserts, replaces or removes: those whose
    /// bytes change.
    pub(crate) fn buckets_written(&self) -> usize {
        self.buckets.iter().filter(|write| write.changed).count()
    }

    /// The bytes this plan writes, for the write-space check. It counts every
    /// guarded row, changed or not, so the allowance stays an upper bound.
    pub(crate) fn encoded_bytes(&self) -> u64 {
        let samples = self
            .samples
            .iter()
            .map(|write| write.row.len() + write.expected.as_ref().map_or(0, Vec::len));
        let buckets = self.buckets.iter().map(|write| {
            write.value.as_ref().map_or(0, Vec::len) + write.expected.as_ref().map_or(0, Vec::len)
        });
        u64::try_from(samples.chain(buckets).sum::<usize>()).unwrap_or(u64::MAX)
    }
}

/// Prepares the next batch from one read snapshot. An empty plan means no
/// committed record is unconsumed; the caller then writes nothing.
pub(crate) fn prepare_availability_aggregation(
    database: &Database,
    limit: usize,
) -> Result<AvailabilityPlan, StorageError> {
    let limit = limit.clamp(1, MAX_AGGREGATION_BATCH);
    let rx = read(database)?;
    let expected_progress = rx
        .open_table(AGGREGATION_PROGRESS)?
        .get(PROGRESS_KEY)?
        .map(|v| v.value().to_vec());
    let (consumed, _) = decode_progress(expected_progress.as_deref())?;
    let ids = unconsumed_ids(&rx, consumed, limit)?;
    let Some(last) = ids.last().copied() else {
        return Ok(AvailabilityPlan {
            expected_progress,
            consumed_through: consumed,
            consumed_records: 0,
            samples: Vec::new(),
            buckets: Vec::new(),
            #[cfg(test)]
            inject_failure: false,
            #[cfg(test)]
            probe: None,
        });
    };

    let mut targets = HashMap::new();
    let mut samples: BTreeMap<JobId, SampleState> = BTreeMap::new();
    for id in &ids {
        let job = observation_job(&rx, OBSERVES, id.get())?;
        let target = job_target(&rx, &mut targets, job)?.ok_or_else(|| {
            StorageError::corrupted(format!(
                "availability record {id} observes {job}, which ran no LLM"
            ))
        })?;
        let record = availability_from(&rx, id.get(), job, Some(target.clone()))?;
        let state = match samples.entry(job) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(load_sample(&rx, job, &target)?),
        };
        state
            .after
            .evidence
            .add(&record)
            .map_err(|error| StorageError::corrupted(error.to_string()))?;
    }

    let mut buckets: BTreeMap<Vec<u8>, BucketState> = BTreeMap::new();
    for state in samples.values() {
        let after = state.after.counted()?;
        let before = state.before.as_ref().map(SampleRow::counted).transpose()?;
        if before.as_ref() == Some(&after) {
            // The same class in the same bucket: this request is already counted.
            continue;
        }
        if let Some((class, key)) = before {
            let bucket = bucket_mut(&rx, &mut buckets, key)?;
            adjust(bucket, class, false)?;
        }
        let (class, key) = after;
        let bucket = bucket_mut(&rx, &mut buckets, key)?;
        adjust(bucket, class, true)?;
    }

    let samples = samples
        .into_iter()
        .map(|(job, state)| {
            let row = state.after.encode();
            SampleWrite {
                job,
                changed: state.expected.as_deref() != Some(row.as_slice()),
                expected: state.expected,
                row,
            }
        })
        .collect();
    let buckets = buckets
        .into_iter()
        .map(|(key, state)| {
            let value = (state.counts != AvailabilityCounts::default())
                .then(|| encode_counts(&state.counts));
            BucketWrite {
                key,
                changed: value != state.expected,
                expected: state.expected,
                value,
            }
        })
        .collect();
    Ok(AvailabilityPlan {
        expected_progress,
        consumed_through: last.get(),
        consumed_records: ids.len(),
        samples,
        buckets,
        #[cfg(test)]
        inject_failure: false,
        #[cfg(test)]
        probe: None,
    })
}

/// The next committed AvailabilityStat IDs after `consumed`, at most `limit`
/// of them, in increasing order. Only records that exist are counted.
fn unconsumed_ids(
    rx: &ReadTransaction,
    consumed: u64,
    limit: usize,
) -> Result<Vec<AvailabilityStatId>, StorageError> {
    let Some(first) = consumed.checked_add(1) else {
        return Ok(Vec::new());
    };
    let table = rx.open_table(AVAILABILITY_STATS)?;
    let mut ids = Vec::new();
    for entry in table.range(first..)?.take(limit) {
        ids.push(AvailabilityStatId::new(entry?.0.value()));
    }
    Ok(ids)
}

fn bucket_mut<'b>(
    rx: &ReadTransaction,
    buckets: &'b mut BTreeMap<Vec<u8>, BucketState>,
    key: Vec<u8>,
) -> Result<&'b mut AvailabilityCounts, StorageError> {
    let state = match buckets.entry(key.clone()) {
        Entry::Occupied(entry) => entry.into_mut(),
        Entry::Vacant(entry) => entry.insert(load_bucket(rx, &key)?),
    };
    Ok(&mut state.counts)
}

/// Commits a prepared batch with its operation marker, which names the last
/// record consumed. Returns the update time when it committed; `None` when
/// the snapshot is stale or the plan consumed no record, and then nothing is
/// written and no marker is made. Rows the batch leaves unchanged are checked
/// but not rewritten; the progress is always written with a non-empty batch.
pub(crate) fn commit_availability_aggregation(
    database: &Database,
    operation: OperationId,
    commit: Commit,
    plan: AvailabilityPlan,
) -> BatchOutcome<Option<Timestamp>> {
    // A plan that consumed no record has nothing to apply, whatever progress
    // it carries: it must not move the update time or make a marker.
    let last = match plan.consumed_through() {
        Some(last) if plan.consumed_records > 0 => last,
        _ => {
            return BatchOutcome {
                result: Ok(None),
                storage_failure: None,
            };
        }
    };
    let subjects = MarkerSubjects::addressed(RecordRef::AvailabilityStat(last));
    run_batch_declinable(
        database,
        operation,
        OperationKind::ConsumeAvailability,
        commit,
        |batch| match apply(batch, plan) {
            Ok(Some(updated_at)) => Ok(Some((updated_at, subjects))),
            Ok(None) => Ok(None),
            Err(error) => Err(BatchError(Failure::Storage(error))),
        },
    )
}

/// Applies a plan inside the batch's transaction, or declines when a row it
/// read has changed.
fn apply(batch: &mut Batch, plan: AvailabilityPlan) -> Result<Option<Timestamp>, StorageError> {
    let tx = &batch.tx;
    let current_progress = tx
        .open_table(AGGREGATION_PROGRESS)?
        .get(PROGRESS_KEY)?
        .map(|v| v.value().to_vec());
    if current_progress != plan.expected_progress {
        return Ok(None);
    }
    {
        let samples = tx.open_table(AVAILABILITY_SAMPLES)?;
        for write in &plan.samples {
            if samples.get(write.job.get())?.map(|v| v.value().to_vec()) != write.expected {
                return Ok(None);
            }
        }
    }
    {
        let buckets = tx.open_table(AVAILABILITY_MINUTES)?;
        for write in &plan.buckets {
            if buckets
                .get(write.key.as_slice())?
                .map(|v| v.value().to_vec())
                != write.expected
            {
                return Ok(None);
            }
        }
    }

    {
        let mut samples = tx.open_table(AVAILABILITY_SAMPLES)?;
        for write in plan.samples.iter().filter(|write| write.changed) {
            samples.insert(write.job.get(), write.row.as_slice())?;
            #[cfg(test)]
            if let Some(probe) = &plan.probe {
                probe
                    .sample_inserts
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }
    }
    {
        let mut buckets = tx.open_table(AVAILABILITY_MINUTES)?;
        for write in plan.buckets.iter().filter(|write| write.changed) {
            match &write.value {
                Some(value) => {
                    buckets.insert(write.key.as_slice(), value.as_slice())?;
                    #[cfg(test)]
                    if let Some(probe) = &plan.probe {
                        probe
                            .bucket_inserts
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }
                }
                None => {
                    buckets.remove(write.key.as_slice())?;
                    #[cfg(test)]
                    if let Some(probe) = &plan.probe {
                        probe
                            .bucket_removes
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }
                }
            }
        }
    }
    #[cfg(test)]
    if plan.inject_failure {
        return Err(StorageError::corrupted(
            "injected failure before the aggregation progress was written",
        ));
    }
    let updated_at = batch.now();
    tx.open_table(AGGREGATION_PROGRESS)?.insert(
        PROGRESS_KEY,
        encode_progress(plan.consumed_through, updated_at).as_slice(),
    )?;
    Ok(Some(updated_at))
}

/// Test fault: replaces one bucket's counts without touching progress, so a
/// prepared plan's guard on that bucket is the only thing that can notice.
#[cfg(test)]
pub(crate) fn replace_minute(
    database: &Database,
    provider: LlmProviderId,
    model: &str,
    minute: i64,
    counts: &AvailabilityCounts,
) -> Result<(), StorageError> {
    let tx = database.begin_write()?;
    tx.open_table(AVAILABILITY_MINUTES)?.insert(
        minute_key(provider, model, minute).as_slice(),
        encode_counts(counts).as_slice(),
    )?;
    tx.commit()?;
    Ok(())
}

/// The aggregation progress, as of one read snapshot.
pub(crate) fn read_availability_progress(
    database: &Database,
) -> Result<AvailabilityAggregationProgress, StorageError> {
    progress_in(&read(database)?)
}

/// The request sample of one Job, with the progress of the same snapshot.
pub(crate) fn read_request_sample(
    database: &Database,
    job: JobId,
) -> Result<ReadSnapshot<Option<RequestSampleRecord>>, StorageError> {
    let rx = read(database)?;
    let progress = progress_in(&rx)?;
    let bytes = rx
        .open_table(AVAILABILITY_SAMPLES)?
        .get(job.get())?
        .map(|v| v.value().to_vec());
    let value = match bytes {
        Some(bytes) => {
            let row = SampleRow::decode(job, &bytes)?;
            let sample = row
                .evidence
                .sample()
                .ok_or_else(|| StorageError::corrupted("a stored request sample has no records"))?;
            Some(RequestSampleRecord {
                job,
                llm_profile: row.llm_profile,
                provider: row.provider,
                model: row.model,
                sample,
            })
        }
        None => None,
    };
    Ok(ReadSnapshot { progress, value })
}

/// The counts of one (Provider, model, minute) bucket, with the progress of
/// the same snapshot. A bucket holding no requests reads as `None`.
pub(crate) fn read_availability_minute(
    database: &Database,
    provider: LlmProviderId,
    model: &str,
    minute: i64,
) -> Result<ReadSnapshot<Option<AvailabilityCounts>>, StorageError> {
    let rx = read(database)?;
    let progress = progress_in(&rx)?;
    let key = minute_key(provider, model, minute);
    let bytes = rx
        .open_table(AVAILABILITY_MINUTES)?
        .get(key.as_slice())?
        .map(|v| v.value().to_vec());
    let value = bytes.as_deref().map(decode_counts).transpose()?;
    Ok(ReadSnapshot { progress, value })
}
