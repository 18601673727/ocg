//! Incremental aggregation of annotated Metrics records into per-Job Token
//! request state and sparse one-minute totals.
//!
//! It follows the AvailabilityStat aggregation: one batch is prepared from
//! one read snapshot, outside the worker's write transaction, and committed
//! only if every row it read still holds the bytes it read. The Token stream
//! has its own progress, in a row of its own. A batch consumes at most
//! [`MAX_AGGREGATION_BATCH`] actual Metrics records after that progress,
//! whatever their IDs.
//!
//! The per-Job state is the finite evidence of [`TokenEvidence`], the same
//! reducer the pure sample uses; this module only stores it. A record
//! without a Token annotation carries no trusted contract: it advances the
//! progress and nothing else. An Excluded record is kept in the Job's
//! evidence, so a later contradictory claim about the same request is still
//! found, but it makes no sample and no contribution.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;

use super::availability_aggregation::{
    MAX_AGGREGATION_BATCH, decode_progress, encode_progress, minute_key,
};
use super::*;
use crate::persistence::minute_of;
use crate::token_usage::{
    EvidenceParts, FieldEvidence, Finals, RequestEligibility, TokenAggregationProgress,
    TokenBatchReport, TokenEvidence, TokenEvidenceError, TokenMinuteTotals, TokenObservation,
    TokenRequestRecord, TokenRequestSample, TokenSnapshot,
};

/// The one progress row of the Token stream.
const PROGRESS_KEY: &str = "token";

/// A request's identity, frozen with its Job.
#[derive(Clone, PartialEq, Eq)]
struct Identity {
    llm_profile: ProfileId,
    provider: LlmProviderId,
    model: String,
}

impl Identity {
    fn of(target: &LlmTarget) -> Self {
        Self {
            llm_profile: target.llm_profile,
            provider: target.frozen.provider.id,
            model: target.frozen.config.model.clone(),
        }
    }
}

fn encode_request(identity: &Identity, evidence: &TokenEvidence) -> Vec<u8> {
    let parts = evidence.parts();
    let mut e = Enc::new(TAG_TOKEN_REQUEST);
    e.u64(identity.llm_profile.get());
    e.u64(identity.provider.get());
    e.str(&identity.model);
    e.opt(&parts.eligibility, |e, eligibility| {
        e.u8(match eligibility {
            RequestEligibility::Eligible => 1,
            RequestEligibility::Excluded => 2,
        });
    });
    e.opt(&parts.first, |e, (id, observed_at)| {
        e.u64(id.get());
        e.ts(*observed_at);
    });
    for field in &parts.fields {
        e.ou32(field.cumulative_max);
        match field.finals {
            Finals::None => e.u8(0),
            Finals::One(value) => {
                e.u8(1);
                e.u32(value);
            }
            Finals::Conflicted => e.u8(2),
        }
    }
    e.0
}

fn decode_request(job: JobId, bytes: &[u8]) -> DecodeResult<(Identity, TokenEvidence)> {
    let mut d = Dec::new(bytes, TAG_TOKEN_REQUEST)?;
    let identity = Identity {
        llm_profile: ProfileId::new(d.u64()?),
        provider: LlmProviderId::new(d.u64()?),
        model: d.str()?,
    };
    let eligibility = d.opt(|d| match d.u8()? {
        1 => Ok(RequestEligibility::Eligible),
        2 => Ok(RequestEligibility::Excluded),
        other => Err(StorageError::corrupted(format!(
            "unknown Token eligibility code {other}"
        ))),
    })?;
    let first = d.opt(|d| Ok((MetricsId::new(d.u64()?), d.ts()?)))?;
    let mut fields = [FieldEvidence::default(); 5];
    for field in &mut fields {
        field.cumulative_max = d.ou32()?;
        field.finals = match d.u8()? {
            0 => Finals::None,
            1 => Finals::One(d.u32()?),
            2 => Finals::Conflicted,
            other => {
                return Err(StorageError::corrupted(format!(
                    "unknown Token final code {other}"
                )));
            }
        };
    }
    d.end()?;
    // An eligible request has its first record, and an excluded one has none.
    if first.is_some() != (eligibility == Some(RequestEligibility::Eligible)) {
        return Err(StorageError::corrupted(format!(
            "the Token state of {job} disagrees with its eligibility"
        )));
    }
    let evidence = TokenEvidence::from_parts(EvidenceParts {
        job: Some(job),
        eligibility,
        first,
        fields,
    });
    Ok((identity, evidence))
}

fn put_u128(e: &mut Enc, value: u128) {
    e.u64((value >> 64) as u64);
    e.u64(value as u64);
}

fn get_u128(d: &mut Dec<'_>) -> DecodeResult<u128> {
    let high = u128::from(d.u64()?);
    Ok((high << 64) | u128::from(d.u64()?))
}

fn encode_totals(totals: &TokenMinuteTotals) -> Vec<u8> {
    let mut e = Enc::new(TAG_TOKEN_MINUTE);
    for count in [
        totals.requests,
        totals.complete_requests,
        totals.incomplete_requests,
        totals.missing_requests,
    ] {
        e.u64(count);
    }
    for field in [
        &totals.input,
        &totals.output,
        &totals.cache_read,
        &totals.cache_write,
        &totals.reasoning,
    ] {
        put_u128(&mut e, field.final_sum);
        put_u128(&mut e, field.provisional_sum);
        for count in [
            field.final_requests,
            field.provisional_requests,
            field.missing_requests,
            field.conflicted_requests,
        ] {
            e.u64(count);
        }
    }
    let total = &totals.total;
    put_u128(&mut e, total.final_sum);
    put_u128(&mut e, total.provisional_sum);
    for count in [
        total.final_requests,
        total.provisional_requests,
        total.unknown_requests,
    ] {
        e.u64(count);
    }
    e.0
}

fn decode_totals(bytes: &[u8]) -> DecodeResult<TokenMinuteTotals> {
    let mut d = Dec::new(bytes, TAG_TOKEN_MINUTE)?;
    let mut totals = TokenMinuteTotals {
        requests: d.u64()?,
        complete_requests: d.u64()?,
        incomplete_requests: d.u64()?,
        missing_requests: d.u64()?,
        ..TokenMinuteTotals::default()
    };
    for field in [
        &mut totals.input,
        &mut totals.output,
        &mut totals.cache_read,
        &mut totals.cache_write,
        &mut totals.reasoning,
    ] {
        field.final_sum = get_u128(&mut d)?;
        field.provisional_sum = get_u128(&mut d)?;
        field.final_requests = d.u64()?;
        field.provisional_requests = d.u64()?;
        field.missing_requests = d.u64()?;
        field.conflicted_requests = d.u64()?;
    }
    totals.total.final_sum = get_u128(&mut d)?;
    totals.total.provisional_sum = get_u128(&mut d)?;
    totals.total.final_requests = d.u64()?;
    totals.total.provisional_requests = d.u64()?;
    totals.total.unknown_requests = d.u64()?;
    d.end()?;
    Ok(totals)
}

fn progress_in(rx: &ReadTransaction) -> Result<TokenAggregationProgress, StorageError> {
    let bytes = rx
        .open_table(AGGREGATION_PROGRESS)?
        .get(PROGRESS_KEY)?
        .map(|v| v.value().to_vec());
    let (consumed, updated_at) = decode_progress(bytes.as_deref())?;
    Ok(TokenAggregationProgress {
        consumed_through: (consumed != 0).then(|| MetricsId::new(consumed)),
        updated_at,
    })
}

/// One Job's stored state as the batch read it, and as the batch leaves it.
struct RequestState {
    expected: Option<Vec<u8>>,
    identity: Identity,
    before: Option<TokenRequestSample>,
    evidence: TokenEvidence,
}

fn load_request(
    rx: &ReadTransaction,
    job: JobId,
    target: &LlmTarget,
) -> Result<RequestState, StorageError> {
    let expected = rx
        .open_table(TOKEN_REQUESTS)?
        .get(job.get())?
        .map(|v| v.value().to_vec());
    let identity = Identity::of(target);
    let (before, evidence) = match &expected {
        Some(bytes) => {
            let (stored, evidence) = decode_request(job, bytes)?;
            if stored != identity {
                return Err(StorageError::corrupted(format!(
                    "the frozen LLM target of {job} changed"
                )));
            }
            (evidence.sample(), evidence)
        }
        None => (None, TokenEvidence::default()),
    };
    Ok(RequestState {
        expected,
        identity,
        before,
        evidence,
    })
}

/// One bucket's stored totals as the batch read them, and after the batch.
struct BucketState {
    expected: Option<Vec<u8>>,
    totals: TokenMinuteTotals,
}

fn bucket_mut<'b>(
    rx: &ReadTransaction,
    buckets: &'b mut BTreeMap<Vec<u8>, BucketState>,
    key: Vec<u8>,
) -> Result<&'b mut TokenMinuteTotals, StorageError> {
    let state = match buckets.entry(key.clone()) {
        Entry::Occupied(entry) => entry.into_mut(),
        Entry::Vacant(entry) => {
            let expected = rx
                .open_table(TOKEN_MINUTES)?
                .get(key.as_slice())?
                .map(|v| v.value().to_vec());
            let totals = match &expected {
                Some(bytes) => decode_totals(bytes)?,
                None => TokenMinuteTotals::default(),
            };
            entry.insert(BucketState { expected, totals })
        }
    };
    Ok(&mut state.totals)
}

fn out_of_range() -> StorageError {
    StorageError::corrupted(
        "a Token count or sum would leave its range; the stored state is inconsistent",
    )
}

struct RequestWrite {
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
    /// guarded but is neither written nor removed.
    changed: bool,
}

/// Why a batch was refused: a Job's records contradict each other. Nothing
/// was written and the progress did not move, so the same refusal comes
/// again until the raw facts are dealt with; that is outside this module.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TokenRefusal {
    pub(crate) metrics: MetricsId,
    pub(crate) job: JobId,
    pub(crate) error: TokenEvidenceError,
}

/// One batch, prepared from one snapshot and ready to commit.
pub(crate) struct TokenPlan {
    expected_progress: Option<Vec<u8>>,
    /// The progress after the batch; it equals the progress read when no
    /// record was consumed.
    consumed_through: u64,
    consumed_records: usize,
    unannotated_records: usize,
    requests: Vec<RequestWrite>,
    buckets: Vec<BucketWrite>,
    /// Test fault: fail the commit after its rows are written and before the
    /// progress is, so the rollback can be observed.
    #[cfg(test)]
    pub(crate) inject_failure: bool,
    /// Test probe: counts the request and bucket writes the commit performs.
    #[cfg(test)]
    pub(crate) probe: Option<std::sync::Arc<super::availability_aggregation::WriteProbe>>,
}

impl TokenPlan {
    pub(crate) fn consumed_through(&self) -> Option<MetricsId> {
        (self.consumed_through != 0).then(|| MetricsId::new(self.consumed_through))
    }

    /// What the batch consumed and writes, when it consumes anything.
    pub(crate) fn report(&self, attempts: u8) -> Option<TokenBatchReport> {
        if self.consumed_records == 0 {
            return None;
        }
        Some(TokenBatchReport {
            consumed_records: self.consumed_records,
            consumed_through: self.consumed_through()?,
            unannotated_records: self.unannotated_records,
            requests_written: self.requests_written(),
            buckets_written: self.buckets_written(),
            attempts,
        })
    }

    /// The request states the commit inserts or replaces: those whose bytes change.
    pub(crate) fn requests_written(&self) -> usize {
        self.requests.iter().filter(|write| write.changed).count()
    }

    /// The bucket rows the commit inserts, replaces or removes: those whose
    /// bytes change.
    pub(crate) fn buckets_written(&self) -> usize {
        self.buckets.iter().filter(|write| write.changed).count()
    }

    /// The bytes this plan writes, for the write-space check. It counts every
    /// guarded row, changed or not, so the allowance stays an upper bound.
    pub(crate) fn encoded_bytes(&self) -> u64 {
        let requests = self
            .requests
            .iter()
            .map(|write| write.row.len() + write.expected.as_ref().map_or(0, Vec::len));
        let buckets = self.buckets.iter().map(|write| {
            write.value.as_ref().map_or(0, Vec::len) + write.expected.as_ref().map_or(0, Vec::len)
        });
        u64::try_from(requests.chain(buckets).sum::<usize>()).unwrap_or(u64::MAX)
    }
}

/// The next committed Metrics IDs after `consumed`, at most `limit` of them,
/// in increasing order. Only records that exist are counted.
fn unconsumed_ids(
    rx: &ReadTransaction,
    consumed: u64,
    limit: usize,
) -> Result<Vec<u64>, StorageError> {
    let Some(first) = consumed.checked_add(1) else {
        return Ok(Vec::new());
    };
    let table = rx.open_table(METRICS)?;
    let mut ids = Vec::new();
    for entry in table.range(first..)?.take(limit) {
        ids.push(entry?.0.value());
    }
    Ok(ids)
}

/// Prepares the next batch from one read snapshot. An empty plan means no
/// committed record is unconsumed; the caller then writes nothing. A
/// refusal means the batch's records contradict each other.
pub(crate) fn prepare_token_aggregation(
    database: &Database,
    limit: usize,
) -> Result<Result<TokenPlan, TokenRefusal>, StorageError> {
    let limit = limit.clamp(1, MAX_AGGREGATION_BATCH);
    let rx = read(database)?;
    let expected_progress = rx
        .open_table(AGGREGATION_PROGRESS)?
        .get(PROGRESS_KEY)?
        .map(|v| v.value().to_vec());
    let (consumed, _) = decode_progress(expected_progress.as_deref())?;
    let ids = unconsumed_ids(&rx, consumed, limit)?;
    let Some(last) = ids.last().copied() else {
        return Ok(Ok(TokenPlan {
            expected_progress,
            consumed_through: consumed,
            consumed_records: 0,
            unannotated_records: 0,
            requests: Vec::new(),
            buckets: Vec::new(),
            #[cfg(test)]
            inject_failure: false,
            #[cfg(test)]
            probe: None,
        }));
    };

    let mut targets = HashMap::new();
    let mut requests: BTreeMap<JobId, RequestState> = BTreeMap::new();
    let mut unannotated = 0;
    for id in &ids {
        let bytes = rx
            .open_table(METRICS)?
            .get(*id)?
            .ok_or_else(|| StorageError::corrupted(format!("metrics {id} vanished")))?
            .value()
            .to_vec();
        let row = MetricsRow::decode(&bytes)?;
        if row.token.is_none() {
            // No trusted Token contract: it advances the progress and no more.
            unannotated += 1;
            continue;
        }
        let job = observation_job(&rx, MEASURES, *id)?;
        let target = job_target(&rx, &mut targets, job)?.ok_or_else(|| {
            StorageError::corrupted(format!(
                "annotated metrics {id} measures {job}, which ran no LLM"
            ))
        })?;
        let record = MetricsRecord {
            id: row.id,
            job,
            target: None,
            observed_at: row.observed_at,
            measurements: row.measurements,
            token: row.token,
            recorded_at: row.recorded_at,
        };
        let state = match requests.entry(job) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(load_request(&rx, job, &target)?),
        };
        let observation = TokenObservation::from_record(&record)
            .ok_or_else(|| StorageError::corrupted("an annotated record lost its annotation"))?;
        if let Err(error) = state.evidence.add(observation) {
            return Ok(Err(TokenRefusal {
                metrics: record.id,
                job,
                error,
            }));
        }
    }

    let mut buckets: BTreeMap<Vec<u8>, BucketState> = BTreeMap::new();
    for state in requests.values() {
        let after = state.evidence.sample();
        if state.before == after {
            // Duplicate evidence: the contribution is already counted.
            continue;
        }
        let key_of = |sample: &TokenRequestSample| {
            minute_key(
                state.identity.provider,
                &state.identity.model,
                minute_of(sample.observed_at),
            )
        };
        if let Some(before) = &state.before {
            let bucket = bucket_mut(&rx, &mut buckets, key_of(before))?;
            bucket.step(before, false).map_err(|_| out_of_range())?;
        }
        if let Some(after) = &after {
            let bucket = bucket_mut(&rx, &mut buckets, key_of(after))?;
            bucket.step(after, true).map_err(|_| out_of_range())?;
        }
    }

    let requests = requests
        .into_iter()
        .map(|(job, state)| {
            let row = encode_request(&state.identity, &state.evidence);
            RequestWrite {
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
            let value = (state.totals != TokenMinuteTotals::default())
                .then(|| encode_totals(&state.totals));
            BucketWrite {
                key,
                changed: value != state.expected,
                expected: state.expected,
                value,
            }
        })
        .collect();
    Ok(Ok(TokenPlan {
        expected_progress,
        consumed_through: last,
        consumed_records: ids.len(),
        unannotated_records: unannotated,
        requests,
        buckets,
        #[cfg(test)]
        inject_failure: false,
        #[cfg(test)]
        probe: None,
    }))
}

/// Commits a prepared batch with its operation marker, which names the last
/// record consumed. Returns the update time when it committed; `None` when
/// the snapshot is stale or the plan consumed no record, and then nothing is
/// written and no marker is made. Rows the batch leaves unchanged are
/// checked but not rewritten; the progress is always written with a
/// non-empty batch.
pub(crate) fn commit_token_aggregation(
    database: &Database,
    operation: OperationId,
    commit: Commit,
    plan: TokenPlan,
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
    let subjects = MarkerSubjects::addressed(RecordRef::Metrics(last));
    run_batch_declinable(
        database,
        operation,
        OperationKind::ConsumeTokenUsage,
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
fn apply(batch: &mut Batch, plan: TokenPlan) -> Result<Option<Timestamp>, StorageError> {
    let tx = &batch.tx;
    let current_progress = tx
        .open_table(AGGREGATION_PROGRESS)?
        .get(PROGRESS_KEY)?
        .map(|v| v.value().to_vec());
    if current_progress != plan.expected_progress {
        return Ok(None);
    }
    {
        let requests = tx.open_table(TOKEN_REQUESTS)?;
        for write in &plan.requests {
            if requests.get(write.job.get())?.map(|v| v.value().to_vec()) != write.expected {
                return Ok(None);
            }
        }
    }
    {
        let buckets = tx.open_table(TOKEN_MINUTES)?;
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
        let mut requests = tx.open_table(TOKEN_REQUESTS)?;
        for write in plan.requests.iter().filter(|write| write.changed) {
            requests.insert(write.job.get(), write.row.as_slice())?;
            #[cfg(test)]
            if let Some(probe) = &plan.probe {
                probe
                    .sample_inserts
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }
    }
    {
        let mut buckets = tx.open_table(TOKEN_MINUTES)?;
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
            "injected failure before the Token progress was written",
        ));
    }
    let updated_at = batch.now();
    tx.open_table(AGGREGATION_PROGRESS)?.insert(
        PROGRESS_KEY,
        encode_progress(plan.consumed_through, updated_at).as_slice(),
    )?;
    Ok(Some(updated_at))
}

/// Test fault: replaces one bucket's totals without touching progress, so a
/// prepared plan's guard on that bucket is the only thing that can notice.
#[cfg(test)]
pub(crate) fn replace_token_minute(
    database: &Database,
    provider: LlmProviderId,
    model: &str,
    minute: i64,
    totals: &TokenMinuteTotals,
) -> Result<(), StorageError> {
    let tx = database.begin_write()?;
    tx.open_table(TOKEN_MINUTES)?.insert(
        minute_key(provider, model, minute).as_slice(),
        encode_totals(totals).as_slice(),
    )?;
    tx.commit()?;
    Ok(())
}

/// Test fault: rewrites one Job's request state with an extra, valid piece of
/// evidence, without touching progress, so the guard on that row is the only
/// thing that can notice.
#[cfg(test)]
pub(crate) fn replace_token_request(database: &Database, job: JobId) -> Result<(), StorageError> {
    let tx = database.begin_write()?;
    {
        let mut table = tx.open_table(TOKEN_REQUESTS)?;
        let bytes = table
            .get(job.get())?
            .map(|v| v.value().to_vec())
            .ok_or_else(|| StorageError::corrupted("no Token request state to replace"))?;
        let (identity, evidence) = decode_request(job, &bytes)?;
        let mut parts = evidence.parts();
        parts.fields[3].cumulative_max = Some(9);
        let row = encode_request(&identity, &TokenEvidence::from_parts(parts));
        table.insert(job.get(), row.as_slice())?;
    }
    tx.commit()?;
    Ok(())
}

/// The Token progress, as of one read snapshot.
pub(crate) fn read_token_progress(
    database: &Database,
) -> Result<TokenAggregationProgress, StorageError> {
    progress_in(&read(database)?)
}

/// The request state of one Job, with the progress of the same snapshot.
pub(crate) fn read_token_request(
    database: &Database,
    job: JobId,
) -> Result<TokenSnapshot<Option<TokenRequestRecord>>, StorageError> {
    let rx = read(database)?;
    let progress = progress_in(&rx)?;
    let bytes = rx
        .open_table(TOKEN_REQUESTS)?
        .get(job.get())?
        .map(|v| v.value().to_vec());
    let value = match bytes {
        Some(bytes) => {
            let (identity, evidence) = decode_request(job, &bytes)?;
            Some(TokenRequestRecord {
                job,
                llm_profile: identity.llm_profile,
                provider: identity.provider,
                model: identity.model,
                sample: evidence.sample(),
            })
        }
        None => None,
    };
    Ok(TokenSnapshot { progress, value })
}

/// The most minutes one window read looks up.
pub(crate) const MAX_TOKEN_WINDOW: usize = 1_000;

/// The totals of the (Provider, model) buckets that exist in the `count`
/// minutes from `from_minute`, with the progress of the same snapshot. One
/// lookup per minute, at most [`MAX_TOKEN_WINDOW`], so the cost never depends
/// on how much history exists. A minute with no request has no row.
pub(crate) fn read_token_minute_window(
    database: &Database,
    provider: LlmProviderId,
    model: &str,
    from_minute: i64,
    count: usize,
) -> Result<TokenSnapshot<Vec<(i64, TokenMinuteTotals)>>, StorageError> {
    let rx = read(database)?;
    let progress = progress_in(&rx)?;
    let table = rx.open_table(TOKEN_MINUTES)?;
    let mut rows = Vec::new();
    for offset in 0..count.clamp(1, MAX_TOKEN_WINDOW) {
        let Some(minute) = i64::try_from(offset)
            .ok()
            .and_then(|offset| from_minute.checked_add(offset))
        else {
            break;
        };
        let key = minute_key(provider, model, minute);
        if let Some(bytes) = table.get(key.as_slice())? {
            rows.push((minute, decode_totals(bytes.value())?));
        }
    }
    Ok(TokenSnapshot {
        progress,
        value: rows,
    })
}
