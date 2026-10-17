//! The dormant FIFO cohort Scheduler of G3 stage B, milestone M2.
//!
//! Nothing in production constructs a [`Scheduler`] or starts its loop: HTTP
//! and the CLI keep using the legacy runtime, and the activation milestone is
//! the one that starts this component. Building the core here lets its claim,
//! capture and sweep contracts be verified while no production path reaches
//! it.
//!
//! Three boundaries are deliberate and must not be crossed here:
//!
//! - An uncertain claim outcome is recorded and halts claiming, but nothing
//!   settles it. [`DatabaseHandle::reconcile_unexecuted_claim`] stays unwired
//!   until the never-spawned release question is decided.
//! - No external execution capacity is released, because no execution runs:
//!   a grant is recorded and returned, never spawned from here.
//! - A sweep never starts itself. The loop that gates, times and repeats
//!   sweeps is the activation milestone's, so no production path can begin
//!   claiming by constructing one of these.
#![allow(dead_code, reason = "M2 keeps Scheduler dispatch dormant")]

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::num::{NonZeroU32, NonZeroUsize};
use std::time::{Duration, Instant};

use crate::database::{
    CommandOutcome, DatabaseError, DatabaseHandle, SchedulerWakeSubscription, StorageErrorKind,
};
use crate::disk_protection::ProtectionWatch;
use crate::persistence::{
    Block, CapacityScope, ClaimGrant, ClaimRefusal, ClaimRequest, EvalContinuation, EvalRequest,
    ExecutionPoolId, JobId, MAX_PENDING_JOBS, OperationId, PendingCapturePage, PendingFence,
    PendingIndexFault, PendingJobKey, PendingReadFault, Readiness, Timestamp,
};
use crate::provider_execution::CancellationToken;

/// How long the Scheduler waits before retrying a read the read queue refused,
/// and the cap of that backoff. A refusal examines no index entry, so the
/// retry keeps every cursor where it was.
const RETRY_START: Duration = Duration::from_millis(5);
const RETRY_CAP: Duration = Duration::from_millis(500);

/// Amendment 1 §C: sustained Scheduler active-time share, burst credit and
/// prompt overdraft. A sweep starts only when the bucket holds the trigger's
/// threshold; the bucket refills at `RHO` while the Scheduler issues nothing.
const RHO: f64 = 0.5;
const C_MAX: Duration = Duration::from_secs(1);
const C_PROMPT: Duration = Duration::from_secs(1);
/// Anti-spin floor for the earliest-due timer.
const DUE_FLOOR: Duration = Duration::from_millis(5);

fn micros(duration: Duration) -> i64 {
    duration.as_micros().min(i64::MAX as u128) as i64
}

fn credit_threshold(trigger: SweepTrigger) -> i64 {
    match trigger {
        SweepTrigger::Startup | SweepTrigger::Recovery => i64::MIN,
        SweepTrigger::Capacity | SweepTrigger::Due => -micros(C_PROMPT),
        SweepTrigger::Dirty | SweepTrigger::Reconcile => 0,
    }
}

/// What requested the next head sweep. Stronger means a lower gate threshold:
/// `Startup` and `Recovery` always run, `Capacity` and `Due` may overdraw one
/// prompt, `Dirty` and `Reconcile` need a non-negative bucket.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum SweepTrigger {
    Startup,
    Recovery,
    Capacity,
    Due,
    Dirty,
    Reconcile,
}

/// Amendment 1 §C.3: refill-then-debit on every counted request, capped above,
/// never reset by wakes. Only time passage refills; wakes only lower the
/// threshold by strengthening the trigger.
#[derive(Debug)]
struct CreditBucket {
    balance_micros: i64,
    refilled_at: std::time::Instant,
}

impl CreditBucket {
    fn new(now: std::time::Instant) -> Self {
        Self {
            balance_micros: micros(C_MAX),
            refilled_at: now,
        }
    }

    fn refill(&mut self, now: std::time::Instant) {
        if now <= self.refilled_at {
            return;
        }
        let gain = (now.duration_since(self.refilled_at).as_micros() as f64 * RHO) as i64;
        self.balance_micros = (self.balance_micros + gain).min(micros(C_MAX));
        self.refilled_at = now;
    }

    fn debit(&mut self, active: Duration) {
        let now = std::time::Instant::now();
        self.refill(now);
        self.balance_micros -= micros(active);
    }

    fn balance(&mut self) -> i64 {
        self.refill(std::time::Instant::now());
        self.balance_micros
    }
}

/// The fixed parameters of the sweep, each named by the approved contract so a
/// later tuning change has one place to land.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SchedulerLimits {
    /// `L`: the most index entries one capture page examines.
    pub(crate) capture_page: NonZeroUsize,
    /// `CP`: the most candidates one evaluation request covers.
    pub(crate) candidates: NonZeroUsize,
    /// `W`: the work budget of one evaluation request, in B-tree units.
    pub(crate) evaluation_budget: NonZeroU32,
    /// `P_MAX`: the J1 Pending cap. An index above it is swept by streaming
    /// its keys page by page instead of materializing a cohort.
    pub(crate) pending_max: u64,
    /// `R`: how long a completed sweep is left alone before a reconciliation
    /// sweep is started whether or not anything published a wake.
    pub(crate) reconcile: Duration,
}

impl Default for SchedulerLimits {
    fn default() -> Self {
        Self {
            capture_page: NonZeroUsize::new(1_000).expect("a capture page has entries"),
            candidates: NonZeroUsize::new(64).expect("a request has candidates"),
            evaluation_budget: NonZeroU32::new(32_768).expect("a budget has units"),
            pending_max: MAX_PENDING_JOBS,
            reconcile: Duration::from_secs(30),
        }
    }
}

/// The one claim whose persistence outcome is unknown. M2 records it and stops
/// claiming; nothing settles it until the never-spawned release question is
/// decided, so it outlives every sweep.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct UncertainClaim {
    pub(crate) job: JobId,
    pub(crate) operation: OperationId,
}

/// The dormant Scheduler. It owns no durable state: every sweep works from the
/// database's committed facts, and a sweep that is abandoned loses nothing.
/// The credit bucket bounds sustained active time; wakes never reset it.
pub(crate) struct Scheduler {
    database: DatabaseHandle,
    wake: SchedulerWakeSubscription,
    protection: ProtectionWatch,
    shutdown: CancellationToken,
    limits: SchedulerLimits,
    seq: u64,
    credit: CreditBucket,
    /// The one claim whose persistence outcome is unknown. M2 records it and
    /// stops claiming; nothing settles it, so it outlives every sweep.
    uncertain: Option<UncertainClaim>,
}

impl Scheduler {
    pub(crate) fn new(
        database: DatabaseHandle,
        wake: SchedulerWakeSubscription,
        shutdown: CancellationToken,
        limits: SchedulerLimits,
    ) -> Self {
        Self {
            protection: database.watch_disk_protection(),
            database,
            wake,
            shutdown,
            limits,
            seq: 0,
            credit: CreditBucket::new(std::time::Instant::now()),
            uncertain: None,
        }
    }

    /// Current credit after refill, for tests. Wakes never touch it.
    #[cfg(test)]
    pub(crate) fn credit_balance(&mut self) -> i64 {
        self.credit.balance()
    }

    /// Whether an unresolved claim stops this Scheduler from claiming. While
    /// it does, no sweep is started at all.
    pub(crate) fn claims_halted(&self) -> bool {
        self.uncertain.is_some()
    }

    /// The unresolved claim, if an earlier sweep met one.
    pub(crate) fn uncertain(&self) -> Option<&UncertainClaim> {
        self.uncertain.as_ref()
    }

    /// One capture, under the approved error policy: a retryable read refusal
    /// is retried on the same cursor, an entry-level index fault degrades to
    /// the keys-only snapshot, engine or metadata damage halts the sweep, and
    /// a keys-only read that cannot be held in memory falls back to streaming
    /// page by page.
    pub(crate) async fn capture(&self) -> Result<Captured, CaptureFailure> {
        let mut captured = Captured::default();
        let first = match self.capture_page(None, &mut captured.ledger).await {
            CaptureStep::Page(page) => page,
            CaptureStep::Fault(PendingReadFault::IndexEntry { fence, key, kind, .. }) => {
                captured.ledger.alarm(CaptureAlarm::IndexEntry { key, fault: kind });
                return self.snapshot(fence, captured).await;
            }
            CaptureStep::Fault(PendingReadFault::Allocation { fence, .. }) => {
                captured.ledger.alarm(CaptureAlarm::AllocationExhausted);
                return Ok(captured.stream(fence));
            }
            CaptureStep::Fault(PendingReadFault::InvalidFence) => {
                return Err(CaptureFailure::IntegrityHalt(
                    "the Pending index fence is missing or undecodable".to_owned(),
                ));
            }
            CaptureStep::Failed(failure) => return Err(failure),
        };
        let fence = first.fence;
        if fence.len == 0 {
            return Ok(captured.material(Vec::new()));
        }
        if fence.len > self.limits.pending_max {
            captured.ledger.alarm(CaptureAlarm::OverCapacity { len: fence.len });
            return Ok(captured.stream(fence));
        }
        let mut keys = Vec::new();
        if keys.try_reserve_exact(fence.len as usize).is_err() {
            captured.ledger.alarm(CaptureAlarm::AllocationExhausted);
            return Ok(captured.stream(fence));
        }
        let mut page = first;
        loop {
            for key in &page.page.visited {
                // The fence names the last key of the captured snapshot. A key
                // past it was admitted after the snapshot and belongs to the
                // next sweep.
                if fence.end.is_none_or(|end| *key > end) {
                    return Ok(captured.material(keys));
                }
                // A Job allocated after the fence was read. Only its keys can
                // be ordered, so the sweep restarts from the keys-only
                // snapshot rather than trusting the page's Jobs.
                if key.job > fence.high_water {
                    return self.snapshot(fence, captured).await;
                }
                keys.push(*key);
            }
            if page.page.exhausted {
                return Ok(captured.material(keys));
            }
            match self
                .capture_page(page.page.last_visited, &mut captured.ledger)
                .await
            {
                CaptureStep::Page(next) => page = next,
                CaptureStep::Fault(PendingReadFault::IndexEntry { key, kind, .. }) => {
                    captured.ledger.alarm(CaptureAlarm::IndexEntry { key, fault: kind });
                    drop(keys);
                    return self.snapshot(fence, captured).await;
                }
                CaptureStep::Fault(PendingReadFault::Allocation { .. }) => {
                    captured.ledger.alarm(CaptureAlarm::AllocationExhausted);
                    drop(keys);
                    return Ok(captured.stream(fence));
                }
                CaptureStep::Fault(PendingReadFault::InvalidFence) => {
                    return Err(CaptureFailure::IntegrityHalt(
                        "the Pending index fence is missing or undecodable".to_owned(),
                    ));
                }
                CaptureStep::Failed(failure) => return Err(failure),
            }
        }
    }

    /// The keys-only fallback: one read transaction over at most `P_MAX` keys,
    /// reading no Job at all, so a damaged entry surfaces as a per-candidate
    /// verdict instead of stalling the capture.
    async fn snapshot(
        &self,
        fence: PendingFence,
        mut captured: Captured,
    ) -> Result<Captured, CaptureFailure> {
        let Some(budget) = usize::try_from(self.limits.pending_max)
            .ok()
            .and_then(NonZeroUsize::new)
        else {
            captured.ledger.alarm(CaptureAlarm::AllocationExhausted);
            return Ok(captured.stream(fence));
        };
        let read = self
            .retry(|| self.database.pending_snapshot(budget))
            .await;
        match read {
            None => Err(CaptureFailure::Shutdown),
            Some(Err(error)) => Err(classify(&error)),
            Some(Ok(Ok(snapshot))) if snapshot.complete => {
                captured.ledger.service += snapshot.service_time;
                Ok(captured.material(snapshot.keys))
            }
            Some(Ok(Ok(snapshot))) => {
                captured.ledger.service += snapshot.service_time;
                captured.ledger.alarm(CaptureAlarm::OverCapacity {
                    len: snapshot.fence.len,
                });
                Ok(captured.stream(snapshot.fence))
            }
            Some(Ok(Err(PendingReadFault::Allocation { fence, service_time }))) => {
                captured.ledger.service += service_time;
                captured.ledger.alarm(CaptureAlarm::AllocationExhausted);
                Ok(captured.stream(fence))
            }
            // A keys-only read verifies no entry, so this cannot name a
            // damaged one; if it is ever reported, the read contradicted its
            // own contract and nothing may be swept on it.
            Some(Ok(Err(PendingReadFault::IndexEntry { key, kind, .. }))) => Err(
                CaptureFailure::IntegrityHalt(format!(
                    "the keys-only snapshot reported {kind:?} at {key:?}"
                )),
            ),
            Some(Ok(Err(PendingReadFault::InvalidFence))) => Err(CaptureFailure::IntegrityHalt(
                "the Pending index fence is missing or undecodable".to_owned(),
            )),
        }
    }

    /// One capture page, retried while the read queue refuses it. Only the
    /// read worker's service time counts; queue wait and backoff never do.
    async fn capture_page(
        &self,
        after: Option<PendingJobKey>,
        ledger: &mut Ledger,
    ) -> CaptureStep<PendingCapturePage> {
        let read = self
            .retry(|| self.database.pending_capture_page(after, self.limits.capture_page))
            .await;
        match read {
            None => CaptureStep::Failed(CaptureFailure::Shutdown),
            Some(Err(error)) => CaptureStep::Failed(classify(&error)),
            Some(Ok(Ok(page))) => {
                ledger.service += page.service_time;
                CaptureStep::Page(page)
            }
            Some(Ok(Err(fault))) => {
                match &fault {
                    PendingReadFault::IndexEntry { service_time, .. }
                    | PendingReadFault::Allocation { service_time, .. } => {
                        ledger.service += *service_time;
                    }
                    PendingReadFault::InvalidFence => {}
                }
                CaptureStep::Fault(fault)
            }
        }
    }

    /// Runs one read attempt, retrying only while the read queue is full. A
    /// refusal examined nothing, so the caller's cursor is unchanged. `None`
    /// means the process began stopping instead of retrying.
    async fn retry<T, F, Fut>(&self, mut attempt: F) -> Option<Result<T, DatabaseError>>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, DatabaseError>>,
    {
        let mut delay = RETRY_START;
        loop {
            if self.shutdown.is_cancelled() {
                return None;
            }
            match attempt().await {
                Err(DatabaseError::ReadQueueFull) => {
                    self.wait_or_stop(delay).await;
                    delay = (delay * 2).min(RETRY_CAP);
                }
                result => return Some(result),
            }
        }
    }

    /// Sleeps for a backoff, returning at once when the process begins
    /// stopping.
    async fn wait_or_stop(&self, delay: Duration) {
        match ntex::util::select(self.shutdown.cancelled(), ntex::time::sleep(delay)).await {
            ntex::util::Either::Left(()) | ntex::util::Either::Right(()) => {}
        }
    }

    /// One dormant sweep: capture a cohort, walk it with acknowledged cursor,
    /// evaluate in `CP` batches and claim Ready candidates. Fresh claims only;
    /// suspension and resume belong to a later milestone. Grants are recorded
    /// and returned, never spawned. An uncertain claim is recorded, halts all
    /// further claiming and abandons the sweep without settlement.
    ///
    /// Never starts itself and never loops: the caller decides whether the
    /// returned restart flag warrants another sweep.
    pub(crate) async fn sweep_once(&mut self) -> SweepOutcome {
        if let Some(held) = self.uncertain.clone() {
            return SweepOutcome::Abandoned(SweepAbandoned {
                reason: SweepAbandonedReason::UncertainClaim {
                    job: held.job,
                    operation: held.operation,
                },
                ledger: Ledger::default(),
                grants_so_far: Vec::new(),
                earliest_due_so_far: None,
            });
        }
        if self.shutdown.is_cancelled() {
            return SweepOutcome::Abandoned(SweepAbandoned {
                reason: SweepAbandonedReason::Shutdown,
                ledger: Ledger::default(),
                grants_so_far: Vec::new(),
                earliest_due_so_far: None,
            });
        }
        if self.protection.status().lock.is_some() {
            return SweepOutcome::Abandoned(SweepAbandoned {
                reason: SweepAbandonedReason::ProtectionActive,
                ledger: Ledger::default(),
                grants_so_far: Vec::new(),
                earliest_due_so_far: None,
            });
        }
        while self.wake.try_recv().is_ok() {}
        let d0 = self.wake.dirty_generation();
        let c0 = self.wake.capacity_generation();
        self.seq += 1;
        let seq = self.seq;
        let captured = match self.capture().await {
            Ok(captured) => captured,
            Err(CaptureFailure::Shutdown) => {
                return SweepOutcome::Abandoned(SweepAbandoned {
                    reason: SweepAbandonedReason::Shutdown,
                    ledger: Ledger::default(),
                    grants_so_far: Vec::new(),
                    earliest_due_so_far: None,
                });
            }
            Err(CaptureFailure::IntegrityHalt(detail)) => {
                return SweepOutcome::Abandoned(SweepAbandoned {
                    reason: SweepAbandonedReason::IntegrityHalt(detail),
                    ledger: Ledger::default(),
                    grants_so_far: Vec::new(),
                    earliest_due_so_far: None,
                });
            }
            Err(CaptureFailure::StorageBoundary(detail)) => {
                return SweepOutcome::Abandoned(SweepAbandoned {
                    reason: SweepAbandonedReason::StorageBoundary(detail),
                    ledger: Ledger::default(),
                    grants_so_far: Vec::new(),
                    earliest_due_so_far: None,
                });
            }
        };
        let ledger = captured.ledger;
        self.credit.debit(ledger.service);
        let Some(cohort) = captured.cohort else {
            return SweepOutcome::Completed(SweepCompleted {
                seq,
                d0,
                c0,
                grants: Vec::new(),
                earliest_due: None,
                ledger,
                cap_deferred: false,
                restart_wanted: self.wake.dirty_generation() != d0,
            });
        };
        let source = cohort.cursor(&self.limits);
        let outcome = self.run_sweep(seq, d0, c0, source, ledger).await;
        if let SweepOutcome::Abandoned(abandoned) = &outcome
            && let SweepAbandonedReason::UncertainClaim { job, operation } = &abandoned.reason
        {
            self.uncertain = Some(UncertainClaim {
                job: *job,
                operation: *operation,
            });
        }
        outcome
    }

    /// Walks one captured cohort to exhaustion. Every `i < pc` has a
    /// definitive disposition; `pc` moves only through acknowledgement.
    /// `NoCapacity(Global)`, `BudgetExhausted`, read-queue backpressure and
    /// pauses never acknowledge. Every counted read debits its worker service
    /// time and every claim its round trip; pauses and backoff never debit.
    async fn run_sweep(
        &mut self,
        seq: u64,
        d0: u64,
        c0: u64,
        mut source: CohortSource,
        mut ledger: Ledger,
    ) -> SweepOutcome {
        let mut batch: Vec<PendingJobKey> = Vec::new();
        let mut batch_pos: usize = 0;
        let mut verdicts: Option<Vec<(JobId, Readiness)>> = None;
        let mut verdict_pos: usize = 0;
        let mut cont: Option<EvalContinuation> = None;
        let mut bars: BTreeMap<ExecutionPoolId, PendingJobKey> = BTreeMap::new();
        let mut earliest_due: Option<Timestamp> = None;
        let mut cap_deferred = false;
        let mut hint: Option<u8> = None;
        let mut grants: Vec<ClaimGrant> = Vec::new();

        let abandon = |reason: SweepAbandonedReason,
                       ledger: Ledger,
                       grants: Vec<ClaimGrant>,
                       earliest_due: Option<Timestamp>| {
            SweepOutcome::Abandoned(SweepAbandoned {
                reason,
                ledger,
                grants_so_far: grants,
                earliest_due_so_far: earliest_due,
            })
        };

        loop {
            if batch_pos >= batch.len() {
                batch.clear();
                batch_pos = 0;
                verdicts = None;
                verdict_pos = 0;
                let want = self.limits.candidates.get();
                if batch.try_reserve_exact(want).is_err() {
                    return abandon(
                        SweepAbandonedReason::StorageBoundary(
                            "the sweep could not hold its candidate batch".to_owned(),
                        ),
                        ledger,
                        grants,
                        earliest_due,
                    );
                }
                for _ in 0..want {
                    let before = ledger.service;
                    let fetched = source.next_key(&*self, &mut ledger).await;
                    self.credit
                        .debit(ledger.service.checked_sub(before).unwrap_or_default());
                    match fetched {
                        Err(CaptureFailure::Shutdown) => {
                            return abandon(
                                SweepAbandonedReason::Shutdown,
                                ledger,
                                grants,
                                earliest_due,
                            );
                        }
                        Err(CaptureFailure::IntegrityHalt(detail)) => {
                            return abandon(
                                SweepAbandonedReason::IntegrityHalt(detail),
                                ledger,
                                grants,
                                earliest_due,
                            );
                        }
                        Err(CaptureFailure::StorageBoundary(detail)) => {
                            return abandon(
                                SweepAbandonedReason::StorageBoundary(detail),
                                ledger,
                                grants,
                                earliest_due,
                            );
                        }
                        Ok(None) => break,
                        Ok(Some(key)) => batch.push(key),
                    }
                }
                if batch.is_empty() {
                    break;
                }
            }

            if verdicts.is_none()
                || verdict_pos >= verdicts.as_ref().map(Vec::len).unwrap_or(0)
            {
                if batch_pos >= batch.len() {
                    continue;
                }
                if self.shutdown.is_cancelled() {
                    return abandon(
                        SweepAbandonedReason::Shutdown,
                        ledger,
                        grants,
                        earliest_due,
                    );
                }
                if self.protection.status().lock.is_some() {
                    return abandon(
                        SweepAbandonedReason::ProtectionActive,
                        ledger,
                        grants,
                        earliest_due,
                    );
                }
                if hint == Some(0) {
                    cap_deferred = true;
                    verdicts = None;
                    verdict_pos = 0;
                    let c_seen = self.wake.capacity_generation();
                    match self.pause_global(c_seen).await {
                        Ok(()) => {
                            hint = None;
                            continue;
                        }
                        Err(reason) => {
                            return abandon(reason, ledger, grants, earliest_due);
                        }
                    }
                }
                let candidates: Vec<JobId> =
                    batch[batch_pos..].iter().map(|key| key.job).collect();
                let request = EvalRequest {
                    candidates,
                    budget: self.limits.evaluation_budget,
                    continuation: cont,
                };
                let evaluated = self
                    .retry(|| {
                        let database = self.database.clone();
                        let request = request.clone();
                        async move { database.evaluate_pending(request).await }
                    })
                    .await;
                match evaluated {
                    None => {
                        return abandon(
                            SweepAbandonedReason::Shutdown,
                            ledger,
                            grants,
                            earliest_due,
                        );
                    }
                    Some(Err(error)) => {
                        let failure = classify(&error);
                        match failure {
                            CaptureFailure::Shutdown => {
                                return abandon(
                                    SweepAbandonedReason::Shutdown,
                                    ledger,
                                    grants,
                                    earliest_due,
                                );
                            }
                            CaptureFailure::IntegrityHalt(detail) => {
                                return abandon(
                                    SweepAbandonedReason::IntegrityHalt(detail),
                                    ledger,
                                    grants,
                                    earliest_due,
                                );
                            }
                            CaptureFailure::StorageBoundary(detail) => {
                                return abandon(
                                    SweepAbandonedReason::StorageBoundary(detail),
                                    ledger,
                                    grants,
                                    earliest_due,
                                );
                            }
                        }
                    }
                    Some(Ok(page)) => {
                        ledger.service += page.service_time;
                        self.credit.debit(page.service_time);
                        cont = page.continuation;
                        if page.verdicts.is_empty() {
                            return abandon(
                                SweepAbandonedReason::IntegrityHalt(
                                    "evaluate_pending returned no verdicts".to_owned(),
                                ),
                                ledger,
                                grants,
                                earliest_due,
                            );
                        }
                        verdicts = Some(page.verdicts);
                        verdict_pos = 0;
                    }
                }
            }

            let page = verdicts.as_ref().expect("an evaluation page");
            let (job, verdict) = page[verdict_pos].clone();
            let key = batch[batch_pos];
            if job != key.job {
                return abandon(
                    SweepAbandonedReason::IntegrityHalt(format!(
                        "evaluate_pending answered {job:?} for candidate {:?}",
                        key.job
                    )),
                    ledger,
                    grants,
                    earliest_due,
                );
            }
            match verdict {
                Readiness::Blocked(Block::NotYetEligible { eligible_at }) => {
                    earliest_due = Some(match earliest_due {
                        Some(seen) => seen.min(eligible_at),
                        None => eligible_at,
                    });
                    batch_pos += 1;
                    verdict_pos += 1;
                }
                Readiness::Blocked(_) | Readiness::Indeterminate(_) => {
                    match &page[verdict_pos].1 {
                        Readiness::Indeterminate(why) if matches!(why, crate::persistence::Why::MissingJob | crate::persistence::Why::Corrupt) => {
                            ledger.alarm(CaptureAlarm::IndexEntry {
                                key,
                                fault: match why {
                                    crate::persistence::Why::MissingJob => PendingIndexFault::MissingJob,
                                    _ => PendingIndexFault::JobDecode,
                                },
                            });
                        }
                        Readiness::Blocked(Block::NotPending) => {
                            ledger.alarm(CaptureAlarm::IndexEntry {
                                key,
                                fault: PendingIndexFault::NotPending,
                            });
                        }
                        _ => {}
                    }
                    batch_pos += 1;
                    verdict_pos += 1;
                }
                Readiness::BudgetExhausted => {
                    verdicts = None;
                    verdict_pos = 0;
                }
                Readiness::Ready { compatible_pools } => {
                    let allowed: BTreeSet<ExecutionPoolId> = compatible_pools
                        .iter()
                        .filter(|pool| !bars.contains_key(pool))
                        .copied()
                        .collect();
                    if allowed.is_empty() {
                        cap_deferred = true;
                        batch_pos += 1;
                        verdict_pos += 1;
                        continue;
                    }
                    if self.shutdown.is_cancelled() {
                        return abandon(
                            SweepAbandonedReason::Shutdown,
                            ledger,
                            grants,
                            earliest_due,
                        );
                    }
                    if self.protection.status().lock.is_some() {
                        return abandon(
                            SweepAbandonedReason::ProtectionActive,
                            ledger,
                            grants,
                            earliest_due,
                        );
                    }
                    let c_claim = self.wake.capacity_generation();
                    let began = Instant::now();
                    let reply = self
                        .database
                        .claim_execution(ClaimRequest {
                            job,
                            allowed_pools: allowed,
                        })
                        .await;
                    let round_trip = began.elapsed();
                    ledger.service += round_trip;
                    self.credit.debit(round_trip);
                    match reply {
                        Ok(Ok(grant)) => {
                            hint = Some(grant.global_free_after);
                            grants.push(grant);
                            batch_pos += 1;
                            verdict_pos += 1;
                        }
                        Ok(Err(ClaimRefusal::NoCapacity(CapacityScope::Global))) => {
                            cap_deferred = true;
                            verdicts = None;
                            verdict_pos = 0;
                            match self.pause_global(c_claim).await {
                                Ok(()) => {
                                    hint = None;
                                }
                                Err(reason) => {
                                    return abandon(reason, ledger, grants, earliest_due);
                                }
                            }
                        }
                        Ok(Err(ClaimRefusal::NoCapacity(CapacityScope::Pools(pools)))) => {
                            for pool in pools {
                                bars.insert(pool, key);
                            }
                            cap_deferred = true;
                            batch_pos += 1;
                            verdict_pos += 1;
                        }
                        Ok(Err(ClaimRefusal::NotYetEligible { eligible_at })) => {
                            earliest_due = Some(match earliest_due {
                                Some(seen) => seen.min(eligible_at),
                                None => eligible_at,
                            });
                            batch_pos += 1;
                            verdict_pos += 1;
                        }
                        Ok(Err(_)) => {
                            batch_pos += 1;
                            verdict_pos += 1;
                        }
                        Err(error) => match error.command_outcome() {
                            Some(CommandOutcome::Unknown) => {
                                let DatabaseError::OutcomeUnknown { operation, .. } = error
                                else {
                                    return abandon(
                                        SweepAbandonedReason::StorageBoundary(
                                            "a claim outcome was unknown without its operation"
                                                .to_owned(),
                                        ),
                                        ledger,
                                        grants,
                                        earliest_due,
                                    );
                                };
                                return abandon(
                                    SweepAbandonedReason::UncertainClaim { job, operation },
                                    ledger,
                                    grants,
                                    earliest_due,
                                );
                            }
                            Some(
                                CommandOutcome::NotAccepted | CommandOutcome::NotCommitted,
                            ) => {
                                return abandon(
                                    SweepAbandonedReason::StorageBoundary(error.to_string()),
                                    ledger,
                                    grants,
                                    earliest_due,
                                );
                            }
                            None => {
                                return abandon(
                                    SweepAbandonedReason::StorageBoundary(error.to_string()),
                                    ledger,
                                    grants,
                                    earliest_due,
                                );
                            }
                        },
                    }
                }
            }
        }

        let restart_wanted = self.wake.dirty_generation() != d0
            || (cap_deferred && self.wake.capacity_generation() != c0);
        SweepOutcome::Completed(SweepCompleted {
            seq,
            d0,
            c0,
            grants,
            earliest_due,
            ledger,
            cap_deferred,
            restart_wanted,
        })
    }

    /// Waits for a global slot without consuming the cohort cursor. Verdicts
    /// are stale and dropped by the caller; the cohort and `pc` are kept.
    /// A dirty-only wake never resumes: only `capacity_gen` releases room.
    async fn pause_global(&self, c_seen: u64) -> Result<(), SweepAbandonedReason> {
        if self.wake.capacity_generation() != c_seen {
            return Ok(());
        }
        loop {
            if self.shutdown.is_cancelled() {
                return Err(SweepAbandonedReason::Shutdown);
            }
            if self.protection.status().lock.is_some() {
                return Err(SweepAbandonedReason::ProtectionActive);
            }
            match ntex::util::select(
                self.shutdown.cancelled(),
                ntex::util::select(
                    self.wake.recv_async(),
                    self.protection.changed(),
                ),
            )
            .await
            {
                ntex::util::Either::Left(()) => {
                    return Err(SweepAbandonedReason::Shutdown);
                }
                ntex::util::Either::Right(ntex::util::Either::Left(wake)) => {
                    if wake.is_err() {
                        return Err(SweepAbandonedReason::StorageBoundary(
                            "the scheduler wake closed while waiting for capacity".to_owned(),
                        ));
                    }
                    if self.protection.status().lock.is_some() {
                        return Err(SweepAbandonedReason::ProtectionActive);
                    }
                    if self.wake.capacity_generation() != c_seen {
                        return Ok(());
                    }
                }
                ntex::util::Either::Right(ntex::util::Either::Right(protection)) => {
                    if protection.is_none() {
                        return Err(SweepAbandonedReason::StorageBoundary(
                            "protection reporting closed while waiting for capacity".to_owned(),
                        ));
                    }
                    if self.protection.status().lock.is_some() {
                        return Err(SweepAbandonedReason::ProtectionActive);
                    }
                    if self.wake.capacity_generation() != c_seen {
                        return Ok(());
                    }
                }
            }
        }
    }

    /// How long until the earliest not-yet-eligible Job may run, on the
    /// monotonic clock, never below the anti-spin floor. `None` arms no timer:
    /// under global saturation capacity, not time, is the binding constraint,
    /// and `capacity_gen` resumes the sweep.
    fn due_sleep_duration(earliest_due: Option<Timestamp>) -> Option<Duration> {
        let eligible = earliest_due?;
        let delta_micros = eligible
            .as_unix_micros()
            .saturating_sub(Timestamp::now().as_unix_micros())
            .max(0);
        let delta = Duration::from_micros(u64::try_from(delta_micros).unwrap_or(u64::MAX));
        Some(delta.max(DUE_FLOOR))
    }

    /// Amendment 1 §C.3 gate: a head sweep starts only when the bucket holds
    /// its trigger's threshold. `Startup` and `Recovery` always run. Waiting
    /// issues nothing, so credit only refills; a stronger trigger lowers the
    /// threshold and returns immediately when covered. Shutdown stops.
    async fn gate(&mut self, mut trigger: SweepTrigger) -> Option<SweepTrigger> {
        loop {
            if self.shutdown.is_cancelled() {
                return None;
            }
            let now = std::time::Instant::now();
            self.credit.refill(now);
            if self.credit.balance_micros >= credit_threshold(trigger) {
                return Some(trigger);
            }
            let deficit = credit_threshold(trigger) - self.credit.balance_micros;
            let wait = Duration::from_micros(((deficit as f64 / RHO) as u64).max(1));
            enum GateWake {
                Shutdown,
                Permit,
                Protection,
                CreditReady,
            }
            let woken = ntex::util::select(
                self.shutdown.cancelled(),
                ntex::util::select(
                    self.wake.recv_async(),
                    ntex::util::select(
                        self.protection.changed(),
                        ntex::time::sleep(wait),
                    ),
                ),
            )
            .await;
            let wake = match woken {
                ntex::util::Either::Left(()) => GateWake::Shutdown,
                ntex::util::Either::Right(ntex::util::Either::Left(_)) => GateWake::Permit,
                ntex::util::Either::Right(ntex::util::Either::Right(
                    ntex::util::Either::Left(_),
                )) => GateWake::Protection,
                ntex::util::Either::Right(ntex::util::Either::Right(
                    ntex::util::Either::Right(()),
                )) => GateWake::CreditReady,
            };
            match wake {
                GateWake::Shutdown => return None,
                GateWake::CreditReady => {}
                GateWake::Permit | GateWake::Protection => {
                    if self.protection.status().lock.is_some() {
                        return None;
                    }
                    // A publication while gate-waiting can only strengthen the
                    // trigger: a dirty-only wake must never weaken a pending
                    // capacity or due overdraft. Recovery stays unconditional.
                    match trigger {
                        SweepTrigger::Startup | SweepTrigger::Recovery => {}
                        SweepTrigger::Capacity | SweepTrigger::Due => {}
                        SweepTrigger::Dirty | SweepTrigger::Reconcile => {
                            trigger = SweepTrigger::Capacity;
                        }
                    }
                }
            }
        }
    }

    /// The dormant loop shell. Never constructed in production: HTTP and the
    /// CLI keep the legacy runtime, and no grant started here ever spawns a
    /// `JobTask`. Sweeps record grants and drop them; occupancy stays held
    /// until an explicit test finalizes.
    ///
    /// Wake handling: the permit is a doorbell only. Every decision compares
    /// `dirty_gen`/`capacity_gen` against the last sweep's `d0`/`c0`, so a
    /// coalesced or lost permit cannot suppress progress and cannot spin: a
    /// permit with no generation change starts no sweep. `R` backstops
    /// classification bugs. Pauses keep `pc`, the batch and the evaluation
    /// continuation; only verdicts are dropped.
    pub(crate) async fn run(&mut self) {
        let mut first = true;
        let mut last_d0 = 0u64;
        let mut last_c0 = 0u64;
        let mut last_cap_deferred = false;
        let mut earliest_due: Option<Timestamp> = None;
        let mut last_completed_at: Option<std::time::Instant> = None;
        let mut need_recovery = false;
        let mut was_protected = self.protection.status().lock.is_some();

        loop {
            if self.shutdown.is_cancelled() {
                return;
            }
            if self.claims_halted() {
                self.shutdown.cancelled().await;
                return;
            }
            let protected = self.protection.status().lock.is_some();
            if protected {
                was_protected = true;
                self.protection.changed().await;
                continue;
            }
            if need_recovery {
                enum RecoveryWake {
                    Shutdown,
                    Permit,
                    Protection,
                }
                let woken = ntex::util::select(
                    self.shutdown.cancelled(),
                    ntex::util::select(self.wake.recv_async(), self.protection.changed()),
                )
                .await;
                let wake = match woken {
                    ntex::util::Either::Left(()) => RecoveryWake::Shutdown,
                    ntex::util::Either::Right(ntex::util::Either::Left(_)) => {
                        RecoveryWake::Permit
                    }
                    ntex::util::Either::Right(ntex::util::Either::Right(_)) => {
                        RecoveryWake::Protection
                    }
                };
                match wake {
                    RecoveryWake::Shutdown => return,
                    RecoveryWake::Permit | RecoveryWake::Protection => {
                        if self.protection.status().lock.is_some() {
                            continue;
                        }
                        need_recovery = false;
                        was_protected = false;
                        if self.gate(SweepTrigger::Recovery).await.is_none() {
                            return;
                        }
                        match self.sweep_once().await {
                            SweepOutcome::Completed(completed) => {
                                last_d0 = completed.d0;
                                last_c0 = completed.c0;
                                last_cap_deferred = completed.cap_deferred;
                                earliest_due = completed.earliest_due;
                                last_completed_at = Some(std::time::Instant::now());
                                first = false;
                            }
                            SweepOutcome::Abandoned(abandoned) => {
                                first = false;
                                match abandoned.reason {
                                    SweepAbandonedReason::Shutdown => return,
                                    SweepAbandonedReason::UncertainClaim { .. } => {
                                        continue;
                                    }
                                    _ => {
                                        need_recovery = true;
                                    }
                                }
                            }
                        }
                        continue;
                    }
                }
            }
            if first {
                first = false;
                if self.gate(SweepTrigger::Startup).await.is_none() {
                    return;
                }
                match self.sweep_once().await {
                    SweepOutcome::Completed(completed) => {
                        last_d0 = completed.d0;
                        last_c0 = completed.c0;
                        last_cap_deferred = completed.cap_deferred;
                        earliest_due = completed.earliest_due;
                        last_completed_at = Some(std::time::Instant::now());
                    }
                    SweepOutcome::Abandoned(abandoned) => {
                        match abandoned.reason {
                            SweepAbandonedReason::Shutdown => return,
                            SweepAbandonedReason::UncertainClaim { .. } => continue,
                            _ => need_recovery = true,
                        }
                    }
                }
                was_protected = self.protection.status().lock.is_some();
                continue;
            }

            let due_wait = Self::due_sleep_duration(earliest_due);
            let r_wait = match last_completed_at {
                None => None,
                Some(at) => {
                    let elapsed = std::time::Instant::now().saturating_duration_since(at);
                    if elapsed >= self.limits.reconcile {
                        Some(Duration::ZERO)
                    } else {
                        Some(self.limits.reconcile - elapsed)
                    }
                }
            };
            let due_sleep = async {
                match due_wait {
                    Some(wait) => ntex::time::sleep(wait).await,
                    None => std::future::pending::<()>().await,
                }
            };
            let r_sleep = async {
                match r_wait {
                    Some(wait) => ntex::time::sleep(wait).await,
                    None => std::future::pending::<()>().await,
                }
            };
            enum IdleWake {
                Shutdown,
                Permit,
                Due,
                Reconcile,
                Protection,
            }
            let woken = ntex::util::select(
                self.shutdown.cancelled(),
                ntex::util::select(
                    self.wake.recv_async(),
                    ntex::util::select(
                        due_sleep,
                        ntex::util::select(r_sleep, self.protection.changed()),
                    ),
                ),
            )
            .await;
            let wake = match woken {
                ntex::util::Either::Left(()) => IdleWake::Shutdown,
                ntex::util::Either::Right(ntex::util::Either::Left(_)) => IdleWake::Permit,
                ntex::util::Either::Right(ntex::util::Either::Right(
                    ntex::util::Either::Left(()),
                )) => IdleWake::Due,
                ntex::util::Either::Right(ntex::util::Either::Right(
                    ntex::util::Either::Right(ntex::util::Either::Left(())),
                )) => IdleWake::Reconcile,
                ntex::util::Either::Right(ntex::util::Either::Right(
                    ntex::util::Either::Right(ntex::util::Either::Right(_)),
                )) => IdleWake::Protection,
            };
            match wake {
                IdleWake::Shutdown => return,
                IdleWake::Protection => {
                    was_protected = self.protection.status().lock.is_some();
                    continue;
                }
                IdleWake::Permit => {
                    if self.protection.status().lock.is_some() {
                        continue;
                    }
                    let dirty = self.wake.dirty_generation();
                    let cap = self.wake.capacity_generation();
                    let was_lift = was_protected && !self.protection.status().lock.is_some();
                    was_protected = self.protection.status().lock.is_some();
                    if was_lift {
                        if self.gate(SweepTrigger::Recovery).await.is_none() {
                            return;
                        }
                    } else if dirty != last_d0
                        && last_cap_deferred
                        && cap != last_c0
                    {
                        if self.gate(SweepTrigger::Capacity).await.is_none() {
                            return;
                        }
                    } else if dirty != last_d0 {
                        if self.gate(SweepTrigger::Dirty).await.is_none() {
                            return;
                        }
                    } else if last_cap_deferred && cap != last_c0 {
                        if self.gate(SweepTrigger::Capacity).await.is_none() {
                            return;
                        }
                    } else {
                        continue;
                    }
                }
                IdleWake::Due => {
                    earliest_due = None;
                    if self.protection.status().lock.is_some() {
                        continue;
                    }
                    if self.gate(SweepTrigger::Due).await.is_none() {
                        return;
                    }
                }
                IdleWake::Reconcile => {
                    if self.protection.status().lock.is_some() {
                        continue;
                    }
                    if self.gate(SweepTrigger::Reconcile).await.is_none() {
                        return;
                    }
                }
            }
            match self.sweep_once().await {
                SweepOutcome::Completed(completed) => {
                    last_d0 = completed.d0;
                    last_c0 = completed.c0;
                    last_cap_deferred = completed.cap_deferred;
                    earliest_due = completed.earliest_due;
                    last_completed_at = Some(std::time::Instant::now());
                }
                SweepOutcome::Abandoned(abandoned) => match abandoned.reason {
                    SweepAbandonedReason::Shutdown => return,
                    SweepAbandonedReason::UncertainClaim { .. } => continue,
                    _ => need_recovery = true,
                },
            }
            was_protected = self.protection.status().lock.is_some();
        }
    }
}

/// One step of a retried capture page.
enum CaptureStep<T> {
    Page(T),
    Fault(PendingReadFault),
    Failed(CaptureFailure),
}

/// What one database request cost and what it met. The capture and the sweep
/// accumulate into one; the credit gate debits [`Ledger::service`].
#[derive(Debug, Default)]
pub(crate) struct Ledger {
    /// Read-worker service time and write round trips this sweep spent.
    pub(crate) service: Duration,
    pub(crate) alarms: Vec<CaptureAlarm>,
}

impl Ledger {
    fn alarm(&mut self, alarm: CaptureAlarm) {
        log::warn!("scheduler: {alarm}");
        self.alarms.push(alarm);
    }
}

/// What one capture produced, and what it spent while producing it.
#[derive(Debug, Default)]
pub(crate) struct Captured {
    pub(crate) cohort: Option<Cohort>,
    pub(crate) ledger: Ledger,
}

impl Captured {
    fn material(mut self, keys: Vec<PendingJobKey>) -> Captured {
        self.cohort = Some(Cohort::Material(keys));
        self
    }

    fn stream(mut self, fence: PendingFence) -> Captured {
        self.cohort = Some(Cohort::Stream(fence));
        self
    }
}

/// The keys one sweep works from. A materialized cohort is immutable once
/// captured; a stream reads one page at a time so an over-capacity index never
/// allocates a cohort for itself.
#[derive(Debug)]
pub(crate) enum Cohort {
    Material(Vec<PendingJobKey>),
    Stream(PendingFence),
}

impl Cohort {
    /// Total key count when it is known without reading.
    pub(crate) fn len(&self) -> u64 {
        match self {
            Self::Material(keys) => keys.len() as u64,
            Self::Stream(fence) => fence.len,
        }
    }

    /// Turns a cohort into the cursor the sweep consumes.
    pub(crate) fn cursor(self, limits: &SchedulerLimits) -> CohortSource {
        match self {
            Self::Material(keys) => CohortSource::Material { keys, next: 0 },
            Self::Stream(fence) => {
                // The hard ceiling of the streaming path: every original key,
                // plus everything a net-growing writer could have added while
                // the database was under the cap, plus one page reading past
                // the end. Reaching it is an integrity alarm, not a stall.
                let bound = fence
                    .len
                    .saturating_add(limits.pending_max)
                    .saturating_add(limits.capture_page.get() as u64);
                CohortSource::Stream(PendingCursor {
                    fence,
                    after: None,
                    page: Vec::new(),
                    next: 0,
                    exhausted: false,
                    visits: 0,
                    bound,
                })
            }
        }
    }
}

/// The order one sweep consumes its keys in.
#[derive(Debug)]
pub(crate) enum CohortSource {
    Material {
        keys: Vec<PendingJobKey>,
        next: usize,
    },
    Stream(PendingCursor),
}

impl CohortSource {
    /// The next key of this sweep, or `None` once the fence is exhausted.
    pub(crate) async fn next_key(
        &mut self,
        scheduler: &Scheduler,
        ledger: &mut Ledger,
    ) -> Result<Option<PendingJobKey>, CaptureFailure> {
        match self {
            Self::Material { keys, next } => {
                let key = keys.get(*next).copied();
                if key.is_some() {
                    *next += 1;
                }
                Ok(key)
            }
            Self::Stream(cursor) => cursor.next(scheduler, ledger).await,
        }
    }
}

/// The streaming cursor of an over-capacity or unmaterializable cohort. Keys
/// are read one page at a time and never collected, so memory stays bounded by
/// one capture page plus what the sweep holds.
#[derive(Debug)]
pub(crate) struct PendingCursor {
    fence: PendingFence,
    after: Option<PendingJobKey>,
    page: Vec<PendingJobKey>,
    next: usize,
    exhausted: bool,
    visits: u64,
    bound: u64,
}

impl PendingCursor {
    async fn next(
        &mut self,
        scheduler: &Scheduler,
        ledger: &mut Ledger,
    ) -> Result<Option<PendingJobKey>, CaptureFailure> {
        loop {
            if self.next < self.page.len() {
                let key = self.page[self.next];
                self.next += 1;
                self.visits += 1;
                if self.visits > self.bound {
                    ledger.alarm(CaptureAlarm::StreamBoundExceeded {
                        visits: self.visits,
                        bound: self.bound,
                    });
                    return Ok(None);
                }
                // Past the captured snapshot: the rest is next sweep's work.
                if self.fence.end.is_none_or(|end| key > end) {
                    return Ok(None);
                }
                // A Job allocated after the fence was read. Its key is inside
                // the range but its work belongs to the next sweep.
                if key.job > self.fence.high_water {
                    continue;
                }
                return Ok(Some(key));
            }
            if self.exhausted {
                return Ok(None);
            }
            let read = scheduler
                .retry(|| {
                    scheduler
                        .database
                        .pending_keys(self.after, scheduler.limits.capture_page)
                })
                .await;
            match read {
                None => return Err(CaptureFailure::Shutdown),
                Some(Err(error)) => return Err(classify(&error)),
                Some(Ok(Ok(page))) => {
                    ledger.service += page.service_time;
                    self.after = page.page.last_visited;
                    self.page = page.page.visited;
                    self.next = 0;
                    self.exhausted = page.page.exhausted;
                }
                Some(Ok(Err(PendingReadFault::IndexEntry { key, kind, service_time, .. }))) => {
                    ledger.service += service_time;
                    ledger.alarm(CaptureAlarm::IndexEntry { key, fault: kind });
                    return Err(CaptureFailure::IntegrityHalt(format!(
                        "the keys-only page reported {kind:?} at {key:?}"
                    )));
                }
                Some(Ok(Err(PendingReadFault::Allocation { service_time, .. }))) => {
                    ledger.service += service_time;
                    ledger.alarm(CaptureAlarm::AllocationExhausted);
                    return Err(CaptureFailure::IntegrityHalt(
                        "a streaming page could not be allocated".to_owned(),
                    ));
                }
                Some(Ok(Err(PendingReadFault::InvalidFence))) => {
                    return Err(CaptureFailure::IntegrityHalt(
                        "the Pending index fence is missing or undecodable".to_owned(),
                    ));
                }
            }
        }
    }
}

/// Something the capture found that an operator has to know about. None of
/// these is a silent skip: each is logged and reported with the capture.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CaptureAlarm {
    /// An index entry names a Job that cannot be evaluated. The sweep fell
    /// back to keys-only reads and the entry surfaces as a verdict.
    IndexEntry {
        key: PendingJobKey,
        fault: PendingIndexFault,
    },
    /// The index holds more keys than J1 admits. T1 refuses further growth and
    /// the sweep streams the index instead of materializing it.
    OverCapacity { len: u64 },
    /// A bounded allocation was refused, so the sweep fell back to the
    /// streaming path, which allocates one page at a time.
    AllocationExhausted,
    /// A streaming sweep examined more entries than the hard ceiling. The
    /// sweep stopped rather than run unbounded.
    StreamBoundExceeded { visits: u64, bound: u64 },
}

impl std::fmt::Display for CaptureAlarm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IndexEntry { key, fault } => {
                write!(f, "the Pending index names {key:?} with {fault:?}")
            }
            Self::OverCapacity { len } => {
                write!(f, "the Pending index holds {len} keys, above its cap")
            }
            Self::AllocationExhausted => {
                f.write_str("a bounded allocation was refused; streaming the index")
            }
            Self::StreamBoundExceeded { visits, bound } => write!(
                f,
                "a streamed sweep examined {visits} entries, above its bound of {bound}"
            ),
        }
    }
}

/// Why a capture stopped without a cohort. The sweep is abandoned either way;
/// the difference is whether waiting for a recovery can help.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CaptureFailure {
    /// The index or its metadata are damaged: no key set can be trusted, so no
    /// sweep and no claim runs until a recovery or a restart reads the facts
    /// again.
    IntegrityHalt(String),
    /// The read side refused or failed without naming damage. The sweep waits
    /// for the recovery publication.
    StorageBoundary(String),
    /// The process is stopping.
    Shutdown,
}

impl std::fmt::Display for CaptureFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IntegrityHalt(detail) => write!(f, "integrity halt: {detail}"),
            Self::StorageBoundary(detail) => write!(f, "storage boundary: {detail}"),
            Self::Shutdown => f.write_str("shutdown"),
        }
    }
}

/// The error policy of every capture read: engine or metadata damage halts,
/// anything else on the read side is a storage boundary the recovery
/// publication lifts.
fn classify(error: &DatabaseError) -> CaptureFailure {
    match error {
        DatabaseError::Read(storage) if storage.kind == StorageErrorKind::Corrupted => {
            CaptureFailure::IntegrityHalt(storage.detail.clone())
        }
        _ => CaptureFailure::StorageBoundary(error.to_string()),
    }
}

/// What one finished sweep did. Grants are recorded, never spawned; the
/// activation milestone owns starting anything from them.
#[derive(Debug)]
pub(crate) struct SweepCompleted {
    pub(crate) seq: u64,
    pub(crate) d0: u64,
    pub(crate) c0: u64,
    pub(crate) grants: Vec<ClaimGrant>,
    pub(crate) earliest_due: Option<Timestamp>,
    pub(crate) ledger: Ledger,
    /// A Pool bar, an empty allowed set or a global pause deferred capacity.
    /// The loop restarts only when a release moved `capacity_gen` as well.
    pub(crate) cap_deferred: bool,
    /// `dirty_gen != d0`, or deferred capacity saw a release. The caller
    /// decides whether another sweep follows; this sweep never starts one.
    pub(crate) restart_wanted: bool,
}

/// Why a sweep stopped before its cohort was exhausted. Grants made before
/// the stop stay committed; nothing else is kept.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SweepAbandonedReason {
    IntegrityHalt(String),
    StorageBoundary(String),
    ProtectionActive,
    Shutdown,
    UncertainClaim { job: JobId, operation: OperationId },
}

/// A sweep that stopped early, with what it had recorded so far.
#[derive(Debug)]
pub(crate) struct SweepAbandoned {
    pub(crate) reason: SweepAbandonedReason,
    pub(crate) ledger: Ledger,
    pub(crate) grants_so_far: Vec<ClaimGrant>,
    pub(crate) earliest_due_so_far: Option<Timestamp>,
}

/// One dormant sweep and nothing more. No loop, no gate, no timer.
#[derive(Debug)]
pub(crate) enum SweepOutcome {
    Completed(SweepCompleted),
    Abandoned(SweepAbandoned),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::DatabaseWorker;
    use crate::persistence::{
        CapabilitySet, ExecutionConfiguration, ExecutionMode, ExecutionOperation, ExecutionPoolConfig,
        FinalResult, JobId, JobInputDraft, LlmConfig, LlmTransport, NewExecutionPool,
        NewJob, NewLlmProvider, NewProfile, NewProfileConfig, PendingJobsPage,
    };
    use crate::storage::StorageError;
    use crate::test_support::{data_directory, wait};
    use std::collections::BTreeSet as TestSet;

    fn pending(database: &DatabaseHandle) -> JobId {
        let input = wait(database.save_job_input(None, JobInputDraft::default())).unwrap();
        wait(database.create_job_with_input(
            NewJob {
                name: None,
                execution_mode: ExecutionMode::Once,
                eligible_at: None,
                scheduled_at: None,
                next_due_at: None,
                next_iteration_no: None,
                iteration_no: None,
                parent: database.genesis(),
                projects: Vec::new(),
                dependencies: Vec::new(),
                configuration: None,
                assigned_executor: None,
                previous: None,
                replaces: None,
            },
            input,
        ))
        .unwrap()
    }

    fn limits(pending_max: u64, capture_page: usize) -> SchedulerLimits {
        SchedulerLimits {
            capture_page: NonZeroUsize::new(capture_page).unwrap(),
            pending_max,
            ..SchedulerLimits::default()
        }
    }

    fn scheduler(
        worker: &mut DatabaseWorker,
        limits: SchedulerLimits,
    ) -> Scheduler {
        let wake = worker.take_scheduler_wake().unwrap();
        Scheduler::new(
            worker.handle(),
            wake,
            CancellationToken::default(),
            limits,
        )
    }

    fn provision(database: &DatabaseHandle) -> (ExecutionPoolId, crate::persistence::ProfileId) {
        let provider = wait(database.create_llm_provider(NewLlmProvider {
            name: "p".into(),
            transport: LlmTransport::OpenAiChatCompletions,
            endpoint: None,
            credential_ref: None,
        }))
        .unwrap();
        let llm = wait(database.create_profile(NewProfile {
            name: "llm".into(),
            config: NewProfileConfig::Llm {
                provider,
                config: LlmConfig {
                    model: "m".into(),
                    parameters: Default::default(),
                    declared_capabilities: Default::default(),
                },
            },
        }))
        .unwrap();
        let profile = wait(database.create_profile(NewProfile {
            name: "pool".into(),
            config: NewProfileConfig::ExecutionPool(ExecutionPoolConfig {
                concurrency_limit: NonZeroU32::new(16).unwrap(),
                capabilities: CapabilitySet {
                    operations: TestSet::from([ExecutionOperation::LlmRequest]),
                    llm_protocols: TestSet::from([LlmTransport::OpenAiChatCompletions]),
                },
            }),
        }))
        .unwrap();
        let pool = wait(database.create_execution_pool(NewExecutionPool {
            name: "ordinary".into(),
            profile,
            enabled: true,
        }))
        .unwrap();
        (pool, llm)
    }

    fn narrow_pool(database: &DatabaseHandle, name: &str) -> ExecutionPoolId {
        let profile = wait(database.create_profile(NewProfile {
            name: name.into(),
            config: NewProfileConfig::ExecutionPool(ExecutionPoolConfig {
                concurrency_limit: NonZeroU32::new(1).unwrap(),
                capabilities: CapabilitySet {
                    operations: TestSet::from([ExecutionOperation::LlmRequest]),
                    llm_protocols: TestSet::from([LlmTransport::OpenAiChatCompletions]),
                },
            }),
        }))
        .unwrap();
        wait(database.create_execution_pool(NewExecutionPool {
            name: name.into(),
            profile,
            enabled: true,
        }))
        .unwrap()
    }

    fn pending_ready(
        database: &DatabaseHandle,
        llm: crate::persistence::ProfileId,
    ) -> JobId {
        let input = wait(database.save_job_input(None, JobInputDraft::default())).unwrap();
        wait(database.create_job_with_input(
            NewJob {
                name: None,
                execution_mode: ExecutionMode::Once,
                eligible_at: None,
                scheduled_at: None,
                next_due_at: None,
                next_iteration_no: None,
                iteration_no: None,
                parent: database.genesis(),
                projects: Vec::new(),
                dependencies: Vec::new(),
                configuration: Some(ExecutionConfiguration {
                    agent_profile: None,
                    llm_profile: llm,
                }),
                assigned_executor: None,
                previous: None,
                replaces: None,
            },
            input,
        ))
        .unwrap()
    }

    fn done_result(summary: &str) -> FinalResult {
        FinalResult {
            summary: Some(summary.into()),
            code: None,
            goal_reached: None,
            value: None,
        }
    }

    async fn index_order(database: &DatabaseHandle) -> Vec<PendingJobKey> {
        let mut keys = Vec::new();
        let mut after = None;
        loop {
            let PendingJobsPage {
                visited,
                last_visited,
                exhausted,
            } = database.pending_jobs(after, 100).await.unwrap();
            keys.extend(visited);
            if exhausted {
                return keys;
            }
            after = last_visited;
        }
    }

    #[test]
    fn capture_materializes_the_whole_index_in_fifo_order() {
        let (_root, directory) = data_directory("ocg-stage-b-scheduler", "capture");
        let mut worker = DatabaseWorker::start(&directory).unwrap();
        let database = worker.handle();
        for _ in 0..3 {
            pending(&database);
        }
        let limits = limits(16, 100);
        let scheduler = scheduler(&mut worker, limits);
        let captured = wait(scheduler.capture()).expect("a healthy index captures");
        let expected = wait(index_order(&database));
        assert_eq!(expected.len(), 3);
        match captured.cohort.expect("a cohort") {
            Cohort::Material(keys) => assert_eq!(keys, expected),
            other => panic!("a small index materializes, got {other:?}"),
        }
        assert!(captured.ledger.alarms.is_empty(), "{:?}", captured.ledger.alarms);
        worker.shutdown().unwrap();
    }

    #[test]
    fn an_over_cap_index_streams_page_by_page_and_alarms() {
        let (_root, directory) = data_directory("ocg-stage-b-scheduler", "over-cap");
        let mut worker = DatabaseWorker::start(&directory).unwrap();
        let database = worker.handle();
        for _ in 0..6 {
            pending(&database);
        }
        let expected = wait(index_order(&database));
        // The cap is two keys and a page is two keys, so the sweep reads three
        // pages and never collects more than one of them.
        let scheduler = scheduler(&mut worker, limits(2, 2));
        let captured = wait(scheduler.capture()).expect("an over-cap index captures");
        assert_eq!(
            captured.ledger.alarms,
            vec![CaptureAlarm::OverCapacity { len: 6 }]
        );
        let cohort = captured.cohort.expect("a cohort");
        assert_eq!(cohort.len(), 6);
        let mut cursor = cohort.cursor(&limits(2, 2));
        let mut captured = Captured::default();
        let mut drained = Vec::new();
        while let Some(key) =
            wait(cursor.next_key(&scheduler, &mut captured.ledger)).expect("the stream continues")
        {
            drained.push(key);
        }
        assert_eq!(drained, expected);
        assert!(captured.ledger.alarms.is_empty(), "{:?}", captured.ledger.alarms);
        worker.shutdown().unwrap();
    }

    #[test]
    fn a_stream_skips_jobs_allocated_after_the_fence() {
        let (_root, directory) = data_directory("ocg-stage-b-scheduler", "intruder");
        let mut worker = DatabaseWorker::start(&directory).unwrap();
        let database = worker.handle();
        for _ in 0..3 {
            pending(&database);
        }
        let expected = wait(index_order(&database));
        let scheduler = scheduler(&mut worker, limits(2, 2));
        let captured = wait(scheduler.capture()).expect("capture");
        let Cohort::Stream(fence) = captured.cohort.expect("a cohort") else {
            panic!("an index above the cap streams");
        };
        // A fence whose high water predates the last Job: the newest key is
        // outside the captured snapshot and the stream leaves it alone.
        let stale = PendingFence {
            high_water: expected[1].job,
            ..fence
        };
        let mut cursor = Cohort::Stream(stale).cursor(&limits(2, 2));
        let mut captured = Captured::default();
        let mut drained = Vec::new();
        while let Some(key) =
            wait(cursor.next_key(&scheduler, &mut captured.ledger)).expect("the stream continues")
        {
            drained.push(key);
        }
        assert_eq!(drained, expected[..2].to_vec());
        worker.shutdown().unwrap();
    }

    #[test]
    fn a_damaged_index_entry_degrades_to_the_keys_only_snapshot() {
        let (_root, directory) = data_directory("ocg-stage-b-scheduler", "damaged");
        let mut worker = DatabaseWorker::start(&directory).unwrap();
        let database = worker.handle();
        let jobs: Vec<_> = (0..3).map(|_| pending(&database)).collect();
        let expected = wait(index_order(&database));
        let dangling = jobs[0];
        wait(database.with_database(move |database| {
            crate::storage::remove_job_row(database, dangling)
        }))
        .expect("remove the Job row");
        let scheduler = scheduler(&mut worker, limits(16, 1));
        let captured = wait(scheduler.capture()).expect("capture");
        assert!(
            matches!(
                captured.ledger.alarms.as_slice(),
                [CaptureAlarm::IndexEntry {
                    fault: PendingIndexFault::MissingJob,
                    ..
                }]
            ),
            "{:?}",
            captured.ledger.alarms
        );
        // Keys-only: the damaged key is still delivered, so the evaluation of
        // that candidate reports it instead of the whole capture stalling.
        match captured.cohort.expect("a cohort") {
            Cohort::Material(keys) => assert_eq!(keys, expected),
            other => panic!("a keys-only snapshot materializes, got {other:?}"),
        }
        worker.shutdown().unwrap();
    }

    #[test]
    fn a_fence_below_the_materialization_cap_that_cannot_be_held_streams() {
        let (_root, directory) = data_directory("ocg-stage-b-scheduler", "fence");
        let mut worker = DatabaseWorker::start(&directory).unwrap();
        let database = worker.handle();
        for _ in 0..2 {
            pending(&database);
        }
        let expected = wait(index_order(&database));
        let scheduler = scheduler(&mut worker, limits(16, 2));
        let captured = wait(scheduler.capture()).expect("capture");
        let Cohort::Material(keys) = captured.cohort.expect("a cohort") else {
            panic!("a small index materializes");
        };
        assert_eq!(keys, expected);
        // The same keys consumed through the streaming cursor, as the sweep
        // does when a bounded allocation is refused: the keys are identical.
        let fence = PendingFence {
            len: keys.len() as u64,
            high_water: keys.last().expect("a key").job,
            end: keys.last().copied(),
        };
        let mut cursor = Cohort::Stream(fence).cursor(&limits(16, 2));
        let mut captured = Captured::default();
        let mut drained = Vec::new();
        while let Some(key) =
            wait(cursor.next_key(&scheduler, &mut captured.ledger)).expect("the stream continues")
        {
            drained.push(key);
        }
        assert_eq!(drained, expected);
        worker.shutdown().unwrap();
    }

    #[test]
    fn a_stream_that_exceeds_its_visit_bound_stops_and_alarms() {
        let (_root, directory) = data_directory("ocg-stage-b-scheduler", "bound");
        let mut worker = DatabaseWorker::start(&directory).unwrap();
        let database = worker.handle();
        for _ in 0..6 {
            pending(&database);
        }
        let expected = wait(index_order(&database));
        let scheduler = scheduler(&mut worker, limits(1, 1));
        // The shape of the backdated flood: a fence that claims two keys while
        // the keys inside its range were allocated after it, so every key but
        // the first is skipped as an intruder and the stream never reaches the
        // fence end. The bound is the claimed length plus the cap plus one page.
        let fence = PendingFence {
            len: 2,
            high_water: expected[0].job,
            end: Some(expected[5]),
        };
        let limits = limits(1, 1);
        let mut cursor = Cohort::Stream(fence).cursor(&limits);
        let mut captured = Captured::default();
        let mut drained = Vec::new();
        while let Some(key) =
            wait(cursor.next_key(&scheduler, &mut captured.ledger)).expect("the stream continues")
        {
            drained.push(key);
        }
        assert_eq!(
            captured.ledger.alarms,
            vec![CaptureAlarm::StreamBoundExceeded {
                visits: 5,
                bound: 2 + 1 + 1,
            }]
        );
        assert_eq!(drained, expected[..1].to_vec());
        worker.shutdown().unwrap();
    }

    #[test]
    fn engine_damage_halts_and_other_read_failures_are_boundaries() {
        let corrupted = DatabaseError::Read(StorageError {
            kind: StorageErrorKind::Corrupted,
            os_error: None,
            detail: "metadata".to_owned(),
        });
        assert_eq!(
            classify(&corrupted),
            CaptureFailure::IntegrityHalt("metadata".to_owned())
        );
        let io = DatabaseError::Read(StorageError {
            kind: StorageErrorKind::Io(std::io::ErrorKind::Other),
            os_error: None,
            detail: "disk".to_owned(),
        });
        assert!(matches!(classify(&io), CaptureFailure::StorageBoundary(_)));
        assert!(matches!(
            classify(&DatabaseError::ReadsClosed),
            CaptureFailure::StorageBoundary(_)
        ));
        assert!(matches!(
            classify(&DatabaseError::ReadPoolStopped),
            CaptureFailure::StorageBoundary(_)
        ));
    }

    #[test]
    fn a_stopping_scheduler_stops_retrying_a_refused_read() {
        let (_root, directory) = data_directory("ocg-stage-b-scheduler", "shutdown");
        let mut worker = DatabaseWorker::start(&directory).unwrap();
        let database = worker.handle();
        let limits = limits(16, 100);
        let wake = worker.take_scheduler_wake().unwrap();
        let shutdown = CancellationToken::default();
        let scheduler = Scheduler::new(database, wake, shutdown.clone(), limits);
        shutdown.cancel();
        assert_eq!(
            wait(scheduler.capture()).expect_err("a stopped scheduler captures nothing"),
            CaptureFailure::Shutdown
        );
        worker.shutdown().unwrap();
    }

    #[test]
    fn a_sweep_claims_ready_jobs_and_records_grants_without_spawning() {
        let (_root, directory) = data_directory("ocg-stage-b-scheduler", "sweep-claim");
        let mut worker = DatabaseWorker::start(&directory).unwrap();
        let database = worker.handle();
        let (pool, llm) = provision(&database);
        let first = pending_ready(&database, llm);
        let second = pending_ready(&database, llm);
        let mut scheduler = scheduler(&mut worker, limits(16, 100));
        let outcome = wait(scheduler.sweep_once());
        let completed = match outcome {
            SweepOutcome::Completed(completed) => completed,
            SweepOutcome::Abandoned(abandoned) => {
                panic!("a healthy sweep completes, got {:?}", abandoned.reason)
            }
        };
        assert_eq!(completed.grants.len(), 2);
        assert_eq!(completed.grants[0].job, first);
        assert_eq!(completed.grants[1].job, second);
        assert_eq!(completed.grants[0].binding.pool, pool);
        assert!(completed.earliest_due.is_none());
        assert!(!completed.restart_wanted);
        assert!(completed.ledger.alarms.is_empty());
        assert!(!scheduler.claims_halted());
        let occupancy = wait(database.occupancy()).unwrap();
        assert_eq!(occupancy.len(), 2);
        worker.shutdown().unwrap();
    }

    #[test]
    fn a_sweep_with_only_a_full_pool_bars_and_claims_nothing() {
        let (_root, directory) = data_directory("ocg-stage-b-scheduler", "sweep-bar-only");
        let mut worker = DatabaseWorker::start(&directory).unwrap();
        let database = worker.handle();
        // A single one-slot Pool, filled before the sweep, so every Ready
        // candidate meets NoCapacity(Pools) and is barred for the sweep.
        let narrow = narrow_pool(&database, "solo");
        let llm = {
            let provider = wait(database.create_llm_provider(NewLlmProvider {
                name: "p".into(),
                transport: LlmTransport::OpenAiChatCompletions,
                endpoint: None,
                credential_ref: None,
            }))
            .unwrap();
            wait(database.create_profile(NewProfile {
                name: "llm".into(),
                config: NewProfileConfig::Llm {
                    provider,
                    config: LlmConfig {
                        model: "m".into(),
                        parameters: Default::default(),
                        declared_capabilities: Default::default(),
                    },
                },
            }))
            .unwrap()
        };
        let filler = pending_ready(&database, llm);
        wait(database.claim_execution(ClaimRequest {
            job: filler,
            allowed_pools: TestSet::from([narrow]),
        }))
        .unwrap()
        .unwrap();
        let _first = pending_ready(&database, llm);
        let _second = pending_ready(&database, llm);
        let mut scheduler = scheduler(&mut worker, limits(16, 100));
        // Remove every other Pool from compatibility by disabling is not
        // possible here; instead check the bar path through a direct narrow
        // claim refused for capacity, which the sweep must also meet. The
        // sweep sees both Pools, so it claims on the wide one when present;
        // with only the narrow Pool present it must bar and defer.
        let outcome = wait(scheduler.sweep_once());
        match outcome {
            SweepOutcome::Completed(completed) => {
                assert_eq!(completed.grants.len(), 0);
                assert!(completed.earliest_due.is_none());
            }
            SweepOutcome::Abandoned(abandoned) => {
                panic!("a barred sweep still completes, got {:?}", abandoned.reason)
            }
        }
        worker.shutdown().unwrap();
    }

    #[test]
    fn a_sweep_records_earliest_due_without_claiming() {
        use crate::persistence::Timestamp;
        let (_root, directory) = data_directory("ocg-stage-b-scheduler", "sweep-due");
        let mut worker = DatabaseWorker::start(&directory).unwrap();
        let database = worker.handle();
        let (_pool, llm) = provision(&database);
        let at = Timestamp::from_unix_micros(Timestamp::now().as_unix_micros() + 60_000_000);
        let input = wait(database.save_job_input(None, JobInputDraft::default())).unwrap();
        let job = wait(database.create_job_with_input(
            NewJob {
                name: None,
                execution_mode: ExecutionMode::Once,
                eligible_at: Some(at),
                scheduled_at: None,
                next_due_at: None,
                next_iteration_no: None,
                iteration_no: None,
                parent: database.genesis(),
                projects: Vec::new(),
                dependencies: Vec::new(),
                configuration: Some(ExecutionConfiguration {
                    agent_profile: None,
                    llm_profile: llm,
                }),
                assigned_executor: None,
                previous: None,
                replaces: None,
            },
            input,
        ))
        .unwrap();
        let mut scheduler = scheduler(&mut worker, limits(16, 100));
        let outcome = wait(scheduler.sweep_once());
        match outcome {
            SweepOutcome::Completed(completed) => {
                assert!(completed.grants.is_empty());
                assert_eq!(completed.earliest_due, Some(at));
                assert!(!completed.restart_wanted);
                let row = wait(database.job(job)).unwrap().unwrap();
                assert!(row.started_at.is_none());
            }
            SweepOutcome::Abandoned(abandoned) => {
                panic!("a due sweep completes, got {:?}", abandoned.reason)
            }
        }
        worker.shutdown().unwrap();
    }

    #[test]
    fn a_sweep_abandons_when_protection_is_active() {
        use crate::disk_protection::{ProtectedArea, ProtectionReason, SpaceShortage};
        let (_root, directory) = data_directory("ocg-stage-b-scheduler", "sweep-protected");
        let mut worker = DatabaseWorker::start(&directory).unwrap();
        let database = worker.handle();
        let (_pool, llm) = provision(&database);
        let _job = pending_ready(&database, llm);
        let path = directory.database_path().to_path_buf();
        assert!(database.activate_filesystem_protection(
            ProtectionReason::InsufficientSpace(SpaceShortage {
                area: ProtectedArea::Database,
                path,
                available_bytes: 0,
                expected_write_bytes: 0,
                threshold_bytes: 1,
            })
        ));
        let mut scheduler = scheduler(&mut worker, limits(16, 100));
        match wait(scheduler.sweep_once()) {
            SweepOutcome::Abandoned(abandoned) => {
                assert_eq!(abandoned.reason, SweepAbandonedReason::ProtectionActive);
                assert!(abandoned.grants_so_far.is_empty());
            }
            SweepOutcome::Completed(_) => panic!("a protected sweep sends no claim"),
        }
        worker.shutdown().unwrap();
    }

    #[test]
    fn a_sweep_abandons_when_shutdown_is_requested() {
        let (_root, directory) = data_directory("ocg-stage-b-scheduler", "sweep-shutdown");
        let mut worker = DatabaseWorker::start(&directory).unwrap();
        let database = worker.handle();
        let (_pool, llm) = provision(&database);
        let _job = pending_ready(&database, llm);
        let wake = worker.take_scheduler_wake().unwrap();
        let shutdown = CancellationToken::default();
        let mut scheduler = Scheduler::new(database, wake, shutdown.clone(), limits(16, 100));
        shutdown.cancel();
        match wait(scheduler.sweep_once()) {
            SweepOutcome::Abandoned(abandoned) => {
                assert_eq!(abandoned.reason, SweepAbandonedReason::Shutdown);
            }
            SweepOutcome::Completed(_) => panic!("a stopped sweep claims nothing"),
        }
        worker.shutdown().unwrap();
    }

    #[test]
    fn a_sweep_skips_a_blocked_child_and_claims_its_parent() {
        use crate::persistence::DependencyCondition;
        let (_root, directory) = data_directory("ocg-stage-b-scheduler", "sweep-blocked");
        let mut worker = DatabaseWorker::start(&directory).unwrap();
        let database = worker.handle();
        let (_pool, llm) = provision(&database);
        let parent = pending_ready(&database, llm);
        let input = wait(database.save_job_input(None, JobInputDraft::default())).unwrap();
        let child = wait(database.create_job_with_input(
            NewJob {
                name: None,
                execution_mode: ExecutionMode::Once,
                eligible_at: None,
                scheduled_at: None,
                next_due_at: None,
                next_iteration_no: None,
                iteration_no: None,
                parent: database.genesis(),
                projects: Vec::new(),
                dependencies: vec![(parent, DependencyCondition::Success)],
                configuration: Some(ExecutionConfiguration {
                    agent_profile: None,
                    llm_profile: llm,
                }),
                assigned_executor: None,
                previous: None,
                replaces: None,
            },
            input,
        ))
        .unwrap();
        let mut scheduler = scheduler(&mut worker, limits(16, 100));
        match wait(scheduler.sweep_once()) {
            SweepOutcome::Completed(completed) => {
                assert_eq!(completed.grants.len(), 1);
                assert_eq!(completed.grants[0].job, parent);
                let row = wait(database.job(child)).unwrap().unwrap();
                assert!(row.started_at.is_none());
            }
            SweepOutcome::Abandoned(abandoned) => {
                panic!("a sweep with a blocked child completes, got {:?}", abandoned.reason)
            }
        }
        worker.shutdown().unwrap();
    }

    #[test]
    fn a_sweep_acks_a_damaged_candidate_and_alarms() {
        let (_root, directory) = data_directory("ocg-stage-b-scheduler", "sweep-damaged");
        let mut worker = DatabaseWorker::start(&directory).unwrap();
        let database = worker.handle();
        let (_pool, llm) = provision(&database);
        let dangling = pending_ready(&database, llm);
        let healthy = pending_ready(&database, llm);
        wait(database.with_database(move |database| {
            crate::storage::remove_job_row(database, dangling)
        }))
        .expect("remove the Job row");
        let mut scheduler = scheduler(&mut worker, limits(16, 100));
        match wait(scheduler.sweep_once()) {
            SweepOutcome::Completed(completed) => {
                assert_eq!(completed.grants.len(), 1);
                assert_eq!(completed.grants[0].job, healthy);
                assert!(
                    completed.ledger.alarms.iter().any(|alarm| matches!(
                        alarm,
                        CaptureAlarm::IndexEntry {
                            fault: PendingIndexFault::MissingJob,
                            ..
                        }
                    )),
                    "{:?}",
                    completed.ledger.alarms
                );
            }
            SweepOutcome::Abandoned(abandoned) => {
                panic!("a sweep with a damaged key completes, got {:?}", abandoned.reason)
            }
        }
        worker.shutdown().unwrap();
    }

    #[test]
    fn a_sweep_drives_a_budget_exhausted_candidate_with_its_continuation() {
        use crate::persistence::DependencyCondition;
        let (_root, directory) = data_directory("ocg-stage-b-scheduler", "sweep-budget");
        let mut worker = DatabaseWorker::start(&directory).unwrap();
        let database = worker.handle();
        let (_pool, llm) = provision(&database);
        let first = pending_ready(&database, llm);
        let second = pending_ready(&database, llm);
        wait(database.finalize_job(first, crate::persistence::JobFinalization::Completed(done_result("one"))))
            .unwrap();
        wait(database.finalize_job(second, crate::persistence::JobFinalization::Completed(done_result("two"))))
            .unwrap();
        let input = wait(database.save_job_input(None, JobInputDraft::default())).unwrap();
        let child = wait(database.create_job_with_input(
            NewJob {
                name: None,
                execution_mode: ExecutionMode::Once,
                eligible_at: None,
                scheduled_at: None,
                next_due_at: None,
                next_iteration_no: None,
                iteration_no: None,
                parent: database.genesis(),
                projects: Vec::new(),
                dependencies: vec![
                    (first, DependencyCondition::Success),
                    (second, DependencyCondition::Success),
                ],
                configuration: Some(ExecutionConfiguration {
                    agent_profile: None,
                    llm_profile: llm,
                }),
                assigned_executor: None,
                previous: None,
                replaces: None,
            },
            input,
        ))
        .unwrap();
        let tiny = SchedulerLimits {
            evaluation_budget: NonZeroU32::new(10).unwrap(),
            ..limits(16, 100)
        };
        let mut scheduler = scheduler(&mut worker, tiny);
        match wait(scheduler.sweep_once()) {
            SweepOutcome::Completed(completed) => {
                assert_eq!(completed.grants.len(), 1);
                assert_eq!(completed.grants[0].job, child);
            }
            SweepOutcome::Abandoned(abandoned) => {
                panic!("an exhausted candidate is retried, got {:?}", abandoned.reason)
            }
        }
        worker.shutdown().unwrap();
    }

    #[test]
    fn a_sweep_pauses_on_global_saturation_and_claims_after_a_release() {
        let (_root, directory) = data_directory("ocg-stage-b-scheduler", "sweep-pause");
        let mut worker = DatabaseWorker::start(&directory).unwrap();
        let database = worker.handle();
        let (pool, llm) = provision(&database);
        let mut filled = Vec::new();
        for _ in 0..16 {
            let job = pending_ready(&database, llm);
            wait(database.claim_execution(ClaimRequest {
                job,
                allowed_pools: TestSet::from([pool]),
            }))
            .unwrap()
            .unwrap();
            filled.push(job);
        }
        let waiting = pending_ready(&database, llm);
        let freed = filled[0];
        let releaser = database.clone();
        let releaser = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(300));
            crate::test_support::wait(releaser.finalize_job(
                freed,
                crate::persistence::JobFinalization::Completed(done_result("free")),
            ))
            .unwrap();
        });
        let mut scheduler = scheduler(&mut worker, limits(16, 100));
        match wait(scheduler.sweep_once()) {
            SweepOutcome::Completed(completed) => {
                assert_eq!(completed.grants.len(), 1);
                assert_eq!(completed.grants[0].job, waiting);
                assert!(completed.restart_wanted);
            }
            SweepOutcome::Abandoned(abandoned) => {
                panic!("a paused sweep resumes, got {:?}", abandoned.reason)
            }
        }
        releaser.join().unwrap();
        worker.shutdown().unwrap();
    }

    #[test]
    fn a_sweep_records_an_uncertain_claim_and_halts() {
        use crate::test_support::{WAIT, poll_once};
        use std::sync::atomic::Ordering;
        let (_root, directory) = data_directory("ocg-stage-b-scheduler", "sweep-uncertain");
        let mut worker = DatabaseWorker::start(&directory).unwrap();
        let database = worker.handle();
        let (_pool, llm) = provision(&database);
        let _job = pending_ready(&database, llm);
        let probe = database.group_probe();
        let (entered, release) = probe.hold_next_group();
        let extra_database = database.clone();
        let extra = std::thread::spawn(move || {
            entered.recv_timeout(WAIT).unwrap();
            let mut extra = std::pin::pin!(extra_database.create_project(
                crate::persistence::NewProject {
                    name: "flush group".into(),
                    description: None,
                }
            ));
            assert!(poll_once(extra.as_mut()).is_none());
            extra_database
                .group_probe()
                .fail_next_flush
                .store(true, Ordering::SeqCst);
            release.send(()).unwrap();
            let _ = crate::test_support::wait(extra);
        });
        let mut scheduler = scheduler(&mut worker, limits(16, 100));
        match wait(scheduler.sweep_once()) {
            SweepOutcome::Abandoned(abandoned) => {
                assert!(
                    matches!(
                        abandoned.reason,
                        SweepAbandonedReason::UncertainClaim { .. }
                    ),
                    "{:?}",
                    abandoned.reason
                );
                assert!(abandoned.grants_so_far.is_empty());
            }
            SweepOutcome::Completed(_) => panic!("an unknown claim never grants"),
        }
        extra.join().unwrap();
        assert!(scheduler.claims_halted());
        assert!(scheduler.uncertain().is_some());
        match wait(scheduler.sweep_once()) {
            SweepOutcome::Abandoned(abandoned) => {
                assert!(matches!(
                    abandoned.reason,
                    SweepAbandonedReason::UncertainClaim { .. }
                ));
            }
            SweepOutcome::Completed(_) => panic!("a halted scheduler starts no sweep"),
        }
        wait(database.recover_storage()).unwrap();
        worker.shutdown().unwrap();
    }

    #[test]
    fn credit_starts_full_caps_refill_and_applies_trigger_thresholds() {
        assert_eq!(credit_threshold(SweepTrigger::Startup), i64::MIN);
        assert_eq!(credit_threshold(SweepTrigger::Recovery), i64::MIN);
        assert_eq!(
            credit_threshold(SweepTrigger::Capacity),
            -micros(C_PROMPT)
        );
        assert_eq!(credit_threshold(SweepTrigger::Due), -micros(C_PROMPT));
        assert_eq!(credit_threshold(SweepTrigger::Dirty), 0);
        assert_eq!(credit_threshold(SweepTrigger::Reconcile), 0);
        let now = std::time::Instant::now();
        let mut bucket = CreditBucket::new(now);
        assert_eq!(bucket.balance_micros, micros(C_MAX));
        bucket.debit(Duration::from_millis(100));
        let after_debit = bucket.balance_micros;
        assert!(after_debit < micros(C_MAX));
        // Wakes never touch the bucket: only time refills, capped above.
        bucket.refill(now + Duration::from_secs(60));
        assert_eq!(bucket.balance_micros, micros(C_MAX));
        let _ = after_debit;
    }

    #[test]
    fn due_timer_arms_nothing_without_a_due_date_and_floors_the_past() {
        assert_eq!(Scheduler::due_sleep_duration(None), None);
        let past = Timestamp::from_unix_micros(
            Timestamp::now().as_unix_micros() - 1_000_000,
        );
        assert_eq!(
            Scheduler::due_sleep_duration(Some(past)),
            Some(DUE_FLOOR)
        );
        let future = Timestamp::from_unix_micros(
            Timestamp::now().as_unix_micros() + 600_000,
        );
        let wait = Scheduler::due_sleep_duration(Some(future)).unwrap();
        assert!(wait >= DUE_FLOOR && wait <= Duration::from_millis(650));
    }

    #[test]
    fn a_sweep_debits_active_time_from_credit() {
        let (_root, directory) = data_directory("ocg-stage-b-scheduler", "credit-debit");
        let mut worker = DatabaseWorker::start(&directory).unwrap();
        let database = worker.handle();
        let (_pool, llm) = provision(&database);
        let _first = pending_ready(&database, llm);
        let _second = pending_ready(&database, llm);
        let mut scheduler = scheduler(&mut worker, limits(16, 100));
        let before = scheduler.credit_balance();
        assert_eq!(before, micros(C_MAX));
        match wait(scheduler.sweep_once()) {
            SweepOutcome::Completed(completed) => {
                assert_eq!(completed.grants.len(), 2);
                assert!(completed.ledger.service > Duration::ZERO);
            }
            SweepOutcome::Abandoned(abandoned) => {
                panic!("a healthy sweep completes, got {:?}", abandoned.reason)
            }
        }
        let after = scheduler.credit_balance();
        assert!(
            after < before,
            "active time must leave the bucket lower than it started"
        );
        worker.shutdown().unwrap();
    }

    fn run_in_background(
        worker: &mut DatabaseWorker,
        limits: SchedulerLimits,
        shutdown: CancellationToken,
    ) -> std::thread::JoinHandle<u64> {
        let scheduler = Scheduler::new(
            worker.handle(),
            worker.take_scheduler_wake().unwrap(),
            shutdown,
            limits,
        );
        std::thread::spawn(move || {
            let mut scheduler = scheduler;
            ntex::rt::System::new(
                "scheduler-loop-test",
                ntex::rt::DefaultRuntime,
            )
            .block_on(async move {
                scheduler.run().await;
                scheduler.seq
            })
        })
    }

    fn wait_for_occupancy(database: &DatabaseHandle, want: usize) {
        let deadline = std::time::Instant::now() + crate::test_support::WAIT;
        loop {
            let occupancy = wait(database.occupancy()).unwrap();
            if occupancy.len() == want {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "occupancy {occupancy:?} never reached {want}"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    #[test]
    fn the_loop_claims_a_job_admitted_after_startup_without_losing_its_wake() {
        let (_root, directory) = data_directory("ocg-stage-b-scheduler", "loop-dirty");
        let mut worker = DatabaseWorker::start(&directory).unwrap();
        let database = worker.handle();
        let (_pool, llm) = provision(&database);
        let shutdown = CancellationToken::default();
        let join = run_in_background(&mut worker, limits(16, 100), shutdown.clone());
        let job = pending_ready(&database, llm);
        wait_for_occupancy(&database, 1);
        shutdown.cancel();
        let seq = join.join().unwrap();
        assert!(seq >= 2, "startup plus the dirty wake, got {seq}");
        let occupancy = wait(database.occupancy()).unwrap();
        assert_eq!(occupancy.len(), 1);
        assert_eq!(occupancy[0].1, job);
        worker.shutdown().unwrap();
    }

    #[test]
    fn the_loop_claims_after_the_due_timer_without_violating_the_budget() {
        let (_root, directory) = data_directory("ocg-stage-b-scheduler", "loop-due");
        let mut worker = DatabaseWorker::start(&directory).unwrap();
        let database = worker.handle();
        let (_pool, llm) = provision(&database);
        let at = Timestamp::from_unix_micros(Timestamp::now().as_unix_micros() + 300_000);
        let input = wait(database.save_job_input(None, JobInputDraft::default())).unwrap();
        let job = wait(database.create_job_with_input(
            NewJob {
                name: None,
                execution_mode: ExecutionMode::Once,
                eligible_at: Some(at),
                scheduled_at: None,
                next_due_at: None,
                next_iteration_no: None,
                iteration_no: None,
                parent: database.genesis(),
                projects: Vec::new(),
                dependencies: Vec::new(),
                configuration: Some(ExecutionConfiguration {
                    agent_profile: None,
                    llm_profile: llm,
                }),
                assigned_executor: None,
                previous: None,
                replaces: None,
            },
            input,
        ))
        .unwrap();
        let shutdown = CancellationToken::default();
        let join = run_in_background(&mut worker, limits(16, 100), shutdown.clone());
        wait_for_occupancy(&database, 1);
        shutdown.cancel();
        let seq = join.join().unwrap();
        assert!(seq >= 2, "startup plus the due fire, got {seq}");
        let occupancy = wait(database.occupancy()).unwrap();
        assert_eq!(occupancy, vec![(occupancy[0].0, job)]);
        worker.shutdown().unwrap();
    }

    #[test]
    fn an_idle_loop_with_no_work_starts_no_extra_sweep() {
        let (_root, directory) = data_directory("ocg-stage-b-scheduler", "loop-idle");
        let mut worker = DatabaseWorker::start(&directory).unwrap();
        let shutdown = CancellationToken::default();
        let join = run_in_background(&mut worker, limits(16, 100), shutdown.clone());
        std::thread::sleep(Duration::from_millis(300));
        shutdown.cancel();
        let seq = join.join().unwrap();
        assert_eq!(seq, 1, "one startup sweep and nothing else, got {seq}");
        worker.shutdown().unwrap();
    }

    #[test]
    fn reconciliation_fires_on_its_cadence_and_stays_bounded() {
        let (_root, directory) = data_directory("ocg-stage-b-scheduler", "loop-reconcile");
        let mut worker = DatabaseWorker::start(&directory).unwrap();
        let tiny = SchedulerLimits {
            reconcile: Duration::from_millis(150),
            ..limits(16, 100)
        };
        let shutdown = CancellationToken::default();
        let join = run_in_background(&mut worker, tiny, shutdown.clone());
        std::thread::sleep(Duration::from_millis(500));
        shutdown.cancel();
        let seq = join.join().unwrap();
        assert!(
            (2..=5).contains(&seq),
            "startup plus bounded reconciles, got {seq}"
        );
        worker.shutdown().unwrap();
    }

    #[test]
    fn the_loop_does_not_lose_a_capacity_release_between_refusal_and_parking() {
        let (_root, directory) = data_directory("ocg-stage-b-scheduler", "loop-capacity");
        let mut worker = DatabaseWorker::start(&directory).unwrap();
        let database = worker.handle();
        let (pool, llm) = provision(&database);
        let mut filled = Vec::new();
        for _ in 0..16 {
            let job = pending_ready(&database, llm);
            wait(database.claim_execution(ClaimRequest {
                job,
                allowed_pools: TestSet::from([pool]),
            }))
            .unwrap()
            .unwrap();
            filled.push(job);
        }
        let waiting = pending_ready(&database, llm);
        let shutdown = CancellationToken::default();
        let join = run_in_background(&mut worker, limits(16, 100), shutdown.clone());
        // Give the startup sweep time to refuse global capacity and park.
        std::thread::sleep(Duration::from_millis(300));
        wait(database.finalize_job(filled[0], crate::persistence::JobFinalization::Completed(done_result("free"))))
            .unwrap();
        wait_for_occupancy(&database, 16);
        shutdown.cancel();
        join.join().unwrap();
        let occupancy = wait(database.occupancy()).unwrap();
        assert!(occupancy.iter().any(|(_, job)| *job == waiting));
        worker.shutdown().unwrap();
    }
}
