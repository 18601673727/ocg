//! The dormant FIFO cohort Scheduler of G3 stage B, milestone M2.
//!
//! Nothing in production constructs a [`Scheduler`] or starts its loop: HTTP
//! and the CLI keep using the legacy runtime, and the activation milestone is
//! the one that starts this component. Building the core here lets its claim,
//! capture and sweep contracts be verified while no production path reaches
//! it.
//!
//! Two boundaries are deliberate and must not be crossed here:
//!
//! - An uncertain claim outcome is recorded and halts claiming, but nothing
//!   settles it. [`DatabaseHandle::reconcile_unexecuted_claim`] stays unwired
//!   until the never-spawned release question is decided.
//! - No external execution capacity is released, because no execution runs:
//!   a grant is recorded, never spawned from here.
#![allow(dead_code, reason = "M2 keeps Scheduler dispatch dormant")]

use std::future::Future;
use std::num::{NonZeroU32, NonZeroUsize};
use std::time::Duration;

use crate::database::{DatabaseError, DatabaseHandle, SchedulerWakeSubscription, StorageErrorKind};
use crate::persistence::{
    MAX_PENDING_JOBS, PendingCapturePage, PendingFence, PendingIndexFault, PendingJobKey,
    PendingReadFault,
};
use crate::provider_execution::CancellationToken;

/// How long the Scheduler waits before retrying a read the read queue refused,
/// and the cap of that backoff. A refusal examines no index entry, so the
/// retry keeps every cursor where it was.
const RETRY_START: Duration = Duration::from_millis(5);
const RETRY_CAP: Duration = Duration::from_millis(500);

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

/// The dormant Scheduler. It owns no durable state: every sweep works from the
/// database's committed facts, and a sweep that is abandoned loses nothing.
pub(crate) struct Scheduler {
    database: DatabaseHandle,
    wake: SchedulerWakeSubscription,
    shutdown: CancellationToken,
    limits: SchedulerLimits,
}

impl Scheduler {
    pub(crate) fn new(
        database: DatabaseHandle,
        wake: SchedulerWakeSubscription,
        shutdown: CancellationToken,
        limits: SchedulerLimits,
    ) -> Self {
        Self {
            database,
            wake,
            shutdown,
            limits,
        }
    }

    /// One capture, under the approved error policy: a retryable read refusal
    /// is retried on the same cursor, an entry-level index fault degrades to
    /// the keys-only snapshot, engine or metadata damage halts the sweep, and
    /// a keys-only read that cannot be held in memory falls back to streaming
    /// page by page.
    pub(crate) async fn capture(&self) -> Result<Captured, CaptureFailure> {
        let mut captured = Captured::default();
        let first = match self.capture_page(None, &mut captured).await {
            CaptureStep::Page(page) => page,
            CaptureStep::Fault(PendingReadFault::IndexEntry { fence, key, kind, .. }) => {
                captured.alarm(CaptureAlarm::IndexEntry { key, fault: kind });
                return self.snapshot(fence, captured).await;
            }
            CaptureStep::Fault(PendingReadFault::Allocation { fence, .. }) => {
                captured.alarm(CaptureAlarm::AllocationExhausted);
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
            captured.alarm(CaptureAlarm::OverCapacity { len: fence.len });
            return Ok(captured.stream(fence));
        }
        let mut keys = Vec::new();
        if keys.try_reserve_exact(fence.len as usize).is_err() {
            captured.alarm(CaptureAlarm::AllocationExhausted);
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
                .capture_page(page.page.last_visited, &mut captured)
                .await
            {
                CaptureStep::Page(next) => page = next,
                CaptureStep::Fault(PendingReadFault::IndexEntry { key, kind, .. }) => {
                    captured.alarm(CaptureAlarm::IndexEntry { key, fault: kind });
                    drop(keys);
                    return self.snapshot(fence, captured).await;
                }
                CaptureStep::Fault(PendingReadFault::Allocation { .. }) => {
                    captured.alarm(CaptureAlarm::AllocationExhausted);
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
            captured.alarm(CaptureAlarm::AllocationExhausted);
            return Ok(captured.stream(fence));
        };
        let began = std::time::Instant::now();
        let read = self
            .retry(|| self.database.pending_snapshot(budget))
            .await;
        captured.service += began.elapsed();
        match read {
            None => Err(CaptureFailure::Shutdown),
            Some(Err(error)) => Err(classify(&error)),
            Some(Ok(Ok(snapshot))) if snapshot.complete => Ok(captured.material(snapshot.keys)),
            Some(Ok(Ok(snapshot))) => {
                captured.alarm(CaptureAlarm::OverCapacity {
                    len: snapshot.fence.len,
                });
                Ok(captured.stream(snapshot.fence))
            }
            Some(Ok(Err(PendingReadFault::Allocation { fence, .. }))) => {
                captured.alarm(CaptureAlarm::AllocationExhausted);
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

    /// One capture page, retried while the read queue refuses it.
    async fn capture_page(
        &self,
        after: Option<PendingJobKey>,
        captured: &mut Captured,
    ) -> CaptureStep<PendingCapturePage> {
        let began = std::time::Instant::now();
        let read = self
            .retry(|| self.database.pending_capture_page(after, self.limits.capture_page))
            .await;
        captured.service += began.elapsed();
        match read {
            None => CaptureStep::Failed(CaptureFailure::Shutdown),
            Some(Err(error)) => CaptureStep::Failed(classify(&error)),
            Some(Ok(Ok(page))) => CaptureStep::Page(page),
            Some(Ok(Err(fault))) => CaptureStep::Fault(fault),
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
}

/// One step of a retried capture page.
enum CaptureStep<T> {
    Page(T),
    Fault(PendingReadFault),
    Failed(CaptureFailure),
}

/// What one capture produced, and what it met while producing it.
#[derive(Debug, Default)]
pub(crate) struct Captured {
    pub(crate) cohort: Option<Cohort>,
    /// The read-worker service time the capture used, for the Scheduler's
    /// active-time budget.
    pub(crate) service: Duration,
    pub(crate) alarms: Vec<CaptureAlarm>,
}

impl Captured {
    fn alarm(&mut self, alarm: CaptureAlarm) {
        log::warn!("scheduler capture: {alarm}");
        self.alarms.push(alarm);
    }

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
        captured: &mut Captured,
    ) -> Result<Option<PendingJobKey>, CaptureFailure> {
        match self {
            Self::Material { keys, next } => {
                let key = keys.get(*next).copied();
                if key.is_some() {
                    *next += 1;
                }
                Ok(key)
            }
            Self::Stream(cursor) => cursor.next(scheduler, captured).await,
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
        captured: &mut Captured,
    ) -> Result<Option<PendingJobKey>, CaptureFailure> {
        loop {
            if self.next < self.page.len() {
                let key = self.page[self.next];
                self.next += 1;
                self.visits += 1;
                if self.visits > self.bound {
                    captured.alarm(CaptureAlarm::StreamBoundExceeded {
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
            let began = std::time::Instant::now();
            let read = scheduler
                .retry(|| {
                    scheduler
                        .database
                        .pending_keys(self.after, scheduler.limits.capture_page)
                })
                .await;
            captured.service += began.elapsed();
            match read {
                None => return Err(CaptureFailure::Shutdown),
                Some(Err(error)) => return Err(classify(&error)),
                Some(Ok(Ok(page))) => {
                    self.after = page.page.last_visited;
                    self.page = page.page.visited;
                    self.next = 0;
                    self.exhausted = page.page.exhausted;
                }
                Some(Ok(Err(PendingReadFault::IndexEntry { key, kind, .. }))) => {
                    captured.alarm(CaptureAlarm::IndexEntry { key, fault: kind });
                    return Err(CaptureFailure::IntegrityHalt(format!(
                        "the keys-only page reported {kind:?} at {key:?}"
                    )));
                }
                Some(Ok(Err(PendingReadFault::Allocation { .. }))) => {
                    captured.alarm(CaptureAlarm::AllocationExhausted);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::DatabaseWorker;
    use crate::persistence::{
        ExecutionMode, JobId, JobInputDraft, NewJob, PendingJobsPage,
    };
    use crate::storage::StorageError;
    use crate::test_support::{data_directory, wait};

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
        assert!(captured.alarms.is_empty(), "{:?}", captured.alarms);
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
            captured.alarms,
            vec![CaptureAlarm::OverCapacity { len: 6 }]
        );
        let cohort = captured.cohort.expect("a cohort");
        assert_eq!(cohort.len(), 6);
        let mut cursor = cohort.cursor(&limits(2, 2));
        let mut captured = Captured::default();
        let mut drained = Vec::new();
        while let Some(key) =
            wait(cursor.next_key(&scheduler, &mut captured)).expect("the stream continues")
        {
            drained.push(key);
        }
        assert_eq!(drained, expected);
        assert!(captured.alarms.is_empty(), "{:?}", captured.alarms);
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
            wait(cursor.next_key(&scheduler, &mut captured)).expect("the stream continues")
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
                captured.alarms.as_slice(),
                [CaptureAlarm::IndexEntry {
                    fault: PendingIndexFault::MissingJob,
                    ..
                }]
            ),
            "{:?}",
            captured.alarms
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
            wait(cursor.next_key(&scheduler, &mut captured)).expect("the stream continues")
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
            wait(cursor.next_key(&scheduler, &mut captured)).expect("the stream continues")
        {
            drained.push(key);
        }
        assert_eq!(
            captured.alarms,
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
}
