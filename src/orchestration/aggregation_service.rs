//! The background aggregation service of one process run.
//!
//! One host owns one loop. Starting the host registers (or reuses) a System
//! Executor, creates a new ordinary Continuous service Job under Genesis, with
//! no LLM configuration and no Project, and starts it. The loop then gives the
//! AvailabilityStat and the Token streams one bounded opportunity each per
//! check, with the first opportunity alternating, using the same
//! `consume_availability` and `consume_token_usage` methods as any caller.
//! Batches are internal operations of the service Job, not child Jobs, and an
//! idle check writes nothing: the Job's only writes are its start and its end.
//!
//! The loop never repairs storage. While the database is not open it makes no
//! write attempt, and it resumes through the same consume methods, which read
//! the progress that is actually committed, so no plan or delta survives a
//! fault. A commit whose outcome is unknown is never replayed: its marker is
//! read before that stream runs again. Corruption is not contention: the loop
//! stops, leaves the consumed prefix intact, and the failure is reported when
//! the service is shut down. Token evidence that cannot be reduced stops the
//! Token stream alone, the same way, while Availability goes on.
//!
//! Stopping is cooperative. [`AggregationService::shutdown`] wakes a waiting
//! timer at once, waits for a batch already in flight to reach its actual
//! boundary, joins the loop's thread, and only then ends the service Job.

use std::future::Future;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use crate::database::{
    AggregationClaim, DatabaseError, DatabaseHandle, DatabaseStatus, OperationStatus,
    TokenConsumeError,
};
use crate::persistence::{
    AvailabilityBatch, ExecutionMode, ExecutorId, ExecutorType, FinalResult, JobFinalization,
    JobId, JobInputDraft, NewExecutor, NewJob, OperationId,
};
use crate::token_usage::TokenBatch;

/// How often a running service checks for new raw records.
pub const CHECK_INTERVAL: Duration = Duration::from_secs(5);
/// The most raw records one consume call takes, for either stream.
pub const BATCH_LIMIT: usize = 1_000;
/// The name of the System Executor that stands for this local execution.
pub const EXECUTOR_NAME: &str = "ocg-aggregation";
/// The name of every run's service Job.
pub const JOB_NAME: &str = "OCG aggregation service";

/// The two raw streams the service aggregates.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stream {
    Availability,
    Token,
}

impl Stream {
    fn other(self) -> Self {
        match self {
            Self::Availability => Self::Token,
            Self::Token => Self::Availability,
        }
    }
}

/// How one consume opportunity ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StreamOutcome {
    /// A batch committed; the number of raw records it took.
    Consumed(usize),
    /// Nothing was unconsumed, so nothing was written.
    Idle,
    /// Every preparation was overtaken. Ordinary: try again on a later check.
    Contended,
    /// A queue was full. Ordinary: try again on a later check.
    Busy,
    /// Database protection or a storage failure refuses work.
    Paused,
    /// The commit was known not to happen while the database stayed open.
    /// Ordinary: a later check prepares again from the committed progress.
    Retry,
    /// A commit of this stream has an unknown outcome that is not resolved
    /// yet. The stream does not run until its marker says what committed.
    Uncertain,
    /// The Token stream stopped on evidence it cannot reduce; see
    /// [`ServiceState::Degraded`].
    Halted,
}

/// What one check did, in the order its streams were tried.
#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RoundReport {
    pub(crate) outcomes: Vec<(Stream, StreamOutcome)>,
}

/// Why the loop stopped by itself.
#[derive(Debug, thiserror::Error)]
pub enum ServiceFailure {
    /// The records of one Job contradict each other, or storage is corrupted.
    /// Neither is contention; the loop stopped without skipping a record.
    #[error("the {stream:?} aggregation cannot proceed safely: {error}")]
    Database {
        stream: Stream,
        #[source]
        error: DatabaseError,
    },
    #[error("the Token aggregation cannot proceed safely: {0}")]
    Evidence(#[source] TokenConsumeError),
    #[error("the aggregation loop panicked")]
    Panicked,
    /// The loop ended without being asked to and without a failure of its own.
    #[error("the aggregation loop ended before it was asked to stop")]
    Exited,
}

/// What a running service is doing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ServiceState {
    Running,
    /// The database refused work at the last check; no write is attempted
    /// until it is open again.
    Paused,
    /// The Token stream stopped on evidence it cannot reduce, without
    /// skipping it; the Availability stream goes on. The run ends as Failed.
    Degraded(String),
    /// The loop stopped by itself, with the reason.
    Failed(String),
}

pub struct AggregationServiceConfig {
    /// How long to wait between checks. Clamped to at least one millisecond.
    pub interval: Duration,
    /// Whether the first check runs at once rather than after one interval.
    pub check_immediately: bool,
    #[cfg(test)]
    pub(crate) seams: TestSeams,
}

impl Default for AggregationServiceConfig {
    fn default() -> Self {
        Self {
            interval: CHECK_INTERVAL,
            check_immediately: true,
            #[cfg(test)]
            seams: TestSeams::default(),
        }
    }
}

/// Test-only seams: observe or hold the loop, or stand in for one outcome.
/// Nothing here replaces aggregation itself.
#[cfg(test)]
#[derive(Clone, Default)]
pub(crate) struct TestSeams {
    /// Called on the loop's thread, before each consume call.
    pub(crate) before_consume: Option<Arc<dyn Fn(Stream) + Send + Sync>>,
    /// Returns `Some` to stand in for the consume call of a stream.
    pub(crate) outcome_override: Option<Arc<dyn Fn(Stream) -> Option<StreamOutcome> + Send + Sync>>,
    /// Returns `Some` to stand in for the consume call of a stream with a
    /// database error, which is then handled like a real one.
    pub(crate) error_override: Option<Arc<dyn Fn(Stream) -> Option<DatabaseError> + Send + Sync>>,
    /// Receives every check's report.
    pub(crate) rounds: Option<flume::Sender<RoundReport>>,
}

enum Control {
    Stop,
    /// Test: run a check now.
    #[cfg(test)]
    Tick,
}

#[derive(Default)]
struct Shared {
    stop: AtomicBool,
    paused: AtomicBool,
    /// Set when the loop's thread has finished, however it finished.
    exited: AtomicBool,
    checks: AtomicU64,
    failure: Mutex<Option<String>>,
    /// Why the Token stream stopped, while Availability goes on.
    token_halted: Mutex<Option<String>>,
}

impl Shared {
    fn record_failure(&self, reason: String) {
        self.failure
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_or_insert(reason);
    }
}

/// What is known of an attempt to end a service Job.
#[derive(Debug)]
pub enum JobEnd {
    /// The end was committed.
    Recorded,
    /// The write was refused or rolled back, so nothing was committed and the
    /// Job is still started.
    NotRecorded(DatabaseError),
    /// The write was accepted but its outcome is unknown, so the Job may or
    /// may not have ended. It is not retried.
    Unconfirmed(DatabaseError),
}

impl JobEnd {
    fn of(result: Result<(), DatabaseError>) -> Self {
        match result {
            Ok(()) => Self::Recorded,
            Err(error @ DatabaseError::OutcomeUnknown { .. }) => Self::Unconfirmed(error),
            Err(error) => Self::NotRecorded(error),
        }
    }
}

impl std::fmt::Display for JobEnd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Recorded => f.write_str("its Job's end was recorded"),
            Self::NotRecorded(error) => write!(
                f,
                "its Job's end was refused and nothing was committed, so the Job is still started: {error}"
            ),
            Self::Unconfirmed(error) => write!(
                f,
                "whether its Job's end was committed is unconfirmed: {error}"
            ),
        }
    }
}

/// Why a service Job that was created did not become a running service.
#[derive(Debug)]
pub enum StartCause {
    /// The Job could not be started.
    Database(DatabaseError),
    /// The loop failed or stopped before it was ready, in its first check.
    Loop(ServiceFailure),
}

impl std::fmt::Display for StartCause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Database(error) => write!(f, "the Job could not be started: {error}"),
            Self::Loop(failure) => write!(f, "{failure}"),
        }
    }
}

/// A start that reached a service Job but not a running service. The loop, if
/// there was one, has been joined, and ending the Job as Failed was attempted.
#[derive(Debug, thiserror::Error)]
#[error("aggregation service {job} did not start: {cause}; {end}")]
pub struct FailedStart {
    pub job: JobId,
    pub cause: StartCause,
    pub end: JobEnd,
}

/// Why the service did not start, by how far it got.
#[derive(Debug, thiserror::Error)]
pub enum ServiceStartError {
    /// Another host holds this opening's aggregation loop. Nothing was written.
    #[error("an aggregation service is already running on this database")]
    AlreadyRunning,
    /// A database call failed before any service Job existed.
    #[error("database operation failed: {0}")]
    Database(#[from] DatabaseError),
    /// A service Job was created and the run failed before it was ready.
    #[error("{0}")]
    Failed(Box<FailedStart>),
}

/// How a service run ended.
pub struct ServiceOutcome {
    pub job: JobId,
    /// The loop's own failure, if it stopped by itself.
    pub failure: Option<ServiceFailure>,
    /// What is known of ending the service Job.
    pub end: JobEnd,
}

#[derive(Debug, thiserror::Error)]
#[error("{}", describe(.job, .failure, .end))]
pub struct ServiceShutdownError {
    pub job: JobId,
    pub failure: Option<ServiceFailure>,
    pub end: JobEnd,
}

fn describe(job: &JobId, failure: &Option<ServiceFailure>, end: &JobEnd) -> String {
    let mut text = format!("aggregation service {job}");
    if let Some(failure) = failure {
        text.push_str(&format!(" failed: {failure}"));
    }
    text.push_str(&format!("; {end}"));
    text
}

impl ServiceOutcome {
    pub fn into_result(self) -> Result<(), Box<ServiceShutdownError>> {
        match (self.failure, self.end) {
            (None, JobEnd::Recorded) => Ok(()),
            (failure, end) => Err(Box::new(ServiceShutdownError {
                job: self.job,
                failure,
                end,
            })),
        }
    }
}

/// The host of one aggregation loop and its service Job.
pub struct AggregationService {
    database: DatabaseHandle,
    job: JobId,
    control: flume::Sender<Control>,
    finished: Option<flume::Receiver<Result<(), ServiceFailure>>>,
    shared: Arc<Shared>,
    _claim: Arc<AggregationClaim>,
}

impl AggregationService {
    /// Creates and starts this run's service Job, then starts its loop. It
    /// returns once the loop has taken its first wake-up: with an immediate
    /// first check, once that check has finished.
    pub async fn start(
        database: DatabaseHandle,
        config: AggregationServiceConfig,
    ) -> Result<Self, ServiceStartError> {
        #[cfg(test)]
        if ntex::rt::System::try_current().is_none() {
            return crate::test_support::on_system(move || Box::pin(Self::start(database, config)));
        }

        let claim = database
            .claim_aggregation_host()
            .ok_or(ServiceStartError::AlreadyRunning)?;
        let executor = service_executor(&database).await?;
        let input = database
            .save_job_input(
                None,
                JobInputDraft {
                    objective: Some(
                        "Incrementally aggregate AvailabilityStat and Token usage records".into(),
                    ),
                    ..JobInputDraft::default()
                },
            )
            .await?;
        let job = database
            .create_job_with_input(
                NewJob {
                    name: Some(JOB_NAME.into()),
                    execution_mode: ExecutionMode::Continuous,
                    eligible_at: None,
                    scheduled_at: None,
                    next_due_at: None,
                    next_iteration_no: None,
                    iteration_no: None,
                    parent: database.genesis(),
                    projects: Vec::new(),
                    dependencies: Vec::new(),
                    configuration: None,
                    assigned_executor: Some(executor),
                    previous: None,
                    replaces: None,
                },
                input,
            )
            .await?;
        if let Err(error) = database.start_local_job(job).await {
            let end = end_failed(&database, job, "the Job could not be started").await;
            return Err(failed_start(job, StartCause::Database(error), end));
        }

        let claim = Arc::new(claim);
        let shared = Arc::new(Shared::default());
        let (control, control_receiver) = flume::unbounded();
        let (ready_sender, ready) = flume::bounded(1);
        let (done, finished) = flume::bounded(1);
        {
            let (database, shared, claim) = (database.clone(), shared.clone(), claim.clone());
            ntex::rt::spawn(async move {
                let _claim = claim;
                let mut serving = std::pin::pin!(serve(
                    database,
                    control_receiver,
                    shared.clone(),
                    config,
                    ready_sender,
                ));
                let result = std::future::poll_fn(|context| {
                    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        serving.as_mut().poll(context)
                    })) {
                        Ok(result) => result,
                        Err(payload) => {
                            shared.record_failure(format!(
                                "{}: {}",
                                ServiceFailure::Panicked,
                                panic_message(payload.as_ref())
                            ));
                            std::task::Poll::Ready(Err(ServiceFailure::Panicked))
                        }
                    }
                })
                .await;
                shared.exited.store(true, Ordering::SeqCst);
                if done.send(result).is_err() {
                    log::debug!("aggregation host dropped before its loop ended");
                }
            });
        }
        let mut service = Self {
            database,
            job,
            control,
            finished: Some(finished),
            shared,
            _claim: claim,
        };
        if ready.recv_async().await.is_err() {
            // The loop ended before it was ready. Its own result says why, and
            // it is joined before the Job is ended and before the claim goes.
            let failure = match service.stop_loop().await {
                Some(Err(failure)) => failure,
                Some(Ok(())) | None => ServiceFailure::Exited,
            };
            let reason = format!("the aggregation service failed to start: {failure}");
            let end = JobEnd::of(
                end_job(
                    &service.database,
                    job,
                    JobFinalization::Failed(summary(&reason)),
                )
                .await,
            );
            return Err(failed_start(job, StartCause::Loop(failure), end));
        }
        Ok(service)
    }

    /// This run's service Job.
    pub fn job(&self) -> JobId {
        self.job
    }

    /// Never `Running` once the loop's thread has finished: a loop that
    /// panicked or ended without being asked to is `Failed`.
    pub fn state(&self) -> ServiceState {
        if let Some(failure) = self
            .shared
            .failure
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
        {
            return ServiceState::Failed(failure);
        }
        if self.shared.exited.load(Ordering::SeqCst) {
            return ServiceState::Failed(ServiceFailure::Exited.to_string());
        }
        if let Some(reason) = self
            .shared
            .token_halted
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
        {
            return ServiceState::Degraded(reason);
        }
        if self.shared.paused.load(Ordering::SeqCst) {
            ServiceState::Paused
        } else {
            ServiceState::Running
        }
    }

    /// The checks the loop has finished.
    pub fn checks(&self) -> u64 {
        self.shared.checks.load(Ordering::SeqCst)
    }

    /// Test: asks for a check now, without waiting for the timer.
    #[cfg(test)]
    pub(crate) fn tick(&self) -> bool {
        self.control.send(Control::Tick).is_ok()
    }

    /// Asks the loop to stop, wakes it, and joins it. A batch in flight
    /// finishes first: it is awaited, never abandoned.
    async fn stop_loop(&mut self) -> Option<Result<(), ServiceFailure>> {
        self.shared.stop.store(true, Ordering::SeqCst);
        self.control.send(Control::Stop).ok();
        let finished = self.finished.take()?;
        Some(
            finished
                .recv_async()
                .await
                .unwrap_or(Err(ServiceFailure::Panicked)),
        )
    }

    /// Stops the loop, and only after it has really stopped ends the service
    /// Job: Completed for a normal stop, Failed when the loop had failed. If
    /// the end is refused the Job stays started; if its outcome is unknown,
    /// that is reported as unconfirmed.
    pub async fn shutdown(mut self) -> ServiceOutcome {
        let loop_result = self.stop_loop().await.unwrap_or(Ok(()));
        let (failure, finalization) = match loop_result {
            Ok(()) => (
                None,
                JobFinalization::Completed(summary("the aggregation service stopped")),
            ),
            Err(failure) => {
                let text = format!("the aggregation service failed: {failure}");
                (Some(failure), JobFinalization::Failed(summary(&text)))
            }
        };
        let end = JobEnd::of(end_job(&self.database, self.job, finalization).await);
        ServiceOutcome {
            job: self.job,
            failure,
            end,
        }
    }
}

impl Drop for AggregationService {
    /// A service dropped without a shutdown still stops and joins its loop, so
    /// no loop outlives its host. Its Job is left as it was: started.
    fn drop(&mut self) {
        self.shared.stop.store(true, Ordering::SeqCst);
        if self.control.send(Control::Stop).is_err() {
            log::debug!("aggregation loop already stopped");
        }
    }
}

fn summary(text: &str) -> FinalResult {
    FinalResult {
        summary: Some(text.to_owned()),
        code: None,
        goal_reached: None,
        value: None,
    }
}

fn failed_start(job: JobId, cause: StartCause, end: JobEnd) -> ServiceStartError {
    ServiceStartError::Failed(Box::new(FailedStart { job, cause, end }))
}

/// Ends a Job that never became a running service as Failed.
async fn end_failed(database: &DatabaseHandle, job: JobId, reason: &str) -> JobEnd {
    let text = format!("the aggregation service failed to start: {reason}");
    JobEnd::of(end_job(database, job, JobFinalization::Failed(summary(&text))).await)
}

async fn end_job(
    database: &DatabaseHandle,
    job: JobId,
    finalization: JobFinalization,
) -> Result<(), DatabaseError> {
    database.finalize_job(job, finalization).await.map(|_| ())
}

/// The System Executor standing for this local execution: an existing one
/// with the service's name, or a newly registered one. Restarts reuse it, so
/// the Executor table does not grow with them.
async fn service_executor(database: &DatabaseHandle) -> Result<ExecutorId, DatabaseError> {
    let mut before = None;
    loop {
        let page = database.executors(before, 100).await?;
        if let Some(found) = page.iter().find(|executor| {
            executor.name == EXECUTOR_NAME && executor.executor_type == ExecutorType::System
        }) {
            return Ok(found.id);
        }
        match page.last() {
            Some(last) if page.len() == 100 => before = Some(last.id),
            _ => break,
        }
    }
    database
        .create_executor(NewExecutor {
            name: EXECUTOR_NAME.into(),
            executor_type: ExecutorType::System,
            concurrency_limit: NonZeroU32::MIN,
            llm_transports: Default::default(),
        })
        .await
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|message| (*message).to_owned())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "a panic without a message".to_owned())
}

/// Gives the other waiting tasks a turn: pending once, then ready.
async fn yield_now() {
    let mut yielded = false;
    std::future::poll_fn(|cx| {
        if yielded {
            std::task::Poll::Ready(())
        } else {
            yielded = true;
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    })
    .await;
}

enum Wake {
    Check,
    Stop,
}

async fn wait_for_check(control: &flume::Receiver<Control>, interval: Duration) -> Wake {
    match ntex::time::timeout(interval, control.recv_async()).await {
        Err(_) => Wake::Check,
        Ok(Ok(Control::Stop)) | Ok(Err(_)) => Wake::Stop,
        #[cfg(test)]
        Ok(Ok(Control::Tick)) => Wake::Check,
    }
}

async fn serve(
    database: DatabaseHandle,
    control: flume::Receiver<Control>,
    shared: Arc<Shared>,
    config: AggregationServiceConfig,
    ready: flume::Sender<()>,
) -> Result<(), ServiceFailure> {
    let interval = config.interval.max(Duration::from_millis(1));
    let mut first = Stream::Availability;
    let mut due = config.check_immediately;
    let mut ready = Some(ready);
    let mut streams = Streams::default();
    let result = loop {
        if shared.stop.load(Ordering::SeqCst) {
            break Ok(());
        }
        if !due {
            if let Some(ready) = ready.take() {
                ready.send(()).ok();
            }
            if let Wake::Stop = wait_for_check(&control, interval).await {
                break Ok(());
            }
        }
        due = false;
        if shared.stop.load(Ordering::SeqCst) {
            break Ok(());
        }
        match check(&database, &shared, &config, &mut streams, first).await {
            Ok(_report) =>
            {
                #[cfg(test)]
                if let Some(rounds) = &config.seams.rounds {
                    rounds.send(_report).ok();
                }
            }
            Err(failure) => break Err(failure),
        }
        shared.checks.fetch_add(1, Ordering::SeqCst);
        first = first.other();
        if let Some(ready) = ready.take() {
            ready.send(()).ok();
        }
    };
    if let Err(failure) = &result {
        // Best effort: the logger may itself be protected.
        log::error!("the aggregation service stopped: {failure}");
        shared.record_failure(failure.to_string());
    }
    // A halted Token stream fails the run once the loop has stopped, so its
    // Job ends as Failed with the evidence that halted it.
    match (result, streams.token_halted) {
        (Ok(()), Some(error)) => Err(ServiceFailure::Evidence(error)),
        (result, _) => result,
    }
}

/// What the loop knows about its streams between checks.
#[derive(Default)]
struct Streams {
    /// Set once Token evidence cannot be reduced; the Token stream then stays
    /// stopped for this run, with its progress where it was.
    token_halted: Option<TokenConsumeError>,
    /// A commit of each stream whose outcome is unknown, resolved by its
    /// marker before that stream runs again.
    uncertain: [Option<OperationId>; 2],
}

impl Streams {
    fn uncertain(&mut self, stream: Stream) -> &mut Option<OperationId> {
        &mut self.uncertain[match stream {
            Stream::Availability => 0,
            Stream::Token => 1,
        }]
    }
}

/// What a check reports.
#[cfg(test)]
type Report = RoundReport;
#[cfg(not(test))]
type Report = ();

/// One check: at most one consume call per stream, `first` first, with a
/// yield between them. A paused database ends the check without a write
/// attempt; a failure that is not contention ends the loop.
async fn check(
    database: &DatabaseHandle,
    shared: &Shared,
    config: &AggregationServiceConfig,
    streams: &mut Streams,
    first: Stream,
) -> Result<Report, ServiceFailure> {
    #[cfg(test)]
    let mut outcomes = Vec::new();
    for stream in [first, first.other()] {
        if shared.stop.load(Ordering::SeqCst) {
            break;
        }
        if database.status() != DatabaseStatus::Open {
            shared.paused.store(true, Ordering::SeqCst);
            #[cfg(test)]
            outcomes.push((stream, StreamOutcome::Paused));
            break;
        }
        let outcome = consume(database, shared, config, streams, stream).await?;
        #[cfg(test)]
        outcomes.push((stream, outcome));
        if outcome == StreamOutcome::Paused {
            shared.paused.store(true, Ordering::SeqCst);
            break;
        }
        shared.paused.store(false, Ordering::SeqCst);
        yield_now().await;
    }
    #[cfg(test)]
    return Ok(RoundReport { outcomes });
    #[cfg(not(test))]
    Ok(())
}

async fn consume(
    database: &DatabaseHandle,
    shared: &Shared,
    config: &AggregationServiceConfig,
    streams: &mut Streams,
    stream: Stream,
) -> Result<StreamOutcome, ServiceFailure> {
    if stream == Stream::Token && streams.token_halted.is_some() {
        return Ok(StreamOutcome::Halted);
    }
    // An unknown commit is never replayed: its marker says whether it
    // committed, and only then does the stream prepare again, from the
    // progress that is actually committed.
    if let Some(operation) = *streams.uncertain(stream) {
        match database.reconcile_operation(operation).await {
            Ok(OperationStatus::NotCommittedYet) => return Ok(StreamOutcome::Uncertain),
            Ok(OperationStatus::Committed(_) | OperationStatus::NotCommitted) => {
                *streams.uncertain(stream) = None;
            }
            Err(error) => return classify(database, streams, stream, error),
        }
    }
    #[cfg(test)]
    {
        if let Some(hold) = &config.seams.before_consume {
            hold(stream);
        }
        if let Some(outcome) = config
            .seams
            .outcome_override
            .as_ref()
            .and_then(|stand_in| stand_in(stream))
        {
            return Ok(outcome);
        }
        if let Some(error) = config
            .seams
            .error_override
            .as_ref()
            .and_then(|stand_in| stand_in(stream))
        {
            return classify(database, streams, stream, error);
        }
    }
    #[cfg(not(test))]
    let _ = config;
    match stream {
        Stream::Availability => match database.consume_availability(BATCH_LIMIT).await {
            Ok(AvailabilityBatch::Consumed(report)) => {
                Ok(StreamOutcome::Consumed(report.consumed_records))
            }
            Ok(AvailabilityBatch::Idle { .. }) => Ok(StreamOutcome::Idle),
            Ok(AvailabilityBatch::Contended { .. }) => Ok(StreamOutcome::Contended),
            Err(error) => classify(database, streams, stream, error),
        },
        Stream::Token => match database.consume_token_usage(BATCH_LIMIT).await {
            Ok(TokenBatch::Consumed(report)) => {
                Ok(StreamOutcome::Consumed(report.consumed_records))
            }
            Ok(TokenBatch::Idle { .. }) => Ok(StreamOutcome::Idle),
            Ok(TokenBatch::Contended { .. }) => Ok(StreamOutcome::Contended),
            Err(TokenConsumeError::Database(error)) => classify(database, streams, stream, error),
            // The refused batch committed nothing and the progress stays
            // before the evidence, so nothing is skipped. Only the Token
            // stream stops; the Availability progress is its own.
            Err(error @ TokenConsumeError::Evidence { .. }) => {
                let reason = format!("the Token aggregation cannot proceed safely: {error}");
                log::error!("the Token aggregation stopped: {reason}");
                *shared
                    .token_halted
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner) = Some(reason);
                streams.token_halted = Some(error);
                Ok(StreamOutcome::Halted)
            }
        },
    }
}

/// Sorts a database error into an ordinary outcome or a stop. Only errors
/// that say the database is busy, protected or failed, or that a commit is
/// known not to have happened, are ordinary; anything else, corruption
/// included, stops the loop rather than being retried. An unknown commit
/// outcome is kept, so the stream resolves it before it runs again.
fn classify(
    database: &DatabaseHandle,
    streams: &mut Streams,
    stream: Stream,
    error: DatabaseError,
) -> Result<StreamOutcome, ServiceFailure> {
    let open = database.status() == DatabaseStatus::Open;
    match &error {
        DatabaseError::QueueFull
        | DatabaseError::ReadQueueFull
        | DatabaseError::ControlQueueFull
        | DatabaseError::InsufficientSpace { .. } => Ok(StreamOutcome::Busy),
        DatabaseError::DiskProtected(_)
        | DatabaseError::RefusedByDiskProtection { .. }
        | DatabaseError::ReadsStorageFailed(_) => Ok(StreamOutcome::Paused),
        DatabaseError::OutcomeUnknown { operation, .. } => {
            *streams.uncertain(stream) = Some(*operation);
            // A failed commit latches the database, which then is not open.
            Ok(if open {
                StreamOutcome::Uncertain
            } else {
                StreamOutcome::Paused
            })
        }
        // Nothing was committed. While storage is latched the stream waits
        // for recovery; otherwise the next check simply prepares again.
        DatabaseError::RolledBack(_) if !open => Ok(StreamOutcome::Paused),
        DatabaseError::RolledBack(_) => Ok(StreamOutcome::Retry),
        _ => Err(ServiceFailure::Database { stream, error }),
    }
}
