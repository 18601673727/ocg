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
//! fault. Evidence contradictions and corruption are not contention: the loop
//! stops, leaves the consumed prefix intact, and the failure is reported when
//! the service is shut down.
//!
//! Stopping is cooperative. [`AggregationService::shutdown`] wakes a waiting
//! timer at once, waits for a batch already in flight to reach its actual
//! boundary, joins the loop's thread, and only then ends the service Job.

use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, PoisonError};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::database::{
    AggregationClaim, DatabaseError, DatabaseHandle, DatabaseStatus, TokenConsumeError,
};
use crate::persistence::{
    AvailabilityBatch, ExecutionMode, ExecutorId, ExecutorType, FinalResult, JobFinalization,
    JobId, JobInputDraft, NewExecutor, NewJob,
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
}

/// What a running service is doing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ServiceState {
    Running,
    /// The database refused work at the last check; no write is attempted
    /// until it is open again.
    Paused,
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
    checks: AtomicU64,
    failure: Mutex<Option<String>>,
}

/// Why the service could not start.
#[derive(Debug, thiserror::Error)]
pub enum ServiceStartError {
    #[error("an aggregation service is already running on this database")]
    AlreadyRunning,
    #[error("database operation failed: {0}")]
    Database(#[from] DatabaseError),
    #[error("failed to start the aggregation loop: {0}")]
    Thread(#[source] std::io::Error),
    #[error("the aggregation loop stopped before it was ready")]
    LoopLost,
}

/// How a service run ended.
pub struct ServiceOutcome {
    pub job: JobId,
    /// The loop's own failure, if it stopped by itself.
    pub failure: Option<ServiceFailure>,
    /// Whether the service Job's end could be stored.
    pub finalization: Result<(), DatabaseError>,
}

#[derive(Debug, thiserror::Error)]
#[error("{}", describe(.job, .failure, .unfinalized))]
pub struct ServiceShutdownError {
    pub job: JobId,
    pub failure: Option<ServiceFailure>,
    pub unfinalized: Option<DatabaseError>,
}

fn describe(
    job: &JobId,
    failure: &Option<ServiceFailure>,
    unfinalized: &Option<DatabaseError>,
) -> String {
    let mut text = format!("aggregation service {job}");
    if let Some(failure) = failure {
        text.push_str(&format!(" failed: {failure}"));
    }
    if let Some(error) = unfinalized {
        text.push_str(&format!(
            "; its Job was left unfinished because its end could not be stored: {error}"
        ));
    }
    text
}

impl ServiceOutcome {
    pub fn into_result(self) -> Result<(), Box<ServiceShutdownError>> {
        match (self.failure, self.finalization) {
            (None, Ok(())) => Ok(()),
            (failure, finalization) => Err(Box::new(ServiceShutdownError {
                job: self.job,
                failure,
                unfinalized: finalization.err(),
            })),
        }
    }
}

/// The host of one aggregation loop and its service Job.
pub struct AggregationService {
    database: DatabaseHandle,
    job: JobId,
    control: flume::Sender<Control>,
    thread: Option<JoinHandle<Result<(), ServiceFailure>>>,
    shared: Arc<Shared>,
    _claim: AggregationClaim,
}

impl AggregationService {
    /// Creates and starts this run's service Job, then starts its loop. It
    /// returns once the loop has taken its first wake-up: with an immediate
    /// first check, once that check has finished.
    pub async fn start(
        database: DatabaseHandle,
        config: AggregationServiceConfig,
    ) -> Result<Self, ServiceStartError> {
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
        database.start_job(job).await?;

        let shared = Arc::new(Shared::default());
        let (control, control_receiver) = flume::unbounded();
        let (ready_sender, ready) = flume::bounded(1);
        let thread = {
            let (database, shared) = (database.clone(), shared.clone());
            std::thread::Builder::new()
                .name("ocg-aggregation".to_owned())
                .spawn(move || {
                    ntex::rt::System::new("ocg-aggregation", ntex::rt::DefaultRuntime).block_on(
                        serve(database, control_receiver, shared, config, ready_sender),
                    )
                })
        };
        let thread = match thread {
            Ok(thread) => thread,
            Err(error) => {
                end_job(
                    &database,
                    job,
                    JobFinalization::Failed(summary("the aggregation loop could not start")),
                )
                .await
                .ok();
                return Err(ServiceStartError::Thread(error));
            }
        };
        let mut service = Self {
            database,
            job,
            control,
            thread: Some(thread),
            shared,
            _claim: claim,
        };
        if ready.recv_async().await.is_err() {
            service.stop_loop();
            return Err(ServiceStartError::LoopLost);
        }
        Ok(service)
    }

    /// This run's service Job.
    pub fn job(&self) -> JobId {
        self.job
    }

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
    fn stop_loop(&mut self) -> Option<Result<(), ServiceFailure>> {
        self.shared.stop.store(true, Ordering::SeqCst);
        self.control.send(Control::Stop).ok();
        let thread = self.thread.take()?;
        Some(thread.join().unwrap_or(Err(ServiceFailure::Panicked)))
    }

    /// Stops the loop, and only after it has really stopped ends the service
    /// Job: Completed for a normal stop, Failed when the loop had failed. If
    /// the end cannot be stored the Job stays started, which is what it was.
    pub async fn shutdown(mut self) -> ServiceOutcome {
        let loop_result = self.stop_loop().unwrap_or(Ok(()));
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
        let finalization = end_job(&self.database, self.job, finalization).await;
        ServiceOutcome {
            job: self.job,
            failure,
            finalization,
        }
    }
}

impl Drop for AggregationService {
    /// A service dropped without a shutdown still stops and joins its loop, so
    /// no loop outlives its host. Its Job is left as it was: started.
    fn drop(&mut self) {
        self.stop_loop();
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
        match check(&database, &shared, &config, first).await {
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
        *shared
            .failure
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(failure.to_string());
    }
    result
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
        let outcome = consume(database, config, stream).await?;
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
    config: &AggregationServiceConfig,
    stream: Stream,
) -> Result<StreamOutcome, ServiceFailure> {
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
            Err(error) => classify(database, stream, error),
        },
        Stream::Token => match database.consume_token_usage(BATCH_LIMIT).await {
            Ok(TokenBatch::Consumed(report)) => {
                Ok(StreamOutcome::Consumed(report.consumed_records))
            }
            Ok(TokenBatch::Idle { .. }) => Ok(StreamOutcome::Idle),
            Ok(TokenBatch::Contended { .. }) => Ok(StreamOutcome::Contended),
            Err(TokenConsumeError::Database(error)) => classify(database, stream, error),
            Err(error @ TokenConsumeError::Evidence { .. }) => Err(ServiceFailure::Evidence(error)),
        },
    }
}

/// Sorts a database error into an ordinary outcome or a stop. Only errors
/// that say the database is busy, protected or failed are ordinary; anything
/// else, corruption included, stops the loop rather than being retried.
fn classify(
    database: &DatabaseHandle,
    stream: Stream,
    error: DatabaseError,
) -> Result<StreamOutcome, ServiceFailure> {
    match &error {
        DatabaseError::QueueFull
        | DatabaseError::ReadQueueFull
        | DatabaseError::ControlQueueFull
        | DatabaseError::InsufficientSpace { .. } => Ok(StreamOutcome::Busy),
        DatabaseError::DiskProtected(_)
        | DatabaseError::RefusedByDiskProtection { .. }
        | DatabaseError::ReadsStorageFailed(_) => Ok(StreamOutcome::Paused),
        // A failed commit latches the database, which then is not open. The
        // unknown outcome is not retried: the next consume reads what was
        // really committed.
        DatabaseError::OutcomeUnknown { .. } | DatabaseError::RolledBack(_)
            if database.status() != DatabaseStatus::Open =>
        {
            Ok(StreamOutcome::Paused)
        }
        _ => Err(ServiceFailure::Database { stream, error }),
    }
}
