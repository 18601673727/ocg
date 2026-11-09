//! The native scheduler: picks submitted native Jobs and runs them without a
//! per-Job invocation, under [`ExecutionControl::AutonomousScheduling`] and
//! nothing else.
//!
//! It is not manual loopback execution, not an external trial, not ordinary
//! manual external execution and not recovery: it holds none of their
//! authority and none of theirs enables it. It is reached only from the host's
//! execution runtime ([`crate::system::scheduler`]), only in the host that
//! serves stored Jobs, and only when the build enables its control and does
//! not enable [`ExecutionControl::LegacyProviderExecution`]. In a build where
//! the control is disabled no [`SchedulingAuthority`] exists, so nothing here
//! claims, starts or sends anything.
//!
//! # Selection
//!
//! A pass offers each enabled Executor that has the single-request Generative
//! capability, in an order that rotates from pass to pass, to the writer,
//! which selects and reserves in one transaction
//! ([`crate::db::entities::ReadySelection::SingleRequestGenerative`]): the
//! first ready Job in the ready index's order (priority, then submission time,
//! then Job) whose frozen configuration can run as the checked single
//! tool-free request and whose frozen endpoint this scheduler may reach.
//! Everything else is passed over untouched and stays queued: Decisions, Jobs
//! whose Agent needs tools or a Workspace, other protocols, and every endpoint
//! that is not a literal loopback one, because the only transport this
//! scheduler holds is [`EndpointPolicy::Loopback`]. A busy Executor, a full
//! Pool and the global limit are refused before any ready entry is read, and a
//! pass that reserves nothing writes nothing, so it wakes nobody.
//!
//! Eligibility does not depend on which capable Executor asks, so the scan
//! position is shared. Each Executor's search starts at the head of the index;
//! when a long run of ineligible entries keeps it from getting further, a
//! bounded search reads the head first and then resumes where the last one
//! stopped, so every ready entry is reached within a bounded number of passes.
//! The order within that reach is the index's; an entry behind the resume
//! point can be reserved before an earlier entry that became eligible while
//! the head search could not reach it.
//!
//! # Dispatch and stop
//!
//! Each reservation runs once: the checked start, with the same request check
//! as manual loopback execution, and one bounded exchange. Its cancellation is
//! registered under its Allocation's token before the start, so a stop
//! ([`NativeStops`]) reaches exactly that execution. Nothing here retries,
//! replays, reconciles or releases. A start refused before any request ends
//! the reservation and fails the Job; an exchange that may have reached the
//! Provider leaves its Allocation held as Uncertain, and a held Executor is
//! never offered again in this run. A restart never resumes anything either:
//! while [`ExecutionControl::RecoveryAndRedispatch`] is disabled the database
//! refuses to open with any Allocation held. Once any reservation or dispatch
//! ends in a state the database does not show as settled or Uncertain, the
//! scheduler stops claiming for the rest of the run.

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use crate::db::entities::{
    AllocationPhase, AllocationToken, DevelopmentFreshReservation, DevelopmentReadyCursor,
    DevelopmentReservation, ExecutionOperation, ExecutionPoolId, ExecutorId, JobId, LlmTransport,
    OutcomeKind, PoolAllocation, UnstartedCause,
};
use crate::db::redb::{NativeResource, NativeResourceKind};
use crate::db::{
    DatabaseError, DatabaseHandle, DevelopmentHandoffError, DevelopmentReservationError,
    SchedulerWakeSubscription,
};
use crate::service::ai::providers::{CancellationToken, ProviderExecutionConfig};
use crate::system::execution_controls::ExecutionControl;
use crate::system::executor::execution_runtime::{
    EndpointPolicy, supervise_development_generative,
};
use crate::system::manual_execution::within_deadline;
use crate::system::manual_loopback::{MAX_TIMEOUT_CEILING, check_prepared};
use crate::system::scheduler::{DispatchBatch, Scheduler};

/// Executors one list read returns.
const EXECUTOR_PAGE: usize = 100;
/// Executor pages one pass reads.
const MAX_EXECUTOR_PAGES: usize = 16;
/// Ready-index pages (each one bounded transaction) one Executor's search may
/// read.
const MAX_READY_PAGES: usize = 8;
/// Of those, the pages read from the head of the index before a search jumps
/// to where the last one stopped.
const HEAD_PAGES: usize = 2;
/// The longest exchange: the longest an operator may allow a manual loopback
/// execution.
const DISPATCH_TIMEOUT: Duration = MAX_TIMEOUT_CEILING;
/// The response bytes the exchange reads before it gives up.
const MAX_RESPONSE_BYTES: usize = 1 << 20;
/// What an Executor must offer for the one execution this scheduler runs.
const CAPABILITY: (ExecutionOperation, Option<LlmTransport>) = (
    ExecutionOperation::LlmRequest,
    Some(LlmTransport::OpenAiChatCompletions),
);

/// The authority of one autonomous scheduling run. [`Self::autonomous`] is
/// the only way to make one, and it makes none unless the build enables
/// [`ExecutionControl::AutonomousScheduling`]. It grants no transport: the
/// scheduler's own endpoint policy is loopback only.
pub(crate) struct SchedulingAuthority(());

impl SchedulingAuthority {
    pub(crate) fn autonomous() -> Option<Self> {
        ExecutionControl::AutonomousScheduling
            .is_enabled()
            .then_some(Self(()))
    }
}

/// The endpoints this scheduler may send to: a literal loopback endpoint the
/// configured proxy does not intercept, exactly what
/// [`EndpointPolicy::Loopback`] admits at the exchange. An external endpoint
/// would need an external transport authority, which this scheduler does not
/// hold.
fn scheduled_endpoint(endpoint: &str) -> bool {
    crate::system::http::direct_diagnostic_loopback(endpoint)
}

/// The cancellations of the executions this host's native scheduler runs, by
/// Job, each with the token of the Allocation it runs under. A stop reaches
/// an execution only through the token the database recorded the stop for,
/// so it never reaches another Job, another Executor's work, a later
/// Allocation or the host. Registered before the checked start and removed
/// when the execution's task ends.
#[derive(Clone, Default)]
pub(crate) struct NativeStops(Arc<Mutex<BTreeMap<JobId, (AllocationToken, CancellationToken)>>>);

impl NativeStops {
    fn register(&self, token: AllocationToken, cancellation: CancellationToken) -> Registration {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(token.job, (token, cancellation));
        Registration {
            stops: self.clone(),
            token,
        }
    }

    /// Cancels the execution registered under exactly `token`, and says
    /// whether there was one. Cancelling twice has no further effect.
    pub(crate) fn cancel(&self, token: AllocationToken) -> bool {
        match self
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&token.job)
        {
            Some((registered, cancellation)) if *registered == token => {
                cancellation.cancel();
                true
            }
            Some(_) | None => false,
        }
    }
}

/// Removes its execution's entry from [`NativeStops`] when dropped, however
/// the task ends.
struct Registration {
    stops: NativeStops,
    token: AllocationToken,
}

impl Drop for Registration {
    fn drop(&mut self) {
        let mut stops = self.stops.0.lock().unwrap_or_else(PoisonError::into_inner);
        if stops
            .get(&self.token.job)
            .is_some_and(|(registered, _)| *registered == self.token)
        {
            stops.remove(&self.token.job);
        }
    }
}

/// Runs the native scheduler until `shutdown`, then waits for every dispatch
/// it started to settle what it can. Without [`SchedulingAuthority`] it only
/// waits for `shutdown`.
pub(crate) async fn run(
    database: DatabaseHandle,
    wake: SchedulerWakeSubscription,
    shutdown: CancellationToken,
    stops: NativeStops,
) {
    let Some(authority) = SchedulingAuthority::autonomous() else {
        shutdown.cancelled().await;
        return;
    };
    run_with(authority, database, wake, shutdown, stops).await;
}

/// [`run`] under an authority already held.
pub(crate) async fn run_with(
    authority: SchedulingAuthority,
    database: DatabaseHandle,
    wake: SchedulerWakeSubscription,
    shutdown: CancellationToken,
    stops: NativeStops,
) {
    log::info!("native scheduler running: single-request Generative Jobs, loopback endpoints only");
    let scheduler = Rc::new(NativeScheduler::new(
        database.clone(),
        authority,
        shutdown.clone(),
        stops,
    ));
    Scheduler::new(&database, wake, shutdown)
        .run(
            || {
                let scheduler = scheduler.clone();
                async move { scheduler.claim().await }
            },
            |allocation| {
                let scheduler = scheduler.clone();
                async move { scheduler.spawn(allocation) }
            },
        )
        .await;
    scheduler.drain().await;
    log::info!("native scheduler stopped");
}

/// What one Executor's search came to.
pub(crate) enum ExecutorPass {
    Reserved(Box<PoolAllocation>),
    /// Nothing for this Executor now: it is busy, its Pool is full or the
    /// global limit is reached. Nothing was read or written.
    Nothing,
    /// The search ran out of budget; the next one resumes where it stopped.
    /// Nothing was written.
    BudgetSpent,
    /// The search read to the end of the ready index without finding an
    /// eligible Job, from the head or completing a rotation that earlier
    /// searches began. The pass ends: another Executor would only begin the
    /// next rotation, and whatever makes a Job eligible wakes the scheduler.
    IndexExhausted,
    /// A reservation may have committed without a reply; the scheduler stops
    /// claiming.
    Halted,
}

/// The native scheduler of one host run.
pub(crate) struct NativeScheduler {
    database: DatabaseHandle,
    _authority: SchedulingAuthority,
    shutdown: CancellationToken,
    stops: NativeStops,
    /// Where the last search that ran out of budget stopped. Process memory
    /// only: a new run starts at the head of the ready index.
    resume: Cell<Option<DevelopmentReadyCursor>>,
    /// Passes so far, which rotate the order Executors are offered in.
    passes: Cell<u64>,
    running: RefCell<Vec<ntex::rt::JoinHandle<Dispatched>>>,
    /// Set once anything ends in a state the database does not show as
    /// settled or Uncertain; nothing is claimed after it.
    halted: Rc<Cell<bool>>,
}

impl NativeScheduler {
    pub(crate) fn new(
        database: DatabaseHandle,
        authority: SchedulingAuthority,
        shutdown: CancellationToken,
        stops: NativeStops,
    ) -> Self {
        Self {
            database,
            _authority: authority,
            shutdown,
            stops,
            resume: Cell::new(None),
            passes: Cell::new(0),
            running: RefCell::new(Vec::new()),
            halted: Rc::new(Cell::new(false)),
        }
    }

    fn halt(&self, reason: &str) {
        if !self.halted.replace(true) {
            log::error!(
                "native scheduler stopped claiming: {reason}; run `ocg inspect-held` once the host has stopped; nothing is retried, reconciled or released"
            );
        }
    }

    /// One pass: at most one reservation per eligible Executor. Reads, and
    /// writes only the reservations it returns.
    pub(crate) async fn claim(&self) -> Result<DispatchBatch<PoolAllocation>, DatabaseError> {
        let mut grants = Vec::new();
        let (finished, running): (Vec<_>, Vec<_>) = std::mem::take(&mut *self.running.borrow_mut())
            .into_iter()
            .partition(|task| task.is_finished());
        *self.running.borrow_mut() = running;
        for task in finished {
            // A dispatch that ended without a report (it panicked) left its
            // Allocation in a state nobody read.
            if task.await.is_err() {
                self.halt("a dispatch ended without a report");
            }
        }
        if self.halted.get() || self.shutdown.is_cancelled() {
            return Ok(DispatchBatch {
                grants,
                next_due: None,
            });
        }
        let mut executors = Vec::new();
        let mut before = None;
        for _ in 0..MAX_EXECUTOR_PAGES {
            let page = self
                .database
                .native_resources(before, EXECUTOR_PAGE, NativeResourceKind::Executor)
                .await?;
            for resource in page.records {
                if let NativeResource::Executor(executor) = resource
                    && executor.enabled
                    && !executor.archived
                    && executor.capabilities.contains(&CAPABILITY)
                {
                    executors.push((executor.pool, executor.id));
                }
            }
            match page.next {
                Some(next) => before = Some(next),
                None => break,
            }
        }
        let pass = self.passes.get();
        self.passes.set(pass.wrapping_add(1));
        if !executors.is_empty() {
            // No Executor is always offered the head of the queue first.
            let start = usize::try_from(pass % executors.len() as u64).unwrap_or(0);
            executors.rotate_left(start);
        }
        let mut rotating = false;
        for (pool, executor) in executors {
            if self.halted.get() || self.shutdown.is_cancelled() {
                break;
            }
            match self.reserve_for(pool, executor).await {
                ExecutorPass::Reserved(allocation) => grants.push(*allocation),
                ExecutorPass::Nothing => {}
                ExecutorPass::BudgetSpent => rotating = true,
                ExecutorPass::IndexExhausted | ExecutorPass::Halted => break,
            }
        }
        // A search that ran out of budget wrote nothing, so no wake follows
        // it: the next pass is due at once, and passes continue while
        // searches advance, until one reads to the end of the index. A pass
        // in which every Executor was refused advanced nothing and waits for
        // a wake, as a settlement that frees one publishes.
        let next_due = (rotating && self.resume.get().is_some() && !self.halted.get())
            .then_some(Duration::ZERO);
        Ok(DispatchBatch { grants, next_due })
    }

    /// One Executor's search: from the head of the ready index, and, when an
    /// earlier search ran out of budget, from where it stopped after
    /// [`HEAD_PAGES`]. Every request is a first attempt; none is replayed.
    pub(crate) async fn reserve_for(
        &self,
        pool: ExecutionPoolId,
        executor: ExecutorId,
    ) -> ExecutorPass {
        let resume = self.resume.get();
        let mut after: Option<DevelopmentReadyCursor> = None;
        for page in 0..MAX_READY_PAGES {
            if page == HEAD_PAGES
                && let Some(resume) = resume
                && after.is_none_or(|reached| reached.0 < resume.0)
            {
                after = Some(resume);
            }
            let fresh = DevelopmentFreshReservation::new(pool, executor, after);
            match self
                .database
                .reserve_scheduled_job(fresh, scheduled_endpoint)
                .await
            {
                Ok(DevelopmentReservation::Reserved(allocation)) => {
                    if allocation.token.executor != executor || allocation.token.pool != pool {
                        self.halt("a reservation names another Executor");
                        return ExecutorPass::Halted;
                    }
                    return ExecutorPass::Reserved(Box::new(allocation));
                }
                Ok(DevelopmentReservation::Continue(cursor)) => after = Some(cursor),
                Ok(DevelopmentReservation::Exhausted) => {
                    self.resume.set(None);
                    return ExecutorPass::IndexExhausted;
                }
                Ok(DevelopmentReservation::Replayed(_) | DevelopmentReservation::Unresolved) => {
                    self.halt("a first reservation attempt was answered as a replay");
                    return ExecutorPass::Halted;
                }
                Err(DevelopmentReservationError::DefinitelyNotCommitted { error, .. }) => {
                    log::debug!("native scheduler: Executor {executor} reserved nothing: {error}");
                    return ExecutorPass::Nothing;
                }
                Err(DevelopmentReservationError::Other(error)) => {
                    self.halt(&format!(
                        "a reservation on Executor {executor} may have committed without a reply ({error})"
                    ));
                    return ExecutorPass::Halted;
                }
            }
        }
        if after.is_some_and(|reached| resume.is_none_or(|resume| reached.0 > resume.0)) {
            self.resume.set(after);
        }
        ExecutorPass::BudgetSpent
    }

    /// Registers the dispatch of one reservation under its token and starts
    /// it as a task of its own.
    pub(crate) fn spawn(&self, allocation: PoolAllocation) {
        let database = self.database.clone();
        let cancellation = self.shutdown.child();
        let registration = self.stops.register(allocation.token, cancellation.clone());
        let halted = self.halted.clone();
        let task = ntex::rt::spawn(async move {
            let _registration = registration;
            let dispatched = dispatch(&database, allocation, &cancellation).await;
            dispatched.log();
            if !dispatched.resolved() && !halted.replace(true) {
                log::error!(
                    "native scheduler stopped claiming: Job {} is not settled or Uncertain after its dispatch; run `ocg inspect-held` once the host has stopped; nothing is retried, reconciled or released",
                    dispatched.job
                );
            }
            dispatched
        });
        self.running.borrow_mut().push(task);
    }

    /// Waits for every dispatch this scheduler started. Shutdown has
    /// cancelled their exchanges; each settles what it can.
    pub(crate) async fn drain(&self) -> Vec<Dispatched> {
        let tasks = std::mem::take(&mut *self.running.borrow_mut());
        let mut finished = Vec::with_capacity(tasks.len());
        for task in tasks {
            match task.await {
                Ok(dispatched) => finished.push(dispatched),
                Err(_) => self.halt("a dispatch ended without a report"),
            }
        }
        finished
    }
}

/// What the database shows after one scheduled dispatch returned.
#[derive(Debug)]
pub(crate) struct Dispatched {
    pub(crate) job: JobId,
    pub(crate) allocation_phase: Option<AllocationPhase>,
    pub(crate) started: bool,
    pub(crate) outcome: Option<OutcomeKind>,
    pub(crate) detail: String,
}

impl Dispatched {
    /// Released with an outcome, or recorded as Uncertain and held. Anything
    /// else is ownership nobody recorded an end for.
    pub(crate) fn resolved(&self) -> bool {
        match self.allocation_phase {
            Some(AllocationPhase::Released) => self.outcome.is_some(),
            Some(AllocationPhase::Uncertain) => true,
            Some(
                AllocationPhase::Reserved
                | AllocationPhase::Running
                | AllocationPhase::StopRequested,
            )
            | None => false,
        }
    }

    fn log(&self) {
        let phase = self
            .allocation_phase
            .map_or("unknown", crate::system::manual_execution::phase_name);
        let outcome = crate::system::manual_execution::outcome_name(self.outcome);
        let started = if self.started {
            "started"
        } else {
            "never started"
        };
        match self.allocation_phase {
            Some(AllocationPhase::Released) => log::info!(
                "native scheduler: Job {} {started}, {outcome}, Allocation released ({})",
                self.job,
                self.detail
            ),
            _ => log::warn!(
                "native scheduler: Job {} {started}, {outcome}, Allocation {phase} and held ({})",
                self.job,
                self.detail
            ),
        }
    }
}

/// The checked start and the one exchange of `allocation`, under
/// `cancellation` (shutdown's, and the Job's own stop), then what the
/// database shows. A reservation whose start a cancellation overtook is ended
/// rather than started, unless a stop already ended it: it never reached a
/// Provider.
pub(crate) async fn dispatch(
    database: &DatabaseHandle,
    allocation: PoolAllocation,
    cancellation: &CancellationToken,
) -> Dispatched {
    let token = allocation.token;
    let detail = if cancellation.is_cancelled() {
        match database
            .fail_unstarted_development_reservation(token, UnstartedCause::StartRefused)
            .await
        {
            Ok(_) => "cancelled before the start; the reservation was ended and nothing was sent"
                .to_owned(),
            Err(error) => {
                format!("cancelled before the start; the reservation was not ended here ({error})")
            }
        }
    } else {
        match database
            .checked_development_handoff_guarded(token, check_prepared)
            .await
        {
            Ok(handoff) => {
                let config = ProviderExecutionConfig {
                    timeout: DISPATCH_TIMEOUT,
                    first_delta_timeout: Some(DISPATCH_TIMEOUT),
                    max_output_bytes: MAX_RESPONSE_BYTES,
                    cancellation: cancellation.clone(),
                    ..ProviderExecutionConfig::default()
                };
                let exchange = within_deadline(
                    cancellation,
                    DISPATCH_TIMEOUT,
                    supervise_development_generative(
                        database,
                        handoff,
                        &config,
                        EndpointPolicy::Loopback,
                    ),
                )
                .await;
                match exchange {
                    Ok(_) => "the exchange returned and was recorded".to_owned(),
                    Err(error) => error.to_string(),
                }
            }
            Err(DevelopmentHandoffError::NotStarted { cause, error }) => {
                match database
                    .fail_unstarted_development_reservation(token, cause)
                    .await
                {
                    Ok(_) => format!(
                        "the start was refused before any request ({error}); the reservation was ended"
                    ),
                    Err(settle) => format!(
                        "the start was refused before any request ({error}); the reservation was not ended here ({settle})"
                    ),
                }
            }
            Err(error) => format!("the start may have committed without a reply ({error:?})"),
        }
    };
    match database.development_trial_job(token.job).await {
        Ok(Some((record, Some(allocation)))) if allocation.token == token => Dispatched {
            job: token.job,
            allocation_phase: Some(allocation.phase),
            started: allocation.started_at.is_some(),
            outcome: record.outcome.as_ref().map(|outcome| outcome.kind),
            detail,
        },
        Ok(_) => Dispatched {
            job: token.job,
            allocation_phase: None,
            started: false,
            outcome: None,
            detail: format!("{detail}; the Allocation could not be found afterwards"),
        },
        Err(error) => Dispatched {
            job: token.job,
            allocation_phase: None,
            started: false,
            outcome: None,
            detail: format!("{detail}; the result could not be read ({error})"),
        },
    }
}
