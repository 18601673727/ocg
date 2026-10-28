use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use ntex::util::{Either, select};

use crate::db::entities::*;
use crate::db::{CommandOutcome, DatabaseError, DatabaseHandle, SchedulerWakeSubscription};
use crate::service::ai::providers::openai_compatible::ToolWire;
use crate::service::ai::providers::{CancellationToken, ProviderExecutionConfig};
use crate::system::executor::blocking;
use crate::system::executor::execution_runtime::*;
use crate::system::monitor::ProtectionWatch;

const BATCH_FLOOR: Duration = Duration::from_millis(5);
const RECONCILE: Duration = Duration::from_secs(30);

pub(crate) struct DispatchBatch<G> {
    pub(crate) grants: Vec<G>,
    pub(crate) next_due: Option<Duration>,
}

pub(crate) struct Scheduler {
    wake: SchedulerWakeSubscription,
    protection: ProtectionWatch,
    shutdown: CancellationToken,
}

impl Scheduler {
    pub(crate) fn new(
        database: &DatabaseHandle,
        wake: SchedulerWakeSubscription,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            wake,
            protection: database.watch_disk_protection(),
            shutdown,
        }
    }

    pub(crate) async fn run<G, Claim, ClaimFuture, Spawn, SpawnFuture>(
        self,
        mut claim: Claim,
        mut spawn: Spawn,
    ) where
        Claim: FnMut() -> ClaimFuture,
        ClaimFuture: Future<Output = Result<DispatchBatch<G>, DatabaseError>>,
        Spawn: FnMut(G) -> SpawnFuture,
        SpawnFuture: Future<Output = ()>,
    {
        let mut previous_batch = None;
        let mut next_due = None;
        loop {
            if self.shutdown.is_cancelled() {
                return;
            }
            let mut recovered = false;
            while self.protection.status().lock.is_some() {
                match select(self.shutdown.cancelled(), self.protection.changed()).await {
                    Either::Left(()) => return,
                    Either::Right(Some(_)) => recovered = true,
                    Either::Right(None) => {
                        self.shutdown.cancelled().await;
                        return;
                    }
                }
            }
            if let Some(previous) = previous_batch {
                if !recovered {
                    let timer = batch_delay(previous, next_due);
                    let waking = select(
                        self.wake.recv_async(),
                        select(self.protection.changed(), ntex::time::sleep(timer)),
                    );
                    match select(self.shutdown.cancelled(), waking).await {
                        Either::Left(()) => return,
                        Either::Right(Either::Left(Err(_))) => return,
                        Either::Right(_) => {}
                    }
                }
                let remaining = BATCH_FLOOR.saturating_sub(previous.elapsed());
                if let Either::Left(()) =
                    select(self.shutdown.cancelled(), ntex::time::sleep(remaining)).await
                {
                    return;
                }
            }
            if self.protection.status().lock.is_some() {
                continue;
            }
            while self.wake.try_recv().is_ok() {}
            let batch = claim().await;
            previous_batch = Some(Instant::now());
            match batch {
                Ok(batch) => {
                    next_due = batch.next_due;
                    for grant in batch.grants {
                        spawn(grant).await;
                    }
                }
                Err(error) => {
                    log::error!("dispatcher claim batch failed: {error}");
                    next_due = None;
                }
            }
        }
    }
}

fn batch_delay(previous: Instant, next_due: Option<Duration>) -> Duration {
    let due = next_due
        .map(|due| due.saturating_sub(previous.elapsed()))
        .unwrap_or(RECONCILE)
        .min(RECONCILE);
    due.max(BATCH_FLOOR.saturating_sub(previous.elapsed()))
}

/// The committed identity of an admitted provider plan.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::system) struct StartedJob {
    pub(in crate::system) executor: ExecutorId,
    pub(in crate::system) input: JobInputId,
    pub(in crate::system) job: JobId,
}

type ExecutionReply = flume::Sender<Result<ExecutionReceipt, ExecutionRuntimeError>>;

/// Admission reports the committed identity; settlement reports the result
/// after dispatch and execution, or cancellation while still pending.
struct ProviderReply {
    started: flume::Sender<Result<StartedJob, AdmissionError>>,
    settled: ExecutionReply,
}

/// One submitted plan with its caller's reply. A provider plan may carry
/// the submission to record with its Job.
enum SchedulerSubmission {
    Plan(ExecutionPlan, ExecutionReply),
    Provider(
        ProviderExecutionPlan,
        Option<Box<Submission>>,
        Option<WorkspaceTools>,
        ProviderReply,
    ),
}

impl SchedulerSubmission {
    /// Answers a plan that will never start: nothing of it was persisted.
    fn refuse(self) {
        self.fail(ExecutionRuntimeError::ShuttingDown);
    }

    /// Answers a plan that never started, for `error`.
    fn fail(self, error: ExecutionRuntimeError) {
        match self {
            Self::Plan(_, reply) => {
                reply.send(Err(error)).ok();
            }
            Self::Provider(_, _, _, reply) => {
                reply
                    .started
                    .send(Err(AdmissionError::NotAdmitted(error)))
                    .ok();
            }
        }
    }
}

enum SchedulerMessage {
    Schedule(Box<SchedulerSubmission>),
    Claim(flume::Sender<Result<DispatchBatch<JobId>, DatabaseError>>),
    Grant(JobId),
    Stop(JobId, Option<crate::db::entities::StopCause>),
    Reconcile,
}

#[derive(Default)]
struct RuntimeState {
    stopping: bool,
    closed: bool,
    current: BTreeMap<JobId, CancellationToken>,
    stops: BTreeMap<JobId, Arc<Mutex<Option<crate::db::entities::StopCause>>>>,
}

struct PendingExecution {
    started: StartedJob,
    execution: PendingPlan,
    cancellation: CancellationToken,
    done: CancellationToken,
    reply: Option<ExecutionReply>,
}

enum PendingPlan {
    Plan(JobOutcome, FinalResult),
    Provider(ProviderTask, Option<Vec<ToolWire>>),
}

type SharedState = Arc<Mutex<RuntimeState>>;

fn lock(state: &SharedState) -> std::sync::MutexGuard<'_, RuntimeState> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
}

/// How many lifecycle notifications may wait in the dispatch queue.
const SCHEDULER_NOTIFICATIONS: usize = 64;

/// Lifecycle requests wait for bounded channel capacity. Completions keep a
/// flag on their already admitted execution and share a single wake pulse.
#[derive(Clone)]
pub(crate) struct SchedulerNotifications {
    messages: flume::Sender<SchedulerMessage>,
    wake: flume::Sender<()>,
}

impl SchedulerNotifications {
    fn new(messages: flume::Sender<SchedulerMessage>, wake: flume::Sender<()>) -> Self {
        Self { messages, wake }
    }

    async fn send(
        &self,
        message: SchedulerMessage,
    ) -> Result<(), flume::SendError<SchedulerMessage>> {
        self.messages.send_async(message).await
    }
}

/// A cloneable endpoint for scheduler submissions.
#[derive(Clone)]
pub struct SchedulerHandle {
    sender: flume::Sender<Box<SchedulerSubmission>>,
    notifications: SchedulerNotifications,
    state: SharedState,
}

#[derive(Clone)]
pub(in crate::system) struct StopHandle {
    notifications: SchedulerNotifications,
    state: SharedState,
}
impl StopHandle {
    pub async fn stop(&self, turn: JobId, cause: crate::db::entities::StopCause) {
        {
            let mut state = lock(&self.state);
            if state.current.contains_key(&turn) || state.stops.contains_key(&turn) {
                *state
                    .stops
                    .entry(turn)
                    .or_default()
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner) = Some(cause);
            }
            if let Some(cancellation) = state.current.get(&turn) {
                cancellation.cancel();
            }
            if state.closed {
                return;
            }
        }
        self.notifications
            .send(SchedulerMessage::Stop(turn, Some(cause)))
            .await
            .ok();
    }
}

impl SchedulerHandle {
    pub async fn stop(&self, turn: JobId, cause: crate::db::entities::StopCause) {
        StopHandle {
            notifications: self.notifications.clone(),
            state: self.state.clone(),
        }
        .stop(turn, cause)
        .await;
    }

    /// Creates a pending Job through T1 and returns after that commit. Its
    /// receipt has no result yet; dispatch and finalization run separately.
    pub async fn schedule(
        &self,
        plan: ExecutionPlan,
    ) -> Result<ExecutionReceipt, ExecutionRuntimeError> {
        let (reply, response) = flume::bounded(1);
        self.submit(SchedulerSubmission::Plan(plan, reply))?;
        response
            .recv_async()
            .await
            .map_err(|_| ExecutionRuntimeError::SchedulerStopped)?
    }

    /// Submits a provider execution and waits until it has settled: it is
    /// [`Self::admit_provider`] followed by [`AdmittedJob::settled`], with
    /// an admission failure reported as its underlying error. Cancelling
    /// `plan.provider_config.cancellation` stops the execution; shutdown of
    /// the runtime stops it too if it is executing then.
    pub async fn schedule_provider(
        &self,
        plan: ProviderExecutionPlan,
    ) -> Result<ExecutionReceipt, ExecutionRuntimeError> {
        self.admit_provider(plan)
            .await
            .map_err(AdmissionError::into_inner)?
            .settled()
            .await
    }

    /// Submits a provider execution and returns as soon as its one Job has
    /// been committed as pending with its input, before any claim or Provider
    /// request. The runtime then owns the execution: it dispatches and settles
    /// the Job, whatever becomes of the returned [`AdmittedJob`]; dropping
    /// it, or the future of its settlement, neither stops nor cancels the
    /// Job. Its stop is requested through a cancellation of its own, a
    /// child of the plan's token: cancelling that token, or shutdown of the
    /// runtime, stops it too, but its stop reaches no other execution,
    /// whatever tokens the plans share.
    ///
    /// The pending cap, a stopped scheduler, shutdown, an unregistered
    /// Executor, invalid tool limits, tools no longer offered, and a
    /// creation known not to have committed are each
    /// [`AdmissionError::NotAdmitted`]; a creation whose commit is unknown
    /// is [`AdmissionError::Uncertain`], never a refusal.
    ///
    /// # Errors
    ///
    /// Returns why no Job is known to have been admitted.
    #[allow(clippy::result_large_err)]
    pub async fn admit_provider(
        &self,
        plan: ProviderExecutionPlan,
    ) -> Result<AdmittedJob, AdmissionError> {
        self.admit(plan, None, None).await
    }

    #[allow(clippy::result_large_err)]
    async fn admit(
        &self,
        mut plan: ProviderExecutionPlan,
        submission: Option<Submission>,
        workspace_tools: Option<WorkspaceTools>,
    ) -> Result<AdmittedJob, AdmissionError> {
        // The execution observes a cancellation of its own, which the
        // plan's token, however widely it is shared, still cancels.
        let cancellation = plan.provider_config.cancellation.child();
        plan.provider_config.cancellation = cancellation.clone();
        let (started, admission) = flume::bounded(1);
        let (settled, settlement) = flume::bounded(1);
        self.submit(SchedulerSubmission::Provider(
            plan,
            submission.map(Box::new),
            workspace_tools,
            ProviderReply { started, settled },
        ))
        .map_err(AdmissionError::NotAdmitted)?;
        // Every refusal is answered before the reply is dropped, so a
        // reply dropped unanswered means a worker stopped without knowing
        // whether the Job was created.
        let started = admission
            .recv_async()
            .await
            .map_err(|_| AdmissionError::Uncertain(ExecutionRuntimeError::SchedulerStopped))??;
        Ok(AdmittedJob {
            started,
            stop: JobStop {
                job: started.job,
                cancellation,
            },
            settlement,
        })
    }

    /// Once shutdown has begun, a plan is refused here without being queued.
    /// The check and the queueing happen under the state lock, which never
    /// waits for queue space, so a plan is either queued ahead of the
    /// scheduler's last drain or refused.
    fn submit(&self, submission: SchedulerSubmission) -> Result<(), ExecutionRuntimeError> {
        let state = lock(&self.state);
        if state.stopping {
            return Err(ExecutionRuntimeError::ShuttingDown);
        }
        if state.closed {
            return Err(ExecutionRuntimeError::SchedulerStopped);
        }
        self.sender
            .try_send(Box::new(submission))
            .map_err(|error| match error {
                flume::TrySendError::Full(_) => ExecutionRuntimeError::SchedulerQueueFull,
                flume::TrySendError::Disconnected(_) => ExecutionRuntimeError::SchedulerStopped,
            })
    }
}

/// A provider plan's committed Job, initially pending. Its identity is the
/// one the execution,
/// its input, outputs, tool evidence and settlement are recorded under.
/// The runtime owns its execution; this only observes its settlement and
/// may request its stop. Dropping it changes nothing about the Job.
#[derive(Debug)]
pub struct AdmittedJob {
    started: StartedJob,
    stop: JobStop,
    settlement: flume::Receiver<Result<ExecutionReceipt, ExecutionRuntimeError>>,
}

impl AdmittedJob {
    pub fn job(&self) -> JobId {
        self.started.job
    }

    pub fn input(&self) -> JobInputId {
        self.started.input
    }

    /// The submitter's registered Pool; the settlement receipt names the
    /// Pool actually selected by the claim batch.
    pub fn executor(&self) -> ExecutorId {
        self.started.executor
    }

    /// Requests the stop of this Job's execution, see [`JobStop::request`].
    pub fn request_stop(&self) {
        self.stop.request();
    }

    /// Waits until the execution has returned, and answers what
    /// [`SchedulerHandle::schedule_provider`] answers for it: the receipt
    /// of the Job, or why it settled, or failed to settle, as it did.
    ///
    /// # Errors
    ///
    /// As [`SchedulerHandle::schedule_provider`] once the Job was started.
    pub async fn settled(self) -> Result<ExecutionReceipt, ExecutionRuntimeError> {
        self.settlement
            .recv_async()
            .await
            .map_err(|_| ExecutionRuntimeError::SchedulerStopped)?
    }
}

/// Requests the stop of one admitted Job's execution. Cloneable; every
/// clone addresses the same Job.
#[derive(Clone, Debug)]
pub struct JobStop {
    job: JobId,
    cancellation: CancellationToken,
}

impl JobStop {
    pub fn job(&self) -> JobId {
        self.job
    }

    /// Cancels the Job's execution: the cancellation made for it at its
    /// admission, the one its execution observes, and nothing else. It
    /// cancels neither the token of the plan it was admitted from nor any
    /// other Job's execution, sharing that token or not. It is a request, not
    /// an outcome: the Job settles as the execution then ends, which is
    /// Unknown, not Cancelled, when a request may have reached the Provider
    /// or a call may have had an effect, and may still be Completed when
    /// the execution had already finished. Once the execution has returned
    /// it changes nothing.
    pub fn request(&self) {
        self.cancellation.cancel();
    }
}

/// Owns admission, the dispatcher, and every granted Job task.
pub struct ExecutionRuntime {
    scheduler: SchedulerHandle,
    shutdown: CancellationToken,
    finished: Option<ntex::rt::JoinHandle<Result<(), ExecutionRuntimeError>>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClaimScope {
    Stored,
    Process,
}

impl ExecutionRuntime {
    pub fn start(
        database: DatabaseHandle,
        scope: ClaimScope,
    ) -> Result<Self, ExecutionRuntimeError> {
        let wake = database
            .take_scheduler_wake()
            .ok_or(ExecutionRuntimeError::SchedulerStopped)?;
        let (messages, receiver) = flume::bounded(SCHEDULER_NOTIFICATIONS);
        let (wakeups, wakes) = flume::bounded(1);
        let sender = SchedulerNotifications::new(messages, wakeups);
        let (admission, admissions) = flume::bounded(64);
        let state = SharedState::default();
        let shutdown = CancellationToken::default();
        let finished = ntex::rt::spawn(dispatch_runtime(
            database,
            scope,
            wake,
            wakes,
            sender.clone(),
            receiver,
            admissions,
            state.clone(),
            shutdown.clone(),
        ));
        Ok(Self {
            scheduler: SchedulerHandle {
                sender: admission,
                notifications: sender,
                state,
            },
            shutdown,
            finished: Some(finished),
        })
    }

    pub fn scheduler(&self) -> SchedulerHandle {
        self.scheduler.clone()
    }

    pub async fn shutdown(mut self) -> Result<(), ExecutionRuntimeError> {
        self.request_stop();
        if let Some(finished) = self.finished.take() {
            finished
                .await
                .map_err(|_| ExecutionRuntimeError::WorkerPanicked)??;
        }
        Ok(())
    }

    fn request_stop(&self) {
        let mut state = lock(&self.scheduler.state);
        state.stopping = true;
        for cancellation in state.current.values() {
            cancellation.cancel();
        }
        self.shutdown.cancel();
    }
}

impl Drop for ExecutionRuntime {
    fn drop(&mut self) {
        self.request_stop();
    }
}

#[allow(clippy::too_many_arguments)]
async fn dispatch_runtime(
    database: DatabaseHandle,
    scope: ClaimScope,
    wake: crate::db::SchedulerWakeSubscription,
    wakes: flume::Receiver<()>,
    sender: SchedulerNotifications,
    receiver: flume::Receiver<SchedulerMessage>,
    admissions: flume::Receiver<Box<SchedulerSubmission>>,
    state: SharedState,
    shutdown: CancellationToken,
) -> Result<(), ExecutionRuntimeError> {
    let dispatcher = ntex::rt::spawn({
        let sender = sender.clone();
        let database = database.clone();
        let shutdown = shutdown.clone();
        async move {
            Scheduler::new(&database, wake, shutdown)
                .run(
                    || {
                        let sender = sender.clone();
                        async move {
                            let (reply, response) = flume::bounded(1);
                            sender
                                .send(SchedulerMessage::Claim(reply))
                                .await
                                .map_err(|_| DatabaseError::ShuttingDown)?;
                            response
                                .recv_async()
                                .await
                                .map_err(|_| DatabaseError::ShuttingDown)?
                        }
                    },
                    |job| {
                        let sender = sender.clone();
                        async move {
                            sender.send(SchedulerMessage::Grant(job)).await.ok();
                        }
                    },
                )
                .await;
        }
    });
    let mut pending = BTreeMap::new();
    let mut running = BTreeMap::new();
    let catalog_changes = database.subscribe_catalog();
    let mut reconcile = true;
    let mut retry_at = Instant::now() + RECONCILE;
    let mut unconfirmed_stopped = BTreeSet::new();
    enum Incoming {
        Message(Result<SchedulerMessage, flume::RecvError>),
        Wake,
        Closed,
    }
    loop {
        let finished: Vec<_> = running
            .iter()
            .filter_map(|(job, task): (&JobId, &RunningExecution)| {
                task.finished.load(Ordering::Acquire).then_some(*job)
            })
            .collect();
        for job in finished {
            finish_job(&database, job, &mut running, &state).await;
        }
        let incoming = async {
            if std::mem::take(&mut reconcile) {
                return Incoming::Message(Ok(SchedulerMessage::Reconcile));
            }
            let activity = async {
                match select(
                    receiver.recv_async(),
                    select(
                        wakes.recv_async(),
                        select(admissions.recv_async(), catalog_changes.changed()),
                    ),
                )
                .await
                {
                    Either::Left(message) => Incoming::Message(message),
                    Either::Right(Either::Left(Ok(()))) => Incoming::Wake,
                    Either::Right(Either::Left(Err(_))) => Incoming::Closed,
                    Either::Right(Either::Right(Either::Left(submission))) => {
                        Incoming::Message(submission.map(SchedulerMessage::Schedule))
                    }
                    Either::Right(Either::Right(Either::Right(Ok(())))) => {
                        Incoming::Message(Ok(SchedulerMessage::Reconcile))
                    }
                    Either::Right(Either::Right(Either::Right(Err(_)))) => Incoming::Closed,
                }
            };
            match select(
                activity,
                ntex::time::sleep(retry_at.saturating_duration_since(Instant::now())),
            )
            .await
            {
                Either::Left(incoming) => incoming,
                Either::Right(()) => Incoming::Message(Ok(SchedulerMessage::Reconcile)),
            }
        };
        let message = match select(shutdown.cancelled(), incoming).await {
            Either::Left(()) | Either::Right(Incoming::Closed) => break,
            Either::Right(Incoming::Wake) => {
                continue;
            }
            Either::Right(Incoming::Message(message)) => message,
        };
        let Ok(message) = message else {
            break;
        };
        match message {
            SchedulerMessage::Schedule(submission) => {
                if lock(&state).stopping {
                    submission.refuse();
                    continue;
                }
                let admitted = match *submission {
                    SchedulerSubmission::Plan(mut plan, reply) => {
                        let created = async {
                            if database.executor(plan.executor_id).await?.is_none() {
                                return Err(ExecutionRuntimeError::ExecutorNotFound(
                                    plan.executor_id,
                                ));
                            }
                            plan.job.input = std::mem::take(&mut plan.input);
                            Ok(database.create_job(plan.job.clone()).await?)
                        }
                        .await;
                        match created {
                            Ok(job) => {
                                let started = StartedJob {
                                    job,
                                    input: JobInputId::new(job.get()),
                                    executor: plan.executor_id,
                                };
                                reply
                                    .send(Ok(ExecutionReceipt {
                                        job,
                                        input: started.input,
                                        executor: started.executor,
                                        result: None,
                                    }))
                                    .ok();
                                Some(PendingExecution {
                                    started,
                                    execution: PendingPlan::Plan(plan.outcome, plan.result),
                                    reply: None,
                                    cancellation: CancellationToken::default(),
                                    done: CancellationToken::default(),
                                })
                            }
                            Err(error) => {
                                reply.send(Err(error)).ok();
                                None
                            }
                        }
                    }
                    SchedulerSubmission::Provider(mut plan, submission, workspace_tools, reply) => {
                        match admit_provider_job(
                            &database,
                            &mut plan,
                            submission.map(|submission| *submission),
                        )
                        .await
                        {
                            Ok((started, offered)) => {
                                let cancellation = plan.provider_config.cancellation.clone();
                                reply.started.send(Ok(started)).ok();
                                let mut task = ProviderTask::from(plan);
                                task.workspace_tools = workspace_tools;
                                Some(PendingExecution {
                                    started,
                                    execution: PendingPlan::Provider(task, offered),
                                    reply: Some(reply.settled),
                                    cancellation,
                                    done: CancellationToken::default(),
                                })
                            }
                            Err(error) => {
                                reply.started.send(Err(error)).ok();
                                None
                            }
                        }
                    }
                };
                if let Some(admitted) = admitted {
                    let job = admitted.started.job;
                    {
                        let mut state = lock(&state);
                        if state.stopping {
                            admitted.cancellation.cancel();
                        }
                        state.current.insert(job, admitted.cancellation.clone());
                    }
                    ntex::rt::spawn({
                        let sender = sender.clone();
                        let cancellation = admitted.cancellation.clone();
                        let done = admitted.done.clone();
                        async move {
                            if let Either::Left(()) =
                                select(cancellation.cancelled(), done.cancelled()).await
                            {
                                sender.send(SchedulerMessage::Stop(job, None)).await.ok();
                            }
                        }
                    });
                    pending.insert(job, admitted);
                }
            }
            SchedulerMessage::Reconcile => {
                retry_at = Instant::now() + RECONCILE;
                retry_unsettled(&database).await;
                match ntex::rt::spawn_blocking(blocking::retry_unproven_groups).await {
                    Ok(recovered) => unconfirmed_stopped.extend(recovered),
                    Err(error) => {
                        log::warn!("failed to retry unproven groups on blocking worker: {error}");
                    }
                }
                let mut retry_next = BTreeSet::new();
                for job in std::mem::take(&mut unconfirmed_stopped) {
                    let result = database
                        .confirm_job_stopped(job, crate::db::entities::StopProof::Proof)
                        .await;
                    if retry_stop_confirmation(&result) {
                        retry_next.insert(job);
                    }
                    match result {
                        Ok(())
                        | Err(DatabaseError::Rejected(
                            Violation::StopAlreadyConfirmed(_)
                            | Violation::StopNeedsUnknownOutcome(_)
                            | Violation::StopNeedsOutcome(_),
                        )) => {}
                        Err(error) => {
                            log::warn!(
                                "failed to confirm job stop after process group cleanup: {error}"
                            );
                        }
                    }
                }
                unconfirmed_stopped = retry_next;
                match database
                    .terminal_jobs(pending.keys().copied().collect())
                    .await
                {
                    Ok(terminal) => {
                        for job in terminal {
                            if let Some(execution) = pending.remove(&job) {
                                execution.done.cancel();
                                lock(&state).current.remove(&job);
                                if let Some(reply) = execution.reply {
                                    reply
                                        .send(Ok(ExecutionReceipt {
                                            job,
                                            input: execution.started.input,
                                            executor: execution.started.executor,
                                            result: None,
                                        }))
                                        .ok();
                                }
                            }
                        }
                    }
                    Err(error) => log::warn!("pending settlement read failed: {error}"),
                }
            }
            SchedulerMessage::Claim(reply) => {
                // Admission and claim requests share this task so a committed
                // grant cannot overtake registration of its execution context.
                let claimed = if scope == ClaimScope::Stored {
                    async {
                        let clock = crate::db::redb::promotion_clock();
                        match database.recurrence_due(clock).await {
                            Ok(true) => {
                                if let Err(error) = database.boundary_b(clock).await {
                                    log::error!("recurrence promotion failed: {error}");
                                }
                            }
                            Ok(false) => {}
                            Err(error) => log::error!("recurrence due read failed: {error}"),
                        }
                        database.claim_batch().await
                    }
                    .await
                } else {
                    let mut grants = Vec::new();
                    let mut failure = None;
                    for execution in pending.values() {
                        match database
                            .claim_execution(crate::db::entities::ClaimRequest {
                                job: execution.started.job,
                                allowed_pools: BTreeSet::from([execution.started.executor.into()]),
                            })
                            .await
                        {
                            Ok(Ok(_)) => grants.push(execution.started.job),
                            Ok(Err(_)) => {}
                            Err(error) => {
                                failure = Some(error);
                                break;
                            }
                        }
                    }
                    match failure {
                        // Granted claims are committed: dropping them would
                        // leave their Jobs claimed with no task.
                        Some(error) if !grants.is_empty() => {
                            log::warn!(
                                "process claim sweep interrupted after {} grants: {error}",
                                grants.len()
                            );
                            Ok(grants)
                        }
                        Some(error) => Err(error),
                        None => Ok(grants),
                    }
                };
                let batch = match claimed {
                    Ok(grants) => {
                        {
                            let mut state = lock(&state);
                            for job in &grants {
                                state.stops.entry(*job).or_default();
                            }
                        }
                        let next_due = match database.next_scheduled_delay().await {
                            Ok(due) => due,
                            Err(error) => {
                                log::warn!("dispatcher timer read failed: {error}");
                                None
                            }
                        };
                        Ok(DispatchBatch { grants, next_due })
                    }
                    Err(error) => Err(error),
                };
                reply.send(batch).ok();
            }
            SchedulerMessage::Grant(job) => {
                spawn_grant(
                    job,
                    pending.remove(&job),
                    &database,
                    &sender,
                    &state,
                    &mut running,
                );
            }
            SchedulerMessage::Stop(job, cause) => {
                if let Some(cause) = cause
                    && let Some(stopped) = lock(&state).stops.get(&job)
                {
                    *stopped.lock().unwrap_or_else(PoisonError::into_inner) = Some(cause);
                }
                stop_job(&database, job, &mut pending, &state).await;
            }
        }
    }
    lock(&state).closed = true;
    for submission in admissions.drain() {
        submission.refuse();
    }
    for message in receiver.drain() {
        match message {
            SchedulerMessage::Claim(reply) => {
                reply.send(Err(DatabaseError::ShuttingDown)).ok();
            }
            SchedulerMessage::Schedule(submission) => submission.refuse(),
            SchedulerMessage::Grant(job) => spawn_grant(
                job,
                pending.remove(&job),
                &database,
                &sender,
                &state,
                &mut running,
            ),
            SchedulerMessage::Stop(..) | SchedulerMessage::Reconcile => {}
        }
    }
    let mut dispatcher = std::pin::pin!(dispatcher);
    loop {
        match select(dispatcher.as_mut(), receiver.recv_async()).await {
            Either::Left(result) => {
                result.map_err(|_| ExecutionRuntimeError::WorkerPanicked)?;
                break;
            }
            Either::Right(Ok(SchedulerMessage::Grant(job))) => spawn_grant(
                job,
                pending.remove(&job),
                &database,
                &sender,
                &state,
                &mut running,
            ),
            Either::Right(Ok(SchedulerMessage::Claim(reply))) => {
                reply.send(Err(DatabaseError::ShuttingDown)).ok();
            }
            Either::Right(Ok(SchedulerMessage::Schedule(submission))) => submission.refuse(),
            Either::Right(Ok(SchedulerMessage::Stop(..) | SchedulerMessage::Reconcile)) => {}
            Either::Right(Err(_)) => break,
        }
    }
    for message in receiver.drain() {
        match message {
            SchedulerMessage::Schedule(submission) => submission.refuse(),
            SchedulerMessage::Claim(reply) => {
                reply.send(Err(DatabaseError::ShuttingDown)).ok();
            }
            SchedulerMessage::Grant(job) => spawn_grant(
                job,
                pending.remove(&job),
                &database,
                &sender,
                &state,
                &mut running,
            ),
            SchedulerMessage::Stop(..) | SchedulerMessage::Reconcile => {}
        }
    }
    for execution in pending.into_values() {
        cancel_pending(&database, &execution).await;
    }
    for (job, task) in running {
        finish_grant(&database, job, task).await;
    }
    retry_unsettled(&database).await;
    lock(&state).current.clear();
    lock(&state).stops.clear();
    Ok(())
}

/// Applies a Stop. A committed Stop may reach us after Finished removed its
/// task: keep the cause through a grant race, but do not retain terminal Jobs.
async fn stop_job(
    database: &DatabaseHandle,
    job: JobId,
    pending: &mut BTreeMap<JobId, PendingExecution>,
    state: &SharedState,
) {
    stop_pending(database, job, pending, state).await;
    if database
        .job(job)
        .await
        .is_ok_and(|record| record.is_none_or(|record| record.any_terminal()))
    {
        lock(state).stops.remove(&job);
    }
}

async fn finish_job(
    database: &DatabaseHandle,
    job: JobId,
    running: &mut BTreeMap<JobId, RunningExecution>,
    state: &SharedState,
) {
    if let Some(task) = running.remove(&job) {
        finish_grant(database, job, task).await;
    }
    lock(state).current.remove(&job);
    lock(state).stops.remove(&job);
}

async fn stop_pending(
    database: &DatabaseHandle,
    job: JobId,
    pending: &mut BTreeMap<JobId, PendingExecution>,
    state: &SharedState,
) {
    if let Some(execution) = pending.get(&job) {
        execution.cancellation.cancel();
        match database.frozen_pool_binding(job).await {
            Ok(None) => {
                if cancel_pending(database, execution).await {
                    pending.remove(&job);
                    lock(state).current.remove(&job);
                }
            }
            Ok(Some(_)) => {} // Its grant will observe the cancelled token.
            Err(error) => log::error!("pending Job {job} stop read failed: {error}"),
        }
    }
}

async fn cancel_pending(database: &DatabaseHandle, execution: &PendingExecution) -> bool {
    let started = execution.started;
    match database
        .finalize_job(
            started.job,
            JobFinalization::Cancelled(FinalResult {
                summary: Some("stopped before execution".into()),
                code: None,
                goal_reached: None,
                value: None,
            }),
        )
        .await
    {
        Ok(result) => {
            execution.done.cancel();
            if let Some(reply) = &execution.reply {
                reply
                    .send(Ok(ExecutionReceipt {
                        executor: started.executor,
                        input: started.input,
                        job: started.job,
                        result,
                    }))
                    .ok();
            }
            true
        }
        Err(DatabaseError::Rejected(Violation::JobAlreadyFinalized(_))) => {
            execution.done.cancel();
            if let Some(reply) = &execution.reply {
                reply
                    .send(Ok(ExecutionReceipt {
                        executor: started.executor,
                        input: started.input,
                        job: started.job,
                        result: None,
                    }))
                    .ok();
            }
            true
        }
        Err(error) => {
            log::error!("pending Job {} could not settle: {error}", started.job);
            false
        }
    }
}

struct RunningExecution {
    task: ntex::rt::JoinHandle<Result<ExecutionReceipt, ExecutionRuntimeError>>,
    reply: Option<ExecutionReply>,
    finished: Arc<AtomicBool>,
}

struct NotifyFinished {
    finished: Arc<AtomicBool>,
    wake: flume::Sender<()>,
}

impl Drop for NotifyFinished {
    fn drop(&mut self) {
        self.finished.store(true, Ordering::Release);
        self.wake.try_send(()).ok();
    }
}

struct JobTask<F>(Pin<Box<F>>);

impl<F: Future<Output = Result<ExecutionReceipt, ExecutionRuntimeError>>> Future for JobTask<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match catch_unwind(AssertUnwindSafe(|| self.0.as_mut().poll(context))) {
            Ok(polled) => polled,
            Err(_) => Poll::Ready(Err(ExecutionRuntimeError::WorkerPanicked)),
        }
    }
}

async fn finish_grant(database: &DatabaseHandle, job: JobId, running: RunningExecution) {
    let result = match running.task.await {
        Ok(Err(ExecutionRuntimeError::WorkerPanicked)) | Err(_) => {
            log::error!("Job {job} task panicked");
            finalize_claim_error(database, job, &ExecutionRuntimeError::WorkerPanicked, true).await;
            Err(ExecutionRuntimeError::WorkerPanicked)
        }
        Ok(result) => result,
    };
    if let Some(reply) = running.reply {
        reply.send(result).ok();
    } else if let Err(error) = result {
        log::error!("granted Job {job} failed: {error}");
    }
}

fn retry_stop_confirmation(result: &Result<(), DatabaseError>) -> bool {
    match result {
        Ok(()) => false,
        Err(DatabaseError::Rejected(violation)) => {
            matches!(violation, Violation::StopNeedsOutcome(_))
        }
        Err(_) => true,
    }
}

/// Codes on Failed results: provider_rejected (provider refusal), not_delivered
/// (delivery ruled out), build_failed (request construction), context_window_exceeded
/// (input budget), not_sent (pre-send refusal), tool_loop_stopped (known local stop),
/// completion_refused (local completion rejected), internal (other runtime failure).
fn spawn_grant(
    job: JobId,
    mut execution: Option<PendingExecution>,
    database: &DatabaseHandle,
    sender: &SchedulerNotifications,
    state: &SharedState,
    running: &mut BTreeMap<JobId, RunningExecution>,
) {
    let cause = lock(state).stops.entry(job).or_default().clone();
    let database = database.with_stop_cause(cause.clone());
    if cause
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .is_some()
        && let Some(execution) = &execution
    {
        execution.cancellation.cancel();
    }
    let finished = Arc::new(AtomicBool::new(false));
    let notify = NotifyFinished {
        finished: finished.clone(),
        wake: sender.wake.clone(),
    };
    let reply = execution
        .as_mut()
        .and_then(|execution| execution.reply.take());
    if let Some(execution) = &execution {
        execution.done.cancel();
        if lock(state).stopping {
            execution.cancellation.cancel();
        }
    }
    let scheduler = StopHandle {
        notifications: sender.clone(),
        state: state.clone(),
    };
    let state = state.clone();
    let task = ntex::rt::spawn(async move {
        let _notify = notify;
        JobTask(Box::pin(async move {
            let tool_may_have_run = Cell::new(false);
            let result = async {
                let execution = match execution {
                    Some(execution) => execution,
                    None => {
                        let cancellation = CancellationToken::default();
                        {
                            let mut state = lock(&state);
                            if state.stopping
                                || state.stops.get(&job).is_some_and(|cause| {
                                    cause
                                        .lock()
                                        .unwrap_or_else(PoisonError::into_inner)
                                        .is_some()
                                })
                            {
                                cancellation.cancel();
                            }
                            state.current.insert(job, cancellation.clone());
                        }
                        let binding = database
                            .frozen_pool_binding(job)
                            .await?
                            .ok_or(ExecutionRuntimeError::MissingPoolBinding(job))?;
                        PendingExecution {
                            started: StartedJob {
                                job,
                                input: JobInputId::new(job.get()),
                                executor: binding.pool.into(),
                            },
                            execution: PendingPlan::Provider(
                                ProviderTask {
                                    workspace_tools: Some(WorkspaceTools::new(
                                        database.data_directory(),
                                    )),
                                    credential_resolver: database.credential_resolver(),
                                    provider_config: ProviderExecutionConfig {
                                        cancellation: cancellation.clone(),
                                        ..ProviderExecutionConfig::default()
                                    },
                                    tools: None,
                                },
                                None,
                            ),
                            cancellation,
                            done: CancellationToken::default(),
                            reply: None,
                        }
                    }
                };
                let binding = database
                    .frozen_pool_binding(job)
                    .await?
                    .ok_or(ExecutionRuntimeError::MissingPoolBinding(job))?;
                let started = StartedJob {
                    job,
                    input: JobInputId::new(job.get()),
                    executor: binding.pool.into(),
                };
                if execution.cancellation.is_cancelled() {
                    let settlement = JobFinalization::Cancelled(FinalResult {
                        summary: Some("stopped before execution".into()),
                        code: None,
                        goal_reached: None,
                        value: None,
                    });
                    let result = database
                        .finalize_job(job, settlement.clone())
                        .await
                        .map_err(|source| ExecutionRuntimeError::NotSettled {
                            source,
                            settlement: Box::new(settlement),
                        })?;
                    return Ok(ExecutionReceipt {
                        job,
                        executor: started.executor,
                        input: started.input,
                        result,
                    });
                }
                match execution.execution {
                    PendingPlan::Plan(outcome, result) => {
                        let settlement = outcome.finalization(result);
                        let result = database
                            .finalize_job(job, settlement.clone())
                            .await
                            .map_err(|source| ExecutionRuntimeError::NotSettled {
                                source,
                                settlement: Box::new(settlement),
                            })?;
                        Ok(ExecutionReceipt {
                            job,
                            executor: started.executor,
                            input: started.input,
                            result,
                        })
                    }
                    PendingPlan::Provider(plan, offered) => {
                        run_started_provider_job(
                            database.clone(),
                            plan,
                            (started, offered),
                            &tool_may_have_run,
                            Some(scheduler),
                        )
                        .await
                    }
                }
            }
            .await;
            if let Err(error) = &result {
                finalize_claim_error(&database, job, error, tool_may_have_run.get()).await;
            }
            result
        }))
        .await
    });
    running.insert(
        job,
        RunningExecution {
            task,
            reply,
            finished,
        },
    );
}

/// Admits the pending Job of a provider plan through T1, once the plan is
/// checked, and returns it with the tool definitions to offer. Nothing is
/// persisted unless the Executor is registered, the tool limits are valid
/// and the tools can still be offered. A `submission` is recorded with the
/// Job in the transaction that creates it.
#[allow(clippy::result_large_err)]
async fn admit_provider_job(
    database: &DatabaseHandle,
    plan: &mut ProviderExecutionPlan,
    submission: Option<Submission>,
) -> Result<(StartedJob, Option<Vec<ToolWire>>), AdmissionError> {
    let refused = AdmissionError::NotAdmitted;
    let executor = plan.executor_id;
    if database
        .executor(executor)
        .await
        .map_err(|error| refused(error.into()))?
        .is_none()
    {
        return Err(refused(ExecutionRuntimeError::ExecutorNotFound(executor)));
    }
    // The limits are checked, and the offer read from the registry, before
    // anything is persisted.
    if let Some(tools) = &plan.tools {
        tools
            .limits
            .validate()
            .map_err(|error| refused(ExecutionRuntimeError::InvalidToolLimits(error)))?;
    }
    let offered = plan
        .tools
        .as_ref()
        .map(|tools| tools.offer.wires())
        .transpose()
        .map_err(|error| refused(ExecutionRuntimeError::ToolsUnavailable(error)))?;
    plan.job.input = std::mem::take(&mut plan.input);

    let created = match submission {
        None => database.create_job(plan.job.clone()).await,
        Some(submission) => {
            database
                .create_submitted_job(plan.job.clone(), submission)
                .await
        }
    };
    let job = created.map_err(|error| match error.command_outcome() {
        Some(CommandOutcome::NotAccepted | CommandOutcome::NotCommitted) => refused(error.into()),
        Some(CommandOutcome::Unknown) | None => AdmissionError::Uncertain(error.into()),
    })?;
    Ok((
        StartedJob {
            executor,
            input: JobInputId::new(job.get()),
            job,
        },
        offered,
    ))
}
