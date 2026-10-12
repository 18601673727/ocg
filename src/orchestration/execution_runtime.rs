//! Bounded scheduler-to-executor handoff for the 0.5 execution plane.
//!
//! The scheduler owns admission to the executor queue. The executor owns the
//! execution plan after dequeueing it and is the only worker that calls the
//! database handle. Every worker boundary is a bounded `flume` channel; the
//! database handle itself has a separate bounded command channel.

use std::thread::{self, JoinHandle};

use crate::database::{DatabaseError, DatabaseHandle};
use crate::persistence::{
    AppendedOutput, ExecutorId, FinalResult, JobId, JobInputDraft, JobInputId, JobOutcome, NewJob,
};

pub const SCHEDULER_QUEUE_CAPACITY: usize = 64;
pub const EXECUTOR_QUEUE_CAPACITY: usize = 64;

/// The durable work supplied by the scheduler to one registered executor.
///
/// Executor registration is a process or installation concern and must happen
/// through [`DatabaseHandle::create_executor`] before a plan is submitted. The
/// executor allocates only the Job lifecycle here: it creates the Job, starts
/// it, and finalizes it with its published result as one ordered lifecycle.
/// Provider-specific execution can be inserted between `start_job` and
/// `finalize_job` without changing the worker ownership model.
#[derive(Debug)]
pub struct ExecutionPlan {
    /// An existing durable Executor record. This is never registered as part
    /// of executing a plan, so repeated plans do not grow the executor table.
    /// When `job.configuration` names an LLM Profile, the Job is refused
    /// unless this Executor is registered to serve exactly that Profile; see
    /// [`DatabaseHandle::compatible_executors`]. Choosing among compatible
    /// Executors is the caller's decision.
    pub executor_id: ExecutorId,
    pub input: JobInputDraft,
    pub job: NewJob,
    pub outcome: JobOutcome,
    pub result: FinalResult,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionReceipt {
    pub executor: ExecutorId,
    pub input: JobInputId,
    pub job: JobId,
    /// `None` for an unknown outcome, which publishes no result.
    pub result: Option<AppendedOutput>,
}

#[derive(Debug, thiserror::Error)]
pub enum ExecutionRuntimeError {
    #[error("scheduler queue is full")]
    SchedulerQueueFull,
    #[error("scheduler worker stopped")]
    SchedulerStopped,
    #[error("executor queue is full")]
    ExecutorQueueFull,
    #[error("executor worker stopped")]
    ExecutorStopped,
    #[error("database operation failed: {0}")]
    Database(#[from] DatabaseError),
    #[error("executor {0} is not registered")]
    ExecutorNotFound(ExecutorId),
    #[error("failed to start execution worker: {0}")]
    Startup(#[source] std::io::Error),
    #[error("execution worker panicked")]
    WorkerPanicked,
}

type ExecutionReply = flume::Sender<Result<ExecutionReceipt, ExecutionRuntimeError>>;

enum SchedulerMessage {
    Schedule {
        plan: ExecutionPlan,
        reply: ExecutionReply,
    },
    Shutdown,
}

enum ExecutorMessage {
    Execute {
        plan: ExecutionPlan,
        reply: ExecutionReply,
    },
    Shutdown,
}

/// A cloneable endpoint for scheduler submissions.
#[derive(Clone)]
pub struct SchedulerHandle {
    sender: flume::Sender<SchedulerMessage>,
}

impl SchedulerHandle {
    /// Submit work without waiting for queue capacity.
    ///
    /// The returned future waits only for the executor's response. Queue
    /// admission remains explicit: a full queue returns an error immediately.
    pub async fn schedule(
        &self,
        plan: ExecutionPlan,
    ) -> Result<ExecutionReceipt, ExecutionRuntimeError> {
        let (reply, response) = flume::bounded(1);
        self.sender
            .try_send(SchedulerMessage::Schedule { plan, reply })
            .map_err(|error| match error {
                flume::TrySendError::Full(_) => ExecutionRuntimeError::SchedulerQueueFull,
                flume::TrySendError::Disconnected(_) => ExecutionRuntimeError::SchedulerStopped,
            })?;
        response
            .recv_async()
            .await
            .map_err(|_| ExecutionRuntimeError::SchedulerStopped)?
    }
}

/// Owns the scheduler and executor threads.
pub struct ExecutionRuntime {
    scheduler: SchedulerHandle,
    scheduler_thread: Option<JoinHandle<Result<(), ExecutionRuntimeError>>>,
    executor_sender: flume::Sender<ExecutorMessage>,
    executor_thread: Option<JoinHandle<Result<(), ExecutionRuntimeError>>>,
}

impl ExecutionRuntime {
    pub fn start(database: DatabaseHandle) -> Result<Self, ExecutionRuntimeError> {
        let (scheduler_sender, scheduler_receiver) = flume::bounded(SCHEDULER_QUEUE_CAPACITY);
        let (executor_sender, executor_receiver) = flume::bounded(EXECUTOR_QUEUE_CAPACITY);

        let executor_thread = thread::Builder::new()
            .name("ocg-executor".to_string())
            .spawn(move || {
                ntex::rt::System::new("ocg-executor", ntex::rt::DefaultRuntime)
                    .block_on(executor_loop(database, executor_receiver))
            })
            .map_err(ExecutionRuntimeError::Startup)?;

        let scheduler_executor_sender = executor_sender.clone();
        let scheduler_thread = match thread::Builder::new()
            .name("ocg-scheduler".to_string())
            .spawn(move || scheduler_loop(scheduler_receiver, scheduler_executor_sender))
        {
            Ok(thread) => thread,
            Err(error) => {
                executor_sender.send(ExecutorMessage::Shutdown).ok();
                executor_thread
                    .join()
                    .map_err(|_| ExecutionRuntimeError::WorkerPanicked)??;
                return Err(ExecutionRuntimeError::Startup(error));
            }
        };

        Ok(Self {
            scheduler: SchedulerHandle {
                sender: scheduler_sender,
            },
            scheduler_thread: Some(scheduler_thread),
            executor_sender,
            executor_thread: Some(executor_thread),
        })
    }

    pub fn scheduler(&self) -> SchedulerHandle {
        self.scheduler.clone()
    }

    pub fn shutdown(mut self) -> Result<(), ExecutionRuntimeError> {
        self.stop_and_join()
    }

    fn stop_and_join(&mut self) -> Result<(), ExecutionRuntimeError> {
        if self.scheduler_thread.is_some() {
            self.scheduler.sender.send(SchedulerMessage::Shutdown).ok();
        }
        let scheduler_result = join_worker(self.scheduler_thread.take());

        self.executor_sender.send(ExecutorMessage::Shutdown).ok();
        let executor_result = join_worker(self.executor_thread.take());

        scheduler_result?;
        executor_result
    }
}

fn join_worker(
    thread: Option<JoinHandle<Result<(), ExecutionRuntimeError>>>,
) -> Result<(), ExecutionRuntimeError> {
    let Some(thread) = thread else {
        return Ok(());
    };
    thread
        .join()
        .map_err(|_| ExecutionRuntimeError::WorkerPanicked)?
}

impl Drop for ExecutionRuntime {
    fn drop(&mut self) {
        let _ = self.stop_and_join();
    }
}

fn scheduler_loop(
    receiver: flume::Receiver<SchedulerMessage>,
    executor_sender: flume::Sender<ExecutorMessage>,
) -> Result<(), ExecutionRuntimeError> {
    while let Ok(message) = receiver.recv() {
        match message {
            SchedulerMessage::Schedule { plan, reply } => {
                let message = ExecutorMessage::Execute { plan, reply };
                match executor_sender.try_send(message) {
                    Ok(()) => {}
                    Err(flume::TrySendError::Full(ExecutorMessage::Execute { reply, .. })) => {
                        reply
                            .send(Err(ExecutionRuntimeError::ExecutorQueueFull))
                            .ok();
                    }
                    Err(flume::TrySendError::Disconnected(ExecutorMessage::Execute {
                        reply,
                        ..
                    })) => {
                        reply.send(Err(ExecutionRuntimeError::ExecutorStopped)).ok();
                        return Err(ExecutionRuntimeError::ExecutorStopped);
                    }
                    Err(flume::TrySendError::Full(ExecutorMessage::Shutdown))
                    | Err(flume::TrySendError::Disconnected(ExecutorMessage::Shutdown)) => {
                        unreachable!("scheduler only sends execution messages")
                    }
                }
            }
            SchedulerMessage::Shutdown => {
                return Ok(());
            }
        }
    }

    Ok(())
}

async fn executor_loop(
    database: DatabaseHandle,
    receiver: flume::Receiver<ExecutorMessage>,
) -> Result<(), ExecutionRuntimeError> {
    while let Ok(message) = receiver.recv_async().await {
        match message {
            ExecutorMessage::Execute { plan, reply } => {
                let result = execute_plan(database.clone(), plan).await;
                reply.send(result).ok();
            }
            ExecutorMessage::Shutdown => return Ok(()),
        }
    }
    Ok(())
}

async fn execute_plan(
    database: DatabaseHandle,
    mut plan: ExecutionPlan,
) -> Result<ExecutionReceipt, ExecutionRuntimeError> {
    let executor = plan.executor_id;
    if database.executor(executor).await?.is_none() {
        return Err(ExecutionRuntimeError::ExecutorNotFound(executor));
    }
    plan.job.assigned_executor = Some(executor);

    let input = database.save_job_input(None, plan.input).await?;
    let job = database.create_job_with_input(plan.job, input).await?;
    database.start_job(job).await?;
    let result = database
        .finalize_job(job, plan.outcome.finalization(plan.result))
        .await?;

    Ok(ExecutionReceipt {
        executor,
        input,
        job,
        result,
    })
}
