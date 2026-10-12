//! Bounded scheduler-to-executor handoff for the 0.5 execution plane.
//!
//! The scheduler owns admission to the executor queue. The executor owns the
//! execution plan after dequeueing it and is the only worker that calls the
//! database handle. Every worker boundary is a bounded `flume` channel; the
//! database handle itself has a separate bounded command channel.

use std::sync::Arc;
use std::thread::{self, JoinHandle};

use crate::database::{DatabaseError, DatabaseHandle};
use crate::persistence::{
    AppendedOutput, ExecutorId, FinalResult, IntermediateOutputKind, JobFinalization, JobId,
    JobInputDraft, JobInputId, JobOutcome, NewAvailabilityStat, NewJob, NewJobOutput, NewMetrics,
    Timestamp,
};
use crate::provider_execution::{
    self, CredentialResolver, EnvironmentCredentialResolver, ProviderExecutionConfig,
    ProviderExecutionError, ProviderOutputSink,
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
    /// unless this is an Llm Executor supporting the transport of the
    /// configuration frozen for it; see [`DatabaseHandle::compatible_executors`].
    /// Choosing among compatible Executors is the caller's decision, and the
    /// choice never changes the frozen Provider, model or parameters.
    pub executor_id: ExecutorId,
    pub input: JobInputDraft,
    pub job: NewJob,
    pub outcome: JobOutcome,
    pub result: FinalResult,
}

/// A provider-backed execution request. Its LLM configuration is frozen when
/// the Job is admitted; execution reads that snapshot after the durable start.
pub struct ProviderExecutionPlan {
    pub executor_id: ExecutorId,
    pub input: JobInputDraft,
    pub job: NewJob,
    pub credential_resolver: Arc<dyn CredentialResolver>,
    pub provider_config: ProviderExecutionConfig,
}

impl ProviderExecutionPlan {
    pub fn environment_credentials(
        executor_id: ExecutorId,
        input: JobInputDraft,
        job: NewJob,
    ) -> Self {
        Self {
            executor_id,
            input,
            job,
            credential_resolver: Arc::new(EnvironmentCredentialResolver),
            provider_config: ProviderExecutionConfig::default(),
        }
    }
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
    #[error("provider execution failed: {0}")]
    Provider(String),
}

type ExecutionReply = flume::Sender<Result<ExecutionReceipt, ExecutionRuntimeError>>;

enum SchedulerMessage {
    Schedule {
        plan: ExecutionPlan,
        reply: ExecutionReply,
    },
    ScheduleProvider {
        plan: ProviderExecutionPlan,
        reply: ExecutionReply,
    },
    Shutdown,
}

enum ExecutorMessage {
    Execute {
        plan: ExecutionPlan,
        reply: ExecutionReply,
    },
    ExecuteProvider {
        plan: ProviderExecutionPlan,
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

    pub async fn schedule_provider(
        &self,
        plan: ProviderExecutionPlan,
    ) -> Result<ExecutionReceipt, ExecutionRuntimeError> {
        let (reply, response) = flume::bounded(1);
        self.sender
            .try_send(SchedulerMessage::ScheduleProvider { plan, reply })
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
                    Err(flume::TrySendError::Full(ExecutorMessage::ExecuteProvider { .. }))
                    | Err(flume::TrySendError::Disconnected(ExecutorMessage::ExecuteProvider {
                        ..
                    })) => {
                        unreachable!("scheduler sent the wrong execution message variant")
                    }
                }
            }
            SchedulerMessage::ScheduleProvider { plan, reply } => {
                let message = ExecutorMessage::ExecuteProvider { plan, reply };
                match executor_sender.try_send(message) {
                    Ok(()) => {}
                    Err(flume::TrySendError::Full(ExecutorMessage::ExecuteProvider {
                        reply,
                        ..
                    })) => {
                        reply
                            .send(Err(ExecutionRuntimeError::ExecutorQueueFull))
                            .ok();
                    }
                    Err(flume::TrySendError::Disconnected(ExecutorMessage::ExecuteProvider {
                        reply,
                        ..
                    })) => {
                        reply.send(Err(ExecutionRuntimeError::ExecutorStopped)).ok();
                        return Err(ExecutionRuntimeError::ExecutorStopped);
                    }
                    Err(flume::TrySendError::Full(ExecutorMessage::Execute { .. }))
                    | Err(flume::TrySendError::Disconnected(ExecutorMessage::Execute { .. }))
                    | Err(flume::TrySendError::Full(ExecutorMessage::Shutdown))
                    | Err(flume::TrySendError::Disconnected(ExecutorMessage::Shutdown)) => {
                        unreachable!("scheduler only sends provider execution messages")
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
            ExecutorMessage::ExecuteProvider { plan, reply } => {
                let result = execute_provider_plan(database.clone(), plan).await;
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

async fn execute_provider_plan(
    database: DatabaseHandle,
    mut plan: ProviderExecutionPlan,
) -> Result<ExecutionReceipt, ExecutionRuntimeError> {
    let executor = plan.executor_id;
    if database.executor(executor).await?.is_none() {
        return Err(ExecutionRuntimeError::ExecutorNotFound(executor));
    }
    plan.job.assigned_executor = Some(executor);
    let input = database.save_job_input(None, plan.input).await?;
    let job = database.create_job_with_input(plan.job, input).await?;
    database.start_job(job).await?;

    let configuration = match database.job_configuration(job).await? {
        Some(configuration) => configuration,
        None => {
            let result = database
                .finalize_job(
                    job,
                    JobFinalization::Failed(FinalResult {
                        summary: Some(
                            "provider execution requires frozen LLM configuration".to_owned(),
                        ),
                        code: None,
                        goal_reached: Some(false),
                        value: None,
                    }),
                )
                .await?;
            return Ok(ExecutionReceipt {
                executor,
                input,
                job,
                result,
            });
        }
    };
    let input_record = match database.job_input(input).await? {
        Some(input) => input,
        None => {
            let result = database
                .finalize_job(
                    job,
                    JobFinalization::Failed(FinalResult {
                        summary: Some("submitted Job input disappeared".to_owned()),
                        code: None,
                        goal_reached: Some(false),
                        value: None,
                    }),
                )
                .await?;
            return Ok(ExecutionReceipt {
                executor,
                input,
                job,
                result,
            });
        }
    };
    let mut output_sink = DatabaseOutputSink {
        database: database.clone(),
        job,
    };
    let execution = provider_execution::execute_chat_with_sink(
        &configuration.frozen,
        input_record.objective.as_deref(),
        plan.credential_resolver.as_ref(),
        &plan.provider_config,
        &mut output_sink,
    )
    .await;

    match execution {
        Ok(result) => {
            let final_output = database
                .finalize_job(
                    job,
                    JobFinalization::Completed(FinalResult {
                        summary: Some(result.text.clone()),
                        code: None,
                        goal_reached: None,
                        value: Some(provider_execution::result_value(&result)),
                    }),
                )
                .await?;
            database
                .record_metrics(NewMetrics {
                    job,
                    observed_at: Timestamp::now(),
                    measurements: provider_execution::metrics(&result),
                })
                .await?;
            database
                .record_availability_stat(NewAvailabilityStat {
                    job,
                    observed_at: Timestamp::now(),
                    observation: provider_execution::success_observation(&result),
                })
                .await?;
            Ok(ExecutionReceipt {
                executor,
                input,
                job,
                result: final_output,
            })
        }
        Err(error) => settle_provider_error(database, executor, input, job, error).await,
    }
}

struct DatabaseOutputSink {
    database: DatabaseHandle,
    job: JobId,
}

impl ProviderOutputSink for DatabaseOutputSink {
    fn append<'a>(
        &'a mut self,
        text: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ProviderExecutionError>> + 'a>>
    {
        Box::pin(async move {
            self.database
                .append_job_output(
                    self.job,
                    NewJobOutput {
                        kind: IntermediateOutputKind::Progress,
                        summary: Some(text.to_owned()),
                        code: None,
                        goal_reached: None,
                        value: None,
                    },
                )
                .await
                .map(|_| ())
                .map_err(|error| ProviderExecutionError::Unknown {
                    message: format!("database could not persist provider output: {error}"),
                })
        })
    }
}

async fn settle_provider_error(
    database: DatabaseHandle,
    executor: ExecutorId,
    input: JobInputId,
    job: JobId,
    error: ProviderExecutionError,
) -> Result<ExecutionReceipt, ExecutionRuntimeError> {
    let unknown = matches!(
        error,
        ProviderExecutionError::Unknown { .. } | ProviderExecutionError::Incomplete { .. }
    );
    let result = if unknown {
        database.finalize_job(job, JobFinalization::Unknown).await?
    } else {
        database
            .finalize_job(
                job,
                JobFinalization::Failed(FinalResult {
                    summary: Some(error.to_string()),
                    code: None,
                    goal_reached: Some(false),
                    value: None,
                }),
            )
            .await?
    };
    if !matches!(
        error,
        ProviderExecutionError::NotDelivered { .. } | ProviderExecutionError::Build { .. }
    ) {
        database
            .record_availability_stat(NewAvailabilityStat {
                job,
                observed_at: Timestamp::now(),
                observation: provider_execution::error_observation(&error),
            })
            .await?;
    }
    Ok(ExecutionReceipt {
        executor,
        input,
        job,
        result,
    })
}
