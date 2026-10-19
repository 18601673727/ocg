//! A host-level caller submits work through the scheduler of the host's own
//! execution runtime, and loses it when the host stops. Nothing here calls a
//! Provider.

use std::future::Future;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

use ocg::data_directory::DataDirectory;
use ocg::database::DatabaseWorker;
use ocg::host::{self, HostContext};
use ocg::orchestration::execution_runtime::{
    ExecutionPlan, ExecutionReceipt, ExecutionRuntimeError, SchedulerHandle,
};
use ocg::persistence::{FinalResult, JobInputDraft, JobOutcome, NewJob, OutputKind};

const WAIT: Duration = Duration::from_secs(30);

struct Unpark(std::thread::Thread);

impl Wake for Unpark {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

fn wait<F: Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
    let deadline = Instant::now() + WAIT;
    loop {
        if let Poll::Ready(value) = future.as_mut().poll(&mut Context::from_waker(&waker)) {
            return value;
        }
        assert!(Instant::now() < deadline, "a call did not resolve in time");
        std::thread::park_timeout(Duration::from_millis(50));
    }
}

fn plan(
    context: &HostContext,
    executor: ocg::persistence::ExecutorId,
    configuration: ocg::persistence::ExecutionConfiguration,
) -> ExecutionPlan {
    ExecutionPlan {
        executor_id: executor,
        input: JobInputDraft {
            objective: Some("summarize the day".into()),
            ..JobInputDraft::default()
        },
        job: NewJob {
            name: Some("host-submitted work".into()),
            eligible_at: None,
            parent: context.database().genesis(),
            project: None,
            dependencies: Vec::new(),
            input: JobInputDraft::default(),
            configuration: Some(configuration),
            previous: None,
        },
        outcome: JobOutcome::Completed,
        result: FinalResult {
            summary: Some("done".into()),
            code: None,
            goal_reached: Some(true),
            value: None,
        },
    }
}

#[test]
fn a_host_level_caller_submits_through_the_host_owned_scheduler() {
    let root = std::env::temp_dir().join(format!("ocg-host-scheduler-{}", std::process::id()));
    std::fs::remove_dir_all(&root).ok();
    let directory = DataDirectory::bootstrap(root.as_os_str().to_owned()).expect("data directory");

    let (kept, ran) = flume::bounded::<(
        SchedulerHandle,
        ExecutionReceipt,
        ocg::persistence::ExecutorId,
    )>(1);
    host::run_hosted(&directory, false, move |context: HostContext| async move {
        let executor = context
            .database()
            .create_executor(NewExecutor {
                name: "host worker".into(),
                concurrency_limit: NonZeroU32::MIN,
                llm_transports: std::collections::BTreeSet::from([
                    ocg::persistence::LlmTransport::OpenAiChatCompletions,
                ]),
            })
            .await
            .expect("register an Executor");
        use ocg::persistence::*;
        let connection = context
            .database()
            .create_config(NewConfig {
                name: "host connection".into(),
                body: ConfigBody::Connection(ConnectionConfig {
                    protocol: LlmTransport::OpenAiChatCompletions,
                    endpoint: None,
                    credential_ref: None,
                }),
            })
            .await
            .expect("connection");
        let model = context
            .database()
            .create_config(NewConfig {
                name: "host model".into(),
                body: ConfigBody::Model(ModelConfig {
                    connection_id: connection,
                    model: "fixture".into(),
                    context_window: None,
                    parameters: Default::default(),
                    declared_capabilities: Default::default(),
                }),
            })
            .await
            .expect("model");
        let configuration = ExecutionConfiguration {
            agent_profile: None,
            llm_profile: model.into(),
        };
        let receipt = context
            .scheduler()
            .schedule(plan(&context, executor, configuration))
            .await
            .expect("the host's runtime executes the Job");
        let deadline = Instant::now() + WAIT;
        while !context
            .database()
            .job(receipt.job)
            .await
            .expect("job")
            .expect("exists")
            .any_terminal()
        {
            assert!(Instant::now() < deadline);
            ntex::time::sleep(Duration::from_millis(5)).await;
        }
        kept.send((context.scheduler().clone(), receipt, executor))
            .expect("keep the scheduler");
    })
    .expect("the host runs and stops cleanly");
    let (scheduler, receipt, executor) = ran.recv().expect("the caller ran");

    // The host has stopped: its runtime refuses work, through every clone.
    let late = ExecutionPlan {
        executor_id: executor,
        input: JobInputDraft::default(),
        job: NewJob {
            name: Some("too late".into()),
            eligible_at: None,
            parent: receipt.job,
            project: None,
            dependencies: Vec::new(),
            input: JobInputDraft::default(),
            configuration: None,
            previous: None,
        },
        outcome: JobOutcome::Completed,
        result: FinalResult {
            summary: None,
            code: None,
            goal_reached: None,
            value: None,
        },
    };
    assert!(matches!(
        wait(scheduler.schedule(late)),
        Err(ExecutionRuntimeError::ShuttingDown)
    ));

    // What the run persisted.
    let worker = DatabaseWorker::start(&directory).expect("reopen");
    let handle = worker.handle();
    assert!(
        handle.abandoned_at_open().is_empty(),
        "the run left nothing running"
    );
    let job = wait(handle.job(receipt.job))
        .expect("read job")
        .expect("job exists");
    assert_eq!(job.parent, handle.genesis());
    assert_eq!(job.assigned_executor, Some(executor));
    assert_eq!(job.input, receipt.input);
    assert!(job.started_at.is_some() && job.completed_at.is_some());
    assert!(job.unknown_at.is_none() && job.failed_at.is_none());
    let outputs = wait(handle.job_outputs(receipt.job, None, 10)).expect("read outputs");
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].kind, OutputKind::Result);
    assert_eq!(outputs[0].summary.as_deref(), Some("done"));
    assert_eq!(
        receipt.result, None,
        "schedule acknowledges admission before finalization"
    );
    let input = wait(handle.job_input(receipt.input))
        .expect("read input")
        .expect("input exists");
    assert_eq!(input.objective.as_deref(), Some("summarize the day"));
    assert!(input.submitted_at.is_some(), "the input was frozen");
    worker.shutdown().expect("close the database");
    std::fs::remove_dir_all(&root).ok();
}
