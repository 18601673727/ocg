use std::collections::BTreeSet;
use std::future::Future;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

use ocg::data_directory::DataDirectory;
use ocg::database::{DatabaseError, DatabaseWorker};
use ocg::persistence::{
    CapabilitySet, ConfigBody, ConfigId, ExecutionOperation, ExecutionPoolConfig, LlmTransport,
    NewConfig, NewExecutionPool, Violation,
};

struct Unpark(std::thread::Thread);
impl Wake for Unpark {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}
fn wait<F: Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Poll::Ready(value) = future.as_mut().poll(&mut Context::from_waker(&waker)) {
            return value;
        }
        assert!(Instant::now() < deadline);
        std::thread::park_timeout(Duration::from_millis(10));
    }
}
fn config(limit: u32) -> ExecutionPoolConfig {
    ExecutionPoolConfig {
        concurrency_limit: NonZeroU32::new(limit).unwrap(),
        capabilities: CapabilitySet {
            operations: BTreeSet::from([ExecutionOperation::LlmRequest]),
            llm_protocols: BTreeSet::from([LlmTransport::OpenAiChatCompletions]),
        },
    }
}
#[test]
fn pools_are_durable_config_and_dormant() {
    let root = std::env::temp_dir().join(format!("ocg-stage-b-pools-{}", std::process::id()));
    let directory = DataDirectory::bootstrap(root.as_os_str().to_owned()).unwrap();
    let mut worker = DatabaseWorker::start(&directory).unwrap();
    let wake = worker.take_scheduler_wake().unwrap();
    let database = worker.handle();
    let pool = wait(database.create_execution_pool(NewExecutionPool {
        name: "ordinary".into(),
        config: config(16),
        enabled: true,
    }))
    .unwrap();
    let first = wait(database.execution_pool(pool)).unwrap().unwrap();
    assert!(first.enabled);
    assert_eq!(first.config, config(16));
    assert_eq!(first.edit_seq, 1);
    assert!(first.config.capabilities.supports(
        ExecutionOperation::LlmRequest,
        Some(LlmTransport::OpenAiChatCompletions)
    ));
    assert!(!CapabilitySet::default().supports(ExecutionOperation::LlmRequest, None));
    let generation = wake.dirty_generation();
    wait(database.set_execution_pool_enabled(pool, false)).unwrap();
    assert!(wake.dirty_generation() > generation);
    // An edit is in place: the identity stays, the edit counter rises.
    wait(database.update_execution_pool(pool, config(2))).unwrap();
    let agent = wait(database.create_config(NewConfig {
        name: "agent".into(),
        body: ConfigBody::Agent(Default::default()),
    }))
    .unwrap();
    // A config of another kind is not a Pool.
    assert!(matches!(
        wait(database.update_execution_pool(agent.into(), config(1))),
        Err(DatabaseError::Rejected(Violation::WrongConfigKind { .. }))
    ));
    let kept = wait(database.execution_pool(pool)).unwrap().unwrap();
    assert_eq!(kept.config, config(2));
    assert_eq!(kept.id, first.id);
    assert_eq!(kept.edit_seq, 3);
    assert!(!kept.enabled);
    assert!(
        wait(database.execution_pool(agent.into()))
            .unwrap()
            .is_none()
    );
    let jobs = wait(database.jobs(None, 100)).unwrap();
    assert_eq!(jobs.len(), 0, "Pool provisioning must create no work");
    worker.shutdown().unwrap();
    let worker = DatabaseWorker::start(&directory).unwrap();
    let database = worker.handle();
    assert_eq!(wait(database.execution_pool(pool)).unwrap(), Some(kept));
    assert_eq!(wait(database.execution_pools(None, 10)).unwrap().len(), 1);
    let record = wait(database.config(ConfigId::new(pool.get())))
        .unwrap()
        .unwrap();
    assert_eq!(record.edit_seq, 3);
    worker.shutdown().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
