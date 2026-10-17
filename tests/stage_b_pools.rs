use std::collections::BTreeSet;
use std::future::Future;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

use ocg::data_directory::DataDirectory;
use ocg::database::{DatabaseError, DatabaseWorker};
use ocg::persistence::{
    CapabilitySet, ExecutionOperation, ExecutionPoolConfig, ExecutionPoolProfile, LlmTransport,
    NewExecutionPool, NewProfile, NewProfileConfig, OperationKind, ProfileConfig, RecordRef,
    Violation,
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
fn pools_and_profiles_are_durable_and_dormant() {
    let root = std::env::temp_dir().join(format!("ocg-stage-b-pools-{}", std::process::id()));
    let directory = DataDirectory::bootstrap(root.as_os_str().to_owned()).unwrap();
    let mut worker = DatabaseWorker::start(&directory).unwrap();
    let wake = worker.take_scheduler_wake().unwrap();
    let database = worker.handle();
    let profile = wait(database.create_profile(NewProfile {
        name: "capacity".into(),
        config: NewProfileConfig::ExecutionPool(config(16)),
    }))
    .unwrap();
    let pool = wait(database.create_execution_pool(NewExecutionPool {
        name: "ordinary".into(),
        profile,
        enabled: true,
    }))
    .unwrap();
    let first = wait(database.execution_pool(pool)).unwrap().unwrap();
    assert!(first.enabled);
    assert_eq!(first.profile, profile);
    let typed: ExecutionPoolProfile = wait(database.profile(profile))
        .unwrap()
        .unwrap()
        .try_into()
        .unwrap();
    assert_eq!(typed.config, config(16));
    assert!(typed.config.capabilities.supports(
        ExecutionOperation::LlmRequest,
        Some(LlmTransport::OpenAiChatCompletions)
    ));
    assert!(!CapabilitySet::default().supports(ExecutionOperation::LlmRequest, None));
    let generation = wake.dirty_generation();
    wait(database.set_execution_pool_enabled(pool, false)).unwrap();
    assert!(wake.dirty_generation() > generation);
    let replacement = wait(database.create_profile(NewProfile {
        name: "smaller".into(),
        config: NewProfileConfig::ExecutionPool(config(2)),
    }))
    .unwrap();
    wait(database.set_execution_pool_profile(pool, replacement)).unwrap();
    wait(database.update_profile(
        replacement,
        NewProfile {
            name: "edited".into(),
            config: NewProfileConfig::ExecutionPool(config(1)),
        },
    ))
    .unwrap();
    let agent = wait(database.create_profile(NewProfile {
        name: "agent".into(),
        config: NewProfileConfig::Agent {
            config: Default::default(),
            llm_targets: vec![],
        },
    }))
    .unwrap();
    assert!(matches!(
        wait(database.set_execution_pool_profile(pool, agent)),
        Err(DatabaseError::Rejected(Violation::WrongProfileKind(_)))
    ));
    let kept = wait(database.execution_pool(pool)).unwrap().unwrap();
    assert_eq!(kept.profile, replacement);
    assert_eq!(kept.id, first.id);
    assert!(!kept.enabled);
    let jobs = wait(database.jobs(None, 100)).unwrap();
    assert_eq!(jobs.len(), 1, "Pool provisioning must create no work");
    let markers = wait(database.operation_markers(None, 100)).unwrap();
    assert!(
        markers
            .iter()
            .any(|marker| marker.kind == OperationKind::CreateExecutionPool
                && marker.created == Some(RecordRef::ExecutionPool(pool)))
    );
    worker.shutdown().unwrap();
    let worker = DatabaseWorker::start(&directory).unwrap();
    let database = worker.handle();
    assert_eq!(wait(database.execution_pool(pool)).unwrap(), Some(kept));
    assert_eq!(wait(database.execution_pools(None, 10)).unwrap().len(), 1);
    assert_eq!(
        wait(database.profile(replacement)).unwrap().unwrap().config,
        ProfileConfig::ExecutionPool(config(1))
    );
    worker.shutdown().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
