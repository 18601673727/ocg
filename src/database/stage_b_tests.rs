use super::*;
use crate::persistence::*;
use crate::test_support::{WAIT, data_directory, poll_once, wait};
use std::collections::BTreeSet;
use std::num::NonZeroU32;

fn provision(database: &DatabaseHandle) -> (ExecutionPoolId, ProfileId) {
    let provider = wait(database.create_llm_provider(NewLlmProvider {
        name: "p".into(),
        transport: LlmTransport::OpenAiChatCompletions,
        endpoint: None,
        credential_ref: None,
    }))
    .unwrap();
    let llm = wait(database.create_profile(NewProfile {
        name: "llm".into(),
        config: NewProfileConfig::Llm {
            provider,
            config: LlmConfig {
                model: "m".into(),
                parameters: Default::default(),
                declared_capabilities: Default::default(),
            },
        },
    }))
    .unwrap();
    let profile = wait(database.create_profile(NewProfile {
        name: "pool".into(),
        config: NewProfileConfig::ExecutionPool(ExecutionPoolConfig {
            concurrency_limit: NonZeroU32::new(16).unwrap(),
            capabilities: CapabilitySet {
                operations: BTreeSet::from([ExecutionOperation::LlmRequest]),
                llm_protocols: BTreeSet::from([LlmTransport::OpenAiChatCompletions]),
            },
        }),
    }))
    .unwrap();
    let pool = wait(database.create_execution_pool(NewExecutionPool {
        name: "ordinary".into(),
        profile,
        enabled: true,
    }))
    .unwrap();
    (pool, llm)
}

/// A second, independent Pool with a single slot, so a test can tell a domain
/// that is quarantined from one that is merely full.
fn narrow_pool(database: &DatabaseHandle, name: &str) -> ExecutionPoolId {
    let profile = wait(database.create_profile(NewProfile {
        name: name.into(),
        config: NewProfileConfig::ExecutionPool(ExecutionPoolConfig {
            concurrency_limit: NonZeroU32::new(1).unwrap(),
            capabilities: CapabilitySet {
                operations: BTreeSet::from([ExecutionOperation::LlmRequest]),
                llm_protocols: BTreeSet::from([LlmTransport::OpenAiChatCompletions]),
            },
        }),
    }))
    .unwrap();
    wait(database.create_execution_pool(NewExecutionPool {
        name: name.into(),
        profile,
        enabled: true,
    }))
    .unwrap()
}

fn pending(database: &DatabaseHandle, llm: ProfileId) -> JobId {
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
            configuration: Some(ExecutionConfiguration {
                agent_profile: None,
                llm_profile: llm,
            }),
            assigned_executor: None,
            previous: None,
            replaces: None,
        },
        input,
    ))
    .unwrap()
}

#[test]
fn consuming_claim_emits_neither_generation_unknown_release_and_recovery_are_durable() {
    let (_root, directory) = data_directory("ocg-stage-b-worker", "wake");
    let mut worker = DatabaseWorker::start(&directory).unwrap();
    let wake = worker.take_scheduler_wake().unwrap();
    let database = worker.handle();
    let (pool, llm) = provision(&database);
    let job = pending(&database, llm);
    let before = (wake.dirty_generation(), wake.capacity_generation());
    wait(database.claim_execution(ClaimRequest {
        job,
        allowed_pools: BTreeSet::from([pool]),
    }))
    .unwrap()
    .unwrap();
    assert_eq!(
        (wake.dirty_generation(), wake.capacity_generation()),
        before
    );
    wait(database.finalize_job(job, JobFinalization::Unknown)).unwrap();
    assert_eq!(wait(database.occupancy()).unwrap(), vec![(pool, job)]);
    let unknown_generation = wake.capacity_generation();
    wait(database.confirm_job_stopped(job)).unwrap();
    assert!(wake.capacity_generation() > unknown_generation);
    assert!(wait(database.occupancy()).unwrap().is_empty());
    let released = (wake.dirty_generation(), wake.capacity_generation());
    wait(database.recover_storage()).unwrap();
    assert!(wake.dirty_generation() > released.0 && wake.capacity_generation() > released.1);
    worker.shutdown().unwrap();
}

#[test]
fn duplicate_racing_claims_yield_one_known_committed_grant() {
    let (_root, directory) = data_directory("ocg-stage-b-worker", "race");
    let worker = DatabaseWorker::start(&directory).unwrap();
    let database = worker.handle();
    let (pool, llm) = provision(&database);
    let job = pending(&database, llm);
    let (entered, release) = database.group_probe().hold_next_group();
    let mut first = std::pin::pin!(database.claim_execution(ClaimRequest {
        job,
        allowed_pools: BTreeSet::from([pool])
    }));
    assert!(poll_once(first.as_mut()).is_none());
    entered.recv_timeout(WAIT).unwrap();
    let mut second = std::pin::pin!(database.claim_execution(ClaimRequest {
        job,
        allowed_pools: BTreeSet::from([pool])
    }));
    assert!(poll_once(second.as_mut()).is_none());
    release.send(()).unwrap();
    assert!(wait(first).unwrap().is_ok());
    assert_eq!(wait(second).unwrap(), Err(ClaimRefusal::NotPending));
    assert_eq!(wait(database.occupancy()).unwrap(), vec![(pool, job)]);
    worker.shutdown().unwrap();
}

#[test]
fn uncertain_claim_flush_never_grants_and_marker_resolution_quarantines_committed_claim() {
    let (_root, directory) = data_directory("ocg-stage-b-worker", "uncertain");
    let mut worker = DatabaseWorker::start(&directory).unwrap();
    let wake = worker.take_scheduler_wake().unwrap();
    let database = worker.handle();
    let (pool, llm) = provision(&database);
    let job = pending(&database, llm);
    let before = (wake.dirty_generation(), wake.capacity_generation());
    let probe = database.group_probe();
    let (entered, release) = probe.hold_next_group();
    let mut claim = std::pin::pin!(database.claim_execution(ClaimRequest {
        job,
        allowed_pools: BTreeSet::from([pool])
    }));
    assert!(poll_once(claim.as_mut()).is_none());
    entered.recv_timeout(WAIT).unwrap();
    let mut extra = std::pin::pin!(database.create_project(NewProject {
        name: "flush group".into(),
        description: None
    }));
    assert!(poll_once(extra.as_mut()).is_none());
    probe.fail_next_flush.store(true, Ordering::SeqCst);
    release.send(()).unwrap();
    let operation = match wait(claim) {
        Err(DatabaseError::OutcomeUnknown { operation, .. }) => operation,
        result => panic!("claim must be unknown, got {result:?}"),
    };
    assert!(matches!(
        wait(extra),
        Err(DatabaseError::OutcomeUnknown { .. })
    ));
    assert_eq!(
        (wake.dirty_generation(), wake.capacity_generation()),
        before
    );
    wait(database.recover_storage()).unwrap();
    assert!(wake.dirty_generation() > before.0 && wake.capacity_generation() > before.1);
    let marker = wait(database.reconcile_operation(operation)).unwrap();
    wait(database.reconcile_unexecuted_claim(job, operation)).unwrap();
    let row = wait(database.job(job)).unwrap().unwrap();
    match marker {
        OperationStatus::Committed(_) => {
            // The claim committed, so the Job started in an opening that is
            // gone. Its outcome is unknown and its stop is *not* confirmed:
            // nothing established that the external work it began has ended.
            assert!(row.unknown_at.is_some() && row.stop_confirmed_at.is_none());
            // So the slot it holds stays occupied and is quarantined.
            assert_eq!(
                wait(database.occupancy()).unwrap(),
                vec![(pool, job)],
                "a committed claim's capacity is quarantined, not released"
            );
            let quarantined = wait(database.unresolved_executions()).unwrap();
            assert_eq!(quarantined.len(), 1);
            assert_eq!(quarantined[0].0, job);
            assert_eq!(quarantined[0].1.pool, Some(pool));
        }
        OperationStatus::NotCommitted => {
            assert!(row.started_at.is_none());
            assert!(wait(database.occupancy()).unwrap().is_empty());
            assert!(wait(database.unresolved_executions()).unwrap().is_empty());
        }
        status => panic!("recovery must resolve operation, got {status:?}"),
    }
    worker.shutdown().unwrap();
}

#[test]
fn process_reopen_quarantines_unspawned_claim_without_rerunning() {
    let (_root, directory) = data_directory("ocg-stage-b-worker", "restart");
    let worker = DatabaseWorker::start(&directory).unwrap();
    let database = worker.handle();
    let (pool, llm) = provision(&database);
    let narrow = narrow_pool(&database, "narrow");
    let job = pending(&database, llm);
    wait(database.claim_execution(ClaimRequest {
        job,
        allowed_pools: BTreeSet::from([narrow]),
    }))
    .unwrap()
    .unwrap();
    let binding = wait(database.frozen_pool_binding(job))
        .unwrap()
        .expect("bound");
    assert_eq!(binding.pool, narrow);
    worker.shutdown().unwrap();
    let worker = DatabaseWorker::start(&directory).unwrap();
    let database = worker.handle();
    assert!(database.abandoned_at_open().contains(&job));
    assert_eq!(
        wait(database.frozen_pool_binding(job)).unwrap(),
        Some(binding)
    );
    // The earlier opening is gone, but nothing showed that the work it began
    // stopped, so the slot stays occupied and its capacity is quarantined.
    assert_eq!(wait(database.occupancy()).unwrap(), vec![(narrow, job)]);
    let quarantined = wait(database.unresolved_executions()).unwrap();
    assert_eq!(quarantined.len(), 1);
    assert_eq!(quarantined[0].0, job);
    assert_eq!(quarantined[0].1.pool, Some(narrow));
    let row = wait(database.job(job)).unwrap().unwrap();
    assert!(row.unknown_at.is_some() && row.stop_confirmed_at.is_none());
    assert_eq!(
        wait(database.claim_execution(ClaimRequest {
            job,
            allowed_pools: BTreeSet::from([narrow])
        }))
        .unwrap(),
        Err(ClaimRefusal::NotPending)
    );
    // The quarantined Pool has no free slot, so a new Job cannot enter it,
    // while a Pool whose safety was established is unaffected and admits.
    let blocked = pending(&database, llm);
    assert_eq!(
        wait(database.claim_execution(ClaimRequest {
            job: blocked,
            allowed_pools: BTreeSet::from([narrow])
        }))
        .unwrap(),
        Err(ClaimRefusal::NoCapacity(CapacityScope::Pools(
            BTreeSet::from([narrow])
        )))
    );
    let admitted = pending(&database, llm);
    assert!(
        wait(database.claim_execution(ClaimRequest {
            job: admitted,
            allowed_pools: BTreeSet::from([pool])
        }))
        .unwrap()
        .is_ok(),
        "an independent Pool still admits"
    );
    worker.shutdown().unwrap();
}

/// One unbound local Job, started the way the shipped runtime starts work: a
/// local execution claim and no frozen Pool binding, so nothing of it is
/// attributable to a Pool.
fn unbound_local_job(database: &DatabaseHandle, llm: ProfileId) -> JobId {
    let input = wait(database.save_job_input(None, JobInputDraft::default())).unwrap();
    wait(database.create_local_job_with_input(
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
            configuration: Some(ExecutionConfiguration {
                agent_profile: None,
                llm_profile: llm,
            }),
            assigned_executor: None,
            previous: None,
            replaces: None,
        },
        input,
    ))
    .unwrap()
}

/// Repeated unclean exits, the way the aggregation service makes them: every
/// start leaves one unbound Job behind when the process is killed, so each cycle
/// adds one more unattributed quarantine until the global domain is past its
/// limit and can never be drained.
///
/// The database must keep starting. It did not: startup refused to open on the
/// nineteenth cycle, which turned a condition that is permanent by design into
/// one no operator could get out of. This is that arithmetic, driven through the
/// real worker start and quarantine path rather than by hand.
#[test]
fn repeated_unclean_exits_saturate_the_global_domain_without_refusing_to_start() {
    let (_root, directory) = data_directory("ocg-stage-b-worker", "unclean-exits");
    // One more than the limit, so the last cycle is the one that used to fail.
    let cycles = usize::from(GLOBAL_EXECUTION_LIMIT) + 1;
    let mut abandoned = Vec::new();

    for cycle in 1..=cycles {
        let worker = DatabaseWorker::start(&directory).unwrap();
        let database = worker.handle();
        let (_pool, llm) = provision(&database);
        let job = unbound_local_job(&database, llm);
        assert!(
            wait(database.job(job))
                .unwrap()
                .unwrap()
                .started_at
                .is_some(),
            "cycle {cycle}: started and left unsettled"
        );
        abandoned.push(job);
        // The opening ends without settling anything, which is all the
        // accounting below turns on: the local claim survives into the next
        // opening. A shutdown reproduces that inside one process; the signal
        // that actually causes it is the subject of the process tests.
        worker.shutdown().unwrap();
        let worker = DatabaseWorker::start(&directory)
            .unwrap_or_else(|error| panic!("cycle {cycle} of {cycles}: startup failed: {error:?}"));
        worker.shutdown().unwrap();
    }

    // Past the limit, everything is quarantined, unattributed, and held.
    let worker = DatabaseWorker::start(&directory).unwrap();
    let database = worker.handle();
    let held = wait(database.unresolved_executions()).unwrap();
    assert_eq!(
        held.len(),
        cycles,
        "one record per unclean exit, none merged"
    );
    assert!(
        held.iter().all(|(_, record)| record.pool.is_none()),
        "none is attributable to a Pool, so none is charged twice"
    );
    assert!(
        wait(database.occupancy()).unwrap().is_empty(),
        "and none released anything into a Pool"
    );
    for &job in &abandoned {
        let record = wait(database.job(job)).unwrap().unwrap();
        assert!(
            record.unknown_at.is_some() && record.stop_confirmed_at.is_none(),
            "{job}: unknown, and never confirmed stopped"
        );
    }

    // Saturated means saturated: the global domain admits nothing new.
    let (pool, llm) = provision(&database);
    let waiting = pending(&database, llm);
    assert_eq!(
        wait(database.claim_execution(ClaimRequest {
            job: waiting,
            allowed_pools: BTreeSet::from([pool])
        }))
        .unwrap(),
        Err(ClaimRefusal::NoCapacity(CapacityScope::Global)),
        "the global domain is held past its limit"
    );
    worker.shutdown().unwrap();

    // Restarts after saturation neither duplicate the accounting nor drain it.
    for round in 1..=2 {
        let worker = DatabaseWorker::start(&directory).unwrap();
        let database = worker.handle();
        assert_eq!(
            wait(database.unresolved_executions()).unwrap().len(),
            cycles,
            "round {round}: no record added, none lost"
        );
        assert!(wait(database.occupancy()).unwrap().is_empty());
        worker.shutdown().unwrap();
    }
}
