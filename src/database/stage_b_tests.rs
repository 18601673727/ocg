use super::*;
use crate::persistence::*;
use crate::test_support::{WAIT, data_directory, poll_once, wait};
use std::collections::BTreeSet;
use std::num::NonZeroU32;

fn pool_config(limit: u32) -> ExecutionPoolConfig {
    ExecutionPoolConfig {
        concurrency_limit: NonZeroU32::new(limit).unwrap(),
        capabilities: CapabilitySet {
            operations: BTreeSet::from([ExecutionOperation::LlmRequest]),
            llm_protocols: BTreeSet::from([LlmTransport::OpenAiChatCompletions]),
        },
    }
}

fn provision(database: &DatabaseHandle) -> (ExecutionPoolId, ProfileId) {
    let connection = wait(database.create_config(NewConfig {
        name: "p".into(),
        body: ConfigBody::Connection(ConnectionConfig {
            protocol: LlmTransport::OpenAiChatCompletions,
            endpoint: None,
            credential_ref: None,
        }),
    }))
    .unwrap();
    let llm = wait(database.create_config(NewConfig {
        name: "llm".into(),
        body: ConfigBody::Model(ModelConfig {
            connection_id: connection,
            model: "m".into(),
            context_window: None,
            parameters: Default::default(),
            declared_capabilities: Default::default(),
        }),
    }))
    .unwrap();
    let pool = wait(database.create_execution_pool(NewExecutionPool {
        name: "ordinary".into(),
        config: pool_config(16),
        enabled: true,
    }))
    .unwrap();
    (pool, llm.into())
}

/// A second, independent Pool with a single slot, so a test can tell a domain
/// that is quarantined from one that is merely full.
fn narrow_pool(database: &DatabaseHandle, name: &str) -> ExecutionPoolId {
    wait(database.create_execution_pool(NewExecutionPool {
        name: name.into(),
        config: pool_config(1),
        enabled: true,
    }))
    .unwrap()
}

fn pending(database: &DatabaseHandle, llm: ProfileId) -> JobId {
    wait(database.create_job(NewJob {
        process_grant: None,
        name: None,
        eligible_at: None,
        parent: database.genesis(),
        project: None,
        input: JobInputDraft::default(),
        configuration: Some(ExecutionConfiguration {
            agent_profile: None,
            llm_profile: llm,
        }),
        previous: None,
    }))
    .unwrap()
}

/// Admits a call of a handler that can spawn, as the tool loop does before it
/// invokes the handler, and leaves it without an outcome: an OS process of the
/// Job may be alive until the call has one.
fn leave_a_spawning_call(database: &DatabaseHandle, job: JobId) -> ToolCallRef {
    let request = RequestNumber::FIRST;
    wait(database.record_metrics(NewMetrics {
        job,
        request,
        observed_at: Timestamp::now(),
        measurements: Measurements {
            ttfb_ms: Some(1),
            ..Measurements::default()
        },
        token: None,
    }))
    .unwrap();
    let call = ToolCallRef {
        job,
        request,
        index: 0,
    };
    let admission = ToolAdmission {
        handler_version: "workspace_run_command-1".into(),
        effect: ToolEffect::NonIdempotent,
        parameters: "{}".into(),
        arguments: ToolPayload::new("{}"),
        authorization: AuthorizationBasis::RegistryGrant,
    };
    wait(database.record_tool_decision(NewToolEvidence {
        approval: None,
        call,
        tool_call_id: "call-1".into(),
        name: "workspace_run_command".into(),
        spawns: admission.spawns(),
        decision: ToolDecision::Admitted(admission),
    }))
    .unwrap();
    call
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
    leave_a_spawning_call(&database, job);
    wait(database.finalize_job(job, JobFinalization::Unknown)).unwrap();
    // A process of the Job may be alive, so the slot is quarantined, not freed.
    assert_eq!(wait(database.occupancy()).unwrap(), vec![(pool, job)]);
    let unknown_generation = wake.capacity_generation();
    wait(database.confirm_job_stopped(job, crate::persistence::StopProof::Proof)).unwrap();
    assert!(wake.capacity_generation() > unknown_generation);
    assert!(wait(database.occupancy()).unwrap().is_empty());
    let released = (wake.dirty_generation(), wake.capacity_generation());
    wait(database.recover_storage()).unwrap();
    assert!(wake.dirty_generation() > released.0 && wake.capacity_generation() > released.1);
    worker.shutdown().unwrap();
}

#[test]
fn stop_confirmation_distinguishes_unsettled_and_completed_jobs() {
    let (_root, directory) = data_directory("ocg-stage-b-worker", "stop-outcome");
    let worker = DatabaseWorker::start(&directory).unwrap();
    let database = worker.handle();
    let (pool, llm) = provision(&database);
    let job = pending(&database, llm);
    wait(database.claim_execution(ClaimRequest {
        job,
        allowed_pools: BTreeSet::from([pool]),
    }))
    .unwrap()
    .unwrap();
    assert!(matches!(
        wait(database.confirm_job_stopped(job, crate::persistence::StopProof::Proof)),
        Err(DatabaseError::Rejected(Violation::StopNeedsOutcome(id))) if id == job
    ));
    wait(database.finalize_job(
        job,
        JobFinalization::Completed(FinalResult {
            summary: Some("done".into()),
            code: None,
            goal_reached: None,
            value: None,
        }),
    ))
    .unwrap();
    assert!(matches!(
        wait(database.confirm_job_stopped(job, crate::persistence::StopProof::Proof)),
        Err(DatabaseError::Rejected(Violation::StopNeedsUnknownOutcome(id))) if id == job
    ));
    worker.shutdown().unwrap();
}

#[test]
fn an_unknown_job_without_a_spawning_call_frees_its_slot_at_once() {
    let (_root, directory) = data_directory("ocg-stage-b-worker", "no-spawn");
    let mut worker = DatabaseWorker::start(&directory).unwrap();
    let wake = worker.take_scheduler_wake().unwrap();
    let database = worker.handle();
    let (pool, llm) = provision(&database);
    let job = pending(&database, llm);
    wait(database.claim_execution(ClaimRequest {
        job,
        allowed_pools: BTreeSet::from([pool]),
    }))
    .unwrap()
    .unwrap();
    let held = wake.capacity_generation();
    wait(database.finalize_job(job, JobFinalization::Unknown)).unwrap();
    // No admitted call can have started a process, so the stop is confirmed
    // by the same transaction and the slot is free.
    let row = wait(database.job(job)).unwrap().unwrap();
    assert!(row.unknown_at.is_some() && row.stop_confirmed_at.is_some());
    assert!(wait(database.occupancy()).unwrap().is_empty());
    assert!(wake.capacity_generation() > held);
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
fn uncertain_claim_flush_never_grants_and_reopening_settles_a_committed_claim() {
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
        description: None,
        root: "/ocg-test/flush-group".into(),
        commands: Vec::new(),
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
    // The claim's natural key says whether it committed. The project command
    // writes no key that could be read.
    let status = wait(database.reconcile_operation(operation)).unwrap();
    let row = wait(database.job(job)).unwrap().unwrap();
    match status {
        OperationStatus::Committed => {
            // The claim committed, so the Job started in an opening that is
            // gone. Its outcome is unknown. No admitted call can have started
            // a process, so its stop is confirmed and its slot is free.
            assert!(row.unknown_at.is_some() && row.stop_confirmed_at.is_some());
            assert!(wait(database.occupancy()).unwrap().is_empty());
            assert!(wait(database.unresolved_executions()).unwrap().is_empty());
        }
        OperationStatus::NotCommitted => {
            assert!(row.started_at.is_none());
            assert!(wait(database.occupancy()).unwrap().is_empty());
        }
        status => panic!("recovery must resolve operation, got {status:?}"),
    }
    worker.shutdown().unwrap();
}

#[test]
fn process_reopen_quarantines_an_unfinished_spawning_call_without_rerunning() {
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
    leave_a_spawning_call(&database, job);
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
    // The earlier opening is gone, but its call may have left a process, so
    // the slot stays occupied and its capacity is quarantined.
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

#[test]
fn process_reopen_confirms_the_stop_of_a_claim_without_a_spawning_call() {
    let (_root, directory) = data_directory("ocg-stage-b-worker", "restart-clean");
    let worker = DatabaseWorker::start(&directory).unwrap();
    let database = worker.handle();
    let (pool, llm) = provision(&database);
    let job = pending(&database, llm);
    wait(database.claim_execution(ClaimRequest {
        job,
        allowed_pools: BTreeSet::from([pool]),
    }))
    .unwrap()
    .unwrap();
    worker.shutdown().unwrap();
    let worker = DatabaseWorker::start(&directory).unwrap();
    let database = worker.handle();
    assert!(database.abandoned_at_open().contains(&job));
    let row = wait(database.job(job)).unwrap().unwrap();
    assert!(row.unknown_at.is_some() && row.stop_confirmed_at.is_some());
    assert!(wait(database.occupancy()).unwrap().is_empty());
    assert!(wait(database.unresolved_executions()).unwrap().is_empty());
    worker.shutdown().unwrap();
}

/// One local Job in a Pool, started the way the shipped runtime starts work,
/// with a spawning call that never got an outcome.
fn local_job_with_a_spawning_call(
    database: &DatabaseHandle,
    pool: ExecutionPoolId,
    llm: ProfileId,
) -> JobId {
    let job = wait(database.create_claimed_fixture(
        NewJob {
            process_grant: None,
            name: None,
            eligible_at: None,
            parent: database.genesis(),
            project: None,
            input: JobInputDraft::default(),
            configuration: Some(ExecutionConfiguration {
                agent_profile: None,
                llm_profile: llm,
            }),
            previous: None,
        },
        pool.into(),
    ))
    .unwrap();
    leave_a_spawning_call(database, job);
    job
}

/// Repeated unclean exits: every start leaves one Job behind with an
/// unfinished spawning call when the process is killed, so each cycle adds one
/// more quarantined slot until the global domain is past its limit and can
/// never be drained.
///
/// The database must keep starting: a condition that is permanent by design
/// must not become one no operator can get out of. This is that arithmetic,
/// driven through the real worker start and open reconcile.
#[test]
fn repeated_unclean_exits_saturate_the_global_domain_without_refusing_to_start() {
    let (_root, directory) = data_directory("ocg-stage-b-worker", "unclean-exits");
    // Fill the global limit; the next claim must remain pending.
    let cycles = usize::from(GLOBAL_EXECUTION_LIMIT);
    let mut abandoned = Vec::new();

    for cycle in 1..=cycles {
        let worker = DatabaseWorker::start(&directory).unwrap();
        let database = worker.handle();
        let (pool, llm) = provision(&database);
        let job = local_job_with_a_spawning_call(&database, pool, llm);
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
        // accounting below turns on: the claim survives into the next
        // opening. A shutdown reproduces that inside one process; the signal
        // that actually causes it is the subject of the process tests.
        worker.shutdown().unwrap();
        let worker = DatabaseWorker::start(&directory)
            .unwrap_or_else(|error| panic!("cycle {cycle} of {cycles}: startup failed: {error:?}"));
        worker.shutdown().unwrap();
    }

    // At the limit, every slot is quarantined and held.
    let worker = DatabaseWorker::start(&directory).unwrap();
    let database = worker.handle();
    let held = wait(database.unresolved_executions()).unwrap();
    assert_eq!(
        held.len(),
        cycles,
        "one record per unclean exit, none merged"
    );
    assert!(
        held.iter().all(|(_, record)| record.pool.is_some()),
        "each is charged to the Pool it was claimed into"
    );
    assert_eq!(
        wait(database.occupancy()).unwrap().len(),
        cycles,
        "and each holds its slot"
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
        "the global domain is held at its limit"
    );
    assert!(
        wait(database.claim_batch()).unwrap().is_empty(),
        "and the claim batch grants nothing"
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
        assert_eq!(wait(database.occupancy()).unwrap().len(), cycles);
        worker.shutdown().unwrap();
    }
}

// ---- stop certainty: real processes, real signals, real survival -------------

use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// The name of the environment variable that turns this test into the
/// supervisor of [`a_sigkilled_supervisor_quarantines_the_capacity_its_surviving_child_uses`].
const SUPERVISOR: &str = "OCG_STOP_CERTAINTY_SUPERVISOR";

/// Whether a process is alive, as the OS reports it. `kill -0` is the kernel's
/// own answer, not this program's reading of a status it wrote earlier.
fn alive(pid: u32) -> bool {
    Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

/// The parent the OS currently records for `pid`, read from `ps` rather than
/// remembered, so "reparented after its supervisor died" is observed and not
/// assumed.
fn parent_of(pid: u32) -> Option<u32> {
    let output = Command::new("ps")
        .args(["-o", "ppid=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    String::from_utf8(output.stdout).ok()?.trim().parse().ok()
}

fn terminate(pid: u32) -> bool {
    Command::new("kill")
        .arg("-KILL")
        .arg(pid.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn open_root(root: &std::path::Path) -> (DatabaseWorker, DatabaseHandle) {
    let directory = crate::data_directory::DataDirectory::bootstrap(root.as_os_str().to_owned())
        .expect("data directory");
    let policy = crate::system_policy::SystemPolicy::load_for_startup(&directory).expect("policy");
    let worker = DatabaseWorker::start_with_policy(&directory, policy).expect("open the database");
    let handle = worker.handle();
    (worker, handle)
}

/// The supervisor half of the probe. Runs as its own process so that the
/// descendants it spawns are its own descendants in the OS process tree, and
/// so that SIGKILL can be delivered to it without this test's own process
/// being involved.
///
/// It is a harness rather than the shipped executable because the shipped
/// executable exposes no way to be told to spawn a child on demand. What it
/// does use is the real database: the same `DatabaseWorker`, the same claim
/// and tool-decision path, the same durable records, on the same file the rest
/// of the probe reads. Only the supervisor's identity differs.
#[test]
fn a_supervisor_that_claims_a_slot_and_leaves_a_running_child() {
    let Ok(root) = std::env::var(SUPERVISOR) else {
        return;
    };
    let (_worker, handle) = open_root(std::path::Path::new(&root));
    let llm = ProfileId::new(
        std::env::var("OCG_STOP_CERTAINTY_LLM")
            .expect("Model")
            .parse()
            .expect("Model ID"),
    );
    let job = pending(&handle, llm);
    let pool = ExecutionPoolId::new(
        std::env::var("OCG_STOP_CERTAINTY_POOL")
            .expect("the Pool to claim")
            .parse()
            .expect("a Pool number"),
    );
    wait(handle.claim_execution(ClaimRequest {
        job,
        allowed_pools: BTreeSet::from([pool]),
    }))
    .expect("claim")
    .expect("the single slot is free before the supervisor takes it");
    // The admission of a command is durable before the command runs.
    leave_a_spawning_call(&handle, job);

    // The external work: a real child of this process, which nothing in OCG
    // tracks and no signal from here reaches.
    let mut child = Command::new("sleep")
        .arg("600")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("start the external child");
    println!("OCG_SUPERVISOR_READY job={job} child={}", child.id());
    // Held open so the worker keeps running and keeps owning the claim, as a
    // supervisor does while its work is in flight.
    loop {
        std::thread::sleep(Duration::from_millis(50));
        if child.try_wait().expect("poll the child").is_some() {
            return;
        }
    }
}

/// The whole stop-safety probe, against real processes and real signals.
///
/// A supervisor claims a Pool's only slot, admits a command and leaves a real
/// child running. It is then SIGKILLed, which is the one signal that cannot be
/// caught, so nothing in it gets to clean up or say goodbye. The child
/// outlives it — proved by the kernel, not by assumption — and the capacity
/// its claim held must not come back into admission, however many times OCG
/// restarts afterwards.
#[test]
fn a_sigkilled_supervisor_quarantines_the_capacity_its_surviving_child_uses() {
    let root = std::env::temp_dir().join(format!("ocg-stop-certainty-{}", std::process::id()));
    std::fs::remove_dir_all(&root).ok();
    std::fs::create_dir_all(&root).expect("create the data directory");

    let (worker, handle) = open_root(&root);
    let (_unused, llm) = provision(&handle);
    let held = narrow_pool(&handle, "held");
    let independent = narrow_pool(&handle, "independent");
    worker.shutdown().expect("close the database");

    // ---- a supervisor that holds the slot and leaves a child running --------
    let mut supervisor = Command::new(std::env::current_exe().expect("this test binary"))
        .env(SUPERVISOR, &root)
        .env("OCG_STOP_CERTAINTY_LLM", llm.get().to_string())
        .env("OCG_STOP_CERTAINTY_POOL", held.get().to_string())
        .args([
            "--exact",
            "database::stage_b_tests::a_supervisor_that_claims_a_slot_and_leaves_a_running_child",
            "--nocapture",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("start the supervisor");
    let supervisor_pid = supervisor.id();
    let stdout = supervisor.stdout.take().expect("stdout");
    let (lines, received) = flume::unbounded();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            lines.send(line).ok();
        }
    });
    let deadline = Instant::now() + WAIT;
    let mut child_pid = None;
    let mut supervisor_job = None;
    let mut seen = Vec::new();
    while child_pid.is_none() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let Ok(line) = received.recv_timeout(remaining) else {
            supervisor.kill().ok();
            supervisor.wait().ok();
            panic!("the supervisor did not report its child in time; it said {seen:?}");
        };
        seen.push(line.clone());
        if let Some((_, rest)) = line.split_once("OCG_SUPERVISOR_READY") {
            supervisor_job = rest
                .split_whitespace()
                .find_map(|field| field.strip_prefix("job="))
                .and_then(|value| value.parse::<JobId>().ok());
            child_pid = rest
                .split_whitespace()
                .find_map(|field| field.strip_prefix("child="))
                .and_then(|value| value.parse().ok());
        }
    }
    let job = supervisor_job.expect("the supervisor named its Job");
    let child_pid: u32 = child_pid.expect("the supervisor named its child");
    assert!(
        alive(child_pid),
        "the external child is running under its supervisor"
    );
    assert_eq!(
        parent_of(child_pid),
        Some(supervisor_pid),
        "the child is a descendant of the supervisor in the OS process tree"
    );

    // ---- SIGKILL: uncatchable, so nothing in the supervisor can report or
    // unwind, and no descendant of it is signalled -----------------------
    supervisor.kill().expect("SIGKILL the supervisor");
    supervisor.wait().expect("reap the supervisor");
    reader.join().expect("supervisor stdout reader");

    assert!(!alive(supervisor_pid), "the supervisor is gone");
    assert!(
        alive(child_pid),
        "the external child survived the supervisor's SIGKILL, which is the \
         whole premise: a supervisor's death is not its descendants' termination"
    );
    assert_ne!(
        parent_of(child_pid),
        Some(supervisor_pid),
        "and it has been reparented away from the dead supervisor"
    );

    // ---- restart: unknown, unconfirmed, occupied, quarantined --------------
    let (worker, handle) = open_root(&root);
    assert_eq!(handle.abandoned_at_open(), [job]);
    let record = wait(handle.job(job)).expect("read").expect("Job exists");
    assert!(
        record.unknown_at.is_some(),
        "the outcome is unknown, not turned into a result"
    );
    assert!(
        record.stop_confirmed_at.is_none(),
        "the restart does not confirm that the external work stopped: its \
         command was admitted and never got an outcome"
    );
    assert_eq!(record.completed_at, None);
    assert_eq!(record.failed_at, None);
    assert_eq!(record.cancelled_at, None);
    assert_eq!(
        wait(handle.occupancy()).expect("occupancy"),
        vec![(held, job)],
        "the occupancy the claim took is retained, not released"
    );
    let quarantined = wait(handle.unresolved_executions()).expect("quarantine");
    assert_eq!(quarantined.len(), 1);
    assert_eq!(quarantined[0].0, job);
    assert_eq!(
        quarantined[0].1.pool,
        Some(held),
        "the quarantine names the actual domain the child may still occupy"
    );

    // ---- the quarantined domain refuses; an independent one admits --------
    let spare = pending(&handle, llm);
    assert_eq!(
        wait(handle.claim_execution(ClaimRequest {
            job: spare,
            allowed_pools: BTreeSet::from([held])
        }))
        .expect("claim"),
        Err(ClaimRefusal::NoCapacity(CapacityScope::Pools(
            BTreeSet::from([held])
        ))),
        "the domain the surviving child may still occupy is not handed out"
    );
    assert!(
        wait(handle.claim_execution(ClaimRequest {
            job: spare,
            allowed_pools: BTreeSet::from([independent])
        }))
        .expect("claim")
        .is_ok(),
        "a Pool whose safety was established still admits"
    );
    let mut occupied = wait(handle.occupancy()).expect("occupancy");
    occupied.sort();
    assert_eq!(
        occupied,
        vec![(held, job), (independent, spare)],
        "and the one admission counted is the one that happened: no double \
         counting, no premature release"
    );
    // Nothing alive can turn the unknown outcome into a result either.
    assert!(
        wait(handle.finalize_job(
            job,
            JobFinalization::Completed(FinalResult {
                summary: None,
                code: None,
                goal_reached: None,
                value: None,
            }),
        ))
        .is_err(),
        "an unresolved execution is not finalized over"
    );
    assert_eq!(
        wait(handle.job(job)).expect("read"),
        Some(record.clone()),
        "and the unknown-unconfirmed state is still what is stored"
    );

    // ---- further restarts do not weaken it ---------------------------------
    worker.shutdown().expect("close the database");
    for round in 1..=2 {
        let (worker, handle) = open_root(&root);
        assert_eq!(
            wait(handle.job(job)).expect("read"),
            Some(record.clone()),
            "round {round}: the unknown-unconfirmed state is unchanged"
        );
        let mut occupied = wait(handle.occupancy()).expect("occupancy");
        occupied.sort();
        assert!(
            occupied.contains(&(held, job)),
            "round {round}: the occupancy is still held"
        );
        assert!(
            wait(handle.unresolved_executions())
                .expect("quarantine")
                .iter()
                .any(|(quarantined, _)| *quarantined == job),
            "round {round}: the killed supervisor's quarantine is not lost"
        );
        worker.shutdown().expect("close the database");
    }

    // ---- cleanup: nothing this probe started is left running ---------------
    assert!(terminate(child_pid), "terminate the probe's external child");
    let deadline = Instant::now() + WAIT;
    while alive(child_pid) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(!alive(child_pid), "the probe's external child is gone");
    std::fs::remove_dir_all(&root).ok();
}
