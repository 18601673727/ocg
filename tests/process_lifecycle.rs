//! The normal executable, started and stopped as real processes in sequence.
//! It checks aggregation hosting only: no credential is set and no Provider
//! is called, so it says nothing about real HTTPS acceptance.

use std::collections::BTreeSet;
use std::future::Future;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

use ocg::data_directory::DataDirectory;
use ocg::database::{DatabaseHandle, DatabaseWorker};
use ocg::openai_compatible::{ChatFinishReason, NormalizedUsage};
use ocg::persistence::{
    AvailabilityObservation, CapabilitySet, CapacityScope, ClaimRefusal, ClaimRequest,
    ExecutionConfiguration, ExecutionMode, ExecutionOperation, ExecutionPoolConfig,
    ExecutionPoolId, FinalResult, JobFinalization, JobId, JobInputDraft, LlmConfig, LlmParameters,
    LlmProviderId, LlmTransport, Measurements, ModelCapability, NewAvailabilityStat,
    NewExecutionPool, NewJob, NewLlmProvider, NewMetrics, NewProfile, NewProfileConfig, ProfileId,
    RequestNumber, RequestRef, Timestamp,
};
use ocg::system_policy::SystemPolicy;
use ocg::token_usage::{TokenAnnotation, TokenUsageState, UsageFinality};

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
        assert!(
            Instant::now() < deadline,
            "a database call did not resolve in time"
        );
        std::thread::park_timeout(Duration::from_millis(50));
    }
}

fn open(root: &Path) -> (DatabaseWorker, DatabaseHandle) {
    let directory = DataDirectory::bootstrap(root.as_os_str().to_owned()).expect("data directory");
    let policy = SystemPolicy::load_for_startup(&directory).expect("system policy");
    let worker = DatabaseWorker::start_with_policy(&directory, policy).expect("open the database");
    let handle = worker.handle();
    (worker, handle)
}

/// One LLM Job with one availability observation and one eligible, final
/// Token fact, none of them consumed.
fn seed(handle: &DatabaseHandle, provider: LlmProviderId, llm: ProfileId, micros: i64) -> JobId {
    let input = wait(handle.save_job_input(None, JobInputDraft::default())).expect("input");
    let job = wait(handle.create_job_with_input(
        NewJob {
            name: Some("request".into()),
            execution_mode: ExecutionMode::Once,
            eligible_at: None,
            scheduled_at: None,
            next_due_at: None,
            next_iteration_no: None,
            iteration_no: None,
            parent: handle.genesis(),
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
    .expect("create the Job");
    let _ = provider;
    let at = Timestamp::from_unix_micros(micros);
    wait(handle.record_availability_stat(NewAvailabilityStat {
        job,
        request: RequestNumber::FIRST,
        observed_at: at,
        observation: AvailabilityObservation {
            status_code: Some(200),
            finish_reason: Some(ChatFinishReason::Stop),
            raw_finish_reason: Some("stop".into()),
            failure: None,
        },
    }))
    .expect("record AvailabilityStat");
    wait(handle.record_metrics(NewMetrics {
        job,
        request: RequestNumber::FIRST,
        observed_at: at,
        measurements: Measurements {
            usage: NormalizedUsage {
                input_tokens: Some(10),
                output_tokens: Some(20),
                ..NormalizedUsage::default()
            },
            ..Measurements::default()
        },
        token: Some(TokenAnnotation::eligible(UsageFinality::Final)),
    }))
    .expect("record Metrics");
    job
}

/// Starts the executable on `root`, waits until its service reports running,
/// stops it with a graceful termination signal and returns the service Job
/// it named and its exit status.
fn run_once(root: &Path) -> (JobId, ExitStatus) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_ocg"))
        .env("OCG_DATA_DIR", root)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start the executable");
    let (lines, received) = flume::unbounded();
    let stderr = child.stderr.take().expect("stderr");
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            lines.send(line).ok();
        }
    });
    let deadline = Instant::now() + WAIT;
    let mut seen = Vec::new();
    let job = loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let line = received.recv_timeout(remaining).unwrap_or_else(|_| {
            panic!("the executable did not report its service in time; it said {seen:?}")
        });
        seen.push(line.clone());
        if let Some(rest) = line.strip_prefix("OCG aggregation service running as job:") {
            break JobId::new(rest.trim().parse().expect("a Job number"));
        }
    };
    assert!(
        child.try_wait().expect("poll the child").is_none(),
        "the process stays alive until it is told to stop"
    );
    let signalled = Command::new("kill")
        .arg("-TERM")
        .arg(child.id().to_string())
        .status()
        .expect("send SIGTERM");
    assert!(signalled.success());
    let status = loop {
        if let Some(status) = child.try_wait().expect("poll the child") {
            break status;
        }
        if Instant::now() > deadline {
            child.kill().ok();
            panic!("the executable did not exit after SIGTERM");
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    reader.join().expect("stderr reader");
    (job, status)
}

#[test]
fn each_process_run_hosts_aggregation_and_ends_its_own_service_job() {
    let root = std::env::temp_dir().join(format!("ocg-process-{}", std::process::id()));
    std::fs::remove_dir_all(&root).ok();
    std::fs::create_dir_all(&root).expect("create the data directory");

    let (worker, handle) = open(&root);
    let provider = wait(handle.create_llm_provider(NewLlmProvider {
        name: "local".into(),
        transport: LlmTransport::OpenAiChatCompletions,
        endpoint: Some("https://local.invalid/v1".into()),
        credential_ref: None,
    }))
    .expect("create the Provider");
    let llm = wait(handle.create_profile(NewProfile {
        name: "model-a".into(),
        config: NewProfileConfig::Llm {
            provider,
            config: LlmConfig {
                model: "model-a".into(),
                parameters: LlmParameters::default(),
                declared_capabilities: BTreeSet::from([ModelCapability::Chat]),
            },
        },
    }))
    .expect("create the Profile");
    let first_job = seed(&handle, provider, llm, 120_000_000);
    worker.shutdown().expect("close the database");

    // Run one: the raw records are consumed by the process itself.
    let (first_service, status) = run_once(&root);
    assert!(
        status.success(),
        "a graceful stop exits successfully: {status:?}"
    );
    let (worker, handle) = open(&root);
    let record = wait(handle.job(first_service))
        .expect("read")
        .expect("Job exists");
    assert!(
        record.completed_at.is_some(),
        "the run ended its own service Job"
    );
    assert_eq!(record.parent, handle.genesis());
    assert_eq!(record.configuration, None);
    let first_progress = (
        wait(handle.token_progress()).expect("progress"),
        wait(handle.availability_progress()).expect("progress"),
    );
    assert!(first_progress.0.consumed_through.is_some());
    assert!(first_progress.1.consumed_through.is_some());
    let sample = wait(handle.token_request(RequestRef {
        job: first_job,
        request: RequestNumber::FIRST,
    }))
    .expect("read")
    .value
    .and_then(|request| request.sample)
    .expect("the request was aggregated");
    assert_eq!(sample.usage, TokenUsageState::Complete);
    assert_eq!(sample.total, Some(30));

    // Run two: only the new raw records are processed, under a new Job, and
    // the first run's history is untouched.
    let second_job = seed(&handle, provider, llm, 240_000_000);
    worker.shutdown().expect("close the database");
    let (second_service, status) = run_once(&root);
    assert!(status.success());
    assert_ne!(
        second_service, first_service,
        "each run has its own service Job"
    );
    let (worker, handle) = open(&root);
    let second = wait(handle.job(second_service))
        .expect("read")
        .expect("Job exists");
    let first = wait(handle.job(first_service))
        .expect("read")
        .expect("Job exists");
    assert!(second.completed_at.is_some());
    assert_eq!(first, record, "the earlier run's Job is untouched");
    let progress = wait(handle.token_progress()).expect("progress");
    assert!(progress.consumed_through > first_progress.0.consumed_through);
    for job in [first_job, second_job] {
        assert!(
            wait(handle.token_request(RequestRef {
                job,
                request: RequestNumber::FIRST
            }))
            .expect("read")
            .value
            .is_some(),
            "{job} is aggregated"
        );
    }
    worker.shutdown().expect("close the database");
    std::fs::remove_dir_all(&root).ok();
}

/// A process killed outright cannot end its service Job. The next start of the
/// database does not end it either: it takes an unknown outcome, its stop stays
/// unconfirmed, and the capacity it held stays quarantined.
#[test]
fn a_killed_process_leaves_its_service_job_quarantined_by_the_next_start() {
    let root = std::env::temp_dir().join(format!("ocg-process-killed-{}", std::process::id()));
    std::fs::remove_dir_all(&root).ok();
    std::fs::create_dir_all(&root).expect("create the data directory");

    let mut child = Command::new(env!("CARGO_BIN_EXE_ocg"))
        .env("OCG_DATA_DIR", &root)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start the executable");
    let (lines, received) = flume::unbounded();
    let stderr = child.stderr.take().expect("stderr");
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            lines.send(line).ok();
        }
    });
    let deadline = Instant::now() + WAIT;
    let mut seen = Vec::new();
    let service = loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let Ok(line) = received.recv_timeout(remaining) else {
            child.kill().ok();
            child.wait().ok();
            panic!("the executable did not report its service in time; it said {seen:?}");
        };
        seen.push(line.clone());
        if let Some(rest) = line.strip_prefix("OCG aggregation service running as job:") {
            break JobId::new(rest.trim().parse().expect("a Job number"));
        }
    };
    // SIGKILL on Unix, TerminateProcess elsewhere: nothing runs on the way out.
    child.kill().expect("kill the executable");
    child.wait().expect("reap the executable");
    reader.join().expect("stderr reader");

    let (worker, handle) = open(&root);
    assert_eq!(handle.abandoned_at_open(), [service]);
    let record = wait(handle.job(service))
        .expect("read")
        .expect("Job exists");
    assert!(record.started_at.is_some());
    assert!(
        record.unknown_at.is_some() && record.stop_confirmed_at.is_none(),
        "the killed run's service Job is unknown and not confirmed stopped: \
         the supervisor's death is not its descendants' termination"
    );
    assert!(record.completed_at.is_none() && record.failed_at.is_none());
    worker.shutdown().expect("close the database");

    let (worker, handle) = open(&root);
    assert!(handle.abandoned_at_open().is_empty());
    assert_eq!(
        wait(handle.job(service)).expect("read"),
        Some(record),
        "quarantined work is not settled again"
    );
    worker.shutdown().expect("close the database");
    std::fs::remove_dir_all(&root).ok();
}

// ---- stop certainty: real processes, real signals, real survival -------------

/// The name of the environment variable that turns this test binary into the
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

fn signal(pid: u32) -> bool {
    Command::new("kill")
        .arg("-KILL")
        .arg(pid.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

/// A Pool whose single slot makes it observable whether the capacity is free.
fn narrow_pool(handle: &DatabaseHandle, name: &str) -> ExecutionPoolId {
    let profile = wait(handle.create_profile(NewProfile {
        name: name.into(),
        config: NewProfileConfig::ExecutionPool(ExecutionPoolConfig {
            concurrency_limit: std::num::NonZeroU32::new(1).unwrap(),
            capabilities: CapabilitySet {
                operations: BTreeSet::from([ExecutionOperation::LlmRequest]),
                llm_protocols: BTreeSet::from([LlmTransport::OpenAiChatCompletions]),
            },
        }),
    }))
    .expect("create the Pool Profile");
    wait(handle.create_execution_pool(NewExecutionPool {
        name: name.into(),
        profile,
        enabled: true,
    }))
    .expect("create the Pool")
}

/// One pending Job that a Pool claim may take.
fn pending_job(handle: &DatabaseHandle, llm: ProfileId) -> JobId {
    let input = wait(handle.save_job_input(None, JobInputDraft::default())).expect("input");
    wait(handle.create_job_with_input(
        NewJob {
            name: None,
            execution_mode: ExecutionMode::Once,
            eligible_at: None,
            scheduled_at: None,
            next_due_at: None,
            next_iteration_no: None,
            iteration_no: None,
            parent: handle.genesis(),
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
    .expect("create the Job")
}

/// The supervisor half of the probe. Runs as its own process so that the
/// descendants it spawns are its own descendants in the OS process tree, and
/// so that SIGKILL can be delivered to it without this test's own process
/// being involved.
///
/// It is a harness rather than the shipped executable because the shipped
/// executable exposes no way to be told to spawn a child on demand. What it
/// does use is the real database: the same `DatabaseWorker`, the same claim
/// path, the same durable records, on the same file the rest of the probe
/// reads. Only the supervisor's identity differs.
#[test]
fn a_supervisor_that_claims_a_slot_and_leaves_a_running_child() {
    let Ok(root) = std::env::var(SUPERVISOR) else {
        return;
    };
    let (_worker, handle) = open(Path::new(&root));
    let job: JobId = JobId::new(
        std::env::var("OCG_STOP_CERTAINTY_JOB")
            .expect("the Job to claim")
            .parse()
            .expect("a Job number"),
    );
    let pool: ExecutionPoolId = ExecutionPoolId::new(
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
/// A supervisor claims a Pool's only slot and leaves a real child running. It
/// is then SIGKILLed, which is the one signal that cannot be caught, so nothing
/// in it gets to clean up or say goodbye. The child outlives it — proved by the
/// kernel, not by assumption — and the capacity its claim held must not come
/// back into admission, however many times OCG restarts afterwards.
#[test]
fn a_sigkilled_supervisor_quarantines_the_capacity_its_surviving_child_uses() {
    let root = std::env::temp_dir().join(format!("ocg-stop-certainty-{}", std::process::id()));
    std::fs::remove_dir_all(&root).ok();
    std::fs::create_dir_all(&root).expect("create the data directory");

    let (worker, handle) = open(&root);
    let provider = wait(handle.create_llm_provider(NewLlmProvider {
        name: "local".into(),
        transport: LlmTransport::OpenAiChatCompletions,
        endpoint: None,
        credential_ref: None,
    }))
    .expect("create the Provider");
    let llm = wait(handle.create_profile(NewProfile {
        name: "model-a".into(),
        config: NewProfileConfig::Llm {
            provider,
            config: LlmConfig {
                model: "model-a".into(),
                parameters: LlmParameters::default(),
                declared_capabilities: BTreeSet::from([ModelCapability::Chat]),
            },
        },
    }))
    .expect("create the Profile");
    let held = narrow_pool(&handle, "held");
    let independent = narrow_pool(&handle, "independent");
    let job = pending_job(&handle, llm);
    let spare = pending_job(&handle, llm);
    worker.shutdown().expect("close the database");

    // ---- a supervisor that holds the slot and leaves a child running --------
    let mut supervisor = Command::new(std::env::current_exe().expect("this test binary"))
        .env(SUPERVISOR, &root)
        .env("OCG_STOP_CERTAINTY_JOB", job.get().to_string())
        .env("OCG_STOP_CERTAINTY_POOL", held.get().to_string())
        .args([
            "--exact",
            "a_supervisor_that_claims_a_slot_and_leaves_a_running_child",
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
    let mut seen = Vec::new();
    while child_pid.is_none() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let Ok(line) = received.recv_timeout(remaining) else {
            supervisor.kill().ok();
            supervisor.wait().ok();
            panic!("the supervisor did not report its child in time; it said {seen:?}");
        };
        seen.push(line.clone());
        if let Some(rest) = line.trim().strip_prefix("OCG_SUPERVISOR_READY") {
            child_pid = rest
                .split_whitespace()
                .find_map(|field| field.strip_prefix("child="))
                .and_then(|value| value.parse().ok());
        }
    }
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
    assert_eq!(supervisor.id(), supervisor_pid);
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
    let (worker, handle) = open(&root);
    assert_eq!(handle.abandoned_at_open(), [job]);
    let record = wait(handle.job(job)).expect("read").expect("Job exists");
    assert!(
        record.unknown_at.is_some(),
        "the outcome is unknown, not turned into a result"
    );
    assert!(
        record.stop_confirmed_at.is_none(),
        "the restart does not confirm that the external work stopped"
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
    let retained = (record, wait(handle.occupancy()).expect("occupancy"));
    worker.shutdown().expect("close the database");
    for round in 1..=2 {
        let (worker, handle) = open(&root);
        assert!(
            !handle.abandoned_at_open().contains(&job),
            "round {round}: the killed supervisor's Job was quarantined once \
             and is not reworked; only this opening's own claim, which it \
             left when it shut down cleanly, is new"
        );
        assert_eq!(
            wait(handle.job(job)).expect("read"),
            Some(retained.0.clone()),
            "round {round}: the unknown-unconfirmed state is unchanged"
        );
        assert_eq!(
            wait(handle.occupancy()).expect("occupancy"),
            retained.1,
            "round {round}: the occupancy is still held"
        );
        assert_eq!(
            wait(handle.unresolved_executions())
                .expect("quarantine")
                .len(),
            2,
            "round {round}: the killed supervisor's quarantine is not lost, \
             and this opening's own abandoned claim is quarantined alongside it"
        );
        worker.shutdown().expect("close the database");
    }

    // ---- cleanup: nothing this probe started is left running ---------------
    assert!(signal(child_pid), "terminate the probe's external child");
    let deadline = Instant::now() + WAIT;
    while alive(child_pid) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(!alive(child_pid), "the probe's external child is gone");
    std::fs::remove_dir_all(&root).ok();
}
