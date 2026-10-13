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
    AvailabilityObservation, ExecutionConfiguration, ExecutionMode, JobId, JobInputDraft,
    LlmConfig, LlmParameters, LlmProviderId, LlmTransport, Measurements, ModelCapability,
    NewAvailabilityStat, NewJob, NewLlmProvider, NewMetrics, NewProfile, NewProfileConfig,
    ProfileId, Timestamp,
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
    let sample = wait(handle.token_request(first_job))
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
            wait(handle.token_request(job))
                .expect("read")
                .value
                .is_some(),
            "{job} is aggregated"
        );
    }
    worker.shutdown().expect("close the database");
    std::fs::remove_dir_all(&root).ok();
}
