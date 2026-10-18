//! Two-process acceptance of a real OpenAI-compatible HTTPS request through an
//! OCG Job: one process executes the Job and exits, a different process then
//! reopens the same redb file and reads that Job's complete execution result.
//!
//! The live test is `#[ignore]`d: an ordinary `cargo test` never contacts a
//! Provider. Run it explicitly, with the target named by the environment and
//! the credential named by reference only:
//!
//! ```text
//! OCG_LIVE_ENDPOINT=https://host/v1/chat/completions \
//! OCG_LIVE_MODEL=<model> \
//! OCG_LIVE_CREDENTIAL_REF=<NAME_OF_ENV_VAR_HOLDING_THE_KEY> \
//! <NAME_OF_ENV_VAR_HOLDING_THE_KEY>=... \
//! cargo test --test live_https_acceptance -- --ignored --exact live_https_job_survives_process_exit
//! ```
//!
//! It issues exactly one Provider request and never retries. The same
//! orchestration also runs offline, against a clearly labelled fixture Job that
//! is settled through the ordinary database APIs instead of a Provider; that
//! checks the process choreography and the read-back logic and says nothing
//! about HTTPS.
//!
//! The child processes are this test binary run again with `--ignored --exact`
//! on the role tests below; the application itself spawns no process.

use std::collections::BTreeSet;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use ocg::data_directory::DataDirectory;
use ocg::database::{DatabaseHandle, DatabaseWorker};
use ocg::openai_compatible::NormalizedUsage;
use ocg::orchestration::execution_runtime::{
    ExecutionRuntime, ExecutionRuntimeError, ProviderExecutionPlan,
};
use ocg::persistence::{
    ConfigBody, ConnectionConfig, ExecutionConfiguration, FinalResult, IntermediateOutputKind,
    JobFinalization, JobId, JobInputDraft, LlmParameters, LlmTransport, Measurements,
    ModelCapability, ModelConfig, NewAvailabilityStat, NewConfig, NewExecutor, NewJob,
    NewJobOutput, NewMetrics, OutputKind, RequestNumber, Timestamp,
};
use ocg::provider_execution::{EnvironmentCredentialResolver, ProviderExecutionConfig};
use ocg::system_policy::SystemPolicy;
use ocg::token_usage::{TokenAnnotation, TokenRequestSample, UsageFinality};

const ROLE_VARIABLE: &str = "OCG_ACCEPTANCE_ROLE";
const MODE_VARIABLE: &str = "OCG_ACCEPTANCE_MODE";
const DIRECTORY_VARIABLE: &str = "OCG_ACCEPTANCE_DIRECTORY";
const MANIFEST_FILE: &str = "manifest.json";
const READER_FILE: &str = "reader.json";

// ---- bounded waiting ----------------------------------------------------------------

struct Unpark(std::thread::Thread);

impl Wake for Unpark {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

/// Drives a future on this thread, failing if it does not resolve in time.
fn wait_for<F: Future>(future: F, bound: Duration) -> F::Output {
    let mut future = std::pin::pin!(future);
    let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
    let deadline = Instant::now() + bound;
    loop {
        if let Poll::Ready(value) = future.as_mut().poll(&mut Context::from_waker(&waker)) {
            return value;
        }
        assert!(
            Instant::now() < deadline,
            "a call did not resolve within {bound:?}"
        );
        std::thread::park_timeout(Duration::from_millis(50));
    }
}

fn wait<F: Future>(future: F) -> F::Output {
    wait_for(future, Duration::from_secs(30))
}

// ---- what the live run needs ----------------------------------------------------------

/// The authorized target and the name of the environment variable holding its
/// credential. The credential value is never read here, stored or printed.
#[derive(Clone, Debug, PartialEq, Eq)]
struct LiveConfig {
    endpoint: String,
    model: String,
    credential_ref: String,
    max_tokens: u32,
    prompt: String,
    timeout: Duration,
}

/// Reads the live configuration through `lookup`. A missing prerequisite is
/// reported by variable name only.
fn live_config(lookup: impl Fn(&str) -> Option<String>) -> Result<LiveConfig, String> {
    let present = |name: &str| lookup(name).filter(|value| !value.trim().is_empty());
    let mut missing = Vec::new();
    let endpoint = present("OCG_LIVE_ENDPOINT");
    let model = present("OCG_LIVE_MODEL");
    let credential_ref = present("OCG_LIVE_CREDENTIAL_REF");
    for (name, value) in [
        ("OCG_LIVE_ENDPOINT", &endpoint),
        ("OCG_LIVE_MODEL", &model),
        ("OCG_LIVE_CREDENTIAL_REF", &credential_ref),
    ] {
        if value.is_none() {
            missing.push(name.to_owned());
        }
    }
    if let Some(reference) = &credential_ref
        && present(reference).is_none()
    {
        missing.push(format!(
            "{reference} (the credential variable named by OCG_LIVE_CREDENTIAL_REF)"
        ));
    }
    if let Some(endpoint) = &endpoint
        && !endpoint.starts_with("https://")
    {
        return Err("OCG_LIVE_ENDPOINT must be an https:// URL; HTTPS is never relaxed".to_owned());
    }
    if !missing.is_empty() {
        return Err(format!("missing prerequisites: {}", missing.join(", ")));
    }
    let number = |name: &str, default: u64| {
        present(name)
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(default)
    };
    Ok(LiveConfig {
        endpoint: endpoint.expect("checked"),
        model: model.expect("checked"),
        credential_ref: credential_ref.expect("checked"),
        max_tokens: u32::try_from(number("OCG_LIVE_MAX_TOKENS", 32)).unwrap_or(32),
        prompt: present("OCG_LIVE_PROMPT")
            .unwrap_or_else(|| "Reply with the single word: ready.".to_owned()),
        timeout: Duration::from_secs(number("OCG_LIVE_TIMEOUT_SECS", 60)),
    })
}

fn host_of(endpoint: &str) -> String {
    endpoint
        .trim_start_matches("https://")
        .split(['/', '?', '#'])
        .next()
        .unwrap_or_default()
        .to_owned()
}

// ---- what a process observes of a Job --------------------------------------------------

fn fnv(text: &str) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in text.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{hash:016x}")
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct MetricFact {
    id: u64,
    token: Option<String>,
    observed_at: i64,
    /// input, output, cache read, cache write, reasoning
    counters: [Option<u32>; 5],
    raw_usage: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct AvailabilityFact {
    id: u64,
    observed_at: i64,
    status_code: Option<u16>,
    classification: String,
}

/// Everything a process reads of one Job through the database APIs. The writer
/// takes it before it exits; the reader takes it again from the reopened file;
/// the two must be equal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Observed {
    job: u64,
    parent: u64,
    input: u64,
    objective: Option<String>,
    assigned_executor: Option<u64>,
    executor_is_llm: bool,
    llm_profile: Option<u64>,
    provider: Option<u64>,
    endpoint_host: Option<String>,
    credential_ref: Option<String>,
    model: Option<String>,
    max_tokens: Option<u32>,
    created_at: i64,
    started_at: Option<i64>,
    completed_at: Option<i64>,
    failed_at: Option<i64>,
    cancelled_at: Option<i64>,
    unknown_at: Option<i64>,
    final_output: Option<u64>,
    final_text_len: usize,
    final_text_fnv: Option<String>,
    progress_outputs: usize,
    progress_text_fnv: Option<String>,
    metrics: Vec<MetricFact>,
    availability: Vec<AvailabilityFact>,
    /// The pure reducer over the raw annotated Metrics.
    token_from_raw: Option<String>,
}

fn micros(at: Timestamp) -> i64 {
    at.as_unix_micros()
}

fn observe_job(handle: &DatabaseHandle, job: JobId) -> Observed {
    let record = wait(handle.job(job))
        .expect("read the Job")
        .expect("the Job exists");
    let configuration = wait(handle.job_configuration(job)).expect("read configuration");
    let input = wait(handle.job_input(record.input))
        .expect("read input")
        .expect("the input exists");
    let executor_is_llm = match record.assigned_executor {
        Some(executor) => wait(handle.executor(executor))
            .expect("read Executor")
            .map(|executor| {
                executor
                    .llm_transports
                    .contains(&LlmTransport::OpenAiChatCompletions)
            })
            .unwrap_or(false),
        None => false,
    };

    // Outputs, in sequence order, in bounded pages.
    let mut outputs = Vec::new();
    loop {
        let page = wait(
            handle.job_outputs(
                job,
                outputs
                    .last()
                    .map(|output: &ocg::persistence::JobOutputRecord| output.seq),
                100,
            ),
        )
        .expect("read outputs");
        let done = page.len() < 100;
        outputs.extend(page);
        if done {
            break;
        }
    }
    let result = outputs
        .iter()
        .rev()
        .find(|output| output.kind == OutputKind::Result);
    let progress: Vec<&str> = outputs
        .iter()
        .filter(|output| output.kind == OutputKind::Progress)
        .filter_map(|output| output.summary.as_deref())
        .collect();

    let raw_metrics = wait(handle.job_metrics(job, None, 100)).expect("read Metrics");
    let stats = wait(handle.job_availability(job, None, 100)).expect("read AvailabilityStats");
    let sample = TokenRequestSample::from_metrics(&raw_metrics).expect("one Job");
    let (provider, model) = match &configuration {
        Some(c) => (
            Some(c.frozen.llm.provider.id),
            Some(c.frozen.llm.config.model.clone()),
        ),
        None => (None, None),
    };

    Observed {
        job: job.get(),
        parent: record.parent.get(),
        input: record.input.get(),
        objective: input.objective,
        assigned_executor: record.assigned_executor.map(|id| id.get()),
        executor_is_llm,
        llm_profile: record.configuration.map(|c| c.llm_profile.get()),
        provider: provider.map(|id| id.get()),
        endpoint_host: configuration
            .as_ref()
            .and_then(|c| c.frozen.llm.provider.endpoint.as_deref().map(host_of)),
        credential_ref: configuration
            .as_ref()
            .and_then(|c| c.frozen.llm.provider.credential_ref.clone()),
        model,
        max_tokens: configuration
            .as_ref()
            .and_then(|c| c.frozen.llm.config.parameters.max_tokens),
        created_at: micros(record.created_at),
        started_at: record.started_at.map(micros),
        completed_at: record.completed_at.map(micros),
        failed_at: record.failed_at.map(micros),
        cancelled_at: record.cancelled_at.map(micros),
        unknown_at: record.unknown_at.map(micros),
        final_output: result.map(|output| output.id.get()),
        final_text_len: result
            .and_then(|output| output.summary.as_deref())
            .map_or(0, str::len),
        final_text_fnv: result.and_then(|output| output.summary.as_deref()).map(fnv),
        progress_outputs: progress.len(),
        progress_text_fnv: (!progress.is_empty()).then(|| fnv(&progress.concat())),
        metrics: raw_metrics
            .iter()
            .map(|record| {
                let usage = &record.measurements.usage;
                MetricFact {
                    id: record.id.get(),
                    token: record
                        .token
                        .map(|token| format!("{:?}/{:?}", token.eligibility, token.finality)),
                    observed_at: micros(record.observed_at),
                    counters: [
                        usage.input_tokens,
                        usage.output_tokens,
                        usage.cache_read_tokens,
                        usage.cache_write_tokens,
                        usage.reasoning_tokens,
                    ],
                    raw_usage: usage.raw.as_ref().map(|raw| raw.to_string()),
                }
            })
            .collect(),
        availability: stats
            .iter()
            .map(|record| AvailabilityFact {
                id: record.id.get(),
                observed_at: micros(record.observed_at),
                status_code: record.observation.status_code,
                classification: format!("{:?}", record.observation.classification()),
            })
            .collect(),
        token_from_raw: sample.map(|sample| format!("{sample:?}")),
    }
}

/// What must hold of a Job that executed successfully. These are facts of the
/// durable record, never of the caller's expectations. Missing usage is
/// reported by `coverage`, not failed.
fn execution_problems(observed: &Observed, genesis: u64) -> Vec<String> {
    let mut problems = Vec::new();
    let mut check = |ok: bool, what: &str| {
        if !ok {
            problems.push(what.to_owned());
        }
    };
    check(
        observed.parent == genesis,
        "the parent is the persistent Genesis",
    );
    check(
        observed.executor_is_llm && observed.assigned_executor.is_some(),
        "a compatible LLM Executor is assigned",
    );
    check(
        observed.model.is_some() && observed.provider.is_some(),
        "the Job carries its frozen Provider and model",
    );
    check(
        observed
            .started_at
            .is_some_and(|started| started >= observed.created_at),
        "the Job was durably started",
    );
    check(
        observed.completed_at.is_some()
            && observed.failed_at.is_none()
            && observed.cancelled_at.is_none()
            && observed.unknown_at.is_none(),
        "the Job is Completed and nothing else",
    );
    check(
        observed.final_output.is_some() && observed.final_text_len > 0,
        "a non-empty final result was persisted",
    );
    if observed.progress_outputs > 0 {
        check(
            observed.progress_text_fnv == observed.final_text_fnv,
            "the persisted streamed deltas add up to the final result",
        );
    }
    check(!observed.metrics.is_empty(), "Metrics were recorded");
    check(
        observed
            .metrics
            .iter()
            .find(|fact| fact.token.is_some())
            .is_some_and(|fact| {
                fact.token
                    .as_deref()
                    .is_some_and(|token| token.starts_with("Eligible"))
            }),
        "the first Token fact marks the request eligible",
    );
    if let Some(started) = observed.started_at {
        check(
            observed
                .metrics
                .iter()
                .filter(|fact| fact.token.is_some())
                .all(|fact| fact.observed_at >= started),
            "delivery facts come after the durable start",
        );
    }
    check(
        observed
            .metrics
            .iter()
            .all(|fact| fact.counters.iter().all(Option::is_none) || fact.raw_usage.is_some()),
        "reported counters keep their raw usage object",
    );
    check(
        !observed.availability.is_empty(),
        "an AvailabilityStat was recorded",
    );
    problems
}

/// Telemetry facts that are reported, not required: how much usage the Provider
/// actually supplied.
fn coverage(observed: &Observed) -> Vec<String> {
    let mut notes = Vec::new();
    let usage_records = observed
        .metrics
        .iter()
        .filter(|fact| fact.raw_usage.is_some())
        .count();
    let finals = observed
        .metrics
        .iter()
        .filter(|fact| {
            fact.raw_usage.is_some() && fact.token.as_deref().is_some_and(|t| t.ends_with("Final"))
        })
        .count();
    notes.push(format!(
        "Provider usage objects: {usage_records} ({finals} final, {} cumulative)",
        usage_records - finals
    ));
    notes.push(match &observed.token_from_raw {
        Some(sample) => format!(
            "token request (reducer over raw): {}",
            sample
                .split("usage: ")
                .nth(1)
                .and_then(|rest| rest.split(',').next())
                .unwrap_or("?")
        ),
        None => "token request: none".to_owned(),
    });
    notes
}

// ---- the writer -----------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum Mode {
    /// A real Provider request through `ProviderExecutionPlan`.
    Live,
    /// An offline Job settled through the ordinary database APIs. Not a Provider.
    Fixture,
}

#[derive(Debug, Serialize, Deserialize)]
struct Manifest {
    mode: Mode,
    writer_pid: u32,
    genesis: u64,
    /// The Job's own execution error, when the request did not complete.
    execution_error: Option<String>,
    /// Set when the Job settled but an observation write did not.
    telemetry_gap: Option<String>,
    shutdown: Vec<String>,
    observed: Observed,
}

fn open_database(directory: &Path) -> (DatabaseWorker, DatabaseHandle) {
    let directory =
        DataDirectory::bootstrap(directory.as_os_str().to_owned()).expect("data directory");
    let policy = SystemPolicy::load_for_startup(&directory).expect("system policy");
    let worker = DatabaseWorker::start_with_policy(&directory, policy).expect("open the database");
    let handle = worker.handle();
    (worker, handle)
}

struct Targets {
    executor: ocg::persistence::ExecutorId,
    configuration: ExecutionConfiguration,
}

fn register_targets(
    handle: &DatabaseHandle,
    endpoint: &str,
    credential_ref: &str,
    model: &str,
    max_tokens: u32,
) -> Targets {
    let executor = wait(handle.create_executor(NewExecutor {
        name: "acceptance-llm".into(),
        concurrency_limit: std::num::NonZeroU32::MIN,
        llm_transports: BTreeSet::from([LlmTransport::OpenAiChatCompletions]),
    }))
    .expect("register the Executor");
    let connection = wait(handle.create_config(NewConfig {
        name: "acceptance-provider".into(),
        body: ConfigBody::Connection(ConnectionConfig {
            protocol: LlmTransport::OpenAiChatCompletions,
            endpoint: Some(endpoint.to_owned()),
            credential_ref: Some(credential_ref.to_owned()),
        }),
    }))
    .expect("register the Connection");
    let llm_profile = wait(handle.create_config(NewConfig {
        name: model.to_owned(),
        body: ConfigBody::Model(ModelConfig {
            connection_id: connection,
            model: model.to_owned(),
            parameters: LlmParameters {
                max_tokens: Some(max_tokens),
                ..LlmParameters::default()
            },
            declared_capabilities: BTreeSet::from([ModelCapability::Chat]),
        }),
    }))
    .expect("create the Model config")
    .into();
    Targets {
        executor,
        configuration: ExecutionConfiguration {
            agent_profile: None,
            llm_profile,
        },
    }
}

fn job_template(handle: &DatabaseHandle, configuration: ExecutionConfiguration) -> NewJob {
    NewJob {
        name: Some("live acceptance request".into()),
        eligible_at: None,
        parent: handle.genesis(),
        projects: Vec::new(),
        dependencies: Vec::new(),
        input: JobInputDraft::default(),
        configuration: Some(configuration),
        previous: None,
    }
}

/// The offline stand-in for a Provider run: a Job settled through the
/// ordinary APIs with streamed outputs, annotated Metrics and an
/// AvailabilityStat. It exercises the harness, never a Provider.
fn settle_fixture_job(handle: &DatabaseHandle, targets: &Targets) -> JobId {
    let mut job = job_template(handle, targets.configuration);
    job.input = JobInputDraft {
        objective: Some("offline fixture".into()),
        ..JobInputDraft::default()
    };
    let job = wait(handle.create_local_job(job, targets.executor)).expect("create the Job");
    let anchor = TokenAnnotation::eligible(UsageFinality::Cumulative);
    wait(handle.record_metrics(NewMetrics {
        job,
        request: RequestNumber::FIRST,
        observed_at: Timestamp::now(),
        measurements: Measurements::default(),
        token: Some(anchor),
    }))
    .expect("record the delivery fact");
    for delta in ["fixture ", "answer"] {
        wait(handle.append_job_output(
            job,
            NewJobOutput {
                kind: IntermediateOutputKind::Progress,
                summary: Some(delta.into()),
                code: None,
                goal_reached: None,
                value: None,
            },
        ))
        .expect("append a delta");
    }
    wait(handle.record_metrics(NewMetrics {
        job,
        request: RequestNumber::FIRST,
        observed_at: Timestamp::now(),
        measurements: Measurements {
            usage: NormalizedUsage {
                input_tokens: Some(4),
                output_tokens: Some(2),
                raw: Some(serde_json::json!({"prompt_tokens": 4, "completion_tokens": 2})),
                ..NormalizedUsage::default()
            },
            ..Measurements::default()
        },
        token: Some(TokenAnnotation::eligible(UsageFinality::Final)),
    }))
    .expect("record usage");
    wait(handle.finalize_job(
        job,
        JobFinalization::Completed(FinalResult {
            summary: Some("fixture answer".into()),
            code: None,
            goal_reached: None,
            value: None,
        }),
    ))
    .expect("finalize the Job");
    wait(handle.record_availability_stat(NewAvailabilityStat {
        job,
        request: RequestNumber::FIRST,
        observed_at: Timestamp::now(),
        observation: ocg::persistence::AvailabilityObservation {
            status_code: Some(200),
            finish_reason: Some(ocg::openai_compatible::ChatFinishReason::Stop),
            raw_finish_reason: Some("stop".into()),
            failure: None,
        },
    }))
    .expect("record AvailabilityStat");
    job
}

fn run_writer(mode: Mode, directory: &Path) {
    let directory = directory.to_path_buf();
    ntex::rt::System::new("acceptance-writer", ntex::rt::DefaultRuntime).block_on(async move {
        run_writer_async(mode, &directory).await;
    });
}

async fn run_writer_async(mode: Mode, directory: &Path) {
    let (worker, handle) = open_database(directory);
    let genesis = handle.genesis().get();
    let live = (mode == Mode::Live).then(|| {
        live_config(|name| std::env::var(name).ok())
            .unwrap_or_else(|blocker| panic!("NOT VERIFIED: {blocker}"))
    });
    let execution = ExecutionRuntime::start(handle.clone()).expect("start the execution runtime");

    let (job, execution_error, telemetry_gap) = match &live {
        Some(config) => {
            let targets = register_targets(
                &handle,
                &config.endpoint,
                &config.credential_ref,
                &config.model,
                config.max_tokens,
            );
            let plan = ProviderExecutionPlan {
                executor_id: targets.executor,
                input: JobInputDraft {
                    objective: Some(config.prompt.clone()),
                    ..JobInputDraft::default()
                },
                job: job_template(&handle, targets.configuration),
                credential_resolver: Arc::new(EnvironmentCredentialResolver),
                provider_config: ProviderExecutionConfig {
                    timeout: config.timeout,
                    ..ProviderExecutionConfig::default()
                },
                tools: None,
            };
            // The one and only Provider request. Nothing below resends it.
            let outcome = ntex::time::timeout(
                config.timeout + Duration::from_secs(60),
                execution.scheduler().schedule_provider(plan),
            )
            .await
            .expect("the execution settled within its bound");
            match outcome {
                Ok(receipt) => (Some(receipt.job), None, None),
                Err(ExecutionRuntimeError::ObservationsNotRecorded { receipt, source }) => {
                    (Some(receipt.job), None, Some(source.to_string()))
                }
                Err(error) => {
                    // The Job may exist; take the newest configured one.
                    let newest = wait(handle.jobs(None, 5))
                        .expect("read Jobs")
                        .into_iter()
                        .find(|job| job.configuration.is_some())
                        .map(|job| job.id);
                    (newest, Some(error.to_string()), None)
                }
            }
        }
        None => {
            let targets = register_targets(
                &handle,
                "https://fixture.invalid/v1/chat/completions",
                "FIXTURE_NOT_A_CREDENTIAL",
                "fixture-model",
                8,
            );
            (Some(settle_fixture_job(&handle, &targets)), None, None)
        }
    };
    let job = job.expect("a Job was created for the request");
    let mut shutdown = Vec::new();
    let observed = observe_job(&handle, job);

    // Component owners are closed cleanly, database last.
    if let Err(error) = execution.shutdown().await {
        shutdown.push(format!("execution runtime: {error}"));
    }
    if let Err(error) = worker.shutdown() {
        shutdown.push(format!("database: {error}"));
    }
    let manifest = Manifest {
        mode,
        writer_pid: std::process::id(),
        genesis,
        execution_error,
        telemetry_gap,
        shutdown,
        observed,
    };
    std::fs::write(
        directory.join(MANIFEST_FILE),
        serde_json::to_string_pretty(&manifest).expect("serialize the manifest"),
    )
    .expect("write the manifest");

    assert!(
        manifest.execution_error.is_none(),
        "the request did not complete: {:?}",
        manifest.execution_error
    );
    let problems = execution_problems(&manifest.observed, genesis);
    assert!(
        problems.is_empty(),
        "the writer's own read-back failed: {problems:?}"
    );
    if let Some(config) = &live {
        let observed = &manifest.observed;
        assert_eq!(
            observed.model.as_deref(),
            Some(config.model.as_str()),
            "the frozen model"
        );
        assert_eq!(
            observed.endpoint_host.as_deref(),
            Some(host_of(&config.endpoint).as_str())
        );
        assert_eq!(
            observed.credential_ref.as_deref(),
            Some(config.credential_ref.as_str())
        );
    }
    assert!(
        manifest
            .shutdown
            .iter()
            .all(|note| !note.contains("database") && !note.contains("execution")),
        "component owners closed cleanly: {:?}",
        manifest.shutdown
    );
}

// ---- the reader -----------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
struct ReaderResult {
    reader_pid: u32,
    problems: Vec<String>,
    observed: Observed,
}

fn run_reader(directory: &Path) {
    let manifest: Manifest = serde_json::from_str(
        &std::fs::read_to_string(directory.join(MANIFEST_FILE)).expect("read the manifest"),
    )
    .expect("parse the manifest");
    assert_ne!(
        manifest.writer_pid,
        std::process::id(),
        "the reader is a different process"
    );
    let (worker, handle) = open_database(directory);
    // Only the Job's ID is carried over; everything else is read from the file.
    let observed = observe_job(&handle, JobId::new(manifest.observed.job));
    worker.shutdown().expect("close the database");

    let mut problems = execution_problems(&observed, manifest.genesis);
    if observed != manifest.observed {
        problems
            .push("the reopened Job differs from what the writer read before exiting".to_owned());
    }
    let result = ReaderResult {
        reader_pid: std::process::id(),
        problems,
        observed,
    };
    std::fs::write(
        directory.join(READER_FILE),
        serde_json::to_string_pretty(&result).expect("serialize the reader result"),
    )
    .expect("write the reader result");
    assert!(
        result.problems.is_empty(),
        "read-back problems: {:?}",
        result.problems
    );
}

// ---- the two child roles: run only when named, with --ignored ------------------------------

fn role_directory(role: &str) -> Option<(Mode, PathBuf)> {
    if std::env::var(ROLE_VARIABLE).ok().as_deref() != Some(role) {
        return None;
    }
    let mode = match std::env::var(MODE_VARIABLE).ok().as_deref() {
        Some("live") => Mode::Live,
        Some("fixture") => Mode::Fixture,
        other => panic!("{MODE_VARIABLE} must be live or fixture, not {other:?}"),
    };
    let directory = std::env::var(DIRECTORY_VARIABLE).expect("the data directory is named");
    Some((mode, PathBuf::from(directory)))
}

#[test]
#[ignore = "a child role of the acceptance run; it does nothing unless the orchestrator names it"]
fn writer_role() {
    if let Some((mode, directory)) = role_directory("writer") {
        run_writer(mode, &directory);
    }
}

#[test]
#[ignore = "a child role of the acceptance run; it does nothing unless the orchestrator names it"]
fn reader_role() {
    if let Some((_, directory)) = role_directory("reader") {
        run_reader(&directory);
    }
}

// ---- the orchestrator -----------------------------------------------------------------

struct Run {
    writer_pid_seen: u32,
    writer_status: ExitStatus,
    reader_pid: u32,
    reader_status: ExitStatus,
    manifest: Manifest,
    reader: ReaderResult,
}

/// Starts this test binary again as one role and waits for it to exit.
fn run_role(
    role: &str,
    test: &str,
    mode: Mode,
    directory: &Path,
    without: Option<&str>,
) -> ExitStatus {
    let mut command = Command::new(std::env::current_exe().expect("this test binary"));
    command
        .args([
            "--exact",
            test,
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(ROLE_VARIABLE, role)
        .env(
            MODE_VARIABLE,
            match mode {
                Mode::Live => "live",
                Mode::Fixture => "fixture",
            },
        )
        .env(DIRECTORY_VARIABLE, directory)
        .stdin(Stdio::null());
    // Each role's output is kept beside its data, not mixed into this run's.
    let log = |extension: &str| {
        std::fs::File::create(directory.join(format!("{role}.{extension}")))
            .expect("create the role log")
    };
    command.stdout(log("out")).stderr(log("err"));
    if let Some(name) = without {
        command.env_remove(name);
    }
    command.status().expect("start the role process")
}

/// Runs the writer to exit, and only then the reader, on one data directory.
fn two_processes(mode: Mode, directory: &Path, credential_ref: Option<&str>) -> Run {
    std::fs::create_dir_all(directory).expect("create the data directory");
    let writer_status = run_role("writer", "writer_role", mode, directory, None);
    // The reader starts only after the writer has exited, holds no credential
    // and is told nothing but where the file is.
    let manifest_text = std::fs::read_to_string(directory.join(MANIFEST_FILE));
    let reader_status = match &manifest_text {
        Ok(_) => run_role("reader", "reader_role", mode, directory, credential_ref),
        Err(_) => panic!("the writer exited ({writer_status:?}) without a manifest"),
    };
    let manifest: Manifest =
        serde_json::from_str(&manifest_text.expect("manifest")).expect("parse the manifest");
    let reader: ReaderResult = serde_json::from_str(
        &std::fs::read_to_string(directory.join(READER_FILE)).expect("the reader wrote its result"),
    )
    .expect("parse the reader result");
    Run {
        writer_pid_seen: manifest.writer_pid,
        writer_status,
        reader_pid: reader.reader_pid,
        reader_status,
        manifest,
        reader,
    }
}

fn assert_run_accepted(run: &Run) {
    assert!(
        run.writer_status.success(),
        "writer exit: {:?}",
        run.writer_status
    );
    assert!(
        run.reader_status.success(),
        "reader exit: {:?}",
        run.reader_status
    );
    assert_ne!(
        run.writer_pid_seen, run.reader_pid,
        "two different processes"
    );
    assert!(run.reader.problems.is_empty(), "{:?}", run.reader.problems);
    assert_eq!(
        run.reader.observed, run.manifest.observed,
        "the same Job, read back whole"
    );
    assert_eq!(run.reader.observed.job, run.manifest.observed.job);
}

fn current_commit() -> String {
    Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .ok()
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|text| text.trim().to_owned())
        .unwrap_or_else(|| "unknown".to_owned())
}

/// The sanitized evidence of a run: no credential, no Provider body beyond the
/// innocuous answer's length and fingerprint.
fn write_evidence(directory: &Path, config: Option<&LiveConfig>, run: &Run) -> PathBuf {
    let observed = &run.reader.observed;
    let evidence = serde_json::json!({
        "commit": current_commit(),
        "mode": run.manifest.mode,
        "provider_host": config.map(|c| host_of(&c.endpoint)),
        "model": observed.model,
        "credential_reference": config.map(|c| c.credential_ref.clone()),
        "writer": {"pid": run.writer_pid_seen, "exit": run.writer_status.to_string()},
        "reader": {"pid": run.reader_pid, "exit": run.reader_status.to_string()},
        "job": observed.job, "input": observed.input, "parent": observed.parent,
        "assigned_executor": observed.assigned_executor, "final_output": observed.final_output,
        "final_text_len": observed.final_text_len, "final_text_fnv": observed.final_text_fnv,
        "progress_outputs": observed.progress_outputs,
        "terminal": {"completed_at": observed.completed_at, "failed_at": observed.failed_at,
                     "unknown_at": observed.unknown_at, "cancelled_at": observed.cancelled_at},
        "metrics": observed.metrics.len(), "availability": observed.availability.len(),
        "coverage": coverage(observed),
        "telemetry_gap": run.manifest.telemetry_gap,
        "shutdown_notes": run.manifest.shutdown,
        "readback_equal_to_writer": run.reader.observed == run.manifest.observed,
    });
    let path = directory.join("evidence.json");
    std::fs::write(
        &path,
        serde_json::to_string_pretty(&evidence).expect("evidence"),
    )
    .expect("write evidence");
    path
}

#[test]
#[ignore = "contacts a real Provider with exactly one request; see the module documentation"]
fn live_https_job_survives_process_exit() {
    let config = match live_config(|name| std::env::var(name).ok()) {
        Ok(config) => config,
        Err(blocker) => panic!("LIVE HTTPS NOT VERIFIED: {blocker}"),
    };
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs());
    let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("live-acceptance")
        .join(format!("{stamp}-{}", std::process::id()));
    let run = two_processes(Mode::Live, &directory, Some(&config.credential_ref));
    let evidence = write_evidence(&directory, Some(&config), &run);
    eprintln!("sanitized evidence kept at {}", evidence.display());
    assert_run_accepted(&run);
    // A settled Job with an unrecorded observation is durable execution
    // evidence with a telemetry gap; say so rather than hide it.
    if let Some(gap) = &run.manifest.telemetry_gap {
        eprintln!(
            "TELEMETRY GAP (execution durable, observation pipeline NOT fully verified): {gap}"
        );
    }
    eprintln!("LIVE HTTPS PASS; NEW-PROCESS RESULT READBACK PASS");
}

// ---- offline checks of the same logic ------------------------------------------------------

#[test]
fn offline_fixture_goes_through_two_real_processes_and_reads_back_whole() {
    let directory =
        std::env::temp_dir().join(format!("ocg-acceptance-fixture-{}", std::process::id()));
    std::fs::remove_dir_all(&directory).ok();
    let run = two_processes(Mode::Fixture, &directory, None);
    assert_run_accepted(&run);
    let observed = &run.reader.observed;
    assert_eq!(observed.progress_outputs, 2);
    assert_eq!(observed.final_text_len, "fixture answer".len());

    assert_eq!(
        run.manifest.mode,
        Mode::Fixture,
        "this was never a Provider run"
    );
    std::fs::remove_dir_all(&directory).ok();
}

#[test]
fn offline_readback_notices_a_changed_result() {
    let directory =
        std::env::temp_dir().join(format!("ocg-acceptance-tamper-{}", std::process::id()));
    std::fs::remove_dir_all(&directory).ok();
    std::fs::create_dir_all(&directory).expect("directory");
    let status = run_role("writer", "writer_role", Mode::Fixture, &directory, None);
    assert!(status.success());
    let path = directory.join(MANIFEST_FILE);
    let mut manifest: Manifest =
        serde_json::from_str(&std::fs::read_to_string(&path).expect("read")).expect("parse");
    manifest.observed.final_text_fnv = Some(fnv("a different answer"));
    std::fs::write(&path, serde_json::to_string(&manifest).expect("serialize")).expect("write");
    let status = run_role("reader", "reader_role", Mode::Fixture, &directory, None);
    assert!(
        !status.success(),
        "a result that differs from the manifest must fail the reader"
    );
    let reader: ReaderResult =
        serde_json::from_str(&std::fs::read_to_string(directory.join(READER_FILE)).expect("read"))
            .expect("parse");
    assert!(
        reader
            .problems
            .iter()
            .any(|problem| problem.contains("differs")),
        "{:?}",
        reader.problems
    );
    std::fs::remove_dir_all(&directory).ok();
}

#[test]
fn missing_prerequisites_are_named_without_any_value() {
    let secret = "sk-this-must-never-be-printed";
    let nothing = live_config(|_| None).expect_err("nothing is configured");
    for name in [
        "OCG_LIVE_ENDPOINT",
        "OCG_LIVE_MODEL",
        "OCG_LIVE_CREDENTIAL_REF",
    ] {
        assert!(nothing.contains(name), "{nothing}");
    }
    // The credential variable is named by reference and checked for presence only.
    let lookup = |name: &str| match name {
        "OCG_LIVE_ENDPOINT" => Some("https://provider.example/v1/chat/completions".to_owned()),
        "OCG_LIVE_MODEL" => Some("a-model".to_owned()),
        "OCG_LIVE_CREDENTIAL_REF" => Some("SOME_KEY".to_owned()),
        _ => None,
    };
    let blocked = live_config(lookup).expect_err("the credential variable is unset");
    assert!(
        blocked.contains("SOME_KEY") && !blocked.contains(secret),
        "{blocked}"
    );
    let with_key = |name: &str| {
        (name == "SOME_KEY")
            .then(|| secret.to_owned())
            .or_else(|| lookup(name))
    };
    let config = live_config(with_key).expect("everything is present");
    assert_eq!(
        (config.model.as_str(), config.credential_ref.as_str()),
        ("a-model", "SOME_KEY")
    );
    assert!(
        !format!("{config:?}").contains(secret),
        "the credential value is never held"
    );
    assert_eq!(
        config.max_tokens, 32,
        "a small explicit output limit by default"
    );
    let insecure = |name: &str| {
        (name == "OCG_LIVE_ENDPOINT")
            .then(|| "http://provider.example/v1".to_owned())
            .or_else(|| lookup(name))
    };
    assert!(
        live_config(insecure)
            .expect_err("HTTP is refused")
            .contains("https")
    );
    assert_eq!(
        host_of("https://provider.example:8443/v1/chat/completions"),
        "provider.example:8443"
    );
}
