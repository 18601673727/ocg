use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use ocg::system::data_directory::DataDirectory;
use serde_json::{Value, json};

use crate::backend_process::{BackendConfig, BackendHandshake, BackendProcess};
use crate::benchmark::{Outcome, OutcomeCounts};
use crate::fixture::{FixtureConfig, FixtureGenerator, FixtureStats};
use crate::http_client::BenchmarkClient;
use crate::metadata::{BenchmarkMetadata, WorkloadParameters};
use crate::mock_provider::{MockProvider, MockProviderConfig, ProviderCounterSnapshot};
use crate::reporting::{
    MeasurementRecord, Record, canonical_metric, emit_begin, emit_section_complete,
};
use crate::{Options, Result, TemporaryRoot};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReferenceJob {
    pub model_requests: usize,
    pub tool_calls: usize,
}

impl ReferenceJob {
    pub const fn standard() -> Self {
        Self {
            model_requests: 3,
            tool_calls: 2,
        }
    }
}

pub struct ConcurrencyLevels;

impl ConcurrencyLevels {
    pub const ALL: [usize; 3] = [1, 8, 16];
    pub const REFERENCE: usize = 8;
}

struct MeasurementContext {
    root: TemporaryRoot,
    generator: FixtureGenerator,
    fixture: FixtureStats,
    metadata: BenchmarkMetadata,
    run_id: String,
}

impl MeasurementContext {
    async fn new(options: &Options) -> Result<Self> {
        let directory = options.directory.clone();
        let files = options.files;
        ntex::rt::spawn_blocking(move || {
            let root = TemporaryRoot::new(&directory)?;
            let generator = FixtureGenerator::new(
                FixtureConfig {
                    file_count: files,
                    ..FixtureConfig::small()
                },
                root.0.clone(),
            );
            let fixture = generator.generate()?;
            if fixture.file_count != files || fixture.probe_file_bytes == 0 {
                return Err(format!("invalid generated fixture: {fixture:?}").into());
            }
            let metadata = BenchmarkMetadata::capture(
                root.0.clone(),
                if cfg!(debug_assertions) {
                    "debug"
                } else {
                    "bench"
                },
            )?;
            Ok(Self {
                root,
                generator,
                fixture,
                metadata,
                run_id: uuid::Uuid::now_v7().to_string(),
            })
        })
        .await?
    }

    fn parameters(
        &self,
        options: &Options,
        section: &str,
        concurrency: usize,
        operations: usize,
    ) -> WorkloadParameters {
        let inapplicable = match section {
            "history" => vec!["payloads", "outputs"],
            "search" => vec![
                "jobs",
                "payloads",
                "outputs",
                "deadline_ms",
                "page_size",
                "writer_jobs",
                "stream_chunks",
                "stream_pace_ms",
            ],
            "database" => vec![
                "iterations",
                "payloads",
                "outputs",
                "warmup",
                "page_size",
                "writer_jobs",
            ],
            _ => vec![
                "iterations",
                "payloads",
                "outputs",
                "page_size",
                "writer_jobs",
            ],
        };
        WorkloadParameters::new(section, concurrency, operations)
            .with("inapplicable_options", json!(inapplicable))
            .with_fixture_size(self.fixture.file_count)
            .with("run_id", json!(self.run_id))
            .with("mode", json!(options.mode))
            .with("scope", json!(section))
            .with("fixture_files", json!(self.fixture.file_count))
            .with("fixture_bytes", json!(self.fixture.total_file_bytes))
            .with("warm_up", json!(options.warmup))
            .with(
                "requested",
                json!({"jobs": options.jobs, "iterations": options.iterations,
                "trials": options.trials, "clients": options.clients, "files": options.files,
                "warmup": options.warmup, "deadline_ms": options.deadline.as_millis(),
                "page_size": options.page_size, "writer_jobs": options.writer_jobs,
                "stream_chunks": options.stream_chunks, "stream_pace_ms": options.stream_pace.as_millis()}),
            )
            .with(
                "applied",
                json!({"jobs": if section == "search" { None } else { Some(options.jobs) },
                "iterations": if matches!(section, "history" | "search") { Some(options.iterations) } else { None },
                "trials": options.trials, "concurrency": concurrency,
                "files": options.files, "warmup": if section == "database" { 0 } else { options.warmup },
                "deadline_ms": if section == "search" { None } else { Some(options.deadline.as_millis()) },
                "page_size": if section == "history" { Some(options.page_size) } else { None },
                "writer_jobs": if section == "history" { Some(options.writer_jobs) } else { None },
                "stream_chunks": if section == "search" { None } else { Some(options.stream_chunks) },
                "stream_pace_ms": if section == "search" { None } else { Some(options.stream_pace.as_millis()) }}),
            )
            .with("load_model", json!("closed_loop"))
            .with("units", json!({"latency": "ms", "timing_window": "s"}))
    }
}

fn provider_config(options: &Options) -> MockProviderConfig {
    MockProviderConfig {
        answer_tokens: options.stream_chunks,
        pace: options.stream_pace,
        gate_timeout: options.deadline,
        fail_requests: options.provider_failure,
        ..MockProviderConfig::default()
    }
}

fn backend_config(
    context: &MeasurementContext,
    options: &Options,
    provider: &MockProvider,
    data_dir: PathBuf,
    concurrency: usize,
    trial: usize,
) -> BackendConfig {
    BackendConfig {
        data_dir,
        workspace: context.generator.workspace_path(),
        provider_endpoint: provider.endpoint(),
        port: 0,
        token: format!("benchmark-0123456789-abcdefghijk-{trial}"),
        concurrency,
        startup_timeout: options.deadline,
        shutdown_timeout: options.deadline.min(Duration::from_secs(15)),
        hang_before_handshake: options.child_hang,
    }
}

async fn spawn_ready(
    context: &MeasurementContext,
    options: &Options,
    provider: &MockProvider,
    data_dir: PathBuf,
    concurrency: usize,
    trial: usize,
) -> Result<(BackendProcess, BenchmarkClient, Duration)> {
    let started = Instant::now();
    let config = backend_config(context, options, provider, data_dir, concurrency, trial);
    let backend = ntex::rt::spawn_blocking(move || BackendProcess::spawn(&config)).await??;
    let handshake = backend.handshake().clone();
    let client = BenchmarkClient::new(handshake.address, handshake.token, options.deadline)
        .await
        .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> { error.into() })?;
    client
        .readiness()
        .await
        .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> { error.into() })?;
    Ok((backend, client, started.elapsed()))
}

async fn shutdown_backend(backend: BackendProcess) -> Result<()> {
    ntex::rt::spawn_blocking(move || backend.shutdown()).await?
}

async fn stop_provider(provider: Rc<MockProvider>) -> Result<()> {
    Rc::try_unwrap(provider)
        .map_err(|_| "mock provider still has active users")?
        .stop()
        .await
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WorkMode {
    Admission,
    Completion,
    Streaming,
}

#[derive(Debug)]
struct JobObservation {
    job_key: String,
    job_id: Option<String>,
    admission: Option<Duration>,
    pending_lookups: usize,
    completion: Option<Duration>,
    first_content: Option<Duration>,
    content_span: Option<Duration>,
    delivery: Vec<Duration>,
    model_requests: usize,
    tool_calls: usize,
    completions: usize,
    error: Option<String>,
}

impl JobObservation {
    fn new(job_key: String) -> Self {
        Self {
            job_key,
            job_id: None,
            admission: None,
            pending_lookups: 0,
            completion: None,
            first_content: None,
            content_span: None,
            delivery: Vec::new(),
            model_requests: 0,
            tool_calls: 0,
            completions: 0,
            error: None,
        }
    }
}

#[derive(Debug)]
struct WorkResult {
    jobs: Vec<JobObservation>,
    attempted: usize,
    max_overlap: usize,
    started: Instant,
    window: Duration,
    provider_before: ProviderCounterSnapshot,
}

impl WorkResult {
    fn outcomes(&self) -> OutcomeCounts {
        let mut outcomes = OutcomeCounts::default();
        for job in &self.jobs {
            if let Some(error) = &job.error {
                let kind = if error.contains("deadline") || error.contains("timed out") {
                    Outcome::TimedOut
                } else {
                    Outcome::Failed
                };
                outcomes.record_failure(kind);
            } else {
                outcomes.record_success();
            }
        }
        outcomes.unfinished = self.attempted.saturating_sub(self.jobs.len());
        outcomes.attempted = self.attempted;
        outcomes
    }

    fn reference_counts(&self) -> (usize, usize, usize) {
        self.jobs.iter().fold((0, 0, 0), |counts, job| {
            (
                counts.0 + job.model_requests,
                counts.1 + job.tool_calls,
                counts.2 + job.completions,
            )
        })
    }
}

struct Overlap {
    active: AtomicUsize,
    maximum: AtomicUsize,
}

impl Overlap {
    fn enter(&self) -> OverlapGuard<'_> {
        let active = self.active.fetch_add(1, Ordering::Relaxed) + 1;
        self.maximum.fetch_max(active, Ordering::Relaxed);
        OverlapGuard { overlap: self }
    }
}

struct OverlapGuard<'a> {
    overlap: &'a Overlap,
}

impl Drop for OverlapGuard<'_> {
    fn drop(&mut self) {
        self.overlap.active.fetch_sub(1, Ordering::Relaxed);
    }
}

struct WorkerResult {
    jobs: Vec<JobObservation>,
}

async fn run_workload(
    handshake: &BackendHandshake,
    provider: &Rc<MockProvider>,
    options: &Options,
    concurrency: usize,
    jobs: usize,
    mode: WorkMode,
) -> Result<WorkResult> {
    let next = Arc::new(AtomicUsize::new(0));
    let overlap = Arc::new(Overlap {
        active: AtomicUsize::new(0),
        maximum: AtomicUsize::new(0),
    });
    let (start_sender, start_receiver) = flume::bounded(concurrency);
    let (ready_sender, ready_receiver) = flume::bounded(concurrency);
    let provider_before = provider.counters().snapshot();
    let mut workers = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let handshake = handshake.clone();
        let next = next.clone();
        let overlap = overlap.clone();
        let start = start_receiver.clone();
        let ready = ready_sender.clone();
        let options = options.clone();
        let provider = provider.clone();
        workers.push(ntex::rt::spawn(async move {
            let client = BenchmarkClient::new(
                handshake.address.clone(),
                handshake.token.clone(),
                options.deadline,
            )
            .await;
            ready
                .send_async(())
                .await
                .map_err(|error| format!("worker readiness barrier failed: {error}"))?;
            start
                .recv_async()
                .await
                .map_err(|error| format!("worker start barrier failed: {error}"))?;
            let client = client?;
            let mut observations = Vec::new();
            loop {
                let index = next.fetch_add(1, Ordering::Relaxed);
                if index >= jobs {
                    break;
                }
                let _active = overlap.enter();
                observations
                    .push(execute_job(&client, &handshake, &provider, &options, mode).await);
            }
            Ok::<_, String>(WorkerResult { jobs: observations })
        }));
    }
    drop(start_receiver);
    drop(ready_sender);
    for _ in 0..concurrency {
        ready_receiver
            .recv_async()
            .await
            .map_err(|error| format!("worker readiness failed: {error}"))?;
    }
    let started = Instant::now();
    for _ in 0..concurrency {
        start_sender
            .send_async(())
            .await
            .map_err(|error| format!("worker start failed: {error}"))?;
    }
    let mut observations = Vec::with_capacity(jobs);
    for worker in workers {
        let mut result = worker
            .await
            .map_err(|_| "benchmark workload worker panicked")?
            .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> { error.into() })?;
        observations.append(&mut result.jobs);
    }
    let window = started.elapsed();
    observations.sort_by(|left, right| left.job_key.cmp(&right.job_key));
    Ok(WorkResult {
        jobs: observations,
        attempted: jobs,
        max_overlap: overlap.maximum.load(Ordering::Relaxed),
        started,
        window,
        provider_before,
    })
}

async fn execute_job(
    client: &BenchmarkClient,
    handshake: &BackendHandshake,
    provider: &MockProvider,
    options: &Options,
    mode: WorkMode,
) -> JobObservation {
    let job_key = uuid::Uuid::now_v7().to_string();
    let mut observation = JobObservation::new(job_key.clone());
    let objective = format!(
        "OCG benchmark job={job_key}\nRead the authorized probe twice, then finish with the streamed markers."
    );
    let result = async {
        let submitted = Instant::now();
        let admission = client
            .submit_job(
                &handshake.executor,
                &handshake.llm_profile,
                &handshake.agent_profile,
                &handshake.project,
                &objective,
            )
            .await?;
        observation.admission = Some(submitted.elapsed());
        observation.pending_lookups = admission.pending_lookups;
        observation.job_id = Some(admission.job_id.clone());
        match mode {
            WorkMode::Admission => {}
            WorkMode::Completion => {
                client
                    .consume_events(
                        &admission.job_id,
                        options.deadline,
                        || provider.release_markers(&job_key),
                        |_| Ok(()),
                    )
                    .await?;
                let validation = client
                    .validate_reference_job(&admission.job_id, &job_key, options.stream_chunks)
                    .await?;
                observation.model_requests = validation.model_requests;
                observation.tool_calls = validation.tool_calls;
                observation.completions = validation.completions;
                observation.completion = Some(submitted.elapsed());
            }
            WorkMode::Streaming => {
                stream_job(
                    client,
                    provider,
                    options,
                    &admission.job_id,
                    &job_key,
                    submitted,
                    &mut observation,
                )
                .await?;
            }
        }
        Ok::<_, String>(())
    }
    .await;
    if let Err(error) = result {
        observation.error = Some(error);
    }
    observation
}

struct MarkerState {
    content: String,
    scanned: usize,
    next_sequence: usize,
    seen: BTreeSet<usize>,
    first_content_at: Option<Instant>,
    final_content_at: Option<Instant>,
    last_output_id: Option<i64>,
}

async fn stream_job(
    client: &BenchmarkClient,
    provider: &MockProvider,
    options: &Options,
    job_id: &str,
    job_key: &str,
    submitted: Instant,
    observation: &mut JobObservation,
) -> std::result::Result<(), String> {
    let mut state = MarkerState {
        content: String::new(),
        scanned: 0,
        next_sequence: 0,
        seen: BTreeSet::new(),
        first_content_at: None,
        final_content_at: None,
        last_output_id: None,
    };
    let prefix = format!("OCGMARK-{job_key}-");
    client
        .consume_events(
            job_id,
            options.deadline,
            || provider.release_markers(job_key),
            |event| {
                if event.name == "output" {
                    let id = event
                        .id
                        .as_deref()
                        .ok_or("output event omitted its sequence ID")?
                        .parse::<i64>()
                        .map_err(|_| "output event has a malformed sequence ID")?;
                    if state.last_output_id.is_some_and(|previous| id <= previous) {
                        return Err(format!("duplicated or reordered output event ID {id}"));
                    }
                    state.last_output_id = Some(id);
                }
                if event.name != "output" || event.data["output"]["kind"] == "result" {
                    return Ok(());
                }
                let Some(text) = event.data["output"]["value"]["text"].as_str() else {
                    return Err(format!("content output omitted value.text: {}", event.data));
                };
                if text.is_empty() {
                    return Ok(());
                }
                state.first_content_at.get_or_insert(event.observed_at);
                state.content.push_str(text);
                while let Some(relative) = state.content[state.scanned..].find("OCGMARK-") {
                    let start = state.scanned + relative;
                    if !state.content[start..].starts_with(&prefix) {
                        return Err(format!("unexpected or malformed marker in {job_id}"));
                    }
                    let digits = start + prefix.len();
                    if state.content.len() < digits + 4 {
                        state.scanned = start;
                        break;
                    }
                    let sequence = state.content[digits..digits + 4]
                        .parse::<usize>()
                        .map_err(|_| format!("malformed marker sequence in {job_id}"))?;
                    if state.content[digits + 4..]
                        .chars()
                        .next()
                        .is_some_and(|character| {
                            character.is_ascii_alphanumeric() || character == '-'
                        })
                    {
                        return Err(format!("unterminated marker in {job_id}"));
                    }
                    if sequence != state.next_sequence || !state.seen.insert(sequence) {
                        return Err(format!(
                            "missing, duplicated, or reordered marker {sequence}; expected {}",
                            state.next_sequence
                        ));
                    }
                    let marker = format!("{prefix}{sequence:04}");
                    let emitted = provider
                        .emitted_at(&marker)
                        .ok_or_else(|| format!("observed marker was not emitted live: {marker}"))?;
                    let delivered = event
                        .observed_at
                        .checked_duration_since(emitted)
                        .ok_or_else(|| {
                            format!("marker {marker} was observed before its live emission")
                        })?;
                    observation.delivery.push(delivered);
                    state.next_sequence += 1;
                    state.scanned = digits + 4;
                    if state.next_sequence == options.stream_chunks {
                        state.final_content_at = Some(event.observed_at);
                    }
                }
                Ok(())
            },
        )
        .await?;
    if state.next_sequence != options.stream_chunks {
        return Err(format!(
            "job {job_id} delivered {} of {} markers",
            state.next_sequence, options.stream_chunks
        ));
    }
    let validation = client
        .validate_reference_job(job_id, job_key, options.stream_chunks)
        .await?;
    observation.model_requests = validation.model_requests;
    observation.tool_calls = validation.tool_calls;
    observation.completions = validation.completions;
    observation.completion = Some(submitted.elapsed());
    observation.first_content = state
        .first_content_at
        .map(|at| at.duration_since(submitted));
    observation.content_span = match (state.first_content_at, state.final_content_at) {
        (Some(first), Some(last)) => Some(last.saturating_duration_since(first)),
        _ => None,
    };
    Ok(())
}

async fn drain_admitted(
    client: &BenchmarkClient,
    provider: &MockProvider,
    options: &Options,
    result: &mut WorkResult,
) {
    for observation in &mut result.jobs {
        if observation.error.is_some() {
            continue;
        }
        let Some(job_id) = observation.job_id.clone() else {
            observation.error = Some("admitted job lost its ID".into());
            continue;
        };
        let drained = async {
            client
                .consume_events(
                    &job_id,
                    options.deadline,
                    || provider.release_markers(&observation.job_key),
                    |_| Ok(()),
                )
                .await?;
            let validation = client
                .validate_reference_job(&job_id, &observation.job_key, options.stream_chunks)
                .await?;
            observation.model_requests = validation.model_requests;
            observation.tool_calls = validation.tool_calls;
            observation.completions = validation.completions;
            Ok::<_, String>(())
        }
        .await;
        if let Err(error) = drained {
            observation.error = Some(error);
        }
    }
}

fn check_provider_counts(
    provider: &MockProvider,
    before: ProviderCounterSnapshot,
    jobs: usize,
    stream_chunks: usize,
) -> std::result::Result<ProviderCounterSnapshot, String> {
    let after = provider.counters().snapshot();
    let delta = ProviderCounterSnapshot {
        requests: after.requests.saturating_sub(before.requests),
        chunks: after.chunks.saturating_sub(before.chunks),
        tool_calls: after.tool_calls.saturating_sub(before.tool_calls),
    };
    let expected = ProviderCounterSnapshot {
        requests: (jobs * 3) as u64,
        chunks: (jobs * stream_chunks) as u64,
        tool_calls: (jobs * 2) as u64,
    };
    if delta != expected {
        return Err(format!(
            "provider counts differ from the validated reference workload: actual={delta:?}, expected={expected:?}"
        ));
    }
    Ok(delta)
}

fn check_work_success(result: &WorkResult) -> std::result::Result<(), String> {
    result
        .outcomes()
        .validate(result.attempted, 0)
        .map_err(|error| {
            let details = result
                .jobs
                .iter()
                .filter_map(|job| job.error.as_deref())
                .take(5)
                .collect::<Vec<_>>();
            format!("{error}; operation errors: {details:?}")
        })?;
    let (requests, tools, completions) = result.reference_counts();
    if (requests, tools, completions)
        != (result.attempted * 3, result.attempted * 2, result.attempted)
    {
        return Err(format!(
            "reference counts differ: requests={requests}, tools={tools}, completions={completions}, jobs={}",
            result.attempted
        ));
    }
    Ok(())
}

fn cpu_values(pid: u32, before: Option<f64>, window: Duration) -> Value {
    let after = crate::benchmark::process_cpu_seconds(pid);
    let elapsed = window.as_secs_f64();
    let used = match (before, after) {
        (Some(before), Some(after)) if after >= before => Some(after - before),
        _ => None,
    };
    let parallelism = std::thread::available_parallelism().map_or(1, |value| value.get());
    json!({
        "backend_cpu_seconds": used,
        "backend_cpu_cores": used.map(|value| value / elapsed),
        "backend_cpu_percent_of_one_core": used.map(|value| 100.0 * value / elapsed),
        "backend_cpu_percent_of_available_parallelism": used.map(|value| 100.0 * value / elapsed / parallelism as f64),
        "backend_cpu_normalization": "delta child total CPU time / measured wall window; also divided by available parallelism",
        "backend_cpu_unavailable_reason": if used.is_none() { Some("child CPU time is unsupported or could not be sampled") } else { None },
    })
}

#[allow(clippy::too_many_arguments)]
fn emit_trial(
    context: &MeasurementContext,
    options: &Options,
    mut parameters: WorkloadParameters,
    name: &str,
    trial: usize,
    durations: Vec<Duration>,
    sample_count: Option<usize>,
    outcomes: OutcomeCounts,
    window: Duration,
    values: Value,
    validation: std::result::Result<(), String>,
) -> Result<()> {
    parameters = parameters.with_trial(trial);
    let applied_warmup = parameters.custom["warm_up"]
        .as_u64()
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(options.warmup);
    let status = match &validation {
        Ok(()) => "success",
        Err(error) if error.starts_with("unsupported:") => "unsupported",
        Err(_) => "failed",
    };
    if let Err(error) = &validation {
        parameters.custom["error"] = json!(error);
    }
    let record = MeasurementRecord {
        name: name.to_owned(),
        parameters,
        metadata: context.metadata.clone(),
        samples: crate::benchmark::Samples::from_durations(durations),
        sample_count,
        outcomes,
        resources: None,
        timing_window: window,
        warm_up_iterations: applied_warmup,
        note: validation.as_ref().err().cloned(),
        status: status.to_owned(),
        values,
    };
    Record::Measurement(Box::new(record)).emit();
    validation.map_err(Into::into)
}

async fn warm_up_jobs(
    handshake: &BackendHandshake,
    provider: &Rc<MockProvider>,
    options: &Options,
    concurrency: usize,
) -> Result<()> {
    if options.warmup == 0 {
        return Ok(());
    }
    let warm = run_workload(
        handshake,
        provider,
        options,
        concurrency.min(options.warmup),
        options.warmup,
        WorkMode::Completion,
    )
    .await?;
    check_work_success(&warm).map_err(|error| format!("warm-up failed: {error}"))?;
    check_provider_counts(
        provider,
        warm.provider_before,
        warm.attempted,
        options.stream_chunks,
    )
    .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> { error.into() })?;
    Ok(())
}

async fn measure_startup(context: &MeasurementContext, options: &Options) -> Result<()> {
    let concurrency = options.clients.first().copied().ok_or("no client count")?;
    let parameters = context
        .parameters(options, "startup", concurrency, 1)
        .with_cache_state("warm OS cache from fixture preparation; new backend process")
        .with("fixture_completed_jobs", json!(options.jobs))
        .with("fixture_preparation", json!("outside startup timer"))
        .with(
            "readiness",
            json!("authenticated /api/v1/health and /api/v1/job-history?limit=1"),
        )
        .with(
            "inapplicable_options",
            json!([
                "iterations",
                "payloads",
                "outputs",
                "page_size",
                "writer_jobs",
                "clients: backend capacity only; no client workload"
            ]),
        );
    emit_begin(&parameters);
    let provider = Rc::new(MockProvider::start(provider_config(options))?);
    let data_dir = context.root.0.join("data");
    let (backend, _client, _) = spawn_ready(
        context,
        options,
        &provider,
        data_dir.clone(),
        concurrency,
        usize::MAX,
    )
    .await?;
    let handshake = backend.handshake().clone();
    let setup = run_workload(
        &handshake,
        &provider,
        options,
        concurrency,
        options.jobs,
        WorkMode::Completion,
    )
    .await?;
    let setup_validation = check_work_success(&setup).and_then(|_| {
        check_provider_counts(
            &provider,
            setup.provider_before,
            setup.attempted,
            options.stream_chunks,
        )
        .map(|_| ())
    });
    shutdown_backend(backend).await?;
    setup_validation.map_err(|error| -> Box<dyn std::error::Error + Send + Sync> {
        format!("startup fixture creation failed: {error}").into()
    })?;
    for warmup in 0..options.warmup {
        let (backend, _client, _) = spawn_ready(
            context,
            options,
            &provider,
            data_dir.clone(),
            concurrency,
            warmup,
        )
        .await?;
        let reopened = backend.handshake();
        let stable = reopened.executor == handshake.executor
            && reopened.llm_profile == handshake.llm_profile
            && reopened.agent_profile == handshake.agent_profile
            && reopened.project == handshake.project;
        shutdown_backend(backend).await?;
        if !stable {
            return Err("startup warm-up reopen changed fixture identities".into());
        }
    }
    for trial in 0..options.trials {
        let (backend, _client, duration) = spawn_ready(
            context,
            options,
            &provider,
            data_dir.clone(),
            concurrency,
            trial,
        )
        .await?;
        let reopened = backend.handshake();
        let stable = reopened.executor == handshake.executor
            && reopened.llm_profile == handshake.llm_profile
            && reopened.agent_profile == handshake.agent_profile
            && reopened.project == handshake.project;
        shutdown_backend(backend).await?;
        let values = json!({"duration_ms": duration.as_secs_f64() * 1000.0,
            "fixture_completed_jobs": setup.attempted, "clean_close": true,
            "cache_conditions": parameters.cache_state, "units": {"duration": "ms"}});
        emit_trial(
            context,
            options,
            parameters.clone(),
            "startup.ready",
            trial,
            vec![duration],
            None,
            OutcomeCounts::new(1),
            duration,
            values,
            if stable {
                Ok(())
            } else {
                Err("startup reopen created duplicate profiles or a duplicate project".into())
            },
        )?;
    }
    stop_provider(provider).await?;
    emit_section_complete(&parameters);
    Ok(())
}

fn overlap_check(result: &WorkResult, concurrency: usize) -> std::result::Result<(), String> {
    if concurrency > 1 && result.attempted >= concurrency && result.max_overlap < 2 {
        return Err(format!(
            "configured concurrency {concurrency} produced no observed overlap"
        ));
    }
    Ok(())
}

async fn measure_throughput(context: &MeasurementContext, options: &Options) -> Result<()> {
    let provider = Rc::new(MockProvider::start(provider_config(options))?);
    for &concurrency in &options.clients {
        let parameters = context
            .parameters(options, "throughput", concurrency, options.jobs)
            .with("requests_per_job", json!(3))
            .with("tool_calls_per_job", json!(2))
            .with(
                "metric",
                json!("validated completed jobs / measured wall second"),
            )
            .with("units", json!({"throughput": "jobs/s", "latency": "ms"}));
        emit_begin(&parameters);
        for trial in 0..options.trials {
            let (backend, _client, _) = spawn_ready(
                context,
                options,
                &provider,
                context.root.0.join(format!("data-{concurrency}-{trial}")),
                concurrency,
                trial,
            )
            .await?;
            let handshake = backend.handshake().clone();
            warm_up_jobs(&handshake, &provider, options, concurrency).await?;
            let cpu_before = crate::benchmark::process_cpu_seconds(backend.pid());
            let result = run_workload(
                &handshake,
                &provider,
                options,
                concurrency,
                options.jobs,
                WorkMode::Completion,
            )
            .await?;
            let mut values = cpu_values(backend.pid(), cpu_before, result.window);
            let outcomes = result.outcomes();
            let durations = result
                .jobs
                .iter()
                .filter(|job| job.error.is_none())
                .filter_map(|job| job.completion)
                .collect::<Vec<_>>();
            let (requests, tools, completions) = result.reference_counts();
            values["throughput_per_second"] =
                json!(outcomes.successful as f64 / result.window.as_secs_f64());
            values["configured_concurrency"] = json!(concurrency);
            values["observed_max_overlap"] = json!(result.max_overlap);
            values["model_requests"] = json!(requests);
            values["tool_calls"] = json!(tools);
            values["validated_completions"] = json!(completions);
            let validation = check_work_success(&result)
                .and_then(|_| overlap_check(&result, concurrency))
                .and_then(|_| {
                    check_provider_counts(
                        &provider,
                        result.provider_before,
                        result.attempted,
                        options.stream_chunks,
                    )
                    .map(|_| ())
                })
                .and_then(|_| {
                    if durations.len() == outcomes.successful {
                        Ok(())
                    } else {
                        Err("completion samples do not match successful jobs".into())
                    }
                });
            shutdown_backend(backend).await?;
            emit_trial(
                context,
                options,
                parameters.clone(),
                "throughput.jobs",
                trial,
                durations,
                None,
                outcomes,
                result.window,
                values,
                validation,
            )?;
        }
        emit_section_complete(&parameters);
    }
    stop_provider(provider).await?;
    Ok(())
}

async fn measure_admission(context: &MeasurementContext, options: &Options) -> Result<()> {
    let provider = Rc::new(MockProvider::start(provider_config(options))?);
    for &concurrency in &options.clients {
        let parameters = context.parameters(options, "admission", concurrency, options.jobs)
            .with("observation_mechanism", json!("POST /api/v1/jobs; poll GET /api/v1/admissions with the same Idempotency-Key while pending"))
            .with("completion_drain", json!("outside admission timing window"))
            .with("units", json!({"latency": "ms"}));
        emit_begin(&parameters);
        for trial in 0..options.trials {
            let (backend, client, _) = spawn_ready(
                context,
                options,
                &provider,
                context.root.0.join(format!("data-{concurrency}-{trial}")),
                concurrency,
                trial,
            )
            .await?;
            let handshake = backend.handshake().clone();
            warm_up_jobs(&handshake, &provider, options, concurrency).await?;
            let cpu_before = crate::benchmark::process_cpu_seconds(backend.pid());
            let mut result = run_workload(
                &handshake,
                &provider,
                options,
                concurrency,
                options.jobs,
                WorkMode::Admission,
            )
            .await?;
            let outcomes = result.outcomes();
            let durations = result
                .jobs
                .iter()
                .filter(|job| job.error.is_none())
                .filter_map(|job| job.admission)
                .collect::<Vec<_>>();
            let admission_validation = outcomes
                .validate(result.attempted, 0)
                .and_then(|_| overlap_check(&result, concurrency))
                .and_then(|_| {
                    if durations.len() == outcomes.successful {
                        Ok(())
                    } else {
                        Err("admission samples do not match confirmed admissions".into())
                    }
                });
            let mut values = cpu_values(backend.pid(), cpu_before, result.window);
            drain_admitted(&client, &provider, options, &mut result).await;
            let drain_validation = check_work_success(&result).and_then(|_| {
                check_provider_counts(
                    &provider,
                    result.provider_before,
                    result.attempted,
                    options.stream_chunks,
                )
                .map(|_| ())
            });
            let (requests, tools, completions) = result.reference_counts();
            values["operation_errors"] = json!(
                result
                    .jobs
                    .iter()
                    .filter_map(|job| job.error.clone())
                    .collect::<Vec<_>>()
            );
            values["configured_concurrency"] = json!(concurrency);
            values["observed_max_overlap"] = json!(result.max_overlap);
            values["pending_lookups"] = json!(
                result
                    .jobs
                    .iter()
                    .map(|job| job.pending_lookups)
                    .sum::<usize>()
            );
            values["drain_model_requests"] = json!(requests);
            values["drain_tool_calls"] = json!(tools);
            values["drain_validated_completions"] = json!(completions);
            let validation = admission_validation.and(drain_validation);
            shutdown_backend(backend).await?;
            emit_trial(
                context,
                options,
                parameters.clone(),
                "admission.latency",
                trial,
                durations,
                None,
                outcomes,
                result.window,
                values,
                validation,
            )?;
        }
        emit_section_complete(&parameters);
    }
    stop_provider(provider).await?;
    Ok(())
}

async fn measure_memory(context: &MeasurementContext, options: &Options) -> Result<()> {
    let provider = Rc::new(MockProvider::start(provider_config(options))?);
    let sample_interval = Duration::from_millis(20);
    for &concurrency in &options.clients {
        let parameters = context
            .parameters(options, "memory", concurrency, options.jobs)
            .with("sample_interval_ms", json!(sample_interval.as_millis()))
            .with("process_scope", json!("isolated backend child"))
            .with(
                "peak_definition",
                json!("maximum RSS observed by periodic sampling during the loaded window"),
            )
            .with("units", json!({"rss": "MiB", "cpu": "normalized cores"}));
        emit_begin(&parameters);
        for trial in 0..options.trials {
            let (backend, _client, _) = spawn_ready(
                context,
                options,
                &provider,
                context.root.0.join(format!("data-{concurrency}-{trial}")),
                concurrency,
                trial,
            )
            .await?;
            let handshake = backend.handshake().clone();
            let idle_rss = crate::benchmark::process_rss_bytes(backend.pid());
            warm_up_jobs(&handshake, &provider, options, concurrency).await?;
            let stop = Arc::new(AtomicBool::new(false));
            let sampler_stop = stop.clone();
            let pid = backend.pid();
            let sampler_started = Instant::now();
            let sampler = ntex::rt::spawn_blocking(move || {
                let mut samples = Vec::new();
                while !sampler_stop.load(Ordering::Relaxed) {
                    if let Some(rss) = crate::benchmark::process_rss_bytes(pid) {
                        samples.push((sampler_started.elapsed(), rss));
                    }
                    std::thread::sleep(sample_interval);
                }
                if let Some(rss) = crate::benchmark::process_rss_bytes(pid) {
                    samples.push((sampler_started.elapsed(), rss));
                }
                samples
            });
            let cpu_before = crate::benchmark::process_cpu_seconds(pid);
            let result = run_workload(
                &handshake,
                &provider,
                options,
                concurrency,
                options.jobs,
                WorkMode::Completion,
            )
            .await?;
            stop.store(true, Ordering::Relaxed);
            let samples = sampler.await.map_err(|_| "memory sampler panicked")?;
            let window_start = result.started.saturating_duration_since(sampler_started);
            let window_end = window_start + result.window;
            let samples = samples
                .into_iter()
                .filter(|(at, _)| *at >= window_start && *at <= window_end)
                .collect::<Vec<_>>();
            let sample_window = result.window;
            let peak = samples.iter().map(|sample| sample.1).max();
            let outcomes = result.outcomes();
            let (requests, tools, completions) = result.reference_counts();
            let mut values = cpu_values(pid, cpu_before, sample_window);
            values["idle_rss_bytes"] = json!(idle_rss);
            values["peak_rss_bytes"] = json!(peak);
            values["bytes"] = json!(peak);
            values["rss_sample_count"] = json!(samples.len());
            values["rss_sample_window_s"] = json!(sample_window.as_secs_f64());
            values["rss_unavailable_reason"] = if peak.is_none() {
                json!("child RSS could not be sampled on this platform")
            } else {
                Value::Null
            };
            values["configured_concurrency"] = json!(concurrency);
            values["observed_max_overlap"] = json!(result.max_overlap);
            values["model_requests"] = json!(requests);
            values["tool_calls"] = json!(tools);
            values["validated_completions"] = json!(completions);
            let validation = check_work_success(&result)
                .and_then(|_| overlap_check(&result, concurrency))
                .and_then(|_| {
                    check_provider_counts(
                        &provider,
                        result.provider_before,
                        result.attempted,
                        options.stream_chunks,
                    )
                    .map(|_| ())
                })
                .and_then(|_| {
                    if idle_rss.is_some() && peak.is_some() && !samples.is_empty() {
                        Ok(())
                    } else if !cfg!(any(target_os = "macos", target_os = "linux")) {
                        Err(
                            "unsupported: child RSS sampling is unavailable on this platform"
                                .into(),
                        )
                    } else {
                        Err("required idle/windowed RSS samples are unavailable".into())
                    }
                });
            shutdown_backend(backend).await?;
            emit_trial(
                context,
                options,
                parameters.clone(),
                "memory.footprint",
                trial,
                Vec::new(),
                Some(samples.len()),
                outcomes,
                sample_window,
                values,
                validation,
            )?;
        }
        emit_section_complete(&parameters);
    }
    stop_provider(provider).await?;
    Ok(())
}

async fn measure_streaming(context: &MeasurementContext, options: &Options) -> Result<()> {
    let provider = Rc::new(MockProvider::start(provider_config(options))?);
    for &concurrency in &options.clients {
        let parameters = context.parameters(options, "streaming", concurrency, options.jobs)
            .with("markers_per_job", json!(options.stream_chunks))
            .with("pace_ms", json!(options.stream_pace.as_millis()))
            .with("clock", json!("shared process monotonic std::time::Instant"))
            .with("subscription_gate", json!("upstream final markers wait for downstream SSE subscription"))
            .with("delivery_definition", json!("upstream marker yield to downstream SSE observation, including backend persistence and buffering"))
            .with("units", json!({"latency": "ms"}));
        emit_begin(&parameters);
        for trial in 0..options.trials {
            let (backend, _client, _) = spawn_ready(
                context,
                options,
                &provider,
                context.root.0.join(format!("data-{concurrency}-{trial}")),
                concurrency,
                trial,
            )
            .await?;
            let handshake = backend.handshake().clone();
            warm_up_jobs(&handshake, &provider, options, concurrency).await?;
            let cpu_before = crate::benchmark::process_cpu_seconds(backend.pid());
            let result = run_workload(
                &handshake,
                &provider,
                options,
                concurrency,
                options.jobs,
                WorkMode::Streaming,
            )
            .await?;
            let outcomes = result.outcomes();
            let successful = result.jobs.iter().filter(|job| job.error.is_none());
            let delivery = successful
                .clone()
                .flat_map(|job| job.delivery.iter().copied())
                .collect::<Vec<_>>();
            let first_content = successful
                .clone()
                .filter_map(|job| job.first_content)
                .collect::<Vec<_>>();
            let content_span = successful
                .clone()
                .filter_map(|job| job.content_span)
                .collect::<Vec<_>>();
            let completion = successful
                .filter_map(|job| job.completion)
                .collect::<Vec<_>>();
            let (requests, tools, completions) = result.reference_counts();
            let mut values = cpu_values(backend.pid(), cpu_before, result.window);
            values["configured_concurrency"] = json!(concurrency);
            values["observed_max_overlap"] = json!(result.max_overlap);
            values["model_requests"] = json!(requests);
            values["tool_calls"] = json!(tools);
            values["validated_completions"] = json!(completions);
            values["delivery_samples"] = json!(delivery.len());
            values["expected_delivery_samples"] =
                json!(outcomes.successful * options.stream_chunks);
            let validation = check_work_success(&result)
                .and_then(|_| overlap_check(&result, concurrency))
                .and_then(|_| {
                    check_provider_counts(
                        &provider,
                        result.provider_before,
                        result.attempted,
                        options.stream_chunks,
                    )
                    .map(|_| ())
                })
                .and_then(|_| {
                    if delivery.len() == outcomes.successful * options.stream_chunks
                        && first_content.len() == outcomes.successful
                        && content_span.len() == outcomes.successful
                        && completion.len() == outcomes.successful
                    {
                        Ok(())
                    } else {
                        Err(
                            "streaming samples do not reconcile with validated jobs and markers"
                                .into(),
                        )
                    }
                });
            shutdown_backend(backend).await?;
            emit_trial(
                context,
                options,
                parameters.clone(),
                "streaming.delivery",
                trial,
                delivery,
                None,
                outcomes,
                result.window,
                values,
                validation,
            )?;
            for (name, samples) in [
                ("streaming.first_content", first_content),
                ("streaming.content_span", content_span),
                ("streaming.completion", completion),
            ] {
                emit_trial(
                    context,
                    options,
                    parameters.clone(),
                    name,
                    trial,
                    samples,
                    None,
                    outcomes,
                    result.window,
                    json!({"scope": name}),
                    Ok(()),
                )?;
            }
        }
        emit_section_complete(&parameters);
    }
    stop_provider(provider).await?;
    Ok(())
}

async fn measure_database(context: &MeasurementContext, options: &Options) -> Result<()> {
    let provider = Rc::new(MockProvider::start(provider_config(options))?);
    for &concurrency in &options.clients {
        let parameters = context
            .parameters(options, "database", concurrency, options.jobs)
            .with("warm_up", json!(0))
            .with(
                "warm_up_explanation",
                json!("no warm-up jobs are included in the footprint fixture"),
            )
            .with("database_file", json!("ocg.redb"))
            .with(
                "excluded",
                json!(["logs", "attachments", "unrelated files"]),
            )
            .with(
                "baseline",
                json!("equivalent configuration, zero completed workload jobs, clean close"),
            )
            .with("units", json!({"footprint": "MiB", "growth": "KiB/job"}));
        emit_begin(&parameters);
        for trial in 0..options.trials {
            let baseline_dir = context
                .root
                .0
                .join(format!("baseline-{concurrency}-{trial}"));
            let (baseline, _client, _) = spawn_ready(
                context,
                options,
                &provider,
                baseline_dir.clone(),
                concurrency,
                trial,
            )
            .await?;
            shutdown_backend(baseline).await?;
            let baseline_bytes = ntex::rt::spawn_blocking(move || -> Result<u64> {
                let directory = DataDirectory::bootstrap(baseline_dir.into_os_string())?;
                Ok(crate::benchmark::ocg_database_bytes(&directory)?)
            })
            .await??;
            let loaded_dir = context.root.0.join(format!("loaded-{concurrency}-{trial}"));
            let (backend, _client, _) = spawn_ready(
                context,
                options,
                &provider,
                loaded_dir.clone(),
                concurrency,
                trial,
            )
            .await?;
            let handshake = backend.handshake().clone();
            let result = run_workload(
                &handshake,
                &provider,
                options,
                concurrency,
                options.jobs,
                WorkMode::Completion,
            )
            .await?;
            let outcomes = result.outcomes();
            let (requests, tools, completions) = result.reference_counts();
            let validation = check_work_success(&result).and_then(|_| {
                check_provider_counts(
                    &provider,
                    result.provider_before,
                    result.attempted,
                    options.stream_chunks,
                )
                .map(|_| ())
            });
            shutdown_backend(backend).await?;
            let loaded_bytes = ntex::rt::spawn_blocking(move || -> Result<u64> {
                let directory = DataDirectory::bootstrap(loaded_dir.into_os_string())?;
                Ok(crate::benchmark::ocg_database_bytes(&directory)?)
            })
            .await??;
            let growth = loaded_bytes.checked_sub(baseline_bytes);
            let kib_per_job = growth.map(|growth| growth as f64 / 1024.0 / completions as f64);
            let values = json!({"bytes": loaded_bytes, "database_bytes": loaded_bytes,
            "baseline_bytes": baseline_bytes, "growth_bytes": growth,
            "growth_kib_per_completed_job": kib_per_job, "completed_jobs": completions,
            "model_requests": requests, "tool_calls": tools, "clean_close": true,
            "fixture_completed_jobs": options.jobs,
            "growth_reference_completed_jobs": completions});
            let validation = validation.and_then(|_| {
                if completions == options.jobs && growth.is_some() && kib_per_job.is_some() {
                    Ok(())
                } else {
                    Err(
                        "database baseline/growth does not match the validated completed workload"
                            .into(),
                    )
                }
            });
            emit_trial(
                context,
                options,
                parameters.clone(),
                "database.footprint",
                trial,
                Vec::new(),
                Some(1),
                outcomes,
                result.window,
                values,
                validation,
            )?;
        }
        emit_section_complete(&parameters);
    }
    stop_provider(provider).await?;
    Ok(())
}

struct HistoryRead {
    cursor: bool,
    duration: Option<Duration>,
    error: Option<String>,
    writers_active: bool,
}

struct HistoryReads {
    records: Vec<HistoryRead>,
    attempted: usize,
    window: Duration,
    max_overlap: usize,
}

impl HistoryReads {
    fn outcomes(&self) -> OutcomeCounts {
        let mut outcomes = OutcomeCounts::default();
        for record in &self.records {
            if record.error.is_some() {
                outcomes.record_failure(Outcome::Failed);
            } else {
                outcomes.record_success();
            }
        }
        outcomes.unfinished = self.attempted.saturating_sub(self.records.len());
        outcomes.attempted = self.attempted;
        outcomes
    }
}

fn validate_history_page(
    page: &Value,
    cursor: bool,
    page_size: usize,
    first_ids: &BTreeSet<String>,
) -> std::result::Result<(), String> {
    let jobs = page["jobs"]
        .as_array()
        .ok_or_else(|| format!("history page omitted jobs: {page}"))?;
    let has_more = page["has_more"]
        .as_bool()
        .ok_or_else(|| format!("history page omitted has_more: {page}"))?;
    if jobs.len() != page_size || (!cursor && !has_more) {
        return Err(format!(
            "history page has {} rows and has_more={has_more}; expected {page_size} rows and a first-page continuation: {page}",
            jobs.len()
        ));
    }
    if has_more != page["next_before"].as_str().is_some() {
        return Err(format!(
            "history continuation does not match has_more: {page}"
        ));
    }
    if cursor
        && jobs
            .iter()
            .any(|job| job["id"].as_str().is_some_and(|id| first_ids.contains(id)))
    {
        return Err("cursor history page duplicated a first-page job".into());
    }
    if jobs.iter().any(|job| job["id"].as_str().is_none()) {
        return Err(format!("history page contains a malformed job: {page}"));
    }
    Ok(())
}

async fn run_history_readers(
    handshake: &BackendHandshake,
    options: &Options,
    concurrency: usize,
    cursor: &str,
    first_ids: BTreeSet<String>,
    background_active: Arc<AtomicUsize>,
) -> Result<HistoryReads> {
    let (start_sender, start_receiver) = flume::bounded(concurrency);
    let (ready_sender, ready_receiver) = flume::bounded(concurrency);
    let overlap = Arc::new(Overlap {
        active: AtomicUsize::new(0),
        maximum: AtomicUsize::new(0),
    });
    let mut workers = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let handshake = handshake.clone();
        let options = options.clone();
        let start = start_receiver.clone();
        let ready = ready_sender.clone();
        let overlap = overlap.clone();
        let cursor = cursor.to_owned();
        let first_ids = first_ids.clone();
        let background_active = background_active.clone();
        workers.push(ntex::rt::spawn(async move {
            let client = BenchmarkClient::new(
                handshake.address.clone(),
                handshake.token.clone(),
                options.deadline,
            )
            .await;
            ready
                .send_async(())
                .await
                .map_err(|error| format!("history readiness barrier failed: {error}"))?;
            start
                .recv_async()
                .await
                .map_err(|error| format!("history start barrier failed: {error}"))?;
            let client = client?;
            let mut records = Vec::with_capacity(options.iterations * 2);
            for _ in 0..options.iterations {
                for use_cursor in [false, true] {
                    let _active = overlap.enter();
                    let began = Instant::now();
                    let result = client
                        .history_page(use_cursor.then_some(cursor.as_str()), options.page_size)
                        .await
                        .and_then(|page| {
                            validate_history_page(&page, use_cursor, options.page_size, &first_ids)
                                .map(|_| page)
                        });
                    records.push(HistoryRead {
                        cursor: use_cursor,
                        duration: result.as_ref().ok().map(|_| began.elapsed()),
                        error: result.err(),
                        writers_active: background_active.load(Ordering::Relaxed) > 0,
                    });
                }
            }
            Ok::<_, String>(records)
        }));
    }
    drop(start_receiver);
    drop(ready_sender);
    for _ in 0..concurrency {
        ready_receiver
            .recv_async()
            .await
            .map_err(|error| format!("history readiness failed: {error}"))?;
    }
    let started = Instant::now();
    for _ in 0..concurrency {
        start_sender
            .send_async(())
            .await
            .map_err(|error| format!("history start failed: {error}"))?;
    }
    let mut records = Vec::new();
    for worker in workers {
        records.extend(
            worker
                .await
                .map_err(|_| "history reader panicked")?
                .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> { error.into() })?,
        );
    }
    Ok(HistoryReads {
        records,
        attempted: concurrency * options.iterations * 2,
        window: started.elapsed(),
        max_overlap: overlap.maximum.load(Ordering::Relaxed),
    })
}

#[derive(Default)]
struct BackgroundTotals {
    attempted: usize,
    successful: usize,
    failed: usize,
    requests: usize,
    tools: usize,
    completions: usize,
    max_overlap: usize,
}

async fn measure_history(context: &MeasurementContext, options: &Options) -> Result<()> {
    if options.jobs < options.page_size * 2 {
        return Err(format!(
            "history needs at least two pages: --jobs {} is smaller than 2 * --page-size {}",
            options.jobs, options.page_size
        )
        .into());
    }
    let provider = Rc::new(MockProvider::start(provider_config(options))?);
    for &concurrency in &options.clients {
        let parameters = context
            .parameters(
                options,
                "history",
                concurrency,
                concurrency * options.iterations * 2,
            )
            .with("route", json!("/api/v1/job-history"))
            .with("page_size", json!(options.page_size))
            .with(
                "cursor_selection",
                json!("next_before from a fixed warm first page; fixed case is before=<cursor>"),
            )
            .with("fixture_completed_jobs", json!(options.jobs))
            .with("reader_concurrency", json!(concurrency))
            .with("background_writers", json!(concurrency))
            .with("background_jobs_per_batch", json!(options.writer_jobs))
            .with(
                "query_mix",
                json!(["first_page", "next_before_cursor_page"]),
            )
            .with("units", json!({"latency": "ms"}));
        emit_begin(&parameters);
        for trial in 0..options.trials {
            let (backend, client, _) = spawn_ready(
                context,
                options,
                &provider,
                context.root.0.join(format!("data-{concurrency}-{trial}")),
                concurrency,
                trial,
            )
            .await?;
            let handshake = backend.handshake().clone();
            let setup = run_workload(
                &handshake,
                &provider,
                options,
                concurrency,
                options.jobs,
                WorkMode::Completion,
            )
            .await?;
            let setup_validation = check_work_success(&setup);
            let warm_page = client
                .history_page(None, options.page_size)
                .await
                .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> { error.into() })?;
            validate_history_page(&warm_page, false, options.page_size, &BTreeSet::new())
                .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> { error.into() })?;
            let cursor = warm_page["next_before"]
                .as_str()
                .ok_or("warm history page has no continuation")?
                .to_owned();
            let first_ids = warm_page["jobs"]
                .as_array()
                .ok_or("warm history page has no jobs")?
                .iter()
                .filter_map(|job| job["id"].as_str().map(str::to_owned))
                .collect::<BTreeSet<_>>();
            for _ in 0..options.warmup {
                for use_cursor in [false, true] {
                    let page = client
                        .history_page(use_cursor.then_some(cursor.as_str()), options.page_size)
                        .await
                        .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> {
                            error.into()
                        })?;
                    validate_history_page(&page, use_cursor, options.page_size, &first_ids)
                        .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> {
                            error.into()
                        })?;
                }
            }
            let stop = Arc::new(AtomicBool::new(false));
            let active = Arc::new(AtomicUsize::new(0));
            let writer_stop = stop.clone();
            let writer_active = active.clone();
            let writer_handshake = handshake.clone();
            let writer_options = options.clone();
            let writer_provider = provider.clone();
            let initial_writer_requests = provider.counters().snapshot().requests;
            let writer = ntex::rt::spawn(async move {
                let mut totals = BackgroundTotals::default();
                while !writer_stop.load(Ordering::Relaxed) {
                    writer_active.fetch_add(1, Ordering::Relaxed);
                    let batch = run_workload(
                        &writer_handshake,
                        &writer_provider,
                        &writer_options,
                        concurrency,
                        writer_options.writer_jobs,
                        WorkMode::Completion,
                    )
                    .await;
                    writer_active.fetch_sub(1, Ordering::Relaxed);
                    let batch = batch?;
                    let outcomes = batch.outcomes();
                    totals.attempted += batch.attempted;
                    totals.successful += outcomes.successful;
                    totals.failed += outcomes.failed + outcomes.timed_out + outcomes.unfinished;
                    let counts = batch.reference_counts();
                    totals.requests += counts.0;
                    totals.tools += counts.1;
                    totals.completions += counts.2;
                    totals.max_overlap = totals.max_overlap.max(batch.max_overlap);
                }
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(totals)
            });
            let writer_deadline = Instant::now() + options.deadline;
            while provider.counters().snapshot().requests == initial_writer_requests {
                if Instant::now() >= writer_deadline {
                    stop.store(true, Ordering::Relaxed);
                    return Err(
                        "history background writer did not start before the deadline".into(),
                    );
                }
                ntex::time::sleep(Duration::from_millis(1)).await;
            }
            let writer_requests_before_reads =
                provider.counters().snapshot().requests - initial_writer_requests;
            let cpu_before = crate::benchmark::process_cpu_seconds(backend.pid());
            let reads = run_history_readers(
                &handshake,
                options,
                concurrency,
                &cursor,
                first_ids,
                active.clone(),
            )
            .await?;
            let mut values = cpu_values(backend.pid(), cpu_before, reads.window);
            stop.store(true, Ordering::Relaxed);
            let background = writer
                .await
                .map_err(|_| "history background writer panicked")??;
            let outcomes = reads.outcomes();
            let samples = reads
                .records
                .iter()
                .filter(|record| record.error.is_none())
                .filter_map(|record| record.duration)
                .collect::<Vec<_>>();
            let active_reads = reads
                .records
                .iter()
                .filter(|record| record.writers_active)
                .count();
            values["configured_reader_concurrency"] = json!(concurrency);
            values["observed_reader_overlap"] = json!(reads.max_overlap);
            values["background_writer_concurrency"] = json!(concurrency);
            values["background_observed_job_overlap"] = json!(background.max_overlap);
            values["background_jobs_attempted"] = json!(background.attempted);
            values["background_model_requests_started_before_reads"] =
                json!(writer_requests_before_reads);
            values["background_jobs_successful"] = json!(background.successful);
            values["background_jobs_failed"] = json!(background.failed);
            values["background_model_requests"] = json!(background.requests);
            values["background_tool_calls"] = json!(background.tools);
            values["background_validated_completions"] = json!(background.completions);
            values["reads_with_background_writer_active"] = json!(active_reads);
            values["fixture_completed_jobs"] = json!(setup.attempted);
            let total_jobs = setup.attempted + background.attempted;
            let validation = setup_validation
                .and_then(|_| outcomes.validate(reads.attempted, 0))
                .and_then(|_| {
                    if reads.max_overlap >= 2 || concurrency == 1 {
                        Ok(())
                    } else {
                        Err("configured history readers did not overlap".into())
                    }
                })
                .and_then(|_| {
                    if active_reads > 0 {
                        Ok(())
                    } else {
                        Err("no history read overlapped background writer load".into())
                    }
                })
                .and_then(|_| {
                    if background.failed == 0
                        && background.successful == background.attempted
                        && background.completions == background.attempted
                    {
                        Ok(())
                    } else {
                        Err("background reference workload failed".into())
                    }
                })
                .and_then(|_| {
                    check_provider_counts(
                        &provider,
                        setup.provider_before,
                        total_jobs,
                        options.stream_chunks,
                    )
                    .map(|_| ())
                })
                .and_then(|_| {
                    if samples.len() == outcomes.successful {
                        Ok(())
                    } else {
                        Err("history latency samples do not match successful reads".into())
                    }
                });
            shutdown_backend(backend).await?;
            emit_trial(
                context,
                options,
                parameters.clone(),
                "history.read",
                trial,
                samples,
                None,
                outcomes,
                reads.window,
                values,
                validation,
            )?;
            for use_cursor in [false, true] {
                let samples = reads
                    .records
                    .iter()
                    .filter(|record| record.cursor == use_cursor && record.error.is_none())
                    .filter_map(|record| record.duration)
                    .collect::<Vec<_>>();
                emit_trial(
                    context,
                    options,
                    parameters.clone(),
                    if use_cursor {
                        "history.read.cursor"
                    } else {
                        "history.read.first"
                    },
                    trial,
                    samples,
                    None,
                    OutcomeCounts::new(concurrency * options.iterations),
                    reads.window,
                    json!({"scope": if use_cursor { "cursor_page" } else { "first_page" }}),
                    Ok(()),
                )?;
            }
        }
        emit_section_complete(&parameters);
    }
    stop_provider(provider).await?;
    Ok(())
}

#[cfg(unix)]
struct SearchRead {
    class: &'static str,
    duration: Option<Duration>,
    error: Option<String>,
}

#[cfg(unix)]
pub(crate) fn validate_query(
    class: &str,
    value: &Value,
    files: usize,
) -> std::result::Result<usize, String> {
    let results = value["results"]
        .as_array()
        .ok_or_else(|| format!("search {class} omitted results: {value}"))?;
    let expected = match class {
        "rare" | "and" | "phrase" => Some(1),
        "missing" | "broad_phrase_zero" => Some(0),
        "common" | "broad_phrase" => Some(files.min(20)),
        _ => None,
    };
    if expected != Some(results.len()) {
        return Err(format!(
            "search {class} returned {} results, expected {expected:?}: {value}",
            results.len()
        ));
    }
    if matches!(class, "rare" | "and" | "phrase")
        && !results[0].to_string().contains("unit00000.rs")
    {
        return Err(format!("search {class} matched the wrong file: {value}"));
    }
    Ok(results.len())
}

#[cfg(unix)]
fn measure_search(
    context: &MeasurementContext,
    options: &Options,
    concurrency: usize,
) -> Result<()> {
    use ocg::service::fts::{FtsLimits, WorkspaceFts};
    use ocg::system::executor::workspace_read::Workspace;
    let source = context
        .root
        .0
        .join(format!("search-workspace-{concurrency}"));
    std::fs::create_dir(&source)?;
    crate::corpus(&source, options.files, [512, 2_048, 4_096, 9_216])?;
    let actual_files = crate::fixture::count_fixture_files(&source)?;
    let actual_bytes = crate::fixture::measure_fixture_bytes(&source)?;
    if actual_files != options.files || actual_bytes == 0 {
        return Err(
            format!("invalid search corpus: files={actual_files}, bytes={actual_bytes}").into(),
        );
    }
    let directory = DataDirectory::bootstrap(
        context
            .root
            .0
            .join(format!("search-data-{concurrency}"))
            .into_os_string(),
    )?;
    let workspace = Workspace::authorize(&source, &directory)?;
    let limits = FtsLimits::default();
    let parameters = context
        .parameters(
            options,
            "search",
            concurrency,
            concurrency * options.iterations * crate::SEARCH_QUERIES.len(),
        )
        .with("scope", json!("fixed_query_mix"))
        .with(
            "query_mix",
            json!(crate::SEARCH_QUERIES.map(|(class, _)| class)),
        )
        .with("query_concurrency", json!(concurrency))
        .with("result_limit", json!(20))
        .with("fixture_files", json!(actual_files))
        .with("fixture_bytes", json!(actual_bytes))
        .with(
            "cache_state",
            json!("warm index; warm OS file cache after build"),
        )
        .with(
            "build_scope",
            json!("direct production WorkspaceFts API, not HTTP"),
        )
        .with("units", json!({"latency": "ms"}));
    emit_begin(&parameters);
    for trial in 0..options.trials {
        let index = WorkspaceFts::new(workspace.clone(), limits);
        let build_started = Instant::now();
        let built = index.build()?;
        let build_duration = build_started.elapsed();
        if !built.coverage.complete
            || built.coverage.files_indexed != actual_files
            || built.coverage.content_bytes_indexed == 0
            || built.coverage.skipped.too_large != 0
        {
            return Err(
                format!("search fixture was not fully indexed: {:?}", built.coverage).into(),
            );
        }
        emit_trial(
            context,
            options,
            parameters.clone(),
            "search.build",
            trial,
            vec![build_duration],
            None,
            OutcomeCounts::new(1),
            build_duration,
            json!({"files_indexed": built.coverage.files_indexed, "content_bytes_indexed": built.coverage.content_bytes_indexed,
                "tokens_indexed": built.coverage.tokens_indexed, "memory_bytes_approx": built.memory_bytes_approx,
                "complete": built.coverage.complete}),
            Ok(()),
        )?;
        for &(class, query) in &crate::SEARCH_QUERIES {
            for _ in 0..options.warmup {
                let value = index.search(query, 20)?;
                validate_query(class, &value, actual_files).map_err(
                    |error| -> Box<dyn std::error::Error + Send + Sync> { error.into() },
                )?;
            }
        }
        let index = Arc::new(index);
        let barrier = Arc::new(std::sync::Barrier::new(concurrency + 1));
        let overlap = Arc::new(Overlap {
            active: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
        });
        let (records, window) = std::thread::scope(|scope| -> Result<_> {
            let mut workers = Vec::with_capacity(concurrency);
            for _ in 0..concurrency {
                let index = index.clone();
                let barrier = barrier.clone();
                let overlap = overlap.clone();
                workers.push(scope.spawn(move || {
                    let mut records = Vec::new();
                    barrier.wait();
                    for _ in 0..options.iterations {
                        for &(class, query) in &crate::SEARCH_QUERIES {
                            let _active = overlap.enter();
                            let began = Instant::now();
                            let result = index
                                .search(query, 20)
                                .map_err(|error| error.to_string())
                                .and_then(|value| {
                                    validate_query(class, &value, actual_files).map(|_| value)
                                });
                            records.push(SearchRead {
                                class,
                                duration: result.as_ref().ok().map(|_| began.elapsed()),
                                error: result.err().map(|error| error.to_string()),
                            });
                        }
                    }
                    records
                }));
            }
            let started = Instant::now();
            barrier.wait();
            let mut records = Vec::new();
            for worker in workers {
                records.extend(worker.join().map_err(|_| "search query worker panicked")?);
            }
            Ok((records, started.elapsed()))
        })?;
        let mut outcomes = OutcomeCounts::default();
        let mut samples = Vec::new();
        let mut per_class: BTreeMap<&str, Vec<Duration>> = BTreeMap::new();
        for record in &records {
            if record.error.is_some() {
                outcomes.record_failure(Outcome::Failed);
            } else {
                outcomes.record_success();
                if let Some(duration) = record.duration {
                    samples.push(duration);
                    per_class.entry(record.class).or_default().push(duration);
                }
            }
        }
        outcomes.attempted = concurrency * options.iterations * crate::SEARCH_QUERIES.len();
        outcomes.unfinished = outcomes.attempted.saturating_sub(records.len());
        let observed_overlap = overlap.maximum.load(Ordering::Relaxed);
        let errors = records
            .iter()
            .filter_map(|record| record.error.as_deref())
            .collect::<Vec<_>>();
        let values = json!({"configured_query_concurrency": concurrency, "observed_query_overlap": observed_overlap,
            "files_indexed": built.coverage.files_indexed, "content_bytes_indexed": built.coverage.content_bytes_indexed,
            "errors": errors,
            "query_mix": "each of 7 validated classes once per iteration per client", "serial": concurrency == 1});
        let validation = outcomes.validate(outcomes.attempted, 0).and_then(|_| {
            if samples.len() == outcomes.successful
                && per_class.len() == crate::SEARCH_QUERIES.len()
                && (concurrency == 1 || observed_overlap >= 2)
            {
                Ok(())
            } else {
                Err("search samples, classes, or observed overlap do not reconcile".into())
            }
        });
        emit_trial(
            context,
            options,
            parameters.clone(),
            "search.latency",
            trial,
            samples,
            None,
            outcomes,
            window,
            values,
            validation,
        )?;
        for (class, samples) in per_class {
            emit_trial(
                context,
                options,
                parameters.clone().with("scope", json!(class)),
                &format!("search.latency.{class}"),
                trial,
                samples,
                None,
                OutcomeCounts::new(concurrency * options.iterations),
                window,
                json!({"query_class": class}),
                Ok(()),
            )?;
        }
        let rebuild_started = Instant::now();
        let rebuilt = index.rebuild()?;
        let rebuild_duration = rebuild_started.elapsed();
        if rebuilt.coverage.files_indexed != actual_files || !rebuilt.coverage.complete {
            return Err(format!("search rebuild lost coverage: {:?}", rebuilt.coverage).into());
        }
        emit_trial(
            context,
            options,
            parameters.clone(),
            "search.rebuild",
            trial,
            vec![rebuild_duration],
            None,
            OutcomeCounts::new(1),
            rebuild_duration,
            json!({"files_indexed": rebuilt.coverage.files_indexed, "content_bytes_indexed": rebuilt.coverage.content_bytes_indexed}),
            Ok(()),
        )?;
    }
    emit_section_complete(&parameters);
    Ok(())
}

#[cfg(not(unix))]
fn measure_search(
    _context: &MeasurementContext,
    _options: &Options,
    _concurrency: usize,
) -> Result<()> {
    Err("unsupported: workspace search requires Unix production FTS".into())
}

pub(super) async fn run(options: &Options) -> Result<()> {
    let context = MeasurementContext::new(options).await?;
    let result = match options.section.as_str() {
        "startup" => measure_startup(&context, options).await,
        "memory" => measure_memory(&context, options).await,
        "admission" => measure_admission(&context, options).await,
        "throughput" => measure_throughput(&context, options).await,
        "streaming" => measure_streaming(&context, options).await,
        "history" => measure_history(&context, options).await,
        "database" => measure_database(&context, options).await,
        "search" => {
            let owned = context;
            let search_options = options.clone();
            let (context, result) = ntex::rt::spawn_blocking(move || {
                let result = search_options.clients.iter().try_for_each(|&concurrency| {
                    measure_search(&owned, &search_options, concurrency)
                });
                (owned, result)
            })
            .await?;
            return finish_section(context, options, result);
        }
        other => return Err(format!("unknown measurement section: {other}").into()),
    };
    finish_section(context, options, result)
}

fn finish_section(
    context: MeasurementContext,
    options: &Options,
    result: Result<()>,
) -> Result<()> {
    if let Err(error) = &result {
        let mut parameters = context.parameters(
            options,
            &options.section,
            options.clients.first().copied().unwrap_or(1),
            0,
        );
        parameters.custom["trial"] = json!(0);
        parameters.custom["phase"] = json!("section/setup");
        let record = MeasurementRecord {
            name: canonical_metric(&options.section).to_owned(),
            parameters,
            metadata: context.metadata.clone(),
            samples: crate::benchmark::Samples::new(),
            sample_count: Some(0),
            outcomes: OutcomeCounts::default(),
            resources: None,
            timing_window: Duration::ZERO,
            warm_up_iterations: options.warmup,
            note: Some(error.to_string()),
            status: if error.to_string().starts_with("unsupported:") {
                "unsupported"
            } else {
                "failed"
            }
            .into(),
            values: json!({"error": error.to_string()}),
        };
        Record::Measurement(Box::new(record)).emit();
    }
    result
}
