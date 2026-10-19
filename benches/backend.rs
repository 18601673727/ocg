use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use ocg::data_directory::DataDirectory;
use ocg::database::{DatabaseHandle, DatabaseWorker};
use ocg::persistence::{
    CapabilitySet, ConfigBody, ConnectionConfig, DependencyCondition, ExecutionConfiguration,
    ExecutionOperation, ExecutionPoolConfig, ExecutorId, FinalResult, IntermediateOutputKind,
    JobFinalization, JobId, JobInputDraft, LlmTransport, ModelConfig, NewConfig, NewExecutionPool,
    NewExecutor, NewJob, NewJobOutput, OutcomeCause, OutcomeKind,
};
use redb::{Database, ReadableDatabase, ReadableTableMetadata, TableDefinition};
use serde_json::{Value, json};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

struct Unpark(std::thread::Thread);

impl Wake for Unpark {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
    loop {
        if let Poll::Ready(value) = future.as_mut().poll(&mut Context::from_waker(&waker)) {
            return value;
        }
        std::thread::park();
    }
}

struct TemporaryRoot(PathBuf);

impl TemporaryRoot {
    fn new(directory: &Path) -> Result<Self> {
        let stamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let root = directory.join(format!("ocg-backend-{}-{stamp}", std::process::id()));
        std::fs::create_dir(&root)?;
        Ok(Self(root))
    }
}

impl Drop for TemporaryRoot {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_dir_all(&self.0) {
            eprintln!("benchmark cleanup failed for {}: {error}", self.0.display());
        }
    }
}

struct Options {
    section: String,
    iterations: usize,
    jobs: usize,
    files: usize,
    clients: Vec<usize>,
    payloads: Vec<usize>,
    outputs: Vec<usize>,
    scheduler_jobs: Vec<usize>,
    provider_jobs: usize,
    directory: PathBuf,
}

/// One comma-separated list of positive sizes, as `--clients 1,8` carries.
fn sizes(argument: &str) -> Result<Vec<usize>> {
    let mut values = Vec::new();
    for part in argument.split(',') {
        let value: usize = part.trim().parse()?;
        if value == 0 {
            return Err("benchmark sizes must be positive".into());
        }
        values.push(value);
    }
    if values.is_empty() {
        return Err("a benchmark list must name at least one size".into());
    }
    Ok(values)
}

impl Options {
    fn parse() -> Result<Option<Self>> {
        let mut options = Self {
            directory: std::env::current_dir()?.join("target/backend-bench"),
            section: "all".into(),
            iterations: 50,
            jobs: 1_000,
            provider_jobs: 100_000,
            files: 1_000,
            clients: vec![1, 8],
            payloads: vec![1_024, 65_536],
            outputs: vec![3],
            scheduler_jobs: vec![1_000, 10_000, 100_000, 1_000_000],
        };
        let mut arguments = std::env::args().skip(1);
        let mut section_seen = false;
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--bench" => {}
                "--help" | "-h" => {
                    println!(
                        "cargo bench --bench backend -- \
                         [all|jobs|scheduler|reads|search|search_scale|mixed|maintenance|provider-record] \
                         [--iterations N] [--jobs N] [--files N] [--dir PATH] \
                         [--clients 1,8] [--payloads 1024,65536] [--outputs 3]\n\
                         Defaults: all, 50 iterations per client, 1000 read-fixture jobs, \
                         1000 search files, 1 and 8 clients, 1/64 KiB payloads, 3 progress \
                         outputs per job.\n\
                         JSONL on stdout; setup/warmup excluded. Errors abort the run.\n\
                         Jobs: create+claim, N progress outputs, finalize; closed loop, one \
                         measurement per command stage and one for the whole lifecycle.\n\
                         Scheduler: 1k, 10k, 100k, 1m Jobs spread over 4 Pools with 16 slots total;\n\
                         T1 admission, claim_batch + finalize loop, dependency settlement, and unclean reopen.\n\
                         Reads: warm point/page/output reads at 100 and N jobs.\n\
                         Mixed: job lifecycles written while readers poll the Job views.\n\
                         Search (Unix): 5 fresh index builds over cached files, warm queries, \
                         5 full rebuilds; production limits and coverage reported.\n\
                         Search scale (Unix): the same queries over corpora of 1, 4 and 16 MiB \
                         (the production content bound), then one corpus over it, which is the \
                         partial snapshot a larger repository indexes to.\n\
                         Maintenance: grows the file past the compaction policy, then reports what \
                         the compaction did and what concurrent readers and commands paid for it; \
                         refusals are counted, not fatal.\n\
                         Provider-record (opt-in): 100000 Jobs, 3 mock HTTP requests each; shared system\n\
                         prompt/tools and 2 history turns; durable command and replay HTTP latency.\n\
                         Other sections exclude HTTP, scheduler dispatch, external providers and LLM inference."
                    );
                    return Ok(None);
                }
                "--dir" => {
                    options.directory =
                        PathBuf::from(arguments.next().ok_or("missing option value")?);
                }
                "--iterations" | "--files" => {
                    let value: usize = arguments.next().ok_or("missing option value")?.parse()?;
                    if value == 0 {
                        return Err("benchmark sizes must be positive".into());
                    }
                    match argument.as_str() {
                        "--iterations" => options.iterations = value,
                        _ => options.files = value,
                    }
                }
                "--jobs" => {
                    let val_str = arguments.next().ok_or("missing option value")?;
                    let values = sizes(&val_str)?;
                    options.jobs = *values.first().unwrap();
                    options.provider_jobs = options.jobs;
                    options.scheduler_jobs = values;
                }
                "--clients" | "--payloads" | "--outputs" => {
                    let value = sizes(&arguments.next().ok_or("missing option value")?)?;
                    match argument.as_str() {
                        "--clients" => options.clients = value,
                        "--payloads" => options.payloads = value,
                        _ => options.outputs = value,
                    }
                }
                "all" | "jobs" | "scheduler" | "reads" | "search" | "search_scale" | "mixed"
                | "maintenance" | "provider-record"
                    if !section_seen =>
                {
                    options.section = argument;
                    section_seen = true;
                }
                _ => return Err(format!("unknown argument: {argument}; use --help").into()),
            }
        }
        if !matches!(options.section.as_str(), "scheduler" | "provider-record")
            && (options.jobs > 4_000 || options.files > 4_000)
        {
            return Err("--jobs and --files must be at most 4000 (production limits)".into());
        }
        std::fs::create_dir_all(&options.directory)?;
        options.directory = options.directory.canonicalize()?;
        Ok(Some(options))
    }

    fn selected(&self, section: &str) -> bool {
        self.section == "all" || self.section == section
    }
}

fn report(name: &str, parameters: &Value, mut samples: Vec<Duration>, elapsed: Option<Duration>) {
    samples.sort_unstable();
    let percentile = |percent: usize| {
        let index = samples
            .len()
            .saturating_mul(percent)
            .div_ceil(100)
            .saturating_sub(1);
        samples
            .get(index)
            .copied()
            .unwrap_or_default()
            .as_secs_f64()
            * 1000.0
    };
    println!(
        "{}",
        json!({
            "type": "measurement", "name": name, "parameters": parameters,
            "samples": samples.len(), "p50_ms": percentile(50),
            "p95_ms": percentile(95), "p99_ms": percentile(99),
            "max_ms": samples.last().copied().unwrap_or_default().as_secs_f64() * 1000.0,
            "elapsed_s": elapsed.map(|duration| duration.as_secs_f64()),
            "throughput_per_second": elapsed.map(|duration| samples.len() as f64 / duration.as_secs_f64()),
        })
    );
}

fn measure<T>(
    iterations: usize,
    mut operation: impl FnMut() -> Result<T>,
) -> Result<(Vec<Duration>, Duration)> {
    for _ in 0..3 {
        std::hint::black_box(operation()?);
    }
    let mut samples = Vec::with_capacity(iterations);
    let started = Instant::now();
    for _ in 0..iterations {
        let began = Instant::now();
        std::hint::black_box(operation()?);
        samples.push(began.elapsed());
    }
    Ok((samples, started.elapsed()))
}

fn with_database(
    options: &Options,
    operation: impl FnOnce(&DatabaseHandle, &DataDirectory) -> Result<()>,
) -> Result<()> {
    let root = TemporaryRoot::new(&options.directory)?;
    let directory = DataDirectory::bootstrap(root.0.join("data").into_os_string())?;
    let worker = DatabaseWorker::start(&directory)?;
    let result = operation(&worker.handle(), &directory);
    let shutdown = worker.shutdown();
    result?;
    shutdown?;
    Ok(())
}

/// Every redb file the data directory holds, added up: what the writes below
/// actually cost on disk, which the compaction policy reads.
fn database_bytes(directory: &DataDirectory) -> Result<u64> {
    let mut total = 0;
    for entry in std::fs::read_dir(directory.database_path())? {
        let entry = entry?;
        if entry
            .path()
            .extension()
            .is_some_and(|extension| extension == "redb")
        {
            total += entry.metadata()?.len();
        }
    }
    Ok(total)
}

#[derive(Clone, Copy)]
struct Target {
    executor: ExecutorId,
    configuration: ExecutionConfiguration,
}

fn executor(handle: &DatabaseHandle) -> Result<Target> {
    let connection = block_on(handle.create_config(NewConfig {
        name: "benchmark connection".into(),
        body: ConfigBody::Connection(ConnectionConfig {
            protocol: LlmTransport::OpenAiChatCompletions,
            endpoint: None,
            credential_ref: None,
        }),
    }))?;
    let model = block_on(handle.create_config(NewConfig {
        name: "benchmark model".into(),
        body: ConfigBody::Model(ModelConfig {
            connection_id: connection,
            model: "benchmark".into(),
            context_window: None,
            parameters: Default::default(),
            declared_capabilities: Default::default(),
        }),
    }))?;
    let executor = block_on(handle.create_executor(NewExecutor {
        name: "benchmark".into(),
        concurrency_limit: NonZeroU32::new(8).ok_or("invalid concurrency")?,
        llm_transports: BTreeSet::from([LlmTransport::OpenAiChatCompletions]),
    }))?;
    Ok(Target {
        executor,
        configuration: ExecutionConfiguration {
            agent_profile: None,
            llm_profile: model.into(),
        },
    })
}

fn claim(handle: &DatabaseHandle, job: JobId, target: Target) -> Result<()> {
    block_on(handle.claim_execution(ocg::persistence::ClaimRequest {
        job,
        allowed_pools: BTreeSet::from([target.executor.into()]),
    }))?
    .map_err(|refusal| format!("benchmark claim refused: {refusal:?}"))?;
    Ok(())
}

fn create_claimed(handle: &DatabaseHandle, target: Target) -> Result<JobId> {
    let id = block_on(handle.create_job(job(handle, target)))?;
    claim(handle, id, target)?;
    Ok(id)
}

fn job(handle: &DatabaseHandle, target: Target) -> NewJob {
    NewJob {
        name: Some("benchmark".into()),
        eligible_at: None,
        parent: handle.genesis(),
        projects: Vec::new(),
        dependencies: Vec::new(),
        input: JobInputDraft {
            objective: Some("measure backend overhead".into()),
            ..JobInputDraft::default()
        },
        configuration: Some(target.configuration),
        previous: None,
    }
}

fn output(payload: &str) -> NewJobOutput {
    NewJobOutput {
        kind: IntermediateOutputKind::Progress,
        summary: None,
        code: None,
        goal_reached: None,
        value: Some(json!({"text": payload})),
    }
}

fn finish(handle: &DatabaseHandle, job: JobId) -> Result<()> {
    block_on(handle.finalize_job(
        job,
        JobFinalization::Completed(FinalResult {
            summary: Some("done".into()),
            code: None,
            goal_reached: Some(true),
            value: None,
        }),
    ))?
    .ok_or("completion did not publish a result")?;
    Ok(())
}

type Timings = BTreeMap<&'static str, Vec<Duration>>;

fn lifecycle(
    handle: &DatabaseHandle,
    executor: Target,
    payload: &str,
    outputs: usize,
    timings: &mut Timings,
) -> Result<()> {
    let started = Instant::now();
    let began = Instant::now();
    let id = create_claimed(handle, executor)?;
    timings
        .entry("job.create_and_claim")
        .or_default()
        .push(began.elapsed());
    for _ in 0..outputs {
        let began = Instant::now();
        block_on(handle.append_job_output(id, output(payload)))?;
        timings
            .entry("job.append_output")
            .or_default()
            .push(began.elapsed());
    }
    let began = Instant::now();
    finish(handle, id)?;
    timings
        .entry("job.finalize")
        .or_default()
        .push(began.elapsed());
    timings
        .entry("job.lifecycle")
        .or_default()
        .push(started.elapsed());
    Ok(())
}

fn jobs(options: &Options) -> Result<()> {
    for clients in options.clients.iter().copied() {
        for payload_bytes in options.payloads.iter().copied() {
            for outputs in options.outputs.iter().copied() {
                with_database(options, |handle, _directory| {
                    let executor = executor(handle)?;
                    let payload = "x".repeat(payload_bytes);
                    for _ in 0..3 {
                        lifecycle(handle, executor, &payload, outputs, &mut Timings::new())?;
                    }
                    let ready = Barrier::new(clients + 1);
                    let (timings, elapsed) = std::thread::scope(|scope| -> Result<_> {
                        let threads: Vec<_> = (0..clients)
                            .map(|_| {
                                let ready = &ready;
                                let payload = &payload;
                                scope.spawn(move || -> Result<Timings> {
                                    let mut timings = Timings::new();
                                    ready.wait();
                                    for _ in 0..options.iterations {
                                        lifecycle(
                                            handle,
                                            executor,
                                            payload,
                                            outputs,
                                            &mut timings,
                                        )?;
                                    }
                                    Ok(timings)
                                })
                            })
                            .collect();
                        let started = Instant::now();
                        ready.wait();
                        let results: Vec<_> =
                            threads.into_iter().map(|thread| thread.join()).collect();
                        let elapsed = started.elapsed();
                        let mut combined = Timings::new();
                        for result in results {
                            for (stage, samples) in
                                result.map_err(|_| "benchmark client panicked")??
                            {
                                combined.entry(stage).or_default().extend(samples);
                            }
                        }
                        Ok((combined, elapsed))
                    })?;
                    let parameters = json!({"clients": clients, "payload_bytes": payload_bytes, "progress_outputs_per_job": outputs});
                    for (stage, samples) in timings {
                        report(
                            stage,
                            &parameters,
                            samples,
                            (stage == "job.lifecycle").then_some(elapsed),
                        );
                    }
                    Ok(())
                })?;
            }
        }
    }
    Ok(())
}

fn setup_scheduler_pools(handle: &DatabaseHandle) -> Result<Target> {
    let connection = block_on(handle.create_config(NewConfig {
        name: "benchmark connection".into(),
        body: ConfigBody::Connection(ConnectionConfig {
            protocol: LlmTransport::OpenAiChatCompletions,
            endpoint: None,
            credential_ref: None,
        }),
    }))?;
    let model = block_on(handle.create_config(NewConfig {
        name: "benchmark model".into(),
        body: ConfigBody::Model(ModelConfig {
            connection_id: connection,
            model: "benchmark".into(),
            context_window: None,
            parameters: Default::default(),
            declared_capabilities: Default::default(),
        }),
    }))?;
    let executor = block_on(handle.create_executor(NewExecutor {
        name: "benchmark".into(),
        concurrency_limit: NonZeroU32::new(16).ok_or("invalid concurrency")?,
        llm_transports: BTreeSet::from([LlmTransport::OpenAiChatCompletions]),
    }))?;
    for i in 0..4 {
        block_on(handle.create_execution_pool(NewExecutionPool {
            name: format!("benchmark-pool-{i}"),
            config: ExecutionPoolConfig {
                concurrency_limit: NonZeroU32::new(4).ok_or("invalid concurrency")?,
                capabilities: CapabilitySet {
                    operations: BTreeSet::from([ExecutionOperation::LlmRequest]),
                    llm_protocols: BTreeSet::from([LlmTransport::OpenAiChatCompletions]),
                },
            },
            enabled: true,
        }))?;
    }
    Ok(Target {
        executor,
        configuration: ExecutionConfiguration {
            agent_profile: None,
            llm_profile: model.into(),
        },
    })
}

fn admit_jobs(
    handle: &DatabaseHandle,
    target: Target,
    count: usize,
    with_dependencies: bool,
) -> Result<(Vec<JobId>, Vec<Duration>, Duration)> {
    let mut job_ids = Vec::with_capacity(count);
    let mut samples = Vec::with_capacity(count);
    let started = Instant::now();
    for i in 0..count {
        let spec = if with_dependencies && i % 2 == 1 {
            let prereq = job_ids[i - 1];
            NewJob {
                name: Some("benchmark".into()),
                eligible_at: None,
                parent: handle.genesis(),
                projects: Vec::new(),
                dependencies: vec![(prereq, DependencyCondition::Success)],
                input: JobInputDraft {
                    objective: Some("measure scheduler overhead".into()),
                    ..JobInputDraft::default()
                },
                configuration: Some(target.configuration),
                previous: None,
            }
        } else {
            job(handle, target)
        };
        let began = Instant::now();
        let id = block_on(handle.create_job(spec))?;
        samples.push(began.elapsed());
        job_ids.push(id);
    }
    let elapsed = started.elapsed();
    Ok((job_ids, samples, elapsed))
}

fn claim_and_finalize(
    handle: &DatabaseHandle,
    count: usize,
) -> Result<(Vec<Duration>, Vec<Duration>, Duration)> {
    let mut claim_batch_samples = Vec::new();
    let mut finalize_samples = Vec::with_capacity(count);
    let mut completed = 0;
    let started = Instant::now();
    while completed < count {
        let began = Instant::now();
        let claimed = block_on(handle.claim_batch())?;
        claim_batch_samples.push(began.elapsed());
        if claimed.is_empty() {
            return Err(format!(
                "claim_batch returned empty before all jobs ran ({completed}/{count} completed)"
            )
            .into());
        }
        for id in claimed {
            let began = Instant::now();
            finish(handle, id)?;
            finalize_samples.push(began.elapsed());
            completed += 1;
        }
    }
    let elapsed = started.elapsed();
    Ok((claim_batch_samples, finalize_samples, elapsed))
}

fn peak_memory_bytes() -> Option<u64> {
    #[cfg(target_os = "macos")]
    unsafe {
        let mut usage = std::mem::zeroed::<libc::rusage>();
        if libc::getrusage(libc::RUSAGE_SELF, &mut usage) == 0 {
            return Some(usage.ru_maxrss as u64);
        }
    }
    #[cfg(all(unix, not(target_os = "macos")))]
    unsafe {
        #[repr(C)]
        struct Rusage {
            ru_utime: [u64; 2],
            ru_stime: [u64; 2],
            ru_maxrss: i64,
            _pad: [i64; 13],
        }
        unsafe extern "C" {
            fn getrusage(who: i32, usage: *mut Rusage) -> i32;
        }
        let mut usage = std::mem::zeroed::<Rusage>();
        if getrusage(0, &mut usage) == 0 {
            return Some((usage.ru_maxrss as u64) * 1024);
        }
    }
    None
}

fn stop_unclean_and_reopen(
    directory: &DataDirectory,
    worker: DatabaseWorker,
    check: impl FnOnce(&DatabaseHandle) -> Result<()>,
) -> Result<(Duration, u64, Option<u64>)> {
    worker.shutdown()?;
    let db_path = directory.database_path().join("ocg.redb");
    {
        let db = Database::open(&db_path)?;
        let tx = db.begin_write()?;
        {
            let table: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
            let mut meta = tx.open_table(table)?;
            meta.insert("clean_shutdown", [0u8].as_slice())?;
        }
        tx.commit()?;
    }
    let file_size = database_bytes(directory)?;
    let began = Instant::now();
    let reopened = DatabaseWorker::start(directory)?;
    let duration = began.elapsed();
    let peak_memory = peak_memory_bytes();

    let pending_page = block_on(reopened.handle().pending_jobs(None, 1))?;
    if !pending_page.visited.is_empty() {
        return Err(format!(
            "expected no pending jobs after reopen, found at least {}",
            pending_page.visited.len()
        )
        .into());
    }
    let claimed = block_on(reopened.handle().claim_batch())?;
    if !claimed.is_empty() {
        return Err(format!(
            "expected no claimable jobs after reopen, found {}",
            claimed.len()
        )
        .into());
    }
    check(&reopened.handle())?;

    reopened.shutdown()?;

    {
        let db = Database::open(&db_path)?;
        let tx = db.begin_read()?;
        let pending_table: TableDefinition<u64, ()> = TableDefinition::new("pending");
        let ready_table: TableDefinition<(u16, u64), ()> = TableDefinition::new("ready");
        let delayed_table: TableDefinition<(i64, u64), ()> = TableDefinition::new("delayed");
        let waiters_table: TableDefinition<(u64, u64), u8> =
            TableDefinition::new("dependency_waiter");
        let unsettled_table: TableDefinition<u64, (u32, u32)> =
            TableDefinition::new("unsettled");
        let pending_count = tx.open_table(pending_table)?.len()?;
        let ready_count = tx.open_table(ready_table)?.len()?;
        let delayed_count = tx.open_table(delayed_table)?.len()?;
        let waiters_count = tx.open_table(waiters_table)?.len()?;
        let unsettled_count = tx.open_table(unsettled_table)?.len()?;
        if pending_count != 0
            || ready_count != 0
            || delayed_count != 0
            || waiters_count != 0
            || unsettled_count != 0
        {
            return Err(format!(
                "jobs still pending after reopen: pending={pending_count}, ready={ready_count}, delayed={delayed_count}, waiters={waiters_count}, unsettled={unsettled_count}"
            )
            .into());
        }
    }

    Ok((duration, file_size, peak_memory))
}

fn scheduler(options: &Options) -> Result<()> {
    let sizes = if options.section == "scheduler" {
        options.scheduler_jobs.clone()
    } else {
        vec![options.jobs]
    };

    let mut prev_time: Option<Duration> = None;
    let mut prev_size: Option<u64> = None;

    for count in sizes {
        if count >= 1_000_000
            && let (Some(t_prev), Some(s_prev)) = (prev_time, prev_size)
        {
            let estimated_time: Duration = t_prev.saturating_mul(10);
            let estimated_bytes: u64 = s_prev.saturating_mul(10);
            let max_time = Duration::from_secs(30 * 60);
            let max_bytes = 20 * 1024 * 1024 * 1024; // 20 GB
            if estimated_time > max_time || estimated_bytes > max_bytes {
                eprintln!(
                    "Skipping {} jobs: estimated time ({:.1}m) or disk ({:.1} GB) exceeds 30m / 20GB",
                    count,
                    estimated_time.as_secs_f64() / 60.0,
                    estimated_bytes as f64 / (1024.0 * 1024.0 * 1024.0)
                );
                println!(
                    "{}",
                    json!({
                        "type": "skip",
                        "name": "scheduler.scale_limit",
                        "jobs": count,
                        "estimated_time_s": estimated_time.as_secs_f64(),
                        "estimated_bytes": estimated_bytes,
                        "reason": "estimated time > 30 minutes or disk > 20 GB",
                    })
                );
                continue;
            }
        }

        let run_started = Instant::now();

        // 1 & 2: Admit independent jobs, claim and finalize
        let root = TemporaryRoot::new(&options.directory)?;
        let directory = DataDirectory::bootstrap(root.0.join("data").into_os_string())?;
        let worker = DatabaseWorker::start(&directory)?;
        let handle = worker.handle();
        let target = setup_scheduler_pools(&handle)?;

        let (_job_ids, admit_samples, elapsed_admit) = admit_jobs(&handle, target, count, false)?;
        let parameters_independent = json!({
            "jobs": count,
            "dependencies": false,
            "pools": 4,
            "slots": 16,
        });
        report(
            "scheduler.admit",
            &parameters_independent,
            admit_samples,
            Some(elapsed_admit),
        );

        let (claim_samples, finalize_samples, elapsed_claim) = claim_and_finalize(&handle, count)?;
        report(
            "scheduler.claim_batch",
            &parameters_independent,
            claim_samples,
            Some(elapsed_claim),
        );
        report(
            "scheduler.finalize",
            &parameters_independent,
            finalize_samples,
            Some(elapsed_claim),
        );
        worker.shutdown()?;
        drop(root);

        // 3: Repeat once with half the Jobs depending on an earlier Job
        let root = TemporaryRoot::new(&options.directory)?;
        let directory = DataDirectory::bootstrap(root.0.join("data").into_os_string())?;
        let worker = DatabaseWorker::start(&directory)?;
        let handle = worker.handle();
        let target = setup_scheduler_pools(&handle)?;

        let (_dep_ids, dep_admit_samples, elapsed_dep_admit) =
            admit_jobs(&handle, target, count, true)?;
        let parameters_dependent = json!({
            "jobs": count,
            "dependencies": true,
            "pools": 4,
            "slots": 16,
        });
        report(
            "scheduler.admit",
            &parameters_dependent,
            dep_admit_samples,
            Some(elapsed_dep_admit),
        );

        let (dep_claim_samples, dep_finalize_samples, elapsed_dep_claim) =
            claim_and_finalize(&handle, count)?;
        report(
            "scheduler.claim_batch",
            &parameters_dependent,
            dep_claim_samples,
            Some(elapsed_dep_claim),
        );
        report(
            "scheduler.finalize",
            &parameters_dependent,
            dep_finalize_samples,
            Some(elapsed_dep_claim),
        );

        // 4: Stop without clean shutdown and reopen it (after all jobs completed)
        let (reopen_duration, file_size, peak_memory) =
            stop_unclean_and_reopen(&directory, worker, |_| Ok(()))?;
        let reopen_params = json!({
            "jobs": count,
            "dependencies": true,
            "work_queued": false,
            "database_file_bytes": file_size,
            "peak_memory_bytes": peak_memory,
        });
        report(
            "scheduler.unclean_open",
            &reopen_params,
            vec![reopen_duration],
            Some(reopen_duration),
        );
        println!(
            "{}",
            json!({
                "type": "fixture",
                "name": "scheduler.reopen",
                "jobs": count,
                "work_queued": false,
                "database_file_bytes": file_size,
                "duration_ms": reopen_duration.as_secs_f64() * 1000.0,
                "peak_memory_bytes": peak_memory,
            })
        );
        drop(root);

        // 5: Work still queued: admit N Jobs (half dependent), claim none,
        // stop without clean shutdown, time reopen, report peak memory,
        // and check after reopen that no Job is still pending.
        let root = TemporaryRoot::new(&options.directory)?;
        let directory = DataDirectory::bootstrap(root.0.join("data").into_os_string())?;
        let worker = DatabaseWorker::start(&directory)?;
        let handle = worker.handle();
        let target = setup_scheduler_pools(&handle)?;

        let (queued_job_ids, _admit_samples, _elapsed) =
            admit_jobs(&handle, target, count, true)?;

        let (queued_reopen_duration, queued_file_size, queued_peak_memory) =
            stop_unclean_and_reopen(&directory, worker, |handle| {
                if let Some(first_id) = queued_job_ids.first() {
                    let record = block_on(handle.job(*first_id))?
                        .ok_or("first job not found after reopen")?;
                    let outcome = record
                        .outcome
                        .ok_or("first job missing outcome after reopen")?;
                    if outcome.kind != OutcomeKind::Cancelled
                        || outcome.cause != Some(OutcomeCause::Abandoned)
                    {
                        return Err(format!(
                            "first job expected Cancelled(Abandoned), got {:?}",
                            outcome
                        )
                        .into());
                    }
                }
                if let Some(last_id) = queued_job_ids.last() {
                    let record = block_on(handle.job(*last_id))?
                        .ok_or("last job not found after reopen")?;
                    let outcome = record
                        .outcome
                        .ok_or("last job missing outcome after reopen")?;
                    if outcome.kind != OutcomeKind::Cancelled
                        || outcome.cause != Some(OutcomeCause::Abandoned)
                    {
                        return Err(format!(
                            "last job expected Cancelled(Abandoned), got {:?}",
                            outcome
                        )
                        .into());
                    }
                }
                Ok(())
            })?;

        let queued_reopen_params = json!({
            "jobs": count,
            "dependencies": true,
            "work_queued": true,
            "database_file_bytes": queued_file_size,
            "peak_memory_bytes": queued_peak_memory,
        });
        report(
            "scheduler.unclean_open_queued",
            &queued_reopen_params,
            vec![queued_reopen_duration],
            Some(queued_reopen_duration),
        );
        println!(
            "{}",
            json!({
                "type": "fixture",
                "name": "scheduler.reopen_queued",
                "jobs": count,
                "work_queued": true,
                "database_file_bytes": queued_file_size,
                "duration_ms": queued_reopen_duration.as_secs_f64() * 1000.0,
                "peak_memory_bytes": queued_peak_memory,
            })
        );
        drop(root);

        let total_run_time = run_started.elapsed();
        prev_time = Some(total_run_time);
        prev_size = Some(file_size.max(queued_file_size));
    }
    Ok(())
}

/// Job lifecycles written while the views a dashboard polls are read: what a
/// reader pays while the write worker is busy, and what the writer pays for
/// the readers ahead of it.
fn mixed(options: &Options) -> Result<()> {
    use std::sync::atomic::{AtomicBool, Ordering};

    let payload_bytes = options.payloads.first().copied().unwrap_or(1_024);
    let outputs = options.outputs.first().copied().unwrap_or(3);
    for writers in options.clients.iter().copied() {
        with_database(options, |handle, _directory| {
            let executor = executor(handle)?;
            let mut ids = Vec::with_capacity(options.jobs);
            for _ in 0..options.jobs {
                ids.push(block_on(handle.create_job(job(handle, executor)))?);
            }
            let payload = "x".repeat(payload_bytes);
            let stop = AtomicBool::new(false);
            // Half the readers on the point read, half on the page read: one
            // Job, and the list a Job list shows.
            let operations = [
                "read.job",
                "read.job",
                "read.jobs_first_page",
                "read.jobs_first_page",
            ];
            let ready = Barrier::new(writers + operations.len() + 1);
            let (writes, reads, elapsed) = std::thread::scope(|scope| -> Result<_> {
                let mut clients = Vec::new();
                for _ in 0..writers {
                    let (ready, payload) = (&ready, &payload);
                    clients.push(scope.spawn(move || -> Result<Vec<Duration>> {
                        let mut samples = Vec::with_capacity(options.iterations);
                        let mut timings = Timings::new();
                        ready.wait();
                        for _ in 0..options.iterations {
                            let began = Instant::now();
                            lifecycle(handle, executor, payload, outputs, &mut timings)?;
                            samples.push(began.elapsed());
                        }
                        Ok(samples)
                    }));
                }
                let mut readers = Vec::new();
                for operation in operations {
                    let (ready, ids, stop) = (&ready, &ids, &stop);
                    readers.push((
                        operation,
                        scope.spawn(move || -> Result<Vec<Duration>> {
                            let mut samples = Vec::new();
                            ready.wait();
                            let mut index = 0usize;
                            while !stop.load(Ordering::Relaxed) {
                                let began = Instant::now();
                                match operation {
                                    "read.job" => {
                                        let id = ids[index % ids.len()];
                                        std::hint::black_box(
                                            block_on(handle.job(id))?.ok_or("job disappeared")?,
                                        );
                                    }
                                    _ => {
                                        let page = block_on(handle.jobs(None, 20))?;
                                        if page.is_empty() {
                                            return Err("empty job page".into());
                                        }
                                        std::hint::black_box(page);
                                    }
                                }
                                samples.push(began.elapsed());
                                index += 1;
                            }
                            Ok(samples)
                        }),
                    ));
                }
                let started = Instant::now();
                ready.wait();
                let mut writes = Vec::new();
                for client in clients {
                    writes.extend(client.join().map_err(|_| "benchmark client panicked")??);
                }
                stop.store(true, Ordering::Relaxed);
                let mut reads: Vec<(&'static str, Vec<Duration>)> = Vec::new();
                for (operation, reader) in readers {
                    let samples = reader.join().map_err(|_| "benchmark reader panicked")??;
                    match reads.iter_mut().find(|(name, _)| *name == operation) {
                        Some((_, existing)) => existing.extend(samples),
                        None => reads.push((operation, samples)),
                    }
                }
                let elapsed = started.elapsed();
                Ok((writes, reads, elapsed))
            })?;
            let parameters = json!({"writers": writers, "readers": operations.len(),
                "payload_bytes": payload_bytes, "progress_outputs_per_job": outputs,
                "interleaved_with_writes": true});
            report(
                "mixed.write_job_lifecycle",
                &parameters,
                writes,
                Some(elapsed),
            );
            for (operation, samples) in reads {
                report(
                    &format!("mixed.{operation}"),
                    &parameters,
                    samples,
                    Some(elapsed),
                );
            }
            Ok(())
        })?;
    }
    Ok(())
}

fn reads(options: &Options) -> Result<()> {
    for count in BTreeSet::from([100.min(options.jobs), options.jobs]) {
        with_database(options, |handle, _directory| {
            let executor = executor(handle)?;
            let mut ids = Vec::with_capacity(count);
            for _ in 0..count {
                ids.push(block_on(handle.create_job(job(handle, executor)))?);
            }
            let first = ids.first().copied().ok_or("empty job fixture")?;
            claim(handle, first, executor)?;
            for _ in 0..100 {
                block_on(handle.append_job_output(first, output(&"x".repeat(1_024))))?;
            }
            finish(handle, first)?;
            let cursor = ids.get(count / 2).copied();
            for clients in [1, 8] {
                for operation in [
                    "read.job",
                    "read.jobs_first_page",
                    "read.jobs_cursor_page",
                    "read.outputs",
                ] {
                    let read = |index: usize| -> Result<()> {
                        match operation {
                            "read.job" => {
                                let id = ids
                                    .get(index % ids.len())
                                    .copied()
                                    .ok_or("missing fixture job")?;
                                std::hint::black_box(
                                    block_on(handle.job(id))?.ok_or("job disappeared")?,
                                );
                            }
                            "read.jobs_first_page" | "read.jobs_cursor_page" => {
                                let before = (operation == "read.jobs_cursor_page")
                                    .then_some(cursor)
                                    .flatten();
                                let page = block_on(handle.jobs(before, 20))?;
                                if page.is_empty() {
                                    return Err("empty job page".into());
                                }
                                std::hint::black_box(page);
                            }
                            _ => {
                                let page = block_on(handle.job_outputs(first, None, 50))?;
                                if page.len() != 50 {
                                    return Err("output page was incomplete".into());
                                }
                                std::hint::black_box(page);
                            }
                        }
                        Ok(())
                    };
                    for index in 0..3 {
                        read(index)?;
                    }
                    let ready = Barrier::new(clients + 1);
                    let (samples, elapsed) = std::thread::scope(|scope| -> Result<_> {
                        let threads: Vec<_> = (0..clients)
                            .map(|client| {
                                let ready = &ready;
                                let read = &read;
                                scope.spawn(move || -> Result<Vec<Duration>> {
                                    let mut samples = Vec::with_capacity(options.iterations);
                                    ready.wait();
                                    for index in 0..options.iterations {
                                        let began = Instant::now();
                                        read(index + client * options.iterations)?;
                                        samples.push(began.elapsed());
                                    }
                                    Ok(samples)
                                })
                            })
                            .collect();
                        let started = Instant::now();
                        ready.wait();
                        let results: Vec<_> =
                            threads.into_iter().map(|thread| thread.join()).collect();
                        let elapsed = started.elapsed();
                        let mut samples = Vec::new();
                        for result in results {
                            samples.extend(result.map_err(|_| "benchmark client panicked")??);
                        }
                        Ok((samples, elapsed))
                    })?;
                    report(
                        operation,
                        &json!({"fixture_jobs": count, "clients": clients, "page_limit": if operation == "read.outputs" {50} else {20}}),
                        samples,
                        Some(elapsed),
                    );
                }
            }
            Ok(())
        })?;
    }
    Ok(())
}

/// The queries one index is measured against, from a term in a single file to a
/// phrase common enough to leave every file a candidate and match in none. The
/// last two are the ones whose cost scales with the corpus: a hit stops the
/// query early, no hit leaves it verifying every candidate.
#[cfg(unix)]
const SEARCH_QUERIES: [(&str, &str); 7] = [
    ("rare", "rarebeacon"),
    ("missing", "neverpresenttoken"),
    ("common", "packet"),
    ("and", "rarebeacon alpha"),
    ("phrase", "\"alpha beta\""),
    ("broad_phrase", "\"packet state\""),
    ("broad_phrase_zero", "\"state error\""),
];

/// Writes `files` files of four rotating `sizes`: one distribution the
/// baseline corpus and every scaled corpus share, so a query's cost moves only
/// with the corpus it is measured against. File 0 carries the rare term.
#[cfg(unix)]
fn corpus(source: &std::path::Path, files: usize, sizes: [usize; 4]) -> Result<()> {
    for file in 0..files {
        let group = source.join(format!("group{:03}", file / 40));
        std::fs::create_dir_all(&group)?;
        let target = sizes[file % 4];
        let mut content = String::new();
        if file == 0 {
            content.push_str("// rarebeacon alpha beta HTTPServer\n");
        }
        while content.len() < target {
            content.push_str(&format!("pub fn itemcode{file:05x}part{:05x}(state: &mut State) -> Result<u64, Error> {{ let packet = state.apply(value)?; Ok(packet) }}\n", content.len()));
        }
        std::fs::write(group.join(format!("unit{file:05}.rs")), content)?;
    }
    Ok(())
}

/// Measures every query class against one installed index. `strict` holds the
/// queries to their expected hits, which a corpus truncated by a production
/// bound may not carry.
#[cfg(unix)]
fn queries(
    index: &ocg::orchestration::workspace_fts::WorkspaceFts,
    prefix: &str,
    parameters: &Value,
    iterations: usize,
    strict: bool,
) -> Result<()> {
    for (class, query) in SEARCH_QUERIES {
        let value = index.search(query, 20)?;
        let results = value["results"]
            .as_array()
            .ok_or("search omitted results")?;
        if strict {
            if matches!(class, "rare" | "and" | "phrase" | "common" | "broad_phrase")
                && results.is_empty()
            {
                return Err(format!("expected search hits for {class}").into());
            }
            if matches!(class, "missing" | "broad_phrase_zero") && !results.is_empty() {
                return Err(format!("unexpected search hits for {class}").into());
            }
        }
        let (samples, elapsed) = measure(iterations, || Ok(index.search(query, 20)?))?;
        let mut parameters = parameters.clone();
        parameters["result_limit"] = json!(20);
        parameters["records"] = json!(results.len());
        parameters["complete"] = value["complete"].clone();
        parameters["limit"] = value["limit"].clone();
        report(
            &format!("{prefix}.{class}"),
            &parameters,
            samples,
            Some(elapsed),
        );
    }
    Ok(())
}

#[cfg(unix)]
fn search(options: &Options) -> Result<()> {
    use ocg::orchestration::workspace_fts::{FtsLimits, WorkspaceFts};
    use ocg::orchestration::workspace_read::Workspace;

    let root = TemporaryRoot::new(&options.directory)?;
    let directory = DataDirectory::bootstrap(root.0.join("data").into_os_string())?;
    let source = root.0.join("workspace");
    std::fs::create_dir(&source)?;
    corpus(&source, options.files, [512, 2_048, 4_096, 9_216])?;
    let workspace = Workspace::authorize(&source, &directory)?;
    let parameters = json!({"files": options.files, "cache": "warm_os_cache", "result_limit": 20});
    let (samples, elapsed) = measure(5, || {
        let index = WorkspaceFts::new(workspace.clone(), FtsLimits::default());
        let built = index.build()?;
        if !built.coverage.complete || built.coverage.files_indexed != options.files {
            return Err(format!("partial search fixture: {:?}", built.coverage).into());
        }
        Ok(built)
    })?;
    report(
        "search.build_fresh_index",
        &parameters,
        samples,
        Some(elapsed),
    );
    let index = WorkspaceFts::new(workspace, FtsLimits::default());
    let built = index.build()?;
    println!(
        "{}",
        json!({"type": "fixture", "name": "search.index", "files": built.coverage.files_indexed,
        "content_bytes": built.coverage.content_bytes_indexed, "tokens": built.coverage.tokens_indexed,
        "memory_bytes_approx": built.memory_bytes_approx, "complete": built.coverage.complete})
    );
    queries(
        &index,
        "search",
        &json!({"files": options.files}),
        options.iterations,
        true,
    )?;
    let (samples, elapsed) = measure(5, || Ok(index.rebuild()?))?;
    report("search.full_rebuild", &parameters, samples, Some(elapsed));
    Ok(())
}

/// The same queries over corpora of growing size: the build and the phrase
/// verification that dominates a query both scale with what was indexed, and
/// one corpus is deliberately past the production content bound, which is the
/// partial snapshot a larger repository indexes to.
#[cfg(unix)]
fn search_scale(options: &Options) -> Result<()> {
    use ocg::orchestration::workspace_fts::{FtsLimits, WorkspaceFts};
    use ocg::orchestration::workspace_read::Workspace;

    let limits = FtsLimits::default();
    for target in [1u64, 4, 16, 64].map(|mib| mib * 1024 * 1024) {
        let within = target <= limits.content_bytes;
        let label = format!("{}mib", target / (1024 * 1024));
        let root = TemporaryRoot::new(&options.directory)?;
        let directory = DataDirectory::bootstrap(root.0.join("data").into_os_string())?;
        let source = root.0.join("workspace");
        std::fs::create_dir(&source)?;
        let per_file = usize::try_from(target / options.files as u64)
            .unwrap_or(256)
            .max(256);
        corpus(
            &source,
            options.files,
            [per_file / 4, per_file / 2, per_file, per_file * 2],
        )?;
        let workspace = Workspace::authorize(&source, &directory)?;
        let parameters = json!({"corpus": label, "corpus_bytes_target": target,
            "files": options.files, "cache": "warm_os_cache",
            "production_content_bytes_bound": limits.content_bytes, "within_bound": within});
        let (samples, elapsed) = measure(5, || {
            let index = WorkspaceFts::new(workspace.clone(), limits);
            let built = index.build()?;
            if built.coverage.files_indexed == 0 {
                return Err(format!("empty search corpus: {:?}", built.coverage).into());
            }
            if within && (!built.coverage.complete || built.coverage.files_indexed != options.files)
            {
                return Err(format!("partial search fixture: {:?}", built.coverage).into());
            }
            if !within && built.coverage.complete {
                return Err("a corpus past the content bound indexed whole".into());
            }
            Ok(built)
        })?;
        report(
            &format!("search_scale.build.{label}"),
            &parameters,
            samples,
            Some(elapsed),
        );
        let index = WorkspaceFts::new(workspace, limits);
        let built = index.build()?;
        println!(
            "{}",
            json!({"type": "fixture", "name": "search_scale.index", "corpus": label,
            "files": built.coverage.files_indexed, "content_bytes": built.coverage.content_bytes_indexed,
            "tokens": built.coverage.tokens_indexed, "terms": built.coverage.terms,
            "memory_bytes_approx": built.memory_bytes_approx,
            "complete": built.coverage.complete, "limit": format!("{:?}", built.coverage.limit)})
        );
        queries(
            &index,
            &format!("search_scale.{label}"),
            &parameters,
            options.iterations,
            within,
        )?;
        let (samples, elapsed) = measure(5, || Ok(index.rebuild()?))?;
        report(
            &format!("search_scale.full_rebuild.{label}"),
            &parameters,
            samples,
            Some(elapsed),
        );
    }
    Ok(())
}

/// Grows the file past the compaction policy's smallest file, then reports what
/// a compaction attempt did and what the readers and commands around it paid.
/// A refusal is a result here, not a failure of the run: the compaction holds
/// every read and command off, and past a full queue they are refused.
fn maintenance(options: &Options) -> Result<()> {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Mutex, PoisonError};

    use ocg::database::CompactionOutcome;

    const TARGET_FILE_BYTES: u64 = 72 * 1024 * 1024;
    const PAYLOAD_BYTES: usize = 65_536;
    const OUTPUTS: usize = 3;

    with_database(options, |handle, directory| {
        let executor = executor(handle)?;
        let payload = "x".repeat(PAYLOAD_BYTES);
        let mut timings = Timings::new();
        let mut samples = Vec::new();
        let started = Instant::now();
        let mut written = 0usize;
        while database_bytes(directory)? < TARGET_FILE_BYTES {
            let began = Instant::now();
            lifecycle(handle, executor, &payload, OUTPUTS, &mut timings)?;
            samples.push(began.elapsed());
            written += 1;
        }
        let elapsed = started.elapsed();
        let file_bytes = database_bytes(directory)?;
        println!(
            "{}",
            json!({"type": "fixture", "name": "maintenance.database", "jobs_written": written,
            "database_file_bytes": file_bytes, "target_file_bytes": TARGET_FILE_BYTES,
            "payload_bytes": PAYLOAD_BYTES, "progress_outputs_per_job": OUTPUTS})
        );
        let grown = json!({"jobs_written": written, "database_file_bytes": file_bytes,
            "payload_bytes": PAYLOAD_BYTES, "progress_outputs_per_job": OUTPUTS});
        report(
            "maintenance.write_job_lifecycle",
            &grown,
            samples,
            Some(elapsed),
        );

        let observed = create_claimed(handle, executor)?;
        let stop = AtomicBool::new(false);
        let refused = AtomicUsize::new(0);
        let first_refusal = Mutex::new(None);
        let note = |error: String| {
            refused.fetch_add(1, Ordering::Relaxed);
            let mut first = first_refusal.lock().unwrap_or_else(PoisonError::into_inner);
            if first.is_none() {
                *first = Some(error);
            }
        };
        let ready = Barrier::new(5);
        let (measured, outcome, compaction) = std::thread::scope(|scope| -> Result<_> {
            let mut clients: Vec<(
                &'static str,
                std::thread::ScopedJoinHandle<Result<Vec<Duration>>>,
            )> = Vec::new();
            for operation in ["read.job", "read.job", "read.jobs_first_page"] {
                let (ready, stop, note) = (&ready, &stop, &note);
                clients.push((
                    operation,
                    scope.spawn(move || -> Result<Vec<Duration>> {
                        let mut samples = Vec::new();
                        ready.wait();
                        while !stop.load(Ordering::Relaxed) {
                            let began = Instant::now();
                            let attempt = match operation {
                                "read.job" => block_on(handle.job(observed)).map(|found| {
                                    std::hint::black_box(found);
                                }),
                                _ => block_on(handle.jobs(None, 20)).map(|page| {
                                    std::hint::black_box(page);
                                }),
                            };
                            match attempt {
                                Ok(_) => samples.push(began.elapsed()),
                                Err(error) => note(error.to_string()),
                            }
                        }
                        Ok(samples)
                    }),
                ));
            }
            let (ready, stop, note) = (&ready, &stop, &note);
            clients.push((
                "write.append_job_output",
                scope.spawn(move || -> Result<Vec<Duration>> {
                    let mut samples = Vec::new();
                    ready.wait();
                    while !stop.load(Ordering::Relaxed) {
                        let began = Instant::now();
                        match block_on(handle.append_job_output(observed, output(&payload))) {
                            Ok(_) => samples.push(began.elapsed()),
                            Err(error) => note(error.to_string()),
                        }
                    }
                    Ok(samples)
                }),
            ));
            ready.wait();
            let began = Instant::now();
            let outcome = block_on(handle.compact_storage())?;
            let compaction = began.elapsed();
            stop.store(true, Ordering::Relaxed);
            let mut measured: Vec<(&'static str, Vec<Duration>)> = Vec::new();
            for (operation, client) in clients {
                let samples = client.join().map_err(|_| "benchmark client panicked")??;
                match measured.iter_mut().find(|(name, _)| *name == operation) {
                    Some((_, existing)) => existing.extend(samples),
                    None => measured.push((operation, samples)),
                }
            }
            Ok((measured, outcome, compaction))
        })?;
        let outcome = json!(match outcome {
            CompactionOutcome::Compacted(report) => json!({
                "result": "compacted",
                "file_bytes_before": report.file_bytes_before,
                "file_bytes_after": report.file_bytes_after,
                "duration_ms": report.duration.as_secs_f64() * 1000.0,
            }),
            CompactionOutcome::NoProgress(report) => json!({
                "result": "no_progress",
                "file_bytes_before": report.file_bytes_before,
                "file_bytes_after": report.file_bytes_after,
                "duration_ms": report.duration.as_secs_f64() * 1000.0,
            }),
            CompactionOutcome::Skipped(skip) => json!({
                "result": "skipped", "reason": format!("{skip:?}"),
            }),
        });
        let around = json!({"database_file_bytes": file_bytes,
            "compaction_duration_ms": compaction.as_secs_f64() * 1000.0,
            "compaction_outcome": outcome,
            "refused_operations": refused.load(Ordering::Relaxed),
            "first_refusal": first_refusal.lock().unwrap_or_else(PoisonError::into_inner).clone()});
        println!(
            "{}",
            json!({"type": "measurement", "name": "maintenance.compact_storage",
            "samples": 1, "duration_ms": compaction.as_secs_f64() * 1000.0, "parameters": around})
        );
        for (operation, samples) in measured {
            report(&format!("maintenance.{operation}"), &around, samples, None);
        }
        Ok(())
    })
}

fn main() -> Result<()> {
    let Some(options) = Options::parse()? else {
        return Ok(());
    };
    if cfg!(debug_assertions) {
        return Err("run with cargo bench (optimized build required)".into());
    }
    let compiler = std::process::Command::new("rustc")
        .arg("--version")
        .output()?;
    if !compiler.status.success() {
        return Err("rustc --version failed".into());
    }
    // The provider-record section drives its own workload and applies neither
    // file, payload, output nor iteration options. Reporting them as if they
    // had been applied would describe a load this section never ran, so they
    // are null and named instead.
    let is_provider_record = options.section == "provider-record";
    // Null means the section did not apply the option, so a reader is not
    // left reading a declared parameter that describes no work.
    let applied = |value: Value| {
        if is_provider_record {
            Value::Null
        } else {
            value
        }
    };
    println!(
        "{}",
        json!({"type": "environment", "os": std::env::consts::OS, "arch": std::env::consts::ARCH,
        "compiler": String::from_utf8(compiler.stdout)?.trim(),
        "available_parallelism": std::thread::available_parallelism()?.get(),
        "section": options.section,
        "iterations_per_client": applied(json!(options.iterations)),
        "jobs": if is_provider_record { options.provider_jobs } else { options.jobs },
        "files": applied(json!(options.files)),
        "clients": options.clients,
        "payloads": applied(json!(options.payloads)),
        "progress_outputs_per_job": applied(json!(options.outputs)),
        "unused_options": if is_provider_record { json!(["iterations", "files", "payloads", "outputs"]) } else { json!([]) },
        "load_model": "closed_loop", "latency_unit": "ms", "storage_dir": options.directory,
        "scope": if is_provider_record { "mock provider HTTP, request persistence and replay HTTP; excludes tool handlers and scheduler dispatch" } else { "public database and FTS APIs; excludes HTTP, scheduler dispatch and providers" }})
    );
    if options.section == "provider-record" {
        return provider_record(&options);
    }
    if options.selected("jobs") {
        jobs(&options)?;
    }
    if options.selected("scheduler") {
        scheduler(&options)?;
    }
    if options.selected("reads") {
        reads(&options)?;
    }
    if options.selected("mixed") {
        mixed(&options)?;
    }
    if options.selected("maintenance") {
        maintenance(&options)?;
    }
    #[cfg(unix)]
    if options.selected("search") {
        search(&options)?;
    }
    #[cfg(unix)]
    if options.selected("search_scale") {
        search_scale(&options)?;
    }
    #[cfg(not(unix))]
    if options.section == "all" || options.section == "search" || options.section == "search_scale"
    {
        return Err("search benchmarks require Unix".into());
    }
    Ok(())
}

#[derive(Default)]
struct ProviderCommits {
    sent: Vec<Duration>,
    output_flush: Vec<Duration>,
    answer: Vec<Duration>,
    delivered: Vec<Duration>,
    usage: Vec<Duration>,
    not_delivered: Vec<Duration>,
}

impl ProviderCommits {
    /// Adds another client's samples, so what is reported describes every
    /// client rather than the one that happened to finish first.
    fn absorb(&mut self, other: ProviderCommits) {
        self.sent.extend(other.sent);
        self.output_flush.extend(other.output_flush);
        self.answer.extend(other.answer);
        self.delivered.extend(other.delivered);
        self.usage.extend(other.usage);
        self.not_delivered.extend(other.not_delivered);
    }
}

struct ProviderBenchSink<'a> {
    database: DatabaseHandle,
    job: JobId,
    request: ocg::persistence::RequestNumber,
    recorder: ocg::provider_execution::RequestRecorder,
    commits: &'a mut ProviderCommits,
    gap: Option<String>,
}

impl ProviderBenchSink<'_> {
    async fn commit<T>(
        commits: &mut Vec<Duration>,
        operation: impl Future<Output = std::result::Result<T, ocg::database::DatabaseError>>,
    ) -> std::result::Result<T, ocg::provider_execution::OutputSinkError> {
        let began = Instant::now();
        let result = operation.await;
        commits.push(began.elapsed());
        result.map_err(|error| ocg::provider_execution::OutputSinkError {
            message: error.to_string(),
        })
    }
}

impl ocg::provider_execution::ProviderOutputSink for ProviderBenchSink<'_> {
    fn sending<'a>(
        &'a mut self,
        body: &'a Value,
    ) -> std::pin::Pin<
        Box<
            dyn Future<Output = std::result::Result<(), ocg::provider_execution::OutputSinkError>>
                + 'a,
        >,
    > {
        Box::pin(async move {
            let (sent, state) = self.recorder.prepare(self.job, self.request, body, 0)?;
            Self::commit(
                &mut self.commits.sent,
                self.database.record_sent_request(sent),
            )
            .await?;
            self.recorder.remember_sent(state);
            Ok(())
        })
    }

    fn append<'a>(
        &'a mut self,
        kind: IntermediateOutputKind,
        text: &'a str,
    ) -> std::pin::Pin<
        Box<
            dyn Future<Output = std::result::Result<(), ocg::provider_execution::OutputSinkError>>
                + 'a,
        >,
    > {
        Box::pin(async move {
            let database = self.database.clone();
            Self::commit(
                &mut self.commits.output_flush,
                database.append_job_output(
                    self.job,
                    NewJobOutput {
                        kind,
                        summary: Some(text.into()),
                        code: None,
                        goal_reached: None,
                        value: None,
                    },
                ),
            )
            .await
            .map(|_| ())
        })
    }

    fn observe<'a>(
        &'a mut self,
        observed: ocg::provider_execution::ProviderObservation,
    ) -> std::pin::Pin<Box<dyn Future<Output = ()> + 'a>> {
        Box::pin(async move {
            use ocg::provider_execution::ProviderFact;
            use ocg::token_usage::{TokenAnnotation, UsageFinality};
            let (measurements, token, commits) = match observed.fact {
                ProviderFact::NotDelivered => (
                    Default::default(),
                    TokenAnnotation::excluded(),
                    &mut self.commits.not_delivered,
                ),
                ProviderFact::Delivered => (
                    Default::default(),
                    TokenAnnotation::eligible(UsageFinality::Cumulative),
                    &mut self.commits.delivered,
                ),
                ProviderFact::Usage { usage, finality } => (
                    ocg::persistence::Measurements {
                        usage,
                        ..Default::default()
                    },
                    TokenAnnotation::eligible(finality),
                    &mut self.commits.usage,
                ),
            };
            let database = self.database.clone();
            if let Err(error) = Self::commit(
                commits,
                database.record_metrics(ocg::persistence::NewMetrics {
                    job: self.job,
                    request: self.request,
                    observed_at: observed.observed_at,
                    measurements,
                    token: Some(token),
                }),
            )
            .await
            {
                self.gap.get_or_insert(error.message);
            }
        })
    }
}

fn provider_record(options: &Options) -> Result<()> {
    let provider_jobs = options.provider_jobs;
    let storage_dir = options.directory.clone();
    let client_sizes = options.clients.clone();
    ntex::rt::System::new("provider-record-bench", ntex::rt::DefaultRuntime).block_on(async move {
        use ntex::web::{self, App, HttpResponse, HttpServer};
        use ocg::control_plane::{ControlPlane, ControlPlaneConfig};
        use ocg::openai_compatible::{ChatConversation, ChatMessage, ChatToolCallWire};
        use ocg::orchestration::execution_runtime::ExecutionRuntime;
        use ocg::persistence::{NewAvailabilityStat, NewProviderAnswer, RequestNumber, Timestamp};
        use ocg::provider_execution::{self, EnvironmentCredentialResolver, ProviderExecutionConfig};
        use redb::ReadableTable;
        use std::sync::atomic::{AtomicUsize, Ordering};

        // Every declared client count is run against its own file, so the
        // reported numbers describe a load that was actually applied.
        for clients in client_sizes {
            let root = TemporaryRoot::new(&storage_dir)?;
            let directory = DataDirectory::bootstrap(root.0.join("data").into_os_string())?;
            let worker = DatabaseWorker::start(&directory)?;
            let handle = worker.handle();
            let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
            let provider_address = listener.local_addr()?;
            let mock = HttpServer::new(async || {
                App::new().default_service(web::to(async |body: web::types::Json<Value>| {
                    let count = body["messages"].as_array().map_or(0, Vec::len);
                    let round = count.saturating_sub(6) / 2;
                    let question = body["messages"][5]["content"].as_str().ok_or_else(||
                        ntex::web::error::ErrorBadRequest("missing Job index"))?;
                    let index = question.strip_prefix("Current question for Job ")
                        .and_then(|value| value.strip_suffix('.'))
                        .and_then(|value| value.parse::<usize>().ok())
                        .ok_or_else(|| ntex::web::error::ErrorBadRequest("invalid Job index"))?;
                    let choice = if round < 2 {
                        json!({"index":0, "delta":{"content":"Checking.", "reasoning_content":"Think.",
                            "tool_calls":[{"index":0,"id":format!("call-{index}-{round}"),"type":"function",
                            "function":{"name":"lookup","arguments":"{}"}}]}, "finish_reason":"tool_calls"})
                    } else {
                        json!({"index":0, "delta":{"content":format!("Done for Job {index}."),"reasoning_content":"Think."}, "finish_reason":"stop"})
                    };
                    let response = format!("data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
                        json!({"choices":[choice]}),
                        json!({"choices":[],"usage":{"prompt_tokens":2048,"completion_tokens":8}}));
                    Ok::<_, ntex::web::Error>(HttpResponse::Ok().content_type("text/event-stream").body(response))
                }))
            }).workers(clients).disable_signals().listen(listener)?.run();
            let mock_finished = ntex::rt::spawn(mock.clone());
            let target = executor(&handle)?;
            // Each client takes the next unused Job index, so one slow Job
            // leaves no client idle, and each keeps its own samples until
            // every client has finished and they can be merged.
            let next = Arc::new(AtomicUsize::new(0));
            let started = Instant::now();
            let mut runners = Vec::with_capacity(clients);
            for _ in 0..clients {
                let handle = handle.clone();
                let next = next.clone();
                runners.push(ntex::rt::spawn(async move {
                    let mut commits = ProviderCommits::default();
                    let mut job_commits = Vec::new();
                    let mut ids = Vec::new();
                    loop {
                        let index = next.fetch_add(1, Ordering::Relaxed);
                        if index >= provider_jobs {
                            break;
                        }
                        let began = Instant::now();
                        let id = handle.create_job(job(&handle, target)).await?;
                        job_commits.push(began.elapsed());
                        let began = Instant::now();
                        handle.claim_execution(ocg::persistence::ClaimRequest {
                            job: id, allowed_pools: BTreeSet::from([target.executor.into()]),
                        }).await?.map_err(|refusal| format!("claim refused: {refusal:?}"))?;
                        job_commits.push(began.elapsed());
                        let mut frozen = handle.job_configuration(id).await?.ok_or("missing configuration")?.frozen;
                        frozen.llm.provider.endpoint = Some(format!("http://{provider_address}/v1"));
                        let mut conversation = ChatConversation {
                            messages: vec![ChatMessage::system("Shared system instructions. ".repeat(160)),
                                ChatMessage::user("First historical question."),
                                ChatMessage::assistant(Some("First historical answer.".into()), vec![]),
                                ChatMessage::user("Second historical question."),
                                ChatMessage::assistant(Some("Second historical answer.".into()), vec![]),
                                ChatMessage::user(format!("Current question for Job {index}."))],
                            tools: vec![serde_json::from_value(json!({"type":"function", "function":{"name":"lookup",
                                "description":"A shared lookup tool", "parameters":{"type":"object","properties":{},"additionalProperties":false}}}))?],
                            ..Default::default()
                        };
                        let mut sink = ProviderBenchSink {
                            database: handle.clone(), job: id, request: RequestNumber::FIRST,
                            recorder: Default::default(), commits: &mut commits, gap: None,
                        };
                        for round in 0..3 {
                            let response = provider_execution::execute_conversation(&frozen, &conversation,
                                &EnvironmentCredentialResolver, &ProviderExecutionConfig::default(), Some(&mut sink)).await?;
                            if let Some(error) = sink.gap.take() { return Err(error.into()); }
                            if response.requests_tools() != (round < 2) { return Err("mock response has wrong round".into()); }
                            let assistant = ChatMessage::assistant(Some(response.text.clone()),
                                response.tool_calls.iter().map(ChatToolCallWire::from).collect());
                            let stored = sink.recorder.response(&assistant).map_err(|error| error.message)?;
                            let digest = stored.digest();
                            let answer = NewProviderAnswer {
                                stat: NewAvailabilityStat { job: id, request: sink.request, observed_at: Timestamp::now(),
                                    observation: provider_execution::success_observation(&response) },
                                measurements: provider_execution::metrics(&response), response: stored,
                            };
                            ProviderBenchSink::commit(&mut sink.commits.answer, handle.record_provider_answer(answer))
                                .await.map_err(|error| error.message)?;
                            conversation.messages.push(assistant);
                            sink.recorder.remember_response(digest);
                            for call in response.tool_calls {
                                conversation.messages.push(ChatMessage::tool(call.id, format!("{{\"job\":{index},\"round\":{round}}}")));
                            }
                            if round < 2 { sink.request = sink.request.next().ok_or("request overflow")?; }
                        }
                        let began = Instant::now();
                        handle.finalize_job(id, JobFinalization::Completed(FinalResult {
                            summary: Some(format!("Done for Job {index}.")), code: None, goal_reached: None, value: None,
                        })).await?;
                        job_commits.push(began.elapsed());
                        ids.push((index, id));
                    }
                    Ok::<_, Box<dyn std::error::Error + Send + Sync>>((commits, job_commits, ids))
                }));
            }
            let mut commits = ProviderCommits::default();
            let mut job_commits = Vec::with_capacity(provider_jobs * 3);
            let mut ids = Vec::with_capacity(provider_jobs);
            for runner in runners {
                let (client_commits, mut client_job_commits, client_ids) =
                    runner.await.map_err(|_| "provider-record client panicked")??;
                commits.absorb(client_commits);
                job_commits.append(&mut client_job_commits);
                ids.extend(client_ids);
            }
            // The replay below checks each Job against the index its own
            // request named, so the merged order is the Job order again.
            ids.sort_unstable_by_key(|(index, _)| *index);
            let write_elapsed = started.elapsed();
            let runtime = ExecutionRuntime::start(handle.clone())?;
            const TOKEN: &str = "provider-record-benchmark-0123456789-ABCDEFGHIJKLMNOPQRSTUVWXYZ";
            let control = ControlPlane::start(ControlPlaneConfig::new(0, TOKEN)?, handle.clone(), runtime.scheduler())?;
            let client = ntex::client::Client::builder().build(ntex::SharedCfg::default()).await?;
            let mut gets = Vec::with_capacity(provider_jobs * 3);
            let gets_started = Instant::now();
            for (index, id) in ids {
                for request in 1..=3 {
                    let began = Instant::now();
                    let response = client.get(format!("http://{}/api/v1/jobs/{id}/requests/{request}", control.address()))
                        .set_header("Authorization", format!("Bearer {TOKEN}")).send().await.map_err(|error| error.to_string())?;
                    if response.status() != ntex::http::StatusCode::OK { return Err(format!("GET failed: {}", response.status()).into()); }
                    let value: Value = response.json().await?;
                    gets.push(began.elapsed());
                    if value["messages"].as_array().map(Vec::len) != Some(6 + (request as usize - 1) * 2)
                        || value["response"]["content"] != if request < 3 { "Checking.".to_owned() } else { format!("Done for Job {index}.") }
                        || value["tools"].as_array().map(Vec::len) != Some(1) {
                        return Err("GET did not reconstruct the mock request".into());
                    }
                }
            }
            let gets_elapsed = gets_started.elapsed();
            control.stop().await?;
            runtime.shutdown().await?;
            mock.stop(true).await;
            mock_finished.await.map_err(|_| "mock server panicked")??;
            drop(handle);
            worker.shutdown()?;
            let database = Database::open(directory.database_path().join("ocg.redb"))?;
            let tx = database.begin_read()?;
            let messages = tx.open_table(TableDefinition::<&[u8], &[u8]>::new("message"))?;
            let mut message_bytes = 0u64;
            for row in messages.iter()? {
                let (key, value) = row?;
                message_bytes += (key.value().len() + value.value().len()) as u64;
            }
            if messages.len()? != 7 + provider_jobs as u64 * 6 {
                return Err("unexpected shared prompts/tools/parameters or unique per-Job message rows".into());
            }
            let parameters = json!({"jobs": provider_jobs, "clients": clients, "requests_per_job":3,"history_turns":2,
                "shared_system_and_tools":true,"load_model":"closed_loop", "commit_latency":"durable command acknowledgement including transaction and queue"});
            let requests = (provider_jobs * 3) as f64;
            let provider_commits = commits.sent.len() + commits.output_flush.len() + commits.answer.len()
                + commits.delivered.len() + commits.usage.len() + commits.not_delivered.len();
            if commits.sent.len() != provider_jobs * 3 || commits.answer.len() != provider_jobs * 3
                || commits.delivered.len() != provider_jobs * 3 || commits.usage.len() != provider_jobs * 3
                || !commits.not_delivered.is_empty() || job_commits.len() != provider_jobs * 3 {
                return Err("unexpected provider commit counts".into());
            }
            println!("{}", json!({"type":"measurement", "name":"provider_record.storage", "parameters":parameters,
                "storage_dir": storage_dir.clone(),
                "commits_per_request": {"sent": commits.sent.len() as f64 / requests,
                    "output_flush": commits.output_flush.len() as f64 / requests,
                    "answer": commits.answer.len() as f64 / requests,
                    "job": job_commits.len() as f64 / requests,
                    "delivered": commits.delivered.len() as f64 / requests,
                    "usage": commits.usage.len() as f64 / requests,
                    "not_delivered": commits.not_delivered.len() as f64 / requests},
                "provider_commits_per_request": provider_commits as f64 / requests,
                "total_commits_per_request": (provider_commits + job_commits.len()) as f64 / requests,
                "message_table_bytes":message_bytes, "message_table_bytes_per_job": message_bytes as f64 / provider_jobs as f64,
                "message_rows":messages.len()?, "database_bytes":database_bytes(&directory)?, "write_elapsed_s":write_elapsed.as_secs_f64(),
                "commits_per_second": (provider_commits + job_commits.len()) as f64 / write_elapsed.as_secs_f64()}));
            for (kind, samples) in [("sent", commits.sent), ("output_flush", commits.output_flush),
                ("answer", commits.answer), ("delivered", commits.delivered), ("usage", commits.usage),
                ("not_delivered", commits.not_delivered)] {
                if !samples.is_empty() {
                    report(&format!("provider_record.{kind}_commit"), &parameters, samples, Some(write_elapsed));
                }
            }
            report("provider_record.job_commit", &parameters, job_commits, Some(write_elapsed));
            report("provider_record.GET_requests_n", &parameters, gets, Some(gets_elapsed));
        }
        Ok(())
    })
}
