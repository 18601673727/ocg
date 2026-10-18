use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::sync::{Arc, Barrier};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use ocg::data_directory::DataDirectory;
use ocg::database::{DatabaseHandle, DatabaseWorker};
use ocg::persistence::{
    ExecutorId, FinalResult, IntermediateOutputKind, JobFinalization, JobId, JobInputDraft,
    NewExecutor, NewJob, NewJobOutput,
};
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
    fn new() -> Result<Self> {
        let stamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let root = std::env::temp_dir().join(format!("ocg-backend-{}-{stamp}", std::process::id()));
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
            section: "all".into(),
            iterations: 50,
            jobs: 1_000,
            files: 1_000,
            clients: vec![1, 8],
            payloads: vec![1_024, 65_536],
            outputs: vec![3],
        };
        let mut arguments = std::env::args().skip(1);
        let mut section_seen = false;
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--bench" => {}
                "--help" | "-h" => {
                    println!(
                        "cargo bench --bench backend -- \
                         [all|jobs|reads|search|search_scale|mixed|maintenance] \
                         [--iterations N] [--jobs N] [--files N] \
                         [--clients 1,8] [--payloads 1024,65536] [--outputs 3]\n\
                         Defaults: all, 50 iterations per client, 1000 read-fixture jobs, \
                         1000 search files, 1 and 8 clients, 1/64 KiB payloads, 3 progress \
                         outputs per job.\n\
                         JSONL on stdout; setup/warmup excluded. Errors abort the run.\n\
                         Jobs: create+claim, N progress outputs, finalize; closed loop, one \
                         measurement per command stage and one for the whole lifecycle.\n\
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
                         No HTTP, scheduler dispatch, external providers or LLM inference."
                    );
                    return Ok(None);
                }
                "--iterations" | "--jobs" | "--files" => {
                    let value: usize = arguments.next().ok_or("missing option value")?.parse()?;
                    if value == 0 {
                        return Err("benchmark sizes must be positive".into());
                    }
                    match argument.as_str() {
                        "--iterations" => options.iterations = value,
                        "--jobs" => options.jobs = value,
                        _ => options.files = value,
                    }
                }
                "--clients" | "--payloads" | "--outputs" => {
                    let value = sizes(&arguments.next().ok_or("missing option value")?)?;
                    match argument.as_str() {
                        "--clients" => options.clients = value,
                        "--payloads" => options.payloads = value,
                        _ => options.outputs = value,
                    }
                }
                "all" | "jobs" | "reads" | "search" | "search_scale" | "mixed" | "maintenance"
                    if !section_seen =>
                {
                    options.section = argument;
                    section_seen = true;
                }
                _ => return Err(format!("unknown argument: {argument}; use --help").into()),
            }
        }
        if options.jobs > 4_000 || options.files > 4_000 {
            return Err("--jobs and --files must be at most 4000 (production limits)".into());
        }
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
    operation: impl FnOnce(&DatabaseHandle, &DataDirectory) -> Result<()>,
) -> Result<()> {
    let root = TemporaryRoot::new()?;
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

fn executor(handle: &DatabaseHandle) -> Result<ExecutorId> {
    Ok(block_on(handle.create_executor(NewExecutor {
        name: "benchmark".into(),
        concurrency_limit: NonZeroU32::new(8).ok_or("invalid concurrency")?,
        llm_transports: BTreeSet::new(),
    }))?)
}

fn job(handle: &DatabaseHandle) -> NewJob {
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
        configuration: None,
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
    executor: ExecutorId,
    payload: &str,
    outputs: usize,
    timings: &mut Timings,
) -> Result<()> {
    let started = Instant::now();
    let began = Instant::now();
    let id = block_on(handle.create_local_job(job(handle), executor))?;
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
                with_database(|handle, _directory| {
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

/// Job lifecycles written while the views a dashboard polls are read: what a
/// reader pays while the write worker is busy, and what the writer pays for
/// the readers ahead of it.
fn mixed(options: &Options) -> Result<()> {
    use std::sync::atomic::{AtomicBool, Ordering};

    let payload_bytes = options.payloads.first().copied().unwrap_or(1_024);
    let outputs = options.outputs.first().copied().unwrap_or(3);
    for writers in options.clients.iter().copied() {
        with_database(|handle, _directory| {
            let executor = executor(handle)?;
            let mut ids = Vec::with_capacity(options.jobs);
            for _ in 0..options.jobs {
                ids.push(block_on(handle.create_job(job(handle)))?);
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
        with_database(|handle, _directory| {
            let mut ids = Vec::with_capacity(count);
            for _ in 0..count {
                ids.push(block_on(handle.create_job(job(handle)))?);
            }
            let first = ids.first().copied().ok_or("empty job fixture")?;
            block_on(handle.start_job(first, executor(handle)?))?;
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

    let root = TemporaryRoot::new()?;
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
        let root = TemporaryRoot::new()?;
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
fn maintenance(_options: &Options) -> Result<()> {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Mutex, PoisonError};

    use ocg::database::CompactionOutcome;

    const TARGET_FILE_BYTES: u64 = 72 * 1024 * 1024;
    const PAYLOAD_BYTES: usize = 65_536;
    const OUTPUTS: usize = 3;

    with_database(|handle, directory| {
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

        let observed = block_on(handle.create_local_job(job(handle), executor))?;
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
    println!(
        "{}",
        json!({"type": "environment", "os": std::env::consts::OS, "arch": std::env::consts::ARCH,
        "compiler": String::from_utf8(compiler.stdout)?.trim(),
        "available_parallelism": std::thread::available_parallelism()?.get(),
        "section": options.section, "iterations_per_client": options.iterations,
        "jobs": options.jobs, "files": options.files,
        "clients": options.clients, "payloads": options.payloads,
        "progress_outputs_per_job": options.outputs,
        "load_model": "closed_loop", "latency_unit": "ms", "temporary_storage": std::env::temp_dir(),
        "scope": "public database and FTS APIs; excludes HTTP, scheduler dispatch and providers"})
    );
    if options.selected("jobs") {
        jobs(&options)?;
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
