use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use ocg::system::data_directory::DataDirectory;
use serde_json::{Value, json};

#[path = "mod.rs"]
pub mod benchmark;

pub mod backend_process;
pub mod fixture;
mod http_client;
pub mod metadata;
pub mod mock_provider;
pub mod reporting;
pub mod summary;
pub mod workload;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
static LEGACY_CONTEXT: OnceLock<Value> = OnceLock::new();

struct TemporaryRoot(PathBuf);

impl TemporaryRoot {
    fn new(directory: &Path) -> Result<Self> {
        let stamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let root = directory.join(format!("ocg-bench-{}-{stamp}", std::process::id()));
        std::fs::create_dir_all(&root)?;
        Ok(Self(root))
    }
}

impl Drop for TemporaryRoot {
    fn drop(&mut self) {
        if self.0.as_os_str().is_empty() {
            return;
        }
        if let Err(error) = std::fs::remove_dir_all(&self.0) {
            eprintln!("benchmark cleanup failed for {}: {error}", self.0.display());
        }
    }
}

#[derive(Clone)]
struct Options {
    section: String,
    iterations: usize,
    trials: usize,
    warmup: usize,
    jobs: usize,
    files: usize,
    clients: Vec<usize>,
    payloads: Vec<usize>,
    outputs: Vec<usize>,
    directory: PathBuf,
    deadline: Duration,
    page_size: usize,
    writer_jobs: usize,
    stream_chunks: usize,
    stream_pace: Duration,
    mode: String,
    provider_failure: bool,
    child_hang: bool,
}

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
            trials: 3,
            warmup: 1,
            jobs: 1_000,
            files: 1_000,
            clients: vec![1, 8],
            payloads: vec![1_024, 65_536],
            outputs: vec![3],
            deadline: Duration::from_secs(60),
            page_size: 20,
            writer_jobs: 4,
            stream_chunks: 4,
            stream_pace: Duration::from_millis(5),
            mode: "smoke".into(),
            provider_failure: false,
            child_hang: false,
        };
        let mut arguments = std::env::args().skip(1);
        let mut section_seen = false;
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--bench" => {}
                "--help" | "-h" => {
                    println!(
                        "cargo bench --features benchmarks --bench backend -- \\\n                         [all|startup|memory|admission|throughput|streaming|history|database|search|search_scale] \\\n                         [--jobs N] [--iterations N] [--trials N] [--warmup N] [--clients 1,8,16] \\\n                         [--files N] [--dir PATH] [--deadline-ms N] [--mode smoke|reference] \\\n                         [--page-size N] [--writer-jobs N] [--stream-chunks N] [--stream-pace-ms N]\n\
                         Every measurement runs against the production host over its authenticated\n\
                         HTTP surface, with a mock provider and real workspace tools; `search_scale`\n\
                         builds its own FTS index directly. Runtime workloads are closed-loop with\n\
                         bounded in-flight jobs. Setup and warm-up are excluded. The reference job\n\
                         makes 3 model requests, 2 real workspace reads, and one validated\n\
                         completion. JSONL is written to stdout; unsuccessful required work returns\n\
                         nonzero. `summary --concurrency N` reads JSONL on stdin. --provider-failure\n\
                         and --child-hang are explicit negative-check controls; --child-hang uses\n\
                         --deadline-ms for cleanup."
                    );
                    return Ok(None);
                }
                "--dir" => {
                    options.directory =
                        PathBuf::from(arguments.next().ok_or("missing option value")?);
                }
                "--iterations" | "--files" | "--trials" | "--page-size" | "--writer-jobs"
                | "--stream-chunks" => {
                    let value: usize = arguments.next().ok_or("missing option value")?.parse()?;
                    if value == 0 {
                        return Err("benchmark sizes must be positive".into());
                    }
                    match argument.as_str() {
                        "--iterations" => options.iterations = value,
                        "--files" => options.files = value,
                        "--trials" => options.trials = value,
                        "--page-size" => options.page_size = value,
                        "--writer-jobs" => options.writer_jobs = value,
                        _ => options.stream_chunks = value,
                    }
                }
                "--warmup" => {
                    options.warmup = arguments.next().ok_or("missing option value")?.parse()?;
                }
                "--deadline-ms" | "--stream-pace-ms" => {
                    let milliseconds: u64 =
                        arguments.next().ok_or("missing option value")?.parse()?;
                    if milliseconds == 0 {
                        return Err("benchmark durations must be positive".into());
                    }
                    match argument.as_str() {
                        "--deadline-ms" => options.deadline = Duration::from_millis(milliseconds),
                        _ => options.stream_pace = Duration::from_millis(milliseconds),
                    }
                }
                "--mode" => {
                    options.mode = arguments.next().ok_or("missing option value")?;
                    if !matches!(options.mode.as_str(), "smoke" | "reference") {
                        return Err("--mode must be smoke or reference".into());
                    }
                }
                "--provider-failure" => options.provider_failure = true,
                "--child-hang" => options.child_hang = true,
                "--jobs" => {
                    let value: usize = arguments.next().ok_or("missing option value")?.parse()?;
                    if value == 0 {
                        return Err("benchmark sizes must be positive".into());
                    }
                    options.jobs = value;
                }
                "--clients" | "--payloads" | "--outputs" => {
                    let value = sizes(&arguments.next().ok_or("missing option value")?)?;
                    match argument.as_str() {
                        "--clients" => options.clients = value,
                        "--payloads" => options.payloads = value,
                        _ => options.outputs = value,
                    }
                }
                "all" | "search" | "search_scale" | "summary" | "startup" | "memory"
                | "admission" | "throughput" | "streaming" | "history" | "database"
                    if !section_seen =>
                {
                    options.section = argument;
                    section_seen = true;
                }
                _ => return Err(format!("unknown argument: {argument}; use --help").into()),
            }
        }
        if options.page_size > 100 {
            return Err("--page-size must not exceed the production maximum of 100".into());
        }
        std::fs::create_dir_all(&options.directory)?;
        options.directory = options.directory.canonicalize()?;
        Ok(Some(options))
    }
}

fn report(
    name: &str,
    parameters: &Value,
    mut samples: Vec<Duration>,
    elapsed: Option<Duration>,
) -> Result<()> {
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
            .map(|sample| sample.as_secs_f64() * 1000.0)
    };
    let mut value = json!({
        "type": "measurement", "name": name, "parameters": parameters,
        "status": if samples.is_empty() { "unavailable" } else { "success" },
        "samples": samples.len(),
        "outcomes": {"attempted": samples.len(), "successful": samples.len(),
            "rejected": 0, "failed": 0, "timed_out": 0, "unfinished": 0},
        "context": LEGACY_CONTEXT.get(), "trial": null,
        "trial_scope": "component samples aggregated in one section run",
        "elapsed_s": elapsed.map(|duration| duration.as_secs_f64()),
    });
    if let Some(object) = value.as_object_mut() {
        if samples.is_empty() {
            object.insert(
                "unavailable_reason".into(),
                json!("no successful latency samples"),
            );
        } else {
            object.insert("p50_ms".into(), json!(percentile(50)));
            let reference = LEGACY_CONTEXT
                .get()
                .is_some_and(|context| context["mode"] == "reference");
            if reference && samples.len() >= 100 {
                object.insert("p95_ms".into(), json!(percentile(95)));
            }
            if reference && samples.len() >= 1_000 {
                object.insert("p99_ms".into(), json!(percentile(99)));
            }
            object.insert(
                "max_ms".into(),
                json!(samples.last().map(|sample| sample.as_secs_f64() * 1000.0)),
            );
            if let Some(elapsed) = elapsed {
                object.insert(
                    "throughput_per_second".into(),
                    json!(samples.len() as f64 / elapsed.as_secs_f64()),
                );
            }
        }
    }
    println!("{value}");
    if samples.is_empty() && !name.starts_with("maintenance.") {
        return Err(format!("{name}: no successful samples where work was required").into());
    }
    Ok(())
}

fn measure<T>(
    iterations: usize,
    warmup: usize,
    mut operation: impl FnMut() -> Result<T>,
) -> Result<(Vec<Duration>, Duration)> {
    for _ in 0..warmup {
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

#[cfg(unix)]
const SEARCH_QUERIES: [(&str, &str); 7] = [
    ("rare", "rarebeacon"),
    ("missing", "neverpresenttoken"),
    ("common", "commonbeacon"),
    ("and", "rarebeacon alpha"),
    ("phrase", "\"alpha beta\""),
    ("broad_phrase", "\"commonbeacon marker\""),
    ("broad_phrase_zero", "\"marker commonbeacon\""),
];

#[cfg(unix)]
fn validate_corpus_plan(
    files: usize,
    sizes: [usize; 4],
    limits: &ocg::service::fts::FtsLimits,
    within_content_bound: bool,
) -> Result<u64> {
    if files == 0 || files > limits.files {
        return Err(format!(
            "--files {files} is outside the production file count bound {}; choose 1..={}",
            limits.files, limits.files
        )
        .into());
    }
    let mut planned_bytes = 0u64;
    for file in 0..files {
        let size = sizes[file % 4];
        if size == 0 || size > limits.file_bytes {
            return Err(format!(
                "--files {files} plans {size} bytes for file {file}, exceeding the production per-file limit of {} bytes; increase --files or reduce the corpus size",
                limits.file_bytes
            )
            .into());
        }
        planned_bytes = planned_bytes
            .checked_add(size as u64)
            .ok_or("planned corpus size overflow")?;
    }
    if within_content_bound && planned_bytes > limits.content_bytes {
        return Err(format!(
            "--files {files} plans {planned_bytes} bytes, exceeding the production total-content limit {}; increase --files or reduce the corpus size",
            limits.content_bytes
        )
        .into());
    }
    Ok(planned_bytes)
}

#[cfg(unix)]
fn corpus(source: &std::path::Path, files: usize, sizes: [usize; 4]) -> Result<()> {
    for file in 0..files {
        let group = source.join(format!("group{:03}", file / 40));
        std::fs::create_dir_all(&group)?;
        let target = sizes[file % 4];
        let mut content = String::from("// commonbeacon marker\n");
        if file == 0 {
            content.push_str("// rarebeacon alpha beta HTTPServer\n");
        }
        while content.len() < target {
            content.push_str(&format!("pub fn itemcode{file:05x}part{:05x}(state: &mut State) -> Result<u64, Error> {{ let packet = state.apply(value)?; Ok(packet) }}\n", content.len()));
        }
        content.truncate(target);
        std::fs::write(group.join(format!("unit{file:05}.rs")), content)?;
    }
    Ok(())
}

#[cfg(unix)]
fn queries(
    index: &ocg::service::fts::WorkspaceFts,
    prefix: &str,
    parameters: &Value,
    iterations: usize,
    warmup: usize,
    strict: bool,
) -> Result<()> {
    for (class, query) in SEARCH_QUERIES {
        let value = index.search(query, 20)?;
        let results = value["results"]
            .as_array()
            .ok_or("search omitted results")?;
        let files = parameters["files"]
            .as_u64()
            .and_then(|value| usize::try_from(value).ok())
            .ok_or("search query parameters omitted the fixture file count")?;
        if strict {
            workload::validate_query(class, &value, files)
                .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> { error.into() })?;
        }
        let (samples, elapsed) = measure(iterations, warmup, || {
            let value = index.search(query, 20)?;
            if strict {
                workload::validate_query(class, &value, files).map_err(
                    |error| -> Box<dyn std::error::Error + Send + Sync> { error.into() },
                )?;
            }
            Ok(value)
        })?;
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
        )?;
    }
    Ok(())
}

#[cfg(unix)]
fn search_scale(options: &Options) -> Result<()> {
    use ocg::service::fts::{FtsLimits, WorkspaceFts};
    use ocg::system::executor::workspace_read::Workspace;

    let limits = FtsLimits::default();
    let plans = [1u64, 4, 16, 64]
        .map(|mib| mib * 1024 * 1024)
        .map(|target| {
            let within = target <= limits.content_bytes;
            let per_file = usize::try_from(target / options.files as u64)
                .map_err(|_| "corpus file size does not fit this platform")?
                .max(256);
            let sizes = [per_file / 4, per_file / 2, per_file, per_file * 2];
            let planned_bytes = validate_corpus_plan(options.files, sizes, &limits, within)?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>((
                target,
                sizes,
                planned_bytes,
                within,
            ))
        })
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
    for (target, sizes, planned_bytes, within) in plans {
        let label = format!("{}mib", target / (1024 * 1024));
        let root = TemporaryRoot::new(&options.directory)?;
        let directory = DataDirectory::bootstrap(root.0.join("data").into_os_string())?;
        let source = root.0.join("workspace");
        std::fs::create_dir(&source)?;
        corpus(&source, options.files, sizes)?;
        let workspace = Workspace::authorize(&source, &directory)?;
        let parameters = json!({"corpus": label, "requested_corpus_bytes": target,
            "effective_corpus_bytes": planned_bytes, "file_size_distribution": sizes,
            "files": options.files, "cache": "warm_os_cache",
            "production_file_bytes_bound": limits.file_bytes,
            "production_content_bytes_bound": limits.content_bytes, "within_bound": within,
            "deliberate_total_content_overflow": !within});
        let (samples, elapsed) = measure(options.trials, options.warmup, || {
            let index = WorkspaceFts::new(workspace.clone(), limits);
            let built = index.build()?;
            if built.coverage.files_indexed == 0 {
                return Err(format!("empty search corpus: {:?}", built.coverage).into());
            }
            if within && (!built.coverage.complete || built.coverage.files_indexed != options.files)
            {
                return Err(format!("partial search fixture: {:?}", built.coverage).into());
            }
            if !within && built.coverage.limit != Some(ocg::service::fts::BuildLimit::ContentBytes)
            {
                return Err(format!(
                    "oversized corpus did not reach the content-byte limit: {:?}",
                    built.coverage
                )
                .into());
            }
            Ok(built)
        })?;
        report(
            &format!("search_scale.build.{label}"),
            &parameters,
            samples,
            Some(elapsed),
        )?;
        let index = WorkspaceFts::new(workspace, limits);
        let built = index.build()?;
        println!(
            "{}",
            json!({"type": "fixture", "name": "search_scale.index", "corpus": label,
            "requested_files": options.files, "generated_files": options.files,
            "generated_bytes": planned_bytes,
            "files": built.coverage.files_indexed, "content_bytes": built.coverage.content_bytes_indexed,
            "tokens": built.coverage.tokens_indexed, "terms": built.coverage.terms,
            "memory_bytes_approx": built.memory_bytes_approx,
            "complete": built.coverage.complete, "limit": format!("{:?}", built.coverage.limit),
            "skipped": format!("{:?}", built.coverage.skipped)})
        );
        queries(
            &index,
            &format!("search_scale.{label}"),
            &parameters,
            options.iterations,
            options.warmup,
            within,
        )?;
        let (samples, elapsed) = measure(options.trials, options.warmup, || Ok(index.rebuild()?))?;
        report(
            &format!("search_scale.full_rebuild.{label}"),
            &parameters,
            samples,
            Some(elapsed),
        )?;
    }
    Ok(())
}

fn main() -> Result<()> {
    if let Some("summary") = std::env::args().nth(1).as_deref() {
        return summary::run_summary();
    }
    let Some(options) = Options::parse()? else {
        return Ok(());
    };
    let environment_metadata = metadata::BenchmarkMetadata::capture(
        options.directory.clone(),
        if cfg!(debug_assertions) {
            "debug"
        } else {
            "bench"
        },
    )?;
    let unused_options: &[&str] = match options.section.as_str() {
        "search_scale" => &[
            "jobs",
            "clients",
            "payloads",
            "outputs",
            "deadline_ms",
            "page_size",
            "writer_jobs",
            "stream_chunks",
            "stream_pace_ms",
        ],
        "search" => &[
            "jobs",
            "payloads",
            "outputs",
            "deadline_ms",
            "page_size",
            "writer_jobs",
            "stream_chunks",
            "stream_pace_ms",
        ],
        "history" => &["payloads", "outputs"],
        "database" => &[
            "iterations",
            "payloads",
            "outputs",
            "warmup",
            "page_size",
            "writer_jobs",
        ],
        "startup" | "memory" | "admission" | "throughput" | "streaming" => &[
            "iterations",
            "payloads",
            "outputs",
            "page_size",
            "writer_jobs",
        ],
        "all" => &[
            "deadline_ms",
            "page_size",
            "writer_jobs",
            "stream_chunks",
            "stream_pace_ms",
        ],
        _ => &[],
    };
    let applied = |name: &str, value: Value| {
        if unused_options.contains(&name) {
            Value::Null
        } else {
            value
        }
    };
    let applied_options = json!({"section": options.section,
        "iterations_per_client": applied("iterations", json!(options.iterations)),
        "jobs": applied("jobs", json!(options.jobs)),
        "files": applied("files", json!(options.files)), "clients": applied("clients", json!(options.clients)),
        "payloads": applied("payloads", json!(options.payloads)),
        "progress_outputs_per_job": applied("outputs", json!(options.outputs)),
        "trials": applied("trials", json!(options.trials)), "warmup": applied("warmup", json!(options.warmup)),
        "deadline_ms": applied("deadline_ms", json!(options.deadline.as_millis())),
        "page_size": applied("page_size", json!(options.page_size)),
        "writer_jobs": applied("writer_jobs", json!(options.writer_jobs)),
        "stream_chunks": applied("stream_chunks", json!(options.stream_chunks)),
        "stream_pace_ms": applied("stream_pace_ms", json!(options.stream_pace.as_millis())),
        "unused_options": unused_options,
        "all_scope": if options.section == "all" { Some("options apply to different legacy sections; see each record") } else { None }});
    let _ = LEGACY_CONTEXT.set(json!({
        "run_id": uuid::Uuid::now_v7().to_string(), "section": options.section,
        "mode": options.mode, "warm_up": applied("warmup", json!(options.warmup)),
        "requested": {"jobs": options.jobs, "iterations": options.iterations,
            "clients": options.clients, "files": options.files, "trials": options.trials},
        "applied": applied_options, "environment": environment_metadata.to_json(),
    }));
    let is_runtime = matches!(
        options.section.as_str(),
        "startup" | "memory" | "admission" | "throughput" | "streaming" | "history" | "database"
    );
    let scope = if is_runtime {
        "authenticated HTTP, scheduler dispatch, execution runtime, mock provider, real workspace tools, and production persistence"
    } else if options.section == "search" {
        "authenticated HTTP over production WorkspaceFts; the host indexes and answers, the harness measures end to end"
    } else if options.section == "search_scale" {
        "direct production WorkspaceFts build and concurrent warm queries at growing corpus sizes; excludes HTTP and providers"
    } else if options.section == "all" {
        "every section, each recording its own scope"
    } else {
        "authenticated HTTP against the production host; excludes in-process shortcuts"
    };
    reporting::Record::Environment(Box::new(reporting::EnvironmentRecord {
        metadata: environment_metadata,
        workload_model: "closed_loop".into(),
        scope: scope.into(),
        requested: json!({"section": options.section, "iterations": options.iterations,
            "trials": options.trials, "warmup": options.warmup, "jobs": options.jobs,
            "files": options.files, "clients": options.clients, "mode": options.mode}),
        applied: applied_options,
    }))
    .emit();
    if options.section == "summary" {
        return summary::run_summary();
    }
    if options.section == "search_scale" {
        #[cfg(unix)]
        {
            return search_scale(&options);
        }
        #[cfg(not(unix))]
        {
            return Err("search_scale requires Unix".into());
        }
    }
    let result = if options.section == "all" {
        run_every_section(&options)
    } else {
        run_measurement_section(&options)
    };
    if let Err(error) = &result {
        println!(
            "{}",
            json!({"type": "diagnostic", "section": options.section,
            "trial": null, "status": "failed", "error": error.to_string(),
            "applied": {"jobs": options.jobs, "iterations": options.iterations,
                "clients": options.clients, "files": options.files, "trials": options.trials},
            "outcomes": {"attempted": 0, "successful": 0, "rejected": 0,
                "failed": 0, "timed_out": 0, "unfinished": 0},
            "outcome_scope": "unavailable for section/setup failure"})
        );
    }
    result
}

fn run_every_section(options: &Options) -> Result<()> {
    const HOST_SECTIONS: [&str; 6] = [
        "startup",
        "admission",
        "throughput",
        "streaming",
        "history",
        "database",
    ];
    for section in HOST_SECTIONS {
        let mut run = options.clone();
        run.section = section.to_owned();
        run_measurement_section(&run)?;
    }
    let mut run = options.clone();
    run.section = "search".to_owned();
    run_measurement_section(&run)?;
    #[cfg(unix)]
    {
        let mut run = options.clone();
        run.section = "search_scale".to_owned();
        search_scale(&run)?;
    }
    Ok(())
}

fn run_measurement_section(options: &Options) -> Result<()> {
    let options = options.clone();
    ntex::rt::System::new("benchmark-client", ntex::rt::DefaultRuntime)
        .block_on(async move { workload::run(&options).await })
}
