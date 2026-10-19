//! Write-path and read-latency benchmark for the database worker.
//!
//! Run with `cargo bench --bench database_writes [-- serial|concurrent|reads]`.
//! It drives the public [`DatabaseHandle`] against a throwaway data
//! directory, so it measures the whole command path: admission, the write
//! worker, the redb commit and the reply. Commands are `create_project`
//! calls, which are one small transaction each.

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

use ocg::data_directory::DataDirectory;
use ocg::database::{DatabaseError, DatabaseHandle, DatabaseWorker};
use ocg::persistence::NewProject;

type BenchResult<T> = Result<T, Box<dyn std::error::Error>>;

struct Unpark(std::thread::Thread);

impl Wake for Unpark {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

/// Drives a database future on the calling thread. Every redb access happens
/// on a database thread, so no async runtime is needed.
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

fn percentile(sorted: &[Duration], fraction: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let last = sorted.len() - 1;
    let index = (last as f64 * fraction).round() as usize;
    sorted.get(index.min(last)).copied().unwrap_or_default()
}

fn report(label: &str, mut latencies: Vec<Duration>, elapsed: Duration) {
    latencies.sort();
    println!(
        "{label}: n={} elapsed={:.3}s throughput={:.0}/s p50={:?} p95={:?} p99={:?} max={:?}",
        latencies.len(),
        elapsed.as_secs_f64(),
        latencies.len() as f64 / elapsed.as_secs_f64(),
        percentile(&latencies, 0.50),
        percentile(&latencies, 0.95),
        percentile(&latencies, 0.99),
        latencies.last().copied().unwrap_or_default(),
    );
}

/// Closed-loop writers: each submits its next command when the previous one
/// is acknowledged, so the queue depth equals the number of writers.
fn write_workload(
    handle: &DatabaseHandle,
    writers: usize,
    per_writer: usize,
    tag: &str,
) -> BenchResult<(Vec<Duration>, Duration)> {
    let latencies = Arc::new(Mutex::new(Vec::new()));
    let start = Instant::now();
    let threads: Vec<_> = (0..writers)
        .map(|writer| {
            let handle = handle.clone();
            let latencies = latencies.clone();
            let tag = tag.to_string();
            std::thread::spawn(move || -> Result<(), DatabaseError> {
                let mut local = Vec::with_capacity(per_writer);
                for index in 0..per_writer {
                    let began = Instant::now();
                    block_on(handle.create_project(NewProject {
                        name: format!("{tag}-{writer}-{index}"),
                        description: Some("benchmark".to_string()),
                        root: format!("/ocg-bench/{tag}-{writer}-{index}"),
                        commands: Vec::new(),
                    }))?;
                    local.push(began.elapsed());
                }
                latencies
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .extend(local);
                Ok(())
            })
        })
        .collect();
    for thread in threads {
        thread.join().map_err(|_| "a writer thread panicked")??;
    }
    let elapsed = start.elapsed();
    let all = std::mem::take(&mut *latencies.lock().unwrap_or_else(PoisonError::into_inner));
    Ok((all, elapsed))
}

fn read_while_writing(handle: &DatabaseHandle) -> BenchResult<()> {
    let stop = Arc::new(AtomicBool::new(false));
    let read_latencies = Arc::new(Mutex::new(Vec::new()));
    let readers: Vec<_> = (0..2)
        .map(|_| {
            let handle = handle.clone();
            let stop = stop.clone();
            let read_latencies = read_latencies.clone();
            std::thread::spawn(move || -> Result<(), DatabaseError> {
                let mut local = Vec::new();
                while !stop.load(Ordering::Relaxed) {
                    let began = Instant::now();
                    block_on(handle.projects(None, 20))?;
                    local.push(began.elapsed());
                }
                read_latencies
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .extend(local);
                Ok(())
            })
        })
        .collect();
    let written = write_workload(handle, 16, 60, "mixed");
    stop.store(true, Ordering::Relaxed);
    for reader in readers {
        reader.join().map_err(|_| "a reader thread panicked")??;
    }
    let (latencies, elapsed) = written?;
    report("mixed writes, 16 writers", latencies, elapsed);
    let reads = std::mem::take(
        &mut *read_latencies
            .lock()
            .unwrap_or_else(PoisonError::into_inner),
    );
    report("mixed reads,   2 readers", reads, elapsed);

    let began = Instant::now();
    let mut alone = Vec::new();
    while began.elapsed() < Duration::from_secs(2) {
        let read_began = Instant::now();
        block_on(handle.projects(None, 20))?;
        alone.push(read_began.elapsed());
    }
    report("reads alone,    1 reader ", alone, began.elapsed());
    Ok(())
}

fn main() -> BenchResult<()> {
    // `cargo bench` appends `--bench`; the first plain argument selects a section.
    let section = std::env::args()
        .skip(1)
        .find(|argument| !argument.starts_with("--"))
        .unwrap_or_else(|| "all".to_string());
    let selected = |name: &str| section == "all" || section == name;

    let root = std::env::temp_dir().join(format!("ocg-bench-{}", std::process::id()));
    std::fs::remove_dir_all(&root).ok();
    let directory = DataDirectory::bootstrap(root.clone().into_os_string())?;
    let worker = DatabaseWorker::start(&directory)?;
    let handle = worker.handle();

    let outcome = (|| -> BenchResult<()> {
        write_workload(&handle, 1, 20, "warm")?;
        if selected("serial") {
            let (latencies, elapsed) = write_workload(&handle, 1, 300, "serial")?;
            report("serial,      1 writer ", latencies, elapsed);
        }
        if selected("concurrent") {
            for writers in [4usize, 16, 64] {
                let per_writer = 600 / writers.min(60) + 20;
                let (latencies, elapsed) =
                    write_workload(&handle, writers, per_writer, &format!("c{writers}"))?;
                report(
                    &format!("concurrent, {writers:>2} writers"),
                    latencies,
                    elapsed,
                );
            }
        }
        if selected("reads") {
            read_while_writing(&handle)?;
        }
        let size = std::fs::metadata(ocg_database_file(&directory)?)?.len();
        println!("database file: {size} bytes");
        Ok(())
    })();
    worker.shutdown()?;
    std::fs::remove_dir_all(&root).ok();
    outcome
}

fn ocg_database_file(directory: &DataDirectory) -> BenchResult<std::path::PathBuf> {
    let mut files = std::fs::read_dir(directory.database_path())?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "redb")
        });
    files.next().ok_or_else(|| "no database file found".into())
}
