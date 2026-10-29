//! Shared benchmark helpers and utilities.
//!
//! Everything here is deliberately free of async runtime and HTTP client
//! dependencies: the harness drives the backend over its own HTTP surface
//! with the `ntex` client the production code already uses, and the helpers
//! below are the plain synchronous parts both roles share.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Temporary isolated data directory for a benchmark run.
///
/// Every measurement owns one, so no run reads another run's state and the
/// footprint a run reports is the footprint of that run alone.
pub struct TemporaryRoot(PathBuf);

impl TemporaryRoot {
    pub fn new(directory: &Path) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let stamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let root = directory.join(format!("ocg-bench-{}-{stamp}", std::process::id()));
        std::fs::create_dir_all(&root)?;
        Ok(Self(root))
    }

    pub fn path(&self) -> &Path {
        &self.0
    }

    /// Keeps the directory after the run, for a report that needs the files.
    pub fn into_path(mut self) -> PathBuf {
        std::mem::take(&mut self.0)
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

/// Trial identifier for multi-trial measurements.
#[derive(Clone, Copy, Debug)]
pub struct TrialId(pub usize);

impl TrialId {
    pub fn index(&self) -> usize {
        self.0
    }
}

/// Outcome counts for a workload measurement.
///
/// A measurement that drops an operation must say so: a latency percentile
/// computed over the survivors alone would otherwise describe a smaller
/// workload than the one the run applied.
#[derive(Clone, Copy, Debug, Default)]
pub struct OutcomeCounts {
    pub attempted: usize,
    pub successful: usize,
    pub rejected: usize,
    pub failed: usize,
    pub timed_out: usize,
    pub unfinished: usize,
}

impl OutcomeCounts {
    pub fn new(attempted: usize) -> Self {
        Self {
            attempted,
            successful: attempted,
            ..Default::default()
        }
    }

    /// Records one operation that did not complete normally.
    pub fn record_failure(&mut self, kind: Outcome) {
        self.attempted += 1;
        match kind {
            Outcome::Success => self.successful += 1,
            Outcome::Rejected => self.rejected += 1,
            Outcome::Failed => self.failed += 1,
            Outcome::TimedOut => self.timed_out += 1,
            Outcome::Unfinished => self.unfinished += 1,
        }
    }

    pub fn record_success(&mut self) {
        self.attempted += 1;
        self.successful += 1;
    }
}

/// How one attempted operation ended.
#[derive(Clone, Copy, Debug)]
pub enum Outcome {
    Success,
    Rejected,
    Failed,
    TimedOut,
    Unfinished,
}

/// Sample collection for latency measurements.
#[derive(Clone, Debug, Default)]
pub struct Samples {
    pub durations: Vec<Duration>,
    pub total_elapsed: Duration,
}

impl Samples {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_durations(mut durations: Vec<Duration>) -> Self {
        durations.sort_unstable();
        Self {
            durations,
            total_elapsed: Duration::ZERO,
        }
    }

    pub fn add(&mut self, duration: Duration) {
        self.durations.push(duration);
    }

    pub fn extend(&mut self, other: Samples) {
        self.durations.extend(other.durations);
        self.total_elapsed += other.total_elapsed;
    }

    pub fn count(&self) -> usize {
        self.durations.len()
    }

    /// The percentile of the sorted samples, by the nearest-rank rule.
    pub fn percentile(&self, p: usize) -> Duration {
        if self.durations.is_empty() {
            return Duration::ZERO;
        }
        let index = self
            .durations
            .len()
            .saturating_mul(p)
            .div_ceil(100)
            .saturating_sub(1);
        self.durations.get(index).copied().unwrap_or_default()
    }

    pub fn median(&self) -> Duration {
        self.percentile(50)
    }

    pub fn p95(&self) -> Duration {
        self.percentile(95)
    }

    pub fn p99(&self) -> Duration {
        self.percentile(99)
    }

    pub fn max(&self) -> Duration {
        self.durations.last().copied().unwrap_or_default()
    }

    pub fn min(&self) -> Duration {
        self.durations.first().copied().unwrap_or_default()
    }

    pub fn sort(&mut self) {
        self.durations.sort_unstable();
    }

    /// The mean of the samples, for a throughput figure that a median cannot
    /// express.
    pub fn mean(&self) -> Option<Duration> {
        if self.durations.is_empty() {
            return None;
        }
        let total: Duration = self.durations.iter().sum();
        Some(total / self.durations.len() as u32)
    }
}

/// Measurement timing window boundaries.
///
/// The window is what the run actually spent inside the measured section, so
/// a rate derived from it covers the same work the samples describe.
#[derive(Clone, Copy, Debug)]
pub struct TimingWindow {
    pub started: Instant,
    pub ended: Instant,
}

impl TimingWindow {
    pub fn start() -> Self {
        let now = Instant::now();
        Self {
            started: now,
            ended: now,
        }
    }

    pub fn finish(&mut self) {
        self.ended = Instant::now();
    }

    pub fn elapsed(&self) -> Duration {
        self.ended.saturating_duration_since(self.started)
    }
}

/// System resource usage measurement.
#[derive(Clone, Copy, Debug)]
pub struct ResourceUsage {
    pub current_rss_bytes: Option<u64>,
    pub peak_rss_bytes: Option<u64>,
    pub user_cpu_seconds: Option<f64>,
    pub system_cpu_seconds: Option<f64>,
}

impl ResourceUsage {
    /// Sample current process resource usage.
    pub fn sample_current() -> Self {
        Self {
            current_rss_bytes: current_rss(),
            peak_rss_bytes: None,
            user_cpu_seconds: None,
            system_cpu_seconds: None,
        }
    }

    /// Sample lifetime resource usage (includes peak).
    ///
    /// The peak is a lifetime figure: it covers everything the process ever
    /// did, setup and warm-up included. A report that uses it says so.
    pub fn sample_lifetime() -> Self {
        let (peak, user, system) = lifetime_rusage();
        Self {
            current_rss_bytes: None,
            peak_rss_bytes: peak,
            user_cpu_seconds: user,
            system_cpu_seconds: system,
        }
    }
}

#[cfg(target_os = "macos")]
fn current_rss() -> Option<u64> {
    process_rss_bytes(std::process::id())
}

#[cfg(all(target_os = "linux", not(target_os = "macos")))]
fn current_rss() -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|content| {
            content
                .lines()
                .find(|line| line.starts_with("VmRSS:"))
                .and_then(|line| {
                    line.split_whitespace()
                        .nth(1)
                        .and_then(|value| value.parse::<u64>().ok())
                        .map(|kb| kb * 1024)
                })
        })
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn current_rss() -> Option<u64> {
    None
}

fn lifetime_rusage() -> (Option<u64>, Option<f64>, Option<f64>) {
    #[cfg(target_os = "macos")]
    unsafe {
        let mut usage = std::mem::zeroed::<libc::rusage>();
        if libc::getrusage(libc::RUSAGE_SELF, &mut usage) == 0 {
            let peak = Some(usage.ru_maxrss as u64);
            let user =
                Some(usage.ru_utime.tv_sec as f64 + usage.ru_utime.tv_usec as f64 / 1_000_000.0);
            let system =
                Some(usage.ru_stime.tv_sec as f64 + usage.ru_stime.tv_usec as f64 / 1_000_000.0);
            return (peak, user, system);
        }
    }
    #[cfg(all(unix, not(target_os = "macos")))]
    unsafe {
        #[repr(C)]
        struct Rusage {
            ru_utime: [i64; 2],
            ru_stime: [i64; 2],
            ru_maxrss: i64,
            _pad: [i64; 13],
        }
        unsafe extern "C" {
            fn getrusage(who: i32, usage: *mut Rusage) -> i32;
        }
        let mut usage = std::mem::zeroed::<Rusage>();
        if getrusage(0, &mut usage) == 0 {
            let peak = Some((usage.ru_maxrss as u64) * 1024);
            let user = Some(usage.ru_utime[0] as f64 + usage.ru_utime[1] as f64 / 1_000_000.0);
            let system = Some(usage.ru_stime[0] as f64 + usage.ru_stime[1] as f64 / 1_000_000.0);
            return (peak, user, system);
        }
    }
    (None, None, None)
}

/// Database footprint measurement.
///
/// Only the redb files count: they are the database the backend reads and
/// writes. Logs and attachments live beside them and are not database state.
pub fn database_bytes(
    directory: &ocg::system::data_directory::DataDirectory,
) -> std::io::Result<u64> {
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

/// The resident memory of another process, by its identifier.
///
/// The backend runs in its own process, so its footprint is measured there
/// rather than in the harness that drives it.
pub fn process_rss_bytes(pid: u32) -> Option<u64> {
    #[cfg(target_os = "macos")]
    {
        let output = std::process::Command::new("ps")
            .args(["-o", "rss=", "-p", &pid.to_string()])
            .output()
            .ok()?;
        let text = String::from_utf8(output.stdout).ok()?;
        text.split_whitespace()
            .next()
            .and_then(|value| value.parse::<u64>().ok())
            .map(|kilobytes| kilobytes * 1024)
    }
    #[cfg(all(target_os = "linux", not(target_os = "macos")))]
    {
        let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
        status
            .lines()
            .find(|line| line.starts_with("VmRSS:"))
            .and_then(|line| {
                line.split_whitespace()
                    .nth(1)
                    .and_then(|value| value.parse::<u64>().ok())
                    .map(|kilobytes| kilobytes * 1024)
            })
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = pid;
        None
    }
}

/// The peak resident memory of another process, by its identifier.
pub fn process_peak_rss_bytes(pid: u32) -> Option<u64> {
    #[cfg(target_os = "macos")]
    {
        let output = std::process::Command::new("ps")
            .args(["-o", "rss=", "-p", &pid.to_string()])
            .output()
            .ok()?;
        let text = String::from_utf8(output.stdout).ok()?;
        text.split_whitespace()
            .next()
            .and_then(|value| value.parse::<u64>().ok())
            .map(|kilobytes| kilobytes * 1024)
    }
    #[cfg(all(target_os = "linux", not(target_os = "macos")))]
    {
        let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
        status
            .lines()
            .find(|line| line.starts_with("VmHWM:"))
            .and_then(|line| {
                line.split_whitespace()
                    .nth(1)
                    .and_then(|value| value.parse::<u64>().ok())
                    .map(|kilobytes| kilobytes * 1024)
            })
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = pid;
        None
    }
}
