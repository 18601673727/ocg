//! The metadata a measurement needs to be reproducible.
//!
//! A latency without the machine, the build and the workload behind it is
//! an anecdote. Every measurement carries the state that produced it, so a
//! reader can tell whether two runs are comparable and can repeat the one
//! they read.

use serde_json::{Value, json};
use std::path::{Path, PathBuf};

/// The environment a measurement was taken in.
#[derive(Clone, Debug)]
pub struct BenchmarkMetadata {
    /// The commit the tree was at, when it was a git tree.
    pub commit_hash: Option<String>,
    /// Whether the tree carried uncommitted changes.
    pub dirty_tree: bool,
    /// The compiler that built the binary.
    pub compiler_version: String,
    /// The Cargo profile the binary was built with.
    pub build_profile: String,
    /// The operating system the binary ran on.
    pub os: String,
    /// The architecture the binary ran on.
    pub arch: String,
    /// The model of the processor, when it could be read.
    pub cpu_model: Option<String>,
    /// How many processors the machine offers.
    pub available_parallelism: usize,
    /// Where the run kept its data.
    pub storage_path: PathBuf,
    /// The filesystem the data directory is on, when it could be read.
    pub filesystem_info: Option<String>,
    /// The memory the machine has, in bytes, when it could be read.
    pub total_memory_bytes: Option<u64>,
}

impl BenchmarkMetadata {
    pub fn capture(
        storage_path: PathBuf,
        build_profile: &str,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let commit_hash = git_output(&["rev-parse", "HEAD"]);
        let dirty_tree = std::process::Command::new("git")
            .args(["diff-index", "--quiet", "HEAD", "--"])
            .status()
            .map(|status| !status.success())
            .unwrap_or(false);

        let compiler_version = std::process::Command::new("rustc")
            .arg("--version")
            .output()
            .ok()
            .and_then(|output| String::from_utf8(output.stdout).ok())
            .map(|text| text.trim().to_owned())
            .unwrap_or_else(|| "unknown".to_owned());

        Ok(Self {
            commit_hash,
            dirty_tree,
            compiler_version,
            build_profile: build_profile.to_owned(),
            os: std::env::consts::OS.to_owned(),
            arch: std::env::consts::ARCH.to_owned(),
            cpu_model: cpu_model_name(),
            available_parallelism: std::thread::available_parallelism()?.get(),
            filesystem_info: filesystem_of(&storage_path),
            storage_path,
            total_memory_bytes: total_memory_bytes(),
        })
    }

    pub fn to_json(&self) -> Value {
        json!({
            "commit": self.commit_hash,
            "dirty_tree": self.dirty_tree,
            "compiler": self.compiler_version,
            "build_profile": self.build_profile,
            "os": self.os,
            "arch": self.arch,
            "cpu_model": self.cpu_model,
            "available_parallelism": self.available_parallelism,
            "total_memory_bytes": self.total_memory_bytes,
            "storage_path": self.storage_path.display().to_string(),
            "filesystem": self.filesystem_info,
        })
    }
}

/// The output of a git command, when it succeeded.
fn git_output(arguments: &[&str]) -> Option<String> {
    let output = std::process::Command::new("git")
        .args(arguments)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8(output.stdout).ok()?.trim().to_owned())
}

#[cfg(target_os = "macos")]
fn cpu_model_name() -> Option<String> {
    std::process::Command::new("sysctl")
        .args(["-n", "machdep.cpu.brand_string"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|text| text.trim().to_owned())
}

#[cfg(target_os = "linux")]
fn cpu_model_name() -> Option<String> {
    std::fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|content| {
            content
                .lines()
                .find(|line| line.starts_with("model name"))
                .and_then(|line| line.split(':').nth(1))
                .map(|value| value.trim().to_owned())
        })
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn cpu_model_name() -> Option<String> {
    None
}

/// The filesystem the path is on, as the operating system names it.
fn filesystem_of(path: &Path) -> Option<String> {
    #[cfg(target_os = "macos")]
    {
        let output = std::process::Command::new("df")
            .args(["-H", &path.display().to_string()])
            .output()
            .ok()?;
        let text = String::from_utf8(output.stdout).ok()?;
        let last = text.lines().last()?;
        let mut fields = last.split_whitespace();
        let _device = fields.next()?;
        Some(fields.next()?.to_owned())
    }
    #[cfg(all(target_os = "linux", not(target_os = "macos")))]
    {
        let output = std::process::Command::new("stat")
            .args(["-f", "-c", "%T", &path.display().to_string()])
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        Some(String::from_utf8(output.stdout).ok()?.trim().to_owned())
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = path;
        None
    }
}

/// The memory the machine has, in bytes.
fn total_memory_bytes() -> Option<u64> {
    #[cfg(target_os = "macos")]
    {
        let output = std::process::Command::new("sysctl")
            .args(["-n", "hw.memsize"])
            .output()
            .ok()?;
        String::from_utf8(output.stdout).ok()?.trim().parse().ok()
    }
    #[cfg(all(target_os = "linux", not(target_os = "macos")))]
    {
        let content = std::fs::read_to_string("/proc/meminfo").ok()?;
        let line = content.lines().find(|line| line.starts_with("MemTotal:"))?;
        let kilobytes = line.split_whitespace().nth(1)?.parse::<u64>().ok()?;
        Some(kilobytes * 1024)
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        None
    }
}

/// The workload parameters of one measurement case.
#[derive(Clone, Debug)]
pub struct WorkloadParameters {
    /// The name of the case.
    pub case_name: String,
    /// Which trial of the case this is.
    pub trial: Option<usize>,
    /// How many clients the case ran.
    pub concurrency: usize,
    /// How many operations the case performed.
    pub operation_count: usize,
    /// How large the fixture was, when the case used one.
    pub fixture_size: Option<usize>,
    /// What state the caches were in.
    pub cache_state: Option<String>,
    /// The parameters of the case beyond the ones every case has.
    pub custom: Value,
}

impl WorkloadParameters {
    pub fn new(case_name: &str, concurrency: usize, operation_count: usize) -> Self {
        Self {
            case_name: case_name.to_owned(),
            trial: None,
            concurrency,
            operation_count,
            fixture_size: None,
            cache_state: None,
            custom: Value::Object(serde_json::Map::new()),
        }
    }

    pub fn with_trial(mut self, trial: usize) -> Self {
        self.trial = Some(trial);
        self
    }

    pub fn with_fixture_size(mut self, size: usize) -> Self {
        self.fixture_size = Some(size);
        self
    }

    pub fn with_cache_state(mut self, state: &str) -> Self {
        self.cache_state = Some(state.to_owned());
        self
    }

    pub fn with(mut self, name: &str, value: Value) -> Self {
        if let Some(map) = self.custom.as_object_mut() {
            map.insert(name.to_owned(), value);
        }
        self
    }

    pub fn to_json(&self) -> Value {
        let mut obj = self.custom.clone();
        if let Some(map) = obj.as_object_mut() {
            map.insert("case".to_string(), json!(self.case_name));
            if let Some(trial) = self.trial {
                map.insert("trial".to_string(), json!(trial));
            }
            map.insert("concurrency".to_string(), json!(self.concurrency));
            map.insert("operation_count".to_string(), json!(self.operation_count));
            if let Some(size) = self.fixture_size {
                map.insert("fixture_size".to_string(), json!(size));
            }
            if let Some(state) = &self.cache_state {
                map.insert("cache_state".to_string(), json!(state));
            }
        }
        obj
    }
}
