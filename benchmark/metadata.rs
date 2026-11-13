use serde_json::{Value, json};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug)]
pub struct BenchmarkMetadata {
    pub commit_hash: Option<String>,
    pub dirty_tree: bool,
    pub tree_fingerprint: Option<String>,
    pub compiler_version: String,
    pub build_profile: String,
    pub os: String,
    pub arch: String,
    pub cpu_model: Option<String>,
    pub available_parallelism: usize,
    pub storage_path: PathBuf,
    pub filesystem_info: Option<String>,
    pub total_memory_bytes: Option<u64>,
}

impl BenchmarkMetadata {
    pub fn capture(
        storage_path: PathBuf,
        build_profile: &str,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let commit_hash = git_output(&["rev-parse", "HEAD"]);
        let status = std::process::Command::new("git")
            .args(["status", "--porcelain", "--untracked-files=all"])
            .output()
            .ok()
            .filter(|output| output.status.success());
        let dirty_tree = status
            .as_ref()
            .map(|output| !output.stdout.is_empty())
            .unwrap_or(false);
        let tree_fingerprint = status
            .as_ref()
            .and_then(|status| worktree_fingerprint(&status.stdout));

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
            tree_fingerprint,
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
            "tree_fingerprint": self.tree_fingerprint,
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

fn worktree_fingerprint(status: &[u8]) -> Option<String> {
    use std::io::Write;
    use std::process::Stdio;
    let diff = std::process::Command::new("git")
        .args(["diff", "--binary", "HEAD", "--"])
        .output()
        .ok()
        .filter(|output| output.status.success())?;
    let mut child = std::process::Command::new("git")
        .args(["hash-object", "--stdin"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .ok()?;
    {
        let mut input = child.stdin.take()?;
        input.write_all(status).ok()?;
        input.write_all(&diff.stdout).ok()?;
    }
    let output = child.wait_with_output().ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

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

fn filesystem_of(path: &Path) -> Option<String> {
    #[cfg(target_os = "macos")]
    {
        let output = std::process::Command::new("df")
            .args(["-P", &path.display().to_string()])
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        let text = String::from_utf8(output.stdout).ok()?;
        let last = text.lines().last()?;
        let fields = last.split_whitespace().collect::<Vec<_>>();
        let device = fields.first()?;
        let mountpoint = fields.last()?;
        let mount = std::process::Command::new("mount")
            .output()
            .ok()
            .filter(|output| output.status.success())
            .and_then(|output| String::from_utf8(output.stdout).ok())
            .and_then(|output| {
                output
                    .lines()
                    .find(|line| line.starts_with(&format!("{device} on {mountpoint} (")))
                    .map(str::to_owned)
            });
        Some(mount.unwrap_or_else(|| format!("device {device} mounted at {mountpoint}")))
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

#[derive(Clone, Debug)]
pub struct WorkloadParameters {
    pub case_name: String,
    pub trial: Option<usize>,
    pub concurrency: usize,
    pub operation_count: usize,
    pub fixture_size: Option<usize>,
    pub cache_state: Option<String>,
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
