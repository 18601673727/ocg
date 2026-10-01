//! Raw verification logs under `.ocg/logs/`.
//!
//! Raw evidence is kept locally so a distilled report can be checked against
//! the original bytes. The directory is:
//!
//! - never committed (the state directory is added to `.gitignore`);
//! - bounded by a conservative total storage cap that prunes the oldest files;
//! - not touched by `ocg cache clean`, which only removes the context cache.

use crate::error::{OcgError, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// The raw log directory under `.ocg/`.
pub const LOGS_DIR: &str = "logs";

/// A process-local monotonic sequence so two logs written in the same second
/// with identical output still get distinct names without randomness.
static NEXT_LOG_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// A stored raw log reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawLogRef {
    /// Path relative to the project root (portable in reports).
    pub path: String,
    pub bytes: u64,
    pub truncated: bool,
}

/// Aggregate log statistics.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct LogStats {
    pub files: usize,
    pub bytes: u64,
}

/// Everything needed to store one raw log.
pub struct RawLogInput<'a> {
    pub created_at: i64,
    /// A human label (the command display) recorded in the header.
    pub label: &'a str,
    pub stdout: &'a [u8],
    pub stderr: &'a [u8],
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
    /// Per-stream byte cap applied again before writing.
    pub max_bytes: usize,
}

/// A handle to one project's raw log directory.
#[derive(Debug, Clone)]
pub struct LogStore {
    root: PathBuf,
    dir: PathBuf,
}

impl LogStore {
    pub fn new(root: &Path) -> Self {
        Self {
            root: root.to_path_buf(),
            dir: root.join(crate::context::repomap::OCG_DIR).join(LOGS_DIR),
        }
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Write one bounded raw log. The filename is safe and collision-resistant
    /// for a given created-at, index, label and content: it includes the
    /// process id, a monotonic sequence and a content hash, so two runs in the
    /// same second cannot silently overwrite each other.
    pub fn store(&self, input: &RawLogInput<'_>) -> Result<RawLogRef> {
        crate::install::ensure_gitignore(&self.root)?;
        fs::create_dir_all(&self.dir).map_err(|error| {
            OcgError::io(format!("cannot create {}", self.dir.display()), error)
        })?;

        let max = input.max_bytes.max(1);
        let stdout = &input.stdout[..input.stdout.len().min(max)];
        let stderr = &input.stderr[..input.stderr.len().min(max)];
        let truncated = input.stdout_truncated
            || input.stderr_truncated
            || stdout.len() < input.stdout.len()
            || stderr.len() < input.stderr.len();

        let mut content = Vec::with_capacity(stdout.len() + stderr.len() + 256);
        content.extend_from_slice(
            format!(
                "# ocg verification raw log\n# command: {}\n# created_at: {}\n# stdout_bytes: {} (truncated: {})\n# stderr_bytes: {} (truncated: {})\n--- stdout ---\n",
                input.label,
                input.created_at,
                stdout.len(),
                input.stdout_truncated,
                stderr.len(),
                input.stderr_truncated,
            )
            .as_bytes(),
        );
        content.extend_from_slice(stdout);
        content.extend_from_slice(b"\n--- stderr ---\n");
        content.extend_from_slice(stderr);
        if !content.ends_with(b"\n") {
            content.push(b'\n');
        }

        let sequence = NEXT_LOG_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let digest = crate::hash::sha256_hex(&content);
        let short = digest.get(..12).unwrap_or(&digest);
        let name = format!(
            "verify-{}-{}-{sequence:06}-{short}.log",
            input.created_at,
            std::process::id(),
        );
        let path = self.dir.join(&name);
        fs::write(&path, &content).map_err(|error| OcgError::write(&path, error))?;
        Ok(RawLogRef {
            path: format!("{}/{}/{}", crate::context::repomap::OCG_DIR, LOGS_DIR, name),
            bytes: content.len() as u64,
            truncated,
        })
    }

    /// Remove the oldest logs until the directory fits the cap. The `keep`
    /// entry (normally the log just written and referenced by the current
    /// report) is never deleted, even when it alone exceeds the cap. Returns
    /// the number of bytes removed. Never touches anything outside the log dir.
    pub fn prune(&self, max_total_bytes: u64, keep: Option<&str>) -> Result<u64> {
        let mut files: Vec<(String, u64)> = Vec::new();
        let Ok(entries) = fs::read_dir(&self.dir) else {
            return Ok(0);
        };
        let mut total = 0u64;
        for entry in entries.flatten() {
            let metadata = match entry.metadata() {
                Ok(metadata) if metadata.is_file() => metadata,
                _ => continue,
            };
            let name = entry.file_name().to_string_lossy().into_owned();
            total += metadata.len();
            files.push((name, metadata.len()));
        }
        // Lexical order is chronological for our `verify-<unix>-<pid>-...`
        // names; the sequence keeps same-second entries ordered too.
        files.sort();
        let mut removed = 0u64;
        for (name, bytes) in files {
            if total <= max_total_bytes {
                break;
            }
            if keep == Some(name.as_str()) {
                continue;
            }
            let path = self.dir.join(&name);
            if fs::remove_file(&path).is_ok() {
                total = total.saturating_sub(bytes);
                removed += bytes;
            }
        }
        Ok(removed)
    }

    pub fn stats(&self) -> LogStats {
        let mut stats = LogStats::default();
        let Ok(entries) = fs::read_dir(&self.dir) else {
            return stats;
        };
        for entry in entries.flatten() {
            if let Ok(metadata) = entry.metadata() {
                if metadata.is_file() {
                    stats.files += 1;
                    stats.bytes = stats.bytes.saturating_add(metadata.len());
                }
            }
        }
        stats
    }
}
