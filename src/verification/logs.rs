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
        crate::runtime::install::ensure_gitignore(&self.root)?;
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
        let digest = crate::runtime::hash::sha256_hex(&content);
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

#[cfg(test)]
mod tests {
    use super::*;

    fn input<'a>(
        label: &'a str,
        stdout: &'a [u8],
        stderr: &'a [u8],
        created_at: i64,
    ) -> RawLogInput<'a> {
        RawLogInput {
            created_at,
            label,
            stdout,
            stderr,
            stdout_truncated: false,
            stderr_truncated: false,
            max_bytes: 1024,
        }
    }

    #[test]
    fn stores_inspectable_raw_logs_with_a_safe_name() {
        let dir = tempfile::tempdir().unwrap();
        let store = LogStore::new(dir.path());
        let reference = store
            .store(&input("cargo test", b"hello", b"oops", 10))
            .unwrap();
        assert!(reference.path.starts_with(".ocg/logs/"));
        assert!(!reference.truncated);
        let text = fs::read_to_string(dir.path().join(&reference.path)).unwrap();
        assert!(text.contains("cargo test"));
        assert!(text.contains("hello"));
        assert!(text.contains("oops"));
        // The state directory is ignored exactly once.
        let gitignore = fs::read_to_string(dir.path().join(".gitignore")).unwrap();
        assert_eq!(gitignore, ".ocg/\n");
    }

    #[test]
    fn same_second_same_command_logs_do_not_collide() {
        let dir = tempfile::tempdir().unwrap();
        let store = LogStore::new(dir.path());
        let mut names = Vec::new();
        for _ in 0..3 {
            let reference = store
                .store(&input("same command", b"same", b"", 7))
                .unwrap();
            names.push(reference.path);
        }
        names.sort();
        names.dedup();
        assert_eq!(names.len(), 3, "logs collided: {names:?}");
        assert_eq!(store.stats().files, 3);
    }

    #[test]
    fn caps_streams_and_marks_truncation() {
        let dir = tempfile::tempdir().unwrap();
        let store = LogStore::new(dir.path());
        let stdout = vec![b'a'; 4096];
        let mut raw = input("tool", &stdout, b"", 1);
        raw.max_bytes = 16;
        let reference = store.store(&raw).unwrap();
        assert!(reference.truncated);
        assert!(reference.bytes < 4096);
    }

    #[test]
    fn prune_removes_oldest_files_first() {
        let dir = tempfile::tempdir().unwrap();
        let store = LogStore::new(dir.path());
        let payload = vec![b'x'; 100];
        for created_at in 0..5 {
            store
                .store(&input("tool", &payload, b"", created_at))
                .unwrap();
        }
        let before = store.stats();
        assert_eq!(before.files, 5);
        let removed = store.prune(250, None).unwrap();
        assert!(removed > 0);
        let after = store.stats();
        assert!(after.bytes <= 250);
        assert!(after.files < before.files);
        // The newest file survives.
        let logs = dir.path().join(".ocg/logs");
        let names: Vec<String> = fs::read_dir(logs)
            .unwrap()
            .flatten()
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .collect();
        assert!(names.iter().all(|name| !name.starts_with("verify-0-")));
    }

    #[test]
    fn prune_keeps_the_referenced_log_even_over_the_cap() {
        let dir = tempfile::tempdir().unwrap();
        let store = LogStore::new(dir.path());
        let payload = vec![b'y'; 200];
        let kept = store.store(&input("keeper", &payload, b"", 100)).unwrap();
        for created_at in 0..4 {
            store
                .store(&input("older", &payload, b"", created_at))
                .unwrap();
        }
        let keep_name = kept.path.rsplit('/').next().unwrap().to_string();
        // A cap far below even the one kept file must not delete it.
        store.prune(1, Some(&keep_name)).unwrap();
        assert!(
            dir.path().join(&kept.path).is_file(),
            "the referenced log was pruned"
        );
        let remaining = store.stats();
        assert_eq!(remaining.files, 1);
    }
}
