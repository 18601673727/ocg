//! Raw verification logs, stored as Project documents in the OCG database.
//!
//! Raw evidence is kept so a distilled report can be checked against the
//! original bytes. The logs are:
//!
//! - never written into the Project;
//! - bounded by a conservative total storage cap that prunes the oldest logs;
//! - not touched by `ocg cache clean`, which only removes the context cache.

use crate::error::Result;
use crate::project_store::ProjectDocuments;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// A process-local monotonic sequence so two logs written in the same second
/// with identical output still get distinct names without randomness.
static NEXT_LOG_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// A stored raw log reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawLogRef {
    /// The log's reference, `verification-log/<name>`.
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

/// The document kind raw logs are stored under; a log's reference is
/// `<kind>/<name>`.
pub const LOG_KIND: &str = "verification-log";

/// A handle to one project's raw logs.
#[derive(Debug, Clone)]
pub struct LogStore {
    root: PathBuf,
}

impl LogStore {
    pub fn new(root: &Path) -> Self {
        Self {
            root: root.to_path_buf(),
        }
    }

    /// Write one bounded raw log. The name is safe and collision-resistant
    /// for a given created-at, index, label and content: it includes the
    /// process id, a monotonic sequence and a content hash, so two runs in the
    /// same second cannot silently overwrite each other.
    pub fn store(&self, input: &RawLogInput<'_>) -> Result<RawLogRef> {
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
        ProjectDocuments::for_root(&self.root)?.put(LOG_KIND, &name, &content)?;
        Ok(RawLogRef {
            path: format!("{LOG_KIND}/{name}"),
            bytes: content.len() as u64,
            truncated,
        })
    }

    /// Remove the oldest logs until the stored logs fit the cap. The `keep`
    /// entry (normally the log just written and referenced by the current
    /// report) is never deleted, even when it alone exceeds the cap. Returns
    /// the number of bytes removed.
    pub fn prune(&self, max_total_bytes: u64, keep: Option<&str>) -> Result<u64> {
        let Some(documents) = ProjectDocuments::existing(&self.root)? else {
            return Ok(0);
        };
        // Lexical order is chronological for `verify-<unix>-<pid>-...` names;
        // the sequence keeps same-second entries ordered too.
        let logs = documents.sizes(LOG_KIND)?;
        let mut total: u64 = logs.iter().map(|(_, bytes, _)| bytes).sum();
        let mut removed = 0u64;
        for (name, bytes, _) in logs {
            if total <= max_total_bytes {
                break;
            }
            if keep == Some(name.as_str()) {
                continue;
            }
            if documents.remove(LOG_KIND, &name)? {
                total = total.saturating_sub(bytes);
                removed += bytes;
            }
        }
        Ok(removed)
    }

    /// The size of one stored log, by its reference.
    pub fn size(&self, reference: &str) -> Option<u64> {
        let name = reference.strip_prefix(LOG_KIND)?.strip_prefix('/')?;
        let documents = ProjectDocuments::existing(&self.root).ok()??;
        documents
            .sizes(LOG_KIND)
            .ok()?
            .into_iter()
            .find(|(key, _, _)| key == name)
            .map(|(_, bytes, _)| bytes)
    }

    pub fn stats(&self) -> LogStats {
        let mut stats = LogStats::default();
        let Ok(Some(documents)) = ProjectDocuments::existing(&self.root) else {
            return stats;
        };
        for (_, bytes, _) in documents.sizes(LOG_KIND).unwrap_or_default() {
            stats.files += 1;
            stats.bytes = stats.bytes.saturating_add(bytes);
        }
        stats
    }
}
