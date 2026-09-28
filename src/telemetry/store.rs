//! The local, inspectable telemetry store.
//!
//! Events are appended as one JSON object per line to
//! `<project>/.ocg/telemetry/events.jsonl`. The location is inside the
//! project (never a platform remote cache), the file is plain JSONL a human can
//! read, and every append uses `write_all` on an `O_APPEND` handle. Readers
//! defensively skip malformed lines, including a partial line left by a crash or
//! an unusual concurrent-write failure.
//!
//! Corruption is never fatal:
//!
//! - reading skips and counts a bad line instead of failing;
//! - a bad line never stops a later append or a normal `ocg` command;
//! - a missing file or directory is an empty log, not an error.
//!
//! Reading never creates state, so `ocg stats` and `ocg doctor` can inspect the
//! store without turning a missing telemetry directory into an artifact.

use crate::error::{OcgError, Result};
use crate::telemetry::task::{is_secret_like, Event, EVENT_SCHEMA_VERSION};
use crate::telemetry::TelemetryConfig;
use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

/// The telemetry directory under `.ocg/`.
pub const TELEMETRY_DIR: &str = "telemetry";
/// The JSONL file name.
pub const TELEMETRY_FILE: &str = "events.jsonl";

/// The parsed log plus what could not be parsed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EventLog {
    pub events: Vec<Event>,
    /// Lines that were not valid JSON or not a valid event.
    pub corrupt_lines: usize,
    /// Valid JSON events written by a newer, unsupported schema.
    pub unsupported_lines: usize,
}

impl EventLog {
    pub fn is_empty(&self) -> bool {
        self.events.is_empty() && self.corrupt_lines == 0 && self.unsupported_lines == 0
    }
}

/// A handle to one project's telemetry directory.
#[derive(Debug, Clone)]
pub struct TelemetryStore {
    root: PathBuf,
    dir: PathBuf,
    file: PathBuf,
    config: TelemetryConfig,
}

impl TelemetryStore {
    pub fn new(root: impl Into<PathBuf>, config: TelemetryConfig) -> Self {
        let root = root.into();
        let dir = root
            .join(crate::context::repomap::OCG_DIR)
            .join(TELEMETRY_DIR);
        let file = dir.join(TELEMETRY_FILE);
        Self {
            root,
            dir,
            file,
            config,
        }
    }

    pub fn config(&self) -> &TelemetryConfig {
        &self.config
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn path(&self) -> &Path {
        &self.file
    }

    /// Whether collection is enabled for this store.
    pub fn enabled(&self) -> bool {
        self.config.enabled
    }

    /// Whether the file already exists. Used by read-only inspectors so a
    /// missing store can be reported as "no data" instead of created.
    pub fn exists(&self) -> bool {
        self.file.is_file()
    }

    /// Append one event. Returns `Ok(false)` when collection is disabled: a
    /// disabled store writes nothing and creates no directory.
    ///
    /// The event is sanitized here as well as by the caller, and the serialized
    /// line is rejected if it still looks secret-shaped.
    pub fn append(&self, event: &Event) -> Result<bool> {
        if !self.config.enabled {
            return Ok(false);
        }
        if !self.config.local_only {
            // Defensive: `TelemetryConfig::from_config` already rejects this.
            return Err(OcgError::config(
                "telemetry.localOnly=false is not supported; there is no remote telemetry mode",
            ));
        }
        let event = event.clone().sanitize();
        let mut line = serde_json::to_string(&event).map_err(|error| {
            OcgError::config(format!("cannot serialize the telemetry event: {error}"))
        })?;
        line.push('\n');
        if is_secret_like(&line) {
            return Err(OcgError::config(
                "telemetry event was rejected: metadata still looks secret-shaped after redaction",
            ));
        }
        crate::runtime::install::ensure_gitignore(&self.root)?;
        fs::create_dir_all(&self.dir).map_err(|error| {
            OcgError::io(format!("cannot create {}", self.dir.display()), error)
        })?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.file)
            .map_err(|error| OcgError::io(format!("cannot open {}", self.file.display()), error))?;
        file.write_all(line.as_bytes())
            .map_err(|error| OcgError::write(&self.file, error))?;
        Ok(true)
    }

    /// Read every parseable event, counting the rest. Never creates state and
    /// never fails on a corrupt file: an unreadable file is an empty log.
    pub fn read(&self) -> EventLog {
        let mut log = EventLog::default();
        let Ok(bytes) = fs::read(&self.file) else {
            return log;
        };
        let text = String::from_utf8_lossy(&bytes);
        for line in text.lines() {
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<Event>(line) {
                Ok(event) if event.schema_version == EVENT_SCHEMA_VERSION => {
                    log.events.push(event.sanitize());
                }
                Ok(_) => log.unsupported_lines += 1,
                Err(_) => log.corrupt_lines += 1,
            }
        }
        log
    }

    /// File size on disk, or zero when the store does not exist.
    pub fn bytes(&self) -> u64 {
        fs::metadata(&self.file)
            .map(|metadata| metadata.len())
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::task::{Outcome, REDACTED};

    fn config(enabled: bool) -> TelemetryConfig {
        TelemetryConfig {
            enabled,
            local_only: true,
        }
    }

    #[test]
    fn disabled_store_writes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let store = TelemetryStore::new(dir.path(), config(false));
        let wrote = store.append(&Event::new("task-1", 1)).unwrap();
        assert!(!wrote);
        assert!(!dir.path().join(".ocg").exists());
        assert!(!store.exists());
    }

    #[test]
    fn enabled_store_round_trips_events() {
        let dir = tempfile::tempdir().unwrap();
        let store = TelemetryStore::new(dir.path(), config(true));
        let mut event = Event::new("task-1", 7);
        event.outcome = Outcome::Success;
        assert!(store.append(&event).unwrap());
        store.append(&Event::new("task-2", 8)).unwrap();
        let log = store.read();
        assert_eq!(log.events.len(), 2);
        assert_eq!(log.corrupt_lines, 0);
        assert_eq!(log.events[0].task_id, "task-1");
        // Creating telemetry ignores the state directory once.
        assert_eq!(
            fs::read_to_string(dir.path().join(".gitignore")).unwrap(),
            ".ocg/\n"
        );
    }

    #[test]
    fn corrupt_lines_are_skipped_counted_and_do_not_block_writes() {
        let dir = tempfile::tempdir().unwrap();
        let store = TelemetryStore::new(dir.path(), config(true));
        store.append(&Event::new("task-good", 1)).unwrap();
        // Inject corrupt and truncated lines by hand.
        let mut file = OpenOptions::new().append(true).open(store.path()).unwrap();
        file.write_all(b"not json at all\n{\"schema_version\":1,\"task_id\":\"cut\"}\n")
            .unwrap();
        drop(file);
        store.append(&Event::new("task-after", 2)).unwrap();

        let log = store.read();
        assert_eq!(log.events.len(), 2);
        assert_eq!(log.corrupt_lines, 2);
        assert_eq!(log.events[1].task_id, "task-after");
    }

    #[test]
    fn a_truncated_trailing_line_is_counted_when_read() {
        let dir = tempfile::tempdir().unwrap();
        let store = TelemetryStore::new(dir.path(), config(true));
        store.append(&Event::new("task-good", 1)).unwrap();
        let mut file = OpenOptions::new().append(true).open(store.path()).unwrap();
        // A crash mid-write leaves a partial final line behind.
        file.write_all(b"{\"schema_version\":1,\"task_id\":\"cut")
            .unwrap();
        drop(file);
        let log = store.read();
        assert_eq!(log.events.len(), 1);
        assert_eq!(log.corrupt_lines, 1);
    }

    #[test]
    fn secret_metadata_is_redacted_before_it_reaches_disk() {
        let dir = tempfile::tempdir().unwrap();
        let store = TelemetryStore::new(dir.path(), config(true));
        let secret = format!("{}{}", concat!("sk", "-"), "B".repeat(40));
        let mut event = Event::new("task-1", 1);
        event.model = Some(secret.clone());
        store.append(&event).unwrap();
        let raw = fs::read_to_string(store.path()).unwrap();
        assert!(!raw.contains(&secret), "{raw}");
        assert!(raw.contains(REDACTED), "{raw}");
    }

    #[test]
    fn unsupported_schema_lines_are_counted_not_parsed() {
        let dir = tempfile::tempdir().unwrap();
        let store = TelemetryStore::new(dir.path(), config(true));
        fs::create_dir_all(store.dir()).unwrap();
        fs::write(
            store.path(),
            "{\"schema_version\":999,\"timestamp\":1,\"task_id\":\"task-x\",\"outcome\":\"success\"}\n",
        )
        .unwrap();
        let log = store.read();
        assert!(log.events.is_empty());
        assert_eq!(log.unsupported_lines, 1);
        assert_eq!(log.corrupt_lines, 0);
    }
}
