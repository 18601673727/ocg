//! The local, inspectable telemetry store.
//!
//! Each event is one Project document in the OCG database, keyed by a
//! time-ordered identifier, so the log reads back in the order it was written.
//! At most [`MAX_EVENTS`] events are kept per Project; older ones are pruned
//! as new ones arrive. Readers defensively skip malformed entries.
//!
//! Corruption is never fatal:
//!
//! - reading skips and counts a bad entry instead of failing;
//! - a bad entry never stops a later append or a normal `ocg` command;
//! - a Project with no events is an empty log, not an error.
//!
//! Reading never creates state, so `ocg stats` and `ocg doctor` can inspect the
//! store without registering anything.

use crate::error::{OcgError, Result};
use crate::project_store::ProjectDocuments;
use crate::telemetry::task::{is_secret_like, Event, EVENT_SCHEMA_VERSION};
use crate::telemetry::TelemetryConfig;
use std::path::{Path, PathBuf};

/// The document kind events are stored under.
const EVENT_KIND: &str = "telemetry-event";
/// The most events one Project keeps.
pub const MAX_EVENTS: usize = 10_000;

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

/// A handle to one project's telemetry.
#[derive(Debug, Clone)]
pub struct TelemetryStore {
    root: PathBuf,
    location: PathBuf,
    config: TelemetryConfig,
}

impl TelemetryStore {
    pub fn new(root: impl Into<PathBuf>, config: TelemetryConfig) -> Self {
        Self {
            root: root.into(),
            location: crate::database::path().unwrap_or_default(),
            config,
        }
    }

    pub fn config(&self) -> &TelemetryConfig {
        &self.config
    }

    /// The database the events are stored in, for reports.
    pub fn path(&self) -> &Path {
        &self.location
    }

    /// Whether collection is enabled for this store.
    pub fn enabled(&self) -> bool {
        self.config.enabled
    }

    /// Whether any event is stored. Used by read-only inspectors so a missing
    /// store can be reported as "no data".
    pub fn exists(&self) -> bool {
        self.bytes() > 0
    }

    /// Append one event. Returns `Ok(false)` when collection is disabled: a
    /// disabled store writes nothing.
    ///
    /// The event is sanitized here as well as by the caller, and the
    /// serialized event is rejected if it still looks secret-shaped.
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
        let line = serde_json::to_string(&event).map_err(|error| {
            OcgError::config(format!("cannot serialize the telemetry event: {error}"))
        })?;
        if is_secret_like(&line) {
            return Err(OcgError::config(
                "telemetry event was rejected: metadata still looks secret-shaped after redaction",
            ));
        }
        let documents = ProjectDocuments::for_root(&self.root)?;
        documents.put(
            EVENT_KIND,
            &uuid::Uuid::now_v7().to_string(),
            line.as_bytes(),
        )?;
        documents.retain_newest(EVENT_KIND, MAX_EVENTS)?;
        Ok(true)
    }

    /// Read every parseable event, counting the rest. Never creates state and
    /// never fails: an unreadable store is an empty log.
    pub fn read(&self) -> EventLog {
        let mut log = EventLog::default();
        let Ok(Some(documents)) = ProjectDocuments::existing(&self.root) else {
            return log;
        };
        for entry in documents.list(EVENT_KIND).unwrap_or_default() {
            match serde_json::from_slice::<Event>(&entry.value) {
                Ok(event) if event.schema_version == EVENT_SCHEMA_VERSION => {
                    log.events.push(event.sanitize());
                }
                Ok(_) => log.unsupported_lines += 1,
                Err(_) => log.corrupt_lines += 1,
            }
        }
        log
    }

    /// Bytes the stored events occupy, or zero when there are none.
    pub fn bytes(&self) -> u64 {
        let Ok(Some(documents)) = ProjectDocuments::existing(&self.root) else {
            return 0;
        };
        documents
            .sizes(EVENT_KIND)
            .unwrap_or_default()
            .iter()
            .map(|(_, bytes, _)| bytes)
            .sum()
    }
}
