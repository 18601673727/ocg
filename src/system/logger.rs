use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::{File, Metadata, OpenOptions};
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::db::{DatabaseHandle, DatabaseStatus};

pub const LOG_QUEUE_CAPACITY: usize = 256;
pub const MAX_LOG_MESSAGE_BYTES: usize = 4000;
pub const LOG_BUFFER_BYTES: usize = 64 * 1024;

pub(crate) type LogFiles = Mutex<HashMap<PathBuf, Metadata>>;

pub(crate) fn runtime_file_name(now: time::OffsetDateTime, sequence: u64) -> String {
    format!(
        "{:04}{:02}{:02}{:02}{:02}{:02}Z_runtime_{sequence:03}.log",
        now.year(),
        u8::from(now.month()),
        now.day(),
        now.hour(),
        now.minute(),
        now.second()
    )
}

pub(crate) fn is_runtime_file_name(name: &OsStr) -> bool {
    let Some(name) = name.to_str().filter(|name| name.is_ascii()) else {
        return false;
    };
    let Some(stamp) = name
        .get(..14)
        .filter(|stamp| stamp.bytes().all(|byte| byte.is_ascii_digit()))
    else {
        return false;
    };
    let Some(sequence) = name
        .get(14..)
        .and_then(|tail| tail.strip_prefix("Z_runtime_"))
        .and_then(|tail| tail.strip_suffix(".log"))
    else {
        return false;
    };
    let Ok(sequence_number) = sequence.parse::<u64>() else {
        return false;
    };
    if sequence_number == 0 || format!("{sequence_number:03}") != sequence {
        return false;
    }
    let number = |range| stamp.get(range).and_then(|v: &str| v.parse::<u16>().ok());
    let (Some(year), Some(month), Some(day), Some(hour), Some(minute), Some(second)) = (
        number(0..4),
        number(4..6),
        number(6..8),
        number(8..10),
        number(10..12),
        number(12..14),
    ) else {
        return false;
    };
    let Ok(month) = time::Month::try_from(month as u8) else {
        return false;
    };
    time::Date::from_calendar_date(year as i32, month, day as u8).is_ok()
        && time::Time::from_hms(hour as u8, minute as u8, second as u8).is_ok()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LoggerLevel {
    Off,
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl LoggerLevel {
    pub fn filter(self) -> log::LevelFilter {
        match self {
            Self::Off => log::LevelFilter::Off,
            Self::Error => log::LevelFilter::Error,
            Self::Warn => log::LevelFilter::Warn,
            Self::Info => log::LevelFilter::Info,
            Self::Debug => log::LevelFilter::Debug,
            Self::Trace => log::LevelFilter::Trace,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoggerConfig {
    pub level: LoggerLevel,
    pub retention_days: u32,
    pub max_file_size_bytes: u64,
    pub flush_interval_ms: u64,
}

impl Default for LoggerConfig {
    fn default() -> Self {
        Self {
            level: LoggerLevel::Info,
            retention_days: 7,
            max_file_size_bytes: 10 * 1024 * 1024,
            flush_interval_ms: 1000,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum LoggerConfigError {
    #[error("retention_days must be greater than zero")]
    RetentionZero,
    #[error("max_file_size_bytes must be greater than zero")]
    FileSizeZero,
    #[error("flush_interval_ms must be greater than zero")]
    FlushIntervalZero,
    #[error("flush_interval_ms cannot be represented by this platform's monotonic clock")]
    FlushIntervalTooLarge,
}

impl LoggerConfig {
    pub fn validate(&self) -> Result<(), LoggerConfigError> {
        if self.retention_days == 0 {
            return Err(LoggerConfigError::RetentionZero);
        }
        if self.max_file_size_bytes == 0 {
            return Err(LoggerConfigError::FileSizeZero);
        }
        if self.flush_interval_ms == 0 {
            return Err(LoggerConfigError::FlushIntervalZero);
        }
        if Instant::now().checked_add(self.flush_interval()).is_none() {
            return Err(LoggerConfigError::FlushIntervalTooLarge);
        }
        Ok(())
    }

    fn flush_interval(self) -> Duration {
        Duration::from_millis(self.flush_interval_ms)
    }
}

#[derive(Clone, Debug, thiserror::Error)]
#[error("runtime logging {operation} failed at {}: {detail}", path.display())]
pub struct LoggerFailure {
    pub path: PathBuf,
    pub operation: &'static str,
    pub kind: io::ErrorKind,
    pub os_error: Option<i32>,
    pub detail: String,
}

#[derive(Clone, Debug)]
pub struct LoggerStatus {
    pub last_failure: Option<Arc<LoggerFailure>>,
}

#[derive(Debug, thiserror::Error)]
pub enum LoggerError {
    #[error("invalid logger configuration: {0}")]
    Configuration(#[source] LoggerConfigError),
    #[error("runtime logging requires an open database protection service")]
    DatabaseNotOpen,
    #[error("failed to start the runtime logging worker: {0}")]
    Spawn(#[source] io::Error),
    #[error("runtime logging worker panicked")]
    WorkerPanicked,
}

struct Shared {
    database: DatabaseHandle,
    config: LoggerConfig,
    accepting: AtomicBool,
    last_failure: Mutex<Option<Arc<LoggerFailure>>>,
}

#[derive(Clone)]
pub struct LoggerHandle {
    shared: Arc<Shared>,
    sender: flume::Sender<String>,
}

impl LoggerHandle {
    pub fn active_config(&self) -> LoggerConfig {
        self.shared.config
    }
}

impl log::Log for LoggerHandle {
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        let configured = self.shared.config.level.filter();
        metadata.level() <= configured
            && (metadata.target().starts_with("ocg") || !metadata.target().contains("::"))
    }

    fn log(&self, record: &log::Record<'_>) {
        if !self.enabled(record.metadata()) || !self.shared.accepting.load(Ordering::Relaxed) {
            return;
        }
        let now = time::OffsetDateTime::now_utc();
        let mut msg = format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z {:<5} [{}] ",
            now.year(),
            u8::from(now.month()),
            now.day(),
            now.hour(),
            now.minute(),
            now.second(),
            now.millisecond(),
            record.level(),
            record.target()
        );
        let body = record.args().to_string();
        for ch in body.chars() {
            if msg.len() >= MAX_LOG_MESSAGE_BYTES {
                msg.push_str("...");
                break;
            }
            match ch {
                '\n' => msg.push_str("\\n"),
                '\r' => msg.push_str("\\r"),
                _ => msg.push(ch),
            }
        }
        msg.push('\n');
        let _ = self.sender.try_send(msg);
    }

    fn flush(&self) {}
}

pub struct RuntimeLogger {
    handle: LoggerHandle,
    stop: Option<flume::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl RuntimeLogger {
    pub fn start(database: DatabaseHandle) -> Result<Self, LoggerError> {
        if database.status() != DatabaseStatus::Open {
            return Err(LoggerError::DatabaseNotOpen);
        }
        let config = database.active_system_policy().logger_config;
        config.validate().map_err(LoggerError::Configuration)?;
        let shared = Arc::new(Shared {
            database,
            config,
            accepting: AtomicBool::new(true),
            last_failure: Mutex::new(None),
        });
        let (sender, receiver) = flume::bounded(LOG_QUEUE_CAPACITY);
        let (stop_tx, stop_rx) = flume::bounded(1);
        let worker_shared = Arc::clone(&shared);
        let thread = std::thread::Builder::new()
            .name("ocg-logger".to_owned())
            .spawn(move || Worker::new(worker_shared, receiver).run(stop_rx))
            .map_err(LoggerError::Spawn)?;
        Ok(Self {
            handle: LoggerHandle { shared, sender },
            stop: Some(stop_tx),
            thread: Some(thread),
        })
    }

    pub fn handle(&self) -> LoggerHandle {
        self.handle.clone()
    }

    pub fn shutdown(mut self) -> Result<LoggerStatus, LoggerError> {
        self.handle.shared.accepting.store(false, Ordering::SeqCst);
        self.stop.take();
        if let Some(thread) = self.thread.take() {
            thread.join().map_err(|_| LoggerError::WorkerPanicked)?;
        }
        let failure = self
            .handle
            .shared
            .last_failure
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        Ok(LoggerStatus {
            last_failure: failure,
        })
    }
}

impl Drop for RuntimeLogger {
    fn drop(&mut self) {
        self.handle.shared.accepting.store(false, Ordering::SeqCst);
        self.stop.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

struct ActiveFile {
    path: PathBuf,
    file: File,
    size: u64,
    log_files: Arc<LogFiles>,
}

impl Drop for ActiveFile {
    fn drop(&mut self) {
        self.log_files
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&self.path);
    }
}

struct Worker {
    shared: Arc<Shared>,
    receiver: flume::Receiver<String>,
    file: Option<ActiveFile>,
    buffer: Vec<u8>,
    epoch: u64,
    next_sequence: u64,
}

impl Worker {
    fn new(shared: Arc<Shared>, receiver: flume::Receiver<String>) -> Self {
        let epoch = shared.database.logging_protection_state().1;
        Self {
            shared,
            receiver,
            file: None,
            buffer: Vec::with_capacity(LOG_BUFFER_BYTES),
            epoch,
            next_sequence: 1,
        }
    }

    fn run(mut self, stop: flume::Receiver<()>) {
        let interval = self.shared.config.flush_interval();
        let mut next_flush = Instant::now() + interval;
        loop {
            let (blocked, epoch) = self.shared.database.logging_protection_state();
            if blocked || epoch != self.epoch {
                self.buffer.clear();
                self.file = None;
                self.epoch = epoch;
            }
            if Instant::now() >= next_flush {
                self.flush_buffer();
                next_flush = Instant::now() + interval;
            }
            enum Ev {
                Msg(Result<String, flume::RecvError>),
                Stop,
            }
            let ev = flume::Selector::new()
                .recv(&self.receiver, Ev::Msg)
                .recv(&stop, |_| Ev::Stop)
                .wait_deadline(next_flush);
            match ev {
                Ok(Ev::Msg(Ok(line))) => {
                    if !blocked {
                        self.buffer.extend_from_slice(line.as_bytes());
                        if self.buffer.len() >= LOG_BUFFER_BYTES {
                            self.flush_buffer();
                            next_flush = Instant::now() + interval;
                        }
                    }
                }
                Ok(Ev::Msg(Err(_)) | Ev::Stop) => {
                    while let Ok(line) = self.receiver.try_recv() {
                        self.buffer.extend_from_slice(line.as_bytes());
                    }
                    self.flush_buffer();
                    self.file = None;
                    return;
                }
                Err(_) => {}
            }
        }
    }

    fn record_failure(&mut self, path: PathBuf, operation: &'static str, error: &io::Error) {
        if error.kind() == io::ErrorKind::StorageFull {
            let reason = super::monitor::ProtectionReason::FilesystemWriteFailed {
                area: super::monitor::ProtectedArea::Logs,
                path: path.clone(),
                kind: error.kind(),
                os_error: error.raw_os_error(),
                detail: error.to_string(),
            };
            self.shared.database.activate_filesystem_protection(reason);
        }
        *self
            .shared
            .last_failure
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(Arc::new(LoggerFailure {
            path,
            operation,
            kind: error.kind(),
            os_error: error.raw_os_error(),
            detail: error.to_string(),
        }));
        self.buffer.clear();
        self.file = None;
    }

    fn flush_buffer(&mut self) {
        if self.buffer.is_empty() {
            return;
        }
        if self.shared.database.logging_protection_state().0 {
            self.buffer.clear();
            self.file = None;
            return;
        }
        let max_size = self.shared.config.max_file_size_bytes;
        if let Some(active) = &self.file
            && active.size.saturating_add(self.buffer.len() as u64) > max_size
        {
            self.file = None;
        }
        if self.file.is_none() && !self.open_next_file() {
            self.buffer.clear();
            return;
        }
        let Some(mut active) = self.file.take() else {
            self.buffer.clear();
            return;
        };
        if let Err(error) = active
            .file
            .write_all(&self.buffer)
            .and_then(|()| active.file.flush())
        {
            let path = active.path.clone();
            drop(active);
            self.record_failure(path, "write", &error);
            return;
        }
        active.size += self.buffer.len() as u64;
        self.buffer.clear();
        self.file = Some(active);
    }

    fn open_next_file(&mut self) -> bool {
        let dir = self.shared.database.log_directory().to_path_buf();
        let now = time::OffsetDateTime::now_utc();
        let seq = self.next_sequence;
        self.next_sequence += 1;
        let path = dir.join(runtime_file_name(now, seq));
        let file = match OpenOptions::new().create_new(true).write(true).open(&path) {
            Ok(file) => file,
            Err(error) => {
                self.record_failure(path, "create", &error);
                return false;
            }
        };
        let meta = match file.metadata() {
            Ok(meta) => meta,
            Err(error) => {
                self.record_failure(path, "metadata", &error);
                return false;
            }
        };
        let log_files = self.shared.database.log_files();
        log_files
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(path.clone(), meta);
        self.file = Some(ActiveFile {
            path,
            file,
            size: 0,
            log_files,
        });
        true
    }
}
