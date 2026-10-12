use std::collections::HashMap;
use std::ffi::OsStr;
use std::fmt;
use std::fmt::Write as _;
use std::fs::{File, Metadata, OpenOptions};
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError, TryLockError};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::database::{DatabaseHandle, DatabaseStatus};
use crate::disk_protection::{self, ProtectedArea, ProtectionReason};

pub const LOG_QUEUE_CAPACITY: usize = 256;
pub const MAX_LOG_RECORD_BYTES: usize = 4096;
pub const MAX_LOG_MESSAGE_BYTES: usize = 4000;
pub const LOG_BUFFER_BYTES: usize = 64 * 1024;
const FILE_CREATION_ALLOWANCE_BYTES: u64 = 4096;

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
    let number = |range| {
        stamp
            .get(range)
            .and_then(|value: &str| value.parse::<u16>().ok())
    };
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
    pub active_file: Option<Arc<PathBuf>>,
    pub last_failure: Option<Arc<LoggerFailure>>,
    pub file_failures: u64,
    pub accepted_records: u64,
    pub written_records: u64,
    pub queued_records: u64,
    pub buffered_records: u64,
    pub dropped_queue_full: u64,
    pub dropped_oversized: u64,
    pub dropped_protected: u64,
    pub dropped_failed: u64,
    pub dropped_closed: u64,
    pub accepting: bool,
    pub protected: bool,
    pub file_logging_disabled: bool,
    pub stopped: bool,
}

impl LoggerStatus {
    pub fn dropped_records(&self) -> u64 {
        self.dropped_queue_full
            .saturating_add(self.dropped_oversized)
            .saturating_add(self.dropped_protected)
            .saturating_add(self.dropped_failed)
            .saturating_add(self.dropped_closed)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("logger status is being updated; retry the nonblocking snapshot")]
pub struct LoggerStatusBusy;

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

#[derive(Default)]
struct FileStatus {
    active_file: Option<Arc<PathBuf>>,
    last_failure: Option<Arc<LoggerFailure>>,
}

struct Shared {
    database: DatabaseHandle,
    config: LoggerConfig,
    accepting: AtomicBool,
    emitting: AtomicUsize,
    disabled: AtomicBool,
    stopped: AtomicBool,
    flush_requested: AtomicBool,
    file_status: Mutex<FileStatus>,
    file_failures: AtomicU64,
    accepted: AtomicU64,
    written: AtomicU64,
    queued: AtomicU64,
    buffered: AtomicU64,
    full: AtomicU64,
    oversized: AtomicU64,
    protected: AtomicU64,
    failed: AtomicU64,
    closed: AtomicU64,
}

#[derive(Clone)]
pub struct LoggerHandle {
    shared: Arc<Shared>,
    sender: flume::Sender<Record>,
    wake: flume::Sender<()>,
}

impl LoggerHandle {
    pub fn active_config(&self) -> LoggerConfig {
        self.shared.config
    }

    pub fn status(&self) -> Result<LoggerStatus, LoggerStatusBusy> {
        let details = match self.shared.file_status.try_lock() {
            Ok(details) => details,
            Err(TryLockError::Poisoned(error)) => error.into_inner(),
            Err(TryLockError::WouldBlock) => return Err(LoggerStatusBusy),
        };
        Ok(self.snapshot(&details))
    }

    fn snapshot(&self, details: &FileStatus) -> LoggerStatus {
        LoggerStatus {
            active_file: details.active_file.clone(),
            last_failure: details.last_failure.clone(),
            file_failures: self.shared.file_failures.load(Ordering::Relaxed),
            accepted_records: self.shared.accepted.load(Ordering::Relaxed),
            written_records: self.shared.written.load(Ordering::Relaxed),
            queued_records: self.shared.queued.load(Ordering::Relaxed),
            buffered_records: self.shared.buffered.load(Ordering::Relaxed),
            dropped_queue_full: self.shared.full.load(Ordering::Relaxed),
            dropped_oversized: self.shared.oversized.load(Ordering::Relaxed),
            dropped_protected: self.shared.protected.load(Ordering::Relaxed),
            dropped_failed: self.shared.failed.load(Ordering::Relaxed),
            dropped_closed: self.shared.closed.load(Ordering::Relaxed),
            accepting: self.shared.accepting.load(Ordering::SeqCst),
            protected: self.shared.database.logging_protection_state().0,
            file_logging_disabled: self.shared.disabled.load(Ordering::SeqCst),
            stopped: self.shared.stopped.load(Ordering::SeqCst),
        }
    }
}

struct Emission<'a>(&'a AtomicUsize);
impl Drop for Emission<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl log::Log for LoggerHandle {
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        metadata.level() <= self.shared.config.level.filter()
    }

    fn log(&self, record: &log::Record<'_>) {
        if !self.enabled(record.metadata()) {
            return;
        }
        self.shared.emitting.fetch_add(1, Ordering::SeqCst);
        let _emission = Emission(&self.shared.emitting);
        if !self.shared.accepting.load(Ordering::SeqCst) {
            self.shared.closed.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if self.shared.disabled.load(Ordering::SeqCst) {
            self.shared.failed.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let (blocked, epoch) = self.shared.database.logging_protection_state();
        if blocked {
            self.shared.protected.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let mut message = Text::<MAX_LOG_MESSAGE_BYTES>::new();
        if fmt::write(&mut message, *record.args()).is_err() {
            self.shared.oversized.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if !self.shared.accepting.load(Ordering::SeqCst) {
            self.shared.closed.fetch_add(1, Ordering::Relaxed);
            return;
        }
        self.shared.queued.fetch_add(1, Ordering::SeqCst);
        match self.sender.try_send(Record {
            epoch,
            level: record.level(),
            message,
        }) {
            Ok(()) => {
                self.shared.accepted.fetch_add(1, Ordering::Relaxed);
            }
            Err(error) => {
                self.shared.queued.fetch_sub(1, Ordering::SeqCst);
                match error {
                    flume::TrySendError::Full(_) => {
                        self.shared.full.fetch_add(1, Ordering::Relaxed);
                    }
                    flume::TrySendError::Disconnected(_) => {
                        self.shared.failed.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
    }

    fn flush(&self) {
        self.shared.flush_requested.store(true, Ordering::SeqCst);
        // Facade flush is only a request: a service thread must never wait for file I/O.
        self.wake.try_send(()).ok();
    }
}

struct Text<const N: usize> {
    bytes: [u8; N],
    len: usize,
}
impl<const N: usize> Text<N> {
    fn new() -> Self {
        Self {
            bytes: [0; N],
            len: 0,
        }
    }
    fn append(&mut self, bytes: &[u8]) -> fmt::Result {
        let Some(end) = self.len.checked_add(bytes.len()).filter(|end| *end <= N) else {
            return Err(fmt::Error);
        };
        self.bytes[self.len..end].copy_from_slice(bytes);
        self.len = end;
        Ok(())
    }
    fn bytes(&self) -> &[u8] {
        &self.bytes[..self.len]
    }
}
impl<const N: usize> fmt::Write for Text<N> {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        self.append(text.as_bytes())
    }
}

struct Record {
    epoch: u64,
    level: log::Level,
    message: Text<MAX_LOG_MESSAGE_BYTES>,
}

pub struct RuntimeLogger {
    handle: LoggerHandle,
    stop: flume::Sender<()>,
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
            emitting: AtomicUsize::new(0),
            disabled: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
            flush_requested: AtomicBool::new(false),
            file_status: Mutex::new(FileStatus::default()),
            file_failures: AtomicU64::new(0),
            accepted: AtomicU64::new(0),
            written: AtomicU64::new(0),
            queued: AtomicU64::new(0),
            buffered: AtomicU64::new(0),
            full: AtomicU64::new(0),
            oversized: AtomicU64::new(0),
            protected: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            closed: AtomicU64::new(0),
        });
        let (sender, receiver) = flume::bounded(LOG_QUEUE_CAPACITY);
        let (wake, awakened) = flume::bounded(1);
        let (stop, stopping) = flume::bounded(1);
        let worker_shared = shared.clone();
        let thread = std::thread::Builder::new()
            .name("ocg-logger".into())
            .spawn(move || {
                let _exit = WorkerExit {
                    shared: worker_shared.clone(),
                    receiver: receiver.clone(),
                };
                Worker::new(worker_shared, receiver).run(awakened, stopping);
            })
            .map_err(LoggerError::Spawn)?;
        Ok(Self {
            handle: LoggerHandle {
                shared,
                sender,
                wake,
            },
            stop,
            thread: Some(thread),
        })
    }

    pub fn handle(&self) -> LoggerHandle {
        self.handle.clone()
    }

    pub fn shutdown(mut self) -> Result<LoggerStatus, LoggerError> {
        self.stop_and_join()?;
        let details = self
            .handle
            .shared
            .file_status
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        Ok(self.handle.snapshot(&details))
    }

    fn stop_and_join(&mut self) -> Result<(), LoggerError> {
        self.handle.shared.accepting.store(false, Ordering::SeqCst);
        self.stop.try_send(()).ok();
        if let Some(thread) = self.thread.take() {
            thread.join().map_err(|_| LoggerError::WorkerPanicked)?;
        }
        Ok(())
    }
}

impl Drop for RuntimeLogger {
    fn drop(&mut self) {
        if let Err(error) = self.stop_and_join() {
            eprintln!("{error}");
        }
    }
}

struct WorkerExit {
    shared: Arc<Shared>,
    receiver: flume::Receiver<Record>,
}
impl Drop for WorkerExit {
    fn drop(&mut self) {
        self.shared.accepting.store(false, Ordering::SeqCst);
        for _ in self.receiver.drain() {
            self.shared.queued.fetch_sub(1, Ordering::SeqCst);
            self.shared.failed.fetch_add(1, Ordering::Relaxed);
        }
        let buffered = self.shared.buffered.swap(0, Ordering::SeqCst);
        self.shared.failed.fetch_add(buffered, Ordering::Relaxed);
        self.shared
            .file_status
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .active_file = None;
        self.shared.stopped.store(true, Ordering::SeqCst);
    }
}

struct ActiveFile {
    file: File,
    path: Arc<PathBuf>,
    written: u64,
    files: Arc<LogFiles>,
}
impl Drop for ActiveFile {
    fn drop(&mut self) {
        self.files
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(self.path.as_ref());
    }
}
struct Worker {
    shared: Arc<Shared>,
    receiver: flume::Receiver<Record>,
    file: Option<ActiveFile>,
    buffer: Vec<u8>,
    records: u64,
    epoch: u64,
    next_sequence: u64,
    flush_due: Option<Instant>,
}
enum Event {
    Record(Box<Result<Record, flume::RecvError>>),
    Wake,
    Protection(Result<(), flume::RecvError>),
    Stop,
}

impl Worker {
    fn new(shared: Arc<Shared>, receiver: flume::Receiver<Record>) -> Self {
        let epoch = shared.database.logging_protection_state().1;
        Self {
            shared,
            receiver,
            file: None,
            buffer: Vec::with_capacity(LOG_BUFFER_BYTES),
            records: 0,
            epoch,
            next_sequence: 1,
            flush_due: None,
        }
    }

    fn run(mut self, wake: flume::Receiver<()>, stop: flume::Receiver<()>) {
        let watch = self.shared.database.watch_disk_protection();
        let mut closing = false;
        loop {
            let blocked = self.refresh_protection();
            if blocked || self.shared.disabled.load(Ordering::SeqCst) {
                for _ in 0..LOG_QUEUE_CAPACITY {
                    let Ok(record) = self.receiver.try_recv() else {
                        break;
                    };
                    self.shared.queued.fetch_sub(1, Ordering::SeqCst);
                    self.append(record);
                }
            }
            if closing
                && self.shared.emitting.load(Ordering::SeqCst) == 0
                && self.receiver.is_empty()
            {
                self.flush_buffer();
                self.close_file();
                return;
            }
            if self.flush_due.is_some_and(|due| Instant::now() >= due)
                || self.shared.flush_requested.swap(false, Ordering::SeqCst)
            {
                self.flush_buffer();
                continue;
            }
            let selector = flume::Selector::new()
                .recv(&self.receiver, |record| Event::Record(Box::new(record)))
                .recv(&wake, |_| Event::Wake)
                .recv(watch.notifications(), Event::Protection);
            let event = if closing {
                selector.wait_timeout(Duration::from_millis(1)).ok()
            } else {
                let selector = selector.recv(&stop, |_| Event::Stop);
                match self.flush_due {
                    Some(due) => selector.wait_deadline(due).ok(),
                    None => Some(selector.wait()),
                }
            };
            match event {
                Some(Event::Record(record)) => match *record {
                    Ok(record) => {
                        self.shared.queued.fetch_sub(1, Ordering::SeqCst);
                        self.append(record);
                    }
                    Err(_) => closing = true,
                },
                Some(Event::Stop) => closing = true,
                Some(Event::Protection(Err(_))) => {
                    self.shared.accepting.store(false, Ordering::SeqCst);
                    self.shared.disabled.store(true, Ordering::SeqCst);
                    self.discard_buffer(false);
                    closing = true;
                }
                Some(Event::Wake | Event::Protection(Ok(()))) | None => {}
            }
        }
    }

    fn refresh_protection(&mut self) -> bool {
        let (blocked, epoch) = self.shared.database.logging_protection_state();
        if blocked || epoch != self.epoch {
            self.discard_buffer(true);
            self.close_file();
            self.epoch = epoch;
        }
        blocked
    }

    fn drop_record(&self) {
        if self.shared.database.logging_protection_state().0 {
            self.shared.protected.fetch_add(1, Ordering::Relaxed);
        } else {
            self.shared.failed.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn discard_buffer(&mut self, protected: bool) {
        let counter = if protected {
            &self.shared.protected
        } else {
            &self.shared.failed
        };
        counter.fetch_add(self.records, Ordering::Relaxed);
        self.buffer.clear();
        self.records = 0;
        self.shared.buffered.store(0, Ordering::SeqCst);
        self.flush_due = None;
    }

    fn close_file(&mut self) {
        // This is a plain File, not a BufWriter: dropping it never writes discarded bytes.
        self.file = None;
        self.shared
            .file_status
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .active_file = None;
    }

    fn failure(&mut self, failure: LoggerFailure, protect: Option<ProtectionReason>) {
        self.shared.file_failures.fetch_add(1, Ordering::Relaxed);
        self.shared
            .file_status
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .last_failure = Some(Arc::new(failure));
        if let Some(reason) = protect {
            if !self.shared.database.activate_filesystem_protection(reason) {
                self.shared.disabled.store(true, Ordering::SeqCst);
            }
        } else {
            self.shared.disabled.store(true, Ordering::SeqCst);
        }
        let protected = self.shared.database.logging_protection_state().0;
        self.discard_buffer(protected);
        self.close_file();
    }

    fn io_failure(&mut self, operation: &'static str, path: PathBuf, error: io::Error) {
        let protect = (error.kind() == io::ErrorKind::StorageFull).then(|| {
            ProtectionReason::FilesystemWriteFailed {
                area: ProtectedArea::Logs,
                path: path.clone(),
                kind: error.kind(),
                os_error: error.raw_os_error(),
                detail: error.to_string(),
            }
        });
        self.failure(
            LoggerFailure {
                path,
                operation,
                kind: error.kind(),
                os_error: error.raw_os_error(),
                detail: error.to_string(),
            },
            protect,
        );
    }

    fn space(&mut self, expected: u64) -> bool {
        let epoch = self.epoch;
        let path = self.shared.database.log_directory().to_path_buf();
        match disk_protection::check_space(
            ProtectedArea::Logs,
            &path,
            expected,
            self.shared
                .database
                .active_system_policy()
                .filesystem_config
                .disk_lock_threshold_bytes,
        ) {
            Ok(_) => !self.refresh_protection() && self.epoch == epoch,
            Err(reason) => {
                let (kind, os_error) = match &reason {
                    ProtectionReason::SpaceQueryFailed(failure) => (failure.kind, failure.os_error),
                    _ => (io::ErrorKind::StorageFull, None),
                };
                self.failure(
                    LoggerFailure {
                        path,
                        operation: "space check",
                        kind,
                        os_error,
                        detail: reason.to_string(),
                    },
                    Some(reason),
                );
                false
            }
        }
    }

    fn create_file(&mut self, record_bytes: u64) -> bool {
        let now = time::OffsetDateTime::now_utc();
        loop {
            if self.refresh_protection()
                || !self.space(record_bytes.saturating_add(FILE_CREATION_ALLOWANCE_BYTES))
            {
                return false;
            }
            let sequence = self.next_sequence;
            let Some(next) = sequence.checked_add(1) else {
                self.io_failure(
                    "create",
                    self.shared.database.log_directory().to_path_buf(),
                    io::Error::other("runtime log filename sequence exhausted"),
                );
                return false;
            };
            self.next_sequence = next;
            let name = runtime_file_name(now, sequence);
            let path = Arc::new(self.shared.database.log_directory().join(name));
            // Creation and registration share the deletion gate; cleanup cannot see
            // a newly created log before its active identity is registered.
            let files = self.shared.database.log_files();
            let mut active = files.lock().unwrap_or_else(PoisonError::into_inner);
            match OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path.as_ref())
            {
                Ok(file) => {
                    let metadata = match file.metadata() {
                        Ok(metadata) => metadata,
                        Err(error) => {
                            drop(active);
                            self.io_failure("identify active file", path.as_ref().clone(), error);
                            return false;
                        }
                    };
                    active.insert(path.as_ref().clone(), metadata);
                    self.file = Some(ActiveFile {
                        file,
                        path: path.clone(),
                        written: 0,
                        files: files.clone(),
                    });
                    self.shared
                        .file_status
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .active_file = Some(path);
                    drop(active);
                    return !self.refresh_protection();
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => {
                    drop(active);
                    self.io_failure("create", path.as_ref().clone(), error);
                    return false;
                }
            }
        }
    }

    fn append(&mut self, record: Record) {
        if self.refresh_protection() || record.epoch != self.epoch {
            self.shared.protected.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if self.shared.disabled.load(Ordering::SeqCst) {
            self.drop_record();
            return;
        }
        let now = time::OffsetDateTime::now_utc();
        let mut line = Text::<MAX_LOG_RECORD_BYTES>::new();
        let rendered = write!(
            &mut line,
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z {} ",
            now.year(),
            u8::from(now.month()),
            now.day(),
            now.hour(),
            now.minute(),
            now.second(),
            now.millisecond(),
            record.level
        )
        .and_then(|()| line.append(record.message.bytes()))
        .and_then(|()| line.write_str("\n"));
        if rendered.is_err() || line.len as u64 > self.shared.config.max_file_size_bytes {
            self.shared.oversized.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if self.file.as_ref().is_some_and(|file| {
            file.written
                .saturating_add(self.buffer.len() as u64)
                .saturating_add(line.len as u64)
                > self.shared.config.max_file_size_bytes
        }) {
            if !self.flush_buffer() {
                self.drop_record();
                return;
            }
            self.close_file();
        }
        if self.file.is_none() && !self.create_file(line.len as u64) {
            self.drop_record();
            return;
        }
        if self.buffer.len() + line.len > LOG_BUFFER_BYTES && !self.flush_buffer() {
            self.drop_record();
            return;
        }
        if self.refresh_protection() || record.epoch != self.epoch {
            self.shared.protected.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if self.buffer.is_empty() {
            self.flush_due = Instant::now().checked_add(self.shared.config.flush_interval());
        }
        self.buffer.extend_from_slice(line.bytes());
        self.records += 1;
        self.shared.buffered.store(self.records, Ordering::SeqCst);
    }

    fn flush_buffer(&mut self) -> bool {
        if self.refresh_protection() || self.shared.disabled.load(Ordering::SeqCst) {
            return false;
        }
        if self.buffer.is_empty() {
            self.flush_due = None;
            return true;
        }
        let mut offset = 0;
        let epoch = self.epoch;
        while offset < self.buffer.len() {
            if self.refresh_protection() || self.epoch != epoch {
                return false;
            }
            if !self.space((self.buffer.len() - offset) as u64) {
                return false;
            }
            let Some(file) = self.file.as_mut() else {
                self.discard_buffer(false);
                return false;
            };
            // Each partial write is a new filesystem call, so it needs a new protection check.
            match file.file.write(&self.buffer[offset..]) {
                Ok(0) => {
                    let path = file.path.as_ref().clone();
                    self.io_failure(
                        "write",
                        path,
                        io::Error::new(
                            io::ErrorKind::WriteZero,
                            "runtime log write made no progress",
                        ),
                    );
                    return false;
                }
                Ok(written) => {
                    file.written += written as u64;
                    offset += written;
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => {
                    let path = file.path.as_ref().clone();
                    self.io_failure("write", path, error);
                    return false;
                }
            }
        }
        if let Some(file) = self.file.as_mut()
            && let Err(error) = file.file.flush()
        {
            let path = file.path.as_ref().clone();
            self.io_failure("flush", path, error);
            return false;
        }
        self.shared
            .written
            .fetch_add(self.records, Ordering::Relaxed);
        self.buffer.clear();
        self.records = 0;
        self.shared.buffered.store(0, Ordering::SeqCst);
        self.flush_due = None;
        true
    }
}
