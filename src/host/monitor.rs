use std::path::{Path, PathBuf};
use std::time::Duration;

pub use crate::db::protection::{
    DiskProtectionStatus, ProtectedArea, ProtectionLock, ProtectionReason, ProtectionWatch,
    RecoveryError, RecoveryReport, ReopenFailure, SpaceMeasurement, SpaceQueryFailure,
    SpaceShortage,
};
use crate::host::data_directory::DataDirectory;

pub const DEFAULT_DISK_LOCK_THRESHOLD_BYTES: u64 = 1024 * 1024 * 1024;
pub const DEFAULT_DISK_RESUME_THRESHOLD_BYTES: u64 = 2 * 1024 * 1024 * 1024;
pub const DEFAULT_DISK_CHECK_INTERVAL_MS: u64 = 120 * 60 * 1000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CleanupConfig {
    pub max_log_files_per_run: u32,
}

impl Default for CleanupConfig {
    fn default() -> Self {
        Self {
            max_log_files_per_run: 1000,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FilesystemConfig {
    pub disk_lock_threshold_bytes: u64,
    pub disk_resume_threshold_bytes: u64,
    pub disk_check_interval_ms: u64,
    #[serde(default)]
    pub cleanup_config: CleanupConfig,
}

impl Default for FilesystemConfig {
    fn default() -> Self {
        Self {
            disk_lock_threshold_bytes: DEFAULT_DISK_LOCK_THRESHOLD_BYTES,
            disk_resume_threshold_bytes: DEFAULT_DISK_RESUME_THRESHOLD_BYTES,
            disk_check_interval_ms: DEFAULT_DISK_CHECK_INTERVAL_MS,
            cleanup_config: CleanupConfig::default(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum FilesystemConfigError {
    #[error("disk_lock_threshold_bytes must be greater than zero")]
    LockThresholdZero,
    #[error(
        "disk_resume_threshold_bytes ({resume}) must be greater than disk_lock_threshold_bytes ({lock})"
    )]
    ResumeNotAboveLock { lock: u64, resume: u64 },
    #[error("disk_check_interval_ms must be greater than zero")]
    CheckIntervalZero,
    #[error("cleanup_config.max_log_files_per_run must be greater than zero")]
    CleanupLimitZero,
}

impl FilesystemConfig {
    pub fn validate(&self) -> Result<(), FilesystemConfigError> {
        if self.disk_lock_threshold_bytes == 0 {
            return Err(FilesystemConfigError::LockThresholdZero);
        }
        if self.disk_resume_threshold_bytes <= self.disk_lock_threshold_bytes {
            return Err(FilesystemConfigError::ResumeNotAboveLock {
                lock: self.disk_lock_threshold_bytes,
                resume: self.disk_resume_threshold_bytes,
            });
        }
        if self.disk_check_interval_ms == 0 {
            return Err(FilesystemConfigError::CheckIntervalZero);
        }
        if self.cleanup_config.max_log_files_per_run == 0 {
            return Err(FilesystemConfigError::CleanupLimitZero);
        }
        Ok(())
    }

    pub fn check_interval(&self) -> Duration {
        Duration::from_millis(self.disk_check_interval_ms)
    }
}

pub(crate) fn protected_paths(data_directory: &DataDirectory) -> [(ProtectedArea, PathBuf); 3] {
    [
        (
            ProtectedArea::Database,
            data_directory.database_path().to_path_buf(),
        ),
        (
            ProtectedArea::Attachments,
            data_directory.attachments_path(),
        ),
        (ProtectedArea::Logs, data_directory.logs_path()),
    ]
}

fn measure(area: ProtectedArea, path: &Path) -> Result<u64, SpaceQueryFailure> {
    fs4::available_space(path).map_err(|error| SpaceQueryFailure {
        area,
        path: path.to_path_buf(),
        kind: error.kind(),
        os_error: error.raw_os_error(),
        detail: error.to_string(),
    })
}

pub(crate) fn database_disk_usage(directory: &Path) -> (Option<u64>, Option<u64>) {
    let database_bytes = std::fs::metadata(crate::db::database_file(directory))
        .ok()
        .map(|metadata| metadata.len());
    let available_bytes = measure(ProtectedArea::Database, directory).ok();
    (database_bytes, available_bytes)
}

pub(crate) fn process_memory() -> (Option<u64>, Option<u64>) {
    #[cfg(unix)]
    let peak = {
        let mut rusage: libc::rusage = unsafe { std::mem::zeroed() };
        (unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut rusage) } == 0).then(|| {
            let rss = rusage.ru_maxrss.max(0) as u64;
            if cfg!(target_os = "macos") {
                rss
            } else {
                rss.saturating_mul(1024)
            }
        })
    };
    #[cfg(not(unix))]
    let peak = None;

    #[cfg(target_os = "linux")]
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        let mut current = None;
        let mut peak = peak;
        for line in status.lines() {
            let Some((name, value)) = line.split_once(':') else {
                continue;
            };
            let metric = match name {
                "VmRSS" => &mut current,
                "VmHWM" => &mut peak,
                _ => continue,
            };
            if let Some(kb) = value
                .split_whitespace()
                .next()
                .and_then(|text| text.parse::<u64>().ok())
            {
                *metric = Some(kb.saturating_mul(1024));
            }
        }
        return (current, peak);
    }
    (None, peak)
}

pub(crate) fn leaves_threshold(
    available_bytes: u64,
    expected_write_bytes: u64,
    threshold_bytes: u64,
) -> bool {
    available_bytes
        .checked_sub(expected_write_bytes)
        .is_some_and(|remaining| remaining >= threshold_bytes)
}

pub(crate) fn check_space(
    area: ProtectedArea,
    path: &Path,
    expected_write_bytes: u64,
    threshold_bytes: u64,
) -> Result<u64, ProtectionReason> {
    let available_bytes = measure(area, path).map_err(ProtectionReason::SpaceQueryFailed)?;
    if leaves_threshold(available_bytes, expected_write_bytes, threshold_bytes) {
        Ok(available_bytes)
    } else {
        Err(ProtectionReason::InsufficientSpace(SpaceShortage {
            area,
            path: path.to_path_buf(),
            available_bytes,
            expected_write_bytes,
            threshold_bytes,
        }))
    }
}

pub(crate) async fn report_protection(watch: ProtectionWatch) {
    let mut lock = watch.status().lock;
    if let Some(notice) = protection_notice(None, lock.as_ref()) {
        eprintln!("{notice}");
    }
    while let Some(status) = watch.changed().await {
        if let Some(notice) = protection_notice(lock.as_ref(), status.lock.as_ref()) {
            eprintln!("{notice}");
        }
        lock = status.lock;
    }
}

fn protection_notice(
    before: Option<&ProtectionLock>,
    after: Option<&ProtectionLock>,
) -> Option<String> {
    match (before, after) {
        (None, Some(lock)) => Some(format!(
            "OCG storage protection is active; commands are refused and the file log is paused: {lock}"
        )),
        (Some(_), None) => {
            Some("OCG storage protection was lifted; commands are accepted again".to_owned())
        }
        _ => None,
    }
}
