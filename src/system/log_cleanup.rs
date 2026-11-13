use std::fs;
use std::io;
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime};

use super::logger::is_runtime_file_name;
use crate::db::DatabaseHandle;

pub const RETENTION_CHECK_INTERVAL: Duration = Duration::from_secs(60 * 60);

#[derive(Debug, thiserror::Error)]
pub enum CleanupError {
    #[error("failed to start log cleanup worker: {0}")]
    Spawn(#[source] io::Error),
    #[error("log cleanup worker panicked")]
    WorkerPanicked,
}

pub struct CoreLogCleanup {
    stop: flume::Sender<()>,
    thread: Option<JoinHandle<()>>,
}

pub async fn start_retention(database: &DatabaseHandle) -> Result<CoreLogCleanup, CleanupError> {
    let database = database.clone();
    let (stop_tx, stop_rx) = flume::bounded(1);
    let thread = std::thread::Builder::new()
        .name("ocg-log-cleanup".to_owned())
        .spawn(move || {
            loop {
                cleanup_expired(&database);
                match stop_rx.recv_timeout(RETENTION_CHECK_INTERVAL) {
                    Ok(()) | Err(flume::RecvTimeoutError::Disconnected) => break,
                    Err(flume::RecvTimeoutError::Timeout) => {}
                }
            }
        })
        .map_err(CleanupError::Spawn)?;
    Ok(CoreLogCleanup {
        stop: stop_tx,
        thread: Some(thread),
    })
}

impl CoreLogCleanup {
    pub fn shutdown(mut self) -> Result<(), CleanupError> {
        let _ = self.stop.try_send(());
        if let Some(thread) = self.thread.take() {
            thread.join().map_err(|_| CleanupError::WorkerPanicked)?;
        }
        Ok(())
    }
}

impl Drop for CoreLogCleanup {
    fn drop(&mut self) {
        let _ = self.stop.try_send(());
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn cleanup_expired(database: &DatabaseHandle) {
    let policy = database.active_system_policy();
    let Some(cutoff) = SystemTime::now().checked_sub(Duration::from_secs(
        u64::from(policy.logger_config.retention_days) * 86400,
    )) else {
        return;
    };
    let limit = policy
        .filesystem_config
        .cleanup_config
        .max_log_files_per_run;
    let Ok(entries) = fs::read_dir(database.log_directory()) else {
        return;
    };
    let mut deleted = 0u32;
    for entry in entries.flatten() {
        if deleted >= limit {
            break;
        }
        if !is_runtime_file_name(&entry.file_name()) {
            continue;
        }
        let path = entry.path();
        if database
            .log_files()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains_key(&path)
        {
            continue;
        }
        let Ok(meta) = fs::symlink_metadata(&path) else {
            continue;
        };
        if meta.is_file() && meta.modified().is_ok_and(|m| m < cutoff) {
            match fs::remove_file(&path) {
                Ok(()) => deleted += 1,
                Err(error) if error.kind() == std::io::ErrorKind::StorageFull => {
                    database.activate_filesystem_protection(
                        super::monitor::ProtectionReason::FilesystemWriteFailed {
                            area: super::monitor::ProtectedArea::Logs,
                            path,
                            kind: error.kind(),
                            os_error: error.raw_os_error(),
                            detail: error.to_string(),
                        },
                    );
                    break;
                }
                Err(_) => {}
            }
        }
    }
    if deleted > 0 {
        database.logs_changed();
    }
}
