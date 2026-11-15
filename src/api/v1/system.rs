use super::contracts::envelope;
use super::{DatabaseHandle, DatabaseStatus, Value};

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "ActiveSystemSettings")]
pub(crate) struct ActiveSystemSettingsView {
    pub(super) path: String,
    pub(super) content: String,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "DatabaseHealth")]
pub(crate) struct DatabaseHealthView {
    status: String,
    accepting_commands: bool,
    readable: bool,
    database_bytes: Option<u64>,
    available_bytes: Option<u64>,
    durable_commits: u64,
    commit_us_mean: Option<u64>,
    commit_us_max: u64,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "Queue")]
pub(crate) struct QueueView {
    pending: Option<u64>,
    ready: Option<u64>,
    delayed: Option<u64>,
    running: Option<u64>,
    commands_waiting: u64,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "Process")]
pub(crate) struct ProcessView {
    rss_bytes: Option<u64>,
    peak_rss_bytes: Option<u64>,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "Quarantined")]
pub(crate) struct QuarantinedView {
    job: String,
    pool: Option<String>,
    since: i64,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "QuarantinedError")]
#[serde(rename_all = "snake_case")]
pub(crate) enum QuarantinedErrorView {
    Unreadable,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "Health")]
pub(crate) struct HealthView {
    version: String,
    host: String,
    database: DatabaseHealthView,
    queue: QueueView,
    process: ProcessView,
    quarantined: Option<Vec<QuarantinedView>>,
    quarantined_error: Option<QuarantinedErrorView>,
}

fn process_memory() -> (Option<u64>, Option<u64>) {
    let mut peak = None;
    #[cfg(unix)]
    {
        let mut rusage: libc::rusage = unsafe { std::mem::zeroed() };
        if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut rusage) } == 0 {
            #[cfg(target_os = "macos")]
            {
                peak = Some(rusage.ru_maxrss.max(0) as u64);
            }
            #[cfg(not(target_os = "macos"))]
            {
                peak = Some((rusage.ru_maxrss.max(0) as u64).saturating_mul(1024));
            }
        }
    }

    #[allow(unused_mut)]
    let mut current = None;
    #[cfg(target_os = "linux")]
    {
        if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
            for line in status.lines() {
                if let Some(rest) = line.strip_prefix("VmRSS:") {
                    let parts: Vec<&str> = rest.split_whitespace().collect();
                    if let Some(kb) = parts.first().and_then(|s| s.parse::<u64>().ok()) {
                        current = Some(kb.saturating_mul(1024));
                    }
                }
                if let Some(rest) = line.strip_prefix("VmHWM:") {
                    let parts: Vec<&str> = rest.split_whitespace().collect();
                    if let Some(kb) = parts.first().and_then(|s| s.parse::<u64>().ok()) {
                        peak = Some(kb.saturating_mul(1024));
                    }
                }
            }
        }
    }

    (current, peak)
}

pub(super) async fn health(database: &DatabaseHandle) -> Value {
    let status = match database.status() {
        DatabaseStatus::Open => "open",
        DatabaseStatus::Protected => "protected",
        DatabaseStatus::ShuttingDown => "shutting_down",
        DatabaseStatus::WorkerStopped => "stopped",
        DatabaseStatus::Closed => "closed",
    };
    let readable = database.check_readable().await.is_ok();
    let directory = database.data_directory().database_path().to_path_buf();
    let (database_bytes, available_bytes) = ntex::rt::spawn_blocking(move || {
        (
            std::fs::metadata(crate::db::database_file(&directory))
                .map(|metadata| metadata.len())
                .ok(),
            fs4::available_space(&directory).ok(),
        )
    })
    .await
    .unwrap_or((None, None));
    let queue = match database.queue_counts().await {
        Ok(counts) => Some(counts),
        Err(error) => {
            log::debug!("failed to read database health queue counts: {error}");
            None
        }
    };
    let (commit_us_mean, commit_us_max) = database.commit_timing();
    let (rss_bytes, peak_rss_bytes) = process_memory();
    let (quarantined, quarantined_error) = match database.uncertain_allocations().await {
        Ok(uncertain) => (
            Some(
                uncertain
                    .into_iter()
                    .map(|allocation| QuarantinedView {
                        job: allocation.job.to_string(),
                        pool: Some(allocation.pool.to_string()),
                        since: allocation.since.as_unix_micros(),
                    })
                    .collect::<Vec<_>>(),
            ),
            None,
        ),
        Err(error) => {
            log::warn!("failed to read uncertain allocations for health: {error}");
            (None, Some(QuarantinedErrorView::Unreadable))
        }
    };
    envelope(serde_json::json!(HealthView {
        version: env!("CARGO_PKG_VERSION").to_owned(),
        host: "running".to_owned(),
        database: DatabaseHealthView {
            status: status.to_owned(),
            accepting_commands: status == "open",
            readable,
            database_bytes,
            available_bytes,
            durable_commits: database.durable_commits(),
            commit_us_mean,
            commit_us_max,
        },
        queue: QueueView {
            pending: queue.as_ref().map(|counts| counts.pending),
            ready: queue.as_ref().map(|counts| counts.ready),
            delayed: queue.as_ref().map(|counts| counts.delayed),
            running: queue.as_ref().map(|counts| counts.running),
            commands_waiting: database.commands_waiting(),
        },
        process: ProcessView {
            rss_bytes,
            peak_rss_bytes,
        },
        quarantined,
        quarantined_error,
    }))
}
