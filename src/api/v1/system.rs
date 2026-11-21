use super::contracts::envelope;
use super::{DatabaseHandle, DatabaseStatus, Value};
use crate::host::data_directory::DataDirectory;
use crate::host::metrics::{database_disk_usage, process_memory};
use crate::host::policy::SystemPolicy;

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "ActiveSystemSettings")]
pub(crate) struct ActiveSystemSettingsView {
    path: String,
    content: String,
}

impl ActiveSystemSettingsView {
    pub(crate) fn new(data_directory: &DataDirectory, policy: &SystemPolicy) -> Self {
        Self {
            path: SystemPolicy::path(data_directory).display().to_string(),
            content: policy.to_conf(),
        }
    }
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
    pool: String,
    executor: String,
    generation: u32,
    epoch: u64,
    job_revision: u64,
    releasable: bool,
    evidence: String,
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
    next_after: Option<String>,
}

pub(super) async fn health(
    database: &DatabaseHandle,
    after: Option<crate::db::entities::AllocationCursor>,
) -> Value {
    let status = match database.status() {
        DatabaseStatus::Open => "open",
        DatabaseStatus::Protected => "protected",
        DatabaseStatus::ShuttingDown => "shutting_down",
        DatabaseStatus::WorkerStopped => "stopped",
        DatabaseStatus::Closed => "closed",
    };
    let readable = database.check_readable().await.is_ok();
    let directory = database.data_directory().database_path().to_path_buf();
    let ((database_bytes, available_bytes), (rss_bytes, peak_rss_bytes)) =
        ntex::rt::spawn_blocking(move || (database_disk_usage(&directory), process_memory()))
            .await
            .unwrap_or_default();
    let queue = match database.queue_counts().await {
        Ok(counts) => Some(counts),
        Err(error) => {
            log::debug!("failed to read database health queue counts: {error}");
            None
        }
    };
    let (commit_us_mean, commit_us_max) = database.commit_timing();
    let (quarantined, quarantined_error, next_after) =
        match database.uncertain_allocations(after).await {
            Ok(uncertain) => (
                Some(
                    uncertain
                        .records
                        .into_iter()
                        .map(|allocation| QuarantinedView {
                            job: allocation.allocation.token.job.to_string(),
                            pool: allocation.allocation.token.pool.to_string(),
                            executor: allocation.allocation.token.executor.to_string(),
                            epoch: allocation.allocation.token.epoch,
                            generation: allocation.allocation.token.generation.get(),
                            job_revision: allocation.allocation.job_revision,
                            releasable: allocation.allocation.stop_proof.is_none(),
                            evidence: allocation.evidence,
                            since: allocation.since.as_unix_micros(),
                        })
                        .collect::<Vec<_>>(),
                ),
                None,
                uncertain.next.map(|(p, e, j)| format!("{p:x}.{e:x}.{j:x}")),
            ),
            Err(error) => {
                log::warn!("failed to read uncertain allocations for health: {error}");
                (None, Some(QuarantinedErrorView::Unreadable), None)
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
        next_after,
    }))
}
