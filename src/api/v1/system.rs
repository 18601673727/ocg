use super::contracts::*;
use super::*;

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
#[ts(rename = "Claims")]
pub(crate) struct ClaimsView {
    count: u64,
    mean_us: Option<u64>,
    max_us: u64,
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

/// Why the quarantined list could not be published. Bounded: the cause stays
/// in the server log.
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
    claims: ClaimsView,
    queue: QueueView,
    process: ProcessView,
    /// Absent (`null`) when the ownership state could not be read, which is
    /// never the same as an empty list.
    quarantined: Option<Vec<QuarantinedView>>,
    quarantined_error: Option<QuarantinedErrorView>,
}

/// What the host can establish: it runs, as the listener answers only
/// while it does, and what the database says of itself now.
pub(super) async fn health(database: &DatabaseHandle) -> Value {
    let status = match database.status() {
        DatabaseStatus::Open => "open",
        DatabaseStatus::Protected => "protected",
        DatabaseStatus::ShuttingDown => "shutting_down",
        DatabaseStatus::WorkerStopped => "stopped",
        DatabaseStatus::Closed => "closed",
    };
    let readable = database.job(database.genesis()).await.is_ok();
    let vitals = database.vitals().await;
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
            database_bytes: vitals.database_bytes,
            available_bytes: vitals.available_bytes,
            durable_commits: vitals.durable_commits,
            commit_us_mean: vitals.commit_us_mean,
            commit_us_max: vitals.commit_us_max,
        },
        claims: ClaimsView {
            count: vitals.claims_count,
            mean_us: vitals.claim_us_mean,
            max_us: vitals.claim_us_max,
        },
        queue: QueueView {
            pending: vitals.queue_pending,
            ready: vitals.queue_ready,
            delayed: vitals.queue_delayed,
            running: vitals.queue_running,
            commands_waiting: vitals.commands_waiting,
        },
        process: ProcessView {
            rss_bytes: vitals.rss_bytes,
            peak_rss_bytes: vitals.peak_rss_bytes,
        },
        quarantined,
        quarantined_error,
    }))
}
