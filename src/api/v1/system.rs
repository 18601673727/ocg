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
#[ts(rename = "Health")]
pub(crate) struct HealthView {
    host: String,
    database: DatabaseHealthView,
    claims: ClaimsView,
    queue: QueueView,
    process: ProcessView,
    quarantined: Vec<QuarantinedView>,
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
    let quarantined = match database.unresolved_executions().await {
        Ok(unresolved) => unresolved
            .into_iter()
            .map(|(job, execution)| QuarantinedView {
                job: job.to_string(),
                pool: execution.pool.map(|pool| pool.to_string()),
                since: execution.unresolved_at.as_unix_micros(),
            })
            .collect::<Vec<_>>(),
        Err(error) => {
            log::warn!("failed to read unresolved executions for health: {error}");
            Vec::new()
        }
    };
    envelope(serde_json::json!(HealthView {
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
            commands_waiting: vitals.commands_waiting,
        },
        process: ProcessView {
            rss_bytes: vitals.rss_bytes,
            peak_rss_bytes: vitals.peak_rss_bytes,
        },
        quarantined,
    }))
}
