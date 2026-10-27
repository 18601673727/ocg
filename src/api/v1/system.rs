use super::contracts::*;
use super::*;

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
            .map(|(job, execution)| {
                serde_json::json!({
                    "job": job.to_string(),
                    "pool": execution.pool.map(|pool| pool.to_string()),
                    "since": execution.unresolved_at.as_unix_micros(),
                })
            })
            .collect::<Vec<_>>(),
        Err(error) => {
            log::warn!("failed to read unresolved executions for health: {error}");
            Vec::new()
        }
    };
    envelope(serde_json::json!({
        "host": "running".to_owned(),
        "database": serde_json::json!({
            "status": status.to_owned(),
            "accepting_commands": status == "open",
            "readable": readable,
            "database_bytes": vitals.database_bytes,
            "available_bytes": vitals.available_bytes,
            "durable_commits": vitals.durable_commits,
            "commit_us_mean": vitals.commit_us_mean,
            "commit_us_max": vitals.commit_us_max,
        }),
        "claims": serde_json::json!({
            "count": vitals.claims_count,
            "mean_us": vitals.claim_us_mean,
            "max_us": vitals.claim_us_max,
        }),
        "queue": serde_json::json!({
            "pending": vitals.queue_pending,
            "ready": vitals.queue_ready,
            "delayed": vitals.queue_delayed,
            "commands_waiting": vitals.commands_waiting,
        }),
        "process": serde_json::json!({
            "rss_bytes": vitals.rss_bytes,
            "peak_rss_bytes": vitals.peak_rss_bytes,
        }),
        "quarantined": quarantined,
    }))
}
