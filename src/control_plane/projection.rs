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
    json!({
        "api_version": API_VERSION,
        "host": "running",
        "database": {
            "status": status,
            "accepting_commands": status == "open",
            "readable": readable,
        },
    })
}

pub(super) fn micros(timestamp: Option<crate::persistence::Timestamp>) -> Value {
    timestamp.map_or(Value::Null, |timestamp| json!(timestamp.as_unix_micros()))
}

/// The Job's settlement: the one outcome its record holds, or none yet; a
/// record holding more than one is not described at all.
pub(super) fn settlement_json(record: &JobRecord) -> Result<Value, Refusal> {
    let outcomes = [
        ("completed", record.completed_at),
        ("failed", record.failed_at),
        ("cancelled", record.cancelled_at),
        ("unknown", record.unknown_at),
    ]
    .into_iter()
    .filter_map(|(state, at)| at.map(|at| (state, at)))
    .collect::<Vec<_>>();
    match outcomes.as_slice() {
        [] => Ok(json!({"state": "unsettled", "settled_at_us": null})),
        [(state, at)] => Ok(json!({"state": state, "settled_at_us": at.as_unix_micros()})),
        _ => Err(INCONSISTENT_RECORD),
    }
}

/// The Job's lifecycle as recorded, with its [`settlement_json`].
pub(super) fn job_json(record: &JobRecord) -> Result<Value, Refusal> {
    let settlement = settlement_json(record)?;
    let execution_mode = match record.execution_mode {
        ExecutionMode::Once => "once",
        ExecutionMode::Continuous => "continuous",
        ExecutionMode::Periodic => "periodic",
        ExecutionMode::Container => "container",
    };
    Ok(json!({
        "id": record.id.to_string(),
        "name": record.name,
        "execution_mode": execution_mode,
        "input": record.input.to_string(),
        "assigned_executor": record.assigned_executor.map(|executor| executor.to_string()),
        "created_at_us": record.created_at.as_unix_micros(),
        "updated_at_us": record.updated_at.as_unix_micros(),
        "started_at_us": micros(record.started_at),
        "paused_at_us": micros(record.paused_at),
        "sterilized_at_us": micros(record.sterilized_at),
        "settlement": settlement,
        // Recorded only for an Unknown Job whose local execution was
        // confirmed stopped; it says nothing about the Provider.
        "stop_confirmed_at_us": micros(record.stop_confirmed_at),
    }))
}

pub(super) fn output_json(record: &JobOutputRecord) -> Value {
    let kind = match record.kind {
        OutputKind::Progress => "progress",
        OutputKind::Evaluation => "evaluation",
        OutputKind::Result => "result",
    };
    json!({
        "id": record.id.to_string(),
        "seq": record.seq,
        "kind": kind,
        "summary": record.summary,
        "code": record.code,
        "goal_reached": record.goal_reached,
        "value": record.value,
        "created_at_us": record.created_at.as_unix_micros(),
    })
}

/// A page of the Job's outputs, exactly as stored and in `seq` order: as
/// many of the first `limit` after `after_seq` as fit the response, whole.
/// `next_after_seq` is the cursor of the next page, `after_seq` again when
/// none was returned; `has_more` says whether a later output was stored
/// when the page was read.
pub(super) async fn outputs(
    shared: &Shared,
    job: JobId,
    after_seq: Option<i64>,
    limit: usize,
) -> Result<Value, Refusal> {
    let database = &shared.database;
    if database.job(job).await?.is_none() {
        return Err(JOB_NOT_FOUND);
    }
    let records = database.job_outputs(job, after_seq, limit + 1).await?;
    let envelope = |outputs: Vec<Value>, next: Option<i64>, more: bool| {
        json!({
            "api_version": API_VERSION,
            "job": job.to_string(),
            "after_seq": after_seq,
            "outputs": outputs,
            "next_after_seq": next,
            "has_more": more,
        })
    };
    let mut size = envelope(Vec::new(), Some(i64::MIN), true).to_string().len();
    let mut page = Vec::new();
    let mut next = after_seq;
    let mut more = records.len() > limit;
    for record in records.iter().take(limit) {
        let output = output_json(record);
        let cost = output.to_string().len() + 1;
        if size + cost > shared.response_bytes {
            if page.is_empty() {
                return Err(OUTPUT_TOO_LARGE);
            }
            more = true;
            break;
        }
        size += cost;
        page.push(output);
        next = Some(record.seq);
    }
    Ok(envelope(page, next, more))
}
