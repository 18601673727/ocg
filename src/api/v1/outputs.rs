use super::contracts::*;
use super::*;

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "Output")]
pub(crate) struct OutputView {
    id: String,
    seq: i64,
    kind: OutputKind,
    summary: Option<String>,
    code: Option<String>,
    goal_reached: Option<bool>,
    value: Option<Value>,
    created_at_us: i64,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "Outputs")]
pub(crate) struct OutputsView {
    job: String,
    after_seq: Option<i64>,
    outputs: Vec<OutputView>,
    next_after_seq: Option<i64>,
    has_more: bool,
}

pub(super) fn output_view(record: &JobOutputRecord) -> OutputView {
    OutputView {
        id: record.id.to_string(),
        seq: record.seq,
        kind: record.kind,
        summary: record.summary.clone(),
        code: record.code.clone(),
        goal_reached: record.goal_reached,
        value: record.value.clone(),
        created_at_us: record.created_at.as_unix_micros(),
    }
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
    let envelope = |outputs: Vec<OutputView>, next: Option<i64>, more: bool| {
        envelope(serde_json::json!(OutputsView {
            job: job.to_string(),
            after_seq,
            outputs,
            next_after_seq: next,
            has_more: more,
        }))
    };
    let mut size = envelope(Vec::new(), Some(i64::MIN), true).to_string().len();
    let mut page = Vec::new();
    let mut next = after_seq;
    let mut more = records.len() > limit;
    for record in records.iter().take(limit) {
        let output = output_view(record);
        let cost = json!(output).to_string().len() + 1;
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

pub(super) async fn latest_output(shared: &Shared, job: JobId) -> Result<Value, Refusal> {
    if shared.database.job(job).await?.is_none() {
        return Err(JOB_NOT_FOUND);
    }
    let output = shared.database.latest_output(job).await?;
    let body = envelope(serde_json::json!(OutputsView {
        job: job.to_string(),
        after_seq: None,
        outputs: output.as_ref().map(output_view).into_iter().collect(),
        next_after_seq: output.as_ref().map(|output| output.seq),
        has_more: false,
    }));
    dashboard::bounded(shared, body).map(|reply| reply.body)
}
