use super::contracts::*;
use super::*;

pub(super) const MAX_EXPOSED_TOOL_STRING_BYTES: usize = 2 * 1024;

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "ToolCall")]
pub(crate) struct ToolCallView {
    tool_call_id: String,
    request: u32,
    index: u32,
    name: String,
    decision: String,
    outcome_kind: Option<ToolOutcomeKind>,
    arguments: Option<String>,
    arguments_cut: bool,
    result: Option<String>,
    result_cut: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    duration_ms: Option<u64>,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "Tools")]
pub(crate) struct ToolsView {
    job: String,
    after: Option<String>,
    tools: Vec<ToolCallView>,
    next_after: Option<String>,
    has_more: bool,
}

pub(super) fn cut_tool_string(text: &str) -> (&str, bool) {
    if text.len() <= MAX_EXPOSED_TOOL_STRING_BYTES {
        (text, false)
    } else {
        let mut boundary = MAX_EXPOSED_TOOL_STRING_BYTES;
        while !text.is_char_boundary(boundary) {
            boundary -= 1;
        }
        (&text[..boundary], true)
    }
}

pub(super) fn parse_tool_cursor(text: &str) -> Result<(RequestNumber, u32), Refusal> {
    if let Some((req_str, idx_str)) = text.split_once(':') {
        let req = routing::decimal::<u32>(req_str)
            .and_then(RequestNumber::new)
            .ok_or(INVALID_REQUEST)?;
        let idx = routing::decimal::<u32>(idx_str).ok_or(INVALID_REQUEST)?;
        return Ok((req, idx));
    }
    Err(INVALID_REQUEST)
}

pub(super) fn tools_query(query: &str) -> Result<(Option<(RequestNumber, u32)>, usize), Refusal> {
    let (mut after, mut limit) = (None, None);
    if !query.is_empty() {
        for pair in query.split('&') {
            let (key, value) = pair.split_once('=').ok_or(INVALID_REQUEST)?;
            match key {
                "after" if after.is_none() => {
                    after = Some(parse_tool_cursor(value)?);
                }
                "limit" if limit.is_none() => {
                    limit = Some(
                        routing::decimal::<usize>(value)
                            .filter(|limit| (1..=100).contains(limit))
                            .ok_or(INVALID_REQUEST)?,
                    );
                }
                _ => return Err(INVALID_REQUEST),
            }
        }
    }
    Ok((after, limit.unwrap_or(50)))
}

fn tool_view(record: &ToolEvidenceRecord) -> ToolCallView {
    tool_view_with_limit(record, MAX_EXPOSED_TOOL_STRING_BYTES)
}

fn tool_view_with_limit(record: &ToolEvidenceRecord, limit: usize) -> ToolCallView {
    let cut = |text: &str| {
        if limit == MAX_EXPOSED_TOOL_STRING_BYTES {
            let (text, cut) = cut_tool_string(text);
            return (text.to_owned(), cut);
        }
        let mut end = text.len().min(limit);
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        (text[..end].to_owned(), end < text.len())
    };
    let decision = match &record.decision.fact {
        ToolDecision::Admitted(_) => "admitted",
        ToolDecision::AwaitingApproval(_) => "awaiting_approval",
        ToolDecision::AwaitingAnswer(_) => "awaiting_answer",
        ToolDecision::Refused(refusal) => refusal.as_str(),
    };
    let (arguments, arguments_cut) = match record.decision.fact.stored_admission() {
        Some(admission) => {
            let (cut, truncated) = cut(admission.arguments.as_str());
            (Some(cut), truncated)
        }
        None => (None, false),
    };
    let raw_result = match &record.outcome {
        Some(outcome) => match &outcome.fact {
            ToolOutcome::Completed { result } => Some(result.as_str()),
            ToolOutcome::CompletedWithheld { reason } => match reason {
                WithheldReason::Unserializable => Some("result withheld: unserializable"),
            },
            ToolOutcome::NotApplied { message } => Some(message.as_str()),
            ToolOutcome::NotInvoked { reason } => match reason {
                NotInvokedReason::Refused(r) => Some(r.as_str()),
                NotInvokedReason::Cancelled => Some("cancelled before invocation"),
            },
            ToolOutcome::Uncertain { message, cause } => message.as_deref().or(match cause {
                UncertainCause::TimedOut => Some("timed out"),
                UncertainCause::Cancelled => Some("cancelled"),
                UncertainCause::HandlerReported => Some("handler reported failure"),
            }),
        },
        None => None,
    };
    let (result, result_cut) = match raw_result {
        Some(text) => {
            let (cut, truncated) = cut(text);
            (Some(cut), truncated)
        }
        None => (None, false),
    };
    ToolCallView {
        tool_call_id: record.tool_call_id.clone(),
        request: record.call.request.get(),
        index: record.call.index,
        name: record.name.clone(),
        decision: decision.to_owned(),
        outcome_kind: record.outcome.as_ref().map(|outcome| outcome.fact.kind()),
        arguments,
        arguments_cut,
        result,
        result_cut,
        duration_ms: record.outcome.as_ref().and_then(|outcome| {
            outcome
                .at
                .as_unix_micros()
                .checked_sub(record.decision.at.as_unix_micros())
                .and_then(|micros| u64::try_from(micros).ok())
                .map(|micros| micros / 1000)
        }),
    }
}

pub(super) async fn tool(
    shared: &Shared,
    job: JobId,
    request: RequestNumber,
    index: u32,
) -> Result<Reply, Refusal> {
    if shared.database.job(job).await?.is_none() {
        return Err(JOB_NOT_FOUND);
    }
    let after = index
        .checked_sub(1)
        .map(|index| (request, index))
        .or_else(|| RequestNumber::new(request.get() - 1).map(|previous| (previous, u32::MAX)));
    let records = shared.database.tool_evidence(job, after, 1).await?;
    let record = records
        .first()
        .filter(|record| record.call.request == request && record.call.index == index)
        .ok_or(NOT_FOUND)?;
    dashboard::bounded(
        shared,
        envelope(serde_json::json!(ToolsView {
            job: job.to_string(),
            after: None,
            tools: vec![tool_view_with_limit(record, usize::MAX)],
            next_after: None,
            has_more: false,
        })),
    )
}

pub(super) async fn tools(
    shared: &Shared,
    job: JobId,
    after: Option<(RequestNumber, u32)>,
    limit: usize,
) -> Result<Value, Refusal> {
    let database = &shared.database;
    if database.job(job).await?.is_none() {
        return Err(JOB_NOT_FOUND);
    }
    let records = database.tool_evidence(job, after, limit + 1).await?;
    let after_cursor = after.map(|(req, idx)| format!("{}:{}", req.get(), idx));
    let envelope = |page: Vec<ToolCallView>, next: Option<(RequestNumber, u32)>, more: bool| {
        envelope(serde_json::json!(ToolsView {
            job: job.to_string(),
            after: after_cursor.clone(),
            tools: page,
            next_after: next.map(|(req, idx)| format!("{}:{}", req.get(), idx)),
            has_more: more,
        }))
    };
    let mut size = envelope(Vec::new(), None, true).to_string().len();
    let mut page = Vec::new();
    let mut next = None;
    let mut more = records.len() > limit;
    for record in records.iter().take(limit) {
        let tool = tool_view(record);
        let cost = json!(tool).to_string().len() + 1;
        if size + cost > shared.response_bytes {
            if page.is_empty() {
                return Err(OUTPUT_TOO_LARGE);
            }
            more = true;
            break;
        }
        size += cost;
        next = Some((record.call.request, record.call.index));
        page.push(tool);
    }
    if !more {
        next = None;
    }
    Ok(envelope(page, next, more))
}
