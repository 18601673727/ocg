use super::contracts::*;
use super::*;

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "ProviderResponse")]
pub(crate) struct ProviderResponseView {
    request: u32,
    text: String,
    text_offset: u32,
    text_complete: bool,
    requests_tools: bool,
    created_at_us: i64,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "ProviderResponses")]
pub(crate) struct ProviderResponsesView {
    job: String,
    responses: Vec<ProviderResponseView>,
    next_after: Option<u32>,
    next_text_offset: Option<u32>,
    has_more: bool,
}

/// Pages received assistant text, splitting large replies only at UTF-8 boundaries.
pub(super) async fn provider_responses(
    shared: &Shared,
    job: JobId,
    after: Option<RequestNumber>,
    limit: usize,
    text_offset: u32,
) -> Result<Value, Refusal> {
    if shared.database.job(job).await?.is_none() {
        return Err(JOB_NOT_FOUND);
    }
    let read_after = if text_offset > 0 {
        after.and_then(|request| RequestNumber::new(request.get() - 1))
    } else {
        after
    };
    let records = shared
        .database
        .provider_responses(job, read_after, limit, shared.response_bytes)
        .await?;
    provider_response_page(job, after, text_offset, records, shared.response_bytes)
}

fn provider_response_page(
    job: JobId,
    after: Option<RequestNumber>,
    text_offset: u32,
    records: crate::db::redb::ProviderResponsesPage,
    response_bytes: usize,
) -> Result<Value, Refusal> {
    if text_offset > 0 && records.responses.first().map(|response| response.request) != after {
        return Err(INVALID_REQUEST);
    }
    let envelope =
        |responses: Vec<ProviderResponseView>, next_after, next_text_offset, has_more| {
            envelope(serde_json::json!(ProviderResponsesView {
                job: job.to_string(),
                responses,
                next_after,
                next_text_offset,
                has_more,
            }))
        };
    let mut size = envelope(Vec::new(), Some(u32::MAX), Some(u32::MAX), true)
        .to_string()
        .len();
    let mut page = Vec::new();
    let mut more = records.has_more;
    let mut next = None;
    let mut next_offset = 0;
    for record in records.responses {
        let offset = if page.is_empty() {
            text_offset as usize
        } else {
            0
        };
        if offset > record.text.len() || !record.text.is_char_boundary(offset) {
            return Err(INVALID_REQUEST);
        }
        let mut response = ProviderResponseView {
            request: record.request.get(),
            text: String::new(),
            text_offset: offset as u32,
            text_complete: false,
            requests_tools: record.requests_tools,
            created_at_us: record.recorded_at.as_unix_micros(),
        };
        let base = json!(response).to_string().len() + 1;
        let available = response_bytes.saturating_sub(size + base);
        let mut bytes = 0;
        let mut end = offset;
        for character in record.text[offset..].chars() {
            let cost = crate::db::redb::escaped_json_char_cost(character);
            if bytes + cost > available {
                break;
            }
            bytes += cost;
            end += character.len_utf8();
        }
        if size + base > response_bytes || (end == offset && offset < record.text.len()) {
            if page.is_empty() {
                return Err(OUTPUT_TOO_LARGE);
            }
            more = true;
            break;
        }
        response.text = record.text[offset..end].to_owned();
        let text_complete = end == record.text.len();
        response.text_complete = text_complete;
        size += base + bytes;
        next = Some(record.request.get());
        next_offset = if text_complete { 0 } else { end as u32 };
        let partial = !text_complete;
        page.push(response);
        if partial {
            more = true;
            break;
        }
    }
    Ok(envelope(
        page,
        if more { next } else { None },
        if more { Some(next_offset) } else { None },
        more,
    ))
}

pub(super) fn responses_query(query: &str) -> Result<(Option<RequestNumber>, usize, u32), Refusal> {
    let (mut after, mut limit, mut offset) = (None, None, None);
    if !query.is_empty() {
        for pair in query.split('&') {
            let (key, value) = pair.split_once('=').ok_or(INVALID_REQUEST)?;
            match key {
                "after" if after.is_none() => {
                    after = Some(
                        routing::decimal::<u32>(value)
                            .and_then(RequestNumber::new)
                            .ok_or(INVALID_REQUEST)?,
                    );
                }
                "limit" if limit.is_none() => {
                    limit = Some(
                        routing::decimal::<usize>(value)
                            .filter(|limit| (1..=100).contains(limit))
                            .ok_or(INVALID_REQUEST)?,
                    );
                }
                "text_offset" if offset.is_none() => {
                    offset = Some(
                        routing::decimal::<u32>(value)
                            .filter(|offset| *offset > 0)
                            .ok_or(INVALID_REQUEST)?,
                    );
                }
                _ => return Err(INVALID_REQUEST),
            }
        }
    }
    if offset.is_some() && after.is_none() {
        return Err(INVALID_REQUEST);
    }
    Ok((after, limit.unwrap_or(50), offset.unwrap_or(0)))
}
