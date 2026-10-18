use super::*;

/// What an error before any response head proves about delivery. Only a
/// failure that positively happened before the request could leave is
/// NotDelivered; anything that may have reached a Provider is Delivered,
/// whatever the Job's outcome becomes.
pub(crate) fn unannounced_fact(error: &ProviderExecutionError) -> ProviderFact {
    match error {
        ProviderExecutionError::Build { .. } | ProviderExecutionError::NotDelivered { .. } => {
            ProviderFact::NotDelivered
        }
        ProviderExecutionError::Unknown { .. }
        | ProviderExecutionError::Rejected { .. }
        | ProviderExecutionError::Incomplete { .. }
        | ProviderExecutionError::Interrupted { .. }
        | ProviderExecutionError::Stopped { .. } => ProviderFact::Delivered,
        ProviderExecutionError::OutputPersistence { stream, .. } => unannounced_fact(stream),
    }
}

/// Hands one fact to the sink with the current time.
pub(crate) async fn announce(sink: Option<&mut (dyn ProviderOutputSink + '_)>, fact: ProviderFact) {
    announce_at(sink, Timestamp::now(), fact).await;
}

/// Hands one fact to the sink with the time the producer learned it. The time
/// is fixed by the caller before this awaits, so a slow sink cannot move it.
pub(super) async fn announce_at(
    sink: Option<&mut (dyn ProviderOutputSink + '_)>,
    observed_at: Timestamp,
    fact: ProviderFact,
) {
    if let Some(sink) = sink {
        sink.observe(ProviderObservation { observed_at, fact })
            .await;
    }
}

pub(super) async fn bounded_body(response: ClientResponse, limit: usize) -> Option<String> {
    response
        .body()
        .limit(limit)
        .await
        .ok()
        .and_then(|body| String::from_utf8(body.to_vec()).ok())
}

/// What OCG keeps of a non-200 response to `conversation`. A Provider or
/// gateway may echo the request it refuses, and an image-bearing request
/// carries whole images, so of a refusal of one only the HTTP status, and
/// the retryability it implies, is kept: never its body, nor the message or
/// code read from it, nor its request id header. Nothing is redacted; all
/// of it is dropped. A text-only refusal is kept as the Provider gave it.
pub(crate) fn rejected_failure(
    conversation: &ChatConversation,
    status_code: u16,
    request_id: Option<String>,
    response_body: Option<String>,
) -> ProviderFailure {
    if !conversation.carries_images() {
        return provider_failure(status_code, request_id, response_body);
    }
    ProviderFailure {
        message: format!("provider rejected image request (HTTP {status_code})"),
        status_code: Some(status_code),
        retryable: retryable_status(status_code),
        ..ProviderFailure::default()
    }
}

/// Whether a refusal with this status may succeed when sent again.
fn retryable_status(status_code: u16) -> bool {
    matches!(status_code, 408 | 409 | 425 | 429 | 500..=599)
}

fn provider_failure(
    status_code: u16,
    request_id: Option<String>,
    response_body: Option<String>,
) -> ProviderFailure {
    let (message, provider_code) = response_body
        .as_deref()
        .and_then(parse_provider_error)
        .unwrap_or_else(|| (format!("provider returned HTTP {status_code}"), None));
    ProviderFailure {
        message,
        status_code: Some(status_code),
        provider_code,
        response_body,
        request_id,
        retry_after_ms: None,
        retryable: retryable_status(status_code),
    }
}

fn parse_provider_error(body: &str) -> Option<(String, Option<String>)> {
    let value: Value = serde_json::from_str(body).ok()?;
    let error = value.get("error")?;
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("provider request failed")
        .to_owned();
    let code = error.get("code").and_then(Value::as_str).map(str::to_owned);
    Some((message, code))
}

/// A failure before any response head: it observed no response fact.
pub(super) fn classify_client_error(error: ClientError) -> ProviderExecutionError {
    let message = error.to_string();
    match error {
        ClientError::Url(_) | ClientError::Connect(_) => {
            ProviderExecutionError::NotDelivered { message }
        }
        ClientError::Request(_) => ProviderExecutionError::NotDelivered { message },
        ClientError::Send(_)
        | ClientError::Timeout
        | ClientError::Response(_)
        | ClientError::Http(_)
        | ClientError::H2(_)
        | ClientError::TunnelNotSupported
        | ClientError::Error(_) => ProviderExecutionError::Unknown {
            message,
            facts: None,
            finished: false,
        },
    }
}

pub(super) fn trim_ascii(value: &[u8]) -> &[u8] {
    let start = value
        .iter()
        .position(|byte| !byte.is_ascii_whitespace())
        .unwrap_or(value.len());
    let end = value
        .iter()
        .rposition(|byte| !byte.is_ascii_whitespace())
        .map_or(start, |index| index + 1);
    &value[start..end]
}

/// The timing of a finished request, and nothing else. Its usage is in the
/// result and in the annotated Metrics records captured while it streamed.
pub fn metrics(result: &ProviderExecutionResult) -> crate::persistence::Measurements {
    crate::persistence::Measurements {
        ttfb_ms: Some(result.ttfb.as_millis().min(u128::from(u64::MAX)) as u64),
        request_finished_in_ms: Some(
            result.finished_in.as_millis().min(u128::from(u64::MAX)) as u64
        ),
        ..crate::persistence::Measurements::default()
    }
}

/// The availability observations an error proves, in the order they were
/// observed; at most two. Only what the provider or transport actually
/// reported becomes a failure: a refusal before delivery proves nothing, and
/// a local stop keeps just the response facts it already had. A failure keeps
/// the response facts observed before it. When the response had finished
/// before an independent transport failure, its facts and the failure are
/// two observations, so the failure is not recorded as part of the finish.
/// Nothing is inferred from an unknown outcome.
pub fn error_observations(
    error: &ProviderExecutionError,
) -> Vec<crate::persistence::AvailabilityObservation> {
    use crate::persistence::AvailabilityObservation;
    let observed =
        |facts: &ResponseFacts, failure: Option<ProviderFailure>| AvailabilityObservation {
            status_code: Some(facts.status_code),
            finish_reason: facts.finish_reason,
            raw_finish_reason: facts.raw_finish_reason.clone(),
            failure,
        };
    let failure = || ProviderFailure {
        message: error.to_string(),
        ..ProviderFailure::default()
    };
    // A failure without any response fact beside it.
    let failure_only = || AvailabilityObservation {
        status_code: None,
        finish_reason: None,
        raw_finish_reason: None,
        failure: Some(failure()),
    };
    match error {
        // A local interruption before any response head observed nothing
        // about the Provider.
        ProviderExecutionError::Build { .. }
        | ProviderExecutionError::NotDelivered { .. }
        | ProviderExecutionError::Interrupted { .. } => Vec::new(),
        ProviderExecutionError::Stopped { facts, .. } => vec![observed(facts, None)],
        ProviderExecutionError::Rejected { failure } => vec![AvailabilityObservation {
            status_code: failure.status_code,
            finish_reason: None,
            raw_finish_reason: None,
            failure: Some(failure.clone()),
        }],
        ProviderExecutionError::Unknown { facts: None, .. } => vec![failure_only()],
        ProviderExecutionError::Unknown {
            facts: Some(facts),
            finished: true,
            ..
        } => vec![observed(facts, None), failure_only()],
        ProviderExecutionError::Unknown {
            facts: Some(facts),
            finished: false,
            ..
        }
        | ProviderExecutionError::Incomplete { facts, .. } => {
            vec![observed(facts, Some(failure()))]
        }
        ProviderExecutionError::OutputPersistence { stream, .. } => error_observations(stream),
    }
}

pub fn success_observation(
    result: &ProviderExecutionResult,
) -> crate::persistence::AvailabilityObservation {
    crate::persistence::AvailabilityObservation {
        status_code: Some(result.status_code),
        finish_reason: result.finish_reason,
        raw_finish_reason: result.raw_finish_reason.clone(),
        failure: None,
    }
}

pub fn result_value(result: &ProviderExecutionResult) -> Value {
    let mut value = serde_json::Map::new();
    value.insert("text".to_owned(), Value::String(result.text.clone()));
    if let Some(reason) = result.raw_finish_reason.as_deref() {
        value.insert("finish_reason".to_owned(), Value::String(reason.to_owned()));
    }
    Value::Object(value)
}

pub(super) fn with_retry_after(
    mut failure: ProviderFailure,
    retry_after_ms: Option<u64>,
) -> ProviderFailure {
    failure.retry_after_ms = retry_after_ms;
    failure
}

pub(crate) fn retry_after_ms(value: &str) -> Option<u64> {
    let value = value.trim();
    if !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Some(
            value
                .parse::<u64>()
                .unwrap_or(u64::MAX)
                .saturating_mul(1_000),
        );
    }
    let parts: Vec<_> = value
        .split(|character: char| {
            character.is_ascii_whitespace() || matches!(character, ',' | '-' | ':')
        })
        .filter(|part| !part.is_empty())
        .collect();
    let (day, month, year, hour, minute, second) = match parts.as_slice() {
        [_, day, month, year, hour, minute, second, "GMT"] => {
            (*day, *month, *year, *hour, *minute, *second)
        }
        [_, month, day, hour, minute, second, year] => {
            (*day, *month, *year, *hour, *minute, *second)
        }
        _ => return None,
    };
    let month = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ]
    .iter()
    .position(|name| *name == month)?
        + 1;
    let mut parsed_year: i32 = year.parse().ok()?;
    if year.len() == 2 {
        let current = time::OffsetDateTime::from_unix_timestamp(
            Timestamp::now().as_unix_micros() / 1_000_000,
        )
        .ok()?
        .year();
        parsed_year += current / 100 * 100;
        if parsed_year > current + 50 {
            parsed_year -= 100;
        }
    }
    let date = time::Date::from_calendar_date(
        parsed_year,
        time::Month::try_from(month as u8).ok()?,
        day.parse().ok()?,
    )
    .ok()?;
    let deadline = date
        .with_hms(
            hour.parse().ok()?,
            minute.parse().ok()?,
            second.parse().ok()?,
        )
        .ok()?
        .assume_utc()
        .unix_timestamp_nanos()
        / 1_000_000;
    let now = i128::from(Timestamp::now().as_unix_micros()) / 1_000;
    Some(u64::try_from((deadline - now).max(0)).unwrap_or(u64::MAX))
}
