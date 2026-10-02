//! Anthropic Messages error normalization.
//!
//! Anthropic reports failures two ways — a non-2xx HTTP response carrying an
//! `error` envelope, and an `error` event inside an otherwise healthy stream.
//! Both are normalized into OCG's existing [`ProviderFailure`], which is what
//! the canonical provider failure path already understands, so no Anthropic
//! error type reaches the Call lifecycle or the Chat surface.

use serde_json::Value;

use crate::http::MAX_PROVIDER_ERROR_BODY;
use crate::openai_compatible::error::ProviderFailure;

/// Largest error message carried out of a provider body, in bytes.
const MAX_MESSAGE_BYTES: usize = 1024;

/// Normalize an Anthropic error envelope, from either an HTTP error body or an
/// in-stream `error` event.
///
/// `value` is the whole envelope (`{"type":"error","error":{…}}`) when one was
/// present. Whatever shape arrives, the observable facts survive: the HTTP
/// status, the provider's own error type, and a bounded message. The body is
/// not interpolated into the message, only quoted as a bounded excerpt, and it
/// is never treated as a success.
#[must_use]
pub fn anthropic_failure(value: Option<&Value>, status: Option<u16>) -> ProviderFailure {
    // The wire wraps the failure in an `error` object; a body that reports the
    // failure at the top level is read as-is rather than dropped.
    let error = value
        .and_then(|value| value.get("error"))
        .or(value.filter(|value| value.get("message").is_some()))
        .unwrap_or(&Value::Null);
    let provider_code = error
        .get("type")
        .and_then(Value::as_str)
        .map(str::to_string);
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .map(bounded)
        .filter(|message| !message.is_empty())
        .unwrap_or_else(|| default_message(status));
    let response_body = value.map(|value| bounded_body(&value.to_string()));
    ProviderFailure {
        message,
        status_code: status,
        // Anthropic marks the conditions that are worth retrying with a distinct
        // `type`; OCG records the fact and owns the decision to act on it.
        retryable: provider_code.as_deref().is_some_and(|code| {
            matches!(code, "overloaded_error" | "api_error" | "rate_limit_error")
        }),
        provider_code,
        response_body,
        request_id: None,
        retry_after_ms: None,
    }
}

/// Normalize a non-2xx Anthropic response body into a provider failure.
///
/// The body has already been bounded and redacted by the caller, so a
/// credential can never be quoted back out of the failure this produces. A body
/// that is not an Anthropic envelope is still reported with its status and a
/// bounded excerpt: an unparseable failure is not a successful round.
#[must_use]
pub fn anthropic_failure_from_body(status: u16, body: &[u8]) -> ProviderFailure {
    let parsed: Option<Value> = serde_json::from_slice(body).ok();
    let mut failure = anthropic_failure(parsed.as_ref(), Some(status));
    if parsed.is_none() {
        let excerpt = bounded(&String::from_utf8_lossy(body));
        failure.response_body = Some(excerpt.clone());
        failure.message = excerpt;
    }
    failure
}

/// The operator-readable rendering of a normalized Anthropic failure.
///
/// [`TransportError`]'s own rendering states the status and the message but
/// drops the provider's error type. That type is the part which tells an
/// operator whether the request was malformed, the key was rejected, or the
/// provider was overloaded, and the Call failure this text lands in is the only
/// place it is read — so it is carried here rather than lost.
#[must_use]
pub fn describe(failure: &ProviderFailure) -> String {
    let described = match failure.status_code {
        Some(status) => format!("HTTP {status}: {}", failure.message),
        None => failure.message.clone(),
    };
    match failure.provider_code.as_deref() {
        Some(code) => format!("{described} [{code}]"),
        None => described,
    }
}

fn default_message(status: Option<u16>) -> String {
    match status {
        Some(status) => format!("Anthropic provider returned HTTP {status}"),
        None => "Anthropic provider reported an error".to_string(),
    }
}

fn bounded(message: &str) -> String {
    let mut end = message.len().min(MAX_MESSAGE_BYTES);
    while end > 0 && !message.is_char_boundary(end) {
        end -= 1;
    }
    message[..end].to_string()
}

/// A bounded excerpt of a response body, for a failure the operator reads.
fn bounded_body(body: &str) -> String {
    let mut end = body.len().min(MAX_PROVIDER_ERROR_BODY);
    while end > 0 && !body.is_char_boundary(end) {
        end -= 1;
    }
    body[..end].to_string()
}
