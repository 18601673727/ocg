use crate::db::entities::identity::{JobId, RequestEventId};
use crate::db::entities::{
    AvailabilityStat, Measurements, RequestEligibility, RequestNumber, Timestamp, UsageFinality,
    output_filter,
};
use serde_json::Value;
use std::fmt;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageBody {
    pub(crate) digest: ContentDigest,
    pub(crate) bytes: Vec<u8>,
}

impl MessageBody {
    pub fn new(value: &Value) -> Result<Self, serde_json::Error> {
        let filtered = if value.get("role").and_then(Value::as_str) == Some("assistant")
            || value.get("output").is_some()
        {
            output_filter::value(value)
        } else {
            std::borrow::Cow::Borrowed(value)
        };
        let bytes = serde_json::to_vec(filtered.as_ref())?;
        Ok(Self {
            digest: ContentDigest::of(&bytes),
            bytes,
        })
    }

    pub fn digest(&self) -> ContentDigest {
        self.digest
    }

    pub fn byte_length(&self) -> usize {
        self.bytes.len()
    }
}

#[derive(Clone, Debug)]
pub struct SentRequest {
    pub job: JobId,
    pub request: RequestNumber,
    pub observed_at: Timestamp,
    pub messages: Vec<ContentDigest>,
    pub tools: Option<ContentDigest>,
    pub parameters: ContentDigest,
    pub dropped_turns: Option<u32>,
    pub bodies: Vec<MessageBody>,
    pub(crate) required_messages: Vec<ContentDigest>,
}

#[derive(Clone, Debug)]
pub struct ProviderAnswer {
    pub stat: AvailabilityStat,
    pub measurements: Measurements,
    pub response: MessageBody,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct ProviderRequestRecord {
    pub messages: Vec<Value>,
    pub tools: Option<Value>,
    pub parameters: Value,
    pub dropped_turns: u32,
    pub response: Option<Value>,
}

#[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct ContentDigest([u8; 32]);

impl ContentDigest {
    #[must_use]
    pub fn of(bytes: &[u8]) -> Self {
        let mut context = aws_lc_rs::digest::Context::new(&aws_lc_rs::digest::SHA256);
        context.update(bytes);
        Self::finish(context)
    }

    #[must_use]
    pub fn finish(context: aws_lc_rs::digest::Context) -> Self {
        let digest = context.finish();
        Self(digest.as_ref().try_into().expect("SHA-256 has 32 bytes"))
    }

    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for ContentDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.iter().try_for_each(|byte| write!(f, "{byte:02x}"))
    }
}

impl fmt::Debug for ContentDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ContentDigest({self})")
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SubmissionKey([u8; 32]);

impl SubmissionKey {
    #[must_use]
    pub const fn from_digest(digest: [u8; 32]) -> Self {
        Self(digest)
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for SubmissionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SubmissionKey(")?;
        self.0[..4]
            .iter()
            .try_for_each(|byte| write!(f, "{byte:02x}"))?;
        f.write_str("…)")
    }
}

impl fmt::Display for SubmissionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in &self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct RequestFingerprint([u8; 32]);

impl RequestFingerprint {
    #[must_use]
    pub const fn from_digest(digest: [u8; 32]) -> Self {
        Self(digest)
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for RequestFingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RequestFingerprint(")?;
        self.0[..4]
            .iter()
            .try_for_each(|byte| write!(f, "{byte:02x}"))?;
        f.write_str("…)")
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Submission {
    pub key: SubmissionKey,
    pub fingerprint: RequestFingerprint,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SubmissionRecord {
    pub key: SubmissionKey,
    pub fingerprint: RequestFingerprint,
    pub job: JobId,
    pub seq: u32,
    pub recorded_at: Timestamp,
}

#[derive(Clone, Debug, PartialEq)]
pub enum RequestPayload {
    Sent {
        messages: Vec<ContentDigest>,
        tools: Option<ContentDigest>,
        parameters: ContentDigest,
        dropped_turns: Option<u32>,
    },
    Received {
        digest: ContentDigest,
    },
    Measurement {
        measurements: Measurements,
    },
    Availability {
        status_code: Option<u16>,
        finish_reason: Option<ChatFinishReason>,
        raw_finish_reason: Option<String>,
        failure: Option<ProviderFailure>,
    },
    Token {
        eligibility: RequestEligibility,
        finality: UsageFinality,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct RequestEventRecord {
    pub id: RequestEventId,
    pub job_id: JobId,
    pub request: RequestNumber,
    pub seq: u32,
    pub observed_at: Timestamp,
    pub recorded_at: Timestamp,
    pub payload: RequestPayload,
}

pub(crate) struct ProviderResponse {
    pub request: RequestNumber,
    pub text: Arc<str>,
    pub requests_tools: bool,
    pub recorded_at: Timestamp,
}

pub(crate) struct ProviderResponsesPage {
    pub responses: Vec<ProviderResponse>,
    pub has_more: bool,
}

pub(crate) fn escaped_json_char_cost(character: char) -> usize {
    match character {
        '"' | '\\' | '\n' | '\r' | '\t' | '\u{08}' | '\u{0c}' => 2,
        c if c <= '\u{1f}' => 6,
        c => c.len_utf8(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChatFinishReason {
    Stop,
    Length,
    ToolCalls,
    ContentFilter,
    Error,
    Other,
}

impl ChatFinishReason {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Stop => "stop",
            Self::Length => "length",
            Self::ToolCalls => "tool_calls",
            Self::ContentFilter => "content_filter",
            Self::Error => "error",
            Self::Other => "other",
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct NormalizedUsage {
    pub input_tokens: Option<u32>,
    pub output_tokens: Option<u32>,
    pub cache_read_tokens: Option<u32>,
    pub cache_write_tokens: Option<u32>,
    pub reasoning_tokens: Option<u32>,
    pub raw: Option<Value>,
}

impl NormalizedUsage {
    pub(crate) fn has_counts(&self) -> bool {
        [
            self.input_tokens,
            self.output_tokens,
            self.cache_read_tokens,
            self.cache_write_tokens,
            self.reasoning_tokens,
        ]
        .iter()
        .any(Option::is_some)
    }

    #[must_use]
    pub fn total_tokens(&self) -> Option<u32> {
        match (self.input_tokens, self.output_tokens) {
            (Some(input), Some(output)) => Some(input.saturating_add(output)),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProviderFailure {
    pub message: String,
    pub status_code: Option<u16>,
    pub provider_code: Option<String>,
    pub response_body: Option<String>,
    pub request_id: Option<String>,
    pub retry_after_ms: Option<u64>,
    pub retryable: bool,
}

impl fmt::Display for ProviderFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.status_code {
            Some(status) => write!(f, "HTTP {status}: {}", self.message),
            None => f.write_str(&self.message),
        }
    }
}

impl std::error::Error for ProviderFailure {}
