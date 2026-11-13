use super::*;
use serde_json::Value;

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
        let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, bytes);
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
    ApprovedCalls,
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
