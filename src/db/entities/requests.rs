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
    // Only the recorder can exempt references it has already durably stored for this Job.
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

/// The SHA-256 of an Attachment's content. It names the immutable file
/// that holds the content, never the Attachment: several Attachments may
/// share one content file.
#[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct ContentDigest([u8; 32]);

impl ContentDigest {
    /// The SHA-256 of `bytes`.
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

/// Lowercase hexadecimal, 64 characters: the name of the content file.
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

/// What a client's idempotency key is recorded as: a SHA-256 digest of it,
/// computed by its caller. The key itself is never stored, and only the
/// first bytes of the digest are ever displayed.
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

/// A SHA-256 digest of a submission's normalized request, computed by its
/// caller, so a key reused for another request is told apart.
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

/// The idempotency of one submitted Job: recorded in the transaction that
/// creates the Job, and refused there when its key is already recorded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Submission {
    pub key: SubmissionKey,
    pub fingerprint: RequestFingerprint,
}

/// A recorded submission: the one Job its key ever created, and the request
/// that created it. Never changed once committed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SubmissionRecord {
    pub key: SubmissionKey,
    pub fingerprint: RequestFingerprint,
    pub job: JobId,
    pub seq: u32,
    /// When the Job was created with it.
    pub recorded_at: Timestamp,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RequestPayloadKind {
    Measurement = 1,
    Availability = 2,
    Token = 3,
    Sent = 4,
    Received = 5,
    ApprovedCalls = 6,
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

pub type ToolCallRecord = super::approvals::ToolEvidenceRecord;
