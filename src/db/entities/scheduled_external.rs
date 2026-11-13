use super::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScheduledExternalPolicyRecord {
    pub id: ScheduledExternalPolicyId,
    pub endpoint: String,
    pub credential_ref: String,
    pub model: String,
    pub proxy: String,
    pub max_request_bytes: u32,
    pub max_output_tokens: u32,
    pub timeout_secs: u32,
    pub enabled: bool,
    pub revoked: bool,
    pub revision: u32,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

impl ScheduledExternalPolicyRecord {
    pub const fn is_live(&self) -> bool {
        self.enabled && !self.revoked
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScheduledExternalBinding {
    pub job: JobId,
    pub policy: ScheduledExternalPolicyId,
    pub policy_revision: u32,
    pub allocation_epoch: u64,
    pub allocation_generation: NonZeroU32,
}
