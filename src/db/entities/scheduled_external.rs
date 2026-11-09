use super::*;

/// The durable authorization for one scheduled external transport. It names
/// exactly one HTTPS endpoint, one credential reference, one exact Model, one
/// trusted loopback proxy and the limits of one exchange. It carries no
/// plaintext credential and no mutable Provider configuration: the credential
/// is a reference resolved at handoff, and the Provider settings live only in
/// the Job's frozen configuration.
///
/// A row is created disabled. `enabled` and `revoked` are independent of the
/// build's [`crate::system::execution_controls::ExecutionControl::ScheduledExternalTransport`],
/// which must also be enabled for a row ever to authorize a dispatch. `revision`
/// changes on every authorized mutation; a binding names the revision it was
/// reserved under, so an update or revocation invalidates older bindings.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScheduledExternalPolicyRecord {
    pub id: ScheduledExternalPolicyId,
    /// The canonical request URL, as [`crate::system::external_approval::ApprovedEndpoint`]
    /// names it. Never a mutable Provider endpoint.
    pub endpoint: String,
    pub credential_ref: String,
    pub model: String,
    /// The canonical `address:port` of the trusted loopback proxy.
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
    /// Whether this row may bind a dispatch at all. The build's control is
    /// asked separately; this only reads the stored state.
    pub const fn is_live(&self) -> bool {
        self.enabled && !self.revoked
    }
}

/// The durable link between one Allocation and the policy that authorized it.
/// It stores only the policy's stable identity and the revision that was
/// live when the Allocation was reserved, plus the Allocation generation
/// itself. The policy's mutable facts are never copied here; dispatch and
/// send read the one policy row again.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScheduledExternalBinding {
    pub job: JobId,
    pub policy: ScheduledExternalPolicyId,
    pub policy_revision: u32,
    pub allocation_epoch: u64,
    pub allocation_generation: NonZeroU32,
}
