//! The manual external-trial boundary: one explicitly invoked, bounded,
//! tool-free Job sent to one operator-approved HTTPS endpoint with one
//! operator-approved credential reference.
//!
//! It is a different capability from its neighbours. It is not manual
//! loopback execution, whose endpoint guard it leaves untouched; it is not
//! ordinary manual external execution ([`crate::system::manual_external`]),
//! which has its own control; and it is not general external transport or
//! autonomous scheduling, which stay disabled. Everything here is refused
//! unless [`ExecutionControl::ManualExternalTrial`] is enabled, a compile-time
//! constant that only reviewed source changes.
//!
//! The approval, its checks and the one exchange are shared with ordinary
//! manual external execution; see [`crate::system::external_approval`]. What
//! is the trial's own is this entry point, its control, and that its approval
//! may connect directly or leave the Model open.

use crate::db::DatabaseHandle;
use crate::service::ai::providers::CancellationToken;
use crate::system::execution_controls::ExecutionControl;
use crate::system::external_approval::{
    ExternalApproval, refuse, require_authority, run_approved_external,
};
use crate::system::manual_execution::{ExecutionReport, Stop, Target};

/// What a trial invocation names.
pub(crate) struct TrialRequest {
    pub(crate) target: Target,
    pub(crate) approval: ExternalApproval,
}

/// Runs the one trial. Everything before the reservation is a read; the
/// reservation, the checked start and the exchange each happen at most once.
/// `cancellation` is the operator's stop; the approved timeout cancels it
/// too. Neither is a claim about the Provider.
pub(crate) async fn run_external_trial(
    database: &DatabaseHandle,
    request: TrialRequest,
    cancellation: CancellationToken,
) -> Result<ExecutionReport, Stop> {
    if !ExecutionControl::ManualExternalTrial.is_enabled() {
        return Err(refuse(
            "control_disabled",
            ExecutionControl::ManualExternalTrial.refusal(),
        )
        .into());
    }
    require_authority(&request.approval, ExecutionControl::ManualExternalTrial)?;
    run_approved_external(database, request.target, &request.approval, cancellation).await
}
