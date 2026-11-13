use crate::db::DatabaseHandle;
use crate::service::ai::providers::CancellationToken;
use crate::system::execution_controls::ExecutionControl;
use crate::system::external_approval::{
    ExternalApproval, refuse, require_authority, run_approved_external,
};
use crate::system::manual_execution::{ExecutionReport, Stop, Target};

pub(crate) struct TrialRequest {
    pub(crate) target: Target,
    pub(crate) approval: ExternalApproval,
}

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
