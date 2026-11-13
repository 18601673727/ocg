use crate::db::DatabaseHandle;
use crate::service::ai::providers::{ApprovedRoute, CancellationToken};
use crate::system::execution_controls::ExecutionControl;
use crate::system::external_approval::{
    ExternalApproval, refuse, require_authority, run_approved_external,
};
use crate::system::manual_execution::{ExecutionReport, Stop, Target};

pub(crate) struct ExternalJobRequest {
    pub(crate) target: Target,
    pub(crate) approval: ExternalApproval,
}

pub(crate) async fn run_external_job(
    database: &DatabaseHandle,
    request: ExternalJobRequest,
    cancellation: CancellationToken,
) -> Result<ExecutionReport, Stop> {
    if !ExecutionControl::ManualExternalExecution.is_enabled() {
        return Err(refuse(
            "control_disabled",
            ExecutionControl::ManualExternalExecution.refusal(),
        )
        .into());
    }
    require_authority(&request.approval, ExecutionControl::ManualExternalExecution)?;
    match request.approval.route() {
        ApprovedRoute::TrustedProxy(_) => {}
        ApprovedRoute::Direct => {
            return Err(refuse(
                "proxy_required",
                "manual external execution connects only through a named trusted HTTP proxy",
            )
            .into());
        }
    }
    run_approved_external(database, request.target, &request.approval, cancellation).await
}
