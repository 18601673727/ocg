//! Ordinary manual external execution: one explicitly named, ordinary,
//! submitted native Generative Job, run once against one operator-approved
//! HTTPS Provider endpoint, through one named trusted loopback HTTP proxy.
//!
//! This is [`ExecutionControl::ManualExternalExecution`] and nothing more. It
//! is not the external trial ([`crate::system::external_trial`] keeps its own
//! control and its own approval), not manual loopback execution (whose
//! [`EndpointPolicy::Loopback`] it does not touch), not general external
//! transport, not Provider discovery, not autonomous scheduling and not
//! recovery. It grants no tools, no Workspace and no child Jobs: the Job must
//! satisfy the shared single-request predicate (a chat Agent without a
//! Workspace grant, the OpenAI Chat Completions protocol, a declared context
//! window), and the exact prepared request must stream and offer no tools.
//!
//! Every identity is named by the operator and none is defaulted: the Job,
//! Pool and Executor, the endpoint, the credential reference, the Model, the
//! trusted proxy and the limits. The endpoint, credential reference and Model
//! are assertions against the Job's frozen configuration, never overrides; a
//! mismatch is refused before anything is reserved. The route is the named
//! proxy only: no direct connection, no ambient proxy, no SOCKS, no fallback.
//!
//! The approval, its checks and the one exchange are shared with the trial;
//! see [`crate::system::external_approval`] and
//! [`crate::system::manual_execution`]. There is one reservation, one checked
//! start and one exchange; an exchange that may have reached the Provider
//! leaves its Allocation held as Uncertain, and nothing here retries,
//! reconciles or releases it.
//!
//! [`EndpointPolicy::Loopback`]: crate::system::executor::execution_runtime::EndpointPolicy::Loopback

use crate::db::DatabaseHandle;
use crate::service::ai::providers::{ApprovedRoute, CancellationToken};
use crate::system::execution_controls::ExecutionControl;
use crate::system::external_approval::{
    ExternalApproval, refuse, require_authority, run_approved_external,
};
use crate::system::manual_execution::{ExecutionReport, Stop, Target};

/// What one invocation names.
pub(crate) struct ExternalJobRequest {
    pub(crate) target: Target,
    pub(crate) approval: ExternalApproval,
}

/// Runs the one execution. Everything before the reservation is a read; the
/// reservation, the checked start and the exchange each happen at most once.
/// `cancellation` is the operator's stop; the approved timeout cancels it
/// too. Neither is a claim about the Provider.
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
