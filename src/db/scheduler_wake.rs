use super::*;

pub(super) struct SchedulerWake {
    permit_tx: flume::Sender<()>,
}

pub struct SchedulerWakeSubscription {
    permit_rx: flume::Receiver<()>,
}

impl SchedulerWake {
    pub(super) fn new() -> (Self, SchedulerWakeSubscription) {
        let (permit_tx, permit_rx) = flume::bounded(1);
        (Self { permit_tx }, SchedulerWakeSubscription { permit_rx })
    }

    pub(super) fn publish(&self, dirty: bool, capacity: bool) {
        if !dirty && !capacity {
            return;
        }
        match self.permit_tx.try_send(()) {
            Ok(()) | Err(flume::TrySendError::Full(())) => {}
            Err(flume::TrySendError::Disconnected(())) => {}
        }
    }
}

impl SchedulerWakeSubscription {
    pub async fn recv_async(&self) -> Result<(), flume::RecvError> {
        self.permit_rx.recv_async().await
    }

    pub fn try_recv(&self) -> Result<(), flume::TryRecvError> {
        self.permit_rx.try_recv()
    }
}

pub(super) fn affects_scheduling(kind: OperationKind) -> bool {
    match kind {
        OperationKind::CreateExecutionPool
        | OperationKind::SetExecutionPoolEnabled
        | OperationKind::SetExecutionPoolProfile
        | OperationKind::CreateJobWithInput
        | OperationKind::StopTurn
        | OperationKind::Close
        | OperationKind::Hold
        | OperationKind::Unhold
        | OperationKind::Retry
        | OperationKind::DecideApproval
        | OperationKind::ApplyJobTool
        | OperationKind::AssignJobExecutor
        | OperationKind::FinalizeJob
        | OperationKind::ConfirmJobStopped
        | OperationKind::QuarantineAbandonedJob
        | OperationKind::CreateExecutor
        // Configurations are frozen for submitted Jobs. These low-frequency
        // writes conservatively dirty the configuration available to admission.
        | OperationKind::CreateLlmProvider
        | OperationKind::CreateProfile
        | OperationKind::UpdateProfile
        | OperationKind::CreateAgent
        // Tool effects gate confirmed finalization and repeatable retries.
        | OperationKind::RecordToolOutcome
        | OperationKind::ReconcileToolEffect => true,
        OperationKind::BoundaryB
        | OperationKind::Open
        | OperationKind::Append
        | OperationKind::ClaimExecution
        | OperationKind::SaveJobInput
        | OperationKind::CreateProject
        | OperationKind::UpdateProject
        | OperationKind::SetConversation
        | OperationKind::AppendJobOutput
        | OperationKind::RecordMetrics
        | OperationKind::RecordAvailabilityStat
        | OperationKind::RecordSentRequest
        | OperationKind::RecordProviderAnswer
        | OperationKind::ConsumeAvailability
        | OperationKind::ConsumeTokenUsage
        // Decisions only add evidence while the owning Job executes locally;
        // outcome/finalization/stop changes announce subsequent eligibility.
        | OperationKind::RecordToolDecision
        | OperationKind::CreateAttachment
        | OperationKind::CleanupAttachment => false,
    }
}

pub(super) fn releases_capacity(kind: OperationKind) -> bool {
    match kind {
        OperationKind::FinalizeJob
        | OperationKind::ConfirmJobStopped
        | OperationKind::CreateExecutionPool
        | OperationKind::SetExecutionPoolEnabled
        | OperationKind::SetExecutionPoolProfile
        | OperationKind::UpdateProfile => true,
        // Quarantining retains the abandoned Job's occupancy rather than
        // freeing it, so no slot is released and a capacity scan on the other
        // side would only be told to re-read what it already knows.
        OperationKind::QuarantineAbandonedJob
        | OperationKind::ClaimExecution
        | OperationKind::SaveJobInput
        | OperationKind::CreateJobWithInput
        | OperationKind::Open
        | OperationKind::Append
        | OperationKind::StopTurn
        | OperationKind::Close
        | OperationKind::Hold
        | OperationKind::Unhold
        | OperationKind::Retry
        | OperationKind::DecideApproval
        | OperationKind::ApplyJobTool
        | OperationKind::BoundaryB
        | OperationKind::AssignJobExecutor
        | OperationKind::CreateExecutor
        | OperationKind::CreateLlmProvider
        | OperationKind::CreateProfile
        | OperationKind::CreateAgent
        | OperationKind::RecordToolOutcome
        | OperationKind::ReconcileToolEffect
        | OperationKind::CreateProject
        | OperationKind::UpdateProject
        | OperationKind::SetConversation
        | OperationKind::AppendJobOutput
        | OperationKind::RecordMetrics
        | OperationKind::RecordAvailabilityStat
        | OperationKind::RecordSentRequest
        | OperationKind::RecordProviderAnswer
        | OperationKind::ConsumeAvailability
        | OperationKind::ConsumeTokenUsage
        | OperationKind::RecordToolDecision
        | OperationKind::CreateAttachment
        | OperationKind::CleanupAttachment => false,
    }
}
