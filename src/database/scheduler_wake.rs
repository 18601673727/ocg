use super::*;

pub(super) struct SchedulerWake {
    dirty_gen: Arc<AtomicU64>,
    capacity_gen: Arc<AtomicU64>,
    permit_tx: flume::Sender<()>,
}

pub struct SchedulerWakeSubscription {
    dirty_gen: Arc<AtomicU64>,
    capacity_gen: Arc<AtomicU64>,
    permit_rx: flume::Receiver<()>,
}

impl SchedulerWake {
    pub(super) fn new() -> (Self, SchedulerWakeSubscription) {
        let dirty_gen = Arc::new(AtomicU64::new(0));
        let capacity_gen = Arc::new(AtomicU64::new(0));
        let (permit_tx, permit_rx) = flume::bounded(1);
        (
            Self {
                dirty_gen: dirty_gen.clone(),
                capacity_gen: capacity_gen.clone(),
                permit_tx,
            },
            SchedulerWakeSubscription {
                dirty_gen,
                capacity_gen,
                permit_rx,
            },
        )
    }

    pub(super) fn publish(&self, dirty: bool, capacity: bool) {
        // The generation must advance even when a permit already exists or
        // the consumer has gone. SeqCst gives generation snapshots one order
        // across a scan; database visibility still comes from its read locks.
        if dirty {
            self.dirty_gen.fetch_add(1, Ordering::SeqCst);
        }
        if capacity {
            self.capacity_gen.fetch_add(1, Ordering::SeqCst);
        }
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
    pub fn capacity_generation(&self) -> u64 {
        self.capacity_gen.load(Ordering::SeqCst)
    }
    pub fn dirty_generation(&self) -> u64 {
        self.dirty_gen.load(Ordering::SeqCst)
    }

    // A permit is only a hint to read authoritative state. The consumer must
    // scan initially and periodically even without a permit, and compare
    // generations across a scan before sleeping. Equality is modulo 2^64.
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
        | OperationKind::RetryJob
        | OperationKind::AssignJobExecutor
        // Starts consume capacity; they do not announce a released slot.
        | OperationKind::StartJob
        | OperationKind::StartLocalJob
        // Combined creation/start still changes the parent and execution set.
        | OperationKind::BeginCoreLogCleanup
        | OperationKind::FinalizeJob
        | OperationKind::ConfirmJobStopped
        | OperationKind::ReconcileAbandonedJob
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
        OperationKind::ClaimExecution
        | OperationKind::SaveJobInput
        | OperationKind::AssociateJobProject
        | OperationKind::CreateProject
        | OperationKind::AppendJobOutput
        | OperationKind::RecordMetrics
        | OperationKind::RecordAvailabilityStat
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
        | OperationKind::ReconcileAbandonedJob
        | OperationKind::CreateExecutionPool
        | OperationKind::SetExecutionPoolEnabled
        | OperationKind::SetExecutionPoolProfile
        | OperationKind::UpdateProfile => true,
        OperationKind::ClaimExecution
        | OperationKind::SaveJobInput
        | OperationKind::CreateJobWithInput
        | OperationKind::RetryJob
        | OperationKind::AssignJobExecutor
        | OperationKind::StartJob
        | OperationKind::StartLocalJob
        | OperationKind::BeginCoreLogCleanup
        | OperationKind::CreateExecutor
        | OperationKind::CreateLlmProvider
        | OperationKind::CreateProfile
        | OperationKind::CreateAgent
        | OperationKind::RecordToolOutcome
        | OperationKind::ReconcileToolEffect
        | OperationKind::AssociateJobProject
        | OperationKind::CreateProject
        | OperationKind::AppendJobOutput
        | OperationKind::RecordMetrics
        | OperationKind::RecordAvailabilityStat
        | OperationKind::ConsumeAvailability
        | OperationKind::ConsumeTokenUsage
        | OperationKind::RecordToolDecision
        | OperationKind::CreateAttachment
        | OperationKind::CleanupAttachment => false,
    }
}
