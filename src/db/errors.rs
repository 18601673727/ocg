use super::*;

#[derive(Debug, thiserror::Error)]
pub enum DatabaseError {
    #[error("invalid startup policy: {0}")]
    StartupPolicy(#[source] SystemPolicyError),
    #[error("the control queue is full; the space check was not accepted")]
    ControlQueueFull,
    #[error(
        "database command queue is full ({COMMAND_QUEUE_CAPACITY} pending commands); the command was not accepted"
    )]
    QueueFull,
    #[error("the database is shutting down; the command was not accepted")]
    ShuttingDown,
    #[error("disk protection is active; the command was not accepted: {0}")]
    DiskProtected(ProtectionLock),
    #[error("the database stopped after a storage failure that was not recovered: {0}")]
    StorageUnavailable(StorageError),
    #[error("the command was refused and nothing was committed: {0}")]
    Rejected(Violation),
    #[error("storage failed before the commit; nothing was committed: {0}")]
    RolledBack(StorageError),
    #[error(
        "{operation} was accepted but the worker stopped before executing it; nothing was committed"
    )]
    NeverExecuted { operation: OperationId },
    #[error(
        "{operation} was accepted but disk protection activated before it could run; nothing was committed: {lock}"
    )]
    RefusedByDiskProtection {
        operation: OperationId,
        lock: ProtectionLock,
    },
    #[error(
        "{operation} was refused before it ran and nothing was committed: {} has {} bytes available, \
         and the command's {} byte allowance would leave less than {}",
        shortage.path.display(), shortage.available_bytes, shortage.expected_write_bytes, shortage.threshold_bytes
    )]
    InsufficientSpace {
        operation: OperationId,
        shortage: SpaceShortage,
    },
    #[error("the outcome of {operation} is unknown: {cause}")]
    OutcomeUnknown {
        operation: OperationId,
        cause: UnknownCause,
    },
    #[error("{0} was not issued by this database")]
    UnknownOperation(OperationId),
    #[error(
        "database read queue is full ({READ_QUEUE_CAPACITY} pending reads); the read was not started"
    )]
    ReadQueueFull,
    #[error("the database is shutting down or closed; the read was not performed")]
    ReadsClosed,
    #[error(
        "the database stopped after a storage failure and must be reopened; the read was not performed: {0}"
    )]
    ReadsStorageFailed(StorageError),
    #[error("the read worker stopped without answering the read")]
    ReadInterrupted,
    #[error("no database read worker is running; the read was not performed")]
    ReadPoolStopped,
    #[error("failed to read from the database: {0}")]
    Read(StorageError),
    #[error("failed to start the database worker: {0}")]
    Startup(#[source] std::io::Error),
    #[error("invalid filesystem configuration: {0}")]
    Configuration(FilesystemConfigError),
    #[error("startup refused by disk protection: {}", describe_reasons(.0))]
    InsufficientStartupSpace(Vec<ProtectionReason>),
    #[error("failed to open the database: {0}")]
    Open(StorageError),
    #[error("the attachment store cannot be used: {0}")]
    AttachmentStore(StoreError),
    #[error("database worker panicked")]
    WorkerPanicked,
}

#[derive(Debug, thiserror::Error)]
pub enum UnknownCause {
    #[error("the commit failed: {0}")]
    Commit(StorageError),
    #[error("the database worker stopped while executing the command")]
    WorkerStopped,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommandOutcome {
    NotAccepted,
    NotCommitted,
    Unknown,
}

impl DatabaseError {
    pub fn command_outcome(&self) -> Option<CommandOutcome> {
        match self {
            Self::QueueFull | Self::ShuttingDown | Self::DiskProtected(_) => {
                Some(CommandOutcome::NotAccepted)
            }
            Self::Rejected(_)
            | Self::RolledBack(_)
            | Self::NeverExecuted { .. }
            | Self::RefusedByDiskProtection { .. }
            | Self::InsufficientSpace { .. } => Some(CommandOutcome::NotCommitted),
            Self::OutcomeUnknown { .. } => Some(CommandOutcome::Unknown),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OperationStatus {
    Committed,
    NotCommitted,
    NotCommittedYet,
    Unresolvable,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CompactionSkip {
    FileTooSmall {
        file_bytes: u64,
    },
    LittleReclaimable {
        file_bytes: u64,
        reclaimable_bytes: u64,
    },
    CoolingDown {
        remaining: Duration,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompactionReport {
    pub file_bytes_before: u64,
    pub file_bytes_after: u64,
    pub duration: Duration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CompactionOutcome {
    Skipped(CompactionSkip),
    Compacted(CompactionReport),
    NoProgress(CompactionReport),
}

#[derive(Debug, thiserror::Error)]
pub enum MaintenanceError {
    #[error("the control queue is full; the compaction was not accepted")]
    ControlQueueFull,
    #[error("the database is shutting down; the compaction was not accepted")]
    ShuttingDown,
    #[error("the database worker stopped; the compaction did not run")]
    WorkerStopped,
    #[error("disk protection is active; the compaction was not accepted: {0}")]
    DiskProtected(ProtectionLock),
    #[error(
        "compaction was refused: {} has {} bytes available, and the {} byte allowance would leave less than {}",
        .0.path.display(), .0.available_bytes, .0.expected_write_bytes, .0.threshold_bytes
    )]
    InsufficientSpace(SpaceShortage),
    #[error("a transaction was still open on the database; nothing was compacted")]
    TransactionInProgress,
    #[error("a savepoint exists on the database; nothing was compacted")]
    SavepointExists,
    #[error("storage failed during maintenance: {0}")]
    Storage(StorageError),
}

pub(super) fn describe_reasons(reasons: &[ProtectionReason]) -> String {
    reasons
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("; ")
}

pub(super) fn operation_status(
    database: &::redb::Database,
    operation: OperationId,
    probe: Option<Probe>,
    epoch: u64,
) -> Result<OperationStatus, StorageError> {
    let Some(probe) = probe else {
        return Ok(OperationStatus::Unresolvable);
    };
    Ok(if redb::probe_committed(database, probe)? {
        OperationStatus::Committed
    } else if operation.epoch() < epoch {
        OperationStatus::NotCommitted
    } else {
        OperationStatus::NotCommittedYet
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DatabaseStatus {
    Open,
    Protected,
    ShuttingDown,
    WorkerStopped,
    Closed,
}
