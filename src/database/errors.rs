use super::*;

/// Command errors fall into three groups, so a caller always knows whether
/// its change can have been applied (see [`DatabaseError::command_outcome`]):
///
/// - not accepted: [`QueueFull`](Self::QueueFull),
///   [`ShuttingDown`](Self::ShuttingDown),
///   [`DiskProtected`](Self::DiskProtected);
/// - accepted and known not to be committed: [`Rejected`](Self::Rejected),
///   [`RolledBack`](Self::RolledBack), [`NeverExecuted`](Self::NeverExecuted),
///   [`RefusedByDiskProtection`](Self::RefusedByDiskProtection),
///   [`InsufficientSpace`](Self::InsufficientSpace);
/// - accepted with an unknown outcome: [`OutcomeUnknown`](Self::OutcomeUnknown).
///   A commit error is not a rollback. Do not retry the command; recover
///   storage with [`DatabaseHandle::recover_storage`], which reconciles it,
///   or call [`DatabaseHandle::reconcile_operation`] after reopening.
/// Read errors are separate and say nothing about any command.
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
    /// Returned by [`DatabaseWorker::shutdown`] when storage had failed and
    /// was not recovered.
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
    /// Space is above the lock threshold, but this command's allowance would
    /// take it below; only this command is refused, without locking.
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
    /// Every read worker has exited unexpectedly. Reads stay refused until
    /// the database is shut down and started again; commands are unaffected.
    #[error("no database read worker is running; the read was not performed")]
    ReadPoolStopped,
    #[error("failed to read from the database: {0}")]
    Read(StorageError),
    #[error("failed to start the database worker: {0}")]
    Startup(#[source] std::io::Error),
    #[error("invalid filesystem configuration: {0}")]
    Configuration(FilesystemConfigError),
    /// A protected filesystem is below the configured lock threshold or its
    /// space could not be measured; the database was not opened.
    #[error("startup refused by disk protection: {}", describe_reasons(.0))]
    InsufficientStartupSpace(Vec<ProtectionReason>),
    #[error("failed to open the database: {0}")]
    Open(StorageError),
    #[error("{0} is not an OCG database")]
    ForeignDatabase(PathBuf),
    #[error(
        "the database uses storage format {found}; this build supports only format {supported}"
    )]
    UnsupportedFormat { found: u32, supported: u32 },
    /// The attachment directory could not be trusted, or its interrupted
    /// publications could not be cleaned up; the database was closed again.
    #[error("the attachment store cannot be used: {0}")]
    AttachmentStore(StoreError),
    #[error("database worker panicked")]
    WorkerPanicked,
}

#[derive(Debug, thiserror::Error)]
pub enum UnknownCause {
    /// The commit failed; its changes may already be durable.
    #[error("the commit failed: {0}")]
    Commit(StorageError),
    /// The worker stopped while executing the command, without answering it.
    #[error("the database worker stopped while executing the command")]
    WorkerStopped,
}

/// Whether a failed command's change can have been applied.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommandOutcome {
    NotAccepted,
    NotCommitted,
    Unknown,
}

impl DatabaseError {
    /// The commit classification of a command error; `None` for errors that
    /// are not command results.
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

/// What the natural key of a command says about it (section 11.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OperationStatus {
    /// The key the command writes is present: it committed.
    Committed,
    /// Issued by an earlier opening and its key is absent: it did not commit,
    /// and the handle that ran it is gone, so it never will.
    NotCommitted,
    /// Issued by this opening and not committed so far.
    NotCommittedYet,
    /// The command writes no key that could be read, so whether it committed
    /// is not known.
    Unresolvable,
}

/// Why [`DatabaseHandle::compact_storage`] left the file alone.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CompactionSkip {
    FileTooSmall {
        file_bytes: u64,
    },
    /// The file's length minus redb's live pages is the most compaction could
    /// return. It is an estimate: fragmentation inside pages is not counted.
    LittleReclaimable {
        file_bytes: u64,
        reclaimable_bytes: u64,
    },
    /// An earlier successful attempt was too recent. Failed attempts do not
    /// start the cooldown.
    CoolingDown {
        remaining: Duration,
    },
}

/// What a compaction attempt did to the file. The sizes are measured after
/// the attempt in both cases, because redb can shrink the file even when it
/// moved no pages.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompactionReport {
    pub file_bytes_before: u64,
    pub file_bytes_after: u64,
    pub duration: Duration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CompactionOutcome {
    Skipped(CompactionSkip),
    /// redb moved pages and every step committed.
    Compacted(CompactionReport),
    /// The attempt completed, but redb found no pages it could move.
    NoProgress(CompactionReport),
}

/// Why [`DatabaseHandle::compact_storage`] failed. Except for
/// [`Storage`](Self::Storage), a failure leaves the database as it was.
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
    /// A storage failure releases the database like a failed command's commit
    /// does, and [`DatabaseHandle::recover_storage`] reopens it.
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
    database: &redb::Database,
    operation: OperationId,
    probe: Option<Probe>,
    epoch: u64,
) -> Result<OperationStatus, StorageError> {
    let Some(probe) = probe else {
        return Ok(OperationStatus::Unresolvable);
    };
    Ok(if storage::probe_committed(database, probe)? {
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
    /// Disk protection refuses commands; [`DatabaseHandle::disk_protection`]
    /// says why. Reads continue unless storage failed, in which case the
    /// database is released until [`DatabaseHandle::recover_storage`]
    /// reopens it.
    Protected,
    ShuttingDown,
    /// The write worker panicked; admission is closed and the database released.
    WorkerStopped,
    Closed,
}
