pub mod attachments;
pub(crate) mod derived;
pub mod entities;
pub(crate) mod mutations;
pub(crate) mod redb;

use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, PoisonError, RwLock, RwLockWriteGuard};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use ::redb::Database;

use crate::db::attachments::AttachmentStore;
pub use crate::db::attachments::{
    AttachmentCleanupReport, AttachmentContent, AttachmentError, AttachmentImageError, Reclamation,
    ReclamationError, StoreError, attachment_images,
};
use crate::db::entities::{
    Actor, Appended, Entry, ExecutionConfiguration, JobAction, JobGroup, StopProof,
};
use crate::db::entities::{
    AppendedOutput, Attachment, AttachmentId, AttachmentOrigin, AttachmentRecord, AvailabilityStat,
    AvailabilityStatId, AvailabilityStatRecord, CleanupKind, Config, ConfigBody, ConfigId,
    ConfigKind, ConfigRecord, ContentDigest, ExecutionPool, ExecutionPoolConfig, ExecutionPoolId,
    ExecutionPoolRecord, Executor, ExecutorId, Job, JobEdit, JobFinalization, JobId, JobInputId,
    JobOutput, JobOutputRecord, JobRecord, Metrics, MetricsId, MetricsRecord, ObservationCursor,
    ObservationFilter, ObservationPage, OperationId, OperationKind, Project, ProjectCommand,
    ProjectId, ProjectRecord, ProviderAnswer, ProviderRequestRecord, RequestNumber, SentRequest,
    Submission, SubmissionKey, SubmissionRecord, ToolCallRef, ToolEvidence, ToolEvidenceRecord,
    ToolOutcome, ToolReconciliation, Violation,
};
use crate::db::entities::{ClaimGrant, ClaimRefusal, ClaimRequest, FrozenPoolBinding};
use crate::db::mutations::PublishedAttachment;
pub(crate) use crate::db::redb::{Batch, BatchError, Failure};
use crate::db::redb::{BatchFailure, Commit, CompactFailure, OpenedFile, Probe};
pub use crate::db::redb::{StorageError, StorageErrorKind};
use crate::system::data_directory::DataDirectory;
use crate::system::monitor::{
    self, DiskProtectionStatus, FilesystemConfig, FilesystemConfigError, ProtectedArea,
    ProtectionCell, ProtectionLock, ProtectionReason, ProtectionWatch, RecoveryError,
    RecoveryReport, ReopenFailure, SharedRequest, SpaceMeasurement, SpaceShortage,
};
use crate::system::policy::{SystemPolicy, SystemPolicyError};

pub const COMMAND_QUEUE_CAPACITY: usize = 256;
/// The most queued commands one group commit covers. Commands that queue up
/// while the worker is busy form a group without any waiting, so a lone
/// command is never delayed. A group is also the most commands whose outcome
/// a failed durable commit leaves unknown, and the longest stretch readers
/// can be held back, so it stays small.
const GROUP_LIMIT: usize = 32;
/// Compaction policy. `compact` is uninterruptible and holds off every read
/// and command while it runs, so it is only worth running on a large file
/// with a lot of free space, and not again soon after an attempt. These are
/// deliberately conservative and not tuned to a measured workload.
const COMPACTION_MIN_FILE_BYTES: u64 = 64 * 1024 * 1024;
const COMPACTION_MIN_RECLAIMABLE_BYTES: u64 = 32 * 1024 * 1024;
/// The estimated free space must also be at least `1 / this` of the file.
const COMPACTION_MIN_RECLAIMABLE_DIVISOR: u64 = 4;
const COMPACTION_COOLDOWN: Duration = Duration::from_secs(24 * 60 * 60);
/// While admission is locked, the protected filesystems are checked at
/// least this often, so a lock that only lacked space lifts soon after the
/// space returns, however long the configured interval is.
pub(crate) const PROTECTED_CHECK_INTERVAL: Duration = Duration::from_secs(30);

/// How long to wait for the next full check: the configured interval, but
/// no longer than [`PROTECTED_CHECK_INTERVAL`] while admission is locked.
pub(crate) fn next_check_interval(configured: Duration, locked: bool) -> Duration {
    if locked {
        configured.min(PROTECTED_CHECK_INTERVAL)
    } else {
        configured
    }
}
/// Reads run on this many dedicated threads, separate from the write worker.
pub const READ_WORKERS: usize = 2;
pub const READ_QUEUE_CAPACITY: usize = 64;
/// The most index entries one observation query reads, matching or not.
pub const MAX_OBSERVATION_SCAN: usize = redb::MAX_OBSERVATION_SCAN;
/// At most one space check and one recovery are pending at a time; later
/// requests join them.
const CONTROL_QUEUE_CAPACITY: usize = 2;
/// The space a command's transaction is allowed beyond twice its
/// variable-length payload. This is an allowance, not a prediction of redb
/// file growth, which depends on page reuse, B-tree splits and region
/// allocation; real I/O errors are still handled after a passing check.
const TRANSACTION_ALLOWANCE_BYTES: u64 = 1024 * 1024;

mod commands;
mod errors;
mod handle;
mod scheduler_wake;
mod shared;

mod worker;

use commands::{Command, Control, Envelope, Message, Reply};
use errors::operation_status;
pub use errors::{
    CommandOutcome, CompactionOutcome, CompactionReport, CompactionSkip, DatabaseError,
    DatabaseStatus, MaintenanceError, OperationStatus, UnknownCause,
};
pub use handle::Vitals;
pub use scheduler_wake::SchedulerWakeSubscription;
use scheduler_wake::{SchedulerWake, affects_scheduling, releases_capacity};

use shared::{
    Admission, Opened, ReadJob, ReadMessage, ReadWorkerExit, ReleaseOnPanic, Shared,
    StagingBarrier, admitted, flush_staged, serve_reads,
};
pub use worker::{DatabaseHandle, DatabaseWorker};
use worker::{Executed, run, settle};

#[derive(Clone, Copy, Debug)]
pub(crate) enum ObservationChange {
    Metrics(JobId, MetricsId),
    Availability(JobId, AvailabilityStatId),
}
