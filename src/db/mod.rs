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
    ToolOutcome, Violation,
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

#[derive(Clone, Copy)]
pub(crate) enum AdmissionCandidate {
    Decision(ExecutorId, entities::ProfileId),
}

#[derive(Clone)]
pub(crate) enum ProviderSettlementEvidence {
    Observations(Vec<AvailabilityStat>),
    Answer(Box<ProviderAnswer>),
}

pub const COMMAND_QUEUE_CAPACITY: usize = 256;

/// A first-attempt permit is returned only when the worker proves no commit.
#[allow(dead_code)]
#[derive(Debug, thiserror::Error)]
pub(crate) enum DevelopmentReservationError {
    #[error("reservation was not committed: {error}")]
    DefinitelyNotCommitted {
        error: Box<DatabaseError>,
        fresh: entities::DevelopmentFreshReservation,
    },
    #[error(transparent)]
    Other(DatabaseError),
}

/// Reconcile a reservation request against a stopped database without opening a worker.
/// Access is restricted to trusted internal components and filesystem access.
#[allow(dead_code)]
pub(crate) async fn reconcile_development_reservation_offline(
    directory: PathBuf,
    request: entities::DevelopmentReservationRequest,
) -> Result<entities::DevelopmentReservationReconciliation, DatabaseError> {
    if !redb::valid_reservation_request_id(request.id) {
        return Err(DatabaseError::Rejected(entities::Violation::InvalidValue(
            "reservation_request",
            "expected a UUIDv7 request identity",
        )));
    }
    ntex::rt::spawn_blocking(move || {
        redb::reconcile_development_reservation_offline(&redb::database_file(&directory), request)
    })
    .await
    .map_err(|_| DatabaseError::ReadInterrupted)?
    .map_err(DatabaseError::Read)
}
/// Read-only inspection of the Allocations a stopped database still holds; see `redb::inspect_held_allocations_offline`.
pub(crate) fn inspect_held_allocations_offline(
    directory: &std::path::Path,
) -> Result<serde_json::Value, DatabaseError> {
    redb::inspect_held_allocations_offline(directory).map_err(DatabaseError::Read)
}

/// Offline integrity check of a stopped database that never opens it for
/// writing; see `redb::verify_offline`.
pub(crate) fn verify_offline(
    directory: &std::path::Path,
    pages: bool,
) -> Result<serde_json::Value, DatabaseError> {
    redb::verify_offline(directory, pages).map_err(DatabaseError::Read)
}

#[cfg(unix)]
pub(crate) fn verify_reserved_publication(
    directory: &std::path::Path,
    database: &std::fs::File,
    exclusive_lock: &std::fs::File,
    expected_device: u64,
    expected_inode: u64,
) -> Result<serde_json::Value, DatabaseError> {
    redb::verify_reserved_publication(
        directory,
        database,
        exclusive_lock,
        expected_device,
        expected_inode,
    )
    .map_err(DatabaseError::Read)
}

#[cfg(unix)]
pub(crate) fn verify_recovered_publication(
    directory: &std::path::Path,
    database: &std::fs::File,
    exclusive_lock: &std::fs::File,
    expected_device: u64,
    expected_inode: u64,
) -> Result<serde_json::Value, DatabaseError> {
    redb::verify_recovered_publication(
        directory,
        database,
        exclusive_lock,
        expected_device,
        expected_inode,
    )
    .map_err(DatabaseError::Read)
}

pub(crate) use redb::{
    DATABASE_FILE_NAME, HeldRecoveryPlan, ReservedRefusal, storage_format_versions,
};

/// Decides on a private copy whether a Job is a Reserved Allocation that
/// never started; see `redb::reconcile_reserved_preflight`.
pub(crate) fn reconcile_reserved_preflight(
    copy: std::fs::File,
    job: entities::JobId,
) -> Result<serde_json::Value, ReservedRefusal> {
    redb::reconcile_reserved_preflight(copy, job)
}

/// Ends an unstarted Reservation on a private copy and verifies the copy
/// completely; see `redb::reconcile_reserved_apply`.
pub(crate) fn reconcile_reserved_apply(
    copy: std::fs::File,
    job: entities::JobId,
) -> Result<serde_json::Value, ReservedRefusal> {
    redb::reconcile_reserved_apply(copy, job)
}

/// Plans recovery of every eligible stopped loopback Allocation on a private
/// copy. The digest binds the source file and every decision in stable order.
pub(crate) fn reconcile_held_preflight(
    copy: std::fs::File,
    source_sha256: &str,
) -> Result<HeldRecoveryPlan, ReservedRefusal> {
    redb::reconcile_held_preflight(copy, source_sha256)
}

/// Applies exactly the previously returned plan on a private copy.
pub(crate) fn reconcile_held_apply(
    copy: std::fs::File,
    source_sha256: &str,
    expected_digest: &str,
) -> Result<serde_json::Value, ReservedRefusal> {
    redb::reconcile_held_apply(copy, source_sha256, expected_digest)
}

pub(crate) fn verify_reserved_publication_delta(
    source: &std::fs::File,
    published: &std::fs::File,
    job: entities::JobId,
) -> Result<serde_json::Value, ReservedRefusal> {
    redb::verify_reserved_publication_delta(source, published, job)
}

pub(crate) fn verify_held_publication_delta(
    source: &std::fs::File,
    published: &std::fs::File,
    source_sha256: &str,
    expected_plan: &serde_json::Value,
    expected_digest: &str,
) -> Result<serde_json::Value, ReservedRefusal> {
    redb::verify_held_publication_delta(
        source,
        published,
        source_sha256,
        expected_plan,
        expected_digest,
    )
}

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
pub(crate) use commands::serialized_len;
mod errors;
mod handle;
pub(crate) use handle::{
    DevelopmentHandoff, DevelopmentHandoffError, DevelopmentPreviewError,
    PreparedDevelopmentHandoff,
};
mod scheduler_wake;
mod shared;

mod worker;

use commands::{Command, Control, Envelope, InFlightCommand, Message, Reply};
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
