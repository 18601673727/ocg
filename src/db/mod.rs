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
use crate::db::entities::{Actor, Appended, Entry, ExecutionConfiguration, JobAction, JobGroup};
use crate::db::entities::{
    AppendedOutput, Attachment, AttachmentId, AttachmentOrigin, AttachmentRecord,
    AvailabilityStatId, AvailabilityStatRecord, Config, ConfigBody, ConfigId, ConfigKind,
    ConfigRecord, ContentDigest, ExecutionPoolId, ExecutorId, JobEdit, JobFinalization, JobId,
    JobInputId, JobOutput, JobOutputRecord, JobRecord, Metrics, MetricsId, MetricsRecord,
    ObservationCursor, ObservationFilter, ObservationPage, OperationId, OperationKind, Project,
    ProjectCommand, ProjectId, ProjectRecord, ProviderAnswer, ProviderRequestRecord, RequestNumber,
    Submission, SubmissionKey, SubmissionRecord, ToolCallRef, ToolEvidence, ToolEvidenceRecord,
    ToolOutcome, Violation,
};
use crate::db::mutations::PublishedAttachment;
pub(crate) use crate::db::redb::{Batch, BatchError, Failure};
use crate::db::redb::{BatchFailure, Commit, CompactFailure, OpenedFile, Probe};
pub use crate::db::redb::{StorageError, StorageErrorKind};
use crate::system::data_directory::DataDirectory;
use crate::system::monitor::{
    self, FilesystemConfig, FilesystemConfigError, ProtectedArea, ProtectionCell, ProtectionLock,
    ProtectionReason, ProtectionWatch, RecoveryError, RecoveryReport, ReopenFailure, SharedRequest,
    SpaceMeasurement, SpaceShortage,
};
use crate::system::policy::{SystemPolicy, SystemPolicyError};

#[derive(Clone, Copy)]
pub(crate) enum AdmissionCandidate {
    Decision(ExecutorId, entities::ProfileId),
}

pub const COMMAND_QUEUE_CAPACITY: usize = 256;

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

#[derive(Debug)]
pub(crate) enum ScheduledReservation {
    Pass(Box<entities::DevelopmentReservation>),
}

pub(crate) fn inspect_held_allocations_offline(
    directory: &std::path::Path,
) -> Result<serde_json::Value, DatabaseError> {
    redb::inspect_held_allocations_offline(directory).map_err(DatabaseError::Read)
}

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
    DATABASE_FILE_NAME, ReservedRefusal, reconcile_held_apply, reconcile_held_preflight,
    reconcile_reserved_apply, reconcile_reserved_preflight, storage_format_versions,
    verify_held_publication_delta, verify_reserved_publication_delta,
};

const GROUP_LIMIT: usize = 32;
const COMPACTION_MIN_FILE_BYTES: u64 = 64 * 1024 * 1024;
const COMPACTION_MIN_RECLAIMABLE_BYTES: u64 = 32 * 1024 * 1024;
const COMPACTION_MIN_RECLAIMABLE_DIVISOR: u64 = 4;
const COMPACTION_COOLDOWN: Duration = Duration::from_secs(24 * 60 * 60);
pub(crate) const PROTECTED_CHECK_INTERVAL: Duration = Duration::from_secs(30);

pub(crate) fn next_check_interval(configured: Duration, locked: bool) -> Duration {
    if locked {
        configured.min(PROTECTED_CHECK_INTERVAL)
    } else {
        configured
    }
}
pub const READ_WORKERS: usize = 2;
pub const READ_QUEUE_CAPACITY: usize = 64;
pub const MAX_OBSERVATION_SCAN: usize = redb::MAX_OBSERVATION_SCAN;
const CONTROL_QUEUE_CAPACITY: usize = 2;
const TRANSACTION_ALLOWANCE_BYTES: u64 = 1024 * 1024;

mod commands;
pub(crate) use commands::serialized_len;
mod errors;
mod handle;
pub(crate) use handle::{
    DevelopmentHandoff, DevelopmentHandoffError, DevelopmentToolContext, DevelopmentToolOffer,
    DevelopmentTooling, PreparedDevelopmentHandoff,
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
