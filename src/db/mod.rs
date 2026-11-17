pub mod attachments;
pub(crate) mod derived;
pub mod entities;
pub(crate) mod mutations;
mod redb;

use std::time::Duration;

pub use crate::db::attachments::{AttachmentContent, AttachmentError, Reclamation, StoreError};
use crate::db::entities::{AvailabilityStatId, JobId, MetricsId};
pub use crate::db::redb::{StorageError, StorageErrorKind};
pub(crate) use redb::StoredCredential;

pub const COMMAND_QUEUE_CAPACITY: usize = 256;

#[derive(Debug, thiserror::Error)]
pub(crate) enum DevelopmentReservationError {
    #[error("reservation was not committed: {0}")]
    DefinitelyNotCommitted(Box<DatabaseError>),
    #[error(transparent)]
    Other(DatabaseError),
}

#[cfg(unix)]
pub(crate) use redb::verify_publication;
pub(crate) use redb::{
    DATABASE_FILE_NAME, ReservedRefusal, database_file, inspect_held_allocations_offline,
    reconcile_held_apply, reconcile_held_preflight, reconcile_reserved_apply,
    reconcile_reserved_preflight, storage_format_versions, verify_held_publication_delta,
    verify_offline, verify_reserved_publication_delta,
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
const MAINTENANCE_QUEUE_CAPACITY: usize = 2;
const TRANSACTION_ALLOWANCE_BYTES: u64 = 1024 * 1024;

mod commands;
pub mod protection;
pub(crate) use commands::serialized_len;
mod errors;
mod handle;
mod handoff;
pub(crate) use handoff::{
    DevelopmentHandoff, DevelopmentHandoffError, PreparedDevelopmentHandoff, classify_unstarted,
};
mod scheduler_wake;
mod shared;

mod worker;

pub use errors::{
    CommandOutcome, CompactionOutcome, CompactionReport, CompactionSkip, DatabaseError,
    DatabaseStatus, MaintenanceError, OperationStatus, UnknownCause,
};
pub use scheduler_wake::SchedulerWakeSubscription;

pub use worker::{DatabaseHandle, DatabaseWorker};

#[derive(Clone, Copy, Debug)]
pub(crate) enum ObservationChange {
    Metrics(JobId, MetricsId),
    Availability(JobId, AvailabilityStatId),
}
