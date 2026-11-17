use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use crate::db::entities::{OperationId, Timestamp};
use crate::db::{OperationStatus, StorageError};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ProtectedArea {
    Database,
    Attachments,
    Logs,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpaceShortage {
    pub area: ProtectedArea,
    pub path: PathBuf,
    pub available_bytes: u64,
    pub expected_write_bytes: u64,
    pub threshold_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpaceQueryFailure {
    pub area: ProtectedArea,
    pub path: PathBuf,
    pub kind: io::ErrorKind,
    pub os_error: Option<i32>,
    pub detail: String,
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ProtectionReason {
    #[error(
        "{} has {} bytes available; {} more bytes would leave less than {}",
        .0.path.display(), .0.available_bytes, .0.expected_write_bytes, .0.threshold_bytes
    )]
    InsufficientSpace(SpaceShortage),
    #[error("the available space of {} could not be measured: {}", .0.path.display(), .0.detail)]
    SpaceQueryFailed(SpaceQueryFailure),
    #[error("filesystem write to {} failed: {detail}", path.display())]
    FilesystemWriteFailed {
        area: ProtectedArea,
        path: PathBuf,
        kind: io::ErrorKind,
        os_error: Option<i32>,
        detail: String,
    },
    #[error("storage failed and the database must be reopened: {0}")]
    StorageUnavailable(StorageError),
}

impl ProtectionReason {
    fn same_subject(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::InsufficientSpace(a), Self::InsufficientSpace(b)) => a.area == b.area,
            (Self::SpaceQueryFailed(a), Self::SpaceQueryFailed(b)) => a.area == b.area,
            (
                Self::FilesystemWriteFailed { area: a, .. },
                Self::FilesystemWriteFailed { area: b, .. },
            ) => a == b,
            (Self::StorageUnavailable(_), Self::StorageUnavailable(_)) => true,
            _ => false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProtectionLock {
    pub reasons: Vec<ProtectionReason>,
    pub locked_at: Timestamp,
}

impl ProtectionLock {
    pub fn storage_failure(&self) -> Option<&StorageError> {
        self.reasons.iter().find_map(|reason| match reason {
            ProtectionReason::StorageUnavailable(error) => Some(error),
            ProtectionReason::InsufficientSpace(_)
            | ProtectionReason::SpaceQueryFailed(_)
            | ProtectionReason::FilesystemWriteFailed { .. } => None,
        })
    }
}

impl std::fmt::Display for ProtectionLock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (index, reason) in self.reasons.iter().enumerate() {
            if index > 0 {
                f.write_str("; ")?;
            }
            write!(f, "{reason}")?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DiskProtectionStatus {
    pub lock: Option<ProtectionLock>,
    pub recovering: bool,
    pub completed_checks: u64,
    pub last_checked_at: Option<Timestamp>,
    pub recovery_attempts: u64,
    pub generation: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpaceMeasurement {
    pub area: ProtectedArea,
    pub path: PathBuf,
    pub available_bytes: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RecoveryReport {
    pub reopened_epoch: Option<u64>,
    pub reconciled: Vec<(OperationId, OperationStatus)>,
    pub available: Vec<SpaceMeasurement>,
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ReopenFailure {
    #[error("{0}")]
    Storage(StorageError),
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum RecoveryError {
    #[error("the maintenance queue is full; recovery was not accepted")]
    MaintenanceQueueFull,
    #[error("a protected filesystem does not have the resume threshold available")]
    Space(Vec<ProtectionReason>),
    #[error("reopening the database failed: {0}")]
    Reopen(ReopenFailure),
    #[error("reconciling {operation} failed: {error}")]
    Reconcile {
        operation: OperationId,
        error: StorageError,
    },
    #[error("the database is shutting down")]
    ShuttingDown,
    #[error("the database worker stopped")]
    WorkerStopped,
}

pub(crate) struct ProtectionCell {
    blocked: AtomicBool,
    lock_epoch: AtomicU64,
    status: Mutex<DiskProtectionStatus>,
    watchers: Mutex<Option<Vec<flume::Sender<()>>>>,
}

impl ProtectionCell {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            blocked: AtomicBool::new(false),
            lock_epoch: AtomicU64::new(0),
            status: Mutex::new(DiskProtectionStatus::default()),
            watchers: Mutex::new(Some(Vec::new())),
        })
    }

    pub(crate) fn status(&self) -> DiskProtectionStatus {
        self.status
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn logging_state(&self) -> (bool, u64) {
        let epoch = self.lock_epoch.load(Ordering::SeqCst);
        (self.blocked.load(Ordering::SeqCst), epoch)
    }

    pub(crate) fn update<T>(&self, change: impl FnOnce(&mut DiskProtectionStatus) -> T) -> T {
        let value = {
            let mut status = self.status.lock().unwrap_or_else(PoisonError::into_inner);
            let value = change(&mut status);
            self.blocked.store(status.lock.is_some(), Ordering::SeqCst);
            status.generation += 1;
            value
        };
        let mut watchers = self.watchers.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(watchers) = watchers.as_mut() {
            watchers.retain(|watcher| {
                !matches!(
                    watcher.try_send(()),
                    Err(flume::TrySendError::Disconnected(()))
                )
            });
        }
        value
    }

    pub(crate) fn lock(&self, reasons: Vec<ProtectionReason>) -> ProtectionLock {
        self.blocked.store(true, Ordering::SeqCst);
        self.lock_epoch.fetch_add(1, Ordering::SeqCst);
        self.update(|status| {
            let lock = status.lock.get_or_insert_with(|| ProtectionLock {
                reasons: Vec::new(),
                locked_at: Timestamp::now(),
            });
            for reason in reasons {
                match lock
                    .reasons
                    .iter_mut()
                    .find(|held| held.same_subject(&reason))
                {
                    Some(held) => *held = reason,
                    None => lock.reasons.push(reason),
                }
            }
            lock.clone()
        })
    }

    pub(crate) fn watch(self: &Arc<Self>) -> ProtectionWatch {
        let (sender, receiver) = flume::bounded(1);
        if let Some(watchers) = self
            .watchers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_mut()
        {
            watchers.push(sender);
        }
        ProtectionWatch {
            receiver,
            cell: self.clone(),
        }
    }

    pub(crate) fn close(&self) {
        self.watchers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
    }
}

pub struct ProtectionWatch {
    receiver: flume::Receiver<()>,
    cell: Arc<ProtectionCell>,
}

impl ProtectionWatch {
    pub fn status(&self) -> DiskProtectionStatus {
        self.cell.status()
    }

    pub async fn changed(&self) -> Option<DiskProtectionStatus> {
        self.receiver
            .recv_async()
            .await
            .ok()
            .map(|()| self.cell.status())
    }
}
