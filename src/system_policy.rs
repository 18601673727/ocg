use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::data_directory::DataDirectory;
use crate::disk_protection::{
    self, FilesystemConfig, FilesystemConfigError, ProtectionLock, ProtectionReason, SpaceShortage,
};
use crate::logger::{LoggerConfig, LoggerConfigError};
use crate::storage;

pub const SYSTEM_POLICY_FILE_NAME: &str = "system_policy.json";
pub const SYSTEM_POLICY_FORMAT_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SystemPolicy {
    pub format_version: u32,
    pub filesystem_config: FilesystemConfig,
    #[serde(default)]
    pub logger_config: LoggerConfig,
}

impl Default for SystemPolicy {
    fn default() -> Self {
        Self {
            format_version: SYSTEM_POLICY_FORMAT_VERSION,
            filesystem_config: FilesystemConfig::default(),
            logger_config: LoggerConfig::default(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SystemPolicyError {
    #[error("system policy {}: {operation}: {source}", path.display())]
    Io {
        path: PathBuf,
        operation: String,
        #[source]
        source: io::Error,
    },
    #[error("system policy {}: invalid JSON or field types: {source}", path.display())]
    Json {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("system policy {}: unsupported format_version {found}; supported version is {SYSTEM_POLICY_FORMAT_VERSION}", path.display())]
    UnsupportedFormat { path: PathBuf, found: u32 },
    #[error("system policy {}: invalid filesystem_config: {source}", path.display())]
    Configuration {
        path: PathBuf,
        #[source]
        source: FilesystemConfigError,
    },
    #[error("system policy {}: invalid logger_config: {source}", path.display())]
    LoggerConfiguration {
        path: PathBuf,
        #[source]
        source: LoggerConfigError,
    },
    #[error("system policy {} is missing alongside existing database {}; startup refused (no automatic migration)", path.display(), database.display())]
    Missing { path: PathBuf, database: PathBuf },
    #[error("system policy {}: startup refused by disk protection: {}", path.display(), reasons.iter().map(ToString::to_string).collect::<Vec<_>>().join("; "))]
    StartupSpace {
        path: PathBuf,
        reasons: Vec<ProtectionReason>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PolicySaveResult {
    SavedForRestart,
    Unchanged,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PolicySaveFailureOutcome {
    NotSaved,
    Uncertain,
}

#[derive(Debug, thiserror::Error)]
pub enum PolicyOperationError {
    #[error("the worker is shutting down; the policy operation was refused")]
    ShuttingDown,
    #[error("the worker stopped before the policy operation ran")]
    WorkerStopped,
    #[error("the control queue is full; the policy operation was not accepted")]
    QueueFull,
    #[error("disk protection is active; the policy save was refused: {0}")]
    DiskProtected(ProtectionLock),
    #[error("the policy save allowance would cross the active lock threshold: {0:?}")]
    InsufficientSpace(SpaceShortage),
    #[error("the saved system policy differs from the expected previous policy")]
    Conflict {
        expected: Box<SystemPolicy>,
        actual: Box<SystemPolicy>,
    },
    #[error("failed to read saved system policy: {0}")]
    Read(#[source] SystemPolicyError),
    #[error("system policy was not saved: {0}")]
    NotSaved(#[source] SystemPolicyError),
    #[error("system policy replacement is visible but durability is uncertain: {0}")]
    OutcomeUncertain(#[source] SystemPolicyError),
    #[error("the worker stopped during the policy save; its outcome is uncertain")]
    SaveInterrupted,
}

impl PolicyOperationError {
    pub fn save_failure_outcome(&self) -> PolicySaveFailureOutcome {
        match self {
            Self::OutcomeUncertain(_) | Self::SaveInterrupted => {
                PolicySaveFailureOutcome::Uncertain
            }
            _ => PolicySaveFailureOutcome::NotSaved,
        }
    }
}

impl SystemPolicy {
    pub fn path(data_directory: &DataDirectory) -> PathBuf {
        data_directory.database_path().join(SYSTEM_POLICY_FILE_NAME)
    }

    pub fn load_for_startup(data_directory: &DataDirectory) -> Result<Self, SystemPolicyError> {
        let path = Self::path(data_directory);
        let (policy, initialize) = match fs::read(&path) {
            Ok(bytes) => (
                serde_json::from_slice::<Self>(&bytes).map_err(|source| {
                    SystemPolicyError::Json {
                        path: path.clone(),
                        source,
                    }
                })?,
                false,
            ),
            Err(source) if source.kind() == io::ErrorKind::NotFound => {
                // A dangling policy symlink is an invalid existing entry, not a fresh directory.
                match fs::symlink_metadata(&path) {
                    Ok(_) => return Err(io_error(&path, "read existing policy", source)),
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                    Err(error) => return Err(io_error(&path, "inspect missing policy", error)),
                }
                ensure_no_database(data_directory, &path)?;
                (Self::default(), true)
            }
            Err(source) => return Err(io_error(&path, "read policy", source)),
        };
        policy.validate(&path)?;
        let reasons = disk_protection::protected_paths(data_directory)
            .iter()
            .filter_map(|(area, protected_path)| {
                disk_protection::check_space(
                    *area,
                    protected_path,
                    0,
                    policy.filesystem_config.disk_lock_threshold_bytes,
                )
                .err()
            })
            .collect::<Vec<_>>();
        if !reasons.is_empty() {
            return Err(SystemPolicyError::StartupSpace { path, reasons });
        }
        if initialize {
            policy.publish(data_directory, &path)?;
        }
        Ok(policy)
    }

    pub(crate) fn read_saved(path: &Path) -> Result<Self, SystemPolicyError> {
        let bytes = fs::read(path).map_err(|source| io_error(path, "read saved policy", source))?;
        let policy: Self =
            serde_json::from_slice(&bytes).map_err(|source| SystemPolicyError::Json {
                path: path.to_path_buf(),
                source,
            })?;
        policy.validate(path)?;
        Ok(policy)
    }

    pub(crate) fn validate(&self, path: &Path) -> Result<(), SystemPolicyError> {
        if self.format_version != SYSTEM_POLICY_FORMAT_VERSION {
            return Err(SystemPolicyError::UnsupportedFormat {
                path: path.to_path_buf(),
                found: self.format_version,
            });
        }
        self.filesystem_config
            .validate()
            .map_err(|source| SystemPolicyError::Configuration {
                path: path.to_path_buf(),
                source,
            })?;
        self.logger_config
            .validate()
            .map_err(|source| SystemPolicyError::LoggerConfiguration {
                path: path.to_path_buf(),
                source,
            })
    }

    fn publish(
        &self,
        data_directory: &DataDirectory,
        path: &Path,
    ) -> Result<(), SystemPolicyError> {
        let directory = data_directory.database_path();
        let directory_file = open_directory(directory).map_err(|source| {
            io_error(
                path,
                "open policy directory for durable publication",
                source,
            )
        })?;
        ensure_no_database(data_directory, path)?;
        let mut bytes =
            serde_json::to_vec_pretty(self).map_err(|source| SystemPolicyError::Json {
                path: path.to_path_buf(),
                source,
            })?;
        bytes.push(b'\n');
        static NEXT_TEMPORARY: AtomicU64 = AtomicU64::new(0);
        let temporary = directory.join(format!(
            ".system_policy-{}-{}.tmp",
            std::process::id(),
            NEXT_TEMPORARY.fetch_add(1, Ordering::Relaxed),
        ));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|source| {
                io_error(
                    path,
                    format!("create temporary file {}", temporary.display()),
                    source,
                )
            })?;
        file.write_all(&bytes)
            .and_then(|()| file.flush())
            .and_then(|()| file.sync_all())
            .map_err(|source| {
                io_error(
                    path,
                    format!("write and persist temporary file {}", temporary.display()),
                    source,
                )
            })?;
        drop(file);
        // Linking publishes the complete, flushed inode atomically and cannot replace any existing entry.
        // Any failure leaves the artifacts in place for inspection; startup never retries publication.
        fs::hard_link(&temporary, path).map_err(|source| {
            io_error(
                path,
                format!("publish without overwrite from {}", temporary.display()),
                source,
            )
        })?;
        fs::remove_file(&temporary).map_err(|source| {
            io_error(
                path,
                format!("remove published temporary file {}", temporary.display()),
                source,
            )
        })?;
        if let Some(directory_file) = directory_file {
            directory_file.sync_all().map_err(|source| {
                io_error(
                    path,
                    "persist policy directory entry (publication durability uncertain)",
                    source,
                )
            })?;
        }
        Ok(())
    }
}

fn ensure_no_database(
    data_directory: &DataDirectory,
    path: &Path,
) -> Result<(), SystemPolicyError> {
    let database = storage::database_file(data_directory.database_path());
    match fs::symlink_metadata(&database) {
        Ok(_) => Err(SystemPolicyError::Missing {
            path: path.to_path_buf(),
            database,
        }),
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(io_error(
            path,
            format!("inspect database {}", database.display()),
            source,
        )),
    }
}

pub(crate) fn io_error(
    path: &Path,
    operation: impl Into<String>,
    source: io::Error,
) -> SystemPolicyError {
    SystemPolicyError::Io {
        path: path.to_path_buf(),
        operation: operation.into(),
        source,
    }
}

#[cfg(unix)]
pub(crate) fn open_directory(path: &Path) -> io::Result<Option<File>> {
    File::open(path).map(Some)
}

/// Directory handles cannot be fsynced on this platform; publication relies
/// on the filesystem persisting the directory entry with the file metadata.
#[cfg(not(unix))]
pub(crate) fn open_directory(_path: &Path) -> io::Result<Option<File>> {
    Ok(None)
}
