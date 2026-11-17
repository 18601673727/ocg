mod format;
mod storage;

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use storage::{ensure_no_database, io_error};

use crate::db::protection::ProtectionReason;
use crate::host::data_directory::DataDirectory;
use crate::host::disk::{self, FilesystemConfig, FilesystemConfigError};
use crate::host::logger::{LoggerConfig, LoggerConfigError};

pub const SYSTEM_POLICY_FILE_NAME: &str = "ocg.conf";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SystemPolicy {
    pub filesystem_config: FilesystemConfig,
    #[serde(default)]
    pub logger_config: LoggerConfig,
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
    #[error("system policy {}: invalid configuration at line {line}: {detail}", path.display())]
    ConfigurationText {
        path: PathBuf,
        line: usize,
        detail: &'static str,
    },
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

impl SystemPolicy {
    pub fn path(data_directory: &DataDirectory) -> PathBuf {
        data_directory.root().join(SYSTEM_POLICY_FILE_NAME)
    }

    pub fn load_for_startup(data_directory: &DataDirectory) -> Result<Self, SystemPolicyError> {
        let path = Self::path(data_directory);
        let (policy, initialize) = match fs::read(&path) {
            Ok(bytes) => (Self::parse(&bytes, &path)?, false),
            Err(source) if source.kind() == io::ErrorKind::NotFound => {
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
        let reasons = disk::protected_paths(data_directory)
            .iter()
            .filter_map(|(area, protected_path)| {
                disk::check_space(
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

    fn validate(&self, path: &Path) -> Result<(), SystemPolicyError> {
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
}
