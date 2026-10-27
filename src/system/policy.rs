use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::db::redb;
use crate::system::data_directory::DataDirectory;
use crate::system::logger::{LoggerConfig, LoggerConfigError};
use crate::system::monitor::{self, FilesystemConfig, FilesystemConfigError, ProtectionReason};

pub const SYSTEM_POLICY_FILE_NAME: &str = "ocg.conf";
pub const SYSTEM_POLICY_FORMAT_VERSION: u32 = 2;

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
    #[error("system policy {}: invalid configuration at line {line}: {detail}", path.display())]
    ConfigurationText {
        path: PathBuf,
        line: usize,
        detail: &'static str,
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

impl SystemPolicy {
    pub fn path(data_directory: &DataDirectory) -> PathBuf {
        data_directory.root().join(SYSTEM_POLICY_FILE_NAME)
    }

    pub fn load_for_startup(data_directory: &DataDirectory) -> Result<Self, SystemPolicyError> {
        let path = Self::path(data_directory);
        let (policy, initialize) = match fs::read(&path) {
            Ok(bytes) => (Self::parse(&bytes, &path)?, false),
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
        let reasons = monitor::protected_paths(data_directory)
            .iter()
            .filter_map(|(area, protected_path)| {
                monitor::check_space(
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
        let directory = data_directory.root();
        let directory_file = open_directory(directory).map_err(|source| {
            io_error(
                path,
                "open policy directory for durable publication",
                source,
            )
        })?;
        ensure_no_database(data_directory, path)?;
        let bytes = self.to_conf().into_bytes();
        static NEXT_TEMPORARY: AtomicU64 = AtomicU64::new(0);
        let temporary = directory.join(format!(
            ".ocg-conf-{}-{}.tmp",
            std::process::id(),
            NEXT_TEMPORARY.fetch_add(1, Ordering::Relaxed),
        ));
        let mut file = crate::system::data_directory::private_file(&mut OpenOptions::new())
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
    let database = redb::database_file(data_directory.database_path());
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

impl SystemPolicy {
    pub fn to_conf(&self) -> String {
        let filesystem = self.filesystem_config;
        let logger = self.logger_config;
        let level = match logger.level {
            crate::system::logger::LoggerLevel::Off => "off",
            crate::system::logger::LoggerLevel::Error => "error",
            crate::system::logger::LoggerLevel::Warn => "warn",
            crate::system::logger::LoggerLevel::Info => "info",
            crate::system::logger::LoggerLevel::Debug => "debug",
            crate::system::logger::LoggerLevel::Trace => "trace",
        };
        format!(
            "format_version = {}\n\n[filesystem]\ndisk_lock_threshold_bytes = {}\ndisk_resume_threshold_bytes = {}\ndisk_check_interval_ms = {}\n\n[cleanup]\nmax_log_files_per_run = {}\n\n[logger]\nlevel = {level}\nretention_days = {}\nmax_file_size_bytes = {}\nflush_interval_ms = {}\n",
            self.format_version,
            filesystem.disk_lock_threshold_bytes,
            filesystem.disk_resume_threshold_bytes,
            filesystem.disk_check_interval_ms,
            filesystem.cleanup_config.max_log_files_per_run,
            logger.retention_days,
            logger.max_file_size_bytes,
            logger.flush_interval_ms
        )
    }

    pub(super) fn parse(bytes: &[u8], path: &Path) -> Result<Self, SystemPolicyError> {
        let invalid = |line, detail| SystemPolicyError::ConfigurationText {
            path: path.to_path_buf(),
            line,
            detail,
        };
        let text =
            std::str::from_utf8(bytes).map_err(|_| invalid(0, "configuration must be UTF-8"))?;
        let mut policy = Self::default();
        let mut section = "";
        let mut seen = std::collections::BTreeSet::new();
        for (index, line) in text.lines().enumerate() {
            let number = index + 1;
            let line = line.split('#').next().unwrap_or_default().trim();
            if line.is_empty() {
                continue;
            }
            if line.starts_with('[') && line.ends_with(']') {
                section = line[1..line.len() - 1].trim();
                if !matches!(section, "filesystem" | "cleanup" | "logger") {
                    return Err(invalid(number, "unknown section"));
                }
                continue;
            }
            let Some((key, value)) = line.split_once('=') else {
                return Err(invalid(number, "expected key = value"));
            };
            let key = key.trim();
            let value = value.trim();
            if !seen.insert((section, key)) {
                return Err(invalid(number, "duplicate configuration key"));
            }
            let integer = || {
                value
                    .parse::<u64>()
                    .map_err(|_| invalid(number, "expected an unsigned integer"))
            };
            let small = || {
                integer().and_then(|value| {
                    u32::try_from(value)
                        .map_err(|_| invalid(number, "integer exceeds 32-bit range"))
                })
            };
            match (section, key) {
                ("", "format_version") => policy.format_version = small()?,
                ("filesystem", "disk_lock_threshold_bytes") => {
                    policy.filesystem_config.disk_lock_threshold_bytes = integer()?
                }
                ("filesystem", "disk_resume_threshold_bytes") => {
                    policy.filesystem_config.disk_resume_threshold_bytes = integer()?
                }
                ("filesystem", "disk_check_interval_ms") => {
                    policy.filesystem_config.disk_check_interval_ms = integer()?
                }
                ("cleanup", "max_log_files_per_run") => {
                    policy
                        .filesystem_config
                        .cleanup_config
                        .max_log_files_per_run = small()?
                }
                ("logger", "level") => {
                    policy.logger_config.level = match value {
                        "off" => crate::system::logger::LoggerLevel::Off,
                        "error" => crate::system::logger::LoggerLevel::Error,
                        "warn" => crate::system::logger::LoggerLevel::Warn,
                        "info" => crate::system::logger::LoggerLevel::Info,
                        "debug" => crate::system::logger::LoggerLevel::Debug,
                        "trace" => crate::system::logger::LoggerLevel::Trace,
                        _ => return Err(invalid(number, "unknown logging level")),
                    }
                }
                ("logger", "retention_days") => policy.logger_config.retention_days = small()?,
                ("logger", "max_file_size_bytes") => {
                    policy.logger_config.max_file_size_bytes = integer()?
                }
                ("logger", "flush_interval_ms") => {
                    policy.logger_config.flush_interval_ms = integer()?
                }
                _ => return Err(invalid(number, "unknown configuration key")),
            }
        }
        if !seen.contains(&("", "format_version")) {
            return Err(invalid(0, "format_version is required"));
        }
        policy.validate(path)?;
        Ok(policy)
    }
}
