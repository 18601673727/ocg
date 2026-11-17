use std::path::Path;

use super::{SystemPolicy, SystemPolicyError};

impl SystemPolicy {
    pub fn to_conf(&self) -> String {
        let filesystem = self.filesystem_config;
        let logger = self.logger_config;
        let level = match logger.level {
            crate::host::logger::LoggerLevel::Off => "off",
            crate::host::logger::LoggerLevel::Error => "error",
            crate::host::logger::LoggerLevel::Warn => "warn",
            crate::host::logger::LoggerLevel::Info => "info",
            crate::host::logger::LoggerLevel::Debug => "debug",
            crate::host::logger::LoggerLevel::Trace => "trace",
        };
        format!(
            "[filesystem]\ndisk_lock_threshold_bytes = {}\ndisk_resume_threshold_bytes = {}\ndisk_check_interval_ms = {}\n\n[cleanup]\nmax_log_files_per_run = {}\n\n[logger]\nlevel = {level}\nretention_days = {}\nmax_file_size_bytes = {}\nflush_interval_ms = {}\n",
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
                        "off" => crate::host::logger::LoggerLevel::Off,
                        "error" => crate::host::logger::LoggerLevel::Error,
                        "warn" => crate::host::logger::LoggerLevel::Warn,
                        "info" => crate::host::logger::LoggerLevel::Info,
                        "debug" => crate::host::logger::LoggerLevel::Debug,
                        "trace" => crate::host::logger::LoggerLevel::Trace,
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
        if seen.is_empty() {
            return Err(invalid(
                0,
                "configuration must contain at least one setting",
            ));
        }
        policy.validate(path)?;
        Ok(policy)
    }
}
