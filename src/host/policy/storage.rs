use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use super::{SystemPolicy, SystemPolicyError};
use crate::db::database_file;
use crate::host::data_directory::DataDirectory;

impl SystemPolicy {
    pub(super) fn publish(
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
        let mut file = crate::host::data_directory::private_file(&mut OpenOptions::new())
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

pub(super) fn ensure_no_database(
    data_directory: &DataDirectory,
    path: &Path,
) -> Result<(), SystemPolicyError> {
    let database = database_file(data_directory.database_path());
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

pub(super) fn io_error(
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
fn open_directory(path: &Path) -> io::Result<Option<File>> {
    File::open(path).map(Some)
}

#[cfg(not(unix))]
fn open_directory(_path: &Path) -> io::Result<Option<File>> {
    Ok(None)
}
