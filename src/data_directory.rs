use std::ffi::OsString;
use std::io;
use std::path::{Path, PathBuf};

pub const DATA_DIRECTORY_VARIABLE: &str = "OCG_DATA_DIR";

#[derive(Debug, thiserror::Error)]
pub enum DataDirectoryError {
    #[error("{DATA_DIRECTORY_VARIABLE} must be set to the OCG data directory")]
    NotConfigured,
    #[error("failed to prepare {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}

/// The absolute, resolved layout under `OCG_DATA_DIR`. The internal paths are
/// derived from the root and cannot be configured individually.
#[derive(Clone, Debug)]
pub struct DataDirectory {
    root: PathBuf,
    database: PathBuf,
    key: PathBuf,
    attachments: PathBuf,
    logs: PathBuf,
}

impl DataDirectory {
    pub fn from_environment() -> Result<Self, DataDirectoryError> {
        let root = std::env::var_os(DATA_DIRECTORY_VARIABLE)
            .filter(|value| !value.is_empty())
            .ok_or(DataDirectoryError::NotConfigured)?;
        Self::bootstrap(root)
    }

    /// Creates the root, database, attachments, and logs directories when
    /// missing. The key path is only reserved; no key file is created. Free
    /// space is checked by the database worker at startup, against the
    /// configured lock threshold.
    pub fn bootstrap(root: OsString) -> Result<Self, DataDirectoryError> {
        let root = PathBuf::from(root);
        create_directory(&root)?;
        let root = root.canonicalize().map_err(|source| DataDirectoryError::Io {
            path: root.clone(),
            source,
        })?;

        let data_directory = Self {
            database: root.join("ocg_db"),
            key: root.join("ocg.key"),
            attachments: root.join("ocg_attachments"),
            logs: root.join("ocg_logs"),
            root,
        };
        for directory in [
            &data_directory.database,
            &data_directory.attachments,
            &data_directory.logs,
        ] {
            create_directory(directory)?;
        }
        Ok(data_directory)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn database_path(&self) -> &Path {
        &self.database
    }

    pub fn key_path(&self) -> &Path {
        &self.key
    }

    pub fn attachments_path(&self) -> &Path {
        &self.attachments
    }

    pub fn logs_path(&self) -> &Path {
        &self.logs
    }
}

fn create_directory(path: &Path) -> Result<(), DataDirectoryError> {
    std::fs::create_dir_all(path).map_err(|source| DataDirectoryError::Io {
        path: path.to_path_buf(),
        source,
    })
}
