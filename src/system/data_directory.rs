use std::ffi::OsString;
use std::fs::OpenOptions;
use std::io;
use std::path::{Path, PathBuf};

pub const DATA_DIRECTORY_VARIABLE: &str = "OCG_DATA_DIR";

#[derive(Debug, thiserror::Error)]
pub enum DataDirectoryError {
    #[error("{DATA_DIRECTORY_VARIABLE} must be set to the OCG data directory")]
    NotConfigured,
    #[error(
        "{path} uses the obsolete ocg_db layout; stop OCG, move ocg.redb and credentials.json into the data root, and remove the empty ocg_db directory before restarting"
    )]
    LegacyLayout { path: PathBuf },
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
}

impl DataDirectory {
    pub fn from_environment() -> Result<Self, DataDirectoryError> {
        let root = std::env::var_os(DATA_DIRECTORY_VARIABLE)
            .filter(|value| !value.is_empty())
            .ok_or(DataDirectoryError::NotConfigured)?;
        Self::bootstrap(root)
    }

    /// Creates the root, attachments, and logs directories when missing.
    /// The key path is only reserved; no key file is created. Free
    /// space is checked by the database worker at startup, against the
    /// configured lock threshold.
    ///
    /// On Unix the root and the two directories are made private to their
    /// owner (0700), including in an existing installation. Nothing below
    /// them is changed, and a directory that is a symbolic link is left as
    /// its target is.
    pub fn bootstrap(root: OsString) -> Result<Self, DataDirectoryError> {
        let root = PathBuf::from(root);
        create_directory(&root)?;
        let root = root
            .canonicalize()
            .map_err(|source| DataDirectoryError::Io {
                path: root.clone(),
                source,
            })?;
        let legacy = root.join("ocg_db");
        match std::fs::symlink_metadata(&legacy) {
            Ok(_) => return Err(DataDirectoryError::LegacyLayout { path: legacy }),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(DataDirectoryError::Io {
                    path: legacy,
                    source,
                });
            }
        }
        make_private(&root)?;

        let data_directory = Self { root };
        for directory in [
            data_directory.attachments_path(),
            data_directory.logs_path(),
        ] {
            create_directory(&directory)?;
            make_private(&directory)?;
        }
        Ok(data_directory)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn database_path(&self) -> &Path {
        &self.root
    }

    pub fn attachments_path(&self) -> PathBuf {
        self.root.join("ocg_attachments")
    }

    pub fn logs_path(&self) -> PathBuf {
        self.root.join("ocg_logs")
    }
}

/// Creates the directory and its missing parents, private on Unix.
fn create_directory(path: &Path) -> Result<(), DataDirectoryError> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    std::os::unix::fs::DirBuilderExt::mode(&mut builder, PRIVATE_DIRECTORY_MODE);
    builder
        .create(path)
        .map_err(|source| DataDirectoryError::Io {
            path: path.to_path_buf(),
            source,
        })
}

fn make_private(path: &Path) -> Result<(), DataDirectoryError> {
    restrict_directory(path).map_err(|source| DataDirectoryError::Io {
        path: path.to_path_buf(),
        source,
    })
}

#[cfg(unix)]
const PRIVATE_DIRECTORY_MODE: u32 = 0o700;
#[cfg(unix)]
const PRIVATE_FILE_MODE: u32 = 0o600;

/// Unix: gives the directory at `path` mode 0700. A symbolic link is not
/// followed, and anything else that is not a directory is left alone.
pub(crate) fn restrict_directory(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        restrict(path, PRIVATE_DIRECTORY_MODE, |metadata| metadata.is_dir())
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(())
    }
}

/// Unix: gives the regular file at `path` mode 0600. A symbolic link is not
/// followed, and a missing file is not an error.
pub(crate) fn restrict_file(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        restrict(path, PRIVATE_FILE_MODE, |metadata| metadata.is_file())
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(())
    }
}

#[cfg(unix)]
fn restrict(
    path: &Path,
    mode: u32,
    applies: impl Fn(&std::fs::Metadata) -> bool,
) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    // `symlink_metadata` describes a link itself, so a link is never applicable.
    if !applies(&metadata) || metadata.permissions().mode() & 0o7777 == mode {
        return Ok(());
    }
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
}

/// Unix: makes a file that `options` creates private to its owner (0600).
pub(crate) fn private_file(options: &mut OpenOptions) -> &mut OpenOptions {
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(options, PRIVATE_FILE_MODE);
    options
}
