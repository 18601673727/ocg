use std::ffi::OsString;
use std::fs::OpenOptions;
use std::io;
use std::path::{Path, PathBuf};

pub const DATA_DIRECTORY_VARIABLE: &str = "OCG_DATA_DIR";
pub(crate) const DATABASE_LOCK_FILE: &str = ".ocg-database.lock";

pub(crate) fn lock_database(directory: &Path) -> io::Result<std::fs::File> {
    let path = directory.join(DATABASE_LOCK_FILE);
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    private_file(&mut options);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let file = options.open(&path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() != 0 {
        return Err(io::Error::other(
            "database lock must be an empty regular file",
        ));
    }
    file.try_lock()
        .map_err(|error| io::Error::other(format!("database lock unavailable: {error}")))?;
    Ok(file)
}

pub(crate) fn hold_database_lock_shared(directory: &Path) -> io::Result<std::fs::File> {
    let path = directory.join(DATABASE_LOCK_FILE);
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let file = options.open(&path).map_err(|error| {
        if error.kind() == io::ErrorKind::NotFound {
            io::Error::other(
                "the data directory has no database lock file, so writers cannot be excluded without creating one; refusing to modify the source",
            )
        } else {
            error
        }
    })?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() != 0 {
        return Err(io::Error::other(
            "database lock must be an empty regular file",
        ));
    }
    file.try_lock_shared().map_err(|error| match error {
        std::fs::TryLockError::WouldBlock => {
            io::Error::other("the database is in use by a running OCG host")
        }
        std::fs::TryLockError::Error(error) => error,
    })?;
    Ok(file)
}

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

    pub fn bootstrap(root: OsString) -> Result<Self, DataDirectoryError> {
        let root = PathBuf::from(root);
        create_directory(&root)?;
        let root = root
            .canonicalize()
            .map_err(|source| DataDirectoryError::Io {
                path: root.clone(),
                source,
            })?;
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

    pub fn existing(root: impl Into<PathBuf>) -> Result<Self, DataDirectoryError> {
        let named = root.into();
        let root = named
            .canonicalize()
            .map_err(|source| DataDirectoryError::Io {
                path: named.clone(),
                source,
            })?;
        let metadata =
            std::fs::symlink_metadata(&root).map_err(|source| DataDirectoryError::Io {
                path: root.clone(),
                source,
            })?;
        if !metadata.is_dir() {
            return Err(DataDirectoryError::Io {
                path: root,
                source: io::Error::other("the data directory is not a directory"),
            });
        }
        Ok(Self { root })
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
    if !applies(&metadata) || metadata.permissions().mode() & 0o7777 == mode {
        return Ok(());
    }
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
}

pub(crate) fn private_file(options: &mut OpenOptions) -> &mut OpenOptions {
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(options, PRIVATE_FILE_MODE);
    options
}
