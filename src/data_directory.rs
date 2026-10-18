use std::ffi::OsString;
use std::fs::OpenOptions;
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
    ///
    /// On Unix the root and the three directories are made private to their
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
        make_private(&root)?;

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
            make_private(directory)?;
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

#[cfg(all(test, unix))]
mod tests {
    use std::os::unix::fs::{PermissionsExt, symlink};

    use super::*;
    use crate::database::DatabaseWorker;
    use crate::system_policy::SystemPolicy;
    use crate::test_support::data_directory;

    fn mode(path: &Path) -> u32 {
        std::fs::symlink_metadata(path)
            .expect("metadata")
            .permissions()
            .mode()
            & 0o7777
    }

    fn set_mode(path: &Path, mode: u32) {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).expect("chmod");
    }

    fn layout(directory: &DataDirectory) -> [&Path; 4] {
        [
            directory.root(),
            directory.database_path(),
            directory.attachments_path(),
            directory.logs_path(),
        ]
    }

    #[test]
    fn a_new_layout_is_private_to_its_owner() {
        let (_root, directory) = data_directory("ocg-data-directory", "new");
        for path in layout(&directory) {
            assert_eq!(mode(path), 0o700, "{}", path.display());
        }
    }

    #[test]
    fn an_existing_layout_is_made_private_without_touching_anything_else() {
        let (_root, directory) = data_directory("ocg-data-directory", "existing");
        for path in layout(&directory) {
            set_mode(path, 0o755);
        }
        let unrelated = directory.root().join("notes.txt");
        std::fs::write(&unrelated, "operator notes").expect("write");
        set_mode(&unrelated, 0o644);
        let nested = directory.logs_path().join("archive");
        std::fs::create_dir(&nested).expect("create nested directory");
        set_mode(&nested, 0o755);

        let directory =
            DataDirectory::bootstrap(directory.root().as_os_str().to_owned()).expect("bootstrap");
        for path in layout(&directory) {
            assert_eq!(mode(path), 0o700, "{}", path.display());
        }
        assert_eq!(mode(&unrelated), 0o644, "files are not rewritten");
        assert_eq!(mode(&nested), 0o755, "nothing is changed recursively");
    }

    #[test]
    fn a_symbolic_link_in_the_layout_is_not_followed() {
        let (_root, directory) = data_directory("ocg-data-directory", "symlink");
        let (_elsewhere_root, elsewhere) = data_directory("ocg-data-directory", "symlink-target");
        let target = elsewhere.root().join("shared-logs");
        std::fs::create_dir(&target).expect("create link target");
        set_mode(&target, 0o755);
        std::fs::remove_dir(directory.logs_path()).expect("remove logs");
        symlink(&target, directory.logs_path()).expect("link logs elsewhere");

        DataDirectory::bootstrap(directory.root().as_os_str().to_owned()).expect("bootstrap");
        assert_eq!(mode(&target), 0o755, "the link target keeps its mode");
        assert!(
            std::fs::symlink_metadata(directory.logs_path())
                .expect("metadata")
                .file_type()
                .is_symlink()
        );

        let file_target = elsewhere.root().join("target.txt");
        std::fs::write(&file_target, "x").expect("write");
        set_mode(&file_target, 0o644);
        let link = directory.root().join("link.txt");
        symlink(&file_target, &link).expect("link a file");
        restrict_file(&link).expect("a link is skipped");
        assert_eq!(mode(&file_target), 0o644);
    }

    #[test]
    fn the_database_file_and_the_system_policy_are_private() {
        let (_root, directory) = data_directory("ocg-data-directory", "files");
        let policy = SystemPolicy::load_for_startup(&directory).expect("initialize policy");
        let policy_path = SystemPolicy::path(&directory);
        assert_eq!(mode(&policy_path), 0o600);
        let worker = DatabaseWorker::start_with_policy(&directory, policy).expect("start");
        worker.shutdown().expect("shutdown");
        let database = crate::storage::database_file(directory.database_path());
        assert_eq!(mode(&database), 0o600, "a new database file is private");

        // An existing installation's database file is made private.
        set_mode(&database, 0o644);
        let policy = SystemPolicy::load_for_startup(&directory).expect("load policy");
        let worker = DatabaseWorker::start_with_policy(&directory, policy).expect("restart");
        worker.shutdown().expect("shutdown");
        assert_eq!(mode(&database), 0o600);
    }
}
