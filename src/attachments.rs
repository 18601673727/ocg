//! The content of Attachments: immutable files in `ocg_attachments/`, each
//! named by the lowercase hexadecimal SHA-256 of its bytes, never by a name
//! an Attachment was given. An Attachment is a database record with an
//! identity of its own, see [`crate::persistence::AttachmentRecord`];
//! Attachments of identical bytes share one file.
//!
//! The directory is opened once, when the database starts, from the data
//! directory root without following a symbolic link, and must be a
//! directory of this user that no one else may use (no group or other
//! permission). Every later step is relative to that handle, and each first
//! checks that the root still is the directory opened then and that
//! `ocg_attachments` in it still names the directory held, so a directory
//! replaced or moved meanwhile is refused rather than trusted.
//!
//! Content is published before any record may name it: staged in a new
//! hidden file, created exclusively with mode 0600, written whole and
//! synced; then renamed to its digest without replacing anything, and the
//! directory synced. A file already under the digest is reused only when it
//! is a private regular file of this user, with one link, whose bytes equal
//! the content; any other file under that name is left as it is and the
//! publication fails. Reading checks the same, then the recorded length and
//! SHA-256 of what was read.
//!
//! A file and a redb transaction are not one atomic step. A crash after a
//! publication and before its record commits leaves a published file that
//! no record names; it is kept, and reused by a later publication of the
//! same bytes. A crash while staging leaves a staging file, which the next
//! start of the database removes: only a regular file whose name has the
//! staging form, never a published file, a link or anything else.
//!
//! What this does not establish: the root's ancestors are resolved by path;
//! the checks guard against mistakes and other users, not against code
//! running as this user, which can change any file between a check and its
//! use (a changed file is still refused when it is read, by its digest); and
//! durability is what `fsync` gives.

use std::fs::File;
use std::path::Path;

use crate::database::DatabaseError;
use crate::persistence::{AttachmentCleanup, AttachmentId, AttachmentRecord, ContentDigest};

/// Why the attachment store refused a step. No message repeats content.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum StoreError {
    #[error("attachment storage is not supported on this platform")]
    Unsupported,
    #[error(
        "the attachment directory is not a private directory of this user reached without a symbolic link"
    )]
    UntrustedDirectory,
    #[error("the attachment directory was replaced, moved or changed since OCG opened it")]
    DirectoryChanged,
    #[error("the content file is missing")]
    Missing,
    #[error("the content file is not a private regular file of this user with a single link")]
    UntrustedFile,
    #[error("the content file does not have the recorded length")]
    LengthMismatch,
    #[error("the content file does not have the recorded SHA-256")]
    DigestMismatch,
    #[error("a different file already has the content's name; it was not replaced")]
    Conflict,
    #[error("attachment storage failed: {0:?}")]
    Io(std::io::ErrorKind),
}

/// Why an Attachment was not created or its content not read.
#[derive(Debug, thiserror::Error)]
pub enum AttachmentError {
    #[error(transparent)]
    Database(#[from] DatabaseError),
    #[error("{0} does not exist")]
    NotFound(AttachmentId),
    #[error("{id} was cleaned up, so its content is no longer available")]
    CleanedUp {
        id: AttachmentId,
        cleanup: AttachmentCleanup,
    },
    #[error("the content of {id} cannot be used: {error}")]
    Content { id: AttachmentId, error: StoreError },
    #[error("the content was not stored: {0}")]
    NotStored(StoreError),
}

/// An available Attachment and its verified content. Its `Debug` shows the
/// content by size only.
#[derive(Clone, PartialEq, Eq)]
pub struct AttachmentContent {
    pub record: AttachmentRecord,
    pub bytes: Vec<u8>,
}

impl std::fmt::Debug for AttachmentContent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AttachmentContent")
            .field("record", &self.record)
            .field("bytes", &self.bytes.len())
            .finish()
    }
}

/// How a staging file's name begins; the rest is three decimal numbers
/// joined by `-`. Never the form of a digest.
const STAGING_PREFIX: &str = ".ocg-staging-";

/// Whether `name` has exactly the form of a staging file's name.
fn is_staging_name(name: &str) -> bool {
    name.strip_prefix(STAGING_PREFIX).is_some_and(|rest| {
        let parts = rest.split('-').collect::<Vec<_>>();
        parts.len() == 3
            && parts
                .iter()
                .all(|part| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit()))
    })
}

#[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
pub(crate) use supported::AttachmentStore;
#[cfg(not(any(target_os = "linux", target_os = "android", target_vendor = "apple")))]
pub(crate) use unsupported::AttachmentStore;

#[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
mod supported {
    use std::io::{Read as _, Write as _};
    use std::os::unix::fs::MetadataExt as _;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use rustix::fs::{AtFlags, FileType, Mode, OFlags, RenameFlags};
    use rustix::io::Errno;

    use super::*;

    /// Names tried for a staging file before staging is given up.
    const STAGING_ATTEMPTS: usize = 8;

    /// A file's identity.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct Identity {
        device: u64,
        inode: u64,
    }

    impl Identity {
        fn of(metadata: &std::fs::Metadata) -> Self {
            Self {
                device: metadata.dev(),
                inode: metadata.ino(),
            }
        }
    }

    fn io(errno: Errno) -> StoreError {
        StoreError::Io(std::io::Error::from(errno).kind())
    }

    fn failed(error: std::io::Error) -> StoreError {
        StoreError::Io(error.kind())
    }

    /// Whether `metadata` is of a file of this user that grants nothing to
    /// anyone else.
    fn is_private(metadata: &std::fs::Metadata) -> bool {
        metadata.uid() == rustix::process::geteuid().as_raw() && metadata.mode() & 0o077 == 0
    }

    /// The attachment directory, held open; see the module documentation.
    #[derive(Debug)]
    pub(crate) struct AttachmentStore {
        root_path: std::path::PathBuf,
        root: Identity,
        name: std::ffi::OsString,
        directory: File,
        identity: Identity,
    }

    impl AttachmentStore {
        /// Opens the directory at `path`, the data directory's
        /// `ocg_attachments`, and checks it.
        pub(crate) fn open(path: &Path) -> Result<Self, StoreError> {
            let (Some(root_path), Some(name)) = (path.parent(), path.file_name()) else {
                return Err(StoreError::UntrustedDirectory);
            };
            let root = open_directory(root_path)?;
            let root_identity = Identity::of(&root.metadata().map_err(failed)?);
            let directory = open_directory_at(&root, name)?;
            let metadata = directory.metadata().map_err(failed)?;
            if !metadata.is_dir() || !is_private(&metadata) {
                return Err(StoreError::UntrustedDirectory);
            }
            Ok(Self {
                root_path: root_path.to_path_buf(),
                root: root_identity,
                name: name.to_owned(),
                identity: Identity::of(&metadata),
                directory,
            })
        }

        /// Checks that the directory held is still the data directory's
        /// attachment directory, and still private.
        fn verify(&self) -> Result<(), StoreError> {
            let identity = |directory: &File| {
                directory
                    .metadata()
                    .ok()
                    .map(|metadata| Identity::of(&metadata))
            };
            let root = open_directory(&self.root_path).map_err(|_| StoreError::DirectoryChanged)?;
            if identity(&root) != Some(self.root) {
                return Err(StoreError::DirectoryChanged);
            }
            let named =
                open_directory_at(&root, &self.name).map_err(|_| StoreError::DirectoryChanged)?;
            if identity(&named) != Some(self.identity) {
                return Err(StoreError::DirectoryChanged);
            }
            if !is_private(&self.directory.metadata().map_err(failed)?) {
                return Err(StoreError::UntrustedDirectory);
            }
            Ok(())
        }

        /// Publishes `content` under its digest, durably, and returns the
        /// digest. Existing content of that digest is reused once its bytes
        /// are verified equal to `content`; nothing is ever replaced.
        pub(crate) fn publish(&self, content: &[u8]) -> Result<ContentDigest, StoreError> {
            let digest = ContentDigest::of(content);
            let name = digest.to_string();
            self.verify()?;
            if self.reuse(&name, content)? {
                return Ok(digest);
            }
            let staged = Staged::new(&self.directory)?;
            staged.fill(content)?;
            match rustix::fs::renameat_with(
                &self.directory,
                staged.name.as_str(),
                &self.directory,
                name.as_str(),
                RenameFlags::NOREPLACE,
            ) {
                Ok(()) => staged.published(),
                // Published meanwhile: the staged copy is not needed.
                Err(Errno::EXIST) => {
                    drop(staged);
                    return if self.reuse(&name, content)? {
                        Ok(digest)
                    } else {
                        Err(StoreError::Conflict)
                    };
                }
                Err(errno) => return Err(io(errno)),
            }
            #[cfg(test)]
            if !publish_seam::reach(publish_seam::Point::Published) {
                return Err(StoreError::Io(std::io::ErrorKind::Other));
            }
            self.directory.sync_all().map_err(failed)?;
            Ok(digest)
        }

        /// Whether a file under `name` already holds `content`, synced with
        /// its directory entry. A missing one does not; any other is a
        /// conflict, and is left as it is.
        fn reuse(&self, name: &str, content: &[u8]) -> Result<bool, StoreError> {
            let (file, bytes) = match self.read_file(name, content.len() as u64) {
                Ok(read) => read,
                Err(StoreError::Missing) => return Ok(false),
                Err(StoreError::LengthMismatch | StoreError::UntrustedFile) => {
                    return Err(StoreError::Conflict);
                }
                Err(error) => return Err(error),
            };
            if bytes != content {
                return Err(StoreError::Conflict);
            }
            file.sync_all().map_err(failed)?;
            self.directory.sync_all().map_err(failed)?;
            Ok(true)
        }

        /// The content named `digest`, verified to be `length` bytes with
        /// that SHA-256.
        pub(crate) fn load(
            &self,
            digest: ContentDigest,
            length: u64,
        ) -> Result<Vec<u8>, StoreError> {
            self.verify()?;
            let (_, bytes) = self.read_file(&digest.to_string(), length)?;
            if ContentDigest::of(&bytes) != digest {
                return Err(StoreError::DigestMismatch);
            }
            Ok(bytes)
        }

        /// The whole of the file `name`, which must be a private regular
        /// file of this user with one link and exactly `length` bytes.
        fn read_file(&self, name: &str, length: u64) -> Result<(File, Vec<u8>), StoreError> {
            let flags = OFlags::RDONLY
                | OFlags::NOFOLLOW
                | OFlags::NOCTTY
                | OFlags::NONBLOCK
                | OFlags::CLOEXEC;
            let file = rustix::fs::openat(&self.directory, name, flags, Mode::empty())
                .map(File::from)
                .map_err(|errno| match errno {
                    Errno::NOENT => StoreError::Missing,
                    Errno::LOOP => StoreError::UntrustedFile,
                    other => io(other),
                })?;
            let metadata = file.metadata().map_err(failed)?;
            if !metadata.is_file() || metadata.nlink() != 1 || !is_private(&metadata) {
                return Err(StoreError::UntrustedFile);
            }
            if metadata.len() != length {
                return Err(StoreError::LengthMismatch);
            }
            let mut bytes = Vec::with_capacity(usize::try_from(length).unwrap_or(0));
            (&file)
                .take(length.saturating_add(1))
                .read_to_end(&mut bytes)
                .map_err(failed)?;
            if bytes.len() as u64 != length {
                return Err(StoreError::LengthMismatch);
            }
            Ok((file, bytes))
        }

        /// Removes the staging files an interrupted publication left, and
        /// nothing else: only regular files whose names have the staging
        /// form. Returns how many were removed. Run once at startup, before
        /// anything is published.
        pub(crate) fn remove_staging_files(&self) -> Result<usize, StoreError> {
            self.verify()?;
            let mut removed = 0;
            for entry in rustix::fs::Dir::read_from(&self.directory).map_err(io)? {
                let entry = entry.map_err(io)?;
                let Some(name) = entry
                    .file_name()
                    .to_str()
                    .ok()
                    .filter(|name| is_staging_name(name))
                else {
                    continue;
                };
                let stat =
                    match rustix::fs::statat(&self.directory, name, AtFlags::SYMLINK_NOFOLLOW) {
                        Ok(stat) => stat,
                        Err(Errno::NOENT) => continue,
                        Err(errno) => return Err(io(errno)),
                    };
                if FileType::from_raw_mode(stat.st_mode) == FileType::RegularFile {
                    rustix::fs::unlinkat(&self.directory, name, AtFlags::empty()).map_err(io)?;
                    removed += 1;
                }
            }
            if removed > 0 {
                self.directory.sync_all().map_err(failed)?;
            }
            Ok(removed)
        }
    }

    fn directory_flags() -> OFlags {
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC
    }

    fn untrusted_directory(errno: Errno) -> StoreError {
        match errno {
            Errno::NOENT => StoreError::Missing,
            Errno::LOOP | Errno::NOTDIR => StoreError::UntrustedDirectory,
            other => io(other),
        }
    }

    /// Opens the directory at `path`, refusing a symbolic link as its last
    /// component.
    fn open_directory(path: &Path) -> Result<File, StoreError> {
        rustix::fs::open(path, directory_flags(), Mode::empty())
            .map(File::from)
            .map_err(untrusted_directory)
    }

    /// Opens the directory `name` in `parent`, refusing a symbolic link.
    fn open_directory_at(parent: &File, name: &std::ffi::OsStr) -> Result<File, StoreError> {
        rustix::fs::openat(parent, name, directory_flags(), Mode::empty())
            .map(File::from)
            .map_err(untrusted_directory)
    }

    /// Content being staged under a hidden name. Removed when dropped
    /// unless it was published, and only while its name still holds it.
    struct Staged<'a> {
        directory: &'a File,
        name: String,
        file: File,
        published: bool,
    }

    impl<'a> Staged<'a> {
        /// A new, empty staging file, created exclusively, never through a
        /// link, readable and writable by its owner alone.
        fn new(directory: &'a File) -> Result<Self, StoreError> {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let flags =
                OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC;
            for _ in 0..STAGING_ATTEMPTS {
                let nanos = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_or(0, |elapsed| elapsed.subsec_nanos());
                let name = format!(
                    "{STAGING_PREFIX}{}-{nanos}-{}",
                    std::process::id(),
                    NEXT.fetch_add(1, Ordering::Relaxed)
                );
                match rustix::fs::openat(directory, name.as_str(), flags, Mode::RUSR | Mode::WUSR) {
                    Ok(file) => {
                        return Ok(Self {
                            directory,
                            name,
                            file: File::from(file),
                            published: false,
                        });
                    }
                    Err(Errno::EXIST) => {}
                    Err(errno) => return Err(io(errno)),
                }
            }
            Err(StoreError::Io(std::io::ErrorKind::AlreadyExists))
        }

        /// Writes the whole content, makes the file private whatever the
        /// umask did, and syncs it.
        fn fill(&self, content: &[u8]) -> Result<(), StoreError> {
            (&self.file).write_all(content).map_err(failed)?;
            rustix::fs::fchmod(&self.file, Mode::RUSR | Mode::WUSR).map_err(io)?;
            self.file.sync_all().map_err(failed)?;
            #[cfg(test)]
            if !publish_seam::reach(publish_seam::Point::Staged) {
                return Err(StoreError::Io(std::io::ErrorKind::Other));
            }
            Ok(())
        }

        fn published(mut self) {
            self.published = true;
        }
    }

    impl Drop for Staged<'_> {
        fn drop(&mut self) {
            if self.published {
                return;
            }
            let in_place = match (
                rustix::fs::fstat(&self.file),
                rustix::fs::statat(
                    self.directory,
                    self.name.as_str(),
                    AtFlags::SYMLINK_NOFOLLOW,
                ),
            ) {
                (Ok(staged), Ok(named)) => {
                    (staged.st_dev, staged.st_ino) == (named.st_dev, named.st_ino)
                }
                _ => false,
            };
            if in_place {
                rustix::fs::unlinkat(self.directory, self.name.as_str(), AtFlags::empty()).ok();
            }
        }
    }
}

#[cfg(not(any(target_os = "linux", target_os = "android", target_vendor = "apple")))]
mod unsupported {
    use super::*;

    /// No attachment storage: every step is refused.
    #[derive(Debug)]
    pub(crate) struct AttachmentStore;

    impl AttachmentStore {
        pub(crate) fn open(_path: &Path) -> Result<Self, StoreError> {
            Ok(Self)
        }

        pub(crate) fn publish(&self, _content: &[u8]) -> Result<ContentDigest, StoreError> {
            Err(StoreError::Unsupported)
        }

        pub(crate) fn load(
            &self,
            _digest: ContentDigest,
            _length: u64,
        ) -> Result<Vec<u8>, StoreError> {
            Err(StoreError::Unsupported)
        }

        pub(crate) fn remove_staging_files(&self) -> Result<usize, StoreError> {
            Ok(0)
        }
    }
}

/// Test seams of a publication: each runs at its point and may make the
/// step there fail.
#[cfg(test)]
pub(crate) mod publish_seam {
    use std::cell::RefCell;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(crate) enum Point {
        /// The content is staged and synced under a staging name.
        Staged,
        /// The content has its digest's name; the directory is not synced.
        Published,
    }

    type Seam = Box<dyn FnMut(Point) -> bool>;

    thread_local! {
        static SEAM: RefCell<Option<Seam>> = const { RefCell::new(None) };
    }

    /// Runs `seam` at every point of this thread's publications; a step
    /// fails when it returns `false`.
    pub(crate) fn install(seam: impl FnMut(Point) -> bool + 'static) {
        SEAM.with(|installed| *installed.borrow_mut() = Some(Box::new(seam)));
    }

    pub(crate) fn clear() {
        SEAM.with(|installed| *installed.borrow_mut() = None);
    }

    #[cfg_attr(
        not(any(target_os = "linux", target_os = "android", target_vendor = "apple")),
        expect(dead_code, reason = "nothing is published on this platform")
    )]
    pub(super) fn reach(point: Point) -> bool {
        let seam = SEAM.with(|installed| installed.borrow_mut().take());
        let Some(mut seam) = seam else {
            return true;
        };
        let proceed = seam(point);
        SEAM.with(|installed| {
            installed.borrow_mut().get_or_insert(seam);
        });
        proceed
    }
}

#[cfg(all(
    test,
    any(target_os = "linux", target_os = "android", target_vendor = "apple")
))]
mod tests {
    use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};
    use std::path::PathBuf;

    use super::publish_seam::Point;
    use super::*;
    use crate::data_directory::DataDirectory;
    use crate::test_support::{TestRoot, data_directory};

    fn store(name: &str) -> (TestRoot, DataDirectory, AttachmentStore) {
        let (root, directory) = data_directory("ocg-attachment-store", name);
        let store = AttachmentStore::open(directory.attachments_path()).expect("open the store");
        (root, directory, store)
    }

    fn content_path(directory: &DataDirectory, digest: ContentDigest) -> PathBuf {
        directory.attachments_path().join(digest.to_string())
    }

    /// The names in the attachment directory, sorted.
    fn names(directory: &DataDirectory) -> Vec<String> {
        let mut names = std::fs::read_dir(directory.attachments_path())
            .expect("list")
            .map(|entry| {
                entry
                    .expect("entry")
                    .file_name()
                    .into_string()
                    .expect("UTF-8")
            })
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    fn mode(path: &Path) -> u32 {
        std::fs::symlink_metadata(path).expect("metadata").mode() & 0o7777
    }

    fn set_mode(path: &Path, mode: u32) {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).expect("chmod");
    }

    #[test]
    fn a_digest_is_the_lowercase_hex_sha256_of_the_content() {
        assert_eq!(
            ContentDigest::of(b"abc").to_string(),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        let digest = ContentDigest::of(b"");
        assert_eq!(ContentDigest::from_bytes(*digest.as_bytes()), digest);
        assert_eq!(digest.to_string().len(), 64);
    }

    #[test]
    fn content_is_published_whole_under_its_digest_and_private() {
        let (_root, directory, store) = store("publish");
        let content = b"\x89PNG\r\n\x1a\nsome image bytes".to_vec();
        let digest = store.publish(&content).expect("publish");
        assert_eq!(digest, ContentDigest::of(&content));
        let path = content_path(&directory, digest);
        assert_eq!(std::fs::read(&path).expect("read"), content);
        assert_eq!(mode(&path), 0o600, "owner-only");
        assert_eq!(mode(directory.attachments_path()), 0o700);
        assert_eq!(
            names(&directory),
            [digest.to_string()],
            "no staging file is left"
        );
        assert_eq!(
            store.load(digest, content.len() as u64).expect("load"),
            content
        );
    }

    #[test]
    fn existing_content_is_reused_only_once_its_bytes_are_verified() {
        let (_root, directory, store) = store("reuse");
        let content = b"shared content".to_vec();
        let digest = store.publish(&content).expect("publish");
        let path = content_path(&directory, digest);
        let inode = std::fs::metadata(&path).expect("metadata").ino();
        assert_eq!(store.publish(&content).expect("publish again"), digest);
        assert_eq!(
            std::fs::metadata(&path).expect("metadata").ino(),
            inode,
            "reused, not rewritten"
        );
        assert_eq!(names(&directory).len(), 1);

        // A file under the digest's name with other bytes is never replaced,
        // whether it has the content's length or not.
        for other in [b"SHARED CONTENT".to_vec(), b"short".to_vec()] {
            std::fs::write(&path, &other).expect("corrupt the file");
            assert_eq!(store.publish(&content), Err(StoreError::Conflict));
            assert_eq!(std::fs::read(&path).expect("read"), other, "left as it was");
            assert_eq!(names(&directory).len(), 1, "no staging file is left");
        }
        // Nor is anything that is not a private regular file.
        std::fs::remove_file(&path).expect("remove");
        std::fs::write(&path, &content).expect("write");
        set_mode(&path, 0o644);
        assert_eq!(store.publish(&content), Err(StoreError::Conflict));
        assert_eq!(mode(&path), 0o644);
    }

    #[test]
    fn content_is_read_only_when_it_is_exactly_what_was_recorded() {
        let (_root, directory, store) = store("load");
        let content = b"recorded content".to_vec();
        let length = content.len() as u64;
        let digest = store.publish(&content).expect("publish");
        let path = content_path(&directory, digest);
        assert_eq!(
            store.load(digest, length + 1),
            Err(StoreError::LengthMismatch)
        );

        std::fs::write(&path, b"RECORDED CONTENT").expect("same length, other bytes");
        assert_eq!(store.load(digest, length), Err(StoreError::DigestMismatch));
        std::fs::write(&path, b"recorded").expect("truncate");
        assert_eq!(store.load(digest, length), Err(StoreError::LengthMismatch));
        std::fs::write(&path, &content).expect("restore");
        set_mode(&path, 0o640);
        assert_eq!(store.load(digest, length), Err(StoreError::UntrustedFile));
        set_mode(&path, 0o600);
        let link = directory.root().join("hard-link");
        std::fs::hard_link(&path, &link).expect("hard link");
        assert_eq!(store.load(digest, length), Err(StoreError::UntrustedFile));
        std::fs::remove_file(&link).expect("remove the link");
        assert_eq!(store.load(digest, length).expect("load"), content);

        std::fs::remove_file(&path).expect("remove");
        let elsewhere = directory.root().join("elsewhere");
        std::fs::write(&elsewhere, &content).expect("write");
        set_mode(&elsewhere, 0o600);
        symlink(&elsewhere, &path).expect("link the content name elsewhere");
        assert_eq!(store.load(digest, length), Err(StoreError::UntrustedFile));
        std::fs::remove_file(&path).expect("remove the link");
        assert_eq!(store.load(digest, length), Err(StoreError::Missing));
    }

    #[test]
    fn only_a_private_directory_reached_without_a_link_is_trusted() {
        let (_root, directory) = data_directory("ocg-attachment-store", "untrusted");
        let path = directory.attachments_path();
        set_mode(path, 0o755);
        assert_eq!(
            AttachmentStore::open(path).err(),
            Some(StoreError::UntrustedDirectory)
        );
        set_mode(path, 0o700);
        let real = directory.root().join("real-attachments");
        std::fs::rename(path, &real).expect("move the directory");
        symlink(&real, path).expect("link the directory");
        assert_eq!(
            AttachmentStore::open(path).err(),
            Some(StoreError::UntrustedDirectory)
        );
        std::fs::remove_file(path).expect("remove the link");
        assert_eq!(AttachmentStore::open(path).err(), Some(StoreError::Missing));
    }

    #[test]
    fn a_directory_replaced_after_it_was_opened_is_refused() {
        let (_root, directory, store) = store("replaced");
        let content = b"content".to_vec();
        let digest = store.publish(&content).expect("publish");
        let path = directory.attachments_path();
        let moved = directory.root().join("moved-attachments");
        std::fs::rename(path, &moved).expect("move the directory away");
        std::fs::create_dir(path).expect("create a replacement");
        set_mode(path, 0o700);
        std::fs::copy(
            moved.join(digest.to_string()),
            path.join(digest.to_string()),
        )
        .expect("copy the content into the replacement");
        assert_eq!(store.publish(b"new"), Err(StoreError::DirectoryChanged));
        assert_eq!(
            store.load(digest, content.len() as u64),
            Err(StoreError::DirectoryChanged)
        );
        assert_eq!(
            names(&directory),
            [digest.to_string()],
            "nothing was written to it"
        );

        std::fs::remove_dir_all(path).expect("remove the replacement");
        symlink(&moved, path).expect("link to the original");
        assert_eq!(
            store.load(digest, content.len() as u64),
            Err(StoreError::DirectoryChanged),
            "not even through a link to the directory held"
        );
        std::fs::remove_file(path).expect("remove the link");
        std::fs::rename(&moved, path).expect("move it back");
        assert_eq!(
            store.load(digest, content.len() as u64).expect("load"),
            content
        );
        set_mode(path, 0o750);
        assert_eq!(store.publish(b"new"), Err(StoreError::UntrustedDirectory));
    }

    #[test]
    fn only_staging_files_are_removed_at_startup() {
        let (_root, directory, store) = store("staging");
        let published = store.publish(b"published").expect("publish");
        let path = directory.attachments_path();
        std::fs::write(path.join(".ocg-staging-1-2-3"), b"partial").expect("write");
        std::fs::write(path.join(".ocg-staging-41-5-6"), b"complete, never renamed")
            .expect("write");
        // Not of the staging form, or not a regular file: all kept.
        let outside = directory.root().join("outside.txt");
        std::fs::write(&outside, b"not OCG's").expect("write");
        symlink(&outside, path.join(".ocg-staging-7-8-9")).expect("link");
        std::fs::create_dir(path.join(".ocg-staging-4-5-6")).expect("directory");
        let kept = [
            ".ocg-staging-1-2",
            ".ocg-staging-1-2-3-4",
            ".ocg-staging-a-2-3",
            ".ocg-staging--2-3",
            "ocg-staging-1-2-3",
            "notes.txt",
        ];
        for name in kept {
            std::fs::write(path.join(name), b"kept").expect("write");
        }

        assert_eq!(store.remove_staging_files().expect("clean up"), 2);
        let mut expected = kept
            .iter()
            .map(|name| (*name).to_owned())
            .chain([
                ".ocg-staging-4-5-6".to_owned(),
                ".ocg-staging-7-8-9".to_owned(),
                published.to_string(),
            ])
            .collect::<Vec<_>>();
        expected.sort();
        assert_eq!(names(&directory), expected);
        assert_eq!(std::fs::read(&outside).expect("read"), b"not OCG's");
        assert_eq!(store.remove_staging_files().expect("again"), 0);
        assert_eq!(names(&directory), expected, "removal is idempotent");
    }

    #[test]
    fn staging_names_never_take_the_form_of_a_digest() {
        assert!(is_staging_name(".ocg-staging-1-2-3"));
        assert!(is_staging_name(".ocg-staging-123-456789-0"));
        for name in [
            ".ocg-staging-",
            ".ocg-staging-1-2",
            ".ocg-staging-1-2-3-",
            ".ocg-staging-1-2-x",
            ".ocg-staging-1--3",
            &ContentDigest::of(b"x").to_string(),
        ] {
            assert!(!is_staging_name(name), "{name}");
        }
    }

    #[test]
    fn a_failed_publication_leaves_no_staging_file() {
        let (_root, directory, store) = store("failed");
        publish_seam::install(|point| point != Point::Staged);
        assert!(matches!(
            store.publish(b"never published"),
            Err(StoreError::Io(_))
        ));
        publish_seam::clear();
        assert!(names(&directory).is_empty());

        // The rename happened; only the directory sync did not, so the
        // published file stays, as after a crash at that point.
        publish_seam::install(|point| point != Point::Published);
        assert!(matches!(
            store.publish(b"published"),
            Err(StoreError::Io(_))
        ));
        publish_seam::clear();
        assert_eq!(
            names(&directory),
            [ContentDigest::of(b"published").to_string()]
        );
    }
}
