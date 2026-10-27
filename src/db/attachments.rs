//! The content of Attachments: immutable files in `ocg_attachments/`, each
//! named by the lowercase hexadecimal SHA-256 of its bytes, never by a name
//! an Attachment was given. An Attachment is a database record with an
//! identity of its own, see [`crate::db::entities::AttachmentRecord`];
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
//! A cleanup ends an Attachment's availability in its record first; its
//! content file is reclaimed afterwards, and only once no available
//! Attachment names the same content and no publication of it is in
//! progress. A publication pins its digest, see [`ContentPin`], before it
//! touches the directory, until whether its record committed is known. A
//! reclamation holds the pins' lock from its check of the pins and of the
//! committed records until the file is gone and the directory synced, so a
//! publication either pinned first, and the content is kept, or waits for
//! the reclamation and then publishes the content anew. A file is removed
//! only when it is a private regular file of this user with one link and
//! exactly the recorded length and SHA-256, still under its name. Removing a
//! file is not erasure: copies in backups, snapshots or freed blocks remain.
//! Reclamation that did not finish is retried by a later cleanup of an
//! Attachment of that content and by the next start of the database, see
//! [`crate::db::DatabaseHandle::reclaimed_at_open`]; neither touches
//! a file no cleanup authorized, such as content no record names.
//!
//! What this does not establish: the root's ancestors are resolved by path;
//! the checks guard against mistakes and other users, not against code
//! running as this user, which can change any file between a check and its
//! use (a changed file is still refused when it is read, by its digest); and
//! durability is what `fsync` gives.

use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::{Mutex, MutexGuard, PoisonError};

use crate::db::entities::{AttachmentCleanup, AttachmentId, AttachmentRecord, ContentDigest};
use crate::db::{DatabaseError, DatabaseHandle};
use crate::service::ai::providers::openai_compatible::image::{
    MAX_IMAGE_BYTES, MAX_REQUEST_IMAGE_BYTES, MAX_REQUEST_IMAGES,
};
use crate::service::ai::providers::openai_compatible::{ImageError, ImageInput, ImageMime};

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

/// What became of an Attachment's content file when a cleanup tried to
/// reclaim it. Removal is never erasure, see [`crate::db::attachments`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reclamation {
    /// The file was verified, removed, and the directory synced.
    Removed,
    /// No file had the content's name; the directory was synced, so that
    /// absence is durable.
    Absent,
    /// The file was kept: another available Attachment names the same
    /// content, or a publication of it is in progress.
    Retained,
}

/// Why a content file was not reclaimed. A later cleanup of an Attachment
/// of that content, or the next start of the database, tries again.
#[derive(Debug, thiserror::Error)]
pub enum ReclamationError {
    #[error("whether other Attachments still need the content could not be read: {0}")]
    Database(DatabaseError),
    #[error("the content file was not removed: {0}")]
    Store(StoreError),
}

/// What a cleanup of an Attachment did. The cleanup of the record and the
/// reclamation of the content are separate facts: the first is durable
/// whatever became of the second.
#[derive(Debug)]
pub struct AttachmentCleanupReport {
    /// The Attachment as it is recorded now: its original metadata, with the
    /// cleanup that applies to it, this call's or an earlier one.
    pub record: AttachmentRecord,
    /// Whether this call recorded the cleanup. `false` when the Attachment
    /// was already cleaned up; that cleanup, its kind and time, is kept.
    pub recorded: bool,
    /// What became of the content file.
    pub content: Result<Reclamation, ReclamationError>,
}

/// The publications in progress, by digest, see [`ContentPin`]. Its lock is
/// also held through a whole reclamation.
#[derive(Debug, Default)]
struct Pins(Mutex<HashMap<ContentDigest, usize>>);

impl Pins {
    fn lock(&self) -> MutexGuard<'_, HashMap<ContentDigest, usize>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Pins `digest`; waits while a reclamation holds the lock.
    fn pin(&self, digest: ContentDigest) -> ContentPin<'_> {
        *self.lock().entry(digest).or_default() += 1;
        ContentPin { pins: self, digest }
    }
}

/// Content published for a record that has not committed yet: as long as
/// it exists, the content is never reclaimed. Released once whether the
/// record committed is known; dropped without that, the content stays
/// pinned for the rest of this process run, because a record whose commit
/// is unknown may still name it.
#[derive(Debug)]
pub(crate) struct ContentPin<'a> {
    pins: &'a Pins,
    digest: ContentDigest,
}

impl ContentPin<'_> {
    pub(crate) fn digest(&self) -> ContentDigest {
        self.digest
    }

    /// Ends the pin: the record committed, and now names the content
    /// itself, or it did not commit and never will.
    pub(crate) fn release(self) {
        let mut pins = self.pins.lock();
        if let Some(count) = pins.get_mut(&self.digest) {
            *count -= 1;
            if *count == 0 {
                pins.remove(&self.digest);
            }
        }
    }
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
        pins: Pins,
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
                pins: Pins::default(),
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
        /// digest pinned, see [`ContentPin`]. Existing content of that
        /// digest is reused once its bytes are verified equal to `content`;
        /// nothing is ever replaced. The digest is pinned before anything
        /// is looked at, after any reclamation of it in progress.
        pub(crate) fn publish(&self, content: &[u8]) -> Result<ContentPin<'_>, StoreError> {
            let pin = self.pins.pin(ContentDigest::of(content));
            match self.publish_pinned(pin.digest(), content) {
                Ok(()) => Ok(pin),
                Err(error) => {
                    pin.release();
                    Err(error)
                }
            }
        }

        fn publish_pinned(&self, digest: ContentDigest, content: &[u8]) -> Result<(), StoreError> {
            let name = digest.to_string();
            self.verify()?;
            if self.reuse(&name, content)? {
                return Ok(());
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
                        Ok(())
                    } else {
                        Err(StoreError::Conflict)
                    };
                }
                Err(errno) => return Err(io(errno)),
            }

            self.directory.sync_all().map_err(failed)?;
            Ok(())
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

        /// Reclaims the content named `digest`, of `length` bytes, which a
        /// committed cleanup no longer lets one Attachment use: it is kept
        /// while a publication of it is pinned or while `needed` says that
        /// an available Attachment names it; otherwise it is removed, see
        /// [`Self::remove`], or found absent and the directory synced. The
        /// pins' lock is held throughout, so no publication of the content
        /// begins between `needed` and the removal; `needed` must read the
        /// committed records within that time. Its error is returned as it
        /// is, and nothing is removed.
        pub(crate) fn reclaim<E>(
            &self,
            digest: ContentDigest,
            length: u64,
            needed: impl FnOnce() -> Result<bool, E>,
        ) -> Result<Result<Reclamation, StoreError>, E> {
            let pins = self.pins.lock();
            if pins.contains_key(&digest) || needed()? {
                return Ok(Ok(Reclamation::Retained));
            }
            let removed = self.remove(digest, length).and_then(|reclaimed| {
                if reclaimed == Reclamation::Absent {
                    self.directory.sync_all().map_err(failed)?;
                }
                Ok(reclaimed)
            });
            drop(pins);
            Ok(removed)
        }

        /// Reclaims, when the database opens, before anything is published,
        /// the content that committed cleanups authorize and no available
        /// Attachment needs: `unneeded`, digests with their lengths. Returns
        /// what became of each file that was found; the absence of the
        /// others is made durable by one sync of the directory.
        pub(crate) fn reclaim_unneeded(
            &self,
            unneeded: &[(ContentDigest, u64)],
        ) -> Vec<(ContentDigest, Result<Reclamation, StoreError>)> {
            let _pins = self.pins.lock();
            let mut found = Vec::new();
            let mut absent = Vec::new();
            for &(digest, length) in unneeded {
                match self.remove(digest, length) {
                    Ok(Reclamation::Absent) => absent.push(digest),
                    removed => found.push((digest, removed)),
                }
            }
            if !absent.is_empty()
                && let Err(error) = self.directory.sync_all()
            {
                found.extend(
                    absent
                        .into_iter()
                        .map(|digest| (digest, Err(StoreError::Io(error.kind())))),
                );
            }
            found
        }

        /// Removes the file named `digest` when it is exactly the recorded
        /// content: a private regular file of this user with one link,
        /// `length` bytes and that SHA-256, and still under its name when it
        /// is removed. [`Reclamation::Absent`] when there is none; the
        /// directory is synced only after a removal. Any other file is left
        /// as it is.
        fn remove(&self, digest: ContentDigest, length: u64) -> Result<Reclamation, StoreError> {
            self.verify()?;
            let name = digest.to_string();
            let file = match self.read_file(&name, length) {
                Ok((file, bytes)) if ContentDigest::of(&bytes) == digest => file,
                Ok(_) => return Err(StoreError::DigestMismatch),
                Err(StoreError::Missing) => return Ok(Reclamation::Absent),
                Err(error) => return Err(error),
            };
            let held = rustix::fs::fstat(&file).map_err(io)?;
            let named =
                match rustix::fs::statat(&self.directory, name.as_str(), AtFlags::SYMLINK_NOFOLLOW)
                {
                    Ok(named) => named,
                    Err(Errno::NOENT) => return Ok(Reclamation::Absent),
                    Err(errno) => return Err(io(errno)),
                };
            if (held.st_dev, held.st_ino) != (named.st_dev, named.st_ino) {
                return Err(StoreError::Conflict);
            }

            match rustix::fs::unlinkat(&self.directory, name.as_str(), AtFlags::empty()) {
                Ok(()) => {}
                Err(Errno::NOENT) => return Ok(Reclamation::Absent),
                Err(errno) => return Err(io(errno)),
            }

            self.directory.sync_all().map_err(failed)?;
            Ok(Reclamation::Removed)
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

        pub(crate) fn publish(&self, _content: &[u8]) -> Result<ContentPin<'_>, StoreError> {
            Err(StoreError::Unsupported)
        }

        pub(crate) fn reclaim<E>(
            &self,
            _digest: ContentDigest,
            _length: u64,
            _needed: impl FnOnce() -> Result<bool, E>,
        ) -> Result<Result<Reclamation, StoreError>, E> {
            Ok(Err(StoreError::Unsupported))
        }

        pub(crate) fn reclaim_unneeded(
            &self,
            _unneeded: &[(ContentDigest, u64)],
        ) -> Vec<(ContentDigest, Result<Reclamation, StoreError>)> {
            Vec::new()
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

/// Why an input's Attachments cannot be sent as images.
#[derive(Debug, thiserror::Error)]
pub enum AttachmentImageError {
    #[error(transparent)]
    Attachment(#[from] AttachmentError),
    #[error("a request may carry at most {MAX_REQUEST_IMAGES} images")]
    TooMany,
    #[error("{attachment} cannot be sent as an image: {error}")]
    NotAnImage {
        attachment: AttachmentId,
        error: ImageError,
    },
    #[error("the images may not exceed {MAX_REQUEST_IMAGE_BYTES} bytes together")]
    TooLarge,
}

/// The images of `attachments`, in order. Each Attachment must exist, be
/// available and be a PNG or JPEG within the image limits, as its record
/// says, before any content is read; then its content is read from the
/// attachment store, verified against the record's length and SHA-256, and
/// checked again as the image its record declares. Nothing is substituted
/// for an Attachment that fails, and nothing is skipped.
///
/// # Errors
///
/// Returns the first Attachment, or the limit, that fails.
pub async fn attachment_images(
    database: &DatabaseHandle,
    attachments: &[AttachmentId],
) -> Result<Vec<ImageInput>, AttachmentImageError> {
    if attachments.len() > MAX_REQUEST_IMAGES {
        return Err(AttachmentImageError::TooMany);
    }
    let mut declared = Vec::with_capacity(attachments.len());
    let mut total = 0u64;
    for &attachment in attachments {
        let record = database
            .attachment(attachment)
            .await
            .map_err(AttachmentError::Database)?
            .ok_or(AttachmentError::NotFound(attachment))?;
        if let Some(cleanup) = record.cleanup {
            return Err(AttachmentError::CleanedUp {
                id: attachment,
                cleanup,
            }
            .into());
        }
        let not_an_image = |error| AttachmentImageError::NotAnImage { attachment, error };
        let mime =
            ImageMime::from_mime(&record.mime).ok_or(not_an_image(ImageError::UnsupportedType))?;
        if record.byte_length > MAX_IMAGE_BYTES as u64 {
            return Err(not_an_image(ImageError::TooLarge));
        }
        total += record.byte_length;
        declared.push((attachment, mime));
    }
    if total > MAX_REQUEST_IMAGE_BYTES as u64 {
        return Err(AttachmentImageError::TooLarge);
    }
    let mut images = Vec::with_capacity(declared.len());
    for (attachment, mime) in declared {
        let content = database.attachment_content(attachment).await?;
        images.push(
            ImageInput::new(mime, content.bytes)
                .map_err(|error| AttachmentImageError::NotAnImage { attachment, error })?,
        );
    }
    Ok(images)
}
