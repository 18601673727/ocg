use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use crate::db::entities::{AttachmentCleanup, AttachmentId, AttachmentRecord, ContentDigest};
use crate::db::{DatabaseError, DatabaseHandle};
use crate::service::ai::providers::openai_compatible::image::{
    MAX_IMAGE_BYTES, MAX_REQUEST_IMAGE_BYTES, MAX_REQUEST_IMAGES,
};
use crate::service::ai::providers::openai_compatible::{ImageError, ImageInput, ImageMime};

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reclamation {
    Removed,
    Absent,
    Retained,
}

#[derive(Debug, thiserror::Error)]
pub enum ReclamationError {
    #[error("whether other Attachments still need the content could not be read: {0}")]
    Database(DatabaseError),
    #[error("the content file was not removed: {0}")]
    Store(StoreError),
}

#[derive(Debug)]
pub struct AttachmentCleanupReport {
    pub record: AttachmentRecord,
    pub recorded: bool,
    pub content: Result<Reclamation, ReclamationError>,
}

#[derive(Debug, Default)]
struct Pins(Mutex<HashMap<ContentDigest, usize>>);

impl Pins {
    fn lock(&self) -> MutexGuard<'_, HashMap<ContentDigest, usize>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn pin(self: &Arc<Self>, digest: ContentDigest) -> ContentPin {
        *self.lock().entry(digest).or_default() += 1;
        ContentPin {
            pins: Arc::clone(self),
            digest,
        }
    }
}

#[derive(Debug)]
pub(crate) struct ContentPin {
    pins: Arc<Pins>,
    digest: ContentDigest,
}

impl ContentPin {
    pub(crate) fn digest(&self) -> ContentDigest {
        self.digest
    }

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

const STAGING_PREFIX: &str = ".ocg-staging-";

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

    const STAGING_ATTEMPTS: usize = 8;

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

    fn is_private(metadata: &std::fs::Metadata) -> bool {
        metadata.uid() == rustix::process::geteuid().as_raw() && metadata.mode() & 0o077 == 0
    }

    #[derive(Debug)]
    pub(crate) struct AttachmentStore {
        root_path: std::path::PathBuf,
        root: Identity,
        name: std::ffi::OsString,
        directory: File,
        identity: Identity,
        pins: Arc<Pins>,
    }

    impl AttachmentStore {
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
                pins: Arc::new(Pins::default()),
            })
        }

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

        pub(crate) fn publish(&self, content: &[u8]) -> Result<ContentPin, StoreError> {
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

    fn open_directory(path: &Path) -> Result<File, StoreError> {
        rustix::fs::open(path, directory_flags(), Mode::empty())
            .map(File::from)
            .map_err(untrusted_directory)
    }

    fn open_directory_at(parent: &File, name: &std::ffi::OsStr) -> Result<File, StoreError> {
        rustix::fs::openat(parent, name, directory_flags(), Mode::empty())
            .map(File::from)
            .map_err(untrusted_directory)
    }

    struct Staged<'a> {
        directory: &'a File,
        name: String,
        file: File,
        published: bool,
    }

    impl<'a> Staged<'a> {
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

    #[derive(Debug)]
    pub(crate) struct AttachmentStore;

    impl AttachmentStore {
        pub(crate) fn open(_path: &Path) -> Result<Self, StoreError> {
            Ok(Self)
        }

        pub(crate) fn publish(&self, _content: &[u8]) -> Result<ContentPin, StoreError> {
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
