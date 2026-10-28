//! `workspace_write_file`: OCG's one tool that changes a file. It creates one
//! small UTF-8 text file inside a Workspace the operator authorized, or
//! replaces the whole content of one whose entire current content the call
//! names exactly, and nothing else: it appends, patches, deletes, renames and
//! creates directories never.
//!
//! The Workspace, its path rules and the walk to the file are those of
//! `workspace_read_file`, see [`crate::system::executor::workspace_read`]: the
//! root is the operator's, a call names only a plain relative path, no
//! symbolic link is followed, and hidden, secret-bearing and OCG-internal
//! names are refused, wherever they appear.
//!
//! New content is never written into the target itself. It is staged in a
//! new hidden file beside the target, created exclusively (mode 0600, never
//! through a link), written whole and synced; only then is it renamed onto
//! the target's name, and the directory is synced. The target's name so
//! always holds either its previous content or the complete new one, never
//! part of it.
//!
//! `create` publishes with an exclusive rename (`RENAME_NOREPLACE`, Apple's
//! `RENAME_EXCL`): when anything exists at the name, a file that appeared
//! meanwhile included, nothing is replaced and the call fails. The created
//! file keeps the staged file's mode, 0600 under the process umask.
//!
//! `update` opens the existing file without following a link and requires a
//! regular file with a single link, within the read size limit. It reads the
//! whole file, refuses it unless its content equals `expected_content`
//! exactly, and refuses it when it changed while it was read. The staged
//! file gets the target's permission bits (never set-id or sticky ones) and
//! group; a file of another owner is refused, since a replacement would
//! change its owner. Immediately before the rename, the target is checked
//! again, through the file read and by its name: the same file, links, size,
//! modification and status-change times. Extended attributes and ACLs are
//! not carried over.
//!
//! What this does not establish. An update is a check followed by an
//! ordinary rename, not an atomic compare-and-swap: a change another process
//! makes after the last check, or through a descriptor it already holds,
//! is lost or left on the replaced file, and a change that keeps a file's
//! size and both times is not seen. Durability is what `fsync` gives (a
//! full flush on Apple platforms); a crash leaves the previous or the new
//! content under the name. A crash, or a failed cleanup, may leave a staged
//! file named `.ocg-write-…` in the target's directory: nothing removes it
//! later, and as a hidden name no Workspace tool reads or writes it. The
//! Workspace limits of the read tool apply. This is for a trusted, local
//! Workspace of a single operator.
//!
//! Every call is recorded, with its arguments, before it runs: the path, the
//! new content and an update's expected content are kept in plaintext tool
//! evidence. No message this tool reports repeats a path or any content.

use std::fs::File;
use std::future::Future;
use std::io::Write as _;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rustix::fs::{AtFlags, FileType, Gid, Mode, OFlags, RenameFlags, Stat};
use rustix::io::Errno;
use serde_json::{Value, json};

use super::workspace_read::{
    self, Refusal as PathRefusal, Step, Workspace, open_at, open_within, read_bytes, read_whole,
};
use crate::db::entities::{ContentDigest, JobId, ToolCallRef, ToolEffect};
use crate::system::executor::blocking::{self, Kind, StopSignal};
use crate::system::executor::tools::{
    ToolFailure, ToolFuture, ToolHandler, ToolRegistry, ToolRegistryError,
};

/// The name the tool is offered and recorded under.
pub const TOOL_NAME: &str = "workspace_write_file";

/// The largest content, expected_content, old or new a call may carry.
const MAX_CONTENT_BYTES: usize = 8 * 1024;

/// Identifies this handler's behavior in every call it records.
const HANDLER_VERSION: &str = "workspace_write_file-3";

/// The largest file a delete may read and delete.
const MAX_DELETE_BYTES: usize = 64 * 1024;

/// The largest file a replace may read and replace within.
const MAX_REPLACE_FILE_BYTES: usize = 1024 * 1024;

/// How a staged file's name begins. The leading dot keeps it out of reach
/// of every Workspace tool.
const STAGING_PREFIX: &str = ".ocg-write-";

/// Names tried for a staged file before staging is given up.
const STAGING_ATTEMPTS: usize = 8;

/// Why a call changed nothing in the Workspace. Its message is recorded and
/// may reach the Provider, so it never repeats the path or any content.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
enum Refusal {
    #[error("the arguments are invalid for the requested operation")]
    InvalidArguments,
    #[error("content, expected_content, old or new is larger than {MAX_CONTENT_BYTES} bytes")]
    TooLarge,
    #[error("content, expected_content, old or new contains a NUL character")]
    NotText,
    #[error(transparent)]
    Path(#[from] PathRefusal),
    #[error("a directory of the path does not exist")]
    MissingDirectory,
    #[error("something already exists at the path")]
    Exists,
    #[error("the file's current content differs from expected_content")]
    Mismatch,
    #[error("the file changed before it could be replaced")]
    Changed,
    #[error("the file belongs to another user")]
    OtherOwner,
    #[error("the file's group could not be kept")]
    GroupNotKept,
    #[error("the new content could not be staged")]
    NotStaged,
    #[error("the new content could not be published")]
    NotPublished,
    #[error("found {0} occurrences of `old`, expected 1")]
    Occurrences(usize),
    #[error("the file is over 64 KiB; ask the user to delete it")]
    FileTooLargeForDelete,
    #[error("the file could not be deleted")]
    NotDeleted,
    #[error("the call was stopped before it changed anything")]
    Stopped,
}

/// Why what a call did to the Workspace is not known, or not all known.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
enum Uncertainty {
    #[error("a staged copy of the new content could not be removed")]
    StagingLeft,
    #[error("whether the new content was published could not be established")]
    Unknown,
    #[error("the new content was published, but its durability could not be confirmed")]
    NotDurable,
    #[error("the file was deleted, but its durability could not be confirmed")]
    DeleteNotDurable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Failure {
    Refused(Refusal),
    Uncertain(Uncertainty),
}

impl From<Refusal> for Failure {
    fn from(refusal: Refusal) -> Self {
        Self::Refused(refusal)
    }
}

impl From<PathRefusal> for Failure {
    fn from(refusal: PathRefusal) -> Self {
        Self::Refused(Refusal::Path(refusal))
    }
}

impl From<Uncertainty> for Failure {
    fn from(uncertainty: Uncertainty) -> Self {
        Self::Uncertain(uncertainty)
    }
}

impl From<Failure> for ToolFailure {
    fn from(failure: Failure) -> Self {
        match failure {
            Failure::Refused(refusal) => Self::not_applied(refusal.to_string()),
            Failure::Uncertain(uncertainty) => Self::uncertain(uncertainty.to_string()),
        }
    }
}

/// What a call asks for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Operation<'a> {
    Create { content: &'a str },
    Update { content: &'a str, expected: &'a str },
    Replace { old: &'a str, new: &'a str },
    Delete { expected: &'a str },
}

/// A call's arguments, checked. The schema already enforces their shape;
/// they are checked again here, so nothing malformed can become a write.
fn parse(arguments: &Value) -> Result<(&str, Operation<'_>), Refusal> {
    let object = arguments.as_object().ok_or(Refusal::InvalidArguments)?;
    let text = |key: &str| {
        object
            .get(key)
            .map(|value| value.as_str().ok_or(Refusal::InvalidArguments))
            .transpose()
    };
    let (Some(operation), Some(path)) = (text("operation")?, text("path")?) else {
        return Err(Refusal::InvalidArguments);
    };
    let operation = match operation {
        "create" => {
            if object.len() != 3 {
                return Err(Refusal::InvalidArguments);
            }
            let Some(content) = text("content")? else {
                return Err(Refusal::InvalidArguments);
            };
            if content.len() > MAX_CONTENT_BYTES {
                return Err(Refusal::TooLarge);
            }
            if content.contains('\0') {
                return Err(Refusal::NotText);
            }
            Operation::Create { content }
        }
        "update" => {
            if object.len() != 4 {
                return Err(Refusal::InvalidArguments);
            }
            let (Some(content), Some(expected)) = (text("content")?, text("expected_content")?)
            else {
                return Err(Refusal::InvalidArguments);
            };
            for t in [content, expected] {
                if t.len() > MAX_CONTENT_BYTES {
                    return Err(Refusal::TooLarge);
                }
                if t.contains('\0') {
                    return Err(Refusal::NotText);
                }
            }
            Operation::Update { content, expected }
        }
        "replace" => {
            if object.len() != 4 {
                return Err(Refusal::InvalidArguments);
            }
            let (Some(old), Some(new)) = (text("old")?, text("new")?) else {
                return Err(Refusal::InvalidArguments);
            };
            if old.is_empty() {
                return Err(Refusal::InvalidArguments);
            }
            for t in [old, new] {
                if t.len() > MAX_CONTENT_BYTES {
                    return Err(Refusal::TooLarge);
                }
                if t.contains('\0') {
                    return Err(Refusal::NotText);
                }
            }
            Operation::Replace { old, new }
        }
        "delete" => {
            if object.len() != 3 {
                return Err(Refusal::InvalidArguments);
            }
            let Some(expected) = text("expected_content")? else {
                return Err(Refusal::InvalidArguments);
            };
            if expected.len() > MAX_DELETE_BYTES {
                return Err(Refusal::FileTooLargeForDelete);
            }
            if expected.contains('\0') {
                return Err(Refusal::NotText);
            }
            Operation::Delete { expected }
        }
        _ => return Err(Refusal::InvalidArguments),
    };
    Ok((path, operation))
}

/// The directory holding the file at `path`, and the file's name in it. A
/// missing directory on the way is never created.
fn parent<'a>(workspace: &Workspace, path: &'a str) -> Result<(File, &'a str), Refusal> {
    workspace.parent(path).map_err(|refusal| match refusal {
        PathRefusal::NotFound => Refusal::MissingDirectory,
        other => Refusal::Path(other),
    })
}

/// Targets with a mutation in progress, by directory identity and name.
static TARGETS: std::sync::Mutex<std::collections::BTreeSet<(u64, u64, String)>> =
    std::sync::Mutex::new(std::collections::BTreeSet::new());
static TARGETS_FREED: std::sync::Condvar = std::sync::Condvar::new();

/// Exclusive hold on one target while it is checked and changed, so two
/// calls cannot both publish over the version each of them checked.
struct TargetLock(Option<(u64, u64, String)>);

impl TargetLock {
    /// Waits for the target of `name` in `directory`, giving up when `stop`
    /// is requested.
    fn acquire(directory: &File, name: &str, stop: &StopSignal) -> Result<Self, Failure> {
        let stat = rustix::fs::fstat(directory).map_err(|_| PathRefusal::Unreadable)?;
        let key = (stat.st_dev as u64, stat.st_ino as u64, name.to_owned());
        let mut held = TARGETS.lock().unwrap_or_else(PoisonError::into_inner);
        loop {
            if stop.requested() {
                return Err(Refusal::Stopped.into());
            }
            if !held.contains(&key) {
                held.insert(key.clone());
                return Ok(Self(Some(key)));
            }
            held = TARGETS_FREED
                .wait_timeout(held, Duration::from_millis(10))
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
    }
}

impl Drop for TargetLock {
    fn drop(&mut self) {
        if let Some(key) = self.0.take() {
            TARGETS
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&key);
            TARGETS_FREED.notify_all();
        }
    }
}

/// Creates the file at `path` with `content`, when nothing exists there.
fn create(
    workspace: &Workspace,
    path: &str,
    content: &str,
    stop: &StopSignal,
) -> Result<(), Failure> {
    let (directory, name) = parent(workspace, path)?;
    let _target = TargetLock::acquire(&directory, name, stop)?;
    // The exclusive rename is what refuses an existing target; this only
    // names what was found.
    match rustix::fs::statat(&directory, name, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(stat) if FileType::from_raw_mode(stat.st_mode) == FileType::Symlink => {
            return Err(PathRefusal::SymbolicLink.into());
        }
        Ok(_) => return Err(Refusal::Exists.into()),
        Err(Errno::NOENT) => {}
        Err(errno) => return Err(PathRefusal::from_errno(errno).into()),
    }
    let staged = Staged::new(&directory, content, None)?;
    staged.publish(name, Publication::Exclusive, stop)
}

/// Replaces the content of the file at `path` with `content`, when its whole
/// current content is `expected`.
fn update(
    workspace: &Workspace,
    path: &str,
    expected: &str,
    content: &str,
    stop: &StopSignal,
) -> Result<(), Failure> {
    let (directory, name) = parent(workspace, path)?;
    let _target = TargetLock::acquire(&directory, name, stop)?;
    let file = open_at(&directory, name, Step::File)?;
    let before = rustix::fs::fstat(&file).map_err(|_| PathRefusal::Unreadable)?;
    let current = read_whole(&file)?;
    let read = rustix::fs::fstat(&file).map_err(|_| PathRefusal::Unreadable)?;
    if version(&before) != version(&read) {
        return Err(PathRefusal::Changed.into());
    }
    if current != expected {
        return Err(Refusal::Mismatch.into());
    }
    let staged = Staged::new(&directory, content, Some(&read))?;
    staged.publish(
        name,
        Publication::Replace {
            file: &file,
            read: &read,
        },
        stop,
    )
}

/// Replaces a single occurrence of `old` with `new` in the file at `path`.
fn replace(
    workspace: &Workspace,
    path: &str,
    old: &str,
    new: &str,
    stop: &StopSignal,
) -> Result<(usize, String, String), Failure> {
    let (directory, name) = parent(workspace, path)?;
    let _target = TargetLock::acquire(&directory, name, stop)?;
    let file = open_within(&directory, name, Step::File, MAX_REPLACE_FILE_BYTES)?;
    let before = rustix::fs::fstat(&file).map_err(|_| PathRefusal::Unreadable)?;
    let current_bytes = read_bytes(&file, MAX_REPLACE_FILE_BYTES)?;
    let read = rustix::fs::fstat(&file).map_err(|_| PathRefusal::Unreadable)?;
    if version(&before) != version(&read) {
        return Err(PathRefusal::Changed.into());
    }
    let current = String::from_utf8(current_bytes).map_err(|_| Refusal::NotText)?;
    if current.contains('\0') {
        return Err(Refusal::NotText.into());
    }
    let old_bytes = old.as_bytes();
    let current_bytes = current.as_bytes();
    let mut count = 0;
    let mut offset = 0;
    let finder = memchr::memmem::Finder::new(old_bytes);
    let mut search_start = 0;
    while search_start <= current_bytes.len() {
        if let Some(hit) = finder.find(&current_bytes[search_start..]) {
            count += 1;
            offset = search_start + hit;
            search_start = offset + 1;
        } else {
            break;
        }
    }
    if count != 1 {
        return Err(Refusal::Occurrences(count).into());
    }
    let mut new_content = Vec::with_capacity(current_bytes.len() - old_bytes.len() + new.len());
    new_content.extend_from_slice(&current_bytes[..offset]);
    new_content.extend_from_slice(new.as_bytes());
    new_content.extend_from_slice(&current_bytes[offset + old_bytes.len()..]);
    let new_content_str = String::from_utf8(new_content).map_err(|_| Refusal::NotText)?;

    let before_sha256 = ContentDigest::of(current_bytes).to_string();
    let after_sha256 = ContentDigest::of(new_content_str.as_bytes()).to_string();

    let staged = Staged::new(&directory, &new_content_str, Some(&read))?;
    staged.publish(
        name,
        Publication::Replace {
            file: &file,
            read: &read,
        },
        stop,
    )?;
    Ok((offset, before_sha256, after_sha256))
}

/// Deletes the regular file at `path`, when its whole current content is `expected`.
fn delete(
    workspace: &Workspace,
    path: &str,
    expected: &str,
    stop: &StopSignal,
) -> Result<(), Failure> {
    let (directory, name) = parent(workspace, path)?;
    let _target = TargetLock::acquire(&directory, name, stop)?;
    let initial_stat =
        rustix::fs::statat(&directory, name, AtFlags::SYMLINK_NOFOLLOW).map_err(|e| match e {
            Errno::NOENT => PathRefusal::NotFound.into(),
            other => Failure::Refused(PathRefusal::from_errno(other).into()),
        })?;
    if FileType::from_raw_mode(initial_stat.st_mode) == FileType::Symlink {
        return Err(PathRefusal::SymbolicLink.into());
    }
    if FileType::from_raw_mode(initial_stat.st_mode) == FileType::Directory {
        return Err(PathRefusal::NotRegularFile.into());
    }
    if initial_stat.st_size > MAX_DELETE_BYTES as i64 {
        return Err(Refusal::FileTooLargeForDelete.into());
    }
    let file = open_within(&directory, name, Step::File, MAX_DELETE_BYTES)?;
    let before = rustix::fs::fstat(&file).map_err(|_| PathRefusal::Unreadable)?;
    if FileType::from_raw_mode(before.st_mode) != FileType::RegularFile {
        return Err(PathRefusal::NotRegularFile.into());
    }
    if before.st_nlink != 1 {
        return Err(PathRefusal::HardLinked.into());
    }
    if before.st_size > MAX_DELETE_BYTES as i64 {
        return Err(Refusal::FileTooLargeForDelete.into());
    }
    let current_bytes = read_bytes(&file, MAX_DELETE_BYTES)?;
    let read = rustix::fs::fstat(&file).map_err(|_| PathRefusal::Unreadable)?;
    if version(&before) != version(&read) {
        return Err(PathRefusal::Changed.into());
    }
    let current = String::from_utf8(current_bytes).map_err(|_| Refusal::NotText)?;
    if current.contains('\0') {
        return Err(Refusal::NotText.into());
    }
    if current != expected {
        return Err(Refusal::Mismatch.into());
    }
    let after = rustix::fs::statat(&directory, name, AtFlags::SYMLINK_NOFOLLOW)
        .map_err(|_| Refusal::Changed)?;
    if version(&read) != version(&after) {
        return Err(Refusal::Changed.into());
    }
    if stop.requested() {
        return Err(Refusal::Stopped.into());
    }
    rustix::fs::unlinkat(&directory, name, AtFlags::empty()).map_err(|_| Refusal::NotDeleted)?;
    directory
        .sync_all()
        .map_err(|_| Uncertainty::DeleteNotDurable.into())
}

/// What identifies a file and its last change: a replacement, a new link,
/// or a write or metadata change all differ.
fn version(stat: &Stat) -> impl PartialEq + use<> {
    (
        stat.st_dev,
        stat.st_ino,
        stat.st_nlink,
        stat.st_mode,
        stat.st_size,
        stat.st_mtime,
        stat.st_mtime_nsec,
        stat.st_ctime,
        stat.st_ctime_nsec,
    )
}

/// How a staged file takes the target's name.
enum Publication<'a> {
    /// Only when nothing has the name.
    Exclusive,
    /// Over the file opened as `file`, still as it was when it was `read`.
    Replace { file: &'a File, read: &'a Stat },
}

/// New content, complete and synced, under a hidden name in the target's
/// directory. Until it is published or removed it is removed when dropped,
/// if its name still holds it.
struct Staged<'a> {
    directory: &'a File,
    name: String,
    file: File,
    settled: bool,
}

impl<'a> Staged<'a> {
    /// Stages `content`, with the owner and permissions of `like` when it
    /// replaces a file.
    fn new(directory: &'a File, content: &str, like: Option<&Stat>) -> Result<Self, Failure> {
        let (name, file) = staging_file(directory)?;
        let staged = Self {
            directory,
            name,
            file,
            settled: false,
        };
        match staged.fill(content, like) {
            Ok(()) => Ok(staged),
            Err(refusal) => Err(staged.abandon(refusal)),
        }
    }

    fn fill(&self, content: &str, like: Option<&Stat>) -> Result<(), Refusal> {
        (&self.file)
            .write_all(content.as_bytes())
            .map_err(|_| Refusal::NotStaged)?;
        if let Some(like) = like {
            let staged = rustix::fs::fstat(&self.file).map_err(|_| Refusal::NotStaged)?;
            if staged.st_uid != like.st_uid {
                return Err(Refusal::OtherOwner);
            }
            if staged.st_gid != like.st_gid {
                rustix::fs::fchown(&self.file, None, Some(Gid::from_raw(like.st_gid)))
                    .map_err(|_| Refusal::GroupNotKept)?;
            }
            let permissions = Mode::RWXU | Mode::RWXG | Mode::RWXO;
            rustix::fs::fchmod(&self.file, Mode::from_raw_mode(like.st_mode) & permissions)
                .map_err(|_| Refusal::NotStaged)?;
        }
        self.file.sync_all().map_err(|_| Refusal::NotStaged)?;

        Ok(())
    }

    /// Gives the target's name the staged content, then syncs the directory.
    fn publish(
        mut self,
        name: &str,
        publication: Publication<'_>,
        stop: &StopSignal,
    ) -> Result<(), Failure> {
        if stop.requested() {
            return Err(self.abandon(Refusal::Stopped));
        }
        let exclusive = matches!(publication, Publication::Exclusive);
        let renamed = match publication {
            Publication::Exclusive => rustix::fs::renameat_with(
                self.directory,
                self.name.as_str(),
                self.directory,
                name,
                RenameFlags::NOREPLACE,
            ),
            Publication::Replace { file, read } => {
                let unchanged = |stat: rustix::io::Result<Stat>| {
                    stat.is_ok_and(|stat| version(&stat) == version(read))
                };
                if !unchanged(rustix::fs::fstat(file))
                    || !unchanged(rustix::fs::statat(
                        self.directory,
                        name,
                        AtFlags::SYMLINK_NOFOLLOW,
                    ))
                {
                    return Err(self.abandon(Refusal::Changed));
                }
                rustix::fs::renameat(self.directory, self.name.as_str(), self.directory, name)
            }
        };
        match renamed {
            Ok(()) => self.settled = true,
            Err(Errno::EXIST) if exclusive => return Err(self.abandon(Refusal::Exists)),
            // A rename that failed left both names as they were, unless the
            // failure itself is ambiguous: the staged file must still be
            // where it was put for the call to have changed nothing.
            Err(_) if self.is_in_place() => return Err(self.abandon(Refusal::NotPublished)),
            Err(_) => {
                self.settled = true;
                return Err(Uncertainty::Unknown.into());
            }
        }

        self.directory
            .sync_all()
            .map_err(|_| Uncertainty::NotDurable.into())
    }

    /// Whether the staged name still holds the staged file.
    fn is_in_place(&self) -> bool {
        let (Ok(staged), Ok(named)) = (
            rustix::fs::fstat(&self.file),
            rustix::fs::statat(
                self.directory,
                self.name.as_str(),
                AtFlags::SYMLINK_NOFOLLOW,
            ),
        ) else {
            return false;
        };
        (staged.st_dev, staged.st_ino) == (named.st_dev, named.st_ino)
    }

    /// Removes the staged file and reports `refusal`, or, when the staged
    /// file is not known to be gone, that a copy of the content may remain.
    fn abandon(mut self, refusal: Refusal) -> Failure {
        self.settled = true;
        if self.remove() {
            Failure::Refused(refusal)
        } else {
            Failure::Uncertain(Uncertainty::StagingLeft)
        }
    }

    /// Removes the staged name when it still holds the staged file; nothing
    /// else is ever removed.
    fn remove(&self) -> bool {
        self.is_in_place()
            && rustix::fs::unlinkat(self.directory, self.name.as_str(), AtFlags::empty()).is_ok()
    }
}

impl Drop for Staged<'_> {
    fn drop(&mut self) {
        if !self.settled {
            self.remove();
        }
    }
}

/// A new, empty file under an unused staging name in `directory`, created
/// exclusively and never through a link.
fn staging_file(directory: &File) -> Result<(String, File), Refusal> {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let flags = OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC;
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
            Ok(file) => return Ok((name, File::from(file))),
            Err(Errno::EXIST) => {}
            Err(_) => return Err(Refusal::NotStaged),
        }
    }
    Err(Refusal::NotStaged)
}

/// The parameters a call must match exactly: a create, update, replace, or delete.
fn parameters() -> Value {
    json!({
        "type": "object",
        "properties": {
            "operation": {
                "type": "string",
                "enum": ["create", "update", "replace", "delete"],
                "description": "create a new file, update an existing one, replace exact text in a file, or delete a file"
            },
            "path": {
                "type": "string",
                "description": "A path relative to the authorized Workspace root"
            },
            "content": {
                "type": "string",
                "description": "For create and update: the complete new content of the file"
            },
            "expected_content": {
                "type": "string",
                "description": "For update and delete: the complete current content of the file, exactly as read"
            },
            "old": {
                "type": "string",
                "description": "For replace: the exact text to replace (must occur exactly once)"
            },
            "new": {
                "type": "string",
                "description": "For replace: the new text to put in place of old"
            }
        },
        "required": ["operation", "path"],
        "oneOf": [
            {
                "properties": {"operation": {"const": "create"}},
                "required": ["content"],
                "not": {"anyOf": [{"required": ["expected_content"]}, {"required": ["old"]}, {"required": ["new"]}]}
            },
            {
                "properties": {"operation": {"const": "update"}},
                "required": ["content", "expected_content"],
                "not": {"anyOf": [{"required": ["old"]}, {"required": ["new"]}]}
            },
            {
                "properties": {"operation": {"const": "replace"}},
                "required": ["old", "new"],
                "not": {"anyOf": [{"required": ["content"]}, {"required": ["expected_content"]}]}
            },
            {
                "properties": {"operation": {"const": "delete"}},
                "required": ["expected_content"],
                "not": {"anyOf": [{"required": ["content"]}, {"required": ["old"]}, {"required": ["new"]}]}
            }
        ],
        "additionalProperties": false
    })
}

/// The handler of `workspace_write_file`, bound to one authorized Workspace.
struct WriteFile {
    workspace: Workspace,
    workers: blocking::WorkerTracker,
}

impl ToolHandler for WriteFile {
    fn version(&self) -> &str {
        HANDLER_VERSION
    }

    /// A create, or an update, run again may change the file again.
    fn effect(&self) -> ToolEffect {
        ToolEffect::NonIdempotent
    }

    /// Its blocking workers are tracked, so a stop waits for them.
    fn spawns(&self) -> bool {
        true
    }

    fn call(&self, arguments: Value) -> ToolFuture<'_> {
        self.write(None, arguments)
    }

    fn call_for(&self, call: ToolCallRef, arguments: Value) -> ToolFuture<'_> {
        self.write(Some(call.job), arguments)
    }

    /// Stops the writes of `job` and waits until their workers have ended.
    fn stop(&self, job: JobId) -> Pin<Box<dyn Future<Output = Result<(), ToolFailure>> + '_>> {
        Box::pin(async move {
            self.workers.stop_job(job).await;
            Ok(())
        })
    }
}

impl WriteFile {
    /// Writes on the blocking pool while holding a file permit.
    fn write(&self, job: Option<JobId>, arguments: Value) -> ToolFuture<'_> {
        Box::pin(async move {
            let (path, operation) = parse(&arguments)
                .map_err(|refusal| ToolFailure::not_applied(refusal.to_string()))?;
            let path = path.to_owned();
            let workspace = self.workspace.clone();
            let path_for_worker = path.clone();

            let result = match operation {
                Operation::Create { content } => {
                    let content = content.to_owned();
                    let after_sha256 = ContentDigest::of(content.as_bytes()).to_string();
                    let bytes_written = content.len();
                    self.workers
                        .run_tracked(job, Kind::File, StopSignal::new(), move |stop| {
                            let _mutation = workspace_read::WorkspaceMutation::begin();
                            create(&workspace, &path_for_worker, &content, stop)
                        })
                        .await
                        .map_err(ToolFailure::uncertain)?
                        .map_err(ToolFailure::from)?;
                    json!({
                        "operation": "create",
                        "path": path,
                        "bytes_written": bytes_written,
                        "after_sha256": after_sha256,
                    })
                }
                Operation::Update { content, expected } => {
                    let content = content.to_owned();
                    let expected = expected.to_owned();
                    let after_sha256 = ContentDigest::of(content.as_bytes()).to_string();
                    let bytes_written = content.len();
                    self.workers
                        .run_tracked(job, Kind::File, StopSignal::new(), move |stop| {
                            let _mutation = workspace_read::WorkspaceMutation::begin();
                            update(&workspace, &path_for_worker, &expected, &content, stop)
                        })
                        .await
                        .map_err(ToolFailure::uncertain)?
                        .map_err(ToolFailure::from)?;
                    json!({
                        "operation": "update",
                        "path": path,
                        "bytes_written": bytes_written,
                        "after_sha256": after_sha256,
                    })
                }
                Operation::Replace { old, new } => {
                    let old = old.to_owned();
                    let new = new.to_owned();
                    let (offset, before_sha256, after_sha256) = self
                        .workers
                        .run_tracked(job, Kind::File, StopSignal::new(), move |stop| {
                            let _mutation = workspace_read::WorkspaceMutation::begin();
                            replace(&workspace, &path_for_worker, &old, &new, stop)
                        })
                        .await
                        .map_err(ToolFailure::uncertain)?
                        .map_err(ToolFailure::from)?;
                    json!({
                        "operation": "replace",
                        "path": path,
                        "offset": offset,
                        "before_sha256": before_sha256,
                        "after_sha256": after_sha256,
                    })
                }
                Operation::Delete { expected } => {
                    let expected = expected.to_owned();
                    self.workers
                        .run_tracked(job, Kind::File, StopSignal::new(), move |stop| {
                            let _mutation = workspace_read::WorkspaceMutation::begin();
                            delete(&workspace, &path_for_worker, &expected, stop)
                        })
                        .await
                        .map_err(ToolFailure::uncertain)?
                        .map_err(ToolFailure::from)?;
                    json!({
                        "operation": "delete",
                        "path": path,
                    })
                }
            };
            Ok(result)
        })
    }
}

/// Registers `workspace_write_file` for `workspace` in `registry`, without
/// authorizing it.
pub(crate) fn register(
    registry: &ToolRegistry,
    workspace: Workspace,
) -> Result<(), ToolRegistryError> {
    registry.register(
        TOOL_NAME,
        Some(
            "Create a new UTF-8 text file, replace the whole content of an existing one, replace exact text in a file, or delete a file in the authorized Workspace. Operations: `create` {path, content} fails if the file exists. `update` {path, content, expected_content} requires the whole file to match `expected_content`. `replace` {path, old, new} replaces `old` (must occur exactly once) with `new` (up to 8 KiB each, in files up to 1 MiB). `delete` {path, expected_content} deletes a file matching `expected_content` (up to 64 KiB). Directories are never created or deleted.",
        ),
        parameters(),
        Arc::new(WriteFile {
            workspace,
            workers: Default::default(),
        }),
    )
}
