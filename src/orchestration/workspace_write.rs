//! `workspace_write_file`: OCG's one tool that changes a file. It creates one
//! small UTF-8 text file inside a Workspace the operator authorized, or
//! replaces the whole content of one whose entire current content the call
//! names exactly, and nothing else: it appends, patches, deletes, renames and
//! creates directories never.
//!
//! The Workspace, its path rules and the walk to the file are those of
//! `workspace_read_file`, see [`crate::orchestration::workspace_read`]: the
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
use std::io::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use rustix::fs::{AtFlags, FileType, Gid, Mode, OFlags, RenameFlags, Stat};
use rustix::io::Errno;
use serde_json::{Value, json};

use crate::orchestration::blocking::{self, Kind};
use crate::orchestration::tools::{
    ToolFailure, ToolFuture, ToolHandler, ToolRegistry, ToolRegistryError,
};
use crate::orchestration::workspace_read::{
    self, MAX_FILE_BYTES, Refusal as PathRefusal, Step, Workspace, open_at, open_within,
    read_bytes, read_whole,
};
use crate::persistence::{ContentDigest, ToolEffect};

/// The name the tool is offered and recorded under.
pub const TOOL_NAME: &str = "workspace_write_file";

/// Identifies this handler's behavior in every call it records.
const HANDLER_VERSION: &str = "workspace_write_file-2";

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
    #[error("content, expected_content, old or new is larger than {MAX_FILE_BYTES} bytes")]
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
            if content.len() > MAX_FILE_BYTES {
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
                if t.len() > MAX_FILE_BYTES {
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
                if t.len() > MAX_FILE_BYTES {
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

/// Creates the file at `path` with `content`, when nothing exists there.
fn create(workspace: &Workspace, path: &str, content: &str) -> Result<(), Failure> {
    let (directory, name) = parent(workspace, path)?;
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
    staged.publish(name, Publication::Exclusive)
}

/// Replaces the content of the file at `path` with `content`, when its whole
/// current content is `expected`.
fn update(workspace: &Workspace, path: &str, expected: &str, content: &str) -> Result<(), Failure> {
    let (directory, name) = parent(workspace, path)?;
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
    )
}

/// Replaces a single occurrence of `old` with `new` in the file at `path`.
fn replace(
    workspace: &Workspace,
    path: &str,
    old: &str,
    new: &str,
) -> Result<(usize, String, String), Failure> {
    let (directory, name) = parent(workspace, path)?;
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
    )?;
    Ok((offset, before_sha256, after_sha256))
}

/// Deletes the regular file at `path`, when its whole current content is `expected`.
fn delete(workspace: &Workspace, path: &str, expected: &str) -> Result<(), Failure> {
    let (directory, name) = parent(workspace, path)?;
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
        #[cfg(test)]
        if !write_seam::reach(write_seam::Point::Staged) {
            return Err(Refusal::NotStaged);
        }
        Ok(())
    }

    /// Gives the target's name the staged content, then syncs the directory.
    fn publish(mut self, name: &str, publication: Publication<'_>) -> Result<(), Failure> {
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
        #[cfg(test)]
        if !write_seam::reach(write_seam::Point::Published) {
            return Err(Uncertainty::NotDurable.into());
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
}

impl ToolHandler for WriteFile {
    fn version(&self) -> &str {
        HANDLER_VERSION
    }

    /// A create, or an update, run again may change the file again.
    fn effect(&self) -> ToolEffect {
        ToolEffect::NonIdempotent
    }

    /// Writes on a dedicated blocking worker while holding a file permit.
    fn call(&self, arguments: Value) -> ToolFuture<'_> {
        Box::pin(async move {
            let (path, operation) = parse(&arguments)
                .map_err(|refusal| ToolFailure::not_applied(refusal.to_string()))?;
            let path = path.to_owned();
            let workspace = self.workspace.clone();
            let path_for_worker = path.clone();
            #[cfg(test)]
            let seam = write_seam::take();
            let result = match operation {
                Operation::Create { content } => {
                    let content = content.to_owned();
                    let after_sha256 = ContentDigest::of(content.as_bytes()).to_string();
                    let bytes_written = content.len();
                    blocking::run(Kind::File, move || {
                        #[cfg(test)]
                        write_seam::install_boxed(seam);
                        let _mutation = workspace_read::WorkspaceMutation::begin();
                        create(&workspace, &path_for_worker, &content)
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
                    blocking::run(Kind::File, move || {
                        #[cfg(test)]
                        write_seam::install_boxed(seam);
                        let _mutation = workspace_read::WorkspaceMutation::begin();
                        update(&workspace, &path_for_worker, &expected, &content)
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
                    let (offset, before_sha256, after_sha256) =
                        blocking::run(Kind::File, move || {
                            #[cfg(test)]
                            write_seam::install_boxed(seam);
                            let _mutation = workspace_read::WorkspaceMutation::begin();
                            replace(&workspace, &path_for_worker, &old, &new)
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
                    blocking::run(Kind::File, move || {
                        #[cfg(test)]
                        write_seam::install_boxed(seam);
                        let _mutation = workspace_read::WorkspaceMutation::begin();
                        delete(&workspace, &path_for_worker, &expected)
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
        Arc::new(WriteFile { workspace }),
    )
}

/// Test seams of a write: each runs at its point and may make the step
/// there fail.
#[cfg(test)]
pub(crate) mod write_seam {
    use std::cell::RefCell;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(crate) enum Point {
        /// The new content is staged and synced; nothing is published. A
        /// failure here is a failure to stage.
        Staged,
        /// The staged file has the target's name; the directory is not
        /// synced. A failure here is a failure to sync it.
        Published,
    }

    type Seam = Box<dyn FnMut(Point) -> bool + Send>;

    thread_local! {
        static SEAM: RefCell<Option<Seam>> = const { RefCell::new(None) };
    }

    /// Runs `seam` at every point of this thread's writes; a step fails when
    /// it returns `false`.
    pub(crate) fn install(seam: impl FnMut(Point) -> bool + Send + 'static) {
        SEAM.with(|installed| *installed.borrow_mut() = Some(Box::new(seam)));
    }

    pub(super) fn take() -> Option<Seam> {
        SEAM.with(|installed| installed.borrow_mut().take())
    }

    pub(super) fn install_boxed(seam: Option<Seam>) {
        SEAM.with(|installed| *installed.borrow_mut() = seam);
    }

    pub(crate) fn clear() {
        SEAM.with(|installed| *installed.borrow_mut() = None);
    }

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

#[cfg(test)]
mod tests {
    use std::os::unix::fs::{PermissionsExt, symlink};
    use std::path::{Path, PathBuf};

    use super::write_seam::Point;
    use super::*;
    use crate::data_directory::DataDirectory;
    use crate::orchestration::execution_runtime::ToolExecution;
    use crate::test_support::{TestRoot, data_directory, temporary_directory};

    const SUITE: &str = "ocg-workspace-write";

    /// An empty Workspace under the system temp dir, removed when dropped,
    /// beside a data directory of its own.
    fn workspace(name: &str) -> (TestRoot, PathBuf, TestRoot, DataDirectory) {
        let (guard, root) = temporary_directory(SUITE, &format!("{name}-workspace"));
        let (data_root, directory) = data_directory(SUITE, name);
        (guard, root, data_root, directory)
    }

    fn write(root: &Path, path: &str, content: &str) {
        let path = root.join(path);
        std::fs::create_dir_all(path.parent().expect("a parent")).expect("create parents");
        std::fs::write(path, content).expect("write");
    }

    fn content(root: &Path, path: &str) -> String {
        std::fs::read_to_string(root.join(path)).expect("read back")
    }

    /// Runs the handler as the tool loop does, with `arguments`, and checks
    /// that no staged file is left anywhere in the Workspace.
    fn call(workspace: &Workspace, arguments: Value) -> Result<Value, ToolFailure> {
        let handler = WriteFile {
            workspace: workspace.clone(),
        };
        let result = crate::test_support::wait(handler.call(arguments));
        write_seam::clear();
        result
    }

    fn create_call(path: &str, content: &str) -> Value {
        json!({"operation": "create", "path": path, "content": content})
    }

    fn update_call(path: &str, expected: &str, content: &str) -> Value {
        json!({"operation": "update", "path": path, "expected_content": expected, "content": content})
    }

    fn refused(refusal: impl Into<Failure>) -> Result<Value, ToolFailure> {
        Err(ToolFailure::from(refusal.into()))
    }

    /// Every staged file left under `root`.
    fn staged_files(root: &Path) -> Vec<PathBuf> {
        let mut found = Vec::new();
        let mut directories = vec![root.to_path_buf()];
        while let Some(directory) = directories.pop() {
            for entry in std::fs::read_dir(&directory).expect("list") {
                let entry = entry.expect("entry");
                let kind = entry.file_type().expect("type");
                if kind.is_dir() {
                    directories.push(entry.path());
                } else if entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(STAGING_PREFIX)
                {
                    found.push(entry.path());
                }
            }
        }
        found
    }

    #[test]
    fn create_publishes_a_new_file_whole() {
        let (_guard, root, _data, directory) = workspace("create");
        std::fs::create_dir(root.join("docs")).expect("a directory");
        let workspace = Workspace::authorize(&root, &directory).expect("authorize");
        let text = "Hello from OCG.\nünïcødé ✓ \"quotes\" \\ and a tab\t\n";
        assert_eq!(
            call(&workspace, create_call("notes.txt", text)),
            Ok(json!({
                "operation": "create",
                "path": "notes.txt",
                "bytes_written": text.len(),
                "after_sha256": ContentDigest::of(text.as_bytes()).to_string(),
            })),
            "the result is compact and repeats no content"
        );
        assert_eq!(content(&root, "notes.txt"), text);
        let mode = std::fs::metadata(root.join("notes.txt"))
            .expect("metadata")
            .permissions()
            .mode();
        assert_eq!(mode & 0o077, 0, "a new file is private to its owner");

        let at_limit = "a".repeat(MAX_FILE_BYTES);
        assert!(call(&workspace, create_call("docs/limit.txt", &at_limit)).is_ok());
        assert_eq!(content(&root, "docs/limit.txt"), at_limit);
        assert!(call(&workspace, create_call("empty.txt", "")).is_ok());
        assert_eq!(content(&root, "empty.txt"), "");
        assert!(staged_files(&root).is_empty());
    }

    #[test]
    fn create_never_replaces_or_follows_anything_at_the_path() {
        let (_guard, root, _data, directory) = workspace("create-existing");
        let (_outside_guard, outside, _outside_data, _) = workspace("create-existing-outside");
        write(&root, "notes.txt", "original\n");
        write(&root, "dir/inner.txt", "inner\n");
        symlink(outside.join("target.txt"), root.join("dangling.txt")).expect("link");
        symlink(root.join("notes.txt"), root.join("alias.txt")).expect("link");
        let workspace = Workspace::authorize(&root, &directory).expect("authorize");

        // Equal content is no reason to treat an existing file as created.
        assert_eq!(
            call(&workspace, create_call("notes.txt", "original\n")),
            refused(Refusal::Exists)
        );
        assert_eq!(
            call(&workspace, create_call("dir", "x")),
            refused(Refusal::Exists)
        );
        for link in ["dangling.txt", "alias.txt"] {
            assert_eq!(
                call(&workspace, create_call(link, "x")),
                refused(PathRefusal::SymbolicLink),
                "{link}"
            );
        }
        assert_eq!(content(&root, "notes.txt"), "original\n");
        assert!(root.join("dir").is_dir());
        assert!(!outside.join("target.txt").exists(), "no link was followed");
        assert!(staged_files(&root).is_empty());
    }

    #[test]
    fn missing_directories_are_never_created() {
        let (_guard, root, _data, directory) = workspace("create-parents");
        let workspace = Workspace::authorize(&root, &directory).expect("authorize");
        assert_eq!(
            call(&workspace, create_call("missing/notes.txt", "x")),
            refused(Refusal::MissingDirectory)
        );
        assert!(!root.join("missing").exists());
        assert_eq!(
            call(&workspace, update_call("missing/notes.txt", "", "x")),
            refused(Refusal::MissingDirectory)
        );
    }

    #[test]
    fn a_failed_staging_exposes_nothing_under_the_target_name() {
        let (_guard, root, _data, directory) = workspace("staging-failure");
        write(&root, "notes.txt", "original\n");
        let workspace = Workspace::authorize(&root, &directory).expect("authorize");
        for (arguments, path, before) in [
            (create_call("new.txt", "complete\n"), "new.txt", None),
            (
                update_call("notes.txt", "original\n", "complete\n"),
                "notes.txt",
                Some("original\n"),
            ),
        ] {
            let (seen_root, target) = (root.clone(), path.to_owned());
            write_seam::install(move |point| {
                assert_eq!(point, Point::Staged);
                let [staged] = staged_files(&seen_root)
                    .try_into()
                    .expect("one staged file");
                assert_eq!(
                    std::fs::read_to_string(staged).expect("read"),
                    "complete\n",
                    "the content is staged whole"
                );
                assert_eq!(
                    std::fs::read_to_string(seen_root.join(&target))
                        .ok()
                        .as_deref(),
                    before,
                    "the target is untouched while the content is staged"
                );
                false
            });
            assert_eq!(call(&workspace, arguments), refused(Refusal::NotStaged));
            assert_eq!(
                std::fs::read_to_string(root.join(path)).ok().as_deref(),
                before
            );
            assert!(staged_files(&root).is_empty(), "the staged file is removed");
        }
    }

    #[test]
    fn a_file_created_meanwhile_is_never_replaced() {
        let (_guard, root, _data, directory) = workspace("create-race");
        let workspace = Workspace::authorize(&root, &directory).expect("authorize");
        let seen_root = root.clone();
        write_seam::install(move |point| {
            if point == Point::Staged {
                write(&seen_root, "notes.txt", "theirs\n");
            }
            true
        });
        assert_eq!(
            call(&workspace, create_call("notes.txt", "ours\n")),
            refused(Refusal::Exists)
        );
        assert_eq!(content(&root, "notes.txt"), "theirs\n");
        assert!(staged_files(&root).is_empty());
    }

    #[test]
    fn update_replaces_only_exactly_the_expected_content() {
        let (_guard, root, _data, directory) = workspace("update");
        let original = "Project: OCG\nStatus: pending\n";
        write(&root, "notes.txt", original);
        std::fs::set_permissions(root.join("notes.txt"), PermissionsExt::from_mode(0o640))
            .expect("set permissions");
        let workspace = Workspace::authorize(&root, &directory).expect("authorize");
        for expected in [
            "Project: OCG\nStatus: pending",
            "Project: OCG\nStatus: pending\n\n",
            "project: OCG\nStatus: pending\n",
            "",
        ] {
            assert_eq!(
                call(&workspace, update_call("notes.txt", expected, "changed\n")),
                refused(Refusal::Mismatch),
                "{expected:?}"
            );
            assert_eq!(content(&root, "notes.txt"), original);
        }
        let updated = "Project: OCG\nStatus: completed\n";
        assert_eq!(
            call(&workspace, update_call("notes.txt", original, updated)),
            Ok(json!({
                "operation": "update",
                "path": "notes.txt",
                "bytes_written": updated.len(),
                "after_sha256": ContentDigest::of(updated.as_bytes()).to_string(),
            }))
        );
        assert_eq!(content(&root, "notes.txt"), updated);
        let metadata = std::fs::metadata(root.join("notes.txt")).expect("metadata");
        assert_eq!(
            metadata.permissions().mode() & 0o7777,
            0o640,
            "permissions kept"
        );
        assert!(staged_files(&root).is_empty());
    }

    #[test]
    fn an_empty_file_is_updated_by_an_explicitly_empty_expected_content() {
        let (_guard, root, _data, directory) = workspace("update-empty");
        write(&root, "empty.txt", "");
        let workspace = Workspace::authorize(&root, &directory).expect("authorize");
        assert!(call(&workspace, update_call("empty.txt", "", "filled\n")).is_ok());
        assert_eq!(content(&root, "empty.txt"), "filled\n");
        assert!(call(&workspace, update_call("empty.txt", "filled\n", "")).is_ok());
        assert_eq!(content(&root, "empty.txt"), "");
    }

    #[test]
    fn update_refuses_absent_linked_and_unsupported_targets() {
        let (_guard, root, _data, directory) = workspace("update-targets");
        write(&root, "notes.txt", "x");
        write(&root, "dir/inner.txt", "x");
        std::fs::hard_link(root.join("notes.txt"), root.join("hard.txt")).expect("hard link");
        symlink(root.join("dir/inner.txt"), root.join("soft.txt")).expect("link");
        write(&root, "big.txt", &"a".repeat(MAX_FILE_BYTES + 1));
        let workspace = Workspace::authorize(&root, &directory).expect("authorize");
        for (path, refusal) in [
            ("absent.txt", PathRefusal::NotFound),
            ("hard.txt", PathRefusal::HardLinked),
            ("notes.txt", PathRefusal::HardLinked),
            ("soft.txt", PathRefusal::SymbolicLink),
            ("dir", PathRefusal::NotRegularFile),
            ("big.txt", PathRefusal::TooLarge),
        ] {
            assert_eq!(
                call(&workspace, update_call(path, "x", "y")),
                refused(refusal),
                "{path}"
            );
        }
        assert!(!root.join("absent.txt").exists());
        assert_eq!(content(&root, "notes.txt"), "x");
        assert_eq!(content(&root, "dir/inner.txt"), "x");
        assert!(staged_files(&root).is_empty());
    }

    #[test]
    fn a_target_changed_before_publication_is_not_replaced() {
        let (_guard, root, _data, directory) = workspace("update-race");
        let workspace = Workspace::authorize(&root, &directory).expect("authorize");
        /// A change another process makes, and what it is.
        type Change = (&'static str, fn(&Path));
        let changes: [Change; 3] = [
            ("written in place", |root| {
                std::fs::OpenOptions::new()
                    .append(true)
                    .open(root.join("notes.txt"))
                    .and_then(|mut file| file.write_all(b"theirs\n"))
                    .expect("append");
            }),
            ("replaced by another file", |root| {
                std::fs::write(root.join("other.txt"), "theirs\n").expect("write");
                std::fs::rename(root.join("other.txt"), root.join("notes.txt")).expect("rename");
            }),
            ("given a second link", |root| {
                std::fs::hard_link(root.join("notes.txt"), root.join("second.txt")).expect("link");
            }),
        ];
        for (case, change) in changes {
            let _ = std::fs::remove_file(root.join("second.txt"));
            write(&root, "notes.txt", "original\n");
            let seen_root = root.clone();
            write_seam::install(move |point| {
                if point == Point::Staged {
                    change(&seen_root);
                }
                true
            });
            assert_eq!(
                call(&workspace, update_call("notes.txt", "original\n", "ours\n")),
                refused(Refusal::Changed),
                "{case}"
            );
            assert_ne!(content(&root, "notes.txt"), "ours\n", "{case}");
            assert!(staged_files(&root).is_empty(), "{case}");
        }
    }

    #[test]
    fn a_failure_after_publication_is_uncertain_not_refused() {
        let (_guard, root, _data, directory) = workspace("publication-failure");
        write(&root, "notes.txt", "original\n");
        let workspace = Workspace::authorize(&root, &directory).expect("authorize");
        for (arguments, path) in [
            (create_call("new.txt", "ours\n"), "new.txt"),
            (
                update_call("notes.txt", "original\n", "ours\n"),
                "notes.txt",
            ),
        ] {
            write_seam::install(|point| point != Point::Published);
            assert_eq!(
                call(&workspace, arguments),
                Err(ToolFailure::uncertain(
                    "the new content was published, but its durability could not be confirmed"
                ))
            );
            assert_eq!(content(&root, path), "ours\n", "the change did happen");
            assert!(staged_files(&root).is_empty());
        }
    }

    #[test]
    fn paths_outside_plain_relative_workspace_names_are_refused() {
        let (_guard, root, _data, directory) = workspace("paths");
        let (_outside_guard, outside, _outside_data, _) = workspace("paths-outside");
        write(&outside, "secret.txt", "outside\n");
        write(&root, "real/file.txt", "real\n");
        symlink(&outside, root.join("outside")).expect("link");
        symlink(root.join("real"), root.join("linked")).expect("link");
        symlink(directory.root(), root.join("data")).expect("link");
        let workspace = Workspace::authorize(&root, &directory).expect("authorize");
        let absolute = outside.join("secret.txt");
        for (path, refusal) in [
            ("/etc/passwd", PathRefusal::InvalidPath),
            (absolute.to_str().expect("UTF-8"), PathRefusal::InvalidPath),
            (
                "../paths-outside-workspace/secret.txt",
                PathRefusal::InvalidPath,
            ),
            ("real/../real/file.txt", PathRefusal::InvalidPath),
            ("", PathRefusal::InvalidPath),
            ("real\\file.txt", PathRefusal::InvalidPath),
            ("outside/secret.txt", PathRefusal::SymbolicLink),
            ("linked/file.txt", PathRefusal::SymbolicLink),
            ("data/ocg_db/new.txt", PathRefusal::Protected),
            ("data/notes.txt", PathRefusal::SymbolicLink),
            (".env", PathRefusal::Protected),
            (".ocg-write-1-2-3", PathRefusal::Protected),
            ("keys/server.pem", PathRefusal::Protected),
            ("ocg_db/ocg.redb", PathRefusal::Protected),
            ("config/secrets.yaml", PathRefusal::Protected),
        ] {
            assert_eq!(
                call(&workspace, create_call(path, "x")),
                refused(refusal),
                "create {path:?}"
            );
            assert_eq!(
                call(&workspace, update_call(path, "outside\n", "x")),
                refused(refusal),
                "update {path:?}"
            );
        }
        assert_eq!(content(&outside, "secret.txt"), "outside\n");
        assert_eq!(content(&root, "real/file.txt"), "real\n");
        assert!(std::fs::read_dir(directory.root()).is_ok_and(|entries| {
            entries
                .filter_map(Result::ok)
                .all(|entry| entry.file_name() != "notes.txt")
        }));
    }

    #[test]
    fn a_replaced_workspace_root_is_refused() {
        let (_guard, root, _data, directory) = workspace("replaced");
        let workspace = Workspace::authorize(&root, &directory).expect("authorize");
        let (_moved_guard, moved) = temporary_directory(SUITE, "replaced-original");
        std::fs::remove_dir(&moved).expect("free the name");
        std::fs::rename(&root, &moved).expect("move the Workspace away");
        std::fs::create_dir(&root).expect("create another directory");
        assert_eq!(
            call(&workspace, create_call("notes.txt", "x")),
            refused(PathRefusal::WorkspaceChanged)
        );
        assert!(!root.join("notes.txt").exists() && !moved.join("notes.txt").exists());
    }

    #[test]
    fn malformed_arguments_are_never_a_write() {
        let (_guard, root, _data, directory) = workspace("arguments");
        write(&root, "notes.txt", "original\n");
        let workspace = Workspace::authorize(&root, &directory).expect("authorize");
        let validator = crate::openai_compatible::admission::compile_parameters(&parameters())
            .expect("an enforceable schema");
        let invalid = [
            json!({"operation": "update", "path": "notes.txt", "content": "x"}),
            json!({"operation": "update", "path": "notes.txt", "expected_content": null, "content": "x"}),
            json!({"operation": "create", "path": "notes.txt", "expected_content": "", "content": "x"}),
            json!({"operation": "overwrite", "path": "notes.txt", "content": "x"}),
            json!({"operation": "create", "path": "notes.txt"}),
            json!({"operation": "create", "content": "x"}),
            json!({"path": "notes.txt", "content": "x"}),
            json!({"operation": "create", "path": "notes.txt", "content": "x", "mode": "0777"}),
            json!({"operation": "create", "path": ["notes.txt"], "content": "x"}),
            json!({"operation": "create", "path": "notes.txt", "content": 7}),
            json!(["create", "notes.txt", "x"]),
            json!({"operation": "replace", "path": "notes.txt", "old": "x"}),
            json!({"operation": "replace", "path": "notes.txt", "new": "x"}),
            json!({"operation": "replace", "path": "notes.txt", "old": "x", "new": "y", "content": "z"}),
            json!({"operation": "delete", "path": "notes.txt"}),
            json!({"operation": "delete", "path": "notes.txt", "expected_content": "x", "content": "y"}),
        ];
        for arguments in invalid {
            assert!(
                !validator.is_valid(&arguments),
                "the schema refuses {arguments}"
            );
            assert_eq!(
                call(&workspace, arguments.clone()),
                refused(Refusal::InvalidArguments),
                "the handler refuses {arguments} too"
            );
        }
        for valid in [
            create_call("new.txt", ""),
            update_call("notes.txt", "", "x"),
            json!({"operation": "replace", "path": "notes.txt", "old": "a", "new": "b"}),
            json!({"operation": "delete", "path": "notes.txt", "expected_content": "x"}),
        ] {
            assert!(validator.is_valid(&valid), "{valid}");
        }
        let oversized = "a".repeat(MAX_FILE_BYTES + 1);
        for arguments in [
            create_call("new.txt", &oversized),
            update_call("notes.txt", &oversized, "x"),
            update_call("notes.txt", "original\n", &oversized),
            json!({"operation": "replace", "path": "notes.txt", "old": &oversized, "new": "x"}),
            json!({"operation": "replace", "path": "notes.txt", "old": "x", "new": &oversized}),
        ] {
            assert_eq!(call(&workspace, arguments), refused(Refusal::TooLarge));
        }
        for arguments in [
            create_call("new.txt", "a\0b"),
            update_call("notes.txt", "original\n", "a\0b"),
            json!({"operation": "replace", "path": "notes.txt", "old": "a\0b", "new": "x"}),
            json!({"operation": "delete", "path": "notes.txt", "expected_content": "a\0b"}),
        ] {
            assert_eq!(call(&workspace, arguments), refused(Refusal::NotText));
        }
        assert_eq!(
            call(
                &workspace,
                json!({"operation": "replace", "path": "notes.txt", "old": "", "new": "x"})
            ),
            refused(Refusal::InvalidArguments)
        );
        let delete_oversized = "a".repeat(MAX_DELETE_BYTES + 1);
        assert_eq!(
            call(
                &workspace,
                json!({"operation": "delete", "path": "notes.txt", "expected_content": &delete_oversized})
            ),
            refused(Refusal::FileTooLargeForDelete)
        );
        assert_eq!(content(&root, "notes.txt"), "original\n");
        assert!(!root.join("new.txt").exists());
    }

    #[test]
    fn failures_repeat_neither_the_path_nor_any_content() {
        let (_guard, root, _data, directory) = workspace("messages");
        write(&root, "private-name.txt", "private content\n");
        let workspace = Workspace::authorize(&root, &directory).expect("authorize");
        for arguments in [
            create_call("private-name.txt", "secret new content"),
            update_call(
                "private-name.txt",
                "secret expected content",
                "secret new content",
            ),
            update_call("missing/private-name.txt", "secret", "secret"),
        ] {
            let failure = call(&workspace, arguments).expect_err("refused");
            for leaked in ["private", "secret"] {
                assert!(!failure.message.contains(leaked), "{}", failure.message);
            }
        }
    }

    #[test]
    fn the_tools_are_offered_only_together_and_only_when_asked() {
        let (_guard, root, _data, directory) = workspace("registration");
        let authorize = || Workspace::authorize(&root, &directory).expect("authorize");
        let read_only = crate::orchestration::workspace_command::workspace_tools(
            authorize(),
            crate::orchestration::workspace_command::Grant::Read,
            &[],
        )
        .expect("read tools");
        let names = |tools: &ToolExecution| {
            tools
                .offer
                .wires()
                .expect("offered")
                .into_iter()
                .map(|wire| wire.function.name)
                .collect::<Vec<_>>()
        };
        assert!(
            !names(&read_only).iter().any(|name| name == TOOL_NAME),
            "reading never offers writing"
        );
        let editing = crate::orchestration::workspace_command::workspace_tools(
            authorize(),
            crate::orchestration::workspace_command::Grant::Edit,
            &[],
        )
        .expect("edit tools");
        assert!(names(&editing).iter().any(|name| name == TOOL_NAME));
        let wires = editing.offer.wires().expect("offered");
        let write_wire = wires
            .iter()
            .find(|w| w.function.name == TOOL_NAME)
            .expect("write wire");
        assert_eq!(write_wire.function.parameters, Some(parameters()));

        let handler = WriteFile {
            workspace: authorize(),
        };
        assert_eq!(handler.effect(), ToolEffect::NonIdempotent);
        assert_eq!(handler.version(), "workspace_write_file-2");
        assert!(crate::persistence::is_handler_version(handler.version()));
        let registry = ToolRegistry::default();
        register(&registry, authorize()).expect("register");
        assert!(
            matches!(
                registry.offer(&[TOOL_NAME]),
                Err(ToolRegistryError::Unauthorized(_))
            ),
            "registering authorizes nothing"
        );
    }

    #[test]
    fn replace_and_delete_in_workspace() {
        let (_guard, root, _data, directory) = workspace("replace-and-delete");
        write(&root, "notes.txt", "hello world\n");
        let workspace = Workspace::authorize(&root, &directory).expect("authorize");

        // Test replace
        let replace_call = json!({
            "operation": "replace",
            "path": "notes.txt",
            "old": "world",
            "new": "everyone",
        });
        let result = call(&workspace, replace_call).expect("replace ok");
        assert_eq!(result["operation"], "replace");
        assert_eq!(result["path"], "notes.txt");
        assert_eq!(result["offset"], 6);
        assert_eq!(
            result["before_sha256"],
            ContentDigest::of(b"hello world\n").to_string()
        );
        assert_eq!(
            result["after_sha256"],
            ContentDigest::of(b"hello everyone\n").to_string()
        );
        assert_eq!(content(&root, "notes.txt"), "hello everyone\n");

        // Occurrences check: 0 occurrences
        let fail_zero = json!({
            "operation": "replace",
            "path": "notes.txt",
            "old": "nonexistent",
            "new": "x",
        });
        assert_eq!(
            call(&workspace, fail_zero),
            refused(Refusal::Occurrences(0))
        );

        // Occurrences check: multiple occurrences
        write(&root, "repeat.txt", "abc abc abc\n");
        let fail_multi = json!({
            "operation": "replace",
            "path": "repeat.txt",
            "old": "abc",
            "new": "x",
        });
        assert_eq!(
            call(&workspace, fail_multi),
            refused(Refusal::Occurrences(3))
        );

        // Occurrences check: overlapping occurrences ("aa" in "aaa" is 2)
        write(&root, "overlap.txt", "aaa");
        let fail_overlap = json!({
            "operation": "replace",
            "path": "overlap.txt",
            "old": "aa",
            "new": "x",
        });
        assert_eq!(
            call(&workspace, fail_overlap),
            refused(Refusal::Occurrences(2))
        );

        // Test delete
        let del_call = json!({
            "operation": "delete",
            "path": "notes.txt",
            "expected_content": "hello everyone\n",
        });
        let del_result = call(&workspace, del_call).expect("delete ok");
        assert_eq!(del_result["operation"], "delete");
        assert_eq!(del_result["path"], "notes.txt");
        assert!(!root.join("notes.txt").exists());

        // Delete mismatch
        write(&root, "mismatch.txt", "actual\n");
        let fail_mismatch = json!({
            "operation": "delete",
            "path": "mismatch.txt",
            "expected_content": "wrong\n",
        });
        assert_eq!(call(&workspace, fail_mismatch), refused(Refusal::Mismatch));

        // Refuse directory
        std::fs::create_dir(root.join("subdir")).expect("mkdir");
        let fail_dir = json!({
            "operation": "delete",
            "path": "subdir",
            "expected_content": "",
        });
        assert_eq!(
            call(&workspace, fail_dir),
            refused(PathRefusal::NotRegularFile)
        );

        // Refuse symlink
        write(&root, "del_target.txt", "target\n");
        symlink(root.join("del_target.txt"), root.join("del_symlink.txt")).expect("symlink");
        let fail_symlink = json!({
            "operation": "delete",
            "path": "del_symlink.txt",
            "expected_content": "target\n",
        });
        assert_eq!(
            call(&workspace, fail_symlink),
            refused(PathRefusal::SymbolicLink)
        );

        // Refuse hard link
        write(&root, "del_hard_target.txt", "hard\n");
        std::fs::hard_link(
            root.join("del_hard_target.txt"),
            root.join("del_hardlink.txt"),
        )
        .expect("hard link");
        let fail_hardlink = json!({
            "operation": "delete",
            "path": "del_hardlink.txt",
            "expected_content": "hard\n",
        });
        assert_eq!(
            call(&workspace, fail_hardlink),
            refused(PathRefusal::HardLinked)
        );

        // Refuse file over 64 KiB
        let big = vec![b'a'; 65 * 1024];
        std::fs::write(root.join("del_big.txt"), &big).expect("write big");
        let fail_big = json!({
            "operation": "delete",
            "path": "del_big.txt",
            "expected_content": "dummy",
        });
        assert_eq!(
            call(&workspace, fail_big),
            refused(Refusal::FileTooLargeForDelete)
        );
    }
}
